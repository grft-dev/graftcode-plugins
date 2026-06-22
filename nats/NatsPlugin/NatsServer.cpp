#include "NatsServer.h"

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#endif

#include <nlohmann/json.hpp>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <mutex>
#include <optional>
#include <regex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#ifdef max
#undef max
#endif
#ifdef min
#undef min
#endif

typedef unsigned char byte;

using namespace GraftcodeGateway;
using namespace Graftcode::Plugins::Nats;

namespace
{
    using Json = nlohmann::json;

    struct NatsServerConfig
    {
        std::string name{ "NatsPlugin" };
        std::string host{ "127.0.0.1" };
        std::uint16_t port{ 4222 };
        std::string queue{};
        std::string replyQueue{};
        std::string user{};
        std::string password{};
        std::string vhost{};
        std::uint32_t rpcTimeoutMs{ 30000 };
    };

#ifdef _WIN32
    using SocketHandle = SOCKET;
    constexpr SocketHandle kInvalidSocket = INVALID_SOCKET;
#else
    using SocketHandle = int;
    constexpr SocketHandle kInvalidSocket = -1;
#endif

    enum class WaitStatus
    {
        Readable,
        Timeout,
        Error
    };

    struct NatsMsgHeader
    {
        std::string subject;
        std::string sid;
        std::string replyTo;
        std::size_t payloadSize{ 0 };
    };

    enum class FrameKind
    {
        Info,
        Ping,
        Pong,
        Ok,
        Error,
        Message,
        Unknown
    };

    struct NatsFrame
    {
        FrameKind kind{ FrameKind::Unknown };
        std::string errorText{};
        std::string subject{};
        std::string replyTo{};
        std::vector<byte> payload{};
    };

    std::atomic_bool g_stopRequested{ false };
    NatsServerConfig g_serverConfig;
    std::mutex g_serverConfigMutex;
    IServer::ProcessMessageFn g_processMessage = nullptr;
    std::mutex g_processMessageMutex;
    std::mutex g_logMutex;
    std::mutex g_socketMutex;
    SocketHandle g_activeSocket = kInvalidSocket;

    void logInfo(const std::string& message)
    {
        std::lock_guard<std::mutex> lock(g_logMutex);
        std::cout << "[INFO] " << message << std::endl;
    }

    void logWarn(const std::string& message)
    {
        std::lock_guard<std::mutex> lock(g_logMutex);
        std::cout << "[WARN] " << message << std::endl;
    }

    bool shouldStop()
    {
        return g_stopRequested.load(std::memory_order_acquire);
    }

    void closeSocket(SocketHandle socket)
    {
        if (socket == kInvalidSocket) {
            return;
        }
#ifdef _WIN32
        closesocket(socket);
#else
        ::shutdown(socket, SHUT_RDWR);
        ::close(socket);
#endif
    }

    void setActiveSocket(SocketHandle socket)
    {
        std::lock_guard<std::mutex> lock(g_socketMutex);
        g_activeSocket = socket;
    }

    void clearActiveSocket(SocketHandle socket)
    {
        std::lock_guard<std::mutex> lock(g_socketMutex);
        if (g_activeSocket == socket) {
            g_activeSocket = kInvalidSocket;
        }
    }

    void closeActiveSocket()
    {
        std::lock_guard<std::mutex> lock(g_socketMutex);
        if (g_activeSocket != kInvalidSocket) {
            closeSocket(g_activeSocket);
            g_activeSocket = kInvalidSocket;
        }
    }

    WaitStatus waitReadable(SocketHandle socket, int timeoutMs)
    {
        fd_set readSet;
        FD_ZERO(&readSet);
        FD_SET(socket, &readSet);

        timeval timeout{};
        timeout.tv_sec = timeoutMs / 1000;
        timeout.tv_usec = (timeoutMs % 1000) * 1000;

#ifdef _WIN32
        const int result = ::select(0, &readSet, nullptr, nullptr, &timeout);
#else
        const int result = ::select(socket + 1, &readSet, nullptr, nullptr, &timeout);
#endif

        if (result > 0 && FD_ISSET(socket, &readSet)) {
            return WaitStatus::Readable;
        }
        if (result == 0) {
            return WaitStatus::Timeout;
        }
        return WaitStatus::Error;
    }

    bool sendAll(SocketHandle socket, const char* buffer, std::size_t size)
    {
        if (size > 0 && buffer == nullptr) {
            return false;
        }

        std::size_t sentTotal = 0;
        while (sentTotal < size) {
#ifdef _WIN32
            const int sent = ::send(socket, buffer + sentTotal, static_cast<int>(size - sentTotal), 0);
#else
            const ssize_t sent = ::send(socket, buffer + sentTotal, size - sentTotal, 0);
#endif
            if (sent <= 0) {
                return false;
            }
            sentTotal += static_cast<std::size_t>(sent);
        }
        return true;
    }

    bool readExact(
        SocketHandle socket,
        char* outBuffer,
        std::size_t size,
        const std::chrono::steady_clock::time_point deadline)
    {
        std::size_t readTotal = 0;
        while (readTotal < size) {
            const auto now = std::chrono::steady_clock::now();
            if (now >= deadline) {
                return false;
            }
            const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
            const WaitStatus status = waitReadable(socket, static_cast<int>(remaining.count()));
            if (status != WaitStatus::Readable) {
                return false;
            }

#ifdef _WIN32
            const int bytesRead = ::recv(socket, outBuffer + readTotal, static_cast<int>(size - readTotal), 0);
#else
            const ssize_t bytesRead = ::recv(socket, outBuffer + readTotal, size - readTotal, 0);
#endif
            if (bytesRead <= 0) {
                return false;
            }
            readTotal += static_cast<std::size_t>(bytesRead);
        }

        return true;
    }

    bool readLine(
        SocketHandle socket,
        std::string& line,
        const std::chrono::steady_clock::time_point deadline)
    {
        line.clear();
        while (true) {
            const auto now = std::chrono::steady_clock::now();
            if (now >= deadline) {
                return false;
            }
            const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
            const WaitStatus status = waitReadable(socket, static_cast<int>(remaining.count()));
            if (status != WaitStatus::Readable) {
                return false;
            }

            char c = '\0';
#ifdef _WIN32
            const int bytesRead = ::recv(socket, &c, 1, 0);
#else
            const ssize_t bytesRead = ::recv(socket, &c, 1, 0);
#endif
            if (bytesRead <= 0) {
                return false;
            }

            if (c == '\n') {
                if (!line.empty() && line.back() == '\r') {
                    line.pop_back();
                }
                return true;
            }

            line.push_back(c);
        }
    }

    bool parseMsgHeader(const std::string& headerLine, NatsMsgHeader& header)
    {
        std::istringstream iss(headerLine);
        std::vector<std::string> tokens;
        for (std::string token; iss >> token;) {
            tokens.push_back(std::move(token));
        }

        if (tokens.size() != 4 && tokens.size() != 5) {
            return false;
        }
        if (tokens[0] != "MSG") {
            return false;
        }

        header.subject = tokens[1];
        header.sid = tokens[2];
        if (tokens.size() == 5) {
            header.replyTo = tokens[3];
        }
        else {
            header.replyTo.clear();
        }

        try {
            header.payloadSize = static_cast<std::size_t>(std::stoull(tokens.back()));
        }
        catch (...) {
            return false;
        }

        return true;
    }

    bool readFrame(
        SocketHandle socket,
        NatsFrame& frame,
        const std::chrono::steady_clock::time_point deadline)
    {
        frame = NatsFrame{};

        std::string line;
        if (!readLine(socket, line, deadline)) {
            return false;
        }

        if (line.rfind("INFO ", 0) == 0) {
            frame.kind = FrameKind::Info;
            return true;
        }
        if (line == "PING") {
            frame.kind = FrameKind::Ping;
            return true;
        }
        if (line == "PONG") {
            frame.kind = FrameKind::Pong;
            return true;
        }
        if (line == "+OK") {
            frame.kind = FrameKind::Ok;
            return true;
        }
        if (line.rfind("-ERR", 0) == 0) {
            frame.kind = FrameKind::Error;
            frame.errorText = line;
            return true;
        }
        if (line.rfind("MSG ", 0) == 0) {
            NatsMsgHeader header;
            if (!parseMsgHeader(line, header)) {
                return false;
            }

            frame.kind = FrameKind::Message;
            frame.subject = std::move(header.subject);
            frame.replyTo = std::move(header.replyTo);
            frame.payload.resize(header.payloadSize);
            if (header.payloadSize > 0 &&
                !readExact(
                    socket,
                    reinterpret_cast<char*>(frame.payload.data()),
                    header.payloadSize,
                    deadline)) {
                return false;
            }

            char crlf[2]{};
            if (!readExact(socket, crlf, 2, deadline) || crlf[0] != '\r' || crlf[1] != '\n') {
                return false;
            }

            return true;
        }

        frame.kind = FrameKind::Unknown;
        return true;
    }

    std::string normalizeRelaxedJson(std::string jsonSource)
    {
        bool inString = false;
        char previous = '\0';
        for (char& c : jsonSource) {
            if (c == '"' && previous != '\\') {
                inString = !inString;
            }
            if (!inString && c == '=') {
                c = ':';
            }
            previous = c;
        }

        jsonSource = std::regex_replace(
            jsonSource,
            std::regex(R"(([\{\s,])([A-Za-z_][A-Za-z0-9_]*)\s*:)"),
            "$1\"$2\":");
        jsonSource = std::regex_replace(jsonSource, std::regex(R"(,\s*([\}\]]))"), "$1");

        return jsonSource;
    }

    std::optional<Json> parseJsonConfig(const std::string& jsonConfig)
    {
        if (jsonConfig.empty()) {
            return std::nullopt;
        }

        try {
            return Json::parse(normalizeRelaxedJson(jsonConfig));
        }
        catch (...) {
            // Try as file path.
        }

        try {
            const std::filesystem::path sourcePath(jsonConfig);
            if (!std::filesystem::exists(sourcePath)) {
                return std::nullopt;
            }

            std::ifstream file(sourcePath);
            if (!file.good()) {
                return std::nullopt;
            }

            std::stringstream buffer;
            buffer << file.rdbuf();
            return Json::parse(normalizeRelaxedJson(buffer.str()));
        }
        catch (...) {
            return std::nullopt;
        }
    }

    const Json* findNatsNode(const Json& node, bool isRoot = false)
    {
        if (node.is_object()) {
            const auto natsIt = node.find("nats");
            if (natsIt != node.end() && natsIt->is_object()) {
                return &(*natsIt);
            }

            const bool hasDedicatedNatsKeys = node.contains("queue")
                || node.contains("replyQueue")
                || node.contains("user")
                || node.contains("password")
                || node.contains("vhost")
                || node.contains("rpcTimeoutMs");
            const bool hasHostAndPort = node.contains("host") && node.contains("port");
            const bool isPluginChannel = node.contains("type")
                && node["type"].is_string()
                && node["type"].get<std::string>() == "plugin";

            if (hasDedicatedNatsKeys || (hasHostAndPort && (isPluginChannel || isRoot))) {
                return &node;
            }

            for (auto it = node.begin(); it != node.end(); ++it) {
                const Json* natsNode = findNatsNode(*it, false);
                if (natsNode != nullptr) {
                    return natsNode;
                }
            }
            return nullptr;
        }

        if (node.is_array()) {
            for (const auto& item : node) {
                const Json* natsNode = findNatsNode(item, false);
                if (natsNode != nullptr) {
                    return natsNode;
                }
            }
        }

        return nullptr;
    }

    NatsServerConfig currentConfig()
    {
        std::lock_guard<std::mutex> lock(g_serverConfigMutex);
        return g_serverConfig;
    }

    IServer::ProcessMessageFn currentProcessMessage()
    {
        std::lock_guard<std::mutex> lock(g_processMessageMutex);
        return g_processMessage;
    }

    void setProcessMessage(IServer::ProcessMessageFn processMessage)
    {
        std::lock_guard<std::mutex> lock(g_processMessageMutex);
        g_processMessage = processMessage;
    }

    SocketHandle connectSocket(const NatsServerConfig& config)
    {
        addrinfo hints{};
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_protocol = IPPROTO_TCP;

        addrinfo* results = nullptr;
        const std::string portAsString = std::to_string(config.port);
        const int gaiResult = getaddrinfo(config.host.c_str(), portAsString.c_str(), &hints, &results);
        if (gaiResult != 0) {
            return kInvalidSocket;
        }

        SocketHandle socket = kInvalidSocket;
        for (addrinfo* ai = results; ai != nullptr; ai = ai->ai_next) {
            socket = ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
            if (socket == kInvalidSocket) {
                continue;
            }
            if (::connect(socket, ai->ai_addr, static_cast<int>(ai->ai_addrlen)) == 0) {
                break;
            }
            closeSocket(socket);
            socket = kInvalidSocket;
        }

        freeaddrinfo(results);
        return socket;
    }

    std::vector<byte> buildErrorPayload(const std::string& message)
    {
        std::vector<byte> payload;
        payload.reserve(message.size() + 1);
        payload.push_back(static_cast<byte>(255));
        payload.insert(payload.end(), message.begin(), message.end());
        return payload;
    }

    bool publishResponse(SocketHandle socket, const std::string& subject, const std::vector<byte>& response)
    {
        if (subject.empty()) {
            return true;
        }

        const std::string header = "PUB " + subject + " " + std::to_string(response.size()) + "\r\n";
        if (!sendAll(socket, header.data(), header.size())) {
            return false;
        }
        if (!response.empty() &&
            !sendAll(socket, reinterpret_cast<const char*>(response.data()), response.size())) {
            return false;
        }
        return sendAll(socket, "\r\n", 2);
    }

    bool configureSession(SocketHandle socket, const NatsServerConfig& config)
    {
        Json connectOptions{
            {"verbose", false},
            {"pedantic", false},
            {"tls_required", false},
            {"lang", "cpp"},
            {"version", "1.0.0"},
            {"protocol", 1}
        };
        if (!config.user.empty()) {
            connectOptions["user"] = config.user;
        }
        if (!config.password.empty()) {
            connectOptions["pass"] = config.password;
        }

        const std::string connectLine = "CONNECT " + connectOptions.dump() + "\r\n";
        const std::string subLine = "SUB " + config.queue + " 1\r\n";
        return sendAll(socket, connectLine.data(), connectLine.size()) &&
            sendAll(socket, subLine.data(), subLine.size()) &&
            sendAll(socket, "PING\r\n", 6);
    }

    bool processMessageFrame(SocketHandle socket, const NatsFrame& frame)
    {
        const auto processMessage = currentProcessMessage();
        if (processMessage == nullptr) {
            return false;
        }

        std::vector<byte> response;
        auto writeResponse = [](void* context, const byte* data, std::size_t size) {
            auto* out = static_cast<std::vector<byte>*>(context);
            if (out == nullptr) {
                return;
            }
            if (data == nullptr || size == 0) {
                out->clear();
                return;
            }
            out->assign(data, data + size);
        };

        if (!processMessage(
            frame.payload.empty() ? nullptr : frame.payload.data(),
            frame.payload.size(),
            writeResponse,
            &response)) {
            response = buildErrorPayload("NATS consumer: processMessage callback returned failure");
        }

        return publishResponse(socket, frame.replyTo, response);
    }
}

void NatsServer::configure(const std::string& jsonConfig, ProcessMessageFn processMessage)
{
    if (processMessage == nullptr) {
        throw std::runtime_error("NATS consumer: processMessage callback is required");
    }

    const std::optional<Json> parsedConfig = parseJsonConfig(jsonConfig);
    if (!parsedConfig.has_value() || !parsedConfig->is_object()) {
        throw std::runtime_error("NATS consumer: configuration must be a JSON object");
    }

    NatsServerConfig config;
    const Json* parsedNode = findNatsNode(parsedConfig.value(), true);
    if (parsedNode == nullptr || !parsedNode->is_object()) {
        throw std::runtime_error("NATS consumer: could not find NATS configuration node");
    }
    const Json& parsed = *parsedNode;

    const auto nameIt = parsed.find("name");
    if (nameIt != parsed.end() && nameIt->is_string()) {
        config.name = nameIt->get<std::string>();
    }

    const auto hostIt = parsed.find("host");
    if (hostIt != parsed.end() && hostIt->is_string()) {
        config.host = hostIt->get<std::string>();
    }

    const auto queueIt = parsed.find("queue");
    if (queueIt != parsed.end() && queueIt->is_string()) {
        config.queue = queueIt->get<std::string>();
    }

    const auto replyQueueIt = parsed.find("replyQueue");
    if (replyQueueIt != parsed.end() && replyQueueIt->is_string()) {
        config.replyQueue = replyQueueIt->get<std::string>();
    }

    const auto userIt = parsed.find("user");
    if (userIt != parsed.end() && userIt->is_string()) {
        config.user = userIt->get<std::string>();
    }

    const auto passwordIt = parsed.find("password");
    if (passwordIt != parsed.end() && passwordIt->is_string()) {
        config.password = passwordIt->get<std::string>();
    }

    const auto vhostIt = parsed.find("vhost");
    if (vhostIt != parsed.end() && vhostIt->is_string()) {
        config.vhost = vhostIt->get<std::string>();
    }

    const auto portIt = parsed.find("port");
    if (portIt != parsed.end() && portIt->is_number_integer()) {
        const auto parsedPort = portIt->get<int64_t>();
        if (parsedPort <= 0 || parsedPort > std::numeric_limits<std::uint16_t>::max()) {
            throw std::runtime_error("NATS consumer: invalid port in configuration");
        }
        config.port = static_cast<std::uint16_t>(parsedPort);
    }

    const auto timeoutIt = parsed.find("rpcTimeoutMs");
    if (timeoutIt != parsed.end() && timeoutIt->is_number_integer()) {
        const auto parsedTimeout = timeoutIt->get<int64_t>();
        if (parsedTimeout <= 0 || parsedTimeout > std::numeric_limits<std::uint32_t>::max()) {
            throw std::runtime_error("NATS consumer: invalid rpcTimeoutMs in configuration");
        }
        config.rpcTimeoutMs = static_cast<std::uint32_t>(parsedTimeout);
    }

    if (config.queue.empty()) {
        throw std::runtime_error("NATS consumer: missing required field 'queue'");
    }

    {
        std::lock_guard<std::mutex> lock(g_serverConfigMutex);
        g_serverConfig = std::move(config);
    }
    setProcessMessage(processMessage);
}

void NatsServer::start()
{
    bool wsaInitialized = false;
    try {
        g_stopRequested.store(false, std::memory_order_release);
        const NatsServerConfig config = currentConfig();

#ifdef _WIN32
        WSAData wsaData{};
        if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
            throw std::runtime_error("NATS consumer: WSAStartup failed");
        }
        wsaInitialized = true;
#endif

        logInfo(
            "NATS consumer '" + config.name + "' is enabled. Subject: " + config.queue +
            " | Broker: " + config.host + ":" + std::to_string(config.port));

        while (!shouldStop()) {
            SocketHandle socket = connectSocket(config);
            if (socket == kInvalidSocket) {
                if (!shouldStop()) {
                    logWarn("NATS consumer failed to connect, retrying in 2 seconds");
                    std::this_thread::sleep_for(std::chrono::seconds(2));
                }
                continue;
            }

            setActiveSocket(socket);
            if (!configureSession(socket, config)) {
                clearActiveSocket(socket);
                closeSocket(socket);
                if (!shouldStop()) {
                    logWarn("NATS consumer failed to initialize session, retrying in 2 seconds");
                    std::this_thread::sleep_for(std::chrono::seconds(2));
                }
                continue;
            }

            bool sessionReady = false;
            const auto handshakeDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
            while (!shouldStop() && std::chrono::steady_clock::now() < handshakeDeadline) {
                const WaitStatus status = waitReadable(socket, 500);
                if (status == WaitStatus::Timeout) {
                    continue;
                }
                if (status == WaitStatus::Error) {
                    break;
                }

                NatsFrame frame;
                if (!readFrame(socket, frame, handshakeDeadline)) {
                    break;
                }

                if (frame.kind == FrameKind::Ping) {
                    if (!sendAll(socket, "PONG\r\n", 6)) {
                        break;
                    }
                    continue;
                }
                if (frame.kind == FrameKind::Error) {
                    break;
                }
                if (frame.kind == FrameKind::Pong) {
                    sessionReady = true;
                    break;
                }
            }

            if (!sessionReady) {
                clearActiveSocket(socket);
                closeSocket(socket);
                if (!shouldStop()) {
                    logWarn("NATS consumer handshake timed out, reconnecting");
                }
                continue;
            }

            logInfo("NATS consumer connected and listening");

            bool reconnectNeeded = false;
            while (!shouldStop()) {
                const WaitStatus status = waitReadable(socket, 1000);
                if (status == WaitStatus::Timeout) {
                    continue;
                }
                if (status == WaitStatus::Error) {
                    reconnectNeeded = true;
                    break;
                }

                NatsFrame frame;
                const auto frameDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
                if (!readFrame(socket, frame, frameDeadline)) {
                    reconnectNeeded = true;
                    break;
                }

                if (frame.kind == FrameKind::Ping) {
                    if (!sendAll(socket, "PONG\r\n", 6)) {
                        reconnectNeeded = true;
                        break;
                    }
                    continue;
                }
                if (frame.kind == FrameKind::Error) {
                    reconnectNeeded = true;
                    break;
                }
                if (frame.kind != FrameKind::Message) {
                    continue;
                }

                if (!processMessageFrame(socket, frame)) {
                    reconnectNeeded = true;
                    break;
                }
            }

            clearActiveSocket(socket);
            closeSocket(socket);

            if (!shouldStop() && reconnectNeeded) {
                logWarn("NATS consumer disconnected, retrying in 2 seconds");
                std::this_thread::sleep_for(std::chrono::seconds(2));
            }
        }

        closeActiveSocket();
        if (wsaInitialized) {
#ifdef _WIN32
            WSACleanup();
#endif
            wsaInitialized = false;
        }
        logInfo("NATS consumer stopped");
    }
    catch (const std::exception& ex) {
        closeActiveSocket();
        if (wsaInitialized) {
#ifdef _WIN32
            WSACleanup();
#endif
            wsaInitialized = false;
        }
        logWarn(std::string("NATS consumer error: ") + ex.what());
    }
    catch (...) {
        closeActiveSocket();
        if (wsaInitialized) {
#ifdef _WIN32
            WSACleanup();
#endif
            wsaInitialized = false;
        }
        logWarn("NATS consumer stopped with unknown error");
    }
}

void NatsServer::stop()
{
    g_stopRequested.store(true, std::memory_order_release);
    closeActiveSocket();
}

#if defined(_WIN32)
#define NATS_PLUGIN_EXPORT extern "C" __declspec(dllexport)
#else
#define NATS_PLUGIN_EXPORT extern "C"
#endif

NATS_PLUGIN_EXPORT GraftcodeGateway::IServer* CreateServer()
{
    return new NatsServer();
}

NATS_PLUGIN_EXPORT void DestroyServer(GraftcodeGateway::IServer* server)
{
    delete server;
}
