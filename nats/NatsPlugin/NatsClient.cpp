#include "NatsClient.h"

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

#include <algorithm>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <optional>
#include <regex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#ifdef max
#undef max
#endif
#ifdef min
#undef min
#endif

using namespace Graftcode::Plugins::Nats;

namespace
{
    using Json = nlohmann::json;

#ifdef _WIN32
    using SocketHandle = SOCKET;
    constexpr SocketHandle kInvalidSocket = INVALID_SOCKET;
#else
    using SocketHandle = int;
    constexpr SocketHandle kInvalidSocket = -1;
#endif

    constexpr std::chrono::milliseconds kDefaultRpcTimeout(30000);

    struct SocketRuntime
    {
#ifdef _WIN32
        bool initialized{ false };
        SocketRuntime()
        {
            WSAData wsaData{};
            initialized = (WSAStartup(MAKEWORD(2, 2), &wsaData) == 0);
        }
        ~SocketRuntime()
        {
            if (initialized) {
                WSACleanup();
            }
        }
#endif
        bool ready() const
        {
#ifdef _WIN32
            return initialized;
#else
            return true;
#endif
        }
    };

    void CloseSocket(SocketHandle socket)
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

    SocketHandle ConnectSocket(const NatsClientConfig& config)
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
            CloseSocket(socket);
            socket = kInvalidSocket;
        }

        freeaddrinfo(results);
        return socket;
    }

    bool WaitReadable(SocketHandle socket, int timeoutMs)
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
        return result > 0 && FD_ISSET(socket, &readSet);
    }

    bool SendAll(SocketHandle socket, const char* buffer, std::size_t size)
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

    bool ReadExact(SocketHandle socket, char* outBuffer, std::size_t size, const std::chrono::steady_clock::time_point deadline)
    {
        std::size_t readTotal = 0;
        while (readTotal < size) {
            const auto now = std::chrono::steady_clock::now();
            if (now >= deadline) {
                return false;
            }
            const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
            if (!WaitReadable(socket, static_cast<int>(remaining.count()))) {
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

    bool ReadLine(SocketHandle socket, std::string& line, const std::chrono::steady_clock::time_point deadline)
    {
        line.clear();
        while (true) {
            const auto now = std::chrono::steady_clock::now();
            if (now >= deadline) {
                return false;
            }
            const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
            if (!WaitReadable(socket, static_cast<int>(remaining.count()))) {
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

    struct NatsMsgHeader
    {
        std::string subject;
        std::string sid;
        std::string replyTo;
        std::size_t payloadSize{ 0 };
    };

    bool ParseMsgHeader(const std::string& headerLine, NatsMsgHeader& header)
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

        const std::string& payloadToken = tokens.back();
        try {
            const std::uint64_t parsedSize = std::stoull(payloadToken);
            header.payloadSize = static_cast<std::size_t>(parsedSize);
        }
        catch (...) {
            return false;
        }

        return true;
    }

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

    bool ReadFrame(SocketHandle socket, NatsFrame& frame, const std::chrono::steady_clock::time_point deadline)
    {
        frame = NatsFrame{};

        std::string line;
        if (!ReadLine(socket, line, deadline)) {
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
            if (!ParseMsgHeader(line, header)) {
                return false;
            }

            frame.kind = FrameKind::Message;
            frame.subject = std::move(header.subject);
            frame.replyTo = std::move(header.replyTo);
            frame.payload.resize(header.payloadSize);
            if (header.payloadSize > 0 && !ReadExact(
                socket,
                reinterpret_cast<char*>(frame.payload.data()),
                header.payloadSize,
                deadline)) {
                return false;
            }

            char crlf[2]{};
            if (!ReadExact(socket, crlf, 2, deadline) || crlf[0] != '\r' || crlf[1] != '\n') {
                return false;
            }
            return true;
        }

        frame.kind = FrameKind::Unknown;
        return true;
    }

    std::string BuildInboxSuffix()
    {
        const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
        const auto tidHash = std::hash<std::thread::id>{}(std::this_thread::get_id());
        return std::to_string(tidHash) + "." + std::to_string(now);
    }

    std::string NormalizeRelaxedJson(std::string jsonSource)
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

    std::optional<Json> ParseJsonConfigSource(const std::string& configSource)
    {
        if (configSource.empty()) {
            return std::nullopt;
        }

        try {
            return Json::parse(NormalizeRelaxedJson(configSource));
        }
        catch (...) {
            // Not inline JSON; continue with path parsing.
        }

        try {
            const std::filesystem::path sourcePath(configSource);
            if (!std::filesystem::exists(sourcePath)) {
                return std::nullopt;
            }

            std::ifstream file(sourcePath);
            if (!file.good()) {
                return std::nullopt;
            }

            std::stringstream buffer;
            buffer << file.rdbuf();
            return Json::parse(NormalizeRelaxedJson(buffer.str()));
        }
        catch (...) {
            return std::nullopt;
        }
    }

    const Json* FindNatsNode(const Json& node, bool isRoot = false)
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
                const Json* natsNode = FindNatsNode(*it, false);
                if (natsNode != nullptr) {
                    return natsNode;
                }
            }
            return nullptr;
        }

        if (node.is_array()) {
            for (const auto& item : node) {
                const Json* natsNode = FindNatsNode(item, false);
                if (natsNode != nullptr) {
                    return natsNode;
                }
            }
        }

        return nullptr;
    }

    void ApplyNatsConfig(const Json& natsNode, NatsClientConfig& config)
    {
        if (!natsNode.is_object()) {
            return;
        }

        const auto hostIt = natsNode.find("host");
        if (hostIt != natsNode.end() && hostIt->is_string()) {
            config.host = hostIt->get<std::string>();
        }

        const auto portIt = natsNode.find("port");
        if (portIt != natsNode.end() && portIt->is_number_integer()) {
            const auto parsedPort = portIt->get<int64_t>();
            if (parsedPort > 0 && parsedPort <= std::numeric_limits<std::uint16_t>::max()) {
                config.port = static_cast<std::uint16_t>(parsedPort);
            }
        }

        const auto queueIt = natsNode.find("queue");
        if (queueIt != natsNode.end() && queueIt->is_string()) {
            config.queue = queueIt->get<std::string>();
        }

        const auto replyQueueIt = natsNode.find("replyQueue");
        if (replyQueueIt != natsNode.end() && replyQueueIt->is_string()) {
            config.replyQueue = replyQueueIt->get<std::string>();
        }

        const auto userIt = natsNode.find("user");
        if (userIt != natsNode.end() && userIt->is_string()) {
            config.user = userIt->get<std::string>();
        }

        const auto passwordIt = natsNode.find("password");
        if (passwordIt != natsNode.end() && passwordIt->is_string()) {
            config.password = passwordIt->get<std::string>();
        }

        const auto vhostIt = natsNode.find("vhost");
        if (vhostIt != natsNode.end() && vhostIt->is_string()) {
            config.vhost = vhostIt->get<std::string>();
        }

        const auto timeoutIt = natsNode.find("rpcTimeoutMs");
        if (timeoutIt != natsNode.end() && timeoutIt->is_number_integer()) {
            const auto parsedTimeout = timeoutIt->get<int64_t>();
            if (parsedTimeout > 0 && parsedTimeout <= std::numeric_limits<std::uint32_t>::max()) {
                config.rpcTimeoutMs = static_cast<std::uint32_t>(parsedTimeout);
            }
        }
    }
}

void NatsClient::ApplyConfigSource(const std::string& configSource, NatsClientConfig& config)
{
    const std::optional<Json> parsedSource = ParseJsonConfigSource(configSource);
    if (!parsedSource.has_value()) {
        return;
    }

    const Json* natsNode = FindNatsNode(parsedSource.value(), true);
    if (natsNode == nullptr) {
        return;
    }

    ApplyNatsConfig(*natsNode, config);
}

bool NatsClient::Send(const NatsClientConfig& config, const std::vector<byte>& payload)
{
    std::vector<byte> responsePayload;
    return SendRpc(config, payload, responsePayload);
}

bool NatsClient::Send(const NatsClientConfig& config, const byte* payload, std::size_t payloadSize)
{
    std::vector<byte> responsePayload;
    return SendRpc(config, payload, payloadSize, responsePayload);
}

bool NatsClient::SendRpc(
    const NatsClientConfig& config,
    const std::vector<byte>& payload,
    std::vector<byte>& responsePayload)
{
    const byte* data = payload.empty() ? nullptr : payload.data();
    return SendRpc(config, data, payload.size(), responsePayload);
}

bool NatsClient::SendRpc(
    const NatsClientConfig& config,
    const byte* payload,
    std::size_t payloadSize,
    std::vector<byte>& responsePayload)
{
    responsePayload.clear();

    NatsClientConfig effectiveConfig = config;
    if (effectiveConfig.host.empty()) {
        effectiveConfig.host = "127.0.0.1";
    }
    if (effectiveConfig.port == 0) {
        effectiveConfig.port = 4222;
    }
    if (effectiveConfig.queue.empty()) {
        return false;
    }
    if (payloadSize > 0 && payload == nullptr) {
        return false;
    }

    const std::chrono::milliseconds rpcTimeout =
        (effectiveConfig.rpcTimeoutMs == 0)
        ? kDefaultRpcTimeout
        : std::chrono::milliseconds(effectiveConfig.rpcTimeoutMs);
    const auto deadline = std::chrono::steady_clock::now() + rpcTimeout;

    SocketRuntime runtime;
    if (!runtime.ready()) {
        return false;
    }

    SocketHandle socket = ConnectSocket(effectiveConfig);
    if (socket == kInvalidSocket) {
        return false;
    }

    const auto cleanup = [&]() {
        CloseSocket(socket);
        socket = kInvalidSocket;
    };

    Json connectOptions{
        {"verbose", false},
        {"pedantic", false},
        {"tls_required", false},
        {"lang", "cpp"},
        {"version", "1.0.0"},
        {"protocol", 1}
    };
    if (!effectiveConfig.user.empty()) {
        connectOptions["user"] = effectiveConfig.user;
    }
    if (!effectiveConfig.password.empty()) {
        connectOptions["pass"] = effectiveConfig.password;
    }

    const std::string connectLine = "CONNECT " + connectOptions.dump() + "\r\n";
    if (!SendAll(socket, connectLine.data(), connectLine.size()) ||
        !SendAll(socket, "PING\r\n", 6)) {
        cleanup();
        return false;
    }

    bool connectionReady = false;
    while (std::chrono::steady_clock::now() < deadline) {
        NatsFrame frame;
        if (!ReadFrame(socket, frame, deadline)) {
            cleanup();
            return false;
        }

        if (frame.kind == FrameKind::Ping) {
            if (!SendAll(socket, "PONG\r\n", 6)) {
                cleanup();
                return false;
            }
            continue;
        }
        if (frame.kind == FrameKind::Pong) {
            connectionReady = true;
            break;
        }
        if (frame.kind == FrameKind::Error) {
            cleanup();
            return false;
        }
    }
    if (!connectionReady) {
        cleanup();
        return false;
    }

    const std::string replyBase = effectiveConfig.replyQueue.empty()
        ? "_INBOX.graftcode"
        : effectiveConfig.replyQueue;
    const std::string replySubject = replyBase + "." + BuildInboxSuffix();
    const std::string sid = "1";

    const std::string subCommand = "SUB " + replySubject + " " + sid + "\r\n";
    const std::string unsubCommand = "UNSUB " + sid + " 1\r\n";
    if (!SendAll(socket, subCommand.data(), subCommand.size()) ||
        !SendAll(socket, unsubCommand.data(), unsubCommand.size())) {
        cleanup();
        return false;
    }

    const std::string pubHeader =
        "PUB " + effectiveConfig.queue + " " + replySubject + " " + std::to_string(payloadSize) + "\r\n";
    if (!SendAll(socket, pubHeader.data(), pubHeader.size())) {
        cleanup();
        return false;
    }
    if (payloadSize > 0 && !SendAll(socket, reinterpret_cast<const char*>(payload), payloadSize)) {
        cleanup();
        return false;
    }
    if (!SendAll(socket, "\r\n", 2) || !SendAll(socket, "PING\r\n", 6)) {
        cleanup();
        return false;
    }

    while (std::chrono::steady_clock::now() < deadline) {
        NatsFrame frame;
        if (!ReadFrame(socket, frame, deadline)) {
            cleanup();
            return false;
        }

        if (frame.kind == FrameKind::Ping) {
            if (!SendAll(socket, "PONG\r\n", 6)) {
                cleanup();
                return false;
            }
            continue;
        }
        if (frame.kind == FrameKind::Error) {
            cleanup();
            return false;
        }
        if (frame.kind == FrameKind::Message && frame.subject == replySubject) {
            responsePayload = std::move(frame.payload);
            cleanup();
            return true;
        }
    }

    cleanup();
    return false;
}
