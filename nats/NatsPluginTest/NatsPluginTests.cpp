#include <gtest/gtest.h>

#include "ITransport.h"
#include "NatsClient.h"
#include "NatsServer.h"
#include "TransportNats.h"

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#endif

#include <chrono>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

extern "C" Hypertube::Native::Interfaces::ITransport* CreateTransportChannel(
    const char* ipAddress,
    unsigned short port,
    const char* configSource);
extern "C" void DestroyTransportChannel(Hypertube::Native::Interfaces::ITransport* transport);

namespace
{
    using Graftcode::Plugins::Nats::NatsClient;
    using Graftcode::Plugins::Nats::NatsClientConfig;
    using Graftcode::Plugins::Nats::NatsServer;
    using Graftcode::Plugins::Nats::TransportNats;

    const char* kValidConfig = R"({
        "name": "NatsPlugin",
        "host": "127.0.0.1",
        "port": 4222,
        "queue": "gg",
        "replyQueue": "gg.reply",
        "user": "guest",
        "password": "guest",
        "vhost": "default",
        "rpcTimeoutMs": 30000
    })";

    bool EchoProcessMessage(
        const byte* requestData,
        std::size_t requestSize,
        GraftcodeGateway::IServer::WriteResponseFn writeResponse,
        void* writeContext)
    {
        writeResponse(writeContext, requestData, requestSize);
        return true;
    }

#ifdef _WIN32
    using SocketHandle = SOCKET;
    constexpr SocketHandle kInvalidSocket = INVALID_SOCKET;
#else
    using SocketHandle = int;
    constexpr SocketHandle kInvalidSocket = -1;
#endif

    void CloseSocket(SocketHandle socket)
    {
        if (socket == kInvalidSocket) {
            return;
        }
#ifdef _WIN32
        closesocket(socket);
#else
        ::close(socket);
#endif
    }

    bool IsNatsBrokerReachable(const std::string& host, unsigned short port)
    {
#ifdef _WIN32
        WSAData wsaData{};
        if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
            return false;
        }
#endif

        addrinfo hints{};
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_protocol = IPPROTO_TCP;

        addrinfo* results = nullptr;
        const std::string portAsString = std::to_string(port);
        const int gaiResult = getaddrinfo(host.c_str(), portAsString.c_str(), &hints, &results);
        if (gaiResult != 0) {
#ifdef _WIN32
            WSACleanup();
#endif
            return false;
        }

        bool connected = false;
        for (addrinfo* ai = results; ai != nullptr; ai = ai->ai_next) {
            const SocketHandle socket = ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
            if (socket == kInvalidSocket) {
                continue;
            }

            if (::connect(socket, ai->ai_addr, static_cast<int>(ai->ai_addrlen)) == 0) {
                connected = true;
                CloseSocket(socket);
                break;
            }

            CloseSocket(socket);
        }

        freeaddrinfo(results);

#ifdef _WIN32
        WSACleanup();
#endif

        return connected;
    }

    std::string BuildConfigJson(const std::string& queue, const std::string& replyQueue)
    {
        std::ostringstream out;
        out << R"({
            "name": "NatsPlugin",
            "host": "127.0.0.1",
            "port": 4222,
            "queue": ")" << queue << R"(",
            "replyQueue": ")" << replyQueue << R"(",
            "user": "guest",
            "password": "guest",
            "vhost": "default",
            "rpcTimeoutMs": 3000
        })";
        return out.str();
    }

    struct ServerThreadGuard
    {
        NatsServer& server;
        std::thread& thread;

        ~ServerThreadGuard()
        {
            server.stop();
            if (thread.joinable()) {
                thread.join();
            }
        }
    };
}

TEST(NatsTransport, ExportedFactoryCreatesAndDestroysTransport)
{
    auto* transport = CreateTransportChannel("127.0.0.1", 4222, kValidConfig);
    ASSERT_NE(transport, nullptr);
    EXPECT_EQ(transport->Initialize(1, 2, 3), 0);
    DestroyTransportChannel(transport);
}

TEST(NatsTransport, SendCommandRejectsInvalidPayloadArguments)
{
    TransportNats transport("127.0.0.1", 4222, kValidConfig);
    std::vector<byte> payload{ 0x01 };

    EXPECT_THROW(transport.SendCommand(nullptr, 0), std::runtime_error);
    EXPECT_THROW(transport.SendCommand(payload.data(), -1), std::runtime_error);
}

TEST(NatsTransport, ReadResponseRejectsInvalidBufferArguments)
{
    TransportNats transport("127.0.0.1", 4222, kValidConfig);
    std::vector<byte> buffer{ 0x00 };

    EXPECT_THROW(transport.ReadResponse(nullptr, 0), std::runtime_error);
    EXPECT_THROW(transport.ReadResponse(buffer.data(), -1), std::runtime_error);
}

TEST(NatsTransport, ReadResponseFailsWhenNoResponseWasStored)
{
    TransportNats transport("127.0.0.1", 4222, kValidConfig);
    std::vector<byte> buffer{ 0x00 };

    EXPECT_THROW(transport.ReadResponse(buffer.data(), 1), std::runtime_error);
}

TEST(NatsClient, SendRpcFailsFastWhenQueueIsMissing)
{
    NatsClientConfig config;
    config.host = "127.0.0.1";
    config.port = 4222;
    config.rpcTimeoutMs = 5;

    std::vector<byte> response;
    EXPECT_FALSE(NatsClient::SendRpc(config, nullptr, 0, response));
}

TEST(NatsServer, ConfigureRejectsNullProcessMessageCallback)
{
    NatsServer server;
    EXPECT_THROW(server.configure(R"({"queue":"gg"})", nullptr), std::runtime_error);
}

TEST(NatsServer, ConfigureRejectsMissingQueue)
{
    NatsServer server;
    EXPECT_THROW(
        server.configure(R"({"host":"127.0.0.1","port":4222})", EchoProcessMessage),
        std::runtime_error);
}

TEST(NatsServer, ConfigureAcceptsNestedNatsConfigurationNode)
{
    NatsServer server;
    EXPECT_NO_THROW(server.configure(
        R"({
            "transport": {
                "nats": {
                    "name": "NatsPlugin",
                    "host": "127.0.0.1",
                    "port": 4222,
                    "queue": "gg",
                    "replyQueue": "gg.reply",
                    "user": "guest",
                    "password": "guest",
                    "rpcTimeoutMs": 30000
                }
            }
        })",
        EchoProcessMessage));
}

TEST(NatsIntegration, RealCommunicationRoundTripWithLocalBroker)
{
    constexpr unsigned short kNatsPort = 4222;
    if (!IsNatsBrokerReachable("127.0.0.1", kNatsPort)) {
        GTEST_SKIP() << "NATS broker is not reachable on 127.0.0.1:4222";
    }

    const auto uniqueSuffix = std::to_string(
        static_cast<unsigned long long>(std::chrono::steady_clock::now().time_since_epoch().count()));
    const std::string queue = "gg.integration." + uniqueSuffix;
    const std::string replyQueue = "gg.reply.integration." + uniqueSuffix;
    const std::string config = BuildConfigJson(queue, replyQueue);

    NatsServer server;
    ASSERT_NO_THROW(server.configure(config, EchoProcessMessage));

    std::thread serverThread([&server]() {
        server.start();
    });
    ServerThreadGuard guard{ server, serverThread };

    TransportNats transport("127.0.0.1", kNatsPort, config);
    const std::vector<byte> payload{
        0x03, 0x00, 0x00, 0x7f, 0x00, 0x00, 0x01, 0x28, 0x16, 0x08, 0x06, 0x00,
        0x01, 0x00, 0x00, 0x00, 0x03, 0x00, 0x01, 0x01, 0x13, 0x00, 0x00, 0x00
    };

    std::vector<byte> responsePayload;
    bool roundTripSucceeded = false;
    for (int attempt = 0; attempt < 15 && !roundTripSucceeded; ++attempt) {
        try {
            const int responseSize = transport.SendCommand(
                const_cast<byte*>(payload.data()),
                static_cast<int32_t>(payload.size()));
            if (responseSize <= 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(150));
                continue;
            }

            responsePayload.resize(static_cast<std::size_t>(responseSize));
            EXPECT_EQ(
                transport.ReadResponse(responsePayload.data(), responseSize),
                0);
            roundTripSucceeded = true;
        }
        catch (const std::exception&) {
            std::this_thread::sleep_for(std::chrono::milliseconds(150));
        }
    }

    ASSERT_TRUE(roundTripSucceeded) << "Failed to complete NATS round trip.";
    EXPECT_EQ(responsePayload, payload);
}
