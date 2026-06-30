#include <gtest/gtest.h>

#include "ITransport.h"
#include "IServer.h"
#include "ServiceBusClient.h"
#include "ServiceBusServer.h"
#include "TransportServiceBus.h"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

extern "C" Hypertube::Native::Interfaces::ITransport* CreateTransportChannel(
	const char* ipAddress,
	unsigned short port,
	const char* configSource);
extern "C" void DestroyTransportChannel(Hypertube::Native::Interfaces::ITransport* transport);

using Graftcode::Plugins::ServiceBus::ServiceBusClient;
using Graftcode::Plugins::ServiceBus::ServiceBusClientConfig;
using Graftcode::Plugins::ServiceBus::ServiceBusServer;

namespace {
	const char* kConnectionStringConfig = R"({
			"connectionString": "Endpoint=sb://example.servicebus.windows.net/;SharedAccessKeyName=root;SharedAccessKey=secret",
			"queue": "gg",
			"replyQueue": "gg.reply",
			"rpcTimeoutMs": 30000
})";

	const char* kComponentsConfig = R"({
			"host": "example.servicebus.windows.net",
			"sharedAccessKeyName": "root",
			"sharedAccessKey": "secret",
			"queue": "gg",
			"replyQueue": "gg.reply"
})";

	const char* kTopicConfig = R"({
			"connectionString": "Endpoint=sb://example.servicebus.windows.net/;SharedAccessKeyName=root;SharedAccessKey=secret",
			"topic": "gg.topic"
})";

	const std::vector<byte> kSamplePayload{
		0x03, 0x00, 0x00, 0x7f, 0x00, 0x00, 0x01, 0x28, 0x16, 0x08, 0x06, 0x00,
		0x01, 0x00, 0x00, 0x00, 0x03, 0x00, 0x01, 0x01, 0x13, 0x00, 0x00, 0x00,
		0x54, 0x65, 0x73, 0x74, 0x43, 0x6c, 0x61, 0x73, 0x73, 0x2e, 0x54, 0x65,
		0x73, 0x74, 0x43, 0x6c, 0x61, 0x73, 0x73
	};

	// State captured by the one-way server callback (function-pointer signature
	// means we have to go through file-scope state).
	std::atomic_bool g_oneWayReceived{ false };
	std::mutex g_oneWayMutex;
	std::vector<byte> g_oneWayPayload;

	// RPC server callback: echo the request straight back as the response.
	bool EchoCallback(
		const byte* data,
		std::size_t size,
		GraftcodeGateway::IServer::WriteResponseFn write,
		void* ctx) {
		write(ctx, data, size);
		return true;
	}

	// One-way server callback: record the payload, never write a response.
	bool RecordOneWayCallback(
		const byte* data,
		std::size_t size,
		GraftcodeGateway::IServer::WriteResponseFn /*write*/,
		void* /*ctx*/) {
		{
			std::lock_guard<std::mutex> lock(g_oneWayMutex);
			g_oneWayPayload.assign(data, data + size);
		}
		g_oneWayReceived.store(true);
		return true;
	}

	std::string envOr(const char* name, const char* fallback) {
		const char* value = std::getenv(name);
		return value != nullptr ? std::string(value) : std::string(fallback);
	}

	std::string jsonEscape(const std::string& value) {
		std::string out;
		out.reserve(value.size() + 8);
		for (const char c : value) {
			if (c == '\\' || c == '"') {
				out.push_back('\\');
			}
			out.push_back(c);
		}
		return out;
	}
}

TEST(ServiceBusClient, AppliesConnectionStringFromConfigSource) {
	ServiceBusClientConfig config;
	ServiceBusClient::ApplyConfigSource(kConnectionStringConfig, config);

	EXPECT_EQ(config.queue, "gg");
	EXPECT_EQ(config.replyQueue, "gg.reply");
	EXPECT_EQ(config.rpcTimeoutMs, 30000u);
	EXPECT_FALSE(config.connectionString.empty());
	EXPECT_EQ(ServiceBusClient::BuildConnectionString(config), config.connectionString);
}

TEST(ServiceBusClient, BuildsConnectionStringFromComponents) {
	ServiceBusClientConfig config;
	ServiceBusClient::ApplyConfigSource(kComponentsConfig, config);

	const std::string connectionString = ServiceBusClient::BuildConnectionString(config);
	EXPECT_NE(connectionString.find("Endpoint=sb://example.servicebus.windows.net/"), std::string::npos);
	EXPECT_NE(connectionString.find("SharedAccessKeyName=root"), std::string::npos);
	EXPECT_NE(connectionString.find("SharedAccessKey=secret"), std::string::npos);
}

TEST(ServiceBusClient, SendRpcFailsFastWithoutEntities) {
	ServiceBusClientConfig config;
	config.connectionString =
		"Endpoint=sb://example.servicebus.windows.net/;SharedAccessKeyName=root;SharedAccessKey=secret";
	std::vector<byte> response;
	EXPECT_FALSE(ServiceBusClient::SendRpc(config, kSamplePayload, response));
}

TEST(ServiceBusClient, AppliesTopicFromConfigSource) {
	ServiceBusClientConfig config;
	ServiceBusClient::ApplyConfigSource(kTopicConfig, config);

	EXPECT_EQ(config.topic, "gg.topic");
	EXPECT_TRUE(config.queue.empty());
	EXPECT_FALSE(config.connectionString.empty());
}

TEST(ServiceBusClient, PublishOneWayFailsFastWithoutTopic) {
	ServiceBusClientConfig config;
	config.connectionString =
		"Endpoint=sb://example.servicebus.windows.net/;SharedAccessKeyName=root;SharedAccessKey=secret";
	EXPECT_FALSE(ServiceBusClient::PublishOneWay(config, kSamplePayload));
}

TEST(ServiceBusTransport, FactoryCreatesAndDestroysTransport) {
	auto* transport = CreateTransportChannel("", 0, kConnectionStringConfig);
	ASSERT_NE(transport, nullptr);
	DestroyTransportChannel(transport);
}

// Self-contained RPC round-trip against a live Service Bus namespace: starts the
// gateway server (echo handler) in a background thread, then issues a client RPC.
// Requires SERVICEBUS_CONNECTION_STRING and a SESSION-ENABLED reply queue.
// Optional: SERVICEBUS_QUEUE, SERVICEBUS_REPLY_QUEUE. Skipped when unset.
TEST(ServiceBusLive, RpcRoundTrip) {
	const char* connectionString = std::getenv("SERVICEBUS_CONNECTION_STRING");
	if (connectionString == nullptr) {
		GTEST_SKIP() << "SERVICEBUS_CONNECTION_STRING not set; skipping live RPC round-trip test.";
	}

	const std::string queue = envOr("SERVICEBUS_QUEUE", "queue-01");
	const std::string replyQueue = envOr("SERVICEBUS_REPLY_QUEUE", "graftcode-rpc-reply-session");

	const std::string serverConfig =
		std::string("{\"name\":\"live-rpc\",\"connectionString\":\"") + jsonEscape(connectionString)
		+ "\",\"queue\":\"" + jsonEscape(queue)
		+ "\",\"replyQueue\":\"" + jsonEscape(replyQueue) + "\"}";

	auto server = std::make_unique<ServiceBusServer>();
	server->configure(serverConfig, &EchoCallback);
	std::thread serverThread([&server]() { server->start(); });
	std::this_thread::sleep_for(std::chrono::seconds(3));

	ServiceBusClientConfig config;
	config.connectionString = connectionString;
	config.queue = queue;
	config.replyQueue = replyQueue;
	config.rpcTimeoutMs = 20000;

	std::vector<byte> response;
	const bool sent = ServiceBusClient::SendRpc(config, kSamplePayload, response);

	server->stop();
	serverThread.join();

	EXPECT_TRUE(sent);
	EXPECT_EQ(response, kSamplePayload);
}

// Self-contained one-way (topic/subscription) round-trip against a live namespace:
// the server consumes from the subscription and never replies; the client publishes
// fire-and-forget. Requires SERVICEBUS_CONNECTION_STRING. Optional: SERVICEBUS_TOPIC,
// SERVICEBUS_SUBSCRIPTION. Skipped when unset.
TEST(ServiceBusLive, OneWayTopicRoundTrip) {
	const char* connectionString = std::getenv("SERVICEBUS_CONNECTION_STRING");
	if (connectionString == nullptr) {
		GTEST_SKIP() << "SERVICEBUS_CONNECTION_STRING not set; skipping live one-way topic test.";
	}

	const std::string topic = envOr("SERVICEBUS_TOPIC", "topic-01");
	const std::string subscription = envOr("SERVICEBUS_SUBSCRIPTION", "graftcode-test-sub");

	g_oneWayReceived.store(false);
	{
		std::lock_guard<std::mutex> lock(g_oneWayMutex);
		g_oneWayPayload.clear();
	}

	const std::string serverConfig =
		std::string("{\"name\":\"live-oneway\",\"connectionString\":\"") + jsonEscape(connectionString)
		+ "\",\"topic\":\"" + jsonEscape(topic)
		+ "\",\"subscription\":\"" + jsonEscape(subscription) + "\"}";

	auto server = std::make_unique<ServiceBusServer>();
	server->configure(serverConfig, &RecordOneWayCallback);
	std::thread serverThread([&server]() { server->start(); });
	std::this_thread::sleep_for(std::chrono::seconds(3));

	ServiceBusClientConfig config;
	config.connectionString = connectionString;
	config.topic = topic;

	const bool sent = ServiceBusClient::PublishOneWay(config, kSamplePayload);
	EXPECT_TRUE(sent);

	for (int i = 0; i < 200 && !g_oneWayReceived.load(); ++i) {
		std::this_thread::sleep_for(std::chrono::milliseconds(100));
	}

	server->stop();
	serverThread.join();

	EXPECT_TRUE(g_oneWayReceived.load());
	std::lock_guard<std::mutex> lock(g_oneWayMutex);
	EXPECT_EQ(g_oneWayPayload, kSamplePayload);
}
