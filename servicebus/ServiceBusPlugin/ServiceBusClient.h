#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

typedef unsigned char byte;

namespace Graftcode::Plugins::ServiceBus
{
	struct ServiceBusClientConfig
	{
		// Full Service Bus connection string, e.g.
		// "Endpoint=sb://<ns>.servicebus.windows.net/;SharedAccessKeyName=<name>;SharedAccessKey=<key>".
		// When empty it is rebuilt from host/sharedAccessKeyName/sharedAccessKey below.
		std::string connectionString{ "" };
		std::string host{ "" };
		std::string sharedAccessKeyName{ "" };
		std::string sharedAccessKey{ "" };

		// Entity (queue) the request is sent to in request/reply (RPC) mode.
		std::string queue{ "" };
		// Entity (queue) the matching response is read back from in RPC mode.
		std::string replyQueue{ "" };

		// Topic to publish to in one-way (fire-and-forget) mode. When set, the
		// client publishes here and does NOT wait for a reply; used for void
		// methods that return nothing. Takes precedence over the queue RPC path.
		std::string topic{ "" };

		bool useDevelopmentEmulator{ false };
		std::uint32_t rpcTimeoutMs{ 30000 };
	};

	class ServiceBusClient
	{
	public:
		ServiceBusClient() = delete;
		~ServiceBusClient() = delete;
		static void ApplyConfigSource(const std::string& configSource, ServiceBusClientConfig& config);

		static bool Send(const ServiceBusClientConfig& config, const std::vector<byte>& payload);
		static bool Send(const ServiceBusClientConfig& config, const byte* payload, std::size_t payloadSize);

		// One-way (fire-and-forget) publish to config.topic. No reply is awaited.
		static bool PublishOneWay(const ServiceBusClientConfig& config, const std::vector<byte>& payload);
		static bool PublishOneWay(const ServiceBusClientConfig& config, const byte* payload, std::size_t payloadSize);

		static bool SendRpc(
			const ServiceBusClientConfig& config,
			const std::vector<byte>& payload,
			std::vector<byte>& responsePayload);
		static bool SendRpc(
			const ServiceBusClientConfig& config,
			const byte* payload,
			std::size_t payloadSize,
			std::vector<byte>& responsePayload);

		// Builds a full Service Bus connection string from the config fields.
		static std::string BuildConnectionString(const ServiceBusClientConfig& config);
	};
}
