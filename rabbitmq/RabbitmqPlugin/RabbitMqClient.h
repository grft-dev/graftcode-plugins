#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

typedef unsigned char byte;

namespace Graftcode::Plugins::Rabbitmq
{
	struct RabbitMqClientConfig
	{
		std::string host{ "" };
		std::uint16_t port{  };
		std::string queue{ "" };
		std::string replyQueue{ "" };
		std::string user{ "" };
		std::string password{ "" };
		std::string vhost{ "" };
		std::uint32_t rpcTimeoutMs{ 30000 };
	};

	class RabbitMqClient
	{
	public:
		RabbitMqClient() = delete;
		~RabbitMqClient() = delete;
		static void ApplyConfigSource(const std::string& configSource, RabbitMqClientConfig& config);

		static bool Send(const RabbitMqClientConfig& config, const std::vector<byte>& payload);
		static bool Send(const RabbitMqClientConfig& config, const byte* payload, std::size_t payloadSize);
		static bool SendRpc(
			const RabbitMqClientConfig& config,
			const std::vector<byte>& payload,
			std::vector<byte>& responsePayload);
		static bool SendRpc(
			const RabbitMqClientConfig& config,
			const byte* payload,
			std::size_t payloadSize,
			std::vector<byte>& responsePayload);
	};
}
