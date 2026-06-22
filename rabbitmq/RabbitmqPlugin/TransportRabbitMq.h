#pragma once

#include "ITransport.h"
#include "RabbitMqClient.h"

#include <map>
#include <shared_mutex>
#include <string>
#include <thread>
#include <vector>

namespace Graftcode::Plugins::Rabbitmq
{
	class TransportRabbitMq : public Hypertube::Native::Interfaces::ITransport
	{
	public:
		TransportRabbitMq(std::string host, unsigned short port, std::string configSource);
		~TransportRabbitMq() override = default;

		int Initialize(byte callingRuntimeNumber, byte calledRuntimeNumber, byte calledRuntimeVersion) override;
		int SendCommand(byte* messageByteArray, int32_t messageByteArrayLen) override;
		int ReadResponse(byte* responseByteArray, int32_t responseByteArrayLen) override;

	private:
		RabbitMqClientConfig config_;
		std::string configSource;
		std::map<std::thread::id, std::vector<byte>> responseByThread_;
		std::shared_mutex responseByThreadMutex_;
	};
}
