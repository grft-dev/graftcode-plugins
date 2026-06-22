#pragma once
#include "IServer.h"
#include <string>

namespace Graftcode::Plugins::Rabbitmq {
	class RabbitMqServer : public GraftcodeGateway::IServer {
	public:
		RabbitMqServer() = default;
		~RabbitMqServer() override = default;
		void configure(const std::string& jsonConfig, ProcessMessageFn processMessage) override;
		void start() override;
		void stop() override;
	};
}
