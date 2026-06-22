#pragma once
#include "IServer.h"
#include <string>

namespace GraftcodeGateway {
	class RabbitMqServer : public IServer {
	public:
		RabbitMqServer() = default;
		~RabbitMqServer() override = default;
		void configure(const std::string& jsonConfig, ProcessMessageFn processMessage) override;
		void start() override;
		void stop() override;
	};
}
