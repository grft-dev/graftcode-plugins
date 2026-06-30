#pragma once
#include "IServer.h"
#include <string>

namespace Graftcode::Plugins::ServiceBus {
	class ServiceBusServer : public GraftcodeGateway::IServer {
	public:
		ServiceBusServer() = default;
		~ServiceBusServer() override = default;
		void configure(const std::string& jsonConfig, ProcessMessageFn processMessage) override;
		void start() override;
		void stop() override;
	};
}
