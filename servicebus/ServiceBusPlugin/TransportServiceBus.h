#pragma once

#include "ITransport.h"
#include "ServiceBusClient.h"

#include <map>
#include <shared_mutex>
#include <string>
#include <thread>
#include <vector>

namespace Graftcode::Plugins::ServiceBus
{
	class TransportServiceBus : public Hypertube::Native::Interfaces::ITransport
	{
	public:
		TransportServiceBus(std::string host, unsigned short port, std::string configSource);
		~TransportServiceBus() override = default;

		int Initialize(byte callingRuntimeNumber, byte calledRuntimeNumber, byte calledRuntimeVersion) override;
		int SendCommand(byte* messageByteArray, int32_t messageByteArrayLen) override;
		int ReadResponse(byte* responseByteArray, int32_t responseByteArrayLen) override;

	private:
		ServiceBusClientConfig config_;
		std::string configSource;
		std::map<std::thread::id, std::vector<byte>> responseByThread_;
		std::shared_mutex responseByThreadMutex_;
	};
}
