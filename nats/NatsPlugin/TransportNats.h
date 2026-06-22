#pragma once

#include "ITransport.h"
#include "NatsClient.h"

#include <map>
#include <shared_mutex>
#include <string>
#include <thread>
#include <vector>

namespace Graftcode::Plugins::Nats
{
    class TransportNats : public Hypertube::Native::Interfaces::ITransport
    {
    public:
        TransportNats(std::string host, unsigned short port, std::string configSource);
        ~TransportNats() override = default;

        int Initialize(byte callingRuntimeNumber, byte calledRuntimeNumber, byte calledRuntimeVersion) override;
        int SendCommand(byte* messageByteArray, int32_t messageByteArrayLen) override;
        int ReadResponse(byte* responseByteArray, int32_t responseByteArrayLen) override;

    private:
        NatsClientConfig config_;
        std::string configSource_;
        std::map<std::thread::id, std::vector<byte>> responseByThread_;
        std::shared_mutex responseByThreadMutex_;
    };
}
