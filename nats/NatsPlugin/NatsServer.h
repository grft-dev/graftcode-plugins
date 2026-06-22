#pragma once

#include "IServer.h"

namespace Graftcode::Plugins::Nats
{
    class NatsServer : public GraftcodeGateway::IServer
    {
    public:
        NatsServer() = default;
        ~NatsServer() override = default;

        void configure(const std::string& jsonConfig, ProcessMessageFn processMessage) override;
        void start() override;
        void stop() override;
    };
}
