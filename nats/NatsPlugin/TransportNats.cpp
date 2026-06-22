#include "TransportNats.h"

#include <mutex>
#include <stdexcept>
#include <thread>

using namespace Graftcode::Plugins::Nats;

TransportNats::TransportNats(std::string host, unsigned short port, std::string configSource)
{
    config_.host = std::move(host);
    config_.port = static_cast<std::uint16_t>(port);
    configSource_ = std::move(configSource);
    NatsClient::ApplyConfigSource(configSource_, config_);
}

int TransportNats::Initialize(byte callingRuntimeNumber, byte calledRuntimeNumber, byte calledRuntimeVersion)
{
    (void)callingRuntimeNumber;
    (void)calledRuntimeNumber;
    (void)calledRuntimeVersion;
    return 0;
}

int TransportNats::SendCommand(byte* messageByteArray, int32_t messageByteArrayLen)
{
    if (messageByteArray == nullptr || messageByteArrayLen < 0) {
        throw std::runtime_error("invalid NATS payload");
    }

    std::vector<byte> responsePayload;
    const bool sent = NatsClient::SendRpc(
        config_,
        messageByteArray,
        static_cast<std::size_t>(messageByteArrayLen),
        responsePayload);
    if (!sent) {
        throw std::runtime_error("failed NATS request/response");
    }

    if (!responsePayload.empty() && responsePayload[0] == static_cast<byte>(255)) {
        const std::string errorMessage(responsePayload.begin() + 1, responsePayload.end());
        throw std::runtime_error(errorMessage);
    }

    {
        std::unique_lock<std::shared_mutex> lock(responseByThreadMutex_);
        responseByThread_[std::this_thread::get_id()] = responsePayload;
    }

    return static_cast<int>(responsePayload.size());
}

int TransportNats::ReadResponse(byte* responseByteArray, int32_t responseByteArrayLen)
{
    if (responseByteArray == nullptr || responseByteArrayLen < 0) {
        throw std::runtime_error("invalid response buffer");
    }

    std::vector<byte> responsePayload;
    {
        std::shared_lock<std::shared_mutex> lock(responseByThreadMutex_);
        const auto it = responseByThread_.find(std::this_thread::get_id());
        if (it == responseByThread_.end()) {
            throw std::runtime_error("NATS response not found for thread");
        }
        responsePayload = it->second;
    }

    if (static_cast<std::size_t>(responseByteArrayLen) > responsePayload.size()) {
        throw std::runtime_error("response buffer length mismatch");
    }

    for (int i = 0; i < responseByteArrayLen; ++i) {
        responseByteArray[i] = responsePayload[static_cast<std::size_t>(i)];
    }

    return 0;
}

#if defined(_WIN32)
#define NATS_PLUGIN_EXPORT extern "C" __declspec(dllexport)
#else
#define NATS_PLUGIN_EXPORT extern "C"
#endif

NATS_PLUGIN_EXPORT Hypertube::Native::Interfaces::ITransport* CreateTransportChannel(
    const char* ipAddress,
    const unsigned short port,
    const char* configSource)
{
    return new TransportNats(
        ipAddress != nullptr ? std::string(ipAddress) : std::string(""),
        port,
        configSource != nullptr ? std::string(configSource) : std::string(""));
}

NATS_PLUGIN_EXPORT void DestroyTransportChannel(Hypertube::Native::Interfaces::ITransport* transport)
{
    delete transport;
}
