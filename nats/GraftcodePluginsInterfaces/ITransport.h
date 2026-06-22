/**
 * @file ITransport.h
 * @brief Transport interface used by gateway native plugins.
 */
#pragma once
#include "_common.h"

namespace Hypertube::Native::Interfaces
{
    class ITransport
    {
    public:
        virtual ~ITransport() = default;
        virtual int Initialize(byte callingRuntimeNumber, byte calledRuntimeNumber, byte calledRuntimeVersion) = 0;
        virtual int SendCommand(byte* messageByteArray, int32_t messageByteArrayLen) = 0;
        virtual int ReadResponse(byte* responseByteArray, int32_t responseByteArrayLen) = 0;
    };
}
