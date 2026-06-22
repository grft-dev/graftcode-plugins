/**
 * @file ITransmitter.h
 * @brief Provides the interface for the Transmitter class.
 */

#pragma once

#include "ITransport.h"

namespace Hypertube::Native::Interfaces
{
    /**
     * @class ITransmitter
     * @brief Provides the interface for the Transmitter class, which is responsible for communication with transport channels.
     */
    class ITransmitter
    {
    public:
        virtual ~ITransmitter() = default;

        /**
         * @brief Sends a byte array to the transport channel.
         * @param messageByteArray The byte array to send.
         * @param messageByteArrayLen The length of the byte array.
         * @return lenght of response byte array
         */
        virtual int SendCommand(byte* messageByteArray, int32_t messageByteArrayLen, const std::string& configSource) = 0;

        /**
         * @brief Reads the response byte array from the transport cahnnel.
         * @param responseByteArray The byte array to store the response.
         * @param responseByteArrayLen The length of the byte array.
         * @return Status code of the operation.
         */
        virtual int ReadResponse(byte* responseByteArray, int32_t responseByteArrayLen) = 0;

        /**
         * @brief Sets the transport channel for the transmitter.
         * @param pTransport The transport channel to set.
         */
        virtual void SetTransportChannel(ITransport* pTransport) = 0;

    };
}