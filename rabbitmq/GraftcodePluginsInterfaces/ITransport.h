/**
 * @file ITransport.h
 * @brief Provides the interface for the Transport class.
 */

#pragma once
#include "_common.h"

namespace Hypertube::Native::Interfaces
{
    /**
     * @class ITransport
     * @brief Provides the interface for the Transport class, which is responsible for choosing transport channel (in memory, tcp or other).
     */
    class ITransport
    {
    public:
        virtual ~ITransport() = default;

        /**
         * @brief Initializes the transport with the calling runtime and called runtime version.
         * @param callingRuntimeNumber The calling runtime number.
         * @param calledRuntimeNumber The called runtime number.
         * @param calledRuntimeVersion The called runtime version.
         * @return Status code of the operation.
         */
        virtual int Initialize(byte callingRuntimeNumber, byte calledRuntimeNumber, byte calledRuntimeVersion) = 0;

        /**
         * @brief Sends a byte array to the transport.
         * @param messageByteArray The byte array to send.
         * @param messageByteArrayLen The length of the byte array.
         * @return lenght of response byte array
         */
        virtual int SendCommand(byte* messageByteArray, int32_t messageByteArrayLen) = 0;

        /**
         * @brief Reads the response byte array from the transport.
         * @param responseByteArray The byte array to store the response.
         * @param responseByteArrayLen The length of the byte array.
         * @return Status code of the operation.
         */
        virtual int ReadResponse(byte* responseByteArray, int32_t responseByteArrayLen) = 0;

    };
}
