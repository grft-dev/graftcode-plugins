/**
 * @file IReceiver.h
 * @brief Provides the interface for the Receiver class.
 */

#pragma once
#include "_common.h"

namespace Hypertube::Native::Interfaces
{
    /**
     * @class IReceiver
     * @brief Provides the interface for the Receiver class, which is responsible for communication with the launcher of specified called runtime.
     */
    class IReceiver
    {
    public:
        virtual ~IReceiver() = default;

        /**
         * @brief Activates Receiver with the provided activation data in form of json string.
         * @param activationData The activation data to use.
         * @return result of the activation.
         */
        virtual std::string Activate(const std::string licenseKey, const std::string callingRuntime) = 0;

        /**
         * @brief Set config path
         * @param configPath The configPath to use.
         * @return result of setting the path
         */
        virtual void SetConfigSource(const std::string configPath) = 0;

        /**
         * @brief Initializes the launcher with the calling runtime and called runtime version.
         * @param callingRuntime The calling runtime.
         * @param calledRuntimeVersion The called runtime version.
         * @return Status code of the operation.
         */
        virtual int Initialize(byte callingRuntime, byte calledRuntime, byte calledRuntimeVersion) = 0;

        /**
         * @brief Sends a byte array to the launcher.
         * @param messageByteArray The byte array to send.
         * @param messageByteArrayLen The length of the byte array.
         * @return lenght of response byte array
         */
        virtual int SendCommand(byte* messageByteArray, int32_t messageByteArrayLen) = 0;

        /**
         * @brief Reads the response byte array from the receiver.
         * @param responseByteArray The byte array to store launcher response.
         * @param responseByteArrayLen The length of the byte array.
         * @return Status code of the operation.
         */
        virtual int ReadResponse(byte* responseByteArray, int32_t responseByteArrayLen) = 0;

        /**
         * @brief Logs an exception to Application Insights with the provided response byte array.
         * @param responseByteArray The byte array that contains the exception information.
         * @param responseByteArrayLen The length of the byte array.
         */
        virtual void LogException(byte* responseByteArray, int32_t responseByteArrayLen) = 0;

        /**
         * @brief Loads an optimized method into the launcher. Not implemented in most runtimes.
         * @param optimizedMethodName The name of the optimized method.
         * @return Status code of the operation.
         */
        virtual int LoadOptimizedMethod(std::string optimizedMethodName) = 0;

    private:
        virtual bool isActivated() = 0;
        virtual void setActive() = 0;

    };
}
