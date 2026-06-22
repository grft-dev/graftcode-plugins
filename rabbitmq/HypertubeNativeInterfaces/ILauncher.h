/**
 * @file ILauncher.h
 * @brief Provides the interface for the Launcher class.
*/

#pragma once
#include "_common.h"

namespace Hypertube::Native::Interfaces
{
    /**
     * @class ILauncher
     * @brief Provides the interface for the Launcher class, which is responsible for initialization and communication with the called runtime.
     */
    class ILauncher
    {
    public:
        virtual ~ILauncher() = default;

        /**
         * @brief Initializes the called runtime with the called runtime version.
         * @param calledRuntimeVersion The version of the called runtime.
         * @return Status code of the operation. 0 for success.
         */
        virtual void Initialize(byte calledRuntimeVersion) = 0;

        /**
         * @brief Sends a byte array to the launcher.
         * @param messageByteArray The byte array to send.
         * @param messageByteArrayLen The length of the byte array.
         * @return Status code of the operation.
         */
        virtual int SendCommand(byte* messageByteArray, int32_t messageByteArrayLen) = 0;

        /**
         * @brief Reads the response byte array from the launcher.
         * @param responseByteArray The byte array to store the response.
         * @param responseByteArrayLen The length of the byte array.
         * @return Length of the response byte array.
         */
        virtual int ReadResponse(byte* responseByteArray, int32_t responseByteArrayLen) = 0;

        /**
         * @brief Loads an optimized method into the launcher. Not implemented in most runtimes.
         * @param optimizedMethodName The name of the optimized method.
         * @return Status code of the operation. 0 for success.
         */
        virtual int LoadOptimizedMethod(std::string optimizedMethodName) = 0;

        /**
         * @brief Checks if the launcher is initialized.
         * @return True if the launcher is initialized, false otherwise.
         */
        virtual bool IsInitialized() = 0;

        /**
         * @brief Sets the launcher as initialized.
         */
        virtual void SetInitialized() = 0;

        /**
         * @brief Gets the name of the runtime.
         * @return The name of the runtime.
         */
        virtual std::string GetRuntimeName() = 0;

		virtual std::string GetRuntimeInfo() = 0;
    };
}
