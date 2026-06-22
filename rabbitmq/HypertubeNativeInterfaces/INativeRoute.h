/**
 * @file INativeRoute.h
 * @brief Provides the interface for the NativeRoute class.
 */

#pragma once

namespace Hypertube::Native::Interfaces
{
    /**
     * @class INativeRoute
     * @brief Provides the interface for the NativeRoute class, which is responsible for generating native routes.
     */
    class INativeRoute
    {
    public:
        virtual ~INativeRoute() = default;

        /**
         * @brief Generates a file for the specified class.
         * @param className The name of the class.
         * @return Status code of the operation.
         */
        virtual int GenerateFile(std::string className) = 0;

        /**
         * @brief Generates a library for the specified class.
         * @param className The name of the class.
         * @return The name of the generated library.
         */
        virtual std::string GenerateLibrary(std::string className) = 0;

        /**
         * @brief Loads the generated library.
         * @return Status code of the operation.
         */
        virtual int LoadGeneratedLibrary() = 0;

        /**
         * @brief Gets the handle to the library instance.
         * @return The handle to the library instance.
         */
        virtual handleToInstance GetLibraryHandle() = 0;
    };
}