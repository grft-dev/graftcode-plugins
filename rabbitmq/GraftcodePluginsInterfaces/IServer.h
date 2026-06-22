#pragma once
#include <string>

namespace GraftcodeGateway {
	class IServer {
	public:
		using byte = unsigned char;
		using WriteResponseFn = void(*)(void* context, const byte* data, std::size_t size);
		using ProcessMessageFn = bool(*)(const byte* requestData, std::size_t requestSize, WriteResponseFn writeResponse, void* writeContext);

		virtual ~IServer() = default;
		virtual void configure(const std::string& jsonConfig, ProcessMessageFn processMessage) = 0;
		virtual void start() = 0;
		virtual void stop() = 0;
	};
}
