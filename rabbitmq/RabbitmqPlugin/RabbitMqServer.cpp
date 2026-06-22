#include "rabbitMqServer.h"

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#endif

#ifdef max
#undef max
#endif
#ifdef min
#undef min
#endif

#include <amqpcpp.h>
#include <nlohmann/json.hpp>

#include <atomic>
#include <chrono>
#include <cstring>
#include <iostream>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

using namespace GraftcodeGateway;

namespace {
	constexpr int kRabbitMqPrefetchCount = 1;
	constexpr int kReadBufferSize = 64 * 1024;
	std::atomic_bool g_stopRequested{ false };
	using Json = nlohmann::json;

	struct RabbitMqServerConfig {
		std::string name{ "" };
		std::string host{ "" };
		std::uint16_t port{ 0 };
		std::string queue{};
		std::string replyQueue{};
		std::string user{ "" };
		std::string password{ "" };
		std::string vhost{ "" };
		std::uint32_t rpcTimeoutMs{ 0 };
	};

	RabbitMqServerConfig g_serverConfig;
	std::mutex g_serverConfigMutex;
	IServer::ProcessMessageFn g_processMessage = nullptr;
	std::mutex g_processMessageMutex;
	std::mutex g_logMutex;

	void logInfo(const std::string& message) {
		std::lock_guard<std::mutex> lock(g_logMutex);
		std::cout << "[INFO] " << message << std::endl;
	}

	void logWarn(const std::string& message) {
		std::lock_guard<std::mutex> lock(g_logMutex);
		std::cout << "[WARN] " << message << std::endl;
	}

	void logDebug(const std::string& message) {
		std::lock_guard<std::mutex> lock(g_logMutex);
		std::cout << "[DEBUG] " << message << std::endl;
	}

#ifdef _WIN32
	using SocketHandle = SOCKET;
	constexpr SocketHandle kInvalidSocket = INVALID_SOCKET;
#else
	using SocketHandle = int;
	constexpr SocketHandle kInvalidSocket = -1;
#endif

	std::mutex g_socketMutex;
	SocketHandle g_activeSocket = kInvalidSocket;

	bool shouldStop() {
		return g_stopRequested.load(std::memory_order_acquire);
	}

	void setActiveSocket(SocketHandle socket) {
		std::lock_guard<std::mutex> lock(g_socketMutex);
		g_activeSocket = socket;
	}

	void clearActiveSocket(SocketHandle socket) {
		std::lock_guard<std::mutex> lock(g_socketMutex);
		if (g_activeSocket == socket) {
			g_activeSocket = kInvalidSocket;
		}
	}

	void closeSocket(SocketHandle socket) {
		if (socket == kInvalidSocket) return;
#ifdef _WIN32
		closesocket(socket);
#else
		::shutdown(socket, SHUT_RDWR);
		::close(socket);
#endif
	}

	void closeActiveSocket() {
		std::lock_guard<std::mutex> lock(g_socketMutex);
		if (g_activeSocket != kInvalidSocket) {
			closeSocket(g_activeSocket);
			g_activeSocket = kInvalidSocket;
		}
	}

	bool isWouldBlockError() {
#ifdef _WIN32
		const int err = WSAGetLastError();
		return err == WSAEWOULDBLOCK || err == WSAEINTR;
#else
		return errno == EWOULDBLOCK || errno == EAGAIN || errno == EINTR;
#endif
	}

	std::string lastSocketErrorMessage(const std::string& prefix) {
#ifdef _WIN32
		return prefix + " (WSA error " + std::to_string(WSAGetLastError()) + ")";
#else
		return prefix + " (" + std::string(std::strerror(errno)) + ")";
#endif
	}

	std::string encodeUriSegment(const std::string& value) {
		static constexpr char kHex[] = "0123456789ABCDEF";
		std::string encoded;
		encoded.reserve(value.size());
		for (unsigned char c : value) {
			const bool safe =
				(c >= 'a' && c <= 'z') ||
				(c >= 'A' && c <= 'Z') ||
				(c >= '0' && c <= '9') ||
				c == '-' || c == '_' || c == '.' || c == '~';
			if (safe) {
				encoded.push_back(static_cast<char>(c));
			}
			else {
				encoded.push_back('%');
				encoded.push_back(kHex[(c >> 4) & 0x0F]);
				encoded.push_back(kHex[c & 0x0F]);
			}
		}
		return encoded;
	}

	std::string buildAmqpAddress(const RabbitMqServerConfig& config) {
		const std::string vhost = config.vhost.empty() ? "/" : config.vhost;
		const std::string encodedVhost =
			(vhost == "/") ? "%2F" : encodeUriSegment(vhost[0] == '/' ? vhost.substr(1) : vhost);

		return "amqp://" +
			encodeUriSegment(config.user) +
			":" +
			encodeUriSegment(config.password) +
			"@" +
			config.host +
			":" +
			std::to_string(config.port) +
			"/" +
			encodedVhost;
	}

	std::string normalizeRelaxedJson(std::string jsonSource) {
		bool inString = false;
		char prev = '\0';
		for (char& c : jsonSource) {
			if (c == '"' && prev != '\\') {
				inString = !inString;
			}
			if (!inString && c == '=') {
				c = ':';
			}
			prev = c;
		}
		return jsonSource;
	}

	std::string getOptionalString(const Json& node, const char* key, const std::string& fallback) {
		const auto it = node.find(key);
		if (it != node.end() && it->is_string()) return it->get<std::string>();
		return fallback;
	}

	void applyConfigFromJson(const std::string& jsonSource) {
		if (jsonSource.empty()) {
			throw std::runtime_error("RabbitMQ consumer: empty configuration JSON");
		}

		const Json parsed = Json::parse(normalizeRelaxedJson(jsonSource));
		if (!parsed.is_object()) {
			throw std::runtime_error("RabbitMQ consumer: configuration must be a JSON object");
		}

		RabbitMqServerConfig config;
		config.name = getOptionalString(parsed, "name", config.name);
		config.host = getOptionalString(parsed, "host", config.host);
		config.queue = getOptionalString(parsed, "queue", config.queue);
		config.replyQueue = getOptionalString(parsed, "replyQueue", config.replyQueue);
		config.user = getOptionalString(parsed, "user", config.user);
		config.password = getOptionalString(parsed, "password", config.password);
		config.vhost = getOptionalString(parsed, "vhost", config.vhost);

		const auto portIt = parsed.find("port");
		if (portIt != parsed.end() && portIt->is_number_integer()) {
			const auto parsedPort = portIt->get<int64_t>();
			if (parsedPort <= 0 || parsedPort > std::numeric_limits<std::uint16_t>::max()) {
				throw std::runtime_error("RabbitMQ consumer: invalid port in configuration");
			}
			config.port = static_cast<std::uint16_t>(parsedPort);
		}

		const auto timeoutIt = parsed.find("rpcTimeoutMs");
		if (timeoutIt != parsed.end() && timeoutIt->is_number_integer()) {
			const auto parsedTimeout = timeoutIt->get<int64_t>();
			if (parsedTimeout <= 0 || parsedTimeout > std::numeric_limits<std::uint32_t>::max()) {
				throw std::runtime_error("RabbitMQ consumer: invalid rpcTimeoutMs in configuration");
			}
			config.rpcTimeoutMs = static_cast<std::uint32_t>(parsedTimeout);
		}

		if (config.queue.empty()) {
			throw std::runtime_error("RabbitMQ consumer: missing required field 'queue'");
		}
		if (config.user.empty()) {
			throw std::runtime_error("RabbitMQ consumer: missing required field 'user'");
		}

		std::lock_guard<std::mutex> lock(g_serverConfigMutex);
		g_serverConfig = std::move(config);
	}

	RabbitMqServerConfig currentConfig() {
		std::lock_guard<std::mutex> lock(g_serverConfigMutex);
		return g_serverConfig;
	}

	void setProcessMessage(IServer::ProcessMessageFn processMessage) {
		std::lock_guard<std::mutex> lock(g_processMessageMutex);
		g_processMessage = processMessage;
	}

	IServer::ProcessMessageFn currentProcessMessage() {
		std::lock_guard<std::mutex> lock(g_processMessageMutex);
		return g_processMessage;
	}

	SocketHandle connectSocketToBroker(const RabbitMqServerConfig& config) {
		addrinfo hints{};
		hints.ai_family = AF_UNSPEC;
		hints.ai_socktype = SOCK_STREAM;
		hints.ai_protocol = IPPROTO_TCP;

		addrinfo* results = nullptr;
		const std::string port = std::to_string(config.port);
		const int gaiResult = getaddrinfo(config.host.c_str(), port.c_str(), &hints, &results);
		if (gaiResult != 0) {
#ifdef _WIN32
			throw std::runtime_error(
				"RabbitMQ consumer: getaddrinfo failed: " + std::to_string(gaiResult));
#else
			throw std::runtime_error(
				"RabbitMQ consumer: getaddrinfo failed: " + std::string(gai_strerror(gaiResult)));
#endif
			return kInvalidSocket;
		}

		SocketHandle socket = kInvalidSocket;
		for (addrinfo* ai = results; ai != nullptr; ai = ai->ai_next) {
			socket = ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
			if (socket == kInvalidSocket) continue;

			if (::connect(socket, ai->ai_addr, static_cast<int>(ai->ai_addrlen)) == 0) {
				break;
			}

			closeSocket(socket);
			socket = kInvalidSocket;
		}

		freeaddrinfo(results);

		if (socket == kInvalidSocket) {
			throw std::runtime_error("RabbitMQ consumer: failed to connect socket to broker");
		}

		return socket;
	}

	bool publishResponseIfRequested(
		AMQP::Channel& channel,
		const AMQP::Message& requestMessage,
		const std::vector<byte>& responseBytes) {
		if (!requestMessage.hasReplyTo() || requestMessage.replyTo().empty()) {
			return true; // Request did not ask for RPC response.
		}

		const char* responseBody = responseBytes.empty()
			? ""
			: reinterpret_cast<const char*>(responseBytes.data());
		AMQP::Envelope responseEnvelope(responseBody, responseBytes.size());
		responseEnvelope.setContentType("application/octet-stream");
		if (requestMessage.hasCorrelationID()) {
			responseEnvelope.setCorrelationID(requestMessage.correlationID());
		}

		const bool published = channel.publish(
			"",
			requestMessage.replyTo(),
			responseEnvelope);
		if (!published) {
			throw std::runtime_error(
				"RabbitMQ consumer: failed to publish response to reply_to queue: " + requestMessage.replyTo());
			return false;
		}

		return true;
	}

	enum class WaitStatus { Readable, Timeout, Error };

	WaitStatus waitForSocketReadable(SocketHandle socket, int timeoutMs) {
		fd_set readSet;
		FD_ZERO(&readSet);
		FD_SET(socket, &readSet);

		timeval timeout{};
		timeout.tv_sec = timeoutMs / 1000;
		timeout.tv_usec = (timeoutMs % 1000) * 1000;

#ifdef _WIN32
		const int result = ::select(0, &readSet, nullptr, nullptr, &timeout);
#else
		const int result = ::select(socket + 1, &readSet, nullptr, nullptr, &timeout);
#endif
		if (result > 0 && FD_ISSET(socket, &readSet)) {
			return WaitStatus::Readable;
		}
		if (result == 0) {
			return WaitStatus::Timeout;
		}
		return WaitStatus::Error;
	}

	struct ConsumerRuntimeState {
		std::atomic_bool connectionReady{ false };
		std::atomic_bool connectionClosed{ false };
		std::atomic_bool connectionError{ false };
	};

	class SocketConnectionHandler final : public AMQP::ConnectionHandler {
	public:
		SocketConnectionHandler(SocketHandle socket, ConsumerRuntimeState& state)
			: socket_(socket), state_(state) {
		}

		void onData(AMQP::Connection* connection, const char* buffer, size_t size) override {
			(void)connection;
			if (size == 0 || !buffer) return;

			std::lock_guard<std::mutex> lock(sendMutex_);
			size_t sentTotal = 0;
			while (sentTotal < size) {
#ifdef _WIN32
				const int sent = ::send(socket_, buffer + sentTotal, static_cast<int>(size - sentTotal), 0);
#else
				const ssize_t sent = ::send(socket_, buffer + sentTotal, size - sentTotal, 0);
#endif
				if (sent <= 0) {
					state_.connectionError.store(true, std::memory_order_release);
					throw std::runtime_error(lastSocketErrorMessage("RabbitMQ consumer: socket send failed"));
					return;
				}
				sentTotal += static_cast<size_t>(sent);
			}
		}

		void onReady(AMQP::Connection* connection) override {
			(void)connection;
			state_.connectionReady.store(true, std::memory_order_release);
			logInfo("RabbitMQ consumer AMQP session is ready");
		}

		void onError(AMQP::Connection* connection, const char* message) override {
			(void)connection;
			state_.connectionError.store(true, std::memory_order_release);
			throw std::runtime_error(
				std::string("RabbitMQ consumer AMQP error: ") + (message ? message : "unknown"));
		}

		void onClosed(AMQP::Connection* connection) override {
			(void)connection;
			state_.connectionClosed.store(true, std::memory_order_release);
		}

	private:
		SocketHandle socket_;
		ConsumerRuntimeState& state_;
		std::mutex sendMutex_;
	};
}

void GraftcodeGateway::RabbitMqServer::configure(
	const std::string& jsonConfig,
	ProcessMessageFn processMessage) {
	if (processMessage == nullptr) {
		throw std::runtime_error("RabbitMQ consumer: processMessage callback is required");
	}
	applyConfigFromJson(jsonConfig);
	setProcessMessage(processMessage);
}

void GraftcodeGateway::RabbitMqServer::start() {
	try {
		g_stopRequested.store(false, std::memory_order_release);
		const RabbitMqServerConfig config = currentConfig();

#ifdef _WIN32
		WSAData wsaData{};
		if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
			throw std::runtime_error("RabbitMQ consumer: WSAStartup failed");
			return;
		}
#endif

		logInfo(
			"RabbitMQ consumer '" + config.name + "' is enabled. Queue: " + config.queue +
			" | Broker: " + config.host + ":" + std::to_string(config.port));

		while (!shouldStop()) {
			SocketHandle socket = connectSocketToBroker(config);
			if (socket == kInvalidSocket) {
				if (!shouldStop()) std::this_thread::sleep_for(std::chrono::seconds(2));
				continue;
			}

			setActiveSocket(socket);
			logInfo("RabbitMQ consumer TCP connection established");

			ConsumerRuntimeState runtimeState{};
			SocketConnectionHandler handler(socket, runtimeState);
			AMQP::Connection connection(
				&handler,
				AMQP::Login(config.user, config.password),
				config.vhost.empty() ? "/" : config.vhost);
			AMQP::Channel channel(&connection);

			channel.onError([&](const char* errorMessage) {
				runtimeState.connectionError.store(true, std::memory_order_release);
				throw std::runtime_error(
					std::string("RabbitMQ consumer channel error: ") + (errorMessage ? errorMessage : "unknown"));
				});

			channel.setQos(kRabbitMqPrefetchCount);
			auto& consumer = channel.consume(config.queue);
			consumer.onSuccess([&](const std::string& consumerTag) {
				logInfo("RabbitMQ consumer started with tag: " + consumerTag);
				});
			consumer.onError([&](const char* errorMessage) {
				runtimeState.connectionError.store(true, std::memory_order_release);
				throw std::runtime_error(
					std::string("RabbitMQ consumer setup failed: ") + (errorMessage ? errorMessage : "unknown"));
				});
			consumer.onReceived([&](const AMQP::Message& message, uint64_t deliveryTag, bool /*redelivered*/) {
				const auto messageSize = message.bodySize();
				if (messageSize == 0) {
					logWarn("RabbitMQ consumer: received empty message body");
					channel.ack(deliveryTag);
					return;
				}

				std::vector<byte> request(messageSize);
				std::memcpy(request.data(), message.body(), messageSize);

				try {
					const auto processMessage = currentProcessMessage();
					if (processMessage == nullptr) {
						throw std::runtime_error("RabbitMQ consumer: processMessage callback is not configured");
					}
					std::vector<byte> response;
					auto writeResponse = [](void* context, const byte* data, std::size_t size) {
						auto* out = static_cast<std::vector<byte>*>(context);
						if (out == nullptr) {
							return;
						}
						if (data == nullptr || size == 0) {
							out->clear();
							return;
						}
						out->assign(data, data + size);
					};
					if (!processMessage(
						request.data(),
						request.size(),
						writeResponse,
						&response)) {
						throw std::runtime_error("RabbitMQ consumer: processMessage callback returned failure");
					}
					if (!publishResponseIfRequested(channel, message, response)) {
						// Requeue when response path failed so request can be retried.
						channel.reject(deliveryTag, AMQP::requeue);
						return;
					}

					logDebug(
						"RabbitMQ consumer: processed message (" + std::to_string(request.size()) +
						"B), response size: " + std::to_string(response.size()) + "B");
					if (message.hasReplyTo() && !message.replyTo().empty()) {
						logDebug(
							"RabbitMQ consumer: response published to reply_to queue: " + message.replyTo());
					}
					else {
						logWarn(
							"RabbitMQ consumer: request has no reply_to, response was not published");
					}
					channel.ack(deliveryTag);
				}
				catch (const std::exception& ex) {
					throw std::runtime_error(std::string("RabbitMQ consumer: processing failed: ") + ex.what());
					channel.reject(deliveryTag);
				}
				catch (...) {
					throw std::runtime_error("RabbitMQ consumer: processing failed with unknown error");
					channel.reject(deliveryTag);
				}
				});

			std::vector<char> receiveBuffer(kReadBufferSize);
			std::vector<char> pendingBytes;
			pendingBytes.reserve(kReadBufferSize);

			while (!shouldStop() &&
				!runtimeState.connectionClosed.load(std::memory_order_acquire) &&
				!runtimeState.connectionError.load(std::memory_order_acquire)) {
				const WaitStatus waitStatus = waitForSocketReadable(socket, 1000);
				if (waitStatus == WaitStatus::Timeout) {
					continue;
				}
				if (waitStatus == WaitStatus::Error) {
					if (!shouldStop()) {
						throw std::runtime_error(lastSocketErrorMessage("RabbitMQ consumer: select failed"));
					}
					runtimeState.connectionError.store(true, std::memory_order_release);
					break;
				}

#ifdef _WIN32
				const int bytesRead = ::recv(socket, receiveBuffer.data(), static_cast<int>(receiveBuffer.size()), 0);
#else
				const ssize_t bytesRead = ::recv(socket, receiveBuffer.data(), receiveBuffer.size(), 0);
#endif

				if (bytesRead == 0) {
					runtimeState.connectionClosed.store(true, std::memory_order_release);
					break;
				}
				if (bytesRead < 0) {
					if (isWouldBlockError()) {
						continue;
					}
					if (!shouldStop()) {
						throw std::runtime_error(lastSocketErrorMessage("RabbitMQ consumer: recv failed"));
					}
					runtimeState.connectionError.store(true, std::memory_order_release);
					break;
				}

				const size_t chunkSize = static_cast<size_t>(bytesRead);
				pendingBytes.insert(
					pendingBytes.end(),
					receiveBuffer.begin(),
					receiveBuffer.begin() + static_cast<std::ptrdiff_t>(chunkSize));

				while (!pendingBytes.empty()) {
					const uint64_t parsed = connection.parse(pendingBytes.data(), pendingBytes.size());
					if (parsed == 0 || parsed > pendingBytes.size()) {
						break;
					}
					pendingBytes.erase(
						pendingBytes.begin(),
						pendingBytes.begin() + static_cast<std::ptrdiff_t>(parsed));
				}
			}

			if (runtimeState.connectionError.load(std::memory_order_acquire)) {
				connection.fail("RabbitMQ consumer connection failure");
			}

			clearActiveSocket(socket);
			closeSocket(socket);

			if (!shouldStop()) {
				logWarn("RabbitMQ consumer disconnected, retrying in 2 seconds");
				std::this_thread::sleep_for(std::chrono::seconds(2));
			}
		}

		clearActiveSocket(kInvalidSocket);

#ifdef _WIN32
		WSACleanup();
#endif
		logInfo("RabbitMQ consumer stopped");
	}
	catch (const std::exception& ex) {
		logWarn(std::string("RabbitMQ consumer error: ") + ex.what());
	}
	catch (...) {
		logWarn("RabbitMQ consumer stopped with unknown error");
	}
}

void GraftcodeGateway::RabbitMqServer::stop() {
	logInfo("Shutting down RabbitMQ consumer... ");
	g_stopRequested.store(true, std::memory_order_release);
	closeActiveSocket();
}

#if defined(_WIN32)
#define RABBITMQ_PLUGIN_EXPORT extern "C" __declspec(dllexport)
#else
#define RABBITMQ_PLUGIN_EXPORT extern "C"
#endif

RABBITMQ_PLUGIN_EXPORT GraftcodeGateway::IServer* CreateServer() {
	return new GraftcodeGateway::RabbitMqServer();
	
}

RABBITMQ_PLUGIN_EXPORT void DestroyServer(GraftcodeGateway::IServer* server) {
	delete server;
}
