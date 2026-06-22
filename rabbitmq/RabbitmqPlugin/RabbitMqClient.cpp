#include "RabbitMqClient.h"

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
#include <filesystem>
#include <fstream>
#include <limits>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

using namespace Graftcode::Plugins::Rabbitmq;

namespace
{
	using Json = nlohmann::json;

#ifdef _WIN32
	using SocketHandle = SOCKET;
	constexpr SocketHandle InvalidSocket = INVALID_SOCKET;
#else
	using SocketHandle = int;
	constexpr SocketHandle InvalidSocket = -1;
#endif

	constexpr int ReadBufferSize = 64 * 1024;
	constexpr std::chrono::seconds ConnectionReadyTimeout(5);

	void CloseSocket(SocketHandle socket)
	{
		if (socket == InvalidSocket) {
			return;
		}
#ifdef _WIN32
		closesocket(socket);
#else
		::shutdown(socket, SHUT_RDWR);
		::close(socket);
#endif
	}

	bool IsWouldBlockError()
	{
#ifdef _WIN32
		const int err = WSAGetLastError();
		return err == WSAEWOULDBLOCK || err == WSAEINTR;
#else
		return errno == EWOULDBLOCK || errno == EAGAIN || errno == EINTR;
#endif
	}

	SocketHandle ConnectSocket(const RabbitMqClientConfig& config)
	{
		addrinfo hints{};
		hints.ai_family = AF_UNSPEC;
		hints.ai_socktype = SOCK_STREAM;
		hints.ai_protocol = IPPROTO_TCP;

		addrinfo* results = nullptr;
		const std::string portAsString = std::to_string(config.port);
		const int gaiResult = getaddrinfo(config.host.c_str(), portAsString.c_str(), &hints, &results);
		if (gaiResult != 0) {
			return InvalidSocket;
		}

		SocketHandle socket = InvalidSocket;
		for (addrinfo* ai = results; ai != nullptr; ai = ai->ai_next) {
			socket = ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
			if (socket == InvalidSocket) {
				continue;
			}

			if (::connect(socket, ai->ai_addr, static_cast<int>(ai->ai_addrlen)) == 0) {
				break;
			}

			CloseSocket(socket);
			socket = InvalidSocket;
		}

		freeaddrinfo(results);
		return socket;
	}

	enum class WaitStatus
	{
		Readable,
		Timeout,
		Error
	};

	WaitStatus WaitForSocketReadable(SocketHandle socket, int timeoutMs)
	{
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

	struct RuntimeState
	{
		std::atomic_bool connectionReady{ false };
		std::atomic_bool connectionClosed{ false };
		std::atomic_bool connectionError{ false };
	};

	struct RpcState
	{
		std::atomic_bool channelError{ false };
		std::atomic_bool queueDeclared{ false };
		std::atomic_bool consumerReady{ false };
		std::atomic_bool publishAttempted{ false };
		std::atomic_bool publishAccepted{ false };
		std::atomic_bool responseReceived{ false };
		std::string replyQueueName{};
		std::string correlationId{};
		std::vector<byte> responsePayload{};
	};

	class SocketConnectionHandler final : public AMQP::ConnectionHandler
	{
	public:
		SocketConnectionHandler(SocketHandle socket, RuntimeState& state)
			: socket_(socket), state_(state)
		{
		}

		void onData(AMQP::Connection* connection, const char* buffer, size_t size) override
		{
			(void)connection;
			if (size == 0 || buffer == nullptr) {
				return;
			}

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
					return;
				}
				sentTotal += static_cast<size_t>(sent);
			}
		}

		void onReady(AMQP::Connection* connection) override
		{
			(void)connection;
			state_.connectionReady.store(true, std::memory_order_release);
		}

		void onError(AMQP::Connection* connection, const char* message) override
		{
			(void)connection;
			(void)message;
			state_.connectionError.store(true, std::memory_order_release);
		}

		void onClosed(AMQP::Connection* connection) override
		{
			(void)connection;
			state_.connectionClosed.store(true, std::memory_order_release);
		}

	private:
		SocketHandle socket_;
		RuntimeState& state_;
		std::mutex sendMutex_;
	};

	std::string BuildCorrelationId()
	{
		const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
		std::ostringstream oss;
		oss << std::this_thread::get_id() << "-" << now;
		return oss.str();
	}

	std::string GetSessionConsumerTag()
	{
		static const std::string sessionConsumerTag = "hypertube-rpc-" + BuildCorrelationId();
		return sessionConsumerTag;
	}

	std::optional<Json> ParseJsonConfigSource(const std::string& configSource)
	{
		if (configSource.empty()) {
			return std::nullopt;
		}

		try {
			return Json::parse(configSource);
		}
		catch (...) {
			// Treat source as a path to a json file.
		}

		try {
			const std::filesystem::path sourcePath(configSource);
			if (!std::filesystem::exists(sourcePath)) {
				return std::nullopt;
			}

			std::ifstream file(sourcePath);
			if (!file.good()) {
				return std::nullopt;
			}

			std::stringstream buffer;
			buffer << file.rdbuf();
			return Json::parse(buffer.str());
		}
		catch (...) {
			return std::nullopt;
		}
	}

	const Json* FindRabbitMqNode(const Json& node, bool isRoot = false)
	{
		if (node.is_object()) {
			const auto rabbitIt = node.find("rabbitmq");
			if (rabbitIt != node.end() && rabbitIt->is_object()) {
				return &(*rabbitIt);
			}

			const bool hasDedicatedRabbitKeys = node.contains("queue")
				|| node.contains("replyQueue")
				|| node.contains("user")
				|| node.contains("password")
				|| node.contains("vhost")
				|| node.contains("rpcTimeoutMs");
			const bool hasHostAndPort = node.contains("host") && node.contains("port");
			const bool isPluginChannel = node.contains("type")
				&& node["type"].is_string()
				&& node["type"].get<std::string>() == "plugin";

			if (hasDedicatedRabbitKeys || (hasHostAndPort && (isPluginChannel || isRoot))) {
				return &node;
			}

			for (auto it = node.begin(); it != node.end(); ++it) {
				const Json* rabbitNode = FindRabbitMqNode(*it, false);
				if (rabbitNode != nullptr) {
					return rabbitNode;
				}
			}
			return nullptr;
		}

		if (node.is_array()) {
			for (const auto& item : node) {
				const Json* rabbitNode = FindRabbitMqNode(item, false);
				if (rabbitNode != nullptr) {
					return rabbitNode;
				}
			}
		}

		return nullptr;
	}

	void ApplyRabbitMqConfig(const Json& rabbitNode, RabbitMqClientConfig& config)
	{
		if (!rabbitNode.is_object()) {
			return;
		}

		const auto hostIt = rabbitNode.find("host");
		if (hostIt != rabbitNode.end() && hostIt->is_string()) {
			config.host = hostIt->get<std::string>();
		}

		const auto portIt = rabbitNode.find("port");
		if (portIt != rabbitNode.end() && portIt->is_number_integer()) {
			const auto parsedPort = portIt->get<int64_t>();
			if (parsedPort > 0 && parsedPort <= std::numeric_limits<std::uint16_t>::max()) {
				config.port = static_cast<std::uint16_t>(parsedPort);
			}
		}

		const auto queueIt = rabbitNode.find("queue");
		if (queueIt != rabbitNode.end() && queueIt->is_string()) {
			config.queue = queueIt->get<std::string>();
		}

		const auto replyQueueIt = rabbitNode.find("replyQueue");
		if (replyQueueIt != rabbitNode.end() && replyQueueIt->is_string()) {
			config.replyQueue = replyQueueIt->get<std::string>();
		}

		const auto userIt = rabbitNode.find("user");
		if (userIt != rabbitNode.end() && userIt->is_string()) {
			config.user = userIt->get<std::string>();
		}

		const auto passwordIt = rabbitNode.find("password");
		if (passwordIt != rabbitNode.end() && passwordIt->is_string()) {
			config.password = passwordIt->get<std::string>();
		}

		const auto vhostIt = rabbitNode.find("vhost");
		if (vhostIt != rabbitNode.end() && vhostIt->is_string()) {
			config.vhost = vhostIt->get<std::string>();
		}

		const auto timeoutIt = rabbitNode.find("rpcTimeoutMs");
		if (timeoutIt != rabbitNode.end() && timeoutIt->is_number_integer()) {
			const auto parsedTimeout = timeoutIt->get<int64_t>();
			if (parsedTimeout > 0 && parsedTimeout <= std::numeric_limits<std::uint32_t>::max()) {
				config.rpcTimeoutMs = static_cast<std::uint32_t>(parsedTimeout);
			}
		}
	}
}

void RabbitMqClient::ApplyConfigSource(const std::string& configSource, RabbitMqClientConfig& config)
{
	const std::optional<Json> parsedSource = ParseJsonConfigSource(configSource);
	if (!parsedSource.has_value()) {
		return;
	}

	const Json* rabbitNode = FindRabbitMqNode(parsedSource.value(), true);
	if (rabbitNode == nullptr) {
		return;
	}

	ApplyRabbitMqConfig(*rabbitNode, config);
}

bool RabbitMqClient::Send(const RabbitMqClientConfig& config, const std::vector<byte>& payload)
{
	std::vector<byte> responsePayload;
	return SendRpc(config, payload, responsePayload);
}

bool RabbitMqClient::Send(const RabbitMqClientConfig& config, const byte* payload, std::size_t payloadSize)
{
	std::vector<byte> responsePayload;
	return SendRpc(config, payload, payloadSize, responsePayload);
}

bool RabbitMqClient::SendRpc(
	const RabbitMqClientConfig& config,
	const std::vector<byte>& payload,
	std::vector<byte>& responsePayload)
{
	const byte* data = payload.empty() ? nullptr : payload.data();
	return SendRpc(config, data, payload.size(), responsePayload);
}

bool RabbitMqClient::SendRpc(
	const RabbitMqClientConfig& config,
	const byte* payload,
	std::size_t payloadSize,
	std::vector<byte>& responsePayload)
{
	responsePayload.clear();

	if (config.queue.empty()) {
		return false;
	}
	if (config.replyQueue.empty()) {
		return false;
	}
	if (payloadSize > 0 && payload == nullptr) {
		return false;
	}

#ifdef _WIN32
	WSAData wsaData{};
	if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
		return false;
	}
#endif

	SocketHandle socket = ConnectSocket(config);
	if (socket == InvalidSocket) {
#ifdef _WIN32
		WSACleanup();
#endif
		return false;
	}

	RuntimeState runtimeState{};
	RpcState rpcState{};
	rpcState.correlationId = BuildCorrelationId();

	SocketConnectionHandler handler(socket, runtimeState);
	AMQP::Connection connection(
		&handler,
		AMQP::Login(config.user, config.password),
		config.vhost.empty() ? "/" : config.vhost);
	AMQP::Channel channel(&connection);

	channel.onError([&](const char*) {
		rpcState.channelError.store(true, std::memory_order_release);
		});

	rpcState.replyQueueName = config.replyQueue;
	rpcState.queueDeclared.store(true, std::memory_order_release);

	auto& consumer = channel.consume(config.replyQueue, GetSessionConsumerTag());
	consumer.onSuccess([&](const std::string&) {
		rpcState.consumerReady.store(true, std::memory_order_release);
		}).onReceived([&](const AMQP::Message& message, uint64_t deliveryTag, bool) {
			if (!message.hasCorrelationID() || message.correlationID() != rpcState.correlationId) {
				channel.reject(deliveryTag, AMQP::requeue);
				return;
			}

			const std::size_t bodySize = message.bodySize();
			rpcState.responsePayload.resize(bodySize);
			if (bodySize > 0) {
				std::memcpy(rpcState.responsePayload.data(), message.body(), bodySize);
			}

			rpcState.responseReceived.store(true, std::memory_order_release);
			channel.ack(deliveryTag);
			}).onError([&](const char*) {
				rpcState.channelError.store(true, std::memory_order_release);
				});

	std::vector<char> receiveBuffer(ReadBufferSize);
	std::vector<char> pendingBytes;
	pendingBytes.reserve(ReadBufferSize);
	const auto startedAt = std::chrono::steady_clock::now();
	const auto rpcTimeout = std::chrono::milliseconds(config.rpcTimeoutMs == 0 ? 30000u : config.rpcTimeoutMs);

	while (!runtimeState.connectionClosed.load(std::memory_order_acquire) &&
		!runtimeState.connectionError.load(std::memory_order_acquire) &&
		!rpcState.channelError.load(std::memory_order_acquire)) {

		if (runtimeState.connectionReady.load(std::memory_order_acquire) &&
			rpcState.queueDeclared.load(std::memory_order_acquire) &&
			rpcState.consumerReady.load(std::memory_order_acquire) &&
			!rpcState.publishAttempted.load(std::memory_order_acquire)) {
			const char* body = payloadSize == 0 ? "" : reinterpret_cast<const char*>(payload);
			AMQP::Envelope envelope(body, payloadSize);
			envelope.setContentType("application/octet-stream");
			envelope.setReplyTo(config.replyQueue);
			envelope.setCorrelationID(rpcState.correlationId);

			const bool publishAccepted = channel.publish("", config.queue, envelope);
			rpcState.publishAttempted.store(true, std::memory_order_release);
			rpcState.publishAccepted.store(publishAccepted, std::memory_order_release);
			if (!publishAccepted) {
				break;
			}
		}

		if ((std::chrono::steady_clock::now() - startedAt) > ConnectionReadyTimeout &&
			!runtimeState.connectionReady.load(std::memory_order_acquire)) {
			break;
		}

		if ((std::chrono::steady_clock::now() - startedAt) > rpcTimeout) {
			break;
		}

		if (rpcState.responseReceived.load(std::memory_order_acquire)) {
			responsePayload = rpcState.responsePayload;
			break;
		}

		const WaitStatus waitStatus = WaitForSocketReadable(socket, 500);
		if (waitStatus == WaitStatus::Timeout) {
			continue;
		}
		if (waitStatus == WaitStatus::Error) {
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
			if (IsWouldBlockError()) {
				continue;
			}
			break;
		}

		const std::size_t chunkSize = static_cast<std::size_t>(bytesRead);
		pendingBytes.insert(
			pendingBytes.end(),
			receiveBuffer.begin(),
			receiveBuffer.begin() + static_cast<std::ptrdiff_t>(chunkSize));

		while (!pendingBytes.empty()) {
			const std::uint64_t parsed = connection.parse(pendingBytes.data(), pendingBytes.size());
			if (parsed == 0 || parsed > pendingBytes.size()) {
				break;
			}
			pendingBytes.erase(
				pendingBytes.begin(),
				pendingBytes.begin() + static_cast<std::ptrdiff_t>(parsed));
		}
	}

	connection.close();
	CloseSocket(socket);

#ifdef _WIN32
	WSACleanup();
#endif

	return rpcState.publishAccepted.load(std::memory_order_acquire) &&
		rpcState.responseReceived.load(std::memory_order_acquire) &&
		!runtimeState.connectionError.load(std::memory_order_acquire) &&
		!rpcState.channelError.load(std::memory_order_acquire);
}
