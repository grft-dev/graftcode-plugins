#include "ServiceBusClient.h"

#include <azure/core/context.hpp>
#include <azure/core/datetime.hpp>
#include <azure/core/amqp/internal/connection.hpp>
#include <azure/core/amqp/internal/connection_string_credential.hpp>
#include <azure/core/amqp/internal/message_receiver.hpp>
#include <azure/core/amqp/internal/message_sender.hpp>
#include <azure/core/amqp/internal/models/message_source.hpp>
#include <azure/core/amqp/internal/session.hpp>
#include <azure/core/amqp/models/amqp_message.hpp>
#include <azure/core/amqp/models/amqp_value.hpp>

#include <nlohmann/json.hpp>

#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

using namespace Graftcode::Plugins::ServiceBus;

namespace
{
	using Json = nlohmann::json;
	namespace Amqp = Azure::Core::Amqp::_internal;
	namespace AmqpModels = Azure::Core::Amqp::Models;

	// Service Bus standard messages are capped at 256 KB (1 MB on premium tiers).
	constexpr std::uint64_t MaxMessageSize = 1024ULL * 1024ULL;
	constexpr std::uint32_t DefaultRpcTimeoutMs = 30000;

	// AMQP source filter symbol used by Azure Service Bus to bind a receiver to a
	// specific session. Matches the value used by the official .NET/Java SDKs.
	constexpr const char* SessionFilterName = "com.microsoft:session-filter";

	std::string BuildCorrelationId()
	{
		const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
		std::ostringstream oss;
		oss << "graftcode-" << std::this_thread::get_id() << "-" << now;
		return oss.str();
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

	const Json* FindServiceBusNode(const Json& node, bool isRoot = false)
	{
		if (node.is_object()) {
			const auto sbIt = node.find("servicebus");
			if (sbIt != node.end() && sbIt->is_object()) {
				return &(*sbIt);
			}

			const bool hasDedicatedKeys = node.contains("connectionString")
				|| node.contains("queue")
				|| node.contains("replyQueue")
				|| node.contains("sharedAccessKeyName")
				|| node.contains("sharedAccessKey");
			const bool isPluginChannel = node.contains("type")
				&& node["type"].is_string()
				&& node["type"].get<std::string>() == "plugin";

			if (hasDedicatedKeys || ((isPluginChannel || isRoot) && node.contains("connectionString"))) {
				return &node;
			}

			for (auto it = node.begin(); it != node.end(); ++it) {
				const Json* found = FindServiceBusNode(*it, false);
				if (found != nullptr) {
					return found;
				}
			}
			return nullptr;
		}

		if (node.is_array()) {
			for (const auto& item : node) {
				const Json* found = FindServiceBusNode(item, false);
				if (found != nullptr) {
					return found;
				}
			}
		}

		return nullptr;
	}

	void ApplyServiceBusConfig(const Json& node, ServiceBusClientConfig& config)
	{
		if (!node.is_object()) {
			return;
		}

		const auto getString = [&node](const char* key, std::string& target) {
			const auto it = node.find(key);
			if (it != node.end() && it->is_string()) {
				target = it->get<std::string>();
			}
			};

		getString("connectionString", config.connectionString);
		getString("host", config.host);
		getString("sharedAccessKeyName", config.sharedAccessKeyName);
		getString("sharedAccessKey", config.sharedAccessKey);
		getString("queue", config.queue);
		getString("replyQueue", config.replyQueue);
		getString("topic", config.topic);

		const auto emulatorIt = node.find("useDevelopmentEmulator");
		if (emulatorIt != node.end() && emulatorIt->is_boolean()) {
			config.useDevelopmentEmulator = emulatorIt->get<bool>();
		}

		const auto timeoutIt = node.find("rpcTimeoutMs");
		if (timeoutIt != node.end() && timeoutIt->is_number_integer()) {
			const auto parsedTimeout = timeoutIt->get<int64_t>();
			if (parsedTimeout > 0 && parsedTimeout <= std::numeric_limits<std::uint32_t>::max()) {
				config.rpcTimeoutMs = static_cast<std::uint32_t>(parsedTimeout);
			}
		}
	}

	std::vector<byte> ExtractMessageBody(const AmqpModels::AmqpMessage& message)
	{
		std::vector<byte> result;
		switch (message.BodyType) {
		case AmqpModels::MessageBodyType::Data: {
			for (const auto& section : message.GetBodyAsBinary()) {
				const auto* data = section.data();
				result.insert(result.end(), data, data + section.size());
			}
			break;
		}
		case AmqpModels::MessageBodyType::Value: {
			try {
				const auto valueAsString = static_cast<std::string>(message.GetBodyAsAmqpValue());
				result.assign(valueAsString.begin(), valueAsString.end());
			}
			catch (...) {
				// Non-string AMQP value bodies are not supported by the graftcode contract.
			}
			break;
		}
		default:
			break;
		}
		return result;
	}
}

std::string ServiceBusClient::BuildConnectionString(const ServiceBusClientConfig& config)
{
	if (!config.connectionString.empty()) {
		return config.connectionString;
	}

	if (config.host.empty() || config.sharedAccessKeyName.empty() || config.sharedAccessKey.empty()) {
		return std::string();
	}

	std::string host = config.host;
	const std::string scheme = "sb://";
	if (host.rfind(scheme, 0) == std::string::npos) {
		host = scheme + host;
	}
	if (!host.empty() && host.back() != '/') {
		host.push_back('/');
	}

	std::string connectionString = "Endpoint=" + host
		+ ";SharedAccessKeyName=" + config.sharedAccessKeyName
		+ ";SharedAccessKey=" + config.sharedAccessKey;
	if (config.useDevelopmentEmulator) {
		connectionString += ";UseDevelopmentEmulator=true";
	}
	return connectionString;
}

void ServiceBusClient::ApplyConfigSource(const std::string& configSource, ServiceBusClientConfig& config)
{
	const std::optional<Json> parsedSource = ParseJsonConfigSource(configSource);
	if (!parsedSource.has_value()) {
		return;
	}

	const Json* node = FindServiceBusNode(parsedSource.value(), true);
	if (node == nullptr) {
		return;
	}

	ApplyServiceBusConfig(*node, config);
}

bool ServiceBusClient::Send(const ServiceBusClientConfig& config, const std::vector<byte>& payload)
{
	std::vector<byte> responsePayload;
	return SendRpc(config, payload, responsePayload);
}

bool ServiceBusClient::Send(const ServiceBusClientConfig& config, const byte* payload, std::size_t payloadSize)
{
	std::vector<byte> responsePayload;
	return SendRpc(config, payload, payloadSize, responsePayload);
}

bool ServiceBusClient::PublishOneWay(const ServiceBusClientConfig& config, const std::vector<byte>& payload)
{
	const byte* data = payload.empty() ? nullptr : payload.data();
	return PublishOneWay(config, data, payload.size());
}

bool ServiceBusClient::PublishOneWay(
	const ServiceBusClientConfig& config,
	const byte* payload,
	std::size_t payloadSize)
{
	if (config.topic.empty()) {
		return false;
	}
	if (payloadSize > 0 && payload == nullptr) {
		return false;
	}

	const std::string connectionString = BuildConnectionString(config);
	if (connectionString.empty()) {
		return false;
	}

	try {
		auto credential = std::make_shared<Amqp::ServiceBusSasConnectionStringCredential>(connectionString);

		Amqp::ConnectionOptions connectionOptions;
		connectionOptions.ContainerId = "graftcode-servicebus-client";
		connectionOptions.EnableTrace = false;
		const std::uint16_t credentialPort = credential->GetPort();
		connectionOptions.Port = credentialPort != 0 ? credentialPort : Amqp::AmqpTlsPort;

		Amqp::Connection connection(credential->GetHostName(), credential, connectionOptions);

		Amqp::SessionOptions sessionOptions;
		sessionOptions.InitialIncomingWindowSize = (std::numeric_limits<std::int32_t>::max)();
		sessionOptions.InitialOutgoingWindowSize = (std::numeric_limits<std::uint16_t>::max)();
		Amqp::Session session(connection.CreateSession(sessionOptions));

		Amqp::MessageSenderOptions senderOptions;
		senderOptions.Name = "graftcode-oneway-publish";
		senderOptions.MessageSource = "graftcode-oneway-publish-source";
		senderOptions.SettleMode = Amqp::SenderSettleMode::Settled;
		senderOptions.MaxMessageSize = MaxMessageSize;
		Amqp::MessageSender sender(session.CreateMessageSender(config.topic, senderOptions, nullptr));

		if (auto openError = sender.Open()) {
			(void)openError;
			return false;
		}

		AmqpModels::AmqpMessage message;
		std::vector<std::uint8_t> body;
		if (payloadSize > 0) {
			body.assign(payload, payload + payloadSize);
		}
		message.SetBody(AmqpModels::AmqpBinaryData(body));
		message.Properties.MessageId = AmqpModels::AmqpValue(BuildCorrelationId());
		message.Properties.ContentType = std::string("application/octet-stream");

		bool sendSucceeded = false;
#if defined(ENABLE_RUST_AMQP) && ENABLE_RUST_AMQP
		sendSucceeded = !static_cast<bool>(sender.Send(message));
#else
		const auto sendResult = sender.Send(message);
		sendSucceeded = std::get<0>(sendResult) == Amqp::MessageSendStatus::Ok;
#endif
		sender.Close();
		return sendSucceeded;
	}
	catch (...) {
		return false;
	}
}

bool ServiceBusClient::SendRpc(
	const ServiceBusClientConfig& config,
	const std::vector<byte>& payload,
	std::vector<byte>& responsePayload)
{
	const byte* data = payload.empty() ? nullptr : payload.data();
	return SendRpc(config, data, payload.size(), responsePayload);
}

bool ServiceBusClient::SendRpc(
	const ServiceBusClientConfig& config,
	const byte* payload,
	std::size_t payloadSize,
	std::vector<byte>& responsePayload)
{
	responsePayload.clear();

	// One-way (topic) mode: publish and return immediately without a reply.
	// Used for void methods that produce no result.
	if (!config.topic.empty()) {
		return PublishOneWay(config, payload, payloadSize);
	}

	if (config.queue.empty()) {
		return false;
	}
	if (config.replyQueue.empty()) {
		return false;
	}
	if (payloadSize > 0 && payload == nullptr) {
		return false;
	}

	const std::string connectionString = BuildConnectionString(config);
	if (connectionString.empty()) {
		return false;
	}

	try {
		auto credential = std::make_shared<Amqp::ServiceBusSasConnectionStringCredential>(connectionString);

		Amqp::ConnectionOptions connectionOptions;
		connectionOptions.ContainerId = "graftcode-servicebus-client";
		connectionOptions.EnableTrace = false;
		const std::uint16_t credentialPort = credential->GetPort();
		connectionOptions.Port = credentialPort != 0 ? credentialPort : Amqp::AmqpTlsPort;

		Amqp::Connection connection(credential->GetHostName(), credential, connectionOptions);

		Amqp::SessionOptions sessionOptions;
		sessionOptions.InitialIncomingWindowSize = (std::numeric_limits<std::int32_t>::max)();
		sessionOptions.InitialOutgoingWindowSize = (std::numeric_limits<std::uint16_t>::max)();
		Amqp::Session session(connection.CreateSession(sessionOptions));

		const std::string correlationId = BuildCorrelationId();
		// Each request gets a unique reply session id. The reply queue is session
		// enabled, so the broker routes the matching response back to this exact
		// client even when many clients share the same reply queue.
		const std::string replySessionId = correlationId;

		// Attach the reply receiver with a Service Bus session filter so it only
		// ever sees the response that belongs to this request's session.
		AmqpModels::_internal::MessageSourceOptions replySourceOptions;
		replySourceOptions.Address = AmqpModels::AmqpValue(config.replyQueue);
		AmqpModels::AmqpMap sessionFilter;
		sessionFilter[AmqpModels::AmqpSymbol(SessionFilterName).AsAmqpValue()]
			= AmqpModels::AmqpValue(replySessionId);
		replySourceOptions.Filter = sessionFilter;
		AmqpModels::_internal::MessageSource replySource(replySourceOptions);

		Amqp::MessageReceiverOptions receiverOptions;
		receiverOptions.Name = "graftcode-rpc-reply";
		receiverOptions.MessageTarget = "graftcode-rpc-reply-target";
		receiverOptions.SettleMode = Amqp::ReceiverSettleMode::First;
		receiverOptions.MaxMessageSize = MaxMessageSize;
		receiverOptions.MaxLinkCredit = 10;
		Amqp::MessageReceiver receiver(session.CreateMessageReceiver(replySource, receiverOptions, nullptr));

		Amqp::MessageSenderOptions senderOptions;
		senderOptions.Name = "graftcode-rpc-request";
		senderOptions.MessageSource = "graftcode-rpc-request-source";
		senderOptions.SettleMode = Amqp::SenderSettleMode::Settled;
		senderOptions.MaxMessageSize = MaxMessageSize;
		Amqp::MessageSender sender(session.CreateMessageSender(config.queue, senderOptions, nullptr));

		receiver.Open();
		if (auto openError = sender.Open()) {
			(void)openError;
			receiver.Close();
			return false;
		}

		AmqpModels::AmqpMessage message;
		std::vector<std::uint8_t> body;
		if (payloadSize > 0) {
			body.assign(payload, payload + payloadSize);
		}
		message.SetBody(AmqpModels::AmqpBinaryData(body));
		message.Properties.MessageId = AmqpModels::AmqpValue(correlationId);
		message.Properties.ReplyTo = AmqpModels::AmqpValue(config.replyQueue);
		// ReplyToGroupId maps to the Service Bus ReplyToSessionId: it tells the
		// server which session id to stamp on the response.
		message.Properties.ReplyToGroupId = replySessionId;
		message.Properties.ContentType = std::string("application/octet-stream");

		bool sendSucceeded = false;
#if defined(ENABLE_RUST_AMQP) && ENABLE_RUST_AMQP
		sendSucceeded = !static_cast<bool>(sender.Send(message));
#else
		const auto sendResult = sender.Send(message);
		sendSucceeded = std::get<0>(sendResult) == Amqp::MessageSendStatus::Ok;
#endif
		if (!sendSucceeded) {
			receiver.Close();
			sender.Close();
			return false;
		}

		const std::uint32_t timeoutMs = config.rpcTimeoutMs == 0 ? DefaultRpcTimeoutMs : config.rpcTimeoutMs;
		const auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(timeoutMs);
		Azure::Core::Context context = Azure::Core::Context{}.WithDeadline(Azure::DateTime(deadline));

		const AmqpModels::AmqpValue expectedCorrelation(correlationId);
		bool received = false;
		while (std::chrono::system_clock::now() < deadline) {
			std::pair<std::shared_ptr<const AmqpModels::AmqpMessage>, Azure::Core::Amqp::Models::_internal::AmqpError> incoming;
			try {
				incoming = receiver.WaitForIncomingMessage(context);
			}
			catch (...) {
				break;
			}

			if (incoming.second) {
				break;
			}
			const auto& incomingMessage = incoming.first;
			if (!incomingMessage) {
				continue;
			}

			const bool correlationMatches =
				incomingMessage->Properties.CorrelationId == expectedCorrelation
				|| incomingMessage->Properties.MessageId == expectedCorrelation;
			if (!correlationMatches) {
				continue;
			}

			responsePayload = ExtractMessageBody(*incomingMessage);
			received = true;
			break;
		}

		receiver.Close();
		sender.Close();
		return received;
	}
	catch (...) {
		responsePayload.clear();
		return false;
	}
}
