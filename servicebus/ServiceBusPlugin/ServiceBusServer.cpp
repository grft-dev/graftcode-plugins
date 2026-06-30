#include "ServiceBusServer.h"

#include "ServiceBusClient.h"

#include <azure/core/context.hpp>
#include <azure/core/datetime.hpp>
#include <azure/core/amqp/internal/connection.hpp>
#include <azure/core/amqp/internal/connection_string_credential.hpp>
#include <azure/core/amqp/internal/message_receiver.hpp>
#include <azure/core/amqp/internal/message_sender.hpp>
#include <azure/core/amqp/internal/session.hpp>
#include <azure/core/amqp/models/amqp_message.hpp>
#include <azure/core/amqp/models/amqp_value.hpp>

#include <nlohmann/json.hpp>

#include <atomic>
#include <chrono>
#include <cstring>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

typedef unsigned char byte;

using namespace GraftcodeGateway;
using namespace Graftcode::Plugins::ServiceBus;

namespace {
	using Json = nlohmann::json;
	namespace Amqp = Azure::Core::Amqp::_internal;
	namespace AmqpModels = Azure::Core::Amqp::Models;

	constexpr std::uint64_t kMaxMessageSize = 1024ULL * 1024ULL;
	constexpr std::uint32_t kDefaultRpcTimeoutMs = 30000;
	constexpr int kReceivePollMs = 1000;

	std::atomic_bool g_stopRequested{ false };

	struct ServiceBusServerConfig {
		std::string name{ "" };
		std::string connectionString{ "" };
		std::string host{ "" };
		std::string sharedAccessKeyName{ "" };
		std::string sharedAccessKey{ "" };
		std::string queue{ "" };
		std::string replyQueue{ "" };
		// Topic + subscription enable one-way mode: the server consumes from the
		// topic subscription and does NOT send a reply (void methods).
		std::string topic{ "" };
		std::string subscription{ "" };
		bool useDevelopmentEmulator{ false };
		std::uint32_t rpcTimeoutMs{ kDefaultRpcTimeoutMs };
	};

	ServiceBusServerConfig g_serverConfig;
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

	bool shouldStop() {
		return g_stopRequested.load(std::memory_order_acquire);
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

	std::string buildConnectionString(const ServiceBusServerConfig& config) {
		ServiceBusClientConfig clientConfig;
		clientConfig.connectionString = config.connectionString;
		clientConfig.host = config.host;
		clientConfig.sharedAccessKeyName = config.sharedAccessKeyName;
		clientConfig.sharedAccessKey = config.sharedAccessKey;
		clientConfig.useDevelopmentEmulator = config.useDevelopmentEmulator;
		return ServiceBusClient::BuildConnectionString(clientConfig);
	}

	void applyConfigFromJson(const std::string& jsonSource) {
		if (jsonSource.empty()) {
			throw std::runtime_error("Service Bus consumer: empty configuration JSON");
		}

		const Json parsed = Json::parse(normalizeRelaxedJson(jsonSource));
		if (!parsed.is_object()) {
			throw std::runtime_error("Service Bus consumer: configuration must be a JSON object");
		}

		ServiceBusServerConfig config;
		config.name = getOptionalString(parsed, "name", config.name);
		config.connectionString = getOptionalString(parsed, "connectionString", config.connectionString);
		config.host = getOptionalString(parsed, "host", config.host);
		config.sharedAccessKeyName = getOptionalString(parsed, "sharedAccessKeyName", config.sharedAccessKeyName);
		config.sharedAccessKey = getOptionalString(parsed, "sharedAccessKey", config.sharedAccessKey);
		config.queue = getOptionalString(parsed, "queue", config.queue);
		config.replyQueue = getOptionalString(parsed, "replyQueue", config.replyQueue);
		config.topic = getOptionalString(parsed, "topic", config.topic);
		config.subscription = getOptionalString(parsed, "subscription", config.subscription);

		const auto emulatorIt = parsed.find("useDevelopmentEmulator");
		if (emulatorIt != parsed.end() && emulatorIt->is_boolean()) {
			config.useDevelopmentEmulator = emulatorIt->get<bool>();
		}

		const auto timeoutIt = parsed.find("rpcTimeoutMs");
		if (timeoutIt != parsed.end() && timeoutIt->is_number_integer()) {
			const auto parsedTimeout = timeoutIt->get<int64_t>();
			if (parsedTimeout <= 0 || parsedTimeout > std::numeric_limits<std::uint32_t>::max()) {
				throw std::runtime_error("Service Bus consumer: invalid rpcTimeoutMs in configuration");
			}
			config.rpcTimeoutMs = static_cast<std::uint32_t>(parsedTimeout);
		}

		const bool topicMode = !config.subscription.empty();
		if (topicMode) {
			if (config.topic.empty()) {
				throw std::runtime_error(
					"Service Bus consumer: 'subscription' requires 'topic' to be set");
			}
		}
		else if (config.queue.empty()) {
			throw std::runtime_error(
				"Service Bus consumer: missing required field 'queue' (or 'topic'+'subscription')");
		}
		if (buildConnectionString(config).empty()) {
			throw std::runtime_error(
				"Service Bus consumer: missing 'connectionString' (or host/sharedAccessKeyName/sharedAccessKey)");
		}

		std::lock_guard<std::mutex> lock(g_serverConfigMutex);
		g_serverConfig = std::move(config);
	}

	ServiceBusServerConfig currentConfig() {
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

	std::vector<byte> extractMessageBody(const AmqpModels::AmqpMessage& message) {
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
			}
			break;
		}
		default:
			break;
		}
		return result;
	}

	std::optional<std::string> tryGetReplyTo(const AmqpModels::AmqpMessage& message) {
		const auto& replyTo = message.Properties.ReplyTo;
		if (replyTo.GetType() != AmqpModels::AmqpValueType::String) {
			return std::nullopt;
		}
		try {
			std::string value = static_cast<std::string>(replyTo);
			if (value.empty()) {
				return std::nullopt;
			}
			return value;
		}
		catch (...) {
			return std::nullopt;
		}
	}

	// The client puts the reply session id in ReplyToGroupId (Service Bus
	// ReplyToSessionId). We stamp it onto the response's GroupId so the broker
	// routes the reply into that session, which is what the client waits on.
	std::optional<std::string> tryGetReplySessionId(const AmqpModels::AmqpMessage& message) {
		const auto& replyToGroupId = message.Properties.ReplyToGroupId;
		if (!replyToGroupId.HasValue() || replyToGroupId.Value().empty()) {
			return std::nullopt;
		}
		return replyToGroupId.Value();
	}

	// Lazily-created, cached senders keyed by reply entity so each reply target reuses a link.
	class ReplySenderCache {
	public:
		explicit ReplySenderCache(Amqp::Session& session) : session_(session) {}

		bool publish(
			const std::string& replyEntity,
			const std::vector<byte>& responseBytes,
			const AmqpModels::AmqpValue& correlationId,
			const std::optional<std::string>& replySessionId) {
			Amqp::MessageSender* sender = acquire(replyEntity);
			if (sender == nullptr) {
				return false;
			}

			AmqpModels::AmqpMessage response;
			std::vector<std::uint8_t> body(responseBytes.begin(), responseBytes.end());
			response.SetBody(AmqpModels::AmqpBinaryData(body));
			response.Properties.CorrelationId = correlationId;
			// GroupId maps to the Service Bus SessionId. Stamping the client's
			// requested reply session id routes the response to that session, so
			// the originating client's session-filtered receiver picks it up.
			if (replySessionId.has_value()) {
				response.Properties.GroupId = replySessionId.value();
			}
			response.Properties.ContentType = std::string("application/octet-stream");

#if defined(ENABLE_RUST_AMQP) && ENABLE_RUST_AMQP
			return !static_cast<bool>(sender->Send(response));
#else
			const auto sendResult = sender->Send(response);
			return std::get<0>(sendResult) == Amqp::MessageSendStatus::Ok;
#endif
		}

		void closeAll() {
			for (auto& entry : senders_) {
				try {
					entry.second->Close();
				}
				catch (...) {
				}
			}
			senders_.clear();
		}

	private:
		Amqp::MessageSender* acquire(const std::string& replyEntity) {
			const auto it = senders_.find(replyEntity);
			if (it != senders_.end()) {
				return it->second.get();
			}

			Amqp::MessageSenderOptions senderOptions;
			senderOptions.Name = "graftcode-rpc-response";
			senderOptions.MessageSource = "graftcode-rpc-response-source";
			senderOptions.SettleMode = Amqp::SenderSettleMode::Settled;
			senderOptions.MaxMessageSize = kMaxMessageSize;

			auto sender = std::make_unique<Amqp::MessageSender>(
				session_.CreateMessageSender(replyEntity, senderOptions, nullptr));
			if (auto openError = sender->Open()) {
				(void)openError;
				return nullptr;
			}

			Amqp::MessageSender* raw = sender.get();
			senders_.emplace(replyEntity, std::move(sender));
			return raw;
		}

		Amqp::Session& session_;
		std::map<std::string, std::unique_ptr<Amqp::MessageSender>> senders_;
	};
}

void ServiceBusServer::configure(
	const std::string& jsonConfig,
	ProcessMessageFn processMessage) {
	if (processMessage == nullptr) {
		throw std::runtime_error("Service Bus consumer: processMessage callback is required");
	}
	applyConfigFromJson(jsonConfig);
	setProcessMessage(processMessage);
}

void ServiceBusServer::start() {
	try {
		g_stopRequested.store(false, std::memory_order_release);
		const ServiceBusServerConfig config = currentConfig();
		const std::string connectionString = buildConnectionString(config);

		// One-way mode: consume from a topic subscription and do not reply.
		const bool oneWay = !config.subscription.empty();
		const std::string sourceAddress = oneWay
			? (config.topic + "/Subscriptions/" + config.subscription)
			: config.queue;

		logInfo(
			"Service Bus consumer '" + config.name + "' is enabled. "
			+ (oneWay ? ("Subscription: " + sourceAddress + " (one-way)") : ("Queue: " + config.queue)));

		while (!shouldStop()) {
			try {
				auto credential = std::make_shared<Amqp::ServiceBusSasConnectionStringCredential>(connectionString);

				Amqp::ConnectionOptions connectionOptions;
				connectionOptions.ContainerId = "graftcode-servicebus-server";
				connectionOptions.EnableTrace = false;
				const std::uint16_t credentialPort = credential->GetPort();
				connectionOptions.Port = credentialPort != 0 ? credentialPort : Amqp::AmqpTlsPort;

				Amqp::Connection connection(credential->GetHostName(), credential, connectionOptions);

				Amqp::SessionOptions sessionOptions;
				sessionOptions.InitialIncomingWindowSize = (std::numeric_limits<std::int32_t>::max)();
				sessionOptions.InitialOutgoingWindowSize = (std::numeric_limits<std::uint16_t>::max)();
				Amqp::Session session(connection.CreateSession(sessionOptions));

				Amqp::MessageReceiverOptions receiverOptions;
				receiverOptions.Name = "graftcode-consumer";
				receiverOptions.MessageTarget = "graftcode-consumer-target";
				receiverOptions.SettleMode = Amqp::ReceiverSettleMode::First;
				receiverOptions.MaxMessageSize = kMaxMessageSize;
				receiverOptions.MaxLinkCredit = 1;
				Amqp::MessageReceiver receiver(session.CreateMessageReceiver(sourceAddress, receiverOptions, nullptr));
				receiver.Open();

				ReplySenderCache replySenders(session);
				logInfo("Service Bus consumer connection established");

				while (!shouldStop()) {
					const auto deadline = std::chrono::system_clock::now()
						+ std::chrono::milliseconds(kReceivePollMs);
					Azure::Core::Context context = Azure::Core::Context{}.WithDeadline(Azure::DateTime(deadline));

					std::pair<std::shared_ptr<const AmqpModels::AmqpMessage>, AmqpModels::_internal::AmqpError> incoming;
					try {
						incoming = receiver.WaitForIncomingMessage(context);
					}
					catch (...) {
						// Deadline elapsed without a message; loop so we can re-check the stop flag.
						continue;
					}

					if (incoming.second) {
						logWarn(std::string("Service Bus consumer receive error: ")
							+ incoming.second.Description);
						break;
					}
					const auto& message = incoming.first;
					if (!message) {
						continue;
					}

					std::vector<byte> request = extractMessageBody(*message);
					if (request.empty()) {
						logWarn("Service Bus consumer: received empty message body");
						continue;
					}

					const auto processMessage = currentProcessMessage();
					if (processMessage == nullptr) {
						throw std::runtime_error("Service Bus consumer: processMessage callback is not configured");
					}

					std::vector<byte> response;
					auto writeResponse = [](void* contextPtr, const byte* data, std::size_t size) {
						auto* out = static_cast<std::vector<byte>*>(contextPtr);
						if (out == nullptr) {
							return;
						}
						if (data == nullptr || size == 0) {
							out->clear();
							return;
						}
						out->assign(data, data + size);
						};

					if (!processMessage(request.data(), request.size(), writeResponse, &response)) {
						logWarn("Service Bus consumer: processMessage callback returned failure");
						continue;
					}

					if (oneWay) {
						// Fire-and-forget: the request targets a void method, so any
						// produced output is intentionally discarded and not published.
						logDebug("Service Bus consumer: processed one-way message ("
							+ std::to_string(request.size()) + "B)");
						continue;
					}

					const auto replyEntity = tryGetReplyTo(*message);
					const auto replySessionId = tryGetReplySessionId(*message);
					if (replyEntity.has_value()) {
						if (!replySessionId.has_value()) {
							logWarn("Service Bus consumer: request has no reply session id; "
								"reply queue must be session-enabled for correlation");
						}
						if (!replySenders.publish(
								replyEntity.value(), response, message->Properties.MessageId, replySessionId)) {
							logWarn("Service Bus consumer: failed to publish response to reply entity: "
								+ replyEntity.value());
						}
						else {
							logDebug("Service Bus consumer: processed message ("
								+ std::to_string(request.size()) + "B), response size: "
								+ std::to_string(response.size()) + "B");
						}
					}
					else {
						logWarn("Service Bus consumer: request has no reply_to, response was not published");
					}
				}

				replySenders.closeAll();
				try {
					receiver.Close();
				}
				catch (...) {
				}
			}
			catch (const std::exception& ex) {
				if (!shouldStop()) {
					logWarn(std::string("Service Bus consumer connection error: ") + ex.what());
				}
			}

			if (!shouldStop()) {
				logWarn("Service Bus consumer disconnected, retrying in 2 seconds");
				std::this_thread::sleep_for(std::chrono::seconds(2));
			}
		}

		logInfo("Service Bus consumer stopped");
	}
	catch (const std::exception& ex) {
		logWarn(std::string("Service Bus consumer error: ") + ex.what());
	}
	catch (...) {
		logWarn("Service Bus consumer stopped with unknown error");
	}
}

void ServiceBusServer::stop() {
	logInfo("Shutting down Service Bus consumer... ");
	g_stopRequested.store(true, std::memory_order_release);
}

#if defined(_WIN32)
#define SERVICEBUS_PLUGIN_EXPORT extern "C" __declspec(dllexport)
#else
#define SERVICEBUS_PLUGIN_EXPORT extern "C"
#endif

SERVICEBUS_PLUGIN_EXPORT GraftcodeGateway::IServer* CreateServer() {
	return new ServiceBusServer();
}

SERVICEBUS_PLUGIN_EXPORT void DestroyServer(GraftcodeGateway::IServer* server) {
	delete server;
}
