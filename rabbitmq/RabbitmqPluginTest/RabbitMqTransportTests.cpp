#include <gtest/gtest.h>

#include "ITransport.h"
#include "RabbitMqClient.h"
#include <map>
#include <shared_mutex>
#include <thread>
#include <mutex>
#include "TransportRabbitMq.h"

#include <memory>
#include <stdexcept>
#include <vector>

extern "C" Hypertube::Native::Interfaces::ITransport* CreateTransportChannel(
	const char* ipAddress,
	unsigned short port,
	const char* configSource);
extern "C" void DestroyTransportChannel(Hypertube::Native::Interfaces::ITransport* transport);

namespace {
	const char* kValidClientConfig = R"({
			"host": "127.0.0.1",
			"port": 5672,
			"queue": "gg",
			"replyQueue": "gg.reply",
			"user": "guest",
			"password": "guest",
			"vhost": "/",
			"rpcTimeoutMs": 30000
})";

	const std::vector<byte> kSamplePayload{
		0x03, 0x00, 0x00, 0x7f, 0x00, 0x00, 0x01, 0x28, 0x16, 0x08, 0x06, 0x00,
		0x01, 0x00, 0x00, 0x00, 0x03, 0x00, 0x01, 0x01, 0x13, 0x00, 0x00, 0x00,
		0x54, 0x65, 0x73, 0x74, 0x43, 0x6c, 0x61, 0x73, 0x73, 0x2e, 0x54, 0x65,
		0x73, 0x74, 0x43, 0x6c, 0x61, 0x73, 0x73
	};

	const std::vector<byte> kReadResponsePayload{
		0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x07, 0x01,
		0x01, 0x24, 0x00, 0x00, 0x00, 0x30, 0x39, 0x39, 0x34, 0x36, 0x36, 0x39,
		0x66, 0x2d, 0x32, 0x61, 0x31, 0x30, 0x2d, 0x34, 0x36, 0x65, 0x35, 0x2d,
		0x61, 0x39, 0x31, 0x61, 0x2d, 0x38, 0x38, 0x38, 0x34, 0x31, 0x63, 0x34,
		0x63, 0x33, 0x63, 0x62, 0x34
	};
}

TEST(RabbitMqTransport, SendCommandRejectsInvalidPayloadArguments) {
	Hypertube::Native::Transport::TransportRabbitMq transport("127.0.0.1", 5672, kValidClientConfig);
	for (int i = 0; i < 300; ++i) {

		int responseSize = transport.SendCommand(const_cast<byte*>(kSamplePayload.data()), static_cast<int32_t>(kSamplePayload.size()));
		std::vector<byte> actualResponse(responseSize);
		transport.ReadResponse(actualResponse.data(), static_cast<int32_t>(actualResponse.size()));
		EXPECT_EQ(actualResponse.size(), kReadResponsePayload.size());
	}
}

