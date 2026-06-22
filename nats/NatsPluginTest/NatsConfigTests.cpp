#include <gtest/gtest.h>

#include "NatsClient.h"

namespace
{
    const char* kRelaxedConfig = R"({
        name = "NatsPlugin",
        host = "127.0.0.1",
        port = 4222,
        queue = "gg",
        replyQueue = "gg.reply",
        user = "guest",
        password = "guest",
        vhost = "default",
        rpcTimeoutMs = 30000,
    })";

    const char* kNestedConfig = R"({
        "transport": {
            "nats": {
                "host": "127.0.0.1",
                "port": 4223,
                "queue": "demo.request",
                "replyQueue": "demo.reply",
                "user": "u",
                "password": "p",
                "rpcTimeoutMs": 1500
            }
        }
    })";
}

TEST(NatsClientConfig, ParsesRelaxedConfigStyle)
{
    Graftcode::Plugins::Nats::NatsClientConfig config;
    config.host = "192.168.0.10";
    config.port = 4221;

    Graftcode::Plugins::Nats::NatsClient::ApplyConfigSource(kRelaxedConfig, config);

    EXPECT_EQ(config.host, "127.0.0.1");
    EXPECT_EQ(config.port, 4222);
    EXPECT_EQ(config.queue, "gg");
    EXPECT_EQ(config.replyQueue, "gg.reply");
    EXPECT_EQ(config.user, "guest");
    EXPECT_EQ(config.password, "guest");
    EXPECT_EQ(config.vhost, "default");
    EXPECT_EQ(config.rpcTimeoutMs, 30000u);
}

TEST(NatsClientConfig, FindsNestedNatsNode)
{
    Graftcode::Plugins::Nats::NatsClientConfig config;
    Graftcode::Plugins::Nats::NatsClient::ApplyConfigSource(kNestedConfig, config);

    EXPECT_EQ(config.host, "127.0.0.1");
    EXPECT_EQ(config.port, 4223);
    EXPECT_EQ(config.queue, "demo.request");
    EXPECT_EQ(config.replyQueue, "demo.reply");
    EXPECT_EQ(config.user, "u");
    EXPECT_EQ(config.password, "p");
    EXPECT_EQ(config.rpcTimeoutMs, 1500u);
}
