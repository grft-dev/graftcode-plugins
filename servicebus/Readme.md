# Azure Service Bus Plugin Build (CMake)

This plugin is the Azure Service Bus counterpart of the RabbitMQ plugin. It implements the
same graftcode plugin interfaces (`Hypertube::Native::Interfaces::ITransport` for the calling
runtime and `GraftcodeGateway::IServer` for the gateway) and exposes the same exported
factory symbols (`CreateTransportChannel` / `DestroyTransportChannel` and
`CreateServer` / `DestroyServer`).

It is written in C++ and talks to Azure Service Bus over its native AMQP 1.0 protocol using
the Azure SDK for C++ AMQP library (`azure-core-amqp`), acquired through vcpkg.

## 1) Clone repository

```bash
git clone https://github.com/grft-dev/graftcode-plugins.git
cd graftcode-plugins/servicebus
```

## 2) Get vcpkg

The plugin depends on `azure-core-amqp-cpp` and `nlohmann-json`, declared in `vcpkg.json`.

```bash
git clone https://github.com/microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh    # on Windows: .\vcpkg\bootstrap-vcpkg.bat
```

## 3) Configure with CMake

Point CMake at the vcpkg toolchain so the dependencies are installed and discovered
automatically (manifest mode):

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_TOOLCHAIN_FILE=./vcpkg/scripts/buildsystems/vcpkg.cmake
```

## 4) Build

```bash
cmake --build build --config Release
```

As a result, you will receive:
- `servicebus/build/ServiceBusPlugin/ServiceBusPlugin.dll`
- or `servicebus/build/ServiceBusPlugin/libServiceBusPlugin.so` / `.dylib` on Linux/macOS

If the generated library is `libServiceBusPlugin.*`, use plugin name: `libServiceBusPlugin`.

## 5) Download GG

Download `gg` from:
- https://github.com/grft-dev/graftcode-gateway/releases/

## 6) Create the queues

In the Azure portal (or via Azure CLI) create two queues in your Service Bus namespace, for
example `myqueue` (requests) and `myqueue.reply` (responses). The **reply queue must be
session-enabled** (`--enable-session true`); the request queue does not need sessions:

```bash
az servicebus queue create --resource-group <rg> --namespace-name <ns> --name myqueue
az servicebus queue create --resource-group <rg> --namespace-name <ns> --name myqueue.reply --enable-session true
```

The request/reply pattern uses Service Bus **sessions** for correlation (the standard,
multi-client-safe approach):

- The client generates a unique reply session id per request and attaches its reply receiver
  with a `com.microsoft:session-filter` for that id, so it only ever receives its own response.
- The request carries that id in `reply-to-group-id` (Service Bus `ReplyToSessionId`) along with
  `reply_to` and `message-id`.
- The gateway stamps the response's `group-id` (Service Bus `SessionId`) with the requested
  reply session id and echoes the request's `message-id` into `correlation-id`, so the broker
  routes the reply into the correct session.

Because each client locks its own reply session, multiple clients can safely share one reply
queue without seeing each other's responses.

### Local development (Service Bus emulator)

The plugin honors the Service Bus emulator. Add `;UseDevelopmentEmulator=true` to the
connection string (or set `"useDevelopmentEmulator": true` in config) and point `host` at the
emulator endpoint. See the official emulator:
https://learn.microsoft.com/azure/service-bus-messaging/test-locally-with-service-bus-emulator

## 7) Run GG with sample library

In your sample folder, create `pluginConfig.json`:

```json
{
  "name": "ServiceBusPlugin",
  "connectionString": "Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=<keyName>;SharedAccessKey=<key>",
  "queue": "myqueue",
  "replyQueue": "myqueue.reply",
  "rpcTimeoutMs": 30000
}
```

If you built `libServiceBusPlugin.*`, set:

```json
"name": "libServiceBusPlugin"
```

Then run:

```powershell
./gg .\PhysicsCalculator.dll --config .\pluginConfig.json
```

## 8) Get installation command

Visit `http://localhost:81/GV`, select your package manager, and copy the generated
installation command.

Example for .NET:

```powershell
dotnet new console
dotnet add package -s https://grft.dev/019cf6aa-e2e0-74e7-a2b0-be30db97ccb5__graftcode graft.nuget.physicscalculator --version 1.0.0
```

## 9) Configure Graft after installation

Use this configuration:

```csharp
string configSource =
"""
{
  "configurations": {
    "graft.nuget.PhysicsCalculator": {
      "runtime": "netcore",
      "stateless": true,
      "plugin": {
        "name": "ServiceBusPlugin",
        "connectionString": "Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=<keyName>;SharedAccessKey=<key>",
        "queue": "myqueue",
        "replyQueue": "myqueue.reply",
        "rpcTimeoutMs": 30000
      }
    }
  }
}
""";

graft.nuget.PhysicsCalculator.GraftConfig.SetConfig(configSource);
```

## Configuration reference

| Field | Required | Description |
|-------|----------|-------------|
| `connectionString` | yes* | Full Service Bus connection string. |
| `host` | * | Namespace host (e.g. `<ns>.servicebus.windows.net`); used when `connectionString` is absent. |
| `sharedAccessKeyName` | * | SAS policy name; used with `host`/`sharedAccessKey`. |
| `sharedAccessKey` | * | SAS key; used with `host`/`sharedAccessKeyName`. |
| `queue` | yes (RPC) | Queue requests are sent to / consumed from in request/reply mode. |
| `replyQueue` | yes (RPC client) | Session-enabled queue responses are read from / published to. |
| `topic` | yes (one-way) | Topic the client publishes to in one-way (fire-and-forget) mode. |
| `subscription` | yes (one-way server) | Subscription the server consumes from in one-way mode (requires `topic`). |
| `rpcTimeoutMs` | no | Request/response timeout in milliseconds (default 30000). |
| `useDevelopmentEmulator` | no | Set to `true` to target the local Service Bus emulator. |

\* Provide either `connectionString` **or** the `host` + `sharedAccessKeyName` + `sharedAccessKey` trio.

## Messaging modes

The plugin supports two modes, selected purely by configuration:

### Request/reply (RPC) over queues — default

Set `queue` (and `replyQueue` on the client). The client sends a request and waits for a
correlated response (see the session-based correlation described above). Use this for methods
that return a value.

### One-way (fire-and-forget) over a topic

Set `topic` on the **client** and `topic` + `subscription` on the **server**. The client
publishes to the topic and returns immediately **without waiting for a reply**; the server
consumes from the topic subscription and, by design, **does not send any response**. Use this
for `void` methods that return nothing.

- When `topic` is set on the client it takes precedence over the queue RPC path.
- The server consumes from `"<topic>/Subscriptions/<subscription>"`; the reply queue and
  sessions are not involved at all.
- Multiple subscriptions on the same topic let several independent consumers each receive
  their own copy of the published message (pub/sub).

Client (one-way) config example:

```json
{
  "name": "ServiceBusPlugin",
  "connectionString": "Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=<keyName>;SharedAccessKey=<key>",
  "topic": "mytopic"
}
```

Server (one-way) config example:

```json
{
  "name": "ServiceBusPlugin",
  "connectionString": "Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=<keyName>;SharedAccessKey=<key>",
  "topic": "mytopic",
  "subscription": "mysubscription"
}
```

Create the topic and subscription (the subscription does **not** need sessions):

```bash
az servicebus topic create --resource-group <rg> --namespace-name <ns> --name mytopic
az servicebus topic subscription create --resource-group <rg> --namespace-name <ns> --topic-name mytopic --name mysubscription
```
