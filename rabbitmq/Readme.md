# RabbitMQ Plugin Build (CMake)

## 1) Clone repository

```bash
git clone https://github.com/grft-dev/graftcode-plugins.git
cd graftcode-plugins/rabbitmq
```

## 2) Configure with CMake

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
```

## 3) Build

```bash
cmake --build build --config Release
```

As a result, you will receive:
- `rabbitmq/build/RabbitmqPlugin/libRabbitmqPlugin.dll`
- or `rabbitmq/build/RabbitmqPlugin/RabbitmqPlugin.dll`

If the generated library is `libRabbitmqPlugin.dll`, use plugin name: `libRabbitmqPlugin`.

## 4) Download GG

Download `gg` from:
- https://github.com/grft-dev/graftcode-gateway/releases/

## 5) Run RabbitMQ in Docker

Build image from `rabbitmq/Dockerfile`:

```bash
docker build -t graftcode-rabbitmq .
```

Run container:

```bash
docker run -d --name graftcode-rabbitmq -p 5672:5672 -p 15672:15672 graftcode-rabbitmq
```

## 6) Create queues `myqueue` and `myqueue.reply`

Option A (RabbitMQ UI):
- Open `http://localhost:15672`
- Log in with `guest` / `guest`
- Go to **Queues and Streams** -> **Add a new queue**
- Set queue name to `myqueue` and click **Add queue**
- Repeat and create queue `myqueue.reply`

Option B (command line):

```bash
docker exec -it graftcode-rabbitmq rabbitmqadmin declare queue name=myqueue durable=true
docker exec -it graftcode-rabbitmq rabbitmqadmin declare queue name=myqueue.reply durable=true
```

## 7) Run GG with sample library

In your sample folder (for example `C:\DEV\Testing\20260610rabbitmq\samplelibrary`), create `pluginConfig.json`:

```json
{
  "name": "RabbitmqPlugin",
  "host": "localhost",
  "port": 5672,
  "queue": "myqueue",
  "replyQueue": "myqueue.reply",
  "user": "guest",
  "password": "guest",
  "vhost": "/",
  "rpcTimeoutMs": 30000
}
```

If you built `libRabbitmqPlugin.dll`, set:

```json
"name": "libRabbitmqPlugin"
```

Then run:

```powershell
./gg .\PhysicsCalculator.dll --config .\pluginConfig.json
```

## 8) Get installation command

Visit `http://localhost:81/GV`, select your package manager, and copy the generated installation command.

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
      "host": "localhost:5672",
      "stateless": true,
      "plugin": {
        "name": "RabbitmqPlugin",
        "queue": "myqueue",
        "replyQueue": "myqueue.reply",
        "user": "guest",
        "password": "guest",
        "vhost": "/",
        "rpcTimeoutMs": 30000
      }
    }
  }
}
""";

graft.nuget.PhysicsCalculator.GraftConfig.SetConfig(configSource);
```

