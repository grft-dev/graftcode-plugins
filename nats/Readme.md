# NATS Docker Run Guide

## Build image

Run from `graftcode-plugins/nats`:

```bash
docker build -t graftcode-nats .
```

## Run container

```bash
docker run -d --name graftcode-nats -p 4222:4222 -p 8222:8222 graftcode-nats
```

This starts NATS with:
- JetStream enabled
- user: `guest`
- password: `guest`
- client port: `4222`
- monitoring port: `8222`

## Useful commands

Check logs:

```bash
docker logs graftcode-nats
```

Stop and remove container:

```bash
docker stop graftcode-nats
docker rm graftcode-nats
```
