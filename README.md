# rabbitmq-rate-limiter

## Simple RabbitMQ demo

Start the consumer:

```sh
go run ./cmd/consumer
```

Start the producer HTTP server:

```sh
go run ./cmd/producer
```

Publish through HTTP:

```sh
curl -X POST http://localhost:8080/publish \
  -H 'Content-Type: application/json' \
  -d '{"message":"hello from http"}'
```

The demo uses these defaults when `.env` is not present:

```sh
SERVER_ADDR=:8080
RABBITMQ_ADDRESSES=localhost:5672
RABBITMQ_USERNAME=guest
RABBITMQ_PASSWORD=guest
RABBITMQ_EXCHANGE=demo.exchange
RABBITMQ_QUEUE=demo.queue
RABBITMQ_ROUTING_KEY=demo
RATELIMIT_REQUESTS_PER_MINUTE=0
RATELIMIT_BURST=1
```

Set `RATELIMIT_REQUESTS_PER_MINUTE` to enable consumer-side processing limits.
The configuration is expressed per minute, and the consumer converts it to the
per-second rate required by `golang.org/x/time/rate`.
