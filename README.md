# rabbitmq-rate-limiter

A production-grade, reusable RabbitMQ integration library for Go. Provides resilient, fault-tolerant, high-performance messaging with automatic recovery, retry/DLQ patterns, and graceful shutdown.

## Features

- **Automatic reconnection** with exponential backoff + jitter
- **Producer** with worker pool, publish confirms, buffered queue, and transient failure retry
- **Consumer** with bounded concurrency, QoS/prefetch, panic recovery, and manual ack
- **Retry/DLQ** via TTL + DLX delayed retry pattern with configurable policies
- **Topology management** — idempotent declaration with automatic redeclaration after reconnect
- **Health checks** — periodic connection health monitoring
- **Graceful shutdown** — ordered drain of consumers → producers → connection
- **Serialization abstraction** — JSON and raw byte serializers included

## Architecture

```
pkg/rabbitmq/
├── client.go       # Top-level Client orchestrating all components
├── config.go       # Configuration types and defaults
├── connection.go   # ConnectionManager with reconnect loop
├── channel.go      # ChannelManager with safe concurrent access
├── topology.go     # TopologyDeclarer for exchanges, queues, bindings, retry/DLQ
├── producer.go     # Producer with worker pool and publish confirms
├── consumer.go     # Consumer with bounded workers and retry integration
├── serializer.go   # Serializer interface + JSON/Raw implementations
└── health.go       # HealthChecker for connection monitoring

examples/
├── producer/       # Publishing example
├── consumer/       # Consuming example
└── retry-dlq/      # Retry and dead-letter queue demo
```

### Component Ownership

| Component | Responsibility |
|---|---|
| `Client` | Lifecycle orchestration, reconnect recovery coordination |
| `ConnectionManager` | AMQP connection, reconnect loop, listener notification |
| `ChannelManager` | Channel creation, recreation, concurrent-safe access |
| `TopologyDeclarer` | Exchange/queue/binding declaration, retry topology |
| `Producer` | Worker pool, buffered publishing, confirm tracking |
| `Consumer` | Message dispatch, panic recovery, retry/DLQ routing |
| `HealthChecker` | Periodic health probes |

## Quick Start

```go
package main

import (
    "context"
    "log/slog"
    "os"

    rmq "github.com/christopherwilliam/rabbitmq-rate-limiter/pkg/rabbitmq"
)

func main() {
    logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
    cfg := rmq.DefaultConfig("amqp://guest:guest@localhost:5672/")

    client, _ := rmq.NewClient(cfg, logger)
    client.Connect(context.Background())
    defer client.Close()

    // Declare topology
    client.DeclareTopology(rmq.TopologyConfig{
        Exchanges: []rmq.ExchangeConfig{{Name: "events", Kind: "topic", Durable: true}},
        Queues:    []rmq.QueueConfig{{Name: "events.orders", Durable: true}},
        Bindings:  []rmq.BindingConfig{{QueueName: "events.orders", ExchangeName: "events", RoutingKey: "order.#"}},
    })

    // Producer
    producer := client.NewProducer(rmq.JSONSerializer{})
    producer.Start()
    producer.Publish(context.Background(), rmq.PublishMessage{
        Exchange: "events", RoutingKey: "order.created",
        Body: map[string]interface{}{"id": "123"},
    })

    // Consumer
    consumer := client.NewConsumer("events.orders", "my-consumer",
        func(ctx context.Context, msg *rmq.Delivery) error {
            slog.Info("received", "body", string(msg.Body))
            return nil
        })
    consumer.Start()
}
```

## Configuration

### Default Configuration

```go
cfg := rmq.DefaultConfig("amqp://guest:guest@localhost:5672/")
```

| Parameter | Default | Description |
|---|---|---|
| `Heartbeat` | 30s | AMQP heartbeat interval |
| `Connection.InitialReconnectDelay` | 1s | First reconnect delay |
| `Connection.MaxReconnectDelay` | 30s | Maximum reconnect delay |
| `Connection.ReconnectMultiplier` | 2.0 | Exponential backoff multiplier |
| `Connection.MaxReconnectAttempts` | 0 (unlimited) | Max reconnect tries |
| `Producer.Workers` | 4 | Publish worker goroutines |
| `Producer.BufferSize` | 1024 | Publish buffer capacity |
| `Producer.PublishTimeout` | 10s | Per-publish timeout |
| `Producer.MaxRetries` | 3 | Transient publish failure retries |
| `Consumer.PrefetchCount` | 10 | QoS prefetch |
| `Consumer.Workers` | 4 | Consumer worker goroutines |
| `Retry.MaxRetries` | 3 | Max delivery retry attempts |
| `Retry.Policy` | Exponential | Fixed or Exponential |
| `Retry.BaseDelay` | 1s | Base retry delay |
| `Retry.MaxDelay` | 60s | Maximum retry delay |

### Tuning Recommendations

- **High throughput**: Increase `Producer.Workers` (8-16), `BufferSize` (4096+), `Consumer.Workers` (8-16)
- **Low latency**: Keep `Producer.Workers` low (2-4), reduce `BufferSize` (256)
- **Reliability over speed**: Keep `PrefetchCount` low (1-5), use synchronous `Publish()`
- **Quorum queues**: Use `rmq.QuorumQueueArgs()` for queue arguments

## Flows

### Reconnect Flow

```
Connection Lost
    → ConnectionManager detects via NotifyClose channel
    → State → Disconnected
    → Enter reconnect loop
        → Calculate delay (exponential backoff + jitter)
        → Sleep for delay
        → Attempt connection
        → On failure: increment attempt, retry
        → On success:
            → State → Connected
            → Notify reconnect listeners
                → Client.onReconnect()
                    → Recreate management channel
                    → Redeclare topology (idempotent)
                    → Recover all producers (recreate channels, re-enable confirms)
                    → Recover all consumers (recreate channels, restart delivery)
```

### Retry Flow

```
Message Received by Consumer
    → Handler executes (with panic recovery)
    → Handler returns error
        → Check retry count from x-retry-count header
        → If retryCount < maxRetries:
            → Calculate delay (exponential + jitter)
            → Publish to retry exchange with:
                - x-retry-count incremented
                - message TTL = delay (per-message expiration)
            → Ack original delivery
            → Message expires in retry queue
            → DLX routes back to original exchange
            → Message redelivered to consumer
        → If retryCount >= maxRetries:
            → Publish to DLQ exchange with:
                - x-final-error header
                - x-dead-lettered-at timestamp
            → Ack original delivery
            → Message is permanently stored in DLQ
```

### Graceful Shutdown Flow

```
Client.Close() called
    → Cancel consumers (stop accepting new messages)
    → Consumer workers drain in-flight handlers
    → Wait for all consumer workers to finish
    → Cancel producers (stop accepting new publishes)
    → Producer workers drain buffered messages
    → Wait for all producer workers to finish
    → Close management channel
    → Close AMQP connection
    → Connection manager stops reconnect loop
```

## Failure Mode Analysis

| Failure | Behavior |
|---|---|
| **Broker down** | Reconnect loop with exponential backoff; producers buffer messages up to buffer size; publishes return error when buffer full |
| **Network partition** | Detected via heartbeat timeout or NotifyClose; triggers reconnect |
| **Channel error** | ChannelManager detects closed channel, recreates on next access |
| **Handler panic** | Recovered; message is nacked or routed to retry/DLQ; worker continues |
| **Slow consumer** | Bounded by prefetch; other messages wait in broker; no goroutine explosion |
| **Publish confirm timeout** | Returns error to caller; caller can retry |
| **Poison message** | After max retries, routed to DLQ with error metadata |
| **Shutdown during processing** | In-flight handlers complete; buffered publishes are drained |
| **Concurrent reconnect** | Single reconnect loop; no racing reconnect goroutines |

## Concurrency & Lifecycle Model

### Goroutine Inventory

| Goroutine | Count | Lifetime | Purpose |
|---|---|---|---|
| Reconnect loop | 1 | Connection lifetime | Watch NotifyClose, trigger reconnect |
| Producer workers | N (configurable) | Producer lifetime | Drain publish buffer, execute publishes |
| Consumer workers | N (configurable) | Consumer lifetime | Process deliveries from shared channel |
| Health checker | 1 | HealthChecker lifetime | Periodic connection probes |

### Synchronization

- `ConnectionManager`: `sync.RWMutex` protects connection pointer; `atomic.Int32` for state; `sync.Once` for close
- `ChannelManager`: `sync.Mutex` for channel creation/access
- `Producer`: `atomic.Bool` for closed flag; `sync.Once` for close; buffered channel for message queue
- `Consumer`: `atomic.Bool` for closed flag; `sync.WaitGroup` for worker draining

### Backpressure

- Producer: bounded buffer channel (`BufferSize`); `Publish()` blocks when full; `PublishAsync()` returns error
- Consumer: bounded by `PrefetchCount`; broker holds messages until ack'd
- No unbounded queues or goroutine spawning

## Design Decisions & Tradeoffs

1. **Single reconnect loop** — Simpler than per-component reconnect; avoids races; single source of truth for connection state.

2. **Per-message TTL for retries** — Using message expiration instead of queue TTL allows different delays per retry attempt (exponential backoff). Tradeoff: requires per-retry-attempt publish.

3. **Ack-then-republish for retries** — Message is ack'd and republished to retry queue. If the republish fails after ack, the message is lost. This is an acceptable tradeoff vs. the alternative of nack+requeue which doesn't support delayed retries.

4. **Shared delivery channel** — Consumer workers share a single `<-chan amqp.Delivery`. This is safe and provides natural load balancing. Tradeoff: ordering is not guaranteed across workers.

5. **Synchronous Publish with confirms** — `Publish()` waits for broker confirmation. Higher latency but guarantees delivery. Use `PublishAsync()` for fire-and-forget.

6. **Channel reuse** — Channels are reused until they fail, minimizing channel churn. Recreated automatically on error.

## Testing Strategy

### Reconnect Behavior
- Start client → kill broker → verify reconnect loop fires → restart broker → verify recovery
- Verify topology is redeclared after reconnect
- Verify producer and consumer resume after reconnect
- Test max reconnect attempts

### Shutdown Behavior
- Publish messages → call Close() → verify all buffered messages are drained
- Start consumer → send messages → call Close() → verify in-flight handlers complete
- Verify Close() is idempotent

### Retry Semantics
- Send message → handler returns error → verify message appears in retry queue with incremented count
- Exceed max retries → verify message routes to DLQ with error metadata
- Verify exponential backoff delays are applied

### Failure Scenarios
- Handler panic → verify worker recovers and continues
- Publish to closed channel → verify channel recreation
- Concurrent Publish from multiple goroutines → verify no races (run with `-race`)

### Load & Concurrency
- Publish 10k messages with 8 workers → verify all delivered
- Consume with varying worker counts → verify no starvation
- Run with `-race` flag to detect data races

## Operational Guidance

### Monitoring

Use `HealthChecker` for liveness/readiness probes:

```go
hc := rmq.NewHealthChecker(client.ConnectionManager(), 10*time.Second)
hc.Start()
defer hc.Stop()

// In health endpoint:
status := hc.Status()
if !status.Healthy {
    // return 503
}
```

### DLQ Processing

Periodically inspect and process DLQ messages:
- Check `x-final-error` header for failure reason
- Check `x-retry-count` for attempt count
- Replay messages by republishing to original exchange

### Quorum Queues

For higher durability, use quorum queues:

```go
topology := rmq.TopologyConfig{
    Queues: []rmq.QueueConfig{
        {Name: "critical.queue", Durable: true, Args: rmq.QuorumQueueArgs()},
    },
}
```

## Requirements

- Go 1.24+
- RabbitMQ 3.8+ (for quorum queues: 3.8+, for streams: 3.9+)
- `github.com/rabbitmq/amqp091-go`

## License

MIT