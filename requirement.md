# Primary Objective

Build a production-grade, reusable infrastructure middleware/library in Go that provides resilient, fault-tolerant, and high-performance RabbitMQ integration for distributed systems.

The implementation must prioritize, in strict order:

1. correctness
2. reliability
3. maintainability
4. performance optimization

Keep the design intentionally minimal and pragmatic. Implement only functionality required for the primary production use cases. Avoid overengineering.

---

# Technical Constraints

- Use Go 1.24+
- Use `github.com/rabbitmq/amqp091-go`
- Follow idiomatic Go conventions
- Follow Clean Architecture and SOLID principles where they improve clarity and maintainability
- Optimize for low allocations and minimal lock contention

Strictly avoid:
- unnecessary abstractions
- goroutine leaks
- deadlocks
- unbounded queues
- busy-loop retries
- hidden panics
- unsafe channel usage
- channel blocking hazards
- uncontrolled retry storms

The library must support:
- safe concurrent usage
- automatic recovery from broker/network failures
- reconnect resilience
- automatic producer and consumer recovery after reconnect
- prevention of message loss under expected failure conditions
- graceful shutdown and draining semantics

---

# Architecture Requirements

Use a single flat package:

```text
pkg/rabbitmq
```

The package must include:
- configuration
- connection management
- channel management
- producer
- consumer
- topology management
- retry/DLQ handling
- serialization
- health checks
- graceful shutdown

Favor cohesive components and clear ownership boundaries over deep package hierarchies.

---

# Connection Management Requirements

Implement:
- connection lifecycle management
- automatic reconnect loop
- exponential backoff with jitter
- heartbeat support
- reconnect event detection
- stale connection detection
- health check support

Reconnect behavior must:
- prevent concurrent reconnect races
- ensure only one active reconnect routine exists
- recover producers automatically
- recover consumers automatically
- redeclare topology after reconnect

Failure handling must:
- avoid hot reconnect loops
- preserve application stability during broker outages
- avoid leaking stale channels or goroutines

---

# Channel Management Requirements

Implement:
- automatic channel recreation
- safe concurrent channel access
- reusable channels for multiple producers/consumers where appropriate

Channel handling must:
- detect closed/invalid channels safely
- prevent concurrent misuse of AMQP channels
- minimize channel churn under stable conditions
- recover cleanly after reconnect

---

# Topology Management Requirements

Support exchanges:
- direct
- topic
- fanout
- headers

Support queues:
- classic
- quorum

Implement:
- topology declaration
- topology redeclaration after reconnect
- retry queue creation
- DLQ creation
- TTL + DLX delayed retry pattern

Topology management must:
- be idempotent
- tolerate reconnect/redeclare scenarios safely
- avoid inconsistent topology state

---

# Producer Requirements

Implement:
- internal worker pool
- configurable worker count
- buffered publish queue
- publish confirms
- transient failure retry
- context-aware publishing
- configurable exchange/routing
- mandatory flag support
- serialization abstraction

Producer behavior must:
- avoid unbounded memory growth
- avoid publish deadlocks
- support graceful draining
- preserve message ordering where applicable within a worker
- handle confirm tracking safely under reconnect scenarios

Publish retry behavior must:
- retry only transient failures
- respect context cancellation and deadlines
- avoid duplicate publish amplification where possible

---

# Consumer Requirements

Implement:
- configurable concurrency
- QoS/prefetch support
- panic recovery in handlers
- nack/requeue policies
- retry handling
- manual ack support
- graceful drain on shutdown
- context-aware cancellation

Worker model must:
- avoid message starvation
- avoid goroutine explosion
- support bounded concurrency
- isolate worker failures

Consumer lifecycle must:
- recover automatically after reconnect
- resume consumption safely
- avoid duplicate consumer registration
- stop cleanly during shutdown

Handler execution must:
- recover from panics safely
- prevent poisoned workers
- preserve broker stability under slow consumers

---

# Retry & DLQ Requirements

Implement:
- delayed retries using TTL + DLX
- configurable retry policies
- max retry limits
- poison-message handling
- DLQ routing support

Retry policies must support:
- exponential backoff
- fixed delay
- jitter

Retry handling must:
- avoid infinite retry loops
- preserve retry metadata
- route permanently failed messages to DLQ
- tolerate reconnect scenarios safely

---

# Graceful Shutdown Requirements

Shutdown behavior must:
- stop accepting new work
- drain internal queues
- wait for in-flight handlers
- flush publish confirms where possible
- close channels safely
- close connections safely
- respect context deadlines/timeouts

Shutdown implementation must:
- be idempotent
- avoid hanging indefinitely
- avoid dropping acknowledged in-flight work where possible
- coordinate producer and consumer termination correctly

---

# Performance Requirements

Target characteristics:
- high-throughput publishing
- bounded memory usage
- stable performance during reconnect conditions
- low lock contention
- efficient goroutine utilization
- predictable backpressure behavior

Performance design must:
- minimize unnecessary allocations
- minimize synchronization overhead
- avoid excessive channel creation
- avoid unnecessary copying of payloads
- maintain stable throughput under moderate failure scenarios

---

# Reliability & Safety Expectations

The implementation must explicitly address:
- broker outages
- network partitions
- reconnect storms
- partial shutdown scenarios
- blocked publishers
- slow consumers
- consumer handler panics
- confirm channel failures
- stale AMQP objects
- context cancellation races

The design must favor correctness and recoverability over maximizing throughput at all costs.

---

# Deliverables

Provide:

1. Complete production-ready implementation
2. Full folder structure
3. Architecture explanation
4. Key design decisions and rationale
5. Example applications:
   - producer
   - consumer
   - retry/DLQ
6. README
7. Operational guidance
8. Configuration guidance and tuning recommendations
9. Sequence diagrams or detailed flow explanations for:
   - reconnect flow
   - retry flow
   - graceful shutdown flow
10. Failure-mode analysis
11. Tradeoff explanations
12. Concurrency and lifecycle model explanation
13. Testing strategy recommendations for:
   - reconnect behavior
   - shutdown behavior
   - retry semantics
   - failure scenarios
   - load/concurrency validation