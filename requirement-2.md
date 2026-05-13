# Producer and Consumer Construction Refactor

Refactor the producer and consumer architecture to become fully queue-centric with explicit dependency ownership and deterministic construction.

The implementation must remove ambiguous/shared topology behavior and make queue ownership explicit at the producer and consumer level.

The design must prioritize:
- explicit ownership
- deterministic lifecycle management
- minimal abstractions
- dependency clarity
- operational predictability
- code simplicity and elegance

The implementation must aggressively remove:
- redundant abstractions
- dead code
- unused configuration
- unnecessary interfaces
- duplicated lifecycle logic
- duplicated topology logic
- implicit orchestration behavior
- scattered ownership boundaries
- unnecessary wrappers and indirection

The resulting codebase should feel:
- cohesive
- minimal
- elegant
- production-oriented
- easy to reason about
- operationally maintainable

Avoid:
- hidden dependencies
- implicit shared queue ownership
- global topology assumptions
- loosely scoped producers/consumers
- service locator patterns
- abstraction for abstraction’s sake
- speculative extensibility

---

# 1. Queue-Dedicated Producer

Refactor the producer architecture so that each producer instance is dedicated to exactly one logical queue topology.

A producer must:
- publish only to its assigned queue topology
- own routing behavior for its assigned queue
- own retry/DLQ routing semantics for its assigned queue
- operate independently from other producers

The implementation must ensure:
- producers are queue-scoped
- topology ownership remains explicit
- routing behavior remains deterministic
- reconnect recovery remains isolated per producer
- producers cannot accidentally publish into unrelated queue topologies

The producer abstraction should represent:
- one queue topology
- one publish domain
- one routing responsibility

Avoid:
- multi-queue producer abstractions
- generic/global publish routing
- implicit exchange selection
- dynamically inferred topology ownership

---

# 2. Producer Constructor Contract

Refactor producer construction into an explicit dependency-injected constructor.

Producer creation must follow this contract:

```go
NewProducer(
    cm ConnectionManager,
    cfg ProducerConfig,
    queue QueueConfig,
    serializer Serializer,
    logger Logger,
)
```

Constructor responsibilities must include:
- validating configuration
- validating queue topology configuration
- initializing worker pool
- initializing bounded publish buffer
- initializing publish confirm handling
- binding producer ownership to exactly one queue topology

The constructor must fail fast on:
- invalid configuration
- invalid topology configuration
- nil critical dependencies
- invalid worker configuration
- invalid retry configuration where applicable

The implementation must:
- avoid hidden dependency creation
- avoid global singleton access
- avoid implicit serializer/logger selection
- keep dependency ownership explicit

---

# 3. Queue-Dedicated Consumer

Refactor the consumer architecture so that each consumer instance is dedicated to exactly one logical queue topology.

A consumer must:
- consume only from its assigned queue
- own retry handling for its assigned queue
- own DLQ behavior for its assigned queue
- recover independently after reconnect events

The implementation must ensure:
- consumers are queue-scoped
- retry ownership remains explicit
- consumer lifecycle remains isolated
- reconnect recovery remains deterministic
- consumers cannot accidentally consume unrelated queue topologies

The consumer abstraction should represent:
- one queue topology
- one consumption domain
- one retry ownership boundary

Avoid:
- multi-queue consumer abstractions
- shared retry ownership
- implicit queue selection
- globally shared consumer routing behavior

---

# 4. Consumer Constructor Contract

Refactor consumer construction into an explicit dependency-injected constructor.

Consumer creation must follow this contract:

```go
NewConsumer(
    cm ConnectionManager,
    cfg ConsumerConfig,
    queue QueueConfig,
    retry RetryConfig,
    serializer Serializer,
    logger Logger,
)
```

Constructor responsibilities must include:
- validating configuration
- validating retry configuration
- validating queue topology configuration
- initializing worker lifecycle state
- initializing QoS/prefetch behavior
- binding retry ownership to exactly one queue topology

The constructor must fail fast on:
- invalid configuration
- invalid retry policy
- nil critical dependencies
- invalid concurrency settings
- invalid topology configuration

The implementation must:
- avoid hidden dependency creation
- avoid implicit retry ownership
- avoid global retry/shared topology assumptions
- keep lifecycle ownership explicit and deterministic

---

# 5. Ownership and Lifecycle Guarantees

The implementation must explicitly enforce:
- one producer → one queue topology
- one consumer → one queue topology
- deterministic reconnect recovery ownership
- deterministic retry ownership
- deterministic DLQ ownership

The architecture must ensure:
- topology ownership remains explicit
- lifecycle boundaries remain isolated
- reconnect recovery cannot create duplicated ownership
- retry behavior remains queue-scoped
- operational debugging remains straightforward

The implementation must also:
- consolidate duplicated logic where appropriate
- centralize shared lifecycle coordination
- minimize unnecessary moving parts
- simplify reconnect orchestration
- simplify topology ownership
- simplify worker lifecycle management

The final design should feel:
- queue-centric
- deterministic
- explicit
- operationally predictable
- minimal
- elegant
- easy to maintain
- easy to debug under production failures