package rabbitmq

import (
	"fmt"
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Topology declares the full set of exchanges, queues, and bindings
// required for the main queue, retry queue, and dead-letter queue.
type Topology struct {
	cfg    QueueConfig
	logger *slog.Logger
}

// NewTopology creates a Topology manager.
func NewTopology(cfg QueueConfig, logger *slog.Logger) *Topology {
	return &Topology{
		cfg:    cfg,
		logger: logger.With("component", "rabbitmq.topology"),
	}
}

// Declare sets up all exchanges, queues, and bindings on the given channel.
// Topology:
//
//	Main Exchange ──(routing_key)──► Main Queue
//	                                   │ (handler error)
//	                 retries < max     │     retries >= max
//	                      ┌────────────┴──────────┐
//	                      ▼                       ▼
//	Retry Exchange ──► Retry Queue          DLX Exchange ──► DLQ
//	                   (TTL expiry)
//	                      │
//	                      └──► Main Exchange (re-delivered via DLX)
func (t *Topology) Declare(ch *amqp.Channel) error {
	// Phase 1: Declare all exchanges.
	if err := t.declareExchanges(ch); err != nil {
		return err
	}

	// Phase 2: Declare all queues.
	if err := t.declareQueues(ch); err != nil {
		return err
	}

	// Phase 3: Bind queues to exchanges.
	if err := t.declareBindings(ch); err != nil {
		return err
	}

	return nil
}

// --- Phase 1: Exchanges ---

func (t *Topology) declareExchanges(ch *amqp.Channel) error {
	// Main exchange — routes messages to the main queue;
	// also serves as the dead-letter exchange for the retry queue.
	if err := ch.ExchangeDeclare(
		t.cfg.Exchange,
		t.cfg.ExchangeType,
		t.cfg.Durable,
		false, // auto-delete
		false, // internal
		false, // no-wait
		nil,
	); err != nil {
		return fmt.Errorf("declare main exchange %q: %w", t.cfg.Exchange, err)
	}

	// DLX exchange — receives messages that exceeded max retries.
	if err := ch.ExchangeDeclare(
		t.cfg.DLXExchange,
		t.cfg.ExchangeType,
		t.cfg.Durable,
		false, // auto-delete
		false, // internal
		false, // no-wait
		nil,
	); err != nil {
		return fmt.Errorf("declare DLX exchange %q: %w", t.cfg.DLXExchange, err)
	}

	// Retry exchange — receives messages for delayed retry.
	if err := ch.ExchangeDeclare(
		t.cfg.RetryExchange,
		t.cfg.ExchangeType,
		t.cfg.Durable,
		false, // auto-delete
		false, // internal
		false, // no-wait
		nil,
	); err != nil {
		return fmt.Errorf("declare retry exchange %q: %w", t.cfg.RetryExchange, err)
	}

	return nil
}

// --- Phase 2: Queues ---

func (t *Topology) declareQueues(ch *amqp.Channel) error {
	queueType := t.cfg.QueueType
	if queueType == "" {
		queueType = "quorum"
	}
	isQuorum := queueType == "quorum"

	// Quorum queues require durable=true, auto-delete=false, exclusive=false.
	durable := t.cfg.Durable
	if isQuorum {
		durable = true
	}

	// Main queue — no dead-letter args; retry/DLQ routing is handled
	// explicitly in the consumer.
	mainArgs := amqp.Table{
		"x-queue-type": queueType,
	}
	if isQuorum && t.cfg.DeliveryLimit > 0 {
		mainArgs["x-delivery-limit"] = int64(t.cfg.DeliveryLimit)
	}
	if _, err := ch.QueueDeclare(
		t.cfg.Queue,
		durable,
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		mainArgs,
	); err != nil {
		return fmt.Errorf("declare main queue %q: %w", t.cfg.Queue, err)
	}

	// Dead-letter queue — terminal destination for poison messages.
	dlqArgs := amqp.Table{
		"x-queue-type": queueType,
	}
	if _, err := ch.QueueDeclare(
		t.cfg.DLQ,
		durable,
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		dlqArgs,
	); err != nil {
		return fmt.Errorf("declare DLQ %q: %w", t.cfg.DLQ, err)
	}

	// Retry queue — messages sit here for RetryTTL, then get dead-lettered
	// back to the main exchange for reprocessing. Declared last because its
	// args reference the main exchange (which must already exist).
	retryArgs := amqp.Table{
		"x-queue-type":              queueType,
		"x-dead-letter-exchange":    t.cfg.Exchange,
		"x-dead-letter-routing-key": t.cfg.RoutingKey,
		"x-message-ttl":             int64(t.cfg.RetryTTL.Milliseconds()),
	}
	if _, err := ch.QueueDeclare(
		t.cfg.RetryQueue,
		durable,
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		retryArgs,
	); err != nil {
		return fmt.Errorf("declare retry queue %q: %w", t.cfg.RetryQueue, err)
	}

	return nil
}

// --- Phase 3: Bindings ---

func (t *Topology) declareBindings(ch *amqp.Channel) error {
	// Main queue → main exchange.
	if err := ch.QueueBind(
		t.cfg.Queue,
		t.cfg.RoutingKey,
		t.cfg.Exchange,
		false, // no-wait
		nil,
	); err != nil {
		return fmt.Errorf("bind main queue to main exchange: %w", err)
	}

	// DLQ → DLX exchange.
	if err := ch.QueueBind(
		t.cfg.DLQ,
		t.cfg.RoutingKey,
		t.cfg.DLXExchange,
		false, // no-wait
		nil,
	); err != nil {
		return fmt.Errorf("bind DLQ to DLX exchange: %w", err)
	}

	// Retry queue → retry exchange.
	if err := ch.QueueBind(
		t.cfg.RetryQueue,
		t.cfg.RoutingKey,
		t.cfg.RetryExchange,
		false, // no-wait
		nil,
	); err != nil {
		return fmt.Errorf("bind retry queue to retry exchange: %w", err)
	}

	return nil
}
