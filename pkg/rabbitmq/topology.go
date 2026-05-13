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
	if err := t.declareExchanges(ch); err != nil {
		return err
	}
	if err := t.declareQueues(ch); err != nil {
		return err
	}
	if err := t.declareBindings(ch); err != nil {
		return err
	}
	return nil
}

func (t *Topology) declareExchanges(ch *amqp.Channel) error {
	exchangeType := t.cfg.ExchangeType
	if exchangeType == "" {
		exchangeType = "direct"
	}

	// Main exchange
	if err := ch.ExchangeDeclare(
		t.cfg.Exchange,
		exchangeType,
		t.cfg.Durable,
		false, // auto-delete
		false, // internal
		false, // no-wait
		nil,
	); err != nil {
		return fmt.Errorf("declare main exchange %q: %w", t.cfg.Exchange, err)
	}

	// DLX exchange
	if err := ch.ExchangeDeclare(
		t.cfg.DLXExchange,
		exchangeType,
		t.cfg.Durable,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("declare DLX exchange %q: %w", t.cfg.DLXExchange, err)
	}

	// Retry exchange
	if err := ch.ExchangeDeclare(
		t.cfg.RetryExchange,
		exchangeType,
		t.cfg.Durable,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("declare retry exchange %q: %w", t.cfg.RetryExchange, err)
	}

	return nil
}

func (t *Topology) declareQueues(ch *amqp.Channel) error {
	queueType := t.cfg.QueueType
	if queueType == "" {
		queueType = "quorum"
	}
	isQuorum := queueType == "quorum"

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
		false,
		false,
		false,
		mainArgs,
	); err != nil {
		return fmt.Errorf("declare main queue %q: %w", t.cfg.Queue, err)
	}

	// Dead-letter queue
	dlqArgs := amqp.Table{
		"x-queue-type": queueType,
	}
	if _, err := ch.QueueDeclare(
		t.cfg.DLQ,
		durable,
		false,
		false,
		false,
		dlqArgs,
	); err != nil {
		return fmt.Errorf("declare DLQ %q: %w", t.cfg.DLQ, err)
	}

	// Retry queue — messages sit here for RetryTTL, then get dead-lettered
	// back to the main exchange for reprocessing.
	retryArgs := amqp.Table{
		"x-queue-type":              queueType,
		"x-dead-letter-exchange":    t.cfg.Exchange,
		"x-dead-letter-routing-key": t.cfg.RoutingKey,
	}
	if t.cfg.RetryTTL > 0 {
		retryArgs["x-message-ttl"] = int64(t.cfg.RetryTTL.Milliseconds())
	}
	if _, err := ch.QueueDeclare(
		t.cfg.RetryQueue,
		durable,
		false,
		false,
		false,
		retryArgs,
	); err != nil {
		return fmt.Errorf("declare retry queue %q: %w", t.cfg.RetryQueue, err)
	}

	return nil
}

func (t *Topology) declareBindings(ch *amqp.Channel) error {
	// Main queue → main exchange
	if err := ch.QueueBind(
		t.cfg.Queue,
		t.cfg.RoutingKey,
		t.cfg.Exchange,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("bind main queue to main exchange: %w", err)
	}

	// DLQ → DLX exchange
	if err := ch.QueueBind(
		t.cfg.DLQ,
		t.cfg.RoutingKey,
		t.cfg.DLXExchange,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("bind DLQ to DLX exchange: %w", err)
	}

	// Retry queue → retry exchange
	if err := ch.QueueBind(
		t.cfg.RetryQueue,
		t.cfg.RoutingKey,
		t.cfg.RetryExchange,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("bind retry queue to retry exchange: %w", err)
	}

	return nil
}
