package rabbitmq

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Consumer reads messages from a RabbitMQ queue, dispatches them to a
// configurable worker pool, and handles retry/DLQ routing on failure.
type Consumer struct {
	cfg    QueueConfig
	conn   *Connection
	logger *slog.Logger

	mu      sync.Mutex
	channel *amqp.Channel

	closed atomic.Bool
	done   chan struct{}

	topology *Topology

	handler HandlerFunc
}

// NewConsumer creates a consumer with the given handler.
func NewConsumer(cfg QueueConfig, conn *Connection, logger *slog.Logger, handler HandlerFunc) *Consumer {
	return &Consumer{
		cfg:      cfg,
		conn:     conn,
		logger:   logger.With("component", "rabbitmq.consumer"),
		done:     make(chan struct{}),
		topology: NewTopology(cfg, logger),
		handler:  handler,
	}
}

// Start declares topology, sets QoS, and launches the worker pool.
func (c *Consumer) Start(ctx context.Context) error {
	if err := c.setup(); err != nil {
		return fmt.Errorf("consumer setup: %w", err)
	}

	deliveries, err := c.consume()
	if err != nil {
		return fmt.Errorf("consumer start: %w", err)
	}

	c.startWorkers(ctx, deliveries)
	go c.watchReconnect(ctx)

	c.logger.Info("Consumer started",
		"queue", c.cfg.Queue,
		"worker_count", c.cfg.WorkerCount,
		"prefetch_count", c.cfg.PrefetchCount,
	)
	return nil
}

// Close gracefully shuts down the consumer.
func (c *Consumer) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	close(c.done)

	c.mu.Lock()
	ch := c.channel
	c.mu.Unlock()

	if ch != nil {
		if err := ch.Cancel(c.consumerTag(), false); err != nil {
			c.logger.Warn("Error cancelling consumer", "error", err)
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.channel != nil {
		err := c.channel.Close()
		c.channel = nil
		return err
	}
	return nil
}

func (c *Consumer) setup() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed.Load() {
		return ErrConsumerClosed
	}

	if c.channel != nil {
		c.channel.Close()
		c.channel = nil
	}

	conn := c.conn.RawConnection()
	if conn == nil {
		return ErrNotConnected
	}

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}

	if err := c.topology.Declare(ch); err != nil {
		ch.Close()
		return err
	}

	if err := ch.Qos(c.cfg.PrefetchCount, 0, false); err != nil {
		ch.Close()
		return fmt.Errorf("set qos: %w", err)
	}

	c.channel = ch
	return nil
}

func (c *Consumer) consume() (<-chan amqp.Delivery, error) {
	c.mu.Lock()
	ch := c.channel
	c.mu.Unlock()

	deliveries, err := ch.Consume(
		c.cfg.Queue,
		c.consumerTag(),
		false, // auto-ack (manual ack)
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("consume queue %q: %w", c.cfg.Queue, err)
	}
	return deliveries, nil
}

func (c *Consumer) startWorkers(ctx context.Context, deliveries <-chan amqp.Delivery) {
	workerCount := c.cfg.WorkerCount
	if workerCount < 1 {
		workerCount = 1
	}
	for i := 0; i < workerCount; i++ {
		go c.worker(ctx, deliveries)
	}
}

func (c *Consumer) worker(ctx context.Context, deliveries <-chan amqp.Delivery) {
	for {
		select {
		case d, ok := <-deliveries:
			if !ok {
				return
			}
			c.processDelivery(ctx, d)
		case <-c.done:
			return
		}
	}
}

func (c *Consumer) processDelivery(ctx context.Context, d amqp.Delivery) {
	defer func() {
		if r := recover(); r != nil {
			c.logger.Error("Panic in message handler",
				"message_id", d.MessageId,
				"panic", r,
			)
			_ = d.Nack(false, false)
		}
	}()

	retryCount := getRetryCount(d)
	err := c.handler(ctx, Delivery{Delivery: d, RetryCount: retryCount})
	if err == nil {
		if ackErr := d.Ack(false); ackErr != nil {
			c.logger.Error("Failed to ack message", "error", ackErr, "message_id", d.MessageId)
		}
		return
	}

	if retryCount >= int64(c.cfg.MaxRetries) {
		c.republish(ctx, d, c.cfg.DLXExchange, d.Headers, false)
	} else {
		headers := copyHeaders(d.Headers)
		headers["x-retry-count"] = retryCount + 1
		c.republish(ctx, d, c.cfg.RetryExchange, headers, true)
	}
}

// republish forwards a delivery to the given exchange, then acks the original.
// On publish failure, nacks with requeue controlled by requeueOnFail.
func (c *Consumer) republish(ctx context.Context, d amqp.Delivery, exchange string, headers amqp.Table, requeueOnFail bool) {
	c.mu.Lock()
	ch := c.channel
	c.mu.Unlock()

	if ch == nil {
		c.logger.Error("No channel available for republish", "message_id", d.MessageId, "exchange", exchange)
		_ = d.Nack(false, requeueOnFail)
		return
	}

	err := ch.PublishWithContext(ctx, exchange, c.cfg.RoutingKey, false, false,
		amqp.Publishing{
			Headers:      headers,
			MessageId:    d.MessageId,
			ContentType:  d.ContentType,
			Body:         d.Body,
			DeliveryMode: amqp.Persistent,
		},
	)
	if err != nil {
		c.logger.Error("Failed to republish", "error", err, "message_id", d.MessageId, "exchange", exchange)
		_ = d.Nack(false, requeueOnFail)
		return
	}

	if ackErr := d.Ack(false); ackErr != nil {
		c.logger.Error("Failed to ack after republish", "error", ackErr, "message_id", d.MessageId)
	}
}

func (c *Consumer) watchReconnect(ctx context.Context) {
	reconnect, unsubscribe := c.conn.NotifyReconnect()
	defer unsubscribe()
	for {
		select {
		case _, ok := <-reconnect:
			if !ok || c.closed.Load() {
				return
			}
			c.logger.Info("Re-establishing consumer after reconnect")

			if err := c.setup(); err != nil {
				c.logger.Error("Failed to re-setup consumer", "error", err)
				continue
			}
			deliveries, err := c.consume()
			if err != nil {
				c.logger.Error("Failed to re-consume after reconnect", "error", err)
				continue
			}
			c.startWorkers(ctx, deliveries)
			c.logger.Info("Consumer re-established after reconnect")
		case <-c.done:
			return
		}
	}
}

func (c *Consumer) consumerTag() string {
	if c.cfg.ConsumerTag != "" {
		return c.cfg.ConsumerTag
	}
	return c.cfg.Queue + "-consumer"
}

// --- Helpers ---

func getRetryCount(d amqp.Delivery) int64 {
	if d.Headers == nil {
		return 0
	}

	if v, ok := d.Headers["x-retry-count"]; ok {
		switch count := v.(type) {
		case int64:
			return count
		case int32:
			return int64(count)
		}
	}

	return 0
}

func copyHeaders(h amqp.Table) amqp.Table {
	out := make(amqp.Table, len(h)+1)
	for k, v := range h {
		out[k] = v
	}
	return out
}
