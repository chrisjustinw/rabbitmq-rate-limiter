package rabbitmq

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Handler processes a consumed message. Return nil to ack, error to nack.
type Handler func(ctx context.Context, msg *Delivery) error

// Delivery wraps an AMQP delivery with helper methods.
type Delivery struct {
	amqp.Delivery
	RetryCount int
}

// Consumer consumes messages from exactly one queue topology.
// Each consumer worker owns a dedicated AMQP channel.
type Consumer struct {
	cfg        ConsumerConfig
	queue      QueueConfig
	retry      RetryConfig
	connMgr    *ConnectionManager
	logger     *slog.Logger
	serializer Serializer
	handler    Handler

	closed atomic.Bool
	wg     sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc

	reconnectChs []chan struct{}
	mu           sync.Mutex
}

// NewConsumer creates a queue-dedicated consumer.
// Fails fast on invalid configuration or nil dependencies.
func NewConsumer(
	connMgr *ConnectionManager,
	cfg ConsumerConfig,
	queue QueueConfig,
	retry RetryConfig,
	serializer Serializer,
	handler Handler,
	logger *slog.Logger,
) (*Consumer, error) {
	if connMgr == nil {
		return nil, fmt.Errorf("rabbitmq: connection manager is required")
	}
	if logger == nil {
		return nil, fmt.Errorf("rabbitmq: logger is required")
	}
	if serializer == nil {
		return nil, fmt.Errorf("rabbitmq: serializer is required")
	}
	if handler == nil {
		return nil, fmt.Errorf("rabbitmq: handler is required")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if err := queue.Validate(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Consumer{
		cfg:        cfg,
		queue:      queue,
		retry:      retry,
		connMgr:    connMgr,
		logger:     logger.With("component", "consumer", "queue", queue.Queue),
		serializer: serializer,
		handler:    handler,
		ctx:        ctx,
		cancel:     cancel,
	}, nil
}

// Start declares topology and begins consuming with dedicated channels per worker.
func (c *Consumer) Start() error {
	if err := c.declareTopology(); err != nil {
		return fmt.Errorf("rabbitmq: consumer topology declaration failed: %w", err)
	}

	c.mu.Lock()
	c.reconnectChs = make([]chan struct{}, c.cfg.Workers)
	for i := 0; i < c.cfg.Workers; i++ {
		c.reconnectChs[i] = make(chan struct{}, 1)
		c.wg.Add(1)
		go c.worker(i, c.reconnectChs[i])
	}
	c.mu.Unlock()

	c.logger.Info("consumer started", "workers", c.cfg.Workers, "prefetch", c.cfg.PrefetchCount)
	return nil
}

// Close gracefully shuts down the consumer.
func (c *Consumer) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	c.cancel()
	c.wg.Wait()
	c.logger.Info("consumer closed")
	return nil
}

// Recover redeclares topology and signals all workers to recreate channels.
func (c *Consumer) Recover() {
	if err := c.declareTopology(); err != nil {
		c.logger.Error("failed to redeclare topology on recover", "error", err)
	}

	c.mu.Lock()
	chs := c.reconnectChs
	c.mu.Unlock()

	for _, ch := range chs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	c.logger.Info("consumer recovery signaled")
}

func (c *Consumer) declareTopology() error {
	conn := c.connMgr.Connection()
	if conn == nil || conn.IsClosed() {
		return fmt.Errorf("rabbitmq: connection not available")
	}
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("rabbitmq: failed to create channel for topology: %w", err)
	}
	defer ch.Close()

	topo := NewTopology(c.queue, c.logger)
	return topo.Declare(ch)
}

func (c *Consumer) worker(id int, reconnectCh <-chan struct{}) {
	defer c.wg.Done()
	logger := c.logger.With("worker_id", id)
	tag := fmt.Sprintf("%s-%d", c.cfg.ConsumerTag, id)
	logger.Debug("consumer worker started")

	ch, deliveries := c.setupWorkerChannel(logger, tag)

	for {
		select {
		case <-reconnectCh:
			c.cancelConsumer(ch, tag, logger)
			closeChannel(ch)
			ch, deliveries = c.setupWorkerChannel(logger, tag)

		case d, ok := <-deliveries:
			if !ok {
				logger.Debug("delivery channel closed, waiting for reconnect")
				closeChannel(ch)
				ch = nil
				deliveries = nil
				select {
				case <-reconnectCh:
					ch, deliveries = c.setupWorkerChannel(logger, tag)
				case <-c.ctx.Done():
					return
				}
				continue
			}
			c.handleDelivery(logger, d)

		case <-c.ctx.Done():
			c.cancelConsumer(ch, tag, logger)
			closeChannel(ch)
			return
		}
	}
}

func (c *Consumer) setupWorkerChannel(logger *slog.Logger, tag string) (*amqp.Channel, <-chan amqp.Delivery) {
	conn := c.connMgr.Connection()
	if conn == nil || conn.IsClosed() {
		logger.Error("connection not available for consumer setup")
		return nil, nil
	}

	ch, err := conn.Channel()
	if err != nil {
		logger.Error("failed to create consumer channel", "error", err)
		return nil, nil
	}

	if err := ch.Qos(c.cfg.PrefetchCount, 0, false); err != nil {
		logger.Error("failed to set QoS", "error", err)
		ch.Close()
		return nil, nil
	}

	deliveries, err := ch.Consume(
		c.queue.Queue,
		tag,
		false, // autoAck
		false, // exclusive
		false, // noLocal
		false, // noWait
		nil,
	)
	if err != nil {
		logger.Error("failed to start consuming", "error", err)
		ch.Close()
		return nil, nil
	}

	logger.Debug("consumer channel created")
	return ch, deliveries
}

func (c *Consumer) cancelConsumer(ch *amqp.Channel, tag string, logger *slog.Logger) {
	if ch != nil && !ch.IsClosed() {
		if err := ch.Cancel(tag, false); err != nil {
			logger.Debug("error cancelling consumer", "error", err)
		}
	}
}

func (c *Consumer) handleDelivery(logger *slog.Logger, d amqp.Delivery) {
	delivery := &Delivery{
		Delivery:   d,
		RetryCount: extractRetryCount(d.Headers),
	}

	var handlerErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				handlerErr = fmt.Errorf("rabbitmq: handler panic: %v", r)
				logger.Error("handler panicked", "panic", r)
			}
		}()
		handlerErr = c.handler(c.ctx, delivery)
	}()

	if handlerErr == nil {
		if err := d.Ack(false); err != nil {
			logger.Error("failed to ack message", "error", err)
		}
		return
	}

	logger.Warn("handler returned error", "error", handlerErr)

	if c.retry.MaxRetries > 0 {
		c.handleRetry(logger, d, delivery.RetryCount, handlerErr)
		return
	}

	if err := d.Nack(false, false); err != nil {
		logger.Error("failed to nack message", "error", err)
	}
}

func (c *Consumer) handleRetry(logger *slog.Logger, d amqp.Delivery, retryCount int, handlerErr error) {
	if retryCount >= c.retry.MaxRetries {
		logger.Warn("max retries reached, routing to DLQ",
			"retry_count", retryCount,
			"error", handlerErr)
		c.publishToDLQ(logger, d, retryCount, handlerErr)
		if err := d.Ack(false); err != nil {
			logger.Error("failed to ack after DLQ routing", "error", err)
		}
		return
	}

	delay := c.retry.RetryDelay(retryCount)
	logger.Info("scheduling retry",
		"retry_count", retryCount+1,
		"delay", fmt.Sprintf("%dms", delay.Milliseconds()),
	)

	c.publishToRetry(logger, d, retryCount+1, delay)
	if err := d.Ack(false); err != nil {
		logger.Error("failed to ack after retry routing", "error", err)
	}
}

func (c *Consumer) publishToRetry(logger *slog.Logger, d amqp.Delivery, retryCount int, delay time.Duration) {
	conn := c.connMgr.Connection()
	if conn == nil || conn.IsClosed() {
		logger.Error("connection not available for retry publish")
		return
	}
	ch, err := conn.Channel()
	if err != nil {
		logger.Error("failed to create channel for retry publish", "error", err)
		return
	}
	defer ch.Close()

	headers := cloneHeaders(d.Headers)
	headers["x-retry-count"] = int32(retryCount)
	headers["x-original-exchange"] = d.Exchange
	headers["x-original-routing-key"] = d.RoutingKey
	headers["x-first-failure-time"] = time.Now().UTC().Format(time.RFC3339)

	err = ch.PublishWithContext(
		context.Background(),
		c.queue.RetryExchange,
		c.queue.RoutingKey,
		false, false,
		amqp.Publishing{
			Headers:      headers,
			ContentType:  d.ContentType,
			Body:         d.Body,
			DeliveryMode: amqp.Persistent,
			Expiration:   strconv.FormatInt(delay.Milliseconds(), 10),
		},
	)
	if err != nil {
		logger.Error("failed to publish to retry queue", "error", err)
	}
}

func (c *Consumer) publishToDLQ(logger *slog.Logger, d amqp.Delivery, retryCount int, handlerErr error) {
	conn := c.connMgr.Connection()
	if conn == nil || conn.IsClosed() {
		logger.Error("connection not available for DLQ publish")
		return
	}
	ch, err := conn.Channel()
	if err != nil {
		logger.Error("failed to create channel for DLQ publish", "error", err)
		return
	}
	defer ch.Close()

	headers := cloneHeaders(d.Headers)
	headers["x-retry-count"] = int32(retryCount)
	headers["x-final-error"] = handlerErr.Error()
	headers["x-dead-lettered-at"] = time.Now().UTC().Format(time.RFC3339)

	err = ch.PublishWithContext(
		context.Background(),
		c.queue.DLXExchange,
		c.queue.RoutingKey,
		false, false,
		amqp.Publishing{
			Headers:      headers,
			ContentType:  d.ContentType,
			Body:         d.Body,
			DeliveryMode: amqp.Persistent,
		},
	)
	if err != nil {
		logger.Error("failed to publish to DLQ", "error", err)
	}
}

func extractRetryCount(headers amqp.Table) int {
	if headers == nil {
		return 0
	}
	v, ok := headers["x-retry-count"]
	if !ok {
		return 0
	}
	switch val := v.(type) {
	case int32:
		return int(val)
	case int64:
		return int(val)
	case int:
		return val
	default:
		return 0
	}
}

func cloneHeaders(h amqp.Table) amqp.Table {
	if h == nil {
		return amqp.Table{}
	}
	out := make(amqp.Table, len(h))
	for k, v := range h {
		out[k] = v
	}
	return out
}
