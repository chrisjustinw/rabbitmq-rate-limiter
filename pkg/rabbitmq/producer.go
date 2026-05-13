package rabbitmq

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// PublishMessage represents a message to be published.
type PublishMessage struct {
	Body    interface{}
	Headers map[string]interface{}
	// Expiration sets per-message TTL (used for delayed retries).
	Expiration string
	// result is used internally for synchronous error feedback.
	result chan error
}

// Producer publishes messages to exactly one queue topology.
// Each producer worker owns a dedicated AMQP channel.
type Producer struct {
	cfg        ProducerConfig
	queue      QueueConfig
	connMgr    *ConnectionManager
	logger     *slog.Logger
	serializer Serializer

	msgCh     chan *PublishMessage
	closed    atomic.Bool
	closeOnce sync.Once
	wg        sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc

	reconnectChs []chan struct{}
	mu           sync.Mutex
}

// NewProducer creates a queue-dedicated producer.
// Fails fast on invalid configuration or nil dependencies.
func NewProducer(
	connMgr *ConnectionManager,
	cfg ProducerConfig,
	queue QueueConfig,
	serializer Serializer,
	logger *slog.Logger,
) (*Producer, error) {
	if connMgr == nil {
		return nil, fmt.Errorf("rabbitmq: connection manager is required")
	}
	if logger == nil {
		return nil, fmt.Errorf("rabbitmq: logger is required")
	}
	if serializer == nil {
		return nil, fmt.Errorf("rabbitmq: serializer is required")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if err := queue.Validate(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Producer{
		cfg:        cfg,
		queue:      queue,
		connMgr:    connMgr,
		logger:     logger.With("component", "producer", "queue", queue.Queue),
		serializer: serializer,
		msgCh:      make(chan *PublishMessage, cfg.BufferSize),
		ctx:        ctx,
		cancel:     cancel,
	}, nil
}

// Start declares topology and begins the producer worker pool.
func (p *Producer) Start() error {
	if err := p.declareTopology(); err != nil {
		return fmt.Errorf("rabbitmq: producer topology declaration failed: %w", err)
	}

	p.mu.Lock()
	p.reconnectChs = make([]chan struct{}, p.cfg.Workers)
	for i := 0; i < p.cfg.Workers; i++ {
		p.reconnectChs[i] = make(chan struct{}, 1)
		p.wg.Add(1)
		go p.worker(i, p.reconnectChs[i])
	}
	p.mu.Unlock()

	p.logger.Info("producer started", "workers", p.cfg.Workers, "buffer_size", p.cfg.BufferSize)
	return nil
}

// Publish sends a message through this producer's queue topology.
// Blocks until publish is confirmed or context is cancelled.
func (p *Producer) Publish(ctx context.Context, msg PublishMessage) error {
	if p.closed.Load() {
		return fmt.Errorf("rabbitmq: producer is closed")
	}

	resultCh := make(chan error, 1)
	msg.result = resultCh

	select {
	case p.msgCh <- &msg:
	case <-ctx.Done():
		return ctx.Err()
	case <-p.ctx.Done():
		return fmt.Errorf("rabbitmq: producer is shutting down")
	}

	select {
	case err := <-resultCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-p.ctx.Done():
		return fmt.Errorf("rabbitmq: producer is shutting down")
	}
}

// PublishAsync sends a message without waiting for confirmation.
func (p *Producer) PublishAsync(ctx context.Context, msg PublishMessage) error {
	if p.closed.Load() {
		return fmt.Errorf("rabbitmq: producer is closed")
	}

	select {
	case p.msgCh <- &msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-p.ctx.Done():
		return fmt.Errorf("rabbitmq: producer is shutting down")
	}
}

// Close gracefully shuts down the producer.
func (p *Producer) Close() error {
	p.closeOnce.Do(func() {
		p.closed.Store(true)
		p.cancel()
		p.wg.Wait()
		p.logger.Info("producer closed")
	})
	return nil
}

// Recover redeclares topology and signals all workers to recreate channels.
func (p *Producer) Recover() {
	if err := p.declareTopology(); err != nil {
		p.logger.Error("failed to redeclare topology on recover", "error", err)
	}

	p.mu.Lock()
	chs := p.reconnectChs
	p.mu.Unlock()

	for _, ch := range chs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	p.logger.Info("producer recovery signaled")
}

func (p *Producer) declareTopology() error {
	conn := p.connMgr.Connection()
	if conn == nil || conn.IsClosed() {
		return fmt.Errorf("rabbitmq: connection not available")
	}
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("rabbitmq: failed to create channel for topology: %w", err)
	}
	defer ch.Close()

	topo := NewTopology(p.queue, p.logger)
	return topo.Declare(ch)
}

func (p *Producer) worker(id int, reconnectCh <-chan struct{}) {
	defer p.wg.Done()
	logger := p.logger.With("worker_id", id)
	logger.Debug("producer worker started")

	ch, confirmCh, err := p.createWorkerChannel(logger)
	if err != nil {
		logger.Error("failed to create initial channel", "error", err)
	}

	for {
		select {
		case <-reconnectCh:
			closeChannel(ch)
			ch, confirmCh, err = p.createWorkerChannel(logger)
			if err != nil {
				logger.Error("failed to recreate channel after reconnect", "error", err)
			}

		case msg, ok := <-p.msgCh:
			if !ok {
				closeChannel(ch)
				return
			}
			p.processMessage(logger, msg, &ch, &confirmCh)

		case <-p.ctx.Done():
			p.drainMessages(logger, &ch, &confirmCh)
			closeChannel(ch)
			return
		}
	}
}

func (p *Producer) drainMessages(logger *slog.Logger, ch **amqp.Channel, confirmCh *chan amqp.Confirmation) {
	for {
		select {
		case msg, ok := <-p.msgCh:
			if !ok {
				return
			}
			p.processMessage(logger, msg, ch, confirmCh)
		default:
			return
		}
	}
}

func (p *Producer) processMessage(logger *slog.Logger, msg *PublishMessage, ch **amqp.Channel, confirmCh *chan amqp.Confirmation) {
	var lastErr error
	maxRetries := p.cfg.MaxRetries
	if maxRetries < 1 {
		maxRetries = 1
	}

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			logger.Debug("retrying publish", "attempt", attempt)
		}

		err := p.doPublish(*ch, *confirmCh, msg)
		if err == nil {
			if msg.result != nil {
				msg.result <- nil
			}
			return
		}

		lastErr = err
		logger.Warn("publish failed", "attempt", attempt, "error", err)

		if !p.connMgr.IsConnected() {
			select {
			case <-time.After(time.Second):
			case <-p.ctx.Done():
				if msg.result != nil {
					msg.result <- lastErr
				}
				return
			}
		}

		closeChannel(*ch)
		newCh, newConfirm, setupErr := p.createWorkerChannel(logger)
		if setupErr != nil {
			logger.Warn("channel setup failed during retry", "error", setupErr)
		} else {
			*ch = newCh
			*confirmCh = newConfirm
		}
	}

	if msg.result != nil {
		msg.result <- fmt.Errorf("rabbitmq: publish failed after %d attempts: %w", maxRetries+1, lastErr)
	}
}

func (p *Producer) doPublish(ch *amqp.Channel, confirmCh chan amqp.Confirmation, msg *PublishMessage) error {
	if ch == nil {
		return fmt.Errorf("rabbitmq: no channel available")
	}

	body, err := p.serializer.Marshal(msg.Body)
	if err != nil {
		return fmt.Errorf("rabbitmq: serialization error: %w", err)
	}

	publishing := amqp.Publishing{
		ContentType:  p.serializer.ContentType(),
		Body:         body,
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		Headers:      amqp.Table(msg.Headers),
	}

	if msg.Expiration != "" {
		publishing.Expiration = msg.Expiration
	}

	ctx := context.Background()
	if p.cfg.PublishTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.cfg.PublishTimeout)
		defer cancel()
	}

	err = ch.PublishWithContext(
		ctx,
		p.queue.Exchange,
		p.queue.RoutingKey,
		p.cfg.Mandatory,
		false,
		publishing,
	)
	if err != nil {
		return fmt.Errorf("rabbitmq: publish error: %w", err)
	}

	if confirmCh != nil {
		select {
		case confirmed, ok := <-confirmCh:
			if !ok {
				return fmt.Errorf("rabbitmq: confirm channel closed")
			}
			if !confirmed.Ack {
				return fmt.Errorf("rabbitmq: publish nacked by broker")
			}
		case <-ctx.Done():
			return fmt.Errorf("rabbitmq: publish confirm timeout")
		}
	}

	return nil
}

func (p *Producer) createWorkerChannel(logger *slog.Logger) (*amqp.Channel, chan amqp.Confirmation, error) {
	conn := p.connMgr.Connection()
	if conn == nil || conn.IsClosed() {
		return nil, nil, fmt.Errorf("rabbitmq: connection is not available")
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, nil, fmt.Errorf("rabbitmq: failed to create channel: %w", err)
	}

	if err := ch.Confirm(false); err != nil {
		ch.Close()
		return nil, nil, fmt.Errorf("rabbitmq: failed to put channel in confirm mode: %w", err)
	}

	confirmCh := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	logger.Debug("worker channel created")
	return ch, confirmCh, nil
}

func closeChannel(ch *amqp.Channel) {
	if ch != nil && !ch.IsClosed() {
		ch.Close()
	}
}
