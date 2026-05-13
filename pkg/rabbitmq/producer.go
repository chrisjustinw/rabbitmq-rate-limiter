package rabbitmq

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Producer publishes messages to RabbitMQ with publish confirms and
// automatic channel recovery on connection loss.
type Producer struct {
	cfg    QueueConfig
	conn   *Connection
	logger *slog.Logger

	mu      sync.Mutex
	channel *amqp.Channel

	closed atomic.Bool
	done   chan struct{}

	topology *Topology
}

// NewProducer creates a producer bound to the given connection.
func NewProducer(cfg QueueConfig, conn *Connection, logger *slog.Logger) *Producer {
	return &Producer{
		cfg:      cfg,
		conn:     conn,
		logger:   logger.With("component", "rabbitmq.producer"),
		done:     make(chan struct{}),
		topology: NewTopology(cfg, logger),
	}
}

// Start initializes the producer channel with publish confirms and
// begins listening for reconnection events.
func (p *Producer) Start(ctx context.Context) error {
	if err := p.setup(); err != nil {
		return fmt.Errorf("producer setup: %w", err)
	}
	go p.watchReconnect()

	p.logger.Info("Producer started",
		"exchange", p.cfg.Exchange,
		"routing_key", p.cfg.RoutingKey,
	)
	return nil
}

// Publish sends a message to the configured exchange.
func (p *Producer) Publish(ctx context.Context, msg amqp.Publishing) error {
	if p.closed.Load() {
		return ErrProducerClosed
	}

	pubCtx, cancel := context.WithTimeout(ctx, p.cfg.PublishTimeout)
	defer cancel()

	p.mu.Lock()
	if p.channel == nil {
		p.mu.Unlock()
		return ErrNotConnected
	}
	confirmation, err := p.channel.PublishWithDeferredConfirmWithContext(
		pubCtx,
		p.cfg.Exchange,
		p.cfg.RoutingKey,
		true,  // mandatory
		false, // immediate
		msg,
	)
	p.mu.Unlock()

	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}

	// Wait for broker confirm without holding the mutex.
	select {
	case <-confirmation.Done():
		if !confirmation.Acked() {
			return ErrPublishNacked
		}
		return nil
	case <-pubCtx.Done():
		return fmt.Errorf("publish confirm timeout: %w", pubCtx.Err())
	}
}

// Close shuts down the producer.
func (p *Producer) Close() error {
	if p.closed.Swap(true) {
		return nil
	}
	close(p.done)

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.channel != nil {
		err := p.channel.Close()
		p.channel = nil
		return err
	}
	return nil
}

func (p *Producer) setup() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed.Load() {
		return ErrProducerClosed
	}

	if p.channel != nil {
		p.channel.Close()
		p.channel = nil
	}

	conn := p.conn.RawConnection()
	if conn == nil {
		return ErrNotConnected
	}

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}

	if err := p.topology.Declare(ch); err != nil {
		ch.Close()
		return err
	}

	if err := ch.Confirm(false); err != nil {
		ch.Close()
		return fmt.Errorf("enable confirms: %w", err)
	}

	p.channel = ch
	return nil
}

func (p *Producer) watchReconnect() {
	reconnect, unsubscribe := p.conn.NotifyReconnect()
	defer unsubscribe()
	for {
		select {
		case _, ok := <-reconnect:
			if !ok || p.closed.Load() {
				return
			}
			p.logger.Info("Re-establishing producer channel after reconnect")
			if err := p.setup(); err != nil {
				p.logger.Error("Failed to re-establish producer channel", "error", err)
			}
			p.logger.Info("Producer re-established after reconnect")
		case <-p.done:
			return
		}
	}
}
