package rabbitmq

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
)

// Client manages a RabbitMQ connection and coordinates reconnect recovery
// across registered producers and consumers.
type Client struct {
	cfg    Config
	logger *slog.Logger

	connMgr *ConnectionManager

	producers []*Producer
	consumers []*Consumer
	mu        sync.Mutex

	closeOnce sync.Once
}

// NewClient creates a new RabbitMQ client.
func NewClient(cfg Config, logger *slog.Logger) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if logger == nil {
		return nil, fmt.Errorf("rabbitmq: logger is required")
	}
	connMgr := NewConnectionManager(cfg, logger)
	return &Client{
		cfg:     cfg,
		logger:  logger.With("component", "client"),
		connMgr: connMgr,
	}, nil
}

// Connect establishes the connection and sets up reconnect handling.
func (c *Client) Connect(ctx context.Context) error {
	if err := c.connMgr.Connect(ctx); err != nil {
		return err
	}
	c.connMgr.OnReconnect(c.onReconnect)
	return nil
}

// NewProducer creates, registers, and returns a queue-dedicated producer.
func (c *Client) NewProducer(cfg ProducerConfig, queue QueueConfig, serializer Serializer) (*Producer, error) {
	p, err := NewProducer(c.connMgr, cfg, queue, serializer, c.logger)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.producers = append(c.producers, p)
	c.mu.Unlock()
	return p, nil
}

// NewConsumer creates, registers, and returns a queue-dedicated consumer.
func (c *Client) NewConsumer(cfg ConsumerConfig, queue QueueConfig, retry RetryConfig, serializer Serializer, handler Handler) (*Consumer, error) {
	consumer, err := NewConsumer(c.connMgr, cfg, queue, retry, serializer, handler, c.logger)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.consumers = append(c.consumers, consumer)
	c.mu.Unlock()
	return consumer, nil
}

// ConnectionManager returns the underlying connection manager.
func (c *Client) ConnectionManager() *ConnectionManager {
	return c.connMgr
}

// Close gracefully shuts down all consumers, producers, and the connection.
func (c *Client) Close() error {
	var firstErr error
	c.closeOnce.Do(func() {
		c.logger.Info("shutting down RabbitMQ client")

		c.mu.Lock()
		consumers := make([]*Consumer, len(c.consumers))
		copy(consumers, c.consumers)
		producers := make([]*Producer, len(c.producers))
		copy(producers, c.producers)
		c.mu.Unlock()

		for _, consumer := range consumers {
			if err := consumer.Close(); err != nil && firstErr == nil {
				firstErr = fmt.Errorf("rabbitmq: consumer close error: %w", err)
			}
		}

		for _, producer := range producers {
			if err := producer.Close(); err != nil && firstErr == nil {
				firstErr = fmt.Errorf("rabbitmq: producer close error: %w", err)
			}
		}

		if err := c.connMgr.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("rabbitmq: connection close error: %w", err)
		}

		c.logger.Info("RabbitMQ client shut down")
	})
	return firstErr
}

func (c *Client) onReconnect() {
	c.logger.Info("reconnect detected, recovering resources")

	c.mu.Lock()
	producers := make([]*Producer, len(c.producers))
	copy(producers, c.producers)
	consumers := make([]*Consumer, len(c.consumers))
	copy(consumers, c.consumers)
	c.mu.Unlock()

	for _, producer := range producers {
		producer.Recover()
	}

	for _, consumer := range consumers {
		consumer.Recover()
	}

	c.logger.Info("reconnect recovery complete")
}
