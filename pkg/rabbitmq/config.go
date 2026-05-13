package rabbitmq

import (
	"fmt"
	"math"
	"time"
)

// Config holds all configuration for the RabbitMQ client.
type Config struct {
	Hosts     []string      `envconfig:"HOSTS"`
	Username  string        `envconfig:"USERNAME"`
	Password  string        `envconfig:"PASSWORD"`
	VHost     string        `envconfig:"VHOST"`
	Heartbeat time.Duration `envconfig:"HEARTBEAT"`

	InitialReconnectDelay time.Duration `envconfig:"RECONNECT_INITIAL_DELAY"`
	MaxReconnectDelay     time.Duration `envconfig:"RECONNECT_MAX_DELAY"`
	ReconnectMultiplier   float64       `envconfig:"RECONNECT_MULTIPLIER"`
	MaxReconnectAttempts  int           `envconfig:"RECONNECT_MAX_ATTEMPTS"`
}

// ProducerConfig configures the producer.
type ProducerConfig struct {
	Workers        int           `envconfig:"PRODUCER_WORKERS"`
	BufferSize     int           `envconfig:"PRODUCER_BUFFER_SIZE"`
	PublishTimeout time.Duration `envconfig:"PRODUCER_PUBLISH_TIMEOUT"`
	MaxRetries     int           `envconfig:"PRODUCER_MAX_RETRIES"`
	Mandatory      bool          `envconfig:"PRODUCER_MANDATORY"`
}

// ConsumerConfig configures the consumer.
type ConsumerConfig struct {
	PrefetchCount int    `envconfig:"CONSUMER_PREFETCH"`
	Workers       int    `envconfig:"CONSUMER_WORKERS"`
	ConsumerTag   string `envconfig:"CONSUMER_TAG"`
}

// QueueConfig defines a queue and its complete topology resource names.
type QueueConfig struct {
	// Resource names
	Exchange      string `envconfig:"EXCHANGE"`
	RetryExchange string `envconfig:"RETRY_EXCHANGE"`
	DLXExchange   string `envconfig:"DLX_EXCHANGE"`
	Queue         string `envconfig:"QUEUE"`
	RetryQueue    string `envconfig:"RETRY_QUEUE"`
	DLQ           string `envconfig:"DLQ"`

	// Exchange/queue settings
	ExchangeType  string        `envconfig:"EXCHANGE_TYPE"` // e.g. "direct", "topic", "fanout"
	QueueType     string        `envconfig:"QUEUE_TYPE"`    // "classic" or "quorum"
	RoutingKey    string        `envconfig:"ROUTING_KEY"`
	Durable       bool          `envconfig:"DURABLE"`
	DeliveryLimit int           `envconfig:"DELIVERY_LIMIT"` // quorum queue delivery limit (0 = disabled)
	RetryTTL      time.Duration `envconfig:"RETRY_TTL"`      // per-message TTL on the retry queue
}

// RetryConfig configures retry/DLQ behavior.
type RetryConfig struct {
	MaxRetries int           `envconfig:"RETRY_MAX_RETRIES"`
	Policy     RetryPolicy   `envconfig:"RETRY_POLICY"`
	BaseDelay  time.Duration `envconfig:"RETRY_BASE_DELAY"`
	MaxDelay   time.Duration `envconfig:"RETRY_MAX_DELAY"`
	Multiplier float64       `envconfig:"RETRY_MULTIPLIER"`
}

// RetryPolicy defines the retry delay strategy.
type RetryPolicy int

const (
	RetryPolicyFixed RetryPolicy = iota
	RetryPolicyExponential
)

// DefaultConfig returns a Config with sensible production defaults.
func DefaultConfig(hosts []string) Config {
	return Config{
		Hosts:                 hosts,
		Username:              "guest",
		Password:              "guest",
		VHost:                 "/",
		Heartbeat:             30 * time.Second,
		InitialReconnectDelay: 1 * time.Second,
		MaxReconnectDelay:     30 * time.Second,
		ReconnectMultiplier:   2.0,
		MaxReconnectAttempts:  0,
	}
}

// Validate checks configuration for obvious errors.
func (c *Config) Validate() error {
	if len(c.Hosts) == 0 {
		return fmt.Errorf("rabbitmq: at least one host is required")
	}
	if c.ReconnectMultiplier < 1 {
		return fmt.Errorf("rabbitmq: reconnect multiplier must be >= 1")
	}
	return nil
}

// Validate checks producer configuration.
func (c *ProducerConfig) Validate() error {
	if c.Workers < 1 {
		return fmt.Errorf("rabbitmq: producer workers must be >= 1")
	}
	if c.BufferSize < 1 {
		return fmt.Errorf("rabbitmq: producer buffer size must be >= 1")
	}
	return nil
}

// Validate checks consumer configuration.
func (c *ConsumerConfig) Validate() error {
	if c.Workers < 1 {
		return fmt.Errorf("rabbitmq: consumer workers must be >= 1")
	}
	if c.PrefetchCount < 1 {
		return fmt.Errorf("rabbitmq: consumer prefetch count must be >= 1")
	}
	if c.ConsumerTag == "" {
		return fmt.Errorf("rabbitmq: consumer tag is required")
	}
	return nil
}

// Validate checks queue configuration.
func (c *QueueConfig) Validate() error {
	if c.Exchange == "" {
		return fmt.Errorf("rabbitmq: exchange name is required")
	}
	if c.Queue == "" {
		return fmt.Errorf("rabbitmq: queue name is required")
	}
	if c.RoutingKey == "" {
		return fmt.Errorf("rabbitmq: routing key is required")
	}
	if c.DLXExchange == "" {
		return fmt.Errorf("rabbitmq: DLX exchange name is required")
	}
	if c.DLQ == "" {
		return fmt.Errorf("rabbitmq: DLQ name is required")
	}
	if c.RetryExchange == "" {
		return fmt.Errorf("rabbitmq: retry exchange name is required")
	}
	if c.RetryQueue == "" {
		return fmt.Errorf("rabbitmq: retry queue name is required")
	}
	return nil
}

// RetryDelay calculates the delay for a given attempt based on the retry policy.
func (r *RetryConfig) RetryDelay(attempt int) time.Duration {
	var delay time.Duration
	switch r.Policy {
	case RetryPolicyFixed:
		delay = r.BaseDelay
	case RetryPolicyExponential:
		delay = time.Duration(float64(r.BaseDelay) * math.Pow(r.Multiplier, float64(attempt)))
	default:
		delay = r.BaseDelay
	}
	if delay > r.MaxDelay {
		delay = r.MaxDelay
	}
	return delay
}

// ReconnectDelay calculates reconnect delay for a given attempt.
func (c *Config) ReconnectDelay(attempt int) time.Duration {
	delay := time.Duration(float64(c.InitialReconnectDelay) * math.Pow(c.ReconnectMultiplier, float64(attempt)))
	if delay > c.MaxReconnectDelay {
		delay = c.MaxReconnectDelay
	}
	return delay
}
