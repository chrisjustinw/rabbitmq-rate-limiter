package rabbitmq

import (
	"time"
)

// ConnectionConfig holds settings for the AMQP connection and reconnection.
// Shared across all producers and consumers on the same connection.
type ConnectionConfig struct {
	Addresses          []string      `envconfig:"ADDRESSES"`
	Username           string        `envconfig:"USERNAME"`
	Password           string        `envconfig:"PASSWORD"`
	VirtualHost        string        `envconfig:"VIRTUAL_HOST"`
	DialTimeout        time.Duration `envconfig:"DIAL_TIMEOUT"`
	RequestedHeartbeat time.Duration `envconfig:"REQUESTED_HEARTBEAT"`
	ReconnectDelay     time.Duration `envconfig:"RECONNECT_DELAY"`
	MaxReconnectDelay  time.Duration `envconfig:"MAX_RECONNECT_DELAY"`
}

// DefaultConnectionConfig returns a ConnectionConfig with sensible production defaults.
func DefaultConnectionConfig() ConnectionConfig {
	return ConnectionConfig{
		VirtualHost:        "/",
		DialTimeout:        10 * time.Second,
		RequestedHeartbeat: 30 * time.Second,
		ReconnectDelay:     2 * time.Second,
		MaxReconnectDelay:  60 * time.Second,
	}
}

// QueueConfig holds per-queue topology, consumer, producer, and retry settings.
type QueueConfig struct {
	// Topology
	Exchange     string `envconfig:"EXCHANGE"`
	ExchangeType string `envconfig:"EXCHANGE_TYPE"`
	Queue        string `envconfig:"QUEUE"`
	RoutingKey   string `envconfig:"ROUTING_KEY"`
	QueueType    string `envconfig:"QUEUE_TYPE"`
	Durable      bool   `envconfig:"DURABLE"`

	// Consumer
	ConsumerTag   string `envconfig:"CONSUMER_TAG"`
	PrefetchCount int    `envconfig:"PREFETCH_COUNT"`
	WorkerCount   int    `envconfig:"WORKER_COUNT"`

	// Retry
	MaxRetries    int           `envconfig:"MAX_RETRIES"`
	DeliveryLimit int           `envconfig:"DELIVERY_LIMIT"`
	RetryTTL      time.Duration `envconfig:"RETRY_TTL"`
	RetryExchange string        `envconfig:"RETRY_EXCHANGE"`
	RetryQueue    string        `envconfig:"RETRY_QUEUE"`

	// Dead Letter
	DLXExchange string `envconfig:"DLX_EXCHANGE"`
	DLQ         string `envconfig:"DLQ"`

	// Producer
	PublishTimeout time.Duration `envconfig:"PUBLISH_TIMEOUT"`
}

// DefaultQueueConfig returns a QueueConfig with sensible production defaults.
func DefaultQueueConfig() QueueConfig {
	return QueueConfig{
		ExchangeType:   "direct",
		QueueType:      "quorum",
		Durable:        true,
		PrefetchCount:  10,
		WorkerCount:    5,
		MaxRetries:     3,
		DeliveryLimit:  5,
		RetryTTL:       30 * time.Second,
		PublishTimeout: 5 * time.Second,
	}
}
