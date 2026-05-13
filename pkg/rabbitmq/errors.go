package rabbitmq

import "errors"

var (
	// ErrNotConnected is returned when an operation requires an active connection
	// but none is available.
	ErrNotConnected = errors.New("rabbitmq: not connected")

	// ErrProducerClosed is returned when publishing on a closed producer.
	ErrProducerClosed = errors.New("rabbitmq: producer closed")

	// ErrConsumerClosed is returned when consuming on a closed consumer.
	ErrConsumerClosed = errors.New("rabbitmq: consumer closed")

	// ErrPublishNacked is returned when the broker nacks a published message.
	ErrPublishNacked = errors.New("rabbitmq: publish nacked by broker")
)
