package rabbitmq

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Delivery wraps an amqp.Delivery with retry metadata.
type Delivery struct {
	amqp.Delivery

	// RetryCount is the number of times this message has been retried.
	RetryCount int64
}

// HandlerFunc processes a single message delivery.
// Return nil to acknowledge, or an error to trigger retry/DLQ logic.
type HandlerFunc func(ctx context.Context, d Delivery) error

// Middleware wraps a HandlerFunc to add cross-cutting concerns.
type Middleware func(next HandlerFunc) HandlerFunc

// Chain applies middlewares in order (first middleware is outermost).
func Chain(handler HandlerFunc, middlewares ...Middleware) HandlerFunc {
	for i := len(middlewares) - 1; i >= 0; i-- {
		handler = middlewares[i](handler)
	}
	return handler
}
