package rabbitmq

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/chrisjustinw/rabbitmq-rate-limiter/pkg/ratelimit"
)

// RecoveryMiddleware catches panics in the handler and converts them to errors.
func RecoveryMiddleware(logger *slog.Logger) Middleware {
	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, d Delivery) (err error) {
			defer func() {
				if r := recover(); r != nil {
					stack := debug.Stack()
					logger.Error("Handler panic recovered",
						"message_id", d.MessageId,
						"panic", r,
						"stack", string(stack),
					)
					err = fmt.Errorf("panic: %v", r)
				}
			}()
			return next(ctx, d)
		}
	}
}

// RateLimitMiddleware applies rate limiting to the handler.
func RateLimitMiddleware(limiter *ratelimit.Limiter) Middleware {
	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, d Delivery) error {
			if err := limiter.Wait(ctx); err != nil {
				return fmt.Errorf("rate limit wait: %w", err)
			}
			return next(ctx, d)
		}
	}
}

// LoggingMiddleware logs message processing start, completion, and duration.
func LoggingMiddleware(logger *slog.Logger) Middleware {
	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, d Delivery) error {
			start := time.Now()
			logger.Debug("Message consumed",
				"message_id", d.MessageId,
				"routing_key", d.RoutingKey,
				"retry_count", d.RetryCount,
			)

			err := next(ctx, d)

			attrs := []any{
				"message_id", d.MessageId,
				"duration", time.Since(start),
			}
			if err != nil {
				attrs = append(attrs, "error", err)
				logger.Warn("Message processing failed", attrs...)
			} else {
				logger.Debug("Message processed", attrs...)
			}
			return err
		}
	}
}
