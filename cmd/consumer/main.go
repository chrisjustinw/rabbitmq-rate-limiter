package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/chrisjustinw/rabbitmq-rate-limiter/config"
	"github.com/chrisjustinw/rabbitmq-rate-limiter/pkg/rabbitmq"
	"github.com/chrisjustinw/rabbitmq-rate-limiter/pkg/ratelimit"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	if _, err := config.ReadEnvFile(); err != nil {
		logger.Error("Failed to read .env", "error", err)
		os.Exit(1)
	}

	connectionCfg, err := config.NewConnectionConfig()
	if err != nil {
		logger.Error("Failed to load RabbitMQ connection config", "error", err)
		os.Exit(1)
	}

	queueCfg, err := config.NewQueueConfig()
	if err != nil {
		logger.Error("Failed to load RabbitMQ queue config", "error", err)
		os.Exit(1)
	}

	rateLimitCfg, err := config.NewRateLimitConfig()
	if err != nil {
		logger.Error("Failed to load rate limit config", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	conn := rabbitmq.NewConnection(connectionCfg, logger)
	if err := conn.Connect(ctx); err != nil {
		logger.Error("Failed to connect to RabbitMQ", "error", err)
		os.Exit(1)
	}
	defer conn.Close()

	limiter, err := ratelimit.NewLimiter(rateLimitCfg)
	if err != nil {
		logger.Error("Failed to create rate limiter", "error", err)
		os.Exit(1)
	}

	handler := rabbitmq.Chain(
		func(ctx context.Context, d rabbitmq.Delivery) error {
			body := string(d.Body)
			logger.Info("Consumed message",
				"message_id", d.MessageId,
				"routing_key", d.RoutingKey,
				"retry_count", d.RetryCount,
				"body", body,
			)

			time.Sleep(50 * time.Millisecond)

			if body == "fail" {
				return fmt.Errorf("demo failure requested by message body")
			}
			return nil
		},
		rabbitmq.RecoveryMiddleware(logger),
		rabbitmq.RateLimitMiddleware(limiter),
	)

	consumer := rabbitmq.NewConsumer(queueCfg, conn, logger, handler)
	if err := consumer.Start(ctx); err != nil {
		logger.Error("Failed to start consumer", "error", err)
		os.Exit(1)
	}
	defer consumer.Close()

	logger.Info("Consumer is running. Press Ctrl+C to stop.")
	<-ctx.Done()
	logger.Info("Consumer stopped")
}
