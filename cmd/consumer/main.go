package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/christopherwilliam/rabbitmq-rate-limiter/config"
	"github.com/christopherwilliam/rabbitmq-rate-limiter/pkg/rabbitmq"
	"github.com/christopherwilliam/rabbitmq-rate-limiter/pkg/ratelimit"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	if _, err := config.ReadEnvFile(); err != nil {
		logger.Error("Failed to read .env", "error", err)
		os.Exit(1)
	}

	cfg, err := config.NewConnectionConfig()
	if err != nil {
		logger.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	consumerCfg, err := config.NewConsumerConfig()
	if err != nil {
		logger.Error("invalid consumer configuration", "error", err)
		os.Exit(1)
	}

	queueCfg, err := config.NewQueueConfig()
	if err != nil {
		logger.Error("invalid queue configuration", "error", err)
		os.Exit(1)
	}

	retryCfg, err := config.NewRetryConfig()
	if err != nil {
		logger.Error("invalid retry configuration", "error", err)
		os.Exit(1)
	}

	client, err := rabbitmq.NewClient(cfg, logger)
	if err != nil {
		logger.Error("failed to create client", "error", err)
		os.Exit(1)
	}

	rateLimitCfg, err := config.NewRateLimitConfig()
	if err != nil {
		logger.Error("invalid rate limit configuration", "error", err)
		os.Exit(1)
	}

	rateLimiter, err := ratelimit.NewLimiter(rateLimitCfg)
	if err != nil {
		logger.Error("failed to create rate limiter", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := client.Connect(ctx); err != nil {
		logger.Error("failed to connect", "error", err)
		os.Exit(1)
	}

	// Handler
	handler := func(ctx context.Context, msg *rabbitmq.Delivery) error {
		if err := rateLimiter.Wait(ctx); err != nil {
			return fmt.Errorf("rate limit error: %w", err)
		}

		logger.Info("received message",
			"body", string(msg.Body),
			"routing_key", msg.RoutingKey,
			"retry_count", msg.RetryCount,
		)

		if string(msg.Body) == "\"fail\"" {
			return errors.New("simulated handler error")
		}

		time.Sleep(50 * time.Millisecond)
		return nil
	}

	// Create queue-dedicated consumer
	consumer, err := client.NewConsumer(consumerCfg, queueCfg, retryCfg, rabbitmq.JSONSerializer{}, handler)
	if err != nil {
		logger.Error("failed to create consumer", "error", err)
		os.Exit(1)
	}

	if err := consumer.Start(); err != nil {
		logger.Error("failed to start consumer", "error", err)
		os.Exit(1)
	}

	logger.Info("consumer is running. Press Ctrl+C to stop.")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	logger.Info("shutting down consumer...")
	if err := client.Close(); err != nil {
		logger.Error("error during shutdown", "error", err)
	}
	logger.Info("consumer shutdown complete")
}
