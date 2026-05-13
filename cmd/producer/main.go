package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/christopherwilliam/rabbitmq-rate-limiter/config"
	"github.com/christopherwilliam/rabbitmq-rate-limiter/pkg/rabbitmq"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	if _, err := config.ReadEnvFile(); err != nil {
		logger.Error("Failed to read .env", "error", err)
		os.Exit(1)
	}

	serverCfg, err := config.NewServerConfig()
	if err != nil {
		logger.Error("invalid server configuration", "error", err)
		os.Exit(1)
	}

	cfg, err := config.NewConnectionConfig()
	if err != nil {
		logger.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	producerCfg, err := config.NewProducerConfig()
	if err != nil {
		logger.Error("invalid producer configuration", "error", err)
		os.Exit(1)
	}

	queueCfg, err := config.NewQueueConfig()
	if err != nil {
		logger.Error("invalid queue configuration", "error", err)
		os.Exit(1)
	}

	client, err := rabbitmq.NewClient(cfg, logger)
	if err != nil {
		logger.Error("failed to create client", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := client.Connect(ctx); err != nil {
		logger.Error("failed to connect", "error", err)
		os.Exit(1)
	}

	// Create queue-dedicated producer
	producer, err := client.NewProducer(producerCfg, queueCfg, rabbitmq.JSONSerializer{})
	if err != nil {
		logger.Error("failed to create producer", "error", err)
		os.Exit(1)
	}

	if err := producer.Start(); err != nil {
		logger.Error("failed to start producer", "error", err)
		os.Exit(1)
	}

	// HTTP API
	mux := http.NewServeMux()
	mux.HandleFunc("POST /publish", publishHandler(producer, logger))

	srv := &http.Server{
		Addr:              serverCfg.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		logger.Info("HTTP server starting", "addr", serverCfg.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("HTTP server error", "error", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	logger.Info("shutting down...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP shutdown error", "error", err)
	}

	if err := client.Close(); err != nil {
		logger.Error("error during shutdown", "error", err)
	}
	logger.Info("producer shutdown complete")
}

type publishRequest struct {
	Body interface{} `json:"body"`
}

func publishHandler(producer *rabbitmq.Producer, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req publishRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"invalid request body"}`, http.StatusBadRequest)
			return
		}

		msg := rabbitmq.PublishMessage{
			Body: req.Body,
		}

		if err := producer.Publish(r.Context(), msg); err != nil {
			logger.Error("publish failed", "error", err)
			http.Error(w, `{"error":"publish failed"}`, http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]string{"status": "accepted"})
	}
}
