package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/chrisjustinw/rabbitmq-rate-limiter/config"
	"github.com/chrisjustinw/rabbitmq-rate-limiter/pkg/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
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

	serverCfg, err := config.NewServerConfig()
	if err != nil {
		logger.Error("Failed to load server config", "error", err)
		os.Exit(1)
	}

	ctx := context.Background()
	conn := rabbitmq.NewConnection(connectionCfg, logger)
	if err := conn.Connect(ctx); err != nil {
		logger.Error("Failed to connect to RabbitMQ", "error", err)
		os.Exit(1)
	}
	defer conn.Close()

	producer := rabbitmq.NewProducer(queueCfg, conn, logger)
	if err := producer.Start(ctx); err != nil {
		logger.Error("Failed to start producer", "error", err)
		os.Exit(1)
	}
	defer producer.Close()

	runHTTPServer(serverCfg.Addr, producer, logger)
}

type publishRequest struct {
	Message string `json:"message"`
}

func runHTTPServer(addr string, producer *rabbitmq.Producer, logger *slog.Logger) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /publish", func(w http.ResponseWriter, r *http.Request) {
		var req publishRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON body", http.StatusBadRequest)
			return
		}

		message := strings.TrimSpace(req.Message)
		if message == "" {
			http.Error(w, "message is required", http.StatusBadRequest)
			return
		}

		id := fmt.Sprintf("demo-%d", time.Now().UnixNano())
		err := producer.Publish(r.Context(), amqp.Publishing{
			ContentType:  "text/plain",
			DeliveryMode: amqp.Persistent,
			MessageId:    id,
			Timestamp:    time.Now(),
			Body:         []byte(message),
		})
		if err != nil {
			logger.Error("Failed to publish message", "message_id", id, "error", err)
			http.Error(w, "failed to publish message", http.StatusInternalServerError)
			return
		}

		logger.Info("Published message", "message_id", id, "body", message)
		writeJSON(w, http.StatusAccepted, map[string]string{
			"message_id": id,
			"status":     "published",
		})
	})

	logger.Info("HTTP producer is running", "addr", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		logger.Error("HTTP server failed", "error", err)
		os.Exit(1)
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
