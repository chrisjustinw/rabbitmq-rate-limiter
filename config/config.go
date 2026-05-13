package config

import (
	"errors"
	"os"

	"github.com/christopherwilliam/rabbitmq-rate-limiter/pkg/rabbitmq"
	"github.com/christopherwilliam/rabbitmq-rate-limiter/pkg/ratelimit"
	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
)

type EnvFileRead struct{}

type ServerConfig struct {
	Addr string `envconfig:"ADDR"`
}

func ReadEnvFile() (EnvFileRead, error) {
	if err := godotenv.Load(".env"); err != nil && !errors.Is(err, os.ErrNotExist) {
		return EnvFileRead{}, err
	}

	return EnvFileRead{}, nil
}

func NewServerConfig() (ServerConfig, error) {
	cfg := ServerConfig{Addr: ":8080"}
	if err := envconfig.Process("SERVER", &cfg); err != nil {
		return cfg, err
	}

	return cfg, nil
}

func NewConnectionConfig() (rabbitmq.Config, error) {
	cfg := rabbitmq.Config{}
	if err := envconfig.Process("RABBITMQ", &cfg); err != nil {
		return cfg, err
	}

	return cfg, nil
}

func NewProducerConfig() (rabbitmq.ProducerConfig, error) {
	cfg := rabbitmq.ProducerConfig{}
	if err := envconfig.Process("RABBITMQ", &cfg); err != nil {
		return cfg, err
	}

	return cfg, nil
}

func NewConsumerConfig() (rabbitmq.ConsumerConfig, error) {
	cfg := rabbitmq.ConsumerConfig{}
	if err := envconfig.Process("RABBITMQ", &cfg); err != nil {
		return cfg, err
	}

	return cfg, nil
}

func NewRetryConfig() (rabbitmq.RetryConfig, error) {
	cfg := rabbitmq.RetryConfig{}
	if err := envconfig.Process("RABBITMQ", &cfg); err != nil {
		return cfg, err
	}

	return cfg, nil
}

func NewQueueConfig() (rabbitmq.QueueConfig, error) {
	cfg := rabbitmq.QueueConfig{}
	if err := envconfig.Process("RABBITMQ", &cfg); err != nil {
		return cfg, err
	}

	return cfg, nil
}

func NewRateLimitConfig() (ratelimit.Config, error) {
	cfg := ratelimit.DefaultConfig()
	if err := envconfig.Process("RATELIMIT", &cfg); err != nil {
		return cfg, err
	}

	return cfg, nil
}
