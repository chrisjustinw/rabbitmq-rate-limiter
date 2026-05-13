package ratelimit

type Config struct {
	RequestsPerMinute int `envconfig:"REQUESTS_PER_MINUTE"`
	Burst             int `envconfig:"BURST"`
}

func DefaultConfig() Config {
	return Config{
		Burst: 1,
	}
}
