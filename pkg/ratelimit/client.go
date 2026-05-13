package ratelimit

import (
	"context"

	"golang.org/x/time/rate"
)

// Limiter wraps x/time/rate with application-level configuration in RPM.
type Limiter struct {
	cfg     Config
	limiter *rate.Limiter
}

func NewLimiter(cfg Config) (*Limiter, error) {
	return &Limiter{
		cfg: cfg,
		limiter: rate.NewLimiter(
			rate.Limit(float64(cfg.RequestsPerMinute)/60),
			cfg.Burst,
		),
	}, nil
}

func (l *Limiter) Wait(ctx context.Context) error {
	if l == nil || l.limiter == nil {
		return nil
	}
	return l.limiter.Wait(ctx)
}
