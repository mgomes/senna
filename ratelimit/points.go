package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/mgomes/senna/internal/script"
	"github.com/redis/go-redis/v9"
)

var pointsCheckScript = script.New("points_check", pointsCheckLua)

var pointsAdjustScript = script.New("points_adjust", pointsAdjustLua)

type PointsLimiter struct {
	name        string
	capacity    int
	refillTime  time.Duration
	waitTimeout time.Duration
	policy      Policy
	client      redis.Cmdable
	keyPrefix   string
}

type PointsConfig struct {
	Name        string
	Capacity    int
	RefillTime  time.Duration
	WaitTimeout time.Duration
	Policy      Policy
	KeyPrefix   string
}

func Points(client redis.Cmdable, cfg PointsConfig) *PointsLimiter {
	if cfg.WaitTimeout == 0 {
		cfg.WaitTimeout = 5 * time.Second
	}
	if cfg.KeyPrefix == "" {
		cfg.KeyPrefix = "senna:ratelimit:points"
	}
	return &PointsLimiter{
		name:        cfg.Name,
		capacity:    cfg.Capacity,
		refillTime:  cfg.RefillTime,
		waitTimeout: cfg.WaitTimeout,
		policy:      cfg.Policy,
		client:      client,
		keyPrefix:   cfg.KeyPrefix,
	}
}

func (l *PointsLimiter) Name() string {
	return l.name
}

func (l *PointsLimiter) refillRate() float64 {
	return float64(l.capacity) / l.refillTime.Seconds()
}

func (l *PointsLimiter) WithinLimit(ctx context.Context, fn func() error) error {
	return l.WithinLimitCost(ctx, 1, fn)
}

func (l *PointsLimiter) WithinLimitCost(ctx context.Context, cost int, fn func() error) error {
	waitTime, err := l.AcquirePoints(ctx, cost)
	if err != nil {
		return err
	}
	if waitTime > 0 {
		return &OverLimitError{
			LimiterName: l.name,
			LimiterType: "points",
			Limit:       l.capacity,
			RetryIn:     waitTime,
		}
	}
	return fn()
}

type PointsHandle struct {
	limiter  *PointsLimiter
	ctx      context.Context
	estimate int
}

func (h *PointsHandle) PointsUsed(actual int) error {
	diff := h.estimate - actual
	if diff == 0 {
		return nil
	}
	return h.limiter.adjust(h.ctx, diff)
}

func (l *PointsLimiter) WithinLimitEstimate(ctx context.Context, estimate int, fn func(h *PointsHandle) error) error {
	waitTime, err := l.AcquirePoints(ctx, estimate)
	if err != nil {
		return err
	}
	if waitTime > 0 {
		return &OverLimitError{
			LimiterName: l.name,
			LimiterType: "points",
			Limit:       l.capacity,
			RetryIn:     waitTime,
		}
	}

	handle := &PointsHandle{
		limiter:  l,
		ctx:      ctx,
		estimate: estimate,
	}
	return fn(handle)
}

func (l *PointsLimiter) Acquire(ctx context.Context) (time.Duration, error) {
	return l.AcquirePoints(ctx, 1)
}

func (l *PointsLimiter) AcquirePoints(ctx context.Context, cost int) (time.Duration, error) {
	deadline := time.Now().Add(l.waitTimeout)

	for {
		now := float64(time.Now().UnixNano()) / 1e9
		refillRate := l.refillRate()
		ttl := int64(l.refillTime.Seconds() * 2)

		result, err := pointsCheckScript.Run(ctx, l.client,
			[]string{l.keyPrefix + ":" + l.name},
			l.capacity, refillRate, cost, now, ttl,
		)
		if err != nil {
			return 0, err
		}

		arr := result.([]any)
		allowed := arr[0].(int64) == 1

		var waitTimeSecs float64
		switch v := arr[2].(type) {
		case int64:
			waitTimeSecs = float64(v)
		case float64:
			waitTimeSecs = v
		}
		retryIn := time.Duration(waitTimeSecs * float64(time.Second))

		if allowed {
			return 0, nil
		}

		if l.policy == PolicySkip {
			return retryIn, nil
		}

		if time.Now().Add(retryIn).After(deadline) {
			var points float64
			switch v := arr[1].(type) {
			case int64:
				points = float64(v)
			case float64:
				points = v
			}
			return retryIn, &OverLimitError{
				LimiterName: l.name,
				LimiterType: "points",
				Limit:       l.capacity,
				Current:     int(points),
				RetryIn:     retryIn,
			}
		}

		sleepTime := retryIn
		if sleepTime > 500*time.Millisecond {
			sleepTime = 500 * time.Millisecond
		}

		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(sleepTime):
		}
	}
}

func (l *PointsLimiter) Release(ctx context.Context) error {
	return nil
}

func (l *PointsLimiter) adjust(ctx context.Context, diff int) error {
	ttl := int64(l.refillTime.Seconds() * 2)
	_, err := pointsAdjustScript.Run(ctx, l.client,
		[]string{l.keyPrefix + ":" + l.name},
		diff, l.capacity, ttl,
	)
	return err
}

func (l *PointsLimiter) AvailablePoints(ctx context.Context) (float64, error) {
	now := float64(time.Now().UnixNano()) / 1e9

	state, err := l.client.HMGet(ctx, l.keyPrefix+":"+l.name, "points", "last_refill").Result()
	if err == redis.Nil {
		return float64(l.capacity), nil
	}
	if err != nil {
		return 0, err
	}

	points := float64(l.capacity)
	var lastRefill float64

	if state[0] != nil {
		if s, ok := state[0].(string); ok {
			fmt.Sscanf(s, "%f", &points)
		}
	}
	if state[1] != nil {
		if s, ok := state[1].(string); ok {
			fmt.Sscanf(s, "%f", &lastRefill)
		}
	}
	if lastRefill == 0 {
		lastRefill = now
	}

	elapsed := now - lastRefill
	refilled := elapsed * l.refillRate()
	points = min(float64(l.capacity), points+refilled)

	return points, nil
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
