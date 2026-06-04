package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/mgomes/senna/internal/script"
	"github.com/redis/go-redis/v9"
)

var leakyScript = script.New("leaky", leakyLua)

// LeakyLimiter implements a leaky-bucket limiter.
type LeakyLimiter struct {
	name        string
	capacity    int
	drainTime   time.Duration
	waitTimeout time.Duration
	policy      Policy
	client      redis.Cmdable
	keyPrefix   string
}

// LeakyConfig configures a LeakyLimiter.
type LeakyConfig struct {
	Name        string
	Capacity    int
	DrainTime   time.Duration
	WaitTimeout time.Duration
	Policy      Policy
	KeyPrefix   string
}

// Leaky constructs a LeakyLimiter.
func Leaky(client redis.Cmdable, cfg LeakyConfig) *LeakyLimiter {
	if cfg.WaitTimeout == 0 {
		cfg.WaitTimeout = 5 * time.Second
	}
	if cfg.KeyPrefix == "" {
		cfg.KeyPrefix = "senna:ratelimit:leaky"
	}
	return &LeakyLimiter{
		name:        cfg.Name,
		capacity:    cfg.Capacity,
		drainTime:   cfg.DrainTime,
		waitTimeout: cfg.WaitTimeout,
		policy:      cfg.Policy,
		client:      client,
		keyPrefix:   cfg.KeyPrefix,
	}
}

// Name returns the limiter name.
func (l *LeakyLimiter) Name() string {
	return l.name
}

// WithinLimit runs fn when the bucket level allows it.
func (l *LeakyLimiter) WithinLimit(ctx context.Context, fn func() error) error {
	return runWithinLimit(ctx, l, TypeLeaky, l.capacity, fn)
}

// Acquire waits for or reports capacity in the leaky bucket.
func (l *LeakyLimiter) Acquire(ctx context.Context) (Lease, time.Duration, error) {
	deadline := time.Now().Add(l.waitTimeout)

	for {
		nowUs := time.Now().UnixMicro()
		drainTimeUs := l.drainTime.Microseconds()
		if drainTimeUs < 1 {
			drainTimeUs = 1
		}
		ttlSeconds := (drainTimeUs * 2) / 1_000_000
		if ttlSeconds < 1 {
			ttlSeconds = 1
		}

		result, err := leakyScript.Run(ctx, l.client,
			[]string{l.keyPrefix + ":" + l.name},
			l.capacity, drainTimeUs, nowUs, ttlSeconds,
		)
		if err != nil {
			return nil, 0, err
		}

		vals, err := script.Ints(result, 3)
		if err != nil {
			return nil, 0, fmt.Errorf("leaky limiter %s: %w", l.name, err)
		}
		allowed := vals[0] == 1
		retryIn := time.Duration(vals[2]) * time.Microsecond

		if allowed {
			return noopLease{}, 0, nil
		}

		if l.policy == PolicySkip {
			return nil, retryIn, nil
		}

		if time.Now().Add(retryIn).After(deadline) {
			return nil, retryIn, &OverLimitError{
				LimiterName: l.name,
				LimiterType: TypeLeaky,
				Limit:       l.capacity,
				Current:     int(vals[1]),
				RetryIn:     retryIn,
			}
		}

		sleepTime := retryIn
		if sleepTime > 500*time.Millisecond {
			sleepTime = 500 * time.Millisecond
		}

		select {
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		case <-time.After(sleepTime):
		}
	}
}

// Level returns the current bucket fill level.
func (l *LeakyLimiter) Level(ctx context.Context) (float64, error) {
	nowUs := time.Now().UnixMicro()
	drainTimeUs := l.drainTime.Microseconds()
	if drainTimeUs < 1 {
		drainTimeUs = 1
	}

	state, err := l.client.HMGet(ctx, l.keyPrefix+":"+l.name, "level", "last_drip").Result()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	var level float64
	var lastDripUs int64
	if state[0] != nil {
		if s, ok := state[0].(string); ok {
			level, err = strconv.ParseFloat(s, 64)
			if err != nil {
				return 0, fmt.Errorf("parse leaky limiter level: %w", err)
			}
			if math.IsNaN(level) || math.IsInf(level, 0) {
				return 0, fmt.Errorf("parse leaky limiter level: non-finite value %q", s)
			}
		}
	}
	if state[1] != nil {
		if s, ok := state[1].(string); ok {
			lastDripUs, err = strconv.ParseInt(s, 10, 64)
			if err != nil {
				return 0, fmt.Errorf("parse leaky limiter last drip: %w", err)
			}
		}
	}
	if lastDripUs == 0 {
		lastDripUs = nowUs
	}

	elapsedUs := nowUs - lastDripUs
	drained := (float64(elapsedUs) * float64(l.capacity)) / float64(drainTimeUs)
	level = max(0, level-drained)

	return level, nil
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
