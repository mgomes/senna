package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/mgomes/senna/internal/script"
	"github.com/redis/go-redis/v9"
)

var windowScript = script.New("window", windowLua)

// WindowLimiter implements a sliding-window limiter.
type WindowLimiter struct {
	name        string
	limit       int
	interval    time.Duration
	waitTimeout time.Duration
	policy      Policy
	client      redis.Cmdable
	keyPrefix   string
}

// WindowConfig configures a WindowLimiter.
type WindowConfig struct {
	Name        string
	Limit       int
	Interval    time.Duration
	WaitTimeout time.Duration
	Policy      Policy
	KeyPrefix   string
}

// Window constructs a WindowLimiter.
func Window(client redis.Cmdable, cfg WindowConfig) *WindowLimiter {
	if cfg.WaitTimeout == 0 {
		cfg.WaitTimeout = 5 * time.Second
	}
	if cfg.KeyPrefix == "" {
		cfg.KeyPrefix = "senna:ratelimit:window"
	}
	return &WindowLimiter{
		name:        cfg.Name,
		limit:       cfg.Limit,
		interval:    cfg.Interval,
		waitTimeout: cfg.WaitTimeout,
		policy:      cfg.Policy,
		client:      client,
		keyPrefix:   cfg.KeyPrefix,
	}
}

// Name returns the limiter name.
func (l *WindowLimiter) Name() string {
	return l.name
}

// WithinLimit runs fn when capacity is available.
func (l *WindowLimiter) WithinLimit(ctx context.Context, fn func() error) error {
	_, waitTime, err := l.Acquire(ctx)
	if err != nil {
		return err
	}
	if waitTime > 0 {
		return &OverLimitError{
			LimiterName: l.name,
			LimiterType: "window",
			Limit:       l.limit,
			RetryIn:     waitTime,
		}
	}
	return fn()
}

// Acquire waits for or reports capacity in the sliding window.
func (l *WindowLimiter) Acquire(ctx context.Context) (Lease, time.Duration, error) {
	deadline := time.Now().Add(l.waitTimeout)

	for {
		nowUs := time.Now().UnixMicro()
		windowUs := l.interval.Microseconds()
		if windowUs < 1 {
			windowUs = 1
		}
		ttlSeconds := (windowUs * 2) / 1_000_000
		if ttlSeconds < 1 {
			ttlSeconds = 1
		}
		member := uuid.New().String()

		result, err := windowScript.Run(ctx, l.client,
			[]string{l.keyPrefix + ":" + l.name},
			l.limit, windowUs, nowUs, member, ttlSeconds,
		)
		if err != nil {
			return nil, 0, err
		}

		arr := result.([]any)
		allowed := arr[0].(int64) == 1
		var retryUs int64
		switch v := arr[2].(type) {
		case int64:
			retryUs = v
		case float64:
			retryUs = int64(v)
		}
		retryIn := time.Duration(retryUs) * time.Microsecond

		if allowed {
			return noopLease{}, 0, nil
		}

		if l.policy == PolicySkip {
			return nil, retryIn, nil
		}

		if time.Now().Add(retryIn).After(deadline) {
			return nil, retryIn, &OverLimitError{
				LimiterName: l.name,
				LimiterType: "window",
				Limit:       l.limit,
				Current:     int(arr[1].(int64)),
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

// Remaining returns the remaining capacity in the current window.
func (l *WindowLimiter) Remaining(ctx context.Context) (int, error) {
	nowUs := time.Now().UnixMicro()
	windowUs := l.interval.Microseconds()
	if windowUs < 1 {
		windowUs = 1
	}

	key := l.keyPrefix + ":" + l.name
	count, err := l.client.ZCount(ctx, key, "-inf", "+inf").Result()
	if err == redis.Nil {
		return l.limit, nil
	}
	if err != nil {
		return 0, err
	}

	l.client.ZRemRangeByScore(ctx, key, "-inf", fmt.Sprintf("%d", nowUs-windowUs))

	return l.limit - int(count), nil
}
