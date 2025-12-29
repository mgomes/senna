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

type WindowLimiter struct {
	name        string
	limit       int
	interval    time.Duration
	waitTimeout time.Duration
	policy      Policy
	client      redis.Cmdable
	keyPrefix   string
}

type WindowConfig struct {
	Name        string
	Limit       int
	Interval    time.Duration
	WaitTimeout time.Duration
	Policy      Policy
	KeyPrefix   string
}

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

func (l *WindowLimiter) Name() string {
	return l.name
}

func (l *WindowLimiter) WithinLimit(ctx context.Context, fn func() error) error {
	waitTime, err := l.Acquire(ctx)
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

func (l *WindowLimiter) Acquire(ctx context.Context) (time.Duration, error) {
	deadline := time.Now().Add(l.waitTimeout)

	for {
		nowMs := time.Now().UnixMilli()
		windowMs := l.interval.Milliseconds()
		if windowMs < 1 {
			windowMs = 1
		}
		ttlSeconds := (windowMs * 2) / 1000
		if ttlSeconds < 1 {
			ttlSeconds = 1
		}
		member := uuid.New().String()

		result, err := windowScript.Run(ctx, l.client,
			[]string{l.keyPrefix + ":" + l.name},
			l.limit, windowMs, nowMs, member, ttlSeconds,
		)
		if err != nil {
			return 0, err
		}

		arr := result.([]any)
		allowed := arr[0].(int64) == 1
		retryIn := time.Duration(arr[2].(int64)) * time.Millisecond

		if allowed {
			return 0, nil
		}

		if l.policy == PolicySkip {
			return retryIn, nil
		}

		if time.Now().Add(retryIn).After(deadline) {
			return retryIn, &OverLimitError{
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
			return 0, ctx.Err()
		case <-time.After(sleepTime):
		}
	}
}

func (l *WindowLimiter) Release(ctx context.Context) error {
	return nil
}

func (l *WindowLimiter) Remaining(ctx context.Context) (int, error) {
	nowMs := time.Now().UnixMilli()
	windowMs := l.interval.Milliseconds()
	if windowMs < 1 {
		windowMs = 1
	}

	key := l.keyPrefix + ":" + l.name
	count, err := l.client.ZCount(ctx, key, "-inf", "+inf").Result()
	if err == redis.Nil {
		return l.limit, nil
	}
	if err != nil {
		return 0, err
	}

	l.client.ZRemRangeByScore(ctx, key, "-inf", fmt.Sprintf("%d", nowMs-windowMs))

	return l.limit - int(count), nil
}
