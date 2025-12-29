package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/mgomes/senna/internal/script"
	"github.com/redis/go-redis/v9"
)

var leakyScript = script.New("leaky", leakyLua)

type LeakyLimiter struct {
	name        string
	capacity    int
	drainTime   time.Duration
	waitTimeout time.Duration
	policy      Policy
	client      redis.Cmdable
	keyPrefix   string
}

type LeakyConfig struct {
	Name        string
	Capacity    int
	DrainTime   time.Duration
	WaitTimeout time.Duration
	Policy      Policy
	KeyPrefix   string
}

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

func (l *LeakyLimiter) Name() string {
	return l.name
}

func (l *LeakyLimiter) WithinLimit(ctx context.Context, fn func() error) error {
	waitTime, err := l.Acquire(ctx)
	if err != nil {
		return err
	}
	if waitTime > 0 {
		return &OverLimitError{
			LimiterName: l.name,
			LimiterType: "leaky",
			Limit:       l.capacity,
			RetryIn:     waitTime,
		}
	}
	return fn()
}

func (l *LeakyLimiter) Acquire(ctx context.Context) (time.Duration, error) {
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
			return 0, err
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
			return 0, nil
		}

		if l.policy == PolicySkip {
			return retryIn, nil
		}

		if time.Now().Add(retryIn).After(deadline) {
			var level float64
			switch v := arr[1].(type) {
			case int64:
				level = float64(v)
			case float64:
				level = v
			}
			return retryIn, &OverLimitError{
				LimiterName: l.name,
				LimiterType: "leaky",
				Limit:       l.capacity,
				Current:     int(level),
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

func (l *LeakyLimiter) Release(ctx context.Context) error {
	return nil
}

func (l *LeakyLimiter) Level(ctx context.Context) (float64, error) {
	nowUs := time.Now().UnixMicro()
	drainTimeUs := l.drainTime.Microseconds()
	if drainTimeUs < 1 {
		drainTimeUs = 1
	}

	state, err := l.client.HMGet(ctx, l.keyPrefix+":"+l.name, "level", "last_drip").Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	var level float64
	var lastDripUs int64
	if state[0] != nil {
		if s, ok := state[0].(string); ok {
			_, _ = fmt.Sscanf(s, "%f", &level)
		}
	}
	if state[1] != nil {
		if s, ok := state[1].(string); ok {
			_, _ = fmt.Sscanf(s, "%d", &lastDripUs)
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
