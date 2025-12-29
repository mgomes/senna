package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/mgomes/senna/internal/script"
	"github.com/redis/go-redis/v9"
)

var windowScript = script.New("window", `
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local member = ARGV[4]
local ttl = tonumber(ARGV[5])

redis.call("ZREMRANGEBYSCORE", key, "-inf", now - window)

local count = redis.call("ZCARD", key)

if count >= limit then
    local oldest = redis.call("ZRANGE", key, 0, 0, "WITHSCORES")
    if #oldest >= 2 then
        local retry_in = tonumber(oldest[2]) + window - now
        return {0, count, retry_in}
    end
    return {0, count, window}
end

redis.call("ZADD", key, now, member)
redis.call("EXPIRE", key, ttl)
return {1, count + 1, 0}
`)

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
		now := float64(time.Now().UnixNano()) / 1e9
		windowSecs := l.interval.Seconds()
		ttl := int64(windowSecs * 2)
		member := uuid.New().String()

		result, err := windowScript.Run(ctx, l.client,
			[]string{l.keyPrefix + ":" + l.name},
			l.limit, windowSecs, now, member, ttl,
		)
		if err != nil {
			return 0, err
		}

		arr := result.([]any)
		allowed := arr[0].(int64) == 1
		retryInSecs := arr[2].(int64)
		if f, ok := arr[2].(float64); ok {
			retryInSecs = int64(f)
		}
		retryIn := time.Duration(float64(retryInSecs) * float64(time.Second))

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
	now := float64(time.Now().UnixNano()) / 1e9
	windowSecs := l.interval.Seconds()

	key := l.keyPrefix + ":" + l.name
	count, err := l.client.ZCount(ctx, key, "-inf", "+inf").Result()
	if err == redis.Nil {
		return l.limit, nil
	}
	if err != nil {
		return 0, err
	}

	l.client.ZRemRangeByScore(ctx, key, "-inf", fmt.Sprintf("%f", now-windowSecs))

	return l.limit - int(count), nil
}
