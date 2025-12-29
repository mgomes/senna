package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/mgomes/senna/internal/script"
	"github.com/redis/go-redis/v9"
)

var leakyScript = script.New("leaky", `
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local drain_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])

local state = redis.call("HMGET", key, "level", "last_drip")
local level = tonumber(state[1] or "0")
local last_drip = tonumber(state[2] or tostring(now))

local elapsed = now - last_drip
local drained = elapsed * drain_rate
level = math.max(0, level - drained)

if level >= capacity then
    local wait_time = (level - capacity + 1) / drain_rate
    return {0, level, wait_time}
end

level = level + 1
redis.call("HMSET", key, "level", level, "last_drip", now)
redis.call("EXPIRE", key, ttl)
return {1, level, 0}
`)

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

func (l *LeakyLimiter) drainRate() float64 {
	return float64(l.capacity) / l.drainTime.Seconds()
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
		now := float64(time.Now().UnixNano()) / 1e9
		drainRate := l.drainRate()
		ttl := int64(l.drainTime.Seconds() * 2)

		result, err := leakyScript.Run(ctx, l.client,
			[]string{l.keyPrefix + ":" + l.name},
			l.capacity, drainRate, now, ttl,
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
		case string:
			// ignore
		}
		retryIn := time.Duration(waitTimeSecs * float64(time.Second))

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
	now := float64(time.Now().UnixNano()) / 1e9

	state, err := l.client.HMGet(ctx, l.keyPrefix+":"+l.name, "level", "last_drip").Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	var level, lastDrip float64
	if state[0] != nil {
		if s, ok := state[0].(string); ok {
			fmt.Sscanf(s, "%f", &level)
		}
	}
	if state[1] != nil {
		if s, ok := state[1].(string); ok {
			fmt.Sscanf(s, "%f", &lastDrip)
		}
	}
	if lastDrip == 0 {
		lastDrip = now
	}

	elapsed := now - lastDrip
	drained := elapsed * l.drainRate()
	level = max(0, level-drained)

	return level, nil
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
