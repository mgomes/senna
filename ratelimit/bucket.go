package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/mgomes/senna/internal/script"
	"github.com/redis/go-redis/v9"
)

var bucketScript = script.New("bucket", bucketLua)

type BucketLimiter struct {
	name        string
	limit       int
	interval    time.Duration
	waitTimeout time.Duration
	policy      Policy
	client      redis.Cmdable
	keyPrefix   string
}

type BucketConfig struct {
	Name        string
	Limit       int
	Interval    time.Duration
	WaitTimeout time.Duration
	Policy      Policy
	KeyPrefix   string
}

func Bucket(client redis.Cmdable, cfg BucketConfig) *BucketLimiter {
	if cfg.WaitTimeout == 0 {
		cfg.WaitTimeout = 5 * time.Second
	}
	if cfg.KeyPrefix == "" {
		cfg.KeyPrefix = "senna:ratelimit:bucket"
	}
	return &BucketLimiter{
		name:        cfg.Name,
		limit:       cfg.Limit,
		interval:    cfg.Interval,
		waitTimeout: cfg.WaitTimeout,
		policy:      cfg.Policy,
		client:      client,
		keyPrefix:   cfg.KeyPrefix,
	}
}

func (l *BucketLimiter) Name() string {
	return l.name
}

func (l *BucketLimiter) WithinLimit(ctx context.Context, fn func() error) error {
	waitTime, err := l.Acquire(ctx)
	if err != nil {
		return err
	}
	if waitTime > 0 {
		return &OverLimitError{
			LimiterName: l.name,
			LimiterType: "bucket",
			Limit:       l.limit,
			RetryIn:     waitTime,
		}
	}
	return fn()
}

func (l *BucketLimiter) Acquire(ctx context.Context) (time.Duration, error) {
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

		result, err := bucketScript.Run(ctx, l.client,
			[]string{l.keyPrefix + ":" + l.name},
			l.limit, windowUs, nowUs, ttlSeconds,
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
			return retryIn, &OverLimitError{
				LimiterName: l.name,
				LimiterType: "bucket",
				Limit:       l.limit,
				Current:     int(arr[1].(int64)),
				RetryIn:     retryIn,
			}
		}

		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(retryIn):
		}
	}
}

func (l *BucketLimiter) Release(ctx context.Context) error {
	return nil
}

func (l *BucketLimiter) Remaining(ctx context.Context) (int, error) {
	nowUs := time.Now().UnixMicro()
	windowUs := l.interval.Microseconds()
	if windowUs < 1 {
		windowUs = 1
	}
	bucketTs := (nowUs / windowUs) * windowUs

	key := fmt.Sprintf("%s:%s:%d", l.keyPrefix, l.name, bucketTs)
	val, err := l.client.Get(ctx, key).Int()
	if err == redis.Nil {
		return l.limit, nil
	}
	if err != nil {
		return 0, err
	}
	return l.limit - val, nil
}
