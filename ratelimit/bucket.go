package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mgomes/senna/internal/script"
	"github.com/redis/go-redis/v9"
)

var bucketScript = script.New("bucket", bucketLua)

// BucketLimiter implements a fixed-window bucket limiter.
type BucketLimiter struct {
	name        string
	limit       int
	interval    time.Duration
	waitTimeout time.Duration
	policy      Policy
	client      redis.Cmdable
	keyPrefix   string
}

// BucketConfig configures a BucketLimiter.
type BucketConfig struct {
	Name        string
	Limit       int
	Interval    time.Duration
	WaitTimeout time.Duration
	Policy      Policy
	KeyPrefix   string
}

// Bucket constructs a BucketLimiter.
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

// Name returns the limiter name.
func (l *BucketLimiter) Name() string {
	return l.name
}

// WithinLimit runs fn when capacity is available.
func (l *BucketLimiter) WithinLimit(ctx context.Context, fn func() error) error {
	return runWithinLimit(ctx, l, TypeBucket, l.limit, fn)
}

// Acquire waits for or reports bucket capacity for a single unit of work.
func (l *BucketLimiter) Acquire(ctx context.Context) (Lease, time.Duration, error) {
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
			return nil, 0, err
		}

		vals, err := script.Ints(result, 3)
		if err != nil {
			return nil, 0, fmt.Errorf("bucket limiter %s: %w", l.name, err)
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
				LimiterType: TypeBucket,
				Limit:       l.limit,
				Current:     int(vals[1]),
				RetryIn:     retryIn,
			}
		}

		select {
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		case <-time.After(retryIn):
		}
	}
}

// Remaining returns the remaining capacity in the current bucket window.
func (l *BucketLimiter) Remaining(ctx context.Context) (int, error) {
	nowUs := time.Now().UnixMicro()
	windowUs := l.interval.Microseconds()
	if windowUs < 1 {
		windowUs = 1
	}
	bucketTs := (nowUs / windowUs) * windowUs

	key := fmt.Sprintf("%s:%s:%d", l.keyPrefix, l.name, bucketTs)
	val, err := l.client.Get(ctx, key).Int()
	if errors.Is(err, redis.Nil) {
		return l.limit, nil
	}
	if err != nil {
		return 0, err
	}
	return l.limit - val, nil
}
