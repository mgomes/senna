package ratelimit

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/mgomes/senna/internal/script"
	"github.com/redis/go-redis/v9"
)

var concurrentInitScript = script.New("concurrent_init", concurrentInitLua)

var concurrentAcquireScript = script.New("concurrent_acquire", concurrentAcquireLua)

var concurrentReleaseScript = script.New("concurrent_release", concurrentReleaseLua)

var concurrentReclaimScript = script.New("concurrent_reclaim", concurrentReclaimLua)

type ConcurrentLimiter struct {
	name        string
	limit       int
	lockTimeout time.Duration
	waitTimeout time.Duration
	policy      Policy
	client      redis.Cmdable
	keyPrefix   string
	initialized atomic.Bool
	lockID      string
}

type ConcurrentConfig struct {
	Name        string
	Limit       int
	LockTimeout time.Duration
	WaitTimeout time.Duration
	Policy      Policy
	KeyPrefix   string
}

func Concurrent(client redis.Cmdable, cfg ConcurrentConfig) *ConcurrentLimiter {
	if cfg.LockTimeout == 0 {
		cfg.LockTimeout = 30 * time.Second
	}
	if cfg.WaitTimeout == 0 {
		cfg.WaitTimeout = 5 * time.Second
	}
	if cfg.KeyPrefix == "" {
		cfg.KeyPrefix = "senna:ratelimit:concurrent"
	}
	return &ConcurrentLimiter{
		name:        cfg.Name,
		limit:       cfg.Limit,
		lockTimeout: cfg.LockTimeout,
		waitTimeout: cfg.WaitTimeout,
		policy:      cfg.Policy,
		client:      client,
		keyPrefix:   cfg.KeyPrefix,
	}
}

func (l *ConcurrentLimiter) Name() string {
	return l.name
}

func (l *ConcurrentLimiter) slotsKey() string {
	return l.keyPrefix + ":" + l.name + ":slots"
}

func (l *ConcurrentLimiter) locksKey() string {
	return l.keyPrefix + ":" + l.name + ":locks"
}

func (l *ConcurrentLimiter) initKey() string {
	return l.keyPrefix + ":" + l.name + ":init"
}

func (l *ConcurrentLimiter) generateLockID() string {
	return fmt.Sprintf("%d:%s", os.Getpid(), uuid.New().String())
}

func (l *ConcurrentLimiter) ensureInitialized(ctx context.Context) error {
	if l.initialized.Load() {
		return nil
	}

	ttl := int64(l.lockTimeout.Seconds() * 3)
	_, err := concurrentInitScript.Run(ctx, l.client,
		[]string{l.slotsKey(), l.locksKey(), l.initKey()},
		l.limit, ttl,
	)
	if err != nil {
		return err
	}

	l.initialized.Store(true)
	return nil
}

func (l *ConcurrentLimiter) reclaim(ctx context.Context) (int, error) {
	now := float64(time.Now().UnixNano()) / 1e9
	lockTimeoutSecs := l.lockTimeout.Seconds()
	ttl := int64(lockTimeoutSecs * 3)

	result, err := concurrentReclaimScript.Run(ctx, l.client,
		[]string{l.slotsKey(), l.locksKey()},
		now, lockTimeoutSecs, ttl,
	)
	if err != nil {
		return 0, err
	}

	return int(result.(int64)), nil
}

func (l *ConcurrentLimiter) WithinLimit(ctx context.Context, fn func() error) error {
	waitTime, err := l.Acquire(ctx)
	if err != nil {
		return err
	}
	if waitTime > 0 {
		return &OverLimitError{
			LimiterName: l.name,
			LimiterType: "concurrent",
			Limit:       l.limit,
			RetryIn:     waitTime,
		}
	}

	defer l.Release(ctx)
	return fn()
}

func (l *ConcurrentLimiter) Acquire(ctx context.Context) (time.Duration, error) {
	if err := l.ensureInitialized(ctx); err != nil {
		return 0, err
	}

	l.reclaim(ctx)

	deadline := time.Now().Add(l.waitTimeout)
	l.lockID = l.generateLockID()

	for {
		now := float64(time.Now().UnixNano()) / 1e9
		ttl := int64(l.lockTimeout.Seconds() * 3)

		result, err := concurrentAcquireScript.Run(ctx, l.client,
			[]string{l.slotsKey(), l.locksKey()},
			l.lockID, now, ttl,
		)
		if err != nil {
			return 0, err
		}

		arr := result.([]any)
		acquired := arr[0].(int64) == 1

		if acquired {
			return 0, nil
		}

		if l.policy == PolicySkip {
			return l.waitTimeout, nil
		}

		if time.Now().After(deadline) {
			return l.waitTimeout, &OverLimitError{
				LimiterName: l.name,
				LimiterType: "concurrent",
				Limit:       l.limit,
				Current:     l.limit,
				RetryIn:     l.waitTimeout,
			}
		}

		sleepTime := 100 * time.Millisecond
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(sleepTime):
		}
	}
}

func (l *ConcurrentLimiter) Release(ctx context.Context) error {
	if l.lockID == "" {
		return nil
	}

	ttl := int64(l.lockTimeout.Seconds() * 3)
	_, err := concurrentReleaseScript.Run(ctx, l.client,
		[]string{l.slotsKey(), l.locksKey()},
		l.lockID, ttl,
	)
	l.lockID = ""
	return err
}

func (l *ConcurrentLimiter) Held(ctx context.Context) (int, error) {
	count, err := l.client.HLen(ctx, l.locksKey()).Result()
	if err == redis.Nil {
		return 0, nil
	}
	return int(count), err
}
