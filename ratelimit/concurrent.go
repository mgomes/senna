package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
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

// ConcurrentLimiter limits how many jobs may run concurrently.
type ConcurrentLimiter struct {
	name        string
	limit       int
	lockTimeout time.Duration
	waitTimeout time.Duration
	policy      Policy
	client      redis.Cmdable
	keyPrefix   string
	initialized atomic.Bool
}

// ConcurrentConfig configures a ConcurrentLimiter.
type ConcurrentConfig struct {
	Name        string
	Limit       int
	LockTimeout time.Duration
	WaitTimeout time.Duration
	Policy      Policy
	KeyPrefix   string
}

// Concurrent constructs a ConcurrentLimiter.
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

// Name returns the limiter name.
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

func (l *ConcurrentLimiter) ttlSeconds() int64 {
	ttl := (l.lockTimeout.Milliseconds() * 3) / 1000
	if ttl < 1 {
		ttl = 1
	}
	return ttl
}

func (l *ConcurrentLimiter) ensureInitialized(ctx context.Context) error {
	if l.initialized.Load() {
		return nil
	}

	_, err := concurrentInitScript.Run(ctx, l.client,
		[]string{l.slotsKey(), l.locksKey(), l.initKey()},
		l.limit, l.ttlSeconds(),
	)
	if err != nil {
		return err
	}

	l.initialized.Store(true)
	return nil
}

func (l *ConcurrentLimiter) reclaim(ctx context.Context) (int, error) {
	nowMs := time.Now().UnixMilli()
	lockTimeoutMs := l.lockTimeout.Milliseconds()

	result, err := concurrentReclaimScript.Run(ctx, l.client,
		[]string{l.slotsKey(), l.locksKey(), l.keyPrefix + ":" + l.name + ":metrics"},
		nowMs, lockTimeoutMs, l.ttlSeconds(),
	)
	if err != nil {
		return 0, err
	}

	reclaimed, err := script.Int(result)
	if err != nil {
		return 0, fmt.Errorf("concurrent limiter %s reclaim: %w", l.name, err)
	}
	return int(reclaimed), nil
}

// WithinLimit runs fn after acquiring a concurrency slot and releases it afterward.
func (l *ConcurrentLimiter) WithinLimit(ctx context.Context, fn func() error) (err error) {
	lease, waitTime, err := l.Acquire(ctx)
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
	defer func() {
		if releaseErr := lease.Release(ctx); releaseErr != nil {
			if err == nil {
				err = releaseErr
				return
			}
			err = errors.Join(err, releaseErr)
		}
	}()

	return fn()
}

// Acquire waits for or reports availability of a concurrency slot.
func (l *ConcurrentLimiter) Acquire(ctx context.Context) (Lease, time.Duration, error) {
	if err := l.ensureInitialized(ctx); err != nil {
		return nil, 0, err
	}

	deadline := time.Now().Add(l.waitTimeout)
	tempLockID := l.generateLockID()

	for {
		if _, err := l.reclaim(ctx); err != nil {
			return nil, 0, fmt.Errorf("reclaim expired concurrent limiter slots: %w", err)
		}
		nowMs := time.Now().UnixMilli()

		result, err := concurrentAcquireScript.Run(ctx, l.client,
			[]string{l.slotsKey(), l.locksKey()},
			tempLockID, nowMs, l.ttlSeconds(),
		)
		if err != nil {
			return nil, 0, err
		}

		vals, err := script.Ints(result, 1)
		if err != nil {
			return nil, 0, fmt.Errorf("concurrent limiter %s: %w", l.name, err)
		}
		acquired := vals[0] == 1

		if acquired {
			return &concurrentLease{limiter: l, lockID: tempLockID}, 0, nil
		}

		if l.policy == PolicySkip {
			return nil, l.waitTimeout, nil
		}

		if time.Now().After(deadline) {
			return nil, l.waitTimeout, &OverLimitError{
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
			return nil, 0, ctx.Err()
		case <-time.After(sleepTime):
		}
	}
}

type concurrentLease struct {
	limiter *ConcurrentLimiter
	lockID  string
	once    sync.Once
	err     error
}

func (l *concurrentLease) Release(ctx context.Context) error {
	if l == nil || l.lockID == "" {
		return nil
	}
	l.once.Do(func() {
		l.err = l.limiter.release(ctx, l.lockID)
	})
	return l.err
}

func (l *ConcurrentLimiter) release(ctx context.Context, lockID string) error {
	_, err := concurrentReleaseScript.Run(ctx, l.client,
		[]string{l.slotsKey(), l.locksKey()},
		lockID, l.ttlSeconds(),
	)
	return err
}

// Held returns the number of currently held slots.
func (l *ConcurrentLimiter) Held(ctx context.Context) (int, error) {
	count, err := l.client.HLen(ctx, l.locksKey()).Result()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	return int(count), err
}
