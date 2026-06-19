package ratelimit_test

import (
	"context"
	"testing"
	"time"

	"github.com/mgomes/senna/ratelimit"
)

func BenchmarkLimiterAcquire(b *testing.B) {
	tests := map[string]func(*testing.B) ratelimit.Acquirer{
		"bucket": func(b *testing.B) ratelimit.Acquirer {
			client := newTestClient(b)
			flushKeys(b, client, "senna:ratelimit:bucket:bench-acquire*")
			b.Cleanup(func() {
				flushKeys(b, client, "senna:ratelimit:bucket:bench-acquire*")
			})
			return ratelimit.Bucket(client, ratelimit.BucketConfig{
				Name:        "bench-acquire",
				Limit:       b.N + 1,
				Interval:    time.Hour,
				WaitTimeout: time.Millisecond,
				Policy:      ratelimit.PolicySkip,
			})
		},
		"window": func(b *testing.B) ratelimit.Acquirer {
			client := newTestClient(b)
			flushKeys(b, client, "senna:ratelimit:window:bench-acquire*")
			b.Cleanup(func() {
				flushKeys(b, client, "senna:ratelimit:window:bench-acquire*")
			})
			return ratelimit.Window(client, ratelimit.WindowConfig{
				Name:        "bench-acquire",
				Limit:       b.N + 1,
				Interval:    time.Hour,
				WaitTimeout: time.Millisecond,
				Policy:      ratelimit.PolicySkip,
			})
		},
		"leaky": func(b *testing.B) ratelimit.Acquirer {
			client := newTestClient(b)
			flushKeys(b, client, "senna:ratelimit:leaky:bench-acquire*")
			b.Cleanup(func() {
				flushKeys(b, client, "senna:ratelimit:leaky:bench-acquire*")
			})
			return ratelimit.Leaky(client, ratelimit.LeakyConfig{
				Name:        "bench-acquire",
				Capacity:    b.N + 1,
				DrainTime:   time.Hour,
				WaitTimeout: time.Millisecond,
				Policy:      ratelimit.PolicySkip,
			})
		},
		"points": func(b *testing.B) ratelimit.Acquirer {
			client := newTestClient(b)
			flushKeys(b, client, "senna:ratelimit:points:bench-acquire*")
			b.Cleanup(func() {
				flushKeys(b, client, "senna:ratelimit:points:bench-acquire*")
			})
			return ratelimit.Points(client, ratelimit.PointsConfig{
				Name:        "bench-acquire",
				Capacity:    b.N + 1,
				RefillTime:  time.Hour,
				WaitTimeout: time.Millisecond,
				Policy:      ratelimit.PolicySkip,
			})
		},
		"concurrent": func(b *testing.B) ratelimit.Acquirer {
			client := newTestClient(b)
			flushKeys(b, client, "senna:ratelimit:concurrent:bench-acquire*")
			b.Cleanup(func() {
				flushKeys(b, client, "senna:ratelimit:concurrent:bench-acquire*")
			})
			return ratelimit.Concurrent(client, ratelimit.ConcurrentConfig{
				Name:        "bench-acquire",
				Limit:       1,
				WaitTimeout: time.Millisecond,
				Policy:      ratelimit.PolicySkip,
			})
		},
	}

	ctx := context.Background()
	for name, makeLimiter := range tests {
		b.Run(name, func(b *testing.B) {
			limiter := makeLimiter(b)

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				lease, waitTime, err := limiter.Acquire(ctx)
				if err != nil {
					b.Fatalf("Limiter.Acquire() error = %v, want nil", err)
				}
				if waitTime != 0 {
					b.Fatalf("Limiter.Acquire() waitTime = %v, want 0", waitTime)
				}
				if lease != nil {
					if err := lease.Release(ctx); err != nil {
						b.Fatalf("Lease.Release() error = %v, want nil", err)
					}
				}
			}
		})
	}
}
