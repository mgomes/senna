package senna

import (
	"testing"
	"time"
)

func FuzzExponentialBackoff(f *testing.F) {
	f.Add(int64(time.Second), int64(time.Minute), 0)
	f.Add(int64(time.Second), int64(time.Minute), 64)
	f.Add(int64(0), int64(time.Minute), 10)

	f.Fuzz(func(t *testing.T, rawBase int64, rawMax int64, rawAttempt int) {
		base := boundedDuration(rawBase, int64(time.Hour))
		maxBackoff := boundedDuration(rawMax, int64(24*time.Hour))
		if maxBackoff < base {
			maxBackoff = base
		}
		attempt := rawAttempt % 128
		if attempt < 0 {
			attempt = -attempt
		}

		backoff := ExponentialBackoff(base, maxBackoff)
		got := backoff(attempt)
		if got < 0 {
			t.Fatalf("ExponentialBackoff(%v, %v)(%d) = %v, want non-negative", base, maxBackoff, attempt, got)
		}
		if got > maxBackoff {
			t.Fatalf("ExponentialBackoff(%v, %v)(%d) = %v, want <= %v", base, maxBackoff, attempt, got, maxBackoff)
		}
		if attempt > 0 {
			prev := backoff(attempt - 1)
			if got < prev {
				t.Fatalf("ExponentialBackoff(%v, %v)(%d) = %v, previous attempt = %v", base, maxBackoff, attempt, got, prev)
			}
		}
	})
}

func FuzzDefaultBackoff(f *testing.F) {
	f.Add(0)
	f.Add(25)
	f.Add(1_000)

	f.Fuzz(func(t *testing.T, rawAttempt int) {
		attempt := rawAttempt % 10_000
		if attempt < 0 {
			attempt = -attempt
		}

		backoff := DefaultBackoff()
		got := backoff(attempt)
		if got < 0 {
			t.Fatalf("DefaultBackoff()(%d) = %v, want non-negative", attempt, got)
		}
		if attempt > 0 {
			prev := backoff(attempt - 1)
			if got < prev {
				t.Fatalf("DefaultBackoff()(%d) = %v, previous attempt = %v", attempt, got, prev)
			}
		}
	})
}

func boundedDuration(raw int64, max int64) time.Duration {
	value := raw % (max + 1)
	if value < 0 {
		value = -value
	}
	return time.Duration(value)
}
