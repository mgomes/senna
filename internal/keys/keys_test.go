package keys

import (
	"testing"
)

func TestKeys_New_DefaultNamespace(t *testing.T) {
	t.Parallel()
	k := New("")

	queue := k.Queue("test")
	if queue != "senna:queue:test" {
		t.Errorf("expected 'senna:queue:test', got '%s'", queue)
	}
}

func TestKeys_New_CustomNamespace(t *testing.T) {
	t.Parallel()
	k := New("myapp")

	queue := k.Queue("test")
	if queue != "myapp:queue:test" {
		t.Errorf("expected 'myapp:queue:test', got '%s'", queue)
	}
}

func TestKeys_Queue(t *testing.T) {
	t.Parallel()
	k := New("app")

	tests := []struct {
		name     string
		expected string
	}{
		{"default", "app:queue:default"},
		{"critical", "app:queue:critical"},
		{"low", "app:queue:low"},
	}

	for _, tt := range tests {
		result := k.Queue(tt.name)
		if result != tt.expected {
			t.Errorf("Queue(%s): expected '%s', got '%s'", tt.name, tt.expected, result)
		}
	}
}

func TestKeys_Scheduled(t *testing.T) {
	t.Parallel()
	k := New("app")

	result := k.Scheduled()
	if result != "app:scheduled" {
		t.Errorf("expected 'app:scheduled', got '%s'", result)
	}
}

func TestKeys_Retry(t *testing.T) {
	t.Parallel()
	k := New("app")

	result := k.Retry()
	if result != "app:retry" {
		t.Errorf("expected 'app:retry', got '%s'", result)
	}
}

func TestKeys_Dead(t *testing.T) {
	t.Parallel()
	k := New("app")

	result := k.Dead()
	if result != "app:dead" {
		t.Errorf("expected 'app:dead', got '%s'", result)
	}
}

func TestKeys_InFlight(t *testing.T) {
	t.Parallel()
	k := New("app")

	result := k.InFlight("worker-123")
	if result != "app:inflight:worker-123" {
		t.Errorf("expected 'app:inflight:worker-123', got '%s'", result)
	}
}

func TestKeys_Workers(t *testing.T) {
	t.Parallel()
	k := New("app")

	result := k.Workers()
	if result != "app:workers" {
		t.Errorf("expected 'app:workers', got '%s'", result)
	}
}

func TestKeys_Worker(t *testing.T) {
	t.Parallel()
	k := New("app")

	result := k.Worker("worker-abc")
	if result != "app:worker:worker-abc" {
		t.Errorf("expected 'app:worker:worker-abc', got '%s'", result)
	}
}

func TestKeys_Stats(t *testing.T) {
	t.Parallel()
	k := New("app")

	result := k.Stats()
	if result != "app:stats" {
		t.Errorf("expected 'app:stats', got '%s'", result)
	}
}

func TestKeys_Queues(t *testing.T) {
	t.Parallel()
	k := New("app")

	result := k.Queues()
	if result != "app:queues" {
		t.Errorf("expected 'app:queues', got '%s'", result)
	}
}

func TestKeys_Batch(t *testing.T) {
	t.Parallel()
	k := New("app")

	result := k.Batch("batch-123")
	if result != "app:batch:batch-123" {
		t.Errorf("expected 'app:batch:batch-123', got '%s'", result)
	}
}

func TestKeys_BatchJobs(t *testing.T) {
	t.Parallel()
	k := New("app")

	result := k.BatchJobs("batch-123")
	if result != "app:batch:batch-123:jobs" {
		t.Errorf("expected 'app:batch:batch-123:jobs', got '%s'", result)
	}
}

func TestKeys_Unique(t *testing.T) {
	t.Parallel()
	k := New("app")

	result := k.Unique("user:123:sync")
	if result != "app:unique:user:123:sync" {
		t.Errorf("expected 'app:unique:user:123:sync', got '%s'", result)
	}
}

func TestKeys_Periodic(t *testing.T) {
	t.Parallel()
	k := New("app")

	result := k.Periodic()
	if result != "app:periodic" {
		t.Errorf("expected 'app:periodic', got '%s'", result)
	}
}

func TestKeys_PeriodicLock(t *testing.T) {
	t.Parallel()
	k := New("app")

	result := k.PeriodicLock("daily_report")
	if result != "app:periodic:daily_report:lock" {
		t.Errorf("expected 'app:periodic:daily_report:lock', got '%s'", result)
	}
}

func TestKeys_Leader(t *testing.T) {
	t.Parallel()
	k := New("app")

	result := k.Leader()
	if result != "app:leader" {
		t.Errorf("expected 'app:leader', got '%s'", result)
	}
}

func TestKeys_RateLimit(t *testing.T) {
	t.Parallel()
	k := New("app")

	result := k.RateLimit("bucket", "api")
	if result != "app:ratelimit:bucket:api" {
		t.Errorf("expected 'app:ratelimit:bucket:api', got '%s'", result)
	}
}

func TestKeys_RateLimitBucket(t *testing.T) {
	t.Parallel()
	k := New("app")

	result := k.RateLimitBucket("api", 1704067200)
	if result != "app:ratelimit:bucket:api:1704067200" {
		t.Errorf("expected 'app:ratelimit:bucket:api:1704067200', got '%s'", result)
	}
}

func TestKeys_RateLimitWindow(t *testing.T) {
	t.Parallel()
	k := New("app")

	result := k.RateLimitWindow("api")
	if result != "app:ratelimit:window:api" {
		t.Errorf("expected 'app:ratelimit:window:api', got '%s'", result)
	}
}

func TestKeys_RateLimitConcurrent(t *testing.T) {
	t.Parallel()
	k := New("app")

	result := k.RateLimitConcurrent("api")
	if result != "app:ratelimit:concurrent:api" {
		t.Errorf("expected 'app:ratelimit:concurrent:api', got '%s'", result)
	}
}

func TestKeys_RateLimitConcurrentSlots(t *testing.T) {
	t.Parallel()
	k := New("app")

	result := k.RateLimitConcurrentSlots("api")
	if result != "app:ratelimit:concurrent:api:slots" {
		t.Errorf("expected 'app:ratelimit:concurrent:api:slots', got '%s'", result)
	}
}

func TestKeys_RateLimitConcurrentLocks(t *testing.T) {
	t.Parallel()
	k := New("app")

	result := k.RateLimitConcurrentLocks("api")
	if result != "app:ratelimit:concurrent:api:locks" {
		t.Errorf("expected 'app:ratelimit:concurrent:api:locks', got '%s'", result)
	}
}

func TestKeys_RateLimitConcurrentInit(t *testing.T) {
	t.Parallel()
	k := New("app")

	result := k.RateLimitConcurrentInit("api")
	if result != "app:ratelimit:concurrent:api:init" {
		t.Errorf("expected 'app:ratelimit:concurrent:api:init', got '%s'", result)
	}
}

func TestKeys_RateLimitLeaky(t *testing.T) {
	t.Parallel()
	k := New("app")

	result := k.RateLimitLeaky("api")
	if result != "app:ratelimit:leaky:api" {
		t.Errorf("expected 'app:ratelimit:leaky:api', got '%s'", result)
	}
}

func TestKeys_RateLimitPoints(t *testing.T) {
	t.Parallel()
	k := New("app")

	result := k.RateLimitPoints("api")
	if result != "app:ratelimit:points:api" {
		t.Errorf("expected 'app:ratelimit:points:api', got '%s'", result)
	}
}

func TestKeys_SpecialCharacters(t *testing.T) {
	t.Parallel()
	k := New("my-app")

	result := k.Queue("high-priority")
	if result != "my-app:queue:high-priority" {
		t.Errorf("expected 'my-app:queue:high-priority', got '%s'", result)
	}

	result = k.Unique("order:123:process")
	if result != "my-app:unique:order:123:process" {
		t.Errorf("expected 'my-app:unique:order:123:process', got '%s'", result)
	}
}
