package keys

import (
	"fmt"
	"testing"
)

func FuzzKeys(f *testing.F) {
	f.Add("", "default", int64(0))
	f.Add("myapp", "critical", int64(1_704_067_200))
	f.Add("tenant:region", "queue:name", int64(-42))

	f.Fuzz(func(t *testing.T, namespace string, name string, bucket int64) {
		if len(namespace) > 512 || len(name) > 512 {
			t.Skip()
		}

		k := New(namespace)
		wantNamespace := namespace
		if wantNamespace == "" {
			wantNamespace = "senna"
		}
		if k.Namespace() != wantNamespace {
			t.Fatalf("New(%q).Namespace() = %q, want %q", namespace, k.Namespace(), wantNamespace)
		}

		tests := []struct {
			method string
			got    string
			want   string
		}{
			{"Queue", k.Queue(name), wantNamespace + ":queue:" + name},
			{"InFlight", k.InFlight(name), wantNamespace + ":inflight:" + name},
			{"Worker", k.Worker(name), wantNamespace + ":worker:" + name},
			{"Batch", k.Batch(name), wantNamespace + ":batch:" + name},
			{"BatchJobs", k.BatchJobs(name), wantNamespace + ":batch:" + name + ":jobs"},
			{"BatchFailed", k.BatchFailed(name), wantNamespace + ":batch:" + name + ":failed"},
			{"BatchCallbacks", k.BatchCallbacks(name), wantNamespace + ":batch:" + name + ":callbacks"},
			{"Unique", k.Unique(name), wantNamespace + ":unique:" + name},
			{"PeriodicLock", k.PeriodicLock(name), wantNamespace + ":periodic:" + name + ":lock"},
			{"SequentialLock", k.SequentialLock(name), wantNamespace + ":sequential:" + name + ":lock"},
			{"RateLimit", k.RateLimit("bucket", name), wantNamespace + ":ratelimit:bucket:" + name},
			{"RateLimitBucket", k.RateLimitBucket(name, bucket), wantNamespace + ":ratelimit:bucket:" + name + ":" + fmt.Sprintf("%d", bucket)},
			{"RateLimitWindow", k.RateLimitWindow(name), wantNamespace + ":ratelimit:window:" + name},
			{"RateLimitConcurrent", k.RateLimitConcurrent(name), wantNamespace + ":ratelimit:concurrent:" + name},
			{"RateLimitConcurrentSlots", k.RateLimitConcurrentSlots(name), wantNamespace + ":ratelimit:concurrent:" + name + ":slots"},
			{"RateLimitConcurrentLocks", k.RateLimitConcurrentLocks(name), wantNamespace + ":ratelimit:concurrent:" + name + ":locks"},
			{"RateLimitConcurrentInit", k.RateLimitConcurrentInit(name), wantNamespace + ":ratelimit:concurrent:" + name + ":init"},
			{"RateLimitLeaky", k.RateLimitLeaky(name), wantNamespace + ":ratelimit:leaky:" + name},
			{"RateLimitPoints", k.RateLimitPoints(name), wantNamespace + ":ratelimit:points:" + name},
			{"IterationState", k.IterationState(name), wantNamespace + ":iteration:" + name},
		}

		for _, tt := range tests {
			if tt.got != tt.want {
				t.Errorf("New(%q).%s(%q) = %q, want %q", namespace, tt.method, name, tt.got, tt.want)
			}
		}
	})
}
