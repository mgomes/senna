package keys

import "fmt"

type Keys struct {
	namespace string
}

func New(namespace string) *Keys {
	if namespace == "" {
		namespace = "senna"
	}
	return &Keys{namespace: namespace}
}

func (k *Keys) Namespace() string {
	return k.namespace
}

func (k *Keys) prefix(parts ...string) string {
	key := k.namespace
	for _, p := range parts {
		key += ":" + p
	}
	return key
}

func (k *Keys) Queue(name string) string {
	return k.prefix("queue", name)
}

func (k *Keys) Scheduled() string {
	return k.prefix("scheduled")
}

func (k *Keys) Retry() string {
	return k.prefix("retry")
}

func (k *Keys) Dead() string {
	return k.prefix("dead")
}

func (k *Keys) InFlight(workerID string) string {
	return k.prefix("inflight", workerID)
}

func (k *Keys) Workers() string {
	return k.prefix("workers")
}

func (k *Keys) Worker(id string) string {
	return k.prefix("worker", id)
}

func (k *Keys) Stats() string {
	return k.prefix("stats")
}

func (k *Keys) Queues() string {
	return k.prefix("queues")
}

func (k *Keys) Batch(id string) string {
	return k.prefix("batch", id)
}

func (k *Keys) BatchJobs(id string) string {
	return k.prefix("batch", id, "jobs")
}

func (k *Keys) BatchFailed(id string) string {
	return k.prefix("batch", id, "failed")
}

func (k *Keys) BatchCallbacks(id string) string {
	return k.prefix("batch", id, "callbacks")
}

func (k *Keys) Batches() string {
	return k.prefix("batches")
}

func (k *Keys) DeadBatches() string {
	return k.prefix("batches", "dead")
}

func (k *Keys) Unique(key string) string {
	return k.prefix("unique", key)
}

func (k *Keys) Periodic() string {
	return k.prefix("periodic")
}

func (k *Keys) PeriodicLock(name string) string {
	return k.prefix("periodic", name, "lock")
}

func (k *Keys) SequentialLock(queueName string) string {
	return k.prefix("sequential", queueName, "lock")
}

func (k *Keys) Leader() string {
	return k.prefix("leader")
}

func (k *Keys) RateLimit(limiterType, name string) string {
	return k.prefix("ratelimit", limiterType, name)
}

func (k *Keys) RateLimitBucket(name string, bucket int64) string {
	return k.prefix("ratelimit", "bucket", name, fmt.Sprintf("%d", bucket))
}

func (k *Keys) RateLimitWindow(name string) string {
	return k.prefix("ratelimit", "window", name)
}

func (k *Keys) RateLimitConcurrent(name string) string {
	return k.prefix("ratelimit", "concurrent", name)
}

func (k *Keys) RateLimitConcurrentSlots(name string) string {
	return k.prefix("ratelimit", "concurrent", name, "slots")
}

func (k *Keys) RateLimitConcurrentLocks(name string) string {
	return k.prefix("ratelimit", "concurrent", name, "locks")
}

func (k *Keys) RateLimitConcurrentInit(name string) string {
	return k.prefix("ratelimit", "concurrent", name, "init")
}

func (k *Keys) RateLimitLeaky(name string) string {
	return k.prefix("ratelimit", "leaky", name)
}

func (k *Keys) RateLimitPoints(name string) string {
	return k.prefix("ratelimit", "points", name)
}

func (k *Keys) IterationState(jobID string) string {
	return k.prefix("iteration", jobID)
}
