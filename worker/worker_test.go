package worker

import (
	"context"
	"crypto/aes"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/mgomes/senna"
	"github.com/mgomes/senna/internal/batch"
	"github.com/redis/go-redis/v9"
)

func TestWorker_New_DefaultSettings(t *testing.T) {
	defaults := senna.DefaultWorkerSettings()

	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-worker-default",
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	if w.config.Settings.Concurrency != 10 {
		t.Errorf("expected default Concurrency 10, got %d", w.config.Settings.Concurrency)
	}
	if len(w.config.Settings.Queues) != 1 {
		t.Fatalf("expected 1 default queue, got %d", len(w.config.Settings.Queues))
	}
	if w.config.Settings.Queues[0].Name != "default" {
		t.Errorf("expected default queue 'default', got '%s'", w.config.Settings.Queues[0].Name)
	}
	if w.config.Settings.ReaperInterval != defaults.ReaperInterval {
		t.Errorf("expected ReaperInterval %v, got %v", defaults.ReaperInterval, w.config.Settings.ReaperInterval)
	}
	if w.config.Settings.SequentialLockTTL != defaults.SequentialLockTTL {
		t.Errorf("expected SequentialLockTTL %v, got %v", defaults.SequentialLockTTL, w.config.Settings.SequentialLockTTL)
	}
	if w.fetcher.sequentialLockTTL != defaults.SequentialLockTTL {
		t.Errorf("expected fetcher sequentialLockTTL %v, got %v", defaults.SequentialLockTTL, w.fetcher.sequentialLockTTL)
	}
	if w.sequentialLockRenewEvery != defaults.SequentialLockRenewInterval {
		t.Errorf("expected sequentialLockRenewEvery %v, got %v", defaults.SequentialLockRenewInterval, w.sequentialLockRenewEvery)
	}
	if w.config.Settings.IterableMaxRuntime != defaults.IterableMaxRuntime {
		t.Errorf("expected IterableMaxRuntime %v, got %v", defaults.IterableMaxRuntime, w.config.Settings.IterableMaxRuntime)
	}
	if w.config.Settings.PeriodicPollInterval != defaults.PeriodicPollInterval {
		t.Errorf("expected PeriodicPollInterval %v, got %v", defaults.PeriodicPollInterval, w.config.Settings.PeriodicPollInterval)
	}
}

func TestWorker_New_WithSettings(t *testing.T) {
	const (
		reaperInterval              = 45 * time.Second
		sequentialLockTTL           = 2 * time.Minute
		sequentialLockRenewInterval = 20 * time.Second
		iterableMaxRuntime          = 2 * time.Minute
		periodicPollInterval        = time.Minute
	)

	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-worker-settings",
		Settings: senna.WorkerSettings{
			Concurrency:                 5,
			Queues:                      []senna.QueueConfig{{Name: "high", Priority: 10}, {Name: "low", Priority: 1}},
			ShutdownTimeout:             time.Minute,
			PollInterval:                50 * time.Millisecond,
			BlockTimeout:                3 * time.Second,
			HeartbeatRate:               time.Second,
			ReaperInterval:              reaperInterval,
			SequentialLockTTL:           sequentialLockTTL,
			SequentialLockRenewInterval: sequentialLockRenewInterval,
			IterableMaxRuntime:          iterableMaxRuntime,
			PeriodicPollInterval:        periodicPollInterval,
			PeriodicEnabled:             true,
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	if w.config.Settings.Concurrency != 5 {
		t.Errorf("expected Concurrency 5, got %d", w.config.Settings.Concurrency)
	}
	if len(w.config.Settings.Queues) != 2 {
		t.Fatalf("expected 2 queues, got %d", len(w.config.Settings.Queues))
	}
	if w.config.Settings.BlockTimeout != 3*time.Second {
		t.Errorf("expected BlockTimeout 3s, got %v", w.config.Settings.BlockTimeout)
	}
	if w.config.Settings.ReaperInterval != reaperInterval {
		t.Errorf("expected ReaperInterval %v, got %v", reaperInterval, w.config.Settings.ReaperInterval)
	}
	if w.config.Settings.SequentialLockTTL != sequentialLockTTL {
		t.Errorf("expected SequentialLockTTL %v, got %v", sequentialLockTTL, w.config.Settings.SequentialLockTTL)
	}
	if w.fetcher.sequentialLockTTL != sequentialLockTTL {
		t.Errorf("expected fetcher sequentialLockTTL %v, got %v", sequentialLockTTL, w.fetcher.sequentialLockTTL)
	}
	if w.sequentialLockRenewEvery != sequentialLockRenewInterval {
		t.Errorf("expected sequentialLockRenewEvery %v, got %v", sequentialLockRenewInterval, w.sequentialLockRenewEvery)
	}
	if w.config.Settings.IterableMaxRuntime != iterableMaxRuntime {
		t.Errorf("expected IterableMaxRuntime %v, got %v", iterableMaxRuntime, w.config.Settings.IterableMaxRuntime)
	}
	if w.config.Settings.PeriodicPollInterval != periodicPollInterval {
		t.Errorf("expected PeriodicPollInterval %v, got %v", periodicPollInterval, w.config.Settings.PeriodicPollInterval)
	}
	if w.periodic == nil {
		t.Fatal("expected periodic scheduler to be configured")
	}
	if w.periodic.PollInterval() != periodicPollInterval {
		t.Errorf("expected periodic scheduler poll interval %v, got %v", periodicPollInterval, w.periodic.PollInterval())
	}
}

func TestWorker_New_PartialSettings(t *testing.T) {
	defaults := senna.DefaultWorkerSettings()

	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-worker-partial-settings",
		Settings: senna.WorkerSettings{
			Concurrency:     1,
			PeriodicEnabled: true,
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	if w.config.Settings.Concurrency != 1 {
		t.Errorf("expected Concurrency 1, got %d", w.config.Settings.Concurrency)
	}
	if diff := cmp.Diff(defaults.Queues, w.config.Settings.Queues); diff != "" {
		t.Errorf("Queues mismatch (-want +got):\n%s", diff)
	}
	if w.config.Settings.ShutdownTimeout != defaults.ShutdownTimeout {
		t.Errorf("expected ShutdownTimeout %v, got %v", defaults.ShutdownTimeout, w.config.Settings.ShutdownTimeout)
	}
	if w.config.Settings.PollInterval != defaults.PollInterval {
		t.Errorf("expected PollInterval %v, got %v", defaults.PollInterval, w.config.Settings.PollInterval)
	}
	if w.config.Settings.BlockTimeout != defaults.BlockTimeout {
		t.Errorf("expected BlockTimeout %v, got %v", defaults.BlockTimeout, w.config.Settings.BlockTimeout)
	}
	if w.config.Settings.ScheduledPollInterval != defaults.ScheduledPollInterval {
		t.Errorf("expected ScheduledPollInterval %v, got %v", defaults.ScheduledPollInterval, w.config.Settings.ScheduledPollInterval)
	}
	if w.config.Settings.HeartbeatRate != defaults.HeartbeatRate {
		t.Errorf("expected HeartbeatRate %v, got %v", defaults.HeartbeatRate, w.config.Settings.HeartbeatRate)
	}
	if w.config.Settings.ReaperOperationTimeout != defaults.ReaperOperationTimeout {
		t.Errorf("expected ReaperOperationTimeout %v, got %v", defaults.ReaperOperationTimeout, w.config.Settings.ReaperOperationTimeout)
	}
	if w.config.Settings.ReaperInterval != defaults.ReaperInterval {
		t.Errorf("expected ReaperInterval %v, got %v", defaults.ReaperInterval, w.config.Settings.ReaperInterval)
	}
	if w.config.Settings.SequentialLockTTL != defaults.SequentialLockTTL {
		t.Errorf("expected SequentialLockTTL %v, got %v", defaults.SequentialLockTTL, w.config.Settings.SequentialLockTTL)
	}
	if w.config.Settings.SequentialLockRenewInterval != defaults.SequentialLockRenewInterval {
		t.Errorf("expected SequentialLockRenewInterval %v, got %v", defaults.SequentialLockRenewInterval, w.config.Settings.SequentialLockRenewInterval)
	}
	if w.config.Settings.IterableMaxRuntime != defaults.IterableMaxRuntime {
		t.Errorf("expected IterableMaxRuntime %v, got %v", defaults.IterableMaxRuntime, w.config.Settings.IterableMaxRuntime)
	}
	if w.config.Settings.PeriodicPollInterval != defaults.PeriodicPollInterval {
		t.Errorf("expected PeriodicPollInterval %v, got %v", defaults.PeriodicPollInterval, w.config.Settings.PeriodicPollInterval)
	}
	if !w.config.Settings.PeriodicEnabled {
		t.Error("expected PeriodicEnabled to remain true")
	}
}

func TestWorkerBlockTimeoutLeavesShutdownHeadroom(t *testing.T) {
	t.Parallel()
	defaults := senna.DefaultWorkerSettings()

	tests := []struct {
		name     string
		settings senna.WorkerSettings
		want     time.Duration
	}{
		{
			name:     "default",
			settings: defaults,
			want:     2 * time.Second,
		},
		{
			name: "shorter than shutdown",
			settings: senna.WorkerSettings{
				BlockTimeout:    500 * time.Millisecond,
				ShutdownTimeout: time.Second,
			},
			want: 500 * time.Millisecond,
		},
		{
			name: "longer than shutdown",
			settings: senna.WorkerSettings{
				BlockTimeout:    5 * time.Second,
				ShutdownTimeout: time.Second,
			},
			want: 500 * time.Millisecond,
		},
		{
			name: "missing block timeout",
			settings: senna.WorkerSettings{
				ShutdownTimeout: 30 * time.Second,
			},
			want: 2 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := workerBlockTimeout(tt.settings); got != tt.want {
				t.Errorf("workerBlockTimeout(%+v) = %v, want %v", tt.settings, got, tt.want)
			}
		})
	}
}

func TestNormalizeWorkerSettings_SequentialLockRenewInterval(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		settings senna.WorkerSettings
		want     time.Duration
	}{
		{
			name:     "default",
			settings: senna.WorkerSettings{},
			want:     senna.DefaultWorkerSettings().SequentialLockRenewInterval,
		},
		{
			name: "explicit below ttl",
			settings: senna.WorkerSettings{
				SequentialLockTTL:           time.Minute,
				SequentialLockRenewInterval: 20 * time.Second,
			},
			want: 20 * time.Second,
		},
		{
			name: "zero with shorter ttl derives safe interval",
			settings: senna.WorkerSettings{
				SequentialLockTTL: 3 * time.Second,
			},
			want: time.Second,
		},
		{
			name: "too large derives safe interval",
			settings: senna.WorkerSettings{
				SequentialLockTTL:           6 * time.Second,
				SequentialLockRenewInterval: 6 * time.Second,
			},
			want: 2 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := normalizeWorkerSettings(tt.settings).SequentialLockRenewInterval
			if got != tt.want {
				t.Errorf("normalizeWorkerSettings(%+v).SequentialLockRenewInterval = %v, want %v", tt.settings, got, tt.want)
			}
		})
	}
}

func TestWorkerReaperInterval(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		settings senna.WorkerSettings
		want     time.Duration
	}{
		{
			name:     "default",
			settings: senna.WorkerSettings{},
			want:     senna.DefaultWorkerSettings().ReaperInterval,
		},
		{
			name: "explicit",
			settings: senna.WorkerSettings{
				ReaperInterval: 45 * time.Second,
			},
			want: 45 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := workerReaperInterval(tt.settings); got != tt.want {
				t.Errorf("workerReaperInterval(%+v) = %v, want %v", tt.settings, got, tt.want)
			}
		})
	}
}

func TestWorker_New_EncryptionEnabled(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}

	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-worker-enc",
		Settings:  senna.DefaultWorkerSettings(),
		Encryption: &senna.EncryptionSettings{
			Enabled: true,
			Key:     key,
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	if w.encryptor == nil {
		t.Error("encryptor should be initialized")
	}
	if len(w.middleware) < 2 {
		t.Error("middleware should include encryption and recovery")
	}
}

func TestWorker_New_InvalidEncryptionKey(t *testing.T) {
	_, err := New(&Config{
		Redis: senna.RedisConfig{
			Addr:        "127.0.0.1:0",
			DialTimeout: time.Millisecond,
		},
		Namespace: "test-worker-invalid-enc",
		Settings:  senna.DefaultWorkerSettings(),
		Encryption: &senna.EncryptionSettings{
			Enabled: true,
			Key:     []byte("tooshort"),
		},
	})
	if err == nil {
		t.Error("expected error for invalid encryption key")
	}
	var keySizeErr aes.KeySizeError
	if !errors.As(err, &keySizeErr) {
		t.Fatalf("New() error = %v, want aes.KeySizeError", err)
	}
}

func TestWorker_StopIsIdempotent(t *testing.T) {
	w := newLifecycleTestWorker(t, "test-worker-stop-idempotent")

	errCh := runWorker(t, w)
	waitForWorkerRunning(t, w)

	w.Stop()
	w.Stop()

	if err := waitForWorkerExit(t, errCh); err != nil {
		t.Fatalf("Worker.Run() error = %v, want nil", err)
	}
}

func TestWorker_RunRestartsAfterCleanShutdown(t *testing.T) {
	w := newLifecycleTestWorker(t, "test-worker-run-restart")

	errCh := runWorker(t, w)
	waitForWorkerRunning(t, w)
	w.Stop()
	if err := waitForWorkerExit(t, errCh); err != nil {
		t.Fatalf("first Worker.Run() error = %v, want nil", err)
	}

	errCh = runWorker(t, w)
	waitForWorkerRunning(t, w)
	w.Stop()
	if err := waitForWorkerExit(t, errCh); err != nil {
		t.Fatalf("second Worker.Run() error = %v, want nil", err)
	}
}

func TestWorker_RunRestartsAfterTimedOutShutdownCompletes(t *testing.T) {
	w := newLifecycleTestWorker(t, "test-worker-timeout-restart")
	w.config.Settings.ShutdownTimeout = 10 * time.Millisecond

	started := make(chan struct{})
	finished := make(chan struct{})
	w.Register("slow_job", func(ctx context.Context, job *senna.Job) error {
		close(started)
		time.Sleep(50 * time.Millisecond)
		close(finished)
		return nil
	})

	job := senna.NewJob("slow_job", nil)
	data, err := job.Marshal()
	if err != nil {
		t.Fatalf("Job.Marshal() error = %v", err)
	}
	if err := w.redis.LPush(context.Background(), w.keys.Queue("default"), string(data)).Err(); err != nil {
		t.Fatalf("LPush(slow_job) error = %v", err)
	}

	errCh := runWorker(t, w)
	waitForWorkerRunning(t, w)

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("slow_job handler did not start")
	}

	w.Stop()
	if err := waitForWorkerExit(t, errCh); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Worker.Run() error = %v, want %v", err, context.DeadlineExceeded)
	}

	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("slow_job handler did not finish")
	}
	waitForWorkerStopped(t, w)

	errCh = runWorker(t, w)
	waitForWorkerRunning(t, w)
	w.Stop()
	if err := waitForWorkerExit(t, errCh); err != nil {
		t.Fatalf("second Worker.Run() error = %v, want nil", err)
	}
}

func TestWorker_RunRejectsRestartUntilTimedOutShutdownCompletes(t *testing.T) {
	w := newLifecycleTestWorker(t, "test-worker-timeout-restart-window")
	w.config.Settings.ShutdownTimeout = 10 * time.Millisecond

	started := make(chan struct{})
	release := make(chan struct{})
	w.Register("stuck_job", func(ctx context.Context, job *senna.Job) error {
		close(started)
		<-release
		return nil
	})

	job := senna.NewJob("stuck_job", nil)
	data, err := job.Marshal()
	if err != nil {
		t.Fatalf("Job.Marshal() error = %v, want nil", err)
	}
	if err := w.redis.LPush(context.Background(), w.keys.Queue("default"), string(data)).Err(); err != nil {
		t.Fatalf("LPush(stuck_job) error = %v, want nil", err)
	}

	errCh := runWorker(t, w)
	waitForWorkerRunning(t, w)

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("stuck_job handler did not start")
	}

	w.Stop()
	if err := waitForWorkerExit(t, errCh); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Worker.Run() error = %v, want %v", err, context.DeadlineExceeded)
	}

	restartErrCh := runWorker(t, w)
	if err := waitForWorkerExit(t, restartErrCh); !errors.Is(err, errWorkerAlreadyRunning) {
		t.Fatalf("second Worker.Run() error = %v, want %v", err, errWorkerAlreadyRunning)
	}

	w.Stop()
	close(release)
	waitForWorkerStopped(t, w)

	errCh = runWorker(t, w)
	waitForWorkerRunning(t, w)
	w.Stop()
	if err := waitForWorkerExit(t, errCh); err != nil {
		t.Fatalf("third Worker.Run() error = %v, want nil", err)
	}
}

func TestWorker_SequentialLockReleasedAfterJobCompletion(t *testing.T) {
	const queueName = "serial"
	w := newSequentialLifecycleTestWorker(t, "test-worker-seq-lock-release", queueName)

	finished := make(chan struct{})
	w.Register("fast_sequential_job", func(ctx context.Context, job *senna.Job) error {
		close(finished)
		return nil
	})

	job := senna.NewJob("fast_sequential_job", nil)
	job.Queue = queueName
	enqueueWorkerJob(t, w, job)

	errCh := runWorker(t, w)
	waitForWorkerRunning(t, w)

	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("fast_sequential_job handler did not finish")
	}

	waitForSequentialLockReleased(t, w, queueName)

	w.Stop()
	if err := waitForWorkerExit(t, errCh); err != nil {
		t.Fatalf("Worker.Run() error = %v, want nil", err)
	}
}

func TestWorker_SequentialLockRenewalStopsAfterShutdownTimeout(t *testing.T) {
	const queueName = "serial"
	w := newSequentialLifecycleTestWorker(t, "test-worker-seq-renew-timeout", queueName)
	w.config.Settings.ShutdownTimeout = 20 * time.Millisecond
	w.sequentialLockRenewEvery = 5 * time.Millisecond

	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseHandler := func() {
		releaseOnce.Do(func() {
			close(release)
		})
	}
	t.Cleanup(releaseHandler)

	w.Register("stuck_sequential_job", func(ctx context.Context, job *senna.Job) error {
		close(started)
		<-release
		return nil
	})

	job := senna.NewJob("stuck_sequential_job", nil)
	job.Queue = queueName
	enqueueWorkerJob(t, w, job)

	errCh := runWorker(t, w)
	waitForWorkerRunning(t, w)

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("stuck_sequential_job handler did not start")
	}
	waitForSequentialLockHolder(t, w, queueName, w.id)

	w.Stop()
	if err := waitForWorkerExit(t, errCh); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Worker.Run() error = %v, want %v", err, context.DeadlineExceeded)
	}

	if err := w.redis.PExpire(context.Background(), w.keys.SequentialLock(queueName), 25*time.Millisecond).Err(); err != nil {
		t.Fatalf("PExpire(%q) error = %v, want nil", w.keys.SequentialLock(queueName), err)
	}
	waitForSequentialLockReleased(t, w, queueName)

	releaseHandler()
	waitForWorkerStopped(t, w)
}

func TestWorker_FinalizationShutdownLogPreservesContextValues(t *testing.T) {
	key := workerLogContextKey("trace_id")
	values := captureLogContextValue(t, key, "leaving job in-flight after finalization failure during shutdown")

	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), key, "trace-finalization"))
	cancel()

	w := &Worker{}
	job := senna.NewJob("finalized_job", nil)
	if retry := w.waitToRetryFinalization(ctx, job, jobFinalizationComplete, errors.New("finalization failed")); retry {
		t.Fatal("waitToRetryFinalization(canceled ctx) = true, want false")
	}

	assertLoggedContextValue(t, values, "trace-finalization")
}

func TestWorker_HeartbeatRemovalLogPreservesContextValues(t *testing.T) {
	w := newLifecycleTestWorker(t, "test-worker-heartbeat-log-context")

	if err := w.redis.Set(context.Background(), w.keys.Workers(), "not-a-hash", 0).Err(); err != nil {
		t.Fatalf("Set(workers wrong type) error = %v, want nil", err)
	}

	key := workerLogContextKey("trace_id")
	values := captureLogContextValue(t, key, "failed to remove worker heartbeat")

	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), key, "trace-heartbeat"))
	cancel()
	w.heartbeat(ctx)

	assertLoggedContextValue(t, values, "trace-heartbeat")
}

type workerLogContextKey string

type contextValueLogHandler struct {
	key     any
	message string
	values  chan any
}

func (h *contextValueLogHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h *contextValueLogHandler) Handle(ctx context.Context, record slog.Record) error {
	if record.Message != h.message {
		return nil
	}

	select {
	case h.values <- ctx.Value(h.key):
	default:
	}
	return nil
}

func (h *contextValueLogHandler) WithAttrs([]slog.Attr) slog.Handler {
	return h
}

func (h *contextValueLogHandler) WithGroup(string) slog.Handler {
	return h
}

func captureLogContextValue(t *testing.T, key any, message string) <-chan any {
	t.Helper()

	values := make(chan any, 1)
	previous := slog.Default()
	slog.SetDefault(slog.New(&contextValueLogHandler{
		key:     key,
		message: message,
		values:  values,
	}))
	t.Cleanup(func() {
		slog.SetDefault(previous)
	})
	return values
}

func assertLoggedContextValue(t *testing.T, values <-chan any, want string) {
	t.Helper()

	select {
	case got := <-values:
		if got != want {
			t.Fatalf("logged context value = %v, want %q", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("log entry was not captured")
	}
}

func newLifecycleTestWorker(t *testing.T, namespace string) *Worker {
	t.Helper()

	settings := senna.DefaultWorkerSettings()
	settings.Concurrency = 1
	settings.PollInterval = 10 * time.Millisecond
	settings.ShutdownTimeout = time.Second
	settings.HeartbeatRate = time.Hour
	settings.ScheduledPollInterval = time.Hour

	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: namespace,
		Settings:  settings,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	flushTestKeys(t, w.redis, namespace+":*")
	t.Cleanup(func() {
		flushTestKeys(t, w.redis, namespace+":*")
		_ = w.redis.Close()
	})

	return w
}

func newSequentialLifecycleTestWorker(t *testing.T, namespace, queueName string) *Worker {
	t.Helper()

	settings := senna.DefaultWorkerSettings()
	settings.Concurrency = 1
	settings.Queues = []senna.QueueConfig{{Name: queueName, Priority: 1, Sequential: true}}
	settings.PollInterval = 10 * time.Millisecond
	settings.ShutdownTimeout = time.Second
	settings.HeartbeatRate = time.Hour
	settings.ScheduledPollInterval = time.Hour

	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: namespace,
		Settings:  settings,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	flushTestKeys(t, w.redis, namespace+":*")
	t.Cleanup(func() {
		flushTestKeys(t, w.redis, namespace+":*")
		_ = w.redis.Close()
	})

	return w
}

func enqueueWorkerJob(t *testing.T, w *Worker, job *senna.Job) {
	t.Helper()

	data, err := job.Marshal()
	if err != nil {
		t.Fatalf("Job.Marshal() error = %v, want nil", err)
	}
	if err := w.redis.LPush(context.Background(), w.keys.Queue(job.Queue), string(data)).Err(); err != nil {
		t.Fatalf("LPush(%q) error = %v, want nil", w.keys.Queue(job.Queue), err)
	}
}

func waitForSequentialLockHolder(t *testing.T, w *Worker, queueName, want string) {
	t.Helper()

	lockKey := w.keys.SequentialLock(queueName)
	deadline := time.After(time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		got, err := w.redis.Get(context.Background(), lockKey).Result()
		if err == nil && got == want {
			return
		}
		if err != nil && !errors.Is(err, redis.Nil) {
			t.Fatalf("Get(%q) error = %v, want nil or redis.Nil", lockKey, err)
		}

		select {
		case <-deadline:
			t.Fatalf("sequential lock holder for %q did not become %q", queueName, want)
		case <-ticker.C:
		}
	}
}

func waitForSequentialLockReleased(t *testing.T, w *Worker, queueName string) {
	t.Helper()

	lockKey := w.keys.SequentialLock(queueName)
	deadline := time.After(time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		exists, err := w.redis.Exists(context.Background(), lockKey).Result()
		if err != nil {
			t.Fatalf("Exists(%q) error = %v, want nil", lockKey, err)
		}
		if exists == 0 {
			return
		}

		select {
		case <-deadline:
			t.Fatalf("sequential lock %q still exists", lockKey)
		case <-ticker.C:
		}
	}
}

func runWorker(t *testing.T, w *Worker) <-chan error {
	t.Helper()

	errCh := make(chan error, 1)
	go func() {
		errCh <- w.Run(context.Background())
	}()
	return errCh
}

func waitForWorkerRunning(t *testing.T, w *Worker) {
	t.Helper()

	deadline := time.After(time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		w.mu.RLock()
		running := w.running
		w.mu.RUnlock()
		if running {
			return
		}

		select {
		case <-deadline:
			t.Fatal("Worker.Run() did not enter running state")
		case <-ticker.C:
		}
	}
}

func waitForWorkerExit(t *testing.T, errCh <-chan error) error {
	t.Helper()

	select {
	case err := <-errCh:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("Worker.Run() did not stop")
		return nil
	}
}

func waitForWorkerStopped(t *testing.T, w *Worker) {
	t.Helper()

	deadline := time.After(time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		w.mu.RLock()
		running := w.running
		stopping := w.stopping
		w.mu.RUnlock()
		if !running && !stopping {
			return
		}

		select {
		case <-deadline:
			t.Fatal("Worker.Run() did not clear lifecycle state")
		case <-ticker.C:
		}
	}
}

func TestWorker_Register_WithOptions(t *testing.T) {
	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-worker-register",
		Settings:  senna.DefaultWorkerSettings(),
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	called := false
	w.Register("test_job", func(ctx context.Context, job *senna.Job) error {
		called = true
		return nil
	}, WithJobMaxRetries(3), WithJobTimeout(time.Second))

	job := senna.NewJob("test_job", nil)
	opts, _ := w.handlers.process(context.Background(), job)

	if !called {
		t.Error("handler should have been called")
	}
	if opts == nil {
		t.Fatal("options should be returned")
	}
	if opts.MaxRetries != 3 {
		t.Errorf("expected MaxRetries 3, got %d", opts.MaxRetries)
	}
}

func TestWorker_Use_AddsMiddleware(t *testing.T) {
	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-worker-mw",
		Settings:  senna.DefaultWorkerSettings(),
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	middlewareCalled := false
	w.Use(func(next senna.Handler) senna.Handler {
		return func(ctx context.Context, job *senna.Job) error {
			middlewareCalled = true
			return next(ctx, job)
		}
	})

	w.Register("test_job", func(ctx context.Context, job *senna.Job) error {
		return nil
	})

	job := senna.NewJob("test_job", nil)
	_, _ = w.handlers.process(context.Background(), job)

	if !middlewareCalled {
		t.Error("middleware should have been called")
	}
}

func TestWorker_Redis_ReturnsClient(t *testing.T) {
	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-worker-redis",
		Settings:  senna.DefaultWorkerSettings(),
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	client := w.Redis()
	if client == nil {
		t.Fatal("Redis() should return client")
	}

	err = client.Ping(context.Background()).Err()
	if err != nil {
		t.Errorf("Redis client should be connected: %v", err)
	}
}

func TestWorker_RecordHeartbeatWritesWorkerInfo(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-worker-heartbeat:*")

	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-worker-heartbeat",
		Settings: senna.WorkerSettings{
			Concurrency:   3,
			Queues:        []senna.QueueConfig{{Name: "default", Priority: 1}},
			HeartbeatRate: time.Second,
		},
	})
	if err != nil {
		t.Fatalf("New(worker heartbeat) error = %v, want nil", err)
	}
	defer func() { _ = w.redis.Close() }()

	ctx := context.Background()
	now := time.Unix(1_704_067_200, 0)
	if err := w.recordHeartbeat(ctx, now); err != nil {
		t.Fatalf("recordHeartbeat(%v) error = %v, want nil", now, err)
	}

	data, err := redisClient.HGet(ctx, w.keys.Workers(), w.id).Result()
	if err != nil {
		t.Fatalf("HGet(%q, %q) error = %v, want nil", w.keys.Workers(), w.id, err)
	}

	var got workerHeartbeatInfo
	if err := json.Unmarshal([]byte(data), &got); err != nil {
		t.Fatalf("json.Unmarshal(recordHeartbeat data) error = %v, want nil", err)
	}
	if got.BeatAt != now.Unix() {
		t.Fatalf("recordHeartbeat(%v).BeatAt = %d, want %d", now, got.BeatAt, now.Unix())
	}
	if got.Concurrency != 3 {
		t.Fatalf("recordHeartbeat(%v).Concurrency = %d, want 3", now, got.Concurrency)
	}
	if len(got.Queues) != 1 || got.Queues[0].Name != "default" {
		t.Fatalf("recordHeartbeat(%v).Queues = %+v, want default queue", now, got.Queues)
	}
}

func TestWorker_HeartbeatHelpersReturnRedisErrors(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-worker-heartbeat-error:*")

	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-worker-heartbeat-error",
		Settings:  senna.DefaultWorkerSettings(),
	})
	if err != nil {
		t.Fatalf("New(worker heartbeat error) error = %v, want nil", err)
	}
	defer func() { _ = w.redis.Close() }()

	ctx := context.Background()
	if err := redisClient.Set(ctx, w.keys.Workers(), "not-a-hash", 0).Err(); err != nil {
		t.Fatalf("Set(%q, wrong type) error = %v, want nil", w.keys.Workers(), err)
	}

	err = w.recordHeartbeat(ctx, time.Unix(1_704_067_200, 0))
	if err == nil {
		t.Fatal("recordHeartbeat(wrong workers key type) error = nil, want error")
	}

	err = w.removeHeartbeat(ctx)
	if err == nil {
		t.Fatal("removeHeartbeat(wrong workers key type) error = nil, want error")
	}
}

func TestWorker_JobOptions(t *testing.T) {
	limiter := &testRateLimiter{}

	tests := []struct {
		name     string
		option   JobOption
		validate func(*testing.T, *JobOptions)
	}{
		{
			name:   "WithJobMaxRetries",
			option: WithJobMaxRetries(5),
			validate: func(t *testing.T, opts *JobOptions) {
				if opts.MaxRetries != 5 {
					t.Errorf("expected MaxRetries 5, got %d", opts.MaxRetries)
				}
			},
		},
		{
			name:   "WithMaxRetries",
			option: WithMaxRetries(5),
			validate: func(t *testing.T, opts *JobOptions) {
				if opts.MaxRetries != 5 {
					t.Errorf("expected MaxRetries 5, got %d", opts.MaxRetries)
				}
			},
		},
		{
			name:   "WithJobTimeout",
			option: WithJobTimeout(30 * time.Second),
			validate: func(t *testing.T, opts *JobOptions) {
				if opts.Timeout != 30*time.Second {
					t.Errorf("expected Timeout 30s, got %v", opts.Timeout)
				}
			},
		},
		{
			name:   "WithMaxConcurrency",
			option: WithMaxConcurrency(3),
			validate: func(t *testing.T, opts *JobOptions) {
				if opts.MaxConcurrency != 3 {
					t.Errorf("expected MaxConcurrency 3, got %d", opts.MaxConcurrency)
				}
			},
		},
		{
			name:   "WithUniqueJob",
			option: WithUniqueJob("user:123", time.Hour),
			validate: func(t *testing.T, opts *JobOptions) {
				if opts.Unique == nil {
					t.Fatal("expected Unique config")
				}
				if opts.Unique.Key != "user:123" {
					t.Errorf("expected Key 'user:123', got '%s'", opts.Unique.Key)
				}
				if opts.Unique.TTL != time.Hour {
					t.Errorf("expected TTL 1h, got %v", opts.Unique.TTL)
				}
			},
		},
		{
			name:   "WithJobRateLimiter",
			option: WithJobRateLimiter(limiter),
			validate: func(t *testing.T, opts *JobOptions) {
				if opts.RateLimiter != limiter {
					t.Errorf("expected RateLimiter %v, got %v", limiter, opts.RateLimiter)
				}
			},
		},
		{
			name:   "WithRateLimiter",
			option: WithRateLimiter(limiter),
			validate: func(t *testing.T, opts *JobOptions) {
				if opts.RateLimiter != limiter {
					t.Errorf("expected RateLimiter %v, got %v", limiter, opts.RateLimiter)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := &JobOptions{
				MaxRetries:   25,
				RetryBackoff: senna.DefaultBackoff(),
			}
			tt.option(opts)
			tt.validate(t, opts)
		})
	}
}

func TestWorker_IterableJobOptions(t *testing.T) {
	limiter := &testRateLimiter{}

	tests := []struct {
		name     string
		option   IterableJobOption
		validate func(*testing.T, *IterableJobOptions)
	}{
		{
			name:   "WithIterableMaxRetries",
			option: WithIterableMaxRetries(2),
			validate: func(t *testing.T, opts *IterableJobOptions) {
				if opts.MaxRetries != 2 {
					t.Errorf("expected MaxRetries 2, got %d", opts.MaxRetries)
				}
			},
		},
		{
			name:   "WithIterableTimeout",
			option: WithIterableTimeout(3 * time.Second),
			validate: func(t *testing.T, opts *IterableJobOptions) {
				if opts.Timeout != 3*time.Second {
					t.Errorf("expected Timeout 3s, got %v", opts.Timeout)
				}
			},
		},
		{
			name:   "WithIterableMaxRuntime",
			option: WithIterableMaxRuntime(3 * time.Second),
			validate: func(t *testing.T, opts *IterableJobOptions) {
				if opts.MaxRuntime != 3*time.Second {
					t.Errorf("expected MaxRuntime 3s, got %v", opts.MaxRuntime)
				}
			},
		},
		{
			name:   "WithIterableRateLimiter",
			option: WithIterableRateLimiter(limiter),
			validate: func(t *testing.T, opts *IterableJobOptions) {
				if opts.RateLimiter != limiter {
					t.Errorf("expected RateLimiter %v, got %v", limiter, opts.RateLimiter)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := &IterableJobOptions{
				MaxRetries:   25,
				RetryBackoff: senna.DefaultBackoff(),
			}
			tt.option(opts)
			tt.validate(t, opts)
		})
	}
}

func TestWorker_ProcessJob_HandlerError(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-process-error:*")

	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-process-error",
		Settings: senna.WorkerSettings{
			Concurrency:     1,
			Queues:          []senna.QueueConfig{{Name: "default", Priority: 1}},
			ShutdownTimeout: time.Second,
			PollInterval:    50 * time.Millisecond,
			HeartbeatRate:   time.Second,
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	w.Register("failing_job", func(ctx context.Context, job *senna.Job) error {
		return errors.New("job failed")
	}, WithJobMaxRetries(1))

	job := senna.NewJob("failing_job", nil)
	opts, err := w.handlers.process(context.Background(), job)

	if err == nil {
		t.Error("expected error from handler")
	}
	if opts == nil {
		t.Error("expected options to be returned")
	}
}

func TestWorker_ProcessJob_RetryableError(t *testing.T) {
	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-process-retry",
		Settings:  senna.DefaultWorkerSettings(),
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	w.Register("retryable_job", func(ctx context.Context, job *senna.Job) error {
		return &senna.RetryableError{
			Job:     job,
			Cause:   errors.New("temporary failure"),
			RetryIn: 5 * time.Second,
		}
	})

	job := senna.NewJob("retryable_job", nil)
	_, err = w.handlers.process(context.Background(), job)

	var retryErr *senna.RetryableError
	if !errors.As(err, &retryErr) {
		t.Fatalf("expected RetryableError, got %T: %v", err, err)
	}
	if retryErr.RetryIn != 5*time.Second {
		t.Errorf("expected RetryIn 5s, got %v", retryErr.RetryIn)
	}
}

func TestWorker_DefaultBackoff(t *testing.T) {
	backoff := senna.DefaultBackoff()

	d0 := backoff(0)
	d1 := backoff(1)
	d5 := backoff(5)

	if d0 < 15*time.Second {
		t.Errorf("expected d0 >= 15s, got %v", d0)
	}
	if d1 <= d0 {
		t.Errorf("expected d1 > d0, got d0=%v, d1=%v", d0, d1)
	}
	if d5 <= d1 {
		t.Errorf("expected d5 > d1, got d1=%v, d5=%v", d1, d5)
	}
}

func TestWorker_MultipleHandlers(t *testing.T) {
	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-multi-handler",
		Settings:  senna.DefaultWorkerSettings(),
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	var job1Called, job2Called atomic.Bool

	w.Register("job_type_1", func(ctx context.Context, job *senna.Job) error {
		job1Called.Store(true)
		return nil
	})

	w.Register("job_type_2", func(ctx context.Context, job *senna.Job) error {
		job2Called.Store(true)
		return nil
	})

	_, _ = w.handlers.process(context.Background(), senna.NewJob("job_type_1", nil))
	_, _ = w.handlers.process(context.Background(), senna.NewJob("job_type_2", nil))

	if !job1Called.Load() {
		t.Error("job_type_1 handler should have been called")
	}
	if !job2Called.Load() {
		t.Error("job_type_2 handler should have been called")
	}
}

func TestWorker_UnknownJobType(t *testing.T) {
	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-unknown-job",
		Settings:  senna.DefaultWorkerSettings(),
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	w.Register("known_job", func(ctx context.Context, job *senna.Job) error {
		return nil
	})

	job := senna.NewJob("unknown_job", nil)
	_, err = w.handlers.process(context.Background(), job)

	var notFoundErr *senna.JobNotFoundError
	if !errors.As(err, &notFoundErr) {
		t.Fatalf("expected JobNotFoundError, got %T: %v", err, err)
	}
}

func TestWorker_MiddlewareOrder(t *testing.T) {
	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-mw-order",
		Settings:  senna.DefaultWorkerSettings(),
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	var order []string

	w.Use(func(next senna.Handler) senna.Handler {
		return func(ctx context.Context, job *senna.Job) error {
			order = append(order, "mw1-before")
			err := next(ctx, job)
			order = append(order, "mw1-after")
			return err
		}
	})

	w.Use(func(next senna.Handler) senna.Handler {
		return func(ctx context.Context, job *senna.Job) error {
			order = append(order, "mw2-before")
			err := next(ctx, job)
			order = append(order, "mw2-after")
			return err
		}
	})

	w.Register("test_job", func(ctx context.Context, job *senna.Job) error {
		order = append(order, "handler")
		return nil
	})

	_, _ = w.handlers.process(context.Background(), senna.NewJob("test_job", nil))

	expected := []string{"mw1-before", "mw2-before", "handler", "mw2-after", "mw1-after"}
	start := len(order) - len(expected)
	if start < 0 {
		t.Fatalf("expected at least %d entries in order, got %d", len(expected), len(order))
	}
	for i, v := range expected {
		if order[start+i] != v {
			t.Errorf("expected order[%d]='%s', got '%s'", i, v, order[start+i])
		}
	}
}

func TestWorker_BatchFailuresCountOncePerJob(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-batch-failures:*")

	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-batch-failures",
		Settings:  senna.DefaultWorkerSettings(),
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	job := senna.NewJob("batch-job", nil)
	job.BatchID = "batch-1"

	state := senna.BatchState{
		ID:        job.BatchID,
		Total:     1,
		Pending:   1,
		Failures:  0,
		Successes: 0,
		CreatedAt: time.Now(),
	}
	data, _ := json.Marshal(state)
	if err := redisClient.Set(context.Background(), w.keys.Batch(job.BatchID), string(data), 0).Err(); err != nil {
		t.Fatalf("set batch state: %v", err)
	}
	if err := redisClient.SAdd(context.Background(), w.keys.BatchJobs(job.BatchID), job.ID).Err(); err != nil {
		t.Fatalf("seed batch jobs set: %v", err)
	}

	ctx := context.Background()
	if err := w.updateBatchProgress(ctx, job, batch.ResultFailure); err != nil {
		t.Fatalf("update batch progress failure: %v", err)
	}

	progress, err := redisClient.HGetAll(ctx, w.keys.BatchProgress(job.BatchID)).Result()
	if err != nil {
		t.Fatalf("HGetAll(%q) error = %v, want nil", w.keys.BatchProgress(job.BatchID), err)
	}
	if progress["failures"] != "1" || progress["pending"] != "1" {
		t.Fatalf("batch progress after first failure = failures:%s pending:%s, want failures:1 pending:1", progress["failures"], progress["pending"])
	}

	if err := w.updateBatchProgress(ctx, job, batch.ResultFailure); err != nil {
		t.Fatalf("update batch progress failure: %v", err)
	}
	progress, err = redisClient.HGetAll(ctx, w.keys.BatchProgress(job.BatchID)).Result()
	if err != nil {
		t.Fatalf("HGetAll(%q) error = %v, want nil", w.keys.BatchProgress(job.BatchID), err)
	}
	if progress["failures"] != "1" || progress["pending"] != "1" {
		t.Fatalf("batch progress after duplicate failure = failures:%s pending:%s, want failures:1 pending:1", progress["failures"], progress["pending"])
	}

	if err := w.updateBatchProgress(ctx, job, batch.ResultSuccess); err != nil {
		t.Fatalf("update batch progress success: %v", err)
	}
	progress, err = redisClient.HGetAll(ctx, w.keys.BatchProgress(job.BatchID)).Result()
	if err != nil {
		t.Fatalf("HGetAll(%q) error = %v, want nil", w.keys.BatchProgress(job.BatchID), err)
	}
	if progress["failures"] != "1" || progress["pending"] != "0" || progress["successes"] != "1" {
		t.Fatalf("batch progress after success = failures:%s pending:%s successes:%s, want failures:1 pending:0 successes:1", progress["failures"], progress["pending"], progress["successes"])
	}
}

func TestWorker_Periodic_NotEnabled(t *testing.T) {
	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-periodic-disabled",
		Settings:  senna.DefaultWorkerSettings(),
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	err = w.Periodic("* * * * *", "test_job")
	if err == nil {
		t.Error("expected error when periodic is not enabled")
	}

	jobs := w.PeriodicJobs()
	if jobs != nil {
		t.Error("expected nil jobs when periodic is not enabled")
	}
}

func TestWorker_Periodic_Enabled(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-periodic-enabled:*")

	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-periodic-enabled",
		Settings: senna.WorkerSettings{
			Concurrency:     1,
			Queues:          []senna.QueueConfig{{Name: "default", Priority: 1}},
			ShutdownTimeout: time.Second,
			PollInterval:    50 * time.Millisecond,
			HeartbeatRate:   time.Second,
			PeriodicEnabled: true,
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	err = w.Periodic("0 * * * *", "hourly_job")
	if err != nil {
		t.Fatalf("failed to register periodic job: %v", err)
	}

	jobs := w.PeriodicJobs()
	if len(jobs) != 1 {
		t.Fatalf("expected 1 periodic job, got %d", len(jobs))
	}
	if jobs[0].JobType != "hourly_job" {
		t.Errorf("expected job type 'hourly_job', got '%s'", jobs[0].JobType)
	}
}

func TestWorker_Periodic_InvalidCron(t *testing.T) {
	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-periodic-invalid",
		Settings: senna.WorkerSettings{
			Concurrency:     1,
			Queues:          []senna.QueueConfig{{Name: "default", Priority: 1}},
			ShutdownTimeout: time.Second,
			PollInterval:    50 * time.Millisecond,
			HeartbeatRate:   time.Second,
			PeriodicEnabled: true,
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	err = w.Periodic("invalid", "test_job")
	if err == nil {
		t.Error("expected error for invalid cron expression")
	}
}

func TestWorker_Scheduler_EnqueuesDueJobs(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-scheduler-due:*")

	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-scheduler-due",
		Settings: senna.WorkerSettings{
			Concurrency:           1,
			Queues:                []senna.QueueConfig{{Name: "default", Priority: 1}},
			ShutdownTimeout:       time.Second,
			PollInterval:          50 * time.Millisecond,
			ScheduledPollInterval: 100 * time.Millisecond,
			HeartbeatRate:         time.Second,
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	ctx := context.Background()

	// Add a job scheduled in the past (should be enqueued immediately)
	pastTime := time.Now().Add(-time.Minute)
	job := senna.NewJob("past_job", nil)
	jobData, _ := job.Marshal()
	redisClient.ZAdd(ctx, w.keys.Scheduled(), redis.Z{
		Score:  float64(pastTime.Unix()),
		Member: string(jobData),
	})

	// Call enqueueScheduled directly
	if err := w.enqueueScheduled(ctx); err != nil {
		t.Fatalf("enqueueScheduled() error = %v, want nil", err)
	}

	// Job should now be in the queue
	queueLen, _ := redisClient.LLen(ctx, w.keys.Queue("default")).Result()
	if queueLen != 1 {
		t.Errorf("expected 1 job in queue, got %d", queueLen)
	}

	// Scheduled set should be empty
	scheduledLen, _ := redisClient.ZCard(ctx, w.keys.Scheduled()).Result()
	if scheduledLen != 0 {
		t.Errorf("expected 0 jobs in scheduled, got %d", scheduledLen)
	}
}

func TestWorker_Scheduler_KeepsScheduledJobsWhenQueueWriteFails(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-scheduler-queue-fail:*")

	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-scheduler-queue-fail",
		Settings:  senna.DefaultWorkerSettings(),
	})
	if err != nil {
		t.Fatalf("worker.New() error = %v, want nil", err)
	}
	defer func() { _ = w.redis.Close() }()

	ctx := context.Background()
	pastTime := time.Now().Add(-time.Minute)

	defaultJob := senna.NewJob("scheduled_job", nil)
	defaultData, err := defaultJob.Marshal()
	if err != nil {
		t.Fatalf("Job.Marshal() default job error = %v, want nil", err)
	}
	criticalJob := senna.NewJob("scheduled_job", nil)
	criticalJob.Queue = "critical"
	criticalData, err := criticalJob.Marshal()
	if err != nil {
		t.Fatalf("Job.Marshal() critical job error = %v, want nil", err)
	}

	if err := redisClient.ZAdd(ctx, w.keys.Scheduled(),
		redis.Z{Score: float64(pastTime.Unix()), Member: string(defaultData)},
		redis.Z{Score: float64(pastTime.Unix()), Member: string(criticalData)},
	).Err(); err != nil {
		t.Fatalf("ZAdd(%q) scheduled jobs error = %v, want nil", w.keys.Scheduled(), err)
	}
	if err := redisClient.Set(ctx, w.keys.Queue("critical"), "wrong-type", 0).Err(); err != nil {
		t.Fatalf("Set(%q) queue error = %v, want nil", w.keys.Queue("critical"), err)
	}

	if err := w.enqueueScheduled(ctx); err == nil {
		t.Fatal("enqueueScheduled() error = nil, want error")
	}

	scheduledLen, err := redisClient.ZCard(ctx, w.keys.Scheduled()).Result()
	if err != nil {
		t.Fatalf("ZCard(%q) error = %v, want nil", w.keys.Scheduled(), err)
	}
	if scheduledLen != 2 {
		t.Errorf("ZCard(%q) = %d, want %d", w.keys.Scheduled(), scheduledLen, 2)
	}

	defaultQueueLen, err := redisClient.LLen(ctx, w.keys.Queue("default")).Result()
	if err != nil {
		t.Fatalf("LLen(%q) error = %v, want nil", w.keys.Queue("default"), err)
	}
	if defaultQueueLen != 0 {
		t.Errorf("LLen(%q) = %d, want %d", w.keys.Queue("default"), defaultQueueLen, 0)
	}
}

func TestWorker_Scheduler_KeepsFutureJobs(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-scheduler-future:*")

	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-scheduler-future",
		Settings:  senna.DefaultWorkerSettings(),
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	ctx := context.Background()

	// Add a job scheduled in the future
	futureTime := time.Now().Add(time.Hour)
	job := senna.NewJob("future_job", nil)
	jobData, _ := job.Marshal()
	redisClient.ZAdd(ctx, w.keys.Scheduled(), redis.Z{
		Score:  float64(futureTime.Unix()),
		Member: string(jobData),
	})

	// Call enqueueScheduled
	if err := w.enqueueScheduled(ctx); err != nil {
		t.Fatalf("enqueueScheduled() error = %v, want nil", err)
	}

	// Job should still be in scheduled set
	scheduledLen, _ := redisClient.ZCard(ctx, w.keys.Scheduled()).Result()
	if scheduledLen != 1 {
		t.Errorf("expected 1 job in scheduled, got %d", scheduledLen)
	}

	// Queue should be empty
	queueLen, _ := redisClient.LLen(ctx, w.keys.Queue("default")).Result()
	if queueLen != 0 {
		t.Errorf("expected 0 jobs in queue, got %d", queueLen)
	}
}

func TestWorker_Scheduler_ProcessesMixedJobs(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-scheduler-mixed:*")

	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-scheduler-mixed",
		Settings:  senna.DefaultWorkerSettings(),
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	ctx := context.Background()

	// Add jobs: 2 due, 2 future
	now := time.Now()

	jobs := []struct {
		offset time.Duration
		name   string
	}{
		{-2 * time.Minute, "past1"},
		{-1 * time.Minute, "past2"},
		{1 * time.Hour, "future1"},
		{2 * time.Hour, "future2"},
	}

	for _, j := range jobs {
		job := senna.NewJob(j.name, nil)
		jobData, _ := job.Marshal()
		redisClient.ZAdd(ctx, w.keys.Scheduled(), redis.Z{
			Score:  float64(now.Add(j.offset).Unix()),
			Member: string(jobData),
		})
	}

	// Call enqueueScheduled
	if err := w.enqueueScheduled(ctx); err != nil {
		t.Fatalf("enqueueScheduled() error = %v, want nil", err)
	}

	// 2 due jobs should be in queue
	queueLen, _ := redisClient.LLen(ctx, w.keys.Queue("default")).Result()
	if queueLen != 2 {
		t.Errorf("expected 2 jobs in queue, got %d", queueLen)
	}

	// 2 future jobs should remain in scheduled
	scheduledLen, _ := redisClient.ZCard(ctx, w.keys.Scheduled()).Result()
	if scheduledLen != 2 {
		t.Errorf("expected 2 jobs in scheduled, got %d", scheduledLen)
	}
}

func TestWorker_Scheduler_EnqueuesRetries(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-scheduler-retry:*")

	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-scheduler-retry",
		Settings:  senna.DefaultWorkerSettings(),
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	ctx := context.Background()

	// Add a job to the retry set with past timestamp
	pastTime := time.Now().Add(-time.Minute)
	job := senna.NewJob("retry_job", nil)
	job.RetryCount = 1
	jobData, _ := job.Marshal()
	redisClient.ZAdd(ctx, w.keys.Retry(), redis.Z{
		Score:  float64(pastTime.Unix()),
		Member: string(jobData),
	})

	// Call enqueueRetries
	if err := w.enqueueRetries(ctx); err != nil {
		t.Fatalf("enqueueRetries() error = %v, want nil", err)
	}

	// Job should now be in the queue
	queueLen, _ := redisClient.LLen(ctx, w.keys.Queue("default")).Result()
	if queueLen != 1 {
		t.Errorf("expected 1 job in queue, got %d", queueLen)
	}

	// Retry set should be empty
	retryLen, _ := redisClient.ZCard(ctx, w.keys.Retry()).Result()
	if retryLen != 0 {
		t.Errorf("expected 0 jobs in retry, got %d", retryLen)
	}
}

func TestWorker_Scheduler_KeepsRetryJobsWhenQueueWriteFails(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-scheduler-retry-queue-fail:*")

	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-scheduler-retry-queue-fail",
		Settings:  senna.DefaultWorkerSettings(),
	})
	if err != nil {
		t.Fatalf("worker.New() error = %v, want nil", err)
	}
	defer func() { _ = w.redis.Close() }()

	ctx := context.Background()
	pastTime := time.Now().Add(-time.Minute)

	job := senna.NewJob("retry_job", nil)
	job.Queue = "critical"
	job.RetryCount = 1
	jobData, err := job.Marshal()
	if err != nil {
		t.Fatalf("Job.Marshal() retry job error = %v, want nil", err)
	}
	if err := redisClient.ZAdd(ctx, w.keys.Retry(), redis.Z{
		Score:  float64(pastTime.Unix()),
		Member: string(jobData),
	}).Err(); err != nil {
		t.Fatalf("ZAdd(%q) retry job error = %v, want nil", w.keys.Retry(), err)
	}
	if err := redisClient.Set(ctx, w.keys.Queue("critical"), "wrong-type", 0).Err(); err != nil {
		t.Fatalf("Set(%q) queue error = %v, want nil", w.keys.Queue("critical"), err)
	}

	if err := w.enqueueRetries(ctx); err == nil {
		t.Fatal("enqueueRetries() error = nil, want error")
	}

	retryLen, err := redisClient.ZCard(ctx, w.keys.Retry()).Result()
	if err != nil {
		t.Fatalf("ZCard(%q) error = %v, want nil", w.keys.Retry(), err)
	}
	if retryLen != 1 {
		t.Errorf("ZCard(%q) = %d, want %d", w.keys.Retry(), retryLen, 1)
	}
}

func TestWorker_Scheduler_RoutesToCorrectQueue(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-scheduler-queue:*")

	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-scheduler-queue",
		Settings:  senna.DefaultWorkerSettings(),
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	ctx := context.Background()
	pastTime := time.Now().Add(-time.Minute)

	// Add jobs to different queues
	job1 := senna.NewJob("job1", nil)
	job1.Queue = "critical"
	data1, _ := job1.Marshal()
	redisClient.ZAdd(ctx, w.keys.Scheduled(), redis.Z{
		Score:  float64(pastTime.Unix()),
		Member: string(data1),
	})

	job2 := senna.NewJob("job2", nil)
	job2.Queue = "low"
	data2, _ := job2.Marshal()
	redisClient.ZAdd(ctx, w.keys.Scheduled(), redis.Z{
		Score:  float64(pastTime.Unix()),
		Member: string(data2),
	})

	// Call enqueueScheduled
	if err := w.enqueueScheduled(ctx); err != nil {
		t.Fatalf("enqueueScheduled() error = %v, want nil", err)
	}

	// Check jobs are in correct queues
	criticalLen, _ := redisClient.LLen(ctx, w.keys.Queue("critical")).Result()
	lowLen, _ := redisClient.LLen(ctx, w.keys.Queue("low")).Result()

	if criticalLen != 1 {
		t.Errorf("expected 1 job in critical queue, got %d", criticalLen)
	}
	if lowLen != 1 {
		t.Errorf("expected 1 job in low queue, got %d", lowLen)
	}
}

func TestWorker_ScheduledPollInterval_Configurable(t *testing.T) {
	customInterval := 50 * time.Millisecond
	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-scheduler-interval",
		Settings: senna.WorkerSettings{
			Concurrency:           1,
			Queues:                []senna.QueueConfig{{Name: "default", Priority: 1}},
			ShutdownTimeout:       time.Second,
			PollInterval:          50 * time.Millisecond,
			ScheduledPollInterval: customInterval,
			HeartbeatRate:         time.Second,
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	if w.config.Settings.ScheduledPollInterval != customInterval {
		t.Errorf("expected ScheduledPollInterval %v, got %v",
			customInterval, w.config.Settings.ScheduledPollInterval)
	}
}

func TestWorker_ScheduledPollInterval_DefaultValue(t *testing.T) {
	settings := senna.DefaultWorkerSettings()

	if settings.ScheduledPollInterval != 5*time.Second {
		t.Errorf("expected default ScheduledPollInterval 5s, got %v", settings.ScheduledPollInterval)
	}
}

func TestWorker_Scheduler_ConcurrentWorkers_NoDuplicates(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-scheduler-concurrent:*")

	// Create multiple workers
	workers := make([]*Worker, 3)
	for i := range workers {
		w, err := New(&Config{
			Redis:     getTestRedisConfig(),
			Namespace: "test-scheduler-concurrent",
			Settings:  senna.DefaultWorkerSettings(),
		})
		if err != nil {
			t.Fatalf("failed to create worker %d: %v", i, err)
		}
		workers[i] = w
		defer func() { _ = w.redis.Close() }()
	}

	ctx := context.Background()
	pastTime := time.Now().Add(-time.Minute)

	// Add 10 jobs
	for i := range 10 {
		job := senna.NewJob("concurrent_job", map[string]any{"index": i})
		jobData, _ := job.Marshal()
		redisClient.ZAdd(ctx, workers[0].keys.Scheduled(), redis.Z{
			Score:  float64(pastTime.Unix()),
			Member: string(jobData),
		})
	}

	// Run schedulers concurrently from all workers
	errCh := make(chan error, len(workers))
	for _, w := range workers {
		go func(w *Worker) {
			errCh <- w.enqueueScheduled(ctx)
		}(w)
	}

	// Wait for all workers
	for range workers {
		if err := <-errCh; err != nil {
			t.Errorf("enqueueScheduled() error = %v, want nil", err)
		}
	}

	// Verify exactly 10 jobs in queue (no duplicates)
	queueLen, _ := redisClient.LLen(ctx, workers[0].keys.Queue("default")).Result()
	if queueLen != 10 {
		t.Errorf("expected exactly 10 jobs in queue (no duplicates), got %d", queueLen)
	}

	// Verify scheduled set is empty
	scheduledLen, _ := redisClient.ZCard(ctx, workers[0].keys.Scheduled()).Result()
	if scheduledLen != 0 {
		t.Errorf("expected 0 jobs in scheduled, got %d", scheduledLen)
	}
}

func TestWorker_Retries_ConcurrentWorkers_NoDuplicates(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-retry-concurrent:*")

	// Create multiple workers
	workers := make([]*Worker, 3)
	for i := range workers {
		w, err := New(&Config{
			Redis:     getTestRedisConfig(),
			Namespace: "test-retry-concurrent",
			Settings:  senna.DefaultWorkerSettings(),
		})
		if err != nil {
			t.Fatalf("failed to create worker %d: %v", i, err)
		}
		workers[i] = w
		defer func() { _ = w.redis.Close() }()
	}

	ctx := context.Background()
	pastTime := time.Now().Add(-time.Minute)

	// Add 10 retry jobs
	for i := range 10 {
		job := senna.NewJob("retry_job", map[string]any{"index": i})
		job.RetryCount = 1
		jobData, _ := job.Marshal()
		redisClient.ZAdd(ctx, workers[0].keys.Retry(), redis.Z{
			Score:  float64(pastTime.Unix()),
			Member: string(jobData),
		})
	}

	// Run retry processors concurrently from all workers
	errCh := make(chan error, len(workers))
	for _, w := range workers {
		go func(w *Worker) {
			errCh <- w.enqueueRetries(ctx)
		}(w)
	}

	// Wait for all workers
	for range workers {
		if err := <-errCh; err != nil {
			t.Errorf("enqueueRetries() error = %v, want nil", err)
		}
	}

	// Verify exactly 10 jobs in queue (no duplicates)
	queueLen, _ := redisClient.LLen(ctx, workers[0].keys.Queue("default")).Result()
	if queueLen != 10 {
		t.Errorf("expected exactly 10 retry jobs in queue (no duplicates), got %d", queueLen)
	}

	// Verify retry set is empty
	retryLen, _ := redisClient.ZCard(ctx, workers[0].keys.Retry()).Result()
	if retryLen != 0 {
		t.Errorf("expected 0 jobs in retry set, got %d", retryLen)
	}
}

type blockingCommandHook struct {
	command string
	started chan struct{}
}

func (h *blockingCommandHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (h *blockingCommandHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() != h.command {
			return next(ctx, cmd)
		}
		select {
		case h.started <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return ctx.Err()
	}
}

func (h *blockingCommandHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func TestWorker_RequeueOrphanedJobs_ScanTimeout(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-orphan-scan-timeout:*")

	settings := senna.DefaultWorkerSettings()
	settings.ReaperOperationTimeout = 20 * time.Millisecond
	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-orphan-scan-timeout",
		Settings:  settings,
	})
	if err != nil {
		t.Fatalf("worker.New() error = %v, want nil", err)
	}
	defer func() { _ = w.redis.Close() }()

	hook := &blockingCommandHook{
		command: "scan",
		started: make(chan struct{}, 1),
	}
	w.redis.AddHook(hook)

	startedAt := time.Now()
	w.requeueOrphanedJobs(context.Background())
	elapsed := time.Since(startedAt)

	select {
	case <-hook.started:
	default:
		t.Fatal("requeueOrphanedJobs() did not attempt SCAN")
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("requeueOrphanedJobs() elapsed = %v, want bounded by SCAN timeout", elapsed)
	}
}

func TestWorker_RequeueOrphanedJobs_StaleHeartbeat(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-orphan:*")

	ctx := context.Background()
	ns := "test-orphan"

	// Create a worker to get access to keys helper
	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: ns,
		Settings: senna.WorkerSettings{
			Concurrency:   1,
			Queues:        []senna.QueueConfig{{Name: "default"}},
			HeartbeatRate: time.Second,
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	// Simulate a crashed worker by:
	// 1. Adding a stale heartbeat entry (beat_at in the past)
	// 2. Adding jobs to its in-flight list
	crashedWorkerID := "crashed-worker-123"
	staleTime := time.Now().Add(-2 * workerHeartbeatTimeout).Unix()

	workerInfo := map[string]any{
		"hostname":   "crashed-host",
		"pid":        12345,
		"beat_at":    staleTime,
		"started_at": staleTime,
	}
	workerData, _ := json.Marshal(workerInfo)
	redisClient.HSet(ctx, w.keys.Workers(), crashedWorkerID, string(workerData))

	// Add orphaned jobs to the crashed worker's in-flight list
	job1 := senna.NewJob("orphan_job", map[string]any{"id": 1})
	job1.Queue = "default"
	data1, _ := job1.Marshal()

	job2 := senna.NewJob("orphan_job", map[string]any{"id": 2})
	job2.Queue = "default"
	data2, _ := job2.Marshal()

	inFlightKey := w.keys.InFlight(crashedWorkerID)
	redisClient.LPush(ctx, inFlightKey, string(data1), string(data2))

	// Verify setup
	inFlightLen, _ := redisClient.LLen(ctx, inFlightKey).Result()
	if inFlightLen != 2 {
		t.Fatalf("expected 2 jobs in crashed worker's in-flight, got %d", inFlightLen)
	}

	// Run orphan recovery
	w.requeueOrphanedJobs(ctx)

	// Verify orphaned jobs were requeued
	queueLen, _ := redisClient.LLen(ctx, w.keys.Queue("default")).Result()
	if queueLen != 2 {
		t.Errorf("expected 2 jobs requeued to default queue, got %d", queueLen)
	}

	// Verify in-flight list was cleaned up
	inFlightLen, _ = redisClient.LLen(ctx, inFlightKey).Result()
	if inFlightLen != 0 {
		t.Errorf("expected 0 jobs in crashed worker's in-flight after recovery, got %d", inFlightLen)
	}

	// Verify stale worker was removed from workers hash
	exists, _ := redisClient.HExists(ctx, w.keys.Workers(), crashedWorkerID).Result()
	if exists {
		t.Error("expected stale worker to be removed from workers hash")
	}
}

func TestWorker_RequeueOrphanedJobs_KeepsInFlightWhenQueueWriteFails(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-orphan-queue-fail:*")

	ctx := context.Background()
	ns := "test-orphan-queue-fail"

	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: ns,
		Settings: senna.WorkerSettings{
			Concurrency:   1,
			Queues:        []senna.QueueConfig{{Name: "default"}},
			HeartbeatRate: time.Second,
		},
	})
	if err != nil {
		t.Fatalf("worker.New() error = %v, want nil", err)
	}
	defer func() { _ = w.redis.Close() }()

	crashedWorkerID := "crashed-worker-queue-fail"
	staleTime := time.Now().Add(-2 * workerHeartbeatTimeout).Unix()
	workerInfo := map[string]any{
		"hostname":   "crashed-host",
		"pid":        12345,
		"beat_at":    staleTime,
		"started_at": staleTime,
	}
	workerData, err := json.Marshal(workerInfo)
	if err != nil {
		t.Fatalf("json.Marshal(workerInfo) error = %v, want nil", err)
	}
	if err := redisClient.HSet(ctx, w.keys.Workers(), crashedWorkerID, string(workerData)).Err(); err != nil {
		t.Fatalf("HSet(%q) worker heartbeat error = %v, want nil", w.keys.Workers(), err)
	}

	defaultJob := senna.NewJob("orphan_job", map[string]any{"id": 1})
	defaultJob.Queue = "default"
	defaultData, err := defaultJob.Marshal()
	if err != nil {
		t.Fatalf("Job.Marshal() default job error = %v, want nil", err)
	}
	criticalJob := senna.NewJob("orphan_job", map[string]any{"id": 2})
	criticalJob.Queue = "critical"
	criticalData, err := criticalJob.Marshal()
	if err != nil {
		t.Fatalf("Job.Marshal() critical job error = %v, want nil", err)
	}

	inFlightKey := w.keys.InFlight(crashedWorkerID)
	if err := redisClient.LPush(ctx, inFlightKey, string(defaultData), string(criticalData)).Err(); err != nil {
		t.Fatalf("LPush(%q) in-flight jobs error = %v, want nil", inFlightKey, err)
	}
	if err := redisClient.Set(ctx, w.keys.Queue("critical"), "wrong-type", 0).Err(); err != nil {
		t.Fatalf("Set(%q) queue error = %v, want nil", w.keys.Queue("critical"), err)
	}

	w.requeueOrphanedJobs(ctx)

	inFlightLen, err := redisClient.LLen(ctx, inFlightKey).Result()
	if err != nil {
		t.Fatalf("LLen(%q) error = %v, want nil", inFlightKey, err)
	}
	if inFlightLen != 2 {
		t.Errorf("LLen(%q) = %d, want %d", inFlightKey, inFlightLen, 2)
	}

	defaultQueueLen, err := redisClient.LLen(ctx, w.keys.Queue("default")).Result()
	if err != nil {
		t.Fatalf("LLen(%q) error = %v, want nil", w.keys.Queue("default"), err)
	}
	if defaultQueueLen != 0 {
		t.Errorf("LLen(%q) = %d, want %d", w.keys.Queue("default"), defaultQueueLen, 0)
	}
}

func TestWorker_RequeueOrphanedJobs_DiscardsInvalidPayloads(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-orphan-invalid:*")

	ctx := context.Background()
	ns := "test-orphan-invalid"

	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: ns,
		Settings: senna.WorkerSettings{
			Concurrency:   1,
			Queues:        []senna.QueueConfig{{Name: "default"}},
			HeartbeatRate: time.Second,
		},
	})
	if err != nil {
		t.Fatalf("worker.New() error = %v, want nil", err)
	}
	defer func() { _ = w.redis.Close() }()

	crashedWorkerID := "crashed-worker-invalid"
	staleTime := time.Now().Add(-2 * workerHeartbeatTimeout).Unix()
	workerInfo := map[string]any{
		"hostname":   "crashed-host",
		"pid":        12345,
		"beat_at":    staleTime,
		"started_at": staleTime,
	}
	workerData, err := json.Marshal(workerInfo)
	if err != nil {
		t.Fatalf("json.Marshal(workerInfo) error = %v, want nil", err)
	}
	if err := redisClient.HSet(ctx, w.keys.Workers(), crashedWorkerID, string(workerData)).Err(); err != nil {
		t.Fatalf("HSet(%q) worker heartbeat error = %v, want nil", w.keys.Workers(), err)
	}

	inFlightKey := w.keys.InFlight(crashedWorkerID)
	if err := redisClient.LPush(ctx, inFlightKey, "not-json", "[]", "{}").Err(); err != nil {
		t.Fatalf("LPush(%q) invalid payloads error = %v, want nil", inFlightKey, err)
	}

	w.requeueOrphanedJobs(ctx)

	inFlightLen, err := redisClient.LLen(ctx, inFlightKey).Result()
	if err != nil {
		t.Fatalf("LLen(%q) error = %v, want nil", inFlightKey, err)
	}
	if inFlightLen != 0 {
		t.Errorf("LLen(%q) = %d, want %d", inFlightKey, inFlightLen, 0)
	}

	defaultQueueLen, err := redisClient.LLen(ctx, w.keys.Queue("default")).Result()
	if err != nil {
		t.Fatalf("LLen(%q) error = %v, want nil", w.keys.Queue("default"), err)
	}
	if defaultQueueLen != 0 {
		t.Errorf("LLen(%q) = %d, want %d", w.keys.Queue("default"), defaultQueueLen, 0)
	}
}

func TestWorker_RequeueOrphanedJobs_ActiveWorkerNotAffected(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-orphan-active:*")

	ctx := context.Background()
	ns := "test-orphan-active"

	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: ns,
		Settings: senna.WorkerSettings{
			Concurrency:   1,
			Queues:        []senna.QueueConfig{{Name: "default"}},
			HeartbeatRate: time.Second,
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	// Simulate an active worker with recent heartbeat
	activeWorkerID := "active-worker-456"
	recentTime := time.Now().Unix()

	workerInfo := map[string]any{
		"hostname":   "active-host",
		"pid":        67890,
		"beat_at":    recentTime,
		"started_at": recentTime,
	}
	workerData, _ := json.Marshal(workerInfo)
	redisClient.HSet(ctx, w.keys.Workers(), activeWorkerID, string(workerData))

	// Add jobs to active worker's in-flight list (simulating jobs being processed)
	job := senna.NewJob("active_job", map[string]any{"id": 1})
	job.Queue = "default"
	data, _ := job.Marshal()

	inFlightKey := w.keys.InFlight(activeWorkerID)
	redisClient.LPush(ctx, inFlightKey, string(data))

	// Run orphan recovery
	w.requeueOrphanedJobs(ctx)

	// Verify active worker's in-flight jobs were NOT requeued
	queueLen, _ := redisClient.LLen(ctx, w.keys.Queue("default")).Result()
	if queueLen != 0 {
		t.Errorf("expected 0 jobs requeued (active worker should not be affected), got %d", queueLen)
	}

	// Verify in-flight list is still intact
	inFlightLen, _ := redisClient.LLen(ctx, inFlightKey).Result()
	if inFlightLen != 1 {
		t.Errorf("expected 1 job still in active worker's in-flight, got %d", inFlightLen)
	}

	// Verify active worker still in workers hash
	exists, _ := redisClient.HExists(ctx, w.keys.Workers(), activeWorkerID).Result()
	if !exists {
		t.Error("expected active worker to remain in workers hash")
	}
}

func seedHealthyBatchJobInFlight(t *testing.T, w *Worker, redisClient *redis.Client, bid, workerID string) *senna.Job {
	t.Helper()
	ctx := context.Background()

	job := senna.NewJob("batch_job", map[string]any{"n": 1})
	job.Queue = "default"
	job.BatchID = bid

	state := senna.BatchState{ID: bid, Total: 1, Pending: 1}
	stateData, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("marshal batch state: %v", err)
	}
	if err := redisClient.Set(ctx, w.keys.Batch(bid), string(stateData), 0).Err(); err != nil {
		t.Fatalf("set batch state: %v", err)
	}
	if err := redisClient.SAdd(ctx, w.keys.BatchJobs(bid), job.ID).Err(); err != nil {
		t.Fatalf("add batch job: %v", err)
	}

	jobData, err := job.Marshal()
	if err != nil {
		t.Fatalf("marshal job: %v", err)
	}
	// Mirror a fetched job: Raw holds the exact in-flight payload, so the ack /
	// move-to-dead LREM matches it regardless of later field mutations.
	job.SetRaw(string(jobData))
	if err := redisClient.LPush(ctx, w.keys.InFlight(workerID), string(jobData)).Err(); err != nil {
		t.Fatalf("seed in-flight: %v", err)
	}

	return job
}

// seedBatchJobInFlight registers a single-job batch in Redis and places the job
// on the worker's in-flight list, returning the job. The batch jobs set is left
// corrupted (wrong Redis type) so the batch completion script fails, simulating
// an inability to record batch progress at the moment of completion.
func seedBatchJobInFlight(t *testing.T, w *Worker, redisClient *redis.Client, bid string) *senna.Job {
	t.Helper()
	ctx := context.Background()

	job := seedHealthyBatchJobInFlight(t, w, redisClient, bid, w.id)

	// Corrupt the batch jobs set: a string where the script expects a set. The
	// completion script returns an error before mutating any state.
	if err := redisClient.Set(ctx, w.keys.BatchJobs(bid), "corrupt", 0).Err(); err != nil {
		t.Fatalf("corrupt batch jobs set: %v", err)
	}

	return job
}

func repairBatchJobSet(t *testing.T, w *Worker, redisClient *redis.Client, bid, jobID string) {
	t.Helper()
	ctx := context.Background()

	if err := redisClient.Del(ctx, w.keys.BatchJobs(bid)).Err(); err != nil {
		t.Fatalf("clear corrupt batch jobs set: %v", err)
	}
	if err := redisClient.SAdd(ctx, w.keys.BatchJobs(bid), jobID).Err(); err != nil {
		t.Fatalf("repair batch jobs set: %v", err)
	}
}

func assertFinalizationStillPending(t *testing.T, done <-chan struct{}) {
	t.Helper()

	select {
	case <-done:
		t.Fatal("job finalization completed before batch progress could be recorded")
	case <-time.After(50 * time.Millisecond):
	}
}

func waitForFinalization(t *testing.T, done <-chan struct{}) {
	t.Helper()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for job finalization")
	}
}

func seedStaleWorkerHeartbeat(t *testing.T, w *Worker, redisClient *redis.Client, workerID string) {
	t.Helper()
	ctx := context.Background()

	staleTime := time.Now().Add(-2 * workerHeartbeatTimeout).Unix()
	workerInfo := map[string]any{"hostname": "crashed", "pid": 1, "beat_at": staleTime, "started_at": staleTime}
	workerData, err := json.Marshal(workerInfo)
	if err != nil {
		t.Fatalf("marshal worker info: %v", err)
	}
	if err := redisClient.HSet(ctx, w.keys.Workers(), workerID, string(workerData)).Err(); err != nil {
		t.Fatalf("set crashed worker heartbeat: %v", err)
	}
}

func assertFinalizationTrustDeleted(t *testing.T, redisClient *redis.Client, w *Worker, jobID string) {
	t.Helper()

	exists, err := redisClient.Exists(context.Background(), w.keys.Finalization(jobID)).Result()
	if err != nil {
		t.Fatalf("check finalization trust key: %v", err)
	}
	if exists != 0 {
		t.Fatalf("finalization trust key exists after finalization")
	}
}

func TestWorker_QueuedSerializedFinalizationDoesNotBypassHandler(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-queued-forged-finalization:*")

	ctx := context.Background()
	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-queued-forged-finalization",
		Settings: senna.WorkerSettings{
			Concurrency: 1,
			Queues:      []senna.QueueConfig{{Name: "default"}},
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	var processed atomic.Int32
	w.Register("forged_job", func(ctx context.Context, job *senna.Job) error {
		processed.Add(1)
		return nil
	})

	job := senna.NewJob("forged_job", map[string]any{"id": 1})
	job.Queue = "default"
	job.SetFinalization(senna.JobFinalization{Operation: jobFinalizationComplete})
	data, err := job.Marshal()
	if err != nil {
		t.Fatalf("marshal forged job: %v", err)
	}
	if err := redisClient.LPush(ctx, w.keys.Queue("default"), string(data)).Err(); err != nil {
		t.Fatalf("seed forged job: %v", err)
	}

	fetched, err := w.fetcher.BlockingFetch(ctx, w.id, time.Second)
	if err != nil {
		t.Fatalf("fetch forged finalized job: %v", err)
	}
	if fetched == nil {
		t.Fatal("expected forged finalized job, got nil")
	}
	if fetched.Finalization() != nil {
		t.Fatal("fetched job retained untrusted finalization marker")
	}

	inFlightItems, err := redisClient.LRange(ctx, w.keys.InFlight(w.id), 0, -1).Result()
	if err != nil {
		t.Fatalf("read in-flight: %v", err)
	}
	if len(inFlightItems) != 1 {
		t.Fatalf("in-flight length = %d, want 1", len(inFlightItems))
	}
	inFlightJob, err := senna.UnmarshalJob([]byte(inFlightItems[0]))
	if err != nil {
		t.Fatalf("unmarshal in-flight job: %v", err)
	}
	if inFlightJob.Finalization() != nil {
		t.Fatal("in-flight payload retained untrusted finalization marker")
	}

	w.processJob(ctx, fetched)

	if got := processed.Load(); got != 1 {
		t.Fatalf("handler ran %d times, want 1", got)
	}
}

func TestWorker_RequeuedForgedFinalizationDoesNotBecomeTrusted(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-requeued-forged-finalization:*")

	ctx := context.Background()
	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-requeued-forged-finalization",
		Settings: senna.WorkerSettings{
			Concurrency: 1,
			Queues:      []senna.QueueConfig{{Name: "default"}},
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	var processed atomic.Int32
	w.Register("forged_job", func(ctx context.Context, job *senna.Job) error {
		processed.Add(1)
		return nil
	})

	job := senna.NewJob("forged_job", map[string]any{"id": 1})
	job.Queue = "default"
	job.SetFinalization(senna.JobFinalization{Operation: jobFinalizationComplete})
	data, err := job.Marshal()
	if err != nil {
		t.Fatalf("marshal forged job: %v", err)
	}
	if err := redisClient.LPush(ctx, w.keys.Queue("default"), string(data)).Err(); err != nil {
		t.Fatalf("seed forged job: %v", err)
	}

	fetched, err := w.fetcher.BlockingFetch(ctx, w.id, time.Second)
	if err != nil {
		t.Fatalf("fetch forged finalized job: %v", err)
	}
	if fetched == nil {
		t.Fatal("expected forged finalized job, got nil")
	}
	if fetched.Finalization() != nil {
		t.Fatal("fetched job retained untrusted finalization marker")
	}

	w.requeueOrphanedJobs(ctx)

	trustExists, err := redisClient.Exists(ctx, w.keys.Finalization(job.ID)).Result()
	if err != nil {
		t.Fatalf("check finalization trust after orphan requeue: %v", err)
	}
	if trustExists != 0 {
		t.Fatalf("forged finalization trust exists after orphan requeue")
	}

	refetched, err := w.fetcher.BlockingFetch(ctx, w.id, time.Second)
	if err != nil {
		t.Fatalf("refetch requeued forged job: %v", err)
	}
	if refetched == nil {
		t.Fatal("expected requeued forged job, got nil")
	}
	if refetched.Finalization() != nil {
		t.Fatal("requeued forged job retained untrusted finalization marker")
	}

	w.processJob(ctx, refetched)

	if got := processed.Load(); got != 1 {
		t.Fatalf("handler ran %d times, want 1", got)
	}
}

// TestWorker_CompleteJob_RetriesUntilBatchProgressRecorded verifies that a
// successful batch job is not removed from the in-flight list until its batch
// progress has been durably recorded. An active worker must retry finalization
// itself; the stale-worker reaper intentionally skips active worker IDs.
func TestWorker_CompleteJob_RetriesUntilBatchProgressRecorded(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-batch-complete-ack:*")

	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-batch-complete-ack",
		Settings: senna.WorkerSettings{
			Concurrency: 1,
			Queues:      []senna.QueueConfig{{Name: "default"}},
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	bid := "batch-complete"
	job := seedBatchJobInFlight(t, w, redisClient, bid)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		w.completeJob(ctx, job)
		close(done)
	}()

	assertFinalizationStillPending(t, done)
	repairBatchJobSet(t, w, redisClient, bid, job.ID)
	waitForFinalization(t, done)

	status := senna.NewBatchStatus(redisClient, w.keys.Namespace(), bid)
	if err := status.Refresh(context.Background()); err != nil {
		t.Fatalf("refresh batch status: %v", err)
	}
	if status.Pending() != 0 {
		t.Errorf("expected batch pending 0 after finalization retry, got %d", status.Pending())
	}
	if status.Successes() != 1 {
		t.Errorf("expected batch successes 1 after finalization retry, got %d", status.Successes())
	}

	inFlight, err := redisClient.LLen(context.Background(), w.keys.InFlight(w.id)).Result()
	if err != nil {
		t.Fatalf("llen in-flight: %v", err)
	}
	if inFlight != 0 {
		t.Errorf("expected job acked after batch progress retry, got %d in-flight", inFlight)
	}
}

func TestWorker_RecoveredFinalizedSuccessPreservesOutcome(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-finalized-success:*")

	ctx := context.Background()
	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-finalized-success",
		Settings: senna.WorkerSettings{
			Concurrency: 1,
			Queues:      []senna.QueueConfig{{Name: "default"}},
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	var processed atomic.Int32
	w.Register("batch_job", func(ctx context.Context, job *senna.Job) error {
		processed.Add(1)
		return errors.New("would fail on replay")
	})

	bid := "batch-finalized-success"
	crashedWorkerID := "crashed-finalized-success"
	job := seedHealthyBatchJobInFlight(t, w, redisClient, bid, crashedWorkerID)
	seedStaleWorkerHeartbeat(t, w, redisClient, crashedWorkerID)

	if err := w.fetcher.MarkFinalization(ctx, crashedWorkerID, job, senna.JobFinalization{Operation: jobFinalizationComplete}); err != nil {
		t.Fatalf("mark finalization: %v", err)
	}
	if err := w.updateBatchProgress(ctx, job, batch.ResultSuccess); err != nil {
		t.Fatalf("record batch success: %v", err)
	}

	w.requeueOrphanedJobs(ctx)
	fetched, err := w.fetcher.BlockingFetch(ctx, w.id, time.Second)
	if err != nil {
		t.Fatalf("fetch recovered finalized job: %v", err)
	}
	if fetched == nil {
		t.Fatal("expected recovered finalized job, got nil")
	}
	w.processJob(ctx, fetched)

	if got := processed.Load(); got != 0 {
		t.Errorf("handler ran %d times for recovered finalized success, want 0", got)
	}

	status := senna.NewBatchStatus(redisClient, w.keys.Namespace(), bid)
	if err := status.Refresh(ctx); err != nil {
		t.Fatalf("refresh batch status: %v", err)
	}
	if status.Pending() != 0 {
		t.Errorf("batch Pending() = %d, want 0", status.Pending())
	}
	if status.Successes() != 1 {
		t.Errorf("batch Successes() = %d, want 1", status.Successes())
	}

	dead, err := redisClient.ZCard(ctx, w.keys.Dead()).Result()
	if err != nil {
		t.Fatalf("zcard dead: %v", err)
	}
	if dead != 0 {
		t.Errorf("dead set size = %d, want 0", dead)
	}
	assertFinalizationTrustDeleted(t, redisClient, w, job.ID)
}

func TestWorker_RecoveredLegacyFinalizedSuccessPreservesOutcome(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-finalized-legacy-success:*")

	ctx := context.Background()
	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-finalized-legacy-success",
		Settings: senna.WorkerSettings{
			Concurrency: 1,
			Queues:      []senna.QueueConfig{{Name: "default"}},
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	var processed atomic.Int32
	w.Register("batch_job", func(ctx context.Context, job *senna.Job) error {
		processed.Add(1)
		return errors.New("would fail on replay")
	})

	bid := "batch-finalized-legacy-success"
	crashedWorkerID := "crashed-finalized-legacy-success"
	job := seedHealthyBatchJobInFlight(t, w, redisClient, bid, crashedWorkerID)
	seedStaleWorkerHeartbeat(t, w, redisClient, crashedWorkerID)

	finalizedData, err := payloadWithFinalization(job.Raw(), senna.JobFinalization{Operation: jobFinalizationComplete})
	if err != nil {
		t.Fatalf("payloadWithFinalization() error = %v, want nil", err)
	}
	if err := redisClient.LSet(ctx, w.keys.InFlight(crashedWorkerID), 0, string(finalizedData)).Err(); err != nil {
		t.Fatalf("store legacy finalized payload: %v", err)
	}
	if err := w.updateBatchProgress(ctx, job, batch.ResultSuccess); err != nil {
		t.Fatalf("record batch success: %v", err)
	}

	trustExists, err := redisClient.Exists(ctx, w.keys.Finalization(job.ID)).Result()
	if err != nil {
		t.Fatalf("check finalization trust before recovery: %v", err)
	}
	if trustExists != 0 {
		t.Fatalf("finalization trust exists before orphan recovery")
	}

	w.requeueOrphanedJobs(ctx)

	trustExists, err = redisClient.Exists(ctx, w.keys.Finalization(job.ID)).Result()
	if err != nil {
		t.Fatalf("check finalization trust after recovery: %v", err)
	}
	if trustExists != 1 {
		t.Fatalf("finalization trust after recovery = %d, want 1", trustExists)
	}

	fetched, err := w.fetcher.BlockingFetch(ctx, w.id, time.Second)
	if err != nil {
		t.Fatalf("fetch recovered finalized job: %v", err)
	}
	if fetched == nil {
		t.Fatal("expected recovered finalized job, got nil")
	}
	if fetched.Finalization() == nil {
		t.Fatal("recovered legacy finalized job finalization = nil, want marker")
	}
	w.processJob(ctx, fetched)

	if got := processed.Load(); got != 0 {
		t.Errorf("handler ran %d times for recovered legacy finalized success, want 0", got)
	}

	status := senna.NewBatchStatus(redisClient, w.keys.Namespace(), bid)
	if err := status.Refresh(ctx); err != nil {
		t.Fatalf("refresh batch status: %v", err)
	}
	if status.Pending() != 0 {
		t.Errorf("batch Pending() = %d, want 0", status.Pending())
	}
	if status.Successes() != 1 {
		t.Errorf("batch Successes() = %d, want 1", status.Successes())
	}
	assertFinalizationTrustDeleted(t, redisClient, w, job.ID)
}

func TestWorker_FinalizationMarksRequeuedPayloadBeforeBatchProgress(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-finalized-requeued:*")

	ctx := context.Background()
	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-finalized-requeued",
		Settings: senna.WorkerSettings{
			Concurrency: 1,
			Queues:      []senna.QueueConfig{{Name: "default"}},
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	var processed atomic.Int32
	w.Register("batch_job", func(ctx context.Context, job *senna.Job) error {
		processed.Add(1)
		return errors.New("would fail on replay")
	})

	bid := "batch-finalized-requeued"
	job := seedHealthyBatchJobInFlight(t, w, redisClient, bid, w.id)
	seedStaleWorkerHeartbeat(t, w, redisClient, w.id)

	w.requeueOrphanedJobs(ctx)
	w.completeJob(ctx, job)

	fetched, err := w.fetcher.BlockingFetch(ctx, w.id, time.Second)
	if err != nil {
		t.Fatalf("fetch requeued finalized job: %v", err)
	}
	if fetched == nil {
		t.Fatal("expected requeued finalized job, got nil")
	}
	finalization := fetched.Finalization()
	if finalization == nil {
		t.Fatal("requeued job finalization = nil, want complete marker")
	}
	if finalization.Operation != jobFinalizationComplete {
		t.Fatalf("requeued job finalization operation = %q, want %q", finalization.Operation, jobFinalizationComplete)
	}

	w.processJob(ctx, fetched)

	if got := processed.Load(); got != 0 {
		t.Errorf("handler ran %d times for requeued finalized success, want 0", got)
	}

	status := senna.NewBatchStatus(redisClient, w.keys.Namespace(), bid)
	if err := status.Refresh(ctx); err != nil {
		t.Fatalf("refresh batch status: %v", err)
	}
	if status.Pending() != 0 {
		t.Errorf("batch Pending() = %d, want 0", status.Pending())
	}
	if status.Successes() != 1 {
		t.Errorf("batch Successes() = %d, want 1", status.Successes())
	}

	dead, err := redisClient.ZCard(ctx, w.keys.Dead()).Result()
	if err != nil {
		t.Fatalf("zcard dead: %v", err)
	}
	if dead != 0 {
		t.Errorf("dead set size = %d, want 0", dead)
	}
	assertFinalizationTrustDeleted(t, redisClient, w, job.ID)
}

// TestWorker_RetryJob_RetriesUntilBatchFailureRecorded verifies that retry
// scheduling also waits for batch failure bookkeeping before removing the job
// from in-flight.
func TestWorker_RetryJob_RetriesUntilBatchFailureRecorded(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-batch-retry-ack:*")

	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-batch-retry-ack",
		Settings: senna.WorkerSettings{
			Concurrency: 1,
			Queues:      []senna.QueueConfig{{Name: "default"}},
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	bid := "batch-retry"
	job := seedBatchJobInFlight(t, w, redisClient, bid)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	retryIn := 5 * time.Second
	beforeRetry, err := redisNow(ctx, redisClient)
	if err != nil {
		t.Fatalf("redisNow() before retryJob error = %v, want nil", err)
	}
	go func() {
		w.retryJob(ctx, job, retryIn)
		close(done)
	}()

	assertFinalizationStillPending(t, done)
	repairBatchJobSet(t, w, redisClient, bid, job.ID)
	waitForFinalization(t, done)
	afterRetry, err := redisNow(ctx, redisClient)
	if err != nil {
		t.Fatalf("redisNow() after retryJob error = %v, want nil", err)
	}

	status := senna.NewBatchStatus(redisClient, w.keys.Namespace(), bid)
	if err := status.Refresh(context.Background()); err != nil {
		t.Fatalf("refresh batch status: %v", err)
	}
	if status.Pending() != 1 {
		t.Errorf("expected batch pending 1 after retry finalization, got %d", status.Pending())
	}
	if status.Failures() != 1 {
		t.Errorf("expected batch failures 1 after retry finalization, got %d", status.Failures())
	}

	inFlight, err := redisClient.LLen(context.Background(), w.keys.InFlight(w.id)).Result()
	if err != nil {
		t.Fatalf("llen in-flight: %v", err)
	}
	if inFlight != 0 {
		t.Errorf("expected retry job removed from in-flight after finalization, got %d", inFlight)
	}

	retryItems, err := redisClient.ZRangeWithScores(context.Background(), w.keys.Retry(), 0, -1).Result()
	if err != nil {
		t.Fatalf("zrange retry: %v", err)
	}
	if len(retryItems) != 1 {
		t.Fatalf("expected 1 scheduled retry, got %d", len(retryItems))
	}
	minScore := beforeRetry.Add(retryIn).Unix()
	maxScore := afterRetry.Add(retryIn).Unix()
	if got := int64(retryItems[0].Score); got < minScore || got > maxScore {
		t.Errorf("retry score = %d, want between Redis-time scores %d and %d", got, minScore, maxScore)
	}
	retryData, ok := retryItems[0].Member.(string)
	if !ok {
		t.Fatalf("retry member type = %T, want string", retryItems[0].Member)
	}
	retryJob, err := senna.UnmarshalJob([]byte(retryData))
	if err != nil {
		t.Fatalf("unmarshal retry job: %v", err)
	}
	if retryJob.RetryCount != 1 {
		t.Errorf("retry job RetryCount = %d, want 1", retryJob.RetryCount)
	}
}

func TestWorker_RecoveredFinalizedRetryPreservesOutcome(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-finalized-retry:*")

	ctx := context.Background()
	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-finalized-retry",
		Settings: senna.WorkerSettings{
			Concurrency: 1,
			Queues:      []senna.QueueConfig{{Name: "default"}},
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	var processed atomic.Int32
	w.Register("batch_job", func(ctx context.Context, job *senna.Job) error {
		processed.Add(1)
		return nil
	})

	bid := "batch-finalized-retry"
	crashedWorkerID := "crashed-finalized-retry"
	job := seedHealthyBatchJobInFlight(t, w, redisClient, bid, crashedWorkerID)
	seedStaleWorkerHeartbeat(t, w, redisClient, crashedWorkerID)

	retryAt := time.Now().Add(5 * time.Minute).UTC()
	finalization := senna.JobFinalization{
		Operation: jobFinalizationRetry,
		RetryAt:   retryAt,
	}
	if err := w.fetcher.MarkFinalization(ctx, crashedWorkerID, job, finalization); err != nil {
		t.Fatalf("mark finalization: %v", err)
	}
	if err := w.updateBatchProgress(ctx, job, batch.ResultFailure); err != nil {
		t.Fatalf("record batch failure: %v", err)
	}

	w.requeueOrphanedJobs(ctx)
	fetched, err := w.fetcher.BlockingFetch(ctx, w.id, time.Second)
	if err != nil {
		t.Fatalf("fetch recovered finalized job: %v", err)
	}
	if fetched == nil {
		t.Fatal("expected recovered finalized job, got nil")
	}
	w.processJob(ctx, fetched)

	if got := processed.Load(); got != 0 {
		t.Errorf("handler ran %d times for recovered finalized retry, want 0", got)
	}

	status := senna.NewBatchStatus(redisClient, w.keys.Namespace(), bid)
	if err := status.Refresh(ctx); err != nil {
		t.Fatalf("refresh batch status: %v", err)
	}
	if status.Pending() != 1 {
		t.Errorf("batch Pending() = %d, want 1", status.Pending())
	}
	if status.Failures() != 1 {
		t.Errorf("batch Failures() = %d, want 1", status.Failures())
	}

	retryItems, err := redisClient.ZRangeWithScores(ctx, w.keys.Retry(), 0, -1).Result()
	if err != nil {
		t.Fatalf("zrange retry: %v", err)
	}
	if len(retryItems) != 1 {
		t.Fatalf("retry set size = %d, want 1", len(retryItems))
	}
	if gotScore, wantScore := int64(retryItems[0].Score), retryAt.Unix(); gotScore != wantScore {
		t.Errorf("retry score = %d, want %d", gotScore, wantScore)
	}
	retryData, ok := retryItems[0].Member.(string)
	if !ok {
		t.Fatalf("retry member type = %T, want string", retryItems[0].Member)
	}
	retryJob, err := senna.UnmarshalJob([]byte(retryData))
	if err != nil {
		t.Fatalf("unmarshal retry job: %v", err)
	}
	if retryJob.RetryCount != 1 {
		t.Errorf("retry job RetryCount = %d, want 1", retryJob.RetryCount)
	}
	if retryJob.Finalization() != nil {
		t.Error("retry job retained finalization marker, want nil")
	}
	assertFinalizationTrustDeleted(t, redisClient, w, job.ID)
}

func TestWorker_RecoveredFinalizedDeathPreservesOutcome(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-finalized-death:*")

	ctx := context.Background()
	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-finalized-death",
		Settings: senna.WorkerSettings{
			Concurrency: 1,
			Queues:      []senna.QueueConfig{{Name: "default"}},
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	var processed atomic.Int32
	w.Register("batch_job", func(ctx context.Context, job *senna.Job) error {
		processed.Add(1)
		return nil
	})

	bid := "batch-finalized-death"
	crashedWorkerID := "crashed-finalized-death"
	job := seedHealthyBatchJobInFlight(t, w, redisClient, bid, crashedWorkerID)
	seedStaleWorkerHeartbeat(t, w, redisClient, crashedWorkerID)

	finalization := senna.JobFinalization{
		Operation: jobFinalizationKill,
		Error:     "boom",
	}
	if err := w.fetcher.MarkFinalization(ctx, crashedWorkerID, job, finalization); err != nil {
		t.Fatalf("mark finalization: %v", err)
	}
	if err := w.updateBatchProgress(ctx, job, batch.ResultDeath); err != nil {
		t.Fatalf("record batch death: %v", err)
	}

	w.requeueOrphanedJobs(ctx)
	fetched, err := w.fetcher.BlockingFetch(ctx, w.id, time.Second)
	if err != nil {
		t.Fatalf("fetch recovered finalized job: %v", err)
	}
	if fetched == nil {
		t.Fatal("expected recovered finalized job, got nil")
	}
	w.processJob(ctx, fetched)

	if got := processed.Load(); got != 0 {
		t.Errorf("handler ran %d times for recovered finalized death, want 0", got)
	}

	status := senna.NewBatchStatus(redisClient, w.keys.Namespace(), bid)
	if err := status.Refresh(ctx); err != nil {
		t.Fatalf("refresh batch status: %v", err)
	}
	if status.Pending() != 0 {
		t.Errorf("batch Pending() = %d, want 0", status.Pending())
	}
	if status.Failures() != 1 {
		t.Errorf("batch Failures() = %d, want 1", status.Failures())
	}
	if !status.Dead() {
		t.Error("batch Dead() = false, want true")
	}

	items, err := redisClient.ZRange(ctx, w.keys.Dead(), 0, -1).Result()
	if err != nil {
		t.Fatalf("zrange dead: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("dead set size = %d, want 1", len(items))
	}
	deadJob, err := senna.UnmarshalJob([]byte(items[0]))
	if err != nil {
		t.Fatalf("unmarshal dead job: %v", err)
	}
	if deadJob.Error != "boom" {
		t.Errorf("dead job Error = %q, want %q", deadJob.Error, "boom")
	}
	if deadJob.Finalization() != nil {
		t.Error("dead job retained finalization marker, want nil")
	}
	assertFinalizationTrustDeleted(t, redisClient, w, job.ID)
}

// TestWorker_KillJob_RetriesUntilBatchProgressRecorded verifies that a dying
// job is not moved to the dead set until its batch death has been durably
// recorded, and that an active worker retries the finalization internally.
func TestWorker_KillJob_RetriesUntilBatchProgressRecorded(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-batch-kill-ack:*")

	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-batch-kill-ack",
		Settings: senna.WorkerSettings{
			Concurrency: 1,
			Queues:      []senna.QueueConfig{{Name: "default"}},
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	bid := "batch-kill"
	job := seedBatchJobInFlight(t, w, redisClient, bid)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		w.killJob(ctx, job, errors.New("boom"))
		close(done)
	}()

	assertFinalizationStillPending(t, done)
	repairBatchJobSet(t, w, redisClient, bid, job.ID)
	waitForFinalization(t, done)

	status := senna.NewBatchStatus(redisClient, w.keys.Namespace(), bid)
	if err := status.Refresh(context.Background()); err != nil {
		t.Fatalf("refresh batch status: %v", err)
	}
	if status.Pending() != 0 {
		t.Errorf("expected batch pending 0 after death finalization retry, got %d", status.Pending())
	}
	if status.Failures() != 1 {
		t.Errorf("expected batch failures 1 after death finalization retry, got %d", status.Failures())
	}
	if !status.Dead() {
		t.Error("expected batch marked dead after death finalization retry")
	}

	inFlight, err := redisClient.LLen(context.Background(), w.keys.InFlight(w.id)).Result()
	if err != nil {
		t.Fatalf("llen in-flight: %v", err)
	}
	if inFlight != 0 {
		t.Errorf("expected job removed from in-flight after death finalization retry, got %d", inFlight)
	}

	dead, err := redisClient.ZCard(context.Background(), w.keys.Dead()).Result()
	if err != nil {
		t.Fatalf("zcard dead: %v", err)
	}
	if dead != 1 {
		t.Errorf("expected job moved to dead set after batch progress retry, got %d dead", dead)
	}
}

// TestWorker_OrphanedBatchJobRecoversAndCompletesBatch exercises the full
// recovery loop for a batch member whose worker crashed mid-flight: the reaper
// requeues it, a live worker processes it, and the batch completes with correct
// counts. This is the end-to-end guarantee that the complete/kill ordering
// upholds — a recovered batch job still drives its batch to completion.
func TestWorker_OrphanedBatchJobRecoversAndCompletesBatch(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-batch-orphan:*")

	ctx := context.Background()
	ns := "test-batch-orphan"

	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: ns,
		Settings: senna.WorkerSettings{
			Concurrency:   1,
			Queues:        []senna.QueueConfig{{Name: "default"}},
			HeartbeatRate: time.Second,
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	var processed atomic.Int32
	w.Register("batch_job", func(ctx context.Context, job *senna.Job) error {
		processed.Add(1)
		return nil
	})

	bid := "batch-orphan"
	job := senna.NewJob("batch_job", map[string]any{"n": 1})
	job.Queue = "default"
	job.BatchID = bid

	// Batch with one pending member, registered in the jobs set.
	state := senna.BatchState{ID: bid, Total: 1, Pending: 1}
	stateData, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("marshal batch state: %v", err)
	}
	if err := redisClient.Set(ctx, w.keys.Batch(bid), string(stateData), 0).Err(); err != nil {
		t.Fatalf("set batch state: %v", err)
	}
	if err := redisClient.SAdd(ctx, w.keys.BatchJobs(bid), job.ID).Err(); err != nil {
		t.Fatalf("seed batch jobs set: %v", err)
	}

	// Simulate a crashed worker that had the job in-flight.
	jobData, err := job.Marshal()
	if err != nil {
		t.Fatalf("marshal job: %v", err)
	}
	crashedWorkerID := "crashed-batch-worker"
	staleTime := time.Now().Add(-2 * workerHeartbeatTimeout).Unix()
	workerInfo := map[string]any{"hostname": "crashed", "pid": 1, "beat_at": staleTime, "started_at": staleTime}
	workerData, err := json.Marshal(workerInfo)
	if err != nil {
		t.Fatalf("marshal worker info: %v", err)
	}
	if err := redisClient.HSet(ctx, w.keys.Workers(), crashedWorkerID, string(workerData)).Err(); err != nil {
		t.Fatalf("set crashed worker heartbeat: %v", err)
	}
	if err := redisClient.LPush(ctx, w.keys.InFlight(crashedWorkerID), string(jobData)).Err(); err != nil {
		t.Fatalf("seed crashed in-flight: %v", err)
	}

	// Reaper requeues the orphaned job to its queue.
	w.requeueOrphanedJobs(ctx)

	// A live worker fetches and processes it to completion.
	fetched, err := w.fetcher.BlockingFetch(ctx, w.id, time.Second)
	if err != nil {
		t.Fatalf("fetch requeued job: %v", err)
	}
	if fetched == nil {
		t.Fatal("expected to fetch the requeued orphaned job, got nil")
	}
	w.processJob(ctx, fetched)

	if got := processed.Load(); got < 1 {
		t.Errorf("expected handler to run at least once, ran %d times", got)
	}

	status := senna.NewBatchStatus(redisClient, ns, bid)
	if err := status.Refresh(ctx); err != nil {
		t.Fatalf("refresh batch status: %v", err)
	}
	if status.Pending() != 0 {
		t.Errorf("expected batch pending 0 after recovery, got %d", status.Pending())
	}
	if status.Successes() != 1 {
		t.Errorf("expected batch successes 1 after recovery, got %d", status.Successes())
	}

	inFlight, err := redisClient.LLen(ctx, w.keys.InFlight(w.id)).Result()
	if err != nil {
		t.Fatalf("llen in-flight: %v", err)
	}
	if inFlight != 0 {
		t.Errorf("expected job acked out of in-flight after completion, got %d", inFlight)
	}
}
