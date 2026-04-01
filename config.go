package senna

import (
	"time"

	"github.com/redis/go-redis/v9"
)

// Config configures shared Senna settings used by clients and workers.
type Config struct {
	Redis      RedisConfig
	Namespace  string
	Worker     WorkerSettings
	Client     ClientSettings
	Encryption EncryptionSettings
	Metrics    MetricsSettings
}

// RedisConfig configures the Redis connection used by Senna.
type RedisConfig struct {
	Addr         string
	Password     string
	DB           int
	PoolSize     int
	MinIdleConns int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// Options converts the RedisConfig into go-redis client options.
func (c RedisConfig) Options() *redis.Options {
	opts := &redis.Options{
		Addr:     c.Addr,
		Password: c.Password,
		DB:       c.DB,
	}
	if c.PoolSize > 0 {
		opts.PoolSize = c.PoolSize
	}
	if c.MinIdleConns > 0 {
		opts.MinIdleConns = c.MinIdleConns
	}
	if c.DialTimeout > 0 {
		opts.DialTimeout = c.DialTimeout
	}
	if c.ReadTimeout > 0 {
		opts.ReadTimeout = c.ReadTimeout
	}
	if c.WriteTimeout > 0 {
		opts.WriteTimeout = c.WriteTimeout
	}
	return opts
}

// WorkerSettings configures worker runtime behavior.
type WorkerSettings struct {
	Concurrency           int
	Queues                []QueueConfig
	ShutdownTimeout       time.Duration
	PollInterval          time.Duration
	ScheduledPollInterval time.Duration
	HeartbeatRate         time.Duration
	PeriodicEnabled       bool
	StrictPriority        bool // If true, always process higher priority queues first (can starve lower priority)
}

const (
	// DefaultQueueName is the default queue used by clients and workers.
	DefaultQueueName = "default"
	// DefaultRetryCount is the default retry count assigned to new jobs.
	DefaultRetryCount = 25
)

// DefaultWorkerSettings returns the default worker settings.
func DefaultWorkerSettings() WorkerSettings {
	return WorkerSettings{
		Concurrency:           10,
		Queues:                []QueueConfig{{Name: DefaultQueueName, Priority: 1}},
		ShutdownTimeout:       30 * time.Second,
		PollInterval:          100 * time.Millisecond,
		ScheduledPollInterval: 5 * time.Second,
		HeartbeatRate:         5 * time.Second,
	}
}

// QueueConfig configures a worker queue and its scheduling behavior.
type QueueConfig struct {
	Name       string
	Priority   int
	Paused     bool
	Sequential bool // Only one worker globally processes this queue at a time
}

// ClientSettings configures enqueue defaults for a client.
type ClientSettings struct {
	DefaultQueue string
	DefaultRetry int
}

// DefaultClientSettings returns the default client settings.
func DefaultClientSettings() ClientSettings {
	return ClientSettings{
		DefaultQueue: DefaultQueueName,
		DefaultRetry: DefaultRetryCount,
	}
}

// EncryptionSettings configures job argument encryption.
type EncryptionSettings struct {
	Enabled bool
	Key     []byte
}

// MetricsSettings configures metrics emission.
type MetricsSettings struct {
	Enabled    bool
	StatsdAddr string
	Prefix     string
	SampleRate float64
	Tags       map[string]string
}

// Option mutates a Config.
type Option func(*Config)

// WithNamespace sets the Redis key namespace.
func WithNamespace(ns string) Option {
	return func(c *Config) {
		c.Namespace = ns
	}
}

// WithConcurrency sets the worker concurrency.
func WithConcurrency(n int) Option {
	return func(c *Config) {
		c.Worker.Concurrency = n
	}
}

// WithQueues sets the worker queue configuration.
func WithQueues(queues ...QueueConfig) Option {
	return func(c *Config) {
		c.Worker.Queues = queues
	}
}

// WithShutdownTimeout sets the worker shutdown timeout.
func WithShutdownTimeout(d time.Duration) Option {
	return func(c *Config) {
		c.Worker.ShutdownTimeout = d
	}
}

// WithEncryptionConfig enables encryption with the provided key.
func WithEncryptionConfig(key []byte) Option {
	return func(c *Config) {
		c.Encryption.Enabled = true
		c.Encryption.Key = key
	}
}

// WithMetrics enables metrics emission with the provided address and prefix.
func WithMetrics(addr, prefix string) Option {
	return func(c *Config) {
		c.Metrics.Enabled = true
		c.Metrics.StatsdAddr = addr
		c.Metrics.Prefix = prefix
	}
}

// WithPeriodicEnabled enables periodic job scheduling for workers.
func WithPeriodicEnabled() Option {
	return func(c *Config) {
		c.Worker.PeriodicEnabled = true
	}
}

// WithScheduledPollInterval sets how often a worker checks scheduled jobs.
func WithScheduledPollInterval(d time.Duration) Option {
	return func(c *Config) {
		c.Worker.ScheduledPollInterval = d
	}
}

// WithStrictPriority makes workers always prefer higher-priority queues.
func WithStrictPriority() Option {
	return func(c *Config) {
		c.Worker.StrictPriority = true
	}
}
