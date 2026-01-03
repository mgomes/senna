package senna

import (
	"time"

	"github.com/redis/go-redis/v9"
)

type Config struct {
	Redis      RedisConfig
	Namespace  string
	Worker     WorkerSettings
	Client     ClientSettings
	Encryption EncryptionSettings
	Metrics    MetricsSettings
}

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

type WorkerSettings struct {
	Concurrency           int
	Queues                []QueueConfig
	ShutdownTimeout       time.Duration
	PollInterval          time.Duration
	ScheduledPollInterval time.Duration
	HeartbeatRate         time.Duration
	PeriodicEnabled       bool
}

func DefaultWorkerSettings() WorkerSettings {
	return WorkerSettings{
		Concurrency:           10,
		Queues:                []QueueConfig{{Name: "default", Priority: 1}},
		ShutdownTimeout:       30 * time.Second,
		PollInterval:          100 * time.Millisecond,
		ScheduledPollInterval: 5 * time.Second,
		HeartbeatRate:         5 * time.Second,
	}
}

type QueueConfig struct {
	Name     string
	Priority int
	Paused   bool
}

type ClientSettings struct {
	DefaultQueue string
	DefaultRetry int
}

func DefaultClientSettings() ClientSettings {
	return ClientSettings{
		DefaultQueue: "default",
		DefaultRetry: 25,
	}
}

type EncryptionSettings struct {
	Enabled bool
	Key     []byte
}

type MetricsSettings struct {
	Enabled    bool
	StatsdAddr string
	Prefix     string
	SampleRate float64
	Tags       map[string]string
}

type Option func(*Config)

func WithNamespace(ns string) Option {
	return func(c *Config) {
		c.Namespace = ns
	}
}

func WithConcurrency(n int) Option {
	return func(c *Config) {
		c.Worker.Concurrency = n
	}
}

func WithQueues(queues ...QueueConfig) Option {
	return func(c *Config) {
		c.Worker.Queues = queues
	}
}

func WithShutdownTimeout(d time.Duration) Option {
	return func(c *Config) {
		c.Worker.ShutdownTimeout = d
	}
}

func WithEncryptionConfig(key []byte) Option {
	return func(c *Config) {
		c.Encryption.Enabled = true
		c.Encryption.Key = key
	}
}

func WithMetrics(addr, prefix string) Option {
	return func(c *Config) {
		c.Metrics.Enabled = true
		c.Metrics.StatsdAddr = addr
		c.Metrics.Prefix = prefix
	}
}

func WithPeriodicEnabled() Option {
	return func(c *Config) {
		c.Worker.PeriodicEnabled = true
	}
}

func WithScheduledPollInterval(d time.Duration) Option {
	return func(c *Config) {
		c.Worker.ScheduledPollInterval = d
	}
}
