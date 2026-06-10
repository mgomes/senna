package senna

import (
	"testing"
	"time"
)

func FuzzRedisConfigOptions(f *testing.F) {
	f.Add("localhost:6379", "", 0, 0, 0, int64(time.Second), int64(0), int64(0))
	f.Add("redis.example.com:6380", "secret", 3, 100, 10, int64(5*time.Second), int64(3*time.Second), int64(2*time.Second))

	f.Fuzz(func(
		t *testing.T,
		addr string,
		password string,
		db int,
		poolSize int,
		minIdleConns int,
		dialTimeout int64,
		readTimeout int64,
		writeTimeout int64,
	) {
		if !smallValidString(addr) || !smallValidString(password) {
			t.Skip()
		}

		cfg := RedisConfig{
			Addr:         addr,
			Password:     password,
			DB:           db,
			PoolSize:     poolSize,
			MinIdleConns: minIdleConns,
			DialTimeout:  time.Duration(dialTimeout),
			ReadTimeout:  time.Duration(readTimeout),
			WriteTimeout: time.Duration(writeTimeout),
		}

		got := cfg.Options()
		if got.Addr != cfg.Addr {
			t.Errorf("RedisConfig.Options().Addr = %q, want %q", got.Addr, cfg.Addr)
		}
		if got.Password != cfg.Password {
			t.Errorf("RedisConfig.Options().Password = %q, want %q", got.Password, cfg.Password)
		}
		if got.DB != cfg.DB {
			t.Errorf("RedisConfig.Options().DB = %d, want %d", got.DB, cfg.DB)
		}
		if !got.ContextTimeoutEnabled {
			t.Error("RedisConfig.Options().ContextTimeoutEnabled = false, want true")
		}
		if cfg.PoolSize > 0 && got.PoolSize != cfg.PoolSize {
			t.Errorf("RedisConfig.Options().PoolSize = %d, want %d", got.PoolSize, cfg.PoolSize)
		}
		if cfg.PoolSize <= 0 && got.PoolSize != 0 {
			t.Errorf("RedisConfig.Options().PoolSize = %d, want 0", got.PoolSize)
		}
		if cfg.MinIdleConns > 0 && got.MinIdleConns != cfg.MinIdleConns {
			t.Errorf("RedisConfig.Options().MinIdleConns = %d, want %d", got.MinIdleConns, cfg.MinIdleConns)
		}
		if cfg.MinIdleConns <= 0 && got.MinIdleConns != 0 {
			t.Errorf("RedisConfig.Options().MinIdleConns = %d, want 0", got.MinIdleConns)
		}
		assertDurationOption(t, "DialTimeout", got.DialTimeout, cfg.DialTimeout)
		assertDurationOption(t, "ReadTimeout", got.ReadTimeout, cfg.ReadTimeout)
		assertDurationOption(t, "WriteTimeout", got.WriteTimeout, cfg.WriteTimeout)
	})
}

func assertDurationOption(t *testing.T, name string, got time.Duration, want time.Duration) {
	t.Helper()
	if want > 0 && got != want {
		t.Errorf("RedisConfig.Options().%s = %v, want %v", name, got, want)
	}
	if want <= 0 && got != 0 {
		t.Errorf("RedisConfig.Options().%s = %v, want 0", name, got)
	}
}
