package script

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/redis/go-redis/v9"
)

type Script struct {
	Name   string
	Source string
	sha    string
	mu     sync.RWMutex
}

func New(name, source string) *Script {
	return &Script{
		Name:   name,
		Source: source,
	}
}

func (s *Script) Run(ctx context.Context, client redis.Scripter, keys []string, args ...any) (any, error) {
	s.mu.RLock()
	sha := s.sha
	s.mu.RUnlock()

	if sha != "" {
		result, err := client.EvalSha(ctx, sha, keys, args...).Result()
		if err == nil {
			return result, nil
		}
		if !isNoScriptError(err) {
			return nil, err
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.sha != "" && s.sha != sha {
		result, err := client.EvalSha(ctx, s.sha, keys, args...).Result()
		if err == nil {
			return result, nil
		}
		if !isNoScriptError(err) {
			return nil, err
		}
	}

	result, err := client.Eval(ctx, s.Source, keys, args...).Result()
	if err != nil {
		return nil, err
	}

	loadedSHA, err := client.ScriptLoad(ctx, s.Source).Result()
	if err == nil {
		s.sha = loadedSHA
	}

	return result, nil
}

// RunJSON runs the script and unmarshals the JSON result into the target.
// The target must be a pointer to the struct to unmarshal into.
func (s *Script) RunJSON(ctx context.Context, client redis.Scripter, target any, keys []string, args ...any) error {
	result, err := s.Run(ctx, client, keys, args...)
	if err != nil {
		return fmt.Errorf("script %s: %w", s.Name, err)
	}

	resultStr, ok := result.(string)
	if !ok {
		return fmt.Errorf("script %s: expected string result, got %T", s.Name, result)
	}

	if err := json.Unmarshal([]byte(resultStr), target); err != nil {
		return fmt.Errorf("script %s: failed to unmarshal result: %w", s.Name, err)
	}

	return nil
}

func (s *Script) Load(ctx context.Context, client redis.Scripter) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	sha, err := client.ScriptLoad(ctx, s.Source).Result()
	if err != nil {
		return err
	}
	s.sha = sha
	return nil
}

func isNoScriptError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "NOSCRIPT")
}

// Int coerces a numeric Lua reply into an int64, accepting both int64 and
// float64 since Redis replies vary by server version and value encoding.
func Int(result any) (int64, error) {
	switch n := result.(type) {
	case int64:
		return n, nil
	case float64:
		return int64(n), nil
	default:
		return 0, fmt.Errorf("expected numeric reply, got %T", result)
	}
}

// Ints coerces an array Lua reply into a slice of int64 values, accepting both
// int64 and float64 elements. It returns an error if result is not an array,
// has fewer than n elements, or contains a non-numeric element. This guards the
// limiters against panicking on an unexpected reply shape.
func Ints(result any, n int) ([]int64, error) {
	arr, ok := result.([]any)
	if !ok {
		return nil, fmt.Errorf("expected array reply, got %T", result)
	}
	if len(arr) < n {
		return nil, fmt.Errorf("expected at least %d elements, got %d", n, len(arr))
	}
	out := make([]int64, len(arr))
	for i, v := range arr {
		val, err := Int(v)
		if err != nil {
			return nil, fmt.Errorf("element %d: %w", i, err)
		}
		out[i] = val
	}
	return out, nil
}
