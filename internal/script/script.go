package script

import (
	"context"
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
