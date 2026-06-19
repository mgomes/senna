package ratelimit

import (
	"strings"
	"testing"
)

func TestLimiterDecisionScriptsUseRedisServerTime(t *testing.T) {
	tests := map[string]string{
		"bucket":             bucketScript.Source,
		"window":             windowScript.Source,
		"leaky":              leakyScript.Source,
		"points_check":       pointsCheckScript.Source,
		"concurrent_acquire": concurrentAcquireScript.Source,
		"concurrent_reclaim": concurrentReclaimScript.Source,
	}

	for name, source := range tests {
		t.Run(name, func(t *testing.T) {
			if !strings.Contains(source, "redis.call(\"TIME\")") {
				t.Fatalf("%s script does not use Redis server time", name)
			}
		})
	}
}
