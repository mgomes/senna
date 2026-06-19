package worker

import (
	"strings"
	"testing"
)

func TestEnqueueScheduledScriptUsesRedisServerTime(t *testing.T) {
	if !strings.Contains(enqueueScheduledLua, "redis.call('TIME')") {
		t.Fatal("enqueue scheduled script does not use Redis server time")
	}
}
