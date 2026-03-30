package client

import (
	"testing"

	"github.com/mgomes/senna"
)

func TestClient_New_PreservesExplicitDefaultRetry(t *testing.T) {
	client, err := New(&Config{
		Redis: senna.RedisConfig{
			Addr: getTestRedisAddr(),
		},
		Namespace: "test",
		Settings: Settings{
			DefaultRetry: 3,
		},
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = client.Close() }()

	if client.settings.DefaultQueue != senna.DefaultQueueName {
		t.Fatalf("expected default queue %q, got %q", senna.DefaultQueueName, client.settings.DefaultQueue)
	}
	if client.settings.DefaultRetry != 3 {
		t.Fatalf("expected default retry 3, got %d", client.settings.DefaultRetry)
	}
}
