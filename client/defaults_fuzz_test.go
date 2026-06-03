package client

import "testing"

func FuzzNormalizeSettings(f *testing.F) {
	f.Add("", 0)
	f.Add("", 3)
	f.Add("critical", 0)

	f.Fuzz(func(t *testing.T, queue string, retry int) {
		if len(queue) > 1024 {
			t.Skip()
		}

		got := normalizeSettings(Settings{
			DefaultQueue: queue,
			DefaultRetry: retry,
		})
		defaults := DefaultSettings()

		wantQueue := queue
		wantRetry := retry
		if queue == "" && retry == 0 {
			wantQueue = defaults.DefaultQueue
			wantRetry = defaults.DefaultRetry
		} else if queue == "" {
			wantQueue = defaults.DefaultQueue
		}

		if got.DefaultQueue != wantQueue {
			t.Errorf("normalizeSettings({DefaultQueue:%q, DefaultRetry:%d}).DefaultQueue = %q, want %q",
				queue, retry, got.DefaultQueue, wantQueue,
			)
		}
		if got.DefaultRetry != wantRetry {
			t.Errorf("normalizeSettings({DefaultQueue:%q, DefaultRetry:%d}).DefaultRetry = %d, want %d",
				queue, retry, got.DefaultRetry, wantRetry,
			)
		}
	})
}
