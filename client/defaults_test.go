package client

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestNormalizeSettings(t *testing.T) {
	t.Parallel()
	defaults := DefaultSettings()

	tests := []struct {
		name string
		in   Settings
		want Settings
	}{
		{
			name: "zero value uses defaults",
			in:   Settings{},
			want: defaults,
		},
		{
			name: "custom retry keeps default queue",
			in: Settings{
				DefaultRetry: 3,
			},
			want: Settings{
				DefaultQueue:  defaults.DefaultQueue,
				DefaultRetry:  3,
				BulkChunkSize: defaults.BulkChunkSize,
			},
		},
		{
			name: "custom queue preserves zero retry",
			in: Settings{
				DefaultQueue: "critical",
			},
			want: Settings{
				DefaultQueue:  "critical",
				DefaultRetry:  0,
				BulkChunkSize: defaults.BulkChunkSize,
			},
		},
		{
			name: "custom bulk chunk size",
			in: Settings{
				BulkChunkSize: 250,
			},
			want: Settings{
				DefaultQueue:  defaults.DefaultQueue,
				DefaultRetry:  0,
				BulkChunkSize: 250,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := normalizeSettings(tt.in)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatalf("normalizeSettings(%+v) mismatch (-want +got):\n%s", tt.in, diff)
			}
		})
	}
}
