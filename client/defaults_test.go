package client

import (
	"testing"
)

func TestNormalizeSettings(t *testing.T) {
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
				DefaultQueue: defaults.DefaultQueue,
				DefaultRetry: 3,
			},
		},
		{
			name: "custom queue preserves zero retry",
			in: Settings{
				DefaultQueue: "critical",
			},
			want: Settings{
				DefaultQueue: "critical",
				DefaultRetry: 0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeSettings(tt.in)
			if got != tt.want {
				t.Fatalf("normalizeSettings(%+v) = %+v, want %+v", tt.in, got, tt.want)
			}
		})
	}
}
