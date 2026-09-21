package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLoadConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		env     map[string]string
		check   func(t *testing.T, cfg config)
		wantErr string
	}{
		{
			name: "defaults",
			env:  map[string]string{},
			check: func(t *testing.T, cfg config) {
				require.Equal(t, ":8080", cfg.Addr)
				require.Equal(t, 8, cfg.Workers)
				require.Equal(t, 5, cfg.MaxAttempts)
				require.Equal(t, 500*time.Millisecond, cfg.BackoffBase)
				require.Equal(t, 20*time.Second, cfg.DrainTimeout)
			},
		},
		{
			name: "overrides",
			env:  map[string]string{"WORKERS": "32", "BACKOFF_MAX": "1m", "ADDR": "127.0.0.1:9000"},
			check: func(t *testing.T, cfg config) {
				require.Equal(t, 32, cfg.Workers)
				require.Equal(t, time.Minute, cfg.BackoffMax)
				require.Equal(t, "127.0.0.1:9000", cfg.Addr)
			},
		},
		{
			name:    "malformed duration",
			env:     map[string]string{"DRAIN_TIMEOUT": "soon"},
			wantErr: "parse env",
		},
		{
			name:    "zero workers",
			env:     map[string]string{"WORKERS": "0"},
			wantErr: "WORKERS must be at least 1",
		},
		{
			name:    "backoff base above max",
			env:     map[string]string{"BACKOFF_BASE": "1m", "BACKOFF_MAX": "1s"},
			wantErr: "need 0 < BACKOFF_BASE <= BACKOFF_MAX",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg, err := loadConfig(func(key string) string { return tt.env[key] })
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			tt.check(t, cfg)
		})
	}
}
