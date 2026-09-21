package main

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLoadConfig(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		env     map[string]string
		check   func(t *testing.T, cfg config)
		wantErr error
	}{
		{
			name: "defaults",
			env:  map[string]string{},
			check: func(t *testing.T, cfg config) {
				require.Equal(t, "localhost:8080", cfg.Addr)
				require.Equal(t, "https://proxy.golang.org", cfg.UpstreamURL.String())
				require.Equal(t, 1024, cfg.CacheSize)
				require.Equal(t, 5*time.Minute, cfg.CacheTTL)
				require.Equal(t, slog.LevelInfo, cfg.LogLevel)
			},
		},
		{
			name: "overrides",
			env: map[string]string{
				"UPSTREAM_URL": "http://localhost:9000/api",
				"CACHE_SIZE":   "10",
				"CACHE_TTL":    "0s",
				"LOG_LEVEL":    "DEBUG",
			},
			check: func(t *testing.T, cfg config) {
				require.Equal(t, "/api", cfg.UpstreamURL.Path)
				require.Equal(t, 10, cfg.CacheSize)
				require.Zero(t, cfg.CacheTTL)
				require.Equal(t, slog.LevelDebug, cfg.LogLevel)
			},
		},
		{
			name:    "non-positive cache size",
			env:     map[string]string{"CACHE_SIZE": "0"},
			wantErr: errInvalidConfig,
		},
		{
			name:    "zero upstream timeout",
			env:     map[string]string{"UPSTREAM_TIMEOUT": "0s"},
			wantErr: errInvalidConfig,
		},
		{
			name:    "negative ttl",
			env:     map[string]string{"CACHE_TTL": "-1m"},
			wantErr: errInvalidConfig,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg, err := loadConfig(func(key string) string { return tt.env[key] })
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			tt.check(t, cfg)
		})
	}
}

func TestLoadConfigRejectsMalformedValues(t *testing.T) {
	t.Parallel()
	_, err := loadConfig(func(key string) string {
		if key == "CACHE_TTL" {
			return "soon"
		}
		return ""
	})
	require.Error(t, err)
}

type countingExpirer struct {
	calls atomic.Int32
}

func (e *countingExpirer) DeleteExpired() int {
	e.calls.Add(1)
	return 1
}

func TestSweepRunsEveryIntervalUntilCanceled(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		var e countingExpirer
		done := make(chan struct{})
		go func() {
			defer close(done)
			sweep(ctx, slog.New(slog.DiscardHandler), &e, time.Minute)
		}()

		time.Sleep(3*time.Minute + time.Second)
		cancel()
		<-done
		require.Equal(t, int32(3), e.calls.Load())
	})
}
