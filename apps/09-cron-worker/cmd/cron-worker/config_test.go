package main

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		env     map[string]string
		check   func(t *testing.T, cfg config)
		wantErr error
	}{
		{
			name: "defaults",
			check: func(t *testing.T, cfg config) {
				require.Equal(t, "postgres://cron:cron@localhost:5432/cron?sslmode=disable", cfg.DatabaseURL)
				require.Equal(t, slog.LevelInfo, cfg.LogLevel)
				require.Equal(t, 30*time.Minute, cfg.PendingOrderTTL)
				require.Equal(t, "0 */5 * * * *", cfg.PurgeSessionsSchedule)
			},
		},
		{
			name: "overrides",
			env: map[string]string{
				"LOG_LEVEL":                "debug",
				"PENDING_ORDER_TTL":        "2h",
				"EXPIRE_ORDERS_SCHEDULE":   "@every 10s",
				"SESSION_PURGE_BATCH_SIZE": "50",
			},
			check: func(t *testing.T, cfg config) {
				require.Equal(t, slog.LevelDebug, cfg.LogLevel)
				require.Equal(t, 2*time.Hour, cfg.PendingOrderTTL)
				require.Equal(t, "@every 10s", cfg.ExpireOrdersSchedule)
				require.Equal(t, 50, cfg.SessionPurgeBatchSize)
			},
		},
		{
			name:    "non-positive batch size",
			env:     map[string]string{"SESSION_PURGE_BATCH_SIZE": "0"},
			wantErr: errInvalidConfig,
		},
		{
			name:    "negative order TTL",
			env:     map[string]string{"PENDING_ORDER_TTL": "-1m"},
			wantErr: errInvalidConfig,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg, err := parseConfig(func(key string) string { return tt.env[key] })

			require.ErrorIs(t, err, tt.wantErr)
			if tt.check != nil {
				tt.check(t, cfg)
			}
		})
	}
}
