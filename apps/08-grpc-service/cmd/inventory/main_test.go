package main

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLoadConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		env     map[string]string
		want    config
		wantErr bool
	}{
		{
			name: "defaults",
			want: config{
				Addr:            ":50051",
				DefaultTimeout:  5 * time.Second,
				ShutdownTimeout: 10 * time.Second,
				LogLevel:        slog.LevelInfo,
			},
		},
		{
			name: "overrides",
			env: map[string]string{
				"INVENTORY_ADDR":             "127.0.0.1:9000",
				"INVENTORY_DEFAULT_TIMEOUT":  "250ms",
				"INVENTORY_SHUTDOWN_TIMEOUT": "3s",
				"INVENTORY_LOG_LEVEL":        "debug",
			},
			want: config{
				Addr:            "127.0.0.1:9000",
				DefaultTimeout:  250 * time.Millisecond,
				ShutdownTimeout: 3 * time.Second,
				LogLevel:        slog.LevelDebug,
			},
		},
		{name: "malformed duration", env: map[string]string{"INVENTORY_DEFAULT_TIMEOUT": "soon"}, wantErr: true},
		{name: "non-positive timeout", env: map[string]string{"INVENTORY_SHUTDOWN_TIMEOUT": "0s"}, wantErr: true},
		{name: "unknown log level", env: map[string]string{"INVENTORY_LOG_LEVEL": "loud"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg, err := loadConfig(func(key string) string { return tt.env[key] })
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, cfg)
		})
	}
}
