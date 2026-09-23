package config

import (
	"log/slog"
	"net/netip"
	"testing"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/require"
)

func TestLoad(t *testing.T) {
	t.Parallel()
	defaults := Config{
		Addr:            "localhost:8080",
		Algorithm:       TokenBucket,
		Limit:           5,
		Period:          time.Second,
		Burst:           10,
		CleanupInterval: time.Minute,
		MaxWaiters:      100,
		ShutdownTimeout: 10 * time.Second,
		LogLevel:        slog.LevelInfo,
	}

	tests := []struct {
		name    string
		env     map[string]string
		want    func(Config) Config
		wantErr error
	}{
		{
			name: "defaults",
			want: func(c Config) Config { return c },
		},
		{
			name: "empty values take defaults",
			env:  map[string]string{"ADDR": "", "LIMIT": ""},
			want: func(c Config) Config { return c },
		},
		{
			name: "overrides",
			env: map[string]string{
				"ADDR":             ":9090",
				"ALGORITHM":        "sliding_window",
				"LIMIT":            "100",
				"PERIOD":           "1m",
				"BURST":            "1",
				"CLEANUP_INTERVAL": "30s",
				"MAX_WAIT":         "250ms",
				"MAX_WAITERS":      "8",
				"TRUSTED_PROXIES":  "10.0.0.0/8,2001:db8::/32",
				"SHUTDOWN_TIMEOUT": "3s",
				"LOG_LEVEL":        "debug",
			},
			want: func(Config) Config {
				return Config{
					Addr:            ":9090",
					Algorithm:       SlidingWindow,
					Limit:           100,
					Period:          time.Minute,
					Burst:           1,
					CleanupInterval: 30 * time.Second,
					MaxWait:         250 * time.Millisecond,
					MaxWaiters:      8,
					TrustedProxies:  []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8"), netip.MustParsePrefix("2001:db8::/32")},
					ShutdownTimeout: 3 * time.Second,
					LogLevel:        slog.LevelDebug,
				}
			},
		},
		{
			name:    "unknown algorithm",
			env:     map[string]string{"ALGORITHM": "leaky_bucket"},
			wantErr: ErrUnknownAlgorithm,
		},
		{
			name: "malformed period",
			env:  map[string]string{"PERIOD": "soon"},
		},
		{
			name: "proxy address without prefix length",
			env:  map[string]string{"TRUSTED_PROXIES": "10.0.0.1"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := Load(func(key string) string { return tt.env[key] })
			if tt.want == nil {
				var parseErr env.ParseError
				require.ErrorAs(t, err, &parseErr)
				if tt.wantErr != nil {
					require.ErrorIs(t, parseErr.Err, tt.wantErr)
				}
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want(defaults), got)
		})
	}
}
