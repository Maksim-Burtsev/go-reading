package main

import (
	"net/netip"
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
			name: "unset values fall back to defaults",
			env:  map[string]string{"DATABASE_URL": "postgres://db:5432/app"},
			want: config{
				Addr:            ":8080",
				DatabaseURL:     "postgres://db:5432/app",
				ShutdownTimeout: 15 * time.Second,
			},
		},
		{
			name: "overrides",
			env: map[string]string{
				"ADDR":             ":9090",
				"DATABASE_URL":     "postgres://db:5432/app",
				"SHUTDOWN_TIMEOUT": "3s",
				"TRUSTED_PROXIES":  "10.0.0.0/8,2001:db8::/32",
			},
			want: config{
				Addr:            ":9090",
				DatabaseURL:     "postgres://db:5432/app",
				ShutdownTimeout: 3 * time.Second,
				TrustedProxies:  []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8"), netip.MustParsePrefix("2001:db8::/32")},
			},
		},
		{
			name:    "invalid trusted proxy",
			env:     map[string]string{"TRUSTED_PROXIES": "10.0.0.1"},
			wantErr: true,
		},
		{
			name:    "invalid duration",
			env:     map[string]string{"SHUTDOWN_TIMEOUT": "soon"},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := loadConfig(func(key string) string { return tt.env[key] })
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
