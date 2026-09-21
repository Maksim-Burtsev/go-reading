package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func mapEnv(m map[string]string) func(string) string {
	return func(key string) string { return m[key] }
}

func TestLoadConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		env     map[string]string
		want    config
		wantErr string
	}{
		{
			name: "defaults",
			env:  nil,
			want: config{
				Addr:              ":8080",
				LogLevel:          slog.LevelInfo,
				ReadHeaderTimeout: 5 * time.Second,
				ReadTimeout:       10 * time.Second,
				WriteTimeout:      15 * time.Second,
				IdleTimeout:       time.Minute,
				HandlerTimeout:    10 * time.Second,
				ShutdownTimeout:   20 * time.Second,
			},
		},
		{
			name: "overrides",
			env: map[string]string{
				"ADDR":             "127.0.0.1:9000",
				"LOG_LEVEL":        "debug",
				"WRITE_TIMEOUT":    "3s",
				"HANDLER_TIMEOUT":  "2s",
				"SHUTDOWN_TIMEOUT": "1s",
			},
			want: config{
				Addr:              "127.0.0.1:9000",
				LogLevel:          slog.LevelDebug,
				ReadHeaderTimeout: 5 * time.Second,
				ReadTimeout:       10 * time.Second,
				WriteTimeout:      3 * time.Second,
				IdleTimeout:       time.Minute,
				HandlerTimeout:    2 * time.Second,
				ShutdownTimeout:   time.Second,
			},
		},
		{
			name:    "invalid duration",
			env:     map[string]string{"READ_TIMEOUT": "soon"},
			wantErr: "parse config",
		},
		{
			name:    "handler timeout not shorter than write timeout",
			env:     map[string]string{"HANDLER_TIMEOUT": "15s"},
			wantErr: "must be shorter than write timeout",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := loadConfig(mapEnv(tt.env))
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestRunServesAndShutsDown(t *testing.T) {
	t.Parallel()

	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var stdout bytes.Buffer
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, nil, mapEnv(map[string]string{"ADDR": addr}), &stdout, io.Discard)
	}()

	require.Eventually(t, func() bool {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/healthz", nil)
		if err != nil {
			return false
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 5*time.Second, 20*time.Millisecond)

	cancel()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return after context cancellation")
	}
	require.Contains(t, stdout.String(), `"msg":"http server stopped"`)
}
