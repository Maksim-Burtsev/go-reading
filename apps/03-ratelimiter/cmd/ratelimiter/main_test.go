package main

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Maksim-Burtsev/go-reading/apps/03-ratelimiter/ratelimit"
)

func TestRunRejectsInvalidSetup(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		args    []string
		env     map[string]string
		wantErr error
	}{
		{name: "positional arguments", args: []string{"ratelimiter", "serve"}, wantErr: errUsage},
		{name: "zero limit", env: map[string]string{"LIMIT": "0"}, wantErr: ratelimit.ErrInvalidConfig},
		{name: "negative burst", env: map[string]string{"BURST": "-1"}, wantErr: ratelimit.ErrInvalidConfig},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			args := tt.args
			if args == nil {
				args = []string{"ratelimiter"}
			}
			err := run(t.Context(), args, func(key string) string { return tt.env[key] }, io.Discard, io.Discard)
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestRunServesUntilCanceled(t *testing.T) {
	t.Parallel()
	env := map[string]string{"ADDR": "127.0.0.1:0", "ALGORITHM": "sliding_window", "LIMIT": "2", "PERIOD": "1h"}
	logs, logWriter := io.Pipe()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, []string{"ratelimiter"}, func(key string) string { return env[key] }, logWriter, io.Discard)
		_ = logWriter.Close()
	}()

	scanner := bufio.NewScanner(logs)
	require.True(t, scanner.Scan())
	var listening struct {
		Msg  string `json:"msg"`
		Addr string `json:"addr"`
	}
	require.NoError(t, json.Unmarshal(scanner.Bytes(), &listening))
	require.Equal(t, "listening", listening.Msg)
	go func() { _, _ = io.Copy(io.Discard, logs) }()

	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	wantStatuses := []int{http.StatusOK, http.StatusOK, http.StatusTooManyRequests}
	for i, want := range wantStatuses {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+listening.Addr+"/api/proverbs/1", nil)
		require.NoError(t, err)
		resp, err := client.Do(req)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, want, resp.StatusCode, "request %d", i)
	}

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return after cancellation")
	}
}
