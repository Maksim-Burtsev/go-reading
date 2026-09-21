//go:build integration

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcclickhouse "github.com/testcontainers/testcontainers-go/modules/clickhouse"
)

func TestRunIngestsIntoClickHouse(t *testing.T) {
	ctx := t.Context()

	ctr, err := tcclickhouse.Run(ctx, "clickhouse/clickhouse-server:25.3",
		tcclickhouse.WithDatabase("sink"),
		tcclickhouse.WithUsername("sink"),
		tcclickhouse.WithPassword("sink"),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	chAddr, err := ctr.ConnectionHost(ctx)
	require.NoError(t, err)

	addr := freeAddr(t)
	environ := map[string]string{
		"ADDR":            addr,
		"CLICKHOUSE_ADDR": chAddr,
		"BATCH_SIZE":      "10",
		"FLUSH_INTERVAL":  "1h",
	}

	runCtx, cancel := context.WithCancel(ctx)
	stopped := make(chan struct{})
	var runErr error
	go func() {
		defer close(stopped)
		runErr = run(runCtx, []string{"clickhouse-sink"}, func(k string) string { return environ[k] }, t.Output(), t.Output())
	}()
	t.Cleanup(func() {
		cancel()
		<-stopped
	})

	baseURL := "http://" + addr
	require.Eventually(t, func() bool {
		status, _, err := get(ctx, baseURL+"/health")
		return err == nil && status == http.StatusOK
	}, 30*time.Second, 100*time.Millisecond)

	require.Equal(t, http.StatusAccepted, postEvents(t, baseURL, 15))
	require.Equal(t, http.StatusAccepted, postEvents(t, baseURL, 10))

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{chAddr},
		Auth: clickhouse.Auth{Database: "sink", Username: "sink", Password: "sink"},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	countRows := func() uint64 {
		var n uint64
		require.NoError(t, conn.QueryRow(ctx, "SELECT uniqExact(event_id) FROM events").Scan(&n))
		return n
	}
	require.Eventually(t, func() bool { return countRows() == 20 }, 10*time.Second, 50*time.Millisecond)

	status, metrics, err := get(ctx, baseURL+"/metrics")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)
	require.Contains(t, metrics, "sink_events_received_total 25")
	require.Contains(t, metrics, "sink_buffer_length 0")

	cancel()
	<-stopped
	require.NoError(t, runErr)
	require.Equal(t, uint64(25), countRows())
}

func freeAddr(t *testing.T) string {
	t.Helper()

	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

func postEvents(t *testing.T, baseURL string, n int) int {
	t.Helper()

	items := make([]string, 0, n)
	for i := range n {
		items = append(items, fmt.Sprintf(
			`{"event_id":%q,"event_type":"signup","user_id":"u-%d","ts":%q,"properties":{"plan":"pro"}}`,
			uuid.NewString(), i, time.Now().UTC().Format(time.RFC3339Nano),
		))
	}
	body := "[" + strings.Join(items, ",") + "]"

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, baseURL+"/ingest", strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	return resp.StatusCode
}

func get(ctx context.Context, url string) (int, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, "", err
	}
	body, err := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body), errors.Join(err, resp.Body.Close())
}
