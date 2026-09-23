//go:build integration

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestRunShutdownCutsOffStuckRequests(t *testing.T) {
	ctx := t.Context()

	ctr, err := postgres.Run(ctx, "postgres:16-alpine", postgres.BasicWaitStrategies())
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)
	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	env := map[string]string{
		"ADDR":             "127.0.0.1:0",
		"DATABASE_URL":     dsn,
		"SHUTDOWN_TIMEOUT": "200ms",
	}
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	logs, logWriter := io.Pipe()
	listening := listeningAddr(logs)
	ran := make(chan error, 1)
	go func() {
		err := run(runCtx, nil, func(key string) string { return env[key] }, io.Discard, logWriter)
		_ = logWriter.Close()
		ran <- err
	}()

	var addr string
	select {
	case addr = <-listening:
	case err := <-ran:
		t.Fatalf("run returned before listening: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("service did not start listening")
	}

	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	var userID int64
	err = pool.QueryRow(ctx, `INSERT INTO users (email, name) VALUES ('stuck@example.com', 'Stuck') RETURNING id`).Scan(&userID)
	require.NoError(t, err)

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback(context.WithoutCancel(ctx)) })
	_, err = tx.Exec(ctx, "LOCK TABLE orders IN ACCESS EXCLUSIVE MODE")
	require.NoError(t, err)

	requested := make(chan error, 1)
	go func() {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://%s/users/%d/orders", addr, userID), nil)
		if err != nil {
			requested <- err
			return
		}
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			_ = resp.Body.Close()
		}
		requested <- err
	}()

	require.Eventually(t, func() bool {
		var waiting int
		err := pool.QueryRow(ctx, "SELECT count(*) FROM pg_locks WHERE NOT granted").Scan(&waiting)
		return err == nil && waiting > 0
	}, 10*time.Second, 20*time.Millisecond, "the request never waited for the table lock")

	stop()
	select {
	case err := <-ran:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(10 * time.Second):
		t.Fatal("run did not return while a request was stuck in the database")
	}
	require.Error(t, <-requested, "the stuck request got a response")
}

func listeningAddr(logs io.Reader) <-chan string {
	addr := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(logs)
		for sc.Scan() {
			var rec struct {
				Msg  string `json:"msg"`
				Addr string `json:"addr"`
			}
			if json.Unmarshal(sc.Bytes(), &rec) == nil && rec.Msg == "listening" {
				addr <- rec.Addr
			}
		}
	}()
	return addr
}
