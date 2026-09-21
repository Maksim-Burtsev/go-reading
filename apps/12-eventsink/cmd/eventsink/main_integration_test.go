//go:build integration

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcclickhouse "github.com/testcontainers/testcontainers-go/modules/clickhouse"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Maksim-Burtsev/go-reading/apps/12-eventsink/internal/pipeline"
)

const (
	topic       = "events"
	dlqTopic    = "events.dlq"
	validEvents = 120
	allRecords  = validEvents + 2
)

func TestRunEndToEnd(t *testing.T) {
	ctx := t.Context()
	environ := startDependencies(t)

	kafka := newKafkaClient(t, environ["KAFKA_BROKERS"])
	adm := kadm.NewClient(kafka)
	created, err := adm.CreateTopics(ctx, 3, 1, nil, topic, dlqTopic)
	require.NoError(t, err)
	require.NoError(t, created.Error())
	produceEvents(t, kafka)

	ch, err := clickhouse.ParseDSN(environ["CLICKHOUSE_URL"])
	require.NoError(t, err)
	chConn, err := clickhouse.Open(ch)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, chConn.Close()) })

	pg, err := pgxpool.New(ctx, environ["DATABASE_URL"])
	require.NoError(t, err)
	t.Cleanup(pg.Close)

	t.Run("events reach clickhouse, batches reach postgres, offsets are committed", func(t *testing.T) {
		environ["KAFKA_GROUP"] = "eventsink"
		baseURL, stop := startService(t, environ)

		requireCommitted(t, adm, "eventsink")
		require.Equal(t, uint64(validEvents), queryCount(t, chConn, "SELECT count() FROM events FINAL"))

		var records, deadLettered, partitionRecords int
		require.NoError(t, pg.QueryRow(t.Context(), `
			SELECT sum(record_count), sum(dead_letter_count),
			       (SELECT sum((p->>'records')::int) FROM batches, jsonb_array_elements(partitions) AS p)
			FROM batches WHERE status = 'inserted'`).Scan(&records, &deadLettered, &partitionRecords))
		require.Equal(t, allRecords, records)
		require.Equal(t, 1, deadLettered)
		require.Equal(t, allRecords, partitionRecords)

		dead := readRecords(t, newKafkaClient(t, environ["KAFKA_BROKERS"], kgo.ConsumeTopics(dlqTopic)), 1)
		require.Equal(t, "not json", string(dead[0].Value))

		status, body := get(t, baseURL+"/health")
		require.Equal(t, http.StatusOK, status)
		require.JSONEq(t, `{"status":"ok","dependencies":{"clickhouse":"ok","kafka":"ok","postgres":"ok"}}`, body)

		status, body = get(t, baseURL+"/metrics")
		require.Equal(t, http.StatusOK, status)
		require.Contains(t, body, "eventsink_records_consumed_total "+strconv.Itoa(allRecords))
		require.Contains(t, body, `eventsink_dead_letters_total{reason="invalid"} 1`)
		require.Contains(t, body, `eventsink_batches_flushed_total{outcome="inserted"}`)

		require.NoError(t, stop())
	})

	t.Run("replaying the topic leaves one row per event", func(t *testing.T) {
		environ["KAFKA_GROUP"] = "eventsink-replay"
		_, stop := startService(t, environ)

		requireCommitted(t, adm, "eventsink-replay")
		require.NoError(t, stop())

		require.Equal(t, uint64(validEvents), queryCount(t, chConn, "SELECT count() FROM events FINAL"))
		require.NoError(t, chConn.Exec(t.Context(), "OPTIMIZE TABLE events FINAL"))
		require.Equal(t, uint64(validEvents), queryCount(t, chConn, "SELECT count() FROM events"))

		var batchRecords int
		require.NoError(t, pg.QueryRow(t.Context(), "SELECT sum(record_count) FROM batches").Scan(&batchRecords))
		require.Equal(t, 2*allRecords, batchRecords)
	})
}

func startDependencies(t *testing.T) map[string]string {
	t.Helper()
	ctx := t.Context()

	var (
		wg                  sync.WaitGroup
		rp                  *redpanda.Container
		ch                  *tcclickhouse.ClickHouseContainer
		pg                  *postgres.PostgresContainer
		rpErr, chErr, pgErr error
	)
	wg.Go(func() { rp, rpErr = redpanda.Run(ctx, "redpandadata/redpanda:v25.1.1") })
	wg.Go(func() {
		ch, chErr = tcclickhouse.Run(ctx, "clickhouse/clickhouse-server:25.3",
			tcclickhouse.WithDatabase("eventsink"),
			tcclickhouse.WithUsername("eventsink"),
			tcclickhouse.WithPassword("eventsink"),
		)
	})
	wg.Go(func() {
		pg, pgErr = postgres.Run(ctx, "postgres:16-alpine",
			postgres.WithDatabase("eventsink"),
			postgres.WithUsername("eventsink"),
			postgres.WithPassword("eventsink"),
			postgres.BasicWaitStrategies(),
		)
	})
	wg.Wait()
	testcontainers.CleanupContainer(t, rp)
	testcontainers.CleanupContainer(t, ch)
	testcontainers.CleanupContainer(t, pg)
	require.NoError(t, rpErr)
	require.NoError(t, chErr)
	require.NoError(t, pgErr)

	broker, err := rp.KafkaSeedBroker(ctx)
	require.NoError(t, err)
	chHost, err := ch.ConnectionHost(ctx)
	require.NoError(t, err)
	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	return map[string]string{
		"KAFKA_BROKERS":  broker,
		"CLICKHOUSE_URL": "clickhouse://eventsink:eventsink@" + chHost + "/eventsink",
		"DATABASE_URL":   dsn,
		"BATCH_SIZE":     "50",
		"BATCH_TIMEOUT":  "200ms",
		"RETRY_BACKOFF":  "10ms",
	}
}

func startService(t *testing.T, environ map[string]string) (string, func() error) {
	t.Helper()

	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	env := map[string]string{"ADDR": addr}
	for k, v := range environ {
		env[k] = v
	}

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- run(ctx, nil, func(k string) string { return env[k] }, t.Output(), t.Output()) }()

	var once sync.Once
	var runErr error
	stop := func() error {
		once.Do(func() {
			cancel()
			runErr = <-done
		})
		return runErr
	}
	t.Cleanup(func() { _ = stop() })

	baseURL := "http://" + addr
	require.Eventually(t, func() bool {
		status, _, err := tryGet(t.Context(), baseURL+"/health")
		return err == nil && status == http.StatusOK
	}, 60*time.Second, 100*time.Millisecond)
	return baseURL, stop
}

func newKafkaClient(t *testing.T, broker string, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(append(opts, kgo.SeedBrokers(broker))...)
	require.NoError(t, err)
	t.Cleanup(cl.Close)
	return cl
}

func produceEvents(t *testing.T, cl *kgo.Client) {
	t.Helper()
	event := func(n int) []byte {
		return fmt.Appendf(nil, `{"id":"e-%d","type":"order.created","source":"checkout","occurred_at":"2026-09-21T10:00:%02d.000Z","payload":{"n":%d}}`,
			n, n%60, n)
	}
	records := make([]*kgo.Record, 0, allRecords)
	for n := range validEvents {
		records = append(records, &kgo.Record{Topic: topic, Key: []byte(strconv.Itoa(n)), Value: event(n)})
	}
	records = append(records,
		&kgo.Record{Topic: topic, Key: []byte("0"), Value: event(0)},
		&kgo.Record{Topic: topic, Key: []byte("poison"), Value: []byte("not json")},
	)
	require.NoError(t, cl.ProduceSync(t.Context(), records...).FirstErr())
}

func requireCommitted(t *testing.T, adm *kadm.Client, group string) {
	t.Helper()
	require.Eventually(t, func() bool {
		committed, err := adm.FetchOffsets(t.Context(), group)
		if err != nil || committed.Error() != nil {
			return false
		}
		ends, err := adm.ListEndOffsets(t.Context(), topic)
		if err != nil || ends.Error() != nil {
			return false
		}
		var total int64
		caughtUp := true
		ends.Each(func(end kadm.ListedOffset) {
			total += end.Offset
			if c, ok := committed.Lookup(topic, end.Partition); end.Offset > 0 && (!ok || c.At != end.Offset) {
				caughtUp = false
			}
		})
		return caughtUp && total == allRecords
	}, 60*time.Second, 100*time.Millisecond)
}

func readRecords(t *testing.T, cl *kgo.Client, n int) []*kgo.Record {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	var records []*kgo.Record
	for len(records) < n {
		fetches := cl.PollFetches(ctx)
		require.NoError(t, fetches.Err())
		records = append(records, fetches.Records()...)
	}
	for _, r := range records {
		for _, h := range r.Headers {
			if h.Key == pipeline.HeaderError {
				require.Contains(t, string(h.Value), "invalid event")
			}
		}
	}
	return records
}

func queryCount(t *testing.T, conn clickhouse.Conn, query string) uint64 {
	t.Helper()
	var n uint64
	require.NoError(t, conn.QueryRow(t.Context(), query).Scan(&n))
	return n
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	status, body, err := tryGet(t.Context(), url)
	require.NoError(t, err)
	return status, body
}

func tryGet(ctx context.Context, url string) (int, string, error) {
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
