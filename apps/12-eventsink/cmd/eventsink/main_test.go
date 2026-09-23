package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLoadConfig(t *testing.T) {
	t.Parallel()

	defaults := config{ //nolint:gosec // G101: the local docker-compose credentials documented in README.md
		Addr:            ":8012",
		KafkaBrokers:    []string{"localhost:19012"},
		KafkaGroup:      "eventsink",
		KafkaTopic:      "events",
		KafkaDLQTopic:   "events.dlq",
		ClickHouseURL:   "clickhouse://eventsink:eventsink@localhost:9012/eventsink?dial_timeout=5s&compress=lz4",
		DatabaseURL:     "postgres://eventsink:eventsink@localhost:5412/eventsink?sslmode=disable",
		BatchSize:       1000,
		BatchTimeout:    2 * time.Second,
		MaxAttempts:     5,
		RetryBackoff:    200 * time.Millisecond,
		AttemptTimeout:  3 * time.Second,
		ShutdownTimeout: 15 * time.Second,
	}
	tests := []struct {
		name    string
		env     map[string]string
		want    func(c *config)
		wantErr bool
	}{
		{name: "defaults", env: map[string]string{}, want: func(*config) {}},
		{
			name: "overrides",
			env: map[string]string{
				"KAFKA_BROKERS": "k1:9092,k2:9092",
				"BATCH_SIZE":    "5000",
				"BATCH_TIMEOUT": "500ms",
				"MAX_ATTEMPTS":  "1",
			},
			want: func(c *config) {
				c.KafkaBrokers = []string{"k1:9092", "k2:9092"}
				c.BatchSize = 5000
				c.BatchTimeout = 500 * time.Millisecond
				c.MaxAttempts = 1
			},
		},
		{name: "malformed duration", env: map[string]string{"BATCH_TIMEOUT": "soon"}, wantErr: true},
		{name: "malformed batch size", env: map[string]string{"BATCH_SIZE": "many"}, wantErr: true},
		{name: "zero batch size", env: map[string]string{"BATCH_SIZE": "0"}, wantErr: true},
		{name: "zero batch timeout", env: map[string]string{"BATCH_TIMEOUT": "0s"}, wantErr: true},
		{name: "zero attempts", env: map[string]string{"MAX_ATTEMPTS": "0"}, wantErr: true},
		{name: "zero attempt timeout", env: map[string]string{"ATTEMPT_TIMEOUT": "0s"}, wantErr: true},
		{name: "dead-letter topic equals input topic", env: map[string]string{"KAFKA_DLQ_TOPIC": "events"}, wantErr: true},
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
			want := defaults
			tt.want(&want)
			require.Equal(t, want, got)
		})
	}
}
