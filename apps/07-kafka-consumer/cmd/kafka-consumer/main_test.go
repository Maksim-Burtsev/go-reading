package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLoadConfig(t *testing.T) {
	t.Parallel()

	defaults := config{
		Brokers:         []string{"localhost:9092"},
		Group:           "events-sink",
		Topic:           "events",
		DeadLetterTopic: "events.dlq",
		BatchSize:       100,
		BatchTimeout:    time.Second,
		MaxAttempts:     3,
		ShutdownTimeout: 10 * time.Second,
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
				"BATCH_SIZE":    "500",
				"BATCH_TIMEOUT": "250ms",
			},
			want: func(c *config) {
				c.Brokers = []string{"k1:9092", "k2:9092"}
				c.BatchSize = 500
				c.BatchTimeout = 250 * time.Millisecond
			},
		},
		{name: "malformed duration", env: map[string]string{"BATCH_TIMEOUT": "soon"}, wantErr: true},
		{name: "zero batch size", env: map[string]string{"BATCH_SIZE": "0"}, wantErr: true},
		{name: "zero attempts", env: map[string]string{"MAX_ATTEMPTS": "0"}, wantErr: true},
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
