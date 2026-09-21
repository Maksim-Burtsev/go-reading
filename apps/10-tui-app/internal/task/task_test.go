package task

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMatches(t *testing.T) {
	t.Parallel()

	tk := Task{Title: "Rotate TLS certificates", Tags: []string{"Security", "infra"}}
	tests := []struct {
		query string
		want  bool
	}{
		{query: "", want: true},
		{query: "tls", want: true},
		{query: "ROTATE", want: true},
		{query: "secur", want: true},
		{query: "INFRA", want: true},
		{query: "billing", want: false},
		{query: "tls certificates rotate", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, tk.Matches(tt.query))
		})
	}
}

func TestOverdue(t *testing.T) {
	t.Parallel()

	due := Date{time.Date(2026, time.September, 20, 0, 0, 0, 0, time.UTC)}
	tokyo := time.FixedZone("UTC+9", 9*60*60)
	tests := []struct {
		name string
		task Task
		now  time.Time
		want bool
	}{
		{
			name: "open and past due",
			task: Task{Status: StatusTodo, Due: due},
			now:  time.Date(2026, time.September, 21, 8, 0, 0, 0, time.UTC),
			want: true,
		},
		{
			name: "due today is not overdue",
			task: Task{Status: StatusInProgress, Due: due},
			now:  time.Date(2026, time.September, 20, 23, 59, 0, 0, time.UTC),
			want: false,
		},
		{
			name: "local calendar day decides",
			task: Task{Status: StatusTodo, Due: due},
			now:  time.Date(2026, time.September, 21, 1, 0, 0, 0, tokyo),
			want: true,
		},
		{
			name: "done is never overdue",
			task: Task{Status: StatusDone, Due: due},
			now:  time.Date(2026, time.October, 1, 0, 0, 0, 0, time.UTC),
			want: false,
		},
		{
			name: "no due date",
			task: Task{Status: StatusTodo},
			now:  time.Date(2026, time.October, 1, 0, 0, 0, 0, time.UTC),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, tt.task.Overdue(tt.now))
		})
	}
}
