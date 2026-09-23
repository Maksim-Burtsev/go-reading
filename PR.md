# jobs: roll up today's event stats every five minutes

Dashboards read `daily_event_stats`, which `rollup-daily-events` fills at 00:15 UTC for the day
before, so the current day is always empty.

This adds a `rollup-recent-events` job. Every five minutes it adds the events of the last complete
five-minute window to today's row, so today's numbers lag by at most one window; at 00:15 the daily
rollup still recomputes the finished day as before. Entries can now set `RunOnStart`, so a freshly
deployed instance publishes numbers right away instead of waiting for the first tick, and an entry
without a job is skipped. The job is configured with `ROLLUP_RECENT_SCHEDULE` (default every five
minutes) and `ROLLUP_RECENT_WINDOW` (default `5m`; it must divide a day, and `0` turns the job
off).

Tests: the job tests check the window and the day passed to SQL, including the window that closes
the previous day at midnight; the scheduler tests check that a `RunOnStart` job runs before its
first tick and that an entry without a job is never run. The invalid-spec test now gives its entry
a job, since entries without one are skipped before their spec is parsed.
