# pipeline: dead-letter only the events ClickHouse rejects

When the insert of a batch runs out of attempts, every record of the batch goes to the DLQ, so one
event ClickHouse cannot store buries up to `BATCH_SIZE` healthy events there. `flush` now splits such
a batch in halves and inserts each half the same way, down to single events, and dead-letters only the
events that are refused on their own, each with the error of its last attempt. A batch with some
refused events is recorded as `partially_dead_lettered`, and `eventsink_rejected_events` shows
refusals. The README's flush description and metrics table and the ADR's dead-letter paragraph are
updated.

Tests cover an insert that recovers after its attempts, one refused event and a batch refused
entirely.
