# batcher: bound the buffer by bytes, not only by events

`BUFFER_SIZE` caps the number of buffered events, but a single event can carry up to 4 MiB of
properties, so the buffer does not bound memory. This adds `BUFFER_MAX_BYTES` (default 256 MiB):
`Enqueue` answers 429 when the payload would exceed it, a batch gives its bytes back once it leaves
memory, and `sink_buffer_bytes` and `sink_batch_size_bytes` show it. `Retry-After` now follows
`FLUSH_INTERVAL` instead of a fixed second.

Tests cover the byte budget in `Enqueue` (fits, exact fill, over budget, larger than the budget) and
the release of bytes after a stored and after a dropped batch.
