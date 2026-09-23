# consumer: poll the next batch while the previous one is written

`Run` stops polling while a batch is written, so every slow sink call or retry backoff leaves the
consumer idle and lag grows by the length of the flush.

This moves flushing to one background goroutine fed by a channel with room for `PENDING_BATCHES`
full batches (default 2). The poll loop keeps filling the next batch while earlier ones are
written, dead-lettered and committed. Only that goroutine flushes, so commits still happen in batch
order, and `Run` waits for it before returning. The loop still allows rebalances only while its
batch is empty, as before. A failed flush no longer takes the consumer down: it is logged, later
batches keep flowing, and `Run` returns the error when it stops.

Tests: the fake sink can now be slow, and two new cases cover polling while a batch is being written
and a consumer that keeps going after a failed flush. `PENDING_BATCHES` is validated like the other
settings.
