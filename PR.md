# 05-lru-cache: persist the cache across restarts

Every deploy restarts the proxy with an empty cache, and for the first minutes after a rollout every
request goes to the upstream. This saves the cache to `SNAPSHOT_PATH` every `SNAPSHOT_INTERVAL`
(default 1m) and on shutdown, and restores it on startup, so a restarted proxy starts warm.

- `lru.Cache` gets `All`, an iterator over the live entries from the least to the most recently
  used.
- `Proxy.SaveSnapshot` streams the cached responses as JSON in that order and
  `Proxy.LoadSnapshot` stores them back, so recency survives the restart. `LoadSnapshot` decodes
  the whole snapshot before storing anything, so a corrupt file leaves the cache untouched.
- Snapshots are off unless `SNAPSHOT_PATH` is set, and a missing file on the first start is fine.

Tests: `TestAllYieldsLiveEntries` for the iterator, `TestSnapshotRoundTrip` from one proxy's cache
to another's, and `TestLoadSnapshotRejectsCorruptInput` for a truncated snapshot.
