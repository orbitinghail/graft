Now: Client Runtime
- storage
- remote reads/writes
- prefetcher
  - do we need an index tracking which offsets we have for the latest snapshot? if not, how does the prefetcher avoid re-fetching offsets we already have? or more generally, how can we avoid refetching efficiently?
- subscriptions
  - rather than N background tasks, consider one background task and a set of volumes we are subscribed to. For now we can refresh all of them at a set interval. Eventually we might want to use a websocket or long polling to handle this.

Upcoming:
- consider switching last_offset to length for a more ergonomic API
- consider switching pagestore to websockets or http streaming bodies
- end to end testing framework
- garbage collection
- authentication (api keys)
