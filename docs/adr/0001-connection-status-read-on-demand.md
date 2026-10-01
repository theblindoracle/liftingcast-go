# Connection status is read on demand from the Client, not pushed as events

The Client records its own connection status (connected, last error) in the goroutine that sees each change, and callers read it with `Status()`. The Client's other output is a channel carrying only meet state. We rejected one ordered event channel (`Connected | Message | Dropped | ServerError`) that consumers would turn into status themselves. With that design, status is only as fresh as the last event a consumer handled, so a consumer blocked on meet state would keep reporting a dead connection as connected. Every consumer would also have to reimplement the same `switch`. The only consumer today, the overlays status endpoint, reads status on demand.

## Consequences

- Status describes the connection right now, even when meet states sent before a drop haven't been consumed yet.
- Nothing can react to status changes without polling. If something needs to, add a change signal next to `Status()` (e.g. `StatusChanged() <-chan struct{}`) rather than replacing it with an event stream.
