# liftingcast-go

[![CI](https://github.com/theblindoracle/liftingcast-go/actions/workflows/ci.yml/badge.svg)](https://github.com/theblindoracle/liftingcast-go/actions/workflows/ci.yml)

Go client for the LiftingCast meet-state WebSocket API.

- `Client` dials the WebSocket, sends heartbeats, detects silent connections and reconnects with backoff until closed or rejected.
- `Cache` builds one meet state from LiftingCast's messages; each section a message carries replaces the cached copy.
- `Hub` keeps an upstream connection alive, merges into a `Cache` and hands each merged state to in-process listeners added with `Listen`. `Connect` switches it to another meet or credentials; listeners stay put.
- `MeetState` and friends are the meet-state types, with `NullableFloat64` for LiftingCast's loosely typed numbers.

```go
cfg := liftingcast.Config{
	BaseURL:  "wss://liftingcast.com/websocket",
	MeetID:   meetID,
	Password: password,
	APIKey:   apiKey,
}

hub := liftingcast.NewHub() // runs until Close
defer hub.Close()
hub.Connect(cfg)
stop := hub.Listen(func(state *liftingcast.MeetState) {
	// state is the full merged meet state, shared with every listener:
	// read it or keep it, but copy it before modifying it
})
defer stop() // once stop returns, the handler is not called again
```

Without a `Hub`, merge the `Client`'s raw messages into a `Cache` yourself:

```go
client := liftingcast.NewClient(cfg)
client.Start() // keeps retrying until Close
defer client.Close()
cache := liftingcast.NewCache()
for raw := range client.Messages() { // closed by Close
	state, err := cache.Merge(raw)
	if err != nil {
		continue
	}
	// state is the full merged meet state, yours to modify
}
```

`client.Status()` reports whether the connection is up right now, whether LiftingCast has rejected it, and the last error it saw. A rejected client never reconnects (bad API key, meet ID or password); `Messages()` stays open until `Close`, and you replace the client to try again. Any other server error is retried.

The library logs nothing unless given a logger: `liftingcast.NewHub(liftingcast.WithLogger(slog.Default()))`. A `Hub` passes its logger on to its `Client`.

The API is unstable until v1.
