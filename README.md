# liftingcast-go

[![CI](https://github.com/theblindoracle/liftingcast-go/actions/workflows/ci.yml/badge.svg)](https://github.com/theblindoracle/liftingcast-go/actions/workflows/ci.yml)

Go client for the LiftingCast meet-state WebSocket API.

- `Client` dials the WebSocket, sends heartbeats, detects silent connections and reconnects with backoff until closed or rejected.
- `Cache` builds one meet state from LiftingCast's messages; each section a message carries replaces the cached copy.
- `Hub` keeps an upstream connection alive, merges into a `Cache` and hands each merged state to in-process `BackendListener`s.
- `MeetApiResponse` and friends are the meet-state types, with `NullableFloat64` for LiftingCast's loosely typed numbers.

```go
hub := liftingcast.NewHub(baseURL, meetID, password, apiKey)
go hub.Run()
hub.RegisterBackendListener() <- liftingcast.NewBackendListener("mine", func(state *liftingcast.MeetApiResponse) {
	// state is the full merged meet state
})
```

Without a `Hub`, merge the `Client`'s raw messages into a `Cache` yourself:

```go
client := liftingcast.NewClient(baseURL, meetID, password, apiKey)
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

The API is unstable until v1.
