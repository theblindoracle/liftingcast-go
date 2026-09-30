# liftingcast-go

Go client for the LiftingCast meet-state WebSocket API.

- `Client` dials the WebSocket, sends heartbeats, detects silent connections and reconnects with backoff.
- `Cache` merges partial messages into one meet state.
- `Hub` keeps an upstream connection alive, merges into a `Cache` and hands each merged state to in-process `BackendListener`s.
- `MeetApiResponse` and friends are the meet-state types, with `NullableFloat64` for LiftingCast's loosely typed numbers.

```go
hub := liftingcast.NewHub(baseURL, meetID, password, apiKey)
go hub.Run()
hub.RegisterBackendListener() <- liftingcast.NewBackendListener("mine", func(state *liftingcast.MeetApiResponse) {
	// state is the full merged meet state
})
```

The API is unstable until v1.
