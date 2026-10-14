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
var last uint64
stop := hub.Listen(func(u liftingcast.Update) {
	// u.State is the full merged meet state, shared with every listener:
	// read it or keep it, but copy it before modifying it
	if last != 0 && u.Connection != last {
		// reconnected since the previous state: anything that happened in
		// the gap may have been missed
		log.Printf("reconnected; state received at %v follows a gap", u.ReceivedAt)
	}
	last = u.Connection
})
defer stop() // once stop returns, the handler is not called again
```

Each `Update` carries the connection its state arrived on and when it was received (`ReceivedAt`). `Connection` only goes up over the hub's life and changes on every reconnect, including after `Connect`, so a listener sees every gap however short it was. A new listener's first update is the current meet state with its original `Connection` and `ReceivedAt`. `hub.Status()` also reports `LastMeetStateAt`, the receive time of the last meet state.

Without a `Hub`, merge the `Client`'s messages into a `Cache` yourself:

```go
client := liftingcast.NewClient(cfg)
client.Start() // keeps retrying until Close
defer client.Close()
cache := liftingcast.NewCache()
for msg := range client.Messages() { // closed by Close
	state, err := cache.Merge(msg.Raw)
	if err != nil {
		continue
	}
	// state is the full merged meet state, yours to modify; msg.Connection
	// is 1 on the first connection and one higher after each reconnect
}
```

`client.Status()` reports whether the connection is up right now, whether LiftingCast has rejected it, and the last error it saw. A rejected client never reconnects (bad API key, meet ID or password); `Messages()` stays open until `Close`, and you replace the client to try again. Any other server error is retried.

The library logs nothing unless given a logger: `liftingcast.NewHub(liftingcast.WithLogger(slog.Default()))`. A `Hub` passes its logger on to its `Client`.

A `Client` waits 2s before its first reconnect attempt, doubling each time up to 5s. `WithMaxBackoff` changes that cap, for a `Client` or for the `Client` a `Hub` starts: `liftingcast.NewHub(liftingcast.WithMaxBackoff(30 * time.Second))`. On Hosted LiftingCast, two connections sharing an API key push each other off; a higher cap slows how often that happens.

The API is unstable until v1.

## Recording and replay

`WithRecorder` writes a recording of each upstream connection: everything LiftingCast sent on it, and how each connection began and ended (dials, handshakes, server errors, closes, timeouts, backoffs, rejections). It works on `NewHub` and `NewClient`. The option is a factory, called once for each `Client` created, so a new meet or new credentials gets a new recording:

```go
hub := liftingcast.NewHub(liftingcast.WithRecorder(
	func(info liftingcast.ClientInfo) (io.WriteCloser, error) {
		name := fmt.Sprintf("rec-%s-%d.jsonl", info.MeetID, info.Sequence)
		return os.Create(name)
	},
))
```

Recordings are JSONL, one event per line, and never contain credentials or the connection URL. Pings and pongs are left out. A recording ends with `closed`, or `replaced` if the Hub replaced the connection; the next recording's `start` names the one it replaced. The library closes the writer after the last event and does not rotate, compress or keep files, so wrap the writer yourself for that. Pass an unbuffered writer such as an `*os.File`, or flush your own buffering after it is closed.

Recording never affects the live feed. Events go onto a buffered queue and are written by a separate goroutine, so a slow or failing writer drops events instead of stalling the connection. The recording then holds a `recorder_dropped` event with a count. A factory error is logged and that connection runs without a recording. A full-day meet can reach hundreds of MB, because every message carries every lifter.

A `Replayer` feeds a recording back through a `Cache`, giving the same meet states, with the same connection numbers and receive times, that listeners were given at the time:

```go
f, err := os.Open("rec-abc123-1.jsonl")
if err != nil {
	log.Fatal(err)
}
defer f.Close()

for u, err := range liftingcast.NewReplayer(f).All() {
	if err != nil {
		log.Fatal(err)
	}
	// u.State, u.Connection and u.ReceivedAt are what a listener saw
}
```

`Next` does the same one state at a time and returns `io.EOF` at the end. A last line cut short by a crash is skipped, and a recording with no final event is not an error. Any other malformed line is an error that names the line. Only meet states are replayed; the other events stay in the file for reading. The recordings in `testdata/` replay too, so a recording from a real incident can become a regression test.

A `ReplaySource` plays a recording back to listeners at the pace it was recorded, for running consumers such as overlays against a past meet. `Speed` 1 is real time, 2 is twice as fast, and 0 is as fast as possible. `Loop` starts the recording again, from an empty meet state, when it ends:

```go
f, err := os.Open("rec-abc123-1.jsonl")
if err != nil {
	log.Fatal(err)
}
defer f.Close()

src := liftingcast.NewReplaySource(f, liftingcast.ReplayOptions{Speed: 1, Loop: true})
defer src.Close()

src.Listen(func(u liftingcast.Update) {
	// the same Updates a Hub's listener gets, paced by the recorded receive times
})
```

The replay starts at the first `Listen`. Its `Status` is always connected, for the meet `"replay"`, because a recording holds meet states and no meet ID. `*Hub` and `*ReplaySource` both satisfy `liftingcast.Source` (`Listen` and `Status`), so a consumer that accepts a `Source` works live and from a recording.

## Releasing

Label the PR before merging it: `release:minor` (v0.3.0 → v0.4.0) or `release:patch` (v0.3.0 → v0.3.1). Once CI passes on `main`, it puts the next annotated `vX.Y.Z` tag on the merge commit. A PR with neither label releases nothing; a PR with both fails the release job. To see the version a label would give, without tagging anything:

```sh
git tag -l | go run ./internal/nextversion release:minor
```
