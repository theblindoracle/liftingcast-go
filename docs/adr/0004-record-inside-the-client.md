---
status: accepted
---

# Record each upstream connection from inside the Client, one file each

To find out later what went wrong in a live meet, we need a recording of exactly what LiftingCast sent and what the client did about it. We record from inside the Client, in the read loop, with `WithRecorder`, an option on both `NewClient` and the Hub. The option is a factory, `func(ClientInfo) (io.WriteCloser, error)`, which the library calls each time it creates a Client. The caller chooses the file, and the library writes the events and closes the file once the Client has ended. `ClientInfo` holds the meet ID, a sequence number and the start time, never credentials. Each upstream connection gets its own recording, so replacing one starts a new file.

A recording is JSONL in the shape of `cmd/lc-tap` and `testdata/`, so existing recordings replay the same way. It adds `start` as the first line and `closed` or `replaced` as the last, and `dial`, `connected`, `timeout`, `backoff` and `rejected` for what the client decided. It records the messages, server errors, handshake results, close frames and read errors, but not our pings or LiftingCast's pongs. It never records the URL or credentials, and a test checks this.

The live feed wins over the recording. Events go onto a channel of 64 and a separate goroutine writes each one as soon as it is received. If the channel fills or a write fails, the client keeps running and the recording gets a `recorder_dropped` event with a count, so a replay knows it is incomplete. A factory error means that Client runs without a recording and logs a warning.

## Considered options

- **Keep `lc-tap` as the recorder.** Rejected: it dials its own connection, and Hosted LiftingCast allows one connection per API key, so it can't run beside a live Client. It also can't capture a real run.
- **Wrap `Messages()`.** Rejected: it never sees server errors, handshake refusals, close frames or read errors, nor our own timeouts and backoff, which are what explain a problem.
- **One writer shared by every Client.** Rejected: connection numbers restart at 1 for each Client, so every line would need a client ID, and a process restart would append unrelated runs to one file. One file per upstream connection avoids both.
- **Write synchronously in the read loop.** Rejected: a slow disk could stall the connection past the 60s message timeout and drop it.
- **The caller closes the writer.** Rejected: the Hub creates the Clients, so the caller doesn't know when each one ends.

## Consequences

- The line format is a compatibility surface: old recordings must keep replaying, so changes to it should only add fields or events.
- A crash can lose the events in the channel and leave a truncated last line. A recording with no `closed` or `replaced` line ended abnormally, and the replayer tolerates this.
- Recordings can reach hundreds of MB for a full-day meet, because every message carries every lifter (ADR-0002). Compression and retention are the caller's job.
- Replay reproduces each meet state's connection number and receive time from the recording.
