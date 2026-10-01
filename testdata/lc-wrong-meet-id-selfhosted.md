# lc-wrong-meet-id-selfhosted.jsonl

Connecting to Self-hosted LiftingCast (`ws://localhost/websocket`, the Docker image) with a meet ID that doesn't exist (`nosuchmeet00`), any
password, and a valid API key. Recorded on 2026-10-01 with `cmd/lc-tap`, which
dialled three times, 2s apart.

On every connection:

1. The handshake succeeds (101). LiftingCast doesn't refuse it.
2. LiftingCast sends one text message that isn't JSON:
   `Error: unauthorized - meet id or password in auth param are incorrect`
3. A few milliseconds later it drops the connection without a close frame
   (`read_error`, 1006 abnormal closure).

Reconnecting gets exactly the same result each time.
