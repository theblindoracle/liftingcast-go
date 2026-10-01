# lc-wrong-api-key-hosted.jsonl

Connecting to Hosted LiftingCast (`wss://liftingcast.com/websocket`) with the right meet ID (`mbho66s0ddh9`) and password but an
API key of all zeros. Recorded on 2026-10-01 with `cmd/lc-tap`, which dialled
three times, 2s apart.

On every connection the handshake succeeds (101), LiftingCast sends
`Error: invalid api key`, and drops the connection without a close frame
straight away. The API key is checked before the meet ID and password.
