# lc-wrong-password-hosted.jsonl

Connecting to Hosted LiftingCast (`wss://liftingcast.com/websocket`) with a real meet ID (`mbho66s0ddh9`),
a wrong password, and a valid API key. Recorded on 2026-10-01 with
`cmd/lc-tap`, which dialled three times, 2s apart.

The same as a wrong meet ID: on every connection the handshake succeeds (101),
LiftingCast sends
`Error: unauthorized - meet id or password in auth param are incorrect`, and
a few milliseconds later drops the connection without a close frame. The
message doesn't say which of the two was wrong.
