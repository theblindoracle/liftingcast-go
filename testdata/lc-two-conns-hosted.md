# lc-two-conns-A-hosted.jsonl, lc-two-conns-B-hosted.jsonl

Two connections to Hosted LiftingCast (`wss://liftingcast.com/websocket`) with the same meet ID (`mbho66s0ddh9`), password and
API key, recorded at once on 2026-10-01 with `cmd/lc-tap`. A started first; B
started 10s later. Each redialled up to three times, 2s after each drop, and
both were stopped 60s after B started. Use the receive times to line the two
files up.

The newest connection always wins. When B connected, A received

`Another websocket connection was established. This connection is closing. You are only allowed to open one connection at a time.`

and was dropped without a close frame within a millisecond. Each side then
pushed the other off as it redialled, every ~2s, for as long as both had
reconnects left. Every new connection received the full meet state before
being pushed off. B, which redialled last, stayed connected.

The limit is per API key, not per meet: see `lc-two-meets-hosted.md`.

Lifter names, member numbers, states, countries and meet names are replaced with fake ones, the same way as `lc-traffic-scenarios-2026-10-01`; IDs and structure are as recorded.
