# lc-two-meets-A-hosted.jsonl, lc-two-meets-B-hosted.jsonl

Two connections to Hosted LiftingCast (`wss://liftingcast.com/websocket`) with
the same API key but **different meets**: A on `mbho66s0ddh9`, B on
`mclz8pc3tu2p`, started 10s later. Recorded on 2026-10-01 with `cmd/lc-tap`,
with no reconnects; both were stopped 20s after B started.

When B connected, A received the same takeover message as in
`lc-two-conns-hosted.md` and was dropped without a close frame. So Hosted
LiftingCast's one-connection limit is per API key, not per meet.

Lifter names, member numbers, states, countries and meet names are replaced with fake ones, the same way as `lc-traffic-scenarios-2026-10-01`; IDs and structure are as recorded.
