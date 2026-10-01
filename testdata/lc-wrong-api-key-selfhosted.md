# lc-wrong-api-key-selfhosted.jsonl

Connecting to Self-hosted LiftingCast (`ws://localhost/websocket`, the Docker image) with the right meet ID (`mbho66s0ddh9`) and password but an
API key of all zeros. Recorded on 2026-10-01 with `cmd/lc-tap`.

Self-hosted LiftingCast ignores the API key: the connection is accepted, the
full meet state arrives, and the connection stays up until it was stopped 45s
later. A wrong API key can't be detected against Self-hosted LiftingCast.

Lifter names, member numbers, states, countries and meet names are replaced with fake ones, the same way as `lc-traffic-scenarios-2026-10-01`; IDs and structure are as recorded.
