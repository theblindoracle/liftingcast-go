---
status: accepted
---

# Only errors known to be permanent stop the client reconnecting

LiftingCast's reference client (`liftingcast/liftingcast-overlays`, `src/lib/useMeetData.ts`) treats every message that isn't meet state as notice of a coming disconnect: it closes the connection and never reconnects. We deliberately do not. An upstream connection is rejected only by a server error whose message is on a list of errors known to be permanent (bad credentials, bad meet ID), or by a handshake LiftingCast refuses as unauthorized or forbidden (HTTP 401/403). Other handshake failures stay temporary. The client then closes the connection itself, stops dialling, and reports `Rejected` in its connection status; it stays rejected until the caller replaces it. Any other server error is temporary: it sets the last error, and the connection stays connected or reconnects as usual. During a live meet, an overlay that stops for good over a transient or unknown error is worse than one that keeps retrying, so unknown errors default to retrying.

## Considered options

- **Treat every server error as permanent, like the reference client.** Rejected: one unexpected message would stop the overlay mid-meet until someone restarts it.
- **List the temporary errors and treat unknown ones as permanent.** Rejected for the same reason. If LiftingCast rewords a message, the client should fall back to retrying, not to stopping.

## Consequences

- The list of permanent messages comes from the `lc-wrong-*` recordings in `testdata/` (#9): `Error: invalid api key` and `Error: unauthorized - meet id or password in auth param are incorrect`, the same on both servers. In every case LiftingCast accepted the handshake, sent the message, and dropped the connection without a close frame within milliseconds. No handshake was refused, so the 401/403 rule is only a safeguard. The takeover message, `Another websocket connection was established. …` (`lc-two-conns-*`), stays temporary. The list matches LiftingCast's wording. If that wording changes, the client quietly falls back to reconnecting with bad credentials, and the last error still shows the message.
- Hosted LiftingCast allows one connection at a time, so two clients sharing it keep pushing each other off; we accept that rather than let either stop for good. To slow the fight down, the reconnect backoff resets only after a connection delivers meet state *and* doesn't end with a server error. Being accepted isn't enough, and neither is the initial meet state alone, since every new connection receives it. Two clients fighting settle at the backoff cap (5s by default, set with `WithMaxBackoff`).
- Hosted LiftingCast's limit is per API key, even across meets (`lc-two-meets-*`). Two of our own tools sharing a key, say the clipper and the overlays, will fight the same way, and no reconnect policy can fix that. It has to be fixed in how they're deployed (separate keys, or one process holding the upstream connection), not here.
- A server error stays the last error through the disconnect that follows it, so the status shows why the connection dropped, not just that it closed.
- A rejected client keeps `Messages()` open until `Close`, so a closed channel still means only a deliberate close.
- If the recordings show bad credentials are neither refused at the handshake nor answered with a recognisable server error, amend this ADR rather than dropping the work.
