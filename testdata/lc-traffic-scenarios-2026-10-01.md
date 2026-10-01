# lc-traffic-scenarios-2026-10-01.jsonl

LiftingCast WebSocket traffic for one meet with one platform, recorded with
liftingcast-clipper's `prototype/lc-tap` while the scenarios below were run in
the LiftingCast UI. One line per message: receive time, size, raw message.
Lifter names, member numbers, states, countries and the meet name are replaced
with fake ones; IDs and structure are as recorded.

| Time (UTC)  | Messages | Scenario |
|-------------|----------|----------|
| 16:59:44    | 0        | Initial state on connect |
| 17:01–17:02 | 1–10     | Add division Men's Masters (`dzczfssal7y5`) with weight classes 93 and 93+. Not sent: it has no lifters yet |
| 17:02:47    | 11–12    | Move Lifter 55 (`lx122y85a58z`) into Men's Masters 93. `lifters` names the new division; no `divisions` is sent |
| 17:03:58    | 13       | Delete weight class 93+. First `divisions` that includes Men's Masters |
| 17:04:12    | 14–16    | Add weight class 105 (`wak75d8hq371`); its fields arrive as they are filled in |
| 17:04:44    | 17–18    | Clear 105's max weight (sent as `""`), then set it again |
| 17:05:11    | 19       | Delete weight class 105 |
| 17:05:29    | 20–21    | Move Lifter 55 back to Men's Raw Open 93 |
| 17:06:04    | 22–32    | Delete Lifter 53 (`lvoirlq9n8to`): platform updates, then `lifters` without them, then a meet-details-only message |
| 17:06:44    | 33       | Delete division Men's Masters |
| 17:07:15    | 34       | Change Lifter 09's deadlift 1 from 122.5 to 125 |
| 17:07:39    | 35–44    | One lift: clock start, head good, left red card then bad, right good, next lifter, lights reset |
