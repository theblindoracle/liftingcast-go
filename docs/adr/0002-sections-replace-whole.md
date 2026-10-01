# Each section a message carries replaces the cached copy whole

LiftingCast sends meet state as messages that carry one or more sections (meet details, `lifters`, `platforms`, `divisions`, `teams`), and a section it leaves out is unchanged. The cache sets each section a message carries to that message's copy and keeps the sections it leaves out, the same as LiftingCast's own reference client (`liftingcast/liftingcast-overlays`, `src/lib/useMeetData.ts`: `{ ...prevData, ...data }`). We are dropping the deep merge, along with its special cases for `ifSuccessfulScores`/`ifSuccessfulPlaces` and empty maps. Deep merging adds and updates entries but never removes one, so it kept deleted lifters, divisions and weight classes until the next reconnect. It could also mix keys from two different values, such as referee cards `{"red": true}` then `{"blue": true}`.

The evidence is two recordings in `testdata/`: one from 2026-09-29 (34 messages, two platforms, lifting) and one from 2026-10-01 (45 messages, scripted adds, deletes and moves; see its `.md`). In all 79 messages, every section that was present was complete. Replaying the 2026-10-01 recording through the deep-merge cache left the deleted lifter and the deleted division in the meet state.

## Considered options

- **Per-key rules** (replace `lifters` whole, deep-merge the rest; the original proposal in #4). Rejected: every section turned out to be complete, so a per-key list would only add rules to keep up to date and places for stale data to survive.
- **Deep merge plus explicit deletion handling.** Rejected: LiftingCast doesn't send deletions. The only way to see one is to compare against a complete section, which is what replacing the section does.

## Consequences

- If LiftingCast ever sends a partial section, the fields it leaves out are lost until the next complete copy arrives. That's about a second for `platforms`, which comes in every message, but could be minutes for `lifters`, which only comes when a lifter or attempt changes. The reference client has the same exposure.
- This reverses part of #3's guarantee. Fields a message leaves out still survive at the section level, but no longer inside a section.
- Entries can disappear between two meet states, so consumers can't assume an ID they've seen will still be there.
- LiftingCast's `divisions` section only includes divisions that have lifters, and it's only sent when a division is edited. When a lifter moves into a division that hasn't been sent, the meet state shows the lifter in an unknown division until the next division edit. No merge rule can fix that; consumers have to tolerate it.
