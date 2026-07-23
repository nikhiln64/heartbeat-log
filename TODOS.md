# TODOS

## Demo GIF/asciinema of the red/green run

- **What:** Record `./gradlew redTest` failing (committed telemetry lost) then the green twin passing on the same seed; embed in README.
- **Why:** Reviewers who never clone the repo still see the money experiment in 10 seconds on the repo page.
- **Pros:** The artifact sells itself; zero risk to the build.
- **Cons:** ~30min; cosmetic; goes stale if trace output format changes.
- **Context:** Parked as post-H14 polish during /plan-eng-review (2026-07-23). The red/green pair and its printed seed are the core deliverable; this is marketing for it. Do after submission-ready, before the panel if time allows.
- **Depends on / blocked by:** Red/green pair implemented (build-order H8-10).

## KIP-279 follow-up module

- **What:** Extend the epoch-lookup truncation to the two-round KIP-279 protocol covering the divergence-after-unclean-election case KIP-101 missed; ideally with a failing-test demo of the anomaly first.
- **Why:** It is the prepared talking point in panel prep; the code version is the strongest possible depth signal for a second-round conversation.
- **Pros:** Upgrades "I can talk about KIP-279" to "I built it."
- **Cons:** Real scope (~3-4h); strictly post-submission - attempting it pre-deadline threatens the weekend budget.
- **Context:** Captured during /plan-eng-review (2026-07-23). The design doc preps KIP-279 verbally; this TODO is the optional code follow-up. Start from the KIP-279 wiki page's two-round OffsetsForLeaderEpoch description.
- **Depends on / blocked by:** Full `EPOCH_BOUNDARY` implementation shipped and submitted.
