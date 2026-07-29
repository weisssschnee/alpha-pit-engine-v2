# ADR 0009: A-share tradability evidence boundary

Status: Accepted as a scope correction on the existing CN development
evaluation and feedback chain. No new evaluator or tradability authority is
created by this decision.

## Context

Phase3CM proves deterministic, train-only, full-coordinate predictive and
portfolio-ranking behavior under a long-only top portfolio, fixed 5 bps cost
and 1/5/15/30 row horizons. It does not prove an executable A-share replay:

- T+1 inventory age is not enforced;
- same-bar execution is not excluded by an execution clock;
- limit-lock and suspension fills are not enforced;
- the complete asymmetric fee schedule is not enforced; and
- the evaluation universe is not yet promotion-grade against listing,
  delisting and survivorship effects.

The existing authority graph nevertheless labelled the candidate-parallel
Phase3CM path as `formal_evaluation_authority`. Pair-native feedback also
allowed a matched increment to become READY while treating the primary
standalone decision as diagnostic. The Hybrid runner could then complete an
Optuna trial from any evaluated pair using
`min(primary_composite_reward, matched_train_increment)`.

That chain overstated predictive development evidence as executable economic
evidence.

## Decision

The existing Phase3CM node is narrowed in place to
`development_predictive_evaluation_authority`. Its output must explicitly
declare `DEVELOPMENT_PREDICTIVE_SCORE_ONLY`,
`A_SHARE_TRADABILITY_UNPROVEN`, and false proof flags for execution clock,
same-bar exclusion, T+1, limit-lock, suspension, complete fees and
promotion-grade universe handling.

No optimizer reward, finalist eligibility, promotion eligibility or economic
claim may cross the existing feedback boundary unless all of the following are
present on both members of the matched pair:

1. an explicit `A_SHARE_TRADABILITY_REPLAY_V1` evidence class;
2. `A_SHARE_TRADABILITY_READY`;
3. every required execution proof flag set true;
4. a completed pair-native READY decision; and
5. a primary standalone `TRAIN_REWARD_FOLLOWUP_READY` decision.

Missing evidence fails closed. A pending optimizer trial may be terminally
closed as failed for lifecycle integrity, but it cannot receive a COMPLETE
reward observation and cannot teach TPE.

The existing `real_market_validation` and T+1 replay implementations remain
engineering capabilities only. This ADR does not promote either one to
authority. A later, separately reviewed integration must bind immutable replay
receipts before it may emit `A_SHARE_TRADABILITY_READY`.

## Consequences

- Historical Phase3CM search artifacts remain immutable development evidence.
  Their prior `productive` label does not become an economic or promotion
  claim. Development productive diagnostics remain available for behavior
  deduplication and train-stability ranking; only optimizer COMPLETE, finalist,
  promotion and economic-claim paths require the replay proof above.
- ADR 0008 continues to select Hybrid TPE plus Availability only inside a
  separately authorized development search. It no longer implies that raw
  Phase3CM scores may feed optimizer state without the tradability proof above.
- No candidate search, replay, validation or sealed-period access is authorized
  by this repair.
- The next implementation step, if separately authorized, is to connect the
  existing replay capabilities to the guard with immutable evidence receipts;
  it is not to create a new platform or authority node.
