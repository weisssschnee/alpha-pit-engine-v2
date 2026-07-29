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

The existing `phase3dy_true1min_tplus1_tradable_replay` path is the execution
adapter for this boundary; it is not a second evaluator or a new authority
node. In authority mode it must bind and hash the exact input panel bytes,
split manifest, promotion-grade universe manifest, explicit historical fee
schedule and execution policy before it may emit a canonical
`A_SHARE_TRADABILITY_REPLAY_V1` receipt.

The executable kernel must model a cash-and-holdings long-only portfolio with
signals observed at close and orders attempted at the next session open. It
must sell before buying, enforce T+1 inventory age, block opening limit-up
buys and opening limit-down sells, carry blocked holdings, block suspended
fills, round to board lots, and charge the frozen complete fee schedule. The
fee schedule must cover every replay date and must bind the account-specific
commission rate and minimum commission instead of relying on a repository
default.

Phase3CM predictive values remain available as diagnostics. The optimizer
reward authority is instead:

```text
search_score =
min(primary executable net daily Sortino,
    primary-minus-control executable net reward)
```

Both members' canonical receipt payloads, hashes, candidate IDs and exact
identities are revalidated at every pair, feedback, search and adaptive
consumer. A copied READY label, an outer-row projection or a Phase3CM score
cannot substitute for either receipt.

This integration is statically accepted at implementation commit
`a33d8fccdb79372ffac54ce55302be362911df19`, but runtime qualification remains
fail-closed. The currently bound 77o train sidecars do not contain the full
execution/universe fields required by the contract (including high/low,
security type, exchange, PIT universe eligibility, listing age, ST/delisting,
suspension and limit prices), and the project has no frozen account commission
contract or promotion-grade survivorship-free, delisting-inclusive universe
manifest. Therefore no 64-pair financial qualification is authorized from the
current inputs.

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
- The implementation step that connects the existing replay path to the guard
  is complete and zero-financial tested. The remaining blocker is an input
  authority gap, not an invitation to run another experiment.
- The next allowed work is narrow data-authority repair: materialize the
  missing PIT session tradability/universe fields from authoritative sources,
  freeze the account commission/minimum contract and bind a
  survivorship-free, delisting-inclusive universe manifest. Only then may a
  separately frozen 64-pair train-only qualification be considered.
- Corporate-action share/cash adjustments and terminal liquidation semantics
  are not yet promotion-qualified. Receipts therefore continue to set
  economic-claim and candidate-promotion authorization false.
