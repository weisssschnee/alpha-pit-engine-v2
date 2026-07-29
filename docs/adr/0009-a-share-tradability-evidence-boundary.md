# ADR 0009: A-share tradability evidence boundary

Status: Superseded in part by ADR 0010. Its executable replay contract and
economic-claim boundary remain accepted; its requirement that every
development optimizer observation carry executable replay proof is revoked.

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
promotion-grade universe handling. Finalist receipts additionally require
corporate-action cash/share and terminal-liquidation proof.

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
schedule, execution policy and corporate-action policy before it may emit a canonical
`A_SHARE_TRADABILITY_REPLAY_V1` receipt.

The executable kernel must model a cash-and-holdings long-only portfolio with
signals observed at close and orders attempted at the next session open. It
must sell before buying, enforce T+1 inventory age, block opening limit-up
buys and opening limit-down sells, carry blocked holdings, block suspended
fills, round to board lots, and charge the frozen complete fee schedule. The
fee schedule must cover every replay date and must bind the account-specific
commission rate and minimum commission instead of relying on a repository
default. Explicit PIT-aligned cash/share actions apply only to opening
holdings before the session rebalance. Delisting exits require an explicit
terminal session and liquidation price, charge ordinary sell-side fees and
cannot silently disappear from the panel. Replay-end liquidation must fail
closed unless the final book is flat.

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

The original integration was statically accepted at commit
`a33d8fccdb79372ffac54ce55302be362911df19`. Existing-authority kernel and
receipt hardening completed at
`3f32f5518f187a4f421208113179ca9d98fc97d3`, but runtime qualification remains
fail-closed. The 77o zero-financial binding proves high/low and ST are present.
Security type, exchange and conservative limit-price derivations exist but are
not materialized. PIT universe eligibility, listing age, delisting, suspension,
corporate-action cash/share and terminal session/price are absent. The project
also lacks the frozen actual-account fee contract and promotion-grade
survivorship-free, delisting-inclusive universe manifest. Therefore no 64-pair
financial qualification is authorized from the current inputs.

## Consequences

- Historical Phase3CM search artifacts remain immutable development evidence.
  Their prior `productive` label does not become an economic or promotion
  claim. Development productive diagnostics remain available for behavior
  deduplication and train-stability ranking. Under ADR 0010, development
  optimizer observations no longer require the replay proof above; finalist,
  promotion and economic-claim paths still do.
- ADR 0008 continues to select Hybrid TPE plus Availability only inside a
  separately authorized development search. ADR 0010 owns the train-only
  development feedback boundary and prevents those observations from becoming
  execution or economic evidence.
- No candidate search, replay, validation or sealed-period access is authorized
  by this repair.
- The implementation step that connects the existing replay path to the guard
  is complete and zero-financial tested. The remaining blocker is an input
  authority gap, not an invitation to run another experiment.
- Before any finalist execution or economic qualification, materialize the
  missing PIT session tradability/universe fields from authoritative sources,
  freeze the account commission/minimum contract and bind a
  survivorship-free, delisting-inclusive universe manifest. This finalist-lane
  work is not a prerequisite for development optimizer feedback.
- Corporate-action share/cash and terminal-liquidation semantics are now
  implemented and zero-financial tested in the existing replay, but their PIT
  input fields are not yet bound. Receipts therefore continue to set economic
  claim and candidate-promotion authorization false.
