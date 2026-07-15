# ADR 0006: Pair-native feedback authority

Status: Accepted for engineering qualification; formal search remains forbidden.

## Context

Phase3CM evaluates a primary candidate and its route-specific matched control as
one indivisible pair. Phase3CN nevertheless inherited standalone-primary reward
decisions and blockers, so a primary-level diagnostic could incorrectly govern
pair feedback.

## Decision

`build_pair_evaluation_rows()` is the authority for the pair-native fields
`pair_train_reward`, `pair_train_reward_decision`,
`pair_train_reward_blockers`, `pair_turnover_metric`,
`pair_support_metric`, and `pair_rank_ic_metric`.

Phase3CN consumes only those pair-native fields. Standalone-primary decisions,
blockers, and turnover remain explicitly renamed diagnostics and have no
feedback authority. A matched control has no independent vote, quota, survivor,
or memory right.

Frozen Broad Event runtime replay uses independently materialized primary and
matched-control channels under one registered logical mechanism handle. Missing
physical replay channels fail closed; the generic evaluator never reconstructs
one channel from the other.

## Consequences

- Nonpositive pair increment, support mismatch, missing evaluator invocation,
  equivalent behavior, missing receipt, or any pair blocker prevents feedback.
- Synthetic runtime qualification can prove wiring and fail-closed behavior but
  cannot authorize search, promotion, validation/holdout feedback, 2026 access,
  or cross-sprint memory.
- Registry, split authority, candidate-parallel architecture, reward weights,
  and frozen Broad Event research results are unchanged.
