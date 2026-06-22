# Phase3 True1min Iteration Tree 2026-06-23

Decision: `ITERATION_TREE_INDEX_READY`

This document is the external-facing index for the true1min research chain. It
explains why the project moved from field/primitive safety audits to guarded
search, why proxy-driven search was rejected, and why Phase3CM train portfolio
Sortino is now the next reward target.

## Current Boundary

```text
Official X0/R3:
  read-only
  not modified
  no production promotion from this repository

Data backbone:
  true trade_time 1min shards only
  no old 1D kline backbone for true1min claims

Search status:
  no accepted alpha proof
  no deployable candidate
  next large search blocked until Phase3CM reward wiring is complete
```

## Iteration Tree

```text
Phase3CD
  AST primitive / field-assumption audit
  -> found unsafe limit/event motif use cases
  -> produced typed primitive registry/spec and rewrite queue

Phase3CE
  unsafe motif quarantine audit
  -> official registry scan showed no unsafe motif hit in scanned baseline roots
  -> found multiple unsafe entry points outside a single Phase3R path

Phase3CE1
  typed gate and search-memory blocked view
  -> unsafe structures are blocked/reclassified instead of deleted
  -> G2 input gate smoke proves candidate-level enforcement path

Phase3CE2
  typed primitive and fullwidth true1min canary
  -> validates evaluator availability and real-panel field readiness
  -> still diagnostic; not alpha proof

Phase3CF
  reward-gated large-search prelaunch
  -> freezes true1min-only backbone and guarded launch constraints
  -> later superseded in reward target by Phase3CM

Phase3CG
  true1min fastpath preparation
  -> packages fastpath run assumptions and acceleration constraints
  -> keeps old 1D paths out of the chain

Phase3CL
  proxy top queue fragment replay audit
  -> 32/32 replayed candidates failed
  -> proxy IC / CEM winners were not tradable reward proof
  -> fragment replay exposed failures but was also judged unsuitable as primary reward

Phase3CM
  train portfolio Sortino reward chain
  -> demotes fragment_sortino to diagnostic-only
  -> introduces train / validation / holdout portfolio reward audit
  -> next search must optimize train_reward, not proxy IC or fragment slices
```

## Evidence Map

| Stage | Question Answered | Primary Evidence |
|---|---|---|
| CD | Are AST primitives safe for new field types? | `reports/phase3cd_ast_primitive_assumption_audit_20260618/PHASE3CD_AST_PRIMITIVE_ASSUMPTION_AUDIT_20260618.md` |
| CE | Did unsafe motif signatures touch official roots or runtime pools? | `reports/phase3ce_unsafe_motif_quarantine_audit_20260618/PHASE3CE_UNSAFE_MOTIF_QUARANTINE_AUDIT_20260618.md` |
| CE1 | Are unsafe structures blocked rather than forgotten? | `reports/PHASE3CE1_TYPED_PRIMITIVE_GATE_IMPLEMENTATION_SUMMARY_20260618.md` |
| CE1 memory | Is search memory kept as blocked bookkeeping? | `reports/phase3ce1_search_memory_blocked_view_20260618/PHASE3CE1_SEARCH_MEMORY_BLOCKED_VIEW_20260618.md` |
| CE2 | Can typed primitives run on actual/validation true1min panels? | `reports/phase3ce2_typed_primitive_candidate_pack_canary_20260618/PHASE3CE2_TYPED_PRIMITIVE_CANARY_20260618.md` |
| CF | What was the guarded large-search prelaunch contract? | `reports/phase3cf_reward_gated_large_search_prelaunch_20260618/PHASE3CF_REWARD_GATED_LARGE_SEARCH_PRELAUNCH_20260618.md` |
| CG | What fastpath assumptions were packaged? | `reports/PHASE3CG_TRUE1MIN_FASTPATH_PREP_20260619.md` |
| CL | Did proxy/CEM winners survive fragment replay? | `reports/PHASE3CL_TRUE1MIN_FRAGMENT_REPLAY_AUDIT_20260622.md` |
| CM | What replaces fragment Sortino as the reward target? | `reports/PHASE3CM_TRAIN_SORTINO_REWARD_CHAIN_20260623.md` |

## Reward Evolution

```text
Old proxy stage:
  aligned IC / spread / hit / proxy quality
  useful for cheap triage
  failed as reward

Phase3CL fragment stage:
  sampled trade_time fragment Sortino
  useful as diagnostic slice replay
  failed as primary reward target because local slices can be overfit

Phase3CM target:
  train-set portfolio PnL curve
  turnover-adjusted cost
  horizon-sleeve aggregation
  train / validation / holdout split
  holdout forbidden as search feedback
```

## Current Route Contract

```text
phase3ca-build-bz-candidate-audit:
  dedupe and hard-reject proxy outputs
  not proof

phase3cm-train-portfolio-sortino-reward-audit:
  primary reward audit target for next search
  no search launch by itself

phase3bz-fragment-replay-audit:
  optional diagnostic replay
  not primary reward
```

## What Was Rejected

```text
Rejected:
  proxy-only candidate language
  CEM feedback on proxy IC as proof
  fragment_sortino as primary reward
  old 1D data for true1min claims
  deleting unsafe memory keys
  promoting X0/R3 changes from this chain

Retained:
  true1min shard backbone
  typed primitive gates
  blocked search memory view
  CA hard-reject bridge
  CM train portfolio reward audit
```

## Next Gate

Before any large search restart:

```text
1. Wire BS/BT/CEM/UCB search feedback to Phase3CM train_reward.
2. Use validation metrics only for arm allocation and kill-line checks.
3. Keep holdout metrics read-only and excluded from optimizer feedback.
4. Keep BZ fragment replay as diagnostic-only.
5. Preserve fresh-search budget so CEM cannot collapse the search space.
```

Success for the next stage is not a high proxy score. It is a candidate family
that survives train reward, validation checks, holdout reporting, wrong-lag
guards, turnover costs, and search-memory crowding controls.
