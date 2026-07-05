# Phase3EO True1min Acceleration And Event Semantics Report

created_at: 2026-07-05
status: DIAGNOSTIC_READY

## Scope

Phase3EO fixed two blockers in the true1min search loop:

1. Candidate-axis CM parallelism caused duplicate shard reads and duplicated per-worker caches.
2. Sparse event/context fields were admitted by schema availability only, which produced invalid or zero-row candidates.

This report is diagnostic evidence only. It is not alpha promotion proof.

## Acceleration Change

CM evaluation now supports shard-axis parallelism:

```text
parallel_axis: shard
duplicate_shard_reads_per_full_pass: 1
reward_atom_rows: written per shard and merged centrally
```

The old candidate-axis shape read the same true1min shards repeatedly across candidate workers. The new shard-axis shape assigns distinct shard subsets to workers, writes reward atoms, and merges candidate reward summaries centrally.

Observed 77O medium run:

```text
workers: 4
effective_cores_5s: ~4.05
python_memory: ~14.4GB
generated: 512
CA selected: 96
CM audited: 24
reward_atoms: 19,808
duplicate_shard_reads_per_full_pass: 1
```

## Event/Context Semantics Change

Sparse `evt_*` payload fields are no longer treated like dense state series.

Blocked from generation:

```text
StateDwell($evt_*)
EventAge($evt_uplimit_* payload)
SinceLastEvent($evt_uplimit_* payload)
evt_uplimit_type_code
MaskedCorr($ctx_billboard/*)
MaskedCorr($ctx_holder/*)
MaskedCorr($ctx_dividend/*)
MaskedCorr($ctx_share/*)
MaskedCorr($ctx_zls/*)
```

Allowed path for sparse event payloads:

```text
EventCount($evt_*, window)
WindowStateCount($evt_*, window)
event-aware sampled trade_time positions plus forward bars
```

Sparse context fields now use lower valid-ratio grids:

```text
billboard/holder/dividend/share/zls: 0.02, 0.05, 0.10
RZRQ: 0.40, 0.60
default dense context: 0.60, 0.80
```

## Coverage Evidence

Event payload coverage probe over 4 shards showed:

```text
evt_uplimit_type_code:
  non_zero: 0
  decision: blacklist

evt_uplimit_fd_close:
  non_zero_ratio: ~0.00031 .. 0.00042
  event_trade_times: ~5,635 .. 6,695
  decision: event-count/window-count only

evt_uplimit_auction_offer:
  non_zero_ratio: ~0.00009 .. 0.00016
  event_trade_times: ~1,732 .. 2,985
  decision: event-count/window-count only

ctx_zls_df_num:
  non_na_ratio: ~0.012
  nonzero_trade_times: 723 per shard
  decision: sparse context, no MaskedCorr
```

## Final 77O Postfix Smoke

Run:

```text
generation_budget: 256
CA selected: 48
CM audited: 24
CM shards: 2
CM parallel_axis: shard
CM event-aware sampling: true
rank_ic_loss_weight: 6.0
regime_stability_weight: 0.06
```

Final smoke result:

```text
followup_count: 2
reward_atom_rows_merged: 19,500
bad StateDwell(evt): 0
bad EventAge(evt payload): 0
bad SinceLastEvent(evt payload): 0
bad sparse MaskedCorr: 0
bad evt_uplimit_type_code usage: 0
```

Arm summary:

```text
event_state:
  best_reward: 1.169470
  followups: 1
  zero_candidates: 0 / 4

random_orthogonal:
  best_reward: 0.265599
  followups: 1
  zero_candidates: 2 / 4

rx_ucb_fresh:
  best_reward: 0.425145
  followups: 0
  zero_candidates: 0 / 4

typed_ast_fresh:
  best_reward: 0.308038
  followups: 0
  zero_candidates: 0 / 4
```

Top followups:

```text
phase3cp_00211:
  arm: event_state
  expression: Neg(CSRank(Mul(Sign(CSRank(WindowStateCount($evt_uplimit_up_limit_keep_times,10))),CSRank(MaskedCorr($ctx_sent_lb_3_num,$vwap,20,0.8)))))
  train_reward: 1.169470
  train_day_sortino: 1.367728
  train_worst_horizon_day_sortino: 0.505940
  train_day_mcmc_prob_gt_0: 0.998333
  validation_day_sortino: -0.332837
  holdout_day_sortino: 0.581978

phase3cp_00242:
  arm: random_orthogonal
  expression: CSRank(Sub(Abs(CSRank(WindowStateCount($evt_uplimit_auction_pre1max_ratio,5))),Abs(CSRank(MaskedZScore($ctx_hfq_pe_ttm,20,0.8)))))
  train_reward: 0.216872
  train_day_sortino: 0.274701
  train_worst_horizon_day_sortino: 0.145164
  train_day_mcmc_prob_gt_0: 0.958333
  validation_day_sortino: 0.304764
  holdout_day_sortino: 0.312250
```

## Remaining Issues

1. The first postfix smoke still had `random_orthogonal` zero-row candidates.
   This was not caused by unsafe event primitives. It was caused by weak event/context combinations that pass schema availability but fail practical cross-sectional sample requirements.

2. Phase3EO added a cheap pre-CM semantic viability gate after this finding:

```text
evaluate candidate on 1 shard / small event-aware sample
record rows_by_candidate
reject or deprioritize if rows == 0
feed zero-row family back into memory as blocked_weak_semantic_viability
```

3. Followups are train-reward followups only. Validation and holdout remain report-only and must not feed optimizer feedback.

## Pre-CM Semantic Gate Follow-up

The pre-CM semantic gate was integrated into `phase3cp_real_cm_small_loop.py`.

Placement:

```text
CA bridge
  -> field availability gate
  -> pre-CM semantic viability gate
  -> formal CM train reward audit
```

The gate uses the real Phase3CM evaluator, not a separate proxy evaluator:

```text
max_shards: 1
sample_trade_times_per_shard: 32
event_sample_trade_times_per_shard: 96
horizons: 1,5
decision input: rows_added only
reward usage: forbidden for this gate
```

77O validation after adding the gate:

```text
field_gate_input: 48
semantic_passed: 46
semantic_rejected: 2
final_cm_kept: 24
formal_cm_zero_candidates: 0
formal_cm_followup_count: 3
formal_cm_reward_atoms: 20,276
```

Rejected examples:

```text
phase3cp_00236:
  arm: random_orthogonal
  reason: semantic_total_rows_below_min|semantic_nonzero_shards_below_min
  expression: CSRank(Mul(Sign(CSRank(EventCount($evt_uplimit_fd_close,5))),Sign(CSRank(ValidRatioGate($ctx_rzrq_rzye,60,0.6)))))

phase3cp_00248:
  arm: random_orthogonal
  reason: semantic_total_rows_below_min|semantic_nonzero_shards_below_min
  expression: CSRank(Sub(Abs(CSRank(EventCount($evt_uplimit_fd_close,5))),Abs(CSRank(ValidRatioGate($ctx_billboard_deal_amount_ratio,60,0.02)))))
```

Formal CM after the gate:

```text
StateDwell($evt_*): 0
EventAge($evt_uplimit_* payload): 0
SinceLastEvent($evt_uplimit_* payload): 0
MaskedCorr($ctx_billboard/holder/dividend/share/zls): 0
zero-row candidates: 0
```

## Next Search Contract

Before a larger run:

```text
required:
  shard-axis CM
  event-aware sample positions
  sparse event payload primitive rules
  sparse context ratio rules
  train Sortino + rankIC loss reward
  low-weight regime stability component
  validation/holdout report-only

recommended:
  keep pre-CM semantic viability gate enabled
  use semantic-blocked candidate audit as memory input
  scale on 77O first, then local only if user allows
```
