# Phase3EL True1min Best Level 2026-07-04

Decision: `TRUE1MIN_BEST_LEVEL_RESTATED`

## Bottom Line

Current strict true1min best is not an accepted alpha.

```text
CN long-only exact-hash Stage1:
  PASS / FOLLOWUP_READY: 0
  best decision: HOLD_TRAIN_REWARD

Research-level train-only:
  one clean-looking event/context candidate exists
  but OOS was intentionally blanked before next-pack generation
  and it has not been exact-hash rechecked under the current long-only policy
```

So the honest answer is:

```text
tradable / deployable 1min best: none
research best: phase3dv_00353, train-only, promising but unverified
legacy diagnostic best: phase3cp_00030, strong old diagnostic numbers, not current-policy proof
```

## Level 1: Strict Current CN Long-Only Best

Source:

```text
reports/phase3ei_historical_ban_short_recheck_77o_20260704/stage1_deeper_train_reward/phase3cm_train_reward.csv
```

Best Stage1 by train reward:

```text
candidate_id: phase3cp_17360
expression_hash: 76a331c865ca6bb8314b891e
decision: HOLD_TRAIN_REWARD
portfolio_mode: long_only_top
short_allowed: false

train_reward: 0.22610725
train_day_sortino: 0.02897237
validation_day_sortino: -0.09990719
holdout_day_sortino: 0.01263062
blocker: non_positive_worst_horizon_train_sortino
```

Formula:

```text
Neg(CSRank(Mul(
  ZScore(Sub(Div(Sub($close,$low),Add(Abs(Sub($high,$low)),0.000001)),
             Mean(Div(Sub($close,$low),Add(Abs(Sub($high,$low)),0.000001)),30))),
  CSRank(ValidRatioGate($ctx_rzrq_rzyezb,60,0.8))
)))
```

Interpretation:

```text
This is the best strict Stage1 long-only row by train reward.
It is not accepted because train-day Sortino is near zero, validation is negative, and worst-horizon train Sortino blocks it.
```

Best Stage1 by train-day Sortino:

```text
candidate_id: phase3dv_00254
expression_hash: 91a4b84e53b0df369f6511a7
decision: HOLD_TRAIN_REWARD

train_reward: 0.03865481
train_day_sortino: 0.43402254
validation_day_sortino: -0.28773360
holdout_day_sortino: -0.34804757
blocker: non_positive_worst_horizon_train_sortino
```

Interpretation:

```text
This has the best train-day Sortino in Stage1, but OOS is bad.
It is evidence of train-slice overfit or regime-specialist fragility, not an accepted alpha.
```

## Level 2: Current Research Best

Source:

```text
runtime/phase3eh_stage0_trainonly_dv_pack_20260704/phase3dv_seed_budget_table.csv
```

Current best train-only candidate:

```text
candidate_id: phase3dv_00353
expression_hash: 3373fb56bca18a8f2ae40388
decision: TRAIN_REWARD_FOLLOWUP_READY

train_reward: 0.36619046
train_day_sortino: 0.33749644
train_minute_sortino: 0.33749644
train_worst_horizon_day_sortino: 0.18773406
train_median_horizon_day_sortino: 0.25516547
train_day_mcmc_p25: 0.15271901
train_day_mcmc_prob_gt_0: 0.94833333
train_rank_ic_mean: 0.01105923
train_regime_stability_score: 0.45918407
train_mean_one_way_turnover: 0.04125333

OOS status: blanked_before_next_pack_generation
```

Formula:

```text
CSRank(Sub(
  Neg(CSRank(StateDwell($evt_uplimit_active,40))),
  CSRank(ValidRatioGate($ctx_hfq_pe_ttm,60,0.5))
))
```

Economic reading:

```text
It combines:
  - limit-event state dwell: avoid / fade persistent uplimit-active crowding state
  - PE availability / valuation context: require a usable lagged fundamental context

This is consistent with a short-horizon A-share behavioral hypothesis:
crowded limit-event state plus valuation context can identify fragile post-event demand rather than stable continuation.
```

Boundary:

```text
This is the current best research candidate, not current best verified alpha.
It has no validation / holdout score because OOS was blanked before the next pack.
It must be rerun by exact expression_hash under current long_only_top settings before it can be compared to Stage1 rows.
```

Important id note:

```text
candidate_id phase3dv_00353 also appeared in Phase3EI stage0 with a different expression_hash.
That is id reuse, not verification.
The source hash 3373fb56bca18a8f2ae40388 is not validated by that id-only match.
```

## Level 3: Legacy Diagnostic Best

Source:

```text
reports/phase3cp_real_cm_balanced_topq30_loop_20260623/phase3cm_train_reward/phase3cm_train_reward.csv
```

Best old diagnostic by train-day Sortino:

```text
candidate_id: phase3cp_00030
expression_hash: c8af19143070433e5893b16e
decision: TRAIN_REWARD_FOLLOWUP_READY

train_reward: 0.32397560
train_day_sortino: 0.98504270
validation_day_sortino: 0.23401585
holdout_day_sortino: 2.36984576
train_day_mcmc_prob_gt_0: 0.94983278
```

Formula:

```text
CSRank(Mul(
  ZScore(Sub(Div(Sub($close,$low),Add(Abs(Sub($high,$low)),0.000001)),
             Mean(Div(Sub($close,$low),Add(Abs(Sub($high,$low)),0.000001)),30))),
  ZScore(Div($m1_first30_vol,Add(Abs($volume),0.000001)))
))
```

Why it is not current best:

```text
The audit summary says diagnostic-only.
It used max_shards=1 and sample_trade_times_per_shard=32.
It predates the strict current CN long-only default and exact-hash verification rule.
It is a useful motif clue, not current-policy alpha proof.
```

## Current Ranking

| Rank | Item | Level | Status |
|---|---|---|---|
| 1 | phase3cp_17360 | strict long-only Stage1 best by train reward | HOLD, not alpha |
| 2 | phase3dv_00254 | strict long-only Stage1 best by train-day Sortino | HOLD, OOS failed |
| 3 | phase3dv_00353 | current train-only research best | needs exact long-only rerun |
| 4 | phase3cp_00030 | legacy diagnostic best | motif clue only |

## Required Next Action

Do not launch another broad search off these numbers directly.

Next controlled action:

```text
Run exact-expression current-policy rerun for phase3dv_00353:
  portfolio_mode: long_only_top
  short_allowed: false
  same expression_hash: 3373fb56bca18a8f2ae40388
  multi-shard train / validation / holdout
  current Phase3CM reward with rankIC and regime-stability components
  report candidate_id and expression_hash together
```

If it survives, it becomes the first real current true1min followup under the corrected policy.
If it fails, current true1min remains at:

```text
best verified level: HOLD_TRAIN_REWARD
accepted alpha count: 0
```
