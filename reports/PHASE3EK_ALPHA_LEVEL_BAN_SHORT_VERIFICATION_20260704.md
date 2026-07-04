# Phase3EK Alpha-Level Ban-Short Verification 2026-07-04

Decision: `CN_LONG_ONLY_ALPHA_LEVEL_VERIFICATION_RESTATED`

## Correction

`1024` was the Phase3EI broad recheck pool size. It was not the number of historical alpha-level products, and it must not be described as historical alphatop count.

Correct scope split:

```text
Mature daily official chain:
  official / registry-level assets.

Current true1min chain:
  mostly research followup candidates, not deployed alpha-level products.

Phase3EI 1024:
  broad historical candidate/proxy/followup recheck pool.
  useful stress test, not alpha inventory.
```

## Verification Result

### 1. Mature Daily Official Chain

Official X0 shadow object:

```text
object_id: X0_official_6_R3_liquidity_low_v1
status: official_daily_shadow
evidence_level: L2.5_daily_regime_gated_shadow_proof
cluster_count: 6
book_rule:
  weighting: locked_equal_weight
  cash_when_gate_off: true
  rebalance_clock: daily_proxy
  execution_level: daily_proxy_no_minute_slippage
```

Verified metrics from `phase3o_x0_official_shadow_v1.json`:

```text
strict_cost_adjusted_sortino_count: 6
strict_cost_adjusted_sortino_min: 2.484331
strict_cost_adjusted_sortino_max: 4.458059
2026 full_calendar_sortino: 6.085253
2026 max_drawdown: -0.03442312
```

Boundary:

```text
This verifies the old official daily-shadow chain at its original daily-proxy evidence level.
The X0 object itself records locked equal-weight, cash_when_gate_off, and strict cost-adjusted daily shadow metrics.
It does not prove minute execution, live trading, true slippage, broker fill, or production readiness.
```

Phase3K 149 representative registry:

```text
representative_count: 149
portfolio_replay_long_only_sortino_nonnull: 68
portfolio_replay_long_only_sortino_positive: 68
portfolio_replay_long_only_sortino_min: 0.512765
portfolio_replay_long_only_sortino_max: 1.630332
```

Boundary:

```text
68 registry rows carry explicit positive long-only replay Sortino.
The remaining rows are not counted here as explicit long-only verified rows unless their source evidence is separately traced.
```

### 2. Current True1min Alpha-Level Products

Narrow scan result:

```text
explicit true1min deployable / production alpha products found: 0
unique true1min TRAIN_REWARD_FOLLOWUP_READY / KEEP-like expressions found: 28
```

These 28 are research followup candidates, not official alpha-level products.

Exact-hash ban-short join against Phase3EI recheck:

```text
stage1_exact_rechecked_HOLD_TRAIN_REWARD: 5
stage0_exact_rechecked_HOLD_TRAIN_REWARD: 4
id_reused_hash_mismatch_not_verified: 1
not_in_phase3ei_recheck_by_exact_hash: 18
```

Important correction:

```text
candidate_id alone is not sufficient evidence because ids can be reused across packs.
Verification must use expression_hash exact match when available.
```

Example of the id-reuse problem:

```text
candidate_id: phase3dv_00353
source expression_hash: 3373fb56bca18a8f2ae40388
Phase3EI stage0 same candidate_id hash: c21c8923c8df6d6125cfb987
decision: not verified by exact hash
```

The exact-hash rechecked true1min followups did not pass:

```text
stage1 exact-hash rows: 5 / 5 HOLD_TRAIN_REWARD
stage0 exact-hash rows: 4 / 4 HOLD_TRAIN_REWARD
```

Stage1 examples:

```text
phase3cp_00002:
  train_reward: -0.19085895
  train_day_sortino: -0.20795443
  validation_day_sortino: -0.24084416
  holdout_day_sortino: -0.22815442
  blockers: non_positive_train_reward | non_positive_train_day_sortino | non_positive_worst_horizon_train_sortino | weak_train_day_mcmc

phase3cp_00013:
  train_reward: -0.52899331
  train_day_sortino: -0.23783819
  validation_day_sortino: -0.27482835
  holdout_day_sortino: -0.30905743
  blockers: non_positive_train_reward | non_positive_train_day_sortino | non_positive_worst_horizon_train_sortino | weak_train_day_mcmc
```

### 3. Phase3EI Broad Recheck Pool

This is retained only as broad stress-test evidence:

```text
Stage0 aggregate rows: 1008
Stage0 decisions: HOLD_TRAIN_REWARD = 1008
Stage0 short_allowed: false = 1008

Stage1 aggregate rows: 120
Stage1 portfolio_mode: long_only_top = 120
Stage1 short_allowed: false = 120
Stage1 decisions: HOLD_TRAIN_REWARD = 120
```

Interpretation:

```text
The broad pool found no long-only followup under the completed Stage1 audit.
It does not define the number of alpha-level historical products.
```

## Final Statement

The correct validation result is:

```text
Mature daily X0/R3 official-shadow chain:
  verified at existing daily-proxy shadow evidence level.
  not invalidated by the no-short audit.

Mature 149 registry:
  68 rows have explicit positive long-only replay Sortino.
  remaining rows need source-level lookup before being counted as explicit long-only verified.

Current true1min chain:
  no deployable alpha-level product found.
  28 followup-like research expressions found.
  9 exact-hash rechecked under ban-short; all HOLD.
  1 id-only match rejected because expression_hash mismatched.
  18 not covered by exact-hash Phase3EI recheck.
```

## Evidence Files

```text
reports/phase3ek_alpha_level_ban_short_verification_20260704/phase3ek_validation_summary.json
reports/phase3ek_alpha_level_ban_short_verification_20260704/phase3ek_true1min_followup_ban_short_join.csv
reports/phase3ei_historical_ban_short_recheck_77o_20260704/stage0_fast_train_reward/phase3cm_train_reward.csv
reports/phase3ei_historical_ban_short_recheck_77o_20260704/stage1_deeper_train_reward/phase3cm_train_reward.csv
```
