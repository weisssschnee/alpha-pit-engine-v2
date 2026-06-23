# Phase3CM Train Portfolio Sortino Reward Audit 2026-06-23

Decision: `PHASE3CM_TRAIN_REWARD_AUDIT_READY_DIAGNOSTIC_ONLY`

## Scope

Computes true1min train / validation / holdout portfolio reward curves for already-generated candidates.
This replaces fragment Sortino as the intended search feedback target. It does not launch search and does not promote candidates.

## Summary

- candidates: `4`
- portfolio pnl rows written: `0`
- followup-ready by train reward only: `0`
- horizons: `[1, 5]`
- train/validation/holdout fractions: `0.6` / `0.2` / `0.2`

## Top Train Reward Rows

| rank | candidate | reward | train day sortino | worst h sortino | val sortino | holdout sortino | turnover | decision | blockers | expression |
|---:|---|---:|---:|---:|---:|---:|---:|---|---|---|
| 1 | `phase3cp_00003` | -0.58857199 | -0.32877161 | -0.33039126 | -0.74853556 | 2.61859704 | 0.82175481 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|extreme_turnover` | `CSRank(Mul(ZScore(Sub(Div(Sub($close,$low),Add(Abs(Sub($high,$low)),0.000001)),Mean(Div(Sub($close,$low),Add(Abs(Sub($hi` |
| 2 | `phase3cp_00002` | -0.64821192 | -0.58280205 | -0.66967995 | 0.19136249 | -0.88452287 | 0.35447716 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc` | `CSRank(Sub(ZScore(Div($m1_first5_amount,Add(Abs($amount),0.000001))),ZScore(Std(Div(Sub($high,$low),Add(Abs($open),0.000` |
| 3 | `phase3cp_00008` | -0.84887634 | -0.75227589 | -0.86029504 | -0.23255585 | -0.99989144 | 0.62317995 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc` | `CSRank(Sub(ZScore(Div($m1_first15_amount,Add(Abs($amount),0.000001))),ZScore(Std(Div(Sub($high,$low),Add(Abs($open),0.00` |
| 4 | `phase3cp_00007` | -1.20816966 | -0.9902977 | -0.97976904 | -0.06795041 | -0.99865128 | 0.84302885 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|extreme_turnover` | `CSRank(Sub(ZScore(Sub(Div(Sub($close,$low),Add(Abs(Sub($high,$low)),0.000001)),Mean(Div(Sub($close,$low),Add(Abs(Sub($hi` |

## Boundary

- This is train-set reward evidence, not final alpha proof.
- Validation and holdout columns are reported for leakage control; searchers must not optimize holdout.
- Horizon sleeves are equal-weighted at each trade_time before portfolio Sortino is computed.
- Costs use turnover-adjusted long-short one-way turnover; this is still not a full fill simulator.
- Phase3BZ fragment replay remains available only as diagnostic slice replay.
