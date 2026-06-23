# Phase3CM Train Portfolio Sortino Reward Audit 2026-06-23

Decision: `PHASE3CM_TRAIN_REWARD_AUDIT_READY_DIAGNOSTIC_ONLY`

## Scope

Computes true1min train / validation / holdout portfolio reward curves for already-generated candidates.
This replaces fragment Sortino as the intended search feedback target. It does not launch search and does not promote candidates.

## Summary

- candidates: `12`
- portfolio pnl rows written: `0`
- followup-ready by train reward only: `1`
- horizons: `[1, 5, 15]`
- train/validation/holdout fractions: `0.6` / `0.2` / `0.2`

## Top Train Reward Rows

| rank | candidate | reward | train day sortino | worst h sortino | val sortino | holdout sortino | turnover | decision | blockers | expression |
|---:|---|---:|---:|---:|---:|---:|---:|---|---|---|
| 1 | `phase3cp_00030` | 0.3239756 | 0.9850427 | 0.03156776 | 0.23401585 | 2.36984576 | 0.71037371 | `TRAIN_REWARD_FOLLOWUP_READY` | `` | `CSRank(Mul(ZScore(Sub(Div(Sub($close,$low),Add(Abs(Sub($high,$low)),0.000001)),Mean(Div(Sub($close,$low),Add(Abs(Sub($hi` |
| 2 | `phase3cp_00003` | -0.15805198 | 0.11524058 | -0.26848687 | 0.80293969 | -0.07317269 | 0.71343811 | `HOLD_TRAIN_REWARD` | `non_positive_worst_horizon_train_sortino|weak_train_day_mcmc` | `CSRank(Mul(ZScore(Sub(Div(Sub($close,$low),Add(Abs(Sub($high,$low)),0.000001)),Mean(Div(Sub($close,$low),Add(Abs(Sub($hi` |
| 3 | `phase3cp_00023` | -0.5292948 | -0.29306513 | -0.54444123 | -0.33648268 | 0.4324352 | 0.73007073 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc` | `CSRank(Mul(ZScore(Sub(Div(Sub($close,$low),Add(Abs(Sub($high,$low)),0.000001)),Mean(Div(Sub($close,$low),Add(Abs(Sub($hi` |
| 4 | `phase3cp_00002` | -0.60969415 | -0.52805141 | -0.73773078 | -0.66896722 | -0.64925742 | 0.41225427 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc` | `CSRank(Sub(ZScore(Div($m1_first5_amount,Add(Abs($amount),0.000001))),ZScore(Std(Div(Sub($high,$low),Add(Abs($open),0.000` |
| 5 | `phase3cp_00014` | -0.84403228 | -0.74277313 | -0.89207948 | -0.71103792 | -0.96612032 | 0.61626952 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc` | `CSRank(Mul(ZScore(Div($m1_first5_amount,Add(Abs($amount),0.000001))),ZScore(Std(Div(Sub($high,$low),Add(Abs($open),0.000` |
| 6 | `phase3cp_00034` | -0.87282014 | -0.78230365 | -0.88435706 | -0.79674441 | -0.96496252 | 0.61862 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc` | `CSRank(Sub(ZScore(Div($m1_first5_amount,Add(Abs($amount),0.000001))),ZScore(Std(Div(Sub($high,$low),Add(Abs($open),0.000` |
| 7 | `phase3cp_00009` | -0.87506302 | -0.80287671 | -0.87388393 | -0.7971145 | -0.89780108 | 0.59770498 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc` | `CSRank(Sub(ZScore(Div($m1_first15_amount,Add(Abs($amount),0.000001))),ZScore(Std(Div(Sub($high,$low),Add(Abs($open),0.00` |
| 8 | `phase3cp_00035` | -0.89514537 | -0.81424007 | -0.90243398 | -0.69093691 | -0.96482945 | 0.61291017 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc` | `CSRank(Mul(ZScore(Div($m1_first15_amount,Add(Abs($amount),0.000001))),ZScore(Std(Div(Sub($high,$low),Add(Abs($open),0.00` |
| 9 | `phase3cp_00026` | -0.95400507 | -0.90254709 | -0.91890511 | -0.80917792 | -0.89780108 | 0.60693077 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc` | `CSRank(Sub(ZScore(Div($m1_first30_amount,Add(Abs($amount),0.000001))),ZScore(Std(Div(Sub($high,$low),Add(Abs($open),0.00` |
| 10 | `phase3cp_00024` | -1.07068129 | -0.937892 | -0.95545153 | -0.97299854 | -0.90731796 | 0.71682401 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc` | `CSRank(CSResidual(CSRank(Sub(Div(Sub($close,$low),Add(Abs(Sub($high,$low)),0.000001)),Mean(Div(Sub($close,$low),Add(Abs(` |
| 11 | `phase3cp_00016` | -1.09496861 | -0.96163452 | -0.96071057 | -0.92128683 | -0.94535973 | 0.72517649 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc` | `CSRank(Sub(ZScore(Sub(Div(Sub($close,$low),Add(Abs(Sub($high,$low)),0.000001)),Mean(Div(Sub($close,$low),Add(Abs(Sub($hi` |
| 12 | `phase3cp_00012` | -1.09929258 | -0.96585504 | -0.96641964 | -0.97779251 | -0.91695895 | 0.72516255 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc` | `CSRank(Sub(ZScore(Sub(Div(Sub($close,$low),Add(Abs(Sub($high,$low)),0.000001)),Mean(Div(Sub($close,$low),Add(Abs(Sub($hi` |

## Boundary

- This is train-set reward evidence, not final alpha proof.
- Validation and holdout columns are reported for leakage control; searchers must not optimize holdout.
- Horizon sleeves are equal-weighted at each trade_time before portfolio Sortino is computed.
- Costs use turnover-adjusted long-short one-way turnover; this is still not a full fill simulator.
- Phase3BZ fragment replay remains available only as diagnostic slice replay.
