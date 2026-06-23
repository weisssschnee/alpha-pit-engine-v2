# Phase3CM Train Portfolio Sortino Reward Audit 2026-06-23

Decision: `PHASE3CM_TRAIN_REWARD_AUDIT_READY_DIAGNOSTIC_ONLY`

## Scope

Computes true1min train / validation / holdout portfolio reward curves for already-generated candidates.
This replaces fragment Sortino as the intended search feedback target. It does not launch search and does not promote candidates.

## Summary

- candidates: `12`
- portfolio pnl rows written: `0`
- followup-ready by train reward only: `0`
- horizons: `[1, 5, 15]`
- train/validation/holdout fractions: `0.6` / `0.2` / `0.2`

## Top Train Reward Rows

| rank | candidate | reward | train day sortino | worst h sortino | val sortino | holdout sortino | turnover | decision | blockers | expression |
|---:|---|---:|---:|---:|---:|---:|---:|---|---|---|
| 1 | `phase3cp_00030` | -0.01381677 | 0.25860162 | 0.06395024 | 0.56475186 | 3.63709504 | 0.76656937 | `HOLD_TRAIN_REWARD` | `extreme_turnover` | `CSRank(Mul(ZScore(Sub(Div(Sub($close,$low),Add(Abs(Sub($high,$low)),0.000001)),Mean(Div(Sub($close,$low),Add(Abs(Sub($hi` |
| 2 | `phase3cp_00003` | -0.37184462 | -0.08234275 | -0.27182099 | 0.04308031 | -0.29133125 | 0.81034088 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|extreme_turnover` | `CSRank(Mul(ZScore(Sub(Div(Sub($close,$low),Add(Abs(Sub($high,$low)),0.000001)),Mean(Div(Sub($close,$low),Add(Abs(Sub($hi` |
| 3 | `phase3cp_00023` | -0.50444835 | -0.20249304 | -0.47113005 | -0.29355221 | 0.17549028 | 0.81742483 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|extreme_turnover` | `CSRank(Mul(ZScore(Sub(Div(Sub($close,$low),Add(Abs(Sub($high,$low)),0.000001)),Mean(Div(Sub($close,$low),Add(Abs(Sub($hi` |
| 4 | `phase3cp_00002` | -0.53254818 | -0.41764637 | -0.71912649 | -0.66513398 | -0.71829181 | 0.47490809 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc` | `CSRank(Sub(ZScore(Div($m1_first5_amount,Add(Abs($amount),0.000001))),ZScore(Std(Div(Sub($high,$low),Add(Abs($open),0.000` |
| 5 | `phase3cp_00009` | -0.8453687 | -0.70531365 | -0.805035 | -0.77873694 | -0.71003335 | 0.67718041 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc` | `CSRank(Sub(ZScore(Div($m1_first15_amount,Add(Abs($amount),0.000001))),ZScore(Std(Div(Sub($high,$low),Add(Abs($open),0.00` |
| 6 | `phase3cp_00014` | -0.86557817 | -0.71765109 | -0.82300889 | -0.74746572 | -0.96173274 | 0.69250441 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc` | `CSRank(Mul(ZScore(Div($m1_first5_amount,Add(Abs($amount),0.000001))),ZScore(Std(Div(Sub($high,$low),Add(Abs($open),0.000` |
| 7 | `phase3cp_00035` | -0.89071584 | -0.75546016 | -0.81985524 | -0.75893198 | -0.96045471 | 0.68857792 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc` | `CSRank(Mul(ZScore(Div($m1_first15_amount,Add(Abs($amount),0.000001))),ZScore(Std(Div(Sub($high,$low),Add(Abs($open),0.00` |
| 8 | `phase3cp_00034` | -0.93664835 | -0.79559752 | -0.88142799 | -0.78736708 | -0.92757678 | 0.69354629 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc` | `CSRank(Sub(ZScore(Div($m1_first5_amount,Add(Abs($amount),0.000001))),ZScore(Std(Div(Sub($high,$low),Add(Abs($open),0.000` |
| 9 | `phase3cp_00026` | -0.94622189 | -0.81797273 | -0.87030381 | -0.7912496 | -0.71003335 | 0.69205446 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc` | `CSRank(Sub(ZScore(Div($m1_first30_amount,Add(Abs($amount),0.000001))),ZScore(Std(Div(Sub($high,$low),Add(Abs($open),0.00` |
| 10 | `phase3cp_00024` | -1.11511405 | -0.90170279 | -0.9458766 | -0.98436642 | -0.95425427 | 0.8133123 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|extreme_turnover` | `CSRank(CSResidual(CSRank(Sub(Div(Sub($close,$low),Add(Abs(Sub($high,$low)),0.000001)),Mean(Div(Sub($close,$low),Add(Abs(` |
| 11 | `phase3cp_00016` | -1.16062275 | -0.95536552 | -0.95455755 | -0.94509592 | -0.95429715 | 0.81891522 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|extreme_turnover` | `CSRank(Sub(ZScore(Sub(Div(Sub($close,$low),Add(Abs(Sub($high,$low)),0.000001)),Mean(Div(Sub($close,$low),Add(Abs(Sub($hi` |
| 12 | `phase3cp_00012` | -1.18017652 | -0.97350753 | -0.96054976 | -0.98464118 | -0.9504996 | 0.82614329 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|extreme_turnover` | `CSRank(Sub(ZScore(Sub(Div(Sub($close,$low),Add(Abs(Sub($high,$low)),0.000001)),Mean(Div(Sub($close,$low),Add(Abs(Sub($hi` |

## Boundary

- This is train-set reward evidence, not final alpha proof.
- Validation and holdout columns are reported for leakage control; searchers must not optimize holdout.
- Horizon sleeves are equal-weighted at each trade_time before portfolio Sortino is computed.
- Costs use turnover-adjusted long-short one-way turnover; this is still not a full fill simulator.
- Phase3BZ fragment replay remains available only as diagnostic slice replay.
