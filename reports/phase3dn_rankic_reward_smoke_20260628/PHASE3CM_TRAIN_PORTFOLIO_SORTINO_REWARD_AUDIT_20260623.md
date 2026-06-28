# Phase3CM Train Portfolio Sortino Reward Audit 2026-06-23

Decision: `PHASE3CM_TRAIN_REWARD_AUDIT_READY_DIAGNOSTIC_ONLY`

## Scope

Computes true1min train / validation / holdout portfolio reward curves for already-generated candidates.
This replaces fragment Sortino as the intended search feedback target. It does not launch search and does not promote candidates.

## Summary

- candidates: `2`
- portfolio pnl rows written: `0`
- followup-ready by train reward only: `0`
- horizons: `[1, 5]`
- train/validation/holdout fractions: `0.6` / `0.2` / `0.2`
- rank IC loss weight/cap: `6.0` / `0.35`

## Top Train Reward Rows

| rank | candidate | reward | train day sortino | train rank IC | rank IC loss | rank IC component | worst h sortino | val sortino | holdout sortino | turnover | decision | blockers | expression |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|
| 1 | `phase3bt_00309` | -0.66433836 | -0.60583895 | 0.11123879 | -0.11123879 | 0.35 | -0.82089791 | -0.99743743 |  | 0.81024638 | `HOLD_TRAIN_REWARD` | `non_positive_train_reward|non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|extreme_turnover` | `CSRank(Sub(ZScore(Div(Sub($m1_first15_last_close,$open),Add(Abs($open),0.000001))),ZScore(Div(Mean($vwap,2),Add(Abs(Mean` |
| 2 | `phase3bt_00368` | -0.70648502 | -0.71569952 | 0.10647529 | -0.10647529 | 0.35 | -0.78300435 | -0.99056708 |  | 0.81389546 | `HOLD_TRAIN_REWARD` | `non_positive_train_reward|non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|extreme_turnover` | `CSRank(Sub(ZScore(Div(Sub($m1_first30_vwap,$open),Add(Abs($open),0.000001))),ZScore(Div(Mean($vwap,2),Add(Abs(Mean($vwap` |

## Boundary

- This is train-set reward evidence, not final alpha proof.
- `rank_ic_loss` is train-side aligned rank IC loss and is included in optimizer reward with a bounded component.
- Validation and holdout columns are reported for leakage control; searchers must not optimize holdout.
- Horizon sleeves are equal-weighted at each trade_time before portfolio Sortino is computed.
- Costs use turnover-adjusted long-short one-way turnover; this is still not a full fill simulator.
- Phase3BZ fragment replay remains available only as diagnostic slice replay.
