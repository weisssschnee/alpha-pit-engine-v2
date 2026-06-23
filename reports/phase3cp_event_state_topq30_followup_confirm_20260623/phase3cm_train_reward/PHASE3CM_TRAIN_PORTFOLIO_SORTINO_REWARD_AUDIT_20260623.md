# Phase3CM Train Portfolio Sortino Reward Audit 2026-06-23

Decision: `PHASE3CM_TRAIN_REWARD_AUDIT_READY_DIAGNOSTIC_ONLY`

## Scope

Computes true1min train / validation / holdout portfolio reward curves for already-generated candidates.
This replaces fragment Sortino as the intended search feedback target. It does not launch search and does not promote candidates.

## Summary

- candidates: `1`
- portfolio pnl rows written: `0`
- followup-ready by train reward only: `0`
- horizons: `[1, 5, 15]`
- train/validation/holdout fractions: `0.6` / `0.2` / `0.2`

## Top Train Reward Rows

| rank | candidate | reward | train day sortino | worst h sortino | val sortino | holdout sortino | turnover | decision | blockers | expression |
|---:|---|---:|---:|---:|---:|---:|---:|---|---|---|
| 1 | `phase3cp_00030` | -0.12542552 | 0.02722219 | -0.08736233 | -0.39938356 | 1.00886859 | 0.69663387 | `HOLD_TRAIN_REWARD` | `non_positive_worst_horizon_train_sortino` | `CSRank(Mul(ZScore(Sub(Div(Sub($close,$low),Add(Abs(Sub($high,$low)),0.000001)),Mean(Div(Sub($close,$low),Add(Abs(Sub($hi` |

## Boundary

- This is train-set reward evidence, not final alpha proof.
- Validation and holdout columns are reported for leakage control; searchers must not optimize holdout.
- Horizon sleeves are equal-weighted at each trade_time before portfolio Sortino is computed.
- Costs use turnover-adjusted long-short one-way turnover; this is still not a full fill simulator.
- Phase3BZ fragment replay remains available only as diagnostic slice replay.
