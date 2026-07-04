# Phase3CM Train Portfolio Sortino Reward Audit 2026-06-23

Decision: `PHASE3CM_TRAIN_REWARD_AUDIT_READY_DIAGNOSTIC_ONLY`

## Scope

Computes true1min train / validation / holdout portfolio reward curves for already-generated candidates.
This replaces fragment Sortino as the intended search feedback target. It does not launch search and does not promote candidates.

## Summary

- candidates: `4`
- portfolio pnl rows written: `0`
- portfolio mode: `long_only_top`; short allowed: `False`
- followup-ready by train reward only: `0`
- horizons: `[1]`
- train/validation/holdout fractions: `0.6` / `0.2` / `0.2`
- rank IC loss weight/cap: `6.0` / `0.35`
- regime stability weight/cap: `0.08` / `0.1`

## Top Train Reward Rows

| rank | candidate | reward | train day sortino | train rank IC | rank IC comp | regime comp | worst h sortino | val sortino | holdout sortino | turnover | decision | blockers | expression |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|
| 1 | `phase3cp_13121` | -0.09594434 | -0.16135936 | 0.12006669 | 0.35 | 0.0 | -0.16135936 | -0.92736338 |  | 0.84557292 | `HOLD_TRAIN_REWARD` | `non_positive_train_reward|non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|extreme_turnover` | `CSRank(Mul(ZScore(Div($m1_first5_amount,Add(Abs($amount),0.000001))),ZScore(Delta($vwap,15))))` |
| 2 | `phase3cp_13122` | -0.53548911 | -0.63810497 | 0.10870707 | 0.35 | 0.0 | -0.63810497 | -0.93221167 |  | 0.83305584 | `HOLD_TRAIN_REWARD` | `non_positive_train_reward|non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|extreme_turnover` | `CSRank(Mul(ZScore(Div($m1_first5_amount,Add(Abs($amount),0.000001))),ZScore(Delta($vwap,20))))` |
| 3 | `phase3cp_13125` | -0.70664979 | -0.71583741 | 0.04934975 | 0.2960985 | 0.0 | -0.71583741 | -0.59416065 |  | 0.87005208 | `HOLD_TRAIN_REWARD` | `non_positive_train_reward|non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|extreme_turnover` | `CSRank(Sub(ZScore(Div($m1_first5_amount,Add(Abs($amount),0.000001))),ZScore(Delta($vwap,10))))` |
| 4 | `cn4_clean_reward_fixture` | -2.3 | None |  |  |  | None |  |  | None | `HOLD_SCHEMA_MISSING_FIELDS` | `candidate_missing_schema_fields:m1_first_ret|range_location` | `Rank(Mean($m1_first_ret,5)) - Rank(Std($range_location,10))` |

## Boundary

- This is train-set reward evidence, not final alpha proof.
- `rank_ic_loss` is train-side aligned rank IC loss and is included in optimizer reward with a bounded component.
- Regime stability is a low-weight train-only reward component; it is not a promotion gate.
- Validation and holdout columns are reported for leakage control; searchers must not optimize holdout.
- Horizon sleeves are equal-weighted at each trade_time before portfolio Sortino is computed.
- `long_short_spread` is a ranking proxy and is not CN/A-share tradable reward.
- CN/A-share train reward should use a long-only mode; costs are turnover-adjusted and still not a full fill simulator.
- Phase3BZ fragment replay remains available only as diagnostic slice replay.
