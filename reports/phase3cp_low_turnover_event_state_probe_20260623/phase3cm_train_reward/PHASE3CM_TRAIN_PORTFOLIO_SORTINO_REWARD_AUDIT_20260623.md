# Phase3CM Train Portfolio Sortino Reward Audit 2026-06-23

Decision: `PHASE3CM_TRAIN_REWARD_AUDIT_READY_DIAGNOSTIC_ONLY`

## Scope

Computes true1min train / validation / holdout portfolio reward curves for already-generated candidates.
This replaces fragment Sortino as the intended search feedback target. It does not launch search and does not promote candidates.

## Summary

- candidates: `24`
- portfolio pnl rows written: `0`
- followup-ready by train reward only: `0`
- horizons: `[1, 5, 15]`
- train/validation/holdout fractions: `0.6` / `0.2` / `0.2`

## Top Train Reward Rows

| rank | candidate | reward | train day sortino | worst h sortino | val sortino | holdout sortino | turnover | decision | blockers | expression |
|---:|---|---:|---:|---:|---:|---:|---:|---|---|---|
| 1 | `phase3cp_lt_event_00060` | -0.48827207 | -0.42657095 | -0.58959837 | -0.61816916 | -0.78247301 | 0.38285492 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc` | `CSRank(Wma(Sub(ZScore(Div($m1_first30_vol,Add(Abs($volume),0.000001))),ZScore(Std(Div(Sub($high,$low),Add(Abs($open),0.0` |
| 2 | `phase3cp_lt_event_00058` | -0.50845985 | -0.44533824 | -0.60670238 | -0.62193405 | -0.79355085 | 0.38558399 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc` | `CSRank(Mean(Sub(ZScore(Div($m1_first30_vol,Add(Abs($volume),0.000001))),ZScore(Std(Div(Sub($high,$low),Add(Abs($open),0.` |
| 3 | `phase3cp_lt_event_00056` | -0.51443687 | -0.44241236 | -0.64790957 | -0.61804686 | -0.75808792 | 0.37725192 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc` | `CSRank(Mean(Sub(ZScore(Div($m1_first30_vol,Add(Abs($volume),0.000001))),ZScore(Std(Div(Sub($high,$low),Add(Abs($open),0.` |
| 4 | `phase3cp_lt_event_00057` | -0.51522809 | -0.4474288 | -0.63615684 | -0.64929166 | -0.76920748 | 0.3900039 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc` | `CSRank(Mean(Sub(ZScore(Div($m1_first30_vol,Add(Abs($volume),0.000001))),ZScore(Std(Div(Sub($high,$low),Add(Abs($open),0.` |
| 5 | `phase3cp_lt_event_00059` | -0.51549708 | -0.44533881 | -0.6528645 | -0.62983981 | -0.77863955 | 0.3882945 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc` | `CSRank(Wma(Sub(ZScore(Div($m1_first30_vol,Add(Abs($volume),0.000001))),ZScore(Std(Div(Sub($high,$low),Add(Abs($open),0.0` |
| 6 | `phase3cp_lt_event_00042` | -0.60335315 | -0.5211478 | -0.79172988 | -0.62069452 | -0.71782748 | 0.46388035 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc` | `CSRank(Mean(Sub(ZScore(Div($m1_first30_vol,Add(Abs($volume),0.000001))),ZScore(Std(Div(Sub($high,$low),Add(Abs($open),0.` |
| 7 | `phase3cp_lt_event_00043` | -0.61577811 | -0.54299396 | -0.76297677 | -0.62908647 | -0.73461428 | 0.44724649 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc` | `CSRank(Mean(Sub(ZScore(Div($m1_first30_vol,Add(Abs($volume),0.000001))),ZScore(Std(Div(Sub($high,$low),Add(Abs($open),0.` |
| 8 | `phase3cp_lt_event_00045` | -0.62723889 | -0.55816126 | -0.76433957 | -0.64905812 | -0.74828813 | 0.45196028 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc` | `CSRank(Wma(Sub(ZScore(Div($m1_first30_vol,Add(Abs($volume),0.000001))),ZScore(Std(Div(Sub($high,$low),Add(Abs($open),0.0` |
| 9 | `phase3cp_lt_event_00044` | -0.64093804 | -0.56587173 | -0.79859908 | -0.65133541 | -0.69896367 | 0.46102084 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc` | `CSRank(Wma(Sub(ZScore(Div($m1_first30_vol,Add(Abs($volume),0.000001))),ZScore(Std(Div(Sub($high,$low),Add(Abs($open),0.0` |
| 10 | `phase3cp_lt_event_00041` | -0.65827684 | -0.59252439 | -0.79167705 | -0.62530691 | -0.69662314 | 0.44761504 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc` | `CSRank(Mean(Sub(ZScore(Div($m1_first30_vol,Add(Abs($volume),0.000001))),ZScore(Std(Div(Sub($high,$low),Add(Abs($open),0.` |
| 11 | `phase3cp_lt_event_00050` | -0.70315872 | -0.44783206 | -0.6843683 | 0.9613048 | -0.72049934 | 0.78005762 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|extreme_turnover` | `CSRank(Wma(Mul(ZScore(Sub(Div(Sub($close,$low),Add(Abs(Sub($high,$low)),0.000001)),Mean(Div(Sub($close,$low),Add(Abs(Sub` |
| 12 | `phase3cp_lt_event_00035` | -0.75209547 | -0.49346649 | -0.73532131 | 0.0280884 | -0.61967652 | 0.78561879 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|extreme_turnover` | `CSRank(Wma(Mul(ZScore(Sub(Div(Sub($close,$low),Add(Abs(Sub($high,$low)),0.000001)),Mean(Div(Sub($close,$low),Add(Abs(Sub` |
| 13 | `phase3cp_lt_event_00054` | -0.77576279 | -0.54891131 | -0.77868963 | -0.5460995 | -0.63586506 | 0.74901398 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc` | `CSRank(Mean(CSRank(Mul(ZScore(Sub(Div(Sub($close,$low),Add(Abs(Sub($high,$low)),0.000001)),Mean(Div(Sub($close,$low),Add` |
| 14 | `phase3cp_lt_event_00039` | -0.79550956 | -0.54888404 | -0.82389096 | -0.70151868 | -0.58928794 | 0.76289167 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|extreme_turnover` | `CSRank(Mean(CSRank(Mul(ZScore(Sub(Div(Sub($close,$low),Add(Abs(Sub($high,$low)),0.000001)),Mean(Div(Sub($close,$low),Add` |
| 15 | `phase3cp_lt_event_00053` | -0.81606543 | -0.60774544 | -0.74583503 | -0.34095389 | -0.48610563 | 0.7558095 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|extreme_turnover` | `CSRank(Mean(CSRank(Mul(ZScore(Sub(Div(Sub($close,$low),Add(Abs(Sub($high,$low)),0.000001)),Mean(Div(Sub($close,$low),Add` |
| 16 | `phase3cp_lt_event_00047` | -0.82928625 | -0.5969705 | -0.74063952 | -0.56961985 | -0.67289766 | 0.78845853 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|extreme_turnover` | `CSRank(Mean(Mul(ZScore(Sub(Div(Sub($close,$low),Add(Abs(Sub($high,$low)),0.000001)),Mean(Div(Sub($close,$low),Add(Abs(Su` |
| 17 | `phase3cp_lt_event_00038` | -0.83907469 | -0.59690492 | -0.82186624 | -0.38156871 | -0.47561149 | 0.77644593 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|extreme_turnover` | `CSRank(Mean(CSRank(Mul(ZScore(Sub(Div(Sub($close,$low),Add(Abs(Sub($high,$low)),0.000001)),Mean(Div(Sub($close,$low),Add` |
| 18 | `phase3cp_lt_event_00051` | -0.86107232 | -0.59654009 | -0.86812983 | -0.84528273 | -0.79268892 | 0.79002404 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|extreme_turnover` | `CSRank(Wma(Mul(ZScore(Sub(Div(Sub($close,$low),Add(Abs(Sub($high,$low)),0.000001)),Mean(Div(Sub($close,$low),Add(Abs(Sub` |
| 19 | `phase3cp_lt_event_00036` | -0.87937631 | -0.63507926 | -0.85178428 | -0.72976098 | -0.76417058 | 0.785527 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|extreme_turnover` | `CSRank(Wma(Mul(ZScore(Sub(Div(Sub($close,$low),Add(Abs(Sub($high,$low)),0.000001)),Mean(Div(Sub($close,$low),Add(Abs(Sub` |
| 20 | `phase3cp_lt_event_00032` | -0.89568393 | -0.65373282 | -0.8130648 | -0.58862982 | -0.57904994 | 0.79754016 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|extreme_turnover` | `CSRank(Mean(Mul(ZScore(Sub(Div(Sub($close,$low),Add(Abs(Sub($high,$low)),0.000001)),Mean(Div(Sub($close,$low),Add(Abs(Su` |
| 21 | `phase3cp_lt_event_00033` | -0.90535629 | -0.66211011 | -0.84376298 | -0.77940056 | -0.6433854 | 0.7924571 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|extreme_turnover` | `CSRank(Mean(Mul(ZScore(Sub(Div(Sub($close,$low),Add(Abs(Sub($high,$low)),0.000001)),Mean(Div(Sub($close,$low),Add(Abs(Su` |
| 22 | `phase3cp_lt_event_00048` | -0.96231557 | -0.74211143 | -0.84845076 | -0.77218796 | -0.6758344 | 0.78953341 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|extreme_turnover` | `CSRank(Mean(Mul(ZScore(Sub(Div(Sub($close,$low),Add(Abs(Sub($high,$low)),0.000001)),Mean(Div(Sub($close,$low),Add(Abs(Su` |
| 23 | `phase3cp_lt_event_00034` | -0.97261497 | -0.71945289 | -0.95395524 | -0.70986314 | -0.90065537 | 0.79154308 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|extreme_turnover` | `CSRank(Mean(Mul(ZScore(Sub(Div(Sub($close,$low),Add(Abs(Sub($high,$low)),0.000001)),Mean(Div(Sub($close,$low),Add(Abs(Su` |
| 24 | `phase3cp_lt_event_00049` | -1.02941845 | -0.79702757 | -0.95975426 | -0.63994363 | -0.89859244 | 0.79225113 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|extreme_turnover` | `CSRank(Mean(Mul(ZScore(Sub(Div(Sub($close,$low),Add(Abs(Sub($high,$low)),0.000001)),Mean(Div(Sub($close,$low),Add(Abs(Su` |

## Boundary

- This is train-set reward evidence, not final alpha proof.
- Validation and holdout columns are reported for leakage control; searchers must not optimize holdout.
- Horizon sleeves are equal-weighted at each trade_time before portfolio Sortino is computed.
- Costs use turnover-adjusted long-short one-way turnover; this is still not a full fill simulator.
- Phase3BZ fragment replay remains available only as diagnostic slice replay.
