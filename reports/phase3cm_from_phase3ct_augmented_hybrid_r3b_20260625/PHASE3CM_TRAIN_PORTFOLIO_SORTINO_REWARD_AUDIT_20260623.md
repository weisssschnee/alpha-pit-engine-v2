# Phase3CM Train Portfolio Sortino Reward Audit 2026-06-23

Decision: `PHASE3CM_TRAIN_REWARD_AUDIT_READY_DIAGNOSTIC_ONLY`

## Scope

Computes true1min train / validation / holdout portfolio reward curves for already-generated candidates.
This replaces fragment Sortino as the intended search feedback target. It does not launch search and does not promote candidates.

## Summary

- candidates: `56`
- portfolio pnl rows written: `0`
- followup-ready by train reward only: `0`
- horizons: `[1, 5, 15, 30]`
- train/validation/holdout fractions: `0.6` / `0.2` / `0.2`

## Top Train Reward Rows

| rank | candidate | reward | train day sortino | worst h sortino | val sortino | holdout sortino | turnover | decision | blockers | expression |
|---:|---|---:|---:|---:|---:|---:|---:|---|---|---|
| 1 | `phase3bp_00089` | -0.90686831 | -0.62551138 | -0.92802443 | -0.65335447 | -0.74470714 | 0.61205387 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|inherited_search_blocker` | `CSRank(Mul(ZScore(Div($m1_first5_amount,Add(Abs($amount),0.000001))),ZScore(Std(Div(Sub($high,$low),Add(Abs($open),0.000` |
| 2 | `phase3bp_00093` | -0.9308196 | -0.63078556 | -0.92486766 | -0.64491053 | -0.74581389 | 0.63864699 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|inherited_search_blocker` | `CSRank(Sub(ZScore(Div($m1_first5_amount,Add(Abs($amount),0.000001))),ZScore(Std(Div(Sub($high,$low),Add(Abs($open),0.000` |
| 3 | `phase3bp_00097` | -0.94059137 | -0.6424277 | -0.9249981 | -0.64479417 | -0.74345162 | 0.64079528 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|inherited_search_blocker` | `CSRank(Sub(ZScore(Div($m1_first15_amount,Add(Abs($amount),0.000001))),ZScore(Std(Div(Sub($high,$low),Add(Abs($open),0.00` |
| 4 | `phase3bp_00095` | -0.95252837 | -0.66182354 | -0.93753854 | -0.65153687 | -0.77648525 | 0.63498983 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|inherited_search_blocker` | `CSRank(Mul(ZScore(Div($m1_first5_amount,Add(Abs($amount),0.000001))),ZScore(Std(Div(Sub($high,$low),Add(Abs($open),0.000` |
| 5 | `phase3bp_00108` | -0.95923133 | -0.66463368 | -0.94329304 | -0.69741632 | -0.745509 | 0.63584993 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|inherited_search_blocker` | `CSRank(Mul(ZScore(Div($m1_first30_amount,Add(Abs($amount),0.000001))),ZScore(Std(Div(Sub($high,$low),Add(Abs($open),0.00` |
| 6 | `phase3bp_00106` | -0.96150817 | -0.673717 | -0.93659622 | -0.64422011 | -0.74098762 | 0.63477949 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|inherited_search_blocker` | `CSRank(Mul(ZScore(Div($m1_first15_amount,Add(Abs($amount),0.000001))),ZScore(Std(Div(Sub($high,$low),Add(Abs($open),0.00` |
| 7 | `phase3bp_00107` | -0.9774677 | -0.65945465 | -0.93730923 | -0.71965606 | -0.75694652 | 0.66915895 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|inherited_search_blocker` | `CSRank(Sub(ZScore(Div($m1_first30_amount,Add(Abs($amount),0.000001))),ZScore(Std(Div(Sub($high,$low),Add(Abs($open),0.00` |
| 8 | `phase3bp_00105` | -1.01088686 | -0.69591717 | -0.93988141 | -0.6562086 | -0.73700772 | 0.67695996 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|inherited_search_blocker` | `CSRank(Mul(ZScore(Div($m1_first15_amount,Add(Abs($amount),0.000001))),ZScore(Std(Div(Sub($high,$low),Add(Abs($open),0.00` |
| 9 | `phase3bp_00111` | -1.01843754 | -0.73039848 | -0.92847722 | -0.6571162 | -0.8712819 | 0.6601511 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|inherited_search_blocker` | `CSRank(Mul(ZScore(Div(Sub($m1_first5_high,$m1_first5_low),Add(Abs($open),0.000001))),ZScore(Std(Div(Sub($high,$low),Add(` |
| 10 | `phase3bp_00109` | -1.03654477 | -0.74168082 | -0.9330779 | -0.73648976 | -0.88555807 | 0.67205602 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|inherited_search_blocker` | `CSRank(Mul(ZScore(Div(Sub($m1_first5_high,$m1_first5_low),Add(Abs($open),0.000001))),ZScore(Std(Div(Sub($high,$low),Add(` |
| 11 | `phase3bp_00071` | -1.03694688 | -0.75429784 | -0.94121713 | -0.66143399 | -0.84066725 | 0.65658288 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|inherited_search_blocker` | `CSRank(Mul(ZScore(Div(Sub($m1_first15_high,$m1_first15_low),Add(Abs($open),0.000001))),ZScore(Std(Div(Sub($high,$low),Ad` |
| 12 | `phase3bp_00112` | -1.04852755 | -0.74900158 | -0.93320746 | -0.80650125 | -0.87954676 | 0.68058423 | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|inherited_search_blocker` | `CSRank(Mul(ZScore(Div(Sub($m1_first5_high,$m1_first5_low),Add(Abs($open),0.000001))),ZScore(Std(Div(Sub($high,$low),Add(` |
| 13 | `phase3bp_00002` | -2.15 | None | None |  |  | None | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|inherited_search_blocker` | `Neg(CSRank(WindowStateCount($evt_uplimit_type_code,10)))` |
| 14 | `phase3bp_00004` | -2.15 | None | None |  |  | None | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|inherited_search_blocker` | `CSRank(WindowStateCount($evt_uplimit_auction_buy,10))` |
| 15 | `phase3bp_00008` | -2.15 | None | None |  |  | None | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|inherited_search_blocker` | `Neg(CSRank(WindowStateCount($evt_uplimit_auction_money,10)))` |
| 16 | `phase3bp_00012` | -2.15 | None | None |  |  | None | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|inherited_search_blocker` | `CSRank(WindowStateCount($evt_uplimit_auction_offer,10))` |
| 17 | `phase3bp_00016` | -2.15 | None | None |  |  | None | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|inherited_search_blocker` | `Neg(CSRank(WindowStateCount($evt_uplimit_fd_close,10)))` |
| 18 | `phase3bp_00020` | -2.15 | None | None |  |  | None | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|inherited_search_blocker` | `Neg(CSRank(SafeCSResidual($ctx_ths_hot_rank_diff,$amount,20,5,0.8)))` |
| 19 | `phase3bp_00028` | -2.15 | None | None |  |  | None | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|inherited_search_blocker` | `CSRank(SafeCSResidual($ctx_ths_hot_circulation_value,$amount,20,5,0.8))` |
| 20 | `phase3bp_00033` | -2.15 | None | None |  |  | None | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|inherited_search_blocker` | `CSRank(Mul(CSRank(WindowStateCount($evt_uplimit_up_limit_keep_times,10)),CSRank(SafeCSResidual($ctx_zls_strong,$amount,2` |
| 21 | `phase3bp_00034` | -2.15 | None | None |  |  | None | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|inherited_search_blocker` | `CSRank(Sub(CSRank(WindowStateCount($evt_uplimit_up_limit_keep_times,10)),CSRank(SafeCSResidual($ctx_ths_hot_last_pct,$am` |
| 22 | `phase3bp_00035` | -2.15 | None | None |  |  | None | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|inherited_search_blocker` | `CSRank(Sub(CSRank(WindowStateCount($evt_uplimit_up_limit_keep_times,10)),CSRank(SafeCSResidual($ctx_sent_lb_3_num,$amoun` |
| 23 | `phase3bp_00036` | -2.15 | None | None |  |  | None | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|inherited_search_blocker` | `CSRank(Sub(CSRank(WindowStateCount($evt_uplimit_type_code,10)),CSRank(SafeCSResidual($ctx_ths_hot_circulation_value,$amo` |
| 24 | `phase3bp_00038` | -2.15 | None | None |  |  | None | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|inherited_search_blocker` | `CSRank(Mul(CSRank(WindowStateCount($evt_uplimit_up_limit_keep_times,10)),CSRank(SafeCSResidual($ctx_sent_mian_num,$amoun` |
| 25 | `phase3bp_00039` | -2.15 | None | None |  |  | None | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|inherited_search_blocker` | `CSRank(Mul(CSRank(WindowStateCount($evt_uplimit_up_limit_keep_times,10)),CSRank(SafeCSResidual($ctx_sent_downlimit_num,$` |
| 26 | `phase3bp_00040` | -2.15 | None | None |  |  | None | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|inherited_search_blocker` | `CSRank(Mul(CSRank(WindowStateCount($evt_uplimit_type_code,10)),CSRank(SafeCSResidual($ctx_sent_lb_2_num,$amount,20,5,0.8` |
| 27 | `phase3bp_00042` | -2.15 | None | None |  |  | None | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|inherited_search_blocker` | `CSRank(Mul(CSRank(WindowStateCount($evt_uplimit_up_limit_keep_times,10)),CSRank(SafeCSResidual($ctx_sent_ditian_num,$amo` |
| 28 | `phase3bp_00043` | -2.15 | None | None |  |  | None | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|inherited_search_blocker` | `CSRank(Sub(CSRank(WindowStateCount($evt_uplimit_type_code,10)),CSRank(SafeCSResidual($ctx_sent_downlimit_num,$amount,20,` |
| 29 | `phase3bp_00044` | -2.15 | None | None |  |  | None | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|inherited_search_blocker` | `CSRank(Mul(CSRank(WindowStateCount($evt_uplimit_type_code,10)),CSRank(SafeCSResidual($ctx_sent_lt5_num,$amount,20,5,0.8)` |
| 30 | `phase3bp_00045` | -2.15 | None | None |  |  | None | `HOLD_TRAIN_REWARD` | `non_positive_train_day_sortino|non_positive_worst_horizon_train_sortino|weak_train_day_mcmc|inherited_search_blocker` | `CSRank(Mul(CSRank(StateDwell($evt_uplimit_up_limit_keep_times,5)),CSRank(SafeCSResidual($ctx_ths_hot_rank_diff,$amount,2` |

## Boundary

- This is train-set reward evidence, not final alpha proof.
- Validation and holdout columns are reported for leakage control; searchers must not optimize holdout.
- Horizon sleeves are equal-weighted at each trade_time before portfolio Sortino is computed.
- Costs use turnover-adjusted long-short one-way turnover; this is still not a full fill simulator.
- Phase3BZ fragment replay remains available only as diagnostic slice replay.
