# Phase3EI Ban-Short Lineage Audit

Created: `2026-07-04T11:59:12.875624+00:00`

## Decision

`PHASE3EI_BAN_SHORT_RECHECK_REQUIRED`

Historical Phase3CM/Phase3BZ/Phase3EG/Phase3EH spread-style outputs are not CN tradable reward evidence unless rechecked under a long-only portfolio mode.

## Policy Counts

| policy | files |
|---|---:|
| `cn_long_only_verified` | 5 |
| `legacy_assume_long_short_recheck_required` | 200 |
| `likely_cn_long_only` | 22 |
| `long_short_spread_proxy_only` | 16 |
| `short_leg_present_proxy_only` | 4 |

## Recheck Pack

- candidate rows selected: `1024`
- required mode: `long_only_top`
- spread/legacy rows are retained only as source attribution, not as alpha proof.

## Top Recheck Candidates By Prior Score

| rank | candidate | prior policy | prior metric | prior score | decision | expression |
|---:|---|---|---|---:|---|---|
| 1 | `cn4_clean_reward_fixture` | `legacy_assume_long_short_recheck_required` | `train_reward` | 0.38 | `TRAIN_REWARD_FOLLOWUP_READY` | `Rank(Mean($m1_first_ret,5)) - Rank(Std($range_location,10))` |
| 2 | `phase3cp_03445` | `legacy_assume_long_short_recheck_required` | `train_reward` | 0.37966456 | `TRAIN_REWARD_FOLLOWUP_READY` | `Neg(CSRank(MaskedZScore($ctx_ths_hot_last_price,60,0.8)))` |
| 3 | `phase3dv_00353` | `legacy_assume_long_short_recheck_required` | `train_reward` | 0.36619046 | `TRAIN_REWARD_FOLLOWUP_READY` | `CSRank(Sub(Neg(CSRank(StateDwell($evt_uplimit_active,40))),CSRank(ValidRatioGate($ctx_hfq_pe_ttm,60,0.5))))` |
| 4 | `phase3cp_03454` | `legacy_assume_long_short_recheck_required` | `train_reward` | 0.33203298 | `TRAIN_REWARD_FOLLOWUP_READY` | `Neg(CSRank(MaskedZScore($ctx_ths_hot_last_pct,60,0.8)))` |
| 5 | `phase3cp_14118` | `legacy_assume_long_short_recheck_required` | `train_reward` | 0.19975108 | `TRAIN_REWARD_FOLLOWUP_READY` | `CSRank(Mul(Sign(ZScore(Sub(Div(Sub($close,$low),Add(Abs(Sub($high,$low)),0.000001)),Mean(Div(Sub($close,$low),Add(Abs(Su` |
| 6 | `phase3cp_14250` | `legacy_assume_long_short_recheck_required` | `train_reward` | 0.19970343 | `TRAIN_REWARD_FOLLOWUP_READY` | `CSRank(Mul(Sign(ZScore(Div(Sub($m1_first5_last_close,$open),Add(Abs($open),0.000001)))),ZScore(Div(Mean($amount,10),Add(` |
| 7 | `phase3cp_14182` | `legacy_assume_long_short_recheck_required` | `train_reward` | 0.19970098 | `TRAIN_REWARD_FOLLOWUP_READY` | `CSRank(Mul(Sign(ZScore(Sub(Div(Sub($close,$low),Add(Abs(Sub($high,$low)),0.000001)),Mean(Div(Sub($close,$low),Add(Abs(Su` |
| 8 | `phase3cp_14186` | `legacy_assume_long_short_recheck_required` | `train_reward` | 0.19970098 | `TRAIN_REWARD_FOLLOWUP_READY` | `CSRank(Mul(Sign(ZScore(Sub(Div(Sub($close,$low),Add(Abs(Sub($high,$low)),0.000001)),Mean(Div(Sub($close,$low),Add(Abs(Su` |
| 9 | `phase3cp_14224` | `legacy_assume_long_short_recheck_required` | `train_reward` | 0.19970098 | `TRAIN_REWARD_FOLLOWUP_READY` | `CSRank(Mul(Sign(ZScore(Sub(Div(Sub($close,$low),Add(Abs(Sub($high,$low)),0.000001)),Mean(Div(Sub($close,$low),Add(Abs(Su` |
| 10 | `phase3cp_14234` | `legacy_assume_long_short_recheck_required` | `train_reward` | 0.19970098 | `TRAIN_REWARD_FOLLOWUP_READY` | `CSRank(Mul(Sign(ZScore(Sub(Div(Sub($close,$low),Add(Abs(Sub($high,$low)),0.000001)),Mean(Div(Sub($close,$low),Add(Abs(Su` |
| 11 | `phase3cp_00021` | `legacy_assume_long_short_recheck_required` | `train_reward` | 0.19904851 | `TRAIN_REWARD_FOLLOWUP_READY` | `CSRank(Sub(ZScore(Sub(Div(Sub($close,$low),Add(Abs(Sub($high,$low)),0.000001)),Mean(Div(Sub($close,$low),Add(Abs(Sub($hi` |
| 12 | `phase3cp_00018` | `legacy_assume_long_short_recheck_required` | `train_reward` | 0.19847395 | `TRAIN_REWARD_FOLLOWUP_READY` | `CSRank(Mul(ZScore(Div($m1_first5_amount,Add(Abs($amount),0.000001))),ZScore(Std(Div(Sub($high,$low),Add(Abs($open),0.000` |
| 13 | `phase3cp_00026` | `legacy_assume_long_short_recheck_required` | `train_reward` | 0.19828243 | `TRAIN_REWARD_FOLLOWUP_READY` | `CSRank(Sub(ZScore(Div($m1_first30_amount,Add(Abs($amount),0.000001))),ZScore(Std(Div(Sub($high,$low),Add(Abs($open),0.00` |
| 14 | `phase3cp_00002` | `legacy_assume_long_short_recheck_required` | `train_reward` | 0.19795411 | `TRAIN_REWARD_FOLLOWUP_READY` | `CSRank(Sub(ZScore(Div($m1_first5_amount,Add(Abs($amount),0.000001))),ZScore(Std(Div(Sub($high,$low),Add(Abs($open),0.000` |
| 15 | `phase3cp_00009` | `legacy_assume_long_short_recheck_required` | `train_reward` | 0.19757107 | `TRAIN_REWARD_FOLLOWUP_READY` | `CSRank(Sub(ZScore(Div($m1_first15_amount,Add(Abs($amount),0.000001))),ZScore(Std(Div(Sub($high,$low),Add(Abs($open),0.00` |
| 16 | `phase3cp_00013` | `legacy_assume_long_short_recheck_required` | `train_reward` | 0.19753824 | `TRAIN_REWARD_FOLLOWUP_READY` | `CSRank(Mul(ZScore($m1_first30_vwap_return_vs_open),ZScore(Std(Div(Sub($high,$low),Add(Abs($open),0.000001)),30))))` |
| 17 | `phase3cp_00024` | `legacy_assume_long_short_recheck_required` | `train_reward` | 0.19751635 | `TRAIN_REWARD_FOLLOWUP_READY` | `CSRank(Sub(ZScore(Sub(Div(Sub($close,$low),Add(Abs(Sub($high,$low)),0.000001)),Mean(Div(Sub($close,$low),Add(Abs(Sub($hi` |
| 18 | `phase3cp_13121` | `legacy_assume_long_short_recheck_required` | `phase3ca_proxy_quality` | 0.25408516 | `` | `CSRank(Mul(ZScore(Div($m1_first5_amount,Add(Abs($amount),0.000001))),ZScore(Delta($vwap,15))))` |
| 19 | `phase3cp_13122` | `legacy_assume_long_short_recheck_required` | `phase3ca_proxy_quality` | 0.25406352 | `` | `CSRank(Mul(ZScore(Div($m1_first5_amount,Add(Abs($amount),0.000001))),ZScore(Delta($vwap,20))))` |
| 20 | `phase3cp_13125` | `legacy_assume_long_short_recheck_required` | `phase3ca_proxy_quality` | 0.25388385 | `` | `CSRank(Sub(ZScore(Div($m1_first5_amount,Add(Abs($amount),0.000001))),ZScore(Delta($vwap,10))))` |
| 21 | `phase3cp_13123` | `legacy_assume_long_short_recheck_required` | `phase3ca_proxy_quality` | 0.25161271 | `` | `CSRank(Sub(ZScore(Div($m1_first5_amount,Add(Abs($amount),0.000001))),ZScore(Delta($vwap,8))))` |
| 22 | `phase3cp_16305` | `legacy_assume_long_short_recheck_required` | `phase3ca_proxy_quality` | 0.24786995 | `` | `CSRank(Mul(Sign(ZScore(Div(Sub($m1_first30_vwap,$open),Add(Abs($open),0.000001)))),ZScore(Std(Div(Sub($high,$low),Add(Ab` |
| 23 | `phase3cp_16292` | `legacy_assume_long_short_recheck_required` | `phase3ca_proxy_quality` | 0.24782988 | `` | `CSRank(Sub(ZScore(Div(Sub($m1_first30_vwap,$open),Add(Abs($open),0.000001))),Mean(ZScore(Div(Mean($vwap,10),Add(Abs(Mean` |
| 24 | `phase3cp_16294` | `legacy_assume_long_short_recheck_required` | `phase3ca_proxy_quality` | 0.24736681 | `` | `Neg(CSRank(Mul(ZScore($m1_first15_vwap_return_vs_open),ZScore(Delta($vwap,8)))))` |
| 25 | `phase3cp_13168` | `legacy_assume_long_short_recheck_required` | `phase3ca_proxy_quality` | 0.24722754 | `` | `CSRank(Sub(ZScore($m1_first5_vwap_return_vs_open),ZScore(Delta($vwap,30))))` |
| 26 | `phase3cp_16355` | `legacy_assume_long_short_recheck_required` | `phase3ca_proxy_quality` | 0.247016 | `` | `CSRank(Sub(ZScore(Div($m1_first15_amount,Add(Abs($amount),0.000001))),Mean(ZScore(Delta($vwap,20)),3)))` |
| 27 | `phase3cp_16322` | `legacy_assume_long_short_recheck_required` | `phase3ca_proxy_quality` | 0.24694942 | `` | `Neg(CSRank(Mul(ZScore(Delta($intraday_ret_from_open,20)),ZScore(Delta($volume,30)))))` |
| 28 | `phase3cp_16369` | `legacy_assume_long_short_recheck_required` | `phase3ca_proxy_quality` | 0.24683834 | `` | `CSRank(Sub(ZScore(Div($m1_first5_range,Add(Abs($open),0.000001))),Mean(ZScore(Delta($vwap,30)),3)))` |
| 29 | `phase3cp_13179` | `legacy_assume_long_short_recheck_required` | `phase3ca_proxy_quality` | 0.24683306 | `` | `CSRank(Sub(ZScore(Delta($intraday_ret_from_open,10)),ZScore(Delta($volume,30))))` |
| 30 | `phase3cp_16338` | `legacy_assume_long_short_recheck_required` | `phase3ca_proxy_quality` | 0.24672545 | `` | `Neg(CSRank(Mul(ZScore(Delta($intraday_ret_from_open,15)),ZScore(Delta($vol,30)))))` |

## Boundary

- `long_short_spread_proxy_only`: valid only as ranking/orthogonality diagnostic.
- `legacy_assume_long_short_recheck_required`: missing explicit long-only proof; treat as invalid for CN reward until rechecked.
- `cn_long_only_verified` / `likely_cn_long_only`: acceptable lineage class, still not automatic alpha promotion.

## Sample Risky Files

| policy | file | reason |
|---|---|---|
| `legacy_assume_long_short_recheck_required` | `reports\PHASE3BZ_TO_PHASE3CA_SEARCH_DEFINITION_20260616.md` | risky phase markdown without long-only marker |
| `legacy_assume_long_short_recheck_required` | `reports\PHASE3CM_INCREMENTAL_CHECKPOINT_FIX_20260623.md` | risky phase markdown without long-only marker |
| `long_short_spread_proxy_only` | `reports\PHASE3CM_TRAIN_SORTINO_REWARD_CHAIN_20260623.md` | markdown mentions top/bottom or long-short |
| `legacy_assume_long_short_recheck_required` | `reports\phase3cl_bz_fragment_replay_20260622_batch1\phase3bz_candidate_fragment_summary.csv` | Phase3CM/BZ/EG/EH artifact lacks explicit long-only proof |
| `long_short_spread_proxy_only` | `reports\phase3cl_bz_fragment_replay_20260622_batch1\PHASE3BZ_FRAGMENT_REPLAY_AUDIT_20260616.md` | markdown mentions top/bottom or long-short |
| `legacy_assume_long_short_recheck_required` | `reports\phase3cl_bz_fragment_replay_20260622_batch1\phase3bz_fragment_replay_summary.json` | risky phase json without long-only marker |
| `legacy_assume_long_short_recheck_required` | `reports\phase3cl_bz_fragment_replay_20260622_batch2\phase3bz_candidate_fragment_summary.csv` | Phase3CM/BZ/EG/EH artifact lacks explicit long-only proof |
| `long_short_spread_proxy_only` | `reports\phase3cl_bz_fragment_replay_20260622_batch2\PHASE3BZ_FRAGMENT_REPLAY_AUDIT_20260616.md` | markdown mentions top/bottom or long-short |
| `legacy_assume_long_short_recheck_required` | `reports\phase3cl_bz_fragment_replay_20260622_batch2\phase3bz_fragment_replay_summary.json` | risky phase json without long-only marker |
| `legacy_assume_long_short_recheck_required` | `reports\phase3cl_bz_fragment_replay_20260622_batch3\phase3bz_candidate_fragment_summary.csv` | Phase3CM/BZ/EG/EH artifact lacks explicit long-only proof |
| `long_short_spread_proxy_only` | `reports\phase3cl_bz_fragment_replay_20260622_batch3\PHASE3BZ_FRAGMENT_REPLAY_AUDIT_20260616.md` | markdown mentions top/bottom or long-short |
| `legacy_assume_long_short_recheck_required` | `reports\phase3cl_bz_fragment_replay_20260622_batch3\phase3bz_fragment_replay_summary.json` | risky phase json without long-only marker |
| `legacy_assume_long_short_recheck_required` | `reports\phase3cl_bz_fragment_replay_20260622_batch4\phase3bz_candidate_fragment_summary.csv` | Phase3CM/BZ/EG/EH artifact lacks explicit long-only proof |
| `long_short_spread_proxy_only` | `reports\phase3cl_bz_fragment_replay_20260622_batch4\PHASE3BZ_FRAGMENT_REPLAY_AUDIT_20260616.md` | markdown mentions top/bottom or long-short |
| `legacy_assume_long_short_recheck_required` | `reports\phase3cl_bz_fragment_replay_20260622_batch4\phase3bz_fragment_replay_summary.json` | risky phase json without long-only marker |
| `legacy_assume_long_short_recheck_required` | `reports\phase3cm_checkpoint_smoke_20260623\phase3cm_candidate_progress.csv` | Phase3CM/BZ/EG/EH artifact lacks explicit long-only proof |
| `legacy_assume_long_short_recheck_required` | `reports\phase3cm_checkpoint_smoke_20260623\phase3cm_candidate_split_horizon_summary.csv` | Phase3CM/BZ/EG/EH artifact lacks explicit long-only proof |
| `legacy_assume_long_short_recheck_required` | `reports\phase3cm_checkpoint_smoke_20260623\phase3cm_candidate_train_reward_summary.csv` | Phase3CM/BZ/EG/EH artifact lacks explicit long-only proof |
| `legacy_assume_long_short_recheck_required` | `reports\phase3cm_checkpoint_smoke_20260623\phase3cm_incremental_checkpoint_summary.json` | risky phase json without long-only marker |
| `legacy_assume_long_short_recheck_required` | `reports\phase3cm_checkpoint_smoke_20260623\phase3cm_incremental_shard_meta.csv` | Phase3CM/BZ/EG/EH artifact lacks explicit long-only proof |
| `long_short_spread_proxy_only` | `reports\phase3cm_checkpoint_smoke_20260623\PHASE3CM_TRAIN_PORTFOLIO_SORTINO_REWARD_AUDIT_20260623.md` | markdown mentions top/bottom or long-short |
| `legacy_assume_long_short_recheck_required` | `reports\phase3cm_checkpoint_smoke_20260623\phase3cm_train_reward.csv` | Phase3CM/BZ/EG/EH artifact lacks explicit long-only proof |
| `legacy_assume_long_short_recheck_required` | `reports\phase3cm_checkpoint_smoke_20260623\phase3cm_train_reward_audit_summary.json` | risky phase json without long-only marker |
| `legacy_assume_long_short_recheck_required` | `reports\phase3cm_checkpoint_smoke_20260623\phase3cm_train_reward_partial.csv` | Phase3CM/BZ/EG/EH artifact lacks explicit long-only proof |
| `legacy_assume_long_short_recheck_required` | `reports\phase3cm_from_phase3ct_augmented_hybrid_r3b_20260625\phase3cm_candidate_progress.csv` | Phase3CM/BZ/EG/EH artifact lacks explicit long-only proof |
| `legacy_assume_long_short_recheck_required` | `reports\phase3cm_from_phase3ct_augmented_hybrid_r3b_20260625\phase3cm_candidate_split_horizon_summary.csv` | Phase3CM/BZ/EG/EH artifact lacks explicit long-only proof |
| `legacy_assume_long_short_recheck_required` | `reports\phase3cm_from_phase3ct_augmented_hybrid_r3b_20260625\phase3cm_candidate_train_reward_summary.csv` | Phase3CM/BZ/EG/EH artifact lacks explicit long-only proof |
| `legacy_assume_long_short_recheck_required` | `reports\phase3cm_from_phase3ct_augmented_hybrid_r3b_20260625\phase3cm_incremental_checkpoint_summary.json` | risky phase json without long-only marker |
| `legacy_assume_long_short_recheck_required` | `reports\phase3cm_from_phase3ct_augmented_hybrid_r3b_20260625\phase3cm_incremental_shard_meta.csv` | Phase3CM/BZ/EG/EH artifact lacks explicit long-only proof |
| `long_short_spread_proxy_only` | `reports\phase3cm_from_phase3ct_augmented_hybrid_r3b_20260625\PHASE3CM_TRAIN_PORTFOLIO_SORTINO_REWARD_AUDIT_20260623.md` | markdown mentions top/bottom or long-short |
| `legacy_assume_long_short_recheck_required` | `reports\phase3cm_from_phase3ct_augmented_hybrid_r3b_20260625\phase3cm_train_reward.csv` | Phase3CM/BZ/EG/EH artifact lacks explicit long-only proof |
| `legacy_assume_long_short_recheck_required` | `reports\phase3cm_from_phase3ct_augmented_hybrid_r3b_20260625\phase3cm_train_reward_audit_summary.json` | risky phase json without long-only marker |
| `legacy_assume_long_short_recheck_required` | `reports\phase3cm_from_phase3ct_augmented_hybrid_r3b_20260625\phase3cm_train_reward_partial.csv` | Phase3CM/BZ/EG/EH artifact lacks explicit long-only proof |
| `legacy_assume_long_short_recheck_required` | `reports\phase3cm_recover_local_challenger_cem_q20_20260623\phase3cm_candidate_progress.csv` | Phase3CM/BZ/EG/EH artifact lacks explicit long-only proof |
| `legacy_assume_long_short_recheck_required` | `reports\phase3cm_recover_local_challenger_cem_q20_20260623\phase3cm_incremental_checkpoint_summary.json` | risky phase json without long-only marker |
| `legacy_assume_long_short_recheck_required` | `reports\phase3cm_recover_local_challenger_cem_q20_20260623\phase3cm_incremental_shard_meta.csv` | Phase3CM/BZ/EG/EH artifact lacks explicit long-only proof |
| `legacy_assume_long_short_recheck_required` | `reports\phase3cm_recover_local_challenger_cem_q20_20260623\phase3cm_train_reward_partial.csv` | Phase3CM/BZ/EG/EH artifact lacks explicit long-only proof |
| `legacy_assume_long_short_recheck_required` | `reports\phase3cm_recover_local_rx_typed_q20_20260623\phase3cm_candidate_progress.csv` | Phase3CM/BZ/EG/EH artifact lacks explicit long-only proof |
| `legacy_assume_long_short_recheck_required` | `reports\phase3cm_recover_local_rx_typed_q20_20260623\phase3cm_incremental_checkpoint_summary.json` | risky phase json without long-only marker |
| `legacy_assume_long_short_recheck_required` | `reports\phase3cm_recover_local_rx_typed_q20_20260623\phase3cm_incremental_shard_meta.csv` | Phase3CM/BZ/EG/EH artifact lacks explicit long-only proof |
| `legacy_assume_long_short_recheck_required` | `reports\phase3cm_recover_local_rx_typed_q20_20260623\phase3cm_train_reward_partial.csv` | Phase3CM/BZ/EG/EH artifact lacks explicit long-only proof |
| `legacy_assume_long_short_recheck_required` | `reports\phase3cn_integrated_feedback_smoke_20260623\phase3cm_reward_fixture\phase3cm_candidate_train_reward_summary.csv` | Phase3CM/BZ/EG/EH artifact lacks explicit long-only proof |
| `legacy_assume_long_short_recheck_required` | `reports\phase3cn_integrated_feedback_smoke_20260623\phase3cm_reward_fixture\phase3cm_train_reward.csv` | Phase3CM/BZ/EG/EH artifact lacks explicit long-only proof |
| `legacy_assume_long_short_recheck_required` | `reports\phase3cn_integrated_feedback_smoke_20260623\phase3cm_reward_fixture\phase3cm_train_reward_audit_summary.json` | risky phase json without long-only marker |
| `legacy_assume_long_short_recheck_required` | `reports\phase3code_verify_bz_smoke_20260618\phase3bz_candidate_fragment_summary.csv` | Phase3CM/BZ/EG/EH artifact lacks explicit long-only proof |
| `long_short_spread_proxy_only` | `reports\phase3code_verify_bz_smoke_20260618\PHASE3BZ_FRAGMENT_REPLAY_AUDIT_20260616.md` | markdown mentions top/bottom or long-short |
| `legacy_assume_long_short_recheck_required` | `reports\phase3code_verify_bz_smoke_20260618\phase3bz_fragment_replay_summary.json` | risky phase json without long-only marker |
| `legacy_assume_long_short_recheck_required` | `reports\phase3cp_event_state_topq30_followup_confirm_20260623\phase3cm_train_reward\phase3cm_candidate_split_horizon_summary.csv` | Phase3CM/BZ/EG/EH artifact lacks explicit long-only proof |
| `legacy_assume_long_short_recheck_required` | `reports\phase3cp_event_state_topq30_followup_confirm_20260623\phase3cm_train_reward\phase3cm_candidate_train_reward_summary.csv` | Phase3CM/BZ/EG/EH artifact lacks explicit long-only proof |
| `long_short_spread_proxy_only` | `reports\phase3cp_event_state_topq30_followup_confirm_20260623\phase3cm_train_reward\PHASE3CM_TRAIN_PORTFOLIO_SORTINO_REWARD_AUDIT_20260623.md` | markdown mentions top/bottom or long-short |
