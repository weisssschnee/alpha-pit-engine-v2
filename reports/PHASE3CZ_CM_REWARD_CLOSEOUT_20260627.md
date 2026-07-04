# Phase3CZ CM Reward Closeout 2026-06-27

## Decision

`PHASE3CZ_PROXY_TOP_REJECTED_BY_TRAIN_PORTFOLIO_SORTINO`

The Phase3CZ proxy/IC-selected top candidates from LAN 77 and the interrupted local run were successfully converted into a clean candidate audit, evaluated by Phase3CM train portfolio Sortino reward, and written into Phase3CN feedback memory.

No candidate passed the train reward follow-up gate.

## Inputs

- LAN 77 BT report: `G:\Chengbo\runtime\lan77_report_pull_phase3cz_20260627\phase3cz_lan77_bt_feedback_after_bu_20260627`
- LAN 77 BS report: `G:\Chengbo\runtime\lan77_report_pull_phase3cz_20260627\phase3cz_lan77_bs_feedback_after_bu_20260627`
- Local partial BT report: `reports\phase3cz_local_bt_feedback_guarded_20260627`
- Combined candidate audit: `reports\phase3cz_combined_candidate_audit_20260627\phase3ca_bz_candidate_audit.csv`
- Clean CM input: `reports\phase3cz_combined_candidate_audit_20260627_clean_for_cm\phase3ca_bz_candidate_audit.csv`

## Candidate Filtering

The combined audit selected `128` candidates from `263` deduped source candidates.

Hard rejected before reward:

- `future_signal_wrong_lag_too_strong`: `1925`
- `signal_corr_abs`: `139`

Additional CM input hygiene:

- input rows: `128`
- clean rows sent to CM: `50`
- removed zero-signal rows: `75`
- removed blank-IC rows: `78`

This matters because many context/SafeCSResidual candidates had `signal_nonnull_sum=0`; those are data-coverage failures, not valid reward candidates.

## Phase3CM Reward Audit

Report root:

`reports\phase3cz_lan77_cm_train_reward_20260627`

Run parameters:

- candidates: `50`
- shard root: `D:\ChengboRemote\data\phase3cy_true1min_sidecar_augmented_shards_20260626`
- max shards requested: `8`
- discovered shard count in checkpoint: `3`
- sample trade times per shard: `240`
- horizons: `1,5,15,30`
- train / validation / holdout: `0.60 / 0.20 / 0.20`
- cost: `5 bps`
- top quantile: `0.2`
- fast mode: `true`
- numexpr / OMP / MKL threads: `8`
- checkpointing: enabled

Acceleration contract recorded by Phase3CM:

- batched shard read: `true`
- column-pruned pyarrow read: `true`
- per-shard expression cache: `true`
- fast group rank: `true`

Result:

- candidate_count: `50`
- followup_count: `0`
- decision: `PHASE3CM_TRAIN_REWARD_AUDIT_READY_DIAGNOSTIC_ONLY`

Top train rewards were still negative:

| rank | candidate | hash | arm | fields | train_reward | train_sortino | validation_sortino | holdout_sortino |
|---:|---|---|---|---|---:|---:|---:|---:|
| 1 | `phase3bt_00187` | `07431237b59862851bc67767` | `cem_exploit` | `m1_first5_last_close|open|vwap` | `-0.57254354` | `-0.22874274` | `-0.47712122` | `-0.49822398` |
| 2 | `phase3bt_00309` | `5e7f3069ea2e592a015ce287` | `typed_ast_fresh` | `m1_first15_last_close|open|vwap` | `-0.59836172` | `-0.23743163` | `-0.40960553` | `-0.48201666` |
| 3 | `phase3bt_00368` | `fd52b26ce157634cc6cd1131` | `typed_ast_fresh` | `m1_first30_vwap|open|vwap` | `-0.64948784` | `-0.29341612` | `-0.38610343` | `-0.50625015` |

## Phase3CN Feedback Memory

Report root:

`reports\phase3cz_cn_feedback_after_cm_20260627`

Result:

- candidate_count: `50`
- arm_count: `2`
- family_count: `15`
- clean_feedback_count: `0`
- blocked_family_count: `15`
- exploit_allowed_family_count: `0`
- validation usage: report-only
- optimizer reward source: train-only Phase3CM

Arm table:

| arm | candidates | clean | update_allowed | positive_train_reward_rate |
|---|---:|---:|---|---:|
| `cem_exploit` | `29` | `0` | `false` | `0.0` |
| `typed_ast_fresh` | `21` | `0` | `false` | `0.0` |

Best family medians were still negative:

| field_family | candidates | clean | median_train_reward |
|---|---:|---:|---:|
| `opening_state` | `1` | `0` | `-0.57254354` |
| `opening_state` | `1` | `0` | `-0.59836172` |
| `opening_state` | `1` | `0` | `-0.64948784` |
| `range_location` | `1` | `0` | `-0.79002319` |
| `range_location` | `6` | `0` | `-0.82821923` |

## Interpretation

The proxy search did find high IC / clean-looking minute candidates, especially:

- opening-state versus VWAP curve spreads
- range-location products
- range-location plus amount/volume interactions

However, when evaluated as a continuous train portfolio Sortino objective with costs, all tested candidates lost money on the train reward metric. This confirms the earlier concern: proxy IC and search quality score are not sufficient optimizer rewards for this line.

The next search must consume the CN feedback memory and should not exploit these families until a structurally different formulation passes train reward.

## Required Next Search Policy

Use:

- `reports\phase3cz_cn_feedback_after_cm_20260627\phase3cn_search_feedback_memory.csv`
- `reports\phase3cz_cn_feedback_after_cm_20260627\phase3cn_arm_score_table.csv`
- `reports\phase3cz_cn_feedback_after_cm_20260627\phase3cn_family_score_table.csv`
- `reports\phase3cz_cn_feedback_after_cm_20260627\phase3cn_blocked_family_table.csv`
- `reports\phase3cz_cn_feedback_after_cm_20260627\phase3cn_exploit_allowed_family_table.csv`

Policy:

- do not treat Phase3CZ proxy score as reward;
- cap or block exploit around `opening_state`, `range_location`, and `flow_amount_volume+range_location` variants until new train reward evidence exists;
- keep fresh exploration budget, but require Phase3CM train reward before feedback update;
- investigate why context/SafeCSResidual candidates have zero signal coverage before using those fields in search.
