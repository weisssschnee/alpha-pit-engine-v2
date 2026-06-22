# Phase3CL True1min Fragment Replay Audit 2026-06-22

## Decision

`HOLD_RESEARCH_FRAGMENT_REPLAY_AUDIT_COMPLETE`

No candidate from the current top32 proxy-ranked queue passed fragment replay.

This result does not promote any alpha, does not modify X0/R3, and does not justify scaling the same proxy reward family.

## Inputs

- Local search reports:
  - `reports/phase3ck_local_true1min_cp_retry_2lane_20260622`
  - `reports/phase3ch_local_fastpath_checkpoint_3lane_20260621`
- Pulled company search reports:
  - `G:/Chengbo/runtime/pulled_company_phase3ch_reports_20260622/phase3ch_company_fastpath_fresh_2lane_20260621`
- Candidate bridge:
  - `reports/phase3cl_bz_candidate_audit_20260622/phase3ca_bz_candidate_audit.csv`

## Candidate Bridge

The BZ bridge selected 32 candidates from 201 deduplicated source candidates.

Hard-rejected before replay:

- `future_signal_wrong_lag_too_strong`: 1504 rows
- `signal_corr_abs`: 485 rows

This confirms that most high proxy scores were still coming from wrong-lag or crowded structures.

## Fragment Replay Runs

All runs used true `trade_time` 1min shards:

`G:/Project_V7_Rotation/alpha_pit_data_feature_workspace_20260531/runtime/phase3au_aq_only_true1min_sharded_20260611`

Replay settings:

- 4 shards
- 96 sampled trade times per shard
- horizons: 1, 5, 15, 30 minutes
- cost: 5 bps
- top quantile: 0.2
- fast mode enabled
- pyarrow column/time filtering active
- per-shard expression cache active
- numexpr threads: 4
- global worker limit: 1

Outputs:

- `reports/phase3cl_bz_fragment_replay_20260622_batch1`
- `reports/phase3cl_bz_fragment_replay_20260622_batch2`
- `reports/phase3cl_bz_fragment_replay_20260622_batch3`
- `reports/phase3cl_bz_fragment_replay_20260622_batch4`

## Aggregate Result

- candidates replayed: 32
- total fragments: 44,912
- followups: 0
- best fragment Sortino: -0.04608804
- worst fragment Sortino: -0.60469318
- best MCMC median Sortino: -0.04690108
- max day-block probability Sortino > 0: 0.305

The best-ranked replayed candidate still failed:

`CSRank(Sub(ZScore(Delta($intraday_ret_from_open,5)),ZScore(Div($m1_first30_vol,Add(Abs($volume),0.000001)))))`

Its replay result:

- fragment Sortino: -0.04608804
- MCMC median Sortino: -0.04690108
- day-block probability Sortino > 0: 0.305
- turnover: 0.78133759
- blockers: `weak_fragment_mcmc|weak_day_block_mcmc|extreme_turnover`

## Interpretation

The proxy search is still over-rewarding structures that do not survive tradable minute-fragment replay.

Observed failure modes:

- CEM improves proxy IC but concentrates on fragile structures.
- `intraday_ret_from_open` / `range_location` / volume-divergence structures dominate top proxy ranks.
- Many high proxy candidates are either wrong-lag blocked, signal-crowded, or high-turnover.
- Even the cleaner `opening_amount x volatility_state` family produced negative fragment Sortino under sampled true1min replay.

The current top32 queue should remain diagnostic only.

## Next Search Contract

Do not scale the same proxy reward as-is.

Required changes before the next large run:

- downweight or quarantine intraday-return/range-location proxy winners unless they pass fragment replay early;
- move BZ fragment replay earlier as an inner-loop checkpoint for small elite batches;
- add turnover-aware reward pressure before CEM feedback;
- require day-block stability in the reward summary, not only aligned IC;
- preserve a fresh-search allocation, but prevent CEM from repeatedly exploiting the same high-proxy/negative-fragment families.

## Running State At Audit Close

Local `phase3ck_local_true1min_cp_retry_2lane_20260622` was still running with 2 actual Python workers.

Company `phase3ch_company_fastpath_fresh_2lane_20260621` was still running with reports through round3 and no stderr.
