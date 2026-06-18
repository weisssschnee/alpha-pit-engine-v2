# Phase3AS True 1min Sidecar Canary Eval

decision: `PHASE3AS_TRUE_1MIN_SIDECAR_CANARY_EVAL_COMPLETE`

## Counts

- input candidates: `104`
- evaluated candidates: `104`
- memory hits: `0`
- errors: `0`
- panel rows: `1231760`
- panel codes: `5134`
- evaluated trade_time groups: `240`

## Hard Rules

- cross-section key is `trade_time`, not `date`.
- labels are future 1min-bar returns by `code`.
- Phase3AR sidecars must already be PIT/cutoff materialized.
- search-memory hits are tagged, not treated as fresh alpha.
- X0/R3 remains read-only.

## Outputs

- rows: `G:\Project_V7_Rotation\alpha_pit_data_feature_workspace_20260531\runtime\phase3ce2_fullwidth_realdata_eval_20260618\phase3as_true_1min_sidecar_canary_eval_rows.csv`
- errors: `G:\Project_V7_Rotation\alpha_pit_data_feature_workspace_20260531\runtime\phase3ce2_fullwidth_realdata_eval_20260618\phase3as_true_1min_sidecar_canary_eval_errors.csv`
- summary: `G:\Project_V7_Rotation\alpha_pit_data_feature_workspace_20260531\runtime\phase3ce2_fullwidth_realdata_eval_20260618\phase3as_true_1min_sidecar_canary_eval_summary.json`
