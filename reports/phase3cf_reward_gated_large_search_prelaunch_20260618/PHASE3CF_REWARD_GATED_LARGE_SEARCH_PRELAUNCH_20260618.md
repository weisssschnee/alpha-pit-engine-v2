# Phase3CF Reward-Gated Large Search Prelaunch

Decision: `PHASE3CF_LARGE_SEARCH_PRELAUNCH_READY`

## Evidence

| name | exists | path |
|---|---:|---|
| ce1_summary | True | `G:\Project_V7_Rotation\alpha_pit_true1min_engine_20260619\reports\PHASE3CE1_TYPED_PRIMITIVE_GATE_IMPLEMENTATION_SUMMARY_20260618.md` |
| ce2_fullwidth_eval_summary | True | `G:\Project_V7_Rotation\alpha_pit_true1min_engine_20260619\reports\phase3ce2_fullwidth_realdata_eval_20260618\phase3as_true_1min_sidecar_canary_eval_summary.json` |
| ce2_fullwidth_compact_summary | True | `G:\Project_V7_Rotation\alpha_pit_true1min_engine_20260619\reports\phase3ce2_fullwidth_realdata_eval_20260618\phase3ce2_fullwidth_eval_compact_summary.csv` |
| ce2_fullwidth_panel_summary | True | `G:\Project_V7_Rotation\alpha_pit_true1min_engine_20260619\reports\phase3ce2_fullwidth_validation_panel_20260618\phase3ce2_fullwidth_validation_panel_summary.json` |
| ce2_fullwidth_still_blocked | True | `G:\Project_V7_Rotation\alpha_pit_true1min_engine_20260619\runtime\phase3ce2_fullwidth_phase3ar_sidecar_adapter_20260618\phase3ar_still_blocked_formula_rows.json` |
| typed_gate_contract | True | `G:\Project_V7_Rotation\alpha_pit_true1min_engine_20260619\reports\PHASE3CE1_TYPED_PRIMITIVE_GATE_CONTRACT_20260618.md` |
| search_memory_blocked_view | True | `G:\Project_V7_Rotation\alpha_pit_true1min_engine_20260619\reports\phase3ce1_search_memory_blocked_view_20260618\phase3ce1_search_memory_blocked_view_summary.json` |

## Launch Order

1. `phase3bs-adaptive-ucb-cem-practice`
2. `phase3bt-ast-algorithm-bakeoff`
3. `phase3ca-build-bz-candidate-audit`
4. `phase3bz-fragment-replay-audit`

## Boundaries

- true `trade_time` 1min shards only
- no old 1D/TDX official backbone
- X0/R3 read-only
- proxy metrics are not promotion evidence
- BZ fragment replay is mandatory before follow-up candidates

## Command Manifest

`G:\Project_V7_Rotation\alpha_pit_true1min_engine_20260619\runtime\phase3cf_reward_gated_large_search_prelaunch_20260618\phase3cf_command_manifest.json`
