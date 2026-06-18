# Phase3CE1 Typed Primitive Gate Implementation Summary

Status: implemented as diagnostic/construction-time gate. No search launched. No official X0/R3 mutation.

## Implemented

Shared validator:

```text
src/our_system_phase2/services/typed_primitive_gate.py
```

Connected entry points:

```text
Phase3R limit diagnostic candidate materialization
Factor-pack / shared-pool preflight
Advisory field-registry outputs
```

Memory policy remains as designed:

```text
do not delete unsafe memory keys
reclassify as blocked_unsafe_known_structure in the future blocked memory view
construction-time validator remains the primary safety mechanism
```

## Smoke Results

Phase3R typed gate smoke:

```text
output: reports/phase3ce1_phase3r_typed_gate_smoke_20260618
pre_memory_candidate_template_count: 18
typed_gate_allowed_count: 2
typed_gate_reject_count: 16
allowed roles: r3_secondary_gate
```

Reject breakdown:

```text
blocked_unsafe_known_structure + Mean: 8
blocked_unsafe_known_structure + Mean|ZScore: 6
blocked_unsafe_known_structure + CSResidual|Mean|ZScore: 2
```

Factor-pack preflight gate smoke:

```text
output: reports/phase3ce1_factor_pack_preflight_gate_smoke_20260618
decision: HOLD_TYPED_PRIMITIVE_GATE_BLOCKS
factor_pack_rows: 760
typed_gate_blocked_rows: 757
```

Violation breakdown:

```text
blocked_unsafe_known_structure: 725
reject_membership_key_formula_input: 32
```

Advisory registry gate smoke:

```text
output: runtime/phase3ce1_field_registry_gate_smoke_20260618
typed_gate_factor_candidate_blocked_count: 2901
typed_gate_selector_selected_blocked_count: 1067
```

Advisory gate audit breakdown:

```text
blocked_unsafe_known_structure: 2221
reject_membership_key_formula_input: 166
require_typed_rewrite: 1581
```

## Interpretation

CE1 confirms the unsafe limit/event/direct-factor issue is not only a historical reporting issue. Once the gate is active, the direct-event/factor-pack line is mostly blocked and cannot be treated as ordinary formula-ready search input.

The next implementation step is the search-memory blocked view:

```text
load inherited memory
run the same typed validator on memory records
keep expression/skeleton keys
mark unsafe rows as blocked_unsafe_known_structure
exclude unsafe rows from positive source/reward attribution
```

Only after that should a typed primitive canary be run.

## Search Memory Blocked View

Implemented:

```text
src/our_system_phase2/runtime/phase3ce1_search_memory_blocked_view.py
app.py phase3ce1-search-memory-blocked-view --allow-diagnostic
```

Smoke result:

```text
output: runtime/phase3ce1_search_memory_blocked_view_20260618
report: reports/phase3ce1_search_memory_blocked_view_20260618

memory_entry_count: 3677
positive_record_count: 3067
blocked_record_count: 610
blocked_expression_key_count: 313
blocked_skeleton_key_count: 16
```

Decision counts:

```text
allow: 3067
blocked_unsafe_known_structure: 344
require_typed_rewrite: 266
```

Policy:

```text
historical memory files are not mutated
unsafe keys are preserved in active_duplicate_block_keys
unsafe rows are excluded from positive_memory_view
construction-time validator remains the primary safety mechanism
```

## G2 Selector Input Gate

Implemented:

```text
src/our_system_phase2/services/typed_primitive_gate.py
src/our_system_phase2/runtime/phase3aa_apply_mature_g2_selector.py
src/our_system_phase2/runtime/phase3ce1_g2_input_gate_smoke.py
app.py phase3ce1-g2-input-gate-smoke --allow-diagnostic
```

Runtime behavior:

```text
phase3aa_apply_mature_g2_selector now runs the typed primitive gate before pool prefilter and mature G2 selection
only g2_input_gate_decision=allow rows enter the selector
rejected rows are written to phase3ce1_g2_input_gate_rejects.csv
the selector run writes phase3ce1_g2_input_gate_summary.json
```

Smoke result:

```text
output: reports/phase3ce1_g2_input_gate_smoke_20260618
input_rows: 760
allowed_rows: 3
rejected_rows: 757
decision: PASS_G2_INPUT_GATE_ACTIVE
```

Rejected decision counts:

```text
blocked_unsafe_known_structure: 725
reject_membership_key_formula_input: 32
```

Rejected reasons:

```text
ordinary continuous primitive consumed sparse event or discrete state field: 725
membership/group fields are context keys until group geometry audit: 32
```

Current CE1 status:

```text
construction-time gate: implemented
Phase3R diagnostic materialization gate: implemented
factor-pack/preflight gate: implemented
advisory registry verdict audit: implemented
search-memory blocked view: implemented without mutating historical memory
G2 selector input gate: implemented

remaining before new-field large search:
typed primitive canary for EventCount/EventAge/StateDwell/ValidRatioGate/MaskedCorr/SafeCSResidual
```

## CE2 Typed Primitive Candidate Canary

Implemented:

```text
src/our_system_phase2/runtime/phase3ce2_typed_primitive_candidate_pack_canary.py
app.py phase3ce2-typed-primitive-canary --allow-diagnostic
```

Smoke command:

```text
app.py phase3ce2-typed-primitive-canary --allow-diagnostic --advisory-rows runtime/phase3ce1_field_registry_gate_smoke_20260618/advisory_registry_gate_audit.csv
```

Smoke result:

```text
output: reports/phase3ce2_typed_primitive_candidate_pack_canary_20260618
source_rows: 4725
candidate_rows: 104
typed_gate_allowed_rows: 104
typed_gate_rejected_rows: 0
g2_input_allowed_rows: 104
g2_input_rejected_rows: 0
decision: PASS_TYPED_PRIMITIVE_ENTRY_PATH_HOLD_EVALUATOR_IMPLEMENTATION
```

Field category attribution:

```text
sparse_event: 32
discrete_state: 24
coverage_sensitive: 48
```

Primitive attribution:

```text
EventAge: 16
EventCount: 16
StateDwell: 12
WindowStateCount: 12
ValidRatioGate: 16
MaskedZScore: 16
SafeCSResidual: 16
```

CE2 stop-condition status:

```text
ce2_01_entry_path: pass
ce2_02_old_primitive_leak: pass
ce2_03_to_ce2_07_runtime_semantic_tests: pending
```

Current blocker:

```text
typed primitive expressions now have a safe candidate/G2 input route
runtime evaluator support for EventCount/EventAge/StateDwell/WindowStateCount/ValidRatioGate/MaskedZScore/SafeCSResidual was added to real_market_validation.evaluate_panel_expression
synthetic true-1min evaluator smoke passed
do not start new-field large search until real-data semantic/placebo/lag/fragment checks exist
```

Evaluator implementation:

```text
src/our_system_phase2/services/real_market_validation.py
app.py phase3ce2-typed-primitive-evaluator-smoke --allow-diagnostic
```

Evaluator smoke result:

```text
output: reports/phase3ce2_typed_primitive_evaluator_smoke_20260618
panel_rows: 64
expression_count: 8
evaluated_expression_count: 8
error_count: 0
failed_check_count: 0
decision: PASS_TYPED_PRIMITIVE_EVALUATOR_SMOKE
```

Semantic checks:

```text
event_age_before_first_event_nan: pass
event_age_resets_on_event: pass
event_count_requires_full_window: pass
valid_ratio_gate_masks_low_coverage: pass
```

Updated CE2 candidate canary:

```text
candidate_rows: 104
typed_gate_allowed_rows: 104
g2_input_allowed_rows: 104
evaluator_support_status: implemented_in_real_market_validation
decision: PASS_TYPED_PRIMITIVE_ENTRY_PATH_EVALUATOR_READY_HOLD_SEMANTIC_PROOF
```

## CE2 Real-Data Evaluator Canary

Field availability against the current Phase3AR true-1min sidecar panel:

```text
panel: runtime/phase3ar_sidecar_field_adapter_20260610/phase3ar_true_1min_sidecar_canary.parquet
unique CE2 typed fields: 44
available in current panel: 17
missing from current panel: 27
availability audit: reports/phase3ce2_typed_primitive_candidate_pack_canary_20260618/typed_primitive_real_panel_field_availability.csv
```

Missing fields include the important limit/open-not-close and high-board direct fields:

```text
limit_up_any_close_not_open_in_t2..t10
limit_up_any_open_not_close_in_t2..t10
high_board_rank
is_market_high_board
turnover_ratio / turnover_ratio_real
fund_ocf_to_assets / fund_cash_to_assets / fund_debt_to_assets / fund_current_ratio / fund_netprofit_margin
ctx_fund_cf_operate_cash_to_netprofit
m1_open_gap_vs_preclose
```

Real-data evaluator canary:

```text
pack: runtime/phase3ce2_typed_primitive_realdata_canary_pack_20260618
output: runtime/phase3ce2_typed_primitive_realdata_eval_20260618
report: reports/phase3ce2_typed_primitive_realdata_eval_20260618
available fields: 17
candidate_count: 41
evaluated_candidate_count: 41
error_count: 0
panel_codes: 6
evaluated_trade_time_count: 240
decision: PHASE3AS_TRUE_1MIN_SIDECAR_CANARY_EVAL_COMPLETE
```

Primitive execution summary on the small real panel:

```text
StateDwell: executed, sparse non-null coverage
WindowStateCount: executed, sparse non-null coverage
ValidRatioGate: executed, broad non-null coverage
MaskedZScore: executed, broad non-null coverage
SafeCSResidual: executed without error but produced no usable IC rows on this 6-code panel
```

Interpretation:

```text
typed primitive evaluator plumbing is now live
current real-data canary proves execution, not alpha quality
full CE2 semantic proof still requires missing limit/high-board fields to be joined into a true-1min sidecar/panel
placebo, lag, and fragment replay remain mandatory before large search
```

## CE2 Field Supplement And 1min Cross-Section Fix

Implemented:

```text
src/our_system_phase2/runtime/phase3ar_sidecar_field_adapter.py
src/our_system_phase2/services/real_market_validation.py
src/our_system_phase2/runtime/phase3ce2_build_event_derived_daily_panel.py
```

Safe field supplements added to Phase3AR sidecar adapter:

```text
turnover_ratio
turnover_ratio_real
fund_cash_to_assets
fund_debt_to_assets
fund_current_ratio
fund_netprofit_margin
ctx_fund_cf_operate_cash_to_netprofit
fund_ocf_to_assets cross-dataset dependency support
m1_open_gap_vs_preclose from true-1min first open vs previous trading-day minute close
limit/high-board event-derived daily state panel as previous-available daily sidecar
```

Real 1min evaluator fix:

```text
CSRank / Rank now use trade_time cross-section when trade_time exists
ZScore / MaskedZScore now use trade_time cross-section when trade_time exists
CSResidual / SafeCSResidual now use trade_time cross-section when trade_time exists
date remains only a fallback for non-minute panels
```

Reason:

```text
true-1min cross-section cannot rank or residualize by date because that mixes all intraday bars in one daily group
```

Event-derived panel repair:

```text
previous default event-derived panel only covered 2026-01-05..2026-05-14
CE2 true-1min canary covers 2025, so event-derived fields previously materialized as all-null
new panel built from HFQ daily 2024-2025:
  runtime/phase3ce2_event_derived_daily_panel_202406_202512/phase3ce2_event_derived_daily_panel.parquet
rows: 2607780
code_count: 5549
date_range: 2024-01-02 .. 2025-12-31
PIT rule in Phase3AR adapter: previous available daily row only; exact same exec_date is not used
```

CE2-specific sidecar materialization:

```text
blocked-row input: runtime/phase3ce2_typed_primitive_candidate_pack_canary_20260618/phase3ce2_typed_primitive_blocked_rows_for_phase3ar.json
output: runtime/phase3ce2_phase3ar_sidecar_adapter_20260618
report: reports/phase3ce2_phase3ar_sidecar_adapter_20260618
augmented panel rows: 351137
augmented panel columns: 56
sidecar_context_formula: 104
still_blocked: 0
smoke errors: 0
```

Newly materialized CE2 fields and coverage:

```text
turnover_ratio / turnover_ratio_real: 1457 non-null daily sidecar rows
fund_cash_to_assets / fund_debt_to_assets / fund_netprofit_margin: 1215 non-null rows
fund_ocf_to_assets: 1215 non-null rows
fund_current_ratio: 972 non-null rows
ctx_fund_cf_operate_cash_to_netprofit: 337 non-null rows
m1_open_gap_vs_preclose: 349691 non-null true-1min rows, 1013 unique values
limit_up_any_close_not_open_in_t2..t10: 351137 non-null true-1min rows, binary state
limit_up_any_open_not_close_in_t2..t10: 351137 non-null true-1min rows, binary state
high_board_rank: 351137 non-null true-1min rows, 7 unique values on CE2 canary
is_market_high_board: 351137 non-null true-1min rows, 1 unique value on CE2 canary
still blocked fields: none for this CE2 typed-primitive canary pack
```

Post-fix real-data evaluator canary:

```text
output: runtime/phase3ce2_realdata_eval_after_field_supplement_xsfix_20260618
report: reports/phase3ce2_realdata_eval_after_field_supplement_xsfix_20260618
candidate_count: 104
evaluated_candidate_count: 104
error_count: 0
memory_hit_count: 0
panel_codes: 6
panel_rows: 14390
evaluated_trade_time_count: 2400
expression_field_count: 45
panel_schema_column_count: 56
decision: PHASE3AS_TRUE_1MIN_SIDECAR_CANARY_EVAL_COMPLETE
```

Selected real-data canary observations:

```text
m1_open_gap_vs_preclose:
  horizon 1 max_signal_nonnull 14096, max_ic_count 2320, mean_ic_mean 0.018491
  horizon 5 max_signal_nonnull 14096, max_ic_count 2346, mean_ic_mean 0.006295

limit_up_any_close_not_open_in_t2:
  horizon 1 max_ic_count 2068, mean_ic_mean -0.030714
  horizon 5 max_ic_count 2105, mean_ic_mean -0.024479

limit_up_any_open_not_close_in_t2:
  horizon 1 max_ic_count 77, mean_ic_mean -0.417920
  horizon 5 max_ic_count 51, mean_ic_mean -0.137805

fund_ocf_to_assets:
  horizon 1 max_signal_nonnull 12000, max_ic_count 2352, mean_ic_mean 0.004209
  horizon 5 max_signal_nonnull 12000, max_ic_count 2393, mean_ic_mean 0.005783

high_board_rank / is_market_high_board:
  sparse/rare on six-code canary; small-sample IC artifacts are diagnostic only
```

Post-fix interpretation:

```text
all 104 CE2 typed primitive canary expressions now materialize and evaluate on the augmented true-1min canary panel
SafeCSResidual remains empty on the 6-code canary because the expression requires min_n=20 per trade_time
small-panel IC values for sparse/discrete state fields are execution diagnostics only, not alpha proof
large search should not be launched from this canary alone
next required validation is full-width CE2 evaluation, because only full-width panels can test SafeCSResidual and high-board/event rarity without six-code artifacts
```

## CE2 Full-Width True-1min Validation

Implemented:

```text
src/our_system_phase2/runtime/phase3ce2_build_fullwidth_validation_panel.py
```

Purpose:

```text
test typed primitives on a wide cross-section instead of the 6-code canary
preserve true trade_time 1min rows
no 1D fallback
no 10min/15min resampling
```

Panel build:

```text
source: runtime/phase3au_aq_only_true1min_sharded_20260611/phase3au_aq_only_shard_manifest.csv
event-date selector: runtime/phase3ce2_event_derived_daily_panel_202406_202512/phase3ce2_event_derived_daily_panel.parquet
selected event dates: 2025-04-10, 2025-04-11
read dates including previous close support: 2025-04-09, 2025-04-10, 2025-04-11
output: runtime/phase3ce2_fullwidth_validation_panel_20260618/phase3ce2_fullwidth_true1min_validation_panel.parquet
rows: 3710677
code_count: 5134
trade_time_count: 723
input_grain: trade_time_1min
```

Adapter performance fix made during full-width validation:

```text
event-derived previous-daily join changed from per-code merge_asof to dense-calendar shifted daily join
cap/HFQ previous-daily join changed from per-code merge_asof to dense-calendar shifted daily join
event-derived sidecar now remains code_exec_date daily grain and is broadcast during augmented merge
reason: full-width panels make per-code merge_asof too slow; dense daily calendars allow equivalent previous-available semantics
```

Full-width Phase3AR materialization:

```text
output: runtime/phase3ce2_fullwidth_phase3ar_sidecar_adapter_20260618
augmented panel: runtime/phase3ce2_fullwidth_phase3ar_sidecar_adapter_20260618/phase3ar_true_1min_sidecar_canary.parquet
still_blocked: 0
note: optional expression smoke was stopped after materialization because it was redundant on the 3710677-row panel
```

Full-width Phase3AS eval:

```text
output: runtime/phase3ce2_fullwidth_realdata_eval_20260618
report: reports/phase3ce2_fullwidth_realdata_eval_20260618
candidate_count: 104
evaluated_candidate_count: 104
error_count: 0
memory_hit_count: 0
panel_codes: 5134
panel_rows_read_for_eval: 1231760
evaluated_trade_time_count: 240
horizons_min: 1, 5
```

Compact bucket summary:

```text
all:
  rows 208, candidate_ids 104, ic_count_max 239, ic_count_median 235, max_abs_ic 0.211530
SafeCSResidual:
  rows 32, candidate_ids 16, nonnull_rows_max 1221495, ic_count_max 237, ic_count_median 236
event_limit_high_board:
  rows 72, candidate_ids 36, nonnull_rows_max 1230560, ic_count_max 239, ic_count_median 234
m1_open_gap:
  rows 6, candidate_ids 3, nonnull_rows_max 815908, ic_count_max 158
fundamental:
  rows 60, candidate_ids 30, nonnull_rows_max 1219280, ic_count_max 239
turnover:
  rows 12, candidate_ids 6, nonnull_rows_max 1230466, ic_count_max 239
```

Interpretation:

```text
CE2 typed primitive runtime path is now validated beyond the 6-code canary
SafeCSResidual no longer collapses to all-null once the panel has enough cross-sectional width
event/high-board fields are executable on true-1min rows, but this remains semantic/plumbing validation, not alpha proof
next stage can start reward-gated search only with these gates active:
  typed primitive construction gate
  sidecar PIT/cutoff materialization
  trade_time cross-section evaluation
  fragment replay / placebo / wrong-lag checks before any promotion language
```
