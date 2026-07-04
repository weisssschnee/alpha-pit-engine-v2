# Phase3EM Field Schema Root-Cause Audit

- created_at: `2026-07-04T15:16:59.108595+00:00`
- decision: `HOLD_LEGACY_ALIAS_SCHEMA_GATE_NEEDED`
- candidate_rows: `1024`
- candidate_missing_schema_rows: `1`
- legacy_alias_candidate_rows: `1`
- schema_has_m1_first_ret: `False`
- schema_has_range_location: `False`
- schema_has_current_firstn_fields: `True`
- schema_has_ctx_fields: `True`
- schema_has_evt_fields: `True`

## Root Cause

Phase3EI historical recheck admitted one Phase3CN/Phase3CX legacy fixture expression using m1_first_ret and range_location. Current true1min sidecar schema uses explicit m1_first5/15/30 fields and explicit range-location formulas, not these aliases. Phase3CM reads all fields for a chunk at once, so one missing-field legacy candidate fails the whole chunk.

## Missing Fields

- `m1_first_ret`: 1
- `range_location`: 1

## Policy

- Do not add ambiguous legacy aliases to parquet just to make old fixtures run.
- Strict recheck should schema-gate or quarantine expressions with `m1_first_ret` / `$range_location` before Phase3CM chunking.
- `range_location` can be rewritten only as an explicit formula for diagnostic-only compatibility if the intended definition is bar close location.
- `m1_first_ret` is ambiguous and must be blocked unless explicitly mapped to `m1_first5_last_return_vs_open` or another firstN field by a named diagnostic policy.

## Output Files

- `G:\Project_V7_Rotation\alpha_pit_true1min_engine_20260619\reports\phase3em_field_schema_rootcause_audit_20260704\phase3em_field_schema_rootcause_summary.json`
- `G:\Project_V7_Rotation\alpha_pit_true1min_engine_20260619\reports\phase3em_field_schema_rootcause_audit_20260704\phase3em_missing_field_candidate_audit.csv`
- `G:\Project_V7_Rotation\alpha_pit_true1min_engine_20260619\reports\phase3em_field_schema_rootcause_audit_20260704\phase3em_legacy_alias_candidate_rows.csv`
- `G:\Project_V7_Rotation\alpha_pit_true1min_engine_20260619\reports\phase3em_field_schema_rootcause_audit_20260704\phase3em_legacy_alias_code_hits.csv`
