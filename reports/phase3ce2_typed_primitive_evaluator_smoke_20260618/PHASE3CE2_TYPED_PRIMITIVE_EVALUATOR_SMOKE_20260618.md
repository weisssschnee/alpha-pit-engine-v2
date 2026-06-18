# Phase3CE2 Typed Primitive Evaluator Smoke

- created_at: 2026-06-18T04:31:04+00:00
- panel_rows: 64
- expression_count: 8
- error_count: 0
- decision: PASS_TYPED_PRIMITIVE_EVALUATOR_SMOKE

## Semantic Checks

- event_age_before_first_event_nan: pass - pre_event_nonnull=0
- event_age_resets_on_event: pass - event_bar_age=0.0
- event_count_requires_full_window: pass - first4_nonnull=0
- valid_ratio_gate_masks_low_coverage: pass - low_coverage_nonnull=0

This is evaluator plumbing and semantic smoke only; it is not alpha proof.
