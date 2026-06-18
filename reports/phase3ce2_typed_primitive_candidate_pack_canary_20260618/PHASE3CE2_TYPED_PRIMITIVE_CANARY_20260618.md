# Phase3CE2 Typed Primitive Candidate Pack Canary

- created_at: 2026-06-18T04:11:57+00:00
- registry_version: `phase3ce1_typed_primitive_gate_v1_20260618`
- candidate_rows: 104
- typed_gate_allowed_rows: 104
- typed_gate_rejected_rows: 0
- g2_input_allowed_rows: 104
- g2_input_rejected_rows: 0
- evaluator_support_status: implemented_in_real_market_validation
- decision: PASS_TYPED_PRIMITIVE_ENTRY_PATH_EVALUATOR_READY_HOLD_SEMANTIC_PROOF

## Field Category Attribution

- coverage_sensitive: 48
- discrete_state: 24
- sparse_event: 32

## Stop Condition Status

- ce2_01_entry_path: pass - {"coverage_sensitive": 48, "discrete_state": 24, "sparse_event": 32}
- ce2_02_old_primitive_leak: pass - typed_rejected=0, g2_rejected=0
- ce2_03_to_ce2_07_runtime_semantic_tests: ready_for_semantic_smoke - runtime evaluator exposes typed primitive operators; still requires semantic smoke, placebo/lag/fragment replay

This canary does not run alpha scoring, selector scoring, replay, or official promotion.
