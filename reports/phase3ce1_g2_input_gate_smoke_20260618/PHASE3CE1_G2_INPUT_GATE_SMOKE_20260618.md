# Phase3CE1 G2 Input Gate Smoke

- created_at: 2026-06-18T03:58:27+00:00
- input: `reports\phase3ce1_factor_pack_preflight_gate_smoke_20260618\cn_factor_pack_candidate_integration.csv`
- registry_version: `phase3ce1_typed_primitive_gate_v1_20260618`
- input rows: 760
- allowed rows: 3
- rejected rows: 757
- decision: PASS_G2_INPUT_GATE_ACTIVE

## Rejected Decisions

- blocked_unsafe_known_structure: 725
- reject_membership_key_formula_input: 32

## Rejected Reasons

- membership/group fields are context keys until group geometry audit: 32
- ordinary continuous primitive consumed sparse event or discrete state field: 725

## Outputs

- g2_input_allowed.csv
- g2_input_rejected.csv
- g2_input_gate_summary.json

This smoke does not run selector scoring, replay, or official book promotion.
