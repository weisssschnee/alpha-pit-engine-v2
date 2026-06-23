# Phase3CP Real CM Small Loop 2026-06-23

Decision: `PHASE3CP_REAL_CM_SMALL_LOOP_PASS_DIAGNOSTIC_ONLY`

## Result

```text
generated_candidates: 36
ca_candidate_count: 30
cm_field_gate_passed: 12
cm_field_gate_rejected_missing: 0
cm_field_gate_passed_over_limit: 18
cm_selection_mode: arm_balanced
cm_lineage_consistent: True
cm_candidate_count: 12
cm_followup_count: 1
cn_candidate_count: 12
next_allocated_budget: 1024
next_fresh_share: 0.71972656
```

## Checks

```text
budget_table_used: True
generated_budget_ok: True
ca_has_candidates: True
field_gate_has_cm_candidates: True
real_cm_eval_used: True
true1min_shard_root_exists: True
suspicious_1d_path_blocked: True
cm_fast_mode: True
cm_candidate_count_ok: True
cm_lineage_consistent: True
cn_memory_matches_cm: True
reschedule_total_ok: True
holdout_not_optimizer_input: True
```

## Boundary

- This route runs real `phase3cm-train-portfolio-sortino-reward-audit`.
- It still uses a bounded small candidate/sample budget.
- CA metrics remain ranking-only.
- Holdout is report-only and not scheduler feedback.
- X0/R3 remain read-only.
