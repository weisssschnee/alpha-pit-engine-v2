# Migration Manifest

Source repository:

```text
G:\Project_V7_Rotation\alpha_pit_data_feature_workspace_20260531
source HEAD observed: dd704a8
source branch observed: feature/data-feature-workspace-20260531
```

Target repository:

```text
G:\Project_V7_Rotation\alpha_pit_true1min_engine_20260619
```

## Imported Runtime Code

Core true1min/search/reward chain:

```text
phase3bl_bk_priority_signal_materialization.py
phase3bn_open_diversified_true1min_canary.py
phase3bp_true1min_search_algorithm_smoke.py
phase3bq_compute_allocation_benchmark.py
phase3bs_adaptive_ucb_cem_practice.py
phase3bt_ast_algorithm_bakeoff.py
phase3bu_ast_fresh_winner_variants.py
phase3bx_bv_sortino_mcmc_audit.py
phase3bz_fragment_replay_audit.py
phase3ca_build_bz_candidate_audit.py
phase3cm_train_portfolio_sortino_reward_audit.py
phase3cf_large_search_prelaunch.py
```

Typed primitive and quarantine chain:

```text
phase3cd_ast_primitive_assumption_audit.py
phase3ce_unsafe_motif_quarantine_audit.py
phase3ce1_g2_input_gate_smoke.py
phase3ce1_search_memory_blocked_view.py
phase3ce2_build_event_derived_daily_panel.py
phase3ce2_build_fullwidth_validation_panel.py
phase3ce2_typed_primitive_candidate_pack_canary.py
phase3ce2_typed_primitive_evaluator_smoke.py
```

Gate integration touchpoints retained for continuity:

```text
phase3aa_apply_mature_g2_selector.py
phase3aa_enrich_shared_candidate_pool.py
cn_factor_pack_shared_pool_preflight.py
cn_field_integration_completeness_audit_v1.py
phase3ar_sidecar_field_adapter.py
phase3r_limit_motif_pack_diagnostic.py
```

Services retained:

```text
real_market_validation.py
real_market_data.py
feature_algebra.py
field_encoder.py
event_derived_features.py
market_regime_state.py
typed_primitive_gate.py
search_memory.py
```

## Imported Evidence

Only compact decision/evidence reports were imported. Large historical run trees
and old diagnostic debris were excluded.

Key imported folders:

```text
reports/phase3cd_ast_primitive_assumption_audit_20260618
reports/phase3ce_unsafe_motif_quarantine_audit_20260618
reports/phase3ce1_search_memory_blocked_view_20260618
reports/phase3ce1_g2_input_gate_smoke_20260618
reports/phase3ce2_typed_primitive_candidate_pack_canary_20260618
reports/phase3ce2_typed_primitive_evaluator_smoke_20260618
reports/phase3ce2_fullwidth_realdata_eval_20260618
reports/phase3ce2_fullwidth_validation_panel_20260618
reports/phase3cf_reward_gated_large_search_prelaunch_20260618
reports/phase3code_verify_ca_bridge_smoke_20260618
reports/phase3code_verify_bz_smoke_20260618
runtime/phase3ce2_fullwidth_phase3ar_sidecar_adapter_20260618/phase3ar_still_blocked_formula_rows.json
```

## Imported Run Plans And Scripts

```text
runtime/run_plans/phase3cf_guarded_smoke_run_plan_20260618.json
runtime/run_plans/phase3cf_reward_gated_large_search_run_plan_20260618.json
runtime/run_plans/phase3by_reward_gated_large_search_run_plan_20260616.json
scripts/phase3cf_guarded_smoke_launcher_20260618.ps1
scripts/phase3cf_guarded_smoke_company_runner_20260618.ps1
scripts/phase3cf_reward_gated_large_search_launcher_20260618.ps1
```

## Exclusions

Intentionally excluded:

```text
old 1D kline data
legacy Phase1/Phase2 launchers
bulk runtime search output trees
bulk report trees not needed for CE/CF/BZ chain proof
__pycache__ and local caches
```

## Known Follow-Up

The new repository currently provides a clean source and evidence package. The
next hardening step is to add tests that assert:

```text
1. app.py exposes no legacy 1D route
2. guarded feedback keeps CEM/UCB unchanged when clean feedback < threshold
3. CA bridge hard-rejects wrong-lag and high-correlation rows by default
4. BZ replay remains diagnostic-only and cannot be used as the search reward
5. Phase3CM train portfolio Sortino reward audit is the next reward target before large search restart
```

## Post-Migration Active Additions

The following files were added after the clean true1min repository was created.
They are not bulk legacy imports; they are current research-chain additions.

```text
src/our_system_phase2/runtime/phase3ds_targeted_family_repair_pack.py
src/our_system_phase2/runtime/phase3dt_survivor_expansion_repair_pack.py
src/our_system_phase2/runtime/phase3du_adaptive_regime_free_deepen_pack.py
src/our_system_phase2/runtime/phase3dv_budget_pool_self_deepen_pack.py
```

Current status:

```text
phase3ds:
  provenance / targeted repair ancestor

phase3dt:
  provenance / survivor expansion ancestor

phase3du:
  retired from current route; diagnostic replay only

phase3dv:
  current budget-pool self-deepen route
```

Current durable records:

```text
reports/PHASE3DV_BUDGET_POOL_SELF_DEEPEN_20260630.md
reports/PHASE3DV_RETIREMENT_AND_WORKSPACE_HYGIENE_20260630.md
runtime/run_plans/phase3dv_budget_pool_self_deepen_cm_reward_20260630.json
```

## 2026-07-10 Semantic And Throughput Hardening

The active CNline2 route now adds construction-time value-domain semantics,
sampled signal equivalence control, and byte-bounded runtime caches:

```text
src/our_system_phase2/services/expression_semantics.py
src/our_system_phase2/services/signal_vector_semantics.py
tests/test_expression_semantics.py
tests/test_signal_vector_semantics.py
tests/test_phase3cm_cache_bounds.py
tests/test_phase3cm_semantic_only_integration.py
reports/PHASE3GA_SEMANTIC_SEARCH_EFFICIENCY_REPAIR_20260710.md
reports/phase3ga_cnline2_acceptance_bundle_20260711/
docs/adr/0001-fixed-global-trade-date-splits.md
runtime/run_plans/phase3ga_true1min_2024_2025_global_split_manifest.csv
```

Current data boundary:

```text
train/search: repaired true1min 2024-2025, 16 shards, 121 columns
2026: separate forward/OOS asset; forbidden in train/search until separately augmented and accepted
old 1D kline: forbidden
```

Current search/reward order:

```text
construction semantic gate
-> typed primitive gate
-> exact/skeleton memory
-> CA ranking
-> semantic-only signal vector gate
-> fixed global trade-date manifest (364 train / 73 validation / 48 holdout)
-> full Phase3CM train reward
-> guarded Phase3CN feedback
```

Historical-run correction: the Phase3FIX large run set
`PRE_CM_SEMANTIC_GATE_DISABLED`. Its 60 semantic-degenerate candidates were
quarantined during exact recovery, not before the historical CM admission.

Field and split publication contract:

```text
accepted parquet schema: 121 columns
metadata/key: 7
raw true1min: 12
firstN opening state: 30
lagged context: 59
typed event/state: 13
split overlap after normalization: 0
2026 forward OOS: separate and not consumed by 2024-2025 search
```

## 2026-07-11 EVALRESET Boundary

ADR 0002 adds explicit development, challenge, sealed, spent, and forward data
roles. The 2025 validation and holdout dates are spent because candidate-level
results were manually exposed. Phase3CN/search/scheduler feedback is now a
physical train-only projection; candidate-level validation, holdout, challenge,
sealed, forward, and OOS columns fail closed at the machine boundary.

Historical Phase3FIX is classified `PROVENANCE_UNVERIFIED`,
`NON_REPRODUCIBLE_AS_EXECUTED`, and `NOT_VALID_FOR_PROOF`. Its key-file hash
agreement is not a reproducible run manifest.

The active Phase A diagnostic route is:

```text
fixed development-only coordinate registry
-> activation/rank/value/SimHash/missingness sketches
-> 384-candidate exact fidelity and A/B stability gate
-> full-generation consensus cluster registry
-> unchanged cluster IDs mapped through proxy/admission/strict/coverage stages
```

This route is label-free and cannot write search memory. Architecture state is
registered in `runtime/run_plans/evalreset_phase1_architecture_registry_v1.json`
and generated into `.planning/architecture/architecture_graph.json`; the graph contract includes
the six forbidden evaluation-feedback edges.
