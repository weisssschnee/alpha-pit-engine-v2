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
