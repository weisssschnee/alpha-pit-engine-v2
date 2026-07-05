# Graph Report - G:\Project_V7_Rotation\alpha_pit_true1min_engine_20260619  (2026-07-05)

## Corpus Check
- cluster-only mode — file stats not available

## Summary
- 1224 nodes · 3465 edges · 47 communities (42 shown, 5 thin omitted)
- Extraction: 92% EXTRACTED · 8% INFERRED · 0% AMBIGUOUS · INFERRED: 272 edges (avg confidence: 0.77)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `cfa0669e`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- [[_COMMUNITY_Canary Candidate Aggregation|Canary Candidate Aggregation]]
- [[_COMMUNITY_Candidate Validation Pipeline|Candidate Validation Pipeline]]
- [[_COMMUNITY_Candidate Pool Enrichment|Candidate Pool Enrichment]]
- [[_COMMUNITY_Elite Candidate Search|Elite Candidate Search]]
- [[_COMMUNITY_Portfolio Reward Audit|Portfolio Reward Audit]]
- [[_COMMUNITY_Feedback Memory Build|Feedback Memory Build]]
- [[_COMMUNITY_Signal Materialization|Signal Materialization]]
- [[_COMMUNITY_Field Integration Audit|Field Integration Audit]]
- [[_COMMUNITY_Event-Derived Daily Panel|Event-Derived Daily Panel]]
- [[_COMMUNITY_Sidecar Field Adapter|Sidecar Field Adapter]]
- [[_COMMUNITY_Atom Lane Inventory Audit|Atom Lane Inventory Audit]]
- [[_COMMUNITY_Budget Pool Deepen|Budget Pool Deepen]]
- [[_COMMUNITY_Real CM Small Loop|Real CM Small Loop]]
- [[_COMMUNITY_CE1 Gate Smoke Test|CE1 Gate Smoke Test]]
- [[_COMMUNITY_AST Primitive Audit|AST Primitive Audit]]
- [[_COMMUNITY_Unsafe Motif Quarantine|Unsafe Motif Quarantine]]
- [[_COMMUNITY_Adaptive Regime Deepen|Adaptive Regime Deepen]]
- [[_COMMUNITY_T+1 Tradable Replay|T+1 Tradable Replay]]
- [[_COMMUNITY_True1min Sidecar Pack|True1min Sidecar Pack]]
- [[_COMMUNITY_CN Tradable Followup|CN Tradable Followup]]
- [[_COMMUNITY_Multi-Arm Scheduler|Multi-Arm Scheduler]]
- [[_COMMUNITY_BZ Candidate Audit|BZ Candidate Audit]]
- [[_COMMUNITY_Short Lineage Audit|Short Lineage Audit]]
- [[_COMMUNITY_Factor Pack Preflight|Factor Pack Preflight]]
- [[_COMMUNITY_Compute Allocation Benchmark|Compute Allocation Benchmark]]
- [[_COMMUNITY_Sortino MCMC Audit|Sortino MCMC Audit]]
- [[_COMMUNITY_Project Documentation|Project Documentation]]
- [[_COMMUNITY_Low Turnover Probe|Low Turnover Probe]]
- [[_COMMUNITY_Shard-Sidecar Augment|Shard-Sidecar Augment]]
- [[_COMMUNITY_Survivor Expansion Repair|Survivor Expansion Repair]]
- [[_COMMUNITY_Targeted Family Repair|Targeted Family Repair]]
- [[_COMMUNITY_Mature G2 Selector|Mature G2 Selector]]
- [[_COMMUNITY_Primitive Evaluator Smoke|Primitive Evaluator Smoke]]
- [[_COMMUNITY_Large Search Prelaunch|Large Search Prelaunch]]
- [[_COMMUNITY_Fullwidth Validation Panel|Fullwidth Validation Panel]]
- [[_COMMUNITY_Application Entry|Application Entry]]
- [[_COMMUNITY_V2.1 Prototype Namespace|V2.1 Prototype Namespace]]
- [[_COMMUNITY_V2.1 Runtime Entrypoints|V2.1 Runtime Entrypoints]]
- [[_COMMUNITY_V2.1 Services|V2.1 Services]]
- [[_COMMUNITY_Sidecar Canary Eval|Sidecar Canary Eval]]
- [[_COMMUNITY_Search Algorithm Smoke|Search Algorithm Smoke]]

## God Nodes (most connected - your core abstractions)
1. `evaluate_panel_expression()` - 34 edges
2. `safe_float()` - 33 edges
3. `normalize_candidate_schema()` - 33 edges
4. `adapt()` - 24 edges
5. `validate_expression_on_loaded_panel()` - 23 edges
6. `main()` - 22 edges
7. `batch_validate_candidate_ledger()` - 21 edges
8. `_generate_cem_elite_candidates()` - 20 edges
9. `main()` - 20 edges
10. `_run_materialization()` - 19 edges

## Surprising Connections (you probably didn't know these)
- `Phase3 True1min Iteration Tree 2026-06-23` --references--> `X0/R3 Read-Only Benchmarks`  [EXTRACTED]
  reports/PHASE3_TRUE1MIN_ITERATION_TREE_20260623.md → AGENTS.md
- `_build_candidate_rows()` --calls--> `validate_row()`  [INFERRED]
  src/our_system_phase2/runtime/cn_factor_pack_shared_pool_preflight.py → src/our_system_phase2/services/typed_primitive_gate.py
- `main()` --calls--> `gate_g2_input_rows()`  [INFERRED]
  src/our_system_phase2/runtime/phase3aa_apply_mature_g2_selector.py → src/our_system_phase2/services/typed_primitive_gate.py
- `adapt()` --calls--> `evaluate_panel_expression()`  [INFERRED]
  src/our_system_phase2/runtime/phase3ar_sidecar_field_adapter.py → src/our_system_phase2/services/real_market_validation.py
- `_add()` --calls--> `_max_expression_window()`  [INFERRED]
  src/our_system_phase2/runtime/phase3bn_open_diversified_true1min_canary.py → src/our_system_phase2/runtime/phase3bl_bk_priority_signal_materialization.py

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Safety and Gate Audit Pipeline** — reports_phase3cd_ast_primitive_assumption_audit_20260618_ast_primitive_audit, reports_phase3ce_unsafe_motif_quarantine_audit_20260618_unsafe_motif_quarantine, reports_phase3ce1_g2_input_gate_smoke_20260618_typed_primitive_gate, reports_phase3ce1_search_memory_blocked_view_20260618_search_memory_blocked_view, reports_phase3ce2_typed_primitive_candidate_pack_canary_20260618_typed_primitive_canary, reports_phase3ce2_typed_primitive_evaluator_smoke_20260618_typed_primitive_evaluator [EXTRACTED 0.90]
- **Reward-Based Search Control Loop** — reports_phase3code_verify_bz_smoke_20260618_bz_fragment_replay, reports_phase3_true1min_iteration_tree_20260623_cm_reward, reports_phase3_true1min_iteration_tree_20260623_cn_feedback, reports_phase3_true1min_iteration_tree_20260623_co_scheduler, reports_phase3_true1min_iteration_tree_20260623_cp_search, reports_phase3_true1min_iteration_tree_20260623_dv_deepen [INFERRED 0.70]
- **Guarded Large Search Prerequisites** — reports_phase3cf_reward_gated_large_search_prelaunch_20260618_reward_gated_prelaunch, reports_phase3code_verify_ca_bridge_smoke_20260618_ca_bridge, reports_phase3code_verify_bz_smoke_20260618_bz_fragment_replay, reports_phase3_true1min_iteration_tree_20260623_cm_reward [INFERRED 0.70]

## Communities (47 total, 5 thin omitted)

### Community 0 - "Canary Candidate Aggregation"
Cohesion: 0.05
Nodes (94): _add(), _aggregate_decisions(), _candidate_templates(), _cap_crowded_vwap(), _hash(), _lane_summary(), _load_memory_hashes(), main() (+86 more)

### Community 1 - "Candidate Validation Pipeline"
Cohesion: 0.09
Nodes (88): expand_derived_fields(), _attach_signal_safe_long_selection_diagnostics(), _attach_tdxgp_limit_status(), audit_expression_panel_exposure_neutrality(), _augment_market_fields(), _available_market_panel_usecols(), batch_validate_candidate_ledger(), _bucket_turnover_by_date() (+80 more)

### Community 2 - "Candidate Pool Enrichment"
Cohesion: 0.07
Nodes (65): CandidateRecord, _copy_event_row(), enrich_pool(), _event_rows(), _expand_memory_roots(), _factor_pack_rows(), _formula_fields(), main() (+57 more)

### Community 3 - "Elite Candidate Search"
Cohesion: 0.09
Nodes (70): _add_candidate(), _aggregate_decisions(), _assert_policy_inputs_ready(), _ast_variables(), _atom_inverted_expr(), _atom_normalized_expr(), _atom_rank_expr(), begin_generation_accounting() (+62 more)

### Community 4 - "Portfolio Reward Audit"
Cohesion: 0.09
Nodes (61): _bootstrap_days(), _bounded(), _BoundedSeriesCache, _build_eval_time_index(), _candidate_event_fields(), _candidate_portfolio_rows_from_frame(), _candidate_portfolio_rows_from_precomputed_time_groups(), _candidate_summary() (+53 more)

### Community 5 - "Feedback Memory Build"
Cohesion: 0.11
Nodes (60): _arm_score_table(), build_feedback_memory(), _discover_cm_tables(), _family_tables(), _group_rows(), _has_wrong_lag_or_corr(), _is_clean(), _is_rewardhack() (+52 more)

### Community 6 - "Signal Materialization"
Cohesion: 0.09
Nodes (58): _candidate_direction(), _candidate_pairwise(), _discover_panels(), _f(), _fields(), _find_matching_paren(), _fmt(), _future_returns() (+50 more)

### Community 7 - "Field Integration Audit"
Cohesion: 0.10
Nodes (50): _best_alias_matches(), _classify(), _extract_expr_fields(), _load_assets(), _load_factor_refs(), _load_field_availability_refs(), _load_selector_refs(), main() (+42 more)

### Community 8 - "Event-Derived Daily Panel"
Cohesion: 0.08
Nodes (46): build(), main(), _normalize_cn_code(), Any, DataFrame, Path, Build a CE2 event-derived daily panel from HFQ daily silver data.  The output is, _read_hfq_root() (+38 more)

### Community 9 - "Sidecar Field Adapter"
Cohesion: 0.17
Nodes (44): adapt(), _append_nan_fields(), _attach_minute_open_gap_fields(), _blocked_candidate_rows(), _canary_codes(), _classify_formula(), _compact_cn_code(), _compute_ctx_fund_field() (+36 more)

### Community 10 - "Atom Lane Inventory Audit"
Cohesion: 0.12
Nodes (34): _candidate_expressions(), _counter(), main(), Any, Path, Phase3CR atom/lane inventory audit.  This audit checks whether the true-1min gen, _read_available_fields(), _resolve() (+26 more)

### Community 11 - "Budget Pool Deepen"
Cohesion: 0.16
Nodes (31): _add_candidate(), _allocate_seed_budgets(), _available_fields_from_shard_root(), _balanced_limit(), _build_candidate_rows(), _build_from_seed(), _choose_context_groups(), _cluster_rows() (+23 more)

### Community 12 - "Real CM Small Loop"
Cohesion: 0.25
Nodes (32): _add_expression_memory_keys(), _add_memory_key(), _add_structural_memory_keys(), _audit_cm_lineage_consistency(), _available_fields(), _budget_table_path(), _canonical_expression_key(), _collect_json_memory_keys() (+24 more)

### Community 13 - "CE1 Gate Smoke Test"
Cohesion: 0.16
Nodes (26): _counter(), main(), Any, Path, Smoke-test the CE1 typed primitive gate at mature G2 selector input.  This comma, _read_csv(), _read_json(), _read_rows() (+18 more)

### Community 14 - "AST Primitive Audit"
Cohesion: 0.19
Nodes (26): build_parser(), _category_route(), _classify_field_name(), _classify_group_line(), _field_category_rows(), _limit_rewrite_hint(), main(), _matrix_decision() (+18 more)

### Community 15 - "Unsafe Motif Quarantine"
Cohesion: 0.20
Nodes (26): build_parser(), _ce2_stop_condition_rows(), _digest(), _entry_point(), _entry_summary(), _expression_text(), _iter_text_files(), _load_cd_signatures() (+18 more)

### Community 16 - "Adaptive Regime Deepen"
Cohesion: 0.19
Nodes (25): _add_candidate(), _balanced_limit(), _build_free_deepen_rows(), _classify_seed(), _compose_forms(), _context_components(), _event_components(), _f() (+17 more)

### Community 17 - "T+1 Tradable Replay"
Cohesion: 0.21
Nodes (24): build_parser(), _candidate_metrics(), _daily_from_minute_frame(), _discover_panels(), _evaluate_shard(), _extract_fields(), _future_columns(), _load_followups() (+16 more)

### Community 18 - "True1min Sidecar Pack"
Cohesion: 0.35
Nodes (23): _billboard_sidecar(), build(), _date_series(), _field_contract_rows(), _hfq_sidecar(), _holder_sidecar(), main(), _market_sentiment_sidecar() (+15 more)

### Community 19 - "CN Tradable Followup"
Cohesion: 0.18
Nodes (22): build_parser(), _compatibility_rows(), _extract_fields(), _field_class(), _ledger_records(), _load_followups(), main(), Any (+14 more)

### Community 20 - "Multi-Arm Scheduler"
Cohesion: 0.19
Nodes (19): _inputs_from_root(), main(), Any, Path, Phase3CO multi-arm scheduler smoke.  This route reads Phase3CN feedback memory a, _render_md(), _resolve(), _arm_health() (+11 more)

### Community 21 - "BZ Candidate Audit"
Cohesion: 0.28
Nodes (19): _blockers(), build_candidate_table(), _count_by_arm(), _f(), _hard_reject_reason(), _iter_decision_files(), main(), _normalize_row() (+11 more)

### Community 22 - "Short Lineage Audit"
Cohesion: 0.29
Nodes (18): _candidate_decision(), _candidate_score(), _classify_csv(), _collect_recheck_candidates(), _digest(), _f(), _is_candidate_level_csv(), _iter_csv_rows() (+10 more)

### Community 23 - "Factor Pack Preflight"
Cohesion: 0.34
Nodes (17): _build_candidate_rows(), _build_field_rows(), build_report(), _counter_to_dict(), _expr_fields(), _factor_pack_rows(), _field_pack_lookup(), main() (+9 more)

### Community 24 - "Compute Allocation Benchmark"
Cohesion: 0.30
Nodes (17): _arm_specs(), _float(), _fmt(), _hot_path_scan(), main(), _package_versions(), Any, Path (+9 more)

### Community 25 - "Sortino MCMC Audit"
Cohesion: 0.31
Nodes (17): _bootstrap(), _collect_run(), _decision(), _float(), main(), _max_drawdown(), Any, Path (+9 more)

### Community 26 - "Project Documentation"
Cohesion: 0.22
Nodes (16): X0/R3 Read-Only Benchmarks, Phase3CM Train Portfolio Sortino Reward Audit, Phase3CN Reward Feedback Wiring, Phase3CO Multi-Arm Scheduler, Phase3CP Reward-Gated Medium Search, Phase3 True1min Iteration Tree 2026-06-23, Phase3DV Budget-Pool Self-Deepen, Phase3CD AST Primitive Assumption Audit (+8 more)

### Community 27 - "Low Turnover Probe"
Cohesion: 0.27
Nodes (15): _candidate_rows(), _flow_ratio(), _hash(), main(), Any, Path, _range_location(), _range_width() (+7 more)

### Community 28 - "Shard-Sidecar Augment"
Cohesion: 0.30
Nodes (15): augment(), _augment_one_panel(), _discover_panels(), _load_pack(), main(), _merge_market_asof(), _merge_stock_asof(), Any (+7 more)

### Community 29 - "Survivor Expansion Repair"
Cohesion: 0.27
Nodes (15): _add_row(), _balanced_limit(), _build_rows(), _ctx(), _event_exprs(), _hash(), main(), Any (+7 more)

### Community 30 - "Targeted Family Repair"
Cohesion: 0.31
Nodes (12): _context_expr(), _event_exprs(), _hash(), main(), Any, Path, Build a targeted Phase3DS family repair candidate pack.  This route does not sea, _render_md() (+4 more)

### Community 31 - "Mature G2 Selector"
Cohesion: 0.35
Nodes (11): _budget(), main(), _normalize_budget(), _prefilter_pool(), Any, Path, Apply the opt-in Phase3AA G2 selector to an enriched shared pool., _read_json() (+3 more)

### Community 32 - "Primitive Evaluator Smoke"
Cohesion: 0.35
Nodes (11): _direct_semantic_checks(), _expression_cases(), main(), Any, DataFrame, Path, Smoke-test typed primitive evaluator semantics on a synthetic true-1min panel., _series_stats() (+3 more)

### Community 33 - "Large Search Prelaunch"
Cohesion: 0.38
Nodes (11): build_prelaunch(), _command(), main(), Any, Path, Phase3CF reward-gated large-search prelaunch.  This module freezes the post-CE2, _read_json(), _resolve() (+3 more)

### Community 34 - "Fullwidth Validation Panel"
Cohesion: 0.47
Nodes (8): build(), main(), Any, DataFrame, Path, _read_shard(), _resolve(), _select_event_dates()

### Community 35 - "Application Entry"
Cohesion: 0.83
Nodes (3): _load_main(), main(), _split_route_args()

## Knowledge Gaps
- **3 isolated node(s):** `ArmProfile`, `Phase3AS True 1min Sidecar Canary Eval (Fullwidth)`, `Phase3BP True-1min Search Algorithm Smoke`
  These have ≤1 connection - possible missing edges or undocumented components.
- **5 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `normalize_candidate_schema()` connect `Feedback Memory Build` to `Canary Candidate Aggregation`, `Elite Candidate Search`, `Portfolio Reward Audit`, `Budget Pool Deepen`, `Adaptive Regime Deepen`, `BZ Candidate Audit`, `Low Turnover Probe`, `Survivor Expansion Repair`, `Targeted Family Repair`?**
  _High betweenness centrality (0.212) - this node is a cross-community bridge._
- **Why does `validate_expression()` connect `Field Integration Audit` to `Atom Lane Inventory Audit`, `Budget Pool Deepen`, `Candidate Pool Enrichment`, `Elite Candidate Search`?**
  _High betweenness centrality (0.190) - this node is a cross-community bridge._
- **Why does `evaluate_panel_expression()` connect `Candidate Validation Pipeline` to `Primitive Evaluator Smoke`, `Portfolio Reward Audit`, `Signal Materialization`, `Sidecar Field Adapter`, `T+1 Tradable Replay`?**
  _High betweenness centrality (0.168) - this node is a cross-community bridge._
- **Are the 9 inferred relationships involving `evaluate_panel_expression()` (e.g. with `adapt()` and `_run_materialization()`) actually correct?**
  _`evaluate_panel_expression()` has 9 INFERRED edges - model-reasoned connections that need verification._
- **Are the 29 inferred relationships involving `safe_float()` (e.g. with `_arm_score_table()` and `build_feedback_memory()`) actually correct?**
  _`safe_float()` has 29 INFERRED edges - model-reasoned connections that need verification._
- **Are the 20 inferred relationships involving `normalize_candidate_schema()` (e.g. with `_normalize_row()` and `_candidate_summary()`) actually correct?**
  _`normalize_candidate_schema()` has 20 INFERRED edges - model-reasoned connections that need verification._
- **What connects `Isolated V2.1 prototype namespace for the next major search stage.`, `Runtime entrypoints for the isolated V2.1 prototype.`, `Audit end-to-end CN field integration completeness.  This is a no-replay governa` to the rest of the system?**
  _73 weakly-connected nodes found - possible documentation gaps or missing edges._