# Phase3CE1 Typed Primitive Gate Contract

Status: design contract before implementation. No search launched. No official X0/R3 mutation.

## Decision

CE1 must prioritize construction-time typed primitive validation. Search memory cleanup is bookkeeping hygiene, not the primary safety mechanism.

The unsafe motif issue has two separate mechanisms:

1. New unsafe candidates can be generated again.
2. Historical unsafe candidates can remain in memory/reporting state.

Only construction-time validation prevents mechanism 1. Memory quarantine only cleans mechanism 2.

## Gate Order

1. Candidate materialization gate
   - every generated expression is parsed before it receives a candidate id;
   - blocked field category inside ordinary continuous primitive is rejected;
   - sparse event/state fields must use typed event/state primitives.

2. Phase3R diagnostic ledger gate
   - diagnostic motif packs cannot write candidate ledger rows unless each row carries `typed_gate_decision`;
   - unsafe rows are written to a reject ledger, not a candidate ledger.

3. Phase3AA shared-pool enrichment gate
   - enrichment must reject or quarantine already-materialized unsafe rows before they enter shared pool.

4. Factor-pack / preflight integration gate
   - direct event and factor packs must run the same validator before preflight writes.

5. Advisory registry output gate
   - completeness/adaptation audit outputs under `runtime/field_registry` must not publish unsafe formula rows as neutral field references;
   - rows rejected by the shared validator must be written with `typed_gate_decision=blocked_unsafe_known_structure`;
   - advisory registries are not official book state, but they are durable decision-support artifacts and must carry the same gate verdict.

6. Search memory inheritance gate
   - unsafe historical memory entries are reclassified as `blocked_unsafe_known_structure`;
   - do not delete expression/skeleton keys as if never searched;
   - blocked keys remain available for duplicate/block checks, not for reward or positive attribution.

7. G2 selector input gate
   - final defense only;
   - any row missing `typed_gate_decision=allow` or an approved typed rewrite is rejected.

## Entry Lines That Must Be Gated In The Same Implementation Batch

CE0 overlap audit showed at least two independent unsafe entry lines:

```text
Phase3R limit diagnostic motif line:
  motif_pack_limit_diagnostic
  -> phase3r_limit_motif_pack_diagnostic
  -> diagnostic candidate ledger / downstream reports

CN factor/direct event pack line:
  cn_event/direct event factor pack
  -> factor-pack / preflight integration
  -> runtime/field_registry advisory views
```

The shared validator must be connected to both lines in the same CE1 implementation batch. Gating only Phase3R is insufficient; gating only factor-pack/preflight is also insufficient.

## Memory Policy

Do not implement memory cleanup as deletion.

Required behavior:

```text
unsafe old memory entry
  -> keep expression_key / skeleton_key in blocked memory view
  -> remove from positive production_rule_stats / source credit
  -> mark as known unsafe structure
  -> block regeneration even if candidate pack proposes it again
```

This prevents the false state:

```text
deleted from memory -> appears fresh -> generated again
```

## Shared Validator Contract

All CE1 gates must call the same validator.

Input:

```text
candidate_id
expression
source_layer
source_generator
entry_lineage
materialization_stage
candidate_role
field_category_registry
typed_primitive_registry
```

Output:

```text
typed_gate_decision:
  allow
  reject_ordinary_primitive_on_blocked_category
  require_typed_rewrite
  reject_missing_cutoff_contract
  reject_membership_key_formula_input
  reject_label_or_future_field

typed_gate_reason
blocked_fields
blocked_primitives
required_rewrite
registry_version
lineage_id
materialization_stage
```

Required caller-provided context:

```text
entry_lineage:
  phase3r_limit_diagnostic
  cn_factor_direct_event_pack
  factor_pack_preflight
  advisory_runtime_registry
  search_memory_inheritance
  g2_selector_input

materialization_stage:
  motif_definition
  candidate_materialization
  ledger_write
  preflight_write
  advisory_registry_write
  memory_load
  selector_input

candidate_role:
  diagnostic
  selector_candidate
  event_state_candidate
  blocked_memory_key
  official_book_candidate
```

## Required Evidence

Before any large search:

```text
typed_gate_violation.csv
candidate_materialization_rejects.csv
shared_pool_quarantine.csv
advisory_registry_gate_audit.csv
search_memory_blocked_view.csv
g2_input_gate_summary.json
```

Pass condition:

```text
ordinary AST blocked-category leaks: 0
search memory unsafe records deleted: 0
search memory unsafe records reclassified: > 0 when old unsafe records exist
official X0/R3 mutation: false
```

## Implementation Priority

1. Build shared validator module.
2. Add validator to Phase3R candidate materialization and ledger writes.
3. Add validator to factor-pack/preflight writes.
4. Add validator verdict propagation to advisory registry outputs.
5. Add search-memory blocked view.
6. Add G2 final input gate.
7. Only then run typed primitive canary.
