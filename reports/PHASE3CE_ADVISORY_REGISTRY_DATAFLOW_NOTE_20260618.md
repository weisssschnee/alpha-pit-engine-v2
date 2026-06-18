# Phase3CE Advisory Registry Dataflow Note

Status: diagnostic note. No search launched. No official X0/R3 mutation.

## Finding

The CE0 `advisory_registry` unsafe hits come from durable field-registry audit outputs:

```text
runtime/field_registry/cn_field_integration_completeness_audit_v1_20260603/factor_candidate_field_refs.csv
runtime/field_registry/cn_field_integration_completeness_audit_v1_20260603/selector_selected_field_refs.csv
```

These files are written by:

```text
src/our_system_phase2/runtime/cn_field_integration_completeness_audit_v1.py
```

They are not official X0/R3 book state.

Current source scan did not find selector/search runtime code directly consuming `factor_candidate_field_refs.csv` or `selector_selected_field_refs.csv` as candidate input. They are derived completeness/advisory artifacts.

## Implication

The advisory registry does not require the same treatment as candidate materialization or G2 input. However, it is still a durable decision-support artifact under `runtime/field_registry`, so CE1 should not leave unsafe rows there as neutral references.

CE1 should add an advisory-registry output gate:

```text
unsafe formula row
  -> keep row for audit provenance
  -> add typed_gate_decision=blocked_unsafe_known_structure
  -> add typed_gate_reason
  -> exclude from positive "field integrated / search ready" counts
```

This keeps the registry useful as evidence without allowing it to silently certify unsafe direct-event formulas.

## Relation To Overlap Matrix

`cn_factor_pack` and `advisory_registry` have identical unsafe expression digest sets:

```text
cn_factor_pack unique digests: 982
advisory_registry unique digests: 982
intersection: 982
jaccard: 1.0
```

This is consistent with advisory registry being a derived view of the factor/direct-event pack line, not a third independent unsafe generator. It still needs an explicit gate verdict because it is durable and may be used for review or planning.

