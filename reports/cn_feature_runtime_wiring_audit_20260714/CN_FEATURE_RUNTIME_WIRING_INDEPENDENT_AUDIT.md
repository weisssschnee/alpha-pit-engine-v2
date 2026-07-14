# CN Feature Runtime Wiring Independent Audit

Status: `CN_FEATURE_RUNTIME_WIRING_MISMATCH_CONFIRMED`

## Outcome

The data and representation work is real, but the runtime is split into two authorities. The unified runner correctly exposes the versioned 121-field Fabric, 147 canonical fundamental representations and frozen Broad Event mechanisms through typed routes. The active legacy `Phase3GA -> Phase3CM -> Phase3CN` search chain still uses hardcoded/schema-derived pools and does not consult the unified registry.

This is a wiring mismatch, not a claim that the new features lack value. No performance search was run.

## Measured universes

- Physical/inventoried source fields: 1,348 (`121` panel fields + `1,227` fundamental source fields).
- Qualified candidate-visible representations: 282.
- Search-eligible unified representations: 242.
- Unified routes executed with non-zero proposal and strict evidence: 8/8.
- Synthetic/planted wiring cases: 9/9 passed.

## Independent findings

1. `RegistryDrivenGenerator` and `TypedRouteCompiler` are used by unified preflight/discovery only. `app.py` still names `phase3dv-budget-pool-self-deepen-pack` as the current search route.
2. The legacy Phase3DV generator owns hardcoded field-family arrays and Phase3CP intersects them with physical parquet schemas. It can therefore expose legacy fields that the unified registry blocks, including latched Event/State columns.
3. All 147 fundamental representations are confined to the completed development-only unified runner. They do not enter legacy Phase3GA, Phase3CM or Phase3CN.
4. The completed two-seed run had 7 survivors per seed and 4 shared exact survivors. All 4 shared exact survivors are old frozen Broad Event replays. They are regression evidence, not new capability discovery.
5. The historical result's `qualified_to_apply_for_independent_challenge=true` was an eligibility bug. Current code now requires at least one non-frozen cross-seed reproduction; no challenge was opened.
6. Final Phase3CM exact normalization uses the fixed 485-date manifest, but worker-local `_split_map(...)` and the chunk-04 recovery workers remain reachable before/without that normalization. The repair is therefore not globally enforced at every worker boundary.
7. Plate/industry remains disabled: build code is present, but no active versioned historical PIT membership release is registered. No placeholder or current snapshot was treated as a capability.

## Access boundary

Only registries, source code, historical audit CSVs, completed development-only ledgers and synthetic candidates were read. Validation, holdout, challenge and 2026 were not read. No reward, survivor selection, promotion, candidate discovery or persistent adaptive memory was executed.
