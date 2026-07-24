# CN Targeted Formula-Space / CEM Qualification Result

Status: `FORMULA_SPACE_INCREMENT_NOT_PROVEN`

Promotion: forbidden

## Frozen target

- Route: `DISCLOSURE_EVENT`
- Skeleton: `cn.comp.v2.disclosure_event.pre_event_path`
- Old structure: `EventWindow($payload,$event,5,0)`
- Frozen extension: `EventWindow(Sign($payload),$event,5,0)`
- Old / expanded exact spaces after the historical exact snapshot: 272 / 544

The extension reused the unified registry, `RegistryDrivenGenerator`,
`CompositionalGrammarV2`, the existing expression AST, `TypedRouteCompiler`,
matched controls, behavior admission, and Phase3CM. It did not create a second
registry, parser, evaluator meaning, optimizer platform, or search memory.

## Engineering closure

The first 77o attempt exposed an authority/evaluator implementation gap:
`EventWindow` was canonical in the typed temporal registry and legal in the
Grammar/compiler, but absent from the Phase3CM streaming executor. Commit
`5a3f4d8642e9e9e5332ccf6fc70bb6110464bc64` added only the existing canonical
semantics, per-code block continuation, and continuation serialization.

Evidence:

- 72 focused local semantic/search tests passed.
- The full local suite produced 748 pass, 4 skip, and 5 unrelated pre-existing
  governance-artifact failures.
- Three official-Python 77o tests passed for reference parity, cross-block
  continuation, and serialized continuation.
- The pre-fix incident is preserved under the original 77o campaign root at
  `infrastructure_incidents/20260724T162825_eventwindow_streaming_gap`.

## Static qualification

| Metric | OLD | Expanded |
|---|---:|---:|
| comparable attempts | 1,000 | 1,000 |
| compile-valid pairs | 1,000 | 1,000 |
| control-valid pairs | 1,000 | 1,000 |
| exact/canonical unique pairs | 272 | 544 |
| AST-shape unique | 1 | 2 |
| attempts/s | 214.00 | 216.00 |

Legacy parity passed. Compile validity, control validity, canonical uniqueness,
and AST-shape expansion passed.

## Hard blocker

The OLD family itself passed the historical post-archive supply gate:

- exact supply: 272, required at least 144;
- behavior supply: 126, required at least 72.

The expanded formula-space behavior gate failed:

| Mode | Probe candidates | Behavior unique | Unique rate |
|---|---:|---:|---:|
| OLD | 256 | 126 | 49.2188% |
| Expanded | 256 | 111 | 43.3594% |

Expanded retained 88.0952% of OLD behavior discovery; the frozen requirement
was at least 90%. This is not a target-family exact-supply bottleneck. It is
evidence that the frozen `Sign` extension compressed behavior too much under
the deterministic probe.

The runner therefore stopped before sampled or full-coordinate financial
evaluation. The accurate failure class is
`FORMULA_SPACE_INCREMENT_NOT_PROVEN`; commit after this result separates that
status from `TARGET_FAMILY_SUPPLY_NOT_PROVEN`.

## Boundaries and disposition

- Phase3CM financial pairs: 0
- CEM updates: 0
- validation reads: 0
- holdout reads: 0
- 2026 reads: 0
- promotion or persistent optimizer state: 0

The detached helper fired the same one-time task twice sequentially. Both runs
stopped at the same deterministic pre-financial gate; no concurrent writer or
financial checkpoint existed. This is launcher/run-health evidence and does not
change route health.

`DISCLOSURE_EVENT` remains supply-qualified, but this frozen structural
extension and its CEM qualification are not accepted. Graph and Obsidian are
not updated because no durable search capability was promoted.

Any retry must be a newly frozen iteration selecting one different
same-signature extension from the existing semantic/operator authority. It must
first pass the same 256-candidate deterministic behavior gate; Phase3CM remains
forbidden until then.
