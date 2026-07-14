# ADR 0004: Unified Candidate Authority and Fixed Split Runtime

Date: 2026-07-15

Status: Accepted

## Context

The accepted wiring audit found two independent runtime-authority gaps. Formal
Phase3CM workers could derive train, validation and holdout roles from their
local timestamp coverage, while recovery workers did not consistently receive
the fixed 485-session manifest. The legacy Phase3GA/Phase3CP chain could also
treat a physically present parquet field as candidate-eligible without proving
the field, route, primitive, observable-time and source-lag contracts against
the unified capability registry.

Final exact normalization could repair output labels, but it could not undo a
wrong split that had already influenced admission or reward. Likewise, a
schema filter could establish physical availability but not semantic authority.

## Decision

- The versioned 485-session manifest is the sole split authority for every
  formal direct, serial and candidate-parallel path.
- Missing manifests, unknown dates and any 2026 date fail closed. Validation
  and holdout remain report-only and cannot enter feedback.
- Legacy generators remain proposal sources only.
- Every proposal must pass `UnifiedCapabilityRegistry + TypedRouteCompiler`
  before admission and receive an immutable candidate submission receipt.
- Receipts bind candidate identity and contracts to registry, compiler, split,
  data-release and evaluator-code hashes.
- Phase3CM recompiles and validates every evaluator candidate against its
  receipt. Phase3CN accepts only train rows carrying the exact validated
  receipt hash propagated by Phase3CM.
- Proposal-only and receipt-authorized projections use separate namespaces;
  historical proposal output is retained and cannot be mistaken for admission
  input.
- Shard-parallel portfolio evaluation and its chunk-recovery route fail closed.
  They may return only after portfolio construction uses a global
  cross-section exact merge that passes actual 1/2/4-worker parity.

## Consequences

- Worker-local split inference is deprecated and unreachable from formal
  Phase3CM execution.
- Parquet schema presence can reject unavailable proposals but can never grant
  search eligibility.
- Existing pre-convergence trajectories remain diagnostic evidence and are not
  rewritten.
- Registry/compiler validation occurs once when receipts are frozen; workers
  consume the frozen receipt table and do not rebuild a second registry.
- Candidate-parallel is the supported multi-worker route: every candidate
  worker scans all shards and therefore preserves the full cross-section.
- Exact reward-atom merge cannot repair a portfolio already constructed from a
  shard-local cross-section; those historical outputs are diagnostic only.
- Formal search, validation, holdout, candidate promotion, cross-sprint memory
  and 2026 forward access remain frozen pending separate authorization.

## Evidence

- `src/our_system_phase2/services/fixed_split_authority.py`
- `src/our_system_phase2/services/candidate_submission_receipt.py`
- `tests/test_runtime_authority_convergence.py`
- `reports/cn_runtime_authority_and_split_repair_20260715/`
