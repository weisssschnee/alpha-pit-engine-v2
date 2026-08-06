# ADR 0015: Typed Candidate Program V1 representation boundary

- Status: Accepted
- Date: 2026-08-06
- Scope: CN candidate representation, compilation and proposal-lineage boundary

## Context

The existing CN stack already owns field and route authorization, compositional
proposal generation, immutable candidate/pair receipts, shared DAG evaluation,
continuous-book replay and fixed-stratified scheduling. It can evaluate legacy
single-expression candidates, but it did not have one typed envelope for a
candidate that combines heterogeneous stock, market, state and frozen-event
components while exposing score, eligibility, exposure and veto outputs.

Rebuilding those existing authorities would create duplicate control planes.
Making proposal seed, attempt order, wrapper identity or reward part of program
identity would also fracture semantic deduplication: an executable candidate
could become a different program merely because it arrived through another
registered proposal adapter.

## Decision

`CN_TYPED_CANDIDATE_PROGRAM_V1` is accepted as a `NON_FORMAL`, experimental
representation/compiler layer.

1. It reuses `UnifiedCapabilityRegistry`, `TypedRouteCompiler`, immutable
   candidate and pair receipts, Phase3CM shared DAG/evaluator and the frozen
   `TOPK_10_EQUAL` portfolio authority. It is not a new search, scheduler,
   evaluator, replay or promotion authority.
2. Semantic program identity binds typed nodes and edges, four explicit
   outputs, component semantic identities, joint-clock policy, matched-control
   transformation, portfolio contract and frozen component references. Reward,
   runtime, seed, attempt, proposal member IDs and registered wrapper provenance
   remain outside semantic identity.
3. Proposal lineage is separately self-hashed and fail closed. The compiler
   deterministically regenerates the registered grammar member and verifies the
   candidate, matched-control and pair IDs plus the registered generator
   authority/version/profile tuple before delegating to the existing route
   compiler.
4. Every output-dependent economic leaf must be covered exactly by the joint
   clock. PIT availability, maturity, source lag, revision policy, support and
   matched-control coordinate policies remain mandatory.
5. Existing Candidate Representation V0 scheduling remains authoritative for
   any separately authorized first production experiment: eight route/template
   strata, fixed sampling and no unified TPE credit or adaptive cross-template
   budget allocation.
6. Unregistered Plate, Industry and Billboard materialization fails closed.
   Broad Event remains a frozen reference and cannot become adaptive search
   input through this representation.
7. Qualification is zero-financial evidence only. It does not authorize search,
   validation, OOS, Forward-B access, alpha claims or promotion.

## Evidence

- Implementation SHA: `3ad4c3dd7005d9eafb5f16a1ef5573fb818bdddf`.
- Focused regression matrix: 176/176 passed.
- Both independent review axes: CLEAN.
- Smoke root:
  `runtime/cn_typed_candidate_program_v1_smoke_20260806_3ad4c3d`.
- Smoke manifest file/payload SHA256:
  `939fe0c96c9f308af5b05ea63f1a2824a235f43f0250ea5971a6d51ac34dc319` /
  `1caaca3c5e8e87c8bf9bb611df17f6fd0ada4d04e19ece93a362e82e49c45f21`.
- Closure file/payload SHA256:
  `49f267d03e06c7f6b51911abb1be3a0d3f2c31ba775280598c80f9b3cef33c45` /
  `43057a328009caedc9290b6e83295f9e59a2aca265e6392712241b985351f9c6`.
- Independent audit payload SHA256:
  `dfe08d08a6db1cced44d2040210f50e998c39a2887e4d216bc9df254962633ce`.
- Six fixtures were closed in fixed order, five compiled, Billboard failed
  closed, all 21 artifacts verified, and financial/sealed reads were zero.

## Consequences

- The project can express and compile heterogeneous candidate programs without
  duplicating the accepted field, route, receipt, evaluator or portfolio stack.
- Raw grammar and registered wrapper paths converge on one semantic program
  identity while retaining independently auditable proposal provenance.
- A later bounded experiment may measure program productivity, but no result is
  implied by this qualification and no adaptive allocator is authorized.

## Rollback

Disable Candidate Program V1 entry points and retain legacy candidate paths.
Do not remove or replace any reused authority. A future promotion to a formal
representation role requires a separate accepted ADR and owning-authority
transition.
