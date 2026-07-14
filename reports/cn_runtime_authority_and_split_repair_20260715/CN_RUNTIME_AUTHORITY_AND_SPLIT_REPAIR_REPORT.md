# CN Runtime Authority and Global Split Convergence Repair

Status: `CN_RUNTIME_AUTHORITY_AND_SPLIT_CONVERGENCE_PARTIALLY_REPAIRED`

## Outcome

The fixed 485-session manifest is now mandatory at direct, serial, candidate-parallel, retry, diagnostic recovery and exact-merge boundaries. Formal workers no longer call a local fraction splitter. Unknown dates fail closed and dates in 2026 are rejected as sealed.

Every formal evaluator input must carry an immutable receipt produced by `UnifiedCapabilityRegistry + TypedRouteCompiler`. The receipt binds the candidate contract to registry, compiler, split manifest, data release and evaluator-code hashes. Phase3CN additionally checks the exact receipt hash before train-only feedback can reach scheduler/memory.

Legacy generators remain proposal sources. The conservative adapter can map only legal raw-minute, FirstN and PIT-qualified slow candidates. It does not infer Event/State/Regime semantics. Blocked metadata, unqualified raw fundamentals, wrong source lag, unresolved event state and plate placeholders fail closed.

## Engineering qualification

- Actual disjoint-shard worker counts 1/2/4 plus exact recovery: `FAIL`; max numeric error `12.0` and train-feedback max error `12.0` at tolerance `1e-12`. The mismatch exposed shard-local cross-sectional portfolio semantics, so the formal shard-parallel path now fails closed.
- Legacy direct versus receipt-gated synthetic parity: `PASS`; maximum reported error `0.0`.
- Fixed split: 485 sessions = 364 train / 73 validation / 48 holdout; manifest SHA-256 `fab9fb17642595456e10c4ad44357193f2dcdc1d39edd785b8298fbe9ca22241`.
- Validation and holdout were not read for decisions. 2026 was not accessed. No search, promotion or cross-sprint memory update ran.

## Historical evidence

Historical tables are preserved. Evidence produced before global worker authority/receipt enforcement is reclassified in `CN_HISTORICAL_EVIDENCE_RECLASSIFICATION.csv`; no historical metric was rewritten.

## Acceptance answers

1. Previous bypasses were Phase3CM direct/worker local splitting, Phase3CP schema-derived field admission, candidate/shard/serial evaluator entry, chunk-04 recovery workers and Phase3CN feedback without a receipt binding.
2. Phase3DV, RX/UCB, CEM, hybrid and fresh generators may still produce proposals; none can authorize fields or enter evaluation directly.
3. `UnifiedCapabilityRegistry + TypedRouteCompiler`, materialized as the candidate submission receipt, owns final field, route, primitive, PIT, source-lag and matched-control authorization.
4. No formal path grants search eligibility merely because a parquet column exists. The physical schema gate may discard infeasible proposals, but every survivor is still receipt-authorized before any evaluator call.
5. No worker-local split is reachable from the formal evaluator; `_split_map` was removed from Phase3CM.
6. Serial, candidate-parallel and retry paths require the same explicit manifest. Shard-parallel and historical chunk recovery also receive it, but are blocked from formal use because split consistency alone cannot repair shard-local cross-sectional ranks.
7. Validation and holdout remain report-only and cannot pass the Phase3CN train-role plus exact-receipt guard.
8. Actual 1/2/4 disjoint-shard worker and exact-recovery comparison failed with maximum numeric error `12.0`; the unsafe path is fail-closed rather than certified.
9. A non-performance, non-selected legal legacy static candidate preserved expression, signal, weights, turnover, cost, train-like synthetic metric and behavior identity under the receipt gate; maximum error was `0.0`.
10. Pre-convergence search trajectories, B1S selection evidence, pre-receipt unified discovery and provenance-unverified Phase3FIX evidence are diagnostic only; the exact classifications are in the CSV.
11. Engineering qualification to apply for a capability run is `NO` until global cross-section exact merge is implemented or shard-parallel portfolio evaluation is permanently removed from the contract.

## Frozen boundaries

`FORMAL_SEARCH_FROZEN`, `FORWARD_2026_SEALED`, `NO_CANDIDATE_PROMOTION`, `NO_CROSS_SPRINT_ADAPTIVE_MEMORY`, plate/industry disabled.
