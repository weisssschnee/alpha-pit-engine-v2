# Decision Change Log

## 2026-07-15 - Runtime authority and split convergence

- Supersede `CN_FEATURE_RUNTIME_WIRING_MISMATCH_CONFIRMED` with `CN_RUNTIME_AUTHORITY_AND_SPLIT_CONVERGENCE_PARTIALLY_REPAIRED`.
- Preserve the prior audit and all historical proposal, reward and performance tables unchanged.
- Make the fixed 485-session manifest the sole formal split authority; deprecate worker-local splitting.
- Make `UnifiedCapabilityRegistry + TypedRouteCompiler` the sole candidate admission authority through immutable receipts.
- Retain legacy generators only as proposal sources. Physical schema presence remains a feasibility observation, not authorization.
- Reclassify pre-convergence search trajectories and pre-receipt evidence as diagnostic according to the machine-readable CSV.
- Record the actual 1/2/4 disjoint-shard parity failure and block shard-parallel portfolio/recovery from formal use.
- Do not apply for or start a capability run until the worker semantic blocker is resolved.
- Keep formal search, validation/holdout feedback, challenge, promotion, cross-sprint memory, plate/industry and forward 2026 frozen.
