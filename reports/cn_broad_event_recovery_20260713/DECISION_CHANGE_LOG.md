# CN Broad Event Recovery Decision Log

Updated: 2026-07-13

## Supersession preserved

The Sprint-2 proposal, strict and performance tables remain unchanged. The
historical single-trigger Event interpretation was withdrawn because
`evt_uplimit_active` is cutoff-latched, the old lifecycle audit used invalid
exit/reseal semantics, and Epoch-C assigned zero Event budgets. Status v1
records that atomic interpretation correction.

## Recovery execution

- r2 stopped before CANARY with insufficient lifecycle support because missing
  PIT ST context was treated as a universal exclusion.
- r3 established support for all eight Event sources, then stopped because the
  validator incorrectly counted conservatively rejected sessions as false
  recognitions.
- r4 completed the two-seed CANARY, but its reporting layer used canonical
  mechanism IDs as signal clusters and let the all-proposal mean override
  preregistered survivors. Raw r4 outputs remain immutable and are explicitly
  superseded only for interpretation.
- r5 kept the same proposal/reward distribution, added exact behavior identity
  and correlation clustering, and completed at
  `f0326962a90e86fa1de3c249760b45a7e4d99542`.

## Accepted r5 evidence

- 477,497 eligible episodes across eight operational sources.
- Limit lifecycle: 243 / 263 vendor episodes conservatively accepted; 241 / 243
  align within two minutes (99.18% precision, 92.40% vendor coverage).
- Per seed: 96 Event proposals, 96 legal, 96 canonical, 46/43 exact behaviors,
  39/36 behavior clusters, N_eff 27.59/26.64, and 18/19 survivors.
- Eleven mechanisms reproduce across both seeds and occupy ten new behavior
  clusters separated from structural, static and temporal controls.
- Reproduced sources: billboard change, chip-structure change, hotness change
  and market-ecology transition.
- Limit lifecycle and vendor occurrence are operational but did not produce a
  shared survivor in this run.
- The all-proposal mean matched increment is negative. The result supports
  localized reproducible mechanisms, not uniform Broad Event superiority.
- Every validation, holdout, 2026, forbidden-file and forbidden-row-group read
  counter is zero.

## Disposition

- `CN_BROAD_EVENT_SYSTEM_RECOVERY_COMPLETED_DISCOVERY_ELIGIBLE`
- `BROAD_EVENT_INCREMENT_OBSERVED_REPRODUCIBLE`
- `BROAD_EVENT_DISCOVERY_ENTRY_AUTHORIZED`
- `FORWARD_2026_SEALED`
- `NO_CANDIDATE_PROMOTION`
- `NO_CROSS_SPRINT_ADAPTIVE_MEMORY`

The 11-mechanism pack may enter only a newly frozen development candidate
discovery contract. It does not authorize formal search, automatic promotion
or sealed-data access.

## Fundamental field clarification

The local data estate includes PIT balance-sheet, profit, cash-flow,
major-holder and business-composition sources. The current 121-field Fabric
materializes valuation, market-cap and holder-context subsets, not the full
financial statements. Full statement fields require a versioned observable-time
and source-lag contract before search use and were not added to r5.
