# CN true1min State

Updated: 2026-07-15

Current state: `CN_RUNTIME_AUTHORITY_AND_SPLIT_CONVERGENCE_PARTIALLY_REPAIRED`

## Accepted foundations

- `PHASE_A_EVALRESET_ACCEPTED`
- `NEXTGEN_DARK_INFRASTRUCTURE_READY`
- `CN_GENERATOR_RESEARCH_SPRINT1_PARTIALLY_COMPLETED`
- `FORWARD_2026_SEALED`
- `NO_CANDIDATE_PROMOTION`
- `NO_CROSS_SPRINT_ADAPTIVE_MEMORY`
- `FORMAL_SEARCH_FROZEN`
- `BROAD_EVENT_CAPABILITY_NOT_EVALUATED`
- `CURRENT_EVENT_TRIGGER_SEMANTICS_MISMATCH`
- `EVENT_AUDIT_CONTRACT_INVALID`
- `EVENT_LANE_ZERO_BUDGET_NOT_EXECUTED`
- `EVENT_DATA_NOT_INVALIDATED`
- `BROAD_EVENT_INCREMENT_OBSERVED_REPRODUCIBLE`
- `BROAD_EVENT_DISCOVERY_ENTRY_AUTHORIZED`
- `CN_BROAD_EVENT_SYSTEM_RECOVERY_COMPLETED_DISCOVERY_ELIGIBLE`
- `CN_PIT_FUNDAMENTAL_FABRIC_PARTIALLY_COMPLETED`
- `CN_UNIFIED_CAPABILITY_DISCOVERY_COMPLETED_DEVELOPMENT_ONLY`
- `CN_FEATURE_RUNTIME_WIRING_MISMATCH_CONFIRMED`
- `CN_RUNTIME_AUTHORITY_AND_SPLIT_CONVERGENCE_PARTIALLY_REPAIRED`
- `UNIFIED_DISCOVERY_CHALLENGE_ELIGIBILITY_SUPERSEDED`

## Runtime authority and split convergence result

The accepted non-performance repair makes the 485-session manifest the sole
formal split authority: 364 train, 73 validation and 48 report-only holdout
dates. Direct, serial and candidate-parallel paths require the manifest.
Phase3CM no longer has a worker-local `_split_map`; unknown dates hard-fail and
2026 is explicitly sealed rather than mapped to holdout. Shard-parallel
portfolio evaluation and its chunk-recovery route now fail closed because
their shard-local cross-sections are not worker-count invariant.

`UnifiedCapabilityRegistry + TypedRouteCompiler` now authorize every formal
evaluator input through an immutable candidate submission receipt before proxy
admission. The receipt binds the real expression and lineage to registry,
compiler, split manifest, data release and evaluator-code hashes. Phase3CN
requires the same receipt hash plus train-only reward metadata before feedback
can reach scheduler or memory.

Legacy Phase3DV/RX/UCB/CEM/hybrid/fresh components remain proposal sources
only. A conservative adapter maps legal raw-minute, FirstN and PIT-qualified
slow proposals; it does not infer Event/State/Regime semantics. Physical
parquet presence cannot authorize a field. Blocked metadata, direct raw
fundamentals, wrong lag, latched Event/State bypass and plate placeholders fail
closed.

Actual Phase3CM entrypoint replay with disjoint shard assignments found that
1/2/4-worker shard-parallel portfolio results are not equivalent: the maximum
numeric error is 12.0 at tolerance 1e-12. Exact atom recovery cannot repair a
portfolio that was already formed inside a shard-local cross-section. The
unsafe route is therefore blocked; candidate-parallel remains the supported
worker route because each worker evaluates full cross-sections. A
current-contract-legal frozen historical candidate did preserve expression,
signal, weights, turnover, cost, synthetic train-like metric and behavior
identity through the receipt gate with zero error. This is engineering
evidence, not Alpha or promotion evidence.

The completed two-seed unified run remains development-only diagnostic evidence.
Its four shared exact survivors are old frozen Broad Event replays; no challenge
was opened and no historical output was rewritten.

Phase A conclusions remain unchanged: generation structural redundancy exists,
but catastrophic signal-level collapse and downstream concentration
amplification were not observed. The deterministic signal-sketch fidelity and
stage-wise concentration audit remain accepted evidence.

## Sprint-2 result

The existing survivor label is now `DEVELOPMENT_ELIGIBLE`. A frozen,
group-aware cross-fitted `STRICT_PRIORITY_ELIGIBLE` layer reduces it to 10%
and beat the current scalar in realized strict top-decile selection in all
three Epoch-C seeds.

Repair Capability CANARY and Epoch-C both completed on the 77o machine using
only the physical development release. Every validation, holdout, forward,
forbidden-file and forbidden-row-group counter is zero.

Epoch-C delivered:

- 49,152 execution rows across three seeds;
- 8,192 / 8,192 exact shared-backbone identities;
- 32,739 / 32,768 globally unique exact proposals;
- 1,024 / 1,024 strict evaluations;
- RX/UCB strict matched-control outperformance in all three seeds;
- 250 temporal and 465 state clusters outside static per seed;
- zero event clusters because the event lane had zero proposal, admission and
  strict budget and was not executed;
- a 377-identity research-only strict union with 320 identities shared by all
  three seeds and three-way Jaccard 0.8488;
- positive mean benchmark increment of 0.03967 for the strict union.

State novelty is operational, but average state strict evidence is weaker than
the matched static group. It is not described as independent economic
increment.

## Broad Event recovery result

The r5 development-only CANARY completed on 77o at
`f0326962a90e86fa1de3c249760b45a7e4d99542`. All validation, holdout, 2026,
forbidden-file and forbidden-row-group counters are zero. It materialized
477,497 eligible episodes across eight operational sources.

The conservative limit lifecycle accepted 243 of 263 vendor occurrences;
241 accepted episodes aligned within two minutes. Precision is 99.18% and
vendor coverage is 92.40%. Rejected uncertain sessions remain unknown rather
than negative observations.

Two fixed seeds produced 18 and 19 matched-control survivors. Eleven mechanisms
reproduced across both seeds and occupy ten behavior clusters separated from
structural, static and temporal controls. The reproduced sources are
billboard change, chip-structure change, hotness change and market-ecology
transition. Limit lifecycle and vendor occurrence passed support but did not
produce a shared survivor in this CANARY.

The mean matched increment across all Event proposals is negative. The accepted
interpretation is localized reproducible Event mechanisms, not uniform Broad
Event superiority. The 11-mechanism discovery entry pack is research-only and
cannot promote candidates or write memory.

## Event interpretation correction

The previous broad Event denial is withdrawn. The historical generator used
only `Transition($evt_uplimit_active,0,1)`, while `evt_uplimit_active` is a
same-day cutoff-latched observation that remains one through the close. It is
not a real-time sealed-board lifecycle state. Requiring that field to exhibit
1-to-0 exits, board breaks or reseals made the historical Event audit contract
invalid.

Epoch-C assigned event-conditioned proposal, admission and strict budgets of
zero. Its zero event clusters therefore mean `NOT_EXECUTED`, not failed
execution. Historical proposal, strict and performance artifacts remain
unchanged; only their active interpretation is superseded.

## PIT Fundamental Fabric result

`CN_PIT_FUNDAMENTAL_FABRIC_V1` inventories 1,227 union-schema fields from
26,188 symbol-partitioned parquet files. Balance-sheet, profit, cash-flow and
major-holder families now have versioned semantic/observable-time contracts,
a conservative stock-session as-of resolver, deterministic session cache and
lazy typed Feature Fabric routes. No new field is exposed to a generator.

The development-only audit stopped at `2025-07-07T15:00:00`. It covered
274,761 balance-sheet rows, 269,643 profit rows, 259,237 cash-flow rows and
2,615,891 holder rows representing 262,629 disclosure episodes. Validation,
holdout, 2026 performance, reward and promotion were not read or executed.

The status is partial for two evidence-backed reasons. `zygc_em` has 17 fields
and 945,812 rows but no independent disclosure/observable clock, so its values
were not read and the family remains `PIT_CONTRACT_UNRESOLVED`. The three
statement tables are current snapshots: the resolver prevents later updates
from appearing early, but superseded historical revision values do not exist
in this release and therefore cannot be replayed.

## Partial-close reason

RX seed expansions overlapped by 29 exact identities. The historical Epoch-C
therefore missed its frozen global-unique proposal contract even though the
strict budget completed. A seed-namespace guard and three-seed regression test
prevent recurrence; the completed run is not relabeled.

All ten research success criteria pass, but contract fidelity does not.
No forward-authorization candidate pack was created.

## Frozen boundaries

- Validation, holdout, spent, sealed and 2026 data remain unavailable to
  reward, admission, scheduler, family decisions and memory.
- Plate/industry remains user-deferred and disabled.
- Broad Event semantics, multi-source episodes, matched controls and CANARY are
  complete. Its byte-for-byte frozen entry pack was replayed by the unified
  development-only run; it was not modified, promoted or written to memory.
- All 1,227 raw fundamental source fields remain outside direct generator
  exposure. The 147 canonical representations can be authorized only by the
  unified typed receipt route; no formal fundamental search was run.
- Sprint-2 selector, RX values and strict results are archived evidence, not
  persistent positive or negative memory.
- Epoch-D remains unrun. No fundamental performance experiment has been run.

## Next formal decision point

Recommendation: `REPAIR_GLOBAL_CROSS_SECTION_MERGE_BEFORE_CAPABILITY_RUN`.

Candidate and split authority convergence is complete, but overall engineering
qualification remains partial until shard-parallel portfolio evaluation is
either replaced by a global cross-section exact merge or removed from the
formal contract. Any next run still requires a separate frozen contract and
explicit approval.
Business composition remains `PIT_CONTRACT_UNRESOLVED`, plate/industry remains
disabled, formal search stays frozen, validation/holdout remain report-only,
and 2026 stays sealed.
