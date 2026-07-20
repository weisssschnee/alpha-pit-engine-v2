# CN true1min Current State

Updated: 2026-07-20

Current state: `CN_PHASE3CM_1024_DEVELOPMENT_CONFIRMATION_COMPLETED_SEARCH_FROZEN`

Mission authority: `.planning/PROJECT.md`

Progress authority: this file

Architecture input: `config/architecture_overlay.json`

Generated CURRENT: `.planning/graphs/current.json`

Complete field authority: `runtime/field_registry/cn_field_master_registry_v1/cn_field_master_registry_v1.json`

## Current accepted capabilities

- Phase A EVALRESET and NEXTGEN-DARK infrastructure remain accepted.
- The frozen current kernel passed exact semantic parity on all 146 active-bar
  pairs and 292 candidate members. The cumulative development confirmation then
  completed 1,024 matched pairs: 584 active-bar and 440 stock-session. All pair
  identities were observed, 989 pairs were evaluable, and 35 were blocked by
  local empty/constant/identity-control semantics. No pair failed because of
  infrastructure, and validation, holdout and 2026 reads were all zero.
- Across the 989 evaluable pairs, 572 had positive matched development reward
  increment and 707 had positive matched Rank-IC increment. Mean/median matched
  reward increments were +0.08451/+0.01289; mean/median Rank-IC increments were
  +0.01903/+0.00559. Intraday state transition was the clearest reward-positive
  route (+0.28067 mean, +0.20734 median, 78.8% positive). Slow cross-sectional
  level was also broadly positive (+0.14512 mean, 69.2% positive). Minute static
  had the strongest Rank-IC breadth (92.4% positive) but a much smaller median
  reward increment than its mean. FirstN had positive Rank-IC breadth but negative
  net reward, market-regime conditioning had no reward increment, disclosure was
  mixed, and slow temporal change was weakly positive.
- The experimental streaming backend remained within the frozen resource
  contract: active-bar processed 446,443,583 rows, stock-session 1,852,463 rows,
  both reported `PARALLELISM_ENGAGED`, and peak active RSS was 20,776,857,600
  bytes below the 24 GiB hard gate. Active cumulative evaluator wall time was
  13,706.14 seconds. The official hot-path result is
  `MIXED_COMPUTE_BOTTLENECK`: rank/mapping was 57.2% and Value DAG 40.5% of the
  three compute-heavy phases, so neither crossed the frozen 60% classification
  threshold. The kernel remains `PARTIALLY_QUALIFIED` and an
  `EXPERIMENTAL_BACKEND`; the formal evaluator authority is unchanged.
- Attempt 1 was interrupted after active block 17 by two orphaned diagnostic
  PowerShell inspectors that consumed about 77.9 GB combined working set. They
  were identity-checked and removed, restoring free memory from 1.17 GB to
  77.97 GB. The unchanged execution plan resumed from the complete checkpoint
  and passed all 37 blocks. The interrupted-attempt evidence is retained without
  rewriting the final execution receipt or historical 256-pair evidence.
- The complete CN field master contains 1,683 records:
  - 1,227 PIT fundamental source fields;
  - 420 registered capability fields;
  - 9 chip sidecar fields;
  - 8 plate-market fields;
  - 13 true1min plate aggregate fields;
  - 6 PIT plate-membership metadata fields.
- The unified capability registry contains 285 canonical fundamental
  representations within the frozen 384-root limit. Disclosure has four event
  pulses plus 138 qualified level/change payloads.
- PIT Fundamental Fabric remains partial: statement snapshot releases cannot
  replay absent superseded values, and `zygc_em` remains
  `PIT_CONTRACT_UNRESOLVED`.
- Chip and plate data families are registered in the complete inventory and
  have tested sidecar/materialization implementations. They are not thereby
  authorized for formal performance search. Plate membership clocks and
  survivorship contracts remain mandatory.
- Broad Event recovery and its frozen discovery entry pack remain accepted
  research evidence. Historical packs are immutable evidence snapshots, not
  field or route authorities.
- `CN_FULL_FIELD_INFORMATION_RESEARCH_V1` now covers all 1,683 authoritative
  field records in one machine-readable universe. It combines the existing
  1,227-field fundamental qualification matrix with runtime evidence for 421
  typed capabilities: 117 true1min, 284 canonical PIT fundamental, nine chip,
  and 11 frozen Broad Event entries. No raw fundamental source column was
  opened to the generator.
- The 77o run used 246,569 deterministic true1min sample rows and successfully
  materialized every one of the 284 eligible canonical fundamental
  representations with zero materialization errors. It measured 4,931
  within-semantic-support-group NMI pairs and produced a 272-field
  `EXPLORATORY_NON_PERFORMANCE_CORE_PACK`: 62 true1min, 204 fundamental, and
  six chip representatives. This is information/redundancy evidence only; it
  changes neither generator authority nor search admission.
- Real PIT plate-minute materialization was not present on 77o. All 21 plate
  capabilities and six membership metadata records therefore remain explicitly
  `NOT_EVALUATED`; no current snapshot, placeholder, or `plate=0` substitute
  was used. `zygc_em` remains `PIT_CONTRACT_UNRESOLVED`.
- The 77o Core Pack generator review executed 466,944 deterministic structural
  attempts across two seeds. Every constructed primary/control pair compiled
  legally, all declared route skeletons were exercised, and the current typed
  grammar produced 42,189 route-level distinct exact/canonical identities
  without reading market data, returns, reward, validation, holdout, or 2026.
- The current grammar consumes 264 of 272 Core Pack roots. Seven roots remain
  correctly fail-closed: five unqualified RZRQ unit fields, unqualified `vol`,
  and latched `evt_uplimit_active` with no direct typed route. The eighth,
  `state_close_range_location_sign`, is registered and information-qualified
  but held because expanding its compound state expression exceeds the current
  depth/leaf contract.
- Four disclosure-age roots are now materialized over the complete 1,852,463-row
  development stock-session panel with nonzero support and 98.89%-99.11%
  coverage. Each has a PIT session as-of materialization/support receipt. Their
  claim remains conservative `PIT_SAFE_CURRENT_SNAPSHOT_ONLY`; they do not
  reconstruct superseded revisions or original disclosure-age history.
- The supplemental typed structural smoke retained 122 exact-unique pairs from
  384 fixed attempts: 19 slow-level disclosure pairs and 97 market-regime pairs
  are runtime-ready, while six intraday-state pairs remain frozen pending the
  compound state materialization. Campaign budget consumption was zero and no
  performance, reward, candidate promotion, validation, holdout or 2026 access
  occurred.
- `cn_core_pack_development_discovery_v1` freezes reachable Core roots plus only
  the support roots actually required by present constructors. It has zero
  active proposal, admission, and strict-evaluation budget and is not search
  authorization. Plate-minute roots remain excluded.

## Hard boundaries

- `FORWARD_2026_SEALED`
- `FORMAL_SEARCH_FROZEN`
- `STRICT_STAGE_A_NOT_AUTHORIZED`
- `NO_CANDIDATE_PROMOTION`
- `NO_CROSS_SPRINT_ADAPTIVE_MEMORY`
- validation and holdout cannot influence reward, admission, scheduler,
  family decisions, or memory;
- a field's presence in the complete master does not grant generator exposure;
- raw 1,227-field fundamental exposure and unresolved PIT families fail closed.

## Current blocker

The 146 exact-parity gate and cumulative 1,024-pair development confirmation are
complete. They establish broad route-asymmetric development evidence but do not
provide independent seed/time-block stability, validation, holdout or OOS
evidence. Formal discovery therefore remains blocked by a separate explicit
authorization and a newly frozen formally evaluable route pack. Plate remains
independently blocked by missing real PIT minute materialization; the compound
intraday state root remains a localized materialization gap. FirstN and
market-regime conditioning require targeted objective/cost or support diagnosis,
not a blind rerun of all 1,024 pairs. No further kernel optimization or replay is
in scope.

## Next action

Do not repeat the 1,024 wave. The next formal decision is whether to prepare a
new frozen, development-only route pack centered on the routes with coherent
matched reward and Rank-IC evidence, while keeping FirstN, market regime,
disclosure and slow temporal routes diagnostic until their route-specific issues
are resolved. That preparation is not Strict Stage A authorization. Strict Stage
A, candidate promotion, adaptive memory, validation/holdout access and 2026
access remain forbidden. Plate resumes only when real PIT minute materialization
is present on 77o.
