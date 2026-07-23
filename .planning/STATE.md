# CN true1min Current State

Updated: 2026-07-23

Current state: `CN_CORE_PACK_LARGE_DEVELOPMENT_COMPLETE_REPORT_ONLY_VALIDATION_COMPLETE_CANDIDATE_DECISION_PENDING`

Mission authority: `.planning/PROJECT.md`

Progress authority: this file

Architecture input: `config/architecture_overlay.json`

Generated CURRENT: `.planning/graphs/current.json`

Complete field authority: `runtime/field_registry/cn_field_master_registry_v1/cn_field_master_registry_v1.json`

## Current accepted capabilities

- `cn_core_pack_development_discovery_v1` is now the active proposal-root
  ontology authority for bounded development discovery. It remains subordinate
  to the unified registry `route_id` authority for route eligibility and
  top-level scheduling. Sidecar schemas filter execution compatibility only and
  cannot redefine ontology scope. The separate active authorization permits
  bounded development execution, but every campaign must still freeze its own
  nonzero budget, seeds, archive snapshot and no-promotion boundary.
- The separately frozen six-checkpoint Core-Pack large development campaign is
  complete on 77o. It scheduled 1,536 pairs, made 16,850 deterministic
  generation attempts, admitted and completed full-coordinate development
  evaluation for 872 matched pairs (1,744 candidate members), and assigned 750
  new behavior families. All seven primary registry routes were represented;
  578 of 872 pairs had positive matched development net increments. These are
  development observations only and do not authorize promotion or an economic
  claim. The compact immutable receipt is
  `runtime/run_plans/cn_core_pack_large_development_20260723_receipt.json`.
- Every train checkpoint is `BATCH_CLOSED_IMMUTABLE`. Post-validation
  verification matched 85/85 artifacts: checkpoint 001 retained 15/15 and
  checkpoints 002-006 retained 14/14 each. The four cumulative train artifacts
  and `train_complete_manifest.json` also matched their frozen hashes. Closed
  Phase3CM results were reused through the existing checkpoint path and were
  never recomputed during recovery.
- Immutable train completion automatically launched the existing report-only
  validation path. It evaluated the same 872 pairs (538 active-bar and 334
  stock-session) over 92,359,058 validation reads. Both backend results closed
  with return code zero; holdout and 2026 reads were zero, feedback/scheduler/
  archive writes and automatic promotion were forbidden, and all train hashes
  remained unchanged.
- The post-close result interpretation is now bound to the immutable result
  hashes in
  `runtime/run_plans/cn_core_pack_large_development_result_interpretation_20260723.json`.
  Of 872 submitted primary pairs, 533 were validation-evaluable and 339 were
  blocked by empty/constant signal or empty common-support semantics. The 333
  positive `pair_validation_report_metric` rows mean that the primary
  validation composite score exceeded its matched control; they do not mean
  333 standalone positive-Sortino or profitable candidates.
- Phase3CM Sortino is stored unannualized (`annualizer=1.0`). The raw train
  maximum was 1.1374896, but that candidate had a negative worst-horizon
  Sortino and its validation pair was blocked. A high-coverage active-bar
  reference, `cn.comp.f9ea7251e72ce711baa4`, retained positive train and
  validation day/worst-horizon Sortino, positive matched-control validation
  increment and 91,578,361 validation support rows. A compact joint-positive
  screen retained 12 analysis candidates, but it is not a global tournament,
  admission decision, OOS result or promotion list.
- Validation reward rows retain recomputed validation Sortino under legacy
  train-prefixed field names while their split rows are correctly tagged
  `validation`; the explicit `validation_day_sortino` reward field is blank.
  This reporting-semantic gap does not mutate immutable train artifacts and
  must not feed search. Candidate interpretation therefore uses the bound
  validation result and records the mapping explicitly rather than presenting
  `pair_validation_report_metric` as BestSortino.
- The completed train and validation results are unequivocally long-only:
  all 3,488 candidate reward row instances use
  `portfolio_mode=long_only_top`, all have `short_allowed=false`, and the
  long-only portfolio branch writes zero short positions. This proves that
  shorting was disabled for this campaign. It does not yet prove complete
  enforcement of every A-share execution rule such as T+1, limit-lock
  fillability and suspension handling; those remain outside the present
  economic claim.
- Large-run acceleration was exercised on real work. The frozen checkpoint-001
  gate allocated 30 active-bar compute threads, measured 19.28 effective cores,
  60.25% logical-CPU occupancy, 58.54% Task-Manager-equivalent mean host CPU and
  20,019,281,920-byte peak RSS. It recorded
  `FULL_HOST_NATIVE_KERNEL_SMT_CEILING_PROVEN` as the alternative bottleneck to
  the 75% host-occupancy target. Stock-session retained its two-thread native
  contract. Cache pressure, memory-headroom and host-connectivity failures are
  preserved as run-health evidence; they never mutated financial route health.
- The 77o authority smoke closed end to end. A 28-pair execution pack was drawn
  from a separate 84-pair supply probe; every one of the seven primary routes
  produced 12 exact-unique pairs within 12-29 attempts under the 1,024-attempt
  cap. Behavior-aware admission retained 20 pairs with all seven routes
  represented. Full-coordinate development Phase3CM train completed 11
  active-bar plus nine stock-session pairs and wrote immutable
  `TRAIN_COMPLETE`. Validation then started automatically in the same workflow,
  evaluated the same 20 pairs over 92,359,058 validation rows, and completed as
  report-only with train hashes unchanged. Validation feedback, scheduler write,
  behavior-archive write, promotion, holdout and 2026 access were all forbidden
  or zero. The immutable compact receipt is
  `runtime/run_plans/cn_core_pack_authority_smoke_20260722_receipt.json`.
- The earlier `ACTIONABLE_FEEDBACK_CLAMPED` exact-supply result is therefore not
  a stable `RegistryDrivenGenerator` route bottleneck at the tested 12-pair,
  frozen-seed scale. This is deliberately bounded: it does not claim unlimited
  supply or extrapolate beyond the tested seeds and historical archive snapshot.
  The smoke used the existing Phase3CM recovery path; infrastructure-only native
  thread contract mismatches were fixed without recomputing completed train
  results.
- Acceleration was exercised rather than inferred. The active-bar train backend
  allocated 11 compute threads and reported `PARALLELISM_ENGAGED`; effective
  cores were 10.02 for rank/mapping, 10.43 for the Value DAG and 7.09 for
  turnover/cost. Peak RSS was 8,663,023,616 bytes, below the 24 GiB gate. The
  observed hot path remained `MIXED_COMPUTE_BOTTLENECK`. Stock-session used the
  existing two-thread native-pool contract.
- Phase A EVALRESET and NEXTGEN-DARK infrastructure remain accepted.
- The bounded route-supply closure passed on 77o. Across two fixed seeds and
  attempt caps 64/256/1024, the legacy constructor showed stable post-archive
  exact-supply exhaustion for Disclosure, Intraday State, and Minute Static;
  this was diagnosed as `LEGACY_CONSTRUCTOR_CYCLE_EXHAUSTION`, not a registry
  route-authority failure. The route-local `registry_compositional_v2` profile
  preserved `RegistryDrivenGenerator` and registry `route_id` authority while
  restoring all seven primary search routes to at least 2x the 12-pair exact
  threshold. Broad Event remained a zero-budget frozen reference.
- A first materialized probe exposed a narrower Disclosure supply gap: eight of
  12 exact pairs referenced fields absent from the current stock-session
  sidecar. Generation is now constrained by the active sidecar schema, and the
  label-free Disclosure probe may use up to 12 condition-activation development
  dates. The first four-route run then exposed a State-specific false bottleneck:
  512/512 attempts were rejected because a lineage-only synthetic state ID was
  treated as a required physical sidecar column even though the expression was
  already expanded to materialized source leaves. Availability filtering now
  checks the expression's actual `$field` leaves while retaining the full
  registry declaration and compiler checks. The final bounded run generated 12
  materializable pairs for each of Disclosure, Market, Intraday State, and
  Minute, then selected only four per route for full-coordinate development
  Phase3CM evaluation. All 16 were resolved, exact behavior-unique within route,
  and retained all four identities. Both 37-block backends completed on 77o with
  zero validation, holdout, or 2026 reads and promotion forbidden. This closes
  the route-supply prerequisite but does not authorize formal or large-scale
  search; `registry_compositional_v2` remains an experimental constructor profile
  requiring explicit acceptance in the next campaign freeze.
- `CN_ITERATIVE_SEARCH_V1` is now closed as a bounded, development-only
  experimental capability. Three immutable batches scheduled 48 registry-route
  proposal pairs each, admitted and completed full-coordinate development
  Phase3CM pair evaluation for 20, 12, and 17 pairs, and retained natural
  underfill instead of manufacturing a 24-pair quota. Batch 1 and Batch 2
  consumed only the immediately preceding immutable feedback snapshot.
- The V1 scheduler uses unified registry `route_id` as its only top-level key;
  generation modes are route-local action labels and legacy arm profiles are
  compatibility only. Pre-admission uses label-free bounded behavior probes,
  unresolved behavior fails closed without structural-family fallback, and
  final exact behavior signatures plus approximate behavior families are
  assigned only from full-coordinate portfolio behavior.
- The feedback-on/off control shared seed, master attempt stream, total budget,
  historical behavior archive, exact/behavior dedupe, registry, and compiler.
  Its master stream hash matched the feedback-on Batch 1 hash exactly while
  route budgets changed. The closure also compared actual legal/canonical pair
  counts route by route. Slow XS and Slow Temporal actual exposure decreased in
  the scheduled negative direction. State expansion and Minute downweight were
  supply-clamped and are recorded as `ACTIONABLE_FEEDBACK_CLAMPED`; FirstN and
  Market increases came from spillover and are explicitly excluded from
  feedback causality. Feedback-off kept the Batch 0 prior. All 49 full behavior
  rows retain all four identities. Synthetic positive and negative rules
  passed. Infrastructure remained separate from financial route health.
- The 77o canary evaluated 49 matched pairs with nine positive-policy and 40
  negative-scheduler observations. Run-health failures and validation, holdout,
  and 2026 reads were all zero; promotion remained forbidden. These are
  campaign mechanics and development evidence, not alpha or economic proof.
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
  the support roots actually required by present constructors. Its proposal-root
  ontology and separate bounded-development authorization are active; this does
  not create a standing campaign budget or authorize Strict Stage A. Plate-minute
  roots remain excluded.

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

There is no remaining global route-supply, ontology-authority,
train-to-validation or acceleration-readiness blocker in the completed campaign.
Its preserved cache, memory-headroom and monitor-path incidents are run-health
facts, not financial route failures. The 93 unresolved rows in the historical V1
archive remain immutable and are not retroactively rewritten. Formal search,
candidate promotion, holdout access, 2026 access and economic claims remain
blocked by their existing authorization and evidence boundaries. Plate remains
independently blocked by missing real PIT minute materialization; additional
compound-state expansion beyond tested expanded source-leaf forms remains a
localized materialization gap. The immediate research blocker is now a bounded
candidate decision, not another whole-batch qualification: either deepen the
current candidate mechanisms before another validation/OOS sequence, or freeze
the current candidates and test them on untouched OOS first. Validation already
used for candidate interpretation is spent and cannot be relabeled as OOS.

## Next action

Do not repeat the 1,024 wave, V1 canary, field qualification, route-supply
qualification, authority smoke, or the just-completed six-checkpoint large
campaign. Do not rank event, state, slow fundamental/chip and active-bar
candidates as one global race. The only next research decision is:

1. expand development search around the current candidate mechanisms and
   behavior families, then repeat report-only validation and obtain additional
   untouched OOS evidence; or
2. recommended default: freeze the current behavior-distinct candidates,
   require non-collapse on explicitly authorized untouched OOS, classify the
   survivors by economic mechanism, intended regime/event/state and portfolio
   role, then expand search only for materially uncovered regimes.

Both options preserve matched controls, long-only execution, immutable evidence
and the existing registry/evaluator authority. Batch-average or route-average
weakness cannot reject a best candidate, while a best candidate cannot claim
coverage of regimes it was not designed to serve. Preserve the fixed 8 GiB cache
cap and proven runtime envelope. `exploit`, `repair`, and `orthogonal` remain
labels unless a real registry-backed constructor is added and tested. Broad
Event remains a zero-budget frozen reference. Strict Stage A, candidate
promotion, cross-campaign adaptive memory, holdout access and 2026 access remain
forbidden until separately authorized. Plate resumes only when real PIT minute
materialization is present on 77o.
