# CN true1min Current State

Updated: 2026-07-31

Current state: `CN_SEARCH_RUN_INVALID_VALIDATION_RECOVERY_ACTIVE`

Mission authority: `.planning/PROJECT.md`

Progress authority: this file

Architecture input: `config/architecture_overlay.json`

Generated CURRENT: `.planning/graphs/current.json`

Complete field authority: `runtime/field_registry/cn_field_master_registry_v1/cn_field_master_registry_v1.json`

## Current accepted capabilities

- The user explicitly superseded the prior no-successor hold for one bounded
  dual-lane run. The active train lane is a full 12,288-formal-ask
  winner-guided continuation search on 77o, not a reduced canary:
  8 checkpoints x 1,536 asks, with 1,504 Slow Temporal and 32 First-N asks per
  checkpoint. It uses fresh in-memory TPE, the existing Availability
  Controller and Phase3CM, 32 compute threads and no cross-campaign reward or
  optimizer state. Its exact deployed SHA is
  `69e33b6bd0d9e213326aaa4340e4ba8351158c52` and its campaign root is
  `D:\ChengboRemote\runtime\cn_winner_guided_large_search_continuation_20260731_1025_69e33b6_12288`.
  Final-SHA zero-financial preflight passed with 16,817 fresh Slow Temporal
  identities versus 14,439 required and 1,037 fresh First-N identities versus
  308 required; validation, holdout and 2026 reads were zero.
  Checkpoints 001-003 are independently verified
  `BATCH_CLOSED_IMMUTABLE`: 4,608 formal fresh exact asks, 2,334 evaluated
  pairs, zero primary exact/pair/full-behavior-signature duplicates, exact
  ask/observation/transcript coverage, zero scored reward mismatch and zero
  sealed-period reads. Productive counts were 246/231/233.
- The original search task `lanjob_20260731_101807_291ccb` stopped at unclosed
  checkpoint 004 because the unchanged runtime acceleration gate failed only
  for stock-session parallelism: 20.1196 effective cores, 62.8736% logical CPU
  occupancy and 31/37 sustained blocks meeting threshold. Active-bar passed,
  stock-session `run_health_status` was PASS, minimum free memory exceeded
  64.41 GB and this is infrastructure evidence rather than route or financial
  health. The unclosed checkpoint and logs are preserved at
  `run_health_incidents\20260731T122800_checkpoint004_runtime_acceleration_gate_failure_dual_lane`;
  incident SHA256 is
  `175d986dac2f68ac56a82e803b45eb5f8e73986a9b07bc797b9497317b1753c9`
  and the 54-file/3,920,983,739-byte preservation manifest SHA256 is
  `504f84d07a839949fe3123eeaba50ad27b6904fc0cbd8198a248acae771a0876`.
  No failed financial result or optimizer state was reused and no threshold
  was weakened. The single same-root recovery task
  `lanjob_20260731_123132_54145c` regenerated checkpoint 004 from verified
  checkpoint 003 but failed the same frozen stock-session acceleration gate a
  second time: 19.9623 effective cores, 62.3822% logical CPU occupancy and
  32/37 sustained blocks meeting threshold. Per the predeclared
  `second_failure_policy=RUN_INVALID`, the search lane is now `RUN_INVALID`;
  no third attempt is authorized. The second failed checkpoint is preserved at
  `run_health_incidents\20260731T131720_checkpoint004_second_runtime_acceleration_gate_failure_run_invalid`;
  incident SHA256 is
  `66df4c87e16856dce543b6f9b8053cd5e1f77056674991bbfeb585618d5b8a1b`
  and the 53-file/3,921,032,540-byte preservation manifest SHA256 is
  `c9da906f3fa362b7d88d20a2e0386c1c1d7956427b42bf336598b60523920037`.
  Checkpoints 001-003 remain the only accepted search evidence.
- The isolated report-only lane originally froze 64 pairs, but its sole
  First-N pair required intraday `intraday_ret_from_open` and could not be
  represented by the stock-session replay sidecar. That attempt stopped before
  replay or OOS and produced no reusable financial result. The corrected
  immutable cohort excludes exactly that incompatible route before financial
  work and retains 63 pairs/126 members in original relative order: 39 Slow
  Temporal and 24 Slow Cross-sectional. Its selection payload SHA256 is
  `c5b2d72aaa3aa3792995db35ba45c7d29237b574e22e75967bcdbaddebbc381b`.
  The first 8-thread task `lanjob_20260731_102229_961008` stopped after 96/126
  replay members because a candidate-specific non-integer corporate-action
  holding was raised as a batch-level `ValueError`. OOS had not started and
  there were no pair results. All 96 incomplete candidate records were moved
  out of the active root and preserved at
  `run_health_incidents\20260731T131228_fractional_corporate_action_batch_abort`;
  incident SHA256 is
  `14b42de58c02c01a4d67b25cd96daf7dd82eb03656d86e1f305fab12fac0dc74`
  and the 99-file preservation manifest SHA256 is
  `ee331cfad92e1c70d447d54a9c0671a21b165c925516d109e7cd8c9d1759b2aa`.
  No incomplete replay financial result is reused.
  SHA `99920eb6e71ceff0c29506d4a009b7f9276b8d28` keeps the unchanged
  `FAIL_CLOSED_NON_INTEGER` corporate-action policy but records that typed
  outcome as candidate-level
  `CORPORATE_ACTION_FRACTIONAL_SHARES`, allowing the fixed cohort to continue.
  Local and official 77o focused tests passed 22/22. The first recovery
  completed all 126 candidate records (58 complete and 68 blocked) but failed
  before pair or replay closure because the new blocker-only scalar
  `corporate_action_policy` collided with the normal structured policy column
  during Parquet serialization. OOS did not start. All 126 unclosed candidate
  results were moved out of the active root and preserved at
  `run_health_incidents\20260731T152803_candidate_summary_parquet_mixed_struct_batch_abort`;
  incident SHA256 is
  `227d97aa63349e173727d169ea98b786704a0c099cf95d57a8cf12e288c22dcd`
  and the 129-file preservation manifest SHA256 is
  `981cb6f18aed58672e0fcde5e972bee583fb778eaaf2d021e6e86823ced8ff5e`.
  No candidate financial result is reused. SHA
  `6a68ad06ca341a14445385b123d978616955328e` renames only that
  blocker-specific scalar field; the replay rule and cohort are unchanged.
  Local and official 77o focused tests again passed 22/22. The current
  recovery task is `lanjob_20260731_153336_a7d457`, with deployment manifest
  SHA256
  `299cd2603431a503ba8c5ed1841522f639ad0026c22c51c417a41c5b51b3f81f`.
  It may read only the fixed validation split after unchanged-cohort train
  replay; it cannot write optimizer, feedback, scheduler, archive or promotion
  state and cannot affect the running search.
- Only the validation recovery lane remains active on 77o. The latest bounded
  check showed a fresh replay with zero candidate records reused and
  83,294,220,288 bytes free memory. The invalid search tasks and both failed
  validation tasks were deleted; there is no duplicate or persistent
  inspector. This is execution evidence only: neither a running process nor a
  prepared cohort is closure or promotion authority.
- Disclosure V2 is now closed in both evidence and code. Sign is
  `REJECTED_BEHAVIOR_DISCOVERY`; CSRank is
  `REJECTED_FINANCIAL_INCREMENT`; Abs is `NOT_EVALUATED`. The historical CEM
  result is scoped only to the Disclosure CSRank expanded space and does not
  establish global CEM quality. All three extensions are excluded from the
  active catalog and CEM; CSRank/Abs require an explicit historical-evidence
  replay flag, while the old runner fails closed by default.
- The repaired `MINUTE_STATIC` production-lane V3 completed on 77o at deployed
  repo SHA `c93f7364efc6e72e8ae6b683b4410b1358950208`. It reused the current
  Registry/compiler/behavior archives and the full 11-root production contract;
  OLD supply retained 76 post-archive exact pairs and 68 behavior-unique pairs,
  above the amended 72/48 floors. The deterministic 25%-of-development-session
  selector preserved full within-session cross-sections and qualified against
  64 hash-bound full-coordinate MINUTE_STATIC pairs: Spearman 0.99748, sign
  agreement 95.31%, full-top-quartile recall 100%, and 4.97x pairs/hour. Four
  current full-coordinate replay pairs passed identity, direction, long-only,
  5 bps cost, horizons, lineage, matched-control and access parity.
- All nine V3 arm checkpoints are `BATCH_CLOSED_IMMUTABLE`. Independent closure
  matched 19/19 root artifacts and 171/171 batch artifacts. Uniform OLD
  evaluated 60 pairs; uniform OLD plus the existing `normalized_ratio`
  production evaluated 72; expanded CEM evaluated 72. The production increment
  qualified on all four frozen checks. A post-campaign causal audit found that
  expanded uniform and CEM used different candidate RNG streams, that the policy
  exposed only two unique decision contexts, and that the apparent three CEM
  updates were the same field-pair context updated once per checkpoint. The
  recorded CEM arm was numerically weaker, but it is not a valid causal estimate
  of a CEM policy increment. `CEM_SEARCH_INCREMENT=NOT_QUALIFIED` remains the
  admission result; the V1 implementation comparison is `INCONCLUSIVE`.
- The V3 campaign originally recorded `PERFORMANCE_CONTRACT=FAIL` because the
  shared comparison helper required a `stock_session_native_contract` even
  though this route selected only `active_bar`. The classifier now applies that
  contract only when stock-session is selected. Replaying the immutable metrics
  changes performance to `PASS` without recomputing financial results: all
  three arms exceeded 80% median logical-CPU occupancy, effective cores were
  25.91-26.64, free memory stayed above 71 GiB, cache stayed below 1.85 GiB,
  pair batch stayed at four, and semantic/metric/access drift remained zero.
  Large-search readiness remains `SEARCH_POLICY_BLOCKED` solely because the
  tested CEM policy did not qualify. The compact receipt is
  `runtime/run_plans/cn_minute_static_production_cem_v3_20260725_receipt.json`.
- The paired Structural CEM V2 canary passed its mechanical causal checks, but
  the medium qualification closed supply-limited and is not qualified. The
  checkpoint-only resume at pushed SHA
  `da5c9f3408e0ebfec00686d43b86177b1b0c479f` reused four closed checkpoints
  and closed both checkpoint-003 batches without recomputation. All six batch
  manifests verify 19/19 and the root manifest verifies 18/18. The frozen
  post-archive catalog had only 76 available exact identities; after two
  32-exact checkpoints, each arm had 12 exact identities left, of which 11
  were behavior-unique and full-coordinate evaluable. Both arms therefore
  completed 59 rather than the frozen minimum 72 pairs. CEM started fresh
  uniform, imported no canary probability or reward state, reached generation
  3 from 59 full-train observations, and updated its production-choice
  probability, but its terminal 59-pair evidence and aggregate financial/
  diversity results were identical to Uniform. Each arm retained 29 positive
  pairs, a -7.45048 median
  signed matched increment, 43 behavior families and 0.72881 behavior discovery
  per evaluated pair. Performance passed: median hot-path host occupancy was
  81.32% Uniform / 83.51% CEM, maximum cache was 1.68 GiB, minimum free memory
  exceeded 71 GiB, and CEM throughput was 109.05 versus Uniform 107.98 full
  pairs/hour. Exact duplicates, semantic/metric/access drift and validation,
  holdout and 2026 reads were zero. The original checkpoint-003 exception is
  preserved as `SEARCH_SUPPLY_CONTRACT_INFEASIBILITY`; it is neither
  infrastructure nor financial route health. The frozen verdict remains
  `STRUCTURAL_CEM_V2_NOT_QUALIFIED` and large search is not authorized.
- The follow-on zero-financial-read structural supply design is closed on 77o
  at implementation SHA `69e0b591e7d6f2fb43dc7838833c6021b09a4f30`.
  It preserved the historical two-production space and added only the existing
  compatibility-qualified Grammar lanes `absolute_state_interaction` and
  `dispersion_interaction`; representation-family and minute-leg metadata
  blocked the three remaining pair lanes without allowlist relaxation. All 440
  categorical rows were legal and exact/canonical unique. After the cumulative
  historical archive plus canary and medium exact memory, 157 exact identities
  remained: 78 absolute-state and 79 dispersion interactions, with zero
  remaining field-spread or normalized-ratio repeats. The label-free probe
  retained 139 behavior-unique pairs after the same cumulative behavior memory.
  Under the frozen 2x non-exhaustive headroom rule, the maximum paired budget is
  69 pairs per arm, so the former 72-pair reference remains blocked but a
  three-checkpoint x 20-pair budget is supportable. The manifest and all six
  artifacts plus 27 inputs independently hash-verified; financial, Phase3CM,
  validation, holdout and 2026 reads were zero. The compact receipt is
  `runtime/run_plans/cn_minute_static_structural_supply_v1_20260725_receipt.json`.
- The resulting paired Structural CEM V2 supply-medium campaign is now closed
  on 77o at deployed SHA
  `2027dda47cc7502752157cd85c3782437db4179e`. It used the verified four-lane
  440-row Grammar surface, cumulative 157-exact/139-behavior supply, a common
  generation-one stream, four checkpoints and 68 full-coordinate train pairs
  per arm. All eight batch manifests are `BATCH_CLOSED_IMMUTABLE`; each
  manifest payload hash and all 19 bound artifacts verified, as did all 21
  root artifacts and the root payload hash. Exact duplicates, semantic/metric/
  access drift and validation/holdout/2026 reads were zero. CEM started fresh
  uniform, imported no probability or reward state, updated only the declared
  production-choice context and reached generation four from 68 full-train
  observations. Its proposal stream remained identical to Uniform through
  generation two, then separated in generations three and four.
- The completed comparison does not qualify Structural CEM V2. CEM retained
  59/68 positive matched pairs versus Uniform 58/68 and nearly identical
  overall medians, but the frozen adaptive-checkpoint median was exactly equal
  at 14.74973, while behavior discovery was lower at 0.79412 versus 0.80882
  and behavior families were 54 versus 55. Both arms exceeded the primary
  75% host-occupancy gate with median effective cores of 25.39 CEM / 25.14
  Uniform and comparable 97.73 / 96.51 full pairs/hour. A stale standalone
  PowerShell inspector, not the evaluator cache or route, leaked 68.4 GiB and
  polluted the frozen minimum-free-memory check; after its exact process was
  removed, free memory recovered above 76 GiB and the following checkpoints
  remained above 71 GiB. The incident is preserved as infrastructure run
  health and does not change the financial result. The compact receipt is
  `runtime/run_plans/cn_minute_static_structural_supply_cem_v2_medium_20260725_receipt.json`.
  No optimizer authority, Graph relation, Obsidian projection or large-search
  authorization changes.
- The follow-on typed-surface audit is closed on 77o at pushed SHA
  `6c490ff147c1a5c2ab146d5081e91a9bc90fb1c5`. It did not create another
  optimizer: the existing authoritative joint Grammar `field_pair_id` was
  projected losslessly into adaptive `production_id`, `left_field_id` and
  `right_field_id` coordinates, with the joint exact mask applied before each
  hierarchical draw. A 32-proposal synthetic non-financial proof retained
  first-generation Uniform/CEM exact-stream parity, updated all three contexts
  and showed that left/right adaptation changes proposals while the production
  stream remains identical. Exact duplicates and financial, Phase3CM,
  validation, holdout and 2026 reads were zero.
- The same audit bound the full historical exact archive, cumulative candidate
  ledger and all 18 prior proposal ledgers. The raw four-lane catalog remains
  healthy at 440 legal exact/canonical-unique formulas, but only 19 genuinely
  fresh exact identities remain: one `absolute_state_interaction` and 18
  `dispersion_interaction`, with both older lanes exhausted. Under the frozen
  2x non-exhaustive rule this supports at most nine pairs per arm, not the
  72-pair reference. Financial qualification and large search are therefore
  blocked before behavior probing; no optimizer/Graph/Obsidian authority
  changed. All five artifacts, 23 inputs and the manifest payload hash
  independently verified. The compact receipt is
  `runtime/run_plans/cn_minute_static_structural_typed_surface_audit_20260725_receipt.json`.
- `MINUTE_STATIC_PHASE3CM_SESSION_SAMPLE_V1` is accepted as the active
  route-local sampled-selection implementation under ADR 0007. This does not
  replace the candidate-parallel `formal_evaluation_authority`, authorize
  Strict Stage A or large search, permit candidate promotion, or open
  validation/holdout/2026 feedback.
- The bounded official CatCMAwM search-policy qualification completed on 77o
  with eight immutable arm-checkpoints and 384 scheduled matched-pair asks.
  CatCMA improved positive matched pairs per wall-hour from 1,728.24 to
  2,025.81, evaluated 44 rather than 34 pairs, and retained the behavior-
  diversity and concentration gates. Its median signed matched increment was
  only 1.49349 versus the baseline's 3.08269, so the frozen composite verdict
  is `SEARCH_POLICY_QUALITY=FAIL`. The current CatCMA configuration is not
  qualified and neither it nor the rejected legacy RX/UCB/CEM optimizers become
  search authority.
- `INTRADAY_STATE_TRANSITION` was not a financial positive control in that
  qualification. A zero-budget exhaustive check covered all 384 frozen
  categorical combinations: 320 were legal, 64 were deterministic source/
  state conflicts, the legal set collapsed to 270 exact identities, and all
  270 already existed in the historical exact archive. Disclosure and Slow
  Temporal supplied the actual development evaluations. Because no novel
  active-bar pair remained, the CPU/throughput gate was not exercised and
  `LARGE_SEARCH_SCALE_READINESS=FAIL`; this is missing evidence rather than a
  financial route-health failure. Validation, holdout and 2026 reads were zero,
  and promotion remained forbidden. The compact bound receipt is
  `runtime/run_plans/cn_search_policy_qualification_20260724_receipt.json`.
- The optimizer-facing Grammar surface has now been repaired without widening
  registry or proposal-root authority. Unified registry `route_id` remains the
  top-level scheduling key; each typed skeleton is a route-local generation
  lane containing only active, compatibility-qualified slots. Exact ordered
  field pairs replace ordinal secondary offsets, fixed lane slots are removed
  from CatCMA dimensions and restored after `ask`, and equivalent blocked or
  invalid outcomes receive equal losses. The seven primary routes expose 51
  usable lanes and 72,307 bounded categorical points. Five additional
  skeletons remain explicitly blocked by missing leg/representation metadata
  or undeclared size roots rather than guessing from field names. This is
  source/test qualification only: the local relevant suite passed 56 tests
  (four official-package tests skipped locally), while the exact patched files
  passed 17/17 tests on 77o against the frozen official `cmaes 0.13.0` wheel.
  It used no financial, validation, holdout or 2026 reads and does not qualify
  CatCMA financially or authorize a large search.
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

The earlier four-production `MINUTE_STATIC` supply question and its paired
68-pair financial comparison are closed. In that frozen comparison the Grammar
produced 440 legal exact/canonical-unique rows and 157 exact identities remained
after cumulative memory. The tested optimizer surface was the blocker.
Structural CEM V2 adapted only `production_id`; concrete typed field pairs
remained uniform without replacement. That shallow treatment did not improve
the frozen adaptive median or behavior discovery even after generation-three/
four proposal separation. `TARGET_FAMILY_LARGE_SEARCH_READINESS` therefore
remains `SEARCH_POLICY_BLOCKED`, and neither the terminal CEM probabilities nor
the near-exhausted campaign catalog may seed another run.

An independent zero-financial-read input-authority audit verified the complete
current `MINUTE_STATIC` path: the production contract's 11 registered
`raw_1min` roots, all 110 ordered non-self field pairs, all 440 primary formula
ASTs and their 110 pair-specific matched-control identities, and the 16 active
77o sidecar shards (446,443,583 rows; 10,813,323,428 bytes). All current file
hashes, physical schemas, row/byte counts and historical source-to-sidecar
parity records matched. Directional left/right order is preserved in the AST
and declared lineage; compiler `field_ids` and `source_field_ids` are sorted
only for canonical identity. This rules out a field-registration, source-input
or formula-leg error in the tested 440-row catalog, while making no Alpha,
behavior-family, validation or OOS claim. Evidence is attached to
`runtime/run_plans/cn_minute_static_structural_typed_surface_audit_20260725_receipt.json`.

The bounded V4 formula-supply extension is now closed on 77o at pushed SHA
`320d4f3fa7dd5c7e5f68ccd9c819591d1b4f4914`. It reuses the same Registry,
`CompositionalGrammarV2`, `TypedRouteCompiler`, streaming executor and
matched-control constructor while exposing five optimizer-visible decisions:
`production_id`, left/right transform and left/right field. The only enabled
transforms are the already registered and supported `ZSCORE`, `ABS_ZSCORE` and
`SIGN`; `IDENTITY` remains excluded because the production signatures are
dimensionless while raw roots span incompatible units, and
`DELTA_EXISTING_WINDOW` remains excluded from the current `MINUTE_STATIC`
compiler allowlist. The resulting bounded catalog contains 1,980 legal
exact/canonical-unique formulas at maximum depth four.

After the complete cumulative exact memory, 1,559 fresh exact identities
remain, versus the frozen minimum 144. A deterministic
`hash(frozen_seed, exact_identity)` probe materialized all 256 selected pairs,
left zero unresolved, and admitted 171 behavior-unique primary pairs versus
the frozen minimum 72. All 11 observed formula roots equal the production
route authority; all are present in the active layout. Primary/control field
and source lineage matched for all 1,559 pairs. The artifact manifest and all
44 inputs plus six outputs independently hash-verified with zero mismatch;
financial, Phase3CM, validation, holdout and 2026 reads were zero. This closes
formula supply only. It does not accept a search policy, promote candidates or
authorize a large search. Evidence is in
`runtime/run_plans/cn_minute_static_structural_formula_v4_supply_20260726_receipt.json`.

The bounded V5 search adapter is now code-ready on the current branch. It no
longer enumerates a complete formula catalog before search. Uniform and
RankWeighted CEM V2 instead select a root binary rule and that rule's
conditional left/right typed transforms and registered fields; the existing
Grammar then constructs, compiles and matches the control online. Exact
availability is enforced after compiler-canonical construction with bounded
resampling, while behavior admission remains in the existing downstream
archive path. The outer `CSRank` mapping is explicitly frozen for this first
slice and is not claimed as an adaptive decision. The bounded rule space is
2,090 formulas: 990 `SUB`, 990 `SAFE_DIV` and 110 matched-control-safe
`MUL(ABS_ZSCORE,SIGN)` combinations at maximum depth four. A 96-proposal
zero-financial test produced 96 legal exact-unique pairs without materializing
the eager catalog; fresh Uniform/CEM proposal streams matched before tell, and
synthetic full-evaluation feedback changed the root and root-conditioned CEM
contexts. The real runner is wired for one paired 3-checkpoint x 24-pair
train-only comparison per arm. Pushed implementation SHA
`957c367af8101b252036ba5455d2a2047b766e03` completed on 77o as the single
writer `minute_static_online_typed_grammar_cem_v1_957c367_single_writer`,
detached task `lanjob_20260726_030400_113a1b`, under campaign root
`D:\ChengboRemote\runtime\cn_minute_static_online_typed_grammar_cem_v1_20260726_957c367_3x24`.
The task exited zero after all six arm/checkpoints closed immutable. Two earlier launcher
attempts stopped before Phase3CM because a behavior-only V4 archive was
incorrectly supplied as exact memory; their logs are run-health evidence, not
search or route health. A deterministic 256-row/256-exact projection now binds
those V4 behavior-probed pair IDs back to the preserved primary exact
identities, without marking the unprobed V4 catalog as searched. Its receipt is
at
`D:\ChengboRemote\runtime\cn_minute_static_online_typed_grammar_v5_inputs_20260726_957c367\projection_receipt.json`.
Validation, holdout and 2026 reads remain zero, and no search or Graph authority
is promoted.

Independent closure matched the root manifest payload and all 17 root
artifacts, plus all six batch-manifest payloads and 114/114 bound batch
artifacts. Generation-one proposal streams and full-evaluation sets matched,
exact duplicates were zero. The recorded `cem_updated_context_count=19` means
19 cumulative context-update events across three checkpoint tells, not 19
unique declared contexts. The V5 decision surface has 13 unique contexts, and
the receipt does not establish that all 13 ever received sufficient support.
The sampled evaluator selected the full-coordinate shortlist but was forbidden
from policy feedback, so CEM learned only from the sampled-top-K truncated full
outcomes and received no signal for candidates discarded by sampled ranking.
This prevents the comparison from cleanly separating optimizer quality from
formula-space quality and shortlist-selection effects.

Uniform completed 54 evaluated pairs and CEM 57, below the frozen 72-per-arm
support minimum. Over adaptive checkpoints CEM retained 15 positive pairs
versus 14 Uniform and a less-negative median (-9.43310 versus -10.05011), but
behavior discovery was lower (1.10526 versus 1.14815 per evaluated pair).
Their ratio is 96.2649%, so this is a strict 100% non-inferiority contract
failure, not a material behavior-diversity collapse under the historical 90%
reference. The overall medians remained negative and CEM's aggregate median
was below Uniform (-8.81610 versus -8.06548). The rejected RankWeighted CEM V2
also gave every evaluated rank positive weight and carried probability state
without campaign-cumulative sufficient statistics; it is not a reusable
mature optimizer. Uniform's median selected-backend host occupancy was 73.04%,
below the frozen 75% threshold; CEM reached 78.83%. The honest verdict remains
`STRUCTURAL_CEM_V2_NOT_QUALIFIED`,
`CEM_SEARCH_INCREMENT=NOT_QUALIFIED`, performance `FAIL`, and
`SEARCH_POLICY_BLOCKED`. No large search or optimizer authority is accepted.
Compact closure evidence is in
`runtime/run_plans/cn_minute_static_online_typed_grammar_v5_20260726_receipt.json`.

Source-only performance commit
`72250f97612ec6db21e2b1142a33c4306ca31d90` parallelizes the support mask and
candidate-by-time label-free selection while preserving support, tie-break,
turnover and behavior identity inputs. The exact commit was deployed to a new
77o workspace, five source/test hashes matched, and 19 focused official-runtime
tests passed in 5.34 seconds. At 30 threads the bounded non-financial
microbenchmarks measured 8.51x for pair common-support masking and 2.58x for
label-free behavior selection. Free memory stayed above 85.8 GB. These are
kernel-direction measurements only, not whole-campaign speedup claims. The
closed campaign's final timing evidence spans 12 timing files: pair common
support consumed 423.43 recorded phase seconds at 9.59 effective cores and
label-free behavior consumed 1,039.58 seconds at 13.62 effective cores. No
financial result was recomputed and no sealed period was read.

Historically, the targeted formula-space/CEM retry selected
`DISCLOSURE_EVENT` /
`cn.comp.v2.disclosure_event.pre_event_path` and reused the frozen OLD
authority without recomputation. `EventWindow(CSRank(payload),event,5,0)`
passed the static gate and retained 124/256 behavior-unique pairs versus OLD
126/256, above the frozen 114-pair / 90% floor. It became the first accepted
extension for that financial qualification, so Abs was correctly not run.
After the financial comparison, CSRank was rejected as
`REJECTED_FINANCIAL_INCREMENT`; the historical Sign extension remains rejected
at 111/256 as `REJECTED_BEHAVIOR_DISCOVERY`. Neither is active or CEM-eligible.

The three-arm full-coordinate comparison completed 57 OLD-uniform, 72
expanded-uniform and 72 expanded-CEM pairs across nine immutable checkpoints.
All arms met financial support. Expanded uniform improved positive pairs/hour
and AST-shape diversity, but its median matched increment and behavior
discovery regressed; `FORMULA_SPACE_INCREMENT=NOT_QUALIFIED`. CEM updated nine
contexts without category collapse and retained checkpoint 2/3 supply, but
failed the 15% positive-throughput, median-increment and behavior-discovery
checks; `CEM_SEARCH_INCREMENT=NOT_QUALIFIED`.
That conclusion is limited to the Disclosure CSRank expanded space;
`GLOBAL_CEM_CONCLUSION=NOT_ESTABLISHED`.

The qualified `MINUTE_STATIC` sampled evaluator is now an active route-local
development selection implementation. It is not a surrogate reward, does not
change formula execution or portfolio semantics, and does not become the
project-wide formal evaluator authority. Fresh-uniform generation-zero parity
still passes, but no large search may start with the rejected V3 CEM state.

Independent closure verified 34 root artifacts, all nine immutable manifests,
135 batch artifacts, nine Phase3CM receipts and the checkpoint chains. Launch
count was one, scheduled triggers zero, writer closed, process exit zero, and
validation/holdout/2026 reads remained zero. Compact evidence is in
`runtime/run_plans/cn_disclosure_pre_event_extension_retry_v2_20260724_receipt.json`
and
`reports/cn_targeted_formula_cem_qualification_20260724/QUALIFICATION_RESULT.md`.
Graph and Obsidian remain unchanged because no durable active search authority
or large-search-ready capability was accepted.

There is no remaining global route-supply, ontology-authority or
train-to-validation blocker in the earlier completed core-pack campaign.
Targeted formula/CEM large-search readiness remains blocked as recorded above.
Its preserved cache, memory-headroom and monitor-path incidents are run-health
facts, not financial route failures. The 93 unresolved rows in the historical V1
archive remain immutable and are not retroactively rewritten. On 2026-07-23 the
user separately authorized and 77o completed one fixed 48-session report-only
holdout evaluation of the already frozen 12 behavior-exact candidate pairs.
All 12 pairs evaluated in `long_only_top` mode with shorting disabled, producing
59,887,982 holdout reads, zero validation/2026 reads, zero feedback/scheduler/
archive/promotion writes and unchanged protected train hashes. Ten candidates
had positive aggregate holdout day Sortino and ten pairs had positive matched
holdout metric. Six passed the fixed strict non-collapse rule requiring positive
aggregate, worst-horizon, standalone report and matched-pair metrics. The
highest aggregate holdout Sortino was 0.44725, but its worst-horizon Sortino was
-0.12190; the highest strict survivor was `cn.comp.f9ea7251e72ce711baa4`
with aggregate/worst-horizon Sortino 0.30441/0.23990 and matched metric 1.19407.
The immutable repository receipt is
`runtime/run_plans/cn_core_pack_fixed_holdout_20260723_receipt.json`.

This is a fixed candidate non-collapse check, not promotion evidence. The
48-date calendar was candidate-untouched for this freeze but remains classified
`spent` at project level after historical exposure. Its OOS sample grade is
`WEAK`, and the evaluator still does not prove complete A-share T+1, suspension
and price-limit fill behavior. Formal search, candidate promotion, economic
claims, any additional holdout access and 2026 access therefore remain blocked.
Plate remains independently blocked by missing real PIT minute materialization;
additional compound-state expansion beyond tested expanded source-leaf forms
remains a localized materialization gap.

The active-chain audit is frozen in
`runtime/run_plans/cn_active_search_chain_gap_audit_20260723.json`. The current
authority is a bounded typed compositional enumerator with behavior-aware
admission and route-budget feedback, not an active MCTS or evolutionary AST
engine. It has partial admitted-candidate memory, deterministic typed skeleton
coverage and immutable checkpoint ledgers, but it does not yet retain every
legal attempted formula, continue elite ASTs, perform typed subtree crossover,
apply mutation, or carry regime-partitioned top-K elites across search
generations. The observed late-checkpoint supply collapse was therefore a
search-policy and optimizer-facing Grammar limitation, not a field-
materialization failure. These gaps do not block the fixed holdout; they bound
the design of any later expanded search. The repository's checkpoint,
train-only feedback, evaluator, archive, expression/skeleton identity and
receipt infrastructure remains reusable. Its former RX/UCB, CEM, hybrid and
orthogonal optimizer algorithms are rejected and must not be restored as search
authority. Any replacement optimizer must remain a thin route-local adapter
beneath registry `route_id`; no MCTS, crossover, mutation or replacement
machinery may be handcrafted.

The bounded `SLOW_CROSS_SECTIONAL_LEVEL` evaluated-fill campaign is now closed
on 77o. Eight immutable train checkpoints produced 392 actual `PAIR_EVALUATED`
results against the 384-pair target, with zero evaluated exact duplicates and
zero validation/holdout/2026 reads during train. The single-backend runtime gate
passed at 25.35 effective cores and 79.20% logical-CPU occupancy. Portfolio
semantics remained `long_only_top`, 5 bps one-way cost and horizons 1/5/15/30.

The campaign's first automatic validation is preserved as inadequate evidence:
all 19 required `fund_*` fields were schema-present but null because the
sidecar builder received the package parent instead of its authoritative
`silver_partitioned` root. Commit `d3de4b9` resolves the packaged partition
root and fails closed on zero aggregate PIT coverage; 21 focused tests passed
locally and on 77o. The candidate-bound validation-only repair rebuilt 381,649
rows with 90.27%-97.53% coverage across all 19 canonical fundamental fields.
It evaluated 392/408 frozen pairs; the remaining 16 were blocked only by a
constant control. All eight train checkpoints, the train-complete manifest and
five protected train projections retained their hashes. Validation recorded
381,649 reads, zero holdout/2026 reads and zero feedback/scheduler/archive
writes; promotion remained forbidden.

The repaired OOS evidence does not qualify the batch for promotion. The matched
validation report metric had median 0.01758 and 55.87% positive pairs, but mean
-0.24525 and a -88.35 left-tail minimum. Matched net increment had median
-0.01192, mean -0.29288 and only 46.43% positive pairs. Median/mean one-way
turnover were 0.04074/0.08945, and train-to-validation rank correlation was
only 0.17435. Validation-run regime positive share had median 0.5, while the
worst-regime day Sortino median was -0.67697 and only 1.02% were positive.
Fundamental-only pairs were the strongest broad family (matched-net median
0.02742, mean 0.04231, 53.55% positive); chip-only and chip/context mixtures
carried the most severe left-tail failures. The frozen verdict is
`BATCH_NOT_PROMOTABLE_HEAVY_LEFT_TAIL`, while a bounded OOS survivor set remains
available for economic/regime classification. Search authority, Graph and
Obsidian remain unchanged. The full remote closure is
`D:\ChengboRemote\runtime\cn_slow_cross_sectional_evaluated384_20260726_351e503_384e\post_train_validation_repaired_d3de4b9\validation_repair_closure.json`
(SHA256 `2fda9e2188068c88a0e6f1f69883b99ae453d9b1481c3a48bfc58d623e220685`).

The separately authorized five-digit development search is now closed on 77o
at deployed SHA `e4cc82fadfc54213de26dd47169cdc81691a7e6a`. Official Optuna
4.8.0 TPE remained a thin route-local conditional typed-Grammar selector
beneath registry `route_id`; no platform, optimizer database, new Graph
authority or cross-campaign reward state was created. The corrected objective
fed TPE `min(primary_composite_reward, matched_train_increment)` from full
coordinate train outcomes only. Exact, behavior, reward, tell and sealed-read
checks remained fail closed.

The campaign exhausted its frozen 73,728 raw asks across 24
`BATCH_CLOSED_IMMUTABLE` checkpoints but produced only 8,736 actual
`PAIR_EVALUATED`, so the 20,000 target closed `CAMPAIGN_INCOMPLETE`.
Exact/behavior/evaluated funnel counts were 12,372/8,843/8,736; the total
raw-ask-to-evaluated yield was 11.85%. Route completion was
7,136/15,500 slow temporal, 665/2,000 First-N, 514/1,200 slow
cross-sectional, 275/800 market regime and 146/500 disclosure. This is fixed
budget and search-supply/admission infeasibility, not a host, evaluator, data
access or financial-route crash.

Independent closure matched all 24 batch-manifest payload hashes, 552/552
declared batch artifacts and 15/15 root-manifest artifacts. All 8,736 evaluated
pair IDs were globally unique; reward/search-score/tell mismatches and train
validation/holdout/2026 reads were zero. All 24 runtime gates passed both
backends. Median effective cores were 25.82 active-bar and 29.27
stock-session; median logical-CPU occupancy was 80.69% and 91.45%;
minimum free memory was 60,965,040,128 bytes and the fixed 8 GiB cache cap
held. The earlier stale-inspector memory incidents remain infrastructure
evidence only.

Train evidence is mixed and not promotable. Of 8,736 evaluated pairs, 4,885
had positive matched train increment; its median was 0.01204. Search-score
median was 0.00330 but mean was -0.00784. Matched net increment had a positive
0.69547 median but a negative -0.39689 mean and material tails. The strongest
train-only positive shares were slow cross-sectional at 72.18% and market
regime at 66.91%; First-N and disclosure were below 50%, and market regime's
standalone primary median remained negative. These are development-train
descriptives, not OOS or economic claims.

Because all route targets were unmet, automatic report-only validation
correctly did not run, `train_finalists.parquet` is empty, and no OOS
regime/turnover evidence exists for this campaign. Promotion remains forbidden;
Graph and Obsidian authority are unchanged. The compact closure receipt is
`runtime/run_plans/cn_large_optuna_tpe_actual20000_v2_20260728_receipt.json`.

The bounded novelty-aware availability repair is now implemented and
independently qualified at pushed SHA
`1f928fd88477741b4f9767ce32b6a000816174e0`. It retains Registry,
generator, Grammar, compiler, matched-control and route scheduler authority,
adds only deterministic route-local exact availability, and records native
and fixed-enqueued Optuna asks in the same full ask/tell transcript. Local and
official 77o focused suites both passed 31/31. The single zero-financial task
`lanjob_20260728_110745_9bb945` closed exit 0; artifact manifest SHA256 is
`1a1caaf01b27ee2e6cd37f2ca0776d4ca21983fea2ed1b20141c9f075f9666a5`.
Its manifest payload hash and all 12 declared artifact sizes/SHA256 values
were independently verified.

Layer A bound 52,254 authoritative legal exact entries and replayed all 24
closed checkpoints and 73,728 frozen native asks without reading reward,
search score or matched increment. The non-causal availability remap emitted
21,384 formal fresh exact asks with zero exact duplicates: 7,288 direct TPE,
9,217 same-bucket replacements and 4,879 global fallbacks. Total native plus
fixed draw attempts were 87,824 against a 663,552 cap, with no unfulfilled
formal request. This proves that availability control repairs the mechanical
late-checkpoint exact-novelty collapse; it does not claim a causal adaptive
TPE counterfactual.

Layer B used the existing label-free probe on a deterministic 2,048-pair
train-field-only sample, with 1,219/269/213/184/163 pairs across slow temporal,
First-N, slow cross-sectional, market regime and disclosure. Financial, label,
validation, holdout, 2026 and Phase3CM reads/calls were all zero. Behavior
admission and historical evaluator-completion lower confidence bounds passed
their component gates, but current route supply did not: post-campaign formal
capacities were 27,418/1,993/1,136/944/384 versus 10%-margin requirements
29,300/2,855/1,431/2,680/1,256. Global fallback share was 22.82%, above the
20% readiness ceiling, leaving only 77.18% TPE-guided emissions. The honest
verdict is therefore mechanical/controller PASS but
`NOVELTY_AWARE_TPE_LARGE_SEARCH_READINESS=SUPPLY_BLOCKED` and
`PROJECTED_20K_ASK_FEASIBILITY=INFEASIBLE_UNDER_CURRENT_ROUTE_TARGETS`.
The implementation is evidence, not accepted active search authority;
promotion remains forbidden and Graph/Obsidian remain unchanged. The compact
receipt is
`runtime/run_plans/cn_tpe_availability_yield_qualification_20260728_receipt.json`.

The successor availability-v3 integration and zero-financial qualification
are now closed at pushed SHA
`bc7a2be832cbb3b9b638b684b382d5a5b2287027`. The live runner counts only
fresh exact formal asks against its frozen budget and retains every official
native or fixed Optuna trial in immutable ask/tell transcripts. A trial may be
`PRUNED` only when a declared live-runner pruning authority supplies a finite
real intermediate value and nonnegative step. Grammar invalid, exact/behavior/
support blocked and availability replacement/exhaustion outcomes remain
`FAIL`; only full-coordinate train evaluation with a finite real reward is
`COMPLETE`. No reward, compiler, Registry, evaluator or sealed-period contract
was changed.

Route targets were refrozen for qualification only at
14,000/1,300/900/380/300, total 16,880, with 36,864 formal fresh-exact asks.
Only economically declared route-local typed supply was appended: payload
direction/magnitude under persistent market regimes, and pre-disclosure
payload direction/rank/magnitude. The target contract remains explicitly
non-executable and non-financially-authorized. Local relevant regression was
99/99. The versioned 77o deployment used official Optuna 4.8.0; all 81 tests
executable in the Git-archive workspace passed, while 18 unrelated historical
builder tests requiring `.git` were correctly unavailable there and had
already passed locally.

The single zero-financial task `lanjob_20260728_121110_0aac11` closed exit 0.
Artifact manifest SHA256 is
`4f4cf510f923c776d3b9e7c479ddea7462e3ffddde7d49bc6bbfc730cea2201f`;
its canonical payload hash and all 12 declared artifact sizes and hashes were
independently verified. Financial, label, reward, search-score, matched-
increment, validation, holdout and 2026 reads were zero; Phase3CM calls were
zero.

Supply and behavior gates now pass for every route. Conservative projected
formal asks were 24,023/1,692/972/1,207/680, total 28,574 within the 36,864
budget. Fresh formal capacities were 27,418/1,993/1,136/1,328/1,200, meeting
their 10% margin requirements 26,426/1,862/1,070/1,328/749. The frozen replay
emitted 21,317 unique formal asks with no unfulfilled request: 7,309 direct
TPE, 9,128 same-bucket replacements and 4,880 global fallbacks. The sole
failed readiness gate is TPE agency: global fallback share was 22.8925%,
above the frozen 20% ceiling, leaving 77.1075% TPE-guided emissions.
The honest final verdict is therefore
`NOVELTY_AWARE_TPE_LARGE_SEARCH_READINESS=ROUTE_TARGET_REDESIGN_REQUIRED`.
No financial authorization was generated, and Graph/Obsidian authority
remains unchanged. The compact receipt is
`runtime/run_plans/cn_tpe_availability_v3_qualification_20260728_receipt.json`.

- The successor Hybrid Search Productivity Medium is now closed on 77o at
  deployed SHA `9bc43e89398ea101393eb00e3d8e220ac4527d64`. It used eight
  immutable checkpoints, 384 formal fresh exact asks per checkpoint, the fixed
  route mix 160/96/48/40/40 and exact per-route/checkpoint 192/192 Hybrid/
  Uniform intention-to-treat balance. All eight manifest payload hashes,
  208/208 batch artifacts and 19/19 root artifacts independently matched.
  Across 3,072 formal asks there were zero exact, pair-ID or full behavior-
  signature duplicates; 2,141 pairs completed full train evaluation.
- Hybrid retained 471 productive candidates from 1,536 formal asks versus 339
  for availability-aware Uniform. Route-standardized productive yield was
  0.30664 versus 0.22070, and productive/shared wall hour was 118.71 versus
  85.44. Pair-evaluation yield was essentially identical at 1,070 versus
  1,071, so the productivity difference was candidate quality rather than
  evaluator admission. The 5,000-replicate relative-uplift median was 38.90%,
  with a 95% interval of 24.68%-55.09%; Hybrid median and p10 search score were
  also noninferior. The independently rerun frozen decision function matched
  the root decision exactly.
- `HYBRID_TPE_AVAILABILITY` is accepted under ADR 0008 as the active bounded
  development search policy. This accepts the combined TPE plus Availability
  Controller system, not a pure TPE causal effect: its 1,536 emissions included
  719 direct TPE draws, 803 same-bucket replacements and 14 global fallbacks.
  Uniform remains the retained baseline. Validation/holdout/2026 reads and
  Uniform optimizer feedback were zero; validation did not run and candidate
  promotion remains forbidden. The compact receipt is
  `runtime/run_plans/cn_hybrid_search_productivity_medium_20260728_receipt.json`.

### 2026-07-29 report-only validation, Hybrid tranche and route contraction

- The accepted Hybrid policy was not reopened. One frozen report-only cohort of
  128 Hybrid plus 128 Uniform pairs completed on 77o. Validation reads were
  positive; feedback, scheduler, archive and promotion writes plus holdout and
  2026 reads were zero. Source Medium train hashes were unchanged. The aggregate
  route report is now spent route-design evidence: it may support this one
  offline fixed budget, but candidate-level validation values cannot enter
  reward, Optuna observations/tells, persistent memory or promotion.
- The authorized Hybrid-only tranche completed on 77o under deployed SHA
  `9e9e113061a2ef7056edac1415fa3235a642a61d`. All eight checkpoints were
  independently verified `BATCH_CLOSED_IMMUTABLE`; 192/192 declared checkpoint
  artifacts and 16/16 root artifacts matched. From 3,072 formal fresh exact
  asks, 2,319 pairs were evaluated and 1,029 were productive. Overall
  evaluated/formal and productive/formal yields were 75.49% and 33.50%;
  throughput was 1,055.81 formal asks, 797.01 evaluated pairs and 353.65
  productive candidates per hour. Cache remained below 1.99 GB, minimum free
  memory stayed above 65.89 GB, and validation/holdout/2026 reads were zero.
- Route productive/formal yields were Slow Temporal 46.61%, Slow
  Cross-sectional 45.31%, Market Regime 2.19%, First-N 1.04% and Disclosure
  3.65%. Aggregate Hybrid validation transfer was 53.70%, 46.88%, 68.75%,
  46.15% and 69.23% respectively, but Market had five blocked pairs and the
  Slow Cross-sectional p10 was -119.995. Quant promotion audit therefore
  remains `HOLD_RESEARCH`; this evidence supports conservative route allocation
  only, not candidate or economic acceptance.
- The post-tranche remaining exact capacities are 34,353/246/1,159/2,661/884
  for Slow Temporal/Slow Cross-sectional/Market/First-N/Disclosure. A refreshed
  identity-only exact archive contains 22,918 unique identities with no
  duplicates. Remote p07 manifest file SHA256 is
  `436a4062b4a8216f8ad1a1a651dcc2c2fb71f4aa36713dad71f3ae507983a9d2`;
  its canonical payload hash and both declared artifacts independently match.
- The next tranche contract was frozen at 6,144 formal asks, eight checkpoints
  of 768 and fixed route mix 672/24/16/8/48. Final route allocations are
  5,376/192/128/64/384. Slow Temporal is the core scale route; Slow
  Cross-sectional is supply-capped with OOS tail risk; Market and Disclosure
  retain small exploration budgets; First-N is reduced to minimum coverage.
  The static runner profile and diagnostic-only behavior-family concentration
  reporting passed 23/23 local and official 77o focused tests. The exact
  zero-financial preflight passed with 39,303 fresh exact identities against
  7,990 required at the frozen 1.20 cap margins; financial, validation,
  holdout and 2026 reads were zero.
- The separately authorized 6,144-ask bounded tranche is now closed on 77o.
  Task `lanjob_20260729_102437_32fddd` /
  `ChengboLanRemote_lanjob_20260729_102437_32fddd` exited 0 at
  `D:\ChengboRemote\runtime\cn_hybrid_bounded_large_tranche_20260729_1030_6288d71_6144`
  from pushed/deployed SHA `6288d715429e6dc84af5e89ac3f73d3d0cebc21b`.
  All eight checkpoint payload hashes, 192/192 checkpoint artifacts and 16/16
  root artifacts independently matched; the checkpoint manifest chain also
  matched from `GENESIS` through `checkpoint_008`.
- The fixed 8x768 Hybrid-only schedule completed exactly 6,144 formal asks,
  5,378 evaluated pairs and 2,676 productive candidates. There were zero
  primary-exact, pair-ID or nonempty behavior-signature duplicates, zero
  pair-search-score or optimizer-observation reward mismatches, and every
  productive candidate mapped to a distinct productive behavior family.
  Cumulative top-10 productive-family concentration was only 0.3737%.
  The two repeated empty behavior signatures were behavior-unresolved blocked
  pairs, not resolved behavior duplicates.
- Route productive/formal yields were Slow Temporal 48.62%, Slow
  Cross-sectional 30.73%, Market Regime 1.56%, First-N 1.56% and Disclosure
  0%. Evaluated/formal yields were 91.20%, 95.83%, 60.94%, 59.38% and 45.57%
  respectively. Slow Temporal median/p10 search score was 0.00871/-0.12759;
  Slow Cross-sectional was -0.01215/-0.29075; Market was
  -0.51944/-1.36581; First-N was -0.34693/-0.60902; Disclosure was
  -0.02325/-0.32328.
- Task-wall throughput was 2,520.90 formal asks, 2,206.61 evaluated pairs and
  1,097.97 productive candidates per hour. Productive yield by checkpoint was
  31.90%, 30.86%, 50.26%, 52.60%, 49.09%, 46.48%, 44.40% and 42.84%.
  New productive families equalled productive candidates in every checkpoint,
  so no family-information collapse occurred even as late-checkpoint yield
  declined. Runtime gates passed with 32 compute threads, pair batch 12,
  long-only 5 bps horizons 1/5/15/30, cache peak below 1.0 GB, minimum free
  memory 68.99 GB and zero validation/holdout/2026 reads.
- Campaign root status is `CAMPAIGN_CLOSED`; automatic validation, promotion,
  a successor scale tranche, unlimited search and a 20,000 target remain
  forbidden. The generated `train_finalists.parquet` is intentionally empty:
  the next phase must deduplicate and rank the 2,676 productive train
  candidates by behavior family and train stability before freezing a small
  finalist set. The compact closure receipt is
  `runtime/run_plans/cn_hybrid_bounded_large_tranche_20260729_receipt.json`.
  Frozen-contract and preflight evidence remains in
  `runtime/run_plans/cn_hybrid_bounded_large_tranche_p07_20260729_receipt.json`
  and
  `runtime/run_plans/cn_hybrid_bounded_large_tranche_p02_preflight_20260729_receipt.json`.

## A-share tradability authority boundary repair (2026-07-29)

- The existing Phase3CM candidate-parallel and streaming evaluators remain
  development-predictive authorities only. Their full-coordinate train scores
  may feed the accepted development optimizer, but do not prove A-share
  executability, tradability or economics.
- ADR 0010 supersedes only ADR 0009's over-broad optimizer hard gate. A
  development observation now requires a completed pair, pair-native
  train-feedback READY, primary standalone
  `TRAIN_REWARD_FOLLOWUP_READY`, immutable submission/pair receipts and the
  existing train-only access guards. It does not require executable replay.
- Historical `productive` labels remain development diagnostics for
  behavior-family deduplication and train-stability ranking. They are not
  finalist, promotion, validation or economic evidence.
- Development ask/tell uses
  `min(primary Phase3CM composite reward, primary-minus-control Phase3CM
  matched increment)` and writes the explicit evidence/scope labels
  `DEVELOPMENT_PREDICTIVE_SCORE_ONLY` and `DEVELOPMENT_SEARCH_ONLY`.
  Validation, holdout and 2026 remain unreachable from feedback.
- The executable replay implemented under ADR 0009 is retained as the same
  separate finalist/economic-eligibility gate. Both members must still carry
  canonical candidate- and exact-identity-bound receipts for execution clock,
  same-bar exclusion, T+1, limit-lock fills, suspensions, complete fees and a
  promotion-grade universe. Missing proof makes
  `finalist_execution_eligible=false`; it does not invalidate a clean
  development observation.
- The executable kernel now enforces close-t signal/next-session-open execution,
  sell-before-buy cash and holdings, T+1 inventory age, opening limit-up buy
  blocks, opening limit-down sell blocks with carry, suspension blocks, board
  lots and a date-covering explicit fee contract. Input panel bytes, universe
  manifest bytes, fee schedule, execution policy and source code are hashed
  into the immutable receipt.
- The current Phase3CM materialized tables do not directly carry every input
  needed by finalist execution replay. This is a finalist adapter/input-binding
  gap, not a failure of the project's PIT fabric and not a prerequisite for
  development feedback. PIT observable-time, release, maturity, suspension,
  limit-lifecycle and universe authorities remain in place; a later frozen
  finalist lane must bind their required fields explicitly.
- Existing replay kernel V2 now applies explicitly PIT-aligned cash/share
  corporate actions to opening holdings, requires integer resulting shares,
  charges sell-side fees on explicit delisting terminal liquidation and fails
  closed unless the final book is flat. The canonical receipt now requires
  both corporate-action and terminal-liquidation proof flags plus the frozen
  corporate-action policy hash. These are existing-authority contract repairs,
  not a second evaluator or new authority node.
- Verification was deliberately non-financial. Source commit
  `ac299246d4390eb256e42f5b472db203ba315e2d` reached 73/73 in the local
  affected suite before the final fail-closed finalist-blocker refinement; its
  impacted eight tests then re-passed. The final source passed 73/73 in the
  official 77o `alpha311` environment plus Python syntax compilation, from
  versioned workspace
  `D:\ChengboRemote\workspace\alpha_pit_feedback_boundary_20260729_220112_ac299246d439`.
  Deployment manifest SHA256 is
  `f987903e02d6fbc8a10e0802e813899c17999697d7985c9831fea9e0e44f7830`
  and package SHA256 is
  `907668c672d4e4cbbadccecfaf54113f4eb849d3abc4c341aeb2a9e345e32824`.
  No search, financial evaluation, validation, holdout or 2026 read was
  launched.
- The authorized zero-financial finalist input binding ran on 77o from pushed
  source SHA `3f32f5518f187a4f421208113179ca9d98fc97d3` and passed 69/69
  focused tests plus syntax compilation. The exact versioned package SHA256 is
  `6a2e921eae38563fe326564c1a23ddf6fe488feff0e58da6c9043c591341e77e`;
  deployment manifest SHA256 is
  `84a5f815c0be8be84fce503fa952fedb0ab47fad64d102d967817e02ddb61deb`.
- The frozen release directly binds code/time/open/high/low/close and
  `ctx_hfq_is_st`. Security type, exchange and conservative up/down limit
  derivations exist but are not materialized into the frozen finalist input.
  Universe eligibility, listing age, delisting, suspension, corporate-action
  cash/share, terminal-session and terminal-price fields are absent. The
  promotion-grade survivorship-free/delisting-inclusive universe manifest and
  actual account commission/minimum contract are also absent.
- The resulting status is therefore
  `HOLD_RESEARCH_FINALIST_INPUTS_INCOMPLETE` with 14 explicit blockers, not a
  PIT-fabric failure and not a financial result. Independent verification
  passed against both root self-hashes, every declared artifact and both input
  manifests. Source manifest SHA256 is
  `32bae0505d93c9653a741898776ac115a0e1095bff80310280f65672c0a4ac11`;
  verification receipt SHA256 is
  `ce24fd063ae68c2bbd074900f9b9e8f9545a8abb62d98be14ca26d2fb43fce4b`.
  Financial replay, validation, holdout, 2026, optimizer, scheduler, archive,
  promotion and successor-search activity remained zero/false. The compact
  receipt is
  `runtime/run_plans/cn_finalist_input_authority_64_20260730_receipt.json`.
- Existing CURRENT nodes and relationships are refreshed in place under ADR
  0010; no new authority node is created. RAW Graph semantic refresh and its
  configured external LLM remain documentation maintenance only, not a runtime
  dependency. No DeepSeek/API call is required for this correction.

## Development keep-review cohort freeze (2026-07-29)

- The closed 6,144-ask campaign's 2,676 development-productive pairs were
  reviewed without rerunning Phase3CM or reading validation, holdout or 2026.
  All 2,676 portfolio behavior-family IDs and all 2,676 behavior signatures
  were already unique, so no pair was removed by synthetic deduplication.
- Frozen Phase3CM train artifacts retained the required stability evidence:
  day Sortino and MCMC p25, worst/median horizon day Sortino, horizon
  dispersion, regime stability, Rank-IC hit rate, matched gross/net
  increments, cost difference and one-way turnover. The selector used only
  these immutable train fields.
- 2,145 pairs passed the fail-closed train-stability screen. The remaining 531
  stay `HOLD_RESEARCH`: 358 had nonpositive matched gross increment, 3 had
  nonpositive matched net increment, 2 had nonpositive train-day MCMC p25 and
  168 had train regime positive share below one half.
- A deterministic 64-pair / 128-member cohort is frozen with
  `review_outcome=ALLOW_KEEP_REVIEW` and
  `evidence_scope=DEVELOPMENT_TRAIN_ONLY`. It contains 39 Slow Temporal, 24
  Slow Cross-sectional and 1 First-N pair. The two Market Regime productives
  remained HOLD because their MCMC p25 was negative; route coverage did not
  override the stability gate. Disclosure had no productive input.
- Diversity is enforced at selection, not claimed from pairwise family
  uniqueness alone: the selected set is capped at 48 pairs per route, 32 per
  structural family and 48 per signal cluster. Actual maxima were 39/32/43.
  Selected exact identities, behavior families and behavior signatures are
  each 64/64 unique.
- The selector ran only on 77o from source SHA
  `76503e099ce90de338f0f8af15301f4ab9886b5c`. Independent verification from
  SHA `dd9662b7dd23a5cfd21dfd2d42a971ff05e2bd53` matched the manifest
  self-hash, all five declared outputs, all 38 immutable source artifacts and
  the complete rank/cap selection. Selection payload SHA256 is
  `5ab6dbdf374d4867cf197b254cf6672d342bed54436bf153a551ae65867a3815`;
  verification receipt SHA256 is
  `9fa1d943a87dfae5f7ab1e9ffd4d14e3d1f4c60082e9499d8b50ed6f8d4081e7`.
- The first selector attempt wrote zero cohort artifacts and failed closed
  because legacy root observations left structural/signal identities blank.
  Evidence is preserved; the repair binds those identities from checkpoint
  `full_behavior` authority by immutable pair ID and separately verifies every
  nonblank legacy identity. No failed result was reused.
- This cohort is not executable, validation, holdout, economic or promotion
  evidence. `finalist_execution_eligible`, `validation_eligible`,
  `promotion_eligible` and successor-search authorization all remain false.
  No optimizer, scheduler or archive write occurred. The compact receipt is
  `runtime/run_plans/cn_productive_keep_review_64_20260729_receipt.json`.

## Winner-guided bounded development search closure (2026-07-30)

- The separately authorized winner-structure-guided Hybrid tranche is closed
  and independently verified on 77o at pushed/deployed SHA
  `4f134281f4e9d13d591c5fd81dd32746a0258456`. Eight immutable checkpoints
  completed the fixed 12,288-ask schedule: 12,160 slow-temporal, 96 First-N,
  32 slow cross-sectional and zero market-regime/disclosure asks.
- The campaign produced 7,379 `PAIR_EVALUATED` results, 5,642 accepted
  full-train-only optimizer observations/COMPLETE trials and 2,836 development-
  productive candidates. Productive throughput was 635.59/hour. Productive
  identities covered 2,835 behavior families; the exact/family ratio was
  1.00035 and top-10 family concentration was 0.388%, so the run did not
  collapse into a small family cluster.
- Slow temporal produced 2,824 productive candidates from 12,160 formal asks
  (23.22%); slow cross-sectional produced 12/32 (37.50%); First-N produced
  0/96. Overall scored median was 0.000117 and p10 was -0.064901. Productive
  yield fell from 50.78% at checkpoint 002 to 14.84%-16.28% across checkpoints
  005-008, so this result does not authorize another scale tranche.
- Canonical payload hashes, all 192 checkpoint-declared artifacts, all 18 root
  artifacts and the full manifest chain matched. There were zero primary-exact,
  pair-ID or full-behavior duplicates and zero eligible-score/formula
  mismatches. Runtime gates passed with 32 threads, pair batch 12, long-only,
  5 bps and horizons 1/5/15/30; cache stayed below 1.10 GB, minimum free memory
  exceeded 68.90 GB and validation/holdout/2026 reads were zero.
- The first checkpoint-008 attempt failed only the unchanged acceleration gate
  (15.964 effective cores versus 16.0), was preserved, and was regenerated from
  verified checkpoint 007 without reusing financial results or optimizer
  state. The clean rerun passed at 16.912 effective cores. The earlier
  development-feedback authority failure is also preserved; no invalid
  financial result or optimizer state entered the valid chain.
- Root status is `CAMPAIGN_CLOSED`; `train_finalists.parquet` is intentionally
  empty. This is development-search evidence only: it does not establish OOS,
  executable, economic or promotion authority and does not reopen the accepted
  Hybrid policy. The subsequent 24-pair winner finalist freeze is the current
  bounded decision input; the earlier 64-pair keep-review cohort remains its
  immutable winner-guide evidence. The compact receipt is
  `runtime/run_plans/cn_winner_guided_large_search_20260730_receipt.json`.

## Phase3CM ordered-day uncertainty repair (2026-07-30)

- ADR 0011 corrects the historical `day_mcmc_*` naming and IID-resampling
  weakness without changing the ADR 0010 development/finalist boundary.
- Phase3CM now uses deterministic ordered-trade-day stationary block bootstrap
  uncertainty. The frozen block rule is rounded cube root of day count clamped
  to 2-20; candidate reward does not tune it.
- Canonical `day_uncertainty_*` output records the contract, method, seed,
  requested/valid/invalid draws, block metadata, tail quantiles, support above
  zero, Monte Carlo probability error, downside-day support and an IID
  diagnostic reference. Historical `day_mcmc_*` fields remain compatibility
  aliases only and do not claim a Bayesian posterior or Markov chain.
- The existing 20% train uncertainty weight and 0.60 support gate are retained.
  Sample-day and invalid-draw warnings are explicit diagnostics, not new hidden
  admission gates.
- Verification is synthetic/non-financial. No search, replay, validation,
  holdout, 2026 access, promotion or successor budget is authorized, and all
  closed artifacts remain immutable under their original reward contract.

## A-share finalist input authority completion (2026-07-30)

- The existing development candidate pool is not empty: the closed
  winner-guided search produced 2,836 development-productive candidates. The
  earlier zero referred to the absence of an authority-valid train-finalist
  input set, not to candidate supply.
- The existing 64-candidate keep-review cohort is now bound to the same
  A-share replay lane under execution SHA
  `c615f74013a6e4e14fdaa731de4a3659daf85bf4`; no new evaluator, platform,
  database or authority node was created.
- The frozen session authority contains 1,854,153 rows. Of 1,852,463 observed
  code/date rows, 1,851,076 have exact PIT ST state and the remaining 1,387
  exact-date source gaps are explicitly ineligible, with no forward fill,
  backfill or assumption of normal status. All 1,852,463 rows are therefore
  resolved or conservatively blocked. The sidecar contains no null ST state.
- `FINALIST_INPUT_AUTHORITY_READY` closed with zero blockers and independent
  verification PASS. The session manifest SHA256 is
  `2a92810a0f6d4ebaf0532ff356be754dba9d8db600568a78dcc8b0cf4b3d4d45`,
  the finalist-input manifest SHA256 is
  `615a0caafedfd1b67173862262f1d820e9203586150dc62637a9af7e7497f3d9`,
  and the verification receipt SHA256 is
  `1769df3507fc4f1f1cf7344b888a628a086c6a521306b3c24c79a5ad2cfda0a4`.
- This was a zero-financial qualification: financial, validation, holdout and
  2026 reads were zero; financial replay and report-only validation remain
  unauthorized. The fee binding remains a conservative research upper bound,
  not an actual broker-account contract and not promotion authority.

## Winner-guided 24-pair finalist freeze (2026-07-30)

- The existing keep-review selector and independent verifier were generalized
  rather than duplicated. Source productive count and cohort size are now
  contract-declared, while legacy 64-pair artifacts remain independently
  verifiable. The current selector performs the existing train hard screen,
  keeps the highest-ranked screen-pass representative for each behavior family
  and signature, then applies the existing 75% route, 50% structural-family
  and 75% signal-cluster concentration caps.
- The closed winner-guided pool contained 2,836 development-productive pairs.
  The train-only hard screen retained 2,067 and held 769: 686 for nonpositive
  matched gross increment, 22 for nonpositive matched net increment and 61 for
  regime-positive share below 0.5. No validation, holdout or 2026 data entered
  ranking or selection.
- The largest clean small cohort under the existing concentration caps is 24
  pairs/48 members: 18 `SLOW_TEMPORAL_CHANGE` and 6
  `SLOW_CROSS_SECTIONAL_LEVEL`, with 24 unique primary exact identities,
  behavior families and behavior signatures. A 32-pair cohort was not
  supply-feasible under the same caps because only six slow cross-sectional
  rows passed the train screen.
- The cohort closed on 77o from exact pushed/deployed SHA
  `06a58e0c5795ba2c7feef44f348db6f31cc27556`. Its manifest file SHA256 is
  `e37312fa79ace9a9244888d959605b438118352cc48030d89d2844302c05a6be`,
  payload SHA256 is
  `40437549a49852ced5f1ea1739edc440d43fdb2596feb1700a1a3360baa59a55`
  and selection payload SHA256 is
  `3bf60efb7816161f2f6a8a8004cc61aa4d5930e2c99d65ffb46ba1da7a215201`.
  Independent verification matched all five declared artifacts and all 38
  immutable source artifacts.
- The 24-pair cohort was bound to the existing A-share development
  release/session authority without creating a new authority node.
  `FINALIST_INPUT_AUTHORITY_READY` closed with zero blockers; its manifest file
  SHA256 is
  `3c4a1fe4d8568e63c1befb3743a18270fc35db5abc651ab49b01680101790189`
  and independent verification payload SHA256 is
  `b58a9eb9db2797b4ae215f8e005ae813e663dab9e36ed44f3a32cc8f5ad10d94`.
  Financial replay, report-only validation, promotion and successor search
  remain unauthorized by this zero-financial freeze.

## Fixed 24-pair executable replay and report-only OOS closure (2026-07-30)

- The exact frozen 24-pair / 48-member winner cohort completed retained-lane
  A-share train replay followed by report-only OOS on 77o at final execution
  SHA `b8e63b05020346b5f1786c7fcb43c1295ad8bf42`. Candidate identities and
  order matched selection payload SHA256
  `3bf60efb7816161f2f6a8a8004cc61aa4d5930e2c99d65ffb46ba1da7a215201`;
  no interstage filtering occurred.
- Executable replay remained deliberately fail-closed: 11/48 candidate
  members completed and 37 were blocked, leaving 2/24 complete matched pairs.
  Thirty-six blockers were final-session unliquidated holdings concentrated in
  `000545`, `002514` and `002581`; one control had no executable fills. These
  are terminal-liquidity/no-fill evidence, not a runtime failure, and they did
  not filter the unchanged OOS cohort.
- Report-only OOS evaluated all 24 pairs. Nine had positive transfer (37.5%);
  validation search-score median was -0.46185411 and p10 was -8.005324319.
  Slow Temporal was positive for 6/18 with median -1.230319075; Slow
  Cross-sectional was positive for 3/6 with median 0.009378515. This evidence
  is `HOLD_RESEARCH`, not candidate promotion or an economic claim.
- OOS closure canonical body SHA256
  `5333b7978c7652d1e10c6ffcf144427cbf2e6723fe30201875a6a4a9d37881be`
  matched with 9/9 declared artifacts. Root closure canonical body SHA256
  `792cb290187ab98dd697ae4931789b37f1da5d0aa1151f982d368ae3391fec4c`
  matched with 5/5 declared artifacts. Validation reads were 381,649;
  holdout/2026 reads and feedback/scheduler/archive/promotion writes were zero
  or forbidden, and protected source hashes were unchanged.
- The replay kernel marks ending holdings at the final PIT close before the
  flat-book check, but blocked candidates do not persist that in-memory path.
  A mark-to-market comparison therefore requires one separately bounded replay
  of the same 48 members; it must remain a diagnostic with unresolved
  liquidation risk reported separately and must not trigger new OOS or search.

## Same-cohort ending-book mark-to-market diagnostic closure (2026-07-31)

- The separately bounded diagnostic replay completed on 77o at exact
  pushed/deployed SHA `9a742e2fd392224cd4e60c773fde0455610ab129`.
  It retained the same ordered 24-pair / 48-member cohort and selection payload
  SHA256
  `3bf60efb7816161f2f6a8a8004cc61aa4d5930e2c99d65ffb46ba1da7a215201`;
  strict replay and OOS were not recomputed and no interstage filtering
  occurred.
- The diagnostic valued remaining positions at the final PIT close without
  fabricating a terminal sale or charging a terminal sale fee. Forty-seven
  candidate members completed; the existing no-fill control
  `cn.comp.4283153e01db8ad7ed21.control` remained fail-closed, leaving 23/24
  complete matched pairs.
- Independent verification matched the canonical manifest body SHA256
  `596c7820d160d47b196ee09a412573d11a4ba11c1e4ee0700079a525a773fc2f`,
  closure file SHA256
  `e5d0e113488b2b018955d5b4e35b93d7c656172d65aa23da9086a32efb39683a`
  and all 58 declared artifacts. Candidate and pair identities and order
  matched both the frozen source and unchanged OOS result. Validation,
  holdout and 2026 reads were zero; feedback, scheduler, archive, promotion
  and successor-search writes remained forbidden.
- Seventeen primary members had positive standalone train mark-to-market
  reward, but only 4/23 complete pairs had positive matched increment. The
  matched-increment median was -0.148394822 and p10 was -1.990572284. Primary
  ending-holdings weight had median 0.415363371 and p90 0.759098296; eight
  pairs ended above 50% invested and six above 75%, so terminal liquidity
  remains material rather than being erased by marking.
- Intersecting this diagnostic with the already closed 24-pair report-only OOS
  left exactly one pair positive on both dimensions:
  `cn.pair.cf7adc00bbda5464908c7ad0dc8fb258` /
  `cn.comp.8ec6fa0046650168ee04` in
  `SLOW_CROSS_SECTIONAL_LEVEL`. Its train mark-to-market matched increment was
  0.018555799, validation search score was 0.037562010 and ending-holdings
  weight was 0.262087250. Validation regime-positive share was only 0.5 and
  worst-regime day Sortino was -0.83256163, so it remains research evidence,
  not a promotion or economic claim.
- The other three train mark-to-market-positive pairs were OOS-negative. Eight
  other OOS-positive pairs lacked a positive matched mark-to-market increment,
  including the one no-fill-blocked pair. The diagnostic therefore resolves
  the terminal-mark accounting question but does not turn the cohort into a
  promotion-ready finalist set or authorize more search.

## Next action

Do not retry or reinterpret the invalid 12,288-ask search lane. Retain only its
three verified immutable checkpoints as partial development evidence.

Monitor only validation recovery task `lanjob_20260731_153336_a7d457`.
Verify exactly 126 candidate and 63 pair replay records with candidate-level
A-share blockers retained, replay closure, validation sidecars, the unchanged
63-pair report-only OOS cohort, positive validation reads and zero
optimizer/feedback/scheduler/archive/promotion writes or holdout/2026 reads.
The new corporate-action blocker must remain fail-closed and must not filter
the OOS cohort.

At valid closure, report actual partial search production, behavior-family
concentration, replay executability and OOS transfer without forcing PASS.
Then return to finalist deduplication and economic selection; do not
automatically launch another search tranche. Holdout/2026 access, promotion, a
new platform/database and cross-campaign reward memory remain unauthorized.

Do not rerun the Medium, its report-only validation, either completed
Hybrid-only tranche or earlier availability-agency canaries. Unlimited search,
another scale tranche and a 20,000-target campaign remain unauthorized.

Do not rerun Sign, CSRank, Abs, the repaired OLD supply proof, sampled
qualification, the nine V3 checkpoints, either Structural CEM V2 medium, the
completed four-lane supply probe, the 4x17 campaign or the typed-surface audit.
Retain the qualified `normalized_ratio` production increment and active
route-local sampled selector. Do not reuse any terminal CEM probabilities and
do not rerun or extend the closed 20,000-target TPE campaign. The Medium has
superseded fallback share as the primary search-policy KPI, but it has not
authorized candidate promotion, validation/holdout/2026 access, cross-campaign
reward memory or an unfrozen successor budget.

Do not rerun or enlarge the completed V4 supply proof or the closed V5 paired
qualification. Performance commit `72250f9` is now officially parity-checked
and directionally benchmarked on 77o; do not repeat that microbenchmark as
another project phase. The mature conditional thin-adapter requirement is now
mechanically demonstrated but the fixed-budget campaign above is incomplete.
Do not continue tuning the rejected rank-weighted CEM, reuse its probabilities,
launch another search, or open validation/holdout/2026 access without a new
explicit authorization and a supply/yield design that addresses the observed
late-checkpoint collapse.

Do not repeat the 1,024 wave, V1 canary, field qualification, route-supply
qualification, authority smoke, or the completed six-checkpoint large campaign.
Classify the six strict survivors and four partial positives by economic
mechanism, intended regime/event/state and portfolio role. Current strict
coverage consists of three intraday state-transition formulas, one
sentiment-conditioned market-regime formula and two high-turnover intraday
return-discrepancy formulas. Slow cross-sectional candidates are positive only
in aggregate and remain horizon-unstable; slow temporal change collapsed, while
Disclosure Event and First-N Aggregated have no strict survivor in this frozen
set. Any next development search should therefore target those materially
uncovered mechanisms rather than rerun a global candidate race.

The zero-financial-read route-lane integration and pre-budget supply
calculations and the authorized Optuna TPE run are complete. Preserve its
immutable ask/tell transcripts, Phase3CM checkpoints and fail-closed incomplete
verdict. The failed CatCMA and V1/V2 CEM states remain evidence, not reusable
initializers. Do not promote
legacy scheduler arm names into a second top-level authority, import
reward-bearing cross-campaign memory, assign invented
fresh/crossover/mutation percentages, or build MCTS/crossover/mutation
machinery. Broad Event remains a zero-budget frozen reference. Strict Stage A,
candidate promotion, any additional holdout access, 2026 access and
cross-campaign reward memory remain forbidden until separately authorized.
Plate resumes only when real PIT minute materialization is present on 77o.

For the closed slow cross-sectional campaign, do not rerun the 392-pair train
search or the repaired validation. Retain the fundamental-only OOS survivors
for economic/regime classification, explicitly reject the severe chip/market-
capitalization left-tail formulas, and require a separate authorization before
any further validation, holdout access, candidate promotion or new search.
