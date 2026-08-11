# CN true1min Current State

Updated: 2026-08-11

Current state: `SEARCH_CONTROL_REPAIR_CLOSED_PHASE_D_HOLD_SEARCH_ENGINE_V2_IMPLEMENTED_CANARY_FROZEN_NOT_RUN_FORWARD_B_SEALED_OOS_NONE_HOLD_PROMOTION`

Mission authority: `.planning/PROJECT.md`

Progress authority: this file

Architecture input: `config/architecture_overlay.json`

Generated CURRENT: `.planning/graphs/current.json`

Complete field authority: `runtime/field_registry/cn_field_master_registry_v1/cn_field_master_registry_v1.json`

## Current accepted capabilities

### Search Engine V2 implemented and prospective 512 canary frozen without a financial run (2026-08-11)

- Code/ADR/frozen-plan authority is commit
  `3ea8091796910da8172b9134f186ef57c725e93a`. The curated CURRENT projection
  now contains experimental node `cn_search_engine_v2_experimental` with
  `STATIC_VERIFIED / PASS` evidence and explicit `NOT_RUN / NO_RUNTIME_ASSURANCE`
  badges. RAW remains intentionally `STALE / RAW_GRAPH_SOURCE_CHANGED`; this
  task did not perform or require a full RAW rebuild.
- ADR 0018 accepts an experimental two-head search-control design. Absolute
  standalone economics is an admission gate only; exact Full-vs-Base enhancer
  uplift becomes program/template-level search credit only after admission.
  The two domains are not scalarized, standalone failure cannot be offset by
  relative uplift, and strong Base economics cannot be credited to a useless
  enhancer. Component attribution remains explicitly
  `COMPONENT_ATTRIBUTION_UNIDENTIFIED`.
- The implementation reuses Candidate Program V1, route-local proposal
  machinery, compiler, matched-control construction, Phase3CM evaluation and
  the existing checkpoint engine. New code is limited to admission,
  conditional uplift, campaign-local hierarchical scheduling, a zero-financial
  freeze and a thin runner adapter. The formal Hybrid TPE plus Availability
  development-search authority is unchanged; Search V2 remains
  `EXPERIMENTAL` and owns no economic, alpha, OOS, sealed-data or promotion
  authority.
- The self-hashed prospective plan freezes exactly 512 asks before any new
  financial observation: 64 per each of eight templates. `BASE` has 64
  `UNIFORM_FRESH`; each enhanced template has 32 `UNIFORM_FRESH`, 24
  `CONDITIONAL_UPLIFT_EXPLOIT` and 8 `NOVELTY_RESERVE`, for exact totals
  288/168/56. Checkpoints are template-local and fixed; no adaptive budget
  reallocation, spillover or result-dependent schedule change is allowed.
- Fresh-state provenance is explicit: zero imported Phase B/C/D financial
  observations, zero development observations, no serialized optimizer state,
  candidate results, factor statistics, behavior statistics or template
  classification. Manual diagnosis and post-parent objective design are true,
  so `cross_campaign_development_feedback=true` honestly describes adaptive
  design without reused optimizer state.
- Prospective decision gates are frozen as
  `NONINFERIOR_ABSOLUTE_AND_SUPERIOR_CONDITIONAL_UPLIFT`: strict zero-margin
  absolute noninferiority plus strictly positive conditional matched-uplift
  improvement, minimum admitted support and at least four improved templates.
  Neither side can compensate for failure of the other and insufficient
  support fails closed. The future runner applies this rule mechanically to a
  single manifest-bound feedback byte buffer; its formal V2 closure is written
  only after the dual PASS/FAIL decision, so there is no pre-gate complete
  artifact to misread.
- Future execution has one physical route,
  `cn-joint-program-search-v2-canary`, behind exact Project Control campaign,
  run, checkout and output-root binding. Its canonical 77o launcher does not
  preclaim a new output root and direct runner invocation denies. No Project
  Control admission was materialized and the canary was not run. The
  prefinancial freeze also rebinds the execution contract, development price,
  development field manifest, registry and capacity to the accepted immutable
  Phase B input metadata before any financial read.
- Evaluation authority remains closed: 2023 is spent/negative/no-retry,
  Forward-B is sealed, validation/holdout cannot feed search, OOS grade is
  `NONE` and promotion remains `HOLD`. This implementation read only code,
  governance, compact immutable outcome/provenance metadata and synthetic
  fixtures; financial dataset reads were zero.

Status summary:

```text
SEARCH_CONTROL_REPAIR = CLOSED
PHASE_D = HOLD
SEARCH_ENGINE_V2 = IMPLEMENTED / CANARY_FROZEN
SEARCH_V2_CANARY = NOT_RUN
FORWARD_B = SEALED
OOS_GRADE = NONE
PROMOTION = HOLD
```

### Search-Control Repair V1 closed without a new financial run (2026-08-11)

- Accepted ADRs 0016 and 0017 and implementation commits
  `0247cbbc466b60287a77ebcd03672a9df6afad5a` and
  `bc8d1fb29a9eb6c8798b43bbdc4528f97d552830`, with exact campaign-authority
  closure at `b41c532818cde6e4a523773abefc2cee57dc56aa` and closed-launcher retirement
  at `4d86772a8ea65f36dde2923bde66ff0d7bf5be7b`, reconcile the execution layer
  with already observed facts. The 2023 historical challenge is permanently
  `spent`, economically `NEGATIVE`, and denied with
  `PERMANENT_DENY_ALREADY_SPENT`; its older unopened authorization remains
  immutable historical evidence and cannot authorize a retry. The destructive
  launcher checks this authority before archive hashing, output creation, data
  conversion, or financial computation.
- Canonical high-cost `app.py` routes now consume an immutable Project Control
  admission before route import. Its trusted Harness bundle and task request
  bind the exact project, repository, clean repo SHA, action, campaign, target
  run, absolute output root, expiry, control receipt, task/profile/context files
  and hashes. Activation internally fixes the canonical trust config, derives
  the actually executing checkout, selects its allowlisted local-or-77o Harness
  authority-store root, derives that checkout's live clean HEAD, and revalidates
  the immutable admission rather than accepting caller-supplied
  trust/repository/SHA/proof values. Current 77o launch wrappers forward the
  exact target run, admission and file hash; the immutable Harness bundle must
  first be mirrored into the fixed 77o authority-store path or execution denies.
  Each route consumes a
  one-use capability
  before argument parsing and rechecks the parsed output root before data
  access. Before route import, a durable exclusive consumption record is written
  at the admitted output root: new freeze/launch/successor/retry requires an
  absent root or only route-qualified launcher control metadata; the gate then
  adds its internal control directory before business execution. Recovery
  requires the original root identity and its own unused
  admission. Direct invocation, replay and freeze-as-launch therefore deny.
  Successors require
  exact parent lineage,
  parent `POST_BATCH=CONTINUE` and child `PREFLIGHT=PROCEED`; technical recovery
  requires the original eligible admission for the same target plus an incident
  binding. Targeted and winner-guided routes bind the live authorization path,
  hash, campaign id and profile to the reviewed request, then reuse that same
  byte-verified payload without reopening the file. Winner-guided
  successor/continuation profiles also preserve successor lineage across
  recovery, so generic launch, unbound retry and recovery of a rejected launch
  cannot bypass parent control. Preparation one-shots stop before the external
  admission boundary. Project Control owns execution admission only and is
  forbidden from deciding economics or selecting alpha. Routes without implemented same-run
  resume semantics fail closed rather than advertising recovery;
  fixed-stratified exposes launch, successor and retry only.
  The completed `cn_large_optuna_tpe_actual20000_v2` wrapper is explicitly
  retired as historical provenance because it is not a current runtime profile;
  Project Control cannot reopen it as a new launch.
- The existing Harness emits no cryptographic receipt signature. Its configured
  runs root is the explicit authority-store trust boundary: canonical location,
  full bundle shape and all bound hashes are verified, while a malicious local
  writer already holding authority-store permissions remains outside this thin
  bridge's detectable threat boundary.
- Phase C and allocator-repair seed writers and readers now distinguish serialized
  optimizer-state import from development financial observations, candidate
  results, behavior/template statistics, manual diagnosis, and whether the new
  objective was designed after parent results. Readers fail closed on dishonest
  or partial embedded provenance and label closed legacy artifacts as explicit
  non-authoritative projections. Existing optimizer objectives, scorers,
  candidate formulas and evaluator semantics were not changed.
- This repair performed no search, financial replay, validation, OOS,
  historical-2023, Forward-B or 2026 data read. Forward-B remains sealed and
  not authorized; Phase D remains valid adaptive development evidence with OOS
  grade `NONE`, held without promotion or automatic successor.
- The accepted overlay and deterministic CURRENT projection now state the
  repaired boundaries. RAW remains at
  `f649507ec3002161735085dde3f16fd2b1a379bc`; therefore CURRENT is explicitly
  `STALE` and not strict-ready even though the required Project-Control-to-search
  precondition is deterministically projected. RAW/CURRENT are advisory until a
  later reliable RAW refresh; no freshness or runtime assurance is fabricated.

### Joint Program Phase D closed; revised allocator held without promotion (2026-08-11)

- The prospective development-only Phase D campaign closed 64/64 immutable
  checkpoints and 512/512 exact records. Checkpoints 001-054 were built at
  exact pushed/deployed runner SHA
  `7e41ddca36337ecc4451ff05648431632a54f940`; after a preserved
  parallel-process failure and bounded single-record localization, checkpoints
  055-064 and the root were recovered with fresh one-record child processes at
  exact pushed/deployed finalizer SHA
  `a1623f87b480c5b21c89868d00e8cd24c3475e46`. No incomplete or diagnostic
  financial result was reused.
- The independently verified root contains exactly 304 `UNIFORM_FRESH`, 96
  `REVISED_EXPLOIT` and 112 `NOVELTY_RESERVE` asks, 109 productive records and
  20 fail-closed replay blockers. Its closure file/payload SHA256 values are
  `0fe0849702c7ca7e4be7d661a0dc7493df8859f98110d3da10afb0342408a985` /
  `41671dbc1d1f116c3e0e979756aebe4995c109019168c1d8a43ae0deee99ada3`.
  All artifact/self-hash/checkpoint chains, exact ordinals 0-511 and accounting
  invariants passed; maximum accounting error was
  `6.853952072560787e-09`. Validation, holdout, spent-2023, Forward-B and 2026
  reads were all zero.
- The independent final audit passed structure, frozen schedule, checkpoint and
  bandit chains, controls, accounting, blocker taxonomy, recovery bindings,
  memory telemetry and sealed-read boundaries. Audit file/payload SHA256 values
  are `ef4569f5e58f4f52c98ff053e3628a2adb370a9c9eb9c192f75d8a73a3a96df1` /
  `d24ce2e09339a780627572b804c9f2ff5242853ae7b0ca9038d2cca9ac7735e1`.
  The fixed 24 GiB hard gate was absent as frozen; telemetry remained active and
  minimum checkpoint-boundary free memory was 84,731,297,792 bytes.
- Revised exploit beat uniform across the four compared templates on productive
  rate (`+0.104167`), all-four-positive rate (`+0.041667`), primary reward and
  return positive rates (`+0.229167` / `+0.208333`), three-window-positive rate
  (`+0.260417`) and blocker rate (`-0.218750`). These are useful
  development-only improvements, not an authority or OOS claim.
- Four frozen gates failed: matched-return mean and median deltas were
  `-0.111195` / `-0.210940`, median return per turnover delta was `-0.041295`,
  and only two of four revised templates met the frozen improvement rule versus
  the required three. `BASE_EVENT`, `BASE_MARKET_EVENT` and
  `BASE_TEMPORAL_EVENT` all had negative matched-return mean deltas; only
  `BASE_TEMPORAL` improved matched-return mean and median.
- Decision: `HOLD_REVISED_ALLOCATOR_NO_PROMOTION` with research-bias status
  `HOLD_RESEARCH` and OOS grade `NONE`. The revised allocator remains non-formal
  evidence and does not replace the accepted development-search authority.
  No automatic successor or promotion is authorized. Compact receipt:
  `runtime/run_plans/cn_joint_program_phase_d_outcome_20260811.json`,
  file/payload SHA256
  `5ad11e6a4b492f7cb7d5e03f0794d2fdf8e79dd97ae732d59d0d9a45e3c4de4f` /
  `abe6f638d1021a38b5872c9be5d1a1d3e7a775a760ed5ec27d4be53b1eb32182`.

### Joint Program allocator repair canary closed; Phase D eligible but not launched (2026-08-10)

- The separately frozen development-only allocator repair canary closed 32/32
  immutable checkpoints and 256/256 records at exact pushed/deployed runner SHA
  `05a2d342d81970eaa6f1d9a63c597a9e5583308f`. Its arm counts are exactly
  116 `UNIFORM_FRESH`, 84 `REVISED_EXPLOIT` and 56 `NOVELTY_RESERVE`; the
  decision comparison uses equal 84-record enhanced-template samples for
  revised exploit and uniform. It imports 56 Phase-B seed observations and
  reuses zero Phase-C financial records.
- The root closure file/payload SHA256 values are
  `44c97f1e829b6ee2c0120d27ff9e2b2386b2b9a611b2e437b973b314895e0896` /
  `e465a03ee588780d2f39cb6cde37820676cc8b1cef97b27ce82b1f93c2b73475`.
  The 24 GiB checkpoint-boundary gate passed with 84,633,890,816 bytes
  minimum free memory; validation, holdout, spent-2023, Forward-B and 2026
  reads are all zero, and Phase D was not launched.
- The independent audit at exact pushed SHA
  `3f15b474f3bb403c7fb1fcce7661c0287cc311f2` passed structure, schedule,
  checkpoint/bandit chains, controls, accounting, blockers, resources and
  zero-read gates. Audit file/payload SHA256 values are
  `6247ca58d8beff8cd2aa5940524dc6aeadcf6ae3f007784e79b16690343864cc` /
  `e2599862dd39c1b1a0cbd166af558894a045e70eb944c1bf83a60a106d68e2eb`.
- All eight frozen numeric decision gates passed. Revised exploit minus
  enhanced uniform improved productive rate by `+0.154762`, all-four-positive
  rate by `+0.011905`, primary reward/return positive rates by
  `+0.142857` / `+0.142857`, three-window-positive rate by `+0.166667`,
  median return per turnover by `+0.799000`, and blocker rate by `-0.083333`.
  Four of seven enhanced templates met the frozen template-improvement rule.
- The qualification is deliberately narrow. Revised exploit's matched-return
  increment mean/median were worse than uniform by `-0.052245` / `-0.064628`,
  and `BASE_MARKET`, `BASE_TEMPORAL_MARKET` and
  `BASE_TEMPORAL_MARKET_EVENT` did not improve; the last had a
  `-0.416667` all-four-positive-rate delta. `NOVELTY_RESERVE` showed strong
  descriptive development rates but had only 56 records and was not the
  frozen Phase-D decision comparator.
- Decision: `PHASE_D_ELIGIBLE_NO_LAUNCH`. This is development-only evidence,
  not OOS, promotion or formal search-policy authority. Compact receipt:
  `runtime/run_plans/cn_joint_program_allocator_repair_canary_outcome_20260810.json`,
  file/payload SHA256
  `013166235cfc6b92d07c5d241b42eccbbeb2d0462caa5c106750b7937e768317` /
  `8d28f30ad90776674fad6b7745b003a59dd8d36937ca16ee80e6f7f56a18b7fb`.
- Curated CURRENT now carries this runtime-verified experimental evidence. RAW
  Graph remains stale at `f649507ec3002161735085dde3f16fd2b1a379bc`
  because its configured DeepSeek refresh is externally blocked by the
  previously recorded 402 insufficient-balance response; no RAW refresh was
  accepted or substituted.

### Joint Program Rolling Search V0 Phase C closed; factorized exploit not qualified and Phase D held (2026-08-10)

- The exact development-only Phase C campaign closed 64/64 immutable
  checkpoints and 512/512 records. Checkpoint construction used exact pushed
  SHA `336b108f66ade8a18c9415d21f32cf6418d69d4a`; a metadata-only root
  finalization recovery at exact pushed SHA
  `6915b24de9f46a77639c19bd99cdc6527721da6d` reran zero financial records.
  Closure file/payload SHA256 values are
  `890480de62b7475ebdfbba09843ba25b6760789d43647782f26e474b97e4a057` /
  `1761e7e6a7c3101a961a60b83e51947757caf669b4dc6d5232417727b8746437`.
- The campaign contains exactly 260 `UNIFORM_FRESH`, 168
  `FACTORIZED_EXPLOIT` and 84 `NOVELTY_RESERVE` asks. All seven enhanced
  templates retain exact per-template quotas 28/24/12, while all 64 BASE
  parity asks remain uniform. The campaign-local bandit chain is intact across
  all 64 checkpoints: 512 feedback records, 421 updates, 51 initial and 472
  final observations, zero validation feedback and zero serialized optimizer
  state import. The 51 initial observations were reconstructed from Phase-B
  development financial results, so cross-campaign development feedback is
  explicitly `true`; this is adaptive development, not fresh confirmation.
- Exact replay closure is 482 complete and 30 fail-closed blocked records.
  All blocked rows are `CORPORATE_ACTION_FRACTIONAL_SHARES`, carry no search
  score or matched economic claim and remain excluded from economic summaries.
  Accounting invariants passed for every available leg; zero validation,
  holdout, spent-2023, Forward-B or 2026 reads occurred. The 24 GiB checkpoint
  boundary gate passed with 84,860,440,576 bytes minimum free memory.
- The factorized arm did not improve the decision metrics over uniform among
  enhanced complete replays. Its productive-rate and all-four-positive deltas
  were `-0.022585` and `-0.002569`; primary reward/return positive-rate deltas
  were `-0.244220` / `-0.244613`; three-window-positive delta was `-0.049879`;
  median primary reward/return deltas were `-0.432628` / `-0.127241`; median
  return-per-turnover delta was `-2.970487`; and blocked-rate delta was
  `+0.148810`.
- Factorized exploit did improve matched-control reward/return positive rates
  by `+0.158877` / `+0.217461`, but this relative gain came with weaker
  standalone economics, stability, turnover efficiency and blocker incidence.
  `NOVELTY_RESERVE`, not factorized exploit, had the strongest development-only
  productive rate (`0.650602`). This is useful allocator-diagnostic evidence,
  not OOS or promotion evidence.
- Final independent audit status is PASS for structure, hashes, checkpoint and
  bandit chains, accounting, blocker taxonomy, resources and zero-read gates;
  its file/payload SHA256 values are
  `65e3174c8a0048e17e5fcdafa1823e8cc8f2dd25005c41b8ceec0c9f861c201b` /
  `60d0ecc7d768b4ef0813951691124574b12955838d719fa3a168617698019288`.
  Research-bias status is `HOLD_RESEARCH`: this was one adaptive development
  campaign with no validation, OOS or untouched confirmation claim.
- Decision: `HOLD_PHASE_D_NO_LAUNCH`. Phase C remains non-formal evidence and
  does not replace the accepted development search policy. Compact receipt:
  `runtime/run_plans/cn_joint_program_phase_c_outcome_20260810.json`, payload
  SHA256 `1eea0dacba0a41912ba97d462ab25a9df469c82d1d65054373b10f05e983aeb0`.

### Joint Program Rolling Search V0 Phase B qualified; bounded development-only Phase C authorized but not launched (2026-08-09)

- The exact frozen 64-record uniform pilot closed at pushed/deployed runner SHA
  `bd771fdd911e8c16a976b9c6c26ede6d464fef18`: eight immutable
  checkpoint-scoped PROCESS_POOL chains, eight records for every one of the
  eight joint templates, 8/8 BASE compiled-versus-legacy parity, 62 complete
  Full/Base replays, two fail-closed candidate-local replay blockers, 23
  productive records, 59 behavior identities and zero semantic no-ops.
- Independent audit executed 3,582 exact checks with zero failures. It verified
  every canonical/file/artifact hash, the GENESIS-to-checkpoint-008 manifest
  chain, exact frozen identity/order, controls, accounting, blocker taxonomy,
  productivity summaries, checkpoint-boundary resource gates and zero
  validation, holdout, spent-2023, Forward-B or 2026 reads. The accepted audit
  file/payload SHA256 values are
  `baae7a4f03073b8b9499336379f9c3a6370b6d5b92743d3b41e3b49db2d09dc8` /
  `12d6700c02d0a71388392120af3d235a598618c7a33a0f07b7eb6907843e1543`.
  Closure file/payload SHA256 values are
  `14623ebc131b84660bd131b11268e40433ea1f219d1ed77caff2eb6ab147106a` /
  `0765c684c3f67ab592eb1006dbd1a5dd6e96dad7825fef251ac0d4f6ee36f3cf`.
- Real development-only template productivity is now comparable. Productive
  counts / median matched increments were: BASE `3/8 / +0.0191`,
  BASE_TEMPORAL `1/8 / -0.4479`, BASE_MARKET `3/8 / -0.0310`,
  BASE_EVENT `4/7 / +1.4196`, BASE_TEMPORAL_MARKET `1/8 / +0.0514`,
  BASE_TEMPORAL_EVENT `5/8 / +0.0345`, BASE_MARKET_EVENT
  `6/8 / +1.0045`, and BASE_TEMPORAL_MARKET_EVENT `0/7 / -1.1476`.
  These differences are evidence for a bounded campaign-local allocation test,
  not proof that event-bearing templates generalize.
- Candidate-local corporate-action failures remain fail closed rather than
  aborting the process pool. Ordinal 29 is an immutable
  `PAIR_REPLAY_BLOCKED` record for `CORPORATE_ACTION_FRACTIONAL_SHARES` on the
  `BASE_CONTROL` leg under unchanged `FAIL_CLOSED_NON_INTEGER`; it carries no
  matched economic claim or search score. Maximum cash/NAV/PnL/lot accounting
  errors were `5.093170329928398e-10` / `2.3283064365386963e-10` /
  `4.0745362639427185e-09` / `0`.
- Checkpoint-scoped recycling passed the unchanged 24 GiB boundary gate with
  86,099,345,408 bytes minimum free memory and 201.5446 records/hour. The
  23,734,984,704-byte in-pool low watermark is telemetry only; enforcement is
  deliberately sampled after each eight-record pool exits and before its
  checkpoint seals.
- Decision: authorize one separately frozen, bounded, development-only Phase C
  using the already implemented campaign-local factorized allocator. Phase C
  is not launched by this closure. It must bind this exact Phase B receipt,
  retain nonzero exploration, unchanged route-local TPE and financial/resource
  authorities, immutable checkpoints and zero sealed reads. It may not export
  reward credit across campaigns, rewrite shared route TPE authority, open
  Forward-B, consume OOS feedback or promote candidates/search policy. Compact
  receipt:
  `runtime/run_plans/cn_joint_program_rolling_search_v0_phase_b_outcome_20260809.json`,
  file/payload SHA256
  `5a09e569219dc78557c8780688b0d351e350262bb54b62ef0365adc5ccc3dd54` /
  `610f49c36e3e25081a8fec731ef26965f6763e26b3689387cd7f59d4c49b4ce6`.

### Typed Candidate Program V1 non-formal representation qualified (2026-08-06)

- `CN_TYPED_CANDIDATE_PROGRAM_V1` is now a qualified experimental
  representation/compiler layer for heterogeneous multi-component candidates.
  It reuses the unified capability registry, existing typed route compiler,
  candidate/pair receipt gates, Phase3CM shared DAG/evaluator and frozen
  `TOPK_10_EQUAL` portfolio authority; it creates no second generator,
  scheduler, evaluator, reward or promotion authority.
- Whole-program identity is reward/runtime/attempt independent and binds the
  component semantic identities, four explicit outputs, joint clock, matched
  control, portfolio contract and frozen component references. Proposal lineage
  remains separately self-hashed. Deterministic grammar replay rejects forged
  candidate, matched-control, pair or generator-provenance receipts, while the
  same candidate reached through the raw grammar or production registry wrapper
  has the same program identity.
- The fixed smoke at exact implementation SHA
  `3ad4c3dd7005d9eafb5f16a1ef5573fb818bdddf` closed all six golden fixtures in
  fixed order: five compiled and the unregistered Billboard episode failed
  closed. All 21 declared artifacts verified; the manifest file/payload SHA256
  values are `939fe0c96c9f308af5b05ea63f1a2824a235f43f0250ea5971a6d51ac34dc319` /
  `1caaca3c5e8e87c8bf9bb611df17f6fd0ada4d04e19ece93a362e82e49c45f21`.
  Closure file/payload SHA256 values are
  `49f267d03e06c7f6b51911abb1be3a0d3f2c31ba775280598c80f9b3cef33c45` /
  `43057a328009caedc9290b6e83295f9e59a2aca265e6392712241b985351f9c6`.
- Independent audit status is
  `CN_TYPED_CANDIDATE_PROGRAM_V1_INDEPENDENT_AUDIT_PASS`, payload SHA256
  `dfe08d08a6db1cced44d2040210f50e998c39a2887e4d216bc9df254962633ce`.
  Full focused regression passed 176/176 and both independent review axes were
  CLEAN. Financial evaluation and validation, holdout, historical, forward-2026
  and Forward-B reads were zero.
- This qualification does not claim alpha, production productivity, formal
  search admission or promotion. The existing eight-template V0 fixed-strata
  policy remains unchanged with no unified TPE credit. Plate, industry and
  Billboard materialization remain fail closed; Broad Event remains frozen
  reference only.

### Candidate Representation V0 fixed-stratified train production evidence accepted (2026-08-06)

- The bounded train-only V0 canary kept all eight template budgets fixed at 32
  attempts with no shared TPE credit, optimizer feedback, dynamic reallocation,
  replacement or cross-template spillover. Execution used exact SHA
  `f1b93620f21679329081e499d86b8a8c8b0da9d5`; the post-closure verifier fixes
  are exact pushed SHA `64fb42788630fe79213e291245595a1909c98b14`.
- The frozen 229-pair / 458-member cohort passed a prefinancial materialization
  screen with 180 compatible pairs and 49 incompatible pairs. Compatible counts
  in registry order were `32/32/32/24/28/32/0/0`; the 49 incompatible pairs were
  seven Slow Temporal, 31 Intraday State Transition and 11 Broad Event pairs.
  The two zero-materialized strata are structural capacity observations, not
  measured economic failures, and no missing slot was reassigned.
- Per-route evaluated/productive counts were `31/0`, `18/2`, `23/7`, `22/11`,
  `20/4`, `24/1`, `0/0`, `0/0`. Across all strata, 138 pairs were evaluated and
  25 were development-productive in 2,508.25 seconds, or
  `198.06638094288846` evaluated pairs/hour versus the frozen `57.25` floor.
  Minimum observed free memory was 69,055,098,880 bytes, above the unchanged
  24 GiB gate.
- The first post-closure self-check failed only because the verifier rejected
  legitimate zero-byte evidence files and then assumed frozen input order for
  backend-canonical pair receipts. The financial root was not rerun or reused
  as an incomplete boundary. The incident is preserved with SHA256
  `546d4c2944fd448a75f2afbdae0779afa62ef4d6be0da2170903783d53e281f4`;
  the corrected pushed verifier independently passed the existing complete root.
- Immutable evidence root:
  `D:\ChengboRemote\runtime\cn_fixed_stratified_production_v0_8x32_20260806_f1b9362`.
  Closure file/canonical SHA256 values are
  `bda072cc965db1f915fd7495b47e91b862917d061534a396831d3ed7c287fc9f` /
  `bc81007a9ee8842ad1d9fcb844332c723cb6499a344bb61ede200e2030d4964e`;
  all 150 declared artifacts passed. Independent audit file/canonical SHA256
  values are `20bae4c61706edadf830cef9d42c796bec81912dbec07736d7362224192a4b2e` /
  `e1839aa2fc1babbd85588b36484ff8d213f589588102e94f0f90c9bf19de158c`.
  Validation, holdout, forward-2026 and sealed reads were zero, and no promotion
  authority changed.
- This is the first real production-rate observation, not authority for an
  adaptive allocator. Rate comparisons are incomplete for two templates and
  based on one frozen tranche; dynamic budget allocation therefore remains
  deferred.

### Candidate Representation V0 fixed-stratified supply preflight accepted (2026-08-06)

- Candidate semantics are unified across the existing eight registry routes;
  `template_id` is exactly the existing `route_id` and creates no second
  scheduler or generator-eligibility authority. V0 uses fixed per-template
  quotas, four frozen seeds and deterministic attempt order. Shared TPE study
  or credit, optimizer feedback, dynamic budget reallocation and underfill
  spillover are all disabled.
- The zero-financial preflight ran 32 attempts in each of the eight strata
  (256 total) and independently reverified its closure at current source SHA
  `1f94ef2255081a745b553b046afcfdb617982aeb`. All 256 attempts had a legal
  primary and valid matched control; 229 unique pairs / 458 candidate members
  remained after exact-identity and pair deduplication. Validation, holdout,
  forward-2026 and all sealed reads were zero.
- Unique pair supply by route was `32/32/32/31/28/32/31/11` in registry order.
  The frozen Broad Event inventory produced only 11 unique pairs from its fixed
  quota, with 21 duplicate underfill attempts. Those slots were intentionally
  not transferred to another route. This is an observed supply constraint,
  not poor economic productivity and not permission to alter V0 quotas.
- Economic production fields are explicitly unavailable (`null`) with state
  `NOT_EVALUATED_ZERO_FINANCIAL_PREFLIGHT`; no false zero productivity was
  recorded. Dynamic allocation remains deferred until a separately bounded
  train-only evaluation supplies actual evaluated/productive/standalone/
  matched/behavior/throughput observations for every stratum.
- Formal receipt:
  `runtime/run_plans/cn_candidate_representation_v0_preflight_20260806.json`.
  Immutable evidence root:
  `G:\Chengbo\runtime\cn_candidate_representation_v0_preflight_8x32_20260806_69ad0a9`;
  closure file/canonical SHA256 values are
  `8ff20659ddd350d9a769f8f8ced4aa3e7b2a6d0cb817fa6bc9cd520a15232d02` /
  `35e30a06d1739ccdf3a3eaa8d30098e6f67bc20699cb139ba1240b265ffd4435`.
  This capability changes neither the negative 2023 result nor
  `HOLD_PROMOTION`; Forward-B remains sealed.

### Fixed-ten 2023 historical challenge negative; Forward-B remains sealed (2026-08-06)

- The unchanged fixed ten pairs completed the one-shot backward-OOT 2023
  challenge at exact pushed/deployed SHA
  `d1b2e3628120e5b1d08892ee4ba6cc1d603243ee`. The task exited zero and
  released its `VALIDATION_EXCLUSIVE_32` lease. The immutable closure at
  `D:\ChengboRemote\runtime\cn_fixed10_historical_challenge_2023_20260806_d1b2e36`
  has file/canonical SHA256 values
  `8701b0acbf99a398594cd25ac5328d8d89741fb0bbcbcda02525f81ee0aa8af4` /
  `677fa24f6ff4ab421b9e75a12a2151d88296021eef7dfe6667864315a37a1eea`.
- Independent audit verified all 24 closure artifacts, 308 authority/source/
  shard artifacts, 20 immutable candidate records, ten pair rows, exact
  identity/order and all accounting invariants. The dated statutory fee
  authority applied 10 bps sell stamp duty through 2023-08-27 and 5 bps from
  2023-08-28. Historical reads were 2,383,217; validation, holdout and
  forward-2026 reads were zero, and optimizer, scheduler, archive and
  promotion writes remained forbidden.
- Economic transfer failed for the fixed cohort. Zero of ten primaries had
  positive standalone reward or cumulative net return; four had positive
  matched reward/return increments, but zero passed all four economic gates.
  Primary cumulative return median/p10 were `-0.195600773` / `-0.283857881`;
  matched return increment median/p10 were `-0.053045413` / `-0.111763203`.
  This is accepted negative backward-OOT evidence, not an infrastructure
  failure and not a reason to reinterpret adaptive-validation positives as
  confirmed alpha.
- The independent decision is therefore not to spend the last sealed
  Forward-B asset on this cohort. Forward-B remains performance-unopened and
  no search, replacement, backfill, tuning, validation, promotion or automatic
  successor run is authorized. The compact outcome receipt is
  `runtime/run_plans/cn_fixed10_historical_challenge_2023_outcome_20260806.json`.

### 2023 historical challenge freeze and pre-execution state (2026-08-05; superseded)

- The unchanged fixed ten pairs are prospectively bound to the immutable
  `2023_1min.zip` archive before any price row read. The archive is
  4,008,027,005 bytes with SHA256
  `b05e2ca0b732821edf48a065c88b402d5c173b6a6b1266e0407bef8d9a546923`,
  5,035 SH/SZ entries and no Beijing Exchange entries. This is a backward
  out-of-time historical challenge, not forward confirmation; the no-BJ
  coverage and single-year horizon cap its evidence grade at weak.
- p03 has now crossed the destructive access boundary. The access transition
  receipt is
  `runtime/run_plans/cn_historical_challenge_2023_access_started.json`,
  canonical payload SHA256
  `3943298896b10ae28a6b6a9f2a17445c2dda3d83633d3d6e09b270dd29cebcdc`.
  The 2023 challenge asset is therefore project-level `spent` regardless of
  execution success. No candidate or economic result exists yet.
- The exact 4,008,027,005-byte archive was transferred to 77o through 60
  independently hash-matched chunks and reassembled to the original SHA256
  `b05e2ca0b732821edf48a065c88b402d5c173b6a6b1266e0407bef8d9a546923`.
  The exact 242-file daily/ST source and immutable fixed-ten cohort were also
  staged. The p03 implementation keeps the unchanged `TOPK_10_EQUAL`
  decoder, next-open/T+1/final-close-MTM ledger, one
  `VALIDATION_EXCLUSIVE_32` lease and 12 single-thread worker processes;
  31 focused tests plus Python compilation and PowerShell parse passed before
  deployment.
- The separate TDX LC1 `FORWARD_B` window, 2026-04-13 through 2026-05-14,
  remains performance-unopened and is not authorized to run. Its metadata-only
  reservation binds inventory SHA256
  `f69cc84f51f9fa094431a81e2ac28d6f901040de6f78383f2228b5959a77b819`,
  5,516 eligible stock-like files / 2,190,935,104 bytes and the same fixed ten
  intended cohort. It may be considered only after independent 2023 challenge
  audit and an explicit workflow replan; no automatic release or substitution
  is permitted.
- The role registry drift is corrected: the previously opened 63-session
  2026-01-05 through 2026-04-10 asset is now explicitly `spent`, matching the
  access and burn ledgers. Historical challenge, Forward-B, spent 2026 and the
  adaptive 2025 assets remain separate evidence budgets.
- p01 was a zero-performance-read authority freeze. p02 then
  qualifies archive conversion and accelerated replay semantics on already
  spent/development data. Only after exact parity may the 2023 challenge be
  opened. The 2023 asset is now spent for the authorized p03 run. Search, OOS
  feedback, promotion and Forward-B access remain held.
- p02 conversion and parity qualification is now closed at exact pushed and
  deployed SHA `7f44713b9cf12de74e5ea9c3d6421d507ba3784b` with 16/16
  focused tests passing on 77o. A 12-process real-shape 2024 canary produced
  7,744 sessions for 32 complete-year securities; all 7,744 coordinates and
  open/high/low/close/volume values matched the official development release.
  Amount matched the frozen float-aggregation tolerance (maximum absolute
  difference 1,194; relative tolerance 1e-6). The canonical parity receipt
  file/payload SHA256 are
  `bbeb017645138e8b16a7ca32261f39e11852d4325b1774b5f700d42571cb0c4d` /
  `b764420c6b432c22e58ce87a2f80e83cead43101cb80a67b1c1f638b9f774b05`.
- The 2024 exact daily-ST builder separately closed 1,293,882 identity/ST rows
  from 242 sessions with 12 workers and no performance columns read. p02 used
  only spent/development 2024 data: 2023 challenge, Forward-B, validation,
  holdout and 2026 performance reads remain zero. p03 may now perform exactly
  one full 2023 conversion and unchanged fixed-ten report-only challenge; it
  must not add search, tune from challenge results or release Forward-B.


### Fixed-ten 2026 forward asset spent without confirmation result (2026-08-05)

- The explicitly authorized one-shot fixed-ten forward attempt opened 2026
  source rows while deriving the allowed-code universe, then failed before
  field-sidecar closure because the legacy chip loader rejected a 2026 maximum
  observable time. Under ADR 0002 the 63-session asset is now project-level
  `spent`; no retry, replacement, backfill, tuning or alternate cohort is
  authorized.
- This is an infrastructure-invalid confirmation attempt, not an economic
  rejection of the ten pairs. It produced zero candidate results, zero pair
  results and no `FORWARD_2026_COMPLETE` closure, so absolute return, matched
  increment, uncertainty and survivor counts are all unavailable. Promotion
  remains `HOLD_PROMOTION`.
- Complete evidence is preserved at
  `D:\ChengboRemote\runtime\run_health_incidents\20260805T120814_fixed10_forward_chip_guard_after_forward_access`;
  incident/preservation-manifest SHA256 values are
  `74d77425c2eb122343ee10568107c9138fdcdcb744927d03c9165166335960bc` /
  `c1488121537f9f26270c1dc387aa4a5f29edd8043cf7984986edafc3c11dc985`.
  The independent failure audit at
  `G:\Chengbo\runtime\cn_fixed10_forward_2026_independent_failure_audit_20260805`
  passed with file/payload SHA256 values
  `7f8df8270b237474de70e9b2363c7123d8c659b63535c7a0cb2d3907351841b6` /
  `f4d23b3e6f8eec318ab7b4c1c7164cb02f9175e61ad39f233e67dfe6c7f272da`.
- Commit `635ace825e6f2b1b5e884f3b0cfb9eeb4acad5b0` adds an explicit
  forward-report-only chip role and moves its compatibility check ahead of
  forward source scanning; focused synthetic and regression tests passed
  91/91. This repairs future execution safety but does not authorize reopening
  the spent 2026 asset. The compact outcome receipt is
  `runtime/run_plans/cn_fixed10_forward_2026_spent_failure_receipt.json`.

### Fixed-ten one-shot 2026 forward confirmation authorized (2026-08-05)

- The user explicitly authorized spending the sole unopened 2026 forward
  asset once for the unchanged ten-pair intention-to-treat cohort. The cohort
  remains exactly finalist orders `2/6/7/8/9/10/11/15/18/21`, selection
  payload SHA256
  `7cfc2e454da7ae7561b57979db8010324422cd87ca3eef42167809400d59ef77`;
  replacement, backfill, post-freeze filtering and tuning remain forbidden.
- The zero-financial preflight bound 63 source parquet files / 1,517,219,879
  bytes over 2026-01-05 through 2026-04-10, an exact 63-date exchange
  calendar, the existing A-share execution policy and `TOPK_10_EQUAL` decoder.
  The local preparation receipt is
  `G:\Chengbo\runtime\cn_fixed10_forward_2026_zero_read_preflight_20260805_a8fdbdb\PREPARED_ZERO_READ.json`,
  SHA256
  `9e0911ba619e5e72e2a8f67e2eb732baca7ac30ead85805196520265f18c5b9a`;
  it read zero forward financial rows and zero validation/holdout rows.
- ADR 0002 now records the one-shot destructive boundary: the asset becomes
  project-level spent on its first forward financial row read regardless of
  success or failure. No result may flow to search, optimizer, scheduler,
  archive or automatic promotion. Promotion remains held pending a separate
  explicit decision.

### Fixed ten-survivor confirmation cohort frozen; 2025 target rejected as spent (2026-08-05)

- The exact ten adaptive-validation all-four survivors are frozen in original
  finalist order `2/6/7/8/9/10/11/15/18/21`, with no replacement, backfill or
  post-freeze filtering. The immutable cohort root is
  `G:\Chengbo\runtime\cn_fixed_survivor_confirmatory_input_10_20260805_bec2de0`;
  manifest file/canonical SHA256 values are
  `98738f695614a15a83e188eb209844f75dc4e4d724e2a2cfcd762676f2c9829b` /
  `71d642f61ec79dc8d289e64180d3a305e5f511d7fdce071b7061b8bc091cf865`,
  and the selection payload SHA256 is
  `7cfc2e454da7ae7561b57979db8010324422cd87ca3eef42167809400d59ef77`.
  Independent verification passed with receipt file/payload SHA256 values
  `b7e787a29a221808bb0bb2ae01b523e63a8427da474ee59e1c9e9fb9f9fc2f23` /
  `54d225068d87fc0e801dd15fd1af1885152359f0a1aa718b4f5691380ab4e89e`.
- This freeze tests the current cohort only and creates no selector authority.
  It read no holdout market/label row and no 2026 row. The initially proposed
  48-date 2025 target was rejected during preflight because ADR 0002 and the
  burn ledger already classify the entire 2025 validation and holdout calendar
  as project-level `spent`. A different candidate cohort does not make those
  dates genuinely untouched, and no second 2025 holdout run was launched.
- At freeze time the only registered unburned time asset was the separate 2026
  forward asset. It has since been opened and classified as spent after the
  infrastructure-invalid attempt recorded in the current section above.
  Promotion remains held. Compact preflight evidence is in
  `runtime/run_plans/cn_fixed_survivor_confirmation_preflight_20260805.json`.

### CN_ALPHA_SELECTION_DIAGNOSTIC_V1 closed without ranker freeze (2026-08-05)

- The diagnostic bound the immutable 32-pair train Decoder V2 cohort, the
  exact 22-pair train finalist freeze and the already spent adaptive-validation
  OOS. It performed no financial recomputation and no new validation, holdout
  or 2026 reads. The formal root is
  `G:\Chengbo\runtime\cn_alpha_selection_diagnostic_v1_32_20260805_8b16135`;
  closure file/canonical SHA256 values are
  `212c1f6e4390594a2815b434a7cecd587d7c73c6ecf8a2f458b9c9afc5f132fa` /
  `2e0b4da81cb564be5af42d78ef4afe50f7c07414cd7feff3382eb43e60cd8177`.
  The independent audit at
  `G:\Chengbo\runtime\cn_alpha_selection_diagnostic_v1_independent_audit_20260805_dc5631e`
  passed with file/canonical SHA256 values
  `fd33de33734f8b3697e617ba9f0b3311734fbbd52e29d79fe6b1ef01e9f47101` /
  `09441435bb8099bff2075e2c9af82a39dc70b221d1f1a3655b941b17e4aadd42`.
- Labels were not fabricated. The labeled analysis universe is exactly the 22
  train-frozen pairs: 10 `ADAPTIVE_VALIDATION_SURVIVOR` and 12
  `ADAPTIVE_VALIDATION_ABSOLUTE_ONLY_RELATIVE_INCOMPLETE`. The remaining ten
  rows are `TRAIN_SCREEN_REJECT_OOS_LABEL_MISSING`; they were used only for
  train covariate-shift analysis and were never imputed as OOS negatives.
- No univariate train-visible feature survived multiplicity correction. The
  largest predeclared direction-adjusted effect was train regime stability
  (rank-biserial `0.45`, raw p `0.0805752`, BH-FDR `0.5317964`). The post-hoc
  descriptive leader, realized-PnL share, captured 7/10 survivors in a top-10
  ranking (`1.54x` enrichment, hypergeometric tail `0.0456061`), but it was not
  a preregistered ranker and creates no selector authority.
- None of the four preregistered heuristics passed the immutable freeze gate of
  at least 6/10 top-10 survivors, strict improvement over current search score
  and leave-one-out minimum 5. The conservative three-axis floor was best at
  5/10 with leave-one-out minimum 5 and Spearman `-0.013575` versus OOS matched
  return increment. Current search score, train decoder economic floor and the
  equal three-axis ranker captured 4/10, 4/10 and 3/10 respectively.
- Therefore no `VALIDATION_INFORMED_EXPERIMENTAL_RANKER` was frozen and no
  prospective selector search is authorized from this result. Promotion
  remains held. The next bounded action is to freeze the exact ten reported
  adaptive-validation survivors without backfill and evaluate that fixed
  intention-to-treat cohort once on a separately bound genuinely untouched
  confirmatory split. This tests the existing alpha cohort, not selector
  generalization.

### Frozen TOPK_10_EQUAL adaptive report-only validation OOS closure (2026-08-05)

- The train-only `TOPK_10_EQUAL` freeze is immutable at
  `D:\ChengboRemote\runtime\cn_decoder_v2_train_finalists_topk10_equal_22_20260805_e6d585d`:
  22 mechanism-distinct pairs / 44 members in original order, with no backfill
  and selection payload SHA256
  `bb01b09328c7ef6919367326ddcd5aa02b1f21c5b17db8c895079d3b42f5c8c9`.
- The first apparently closed all-zero OOS was invalidated rather than treated
  as an alpha result. Its validation authority had classified every row as ST
  after consuming an all-null field. The complete invalid output and audit are
  preserved at
  `D:\ChengboRemote\runtime\run_health_incidents\20260805T022504_decoder_oos_zero_eligible_st_authority_invalid`;
  incident/preservation-manifest SHA256 values are
  `666a37da4bc0cff69b3aed4e47d20894068487f3bc77ea57cee31c00bca39220` /
  `e18c9b3ae9f61c6490299341142530c73139b8df6bfeb5ba5ffc0982840a956d`.
  No invalid candidate result was reused.
- The corrected authority binds the immutable 2025 daily HFQ ST source at
  SHA256 `7060dd78cde6b826157f10c03541a4c302d11fe3213517236dc893393c071e68`.
  It retains 370,972 exact observed ST coordinates, conservatively excludes
  only 278 sparse source-gap coordinates, and contains 362,442 non-ST authority
  sessions. The authority root is
  `D:\ChengboRemote\runtime\cn_decoder_v2_validation_session_authority_v2_recovery2_20260805_6eb643d`;
  manifest file/payload SHA256 values are
  `2a8addb3f37762e635a1219b0cbeb7d587424da1316c2a7df377e6fb305f4d9c` /
  `76f092eaf86a3b7297196d8781f3f57cf49f3b7b70203d21c0864d46b5d6437e`.
  Independent audit SHA256 is
  `95868cd8405cc6abf65d534b34de540642f29738e6450938a34eaf28e06d1fc1`.
- The unchanged accelerated OOS then closed at
  `D:\ChengboRemote\runtime\cn_decoder_v2_topk10_equal_report_only_oos_22_recovery4_20260805_6eb643d`.
  Closure file/payload SHA256 values are
  `457c1dce5176f9f795e0827e07c8ceca6cce7f06b1939f04029ae992f617729e` /
  `5ac70d791f4aad03259a8cdbf44dfbed83eab58a0a88609cedf53ec35a6824b4`;
  independent audit SHA256 is
  `395d5aa96aee00e5581536cc4b02f5d8ec71f3088f8e52036da02182b38cc373`.
  All 44 candidates / 22 pairs, canonical self-hashes, pair recomputation,
  identities/order and accounting invariants passed. The run used 362,193
  eligible sessions and produced 31,312 fills. Validation reads were 756,720;
  holdout/2026 reads and optimizer, scheduler, archive and promotion writes
  were zero.
- Absolute transfer is strong within this adaptive validation slice: all 22
  primary candidates had positive net return and positive continuous-book net
  reward. Primary cumulative net return median/p10 were `0.090252051` /
  `0.045685622`; reward median/p10 were `2.622437181` / `1.379451569`.
  Median one-way turnover was `0.284526945`, median net return per turnover was
  `0.284322043`, median daily-return p10 was `-0.013708211`, and the worst
  observed primary daily return was `-0.032239356`.
- Relative alpha is narrower: 11/22 matched return increments were positive,
  10/22 matched reward increments were positive, and 10/22 pairs passed all
  four absolute-plus-relative reward-and-return gates. Matched return increment
  median/p10 were `0.001166406` / `-0.059396143`; matched reward increment
  median/p10 were `-0.094704456` / `-1.908352168`. Positive-return top-one and
  top-five concentration were `0.079972824` / `0.362554957`, so the absolute
  result is not carried by one outlier.
- High ending exposure is not being mistaken for a forced sale: ending holdings
  weight median was `0.963235946`, while median realized trade PnL was CNY
  `86,283.80` versus median ending unrealized PnL CNY `7,277.97`. Final-close
  holdings remain marked without a fabricated terminal sale or terminal fee.
  Cross-layer ranking remains weak: train decoder to OOS net-return Spearman was
  `0.110107284` with 2/5 top-five retention, while train search score to OOS
  return was `-0.047995483`. The result supports a small economic survivor set,
  not the current search reward as a reliable selector.
- This is `ADAPTIVE_REPORT_ONLY_VALIDATION_OOS`, not untouched confirmatory
  holdout evidence and not promotion authority. The accepted state is therefore
  positive research evidence with promotion held, not a production-alpha claim.

### Auditable trading ledger and accelerated CN_PORTFOLIO_DECODER_V2 closure (2026-08-04)

- The frozen 32-pair / 64-member train cohort first completed a final-close
  continuous-book replay with persisted FIFO lots, lot consumption, daily cash
  and PnL ledgers at
  `D:\ChengboRemote\runtime\cn_finalist_mark_to_market_replay_ledger_v1_32_20260804_6e6790d`.
  Closure file/canonical SHA256 values are
  `44acf0edc09a19bb420f19f1f761adba345df9b6a32ede8cfbf23653899d300a` /
  `7328d0593a22f0fbb790017e4ddc9efff89acef5d9007a0ff9ac899ba828016e`.
  All 266 declared artifacts, 64 candidate identities, 32 pair bindings and
  192 accounting ledgers passed independent audit. Historical NAV, reward,
  return, fees, fills and blockers had no mismatch. Maximum cash/NAV/PnL/lot
  errors were `3.259629011e-09` / `0` / `2.118758857e-08` / `0`.
  The independent audit file/canonical SHA256 values are
  `cf8ca740e010ead2e0ab37738169214f91b822d0bd73669400228afc2403678a` /
  `58662fb8cba09aece09c4d75736346819caf45b17c66314e7e96c909ce050855`.
- Decoder V2 then replayed exactly three one-session mappings over the same
  formulas, signals, candidate order, PIT data, next-open execution, T+1,
  frozen fees and final-close mark: `CURRENT_TOP20PCT_EQUAL`,
  `TOPK_10_EQUAL` and `TOPK_10_RANK`. The immutable diagnostic root is
  `D:\ChengboRemote\runtime\cn_portfolio_decoder_v2_process_full_3x1_64_20260804_7051ae6`.
  Its closure file/canonical SHA256 values are
  `216fe608130f8cd752f00c196e7c6cdd34ba1b2907d7826921ff41ff7fd0c426` /
  `173aafaeb54a29797fc0b1e4c66b8b70af2a8d13f673b5e24eeda98f93def967`.
  Independent audit matched all 71 artifacts, 64 candidate receipts, 192
  candidate metrics, 96 pair metrics, the 32-row transition matrix, all rank
  summaries and 64/64 zero-delta baseline rows. Audit file/payload SHA256 values
  are `a63c121c16b31fa053c036614900a1987683372520330a5d7bb4b0e6ceee7d7a` /
  `bc024ae854fb25393d29528a161b02558a83d9d41739ce01b2c1dd5e9b1db3b2`.
  Validation/holdout/2026 reads, search, reward/evaluator change and promotion
  writes were zero or false throughout.
- The existing top-20%-equal mapping remains economically negative: 0/32
  primaries and 0/32 pairs passed the absolute-plus-matched gates; primary net
  return median/p10 were `-0.134002815` / `-0.223560933` and matched-increment
  median was `-0.023157311`. Its signal-to-net Spearman was `-0.087243402`.
  The failure is therefore reproduced under the auditable ledger rather than
  explained by terminal liquidation or accounting drift.
- `TOPK_10_EQUAL` is the clear train-only decoder candidate. Primary net return
  median/p10 became `0.192914684` / `0.025385922`; 29/32 primaries were
  positive and 22/32 pairs passed both standalone and matched-return gates.
  Matched-return increment median/p10 were `0.112225690` / `-0.119185377`.
  Signal-to-net Spearman improved to `0.391495601`, while top-decile and top-five
  retention remained 1/4 and 1/5. Median one-way turnover and net return per
  turnover were `0.250063342` and `0.806681926`. Positive-return top-one/top-five
  concentration was only `0.081168717` / `0.327439988`; median realized trade
  PnL was CNY `164,638.58` versus ending unrealized PnL CNY `5,489.02`, so the
  cross-candidate improvement is neither a single-winner nor ending-mark
  artifact.
- `TOPK_10_RANK` also improves train economics but is weaker on the joint
  evidence: 27/32 primaries were positive and 20/32 pairs passed both gates;
  primary return median/p10 were `0.153759475` / `-0.005651324`, matched
  increment median/p10 were `0.083053336` / `-0.187456320`, median one-way
  turnover was `0.265846320` and return per turnover was `0.576390983`.
  Its signal-to-net Spearman `0.392595308` is only marginally above equal
  weighting, with the same 1/4 and 1/5 elite retention and worse economics,
  turnover efficiency and concentration. It is not the preferred treatment.
- The slow validation path was replaced only after an exact-shape parity and
  throughput qualification. The accepted 12-process, single-native-thread
  backend produced `136.5951` candidates/hour in qualification, `8.2632x` the
  historical eight-worker rate, with 32/32 exact parity. The full 3x1 run
  closed in `1781.16` seconds at `129.3535` candidates/hour; minimum free memory
  was `53,487,345,664` bytes, peak process-tree RSS was `31,104,782,336` bytes
  and saturated-compute sample fraction was `0.87638`. This qualifies the
  bounded process backend for this decoder shape without changing semantics or
  weakening the 24 GiB/8 GiB gates.
- This is development-train diagnostic evidence, not OOS alpha and not a
  decoder authority promotion. `TOPK_10_EQUAL` may advance only through a
  separately authorized deterministic freeze of the actual positive
  standalone-plus-matched, mechanism-independent subset, followed by one
  unchanged report-only OOS replay. No formula search, reward change, automatic
  promotion, ADR or CURRENT Graph transition is authorized by this closure.

### CN_PORTFOLIO_DECODER_AUTOPSY_V1 closed over frozen train evidence (2026-08-04)

- The decoder autopsy closed at
  `D:\ChengboRemote\runtime\cn_portfolio_decoder_autopsy_v1_32_20260804_3e67d1e`
  over the unchanged 32-pair / 64-member cohort with selection payload SHA256
  `24aa602dbcd19f02b2c6f773a3441fde78d1612e327b11039684764569b6e5c1`.
  The closure file/canonical SHA256 values are
  `1203101eacf63bed50736d34c921178e447b2f7fe79804b7043fdc536d6a43c9` /
  `598b56aa54e275067069fcff46254d80d3147d0e6796d55361647af7c0ab4ad0`.
  Its canonical self-hash and all 71 declared artifacts passed independent
  verification. The metric tables contain exactly 2,560 candidate rows, 1,280
  pair rows and 40 rank-preservation rows, with 64 candidates, 32 pairs, ten
  decoders, four holding-session values and zero duplicate composite keys.
- The current executable decoder canary reproduced the immutable replay
  exactly: ending NAV, fees, trade/fill counts and typed blocked-buy/sell counts
  all had zero delta. Input identity sets and pair/member bindings match the
  frozen cohort; no candidate was filtered or added. No search, reward or
  evaluator change, validation/holdout/2026 read, archive/promotion write or
  Graph authority transition occurred.
- The sidecar has one 15:00 row per stock/session. Therefore the historical
  stock-session `horizon_min=1/5/15/30` labels are session shifts here, not true
  minute clocks. The accepted report names them holding sessions and does not
  make a minute-horizon claim.
- The current top-20%-equal decoder at one session does not preserve the full
  signal ordering (signal-to-gross Spearman `-0.08431085`) but retains 2/4 of
  the signal top decile and 3/5 of the signal top five. It is also the only
  tested mapping with material positive agreement to the existing exact MTM
  ordering: gross-to-existing-MTM reward/NAV Spearman values are
  `0.37719941` / `0.43071848`. At 5/15/30 sessions its signal-to-gross
  Spearman values are `-0.05681818` / `-0.28005865` / `-0.17925220`, with
  zero top-five retention at every longer holding period.
- `TOPK_10_RANK` at one session has the highest observed signal-to-gross
  Spearman, `0.32917889`, but this is only marginal train evidence
  (`p=0.06581495`, n=32). It retains only 1/4 of the signal top decile and 1/5
  of the top five, reverses against existing MTM reward ordering
  (`-0.27089443`) and raises median target one-way turnover from the current
  decoder's `0.07144072` to `0.34337234`. It therefore does not dominate the
  current decoder and is not promoted. The other tested Top-K/rank/softmax
  mappings also failed to win full-order preservation, elite retention and
  existing-replay agreement simultaneously.
- The 1,249/1,280 positive absolute-and-matched cells are theoretical gross
  train diagnostics (next-open entry and final-close mark), not executable net
  PnL or finalist evidence. The accepted state remains `HOLD_RESEARCH`: the
  autopsy localizes a real decoder/horizon mismatch but does not authorize a
  portfolio authority change, report-only OOS or more formula search.

### CN_ALPHA_AUTOPSY_V1 closed over immutable 32-pair evidence (2026-08-03)

- The read-only autopsy closed at
  `D:\ChengboRemote\runtime\cn_alpha_autopsy_v1_32_20260803_77b7a24`
  over the exact unchanged 32-pair / 64-member Slow Temporal cohort with
  selection payload SHA256
  `24aa602dbcd19f02b2c6f773a3441fde78d1612e327b11039684764569b6e5c1`.
  Its manifest file/canonical SHA256 values are
  `8e74b2796f5645ae06093480025ab168b723350e08d94cd8b68c07d99278f704` /
  `65e596e79efbac36160d0d5c4df1405ddf268e6290a32b86df66fa430caa2eef`.
  The closure directly binds pushed builder commit
  `77b7a2409419b7990450198ba39618980de3a74a` and builder-source SHA256
  `83f2a2f247febc495b905a2f0203ea2701b47b5506172694bab37809009045d4`.
  All declared output artifacts and every bound replay, OOS and train-MTM
  source closure/artifact passed independent hash verification. The separate
  verification receipt SHA256 is
  `cb3b6ef7da760d8b5be285cb8d05c509de5e7a38ebaa8d486a83c72d8f05373a`.
- No search, financial replay, validation rerun, reward/evaluator change,
  holdout/2026 read, promotion write or Graph authority transition occurred.
  The autopsy only read existing immutable artifacts. Candidate-specific OOS
  observations were 51-68 days, so this remains weak-sample research evidence.
- Signal evidence is mixed rather than empty: 17/32 candidates were positive
  in both train and OOS RankIC, 12 decayed from positive train RankIC to
  nonpositive OOS RankIC, one was OOS-divergent and two failed in both. Overall
  OOS RankIC was positive for 18/32 candidates.
- The principal measured break is ranking preservation through portfolio
  construction. OOS signal RankIC versus gross portfolio return had Spearman
  `-0.1414956012`, with zero top-decile and zero top-5 retention. Gross versus
  net portfolio return preserved rank exactly (`1.0`, full top-set retention),
  so the modeled cost layer did not cause the cross-candidate ranking collapse.
  Net portfolio return versus the existing validation search score was also
  effectively unaligned (Spearman `-0.0953079179`, zero top-set retention).
- Search/reward alignment is weak. Development search score versus existing
  validation search score had Spearman `-0.0175953079`; train continuous-book
  absolute MTM reward versus validation search score was `-0.2613636364`, and
  train matched MTM increment versus validation search score was
  `-0.3277126100`. These are diagnostic correlations against the existing
  Phase3CM report metric, not continuous-book OOS MTM and not promotion proof.
  Economics remain poor: 0/32 primary train MTM rewards were positive, only
  5/32 matched train MTM increments were positive, while 21/32 existing
  validation search scores were positive.
- Required but unpersisted evidence is labeled
  `UNAVAILABLE_FROM_IMMUTABLE_EVIDENCE`: top-bottom spread, average/maximum
  position age, realized/unrealized PnL split, monetary counterfactual losses
  by T+1/limit/suspension/liquidity and continuous-book OOS MTM. The old MTM
  receipts also omit initial cash, so a first audit that overstated fee-addback
  reconciliation was rejected and preserved under
  `run_health_incidents\20260803T224000_alpha_autopsy_initial_cash_reconciliation_overclaim`
  (incident SHA256
  `3d3f93698e7e63cdef582429fc80188beb37b8711386015e539d691e8109430d`).
  The accepted report does not fabricate that reconciliation.
- The accepted decision remains `HOLD_RESEARCH`. This is a genuine evidence
  state update, not an architecture or authority change, so no ADR or CURRENT
  Graph transition is warranted.

### Continuous-book MTM authority and full productive-supply retest closed (2026-08-03)

- ADR 0014 supersedes the arbitrary terminal-flat interpretation and 5%
  ending-holdings veto for train economic evidence. The accepted authority is
  a continuous book valued at each daily final PIT close, with no fabricated
  terminal sale or terminal sell fee. Ending holdings remain reported risk;
  they are not an alpha veto. Identity, PIT, hash, sealed-data and A-share
  execution semantics remain hard gates, while post-financial CPU occupancy
  and free-memory observations are operational diagnostics rather than reasons
  to erase completed economics.
- The exact full 10-pair / 20-member productive supply from the closed
  primary-absolute search was retested on train under this authority at
  `D:\ChengboRemote\runtime\cn_primary_absolute_mtm_retest_10_20260803_a3ca129`.
  All 20 candidates and all 10 pairs completed. The immutable MTM closure file
  SHA256 is
  `2f91bb985697e66131ba0fb71e1f44f15fb45bc62d0c9a2b66721bb13435ff66`;
  canonical payload SHA256 is
  `6066995c6908ad53f7bc8fdce2b40413e4620a1c988092f20265e1a2a80e64d0`,
  and an independent audit matched all 29 declared artifacts and exact pair
  identities. Train reads were 37,083,060; validation/holdout/2026 reads and
  prohibited writes were zero.
- MTM removed the false terminal-liquidation blocker but did not reveal a
  matched economic winner. One primary had positive standalone MTM reward and
  one had positive standalone cumulative return, but zero pairs had a positive
  primary-minus-control MTM reward increment; only one had a positive
  cumulative-return increment. Primary reward median was -4.326080872,
  primary cumulative-return median/p10 were -0.382425271 / -0.701854040,
  matched reward-increment median was -2.083945696 and matched
  cumulative-return-increment median was -0.148460191. Ending-holdings weight
  median/p90 were 0.050336154 / 0.397601130 and remain risk diagnostics only.
- The deterministic finalist freeze therefore closed with exactly zero pairs,
  no backfill and status
  `MTM_TRAIN_ONLY_FINALISTS_CLOSED_ZERO_HOLD_RESEARCH`. Finalist manifest
  file/payload SHA256 values are
  `679a64c0a4a65034a219252156fd8394ebd6b746bba87cdb3dd25c42f9baeafe` /
  `d74036e7e648a534a46e64a07242dbc382f9ed51f5bed2a768f024dacb247d0b`;
  all five declared artifacts and the zero selected pair/candidate sets passed
  independent verification. Because no train MTM finalist exists, report-only
  OOS was not authorized or run.
- Two post-financial freezer failures exposed residual funnel-only schema
  assumptions for `economic_mechanism_id` and
  `portfolio_exposure_family_id`; neither was a financial or alpha failure.
  Evidence is preserved under the corresponding 19:07:50 and 19:39:38
  `run_health_incidents` roots. SHA
  `74baf5d71cdf846729fc6b7d4ac52e01b1e06dd5` makes empty economic outcomes
  close honestly while still requiring mechanism identity whenever any row is
  genuinely eligible; 25 related local and 77o tests passed. This is a
  post-processing robustness repair, not a promotion or search-policy change,
  so no new ADR or CURRENT Graph transition is warranted.

### Primary-absolute bounded train search closed (2026-08-03)

- The single authorized 77o train-only search closed normally at
  `D:\ChengboRemote\runtime\cn_winner_guided_large_search_primary_absolute_20260803_31ae22f_288`
  under exact pushed/deployed SHA
  `31ae22fccca7ff49f45241c385e5f3a84e18610d`. The frozen one-checkpoint
  Slow Temporal / First-N / Slow Cross-sectional / other-route schedule was
  exactly `24/256/8/0`, with 288 fresh formal asks, fresh TPE plus the existing
  Availability Controller and one `SEARCH_EXCLUSIVE_32` lease.
- The checkpoint is independently verified `BATCH_CLOSED_IMMUTABLE`.
  Batch-manifest file/canonical SHA256 values are
  `59d6794bf837b1b7e3edccc55c27168cd89d4ae4056a95f8474f15db4f632043` /
  `b58e7a5088b85a140162c768147e8e455855f5af7eb3feae76f79cd44cbcac5a`;
  all 24 checkpoint artifacts match. Root `run_manifest.json` SHA256 is
  `bf65d54daee468f5fa650b6d84daf1a152123c8dbcf122fb85e1cb2046362d1a`,
  all 17 declared root artifacts match, and the train-complete manifest SHA256
  is `47594c3f301f4488e7a068eefa51a58722ca7ea0bfce63144f30b892cab292c9`.
- Actual output was 287 evaluated pairs from 288 formal asks and 10 development
  productive candidates. Evaluated/formal, productive/formal and
  productive/evaluated yields were 99.6528%, 3.4722% and 3.4843%. Productive
  route counts were 4 Slow Temporal, 5 First-N and 1 Slow Cross-sectional;
  Market Regime and Disclosure remained zero-budget. All 10 productive rows
  map to distinct behavior families, so exact/family is 1.0; top-10 family
  concentration is 100% only because the entire productive set contains ten
  families.
- Full-train optimizer authority remained exact: 10 accepted feedback proposal
  IDs equal the eligible READY set, local COMPLETE tells also equal 10, every
  finite search score equals
  `min(primary_composite_reward, matched_train_increment)`, blocked rows have no
  score, and primary-exact, pair-ID and full-behavior-signature duplicates are
  zero. Score median/p10 across the ten accepted feedback rows were
  0.18424747 / -0.006097989; negative feedback remains valid development
  feedback and is not a productive or promotion claim.
- Runtime, acceleration and sealed-access gates passed. Active-bar and
  stock-session effective cores were 23.7888 and 28.4238 under the 32-thread
  entitlement; pair batches were 12/24, maximum evaluator cache peak was
  8,588,754,144 bytes below the fixed 8 GiB cap, minimum free memory was
  65,145,593,856 bytes, and validation/holdout/2026 reads were zero. Long-only,
  no-shorting, 5 bps and 1/5/15/30-minute contracts remained unchanged.
- Independent audit status is PASS. Its file/payload SHA256 values are
  `21956d0378fae9060afee3341cab9ea30d90798dd3955ef33738620c2f688346` /
  `705c8bfa3fc234734421061637bcd56cbac58a96800701254f2a01561d477714`.
  This bounded run creates development evidence only: none of the ten rows has
  yet passed ADR 0013 strict executable or MTM terminal-budget admission, no
  validation was run, and no promotion or successor search is authorized.
  Search, reward, execution and promotion authority did not change, so no new
  ADR or CURRENT Graph transition is warranted.

### Primary-absolute finalist admission authority (2026-08-03)

- ADR 0013 is accepted and the existing finalist execution-evidence authority
  now rejects relative-only winners. Strict replay admission requires both a
  positive standalone primary A-share executable net reward and a positive
  primary-minus-control executable increment. Blocked, nonpositive and
  duplicate-mechanism rows cannot be used as backfill.
- ADR 0014 supersedes the earlier 5% terminal-holdings veto on the separate
  final-close MTM admission path. That path still applies positive standalone
  primary and positive matched-increment economic tests, but reports terminal
  holdings as risk rather than rejecting alpha mechanically. Passing remains
  train-only evidence for a separately authorized report-only OOS run; it does
  not establish strict executability, promotion or an economic claim.
- Phase3CM development feedback remains unchanged under ADR 0010. Replay, MTM
  and OOS evidence cannot mutate TPE reward, optimizer, scheduler or archive
  state. The source authority is commit
  `b2bc6c9157e822bef75fdd66e4d60dfe6436b5bd`; 58 focused tests passed (10 new
  admission/freeze tests and 48 existing search/replay/OOS regressions) plus
  Python compilation. No 77o financial run, validation, holdout/2026 read or
  promotion is implied by this source qualification.

### Terminal-liquidity MTM diagnostic and bounded search closure (2026-08-03)

- The user-authorized shared-control continuation is closed without promotion
  or sealed-period access. The bounded `SEARCH_DUAL_24` recovery completed one
  train-only checkpoint with the exact Slow Temporal / Slow Cross-sectional /
  other-route mix `144/40/0`, 184 fresh formal asks and 184 evaluated pairs.
  Sixty-four were productive (34.7826%); all 64 productive rows had distinct
  behavior-family IDs, accepted optimizer feedback/COMPLETE tells were 57,
  exact/pair/full-signature duplicates were zero and validation/holdout/2026
  reads were zero. The batch manifest file/canonical SHA256 values are
  `a8b332d9329d1b4675a9594908308791a24c7fc58cebd4b5e0e5d6d50509bdf2` /
  `48b68f4d47e256d529f1b20ccc0939c030d8464d88bcb8681629261c427cce1c`;
  all 22 checkpoint artifacts and all 17 root artifacts passed independent
  verification. The independent audit file SHA256 is
  `639a2ea62cfe55291278d39a0e079041615514a4a8bb3e61c178701fabbe6a9d`.
- The first search attempt failed only the unchanged stock-session
  acceleration gate at 14.710138 effective cores versus 16.0. Its evidence is
  preserved under
  `run_health_incidents\20260803T095807_terminal_search_acceleration_gate_failure`;
  incident SHA256 is
  `a460a902a3c98452db57e0e803466200ec30acae8042e693ce0d9b0a67a7d436`.
  The successful same-root recovery reused neither rejected financial results
  nor optimizer state and did not weaken any gate.
- The unchanged 32-pair/64-member terminal-liquidity cohort then completed a
  separately bounded train-only final-close mark-to-market diagnostic. All 64
  candidate records and all 32 pair records completed in the original order;
  ending holdings were valued at `FINAL_PIT_CLOSE` with no fabricated terminal
  sale or terminal sale fee. The immutable closure file/canonical SHA256 values
  are
  `4eac16067a51b1b19ecc98ba35b670759aae49e191ab76819b7eb695b627b647` /
  `5273a915a9d788ec5be1af19c28caa0b8a92239c2b8cd56fe222efede9e2b4a4`;
  all 74 declared artifacts, selection/replay bindings and identity order
  passed. Validation/holdout/2026 reads and optimizer/feedback/scheduler/
  archive/promotion writes were zero or forbidden.
- Mark-to-market resolves the accounting ambiguity but does not rescue the
  cohort economically. Only 5/32 pairs had positive primary-minus-control MTM
  increment; the increment median/p10 were -0.278412966 / -0.549645061. No
  primary candidate had positive standalone MTM reward; primary reward
  median/p10 were -0.954243201 / -1.864186638. Ending-holdings weight
  median/p90/max were 0.397880801 / 0.420949810 / 0.422067092. Only two pairs
  were positive both on matched MTM increment and the already closed OOS
  transfer: `cn.pair.0c464b935d4c113953059dd9c9f4e728` and
  `cn.pair.dddacc07644bccd244895b39415342ef`. Because neither result supplies a
  positive standalone primary economic claim, both remain diagnostic
  `HOLD_RESEARCH`, not executable finalists.
- Independent MTM audit file/payload SHA256 values are
  `ae19e68a6c0117f9d42e415b9a57d63d128020a6e2117fd687929ec85b7aa99c` /
  `5ecdddc64bb110b4a24a481ef7260e7decc9395a195e7143b85190d7cdf59bbf`.
  The earlier cardinality- and replay-root-binding failures wrote zero
  candidate results and remain preserved under their incident roots; neither
  was reused. This evidence changes project conclusion but not search,
  execution, resource or promotion authority, so no ADR or CURRENT Graph
  transition is warranted.

### Continuous search plus unchanged 32-pair report-only OOS closure (2026-08-03)

- The user-authorized shared-control workflow is closed without promotion or
  holdout/2026 access. Its first bounded `SEARCH_DUAL_24` continuation closed
  1,152 fresh train-only asks with 562 evaluated pairs and 174 productive
  candidates; its second bounded continuation closed 768 asks with 521
  evaluated pairs and 135 productive candidates. All 309 productive rows have
  distinct behavior-family IDs, identity duplicates were zero and sealed reads
  were zero. The second search batch manifest file SHA256 is
  `29aaac16048800f6768d8e619fca4cc666b28ad12367fdaeae331e3073f996cc`,
  canonical payload SHA256 is
  `acb324178743a7af8bc72acc5ac468bec1538654b96c4ff4d1c624b4fb03889e`,
  all 24 artifacts passed and the root train-complete manifest SHA256 is
  `df793f16e890508bcf6e28a6b7f6cf2155b08d7e18d73dd31adddbe5a3421e69`.
- Strict A-share train replay over the unchanged 32-pair/64-member cohort
  remained fully fail-closed: all 64 candidates are typed
  `FINAL_SESSION_UNLIQUIDATED_HOLDINGS`, all 32 matched pairs are
  `PAIR_REPLAY_BLOCKED`, and the deterministic positive-executable freeze
  selected zero pairs without backfill. Replay executability is therefore zero
  and must not be inferred from OOS transfer.
- Unchanged-cohort intention-to-treat report-only OOS evaluated all 32 pairs
  without filtering. Twenty-one were positive-transfer (65.625%); validation
  pair-score median/p10/min were 0.051992185 / -0.08652519 / -0.14865653,
  leaving 11 nonpositive pairs. Median one-way turnover was 0.04871005. Every
  pair had validation regime-positive share 0.0; worst-regime day Sortino
  median/p10/min were -0.73103355 / -0.755413956 / -0.7710099. The positive
  aggregate transfer is therefore offset by unresolved strict executability
  and uniformly weak regime evidence and remains `HOLD_RESEARCH`.
- The independently verified OOS closure file SHA256 is
  `2f345630fc77a00d75e72f18b82e5c3bd940f385c418d0b8f7f923d9b6ba39e6`;
  the corrected root closure file/canonical SHA256 values are
  `fad6b2ab9955d0df52546fd44b9e53f21131b3a8dd475eda090b701065982bc3` /
  `1e4bd86a8c6ffba2a591dbf3a6518264fbd7dd2d4353bf903c9d6c558059a19c`.
  Independent audit file SHA256 is
  `fc5ecab57894771b60183c1dd3b18ea56d5b24c8cad7bbb6f007a437b6c55a1e`;
  it verified 87 declared replay/OOS/root artifacts, exact pair and candidate
  order, 12 protected source hashes, long-only next-session-open T+1 execution,
  5 bps Phase3CM cost, horizons 1/5/15/30, 381,649 validation reads, zero
  holdout/2026 reads and zero prohibited writes.
- The first OOS resume at SHA `2ed4f24` failed before label closure or OOS
  financial work because the resume branch omitted the serial Numba/eight-thread
  Polars sidecar environment. Evidence is preserved under
  `run_health_incidents\20260803T014658_closed_replay_oos_label_thread_env_missing`;
  incident file SHA256 is
  `d5da3a5e46cba510ba682ffcb9ef4e4af38b48fc9c30abb6da8baa15bdf2fd48`.
  SHA `3881132abe9598d94677eaf68f7e3ccc51277e7c` repaired the common
  sidecar boundary and passed local/77o focused tests 14/14 plus PowerShell
  parse.
- The fast repaired task exposed a separate detached-task launcher defect: it
  was started immediately and also retained a one-minute trigger. The second
  invocation reused the already complete identical Phase3CM result, so no
  second financial evaluation or train replay occurred, but it overwrote
  closure metadata. Evidence is preserved under
  `run_health_incidents\20260803T025622_oos_fast_task_scheduled_trigger_duplicate`;
  incident file/canonical SHA256 values are
  `2d9f4aa08c82698e0837148a156ddae8d103034baa9b0f31215d7a1acd6a57d1` /
  `7d20cbd0bcbd717a7d0acc4342d988c6a946c2caa06a74fe71daffab4b0fe238`.
  The local LAN launcher now registers triggerless tasks and starts them once.
  Project SHA `89eb321ed6087f989f3de8dc367ed09ecfa5fc5c` additionally refuses
  closure overwrite and records closed-replay attribution explicitly; focused
  tests passed 16/16 locally and on 77o. Root closure was then re-finalized
  with `train_replay_recomputed=false` without changing the OOS closure or any
  financial result artifact.
- This evidence does not create executable finalists, authorize promotion,
  enter validation results into search feedback, or authorize another search
  tranche. No ADR or CURRENT Graph transition is warranted because search,
  validation, promotion and resource authority boundaries are unchanged.

### Full-compute bounded successor train search closed (2026-08-02)

- The user-authorized campaign closed under the single same-root 77o recovery
  task, `lanjob_20260802_184157_de3a3b` / scheduled task
  `ChengboLanRemote_lanjob_20260802_184157_de3a3b`, at
  `D:\ChengboRemote\runtime\cn_winner_guided_large_search_full_compute_successor_20260802_1740_a81c05c_4608`.
  Exact pushed/deployed execution SHA is
  `a81c05cd9877c657a38be78d7423e2703bc76e70` in workspace
  `D:\ChengboRemote\workspace\alpha_pit_full_compute_successor_a81c05c_20260802_1720`;
  deployment manifest SHA256 is
  `d471830f4d3f84a300c434cadff71a292e4785d635680bc3fbe3f35e5bd13c5a`.
  Local/tracking SHA equality passed and local plus official 77o focused suites
  each passed 67/67 with PowerShell parse PASS.
- This is a new bounded authority, not a retry of either terminal
  `RUN_INVALID` checkpoint. The frozen budget is four checkpoints x 1,152
  formal fresh exact asks = 4,608, with exactly 1,120 Slow Temporal and 32
  First-N asks per checkpoint. It uses fresh TPE, the existing Availability
  Controller, `SEARCH_EXCLUSIVE_32`, 32 threads, active pair batch 12,
  stock-session pair batch 24, the unchanged 8 GiB cache and 24 GiB
  minimum-free-memory gate. Reward, Grammar, seed, route mix, PIT/sealed
  periods, long-only 5 bps execution and 1/5/15/30-minute horizons remain
  unchanged; validation, holdout and 2026 reads are forbidden.
- The zero-financial identity refresh verified and merged only immutable
  qualified-successor checkpoints 001-004, explicitly excluded unclosed
  checkpoint 005, and imported zero reward, optimizer or scheduler state. It
  produced 65,606 exact and 52,919 label-free behavior identities; refresh
  receipt file SHA256 is
  `e7b3543786f1485ad9228bceb9170f62deace6dbd60fade1694ee5b1ec691a1b`.
- Final-SHA zero-financial preflight PASS at
  `D:\ChengboRemote\runtime\cn_winner_guided_large_search_full_compute_successor_preflight_20260802_1730_a81c05c_4608`.
  Slow Temporal had 6,513 fresh exact identities versus 5,376 required at the
  frozen 1.20 margin; First-N had 557 versus 154 required. Total fresh exact
  was 8,655, all financial/validation/holdout/2026 reads were zero and the
  process exited zero. Supply receipt file SHA256 is
  `e5119e00058c4d0ef1dc80ea86f2edef8e72366022dede3f1445fbdf28ad91be`.
- `checkpoint_001` is independently verified `BATCH_CLOSED_IMMUTABLE`.
  Manifest file SHA256 is
  `6ab905b7836f09bbaaf9e8c01536df6c61d763ca326b34974f2ef71ad2ecb424`
  and payload SHA256 is
  `3a850160863646d975fa4d2d3ca45fe5a619a46e4d53f42a0caf7920f2ebc328`;
  all 24 artifacts, the Genesis link and exact 1,120/32 route schedule passed.
  It contains 1,152 fresh primary exact asks, 595 evaluated pairs, 180
  productive candidates and 176 accepted optimizer-feedback rows, with zero
  exact, pair or full-signature duplicates and zero sealed reads. Active-bar
  and stock-session reached 28.851 and 25.685 effective cores; minimum free
  memory was 39,154,376,704 bytes.
- The first `checkpoint_002` attempt completed financial work but remained
  unclosed because active-bar minimum free memory fell to 18,300,313,600
  bytes below the unchanged 24 GiB gate. Both acceleration gates passed at
  28.944 active-bar and 26.260 stock-session effective cores. The rejected
  checkpoint and logs are preserved at
  `D:\ChengboRemote\runtime\run_health_incidents\20260802T175843_checkpoint002_memory_headroom_gate_failure_full_compute_successor`;
  incident SHA256 is
  `e0867e3e270a64d02f031bf73e47bd9713d0811404d14955ef94ae99d28b5f7a`
  and the 57-file/1,683,339,759-byte preservation manifest SHA256 is
  `abd7511a27f80b165e6a85d008c68730ed58fc5aff4e16135a4b0167b83df243`.
  Failed financial results and optimizer state were not reused and no
  threshold was weakened.
- The original task was deleted. The one authorized same-root recovery began
  only from immutable `checkpoint_001`; regenerated `checkpoint_002` route
  schedule, asked population and candidate attempts exactly matched the
  preserved attempt by SHA256. It then closed cleanly without reusing failed
  financial results or optimizer state and without weakening any threshold.
- `checkpoint_002` through `checkpoint_004` are independently verified
  `BATCH_CLOSED_IMMUTABLE`. Their manifest file/payload SHA256 pairs are
  `f992c19c0d4e5291d31cf7f3b2af9b6e49ca72d90a55885ff1c4cfa5cf8c5155` /
  `b31a39775bdba1083b5fe65c991a434b9abc30120a55e66c9df07af8e9cfd362`,
  `89ef0248e1f7f1d0b5f3269e8afba999762c4656ec1e3de3835b9acb6d49a11a` /
  `ba10871b6f7c25ffa74957f027d906b4740ffc00bd42fe4690b2fd233990191a`,
  and
  `89cf8773e0ec9be33c076aa0b1d3cb46c999bff175525ba0be27149095342c25` /
  `94527950935d9fc2cd8b0af5568443d77ce7d7fc5c4576243a4cfbc13f60e276`.
  Every canonical payload hash, all 24 declared artifacts per checkpoint and
  every prior-manifest link passed. The root `run_manifest.json` file SHA256
  is `17422ea8db593a1b02dca75aa6b16b7c065e2be2d45db7e25b223eb8af7174ee`;
  all 18 declared root artifacts matched size/SHA256, and
  `train_complete_manifest.json` file SHA256 is
  `cc7d994b969b11adac810d7c8377e3b81b9068b1eec7de88f1f62e8abfafd132`.
- Final accepted evidence is 4,608 unique primary exact asks, 2,365 evaluated
  pairs and 731 productive candidates. Evaluated/formal, productive/formal and
  productive/evaluated yields are 51.3238%, 15.8637% and 30.9091%. Slow
  Temporal contributed 730 productive candidates from 4,480 asks; First-N
  contributed one from 128. There are zero primary-exact, pair-ID or full
  behavior-signature duplicates.
- The optimizer boundary is exact in all four checkpoints: 653 accepted
  development-feedback proposal IDs equal the `PAIR_TRAIN_FEEDBACK_READY` set,
  local completed tells equal those counts, every finite search score equals
  `min(primary_composite_reward, matched_train_increment)`, and there are zero
  missing eligible scores or scores on blocked rows. Productive labels exactly
  recompute from the frozen evaluated + positive matched increment + primary
  standalone-ready definition.
- Productive diversity remains high: 731 productive exact identities map to
  730 behavior families, exact/family is 1.00137 and top-10 family
  concentration is 1.5048%. Across evaluated rows, finite search-score median
  and p10 are 0.00471226 and -0.012041004; matched-increment median and p10 are
  -0.00080130 and -0.020993632. Among productive rows, matched-increment median
  and p10 are 0.00833778 and 0.00166249.
- Summed immutable-checkpoint wall time was 4,349.654 seconds, equivalent to
  3,813.82 formal asks, 1,957.40 evaluated pairs and 605.01 productive
  candidates per shared wall hour. All backends passed with 32-thread
  entitlement; active-bar effective cores ranged 28.536-30.637 and
  stock-session 25.685-26.474. Maximum observed evaluator cache peak was
  1,090,239,656 bytes, accepted-checkpoint minimum free memory stayed above
  39,154,376,704 bytes, and validation/holdout/2026 reads remained zero.
- This closure is train-development evidence only. It does not authorize OOS,
  promotion, automatic validation, another successor tranche or a weakened
  memory/acceleration gate. No ADR or CURRENT Graph authority transition is
  warranted because the accepted search policy and system boundaries did not
  change.

### Stock-session rank-mapping acceleration qualified (2026-08-02)

- The measured `cross_sectional_rank_mapping` bottleneck is repaired at exact
  pushed/deployed SHA
  `5fda05835ef7dd36f6b768956ba9c72f81b2f6e7`. The Phase3CM portfolio mapper
  now ranks once per candidate/session group and evaluates all four horizons
  inside the same Numba/OpenMP team. Ranking, tie handling, selection,
  rank-IC, reward and financial semantics are unchanged; pair batch 12, the
  32-thread entitlement, 8 GiB cache and all existing runtime gates remain
  unchanged. The old two-stage kernel remains available only as the exact
  qualification reference.
- Local and official 77o focused regression suites each passed 41/41. The
  deployed workspace is
  `D:\ChengboRemote\workspace\alpha_pit_mapping_accel_5fda058_20260802`;
  its HEAD equals local and tracking SHA, its three changed source Git blobs
  match the pushed commit, and deployment manifest SHA256 is
  `8ed1fbdd32275d250d3fd1d0e2dc57594931186b2b6b6a384eefd5f2dafe9f65`.
- The official zero-financial 77o qualification at
  `D:\ChengboRemote\runtime\cn_stock_session_mapping_acceleration_qualification_20260802_5fda058`
  used the representative 24-candidate x 50,352-row x 10-group x 4-horizon
  stock-session shape for 40 alternating legacy/fused repetitions. Selected
  masks and all mapping metrics were bit-exact. The fused kernel reached
  30.280 effective cores versus the unchanged 16-core gate and reduced mapping
  wall time from 0.415518 to 0.337473 seconds, a 1.23126x speedup. It also
  avoids the representative 14,502,336-byte block-wide legacy signal-rank
  surface.
- Qualification status is
  `ZERO_FINANCIAL_MAPPING_ACCELERATION_QUALIFIED`; receipt file SHA256 is
  `e3bdaee4046aac0d9c4d598cc858e8a1e20829227238bbe48a01d24d29acb420`,
  receipt canonical SHA256 is
  `bbb3ee3f1fb0f0a8c6ee5cf47d712d4d2050fcae543f2ed2507d96498f44fdef`
  and closure canonical SHA256 is
  `4acc9b5c21edb37ef1328e0ad7d583b78acf8d99d62573946751cbd1c778684f`.
  Independent recomputation matched both self-hashes, the declared artifact
  size/SHA256 and every threshold. Financial, validation, holdout and 2026
  reads and optimizer/feedback/scheduler/archive/promotion writes were zero.
- This qualification resolves the source-level performance blocker only. It
  does not retroactively close checkpoint 004, reclassify either rejected
  financial attempt, change search authority or authorize a successor search.
  No ADR or CURRENT Graph transition was required because no durable authority
  or financial contract changed.

### Qualified-successor train search terminal partial closure (2026-08-02)

- The separately authorized successor campaign at
  `D:\ChengboRemote\runtime\cn_winner_guided_large_search_qualified_successor_20260802_e0a89dd_9216`
  is terminal `RUN_INVALID` under exact pushed/deployed SHA
  `e0a89dd811ba15dcbc702d39e3c89672a9ac8a22`. Its frozen contract remained
  8 checkpoints x 1,152 train-only asks, with 1,120 Slow Temporal and 32
  First-N asks per checkpoint, fresh TPE, the existing Availability
  Controller, `SEARCH_EXCLUSIVE_32`, pair batch 12, an 8 GiB cache and the
  unchanged 24 GiB minimum-free-memory gate. Validation, holdout and 2026
  reads remained zero.
- Checkpoints 001-004 are independently verified
  `BATCH_CLOSED_IMMUTABLE`: all canonical manifest self-hashes, all 96
  declared artifact size/SHA256 values, the prior-manifest chain and the exact
  route schedule passed. They contain 4,608 formal fresh exact asks, 2,388
  `PAIR_EVALUATED` rows and 708 productive candidates. The immutable-boundary
  audit SHA256 is
  `6cef2c89301e4caeb96d89951a4e4f109a45b8b156c0633b91f54359f4e8f474`;
  checkpoint 004 manifest file SHA256 is
  `3e86b212e8f07721786d9d3e997c586f477f74bfa72ec4ee439b17e1c93b26ab`.
- The first checkpoint-005 attempt completed both financial backends but failed
  before immutable closure because stock-session
  `cross_sectional_rank_mapping` reached 14.356927 effective cores against the
  unchanged 16-core acceleration gate. Its complete evidence remains under
  `run_health_incidents\20260802T135009_checkpoint005_runtime_acceleration_gate_failure_qualified_successor`;
  incident SHA256 is
  `29b9e1823557636b654942f54616b3fa64472715a5e60945ffb7fe6cce222067`
  and the 64-file/2,993,676,419-byte preservation manifest SHA256 is
  `2f3f3ab0c3230c4f2b06503bac80b6078878effc6aa5d1a1e61e5b6785545b6e`.
- The one authorized same-root recovery regenerated checkpoint 005 without
  reusing the rejected financial result or optimizer state. Its route
  schedule, asked population, Phase3CM input binding and both candidate tables
  exactly matched the preserved first attempt by SHA256. It failed the same
  frozen phase gate again at 15.718638 effective cores. Active-bar passed at
  28.716 effective cores and 89.74% occupancy; stock-session run health passed
  at 22.168 effective cores and 69.27% occupancy, memory/cache/identity gates
  passed and sealed reads remained zero.
- Per `second_failure_policy=RUN_INVALID`, no third attempt is authorized. The
  second rejected checkpoint and logs remain preserved at
  `run_health_incidents\20260802T142513_checkpoint005_second_runtime_acceleration_gate_failure_run_invalid`;
  incident SHA256 is
  `9d026ffd288e95f6343f6ecc824a7717addf98f110c7f7347b2ded2ae1c22469`
  and the 63-file/2,992,287,603-byte preservation manifest SHA256 is
  `95100d825e0007d6be08c552218f6f6068f0e341f812d5fcc6dda371127ed39a`.
  Root `RUN_INVALID.json` file SHA256 is
  `371debc9a2739093ce0ab3f2d55b5ceaaf8cf80dea24d8e6a80dfe348024ce43`.
  Failed checkpoint-005 financial/optimizer state is non-authoritative; the
  campaign has no root closure and authorizes neither validation/OOS nor a
  successor search.

### Compute-efficient bounded train search terminal partial closure (2026-08-02)

- The separately authorized compute-efficient train-only campaign at
  `D:\ChengboRemote\runtime\cn_winner_guided_large_search_compute_efficient_20260801_2110_d85ff7d_12288`
  is terminal `RUN_INVALID` under exact pushed/deployed SHA
  `d85ff7d6a98afc2fb0fde23608c747d134605020`. Its frozen contract remained
  8 checkpoints x 1,536 asks, with 1,504 Slow Temporal and 32 First-N asks per
  checkpoint, fresh TPE, the existing Availability Controller,
  `SEARCH_EXCLUSIVE_32`, pair batch 12, an 8 GiB cache and the unchanged
  24 GiB minimum-free-memory gate. The final-SHA zero-financial preflight had
  17,871 fresh exact identities and zero financial, validation, holdout or 2026
  reads.
- Checkpoints 001-003 are independently verified
  `BATCH_CLOSED_IMMUTABLE`. Their manifest file/payload SHA256 pairs are
  `4d9ccad9dd437a4685a914b43b1da3a8fc896b753d8c1f1c9daec49fd7f715eb` /
  `99052039589e03112fbe9c3e64c2cc9b7de4db91fae9f7bf9fd99e5a869b248a`,
  `88512c816682942f1cdfecce7dd0c033811a4a9cb714185244faf0a393be9f9f` /
  `de6a06ea9b59b1b346194a7755b2fb2f0343b3972788724e4b7f0cdb539ce471`
  and
  `de4f74ca7b5181a19ed486fe48027fc205a4a0b2c1dc6ec2504cde068ffe0061` /
  `92e1a91431055d6a3d2038e69d5a4cd1504cf3ca6eb6ac02a5995f651a23474f`.
  Every canonical self-hash, all 72 declared artifacts and the prior-manifest
  chain passed. These accepted checkpoints contain 4,608 unique primary exact
  asks, 2,361 evaluated pairs, 731 productive candidates and 635 accepted
  optimizer-feedback/COMPLETE trials, with exact ask/observation/transcript and
  score-formula parity, zero primary exact/pair/full-signature duplicates and
  zero validation/holdout/2026 reads. The 731 productive behavior-family IDs
  are unique and top-ten concentration is 1.368%.
- The first checkpoint-004 attempt completed financial work but failed before
  immutable closure on the unchanged stock-session acceleration gate. Its
  rejected checkpoint, optimizer state and logs remain preserved at
  `run_health_incidents\20260801T225452_checkpoint004_runtime_acceleration_gate_failure_compute_efficient`;
  incident SHA256 is
  `30521413afeb398716386a753cb17cf9e2e7f764c61dd0a9ceef045bd8c6f3cc`
  and the 56-file/3,899,964,169-byte preservation manifest SHA256 is
  `a982e37ccc4f6cfdf6716ba99ab622b5b07a913e66b450e3eaa27a4f187b4689`.
  The same-root recovery regenerated checkpoint 004 from verified checkpoint
  003 without reusing financial results or optimizer state, but failed the same
  frozen gate again. Active-bar passed at 25.350 effective cores/79.22%
  occupancy; stock-session run health passed at 21.203 effective cores/66.26%
  occupancy, but `cross_sectional_rank_mapping` reached only about 14.855
  effective cores and remained below the phase acceleration threshold.
- Per the predeclared `second_failure_policy=RUN_INVALID`, no third attempt is
  authorized. The second rejected checkpoint and logs are preserved at
  `run_health_incidents\20260802T000003_checkpoint004_second_runtime_acceleration_gate_failure_run_invalid`;
  incident SHA256 is
  `e261757c3aeb7d2ee3057de4cc13f45c6617dba693305dc911805a3d14fb5a9e`
  and the 56-file/3,900,005,058-byte preservation manifest SHA256 is
  `837ca415d1d30facece8d3f31eace2cea11a067a037f841f490659691bad9f7c`.
  The threshold was not weakened, failed financial/optimizer state remains
  non-authoritative, the scheduled task was deleted and the resource lease was
  released. Only checkpoints 001-003 are accepted evidence; the campaign has
  no root closure and authorizes neither validation/OOS nor a successor search.

### Train-only finalist funnel closure (2026-08-01)

- The deterministic finalist funnel over the closed 2,261-candidate search is
  complete without validation, holdout or 2026 access. The independently
  verified 256-pair review pool remains at
  `D:\ChengboRemote\runtime\cn_train_only_finalist_review_256_20260801_ea4ca47`;
  its manifest SHA256 is
  `a19eaa8c7f6516c8691e0e8133b7efbff894757c4ed5afe316e7a1fc4a094030`.
  The derived strict replay input is exactly 64 pairs/128 members with 64
  unique economic mechanisms at
  `D:\ChengboRemote\runtime\cn_finalist_train_replay_input_64_20260801_e05a622`;
  its manifest SHA256 is
  `f5c5ac396dffdf0a0172b39f7fd777acddb596158802508e0c12dd4704874379`.
- The original single 77o task `lanjob_20260801_150436_b321cb` completed at
  17:30 HKT with exit code zero. Strict train replay closure file SHA256 is
  `cfa8ad50a6ee4950968a43744fe2d222d13be2e725ecb907118d9c1c5d22e32f`;
  canonical body SHA256 is
  `e6cf708cae8bbbde260e797f2dae89b2f4c14df4aff3912a4c9636b7e5180f6c`,
  all 137 declared artifacts and all protected source hashes matched, and the
  original 128 candidate/64 pair identities remained exact. The run stopped at
  `TRAIN_REPLAY_ONLY_CLOSED`; no validation or OOS directory was created.
- Replay completed for 32/128 candidate members and 2/64 pairs. Candidate-level
  blockers were 89 `FINAL_SESSION_UNLIQUIDATED_HOLDINGS`, six
  `CORPORATE_ACTION_FRACTIONAL_SHARES` and one `NO_EXECUTABLE_FILLS`. Exactly
  one replay-complete pair had positive executable train increment. This is
  fail-closed A-share executability evidence, not a runtime failure.
- The train-only finalist freeze was generated and independently verified on
  77o at exact pushed/deployed SHA
  `2c21424cd16558ab5959e8328e9510e9db69d484`. Deployment manifest SHA256 is
  `a5795e6d3db62cc0bc6611e5c5d2af3b60676f84e8697bada035dd1d5bc46ca1`;
  local and 77o focused tests passed 11/11. The immutable finalist root is
  `D:\ChengboRemote\runtime\cn_train_replay_finalists_20260801_2c21424`;
  manifest file SHA256 is
  `0b1ab7e0d9017b689a891494b8a1173ac69b979764ab0852c1b76f97dd65d9da`,
  manifest payload SHA256 is
  `11ef67e0d8bf903193e6ad8c0ac4a79434b4fe2e7a7a10ab0300a2b7126ade6d`
  and selection payload SHA256 is
  `a247b3e6fe457d8e756e26def506ce9608efb9621085482823a48b19a30e57ea`.
- Per the frozen insufficient-supply rule, the finalist set contains the actual
  smaller count of one pair and does not backfill blocked or nonpositive rows:
  `cn.pair.7475c3bdaf4e501835a2a6301e937616`, primary
  `cn.comp.97a36b0cd65b4e1f1c2e`, economic mechanism
  `cn.economic_mechanism.1087d7be91d08a9e9cd8332c43a54cb9`. Its executable
  train increment is `0.2010825771` (primary `0.6353401846`, control
  `0.4342576076`). Independent verification receipt file SHA256 is
  `8d61286c64ee80076f735fbfdac4cf1a75a18b76f46ed5eba8d15c0fb3735159`.
  Financial results were not recomputed; validation/holdout/2026 reads and
  optimizer, scheduler, archive, promotion and successor-search writes remain
  zero or forbidden.

### Single executable finalist report-only OOS closure (2026-08-01)

- The separately authorized single-pair report-only OOS completed on 77o at
  exact pushed/deployed SHA
  `9d365b0b9216e9c8892653b1f3656844bf3759e0` under scheduled task
  `ChengboLanRemote_lanjob_20260801_200219_f970a5`. The task exited zero and
  released its `VALIDATION_DUAL_8` lease. The output root is
  `D:\ChengboRemote\runtime\cn_single_finalist_report_only_oos_20260801_9d365b0`.
- Root status is `REPLAY_THEN_OOS_COMPLETE_IMMUTABLE_REPORT_ONLY`; the root
  closure file SHA256 is
  `b6f20f80d179ee086bae2ec049febe4efc05db3e4fbf6d687922e738585fc0fc`
  with canonical body SHA256
  `c288b3d78b445e286ba804d526e0c930bc78f12b9f0c567538ef04e4655889f4`.
  OOS closure file SHA256 is
  `de3a25d6d3f04275e6415d43ea0a137243902dbd1e801aa0a43389b4eb0e041c`
  with canonical body SHA256
  `65869c2699e95bff1d03759206324da88e19421b3e34661bd12a857ef4096817`.
  Independent verification matched all five root artifacts and all nine OOS
  artifacts by size and SHA256.
- The unchanged pair `cn.pair.7475c3bdaf4e501835a2a6301e937616`
  evaluated without an OOS blocker but did not transfer positively. Its strict
  executable train increment remained `0.2010825771`, while validation score
  was `-16.46010423`; mean one-way turnover was `0.04631402`, regime-positive
  share was zero, regime worst-day Sortino was `-0.4628121` and worst-horizon
  day Sortino was `0.34182571`. This is negative report-only evidence, not a
  promotion or economic claim.
- Train replay was not recomputed, protected source hashes were unchanged,
  validation reads were 381,649, and holdout/2026 reads plus optimizer,
  feedback, scheduler, archive and promotion writes were zero or forbidden.
  The backend used only about 1.65 effective cores on average for this one-pair
  workload, so increasing validation threads would not have improved useful
  throughput; the durable compute correction is to keep independent train work
  on the search lane whenever it is authorized.

### Shared-control dual-lane closure (2026-08-01)

- The accepted shared-node control plane completed one isolated dual-lane cycle
  without CPU oversubscription or cross-lane feedback. The fixed validation lane
  closed first and was deleted; the search lane then retained the single
  `SEARCH_DUAL_24` lease until exit. No third task, persistent inspector,
  platform/database or holdout/2026 access was introduced.
- The fixed 32-pair/64-member validation cohort at
  `D:\ChengboRemote\runtime\cn_finalist_replay_then_oos_shared_dual_32_20260801_0050_840001a`
  is independently closed and must not be rerun. Root closure file SHA256 is
  `5945805de222e05bf61463f4780ee6cdd644b4deb255efc281fb52bb30ea4850`;
  replay/OOS self-hashes and all 87 declared artifacts passed, pair identity
  and order were unchanged and no interstage filtering occurred. Replay
  completed for 27/64 candidates and 6/32 pairs, with one positive executable
  train increment. Report-only OOS evaluated all 32 pairs and found 20 positive
  transfers (62.5%); validation score median was `0.06184129` and p10 was
  `-3.005524837`. Validation reads were 381,649; holdout/2026 reads and
  optimizer, feedback, scheduler, archive and promotion writes were zero, and
  protected source hashes were unchanged. This remains `HOLD_RESEARCH`, not
  promotion or an economic claim.
- The replacement shared-control search closed on 77o at exact execution SHA
  `b6c4efaed0a04453a8891241e278d0dfdff75e62` under task
  `lanjob_20260801_0425_dualsearch` and campaign root
  `D:\ChengboRemote\runtime\cn_winner_guided_large_search_shared_dual_recovery_20260801_0110_1974c3a_9216`.
  The task exited zero after eight immutable checkpoints and exactly 9,216
  train-only formal asks with the fixed per-checkpoint Slow Temporal/First-N
  mix 1,120/32. Fresh TPE, the existing Availability Controller and Phase3CM
  were used; failed or cross-campaign reward/optimizer state was not imported.
- Every checkpoint canonical hash, all 192 checkpoint-declared artifacts and
  the prior-manifest chain independently passed. The root `run_manifest.json`
  SHA256 is
  `e440206b2ed87c015b345c563a2d402f863909686a0de0c4d71be095ffd9b1b9`,
  `train_complete_manifest.json` SHA256 is
  `d49c5a4643239513f9bdc2a1bbb8f3d49ea7e42c8a99e172cc6c686e2546c61f`,
  and all 17 root-declared artifacts matched. There were 5,860 evaluated pairs
  and 2,261 productive candidates; all 9,216 primary exact identities, 5,860
  pair IDs and 5,860 full behavior signatures were unique. Ask/observation/
  transcript coverage, development-only feedback, accepted score joins and
  `min(primary_composite_reward, matched_train_increment)` calculations all
  matched. Runtime gates passed at the 24-thread entitlement, pair batch stayed
  at or below 12, cache stayed below 2.28 GB, minimum free memory exceeded
  64.29 GB and validation/holdout/2026 reads were zero.
- Slow Temporal supplied 5,604 evaluated and 2,252 productive candidates from
  8,960 formal asks; First-N supplied 256 evaluated and nine productive from
  256 asks. Overall evaluated/formal yield was 63.59% and productive/formal
  yield was 24.53%. The derived search-score median was `-0.00120398` and p10
  was `-0.122367934`; First-N remained materially weaker with median
  `-0.41627747` and p10 `-0.591738175`. All 2,261 productive candidates had
  distinct recorded behavior-family IDs and the top-ten family concentration
  was 0.4423%, but productive yield fell from 48.52% at checkpoint 003 to
  14.15% at checkpoint 008. This closes candidate-pool expansion and does not
  authorize another tranche.
- The predecessor at SHA `1974c3a55e9f03b54c51a8ea21244eaa80076ef0`
  failed after checkpoint-001 financial work but before immutable closure
  because a campaign-level resource binding escaped the checkpoint-local
  artifact namespace. Its full failed root remains preserved at
  `run_health_incidents\20260801T020946_checkpoint001_manifest_campaign_input_escape`;
  incident SHA256 is
  `9c8fe73229c4803f62835ead191e935996fe39840aaa88e5fff4cc208df05d73`.
  No failed financial result or optimizer state was reused; SHA `b6c4efa`
  changed only the manifest binding boundary.
- The compact joint closure receipt is
  `runtime/run_plans/cn_shared_control_dual_lane_alpha_20260801_receipt.json`;
  its SHA256 is
  `0ff7475148cdc08b8c1adfe6ec3ef57f11731f5425d959a0475051fb05d3fe32`.

### Superseded pre-control dual-lane evidence (historical)

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
  Local and official 77o focused tests again passed 22/22. The final
  recovery task was `lanjob_20260731_153336_a7d457`, with deployment manifest
  SHA256
  `299cd2603431a503ba8c5ed1841522f639ad0026c22c51c417a41c5b51b3f81f`.
  Its train replay is independently verified
  `A_SHARE_REPLAY_CLOSED_IMMUTABLE`: closure file SHA256
  `ee4e9ff83dd09115dec58f4d5d96bef4d1a6be8791bee7915bab348022af02db`,
  canonical body SHA256
  `1935bbd557cbc5714242795c87fedff804e8839a5eec0d5da0f4aa53ce555ca7`,
  and all 135 declared artifacts matched. All 126 candidates and 63 pairs
  were unique and complete as a cohort. Fifty-eight candidate members
  completed and 68 were blocked; 14/63 pairs were replay-complete and 49
  blocked. Seven replay-complete pairs had positive executable train
  increment, with median increment `-0.0020958822`. Validation, holdout and
  2026 reads were zero.
- The unchanged 63-pair report-only OOS then closed successfully and the task
  exited zero. OOS status is `REPORT_ONLY_OOS_CLOSED_IMMUTABLE`; closure file
  SHA256 is
  `de1694e157336dcbf5fafe32bc057dee3fe1907fe084894026cc506bcaff54ea`,
  canonical body SHA256 is
  `43dafc7f45bab3aafba840434a0ae947f1551392b35fc6362195393dc34aa483`,
  and all 9 declared artifacts matched. All 63 fixed pairs were evaluated with
  no interstage filtering: 35/63 had positive OOS transfer (55.56%), validation
  score median was `0.03418275` and p10 was `-9.276330014`. Slow Temporal had
  26/39 positive OOS transfers but zero replay-complete pairs; Slow
  Cross-sectional had 9/24 positive OOS transfers, 14 replay-complete pairs and
  seven positive executable train increments. Only one pair was positive on
  both strict executable train increment and OOS transfer:
  `cn.pair.5e25230a83e937a50805fe63712d5cb9`, with train increment
  `0.0654307020` and validation score `0.56012896`. It remains research
  evidence, not promotion authority.
- Root status is `REPLAY_THEN_OOS_COMPLETE_IMMUTABLE_REPORT_ONLY`; closure file
  SHA256 is
  `c6e8a349255683cb3e6010448049b625c14bafdab93f66c48be6e060c2b73521`,
  canonical body SHA256 is
  `9c729a489130894d9137e1687ea7837429d23f44bf43f0f3d01c9febc096f3b4`,
  and all 5 declared artifacts matched. Protected source hashes were unchanged,
  validation reads were 381,649, holdout/2026 reads and prohibited writes were
  zero, and promotion/economic claims remain false. No search or validation
  task remains active; the invalid search and failed recovery evidence remain
  preserved.
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

## Shared-node alpha compute control-plane hardening (2026-07-31)

- ADR 0012 accepts a common 77o resource authority for future heavy alpha work.
  It separates resource topology from search semantics and permits only one
  32-thread exclusive search or one explicitly authorized 24-thread search plus
  one 8-thread validation lane. Profile changes are allowed only after a closed
  immutable checkpoint; existing `RUN_INVALID` evidence is not reclassified.
- The node governor uses one host-bound, self-hashed capacity manifest, an
  atomic file-locked lease state and content-addressed receipts. Admission
  accounts for CPU, declared memory and the 24 GiB reserve. Dead launchers do
  not release claims while workload-bound children survive, released receipts
  fail validation, and a bounded status command replaces persistent resource
  inspectors.
- Future runtime acceleration gates are entitlement-aware: full-host tasks
  retain the host-occupancy requirement, while partial tasks are measured
  against the CPU threads actually leased. Nested native parallelism remains
  forbidden and the fixed 8 GiB evaluator cache is unchanged.
- Finalist replay now persists heterogeneous candidate outcomes through an
  explicit scalar Parquet envelope plus canonical payload JSON/SHA256. Typed
  terminal-liquidity and corporate-action outcomes remain candidate-local;
  unknown authority, data, schema or execution errors remain batch-fatal.
- A zero-financial stock-session capability builder records registry coverage,
  shard schema coverage and within-session variation. Cohort freezing checks
  actual primary/control expressions, so incompatible candidates are identified
  before financial replay without route-wide over-exclusion.
- Local source verification passed 98 search/replay/control-plane tests with two
  expected Optuna experimental warnings. The exact versioned 77o deployment at
  SHA `5717cbe358aa5abff3c06067798c6849439c1a4e` passed 99/99 focused tests and
  both PowerShell launcher parse checks. Its workspace is
  `D:\ChengboRemote\workspace\alpha_pit_compute_control_5717cbe_20260731_2205`;
  deployment manifest SHA256 is
  `86808f55757f3fca830881431317117867115612acd9399a2ad3a91a25b2268a`.
- A separate zero-financial 77o qualification admitted the frozen
  `SEARCH_DUAL_24` plus `VALIDATION_DUAL_8` profiles concurrently at exactly two
  leases / 32 threads, rejected a simultaneous `SEARCH_EXCLUSIVE_32` request
  with nonzero exit, then released to zero leases / zero threads. The closure
  status is `PASS_ZERO_FINANCIAL`, closure-file SHA256 is
  `472adde938f19befb5bb23062713ed669d307e710633a212c1a0b2187380bf5b`,
  and the observed host matched 32 logical CPUs / 100,265,193,472 physical
  memory bytes. Financial, validation, holdout and 2026 reads and prohibited
  writes were all zero. The final bounded process check found no Chengbo Python
  process or running Chengbo scheduled task.
- No financial search, replay, OOS, holdout/2026 access, optimizer import,
  promotion or platform/database creation was performed by this hardening and
  deployment phase. Direct GitHub clone on 77o remained blocked by its stale
  inherited `127.0.0.1:7890` proxy, so the exact pushed commit was deployed via
  a SHA-verified full Git bundle without changing the node's proxy authority.

## Next action

Do not rerun Phase A, the materialization preflight, the uniform Phase B pilot,
Phase C, the allocator repair canary, Phase D or any preserved recovery or
diagnostic incident. Phase D is closed and the revised allocator is held: its
absolute-development and blocker improvements did not offset negative
matched-return and turnover-efficiency deltas or insufficient per-template
improvement breadth. Do not promote it, replace formal search policy, launch an
automatic successor or reuse Phase C/canary/diagnostic/incomplete financial
records. Any later experiment requires separate authorization and a fresh
prospective freeze that changes the allocator objective, not these accepted
records, to address matched-return downside, turnover efficiency and template
heterogeneity. Validation/holdout/spent-2023/Forward-B/2026 remain sealed.

Do not rebuild the registry, route compiler, DAG/evaluator, portfolio decoder or
fixed-stratified scheduler around Candidate Program V1. The qualified V1 layer
is non-formal and zero-financial. Any later program search requires a separate
bounded authorization, keeps all eight template budgets fixed without shared
TPE credit, and must first satisfy the same PIT, joint-clock, matched-control,
identity and materialization gates. It grants no permission to open Forward-B,
rerun historical challenge evidence or promote a candidate.

The first fixed-stratified V0 production-rate canary is closed and must not be
rerun. Do not dynamically reweight the eight strata from this one tranche:
Intraday State Transition and Broad Event still have zero materialized coverage,
so their recorded zero rates are structural unknowns rather than economic
productivity. If separately authorized, first repair and independently qualify
materialization coverage for those two templates without changing semantics;
then run one prospective fixed-stratified cohort with the same no-credit,
no-spillover policy. Only comparable multi-template production evidence from a
future frozen cohort may support freezing a dynamic budget allocator. Do not
adapt or rescore the completed cohort.

The frozen 22-pair `TOPK_10_EQUAL` adaptive report-only validation OOS is closed
and must not be rerun. It produced 10 pairs that passed all four absolute and
matched reward/return gates, but the same validation evidence used to report
them cannot now select and revalidate them without adaptation bias. Do not feed
this OOS result into search, TPE, reward, scheduler or archive state and do not
promote the decoder or candidates from this evidence alone. If separately
authorized, the next bounded action is to freeze the ten reported survivors
exactly as a research cohort and test them once on a genuinely untouched
confirmatory split or forward shadow portfolio, with no backfill or post-read
tuning. Until that evidence exists, retain
`CN_DECODER_V2_TOPK10_EQUAL_ADAPTIVE_OOS_POSITIVE_HOLD_PROMOTION`.

The continuous-book MTM retest over the complete ten-pair productive supply is
closed and must not be rerun. It resolved the accounting objection directly:
forced terminal liquidation and the 5% ending-book veto were removed, yet zero
pairs retained both positive standalone primary economics and positive matched
economics. Do not backfill, reinterpret OOS-positive historical rows as train
winners, or launch report-only OOS for an empty finalist set. The project is
`HOLD_RESEARCH`; any next search must be separately designed around improving
standalone primary economic quality rather than generating more relative-only
productives under the unchanged development reward.

The terminal-liquidity classification and final-close MTM diagnostic are now
closed and must not be rerun. Preserve the 184-ask search closure and the
64-member MTM closure as immutable evidence. The diagnostic rejects the idea
that forced terminal liquidation alone hid a broad executable alpha set: only
5/32 matched increments were positive, none of the 32 primary candidates had
positive standalone MTM reward and only two matched the already closed OOS
positive-transfer set. Do not promote those two, backfill the zero-finalist
freeze, feed OOS into the optimizer, weaken flat-book/T+1 rules or launch a
successor automatically. Any later compute contract must first make a separate
explicit decision about a primary-absolute-positive train admission criterion
and terminal exposure budget; it must not treat control-relative positivity as
an economic claim.

The full-compute successor is closed and must not be rerun. Preserve its 731
productive train-development candidates as immutable input to the existing
behavior-family/mechanism deduplication and strict A-share train-replay funnel;
any new bounded finalist freeze requires separate authorization and must not
backfill blocked or nonpositive rows. Do not automatically launch validation,
OOS, another successor tranche or a 20,000-target search. Before any later
large search, qualify checkpoint-boundary process recycling and memory-owner
attribution under the existing 24 GiB reserve rather than weakening the gate.

The shared-control validation, 9,216-ask search, bounded train-only finalist
funnel and separately authorized one-pair report-only OOS are closed and must
not be rerun. The sole strict-train-positive pair was OOS-negative and remains
`HOLD_RESEARCH`; blocked or nonpositive rows must not be backfilled, and the
OOS result must not enter search reward, optimizer or scheduler state.

The separately authorized compute-efficient train campaign is terminal
`RUN_INVALID` after the same frozen checkpoint-004 stock-session acceleration
gate failed twice. Do not retry checkpoint 004, launch a third attempt or treat
either rejected checkpoint's financial/optimizer state as evidence. Retain only
the independently closed checkpoints 001-003: 4,608 formal asks, 2,361
evaluated pairs and 731 productive candidates with zero sealed reads.

The separately authorized qualified-successor train search is now terminal
`RUN_INVALID` after checkpoint 005 failed the same frozen stock-session mapping
gate twice. Do not retry checkpoint 005, launch a third attempt or treat either
rejected checkpoint's financial/optimizer state as evidence. Retain only the
independently closed checkpoints 001-004: 4,608 formal asks, 2,388 evaluated
pairs and 708 productive candidates with zero sealed reads. This partial train
evidence does not authorize validation/OOS, promotion or another search
contract.

Retain the already-closed 32-pair validation only as route/evidence calibration:
20/32 were OOS-positive, but only 6/32 were replay-complete and one had a
positive strict executable train increment. That historical OOS must not be
joined adaptively to any new search or selector.

The new train budget and route mix must be bounded by refreshed post-archive
exact supply and the observed late-checkpoint yield collapse. It must not be a
blind replay of the closed global candidate race: preserve useful winner-guided
lanes only where supply remains material, and direct any additional coverage
toward mechanisms not already saturated by the 2,261-candidate pool. Generate
and bind a candidate-level execution-clock capability manifest before freezing
any later replay cohort.

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
