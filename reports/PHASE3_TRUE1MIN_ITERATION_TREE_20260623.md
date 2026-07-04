# Phase3 True1min Iteration Tree 2026-06-23

Decision: `ITERATION_TREE_INDEX_READY`

This document is the external-facing index for the true1min research chain. It
explains why the project moved from field/primitive safety audits to guarded
search, why proxy-driven search was rejected, and why Phase3CM train portfolio
Sortino is now the next reward target.

## Current Boundary

```text
Official X0/R3:
  read-only
  not modified
  no production promotion from this repository

Data backbone:
  true trade_time 1min shards only
  no old 1D kline backbone for true1min claims

Search status:
  no accepted alpha proof
  no deployable candidate
  Phase3EK restates ban-short verification: 1024 was a broad recheck pool, not historical alpha count
  true1min followup candidates require expression_hash-exact long-only verification
  current active route must use CN long-only reward, not long-short spread
  Phase3DU is retired as current route
```

## Forward Iteration Tree

```text
Phase3CN  Reward Feedback Wiring
  - CN0: candidate schema unification
  - CN1: CM reward table standardization
  - CN2: BS/BT/BU consume feedback_table
  - CN3: CEM/UCB feedback guard tests
  - CN4: integrated smoke

Phase3CO  Multi-Arm Scheduler
  - CO0: arm definition and budget table
  - CO1: family_id / motif_id / cluster_id unification
  - CO2: arm health calculation
  - CO3: family-level kill / freeze / downweight
  - CO4: scheduler smoke
  - status: implemented, no search started

Phase3CP  Reward-Gated Medium Search
  - CP0: 5-arm controlled generation
  - CP1: CA bridge top queue
  - CP2: CM train_reward audit
  - CP3: validation kill-line
  - CP4: family memory writeback
  - CP5: next-budget decision
  - status: real-CM small/balanced loops passed wiring, produced zero followup families

Phase3CQ  Efficient Large Search Restart
  - CQ0: fastpath throughput check
  - CQ1: rolling checkpoint search
  - CQ2: adaptive budget allocation
  - CQ3: MAP-Elites-lite archive
  - CQ4: full CM / BZ diagnostic / holdout report
  - CQ5: promotion queue, not deployment

Phase3CR  Challenger Algorithms
  - CR0: MCTS-M0 no-neural local repair
  - CR1: archive-prior MCTS
  - CR2: policy/value supervised warm-start
  - CR3: GFlowNet / novelty branch
  - CR4: same-budget challenger decision

Phase3CS  Proof Escalation
  - CS0: full-shard CM reward
  - CS1: non-gap replay
  - CS2: new-vs-memory / crowding audit
  - CS3: regime / event / turnover stress
  - CS4: shadow-forward pack
  - CS5: candidate book decision

Phase3DV  Budget-Pool Self-Deepen
  - DV0: retire rigid Phase3DU deepen/freeze route
  - DV1: classify train evidence into global_survivor / regime_specialist / probation
  - DV2: keep fresh self-deepen lane as novelty reserve
  - DV3: audit candidates through Phase3CM train composite reward
  - DV4: allocate next budget by lane/family, not by one-shot perfect-candidate filtering
  - status: superseded for reward-policy purposes by Phase3EK ban-short verification

Phase3EI / Phase3EK  CN Ban-Short Verification
  - EI0: scan historical Phase3CM/BZ/EG/EH artifacts for short-leg or missing long-only policy
  - EI1: build 1024-candidate broad candidate/proxy/followup recheck pack
  - EI2: recheck on DESKTOP-77OPJ6F with portfolio_mode=long_only_top and short_allowed=false
  - EK0: separate mature daily official alpha-level assets from true1min research followups
  - EK1: verify true1min followups by expression_hash exact match, not candidate_id alone
  - EJ1: patch Phase3CM default portfolio mode to long_only_top
  - status: mature daily official shadow not invalidated at daily-proxy level; current true1min has no deployable alpha-level product
```

Priority:

```text
CN must be completed first.
CO follows CN.
CP is the first real medium search.
CQ is the efficient large search restart.
CR enters only after CN/CO/CP are stable.
CS is proof escalation and must not feed search optimization.
```

## Historical Evidence Path

```text
Phase3CD
  AST primitive / field-assumption audit
  -> found unsafe limit/event motif use cases
  -> produced typed primitive registry/spec and rewrite queue

Phase3CE
  unsafe motif quarantine audit
  -> official registry scan showed no unsafe motif hit in scanned baseline roots
  -> found multiple unsafe entry points outside a single Phase3R path

Phase3CE1
  typed gate and search-memory blocked view
  -> unsafe structures are blocked/reclassified instead of deleted
  -> G2 input gate smoke proves candidate-level enforcement path

Phase3CE2
  typed primitive and fullwidth true1min canary
  -> validates evaluator availability and real-panel field readiness
  -> still diagnostic; not alpha proof

Phase3CF
  reward-gated large-search prelaunch
  -> freezes true1min-only backbone and guarded launch constraints
  -> later superseded in reward target by Phase3CM

Phase3CG
  true1min fastpath preparation
  -> packages fastpath run assumptions and acceleration constraints
  -> keeps old 1D paths out of the chain

Phase3CL
  proxy top queue fragment replay audit
  -> 32/32 replayed candidates failed
  -> proxy IC / CEM winners were not tradable reward proof
  -> fragment replay exposed failures but was also judged unsuitable as primary reward

Phase3CM
  train portfolio Sortino reward chain
  -> demotes fragment_sortino to diagnostic-only
  -> introduces train / validation / holdout portfolio reward audit
  -> next search must optimize train_reward, not proxy IC or fragment slices

Phase3CN
  reward feedback wiring
  -> turns CM train_reward into search feedback memory
  -> standardizes candidate schema and family feedback tables
  -> proves searchers safely consume feedback under sparse-evidence guard
  -> integrated contract smoke validates search output -> CA -> CM fixture -> CN -> guard

Phase3CO
  multi-arm scheduler
  -> reads CN arm/family feedback tables
  -> emits arm budget table and family action table
  -> caps CEM exploit when feedback_update_allowed=false
  -> preserves fresh budget floor before Phase3CP

Phase3CP
  reward-gated medium search smoke
  -> reads CO budget table
  -> generates small scheduler-controlled true1min expression set
  -> passes CA bridge, controlled CM fixture, CN memory, and CO reschedule
  -> does not yet run true Phase3CM portfolio reward audit
  -> next gate replaces fixture with true Phase3CM audit

Phase3CP real CM small loop
  -> replaces the controlled CM fixture with phase3cm-train-portfolio-sortino-reward-audit
  -> uses explicit true1min shard root and field-availability gate before CM
  -> fixes CM reward lineage so generator_arm/candidate_id survive reward output
  -> ca_ranked: 16 generated, 12 CA rows, 4 CM rows, 0 followup
  -> arm_balanced: 36 generated, 30 CA rows, 12 CM rows, 0 followup
  -> best arm-balanced row is event_state but fails turnover gate
  -> proves wiring, but does not authorize large-search expansion

Phase3CP low-turnover event-state probe
  -> generates smoothed/long-window variants around the best event_state near-pass motif
  -> low-turnover variants reduce turnover but lose train/validation reward
  -> top_quantile=0.30 creates one small-sample followup, but the family remains frozen
  -> 2-shard confirmation rejects that followup
  -> does not authorize Phase3CQ

Phase3DN
  -> formalizes progressive reward allocation
  -> cheap proxy and small probes may allocate compute but must not permanently
     reject novelty unless structural blockers are present
  -> validation and holdout remain report-only

Phase3DP
  -> adds low-weight train regime-stability reward to Phase3CM
  -> adds bounded factor expression, feature matrix, and operator subtree caches
  -> keeps regime stability as a small gradient term, not a promotion gate

Phase3DS / Phase3DT / Phase3DU
  -> DS targeted family repair and DT survivor expansion are retained as
     provenance and seed ancestors
  -> DU adaptive-regime-free deepen is retired as current route because it kept
     too much hard-freeze behavior

Phase3DV
  -> replaces hard early performance elimination with budget-pool allocation
  -> keeps global survivor, regime specialist, probation, and fresh self-deepen
     lanes
  -> uses true1min shard schema filtering and typed primitive gate before CM
  -> later audited under Phase3EI/EK for CN ban-short reward compliance

Phase3EI / Phase3EK
  -> found true1min Phase3CM/BZ/EG/EH historical outputs were not system-level CN tradable proof unless explicit long-only evidence exists
  -> rechecked 1024 broad candidate/proxy/followup rows under long_only_top on DESKTOP-77OPJ6F
  -> completed Stage1 evidence found 0 followup / 120 audited under no-short
  -> exact alpha-level restatement found 28 true1min followup-like expressions, 9 exact-hash rechecked, all HOLD
  -> rejected candidate_id-only evidence where expression_hash mismatched
  -> patched Phase3CM default portfolio mode from long_short_spread to long_only_top
  -> preserved mature daily official X0/R3 as a separate read-only daily-shadow chain with existing long-only replay evidence
```

## Evidence Map

| Stage | Question Answered | Primary Evidence |
|---|---|---|
| CD | Are AST primitives safe for new field types? | `reports/phase3cd_ast_primitive_assumption_audit_20260618/PHASE3CD_AST_PRIMITIVE_ASSUMPTION_AUDIT_20260618.md` |
| CE | Did unsafe motif signatures touch official roots or runtime pools? | `reports/phase3ce_unsafe_motif_quarantine_audit_20260618/PHASE3CE_UNSAFE_MOTIF_QUARANTINE_AUDIT_20260618.md` |
| CE1 | Are unsafe structures blocked rather than forgotten? | `reports/PHASE3CE1_TYPED_PRIMITIVE_GATE_IMPLEMENTATION_SUMMARY_20260618.md` |
| CE1 memory | Is search memory kept as blocked bookkeeping? | `reports/phase3ce1_search_memory_blocked_view_20260618/PHASE3CE1_SEARCH_MEMORY_BLOCKED_VIEW_20260618.md` |
| CE2 | Can typed primitives run on actual/validation true1min panels? | `reports/phase3ce2_typed_primitive_candidate_pack_canary_20260618/PHASE3CE2_TYPED_PRIMITIVE_CANARY_20260618.md` |
| CF | What was the guarded large-search prelaunch contract? | `reports/phase3cf_reward_gated_large_search_prelaunch_20260618/PHASE3CF_REWARD_GATED_LARGE_SEARCH_PRELAUNCH_20260618.md` |
| CG | What fastpath assumptions were packaged? | `reports/PHASE3CG_TRUE1MIN_FASTPATH_PREP_20260619.md` |
| CL | Did proxy/CEM winners survive fragment replay? | `reports/PHASE3CL_TRUE1MIN_FRAGMENT_REPLAY_AUDIT_20260622.md` |
| CM | What replaces fragment Sortino as the reward target? | `reports/PHASE3CM_TRAIN_SORTINO_REWARD_CHAIN_20260623.md` |
| CN | How does CM reward become search feedback memory? | `reports/PHASE3CN_REWARD_FEEDBACK_WIRING_20260623.md` |
| CN integrated | Does the feedback contract close across CA/CM/CN/searcher guard? | `reports/phase3cn_integrated_feedback_smoke_20260623/PHASE3CN_INTEGRATED_FEEDBACK_SMOKE_20260623.md` |
| CO | What budget plan controls Phase3CP search arms? | `reports/PHASE3CO_MULTI_ARM_SCHEDULER_20260623.md` |
| CO smoke | Does the scheduler cap exploit/freeze families and keep fresh floor? | `reports/phase3co_multi_arm_scheduler_smoke_20260623/PHASE3CO_MULTI_ARM_SCHEDULER_SMOKE_20260623.md` |
| CP smoke | Does a CO-budgeted generation pass close CA/CM/CN/CO loop? | `reports/PHASE3CP_REWARD_GATED_MEDIUM_SEARCH_20260623.md` |
| CP smoke outputs | What candidates, CA table, CN memory, and next budgets were produced? | `reports/phase3cp_reward_gated_medium_search_smoke_20260623/PHASE3CP_REWARD_GATED_MEDIUM_SEARCH_SMOKE_20260623.md` |
| CP real CM small loop | Can CP replace the fixture with real train portfolio Sortino reward? | `reports/phase3cp_real_cm_small_loop_20260623/PHASE3CP_REAL_CM_SMALL_LOOP_20260623.md` |
| CP real CM balanced loop | Does arm-balanced CM sampling expose a better direction than CA-ranked top rows? | `reports/phase3cp_real_cm_balanced_loop_20260623/PHASE3CP_REAL_CM_SMALL_LOOP_20260623.md` |
| CP low-turnover event probe | Can the best event_state near-pass be repaired by lower-turnover variants? | `reports/phase3cp_low_turnover_event_state_probe_20260623/PHASE3CP_LOW_TURNOVER_EVENT_STATE_PROBE_20260623.md` |
| CP topq30 confirmation | Does the topq30 event_state followup survive larger true-CM confirmation? | `reports/phase3cp_event_state_topq30_followup_confirm_20260623/phase3cm_train_reward/PHASE3CM_TRAIN_PORTFOLIO_SORTINO_REWARD_AUDIT_20260623.md` |
| DN | How should reward allocate compute without premature rejection? | `reports/PHASE3DN_PROGRESSIVE_REWARD_ALLOCATION_20260628.md` |
| DP | How were regime reward and bounded evaluator caches added? | `reports/PHASE3DP_REGIME_CACHE_REWARD_20260629.md` |
| DV | What is the current budget-pool self-deepen route? | `reports/PHASE3DV_BUDGET_POOL_SELF_DEEPEN_20260630.md` |
| DV hygiene | Which routes are retired or provenance-only? | `reports/PHASE3DV_RETIREMENT_AND_WORKSPACE_HYGIENE_20260630.md` |
| EI | Which historical true1min artifacts lack CN long-only evidence? | `reports/phase3ei_ban_short_lineage_audit_20260704/PHASE3EI_BAN_SHORT_LINEAGE_AUDIT_20260704.md` |
| EI 77O recheck | Does the broad candidate/proxy/followup pool survive no-short recheck? | `reports/phase3ei_historical_ban_short_recheck_77o_20260704/phase3ei_historical_ban_short_recheck_summary.json` |
| EK | Which actual alpha-level or followup assets are verified under CN long-only? | `reports/PHASE3EK_ALPHA_LEVEL_BAN_SHORT_VERIFICATION_20260704.md` |
| EL | What is the current true1min best level after the ban-short restatement? | `reports/PHASE3EL_TRUE1MIN_BEST_LEVEL_20260704.md` |

## Reward Evolution

```text
Old proxy stage:
  aligned IC / spread / hit / proxy quality
  useful for cheap triage
  failed as reward

Phase3CL fragment stage:
  sampled trade_time fragment Sortino
  useful as diagnostic slice replay
  failed as primary reward target because local slices can be overfit

Phase3CM target:
  train-set portfolio PnL curve
  turnover-adjusted cost
  horizon-sleeve aggregation
  train / validation / holdout split
  holdout forbidden as search feedback
  CN default portfolio mode is long_only_top after Phase3EK
  long_short_spread is diagnostic proxy only
```

## Current Route Contract

```text
phase3ca-build-bz-candidate-audit:
  dedupe and hard-reject proxy outputs
  not proof

phase3cm-train-portfolio-sortino-reward-audit:
  primary reward audit target for next search
  no search launch by itself

phase3cn-integrated-feedback-smoke:
  contract smoke for CA -> CM reward fixture -> CN -> guarded searcher feedback
  no search launch and no true1min portfolio evaluation

phase3co-multi-arm-scheduler-smoke:
  reads CN feedback memory and emits CP arm/family budgets
  no search launch and no true1min portfolio evaluation

phase3cp-reward-gated-medium-search-smoke:
  reads CO budget table and generates a small scheduler-controlled candidate set
  uses controlled CM reward fixture; not alpha proof

phase3cp-real-cm-small-loop:
  reads CP/CO budget table, generates candidates, CA-ranks them, field-gates
  against true1min shard schemas, runs real CM train portfolio Sortino reward,
  writes CN feedback memory, and emits next CO budgets
  supports `ca_ranked` and `arm_balanced` CM selection
  checks CM lineage consistency before CN/CO feedback
  diagnostic-only; zero followup in the current small and balanced runs

phase3cp-low-turnover-event-state-probe:
  generates targeted low-turnover opening-state variants around the best
  event_state near-pass motif
  diagnostic-only; produced zero followup and shows naive smoothing kills reward

phase3bz-fragment-replay-audit:
  optional diagnostic replay
  not primary reward

phase3du-adaptive-regime-free-deepen-pack:
  retired route
  requires --allow-diagnostic in app.py
  provenance replay only

phase3dv-budget-pool-self-deepen-pack:
  current candidate-pack route
  allocates bounded budget to global survivor, regime specialist, probation,
  and fresh self-deepen lanes before Phase3CM reward audit
```

## What Was Rejected

```text
Rejected:
  proxy-only candidate language
  CEM feedback on proxy IC as proof
  fragment_sortino as primary reward
  old 1D data for true1min claims
  deleting unsafe memory keys
  promoting X0/R3 changes from this chain

Retained:
  true1min shard backbone
  typed primitive gates
  blocked search memory view
  CA hard-reject bridge
  CM train portfolio reward audit
```

## Next Gate

Before any large search restart:

```text
1. Treat the Phase3CP real-CM small and balanced loops as negative reward checkpoints.
2. Do not enter Phase3CQ scale-up because followup_count=0 in both runs.
3. Do not continue naive low-turnover smoothing of the same event_state motif; it reduced turnover but killed reward.
4. Broaden fresh generation beyond this motif and require multi-shard confirmation before CN exploit permission.
5. Enter Phase3CQ rolling large search only if CP produces CM-positive / validation-surviving new families.
6. Keep BZ fragment replay diagnostic-only and holdout read-only.
7. Treat Phase3DV as the active next-gate experiment: judge whether budget-pool
   allocation produces better search information than Phase3DU hard early
   elimination.
```

Success for the next stage is not a high proxy score. It is a candidate family
that survives train reward, validation checks, holdout reporting, wrong-lag
guards, turnover costs, and search-memory crowding controls.
