# Phase3CN Reward Feedback Wiring 2026-06-23

Decision: `PHASE3CN_CN0_CN4_IMPLEMENTED_NO_SEARCH_STARTED`

## Scope

Phase3CN turns Phase3CM train portfolio reward outputs into standardized search feedback memory. It does not run search.

## Implemented

```text
CN0 candidate schema:
  implemented shared schema helper:
    src/our_system_phase2/services/candidate_schema.py

CN1 CM reward table standardization:
  Phase3CM now writes:
    phase3cm_train_reward.csv
    phase3cm_candidate_train_reward_summary.csv

CN1 feedback memory builder:
  app route:
    phase3cn-feedback-memory-smoke

CN2 searcher feedback inputs:
  BS/BT/BU now expose:
    --feedback-table
    --arm-score-table
    --family-memory
    --blocked-family-table
    --exploit-allowed-family-table
    --arm-id

CN3 searcher feedback guard smoke:
  app route:
    phase3cn-searcher-feedback-smoke

CN4 integrated feedback contract smoke:
  app route:
    phase3cn-integrated-feedback-smoke
```

The feedback builder writes:

```text
phase3cn_search_feedback_memory.csv
phase3cn_arm_score_table.csv
phase3cn_family_score_table.csv
phase3cn_blocked_family_table.csv
phase3cn_exploit_allowed_family_table.csv
phase3cn_feedback_memory_summary.json
```

## Candidate Schema

Every generator output must be comparable on these fields:

```text
candidate_id
expression_hash
expression
generator_arm
generator_route
seed
round_id
parent_id
mutation_type
field_family
primitive_family
event_state_family
horizon_bucket
turnover_bucket
family_id
motif_id
subtree_hashes
proxy_quality
aligned_ic_mean
spread_hit_rate
mean_one_way_turnover
blocker_flags
phase3ca_proxy_quality
train_reward
train_reward_decision
train_reward_blockers
validation_day_sortino
validation_mcmc_prob_gt_0
holdout_day_sortino
holdout_mcmc_prob_gt_0
```

## Feedback Rules

```text
CEM/UCB update allowed:
  only if clean_feedback_count >= min_clean_feedback

Exploit allowed family:
  CM-positive
  validation survivor
  not wrong-lag / high-corr
  not extreme-turnover
  not over family-share cap

Blocked/frozen family:
  wrong-lag or high-corr
  proxy-high but CM-negative
  high-turnover
  repeated rewardhack structure

Holdout:
  carried through
  read-only
  excluded from arm_score
```

## Searcher Guard

```text
External Phase3CN feedback is guarded before CEM/UCB mutation:
  if feedback table is absent:
    legacy in-run seed feedback path remains unchanged

  if feedback table is present and clean_feedback_count < threshold:
    CEM/UCB policy scores remain unchanged
    feedback.updated=false

  if feedback table is present but no exploit-allowed family exists:
    CEM/UCB policy scores remain unchanged

Holdout:
  columns may be present
  optimizer does not use them for arm score or update permission
```

## Remaining CN Work

```text
After CN4:
  Phase3CO scheduler design/implementation
```

## Next Stage

Do not start Phase3CP medium search until Phase3CO scheduler has explicit arm budgets and family freeze/downweight rules.

## Smoke Verification

A tiny non-search CM -> CN smoke was run:

```text
CM:
  candidate_limit: 1
  max_shards: 1
  sample_trade_times_per_shard: 8
  horizons: 1,5

CN:
  min_clean_feedback: 2
```

Result:

```text
status: ok
candidate_count: 1
family_count: 1
arm_count: 1
exploit_allowed_family_count: 0
blocked_family_count: 1
```

The single tested old proxy winner was mapped as:

```text
candidate_id: phase3bs_00122
generator_arm: cem_exploit
train_reward: -0.9922991
clean_feedback_count: 0
feedback_update_allowed: false
family_status: freeze
family_reasons: top_family_share_cap|high_turnover
```

This verifies the intended guard: a proxy/CEM winner with negative CM reward does not become exploit feedback.

## Searcher Feedback Smoke Verification

The searcher feedback smoke was run to verify the consumer side without running search:

```text
route:
  phase3cn-searcher-feedback-smoke

checked:
  BS/BT/BU feedback args present
  low clean feedback blocks update
  CEM/UCB policy scores unchanged
  holdout columns present but not used for score

result:
  decision: PHASE3CN_SEARCHER_FEEDBACK_GUARD_PASS_DIAGNOSTIC_ONLY
  clean_feedback_count: 0
  feedback_update_allowed: false
  policy_scores_unchanged: true
  holdout_used_for_score: false
```

## Integrated Feedback Smoke Verification

The integrated smoke validates the interface chain without launching search:

```text
route:
  phase3cn-integrated-feedback-smoke

chain:
  synthetic search top_decisions
    -> Phase3CA candidate bridge
    -> controlled Phase3CM reward fixture
    -> Phase3CN feedback memory
    -> search_feedback guard

boundary:
  no search generation
  no true1min portfolio reward evaluation
  validates schema handoff and feedback safety gates only

result:
  decision: PHASE3CN_INTEGRATED_FEEDBACK_SMOKE_PASS_DIAGNOSTIC_ONLY
  ca_candidate_count: 2
  cn_candidate_count: 2
  clean_feedback_count: 1
  strict_min_clean_feedback: 2
  strict_update_allowed: false
  loose_min_clean_feedback: 1
  loose_update_allowed: true
  strict_policy_scores_unchanged: true
  holdout_used_for_score: false
```
