# ADR 0008: Hybrid TPE plus Availability development search policy

Status: Accepted as the active CN development search-policy authority; candidate
promotion, validation feedback, holdout access and 2026 access remain forbidden.

## Context

The availability-aware runner can emit exact-novel candidates through direct TPE
draws, deterministic same-bucket replacement and a bounded global fallback. Its
mechanics and trial-state accounting were already qualified, but the previous
20% fallback ceiling did not answer whether this hybrid system produced useful
candidates more efficiently than availability-aware Uniform.

The frozen Hybrid Search Productivity Medium therefore assigned 3,072 formal
fresh exact asks, within every route and checkpoint, 50/50 between:

- `HYBRID_TPE_AVAILABILITY`; and
- `AVAILABILITY_AWARE_UNIFORM`.

Both arms used the same typed availability index, Grammar, compiler, archives,
behavior admission, Phase3CM evaluation, reward, cost, horizons and train-only
access. Uniform had no Optuna trial and no optimizer feedback. The predeclared
decision rule required the lower bound of a 5,000-replicate route-standardized
relative-productivity interval to exceed 10%, higher productive throughput, and
noninferior median and p10 search score.

## Decision

`HYBRID_TPE_AVAILABILITY` is the active CN development search policy.

The accepted policy is the whole hybrid emitter:

- official Optuna TPE chooses typed proposals;
- the Availability Controller enforces exact novelty and may make deterministic
  same-bucket replacements;
- global fallback remains a bounded emission mode rather than a policy-quality
  veto; and
- only Hybrid trials receive train-only optimizer observations and tells.

The Medium produced 471 productive Hybrid candidates from 1,536 formal asks
versus 339 from Uniform. Route-standardized productive yield was 0.30664 versus
0.22070. The 5,000-replicate relative-uplift median was 38.90%, with a 95%
interval of 24.68%-55.09%. Productive throughput was 118.71 versus 85.44 per
shared wall hour, while Hybrid median and p10 search score were both no worse.
The independent recomputation matched the immutable runner decision exactly.

This decision establishes the default policy for a separately frozen bounded
development tranche. It does not authorize an unbounded or automatic successor
campaign. Each successor must still freeze its route floors, route caps,
flexible budget, seeds, archives, runtime envelope and no-promotion boundary.

## Boundaries

- The result accepts the Hybrid system as a whole; it does not identify a pure
  TPE causal effect separate from Availability replacement.
- The unified Registry remains the top-level route authority. The Hybrid policy
  is a thin route-local search policy beneath it.
- The matched-control and Phase3CM evaluator authorities are unchanged.
- Reward remains
  `min(primary_composite_reward, matched_train_increment)` from full-coordinate
  train outcomes only.
- Validation, holdout and 2026 data cannot influence reward, search policy,
  scheduler, archives or memory.
- Candidate promotion, economic claims and additional sealed-period access
  remain forbidden.

## Consequences

- Availability-aware Uniform remains the retained baseline and operational
  fallback, but it is not the default development search policy.
- Do not rerun the completed agency canary or the 3,072-ask Medium.
- The next development tranche should replace administrative fill targets with
  frozen route coverage floors, maximum caps and a bounded flexible budget,
  informed by productive-yield evidence.
- Structural-bucket-only TPE attribution is a possible later refinement, not a
  prerequisite for the next bounded tranche.
- Evidence is bound in
  `runtime/run_plans/cn_hybrid_search_productivity_medium_20260728_receipt.json`
  at deployed implementation SHA
  `9bc43e89398ea101393eb00e3d8e220ac4527d64`.
