# ADR 0011: Use ordered-day stationary bootstrap uncertainty in Phase3CM

Status: Accepted by user direction on 2026-07-30.

## Context

Phase3CM historically exposed `day_mcmc_*` fields, but the implementation was
an IID bootstrap over daily portfolio returns. It did not run a Markov chain or
estimate a Bayesian posterior. IID resampling also broke adjacent-day
dependence, did not identify invalid Sortino draws, and did not bind the block
rule or Monte Carlo error into the evaluator output.

This uncertainty term contributes 20 percent of the existing train-only
Phase3CM composite reward. The primary/composite, matched-control and
development-only authority boundaries established by ADR 0010 remain
unchanged.

The first implementation closed standalone candidate uncertainty but left the
matched increment as a difference between two scalar composite rewards. That
did not estimate uncertainty of the primary-minus-control path itself and
allowed reward provenance to reach feedback/checkpoint state without an
explicit uncertainty-contract binding.

## Decision

Phase3CM uses the existing ordered daily net-return curve as the resampling
unit and applies a circular Politis-Romano stationary bootstrap:

- blocks have geometric length with restart probability `1 / L`;
- expected block length `L` is frozen as rounded cube root of the day count,
  clamped to `[2, 20]` and never tuned from candidate reward;
- existing candidate seed derivation continues to own deterministic replay;
- final curves retain 600 requested draws and incremental checkpoints retain
  their existing requested-draw contract;
- Sortino p05/p25/median/p75/p95, support above zero, probability Monte Carlo
  standard error, requested/valid/invalid draw counts, block metadata,
  downside-day support and an IID diagnostic reference are emitted;
- the stationary p25 and support above zero replace the IID values at the
  existing reward weight and `0.60` admission threshold;
- day-count and valid-draw support warnings are recorded as quality flags but
  do not silently create a new admission gate.

Canonical output names use `day_uncertainty_*`. Historical `day_mcmc_*` names
remain exact compatibility aliases and must not be interpreted as a Bayesian
posterior or Markov-chain result. The contract identity is
`phase3cm_ordered_day_stationary_block_uncertainty_v2`.

### Matched-increment amendment

The same accepted decision also governs matched feedback as follows:

- primary and control net returns are paired on the existing exact shared
  support coordinate before resampling;
- horizons are averaged at each shared trade time and then summed by ordered
  trade date, matching the evaluator's frozen all-horizon aggregation;
- one stationary-bootstrap index path resamples the paired daily net-return
  delta, so primary and control can never receive independent blocks;
- 600 draws estimate the mean paired daily delta, its
  p05/p25/median/p75/p95, support above zero and Monte Carlo standard error;
- mean-delta support, rather than paired Sortino, owns this gate so an
  all-positive path remains well-defined when downside deviation is zero;
- support below `0.60`, missing daily atoms, support mismatch, or standalone
  uncertainty-contract mismatch blocks optimizer feedback but does not erase
  a completed pair evaluation;
- the matched reward contract is
  `cn_matched_train_composite_increment_with_paired_uncertainty_v2`; its
  uncertainty contract is
  `phase3cm_paired_daily_net_delta_stationary_bootstrap_v1`;
- both identities must survive the matched row, train-only feedback
  projection, optimizer observation/transcript, frozen search contract,
  optimizer snapshot and streaming execution-plan hash. Missing or mismatched
  identities fail closed on resume.

The shared resampling implementation lives in
`src/our_system_phase2/services/time_series_uncertainty.py`; batch and
streaming evaluators consume that one implementation.

## Consequences

- No new evaluator, optimizer, database, dependency or authority node is
  created.
- Validation, holdout and 2026 outputs remain report-only and cannot feed
  search.
- A-share executable replay remains a separate finalist/economic gate.
- Historical closed artifacts remain immutable under their original IID
  bootstrap semantics; they are not silently relabeled or recomputed.
- Future replay or cache reuse must bind source identity and the uncertainty
  contract before treating reward values as comparable.
- Existing frozen execution plans and optimizer snapshots use the prior
  contract and are intentionally not resume-compatible with Reward V2; new
  plans/snapshots use versioned schemas whose hashes include both contract
  identities.
- This decision does not authorize a search, financial replay, validation,
  holdout, 2026 access, promotion or a successor budget.
