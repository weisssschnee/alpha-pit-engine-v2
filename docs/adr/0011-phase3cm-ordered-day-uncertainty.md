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
- This decision does not authorize a search, financial replay, validation,
  holdout, 2026 access, promotion or a successor budget.

