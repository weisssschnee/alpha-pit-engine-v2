# ADR 0003: Deterministic Development-Only Signal Sketches

Date: 2026-07-11

Status: Accepted for Phase A diagnostics; not a search or promotion mechanism.

## Context

The historical candidate funnel contains 24,576 generated expressions but only
131 AST skeletons. Syntax redundancy does not establish signal equivalence.
Materializing every candidate over 598,061,503 rows would be expensive and
would not improve evaluation governance. A 384-candidate legacy semantic-only
canary exceeded 120 seconds because it retained full candidate Series and
repeated rank work over context rows.

## Decision

Use a two-pass, label-free signal-sketch audit:

1. Select two deterministic coordinate sets from fixed development/train dates.
   Coordinates are stratified by month, intraday period, stock coverage
   interval, observed listing-age bucket, and neutral bar-activation density.
   They never use returns, labels, validation, holdout, performance regimes, or
   forward data.
2. Evaluate each candidate on a compact context panel and immediately emit an
   activation bitmap, rank/sign sketch, robust quantized-value sketch, SimHash,
   missingness pattern, and coverage profile.
3. Validate sketches on the fixed 384 admission candidates using exact rank and
   value vectors. Required gates were preregistered in the Phase A run manifest.
4. Candidates with fewer than `max(128, 5% of coordinates)` finite observations
   in either coordinate set are `signal_coverage_limited`. They remain mapped
   but are not treated as one economic signal cluster.
5. Assign generation cluster IDs once and map those IDs unchanged through
   proxy, admission, strict reward, and coverage-qualified stages.

## Alternatives rejected

- Full 598M-row replay: excessive resource use for an identity question.
- AST skeleton as signal identity: conflates grammar with numerical behaviour.
- Single coordinate set: cannot measure coordinate sensitivity.
- Disjoint tiny stock universes: pilot ARI/NMI showed universe composition
  dominated the stability test.
- Treating all-missing vectors as a signal cluster: confuses insufficient
  coverage with economic equivalence.

## Consequences

- Sketches are diagnostic-only and cannot update CEM/UCB/MCTS memory, scheduler
  credit, family kill/freeze decisions, or reward.
- Exact fidelity, A/B stability, coverage-limited counts, and boundary errors
  must be visible alongside every full-generation cluster claim.
- Failed fidelity gates block full-generation interpretation and Phase A
  acceptance.
- The coordinate and sketch versions, manifests, hashes, worker summaries, and
  artifact index are part of the Phase A evidence bundle.
