# EVALRESET Signal-Sketch Audit

Decision: `EVALRESET_SIGNAL_SKETCH_AUDIT_DIAGNOSTIC_ONLY`

Coordinates and clustering use development data only; labels, returns, validation, holdout, and forward data are excluded.

## Fidelity and stability

- Joint coverage minimum: `183` coordinates.
- Qualified / limited: `10529` / `14047`.
- A/B ARI: `0.98031151`; NMI: `0.97131561`.
- Top-1 share difference: `0.00056986`.
- Sketch/exact correlation: `0.9999415`.
- Cluster-pair purity: `1.0`.
- Boundary misclassification: `0.0`.
- All preregistered gates pass: `True`.

## Unified stage mapping

| stage | rows | signal-qualified | limited | clusters | N_eff | top-1 eligible | top-3 eligible | top-1 all | survival |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| generation | 24576 | 10529 | 14047 | 4508 | 97.35760661 | 0.09431095 | 0.12327856 | 0.04040527 | 1.0 |
| proxy | 1536 | 654 | 882 | 501 | 74.17898023 | 0.10550459 | 0.13455657 | 0.04492188 | 0.11113576 |
| admission | 384 | 186 | 198 | 157 | 62.0 | 0.10215054 | 0.13978495 | 0.04947917 | 0.31337325 |
| strict_reward | 324 | 153 | 171 | 144 | 130.77653631 | 0.02614379 | 0.05882353 | 0.01234568 | 0.91719745 |
| coverage_qualified | 202 | 144 | 58 | 137 | 126.43902439 | 0.02777778 | 0.05555556 | 0.01980198 | 0.95138889 |

## Interpretation boundary

- Coverage-limited candidates are reported separately and are not treated as an economic signal cluster.
- Cluster IDs are fixed once at generation and mapped unchanged through every downstream stage.
- The report does not compare AST-skeleton N_eff directly with signal-cluster N_eff.
- Stagewise review adds no post-hoc numerical collapse threshold.
- Proxy/admission top-1 shares (`10.55%` / `10.22%`) remain close to
  generation (`9.43%`); strict reward and coverage qualification fall to
  `2.61%` / `2.78%`.
- Cluster survival is not disproportionately lower than candidate-budget
  retention at any downstream transition.
- First signal-level collapse status:
  `not_observed_in_stagewise_signal_sketch_audit`.
