# EVALRESET Search Collapse Audit

Created: 2026-07-11T06:34:38.639164+00:00

All behaviour clustering uses fixed-calendar train/development dates only. Validation, holdout, and forward data are excluded.

| stage | rows | exact | signal clusters | behaviour clusters | skeletons | skeleton N_eff | top skeleton | lineage entropy | effective multiplicity |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| generation | 24576 | 24576 | N/A | N/A | 131 | 33.54784544 | 0.09313965 | 8.22886031 | 732.56567383 |
| proxy | 1536 | 1536 | N/A | N/A | 106 | 41.10558227 | 0.06119792 | 7.0123261 | 37.3671875 |
| admission | 384 | 384 | N/A | N/A | 71 | 33.34599729 | 0.07291667 | 5.75650922 | 11.515625 |
| strict_reward | 324 | 324 | N/A | 84 | N/A | N/A | N/A | 5.55065957 | 6.87128713 |
| memory_predecessor | 192 | 192 | N/A | N/A | N/A | N/A | N/A | 5.06749654 | 1.36458333 |
| scheduler_predecessor | 154 | 154 | N/A | N/A | N/A | N/A | N/A | 5.0369526 | 1.0 |

## Collapse Status

- Earliest observed structural redundancy: `generation`
- Structural basis: AST-skeleton N_eff / candidate rows < 0.25 or top skeleton share > 0.25; this identifies redundancy only and is not evidence of signal-level collapse
- First signal-level collapse: `not_observed`
- Signal-level status: the completed two-coordinate audit passed all fidelity
  gates and found no downstream concentration amplification. See
  `signal_sketch/SIGNAL_SKETCH_AUDIT.md` for the fixed cluster-ID funnel.

## Coverage Limits

- This structural report retains `N/A` signal columns; the completed signal
  registry and five-stage metrics are authoritative in the linked signal-sketch
  report rather than being copied into a second identity space.
- Behaviour clusters exclude candidates with fewer than the configured minimum train/development dates.
- Family and motif are lineage/grammar views, not economic hypothesis identities.
