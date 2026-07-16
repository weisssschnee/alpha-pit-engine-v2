# CN Phase3CM Streaming Repair Report

## Outcome

`CN_PHASE3CM_STREAMING_EVALUATOR_QUALIFIED`

Stage A resource readiness: `CN_COMPOSITIONAL_STAGE_A_ENTRY_READY`. Strict Stage A remains `NOT_AUTHORIZED` and was not executed.

## Frozen scope

- Source implementation SHA: `984181aeaaa54ed7602dd47cc726713224b25a42`
- Final qualification repo SHA: `71828442e0b566bdec27a7a091e1b24c16b88b7b`
- Frozen input binding: `68eeb9dc1819b5f52f586ceb6740bee25df9d1e687b3b924948a75e7bb3da93f`
- Development release: `cfb2742d975f2f6f1dcdf78d011f6d471b8d0e444164bae1d1816ba1fdcc5827`
- Split manifest: `fab9fb17642595456e10c4ad44357193f2dcdc1d39edd785b8298fbe9ca22241`
- Registry: `e4b8bc809ab9ae8e1463aa85c53c13c1710aed6915c967ff806c8906deea886d`
- Validation / holdout / 2026 reads: `0 / 0 / 0`

## Proven results

- Full-coordinate session parity: `FULL_COORDINATE_PARITY_PASS`, mismatches `0`.
- Resume parity: `RESUME_UNINTERRUPTED_PARITY_PASS`.
- Single-pair Phase C: 361.36s, peak RSS 5.64GiB, `SINGLE_PAIR_FULL_COORDINATE_GATE_PASS`.
- Phase D 32-pair scaling: 3660.86s, global peak RSS 13.43GiB.
- Phase E frozen 32-pair: 3657.57s, global peak RSS 13.37GiB, `CN_PHASE3CM_PHASE_E_32PAIR_QUALIFICATION_PASS`.
- Coordinate rows retained: `0`.
- Dominant hot path: `BATCHED_PORTFOLIO_KERNEL_BOTTLENECK`.

## Architecture result

The evaluator now executes route-native time-major blocks, shares a two-level Value/Mapping DAG, materializes pair-common support per block, runs native batched portfolio kernels, updates bounded reducers, releases DAG nodes after their last consumer, and writes real recoverable checkpoints. The full-market trade-time barrier is preserved across all 16 physical shards.

## Cost result

- Sidecar build: 329.15s, footprint 13.88GiB.
- Application-cold single-pair: 361.36s.
- OS-page-cache-warm single-pair: 360.34s.
- Sidecar-build-inclusive single-pair: 690.51s.
- Sidecar-ready single-pair: 361.36s.

## Search boundary

No candidate was promoted, no Stage A search ran, no positive memory was written, and forward 2026 remains sealed.
