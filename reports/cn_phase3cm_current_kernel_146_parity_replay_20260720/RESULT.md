# CN current-kernel 146-pair parity replay

Status: `CN_PHASE3CM_CURRENT_KERNEL_146_PARITY_PASS`

The frozen partial kernel replay covered all 146 active-bar pairs and all 292 candidate members. Candidate identity, support, rank/mapping behavior, portfolio weights, turnover, cost, reward, RankIC/blocker evidence, and the temporal/state/support/portfolio/reducer continuation payloads matched exactly across both frozen partitions.

The heavy replay ran at source SHA `790b9b29fd289d570f8cb3ccb09aeba25d622c23`. Its initial post-run receipt failed because the 146-specific adjudicator consumed the generic scaling-probe status, which also requires zero phase-level `parallelism_not_engaged_events`. Both checkpoint receipts contained exact continuation payloads with empty mismatch lists, while each retained three parallelism diagnostic events. Adjudicator commit `286f813625751f07721a31a69745a6cae6c8f283` separated those diagnostics from semantic parity. The original fail receipt remains immutable and hash-bound; the readjudicated receipt supersedes only that gate interpretation.

## Observed execution

- Wall time: 2,179.014 seconds (36.32 minutes)
- Global peak RSS: 29,656,395,776 bytes (27.62 GiB)
- Partition peak RSS: 13,848,854,528 and 15,855,566,848 bytes
- Final output: 435,618,229 bytes
- Gate failure / launch failure: none / none
- Development-only reads: validation 0, holdout 0, forward 2026 0
- Promotion: forbidden
- Formal evaluator authority: unchanged
- Backend state: `PARTIALLY_QUALIFIED` / `EXPERIMENTAL_BACKEND`

## 1,024 resource decision

The frozen projection passes without a speedup threshold:

- 584 active-bar + 440 stock-session pairs
- Projected host time: 3.25 hours (12-hour hard budget)
- Projected active worker RSS: about 18.5 GiB (20 GiB GO gate)
- Projected active global RSS: about 34.5 GiB (48 GiB GO gate)
- Projected output: about 3.6 GB (64 GiB hard gate)

Next decision: `FREEZE_1024_RESOURCE_AND_EXECUTION_CONTRACT`.
