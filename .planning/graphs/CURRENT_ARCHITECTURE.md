# Current Architecture

Updated: 2026-07-10

This is the curated active CNline2 architecture. It intentionally excludes
historical diagnostic branches that still appear in the raw knowledge graph.

```mermaid
flowchart LR
    D["Repaired true1min 2024-2025\n16 shards / 121 columns"] --> G["Multi-arm generator\nRX / UCB / CEM / hybrid / fresh"]
    G --> S["Expression semantics\nvalue domains + exact rewrites"]
    S --> T["Typed primitive gate"]
    T --> M["Search memory\nexact expressions + unsafe skeletons"]
    M --> CA["CA ranking and balanced quota"]
    CA --> V["Pre-CM semantic-only evaluator\nconstant + rank + bucket equivalence"]
    V --> CM["Phase3CM full train reward\nlong-only + costs + RankIC + regime"]
    CM --> CN["Phase3CN guarded feedback"]
    CN --> G
    OOS["Validation / holdout / 2026 forward"] -. "report only" .-> CM
```

## Active Contracts

- Data is true `trade_time` 1min; old 1D roots are forbidden.
- Search/training uses repaired 2024-2025 data. 2026 is separate forward/OOS.
- Construction safety is independent of memory; clearing memory cannot disable the semantic or typed gates.
- Sampled signal equivalence removes duplicate work for the current run but does not create unsafe skeleton blocks.
- Phase3CM optimizer reward is train-only and long-only for CN tradability.
- Validation and holdout are report-only.
- Operator cache default cap is 2 GiB per process; feature-matrix cache default cap is 4 GiB per process.
- Persistent disk cache remains size, free-space, and TTL bounded.

## Current Caveats

- The repaired root has no accepted full PIT plate-membership lane.
- Sampled vector equivalence is not final OOS evidence.
- SafeDiv tails are diagnosed; automatic clipping is not part of the current contract.
