# Current Architecture

Updated: 2026-07-11

State: `A_EVALRESET / COMMITTED_AWAITING_USER_ACCEPTANCE`; Phase B is `FROZEN`.

The machine-readable node contract is generated from
`runtime/run_plans/evalreset_phase1_architecture_registry_v1.json` into
`graph.json`. Every node there contains status, implementation path,
input/output artifact, data role, feedback permission, test/evidence,
last-verified SHA, and blocker.

```mermaid
flowchart LR
    RAW["IMPLEMENTED<br/>16-shard true1min 2024-2025"] --> AUG["IMPLEMENTED/PARTIAL<br/>field + firstN + event/state sidecars"]
    PLATE["FROZEN<br/>plate/industry PIT sidecar"] -. "Phase B" .-> AUG
    SPLIT["IMPLEMENTED<br/>fixed calendar: 364 development / 73+48 spent"] --> STRICT
    AUG --> GEN["IMPLEMENTED<br/>generation 24,576"]
    GEN --> PROXY["IMPLEMENTED<br/>proxy 1,536"]
    PROXY --> ADM["IMPLEMENTED<br/>admission 384"]
    ADM --> STRICT["IMPLEMENTED<br/>strict train reward 324"]
    STRICT --> QUAL["IMPLEMENTED<br/>coverage-qualified 202"]

    GEN --> CID["IMPLEMENTED<br/>candidate identity registry"]
    CID --> SKETCH["IMPLEMENTED<br/>two-set deterministic signal sketch"]
    SKETCH --> CLUSTER["IMPLEMENTED<br/>consensus cluster registry"]
    CLUSTER --> FUNNEL["IMPLEMENTED<br/>unified stage cluster funnel"]

    DEV["IMPLEMENTED<br/>development role"] --> STRICT
    SPENT["IMPLEMENTED<br/>spent validation/holdout"] --> LEDGER["IMPLEMENTED<br/>access + burn ledger"]
    SEALED["PLANNED<br/>sealed/challenge epoch"] --> LEDGER
    FWD["FROZEN<br/>2026 forward OOS"] --> LEDGER

    STRICT --> GUARD["IMPLEMENTED<br/>fail-closed train-only guard"]
    GUARD --> MEM["FROZEN<br/>memory + scheduler feedback"]
    MEM -. "no positive update in Phase A" .-> GEN

    SPENT -. "FORBIDDEN: candidate reward" .-> STRICT
    SPENT -. "FORBIDDEN: scheduler" .-> MEM
    SPENT -. "FORBIDDEN: family kill/freeze" .-> MEM
    SPENT -. "FORBIDDEN: spent → development" .-> DEV
    SPENT -. "FORBIDDEN: report-only → positive memory" .-> MEM
    FWD -. "FORBIDDEN: 2026 → search policy" .-> GEN
```

## Active boundaries

- Only fixed-calendar `development/train` coordinates may enter the signal
  sketch, reward, feedback, or scheduler inputs.
- Validation and holdout are spent. Their candidate-level results are forensic
  and report-only; they cannot kill, freeze, rank, schedule, or update memory.
- 2026 is separate forward OOS and remains sealed. No performance path is
  connected to search policy.
- Historical Phase3FIX is `PROVENANCE_UNVERIFIED`,
  `NON_REPRODUCIBLE_AS_EXECUTED`, and `NOT_VALID_FOR_PROOF`.
- Generation AST redundancy is observed. The stable signal-sketch audit found
  no significant signal-level collapse in generation through
  coverage-qualified transitions; this does not erase the separate structural
  redundancy finding.
- Signal sketches are diagnostic-only. They cannot create positive feedback or
  admit Phase B fields/events into the reward loop.
