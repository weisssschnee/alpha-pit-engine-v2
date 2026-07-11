# Evolution Map

Updated: 2026-07-11

| Change | Previous state | Current state / consequence |
|---|---|---|
| Shard-local split leakage discovered | Each worker/shard could derive its own temporal boundaries; atom labels conflicted with the global calendar | Fixed 485-date manifest is authoritative for every shard and recovery path; raw atom labels are diagnostic and conflicts are counted |
| Pre-fix OOS downgraded | Validation/holdout were described as report-only OOS despite candidate-level reports and human canary selection | Both are formally `spent`; they are not reusable OOS and cannot affect future candidate distributions |
| High-frequency CM/validation feedback withdrawn | Reward-integrated search could repeatedly expose validation/holdout candidate metrics to human and automated decisions | Candidate-level non-development fields fail at memory, search-feedback, and scheduler boundaries; report-only outputs cannot create positive memory |
| Reward-integrated search → sealed-epoch governance | Continuous reward/memory/scheduler loop with weak evaluation access accounting | Development-only discovery is separated from future sealed/challenge epochs; access and burn ledgers record exposure before use |
| Historical Phase3FIX downgraded | Key-file hash agreement was at risk of being treated as run reproduction | `PROVENANCE_UNVERIFIED`, `NON_REPRODUCIBLE_AS_EXECUTED`, `NOT_VALID_FOR_PROOF`; no infinite historical replay |
| Generation AST redundancy observed | 24,576 nominal expressions could be misread as independent hypotheses | 131 skeletons and skeleton N_eff 33.55 prove structural redundancy only; the later signal audit did not locate a significant signal-level collapse |
| Full-panel replay replaced by two-pass sketches | 384 semantic-only canary exceeded 120 seconds and retained full candidate Series | Two independent deterministic development-coordinate sketches completed for 24,576 candidates; all fidelity gates passed and the five-stage cluster mapping found no downstream concentration amplification |
| Phase B hypothesis redesign frozen | Feature/event/state/industry lanes existed as canaries and historical routes | No new feature/state/event/benchmark lane may enter reward until Phase A architecture acceptance |

## Active lineage

```text
shard-local split repair
  -> evaluation role reset and OOS burn ledger
  -> fail-closed feedback boundaries
  -> deterministic signal-sketch fidelity audit
  -> unified cluster funnel
  -> Phase A architecture acceptance
  -> only then Phase B hypothesis-space reconstruction
```
