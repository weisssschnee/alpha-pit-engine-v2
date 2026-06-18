# Alpha Pit True1min Engine Operating Rules

## Source Of Truth

1. Current user instruction.
2. This `AGENTS.md`.
3. `MIGRATION_MANIFEST.md`.
4. Run plans under `runtime/run_plans`.
5. Reports under `reports`.
6. Source code and command output.

## Repository Scope

This repository is for the true1min A-share chain only. It is not a general
legacy alpha workspace.

Allowed work:

- true `trade_time` 1min shard search
- typed primitive gate implementation and audits
- unsafe motif quarantine and blocked memory views
- guarded CEM/UCB/RX/AST search
- CA bridge and BZ fragment replay validation
- company/local launch scripts for this chain

Disallowed by default:

- old 1D kline search
- direct promotion of X0/R3
- proxy-score-only promotion
- deleting unsafe memory keys instead of blocking them
- importing bulk legacy runtime/report output without a manifest reason

## Execution Rules

- Set `PYTHONPATH=src` before running.
- Use `app.py` routes; do not add broad historical routes casually.
- Treat BZ fragment replay as the first reward-like gate.
- Treat CEM as an optimizer, not as reward-hacking defense.
- Keep company-machine heavy work isolated from crypto-line tasks.

## Data Boundary

Canonical true1min roots are external data assets and should not be committed.

```text
local:   runtime/phase3au_aq_only_true1min_sharded_20260611
company: runtime/phase3au_company_full_true1min_sharded_20260611
```
