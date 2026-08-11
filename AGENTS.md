# Alpha Pit True1min Engine Operating Rules

## Source Of Truth

1. Current user instruction.
2. This `AGENTS.md`.
3. `.planning/PROJECT.md` for the accepted project mission.
4. `.planning/STATE.md` for current progress, boundaries, blockers, and next action.
5. Accepted run plans under `runtime/run_plans` and architecture decisions.
6. Runtime behavior, tests, source, and generated artifacts as observed reality.
7. Reports and historical planning claims.

Architecture authority is projected from `config/architecture_overlay.json` into
`.planning/graphs/current.json`. RAW `.planning/graphs/graph.json` is for source
navigation and is not current-state authority.

## Repository Scope

This repository is for the true1min A-share chain only. It is not a general
legacy alpha workspace.

Allowed work:

- true `trade_time` 1min shard search
- typed primitive gate implementation and audits
- unsafe motif quarantine and blocked memory views
- guarded CEM/UCB/RX/AST search
- CA bridge, BZ fragment diagnostic replay, and CM train portfolio Sortino reward audit
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
- High-cost freeze, launch, successor, retry, and technical-recovery actions
  must enter through an `app.py` route that consumes a Project Control
  admission bound to the exact project, repo SHA, action, campaign and run.
  Calling an underlying script directly is not authorization for a new
  high-cost execution. Project Control governs admission only; it cannot
  select alpha or overrule observed economic results.
- Treat BZ fragment replay as a diagnostic slice replay, not the search reward.
- Treat the qualified Phase3CM streaming evaluator as engineering capability,
  not authorization for strict Stage A or formal search.
- Treat CEM as an optimizer, not as reward-hacking defense.
- Keep company-machine heavy work isolated from crypto-line tasks.
- The complete field-universe authority is
  `runtime/field_registry/cn_field_master_registry_v1/cn_field_master_registry_v1.json`.
  Presence there does not grant generator exposure; route eligibility remains
  owned by the unified capability registry.

## Data Boundary

Canonical true1min roots are external data assets and should not be committed.

```text
local:   runtime/phase3au_aq_only_true1min_sharded_20260611
company: runtime/phase3au_company_full_true1min_sharded_20260611
```
