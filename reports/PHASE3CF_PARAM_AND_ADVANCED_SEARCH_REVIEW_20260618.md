# Phase3CF Parameter And Advanced Search Review 2026-06-18

## Scope

This review checks Phase3CF true-1min search parameters and compares the current
implementation against mature CEM/UCB/QD patterns. It does not launch search.

## Current Implementation Diagnosis

Current Phase3CF is not a full quality-diversity system. It is closer to:

```text
RX/UCB candidate generation
  -> proxy minute evaluation
  -> blocker tagging
  -> CEM/UCB feedback update
  -> CA bridge
  -> optional BZ fragment replay
```

The latest guardrail patch fixed a major issue: wrong-lag and high-correlation
rows no longer update feedback and no longer enter the BZ bridge by default.

Remaining parameter problem: if too few clean rows survive, the system still has
configuration values sized for a large adaptive run. That creates a risk of
running CEM rounds with insufficient valid feedback.

## Evidence From Interrupted Phase3CF

Interrupted BS round:

```text
candidate_count: 256
decision_rows: 160
hard_blocked_ratio: 0.98125
feedback_eligible_after_guardrail: 1
```

Interrupted BU round:

```text
candidate_count: 266
decision_rows: 240
hard_blocked_ratio: 0.983333
feedback_eligible_after_guardrail: 2
```

Guarded CA bridge on old outputs:

```text
candidate_count: 6
hard_rejected_counts:
  future_signal_wrong_lag_too_strong: 275
  signal_corr_abs: 118
```

Interpretation: previous proxy ranking was strongly polluted. With guardrails,
the old outputs are not sufficient to support a large feedback-CEM update.

## Parameter Assessment

### CEM elite fraction

Current values:

```text
BS: 0.10 .. 0.16
BT: 0.10 .. 0.18
BU: 0.12 .. 0.24
```

Assessment:

```text
Reasonable for clean objective optimization.
Too exploitative when the objective is noisy and blocker-heavy.
```

Recommendation:

```text
Use an eligible elite floor, not only elite_frac.
Require >= 32 clean feedback rows before any adaptive CEM update.
If eligible rows < 32:
  disable feedback CEM
  run fresh / QD archive expansion instead
```

### Learning rate

Current:

```text
learning_rate: 0.55
```

Assessment:

```text
Too aggressive for noisy minute proxy feedback.
```

Recommendation:

```text
guarded smoke: 0.15 .. 0.25
company large: 0.20 .. 0.35 only if clean feedback count >= 64
otherwise: no adaptive update
```

### Entropy floor

Current:

```text
BS: 0.015
BT/BU base: 0.02
BU variants: 0.035 .. 0.06
```

Assessment:

```text
0.015 .. 0.02 is too low after observed mode collapse.
BU fresh variants are closer to acceptable.
```

Recommendation:

```text
guarded smoke: 0.06 .. 0.10
company large: 0.04 .. 0.08
pure CEM: require entropy floor >= 0.05
```

### Top decisions

Current:

```text
local interrupted: 160 / 240
planned company: 320
```

Assessment:

```text
Too large if blocker-first ranking is not active.
Acceptable after blocker-first ranking only if clean top share is monitored.
```

Recommendation:

```text
guarded smoke:
  top_decisions: 96 .. 160
  clean_top_share target >= 0.35

company large:
  top_decisions: 256 .. 320
  stop if clean_top_share < 0.20 after first round
```

### Sample trade times

Current:

```text
local: 64 .. 80
planned company search: 96
planned BZ: 180
```

Assessment:

```text
Adequate for screen-level proxy.
Too shallow for proof.
```

Recommendation:

```text
search proxy:
  96 .. 128 / shard

BZ fragment replay:
  180 .. 240 / shard

Do not increase search proxy depth until clean_top_share improves.
```

### Lane and fieldset caps

Current:

```text
RX lane_cap: 10% of max_candidates
CEM lane_cap: 12% of max_candidates
fieldset_cap: 4 or 5
```

Assessment:

```text
Useful but too local.
It caps exact lanes/fieldsets, not higher-level behavior cells.
```

Recommendation:

```text
Add behavior cells:
  primitive_family
  field_family
  horizon
  turnover_bucket
  blocker_status
  novelty_bucket
  event_vs_continuous

Keep one elite per cell before CEM feedback.
```

## Advanced Implementation Comparison

### CEM

The Cross-Entropy Method is suitable only when elite samples are trustworthy.
In noisy or adversarial proxy settings, elite selection must be guarded by
hard validity filters and sufficient eligible sample counts.

For Phase3CF, CEM should be disabled when clean feedback is sparse.

### UCB

Current UCB-like behavior scores historical feature/operator/lane reward, but
does not maintain per-arm uncertainty in a full bandit sense.

Recommendation:

```text
Track each emitter/lane arm with:
  n_trials
  clean_followup_rate
  BZ_pass_rate
  blocker_rate
  novelty_insert_rate

Use UCB on clean archive insertion / BZ followup, not raw proxy IC.
```

### MAP-Elites / Quality Diversity

The current system does not yet maintain a true archive where each behavior cell
stores one elite. Mature QD systems separate:

```text
archive: stores best solution per behavior cell
emitters: generate candidates
scheduler: allocates budget across emitters
```

Phase3CF should move toward this structure before another very large run.

Recommended minimal Phase3CG design:

```text
Archive cell dimensions:
  primitive_family
  field_family
  horizon_bucket
  turnover_bucket
  blocker_cleanliness
  novelty_bucket

Emitters:
  fresh_rx_ucb
  guarded_cem
  event_typed_primitive
  sidecar_fundamental_flow
  mutation_repair

Scheduler:
  allocate by UCB on clean archive insertions
  not raw proxy IC
```

## Recommended Next Run Shape

Do not resume Phase3CF large as-is.

First run a guarded smoke:

```text
candidate budget:
  BS seed: 192
  BT/BU fresh: 192
  adaptive CEM: disabled unless clean feedback >= 32

sample_trade_times_per_shard:
  96

max_shards:
  local 8 or company 12

top_decisions:
  128

success gate:
  clean_top_share >= 0.35
  feedback_eligible >= 16
  wrong_lag_top_share <= 0.05
  high_corr_top_share <= 0.35
```

If guarded smoke passes:

```text
company large:
  4 emitters
  512 candidates / emitter
  16 shards
  96-128 sample trade_times / shard
  CA bridge hard filter on
  BZ fragment replay top64 first, top128 only if clean pool is deep
```

If guarded smoke fails:

```text
Do not increase budget.
Build Phase3CG archive/emitter scheduler first.
```

## Bottom Line

The current parameter scale is not the main issue. The main issue is that
adaptive feedback was allowed before enough clean evidence existed.

After the guardrail patch, the correct policy is:

```text
fresh/QD exploration first
CEM only after sufficient clean feedback
BZ fragment replay before candidate language
```

