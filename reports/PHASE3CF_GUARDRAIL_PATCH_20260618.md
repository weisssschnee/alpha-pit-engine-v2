# Phase3CF Guardrail Patch 2026-06-18

## Decision

Phase3CF search should not be resumed with the previous feedback and bridge rules.
The interrupted BS/BU outputs showed that proxy-ranked true-1min search was still
dominated by wrong-lag and signal-crowded structures.

## Patch

Changed three guardrail points:

1. `phase3bs_adaptive_ucb_cem_practice.py`
   - CEM/UCB feedback now excludes rows with:
     - `future_signal_wrong_lag_too_strong`
     - `signal_corr_abs`
     - extreme turnover or weak/stale primary evidence
   - If no eligible rows remain, feedback is not updated from blocked rows.

2. `phase3bp_true1min_search_algorithm_smoke.py`
   - `top_decisions` are now sorted by blocker safety first.
   - Blocked high-IC rows remain diagnostic rows but should not occupy the front
     of the feedback/top queue.

3. `phase3ca_build_bz_candidate_audit.py`
   - CA bridge now hard-rejects `future_signal_wrong_lag_too_strong`.
   - CA bridge also hard-rejects high-correlation rows by default.
   - `--allow-high-corr` exists only as a diagnostic override.

## Verification

Compile check passed:

```text
python -m py_compile phase3bs_adaptive_ucb_cem_practice.py phase3bp_true1min_search_algorithm_smoke.py phase3ca_build_bz_candidate_audit.py
```

Guarded CA bridge rerun on existing interrupted Phase3CF outputs:

```text
source roots:
  reports/phase3cf_bs_adaptive_ucb_cem_20260618
  reports/phase3cf_bu_parallel_fresh_20260618

output:
  runtime/phase3cf_guarded_bz_candidate_audit_20260618
```

Result:

```text
candidate_count: 6
deduped_source_candidate_count: 6
hard_rejected_counts:
  future_signal_wrong_lag_too_strong: 275
  signal_corr_abs: 118
```

## Interpretation

This patch does not prove any alpha. It proves the previous queue was polluted:
after guardrails, only 6 candidates survived from the interrupted BS/BU outputs.

Next allowed action is a short guarded smoke run, not immediate full scale:

```text
1. run one guarded BS/BT or BU smoke
2. confirm top_decisions clean ratio improves
3. only then launch company-machine large search
4. BZ fragment replay remains mandatory before candidate language
```

