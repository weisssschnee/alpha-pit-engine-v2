# Phase3CW LAN Reward Gate Results 2026-06-26

Decision: `HOLD_RESEARCH_WITH_2_FOLLOWUP_READY_DIAGNOSTICS`

This report records the first LAN run after adding the turnover-aware true1min search arm and pre-CM turnover proxy gate.
It is diagnostic-only. It does not modify X0/R3, official books, or promotion gates.

## Code and Runtime

- code commit: `d38bbd0 Add turnover-aware true1min search arm`
- remote node: `DESKTOP-77OPJ6F`
- remote workspace: `D:\ChengboRemote\workspace\alpha_pit_true1min_engine_20260626_021103_d38bbd034757`
- true1min shard root: `D:\ChengboRemote\workspace\alpha_pit_true1min_engine_20260624_182909_bea5eacbe591\runtime\phase3cs_true1min_sidecar_augmented_shards_zls4_20260624`
- shard schema: 4 shards, 86 columns, `trade_time/amount/close/volume` present
- Python: `D:\ChengboRemote\venvs\alpha311\Scripts\python.exe`

## Runs

| run | job id | candidates generated | CA rows | CM audited | CM workers | result |
|---|---|---:|---:|---:|---:|---|
| Phase3CW turnover-aware | `lanjob_20260626_021707_4abf11` | 2048 | 1024 | 256 | 4 | 0 followup |
| Phase3CW2 wide fresh | `lanjob_20260626_021934_08217e` | 4096 | 2048 | 384 | 6 | 2 followup |

Both jobs exited with code `0`.

## Guardrails

- true1min shard path only; no old 1D root.
- sidecar augmented shard root preferred.
- `pre_cm_turnover_proxy_max` applied before real CM.
- CEM kept probe-only at 2 percent budget.
- final reward gate is real Phase3CM train reward, not proxy IC.
- lineage consistency passed for both runs.

## Final Results

### Phase3CW

```text
summary_decision: PHASE3CP_REAL_CM_SMALL_LOOP_PASS_DIAGNOSTIC_ONLY
cm_candidate_count: 256
cm_followup_count: 0
cn_candidate_count: 256
lineage_consistent: true
```

Best rows remained blocked by `extreme_turnover`.

```text
typed_ast_fresh best:
  reward: 0.36081712
  train_day_sortino: 0.62873905
  validation_day_sortino: -0.0396787
  train_mean_one_way_turnover: 0.78847621
  decision: HOLD_TRAIN_REWARD
  blockers: extreme_turnover
```

### Phase3CW2

```text
summary_decision: PHASE3CP_REAL_CM_SMALL_LOOP_PASS_DIAGNOSTIC_ONLY
cm_candidate_count: 384
cm_followup_count: 2
cn_candidate_count: 384
lineage_consistent: true
```

Followup-ready diagnostics:

```text
candidate: phase3cp_03445
arm: challenger_repair
expression: Neg(CSRank(MaskedZScore($ctx_ths_hot_last_price,60,0.8)))
hash: f3bc9698450d8d8b0e9781c3
train_reward: 0.37966456
train_day_sortino: 0.476396
validation_day_sortino: -0.29240887
train_mean_one_way_turnover: 0.02161538
decision: TRAIN_REWARD_FOLLOWUP_READY

candidate: phase3cp_03454
arm: challenger_repair
expression: Neg(CSRank(MaskedZScore($ctx_ths_hot_last_pct,60,0.8)))
hash: d414a051a71e40edc9e2896e
train_reward: 0.33203298
train_day_sortino: 0.4100467
validation_day_sortino: -0.25258872
train_mean_one_way_turnover: 0.03816654
decision: TRAIN_REWARD_FOLLOWUP_READY
```

## Interpretation

The new turnover-aware arm did not produce followup-ready rows in this run.
Its best final reward remained negative and was blocked by weak train sortino and MCMC evidence.

The useful discovery is narrower: `challenger_repair` produced two very low-turnover followup-ready diagnostics from THS hot context fields.
This is not promotion evidence. Both rows have negative validation day Sortino, so they must be treated as followup diagnostics only.

The practical implication is that the previous all-high-turnover failure was not the only accessible mode.
The system can now surface low-turnover candidates under the real CM reward gate, but the active winning family is not yet proven stable.

## Required Next Checks

1. PIT/observable-time audit for `ctx_ths_hot_last_price` and `ctx_ths_hot_last_pct`.
2. Same-field shuffle and wrong-lag controls for both expressions.
3. Larger split replay with stricter validation penalty.
4. Compare against coverage-mask placebo because THS hot fields may encode availability or attention coverage.
5. If those pass, run a focused repair lane around THS hot context with capped turnover and no event-state leakage.

## Artifacts

Remote result pack copied under:

```text
reports/phase3cw_lan_reward_gate_results_20260626/remote_result_pack/
```

Key files:

```text
remote_result_pack/phase3cw_lan_turnover_aware_realcm_20260626/phase3cm_train_reward.csv
remote_result_pack/phase3cw_lan_turnover_aware_realcm_20260626/phase3cp_real_cm_small_loop_summary.json
remote_result_pack/phase3cw2_lan_wide_fresh_realcm_20260626/phase3cm_train_reward.csv
remote_result_pack/phase3cw2_lan_wide_fresh_realcm_20260626/phase3cp_real_cm_small_loop_summary.json
```
