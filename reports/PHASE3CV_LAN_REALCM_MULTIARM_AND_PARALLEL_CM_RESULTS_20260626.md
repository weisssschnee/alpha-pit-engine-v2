# Phase3CV LAN Real-CM Multiarm Results 2026-06-26

## Scope

This record summarizes the LAN-node true1min real-CM search runs executed on
`DESKTOP-77OPJ6F` after the mature-prior/search-memory fixes.

The runs used true1min shards only:

```text
D:\ChengboRemote\workspace\alpha_pit_true1min_engine_20260624_182909_bea5eacbe591\runtime\phase3cs_true1min_sidecar_augmented_shards_zls4_20260624
```

Official X0/R3 remained read-only.

## Runs

| run | workspace | generated | CA rows | CM audited | CM mode | status |
|---|---|---:|---:|---:|---|---|
| `phase3cv_lan_multiarm_realcm_20260626` | `alpha_pit_true1min_engine_20260626_004718_7ee3d910c1ec` | 512 | 384 | 128 | serial | final |
| `phase3cvb_lan_event_ast_realcm_20260626` | `alpha_pit_true1min_engine_20260626_004718_7ee3d910c1ec` | 768 | 512 | 160 | serial | final |
| `phase3cvc_lan_open_fresh_realcm_20260626` | `alpha_pit_true1min_engine_20260626_004718_7ee3d910c1ec` | 1536 | 768 | 192 | serial | final |
| `phase3cvd_lan_deep_fresh_realcm_20260626` | `alpha_pit_true1min_engine_20260626_004718_7ee3d910c1ec` | 1024 | 512 | 96 | serial, deeper sample | final |
| `phase3cwe_lan_parallel_cm_realcm_20260626` | `alpha_pit_true1min_engine_20260626_011710_0f09110491be` | 768 | 512 | 128 | parallel CM, 4 workers | final |

Totals:

```text
generated: 4608
CA rows: 2688
real-CM audited: 704
followup-ready: 0
```

## Reward Results

| run | positive train_reward | followup-ready | best arm | best reward | best train sortino | best validation sortino | blocker |
|---|---:|---:|---|---:|---:|---:|---|
| `phase3cv` | 2 | 0 | `rx_ucb_fresh` | 0.192365 | 0.512543 | 0.233057 | `extreme_turnover` |
| `phase3cvb` | 2 | 0 | `rx_ucb_fresh` | 1.474727 | 2.287409 | -0.029868 | `extreme_turnover` |
| `phase3cvc` | 1 | 0 | `challenger_repair` | 0.371941 | 0.722741 | 1.065776 | `extreme_turnover` |
| `phase3cvd` | 1 | 0 | `challenger_repair` | 0.253584 | 0.568806 | 0.140861 | `extreme_turnover` |
| `phase3cwe` | 3 | 0 | `challenger_repair` | 0.359162 | 0.628739 | -0.039679 | `extreme_turnover` |

Interpretation:

```text
The search did not fail because every reward was negative.
It failed promotion because every attractive row was turnover-dominated.
The consistent blocker is extreme_turnover.
```

## Arm-Level Observations

`rx_ucb_fresh` produced the single highest train reward in this batch, but it
also produced high-turnover rejects. `challenger_repair` produced the best
validation-looking rows in open/deep/parallel runs, but those rows were also
blocked by turnover.

`typed_ast_fresh` had occasional positive rows but no followup-safe candidate.

`event_state` was weak in these mixed real-CM runs and often produced no usable
portfolio rows under the current expression/field path.

`random_orthogonal` remained useful as a novelty control, but did not produce a
competitive top reward.

## Engineering Result

`phase3cp_real_cm_small_loop` now supports:

```text
--cm-workers N
```

When `N > 1`, the CP loop:

```text
1. splits the CM candidate table by candidate rows;
2. launches parallel Phase3CM subprocess workers;
3. merges phase3cm_train_reward.csv and split-horizon summaries;
4. emits the same standard output filenames used by CN feedback.
```

Validation:

```text
py_compile: passed
CLI help exposes --cm-workers: passed
local merge fixture: PARALLEL_CM_MERGE_FIXTURE_OK 6 3
LAN parallel run: 4 chunk dirs, all final, merged 128 rows
```

Commit:

```text
0f09110 Add parallel CM reward workers to real loop
```

Synced LAN workspace:

```text
D:\ChengboRemote\workspace\alpha_pit_true1min_engine_20260626_011710_0f09110491be
```

## Decision

This batch is diagnostic-only.

```text
No candidate is promoted.
No X0/R3 change is allowed.
No positive reward row is accepted without turnover repair.
```

## Next Search Change

The next large search should not simply increase CEM/UCB budget. The immediate
failure mode is turnover. The next generator/reward change should add a
turnover-aware branch:

```text
1. low-turnover expression motifs;
2. pre-CM turnover proxy cap or penalty;
3. delayed / smoothed signal state variants;
4. challenger_repair and rx_ucb_fresh with turnover-aware mutation;
5. parallel CM workers as default on LAN/company machines.
```

This keeps the real-CM gate strict while avoiding spending most CM budget on
signals that are predictably blocked by `extreme_turnover`.
