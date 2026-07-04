# Phase3DO Reward-Calibrated Fresh Search 2026-06-28

## Experiment Record

- date: 2026-06-28
- experiment_id: 20260628_phase3do_reward_calibrated_fresh_large_001
- objective: run a true1min fresh search with train portfolio Sortino plus rankIC loss as optimizer feedback, while keeping validation and holdout report-only
- status: running
- mode: heavy

### Inputs

- code workspace, local source: `G:\Project_V7_Rotation\alpha_pit_true1min_engine_20260619`
- local source HEAD: `6404ef72a0bb1993e574e2630dba52622dd9e2fa`
- remote compute workspace: `D:\ChengboRemote\workspace\alpha_pit_true1min_engine_20260627_222934_68d1d62eb94e_memfill`
- remote true1min shard root: `D:\ChengboRemote\data\phase3cy_true1min_sidecar_augmented_shards_20260626`
- remote arm budget table: `reports\phase3db_from_phase3da_lan77_partial512_20260627\phase3co_arm_budget_table.csv`
- remote memory roots include CE1 blocked view, DC/DD/DJ/DM prior train-reward loops, and `runtime\search_memory`

### Parameters

- generation_budget: 131072
- shortfall_fill_rounds: 6
- shortfall_oversample_multiplier: 3.0
- ca_top_n: 16384
- cm_candidate_limit: 1536
- cm_selection_mode: arm_balanced
- cm_max_shards: 6
- cm_sample_trade_times_per_shard: 384
- cm_horizons: `1,5,15,30,60`
- cm_workers: 12
- numexpr_threads: 2
- rank_ic_loss_weight: 6.0
- rank_ic_component_cap: 0.35
- reschedule_total_budget: 131072

### Commands

```text
powershell -ExecutionPolicy Bypass -File G:\Chengbo\tools\lan-remote\lan-remote.ps1 -Config G:\Chengbo\lan-ssh-config.chengbo-lan-77opj6f -HostAlias chengbo-lan-77opj6f -Action start-detached -RemoteJobRoot D:\ChengboRemote\runtime\jobs -DetachedCommand "powershell -NoProfile -ExecutionPolicy Bypass -File D:\ChengboRemote\runtime\lan77_phase3do_reward_calibrated_fresh_large_20260628.ps1"
```

### Remote Task

- task_id: `lanjob_20260628_222603_17e7d5`
- log: `D:\ChengboRemote\runtime\jobs\lanjob_20260628_222603_17e7d5.log`
- status: running at first checkpoint
- first checkpoint:
  - generated rows: 131072
  - CA rows: 16384
  - CM candidate rows: 1536
  - current stage: 12-way CM train reward evaluation
  - memory state: near full physical memory on DESKTOP-77OPJ6F, no extra CN load should be added until chunks complete

### Parallel Remote Task

- experiment_id: `20260628_phase3do_b_feedback_fresh_parallel_002`
- objective: run a smaller parallel fresh search from the latest Phase3DM feedback budget table while Phase3DO main is in its single-process generation phase
- task_id: `lanjob_20260628_223352_42d4e8`
- log: `D:\ChengboRemote\runtime\jobs\lanjob_20260628_223352_42d4e8.log`
- status: stopped intentionally after main Phase3DO entered 12-worker CM stage, to avoid OOM/pagefile collapse
- remote runtime root: `runtime\phase3do_b_feedback_fresh_parallel_20260628`
- remote report root: `reports\phase3do_b_feedback_fresh_parallel_20260628`
- generation_budget: 65536
- cm_candidate_limit: 768
- cm_max_shards: 4
- cm_sample_trade_times_per_shard: 256
- cm_workers: 8

### Local Companion Task

- experiment_id: `20260629_phase3do_c_local_light_fresh_prep_003`
- objective: use local CPU for low-memory fresh candidate generation and CA/feedback prep while 77o completes heavy CM reward
- boundary: not final reward; no heavy true1min CM evaluation; uses true1min schema only to avoid old 1D drift
- local script: `G:\Chengbo\runtime\local_phase3do_c_light_fresh_prep_20260629.ps1`
- local log root: `G:\Chengbo\runtime\phase3do_c_local_light_fresh_prep_20260629_logs`
- local runtime root: `runtime\phase3do_c_local_light_fresh_prep_20260629`
- local report root: `reports\phase3do_c_local_light_fresh_prep_20260629`
- smoke_total_candidates: 16384
- ca_top_n: 4096
- schema_max_shards: 3
- started: 2026-06-29 03:18 local time
- resource guard: started only because it is single-process and low-memory; do not start local CM reward while available memory is around 3GB

### Local Companion Task 2

- experiment_id: `20260629_phase3do_d_local_light_fresh_prep_004`
- objective: expand local low-memory fresh generation from Phase3DO-C feedback budget while 77o continues heavy CM reward
- boundary: not final reward; no heavy true1min CM evaluation; uses true1min schema only
- local script: `G:\Chengbo\runtime\local_phase3do_d_light_fresh_prep_20260629.ps1`
- local log root: `G:\Chengbo\runtime\phase3do_d_local_light_fresh_prep_20260629_logs`
- local runtime root: `runtime\phase3do_d_local_light_fresh_prep_20260629`
- local report root: `reports\phase3do_d_local_light_fresh_prep_20260629`
- smoke_total_candidates: 32768
- ca_top_n: 8192
- status: completed
- result:
  - generated rows: 32768
  - CA rows: 8192
  - feedback rows: 8192
  - CA arm mix: typed_ast_fresh 4968, rx_ucb_fresh 2780, challenger_repair 444
  - next fresh share: 0.66801453
  - next CEM budget: 3932 / 65536

### Outputs

- remote runtime root: `runtime\phase3do_reward_calibrated_fresh_large_20260628`
- remote report root: `reports\phase3do_reward_calibrated_fresh_large_20260628`
- first manifest: `reports\phase3do_reward_calibrated_fresh_large_20260628\phase3do_run_manifest.json`
- parallel manifest: `reports\phase3do_b_feedback_fresh_parallel_20260628\phase3do_b_run_manifest.json`

### Metrics

- generation checkpoint:
  - `all_generated_rows`: 131072
  - `ca_rows`: 16384
  - `cm_candidate_rows`: 1536
  - `accepted_to_emitted_unexplained`: 0 in the first accounting read
  - `reject_memory_expr`: 0 in the first accounting read
  - dominant rejection families: unsafe skeleton and typed gate, not parse errors
- reward not available yet; expected checkpoints:
  - generation accounting: `phase3cp_real_cm_generation_attempts.csv`
  - candidate audit: `phase3cp_real_cm_candidate_audit.csv`
  - train reward table: `phase3cm_train_reward\phase3cm_train_reward.csv`
  - summary: `phase3cp_real_cm_small_loop_summary.json`

### 2026-06-29 08:31 Checkpoint

- 77o main task still running; final status file not written.
- 77o memory is tight, around 0.23GB free physical memory at probe time.
- Remote reward progress:
  - generated rows: 131072
  - CA rows: 16384
  - CM candidate rows: 1536
  - all 12 chunks have 128 partial rows
  - chunk_10 is final
  - most other chunks have completed shard 2 of 3; remaining work is final shard/merge
- Local companion D completed; no further local proxy-only expansion launched pending 77o real reward final.

### 2026-06-29 11:05 Final Collection

- 77o main task completed normally.
- task_id: `lanjob_20260628_222603_17e7d5`
- exit_code: 0
- started_at: `2026-06-28T22:26:12`
- ended_at: `2026-06-29T09:57:40`
- remote python process count after completion: 0
- 77o free physical memory after completion: about 82.95GB
- final report root: `D:\ChengboRemote\workspace\alpha_pit_true1min_engine_20260627_222934_68d1d62eb94e_memfill\reports\phase3do_reward_calibrated_fresh_large_20260628`

Final scale:

- generated candidates: 131072
- CA candidates: 16384
- train reward rows: 1536
- optimizer reward metric: `train_portfolio_sortino_rankic_composite_reward`
- horizons: `1,5,15,30,60`
- max shards: 6
- sampled trade times per shard: 384
- parallel CM workers: 12
- followup_count: 0
- final decision count: `HOLD_TRAIN_REWARD` 1536 / 1536

Reward distribution:

- positive optimizer reward: 42 / 1536
- positive train day Sortino: 38 / 1536
- positive validation and holdout day Sortino: 23 / 1536
- positive train, validation, and holdout day Sortino: 14 / 1536
- train reward max: 0.759386
- train day Sortino max: 1.547600
- train rank IC mean max: 0.249915
- validation day Sortino max: 3.660559
- holdout day Sortino max: 1.594619

Blocker accounting:

- `non_positive_worst_horizon_train_sortino`: 1526
- `weak_train_day_mcmc`: 1506
- `non_positive_train_day_sortino`: 1498
- `non_positive_train_reward`: 1494
- `no_valid_train_rank_ic`: 1027
- `extreme_turnover`: 421
- inherited blockers: empty for all 1536 rows

Top optimizer reward rows:

| candidate_id | arm | optimizer_reward | train_day_sortino | train_rank_ic_mean | validation_day_sortino | holdout_day_sortino | train_turnover | worst_horizon_train_sortino | blockers |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| phase3cp_118990 | typed_ast_fresh | 0.759386 | 0.970971 | 0.099672 | 0.361480 | 0.651369 | 0.821883 | 0.012635 | extreme_turnover |
| phase3cp_35506 | typed_ast_fresh | 0.726577 | 0.772226 | 0.100009 | -0.220845 | -0.448186 | 0.804370 | 0.215734 | extreme_turnover |
| phase3cp_35598 | typed_ast_fresh | 0.653502 | 0.702254 | 0.105847 | 0.375501 | -0.479471 | 0.827068 | 0.172270 | extreme_turnover |
| phase3cp_98027 | turnover_aware_fresh | 0.645562 | 1.547600 | 0.249915 | 2.208193 | 0.000000 | 0.937500 | -0.114994 | non_positive_worst_horizon_train_sortino, extreme_turnover |
| phase3cp_32989 | typed_ast_fresh | 0.621578 | 0.628530 | 0.103093 | 0.307403 | 0.109645 | 0.804936 | 0.222726 | extreme_turnover |
| phase3cp_36032 | typed_ast_fresh | 0.550902 | 0.568621 | 0.104041 | 1.964482 | 0.723907 | 0.802180 | 0.051088 | extreme_turnover |
| phase3cp_38577 | typed_ast_fresh | 0.501406 | 0.658256 | 0.085905 | 3.660559 | 1.554236 | 0.792552 | -0.350307 | non_positive_worst_horizon_train_sortino, extreme_turnover |

Interpretation:

- The run did not fail operationally. It completed and released resources.
- The new reward loop is no longer purely proxy-driven: reward includes train Sortino plus rankIC, with validation and holdout report-only.
- The main issue is not zero signal. There are positive train/validation/holdout rows, but nearly all attractive rows are high-turnover and/or fail worst-horizon train stability.
- No candidate should be promoted from this run. The useful output is a family-level direction: typed AST fresh is producing most of the interesting rows, while the next step should reduce turnover and worst-horizon fragility before scaling.

### Cost and Time

- estimated: multi-hour on DESKTOP-77OPJ6F
- actual: about 11h31m wall-clock on DESKTOP-77OPJ6F

### Reproducibility

- reproducible: partial
- blocker: remote workspace is a patched compute snapshot because GitHub push failed over HTTPS; local source commit is recorded, and the patched source files were compiled remotely before launch

### Decision

HOLD_RESEARCH.

Reason: Phase3DO produced nonzero reward pockets, but all 1536 reward rows are `HOLD_TRAIN_REWARD`; dominant blockers are worst-horizon instability, weak train MCMC, non-positive train reward, missing/invalid rankIC for many rows, and extreme turnover in the best rows.

### Next Action

- Build the next search around turnover control and worst-horizon robustness, not around simply increasing raw candidate count.
- Keep CEM exploit capped until at least two clean train-reward families exist.
- Use typed AST fresh as the main exploration lane, but add lower-turnover variants and horizon-stability penalties.
- Do not add a company CN job while crypto A7 search is active on DESKTOP-7877972.
