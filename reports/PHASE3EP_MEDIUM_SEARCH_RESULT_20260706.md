# Phase3EP Medium Search Result

Date checked: 2026-07-06

## Runtime Status

- Machine: `DESKTOP-77OPJ6F`
- Task id: `lanjob_20260705_165934_2f731c`
- Status: completed
- Exit code: `0`
- Started: `2026-07-05T16:59:39`
- Ended: `2026-07-05T17:37:57`
- Current Python process count on 77O after completion: `0`

## Inputs

- Remote repo: `D:\ChengboRemote\workspace\alpha_pit_true1min_engine_20260627_222934_68d1d62eb94e_memfill`
- Shard root: `D:\ChengboRemote\data\phase3dz_true1min_sidecar_augmented_full16_20260702`
- Output root: `runtime\phase3ep_medium_search_20260705_77o`
- Search budget: `4096`
- CA top-n: `384`
- CM candidate limit: `128`
- CM max shards: `6`
- CM sample trade times per shard: `128`
- Event-aware sampling: enabled
- Reward metric: `train_portfolio_sortino_rankic_regime_composite_reward`
- Portfolio mode: long-only top, shorting disabled
- Validation / holdout: report-only, not optimizer input

## Gate Results

Pre-CM semantic viability gate:

| metric | value |
|---|---:|
| input candidates | 320 |
| passed | 209 |
| rejected | 111 |
| kept for CM | 128 |

By arm:

| arm | input | passed | rejected |
|---|---:|---:|---:|
| `cem_exploit` | 53 | 30 | 23 |
| `challenger_repair` | 53 | 36 | 17 |
| `event_state` | 53 | 45 | 8 |
| `random_orthogonal` | 53 | 20 | 33 |
| `rx_ucb_fresh` | 54 | 30 | 24 |
| `typed_ast_fresh` | 54 | 48 | 6 |

Semantic block memory was written to:

```text
runtime\search_memory\phase3cp_semantic_blocks\phase3ep_medium_search_20260705_77o_semantic_block_memory.csv
```

## Reward Results

CM reward audit:

| metric | value |
|---|---:|
| candidates evaluated | 128 |
| followup-ready | 28 |
| hold/train-held | 100 |
| selected shards | 6 |
| reward atom rows | about 56k |

Followup OOS distribution:

| condition | count |
|---|---:|
| followup-ready | 28 |
| validation Sortino > 0 | 25 |
| holdout Sortino > 0 | 13 |
| validation and holdout both > 0 | 12 |

## Top Train Reward Candidates

The train-top candidate is not the best robust candidate because validation/holdout degrade.

| rank | candidate | arm | train reward | train sortino | validation sortino | holdout sortino | expression |
|---:|---|---|---:|---:|---:|---:|---|
| 1 | `phase3cp_02404` | `challenger_repair` | 1.0264 | 1.3685 | -0.0482 | -0.5004 | `CSRank(MaskedZScore($ctx_holder_avg_market_cap,20,0.05))` |
| 2 | `phase3cp_03400` | `event_state` | 0.8214 | 1.1241 | 1.0704 | -0.0334 | `CSRank(Add(CSRank(WindowStateCount($evt_uplimit_up_limit_keep_times,10)),CSRank(MaskedCorr($ctx_sent_ditian_num,$vwap,20,0.6))))` |
| 3 | `phase3cp_01396` | `typed_ast_fresh` | 0.8156 | 1.0927 | 1.0388 | 0.0111 | `CSRank(MaskedCorr($ctx_sent_ditian_num,$vwap,20,0.8))` |
| 4 | `phase3cp_03247` | `event_state` | 0.7850 | 1.0720 | 0.9151 | 0.0194 | `CSRank(Mul(CSRank(WindowStateCount($evt_uplimit_up_limit_keep_times,10)),CSRank(MaskedCorr($ctx_sent_ditian_num,$vwap,40,0.8))))` |

## More Robust Followup Set

Ranking by train reward plus the weaker of validation/holdout favors:

| rank | candidate | arm | train reward | validation sortino | holdout sortino | expression |
|---:|---|---|---:|---:|---:|---|
| 1 | `phase3cp_01184` | `typed_ast_fresh` | 0.4209 | 1.8567 | 0.5542 | `CSRank(MaskedCorr($ctx_sent_lb_h_num,$vwap,20,0.8))` |
| 2 | `phase3cp_02594` | `challenger_repair` | 0.6241 | 0.2157 | 0.5049 | `CSRank(MaskedCorr($ctx_sent_damian_num,$amount,20,0.6))` |
| 3 | `phase3cp_01396` | `typed_ast_fresh` | 0.8156 | 1.0388 | 0.0111 | `CSRank(MaskedCorr($ctx_sent_ditian_num,$vwap,20,0.8))` |
| 4 | `phase3cp_03247` | `event_state` | 0.7850 | 0.9151 | 0.0194 | `CSRank(Mul(CSRank(WindowStateCount($evt_uplimit_up_limit_keep_times,10)),CSRank(MaskedCorr($ctx_sent_ditian_num,$vwap,40,0.8))))` |
| 5 | `phase3cp_02342` | `challenger_repair` | 0.2743 | 0.2261 | 0.4697 | `CSRank(Sub(CSRank(WindowStateCount($evt_uplimit_up_limit_keep_times,10)),CSRank(MaskedCorr($ctx_sent_lb_h_num,$vwap,20,0.8))))` |
| 6 | `phase3cp_02618` | `challenger_repair` | 0.2926 | 0.1439 | 0.1917 | `CSRank(MaskedCorr($ctx_rzrq_rzjme,$amount,20,0.4))` |

## Arm Readout

| arm | evaluated | followup-ready | best train reward |
|---|---:|---:|---:|
| `cem_exploit` | 18 | 13 | 0.7553 |
| `challenger_repair` | 22 | 7 | 1.0264 |
| `event_state` | 28 | 2 | 0.8214 |
| `random_orthogonal` | 12 | 4 | 0.4129 |
| `rx_ucb_fresh` | 17 | 0 | 0.4572 |
| `typed_ast_fresh` | 31 | 2 | 0.8156 |

## Interpretation

This run is a meaningful improvement over the collapsed proxy/fragment line:

1. It used true1min shard root and blocked suspicious 1D paths.
2. It used long-only top portfolio mode with shorting disabled.
3. The semantic gate is useful but not over-tight: it rejected 111 / 320 and still left 128 CM candidates.
4. The best train candidate does not generalize; next selection should not chase train top1.
5. The strongest repeated economic motif is market sentiment / limit-board breadth interacting with `vwap` or `amount`.
6. Holder/fundamental sparse fields produced some train winners, but the top holder-market-cap candidate failed holdout.
7. `rx_ucb_fresh` underperformed this run; `typed_ast_fresh`, `challenger_repair`, and event-context formulas deserve the next validation budget.

## Next Action

Run a Phase3EQ focused validation on the robust followup set:

- top 12 validation-and-holdout-positive followups
- all 16 shards if available, or at least 12 shards
- larger trade-time sample per shard
- same long-only / no-short contract
- no optimizer feedback from holdout
- output a source-family and expression-family attribution report

Do not expand broad search until these candidates are checked for multi-shard stability.
