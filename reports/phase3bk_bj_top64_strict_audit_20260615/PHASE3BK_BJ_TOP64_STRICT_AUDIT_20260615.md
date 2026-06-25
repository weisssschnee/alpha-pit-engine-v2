# Phase3BK BJ Top64 Strict Audit 2026-06-15

Decision: `PHASE3BK_BJ_TOP64_STRICT_AUDIT_COMPLETE_DIAGNOSTIC_ONLY`

## Coverage

- shortlist candidates: `64`
- aggregate expression-horizon rows: `32768`
- exact search-memory hits: `0`
- metric-vector clusters: `2`
- replay priority rows: `4`

## Boundary

- This consumes Phase3BJ true-1min strict-eval outputs.
- This is still diagnostic and does not modify X0/R3.
- This stage performs metric-vector reclustering, not final signal-vector reclustering.
- Final novelty requires materialized sampled signal vectors for selected candidates.

## Top Phase3BK Candidates

| rank | tier | h | factor | fields | score | dir | memory | family crowd | expression |
|---:|---|---:|---|---|---:|---:|---|---:|---|
| 1 | `bk_replay_priority` | 1 | `pure_fresh_cross` | `close|volume|vwap` | 0.0941374 | 1.00 | False | 1 | `CSRank(Sub(ZScore(Sub(ZScore(Delta($volume,15)),ZScore(Delta($vwap,15)))),ZScore(Sub(ZScore(Delta($volume,15)),ZScore(Delta($close,15))))))` |
| 2 | `bk_replay_priority` | 1 | `pure_fresh_cross` | `close|volume|vwap` | 0.0618205 | 1.00 | False | 1 | `CSRank(Div(ZScore(Sub(ZScore(Delta($volume,10)),ZScore(Delta($close,10)))),Add(Abs(ZScore(Sub(ZScore(Delta($volume,10)),ZScore(Delta($vwap,10))))),0.000001)))` |
| 3 | `bk_replay_priority` | 30 | `pure_fresh_cross` | `close|m1_first30_vwap|vwap` | 0.0563756 | 1.00 | False | 1 | `CSRank(Sub(ZScore(Div(Std($close,20),Add(Abs(Mean($close,20)),0.000001))),ZScore(Div(Sub($m1_first30_vwap,$vwap),Add(Abs($vwap),0.000001)))))` |
| 4 | `bk_replay_priority` | 5 | `opening_vwap_residual` | `close|m1_first30_vwap` | 0.0556099 | 1.00 | False | 3 | `CSRank(Div(Sub($m1_first30_vwap,$close),Add(Abs($close),0.000001)))` |
| 5 | `bk_watchlist` | 5 | `pure_fresh_cross` | `amount|close|m1_first30_vwap` | 0.0532237 | 1.00 | False | 2 | `CSRank(Div(ZScore(Div(Sub($m1_first30_vwap,$close),Add(Abs($close),0.000001))),Add(Abs(ZScore(Div(Delta($amount,5),Add(Abs(Add(Std($close,5),0.000001)),0.000001)))),0.000001)))` |
| 6 | `bk_watchlist` | 5 | `opening_vwap_residual` | `close|m1_first15_vwap` | 0.0525033 | 1.00 | False | 3 | `CSRank(Div(Sub($m1_first15_vwap,$close),Add(Abs($close),0.000001)))` |
| 7 | `bk_watchlist` | 5 | `pure_fresh_cross` | `amount|close|m1_first15_vwap|vwap` | 0.0519831 | 1.00 | False | 2 | `CSRank(Add(ZScore(Div(Sub($m1_first15_vwap,$close),Add(Abs($close),0.000001))),ZScore(Mul(ZScore(Delta($amount,2)),Neg(ZScore(Delta($vwap,2)))))))` |
| 8 | `bk_watchlist` | 5 | `pure_fresh_cross` | `amount|close|m1_first15_vwap|vwap` | 0.0519126 | 1.00 | False | 2 | `CSRank(Add(ZScore(Div(Sub($m1_first15_vwap,$close),Add(Abs($close),0.000001))),ZScore(Mul(ZScore(Delta($amount,10)),Neg(ZScore(Delta($vwap,10)))))))` |
| 9 | `bk_watchlist` | 30 | `pure_fresh_cross` | `amount|close|m1_first15_vwap|m1_first5_amount` | 0.0515324 | 1.00 | False | 1 | `CSRank(Sub(ZScore(Div($m1_first5_amount,Add(Abs(Mean($amount,30)),0.000001))),ZScore(Div(Sub($m1_first15_vwap,$close),Add(Abs($close),0.000001)))))` |
| 10 | `bk_watchlist` | 5 | `pure_fresh_cross` | `amount|close|m1_first30_vwap|m1_first5_vol` | 0.051424 | 1.00 | False | 1 | `CSRank(Div(ZScore(Div(Sub($m1_first30_vwap,$close),Add(Abs($close),0.000001))),Add(Abs(ZScore(Sub(ZScore($m1_first5_vol),ZScore(Mean($amount,30))))),0.000001)))` |
| 11 | `bk_watchlist` | 5 | `pure_fresh_cross` | `amount|close|m1_first30_vwap` | 0.0505205 | 1.00 | False | 1 | `CSRank(Sub(ZScore(Mul(ZScore(Delta($amount,10)),Neg(ZScore(Delta($close,10))))),ZScore(Div(Sub($m1_first30_vwap,$close),Add(Abs($close),0.000001)))))` |
| 12 | `bk_watchlist` | 30 | `micro_volatility` | `close` | 0.0503462 | 1.00 | False | 7 | `CSRank(Div(Std($close,10),Add(Abs(Mean($close,10)),0.000001)))` |
| 13 | `bk_watchlist` | 30 | `pure_fresh_cross` | `amount|close|vwap` | 0.050329 | 1.00 | False | 3 | `CSRank(Sub(ZScore(Mul(ZScore(Delta($amount,2)),Neg(ZScore(Delta($vwap,2))))),ZScore(Div(Std($close,8),Add(Abs(Mean($close,8)),0.000001)))))` |
| 14 | `bk_watchlist` | 30 | `micro_volatility` | `close` | 0.0503048 | 1.00 | False | 7 | `CSRank(Div(Std($close,15),Add(Abs(Mean($close,15)),0.000001)))` |
| 15 | `bk_watchlist` | 5 | `pure_fresh_cross` | `amount|close|m1_first15_vwap` | 0.0502945 | 1.00 | False | 1 | `CSRank(Div(ZScore(Div(Sub($m1_first15_vwap,$close),Add(Abs($close),0.000001))),Add(Abs(ZScore(Mul(ZScore(Delta($amount,15)),Neg(ZScore(Delta($close,15)))))),0.000001)))` |
| 16 | `bk_watchlist` | 30 | `micro_volatility` | `close` | 0.0499834 | 1.00 | False | 7 | `CSRank(Div(Std($close,20),Add(Abs(Mean($close,20)),0.000001)))` |
| 17 | `bk_watchlist` | 5 | `opening_vwap_residual` | `close|m1_first5_vwap` | 0.0499265 | 1.00 | False | 3 | `CSRank(Div(Sub($m1_first5_vwap,$close),Add(Abs($close),0.000001)))` |
| 18 | `bk_watchlist` | 30 | `micro_volatility` | `close` | 0.0497982 | 1.00 | False | 7 | `CSRank(Div(Std($close,8),Add(Abs(Mean($close,8)),0.000001)))` |
| 19 | `bk_watchlist` | 30 | `pure_fresh_cross` | `amount|close|m1_first30_vol` | 0.0495703 | 1.00 | False | 2 | `CSRank(Div(ZScore(Div(Std($close,5),Add(Abs(Mean($close,5)),0.000001))),Add(Abs(ZScore(Div($m1_first30_vol,Add(Abs(Mean($amount,30)),0.000001)))),0.000001)))` |
| 20 | `bk_watchlist` | 5 | `pure_fresh_cross` | `amount|close|m1_first30_vwap` | 0.0495663 | 1.00 | False | 1 | `CSRank(Add(ZScore(Sub(ZScore(Delta($amount,30)),ZScore(Delta($close,30)))),ZScore(Div(Sub($m1_first30_vwap,$close),Add(Abs($close),0.000001)))))` |

## Factor Lane Summary

| factor_lane | count | priority | best | mean | memory hits |
|---|---:|---:|---:|---:|---:|
| `pure_fresh_cross` | 53 | 3 | 0.0941374 | 0.0490382 | 0 |
| `opening_vwap_residual` | 3 | 1 | 0.0556099 | 0.0526799 | 0 |
| `micro_volatility` | 7 | 0 | 0.0503462 | 0.0491619 | 0 |
| `price_flow_divergence` | 1 | 0 | 0.0462673 | 0.0462673 | 0 |

## Next Gate

Run Phase3BL/Phase3BK-signal materialization for the priority set only:

1. materialize sampled per-minute signal vectors for priority candidates;
2. compare against frozen 149 / existing minute survivor signal caches;
3. rerun cost/turnover and wrong-lag checks;
4. only then discuss challenger or overlay tests.
