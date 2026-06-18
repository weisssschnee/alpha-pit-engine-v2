# Phase3BZ To Phase3CA Search Definition 2026-06-16

## Phase3BZ Evidence

Phase3BZ was a formal true-1min fragment replay audit for BV/BX-generated candidates. It was not a production proof and not a full exchange fill simulation, but it is the first reward-like gate after the proxy algorithm bakeoff.

Inputs:

- local BZ: 24 candidates, 4 shards, 120 sampled trade_times per shard
- company BZ: 48 candidates, 8 shards, 160 sampled trade_times per shard
- source candidates: Phase3BX BV proxy Sortino/MCMC audit
- data: true `trade_time` 1min shards only
- cost proxy: 5 bps
- horizons: 1, 5, 15, 30 minutes

Results:

- local: 44,592 fragments, 0 followups, 24/24 `HOLD_FRAGMENT_REPLAY`
- company: 237,888 fragments, 0 followups, 48/48 `HOLD_FRAGMENT_REPLAY`
- best local fragment Sortino: -0.21346405
- best company fragment Sortino: -0.23032872
- day-block probability Sortino > 0: 0.0 for the top candidates

Interpretation:

The BV/BX proxy scores were not reliable promotion evidence. The strongest proxy candidates collapse under true-1min fragment replay, mostly through weak fragment MCMC, weak day-block MCMC, high turnover, inherited crowding, and wrong-lag blockers.

This does not prove true-1min alpha is absent. It does prove that the previous proxy-heavy search cannot be scaled as-is.

## Phase3CA Large Search Definition

Phase3CA should be a reward-gated true-1min search, not another algorithm bakeoff.

Primary objective:

- discover formulas that survive direct true-1min fragment replay, not merely high proxy IC or research-quality score

Hard constraints:

- use true `trade_time` 1min shards only
- do not use old 1D kline panels for minute-alpha claims
- keep X0/R3 read-only
- apply search memory before generation and before replay
- reject wrong-lag/future-signal leakage immediately
- require fragment replay after every checkpoint
- do not treat BV/BU/BX proxy Sortino as final reward

Generator allocation:

- 35% fresh AST / rx_typed_beam with strict novelty memory
- 20% CEM-led local search, but only with fragment-reward feedback
- 15% UCB/CEM hybrid allocation across families
- 15% repair of near-miss true-1min candidates, excluding inherited wrong-lag families
- 10% event/auction/high-board state candidates with event validation gates
- 5% pure random fresh tail for exploration

Reward vector:

- primary: fragment net Sortino
- primary: day-block MCMC median and probability Sortino > 0
- primary: signed IC stability by day block
- penalty: turnover and 5 bps cost fragility
- penalty: signal crowding / nearest-memory similarity
- hard blocker: wrong-lag stronger than valid lag
- hard blocker: future/timestamp/open-label leakage

Checkpoint gate:

- every 2,000 generated candidates
- select at most 128 for proxy eval
- replay at most 32 through fragment gate
- continue only if at least 1 candidate has positive fragment Sortino, positive MCMC median, and day-block prob > 0.55
- expand only if at least 2 candidates are new-vs-memory and not in the previous BV/BX collapsed families

Machine allocation:

- company machine: main generation + fragment replay, larger shard coverage
- local machine: lighter validation, report aggregation, and independent sanity/parity checks
- do not over-parallelize fragment replay beyond the I/O-safe limit without a parity check

Initial Phase3CA launch size:

- generated candidates: 20,000 to 40,000
- proxy-evaluated candidates: 1,024
- fragment replay candidates: 128
- shards: 8 to 12 initially, then expand only after checkpoint pass

Stop condition:

- if two consecutive checkpoints produce 0 positive fragment replay candidates, stop that generator family and reallocate budget to fresh or event-state lanes

Go / No-Go:

- BZ result is `NO-GO` for scaling BV/BX proxy winners directly
- BZ result is `GO` for Phase3CA only if reward gating is moved into the search loop
