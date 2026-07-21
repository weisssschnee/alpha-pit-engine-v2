# CN Route Supply Closure — 77o bounded qualification

Date: 2026-07-21
Status: `CN_ROUTE_SUPPLY_CLOSURE_PASS`

## Decision

`ACTIONABLE_FEEDBACK_CLAMPED` 不是单纯由一次小预算随机波动造成。旧
`legacy_registry_v1` constructor 在历史 exact archive 之后，对
`INTRADAY_STATE_TRANSITION` 与 `MINUTE_STATIC` 呈现跨 seed、跨 attempt-cap
稳定的 exact-supply 耗尽；`DISCLOSURE_EVENT` 也有同类问题。瓶颈属于旧
constructor cycle，不属于 unified registry route authority。

保持 `RegistryDrivenGenerator` 为唯一生成 authority、保持 registry
`route_id` 为唯一顶层调度 key 后，route-local
`registry_compositional_v2` profile 已把七条 primary search route 全部恢复到
至少 2 倍 12-pair 门槛的 exact headroom。该修复没有建立第二 scheduler、
第二 compiler 或新搜索平台。

```text
LEGACY_CONSTRUCTOR_CYCLE_EXHAUSTION
REPAIRED_BY_REGISTRY_COMPOSITIONAL_PROFILE
BLOCKS_NEXT_DEVELOPMENT_PHASE = false
READY_FOR_SEPARATELY_FROZEN_DEVELOPMENT_CAMPAIGN
FORMAL_SEARCH = FROZEN
```

## Exact-supply ladder

数字为 1024-attempt cap 下两个固定 seed 的最终 exact-unique pair 数；分类门槛为
12 pairs，headroom 门槛为 24。

| Route | Legacy | Compositional V2 | Final classification |
|---|---:|---:|---|
| Disclosure Event | 0 / 0 | 110 / 115 | ready with headroom |
| FirstN Path | 301 / 301 | 718 / 716 | ready with headroom |
| Intraday State Transition | 0 / 0 | 80 / 80 | ready with headroom |
| Market Regime Condition | 147 / 147 | 176 / 176 | ready with headroom |
| Minute Static | 0 / 0 | 183 / 184 | ready with headroom |
| Slow Cross-Sectional Level | 22 / 22 | 524 / 531 | ready with headroom |
| Slow Temporal Change | 125 / 125 | 867 / 863 | ready with headroom |

`BROAD_EVENT_FROZEN_ENTRY` 不进入上述搜索供给统计。既有 11-mechanism entry
pack 继续作为 `FROZEN_REFERENCE_ONLY`，search budget 为零。

## Materialization-aware bounded repair

第一次 77o probe 保留为失败证据：Market 已有 8 个 behavior-unique pair，
Disclosure 只有 1 个。Disclosure 的 12 个 exact pair 中有 8 个引用了当前
stock-session sidecar 没有的字段，其余少量 pair 又受 4-date 坐标抽样限制。
这证明仅有 registry-level exact headroom 不等于当前 materialized searchable
supply。

修复仅增加两项 route-local admission 约束：

- generation attempt stream 按当前 field-sidecar schema 拒绝缺失字段，但不改变
  registry/compiler/scheduler authority；
- Disclosure 的 label-free condition-activation probe 最多使用 12 个开发日期，
  Market 仍使用 4 个 calendar-stratified 开发日期。

最终 bounded run 仍只生成每条目标 route 12 个 probe pair，并只选择 4 个 pair
进入 full-coordinate evaluation：

| Route | Attempts | Missing-field rejects | Unsupported rejects | Probe behavior-unique | Full-coordinate resolved / unique |
|---|---:|---:|---:|---:|---:|
| Disclosure Event | 221 | 16 | 83 | 5 | 4 / 4 |
| Market Regime Condition | 16 | 4 | 0 | 10 | 4 / 4 |

8 个 full-coordinate rows 全部为 `RESOLVED`，且每行同时保留非空的：

- `structural_family_id`
- `signal_cluster_id`
- `portfolio_behavior_signature_id`
- `portfolio_behavior_family_id`

## 77o Phase3CM result

Evaluation name: `full-coordinate development Phase3CM pair evaluation`

| Backend | Pairs | Status | Wall seconds | Result SHA256 |
|---|---:|---|---:|---|
| active_bar | 4 | completed | 277.85 | `cfec05b157c1bbff3e3ffa16692307fdc32b4c78f40edfb3e528c1c37c8eacc7` |
| stock_session | 4 | completed | 12.49 | `f13f904f7cd9090e3736565631a5db9d373c193fe2f808ee76640bb43ce3185c` |

Both backends completed 37 recoverable blocks. The run used source commit
`0c18916eceda45e33676c53f7efeccc010c131ca` on `DESKTOP-77OPJ6F`.

## Boundaries and evidence

- `validation_reads=0`
- `holdout_reads=0`
- `forward_2026_reads=0`
- `promotion=FORBIDDEN`
- `strict_stage_a=NOT_AUTHORIZED`
- no alpha, economic-validity, OOS, or promotion conclusion

Authoritative local evidence mirror:
`runtime/cn_route_supply_closure_20260721_0c18916/`.

The seven top-level manifest hashes and both backend result hashes were
recomputed after copying from 77o and matched exactly. Key closure hashes:

- `final_decision.json`: `e0a0205972b03dceee216d15ab613fd6fb821d99b70b02ccac6fb05cb3740baa`
- `behavior_qualification.json`: `9c154d5bfdaeb134340c86c1c90720e814cf018e9f602cb9831aca853002f89a`
- `clamp_root_cause.json`: `474bbaf9d81c576ede67b9b07463cbad6fb6a1efcaf3dd55077c81723163bce0`

The partial first attempt remains on 77o at
`D:\ChengboRemote\runtime\cn_route_supply_closure_20260721_8e6e8cc`; the passing
run is
`D:\ChengboRemote\runtime\cn_route_supply_closure_20260721_0c18916`.

## Next phase

Route supply no longer blocks the next system iteration. The next step is to
freeze a separate development campaign contract, fixed budgets, seeds, archive
snapshot, and feedback-on/off control before starting a larger development
search. That authorization is deliberately not inferred from this qualification.
Plate PIT minute materialization and compound-state expansion remain independent
localized gaps, not reasons to reopen the search platform.
