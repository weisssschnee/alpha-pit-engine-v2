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

materialized 供给又暴露了第二个、范围更窄的稳定瓶颈：State expression 已展开到
sidecar 中真实存在的 source leaves，但 admission filter 曾把只用于 registry lineage
的 synthetic state ID 误当成必须已有的物理列，造成 512/512 attempts 被错误拒绝。
filter 现只检查表达式实际引用的 `$field`；registry 声明和 compiler 校验仍完整保留。
因此本次 clamp 既不是单次小预算偶然，也不是 RegistryDrivenGenerator 的 route
authority 失效，而是“旧 constructor cycle 耗尽 + State materialization filter 假瓶颈”
两项局部、可修复问题。

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

修复仅增加三项 route-local admission 约束：

- generation attempt stream 按当前 field-sidecar schema 拒绝缺失字段，但不改变
  registry/compiler/scheduler authority；
- materialization availability 按表达式真实引用的 source leaves 判断，不再要求
  lineage-only synthetic state ID 已预先成为 sidecar 列；
- Disclosure 的 label-free condition-activation probe 最多使用 12 个开发日期，
  其余目标 route 仍使用 4 个 calendar-stratified 开发日期。

第一次四-route run 使这个 State filter 问题显性化：State 生成 0 pair，而 Minute
已得到 12/12 probe behavior-unique。该 partial run 没有启动 Phase3CM，也没有被
包装成通过；修复后以新的 immutable output root 重跑。

最终 bounded run 仍只生成每条目标 route 12 个 probe pair，并只选择 4 个 pair
进入 full-coordinate evaluation：

| Route | Attempts | Missing-field rejects | Unsupported rejects | Probe behavior-unique | Full-coordinate resolved / unique |
|---|---:|---:|---:|---:|---:|
| Disclosure Event | 221 | 16 | 83 | 5 | 4 / 4 |
| Market Regime Condition | 16 | 4 | 0 | 10 | 4 / 4 |
| Intraday State Transition | 16 | 3 | 0 | 12 | 4 / 4 |
| Minute Static | 61 | 44 | 0 | 12 | 4 / 4 |

16 个 full-coordinate rows 全部为 `RESOLVED`；每条 route 的 4 个 exact behavior
signature 均唯一，且每行同时保留非空的：

- `structural_family_id`
- `signal_cluster_id`
- `portfolio_behavior_signature_id`
- `portfolio_behavior_family_id`

## 77o Phase3CM result

Evaluation name: `full-coordinate development Phase3CM pair evaluation`

| Backend | Pairs | Status | Wall seconds | Result SHA256 |
|---|---:|---|---:|---|
| active_bar | 12 | completed | 538.62 | `8cd09bb760e4b8b90dead9b9f4e16d7f50a834e6c065e7fbba4f85cedd41e28f` |
| stock_session | 4 | completed | 8.63 | `f4fac37fd4319c03cc52e603dd5fdec4df83a24cef1f428b4b954df94541183f` |

Both backends completed 37 recoverable blocks. The run used source commit
`c939edfd041ed30a1529ebaf3a2cbccd0bc0d1f5` on `DESKTOP-77OPJ6F`.

## Boundaries and evidence

- `validation_reads=0`
- `holdout_reads=0`
- `forward_2026_reads=0`
- `promotion=FORBIDDEN`
- `strict_stage_a=NOT_AUTHORIZED`
- no alpha, economic-validity, OOS, or promotion conclusion

Authoritative local evidence mirror:
`runtime/cn_route_supply_closure_20260721_c939edf_4route/`.

All 34 compact-manifest artifacts and both backend receipt-bound result hashes
were recomputed after copying from 77o and matched exactly. The manifest binds
the source runtime root, host, and source commit. Key closure hashes:

- `final_decision.json`: `39a85ab5deb7606eb8169b28ca2aedd5877e729578d5cf40f4e12b87cf100f5b`
- `behavior_qualification.json`: `fae8b5b65304b83e257c9099fa3a32de65aeedfb7576b2a8aefbf16a5e7f322b`
- `clamp_root_cause.json`: `474bbaf9d81c576ede67b9b07463cbad6fb6a1efcaf3dd55077c81723163bce0`

Partial attempts remain on 77o at
`D:\ChengboRemote\runtime\cn_route_supply_closure_20260721_8e6e8cc` and
`D:\ChengboRemote\runtime\cn_route_supply_closure_20260721_1922842_4route`.
The authoritative passing run is
`D:\ChengboRemote\runtime\cn_route_supply_closure_20260721_c939edf_4route`.

## Next phase

Route supply no longer blocks the next system iteration. The next step is to
freeze a separate development campaign contract, fixed budgets, seeds, archive
snapshot, and feedback-on/off control before starting a larger development
search. That authorization is deliberately not inferred from this qualification.
The `registry_compositional_v2` constructor profile remains experimental and must
be explicitly accepted in that next campaign freeze; this closure does not promote
it to formal search authority.
Plate PIT minute materialization and compound-state expansion remain independent
localized gaps, not reasons to reopen the search platform.
