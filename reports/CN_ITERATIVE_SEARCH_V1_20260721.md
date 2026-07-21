# CN Iterative Search V1 — 77o CANARY Closure

Date: 2026-07-21
Status: `CN_ITERATIVE_SEARCH_V1_CANARY_PASS`

## Decision

```text
CONTRACT_ACCEPTABLE_WITH_TARGETED_AMENDMENTS
REUSE_EXISTING_REPOSITORY_AND_GRAPH_AUTHORITY
NO_NEW_SEARCH_PLATFORM
CN_ITERATIVE_SEARCH_V1_CANARY_PASS
```

V1 已按“route 预算适配 + behavior-aware admission”的边界落地并在 77o
完成三批真实 CANARY。该结论只证明受限开发搜索的调度、行为去重、反馈因果
对照与恢复闭环可运行，不构成 alpha、经济有效性、Strict Stage A、OOS 或晋级
证据。

## Contract closure

- 顶层调度 authority 是 unified registry `route_id`；`generation_mode` 只是
  route 内 action label，`DEFAULT_ARM_PROFILES` 仅兼容旧接口。
- 身份分为 `structural_family_id`、`signal_cluster_id`、精确
  `portfolio_behavior_signature_id` 与近似 `portfolio_behavior_family_id`。
  admission 前只使用 label-free bounded `behavior_probe_id`；无法解析时为
  `BEHAVIOR_UNRESOLVED`，不回退 structural family。
- full-coordinate behavior 只由 support、selection、weights、turnover 等组合行为
  生成，不使用 return、reward、RankIC 或 cost outcome。
- 每批关闭时写入不可变 `batch_manifest.json`；根目录 Parquet 只是最终累计投影。
  恢复复用原 Phase3CM checkpoint/complete-result 机制，没有建立第二套 checkpoint。
- Feedback-on/off 共享 seed、master attempt stream、总预算、历史 behavior archive、
  exact/behavior dedupe、registry 与 compiler；唯一允许差异是 feedback 消费及其
  adaptive route schedule。
- synthetic 正负规则均通过；真实 CANARY 只对有 actionable support 的方向动作。
  `FREEZE` 未用于小样本收益、成本或不稳定结论；基础设施失败只进入 run health。
- full materialization 与 full-coordinate development Phase3CM pair evaluation 仅在
  77o 执行；本机只执行源码检查、轻量测试、synthetic parity 与证据校验。

## Three-batch result

| Batch | Proposals | Admitted / evaluated | Positive | Negative | Feedback | Run-health failures |
|---|---:|---:|---:|---:|---|---:|
| 000 | 48 | 20 | 4 | 16 | no preceding actionable sample | 0 |
| 001 | 48 | 12 | 3 | 9 | schedule: +/- applied; actual: + clamped / - applied | 0 |
| 002 | 48 | 17 | 2 | 15 | positive + negative applied | 0 |

累计完成 49 个 matched pairs；最终投影为 49 条 observation、9 条 positive-policy、
40 条 negative-scheduler、0 条 run-health failure。自然 underfill 被保留并逐 route
记录，没有为满足 24 pairs/batch 人工制造 admission 或 feedback。

Batch 1 feedback-on/off 的 master attempt stream SHA256 均为：

```text
de2e42ca812e1f86f5ba05289e55e42aa969e1ccbc9553a901c6ed439224b2f8
```

共享输入检查为真，且 feedback-on route budgets 确实发生变化。Batch 1 相对初始
schedule 对 `MINUTE_STATIC`、`SLOW_CROSS_SECTIONAL_LEVEL`、
`SLOW_TEMPORAL_CHANGE` 执行 `DOWNWEIGHT`，对
`INTRADAY_STATE_TRANSITION` 执行 `EXPAND`；Batch 2 对 `FIRSTN_PATH`、
`SLOW_TEMPORAL_CHANGE` 执行 `DOWNWEIGHT`，并继续对
`INTRADAY_STATE_TRANSITION` 执行 `EXPAND`。

最终 closure 逐项核验了 seed、master stream、48-pair 总预算、历史 exact
archive、历史 behavior archive、exact/behavior dedupe policy、registry、compiler、
generator 与 probe coordinate binding。Batch 1 feedback-on/off 的实际
legal/canonical pair 分布分别为：`FIRSTN 12/7`、`MARKET 12/5`、`SLOW_XS 8/12`、
`SLOW_TEMP 4/12`、`MINUTE 4/4`、`STATE 2/2`、`BROAD 6/6`、`DISCLOSURE 0/0`。
actionable route 中，`SLOW_XS 12→8`、`SLOW_TEMP 12→4` 与负向 schedule 同向；
`STATE 2→2` 和 `MINUTE 4→4` 因 exact supply 上限分别记为
`ACTIONABLE_FEEDBACK_CLAMPED`。FIRSTN/MARKET 的增加明确标记为
`SPILLOVER_ONLY_NOT_FEEDBACK`，不进入反馈因果结论。feedback-off route budget
保持 Batch 0 初始 prior。三批 20/12/17 条 full behavior rows 均同时保留四种身份。

## Immutable evidence

| Batch | Batch manifest SHA256 | Archive snapshot SHA256 | Master stream SHA256 |
|---|---|---|---|
| 000 | `59b42725d65cd6fe0486163a63431889779007d87fa1f1f509368450ce4a809f` | `a83b4a74fac67c05d759537230d292f6bb92a78344223f1e197e78b32f19ddd4` | `98485f480660b70f0862bfda1e3d41e9e3f1cfa9521926a60a72da69c13e7cc6` |
| 001 | `60378bea30b4565aa3b21b6e10c9c1e98310e4d14fce9d6ed7bb986670755b54` | `d76b730ab65ae66182531e96d7a3523d0d955aef12df0da9dc9019612a65cc8a` | `de2e42ca812e1f86f5ba05289e55e42aa969e1ccbc9553a901c6ed439224b2f8` |
| 002 | `ec22f70bca059b75f2009f3c88fe34e9b1ea7e8ebbd3c782d25b93c634d34b68` | `2b3194c6fd53ad9b2cad892d4d80130e165339018b2173be96d3b85ca44f2c50` | `faea7ee4016ec046dc8af04e80b4cd9d040bc7a219978470cbff9e77cb89428c` |

本机完整证据镜像：`runtime/cn_iterative_search_v1_20260721/`。最终访问计数为
`validation_reads=0`、`holdout_reads=0`、`forward_2026_reads=0`，promotion 为
`FORBIDDEN`。所有 batch manifest 内绑定的 artifact hashes 已在本机重新计算通过。

## Execution and recovery notes

第一次 77o 尝试暴露 bounded mapping probe 的性能瓶颈，原现场保留在
`D:\ChengboRemote\runtime\cn_iterative_search_v1_20260721_1f67203_attempt1_slow`。
向量化后完成真实评估；随后修复 mixed-Parquet batch close 与 resume identity
binding。完整 Phase3CM 结果由 evaluator source `b7d605ca` 产生，最终因果 receipt、
实际 route-distribution gate、四身份 join、master-stream binding 与 immutable
manifests 由 source `8c09bf25` 重新闭合；已完成
结果仅在输入 identity 完全一致时复用，没有伪装成重新评估。

最终 77o 根目录：
`D:\ChengboRemote\runtime\cn_iterative_search_v1_20260721_final_8c09bf2`。

## Remaining gaps

- 累计 behavior archive 193 rows：100 `RESOLVED`，93
  `BEHAVIOR_UNRESOLVED`。bounded probe 不得用 structural family 填补这 93 条。
- `BROAD_EVENT_FROZEN_ENTRY` 尚无可用的 streaming bounded-probe materialization，
  三批均未产生 behavior-unique admission。
- `DISCLOSURE_EVENT` 的小型 exact stream 在 Batch 0 后耗尽；后续 exact-unique
  supply 为零，且现有 probe 未形成 behavior admission。
- `MARKET_REGIME_CONDITION` 因 behavior unresolved/duplicate 未形成 admission。
- `exploit`、`repair`、`orthogonal` 仍是 action labels；没有 registry-backed 真实
  constructor 前，不宣称公式生成策略已实现。
- streaming backend 仍是 `EXPERIMENTAL_BACKEND`；Strict Stage A、validation、
  holdout、2026、promotion 与跨 campaign adaptive memory 均未开放。

这些 gap 合并在本报告中；没有新增独立治理报告、attestation、逐批 Graph 或 ADR。
