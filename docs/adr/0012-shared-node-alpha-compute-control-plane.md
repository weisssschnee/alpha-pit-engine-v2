# ADR 0012: Shared-node alpha compute control plane

- Status: Accepted
- Date: 2026-07-31
- Scope: future CN alpha search, executable replay, and report-only validation on 77o

## Context

The alpha pipeline already separates development search, executable A-share
replay and report-only validation, but the shared 77o host had no common
cross-process resource authority. Search and validation could each declare
their own thread count while competing for the same 32 logical CPUs. A runtime
acceleration gate then judged the search against the whole host rather than its
actual entitlement. This produced operationally valid evidence of an invalid
run, but it did not identify whether the algorithm or an ungoverned resource
collision was responsible.

The finalist path exposed two other system faults. Candidate-specific A-share
outcomes were sometimes raised as batch-fatal exceptions, and heterogeneous
success/blocker dictionaries were inferred into one Parquet schema. In
addition, execution-clock incompatibility was discovered only after a cohort
entered replay instead of during a label-free capability preflight.

These are control-plane defects. Fixing one incident at a time would leave the
node oversubscribed, waste financial evaluation on structurally incompatible
candidates, and keep result persistence dependent on whichever blocker happened
to appear first.

## Decision

### 1. One shared resource authority per compute node

Every future heavy search or validation launcher on 77o must acquire a
cross-process lease from the same atomic, file-locked state root before
financial work. The capacity authority is content-addressed and host-bound.
Admission accounts for CPU entitlement, declared memory claim, actual available
memory and the fixed 24 GiB reserve.

The initial profiles are:

- `SEARCH_EXCLUSIVE_32`: one search owns all 32 logical CPUs;
- `SEARCH_DUAL_24`: search owns 24 logical CPUs;
- `VALIDATION_DUAL_8`: validation owns the remaining 8 logical CPUs.

Only `32` or `24+8` heavy-lane topology is authorized. Nested native
parallelism remains forbidden. A dead launcher does not release a claim while
its workload-bound child process remains alive. A released or state-detached
receipt cannot authorize a runtime. A bounded status command exposes active,
stale and remaining entitlements without a persistent inspector or database.

### 2. Resource topology is not search semantic authority

The search contract continues to own Grammar, seed, route schedule, candidate
identity, optimizer state, reward and sealed-data boundaries. A separate
resource-topology authorization may allow `SEARCH_EXCLUSIVE_32` and
`SEARCH_DUAL_24`; it removes thread count from the semantic contract and binds
the capacity manifest instead.

Profile switching is permitted only at a `BATCH_CLOSED_IMMUTABLE` boundary.
The active lease and profile are included in each new checkpoint manifest.
Changing topology never imports cross-campaign reward or optimizer state and
never reorders or filters a frozen cohort.

Runtime acceleration is judged against the task's CPU entitlement. Full-host
occupancy remains required for a full-host lease; a 24-thread search is judged
against 24 entitled threads, not against 32 host threads. Existing invalid
runs are not reclassified retroactively.

### 3. Candidate result persistence uses a stable envelope

Candidate JSON remains the lossless authority. The aggregate Parquet table has
an explicit scalar schema and stores the complete heterogeneous candidate
payload as canonical JSON plus SHA256. Success rows, terminal-liquidity blocks,
no-fill blocks and corporate-action blocks therefore cannot collide through
Arrow type inference.

Known candidate-local economic outcomes inherit from one typed blocker base and
close as immutable candidate evidence. Unknown identity, hash, data-access,
execution-rule or schema errors remain batch-fatal. This preserves fail-closed
semantics without allowing one economically blocked candidate to destroy an
unchanged cohort.

### 4. Execution-clock capability is checked before financial replay

A zero-financial, train-only capability builder binds candidate-visible fields
to an execution clock. It records registry coverage, shard schema coverage and
within-session variation. Cohort freezing evaluates the actual primary and
control expressions; it does not exclude an entire route merely because one
field in that route is incompatible.

The capability artifact must have zero financial, validation, holdout and 2026
reads and zero optimizer, feedback, archive and promotion writes. Missing or
incompatible fields become explicit preflight reasons before replay consumes
financial compute.

## Consequences

- Safe parallelism is a declared 24+8 allocation, not two processes both
  believing they own the node.
- A single search may still use all 32 threads; dual-lane operation is chosen
  only when both lanes are independently authorized and supply-feasible.
- Performance failures can be separated into algorithmic throughput,
  entitlement-relative acceleration and host-resource admission evidence.
- Replay compute is spent on candidate-compatible evidence, while candidate
  economic blockers remain reportable rather than becoming infrastructure
  crashes.
- The design remains file- and manifest-based. It creates no platform,
  scheduler service or database and does not require a persistent inspector.
- No search, OOS, promotion, holdout or 2026 access is authorized by this ADR.

## Verification

Required evidence before a deployment is considered usable:

1. capacity and topology self-hashes match;
2. 32-exclusive rejects a concurrent 8-thread validation lease;
3. 24+8 admits exactly 32 entitled threads and no more;
4. orphan workloads retain their resource claim and released receipts fail;
5. full-host and partial-entitlement acceleration gates are both covered;
6. mixed candidate outcomes round-trip through the stable Parquet envelope;
7. candidate-level clock incompatibility and shard coverage are fail-closed;
8. search/replay regression tests preserve reward, identity and sealed-read
   contracts;
9. the versioned 77o workspace passes focused tests and bounded status checks.

## Rollback

If dual-lane evidence is unreliable, freeze future execution to
`SEARCH_EXCLUSIVE_32` and run validation only after search releases its lease.
Do not disable lease validation, weaken memory gates, reinterpret prior invalid
runs or restore heterogeneous Parquet inference. Reverting this ADR requires a
replacement resource authority and a new accepted decision.
