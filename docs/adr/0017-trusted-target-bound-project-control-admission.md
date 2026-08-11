# ADR 0017: Trusted target-bound Project Control admission

- Status: Accepted
- Date: 2026-08-11
- Scope: CN high-cost route identity, Project Control trust and recovery lineage
- Supersedes: the self-attested receipt boundary in ADR 0016; all other ADR 0016 decisions remain active

## Context

The first physical Project Control bridge correctly put an admission check in
front of canonical route imports, but its inputs were still self-attested by
the caller. A generic `PROCEED` record and caller-supplied receipt hash could be
rebound to an unrelated action, campaign or target run. Direct module entry
also bypassed the `app.py` seam. Successor and recovery labels did not prove
their parent or original authorization lineage.

Those weaknesses undermine factual integrity without adding useful execution
speed. Project Control must answer one narrow question: whether a trusted,
target-bound Harness decision authorizes this exact execution. It must never
reinterpret observed economics, choose an alpha or grant sealed-data access.

## Decision

1. The project owns a self-hashed trust configuration that fixes the exact
   project id and an allowlisted deployment-to-Harness-store mapping. Local G:
   uses the existing Harness authority store; 77o uses the fixed
   `D:\ChengboRemote\runtime\cn_project_control_authority_store` mirror. Run
   records outside those roots are untrusted even if their JSON shape and
   verdict look valid. Activation fixes this canonical config internally,
   derives the executing checkout from the service module, requires that path
   to match one deployment, and derives that checkout's live clean HEAD itself;
   callers cannot substitute a trust config, repository, project identity or
   expected code SHA. Before an authorized 77o execution, the control-plane
   operator must copy the exact immutable Harness bundle and materialize the
   admission inside the pinned 77o authority store; an absent mirror denies.
2. A trusted Harness bundle consists of sibling `run_record.json`,
   `task_spec.json` and `project_profile.json` files. The task specification
   embeds a self-hashed execution request binding the exact project, repository
   SHA, action, campaign, target run, absolute output root and expiry. The run,
   task, profile and request identities must agree. The admission binds the
   hashes of all three source files, the generated execution context and the
   request payload.
3. The builder emits a deterministic task id from the request hash so a generic
   or edited Project Control result cannot be rebound after review. Expired
   preflight requests fail closed. Historical `POST_BATCH` results may be read
   only for lineage checks and do not independently authorize a child.
4. `app.py` atomically revalidates the immutable admission before route import
   and activates a one-use, in-process capability for the exact route and
   action. Caller-created proof dictionaries cannot activate it. Every
   high-cost module consumes the capability before argument parsing and then
   compares the parsed output root with the admitted absolute output root
   before creating directories or reading data. The targeted and large-TPE
   routes also read only campaign-authorization metadata at this seam and bind
   the live absolute path, file hash, campaign id and profile to the immutable
   Project Control request. Hashing and JSON parsing use one byte buffer, and
   the same verified payload is passed into runtime authority binding without a
   second path read. Successor or continuation profiles must carry
   `SUCCESSOR_CAMPAIGN` (or technical recovery whose bound root lineage is still
   a successor), so generic `LAUNCH`/`RETRY` and recovery of a rejected launch
   cannot bypass parent lineage. Direct invocation,
   freeze-as-launch and same-admission/different-output invocation therefore
   fail closed.
5. Activation atomically creates a durable control record at the admitted
   output root before route import. A new freeze, launch, successor or retry
   requires either an absent root or a route-qualified root containing only
   launcher control metadata such as `deployment_binding.json`, redirected
   logs and a resource-lease directory. Activation adds the internal
   `.project_control_execution` directory before business execution. The
   fixed-stratified route freshness check explicitly treats that directory as
   control metadata rather than business output. Every admission hash has one exclusive
   consumption marker, so the same admission cannot be replayed by another
   process. A failed import spends that entrance conservatively; later work
   requires an explicitly authorized retry or recovery.
6. An automatic successor requires both a parent `POST_BATCH=CONTINUE` bundle
   and a child `PREFLIGHT=PROCEED` bundle. The child request must name the exact
   parent Project Control run, campaign and target run.
7. Technical recovery requires a trusted recovery preflight for the same
   target, an immutable incident file binding, and the original validated
   execution admission for that target. Its output-root identity must name the
   original admission hash, and the recovery admission itself is consumed only
   once. A changed campaign, target run, repository SHA, output root or original
   admission is a new execution and is denied.
   A route without implemented same-run resume semantics does not advertise
   `RECOVERY`; fixed-stratified production therefore exposes launch, successor
   and retry only instead of pretending that a nonempty root can be resumed.
8. Project Control remains admission-only authority. Economic results,
   evaluator evidence and the accepted research authority remain decisive.
   This decision grants no search, retry, promotion, OOS or sealed-data access.

## Consequences

- A caller cannot manufacture authority from an arbitrary JSON file, receipt
  hash or generic `PROCEED` verdict.
- Canonical entry and direct-module entry share one fail-closed physical seam.
- Current 77o wrappers forward the target run, admission and immutable file hash;
  fixed-stratified does not advertise unsupported recovery semantics.
- Winner-guided and continuity wrappers forward the requested action, and the
  route independently binds it to the exact immutable campaign authorization.
- Zero-financial preflight requests derive the future
  `qualification_authorization.json` bytes and hash without claiming the fresh
  output root. One-shot preparation scripts stop at a machine-readable external
  Project Control boundary; only a later separately admitted canonical launcher
  call may create the planned authorization and start the route.
- LAUNCH/SUCCESSOR cannot be replayed as an unlabelled retry or recovery, even
  from another process using the same output root.
- Successor and recovery are provable continuations, not alternative names for
  a new run.
- Synthetic tests can verify the entire control path without reading financial
  data or invoking a search.
- Existing closed economic evidence is unchanged and remains superior to any
  admission heuristic or control verdict.
- The existing Harness has no cryptographic receipt-signing service. Its
  configured runs root is therefore the authority-store trust boundary: this
  bridge validates canonical location, full Harness bundle shape and bound
  hashes, but cannot distinguish a Harness write from a malicious local writer
  with authority-store permissions. Adding signatures or a distinct service
  identity would be a Harness change outside this thin-bridge repair.

## Rollback

Replacing this seam requires a later accepted decision with equivalent trusted
source, target, expiry, direct-entry and lineage guarantees. Rollback cannot
weaken the permanent spent-asset deny or turn Project Control into economic,
promotion or sealed-data authority.
