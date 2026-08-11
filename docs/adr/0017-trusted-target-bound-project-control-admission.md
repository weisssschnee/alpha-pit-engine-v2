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
   project id, repository path and canonical Harness runs root. Run records
   outside that root are untrusted even if their JSON shape and verdict look
   valid.
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
   before creating directories or reading data. Direct invocation,
   freeze-as-launch and same-admission/different-output invocation therefore
   fail closed.
5. An automatic successor requires both a parent `POST_BATCH=CONTINUE` bundle
   and a child `PREFLIGHT=PROCEED` bundle. The child request must name the exact
   parent Project Control run, campaign and target run.
6. Technical recovery requires a trusted recovery preflight for the same
   target, an immutable incident file binding, and the original validated
   execution admission for that target. A changed campaign, target run,
   repository SHA or original admission is a new execution and is denied.
7. Project Control remains admission-only authority. Economic results,
   evaluator evidence and the accepted research authority remain decisive.
   This decision grants no search, retry, promotion, OOS or sealed-data access.

## Consequences

- A caller cannot manufacture authority from an arbitrary JSON file, receipt
  hash or generic `PROCEED` verdict.
- Canonical entry and direct-module entry share one fail-closed physical seam.
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
