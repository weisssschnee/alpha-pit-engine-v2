"""Build an immutable zero-financial V0 candidate supply preflight."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from our_system_phase2.services.candidate_representation_v0 import (
    BROAD_EVENT_TEMPLATE_ID,
    TEMPLATE_CONTRACTS_V0,
    CandidateSpecV0,
)
from our_system_phase2.services.fixed_stratified_candidate_sampling import (
    build_fixed_stratified_plan_v0,
    generate_fixed_stratified_epoch_v0,
)
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
    stable_hash,
)
from our_system_phase2.services.unified_discovery_generators import (
    load_development_discovery_root_authority,
)


CLOSURE_NAME = "CANDIDATE_REPRESENTATION_V0_PREFLIGHT_COMPLETE.json"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            default=str,
        )
        + "\n",
        encoding="utf-8",
    )


def _write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> int:
    count = 0
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(
                json.dumps(
                    dict(row),
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                    default=str,
                )
                + "\n"
            )
            count += 1
    return count


def _line_count(path: Path) -> int:
    with path.open("r", encoding="utf-8") as handle:
        return sum(bool(line.strip()) for line in handle)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def _artifact(root: Path, path: Path, *, row_count: int | None) -> dict[str, Any]:
    return {
        "path": path.relative_to(root).as_posix(),
        "sha256": _sha256(path),
        "bytes": path.stat().st_size,
        "row_count": row_count,
    }


def build_candidate_representation_v0_preflight(
    *,
    registry_path: Path,
    root_contract_path: Path,
    output_root: Path,
    quota_per_template: int,
    seeds: Sequence[int],
) -> dict[str, Any]:
    root = Path(output_root).resolve()
    if root.exists():
        raise FileExistsError(f"preflight output root already exists: {root}")
    if int(quota_per_template) <= 0:
        raise ValueError("quota_per_template must be positive")
    registry_source = Path(registry_path).resolve()
    contract_source = Path(root_contract_path).resolve()
    registry = UnifiedCapabilityRegistry.read(registry_source)
    authority = load_development_discovery_root_authority(
        contract_source,
        registry=registry,
    )
    root_allowlists = authority["route_root_allowlists"]
    broad_inventory = [
        row.to_dict()
        for row in registry.fields_for_route(BROAD_EVENT_TEMPLATE_ID)
    ]
    if not broad_inventory:
        raise RuntimeError("Broad Event frozen inventory is empty")
    broad_inventory_hash = stable_hash(broad_inventory)
    root_scope_hash = stable_hash(root_allowlists)
    plan = build_fixed_stratified_plan_v0(
        template_attempt_quotas={
            template_id: int(quota_per_template)
            for template_id in TEMPLATE_CONTRACTS_V0
        },
        seeds=seeds,
        registry_hash=registry.registry_hash,
        root_scope_hash=root_scope_hash,
        frozen_inventory_hashes={
            BROAD_EVENT_TEMPLATE_ID: broad_inventory_hash
        },
    )
    result = generate_fixed_stratified_epoch_v0(
        registry,
        plan=plan,
        route_root_allowlist=root_allowlists,
    )

    root.mkdir(parents=True)

    plan_path = root / "fixed_stratified_plan_v0.json"
    specs_path = root / "candidate_specs_v0.jsonl"
    rows_path = root / "compatible_candidate_rows_v0.jsonl"
    ledger_path = root / "generation_ledger_v0.jsonl"
    waterfall_path = root / "template_waterfall_v0.json"
    summary_path = root / "summary_v0.json"
    _write_json(plan_path, result.plan)
    spec_count = _write_jsonl(specs_path, result.candidate_specs)
    compatible_count = _write_jsonl(rows_path, result.candidate_rows)
    ledger_count = _write_jsonl(ledger_path, result.ledger)
    _write_json(waterfall_path, result.waterfall)
    _write_json(summary_path, result.summary)

    artifacts = [
        _artifact(root, plan_path, row_count=1),
        _artifact(root, specs_path, row_count=spec_count),
        _artifact(root, rows_path, row_count=compatible_count),
        _artifact(root, ledger_path, row_count=ledger_count),
        _artifact(root, waterfall_path, row_count=len(result.waterfall)),
        _artifact(root, summary_path, row_count=1),
    ]
    closure: dict[str, Any] = {
        "schema_version": "cn_candidate_representation_v0_preflight_closure",
        "status": "PREPARED_ZERO_FINANCIAL",
        "output_root": str(root),
        "registry_path": str(registry_source),
        "registry_file_sha256": _sha256(registry_source),
        "registry_payload_hash": registry.registry_hash,
        "root_contract_path": str(contract_source),
        "root_contract_file_sha256": _sha256(contract_source),
        "root_contract_hash": authority["contract_hash"],
        "root_scope_hash": root_scope_hash,
        "broad_event_frozen_inventory_hash": broad_inventory_hash,
        "plan_hash": plan["plan_hash"],
        "template_count": len(TEMPLATE_CONTRACTS_V0),
        "scheduled_attempts": result.summary["scheduled_attempts"],
        "attempted": result.summary["attempted"],
        "candidate_spec_count": spec_count,
        "compatible_candidate_row_count": compatible_count,
        "generation_ledger_count": ledger_count,
        "artifact_count": len(artifacts),
        "artifacts": artifacts,
        "shared_tpe_credit": False,
        "optimizer_feedback_accessed": False,
        "dynamic_budget_reallocation_allowed": False,
        "underfill_spillover_allowed": False,
        "economic_evaluator_accessed": False,
        "financial_read_count": 0,
        "validation_read_count": 0,
        "holdout_read_count": 0,
        "forward_2026_read_count": 0,
        "sealed_data_read_count": 0,
        "promotion_authorized": False,
    }
    closure["closure_payload_sha256"] = stable_hash(closure)
    _write_json(root / CLOSURE_NAME, closure)
    return closure


def verify_candidate_representation_v0_preflight(
    output_root: Path,
) -> dict[str, Any]:
    root = Path(output_root).resolve()
    closure_path = root / CLOSURE_NAME
    closure = json.loads(closure_path.read_text(encoding="utf-8"))
    payload = {
        key: value
        for key, value in closure.items()
        if key != "closure_payload_sha256"
    }
    if str(closure.get("closure_payload_sha256") or "") != stable_hash(
        payload
    ):
        raise RuntimeError("V0 preflight closure self-hash mismatch")
    if str(closure.get("status") or "") != "PREPARED_ZERO_FINANCIAL":
        raise RuntimeError("V0 preflight closure status mismatch")
    artifacts = list(closure.get("artifacts") or ())
    if len(artifacts) != int(closure.get("artifact_count") or -1):
        raise RuntimeError("V0 preflight artifact count mismatch")
    for artifact in artifacts:
        target = (root / str(artifact["path"])).resolve()
        if root not in target.parents:
            raise RuntimeError("V0 preflight artifact escaped output root")
        if not target.is_file():
            raise RuntimeError(f"V0 preflight artifact missing: {target}")
        if _sha256(target) != str(artifact["sha256"]):
            raise RuntimeError(f"V0 preflight artifact hash mismatch: {target}")
        expected_rows = artifact.get("row_count")
        if target.suffix == ".jsonl" and expected_rows is not None:
            if _line_count(target) != int(expected_rows):
                raise RuntimeError(
                    f"V0 preflight artifact row count mismatch: {target}"
                )
    specs_path = root / "candidate_specs_v0.jsonl"
    candidate_rows_path = root / "compatible_candidate_rows_v0.jsonl"
    specs = _read_jsonl(specs_path)
    if len(specs) != int(closure.get("candidate_spec_count") or -1):
        raise RuntimeError("V0 preflight candidate spec count mismatch")
    verified_specs = [CandidateSpecV0.from_record(row) for row in specs]
    if len({spec.proposal_hash for spec in verified_specs}) != len(
        verified_specs
    ):
        raise RuntimeError("V0 preflight duplicate candidate proposal hash")
    rows = _read_jsonl(candidate_rows_path)
    if len(rows) != int(
        closure.get("compatible_candidate_row_count") or -1
    ):
        raise RuntimeError("V0 preflight compatible row count mismatch")
    expected = {
        (spec.candidate_id, spec.proposal_hash): spec.spec_hash
        for spec in verified_specs
    }
    observed = {
        (
            str(row.get("candidate_id") or ""),
            str(row.get("candidate_proposal_v0_hash") or ""),
        ): str(row.get("candidate_spec_v0_hash") or "")
        for row in rows
    }
    if observed != expected:
        raise RuntimeError("V0 preflight compatible row/spec binding mismatch")
    forbidden = (
        "shared_tpe_credit",
        "optimizer_feedback_accessed",
        "dynamic_budget_reallocation_allowed",
        "underfill_spillover_allowed",
        "economic_evaluator_accessed",
        "promotion_authorized",
    )
    if any(bool(closure.get(key)) for key in forbidden):
        raise RuntimeError("V0 preflight enabled forbidden authority")
    read_counts = (
        "financial_read_count",
        "validation_read_count",
        "holdout_read_count",
        "forward_2026_read_count",
        "sealed_data_read_count",
    )
    if any(int(closure.get(key) or 0) for key in read_counts):
        raise RuntimeError("V0 preflight recorded prohibited data reads")
    return closure


def _parse_seeds(value: str) -> tuple[int, ...]:
    output = tuple(int(item.strip()) for item in value.split(",") if item.strip())
    if not output:
        raise argparse.ArgumentTypeError("at least one seed is required")
    return output


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--root-contract", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--quota-per-template", type=int, default=32)
    parser.add_argument("--seeds", type=_parse_seeds, default=(1729, 2718, 31415, 65537))
    parser.add_argument("--verify-only", action="store_true")
    args = parser.parse_args()
    if args.verify_only:
        closure = verify_candidate_representation_v0_preflight(
            args.output_root
        )
    else:
        closure = build_candidate_representation_v0_preflight(
            registry_path=args.registry,
            root_contract_path=args.root_contract,
            output_root=args.output_root,
            quota_per_template=args.quota_per_template,
            seeds=args.seeds,
        )
        verify_candidate_representation_v0_preflight(args.output_root)
    print(json.dumps(closure, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
