"""Independently audit PROGRAM_MATERIALIZATION_PREFLIGHT_V1 evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys
from typing import Any, Mapping


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from our_system_phase2.services.candidate_program_materialization_v1 import (
    verify_program_information_coverage_v1,
    verify_program_materialization_plan_v1,
)
from our_system_phase2.services.unified_capability_registry import stable_hash


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _read(path: Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def _verify_hash(payload: Mapping[str, Any], field: str, label: str) -> None:
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if len(claimed) != 64 or stable_hash(body) != claimed:
        raise ValueError(f"{label} self-hash drift")


def audit(root: Path, output: Path | None = None) -> dict[str, Any]:
    root = Path(root).resolve()
    closure_path = root / "PROGRAM_MATERIALIZATION_PREFLIGHT_V1.json"
    closure = _read(closure_path)
    _verify_hash(closure, "closure_sha256", "program materialization closure")
    if closure.get("status") != "PROGRAM_MATERIALIZATION_PREFLIGHT_COMPLETE":
        raise ValueError("program materialization closure status drift")
    if not bool(closure.get("required_equals_materializable_equals_program_covered")):
        raise ValueError("program materialization exact coverage did not pass")
    if not bool(closure.get("lag_applied_exactly_once")):
        raise ValueError("program materialization lag-once gate did not pass")
    if not bool(closure.get("all_required_fields_information_qualified")):
        raise ValueError("program materialization information coverage did not pass")
    if (
        closure.get("unexpected_unbound_added_fields")
        or closure.get("wrong_scope_fields")
        or closure.get("wrong_clock_fields")
    ):
        raise ValueError("program materialization closure contains semantic drift")
    if bool(closure.get("financial_evaluation_executed")) or any(
        int(closure.get(key) or 0)
        for key in (
            "validation_reads",
            "holdout_reads",
            "historical_2023_reads",
            "forward_b_reads",
            "forward_2026_reads",
        )
    ):
        raise PermissionError("program materialization used prohibited data/economics")

    manifest_path = Path(closure["output_manifest"]["path"]).resolve()
    if manifest_path.parent != root or _sha256(manifest_path) != closure["output_manifest"]["file_sha256"]:
        raise ValueError("program materialization output-manifest binding drift")
    manifest = _read(manifest_path)
    _verify_hash(manifest, "manifest_hash", "program materialized sidecar manifest")
    if str(manifest["manifest_hash"]) != str(closure["output_manifest"]["payload_sha256"]):
        raise ValueError("program materialization output-manifest payload drift")
    if (
        manifest.get("status") != "TIME_MAJOR_LAYOUT_PARITY_PASS"
        or manifest.get("data_role") != "development_train_only"
    ):
        raise ValueError("program materialized sidecar authority drift")
    required = set(str(value) for value in closure["required_physical_leaf_ids"])
    added = set(str(value) for value in closure["added_field_ids"])
    if not required <= set(str(value) for value in manifest["fields"]):
        raise ValueError("program materialized sidecar schema coverage drift")
    source_manifest_binding = dict(manifest["source_manifest"])
    source_manifest_path = Path(str(source_manifest_binding["path"])).resolve()
    source_manifest = _read(source_manifest_path)
    _verify_hash(source_manifest, "manifest_hash", "program materialization source manifest")
    if (
        not source_manifest_path.is_file()
        or _sha256(source_manifest_path)
        != str(source_manifest_binding["file_sha256"])
        or str(source_manifest["manifest_hash"])
        != str(source_manifest_binding["payload_sha256"])
        or set(str(value) for value in manifest["fields"])
        - set(str(value) for value in source_manifest["fields"])
        != added
    ):
        raise ValueError("program materialization source-manifest identity drift")
    bar_manifest_binding = dict(manifest["bar_source_manifest"])
    bar_manifest_path = Path(str(bar_manifest_binding["path"])).resolve()
    if (
        not bar_manifest_path.is_file()
        or _sha256(bar_manifest_path) != str(bar_manifest_binding["file_sha256"])
    ):
        raise ValueError("program materialization bar-source identity drift")
    bar_manifest = _read(bar_manifest_path)
    if bar_manifest.get("forbidden_roles_present") or bool(
        bar_manifest.get("forward_2026_present")
    ):
        raise PermissionError("program materialization bar source is not development-only")
    shards = list(manifest.get("shards") or ())
    if len(shards) != int(manifest.get("source_shard_count") or -1):
        raise ValueError("program materialized sidecar shard count drift")
    rows = 0
    market_digests: dict[str, set[str]] = {}
    for shard in shards:
        path = Path(str(shard["output_path"])).resolve()
        if not path.is_relative_to(root):
            raise ValueError("program materialized sidecar shard escapes root")
        if (
            not path.is_file()
            or path.stat().st_size != int(shard["output_bytes"])
            or _sha256(path) != str(shard["output_sha256"])
        ):
            raise ValueError(f"program materialized sidecar shard drift: {path}")
        rows += int(shard["rows"])
        receipt_path = Path(str(shard["materialization_receipt_path"])).resolve()
        if (
            not receipt_path.is_relative_to(root)
            or _sha256(receipt_path)
            != str(shard["materialization_receipt_sha256"])
        ):
            raise ValueError("program materialization receipt binding drift")
        receipt = _read(receipt_path)
        _verify_hash(receipt, "receipt_sha256", "program materialization receipt")
        if (
            set(str(value) for value in receipt["materialized_field_ids"]) != added
            or str(receipt["source"]["path"]) != str(shard["source_path"])
            or str(receipt["source"]["sha256"]) != str(shard["source_sha256"])
            or str(receipt["output"]["path"]) != str(shard["output_path"])
            or str(receipt["output"]["sha256"]) != str(shard["output_sha256"])
            or int(receipt["output"]["rows"]) != int(shard["rows"])
        ):
            raise ValueError("program materialization receipt identity/shape drift")
        for adapter_id, raw_evidence in dict(receipt["adapter_evidence"]).items():
            evidence = dict(raw_evidence)
            if bool(evidence.get("lag_reapplied")):
                raise ValueError("program materialization reapplied source lag")
            evidence_source = Path(str(evidence["source"])).resolve()
            if (
                not evidence_source.is_file()
                or not evidence_source.is_relative_to(bar_manifest_path.parent)
                or _sha256(evidence_source) != str(evidence["source_sha256"])
            ):
                raise ValueError("program materialization adapter source identity drift")
            if adapter_id == "MARKET_SESSION_PRELAGGED_BROADCAST":
                if (
                    evidence.get("entity_scope") != "MARKET"
                    or evidence.get("join_policy")
                    != "same_session_1500_broadcast_of_pre_lagged_market_state"
                    or any(
                        int(value) > 1
                        for value in dict(
                            evidence["maximum_intraday_cross_sectional_unique_values"]
                        ).values()
                    )
                ):
                    raise ValueError("program materialization market-broadcast drift")
                fields_key = "|".join(sorted(str(value) for value in evidence["fields"]))
                market_digests.setdefault(fields_key, set()).add(
                    str(evidence["session_value_digest"])
                )
    if rows != int(manifest["sidecar_rows"]):
        raise ValueError("program materialized sidecar row count drift")
    inconsistent_market = {
        key: sorted(values) for key, values in market_digests.items() if len(values) != 1
    }
    if inconsistent_market:
        raise ValueError(
            f"program materialization market source differs across shards: {inconsistent_market}"
        )

    plan_path = Path(closure["materialization_plan"]["path"]).resolve()
    plan = _read(plan_path)
    verify_program_materialization_plan_v1(plan)
    if (
        _sha256(plan_path) != str(closure["materialization_plan"]["file_sha256"])
        or str(plan["plan_sha256"]) != str(closure["materialization_plan"]["payload_sha256"])
    ):
        raise ValueError("program materialization plan binding drift")
    information_path = Path(closure["information_coverage"]["path"]).resolve()
    information = _read(information_path)
    verify_program_information_coverage_v1(information)
    if (
        information_path.parent != root
        or _sha256(information_path)
        != str(closure["information_coverage"]["file_sha256"])
        or str(information["information_coverage_sha256"])
        != str(closure["information_coverage"]["payload_sha256"])
        or set(information["required_field_ids"]) != required
    ):
        raise ValueError("program information coverage binding drift")
    metrics_authority = dict(information["information_metrics_authority"])
    metrics_path = Path(str(metrics_authority["path"])).resolve()
    if (
        not metrics_path.is_file()
        or _sha256(metrics_path) != str(metrics_authority["file_sha256"])
        or str(metrics_authority["file_sha256"])
        != str(
            closure["information_coverage"][
                "information_metrics_authority_file_sha256"
            ]
        )
    ):
        raise ValueError("program information metrics authority drift")
    fixtures_path = Path(closure["template_fixtures"]["path"]).resolve()
    fixtures = _read(fixtures_path)
    _verify_hash(fixtures, "fixture_sha256", "program materialization fixtures")
    if (
        _sha256(fixtures_path) != str(closure["template_fixtures"]["file_sha256"])
        or int(fixtures.get("template_count") or 0) != 8
        or {str(row.get("status")) for row in fixtures.get("fixtures") or ()}
        != {"PROGRAM_APPLY_PASS"}
    ):
        raise ValueError("program materialization eight-template fixture drift")

    artifact_path = Path(closure["artifact_manifest"]["path"]).resolve()
    artifacts = _read(artifact_path)
    _verify_hash(artifacts, "manifest_sha256", "program materialization artifacts")
    if _sha256(artifact_path) != str(closure["artifact_manifest"]["file_sha256"]):
        raise ValueError("program materialization artifact-manifest binding drift")
    for artifact in artifacts["artifacts"]:
        path = Path(str(artifact["path"])).resolve()
        if (
            not path.is_file()
            or path.stat().st_size != int(artifact["bytes"])
            or _sha256(path) != str(artifact["sha256"])
        ):
            raise ValueError(f"program materialization artifact drift: {path}")

    payload = {
        "schema_version": "cn_program_materialization_independent_audit_v1",
        "status": "PROGRAM_MATERIALIZATION_INDEPENDENT_AUDIT_PASS",
        "root": str(root),
        "closure_file_sha256": _sha256(closure_path),
        "closure_payload_sha256": closure["closure_sha256"],
        "artifact_count": int(artifacts["artifact_count"]),
        "shard_count": len(shards),
        "sidecar_rows": rows,
        "required_physical_leaf_count": len(required),
        "information_qualified_field_count": int(
            information["required_field_count"]
        ),
        "added_field_ids": sorted(added),
        "template_fixture_count": int(fixtures["template_count"]),
        "required_equals_materializable_equals_program_covered": True,
        "lag_applied_exactly_once": True,
        "financial_evaluation_executed": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "historical_2023_reads": 0,
        "forward_b_reads": 0,
        "forward_2026_reads": 0,
    }
    payload["audit_sha256"] = stable_hash(payload)
    if output is not None:
        output = Path(output).resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(audit(args.root, args.output), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
