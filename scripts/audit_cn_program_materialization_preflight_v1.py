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


def audit(root: Path, output: Path) -> dict[str, Any]:
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
    if not required <= set(str(value) for value in manifest["fields"]):
        raise ValueError("program materialized sidecar schema coverage drift")
    shards = list(manifest.get("shards") or ())
    if len(shards) != int(manifest.get("source_shard_count") or -1):
        raise ValueError("program materialized sidecar shard count drift")
    rows = 0
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
    if rows != int(manifest["sidecar_rows"]):
        raise ValueError("program materialized sidecar row count drift")

    plan_path = Path(closure["materialization_plan"]["path"]).resolve()
    plan = _read(plan_path)
    verify_program_materialization_plan_v1(plan)
    if (
        _sha256(plan_path) != str(closure["materialization_plan"]["file_sha256"])
        or str(plan["plan_sha256"]) != str(closure["materialization_plan"]["payload_sha256"])
    ):
        raise ValueError("program materialization plan binding drift")
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
        "added_field_ids": list(closure["added_field_ids"]),
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
