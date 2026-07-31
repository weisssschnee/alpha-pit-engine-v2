"""Bind a search authority to safe exclusive or dual-lane node profiles."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any


ALLOWED_SEARCH_PROFILES = (
    "SEARCH_EXCLUSIVE_32",
    "SEARCH_DUAL_24",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(4 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def freeze_authorization(
    *,
    source_authorization: Path,
    capacity_manifest: Path,
) -> dict[str, Any]:
    source_path = Path(source_authorization).resolve()
    capacity_path = Path(capacity_manifest).resolve()
    source = json.loads(source_path.read_text(encoding="utf-8-sig"))
    capacity = json.loads(capacity_path.read_text(encoding="utf-8-sig"))
    capacity_body = dict(capacity)
    claimed_capacity_hash = str(
        capacity_body.pop("capacity_manifest_sha256", "")
    )
    if claimed_capacity_hash != _stable_hash(capacity_body):
        raise RuntimeError("node capacity manifest self-hash drift")
    if not bool(source.get("execution_authorized")):
        raise RuntimeError("source search authorization is not executable")
    profiles = dict(capacity.get("profiles") or {})
    for profile_id in ALLOWED_SEARCH_PROFILES:
        row = dict(profiles.get(profile_id) or {})
        if str(row.get("role") or "") != "SEARCH":
            raise RuntimeError(f"search resource profile missing: {profile_id}")
    payload = dict(source)
    payload["schema_version"] = (
        "cn_search_resource_flexible_execution_authorization_v1"
    )
    payload.pop("active_threads", None)
    payload.pop("session_threads", None)
    payload.update(
        {
            "base_authorization": {
                "path": str(source_path),
                "sha256": _sha256(source_path),
            },
            "node_resource_capacity_manifest": {
                "path": str(capacity_path),
                "sha256": _sha256(capacity_path),
            },
            "node_resource_capacity_manifest_sha256": claimed_capacity_hash,
            "node_resource_profiles_allowed": list(ALLOWED_SEARCH_PROFILES),
            "resource_profile_switch_boundary": (
                "BATCH_CLOSED_IMMUTABLE_ONLY"
            ),
            "resource_topology_changes_search_semantics": False,
            "resource_topology_changes_candidate_order": False,
            "resource_topology_changes_reward": False,
        }
    )
    payload["resource_topology_authorization_sha256"] = _stable_hash(payload)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-authorization", type=Path, required=True)
    parser.add_argument("--capacity-manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = freeze_authorization(
        source_authorization=args.source_authorization,
        capacity_manifest=args.capacity_manifest,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

