"""Freeze one history-bound 24-thread search authorization from the dual-lane contract."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping


SHARED_CONTROL_WINNER_GUIDED_SEARCH_PROFILE = (
    "cn_shared_control_winner_guided_search_v1"
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


def _load(path: Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def _verify_history(
    *,
    manifest_path: Path,
    candidate_archive: Path,
    behavior_archive: Path,
) -> dict[str, Any]:
    history = _load(manifest_path)
    if str(history.get("status") or "") != "PASS":
        raise RuntimeError("history snapshot is not PASS")
    if (
        str(history.get("merge_policy") or "")
        != "IDENTITY_ONLY_NO_REWARD_NO_SCHEDULER_STATE"
    ):
        raise RuntimeError("history snapshot merge policy drift")
    expected = {
        "candidate_exact_archive": candidate_archive,
        "behavior_archive": behavior_archive,
    }
    for key, path in expected.items():
        row = dict(history.get(key) or {})
        if Path(str(row.get("path") or "")).resolve() != path.resolve():
            raise RuntimeError(f"history snapshot path drift: {key}")
        if str(row.get("sha256") or "") != _sha256(path):
            raise RuntimeError(f"history snapshot hash drift: {key}")
    winner_row = dict(history.get("winner_structural_guide") or {})
    winner_path = Path(str(winner_row.get("path") or "")).resolve()
    if (
        not winner_path.is_file()
        or str(winner_row.get("sha256") or "") != _sha256(winner_path)
    ):
        raise RuntimeError("history snapshot winner guide drift")
    if history.get("reward_columns_imported") not in ([], None):
        raise RuntimeError("history snapshot imports reward columns")
    if bool(history.get("scheduler_state_imported")):
        raise RuntimeError("history snapshot imports scheduler state")
    return history


def freeze_authorization(
    *,
    source_authorization: Path,
    dual_lane_contract: Path,
    capacity_manifest: Path,
    candidate_archive: Path,
    behavior_archive: Path,
    history_manifest: Path,
) -> dict[str, Any]:
    source_path = Path(source_authorization).resolve()
    contract_path = Path(dual_lane_contract).resolve()
    capacity_path = Path(capacity_manifest).resolve()
    candidate_path = Path(candidate_archive).resolve()
    behavior_path = Path(behavior_archive).resolve()
    history_path = Path(history_manifest).resolve()
    source = _load(source_path)
    contract = _load(contract_path)
    capacity = _load(capacity_path)
    history = _verify_history(
        manifest_path=history_path,
        candidate_archive=candidate_path,
        behavior_archive=behavior_path,
    )
    if str(source.get("winner_structural_guide_sha256") or "") != str(
        (history.get("winner_structural_guide") or {}).get("sha256") or ""
    ):
        raise RuntimeError("source authorization winner guide drift")
    if not bool(source.get("execution_authorized")):
        raise RuntimeError("source authorization is not executable")
    if (
        str(contract.get("status") or "")
        != "FROZEN_USER_AUTHORIZED_QUALIFICATION_REQUIRED_BEFORE_LAUNCH"
    ):
        raise RuntimeError("dual-lane contract is not frozen")
    search = dict(contract.get("search_lane") or {})
    if (
        str(search.get("campaign_profile") or "")
        != SHARED_CONTROL_WINNER_GUIDED_SEARCH_PROFILE
    ):
        raise RuntimeError("dual-lane search campaign profile drift")
    profile = dict(contract.get("shared_resource_authority") or {})
    if str(profile.get("search_profile") or "") != "SEARCH_DUAL_24":
        raise RuntimeError("dual-lane contract search profile drift")
    if int(profile.get("total_cpu_threads") or 0) != 32:
        raise RuntimeError("dual-lane CPU total drift")
    checkpoint_count = int(search.get("checkpoint_count") or 0)
    asks_per_checkpoint = int(
        search.get("formal_fresh_exact_asks_per_checkpoint") or 0
    )
    maximum_asks = int(search.get("maximum_formal_fresh_exact_asks") or 0)
    fixed_routes = dict(search.get("fixed_route_formal_asks_per_checkpoint") or {})
    final_routes = dict(search.get("final_route_formal_ask_allocation") or {})
    if checkpoint_count * asks_per_checkpoint != maximum_asks:
        raise RuntimeError("dual-lane search checkpoint budget drift")
    if sum(int(value) for value in fixed_routes.values()) != asks_per_checkpoint:
        raise RuntimeError("dual-lane per-checkpoint route budget drift")
    if sum(int(value) for value in final_routes.values()) != maximum_asks:
        raise RuntimeError("dual-lane final route budget drift")
    capacity_body = dict(capacity)
    claimed_capacity_hash = str(
        capacity_body.pop("capacity_manifest_sha256", "")
    )
    if claimed_capacity_hash != _stable_hash(capacity_body):
        raise RuntimeError("node capacity manifest self-hash drift")
    search_profile = dict(
        (capacity.get("profiles") or {}).get("SEARCH_DUAL_24") or {}
    )
    if (
        str(search_profile.get("role") or "") != "SEARCH"
        or int(search_profile.get("cpu_threads") or 0) != 24
    ):
        raise RuntimeError("SEARCH_DUAL_24 profile drift")

    payload = dict(source)
    payload.pop("active_threads", None)
    payload.pop("session_threads", None)
    payload.update(
        {
            "schema_version": "cn_shared_control_dual_search_authorization_v1",
            "status": "EXECUTION_AUTHORIZED_AFTER_ZERO_FINANCIAL_QUALIFICATION",
            "authority_source": str(contract.get("authority_source") or ""),
            "campaign_id": str(search.get("campaign_id") or ""),
            "campaign_profile": str(search.get("campaign_profile") or ""),
            "coverage_floors": final_routes,
            "route_formal_ask_caps": final_routes,
            "final_route_formal_ask_allocation": final_routes,
            "fixed_route_formal_asks_per_checkpoint": fixed_routes,
            "maximum_checkpoints": checkpoint_count,
            "asks_per_checkpoint": asks_per_checkpoint,
            "maximum_raw_asks": maximum_asks,
            "maximum_wall_seconds": int(
                (search.get("throughput_contract") or {}).get(
                    "maximum_wall_seconds"
                )
                or 0
            ),
            "seed_base": int(search.get("seed_base") or 0),
            "historical_candidate_archive_sha256": _sha256(candidate_path),
            "historical_behavior_archive_sha256": _sha256(behavior_path),
            "historical_manifest_sha256": _sha256(history_path),
            "node_resource_profiles_allowed": ["SEARCH_DUAL_24"],
            "resource_profile_switch_boundary": "BATCH_CLOSED_IMMUTABLE_ONLY",
            "resource_topology_changes_search_semantics": False,
            "resource_topology_changes_candidate_order": False,
            "resource_topology_changes_reward": False,
            "node_resource_capacity_manifest_sha256": claimed_capacity_hash,
            "base_authorization": {
                "path": str(source_path),
                "sha256": _sha256(source_path),
            },
            "dual_lane_contract": {
                "path": str(contract_path),
                "sha256": _sha256(contract_path),
            },
            "node_resource_capacity_manifest": {
                "path": str(capacity_path),
                "sha256": _sha256(capacity_path),
            },
            "historical_snapshot": {
                "manifest": str(history_path),
                "manifest_sha256": _sha256(history_path),
                "exact_identity_count": int(
                    history.get("exact_identity_count") or 0
                ),
                "behavior_identity_row_count": int(
                    history.get("behavior_identity_row_count") or 0
                ),
                "reward_rows_imported": 0,
                "optimizer_state_imported": False,
                "scheduler_state_imported": False,
            },
            "first_checkpoint_minimum_pair_evaluated_per_hour": float(
                (search.get("throughput_contract") or {}).get(
                    "first_checkpoint_minimum_pair_evaluated_per_hour"
                )
                or 0.0
            ),
            "first_checkpoint_minimum_entitlement_occupancy": float(
                (search.get("throughput_contract") or {}).get(
                    "first_checkpoint_minimum_entitlement_occupancy"
                )
                or 0.0
            ),
            "parallel_validation_lane": (
                "SEPARATE_FIXED_32_PAIR_REPORT_ONLY_NO_FEEDBACK"
            ),
        }
    )
    payload["resource_topology_authorization_sha256"] = _stable_hash(payload)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-authorization", type=Path, required=True)
    parser.add_argument("--dual-lane-contract", type=Path, required=True)
    parser.add_argument("--capacity-manifest", type=Path, required=True)
    parser.add_argument("--candidate-archive", type=Path, required=True)
    parser.add_argument("--behavior-archive", type=Path, required=True)
    parser.add_argument("--history-manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = freeze_authorization(
        source_authorization=args.source_authorization,
        dual_lane_contract=args.dual_lane_contract,
        capacity_manifest=args.capacity_manifest,
        candidate_archive=args.candidate_archive,
        behavior_archive=args.behavior_archive,
        history_manifest=args.history_manifest,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
