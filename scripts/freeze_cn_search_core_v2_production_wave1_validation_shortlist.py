"""Freeze a balanced Production Wave 1 shortlist before any validation read."""
from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

from our_system_phase2.services.unified_capability_registry import stable_hash

STATUS = "SEARCH_CORE_V2_PRODUCTION_WAVE1_VALIDATION_SHORTLIST_FROZEN_BEFORE_VALIDATION"
PER_TEMPLATE = 6
TEMPLATES = (
    "BASE_EVENT",
    "BASE_MARKET",
    "BASE_MARKET_EVENT",
    "BASE_TEMPORAL",
    "BASE_TEMPORAL_EVENT",
    "BASE_TEMPORAL_MARKET",
    "BASE_TEMPORAL_MARKET_EVENT",
)


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _verify(payload: Mapping[str, Any], field: str, label: str) -> str:
    body = dict(payload); claimed = str(body.pop(field, ""))
    if not claimed or stable_hash(body) != claimed:
        raise RuntimeError(f"{label} self-hash drift")
    return claimed


def freeze(*, summary_path: Path, representatives_path: Path, productive_path: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    summary = _read(summary_path)
    summary_hash = _verify(summary, "archive_summary_payload_sha256", "Wave1 development archive")
    if (
        summary.get("status") != "SEARCH_CORE_V2_PRODUCTION_WAVE1_DEVELOPMENT_ARCHIVE_READY_NOT_ALPHA_QUALIFIED"
        or summary.get("classification") != "DEVELOPMENT_ONLY_NOT_ALPHA_QUALIFIED"
        or bool(summary.get("validation_read"))
        or summary.get("oos_authority") != "NONE"
        or bool(summary.get("automatic_promotion_authorized"))
    ):
        raise RuntimeError("Wave1 development archive authority drift")
    reps = _read_jsonl(representatives_path)
    productive = _read_jsonl(productive_path)
    prod_by_exact = {str(row["exact_identity"]): row for row in productive}
    if len(prod_by_exact) != len(productive) or len(reps) != int(summary["behavior_representative_count"]):
        raise RuntimeError("Wave1 shortlist source cardinality drift")

    stable_by_template: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in reps:
        if bool(row.get("stable")):
            stable_by_template[str(row["template_id"])].append(row)
    if set(stable_by_template) != set(TEMPLATES) or any(len(stable_by_template[t]) < PER_TEMPLATE for t in TEMPLATES):
        raise RuntimeError("Wave1 stable representative supply underfill")

    selected: list[dict[str, Any]] = []
    for template in TEMPLATES:
        for template_rank, rep in enumerate(stable_by_template[template][:PER_TEMPLATE], start=1):
            exact = str(rep["exact_identity"])
            source = prod_by_exact.get(exact)
            if source is None or not bool(source.get("stable")) or not bool(source.get("productive")):
                raise RuntimeError(f"Wave1 shortlist source result drift: {exact}")
            member = {
                "schema_version": "cn_search_core_v2_production_wave1_validation_shortlist_member_v1",
                "shortlist_ordinal": len(selected),
                "template_shortlist_rank": template_rank,
                "development_rank": int(rep["development_rank"]),
                "exact_identity": exact,
                "template_id": template,
                "behavior_pair_identity": str(rep["behavior_pair_identity"]),
                "structural_region_identity": str(rep["structural_region_identity"]),
                "source_checkpoint_ordinal": int(source["checkpoint_ordinal"]),
                "source_result_payload_sha256": str(source["result_payload_sha256"]),
                "source_record_payload_sha256": str(source["source_record_payload_sha256"]),
                "development_credit": dict(rep["development_credit"]),
                "productive": True,
                "stable": True,
                "validation_read": False,
                "oos_read": False,
                "alpha_qualified": False,
            }
            member["member_payload_sha256"] = stable_hash(member)
            selected.append(member)
    exacts = [row["exact_identity"] for row in selected]
    cells = [(row["template_id"], row["structural_region_identity"]) for row in selected]
    if len(selected) != PER_TEMPLATE * len(TEMPLATES) or len(set(exacts)) != len(selected) or len(set(cells)) != len(selected):
        raise RuntimeError("Wave1 shortlist diversity/cardinality drift")
    if Counter(row["template_id"] for row in selected) != Counter({t: PER_TEMPLATE for t in TEMPLATES}):
        raise RuntimeError("Wave1 shortlist template balance drift")

    freeze_payload = {
        "schema_version": "cn_search_core_v2_production_wave1_validation_shortlist_freeze_v1",
        "status": STATUS,
        "selection_contract": "TOP_6_STABLE_BEHAVIOR_REPRESENTATIVES_PER_TEMPLATE_BY_FROZEN_DEVELOPMENT_PRIORITY",
        "source_archive_summary_file_sha256": _sha(summary_path),
        "source_archive_summary_payload_sha256": summary_hash,
        "source_representatives_file_sha256": _sha(representatives_path),
        "source_productive_archive_file_sha256": _sha(productive_path),
        "candidate_count": len(selected),
        "per_template_count": PER_TEMPLATE,
        "template_counts": dict(sorted(Counter(row["template_id"] for row in selected).items())),
        "candidate_exact_identities": exacts,
        "candidate_exact_identities_sha256": stable_hash(exacts),
        "candidate_member_payloads_sha256": stable_hash([row["member_payload_sha256"] for row in selected]),
        "behavior_identity_count": len({row["behavior_pair_identity"] for row in selected}),
        "template_structural_cell_count": len(set(cells)),
        "membership_frozen_before_validation": True,
        "candidate_generation_allowed_during_validation": False,
        "same_slice_reselection_allowed": False,
        "threshold_tuning_allowed": False,
        "optimizer_feedback_allowed": False,
        "policy_memory_write_allowed": False,
        "validation_read": False,
        "holdout_read": False,
        "forward_read": False,
        "oos_authority": "NONE",
        "automatic_promotion_authorized": False,
        "financial_evaluation_executed_by_freeze": False,
    }
    freeze_payload["freeze_payload_sha256"] = stable_hash(freeze_payload)
    return freeze_payload, selected


def _write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(dict(row), ensure_ascii=False, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--summary", type=Path, required=True)
    parser.add_argument("--representatives", type=Path, required=True)
    parser.add_argument("--productive-archive", type=Path, required=True)
    parser.add_argument("--freeze-output", type=Path, required=True)
    parser.add_argument("--members-output", type=Path, required=True)
    args = parser.parse_args(argv)
    payload, members = freeze(summary_path=args.summary, representatives_path=args.representatives, productive_path=args.productive_archive)
    args.freeze_output.parent.mkdir(parents=True, exist_ok=True)
    args.freeze_output.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    _write_jsonl(args.members_output, members)
    print(json.dumps({"status":payload["status"],"candidates":payload["candidate_count"],"per_template":payload["per_template_count"],"behaviors":payload["behavior_identity_count"],"cells":payload["template_structural_cell_count"],"payload":payload["freeze_payload_sha256"]},sort_keys=True))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())