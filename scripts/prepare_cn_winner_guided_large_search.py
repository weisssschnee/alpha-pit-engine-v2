"""Freeze cumulative identity memory and winner structural lanes without finance."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq


EXPECTED_SELECTION_PAYLOAD_SHA256 = (
    "5ab6dbdf374d4867cf197b254cf6672d342bed54436bf153a551ae65867a3815"
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _payload_sha256(payload: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2)
        + "\n",
        encoding="utf-8",
    )


def prepare(
    *,
    prior_candidate_archive: Path,
    completed_campaign_root: Path,
    winner_cohort_root: Path,
    output_root: Path,
) -> dict[str, Any]:
    output_root.mkdir(parents=True, exist_ok=True)
    exact = set(
        map(
            str,
            pq.read_table(
                prior_candidate_archive, columns=["exact_identity"]
            )
            .column("exact_identity")
            .to_pylist(),
        )
    )
    exact.discard("")
    checkpoint_sources: list[dict[str, Any]] = []
    checkpoint_roots = sorted(
        (completed_campaign_root / "checkpoints").glob("checkpoint_*")
    )
    if len(checkpoint_roots) != 8:
        raise RuntimeError("completed campaign checkpoint count drift")
    for checkpoint_root in checkpoint_roots:
        asked_path = checkpoint_root / "asked_population.json"
        asked = json.loads(
            asked_path.read_text(encoding="utf-8-sig")
        )
        checkpoint_exact = {
            str(row.get("availability_exact_identity") or "")
            for row in asked
            if bool(row.get("formal_fresh_exact_ask"))
        }
        checkpoint_exact.discard("")
        if len(checkpoint_exact) != 768:
            raise RuntimeError(
                f"{checkpoint_root.name}: formal exact identity drift"
            )
        exact.update(checkpoint_exact)
        checkpoint_sources.append(
            {
                "path": str(asked_path),
                "sha256": _sha256(asked_path),
                "formal_exact": len(checkpoint_exact),
            }
        )

    candidate_archive = (
        output_root / "candidate_exact_archive_after_bounded_large.parquet"
    )
    pq.write_table(
        pa.table({"exact_identity": sorted(exact)}),
        candidate_archive,
        compression="zstd",
    )

    winner_candidates = winner_cohort_root / "keep_review_candidates.parquet"
    winner_rows = pq.read_table(winner_candidates).to_pylist()
    primary_rows = [
        row
        for row in winner_rows
        if str(row.get("pair_member_role") or "") == "PRIMARY"
    ]
    if len(primary_rows) != 64:
        raise RuntimeError("winner cohort primary-member count drift")
    guide: dict[str, set[str]] = {}
    for row in primary_rows:
        guide.setdefault(str(row["route_id"]), set()).add(
            str(row["skeleton_id"])
        )
    frozen_guide = {
        route_id: sorted(skeletons)
        for route_id, skeletons in sorted(guide.items())
    }
    if sum(map(len, frozen_guide.values())) != 4:
        raise RuntimeError("winner structural lane count drift")
    winner_guide = {
        "schema_version": "cn_winner_structural_guide_v1",
        "status": "FROZEN_DEVELOPMENT_WINNER_STRUCTURES",
        "source_cohort_root": str(winner_cohort_root),
        "source_selection_payload_sha256": (
            EXPECTED_SELECTION_PAYLOAD_SHA256
        ),
        "selected_pairs": 64,
        "allowed_skeletons_by_route": frozen_guide,
        "optimizer_state_reused": False,
        "reward_rows_imported": 0,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }
    winner_guide_path = output_root / "winner_structural_guide.json"
    _write_json(winner_guide_path, winner_guide)

    behavior_archive = completed_campaign_root / "behavior_archive.parquet"
    winner_manifest = winner_cohort_root / "keep_review_manifest.json"
    manifest = {
        "schema_version": (
            "cn_winner_guided_large_search_identity_manifest_v1"
        ),
        "status": "PASS",
        "candidate_exact_archive": {
            "path": str(candidate_archive),
            "sha256": _sha256(candidate_archive),
            "rows": len(exact),
        },
        "behavior_archive": {
            "path": str(behavior_archive),
            "sha256": _sha256(behavior_archive),
        },
        "winner_structural_guide": {
            "path": str(winner_guide_path),
            "sha256": _sha256(winner_guide_path),
        },
        "sources": {
            "prior_candidate_archive": {
                "path": str(prior_candidate_archive),
                "sha256": _sha256(prior_candidate_archive),
            },
            "checkpoint_asks": checkpoint_sources,
            "winner_candidates": {
                "path": str(winner_candidates),
                "sha256": _sha256(winner_candidates),
            },
            "winner_manifest": {
                "path": str(winner_manifest),
                "sha256": _sha256(winner_manifest),
            },
        },
        "financial_reads": 0,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }
    manifest["manifest_payload_sha256"] = _payload_sha256(manifest)
    manifest_path = output_root / "winner_guided_identity_manifest.json"
    _write_json(manifest_path, manifest)
    return {
        "output_root": str(output_root),
        "candidate_exact_rows": len(exact),
        "candidate_exact_sha256": _sha256(candidate_archive),
        "behavior_archive_sha256": _sha256(behavior_archive),
        "winner_guide_sha256": _sha256(winner_guide_path),
        "manifest_sha256": _sha256(manifest_path),
        "manifest_payload_sha256": manifest[
            "manifest_payload_sha256"
        ],
        "allowed_skeletons_by_route": frozen_guide,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--prior-candidate-archive", type=Path, required=True
    )
    parser.add_argument(
        "--completed-campaign-root", type=Path, required=True
    )
    parser.add_argument("--winner-cohort-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args()
    print(
        json.dumps(
            prepare(
                prior_candidate_archive=args.prior_candidate_archive.resolve(),
                completed_campaign_root=args.completed_campaign_root.resolve(),
                winner_cohort_root=args.winner_cohort_root.resolve(),
                output_root=args.output_root.resolve(),
            ),
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
