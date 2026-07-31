"""Refresh identity-only history from verified immutable search checkpoints.

This helper is deliberately zero-financial.  It verifies the closed checkpoint
chain and declared artifacts, then merges only exact identities and label-free
behavior identities into a new campaign history snapshot.  Reward rows,
optimizer state, and scheduler state are never imported.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

try:
    from scripts.build_cn_campaign_history_snapshot import build_snapshot
except ModuleNotFoundError:  # Direct ``python scripts/<file>.py`` execution.
    from build_cn_campaign_history_snapshot import build_snapshot


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


def _verify_checkpoint(
    *,
    root: Path,
    checkpoint_id: str,
    expected_prior_manifest_sha256: str,
    expected_route_mix: Mapping[str, int],
) -> dict[str, Any]:
    checkpoint_root = root / "checkpoints" / checkpoint_id
    manifest_path = checkpoint_root / "batch_manifest.json"
    manifest = _load(manifest_path)
    if str(manifest.get("status") or "") != "BATCH_CLOSED_IMMUTABLE":
        raise RuntimeError(f"checkpoint is not immutable: {checkpoint_id}")
    claimed = str(manifest.get("manifest_payload_hash") or "")
    unsigned = dict(manifest)
    unsigned.pop("manifest_payload_hash", None)
    if not claimed or claimed != _stable_hash(unsigned):
        raise RuntimeError(f"checkpoint manifest self-hash drift: {checkpoint_id}")
    prior = str(
        (manifest.get("input_hashes") or {}).get("prior_checkpoint_manifest")
        or ""
    )
    if prior != expected_prior_manifest_sha256:
        raise RuntimeError(f"checkpoint manifest chain drift: {checkpoint_id}")

    artifacts: dict[str, dict[str, Any]] = {}
    for artifact in manifest.get("artifacts") or []:
        relative = str(artifact.get("path") or "")
        path = checkpoint_root / relative
        if (
            not path.is_file()
            or path.stat().st_size != int(artifact.get("bytes") or -1)
            or _sha256(path) != str(artifact.get("sha256") or "")
        ):
            raise RuntimeError(f"checkpoint artifact drift: {path}")
        artifacts[relative.replace("\\", "/")] = dict(artifact)

    required = {
        "route_schedule.json",
        "candidate_attempts.parquet",
        "full_behavior.parquet",
    }
    missing = required - set(artifacts)
    if missing:
        raise RuntimeError(
            f"checkpoint history artifacts missing in {checkpoint_id}: "
            + ",".join(sorted(missing))
        )
    schedule = _load(checkpoint_root / "route_schedule.json")
    observed_mix = {
        str(row.get("route_id") or ""): int(row.get("asked_pairs") or 0)
        for row in schedule.get("routes") or []
    }
    normalized_expected = {
        str(route_id): int(count)
        for route_id, count in expected_route_mix.items()
    }
    if observed_mix != normalized_expected:
        raise RuntimeError(f"checkpoint route schedule drift: {checkpoint_id}")
    if int(schedule.get("formal_fresh_exact_asks") or sum(observed_mix.values())) != sum(
        normalized_expected.values()
    ):
        raise RuntimeError(f"checkpoint formal ask count drift: {checkpoint_id}")

    return {
        "checkpoint_id": checkpoint_id,
        "manifest": str(manifest_path.resolve()),
        "manifest_file_sha256": _sha256(manifest_path),
        "manifest_payload_hash": claimed,
        "declared_artifact_count": len(artifacts),
        "candidate_source": str(
            (checkpoint_root / "candidate_attempts.parquet").resolve()
        ),
        "behavior_source": str(
            (checkpoint_root / "full_behavior.parquet").resolve()
        ),
        "route_mix": observed_mix,
    }


def build_verified_history(
    *,
    campaign_root: Path,
    base_candidate_archive: Path,
    base_behavior_archive: Path,
    winner_structural_guide: Path,
    output_root: Path,
    checkpoint_count: int,
    expected_route_mix: Mapping[str, int],
) -> dict[str, Any]:
    campaign_root = Path(campaign_root).resolve()
    base_candidate_archive = Path(base_candidate_archive).resolve()
    base_behavior_archive = Path(base_behavior_archive).resolve()
    winner_structural_guide = Path(winner_structural_guide).resolve()
    output_root = Path(output_root).resolve()
    if checkpoint_count <= 0:
        raise ValueError("checkpoint_count must be positive")
    if output_root.exists():
        raise FileExistsError(output_root)
    if not winner_structural_guide.is_file():
        raise FileNotFoundError(winner_structural_guide)
    output_root.mkdir(parents=True)

    verified: list[dict[str, Any]] = []
    prior = "GENESIS"
    for index in range(1, checkpoint_count + 1):
        checkpoint_id = f"checkpoint_{index:03d}"
        row = _verify_checkpoint(
            root=campaign_root,
            checkpoint_id=checkpoint_id,
            expected_prior_manifest_sha256=prior,
            expected_route_mix=expected_route_mix,
        )
        verified.append(row)
        prior = str(row["manifest_file_sha256"])

    excluded_checkpoint_id = f"checkpoint_{checkpoint_count + 1:03d}"
    excluded_manifest = (
        campaign_root
        / "checkpoints"
        / excluded_checkpoint_id
        / "batch_manifest.json"
    )
    if excluded_manifest.is_file():
        raise RuntimeError(
            "next checkpoint unexpectedly closed; frozen partial-history "
            f"boundary is stale: {excluded_checkpoint_id}"
        )

    candidate_output = output_root / "candidate_exact_archive.parquet"
    behavior_output = output_root / "behavior_archive.parquet"
    history_manifest = output_root / "history_manifest.json"
    history = build_snapshot(
        candidate_sources=(
            base_candidate_archive,
            *(Path(row["candidate_source"]) for row in verified),
        ),
        behavior_sources=(
            base_behavior_archive,
            *(Path(row["behavior_source"]) for row in verified),
        ),
        candidate_output=candidate_output,
        behavior_output=behavior_output,
        manifest_output=history_manifest,
    )
    history["winner_structural_guide"] = {
        "path": str(winner_structural_guide),
        "bytes": winner_structural_guide.stat().st_size,
        "sha256": _sha256(winner_structural_guide),
    }
    history_manifest.write_text(
        json.dumps(history, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    receipt: dict[str, Any] = {
        "schema_version": "cn_verified_partial_campaign_history_receipt_v1",
        "status": "ZERO_FINANCIAL_HISTORY_REFRESH_PASS",
        "campaign_root": str(campaign_root),
        "verified_checkpoints": verified,
        "excluded_unclosed_checkpoint": excluded_checkpoint_id,
        "history_manifest": str(history_manifest),
        "history_manifest_sha256": _sha256(history_manifest),
        "candidate_exact_archive": str(candidate_output),
        "candidate_exact_archive_sha256": _sha256(candidate_output),
        "behavior_archive": str(behavior_output),
        "behavior_archive_sha256": _sha256(behavior_output),
        "winner_structural_guide": str(winner_structural_guide),
        "winner_structural_guide_sha256": _sha256(
            winner_structural_guide
        ),
        "exact_identity_count": int(history["exact_identity_count"]),
        "behavior_identity_row_count": int(
            history["behavior_identity_row_count"]
        ),
        "merge_policy": "IDENTITY_ONLY_NO_REWARD_NO_SCHEDULER_STATE",
        "reward_rows_imported": 0,
        "optimizer_state_imported": False,
        "scheduler_state_imported": False,
        "financial_reads": 0,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }
    receipt["receipt_payload_sha256"] = _stable_hash(receipt)
    receipt_path = output_root / "partial_history_receipt.json"
    receipt_path.write_text(
        json.dumps(receipt, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return receipt


def _route_mix(values: Sequence[str]) -> dict[str, int]:
    result: dict[str, int] = {}
    for value in values:
        route_id, separator, count = str(value).partition("=")
        if not separator or not route_id or int(count) < 0:
            raise argparse.ArgumentTypeError(
                f"expected ROUTE_ID=COUNT, got {value!r}"
            )
        result[route_id] = int(count)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign-root", type=Path, required=True)
    parser.add_argument("--base-candidate-archive", type=Path, required=True)
    parser.add_argument("--base-behavior-archive", type=Path, required=True)
    parser.add_argument("--winner-structural-guide", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--checkpoint-count", type=int, required=True)
    parser.add_argument("--expected-route", action="append", required=True)
    args = parser.parse_args()
    result = build_verified_history(
        campaign_root=args.campaign_root,
        base_candidate_archive=args.base_candidate_archive,
        base_behavior_archive=args.base_behavior_archive,
        winner_structural_guide=args.winner_structural_guide,
        output_root=args.output_root,
        checkpoint_count=args.checkpoint_count,
        expected_route_mix=_route_mix(args.expected_route),
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
