from __future__ import annotations

import argparse
import hashlib
import json
import platform
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd

from our_system_phase2.runtime.cn_iterative_search_v1 import (
    _context_and_binding,
    _run_phase3cm,
    _sha256,
)
from our_system_phase2.services.fixed_split_authority import FixedSplitAuthority
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
)


AUTHORIZED_HOST = "DESKTOP-77OPJ6F"
BACKENDS = ("active_bar", "stock_session")
PROTECTED_TRAIN_ARTIFACTS = (
    "candidate_ledger.parquet",
    "observation_ledger.parquet",
    "behavior_archive.parquet",
    "campaign_metrics.parquet",
)


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _write_json(path: Path, payload: Mapping[str, Any]) -> Path:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(destination)
    return destination


def _artifact(path: Path, *, root: Path) -> dict[str, Any]:
    source = Path(path).resolve()
    return {
        "path": str(source.relative_to(root.resolve())).replace("\\", "/"),
        "sha256": _sha256(source),
        "bytes": source.stat().st_size,
    }


def _verify_hashes(root: Path, expected: Mapping[str, str]) -> dict[str, str]:
    observed: dict[str, str] = {}
    for relative, expected_hash in expected.items():
        path = root / relative
        if not path.is_file():
            raise FileNotFoundError(f"protected artifact missing: {path}")
        observed[relative] = _sha256(path)
        if observed[relative] != str(expected_hash):
            raise RuntimeError(f"protected artifact hash drift: {relative}")
    return observed


def _read_report_only_sidecar_manifest(
    root: Path,
    *,
    filename: str,
    split_manifest_hash: str,
) -> dict[str, Any]:
    path = Path(root) / filename
    payload = json.loads(path.read_text(encoding="utf-8"))
    expected_status = (
        "TIME_MAJOR_LAYOUT_PARITY_PASS"
        if filename == "CN_DEVELOPMENT_TIME_MAJOR_EXECUTION_LAYOUT_V2.json"
        else "GLOBAL_SYMBOL_CONTINUITY_LABEL_SIDECARS_READY"
    )
    if str(payload.get("status") or "") != expected_status:
        raise RuntimeError(f"sidecar is not qualified: {path}")
    if str(payload.get("evaluation_role") or "") != "holdout":
        raise RuntimeError(f"sidecar is not holdout-bound: {path}")
    if str(payload.get("data_role") or "") != "holdout_report_only":
        raise RuntimeError(f"sidecar is not report-only: {path}")
    if str(payload.get("split_manifest_hash") or "") != split_manifest_hash:
        raise RuntimeError(f"sidecar split hash drift: {path}")
    if int(payload.get("eligible_holdout_date_count") or 0) != 48:
        raise RuntimeError(f"sidecar holdout calendar drift: {path}")
    if int(payload.get("holdout_reads") or 0) <= 0:
        raise RuntimeError(f"sidecar contains no holdout rows: {path}")
    if int(payload.get("forward_2026_reads") or 0) != 0:
        raise RuntimeError(f"sidecar contains forward-2026 reads: {path}")
    return payload


def _freeze_candidates(
    *,
    candidate_ledger_path: Path,
    freeze: Mapping[str, Any],
) -> list[dict[str, Any]]:
    frame = pd.read_parquet(candidate_ledger_path).fillna("")
    rows: list[dict[str, Any]] = []
    for expected in freeze["candidates"]:
        pair_id = str(expected["pair_id"])
        pair = frame.loc[frame["pair_id"].astype(str) == pair_id].copy()
        if len(pair) != 2:
            raise RuntimeError(f"frozen pair membership drift: {pair_id}")
        by_role = {
            str(row["pair_member_role"]): row
            for row in pair.to_dict(orient="records")
        }
        if set(by_role) != {"PRIMARY", "CONTROL"}:
            raise RuntimeError(f"frozen pair role drift: {pair_id}")
        primary = by_role["PRIMARY"]
        control = by_role["CONTROL"]
        if str(primary["candidate_id"]) != str(expected["candidate_id"]):
            raise RuntimeError(f"frozen primary identity drift: {pair_id}")
        if str(control["candidate_id"]) != str(expected["control_candidate_id"]):
            raise RuntimeError(f"frozen control identity drift: {pair_id}")
        if str(primary["exact_identity"]) != str(expected["primary_exact_identity"]):
            raise RuntimeError(f"frozen primary exact identity drift: {pair_id}")
        if str(control["exact_identity"]) != str(expected["control_exact_identity"]):
            raise RuntimeError(f"frozen control exact identity drift: {pair_id}")
        rows.extend((primary, control))
    if len(rows) != 2 * int(freeze["frozen_pair_count"]):
        raise RuntimeError("frozen candidate member count drift")
    return rows


def _holdout_summary(
    results: Sequence[Mapping[str, Any]],
    *,
    freeze: Mapping[str, Any],
) -> dict[str, Any]:
    candidate_rows: dict[str, dict[str, Any]] = {}
    pair_rows: dict[str, dict[str, Any]] = {}
    for result in results:
        for row in result.get("candidate_rewards") or ():
            candidate_rows[str(row["candidate_id"])] = dict(row)
        for row in result.get("pair_results") or ():
            pair_rows[str(row["pair_id"])] = dict(row)
    candidates = []
    for expected in freeze["candidates"]:
        candidate_id = str(expected["candidate_id"])
        pair_id = str(expected["pair_id"])
        reward = candidate_rows[candidate_id]
        pair = pair_rows[pair_id]
        candidates.append(
            {
                "candidate_id": candidate_id,
                "pair_id": pair_id,
                "route_id": expected["route_id"],
                "expression": expected["expression"],
                "portfolio_behavior_signature_id": expected[
                    "portfolio_behavior_signature_id"
                ],
                "portfolio_behavior_family_id": expected[
                    "portfolio_behavior_family_id"
                ],
                "holdout_day_sortino": reward.get("train_day_sortino"),
                "holdout_worst_horizon_day_sortino": reward.get(
                    "train_worst_horizon_day_sortino"
                ),
                "holdout_report_metric": reward.get("holdout_report_metric"),
                "pair_holdout_report_metric": pair.get(
                    "pair_holdout_report_metric"
                ),
                "pair_evaluation_status": pair.get("pair_evaluation_status"),
                "pair_evaluation_blockers": pair.get(
                    "pair_evaluation_blockers"
                ),
                "pair_support_count": pair.get("pair_support_count"),
                "portfolio_mode": reward.get("portfolio_mode"),
                "short_allowed": reward.get("short_allowed"),
                "mean_one_way_turnover": reward.get(
                    "train_mean_one_way_turnover"
                ),
            }
        )
    return {
        "schema_version": "cn_core_pack_fixed_holdout_result_v1",
        "status": "FIXED_HOLDOUT_REPORT_COMPLETE",
        "evaluation_role": "holdout",
        "usage": "report_only",
        "holdout_trade_date_count": 48,
        "oos_sample_grade": "WEAK",
        "bias_audit_decision": "HOLD_RESEARCH",
        "bias_audit_reason": (
            "48 daily observations are weak OOS evidence and the Phase3CM path "
            "does not yet prove full A-share T+1, suspension and price-limit fill rules"
        ),
        "candidate_selection_reopened": False,
        "automatic_promotion": "FORBIDDEN",
        "feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "forward_2026_reads": 0,
        "candidates": candidates,
    }


def run(args: argparse.Namespace) -> dict[str, Any]:
    if platform.node().upper() != AUTHORIZED_HOST:
        raise RuntimeError(
            f"fixed holdout is authorized only on {AUTHORIZED_HOST}"
        )
    campaign_root = args.campaign_root.resolve()
    output_root = args.output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    freeze_path = args.freeze_manifest.resolve()
    freeze = json.loads(freeze_path.read_text(encoding="utf-8"))
    if str(freeze.get("status") or "") != "FROZEN_BEFORE_HOLDOUT_ACCESS":
        raise RuntimeError("candidate freeze is not closed")
    if int(freeze.get("frozen_pair_count") or 0) != 12:
        raise RuntimeError("fixed holdout requires the frozen 12-pair pack")

    expected_hashes = dict(freeze["protected_train_artifact_hashes"])
    if set(expected_hashes) != set(PROTECTED_TRAIN_ARTIFACTS):
        raise RuntimeError("protected train artifact set drift")
    train_hashes_before = _verify_hashes(campaign_root, expected_hashes)
    candidate_ledger_path = campaign_root / "candidate_ledger.parquet"
    candidates = _freeze_candidates(
        candidate_ledger_path=candidate_ledger_path,
        freeze=freeze,
    )

    split = FixedSplitAuthority.read(args.split_manifest.resolve())
    if split.manifest_hash != str(freeze["split_manifest_sha256"]):
        raise RuntimeError("fixed split authority drift")
    holdout_dates = tuple(
        str(row["trade_date"])
        for row in split.rows
        if str(row["split"]) == "holdout"
    )
    if len(holdout_dates) != 48 or len(set(holdout_dates)) != 48:
        raise RuntimeError("fixed holdout calendar drift")

    registry = UnifiedCapabilityRegistry.read(args.registry.resolve())
    field_roots = {
        "active_bar": args.active_field_root.resolve(),
        "stock_session": args.session_field_root.resolve(),
    }
    label_roots = {
        "active_bar": args.active_label_root.resolve(),
        "stock_session": args.session_label_root.resolve(),
    }
    sidecar_manifests = []
    for backend in BACKENDS:
        sidecar_manifests.append(
            _read_report_only_sidecar_manifest(
                field_roots[backend],
                filename="CN_DEVELOPMENT_TIME_MAJOR_EXECUTION_LAYOUT_V2.json",
                split_manifest_hash=split.manifest_hash,
            )
        )
        sidecar_manifests.append(
            _read_report_only_sidecar_manifest(
                label_roots[backend],
                filename="CN_FORWARD_LABEL_SIDECAR_MANIFEST.json",
                split_manifest_hash=split.manifest_hash,
            )
        )
    sidecar_binding_hash = _stable_hash(
        [
            {
                "root": str(root),
                "manifest_sha256": _sha256(
                    root
                    / (
                        "CN_DEVELOPMENT_TIME_MAJOR_EXECUTION_LAYOUT_V2.json"
                        if index % 2 == 0
                        else "CN_FORWARD_LABEL_SIDECAR_MANIFEST.json"
                    )
                ),
            }
            for index, root in enumerate(
                (
                    field_roots["active_bar"],
                    label_roots["active_bar"],
                    field_roots["stock_session"],
                    label_roots["stock_session"],
                )
            )
        ]
    )
    binding_path, table_paths = _context_and_binding(
        batch_root=output_root,
        candidates=candidates,
        registry=registry,
        split=split,
        data_release_hash=sidecar_binding_hash,
        evaluation_role="holdout",
    )
    receipts = _run_phase3cm(
        batch_id="fixed_holdout_48d",
        batch_root=output_root,
        binding_path=binding_path,
        table_paths=table_paths,
        split_manifest=args.split_manifest.resolve(),
        field_roots=field_roots,
        label_roots=label_roots,
        compute_threads={
            "active_bar": int(args.active_threads),
            "stock_session": int(args.session_threads),
        },
        evaluation_role="holdout",
    )
    results = []
    for backend in BACKENDS:
        path = (
            output_root
            / "phase3cm_holdout"
            / backend
            / "CN_STREAMING_BACKEND_RESULT.json"
        )
        if not path.is_file():
            continue
        result = json.loads(path.read_text(encoding="utf-8"))
        if str(result.get("evaluation_role") or "") != "holdout":
            raise RuntimeError(f"holdout result role drift: {backend}")
        if int(result.get("holdout_reads") or 0) <= 0:
            raise RuntimeError(f"holdout result has no holdout reads: {backend}")
        if int(result.get("validation_reads") or 0) != 0:
            raise RuntimeError(f"holdout result read validation: {backend}")
        if int(result.get("forward_2026_reads") or 0) != 0:
            raise RuntimeError(f"holdout result read forward-2026: {backend}")
        if any(
            str(result.get(key) or "") != "FORBIDDEN"
            for key in ("feedback_write", "scheduler_write", "archive_write")
        ):
            raise RuntimeError(f"holdout result mutated search state: {backend}")
        for reward in result.get("candidate_rewards") or ():
            if str(reward.get("portfolio_mode") or "") != "long_only_top":
                raise RuntimeError(f"holdout portfolio is not long-only: {backend}")
            if str(reward.get("short_allowed") or "").strip().lower() in {
                "1",
                "true",
                "yes",
            }:
                raise RuntimeError(f"holdout portfolio enabled shorting: {backend}")
        results.append(result)
    if not results:
        raise RuntimeError("fixed holdout produced no backend result")

    train_hashes_after = _verify_hashes(campaign_root, expected_hashes)
    if train_hashes_after != train_hashes_before:
        raise RuntimeError("protected train artifacts changed during holdout")
    summary = _holdout_summary(results, freeze=freeze)
    summary["holdout_reads"] = sum(
        int(result.get("holdout_reads") or 0) for result in results
    )
    summary["freeze_manifest_sha256"] = _sha256(freeze_path)
    summary["sidecar_binding_hash"] = sidecar_binding_hash
    summary["protected_train_hashes_unchanged"] = True
    summary_path = _write_json(
        output_root / "fixed_holdout_result.json",
        summary,
    )
    complete = {
        "schema_version": "cn_core_pack_fixed_holdout_complete_v1",
        "status": "HOLDOUT_COMPLETE_IMMUTABLE",
        "freeze_manifest": {
            "path": str(freeze_path),
            "sha256": _sha256(freeze_path),
        },
        "fixed_holdout_result": _artifact(summary_path, root=output_root),
        "backend_results": [
            _artifact(
                output_root
                / "phase3cm_holdout"
                / backend
                / "CN_STREAMING_BACKEND_RESULT.json",
                root=output_root,
            )
            for backend in BACKENDS
            if (
                output_root
                / "phase3cm_holdout"
                / backend
                / "CN_STREAMING_BACKEND_RESULT.json"
            ).is_file()
        ],
        "access_receipts": receipts,
        "protected_train_artifact_hashes": train_hashes_after,
        "holdout_reads": summary["holdout_reads"],
        "validation_reads": 0,
        "forward_2026_reads": 0,
        "feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "automatic_promotion": "FORBIDDEN",
    }
    complete_path = output_root / "HOLDOUT_COMPLETE.json"
    complete["manifest_body_sha256"] = _stable_hash(complete)
    _write_json(complete_path, complete)
    return complete


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--campaign-root", type=Path, required=True)
    parser.add_argument("--freeze-manifest", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--split-manifest", type=Path, required=True)
    parser.add_argument("--active-field-root", type=Path, required=True)
    parser.add_argument("--active-label-root", type=Path, required=True)
    parser.add_argument("--session-field-root", type=Path, required=True)
    parser.add_argument("--session-label-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--active-threads", type=int, default=30)
    parser.add_argument("--session-threads", type=int, default=2)
    args = parser.parse_args()
    result = run(args)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
