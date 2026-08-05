from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

import pandas as pd


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
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        )
        + "\n",
        encoding="utf-8",
    )


def _quality_rank(values: pd.Series, *, higher: bool = True) -> pd.Series:
    numeric = pd.to_numeric(values, errors="raise").astype(float)
    quality = numeric if higher else -numeric
    return quality.rank(method="average", pct=True)


def audit(
    *,
    contract_path: Path,
    input_root: Path,
    output_root: Path,
    audit_root: Path,
    auditor_commit_sha: str,
) -> dict[str, Any]:
    contract_path = contract_path.resolve()
    input_root = input_root.resolve()
    output_root = output_root.resolve()
    audit_root = audit_root.resolve()
    if audit_root.exists() and any(audit_root.iterdir()):
        raise RuntimeError(f"audit root must be empty: {audit_root}")
    audit_root.mkdir(parents=True, exist_ok=True)
    contract = _read_json(contract_path)
    manifest_path = output_root / "SELECTION_DIAGNOSTIC_COMPLETE.json"
    manifest = _read_json(manifest_path)
    body = dict(manifest)
    stored_body_hash = str(body.pop("manifest_body_sha256"))
    if _stable_hash(body) != stored_body_hash:
        raise RuntimeError("closure canonical self-hash mismatch")
    verified_artifacts = 0
    for record in manifest["artifacts"]:
        path = output_root / str(record["path"])
        if not path.is_file():
            raise RuntimeError(f"declared artifact missing: {path}")
        if path.stat().st_size != int(record["bytes"]):
            raise RuntimeError(f"declared artifact size mismatch: {path}")
        if _sha256(path) != str(record["sha256"]):
            raise RuntimeError(f"declared artifact hash mismatch: {path}")
        verified_artifacts += 1
    for record in contract["source_artifacts"]:
        path = input_root / str(record["scope"]) / str(record["name"])
        if _sha256(path) != str(record["sha256"]):
            raise RuntimeError(f"source artifact binding drift: {path}")

    review = pd.read_parquet(input_root / "freeze" / "finalist_review_ledger.parquet")
    oos = pd.read_parquet(input_root / "oos" / "oos_pair_metrics.parquet")
    diagnosis = pd.read_parquet(output_root / "candidate_diagnosis.parquet")
    if review["pair_id"].astype(str).nunique() != 32 or len(review) != 32:
        raise RuntimeError("independent source 32-pair check failed")
    if oos["pair_id"].astype(str).nunique() != 22 or len(oos) != 22:
        raise RuntimeError("independent source 22-label check failed")
    if diagnosis["pair_id"].astype(str).tolist() != review.sort_values(
        "source_pair_order", kind="stable"
    )["pair_id"].astype(str).tolist():
        raise RuntimeError("diagnostic identity/order drift")
    oos_map = oos.set_index(oos["pair_id"].astype(str))[
        "all_four_economic_gates_positive"
    ].astype(bool).to_dict()
    expected_groups = []
    for pair_id in diagnosis["pair_id"].astype(str):
        if pair_id not in oos_map:
            expected_groups.append("TRAIN_SCREEN_REJECT_OOS_LABEL_MISSING")
        elif oos_map[pair_id]:
            expected_groups.append("ADAPTIVE_VALIDATION_SURVIVOR")
        else:
            expected_groups.append(
                "ADAPTIVE_VALIDATION_ABSOLUTE_ONLY_RELATIVE_INCOMPLETE"
            )
    if diagnosis["diagnostic_group"].astype(str).tolist() != expected_groups:
        raise RuntimeError("independently derived group labels do not match")
    if diagnosis["diagnostic_group"].value_counts().to_dict() != {
        "ADAPTIVE_VALIDATION_ABSOLUTE_ONLY_RELATIVE_INCOMPLETE": 12,
        "ADAPTIVE_VALIDATION_SURVIVOR": 10,
        "TRAIN_SCREEN_REJECT_OOS_LABEL_MISSING": 10,
    }:
        raise RuntimeError("independently derived group counts drift")
    oos_columns = [
        column
        for column in diagnosis
        if column.startswith("oos_") and column != "oos_label_observed"
    ]
    if diagnosis.loc[~diagnosis["oos_label_observed"], oos_columns].notna().any().any():
        raise RuntimeError("unlabeled train rejects contain OOS values")

    scores = pd.DataFrame({"pair_id": diagnosis["pair_id"].astype(str)})
    scores["CURRENT_SEARCH_SCORE_BASELINE"] = _quality_rank(
        diagnosis["search_score"]
    )
    scores["TRAIN_DECODER_ECONOMIC_FLOOR"] = _quality_rank(
        diagnosis["decoder_absolute_and_matched_margin_floor"]
    )
    equal_axes = pd.DataFrame(
        {
            "ic": _quality_rank(diagnosis["train_rank_ic_hit_rate"]),
            "economic": _quality_rank(
                diagnosis["decoder_absolute_and_matched_margin_floor"]
            ),
            "efficiency": _quality_rank(
                diagnosis["primary_net_return_per_turnover"]
            ),
        }
    )
    scores["THREE_AXIS_EQUAL_RANK"] = equal_axes.mean(axis=1)
    scores["CONSERVATIVE_THREE_AXIS_FLOOR"] = pd.DataFrame(
        {
            "regime": _quality_rank(diagnosis["train_regime_stability_score"]),
            "economic": equal_axes["economic"],
            "efficiency": equal_axes["efficiency"],
        }
    ).min(axis=1)
    for ranker in scores.columns[1:]:
        if not scores[ranker].equals(
            pd.to_numeric(diagnosis[ranker], errors="raise").astype(float)
        ):
            delta = (
                scores[ranker]
                - pd.to_numeric(diagnosis[ranker], errors="raise").astype(float)
            ).abs().max()
            if float(delta) > 1e-15:
                raise RuntimeError(f"independent ranker score mismatch: {ranker}")

    precision = pd.read_parquet(output_root / "feature_precision_at_k.parquet")
    labeled = diagnosis.loc[diagnosis["oos_label_observed"]].copy()
    high = set(contract["predeclared_feature_directions"]["higher_is_better"])
    low = set(contract["predeclared_feature_directions"]["lower_is_better"])
    checked_precision_rows = 0
    for row in precision.itertuples(index=False):
        feature = str(row.feature)
        higher = feature in high
        if not higher and feature not in low:
            raise RuntimeError(f"uncontracted feature direction: {feature}")
        ranked = labeled.assign(
            _value=pd.to_numeric(labeled[feature], errors="raise").astype(float)
        ).sort_values(
            ["_value", "pair_id"],
            ascending=[not higher, True],
            kind="stable",
        )
        selected = ranked.head(int(row.k))
        hits = int(selected["oos_all_four_economic_gates_positive"].astype(bool).sum())
        if hits != int(row.survivor_count):
            raise RuntimeError(f"independent precision-at-k mismatch: {feature}/{row.k}")
        checked_precision_rows += 1

    ranker_table = pd.read_parquet(output_root / "ranker_comparison.parquet")
    top10 = ranker_table.loc[ranker_table["k"].astype(int).eq(10)].copy()
    baseline_hits = int(
        top10.loc[
            top10["ranker_id"].astype(str).eq("CURRENT_SEARCH_SCORE_BASELINE"),
            "survivor_count",
        ].iloc[0]
    )
    gate = contract["experimental_ranker_freeze_gate"]
    gate_rows: list[dict[str, Any]] = []
    for row in top10.itertuples(index=False):
        passes = (
            int(row.survivor_count) >= int(gate["minimum_top10_survivor_count"])
            and int(row.survivor_count) > baseline_hits
            and int(row.leave_one_out_top10_survivor_count_minimum)
            >= int(gate["leave_one_out_top10_survivor_count_minimum"])
        )
        gate_rows.append(
            {
                "ranker_id": str(row.ranker_id),
                "top10_survivor_count": int(row.survivor_count),
                "leave_one_out_top10_survivor_count_minimum": int(
                    row.leave_one_out_top10_survivor_count_minimum
                ),
                "passes_freeze_gate": passes,
            }
        )
    if any(record["passes_freeze_gate"] for record in gate_rows):
        ranker_freeze_verdict = "GATE_PASS_CANDIDATE_PRESENT"
    else:
        ranker_freeze_verdict = "HOLD_RESEARCH_NO_RANKER_FREEZE"

    audit_payload: dict[str, Any] = {
        "schema_version": "cn_alpha_selection_diagnostic_independent_audit_v1",
        "status": "PASS",
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "auditor_commit_sha": auditor_commit_sha,
        "audit_source_sha256": _sha256(Path(__file__).resolve()),
        "diagnostic_manifest_file_sha256": _sha256(manifest_path),
        "diagnostic_manifest_body_sha256": stored_body_hash,
        "artifact_count_verified": verified_artifacts,
        "source_artifact_count_verified": len(contract["source_artifacts"]),
        "source_pair_count": 32,
        "labeled_pair_count": 22,
        "survivor_count": 10,
        "relative_incomplete_count": 12,
        "unlabeled_train_reject_count": 10,
        "unlabeled_negative_imputation": False,
        "precision_rows_independently_recomputed": checked_precision_rows,
        "ranker_gate_rows": gate_rows,
        "ranker_freeze_verdict": ranker_freeze_verdict,
        "new_financial_reads": 0,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion_authorized": False,
    }
    audit_payload["audit_body_sha256"] = _stable_hash(audit_payload)
    audit_path = audit_root / "audit.json"
    _write_json(audit_path, audit_payload)
    return {
        "status": "PASS",
        "audit_path": str(audit_path),
        "audit_file_sha256": _sha256(audit_path),
        "audit_body_sha256": audit_payload["audit_body_sha256"],
        "ranker_freeze_verdict": ranker_freeze_verdict,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--contract", type=Path, required=True)
    parser.add_argument("--input-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--audit-root", type=Path, required=True)
    parser.add_argument("--auditor-commit-sha", required=True)
    args = parser.parse_args()
    result = audit(
        contract_path=args.contract,
        input_root=args.input_root,
        output_root=args.output_root,
        audit_root=args.audit_root,
        auditor_commit_sha=args.auditor_commit_sha,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
