"""Recover pair-native D1 DEVELOPMENT features from immutable consumed runs.

This performs no financial evaluation and opens no market/validation/holdout
sidecar. Frozen candidate members identify source root/wave/exact/result hashes;
the durable development-window evidence binds every physical-results file hash.
The script follows each physical result's source_record_sha256 into the already-
closed raw record and freezes primary/control absolute returns plus window paths.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

from our_system_phase2.services.unified_capability_registry import stable_hash

STATUS = "D1_PAIR_NATIVE_DEVELOPMENT_FEATURES_RECOVERED_FROM_IMMUTABLE_RESULTS"
WINDOWS = ("development_1", "development_2", "development_3")


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _verify(payload: Mapping[str, Any], field: str, label: str) -> str:
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if not claimed or stable_hash(body) != claimed:
        raise RuntimeError(f"{label} self-hash drift")
    return claimed


def _result_path(run_root: Path, wave: int) -> Path:
    wave_root = run_root / f"wave_{wave:03d}"
    candidates = (wave_root / "physical_results.jsonl", wave_root / "wave_physical_results.jsonl")
    existing = [path for path in candidates if path.is_file()]
    if len(existing) != 1:
        raise RuntimeError(f"physical result artifact cardinality drift: root={run_root} wave={wave}")
    return existing[0]


def _raw_records(run_root: Path, wave: int) -> dict[str, dict[str, Any]]:
    record_root = run_root / f"wave_{wave:03d}" / "records"
    files = sorted(record_root.glob("record_*.json"))
    if not files:
        raise RuntimeError(f"raw development records missing: {record_root}")
    rows: dict[str, dict[str, Any]] = {}
    for path in files:
        payload = _read(path)
        claimed = _verify(payload, "record_payload_sha256", f"raw record {path}")
        restored = dict(payload)
        restored["record_payload_sha256"] = claimed
        if claimed in rows:
            raise RuntimeError(f"duplicate raw record payload hash: {claimed}")
        rows[claimed] = restored
    return rows


def _window_returns(side: Mapping[str, Any], label: str) -> tuple[list[float], list[int]]:
    windows = {str(row["window_id"]): dict(row) for row in side.get("development_subwindows") or ()}
    if set(windows) != set(WINDOWS):
        raise RuntimeError(f"{label} development-window identity drift")
    returns = [float(windows[window]["cumulative_net_return"]) for window in WINDOWS]
    sessions = [int(windows[window]["session_count"]) for window in WINDOWS]
    if not all(math.isfinite(value) for value in returns) or any(count <= 0 for count in sessions):
        raise RuntimeError(f"{label} development-window value drift")
    return returns, sessions


def freeze(
    repo: Path,
    *,
    member_paths: Sequence[Path],
    development_window_evidence: Path,
) -> dict[str, Any]:
    repo = repo.resolve()
    evidence_path = development_window_evidence.resolve()
    evidence = _read(evidence_path)
    evidence_hash = _verify(evidence, "evidence_payload_sha256", "D1 development-window evidence")
    if (
        evidence.get("status") != "FROZEN_DURABLE_DEVELOPMENT_WINDOW_FEATURE_EVIDENCE"
        or int(evidence.get("candidate_count") or 0) != 183
        or bool(evidence.get("financial_evaluation_performed_by_this_freeze"))
        or int(evidence.get("validation_reads_by_this_freeze") or 0) != 0
        or int(evidence.get("holdout_reads_by_this_freeze") or 0) != 0
        or int(evidence.get("forward_2026_reads_by_this_freeze") or 0) != 0
    ):
        raise RuntimeError("D1 development-window evidence contract drift")

    expected_member_hashes = {
        str(row["file_sha256"]): int(row["candidate_count"])
        for row in evidence["source_candidate_member_files"]
    }
    members: list[dict[str, Any]] = []
    source_member_files = []
    for raw in member_paths:
        path = raw.resolve()
        file_hash = _sha(path)
        rows = _read_jsonl(path)
        if file_hash not in expected_member_hashes or expected_member_hashes[file_hash] != len(rows):
            raise RuntimeError(f"candidate-member source authority drift: {path}")
        members.extend(rows)
        source_member_files.append({"path": str(path), "file_sha256": file_hash, "candidate_count": len(rows)})
    exacts = [str(row.get("exact_identity") or "") for row in members]
    if len(members) != 183 or any(not exact for exact in exacts) or len(set(exacts)) != 183:
        raise RuntimeError("D1 pair-native member geometry drift")

    expected_results = {
        str(Path(str(row["path"])).resolve()).lower(): str(row["file_sha256"])
        for row in evidence["source_physical_result_files"]
    }
    grouped: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    for member in members:
        root = str(member.get("source_root") or "")
        wave = int(member.get("source_wave"))
        if not root:
            raise RuntimeError("candidate member missing source_root")
        grouped[(root, wave)].append(member)

    output_rows: list[dict[str, Any]] = []
    source_result_files: list[dict[str, Any]] = []
    source_raw_record_files: list[dict[str, Any]] = []
    for (root_text, wave), group in sorted(grouped.items(), key=lambda item: (item[0][0], item[0][1])):
        run_root = Path(root_text).resolve()
        result_path = _result_path(run_root, wave).resolve()
        result_key = str(result_path).lower()
        expected_hash = expected_results.get(result_key)
        actual_hash = _sha(result_path)
        if expected_hash is None or actual_hash != expected_hash:
            raise RuntimeError(f"physical result file authority drift: {result_path}")
        result_rows = _read_jsonl(result_path)
        by_exact = {str(row.get("exact_identity") or ""): dict(row) for row in result_rows}
        if len(by_exact) != len(result_rows):
            raise RuntimeError(f"duplicate exact identity in {result_path}")
        raw_by_hash = _raw_records(run_root, wave)
        source_result_files.append({"path": str(result_path), "file_sha256": actual_hash, "wave": wave})

        for member in group:
            exact = str(member["exact_identity"])
            result = by_exact.get(exact)
            if result is None:
                raise RuntimeError(f"missing physical result for {exact}")
            if str(result.get("source_record_sha256") or "") != str(member.get("source_record_sha256") or ""):
                raise RuntimeError(f"source_record_sha256 drift for {exact}")
            if str(result.get("physical_result_hash") or "") != str(member.get("physical_result_hash") or ""):
                raise RuntimeError(f"physical_result_hash drift for {exact}")
            source_hash = str(result["source_record_sha256"])
            raw = raw_by_hash.get(source_hash)
            if raw is None:
                raise RuntimeError(f"raw source record missing for {exact}: {source_hash}")
            if raw.get("primary") is None or raw.get("base_control") is None:
                raise RuntimeError(f"pair-native source record incomplete for {exact}")
            if str(raw.get("template_id") or "") != str(member.get("template_id") or ""):
                raise RuntimeError(f"template identity drift for {exact}")
            primary_windows, primary_sessions = _window_returns(raw["primary"], f"primary {exact}")
            control_windows, control_sessions = _window_returns(raw["base_control"], f"control {exact}")
            if primary_sessions != control_sessions:
                raise RuntimeError(f"primary/control session geometry drift for {exact}")
            matched_windows = [a - b for a, b in zip(primary_windows, control_windows, strict=True)]
            credit = dict((result.get("uplift") or {}).get("program_credit") or {})
            frozen_matched_windows = list(map(float, credit.get("window_return_increments") or ()))
            if len(frozen_matched_windows) != 3 or any(
                not math.isclose(left, right, rel_tol=0.0, abs_tol=1e-15)
                for left, right in zip(matched_windows, frozen_matched_windows, strict=True)
            ):
                raise RuntimeError(f"matched-window reconstruction drift for {exact}")
            matched_return = float(raw["matched_cumulative_return_increment"])
            matched_reward = float(raw["matched_net_reward_increment"])
            if not math.isclose(
                matched_return,
                float(member["development_matched_cumulative_net_return_increment"]),
                rel_tol=0.0,
                abs_tol=1e-15,
            ):
                raise RuntimeError(f"member matched return drift for {exact}")
            if not math.isclose(
                matched_reward,
                float(member["development_matched_net_reward_increment"]),
                rel_tol=0.0,
                abs_tol=1e-15,
            ):
                raise RuntimeError(f"member matched reward drift for {exact}")
            output_rows.append(
                {
                    "schema_version": "cn_program_optimizer_d1_pair_native_development_feature_v1",
                    "exact_identity": exact,
                    "source_cohort": str(member["source_cohort"]),
                    "template_id": str(member["template_id"]),
                    "selection_kind": str(member["selection_kind"]),
                    "source_wave": wave,
                    "primary_cumulative_net_return": float(raw["primary"]["cumulative_net_return"]),
                    "control_cumulative_net_return": float(raw["base_control"]["cumulative_net_return"]),
                    "matched_cumulative_net_return_increment": matched_return,
                    "primary_continuous_book_net_reward": float(raw["primary"]["continuous_book_net_reward"]),
                    "control_continuous_book_net_reward": float(raw["base_control"]["continuous_book_net_reward"]),
                    "matched_net_reward_increment": matched_reward,
                    "development_window_ids": list(WINDOWS),
                    "session_counts": primary_sessions,
                    "primary_window_returns": primary_windows,
                    "control_window_returns": control_windows,
                    "matched_window_return_increments": matched_windows,
                    "primary_mean_one_way_turnover": float(raw["primary"]["mean_one_way_turnover"]),
                    "control_mean_one_way_turnover": float(raw["base_control"]["mean_one_way_turnover"]),
                    "turnover_differential": float(raw["primary"]["mean_one_way_turnover"])
                    - float(raw["base_control"]["mean_one_way_turnover"]),
                    "control_program_id": str(raw["control_program_id"]),
                    "source_record_payload_sha256": source_hash,
                    "physical_result_hash": str(result["physical_result_hash"]),
                }
            )
        for record_path in sorted((run_root / f"wave_{wave:03d}" / "records").glob("record_*.json")):
            source_raw_record_files.append({"path": str(record_path.resolve()), "file_sha256": _sha(record_path), "wave": wave})

    output_rows.sort(key=lambda row: str(row["exact_identity"]))
    if len(output_rows) != 183 or len({row["exact_identity"] for row in output_rows}) != 183:
        raise RuntimeError("D1 pair-native output exact coverage drift")
    if {row["exact_identity"] for row in output_rows} != set(exacts):
        raise RuntimeError("D1 pair-native member/output exact set drift")

    cohort_counts: dict[str, int] = {}
    for row in output_rows:
        cohort = str(row["source_cohort"])
        cohort_counts[cohort] = cohort_counts.get(cohort, 0) + 1
    if dict(sorted(cohort_counts.items())) != {
        "D1_CONTINUATION_B": 63,
        "D1_FRESH": 64,
        "SUCCESSOR_D1": 56,
    }:
        raise RuntimeError(f"D1 pair-native cohort geometry drift: {cohort_counts}")

    payload = {
        "schema_version": "cn_program_optimizer_d1_pair_native_development_features_v1",
        "status": STATUS,
        "candidate_count": 183,
        "cohort_counts": dict(sorted(cohort_counts.items())),
        "candidate_exact_identities_sha256": stable_hash(sorted(exacts)),
        "source_development_window_evidence_file_sha256": _sha(evidence_path),
        "source_development_window_evidence_payload_sha256": evidence_hash,
        "source_candidate_member_files": source_member_files,
        "source_physical_result_files": source_result_files,
        "source_raw_record_file_count": len(source_raw_record_files),
        "source_raw_record_files_sha256": stable_hash(source_raw_record_files),
        "feature_rows": output_rows,
        "feature_rows_sha256": stable_hash(output_rows),
        "research_boundaries": {
            "financial_evaluation_performed": False,
            "market_or_sidecar_read": False,
            "validation_read": False,
            "holdout_read": False,
            "forward_read": False,
            "optimizer_feedback_write": "FORBIDDEN",
            "promotion_authorized": False,
        },
    }
    payload["evidence_payload_sha256"] = stable_hash(payload)
    return payload


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--members", type=Path, action="append", required=True)
    parser.add_argument("--development-window-evidence", type=Path, required=True)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("runtime/run_plans/cn_program_optimizer_d1_pair_native_development_features_20260825.json"),
    )
    args = parser.parse_args(argv)
    payload = freeze(
        args.repo_root,
        member_paths=args.members,
        development_window_evidence=args.development_window_evidence,
    )
    output = args.output if args.output.is_absolute() else args.repo_root / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "status": payload["status"],
                "candidate_count": payload["candidate_count"],
                "cohorts": payload["cohort_counts"],
                "payload": payload["evidence_payload_sha256"],
                "output": str(output.resolve()),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
