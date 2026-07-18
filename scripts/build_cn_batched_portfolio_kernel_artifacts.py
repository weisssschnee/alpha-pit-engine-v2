from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess
from typing import Any, Mapping, Sequence

if __package__:
    from .qualify_cn_batched_portfolio_kernel import compare_kernel_runs
else:  # pragma: no cover - direct script execution
    from qualify_cn_batched_portfolio_kernel import compare_kernel_runs


OUTPUT_NAMES = (
    "frozen_contract.json",
    "benchmark_profile.json",
    "parity_receipt.json",
    "run_manifest.json",
)
QUALIFIED = "CN_BATCHED_PORTFOLIO_KERNEL_QUALIFIED"
PARTIAL = "CN_BATCHED_PORTFOLIO_KERNEL_PARTIALLY_QUALIFIED"
PARITY_FAILED = "CN_BATCHED_PORTFOLIO_KERNEL_PARITY_FAILED"
PORTFOLIO_SOURCE = Path(
    "src/our_system_phase2/services/phase3cm_streaming_portfolio.py"
)
EXPECTED_CHECKPOINT_SECTIONS = {
    "temporal",
    "state",
    "support",
    "portfolio",
    "reducer",
}


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)
        + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
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


def _require_sha256(value: str, label: str) -> str:
    normalized = value.strip().lower()
    if len(normalized) != 64 or any(character not in "0123456789abcdef" for character in normalized):
        raise ValueError(f"{label} must be a 64-character SHA-256")
    return normalized


def _require_git_sha(value: str, label: str) -> str:
    normalized = value.strip().lower()
    if len(normalized) != 40 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise ValueError(f"{label} must be a 40-character Git commit SHA")
    return normalized


def _git(repo_root: Path, *arguments: str) -> bytes:
    return subprocess.check_output(
        ["git", "-C", str(repo_root), *arguments], stderr=subprocess.STDOUT
    )


def _verify_repo_head(repo_root: Path, repo_sha: str) -> str:
    expected = _require_git_sha(repo_sha, "repo SHA")
    actual = _git(repo_root, "rev-parse", "HEAD").decode("ascii").strip().lower()
    if actual != expected:
        raise ValueError(f"repo SHA does not match Git HEAD: expected {expected}, got {actual}")
    return actual


def _verify_revision_source(
    repo_root: Path, revision: str, expected_sha256: str, label: str
) -> dict[str, Any]:
    commit = _git(repo_root, "rev-parse", f"{revision}^{{commit}}").decode("ascii").strip()
    commit = _require_git_sha(commit, f"{label} commit")
    content = _git(repo_root, "show", f"{commit}:{PORTFOLIO_SOURCE.as_posix()}")
    lf_sha = hashlib.sha256(content).hexdigest()
    crlf_sha = hashlib.sha256(content.replace(b"\n", b"\r\n")).hexdigest()
    expected = _require_sha256(expected_sha256, f"{label} portfolio SHA-256")
    if expected not in {lf_sha, crlf_sha}:
        raise ValueError(f"{label} portfolio SHA-256 is not bound to Git revision {commit}")
    blob = _git(
        repo_root, "rev-parse", f"{commit}:{PORTFOLIO_SOURCE.as_posix()}"
    ).decode("ascii").strip()
    return {
        "commit": commit,
        "git_blob": blob,
        "portfolio_sha256": expected,
        "checkout_eol": "LF" if expected == lf_sha else "CRLF",
    }


def _verify_current_source(repo_root: Path, expected_sha256: str) -> dict[str, Any]:
    source = repo_root / PORTFOLIO_SOURCE
    actual = _require_file_hash(source, expected_sha256, "iteration2 portfolio SHA-256")
    return {
        "path": str(source.resolve()),
        "sha256": actual,
        "bound_to_current_worktree": True,
    }


def _require_file_hash(path: Path, expected: str, label: str) -> str:
    actual = _sha256(path)
    if actual != _require_sha256(expected, label):
        raise ValueError(f"{label} mismatch: expected {expected}, got {actual}")
    return actual


def _zero_read_boundary(value: Mapping[str, Any], label: str) -> None:
    for field in ("validation_reads", "holdout_reads", "forward_2026_reads"):
        if field not in value or int(value[field]) != 0:
            raise ValueError(f"{label} does not prove {field}=0")


def _binding_boundary(binding: Mapping[str, Any]) -> None:
    sealed = binding.get("sealed_reads")
    if not isinstance(sealed, Mapping):
        _zero_read_boundary(binding, "frozen binding")
        return
    for field in ("validation", "holdout", "forward_2026"):
        if field not in sealed or int(sealed[field]) != 0:
            raise ValueError(f"frozen binding does not prove sealed_reads.{field}=0")


def _verify_golden_index(path: Path, index: Mapping[str, Any]) -> dict[str, Any]:
    _zero_read_boundary(index.get("boundaries") or {}, "golden artifact index")
    artifacts = index.get("artifacts")
    if not isinstance(artifacts, list) or not artifacts:
        raise ValueError("golden artifact index has no artifacts")
    verified: list[dict[str, Any]] = []
    for raw in artifacts:
        relative = Path(str(raw.get("path") or ""))
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError(f"golden artifact path is not bundle-relative: {relative}")
        artifact = path.parent / relative
        if not artifact.is_file():
            raise FileNotFoundError(f"golden artifact missing: {artifact}")
        expected = _require_sha256(str(raw.get("sha256") or ""), f"golden:{relative}")
        actual = _sha256(artifact)
        if actual != expected:
            raise ValueError(f"golden artifact hash drift: {relative}")
        verified.append({"path": relative.as_posix(), "sha256": actual, "bytes": artifact.stat().st_size})
    return {
        "artifact_index_sha256": _sha256(path),
        "verified_artifact_count": len(verified),
        "verified_artifact_digest": _stable_hash(verified),
        "historical_golden_unchanged": True,
    }


def _verify_binding(path: Path, expected_sha256: str) -> tuple[dict[str, Any], dict[str, Any]]:
    expected_sha256 = _require_sha256(expected_sha256, "frozen binding SHA-256")
    metadata = _read_json(path)
    if not metadata.get("binding_hash"):
        raise ValueError("frozen binding must contain the original binding content and binding_hash")
    file_sha = _require_file_hash(path, expected_sha256, "frozen binding SHA-256")
    _binding_boundary(metadata)
    claimed = _require_sha256(str(metadata["binding_hash"]), "frozen binding self-hash")
    body = dict(metadata)
    body.pop("binding_hash", None)
    if _stable_hash(body) != claimed:
        raise ValueError("frozen binding self-hash drift")
    split_manifest_hash = _require_sha256(
        str(metadata.get("split_manifest_hash") or ""),
        "frozen binding split_manifest_hash",
    )
    if str(metadata.get("data_role") or "") not in {"development", "development_train_only"}:
        raise ValueError("frozen binding data_role is not development-only")
    if metadata.get("promotion") != "FORBIDDEN":
        raise ValueError("frozen binding does not forbid promotion")
    if metadata.get("strict_stage_a") != "NOT_AUTHORIZED":
        raise ValueError("frozen binding does not keep strict Stage A unauthorized")
    return metadata, {
        "path": str(path.resolve()),
        "sha256": file_sha,
        "binding_hash": claimed,
        "metadata_sha256": _sha256(path),
        "binding_content_verified": True,
        "split_manifest_hash": split_manifest_hash,
        "data_role": "development_train_only",
        "status": metadata.get("status"),
    }


def _phase_wall(result: Mapping[str, Any], phase: str) -> float | None:
    try:
        return float((result.get("phase_totals") or {}).get(phase, {}).get("wall_seconds"))
    except (TypeError, ValueError):
        return None


def _result_evidence(
    root: Path,
    binding_hash: str,
    split_manifest_hash: str,
    label: str,
    *,
    source_label_token: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    result_path = root / "CN_STREAMING_BACKEND_RESULT.json"
    result = _read_json(result_path)
    _zero_read_boundary(result, f"{label} result")
    if str(result.get("input_binding_hash") or "") != binding_hash:
        raise ValueError(f"{label} input binding mismatch")
    if str(result.get("split_manifest_hash") or "") != split_manifest_hash:
        raise ValueError(f"{label} split manifest mismatch")
    if result.get("promotion") != "FORBIDDEN":
        raise ValueError(f"{label} does not forbid promotion")
    if result.get("strict_stage_a") != "NOT_AUTHORIZED":
        raise ValueError(f"{label} does not keep Stage A unauthorized")
    plan_path = root / "CN_FROZEN_EXECUTION_PLAN.json"
    plan = _read_json(plan_path) if plan_path.is_file() else {}
    command_path = root.parent / "command.json"
    if not command_path.is_file():
        raise FileNotFoundError(f"{label} command evidence missing: {command_path}")
    command = _read_json(command_path)
    source_label = str(command.get("source_label") or "")
    if source_label_token.lower() not in source_label.lower():
        raise ValueError(
            f"{label} command source_label does not bind token {source_label_token!r}"
        )
    summary = {
        "root": str(root.resolve()),
        "result_sha256": _sha256(result_path),
        "status": result.get("status"),
        "backend": result.get("backend"),
        "phase": result.get("phase"),
        "pair_count": result.get("pair_count"),
        "candidate_count": result.get("candidate_count"),
        "wall_seconds": result.get("wall_seconds"),
        "phase_wall_seconds": {
            name: _phase_wall(result, name)
            for name in (
                "expression_value_dag",
                "cross_sectional_rank_mapping",
                "turnover_and_cost",
                "streaming_reducer",
                "checkpoint",
            )
        },
        "peak_rss_bytes": result.get("peak_rss_bytes"),
        "parallelism_status": result.get("parallelism_status"),
        "input_binding_hash": result.get("input_binding_hash"),
        "split_manifest_hash": split_manifest_hash,
        "execution_plan": {
            "path": str(plan_path.resolve()) if plan_path.is_file() else None,
            "sha256": _sha256(plan_path) if plan_path.is_file() else None,
            "execution_plan_hash": plan.get("execution_plan_hash"),
            "compute_threads": plan.get("compute_threads"),
            "primary_thread_pool": plan.get("primary_thread_pool"),
            "thread_environment": plan.get("thread_environment"),
            "pair_batch_count": len(plan.get("pair_batches") or []),
            "pair_batch_size": max(
                (len(batch) for batch in (plan.get("pair_batches") or [])), default=0
            ),
        },
        "command_evidence": {
            "path": str(command_path.resolve()),
            "sha256": _sha256(command_path),
            "source_label": source_label,
            "source_label_token_verified": source_label_token,
            "python": command.get("python"),
            "arguments": command.get("arguments"),
            "thread_environment": command.get("thread_environment"),
        },
        "access": {"validation_reads": 0, "holdout_reads": 0, "forward_2026_reads": 0},
    }
    return result, summary


def _same_path(left: Any, right: Path) -> bool:
    if not left:
        return False
    return os.path.normcase(str(Path(str(left)).resolve())) == os.path.normcase(
        str(right.resolve())
    )


def _numbers_equal(left: Any, right: Any) -> bool:
    try:
        return math.isclose(float(left), float(right), rel_tol=1e-12, abs_tol=1e-12)
    except (TypeError, ValueError):
        return False


def _compute_wall(result: Mapping[str, Any]) -> float:
    phases = result.get("phase_totals") or {}
    return sum(
        float((phases.get(name) or {}).get("wall_seconds"))
        for name in (
            "expression_value_dag",
            "cross_sectional_rank_mapping",
            "turnover_and_cost",
        )
    )


def _checkpoint_parity_pass(
    value: Mapping[str, Any],
    reference_result: Mapping[str, Any],
    candidate_result: Mapping[str, Any],
    reference_summary: Mapping[str, Any],
    candidate_summary: Mapping[str, Any],
    performance: Mapping[str, Any],
) -> bool:
    status = str(value.get("status") or "")
    if not status.endswith("PASS"):
        return False
    parity = value.get("parity")
    if not isinstance(parity, Mapping) or set(parity) != EXPECTED_CHECKPOINT_SECTIONS:
        return False
    if not all(
        isinstance(row, Mapping)
        and row.get("exact") is True
        and isinstance(row.get("mismatches"), list)
        and not row["mismatches"]
        for row in parity.values()
    ):
        return False
    _zero_read_boundary(value, "checkpoint parity")
    if value.get("data_role") != "development_train_only":
        return False
    if value.get("backend") != reference_result.get("backend") or value.get(
        "backend"
    ) != candidate_result.get("backend"):
        return False
    expected_rows = {
        "reference": (reference_result, reference_summary),
        "candidate": (candidate_result, candidate_summary),
    }
    for side, (result, summary) in expected_rows.items():
        raw = value.get(side)
        plan = summary.get("execution_plan") or {}
        if not isinstance(raw, Mapping):
            return False
        expected = {
            "compute_threads": plan.get("compute_threads"),
            "compute_wall_seconds": _compute_wall(result),
            "mapping_wall_seconds": _phase_wall(result, "cross_sectional_rank_mapping"),
            "pair_batch_size": plan.get("pair_batch_size"),
            "peak_rss_bytes": result.get("peak_rss_bytes"),
        }
        if any(not _numbers_equal(raw.get(field), expected_value) for field, expected_value in expected.items()):
            return False
    if not _numbers_equal(value.get("mapping_speedup"), performance.get("mapping_speedup")):
        return False
    if not _numbers_equal(value.get("compute_speedup"), performance.get("compute_speedup")):
        return False
    return True


def _attempt_evidence(
    *,
    attempt_id: str,
    result_root: Path,
    qualification_path: Path,
    checkpoint_path: Path,
    portfolio_sha256: str,
    binding_hash: str,
    split_manifest_hash: str,
    reference_root: Path,
    reference_raw: Mapping[str, Any],
    reference_summary: Mapping[str, Any],
    source_label_token: str,
) -> dict[str, Any]:
    candidate_raw, result = _result_evidence(
        result_root,
        binding_hash,
        split_manifest_hash,
        attempt_id,
        source_label_token=source_label_token,
    )
    qualification = _read_json(qualification_path)
    checkpoint = _read_json(checkpoint_path)
    _zero_read_boundary(qualification, f"{attempt_id} qualification")
    _zero_read_boundary(checkpoint, f"{attempt_id} checkpoint parity")
    if not _same_path(qualification.get("reference_root"), reference_root):
        raise ValueError(f"{attempt_id} qualification reference_root mismatch")
    if not _same_path(qualification.get("candidate_root"), result_root):
        raise ValueError(f"{attempt_id} qualification candidate_root mismatch")
    recomputed = compare_kernel_runs(reference_root, result_root)
    required_qualification_fields = (
        "status",
        "comparable",
        "semantic_parity_exact",
        "comparability",
        "semantic_parity",
        "reference_artifact_hashes",
        "candidate_artifact_hashes",
        "missing_artifacts",
        "next_decision",
    )
    for field in required_qualification_fields:
        if _stable_hash(qualification.get(field)) != _stable_hash(recomputed.get(field)):
            raise ValueError(f"{attempt_id} qualification {field} is not bound to actual roots")
    performance_fields = (
        "reference_wall_seconds",
        "candidate_wall_seconds",
        "wall_speedup",
        "reference_mapping_wall_seconds",
        "candidate_mapping_wall_seconds",
        "mapping_speedup",
        "reference_compute_wall_seconds",
        "candidate_compute_wall_seconds",
        "compute_speedup",
        "candidate_mapping_effective_cores",
        "allocated_compute_threads",
        "minimum_effective_cores",
        "mapping_parallelism_engaged",
    )
    for field in performance_fields:
        if not _numbers_equal(
            (qualification.get("performance") or {}).get(field),
            (recomputed.get("performance") or {}).get(field),
        ) and (
            (qualification.get("performance") or {}).get(field)
            != (recomputed.get("performance") or {}).get(field)
        ):
            raise ValueError(f"{attempt_id} qualification performance.{field} drift")
    artifact_hash_match = {
        name: expected == (recomputed.get("candidate_artifact_hashes") or {}).get(name)
        for name, expected in (qualification.get("candidate_artifact_hashes") or {}).items()
    }
    artifacts_bound = bool(artifact_hash_match) and all(artifact_hash_match.values())
    checkpoint_pass = _checkpoint_parity_pass(
        checkpoint,
        reference_raw,
        candidate_raw,
        reference_summary,
        result,
        recomputed.get("performance") or {},
    )
    if not checkpoint_pass:
        raise ValueError(f"{attempt_id} checkpoint parity is incomplete, unbound, or non-exact")
    qualification_status = str(recomputed.get("status") or "")
    eligible = all(
        (
            qualification_status == QUALIFIED,
            recomputed.get("comparable") is True,
            recomputed.get("semantic_parity_exact") is True,
            artifacts_bound,
            checkpoint_pass,
        )
    )
    return {
        "attempt_id": attempt_id,
        "portfolio_sha256": _require_sha256(portfolio_sha256, f"{attempt_id} portfolio SHA-256"),
        "result": result,
        "qualification": {
            "path": str(qualification_path.resolve()),
            "sha256": _sha256(qualification_path),
            "status": qualification_status,
            "comparable": recomputed.get("comparable"),
            "semantic_parity_exact": recomputed.get("semantic_parity_exact"),
            "performance": recomputed.get("performance"),
            "artifact_hash_match": artifact_hash_match,
            "candidate_artifacts_bound": artifacts_bound,
            "reference_root_bound": True,
            "candidate_root_bound": True,
            "recomputed_qualification_digest": _stable_hash(recomputed),
        },
        "checkpoint_parity": {
            "path": str(checkpoint_path.resolve()),
            "sha256": _sha256(checkpoint_path),
            "status": checkpoint.get("status"),
            "exact_pass": checkpoint_pass,
        },
        "eligible_for_separate_146_replay": eligible,
    }


def _overall_status(attempts: Sequence[Mapping[str, Any]]) -> str:
    if any(attempt.get("eligible_for_separate_146_replay") is True for attempt in attempts):
        return QUALIFIED
    statuses = [str((attempt.get("qualification") or {}).get("status") or "") for attempt in attempts]
    evidence_failed = any(
        (attempt.get("qualification") or {}).get("semantic_parity_exact") is not True
        or (attempt.get("qualification") or {}).get("comparable") is not True
        or (attempt.get("qualification") or {}).get("candidate_artifacts_bound") is not True
        or (attempt.get("checkpoint_parity") or {}).get("exact_pass") is not True
        for attempt in attempts
    )
    if evidence_failed or any(
        status in {PARITY_FAILED, "CN_BATCHED_PORTFOLIO_KERNEL_SUBSET_NOT_COMPARABLE"}
        for status in statuses
    ):
        return PARITY_FAILED
    if PARTIAL in statuses:
        return PARTIAL
    return "CN_BATCHED_PORTFOLIO_KERNEL_NOT_QUALIFIED"


def _retained_attempt(attempts: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    for attempt in reversed(attempts):
        qualification = attempt.get("qualification") or {}
        checkpoint = attempt.get("checkpoint_parity") or {}
        if all(
            (
                qualification.get("status") in {QUALIFIED, PARTIAL},
                qualification.get("comparable") is True,
                qualification.get("semantic_parity_exact") is True,
                qualification.get("candidate_artifacts_bound") is True,
                checkpoint.get("exact_pass") is True,
            )
        ):
            return attempt
    return None


def _prepare_output_root(output_root: Path) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    unexpected = sorted(path.name for path in output_root.iterdir() if path.name not in OUTPUT_NAMES)
    if unexpected:
        raise ValueError(f"output root contains unexpected entries: {unexpected}")


def build_final_artifacts(
    *,
    golden_artifact_index: Path,
    frozen_binding: Path,
    frozen_binding_sha256: str,
    baseline_result_root: Path,
    fb25_result_root: Path,
    fb25_qualification: Path,
    fb25_checkpoint_parity: Path,
    repo_sha: str,
    baseline_portfolio_sha256: str,
    fb25_portfolio_sha256: str,
    output_root: Path,
    iteration2_result_root: Path | None = None,
    iteration2_qualification: Path | None = None,
    iteration2_checkpoint_parity: Path | None = None,
    iteration2_portfolio_sha256: str | None = None,
    report_path: Path | None = None,
    remote_machine: str | None = None,
    test_evidence: Sequence[str] = (),
    repo_root: Path | None = None,
    fb25_repo_revision: str = "fb25a91",
) -> dict[str, Any]:
    optional = (
        iteration2_result_root,
        iteration2_qualification,
        iteration2_checkpoint_parity,
        iteration2_portfolio_sha256,
    )
    if any(item is not None for item in optional) and not all(item is not None for item in optional):
        raise ValueError("iteration2 arguments must be supplied together")
    output_root = output_root.resolve()
    repo_root = (repo_root or Path(__file__).resolve().parents[1]).resolve()
    repo_sha = _verify_repo_head(repo_root, repo_sha)
    if report_path is not None and output_root in report_path.resolve().parents:
        raise ValueError("optional report must be outside the strict four-JSON output root")
    _prepare_output_root(output_root)

    golden_artifact_index = golden_artifact_index.resolve()
    golden_index = _read_json(golden_artifact_index)
    golden_verification = _verify_golden_index(golden_artifact_index, golden_index)
    historical_golden_repo_sha = _require_git_sha(
        str((golden_index.get("repo_heads") or {}).get("phase3cm_evaluator_sha") or ""),
        "historical golden repo SHA",
    )
    baseline_source = _verify_revision_source(
        repo_root,
        historical_golden_repo_sha,
        baseline_portfolio_sha256,
        "baseline",
    )
    fb25_source = _verify_revision_source(
        repo_root,
        fb25_repo_revision,
        fb25_portfolio_sha256,
        "fb25",
    )
    iteration2_source = (
        _verify_current_source(repo_root, str(iteration2_portfolio_sha256))
        if iteration2_portfolio_sha256 is not None
        else None
    )
    binding, binding_evidence = _verify_binding(frozen_binding.resolve(), frozen_binding_sha256)
    binding_hash = str(binding["binding_hash"])
    split_manifest_hash = str(binding_evidence["split_manifest_hash"])
    baseline_result_root = baseline_result_root.resolve()
    baseline_raw, baseline = _result_evidence(
        baseline_result_root,
        binding_hash,
        split_manifest_hash,
        "baseline",
        source_label_token=historical_golden_repo_sha[:7],
    )

    attempts = [
        _attempt_evidence(
            attempt_id="fb25a91",
            result_root=fb25_result_root.resolve(),
            qualification_path=fb25_qualification.resolve(),
            checkpoint_path=fb25_checkpoint_parity.resolve(),
            portfolio_sha256=fb25_portfolio_sha256,
            binding_hash=binding_hash,
            split_manifest_hash=split_manifest_hash,
            reference_root=baseline_result_root,
            reference_raw=baseline_raw,
            reference_summary=baseline,
            source_label_token="fb25",
        )
    ]
    if all(item is not None for item in optional):
        attempts.append(
            _attempt_evidence(
                attempt_id="iteration2",
                result_root=iteration2_result_root.resolve(),  # type: ignore[union-attr]
                qualification_path=iteration2_qualification.resolve(),  # type: ignore[union-attr]
                checkpoint_path=iteration2_checkpoint_parity.resolve(),  # type: ignore[union-attr]
                portfolio_sha256=str(iteration2_portfolio_sha256),
                binding_hash=binding_hash,
                split_manifest_hash=split_manifest_hash,
                reference_root=baseline_result_root,
                reference_raw=baseline_raw,
                reference_summary=baseline,
                source_label_token="order_reuse",
            )
        )

    status = _overall_status(attempts)
    replay_candidate = next(
        (attempt for attempt in reversed(attempts) if attempt["eligible_for_separate_146_replay"]),
        None,
    )
    retained = _retained_attempt(attempts)
    next_decision = (
        "PREPARE_SEPARATE_146_REPLAY" if status == QUALIFIED else "STOP_BEFORE_146_REPLAY"
    )
    boundaries = {
        "data_role": "development_train_only",
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "forward_2026": "SEALED",
        "candidate_promotion": "FORBIDDEN",
        "strict_stage_a": "NOT_AUTHORIZED",
        "wave_1024": "REQUIRES_SEPARATE_USER_AUTHORIZATION",
    }

    frozen_contract = {
        "schema_version": "cn_batched_portfolio_kernel_frozen_contract_v1",
        "status": status,
        "repo_sha": repo_sha,
        "source_identity": {
            "historical_golden_repo_sha": historical_golden_repo_sha,
            "current_repo_sha": repo_sha,
            "baseline": baseline_source,
            "fb25": fb25_source,
            "iteration2": iteration2_source,
        },
        "golden_evidence": golden_verification,
        "frozen_input_binding": binding_evidence,
        "workload": {
            "qualification_scope": "same-input engineering subset only",
            "historical_full_pack": {
                "total_pairs": 256,
                "active_bar_pairs": 146,
                "immutable_stock_session_pairs": 110,
            },
            "current_kernel_full_pack_execution": "NOT_EXECUTED",
        },
        "portfolio_semantics": {
            "turnover": "one_way_overlap_turnover",
            "cost_formula": "net_return = raw_return - one_way_cost * turnover",
            "semantic_change_authorized": False,
        },
        "qualification_thresholds": {
            "qualified_wall_speedup_gte": 2.0,
            "partially_qualified_wall_speedup_gte": 1.3,
            "exact_semantic_parity_required": True,
            "checkpoint_parity_required": True,
            "parallelism_gate_required": True,
        },
        "boundaries": boundaries,
    }

    benchmark_profile = {
        "schema_version": "cn_batched_portfolio_kernel_benchmark_profile_v1",
        "status": status,
        "selection_basis": "engineering parity, access boundaries, checkpoint parity, and runtime only; reward not used",
        "historical_golden": {
            "status": "UNCHANGED",
            "artifact_index_sha256": golden_verification["artifact_index_sha256"],
            "hot_path_bottleneck": (golden_index.get("phase3cm_summary") or {}).get("hot_path_bottleneck"),
            "full_pack_execution": "HISTORICAL_7DF57CE_EVIDENCE_ONLY",
        },
        "same_input_subset": {
            "baseline": baseline,
            "attempts": attempts,
            "retained_engineering_attempt_id": retained["attempt_id"] if retained is not None else None,
            "replay_candidate_attempt_id": (
                replay_candidate["attempt_id"] if replay_candidate is not None else None
            ),
        },
        "full_256_current_kernel_benchmark": "NOT_EXECUTED_BY_CURRENT_KERNEL",
        "boundaries": boundaries,
    }

    parity_receipt = {
        "schema_version": "cn_batched_portfolio_kernel_parity_receipt_v1",
        "status": status,
        "subset_attempts": [
            {
                "attempt_id": attempt["attempt_id"],
                "qualification_status": attempt["qualification"]["status"],
                "comparable": attempt["qualification"]["comparable"],
                "semantic_parity_exact": attempt["qualification"]["semantic_parity_exact"],
                "candidate_artifacts_bound": attempt["qualification"]["candidate_artifacts_bound"],
                "checkpoint_parity_status": attempt["checkpoint_parity"]["status"],
                "checkpoint_parity_exact": attempt["checkpoint_parity"]["exact_pass"],
                "eligible_for_separate_146_replay": attempt["eligible_for_separate_146_replay"],
            }
            for attempt in attempts
        ],
        "full_256": {
            "status": "NOT_EXECUTED_BY_CURRENT_KERNEL",
            "current_active_bar_pairs_replayed": 0,
            "current_stock_session_pairs_replayed": 0,
            "historical_composition": "146 active_bar + 110 immutable stock_session = 256",
            "historical_golden_unchanged": True,
            "claim": "No current-kernel 146-pair or 256-pair parity claim is made.",
        },
        "next_decision": next_decision,
        "retained_engineering_attempt_id": retained["attempt_id"] if retained is not None else None,
        "boundaries": boundaries,
    }

    _write_json(output_root / "frozen_contract.json", frozen_contract)
    _write_json(output_root / "benchmark_profile.json", benchmark_profile)
    _write_json(output_root / "parity_receipt.json", parity_receipt)
    run_manifest = {
        "schema_version": "cn_batched_portfolio_kernel_run_manifest_v1",
        "status": status,
        "repo_sha": repo_sha,
        "inputs": {
            "golden_artifact_index": {
                "path": str(golden_artifact_index),
                "sha256": golden_verification["artifact_index_sha256"],
            },
            "frozen_binding": binding_evidence,
            "baseline_result": baseline,
            "attempt_evidence": [
                {
                    "attempt_id": attempt["attempt_id"],
                    "result_sha256": attempt["result"]["result_sha256"],
                    "qualification_sha256": attempt["qualification"]["sha256"],
                    "checkpoint_parity_sha256": attempt["checkpoint_parity"]["sha256"],
                }
                for attempt in attempts
            ],
        },
        "outputs": {
            name: {
                "path": str(output_root / name),
                "sha256": _sha256(output_root / name),
            }
            for name in OUTPUT_NAMES[:-1]
        },
        "decision": {
            "retained_engineering_attempt_id": retained["attempt_id"] if retained is not None else None,
            "replay_candidate_attempt_id": (
                replay_candidate["attempt_id"] if replay_candidate is not None else None
            ),
            "next_decision": next_decision,
            "full_256_current_kernel": "NOT_EXECUTED_BY_CURRENT_KERNEL",
            "historical_golden": "UNCHANGED",
        },
        "execution_evidence": {
            "remote_machine": remote_machine or "NOT_RECORDED",
            "baseline_execution_plan": baseline["execution_plan"],
            "retained_execution_plan": (
                retained["result"]["execution_plan"] if retained is not None else None
            ),
            "retained_command": (
                retained["result"]["command_evidence"] if retained is not None else None
            ),
            "tests": list(test_evidence),
            "resource_accounting": (
                {
                    "basis": "observed_peak_rss",
                    "peak_rss_delta_bytes": (
                        retained["qualification"].get("performance") or {}
                    ).get("peak_rss_delta_bytes"),
                    "mapping_temporary_bytes": "NOT_USED_FOR_QUALIFICATION",
                    "note": (
                        "Executed mapping_temporary_bytes telemetry excludes label-order "
                        "scratch; resource qualification therefore relies on observed peak RSS."
                    ),
                }
                if retained is not None
                else None
            ),
        },
        "boundaries": boundaries,
    }
    _write_json(output_root / "run_manifest.json", run_manifest)
    if set(path.name for path in output_root.iterdir()) != set(OUTPUT_NAMES):
        raise RuntimeError("strict output root does not contain exactly four JSON artifacts")

    if report_path is not None:
        performance = {} if retained is None else retained["qualification"].get("performance") or {}
        speedup = performance.get("wall_speedup")
        mapping_speedup = performance.get("mapping_speedup")
        compute_speedup = performance.get("compute_speedup")
        rss_delta = performance.get("peak_rss_delta_bytes")
        parallelism = performance.get("mapping_parallelism_engaged")
        report_path = report_path.resolve()
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(
            "# CN Batched Portfolio Kernel V1\n\n"
            f"- Status: `{status}`\n"
            f"- Retained engineering attempt: `{retained['attempt_id'] if retained else 'none'}`\n"
            f"- Same-input subset wall speedup: `{speedup}`\n"
            f"- Mapping / compute speedup: `{mapping_speedup}` / `{compute_speedup}`\n"
            f"- Observed peak RSS delta bytes: `{rss_delta}`\n"
            f"- Mapping parallelism engaged: `{parallelism}`\n"
            "- Resource qualification basis: `observed peak RSS`; executed "
            "`mapping_temporary_bytes` excludes label-order scratch and is not used for qualification.\n"
            "- Current-kernel 146/256 replay: `NOT_EXECUTED`\n"
            "- Historical golden 256: `UNCHANGED`\n"
            f"- Next decision: `{next_decision}`\n"
            "- Validation / holdout / 2026 reads: `0 / 0 / 0`\n",
            encoding="utf-8",
        )
    return {
        "status": status,
        "next_decision": next_decision,
        "output_root": str(output_root),
        "output_files": list(OUTPUT_NAMES),
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--golden-artifact-index", type=Path, required=True)
    parser.add_argument("--frozen-binding", type=Path, required=True)
    parser.add_argument("--frozen-binding-sha256", required=True)
    parser.add_argument("--baseline-result-root", type=Path, required=True)
    parser.add_argument("--fb25-result-root", type=Path, required=True)
    parser.add_argument("--fb25-qualification", type=Path, required=True)
    parser.add_argument("--fb25-checkpoint-parity", type=Path, required=True)
    parser.add_argument("--repo-sha", required=True)
    parser.add_argument("--baseline-portfolio-sha256", required=True)
    parser.add_argument("--fb25-portfolio-sha256", required=True)
    parser.add_argument("--iteration2-result-root", type=Path)
    parser.add_argument("--iteration2-qualification", type=Path)
    parser.add_argument("--iteration2-checkpoint-parity", type=Path)
    parser.add_argument("--iteration2-portfolio-sha256")
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--report-path", type=Path)
    parser.add_argument("--remote-machine")
    parser.add_argument("--test-evidence", action="append", default=[])
    parser.add_argument("--repo-root", type=Path)
    parser.add_argument("--fb25-repo-revision", default="fb25a91")
    args = parser.parse_args(argv)
    result = build_final_artifacts(**vars(args))
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
