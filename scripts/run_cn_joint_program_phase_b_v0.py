from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from dataclasses import asdict
import gc
import hashlib
import json
import math
from pathlib import Path
import platform
import statistics
import sys
import time
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd
import psutil


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts import run_cn_portfolio_decoder_v2 as decoder_v2
from our_system_phase2.runtime.cn_iterative_search_v1 import (
    _artifact,
    _batch_manifest,
)
from our_system_phase2.runtime.cn_joint_program_phase_b_v0 import (
    FREEZE_CLOSURE_NAME,
    TEMPLATE_ORDER,
    verify_phase_b_prefinancial_freeze_v0,
)
from our_system_phase2.runtime.cn_unified_capability_discovery import (
    _behavior_identity,
)
from our_system_phase2.services.a_share_executable_replay import (
    ASharePortfolioDecoderPolicy,
    ENDING_BOOK_FINAL_CLOSE_MARK_TO_MARKET,
    run_a_share_long_only_replay,
)
from our_system_phase2.services.candidate_program_execution_v1 import (
    apply_compiled_candidate_program_v1,
)
from our_system_phase2.services.candidate_program_v1 import (
    CandidateProgramSpecV1,
    ProgramCompilerV1,
)
from our_system_phase2.services.real_market_validation import (
    evaluate_panel_expression,
)
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
    stable_hash,
)


AUTHORIZED_HOST = "DESKTOP-77OPJ6F"
STATUS = "CN_JOINT_PROGRAM_PHASE_B_COMPLETE"
CLOSURE_NAME = "CN_JOINT_PROGRAM_PHASE_B_COMPLETE.json"
MINIMUM_FREE_MEMORY_BYTES = 24 * 1024**3
EXPECTED_RECORDS = 64
RECORDS_PER_CHECKPOINT = 8
CHECKPOINT_COUNT = 8
ACCEPTED_FIELD_MANIFEST_FILE_SHA256 = (
    "f13126a9052082123d3ae5f31125800603affdf4c61e10878ec46962c1d5ea59"
)
ACCEPTED_FIELD_MANIFEST_PAYLOAD_SHA256 = (
    "de720e48b4d23a1dc2e110b4f1cf5c3bc7e9d19a97ee8f7027700649c8410b98"
)
DECODER = ASharePortfolioDecoderPolicy(
    decoder_id="TOPK_10_EQUAL",
    selection="TOP_K",
    top_k=10,
    weighting="EQUAL",
)

_WORKER_CONTEXT: dict[str, Any] | None = None
_WORKER_REGISTRY: UnifiedCapabilityRegistry | None = None
_WORKER_INPUT_HASH: str | None = None
_WORKER_WINDOWS: tuple[Mapping[str, Any], ...] = ()


def _sha256(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in Path(path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


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


def _self_hashed(payload: Mapping[str, Any], field: str) -> dict[str, Any]:
    body = dict(payload)
    body[field] = stable_hash(body)
    return body


def _hash_series(frame: pd.DataFrame, columns: Sequence[str]) -> str:
    selected = frame[list(columns)].copy(deep=False)
    digest = hashlib.sha256()
    digest.update("|".join(columns).encode("utf-8"))
    digest.update(pd.util.hash_pandas_object(selected, index=False).values.tobytes())
    return digest.hexdigest()


def _finite(value: Any) -> float | None:
    try:
        rendered = float(value)
    except (TypeError, ValueError):
        return None
    return rendered if math.isfinite(rendered) else None


def _compiled(
    record: Mapping[str, Any],
    *,
    program_key: str,
    compiled_key: str,
    registry: UnifiedCapabilityRegistry,
):
    program = CandidateProgramSpecV1.from_record(dict(record[program_key]))
    compiled = ProgramCompilerV1(registry).compile(program)
    if compiled.to_record() != dict(record[compiled_key]):
        raise RuntimeError(f"frozen compiled program drift: {compiled_key}")
    return compiled


def _validate_phase_b_materialized_sidecar(
    train_field_root: Path,
    *,
    split_manifest_sha256: str,
    verify_shards: bool,
    expected_manifest_file_sha256: str = ACCEPTED_FIELD_MANIFEST_FILE_SHA256,
    expected_manifest_payload_sha256: str = ACCEPTED_FIELD_MANIFEST_PAYLOAD_SHA256,
) -> tuple[dict[str, Any], Path]:
    root = Path(train_field_root).resolve()
    manifest_path = root / "CN_DEVELOPMENT_TIME_MAJOR_EXECUTION_LAYOUT_V2.json"
    if _sha256(manifest_path) != expected_manifest_file_sha256:
        raise RuntimeError("accepted Phase B field manifest file hash drift")
    manifest = _read_json(manifest_path)
    body = dict(manifest)
    observed_payload_hash = str(body.pop("manifest_hash", ""))
    if (
        observed_payload_hash != expected_manifest_payload_sha256
        or stable_hash(body) != observed_payload_hash
    ):
        raise RuntimeError("accepted Phase B field manifest self-hash drift")
    required = {
        "schema_version": (
            "cn_development_time_major_execution_layout_manifest_v2_train_only"
        ),
        "status": "TIME_MAJOR_LAYOUT_PARITY_PASS",
        "data_role": "development_train_only",
        "split_manifest_hash": split_manifest_sha256,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "source_shard_count": 16,
    }
    drift = [
        key for key, value in required.items() if manifest.get(key) != value
    ]
    if drift:
        raise RuntimeError(
            "accepted Phase B development sidecar drift: " + ",".join(drift)
        )
    later_authority_fields = {
        "evaluation_role",
        "feedback_write",
        "scheduler_write",
        "archive_write",
        "promotion",
    }
    if later_authority_fields.intersection(manifest):
        raise RuntimeError("accepted Phase B sidecar schema revision drift")
    if not {"trade_time", "code", "open", "close"}.issubset(
        set(manifest.get("fields") or ())
    ):
        raise RuntimeError("accepted Phase B field sidecar lacks replay fields")
    shards = list(manifest.get("shards") or ())
    if len(shards) != 16:
        raise RuntimeError("accepted Phase B field shard cardinality drift")
    observed_rows = 0
    for shard in shards:
        path = Path(str(shard["output_path"])).resolve()
        if not path.is_relative_to(root):
            raise RuntimeError("accepted Phase B field shard escapes its root")
        if (
            str(shard.get("status") or "") != "TIME_MAJOR_SHARD_READY"
            or str(shard.get("split_manifest_hash") or "")
            != split_manifest_sha256
        ):
            raise RuntimeError("accepted Phase B field shard authority drift")
        observed_rows += int(shard["rows"])
        if verify_shards and (
            not path.is_file()
            or path.stat().st_size != int(shard["output_bytes"])
            or _sha256(path) != str(shard["output_sha256"])
        ):
            raise RuntimeError(f"accepted Phase B field shard hash drift: {path}")
    if (
        observed_rows != int(manifest.get("sidecar_rows") or -1)
        or observed_rows != int(manifest.get("source_rows") or -1)
    ):
        raise RuntimeError("accepted Phase B field row-count drift")
    return manifest, manifest_path


def _load_context(contract_path: Path, train_field_root: Path) -> dict[str, Any]:
    contract = _read_json(contract_path)
    decoder_v2.base._verify_payload_hash(
        contract,
        field="contract_payload_sha256",
        label="joint-program Phase B execution contract",
    )
    field_manifest, field_manifest_path = _validate_phase_b_materialized_sidecar(
        train_field_root,
        split_manifest_sha256=str(contract["split_manifest_sha256"]),
        verify_shards=False,
    )
    context = decoder_v2._load_evaluation_context(
        contract_path=contract_path,
        train_field_root=train_field_root,
        qualification_mode=True,
        validated_field_manifest=field_manifest,
        validated_field_manifest_path=field_manifest_path,
    )
    for key in (
        "prepared_index",
        "future_returns",
        "codes",
        "dates",
        "eligible",
        "date_groups",
        "evaluation_policies",
    ):
        context.pop(key, None)
    return context


def _initialize_worker(
    contract_path: str,
    train_field_root: str,
    registry_path: str,
    input_hash: str,
    windows: Sequence[Mapping[str, Any]],
) -> None:
    global _WORKER_CONTEXT, _WORKER_REGISTRY, _WORKER_INPUT_HASH, _WORKER_WINDOWS
    _WORKER_CONTEXT = _load_context(Path(contract_path), Path(train_field_root))
    _WORKER_REGISTRY = UnifiedCapabilityRegistry.read(Path(registry_path))
    _WORKER_INPUT_HASH = str(input_hash)
    _WORKER_WINDOWS = tuple(dict(row) for row in windows)


def _signal_diagnostics(
    frame: pd.DataFrame, signal: pd.Series
) -> dict[str, Any]:
    observed = pd.DataFrame(
        {
            "trade_time": pd.to_datetime(frame["trade_time"]),
            "code": frame["code"].astype(str),
            "signal": pd.to_numeric(signal, errors="coerce"),
        }
    )
    observed["signal_rank"] = observed.groupby(
        "trade_time", sort=False
    )["signal"].rank(method="average", pct=True)
    behavior = _behavior_identity(
        observed["signal_rank"].to_numpy(dtype=float),
        observed["trade_time"].astype("int64").to_numpy(dtype=np.int64),
        observed["code"].to_numpy(),
    )
    order = observed.groupby("trade_time", sort=False)["signal"].rank(
        method="first", ascending=False
    )
    selected = observed.loc[order.le(10.0) & observed["signal"].notna(), "code"]
    frequency = selected.value_counts()
    return {
        "signal_sha256": _hash_series(observed, ("trade_time", "code", "signal")),
        "signal_rank_sha256": _hash_series(
            observed, ("trade_time", "code", "signal_rank")
        ),
        "behavior_identity": behavior,
        "signal_support_rows": int(observed["signal"].notna().sum()),
        "selected_symbol_count": int(frequency.size),
        "top_selected_symbol_share": (
            float(frequency.iloc[0] / max(1, int(frequency.sum())))
            if not frequency.empty
            else None
        ),
    }


def _window_returns(
    daily: pd.DataFrame, windows: Sequence[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    frame = daily.copy(deep=False)
    dates = pd.to_datetime(frame["date"])
    output: list[dict[str, Any]] = []
    for window in windows:
        mask = dates.between(
            pd.Timestamp(str(window["start_date"])),
            pd.Timestamp(str(window["end_date"])),
        )
        section = frame.loc[mask]
        if section.empty:
            raise RuntimeError(f"development subwindow has no replay rows: {window}")
        daily_returns = pd.to_numeric(section["daily_net_return"], errors="coerce")
        output.append(
            {
                "window_id": str(window["window_id"]),
                "session_count": int(len(section)),
                "cumulative_net_return": float((1.0 + daily_returns).prod() - 1.0),
                "daily_return_p10": float(daily_returns.quantile(0.10)),
                "worst_daily_return": float(daily_returns.min()),
            }
        )
    return output


def _metric(
    *,
    result: Mapping[str, Any],
    signal_diagnostics: Mapping[str, Any],
    windows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    initial_cash = float(result["execution_policy"]["initial_cash_cny"])
    cumulative_return = float(result["ending_nav_cny"]) / initial_cash - 1.0
    turnover = _finite(result["a_share_mean_one_way_turnover"])
    invariants = dict(result["accounting_invariants"])
    if str(invariants.get("status") or "") != "PASS":
        raise RuntimeError("continuous-book accounting invariants failed")
    return {
        **dict(signal_diagnostics),
        "continuous_book_net_reward": float(result["a_share_executable_net_reward"]),
        "ending_nav_cny": float(result["ending_nav_cny"]),
        "cumulative_net_return": cumulative_return,
        "cumulative_net_pnl_cny": float(result["cumulative_net_pnl_cny"]),
        "cumulative_realized_trade_pnl_cny": float(
            result["cumulative_realized_trade_pnl_cny"]
        ),
        "ending_unrealized_pnl_cny": float(result["ending_unrealized_pnl_cny"]),
        "total_fees_cny": float(result["total_fees_cny"]),
        "mean_one_way_turnover": turnover,
        "net_return_per_turnover": (
            cumulative_return / turnover
            if turnover is not None and turnover > 0
            else None
        ),
        "ending_holdings_weight": _finite(result["ending_holdings_weight"]),
        "fill_count": int(result["fill_count"]),
        "blocked_buy_count": int(result["blocked_buy_count"]),
        "blocked_sell_count": int(result["blocked_sell_count"]),
        "daily_sha256": decoder_v2._hash_frame(result["daily"]),
        "fills_sha256": decoder_v2._hash_frame(result["fills"]),
        "daily_accounting_ledger_sha256": decoder_v2._hash_frame(
            result["daily_accounting_ledger"]
        ),
        "lot_ledger_sha256": decoder_v2._hash_frame(result["lot_ledger"]),
        "lot_consumption_ledger_sha256": decoder_v2._hash_frame(
            result["lot_consumption_ledger"]
        ),
        "accounting_invariants": invariants,
        "development_subwindows": _window_returns(result["daily"], windows),
    }


def _replay_signal(
    signal: pd.Series,
    *,
    context: Mapping[str, Any],
    signal_diagnostics: Mapping[str, Any],
    windows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    signal_series = pd.Series(
        pd.to_numeric(signal, errors="coerce").to_numpy(),
        index=context["observed_index"],
    )
    replay_frame = context["master"].copy(deep=False)
    replay_frame["signal"] = signal_series.reindex(context["authority_index"]).to_numpy()
    result = run_a_share_long_only_replay(
        replay_frame,
        fee_schedule=context["fee"],
        universe_policy=context["universe"],
        execution_policy=context["execution"],
        corporate_action_policy=context["corporate"],
        portfolio_decoder_policy=DECODER,
        ending_book_policy=ENDING_BOOK_FINAL_CLOSE_MARK_TO_MARKET,
    )
    return _metric(
        result=result,
        signal_diagnostics=signal_diagnostics,
        windows=windows,
    )


def _evaluate_compiled(
    compiled,
    *,
    context: Mapping[str, Any],
    windows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    output = apply_compiled_candidate_program_v1(
        context["field_frame"],
        compiled,
        data_role="development",
        materialized_sidecar_clock_column="trade_time",
        materialized_sidecar_authority="PIT_MATERIALIZED_FIELD_SIDECAR",
    )
    diagnostics = _signal_diagnostics(output, output["signal"])
    return _replay_signal(
        output["signal"],
        context=context,
        signal_diagnostics=diagnostics,
        windows=windows,
    )


def _evaluate_legacy(
    candidate: Mapping[str, Any],
    *,
    context: Mapping[str, Any],
    windows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    signal = pd.to_numeric(
        evaluate_panel_expression(
            context["field_frame"],
            str(candidate["canonical_expression"]),
            cache={},
            data_role="development",
        ),
        errors="coerce",
    )
    diagnostics = _signal_diagnostics(context["field_frame"], signal)
    return _replay_signal(
        signal,
        context=context,
        signal_diagnostics=diagnostics,
        windows=windows,
    )


PARITY_FIELDS = (
    "signal_sha256",
    "signal_rank_sha256",
    "behavior_identity",
    "continuous_book_net_reward",
    "ending_nav_cny",
    "cumulative_net_return",
    "cumulative_net_pnl_cny",
    "total_fees_cny",
    "fill_count",
    "daily_sha256",
    "fills_sha256",
    "daily_accounting_ledger_sha256",
    "lot_ledger_sha256",
    "lot_consumption_ledger_sha256",
)


def _parity_differences(
    wrapper: Mapping[str, Any], legacy: Mapping[str, Any]
) -> list[str]:
    return [field for field in PARITY_FIELDS if wrapper.get(field) != legacy.get(field)]


def _validate_frozen_execution_contract(
    run_contract: Mapping[str, Any],
    *,
    execution_contract_sha256: str,
    capacity_manifest_sha256: str,
    executor_workers: int,
) -> None:
    if (
        str(run_contract.get("portfolio_decoder_id") or "") != "TOPK_10_EQUAL"
        or str(run_contract.get("executor_backend") or "") != "PROCESS_POOL"
        or int(run_contract.get("executor_workers") or 0) != executor_workers
        or str(run_contract.get("execution_contract_snapshot_file_sha256") or "")
        != execution_contract_sha256
        or str(run_contract.get("node_resource_capacity_manifest_sha256") or "")
        != capacity_manifest_sha256
    ):
        raise RuntimeError("Phase B frozen execution contract drift")


def _evaluate_record(
    record: Mapping[str, Any], target_path: str
) -> dict[str, Any]:
    if _WORKER_CONTEXT is None or _WORKER_REGISTRY is None or _WORKER_INPUT_HASH is None:
        raise RuntimeError("joint-program worker context is not initialized")
    schedule = dict(record)
    if bool(schedule.get("semantic_noop")):
        raise RuntimeError("semantic no-op entered the fixed Phase-B quota")
    primary_compiled = _compiled(
        schedule,
        program_key="primary_program",
        compiled_key="primary_compiled",
        registry=_WORKER_REGISTRY,
    )
    control_compiled = _compiled(
        schedule,
        program_key="control_program",
        compiled_key="control_compiled",
        registry=_WORKER_REGISTRY,
    )
    primary = _evaluate_compiled(
        primary_compiled,
        context=_WORKER_CONTEXT,
        windows=_WORKER_WINDOWS,
    )
    control = _evaluate_compiled(
        control_compiled,
        context=_WORKER_CONTEXT,
        windows=_WORKER_WINDOWS,
    )
    parity: dict[str, Any] | None = None
    if str(schedule["record_kind"]) == "BASE_WRAPPER_PARITY":
        legacy_primary = _evaluate_legacy(
            dict(schedule["legacy_primary_candidate"]),
            context=_WORKER_CONTEXT,
            windows=_WORKER_WINDOWS,
        )
        legacy_control = _evaluate_legacy(
            dict(schedule["legacy_control_candidate"]),
            context=_WORKER_CONTEXT,
            windows=_WORKER_WINDOWS,
        )
        primary_drift = _parity_differences(primary, legacy_primary)
        control_drift = _parity_differences(control, legacy_control)
        if primary_drift or control_drift:
            raise RuntimeError(
                "BASE wrapper financial parity failed: "
                f"primary={primary_drift} control={control_drift}"
            )
        parity = {
            "status": "PASS",
            "primary_field_mismatches": [],
            "control_field_mismatches": [],
            "legacy_primary": legacy_primary,
            "legacy_control": legacy_control,
        }
    increment = float(primary["continuous_book_net_reward"]) - float(
        control["continuous_book_net_reward"]
    )
    return_increment = float(primary["cumulative_net_return"]) - float(
        control["cumulative_net_return"]
    )
    search_score = min(float(primary["continuous_book_net_reward"]), increment)
    blockers: list[str] = []
    if int(primary["fill_count"]) == 0:
        blockers.append("NO_EXECUTABLE_FILLS")
    if str(primary["behavior_identity"]) == str(control["behavior_identity"]):
        blockers.append("BEHAVIOR_EQUIVALENT_TO_BASE")
    payload = _self_hashed(
        {
            "schema_version": "cn_joint_program_phase_b_record_v0",
            "status": "JOINT_PROGRAM_PHASE_B_RECORD_CLOSED_IMMUTABLE",
            "input_binding_sha256": _WORKER_INPUT_HASH,
            "main_record_ordinal": int(schedule["main_record_ordinal"]),
            "template_id": str(schedule["template_id"]),
            "record_kind": str(schedule["record_kind"]),
            "schedule_record_sha256": str(schedule["schedule_record_sha256"]),
            "program_id": str(schedule["primary_program"]["program_id"]),
            "control_program_id": str(schedule["control_program"]["program_id"]),
            "pair_id": str(schedule["pair_id"]),
            "proposal_receipt_sha256": str(
                schedule["proposal_receipt"]["proposal_receipt_sha256"]
            ),
            "generation_arm": "UNIFORM_FRESH",
            "adaptive_template_credit_used": False,
            "control_contract_valid": True,
            "compile_status": "PASS",
            "physical_ready": True,
            "dag_ready": True,
            "semantic_noop": False,
            "primary": primary,
            "base_control": control,
            "matched_net_reward_increment": increment,
            "matched_cumulative_return_increment": return_increment,
            "search_score": search_score,
            "productive": bool(
                float(primary["continuous_book_net_reward"]) > 0.0
                and increment > 0.0
            ),
            "blockers": blockers,
            "base_wrapper_parity": parity,
            "validation_reads": 0,
            "holdout_reads": 0,
            "historical_2023_reads": 0,
            "forward_b_reads": 0,
            "forward_2026_reads": 0,
            "optimizer_feedback_write": "FORBIDDEN",
            "scheduler_write": "FORBIDDEN",
            "promotion": "FORBIDDEN",
        },
        "record_payload_sha256",
    )
    _write_json(Path(target_path), payload)
    gc.collect()
    return payload


def _resource_snapshot() -> tuple[int, int, int]:
    process = psutil.Process()
    parent = int(process.memory_info().rss)
    tree = parent
    for child in process.children(recursive=True):
        try:
            tree += int(child.memory_info().rss)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return int(psutil.virtual_memory().available), parent, tree


def _verify_record(
    path: Path,
    *,
    expected_input_hash: str,
    schedule: Mapping[str, Any],
) -> dict[str, Any]:
    row = _read_json(path)
    body = dict(row)
    expected = str(body.pop("record_payload_sha256", ""))
    if expected != stable_hash(body):
        raise RuntimeError(f"record self-hash drift: {path}")
    if (
        str(row.get("status") or "")
        != "JOINT_PROGRAM_PHASE_B_RECORD_CLOSED_IMMUTABLE"
        or str(row.get("input_binding_sha256") or "") != expected_input_hash
        or int(row.get("main_record_ordinal", -1))
        != int(schedule["main_record_ordinal"])
        or str(row.get("schedule_record_sha256") or "")
        != str(schedule["schedule_record_sha256"])
    ):
        raise RuntimeError(f"record identity drift: {path}")
    return row


def _verify_checkpoint(
    checkpoint_root: Path,
    *,
    checkpoint_id: str,
    previous_manifest: Path | None,
    input_hash: str,
    schedule_by_ordinal: Mapping[int, Mapping[str, Any]],
) -> tuple[Path, list[dict[str, Any]]]:
    manifest_path = checkpoint_root / "batch_manifest.json"
    manifest = _read_json(manifest_path)
    body = dict(manifest)
    expected = str(body.pop("manifest_payload_hash", ""))
    if expected != stable_hash(body):
        raise RuntimeError(f"checkpoint self-hash drift: {checkpoint_id}")
    expected_prior = _sha256(previous_manifest) if previous_manifest else "GENESIS"
    if (
        str(manifest.get("status") or "") != "BATCH_CLOSED_IMMUTABLE"
        or str(manifest.get("batch_id") or "") != checkpoint_id
        or str((manifest.get("input_hashes") or {}).get("prior_checkpoint_manifest"))
        != expected_prior
        or str((manifest.get("input_hashes") or {}).get("phase_b_input_binding"))
        != input_hash
    ):
        raise RuntimeError(f"checkpoint chain drift: {checkpoint_id}")
    for artifact in manifest.get("artifacts") or ():
        path = (checkpoint_root / str(artifact["path"])).resolve()
        if not path.is_relative_to(checkpoint_root.resolve()):
            raise RuntimeError(f"checkpoint artifact escape: {checkpoint_id}")
        if (
            not path.is_file()
            or path.stat().st_size != int(artifact["bytes"])
            or _sha256(path) != str(artifact["sha256"])
        ):
            raise RuntimeError(f"checkpoint artifact drift: {path}")
    rows: list[dict[str, Any]] = []
    for path in sorted((checkpoint_root / "records").glob("record_*.json")):
        ordinal = int(path.stem.split("_")[-1])
        rows.append(
            _verify_record(
                path,
                expected_input_hash=input_hash,
                schedule=schedule_by_ordinal[ordinal],
            )
        )
    if len(rows) != RECORDS_PER_CHECKPOINT:
        raise RuntimeError(f"checkpoint record count drift: {checkpoint_id}")
    return manifest_path, rows


def _close_checkpoint(
    inflight_root: Path,
    final_root: Path,
    *,
    checkpoint_id: str,
    previous_manifest: Path | None,
    input_hash: str,
    freeze_closure_sha256: str,
    schedule_file_sha256: str,
    schedule_rows: Sequence[Mapping[str, Any]],
) -> Path:
    records = [
        _verify_record(
            inflight_root / "records" / f"record_{int(row['main_record_ordinal']):04d}.json",
            expected_input_hash=input_hash,
            schedule=row,
        )
        for row in schedule_rows
    ]
    summary = _self_hashed(
        {
            "schema_version": "cn_joint_program_phase_b_checkpoint_summary_v0",
            "status": "JOINT_PROGRAM_PHASE_B_CHECKPOINT_COMPLETE",
            "checkpoint_id": checkpoint_id,
            "record_count": len(records),
            "template_id": str(records[0]["template_id"]),
            "productive_count": sum(bool(row["productive"]) for row in records),
            "replay_complete_count": len(records),
            "behavior_unique": len(
                {str(row["primary"]["behavior_identity"]) for row in records}
            ),
            "validation_reads": 0,
            "holdout_reads": 0,
            "historical_2023_reads": 0,
            "forward_b_reads": 0,
            "forward_2026_reads": 0,
        },
        "summary_payload_sha256",
    )
    summary_path = _write_json(inflight_root / "checkpoint_summary.json", summary)
    record_paths = sorted((inflight_root / "records").glob("record_*.json"))
    manifest_path = _batch_manifest(
        batch_root=inflight_root,
        batch_id=checkpoint_id,
        input_hashes={
            "phase_b_input_binding": input_hash,
            "phase_b_prefinancial_closure": freeze_closure_sha256,
            "phase_b_uniform_schedule": schedule_file_sha256,
            "prior_checkpoint_manifest": (
                _sha256(previous_manifest) if previous_manifest else "GENESIS"
            ),
        },
        paths=[*record_paths, summary_path],
        access_receipts=[],
        evaluation_name="joint candidate-program development continuous-book evaluation",
    )
    if final_root.exists():
        raise RuntimeError(f"closed checkpoint target already exists: {final_root}")
    inflight_root.replace(final_root)
    return final_root / manifest_path.name


def _template_summary(records: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for template_id in TEMPLATE_ORDER:
        rows = [row for row in records if str(row["template_id"]) == template_id]
        scores = [float(row["search_score"]) for row in rows]
        increments = [float(row["matched_net_reward_increment"]) for row in rows]
        primary_rewards = [
            float(row["primary"]["continuous_book_net_reward"]) for row in rows
        ]
        output.append(
            {
                "template_id": template_id,
                "component_supply": len(rows),
                "program_proposed": len(rows),
                "semantic_unique": len({str(row["program_id"]) for row in rows}),
                "control_valid": sum(bool(row["control_contract_valid"]) for row in rows),
                "compile_pass": sum(str(row["compile_status"]) == "PASS" for row in rows),
                "physical_ready": sum(bool(row["physical_ready"]) for row in rows),
                "dag_ready": sum(bool(row["dag_ready"]) for row in rows),
                "replay_complete": len(rows),
                "behavior_unique": len(
                    {str(row["primary"]["behavior_identity"]) for row in rows}
                ),
                "economic_rows_complete": len(rows),
                "semantic_noop": sum(bool(row["semantic_noop"]) for row in rows),
                "productive": sum(bool(row["productive"]) for row in rows),
                "positive_primary_reward": sum(value > 0.0 for value in primary_rewards),
                "positive_matched_increment": sum(value > 0.0 for value in increments),
                "median_primary_reward": statistics.median(primary_rewards),
                "median_matched_increment": statistics.median(increments),
                "median_search_score": statistics.median(scores),
            }
        )
    return output


def run(args: argparse.Namespace) -> dict[str, Any]:
    if platform.node().upper() != AUTHORIZED_HOST:
        raise RuntimeError(
            f"Phase B financial pilot is authorized only on {AUTHORIZED_HOST}"
        )
    root = args.output_root.resolve()
    freeze_root = args.phase_b_freeze_root.resolve()
    freeze = verify_phase_b_prefinancial_freeze_v0(freeze_root)
    schedule_path = freeze_root / "phase_b_uniform_schedule.jsonl"
    schedule = _read_jsonl(schedule_path)
    if len(schedule) != EXPECTED_RECORDS:
        raise RuntimeError("Phase B frozen schedule count drift")
    if any(bool(row.get("semantic_noop")) for row in schedule):
        raise RuntimeError("Phase B frozen schedule contains semantic no-op")
    schedule_by_ordinal = {
        int(row["main_record_ordinal"]): row for row in schedule
    }
    if set(schedule_by_ordinal) != set(range(EXPECTED_RECORDS)):
        raise RuntimeError("Phase B frozen schedule ordinal drift")
    contract_path = args.execution_contract.resolve()
    train_field_root = args.train_field_root.resolve()
    registry_path = args.registry.resolve()
    capacity_path = args.node_resource_capacity.resolve()
    for path in (contract_path, registry_path, capacity_path):
        if not path.is_file():
            raise FileNotFoundError(path)
    if not train_field_root.is_dir():
        raise FileNotFoundError(train_field_root)
    execution_contract = _read_json(contract_path)
    decoder_v2.base._verify_payload_hash(
        execution_contract,
        field="contract_payload_sha256",
        label="joint-program Phase B execution contract",
    )
    field_manifest, field_manifest_path = _validate_phase_b_materialized_sidecar(
        train_field_root,
        split_manifest_sha256=str(execution_contract["split_manifest_sha256"]),
        verify_shards=True,
    )
    capacity = _read_json(capacity_path)
    capacity_body = dict(capacity)
    capacity_expected = str(capacity_body.pop("capacity_manifest_sha256", ""))
    if capacity_expected != stable_hash(capacity_body):
        raise RuntimeError("node resource capacity self-hash drift")
    profile = dict(capacity.get("profiles", {}).get("VALIDATION_EXCLUSIVE_32") or {})
    if (
        int(profile.get("cpu_threads") or 0) != 32
        or int(profile.get("minimum_free_memory_bytes") or 0)
        != MINIMUM_FREE_MEMORY_BYTES
        or int(args.executor_workers) != 12
    ):
        raise RuntimeError("Phase B process resource contract drift")
    run_contract = _read_json(freeze_root / "phase_b_run_contract.json")
    _validate_frozen_execution_contract(
        run_contract,
        execution_contract_sha256=_sha256(contract_path),
        capacity_manifest_sha256=str(capacity.get("capacity_manifest_sha256") or ""),
        executor_workers=int(args.executor_workers),
    )
    normalized_field_root = str(train_field_root).replace("/", "\\").lower()
    if (
        normalized_field_root
        != str(run_contract.get("remote_train_session_field_root") or "")
        .replace("/", "\\")
        .lower()
        or any(
            token in normalized_field_root
            for token in (
                "validation",
                "holdout",
                "historical_challenge_2023",
                "forward_b",
                "forward_2026",
            )
        )
    ):
        raise RuntimeError("Phase B train field root binding drift")
    windows = tuple(dict(row) for row in run_contract["development_subwindows"])
    input_binding = _self_hashed(
        {
            "schema_version": "cn_joint_program_phase_b_input_binding_v0",
            "status": "JOINT_PROGRAM_PHASE_B_INPUTS_BOUND",
            "runner_repo_sha": str(args.builder_commit_sha),
            "freeze_repo_sha": str(freeze["repo_sha"]),
            "phase_b_prefinancial_closure_file_sha256": _sha256(
                freeze_root / FREEZE_CLOSURE_NAME
            ),
            "phase_b_prefinancial_closure_payload_sha256": str(
                freeze["closure_sha256"]
            ),
            "phase_b_uniform_schedule_sha256": _sha256(schedule_path),
            "execution_contract_sha256": _sha256(contract_path),
            "train_field_root": str(train_field_root),
            "train_field_manifest_sha256": _sha256(field_manifest_path),
            "train_field_manifest_payload_sha256": str(
                field_manifest["manifest_hash"]
            ),
            "registry_sha256": _sha256(registry_path),
            "node_resource_capacity_sha256": _sha256(capacity_path),
            "decoder_policy": {
                **asdict(DECODER),
                "payload_sha256": DECODER.payload_sha256,
            },
            "record_count": EXPECTED_RECORDS,
            "checkpoint_count": CHECKPOINT_COUNT,
            "records_per_checkpoint": RECORDS_PER_CHECKPOINT,
            "executor_backend": "PROCESS_POOL",
            "executor_workers": int(args.executor_workers),
            "native_threads_per_worker": 1,
            "validation_reads": 0,
            "holdout_reads": 0,
            "historical_2023_reads": 0,
            "forward_b_reads": 0,
            "forward_2026_reads": 0,
            "promotion": "FORBIDDEN",
        },
        "input_binding_sha256",
    )
    input_hash = str(input_binding["input_binding_sha256"])
    if root.exists():
        existing = root / "input_binding.json"
        if existing.is_file():
            if _read_json(existing) != input_binding:
                raise RuntimeError("Phase B same-root recovery input binding drift")
        else:
            allowed_bootstrap = {
                "deployment_binding.json",
                "joint_program_phase_b.stdout.log",
                "joint_program_phase_b.stderr.log",
                "resource_leases",
            }
            unexpected = sorted(
                path.name for path in root.iterdir() if path.name not in allowed_bootstrap
            )
            if unexpected:
                raise RuntimeError(
                    f"Phase B bootstrap root contains unexpected files: {unexpected}"
                )
            _write_json(existing, input_binding)
    else:
        root.mkdir(parents=True)
        _write_json(root / "input_binding.json", input_binding)
    inflight_root = root / "inflight"
    if inflight_root.exists() and any(inflight_root.iterdir()):
        raise RuntimeError(
            "Phase B contains incomplete checkpoint results; preserve them before recovery"
        )
    checkpoints_root = root / "checkpoints"
    checkpoints_root.mkdir(exist_ok=True)
    previous_manifest: Path | None = None
    records: list[dict[str, Any]] = []
    closed_checkpoints = 0
    for index in range(CHECKPOINT_COUNT):
        checkpoint_id = f"checkpoint_{index + 1:03d}"
        checkpoint_root = checkpoints_root / checkpoint_id
        if not checkpoint_root.exists():
            break
        previous_manifest, checkpoint_rows = _verify_checkpoint(
            checkpoint_root,
            checkpoint_id=checkpoint_id,
            previous_manifest=previous_manifest,
            input_hash=input_hash,
            schedule_by_ordinal=schedule_by_ordinal,
        )
        records.extend(checkpoint_rows)
        closed_checkpoints += 1
    if any(
        (checkpoints_root / f"checkpoint_{index + 1:03d}").exists()
        for index in range(closed_checkpoints + 1, CHECKPOINT_COUNT)
    ):
        raise RuntimeError("Phase B checkpoint chain contains a gap")
    remaining = schedule[closed_checkpoints * RECORDS_PER_CHECKPOINT :]
    minimum_free, maximum_parent_rss, maximum_tree_rss = _resource_snapshot()
    host_cpu_samples: list[float] = []
    psutil.cpu_percent(interval=None)
    started = time.perf_counter()
    completed_ordinals = {
        int(row["main_record_ordinal"]) for row in records
    }
    if remaining:
        inflight_root.mkdir(parents=True, exist_ok=True)
        for index in range(closed_checkpoints, CHECKPOINT_COUNT):
            (inflight_root / f"checkpoint_{index + 1:03d}" / "records").mkdir(
                parents=True, exist_ok=True
            )
        with ProcessPoolExecutor(
            max_workers=int(args.executor_workers),
            initializer=_initialize_worker,
            initargs=(
                str(contract_path),
                str(train_field_root),
                str(registry_path),
                input_hash,
                windows,
            ),
        ) as executor:
            futures = {}
            for row in remaining:
                ordinal = int(row["main_record_ordinal"])
                checkpoint_index = ordinal // RECORDS_PER_CHECKPOINT
                target = (
                    inflight_root
                    / f"checkpoint_{checkpoint_index + 1:03d}"
                    / "records"
                    / f"record_{ordinal:04d}.json"
                )
                futures[executor.submit(_evaluate_record, row, str(target))] = ordinal
            pending = set(futures)
            next_to_close = closed_checkpoints
            while pending:
                done, pending = wait(
                    pending, timeout=2.0, return_when=FIRST_COMPLETED
                )
                for future in done:
                    payload = future.result()
                    completed_ordinals.add(int(payload["main_record_ordinal"]))
                available, parent_rss, tree_rss = _resource_snapshot()
                minimum_free = min(minimum_free, available)
                maximum_parent_rss = max(maximum_parent_rss, parent_rss)
                maximum_tree_rss = max(maximum_tree_rss, tree_rss)
                host_cpu_samples.append(float(psutil.cpu_percent(interval=None)))
                if available < MINIMUM_FREE_MEMORY_BYTES:
                    for future in pending:
                        future.cancel()
                    raise RuntimeError("Phase B runtime memory gate failed")
                while next_to_close < CHECKPOINT_COUNT:
                    checkpoint_ordinals = set(
                        range(
                            next_to_close * RECORDS_PER_CHECKPOINT,
                            (next_to_close + 1) * RECORDS_PER_CHECKPOINT,
                        )
                    )
                    if not checkpoint_ordinals.issubset(completed_ordinals):
                        break
                    checkpoint_id = f"checkpoint_{next_to_close + 1:03d}"
                    checkpoint_rows = schedule[
                        next_to_close * RECORDS_PER_CHECKPOINT :
                        (next_to_close + 1) * RECORDS_PER_CHECKPOINT
                    ]
                    previous_manifest = _close_checkpoint(
                        inflight_root / checkpoint_id,
                        checkpoints_root / checkpoint_id,
                        checkpoint_id=checkpoint_id,
                        previous_manifest=previous_manifest,
                        input_hash=input_hash,
                        freeze_closure_sha256=_sha256(freeze_root / FREEZE_CLOSURE_NAME),
                        schedule_file_sha256=_sha256(schedule_path),
                        schedule_rows=checkpoint_rows,
                    )
                    next_to_close += 1
        if inflight_root.exists() and not any(inflight_root.iterdir()):
            inflight_root.rmdir()
    elapsed = float(time.perf_counter() - started)
    previous_manifest = None
    records = []
    checkpoint_manifests: list[Path] = []
    for index in range(CHECKPOINT_COUNT):
        checkpoint_id = f"checkpoint_{index + 1:03d}"
        previous_manifest, checkpoint_rows = _verify_checkpoint(
            checkpoints_root / checkpoint_id,
            checkpoint_id=checkpoint_id,
            previous_manifest=previous_manifest,
            input_hash=input_hash,
            schedule_by_ordinal=schedule_by_ordinal,
        )
        checkpoint_manifests.append(previous_manifest)
        records.extend(checkpoint_rows)
    records.sort(key=lambda row: int(row["main_record_ordinal"]))
    if len(records) != EXPECTED_RECORDS:
        raise RuntimeError("Phase B root result count drift")
    if any(
        str(row["base_wrapper_parity"]["status"]) != "PASS"
        for row in records
        if str(row["record_kind"]) == "BASE_WRAPPER_PARITY"
    ):
        raise RuntimeError("Phase B BASE parity root gate failed")
    results_path = root / "phase_b_record_results.jsonl"
    results_path.write_text(
        "".join(
            json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n"
            for row in records
        ),
        encoding="utf-8",
    )
    template_summary = _template_summary(records)
    template_path = _write_json(
        root / "template_productivity.json",
        _self_hashed(
            {
                "schema_version": "cn_joint_program_phase_b_template_productivity_v0",
                "status": "PHASE_B_TEMPLATE_PRODUCTIVITY_REPORTED",
                "templates": template_summary,
            },
            "template_productivity_payload_sha256",
        ),
    )
    blockers = Counter(
        blocker for row in records for blocker in row.get("blockers") or ()
    )
    blocker_path = _write_json(
        root / "blocker_taxonomy.json",
        _self_hashed(
            {
                "schema_version": "cn_joint_program_phase_b_blockers_v0",
                "status": "PHASE_B_BLOCKERS_REPORTED",
                "counts": dict(sorted(blockers.items())),
            },
            "blocker_taxonomy_payload_sha256",
        ),
    )
    logical_cpu = int(psutil.cpu_count(logical=True) or 1)
    cpu_mean = float(np.mean(host_cpu_samples)) if host_cpu_samples else 0.0
    resource = _self_hashed(
        {
            "schema_version": "cn_joint_program_phase_b_resource_summary_v0",
            "status": "PASS" if minimum_free >= MINIMUM_FREE_MEMORY_BYTES else "FAIL",
            "wall_seconds": elapsed,
            "main_records_per_hour": EXPECTED_RECORDS / max(elapsed / 3600.0, 1e-12),
            "minimum_free_memory_bytes": minimum_free,
            "maximum_parent_rss_bytes": maximum_parent_rss,
            "maximum_process_tree_rss_bytes": maximum_tree_rss,
            "mean_host_cpu_percent": cpu_mean,
            "mean_effective_cores": cpu_mean * logical_cpu / 100.0,
            "logical_cpu_count": logical_cpu,
            "executor_workers": int(args.executor_workers),
            "native_threads_per_worker": 1,
        },
        "resource_summary_payload_sha256",
    )
    if str(resource["status"]) != "PASS":
        raise RuntimeError("Phase B resource summary failed")
    resource_path = _write_json(root / "resource_summary.json", resource)
    access_path = _write_json(
        root / "access_ledger.json",
        _self_hashed(
            {
                "schema_version": "cn_joint_program_phase_b_access_ledger_v0",
                "financial_role": "DEVELOPMENT_ONLY",
                "market_price_rows_read": "POSITIVE_DEVELOPMENT_ONLY",
                "validation_reads": 0,
                "holdout_reads": 0,
                "historical_2023_reads": 0,
                "forward_b_reads": 0,
                "forward_2026_reads": 0,
                "optimizer_feedback_write": "FORBIDDEN",
                "scheduler_write": "FORBIDDEN",
                "archive_write": "FORBIDDEN",
                "promotion": "FORBIDDEN",
            },
            "access_ledger_sha256",
        ),
    )
    root_artifacts = [
        root / "input_binding.json",
        results_path,
        template_path,
        blocker_path,
        resource_path,
        access_path,
        *checkpoint_manifests,
    ]
    manifest = _self_hashed(
        {
            "schema_version": "cn_joint_program_phase_b_artifact_manifest_v0",
            "artifacts": [_artifact(path, root=root) for path in root_artifacts],
        },
        "artifact_manifest_sha256",
    )
    manifest_path = _write_json(root / "ARTIFACT_MANIFEST.json", manifest)
    closure = _self_hashed(
        {
            "schema_version": "cn_joint_program_phase_b_closure_v0",
            "status": STATUS,
            "output_root": str(root),
            "runner_repo_sha": str(args.builder_commit_sha),
            "phase_b_input_binding_sha256": input_hash,
            "record_count": len(records),
            "checkpoint_count": len(checkpoint_manifests),
            "base_parity_pass": sum(
                str(row["record_kind"]) == "BASE_WRAPPER_PARITY"
                and str(row["base_wrapper_parity"]["status"]) == "PASS"
                for row in records
            ),
            "enhanced_replay_complete": sum(
                str(row["record_kind"]) == "ENHANCED_FULL_BASE_PAIR"
                for row in records
            ),
            "productive_count": sum(bool(row["productive"]) for row in records),
            "behavior_unique": len(
                {str(row["primary"]["behavior_identity"]) for row in records}
            ),
            "semantic_noop_count": sum(bool(row["semantic_noop"]) for row in records),
            "control_mismatch_count": sum(
                not bool(row["control_contract_valid"]) for row in records
            ),
            "ledger_mismatch_count": sum(
                str(row["primary"]["accounting_invariants"]["status"]) != "PASS"
                or str(row["base_control"]["accounting_invariants"]["status"])
                != "PASS"
                for row in records
            ),
            "artifact_manifest": _artifact(manifest_path, root=root),
            "artifact_count": len(manifest["artifacts"]),
            "minimum_free_memory_bytes": minimum_free,
            "validation_reads": 0,
            "holdout_reads": 0,
            "historical_2023_reads": 0,
            "forward_b_reads": 0,
            "forward_2026_reads": 0,
            "promotion": "FORBIDDEN",
        },
        "closure_payload_sha256",
    )
    return _read_json(_write_json(root / CLOSURE_NAME, closure))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase-b-freeze-root", required=True, type=Path)
    parser.add_argument("--execution-contract", required=True, type=Path)
    parser.add_argument("--train-field-root", required=True, type=Path)
    parser.add_argument("--registry", required=True, type=Path)
    parser.add_argument("--node-resource-capacity", required=True, type=Path)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--builder-commit-sha", required=True)
    parser.add_argument("--executor-workers", type=int, default=12)
    args = parser.parse_args(argv)
    if len(str(args.builder_commit_sha)) != 40:
        parser.error("builder-commit-sha must be a full Git SHA")
    closure = run(args)
    print(json.dumps(closure, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
