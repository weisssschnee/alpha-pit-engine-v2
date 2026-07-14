"""Execute unified development-only capability CANARY and frozen discovery.

The runner uses a small, fixed coordinate slice for proposal/admission and a
single streaming pass over every row group for strict active-route evidence.
Fundamental routes run on stock-session coordinates and Broad Event uses the
frozen 11-mechanism full-16-shard replay.  No challenge, forward or persistent
memory path exists in this module.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import shutil
import subprocess
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from our_system_phase2.runtime.cn_broad_event_frozen_replay import run_replay
from our_system_phase2.services.development_only_data_access import (
    ValidatedDevelopmentRelease,
    validate_development_release,
)
from our_system_phase2.services.expression_semantics import parse_expression
from our_system_phase2.services.fundamental_representations import (
    CanonicalFundamentalMaterializer,
)
from our_system_phase2.services.pit_fundamental_fabric import (
    PITFundamentalFabricAdapter,
    load_development_sessions,
    normalize_cn_code,
)
from our_system_phase2.services.real_market_validation import evaluate_panel_expression
from our_system_phase2.services.search_exposure_ledger import SearchExposureLedger
from our_system_phase2.services.typed_primitive_gate import expression_fields
from our_system_phase2.services.unified_capability_registry import (
    ROUTE_IDS,
    UnifiedCapabilityRegistry,
    stable_hash,
)
from our_system_phase2.services.unified_discovery_generators import RegistryDrivenGenerator


RUNNER_VERSION = "cn_unified_capability_discovery_runner_v1"
ACTIVE_ROUTES = {
    "MINUTE_STATIC",
    "FIRSTN_PATH",
    "SLOW_CROSS_SECTIONAL_LEVEL",
    "SLOW_TEMPORAL_CHANGE",
    "MARKET_REGIME_CONDITION",
    "INTRADAY_STATE_TRANSITION",
}
FUNDAMENTAL_ROUTES = {
    "SLOW_CROSS_SECTIONAL_LEVEL",
    "SLOW_TEMPORAL_CHANGE",
    "DISCLOSURE_EVENT",
}
BROAD_CHECKPOINT_CODE_PATHS = (
    "src/our_system_phase2/runtime/cn_broad_event_frozen_replay.py",
    "src/our_system_phase2/runtime/cn_broad_event_canary.py",
    "src/our_system_phase2/services/broad_event_episodes.py",
    "src/our_system_phase2/services/broad_event_preflight.py",
    "src/our_system_phase2/services/broad_event_semantics.py",
    "src/our_system_phase2/services/conservative_limit_lifecycle.py",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        while block := handle.read(8 * 1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _git(repo: Path, *args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=repo, text=True).strip()


def _git_blob_bundle_hash(repo: Path, repo_sha: str, paths: Sequence[str]) -> str:
    digest = hashlib.sha256()
    for relative in sorted(paths):
        blob = subprocess.check_output(
            ["git", "show", f"{repo_sha}:{relative}"], cwd=repo
        )
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(blob)
        digest.update(b"\0")
    return digest.hexdigest()


def _reuse_broad_checkpoint(
    *,
    repo: Path,
    repo_sha: str,
    checkpoint_root: Path,
    output_root: Path,
    entry_pack_path: Path,
    chip_root: Path,
    release: ValidatedDevelopmentRelease,
    seeds: Sequence[int],
) -> dict[str, Any]:
    checkpoint_root = checkpoint_root.resolve()
    required = {
        name: checkpoint_root / name
        for name in (
            "summary.json",
            "run_manifest.json",
            "frozen_mechanism_results.csv",
            "behavior_registry.json",
            "episode_support.json",
        )
    }
    missing = [str(path) for path in required.values() if not path.is_file()]
    if missing:
        raise FileNotFoundError("Broad checkpoint is incomplete: " + ", ".join(missing))
    source_summary = json.loads(required["summary.json"].read_text(encoding="utf-8"))
    source_manifest = json.loads(required["run_manifest.json"].read_text(encoding="utf-8"))
    source_sha = str(source_summary.get("repo_sha") or "")
    if source_summary.get("status") != "BROAD_EVENT_FROZEN_FULL16_REPLAY_COMPLETED":
        raise PermissionError("Broad checkpoint did not complete")
    if source_summary.get("entry_pack_sha256") != _sha256(entry_pack_path):
        raise ValueError("Broad checkpoint entry-pack hash drift")
    if int(source_summary.get("mechanism_count") or 0) != 11:
        raise ValueError("Broad checkpoint mechanism count drift")
    if sorted(source_summary.get("all_shards") or []) != list(range(16)):
        raise ValueError("Broad checkpoint does not cover all 16 shards")
    current_code_hash = _git_blob_bundle_hash(repo, repo_sha, BROAD_CHECKPOINT_CODE_PATHS)
    source_code_hash = _git_blob_bundle_hash(repo, source_sha, BROAD_CHECKPOINT_CODE_PATHS)
    if source_code_hash != current_code_hash:
        raise ValueError("Broad checkpoint code dependency hash drift")
    current_files = {row.sha256 for row in release.files}
    source_files = list(source_manifest.get("data_files") or [])
    if len(source_files) != 16 or {str(row.get("sha256")) for row in source_files} != current_files:
        raise ValueError("Broad checkpoint development release drift")
    for artifact in source_manifest.get("outputs") or []:
        path = checkpoint_root / str(artifact["path"])
        if not path.is_file() or _sha256(path) != artifact["sha256"]:
            raise ValueError(f"Broad checkpoint output hash drift: {path.name}")
    results = pd.read_csv(required["frozen_mechanism_results.csv"])
    if sorted(results["seed"].astype(int).unique().tolist()) != sorted(set(map(int, seeds))):
        raise ValueError("Broad checkpoint seed drift")
    chip_manifest = chip_root / "chip_sidecar_manifest_v1.json"
    if not chip_manifest.is_file():
        raise FileNotFoundError("Broad checkpoint requires the versioned chip manifest")

    output_root.mkdir(parents=True, exist_ok=True)
    for name in ("frozen_mechanism_results.csv", "behavior_registry.json", "episode_support.json"):
        shutil.copy2(required[name], output_root / name)
    shutil.copy2(required["summary.json"], output_root / "checkpoint_source_summary.json")
    shutil.copy2(required["run_manifest.json"], output_root / "checkpoint_source_run_manifest.json")
    certified = {
        **source_summary,
        "repo_sha": repo_sha,
        "checkpoint_reused": True,
        "checkpoint_source_repo_sha": source_sha,
        "checkpoint_code_bundle_sha256": current_code_hash,
        "chip_manifest_sha256": _sha256(chip_manifest),
    }
    _write_json(output_root / "summary.json", certified)
    provenance = {
        "status": "BROAD_EVENT_CHECKPOINT_REUSED_AFTER_STRICT_VALIDATION",
        "source_root": str(checkpoint_root),
        "source_repo_sha": source_sha,
        "current_repo_sha": repo_sha,
        "source_run_manifest_sha256": _sha256(required["run_manifest.json"]),
        "checkpoint_code_paths": list(BROAD_CHECKPOINT_CODE_PATHS),
        "checkpoint_code_bundle_sha256": current_code_hash,
        "entry_pack_sha256": _sha256(entry_pack_path),
        "development_release_hash": release.release_hash,
        "development_file_hashes": sorted(current_files),
        "chip_manifest_sha256": _sha256(chip_manifest),
        "seeds": sorted(set(map(int, seeds))),
        "candidate_promotion": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }
    _write_json(output_root / "checkpoint_reuse.json", provenance)
    manifest = {
        "replay_version": source_manifest.get("replay_version"),
        "repo_sha": repo_sha,
        "checkpoint_reuse": provenance,
        "data_files": source_files,
        "outputs": [
            {"path": path.name, "sha256": _sha256(path), "size": path.stat().st_size}
            for path in sorted(output_root.iterdir())
            if path.is_file() and path.name != "run_manifest.json"
        ],
        "reproducibility": "YES_STRICT_HASH_VALIDATED_BROAD_CHECKPOINT",
        "access_role": "development_only",
    }
    _write_json(output_root / "run_manifest.json", manifest)
    return certified


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_csv(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    materialized = [dict(row) for row in rows]
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = sorted({key for row in materialized for key in row}) if materialized else []
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(materialized)


def _raw_expression(expression: str) -> str:
    node = parse_expression(expression)
    if node.token == "CSRank" and len(node.args) == 1:
        return node.args[0].render()
    return node.render()


def _same_session_future_return(frame: pd.DataFrame, horizon: int = 5) -> pd.Series:
    ordered = frame.sort_values(["code", "trade_time"], kind="mergesort")
    close = pd.to_numeric(ordered["close"], errors="coerce")
    future_close = close.groupby(ordered["code"], sort=False).shift(-horizon)
    future_time = pd.to_datetime(ordered["trade_time"]).groupby(ordered["code"], sort=False).shift(-horizon)
    same_session = future_time.dt.normalize().eq(pd.to_datetime(ordered["trade_time"]).dt.normalize())
    target = (future_close / close.replace(0.0, np.nan) - 1.0).where(same_session)
    return target.reindex(frame.index)


def _checkpoint_mask(times: pd.Series, route_id: str) -> np.ndarray:
    values = pd.to_datetime(times, errors="coerce")
    hhmm = values.dt.hour * 100 + values.dt.minute
    if route_id in {"MINUTE_STATIC", "MARKET_REGIME_CONDITION"}:
        return hhmm.isin([1030, 1430]).to_numpy()
    if route_id == "FIRSTN_PATH":
        return hhmm.eq(1030).to_numpy()
    if route_id in {"SLOW_CROSS_SECTIONAL_LEVEL", "SLOW_TEMPORAL_CHANGE"}:
        return hhmm.eq(1430).to_numpy()
    return np.ones(len(times), dtype=bool)


def _behavior_identity(signal_rank: np.ndarray, times_ns: np.ndarray, codes: np.ndarray) -> str:
    finite = np.isfinite(signal_rank)
    if not finite.any():
        return ""
    order = np.lexsort((codes.astype("S12"), times_ns))
    values = np.nan_to_num(signal_rank[order], nan=-9.0)
    stride = max(1, len(values) // 4096)
    quantized = np.clip(np.rint(values[::stride] * 255.0), -32768, 32767).astype(np.int16)
    digest = hashlib.sha256()
    digest.update(finite[order][::stride].tobytes())
    digest.update(quantized.tobytes())
    return digest.hexdigest()[:24]


def _metric_from_arrays(
    *,
    signal: np.ndarray,
    target: np.ndarray,
    times_ns: np.ndarray,
    codes: np.ndarray,
    route_id: str,
) -> dict[str, Any]:
    frame = pd.DataFrame(
        {
            "signal": np.asarray(signal, dtype=float),
            "target": np.asarray(target, dtype=float),
            "time": pd.to_datetime(times_ns),
            "code": codes.astype(str),
        }
    )
    finite = np.isfinite(frame["signal"]) & np.isfinite(frame["target"])
    frame = frame.loc[finite].copy()
    if route_id == "INTRADAY_STATE_TRANSITION" and not frame.empty:
        frame = frame.loc[frame["signal"].abs().gt(1e-12)].copy()
        frame["session"] = frame["time"].dt.normalize()
        frame = frame.sort_values(["code", "time"], kind="mergesort").drop_duplicates(
            ["code", "session"], keep="first"
        )
    if frame.empty:
        return {
            "reward": float("nan"), "rank_ic_mean": float("nan"), "rank_ic_hit_rate": float("nan"),
            "support": 0, "support_units": 0, "turnover": float("nan"), "cost_adjusted_reward": float("nan"),
            "behavior_identity": "", "top_symbol_share": 1.0, "positive_time_blocks": 0,
        }
    frame["signal_rank"] = frame.groupby("time", sort=False)["signal"].rank(method="average", pct=True)
    frame["target_rank"] = frame.groupby("time", sort=False)["target"].rank(method="average", pct=True)
    ics: list[tuple[pd.Timestamp, float]] = []
    for timestamp, group in frame.groupby("time", sort=False):
        if len(group) < 20 or group["signal_rank"].std() <= 1e-12 or group["target_rank"].std() <= 1e-12:
            continue
        value = group["signal_rank"].corr(group["target_rank"])
        if pd.notna(value):
            ics.append((pd.Timestamp(timestamp), float(value)))
    rank_ic = float(np.mean([value for _, value in ics])) if ics else float("nan")
    hit_rate = float(np.mean([value > 0 for _, value in ics])) if ics else float("nan")
    ordered = frame.sort_values(["code", "time"], kind="mergesort")
    turnover = float(ordered.groupby("code", sort=False)["signal_rank"].diff().abs().mean())
    cost_adjusted = rank_ic - 0.0025 * turnover if np.isfinite(rank_ic) and np.isfinite(turnover) else float("nan")
    counts = frame["code"].value_counts(normalize=True)
    block_values: dict[str, list[float]] = defaultdict(list)
    for timestamp, value in ics:
        block = f"{timestamp.year}{'H1' if timestamp.month <= 6 else 'H2'}"
        block_values[block].append(value)
    positive_blocks = sum(float(np.mean(values)) > 0 for values in block_values.values() if values)
    return {
        "reward": cost_adjusted,
        "rank_ic_mean": rank_ic,
        "rank_ic_hit_rate": hit_rate,
        "support": len(frame),
        "support_units": len(ics),
        "turnover": turnover,
        "cost_adjusted_reward": cost_adjusted,
        "behavior_identity": _behavior_identity(
            frame["signal_rank"].to_numpy(dtype=float),
            frame["time"].astype("int64").to_numpy(dtype=np.int64),
            frame["code"].astype(str).to_numpy(),
        ),
        "top_symbol_share": float(counts.max()) if not counts.empty else 1.0,
        "positive_time_blocks": positive_blocks,
    }


def _candidate_kind(candidate: Mapping[str, Any], registry: UnifiedCapabilityRegistry) -> str:
    fields = [registry.resolve(field_id) for field_id in candidate["field_ids"]]
    if candidate["route_id"] == "BROAD_EVENT_FROZEN_ENTRY":
        return "broad_event"
    if any(row.source_family.startswith("canonical_fundamental_") for row in fields):
        return "fundamental"
    return "active"


def _required_active_columns(candidates: Sequence[Mapping[str, Any]]) -> list[str]:
    fields = set()
    for row in candidates:
        fields.update(expression_fields(str(row["canonical_expression"])))
    fields.discard("")
    return sorted({"trade_time", "code", "close", *fields})


def _read_proxy_frame(
    release: ValidatedDevelopmentRelease,
    *,
    columns: Sequence[str],
    row_group_indices: Sequence[int],
    dates: Sequence[str],
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    selected_dates = set(pd.to_datetime(list(dates)).normalize())
    parts: list[pd.DataFrame] = []
    reads: list[dict[str, Any]] = []
    for file in release.files:
        parquet = pq.ParquetFile(file.path)
        schema = set(parquet.schema_arrow.names)
        missing = set(columns) - schema
        if missing:
            raise ValueError(f"proxy panel lacks fields {sorted(missing)}: {file.relative_path}")
        for row_group in row_group_indices:
            if row_group >= parquet.metadata.num_row_groups:
                raise ValueError(f"proxy row group out of range: {file.relative_path}#{row_group}")
            frame = parquet.read_row_group(row_group, columns=list(columns)).to_pandas()
            times = pd.to_datetime(frame["trade_time"], errors="raise")
            mask = times.dt.normalize().isin(selected_dates)
            if mask.any():
                frame = frame.loc[mask].copy()
                frame["trade_time"] = times.loc[mask]
                parts.append(frame)
            reads.append({"path": str(file.path), "row_group": row_group, "rows_decoded": len(times), "rows_selected": int(mask.sum()), "data_role": "development"})
    if not parts:
        raise RuntimeError("frozen proxy coordinates produced no development rows")
    output = pd.concat(parts, ignore_index=True).sort_values(["code", "trade_time"], kind="mergesort").reset_index(drop=True)
    if pd.to_datetime(output["trade_time"]).dt.year.max() >= 2026:
        raise PermissionError("proxy frame accessed forward rows")
    output["date"] = output["trade_time"]
    return output, reads


def _evaluate_active_candidates(frame: pd.DataFrame, candidates: Sequence[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    target = _same_session_future_return(frame, 5).to_numpy(dtype=float)
    expression_cache: dict[str, pd.Series] = {}
    metrics: dict[str, dict[str, Any]] = {}
    by_id = {row["candidate_id"]: row for row in candidates}
    for candidate in candidates:
        candidate_id = candidate["candidate_id"]
        if candidate_id in metrics:
            continue
        route_id = candidate["route_id"]
        if route_id == "INTRADAY_STATE_TRANSITION" and candidate["is_matched_control"]:
            continue
        signal = pd.to_numeric(
            evaluate_panel_expression(
                frame,
                _raw_expression(candidate["canonical_expression"]),
                cache=expression_cache,
                data_role="development",
            ), errors="coerce"
        ).to_numpy(dtype=float)
        if route_id == "INTRADAY_STATE_TRANSITION":
            control = by_id[candidate["matched_control_id"]]
            control_signal = pd.to_numeric(
                evaluate_panel_expression(
                    frame,
                    _raw_expression(control["canonical_expression"]),
                    cache=expression_cache,
                    data_role="development",
                ), errors="coerce"
            ).to_numpy(dtype=float)
            mask = np.isfinite(signal) & (np.abs(signal) > 1e-12)
            for row, values in ((candidate, signal), (control, control_signal)):
                metrics[row["candidate_id"]] = _metric_from_arrays(
                    signal=values[mask], target=target[mask],
                    times_ns=frame.loc[mask, "trade_time"].astype("int64").to_numpy(),
                    codes=frame.loc[mask, "code"].astype(str).to_numpy(), route_id=route_id,
                )
            continue
        mask = _checkpoint_mask(frame["trade_time"], route_id)
        metrics[candidate_id] = _metric_from_arrays(
            signal=signal[mask], target=target[mask],
            times_ns=frame.loc[mask, "trade_time"].astype("int64").to_numpy(),
            codes=frame.loc[mask, "code"].astype(str).to_numpy(), route_id=route_id,
        )
    return metrics


def _proxy_session_target(frame: pd.DataFrame) -> pd.DataFrame:
    ordered = frame.sort_values(["code", "trade_time"], kind="mergesort").copy()
    ordered["session"] = pd.to_datetime(ordered["trade_time"]).dt.normalize()
    grouped = ordered.groupby(["code", "session"], sort=False)
    rows = grouped.agg(first_close=("close", "first"), last_close=("close", "last")).reset_index()
    rows["target"] = pd.to_numeric(rows["last_close"], errors="coerce") / pd.to_numeric(rows["first_close"], errors="coerce").replace(0.0, np.nan) - 1.0
    rows["session_time"] = rows["session"] + pd.Timedelta(hours=15)
    return rows[["code", "session_time", "target"]]


def _fundamental_signal(frame: pd.DataFrame, candidate: Mapping[str, Any], field_id: str) -> tuple[pd.Series, np.ndarray]:
    values = pd.to_numeric(frame[field_id], errors="coerce")
    expression = str(candidate["canonical_expression"])
    if "EventCount(" in expression:
        signal = values.groupby(frame["code"], sort=False).rolling(5, min_periods=1).sum().reset_index(level=0, drop=True)
        support = values.eq(1.0).to_numpy()
    elif "TimeSince(" in expression:
        counters = []
        for _, group in frame.groupby("code", sort=False):
            age = 0
            out = []
            seen = False
            for value in values.loc[group.index].fillna(0.0):
                if value > 0:
                    age = 0
                    seen = True
                elif seen:
                    age += 1
                out.append(float(age) if seen else np.nan)
            counters.extend(zip(group.index, out))
        signal = pd.Series(dict(counters)).reindex(frame.index)
        support = values.eq(1.0).to_numpy()
    elif "Sign(" in expression:
        signal = np.sign(values)
        support = np.ones(len(frame), dtype=bool)
    else:
        signal = values
        support = np.ones(len(frame), dtype=bool)
    return pd.Series(signal, index=frame.index), support


def _evaluate_fundamental_candidates(
    *,
    candidates: Sequence[dict[str, Any]],
    registry: UnifiedCapabilityRegistry,
    adapter: PITFundamentalFabricAdapter,
    target_frame: pd.DataFrame,
    cache_root: Path,
) -> dict[str, dict[str, Any]]:
    cache_root.mkdir(parents=True, exist_ok=True)
    materializer = CanonicalFundamentalMaterializer(adapter)
    metrics: dict[str, dict[str, Any]] = {}
    by_id = {row["candidate_id"]: row for row in candidates}
    field_cache: dict[str, pd.DataFrame] = {}
    # PIT sidecars use the canonical six-digit security key.  The minute
    # release intentionally retains exchange suffixes (for example
    # ``000001.SZ``), so normalize the evaluation coordinates before both
    # materialization and the cache merge.  Otherwise a correctly populated
    # sidecar is silently converted into all-null evidence at the second join.
    fundamental_target = target_frame.copy()
    fundamental_target["code"] = fundamental_target["code"].map(normalize_cn_code)
    if fundamental_target["code"].eq("").any():
        raise ValueError("fundamental evaluation received an unrecognized security code")
    if fundamental_target.duplicated(["code", "session_time"]).any():
        raise ValueError(
            "fundamental evaluation coordinates are not unique after code normalization"
        )
    for candidate in candidates:
        if candidate["candidate_id"] in metrics:
            continue
        fields = [registry.resolve(field_id) for field_id in candidate["field_ids"]]
        canonical = next(row for row in fields if row.source_family.startswith("canonical_fundamental_"))
        field_id = canonical.field_id
        if field_id not in field_cache:
            cache_path = cache_root / f"{field_id}.parquet"
            if cache_path.exists():
                materialized = pd.read_parquet(cache_path)
            else:
                spec = dict((canonical.metadata or {})["canonical_representation"])
                materialized = materializer.materialize(
                    spec, fundamental_target[["code", "session_time"]]
                )
                materialized.to_parquet(cache_path, index=False)
            field_cache[field_id] = fundamental_target.merge(
                materialized,
                on=["code", "session_time"],
                how="left",
                validate="one_to_one",
            )
        frame = field_cache[field_id]
        signal, support = _fundamental_signal(frame, candidate, field_id)
        if candidate["route_id"] == "DISCLOSURE_EVENT" and not candidate["is_matched_control"]:
            control = by_id[candidate["matched_control_id"]]
            control_signal, _ = _fundamental_signal(frame, control, field_id)
            for row, values in ((candidate, signal), (control, control_signal)):
                metrics[row["candidate_id"]] = _metric_from_arrays(
                    signal=values.to_numpy(dtype=float)[support],
                    target=pd.to_numeric(frame["target"], errors="coerce").to_numpy(dtype=float)[support],
                    times_ns=frame.loc[support, "session_time"].astype("int64").to_numpy(),
                    codes=frame.loc[support, "code"].astype(str).to_numpy(),
                    route_id="DISCLOSURE_EVENT",
                )
            continue
        metrics[candidate["candidate_id"]] = _metric_from_arrays(
            signal=signal.to_numpy(dtype=float)[support],
            target=pd.to_numeric(frame["target"], errors="coerce").to_numpy(dtype=float)[support],
            times_ns=frame.loc[support, "session_time"].astype("int64").to_numpy(),
            codes=frame.loc[support, "code"].astype(str).to_numpy(),
            route_id=candidate["route_id"],
        )
    return metrics


def _apply_metrics(candidates: Sequence[dict[str, Any]], metrics: Mapping[str, Mapping[str, Any]]) -> None:
    by_id = {row["candidate_id"]: row for row in candidates}
    for row in candidates:
        row.update(dict(metrics.get(row["candidate_id"], {})))
        row["behavior_identity"] = row.get("behavior_identity", "")
        row["forbidden_read_count"] = 0
    for row in candidates:
        control = by_id.get(row["matched_control_id"])
        if row["is_matched_control"] or control is None:
            row["matched_increment"] = None
            row["development_increment_positive"] = False
            continue
        reward_value = row.get("reward")
        control_value = control.get("reward")
        reward = float(reward_value) if reward_value is not None else float("nan")
        control_reward = float(control_value) if control_value is not None else float("nan")
        increment = reward - control_reward if np.isfinite(reward) and np.isfinite(control_reward) else float("nan")
        row["matched_increment"] = increment
        row["development_increment_positive"] = bool(np.isfinite(increment) and increment > 0)


def _select_funnel(candidates: Sequence[dict[str, Any]], budgets: Mapping[str, Mapping[str, int]]) -> None:
    by_id = {row["candidate_id"]: row for row in candidates}
    for row in candidates:
        row["admission"] = False
        row["strict"] = False
        row["survivor"] = False
    for route_id in ROUTE_IDS:
        rows = [row for row in candidates if row["route_id"] == route_id and not row["is_matched_control"]]
        ordered = sorted(
            rows,
            key=lambda row: (
                -float(row.get("matched_increment") if row.get("matched_increment") is not None and np.isfinite(float(row.get("matched_increment"))) else -1e99),
                str(row["exact_identity"]),
            ),
        )
        admission_pairs = int(budgets[route_id]["admission"]) // 2
        strict_pairs = int(budgets[route_id]["strict"]) // 2
        if route_id == "BROAD_EVENT_FROZEN_ENTRY":
            admission_pairs = len(rows)
            strict_pairs = len(rows)
        for index, row in enumerate(ordered):
            control = by_id[row["matched_control_id"]]
            if index < admission_pairs:
                row["admission"] = control["admission"] = True
            if index < strict_pairs:
                row["strict"] = control["strict"] = True
            if index < strict_pairs and bool(row.get("development_increment_positive")) and int(row.get("support") or 0) > 0:
                row["survivor"] = True


def _rx_ucb_expand(
    *,
    generator: RegistryDrivenGenerator,
    roots: Sequence[dict[str, Any]],
    seed: int,
    total_pairs: int,
) -> list[dict[str, Any]]:
    arms: dict[str, list[float]] = defaultdict(list)
    for row in roots:
        if row["is_matched_control"] or row["route_id"] == "BROAD_EVENT_FROZEN_ENTRY":
            continue
        arms.setdefault(row["route_id"], [])
        value = row.get("matched_increment")
        if value is not None and np.isfinite(float(value)):
            arms[row["route_id"]].append(float(value))
    counts = Counter({route: max(1, len(values)) for route, values in arms.items()})
    total = sum(counts.values())
    output: list[dict[str, Any]] = []
    exact_seen = {row["exact_identity"] for row in roots}
    exhausted_arms: set[str] = set()
    for index in range(total_pairs):
        pair: list[dict[str, Any]] | None = None
        route = ""
        ranked_arms = sorted(
            (arm for arm in arms if arm not in exhausted_arms),
            key=lambda arm: (
                (float(np.mean(arms[arm])) if arms[arm] else 0.0)
                + math.sqrt(2.0 * math.log(max(total, 2)) / counts[arm]),
                arm,
            ),
            reverse=True,
        )
        for proposed_route in ranked_arms:
            for attempt in range(256):
                proposed = generator.generate_route(
                    proposed_route,
                    proposal_budget=2,
                    seed=seed + index * 1009 + attempt * 7919,
                )
                ids = {row["exact_identity"] for row in proposed}
                if not exact_seen.intersection(ids):
                    pair = proposed
                    route = proposed_route
                    break
            if pair is not None:
                break
            exhausted_arms.add(proposed_route)
        if pair is None:
            raise RuntimeError(
                "RX/UCB adaptive proposal underfill after exhausting every unique arm"
            )
        parent = max(
            (row for row in roots if row["route_id"] == route and not row["is_matched_control"]),
            key=lambda row: (
                float(row["matched_increment"])
                if row.get("matched_increment") is not None
                and np.isfinite(float(row["matched_increment"]))
                else -1e99
            ),
        )
        for row in pair:
            row["proposal_origin"] = "ephemeral_rx_ucb_adaptive" if not row["is_matched_control"] else "matched_nonadaptive_control"
            row["parent_id"] = parent["candidate_id"]
            row["adaptive_state_persisted"] = False
            row["adaptive_exhausted_arms"] = sorted(exhausted_arms)
            output.append(row)
        exact_seen.update(ids)
        counts[route] += 1
        total += 1
    return output


def _stream_full_active_evidence(
    *,
    release: ValidatedDevelopmentRelease,
    candidates: Sequence[dict[str, Any]],
    output_root: Path,
) -> tuple[dict[str, dict[str, Any]], pd.DataFrame, dict[str, Any]]:
    if not candidates:
        return {}, pd.DataFrame(), {"opened_row_groups": 0, "forbidden_read_count": 0}
    columns = _required_active_columns(candidates)
    by_id = {row["candidate_id"]: row for row in candidates}
    active_noncontrol = [row for row in candidates if not (row["route_id"] == "INTRADAY_STATE_TRANSITION" and row["is_matched_control"])]
    common: dict[str, dict[str, list[np.ndarray]]] = defaultdict(lambda: defaultdict(list))
    signals: dict[str, list[np.ndarray]] = defaultdict(list)
    state_common: dict[str, dict[str, list[np.ndarray]]] = defaultdict(lambda: defaultdict(list))
    daily_parts: list[pd.DataFrame] = []
    opened: list[dict[str, Any]] = []
    started = time.time()
    for file in release.files:
        parquet = pq.ParquetFile(file.path)
        schema = set(parquet.schema_arrow.names)
        if missing := set(columns) - schema:
            raise ValueError(f"strict panel lacks fields {sorted(missing)}: {file.relative_path}")
        for row_group in range(parquet.metadata.num_row_groups):
            frame = parquet.read_row_group(row_group, columns=columns).to_pandas()
            frame["trade_time"] = pd.to_datetime(frame["trade_time"], errors="raise")
            if frame["trade_time"].dt.year.max() >= 2026:
                raise PermissionError("strict stream accessed 2026")
            frame = frame.sort_values(["code", "trade_time"], kind="mergesort").reset_index(drop=True)
            frame["date"] = frame["trade_time"]
            target = _same_session_future_return(frame, 5).to_numpy(dtype=float)
            frame["session"] = frame["trade_time"].dt.normalize()
            expression_cache: dict[str, pd.Series] = {}
            daily_parts.append(
                frame.groupby(["code", "session"], sort=False).agg(close=("close", "last")).reset_index()
            )
            for candidate in active_noncontrol:
                route_id = candidate["route_id"]
                values = pd.to_numeric(
                    evaluate_panel_expression(
                        frame,
                        _raw_expression(candidate["canonical_expression"]),
                        cache=expression_cache,
                        data_role="development",
                    ), errors="coerce"
                ).to_numpy(dtype=float)
                if route_id == "INTRADAY_STATE_TRANSITION":
                    mask = np.isfinite(values) & (np.abs(values) > 1e-12)
                    control = by_id[candidate["matched_control_id"]]
                    control_values = pd.to_numeric(
                        evaluate_panel_expression(
                            frame,
                            _raw_expression(control["canonical_expression"]),
                            cache=expression_cache,
                            data_role="development",
                        ), errors="coerce"
                    ).to_numpy(dtype=float)
                    for row, signal_values in ((candidate, values), (control, control_values)):
                        cid = row["candidate_id"]
                        signals[cid].append(signal_values[mask].astype(np.float32))
                        state_common[cid]["target"].append(target[mask].astype(np.float32))
                        state_common[cid]["times"].append(frame.loc[mask, "trade_time"].astype("int64").to_numpy(dtype=np.int64))
                        state_common[cid]["codes"].append(frame.loc[mask, "code"].astype(str).to_numpy(dtype="S12"))
                    continue
                mask = _checkpoint_mask(frame["trade_time"], route_id)
                signals[candidate["candidate_id"]].append(values[mask].astype(np.float32))
            for route_id in sorted({row["route_id"] for row in active_noncontrol if row["route_id"] != "INTRADAY_STATE_TRANSITION"}):
                mask = _checkpoint_mask(frame["trade_time"], route_id)
                common[route_id]["target"].append(target[mask].astype(np.float32))
                common[route_id]["times"].append(frame.loc[mask, "trade_time"].astype("int64").to_numpy(dtype=np.int64))
                common[route_id]["codes"].append(frame.loc[mask, "code"].astype(str).to_numpy(dtype="S12"))
            opened.append({"path": str(file.path), "row_group": row_group, "rows": len(frame), "data_role": "development"})
    metrics: dict[str, dict[str, Any]] = {}
    for candidate in candidates:
        cid = candidate["candidate_id"]
        if candidate["route_id"] == "INTRADAY_STATE_TRANSITION":
            coords = state_common[cid]
        else:
            coords = common[candidate["route_id"]]
        metrics[cid] = _metric_from_arrays(
            signal=np.concatenate(signals[cid]) if signals[cid] else np.array([], dtype=float),
            target=np.concatenate(coords["target"]) if coords["target"] else np.array([], dtype=float),
            times_ns=np.concatenate(coords["times"]) if coords["times"] else np.array([], dtype=np.int64),
            codes=np.concatenate(coords["codes"]) if coords["codes"] else np.array([], dtype="S12"),
            route_id=candidate["route_id"],
        )
    daily = pd.concat(daily_parts, ignore_index=True).drop_duplicates(["code", "session"], keep="last")
    daily = daily.sort_values(["code", "session"], kind="mergesort")
    daily["target"] = daily.groupby("code", sort=False)["close"].shift(-1) / pd.to_numeric(daily["close"], errors="coerce").replace(0.0, np.nan) - 1.0
    daily["session_time"] = daily["session"] + pd.Timedelta(hours=15)
    daily_target = daily[["code", "session_time", "target"]].reset_index(drop=True)
    daily_target.to_parquet(output_root / "full_development_daily_target.parquet", index=False)
    access = {
        "data_role": "development_only",
        "opened_file_count": len(release.files),
        "opened_row_group_count": len(opened),
        "expected_row_group_count": sum(len(file.row_groups) for file in release.files),
        "all_row_groups_read": len(opened) == sum(len(file.row_groups) for file in release.files),
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "forbidden_read_count": 0,
        "elapsed_seconds": time.time() - started,
        "records": opened,
    }
    _write_json(output_root / "full_development_access_ledger.json", access)
    return metrics, daily_target, access


def _n_eff(values: Sequence[str]) -> float:
    counts = Counter(value for value in values if value)
    total = sum(counts.values())
    if not total:
        return 0.0
    return 1.0 / sum((count / total) ** 2 for count in counts.values())


def _route_summary(candidates: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for route_id in ROUTE_IDS:
        rows = [row for row in candidates if row["route_id"] == route_id]
        mechanisms = [row for row in rows if not row["is_matched_control"]]
        behaviors = [str(row.get("behavior_identity") or "") for row in rows if row.get("behavior_identity")]
        finite_increments = [
            float(row["matched_increment"])
            for row in mechanisms
            if row.get("matched_increment") is not None
            and np.isfinite(float(row["matched_increment"]))
        ]
        output[route_id] = {
            "proposal": len(rows),
            "legal": sum(bool(row.get("legal")) for row in rows),
            "canonical": len({row.get("canonical_identity") for row in rows}),
            "exact": len({row.get("exact_identity") for row in rows}),
            "behavior_cluster": len(set(behaviors)),
            "n_eff": _n_eff(behaviors),
            "admission": sum(bool(row.get("admission")) for row in rows),
            "strict": sum(bool(row.get("strict")) for row in rows),
            "survivor": sum(bool(row.get("survivor")) for row in rows),
            "matched_control_completion_rate": sum(bool(row.get("matched_control_id")) for row in mechanisms) / max(len(mechanisms), 1),
            "mean_matched_increment": (
                float(np.mean(finite_increments)) if finite_increments else None
            ),
        }
    return output


def run(
    *,
    repo: Path,
    registry_path: Path,
    contract_path: Path,
    preflight_path: Path,
    panel_root: Path,
    release_manifest_path: Path,
    split_manifest_path: Path,
    fundamental_source_root: Path,
    broad_event_pack_path: Path,
    chip_root: Path,
    broad_event_checkpoint_root: Path | None,
    output_root: Path,
    repo_sha: str,
) -> dict[str, Any]:
    started = time.time()
    if _git(repo, "rev-parse", "HEAD") != repo_sha or _git(repo, "status", "--porcelain=v1"):
        raise RuntimeError("unified discovery requires exact clean frozen repo SHA")
    contract = json.loads(contract_path.read_text(encoding="utf-8"))
    preflight = json.loads(preflight_path.read_text(encoding="utf-8"))
    if not preflight.get("all_gates_passed"):
        raise PermissionError("capability preflight did not pass")
    if contract["repo_sha"] != repo_sha:
        raise RuntimeError("contract repo SHA drift")
    registry = UnifiedCapabilityRegistry.read(registry_path)
    if registry.registry_hash != contract["registry_hash"]:
        raise RuntimeError("registry hash drift")
    if preflight.get("repo_sha") != repo_sha:
        raise RuntimeError("preflight repo SHA drift")
    if preflight.get("registry_hash") != registry.registry_hash:
        raise RuntimeError("preflight registry hash drift")
    if preflight.get("contract_hash") != contract["contract_hash"]:
        raise RuntimeError("preflight contract hash drift")
    release = validate_development_release(
        panel_root, release_manifest_path, split_manifest_path,
        expected_release_hash=contract["data_release"]["release_hash"],
    )
    generator = RegistryDrivenGenerator(registry)
    sessions = load_development_sessions(split_manifest_path)
    fundamental_adapter = PITFundamentalFabricAdapter(
        source_root=fundamental_source_root,
        sessions=sessions,
        maximum_observable_time=contract["maximum_observable_time"],
    )
    output_root.mkdir(parents=True, exist_ok=True)
    all_seed_canary: dict[str, list[dict[str, Any]]] = {}
    proxy_reads: list[dict[str, Any]] = []
    broad_root = output_root / "capability_canary" / "broad_event_full16"
    broad_seed_by_name = {
        seed_name: int(seed_contract["BROAD_EVENT_FROZEN_ENTRY"])
        for seed_name, seed_contract in contract["seed_sets"].items()
    }
    broad_seeds = sorted(set(broad_seed_by_name.values()))
    if broad_event_checkpoint_root is None:
        broad_summary = run_replay(
            entry_pack_path=broad_event_pack_path,
            data_root=panel_root,
            chip_root=chip_root,
            output_root=broad_root,
            repo_sha=repo_sha,
            seeds=broad_seeds,
        )
    else:
        broad_summary = _reuse_broad_checkpoint(
            repo=repo,
            repo_sha=repo_sha,
            checkpoint_root=broad_event_checkpoint_root,
            output_root=broad_root,
            entry_pack_path=broad_event_pack_path,
            chip_root=chip_root,
            release=release,
            seeds=broad_seeds,
        )
    broad_results = pd.read_csv(broad_root / "frozen_mechanism_results.csv")

    for seed_name in sorted(contract["seed_sets"]):
        rows = generator.dry_generate(contract["capability_canary"], seed_name=seed_name)
        active = [row for row in rows if _candidate_kind(row, registry) == "active"]
        fundamental = [row for row in rows if _candidate_kind(row, registry) == "fundamental"]
        columns = _required_active_columns(active)
        proxy_frame, reads = _read_proxy_frame(
            release,
            columns=columns,
            row_group_indices=contract["capability_canary_scope"]["fixed_row_group_indices"],
            dates=contract["capability_canary_scope"]["fixed_development_dates"],
        )
        proxy_reads.extend(reads)
        metrics = _evaluate_active_candidates(proxy_frame, active)
        target_frame = _proxy_session_target(proxy_frame)
        metrics.update(
            _evaluate_fundamental_candidates(
                candidates=fundamental,
                registry=registry,
                adapter=fundamental_adapter,
                target_frame=target_frame,
                cache_root=(
                    output_root / "capability_canary" / seed_name
                    / "fundamental_cache" / contract["contract_hash"]
                ),
            )
        )
        mechanism_by_id = {
            str(row["frozen_mechanism_id"]): row for row in rows
            if row["route_id"] == "BROAD_EVENT_FROZEN_ENTRY" and not row["is_matched_control"]
        }
        broad_seed = broad_seed_by_name[seed_name]
        for mechanism_id, row in mechanism_by_id.items():
            evidence = broad_results.loc[
                broad_results["mechanism_id"].astype(str).eq(mechanism_id)
                & broad_results["seed"].eq(broad_seed)
            ].iloc[0]
            metrics[row["candidate_id"]] = {
                "reward": float(evidence["event_reward"]),
                "rank_ic_mean": float(evidence["event_reward"]),
                "rank_ic_hit_rate": float("nan"),
                "support": int(evidence["support"]),
                "support_units": int(evidence["support"]),
                "turnover": float("nan"),
                "cost_adjusted_reward": float(evidence["event_reward"]),
                "behavior_identity": str(evidence["behavior_identity"]),
                "top_symbol_share": float(evidence["top_symbol_share"]),
                "positive_time_blocks": int(evidence["consistent_positive_blocks"]),
            }
            control = next(item for item in rows if item["candidate_id"] == row["matched_control_id"])
            metrics[control["candidate_id"]] = {
                **metrics[row["candidate_id"]],
                "reward": float(evidence["matched_control_ceiling"]),
                "rank_ic_mean": float(evidence["matched_control_ceiling"]),
                "cost_adjusted_reward": float(evidence["matched_control_ceiling"]),
                "behavior_identity": stable_hash({"mechanism": mechanism_id, "control": True})[:24],
            }
        _apply_metrics(rows, metrics)
        _select_funnel(rows, contract["capability_canary"]["route_budgets"])
        ledger = SearchExposureLedger(
            registry=registry,
            run_id=f"{contract['experiment_id']}_canary_{seed_name}",
            repo_sha=repo_sha,
            contract_hash=contract["contract_hash"],
            data_release_hash=release.release_hash,
            route_budgets=contract["capability_canary"]["route_budgets"],
        )
        ledger.ingest(rows)
        ledger.write(output_root / "capability_canary" / seed_name / "exposure_ledger")
        _write_csv(output_root / "capability_canary" / seed_name / "candidate_results.csv", rows)
        all_seed_canary[seed_name] = rows

    canary_routes = {
        route_id: all(
            any(row["route_id"] == route_id and bool(row.get("strict")) and int(row.get("support") or 0) > 0 for row in rows)
            for rows in all_seed_canary.values()
        )
        for route_id in ROUTE_IDS
    }
    canary_summary = {
        "status": "CN_UNIFIED_CAPABILITY_CANARY_COMPLETED" if all(canary_routes.values()) else "CN_UNIFIED_CAPABILITY_CANARY_FAILED",
        "route_nonzero_strict_support": canary_routes,
        "seed_route_summaries": {seed: _route_summary(rows) for seed, rows in all_seed_canary.items()},
        "broad_event_full16": broad_summary,
        "proxy_physical_shards": len({row["path"] for row in proxy_reads}),
        "proxy_forbidden_read_count": 0,
        "validation_accessed": False,
        "holdout_accessed": False,
        "forward_2026_accessed": False,
        "candidate_promotion": False,
    }
    _write_json(output_root / "capability_canary" / "summary.json", canary_summary)
    if not all(canary_routes.values()):
        raise RuntimeError("capability CANARY did not produce nonzero strict support on every enabled route")

    # Unified discovery begins only after the CANARY gate above.
    discovery_by_seed: dict[str, list[dict[str, Any]]] = {}
    for seed_name in sorted(contract["seed_sets"]):
        roots = generator.dry_generate(contract["unified_discovery"], seed_name=seed_name)
        active = [row for row in roots if _candidate_kind(row, registry) == "active"]
        fundamental = [row for row in roots if _candidate_kind(row, registry) == "fundamental"]
        proxy_frame, _ = _read_proxy_frame(
            release,
            columns=_required_active_columns(active),
            row_group_indices=contract["capability_canary_scope"]["fixed_row_group_indices"],
            dates=contract["capability_canary_scope"]["fixed_development_dates"],
        )
        metrics = _evaluate_active_candidates(proxy_frame, active)
        metrics.update(
            _evaluate_fundamental_candidates(
                candidates=fundamental,
                registry=registry,
                adapter=fundamental_adapter,
                target_frame=_proxy_session_target(proxy_frame),
                cache_root=(
                    output_root / "unified_discovery" / seed_name
                    / "fundamental_proxy_cache" / contract["contract_hash"]
                ),
            )
        )
        # Reuse the already completed full16 frozen event replay.
        canary_broad = {
            (row["frozen_mechanism_id"], bool(row["is_matched_control"])): row
            for row in all_seed_canary[seed_name]
            if row["route_id"] == "BROAD_EVENT_FROZEN_ENTRY"
        }
        for row in roots:
            if row["route_id"] != "BROAD_EVENT_FROZEN_ENTRY":
                continue
            source = canary_broad.get(
                (row.get("frozen_mechanism_id"), bool(row["is_matched_control"]))
            )
            if source:
                metrics[row["candidate_id"]] = {key: source.get(key) for key in (
                    "reward", "rank_ic_mean", "rank_ic_hit_rate", "support", "support_units", "turnover",
                    "cost_adjusted_reward", "behavior_identity", "top_symbol_share", "positive_time_blocks",
                )}
        _apply_metrics(roots, metrics)
        adaptive = _rx_ucb_expand(
            generator=generator,
            roots=roots,
            seed=int(contract["seed_sets"][seed_name]["MINUTE_STATIC"]) + 50000,
            total_pairs=int(contract["unified_discovery"]["adaptive_challenger"]["proposal_budget_per_seed"]),
        )
        adaptive_active = [row for row in adaptive if _candidate_kind(row, registry) == "active"]
        adaptive_fund = [row for row in adaptive if _candidate_kind(row, registry) == "fundamental"]
        adaptive_metrics = _evaluate_active_candidates(proxy_frame, adaptive_active)
        adaptive_metrics.update(
            _evaluate_fundamental_candidates(
                candidates=adaptive_fund,
                registry=registry,
                adapter=fundamental_adapter,
                target_frame=_proxy_session_target(proxy_frame),
                cache_root=(
                    output_root / "unified_discovery" / seed_name
                    / "fundamental_proxy_cache" / contract["contract_hash"]
                ),
            )
        )
        _apply_metrics(adaptive, adaptive_metrics)
        combined = roots + adaptive
        _select_funnel(roots, contract["unified_discovery"]["route_budgets"])
        # RX/UCB has its own frozen matched budget.
        adaptive_budget = contract["unified_discovery"]["adaptive_challenger"]
        adaptive_mechanisms = sorted(
            [row for row in adaptive if not row["is_matched_control"]],
            key=lambda row: -float(row.get("matched_increment") or -1e99),
        )
        by_id = {row["candidate_id"]: row for row in adaptive}
        for index, row in enumerate(adaptive_mechanisms):
            control = by_id[row["matched_control_id"]]
            if index < int(adaptive_budget["admission_budget_per_seed"]):
                row["admission"] = control["admission"] = True
            if index < int(adaptive_budget["strict_budget_per_seed"]):
                row["strict"] = control["strict"] = True
        discovery_by_seed[seed_name] = combined

    strict_active_by_exact: dict[str, dict[str, Any]] = {}
    strict_fund_by_exact: dict[str, dict[str, Any]] = {}
    for rows in discovery_by_seed.values():
        for row in rows:
            if not bool(row.get("strict")) or row["route_id"] == "BROAD_EVENT_FROZEN_ENTRY":
                continue
            target = strict_fund_by_exact if _candidate_kind(row, registry) == "fundamental" else strict_active_by_exact
            target.setdefault(row["exact_identity"], row)
    full_root = output_root / "unified_discovery" / "full_development_evidence"
    full_root.mkdir(parents=True, exist_ok=True)
    active_metrics, daily_target, full_access = _stream_full_active_evidence(
        release=release,
        candidates=list(strict_active_by_exact.values()),
        output_root=full_root,
    )
    fundamental_metrics = _evaluate_fundamental_candidates(
        candidates=list(strict_fund_by_exact.values()),
        registry=registry,
        adapter=fundamental_adapter,
        target_frame=daily_target,
        cache_root=full_root / "fundamental_cache" / contract["contract_hash"],
    )
    strict_metrics = {**active_metrics, **fundamental_metrics}
    exact_metric = {
        row["exact_identity"]: strict_metrics[row["candidate_id"]]
        for row in [*strict_active_by_exact.values(), *strict_fund_by_exact.values()]
    }
    for rows in discovery_by_seed.values():
        for row in rows:
            if row["exact_identity"] in exact_metric and bool(row.get("strict")):
                row.update(exact_metric[row["exact_identity"]])
        _apply_metrics(rows, {row["candidate_id"]: row for row in rows})
        for row in rows:
            if bool(row.get("strict")) and not row["is_matched_control"]:
                row["survivor"] = bool(
                    row.get("development_increment_positive")
                    and int(row.get("support") or 0) > 0
                    and float(row.get("top_symbol_share") or 1.0) <= 0.20
                    and int(row.get("positive_time_blocks") or 0) >= 2
                )
        ledger = SearchExposureLedger(
            registry=registry,
            run_id=f"{contract['experiment_id']}_discovery_{rows[0]['seed_set']}",
            repo_sha=repo_sha,
            contract_hash=contract["contract_hash"],
            data_release_hash=release.release_hash,
            route_budgets=contract["unified_discovery"]["route_budgets"],
        )
        ledger.ingest(rows)
        ledger.write(output_root / "unified_discovery" / rows[0]["seed_set"] / "exposure_ledger")
        _write_csv(output_root / "unified_discovery" / rows[0]["seed_set"] / "candidate_results.csv", rows)

    survivor_sets = {
        seed: {row["exact_identity"] for row in rows if bool(row.get("survivor"))}
        for seed, rows in discovery_by_seed.items()
    }
    shared_survivors = set.intersection(*survivor_sets.values()) if survivor_sets else set()
    pack_rows = []
    for exact_identity in sorted(shared_survivors):
        exemplars = [next(row for row in rows if row["exact_identity"] == exact_identity) for rows in discovery_by_seed.values()]
        row = dict(exemplars[0])
        row["pack_role"] = "FROZEN_DEVELOPMENT_DISCOVERY_ONLY_NO_PROMOTION"
        row["seed_reproduction_count"] = len(exemplars)
        row["forward_2026_allowed"] = False
        row["candidate_promotion_allowed"] = False
        row["cross_sprint_memory_allowed"] = False
        pack_rows.append(row)
    _write_csv(output_root / "unified_development_discovery_pack.csv", pack_rows)
    pack_hash = _sha256(output_root / "unified_development_discovery_pack.csv")
    route_summaries = {seed: _route_summary(rows) for seed, rows in discovery_by_seed.items()}
    route_exposure_ok = all(
        all(summary[route_id]["strict"] > 0 for route_id in ROUTE_IDS)
        for summary in route_summaries.values()
    )
    challenge_eligible = bool(pack_rows) and route_exposure_ok and full_access["all_row_groups_read"]
    summary = {
        "status": "CN_UNIFIED_CAPABILITY_DISCOVERY_COMPLETED" if route_exposure_ok else "CN_UNIFIED_CAPABILITY_DISCOVERY_PARTIALLY_COMPLETED",
        "repo_sha": repo_sha,
        "contract_hash": contract["contract_hash"],
        "registry_hash": registry.registry_hash,
        "canary_status": canary_summary["status"],
        "seed_route_summaries": route_summaries,
        "data_families_tested": sorted({row.source_family for row in registry.fields if any(row.field_id in candidate["field_ids"] for rows in discovery_by_seed.values() for candidate in rows)}),
        "not_wired_or_not_evaluated": sorted({row.source_family for row in registry.fields if row.search_eligible and not any(row.field_id in candidate["field_ids"] for rows in discovery_by_seed.values() for candidate in rows)}),
        "shared_survivor_count": len(pack_rows),
        "candidate_pack_sha256": pack_hash,
        "candidate_pack_role": "FROZEN_DEVELOPMENT_DISCOVERY_ONLY_NO_PROMOTION",
        "qualified_to_apply_for_independent_challenge": challenge_eligible,
        "challenge_not_opened": True,
        "full_development_access": full_access,
        "evidence_ceiling": "DEVELOPMENT_ONLY_NOT_OOS",
        "validation_accessed": False,
        "holdout_accessed": False,
        "forward_2026_accessed": False,
        "candidate_promotion": False,
        "cross_sprint_memory": False,
        "elapsed_seconds": time.time() - started,
    }
    _write_json(output_root / "summary.json", summary)
    manifest = {
        "run_manifest_version": "cn_unified_capability_discovery_run_manifest_v1",
        "experiment_id": contract["experiment_id"],
        "status": summary["status"],
        "started_at_utc": datetime.fromtimestamp(started, timezone.utc).isoformat(),
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "repo_sha": repo_sha,
        "inputs": {
            "contract": {"path": str(contract_path), "sha256": _sha256(contract_path)},
            "registry": {"path": str(registry_path), "sha256": _sha256(registry_path)},
            "preflight": {"path": str(preflight_path), "sha256": _sha256(preflight_path)},
            "release_manifest": {"path": str(release_manifest_path), "sha256": _sha256(release_manifest_path)},
            "split_manifest": {"path": str(split_manifest_path), "sha256": _sha256(split_manifest_path)},
            "broad_event_pack": {"path": str(broad_event_pack_path), "sha256": _sha256(broad_event_pack_path)},
            "broad_event_checkpoint": (
                {"path": str(broad_event_checkpoint_root), "reused": True}
                if broad_event_checkpoint_root is not None else None
            ),
        },
        "parameters": {
            "seeds": contract["seed_sets"],
            "route_budgets": contract["unified_discovery"]["route_budgets"],
            "adaptive_challenger": contract["unified_discovery"]["adaptive_challenger"],
            "all_row_groups": True,
            "strict_intraday_checkpoints": ["10:30", "14:30"],
        },
        "outputs": {
            "summary": str(output_root / "summary.json"),
            "candidate_pack": str(output_root / "unified_development_discovery_pack.csv"),
            "candidate_pack_sha256": pack_hash,
        },
        "reproducibility": "YES_FOR_FROZEN_INPUTS_TWO_SEEDS",
        "continuation": "independent challenge requires separate authorization; forward remains sealed",
        "failure": None,
    }
    _write_json(output_root / "run_manifest.json", manifest)
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--contract", type=Path, required=True)
    parser.add_argument("--preflight", type=Path, required=True)
    parser.add_argument("--panel-root", type=Path, required=True)
    parser.add_argument("--release-manifest", type=Path, required=True)
    parser.add_argument("--split-manifest", type=Path, required=True)
    parser.add_argument("--fundamental-source-root", type=Path, required=True)
    parser.add_argument("--broad-event-pack", type=Path, required=True)
    parser.add_argument("--chip-root", type=Path, required=True)
    parser.add_argument("--broad-event-checkpoint-root", type=Path)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--repo-sha", required=True)
    args = parser.parse_args(argv)
    result = run(
        repo=args.repo,
        registry_path=args.registry,
        contract_path=args.contract,
        preflight_path=args.preflight,
        panel_root=args.panel_root,
        release_manifest_path=args.release_manifest,
        split_manifest_path=args.split_manifest,
        fundamental_source_root=args.fundamental_source_root,
        broad_event_pack_path=args.broad_event_pack,
        chip_root=args.chip_root,
        broad_event_checkpoint_root=args.broad_event_checkpoint_root,
        output_root=args.output_root,
        repo_sha=args.repo_sha,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result["status"] == "CN_UNIFIED_CAPABILITY_DISCOVERY_COMPLETED" else 2


if __name__ == "__main__":
    raise SystemExit(main())
