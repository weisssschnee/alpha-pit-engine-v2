"""Label-free portfolio behavior identities for iterative-search admission.

The bounded probe and the full-coordinate identity deliberately consume only
signals and portfolio mapping responses.  Returns, rewards, RankIC, costs and
labels are not accepted by this module, so they cannot leak into dedupe or
diversity admission.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Sequence

import numpy as np
import pandas as pd
import polars as pl

from our_system_phase2.services.phase3cm_time_major_sidecar import STABLE_KEY


BEHAVIOR_VERSION = "cn_portfolio_behavior_v1"
BEHAVIOR_UNRESOLVED = "BEHAVIOR_UNRESOLVED"
_FIELD_PATTERN = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)")


def _stable_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _identity(prefix: str, value: Any) -> str:
    digest = hashlib.sha256(_stable_json(value).encode("utf-8")).hexdigest()
    return f"{prefix}.{digest[:32]}"


def _finite_float(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


@dataclass
class _CandidateBehaviorState:
    block_digests: list[str] = field(default_factory=list)
    observation_count: int = 0
    coordinate_count: int = 0
    selected_count: int = 0
    support_sum: float = 0.0
    turnover_sum: float = 0.0
    turnover_observations: int = 0
    selected_frequency: dict[int, int] = field(default_factory=dict)
    previous_weights: dict[int, float] = field(default_factory=dict)


class StreamingLabelFreeBehavior:
    """Accumulate deterministic behavior from signals without outcome access."""

    def __init__(
        self,
        *,
        candidate_ids: Sequence[str],
        code_count: int,
        coordinate_binding: str,
        scope: str,
        min_obs: int = 2,
        top_quantile: float = 0.2,
    ) -> None:
        if scope not in {"probe", "full"}:
            raise ValueError("behavior scope must be 'probe' or 'full'")
        if code_count <= 0:
            raise ValueError("code_count must be positive")
        if not 0.0 < float(top_quantile) <= 1.0:
            raise ValueError("top_quantile must be in (0, 1]")
        self.candidate_ids = tuple(str(value) for value in candidate_ids)
        self.code_count = int(code_count)
        self.coordinate_binding = str(coordinate_binding)
        self.scope = scope
        self.min_obs = max(1, int(min_obs))
        self.top_quantile = float(top_quantile)
        self._states = [_CandidateBehaviorState() for _ in self.candidate_ids]
        self._time_count = 0

    def update_block(
        self,
        *,
        signals: np.ndarray,
        time_ids: np.ndarray,
        code_ids: np.ndarray,
        trade_times_ns: np.ndarray,
        directions: np.ndarray | Sequence[float] | None = None,
    ) -> None:
        matrix = np.asarray(signals, dtype=np.float64)
        if matrix.ndim != 2 or matrix.shape[0] != len(self.candidate_ids):
            raise ValueError("signals must have shape [candidate_count, observation_count]")
        count = matrix.shape[1]
        times = np.asarray(time_ids, dtype=np.int64)
        codes = np.asarray(code_ids, dtype=np.int64)
        trade_times = np.asarray(trade_times_ns, dtype=np.int64)
        if times.shape != (count,) or codes.shape != (count,) or trade_times.shape != (count,):
            raise ValueError("coordinate arrays must match the signal observation count")
        if np.any(codes < 0) or np.any(codes >= self.code_count):
            raise ValueError("code_ids exceed the declared code_count")
        direction_values = (
            np.ones(len(self.candidate_ids), dtype=np.float64)
            if directions is None
            else np.asarray(directions, dtype=np.float64)
        )
        if direction_values.shape != (len(self.candidate_ids),):
            raise ValueError("directions must have one value per candidate")

        ordered_times = np.unique(times)
        self._time_count += int(len(ordered_times))
        block_digests = [hashlib.sha256() for _ in self.candidate_ids]
        for time_id in ordered_times:
            mask = times == time_id
            block_codes = codes[mask]
            block_trade_times = trade_times[mask]
            if block_codes.size == 0:
                continue
            order = np.lexsort((block_codes, block_trade_times))
            block_codes = block_codes[order]
            block_trade_times = block_trade_times[order]
            for candidate_index, state in enumerate(self._states):
                values = matrix[candidate_index, mask][order]
                finite = np.isfinite(values)
                finite_count = int(finite.sum())
                state.coordinate_count += int(values.size)
                state.observation_count += finite_count
                support = finite_count / max(1, int(values.size))
                state.support_sum += support
                selected_codes: list[int] = []
                weights: dict[int, float] = {}
                if finite_count:
                    finite_codes = block_codes[finite]
                    finite_values = values[finite] * (1.0 if direction_values[candidate_index] >= 0 else -1.0)
                    take = max(1, int(math.ceil(finite_count * self.top_quantile)))
                    ranked = sorted(
                        zip(finite_values.tolist(), finite_codes.tolist()),
                        key=lambda item: (-float(item[0]), int(item[1])),
                    )
                    selected_codes = sorted(int(code) for _, code in ranked[:take])
                    equal_weight = round(1.0 / len(selected_codes), 12)
                    weights = {code: equal_weight for code in selected_codes}
                all_weight_codes = set(state.previous_weights) | set(weights)
                turnover = 0.5 * sum(
                    abs(weights.get(code, 0.0) - state.previous_weights.get(code, 0.0))
                    for code in all_weight_codes
                )
                state.turnover_sum += turnover
                state.turnover_observations += 1
                state.previous_weights = weights
                state.selected_count += len(selected_codes)
                for code in selected_codes:
                    state.selected_frequency[code] = state.selected_frequency.get(code, 0) + 1
                response = {
                    "time_id": int(time_id),
                    "trade_time_ns": int(block_trade_times[0]),
                    "support": round(support, 12),
                    "selected_codes": selected_codes,
                    "weights": [[code, weights[code]] for code in selected_codes],
                    "turnover": round(turnover, 12),
                }
                block_digests[candidate_index].update((_stable_json(response) + "\n").encode("utf-8"))
        for state, block_digest in zip(self._states, block_digests):
            state.block_digests.append(block_digest.hexdigest())

    def rows(self) -> list[dict[str, Any]]:
        output: list[dict[str, Any]] = []
        time_count = max(1, self._time_count)
        for candidate_id, state in zip(self.candidate_ids, self._states):
            resolved = state.observation_count >= self.min_obs and state.selected_count > 0
            if not resolved:
                output.append(
                    {
                        "candidate_id": candidate_id,
                        "behavior_scope": self.scope,
                        "behavior_status": BEHAVIOR_UNRESOLVED,
                        "behavior_probe_id": "",
                        "portfolio_behavior_signature_id": "",
                        "portfolio_behavior_family_id": "",
                        "coordinate_binding": self.coordinate_binding,
                        "coordinate_count": state.coordinate_count,
                        "finite_observation_count": state.observation_count,
                        "support_rate": round(state.observation_count / max(1, state.coordinate_count), 12),
                        "mean_turnover": round(state.turnover_sum / max(1, state.turnover_observations), 12),
                    }
                )
                continue
            exact_payload = {
                "version": BEHAVIOR_VERSION,
                "scope": self.scope,
                "coordinate_binding": self.coordinate_binding,
                "candidate_response_block_digests": list(state.block_digests),
            }
            exact_digest = _identity("cn.behavior", exact_payload)
            family_vector = [
                round(state.selected_frequency.get(code, 0) / time_count, 1)
                for code in range(self.code_count)
            ]
            family_payload = {
                "version": BEHAVIOR_VERSION,
                "selection_frequency_decile": family_vector,
                "support_decile": round(state.observation_count / max(1, state.coordinate_count), 1),
                "turnover_decile": round(state.turnover_sum / max(1, state.turnover_observations), 1),
            }
            output.append(
                {
                    "candidate_id": candidate_id,
                    "behavior_scope": self.scope,
                    "behavior_status": "RESOLVED",
                    "behavior_probe_id": exact_digest if self.scope == "probe" else "",
                    "portfolio_behavior_signature_id": exact_digest if self.scope == "full" else "",
                    "portfolio_behavior_family_id": _identity("cn.behavior_family", family_payload)
                    if self.scope == "full"
                    else "",
                    "coordinate_binding": self.coordinate_binding,
                    "coordinate_count": state.coordinate_count,
                    "finite_observation_count": state.observation_count,
                    "support_rate": round(state.observation_count / max(1, state.coordinate_count), 12),
                    "mean_turnover": round(state.turnover_sum / max(1, state.turnover_observations), 12),
                }
            )
        return output

    def continuation_payload(self) -> dict[str, Any]:
        """Return a JSON-safe continuation embedded in the Phase3CM checkpoint."""

        return {
            "schema_version": "cn_label_free_behavior_continuation_v1",
            "candidate_ids": list(self.candidate_ids),
            "code_count": self.code_count,
            "coordinate_binding": self.coordinate_binding,
            "scope": self.scope,
            "min_obs": self.min_obs,
            "top_quantile": self.top_quantile,
            "time_count": self._time_count,
            "states": [
                {
                    "block_digests": list(state.block_digests),
                    "observation_count": state.observation_count,
                    "coordinate_count": state.coordinate_count,
                    "selected_count": state.selected_count,
                    "support_sum": state.support_sum,
                    "turnover_sum": state.turnover_sum,
                    "turnover_observations": state.turnover_observations,
                    "selected_frequency": {str(key): value for key, value in state.selected_frequency.items()},
                    "previous_weights": {str(key): value for key, value in state.previous_weights.items()},
                }
                for state in self._states
            ],
        }

    def restore_continuation_payload(self, payload: dict[str, Any]) -> None:
        if str(payload.get("schema_version") or "") != "cn_label_free_behavior_continuation_v1":
            raise ValueError("label-free behavior continuation schema drift")
        if tuple(map(str, payload.get("candidate_ids") or ())) != self.candidate_ids:
            raise ValueError("label-free behavior candidate identity drift")
        if int(payload.get("code_count") or 0) != self.code_count:
            raise ValueError("label-free behavior code universe drift")
        if str(payload.get("coordinate_binding") or "") != self.coordinate_binding or str(payload.get("scope") or "") != self.scope:
            raise ValueError("label-free behavior coordinate/scope drift")
        rows = list(payload.get("states") or ())
        if len(rows) != len(self._states):
            raise ValueError("label-free behavior state count drift")
        self._time_count = int(payload.get("time_count") or 0)
        for state, row in zip(self._states, rows):
            state.block_digests = list(map(str, row.get("block_digests") or ()))
            state.observation_count = int(row.get("observation_count") or 0)
            state.coordinate_count = int(row.get("coordinate_count") or 0)
            state.selected_count = int(row.get("selected_count") or 0)
            state.support_sum = float(row.get("support_sum") or 0.0)
            state.turnover_sum = float(row.get("turnover_sum") or 0.0)
            state.turnover_observations = int(row.get("turnover_observations") or 0)
            state.selected_frequency = {int(key): int(value) for key, value in dict(row.get("selected_frequency") or {}).items()}
            state.previous_weights = {int(key): float(value) for key, value in dict(row.get("previous_weights") or {}).items()}


def pair_behavior_record(
    *,
    batch_id: str,
    pair_id: str,
    route_id: str,
    primary_candidate_id: str,
    control_candidate_id: str,
    structural_family_id: str,
    primary_behavior: dict[str, Any],
    control_behavior: dict[str, Any],
    signal_cluster_id: str = "",
) -> dict[str, Any]:
    """Bind a pair to exact and approximate identities without fallback."""

    resolved = all(
        str(row.get("behavior_status") or "") == "RESOLVED"
        for row in (primary_behavior, control_behavior)
    )
    scopes = {str(primary_behavior.get("behavior_scope") or ""), str(control_behavior.get("behavior_scope") or "")}
    probe_ids = [
        str(primary_behavior.get("behavior_probe_id") or ""),
        str(control_behavior.get("behavior_probe_id") or ""),
    ]
    signature_ids = [
        str(primary_behavior.get("portfolio_behavior_signature_id") or ""),
        str(control_behavior.get("portfolio_behavior_signature_id") or ""),
    ]
    family_ids = [
        str(primary_behavior.get("portfolio_behavior_family_id") or ""),
        str(control_behavior.get("portfolio_behavior_family_id") or ""),
    ]
    if not resolved:
        status = BEHAVIOR_UNRESOLVED
        probe_id = signature_id = family_id = ""
    elif scopes == {"probe"} and all(probe_ids):
        status = "RESOLVED"
        probe_id = _identity("cn.pair_behavior_probe", probe_ids)
        signature_id = family_id = ""
    elif scopes == {"full"} and all(signature_ids) and all(family_ids):
        status = "RESOLVED"
        probe_id = ""
        signature_id = _identity("cn.pair_behavior", signature_ids)
        family_id = _identity("cn.pair_behavior_family", family_ids)
    else:
        status = BEHAVIOR_UNRESOLVED
        probe_id = signature_id = family_id = ""
    return {
        "behavior_identity_version": BEHAVIOR_VERSION,
        "batch_id": str(batch_id),
        "pair_id": str(pair_id),
        "route_id": str(route_id),
        "primary_candidate_id": str(primary_candidate_id),
        "control_candidate_id": str(control_candidate_id),
        "structural_family_id": str(structural_family_id),
        "signal_cluster_id": str(signal_cluster_id),
        "behavior_probe_id": probe_id,
        "primary_behavior_probe_id": probe_ids[0] if status == "RESOLVED" and scopes == {"probe"} else "",
        "control_behavior_probe_id": probe_ids[1] if status == "RESOLVED" and scopes == {"probe"} else "",
        "portfolio_behavior_signature_id": signature_id,
        "portfolio_behavior_family_id": family_id,
        "behavior_status": status,
        "primary_support_rate": _finite_float(primary_behavior.get("support_rate")),
        "control_support_rate": _finite_float(control_behavior.get("support_rate")),
        "primary_mean_turnover": _finite_float(primary_behavior.get("mean_turnover")),
        "control_mean_turnover": _finite_float(control_behavior.get("mean_turnover")),
        "coordinate_binding": str(primary_behavior.get("coordinate_binding") or ""),
    }


class PortfolioBehaviorArchive:
    """Cumulative exact behavior archive; batch manifests freeze batch views."""

    def __init__(self, rows: Iterable[dict[str, Any]] | None = None) -> None:
        self.rows = [dict(row) for row in (rows or ())]

    def add(self, row: dict[str, Any]) -> None:
        self.rows.append(dict(row))

    def contains_probe(self, behavior_probe_id: str) -> bool:
        identity = str(behavior_probe_id)
        return bool(identity) and any(str(row.get("behavior_probe_id") or "") == identity for row in self.rows)

    def contains_signature(self, signature_id: str) -> bool:
        identity = str(signature_id)
        return bool(identity) and any(
            str(row.get("portfolio_behavior_signature_id") or "") == identity for row in self.rows
        )

    def write_parquet(self, path: Path) -> None:
        destination = Path(path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(self.rows).fillna("").to_parquet(destination, index=False)

    @classmethod
    def read_parquet(cls, path: Path) -> "PortfolioBehaviorArchive":
        source = Path(path)
        if not source.exists():
            return cls()
        frame = pd.read_parquet(source).fillna("")
        return cls(frame.to_dict(orient="records"))


def _structural_family_id(candidate: dict[str, Any]) -> str:
    explicit = str(candidate.get("structural_family_id") or "")
    if explicit:
        return explicit
    expression = str(candidate.get("expression") or "")
    skeleton = _FIELD_PATTERN.sub("$FIELD", expression)
    skeleton = re.sub(r"(?<![A-Za-z_])\d+(?:\.\d+)?", "N", skeleton)
    return _identity(
        "cn.structural_family",
        {
            "route_id": str(candidate.get("route_id") or ""),
            "operator_family": str(candidate.get("operator_family") or ""),
            "expression_skeleton": skeleton,
        },
    )


def _signal_cluster_id(values: np.ndarray, time_ids: np.ndarray) -> str:
    hist = np.zeros(10, dtype=np.int64)
    time_means: list[float] = []
    for time_id in np.unique(time_ids):
        block = np.asarray(values[time_ids == time_id], dtype=np.float64)
        finite = np.isfinite(block)
        if not finite.any():
            continue
        clean = block[finite]
        order = np.argsort(np.argsort(clean, kind="stable"), kind="stable")
        percentiles = (order + 0.5) / len(clean)
        hist += np.histogram(percentiles, bins=np.linspace(0.0, 1.0, 11))[0]
        time_means.append(float(np.mean(clean)))
    if not time_means:
        return ""
    total = max(1, int(hist.sum()))
    mean_sign_changes = sum(
        1
        for left, right in zip(time_means, time_means[1:])
        if (left < 0.0) != (right < 0.0)
    )
    return _identity(
        "cn.signal_cluster",
        {
            "rank_histogram_pct": [round(int(value) / total, 1) for value in hist],
            "mean_sign_change_bucket": min(9, mean_sign_changes),
        },
    )


def bounded_label_free_behavior_probe(
    *,
    candidates: Sequence[dict[str, Any]],
    field_sidecars: Sequence[Path],
    eligible_trade_dates: Sequence[str],
    coordinate_binding: str,
    batch_id: str,
    compute_threads: int = 8,
    max_trade_times: int = 30,
    pair_batch_size: int = 8,
    min_obs: int = 20,
    top_quantile: float = 0.2,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Materialize a bounded field-only probe before Phase3CM admission.

    No label sidecar path is accepted.  Missing fields or unsupported frozen
    replay operators become ``BEHAVIOR_UNRESOLVED`` and never fall back to a
    structural identity.
    """

    from our_system_phase2.services.phase3cm_streaming_expression import (
        StreamingExpressionExecutor,
    )

    rows = [dict(row) for row in candidates]
    if len(rows) % 2:
        raise ValueError("behavior probe requires whole primary/control pairs")
    paths = tuple(Path(path) for path in field_sidecars)
    if not paths or not eligible_trade_dates:
        raise ValueError("behavior probe requires field sidecars and a development calendar")
    schema = set(pl.scan_parquet(paths[0]).collect_schema().names())
    supported_pairs: list[tuple[dict[str, Any], dict[str, Any]]] = []
    unresolved: list[dict[str, Any]] = []
    required_fields: set[str] = {"close"}
    for index in range(0, len(rows), 2):
        primary, control = rows[index], rows[index + 1]
        if str(primary.get("pair_id") or "") != str(control.get("pair_id") or ""):
            raise ValueError("behavior probe pair ordering drift")
        fields = set(_FIELD_PATTERN.findall(str(primary.get("expression") or ""))) | set(
            _FIELD_PATTERN.findall(str(control.get("expression") or ""))
        )
        unsupported_operator = any(
            token in (str(primary.get("expression") or "") + str(control.get("expression") or ""))
            for token in ("FrozenMechanismReplay(", "MatchedControlReplay(")
        )
        missing = sorted(fields - schema)
        if missing or unsupported_operator:
            unresolved.append(
                {
                    "behavior_identity_version": BEHAVIOR_VERSION,
                    "batch_id": batch_id,
                    "pair_id": str(primary.get("pair_id") or ""),
                    "route_id": str(primary.get("route_id") or ""),
                    "primary_candidate_id": str(primary.get("candidate_id") or ""),
                    "control_candidate_id": str(control.get("candidate_id") or ""),
                    "structural_family_id": _structural_family_id(primary),
                    "signal_cluster_id": "",
                    "behavior_probe_id": "",
                    "portfolio_behavior_signature_id": "",
                    "portfolio_behavior_family_id": "",
                    "behavior_status": BEHAVIOR_UNRESOLVED,
                    "behavior_unresolved_reason": (
                        "UNSUPPORTED_FROZEN_REPLAY_OPERATOR"
                        if unsupported_operator
                        else "MISSING_FIELD_SIDECAR:" + "|".join(missing)
                    ),
                    "coordinate_binding": coordinate_binding,
                }
            )
            continue
        required_fields.update(fields)
        supported_pairs.append((primary, control))

    probe_date = str(sorted(map(str, eligible_trade_dates))[0])
    start = np.datetime64(probe_date)
    end = start + np.timedelta64(1, "D")
    condition = (pl.col("trade_time") >= pl.lit(start)) & (pl.col("trade_time") < pl.lit(end))
    time_frame = pl.concat(
        [
            pl.scan_parquet(path, rechunk=False, low_memory=True)
            .filter(condition)
            .select("trade_time")
            for path in paths
        ],
        how="vertical_relaxed",
        rechunk=False,
    ).select(pl.col("trade_time").unique().sort().head(max(1, int(max_trade_times))))
    probe_times = time_frame.collect(engine="streaming")["trade_time"].to_list()
    if not probe_times:
        raise RuntimeError("bounded behavior probe found no development coordinates")
    probe_condition = (pl.col("trade_time") >= pl.lit(probe_times[0])) & (
        pl.col("trade_time") <= pl.lit(probe_times[-1])
    )
    frame = pl.concat(
        [
            pl.scan_parquet(path, rechunk=False, low_memory=True)
            .filter(probe_condition)
            .select(*STABLE_KEY, *sorted(required_fields))
            .collect(engine="streaming")
            for path in paths
        ],
        how="vertical_relaxed",
        rechunk=False,
    ).sort(list(STABLE_KEY), maintain_order=True)
    times_ns = frame["trade_time"].to_numpy().astype("datetime64[ns]").astype(np.int64)
    _, time_ids = np.unique(times_ns, return_inverse=True)
    codes = frame["code"].cast(pl.String).to_list()
    symbols = tuple(sorted(set(codes)))
    symbol_to_id = {symbol: index for index, symbol in enumerate(symbols)}
    code_ids = np.fromiter((symbol_to_id[code] for code in codes), dtype=np.int32, count=len(codes))
    raw_fields = {
        field: frame[field].cast(pl.Float64, strict=False).to_numpy()
        for field in sorted(required_fields)
    }
    output = list(unresolved)
    materialized_pairs = 0
    for start_index in range(0, len(supported_pairs), max(1, int(pair_batch_size))):
        pair_batch = supported_pairs[start_index : start_index + max(1, int(pair_batch_size))]
        members = [member for pair in pair_batch for member in pair]
        executor = StreamingExpressionExecutor(
            code_count=len(symbols),
            compute_threads=int(compute_threads),
            cache_max_bytes=4 * 1024**3,
        ).bind_block(raw_fields=raw_fields, code_ids=code_ids, time_ids=time_ids.astype(np.int64))
        signals = executor.evaluate_ordered_into(str(member["expression"]) for member in members)
        for pair_index in range(len(pair_batch)):
            common = np.isfinite(signals[2 * pair_index]) & np.isfinite(signals[2 * pair_index + 1])
            signals[2 * pair_index, ~common] = np.nan
            signals[2 * pair_index + 1, ~common] = np.nan
        accumulator = StreamingLabelFreeBehavior(
            candidate_ids=tuple(str(member["candidate_id"]) for member in members),
            code_count=len(symbols),
            coordinate_binding=coordinate_binding,
            scope="probe",
            min_obs=int(min_obs),
            top_quantile=float(top_quantile),
        )
        accumulator.update_block(
            signals=signals,
            time_ids=time_ids.astype(np.int64),
            code_ids=code_ids,
            trade_times_ns=times_ns,
            directions=np.ones(len(members), dtype=np.float64),
        )
        behavior_by_candidate = {
            str(row["candidate_id"]): row for row in accumulator.rows()
        }
        for pair_index, (primary, control) in enumerate(pair_batch):
            record = pair_behavior_record(
                batch_id=batch_id,
                pair_id=str(primary["pair_id"]),
                route_id=str(primary["route_id"]),
                primary_candidate_id=str(primary["candidate_id"]),
                control_candidate_id=str(control["candidate_id"]),
                structural_family_id=_structural_family_id(primary),
                signal_cluster_id=_signal_cluster_id(signals[2 * pair_index], time_ids),
                primary_behavior=behavior_by_candidate[str(primary["candidate_id"])],
                control_behavior=behavior_by_candidate[str(control["candidate_id"])],
            )
            record["behavior_unresolved_reason"] = (
                "" if record["behavior_status"] == "RESOLVED" else "INSUFFICIENT_FINITE_PROBE_SUPPORT"
            )
            output.append(record)
            materialized_pairs += 1
        del signals
    order = {str(rows[index].get("pair_id") or ""): index // 2 for index in range(0, len(rows), 2)}
    output.sort(key=lambda row: order[str(row["pair_id"])])
    audit = {
        "probe_scope": "LABEL_FREE_BOUNDED_MAPPING",
        "probe_date": probe_date,
        "trade_time_count": len(probe_times),
        "coordinate_rows": frame.height,
        "candidate_pair_count": len(rows) // 2,
        "materialized_pair_count": materialized_pairs,
        "unresolved_pair_count": sum(1 for row in output if row["behavior_status"] != "RESOLVED"),
        "label_sidecar_paths_accepted": 0,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }
    return output, audit
