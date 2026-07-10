"""Numerical signal diagnostics and equivalence control for pre-CM search."""

from __future__ import annotations

import base64
import hashlib
import math
import zlib
from collections import defaultdict
from typing import Any, Iterable

import numpy as np


_INVALID_RANK = np.int16(-32768)
_RANK_SCALE = 10_000.0


def _float(value: Any, default: float = float("nan")) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def _quantized_ranks(values: np.ndarray) -> np.ndarray:
    ranks = np.asarray(values, dtype=float)
    valid = np.isfinite(ranks)
    out = np.full(len(ranks), _INVALID_RANK, dtype=np.int16)
    if bool(valid.any()):
        scaled = np.rint(np.clip(ranks[valid], -3.0, 3.0) * _RANK_SCALE)
        out[valid] = scaled.astype(np.int16)
    return out


def _hash_bytes(payload: bytes) -> str:
    return hashlib.blake2b(payload, digest_size=16).hexdigest()


def _encode_sketch(values: np.ndarray) -> str:
    payload = np.asarray(values, dtype="<i2").tobytes(order="C")
    return base64.b64encode(zlib.compress(payload, level=6)).decode("ascii")


def decode_rank_sketch(payload: str) -> np.ndarray:
    if not payload:
        return np.asarray([], dtype=float)
    raw = zlib.decompress(base64.b64decode(payload.encode("ascii")))
    quantized = np.frombuffer(raw, dtype="<i2")
    out = quantized.astype(float)
    invalid = quantized == _INVALID_RANK
    out[invalid] = np.nan
    out[~invalid] /= _RANK_SCALE
    return out


def build_signal_semantic_diagnostics(
    signal: Iterable[float] | np.ndarray,
    signal_rank: Iterable[float] | np.ndarray,
    *,
    sketch_size: int = 512,
) -> dict[str, Any]:
    signal_arr = np.asarray(signal, dtype=float)
    rank_arr = np.asarray(signal_rank, dtype=float)
    if len(signal_arr) != len(rank_arr):
        raise ValueError("signal and signal_rank must have equal lengths")

    finite = np.isfinite(signal_arr)
    finite_values = signal_arr[finite]
    finite_count = int(finite.sum())
    unique_count = int(len(np.unique(finite_values))) if finite_count else 0
    signal_std = float(np.std(finite_values)) if finite_count else float("nan")
    is_constant = finite_count > 0 and (unique_count <= 1 or not math.isfinite(signal_std) or signal_std <= 1e-12)

    abs_values = np.abs(finite_values)
    if len(abs_values):
        p99 = float(np.quantile(abs_values, 0.99))
        p999 = float(np.quantile(abs_values, 0.999))
        abs_max = float(np.max(abs_values))
        top_count = max(1, int(math.ceil(len(abs_values) * 0.01)))
        top_share = float(np.partition(abs_values, len(abs_values) - top_count)[-top_count:].sum()) / max(
            float(abs_values.sum()),
            1e-30,
        )
    else:
        p99 = p999 = abs_max = top_share = float("nan")
    tail_ratio = p999 / max(p99, 1e-30) if math.isfinite(p999) and math.isfinite(p99) else float("nan")
    tail_flag = bool(
        (math.isfinite(top_share) and top_share >= 0.50)
        or (math.isfinite(tail_ratio) and tail_ratio >= 25.0)
    )

    quantized = _quantized_ranks(rank_arr)
    valid_rank = quantized != _INVALID_RANK
    mask_payload = np.packbits(valid_rank, bitorder="little").tobytes()
    rank_hash = _hash_bytes(quantized.astype("<i2", copy=False).tobytes(order="C"))
    mask_hash = _hash_bytes(mask_payload)

    target_size = max(1, min(len(rank_arr), int(sketch_size))) if len(rank_arr) else 0
    if target_size:
        positions = np.linspace(0, len(rank_arr) - 1, num=target_size, dtype=np.int64)
        sketch = quantized[positions]
    else:
        sketch = np.asarray([], dtype=np.int16)

    return {
        "signal_total_count": int(len(signal_arr)),
        "signal_finite_count": finite_count,
        "signal_finite_ratio": round(finite_count / max(1, len(signal_arr)), 8),
        "signal_unique_count": unique_count,
        "signal_std": round(signal_std, 10) if math.isfinite(signal_std) else "",
        "signal_is_constant": bool(is_constant),
        "signal_abs_p99": round(p99, 10) if math.isfinite(p99) else "",
        "signal_abs_p999": round(p999, 10) if math.isfinite(p999) else "",
        "signal_abs_max": round(abs_max, 10) if math.isfinite(abs_max) else "",
        "signal_p999_to_p99": round(tail_ratio, 8) if math.isfinite(tail_ratio) else "",
        "signal_top1pct_abs_share": round(top_share, 8) if math.isfinite(top_share) else "",
        "signal_tail_concentration_flag": bool(tail_flag),
        "signal_rank_hash": rank_hash,
        "signal_valid_mask_hash": mask_hash,
        "signal_rank_sketch": _encode_sketch(sketch),
        "signal_rank_sketch_size": int(len(sketch)),
    }


def _truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _candidate_vector(rows: list[dict[str, Any]]) -> tuple[np.ndarray, str, bool, dict[str, Any]]:
    ordered = sorted(rows, key=lambda row: (int(_float(row.get("shard_index"), 0.0)), int(_float(row.get("sample_block_index"), 0.0))))
    sketches = [decode_rank_sketch(str(row.get("signal_rank_sketch") or "")) for row in ordered]
    sketches = [sketch for sketch in sketches if len(sketch)]
    vector = np.concatenate(sketches) if sketches else np.asarray([], dtype=float)
    signature_source = "|".join(
        f"{row.get('shard_index')}:{row.get('sample_block_index')}:{row.get('signal_rank_hash')}:{row.get('signal_valid_mask_hash')}"
        for row in ordered
    )
    signature = hashlib.sha1(signature_source.encode("utf-8")).hexdigest() if signature_source else ""
    all_constant = bool(ordered) and all(_truthy(row.get("signal_is_constant")) for row in ordered)
    summary = {
        "signal_checked_shards": len(ordered),
        "signal_nonzero_shards": sum(
            1 for row in ordered if int(_float(row.get("signal_finite_count"), 0.0)) > 0
        ),
        "signal_finite_count": sum(int(_float(row.get("signal_finite_count"), 0.0)) for row in ordered),
        "signal_unique_count_max": max((int(_float(row.get("signal_unique_count"), 0.0)) for row in ordered), default=0),
        "signal_tail_concentration_flag": any(_truthy(row.get("signal_tail_concentration_flag")) for row in ordered),
        "signal_top1pct_abs_share_max": max((_float(row.get("signal_top1pct_abs_share"), 0.0) for row in ordered), default=0.0),
        "division_floor_hit_ratio_max": max((_float(row.get("division_floor_hit_ratio_max"), 0.0) for row in ordered), default=0.0),
        "division_min_abs": min(
            (value for row in ordered for value in [_float(row.get("division_min_abs"))] if math.isfinite(value)),
            default=float("nan"),
        ),
    }
    return vector, signature, all_constant, summary


def _mask_jaccard(left: np.ndarray, right: np.ndarray) -> float:
    left_valid = np.isfinite(left)
    right_valid = np.isfinite(right)
    union = int((left_valid | right_valid).sum())
    if union == 0:
        return 0.0
    return float((left_valid & right_valid).sum()) / union


def _rank_correlation(left: np.ndarray, right: np.ndarray) -> float:
    valid = np.isfinite(left) & np.isfinite(right)
    if int(valid.sum()) < 3:
        return float("nan")
    x = left[valid]
    y = right[valid]
    x = x - float(x.mean())
    y = y - float(y.mean())
    denominator = float(np.sqrt(np.dot(x, x) * np.dot(y, y)))
    if denominator <= 0.0 or not math.isfinite(denominator):
        return float("nan")
    return float(np.dot(x, y) / denominator)


def _set_jaccard(left: np.ndarray, right: np.ndarray) -> float:
    union = int((left | right).sum())
    if union == 0:
        return 0.0
    return float((left & right).sum()) / union


def _portfolio_bucket_overlap(left: np.ndarray, right: np.ndarray, *, quantile: float) -> float:
    valid = np.isfinite(left) & np.isfinite(right)
    if int(valid.sum()) < 5:
        return float("nan")
    x = left[valid]
    y = right[valid]
    q = max(0.01, min(0.49, float(quantile)))
    x_low, x_high = np.quantile(x, [q, 1.0 - q])
    y_low, y_high = np.quantile(y, [q, 1.0 - q])
    x_bottom, x_top = x <= x_low, x >= x_high
    y_bottom, y_top = y <= y_low, y >= y_high
    same_direction = 0.5 * (_set_jaccard(x_top, y_top) + _set_jaccard(x_bottom, y_bottom))
    opposite_direction = 0.5 * (_set_jaccard(x_top, y_bottom) + _set_jaccard(x_bottom, y_top))
    return max(same_direction, opposite_direction)


def classify_candidate_signal_semantics(
    candidates: list[dict[str, Any]],
    progress_rows: list[dict[str, Any]],
    *,
    correlation_threshold: float = 0.9995,
    min_mask_jaccard: float = 0.98,
    position_overlap_threshold: float = 0.995,
    position_quantile: float = 0.20,
) -> dict[str, dict[str, Any]]:
    by_candidate: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in progress_rows:
        candidate_id = str(row.get("candidate_id") or "")
        if candidate_id:
            by_candidate[candidate_id].append(row)

    decisions: dict[str, dict[str, Any]] = {}
    kept: list[tuple[str, np.ndarray, str]] = []
    signature_owner: dict[str, str] = {}
    threshold = max(0.0, min(1.0, float(correlation_threshold)))
    mask_threshold = max(0.0, min(1.0, float(min_mask_jaccard)))
    overlap_threshold = max(0.0, min(1.0, float(position_overlap_threshold)))

    for candidate in candidates:
        candidate_id = str(candidate.get("candidate_id") or "")
        rows = by_candidate.get(candidate_id, [])
        vector, signature, all_constant, summary = _candidate_vector(rows)
        decision = {
            **summary,
            "signal_semantic_decision": "PASS",
            "signal_semantic_reasons": "",
            "signal_equivalent_to": "",
            "signal_equivalence_correlation": "",
            "signal_equivalence_mask_jaccard": "",
            "signal_equivalence_position_overlap": "",
        }
        if not rows or not len(vector):
            decision["signal_semantic_decision"] = "REJECT_MISSING_SIGNAL_DIAGNOSTICS"
            decision["signal_semantic_reasons"] = "missing_rank_sketch"
            decisions[candidate_id] = decision
            continue
        if all_constant:
            decision["signal_semantic_decision"] = "REJECT_CONSTANT_SIGNAL"
            decision["signal_semantic_reasons"] = "constant_on_all_checked_shards"
            decisions[candidate_id] = decision
            continue
        owner = signature_owner.get(signature) if signature else None
        if owner:
            decision["signal_semantic_decision"] = "REJECT_SIGNAL_EQUIVALENT"
            decision["signal_semantic_reasons"] = "exact_quantized_rank_and_mask_match"
            decision["signal_equivalent_to"] = owner
            decision["signal_equivalence_correlation"] = 1.0
            decision["signal_equivalence_mask_jaccard"] = 1.0
            decision["signal_equivalence_position_overlap"] = 1.0
            decisions[candidate_id] = decision
            continue

        equivalent_to = ""
        equivalent_corr = float("nan")
        equivalent_mask = float("nan")
        equivalent_overlap = float("nan")
        equivalent_reason = ""
        for kept_id, kept_vector, _kept_signature in kept:
            if len(kept_vector) != len(vector):
                continue
            mask_jaccard = _mask_jaccard(vector, kept_vector)
            if mask_jaccard < mask_threshold:
                continue
            correlation = _rank_correlation(vector, kept_vector)
            position_overlap = _portfolio_bucket_overlap(
                vector,
                kept_vector,
                quantile=position_quantile,
            )
            if math.isfinite(correlation) and abs(correlation) >= threshold:
                equivalent_to = kept_id
                equivalent_corr = correlation
                equivalent_mask = mask_jaccard
                equivalent_overlap = position_overlap
                equivalent_reason = "near_identical_rank_vector"
                break
            if math.isfinite(position_overlap) and position_overlap >= overlap_threshold:
                equivalent_to = kept_id
                equivalent_corr = correlation
                equivalent_mask = mask_jaccard
                equivalent_overlap = position_overlap
                equivalent_reason = "position_equivalent_portfolio_buckets"
                break
        if equivalent_to:
            decision["signal_semantic_decision"] = "REJECT_SIGNAL_EQUIVALENT"
            decision["signal_semantic_reasons"] = equivalent_reason
            decision["signal_equivalent_to"] = equivalent_to
            decision["signal_equivalence_correlation"] = (
                round(equivalent_corr, 8) if math.isfinite(equivalent_corr) else ""
            )
            decision["signal_equivalence_mask_jaccard"] = round(equivalent_mask, 8)
            decision["signal_equivalence_position_overlap"] = (
                round(equivalent_overlap, 8) if math.isfinite(equivalent_overlap) else ""
            )
        else:
            kept.append((candidate_id, vector, signature))
            if signature:
                signature_owner[signature] = candidate_id
        decisions[candidate_id] = decision
    return decisions
