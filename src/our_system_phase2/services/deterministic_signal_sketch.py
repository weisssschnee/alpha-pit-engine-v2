"""Deterministic, label-free signal sketches for EVALRESET collapse forensics."""

from __future__ import annotations

import base64
import hashlib
import json
import math
import zlib
from collections import Counter, defaultdict
from functools import lru_cache
from typing import Any, Iterable

import numpy as np


SKETCH_VERSION = "evalreset_signal_sketch_v1"
INVALID_I8 = np.int8(-128)


def _encode_i8(values: np.ndarray) -> str:
    raw = np.asarray(values, dtype=np.int8).tobytes(order="C")
    return base64.b64encode(zlib.compress(raw, level=6)).decode("ascii")


def decode_i8(payload: str) -> np.ndarray:
    if not payload:
        return np.asarray([], dtype=np.int8)
    return np.frombuffer(zlib.decompress(base64.b64decode(payload)), dtype=np.int8).copy()


def _encode_bits(values: np.ndarray) -> str:
    raw = np.packbits(np.asarray(values, dtype=bool), bitorder="little").tobytes()
    return base64.b64encode(zlib.compress(raw, level=6)).decode("ascii")


def decode_bits(payload: str, size: int) -> np.ndarray:
    if not payload:
        return np.zeros(size, dtype=bool)
    raw = zlib.decompress(base64.b64decode(payload))
    return np.unpackbits(np.frombuffer(raw, dtype=np.uint8), bitorder="little")[:size].astype(bool)


def _hash_bytes(payload: bytes, *, size: int = 16) -> str:
    return hashlib.blake2b(payload, digest_size=size).hexdigest()


def projection_matrix(coordinate_ids: Iterable[str], *, seed: int, bits: int = 64) -> np.ndarray:
    """Build a stable sparse +/-1 SimHash projection independent of labels."""

    ids = list(coordinate_ids)
    out = np.empty((len(ids), bits), dtype=np.int8)
    for row, coordinate_id in enumerate(ids):
        digest = hashlib.shake_256(f"{SKETCH_VERSION}|{seed}|{coordinate_id}".encode()).digest(bits)
        byte = np.frombuffer(digest, dtype=np.uint8)
        out[row] = np.where((byte & 3) == 0, 0, np.where((byte & 1) == 0, -1, 1))
    return out


def _quantize_rank(rank: np.ndarray) -> np.ndarray:
    valid = np.isfinite(rank)
    out = np.full(len(rank), INVALID_I8, dtype=np.int8)
    out[valid] = np.rint(np.clip(rank[valid], 0.0, 1.0) * 126.0).astype(np.int8)
    return out


def _quantize_value(values: np.ndarray) -> np.ndarray:
    valid = np.isfinite(values)
    out = np.full(len(values), INVALID_I8, dtype=np.int8)
    if not bool(valid.any()):
        return out
    finite = values[valid]
    median = float(np.median(finite))
    mad = float(np.median(np.abs(finite - median)))
    scale = max(1e-12, 1.4826 * mad, float(np.std(finite)) * 0.10)
    normalized = np.clip((finite - median) / scale, -7.875, 7.875)
    out[valid] = np.rint(normalized * 16.0).astype(np.int8)
    return out


def _profile(valid: np.ndarray, groups: list[str]) -> str:
    totals: Counter[str] = Counter(groups)
    finite: Counter[str] = Counter(group for group, ok in zip(groups, valid, strict=True) if ok)
    return json.dumps(
        {key: round(finite[key] / max(1, totals[key]), 6) for key in sorted(totals)},
        sort_keys=True,
        separators=(",", ":"),
    )


def build_signal_sketch(
    values: np.ndarray,
    ranks: np.ndarray,
    coordinate_rows: list[dict[str, Any]],
    *,
    coordinate_set: str,
    projection: np.ndarray,
) -> dict[str, Any]:
    values = np.asarray(values, dtype=float)
    ranks = np.asarray(ranks, dtype=float)
    if len(values) != len(ranks) or len(values) != len(coordinate_rows):
        raise ValueError("values, ranks, and coordinates must have equal lengths")
    if projection.shape[0] != len(values):
        raise ValueError("projection row count must match coordinates")

    valid = np.isfinite(values) & np.isfinite(ranks)
    activation = valid & (np.abs(values) > 1e-12)
    rank_q = _quantize_rank(ranks)
    value_q = _quantize_value(values)
    sign = np.full(len(values), INVALID_I8, dtype=np.int8)
    sign[valid] = np.sign(values[valid]).astype(np.int8)
    centered_rank = np.where(valid, ranks - 0.5, 0.0)
    projection_score = centered_rank @ projection
    simhash_bits = projection_score >= 0.0
    simhash_hex = np.packbits(simhash_bits, bitorder="little").tobytes().hex()
    exact_payload = b"|".join(
        [rank_q.tobytes(), value_q.tobytes(), np.packbits(valid, bitorder="little").tobytes()]
    )

    def groups(name: str) -> list[str]:
        return [str(row.get(name) or "unknown") for row in coordinate_rows]

    return {
        "sketch_version": SKETCH_VERSION,
        "coordinate_set": coordinate_set,
        "coordinate_count": len(values),
        "finite_count": int(valid.sum()),
        "finite_ratio": round(float(valid.mean()) if len(valid) else 0.0, 8),
        "activation_count": int(activation.sum()),
        "activation_ratio": round(float(activation.mean()) if len(activation) else 0.0, 8),
        "activation_bitmap_sketch": _encode_bits(activation),
        "missingness_pattern": _encode_bits(~valid),
        "rank_quantized_sketch": _encode_i8(rank_q),
        "sign_sketch": _encode_i8(sign),
        "value_quantized_sketch": _encode_i8(value_q),
        "simhash": simhash_hex,
        "exact_sketch_hash": _hash_bytes(exact_payload),
        "activation_hash": _hash_bytes(np.packbits(activation, bitorder="little").tobytes()),
        "missingness_hash": _hash_bytes(np.packbits(~valid, bitorder="little").tobytes()),
        "coverage_by_month": _profile(valid, groups("trade_month")),
        "coverage_by_intraday_period": _profile(valid, groups("intraday_period")),
        "coverage_by_stock_interval": _profile(valid, groups("stock_coverage_interval")),
        "coverage_by_listing_age": _profile(valid, groups("listing_age_bucket")),
        "coverage_by_activation_density": _profile(valid, groups("activation_density_bucket")),
    }


def _decode_similarity_inputs(
    row: dict[str, Any],
) -> tuple[int, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    size = int(row.get("coordinate_count") or 0)
    return (
        size,
        decode_i8(str(row.get("rank_quantized_sketch") or "")),
        decode_i8(str(row.get("value_quantized_sketch") or "")),
        decode_bits(str(row.get("missingness_pattern") or ""), size),
        decode_bits(str(row.get("activation_bitmap_sketch") or ""), size),
    )


def _decoded_sketch_similarity(
    left: tuple[int, np.ndarray, np.ndarray, np.ndarray, np.ndarray],
    right: tuple[int, np.ndarray, np.ndarray, np.ndarray, np.ndarray],
) -> dict[str, float]:
    size, lrank, lval, lmiss, lact = left
    right_size, rrank, rval, rmiss, ract = right
    if size != right_size:
        return {"rank_corr": float("nan"), "value_corr": float("nan"), "mask_jaccard": 0.0, "activation_jaccard": 0.0}
    valid = ~lmiss & ~rmiss

    def corr(x: np.ndarray, y: np.ndarray) -> float:
        if int(valid.sum()) < 3:
            return float("nan")
        xv, yv = x[valid], y[valid]
        if float(np.std(xv)) <= 0.0 or float(np.std(yv)) <= 0.0:
            return float("nan")
        return float(np.corrcoef(xv, yv)[0, 1])

    def jaccard(x: np.ndarray, y: np.ndarray) -> float:
        union = int((x | y).sum())
        return float((x & y).sum()) / union if union else 1.0

    return {
        "rank_corr": corr(lrank, rrank),
        "value_corr": corr(lval, rval),
        "mask_jaccard": jaccard(~lmiss, ~rmiss),
        "activation_jaccard": jaccard(lact, ract),
    }


def sketch_similarity(left: dict[str, Any], right: dict[str, Any]) -> dict[str, float]:
    return _decoded_sketch_similarity(
        _decode_similarity_inputs(left),
        _decode_similarity_inputs(right),
    )


def _simhash_bands(value: str, *, bands: int = 8, complement: bool = False) -> list[str]:
    raw = bytes.fromhex(value)
    if not raw:
        return []
    if complement:
        raw = bytes((~byte) & 0xFF for byte in raw)
    width = max(1, len(raw) // bands)
    return [f"{index}:{raw[index * width:(index + 1) * width].hex()}" for index in range(bands)]


def cluster_sketches(
    rows: list[dict[str, Any]],
    *,
    rank_threshold: float = 0.95,
    value_threshold: float = 0.90,
    mask_threshold: float = 0.90,
    activation_threshold: float = 0.85,
    max_bucket_representatives: int = 32,
) -> dict[str, int]:
    """Scalable deterministic LSH + representative clustering (no labels/reward)."""

    ordered = sorted(rows, key=lambda row: str(row.get("candidate_id") or ""))
    owners: list[int] = []
    members: dict[int, list[int]] = defaultdict(list)
    exact_owner: dict[str, int] = {}
    band_representatives: dict[str, list[int]] = defaultdict(list)
    labels: dict[str, int] = {}
    owner_labels: dict[int, int] = {}

    # LSH owners are compared repeatedly. Decoding their compressed sketches on
    # every comparison dominates runtime while adding no information. Keep a
    # bounded cache so the clustering decision remains byte-for-byte equivalent
    # without materializing every decoded candidate at once.
    @lru_cache(maxsize=4096)
    def decoded(index: int) -> tuple[int, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        return _decode_similarity_inputs(ordered[index])

    for index, row in enumerate(ordered):
        candidate_id = str(row.get("candidate_id") or "")
        exact = str(row.get("exact_sketch_hash") or "")
        owner = exact_owner.get(exact) if exact else None
        candidate_owners: list[int] = []
        if owner is None:
            for band in _simhash_bands(str(row.get("simhash") or "")):
                candidate_owners.extend(band_representatives.get(band, ()))
            for band in _simhash_bands(str(row.get("simhash") or ""), complement=True):
                candidate_owners.extend(band_representatives.get(band, ()))
            decoded_row = decoded(index) if candidate_owners else None
            for possible in sorted(set(candidate_owners)):
                assert decoded_row is not None
                similarity = _decoded_sketch_similarity(decoded_row, decoded(possible))
                rank_corr = similarity["rank_corr"]
                value_corr = similarity["value_corr"]
                if (
                    math.isfinite(rank_corr)
                    and abs(rank_corr) >= rank_threshold
                    and (not math.isfinite(value_corr) or abs(value_corr) >= value_threshold)
                    and similarity["mask_jaccard"] >= mask_threshold
                    and similarity["activation_jaccard"] >= activation_threshold
                ):
                    owner = possible
                    break
        if owner is None:
            owner = index
            owners.append(index)
            owner_labels[owner] = len(owners)
        members[owner].append(index)
        labels[candidate_id] = owner_labels[owner]
        if exact:
            exact_owner.setdefault(exact, owner)
        for band in _simhash_bands(str(row.get("simhash") or "")):
            reps = band_representatives[band]
            if owner not in reps and len(reps) < max_bucket_representatives:
                reps.append(owner)
    return labels


def cluster_distribution(labels: Iterable[int]) -> dict[str, float | int]:
    counts = np.asarray(list(Counter(labels).values()), dtype=float)
    if not len(counts):
        return {"cluster_count": 0, "n_eff": 0.0, "top1_share": 0.0, "top3_share": 0.0}
    shares = counts / counts.sum()
    return {
        "cluster_count": int(len(counts)),
        "n_eff": round(float(1.0 / np.square(shares).sum()), 8),
        "top1_share": round(float(np.max(shares)), 8),
        "top3_share": round(float(np.sort(shares)[-3:].sum()), 8),
    }
