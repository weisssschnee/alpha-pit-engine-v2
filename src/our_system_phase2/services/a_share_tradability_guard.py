"""Fail-closed A-share tradability evidence boundary.

Phase3CM is a development predictive/ranking evaluator. Its scores must not
become optimizer feedback, finalist evidence, or economic claims unless a
separate replay has explicitly proved the execution rules below.
"""

from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
from typing import Any, Mapping


DEVELOPMENT_PREDICTIVE_EVIDENCE_CLASS = "DEVELOPMENT_PREDICTIVE_SCORE_ONLY"
A_SHARE_TRADABILITY_EVIDENCE_CLASS = "A_SHARE_TRADABILITY_REPLAY_V1"
A_SHARE_TRADABILITY_READY = "A_SHARE_TRADABILITY_READY"
A_SHARE_TRADABILITY_UNPROVEN = "A_SHARE_TRADABILITY_UNPROVEN"
A_SHARE_EXECUTABLE_REWARD_READY = "A_SHARE_EXECUTABLE_REWARD_READY"
A_SHARE_REPLAY_RECEIPT_SCHEMA_VERSION = "a_share_tradability_replay_receipt_v1"
A_SHARE_EXECUTABLE_REWARD_METRIC = "EXECUTABLE_NET_DAILY_SORTINO_V1"
A_SHARE_EXECUTABLE_REWARD_SOURCE = (
    "A_SHARE_TRADABILITY_REPLAY_V1_EXECUTABLE_NET_REWARD"
)

REQUIRED_TRADABILITY_PROOFS = (
    "execution_clock_enforced",
    "same_bar_execution_excluded",
    "t_plus_one_enforced",
    "limit_lock_fill_enforced",
    "suspension_fill_enforced",
    "full_fee_schedule_enforced",
    "promotion_grade_universe_enforced",
)

REQUIRED_REPLAY_BINDINGS = (
    "candidate_id",
    "candidate_exact_identity",
    "replay_code_sha256",
    "input_data_sha256",
    "universe_manifest_sha256",
    "fee_schedule_sha256",
    "execution_policy_sha256",
)


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _canonical_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def _is_sha256(value: Any) -> bool:
    text = str(value or "")
    return len(text) == 64 and all(char in "0123456789abcdef" for char in text)


def replay_receipt_payload_hash(row: Mapping[str, Any]) -> str:
    """Hash a replay receipt while excluding its self-hash."""

    canonical = row.get("replay_receipt_canonical_json")
    if isinstance(canonical, str) and canonical:
        parsed = json.loads(canonical)
        return _canonical_hash(parsed)
    payload = dict(row)
    payload.pop("replay_receipt_payload_sha256", None)
    payload.pop("replay_receipt_canonical_json", None)
    return _canonical_hash(payload)


def _receipt_evidence(row: Mapping[str, Any]) -> Mapping[str, Any]:
    canonical = row.get("replay_receipt_canonical_json")
    if not isinstance(canonical, str) or not canonical:
        return row
    parsed = json.loads(canonical)
    if not isinstance(parsed, Mapping):
        raise ValueError("replay receipt canonical payload must be an object")
    return parsed


def build_a_share_tradability_receipt(
    *,
    candidate_id: str,
    candidate_exact_identity: str,
    replay_code_sha256: str,
    input_data_sha256: str,
    universe_manifest_sha256: str,
    fee_schedule_sha256: str,
    execution_policy_sha256: str,
    executable_net_reward: float,
    train_read_count: int,
    trade_count: int,
    fill_count: int,
    blocked_buy_count: int,
    blocked_sell_count: int,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the only receipt shape allowed to authorize optimizer feedback.

    Callers must derive every supplied hash from frozen inputs. This helper
    deliberately does not infer or weaken any execution proof.
    """

    reward = _finite(executable_net_reward)
    if reward is None:
        raise ValueError("executable_net_reward must be finite")
    payload: dict[str, Any] = {
        "replay_receipt_schema_version": A_SHARE_REPLAY_RECEIPT_SCHEMA_VERSION,
        "evaluation_evidence_class": A_SHARE_TRADABILITY_EVIDENCE_CLASS,
        "a_share_tradability_decision": A_SHARE_TRADABILITY_READY,
        **{field: True for field in REQUIRED_TRADABILITY_PROOFS},
        "candidate_id": str(candidate_id),
        "candidate_exact_identity": str(candidate_exact_identity),
        "replay_code_sha256": str(replay_code_sha256),
        "input_data_sha256": str(input_data_sha256),
        "universe_manifest_sha256": str(universe_manifest_sha256),
        "fee_schedule_sha256": str(fee_schedule_sha256),
        "execution_policy_sha256": str(execution_policy_sha256),
        "a_share_executable_net_reward": reward,
        "a_share_reward_metric": A_SHARE_EXECUTABLE_REWARD_METRIC,
        "a_share_reward_source": A_SHARE_EXECUTABLE_REWARD_SOURCE,
        "a_share_reward_split": "train",
        "data_access_counts": {
            "train": int(train_read_count),
            "validation": 0,
            "holdout": 0,
            "forward_2026": 0,
        },
        "trade_count": int(trade_count),
        "fill_count": int(fill_count),
        "blocked_buy_count": int(blocked_buy_count),
        "blocked_sell_count": int(blocked_sell_count),
        "economic_claim_authorized": False,
        "candidate_promotion_authorized": False,
    }
    if extra:
        reserved = set(payload) | {"replay_receipt_payload_sha256"}
        overlap = sorted(reserved.intersection(extra))
        if overlap:
            raise ValueError(f"extra replay receipt fields overlap reserved fields: {overlap}")
        payload.update(dict(extra))
    canonical_json = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    payload["replay_receipt_canonical_json"] = canonical_json
    payload["replay_receipt_payload_sha256"] = _canonical_hash(
        json.loads(canonical_json)
    )
    return payload


def read_a_share_tradability_receipts(
    path: Path,
) -> list[dict[str, Any]]:
    """Read an immutable replay receipt table and reject ambiguous identities."""

    target = Path(path)
    if not target.is_file():
        raise FileNotFoundError(
            f"A-share replay receipt table does not exist: {target}"
        )
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for line_number, line in enumerate(
        target.read_text(encoding="utf-8").splitlines(),
        1,
    ):
        if not line.strip():
            continue
        payload = json.loads(line)
        if not isinstance(payload, dict):
            raise ValueError(
                f"A-share replay receipt line {line_number} is not an object"
            )
        candidate_id = str(payload.get("candidate_id") or "")
        if not candidate_id:
            raise ValueError(
                f"A-share replay receipt line {line_number} lacks candidate_id"
            )
        if candidate_id in seen:
            raise ValueError(
                f"duplicate A-share replay receipt candidate_id: {candidate_id}"
            )
        seen.add(candidate_id)
        rows.append(payload)
    if not rows:
        raise ValueError(f"A-share replay receipt table is empty: {target}")
    return rows


def development_predictive_evidence() -> dict[str, Any]:
    """Return the explicit non-authoritative evidence declaration for Phase3CM."""

    return {
        "evaluation_evidence_class": DEVELOPMENT_PREDICTIVE_EVIDENCE_CLASS,
        "a_share_tradability_decision": A_SHARE_TRADABILITY_UNPROVEN,
        **{field: False for field in REQUIRED_TRADABILITY_PROOFS},
        "economic_claim_authorized": False,
        "candidate_promotion_authorized": False,
    }


def a_share_tradability_blockers(
    row: Mapping[str, Any],
    *,
    expected_candidate_id: str | None = None,
    expected_exact_identity: str | None = None,
) -> tuple[str, ...]:
    """Return stable blockers; missing evidence is always blocked."""

    blockers: list[str] = []
    try:
        evidence = _receipt_evidence(row)
    except (json.JSONDecodeError, TypeError, ValueError):
        evidence = row
        blockers.append("replay_receipt_canonical_payload_invalid")
    evidence_class = str(evidence.get("evaluation_evidence_class") or "")
    if evidence_class != A_SHARE_TRADABILITY_EVIDENCE_CLASS:
        blockers.append("a_share_tradability_evidence_class_not_ready")
    decision = str(evidence.get("a_share_tradability_decision") or "")
    if decision != A_SHARE_TRADABILITY_READY:
        blockers.append("a_share_tradability_decision_not_ready")
    blockers.extend(
        f"{field}_not_proven"
        for field in REQUIRED_TRADABILITY_PROOFS
        if not _truthy(evidence.get(field))
    )
    if (
        str(evidence.get("replay_receipt_schema_version") or "")
        != A_SHARE_REPLAY_RECEIPT_SCHEMA_VERSION
    ):
        blockers.append("replay_receipt_schema_version_not_ready")
    blockers.extend(
        f"{field}_missing_or_invalid"
        for field in REQUIRED_REPLAY_BINDINGS
        if (
            not str(evidence.get(field) or "")
            if field in {"candidate_id", "candidate_exact_identity"}
            else not _is_sha256(evidence.get(field))
        )
    )
    if (
        expected_candidate_id is not None
        and str(evidence.get("candidate_id") or "") != str(expected_candidate_id)
    ):
        blockers.append("replay_candidate_id_mismatch")
    if (
        expected_exact_identity is not None
        and str(evidence.get("candidate_exact_identity") or "")
        != str(expected_exact_identity)
    ):
        blockers.append("replay_candidate_exact_identity_mismatch")
    receipt_hash = str(row.get("replay_receipt_payload_sha256") or "")
    if not _is_sha256(receipt_hash):
        blockers.append("replay_receipt_payload_sha256_missing_or_invalid")
    else:
        try:
            expected_hash = replay_receipt_payload_hash(row)
        except (TypeError, ValueError):
            blockers.append("replay_receipt_payload_not_canonical")
        else:
            if receipt_hash != expected_hash:
                blockers.append("replay_receipt_payload_sha256_mismatch")
    if _finite(evidence.get("a_share_executable_net_reward")) is None:
        blockers.append("a_share_executable_net_reward_not_finite")
    if (
        str(evidence.get("a_share_reward_metric") or "")
        != A_SHARE_EXECUTABLE_REWARD_METRIC
    ):
        blockers.append("a_share_reward_metric_mismatch")
    if (
        str(evidence.get("a_share_reward_source") or "")
        != A_SHARE_EXECUTABLE_REWARD_SOURCE
    ):
        blockers.append("a_share_reward_source_mismatch")
    if str(evidence.get("a_share_reward_split") or "").lower() != "train":
        blockers.append("a_share_reward_split_not_train")
    counts = evidence.get("data_access_counts")
    if not isinstance(counts, Mapping):
        blockers.append("data_access_counts_missing")
    else:
        try:
            train_reads = int(counts.get("train", 0))
            sealed_reads = sum(
                int(counts.get(name, 0))
                for name in ("validation", "holdout", "forward_2026")
            )
        except (TypeError, ValueError):
            blockers.append("data_access_counts_invalid")
        else:
            if train_reads <= 0:
                blockers.append("train_read_count_not_positive")
            if sealed_reads != 0:
                blockers.append("sealed_period_reads_nonzero")
    try:
        trade_count = int(evidence.get("trade_count", 0))
        fill_count = int(evidence.get("fill_count", 0))
    except (TypeError, ValueError):
        blockers.append("replay_trade_counts_invalid")
    else:
        if trade_count <= 0 or fill_count <= 0:
            blockers.append("replay_has_no_executable_fills")
    return tuple(blockers)


def a_share_tradability_ready(row: Mapping[str, Any]) -> bool:
    """True only for an explicit replay receipt satisfying every hard proof."""

    return not a_share_tradability_blockers(row)


def prefixed_tradability_evidence(
    row: Mapping[str, Any],
    *,
    prefix: str,
) -> dict[str, Any]:
    """Project comparable evidence fields onto a pair outcome."""

    fields = (
        "evaluation_evidence_class",
        "a_share_tradability_decision",
        *REQUIRED_TRADABILITY_PROOFS,
        "economic_claim_authorized",
        "candidate_promotion_authorized",
        "replay_receipt_schema_version",
        "replay_receipt_canonical_json",
        "replay_receipt_payload_sha256",
        "candidate_id",
        "candidate_exact_identity",
        "a_share_executable_net_reward",
        "a_share_reward_metric",
        "a_share_reward_source",
        "a_share_reward_split",
    )
    return {f"{prefix}{field}": row.get(field) for field in fields}


def prefixed_tradability_blockers(
    row: Mapping[str, Any],
    *,
    prefix: str,
    expected_candidate_id: str | None = None,
    expected_exact_identity: str | None = None,
) -> tuple[str, ...]:
    """Rebuild and verify one canonical member receipt from a pair row."""

    projected = {
        str(key)[len(prefix) :]: value
        for key, value in row.items()
        if str(key).startswith(prefix)
    }
    return a_share_tradability_blockers(
        projected,
        expected_candidate_id=expected_candidate_id,
        expected_exact_identity=expected_exact_identity,
    )


def pair_row_tradability_blockers(
    row: Mapping[str, Any],
) -> tuple[str, ...]:
    """Verify both immutable replay receipts carried by a pair outcome."""

    primary = prefixed_tradability_blockers(
        row,
        prefix="primary_",
        expected_candidate_id=str(row.get("primary_candidate_id") or ""),
    )
    control = prefixed_tradability_blockers(
        row,
        prefix="control_",
        expected_candidate_id=str(row.get("control_candidate_id") or ""),
    )
    return tuple(
        [
            *(f"primary_{value}" for value in primary),
            *(f"control_{value}" for value in control),
        ]
    )


def pair_tradability_evidence(
    primary: Mapping[str, Any],
    control: Mapping[str, Any],
) -> dict[str, Any]:
    """Combine comparable replay evidence; either missing member blocks the pair."""

    ready = a_share_tradability_ready(primary) and a_share_tradability_ready(
        control
    )
    return {
        "evaluation_evidence_class": (
            A_SHARE_TRADABILITY_EVIDENCE_CLASS
            if ready
            else DEVELOPMENT_PREDICTIVE_EVIDENCE_CLASS
        ),
        "a_share_tradability_decision": (
            A_SHARE_TRADABILITY_READY if ready else A_SHARE_TRADABILITY_UNPROVEN
        ),
        **{
            field: _truthy(primary.get(field)) and _truthy(control.get(field))
            for field in REQUIRED_TRADABILITY_PROOFS
        },
        "economic_claim_authorized": False,
        "candidate_promotion_authorized": False,
        "primary_a_share_replay_receipt_sha256": primary.get(
            "replay_receipt_payload_sha256"
        ),
        "control_a_share_replay_receipt_sha256": control.get(
            "replay_receipt_payload_sha256"
        ),
        "primary_a_share_executable_net_reward": primary.get(
            "a_share_executable_net_reward"
        ),
        "control_a_share_executable_net_reward": control.get(
            "a_share_executable_net_reward"
        ),
        "a_share_matched_executable_increment": (
            _finite(primary.get("a_share_executable_net_reward"))
            - _finite(control.get("a_share_executable_net_reward"))
            if _finite(primary.get("a_share_executable_net_reward"))
            is not None
            and _finite(control.get("a_share_executable_net_reward"))
            is not None
            else None
        ),
    }
