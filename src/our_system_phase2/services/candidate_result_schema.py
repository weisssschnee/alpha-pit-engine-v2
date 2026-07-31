"""Stable Parquet envelopes for heterogeneous finalist replay outcomes."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping, Sequence

import pandas as pd


CANDIDATE_RESULT_COLUMNS = (
    "schema_version",
    "candidate_id",
    "pair_id",
    "pair_member_role",
    "route_id",
    "exact_identity",
    "candidate_replay_status",
    "blocker_code",
    "a_share_executable_net_reward",
    "diagnostic_net_reward",
    "train_read_count",
    "trade_count",
    "fill_count",
    "economic_claim_authorized",
    "promotion_authorized",
    "result_payload_sha256",
    "result_payload_json",
)


def _payload_json(row: Mapping[str, Any]) -> str:
    return json.dumps(
        dict(row),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def candidate_result_summary_frame(
    rows: Sequence[Mapping[str, Any]],
) -> pd.DataFrame:
    """Return one explicit scalar envelope regardless of outcome variant."""

    output = []
    identities: set[str] = set()
    for raw in rows:
        row = dict(raw)
        candidate_id = str(row.get("candidate_id") or "")
        if not candidate_id:
            raise ValueError("candidate result lacks candidate_id")
        if candidate_id in identities:
            raise ValueError(f"duplicate candidate result: {candidate_id}")
        identities.add(candidate_id)
        status = str(row.get("candidate_replay_status") or "")
        if status not in {
            "CANDIDATE_REPLAY_COMPLETE",
            "CANDIDATE_REPLAY_BLOCKED",
        }:
            raise ValueError(f"unknown candidate replay status: {status}")
        payload_json = _payload_json(row)
        output.append(
            {
                "schema_version": "cn_candidate_replay_summary_envelope_v1",
                "candidate_id": candidate_id,
                "pair_id": str(row.get("pair_id") or ""),
                "pair_member_role": str(row.get("pair_member_role") or ""),
                "route_id": str(row.get("route_id") or ""),
                "exact_identity": str(row.get("exact_identity") or ""),
                "candidate_replay_status": status,
                "blocker_code": (
                    str(row.get("blocker_code"))
                    if row.get("blocker_code") is not None
                    else None
                ),
                "a_share_executable_net_reward": row.get(
                    "a_share_executable_net_reward"
                ),
                "diagnostic_net_reward": row.get("diagnostic_net_reward"),
                "train_read_count": row.get("train_read_count"),
                "trade_count": row.get("trade_count"),
                "fill_count": row.get("fill_count"),
                "economic_claim_authorized": bool(
                    row.get("economic_claim_authorized", False)
                ),
                "promotion_authorized": bool(
                    row.get("promotion_authorized", False)
                ),
                "result_payload_sha256": hashlib.sha256(
                    payload_json.encode("utf-8")
                ).hexdigest(),
                "result_payload_json": payload_json,
            }
        )
    frame = pd.DataFrame(output, columns=CANDIDATE_RESULT_COLUMNS)
    for column in (
        "schema_version",
        "candidate_id",
        "pair_id",
        "pair_member_role",
        "route_id",
        "exact_identity",
        "candidate_replay_status",
        "blocker_code",
        "result_payload_sha256",
        "result_payload_json",
    ):
        frame[column] = frame[column].astype("string")
    for column in ("a_share_executable_net_reward", "diagnostic_net_reward"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce").astype(
            "Float64"
        )
    for column in ("train_read_count", "trade_count", "fill_count"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce").astype(
            "Int64"
        )
    for column in ("economic_claim_authorized", "promotion_authorized"):
        frame[column] = frame[column].astype("boolean")
    return frame

