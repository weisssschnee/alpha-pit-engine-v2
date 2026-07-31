"""Candidate-level field compatibility for executable replay clocks."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any, Mapping, Sequence


FIELD_PATTERN = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)")


class ExecutionClockCapabilityDriftError(RuntimeError):
    """The label-free execution capability authority is incomplete or stale."""


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def load_execution_capability_manifest(path: Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    claimed = str(payload.get("capability_manifest_sha256") or "")
    body = dict(payload)
    body.pop("capability_manifest_sha256", None)
    if claimed != _stable_hash(body):
        raise ExecutionClockCapabilityDriftError(
            "execution capability manifest self-hash drift"
        )
    if str(payload.get("status") or "") != "ZERO_FINANCIAL_CAPABILITY_CLOSED":
        raise ExecutionClockCapabilityDriftError(
            "execution capability manifest is not closed"
        )
    for key in (
        "financial_reads",
        "validation_reads",
        "holdout_reads",
        "forward_2026_reads",
        "optimizer_writes",
        "feedback_writes",
        "archive_writes",
        "promotion_writes",
    ):
        if int(payload.get(key) or 0) != 0:
            raise ExecutionClockCapabilityDriftError(
                f"execution capability manifest has prohibited reads: {key}"
            )
    if not isinstance(payload.get("fields"), Mapping):
        raise ExecutionClockCapabilityDriftError(
            "execution capability field map is missing"
        )
    return payload


def expression_field_ids(expression: str) -> tuple[str, ...]:
    return tuple(sorted(set(FIELD_PATTERN.findall(str(expression)))))


def assess_candidate_field_capability(
    expressions: Sequence[str],
    manifest: Mapping[str, Any],
) -> dict[str, Any]:
    fields = sorted(
        {
            field_id
            for expression in expressions
            for field_id in expression_field_ids(str(expression))
        }
    )
    authority = dict(manifest.get("fields") or {})
    missing = [field_id for field_id in fields if field_id not in authority]
    unsupported = [
        field_id
        for field_id in fields
        if field_id in authority
        and str((authority[field_id] or {}).get("status") or "")
        != "SUPPORTED"
    ]
    reasons = {
        field_id: str((authority.get(field_id) or {}).get("reason") or "")
        for field_id in (*missing, *unsupported)
    }
    return {
        "execution_clock": str(manifest.get("execution_clock") or ""),
        "required_field_ids": fields,
        "missing_field_ids": missing,
        "unsupported_field_ids": unsupported,
        "incompatibility_reasons": reasons,
        "compatible": not missing and not unsupported,
    }
