"""Clock-safe input classification for compositional signal sketches."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping


SESSION_ROUTES = {
    "SLOW_CROSS_SECTIONAL_LEVEL",
    "SLOW_TEMPORAL_CHANGE",
    "DISCLOSURE_EVENT",
}


@dataclass(frozen=True, slots=True)
class SignalSketchInputClassification:
    active_rows: list[dict[str, Any]]
    session_rows: list[dict[str, Any]]
    session_context_fields: tuple[str, ...]
    canonical_fundamental_fields: tuple[str, ...]
    summary: dict[str, Any]


def classify_signal_sketch_receipts(
    receipts: Iterable[Mapping[str, Any]],
    selected_exact_identities: set[str],
    *,
    source_family_by_field: Mapping[str, str],
) -> SignalSketchInputClassification:
    active: list[dict[str, Any]] = []
    session: list[dict[str, Any]] = []
    session_context: set[str] = set()
    canonical_fields: set[str] = set()
    seen: set[str] = set()
    for raw in receipts:
        exact_identity = str(raw.get("exact_identity") or "")
        if exact_identity not in selected_exact_identities:
            continue
        if exact_identity in seen:
            raise ValueError(f"duplicate exact receipt: {exact_identity}")
        seen.add(exact_identity)
        row = dict(raw)
        route_id = str(row.get("route_id") or "")
        if route_id == "BROAD_EVENT_FROZEN_ENTRY":
            raise ValueError("frozen Broad Event reference cannot enter new signal-sketch admission")
        fields = [str(value) for value in row.get("declared_field_ids", ())]
        for field_id in fields:
            family = str(source_family_by_field.get(field_id) or "")
            if not family:
                raise ValueError(f"signal-sketch receipt has unregistered field: {field_id}")
            if route_id in SESSION_ROUTES:
                if family.startswith("canonical_fundamental_"):
                    canonical_fields.add(field_id)
                else:
                    session_context.add(field_id)
        if route_id in SESSION_ROUTES:
            session.append(row)
        else:
            active.append(row)
    if seen != selected_exact_identities:
        missing = sorted(selected_exact_identities - seen)
        raise ValueError(f"preadmission identities missing pair receipts: {missing[:8]}")
    active.sort(key=lambda row: str(row["exact_identity"]))
    session.sort(key=lambda row: str(row["exact_identity"]))
    return SignalSketchInputClassification(
        active_rows=active,
        session_rows=session,
        session_context_fields=tuple(sorted(session_context)),
        canonical_fundamental_fields=tuple(sorted(canonical_fields)),
        summary={
            "selected_exact_identity_count": len(seen),
            "active_candidate_count": len(active),
            "session_candidate_count": len(session),
            "session_context_field_count": len(session_context),
            "canonical_fundamental_field_count": len(canonical_fields),
            "reward_columns_read": 0,
            "labels_read": 0,
            "validation_holdout_forward_read": False,
            "classification_uses_performance": False,
        },
    )
