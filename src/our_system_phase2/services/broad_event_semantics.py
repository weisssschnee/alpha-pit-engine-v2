"""Authoritative semantic inventory for the CN Broad Event recovery."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
from pathlib import Path
from typing import Any

from our_system_phase2.services.chip_sidecar import CHIP_FIELDS


SEMANTIC_REGISTRY_VERSION = "cn_broad_event_semantic_registry_v1"
SEMANTIC_TYPES = {
    "EVENT_PULSE",
    "LIFECYCLE_STATE",
    "LATCHED_OBSERVATION",
    "REGIME_STATE",
    "LAGGED_CONTEXT",
}


@dataclass(frozen=True, slots=True)
class EventFieldSemantic:
    field_name: str
    registry_family: str
    source_artifact: str
    raw_source_field: str
    event_relevance: str
    semantic_type: str | None
    true_semantics: str
    event_time: str
    observed_time: str
    maturity: str
    source_lag: str
    latched: bool
    persistent_state: bool
    entry_detectable: bool
    exit_detectable: bool
    transition_detectable: bool
    reset_rule: str
    entity_scope: str
    inference_unit: str
    episode_definition: str
    search_role: str
    cross_sectional_rank_allowed: bool
    negative_control_eligible: bool
    missing_semantics: str

    def canonical(self) -> dict[str, Any]:
        return asdict(self)


def _source(field: str) -> tuple[str, str]:
    routes = (
        ("evt_uplimit_", "zls/uplimit_stocks.parquet"),
        ("ctx_sent_", "zls/open_sentiment_data.parquet"),
        ("ctx_zls_", "zls/sentiment_hot_day.parquet"),
        ("ctx_billboard_", "xsection/billboard_details/billboard_details.parquet"),
        ("ctx_rzrq_", "cn_public_rzrq_daily_silver/rzrq_margin_xsection_daily.parquet"),
        ("ctx_holder_", "xsection/holder_num_detail/holder_num_detail.parquet"),
        ("ctx_ths_hot_", "zls/ths_hot_top.parquet"),
        ("ctx_hfq_", "hfq_daily_context/*.parquet"),
        ("m1_first", "true1min firstN materialization"),
    )
    for prefix, artifact in routes:
        if field.startswith(prefix):
            return artifact, field[len(prefix) :]
    if field.startswith("chip_"):
        return "user_supplied_daily_chip_archive", field
    return "true1min_16_shards", field


def _inventory_row(field: dict[str, Any]) -> EventFieldSemantic:
    name = str(field["name"])
    family = str(field["family"])
    source_artifact, raw = _source(name)
    if name.startswith("evt_uplimit_"):
        is_active = name == "evt_uplimit_active"
        is_clock = name in {"evt_uplimit_cutoff_minute", "evt_uplimit_age_min"}
        semantics = (
            "one after vendor up_limit_time through session close"
            if is_active
            else "minutes since vendor up_limit_time through session close"
            if name == "evt_uplimit_age_min"
            else "vendor same-day limit-up payload revealed at and latched after up_limit_time"
        )
        return EventFieldSemantic(
            name, family, source_artifact, raw, "VENDOR_OCCURRENCE_INPUT",
            "LATCHED_OBSERVATION", semantics,
            "vendor up_limit_time", "first bar close at or after vendor up_limit_time",
            "same bar close", "same session after vendor cutoff", True, False,
            is_active or is_clock, False, False, "session close", "SYMBOL",
            "symbol-day occurrence episode", "one symbol-day beginning at vendor cutoff",
            "occurrence-condition-only" if is_active or is_clock else "event-interaction-only",
            not is_active, False, "missing means vendor occurrence/payload unavailable, not no event",
        )
    if name.startswith(("ctx_sent_", "ctx_zls_")):
        return EventFieldSemantic(
            name, family, source_artifact, raw, "MARKET_REGIME_INPUT", "REGIME_STATE",
            "market-wide daily sentiment or limit-ecosystem state replicated across symbols",
            "source session close", "next available development session", "one session",
            "strictly previous source date", False, True, False, False, True,
            "changes only at session boundary", "MARKET", "market-time block",
            "maximal consecutive market block under preregistered state bucket",
            "condition-only", False, False,
            "missing market state is unknown and cannot be a cross-sectional negative",
        )
    if name.startswith(("ctx_billboard_", "ctx_rzrq_", "ctx_holder_", "ctx_ths_hot_", "ctx_hfq_")):
        source_family = name.split("_", 2)[1]
        return EventFieldSemantic(
            name, family, source_artifact, raw, "DAILY_CONTEXT_INPUT", "LAGGED_CONTEXT",
            f"previously observable {source_family} daily value carried by strict as-of join",
            "source date or disclosure date", "next available development session",
            "one session", "strictly previous source date", True, True, False, False, True,
            "changes when a newer source row becomes observable", "SYMBOL", "symbol-day",
            "numeric-change episode; disclosure existence requires source-date evidence",
            "condition-only" if name == "ctx_hfq_is_st" else "event-interaction-only",
            name != "ctx_hfq_is_st", False,
            "missing is unavailable; unchanged carried value is not proof of no disclosure",
        )
    relevance = (
        "LIFECYCLE_INPUT" if name in {"open", "high", "low", "close", "trade_time", "date", "code"}
        else "PRE_POST_PATH_INPUT" if family in {"raw_1min", "firstN"}
        else "NOT_EVENT_ADJACENT"
    )
    source_lag = f"{field.get('source_lag', 0)} {field.get('source_lag_unit', 'bars')}"
    return EventFieldSemantic(
        name, family, source_artifact, raw, relevance, None,
        "ordinary observation or metadata; not an event by itself", "not applicable",
        str(field.get("observable_clock", "unknown")),
        f"{field.get('maturity', 0)} {field.get('maturity_unit', 'bars')}",
        source_lag, False, False, False, False, False, "not applicable",
        "SYMBOL" if name not in {"date", "trade_time"} else "COORDINATE",
        "not applicable", "not applicable", "path-input-only" if relevance != "NOT_EVENT_ADJACENT" else "not-event-searchable",
        False, False, str(field.get("missing_policy", "propagate")),
    )


def _chip_rows() -> list[EventFieldSemantic]:
    rows: list[EventFieldSemantic] = []
    for name in CHIP_FIELDS.values():
        rows.append(
            EventFieldSemantic(
                name, "chip_distribution", "user_supplied_daily_chip_archive", name,
                "DAILY_CONTEXT_INPUT", "LAGGED_CONTEXT",
                "daily chip distribution or cost observation", "source session close",
                "next development session", "one session", "one session", True, True,
                False, False, True, "new source session", "SYMBOL", "symbol-day",
                "preregistered material chip-change threshold on consecutive observable sessions",
                "event-interaction-only", True, False,
                "missing source or previous observation blocks chip-change detection",
            )
        )
    return rows


def build_semantic_registry(field_registry: dict[str, Any]) -> dict[str, Any]:
    rows = [_inventory_row(field) for field in field_registry["fields"]]
    chip_rows = _chip_rows()
    sources = [
        {
            "source_id": "minute_limit_lifecycle",
            "semantic_type": "LIFECYCLE_STATE",
            "entity_scope": "SYMBOL",
            "inputs": ["open", "high", "low", "close", "PIT limit prices or conservative rule derivation"],
            "episodes": ["TOUCH_ONLY", "CLOSED_AT_LIMIT_RUN"],
            "allowed_pulses": ["FIRST_TOUCH", "CLOSED_AT_LIMIT_ENTRY", "LEFT_LIMIT_BETWEEN_BARS", "RESEALED_BETWEEN_BARS"],
            "intrabar_unknown": "INTRABAR_ORDER_AMBIGUOUS propagates unknown and is never a negative control",
        },
        {
            "source_id": "vendor_limit_occurrence",
            "semantic_type": "LATCHED_OBSERVATION",
            "entity_scope": "SYMBOL",
            "inputs": [row.field_name for row in rows if row.field_name.startswith("evt_uplimit_")],
            "episodes": ["one symbol-day occurrence at vendor cutoff"],
            "allowed_pulses": ["entry only"],
        },
        {
            "source_id": "daily_disclosure_changes",
            "semantic_type": "EVENT_PULSE",
            "entity_scope": "SYMBOL",
            "inputs": ["ctx_billboard_*", "ctx_rzrq_*", "ctx_holder_*", "ctx_ths_hot_*"],
            "episodes": ["symbol-day numeric-change episode after one-session lag"],
            "allowed_pulses": ["observable numeric change; not raw disclosure existence without source date"],
        },
        {
            "source_id": "market_ecology_transitions",
            "semantic_type": "REGIME_STATE",
            "entity_scope": "MARKET",
            "inputs": ["ctx_sent_*", "ctx_zls_*"],
            "episodes": ["market-time block"],
            "allowed_pulses": ["session-boundary transition"],
            "cross_sectional_rank_allowed": False,
            "search_role": "condition-only",
        },
        {
            "source_id": "chip_structure_changes",
            "semantic_type": "EVENT_PULSE",
            "entity_scope": "SYMBOL",
            "inputs": [row.field_name for row in chip_rows],
            "episodes": ["symbol-day material change after source becomes observable"],
            "allowed_pulses": ["preregistered delta threshold"],
        },
    ]
    payload: dict[str, Any] = {
        "version": SEMANTIC_REGISTRY_VERSION,
        "source_field_registry_version": field_registry["registry_version"],
        "source_field_registry_hash": field_registry["registry_hash"],
        "scanned_field_count": len(rows),
        "external_sidecar_field_count": len(chip_rows),
        "semantic_types": sorted(SEMANTIC_TYPES),
        "field_inventory": [row.canonical() for row in rows],
        "external_sidecar_inventory": [row.canonical() for row in chip_rows],
        "event_source_contracts": sources,
        "plate_industry_enabled": False,
        "performance_or_winner_directed": False,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    payload["registry_hash"] = hashlib.sha256(encoded).hexdigest()
    return payload


def load_and_build(field_registry_path: Path) -> dict[str, Any]:
    return build_semantic_registry(json.loads(field_registry_path.read_text(encoding="utf-8")))


def validate_semantic_registry(registry: dict[str, Any]) -> None:
    if registry["scanned_field_count"] != 121:
        raise ValueError("Broad Event semantic inventory must scan all 121 registered fields")
    if set(registry["semantic_types"]) != SEMANTIC_TYPES:
        raise ValueError("Broad Event semantic type contract mismatch")
    fields = registry["field_inventory"] + registry["external_sidecar_inventory"]
    for row in fields:
        if row["semantic_type"] is not None and row["semantic_type"] not in SEMANTIC_TYPES:
            raise ValueError(f"unknown semantic type for {row['field_name']}")
        if row["semantic_type"] == "REGIME_STATE":
            if row["entity_scope"] != "MARKET" or row["cross_sectional_rank_allowed"]:
                raise ValueError(f"market regime contract invalid for {row['field_name']}")
            if row["search_role"] != "condition-only":
                raise ValueError(f"market regime must be condition-only: {row['field_name']}")
        if row["semantic_type"] == "LATCHED_OBSERVATION" and row["exit_detectable"]:
            raise ValueError(f"latched field cannot claim exit detection: {row['field_name']}")

