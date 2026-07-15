"""Formal matched-control pair contracts and immutable pair receipts.

Candidate receipts authorize individual expressions.  This module adds the
second authority boundary required by the formal evaluator: a primary and its
route-specific control are one indivisible evaluation/admission unit.
"""

from __future__ import annotations

import json
import hashlib
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from our_system_phase2.services.candidate_submission_receipt import (
    CandidateReceiptError,
    normalize_candidate_contract,
)
from our_system_phase2.services.unified_capability_registry import stable_hash


PAIR_RECEIPT_SCHEMA_VERSION = "cn_candidate_pair_receipt_v2"
PAIR_AUTHORIZATION_STATUS = "AUTHORIZED_FOR_FORMAL_PAIR_EVALUATION"
MATCHED_OPTIMIZER_REWARD_SOURCE = "train_only_phase3cm_matched_increment"
MATCHED_OPTIMIZER_REWARD_METRIC = "matched_train_primary_minus_control_composite_reward"
PAIR_TRAIN_FEEDBACK_READY = "PAIR_TRAIN_FEEDBACK_READY"
PAIR_TRAIN_FEEDBACK_BLOCKED = "PAIR_TRAIN_FEEDBACK_BLOCKED"
PAIR_MAPPING_PORTFOLIO_CONTRACT = (
    "SAME_FULL_SHARD_UNIVERSE|SAME_TRADE_TIMES|SAME_SPLIT_ROLES|SAME_HORIZONS|"
    "SAME_SUPPORT_COORDINATES|SAME_PORTFOLIO_MODE|SAME_COST_ASSUMPTIONS"
)


CONTROL_CONSTRUCTOR_MATRIX: dict[str, dict[str, Any]] = {
    "MINUTE_STATIC": {
        "control_constructor_id": "minute_static_remove_interaction_keep_base_v1",
        "keeps": ["base minute field", "stock-time support", "PIT clock", "portfolio contract"],
        "removes": "secondary-field interaction and magnitude combination",
        "control_form": "cross-sectional signed/base projection",
        "legacy_mapping": "requires at least two raw-minute fields and an explicit interaction operator",
        "rationale": "isolates the incremental interaction over a simpler minute cross-section",
    },
    "FIRSTN_PATH": {
        "control_constructor_id": "firstn_path_remove_intraday_path_keep_clock_v2",
        "keeps": ["FirstN field", "raw path maturity mask", "stock-session support", "maturity clock", "portfolio contract"],
        "removes": "post-FirstN intraday path value while retaining its zero-valued clock carrier",
        "control_form": "FirstN projection plus zero-times identical Delta path for support/maturity alignment",
        "legacy_mapping": "requires one qualified FirstN field and an explicit raw-minute Delta path with a fixed window",
        "rationale": "tests whether the intraday path adds information beyond the opening state",
    },
    "SLOW_CROSS_SECTIONAL_LEVEL": {
        "control_constructor_id": "slow_level_sign_projection_v1",
        "keeps": ["same PIT field", "stock-session as-of support", "source lag", "portfolio contract"],
        "removes": "continuous level magnitude",
        "control_form": "sign-only projection of the same field",
        "legacy_mapping": "requires exactly one PIT-qualified slow level field",
        "rationale": "separates continuous level information from coarse direction",
    },
    "SLOW_TEMPORAL_CHANGE": {
        "control_constructor_id": "slow_change_sign_projection_v1",
        "keeps": ["same PIT change representation", "disclosure/session support", "source lag", "portfolio contract"],
        "removes": "continuous change magnitude",
        "control_form": "sign-only projection of the same change representation",
        "legacy_mapping": "requires exactly one qualified temporal representation and a registered change operator",
        "rationale": "tests magnitude/path value beyond the direction of the disclosed change",
    },
    "DISCLOSURE_EVENT": {
        "control_constructor_id": "disclosure_event_age_control_v1",
        "keeps": ["same disclosure field", "episode support", "observable/maturity clock", "portfolio contract"],
        "removes": "event-count/window mechanism",
        "control_form": "time-since-disclosure control",
        "legacy_mapping": "explicit typed proposals only",
        "rationale": "compares event-window structure against disclosure recency on identical episodes",
    },
    "MARKET_REGIME_CONDITION": {
        "control_constructor_id": "market_regime_drop_condition_v1",
        "keeps": ["same stock payload", "market-time blocks", "maturity clock", "portfolio contract"],
        "removes": "market-regime conditioning term",
        "control_form": "unconditional payload projection",
        "legacy_mapping": "explicit typed proposals only",
        "rationale": "measures the increment from conditioning without changing stock payload timing",
    },
    "INTRADAY_STATE_TRANSITION": {
        "control_constructor_id": "intraday_state_drop_transition_v1",
        "keeps": ["same raw payload", "intraday episode clock", "horizon", "portfolio contract"],
        "removes": "state-transition gate",
        "control_form": "unconditional intraday path projection",
        "legacy_mapping": "explicit typed proposals only",
        "rationale": "isolates transition conditioning from the underlying intraday path",
    },
    "BROAD_EVENT_FROZEN_ENTRY": {
        "control_constructor_id": "broad_event_frozen_matched_control_v1",
        "keeps": ["frozen mechanism field", "episode support", "maturity clock", "portfolio contract"],
        "removes": "frozen event trigger/mechanism",
        "control_form": "frozen preregistered matched-control replay",
        "legacy_mapping": "explicit frozen typed proposals only",
        "rationale": "preserves the accepted Broad Event control without redesign from performance",
    },
}


# Experiment-scoped compositional controls keep every source field and clock
# carrier.  The legacy matrix above remains valid for historical receipts.
COMPOSITIONAL_CONTROL_CONSTRUCTOR_MATRIX: dict[str, dict[str, Any]] = {
    route_id: {
        "control_constructor_id": f"cn_compositional_same_fields_{route_id.lower()}_v2",
        "keeps": [
            "same source fields",
            "same observable clocks",
            "same source lags",
            "same maturity",
            "same support",
            "same outer mapping",
        ],
        "removes": "only the registered skeleton core mechanism",
        "control_form": "route-specific neutral ablation with zero-valued field carriers",
        "legacy_mapping": "not applicable; experiment-scoped compositional grammar only",
        "rationale": "isolates the declared mechanism without changing field or support eligibility",
    }
    for route_id in CONTROL_CONSTRUCTOR_MATRIX
}

PAIR_RANK_IC_REQUIRED_ROUTES = frozenset(CONTROL_CONSTRUCTOR_MATRIX)


class CandidatePairError(RuntimeError):
    """Raised when a primary/control pair cannot enter formal evaluation."""


def constructor_for_route(
    route_id: str,
    control_constructor_id: str | None = None,
) -> dict[str, Any]:
    try:
        legacy = dict(CONTROL_CONSTRUCTOR_MATRIX[str(route_id)])
        compositional = dict(COMPOSITIONAL_CONTROL_CONSTRUCTOR_MATRIX[str(route_id)])
    except KeyError as exc:
        raise CandidatePairError(f"no registered matched-control constructor for route: {route_id}") from exc
    selected = str(control_constructor_id or legacy["control_constructor_id"])
    if selected == legacy["control_constructor_id"]:
        return legacy
    if selected == compositional["control_constructor_id"]:
        return compositional
    raise CandidatePairError(f"unregistered control constructor for {route_id}: {selected}")


def pair_identity(
    primary_candidate_id: str,
    control_candidate_id: str,
    route_id: str,
    control_constructor_id: str,
) -> str:
    digest = stable_hash(
        {
            "primary_candidate_id": str(primary_candidate_id),
            "control_candidate_id": str(control_candidate_id),
            "route_id": str(route_id),
            "control_constructor_id": str(control_constructor_id),
        }
    )
    return "cn.pair." + digest[:32]


def _validate_constructor_shape(
    route_id: str,
    primary: Mapping[str, Any],
    control: Mapping[str, Any],
    control_constructor_id: str,
) -> None:
    primary_expression = str(primary.get("expression") or "")
    control_expression = str(control.get("expression") or "")
    primary_fields = {str(value) for value in primary.get("declared_field_ids") or ()}
    control_fields = {str(value) for value in control.get("declared_field_ids") or ()}
    compositional_id = str(
        COMPOSITIONAL_CONTROL_CONSTRUCTOR_MATRIX.get(route_id, {}).get("control_constructor_id") or ""
    )
    if control_constructor_id == compositional_id:
        valid = (
            bool(primary_fields)
            and primary_fields == control_fields
            and primary_expression != control_expression
            and str(primary.get("clock_contract") or "") == str(control.get("clock_contract") or "")
            and str(primary.get("maturity_contract") or "") == str(control.get("maturity_contract") or "")
            and str(primary.get("unit_signature") or "") == str(control.get("unit_signature") or "")
            and str(primary.get("support_unit") or "") == str(control.get("support_unit") or "")
            and bool(str(primary.get("control_ablation_rule") or ""))
            and str(primary.get("control_ablation_rule") or "")
            == str(control.get("control_ablation_rule") or "")
        )
        if route_id == "BROAD_EVENT_FROZEN_ENTRY":
            valid = valid and "FrozenMechanismReplay(" in primary_expression and "MatchedControlReplay(" in control_expression
        elif route_id == "DISCLOSURE_EVENT":
            valid = valid and "TimeSince(" in control_expression
        else:
            valid = valid and ("Mul(0," in control_expression or "Sign(" in control_expression)
        if not valid:
            raise CandidatePairError(
                f"FAIL_CLOSED_NO_MATCHED_CONTROL_CONSTRUCTOR: proposal does not match registered {route_id} compositional constructor"
            )
        return

    valid = False
    if route_id == "MINUTE_STATIC":
        valid = (
            len(primary_fields) >= 2
            and bool(control_fields)
            and control_fields < primary_fields
            and any(token in primary_expression for token in ("Add(", "Sub(", "Mul(", "Div(", "ZScore("))
            and "Sign(" in control_expression
        )
    elif route_id == "FIRSTN_PATH":
        valid = (
            len(primary_fields) >= 2
            and control_fields == primary_fields
            and "Delta(" in primary_expression
            and "Mul(0,Delta(" in control_expression
            and "Sign(Delta(" not in control_expression
        )
    elif route_id in {"SLOW_CROSS_SECTIONAL_LEVEL", "SLOW_TEMPORAL_CHANGE"}:
        valid = primary_fields == control_fields and len(primary_fields) == 1 and "Sign(" in control_expression
    elif route_id == "DISCLOSURE_EVENT":
        valid = "EventCount(" in primary_expression and "TimeSince(" in control_expression
    elif route_id == "MARKET_REGIME_CONDITION":
        valid = "Mul(" in primary_expression and "Mul(" not in control_expression
    elif route_id == "INTRADAY_STATE_TRANSITION":
        valid = "Transition(" in primary_expression and "Transition(" not in control_expression
    elif route_id == "BROAD_EVENT_FROZEN_ENTRY":
        valid = (
            "FrozenMechanismReplay(" in primary_expression
            and "MatchedControlReplay(" in control_expression
        )
    if not valid:
        raise CandidatePairError(
            f"FAIL_CLOSED_NO_MATCHED_CONTROL_CONSTRUCTOR: proposal does not match registered {route_id} constructor"
        )


def attach_pair_contract(
    primary: Mapping[str, Any],
    control: Mapping[str, Any],
    *,
    control_constructor_id: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    primary_row = normalize_candidate_contract(primary)
    control_row = normalize_candidate_contract(control)
    route_id = str(primary_row.get("route_id") or "")
    if not route_id or str(control_row.get("route_id") or "") != route_id:
        raise CandidatePairError("primary/control route mismatch")
    selected_constructor = str(
        control_constructor_id
        or CONTROL_CONSTRUCTOR_MATRIX.get(route_id, {}).get("control_constructor_id")
        or ""
    )
    constructor = constructor_for_route(route_id, selected_constructor)
    expected_constructor = str(constructor["control_constructor_id"])
    if selected_constructor != expected_constructor:
        raise CandidatePairError(f"unregistered control constructor for {route_id}: {selected_constructor}")
    primary_id = str(primary_row.get("candidate_id") or "")
    control_id = str(control_row.get("candidate_id") or "")
    if not primary_id or not control_id or primary_id == control_id:
        raise CandidatePairError("pair requires distinct primary and control candidate IDs")
    if str(primary_row.get("matched_control_id") or "") != control_id:
        raise CandidatePairError("primary does not point to its control")
    if str(control_row.get("matched_control_id") or "") != primary_id:
        raise CandidatePairError("control does not point to its primary")
    if bool(primary_row.get("is_matched_control")) or not bool(control_row.get("is_matched_control")):
        raise CandidatePairError("pair member roles are invalid")
    _validate_constructor_shape(route_id, primary_row, control_row, selected_constructor)
    pair_id = pair_identity(primary_id, control_id, route_id, selected_constructor)
    shared = {
        "pair_id": pair_id,
        "control_constructor_id": selected_constructor,
        "pair_mapping_portfolio_contract": PAIR_MAPPING_PORTFOLIO_CONTRACT,
        "allow_behavior_equivalence": False,
    }
    return (
        {**primary_row, **shared, "pair_member_role": "PRIMARY"},
        {**control_row, **shared, "pair_member_role": "CONTROL"},
    )


def _receipt_payload(receipt: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(receipt)
    payload.pop("pair_receipt_hash", None)
    payload.pop("pair_receipt_id", None)
    return payload


def _field_contract(
    receipt: Mapping[str, Any],
    key: str,
    *,
    pair_id: str,
) -> tuple[str, ...]:
    raw = receipt.get(key)
    if not isinstance(raw, (list, tuple)) or not raw:
        raise CandidatePairError(f"candidate receipt lacks {key}: pair={pair_id}")
    values = tuple(str(value) for value in raw)
    if len(values) != len(set(values)):
        raise CandidatePairError(f"candidate receipt has duplicate {key}: pair={pair_id}")
    field_ids = {str(value) for value in receipt.get("field_ids") or ()}
    contract_field_ids = {value.split("|", 1)[0] for value in values if "|" in value}
    if not field_ids or contract_field_ids != field_ids:
        raise CandidatePairError(f"candidate receipt {key} field lineage mismatch: pair={pair_id}")
    return values


@dataclass(frozen=True, slots=True)
class CandidatePairAuthority:
    """Authorize and revalidate immutable primary/control pair receipts."""

    def authorize_table(
        self,
        candidates: Iterable[Mapping[str, Any]],
        candidate_receipts: Iterable[Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        rows = [normalize_candidate_contract(row) for row in candidates]
        by_id = {str(row.get("candidate_id") or ""): row for row in rows}
        if len(by_id) != len(rows) or "" in by_id:
            raise CandidatePairError("candidate pair table contains missing or duplicate candidate IDs")
        receipts = [dict(row) for row in candidate_receipts]
        receipt_by_id = {str(row.get("candidate_id") or ""): row for row in receipts}
        if len(receipt_by_id) != len(receipts):
            raise CandidatePairError("candidate receipt table contains duplicate candidate IDs")

        output: list[dict[str, Any]] = []
        seen_pairs: set[str] = set()
        for primary in rows:
            if bool(primary.get("is_matched_control")):
                continue
            control_id = str(primary.get("matched_control_id") or "")
            control = by_id.get(control_id)
            if control is None:
                raise CandidatePairError(f"control row is absent for primary: {primary.get('candidate_id')}")
            primary, control = attach_pair_contract(
                primary,
                control,
                control_constructor_id=str(primary.get("control_constructor_id") or "") or None,
            )
            pair_id = str(primary["pair_id"])
            if pair_id in seen_pairs:
                raise CandidatePairError(f"duplicate pair identity: {pair_id}")
            seen_pairs.add(pair_id)
            primary_receipt = receipt_by_id.get(str(primary["candidate_id"]))
            control_receipt = receipt_by_id.get(str(control["candidate_id"]))
            if primary_receipt is None or control_receipt is None:
                raise CandidatePairError(f"candidate receipt missing for pair: {pair_id}")
            for key in (
                "route_id",
                "support_unit",
                "split_manifest_hash",
                "data_release_hash",
                "evaluator_code_hash",
                "unified_registry_hash",
                "typed_compiler_hash",
            ):
                if primary_receipt.get(key) != control_receipt.get(key):
                    raise CandidatePairError(f"primary/control receipt context mismatch: pair={pair_id} key={key}")
            primary_observable = _field_contract(
                primary_receipt, "observable_time_contract", pair_id=pair_id
            )
            control_observable = _field_contract(
                control_receipt, "observable_time_contract", pair_id=pair_id
            )
            primary_pit_lag = _field_contract(
                primary_receipt, "pit_source_lag_contract", pair_id=pair_id
            )
            control_pit_lag = _field_contract(
                control_receipt, "pit_source_lag_contract", pair_id=pair_id
            )
            # Route controls may remove a mechanism field, but they may not
            # introduce a different clock or lag.  Exact eligible coordinates
            # are checked again after real signal evaluation.
            if not set(control_observable).issubset(primary_observable):
                raise CandidatePairError(
                    f"primary/control observable time contract mismatch: pair={pair_id}"
                )
            if not set(control_pit_lag).issubset(primary_pit_lag):
                raise CandidatePairError(
                    f"primary/control PIT/source-lag contract mismatch: pair={pair_id}"
                )
            if str(primary_receipt.get("exact_identity") or "") == str(control_receipt.get("exact_identity") or ""):
                raise CandidatePairError(f"control exact identity equals primary: {pair_id}")
            constructor = constructor_for_route(str(primary["route_id"]))
            body: dict[str, Any] = {
                "pair_receipt_schema_version": PAIR_RECEIPT_SCHEMA_VERSION,
                "pair_authorization_status": PAIR_AUTHORIZATION_STATUS,
                "pair_id": pair_id,
                "primary_candidate_id": str(primary["candidate_id"]),
                "control_candidate_id": str(control["candidate_id"]),
                "primary_receipt_hash": str(primary_receipt.get("receipt_hash") or ""),
                "control_receipt_hash": str(control_receipt.get("receipt_hash") or ""),
                "route_id": str(primary["route_id"]),
                "primary_field_ids": list(primary_receipt.get("field_ids") or ()),
                "control_field_ids": list(control_receipt.get("field_ids") or ()),
                "source_field_ids": sorted(
                    set(primary_receipt.get("source_field_ids") or ())
                    | set(control_receipt.get("source_field_ids") or ())
                ),
                "representation_ids": sorted(
                    set(primary_receipt.get("representation_ids") or ())
                    | set(control_receipt.get("representation_ids") or ())
                ),
                "support_unit": str(primary_receipt.get("support_unit") or ""),
                "pair_clock_alignment_policy": "PRIMARY_CLOCK_AND_EXACT_ELIGIBLE_SUPPORT",
                "primary_observable_time_contract": list(primary_observable),
                "control_observable_time_contract": list(control_observable),
                "primary_pit_source_lag_contract": list(primary_pit_lag),
                "control_pit_source_lag_contract": list(control_pit_lag),
                "mapping_portfolio_contract": PAIR_MAPPING_PORTFOLIO_CONTRACT,
                "split_manifest_hash": str(primary_receipt.get("split_manifest_hash") or ""),
                "data_release_hash": str(primary_receipt.get("data_release_hash") or ""),
                "evaluator_code_hash": str(primary_receipt.get("evaluator_code_hash") or ""),
                "pair_authority_code_hash": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                "unified_registry_hash": str(primary_receipt.get("unified_registry_hash") or ""),
                "typed_compiler_hash": str(primary_receipt.get("typed_compiler_hash") or ""),
                "control_constructor_id": str(constructor["control_constructor_id"]),
                "control_constructor_contract": constructor,
                "allow_behavior_equivalence": bool(primary.get("allow_behavior_equivalence", False)),
                "control_vote_policy": "CONTROL_NO_SEPARATE_VOTE",
                "primary_vote_policy": "ONE_SUPPORT_UNIT_ONE_VOTE",
            }
            if not body["primary_receipt_hash"] or not body["control_receipt_hash"]:
                raise CandidatePairError(f"candidate receipt hash missing for pair: {pair_id}")
            body["pair_receipt_hash"] = stable_hash(body)
            body["pair_receipt_id"] = "cn.pair.receipt." + body["pair_receipt_hash"][:32]
            output.append(body)
        if len(output) * 2 != len(rows):
            raise CandidatePairError("every authorized candidate row must belong to exactly one primary/control pair")
        return sorted(output, key=lambda row: str(row["pair_id"]))

    def validate_table(
        self,
        candidates: Iterable[Mapping[str, Any]],
        candidate_receipts: Iterable[Mapping[str, Any]],
        pair_receipts: Iterable[Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        supplied = [dict(row) for row in pair_receipts]
        for row in supplied:
            expected_hash = stable_hash(_receipt_payload(row))
            if str(row.get("pair_receipt_hash") or "") != expected_hash:
                raise CandidatePairError("candidate pair receipt hash does not match payload")
            if str(row.get("pair_receipt_id") or "") != "cn.pair.receipt." + expected_hash[:32]:
                raise CandidatePairError("candidate pair receipt ID does not match payload")
        expected = self.authorize_table(candidates, candidate_receipts)
        if expected != supplied:
            raise CandidatePairError("candidate pair receipt authority drift")
        return expected


def group_candidate_pairs(rows: Iterable[Mapping[str, Any]]) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for raw in rows:
        row = normalize_candidate_contract(raw)
        pair_id = str(row.get("pair_id") or "")
        if not pair_id:
            raise CandidatePairError("formal candidate row lacks pair_id")
        grouped.setdefault(pair_id, []).append(row)
    output: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for pair_id, members in sorted(grouped.items()):
        primaries = [row for row in members if not bool(row.get("is_matched_control"))]
        controls = [row for row in members if bool(row.get("is_matched_control"))]
        if len(primaries) != 1 or len(controls) != 1 or len(members) != 2:
            raise CandidatePairError(f"pair must contain one primary and one control: {pair_id}")
        primary, control = attach_pair_contract(
            primaries[0],
            controls[0],
            control_constructor_id=str(primaries[0].get("control_constructor_id") or "") or None,
        )
        if str(primary["pair_id"]) != pair_id:
            raise CandidatePairError(f"pair identity drift: supplied={pair_id} expected={primary['pair_id']}")
        output.append((primary, control))
    return output


def flatten_candidate_pairs(
    pairs: Sequence[tuple[Mapping[str, Any], Mapping[str, Any]]],
) -> list[dict[str, Any]]:
    return [dict(member) for primary, control in pairs for member in (primary, control)]


def select_candidate_pairs(
    rows: Iterable[Mapping[str, Any]],
    *,
    primary_ids: Iterable[str] | None = None,
    pair_limit: int | None = None,
) -> list[dict[str, Any]]:
    selected_ids = None if primary_ids is None else {str(value) for value in primary_ids}
    pairs = group_candidate_pairs(rows)
    if selected_ids is not None:
        pairs = [pair for pair in pairs if str(pair[0]["candidate_id"]) in selected_ids]
        found = {str(pair[0]["candidate_id"]) for pair in pairs}
        if found != selected_ids:
            raise CandidatePairError(f"selected primary IDs lack authorized pairs: {sorted(selected_ids - found)}")
    if pair_limit is not None and int(pair_limit) > 0:
        pairs = pairs[: int(pair_limit)]
    return flatten_candidate_pairs(pairs)


def partition_candidate_pairs(
    rows: Iterable[Mapping[str, Any]],
    *,
    worker_count: int,
) -> list[list[dict[str, Any]]]:
    """Deterministically assign whole pairs while every worker reads all shards."""

    pairs = group_candidate_pairs(rows)
    workers = max(1, min(int(worker_count), len(pairs)))
    chunks: list[list[tuple[dict[str, Any], dict[str, Any]]]] = [[] for _ in range(workers)]
    for index, pair in enumerate(pairs):
        chunks[index % workers].append(pair)
    return [flatten_candidate_pairs(chunk) for chunk in chunks if chunk]


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _coordinate(row: Mapping[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(row.get("shard_index") or ""),
        str(row.get("trade_time") or ""),
        str(row.get("horizon_min") or ""),
        str(row.get("split") or ""),
    )


def _support_coordinate(row: Mapping[str, Any]) -> tuple[str, str, str, str, str, int] | None:
    eligible_identity = str(row.get("eligible_code_identity") or "")
    try:
        eligible_count = int(float(row.get("eligible_code_count") or 0))
    except (TypeError, ValueError):
        eligible_count = 0
    if not eligible_identity or eligible_count <= 0:
        return None
    return (*_coordinate(row), eligible_identity, eligible_count)


def _behavior_identity(rows: Sequence[Mapping[str, Any]]) -> str:
    """Hash portfolio behavior, deliberately excluding signal magnitude."""

    payload = []
    for row in sorted(rows, key=_coordinate):
        payload.append(
            {
                "coordinate": _coordinate(row),
                "long_count": int(float(row.get("long_count") or 0)),
                "short_count": int(float(row.get("short_count") or 0)),
                "one_way_turnover": _finite(row.get("one_way_turnover")),
                "eligible_code_identity": str(row.get("eligible_code_identity") or ""),
                "selected_code_identity": str(row.get("selected_code_identity") or ""),
                "portfolio_weight_identity": str(row.get("portfolio_weight_identity") or ""),
            }
        )
    return stable_hash(payload)


def _signal_value_identity(rows: Sequence[Mapping[str, Any]]) -> str:
    return stable_hash(
        [
            {
                "coordinate": _coordinate(row),
                "top_signal_mean": _finite(row.get("top_signal_mean")),
                "bottom_signal_mean": _finite(row.get("bottom_signal_mean")),
            }
            for row in sorted(rows, key=_coordinate)
        ]
    )


def build_pair_evaluation_rows(
    *,
    candidates: Iterable[Mapping[str, Any]],
    candidate_receipts: Iterable[Mapping[str, Any]],
    pair_receipts: Iterable[Mapping[str, Any]],
    reward_rows: Iterable[Mapping[str, Any]],
    portfolio_rows_by_expression_hash: Mapping[str, Sequence[Mapping[str, Any]]],
    reward_atom_rows: Iterable[Mapping[str, Any]] = (),
    evaluator_invocation_counts: Mapping[str, int] | None = None,
) -> list[dict[str, Any]]:
    pairs = group_candidate_pairs(candidates)
    candidate_receipt_by_id = {str(row.get("candidate_id") or ""): dict(row) for row in candidate_receipts}
    pair_receipt_by_id = {str(row.get("pair_id") or ""): dict(row) for row in pair_receipts}
    reward_by_id = {str(row.get("candidate_id") or ""): dict(row) for row in reward_rows}
    atoms = [dict(row) for row in reward_atom_rows]
    invocation_counts = {str(key): int(value) for key, value in (evaluator_invocation_counts or {}).items()}
    output: list[dict[str, Any]] = []
    for primary, control in pairs:
        pair_id = str(primary["pair_id"])
        pair_receipt = pair_receipt_by_id.get(pair_id)
        primary_receipt = candidate_receipt_by_id.get(str(primary["candidate_id"]))
        control_receipt = candidate_receipt_by_id.get(str(control["candidate_id"]))
        primary_reward = reward_by_id.get(str(primary["candidate_id"]))
        control_reward = reward_by_id.get(str(control["candidate_id"]))
        blockers: list[str] = []
        if pair_receipt is None:
            blockers.append("pair_receipt_missing")
        if primary_receipt is None:
            blockers.append("primary_receipt_missing")
        if control_receipt is None:
            blockers.append("control_receipt_missing")
        if primary_reward is None:
            blockers.append("primary_not_evaluated")
        if control_reward is None:
            blockers.append("control_not_evaluated")

        primary_rows = list(portfolio_rows_by_expression_hash.get(str(primary.get("expression_hash") or ""), ()))
        control_rows = list(portfolio_rows_by_expression_hash.get(str(control.get("expression_hash") or ""), ()))
        primary_support = [_support_coordinate(row) for row in primary_rows]
        control_support = [_support_coordinate(row) for row in control_rows]
        if any(coordinate is None for coordinate in primary_support + control_support):
            blockers.append("eligible_support_identity_missing")
        primary_coordinates = {coordinate for coordinate in primary_support if coordinate is not None}
        control_coordinates = {coordinate for coordinate in control_support if coordinate is not None}
        union = primary_coordinates | control_coordinates
        overlap = len(primary_coordinates & control_coordinates) / max(1, len(union))
        if not primary_coordinates or not control_coordinates:
            blockers.append("empty_pair_support")
        elif primary_coordinates != control_coordinates:
            blockers.append("pair_support_coordinates_mismatch")

        primary_behavior = _behavior_identity(primary_rows)
        control_behavior = _behavior_identity(control_rows)
        primary_signal_identity = _signal_value_identity(primary_rows)
        control_signal_identity = _signal_value_identity(control_rows)
        allow_behavior_equivalence = bool((pair_receipt or {}).get("allow_behavior_equivalence", False))
        if primary_receipt and control_receipt:
            if str(primary_receipt.get("exact_identity") or "") == str(control_receipt.get("exact_identity") or ""):
                blockers.append("control_exact_identity_equals_primary")
        if primary_rows and control_rows and primary_behavior == control_behavior and not allow_behavior_equivalence:
            blockers.append("control_behavior_identity_equals_primary")

        primary_signal_spread = [
            abs(float(row["top_signal_mean"]) - float(row["bottom_signal_mean"]))
            for row in primary_rows
            if _finite(row.get("top_signal_mean")) is not None and _finite(row.get("bottom_signal_mean")) is not None
        ]
        control_signal_spread = [
            abs(float(row["top_signal_mean"]) - float(row["bottom_signal_mean"]))
            for row in control_rows
            if _finite(row.get("top_signal_mean")) is not None and _finite(row.get("bottom_signal_mean")) is not None
        ]
        if not primary_signal_spread or max(primary_signal_spread, default=0.0) <= 1e-15:
            blockers.append("primary_signal_empty_or_constant")
        if not control_signal_spread or max(control_signal_spread, default=0.0) <= 1e-15:
            blockers.append("control_signal_empty_or_constant")
        primary_invocations = invocation_counts.get(str(primary["candidate_id"]), 0)
        control_invocations = invocation_counts.get(str(control["candidate_id"]), 0)
        if primary_invocations <= 0:
            blockers.append("primary_evaluator_not_invoked")
        if control_invocations <= 0:
            blockers.append("control_evaluator_not_invoked")

        primary_value = _finite((primary_reward or {}).get("optimizer_reward"))
        control_value = _finite((control_reward or {}).get("optimizer_reward"))
        primary_turnover = _finite((primary_reward or {}).get("train_mean_one_way_turnover"))
        control_turnover = _finite((control_reward or {}).get("train_mean_one_way_turnover"))
        primary_rank_ic = _finite((primary_reward or {}).get("train_rank_ic_mean"))
        control_rank_ic = _finite((control_reward or {}).get("train_rank_ic_mean"))
        if primary_value is None or control_value is None:
            blockers.append("matched_train_reward_unavailable")
        matched_increment = None if primary_value is None or control_value is None else primary_value - control_value
        turnover_increment = (
            None if primary_turnover is None or control_turnover is None else primary_turnover - control_turnover
        )
        rank_ic_increment = None if primary_rank_ic is None or control_rank_ic is None else primary_rank_ic - control_rank_ic
        status = "PAIR_EVALUATED" if not blockers else "PAIR_EVALUATION_BLOCKED"
        pair_turnover_metric = (
            None if primary_turnover is None or control_turnover is None else max(primary_turnover, control_turnover)
        )
        pair_support_metric = overlap
        pair_rank_ic_metric = rank_ic_increment
        pair_feedback_blockers = set(blockers)
        if status != "PAIR_EVALUATED":
            pair_feedback_blockers.add("pair_evaluation_not_completed")
        if not str((primary_receipt or {}).get("receipt_hash") or ""):
            pair_feedback_blockers.add("primary_receipt_hash_missing")
        if not str((control_receipt or {}).get("receipt_hash") or ""):
            pair_feedback_blockers.add("control_receipt_hash_missing")
        if not str((pair_receipt or {}).get("pair_receipt_hash") or ""):
            pair_feedback_blockers.add("pair_receipt_hash_missing")
        if matched_increment is None:
            pair_feedback_blockers.add("matched_train_increment_not_finite")
        elif matched_increment <= 0.0:
            pair_feedback_blockers.add("matched_train_increment_nonpositive")
        if pair_support_metric != 1.0:
            pair_feedback_blockers.add("pair_support_mismatch")
        if pair_turnover_metric is None:
            pair_feedback_blockers.add("pair_turnover_metric_not_finite")
        if str(primary.get("route_id") or "") in PAIR_RANK_IC_REQUIRED_ROUTES and pair_rank_ic_metric is None:
            pair_feedback_blockers.add("pair_rank_ic_metric_not_finite")
        pair_feedback_decision = (
            PAIR_TRAIN_FEEDBACK_READY
            if not pair_feedback_blockers
            else PAIR_TRAIN_FEEDBACK_BLOCKED
        )
        primary_atoms = sum(str(row.get("candidate_id") or "") == str(primary["candidate_id"]) for row in atoms)
        control_atoms = sum(str(row.get("candidate_id") or "") == str(control["candidate_id"]) for row in atoms)
        row = {
            **dict(primary_reward or {}),
            "candidate_id": str(primary["candidate_id"]),
            "pair_member_role": "PRIMARY",
            "pair_id": pair_id,
            "primary_candidate_id": str(primary["candidate_id"]),
            "control_candidate_id": str(control["candidate_id"]),
            "primary_expression": str(primary.get("expression") or ""),
            "control_expression": str(control.get("expression") or ""),
            "primary_expression_hash": str(primary.get("expression_hash") or ""),
            "control_expression_hash": str(control.get("expression_hash") or ""),
            "primary_receipt_hash": str((primary_receipt or {}).get("receipt_hash") or ""),
            "control_receipt_hash": str((control_receipt or {}).get("receipt_hash") or ""),
            "pair_receipt_hash": str((pair_receipt or {}).get("pair_receipt_hash") or ""),
            "route_id": str(primary.get("route_id") or ""),
            "control_constructor_id": str(primary.get("control_constructor_id") or ""),
            "pair_evaluation_status": status,
            "pair_evaluation_blockers": "|".join(sorted(set(blockers))),
            "pair_train_reward": matched_increment,
            "pair_train_reward_decision": pair_feedback_decision,
            "pair_train_reward_blockers": "|".join(sorted(pair_feedback_blockers)),
            "pair_turnover_metric": pair_turnover_metric,
            "pair_support_metric": pair_support_metric,
            "pair_rank_ic_metric": pair_rank_ic_metric,
            "primary_train_reward": primary_value,
            "control_train_reward": control_value,
            "matched_train_increment": matched_increment,
            "primary_turnover": primary_turnover,
            "control_turnover": control_turnover,
            "matched_turnover_increment": turnover_increment,
            "primary_rank_ic": primary_rank_ic,
            "control_rank_ic": control_rank_ic,
            "matched_rank_ic_increment": rank_ic_increment,
            "primary_behavior_identity": primary_behavior,
            "control_behavior_identity": control_behavior,
            "primary_signal_value_identity": primary_signal_identity,
            "control_signal_value_identity": control_signal_identity,
            "pair_support_overlap": overlap,
            "primary_support_coordinate_count": len(primary_coordinates),
            "control_support_coordinate_count": len(control_coordinates),
            "primary_reward_atom_count": primary_atoms,
            "control_reward_atom_count": control_atoms,
            "primary_evaluator_invocation_count": primary_invocations,
            "control_evaluator_invocation_count": control_invocations,
            "primary_standalone_train_reward_decision": str(
                (primary_reward or {}).get("train_reward_decision") or ""
            ),
            "primary_standalone_train_reward_blockers": str(
                (primary_reward or {}).get("train_reward_blockers") or ""
            ),
            "primary_standalone_train_mean_one_way_turnover": primary_turnover,
            "optimizer_reward": matched_increment if pair_feedback_decision == PAIR_TRAIN_FEEDBACK_READY else "",
            "train_reward": matched_increment if pair_feedback_decision == PAIR_TRAIN_FEEDBACK_READY else "",
            "optimizer_reward_source": MATCHED_OPTIMIZER_REWARD_SOURCE,
            "optimizer_reward_metric": MATCHED_OPTIMIZER_REWARD_METRIC,
            "optimizer_reward_split": "train",
            "control_independent_vote": False,
            "control_independent_memory": False,
        }
        row.pop("train_reward_decision", None)
        row.pop("train_reward_blockers", None)
        row.pop("train_mean_one_way_turnover", None)
        output.append(row)
    return sorted(output, key=lambda row: str(row["pair_id"]))


def write_pair_receipt_table(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        "\n".join(json.dumps(dict(row), ensure_ascii=False, sort_keys=True) for row in rows) + "\n",
        encoding="utf-8",
    )


def read_pair_receipt_table(path: Path) -> list[dict[str, Any]]:
    target = Path(path)
    if not target.is_file():
        raise CandidatePairError(f"candidate pair receipt table does not exist: {target}")
    rows = [json.loads(line) for line in target.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not rows:
        raise CandidatePairError(f"candidate pair receipt table is empty: {target}")
    return rows
