from __future__ import annotations

"""Prove that a frozen 1,024 wave retains the exact historical 256 identities."""

import argparse
import csv
import hashlib
import json
import os
import uuid
from pathlib import Path
from typing import Any, Mapping, Sequence


HISTORICAL_PAIR_COUNT = 256
FREEZE_PAIR_COUNT = 1024
PAIR_RECEIPT_SCHEMA_VERSION = "cn_candidate_pair_receipt_v2"

PAIR_SCALAR_FIELDS = (
    "allow_behavior_equivalence",
    "route_id",
    "control_constructor_id",
    "primary_vote_policy",
    "control_vote_policy",
    "support_unit",
    "pair_clock_alignment_policy",
    "mapping_portfolio_contract",
    "pair_authorization_status",
)
PAIR_LIST_FIELDS = (
    "primary_field_ids",
    "control_field_ids",
    "primary_observable_time_contract",
    "control_observable_time_contract",
    "primary_pit_source_lag_contract",
    "control_pit_source_lag_contract",
    "representation_ids",
    "source_field_ids",
)

CANDIDATE_REQUIRED_TEXT_FIELDS = (
    "candidate_id",
    "pair_id",
    "pair_member_role",
    "route_id",
    "canonical_expression",
    "exact_identity",
    "clock_contract",
    "maturity_contract",
    "support_unit",
    "seed",
    "skeleton_id",
    "pair_mapping_portfolio_contract",
    "pair_maturity_alignment_policy",
    "pair_support_alignment_policy",
)
CANDIDATE_REQUIRED_LIST_FIELDS = (
    "access_roles",
    "field_ids",
    "declared_field_ids",
    "representation_ids",
    "source_field_ids",
)
CANDIDATE_REQUIRED_BOOL_FIELDS = (
    "legal",
    "is_matched_control",
    "uses_future_revision",
)
CANDIDATE_OPTIONAL_TEXT_FIELDS = (
    "canonical_identity",
    "matched_control_id",
    "control_constructor_id",
    "control_ablation_rule",
    "generator_arm",
    "generator_version",
    "proposal_origin",
    "operator_family",
    "outer_mapping",
    "vote_policy",
    "maturity_rule",
    "episode_policy",
    "entity_scope",
    "claimed_state_field_id",
    "state_source_expression",
    "state_support_unit",
    "market_vote_policy",
)
CANDIDATE_OPTIONAL_LIST_FIELDS = (
    "condition_field_ids",
    "input_roles",
    "operator_paths",
)
CANDIDATE_OPTIONAL_BOOL_FIELDS = (
    "allow_behavior_equivalence",
    "requires_intrabar_order",
    "maturity_contract_registered",
)

IGNORED_REFREEZE_FIELDS = (
    "round_id",
    "preflight_ordinal",
    "pair_receipt_id",
    "pair_receipt_hash",
    "primary_receipt_hash",
    "control_receipt_hash",
    "expression_hash",
)


class SemanticSubsetError(ValueError):
    pass


def _text(value: Any, label: str, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str):
        raise SemanticSubsetError(f"{label} must be text")
    value = value.strip()
    if not value and not allow_empty:
        raise SemanticSubsetError(f"{label} must be non-empty")
    return value


def _boolean(value: Any, label: str) -> bool:
    if type(value) is bool:
        return value
    if isinstance(value, str) and value.strip().lower() in {"true", "false"}:
        return value.strip().lower() == "true"
    raise SemanticSubsetError(f"{label} must be boolean")


def _json_value(value: Any, label: str) -> Any:
    if isinstance(value, str):
        value = value.strip()
        if not value:
            raise SemanticSubsetError(f"{label} must be non-empty JSON")
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise SemanticSubsetError(f"{label} is invalid JSON") from exc
    try:
        encoded = json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
        )
    except (TypeError, ValueError) as exc:
        raise SemanticSubsetError(f"{label} is not canonical JSON") from exc
    return json.loads(encoded)


def _set_like_list(value: Any, label: str, *, allow_empty: bool = False) -> list[str]:
    value = _json_value(value, label)
    if not isinstance(value, list) or (not value and not allow_empty):
        raise SemanticSubsetError(f"{label} must be a JSON array")
    items = [_text(item, label) for item in value]
    # Multiplicity can be semantic (for example two representations may map to
    # the same source field), while producer order is not.
    return sorted(items)


def _pair_payload(row: Mapping[str, Any], label: str) -> dict[str, Any]:
    if row.get("pair_receipt_schema_version") != PAIR_RECEIPT_SCHEMA_VERSION:
        raise SemanticSubsetError(f"{label} pair receipt schema drift")
    payload: dict[str, Any] = {
        "pair_id": _text(row.get("pair_id"), f"{label}.pair_id"),
        "primary_candidate_id": _text(
            row.get("primary_candidate_id"), f"{label}.primary_candidate_id"
        ),
        "control_candidate_id": _text(
            row.get("control_candidate_id"), f"{label}.control_candidate_id"
        ),
    }
    for field in PAIR_SCALAR_FIELDS:
        value = row.get(field)
        payload[field] = (
            _boolean(value, f"{label}.{field}")
            if field == "allow_behavior_equivalence"
            else _text(value, f"{label}.{field}")
        )
    for field in PAIR_LIST_FIELDS:
        payload[field] = _set_like_list(row.get(field), f"{label}.{field}")
    contract = row.get("control_constructor_contract")
    contract = _json_value(contract, f"{label}.control_constructor_contract")
    if not isinstance(contract, dict) or not contract:
        raise SemanticSubsetError(f"{label}.control_constructor_contract must be an object")
    payload["control_constructor_contract"] = contract
    return payload


def _candidate_payload(row: Mapping[str, Any], label: str) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for field in CANDIDATE_REQUIRED_TEXT_FIELDS:
        payload[field] = _text(row.get(field), f"{label}.{field}")
    if payload["pair_member_role"] not in {"PRIMARY", "CONTROL"}:
        raise SemanticSubsetError(f"{label}.pair_member_role is invalid")
    try:
        payload["seed"] = int(payload["seed"])
    except ValueError as exc:
        raise SemanticSubsetError(f"{label}.seed must be an integer") from exc
    for field in CANDIDATE_REQUIRED_LIST_FIELDS:
        payload[field] = _set_like_list(row.get(field), f"{label}.{field}")
    for field in CANDIDATE_REQUIRED_BOOL_FIELDS:
        payload[field] = _boolean(row.get(field), f"{label}.{field}")
    if payload["is_matched_control"] != (payload["pair_member_role"] == "CONTROL"):
        raise SemanticSubsetError(f"{label} role/control flag mismatch")
    for field in CANDIDATE_OPTIONAL_TEXT_FIELDS:
        if field in row:
            payload[field] = _text(row.get(field), f"{label}.{field}", allow_empty=True)
    for field in CANDIDATE_OPTIONAL_LIST_FIELDS:
        if field in row:
            payload[field] = _set_like_list(
                row.get(field), f"{label}.{field}", allow_empty=True
            )
    for field in CANDIDATE_OPTIONAL_BOOL_FIELDS:
        if field in row and _text(str(row.get(field)), f"{label}.{field}", allow_empty=True):
            payload[field] = _boolean(row.get(field), f"{label}.{field}")
    return payload


def _index(
    rows: Sequence[Mapping[str, Any]],
    *,
    key: str,
    expected_count: int,
    label: str,
    payload_builder: Any,
) -> dict[str, dict[str, Any]]:
    if len(rows) != expected_count:
        raise SemanticSubsetError(
            f"{label} must contain exactly {expected_count} rows; observed {len(rows)}"
        )
    output: dict[str, dict[str, Any]] = {}
    for ordinal, row in enumerate(rows, start=1):
        payload = payload_builder(row, f"{label}[{ordinal}]")
        identity = _text(payload.get(key), f"{label}[{ordinal}].{key}")
        if identity in output:
            raise SemanticSubsetError(f"{label} duplicate {key}: {identity}")
        output[identity] = payload
    return output


def _validate_pair_members(
    pairs: Mapping[str, Mapping[str, Any]],
    candidates: Mapping[str, Mapping[str, Any]],
    *,
    label: str,
) -> None:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for candidate in candidates.values():
        grouped.setdefault(str(candidate["pair_id"]), []).append(candidate)
    if set(grouped) != set(pairs):
        raise SemanticSubsetError(f"{label} pair/candidate membership set drift")
    for pair_id, pair in pairs.items():
        members = grouped[pair_id]
        roles = {str(row["pair_member_role"]): row for row in members}
        if len(members) != 2 or set(roles) != {"PRIMARY", "CONTROL"}:
            raise SemanticSubsetError(f"{label}.{pair_id} must contain one primary and one control")
        if roles["PRIMARY"]["candidate_id"] != pair["primary_candidate_id"]:
            raise SemanticSubsetError(f"{label}.{pair_id} primary candidate identity drift")
        if roles["CONTROL"]["candidate_id"] != pair["control_candidate_id"]:
            raise SemanticSubsetError(f"{label}.{pair_id} control candidate identity drift")


def _digest(values: Sequence[str]) -> str:
    return hashlib.sha256(
        json.dumps(sorted(values), ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def verify_historical_subset(
    historical_pair_rows: Sequence[Mapping[str, Any]],
    freeze_pair_rows: Sequence[Mapping[str, Any]],
    historical_candidate_rows: Sequence[Mapping[str, Any]],
    freeze_candidate_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    historical_pairs = _index(
        historical_pair_rows,
        key="pair_id",
        expected_count=HISTORICAL_PAIR_COUNT,
        label="historical_pairs",
        payload_builder=_pair_payload,
    )
    freeze_pairs = _index(
        freeze_pair_rows,
        key="pair_id",
        expected_count=FREEZE_PAIR_COUNT,
        label="freeze_pairs",
        payload_builder=_pair_payload,
    )
    historical_candidates = _index(
        historical_candidate_rows,
        key="candidate_id",
        expected_count=2 * HISTORICAL_PAIR_COUNT,
        label="historical_candidates",
        payload_builder=_candidate_payload,
    )
    freeze_candidates = _index(
        freeze_candidate_rows,
        key="candidate_id",
        expected_count=2 * FREEZE_PAIR_COUNT,
        label="freeze_candidates",
        payload_builder=_candidate_payload,
    )
    _validate_pair_members(historical_pairs, historical_candidates, label="historical_256")
    _validate_pair_members(freeze_pairs, freeze_candidates, label="freeze_1024")

    missing_pairs = sorted(set(historical_pairs) - set(freeze_pairs))
    missing_candidates = sorted(set(historical_candidates) - set(freeze_candidates))
    pair_mismatches = sorted(
        pair_id
        for pair_id in set(historical_pairs) & set(freeze_pairs)
        if historical_pairs[pair_id] != freeze_pairs[pair_id]
    )
    candidate_mismatches = sorted(
        candidate_id
        for candidate_id in set(historical_candidates) & set(freeze_candidates)
        if historical_candidates[candidate_id] != freeze_candidates[candidate_id]
    )
    passed = not (missing_pairs or missing_candidates or pair_mismatches or candidate_mismatches)
    return {
        "schema_version": "cn_phase3cm_historical_256_semantic_subset_receipt_v2",
        "status": (
            "CN_PHASE3CM_HISTORICAL_256_SEMANTIC_SUBSET_PASS"
            if passed
            else "CN_PHASE3CM_HISTORICAL_256_SEMANTIC_SUBSET_FAIL_CLOSED"
        ),
        "historical_pair_count": len(historical_pairs),
        "freeze_pair_count": len(freeze_pairs),
        "historical_candidate_count": len(historical_candidates),
        "freeze_candidate_count": len(freeze_candidates),
        "pair_identity_subset_exact": not missing_pairs,
        "candidate_identity_subset_exact": not missing_candidates,
        "pair_semantics_exact": not pair_mismatches,
        "candidate_semantics_exact": not candidate_mismatches,
        "historical_pair_identity_digest": _digest(list(historical_pairs)),
        "historical_candidate_identity_digest": _digest(list(historical_candidates)),
        "missing_pair_ids": missing_pairs[:20],
        "missing_candidate_ids": missing_candidates[:20],
        "pair_semantic_mismatch_ids": pair_mismatches[:20],
        "candidate_semantic_mismatch_ids": candidate_mismatches[:20],
        "ignored_refreeze_fields": list(IGNORED_REFREEZE_FIELDS),
    }


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8-sig") as handle:
        for ordinal, raw in enumerate(handle, start=1):
            if not raw.strip():
                raise SemanticSubsetError(f"{path}: blank JSONL row {ordinal}")
            value = json.loads(raw)
            if not isinstance(value, dict):
                raise SemanticSubsetError(f"{path}: JSONL row {ordinal} must be an object")
            rows.append(value)
    return rows


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise SemanticSubsetError(f"{path}: CSV header is missing")
        return [dict(row) for row in reader]


def _load_root(root: Path) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    root = Path(root).resolve()
    pairs: list[dict[str, Any]] = []
    candidates: list[dict[str, str]] = []
    for clock in ("active", "session"):
        pairs.extend(_read_jsonl(root / f"preflight_{clock}_pair_receipts.jsonl"))
        candidates.extend(_read_csv(root / f"preflight_{clock}_candidates.csv"))
    return pairs, candidates


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--historical-root", type=Path, required=True)
    parser.add_argument("--freeze-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        historical_pairs, historical_candidates = _load_root(args.historical_root)
        freeze_pairs, freeze_candidates = _load_root(args.freeze_root)
        receipt = verify_historical_subset(
            historical_pairs,
            freeze_pairs,
            historical_candidates,
            freeze_candidates,
        )
    except (OSError, json.JSONDecodeError, SemanticSubsetError, TypeError, ValueError) as exc:
        receipt = {
            "schema_version": "cn_phase3cm_historical_256_semantic_subset_receipt_v2",
            "status": "CN_PHASE3CM_HISTORICAL_256_SEMANTIC_SUBSET_FAIL_CLOSED",
            "errors": [str(exc)],
        }
    _write_json_atomic(args.output.resolve(), receipt)
    print(json.dumps(receipt, ensure_ascii=False, sort_keys=True))
    return 0 if receipt["status"] == "CN_PHASE3CM_HISTORICAL_256_SEMANTIC_SUBSET_PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
