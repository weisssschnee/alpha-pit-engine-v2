from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from typing import Any, Mapping, Sequence

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts import run_cn_finalist_replay_then_oos as base
from our_system_phase2.services.a_share_executable_replay import (
    AShareCorporateActionPolicy,
    AShareExecutionPolicy,
    AShareFeeSchedule,
    AShareUniversePolicy,
    ENDING_BOOK_FINAL_CLOSE_MARK_TO_MARKET,
    run_a_share_long_only_replay,
)


SCHEMA_VERSION = "cn_finalist_mark_to_market_replay_v1"
CANDIDATE_COMPLETE = "CANDIDATE_MARK_TO_MARKET_COMPLETE"
CANDIDATE_BLOCKED = "CANDIDATE_MARK_TO_MARKET_BLOCKED"
PAIR_COMPLETE = "PAIR_MARK_TO_MARKET_COMPLETE"
PAIR_BLOCKED = "PAIR_MARK_TO_MARKET_BLOCKED"


def _finite(value: Any) -> float | None:
    return base._finite(value)


def _pair_results(
    candidates: pd.DataFrame,
    candidate_rows: Sequence[Mapping[str, Any]],
) -> pd.DataFrame:
    result_by_id = {
        str(row["candidate_id"]): dict(row) for row in candidate_rows
    }
    rows: list[dict[str, Any]] = []
    for pair_id, group in candidates.groupby("pair_id", sort=False):
        members = {
            str(row["pair_member_role"]): row
            for row in group.to_dict(orient="records")
        }
        primary = members["PRIMARY"]
        control = members["CONTROL"]
        primary_result = result_by_id[str(primary["candidate_id"])]
        control_result = result_by_id[str(control["candidate_id"])]
        primary_status = str(
            primary_result["candidate_mark_to_market_status"]
        )
        control_status = str(
            control_result["candidate_mark_to_market_status"]
        )
        pair_complete = (
            primary_status == CANDIDATE_COMPLETE
            and control_status == CANDIDATE_COMPLETE
        )
        primary_reward = _finite(
            primary_result.get("mark_to_market_net_reward")
        )
        control_reward = _finite(
            control_result.get("mark_to_market_net_reward")
        )
        rows.append(
            {
                "pair_id": str(pair_id),
                "route_id": str(primary["route_id"]),
                "primary_candidate_id": str(primary["candidate_id"]),
                "control_candidate_id": str(control["candidate_id"]),
                "primary_candidate_mark_to_market_status": primary_status,
                "control_candidate_mark_to_market_status": control_status,
                "primary_mark_to_market_net_reward": primary_reward,
                "control_mark_to_market_net_reward": control_reward,
                "mark_to_market_net_increment": (
                    primary_reward - control_reward
                    if pair_complete
                    and primary_reward is not None
                    and control_reward is not None
                    else None
                ),
                "primary_ending_holdings_weight": _finite(
                    primary_result.get("ending_holdings_weight")
                ),
                "control_ending_holdings_weight": _finite(
                    control_result.get("ending_holdings_weight")
                ),
                "primary_mean_one_way_turnover": _finite(
                    primary_result.get("a_share_mean_one_way_turnover")
                ),
                "control_mean_one_way_turnover": _finite(
                    control_result.get("a_share_mean_one_way_turnover")
                ),
                "primary_blocker_code": primary_result.get("blocker_code"),
                "control_blocker_code": control_result.get("blocker_code"),
                "pair_mark_to_market_status": (
                    PAIR_COMPLETE if pair_complete else PAIR_BLOCKED
                ),
                "evidence_class": (
                    "FINAL_CLOSE_MARK_TO_MARKET_DIAGNOSTIC_ONLY"
                ),
                "economic_claim_authorized": False,
                "promotion_authorized": False,
            }
        )
    return pd.DataFrame(rows)


def _candidate_receipt(
    *,
    candidate: Mapping[str, Any],
    result: Mapping[str, Any],
    input_data_sha256: str,
    source_code_sha256: str,
    train_read_count: int,
    selection_payload_sha256: str,
    strict_replay_closure_sha256: str,
    blocked: bool,
) -> dict[str, Any]:
    diagnostic_reward = float(result["a_share_executable_net_reward"])
    return {
        "schema_version": "cn_finalist_mark_to_market_receipt_v1",
        "status": (
            "MARK_TO_MARKET_BLOCKED_NO_EXECUTABLE_FILLS"
            if blocked
            else "MARK_TO_MARKET_DIAGNOSTIC_READY"
        ),
        "candidate_id": str(candidate["candidate_id"]),
        "pair_id": str(candidate["pair_id"]),
        "pair_member_role": str(candidate["pair_member_role"]),
        "route_id": str(candidate["route_id"]),
        "exact_identity": str(candidate["exact_identity"]),
        "selection_payload_sha256": selection_payload_sha256,
        "strict_replay_closure_sha256": strict_replay_closure_sha256,
        "input_data_sha256": input_data_sha256,
        "source_code_sha256": source_code_sha256,
        "ending_book_policy": ENDING_BOOK_FINAL_CLOSE_MARK_TO_MARKET,
        "no_fabricated_terminal_sale": True,
        "terminal_sale_fee_applied": False,
        "final_mark_source": "FINAL_PIT_CLOSE",
        "diagnostic_net_reward": diagnostic_reward,
        "mark_to_market_net_reward": None if blocked else diagnostic_reward,
        "train_read_count": train_read_count,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "trade_count": int(result["trade_count"]),
        "fill_count": int(result["fill_count"]),
        "blocked_buy_count": int(result["blocked_buy_count"]),
        "blocked_sell_count": int(result["blocked_sell_count"]),
        "total_fees_cny": float(result["total_fees_cny"]),
        "ending_nav_cny": float(result["ending_nav_cny"]),
        "ending_cash_cny": float(result["ending_cash_cny"]),
        "ending_holding_count": int(result["ending_holding_count"]),
        "ending_holdings": list(result["ending_holdings"]),
        "ending_holdings_market_value_cny": float(
            result["ending_holdings_market_value_cny"]
        ),
        "ending_holdings_weight": _finite(
            result["ending_holdings_weight"]
        ),
        "a_share_mean_one_way_turnover": _finite(
            result["a_share_mean_one_way_turnover"]
        ),
        "fee_schedule_sha256": str(result["fee_schedule_sha256"]),
        "execution_policy_sha256": str(
            result["execution_policy_sha256"]
        ),
        "corporate_action_policy_sha256": str(
            result["corporate_action_policy_sha256"]
        ),
        "proofs": dict(result["proofs"]),
        "blocker_code": "NO_EXECUTABLE_FILLS" if blocked else None,
        "research_replay_only": True,
        "economic_claim_authorized": False,
        "promotion_authorized": False,
        "successor_search_authorized": False,
        "feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
    }


def replay_mark_to_market(
    *,
    freeze_path: Path,
    train_field_root: Path,
    strict_replay_root: Path,
    output_root: Path,
) -> dict[str, Any]:
    initial_free_memory = base._host_and_memory_gate()
    freeze_path = Path(freeze_path).resolve()
    train_field_root = Path(train_field_root).resolve()
    strict_replay_root = Path(strict_replay_root).resolve()
    output_root = Path(output_root).resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    freeze = base._load_freeze(freeze_path)
    frozen_root = freeze_path.parents[1]
    candidate_path = base._resolved_artifact(
        frozen_root,
        freeze["candidate_artifact"],
    )
    contract_path = base._resolved_artifact(
        frozen_root,
        freeze["execution_contract_artifact"],
    )
    candidates = pd.read_parquet(candidate_path).where(pd.notna, None)
    if candidates["candidate_id"].astype(str).tolist() != list(
        freeze["candidate_ids"]
    ):
        raise RuntimeError("mark-to-market cohort order drift")

    strict_closure_path = strict_replay_root / "REPLAY_COMPLETE.json"
    strict_closure = base._read_json(strict_closure_path)
    base._verify_payload_hash(
        strict_closure,
        field="manifest_body_sha256",
        label="strict replay closure",
    )
    if str(strict_closure.get("status")) != (
        "A_SHARE_REPLAY_CLOSED_IMMUTABLE"
    ):
        raise RuntimeError("strict replay closure status drift")
    if str(strict_closure.get("selection_payload_sha256")) != str(
        freeze["selection_payload_sha256"]
    ):
        raise RuntimeError("strict replay selection binding drift")
    strict_replay_closure_sha256 = base._sha256(strict_closure_path)

    contract = base._read_json(contract_path)
    base._verify_payload_hash(
        contract,
        field="contract_payload_sha256",
        label="replay/OOS execution contract",
    )
    split_hash = str(contract["split_manifest_sha256"])
    field_manifest, field_manifest_path = base._validate_sidecar(
        train_field_root,
        evaluation_role="train",
        split_hash=split_hash,
    )
    field_frame = base._load_field_frame(
        train_field_root,
        field_manifest,
    )
    observed_dates = set(field_frame["date"].dt.date.astype(str))
    split = pd.read_csv(contract["split_manifest"], dtype=str)
    train_dates = set(
        split.loc[split["split"].eq("train"), "trade_date"].astype(str)
    )
    if observed_dates != train_dates:
        raise RuntimeError("mark-to-market train calendar drift")

    session_manifest_path = Path(
        str(contract["session_authority_manifest"])
    ).resolve()
    if base._sha256(session_manifest_path) != str(
        contract["session_authority_manifest_sha256"]
    ):
        raise RuntimeError("session authority manifest hash drift")
    session_path = Path(str(contract["session_authority_path"])).resolve()
    session_authority = pd.read_parquet(session_path)
    master, observed_index, authority_index = (
        base._materialize_replay_master(field_frame, session_authority)
    )
    if set(master["date"].dt.date.astype(str)) != train_dates:
        raise RuntimeError("session authority train calendar drift")

    fee = AShareFeeSchedule(**dict(contract["fee_schedule"]))
    universe_raw = dict(contract["universe_policy"])
    universe_raw["allowed_exchanges"] = tuple(
        universe_raw["allowed_exchanges"]
    )
    universe = AShareUniversePolicy(**universe_raw)
    execution = AShareExecutionPolicy(**dict(contract["execution_policy"]))
    corporate = AShareCorporateActionPolicy(
        **dict(contract["corporate_action_policy"])
    )
    source_code_sha256 = base._stable_hash(
        {
            "driver": base._sha256(Path(__file__).resolve()),
            "kernel": base._sha256(
                Path(__file__).resolve().parents[1]
                / "src"
                / "our_system_phase2"
                / "services"
                / "a_share_executable_replay.py"
            ),
            "expression_evaluator": base._sha256(
                Path(__file__).resolve().parents[1]
                / "src"
                / "our_system_phase2"
                / "services"
                / "real_market_validation.py"
            ),
        }
    )
    input_data_sha256 = base._stable_hash(
        {
            "freeze_sha256": base._sha256(freeze_path),
            "train_field_manifest_sha256": base._sha256(
                field_manifest_path
            ),
            "session_authority_manifest_sha256": base._sha256(
                session_manifest_path
            ),
            "session_authority_sha256": base._sha256(session_path),
            "strict_replay_closure_sha256": (
                strict_replay_closure_sha256
            ),
            "ending_book_policy": (
                ENDING_BOOK_FINAL_CLOSE_MARK_TO_MARKET
            ),
        }
    )

    candidate_root = output_root / "candidates"
    candidate_root.mkdir(parents=True, exist_ok=True)
    candidate_rows: list[dict[str, Any]] = []
    receipts: list[dict[str, Any]] = []
    blockers: list[dict[str, Any]] = []
    for candidate in candidates.to_dict(orient="records"):
        candidate_id = str(candidate["candidate_id"])
        target = candidate_root / f"{candidate_id}.json"
        if target.is_file():
            row = base._read_json(target)
            if (
                str(row.get("candidate_id")) != candidate_id
                or str(row.get("input_data_sha256"))
                != input_data_sha256
                or str(row.get("status"))
                not in {
                    "CANDIDATE_MARK_TO_MARKET_COMPLETE_IMMUTABLE",
                    "CANDIDATE_MARK_TO_MARKET_BLOCKED_IMMUTABLE",
                }
            ):
                raise RuntimeError(
                    "mark-to-market candidate resume drift: "
                    f"{candidate_id}"
                )
            candidate_rows.append(dict(row["summary"]))
            receipts.append(dict(row["receipt"]))
            if row.get("blocker"):
                blockers.append(dict(row["blocker"]))
            continue

        signal = pd.to_numeric(
            base.evaluate_panel_expression(
                field_frame,
                str(candidate["expression"]),
                cache={},
                data_role="development",
            ),
            errors="coerce",
        )
        signal_by_coordinate = pd.Series(
            signal.to_numpy(),
            index=observed_index,
        )
        replay_frame = master.copy()
        replay_frame["signal"] = signal_by_coordinate.reindex(
            authority_index
        ).to_numpy()
        result = run_a_share_long_only_replay(
            replay_frame,
            fee_schedule=fee,
            universe_policy=universe,
            execution_policy=execution,
            corporate_action_policy=corporate,
            ending_book_policy=(
                ENDING_BOOK_FINAL_CLOSE_MARK_TO_MARKET
            ),
        )
        blocked = int(result["fill_count"]) <= 0
        receipt = _candidate_receipt(
            candidate=candidate,
            result=result,
            input_data_sha256=input_data_sha256,
            source_code_sha256=source_code_sha256,
            train_read_count=len(replay_frame),
            selection_payload_sha256=str(
                freeze["selection_payload_sha256"]
            ),
            strict_replay_closure_sha256=(
                strict_replay_closure_sha256
            ),
            blocked=blocked,
        )
        summary = {
            "candidate_id": candidate_id,
            "pair_id": str(candidate["pair_id"]),
            "pair_member_role": str(candidate["pair_member_role"]),
            "route_id": str(candidate["route_id"]),
            "exact_identity": str(candidate["exact_identity"]),
            "candidate_mark_to_market_status": (
                CANDIDATE_BLOCKED if blocked else CANDIDATE_COMPLETE
            ),
            "mark_to_market_net_reward": (
                None
                if blocked
                else float(result["a_share_executable_net_reward"])
            ),
            "diagnostic_net_reward": float(
                result["a_share_executable_net_reward"]
            ),
            "blocker_code": (
                "NO_EXECUTABLE_FILLS" if blocked else None
            ),
            "train_read_count": len(replay_frame),
            "trade_count": int(result["trade_count"]),
            "fill_count": int(result["fill_count"]),
            "blocked_buy_count": int(result["blocked_buy_count"]),
            "blocked_sell_count": int(result["blocked_sell_count"]),
            "total_fees_cny": float(result["total_fees_cny"]),
            "a_share_mean_one_way_turnover": _finite(
                result["a_share_mean_one_way_turnover"]
            ),
            "ending_nav_cny": float(result["ending_nav_cny"]),
            "ending_cash_cny": float(result["ending_cash_cny"]),
            "ending_holding_count": int(result["ending_holding_count"]),
            "ending_holdings": list(result["ending_holdings"]),
            "ending_holdings_market_value_cny": float(
                result["ending_holdings_market_value_cny"]
            ),
            "ending_holdings_weight": _finite(
                result["ending_holdings_weight"]
            ),
            "economic_claim_authorized": False,
            "promotion_authorized": False,
        }
        blocker = (
            {
                "schema_version": (
                    "cn_finalist_mark_to_market_blocker_v1"
                ),
                "candidate_id": candidate_id,
                "pair_id": str(candidate["pair_id"]),
                "pair_member_role": str(candidate["pair_member_role"]),
                "route_id": str(candidate["route_id"]),
                "exact_identity": str(candidate["exact_identity"]),
                "blocker_code": "NO_EXECUTABLE_FILLS",
                "input_data_sha256": input_data_sha256,
                "fail_closed": True,
                "economic_claim_authorized": False,
                "promotion_authorized": False,
            }
            if blocked
            else None
        )
        base._write_json(
            target,
            {
                "schema_version": (
                    "cn_finalist_mark_to_market_candidate_result_v1"
                ),
                "status": (
                    "CANDIDATE_MARK_TO_MARKET_BLOCKED_IMMUTABLE"
                    if blocked
                    else "CANDIDATE_MARK_TO_MARKET_COMPLETE_IMMUTABLE"
                ),
                "candidate_id": candidate_id,
                "input_data_sha256": input_data_sha256,
                "summary": summary,
                "receipt": receipt,
                "blocker": blocker,
            },
        )
        candidate_rows.append(summary)
        receipts.append(receipt)
        if blocker is not None:
            blockers.append(blocker)
        print(
            json.dumps(
                {
                    "candidate_id": candidate_id,
                    "status": summary[
                        "candidate_mark_to_market_status"
                    ],
                    "reward": summary["mark_to_market_net_reward"],
                    "ending_holdings_weight": summary[
                        "ending_holdings_weight"
                    ],
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            flush=True,
        )

    if len(candidate_rows) != base.EXPECTED_MEMBER_COUNT:
        raise RuntimeError("mark-to-market candidate result count drift")
    if [str(row["candidate_id"]) for row in candidate_rows] != list(
        freeze["candidate_ids"]
    ):
        raise RuntimeError("mark-to-market candidate identity/order drift")

    candidate_results_path = (
        output_root / "candidate_mark_to_market_results.parquet"
    )
    pd.DataFrame(candidate_rows).to_parquet(
        candidate_results_path,
        index=False,
    )
    pair_results = _pair_results(candidates, candidate_rows)
    pair_results_path = (
        output_root / "pair_mark_to_market_results.parquet"
    )
    pair_results.to_parquet(pair_results_path, index=False)
    receipts_path = base._write_jsonl(
        output_root / "mark_to_market_candidate_receipts.jsonl",
        receipts,
    )
    blockers_path = base._write_jsonl(
        output_root / "mark_to_market_blockers.jsonl",
        blockers,
    )

    complete_candidates = sum(
        row["candidate_mark_to_market_status"] == CANDIDATE_COMPLETE
        for row in candidate_rows
    )
    complete_pairs = pair_results["pair_mark_to_market_status"].eq(
        PAIR_COMPLETE
    )
    summary = {
        "schema_version": "cn_finalist_mark_to_market_summary_v1",
        "status": "FINAL_CLOSE_MARK_TO_MARKET_REPLAY_COMPLETE",
        "selection_payload_sha256": freeze["selection_payload_sha256"],
        "strict_replay_closure_sha256": strict_replay_closure_sha256,
        "ending_book_policy": ENDING_BOOK_FINAL_CLOSE_MARK_TO_MARKET,
        "pair_count": len(pair_results),
        "candidate_member_count": len(candidate_rows),
        "candidate_complete_count": int(complete_candidates),
        "candidate_blocked_count": int(
            len(candidate_rows) - complete_candidates
        ),
        "pair_complete_count": int(complete_pairs.sum()),
        "pair_blocked_count": int((~complete_pairs).sum()),
        "primary_positive_reward_count": int(
            (
                pair_results["primary_mark_to_market_net_reward"] > 0
            )
            .loc[complete_pairs]
            .sum()
        ),
        "positive_increment_count": int(
            (
                pair_results["mark_to_market_net_increment"] > 0
            )
            .loc[complete_pairs]
            .sum()
        ),
        "primary_reward_median": _finite(
            pair_results[
                "primary_mark_to_market_net_reward"
            ]
            .loc[complete_pairs]
            .median()
        ),
        "increment_median": _finite(
            pair_results["mark_to_market_net_increment"]
            .loc[complete_pairs]
            .median()
        ),
        "ending_holdings_weight_median": _finite(
            pd.to_numeric(
                pd.DataFrame(candidate_rows)["ending_holdings_weight"],
                errors="coerce",
            ).median()
        ),
        "ending_holdings_weight_p90": _finite(
            pd.to_numeric(
                pd.DataFrame(candidate_rows)["ending_holdings_weight"],
                errors="coerce",
            ).quantile(0.9)
        ),
        "train_reads": int(
            sum(int(row["train_read_count"]) for row in candidate_rows)
        ),
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "strict_replay_recomputed": False,
        "oos_recomputed": False,
        "feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "promotion": "FORBIDDEN",
        "economic_claim_authorized": False,
        "successor_search_authorized": False,
    }
    summary_path = base._write_json(
        output_root / "mark_to_market_summary.json",
        summary,
    )
    artifacts = [
        freeze_path,
        contract_path,
        field_manifest_path,
        session_manifest_path,
        strict_closure_path,
        candidate_results_path,
        pair_results_path,
        receipts_path,
        blockers_path,
        summary_path,
        *sorted(candidate_root.glob("*.json")),
    ]
    closure = {
        "schema_version": SCHEMA_VERSION,
        "status": (
            "FINAL_CLOSE_MARK_TO_MARKET_REPLAY_CLOSED_IMMUTABLE_"
            "DIAGNOSTIC_ONLY"
        ),
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "selection_payload_sha256": freeze["selection_payload_sha256"],
        "strict_replay_closure_sha256": strict_replay_closure_sha256,
        "input_data_sha256": input_data_sha256,
        "source_code_sha256": source_code_sha256,
        "ending_book_policy": ENDING_BOOK_FINAL_CLOSE_MARK_TO_MARKET,
        "pair_count": base.EXPECTED_PAIR_COUNT,
        "candidate_member_count": base.EXPECTED_MEMBER_COUNT,
        "initial_free_memory_bytes": initial_free_memory,
        "minimum_free_memory_gate_bytes": (
            base.MINIMUM_FREE_MEMORY_BYTES
        ),
        "train_reads": summary["train_reads"],
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "strict_replay_recomputed": False,
        "oos_recomputed": False,
        "no_fabricated_terminal_sale": True,
        "terminal_sale_fee_applied": False,
        "feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "promotion": "FORBIDDEN",
        "economic_claim_authorized": False,
        "successor_search_authorized": False,
        "artifacts": [base._artifact(path) for path in artifacts],
    }
    closure["manifest_body_sha256"] = base._stable_hash(closure)
    closure_path = base._write_json(
        output_root / "MARK_TO_MARKET_REPLAY_COMPLETE.json",
        closure,
    )
    return {
        **summary,
        "closure_path": str(closure_path),
        "closure_sha256": base._sha256(closure_path),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--freeze-manifest", type=Path, required=True)
    parser.add_argument("--train-field-root", type=Path, required=True)
    parser.add_argument("--strict-replay-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args()
    result = replay_mark_to_market(
        freeze_path=args.freeze_manifest,
        train_field_root=args.train_field_root,
        strict_replay_root=args.strict_replay_root,
        output_root=args.output_root,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
