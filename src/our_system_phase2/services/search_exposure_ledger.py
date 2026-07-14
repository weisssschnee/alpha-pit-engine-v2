"""Proof ledger for field and route exposure through the search funnel."""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping

from our_system_phase2.services.unified_capability_registry import (
    ROUTE_IDS,
    UnifiedCapabilityRegistry,
)


LEDGER_VERSION = "cn_search_exposure_ledger_v1"

CANDIDATE_FIELD_LINEAGE_COLUMNS = (
    "run_id", "repo_sha", "contract_hash", "data_release_hash", "candidate_id",
    "exact_identity", "behavior_identity", "route_id", "field_id", "source_field_id",
    "representation_id", "entity_scope", "temporal_semantics", "operator_family",
    "operator_path", "maturity_rule", "support_unit", "matched_control_id",
    "proposal_origin", "seed", "access_role",
)

FIELD_ROUTE_STAGE_COLUMNS = (
    "run_id", "field_id", "source_field_id", "source_family", "route_id",
    "operator_family", "proposal_count", "legal_count", "canonical_count", "exact_count",
    "admission_count", "strict_count", "survivor_count", "behavior_cluster_count",
    "matched_control_count", "development_increment_positive_count", "challenge_count",
    "forward_count", "zero_budget", "not_executed_reason",
)

LANE_CAPABILITY_COLUMNS = (
    "run_id", "route_id", "qualified_field_count", "wired_field_count",
    "proposal_exposed_field_count", "strict_exposed_field_count", "survivor_field_count",
    "untested_field_count", "matched_control_completion_rate", "forbidden_read_count",
    "claim_ceiling",
)


def _write_csv(path: Path, rows: Iterable[Mapping[str, Any]], columns: Iterable[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(columns), extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _stage(row: Mapping[str, Any], key: str) -> bool:
    if key == "proposal":
        return True
    return bool(row.get(key, False))


class SearchExposureLedger:
    def __init__(
        self,
        *,
        registry: UnifiedCapabilityRegistry,
        run_id: str,
        repo_sha: str,
        contract_hash: str,
        data_release_hash: str,
        route_budgets: Mapping[str, Mapping[str, int]],
    ) -> None:
        self.registry = registry
        self.run_id = str(run_id)
        self.repo_sha = str(repo_sha)
        self.contract_hash = str(contract_hash)
        self.data_release_hash = str(data_release_hash)
        self.route_budgets = {str(key): dict(value) for key, value in route_budgets.items()}
        self.rows: list[dict[str, Any]] = []

    def ingest(self, candidates: Iterable[Mapping[str, Any]]) -> None:
        for candidate in candidates:
            row = dict(candidate)
            if not row.get("candidate_id") or not row.get("route_id"):
                raise ValueError("exposure row lacks candidate_id/route_id")
            self.rows.append(row)

    def candidate_field_lineage(self) -> list[dict[str, Any]]:
        output: list[dict[str, Any]] = []
        for candidate in self.rows:
            field_ids = list(candidate.get("field_ids") or candidate.get("declared_field_ids") or ())
            operator_paths = list(candidate.get("operator_paths") or (str(candidate.get("operator_family") or ""),))
            for field_id in sorted(set(str(value) for value in field_ids)):
                field = self.registry.resolve(field_id)
                for operator_path in operator_paths:
                    output.append(
                        {
                            "run_id": self.run_id,
                            "repo_sha": self.repo_sha,
                            "contract_hash": self.contract_hash,
                            "data_release_hash": self.data_release_hash,
                            "candidate_id": candidate["candidate_id"],
                            "exact_identity": candidate.get("exact_identity", ""),
                            "behavior_identity": candidate.get("behavior_identity", ""),
                            "route_id": candidate["route_id"],
                            "field_id": field.field_id,
                            "source_field_id": field.source_field_id,
                            "representation_id": field.representation_id,
                            "entity_scope": field.entity_scope,
                            "temporal_semantics": field.temporal_semantics,
                            "operator_family": candidate.get("operator_family", ""),
                            "operator_path": operator_path,
                            "maturity_rule": candidate.get("maturity_rule", field.maturity_rule),
                            "support_unit": candidate.get("support_unit", field.support_unit),
                            "matched_control_id": candidate.get("matched_control_id", ""),
                            "proposal_origin": candidate.get("proposal_origin", ""),
                            "seed": candidate.get("seed", ""),
                            "access_role": "development",
                        }
                    )
        unique: dict[tuple[Any, ...], dict[str, Any]] = {}
        for row in output:
            key = (
                row["run_id"], row["candidate_id"], row["field_id"],
                row["representation_id"], row["operator_path"],
            )
            unique[key] = row
        return [unique[key] for key in sorted(unique)]

    def field_route_stage_summary(self) -> list[dict[str, Any]]:
        groups: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
        for candidate in self.rows:
            for field_id in sorted(set(candidate.get("field_ids") or candidate.get("declared_field_ids") or ())):
                groups[(str(field_id), str(candidate["route_id"]), str(candidate.get("operator_family") or ""))].append(candidate)
        output: list[dict[str, Any]] = []
        for (field_id, route_id, operator), rows in sorted(groups.items()):
            field = self.registry.resolve(field_id)
            legal = [row for row in rows if bool(row.get("legal"))]
            canonical = {str(row.get("canonical_identity")) for row in legal if row.get("canonical_identity")}
            exact = {str(row.get("exact_identity")) for row in legal if row.get("exact_identity")}
            behaviors = {str(row.get("behavior_identity")) for row in rows if row.get("behavior_identity")}
            matched = sum(bool(row.get("matched_control_id")) for row in rows if not bool(row.get("is_matched_control")))
            budget = self.route_budgets[route_id]
            output.append(
                {
                    "run_id": self.run_id,
                    "field_id": field_id,
                    "source_field_id": field.source_field_id,
                    "source_family": field.source_family,
                    "route_id": route_id,
                    "operator_family": operator,
                    "proposal_count": len(rows),
                    "legal_count": len(legal),
                    "canonical_count": len(canonical),
                    "exact_count": len(exact),
                    "admission_count": sum(_stage(row, "admission") for row in rows),
                    "strict_count": sum(_stage(row, "strict") for row in rows),
                    "survivor_count": sum(_stage(row, "survivor") for row in rows),
                    "behavior_cluster_count": len(behaviors),
                    "matched_control_count": matched,
                    "development_increment_positive_count": sum(bool(row.get("development_increment_positive")) for row in rows),
                    "challenge_count": 0,
                    "forward_count": 0,
                    "zero_budget": int(budget.get("proposal", 0)) == 0,
                    "not_executed_reason": "NOT_EXECUTED_ZERO_BUDGET" if int(budget.get("proposal", 0)) == 0 else "",
                }
            )
        return output

    def lane_capability_summary(self) -> list[dict[str, Any]]:
        output: list[dict[str, Any]] = []
        forbidden_reads = sum(int(row.get("forbidden_read_count") or 0) for row in self.rows)
        for route_id in ROUTE_IDS:
            qualified = self.registry.fields_for_route(route_id)
            rows = [row for row in self.rows if row["route_id"] == route_id]
            field_ids = set(
                str(value)
                for row in rows
                for value in (row.get("field_ids") or row.get("declared_field_ids") or ())
            )
            strict_fields = set(
                str(value)
                for row in rows if bool(row.get("strict"))
                for value in (row.get("field_ids") or row.get("declared_field_ids") or ())
            )
            survivor_fields = set(
                str(value)
                for row in rows if bool(row.get("survivor"))
                for value in (row.get("field_ids") or row.get("declared_field_ids") or ())
            )
            mechanisms = [row for row in rows if not bool(row.get("is_matched_control"))]
            matched = sum(bool(row.get("matched_control_id")) for row in mechanisms)
            if int(self.route_budgets[route_id].get("proposal", 0)) == 0:
                claim = "NOT_EXECUTED"
            elif not any(bool(row.get("strict")) for row in rows):
                claim = "NOT_EVALUATED"
            else:
                claim = "DEVELOPMENT_EVIDENCE_ONLY"
            output.append(
                {
                    "run_id": self.run_id,
                    "route_id": route_id,
                    "qualified_field_count": len(qualified),
                    "wired_field_count": len(field_ids),
                    "proposal_exposed_field_count": len(field_ids),
                    "strict_exposed_field_count": len(strict_fields),
                    "survivor_field_count": len(survivor_fields),
                    "untested_field_count": max(0, len(qualified) - len(field_ids)),
                    "matched_control_completion_rate": matched / max(len(mechanisms), 1),
                    "forbidden_read_count": forbidden_reads,
                    "claim_ceiling": claim,
                }
            )
        return output

    def untested_information_families(self) -> list[dict[str, Any]]:
        exposed = set(
            str(value)
            for row in self.rows
            for value in (row.get("field_ids") or row.get("declared_field_ids") or ())
        )
        output = []
        for field in self.registry.fields:
            if not field.search_eligible or field.field_id in exposed:
                continue
            output.append(
                {
                    "run_id": self.run_id,
                    "field_id": field.field_id,
                    "source_field_id": field.source_field_id,
                    "source_family": field.source_family,
                    "allowed_routes": "|".join(field.allowed_routes),
                    "status": "NOT_WIRED" if field.allowed_routes else "NOT_EVALUATED",
                    "blocked_reason": field.blocked_reason,
                }
            )
        return sorted(output, key=lambda row: (row["source_family"], row["field_id"]))

    def validate(self) -> None:
        candidate_ids = {str(row["candidate_id"]) for row in self.rows}
        exact = [str(row["exact_identity"]) for row in self.rows if bool(row.get("legal"))]
        if len(exact) != len(set(exact)):
            raise ValueError("exact identity dedup did not occur before budget accounting")
        for row in self.rows:
            if row.get("matched_control_id") not in candidate_ids:
                raise ValueError(f"missing matched control row for {row['candidate_id']}")
            if bool(row.get("survivor")) and not bool(row.get("strict")):
                raise ValueError("survivor_count cannot exceed strict_count")
            if bool(row.get("strict")) and not bool(row.get("admission")):
                raise ValueError("strict_count cannot exceed admission_count")
            if bool(row.get("development_increment_positive")) and not row.get("matched_control_id"):
                raise ValueError("development increment lacks matched control")
            if any(int(row.get(key) or 0) for key in ("challenge_count", "forward_count")):
                raise ValueError("challenge/forward exposure is forbidden")

    def write(self, output_root: Path) -> dict[str, Any]:
        self.validate()
        output_root.mkdir(parents=True, exist_ok=True)
        lineage = self.candidate_field_lineage()
        stages = self.field_route_stage_summary()
        lanes = self.lane_capability_summary()
        untested = self.untested_information_families()
        _write_csv(output_root / "CANDIDATE_FIELD_LINEAGE.csv", lineage, CANDIDATE_FIELD_LINEAGE_COLUMNS)
        _write_csv(output_root / "FIELD_FAMILY_EXPOSURE_MATRIX.csv", stages, FIELD_ROUTE_STAGE_COLUMNS)
        _write_csv(output_root / "LANE_CAPABILITY_COVERAGE.csv", lanes, LANE_CAPABILITY_COLUMNS)
        untested_columns = (
            "run_id", "field_id", "source_field_id", "source_family", "allowed_routes", "status", "blocked_reason"
        )
        _write_csv(output_root / "UNTESTED_INFORMATION_FAMILIES.csv", untested, untested_columns)
        summary = {
            "ledger_version": LEDGER_VERSION,
            "run_id": self.run_id,
            "candidate_count": len(self.rows),
            "legal_count": sum(bool(row.get("legal")) for row in self.rows),
            "exact_count": len({row.get("exact_identity") for row in self.rows if row.get("exact_identity")}),
            "route_rows": {route_id: sum(row["route_id"] == route_id for row in self.rows) for route_id in ROUTE_IDS},
            "strict_count": sum(bool(row.get("strict")) for row in self.rows),
            "survivor_count": sum(bool(row.get("survivor")) for row in self.rows),
            "forbidden_read_count": sum(int(row.get("forbidden_read_count") or 0) for row in self.rows),
            "challenge_count": 0,
            "forward_count": 0,
            "claim_ceiling": "DEVELOPMENT_EVIDENCE_ONLY" if any(bool(row.get("strict")) for row in self.rows) else "NOT_EVALUATED",
        }
        (output_root / "exposure_ledger_summary.json").write_text(
            json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return summary
