"""Close bounded CN route supply and diagnose prior feedback clamps.

Structural exact-supply diagnosis may run locally.  Label-free behavior probes
and the small full-coordinate Phase3CM qualification are restricted to 77o.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import platform
from pathlib import Path
from typing import Any, Sequence

import pandas as pd

from our_system_phase2.runtime.cn_iterative_search_v1 import (
    AUTHORIZED_HOST,
    _context_and_binding,
    _join_full_behavior_identities,
    _outcome_rows,
    _run_phase3cm,
    _write_csv,
    _write_parquet,
)
from our_system_phase2.services.fixed_split_authority import FixedSplitAuthority
from our_system_phase2.services.portfolio_behavior_archive import (
    bounded_label_free_behavior_probe,
)
from our_system_phase2.services.route_supply_closure import (
    PRIMARY_SEARCH_ROUTES,
    compare_clamped_routes,
    diagnose_exact_supply,
)
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
)
from our_system_phase2.services.unified_discovery_generators import (
    COMPOSITIONAL_V2_PROFILE,
    LEGACY_V1_PROFILE,
    RegistryDrivenGenerator,
)


REPO = Path(__file__).resolve().parents[1]
TARGET_BEHAVIOR_ROUTES = ("DISCLOSURE_EVENT", "MARKET_REGIME_CONDITION")
PROBE_PAIRS_PER_ROUTE = 12
FULL_PAIRS_PER_ROUTE = 4


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _write_json(path: Path, payload: Any) -> Path:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(destination.name + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str)
        + "\n",
        encoding="utf-8",
    )
    temporary.replace(destination)
    return destination


def _historical_exact(prior_root: Path) -> set[str]:
    exact: set[str] = set()
    for path in sorted(Path(prior_root).glob("batch_[0-9][0-9][0-9]/candidate_attempt_stream.csv")):
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                identity = str(row.get("exact_identity") or "")
                if identity:
                    exact.add(identity)
    return exact


def _prior_route_comparison(prior_root: Path) -> dict[str, dict[str, Any]]:
    path = Path(prior_root) / "causal_attribution.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {
        str(route_id): dict(row)
        for route_id, row in dict(
            payload.get("feedback_on_off_route_comparison") or {}
        ).items()
    }


def _train_dates(split: FixedSplitAuthority) -> tuple[str, ...]:
    return tuple(row["trade_date"] for row in split.rows if row["split"] == "train")


def _route_probe(
    *,
    route_id: str,
    generator: RegistryDrivenGenerator,
    seed: int,
    historical_exact: set[str],
    field_root: Path,
    train_dates: Sequence[str],
    output_root: Path,
    compute_threads: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    candidates, funnel = generator.generate_route_attempts(
        route_id,
        scheduled_pairs=PROBE_PAIRS_PER_ROUTE,
        seed=seed,
        attempt_limit=512,
        existing_exact_identities=set(historical_exact),
    )
    paths = tuple(sorted(Path(field_root).glob("shard_*.parquet")))
    if not paths:
        raise FileNotFoundError(f"no Phase3CM field sidecars under {field_root}")
    probe_rows, audit = bounded_label_free_behavior_probe(
        candidates=candidates,
        field_sidecars=paths,
        eligible_trade_dates=train_dates,
        coordinate_binding=hashlib.sha256(
            f"route-supply|{route_id}|{seed}".encode("utf-8")
        ).hexdigest(),
        batch_id="route_supply_closure",
        compute_threads=compute_threads,
        max_trade_dates=4,
        max_trade_times=1 if route_id == "DISCLOSURE_EVENT" else 8,
        date_selection=(
            "condition_activation"
            if route_id == "DISCLOSURE_EVENT"
            else "calendar_stratified"
        ),
    )
    route_root = output_root / route_id.lower()
    _write_csv(route_root / "candidate_attempt_stream.csv", candidates)
    _write_parquet(route_root / "behavior_probe.parquet", probe_rows)
    _write_json(route_root / "behavior_probe_audit.json", audit)
    _write_json(route_root / "generation_funnel.json", funnel)
    return candidates, probe_rows, audit


def _selected_full_coordinate_members(
    *,
    candidates: Sequence[dict[str, Any]],
    probe_rows: Sequence[dict[str, Any]],
    maximum_pairs: int,
) -> list[dict[str, Any]]:
    unique_probe_ids: set[str] = set()
    selected_pair_ids: list[str] = []
    for row in probe_rows:
        probe_id = str(row.get("behavior_probe_id") or "")
        if str(row.get("behavior_status") or "") != "RESOLVED" or not probe_id:
            continue
        if probe_id in unique_probe_ids:
            continue
        unique_probe_ids.add(probe_id)
        selected_pair_ids.append(str(row["pair_id"]))
        if len(selected_pair_ids) >= int(maximum_pairs):
            break
    selected = set(selected_pair_ids)
    return [dict(row) for row in candidates if str(row.get("pair_id") or "") in selected]


def _behavior_and_full_coordinate_qualification(
    args: argparse.Namespace,
    *,
    registry: UnifiedCapabilityRegistry,
    historical_exact: set[str],
    output_root: Path,
) -> dict[str, Any]:
    if platform.node().upper() != AUTHORIZED_HOST:
        raise RuntimeError(
            f"behavior materialization and Phase3CM are authorized only on 77o "
            f"({AUTHORIZED_HOST}); host={platform.node()}"
        )
    split = FixedSplitAuthority.read(args.split_manifest.resolve())
    train_dates = _train_dates(split)
    generator = RegistryDrivenGenerator(
        registry, constructor_profile=COMPOSITIONAL_V2_PROFILE
    )
    field_roots = {
        "active_bar": args.active_field_root.resolve(),
        "stock_session": args.session_field_root.resolve(),
    }
    label_roots = {
        "active_bar": args.active_label_root.resolve(),
        "stock_session": args.session_label_root.resolve(),
    }
    compute_threads = {
        "active_bar": int(args.active_threads),
        "stock_session": int(args.session_threads),
    }
    all_candidates: list[dict[str, Any]] = []
    all_probe_rows: list[dict[str, Any]] = []
    selected: list[dict[str, Any]] = []
    route_summaries: dict[str, dict[str, Any]] = {}
    for ordinal, route_id in enumerate(TARGET_BEHAVIOR_ROUTES):
        backend = "active_bar" if route_id == "MARKET_REGIME_CONDITION" else "stock_session"
        candidates, probe_rows, audit = _route_probe(
            route_id=route_id,
            generator=generator,
            seed=int(args.seed + ordinal * 1009),
            historical_exact=historical_exact,
            field_root=field_roots[backend],
            train_dates=train_dates,
            output_root=output_root,
            compute_threads=compute_threads[backend],
        )
        route_selected = _selected_full_coordinate_members(
            candidates=candidates,
            probe_rows=probe_rows,
            maximum_pairs=FULL_PAIRS_PER_ROUTE,
        )
        resolved = [
            row for row in probe_rows if str(row.get("behavior_status") or "") == "RESOLVED"
        ]
        route_summaries[route_id] = {
            "generated_pairs": len(candidates) // 2,
            "probe_resolved_pairs": len(resolved),
            "probe_unresolved_pairs": len(probe_rows) - len(resolved),
            "probe_behavior_unique_pairs": len(
                {str(row.get("behavior_probe_id") or "") for row in resolved}
                - {""}
            ),
            "selected_full_coordinate_pairs": len(route_selected) // 2,
            "probe_dates": list(audit.get("probe_dates") or ()),
            "date_selection": audit.get("date_selection"),
        }
        all_candidates.extend(candidates)
        all_probe_rows.extend(probe_rows)
        selected.extend(route_selected)

    if any(
        int(route_summaries[route_id]["selected_full_coordinate_pairs"])
        < FULL_PAIRS_PER_ROUTE
        for route_id in TARGET_BEHAVIOR_ROUTES
    ):
        return {
            "status": "BOUNDED_PROBE_SUPPLY_PARTIAL",
            "routes": route_summaries,
            "full_coordinate_phase3cm": "NOT_RUN_INSUFFICIENT_BEHAVIOR_UNIQUE_PROBE_SUPPLY",
            "selected_candidate_members": len(selected),
        }

    full_root = output_root / "full_coordinate_development_qualification"
    binding_path, table_paths = _context_and_binding(
        batch_root=full_root,
        candidates=selected,
        registry=registry,
        split=split,
        data_release_hash=_sha256(args.sidecar_closure.resolve()),
    )
    receipts = _run_phase3cm(
        batch_id="route_supply_closure",
        batch_root=full_root,
        binding_path=binding_path,
        table_paths=table_paths,
        split_manifest=args.split_manifest.resolve(),
        field_roots=field_roots,
        label_roots=label_roots,
        compute_threads=compute_threads,
    )
    outcomes, full_behavior = _outcome_rows(full_root)
    selected_pair_ids = {str(row["pair_id"]) for row in selected}
    selected_probes = [
        row for row in all_probe_rows if str(row["pair_id"]) in selected_pair_ids
    ]
    joined = _join_full_behavior_identities(full_behavior, selected_probes)
    _write_parquet(full_root / "full_behavior_four_identities.parquet", joined)
    _write_parquet(full_root / "pair_outcomes.parquet", outcomes)
    for route_id in TARGET_BEHAVIOR_ROUTES:
        route_full = [row for row in joined if str(row.get("route_id") or "") == route_id]
        route_summaries[route_id].update(
            {
                "full_coordinate_pairs": len(route_full),
                "full_coordinate_resolved_pairs": sum(
                    str(row.get("behavior_status") or "") == "RESOLVED"
                    for row in route_full
                ),
                "full_coordinate_behavior_unique_pairs": len(
                    {
                        str(row.get("portfolio_behavior_signature_id") or "")
                        for row in route_full
                        if str(row.get("behavior_status") or "") == "RESOLVED"
                    }
                    - {""}
                ),
                "four_identities_closed": all(
                    all(
                        str(row.get(key) or "")
                        for key in (
                            "structural_family_id",
                            "signal_cluster_id",
                            "portfolio_behavior_signature_id",
                            "portfolio_behavior_family_id",
                        )
                    )
                    for row in route_full
                    if str(row.get("behavior_status") or "") == "RESOLVED"
                ),
            }
        )
    gates = {
        route_id: (
            int(route_summaries[route_id].get("full_coordinate_resolved_pairs") or 0)
            >= FULL_PAIRS_PER_ROUTE
            and int(
                route_summaries[route_id].get(
                    "full_coordinate_behavior_unique_pairs"
                )
                or 0
            )
            >= FULL_PAIRS_PER_ROUTE
            and bool(route_summaries[route_id].get("four_identities_closed"))
        )
        for route_id in TARGET_BEHAVIOR_ROUTES
    }
    return {
        "status": "ROUTE_BEHAVIOR_AND_FULL_COORDINATE_QUALIFICATION_PASS"
        if all(gates.values())
        else "ROUTE_BEHAVIOR_AND_FULL_COORDINATE_QUALIFICATION_PARTIAL",
        "routes": route_summaries,
        "route_gates": gates,
        "phase3cm_access_receipts": receipts,
        "full_coordinate_phase3cm": "COMPLETED",
        "outcome_pair_count": len(outcomes),
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "FORBIDDEN",
    }


def run(args: argparse.Namespace) -> dict[str, Any]:
    output_root = args.output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    registry = UnifiedCapabilityRegistry.read(args.registry.resolve())
    historical_exact = _historical_exact(args.prior_run_root.resolve())
    caps = tuple(int(value) for value in str(args.attempt_caps).split(",") if value)
    seeds = tuple(int(value) for value in str(args.seeds).split(",") if value)
    contract = {
        "schema_version": "cn_route_supply_closure_contract_v1",
        "authorization": "BOUNDED_DEVELOPMENT_ROUTE_SUPPLY_QUALIFICATION",
        "top_level_scheduling_key": "unified_registry_route_id",
        "generator_authority": "RegistryDrivenGenerator",
        "legacy_profile": LEGACY_V1_PROFILE,
        "upgraded_profile": COMPOSITIONAL_V2_PROFILE,
        "attempt_caps": list(caps),
        "seeds": list(seeds),
        "required_pairs_per_route": int(args.required_pairs),
        "required_headroom_multiplier": 2,
        "broad_event": "FROZEN_REFERENCE_ONLY_ZERO_SEARCH_BUDGET",
        "probe_pairs_per_target_route": PROBE_PAIRS_PER_ROUTE,
        "full_coordinate_pairs_per_target_route": FULL_PAIRS_PER_ROUTE,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "FORBIDDEN",
    }
    contract_path = _write_json(output_root / "frozen_contract.json", contract)
    legacy = diagnose_exact_supply(
        RegistryDrivenGenerator(registry, constructor_profile=LEGACY_V1_PROFILE),
        seeds=seeds,
        attempt_caps=caps,
        required_pairs=int(args.required_pairs),
        historical_exact=historical_exact,
    )
    upgraded = diagnose_exact_supply(
        RegistryDrivenGenerator(
            registry, constructor_profile=COMPOSITIONAL_V2_PROFILE
        ),
        seeds=seeds,
        attempt_caps=caps,
        required_pairs=int(args.required_pairs),
        historical_exact=historical_exact,
    )
    legacy_path = _write_json(output_root / "legacy_exact_supply.json", legacy)
    upgraded_path = _write_json(output_root / "upgraded_exact_supply.json", upgraded)
    clamped = compare_clamped_routes(
        prior_route_comparison=_prior_route_comparison(args.prior_run_root.resolve()),
        legacy_report=legacy,
        upgraded_report=upgraded,
    )
    clamp_path = _write_json(output_root / "clamp_root_cause.json", clamped)

    broad_pack = json.loads(args.broad_event_entry_pack.read_text(encoding="utf-8"))
    broad_reference = {
        "search_role": "FROZEN_REFERENCE_ONLY",
        "search_budget": 0,
        "mechanism_count": len(broad_pack.get("mechanisms") or ()),
        "entry_pack_path": str(args.broad_event_entry_pack.resolve()),
        "entry_pack_sha256": _sha256(args.broad_event_entry_pack.resolve()),
        "status": "FROZEN_REFERENCE_EVIDENCE_BOUND"
        if len(broad_pack.get("mechanisms") or ()) == 11
        else "FROZEN_REFERENCE_EVIDENCE_INVALID",
    }
    broad_path = _write_json(output_root / "broad_event_reference.json", broad_reference)

    behavior = {"status": "NOT_RUN_LOCAL_STRUCTURAL_SCOPE"}
    if not args.structural_only:
        behavior = _behavior_and_full_coordinate_qualification(
            args,
            registry=registry,
            historical_exact=historical_exact,
            output_root=output_root,
        )
    behavior_path = _write_json(output_root / "behavior_qualification.json", behavior)

    exact_ready = all(
        str(upgraded["routes"][route_id]["classification"])
        == "SEARCHABLE_SUPPLY_READY_WITH_HEADROOM"
        for route_id in PRIMARY_SEARCH_ROUTES
    )
    clamp_nonblocking = all(
        not bool(row["blocks_next_development_phase"]) for row in clamped
    )
    pass_gates = {
        "all_primary_routes_have_exact_supply_headroom": exact_ready,
        "prior_actionable_clamps_classified_nonblocking": clamp_nonblocking,
        "broad_event_remains_zero_budget_frozen_reference": broad_reference["status"]
        == "FROZEN_REFERENCE_EVIDENCE_BOUND",
        "behavior_and_full_coordinate_target_routes_pass": behavior.get("status")
        == "ROUTE_BEHAVIOR_AND_FULL_COORDINATE_QUALIFICATION_PASS",
        "validation_holdout_2026_reads_zero": True,
        "promotion_forbidden": True,
    }
    if args.structural_only:
        status = "CN_ROUTE_SUPPLY_STRUCTURAL_DIAGNOSIS_COMPLETED"
    else:
        status = (
            "CN_ROUTE_SUPPLY_CLOSURE_PASS"
            if all(pass_gates.values())
            else "CN_ROUTE_SUPPLY_CLOSURE_PARTIAL"
        )
    decision = {
        "status": status,
        "gates": pass_gates,
        "clamped_routes": clamped,
        "broad_event": broad_reference,
        "behavior_qualification": behavior,
        "next_phase": (
            "READY_FOR_SEPARATELY_FROZEN_DEVELOPMENT_CAMPAIGN"
            if status == "CN_ROUTE_SUPPLY_CLOSURE_PASS"
            else "HOLD_ONLY_FAILED_ROUTE_GATES_NOT_GLOBAL_PLATFORM"
        ),
        "formal_search": "FROZEN",
        "strict_stage_a": "NOT_AUTHORIZED",
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "FORBIDDEN",
    }
    decision_path = _write_json(output_root / "final_decision.json", decision)
    _write_json(
        output_root / "run_manifest.json",
        {
            "status": status,
            "repo_sha": args.repo_sha,
            "host": platform.node(),
            "registry_hash": registry.registry_hash,
            "historical_exact_identity_count": len(historical_exact),
            "artifacts": [
                {
                    "path": str(path.relative_to(output_root)).replace("\\", "/"),
                    "sha256": _sha256(path),
                    "bytes": path.stat().st_size,
                }
                for path in (
                    contract_path,
                    legacy_path,
                    upgraded_path,
                    clamp_path,
                    broad_path,
                    behavior_path,
                    decision_path,
                )
            ],
            "validation_reads": 0,
            "holdout_reads": 0,
            "forward_2026_reads": 0,
            "promotion": "FORBIDDEN",
        },
    )
    return decision


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--prior-run-root", type=Path, required=True)
    parser.add_argument("--broad-event-entry-pack", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--repo-sha", required=True)
    parser.add_argument("--seeds", default="1729,2718")
    parser.add_argument("--attempt-caps", default="64,256,1024")
    parser.add_argument("--required-pairs", type=int, default=12)
    parser.add_argument("--seed", type=int, default=2026072201)
    parser.add_argument("--structural-only", action="store_true")
    parser.add_argument("--split-manifest", type=Path)
    parser.add_argument("--sidecar-closure", type=Path)
    parser.add_argument("--active-field-root", type=Path)
    parser.add_argument("--active-label-root", type=Path)
    parser.add_argument("--session-field-root", type=Path)
    parser.add_argument("--session-label-root", type=Path)
    parser.add_argument("--active-threads", type=int, default=11)
    parser.add_argument("--session-threads", type=int, default=2)
    args = parser.parse_args(argv)
    if not args.structural_only:
        required = (
            "split_manifest",
            "sidecar_closure",
            "active_field_root",
            "active_label_root",
            "session_field_root",
            "session_label_root",
        )
        missing = [name for name in required if getattr(args, name) is None]
        if missing:
            parser.error("missing 77o qualification arguments: " + ", ".join(missing))
    result = run(args)
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True, default=str))
    return 0 if result["status"] in {
        "CN_ROUTE_SUPPLY_CLOSURE_PASS",
        "CN_ROUTE_SUPPLY_STRUCTURAL_DIAGNOSIS_COMPLETED",
    } else 2


if __name__ == "__main__":
    raise SystemExit(main())
