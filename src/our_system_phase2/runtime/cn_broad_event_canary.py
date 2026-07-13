"""Fixed-budget, development-only CN Broad Event CANARY."""

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import math
from pathlib import Path
import time
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from our_system_phase2.services.broad_event_episodes import (
    audit_episode_support,
    matched_control_contract,
    materialize_broad_event_episodes,
)
from our_system_phase2.services.broad_event_preflight import run_preflight, validate_canary_contract
from our_system_phase2.services.broad_event_semantics import validate_semantic_registry
from our_system_phase2.services.conservative_limit_lifecycle import (
    materialize_conservative_limit_lifecycle,
    validate_derived_limits_against_vendor_occurrence,
)


RUNNER_VERSION = "cn_broad_event_development_canary_runner_v2"
PLACEBO_OFFSETS = (30, 60, 90, -30, -60, -90)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def _discover_shard_file(root: Path, shard: int) -> Path:
    candidates = sorted((root / f"shard_{shard:02d}").rglob("*.parquet"))
    if len(candidates) != 1:
        raise RuntimeError(f"expected one development parquet for shard {shard}: {candidates}")
    return candidates[0]


def _required_columns(schema: set[str]) -> list[str]:
    prefixes = ("ctx_billboard_", "ctx_rzrq_", "ctx_holder_", "ctx_ths_hot_", "ctx_sent_", "ctx_zls_")
    base = {
        "code", "trade_time", "open", "high", "low", "close", "ctx_hfq_is_st",
        "evt_uplimit_active", "evt_uplimit_amount",
    }
    return sorted((base | {column for column in schema if column.startswith(prefixes)}) & schema)


def _load_chip_context(root: Path, allowed_codes: set[str] | None = None) -> pd.DataFrame:
    paths = sorted((root / "shards").glob("*.parquet"))
    if not paths:
        raise RuntimeError("chip sidecar has no parquet shards")
    parts = []
    for path in paths:
        frame = pd.read_parquet(path)
        frame["code"] = frame["code"].astype(str).str.extract(r"(\d{6})", expand=False)
        if allowed_codes is not None:
            frame = frame.loc[frame["code"].isin(allowed_codes)]
        parts.append(frame)
    chip = pd.concat(parts, ignore_index=True)
    observed = pd.to_datetime(chip["source_observed_at"], errors="raise")
    if observed.dt.year.ge(2026).any():
        raise RuntimeError("chip sidecar exposed 2026 observed values")
    return chip


def _attach_episode_outcomes(frame: pd.DataFrame, episodes: pd.DataFrame, horizons: list[int]) -> pd.DataFrame:
    ordered = frame[["code", "trade_time", "close"]].copy()
    ordered["code"] = ordered["code"].astype(str)
    ordered["trade_time"] = pd.to_datetime(ordered["trade_time"], errors="raise")
    ordered["session"] = ordered["trade_time"].dt.normalize()
    ordered = ordered.sort_values(["code", "trade_time"], kind="mergesort").reset_index(drop=True)
    prior_path = ordered.groupby("code", sort=False)["close"]
    grouped = ordered.groupby(["code", "session"], sort=False)["close"]
    ordered["pre_return_5"] = ordered["close"] / prior_path.shift(5) - 1.0
    ordered["pre_return_15"] = ordered["close"] / prior_path.shift(15) - 1.0
    for horizon in horizons:
        ordered[f"target_h{horizon}"] = grouped.shift(-horizon) / ordered["close"] - 1.0
        for offset in PLACEBO_OFFSETS:
            anchor = grouped.shift(-offset)
            future = grouped.shift(-(offset + horizon))
            ordered[f"placebo_target_h{horizon}_o{offset}"] = future / anchor - 1.0
    first = ordered.drop_duplicates(["code", "session"], keep="first")
    symbol = episodes.loc[episodes["entity_scope"].eq("SYMBOL")].copy().reset_index(drop=True)
    symbol["code"] = symbol["entity_id"].astype(str)
    symbol["eligible_action_time"] = pd.to_datetime(symbol["eligible_action_time"], errors="coerce")
    exact = symbol.merge(
        ordered.drop(columns="session"),
        left_on=["code", "eligible_action_time"], right_on=["code", "trade_time"],
        how="left", validate="many_to_one",
    )
    missing = exact["close"].isna()
    if missing.any():
        fallback = symbol.loc[missing].drop(columns=[column for column in ("trade_time", "close") if column in symbol])
        fallback = fallback.merge(first, on=["code", "session"], how="left", validate="many_to_one")
        outcome_columns = [f"target_h{h}" for h in horizons]
        outcome_columns += [f"placebo_target_h{h}_o{o}" for h in horizons for o in PLACEBO_OFFSETS]
        for column in ["trade_time", "close", "pre_return_5", "pre_return_15", *outcome_columns]:
            exact.loc[missing, column] = fallback[column].to_numpy()
    market = episodes.loc[episodes["entity_scope"].eq("MARKET")].copy()
    if not market.empty:
        target_columns = [f"target_h{h}" for h in horizons]
        target_columns += [f"placebo_target_h{h}_o{o}" for h in horizons for o in PLACEBO_OFFSETS]
        market_targets = first.groupby("session", as_index=False).agg(
            close=("close", "mean"),
            pre_return_5=("pre_return_5", "mean"),
            pre_return_15=("pre_return_15", "mean"),
            **{column: (column, "mean") for column in target_columns},
        )
        market = market.merge(market_targets, on="session", how="left", validate="one_to_one")
        market["code"] = "CN_MARKET"
        market["trade_time"] = market["eligible_action_time"]
    columns = sorted(set(exact.columns) | set(market.columns))
    return pd.concat([exact.reindex(columns=columns), market.reindex(columns=columns)], ignore_index=True)


def _time_block(session: pd.Series) -> pd.Series:
    date = pd.to_datetime(session, errors="coerce")
    return pd.Series(
        np.select(
            [date.lt("2024-07-01"), date.lt("2025-01-01"), date.lt("2025-07-01")],
            ["2024H1", "2024H2", "2025H1"], default="2025H2",
        ), index=session.index,
    )


def _signal(group: pd.DataFrame, transform: str, *, event: bool) -> pd.Series:
    direction = pd.to_numeric(group["direction"], errors="coerce").replace(0, np.nan)
    intensity = np.log1p(pd.to_numeric(group["trigger_intensity"], errors="coerce").abs())
    pre5 = pd.to_numeric(group["pre_return_5"], errors="coerce")
    pre15 = pd.to_numeric(group["pre_return_15"], errors="coerce")
    if event:
        return {
            "direction": direction,
            "intensity": direction * intensity,
            "pre5_interaction": direction * np.sign(pre5),
            "pre15_interaction": direction * np.sign(pre15) * intensity,
        }[transform]
    return {
        "direction": np.sign(pre5),
        "intensity": np.sign(pre15) * np.log1p(pre5.abs()),
        "pre5_interaction": pre5,
        "pre15_interaction": pre15,
    }[transform]


def _variant(signal: pd.Series, variant: str) -> pd.Series:
    numeric = pd.to_numeric(signal, errors="coerce")
    if variant == "raw":
        return numeric
    if variant == "compressed":
        return np.sign(numeric) * np.sqrt(numeric.abs())
    raise ValueError(f"unknown signal variant: {variant}")


def _reward(signal: pd.Series, target: pd.Series) -> tuple[float, int]:
    value = pd.to_numeric(signal, errors="coerce") * pd.to_numeric(target, errors="coerce")
    value = value.replace([np.inf, -np.inf], np.nan).dropna()
    if value.empty:
        return float("nan"), 0
    return float(value.mean()), len(value)


def _placebo_target(group: pd.DataFrame, seed: int, horizon: int) -> pd.Series:
    """Choose a precomputed same-symbol, same-session shifted action time per episode."""
    episode_ids = group["episode_id"].astype(str)
    selectors = np.fromiter(
        ((int(value[:8], 16) ^ int(seed) ^ int(horizon)) % len(PLACEBO_OFFSETS) for value in episode_ids),
        dtype=np.int8,
        count=len(group),
    )
    alternatives = np.column_stack([
        pd.to_numeric(group[f"placebo_target_h{horizon}_o{offset}"], errors="coerce").to_numpy()
        for offset in PLACEBO_OFFSETS
    ])
    values = np.full(len(group), np.nan, dtype=float)
    row_indices = np.arange(len(group))
    for displacement in range(len(PLACEBO_OFFSETS)):
        candidate_columns = (selectors + displacement) % len(PLACEBO_OFFSETS)
        candidate_values = alternatives[row_indices, candidate_columns]
        fill = np.isnan(values) & np.isfinite(candidate_values)
        values[fill] = candidate_values[fill]
    return pd.Series(values, index=group.index, dtype=float)


def _candidate_rows(observations: pd.DataFrame, contract: dict[str, Any], seed: int) -> list[dict[str, Any]]:
    sources = list(contract["required_event_sources"])
    transforms = ["direction", "intensity", "pre5_interaction", "pre15_interaction"]
    variants = ["raw", "compressed"]
    rng = np.random.default_rng(seed)
    chosen: list[tuple[str, int, str, str]] = []
    per_source_budget = int(contract["budgets"]["event_conditioned"]["proposal"]) // len(sources)
    if per_source_budget * len(sources) != int(contract["budgets"]["event_conditioned"]["proposal"]):
        raise ValueError("event proposal budget must divide evenly across required event sources")
    for source in sources:
        source_specs = [
            (source, horizon, transform, variant)
            for horizon in contract["horizons_bars"] for transform in transforms for variant in variants
        ]
        indices = sorted(rng.choice(len(source_specs), size=per_source_budget, replace=False))
        chosen.extend(source_specs[index] for index in indices)
    source_groups = {
        source: observations.loc[observations["event_source"].eq(source)] for source in sources
    }
    placebo_cache: dict[tuple[str, int], pd.Series] = {}
    rows: list[dict[str, Any]] = []
    for source, horizon, transform, variant in chosen:
        group = source_groups[source]
        target = pd.to_numeric(group[f"target_h{horizon}"], errors="coerce")
        event_signal = _variant(_signal(group, transform, event=True), variant)
        control_signal = _variant(_signal(group, transform, event=False), variant)
        placebo_key = (source, horizon)
        if placebo_key not in placebo_cache:
            placebo_cache[placebo_key] = _placebo_target(group, seed, horizon)
        placebo = placebo_cache[placebo_key]
        event_reward, support = _reward(event_signal, target)
        structural_reward, _ = _reward(control_signal, target)
        placebo_reward, _ = _reward(event_signal, placebo)
        temporal_signal = pd.to_numeric(group["pre_return_5"], errors="coerce") - pd.to_numeric(group["pre_return_15"], errors="coerce")
        temporal_reward, _ = _reward(temporal_signal, target)
        static_reward, _ = _reward(pd.to_numeric(group["pre_return_5"], errors="coerce"), target)
        controls = {
            "structural_control": structural_reward,
            "episode_placebo": placebo_reward,
            "static_control": static_reward,
            "temporal_control": temporal_reward,
        }
        finite_controls = [value for value in controls.values() if np.isfinite(value)]
        control_ceiling = max(finite_controls) if finite_controls else float("nan")
        increment = event_reward - control_ceiling if np.isfinite(event_reward) and np.isfinite(control_ceiling) else float("nan")
        blocks = _time_block(group["session"])
        block_increment: dict[str, float | None] = {}
        for block in ("2024H1", "2024H2", "2025H1", "2025H2"):
            mask = blocks.eq(block)
            er, _ = _reward(event_signal.loc[mask], target.loc[mask])
            cr, _ = _reward(control_signal.loc[mask], target.loc[mask])
            block_increment[block] = er - cr if np.isfinite(er) and np.isfinite(cr) else None
        consistent = sum(value is not None and value > 0 for value in block_increment.values())
        date_share = group["session"].value_counts(normalize=True).max() if not group.empty else 1.0
        symbols = group.loc[group["entity_scope"].eq("SYMBOL"), "entity_id"]
        symbol_share = symbols.value_counts(normalize=True).max() if not symbols.empty else 0.0
        canonical = f"{source}|h{horizon}|{transform}|{variant}"
        exact_id = hashlib.sha256(f"{seed}|{canonical}".encode()).hexdigest()[:24]
        mechanism_id = hashlib.sha256(canonical.encode()).hexdigest()[:16]
        survivor = (
            support >= 30 and np.isfinite(increment) and increment > 0 and consistent >= 3
            and date_share <= 0.25 and symbol_share <= 0.20
        )
        base = {
            "seed": seed, "source": source, "horizon": horizon, "transform": transform, "variant": variant,
            "canonical": canonical, "exact_id": exact_id, "mechanism_id": mechanism_id,
            "support": support, "event_reward": event_reward, "matched_control_ceiling": control_ceiling,
            "matched_increment": increment, "consistent_positive_blocks": consistent,
            "block_increment": block_increment, "top_date_share": float(date_share),
            "top_symbol_share": float(symbol_share), "survivor": bool(survivor),
        }
        rows.append({**base, "lane": "event_conditioned", "reward": event_reward})
        for lane, reward in controls.items():
            rows.append({**base, "lane": lane, "reward": reward, "survivor": False})
    return rows


def _lane_summary(frame: pd.DataFrame, contract: dict[str, Any]) -> dict[str, Any]:
    summaries: dict[str, Any] = {}
    for lane, group in frame.groupby("lane", sort=True):
        budget = contract["budgets"][lane]
        ranked = group.sort_values(["reward", "exact_id"], ascending=[False, True], na_position="last")
        admission = ranked.head(int(budget["admission"]))
        strict = admission.head(int(budget["strict"]))
        counts = group["mechanism_id"].value_counts()
        probabilities = counts / counts.sum()
        summaries[str(lane)] = {
            "proposal": len(group), "legal": int(group["reward"].notna().sum()),
            "canonical": int(group["canonical"].nunique()), "exact": int(group["exact_id"].nunique()),
            "signal_cluster": int(group["mechanism_id"].nunique()),
            "n_eff": float(1.0 / np.square(probabilities).sum()),
            "admission": len(admission), "strict": len(strict),
            "survivor": int(group["survivor"].sum()) if lane == "event_conditioned" else 0,
        }
    return summaries


def run_canary(*, contract_path: Path, semantic_registry_path: Path, data_root: Path, chip_root: Path, output_root: Path, repo_sha: str) -> dict[str, Any]:
    started = time.time()
    contract = json.loads(contract_path.read_text(encoding="utf-8"))
    semantics = json.loads(semantic_registry_path.read_text(encoding="utf-8"))
    validate_canary_contract(contract)
    validate_semantic_registry(semantics)
    output_root.mkdir(parents=True, exist_ok=True)
    files = [_discover_shard_file(data_root, shard) for shard in contract["data_release"]["selected_shards"]]
    access = {
        "data_role": "development_only", "opened_files": [str(path) for path in files],
        "validation_reads": 0, "holdout_reads": 0, "forward_2026_reads": 0,
        "forbidden_file_reads": 0, "forbidden_row_group_reads": 0,
    }
    chip = _load_chip_context(chip_root)
    all_episodes: list[pd.DataFrame] = []
    all_observations: list[pd.DataFrame] = []
    lifecycle_manifests = []
    validations = []
    shard_records = []
    for path in files:
        schema = set(pq.ParquetFile(path).schema_arrow.names)
        columns = _required_columns(schema)
        frame = pd.read_parquet(path, columns=columns)
        frame["code"] = frame["code"].astype(str).str.extract(r"(\d{6})", expand=False)
        if frame["code"].isna().any():
            raise RuntimeError("development shard contains an unrecognized security code")
        years = pd.to_datetime(frame["trade_time"], errors="raise").dt.year
        if not years.isin(contract["boundaries"]["allowed_years"]).all():
            raise RuntimeError("physical development shard exposed a forbidden year")
        life_frame, life_episodes, life_manifest = materialize_conservative_limit_lifecycle(
            frame[[column for column in ("code", "trade_time", "open", "high", "low", "close", "ctx_hfq_is_st", "evt_uplimit_active", "evt_uplimit_amount") if column in frame]]
        )
        validation = validate_derived_limits_against_vendor_occurrence(life_frame)
        codes = set(frame["code"].astype(str).unique())
        episodes, _ = materialize_broad_event_episodes(
            frame, lifecycle_episodes=life_episodes, chip_context=chip.loc[chip["code"].isin(codes)]
        )
        observations = _attach_episode_outcomes(frame, episodes, contract["horizons_bars"])
        all_episodes.append(episodes)
        all_observations.append(observations)
        lifecycle_manifests.append(life_manifest)
        validations.append(validation)
        shard_records.append({"path": str(path), "rows": len(frame), "episodes": len(episodes), "columns": columns})
        del frame, life_frame, life_episodes, episodes, observations
        gc.collect()
    episodes = pd.concat(all_episodes, ignore_index=True).drop_duplicates("episode_id", keep="first")
    observation_parts = pd.concat(all_observations, ignore_index=True)
    symbol_observations = observation_parts.loc[observation_parts["entity_scope"].eq("SYMBOL")].drop_duplicates(
        "episode_id", keep="first"
    )
    market_parts = observation_parts.loc[observation_parts["entity_scope"].eq("MARKET")]
    if market_parts.empty:
        market_observations = market_parts
    else:
        market_observations = market_parts.drop_duplicates("episode_id", keep="first").copy()
        numeric_outcomes = [
            column for column in market_parts.columns
            if column in {"close", "pre_return_5", "pre_return_15"}
            or column.startswith("target_h")
            or column.startswith("placebo_target_h")
        ]
        means = market_parts.groupby("episode_id", sort=False)[numeric_outcomes].mean()
        market_observations = market_observations.drop(columns=numeric_outcomes).merge(
            means, left_on="episode_id", right_index=True, how="left", validate="one_to_one"
        )
    observations = pd.concat([symbol_observations, market_observations], ignore_index=True)
    allowed_years = set(int(year) for year in contract["boundaries"]["allowed_years"])
    observations = observations.loc[
        observations["close"].notna()
        & pd.to_datetime(observations["session"], errors="coerce").dt.year.isin(allowed_years)
    ].copy()
    valid_episode_ids = set(observations["episode_id"].astype(str))
    episodes = episodes.loc[episodes["episode_id"].astype(str).isin(valid_episode_ids)].copy()
    support = audit_episode_support(episodes)
    combined_lifecycle = {
        "one_episode_one_admission_vote": all(row["one_episode_one_admission_vote"] for row in lifecycle_manifests),
        "exact_limit_session_count": sum(row["exact_limit_session_count"] for row in lifecycle_manifests),
        "derived_limit_session_count": sum(row["derived_limit_session_count"] for row in lifecycle_manifests),
        "vendor_confirmed_limit_session_count": sum(
            row.get("vendor_confirmed_limit_session_count", 0) for row in lifecycle_manifests
        ),
        "episode_count": sum(row["episode_count"] for row in lifecycle_manifests),
    }
    vendor_total = sum(row.get("vendor_episode_count", 0) for row in validations)
    matched_total = sum(row.get("matched_within_two_minutes", 0) for row in validations)
    limit_validation = {
        "decision": "CONSERVATIVE_LIMIT_VENDOR_CONSISTENCY_PASS" if vendor_total >= 30 and matched_total / max(vendor_total, 1) >= 0.95 else "CONSERVATIVE_LIMIT_VENDOR_CONSISTENCY_FAIL",
        "vendor_episode_count": vendor_total,
        "matched_within_two_minutes": matched_total,
        "match_rate": matched_total / max(vendor_total, 1),
    }
    controls = matched_control_contract()
    preflight = run_preflight(
        contract=contract, semantic_registry=semantics, episode_support=support,
        lifecycle_manifest=combined_lifecycle, limit_validation=limit_validation,
        access_ledger=access, controls=controls,
    )
    _write_json(output_root / "preflight.json", preflight)
    _write_json(output_root / "episode_support.json", support)
    _write_json(output_root / "limit_validation.json", limit_validation)
    _write_json(output_root / "access_ledger.json", access)
    if not preflight["canary_authorized"]:
        summary = {"status": preflight["decision"], "canary_executed": False, "preflight": preflight}
        _write_json(output_root / "summary.json", summary)
        return summary
    candidate_parts = []
    seed_reports = {}
    for seed in contract["seeds"]:
        rows = pd.DataFrame(_candidate_rows(observations, contract, int(seed)))
        candidate_parts.append(rows)
        seed_reports[str(seed)] = {
            "lanes": _lane_summary(rows, contract),
            "survivor_mechanisms": sorted(rows.loc[rows["lane"].eq("event_conditioned") & rows["survivor"], "mechanism_id"].unique().tolist()),
            "survivor_sources": sorted(rows.loc[rows["lane"].eq("event_conditioned") & rows["survivor"], "source"].unique().tolist()),
        }
    candidates = pd.concat(candidate_parts, ignore_index=True)
    candidates.drop(columns="block_increment").to_csv(output_root / "candidate_results.csv", index=False)
    seed_sets = [set(report["survivor_mechanisms"]) for report in seed_reports.values()]
    shared = set.intersection(*seed_sets) if seed_sets else set()
    event_rows = candidates.loc[candidates["lane"].eq("event_conditioned")]
    positive_increment = event_rows["matched_increment"].dropna().mean() > 0
    if shared and positive_increment:
        decision = "BROAD_EVENT_INCREMENT_OBSERVED_REPRODUCIBLE"
        disposition = "NEXT_DISCOVERY_ELIGIBLE_NO_AUTOMATIC_MAIN_SEARCH_ENTRY"
    elif all(not values for values in seed_sets):
        decision = "REPEATED_NO_INCREMENT_WITH_ADEQUATE_SUPPORT"
        disposition = "EXPLORATORY_EVENT_ARCHIVE"
    else:
        decision = "CANARY_NO_INCREMENT_OBSERVED"
        disposition = "EXPLORATORY_EVENT_ARCHIVE"
    summary = {
        "status": decision, "canary_executed": True, "disposition": disposition,
        "repo_sha": repo_sha, "contract_hash": contract["contract_hash"],
        "semantic_registry_hash": semantics["registry_hash"],
        "data_release_manifest_sha256": contract["data_release"]["manifest_sha256"],
        "split_manifest_sha256": contract["split_manifest_sha256"],
        "seed_reports": seed_reports, "shared_survivor_mechanisms": sorted(shared),
        "mean_matched_increment": float(event_rows["matched_increment"].dropna().mean()),
        "episode_count": len(episodes), "observation_count": len(observations),
        "operational_event_sources": support["operational_sources"],
        "limit_validation": limit_validation, "access_ledger": access,
        "elapsed_seconds": time.time() - started,
        "forward_2026_accessed": False, "candidate_promotion": False,
        "cross_sprint_memory_persisted": False, "plate_industry_used": False,
    }
    _write_json(output_root / "summary.json", summary)
    manifest = {
        "version": RUNNER_VERSION, "command_contract": str(contract_path), "repo_sha": repo_sha,
        "inputs": {
            "contract": {"path": str(contract_path), "sha256": _sha256(contract_path)},
            "semantic_registry": {"path": str(semantic_registry_path), "sha256": _sha256(semantic_registry_path)},
            "data_files": shard_records,
            "chip_manifest": {"path": str(chip_root / "chip_sidecar_manifest_v1.json"), "sha256": _sha256(chip_root / "chip_sidecar_manifest_v1.json")},
        },
        "outputs": [
            {"path": path.name, "sha256": _sha256(path), "size": path.stat().st_size}
            for path in sorted(output_root.iterdir()) if path.is_file() and path.name != "run_manifest.json"
        ],
        "reproducibility": "YES_FIXED_CONTRACT_TWO_SEEDS",
        "failure_reason": "",
    }
    _write_json(output_root / "run_manifest.json", manifest)
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--contract", type=Path, required=True)
    parser.add_argument("--semantic-registry", type=Path, required=True)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--chip-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--repo-sha", required=True)
    args = parser.parse_args(argv)
    summary = run_canary(
        contract_path=args.contract.resolve(), semantic_registry_path=args.semantic_registry.resolve(),
        data_root=args.data_root.resolve(), chip_root=args.chip_root.resolve(),
        output_root=args.output_root.resolve(), repo_sha=args.repo_sha,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True, default=str))
    return 0 if summary.get("canary_executed") else 2


if __name__ == "__main__":
    raise SystemExit(main())
