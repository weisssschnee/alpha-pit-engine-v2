"""Replay the frozen 11-mechanism Broad Event pack on all development shards."""

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from our_system_phase2.runtime.cn_broad_event_canary import (
    _attach_episode_outcomes,
    _behavior_key,
    _build_behavior_registry,
    _discover_shard_file,
    _load_chip_context,
    _placebo_target,
    _required_columns,
    _reward,
    _signal,
    _time_block,
    _variant,
    _write_json,
)
from our_system_phase2.services.broad_event_episodes import (
    audit_episode_support,
    materialize_broad_event_episodes,
)
from our_system_phase2.services.conservative_limit_lifecycle import (
    materialize_conservative_limit_lifecycle,
)


REPLAY_VERSION = "cn_broad_event_frozen_11_full16_replay_v1"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        while block := handle.read(8 * 1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _materialize_observations(
    *,
    data_root: Path,
    chip_root: Path,
    shard_indices: list[int],
    horizons: list[int],
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any], list[dict[str, Any]]]:
    chip = _load_chip_context(chip_root)
    all_episodes: list[pd.DataFrame] = []
    all_observations: list[pd.DataFrame] = []
    files: list[dict[str, Any]] = []
    for shard in shard_indices:
        path = _discover_shard_file(data_root, shard)
        schema = set(pq.ParquetFile(path).schema_arrow.names)
        columns = _required_columns(schema)
        frame = pd.read_parquet(path, columns=columns)
        frame["code"] = frame["code"].astype(str).str.extract(r"(\d{6})", expand=False)
        if frame["code"].isna().any():
            raise RuntimeError("development shard contains an unrecognized security code")
        years = pd.to_datetime(frame["trade_time"], errors="raise").dt.year
        if not years.isin({2024, 2025}).all():
            raise PermissionError("Broad Event replay escaped development years")
        lifecycle_columns = [
            column for column in (
                "code", "trade_time", "open", "high", "low", "close",
                "ctx_hfq_is_st", "evt_uplimit_active", "evt_uplimit_amount",
            ) if column in frame
        ]
        _, lifecycle_episodes, lifecycle_manifest = materialize_conservative_limit_lifecycle(
            frame[lifecycle_columns]
        )
        codes = set(frame["code"].astype(str).unique())
        episodes, _ = materialize_broad_event_episodes(
            frame,
            lifecycle_episodes=lifecycle_episodes,
            chip_context=chip.loc[chip["code"].isin(codes)],
        )
        observations = _attach_episode_outcomes(frame, episodes, horizons)
        all_episodes.append(episodes)
        all_observations.append(observations)
        files.append(
            {
                "shard": shard,
                "path": str(path),
                "sha256": _sha256(path),
                "rows": len(frame),
                "episode_count": len(episodes),
                "one_episode_one_admission_vote": lifecycle_manifest["one_episode_one_admission_vote"],
            }
        )
        del frame, lifecycle_episodes, episodes, observations
        gc.collect()
    episodes = pd.concat(all_episodes, ignore_index=True).drop_duplicates("episode_id", keep="first")
    parts = pd.concat(all_observations, ignore_index=True)
    symbol = parts.loc[parts["entity_scope"].eq("SYMBOL")].drop_duplicates("episode_id", keep="first")
    market_parts = parts.loc[parts["entity_scope"].eq("MARKET")]
    if market_parts.empty:
        market = market_parts
    else:
        market = market_parts.drop_duplicates("episode_id", keep="first").copy()
        numeric = [
            column for column in market_parts
            if column in {"close", "pre_return_5", "pre_return_15"}
            or column.startswith("target_h") or column.startswith("placebo_target_h")
        ]
        means = market_parts.groupby("episode_id", sort=False)[numeric].mean()
        market = market.drop(columns=numeric).merge(
            means, left_on="episode_id", right_index=True, how="left", validate="one_to_one"
        )
    observations = pd.concat([symbol, market], ignore_index=True)
    observations = observations.loc[
        observations["close"].notna()
        & pd.to_datetime(observations["session"], errors="coerce").dt.year.isin({2024, 2025})
    ].copy()
    valid_ids = set(observations["episode_id"].astype(str))
    episodes = episodes.loc[episodes["episode_id"].astype(str).isin(valid_ids)].copy()
    support = audit_episode_support(episodes)
    return episodes, observations, support, files


def _mechanism_rows(
    observations: pd.DataFrame,
    mechanisms: list[dict[str, Any]],
    seeds: list[int],
    behavior_mapping: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for seed in seeds:
        for mechanism in mechanisms:
            source = str(mechanism["source"])
            horizon = int(mechanism["horizon_bars"])
            transform = str(mechanism["transform"])
            variant = str(mechanism["variant"])
            group = observations.loc[observations["event_source"].eq(source)]
            target = pd.to_numeric(group[f"target_h{horizon}"], errors="coerce")
            event_signal = _variant(_signal(group, transform, event=True), variant)
            structural_signal = _variant(_signal(group, transform, event=False), variant)
            placebo = _placebo_target(group, seed, horizon)
            static_signal = pd.to_numeric(group["pre_return_5"], errors="coerce")
            temporal_signal = static_signal - pd.to_numeric(group["pre_return_15"], errors="coerce")
            event_reward, support = _reward(event_signal, target)
            controls = {
                "structural_control": _reward(structural_signal, target)[0],
                "episode_placebo": _reward(event_signal, placebo)[0],
                "static_control": _reward(static_signal, target)[0],
                "temporal_control": _reward(temporal_signal, target)[0],
            }
            finite = [value for value in controls.values() if np.isfinite(value)]
            ceiling = max(finite) if finite else float("nan")
            increment = event_reward - ceiling if np.isfinite(event_reward) and np.isfinite(ceiling) else float("nan")
            blocks = _time_block(group["session"])
            block_increment: dict[str, float | None] = {}
            for block in ("2024H1", "2024H2", "2025H1", "2025H2"):
                mask = blocks.eq(block)
                event_block = _reward(event_signal.loc[mask], target.loc[mask])[0]
                control_block = _reward(structural_signal.loc[mask], target.loc[mask])[0]
                block_increment[block] = (
                    event_block - control_block
                    if np.isfinite(event_block) and np.isfinite(control_block) else None
                )
            consistent = sum(value is not None and value > 0 for value in block_increment.values())
            date_share = float(group["session"].value_counts(normalize=True).max()) if not group.empty else 1.0
            symbols = group.loc[group["entity_scope"].eq("SYMBOL"), "entity_id"]
            symbol_share = float(symbols.value_counts(normalize=True).max()) if not symbols.empty else 0.0
            behavior = behavior_mapping[_behavior_key(source, "event_conditioned", transform, variant)]
            survived = (
                support >= 30 and np.isfinite(increment) and increment > 0 and consistent >= 3
                and date_share <= 0.25 and symbol_share <= 0.20
            )
            rows.append(
                {
                    "seed": seed,
                    "mechanism_id": mechanism["mechanism_id"],
                    "source": source,
                    "horizon_bars": horizon,
                    "transform": transform,
                    "variant": variant,
                    "support": support,
                    "event_reward": event_reward,
                    "matched_control_ceiling": ceiling,
                    "matched_increment": increment,
                    "consistent_positive_blocks": consistent,
                    "top_date_share": date_share,
                    "top_symbol_share": symbol_share,
                    "behavior_identity": behavior["exact_behavior_id"],
                    "behavior_cluster_id": behavior["behavior_cluster_id"],
                    "survivor": survived,
                    "block_increment": block_increment,
                    "control_rewards": controls,
                    "candidate_promotion": False,
                }
            )
    return rows


def run_replay(
    *,
    entry_pack_path: Path,
    data_root: Path,
    chip_root: Path,
    output_root: Path,
    repo_sha: str,
    seeds: list[int],
) -> dict[str, Any]:
    started = time.time()
    entry_pack = json.loads(entry_pack_path.read_text(encoding="utf-8"))
    mechanisms = list(entry_pack["mechanisms"])
    if len(mechanisms) != 11:
        raise ValueError("frozen Broad Event pack must contain exactly 11 mechanisms")
    horizons = sorted({int(row["horizon_bars"]) for row in mechanisms})
    episodes, observations, support, files = _materialize_observations(
        data_root=data_root,
        chip_root=chip_root,
        shard_indices=list(range(16)),
        horizons=horizons,
    )
    behavior_contract = {
        "required_event_sources": sorted(set(row["source"] for row in mechanisms)),
        "behavior_cluster_contract": {
            "minimum_common_episode_count": 30,
            "absolute_correlation_threshold": 0.95,
        },
    }
    behavior_mapping, behavior_report = _build_behavior_registry(observations, behavior_contract)
    rows = _mechanism_rows(observations, mechanisms, seeds, behavior_mapping)
    frame = pd.DataFrame(rows)
    output_root.mkdir(parents=True, exist_ok=True)
    csv_frame = frame.drop(columns=["block_increment", "control_rewards"])
    csv_frame.to_csv(output_root / "frozen_mechanism_results.csv", index=False)
    _write_json(output_root / "behavior_registry.json", behavior_report)
    _write_json(output_root / "episode_support.json", support)
    seed_survivors = {
        str(seed): sorted(frame.loc[frame["seed"].eq(seed) & frame["survivor"], "mechanism_id"].unique())
        for seed in seeds
    }
    shared = sorted(set.intersection(*(set(value) for value in seed_survivors.values()))) if seed_survivors else []
    summary = {
        "status": "BROAD_EVENT_FROZEN_FULL16_REPLAY_COMPLETED",
        "replay_version": REPLAY_VERSION,
        "repo_sha": repo_sha,
        "entry_pack_sha256": _sha256(entry_pack_path),
        "shard_count": len(files),
        "all_shards": [row["shard"] for row in files],
        "mechanism_count": 11,
        "seed_count": len(seeds),
        "episode_count": len(episodes),
        "observation_count": len(observations),
        "operational_sources": support["operational_sources"],
        "seed_survivors": seed_survivors,
        "shared_survivors": shared,
        "mean_matched_increment": float(frame["matched_increment"].dropna().mean()),
        "candidate_promotion": False,
        "cross_sprint_memory": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "elapsed_seconds": time.time() - started,
    }
    _write_json(output_root / "summary.json", summary)
    manifest = {
        "replay_version": REPLAY_VERSION,
        "repo_sha": repo_sha,
        "entry_pack": {"path": str(entry_pack_path), "sha256": _sha256(entry_pack_path)},
        "data_files": files,
        "outputs": [
            {"path": path.name, "sha256": _sha256(path), "size": path.stat().st_size}
            for path in sorted(output_root.iterdir()) if path.is_file() and path.name != "run_manifest.json"
        ],
        "reproducibility": "YES_FIXED_PACK_FULL16_TWO_SEEDS",
        "access_role": "development_only",
    }
    _write_json(output_root / "run_manifest.json", manifest)
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--entry-pack", type=Path, required=True)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--chip-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--repo-sha", required=True)
    parser.add_argument("--seeds", default="1729,2718")
    args = parser.parse_args(argv)
    result = run_replay(
        entry_pack_path=args.entry_pack,
        data_root=args.data_root,
        chip_root=args.chip_root,
        output_root=args.output_root,
        repo_sha=args.repo_sha,
        seeds=[int(value) for value in args.seeds.split(",") if value],
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
