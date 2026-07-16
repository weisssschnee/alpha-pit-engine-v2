from __future__ import annotations

import argparse
import json
import os
from collections import Counter, defaultdict
from pathlib import Path

import polars as pl


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-root", type=Path, required=True)
    parser.add_argument("--polars-threads", type=int, required=True)
    args = parser.parse_args()

    expected = {
        "NUMBA_NUM_THREADS": "1",
        "ARROW_NUM_THREADS": "1",
        "OMP_NUM_THREADS": "1",
        "MKL_NUM_THREADS": "1",
        "OPENBLAS_NUM_THREADS": "1",
        "NUMEXPR_MAX_THREADS": "1",
        "POLARS_MAX_THREADS": str(int(args.polars_threads)),
    }
    observed = {key: os.environ.get(key) for key in expected}
    if observed != expected:
        raise RuntimeError(f"thread environment is not frozen: observed={observed} expected={expected}")
    files = tuple(sorted(args.input_root.resolve().glob("shard_*.parquet")))
    if len(files) != 16:
        raise RuntimeError(f"expected 16 sidecars, got {len(files)}")

    records: list[dict[str, object]] = []
    shards_by_code: dict[str, set[int]] = defaultdict(set)
    fragments_by_code: dict[str, list[dict[str, object]]] = defaultdict(list)
    for shard, path in enumerate(files):
        aggregate = (
            pl.scan_parquet(path, low_memory=True)
            .select(
                pl.len().alias("rows"),
                pl.col("code").n_unique().alias("code_count"),
                pl.col("trade_time").min().alias("min_trade_time"),
                pl.col("trade_time").max().alias("max_trade_time"),
                pl.col("trade_time").n_unique().alias("trade_time_count"),
            )
            .collect(engine="streaming")
            .row(0, named=True)
        )
        codes = (
            pl.scan_parquet(path, low_memory=True)
            .select(pl.col("code").cast(pl.String).unique())
            .collect(engine="streaming")["code"]
            .to_list()
        )
        for code in codes:
            shards_by_code[str(code)].add(shard)
        fragments = (
            pl.scan_parquet(path, low_memory=True)
            .group_by(pl.col("code").cast(pl.String).alias("code"))
            .agg(
                pl.len().alias("rows"),
                pl.col("trade_time").min().alias("min_trade_time"),
                pl.col("trade_time").max().alias("max_trade_time"),
            )
            .collect(engine="streaming")
        )
        for fragment in fragments.iter_rows(named=True):
            fragments_by_code[str(fragment["code"])].append(
                {
                    "shard": shard,
                    "rows": int(fragment["rows"]),
                    "min_trade_time": str(fragment["min_trade_time"]),
                    "max_trade_time": str(fragment["max_trade_time"]),
                }
            )
        records.append(
            {
                "shard": shard,
                "path": str(path),
                "rows": int(aggregate["rows"]),
                "code_count": int(aggregate["code_count"]),
                "trade_time_count": int(aggregate["trade_time_count"]),
                "min_trade_time": str(aggregate["min_trade_time"]),
                "max_trade_time": str(aggregate["max_trade_time"]),
            }
        )
    multiplicity = Counter(len(shards) for shards in shards_by_code.values())
    membership_pairs = Counter(tuple(sorted(shards)) for shards in shards_by_code.values())
    fragment_relation = Counter()
    fragment_samples: list[dict[str, object]] = []
    for code, fragments in sorted(fragments_by_code.items()):
        ordered = sorted(fragments, key=lambda row: (str(row["min_trade_time"]), int(row["shard"])))
        if len(ordered) == 1:
            fragment_relation["single_fragment"] += 1
            continue
        if len(ordered) != 2:
            fragment_relation["more_than_two_fragments"] += 1
            continue
        relation = (
            "strictly_ordered_nonoverlap"
            if str(ordered[0]["max_trade_time"]) < str(ordered[1]["min_trade_time"])
            else "overlap_or_interleave"
        )
        fragment_relation[relation] += 1
        if len(fragment_samples) < 20:
            fragment_samples.append({"code": code, "relation": relation, "fragments": ordered})
    adjacent_time_ordered = all(
        str(records[index]["max_trade_time"]) < str(records[index + 1]["min_trade_time"])
        for index in range(len(records) - 1)
    )
    report = {
        "schema_version": "cn_phase3cm_physical_shard_layout_audit_v1",
        "status": "SHARD_LAYOUT_AUDIT_COMPLETE",
        "input_root": str(args.input_root.resolve()),
        "shard_count": len(files),
        "total_rows": sum(int(row["rows"]) for row in records),
        "global_code_count": len(shards_by_code),
        "codes_in_multiple_shards": sum(1 for shards in shards_by_code.values() if len(shards) > 1),
        "max_shards_per_code": max(map(len, shards_by_code.values()), default=0),
        "code_shard_multiplicity": {str(key): value for key, value in sorted(multiplicity.items())},
        "code_membership_pairs": {
            "+".join(str(value) for value in key): count
            for key, count in sorted(membership_pairs.items())
        },
        "fragment_relation": dict(sorted(fragment_relation.items())),
        "fragment_samples": fragment_samples,
        "adjacent_shards_strictly_time_ordered": adjacent_time_ordered,
        "label_shift_partition_safe": all(len(shards) == 1 for shards in shards_by_code.values()),
        "shards": records,
        "data_role": "development_train_only",
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "thread_environment": expected,
    }
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
