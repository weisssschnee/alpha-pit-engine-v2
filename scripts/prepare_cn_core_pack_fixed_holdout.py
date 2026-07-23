from __future__ import annotations

import argparse
import json
import platform
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
SRC = REPO / "src"
for search_path in (REPO, SRC):
    if str(search_path) not in sys.path:
        sys.path.insert(0, str(search_path))

from our_system_phase2.runtime.cn_iterative_search_v1 import (
    _clock_for_route,
    _sha256,
    _write_csv,
)
from scripts.run_cn_core_pack_fixed_holdout import (
    AUTHORIZED_HOST,
    _freeze_candidates,
    _write_json,
)


def run(args: argparse.Namespace) -> dict[str, Any]:
    if platform.node().upper() != AUTHORIZED_HOST:
        raise RuntimeError(
            f"fixed holdout preparation is authorized only on {AUTHORIZED_HOST}"
        )
    campaign_root = args.campaign_root.resolve()
    freeze_path = args.freeze_manifest.resolve()
    freeze = json.loads(freeze_path.read_text(encoding="utf-8"))
    candidate_ledger = campaign_root / "candidate_ledger.parquet"
    expected = str(
        freeze["protected_train_artifact_hashes"]["candidate_ledger.parquet"]
    )
    if _sha256(candidate_ledger) != expected:
        raise RuntimeError("protected candidate ledger hash drift")
    candidates = _freeze_candidates(
        candidate_ledger_path=candidate_ledger,
        freeze=freeze,
    )
    rows_by_backend: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in candidates:
        rows_by_backend[_clock_for_route(str(row["route_id"]))].append(row)
    output_root = args.output_root.resolve()
    tables = {
        backend: _write_csv(
            output_root / f"fixed_holdout_{backend}_candidates.csv",
            rows,
        )
        for backend, rows in rows_by_backend.items()
    }
    receipt = {
        "schema_version": "cn_core_pack_fixed_holdout_preparation_v1",
        "status": "FROZEN_CANDIDATE_TABLES_PREPARED",
        "freeze_manifest_sha256": _sha256(freeze_path),
        "candidate_ledger_sha256": expected,
        "pair_count": int(freeze["frozen_pair_count"]),
        "candidate_member_count": len(candidates),
        "backend_tables": {
            backend: {
                "path": str(path),
                "sha256": _sha256(path),
                "pair_count": len(
                    {str(row["pair_id"]) for row in rows_by_backend[backend]}
                ),
            }
            for backend, path in tables.items()
        },
        "holdout_reads": 0,
        "validation_reads": 0,
        "forward_2026_reads": 0,
        "feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
    }
    _write_json(output_root / "FIXED_HOLDOUT_PREPARED.json", receipt)
    return receipt


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--campaign-root", type=Path, required=True)
    parser.add_argument("--freeze-manifest", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args()
    result = run(args)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
