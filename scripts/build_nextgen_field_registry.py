"""Build a versioned NEXTGEN-DARK field registry from a Parquet schema only."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pyarrow.parquet as pq

from our_system_phase2.services.feature_state_fabric import registry_from_schema_fields


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema-parquet", type=Path, required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--expected-field-count", type=int, default=121)
    args = parser.parse_args(argv)
    names = pq.ParquetFile(args.schema_parquet).schema_arrow.names
    if len(names) != args.expected_field_count:
        raise RuntimeError(f"expected {args.expected_field_count} schema fields, observed {len(names)}")
    registry = registry_from_schema_fields(names, version=args.version)
    registry.write(args.output)
    print(json.dumps({"field_count": len(names), "registry_hash": registry.registry_hash, "output": str(args.output)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
