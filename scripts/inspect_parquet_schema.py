from __future__ import annotations

import argparse
import json
from pathlib import Path

import pyarrow.parquet as pq


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("path", type=Path)
    args = parser.parse_args()
    parquet = pq.ParquetFile(args.path)
    print(
        json.dumps(
            {
                "path": str(args.path),
                "rows": parquet.metadata.num_rows,
                "row_groups": parquet.num_row_groups,
                "schema": {field.name: str(field.type) for field in parquet.schema_arrow},
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
