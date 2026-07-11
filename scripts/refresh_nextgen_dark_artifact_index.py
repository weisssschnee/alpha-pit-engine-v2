"""Refresh NEXTGEN-DARK artifact evidence from the repository filesystem."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import tempfile


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def refresh(repo: Path, index_path: Path) -> dict:
    index = json.loads(index_path.read_text(encoding="utf-8"))
    for row in index["artifacts"]:
        path = repo / row["path"]
        exists = path.is_file()
        row["exists"] = exists
        row["size_bytes"] = path.stat().st_size if exists else 0
        row["last_verified_sha"] = _sha256(path) if exists else ""
    index["refreshed_at"] = datetime.now(timezone.utc).isoformat()
    descriptor, name = tempfile.mkstemp(
        prefix=f".{index_path.name}.", suffix=".tmp", dir=index_path.parent
    )
    os.close(descriptor)
    temporary = Path(name)
    try:
        temporary.write_text(
            json.dumps(index, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        os.replace(temporary, index_path)
    finally:
        temporary.unlink(missing_ok=True)
    return index


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument(
        "--index",
        type=Path,
        default=Path("reports/nextgen_dark_20260711/ARTIFACT_INDEX.json"),
    )
    args = parser.parse_args(argv)
    repo = args.repo.resolve()
    index_path = args.index if args.index.is_absolute() else repo / args.index
    index = refresh(repo, index_path)
    print(
        json.dumps(
            {
                "status": index["status"],
                "artifact_count": len(index["artifacts"]),
                "index": str(index_path),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
