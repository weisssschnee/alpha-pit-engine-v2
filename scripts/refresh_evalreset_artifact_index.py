"""Refresh file existence, size, and SHA evidence in the Phase A artifact index."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def refresh(index: dict, *, repo: Path) -> dict:
    rows = []
    for row in index.get("artifacts", []):
        item = dict(row)
        path = Path(str(item["path"]))
        resolved = path if path.is_absolute() else repo / path
        item["exists"] = resolved.is_file()
        item["size_bytes"] = resolved.stat().st_size if resolved.is_file() else 0
        item["last_verified_sha"] = _sha256(resolved) if resolved.is_file() else ""
        if item.get("state") == "IMPLEMENTED" and not item["exists"]:
            raise RuntimeError(f"implemented artifact is missing: {item['id']} -> {item['path']}")
        rows.append(item)
    return {**index, "refreshed_at": datetime.now(timezone.utc).isoformat(), "artifacts": rows}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--index", type=Path, required=True)
    args = parser.parse_args(argv)
    repo = Path(__file__).resolve().parents[1]
    payload = json.loads(args.index.read_text(encoding="utf-8"))
    payload = refresh(payload, repo=repo)
    args.index.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"artifact_count": len(payload["artifacts"]), "existing_count": sum(row["exists"] for row in payload["artifacts"])}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
