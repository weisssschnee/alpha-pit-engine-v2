"""Materialize a hash-valid, zero-financial search preflight authority."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def materialize_authorization(source: Path) -> dict[str, Any]:
    payload = json.loads(Path(source).read_text(encoding="utf-8-sig"))
    claimed = str(payload.pop("resource_topology_authorization_sha256", ""))
    if not claimed or claimed != _stable_hash(payload):
        raise RuntimeError("source resource topology authorization hash drift")
    payload.update(
        {
            "status": "ZERO_FINANCIAL_PREFLIGHT_AUTHORIZED",
            "execution_authorized": False,
            "financial_campaign_authorized": False,
            "qualification_authorized": True,
        }
    )
    payload["resource_topology_authorization_sha256"] = _stable_hash(payload)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    payload = materialize_authorization(args.source)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
