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


def authorization_bytes(payload: dict[str, Any]) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")


def planned_authorization_binding(
    source: Path, future_output: Path
) -> dict[str, str]:
    """Derive exact future preflight metadata without creating its output root."""

    payload = materialize_authorization(source)
    return {
        "campaign_authorization_path": str(Path(future_output).resolve()),
        "campaign_authorization_file_sha256": hashlib.sha256(
            authorization_bytes(payload)
        ).hexdigest(),
        "target_campaign_instance_id": str(payload.get("campaign_id") or ""),
        "target_campaign_profile": str(payload.get("campaign_profile") or ""),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    payload = materialize_authorization(args.source)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(authorization_bytes(payload))
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
