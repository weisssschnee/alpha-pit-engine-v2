from __future__ import annotations

import argparse
import json
from pathlib import Path

from our_system_phase2.services.broad_event_preflight import contract_hash, validate_canary_contract


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--contract", type=Path, required=True)
    args = parser.parse_args(argv)
    payload = json.loads(args.contract.read_text(encoding="utf-8"))
    payload["contract_hash"] = contract_hash(payload)
    args.contract.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    validate_canary_contract(payload)
    print(json.dumps({"contract_hash": payload["contract_hash"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
