from __future__ import annotations

import argparse
import json
from pathlib import Path

from build_cn_alpha_selection_diagnostic_v1 import verify_diagnostic


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--contract", type=Path, required=True)
    parser.add_argument("--input-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args()
    result = verify_diagnostic(
        output_root=args.output_root,
        contract_path=args.contract,
        input_root=args.input_root,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
