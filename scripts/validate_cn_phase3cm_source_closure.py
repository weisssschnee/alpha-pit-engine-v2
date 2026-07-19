from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate a transferred Phase3CM critical source closure without Git."
    )
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--expected-repo-sha", required=True)
    args = parser.parse_args(argv)

    repo_root = args.repo_root.resolve()
    sys.path.insert(0, str(repo_root / "src"))
    sys.path.insert(0, str(repo_root / "scripts"))
    from preflight_cn_phase3cm_dag_cache import validate_source_closure_manifest

    payload = validate_source_closure_manifest(
        args.manifest.resolve(),
        repo_root=repo_root,
        expected_repo_sha=str(args.expected_repo_sha),
    )
    print(
        json.dumps(
            {
                "status": payload["status"],
                "repo_sha": payload["repo_sha"],
                "source_count": len(payload["sources"]),
                "source_closure_hash": payload["source_closure_hash"],
                "manifest_hash": payload["manifest_hash"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
