from __future__ import annotations

from pathlib import Path

from scripts import build_cn_search_core_v2_production_wave1_validation_prep_authorization as auth_builder
from scripts import prepare_cn_search_core_v2_production_wave1_validation_v1 as prep


def test_validation_prep_source_hash_is_crlf_invariant_but_raw_hash_is_not(tmp_path: Path) -> None:
    lf = tmp_path / "lf.py"
    crlf = tmp_path / "crlf.py"
    lf.write_bytes(b"a=1\nb=2\n")
    crlf.write_bytes(b"a=1\r\nb=2\r\n")

    assert auth_builder._source_sha(lf) == auth_builder._source_sha(crlf)
    assert prep._source_sha(lf) == prep._source_sha(crlf)
    assert auth_builder._source_sha(lf) == prep._source_sha(crlf)
    assert auth_builder._sha(lf) != auth_builder._sha(crlf)
    assert prep._sha(lf) != prep._sha(crlf)