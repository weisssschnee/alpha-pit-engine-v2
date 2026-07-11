from __future__ import annotations

import app


def test_external_sidecar_builder_is_registered_as_an_app_route() -> None:
    assert (
        app.ROUTES["nextgen-build-external-sidecars"]
        == "scripts.build_nextgen_external_sidecars"
    )
    main = app._load_main("nextgen-build-external-sidecars")
    assert main.__module__ == "scripts.build_nextgen_external_sidecars"
