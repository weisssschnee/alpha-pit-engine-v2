from __future__ import annotations

import app


def test_external_sidecar_builder_is_registered_as_an_app_route() -> None:
    assert (
        app.ROUTES["nextgen-build-external-sidecars"]
        == "scripts.build_nextgen_external_sidecars"
    )
    main = app._load_main("nextgen-build-external-sidecars")
    assert main.__module__ == "scripts.build_nextgen_external_sidecars"


def test_true1min_plate_materialization_smoke_is_registered_as_an_app_route() -> None:
    assert (
        app.ROUTES["nextgen-true1min-plate-materialization-smoke"]
        == "our_system_phase2.runtime.nextgen_true1min_plate_materialization_smoke"
    )


def test_nextgen_dark_development_canary_is_registered_as_an_app_route() -> None:
    assert (
        app.ROUTES["nextgen-dark-development-canary"]
        == "our_system_phase2.runtime.nextgen_dark_development_canary"
    )


def test_cn_b1s_development_canary_is_registered_as_an_app_route() -> None:
    assert (
        app.ROUTES["cn-b1s-development-canary"]
        == "our_system_phase2.runtime.cn_b1s_development_canary"
    )


def test_development_only_release_builder_is_registered_as_an_app_route() -> None:
    assert (
        app.ROUTES["build-development-only-true1min-release"]
        == "our_system_phase2.runtime.build_development_only_true1min_release"
    )


def test_cn_generator_funnel_diagnosis_is_registered_as_an_app_route() -> None:
    assert (
        app.ROUTES["cn-generator-funnel-diagnosis"]
        == "our_system_phase2.runtime.cn_generator_funnel_diagnosis"
    )
