"""Non-semantic ask-phase labels for existing route-local Optuna studies."""

SOURCE_SAMPLING_PHASES = frozenset(
    {"AVAILABILITY_FIXED", "STARTUP_RANDOM", "TPE_GUIDED"}
)


def source_sampling_phase_v0(
    *, optimizer_ask_kind: str, trial_number: int, n_startup_trials: int
) -> str:
    """Classify ask provenance without changing the route startup threshold."""

    if int(n_startup_trials) < 1:
        raise ValueError("n_startup_trials must remain positive")
    if str(optimizer_ask_kind) == "AVAILABILITY_FIXED_ENQUEUED":
        return "AVAILABILITY_FIXED"
    if str(optimizer_ask_kind) not in {"", "TPE_NATIVE_DRAW"}:
        raise ValueError(f"unknown route optimizer ask kind: {optimizer_ask_kind}")
    return (
        "STARTUP_RANDOM"
        if int(trial_number) < int(n_startup_trials)
        else "TPE_GUIDED"
    )
