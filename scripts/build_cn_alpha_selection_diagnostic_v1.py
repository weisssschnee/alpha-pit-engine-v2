from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd
from scipy.stats import hypergeom, mannwhitneyu, spearmanr


SCHEMA_VERSION = "cn_alpha_selection_diagnostic_v1"
EXPECTED_SOURCE_PAIRS = 32
EXPECTED_LABELED_PAIRS = 22
EXPECTED_SURVIVORS = 10
EXPECTED_UNLABELED_PAIRS = 10


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(4 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def _write_json(path: Path, payload: Mapping[str, Any]) -> Path:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        )
        + "\n",
        encoding="utf-8",
    )
    return destination


def _artifact(path: Path, *, root: Path) -> dict[str, Any]:
    source = Path(path).resolve()
    return {
        "path": source.relative_to(Path(root).resolve()).as_posix(),
        "bytes": source.stat().st_size,
        "sha256": _sha256(source),
    }


def _require_columns(frame: pd.DataFrame, columns: Iterable[str], label: str) -> None:
    missing = sorted(set(columns) - set(frame.columns))
    if missing:
        raise RuntimeError(f"{label} missing required columns: {missing}")


def _finite(value: Any) -> float | None:
    try:
        rendered = float(value)
    except (TypeError, ValueError):
        return None
    return rendered if math.isfinite(rendered) else None


def _as_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype(float)


def _quality_percentile(series: pd.Series, *, higher_is_better: bool) -> pd.Series:
    numeric = _as_numeric(series)
    if numeric.isna().any():
        raise RuntimeError(f"ranker feature contains missing values: {series.name}")
    quality = numeric if higher_is_better else -numeric
    return quality.rank(method="average", pct=True)


def _bootstrap_median_ci(
    values: np.ndarray,
    *,
    iterations: int,
    rng: np.random.Generator,
) -> tuple[float | None, float | None]:
    values = values[np.isfinite(values)]
    if values.size == 0:
        return None, None
    indices = rng.integers(0, values.size, size=(iterations, values.size))
    medians = np.median(values[indices], axis=1)
    low, high = np.quantile(medians, [0.025, 0.975])
    return float(low), float(high)


def _rank_biserial(a: np.ndarray, b: np.ndarray) -> float | None:
    a = a[np.isfinite(a)]
    b = b[np.isfinite(b)]
    if not len(a) or not len(b):
        return None
    u = float(mannwhitneyu(a, b, alternative="two-sided", method="auto").statistic)
    return float((2.0 * u) / (len(a) * len(b)) - 1.0)


def _bh_fdr(values: Sequence[float | None]) -> list[float | None]:
    valid = [(index, float(value)) for index, value in enumerate(values) if value is not None]
    if not valid:
        return [None] * len(values)
    ordered = sorted(valid, key=lambda item: item[1])
    count = len(ordered)
    adjusted: dict[int, float] = {}
    running = 1.0
    for reverse_rank, (index, value) in enumerate(reversed(ordered), start=1):
        rank = count - reverse_rank + 1
        running = min(running, value * count / rank)
        adjusted[index] = min(1.0, running)
    return [adjusted.get(index) for index in range(len(values))]


def _summary(values: pd.Series, *, iterations: int, seed: int) -> dict[str, Any]:
    numeric = _as_numeric(values)
    finite = numeric[np.isfinite(numeric)]
    if finite.empty:
        return {
            "count": 0,
            "missing_count": int(len(numeric)),
            "median": None,
            "p25": None,
            "p75": None,
            "bootstrap_median_ci95_low": None,
            "bootstrap_median_ci95_high": None,
        }
    rng = np.random.default_rng(seed)
    low, high = _bootstrap_median_ci(
        finite.to_numpy(dtype=float), iterations=iterations, rng=rng
    )
    return {
        "count": int(len(finite)),
        "missing_count": int(numeric.isna().sum()),
        "median": float(finite.median()),
        "p25": float(finite.quantile(0.25)),
        "p75": float(finite.quantile(0.75)),
        "bootstrap_median_ci95_low": low,
        "bootstrap_median_ci95_high": high,
    }


def _verify_bound_artifacts(contract: Mapping[str, Any], input_root: Path) -> list[dict[str, Any]]:
    verified: list[dict[str, Any]] = []
    for record in contract["source_artifacts"]:
        path = input_root / str(record["scope"]) / str(record["name"])
        if not path.is_file():
            raise RuntimeError(f"bound source artifact missing: {path}")
        actual = _sha256(path)
        expected = str(record["sha256"])
        if actual != expected:
            raise RuntimeError(f"bound source artifact hash drift: {path}")
        verified.append(
            {
                "scope": str(record["scope"]),
                "name": str(record["name"]),
                "bytes": path.stat().st_size,
                "sha256": actual,
            }
        )
    return verified


def _load_candidate_diagnosis(
    *, input_root: Path, contract: Mapping[str, Any]
) -> tuple[pd.DataFrame, list[str], list[str]]:
    review = pd.read_parquet(input_root / "freeze" / "finalist_review_ledger.parquet")
    oos = pd.read_parquet(input_root / "oos" / "oos_pair_metrics.parquet")
    transition = pd.read_parquet(
        input_root / "decoder" / "candidate_transition_matrix.parquet"
    )
    _require_columns(
        review,
        (
            "pair_id",
            "primary_candidate_id",
            "control_candidate_id",
            "source_pair_order",
            "finalist_outcome",
            "decoder_id",
            "validation_reads",
            "holdout_reads",
            "forward_2026_reads",
        ),
        "finalist review ledger",
    )
    _require_columns(
        oos,
        (
            "pair_id",
            "finalist_order",
            "absolute_reward_positive",
            "absolute_return_positive",
            "matched_reward_increment_positive",
            "matched_return_increment_positive",
            "all_four_economic_gates_positive",
            "interstage_filter_applied",
            "promotion_authorized",
        ),
        "adaptive validation pair metrics",
    )
    _require_columns(
        transition,
        ("pair_id", "candidate_id", "signal_rank_ic_mean"),
        "decoder transition matrix",
    )
    if len(review) != EXPECTED_SOURCE_PAIRS or review["pair_id"].astype(str).nunique() != EXPECTED_SOURCE_PAIRS:
        raise RuntimeError("source 32-pair identity/count drift")
    review = review.sort_values("source_pair_order", kind="stable").reset_index(drop=True)
    if review["source_pair_order"].astype(int).tolist() != list(range(1, 33)):
        raise RuntimeError("source pair order drift")
    if not review["decoder_id"].astype(str).eq("TOPK_10_EQUAL").all():
        raise RuntimeError("source decoder drift")
    if int(review["validation_reads"].fillna(0).sum()) != 0:
        raise RuntimeError("train evidence contains validation reads")
    if int(review["holdout_reads"].fillna(0).sum()) != 0:
        raise RuntimeError("train evidence contains holdout reads")
    if int(review["forward_2026_reads"].fillna(0).sum()) != 0:
        raise RuntimeError("train evidence contains 2026 reads")
    if len(oos) != EXPECTED_LABELED_PAIRS or oos["pair_id"].astype(str).nunique() != EXPECTED_LABELED_PAIRS:
        raise RuntimeError("adaptive validation 22-pair identity/count drift")
    if not oos["absolute_reward_positive"].astype(bool).all() or not oos[
        "absolute_return_positive"
    ].astype(bool).all():
        raise RuntimeError("labeled adaptive-validation universe is not absolute-positive")
    if oos["interstage_filter_applied"].astype(bool).any():
        raise RuntimeError("adaptive validation evidence applied an interstage filter")
    if oos["promotion_authorized"].astype(bool).any():
        raise RuntimeError("adaptive validation evidence authorized promotion")
    labeled_ids = oos["pair_id"].astype(str).tolist()
    selected_ids = review.loc[
        review["finalist_outcome"].astype(str).eq("FROZEN_DECODER_V2_TRAIN_ONLY_FINALIST"),
        "pair_id",
    ].astype(str).tolist()
    if labeled_ids != selected_ids:
        raise RuntimeError("adaptive-validation identity/order drift from train freeze")
    unlabeled_ids = review.loc[
        ~review["pair_id"].astype(str).isin(set(labeled_ids)), "pair_id"
    ].astype(str).tolist()
    if len(unlabeled_ids) != EXPECTED_UNLABELED_PAIRS:
        raise RuntimeError("unlabeled train-reject count drift")
    unlabeled_outcomes = set(
        review.loc[
            review["pair_id"].astype(str).isin(set(unlabeled_ids)), "finalist_outcome"
        ].astype(str)
    )
    if unlabeled_outcomes != {"ECONOMIC_ADMISSION_BLOCKED_NO_BACKFILL"}:
        raise RuntimeError("unlabeled pairs are not exact train economic rejects")
    if int(oos["all_four_economic_gates_positive"].astype(bool).sum()) != EXPECTED_SURVIVORS:
        raise RuntimeError("adaptive-validation survivor count drift")

    numeric_features = list(contract["allowed_train_visible_numeric_features"])
    categorical_features = list(contract["allowed_train_visible_categorical_features"])
    _require_columns(review, numeric_features + categorical_features, "train feature ledger")
    if review[numeric_features].isna().any().any():
        missing = review[numeric_features].isna().sum()
        raise RuntimeError(
            "predeclared train numeric feature missingness: "
            + str(missing[missing > 0].to_dict())
        )
    diagnosis = review[
        [
            "source_pair_order",
            "pair_id",
            "primary_candidate_id",
            "control_candidate_id",
            "finalist_outcome",
        ]
        + numeric_features
        + categorical_features
    ].copy()
    transition_primary = transition[
        ["pair_id", "candidate_id", "signal_rank_ic_mean"]
    ].rename(
        columns={
            "candidate_id": "transition_primary_candidate_id",
            "signal_rank_ic_mean": "decoder_signal_rank_ic_mean",
        }
    )
    diagnosis = diagnosis.merge(
        transition_primary, on="pair_id", how="left", validate="one_to_one", sort=False
    )
    if not diagnosis["transition_primary_candidate_id"].astype(str).eq(
        diagnosis["primary_candidate_id"].astype(str)
    ).all():
        raise RuntimeError("decoder transition primary identity drift")
    diagnosis["realized_pnl_share"] = diagnosis[
        "primary_cumulative_realized_trade_pnl_cny"
    ].abs() / (
        diagnosis["primary_cumulative_realized_trade_pnl_cny"].abs()
        + diagnosis["primary_ending_unrealized_pnl_cny"].abs()
    ).replace(0.0, np.nan)
    signal_pct = _as_numeric(diagnosis["train_rank_ic_mean"]).rank(
        method="average", pct=True
    )
    decoder_pct = _as_numeric(diagnosis["primary_cumulative_net_return"]).rank(
        method="average", pct=True
    )
    diagnosis["train_signal_to_decoder_rank_displacement"] = (
        signal_pct - decoder_pct
    ).abs()
    diagnosis["decoder_absolute_and_matched_margin_floor"] = diagnosis[
        ["primary_cumulative_net_return", "matched_cumulative_net_return_increment"]
    ].min(axis=1)
    diagnosis["decoder_reward_absolute_and_matched_margin_floor"] = diagnosis[
        [
            "primary_continuous_book_net_reward",
            "matched_continuous_book_net_reward_increment",
        ]
    ].min(axis=1)
    numeric_features.extend(list(contract["allowed_derived_train_features"].keys()))
    if diagnosis[numeric_features].isna().any().any():
        missing = diagnosis[numeric_features].isna().sum()
        raise RuntimeError(
            "derived train numeric feature missingness: "
            + str(missing[missing > 0].to_dict())
        )

    oos_columns = [
        "pair_id",
        "absolute_reward_positive",
        "matched_reward_increment_positive",
        "absolute_return_positive",
        "matched_return_increment_positive",
        "all_four_economic_gates_positive",
        "primary_continuous_book_net_reward",
        "primary_cumulative_net_return",
        "matched_continuous_book_net_reward_increment",
        "matched_cumulative_net_return_increment",
        "primary_mean_one_way_turnover",
        "primary_net_return_per_turnover",
        "primary_daily_net_return_p10",
        "primary_daily_net_return_worst",
        "primary_quarterly_regime_positive_share",
        "primary_quarterly_regime_worst_return",
    ]
    labels = oos[oos_columns].rename(
        columns={name: f"oos_{name}" for name in oos_columns if name != "pair_id"}
    )
    diagnosis = diagnosis.merge(
        labels, on="pair_id", how="left", validate="one_to_one", sort=False
    )
    for column in (
        "oos_absolute_reward_positive",
        "oos_matched_reward_increment_positive",
        "oos_absolute_return_positive",
        "oos_matched_return_increment_positive",
        "oos_all_four_economic_gates_positive",
    ):
        diagnosis[column] = diagnosis[column].astype("boolean")
    diagnosis["oos_label_observed"] = diagnosis["pair_id"].astype(str).isin(
        set(labeled_ids)
    )
    diagnosis["diagnostic_group"] = "TRAIN_SCREEN_REJECT_OOS_LABEL_MISSING"
    labeled_mask = diagnosis["oos_label_observed"]
    survivor_mask = diagnosis["oos_all_four_economic_gates_positive"].eq(True).fillna(False)
    diagnosis.loc[labeled_mask, "diagnostic_group"] = (
        "ADAPTIVE_VALIDATION_ABSOLUTE_ONLY_RELATIVE_INCOMPLETE"
    )
    diagnosis.loc[labeled_mask & survivor_mask, "diagnostic_group"] = (
        "ADAPTIVE_VALIDATION_SURVIVOR"
    )
    counts = diagnosis["diagnostic_group"].value_counts().to_dict()
    expected = {
        "ADAPTIVE_VALIDATION_SURVIVOR": 10,
        "ADAPTIVE_VALIDATION_ABSOLUTE_ONLY_RELATIVE_INCOMPLETE": 12,
        "TRAIN_SCREEN_REJECT_OOS_LABEL_MISSING": 10,
    }
    if counts != expected:
        raise RuntimeError(f"diagnostic group drift: {counts}")
    oos_prefixed = [column for column in diagnosis if column.startswith("oos_")]
    if diagnosis.loc[~diagnosis["oos_label_observed"], oos_prefixed].drop(
        columns=["oos_label_observed"]
    ).notna().any().any():
        raise RuntimeError("unlabeled train rejects received OOS values")
    return diagnosis, numeric_features, categorical_features


def _feature_diagnostics(
    diagnosis: pd.DataFrame,
    *,
    features: Sequence[str],
    contract: Mapping[str, Any],
) -> pd.DataFrame:
    seed = int(contract["diagnostic_plan"]["fixed_bootstrap_seed"])
    iterations = int(contract["diagnostic_plan"]["bootstrap_iterations"])
    high = set(contract["predeclared_feature_directions"]["higher_is_better"])
    low = set(contract["predeclared_feature_directions"]["lower_is_better"])
    rows: list[dict[str, Any]] = []
    for index, feature in enumerate(features):
        a = diagnosis.loc[
            diagnosis["diagnostic_group"].eq("ADAPTIVE_VALIDATION_SURVIVOR"),
            feature,
        ]
        b = diagnosis.loc[
            diagnosis["diagnostic_group"].eq(
                "ADAPTIVE_VALIDATION_ABSOLUTE_ONLY_RELATIVE_INCOMPLETE"
            ),
            feature,
        ]
        c = diagnosis.loc[
            diagnosis["diagnostic_group"].eq(
                "TRAIN_SCREEN_REJECT_OOS_LABEL_MISSING"
            ),
            feature,
        ]
        sa = _summary(a, iterations=iterations, seed=seed + index * 11 + 1)
        sb = _summary(b, iterations=iterations, seed=seed + index * 11 + 2)
        sc = _summary(c, iterations=iterations, seed=seed + index * 11 + 3)
        av = _as_numeric(a).dropna().to_numpy(dtype=float)
        bv = _as_numeric(b).dropna().to_numpy(dtype=float)
        p_value = (
            float(mannwhitneyu(av, bv, alternative="two-sided", method="auto").pvalue)
            if len(av) and len(bv)
            else None
        )
        effect = _rank_biserial(av, bv)
        if feature in high:
            direction = "HIGHER_IS_BETTER"
            direction_effect = effect
        elif feature in low:
            direction = "LOWER_IS_BETTER"
            direction_effect = -effect if effect is not None else None
        else:
            direction = "DESCRIPTIVE_ONLY_UNDIRECTED"
            direction_effect = None
        row: dict[str, Any] = {
            "feature": feature,
            "predeclared_direction": direction,
            "a_vs_b_rank_biserial_raw": effect,
            "a_vs_b_rank_biserial_direction_adjusted": direction_effect,
            "a_vs_b_mann_whitney_p": p_value,
        }
        for prefix, summary in (("group_a", sa), ("group_b", sb), ("group_c", sc)):
            row.update({f"{prefix}_{name}": value for name, value in summary.items()})
        rows.append(row)
    adjusted = _bh_fdr([row["a_vs_b_mann_whitney_p"] for row in rows])
    for row, value in zip(rows, adjusted):
        row["a_vs_b_bh_fdr"] = value
    return pd.DataFrame(rows).sort_values(
        ["a_vs_b_rank_biserial_direction_adjusted", "feature"],
        ascending=[False, True],
        kind="stable",
    ).reset_index(drop=True)


def _feature_precision(
    diagnosis: pd.DataFrame,
    *,
    features: Sequence[str],
    contract: Mapping[str, Any],
) -> pd.DataFrame:
    labeled = diagnosis.loc[diagnosis["oos_label_observed"]].copy()
    high = set(contract["predeclared_feature_directions"]["higher_is_better"])
    low = set(contract["predeclared_feature_directions"]["lower_is_better"])
    survivor_count = int(
        labeled["oos_all_four_economic_gates_positive"].astype(bool).sum()
    )
    rows: list[dict[str, Any]] = []
    for feature in features:
        if feature not in high | low:
            continue
        higher = feature in high
        ranked = labeled.assign(_value=_as_numeric(labeled[feature])).sort_values(
            ["_value", "pair_id"],
            ascending=[not higher, True],
            kind="stable",
        )
        for k in contract["diagnostic_plan"]["fixed_precision_k"]:
            selected = ranked.head(int(k))
            hits = int(selected["oos_all_four_economic_gates_positive"].astype(bool).sum())
            precision = hits / int(k)
            recall = hits / survivor_count
            random_precision = survivor_count / len(labeled)
            rows.append(
                {
                    "feature": feature,
                    "predeclared_direction": (
                        "HIGHER_IS_BETTER" if higher else "LOWER_IS_BETTER"
                    ),
                    "k": int(k),
                    "selected_count": int(k),
                    "survivor_count": hits,
                    "precision": precision,
                    "recall": recall,
                    "random_expected_precision": random_precision,
                    "precision_enrichment": precision / random_precision,
                    "hypergeometric_tail_probability": float(
                        hypergeom.sf(hits - 1, len(labeled), survivor_count, int(k))
                    ),
                    "selected_pair_ids_json": json.dumps(
                        selected["pair_id"].astype(str).tolist(),
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ),
                }
            )
    return pd.DataFrame(rows)


def _feature_quartiles(
    diagnosis: pd.DataFrame,
    *,
    features: Sequence[str],
    contract: Mapping[str, Any],
) -> pd.DataFrame:
    labeled = diagnosis.loc[diagnosis["oos_label_observed"]].copy()
    high = set(contract["predeclared_feature_directions"]["higher_is_better"])
    baseline = EXPECTED_SURVIVORS / EXPECTED_LABELED_PAIRS
    rows: list[dict[str, Any]] = []
    for feature in features:
        if feature not in high:
            low = set(contract["predeclared_feature_directions"]["lower_is_better"])
            if feature not in low:
                continue
        quality = _quality_percentile(
            labeled[feature], higher_is_better=feature in high
        )
        buckets = np.minimum(4, np.maximum(1, np.ceil(quality * 4))).astype(int)
        local = labeled.assign(_quality_quartile=buckets)
        for quartile in range(1, 5):
            group = local.loc[local["_quality_quartile"].eq(quartile)]
            hits = int(group["oos_all_four_economic_gates_positive"].astype(bool).sum())
            count = int(len(group))
            rate = hits / count if count else None
            rows.append(
                {
                    "feature": feature,
                    "quality_quartile": quartile,
                    "pair_count": count,
                    "survivor_count": hits,
                    "survivor_rate": rate,
                    "enrichment_over_labeled_baseline": (
                        rate / baseline if rate is not None else None
                    ),
                }
            )
    return pd.DataFrame(rows)


def _categorical_enrichment(
    diagnosis: pd.DataFrame, *, features: Sequence[str]
) -> pd.DataFrame:
    baseline = EXPECTED_SURVIVORS / EXPECTED_LABELED_PAIRS
    rows: list[dict[str, Any]] = []
    for feature in features:
        rendered = diagnosis[feature].fillna("<MISSING>").astype(str)
        for value in sorted(rendered.unique()):
            mask = rendered.eq(value)
            labeled = diagnosis.loc[mask & diagnosis["oos_label_observed"]]
            unlabeled = diagnosis.loc[mask & ~diagnosis["oos_label_observed"]]
            hits = int(
                labeled["oos_all_four_economic_gates_positive"].eq(True).sum()
            )
            count = int(len(labeled))
            rate = hits / count if count else None
            rows.append(
                {
                    "feature": feature,
                    "value": value,
                    "full_32_pair_count": int(mask.sum()),
                    "labeled_pair_count": count,
                    "unlabeled_pair_count": int(len(unlabeled)),
                    "survivor_count": hits,
                    "labeled_survivor_rate": rate,
                    "enrichment_over_labeled_baseline": (
                        rate / baseline if rate is not None else None
                    ),
                }
            )
    return pd.DataFrame(rows)


def _compute_ranker_scores(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame[["pair_id"]].copy()
    result["CURRENT_SEARCH_SCORE_BASELINE"] = _quality_percentile(
        frame["search_score"], higher_is_better=True
    )
    result["TRAIN_DECODER_ECONOMIC_FLOOR"] = _quality_percentile(
        frame["decoder_absolute_and_matched_margin_floor"], higher_is_better=True
    )
    axes = pd.DataFrame(
        {
            "ic": _quality_percentile(
                frame["train_rank_ic_hit_rate"], higher_is_better=True
            ),
            "economic": _quality_percentile(
                frame["decoder_absolute_and_matched_margin_floor"],
                higher_is_better=True,
            ),
            "efficiency": _quality_percentile(
                frame["primary_net_return_per_turnover"], higher_is_better=True
            ),
        },
        index=frame.index,
    )
    result["THREE_AXIS_EQUAL_RANK"] = axes.mean(axis=1)
    conservative_axes = pd.DataFrame(
        {
            "regime": _quality_percentile(
                frame["train_regime_stability_score"], higher_is_better=True
            ),
            "economic": axes["economic"],
            "efficiency": axes["efficiency"],
        },
        index=frame.index,
    )
    result["CONSERVATIVE_THREE_AXIS_FLOOR"] = conservative_axes.min(axis=1)
    return result


def _loo_top10_minimum(
    diagnosis: pd.DataFrame, *, ranker_id: str
) -> int:
    labeled_ids = diagnosis.loc[diagnosis["oos_label_observed"], "pair_id"].astype(str)
    counts: list[int] = []
    for omitted in labeled_ids:
        reduced = diagnosis.loc[~diagnosis["pair_id"].astype(str).eq(omitted)].copy()
        scores = _compute_ranker_scores(reduced)
        reduced = reduced.merge(scores, on="pair_id", validate="one_to_one")
        selected = reduced.loc[reduced["oos_label_observed"]].sort_values(
            [ranker_id, "pair_id"], ascending=[False, True], kind="stable"
        ).head(10)
        counts.append(
            int(selected["oos_all_four_economic_gates_positive"].astype(bool).sum())
        )
    return min(counts)


def _ranker_comparison(
    diagnosis: pd.DataFrame, *, contract: Mapping[str, Any]
) -> tuple[pd.DataFrame, pd.DataFrame]:
    scores = _compute_ranker_scores(diagnosis)
    scored = diagnosis.merge(scores, on="pair_id", validate="one_to_one")
    labeled = scored.loc[scored["oos_label_observed"]].copy()
    ranker_ids = [
        str(record["ranker_id"])
        for record in contract["predeclared_ranking_heuristics"]
    ]
    if set(ranker_ids) != set(scores.columns) - {"pair_id"}:
        raise RuntimeError("ranker implementation drift from predeclared contract")
    loo = {ranker: _loo_top10_minimum(diagnosis, ranker_id=ranker) for ranker in ranker_ids}
    rows: list[dict[str, Any]] = []
    for ranker in ranker_ids:
        ranked = labeled.sort_values(
            [ranker, "pair_id"], ascending=[False, True], kind="stable"
        )
        rho_return = _finite(
            spearmanr(
                ranked[ranker], ranked["oos_matched_cumulative_net_return_increment"]
            ).statistic
        )
        rho_reward = _finite(
            spearmanr(
                ranked[ranker], ranked["oos_matched_continuous_book_net_reward_increment"]
            ).statistic
        )
        for k in contract["diagnostic_plan"]["fixed_precision_k"]:
            selected = ranked.head(int(k))
            hits = int(selected["oos_all_four_economic_gates_positive"].astype(bool).sum())
            rows.append(
                {
                    "ranker_id": ranker,
                    "k": int(k),
                    "survivor_count": hits,
                    "precision": hits / int(k),
                    "recall": hits / EXPECTED_SURVIVORS,
                    "random_expected_precision": EXPECTED_SURVIVORS
                    / EXPECTED_LABELED_PAIRS,
                    "precision_enrichment": (hits / int(k))
                    / (EXPECTED_SURVIVORS / EXPECTED_LABELED_PAIRS),
                    "hypergeometric_tail_probability": float(
                        hypergeom.sf(
                            hits - 1,
                            EXPECTED_LABELED_PAIRS,
                            EXPECTED_SURVIVORS,
                            int(k),
                        )
                    ),
                    "spearman_with_oos_matched_return_increment": rho_return,
                    "spearman_with_oos_matched_reward_increment": rho_reward,
                    "leave_one_out_top10_survivor_count_minimum": loo[ranker],
                    "selected_pair_ids_json": json.dumps(
                        selected["pair_id"].astype(str).tolist(),
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ),
                }
            )
    score_columns = ["pair_id"] + ranker_ids
    return pd.DataFrame(rows), scored[score_columns]


def _unlabeled_shift(
    diagnosis: pd.DataFrame, *, features: Sequence[str]
) -> pd.DataFrame:
    labeled = diagnosis.loc[diagnosis["oos_label_observed"]]
    unlabeled = diagnosis.loc[~diagnosis["oos_label_observed"]]
    rows: list[dict[str, Any]] = []
    for feature in features:
        a = _as_numeric(labeled[feature]).dropna().to_numpy(dtype=float)
        b = _as_numeric(unlabeled[feature]).dropna().to_numpy(dtype=float)
        rows.append(
            {
                "feature": feature,
                "labeled_count": int(len(a)),
                "unlabeled_count": int(len(b)),
                "labeled_median": float(np.median(a)) if len(a) else None,
                "unlabeled_median": float(np.median(b)) if len(b) else None,
                "unlabeled_vs_labeled_rank_biserial": _rank_biserial(b, a),
                "mann_whitney_p": (
                    float(
                        mannwhitneyu(b, a, alternative="two-sided", method="auto").pvalue
                    )
                    if len(a) and len(b)
                    else None
                ),
                "interpretation": "TRAIN_SCREEN_COVARIATE_SHIFT_ONLY_NO_OOS_LABEL",
            }
        )
    adjusted = _bh_fdr([row["mann_whitney_p"] for row in rows])
    for row, value in zip(rows, adjusted):
        row["bh_fdr"] = value
    return pd.DataFrame(rows)


def _selection_ceiling(
    *,
    feature_precision: pd.DataFrame,
    ranker_comparison: pd.DataFrame,
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    by_k: list[dict[str, Any]] = []
    for k in contract["diagnostic_plan"]["fixed_precision_k"]:
        k = int(k)
        feature_rows = feature_precision.loc[feature_precision["k"].eq(k)]
        ranker_rows = ranker_comparison.loc[ranker_comparison["k"].eq(k)]
        best_feature = feature_rows.sort_values(
            ["survivor_count", "feature"], ascending=[False, True], kind="stable"
        ).iloc[0]
        best_ranker = ranker_rows.sort_values(
            ["survivor_count", "ranker_id"],
            ascending=[False, True],
            kind="stable",
        ).iloc[0]
        oracle_hits = min(k, EXPECTED_SURVIVORS)
        by_k.append(
            {
                "k": k,
                "oracle_survivor_count": oracle_hits,
                "oracle_precision": oracle_hits / k,
                "oracle_recall": oracle_hits / EXPECTED_SURVIVORS,
                "random_expected_survivor_count": k
                * EXPECTED_SURVIVORS
                / EXPECTED_LABELED_PAIRS,
                "random_expected_precision": EXPECTED_SURVIVORS
                / EXPECTED_LABELED_PAIRS,
                "best_predeclared_feature": str(best_feature["feature"]),
                "best_predeclared_feature_survivor_count": int(
                    best_feature["survivor_count"]
                ),
                "best_predeclared_ranker": str(best_ranker["ranker_id"]),
                "best_predeclared_ranker_survivor_count": int(
                    best_ranker["survivor_count"]
                ),
            }
        )
    return {
        "schema_version": "cn_alpha_selection_ceiling_v1",
        "scope": "22_LABELED_ADAPTIVE_VALIDATION_PAIRS_ONLY",
        "labeled_pair_count": EXPECTED_LABELED_PAIRS,
        "known_survivor_count": EXPECTED_SURVIVORS,
        "unlabeled_train_reject_count": EXPECTED_UNLABELED_PAIRS,
        "full_32_known_survivor_rate_lower_bound": EXPECTED_SURVIVORS
        / EXPECTED_SOURCE_PAIRS,
        "full_32_logical_survivor_rate_upper_bound_if_all_unlabeled_survive": (
            EXPECTED_SURVIVORS + EXPECTED_UNLABELED_PAIRS
        )
        / EXPECTED_SOURCE_PAIRS,
        "unlabeled_negative_imputation": "FORBIDDEN",
        "by_k": by_k,
    }


def _report_markdown(
    *,
    diagnostics: pd.DataFrame,
    feature_precision: pd.DataFrame,
    ranker_comparison: pd.DataFrame,
    selection_ceiling: Mapping[str, Any],
) -> str:
    top_effects = diagnostics.head(8)
    top10_features = feature_precision.loc[feature_precision["k"].eq(10)].sort_values(
        ["survivor_count", "feature"], ascending=[False, True], kind="stable"
    ).head(8)
    top10_rankers = ranker_comparison.loc[ranker_comparison["k"].eq(10)].sort_values(
        ["survivor_count", "ranker_id"], ascending=[False, True], kind="stable"
    )
    lines = [
        "# CN_ALPHA_SELECTION_DIAGNOSTIC_V1",
        "",
        "Status: `POST_HOC_ADAPTIVE_VALIDATION_DIAGNOSTIC_HOLD_RESEARCH`",
        "",
        "- Source population: 32 train pairs.",
        "- OOS-labeled universe: 22 pairs, including 10 adaptive-validation survivors and 12 absolute-only/relative-incomplete pairs.",
        "- Unlabeled universe: 10 train-screen rejects. They are never imputed as OOS failures.",
        "- New financial, validation, holdout and 2026 reads: zero.",
        "- This report fits no ML model and creates no selection or promotion authority.",
        "",
        "## Largest predeclared univariate effects (A versus B)",
        "",
        "| feature | direction | effect | p | FDR | A median | B median | C median |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for _, row in top_effects.iterrows():
        lines.append(
            "| {feature} | {direction} | {effect:.4f} | {p:.4f} | {fdr:.4f} | {a:.6g} | {b:.6g} | {c:.6g} |".format(
                feature=row["feature"],
                direction=row["predeclared_direction"],
                effect=float(row["a_vs_b_rank_biserial_direction_adjusted"]),
                p=float(row["a_vs_b_mann_whitney_p"]),
                fdr=float(row["a_vs_b_bh_fdr"]),
                a=float(row["group_a_median"]),
                b=float(row["group_b_median"]),
                c=float(row["group_c_median"]),
            )
        )
    lines.extend(
        [
            "",
            "## Feature precision at K=10",
            "",
            "| feature | survivors | precision | enrichment | hypergeom tail |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    for _, row in top10_features.iterrows():
        lines.append(
            f"| {row['feature']} | {int(row['survivor_count'])}/10 | {float(row['precision']):.4f} | {float(row['precision_enrichment']):.4f} | {float(row['hypergeometric_tail_probability']):.4f} |"
        )
    lines.extend(
        [
            "",
            "## Predeclared heuristic comparison at K=10",
            "",
            "| ranker | survivors | precision | enrichment | LOO minimum | rho matched return |",
            "|---|---:|---:|---:|---:|---:|",
        ]
    )
    for _, row in top10_rankers.iterrows():
        lines.append(
            f"| {row['ranker_id']} | {int(row['survivor_count'])}/10 | {float(row['precision']):.4f} | {float(row['precision_enrichment']):.4f} | {int(row['leave_one_out_top10_survivor_count_minimum'])} | {float(row['spearman_with_oos_matched_return_increment']):.4f} |"
        )
    k10 = next(record for record in selection_ceiling["by_k"] if record["k"] == 10)
    lines.extend(
        [
            "",
            "## Selection ceiling",
            "",
            f"- Oracle top-10 ceiling: {k10['oracle_survivor_count']}/10.",
            f"- Random top-10 expected survivors: {k10['random_expected_survivor_count']:.4f}.",
            f"- Best predeclared feature: `{k10['best_predeclared_feature']}` with {k10['best_predeclared_feature_survivor_count']}/10.",
            f"- Best predeclared heuristic: `{k10['best_predeclared_ranker']}` with {k10['best_predeclared_ranker_survivor_count']}/10.",
            "- The 10 train rejects remain unlabeled; 10/32 is a known-survivor lower bound, not the full-cohort survivor rate.",
            "",
            "## Bias audit",
            "",
            "Decision: `HOLD_RESEARCH`.",
            "",
            "The adaptive-validation labels may diagnose and freeze an explicitly validation-informed experimental heuristic, but they cannot establish prospective selection ability or promotion. That requires a new candidate cohort selected before an untouched confirmation read.",
            "",
        ]
    )
    return "\n".join(lines)


def build_diagnostic(
    *,
    contract_path: Path,
    input_root: Path,
    output_root: Path,
    builder_commit_sha: str,
) -> dict[str, Any]:
    contract_path = Path(contract_path).resolve()
    input_root = Path(input_root).resolve()
    output_root = Path(output_root).resolve()
    if output_root.exists() and any(output_root.iterdir()):
        raise RuntimeError(f"diagnostic output root must be empty: {output_root}")
    output_root.mkdir(parents=True, exist_ok=True)
    contract = _read_json(contract_path)
    if contract.get("schema_version") != "cn_alpha_selection_diagnostic_v1_contract":
        raise RuntimeError("diagnostic contract schema drift")
    if str(contract.get("status")) != "ACTIVE_ZERO_FINANCIAL_READ_DIAGNOSTIC_CONTRACT":
        raise RuntimeError("diagnostic contract status drift")
    verified_sources = _verify_bound_artifacts(contract, input_root)
    diagnosis, numeric_features, categorical_features = _load_candidate_diagnosis(
        input_root=input_root, contract=contract
    )
    diagnostics = _feature_diagnostics(
        diagnosis, features=numeric_features, contract=contract
    )
    precision = _feature_precision(
        diagnosis, features=numeric_features, contract=contract
    )
    quartiles = _feature_quartiles(
        diagnosis, features=numeric_features, contract=contract
    )
    categorical = _categorical_enrichment(
        diagnosis, features=categorical_features
    )
    ranker_comparison, ranker_scores = _ranker_comparison(
        diagnosis, contract=contract
    )
    diagnosis = diagnosis.merge(ranker_scores, on="pair_id", validate="one_to_one")
    unlabeled_shift = _unlabeled_shift(diagnosis, features=numeric_features)
    ceiling = _selection_ceiling(
        feature_precision=precision,
        ranker_comparison=ranker_comparison,
        contract=contract,
    )
    artifacts: list[Path] = []
    tables = {
        "candidate_diagnosis": diagnosis,
        "numeric_feature_diagnostics": diagnostics,
        "feature_precision_at_k": precision,
        "feature_quartile_enrichment": quartiles,
        "categorical_enrichment": categorical,
        "ranker_comparison": ranker_comparison,
        "unlabeled_shift_diagnostics": unlabeled_shift,
    }
    for name, frame in tables.items():
        path = output_root / f"{name}.parquet"
        frame.to_parquet(path, index=False)
        artifacts.append(path)
    ceiling_path = _write_json(output_root / "selection_ceiling.json", ceiling)
    artifacts.append(ceiling_path)
    builder_source_sha256 = _sha256(Path(__file__).resolve())
    input_binding = {
        "schema_version": "cn_alpha_selection_diagnostic_input_binding_v1",
        "contract_path": str(contract_path),
        "contract_file_sha256": _sha256(contract_path),
        "input_root": str(input_root),
        "verified_source_artifacts": verified_sources,
        "builder_commit_sha": builder_commit_sha,
        "builder_source_sha256": builder_source_sha256,
        "source_pair_count": EXPECTED_SOURCE_PAIRS,
        "labeled_pair_count": EXPECTED_LABELED_PAIRS,
        "unlabeled_pair_count": EXPECTED_UNLABELED_PAIRS,
        "new_financial_reads": 0,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "optimizer_feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "promotion_write": "FORBIDDEN",
    }
    binding_path = _write_json(output_root / "input_binding.json", input_binding)
    artifacts.append(binding_path)
    report_path = output_root / "CN_ALPHA_SELECTION_DIAGNOSTIC_V1.md"
    report_path.write_text(
        _report_markdown(
            diagnostics=diagnostics,
            feature_precision=precision,
            ranker_comparison=ranker_comparison,
            selection_ceiling=ceiling,
        ),
        encoding="utf-8",
    )
    artifacts.append(report_path)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "status": "CN_ALPHA_SELECTION_DIAGNOSTIC_V1_CLOSED_IMMUTABLE_HOLD_RESEARCH",
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "builder_commit_sha": builder_commit_sha,
        "builder_source_sha256": builder_source_sha256,
        "contract_file_sha256": _sha256(contract_path),
        "source_pair_count": EXPECTED_SOURCE_PAIRS,
        "labeled_pair_count": EXPECTED_LABELED_PAIRS,
        "adaptive_validation_survivor_count": EXPECTED_SURVIVORS,
        "adaptive_validation_relative_incomplete_count": 12,
        "unlabeled_train_reject_count": EXPECTED_UNLABELED_PAIRS,
        "new_financial_reads": 0,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "model_fitted": False,
        "threshold_optimized": False,
        "search_run": False,
        "reward_changed": False,
        "decoder_changed": False,
        "promotion_authorized": False,
        "decision": "HOLD_RESEARCH",
        "artifacts": [_artifact(path, root=output_root) for path in artifacts],
    }
    manifest["manifest_body_sha256"] = _stable_hash(manifest)
    manifest_path = _write_json(
        output_root / "SELECTION_DIAGNOSTIC_COMPLETE.json", manifest
    )
    return {
        "status": manifest["status"],
        "output_root": str(output_root),
        "manifest_path": str(manifest_path),
        "manifest_file_sha256": _sha256(manifest_path),
        "manifest_body_sha256": manifest["manifest_body_sha256"],
    }


def verify_diagnostic(
    *, output_root: Path, contract_path: Path, input_root: Path
) -> dict[str, Any]:
    output_root = Path(output_root).resolve()
    contract = _read_json(contract_path)
    manifest_path = output_root / "SELECTION_DIAGNOSTIC_COMPLETE.json"
    manifest = _read_json(manifest_path)
    if manifest.get("status") != "CN_ALPHA_SELECTION_DIAGNOSTIC_V1_CLOSED_IMMUTABLE_HOLD_RESEARCH":
        raise RuntimeError("diagnostic closure status drift")
    body = dict(manifest)
    stored = str(body.pop("manifest_body_sha256"))
    if _stable_hash(body) != stored:
        raise RuntimeError("diagnostic closure self-hash mismatch")
    verified = 0
    for record in manifest.get("artifacts") or []:
        path = output_root / str(record["path"])
        if not path.is_file():
            raise RuntimeError(f"diagnostic artifact missing: {path}")
        if path.stat().st_size != int(record["bytes"]):
            raise RuntimeError(f"diagnostic artifact size drift: {path}")
        if _sha256(path) != str(record["sha256"]):
            raise RuntimeError(f"diagnostic artifact hash drift: {path}")
        verified += 1
    if str(manifest.get("contract_file_sha256")) != _sha256(contract_path):
        raise RuntimeError("diagnostic contract binding drift")
    _verify_bound_artifacts(contract, Path(input_root).resolve())
    diagnosis = pd.read_parquet(output_root / "candidate_diagnosis.parquet")
    if len(diagnosis) != 32 or diagnosis["pair_id"].astype(str).nunique() != 32:
        raise RuntimeError("diagnostic candidate table count/identity drift")
    counts = diagnosis["diagnostic_group"].value_counts().to_dict()
    expected = {
        "ADAPTIVE_VALIDATION_SURVIVOR": 10,
        "ADAPTIVE_VALIDATION_ABSOLUTE_ONLY_RELATIVE_INCOMPLETE": 12,
        "TRAIN_SCREEN_REJECT_OOS_LABEL_MISSING": 10,
    }
    if counts != expected:
        raise RuntimeError("diagnostic candidate group drift")
    unlabeled = diagnosis.loc[~diagnosis["oos_label_observed"]]
    oos_values = [
        column
        for column in diagnosis.columns
        if column.startswith("oos_") and column != "oos_label_observed"
    ]
    if unlabeled[oos_values].notna().any().any():
        raise RuntimeError("unlabeled train rejects were assigned OOS labels")
    precision = pd.read_parquet(output_root / "feature_precision_at_k.parquet")
    if not precision["selected_count"].astype(int).isin({5, 10, 15, 20}).all():
        raise RuntimeError("precision-at-k scope drift")
    ceiling = _read_json(output_root / "selection_ceiling.json")
    if ceiling.get("scope") != "22_LABELED_ADAPTIVE_VALIDATION_PAIRS_ONLY":
        raise RuntimeError("selection ceiling scope drift")
    if ceiling.get("unlabeled_negative_imputation") != "FORBIDDEN":
        raise RuntimeError("unlabeled negative-imputation boundary drift")
    rankers = pd.read_parquet(output_root / "ranker_comparison.parquet")
    if set(rankers["ranker_id"].astype(str)) != {
        str(record["ranker_id"])
        for record in contract["predeclared_ranking_heuristics"]
    }:
        raise RuntimeError("ranker comparison identity drift")
    return {
        "status": "PASS",
        "manifest_file_sha256": _sha256(manifest_path),
        "manifest_body_sha256": stored,
        "artifact_count_verified": verified,
        "source_pair_count": 32,
        "labeled_pair_count": 22,
        "unlabeled_pair_count": 10,
        "survivor_count": 10,
        "unlabeled_negative_imputation": False,
        "new_financial_reads": 0,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--contract", type=Path, required=True)
    parser.add_argument("--input-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--builder-commit-sha", required=True)
    parser.add_argument("--verify-only", action="store_true")
    args = parser.parse_args()
    if args.verify_only:
        result = verify_diagnostic(
            output_root=args.output_root,
            contract_path=args.contract,
            input_root=args.input_root,
        )
    else:
        result = build_diagnostic(
            contract_path=args.contract,
            input_root=args.input_root,
            output_root=args.output_root,
            builder_commit_sha=args.builder_commit_sha,
        )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
