"""Trainable structured Program-search models for offline development replay.

The models consume only Program structural genes. Cohort, wave, prior optimizer
arm and validation-derived fields are deliberately excluded from the feature
surface. The primary model is a multi-head ExtraTrees surrogate; LambdaMART is
an optional mature learning-to-rank backend when xgboost is installed.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

import numpy as np

NUMERIC_GENE_SLOTS = frozenset({"raw_field_count", "rolling_node_count"})
DEFAULT_SEED = 8261701


def structural_feature_dict(row: Mapping[str, Any]) -> dict[str, Any]:
    genes = dict(row.get("structural_genes") or {})
    if not genes:
        raise ValueError("trained Program search requires structural_genes")
    features: dict[str, Any] = {}
    for key, value in sorted(genes.items()):
        if key in NUMERIC_GENE_SLOTS:
            features[f"gene::{key}"] = float(value)
        else:
            features[f"gene::{key}"] = str(value)
    group = str(row.get("base_group_id") or "")
    if group:
        features["base_group_id"] = group
    return features


def _safe_scale(values: Sequence[float]) -> float:
    finite = np.asarray([abs(float(v)) for v in values if math.isfinite(float(v))], dtype=float)
    if not len(finite):
        return 1.0
    value = float(np.median(finite))
    return value if value > 1e-12 else max(float(np.mean(finite)), 1.0)


def _positive_probability(model: Any, matrix: np.ndarray, fallback: float) -> np.ndarray:
    if model is None:
        return np.full(matrix.shape[0], float(fallback), dtype=float)
    classes = tuple(int(v) for v in model.classes_)
    if classes == (1,):
        return np.ones(matrix.shape[0], dtype=float)
    if classes == (0,):
        return np.zeros(matrix.shape[0], dtype=float)
    probabilities = model.predict_proba(matrix)
    return np.asarray(probabilities[:, classes.index(1)], dtype=float)


def _tree_distribution(model: Any, matrix: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    if model is None:
        zeros = np.zeros(matrix.shape[0], dtype=float)
        return zeros, zeros
    predictions = np.asarray([tree.predict(matrix) for tree in model.estimators_], dtype=float)
    return predictions.mean(axis=0), predictions.std(axis=0)


@dataclass(slots=True)
class StructuredMatrixV2:
    vectorizer: Any
    matrix: np.ndarray

    @classmethod
    def fit(cls, rows: Sequence[Mapping[str, Any]]) -> "StructuredMatrixV2":
        from sklearn.feature_extraction import DictVectorizer

        vectorizer = DictVectorizer(sparse=False, sort=True)
        matrix = np.asarray(
            vectorizer.fit_transform([structural_feature_dict(row) for row in rows]),
            dtype=float,
        )
        return cls(vectorizer=vectorizer, matrix=matrix)

    def transform(self, rows: Sequence[Mapping[str, Any]]) -> np.ndarray:
        return np.asarray(
            self.vectorizer.transform([structural_feature_dict(row) for row in rows]),
            dtype=float,
        )


class StructuredExtraTreesReplayV1:
    """Replay of the existing two-head surrogate: admission x return uplift."""

    def __init__(self, *, seed: int = DEFAULT_SEED, beta: float = 1.0) -> None:
        self.seed = int(seed)
        self.beta = float(beta)
        self.matrix: StructuredMatrixV2 | None = None
        self.admission_model: Any = None
        self.return_model: Any = None
        self.return_scale = 1.0

    def fit(self, rows: Sequence[Mapping[str, Any]]) -> "StructuredExtraTreesReplayV1":
        from sklearn.ensemble import ExtraTreesClassifier, ExtraTreesRegressor

        self.matrix = StructuredMatrixV2.fit(rows)
        X = self.matrix.matrix
        admitted = np.asarray([bool(row["admitted"]) for row in rows], dtype=int)
        self.admission_model = ExtraTreesClassifier(
            n_estimators=256, min_samples_leaf=2, max_features="sqrt",
            class_weight="balanced", random_state=self.seed, n_jobs=1,
        ).fit(X, admitted)
        eligible = [i for i, row in enumerate(rows) if bool(row["admitted"])]
        returns = [float(rows[i]["matched_cumulative_net_return_increment"]) for i in eligible]
        self.return_scale = _safe_scale(returns)
        self.return_model = ExtraTreesRegressor(
            n_estimators=256, min_samples_leaf=2, max_features="sqrt",
            random_state=self.seed + 1, n_jobs=1,
        ).fit(X[eligible], np.asarray(returns, dtype=float)) if eligible else None
        return self

    def score_rows(self, rows: Sequence[Mapping[str, Any]]) -> list[dict[str, float]]:
        if self.matrix is None:
            raise RuntimeError("structured replay model is not fit")
        X = self.matrix.transform(rows)
        p_admit = _positive_probability(self.admission_model, X, 0.5)
        ret_mean, ret_std = _tree_distribution(self.return_model, X)
        ret_ucb = np.maximum(0.0, (ret_mean + self.beta * ret_std) / self.return_scale)
        score = p_admit * ret_ucb
        return [
            {
                "score": float(score[i]),
                "p_admit": float(p_admit[i]),
                "return_mean": float(ret_mean[i]),
                "return_std": float(ret_std[i]),
            }
            for i in range(len(rows))
        ]


class StructuredMultiHeadSearchV2:
    """Four-head trained searcher aligned to the actual productive definition."""

    def __init__(self, *, seed: int = DEFAULT_SEED, beta: float = 0.75) -> None:
        self.seed = int(seed)
        self.beta = float(beta)
        self.matrix: StructuredMatrixV2 | None = None
        self.admission_model: Any = None
        self.productive_model: Any = None
        self.return_model: Any = None
        self.reward_model: Any = None
        self.return_scale = 1.0
        self.reward_scale = 1.0
        self.productive_fallback = 0.5

    def fit(self, rows: Sequence[Mapping[str, Any]]) -> "StructuredMultiHeadSearchV2":
        from sklearn.ensemble import ExtraTreesClassifier, ExtraTreesRegressor

        self.matrix = StructuredMatrixV2.fit(rows)
        X = self.matrix.matrix
        admitted = np.asarray([bool(row["admitted"]) for row in rows], dtype=int)
        self.admission_model = ExtraTreesClassifier(
            n_estimators=384, min_samples_leaf=2, max_features="sqrt",
            class_weight="balanced", random_state=self.seed, n_jobs=1,
        ).fit(X, admitted)
        eligible = [i for i, row in enumerate(rows) if bool(row["admitted"])]
        productive = np.asarray([bool(rows[i]["productive"]) for i in eligible], dtype=int)
        self.productive_fallback = float(productive.mean()) if len(productive) else 0.0
        if len(set(productive.tolist())) > 1:
            self.productive_model = ExtraTreesClassifier(
                n_estimators=384, min_samples_leaf=2, max_features="sqrt",
                class_weight="balanced", random_state=self.seed + 1, n_jobs=1,
            ).fit(X[eligible], productive)
        returns = [float(rows[i]["matched_cumulative_net_return_increment"]) for i in eligible]
        rewards = [float(rows[i]["matched_net_reward_increment"]) for i in eligible]
        self.return_scale = _safe_scale(returns)
        self.reward_scale = _safe_scale(rewards)
        if eligible:
            self.return_model = ExtraTreesRegressor(
                n_estimators=384, min_samples_leaf=2, max_features="sqrt",
                random_state=self.seed + 2, n_jobs=1,
            ).fit(X[eligible], np.asarray(returns, dtype=float))
            self.reward_model = ExtraTreesRegressor(
                n_estimators=384, min_samples_leaf=2, max_features="sqrt",
                random_state=self.seed + 3, n_jobs=1,
            ).fit(X[eligible], np.asarray(rewards, dtype=float))
        return self

    def score_rows(self, rows: Sequence[Mapping[str, Any]]) -> list[dict[str, float]]:
        if self.matrix is None:
            raise RuntimeError("multi-head search model is not fit")
        X = self.matrix.transform(rows)
        p_admit = _positive_probability(self.admission_model, X, 0.5)
        p_productive_cond = _positive_probability(
            self.productive_model, X, self.productive_fallback
        )
        ret_mean, ret_std = _tree_distribution(self.return_model, X)
        reward_mean, reward_std = _tree_distribution(self.reward_model, X)
        ret_ucb = np.maximum(
            0.0, (ret_mean + self.beta * ret_std) / self.return_scale
        )
        reward_ucb = np.maximum(
            0.0, (reward_mean + self.beta * reward_std) / self.reward_scale
        )
        joint_economic = np.sqrt(ret_ucb * reward_ucb)
        normalized_uncertainty = np.sqrt(
            np.square(ret_std / self.return_scale)
            + np.square(reward_std / self.reward_scale)
        )
        productive_probability = p_admit * p_productive_cond
        score = (
            productive_probability * (1.0 + joint_economic)
            + 0.10 * self.beta * p_admit * normalized_uncertainty
        )
        return [
            {
                "score": float(score[i]),
                "p_admit": float(p_admit[i]),
                "p_productive_conditional": float(p_productive_cond[i]),
                "p_productive": float(productive_probability[i]),
                "return_mean": float(ret_mean[i]),
                "return_std": float(ret_std[i]),
                "reward_mean": float(reward_mean[i]),
                "reward_std": float(reward_std[i]),
                "joint_economic_ucb": float(joint_economic[i]),
                "uncertainty": float(normalized_uncertainty[i]),
            }
            for i in range(len(rows))
        ]


def xgboost_available() -> bool:
    try:
        import xgboost  # noqa: F401
    except Exception:
        return False
    return True


class LambdaMARTRankerV2:
    """XGBoost LambdaMART over structural Program genes when available."""

    def __init__(self, *, seed: int = DEFAULT_SEED) -> None:
        self.seed = int(seed)
        self.vectorizer: Any = None
        self.model: Any = None

    def fit(self, rows: Sequence[Mapping[str, Any]]) -> "LambdaMARTRankerV2":
        if not xgboost_available():
            raise RuntimeError("xgboost is unavailable")
        from sklearn.feature_extraction import DictVectorizer
        import xgboost as xgb

        ordered = sorted(
            rows,
            key=lambda row: (
                str(row["query_group"]),
                int(row["source_order"]),
                str(row["exact_identity"]),
            ),
        )
        self.vectorizer = DictVectorizer(sparse=True, sort=True)
        X = self.vectorizer.fit_transform(
            [structural_feature_dict(row) for row in ordered]
        )
        y = np.asarray([int(row["relevance_grade"]) for row in ordered], dtype=float)
        group_names = [str(row["query_group"]) for row in ordered]
        group_ids = {name: idx for idx, name in enumerate(sorted(set(group_names)))}
        qid = np.asarray([group_ids[name] for name in group_names], dtype=np.int32)
        self.model = xgb.XGBRanker(
            objective="rank:ndcg",
            tree_method="hist",
            n_estimators=320,
            max_depth=4,
            learning_rate=0.05,
            min_child_weight=3.0,
            subsample=0.85,
            colsample_bytree=0.85,
            reg_lambda=2.0,
            reg_alpha=0.05,
            lambdarank_pair_method="mean",
            lambdarank_num_pair_per_sample=4,
            random_state=self.seed,
            n_jobs=1,
        )
        self.model.fit(X, y, qid=qid, verbose=False)
        return self

    def score_rows(self, rows: Sequence[Mapping[str, Any]]) -> np.ndarray:
        if self.vectorizer is None or self.model is None:
            raise RuntimeError("LambdaMART model is not fit")
        X = self.vectorizer.transform([structural_feature_dict(row) for row in rows])
        return np.asarray(self.model.predict(X), dtype=float)


def zscore(values: Sequence[float]) -> np.ndarray:
    array = np.asarray(values, dtype=float)
    std = float(array.std())
    if std <= 1e-12:
        return np.zeros(len(array), dtype=float)
    return (array - float(array.mean())) / std


def lightgbm_available() -> bool:
    try:
        import lightgbm  # noqa: F401
    except Exception:
        return False
    return True


def catboost_available() -> bool:
    try:
        import catboost  # noqa: F401
    except Exception:
        return False
    return True


def _ordered_rank_rows(rows: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    return sorted(
        rows,
        key=lambda row: (
            str(row["query_group"]),
            int(row["source_order"]),
            str(row["exact_identity"]),
        ),
    )


def _group_sizes(rows: Sequence[Mapping[str, Any]]) -> list[int]:
    groups: list[int] = []
    last: str | None = None
    size = 0
    for row in rows:
        current = str(row["query_group"])
        if last is None:
            last = current
        if current != last:
            groups.append(size)
            last = current
            size = 0
        size += 1
    if size:
        groups.append(size)
    if sum(groups) != len(rows) or any(value < 1 for value in groups):
        raise RuntimeError("ranker query-group cardinality drift")
    return groups


class LightGBMLambdaRankerV2:
    """LightGBM LambdaRank over the same structural feature surface."""

    def __init__(self, *, seed: int = DEFAULT_SEED) -> None:
        self.seed = int(seed)
        self.vectorizer: Any = None
        self.model: Any = None

    def fit(self, rows: Sequence[Mapping[str, Any]]) -> "LightGBMLambdaRankerV2":
        if not lightgbm_available():
            raise RuntimeError("lightgbm is unavailable")
        from sklearn.feature_extraction import DictVectorizer
        from lightgbm import LGBMRanker

        ordered = _ordered_rank_rows(rows)
        self.vectorizer = DictVectorizer(sparse=True, sort=True)
        X = self.vectorizer.fit_transform([structural_feature_dict(row) for row in ordered])
        y = np.asarray([int(row["relevance_grade"]) for row in ordered], dtype=float)
        self.model = LGBMRanker(
            objective="lambdarank",
            metric="ndcg",
            n_estimators=320,
            learning_rate=0.04,
            num_leaves=31,
            max_depth=-1,
            min_child_samples=12,
            subsample=0.85,
            colsample_bytree=0.85,
            reg_lambda=2.0,
            reg_alpha=0.05,
            random_state=self.seed,
            n_jobs=1,
            verbosity=-1,
        )
        self.model.fit(
            X,
            y,
            group=_group_sizes(ordered),
            eval_at=(3, 7, 14, 24),
        )
        return self

    def score_rows(self, rows: Sequence[Mapping[str, Any]]) -> np.ndarray:
        if self.vectorizer is None or self.model is None:
            raise RuntimeError("LightGBM ranker is not fit")
        X = self.vectorizer.transform([structural_feature_dict(row) for row in rows])
        return np.asarray(self.model.predict(X), dtype=float)


def _catboost_feature_table(
    rows: Sequence[Mapping[str, Any]],
    *,
    columns: Sequence[str] | None = None,
) -> tuple[list[list[Any]], tuple[str, ...], tuple[int, ...]]:
    feature_rows = [structural_feature_dict(row) for row in rows]
    if columns is None:
        names = tuple(sorted({key for row in feature_rows for key in row}))
    else:
        names = tuple(map(str, columns))
    categorical = tuple(
        index
        for index, name in enumerate(names)
        if name not in {"gene::raw_field_count", "gene::rolling_node_count"}
    )
    categorical_set = set(categorical)
    matrix: list[list[Any]] = []
    for row in feature_rows:
        values = []
        for index, name in enumerate(names):
            value = row.get(name)
            if index in categorical_set:
                values.append("__MISSING__" if value is None else str(value))
            else:
                values.append(float("nan") if value is None else float(value))
        matrix.append(values)
    return matrix, names, categorical


class CatBoostRankerV2:
    """Native-categorical CatBoost ranking baseline for the Program pool."""

    def __init__(self, *, seed: int = DEFAULT_SEED, loss: str = "YetiRank") -> None:
        self.seed = int(seed)
        self.loss = str(loss)
        self.columns: tuple[str, ...] | None = None
        self.categorical: tuple[int, ...] = ()
        self.model: Any = None

    def fit(self, rows: Sequence[Mapping[str, Any]]) -> "CatBoostRankerV2":
        if not catboost_available():
            raise RuntimeError("catboost is unavailable")
        from catboost import CatBoostRanker, Pool

        ordered = _ordered_rank_rows(rows)
        matrix, columns, categorical = _catboost_feature_table(ordered)
        self.columns = columns
        self.categorical = categorical
        group_names = [str(row["query_group"]) for row in ordered]
        group_map = {name: index for index, name in enumerate(sorted(set(group_names)))}
        group_id = [group_map[name] for name in group_names]
        labels = [int(row["relevance_grade"]) for row in ordered]
        pool = Pool(
            matrix,
            label=labels,
            group_id=group_id,
            cat_features=list(categorical),
            feature_names=list(columns),
        )
        self.model = CatBoostRanker(
            loss_function=self.loss,
            iterations=360,
            depth=6,
            learning_rate=0.04,
            l2_leaf_reg=5.0,
            random_seed=self.seed,
            random_strength=0.5,
            bootstrap_type="Bayesian",
            bagging_temperature=0.5,
            thread_count=1,
            verbose=False,
            allow_writing_files=False,
        )
        self.model.fit(pool, verbose=False)
        return self

    def score_rows(self, rows: Sequence[Mapping[str, Any]]) -> np.ndarray:
        if self.columns is None or self.model is None:
            raise RuntimeError("CatBoost ranker is not fit")
        from catboost import Pool

        matrix, columns, categorical = _catboost_feature_table(
            rows, columns=self.columns
        )
        if columns != self.columns or categorical != self.categorical:
            raise RuntimeError("CatBoost feature schema drift")
        pool = Pool(
            matrix,
            cat_features=list(categorical),
            feature_names=list(columns),
        )
        return np.asarray(self.model.predict(pool), dtype=float)
