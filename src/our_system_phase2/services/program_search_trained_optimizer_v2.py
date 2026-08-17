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
