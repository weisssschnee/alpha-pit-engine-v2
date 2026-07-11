from __future__ import annotations

from scripts.audit_search_collapse import _greedy_clique_clusters, distribution_metrics


def test_distribution_metrics_reports_effective_multiplicity() -> None:
    metrics = distribution_metrics(["a", "a", "a", "b"])

    assert metrics["cluster_count"] == 2
    assert metrics["top_cluster_share"] == 0.75
    assert metrics["n_eff"] == 1.6
    assert metrics["trial_multiplicity"] == 2.0
    assert metrics["effective_trial_multiplicity"] == 2.5


def test_complete_link_does_not_chain_loose_clusters() -> None:
    correlation = __import__("numpy").array(
        [
            [1.0, 0.98, 0.90],
            [0.98, 1.0, 0.98],
            [0.90, 0.98, 1.0],
        ]
    )

    labels = _greedy_clique_clusters(correlation, 0.95)

    assert labels[0] == labels[1]
    assert labels[2] != labels[0]
