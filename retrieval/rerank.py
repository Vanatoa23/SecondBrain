"""Reranking via bge-reranker-v2-m3 cross-encoder."""

from __future__ import annotations

import numpy as np

_MODEL_NAME = "BAAI/bge-reranker-v2-m3"
_model = None


def _get_model():
    global _model
    if _model is None:
        from sentence_transformers import CrossEncoder

        _model = CrossEncoder(_MODEL_NAME)
    return _model


def _sigmoid(x: np.ndarray) -> np.ndarray:
    """Logistic sigmoid, clipped to avoid exp overflow. Monotonic, maps to (0, 1)."""
    return 1.0 / (1.0 + np.exp(-np.clip(x, -500.0, 500.0)))


def _pair_document(c: dict, text_field: str) -> str:
    """Rerank pair document: title + graph edge context + content."""
    via = c.get("via_relationship") or {}
    return (
        (c.get("title") or "")
        + "\n"
        + (via.get("context") or "")
        + "\n"
        + (c.get(text_field, "") or "")[:4096]
    )


def rerank(query: str, candidates: list[dict], text_field: str = "content", top_k: int | None = None) -> list[dict]:
    """Rerank candidate nodes against the query. Returns candidates sorted by reranker score.

    The cross-encoder emits raw logits; they are sigmoid-normalized here so
    ``rerank_score`` is in (0, 1). Sigmoid is monotonic, so the ranking order
    is identical to the raw-logit order.
    """
    if not candidates:
        return []
    model = _get_model()
    pairs = [(query, _pair_document(c, text_field)) for c in candidates]
    scores = _sigmoid(np.asarray(model.predict(pairs), dtype=np.float64))
    for c, s in zip(candidates, scores):
        c["rerank_score"] = float(s)
    ranked = sorted(candidates, key=lambda x: x["rerank_score"], reverse=True)
    return ranked[:top_k] if top_k else ranked
