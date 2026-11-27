"""Reranking via bge-reranker-v2-m3 cross-encoder."""

from __future__ import annotations

_MODEL_NAME = "BAAI/bge-reranker-v2-m3"
_model = None


def _get_model():
    global _model
    if _model is None:
        from sentence_transformers import CrossEncoder

        _model = CrossEncoder(_MODEL_NAME)
    return _model


def rerank(query: str, candidates: list[dict], text_field: str = "content", top_k: int | None = None) -> list[dict]:
    """Rerank candidate nodes against the query. Returns candidates sorted by reranker score."""
    if not candidates:
        return []
    model = _get_model()
    pairs = [(query, c.get(text_field, "")[:4096]) for c in candidates]
    scores = model.predict(pairs)
    for c, s in zip(candidates, scores):
        c["rerank_score"] = float(s)
    ranked = sorted(candidates, key=lambda x: x["rerank_score"], reverse=True)
    return ranked[:top_k] if top_k else ranked
