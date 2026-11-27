"""Embedding via bge-m3 (1024 dimensions, cosine-normalized)."""

from __future__ import annotations

import numpy as np

_MODEL_NAME = "BAAI/bge-m3"
_model = None


def _get_model():
    """Lazy-load the sentence-transformers model (heavy import)."""
    global _model
    if _model is None:
        from sentence_transformers import SentenceTransformer

        _model = SentenceTransformer(_MODEL_NAME)
    return _model


def embed_texts(texts: list[str]) -> np.ndarray:
    """Embed a batch of texts. Returns float32 array of shape (n, 1024), L2-normalized."""
    model = _get_model()
    emb = model.encode(texts, normalize_embeddings=True, convert_to_numpy=True)
    return emb.astype(np.float32)


def embed_text(text: str) -> np.ndarray:
    """Embed a single text. Returns float32 array of shape (1024,)."""
    return embed_texts([text])[0]
