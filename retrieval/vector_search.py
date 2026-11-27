"""Vector similarity search via pgvector (cosine distance, <=> operator)."""

from __future__ import annotations

import numpy as np


def vector_search(conn, query_embedding: np.ndarray, top_k: int = 20) -> list[dict]:
    """Return top_k nodes by cosine similarity to the query embedding."""
    rows = conn.execute(
        """
        SELECT id, type, title, content, mime_type, source_uri,
               distillation_score, created_at, update_period, metadata,
               1 - (embedding <=> %s::vector) AS score
        FROM knowledge_items
        WHERE embedding IS NOT NULL
        ORDER BY embedding <=> %s::vector
        LIMIT %s
        """,
        (query_embedding.tolist(), query_embedding.tolist(), top_k),
    ).fetchall()
    return [dict(r) for r in rows]
