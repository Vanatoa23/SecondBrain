"""Full-text search via PostgreSQL tsvector / tsquery with ts_rank scoring."""

from __future__ import annotations


def fts_search(conn, query: str, top_k: int = 20) -> list[dict]:
    """Return top_k nodes matching the query via full-text search.

    Uses websearch_to_tsquery, so web-style operators work in natural
    questions ("a or b" is an OR, unlike plainto_tsquery which ANDs every
    term and silently drops the word "or" as a stopword).
    """
    rows = conn.execute(
        """
        SELECT id, type, title, content, mime_type, source_uri,
               distillation_score, created_at, update_period, metadata,
               ts_rank(
                 to_tsvector('english', content || ' ' || coalesce(title, '')),
                 websearch_to_tsquery('english', %s)
               ) AS score
        FROM knowledge_items
        WHERE to_tsvector('english', content || ' ' || coalesce(title, ''))
              @@ websearch_to_tsquery('english', %s)
        ORDER BY score DESC
        LIMIT %s
        """,
        (query, query, top_k),
    ).fetchall()
    return [dict(r) for r in rows]
