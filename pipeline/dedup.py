"""Deduplication — prevent the same knowledge from being stored twice.

Checks, in order:

  1. source_uri + whole-document hash: same source_uri AND the stored
     metadata.document_hash equals the incoming document_hash (or the row is
     a legacy row without document_hash) -> duplicate. Same source_uri but a
     DIFFERENT document_hash -> NOT a duplicate (new version, append-only).
  2. Whole-document hash: any row whose metadata.document_hash == incoming
     document_hash -> duplicate (regardless of source_uri).
  3. Content hash (sha256 of normalized text — legacy chunk-level key)
  4. Semantic similarity (cosine > SIMILARITY_THRESHOLD against existing embeddings)

Never overwrites existing rows — on duplicate, returns the existing id.
"""

from __future__ import annotations

import hashlib
import re

import numpy as np

from .db import get_connection
from .embed import embed_text

SIMILARITY_THRESHOLD = 0.95


def normalize_text(text: str) -> str:
    """Lowercase, collapse whitespace — for stable content hashing."""
    return re.sub(r"\s+", " ", text.lower()).strip()


def content_hash(text: str) -> str:
    return hashlib.sha256(normalize_text(text).encode()).hexdigest()


def find_duplicate(
    text: str,
    source_uri: str | None,
    conn,
    document_hash: str | None = None,
) -> str | None:
    """Return the id of an existing duplicate node, or None.

    Uses the provided open connection. Read-only — never modifies data.

    Args:
        text: full extracted text of the incoming document.
        source_uri: source URI of the incoming document (may be None).
        document_hash: whole-document hash (sha256 of the normalized full
            text, i.e. content_hash(text)). When None, only the legacy
            behavior applies: any row with the same source_uri is a
            duplicate, and the document-hash legs are skipped.
    """
    # 1. source_uri + whole-document hash.
    # Only document-level rows are considered: rows that carry a
    # document_hash, plus legacy rows (no document_hash and not an excerpt).
    # Excerpt rows share the document's source_uri but have no document_hash,
    # so they must not make a changed document look like a "legacy" duplicate.
    if source_uri:
        rows = conn.execute(
            """
            SELECT id, metadata->>'document_hash' AS doc_hash
            FROM knowledge_items
            WHERE source_uri = %s
              AND (metadata->>'document_hash' IS NOT NULL
                   OR source_document_id IS NULL)
            """,
            (source_uri,),
        ).fetchall()
        if rows:
            if document_hash is None:
                # Legacy callers: uri match alone means duplicate.
                return rows[0]["id"]
            for row in rows:
                if row["doc_hash"] is None or row["doc_hash"] == document_hash:
                    return row["id"]
            # Same source_uri but a different document_hash -> new version,
            # NOT a duplicate. Fall through to the remaining checks.

    # 2. Whole-document hash match (any source_uri).
    if document_hash:
        row = conn.execute(
            "SELECT id FROM knowledge_items"
            " WHERE metadata->>'document_hash' = %s LIMIT 1",
            (document_hash,),
        ).fetchone()
        if row:
            return row["id"]

    # 3. Content hash match (legacy chunk-level key).
    chash = content_hash(text)
    row = conn.execute(
        "SELECT id FROM knowledge_items WHERE metadata->>'content_hash' = %s LIMIT 1",
        (chash,),
    ).fetchone()
    if row:
        return row["id"]

    # 4. Semantic similarity against existing embeddings.
    if len(text) < 20:
        return None  # too short for meaningful similarity
    q_emb = embed_text(text)
    rows = conn.execute(
        """
        SELECT id, embedding FROM knowledge_items
        WHERE embedding IS NOT NULL
        ORDER BY embedding <=> %s::vector LIMIT 5
        """,
        (q_emb.tolist(),),
    ).fetchall()
    for row in rows:
        existing = np.array(row["embedding"], dtype=np.float32)
        sim = float(np.dot(q_emb, existing) / (np.linalg.norm(q_emb) * np.linalg.norm(existing) + 1e-9))
        if sim >= SIMILARITY_THRESHOLD:
            return row["id"]

    return None
