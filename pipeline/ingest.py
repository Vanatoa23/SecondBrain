"""Main ingestion pipeline: extract → chunk → embed → insert → link.

Flow (locked spec — see PM_LEDGER.md):
  1. Extract searchable text from the raw input
  2. Dedup check — skip if this content already exists
  3. Chunk semantically (whole if <= 2000 tokens, else split)
  4. Embed each chunk with bge-m3
  5. INSERT nodes (document + excerpts) — never UPDATE/DELETE existing rows
  6. Auto-extract relationships to existing nodes
  7. Estimate update_period and distillation_score (heuristic by default,
     LLM-pluggable via estimate functions)
  8. Optional LLM entity/relation extraction (Phase 3, EXTRACT_ENABLED=1):
     env-gated post-step — pipeline/extract.py via one OpenAI-compatible
     endpoint; results stored insert-only as entity nodes + relationships.
     Any extraction failure is logged to stderr and never breaks ingestion.
"""

from __future__ import annotations

import json
import os
import sys

import numpy as np

from .chunk import chunk_text
from .db import get_connection
from .dedup import content_hash, find_duplicate
from .embed import embed_texts
from .extractors import extract
from .raw_store import store_raw_bytes

# ---------------------------------------------------------------------------
# Heuristic estimators (replace with LLM calls when an API is wired in)
# ---------------------------------------------------------------------------

_PERMANENT_HINTS = ("book", "paper", "classic", "theorem", "philosophy")
_EVENT_HINTS = ("hackathon", "deadline", "starts", "ends", "event", "meeting", "conference")


def estimate_update_period(text: str, mime_type: str | None) -> int:
    """Estimate seconds until this knowledge may go stale. -1 = never."""
    lower = text[:2000].lower()
    if any(h in lower for h in _PERMANENT_HINTS):
        return -1
    if any(h in lower for h in _EVENT_HINTS):
        return 86400  # ~1 day — events change fast
    return -1  # default: treat as permanent until an LLM refines this


def estimate_distillation_score(source_type: str) -> float:
    """0.0 = raw source, 1.0 = own insight. Raw ingested content defaults to 0.1.

    All raw third-party content (url/pdf/image/youtube/audio/github) scores LOW;
    only own 'text' notes score high.
    """
    return 0.1 if source_type in ("url", "pdf", "image", "youtube", "audio", "github") else 0.8


# ---------------------------------------------------------------------------
# Relationship extraction
# ---------------------------------------------------------------------------

_RELATIONSHIP_TYPES = ("associative", "problem-solution", "cause-effect", "related")


def auto_link(node_id: str, text: str, conn, top_candidates: int = 10) -> list[str]:
    """Find existing nodes semantically similar to the new node and create links.

    Returns list of created relationship ids. INSERT-only on relationships
    (ON CONFLICT DO NOTHING due to UNIQUE(from_id, to_id, type)).
    """
    q_emb = embed_texts([text])[0]
    # Single query: candidate rows come back WITH their embedding, so the loop
    # below never re-SELECTs per candidate (was an N+1).
    candidates = conn.execute(
        """
        SELECT id, content, embedding FROM knowledge_items
        WHERE id != %s AND embedding IS NOT NULL
        ORDER BY embedding <=> %s::vector LIMIT %s
        """,
        (node_id, q_emb.tolist(), top_candidates),
    ).fetchall()

    created: list[str] = []
    for cand in candidates:
        cand_emb = np.array(cand["embedding"], dtype=np.float32)
        sim = float(
            np.dot(q_emb, cand_emb)
            / (np.linalg.norm(q_emb) * np.linalg.norm(cand_emb) + 1e-9)
        )
        if sim < 0.75:  # not similar enough to link
            continue
        strength = round(min(sim, 1.0), 4)
        rel_type = "associative"  # default; LLM would classify more precisely
        row = conn.execute(
            """
            INSERT INTO relationships (from_id, to_id, type, strength, direction, context)
            VALUES (%s, %s, %s, %s, 'bidirectional', %s)
            ON CONFLICT (from_id, to_id, type) DO NOTHING
            RETURNING id
            """,
            (node_id, cand["id"], rel_type, strength, f"auto-linked (sim={strength})"),
        ).fetchone()
        if row:
            created.append(row["id"])
    return created


# ---------------------------------------------------------------------------
# Phase 3 hook: optional LLM entity/relation extraction (env-gated)
# ---------------------------------------------------------------------------

# Source types whose extracted text is mined for entities/relations when
# EXTRACT_ENABLED=1 (checked here, at the call site, per the locked design).
_EXTRACT_SOURCE_TYPES = ("text", "url", "pdf", "youtube", "audio", "github")


def _store_extracted_knowledge(conn, knowledge: dict, source_uri: str | None) -> dict:
    """Store validated LLM extraction results. INSERT-ONLY, all parameterized.

    * One knowledge_items row per unique entity name, addressed by
      source_uri = "entity://<slug>" (slug = [a-z0-9-]). An existing row with
      that source_uri is reused (SELECT first), never updated.
    * One relationships row per relation with strength = confidence,
      direction 'one_way', context 'llm-extracted', ON CONFLICT (from_id,
      to_id, type) DO NOTHING (schema UNIQUE constraint).
    """
    from . import extract as _llm_extract  # lazy — the module the caller patches

    name_to_id: dict[str, str] = {}
    slug_to_id: dict[str, str] = {}
    for ent in knowledge.get("entities", []):
        name = ent["name"]
        slug = _llm_extract.slugify(name)
        if not slug:
            continue
        entity_id = slug_to_id.get(slug)
        if entity_id is None:
            uri = f"entity://{slug}"
            # SELECT-first: an existing entity row is reused, never updated.
            row = conn.execute(
                "SELECT id FROM knowledge_items WHERE source_uri = %s",
                (uri,),
            ).fetchone()
            if row is not None:
                entity_id = row["id"]
            else:
                # Race-safe upsert: another process may INSERT the same
                # entity:// URI between our SELECT and INSERT. The partial
                # UNIQUE index knowledge_items_entity_source_uri_unique
                # (db/schema.sql) makes ON CONFLICT ... DO NOTHING catch it;
                # rowcount 0 / no RETURNING row means we lost the race, so
                # re-SELECT to resolve the id of the row that won. The
                # savepoint guarantees that a unique violation (or any other
                # failure of the INSERT) can never abort the outer
                # transaction. (The %% is psycopg placeholder escaping for
                # the literal % in the LIKE pattern.)
                etype = ent.get("type", "entity")
                conn.execute("SAVEPOINT entity_upsert")
                try:
                    row = conn.execute(
                        """
                        INSERT INTO knowledge_items
                            (type, title, content, mime_type, source_uri,
                             distillation_score, update_period, metadata)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                        ON CONFLICT (source_uri) WHERE source_uri LIKE 'entity://%%'
                        DO NOTHING
                        RETURNING id
                        """,
                        (
                            "entity", name,
                            f"Entity: {name} (type {etype}) extracted from {source_uri}",
                            "text/plain", uri,
                            0.1, -1,
                            json.dumps({
                                "extraction": "llm",
                                "entity_type": etype,
                                "origin_source_uri": source_uri,
                            }),
                        ),
                    ).fetchone()
                    if row is None:
                        # Lost the race — resolve to the winning row.
                        row = conn.execute(
                            "SELECT id FROM knowledge_items WHERE source_uri = %s",
                            (uri,),
                        ).fetchone()
                except Exception:
                    conn.execute("ROLLBACK TO SAVEPOINT entity_upsert")
                    row = conn.execute(
                        "SELECT id FROM knowledge_items WHERE source_uri = %s",
                        (uri,),
                    ).fetchone()
                finally:
                    conn.execute("RELEASE SAVEPOINT entity_upsert")
                if row is None:
                    raise RuntimeError(
                        f"entity upsert for {uri!r} found no row after a conflict"
                    )
                entity_id = row["id"]
            slug_to_id[slug] = entity_id
        name_to_id[name] = entity_id

    rel_count = 0
    for rel in knowledge.get("relations", []):
        from_id = name_to_id.get(rel["from"])
        to_id = name_to_id.get(rel["to"])
        if from_id is None or to_id is None:
            continue
        cur = conn.execute(
            """
            INSERT INTO relationships (from_id, to_id, type, strength, direction, context)
            VALUES (%s, %s, %s, %s, 'one_way', 'llm-extracted')
            ON CONFLICT (from_id, to_id, type) DO NOTHING
            """,
            (from_id, to_id, rel["type"], float(rel["confidence"])),
        )
        # Count only rows ACTUALLY inserted: ON CONFLICT DO NOTHING leaves
        # cursor.rowcount at 0 for a skipped duplicate (reviewer LOW — the
        # count used to increment unconditionally). Test doubles without a
        # rowcount attribute fall back to the legacy count behavior.
        if getattr(cur, "rowcount", 1) != 0:
            rel_count += 1
    return {"entities": len(name_to_id), "relations": rel_count}


def _run_llm_extraction(text: str, source_uri: str | None, conn) -> dict:
    """Env-gated Phase 3 hook body: extract entities/relations and store them.

    pipeline.extract is imported lazily so the default pipeline (extraction
    off) pays nothing. Extraction must NEVER break ingestion: every exception
    is sanitized (no API key / Authorization material), logged to stderr,
    and swallowed.
    """
    from . import extract as _llm_extract
    try:
        knowledge = _llm_extract.extract_knowledge(text)
        return _store_extracted_knowledge(conn, knowledge, source_uri)
    except Exception as exc:
        detail = _llm_extract._sanitize(f"{type(exc).__name__}: {exc}")
        print(f"[ingest] llm extraction failed (ignored): {detail}", file=sys.stderr)
        return {"entities": 0, "relations": 0, "error": detail}


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def ingest(source_type: str, payload: str | bytes, source_uri: str | None = None,
           title: str | None = None, store_raw: bool = False) -> dict:
    """Ingest raw content into the knowledge base.

    Args:
        store_raw: when True, the raw payload bytes are written to the
            content-addressed store (pipeline/raw_store.py: raw/<sha[:2]>/<sha>)
            and the hash is recorded in every node's metadata["raw_sha256"].
            If no source_uri was given, the node's source_uri becomes
            "raw://<sha256>". The pipeline stays insert-only and dedup still
            runs on the extracted text (plus source_uri) first.

    Returns a dict with keys: status ('inserted' | 'duplicate' | 'empty'),
    node_ids, relationship_ids, chunks — plus raw_sha256 when store_raw=True.
    """
    result = extract(source_type, payload, source_uri)

    raw_sha256: str | None = None
    if store_raw:
        raw_bytes = payload if isinstance(payload, bytes) else str(payload).encode()
        info = store_raw_bytes(raw_bytes)
        raw_sha256 = info["sha256"]
        if source_uri is None:
            source_uri = f"raw://{raw_sha256}"
            if result.source_uri is None:
                result.source_uri = source_uri

    text = result.text
    if not text.strip():
        out = {"status": "empty", "node_ids": [], "relationship_ids": [], "chunks": 0}
        if raw_sha256:
            out["raw_sha256"] = raw_sha256
        return out

    extraction = None
    with get_connection() as conn:
        # Dedup. document_hash is the whole-document hash (sha256 of the
        # normalized full extracted text) — stored on the document node's
        # metadata so later ingests can tell "same version" from "new version".
        doc_hash = content_hash(text)
        existing_id = find_duplicate(text, result.source_uri, conn, document_hash=doc_hash)
        if existing_id:
            out = {
                "status": "duplicate",
                "node_ids": [existing_id],
                "relationship_ids": [],
                "chunks": 0,
            }
            if raw_sha256:
                out["raw_sha256"] = raw_sha256
            return out

        chunks = chunk_text(text)
        embeddings = embed_texts(chunks)
        update_period = estimate_update_period(text, result.mime_type)
        distill = estimate_distillation_score(source_type)

        # If single chunk → one node. If multiple → first chunk is the document
        # node, rest are excerpts pointing to it.
        node_ids: list[str] = []
        doc_id: str | None = None

        for i, (chunk, emb) in enumerate(zip(chunks, embeddings)):
            is_excerpt = len(chunks) > 1 and i > 0
            node_type = "excerpt" if is_excerpt else ("document" if len(chunks) > 1 else "note")
            parent = doc_id if is_excerpt else None
            meta = {"content_hash": content_hash(chunk)}
            if i == 0:
                # The document node (single node when 1 chunk, else the first
                # node) carries the whole-document hash.
                meta["document_hash"] = doc_hash
            if raw_sha256:
                meta["raw_sha256"] = raw_sha256
            row = conn.execute(
                """
                INSERT INTO knowledge_items
                    (type, title, content, mime_type, source_uri, source_document_id,
                     distillation_score, embedding, update_period, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s::vector, %s, %s::jsonb)
                RETURNING id
                """,
                (
                    node_type, title, chunk, result.mime_type, result.source_uri,
                    parent, distill, emb.tolist(), update_period, json.dumps(meta),
                ),
            ).fetchone()
            nid = row["id"]
            node_ids.append(nid)
            if doc_id is None:
                doc_id = nid

        # Auto-link every new node to existing knowledge
        rel_ids: list[str] = []
        for nid, chunk in zip(node_ids, chunks):
            rel_ids.extend(auto_link(nid, chunk, conn))

        # Phase 3 hook (env-gated, optional): LLM entity/relation extraction
        # at the END of a successful insert. Extraction must never break
        # ingestion — any failure is logged to stderr and swallowed here.
        if os.environ.get("EXTRACT_ENABLED") == "1" and source_type in _EXTRACT_SOURCE_TYPES:
            try:
                extraction = _run_llm_extraction(text, result.source_uri, conn)
            except Exception as exc:
                # Defense in depth: _run_llm_extraction already sanitizes, but
                # this outer catch must NEVER print raw exception text — it may
                # embed EXTRACT_API_KEY / Authorization material. Pass it
                # through pipeline.extract._sanitize before printing (reviewer LOW).
                try:
                    from . import extract as _llm_extract_mod
                    detail = _llm_extract_mod._sanitize(f"{type(exc).__name__}: {exc}")
                except Exception:
                    detail = type(exc).__name__  # _sanitize itself failed — leak nothing
                print(f"[ingest] llm extraction hook failed (ignored): {detail}",
                      file=sys.stderr)

    out = {
        "status": "inserted",
        "node_ids": node_ids,
        "relationship_ids": rel_ids,
        "chunks": len(chunks),
    }
    if extraction is not None:
        out["extraction"] = extraction
    if raw_sha256:
        out["raw_sha256"] = raw_sha256
    return out
