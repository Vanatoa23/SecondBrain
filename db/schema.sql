-- Second Brain — Database Schema v1
-- PostgreSQL 16 + pgvector
-- Locked spec: see PM_LEDGER.md [2026-11-25 17:56] ARCHITECTURE: Database schema v1 locked

CREATE EXTENSION IF NOT EXISTS vector;

-- ---------------------------------------------------------------------------
-- knowledge_items: every node in the knowledge graph.
-- type: document | excerpt | note | media
-- content: searchable text (extracted text, OCR, transcript, or description)
-- source_uri: where the original lives (URL, GCS path, disk path)
-- source_document_id: set on excerpts pointing to their parent document
-- distillation_score: 0.0 = raw source, 1.0 = own original insight
-- embedding: bge-m3, 1024 dims, cosine distance
-- update_period: seconds until this knowledge may go stale; -1 = never expires
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS knowledge_items (
    id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    type                text NOT NULL,
    title               text,
    content             text NOT NULL,
    mime_type           text,
    source_uri          text,
    source_document_id  uuid REFERENCES knowledge_items(id) ON DELETE SET NULL,
    distillation_score  real CHECK (distillation_score BETWEEN 0 AND 1),
    embedding           vector(1024),
    created_at          timestamptz NOT NULL DEFAULT now(),
    update_period       bigint NOT NULL DEFAULT -1,
    metadata            jsonb NOT NULL DEFAULT '{}'::jsonb
);

-- ---------------------------------------------------------------------------
-- relationships: edges between knowledge nodes.
-- type: associative | problem-solution | cause-effect | contradiction | ...
-- direction: one_way | bidirectional
-- context: free-text "why these are linked"
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS relationships (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    from_id     uuid NOT NULL REFERENCES knowledge_items(id) ON DELETE CASCADE,
    to_id       uuid NOT NULL REFERENCES knowledge_items(id) ON DELETE CASCADE,
    type        text NOT NULL,
    strength    real CHECK (strength BETWEEN 0 AND 1),
    direction   text NOT NULL DEFAULT 'one_way'
                CHECK (direction IN ('one_way', 'bidirectional')),
    context     text,
    created_at  timestamptz NOT NULL DEFAULT now(),
    UNIQUE (from_id, to_id, type)
);

-- HNSW index for vector similarity search (cosine distance)
CREATE INDEX IF NOT EXISTS idx_embedding_hnsw
    ON knowledge_items USING hnsw (embedding vector_cosine_ops);

-- GIN index for full-text search on content + title
CREATE INDEX IF NOT EXISTS idx_content_fts
    ON knowledge_items USING gin (to_tsvector('english', content || ' ' || coalesce(title, '')));

-- Indexes for graph traversal and staleness computation
CREATE INDEX IF NOT EXISTS idx_relationships_from ON relationships (from_id);
CREATE INDEX IF NOT EXISTS idx_relationships_to   ON relationships (to_id);
CREATE INDEX IF NOT EXISTS idx_items_source_doc     ON knowledge_items (source_document_id);
CREATE INDEX IF NOT EXISTS idx_items_created        ON knowledge_items (created_at);

-- Partial UNIQUE index on entity:// source URIs (audit-E/F LOW fix): LLM-extracted
-- entity nodes are addressed by source_uri = "entity://<slug>". This index makes the
-- SELECT-first upsert in pipeline/ingest.py race-safe across processes — a concurrent
-- INSERT of the same entity:// URI conflicts here, and the ingest path handles it with
-- ON CONFLICT (source_uri) WHERE source_uri LIKE 'entity://%' DO NOTHING + re-SELECT.
CREATE UNIQUE INDEX IF NOT EXISTS knowledge_items_entity_source_uri_unique
    ON knowledge_items (source_uri) WHERE source_uri LIKE 'entity://%';
