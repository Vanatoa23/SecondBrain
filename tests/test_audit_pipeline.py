"""Audit-worker W2 tests — dedup versioning, supersedes edges, entity
embeddings + extracted_from edges, extraction outside the insert txn,
auto_link same-source exclusion / no reverse edges / q_emb reuse,
distillation_override, sentence-split of >CHUNK_THRESHOLD paragraphs,
schema indexes.

Runs against a REAL PostgreSQL + pgvector database. Setup — DEDICATED
THROWAWAY database only:

    createdb audit_w2_test
    export TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:5433/audit_w2_test
    python tests/test_audit_pipeline.py

Guard pattern (identical to tests/test_e2e.py):
  * TEST_DATABASE_URL is the ONLY accepted variable — no DATABASE_URL
    fallback, because apply_schema() drops the knowledge tables.
  * Refuses any database whose name does not match (^|_)test(_|$)
    (REFUSING, exit 1) BEFORE any connection or destructive op.
  * Unset/unreachable TEST_DATABASE_URL -> "SKIP: ..." exit 0.

Run one test process at a time: loading torch in 2+ processes at once can
hit the 16GB commit limit (os error 1455), so the intra-op thread caps are
set BEFORE any torch import.

Covered fixes (pipeline/dedup.py, pipeline/ingest.py, pipeline/chunk.py,
pipeline/db.py, db/schema.sql):
  (a) note v1 then v2 (v1 + one sentence, same source_uri) -> v2 "inserted"
      (NOT a semantic duplicate) and a supersedes edge new->old exists.
  (b) re-ingesting identical v2 -> "duplicate".
  (c) chunk_text splits a ~3000-token single-paragraph text into >1 chunk.
  (d) distillation_override is respected in the inserted row.
  (e) auto_link never links two chunks of the same document and never
      creates both (A,B) and (B,A).
  (f) with pipeline.extract mocked: entity rows have embedding NOT NULL and
      an extracted_from edge from the doc node to each entity.
  (g) the three new indexes exist in pg_indexes after applying schema.sql.

All SQL is parameterized.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

# 16GB-box commit-limit safety: cap intra-op threads BEFORE any torch import.
os.environ["OMP_NUM_THREADS"] = "2"
os.environ["MKL_NUM_THREADS"] = "2"
os.environ["NUMEXPR_NUM_THREADS"] = "2"

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

REPO_ROOT = Path(__file__).resolve().parent.parent

import psycopg  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402

SKIP_MSG = "SKIP: no test database available (set TEST_DATABASE_URL)"

TEST_DBNAME_RE = re.compile(r"(?:^|_)test(?:_|$)", re.IGNORECASE)

URI_PREFIX = "auditw2://"


# --- guard helpers (pattern copied from tests/test_e2e.py) --------------------

def get_test_url() -> str | None:
    # TEST_DATABASE_URL only — never fall back to DATABASE_URL: apply_schema()
    # DROPs the two tables and must never touch a non-test database.
    return os.environ.get("TEST_DATABASE_URL")


def _safe_db_label(url: str) -> str:
    """Credential-free label for SKIP/connect-failed messages (host:port/dbname)."""
    try:
        from psycopg import conninfo
        d = conninfo.conninfo_to_dict(url)
        d.pop("password", None)
        host = d.get("host") or "localhost"
        port = d.get("port") or "5432"
        dbname = d.get("dbname") or ""
        return f"{host}:{port}/{dbname}"
    except Exception:
        return "<unparseable conninfo>"


def db_name(url: str) -> str:
    try:
        from psycopg import conninfo
        return conninfo.conninfo_to_dict(url).get("dbname", "") or ""
    except Exception:
        import re
        m = re.search(r"/([^/?]+)", url)
        return m.group(1) if m else ""


def is_test_dbname(name: str) -> bool:
    return bool(TEST_DBNAME_RE.search(name or ""))


def db_reachable(url: str) -> bool:
    try:
        conn = psycopg.connect(url, connect_timeout=5)
        conn.close()
        return True
    except Exception:
        return False


def apply_schema(url: str) -> None:
    """Idempotently apply db/schema.sql to the test DB (drops the two tables first)."""
    schema_path = REPO_ROOT / "db" / "schema.sql"
    schema_sql = schema_path.read_text()
    with psycopg.connect(url) as conn:
        conn.execute("DROP TABLE IF EXISTS relationships CASCADE")
        conn.execute("DROP TABLE IF EXISTS knowledge_items CASCADE")
    psql = shutil.which("psql")
    if psql:
        subprocess.run(
            [psql, url, "-v", "ON_ERROR_STOP=1", "-f", str(schema_path)],
            check=True, capture_output=True, text=True,
        )
    else:
        with psycopg.connect(url) as conn:
            conn.execute(schema_sql)  # no params -> simple protocol, multi-statement ok


def check_models() -> tuple[bool, str]:
    """Try to load bge-m3 (embed) and bge-reranker-v2-m3 (rerank); trigger downloads."""
    try:
        import torch
        torch.set_num_threads(2)  # commit-limit safety on the 16GB box
        from pipeline.embed import embed_text
        embed_text("warmup")
        from retrieval.rerank import rerank
        rerank("warmup", [{"content": "warmup"}])
        return True, ""
    except Exception as e:
        return False, f"{type(e).__name__}: {str(e)[:200]}"


def connect(url: str):
    return psycopg.connect(url, row_factory=dict_row)


# --- test documents ------------------------------------------------------------

V1 = ("The Second Brain audit note covers versioning of ingested documents "
      "and how new versions relate to earlier ones.")
V2 = V1 + " A new version appends one extra sentence about supersedes edges."

_OVERRIDE_TXT = "A short unique note used to verify the distillation override value."

_SENT = [
    "{t} is a core idea that practitioners apply daily in real systems.",
    "Measurements of {t} show consistent results across repeated trials.",
    "Critics of {t} point to edge cases, but the evidence still favors it.",
    "A short history of {t} traces back to early experiments decades ago.",
    "Implementing {t} requires careful attention to constraints and tradeoffs.",
    "Comparisons between {t} and its alternatives highlight clear differences.",
    "Case studies of {t} appear frequently in industry writeups this year.",
    "Common mistakes when using {t} include skipping validation steps early.",
    "Tooling around {t} has matured enough for routine production use now.",
    "Beginners often misunderstand {t} until they see a worked example.",
    "Benchmarks of {t} vary widely depending on the dataset chosen.",
    "The community around {t} maintains extensive public documentation.",
]


def _doc(topic: str, n_paras: int = 6) -> str:
    """~470-token paragraphs, n_paras of them -> a multi-chunk document (>2000 tokens)."""
    paras = []
    for p in range(n_paras):
        sents = [_SENT[(p * 3 + i) % len(_SENT)].format(t=topic) for i in range(24)]
        paras.append(" ".join(sents))
    return "\n\n".join(paras)


BIG = _doc("pgvector HNSW indexing and approximate nearest neighbor search")
BIG_SIMILAR = _doc("pgvector HNSW index and approximate nearest neighbor search over vectors")

# deliberately near-identical short docs (cosine >= 0.75 to link, < 0.95 to insert)
LINK_A = ("PostgreSQL pgvector HNSW index gives approximate nearest neighbor "
          "search over 1024-dim bge-m3 embeddings")
LINK_B = ("bge-m3 embeddings are 1024 dimensional and stored in Postgres "
          "pgvector with an HNSW index")


# --- steps ---------------------------------------------------------------------

def step_g_schema_indexes(ctx) -> None:
    """(g) the three new indexes (and pg_trgm) exist after applying schema.sql."""
    with connect(ctx["url"]) as conn:
        names = {r["indexname"] for r in conn.execute(
            "SELECT indexname FROM pg_indexes WHERE schemaname = 'public'").fetchall()}
        exts = {r["extname"] for r in conn.execute(
            "SELECT extname FROM pg_extension").fetchall()}
    for want in ("idx_items_source_uri", "idx_items_document_hash", "idx_items_content_hash"):
        assert want in names, f"missing index {want} in pg_indexes: {sorted(names)}"
    assert "pg_trgm" in exts, f"pg_trgm extension missing: {sorted(exts)}"
    print("PASS step_g_schema_indexes (idx_items_source_uri, idx_items_document_hash, "
          "idx_items_content_hash in pg_indexes; pg_trgm extension present)")


def step_c_chunk_single_paragraph(ctx) -> None:
    """(c) a ~3000-token SINGLE-paragraph text is split into >1 chunk."""
    from pipeline.chunk import CHUNK_THRESHOLD, HARD_CAP, chunk_text, estimate_tokens
    text = "This is a test sentence about chunking single paragraphs. " * 200  # ~3000 tokens, one paragraph
    assert "\n\n" not in text
    assert estimate_tokens(text) > CHUNK_THRESHOLD, estimate_tokens(text)
    chunks = chunk_text(text)
    assert len(chunks) > 1, f"single paragraph of {estimate_tokens(text)} tokens must split, got {len(chunks)}"
    for c in chunks:
        assert estimate_tokens(c) <= HARD_CAP, estimate_tokens(c)
    print(f"PASS step_c_chunk_single_paragraph ({estimate_tokens(text)} tok, 1 paragraph -> "
          f"{len(chunks)} chunks, all <= HARD_CAP={HARD_CAP})")


def step_a_version_inserted_supersedes(ctx) -> None:
    """(a) v2 = v1 + one sentence, same source_uri -> inserted + supersedes edge."""
    from pipeline.ingest import ingest
    uri = URI_PREFIX + "versions"
    r1 = ingest("text", V1, source_uri=uri, title="audit w2 v1")
    assert r1["status"] == "inserted", r1
    r2 = ingest("text", V2, source_uri=uri, title="audit w2 v2")
    assert r2["status"] == "inserted", f"v2 must NOT be dropped as a semantic duplicate: {r2}"
    assert r2["node_ids"][0] != r1["node_ids"][0]
    with connect(ctx["url"]) as conn:
        edge = conn.execute(
            """
            SELECT from_id, to_id, type, direction, context FROM relationships
            WHERE type = 'supersedes' AND from_id = %s AND to_id = %s
            """,
            (r2["node_ids"][0], r1["node_ids"][0]),
        ).fetchone()
    assert edge is not None, "supersedes edge new->old missing"
    assert edge["direction"] == "one_way", edge
    assert uri in (edge["context"] or ""), edge
    ctx["v1_id"], ctx["v2_id"], ctx["v_uri"] = r1["node_ids"][0], r2["node_ids"][0], uri
    print("PASS step_a_version_inserted_supersedes (v2 inserted despite ~0.99 cosine to v1; "
          "supersedes edge new->old exists)")


def step_b_reingest_duplicate(ctx) -> None:
    """(b) re-ingesting identical v2 -> duplicate of the v2 node."""
    from pipeline.ingest import ingest
    r = ingest("text", V2, source_uri=ctx["v_uri"], title="audit w2 v2 replay")
    assert r["status"] == "duplicate", r
    assert r["node_ids"][0] == ctx["v2_id"], r
    print("PASS step_b_reingest_duplicate (identical v2 -> duplicate)")


def step_d_distillation_override(ctx) -> None:
    """(d) distillation_override is stored on the inserted row."""
    from pipeline.ingest import ingest
    r = ingest("text", _OVERRIDE_TXT, source_uri=URI_PREFIX + "override",
               title="audit w2 override", distillation_override=0.1)
    assert r["status"] == "inserted", r
    with connect(ctx["url"]) as conn:
        row = conn.execute(
            "SELECT distillation_score FROM knowledge_items WHERE id = %s",
            (r["node_ids"][0],),
        ).fetchone()
    assert row is not None and abs(row["distillation_score"] - 0.1) < 1e-6, row
    # default for source_type "text" would be 0.8 — the override must win.
    assert abs(row["distillation_score"] - 0.8) > 1e-6
    print(f"PASS step_d_distillation_override (row distillation_score={row['distillation_score']}, "
          "not the 0.8 default for text)")


def step_e_autolink_same_source_and_bidir(ctx) -> None:
    """(e) no links between chunks of the same document; never both (A,B) and (B,A)."""
    from pipeline.ingest import ingest
    rbig = ingest("text", BIG, source_uri=URI_PREFIX + "big", title="audit w2 big")
    assert rbig["status"] == "inserted", rbig
    assert rbig["chunks"] > 1, rbig
    big_ids = set(rbig["node_ids"])

    rsim = ingest("text", BIG_SIMILAR, source_uri=URI_PREFIX + "big-similar",
                  title="audit w2 big similar")
    assert rsim["status"] == "inserted", rsim
    rlink = ingest("text", LINK_A, source_uri=URI_PREFIX + "link-a", title="audit w2 link a")
    assert rlink["status"] == "inserted", rlink
    rlink2 = ingest("text", LINK_B, source_uri=URI_PREFIX + "link-b", title="audit w2 link b")
    assert rlink2["status"] == "inserted", rlink2

    sim_ids = set(rsim["node_ids"])
    link_a_ids = set(rlink["node_ids"])
    link_b_ids = set(rlink2["node_ids"])
    all_ids = big_ids | sim_ids | link_a_ids | link_b_ids
    ctx["all_ids"] = all_ids
    with connect(ctx["url"]) as conn:
        rels = conn.execute(
            """
            SELECT from_id, to_id, type FROM relationships
            WHERE from_id = ANY(%s) OR to_id = ANY(%s)
            """,
            (list(all_ids), list(all_ids)),
        ).fetchall()
    pairs = {(r["from_id"], r["to_id"], r["type"]) for r in rels}
    # no edge between two chunks of the SAME document (shared source_uri)
    same_doc = sum(1 for f, t, _ in pairs if f in big_ids and t in big_ids)
    # never both (A,B) and (B,A) for the same type
    both_dirs = [p for p in pairs if (p[1], p[0], p[2]) in pairs]
    # cross-document links exist between the similar docs (either direction)
    cross = sum(
        1 for f, t, _ in pairs
        if (f in big_ids and t in sim_ids) or (f in sim_ids and t in big_ids)
        or (f in link_a_ids and t in link_b_ids) or (f in link_b_ids and t in link_a_ids)
    )
    assert same_doc == 0, f"chunks of the same document were linked: {same_doc}"
    assert not both_dirs, f"both directions of an edge exist: {both_dirs}"
    assert cross > 0, "expected cross-document auto_links between similar docs"
    print(f"PASS step_e_autolink_same_source_and_bidir (0 same-doc edges; 0 reverse pairs; "
          f"{cross} cross-doc edges)")


def step_f_extraction_entities(ctx) -> None:
    """(f) mocked pipeline.extract: entity rows embedded + extracted_from edges."""
    import pipeline.extract as extract_mod
    from pipeline.ingest import ingest

    os.environ["EXTRACT_ENABLED"] = "1"
    orig = extract_mod.extract_knowledge
    extract_mod.extract_knowledge = lambda text, **kw: {
        "entities": [
            {"name": "Audit Widget", "type": "concept"},
            {"name": "Versioning", "type": "process"},
        ],
        "relations": [
            {"from": "Audit Widget", "to": "Versioning", "type": "related", "confidence": 0.9},
        ],
    }
    try:
        r = ingest("text", "The audit widget demonstrates versioning of ingested notes.",
                   source_uri=URI_PREFIX + "extract", title="audit w2 extract")
    finally:
        extract_mod.extract_knowledge = orig
        os.environ.pop("EXTRACT_ENABLED", None)
    assert r["status"] == "inserted", r
    assert r.get("extraction", {}).get("entities") == 2, r.get("extraction")
    doc_id = r["node_ids"][0]
    with connect(ctx["url"]) as conn:
        ents = conn.execute(
            """
            SELECT id, source_uri, embedding FROM knowledge_items
            WHERE source_uri LIKE %s
              AND metadata->>'origin_source_uri' = %s
            """,
            ("entity://%", URI_PREFIX + "extract"),
        ).fetchall()
        edges = conn.execute(
            """
            SELECT to_id, type, direction, strength, context FROM relationships
            WHERE from_id = %s AND type = 'extracted_from'
            """,
            (doc_id,),
        ).fetchall()
    assert len(ents) == 2, ents
    for e in ents:
        assert e["embedding"] is not None, f"entity {e['source_uri']} has NULL embedding"
    ent_ids = {e["id"] for e in ents}
    assert {e["to_id"] for e in edges} == ent_ids, (edges, ent_ids)
    for e in edges:
        assert e["direction"] == "one_way" and abs(e["strength"] - 1.0) < 1e-6, e
        assert (URI_PREFIX + "extract") in (e["context"] or ""), e
    print(f"PASS step_f_extraction_entities ({len(ents)} entity rows with embedding NOT NULL; "
          f"{len(edges)} extracted_from edges from the doc node, strength=1.0)")


def cleanup(ctx) -> None:
    with connect(ctx["url"]) as conn:
        conn.execute(
            "DELETE FROM knowledge_items WHERE source_uri LIKE %s OR source_uri LIKE %s",
            (URI_PREFIX + "%", "entity://%"),
        )
        remaining = conn.execute(
            "SELECT count(*) AS n FROM knowledge_items WHERE source_uri LIKE %s OR source_uri LIKE %s",
            (URI_PREFIX + "%", "entity://%"),
        ).fetchone()["n"]
    assert remaining == 0
    print("PASS cleanup (auditw2:// + entity:// test rows deleted)")


def main() -> None:
    url = get_test_url()
    if not url:
        print(SKIP_MSG)
        sys.exit(0)

    # Refusal guard BEFORE any connection or destructive op.
    name = db_name(url)
    if not is_test_dbname(name):
        print(f"REFUSING to run against database {name!r}: TEST_DATABASE_URL must "
              "point at a test database (dbname matching (^|_)test(_|$)) — "
              "apply_schema() drops the knowledge tables.")
        sys.exit(1)

    if not db_reachable(url):
        print(SKIP_MSG)
        print(f"(connect to {_safe_db_label(url)} failed)")
        sys.exit(0)

    os.environ["DATABASE_URL"] = url  # pipeline/ and retrieval/ read DATABASE_URL

    apply_schema(url)
    print("PASS setup_schema (db/schema.sql applied to test DB)")

    ctx = {"url": url}
    try:
        step_g_schema_indexes(ctx)   # (g) — no model needed
        step_c_chunk_single_paragraph(ctx)  # (c) — no model needed

        model_ok, model_err = check_models()
        if not model_ok:
            print(f"SKIP: embedding/reranker models unavailable ({model_err}) — "
                  "model-dependent steps will be skipped")
            return
        ctx["model_ok"] = model_ok

        step_a_version_inserted_supersedes(ctx)  # (a)
        step_b_reingest_duplicate(ctx)           # (b)
        step_d_distillation_override(ctx)        # (d)
        step_e_autolink_same_source_and_bidir(ctx)  # (e)
        step_f_extraction_entities(ctx)          # (f)
    finally:
        cleanup(ctx)
    print("All audit-pipeline W2 tests passed.")


if __name__ == "__main__":
    main()
