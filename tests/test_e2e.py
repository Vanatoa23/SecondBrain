"""End-to-end integration test against a REAL PostgreSQL + pgvector database.

Setup — use a DEDICATED THROWAWAY database, never a production one:

    # docker (see db/README.md)
    docker run -d --name secondbrain-db -e POSTGRES_PASSWORD=postgres \
        -e POSTGRES_DB=secondbrain_test -p 5432:5432 pgvector/pgvector:pg16
    # or against an existing server:
    createdb secondbrain_test

    export TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/secondbrain_test
    python tests/test_e2e.py

TEST_DATABASE_URL is the ONLY accepted variable — there is deliberately no
DATABASE_URL fallback, because apply_schema() drops tables and must never be
pointed at a real database. As a second guard, the script refuses to run
against any database whose name does not contain "test". If TEST_DATABASE_URL
is unset or unreachable, it prints
"SKIP: no test database available (set TEST_DATABASE_URL)" and exits 0, so it
never breaks environments without a DB.

The script applies db/schema.sql itself, idempotently: before applying it drops
and recreates the two tables (knowledge_items, relationships) in the TEST
database only — no other database is touched. Schema is applied with
`psql -f` when psql is available, otherwise by executing the file contents
via psycopg.

If the embedding/reranker models (bge-m3, bge-reranker-v2-m3) cannot be
downloaded, model-dependent steps print SKIP; the DB / dedup / FTS steps still
run (via direct SQL inserts and retrieval.fts.fts_search).

All SQL is parameterized.
"""

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

REPO_ROOT = Path(__file__).resolve().parent.parent

import psycopg  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402

DOC_A = ("PostgreSQL pgvector HNSW index gives approximate nearest neighbor "
         "search over 1024-dim bge-m3 embeddings")
DOC_B = ("bge-m3 embeddings are 1024 dimensional and stored in Postgres "
         "pgvector with an HNSW index")

SKIP_MSG = "SKIP: no test database available (set TEST_DATABASE_URL)"

TEST_DBNAME_RE = re.compile(r"(?:^|_)test(?:_|$)", re.IGNORECASE)


def get_test_url() -> str | None:
    # TEST_DATABASE_URL only — never fall back to DATABASE_URL: apply_schema()
    # DROPs the two tables and must never touch a non-test database.
    return os.environ.get("TEST_DATABASE_URL")


def _safe_db_label(url: str) -> str:
    """Credential-free label for SKIP/connect-failed messages (host:port/dbname).

    Parsed with psycopg.conninfo.conninfo_to_dict and stripped of the password:
    keyword-form conninfo has no '@' at all, so a naive split on the '@'
    separator would echo the whole conninfo — credentials included.
    """
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
        from pipeline.embed import embed_text
        embed_text("warmup")
        from retrieval.rerank import rerank
        rerank("warmup", [{"content": "warmup"}])
        return True, ""
    except Exception as e:
        return False, f"{type(e).__name__}: {str(e)[:200]}"


# --- steps -------------------------------------------------------------------

def step_1_ingest_doc_a(ctx) -> None:
    if not ctx["model_ok"]:
        print(f"SKIP step_1_ingest_doc_a (model unavailable: {ctx['model_err']})")
        return
    from pipeline.ingest import ingest
    res = ingest("text", DOC_A, source_uri="e2e://doc-a", title="pgvector HNSW note")
    assert res["status"] == "inserted", res
    assert res["node_ids"], res
    ctx["doc_a_ids"] = res["node_ids"]
    print("PASS step_1_ingest_doc_a")


def step_2_ingest_doc_b_autolink(ctx) -> None:
    if not ctx["model_ok"]:
        print(f"SKIP step_2_ingest_doc_b_autolink (model unavailable: {ctx['model_err']})")
        return
    from pipeline.ingest import ingest
    res = ingest("text", DOC_B, source_uri="e2e://doc-b", title="bge-m3 note")
    assert res["status"] == "inserted", res
    assert res["node_ids"], res
    ctx["doc_b_ids"] = res["node_ids"]
    # auto_link (pipeline/ingest.py) links a new node to semantically similar
    # existing nodes (cosine >= 0.75) via INSERT into relationships.
    # DOC_A/DOC_B are deliberately near-identical, so at least one
    # relationships row MUST exist after this ingest (strict — no fallback).
    with psycopg.connect(ctx["url"], row_factory=dict_row) as conn:
        row = conn.execute(
            """
            SELECT count(*) AS n FROM relationships
            WHERE from_id = ANY(%s) OR to_id = ANY(%s)
            """,
            (res["node_ids"], res["node_ids"]),
        ).fetchone()
    assert res["relationship_ids"] or row["n"] > 0, (
        "expected at least one relationships row after ingesting the "
        f"deliberately-similar DOC_B (got relationship_ids={res['relationship_ids']}, "
        f"db rows={row['n']})"
    )
    print("PASS step_2_ingest_doc_b_autolink (relationships row created)")


def step_3_duplicate_detected(ctx) -> None:
    from pipeline.dedup import find_duplicate
    if not ctx["model_ok"]:
        # DB/dedup path without the model: insert doc A directly, then the
        # source_uri/content-hash checks in find_duplicate must catch it
        # (they run before the semantic-embedding check).
        with psycopg.connect(ctx["url"], row_factory=dict_row) as conn:
            conn.execute(
                "INSERT INTO knowledge_items (type, title, content, mime_type, source_uri)"
                " VALUES (%s, %s, %s, %s, %s)",
                ("note", "pgvector HNSW note", DOC_A, "text/plain", "e2e://doc-a"),
            )
            dup = find_duplicate(DOC_A, "e2e://doc-a", conn)
        assert dup is not None
        print("PASS step_3_duplicate_detected (content/source dedup without model)")
        return
    from pipeline.ingest import ingest
    res = ingest("text", DOC_A, source_uri="e2e://doc-a", title="pgvector HNSW note")
    assert res["status"] == "duplicate", res
    print("PASS step_3_duplicate_detected")


def step_4_search_hybrid(ctx) -> None:
    if not ctx["model_ok"]:
        print(f"SKIP step_4_search_hybrid (model unavailable: {ctx['model_err']})")
        return
    from retrieval.search import search
    out = search("HNSW pgvector index", top_k=5)
    assert out["results"], out
    assert any(r.get("source_uri") == "e2e://doc-a" for r in out["results"]), out["results"]
    print("PASS step_4_search_hybrid")


def step_5_search_fts(ctx) -> None:
    if not ctx["model_ok"]:
        # FTS path without the model: exercise retrieval.fts directly.
        with psycopg.connect(ctx["url"], row_factory=dict_row) as conn:
            conn.execute(
                "INSERT INTO knowledge_items (type, title, content, mime_type, source_uri)"
                " VALUES (%s, %s, %s, %s, %s)"
                " ON CONFLICT DO NOTHING",
                ("note", "bge-m3 note", DOC_B, "text/plain", "e2e://doc-b"),
            )
            from retrieval.fts import fts_search
            rows = fts_search(conn, "dimensional", top_k=5)
        assert any(r["source_uri"] == "e2e://doc-b" for r in rows), rows
        print("PASS step_5_search_fts (direct fts_search without model)")
        return
    from retrieval.search import search
    # "dimensional" occurs only in doc B
    out = search("dimensional embeddings", methods=["fts"], top_k=5)
    assert out["results"], out
    assert any(r.get("source_uri") == "e2e://doc-b" for r in out["results"]), out["results"]
    print("PASS step_5_search_fts")


def step_6_search_vector(ctx) -> None:
    if not ctx["model_ok"]:
        print(f"SKIP step_6_search_vector (model unavailable: {ctx['model_err']})")
        return
    from retrieval.search import search
    out = search("HNSW pgvector index", methods=["vector"], top_k=5)
    assert out["results"], out
    uris = {r.get("source_uri") for r in out["results"]}
    assert uris & {"e2e://doc-a", "e2e://doc-b"}, out["results"]
    print("PASS step_6_search_vector")


def step_7_staleness_soft_penalty(ctx) -> None:
    from retrieval.merge import is_stale
    node_id = (ctx.get("doc_a_ids") or [None])[0]
    with psycopg.connect(ctx["url"], row_factory=dict_row) as conn:
        if node_id is None:
            # no model -> no ingested rows; insert one directly for the DB check
            row = conn.execute(
                "INSERT INTO knowledge_items (type, title, content, mime_type, source_uri)"
                " VALUES (%s, %s, %s, %s, %s) RETURNING id",
                ("note", "pgvector HNSW note", DOC_A, "text/plain", "e2e://doc-a"),
            ).fetchone()
            node_id = row["id"]
        conn.execute(
            "UPDATE knowledge_items SET update_period = %s,"
            " created_at = now() - make_interval(days => %s) WHERE id = %s",
            (1, 10, node_id),
        )
        fetched = conn.execute(
            "SELECT id, update_period, created_at FROM knowledge_items WHERE id = %s",
            (node_id,),
        ).fetchone()
    assert is_stale(dict(fetched)) is True
    if not ctx["model_ok"]:
        print("PASS step_7_staleness_soft_penalty (is_stale=True; search SKIPPED without model)")
        return
    from retrieval.search import search
    out = search("HNSW pgvector index", top_k=5)
    # locked spec: stale items are penalized, NEVER filtered out
    assert any(r["id"] == node_id for r in out["results"]), out["results"]
    stale_entry = next(r for r in out["results"] if r["id"] == node_id)
    assert stale_entry.get("stale") is True
    print("PASS step_7_staleness_soft_penalty (stale item still present, penalized)")


def step_8_cleanup(ctx) -> None:
    with psycopg.connect(ctx["url"], row_factory=dict_row) as conn:
        conn.execute("DELETE FROM knowledge_items WHERE source_uri LIKE %s", ("e2e://%",))
        remaining = conn.execute(
            "SELECT count(*) AS n FROM knowledge_items WHERE source_uri LIKE %s",
            ("e2e://%",),
        ).fetchone()["n"]
    assert remaining == 0
    print("PASS step_8_cleanup")


def main() -> None:
    url = get_test_url()
    if not url:
        print(SKIP_MSG)
        sys.exit(0)

    name = db_name(url)
    if not TEST_DBNAME_RE.search(name):
        print(f"REFUSING to run against database {name!r}: TEST_DATABASE_URL must "
              "point at a test database (delimited 'test' component) — apply_schema() "
              "drops the knowledge tables.")
        sys.exit(1)

    if not db_reachable(url):
        print(SKIP_MSG)
        print(f"(connect to {_safe_db_label(url)} failed)")
        sys.exit(0)

    # pipeline/ and retrieval/ read DATABASE_URL — point them at the test DB
    os.environ["DATABASE_URL"] = url

    apply_schema(url)
    print("PASS setup_schema (db/schema.sql applied to test DB)")

    # Vector columns load as numpy arrays via the loader registered in
    # pipeline/db.py and retrieval/db.py get_connection() — no shim needed here.

    model_ok, model_err = check_models()
    if not model_ok:
        print(f"SKIP: embedding/reranker models unavailable ({model_err}) — "
              "model-dependent steps will be skipped")

    ctx = {"url": url, "model_ok": model_ok, "model_err": model_err}
    steps = [
        step_1_ingest_doc_a,
        step_2_ingest_doc_b_autolink,
        step_3_duplicate_detected,
        step_4_search_hybrid,
        step_5_search_fts,
        step_6_search_vector,
        step_7_staleness_soft_penalty,
    ]
    try:
        for step in steps:
            step(ctx)
    finally:
        step_8_cleanup(ctx)
    print("All e2e tests passed.")


if __name__ == "__main__":
    main()
