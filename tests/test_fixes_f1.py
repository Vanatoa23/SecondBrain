"""Fix-worker F1 tests — whole-doc hash dedup, staleness final ranking, 8k
token output budget, chunk hard cap, bridge input clamps.

Runs against a REAL PostgreSQL + pgvector database. Setup — DEDICATED
THROWAWAY database only:

    createdb fixf1_test
    export TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:5433/fixf1_test
    python tests/test_fixes_f1.py

Guard pattern (identical to tests/test_e2e.py):
  * TEST_DATABASE_URL is the ONLY accepted variable — no DATABASE_URL
    fallback, because apply_schema() drops the knowledge tables.
  * Refuses any database whose name does not contain "test" (REFUSING,
    exit 1) BEFORE any destructive op.
  * Unset/unreachable TEST_DATABASE_URL -> "SKIP: ..." exit 0.

Run one test process at a time: loading torch in 2+ processes at once can hit
the 16GB commit limit (os error 1455). The bridge-clamp step runs as ONE
subprocess BEFORE this process imports any model code, so at most one torch
process is alive at any moment.

Covered fixes:
  1. doc-hash dedup: multi-chunk doc (>2000 tokens) ingested twice with
     source_uri=None -> second is duplicate; same source_uri with different
     content -> NEW row inserted, both rows kept (append-only); legacy
     find_duplicate() without document_hash still matches on uri alone.
  3. staleness penalty in final ranking: two near-identical items, one marked
     stale via parameterized UPDATE -> fresh ranks above stale, stale kept.
  4. 8k token budget: 3 big docs (~3000 tokens each) -> returned results fit
     the budget (last one truncated), at least 1 result.
  6. chunk hard cap: one giant punctuation-free sentence (>8192 tokens) ->
     every chunk <= HARD_CAP.
  4b. bridge clamps: single-shot `python -m bridge search` with hops=99,
     top_k=999, candidate_k=9999 -> ok:true with hops=5, top_k=100,
     candidate_k=200; token_budget passed through.

All SQL is parameterized.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

# This dev box has a small Windows commit limit (os error 1455 / segfaults
# when torch over-allocates with default 16 threads): force intra-op thread
# caps BEFORE any torch import. The bridge subprocess in step_1 inherits
# os.environ, so it is capped too.
os.environ["OMP_NUM_THREADS"] = "2"
os.environ["MKL_NUM_THREADS"] = "2"
os.environ["NUMEXPR_NUM_THREADS"] = "2"

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

REPO_ROOT = Path(__file__).resolve().parent.parent

import psycopg  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402

SKIP_MSG = "SKIP: no test database available (set TEST_DATABASE_URL)"

TEST_DBNAME_RE = re.compile(r"(?:^|_)test(?:_|$)", re.IGNORECASE)

URI_PREFIX = "fixf1://"


# --- guard helpers (pattern copied from tests/test_e2e.py) --------------------

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


# --- test documents ------------------------------------------------------------

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
    "Regulators have started asking questions about {t} recently.",
    "Fallbacks for {t} exist when the primary approach is unavailable.",
    "Scaling {t} beyond a single node introduces coordination overhead.",
    "Interview questions about {t} usually focus on fundamentals first.",
]


def _doc(topic: str, n_paras: int = 6) -> str:
    """~470-token paragraphs, n_paras of them -> a multi-chunk document (>2000 tokens).
    Every sentence mentions the (distinct) topic, so different docs stay well
    below the 0.95 semantic-dedup cosine threshold."""
    paras = []
    for p in range(n_paras):
        sents = [_SENT[(p * 3 + i) % len(_SENT)].format(t=topic) for i in range(24)]
        paras.append(" ".join(sents))
    return "\n\n".join(paras)


BIG_1 = _doc("vector database HNSW indexing and approximate nearest neighbor search")
BIG_2 = _doc("sourdough hydration ratios and bulk fermentation temperature control")
BIG_3 = _doc("marathon polarized training zones and weekly mileage progression")
BIG_4 = _doc("quantum error correction surface codes and syndrome decoding")
BIG_5 = _doc("mycology of oyster mushrooms on spent coffee grounds substrate")
BIG_6 = _doc("baroque counterpoint rules for writing invertible canons")

FRESH_TXT = ("Aurora borealis forecasting relies on solar wind speed and the "
             "interplanetary magnetic field orientation measured by satellites.")


# --- steps ---------------------------------------------------------------------

def step_1_bridge_clamps(ctx) -> None:
    """Single-shot bridge search with out-of-range args -> ok:true, clamped."""
    env = dict(os.environ)
    env["DATABASE_URL"] = ctx["url"]  # the bridge subprocess reads DATABASE_URL
    proc = subprocess.run(
        [sys.executable, "-m", "bridge", "search", json.dumps({
            "query": "aurora borealis forecasting",
            "hops": 99, "top_k": 999, "candidate_k": 9999, "token_budget": 500,
        })],
        cwd=REPO_ROOT, env=env, capture_output=True, text=True, timeout=900,
    )
    lines = [ln for ln in proc.stdout.splitlines() if ln.strip()]
    assert len(lines) == 1, f"expected 1 stdout line, got {proc.stdout[:400]!r}"
    envelope = json.loads(lines[0])
    assert envelope["ok"] is True, (envelope, proc.stderr[-2000:])
    assert proc.returncode == 0, (proc.returncode, envelope)
    params = envelope["result"]["params"]
    assert params["hops"] == 5, params
    assert params["top_k"] == 100, params
    assert params["candidate_k"] == 200, params
    assert params["token_budget"] == 500, params  # passed through, not clamped away
    print("PASS step_1_bridge_clamps (hops=99 -> 5, top_k=999 -> 100, "
          "candidate_k=9999 -> 200, token_budget=500 passthrough, ok:true)")


def step_2_doc_hash_dedup(ctx) -> None:
    from pipeline.chunk import estimate_tokens
    from pipeline.dedup import content_hash, find_duplicate
    from pipeline.ingest import ingest

    assert estimate_tokens(BIG_1) > 2000

    # (a) multi-chunk doc, source_uri=None, ingested twice -> second is duplicate
    r1 = ingest("text", BIG_1, source_uri=None, title="fixf1 big doc 1")
    assert r1["status"] == "inserted", r1
    assert r1["chunks"] > 1, f"expected multi-chunk ingest, got {r1['chunks']} chunks"
    r2 = ingest("text", BIG_1, source_uri=None, title="fixf1 big doc 1 again")
    assert r2["status"] == "duplicate", r2
    assert r2["node_ids"] and r2["node_ids"][0] in r1["node_ids"], r2

    # the document node carries metadata.document_hash of the FULL text
    with psycopg.connect(ctx["url"], row_factory=dict_row) as conn:
        row = conn.execute(
            "SELECT metadata->>'document_hash' AS dh FROM knowledge_items WHERE id = %s",
            (r1["node_ids"][0],),
        ).fetchone()
    assert row["dh"] == content_hash(BIG_1), (row["dh"], content_hash(BIG_1))

    # (b) same source_uri, DIFFERENT content -> NEW row inserted, both kept
    uri = URI_PREFIX + "multi"
    ra = ingest("text", BIG_2, source_uri=uri, title="fixf1 version A")
    assert ra["status"] == "inserted", ra
    rb = ingest("text", BIG_3, source_uri=uri, title="fixf1 version B")
    assert rb["status"] == "inserted", rb  # new version — NOT a duplicate
    with psycopg.connect(ctx["url"], row_factory=dict_row) as conn:
        n = conn.execute(
            "SELECT count(*) AS n FROM knowledge_items WHERE source_uri = %s", (uri,)
        ).fetchone()["n"]
        doc_hashes = conn.execute(
            "SELECT DISTINCT metadata->>'document_hash' AS dh FROM knowledge_items"
            " WHERE source_uri = %s AND metadata->>'document_hash' IS NOT NULL",
            (uri,),
        ).fetchall()
    assert n == ra["chunks"] + rb["chunks"], (n, ra, rb)  # both versions present
    assert len(doc_hashes) == 2, doc_hashes

    # re-ingesting version A under the same uri -> duplicate again
    rc = ingest("text", BIG_2, source_uri=uri, title="fixf1 version A replay")
    assert rc["status"] == "duplicate", rc

    # (c) legacy call without document_hash: uri match alone -> duplicate
    with psycopg.connect(ctx["url"], row_factory=dict_row) as conn:
        legacy = find_duplicate(BIG_3, uri, conn)
    assert legacy is not None, "legacy find_duplicate(text, uri, conn) must still match on uri"

    print("PASS step_2_doc_hash_dedup (multi-chunk doc twice w/o uri -> duplicate; "
          "same uri + different content -> NEW row, both kept; document_hash on doc node; "
          "legacy uri-only call still works)")


def step_3_staleness_ordering(ctx) -> None:
    from pipeline.ingest import ingest
    from retrieval.search import search

    fresh_uri = URI_PREFIX + "fresh"
    stale_uri = URI_PREFIX + "stale"
    r = ingest("text", FRESH_TXT, source_uri=fresh_uri, title="fixf1 fresh")
    assert r["status"] == "inserted", r

    # near-identical second item: same embedding, content + query terms appended
    # (so its rerank score would be >= the fresh one's without the penalty)
    with psycopg.connect(ctx["url"], row_factory=dict_row) as conn:
        conn.execute(
            """
            INSERT INTO knowledge_items
                (type, title, content, mime_type, source_uri, distillation_score,
                 embedding, update_period, metadata)
            SELECT type, title, content || ' aurora borealis forecasting aurora wind',
                   mime_type, %s, distillation_score, embedding, update_period, metadata
            FROM knowledge_items WHERE source_uri = %s
            """,
            (stale_uri, fresh_uri),
        )
        # mark it stale: update_period=1s, created 10 days ago (parameterized)
        conn.execute(
            "UPDATE knowledge_items SET update_period = %s,"
            " created_at = now() - make_interval(days => %s) WHERE source_uri = %s",
            (1, 10, stale_uri),
        )

    out = search("aurora borealis forecasting solar wind magnetic field",
                 top_k=20, token_budget=100000)
    results = out["results"]
    by_uri = {r.get("source_uri"): r for r in results}
    assert fresh_uri in by_uri and stale_uri in by_uri, [r.get("source_uri") for r in results]
    fresh, stale = by_uri[fresh_uri], by_uri[stale_uri]
    assert stale.get("stale") is True, stale
    assert fresh.get("stale") is False, fresh
    # the penalty must actually flip the rerank order in the final ranking
    assert stale["rerank_score"] > fresh["rerank_score"], (stale["rerank_score"], fresh["rerank_score"])
    assert abs(stale["final_score"] - stale["rerank_score"] * 0.5) < 1e-9, stale
    assert fresh["final_score"] == fresh["rerank_score"], fresh
    order = [r.get("source_uri") for r in results]
    assert order.index(fresh_uri) < order.index(stale_uri), order
    print(f"PASS step_3_staleness_ordering (rerank: stale={stale['rerank_score']:.4f} > "
          f"fresh={fresh['rerank_score']:.4f}; final: fresh ranks above stale "
          f"({fresh['final_score']:.4f} > {stale['final_score']:.4f}); stale still present)")


def step_4_token_budget(ctx) -> None:
    from pipeline.chunk import estimate_tokens
    from pipeline.ingest import ingest
    from retrieval.search import search

    for i, big in enumerate((BIG_4, BIG_5, BIG_6), start=1):
        r = ingest("text", big, source_uri=f"{URI_PREFIX}big-{i}", title=f"fixf1 big {i}")
        assert r["status"] == "inserted", r
        assert r["chunks"] >= 2, r

    out = search("detailed guide concepts methods examples", top_k=10)  # default budget 8000
    results = out["results"]
    assert len(results) >= 1, out
    assert len(results) <= 10, len(results)
    total = sum(estimate_tokens(r.get("content") or "") for r in results)
    assert total == out["estimated_tokens"], (total, out["estimated_tokens"])
    assert total <= 8000 + 100, f"token budget exceeded: {total}"
    assert out["params"]["token_budget"] == 8000, out["params"]

    out2 = search("detailed guide concepts methods examples", top_k=10, token_budget=2000)
    results2 = out2["results"]
    assert len(results2) >= 1, out2
    total2 = sum(estimate_tokens(r.get("content") or "") for r in results2)
    assert total2 <= 2000 + 100, f"small budget exceeded: {total2}"
    assert out2["params"]["token_budget"] == 2000, out2["params"]

    print(f"PASS step_4_token_budget (top_k=10, 6 candidate chunks ~1560 tok each -> "
          f"{len(results)} results, total {total} <= 8000; budget=2000 -> {len(results2)} "
          f"results, total {total2} <= 2000; always >= 1 result)")


def step_5_chunk_hard_cap(ctx) -> None:
    from pipeline.chunk import HARD_CAP, chunk_text, estimate_tokens

    # one giant punctuation-free sentence, ~11250 tokens > HARD_CAP
    text = "word " * 9000
    assert estimate_tokens(text) > HARD_CAP
    chunks = chunk_text(text)
    assert len(chunks) >= 2, len(chunks)
    for c in chunks:
        assert estimate_tokens(c) <= HARD_CAP, (estimate_tokens(c), len(c))
    # no content lost beyond hard splits: concatenation covers the original
    assert "".join(chunks).replace(" ", "") == text.replace(" ", "")
    print(f"PASS step_5_chunk_hard_cap (giant sentence {estimate_tokens(text)} tok -> "
          f"{len(chunks)} chunks, all <= HARD_CAP={HARD_CAP})")


def cleanup(ctx) -> None:
    with psycopg.connect(ctx["url"], row_factory=dict_row) as conn:
        conn.execute(
            "DELETE FROM knowledge_items WHERE source_uri LIKE %s",
            (URI_PREFIX + "%",),
        )
        remaining = conn.execute(
            "SELECT count(*) AS n FROM knowledge_items WHERE source_uri LIKE %s",
            (URI_PREFIX + "%",),
        ).fetchone()["n"]
    assert remaining == 0
    print("PASS cleanup (fixf1:// test rows deleted)")


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

    os.environ["DATABASE_URL"] = url  # pipeline/ and retrieval/ read DATABASE_URL

    apply_schema(url)
    print("PASS setup_schema (db/schema.sql applied to test DB)")

    ctx = {"url": url}
    try:
        # Subprocess FIRST, before this process imports/loads any model code:
        # at most one torch process may be alive at a time (16GB commit limit).
        step_1_bridge_clamps(ctx)

        model_ok, model_err = check_models()
        if not model_ok:
            print(f"SKIP: embedding/reranker models unavailable ({model_err})")
            sys.exit(0)

        for step in (
            step_2_doc_hash_dedup,
            step_3_staleness_ordering,
            step_4_token_budget,
            step_5_chunk_hard_cap,
        ):
            step(ctx)
    finally:
        cleanup(ctx)
    print("All fix-worker F1 tests passed.")


if __name__ == "__main__":
    main()
