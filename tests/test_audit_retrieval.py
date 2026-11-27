"""Audit-fix tests for retrieval/ (hybrid search, rerank, merge, fts, graph).

Covers the six external-audit findings in retrieval/:

  (a) rerank scores are sigmoid-normalized into (0, 1) with the ranking order
      identical to the raw-logit order (monkeypatched _get_model with known
      logits, including negatives); the rerank pair document includes the
      title and the graph edge context, not just the content.
  (b) a stale item with a negative raw logit ranks BELOW a fresh item once the
      staleness penalty is applied (search() with a monkeypatched rerank).
  (c) an item with metadata.review_status = "invalidated-pending-review" gets
      a x0.5 penalty and is still returned (never filtered) — both via
      search() and via merge_results directly.
  (d) search(methods=["fts"]) never calls embed_text (monkeypatched to raise).
  (e) FTS uses websearch_to_tsquery: a query with one out-of-corpus word joined
      by "or" still returns the in-corpus hits (OR semantics).
  (f) graph_expand: a strength = 0.0 edge scores exactly 0.0 (not the 0.5
      NULL default); a 4-chain at hops=3 reaches hop 3 (multi-hop intact); a
      hop query (visited set — no re-expansion).

DB guard (same pattern as tests/test_e2e.py): TEST_DATABASE_URL is the ONLY
accepted variable; the script refuses (exit 1) any database whose name lacks
a delimited "test" component BEFORE connecting, prints SKIP and exits 0 when
unset/unreachable, and never touches the `postgres` database. All SQL is
parameterized. Run from the project root:

    export TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:5433/audit_w1_test
    python tests/test_audit_retrieval.py
"""

import importlib
import math
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

REPO_ROOT = Path(__file__).resolve().parent.parent

import numpy as np  # noqa: E402
import psycopg  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402
from psycopg.types.json import Jsonb  # noqa: E402

SKIP_MSG = "SKIP: no test database available (set TEST_DATABASE_URL)"

TEST_DBNAME_RE = re.compile(r"(?:^|_)test(?:_|$)", re.IGNORECASE)


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


# --- helpers -----------------------------------------------------------------

def sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


def seed_item(conn, *, title, content, update_period=-1, created_at=None, metadata=None,
              source_uri=None):
    if created_at is None:
        from datetime import datetime, timezone
        created_at = datetime.now(timezone.utc)
    row = conn.execute(
        """
        INSERT INTO knowledge_items
            (type, title, content, mime_type, source_uri, update_period, created_at, metadata)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        ("note", title, content, "text/plain",
         source_uri or f"auditw1://{abs(hash(title)) % 10**8}",
         update_period, created_at, Jsonb(metadata or {})),
    ).fetchone()
    return row["id"]


def seed_edge(conn, from_id, to_id, *, strength=None, direction="one_way", context=None,
              rtype="associative"):
    conn.execute(
        """
        INSERT INTO relationships (from_id, to_id, type, strength, direction, context)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (from_id, to_id, type) DO NOTHING
        """,
        (from_id, to_id, rtype, strength, direction, context),
    )


class CountingConn:
    """Delegating wrapper that counts execute() calls (hop-query counting)."""

    def __init__(self, conn):
        self._conn = conn
        self.n = 0

    def execute(self, *args, **kwargs):
        self.n += 1
        return self._conn.execute(*args, **kwargs)


# --- tests -------------------------------------------------------------------

def test_a_rerank_sigmoid_order_and_pair() -> None:
    """(a) rerank_score in (0,1), order == raw-logit order, pair has title+context."""
    rerank_mod = importlib.import_module("retrieval.rerank")

    logits = [2.5, -3.0, 0.0, 7.0, -0.1]
    candidates = [
        {"id": i, "title": f"Title{i}", "content": f"content body {i} " * 5,
         "via_relationship": {"context": f"edge context {i}", "type": "associative"}}
        for i in range(len(logits))
    ]

    class FakeCrossEncoder:
        def __init__(self):
            self.pairs = None

        def predict(self, pairs):
            self.pairs = pairs
            return np.array(logits, dtype=np.float64)

    fake = FakeCrossEncoder()
    orig_get_model = rerank_mod._get_model
    rerank_mod._get_model = lambda: fake
    try:
        ranked = rerank_mod.rerank("some query", [dict(c) for c in candidates])
    finally:
        rerank_mod._get_model = orig_get_model

    # scores in (0, 1) and equal to the sigmoid of the raw logits
    assert len(ranked) == len(logits)
    for r, logit in zip(ranked, sorted(logits, reverse=True)):
        assert 0.0 < r["rerank_score"] < 1.0, r
        assert abs(r["rerank_score"] - sigmoid(logit)) < 1e-9, (r, logit)
    # order preserved: descending raw logits (sigmoid is monotonic)
    assert [r["id"] for r in ranked] == [3, 0, 2, 4, 1], [r["id"] for r in ranked]
    # pair document = title + via context + content
    pair = fake.pairs[0]
    assert pair[0] == "some query"
    assert pair[1].startswith("Title0\nedge context 0\ncontent body 0"), pair[1][:80]
    # top_k still works
    rerank_mod._get_model = lambda: fake
    try:
        top2 = rerank_mod.rerank("q", [dict(c) for c in candidates], top_k=2)
    finally:
        rerank_mod._get_model = orig_get_model
    assert [r["id"] for r in top2] == [3, 0]
    print("PASS (a) rerank sigmoid-normalized (0,1), order preserved, pair has title+context")


def _patch_search(search_mod, *, rerank_fn=None, embed_fn=None):
    """Monkeypatch retrieval.search.rerank / embed_text; returns restore()."""
    orig = (search_mod.rerank, search_mod.embed_text)
    if rerank_fn is not None:
        search_mod.rerank = rerank_fn
    if embed_fn is not None:
        search_mod.embed_text = embed_fn

    def restore():
        search_mod.rerank, search_mod.embed_text = orig
    return restore


def test_b_stale_negative_logit_ranks_below_fresh(url: str) -> None:
    """(b) stale item (negative raw logit) ranks below fresh item after penalty."""
    search_mod = importlib.import_module("retrieval.search")
    with psycopg.connect(url, row_factory=dict_row) as conn:
        from datetime import datetime, timedelta, timezone
        stale_id = seed_item(
            conn, title="Stale photosynthesis note",
            content="Photosynthesis converts sunlight into chemical energy in old textbooks.",
            update_period=1, created_at=datetime.now(timezone.utc) - timedelta(hours=2),
            source_uri="auditw1://stale",
        )
        fresh_id = seed_item(
            conn, title="Fresh photosynthesis note",
            content="Photosynthesis converts sunlight into chemical energy in chloroplasts.",
            update_period=-1, source_uri="auditw1://fresh",
        )
        conn.commit()

    # raw logits: stale -0.1 (higher), fresh -0.5 (lower) — both negative
    raw = {stale_id: -0.1, fresh_id: -0.5}

    def fake_rerank(query, candidates, text_field="content", top_k=None):
        for c in candidates:
            c["rerank_score"] = sigmoid(raw[c["id"]])
        return sorted(candidates, key=lambda x: x["rerank_score"], reverse=True)

    restore = _patch_search(search_mod, rerank_fn=fake_rerank)
    try:
        out = search_mod.search("photosynthesis", methods=["fts"], top_k=10)
    finally:
        restore()

    results = out["results"]
    by_id = {r["id"]: r for r in results}
    assert stale_id in by_id and fresh_id in by_id, "stale item must NOT be filtered out"
    stale, fresh = by_id[stale_id], by_id[fresh_id]
    assert stale["stale"] is True and fresh["stale"] is False
    assert abs(stale["rerank_score"] - sigmoid(-0.1)) < 1e-9
    assert abs(stale["final_score"] - sigmoid(-0.1) * 0.5) < 1e-12, stale["final_score"]
    assert abs(fresh["final_score"] - sigmoid(-0.5)) < 1e-12, fresh["final_score"]
    # the penalty flips the order: stale (higher raw logit) now ranks BELOW fresh
    assert results[0]["id"] == fresh_id, [r["title"] for r in results]
    assert stale["final_score"] < fresh["final_score"]
    print(f"PASS (b) stale negative-logit item ranks below fresh after x0.5 "
          f"(stale final={stale['final_score']:.4f} < fresh final={fresh['final_score']:.4f}, "
          f"stale still returned)")


def test_c_invalidated_review_status_penalty(url: str) -> None:
    """(c) review_status 'invalidated-pending-review' -> x0.5, still returned."""
    search_mod = importlib.import_module("retrieval.search")
    merge_mod = importlib.import_module("retrieval.merge")
    with psycopg.connect(url, row_factory=dict_row) as conn:
        inv_id = seed_item(
            conn, title="Invalidated photosynthesis claim",
            content="Photosynthesis happens in mitochondria (invalidated claim).",
            update_period=-1, source_uri="auditw1://invalidated",
            metadata={"review_status": "invalidated-pending-review"},
        )
        ok_id = seed_item(
            conn, title="OK photosynthesis note",
            content="Photosynthesis happens in chloroplasts.",
            update_period=-1, source_uri="auditw1://ok",
        )
        conn.commit()

    def fake_rerank(query, candidates, text_field="content", top_k=None):
        for c in candidates:
            # invalidated item has the HIGHEST rerank score (0.9 vs 0.5 for ok)
            if c["id"] == inv_id:
                c["rerank_score"] = 0.9
            elif c["id"] == ok_id:
                c["rerank_score"] = 0.5
            else:
                c["rerank_score"] = 0.1  # unrelated leftovers from earlier steps
        return sorted(candidates, key=lambda x: x["rerank_score"], reverse=True)

    restore = _patch_search(search_mod, rerank_fn=fake_rerank)
    try:
        out = search_mod.search("photosynthesis", methods=["fts"], top_k=10)
    finally:
        restore()

    by_id = {r["id"]: r for r in out["results"]}
    assert inv_id in by_id, "invalidated item must NOT be filtered out"
    inv, ok = by_id[inv_id], by_id[ok_id]
    assert inv["review_status"] == "invalidated-pending-review", inv.get("review_status")
    assert ok["review_status"] == ""
    assert abs(inv["final_score"] - 0.9 * 0.5) < 1e-12, inv["final_score"]
    assert abs(ok["final_score"] - 0.5) < 1e-12
    # penalty applied: invalidated (0.45) now below the OK item (0.5) despite
    # the higher rerank score
    assert out["results"][0]["id"] == ok_id

    # unit-level: merge_results applies REVIEW_STATUS_PENALTY, sets review_status,
    # never filters
    merged = merge_mod.merge_results({
        "vector": [
            {"id": "x", "content": "a", "score": 0.8,
             "metadata": {"review_status": "invalidated-pending-review"}},
            {"id": "y", "content": "b", "score": 0.6, "metadata": {}},
        ],
    })
    assert len(merged) == 2, "merge must never filter entries out"
    by = {e["id"]: e for e in merged}
    assert abs(by["x"]["score"] - 0.8 * 0.5) < 1e-12, by["x"]["score"]
    assert by["x"]["review_status"] == "invalidated-pending-review"
    assert abs(by["y"]["score"] - 0.6) < 1e-12
    assert by["y"]["review_status"] == ""
    print("PASS (c) invalidated-pending-review gets x0.5 (final and merged score), "
          "review_status set, still returned")


def test_d_fts_only_never_embeds(url: str) -> None:
    """(d) search(methods=['fts']) never calls embed_text."""
    search_mod = importlib.import_module("retrieval.search")

    def boom(text):
        raise AssertionError("embed_text must not be called for methods=['fts']")

    def fake_rerank(query, candidates, text_field="content", top_k=None):
        for c in candidates:
            c["rerank_score"] = 0.5
        return list(candidates)

    restore = _patch_search(search_mod, rerank_fn=fake_rerank, embed_fn=boom)
    try:
        out = search_mod.search("photosynthesis", methods=["fts"], top_k=5)
    finally:
        restore()
    assert out["results"], "fts-only search must still return results"
    print(f"PASS (d) search(methods=['fts']) returned {len(out['results'])} results "
          "without calling embed_text")


def test_e_fts_or_semantics(url: str) -> None:
    """(e) websearch_to_tsquery: one out-of-corpus word joined by 'or' still hits."""
    fts_mod = importlib.import_module("retrieval.fts")
    with psycopg.connect(url, row_factory=dict_row) as conn:
        # one in-corpus word (photosynthesis), one out-of-corpus word (zebra)
        q = "zebra OR photosynthesis"
        tsq = conn.execute(
            "SELECT websearch_to_tsquery('english', %s)::text AS q", (q,)
        ).fetchone()["q"]
        assert "|" in tsq, f"expected OR in tsquery, got {tsq!r}"
        # the old plainto_tsquery would AND everything and match nothing here
        plainto_hits = conn.execute(
            """
            SELECT count(*) AS n FROM knowledge_items
            WHERE to_tsvector('english', content || ' ' || coalesce(title, ''))
                  @@ plainto_tsquery('english', %s)
            """,
            (q,),
        ).fetchone()["n"]
        assert plainto_hits == 0, "precondition: plainto would return 0 hits for this query"

        from retrieval.db import get_connection
        with get_connection() as c2:
            hits = fts_mod.fts_search(c2, q, top_k=10)
        assert hits, "websearch OR query must return the in-corpus hits"
        assert any("photosynth" in (h["content"] or "").lower() for h in hits)
    print(f"PASS (e) fts OR-semantics: {q!r} -> tsquery {tsq!r}, "
          f"{len(hits)} in-corpus hits (plainto would give 0)")


def test_f_graph_expand_zero_strength_and_cycle(url: str) -> None:
    """(f) strength=0.0 edge scores 0.0; 2-node cycle with hops=3 terminates fast."""
    graph_mod = importlib.import_module("retrieval.graph")
    with psycopg.connect(url, row_factory=dict_row) as conn:
        # zero-strength edge
        s0 = seed_item(conn, title="Zero-strength source",
                       content="zero strength probe source", source_uri="auditw1://z-src")
        z = seed_item(conn, title="Zero-strength target",
                      content="zero strength probe target", source_uri="auditw1://z-dst")
        seed_edge(conn, s0, z, strength=0.0, context="zero edge")
        # 2-node cycle A <-> B (two one_way edges)
        a = seed_item(conn, title="Cycle A", content="cycle node A", source_uri="auditw1://cyc-a")
        b = seed_item(conn, title="Cycle B", content="cycle node B", source_uri="auditw1://cyc-b")
        seed_edge(conn, a, b, strength=0.8)
        seed_edge(conn, b, a, strength=0.8)
        conn.commit()

        with psycopg.connect(url, row_factory=dict_row) as c2:
            res = graph_mod.graph_expand(c2, [s0], hops=1, top_k=10)
            assert len(res) == 1 and res[0]["id"] == z
            assert res[0]["score"] == 0.0, f"strength 0.0 must score 0.0, got {res[0]['score']}"
            assert res[0]["via_relationship"]["strength"] == 0.0

            wrapped = CountingConn(c2)
            t0 = time.perf_counter()
            res = graph_mod.graph_expand(wrapped, [a], hops=3, top_k=10)
            elapsed = time.perf_counter() - t0
            assert len(res) == 1 and res[0]["id"] == b, res
            assert abs(res[0]["score"] - 0.8) < 1e-9, res[0]["score"]
            # visited set: hop 2 re-queries from B, finds only the seed A, stops
            # -> exactly 2 hop queries for a 2-node cycle at hops=3
            assert wrapped.n == 2, f"expected 2 hop queries with visited set, got {wrapped.n}"
            assert elapsed < 5.0, f"2-node cycle hops=3 took {elapsed:.2f}s"

            # multi-hop regression: chain C1 -> C2 -> C3 -> C4, hops=3 must
            # reach C2 (hop 1), C3 (hop 2), C4 (hop 3)
            n1 = seed_item(conn, title="Chain 1", content="chain node 1", source_uri="auditw1://ch-1")
            n2 = seed_item(conn, title="Chain 2", content="chain node 2", source_uri="auditw1://ch-2")
            n3 = seed_item(conn, title="Chain 3", content="chain node 3", source_uri="auditw1://ch-3")
            n4 = seed_item(conn, title="Chain 4", content="chain node 4", source_uri="auditw1://ch-4")
            seed_edge(conn, n1, n2, strength=0.9)
            seed_edge(conn, n2, n3, strength=0.9)
            seed_edge(conn, n3, n4, strength=0.9)
            conn.commit()
            wrapped2 = CountingConn(c2)
            res = graph_mod.graph_expand(wrapped2, [n1], hops=3, top_k=10)
            got = {r["id"] for r in res}
            assert got == {n2, n3, n4}, f"hops=3 must reach all 3 chain nodes, got {got}"
            assert wrapped2.n == 3, f"expected 3 hop queries on a 4-chain at hops=3, got {wrapped2.n}"
    print(f"PASS (f) strength=0.0 edge scores 0.0 (not 0.5); 2-node cycle hops=3 "
          f"terminated in {elapsed:.3f}s with {wrapped.n} hop query")


# --- main --------------------------------------------------------------------

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

    # retrieval/ reads DATABASE_URL — point it at the test DB
    os.environ["DATABASE_URL"] = url

    apply_schema(url)
    print("PASS setup_schema (db/schema.sql applied to test DB)")

    try:
        test_a_rerank_sigmoid_order_and_pair()
        test_b_stale_negative_logit_ranks_below_fresh(url)
        test_c_invalidated_review_status_penalty(url)
        test_d_fts_only_never_embeds(url)
        test_e_fts_or_semantics(url)
        test_f_graph_expand_zero_strength_and_cycle(url)
    finally:
        with psycopg.connect(url) as conn:
            conn.execute("DELETE FROM knowledge_items WHERE source_uri LIKE %s",
                         ("auditw1://%",))
        print("PASS cleanup (auditw1:// rows removed from test DB)")
    print("All audit-retrieval tests passed.")


if __name__ == "__main__":
    main()
