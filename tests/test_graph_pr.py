"""Phase 3 graph depth tests — personalized PageRank context assembly (HippoRAG).

Covers retrieval/graph.py page_rank_expand (h-hop neighborhood + weighted
personalized PageRank over the relationships subgraph, direction-aware,
staleness-decayed) and its wiring into retrieval/search.py (graph method uses
PageRank when hops>1, keeps the v1 graph_expand path for hops=1).

Runs against a REAL PostgreSQL + pgvector database. Setup — DEDICATED
THROWAWAY database only:

    createdb graphpr_test
    export TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:5433/graphpr_test
    python tests/test_graph_pr.py

Guard pattern (identical to tests/test_e2e.py / tests/test_fixes_f1.py):
  * TEST_DATABASE_URL is the ONLY accepted variable — no DATABASE_URL
    fallback, because apply_schema() drops the knowledge tables.
  * Refuses any database whose name has no delimited "test" component
    (regex (?:^|_)test(?:_|$)) — REFUSING, exit 1 — BEFORE any connection.
  * Unset/unreachable TEST_DATABASE_URL -> "SKIP: ..." exit 0.
  * Own database (graphpr_test), all SQL parameterized, test rows planted
    under source_uri LIKE 'graphpr://%' and deleted in a finally block.

Run one test process at a time: loading torch in 2+ processes at once can hit
the 16GB commit limit (os error 1455). The thread caps below are set BEFORE
any torch import.

All SQL is parameterized.
"""

from __future__ import annotations

import os
import re
import shutil
import sys
from pathlib import Path

# Commit-limit safety on the 16GB box: cap threads BEFORE any torch import.
os.environ["OMP_NUM_THREADS"] = "2"
os.environ["MKL_NUM_THREADS"] = "2"
os.environ["NUMEXPR_NUM_THREADS"] = "2"

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

REPO_ROOT = Path(__file__).resolve().parent.parent

import psycopg  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402

SKIP_MSG = "SKIP: no test database available (set TEST_DATABASE_URL)"

TEST_DBNAME_RE = re.compile(r"(?:^|_)test(?:_|$)", re.IGNORECASE)

URI_PREFIX = "graphpr://"


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
        import subprocess
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


# --- planting helpers (all SQL parameterized) -----------------------------------

def plant_item(conn, title: str, content: str, uri: str, embedding=None,
               update_period: int = -1) -> str:
    emb = embedding.tolist() if embedding is not None else None
    row = conn.execute(
        """
        INSERT INTO knowledge_items
            (type, title, content, mime_type, source_uri, distillation_score,
             embedding, update_period, metadata)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
        RETURNING id
        """,
        ("note", title, content, "text/plain", uri, 0.5, emb, update_period, "{}"),
    ).fetchone()
    return row["id"]


def plant_edge(conn, from_id: str, to_id: str, type_: str = "relates_to",
               strength: float | None = 0.9, direction: str = "one_way",
               context: str | None = None) -> str:
    row = conn.execute(
        """
        INSERT INTO relationships (from_id, to_id, type, strength, direction, context)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (from_id, to_id, type_, strength, direction, context),
    ).fetchone()
    return row["id"]


def mark_stale(conn, node_id: str) -> None:
    # update_period=1s with created_at 10 days ago -> is_stale() is True
    conn.execute(
        "UPDATE knowledge_items SET update_period = %s,"
        " created_at = now() - make_interval(days => %s) WHERE id = %s",
        (1, 10, node_id),
    )


# --- steps ---------------------------------------------------------------------

def step_1_unit_pagerank_core() -> None:
    """Pure unit tests of the PageRank core + direction helper (no DB)."""
    from retrieval.graph import _directed_endpoints, personalized_pagerank

    # direction semantics: one_way from->to, bidirectional both, 'in' reversed
    assert _directed_endpoints("a", "b", "one_way") == (("a", "b"),)
    assert _directed_endpoints("a", "b", "bidirectional") == (("a", "b"), ("b", "a"))
    assert _directed_endpoints("a", "b", "in") == (("b", "a"),)
    assert _directed_endpoints("a", "b", None) == (("a", "b"),)

    # chain A->B->C->D: PR decreases strictly along the chain
    edges = [("A", "B", 0.9), ("B", "C", 0.9), ("C", "D", 0.9)]
    pr = personalized_pagerank(["A", "B", "C", "D"], edges, ["A"])
    assert pr["B"] > pr["C"] > pr["D"] > 0, pr

    # staleness decay: stale S gets lower PR than fresh equivalent F
    edges2 = [("A", "F", 0.9), ("A", "S", 0.9), ("F", "D1", 0.9), ("S", "D2", 0.9)]
    pr2 = personalized_pagerank(
        ["A", "F", "S", "D1", "D2"], edges2, ["A"], node_factor={"S": 0.5},
    )
    assert pr2["S"] < pr2["F"], pr2
    assert pr2["D2"] < pr2["D1"], pr2

    # NULL-strength edges are the caller's job (DEFAULT_STRENGTH); weight<=0 dropped
    pr3 = personalized_pagerank(["A", "B"], [("A", "B", 0.0)], ["A"])
    assert pr3 == {"A": 1.0, "B": 0.0} or pr3["B"] == 0.0, pr3
    print("PASS step_1_unit_pagerank_core (direction helper + chain monotonic + stale decay)")


def step_2_chain_pr_db(ctx) -> dict:
    """Plant chain A->B->C->D (one_way) and assert page_rank_expand ranks it."""
    from retrieval.graph import page_rank_expand

    with psycopg.connect(ctx["url"], row_factory=dict_row) as conn:
        a = plant_item(conn, "pr A", "graphpr alpha node about mycelium networks",
                       URI_PREFIX + "chain/a")
        b = plant_item(conn, "pr B", "graphpr bravo node about fungal decomposition",
                       URI_PREFIX + "chain/b")
        c = plant_item(conn, "pr C", "graphpr charlie node about forest soil ecology",
                       URI_PREFIX + "chain/c")
        d = plant_item(conn, "pr D", "graphpr delta node about carbon storage in soil",
                       URI_PREFIX + "chain/d")
        plant_edge(conn, a, b, strength=0.9)
        plant_edge(conn, b, c, strength=0.9)
        plant_edge(conn, c, d, strength=0.9)

        res3 = page_rank_expand(conn, [a], hops=3, top_k=10)
        ids3 = [r["id"] for r in res3]
        assert set(ids3) == {b, c, d}, ids3  # seeds excluded, whole chain reached
        by_id = {r["id"]: r for r in res3}
        assert by_id[b]["score"] > by_id[c]["score"] > by_id[d]["score"] > 0, \
            {k: v["score"] for k, v in by_id.items()}
        # merge-facing fields are present
        for r in res3:
            for f in ("id", "type", "title", "content", "mime_type", "source_uri",
                      "distillation_score", "created_at", "update_period",
                      "metadata", "score", "via_relationship"):
                assert f in r, (f, r)
        assert by_id[d]["via_relationship"]["hop"] == 3, by_id[d]["via_relationship"]
        # hops=2 must NOT reach D (neighborhood is really h-hop)
        res2 = page_rank_expand(conn, [a], hops=2, top_k=10)
        assert d not in [r["id"] for r in res2], res2
        # sorted by score desc
        scores = [r["score"] for r in res3]
        assert scores == sorted(scores, reverse=True), scores
        ctx["chain"] = {"a": a, "b": b, "c": c, "d": d}
    print("PASS step_2_chain_pr_db (A->B->C->D: D reached at hops=3, not at hops=2, "
          "pr scores strictly decreasing along the chain, merge fields present)")
    return ctx


def step_3_direction_one_way(ctx) -> None:
    """one_way A->B must not be traversed B->A from seed B."""
    from retrieval.graph import page_rank_expand

    ch = ctx["chain"]
    with psycopg.connect(ctx["url"], row_factory=dict_row) as conn:
        res_from_b = page_rank_expand(conn, [ch["b"]], hops=3, top_k=10)
        ids = [r["id"] for r in res_from_b]
        assert ch["a"] not in ids, ids  # cannot go backwards over one_way
        assert set(ids) == {ch["c"], ch["d"]}, ids
        res_from_a = page_rank_expand(conn, [ch["a"]], hops=1, top_k=10)
        assert [r["id"] for r in res_from_a] == [ch["b"]], res_from_a
    print("PASS step_3_direction_one_way (seed B does not reach A over one_way A->B; "
          "seed A reaches B)")


def step_4_bidirectional(ctx) -> None:
    """bidirectional edge is traversable both ways."""
    from retrieval.graph import page_rank_expand

    with psycopg.connect(ctx["url"], row_factory=dict_row) as conn:
        x = plant_item(conn, "pr X", "graphpr xray node about tidal locking",
                       URI_PREFIX + "bi/x")
        y = plant_item(conn, "pr Y", "graphpr yankee node about moon synchronous rotation",
                       URI_PREFIX + "bi/y")
        plant_edge(conn, x, y, strength=0.8, direction="bidirectional")
        from_x = page_rank_expand(conn, [x], hops=1, top_k=10)
        from_y = page_rank_expand(conn, [y], hops=1, top_k=10)
        assert [r["id"] for r in from_x] == [y], from_x
        assert [r["id"] for r in from_y] == [x], from_y
    print("PASS step_4_bidirectional (X<->Y reached from both seeds)")


def step_5_stale_lower_pr(ctx) -> None:
    """A stale mid-chain node gets a lower PR score than a fresh equivalent."""
    from retrieval.graph import page_rank_expand
    from retrieval.merge import is_stale

    with psycopg.connect(ctx["url"], row_factory=dict_row) as conn:
        a = plant_item(conn, "pr root", "graphpr root node about composting systems",
                       URI_PREFIX + "stale/root")
        f = plant_item(conn, "pr fresh", "graphpr fresh branch about thermophilic compost",
                       URI_PREFIX + "stale/fresh")
        s = plant_item(conn, "pr stale", "graphpr stale branch about outdated compost claims",
                       URI_PREFIX + "stale/stale")
        d1 = plant_item(conn, "pr fresh leaf", "graphpr leaf under fresh branch",
                        URI_PREFIX + "stale/d1")
        d2 = plant_item(conn, "pr stale leaf", "graphpr leaf under stale branch",
                        URI_PREFIX + "stale/d2")
        plant_edge(conn, a, f, strength=0.9)
        plant_edge(conn, a, s, strength=0.9)
        plant_edge(conn, f, d1, strength=0.9)
        plant_edge(conn, s, d2, strength=0.9)
        mark_stale(conn, s)

        row = conn.execute(
            "SELECT id, update_period, created_at FROM knowledge_items WHERE id = %s",
            (s,),
        ).fetchone()
        assert is_stale(dict(row)) is True

        res = page_rank_expand(conn, [a], hops=2, top_k=10)
        by_id = {r["id"]: r["score"] for r in res}
        assert set(by_id) == {f, s, d1, d2}, by_id
        assert by_id[s] < by_id[f], by_id  # stale node ranks below fresh equivalent
        assert by_id[d2] < by_id[d1], by_id  # and so does everything downstream of it
    print(f"PASS step_5_stale_lower_pr (stale S pr={by_id[s]:.6f} < fresh F pr={by_id[f]:.6f}; "
          f"downstream D2={by_id[d2]:.6f} < D1={by_id[d1]:.6f})")


def step_6_search_graph_hops3(ctx) -> None:
    """search(methods=['graph'], hops=3) surfaces the deep node; hops=1 does not."""
    if not ctx["model_ok"]:
        print(f"SKIP step_6_search_graph_hops3 (model unavailable: {ctx['model_err']})")
        return
    from pipeline.embed import embed_text
    from retrieval.search import search

    with psycopg.connect(ctx["url"], row_factory=dict_row) as conn:
        a = plant_item(conn, "search A",
                       "graphpr anchor note about pagrank context assembly over knowledge graphs",
                       URI_PREFIX + "search/a",
                       embedding=embed_text(
                           "graphpr anchor note about pagrank context assembly over knowledge graphs"))
        b = plant_item(conn, "search B", "graphpr hop one note about hipporag retrieval",
                       URI_PREFIX + "search/b")
        c = plant_item(conn, "search C", "graphpr hop two note about personalized pagerank seeds",
                       URI_PREFIX + "search/c")
        d = plant_item(conn, "search D", "graphpr deep note about multi hop associative recall",
                       URI_PREFIX + "search/d")
        plant_edge(conn, a, b, strength=0.9)
        plant_edge(conn, b, c, strength=0.9)
        plant_edge(conn, c, d, strength=0.9)

    query = "pagrank context assembly knowledge graph anchor"
    out3 = search(query, methods=["graph"], hops=3, top_k=10, candidate_k=20)
    assert out3["params"]["graph_ranking"] == "pagerank", out3["params"]
    assert out3["params"]["hops"] == 3, out3["params"]
    graph_ids3 = [r["id"] for r in out3["by_method"]["graph"]]
    assert d in graph_ids3, graph_ids3
    result_ids3 = [r["id"] for r in out3["results"]]
    assert d in result_ids3, result_ids3  # deep node surfaces end-to-end
    # downstream behavior unchanged: merged results carry found_by + staleness flag
    for r in out3["results"]:
        assert "found_by" in r and "stale" in r and "rerank_score" in r, r

    out1 = search(query, methods=["graph"], hops=1, top_k=10, candidate_k=20)
    assert out1["params"]["graph_ranking"] == "expand", out1["params"]  # hops=1 = v1 path
    result_ids1 = [r["id"] for r in out1["results"]]
    assert b in result_ids1 and d not in result_ids1, result_ids1
    print("PASS step_6_search_graph_hops3 (methods=['graph'] hops=3 -> ranking=pagerank, "
          "deep node D in by_method['graph'] and in final results; hops=1 -> ranking=expand, "
          "only B; merged results still carry found_by/stale/rerank_score)")


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
    print("PASS cleanup (graphpr:// test rows deleted)")


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

    model_ok, model_err = check_models()
    if not model_ok:
        print(f"SKIP: embedding/reranker models unavailable ({model_err}) — "
              "the search() step will be skipped")

    ctx = {"url": url, "model_ok": model_ok, "model_err": model_err}
    try:
        step_1_unit_pagerank_core()
        step_2_chain_pr_db(ctx)
        step_3_direction_one_way(ctx)
        step_4_bidirectional(ctx)
        step_5_stale_lower_pr(ctx)
        step_6_search_graph_hops3(ctx)
    finally:
        cleanup(ctx)
    print("All graph PageRank tests passed.")


if __name__ == "__main__":
    main()
