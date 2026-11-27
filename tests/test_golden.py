"""Golden test set — recall@5 of the hybrid retrieval over a synthetic seed corpus.

Phase 2 (ROADMAP.md): 30 questions over a 30-document golden corpus spanning
media types as they appear after ingestion, recall@5 >= 80% (>= 24/30).

Phase 3 batch 2 (ROADMAP.md Phase 3 multi-hop acceptance): 6 more seed docs
(docMH1..docMH6, source_uri golden://mh-1..mh-6) forming 2 planted chains
(mh-1 -> mh-2 -> mh-3 and mh-4 -> mh-5 -> mh-6), plus 6 multi-hop questions
(36 docs / 36 questions total). The chain relationships are NOT seeded as SQL:
the test CREATES them after seeding via the bridge `link_items` command
(parameterized, deterministic type/strength/direction, declared per seed doc
in the optional "links" field). Each multi-hop question asks about the END of
a chain using only the START doc's vocabulary, so vector-only search misses
the end doc; those questions.json entries carry {"min_hops": 2,
"require_graph": true}. For every require_graph question the test ALSO runs
search(methods=["graph"], hops=min_hops) and ASSERTS the expected end doc is
present (the graph leg), and reports the vector-only top-5 hit/miss
separately (informational, never asserted to fail). Per-question PASS/FAIL is
printed with the mode ([hybrid] / [graph hops=N] / [vector-only]).
Thresholds: overall recall@5 >= 0.8 over ALL 36 questions AND every
require_graph question found via its graph leg.

Corpus + questions live in tests/golden/:

    seed_docs.json   — 36 synthetic documents, each on a DISTINCT topic with a
                       unique marker term (e.g. "aurora-7"). Media variety is
                       expressed through the source_uri scheme, exactly as the
                       corresponding extractors record provenance after ingestion:
                       text notes golden://..., a paper pdf://...,
                       an audio transcript audio://..., a YouTube transcript
                       youtube://..., a GitHub file github://...
                       The 6 multi-hop docs (golden://mh-N) additionally carry an
                       optional "links" list declaring the planted chain edges.
    questions.json   — 36 natural-language questions, one per seed doc, each
                       pointing at exactly one seed doc via expected_source_uri;
                       multi-hop entries add "min_hops" + "require_graph"

Setup — use a DEDICATED THROWAWAY database, never a production one:

    psql "postgresql://postgres@127.0.0.1:5433/postgres" \
        -c "CREATE DATABASE golden_test"

    TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:5433/golden_test \
        OMP_NUM_THREADS=2 python tests/test_golden.py

TEST_DATABASE_URL is the ONLY accepted variable — there is deliberately no
DATABASE_URL fallback, because apply_schema() drops tables and must never be
pointed at a real database. As a second guard, the script refuses to run
against any database whose name does not match (^|_)test(_|$) (case-insensitive). If TEST_DATABASE_URL
is unset or unreachable, it prints
"SKIP: no test database available (set TEST_DATABASE_URL)" and exits 0, so it
never breaks environments without a DB.

The script applies db/schema.sql itself, idempotently: before applying it drops
and recreates the two tables (knowledge_items, relationships) in the TEST
database only — no other database is touched. Schema is applied with
`psql -f` when psql is available, otherwise by executing the file contents
via psycopg.

Seeding, CHAIN PLANTING AND querying all go through ONE persistent
`python -m bridge serve` process (same approach as
tests/test_mcp_acceptance.py, with OMP_NUM_THREADS=2 in the child env): the
heavy torch/model load is paid once in a single child, never in this test
process, so two torch processes never coexist (the 16GB commit limit /
os error 1455). Every question is run through the bridge `search` command
with top_k=5; a hit means the expected source_uri (or its title) appears in
the top 5. The 6 multi-hop seed docs' "links" are planted through the same
child via `link_items` right after seeding.

If the embedding/reranker models (bge-m3, bge-reranker-v2-m3) are not in the
HuggingFace cache, the corpus is seeded with direct parameterized SQL
(embedding NULL) and the questions are run fts-only via retrieval.fts; recall@5
is then REPORTED but not asserted.

Cleanup at the end deletes exactly the seeded source_uris (parameterized
`source_uri = ANY(%s)`). All SQL is parameterized.
"""

import json
import os
import queue
import re
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

REPO_ROOT = Path(__file__).resolve().parent.parent
GOLDEN_DIR = REPO_ROOT / "tests" / "golden"

import psycopg  # noqa: E402
from psycopg import conninfo, sql  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402

SKIP_MSG = "SKIP: no test database available (set TEST_DATABASE_URL)"
TOP_K = 5
RECALL_THRESHOLD = 0.8
# Media schemes the corpus spans (as recorded in source_uri after ingestion).
ALLOWED_PREFIXES = ("golden://", "pdf://", "audio://", "youtube://", "github://")
CALL_TIMEOUT_S = 600.0


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
        return conninfo.conninfo_to_dict(url).get("dbname", "") or ""
    except Exception:
        m = re.search(r"/([^/?]+)", url)
        return m.group(1) if m else ""


# Locked guard: a test dbname matches (^|_)test(_|$) case-insensitively —
# stricter than a substring check, so e.g. "latest" is refused.
TEST_DBNAME_RE = re.compile(r"(?:^|_)test(?:_|$)", re.IGNORECASE)


def is_test_dbname(name: str) -> bool:
    return bool(TEST_DBNAME_RE.search(name or ""))


def ensure_database(url: str) -> bool:
    """Create the target DB if missing. Returns False if the server is unreachable."""
    info = conninfo.conninfo_to_dict(url)
    name = info.get("dbname") or ""
    if not name:
        return False
    for maint in ("postgres", "template1"):
        m = dict(info)
        m["dbname"] = maint
        try:
            conn = psycopg.connect(conninfo.make_conninfo(**{k: v for k, v in m.items() if v}),
                                   connect_timeout=5, autocommit=True)
        except Exception:
            continue
        try:
            exists = conn.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,)).fetchone()
            if not exists:
                conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
                print(f"created test database {name!r}")
            return True
        finally:
            conn.close()
    return False


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
    """Check the bge models are in the HF cache WITHOUT loading them in-process.

    Loading torch + both models inside this test process on top of the spawned
    bridge serve child can exhaust the Windows commit limit (os error 1455),
    so the cache is inspected instead (same approach as test_mcp_acceptance).
    """
    cache = Path.home() / ".cache" / "huggingface" / "hub"
    have_embed = (cache / "models--BAAI--bge-m3").exists()
    have_rerank = (cache / "models--BAAI--bge-reranker-v2-m3").exists()
    if have_embed and have_rerank:
        return True, ""
    missing = []
    if not have_embed:
        missing.append("bge-m3")
    if not have_rerank:
        missing.append("bge-reranker-v2-m3")
    return False, f"not in HF cache: {', '.join(missing)}"


def load_seed_docs() -> list[dict]:
    docs = json.loads((GOLDEN_DIR / "seed_docs.json").read_text(encoding="utf-8"))
    assert isinstance(docs, list) and docs, "seed_docs.json must be a non-empty list"
    uris, titles, markers, ids = set(), set(), set(), set()
    for d in docs:
        assert {"id", "title", "source_uri", "text", "marker"} <= set(d), d
        assert d["source_uri"].startswith(ALLOWED_PREFIXES), d["source_uri"]
        assert d["marker"] and d["marker"] in d["text"].lower(), d["id"]
        assert d["source_uri"] not in uris and d["title"] not in titles and d["marker"] not in markers, d
        assert d["id"] not in ids, d["id"]
        uris.add(d["source_uri"])
        titles.add(d["title"])
        markers.add(d["marker"])
        ids.add(d["id"])
    # optional planted-chain edges ("links"), resolved by doc id
    n_links = 0
    for d in docs:
        for link in d.get("links", []):
            assert {"to", "type", "strength", "direction"} <= set(link), link
            assert link["to"] in ids and link["to"] != d["id"], link
            assert isinstance(link["type"], str) and link["type"], link
            assert 0.0 <= float(link["strength"]) <= 1.0, link
            assert link["direction"] in ("out", "in", "both"), link
            n_links += 1
    assert n_links > 0, "seed_docs.json must declare at least one planted chain link"
    return docs


def load_questions(doc_uris: set[str]) -> list[dict]:
    questions = json.loads((GOLDEN_DIR / "questions.json").read_text(encoding="utf-8"))
    assert isinstance(questions, list) and questions, "questions.json must be a non-empty list"
    n_graph = 0
    for q in questions:
        assert {"query", "expected_source_uri"} <= set(q), q
        assert q["expected_source_uri"] in doc_uris, q
        if q.get("require_graph"):
            assert isinstance(q.get("min_hops"), int) and q["min_hops"] >= 2, q
            n_graph += 1
        elif "min_hops" in q:
            assert isinstance(q["min_hops"], int) and q["min_hops"] >= 1, q
    assert n_graph > 0, "questions.json must contain at least one require_graph question"
    return questions


# --- persistent bridge serve client (one process pays the model load once) ---


class BridgeServe:
    """Minimal client for `python -m bridge serve` (one JSON request per line)."""

    def __init__(self, url: str):
        env = dict(os.environ)
        env["DATABASE_URL"] = url
        env["OMP_NUM_THREADS"] = "2"
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "bridge", "serve"],
            cwd=str(REPO_ROOT), env=env,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL, text=True, bufsize=1,
        )
        self._next_id = 1
        self._lines: queue.Queue = queue.Queue()
        self._reader = threading.Thread(target=self._read_loop, daemon=True)
        self._reader.start()

    def _read_loop(self):
        for line in self.proc.stdout:
            self._lines.put(line)
        self._lines.put("")  # EOF sentinel

    def call(self, command: str, args: dict, timeout: float = CALL_TIMEOUT_S) -> dict:
        rid = self._next_id
        self._next_id += 1
        self.proc.stdin.write(json.dumps({"id": rid, "command": command, "args": args}) + "\n")
        self.proc.stdin.flush()
        line = self._lines.get(timeout=timeout)
        if not line:
            raise RuntimeError(f"bridge serve closed stdout before answering {command!r}")
        resp = json.loads(line)
        assert resp.get("id") == rid, resp
        if not resp.get("ok"):
            raise RuntimeError(f"bridge {command!r} failed: {resp.get('error')}")
        return resp["result"]

    def close(self):
        try:
            self.proc.stdin.close()
            self.proc.wait(timeout=60)
        except Exception:
            try:
                self.proc.kill()
            except Exception:
                pass


# --- steps -------------------------------------------------------------------


def step_seed_via_bridge(serve: BridgeServe, url: str, docs: list[dict]) -> dict[str, str]:
    """Seed the corpus through the one persistent bridge serve process.

    Returns {source_uri: node_id} for planting the chain links afterwards.
    """
    t0 = time.perf_counter()
    ids: dict[str, str] = {}
    for i, doc in enumerate(docs, 1):
        res = serve.call("ingest", {
            "source_type": "text",
            "payload": doc["text"],
            "source_uri": doc["source_uri"],
            "title": doc["title"],
        })
        assert res["status"] == "inserted", (doc["id"], res)
        assert res["node_ids"], (doc["id"], res)
        ids[doc["source_uri"]] = res["node_ids"][0]
        if i % 10 == 0:
            print(f"  seed progress {i}/{len(docs)} ({time.perf_counter() - t0:.1f}s)")
    # verify all seed docs are present in the DB
    with psycopg.connect(url, row_factory=dict_row) as conn:
        rows = conn.execute(
            "SELECT source_uri, count(*) AS n FROM knowledge_items"
            " WHERE source_uri = ANY(%s) GROUP BY source_uri",
            ([d["source_uri"] for d in docs],),
        ).fetchall()
    uris = {r["source_uri"] for r in rows}
    expected = {d["source_uri"] for d in docs}
    assert uris == expected, (sorted(uris ^ expected))
    print(f"PASS seed_corpus_loaded ({len(rows)}/{len(docs)} source_uris present in DB, "
          f"{time.perf_counter() - t0:.1f}s via one bridge serve process)")
    return ids


# API direction -> schema direction, mirroring bridge.cmd_link_items.
_DIRECTION_TO_SCHEMA = {"out": "one_way", "in": "one_way", "both": "bidirectional"}


def step_plant_links(serve: BridgeServe, docs: list[dict], ids: dict[str, str]) -> None:
    """CREATE the planted chain relationships via serve link_items.

    Deterministic and parameterized: type/strength/direction come from each
    seed doc's "links" declaration.
    """
    by_id = {d["id"]: d for d in docs}
    n = 0
    for d in docs:
        for link in d.get("links", []):
            target = by_id[link["to"]]
            res = serve.call("link_items", {
                "from_id": ids[d["source_uri"]],
                "to_id": ids[target["source_uri"]],
                "type": link["type"],
                "strength": link["strength"],
                "direction": link["direction"],
                "context": link.get("context", "planted golden chain"),
            })
            assert res.get("type") == link["type"], res
            n += 1
    print(f"PASS plant_chain_links ({n} chain edges created via serve link_items)")


def step_plant_links_sql(url: str, docs: list[dict]) -> None:
    """No embedding model -> plant the chain edges with direct parameterized SQL."""
    by_id = {d["id"]: d for d in docs}
    with psycopg.connect(url, row_factory=dict_row) as conn:
        ids: dict[str, str] = {}
        for d in docs:
            row = conn.execute(
                "SELECT id FROM knowledge_items WHERE source_uri = %s"
                " ORDER BY created_at LIMIT 1",
                (d["source_uri"],),
            ).fetchone()
            assert row is not None, d["source_uri"]
            ids[d["source_uri"]] = row["id"]
        n = 0
        for d in docs:
            for link in d.get("links", []):
                target = by_id[link["to"]]
                edge_from, edge_to = ids[d["source_uri"]], ids[target["source_uri"]]
                if link["direction"] == "in":
                    edge_from, edge_to = edge_to, edge_from
                conn.execute(
                    "INSERT INTO relationships (from_id, to_id, type, strength, direction, context)"
                    " VALUES (%s, %s, %s, %s, %s, %s)"
                    " ON CONFLICT (from_id, to_id, type) DO NOTHING",
                    (edge_from, edge_to, link["type"], link["strength"],
                     _DIRECTION_TO_SCHEMA[link["direction"]],
                     link.get("context", "planted golden chain")),
                )
                n += 1
    print(f"PASS plant_chain_links ({n} chain edges via direct SQL — no embedding model)")


def step_seed_direct_sql(url: str, docs: list[dict]) -> None:
    """No embedding model -> insert the seed rows directly (embedding NULL)."""
    with psycopg.connect(url, row_factory=dict_row) as conn:
        for doc in docs:
            conn.execute(
                "INSERT INTO knowledge_items (type, title, content, mime_type, source_uri)"
                " VALUES (%s, %s, %s, %s, %s)",
                ("document", doc["title"], doc["text"], "text/plain", doc["source_uri"]),
            )
    print(f"PASS seed_corpus_loaded ({len(docs)} seed docs via direct SQL — no embedding model)")


def run_question_bridge(serve: BridgeServe, q: dict) -> list[dict]:
    out = serve.call("search", {"query": q["query"], "top_k": TOP_K})
    return out["results"]


def run_question_fts(url: str, q: dict) -> list[dict]:
    # search() embeds the query even for methods=["fts"]; without the model that
    # raises, so fall back to plain FTS via retrieval.fts.fts_search.
    from retrieval.db import get_connection
    from retrieval.fts import fts_search
    with get_connection() as conn:
        return fts_search(conn, q["query"], top_k=TOP_K)


def step_run_questions(serve: BridgeServe | None, url: str, docs: list[dict],
                       questions: list[dict], model_ok: bool) -> tuple[int, int, int, list[str]]:
    """Run every question; return (hybrid_hits, graph_hits, graph_total, failures).

    Legs per question:
      [hybrid]     default bridge search top_k=5 — feeds recall@5 over ALL questions
      [graph]      require_graph only: search(methods=["graph"], hops=min_hops,
                   top_k=20) — ASSERTED to contain the expected end doc
      [vector-only] require_graph only: search(methods=["vector"], top_k=5) —
                   hit/miss reported as INFO, never asserted
    """
    titles_by_uri = {d["source_uri"]: d["title"] for d in docs}
    hits = 0
    graph_hits = 0
    graph_total = 0
    vector_misses = 0
    failures: list[str] = []
    total = len(questions)
    t0 = time.perf_counter()
    for i, q in enumerate(questions, 1):
        expected_uri = q["expected_source_uri"]
        expected_title = titles_by_uri[expected_uri]

        def is_hit(results: list[dict]) -> bool:
            return any(r.get("source_uri") == expected_uri or r.get("title") == expected_title
                       for r in results)

        # --- [hybrid] recall leg (default search, top_k=5) --------------------
        if model_ok:
            assert serve is not None
            results = run_question_bridge(serve, q)
        else:
            results = run_question_fts(url, q)
        hit = is_hit(results)
        hits += 1 if hit else 0
        retrieved = [str(r.get("title") or r.get("source_uri") or r.get("id")) for r in results]
        print(f"{'PASS' if hit else 'FAIL'} question {i}/{total} [hybrid]: {q['query']}")
        print(f"      expected {expected_uri} — retrieved top-{TOP_K}: {retrieved}")

        # --- [graph] + [vector-only] legs for require_graph questions ---------
        if q.get("require_graph"):
            min_hops = int(q.get("min_hops", 2))
            if model_ok:
                assert serve is not None
                graph_total += 1
                gres = serve.call("search", {
                    "query": q["query"], "methods": ["graph"],
                    "hops": min_hops, "top_k": 20,
                })
                ghit = is_hit(gres["results"])
                graph_hits += 1 if ghit else 0
                print(f"{'PASS' if ghit else 'FAIL'} question {i}/{total} "
                      f"[graph hops={min_hops}]: {q['query']}")
                if ghit:
                    g = next(r for r in gres["results"]
                             if r.get("source_uri") == expected_uri or r.get("title") == expected_title)
                    print(f"      graph leg found {expected_uri} "
                          f"via_relationship={g.get('via_relationship')} "
                          f"graph_ranking={gres['params'].get('graph_ranking')}")
                else:
                    failures.append(
                        f"graph leg missed {expected_uri} (question {i}, hops={min_hops}): {q['query']}")
                    print(f"      graph uris: {[r.get('source_uri') for r in gres['results']]}")
                # vector-only informational leg
                vres = serve.call("search", {
                    "query": q["query"], "methods": ["vector"], "top_k": TOP_K,
                })
                vhit = is_hit(vres["results"])
                if not vhit:
                    vector_misses += 1
                print(f"INFO question {i}/{total} [vector-only top-{TOP_K}]: "
                      f"{'HIT' if vhit else 'MISS'} {expected_uri} (informational, not asserted)")
            else:
                print(f"SKIP question {i}/{total} [graph hops={min_hops}] "
                      "(models unavailable — fts-only mode, graph legs not run)")
    print(f"questions ran in {time.perf_counter() - t0:.1f}s")
    print(f"graph legs: {graph_hits}/{graph_total} require_graph questions found via graph; "
          f"vector-only misses: {vector_misses}/{graph_total} (informational)")
    return hits, graph_hits, graph_total, failures


def step_cleanup(url: str, docs: list[dict]) -> None:
    uris = [d["source_uri"] for d in docs]
    with psycopg.connect(url, row_factory=dict_row) as conn:
        # relationships cascade via FK ON DELETE CASCADE
        conn.execute("DELETE FROM knowledge_items WHERE source_uri = ANY(%s)", (uris,))
        remaining = conn.execute(
            "SELECT count(*) AS n FROM knowledge_items WHERE source_uri = ANY(%s)",
            (uris,),
        ).fetchone()["n"]
        orphans = conn.execute(
            "SELECT count(*) AS n FROM relationships r"
            " LEFT JOIN knowledge_items k ON k.id = r.from_id WHERE k.id IS NULL"
        ).fetchone()["n"]
    assert remaining == 0 and orphans == 0, (remaining, orphans)
    print(f"PASS cleanup (seeded source_uris deleted, remaining={remaining}, "
          f"orphan edges={orphans})")


def main() -> None:
    os.environ.setdefault("OMP_NUM_THREADS", "2")
    url = get_test_url()
    if not url:
        print(SKIP_MSG)
        sys.exit(0)

    # Refusal guard BEFORE any connection or destructive op: the dbname must
    # match (^|_)test(_|$) case-insensitively (stricter than a substring check).
    name = db_name(url)
    if not is_test_dbname(name):
        print(f"REFUSING to run against database {name!r}: TEST_DATABASE_URL must "
              "point at a test database (dbname matching (^|_)test(_|$)) — "
              "apply_schema() drops the knowledge tables.")
        sys.exit(1)

    if not ensure_database(url) or not db_reachable(url):
        print(SKIP_MSG)
        print(f"(connect to {_safe_db_label(url)} failed)")
        sys.exit(0)

    # pipeline/ and retrieval/ read DATABASE_URL — point them at the test DB
    os.environ["DATABASE_URL"] = url

    apply_schema(url)
    print("PASS setup_schema (db/schema.sql applied to test DB)")

    model_ok, model_err = check_models()
    if not model_ok:
        print(f"SKIP: embedding/reranker models unavailable ({model_err}) — "
              "questions run fts-only and recall@5 is reported, not asserted")

    docs = load_seed_docs()
    questions = load_questions({d["source_uri"] for d in docs})
    print(f"loaded {len(docs)} seed docs and {len(questions)} questions from tests/golden/")

    failures: list[str] = []
    try:
        # ONE persistent serve child pays the model load once and handles
        # seeding + chain planting + all question legs.
        serve = BridgeServe(url) if model_ok else None
        try:
            if model_ok:
                assert serve is not None
                ids = step_seed_via_bridge(serve, url, docs)
                step_plant_links(serve, docs, ids)
            else:
                step_seed_direct_sql(url, docs)
                step_plant_links_sql(url, docs)
            hits, graph_hits, graph_total, failures = step_run_questions(
                serve, url, docs, questions, model_ok)
        finally:
            if serve is not None:
                serve.close()

        total = len(questions)
        print(f"recall@5 = {hits}/{total}")
        if model_ok:
            if hits / total >= RECALL_THRESHOLD:
                print(f"PASS recall@5 {hits}/{total} >= {RECALL_THRESHOLD:.0%} threshold "
                      f"(>= {int(RECALL_THRESHOLD * total)}/{total})")
            else:
                failures.append(
                    f"recall@5 = {hits}/{total} below required {RECALL_THRESHOLD:.0%} "
                    f"(>= {int(RECALL_THRESHOLD * total)}/{total})")
            if graph_total > 0 and graph_hits == graph_total:
                print(f"PASS graph legs {graph_hits}/{graph_total} — every require_graph "
                      "question found via search(methods=['graph'], hops=min_hops)")
            elif graph_total > 0:
                failures.append(f"graph legs {graph_hits}/{graph_total} — every "
                                "require_graph question must be found via its graph leg")
        else:
            print(f"NOTE recall@5 = {hits}/{total} (fts-only, models unavailable) — "
                  "reported, not asserted")
    finally:
        step_cleanup(url, docs)

    if failures:
        print(f"\nGOLDEN FAILED ({len(failures)} failure(s)):")
        for f in failures:
            print(f"  - {f}")
        sys.exit(1)
    if model_ok:
        print("All golden tests passed.")
    else:
        print("Golden test run completed (fts-only mode, models unavailable).")
    sys.exit(0)


if __name__ == "__main__":
    main()
