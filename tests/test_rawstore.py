"""Integration test for the content-addressed raw document store (ROADMAP Phase 1).

What is under test:
  - pipeline/raw_store.py: store_raw_bytes / read_raw / raw_path_for write to
    <repo root>/raw/<sha256[:2]>/<sha256> (raw/ is gitignored — layout documented
    in pipeline/raw_store.py's docstring; this note is the in-tests pointer).
    Idempotent, atomic (tmp + rename), path-traversal safe (hex-validated hash).
  - pipeline/ingest.py store_raw=True: raw bytes archived, metadata.raw_sha256
    set, source_uri defaults to raw://<sha256> when not given, explicit
    source_uri kept, dedup still hits on re-ingested identical bytes, and the
    default store_raw=False path is unchanged (no raw file, no raw_sha256).

Setup — use a DEDICATED THROWAWAY database, never a production one:

    createdb rawstore_test
    export TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:5433/rawstore_test
    python tests/test_rawstore.py

Guard pattern copied exactly from tests/test_e2e.py: TEST_DATABASE_URL is the
ONLY accepted variable (no DATABASE_URL fallback — apply_schema() drops tables
and must never be pointed at a real database); the script refuses any database
whose name does not match (^|_)test(_|$) (exit 1, BEFORE any destructive op); if
TEST_DATABASE_URL is unset or unreachable it prints
"SKIP: no test database available (set TEST_DATABASE_URL)" and exits 0.
The script applies db/schema.sql itself (idempotently) to the TEST database
only, points DATABASE_URL at it for pipeline/ + retrieval/, and in a finally
block deletes the raw:// / rawstore:// rows it created and unlinks the raw
files it created (tracked by hash, rmdir if empty).

All SQL is parameterized.
"""

import hashlib
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

REPO_ROOT = Path(__file__).resolve().parent.parent

import psycopg  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402

from pipeline.raw_store import RAW_ROOT, raw_path_for, read_raw, store_raw_bytes  # noqa: E402

SKIP_MSG = "SKIP: no test database available (set TEST_DATABASE_URL)"

# Raw payloads used as ingest input (bytes, so we can assert byte-exact storage)
DOC_RAW_A = (b"Raw store test document alpha: the content-addressed store keeps "
             b"the original bytes under raw/ addressed by their sha256 digest.")
DOC_RAW_B = (b"Raw store test document beta: an explicit source URI is preserved "
             b"while the raw bytes are still archived in the content store.")
DOC_RAW_C = ("Raw store test document gamma: ingested without store_raw, so no "
             "raw object is written and no raw_sha256 appears in metadata.")


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


# Locked guard: a test dbname matches (^|_)test(_|$) case-insensitively —
# stricter than a substring check, so e.g. "latest" is refused.
TEST_DBNAME_RE = re.compile(r"(?:^|_)test(?:_|$)", re.IGNORECASE)


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
        from pipeline.embed import embed_text
        embed_text("warmup")
        from retrieval.rerank import rerank
        rerank("warmup", [{"content": "warmup"}])
        return True, ""
    except Exception as e:
        return False, f"{type(e).__name__}: {str(e)[:200]}"


def raw_files() -> set[Path]:
    """All files currently under the repo's raw/ store."""
    if not RAW_ROOT.is_dir():
        return set()
    return {p for p in RAW_ROOT.rglob("*") if p.is_file()}


# --- steps -------------------------------------------------------------------

def step_1_raw_store_unit(ctx) -> None:
    """Pure raw_store checks — no model, no DB rows (uses a temp raw_root)."""
    data = b"unit-test raw bytes \x00\xff binary-safe \n\r\n"
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        info = store_raw_bytes(data, raw_root=root)
        sha = info["sha256"]
        assert sha == hashlib.sha256(data).hexdigest()
        p = raw_path_for(sha, root)
        assert info["path"] == str(p)
        assert p == root / sha[:2] / sha, p
        assert p.is_file() and p.read_bytes() == data
        assert read_raw(sha, root) == data
        # idempotent: existing object is NOT rewritten
        mtime = p.stat().st_mtime_ns
        info2 = store_raw_bytes(data, raw_root=root)
        assert info2 == info
        assert p.stat().st_mtime_ns == mtime
        # different bytes -> different path
        other = store_raw_bytes(data + b"x", raw_root=root)
        assert other["sha256"] != sha and raw_path_for(other["sha256"], root).is_file()
    # default root is <repo root>/raw
    assert raw_path_for(sha) == RAW_ROOT / sha[:2] / sha
    # path-traversal safety: only 64 lowercase hex chars are accepted
    for bad in ("../etc/passwd", ".." + "/" * 30, "zz" * 32, "A" * 64, "a" * 63, "", None):
        for fn in (raw_path_for, read_raw):
            try:
                fn(bad)
                raise AssertionError(f"{fn.__name__} accepted invalid hash {bad!r}")
            except (ValueError, TypeError):
                pass
    try:
        read_raw("0" * 64)  # valid format, absent object
        raise AssertionError("read_raw should raise FileNotFoundError for missing object")
    except FileNotFoundError:
        pass
    print("PASS step_1_raw_store_unit (addressing, idempotency, atomic layout, traversal-safe)")


def step_2_ingest_store_raw_no_uri(ctx) -> None:
    if not ctx["model_ok"]:
        print(f"SKIP step_2_ingest_store_raw_no_uri (model unavailable: {ctx['model_err']})")
        return
    from pipeline.ingest import ingest
    sha = hashlib.sha256(DOC_RAW_A).hexdigest()
    res = ingest("text", DOC_RAW_A, title="rawstore doc A", store_raw=True)
    assert res["status"] == "inserted", res
    assert res.get("raw_sha256") == sha, res
    ctx["hashes"].append(sha)
    ctx["sha_a"] = sha
    expected = RAW_ROOT / sha[:2] / sha
    assert expected == raw_path_for(sha)
    assert expected.is_file(), f"raw file missing at {expected}"
    assert expected.read_bytes() == DOC_RAW_A  # byte-exact match with original payload
    assert read_raw(sha) == DOC_RAW_A
    with psycopg.connect(ctx["url"], row_factory=dict_row) as conn:
        rows = conn.execute(
            "SELECT source_uri, metadata->>'raw_sha256' AS raw_sha FROM knowledge_items"
            " WHERE id = ANY(%s)",
            (res["node_ids"],),
        ).fetchall()
    assert rows, "no rows inserted"
    for r in rows:
        assert r["raw_sha"] == sha, r
        assert r["source_uri"] is not None and r["source_uri"].startswith("raw://"), r
        assert r["source_uri"] == f"raw://{sha}", r
    print("PASS step_2_ingest_store_raw_no_uri (raw file at raw/<2>/<sha>, metadata.raw_sha256, raw:// source_uri)")


def step_3_ingest_store_raw_explicit_uri(ctx) -> None:
    if not ctx["model_ok"]:
        print(f"SKIP step_3_ingest_store_raw_explicit_uri (model unavailable: {ctx['model_err']})")
        return
    from pipeline.ingest import ingest
    sha = hashlib.sha256(DOC_RAW_B).hexdigest()
    res = ingest("text", DOC_RAW_B, source_uri="rawstore://explicit-b",
                 title="rawstore doc B", store_raw=True)
    assert res["status"] == "inserted", res
    assert res.get("raw_sha256") == sha, res
    ctx["hashes"].append(sha)
    p = raw_path_for(sha)
    assert p.is_file() and p.read_bytes() == DOC_RAW_B, "raw file not stored for explicit-uri doc"
    with psycopg.connect(ctx["url"], row_factory=dict_row) as conn:
        rows = conn.execute(
            "SELECT source_uri, metadata->>'raw_sha256' AS raw_sha FROM knowledge_items"
            " WHERE id = ANY(%s)",
            (res["node_ids"],),
        ).fetchall()
    assert rows, "no rows inserted"
    for r in rows:
        assert r["source_uri"] == "rawstore://explicit-b", r  # explicit source_uri kept
        assert r["raw_sha"] == sha, r
    print("PASS step_3_ingest_store_raw_explicit_uri (raw file stored AND source_uri kept)")


def step_4_duplicate_same_bytes(ctx) -> None:
    if not ctx["model_ok"]:
        print(f"SKIP step_4_duplicate_same_bytes (model unavailable: {ctx['model_err']})")
        return
    from pipeline.ingest import ingest
    before = raw_files()
    res = ingest("text", DOC_RAW_A, title="rawstore doc A, different title", store_raw=True)
    assert res["status"] == "duplicate", res
    after = raw_files()
    assert after == before, f"a second raw file was created for duplicate bytes: {after ^ before}"
    print("PASS step_4_duplicate_same_bytes (status duplicate, no second raw file)")


def step_5_search_finds_raw_doc(ctx) -> None:
    if not ctx["model_ok"]:
        print(f"SKIP step_5_search_finds_raw_doc (model unavailable: {ctx['model_err']})")
        return
    from retrieval.search import search
    out = search("content-addressed raw store sha256 original bytes", top_k=5)
    assert out["results"], out
    assert any(r.get("source_uri") == f"raw://{ctx['sha_a']}" for r in out["results"]), out["results"]
    print("PASS step_5_search_finds_raw_doc (retrieval.search finds the raw:// doc)")


def step_6_backward_compat_no_store_raw(ctx) -> None:
    if not ctx["model_ok"]:
        print(f"SKIP step_6_backward_compat_no_store_raw (model unavailable: {ctx['model_err']})")
        return
    from pipeline.ingest import ingest
    before = raw_files()
    res = ingest("text", DOC_RAW_C, source_uri="rawstore://no-raw", title="rawstore doc C")
    assert res["status"] == "inserted", res
    assert "raw_sha256" not in res, res  # default path unchanged
    assert raw_files() == before, "store_raw=False must not write any raw file"
    with psycopg.connect(ctx["url"], row_factory=dict_row) as conn:
        rows = conn.execute(
            "SELECT source_uri, metadata->>'raw_sha256' AS raw_sha FROM knowledge_items"
            " WHERE id = ANY(%s)",
            (res["node_ids"],),
        ).fetchall()
    assert rows, "no rows inserted"
    for r in rows:
        assert r["source_uri"] == "rawstore://no-raw", r
        assert r["raw_sha"] is None, r
    print("PASS step_6_backward_compat_no_store_raw (no raw file, no raw_sha256, source_uri kept)")


def step_7_cleanup(ctx) -> None:
    with psycopg.connect(ctx["url"], row_factory=dict_row) as conn:
        conn.execute(
            "DELETE FROM knowledge_items WHERE source_uri LIKE %s OR source_uri LIKE %s",
            ("raw://%", "rawstore://%"),
        )
        remaining = conn.execute(
            "SELECT count(*) AS n FROM knowledge_items WHERE source_uri LIKE %s OR source_uri LIKE %s",
            ("raw://%", "rawstore://%"),
        ).fetchone()["n"]
    assert remaining == 0
    for sha in ctx["hashes"]:
        p = raw_path_for(sha)
        if p.is_file():
            p.unlink()
        try:
            p.parent.rmdir()  # only succeeds when empty
        except OSError:
            pass
    try:
        RAW_ROOT.rmdir()  # only succeeds when empty
    except OSError:
        pass
    print("PASS step_7_cleanup (raw:// + rawstore:// rows deleted; test raw files unlinked, dirs rmdir'd if empty)")


def main() -> None:
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

    if not db_reachable(url):
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
              "model-dependent steps will be skipped")

    ctx = {"url": url, "model_ok": model_ok, "model_err": model_err, "hashes": []}
    steps = [
        step_1_raw_store_unit,
        step_2_ingest_store_raw_no_uri,
        step_3_ingest_store_raw_explicit_uri,
        step_4_duplicate_same_bytes,
        step_5_search_finds_raw_doc,
        step_6_backward_compat_no_store_raw,
    ]
    try:
        for step in steps:
            step(ctx)
    finally:
        step_7_cleanup(ctx)
    print("All rawstore tests passed.")


if __name__ == "__main__":
    main()
