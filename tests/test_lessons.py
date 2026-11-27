"""Tests for scripts/extract_lessons.py — Phase 4 lesson extraction from postmortems.

Two layers:

  1. No-DB unit tests (always run): the pure parse_lessons() section/bullet
     parser over in-test fixture texts (postmortem with Lesson/Decision/
     What-went-wrong sections + noise, bare "Lesson:"/"Decision:" bullets,
     noise-only text, garbage input), read_text_file() clean error on a
     missing file, slugify(), and the CLI guard that exits 1 with a clean
     message on a nonexistent input path (no DB is touched in that case).

  2. Guarded DB section (only with TEST_DATABASE_URL): writes 2 postmortem
     .md files (Lesson/Decision sections + noise) and 1 noise-only .md into
     a tmpdir, runs the exporter as a subprocess with TEST_DATABASE_URL,
     and asserts the lesson:// rows (titles carry the [lesson]/[decision]
     kind prefix, content non-empty), then re-runs the exporter and asserts
     every candidate is skipped-duplicate with no new rows (idempotent),
     then runs it on the noise-only file and asserts 0 candidates.

Setup — use a DEDICATED THROWAWAY database, never a production one:

    TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:5433/lessons_test \
        python tests/test_lessons.py

Guard pattern (same as tests/test_e2e.py / tests/test_export_chat.py):

  - TEST_DATABASE_URL is the ONLY accepted variable — no DATABASE_URL fallback.
  - REFUSE (exit 1) against any database whose name does not match (^|_)test(_|$)
    case-insensitively, checked BEFORE any connection or subprocess spawn — so
    the exporter is never pointed at a non-test DB when run by tests.
  - SKIP (exit 0) when TEST_DATABASE_URL is unset or unreachable.
  - The DB section applies db/schema.sql itself (drops the two tables in the
    TEST database only) and cleans up all lesson:// rows in a finally block.

All SQL is parameterized.

"""

from __future__ import annotations

import importlib.util
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
from psycopg import conninfo, sql  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402

# Load scripts/extract_lessons.py as a module (scripts/ is not a package).
_spec = importlib.util.spec_from_file_location(
    "extract_lessons", REPO_ROOT / "scripts" / "extract_lessons.py"
)
extract_lessons = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(extract_lessons)

SKIP_MSG = "SKIP: no test database available (set TEST_DATABASE_URL)"
LESSON_PREFIX = "lesson://%"
LESSON_URI_RE = re.compile(r"^lesson://[^#]+#\d+$")

# Locked guard: a test dbname matches (^|_)test(_|$) case-insensitively —
# stricter than a substring check, so e.g. "latest" is refused.
TEST_DBNAME_RE = re.compile(r"(?:^|_)test(?:_|$)", re.IGNORECASE)


# --- guards -------------------------------------------------------------------


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


# --- fixture documents ----------------------------------------------------------

PM_ALPHA_MD = """# Postmortem: Alpha deploy

## Context
The deploy of alpha-42 went out on Tuesday morning.

## What went wrong
The migration locked the orders table for forty minutes because it was never dry-run.

## Lessons learned
- Lesson: Always dry-run migrations before prod
- Pin transitive dependencies in requirements.txt
- Add a canary stage before full rollout

## Decision
- Decision: Ship alpha-42 behind a feature flag for two weeks

## Timeline
- 10:00 deploy started
- 10:05 alerts fired
"""

PM_BETA_MD = """# Decision log: storage layer

## Background
We compared Postgres against a dedicated graph database.

## Decision
- Decision: Keep Postgres as the single system of record
Postgres won on portability and operational simplicity.

## Retrospective
- Lesson: Do not stand up a dedicated graph DB before a measured failure
- Lesson: Rehearse backup and restore before launch
"""

NOISE_MD = """# Random notes

Just some prose about lunch.

- a plain bullet
- another plain bullet

## More
Text without any lesson or decision markers.
"""

# Expected parse results for the fixtures (kept explicit so the unit tests
# pin the heuristic contract, not just echo it).
ALPHA_KINDS = ["lesson", "lesson", "lesson", "lesson", "decision"]
BETA_KINDS = ["decision", "decision", "lesson", "lesson"]


# --- no-DB unit tests -----------------------------------------------------------


def test_parse_postmortem_sections():
    cands = extract_lessons.parse_lessons(PM_ALPHA_MD)
    assert [c["kind"] for c in cands] == ALPHA_KINDS, cands
    titles = [c["title"] for c in cands]
    assert "Always dry-run migrations before prod" in titles, titles
    assert "Pin transitive dependencies in requirements.txt" in titles, titles
    assert "Add a canary stage before full rollout" in titles, titles
    assert "Ship alpha-42 behind a feature flag for two weeks" in titles, titles
    assert all(c["text"].strip() for c in cands), "every candidate has non-empty text"
    # noise must NOT leak: context paragraph, timeline bullets
    joined = " ".join(c["text"] for c in cands)
    assert "went out on Tuesday" not in joined
    assert "10:00 deploy started" not in joined
    # the What-went-wrong prose paragraph is a lesson candidate
    assert any("locked the orders table" in c["text"] for c in cands)


def test_parse_decision_log_and_retro():
    cands = extract_lessons.parse_lessons(PM_BETA_MD)
    assert [c["kind"] for c in cands] == BETA_KINDS, cands
    titles = [c["title"] for c in cands]
    assert "Keep Postgres as the single system of record" in titles, titles
    assert "Do not stand up a dedicated graph DB before a measured failure" in titles, titles
    joined = " ".join(c["text"] for c in cands)
    assert "compared Postgres against" not in joined  # Background section is noise


def test_parse_bare_markers_outside_sections():
    text = "intro line\n\n- Lesson: Always pin your deps\n* Decision: Use Postgres\n- Note: ignored\n"
    cands = extract_lessons.parse_lessons(text)
    assert [(c["kind"], c["title"]) for c in cands] == [
        ("lesson", "Always pin your deps"),
        ("decision", "Use Postgres"),
    ], cands


def test_parse_no_markers_returns_empty():
    assert extract_lessons.parse_lessons(NOISE_MD) == []
    assert extract_lessons.parse_lessons("just prose\n\n- a plain bullet\n") == []


def test_parse_garbage_does_not_crash():
    out = extract_lessons.parse_lessons("\x00\xff\xfe###\n- Decision:\n- Lesson: x\n# \n")
    assert isinstance(out, list)
    assert all(isinstance(c["text"], str) for c in out)


def test_read_missing_file_clean_error():
    try:
        extract_lessons.read_text_file(REPO_ROOT / "tests" / "definitely_missing_xyz.md")
    except ValueError as e:
        assert "cannot read file" in str(e), str(e)
    else:
        raise AssertionError("read_text_file did not raise ValueError on a missing file")


def test_slugify():
    assert extract_lessons.slugify("PM Alpha.md") == "pm-alpha-md"
    assert extract_lessons.slugify("???") == "doc"
    assert len(extract_lessons.slugify("a" * 200)) <= 80


def test_cli_bad_path_exit1_clean_message():
    # Nonexistent input path -> exit 1 with a clean message; no DB is touched
    # (input validation happens before the database URL check).
    env = {k: v for k, v in os.environ.items()
           if k not in ("TEST_DATABASE_URL", "DATABASE_URL")}
    proc = subprocess.run(
        [sys.executable, "scripts/extract_lessons.py", "tests/definitely_missing_xyz.md"],
        cwd=str(REPO_ROOT), env=env, capture_output=True, text=True, timeout=60,
    )
    assert proc.returncode == 1, (proc.returncode, proc.stdout, proc.stderr)
    assert "definitely_missing_xyz.md" in proc.stderr, proc.stderr
    assert "DATABASE_URL" not in proc.stderr, proc.stderr  # path error, not a DB error


# --- guarded DB section -----------------------------------------------------------


def run_exporter(target: str, url: str) -> subprocess.CompletedProcess:
    """Run the exporter subprocess with TEST_DATABASE_URL (never DATABASE_URL)
    and extraction env vars stripped, so the heuristic-only path is tested."""
    env = {k: v for k, v in os.environ.items()
           if k not in ("DATABASE_URL", "EXTRACT_ENABLED", "EXTRACT_API_URL",
                        "EXTRACT_API_KEY", "EXTRACT_MODEL")}
    env["TEST_DATABASE_URL"] = url
    env["OMP_NUM_THREADS"] = "2"
    return subprocess.run(
        [sys.executable, "scripts/extract_lessons.py", target, "--glob", "*.md"],
        cwd=str(REPO_ROOT), env=env, capture_output=True, text=True, timeout=900,
    )


def lesson_rows(url: str) -> list[dict]:
    with psycopg.connect(url, row_factory=dict_row) as conn:
        return conn.execute(
            "SELECT id, source_uri, title, content, type FROM knowledge_items"
            " WHERE source_uri LIKE %s ORDER BY source_uri",
            (LESSON_PREFIX,),
        ).fetchall()


def db_section(url: str) -> list[str]:
    """Run the exporter against the guarded test DB. Returns failure messages."""
    failures: list[str] = []

    def check(cond: bool, msg: str) -> bool:
        print(f"{'PASS' if cond else 'FAIL'} {msg}")
        if not cond:
            failures.append(msg)
        return cond

    apply_schema(url)
    print("PASS setup_schema (db/schema.sql applied to test DB)")

    with tempfile.TemporaryDirectory() as td:
        docs = {
            "pm_alpha.md": PM_ALPHA_MD,
            "pm_beta.md": PM_BETA_MD,
            "noise.md": NOISE_MD,
        }
        paths = {}
        for name, content in docs.items():
            p = Path(td) / name
            p.write_text(content, encoding="utf-8")
            paths[name] = p

        expected = {name: extract_lessons.parse_lessons(content)
                    for name, content in docs.items()}
        assert expected["noise.md"] == []
        expected_total = sum(len(v) for v in expected.values())
        assert expected_total == len(ALPHA_KINDS) + len(BETA_KINDS) == 9, expected_total

        # --- first run on the directory: all candidates ingested ------------
        proc = run_exporter(str(Path(td)), url)
        check(proc.returncode == 0,
              f"exporter exit 0 on dir run (rc={proc.returncode}, stderr tail: "
              f"{(proc.stderr or '')[-300:]!r})")
        check(proc.stdout.count("ingested lesson://") == expected_total,
              f"exporter printed {expected_total} 'ingested lesson://' lines "
              f"(stdout: {proc.stdout!r})")
        check(f"summary: files=3 candidates={expected_total} ingested={expected_total} "
              f"skipped-duplicate=0 skipped-empty=0 errors=0" in proc.stdout,
              f"first-run summary line (stdout: {proc.stdout!r})")

        rows = lesson_rows(url)
        check(len(rows) == expected_total,
              f"DB holds {expected_total} lesson:// rows (got {len(rows)})")
        check(all(r["type"] == "note" for r in rows),
              "all lesson:// rows have type 'note'")
        check(all((r["content"] or "").strip() for r in rows),
              "all lesson:// rows have non-empty content")
        check(all(LESSON_URI_RE.match(r["source_uri"] or "") for r in rows),
              f"source_uris are lesson://<slug>#<n> form (got "
              f"{[r['source_uri'] for r in rows]})")
        titles = [r["title"] or "" for r in rows]
        check(all(t.startswith("[") for t in titles), "titles carry the [kind] prefix")
        check(any(t.startswith("[lesson] ") for t in titles), "at least one [lesson] title")
        check(any(t.startswith("[decision] ") for t in titles), "at least one [decision] title")
        check("[lesson] Always dry-run migrations before prod" in titles,
              f"expected lesson title present (titles: {titles})")
        check("[decision] Ship alpha-42 behind a feature flag for two weeks" in titles,
              f"expected decision title present (titles: {titles})")
        bodies = " ".join(r["content"] or "" for r in rows)
        check("10:00 deploy started" not in bodies and "went out on Tuesday" not in bodies,
              "noise content was not ingested")

        # --- re-run: everything is a duplicate, no new rows ------------------
        proc2 = run_exporter(str(Path(td)), url)
        check(proc2.returncode == 0, f"re-run exit 0 (rc={proc2.returncode})")
        check(proc2.stdout.count("skipped-duplicate lesson://") == expected_total,
              f"re-run reports all {expected_total} as skipped-duplicate "
              f"(stdout: {proc2.stdout!r})")
        check("ingested lesson://" not in proc2.stdout,
              "re-run ingested nothing")
        check(f"summary: files=3 candidates={expected_total} ingested=0 "
              f"skipped-duplicate={expected_total} skipped-empty=0 errors=0" in proc2.stdout,
              f"re-run summary line (stdout: {proc2.stdout!r})")
        check(len(lesson_rows(url)) == expected_total,
              "re-run created no new rows (idempotent)")

        # --- noise-only file: zero candidates, exit 0, no rows ---------------
        proc3 = run_exporter(str(paths["noise.md"]), url)
        check(proc3.returncode == 0, f"noise-file run exit 0 (rc={proc3.returncode})")
        check("candidates=0" in proc3.stdout, f"noise-file run reports 0 candidates "
                                             f"(stdout: {proc3.stdout!r})")
        check("ingested lesson://" not in proc3.stdout, "noise-file run ingested nothing")
        check(len(lesson_rows(url)) == expected_total,
              "noise-file run added no rows")

    return failures


def step_cleanup(url: str) -> None:
    with psycopg.connect(url, row_factory=dict_row) as conn:
        conn.execute("DELETE FROM knowledge_items WHERE source_uri LIKE %s", (LESSON_PREFIX,))
        remaining = conn.execute(
            "SELECT count(*) AS n FROM knowledge_items WHERE source_uri LIKE %s",
            (LESSON_PREFIX,),
        ).fetchone()["n"]
    assert remaining == 0
    print("PASS cleanup (lesson:// rows deleted)")


# --- main -------------------------------------------------------------------------


def main() -> None:
    url = get_test_url()

    # Refusal guard BEFORE any connection / subprocess spawn: dbname must match
    # (^|_)test(_|$) case-insensitively (stricter than a substring check).
    if url is not None:
        name = db_name(url)
        if not is_test_dbname(name):
            print(f"REFUSING to run against database {name!r}: TEST_DATABASE_URL must "
                  "point at a test database (dbname matching (^|_)test(_|$)) — "
                  "the DB section drops the knowledge tables and spawns the exporter "
                  "with TEST_DATABASE_URL pointed there.")
            sys.exit(1)

    # --- no-DB unit tests (always run) ---
    failed = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
            except Exception as exc:  # noqa: BLE001 — report and continue
                failed += 1
                print(f"FAIL {name}: {type(exc).__name__}: {exc}")
            else:
                print(f"PASS {name}")

    # --- guarded DB section ---
    if url is None:
        print(SKIP_MSG)
        sys.exit(1 if failed else 0)
    if not ensure_database(url) or not db_reachable(url):
        print(SKIP_MSG)
        print(f"(connect to {_safe_db_label(url)} failed)")
        sys.exit(1 if failed else 0)

    os.environ["DATABASE_URL"] = url  # in-process pipeline (if any) reads this
    try:
        failures = db_section(url)
    finally:
        step_cleanup(url)
    failed += len(failures)

    if failed:
        print(f"{failed} test(s) FAILED.")
        sys.exit(1)
    print("All tests passed.")


if __name__ == "__main__":
    main()
