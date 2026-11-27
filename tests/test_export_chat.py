"""Tests for scripts/export_chat.py — the AI-chat export importer.

Two layers:

  1. No-DB unit tests (always run): the pure parse_export() function over
     in-test fixture JSONs (generic list shape, {"conversations": [...]} shape,
     content-as-parts shape, "text" message key), malformed JSON -> clean
     ValueError, slugify(), and the CLI guard that exits 1 with a clear message
     when DATABASE_URL is unset (no DB is touched in that case).

  2. Guarded DB section (only with TEST_DATABASE_URL): runs the exporter as a
     subprocess against a throwaway test database and asserts the ingested
     rows (chat:// source_uris, role-prefixed content), plus duplicate handling
     on re-run.

Setup — use a DEDICATED THROWAWAY database, never a production one:

    TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:5433/chatexport_test \
        python tests/test_export_chat.py

Guard pattern (same as tests/test_e2e.py / tests/test_golden.py):

  - TEST_DATABASE_URL is the ONLY accepted variable — no DATABASE_URL fallback.
  - REFUSE (exit 1) against any database whose name does not match (^|_)test(_|$)
    case-insensitively, checked BEFORE any connection or subprocess spawn — so
    the exporter is never pointed at a non-test DB when run by tests.
  - SKIP (exit 0) when TEST_DATABASE_URL is unset or unreachable.
  - The DB section applies db/schema.sql itself (drops the two tables in the
    TEST database only) and cleans up all chat:// rows in a finally block.

All SQL is parameterized.
"""

from __future__ import annotations

import importlib.util
import json
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

# Load scripts/export_chat.py as a module (scripts/ is not a package).
_spec = importlib.util.spec_from_file_location("export_chat", REPO_ROOT / "scripts" / "export_chat.py")
export_chat = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(export_chat)

SKIP_MSG = "SKIP: no test database available (set TEST_DATABASE_URL)"
CHAT_PREFIX = "chat://%"

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


# --- fixture exports ------------------------------------------------------------

LIST_SHAPE = [
    {
        "title": "Trip planning",
        "messages": [
            {"role": "user", "content": "We need a hand-crank radio for the go-bag. Which one?"},
            {"role": "assistant", "content": "The zephyr-9: one minute of cranking gives about 45 minutes of listening."},
        ],
    },
    {
        "title": "Cooking ideas",
        "messages": [
            {"role": "user", "content": "What should we cook with the CSA box this week?"},
            {"role": "assistant", "content": "Frittata with the greens, and roast the squash with sage."},
        ],
    },
]

WRAPPED_SHAPE = {
    "conversations": [
        {
            "title": "Budget review",
            "messages": [
                {"role": "user", "content": [
                    {"type": "text", "text": "How much did we spend on "},
                    {"type": "text", "text": "software subscriptions in March?"},
                    {"type": "image_url", "image_url": {"url": "data:image/png;base64,xxx"}},
                ]},
                {"role": "assistant", "content": "About 212 dollars across five subscriptions."},
            ],
        },
        {
            "title": "Book club",
            "messages": [
                {"role": "user", "text": "Next month we read the permafrost book, right?"},
                {"role": "assistant", "text": "Yes — tundra-2 station log, chapters 1 to 4."},
            ],
        },
    ]
}

MIXED_SHAPE = [
    {
        "name": "Standup notes",
        "messages": [
            {"role": "user", "content": "Ship the harbor-5 microgrid dashboard today?"},
            {"role": "assistant", "content": [
                {"type": "text", "text": "Yes, after the tidal prediction widget "},
                {"type": "text", "text": "is verified against the buoy data."},
            ]},
        ],
    }
]


def write_fixture(dir_path: Path, name: str, obj) -> Path:
    p = dir_path / name
    p.write_text(json.dumps(obj, ensure_ascii=False), encoding="utf-8")
    return p


# --- no-DB unit tests ------------------------------------------------------------


def test_parse_list_shape():
    with tempfile.TemporaryDirectory() as td:
        p = write_fixture(Path(td), "list.json", LIST_SHAPE)
        convs = export_chat.parse_export(p)
    assert len(convs) == 2, convs
    assert convs[0]["title"] == "Trip planning"
    assert convs[0]["index"] == 0 and convs[1]["index"] == 1
    text = convs[0]["text"]
    assert text.startswith("# Trip planning\n\n"), text
    assert "user: We need a hand-crank radio" in text, text
    assert "assistant: The zephyr-9" in text, text
    assert convs[0]["n_messages"] == 2


def test_parse_conversations_key_shape():
    with tempfile.TemporaryDirectory() as td:
        p = write_fixture(Path(td), "wrapped.json", WRAPPED_SHAPE)
        convs = export_chat.parse_export(p)
    assert len(convs) == 2, convs
    # content given as a list of parts: text parts joined, image part skipped
    budget = convs[0]["text"]
    assert "user: How much did we spend on software subscriptions in March?" in budget, budget
    assert "image_url" not in budget and "base64" not in budget, budget
    assert "assistant: About 212 dollars" in budget, budget
    # message using the "text" key instead of "content"
    assert "user: Next month we read the permafrost book, right?" in convs[1]["text"]


def test_parse_content_parts_shape():
    with tempfile.TemporaryDirectory() as td:
        p = write_fixture(Path(td), "mixed.json", MIXED_SHAPE)
        convs = export_chat.parse_export(p)
    assert len(convs) == 1
    assert convs[0]["title"] == "Standup notes"  # fell back to "name"
    text = convs[0]["text"]
    assert text.startswith("# Standup notes\n\n")
    assert "assistant: Yes, after the tidal prediction widget is verified against the buoy data." in text, text


def test_parse_malformed_json_clean_error():
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "broken.json"
        p.write_text("{not json", encoding="utf-8")
        try:
            export_chat.parse_export(p)
        except ValueError as e:
            assert "not valid JSON" in str(e), str(e)
        else:
            raise AssertionError("parse_export did not raise ValueError on malformed JSON")


def test_parse_unsupported_shape_clean_error():
    with tempfile.TemporaryDirectory() as td:
        p = write_fixture(Path(td), "weird.json", {"foo": [1, 2, 3]})
        try:
            export_chat.parse_export(p)
        except ValueError as e:
            assert "unsupported shape" in str(e), str(e)
        else:
            raise AssertionError("parse_export did not raise ValueError on unsupported shape")


def test_parse_missing_messages_clean_error():
    with tempfile.TemporaryDirectory() as td:
        p = write_fixture(Path(td), "nomsg.json", [{"title": "x"}])
        try:
            export_chat.parse_export(p)
        except ValueError as e:
            assert "messages" in str(e), str(e)
        else:
            raise AssertionError("parse_export did not raise ValueError when messages missing")


def test_slugify():
    assert export_chat.slugify("Trip planning") == "trip-planning"
    assert export_chat.slugify("  Weird: title/ HERE!! ") == "weird-title-here"
    assert export_chat.slugify("???") == "conversation"
    assert len(export_chat.slugify("a" * 200)) <= 60


def test_cli_requires_database_url():
    # No DATABASE_URL -> exit 1 with a clear message; no DB is touched.
    with tempfile.TemporaryDirectory() as td:
        p = write_fixture(Path(td), "list.json", LIST_SHAPE)
        env = {k: v for k, v in os.environ.items() if k != "DATABASE_URL"}
        proc = subprocess.run(
            [sys.executable, "scripts/export_chat.py", str(p), "--once"],
            cwd=str(REPO_ROOT), env=env, capture_output=True, text=True, timeout=60,
        )
    assert proc.returncode == 1, (proc.returncode, proc.stdout, proc.stderr)
    assert "DATABASE_URL" in proc.stderr, proc.stderr


# --- guarded DB section -----------------------------------------------------------


def run_exporter(export_file: Path, url: str) -> subprocess.CompletedProcess:
    env = {k: v for k, v in os.environ.items() if k != "DATABASE_URL"}
    env["DATABASE_URL"] = url
    env["OMP_NUM_THREADS"] = "2"
    return subprocess.run(
        [sys.executable, "scripts/export_chat.py", str(export_file),
         "--source-uri-prefix", "chat://", "--once"],
        cwd=str(REPO_ROOT), env=env, capture_output=True, text=True, timeout=900,
    )


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
        fixtures = [
            write_fixture(Path(td), "list.json", LIST_SHAPE),
            write_fixture(Path(td), "wrapped.json", WRAPPED_SHAPE),
            write_fixture(Path(td), "mixed.json", MIXED_SHAPE),
        ]
        expected_per_file = {fixtures[0]: 2, fixtures[1]: 2, fixtures[2]: 1}
        total = sum(expected_per_file.values())

        # --- first run: everything ingested --------------------------------
        for f in fixtures:
            proc = run_exporter(f, url)
            n = expected_per_file[f]
            check(proc.returncode == 0,
                  f"exporter exit 0 on {f.name} (rc={proc.returncode}, stderr tail: "
                  f"{(proc.stderr or '')[-200:]!r})")
            check(proc.stdout.count("ingested chat://") == n,
                  f"exporter printed {n} 'ingested chat://' lines for {f.name} "
                  f"(stdout: {proc.stdout!r})")
            check(f"summary: ingested={n} skipped-duplicate=0 skipped-empty=0 errors=0" in proc.stdout,
                  f"summary line for {f.name} (stdout: {proc.stdout!r})")

        with psycopg.connect(url, row_factory=dict_row) as conn:
            rows = conn.execute(
                "SELECT source_uri, title, content FROM knowledge_items"
                " WHERE source_uri LIKE %s ORDER BY source_uri",
                (CHAT_PREFIX,),
            ).fetchall()
        check(len(rows) == total, f"DB holds {total} chat:// rows (got {len(rows)})")
        uris = {r["source_uri"] for r in rows}
        expected_uris = {
            "chat://trip-planning-0", "chat://cooking-ideas-1",
            "chat://budget-review-0", "chat://book-club-1",
            "chat://standup-notes-0",
        }
        check(uris == expected_uris, f"source_uris are slug-index form (got {sorted(uris)})")
        for r in rows:
            ok = (r["content"] or "").startswith("# ") and "user:" in r["content"] \
                and "assistant:" in r["content"]
            if not ok:
                failures.append(f"row {r['source_uri']} content malformed: {r['content'][:120]!r}")
        check(all((r["content"] or "").startswith("# ") for r in rows),
              "all chat:// rows start with '# <title>'")
        check(any("user: How much did we spend on software subscriptions in March?" in r["content"]
                  for r in rows), "parts-style content joined correctly in DB")
        print(f"PASS db_rows ({len(rows)} rows, roles + title header present)"
              if not any("content malformed" in f for f in failures) else "FAIL db_rows content")

        # --- re-run: everything is a duplicate -------------------------------
        proc = run_exporter(fixtures[2], url)  # 1-conversation file
        check(proc.returncode == 0, f"re-run exit 0 (rc={proc.returncode})")
        check(proc.stdout.count("skipped-duplicate chat://") == 1,
              f"re-run reports the conversation as skipped-duplicate (stdout: {proc.stdout!r})")
        check("summary: ingested=0 skipped-duplicate=1 skipped-empty=0 errors=0" in proc.stdout,
              f"re-run summary line (stdout: {proc.stdout!r})")
        with psycopg.connect(url, row_factory=dict_row) as conn:
            n = conn.execute(
                "SELECT count(*) AS n FROM knowledge_items WHERE source_uri LIKE %s",
                (CHAT_PREFIX,),
            ).fetchone()["n"]
        check(n == total, f"re-run created no new rows (still {total}, got {n})")

    return failures


def step_cleanup(url: str) -> None:
    with psycopg.connect(url, row_factory=dict_row) as conn:
        conn.execute("DELETE FROM knowledge_items WHERE source_uri LIKE %s", (CHAT_PREFIX,))
        remaining = conn.execute(
            "SELECT count(*) AS n FROM knowledge_items WHERE source_uri LIKE %s",
            (CHAT_PREFIX,),
        ).fetchone()["n"]
    assert remaining == 0
    print("PASS cleanup (chat:// rows deleted)")


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
                  "with DATABASE_URL pointed there.")
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
