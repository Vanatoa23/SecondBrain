"""Tests for scripts/sync_notes.py (Markdown notes vault sync) against a REAL
PostgreSQL + pgvector database.

Setup — use a DEDICATED THROWAWAY database, never a production one:

    createdb notessync_test
    export TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:5433/notessync_test
    python tests/test_notes_sync.py

Guard pattern (identical to tests/test_e2e.py):
  * TEST_DATABASE_URL is the ONLY accepted variable — there is deliberately no
    DATABASE_URL fallback, because apply_schema() drops the knowledge tables.
  * The script refuses to run against any database whose name does not match
    (^|_)test(_|$) (prints REFUSING, exit 1) BEFORE any destructive op.
  * If TEST_DATABASE_URL is unset or unreachable it prints
    "SKIP: no test database available (set TEST_DATABASE_URL)" and exits 0.

Flow: apply db/schema.sql to the test DB, create a temp vault with 3 .md files,
run `python scripts/sync_notes.py --once --dir <tmp>` via subprocess (cwd=repo
root, DATABASE_URL pointed at the test DB):
  1. first run  -> 3 ingested, 3 knowledge_items rows
  2. modify 1 file + add 1 new file -> 2 ingested, 2 skipped-unchanged;
     changed file created a NEW row (append-only versioning: old row kept,
     rows for that source_uri == 2); total rows grow 3 -> 5
  3. rerun with no changes -> 0 ingested, all skipped, row count unchanged
  4. guard paths: self-subprocess with TEST_DATABASE_URL=<.../postgres> ->
     REFUSING exit 1; self-subprocess without TEST_DATABASE_URL -> SKIP exit 0;
     sync script without DATABASE_URL -> exit 1
Cleanup (finally): delete the test rows (parameterized, prefix match on the
temp dir path) and remove the temp vault.

All SQL is parameterized.
"""

from __future__ import annotations

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
from psycopg import conninfo  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402

SKIP_MSG = "SKIP: no test database available (set TEST_DATABASE_URL)"

NOTE_1 = """# Database internals

PostgreSQL write-ahead logging protects every committed transaction: changes
are appended to the WAL before they land in the data files, so a crash mid
write never corrupts the table. Checkpoints flush dirty buffers and allow WAL
recycling. A standby replays the same WAL stream, which is why physical
replication is nearly free once the primary is healthy.
"""

NOTE_2 = """# Sourdough notes

Hydration around 75 percent keeps the dough slack and extensible. Bulk ferment
at 24 degrees Celsius until the volume grows by half, then preshape and cold
retard overnight in the fridge. Score deeply before baking in a Dutch oven at
230 degrees with the lid on for twenty minutes to trap steam.
"""

NOTE_3 = """# Marathon training block

Twelve weeks, four runs per week: one long slow run on Sunday building to
thirty kilometers, one tempo run at goal pace on Wednesday, easy aerobic
recovery runs otherwise, and strides after easy days. Fuel the long run with
sixty grams of carbohydrate per hour and taper for ten days before race day.
"""

NOTE_4 = """# Telescope log

TheDobsonian saw first light on Jupiter: cloud bands resolved cleanly at 120x
and the great red spot transited near midnight. Collimation held after the
car ride. Next target is the Orion nebula once the moon wanes, with the OIII
filter to bring out the teal glow of the trapezium region.
"""

# Replacement content for note 1 in step 2 — a different topic so the semantic
# dedup check (cosine >= 0.95) does not swallow the changed file as a duplicate.
NOTE_1_V2 = """# Quantum error correction primer

A logical qubit is encoded across many physical qubits so that a single
physical error can be detected and corrected without measuring the logical
state. Surface codes are the leading candidate: they need only nearest
neighbor coupling on a 2D grid and tolerate relatively high physical error
rates around one percent. Syndrome measurements are repeated every cycle and
decoded classically to decide which physical qubits flipped.
"""


# --- guard helpers (copied pattern from tests/test_e2e.py) --------------------


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


# --- helpers -------------------------------------------------------------------


def run_sync(vault: str, url: str | None, drop_env: tuple[str, ...] = ()) -> subprocess.CompletedProcess:
    """Run scripts/sync_notes.py --once --dir <vault> as a subprocess from the repo root.
    If url is not None, DATABASE_URL is set to it (pipeline modules read DATABASE_URL)."""
    env = {k: v for k, v in os.environ.items() if k not in drop_env}
    if url is not None:
        env["DATABASE_URL"] = url
    return subprocess.run(
        [sys.executable, str(REPO_ROOT / "scripts" / "sync_notes.py"),
         "--once", "--dir", vault],
        cwd=REPO_ROOT, env=env, capture_output=True, text=True, timeout=900,
    )


def n_actions(stdout: str, prefix: str) -> int:
    return sum(1 for ln in stdout.splitlines() if ln.startswith(prefix))


def count_rows(url: str, prefix: str, extra_where: str = "", extra_params: tuple = ()) -> int:
    with psycopg.connect(url, row_factory=dict_row) as conn:
        row = conn.execute(
            "SELECT count(*) AS n FROM knowledge_items"
            f" WHERE strpos(source_uri, %s) = 1{extra_where}",
            (prefix, *extra_params),
        ).fetchone()
    return row["n"]


# --- steps ---------------------------------------------------------------------


def step_1_first_sync(ctx) -> None:
    vault = ctx["vault"]
    proc = run_sync(vault, ctx["url"])
    assert proc.returncode == 0, (proc.returncode, proc.stdout, proc.stderr)
    assert n_actions(proc.stdout, "ingested") == 3, proc.stdout
    assert n_actions(proc.stdout, "skipped") == 0, proc.stdout
    assert n_actions(proc.stdout, "error") == 0, proc.stdout
    assert count_rows(ctx["url"], vault) == 3
    print("PASS step_1_first_sync (3 files -> 3 ingested, 3 rows)")


def step_2_change_and_add(ctx) -> None:
    vault = ctx["vault"]
    note1 = Path(vault) / "note1.md"
    note1.write_text(NOTE_1_V2, encoding="utf-8")  # modify
    (Path(vault) / "note4.md").write_text(NOTE_4, encoding="utf-8")  # add new
    proc = run_sync(vault, ctx["url"])
    assert proc.returncode == 0, (proc.returncode, proc.stdout, proc.stderr)
    assert n_actions(proc.stdout, "ingested") == 2, proc.stdout  # changed + new
    assert n_actions(proc.stdout, "skipped") == 2, proc.stdout   # 2 unchanged
    assert n_actions(proc.stdout, "error") == 0, proc.stdout
    # append-only: 3 old rows kept + 2 new rows
    assert count_rows(ctx["url"], vault) == 5
    # the modified file now has TWO rows under its source_uri (old version kept)
    abs_note1 = os.path.abspath(str(note1))
    assert count_rows(ctx["url"], abs_note1) == 2
    print("PASS step_2_change_and_add (append-only: changed file -> NEW row,"
          " old row kept; rows 3 -> 5; 2 skipped-unchanged)")


def step_3_no_change_rerun(ctx) -> None:
    proc = run_sync(ctx["vault"], ctx["url"])
    assert proc.returncode == 0, (proc.returncode, proc.stdout, proc.stderr)
    assert n_actions(proc.stdout, "ingested") == 0, proc.stdout
    assert n_actions(proc.stdout, "skipped") == 4, proc.stdout  # all 4 files
    assert n_actions(proc.stdout, "error") == 0, proc.stdout
    assert count_rows(ctx["url"], ctx["vault"]) == 5
    print("PASS step_3_no_change_rerun (idempotent: 0 ingested, 4 skipped, rows stay 5)")


def step_4_guard_paths(ctx) -> None:
    # 4a. refusal: self-subprocess pointed at a NON-test dbname must exit 1
    #     with REFUSING before any destructive op.
    parts = conninfo.conninfo_to_dict(ctx["url"])
    parts["dbname"] = "postgres"
    postgres_url = conninfo.make_conninfo(**parts)
    env = dict(os.environ)
    env["TEST_DATABASE_URL"] = postgres_url
    proc = subprocess.run(
        [sys.executable, str(Path(__file__).resolve())],
        cwd=REPO_ROOT, env=env, capture_output=True, text=True, timeout=120,
    )
    assert proc.returncode == 1, (proc.returncode, proc.stdout, proc.stderr)
    assert "REFUSING" in proc.stdout, proc.stdout

    # 4b. SKIP: self-subprocess without TEST_DATABASE_URL exits 0 with SKIP.
    env = {k: v for k, v in os.environ.items() if k != "TEST_DATABASE_URL"}
    proc = subprocess.run(
        [sys.executable, str(Path(__file__).resolve())],
        cwd=REPO_ROOT, env=env, capture_output=True, text=True, timeout=120,
    )
    assert proc.returncode == 0, (proc.returncode, proc.stdout, proc.stderr)
    assert SKIP_MSG in proc.stdout, proc.stdout

    # 4c. sync script without DATABASE_URL exits 1 (before touching any DB).
    proc = run_sync(ctx["vault"], url=None, drop_env=("DATABASE_URL",))
    assert proc.returncode == 1, (proc.returncode, proc.stdout, proc.stderr)
    assert "DATABASE_URL" in proc.stderr, proc.stderr

    print("PASS step_4_guard_paths (REFUSING exit 1 on /postgres; SKIP exit 0"
          " without TEST_DATABASE_URL; sync exit 1 without DATABASE_URL)")


def cleanup(ctx) -> None:
    vault = ctx.get("vault")
    if vault:
        with psycopg.connect(ctx["url"], row_factory=dict_row) as conn:
            conn.execute(
                "DELETE FROM knowledge_items WHERE strpos(source_uri, %s) = 1",
                (vault,),
            )
            remaining = conn.execute(
                "SELECT count(*) AS n FROM knowledge_items WHERE strpos(source_uri, %s) = 1",
                (vault,),
            ).fetchone()["n"]
        assert remaining == 0
        shutil.rmtree(vault, ignore_errors=True)
        print("PASS cleanup (test rows deleted, temp vault removed)")


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

    # pipeline/ reads DATABASE_URL — point it at the test DB
    os.environ["DATABASE_URL"] = url

    apply_schema(url)
    print("PASS setup_schema (db/schema.sql applied to test DB)")

    vault = tempfile.mkdtemp(prefix="notes_sync_test_")
    (Path(vault) / "note1.md").write_text(NOTE_1, encoding="utf-8")
    (Path(vault) / "note2.md").write_text(NOTE_2, encoding="utf-8")
    (Path(vault) / "note3.md").write_text(NOTE_3, encoding="utf-8")
    vault = os.path.abspath(vault)

    ctx = {"url": url, "vault": vault}
    try:
        step_1_first_sync(ctx)
        step_2_change_and_add(ctx)
        step_3_no_change_rerun(ctx)
        step_4_guard_paths(ctx)
    finally:
        cleanup(ctx)
    print("All notes sync tests passed.")


if __name__ == "__main__":
    main()
