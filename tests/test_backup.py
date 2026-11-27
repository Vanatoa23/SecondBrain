"""Backup + restore rehearsal test against a REAL PostgreSQL + pgvector server.

Guard pattern (locked — copied from tests/test_e2e.py):
  - TEST_DATABASE_URL only, never a DATABASE_URL fallback.
  - Refuses (exit 1) any dbname not matching (^|_)test(_|$) before destructive ops.
  - Prints SKIP and exits 0 when TEST_DATABASE_URL is unset or the server is
    unreachable.

The rehearsal uses TWO dedicated throwaway databases on the same server:
  backup_test   — source (TEST_DATABASE_URL must point at it)
  restore_test  — restore target
Both are dropped-if-exists and recreated at setup, and dropped again at
cleanup, so re-runs are idempotent. Databases are created via a psycopg
admin connection to .../postgres with a (^|_)test(_|$) guard on every name.

    TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:5433/backup_test \
        python tests/test_backup.py

Client tools: the test needs pg_dump/pg_restore whose major version matches
the server (a pg_dump 18 archive contains `SET transaction_timeout`, which
servers < 17 reject during restore). It uses PATH, then a well-known native
install dir, then — on Windows boxes where the matching client lives in WSL —
auto-generated shim wrappers that exec the WSL client (see find_pg_bin).

All SQL is parameterized; identifiers go through psycopg.sql.Identifier after
the "test" guard check.

"""

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

SKIP_MSG = "SKIP: no test database available (set TEST_DATABASE_URL)"

SOURCE_DBNAME_FALLBACK = "backup_test"
RESTORE_DBNAME = "restore_test"
ADMIN_DBNAME = "postgres"

# Marker term that occurs only in doc A — proves FTS survived the round trip.
MARKER = "heliotrope"
DOC_A = ("The heliotrope marker proves that a logical pg_dump restores the "
         "knowledge_items table together with its full-text search index.")
DOC_B = ("Backup restore rehearsals should run weekly against throwaway "
         "databases, never against the real production database.")


# --- helpers (guard pattern from tests/test_e2e.py) ---------------------------

def get_test_url() -> str | None:
    # TEST_DATABASE_URL only — never fall back to DATABASE_URL.
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


def with_dbname(url: str, new_name: str) -> str:
    d = {k: v for k, v in conninfo.conninfo_to_dict(url).items() if v not in (None, "")}
    d["dbname"] = new_name
    return conninfo.make_conninfo(**d)


def db_reachable(url: str) -> bool:
    try:
        conn = psycopg.connect(url, connect_timeout=5)
        conn.close()
        return True
    except Exception:
        return False


def guard_dbname(name: str) -> None:
    """Hard guard: only dbnames matching (^|_)test(_|$) (case-insensitive) may be touched."""
    if not is_test_dbname(name):
        print(f"REFUSING to touch database {name!r}: dbname must match (^|_)test(_|$) "
              "(data-safety guard — this test drops and creates databases).")
        sys.exit(1)


# --- pg_dump / pg_restore discovery -------------------------------------------

def server_major(admin_url: str) -> int:
    with psycopg.connect(admin_url) as conn:
        v = conn.execute("SELECT current_setting('server_version_num')").fetchone()[0]
    return int(v) // 10000


def _tool_major(exe: str) -> int | None:
    try:
        out = subprocess.run([exe, "--version"], capture_output=True, text=True, timeout=30)
        m = re.search(r"(\d+)\.", out.stdout)
        return int(m.group(1)) if m else None
    except Exception:
        return None


def _to_posix(d: str) -> str:
    """C:\\foo\\bar -> /c/foo/bar (for prepending to a Git Bash subprocess PATH)."""
    m = re.match(r"^([A-Za-z]):[\\/](.*)$", d)
    if m:
        return "/" + m.group(1).lower() + "/" + m.group(2).replace("\\", "/")
    return d


def _native_pg_dirs() -> list[str]:
    import glob
    drive = os.environ.get("SystemDrive", "C:")
    candidates: list[str] = []
    for pat in (f"{drive}/Program Files/PostgreSQL/*/bin",
                f"{drive}/Program Files (x86)/PostgreSQL/*/bin",
                "/usr/lib/postgresql/*/bin",
                "/opt/homebrew/opt/postgresql@*/bin",
                "/usr/local/opt/postgresql@*/bin"):
        candidates.extend(sorted(glob.glob(pat), reverse=True))
    out = []
    for d in candidates:
        exe = "pg_dump.exe" if (Path(d) / "pg_dump.exe").exists() else "pg_dump"
        if (Path(d) / exe).exists() and (Path(d) / exe.replace("pg_dump", "pg_restore")).exists():
            out.append(d)
    return out


def _wsl_distros() -> list[str]:
    wsl = shutil.which("wsl")
    if not wsl:
        return []
    try:
        raw = subprocess.run([wsl, "-l", "-q"], capture_output=True, timeout=60).stdout
        text = raw.decode("utf-16-le", errors="ignore") if b"\x00" in raw[:40] else raw.decode(errors="ignore")
        return [d.strip() for d in text.splitlines()
                if d.strip() and "default" not in d.lower()]
    except Exception:
        return []


def _find_wsl_pg(server_mj: int):
    """Return (distro) whose /usr/bin/pg_dump+pg_restore match the server major."""
    wsl = shutil.which("wsl")
    if not wsl:
        return None
    for d in _wsl_distros():
        try:
            ok = subprocess.run([wsl, "-d", d, "--", "test", "-x", "/usr/bin/pg_dump"],
                                capture_output=True, timeout=60).returncode == 0
            if not ok:
                continue
            mj = _tool_major_wsl(wsl, d, "/usr/bin/pg_restore")
            if mj == server_mj:
                return d
        except Exception:
            continue
    return None


def _tool_major_wsl(wsl: str, distro: str, tool: str) -> int | None:
    try:
        out = subprocess.run([wsl, "-d", distro, "--", tool, "--version"],
                             capture_output=True, text=True, timeout=60)
        m = re.search(r"(\d+)\.", out.stdout)
        return int(m.group(1)) if m else None
    except Exception:
        return None


_SHIM_TEMPLATE = """#!/usr/bin/env bash
# Auto-generated by tests/test_backup.py — run the WSL PostgreSQL client,
# converting MSYS/Windows path arguments to /mnt/<drive>/... form.
export MSYS_NO_PATHCONV=1
args=()
for a in "$@"; do
  case "$a" in
    [A-Za-z]:[\\\\/]*) d="${a:0:1}"; rest="${a:3}"; args+=("/mnt/$(echo "$d" | tr 'A-Z' 'a-z')/${rest//\\\\//}") ;;
    /[a-z]/?*) args+=("/mnt/${a:1:1}${a:2}") ;;
    *) args+=("$a") ;;
  esac
done
exec wsl -d @DISTRO@ -- /usr/bin/@TOOL@ "${args[@]}"
"""


def make_wsl_shims(shim_dir: Path, distro: str) -> None:
    shim_dir.mkdir(parents=True, exist_ok=True)
    for tool in ("pg_dump", "pg_restore"):
        p = shim_dir / tool
        p.write_text(_SHIM_TEMPLATE.replace("@DISTRO@", distro).replace("@TOOL@", tool))
        p.chmod(0o755)


def find_pg_bin(admin_url: str, shim_dir: Path) -> str | None:
    """POSIX-style dir containing pg_dump/pg_restore matching the server major.

    Returns None when both tools are already on PATH. Preference order:
    native install dir with matching major -> WSL client with matching major
    (via generated shims) -> any native dir.
    """
    if shutil.which("pg_dump") and shutil.which("pg_restore"):
        return None  # already on PATH
    mj = server_major(admin_url)
    for d in _native_pg_dirs():
        exe = "pg_dump.exe" if (Path(d) / "pg_dump.exe").exists() else "pg_dump"
        if _tool_major(str(Path(d) / exe)) == mj:
            return _to_posix(d)
    distro = _find_wsl_pg(mj)
    if distro:
        make_wsl_shims(shim_dir, distro)
        return _to_posix(str(shim_dir))
    dirs = _native_pg_dirs()
    return _to_posix(dirs[0]) if dirs else None


def script_env(pg_bin: str | None) -> dict:
    env = os.environ.copy()
    if pg_bin:
        env["PATH"] = pg_bin + os.pathsep + env.get("PATH", "")
    env.pop("ALLOW_NON_TEST_RESTORE", None)  # guard tests must not inherit a bypass
    return env


def run_script(pg_bin: str | None, script: str, *args: str, extra_env: dict | None = None):
    env = script_env(pg_bin)
    if extra_env:
        env.update(extra_env)
    proc = subprocess.run(
        ["bash", str(REPO_ROOT / "scripts" / script), *args],
        capture_output=True, text=True, cwd=REPO_ROOT, env=env,
    )
    if proc.stdout.strip():
        print(f"  [{script} stdout] " + proc.stdout.strip().replace("\n", "\n  "))
    if proc.stderr.strip():
        print(f"  [{script} stderr] " + proc.stderr.strip().replace("\n", "\n  "))
    return proc


def apply_schema(url: str) -> None:
    """Idempotently apply db/schema.sql (drops the two tables first)."""
    schema_sql = (REPO_ROOT / "db" / "schema.sql").read_text()
    with psycopg.connect(url) as conn:
        conn.execute("DROP TABLE IF EXISTS relationships CASCADE")
        conn.execute("DROP TABLE IF EXISTS knowledge_items CASCADE")
        conn.execute(schema_sql)  # no params -> simple protocol, multi-statement ok


def ensure_database(admin_conn, name: str) -> None:
    """Drop-if-exists + create a throwaway database (name must contain 'test')."""
    guard_dbname(name)
    admin_conn.execute(
        sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name))
    )
    admin_conn.execute(
        sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name))
    )


def drop_database(admin_conn, name: str) -> None:
    guard_dbname(name)
    admin_conn.execute(
        sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name))
    )


def count_items(url: str) -> int:
    with psycopg.connect(url, row_factory=dict_row) as conn:
        return conn.execute("SELECT count(*) AS n FROM knowledge_items").fetchone()["n"]


# --- main ---------------------------------------------------------------------

def main() -> None:
    url = get_test_url()
    if not url:
        print(SKIP_MSG)
        sys.exit(0)

    source_name = db_name(url) or SOURCE_DBNAME_FALLBACK
    guard_dbname(source_name)          # refuse before any destructive op
    guard_dbname(RESTORE_DBNAME)       # restore target must be a test db too
    # ADMIN_DBNAME ("postgres") is only used for the admin connection (CREATE/DROP
    # DATABASE of the two *test* DBs) and as the refusal-check target — never written.

    source_url = with_dbname(url, source_name)
    restore_url = with_dbname(url, RESTORE_DBNAME)
    admin_url = with_dbname(url, ADMIN_DBNAME)
    non_test_url = admin_url           # .../postgres — used for the refusal check

    # The throwaway source DB does not exist yet on a first run (this test creates
    # it), so reachability is probed on the server via the admin DB, not the source.
    if not db_reachable(admin_url):
        print(SKIP_MSG)
        print(f"(connect to {_safe_db_label(admin_url)} failed)")
        sys.exit(0)

    pg_bin = find_pg_bin(admin_url, shim_dir := Path(tempfile.mkdtemp(prefix="secondbrain_pgshim_")))
    if shutil.which("pg_dump") is None and pg_bin is None:
        print("ERROR: pg_dump/pg_restore not found (PATH, native install dirs, or WSL) — "
              "install PostgreSQL client tools matching the server major version.")
        sys.exit(1)
    if pg_bin:
        print(f"setup: pg client dir = {pg_bin}")

    tmpdir = Path(tempfile.mkdtemp(prefix="secondbrain_backup_test_"))
    print(f"setup: source={_safe_db_label(source_url)} restore={_safe_db_label(restore_url)}")
    print(f"setup: backup dir={tmpdir}")

    try:
        # --- setup: two throwaway DBs, drop-if-exists for idempotency ---
        with psycopg.connect(admin_url, autocommit=True) as admin:
            ensure_database(admin, source_name)
            ensure_database(admin, RESTORE_DBNAME)
        print(f"PASS setup_databases (drop-if-exists + create {source_name}, {RESTORE_DBNAME})")

        apply_schema(source_url)
        print(f"PASS setup_schema_source (db/schema.sql applied to {source_name})")
        apply_schema(restore_url)
        print(f"PASS setup_schema_restore (empty schema applied to {RESTORE_DBNAME})")

        # pipeline/ reads DATABASE_URL — point it at the SOURCE test DB
        os.environ["DATABASE_URL"] = source_url

        # --- ingest 2 small docs via pipeline.ingest ---
        model_ok = True
        try:
            from pipeline.embed import embed_text
            embed_text("warmup")
        except Exception as e:
            model_ok = False
            print(f"SKIP pipeline.ingest via bge-m3 (model unavailable: "
                  f"{type(e).__name__}: {str(e)[:120]}) — inserting via direct SQL")

        if model_ok:
            from pipeline.ingest import ingest
            res_a = ingest("text", DOC_A, source_uri="backup://a", title="Backup rehearsal doc A")
            res_b = ingest("text", DOC_B, source_uri="backup://b", title="Backup rehearsal doc B")
            assert res_a["status"] == "inserted", res_a
            assert res_b["status"] == "inserted", res_b
        else:
            with psycopg.connect(source_url) as conn:
                for uri, title, content in (
                    ("backup://a", "Backup rehearsal doc A", DOC_A),
                    ("backup://b", "Backup rehearsal doc B", DOC_B),
                ):
                    conn.execute(
                        "INSERT INTO knowledge_items (type, title, content, mime_type, source_uri)"
                        " VALUES (%s, %s, %s, %s, %s)",
                        ("note", title, content, "text/plain", uri),
                    )
        assert count_items(source_url) == 2
        print("PASS ingest_2_docs (pipeline.ingest x2, knowledge_items=2)"
              if model_ok else "PASS ingest_2_docs (direct SQL fallback, knowledge_items=2)")

        # --- backup via scripts/backup.sh ---
        proc = run_script(
            pg_bin, "backup.sh",
            extra_env={"BACKUP_SOURCE_URL": source_url, "BACKUP_DIR": tmpdir.as_posix()},
        )
        assert proc.returncode == 0, proc
        dumps = sorted(tmpdir.glob("*.dump"))
        assert len(dumps) == 1, f"expected exactly one .dump in {tmpdir}, found {dumps}"
        dump = dumps[0]
        size = dump.stat().st_size
        assert size > 0
        print(f"PASS backup_dump_created (dump={dump.name}, size_bytes={size})")

        # --- restore via scripts/restore.sh into restore_test ---
        proc = run_script(pg_bin, "restore.sh", dump.as_posix(), restore_url)
        assert proc.returncode == 0, proc
        print(f"PASS restore_ran (restore.sh -> {RESTORE_DBNAME})")

        # --- verify: counts + FTS marker in the restored DB ---
        with psycopg.connect(restore_url, row_factory=dict_row) as conn:
            n = conn.execute("SELECT count(*) AS n FROM knowledge_items").fetchone()["n"]
            assert n == 2, f"expected 2 knowledge_items in restored DB, got {n}"
            uris = {r["source_uri"] for r in conn.execute(
                "SELECT source_uri FROM knowledge_items").fetchall()}
            assert uris == {"backup://a", "backup://b"}, uris
            from retrieval.fts import fts_search
            rows = fts_search(conn, MARKER, top_k=5)
        assert any(r["source_uri"] == "backup://a" for r in rows), rows
        print(f"PASS restore_verified (knowledge_items={n}, fts_search('{MARKER}') "
              f"-> {len(rows)} hit(s), backup://a present)")

        # --- refusal path: restore.sh against a non-test dbname must exit non-zero ---
        def tables_in_pg(target: str) -> int:
            with psycopg.connect(target) as c:
                return c.execute(
                    "SELECT count(*) FROM pg_tables WHERE tablename = %s",
                    ("knowledge_items",),
                ).fetchone()[0]

        before = tables_in_pg(non_test_url)
        proc = run_script(pg_bin, "restore.sh", dump.as_posix(), non_test_url)
        out = (proc.stdout + proc.stderr).lower()
        assert proc.returncode != 0, f"refusal path must exit non-zero, got {proc.returncode}"
        assert "refus" in out, out
        after = tables_in_pg(non_test_url)
        assert before == after, "non-test target was modified despite the refusal!"
        print(f"PASS refusal_non_test_target (exit={proc.returncode}, target db unchanged)")

        # --- G1 regression: conninfo parsing must follow libpq semantics ---
        # Duplicate dbname in keyword conninfo: LAST wins (-> postgres, not
        # "latest"), so the delimited guard must refuse naming 'postgres'.
        # (The old sed parsing took the FIRST dbname= and reported 'latest'.)
        proc = run_script(
            pg_bin, "restore.sh", dump.as_posix(),
            "host=127.0.0.1 port=5433 user=postgres dbname=latest dbname=postgres",
        )
        out = proc.stdout + proc.stderr
        assert proc.returncode != 0, f"dup-dbname conninfo must be refused, got {proc.returncode}"
        assert re.search(r"REFUSING to restore into database 'postgres'", out), out
        print("PASS refusal_conninfo_duplicate_dbname_last_wins (keyword form -> 'postgres' refused)")

        # URI form where the query param overrides the path dbname
        # (postgresql://.../x?dbname=postgres -> libpq resolves 'postgres').
        proc = run_script(
            pg_bin, "restore.sh", dump.as_posix(),
            "postgresql://postgres@127.0.0.1:5433/x?dbname=postgres",
        )
        out = proc.stdout + proc.stderr
        assert proc.returncode != 0, f"URI ?dbname= override must be refused, got {proc.returncode}"
        assert re.search(r"REFUSING to restore into database 'postgres'", out), out
        after = tables_in_pg(non_test_url)
        assert before == after, "non-test target was modified despite the refusal!"
        print("PASS refusal_conninfo_uri_query_dbname (URI ?dbname=postgres -> refused, target unchanged)")

    finally:
        # --- cleanup: delete backup:// rows, drop both test DBs, remove temp dirs ---
        try:
            with psycopg.connect(source_url, row_factory=dict_row) as conn:
                conn.execute("DELETE FROM knowledge_items WHERE source_uri LIKE %s",
                             ("backup://%",))
                remaining = conn.execute(
                    "SELECT count(*) AS n FROM knowledge_items WHERE source_uri LIKE %s",
                    ("backup://%",),
                ).fetchone()["n"]
            assert remaining == 0
            print("PASS cleanup_rows_deleted (backup:// rows removed from source DB)")
        except Exception as e:
            print(f"cleanup_rows note: {type(e).__name__}: {str(e)[:120]}")
        try:
            with psycopg.connect(admin_url, autocommit=True) as admin:
                drop_database(admin, source_name)
                drop_database(admin, RESTORE_DBNAME)
            print(f"PASS cleanup_databases_dropped ({source_name}, {RESTORE_DBNAME})")
        except Exception as e:
            print(f"cleanup_databases note: {type(e).__name__}: {str(e)[:120]}")
        shutil.rmtree(tmpdir, ignore_errors=True)
        shutil.rmtree(shim_dir, ignore_errors=True)
        print(f"PASS cleanup_tmpdirs_removed ({tmpdir}, {shim_dir})")

    print("All backup/restore rehearsal tests passed.")


if __name__ == "__main__":
    main()
