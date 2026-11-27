#!/usr/bin/env bash
# Second Brain — restore a pg_dump custom-format archive into an EXISTING database.
#
# Usage:
#   ./scripts/restore.sh <dump-file> <target-url>
#   # or: RESTORE_TARGET_URL=postgresql://... ./scripts/restore.sh <dump-file>
#
# Runs:  pg_restore --clean --if-exists --single-transaction --exit-on-error
#          -d "$TARGET" "$DUMP"
#   --clean --if-exists drops the dumped objects before recreating them, so the
#   target should be a throwaway/rehearsal database.
#   --single-transaction wraps the whole restore in ONE transaction (all or
#   nothing) and --exit-on-error aborts on the first error instead of
#   continuing with a half-restored database.
#
# The script NEVER creates or drops databases itself — the target database
# must already exist (e.g. `createdb secondbrain_restore`).
#
# SAFETY GUARD (locked): the target dbname must contain a DELIMITED "test"
# component — grep -qiE '(^|_)test(_|$)', so "secondbrain_test" or "test"
# pass but a bare substring like "latest" does NOT. Otherwise the script
# refuses with exit 1 — unless ALLOW_NON_TEST_RESTORE=1 is set, in which
# case it prints a loud warning and proceeds. This protects the real database
# from a rehearsal mistake (see scripts/README.md).
#
# All operations are logged to stdout.
set -euo pipefail

# Locate pg_restore: PATH first, then well-known install dirs (Windows/macOS/Linux).
if ! command -v pg_restore >/dev/null 2>&1; then
  for d in "/c/Program Files/PostgreSQL"/*/bin \
           "/c/Program Files (x86)/PostgreSQL"/*/bin \
           /usr/lib/postgresql/*/bin \
           /opt/homebrew/opt/postgresql@*/bin \
           /usr/local/opt/postgresql@*/bin; do
    if [ -x "$d/pg_restore" ] || [ -x "$d/pg_restore.exe" ]; then
      PATH="$d:$PATH"
      export PATH
      break
    fi
  done
fi

DUMP="${1:-}"
TARGET="${2:-${RESTORE_TARGET_URL:-}}"

if [ -z "$DUMP" ] || [ -z "$TARGET" ]; then
  echo "usage: restore.sh <dump-file> <target-url>   (or set RESTORE_TARGET_URL)" >&2
  exit 1
fi

# Parse the target dbname with libpq semantics via psycopg (a project dep):
# handles keyword conninfo ("host=... dbname=...") AND URIs, including
# DUPLICATE keys (last wins, exactly like libpq — the old sed parsing took
# the FIRST dbname= and could be fooled) and URI query params (?dbname=...).
# Fails CLOSED: if no python is missing or the conninfo is unparseable, refuse.
# Prefers python3, falls back to python.
if command -v python3 >/dev/null 2>&1; then
  PY=python3
elif command -v python >/dev/null 2>&1; then
  PY=python
else
  echo "ERROR: neither python3 nor python found in PATH — required to parse the target conninfo safely." >&2
  exit 1
fi
if ! dbname="$("$PY" -c 'import sys; from psycopg import conninfo; print(conninfo.conninfo_to_dict(sys.argv[1]).get("dbname", ""))' "$TARGET")"; then
  echo "ERROR: could not parse the target conninfo (refusing, fail-closed)." >&2
  exit 1
fi

# Redacted target description (host/dbname only) — the full conninfo may embed
# a password and must never be echoed.
redacted="$("$PY" -c 'import sys; from psycopg import conninfo; d = conninfo.conninfo_to_dict(sys.argv[1]); print(str(d.get("host") or "") + "/" + str(d.get("dbname") or ""))' "$TARGET" 2>/dev/null)" \
  || redacted="(unparseable conninfo)"

echo "restore: dump     = $DUMP"
echo "restore: target   = $redacted (dbname='$dbname') — full conninfo never echoed"

# --- SAFETY GUARD (locked): refuse non-test targets unless explicitly bypassed ---
# Delimited match: the dbname must have "test" as its own component
# (start/end of name or surrounded by underscores) — a bare substring such
# as "latest" must NOT pass.
if ! printf '%s' "$dbname" | grep -qiE '(^|_)test(_|$)'; then
  if [ "${ALLOW_NON_TEST_RESTORE:-0}" = "1" ]; then
    echo "WARNING: ============================================================" >&2
    echo "WARNING: ALLOW_NON_TEST_RESTORE=1 is set — restoring into NON-TEST" >&2
    echo "WARNING: database '$dbname'. pg_restore --clean will DROP objects" >&2
    echo "WARNING: in that database. Only use this for a deliberate restore." >&2
    echo "WARNING: ============================================================" >&2
  else
    echo "REFUSING to restore into database '$dbname': the name has no delimited 'test' component (^|_)test(_|$)." >&2
    echo "This guard protects the real database from a rehearsal mistake." >&2
    echo "If this really is intended, re-run with ALLOW_NON_TEST_RESTORE=1." >&2
    exit 1
  fi
fi

if [ ! -f "$DUMP" ]; then
  echo "ERROR: dump file not found: $DUMP" >&2
  exit 1
fi
if ! command -v pg_restore >/dev/null 2>&1; then
  echo "ERROR: pg_restore not found in PATH (install PostgreSQL client tools)." >&2
  exit 1
fi

echo "restore: running pg_restore --clean --if-exists --single-transaction --exit-on-error (target DB must already exist)"
pg_restore --clean --if-exists --single-transaction --exit-on-error -d "$TARGET" "$DUMP" 2>&1
echo "restore: OK — restored $DUMP into $redacted"
