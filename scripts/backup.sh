#!/usr/bin/env bash
# Second Brain — logical backup of a Postgres database (pg_dump custom format).
#
# Usage:
#   BACKUP_SOURCE_URL=postgresql://postgres@127.0.0.1:5433/secondbrain \
#       ./scripts/backup.sh
#   # or via DATABASE_URL; output dir via BACKUP_DIR (default ./backups)
#
# Writes  $BACKUP_DIR/secondbrain_YYYYMMDD_HHMMSS.dump  and prints path + size.
#
# NOTE (README.md Part 3): a logical pg_dump is portable insurance, NOT
# point-in-time recovery. PITR needs continuous WAL archiving + physical
# base backups — see scripts/README.md and
# https://www.postgresql.org/docs/18/continuous-archiving.html
#
# The backup is read-only, so no test-dbname guard is needed here; the script
# only refuses to run when no source URL is set.
set -euo pipefail

# Locate pg_dump: PATH first, then well-known install dirs (Windows/macOS/Linux).
if ! command -v pg_dump >/dev/null 2>&1; then
  for d in "/c/Program Files/PostgreSQL"/*/bin \
           "/c/Program Files (x86)/PostgreSQL"/*/bin \
           /usr/lib/postgresql/*/bin \
           /opt/homebrew/opt/postgresql@*/bin \
           /usr/local/opt/postgresql@*/bin; do
    if [ -x "$d/pg_dump" ] || [ -x "$d/pg_dump.exe" ]; then
      PATH="$d:$PATH"
      export PATH
      break
    fi
  done
fi

SRC="${BACKUP_SOURCE_URL:-${DATABASE_URL:-}}"
if [ -z "$SRC" ]; then
  echo "ERROR: no source database URL — set BACKUP_SOURCE_URL (or DATABASE_URL)." >&2
  exit 1
fi
if ! command -v pg_dump >/dev/null 2>&1; then
  echo "ERROR: pg_dump not found in PATH (install PostgreSQL client tools)." >&2
  exit 1
fi

BACKUP_DIR="${BACKUP_DIR:-./backups}"
mkdir -p "$BACKUP_DIR"

# Redacted source description: host + dbname ONLY — the full source URL may
# embed credentials (postgresql://user:password@host/db) and must never be
# echoed. Parsed with libpq semantics via psycopg (a project dep); falls back
# to sed extraction for URI / keyword forms when python is unavailable.
describe_conninfo() {
  local ci="$1" out
  if command -v python >/dev/null 2>&1; then
    if out="$(python -c 'import sys; from psycopg import conninfo; d = conninfo.conninfo_to_dict(sys.argv[1]); print(str(d.get("host") or "") + "/" + str(d.get("dbname") or ""))' "$ci" 2>/dev/null)" && [ -n "$out" ]; then
      printf '%s' "$out"
      return 0
    fi
  fi
  # sed fallback: URI form postgresql://[user[:pass]@]host[:port]/dbname?params
  case "$ci" in
    *://*)
      printf '%s' "$ci" \
        | sed -e 's#^[a-zA-Z0-9+.-]*://##' -e 's#^[^@/]*@##' \
        | awk '{ split($0, a, "/"); split(a[1], h, ":"); db = a[2]; sub(/[?].*/, "", db); print h[1] "/" (db == "" ? "?" : db) }'
      ;;
    *)
      # keyword form: host=... dbname=...
      printf '%s' "$ci" | tr ' ' '\n' \
        | awk -F= '/^(host|dbname)=/ { if ($1=="host") h=$2; else d=$2 } END { print (h==""?"?":h) "/" (d==""?"?":d) }'
      ;;
  esac
}

SRC_DESC="$(describe_conninfo "$SRC")"

DUMP="$BACKUP_DIR/secondbrain_$(date +%Y%m%d_%H%M%S).dump"

echo "backup: source   = $SRC_DESC (host/dbname only — credentials redacted)"
echo "backup: output   = $DUMP"
pg_dump -Fc -f "$DUMP" "$SRC"

SIZE_BYTES=$(wc -c < "$DUMP" | tr -d ' ')
echo "backup: OK dump=$DUMP size_bytes=$SIZE_BYTES"
if command -v du >/dev/null 2>&1; then
  du -h "$DUMP"
fi
