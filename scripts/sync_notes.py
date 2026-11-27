"""Markdown notes vault sync — ingest *.md files from a local vault into the
Second Brain knowledge base (one-shot or watch mode).

Usage (from the repo root):

    python scripts/sync_notes.py [--dir NOTES_DIR] [--once | --watch]

NOTES_DIR resolution order: --dir flag > $NOTES_DIR > <repo>/notes

Behaviour:
  * Scans *.md recursively under NOTES_DIR.
  * For each file, sha = pipeline.dedup.content_hash(text) (sha256 of the
    normalized file text). If a knowledge_items row already exists with
    source_uri = <abs file path> AND metadata->>'document_hash' = sha, the
    file is skipped as unchanged.
  * Otherwise the file is ingested via pipeline.ingest.ingest("text", ...,
    source_uri=<abs file path>). Ingest's dedup compares source_uri +
    whole-document hash: an unchanged file is a duplicate, a CHANGED file
    has a different document_hash under the same source_uri and is inserted
    as a NEW row (append-only versioning — the old row is never modified or
    deleted).
  * --watch re-scans every 60 seconds; Ctrl-C stops.

Per-file actions are printed as lines starting with one of:
    ingested | skipped-unchanged | skipped-duplicate | skipped-empty | error
followed by a "summary: ingested=N skipped=M errors=K" line.

Exit codes: 0 = scan completed (per-file errors, if any, are counted in the
summary and still yield exit 0 in --watch; in --once mode any error -> 1),
1 = fatal error (DATABASE_URL missing/unreachable, notes dir missing).

The default vault dir <repo>/notes is meant to stay local: only notes/.gitkeep
is tracked in git; keep real notes out of the repo by convention (e.g. add
"notes/" to your global git excludes).

All SQL is parameterized.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from pipeline.db import get_connection, get_database_url  # noqa: E402
from pipeline.dedup import content_hash  # noqa: E402
from pipeline.ingest import ingest  # noqa: E402

WATCH_INTERVAL_S = 60


def resolve_notes_dir(cli_dir: str | None) -> Path:
    """--dir > $NOTES_DIR > <repo>/notes"""
    return Path(cli_dir or os.environ.get("NOTES_DIR") or (REPO_ROOT / "notes"))


def find_md_files(notes_dir: Path) -> list[Path]:
    return sorted(p for p in notes_dir.rglob("*.md") if p.is_file())


def _is_synced(conn, abs_path: str, sha: str) -> bool:
    """True when a row already exists for this exact file path AND its stored
    whole-document hash matches — i.e. the file is unchanged since the last
    sync. Parameterized; read-only."""
    row = conn.execute(
        "SELECT 1 FROM knowledge_items"
        " WHERE source_uri = %s AND metadata->>'document_hash' = %s LIMIT 1",
        (abs_path, sha),
    ).fetchone()
    return row is not None


def sync_file(path: Path) -> str:
    """Sync one markdown file. Returns the action line (without the path)."""
    abs_path = os.path.abspath(str(path))
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError as e:
        return f"error: {e}"
    if not text.strip():
        return "skipped-empty"
    sha = content_hash(text)
    try:
        with get_connection() as conn:
            if _is_synced(conn, abs_path, sha):
                return "skipped-unchanged"
        # Ingest with the absolute file path as source_uri: pipeline dedup
        # treats same-uri + same document_hash as duplicate (skipped above or
        # "skipped-duplicate" here) and same-uri + different document_hash as
        # a NEW VERSION — a changed note is inserted as a NEW row, keeping
        # versioning append-only (old row untouched).
        res = ingest("text", text, source_uri=abs_path, title=path.name)
        status = res.get("status")
        if status == "inserted" and res.get("node_ids"):
            return f"ingested (nodes={len(res['node_ids'])})"
        if status == "duplicate":
            return "skipped-duplicate"
        if status == "empty":
            return "skipped-empty"
        return f"error: unexpected ingest status {status!r}"
    except Exception as e:
        return f"error: {type(e).__name__}: {e}"


def sync_once(notes_dir: Path) -> dict:
    files = find_md_files(notes_dir)
    counts = {"ingested": 0, "skipped": 0, "errors": 0}
    print(f"scan: {len(files)} markdown file(s) under {notes_dir}")
    for path in files:
        action = sync_file(path)
        if action.startswith("ingested"):
            counts["ingested"] += 1
            print(f"{action} {path}")
        elif action.startswith("error"):
            counts["errors"] += 1
            print(f"{action} {path}")
        else:
            counts["skipped"] += 1
            print(f"{action} {path}")
    print(
        f"summary: ingested={counts['ingested']}"
        f" skipped={counts['skipped']} errors={counts['errors']}"
    )
    return counts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Sync a Markdown notes vault into the Second Brain knowledge base."
    )
    parser.add_argument("--dir", default=None,
                        help="notes vault dir (default: $NOTES_DIR or <repo>/notes)")
    parser.add_argument("--once", action="store_true",
                        help="single scan and exit (default when --watch is absent)")
    parser.add_argument("--watch", action="store_true",
                        help=f"re-scan every {WATCH_INTERVAL_S}s until Ctrl-C")
    args = parser.parse_args(argv)

    notes_dir = resolve_notes_dir(args.dir)
    if not notes_dir.is_dir():
        print(f"error: notes dir {notes_dir} does not exist", file=sys.stderr)
        return 1
    try:
        get_database_url()
    except RuntimeError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    try:
        with get_connection() as conn:
            conn.execute("SELECT 1")
    except Exception as e:
        print(f"error: cannot connect to DATABASE_URL: {e}", file=sys.stderr)
        return 1

    print(
        f"vault: {notes_dir} ({'watch' if args.watch else 'once'} mode;"
        " local notes stay out of git by convention — only notes/.gitkeep is tracked)"
    )
    if args.watch:
        print(f"watch: rescanning every {WATCH_INTERVAL_S}s — Ctrl-C to stop")
        try:
            while True:
                sync_once(notes_dir)
                time.sleep(WATCH_INTERVAL_S)
        except KeyboardInterrupt:
            print("stopped.")
            return 0
    counts = sync_once(notes_dir)
    return 1 if counts["errors"] else 0


if __name__ == "__main__":
    sys.exit(main())
