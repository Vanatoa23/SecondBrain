#!/usr/bin/env python
"""export_wiki.py — wiki/Markdown export (Phase 4, ROADMAP "Wiki/Markdown export to git").

Read-only portability view of the knowledge base: exports every
knowledge_items row to a Markdown wiki directory — an index.md table plus
one page per item — so the knowledge stays human-auditable, greppable and
versionable in git. The database remains the system of record; this export
is a derived view (README Part 3: "wiki/ export is a git repo (third copy,
human-readable)", regenerated weekly).

Because the export is designed to be committed to git (README: "Derived
knowledge view: Generated Markdown wiki/ pages to git ... the export IS your
portability insurance"), the wiki/ output directory is intentionally NOT
added to .gitignore — it is a versioned portability layer, not a local-only
build artifact.

Usage (from the repo root):

    python scripts/export_wiki.py [--out wiki] [--base-url ""]

    --out       output directory (default: wiki, relative to the CWD);
                created if missing; every run rewrites all files
    --base-url  prefix prepended to every page link, e.g.
                "https://github.com/<user>/<repo>/blob/main/wiki/"
                (default: "" -> relative links like (<id>.md))

Connection URL comes from the environment ONLY (never a CLI flag, so no
credential can end up in a process list / shell history):

    TEST_DATABASE_URL first (tests point it at a throwaway test DB),
    then DATABASE_URL (real ops use — same convention as scripts/audit_edges.py).

Output layout:

    <out>/index.md   table of ALL items: title (link to page), type, source_uri,
                     created_at, distillation_score, stale flag (update_period
                     vs age — same rule as retrieval.merge.is_stale)
    <out>/<id>.md    one page per item — the filename slug IS the id (uuid), so
                     id reuse can never collide. Frontmatter (id, type,
                     source_uri, created_at, distillation_score, update_period,
                     stale, metadata) + content in a fenced block +
                     relationships (outgoing/incoming with type, strength,
                     direction, context and links to the other page)

Idempotency: every run rewrites index.md and every item page with
deterministic content (rows sorted by created_at, id; no wall-clock
timestamps inside the files), so re-running against an unchanged DB produces
byte-identical files. Items are insert-only in v1, so pages for removed ids
are NOT deleted — files are simply overwritten; if an id were ever reused,
slug=id keeps the page filename unique per id.

Markdown safety: item content is always emitted inside a fenced code block,
and the fence is widened (``` -> ~~~~ -> longer) whenever the content itself
contains a fence, so ``` inside content can never break out of the block.
Table cells escape "|" and newlines. Frontmatter values are JSON-encoded
(valid YAML scalars / flow mappings).

The export is READ-ONLY (SELECTs only), so like audit_edges.py it needs no
test-dbname guard. The connection URL is never printed — only host/dbname,
credentials redacted; exception text is sanitized the same way before printing.

Prints counts (items, relationships, pages, index) and a final
"summary: items=N relationships=M pages=P index=1" line.

Exit codes: 0 = ok, 1 = fatal (bad args, no URL, DB error, write error).

All SQL is parameterized.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

DEFAULT_OUT = "wiki"


# ---------------------------------------------------------------------------
# Connection URL resolution + credential-safe description (audit_edges.py style)
# ---------------------------------------------------------------------------


def resolve_url() -> str:
    url = (os.environ.get("TEST_DATABASE_URL") or "").strip()
    if not url:
        url = (os.environ.get("DATABASE_URL") or "").strip()
    if not url:
        raise RuntimeError(
            "no database URL — set TEST_DATABASE_URL (tests) or DATABASE_URL "
            "(ops), e.g. postgresql://postgres@127.0.0.1:5433/secondbrain"
        )
    return url


def describe_conninfo(url: str) -> str:
    """Redacted description: host + dbname ONLY — the URL may embed credentials."""
    try:
        from psycopg import conninfo
        d = conninfo.conninfo_to_dict(url)
        return f"{d.get('host') or ''}/{d.get('dbname') or ''}"
    except Exception:
        m = re.search(r"://(?:[^@/]*@)?([^/:?]+)(?::\d+)?/([^/?]+)", url)
        if m:
            return f"{m.group(1)}/{m.group(2)}"
        return "<unparseable>"


def sanitize_error(msg: str, url: str, db_desc: str) -> str:
    """Strip credentials that could ride along in an exception message."""
    msg = msg.replace(url, db_desc)
    msg = re.sub(r"password=\S+", "password=<redacted>", msg)
    msg = re.sub(r"://([^@/\s]+)@", "://<redacted>@", msg)
    return msg


# ---------------------------------------------------------------------------
# Rendering helpers (pure — no DB)
# ---------------------------------------------------------------------------


def _is_stale(item: dict, now: datetime) -> bool:
    """Same rule as retrieval.merge.is_stale, kept inline on purpose: importing
    the retrieval package would pull the embedding/reranker stack into a
    read-only exporter. update_period == -1 (or NULL) means never stale;
    otherwise stale when age in seconds exceeds update_period."""
    period = item.get("update_period")
    if period is None or int(period) == -1:
        return False
    created = item.get("created_at")
    if created is None:
        return False
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    return (now - created).total_seconds() > int(period)


def _fence_for(content: str) -> str:
    """Smallest code fence that does not occur anywhere in the content, so a
    fence inside the content can never close the block early."""
    for fence in ("```", "~~~~", "````", "~~~~~", "`````", "~~~~~~"):
        if fence not in content:
            return fence
    run = 7
    while True:
        fence = "`" * run
        if fence not in content:
            return fence
        run += 1


def _cell(text) -> str:
    """Minimal markdown-table escaping: pipes and newlines."""
    if text is None:
        return ""
    return str(text).replace("|", "\\|").replace("\r", " ").replace("\n", " ")


def _fm(value) -> str:
    """Frontmatter scalar: JSON encoding is valid YAML (strings quoted, flow
    mappings for objects, null for None) and deterministic with sort_keys."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _rel_line(rel: dict, other_id, items_by_id: dict, base_url: str) -> str:
    other = items_by_id.get(str(other_id)) or {}
    other_title = other.get("title") or "(untitled)"
    strength = rel.get("strength")
    strength_s = "null" if strength is None else str(float(strength))
    direction = rel.get("direction") or ""
    ctx = rel.get("context")
    ctx_s = f" - {_cell(ctx)}" if ctx else ""
    return (f"- **{_cell(rel['type'])}** (strength {strength_s}, {_cell(direction)}) -> "
            f"[{_cell(other_title)}](<{base_url}{other_id}.md>){ctx_s}")


def render_item_page(item: dict, outgoing: list, incoming: list,
                     items_by_id: dict, base_url: str) -> str:
    created = item.get("created_at")
    created_s = created.isoformat() if created is not None else None
    meta = item.get("metadata")
    if meta is None:
        meta = {}
    title = item.get("title") or "(untitled)"

    head = [
        "---",
        f"id: {_fm(str(item['id']))}",
        f"type: {_fm(item.get('type'))}",
        f"source_uri: {_fm(item.get('source_uri'))}",
        f"created_at: {_fm(created_s)}",
        f"distillation_score: {_fm(item.get('distillation_score'))}",
        f"update_period: {_fm(item.get('update_period'))}",
        f"stale: {_fm(bool(item.get('stale')))}",
        f"metadata: {_fm(meta)}",
        "---",
        "",
        f"# {_cell(title)}",
        "",
        "## Content",
        "",
    ]
    page = "\n".join(head) + "\n"

    content = item.get("content") or ""
    if not content.endswith("\n"):
        content += "\n"
    fence = _fence_for(content)
    page += f"{fence}\n{content}{fence}\n"

    page += "\n## Relationships\n\n### Outgoing\n\n"
    if outgoing:
        for r in outgoing:
            page += _rel_line(r, r["to_id"], items_by_id, base_url) + "\n"
    else:
        page += "_None._\n"
    page += "\n### Incoming\n\n"
    if incoming:
        for r in incoming:
            page += _rel_line(r, r["from_id"], items_by_id, base_url) + "\n"
    else:
        page += "_None._\n"
    return page


def render_index(items: list, rel_count: int, base_url: str) -> str:
    out = [
        "# Second Brain - Knowledge Index",
        "",
        "Generated by `scripts/export_wiki.py` from the PostgreSQL knowledge base "
        "(read-only export; the database is the system of record). Do not edit by "
        "hand - re-run the exporter to regenerate.",
        "",
        f"Items: {len(items)} | Relationships: {rel_count}",
        "",
        "| Title | Type | Source URI | Created at | Distillation | Stale |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for it in items:
        title = it.get("title") or "(untitled)"
        created = it.get("created_at")
        created_s = created.isoformat() if created is not None else ""
        score = it.get("distillation_score")
        score_s = "" if score is None else str(float(score))
        stale_s = "yes" if it.get("stale") else "no"
        out.append(f"| [{_cell(title)}](<{base_url}{it['id']}.md>) | {_cell(it.get('type'))} | "
                   f"{_cell(it.get('source_uri'))} | {created_s} | {score_s} | {stale_s} |")
    return "\n".join(out) + "\n"


# ---------------------------------------------------------------------------
# Export (read-only)
# ---------------------------------------------------------------------------


def export_wiki(out_dir: Path, base_url: str, url: str) -> dict:
    """Export all knowledge_items + relationships to out_dir. Returns counts."""
    db_desc = describe_conninfo(url)
    conn = psycopg.connect(url, row_factory=dict_row, connect_timeout=10)
    try:
        with conn:
            items = [dict(r) for r in conn.execute(
                "SELECT id, type, title, content, mime_type, source_uri, source_document_id,"
                " distillation_score, created_at, update_period, metadata"
                " FROM knowledge_items ORDER BY created_at, id"
            ).fetchall()]
            rels = [dict(r) for r in conn.execute(
                "SELECT id, from_id, to_id, type, strength, direction, context, created_at"
                " FROM relationships ORDER BY created_at, id"
            ).fetchall()]
    finally:
        conn.close()

    now = datetime.now(timezone.utc)
    for it in items:
        it["stale"] = _is_stale(it, now)
    items_by_id = {str(it["id"]): it for it in items}

    outgoing: dict[str, list] = {}
    incoming: dict[str, list] = {}
    n_out = n_in = 0
    for r in rels:
        f, t = str(r["from_id"]), str(r["to_id"])
        if f in items_by_id:
            outgoing.setdefault(f, []).append(r)
            n_out += 1
        if t in items_by_id:
            incoming.setdefault(t, []).append(r)
            n_in += 1

    out_dir.mkdir(parents=True, exist_ok=True)
    pages = 0
    for it in items:
        page = render_item_page(it, outgoing.get(str(it["id"]), []),
                                incoming.get(str(it["id"]), []), items_by_id, base_url)
        (out_dir / f"{it['id']}.md").write_text(page, encoding="utf-8")
        pages += 1
    (out_dir / "index.md").write_text(render_index(items, len(rels), base_url),
                                      encoding="utf-8")
    return {
        "db": db_desc,
        "items": len(items),
        "relationships": len(rels),
        "outgoing": n_out,
        "incoming": n_in,
        "pages": pages,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="export_wiki.py",
        description="Export the knowledge base to a Markdown wiki "
                    "(index.md + one page per knowledge item). Read-only.",
    )
    parser.add_argument("--out", default=DEFAULT_OUT,
                        help=f"output directory (default: {DEFAULT_OUT!r}, relative to CWD)")
    parser.add_argument("--base-url", default="",
                        help='prefix prepended to every page link, e.g. '
                             '"https://github.com/<user>/<repo>/blob/main/wiki/" '
                             '(default: "" -> relative links)')
    try:
        args = parser.parse_args(argv)
    except SystemExit as e:
        if e.code == 0:
            raise  # --help
        print("ERROR: invalid arguments (see --help)", file=sys.stderr)
        return 1

    # argument validation BEFORE any DB access
    if not str(args.out).strip():
        print("ERROR: --out must be a non-empty path", file=sys.stderr)
        return 1

    try:
        url = resolve_url()
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    db_desc = describe_conninfo(url)
    out_dir = Path(args.out)

    try:
        stats = export_wiki(out_dir, args.base_url, url)
    except Exception as exc:
        print(f"ERROR: wiki export failed on {db_desc}: "
              f"{type(exc).__name__}: {sanitize_error(str(exc), url, db_desc)}",
              file=sys.stderr)
        return 1

    print(f"wiki-export: db={stats['db']} (credentials redacted)")
    print(f"wiki-export: out={out_dir.resolve()}")
    print(f"wiki-export: items={stats['items']} relationships={stats['relationships']} "
          f"(outgoing={stats['outgoing']} incoming={stats['incoming']})")
    print(f"wiki-export: pages_written={stats['pages']} index_written=1")
    print(f"summary: items={stats['items']} relationships={stats['relationships']} "
          f"pages={stats['pages']} index=1")
    return 0


if __name__ == "__main__":
    sys.exit(main())
