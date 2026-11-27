#!/usr/bin/env python
"""curator.py — Phase 4 curator core: staleness rescan, contradiction detection,
gap detection, review queue maintenance, and soft invalidation.

Usage (from the repo root):

    python scripts/curator.py [--queue review_queue.json] [--dry-run]

Connection URL comes from the environment ONLY (never a CLI flag, so no
credential can end up in a process list / shell history):

    TEST_DATABASE_URL first (tests point it at a throwaway test DB),
    then DATABASE_URL (real ops use — same convention as scripts/audit_edges.py).

Steps:

  1. STALENESS RESCAN — items with update_period != -1 whose age exceeds
     update_period are flagged {reason: "stale"}. They already get the soft
     x0.5 penalty at retrieval time (retrieval/merge.py is_stale); the queue
     surfaces them for human review. update_period = -1 means never stale.
  2. CONTRADICTION DETECTION — every relationships row with type='contradicts'
     flags BOTH endpoints {reason: "contradiction", evidence: edge context}.
     Additionally, items sharing one source_uri but carrying different
     metadata.document_hash (multiple versions of one document) linked by a
     'supersedes' or 'contradicts' edge get the OLDER version flagged
     {reason: "superseded"/"contradiction"} for invalidation review.
  3. GAP DETECTION — items with zero relationships (neither from_id nor to_id)
     are flagged {reason: "isolated"}.
  3b. FLAG-QUEUE PICKUP — the quarantine flag queue at audit.FLAG_QUEUE
     (env SECONDBRAIN_FLAG_QUEUE or <repo>/flagged_issues.json, written by
     bridge flag_issue) is loaded when it exists; every entry becomes a
     queue flag {reason: "flagged-issue", id: item_id or entry id, evidence:
     "<description> (severity=..., reporter=..., issue=...)"}. merge_queue
     keys by (id, reason), so these integrate idempotently. When the audit
     module is not importable, this step is skipped. DECISION BACK-FILL: if
     the merged queue has a DECIDED (approved/dismissed) entry for
     (id, "flagged-issue"), the quarantine entry's status is set to that
     decision and the flag queue is written atomically (never in --dry-run).
  4. REVIEW QUEUE — the flags are merged into the queue JSON at --queue
     (default review_queue.json in the CWD):
         {"updated_at": <iso>, "flagged": [{id, title, source_uri, reason,
                                             evidence, status}, ...]}
     Merging keeps existing decisions intact: entries whose status is
     "approved"/"dismissed" are preserved untouched; pending entries are
     re-flagged (evidence refreshed, still pending, never duplicated);
     new flags are appended as pending. The load-merge-write runs under an
     exclusive lock (audit.locked_json_update, atomic write via
     audit.write_json_atomic) so concurrent bridge/curator writers cannot
     tear the file or lose updates (audit-A finding 5). The queue file is a
     RUNTIME artifact (gitignored). --dry-run prints everything and writes
     NOTHING.
  5. INVALIDATION (soft — NEVER deletes) — for each contradiction pair where
     one item is clearly newer (created_at; ties broken by higher
     distillation_score), the older/lower item gets
         metadata.review_status = "invalidated-pending-review"
     via a parameterized UPDATE that merges ONLY into the metadata jsonb —
     content and every other column are never touched. The pipeline stays
     insert-only for ingest; the curator is the sanctioned writer for review
     metadata. Items whose queue entry for the contradiction is already
     decided (approved/dismissed), or that already carry a review_status
     marker, are left alone. In --dry-run the run only reports what WOULD be
     invalidated.
  6. SUMMARY — counts are printed; the connection URL is never printed, only
     host/dbname (credentials redacted, audit_edges.py / backup.sh style).

Exit codes: 0 = ok (including --dry-run), 1 = fatal (bad args, no URL, DB
error, unwritable queue). All SQL is parameterized.

The script deliberately imports only psycopg (no pipeline/retrieval modules,
no model loading) so a curator run costs milliseconds. The staleness check
below mirrors retrieval/merge.py is_stale() exactly.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import re
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

DEFAULT_QUEUE = "review_queue.json"
REVIEW_STATUS_INVALIDATED = "invalidated-pending-review"
DECIDED_STATUSES = ("approved", "dismissed")
VERSION_EDGE_TYPES = ("supersedes", "contradicts")
FLAG_REASON = "flagged-issue"  # reason used for quarantine flag-queue entries

USAGE = ("usage: python scripts/curator.py [--queue review_queue.json] [--dry-run]\n"
         "  reads TEST_DATABASE_URL (tests) or DATABASE_URL (ops) from the env;\n"
         "  scans staleness/contradictions/gaps, merges review_queue.json, merges\n"
         "  the audit flag queue (audit.FLAG_QUEUE) as reason \"flagged-issue\" and\n"
         "  back-fills decided statuses into it, and soft-invalidates clearly-\n"
         "  superseded contradiction losers (metadata only)")


# ---------------------------------------------------------------------------
# Connection URL resolution + credential-safe description (audit_edges style)
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


# ---------------------------------------------------------------------------
# Argument parsing (BEFORE any DB access)
# ---------------------------------------------------------------------------


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="curator.py",
        description="Phase 4 curator: staleness/contradiction/gap scan + review "
                    "queue + soft invalidation (metadata only, never deletes).",
    )
    p.add_argument("--queue", type=str, default=DEFAULT_QUEUE,
                   help=f"review queue JSON path (default {DEFAULT_QUEUE})")
    p.add_argument("--dry-run", action="store_true",
                   help="print what would happen; write neither the queue nor any UPDATE")
    args = p.parse_args(argv)
    if not str(args.queue).strip():
        raise SystemExit("ERROR: --queue must be a non-empty path when given")
    return args


# ---------------------------------------------------------------------------
# Staleness — mirrors retrieval/merge.py is_stale() exactly
# ---------------------------------------------------------------------------


def is_stale_row(row: dict, now: datetime) -> bool:
    """A row is stale if update_period != -1 and its age exceeds update_period."""
    period = row.get("update_period")
    if period is None or period == -1:
        return False
    created = row.get("created_at")
    if created is None:
        return False
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    return (now - created).total_seconds() > period


# ---------------------------------------------------------------------------
# Scans
# ---------------------------------------------------------------------------


def fetch_items(conn) -> list[dict]:
    rows = conn.execute(
        "SELECT id, title, source_uri, update_period, created_at, "
        "       distillation_score, metadata "
        "FROM knowledge_items"
    ).fetchall()
    return [dict(r) for r in rows]


def fetch_contradiction_edges(conn) -> list[dict]:
    rows = conn.execute(
        "SELECT id, from_id, to_id, type, context, created_at FROM relationships "
        "WHERE type = 'contradicts' ORDER BY id"
    ).fetchall()
    return [dict(r) for r in rows]


def fetch_version_groups(conn) -> dict[str, list[dict]]:
    """Items sharing a source_uri with >1 distinct metadata.document_hash,
    grouped by source_uri (excerpts have no document_hash and are excluded)."""
    rows = conn.execute(
        """
        SELECT id, title, source_uri, created_at,
               metadata->>'document_hash' AS doc_hash
        FROM knowledge_items
        WHERE source_uri IS NOT NULL
          AND metadata->>'document_hash' IS NOT NULL
          AND source_uri IN (
              SELECT source_uri FROM knowledge_items
              WHERE metadata->>'document_hash' IS NOT NULL
              GROUP BY source_uri
              HAVING count(DISTINCT metadata->>'document_hash') > 1
          )
        ORDER BY source_uri, created_at
        """
    ).fetchall()
    groups: dict[str, list[dict]] = {}
    for r in rows:
        groups.setdefault(r["source_uri"], []).append(dict(r))
    return groups


def fetch_version_edges(conn, group_ids: list) -> list[dict]:
    """supersedes/contradicts edges whose BOTH endpoints are in group_ids."""
    if not group_ids:
        return []
    rows = conn.execute(
        "SELECT id, from_id, to_id, type, context FROM relationships "
        "WHERE type = ANY(%s) AND from_id = ANY(%s::uuid[]) AND to_id = ANY(%s::uuid[]) "
        "ORDER BY id",
        (list(VERSION_EDGE_TYPES), [str(i) for i in group_ids], [str(i) for i in group_ids]),
    ).fetchall()
    return [dict(r) for r in rows]


def fetch_isolated(conn) -> list[dict]:
    rows = conn.execute(
        """
        SELECT k.id, k.title, k.source_uri
        FROM knowledge_items k
        WHERE NOT EXISTS (
            SELECT 1 FROM relationships r
            WHERE r.from_id = k.id OR r.to_id = k.id
        )
        ORDER BY k.id
        """
    ).fetchall()
    return [dict(r) for r in rows]


def _item_brief(item: dict) -> dict:
    return {
        "id": str(item["id"]),
        "title": item.get("title"),
        "source_uri": item.get("source_uri"),
    }


def scan(conn, now: datetime) -> tuple[list[dict], list[dict], list[dict]]:
    """Run all scans. Returns (flags, contradiction_edges, version_flags)."""
    items = fetch_items(conn)
    by_id = {str(i["id"]): i for i in items}
    flags: list[dict] = []

    # 1) staleness rescan
    for it in items:
        if is_stale_row(it, now):
            age = (now - (it["created_at"].replace(tzinfo=timezone.utc)
                          if it["created_at"].tzinfo is None else it["created_at"])).total_seconds()
            flags.append({
                **_item_brief(it),
                "reason": "stale",
                "evidence": (f"update_period={it['update_period']}s; age={age:.0f}s exceeds "
                             f"the period (created_at={it['created_at'].isoformat()})"),
            })

    # 2a) contradiction edges -> flag both endpoints
    contradiction_edges = fetch_contradiction_edges(conn)
    for edge in contradiction_edges:
        for endpoint, other in ((edge["from_id"], edge["to_id"]),
                                (edge["to_id"], edge["from_id"])):
            item = by_id.get(str(endpoint))
            if item is None:
                continue
            flags.append({
                **_item_brief(item),
                "reason": "contradiction",
                "evidence": (f"relationship {edge['id']} type=contradicts with item "
                             f"{other}; context: {edge.get('context') or '<none>'}"),
            })

    # 2b) multi-version source_uri + supersedes/contradicts edge -> flag older version
    version_flags: list[dict] = []
    for uri, group in fetch_version_groups(conn).items():
        if len(group) < 2:
            continue
        edges = fetch_version_edges(conn, [g["id"] for g in group])
        if not edges:
            continue
        group_by_id = {str(g["id"]): g for g in group}
        for edge in edges:
            a = group_by_id.get(str(edge["from_id"]))
            b = group_by_id.get(str(edge["to_id"]))
            if a is None or b is None or str(edge["from_id"]) == str(edge["to_id"]):
                continue
            older, newer = (a, b) if a["created_at"] <= b["created_at"] else (b, a)
            reason = "superseded" if edge["type"] == "supersedes" else "contradiction"
            flag = {
                **_item_brief(older),
                "reason": reason,
                "evidence": (f"source_uri {uri!r} has multiple versions; relationship "
                             f"{edge['id']} type={edge['type']} links this OLDER version "
                             f"(doc_hash={older.get('doc_hash')}, "
                             f"created_at={older['created_at'].isoformat()}) to item "
                             f"{newer['id']} (doc_hash={newer.get('doc_hash')}); "
                             f"context: {edge.get('context') or '<none>'}"),
            }
            version_flags.append(flag)
            flags.append(flag)

    # 3) gap detection — zero relationships
    for it in fetch_isolated(conn):
        flags.append({
            **_item_brief(it),
            "reason": "isolated",
            "evidence": "zero relationships (item is neither from_id nor to_id of any edge)",
        })

    return flags, contradiction_edges, version_flags


# ---------------------------------------------------------------------------
# Soft invalidation (metadata-only jsonb merge — never content, never delete)
# ---------------------------------------------------------------------------


def decide_invalidations(contradiction_edges: list[dict], items: list[dict],
                        decided: set[tuple[str, str]]) -> list[tuple[str, str, str]]:
    """For each contradiction pair pick the clearly newer item (created_at,
    tie-broken by higher distillation_score) and return the losers as
    (loser_id, winner_id, why). Skips undecidable ties, self-loops, losers
    whose contradiction flag is already decided in the queue, and losers that
    already carry a review_status marker."""
    by_id = {str(i["id"]): i for i in items}
    losers: dict[str, tuple[str, str, str]] = {}
    for edge in contradiction_edges:
        a = by_id.get(str(edge["from_id"]))
        b = by_id.get(str(edge["to_id"]))
        if a is None or b is None or str(edge["from_id"]) == str(edge["to_id"]):
            continue
        ca, cb = a["created_at"], b["created_at"]
        if ca.tzinfo is None:
            ca = ca.replace(tzinfo=timezone.utc)
        if cb.tzinfo is None:
            cb = cb.replace(tzinfo=timezone.utc)
        if ca != cb:
            if ca > cb:
                winner, loser, why = a, b, "loser has the older created_at"
            else:
                winner, loser, why = b, a, "loser has the older created_at"
        else:
            sa = a.get("distillation_score") or 0.0
            sb = b.get("distillation_score") or 0.0
            if sa == sb:
                continue  # no clear winner — leave for human review
            if sa > sb:
                winner, loser, why = a, b, "loser has the lower distillation_score"
            else:
                winner, loser, why = b, a, "loser has the lower distillation_score"
        lid = str(loser["id"])
        if (lid, "contradiction") in decided:
            continue  # human already decided this flag — respect it
        if (loser.get("metadata") or {}).get("review_status"):
            continue  # an existing review marker is never overwritten
        losers[lid] = (lid, str(winner["id"]), why)
    return list(losers.values())


def apply_invalidations(conn, invalidations: list[tuple[str, str, str]]) -> int:
    """Parameterized UPDATE merging ONLY into metadata jsonb. Returns count."""
    applied = 0
    for loser_id, _winner_id, _why in invalidations:
        conn.execute(
            "UPDATE knowledge_items "
            "SET metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb "
            "WHERE id = %s",
            (json.dumps({"review_status": REVIEW_STATUS_INVALIDATED}), loser_id),
        )
        applied += 1
    return applied


# ---------------------------------------------------------------------------
# Review queue load / merge / atomic write
# ---------------------------------------------------------------------------


def load_queue(queue_path: Path) -> dict:
    if not queue_path.exists():
        return {"updated_at": None, "flagged": []}
    try:
        data = json.loads(queue_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        raise RuntimeError(
            f"existing queue file {queue_path} is not readable JSON ({exc}) — "
            "refusing to overwrite it; fix or move it manually"
        )
    if not isinstance(data, dict) or not isinstance(data.get("flagged"), list):
        raise RuntimeError(
            f"existing queue file {queue_path} has an unexpected shape "
            "(expected {\"updated_at\": ..., \"flagged\": [...]}) — refusing to overwrite"
        )
    return data


def merge_queue(existing: dict, new_flags: list[dict], now: datetime) -> dict:
    """Merge new flags into the existing queue.

    Keyed by (id, reason): existing entries keep their status (decisions
    approved/dismissed stay intact; pending entries are re-flagged — evidence
    refreshed, still pending, never duplicated); brand-new flags append as
    pending. Entries not re-flagged this run are kept as-is.
    """
    flagged: list[dict] = []
    index: dict[tuple[str, str], dict] = {}
    for entry in existing.get("flagged", []):
        if not isinstance(entry, dict):
            continue
        eid, reason = str(entry.get("id")), str(entry.get("reason"))
        if eid in ("None", "") or not reason:
            continue
        entry.setdefault("status", "pending")
        key = (eid, reason)
        if key in index:
            continue  # drop accidental duplicates from an older merge
        index[key] = entry
        flagged.append(entry)
    for flag in new_flags:
        key = (str(flag["id"]), flag["reason"])
        if key in index:
            entry = index[key]
            # re-flag: refresh the descriptive fields, NEVER touch the status
            entry["title"] = flag.get("title")
            entry["source_uri"] = flag.get("source_uri")
            entry["evidence"] = flag.get("evidence")
        else:
            entry = {**flag, "status": "pending"}
            index[key] = entry
            flagged.append(entry)
    return {"updated_at": now.isoformat(), "flagged": flagged}


def write_queue_atomic(queue_path: Path, queue: dict) -> None:
    """Atomic queue write: audit.write_json_atomic (unique tmp + fsync + 0600)
    when the audit module is available (audit-A finding 5); otherwise a local
    fallback that still uses a UNIQUE tmp name + fsync before os.replace, so
    concurrent writers can never tear the file or clobber each other's tmp."""
    audit = _load_audit()
    if audit is not None and hasattr(audit, "write_json_atomic"):
        audit.write_json_atomic(str(queue_path), queue)
        return
    payload = json.dumps(queue, indent=2, default=str, ensure_ascii=False) + "\n"
    tmp = queue_path.with_name(
        f"{queue_path.name}.tmp.{os.getpid()}.{uuid.uuid4().hex[:8]}"
    )
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(payload)
        fh.flush()
        os.fsync(fh.fileno())
    os.chmod(tmp, 0o600)
    os.replace(tmp, queue_path)


def _locked_json_update_local(path: Path, mutator, default_factory):
    """Local stand-in for audit.locked_json_update (used only when the audit
    module — or its locked_json_update — is unavailable): an exclusive lock
    file guards load -> mutator -> atomic write, so concurrent bridge/curator
    writers cannot tear or lose updates. A corrupt EXISTING file raises
    RuntimeError (refuse-on-corrupt, same as audit.load_json_file)."""
    path = Path(path)
    lock_path = path.with_name(path.name + ".lock")
    deadline = time.time() + 10.0
    while True:
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            try:
                os.write(fd, str(os.getpid()).encode("utf-8"))
            finally:
                os.close(fd)
            break
        except FileExistsError:
            if time.time() >= deadline:
                raise RuntimeError(
                    f"timed out acquiring lock {lock_path} — another writer holds it"
                ) from None
            time.sleep(0.02)
    try:
        if path.exists():
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError) as exc:
                raise RuntimeError(
                    f"existing file {path} is not readable JSON ({exc}) — "
                    "refusing to overwrite it; fix or move it manually"
                ) from None
            if not isinstance(data, dict):
                raise RuntimeError(
                    f"existing file {path} has an unexpected shape — refusing to overwrite it"
                ) from None
        else:
            data = default_factory()
        mutator(data)
        write_queue_atomic(path, data)
        yield data
    finally:
        try:
            os.unlink(lock_path)
        except OSError:
            pass


def _locked_json_update(path, mutator, default_factory):
    """Locked load-merge-write context manager (audit-A finding 5).

    Uses audit.locked_json_update (the locked Phase 5 API: exclusive lock +
    refuse-on-corrupt load + mutator + atomic write) when the audit module
    provides it; otherwise falls back to the local equivalent
    (_locked_json_update_local). Usage:

        with _locked_json_update(path, mutator, default_factory) as data:
            ...  # mutator has been applied under the lock; data is the result
    """
    audit = _load_audit()
    if audit is not None:
        fn = getattr(audit, "locked_json_update", None)
        if fn is not None:
            return fn(str(path), mutator, default_factory)
    return _locked_json_update_local(Path(path), mutator, default_factory)


_locked_json_update_local = contextlib.contextmanager(_locked_json_update_local)


# ---------------------------------------------------------------------------
# Quarantine flag queue (audit.FLAG_QUEUE — written by bridge flag_issue)
# ---------------------------------------------------------------------------


def _load_audit():
    """Lazily import the repo-root `audit` module (Phase 5). Returns None
    when it is not importable — flag pickup is then skipped gracefully."""
    try:
        import audit
        return audit
    except Exception:
        return None


def flag_queue_path():
    """Path of the quarantine flag queue (audit.FLAG_QUEUE), or None."""
    audit = _load_audit()
    if audit is None:
        return None
    p = getattr(audit, "FLAG_QUEUE", None)
    return Path(p) if p else None


def load_flag_queue(path: Path | None) -> dict:
    """Load the flag queue at `path` if it exists.

    Shape: {"updated_at": ..., "flagged": [{id, item_id, title, description,
    severity, reporter, status, created_at}]}. A missing file yields an empty
    queue; a corrupt/unexpected file is reported on stderr and ignored (the
    curator never crashes on it and never overwrites it).
    """
    empty = {"updated_at": None, "flagged": []}
    if path is None or not path.exists():
        return empty
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        print(f"WARNING: flag queue {path} is not readable JSON ({exc}) — ignoring it",
              file=sys.stderr)
        return empty
    if not isinstance(data, dict) or not isinstance(data.get("flagged"), list):
        print(f"WARNING: flag queue {path} has an unexpected shape — ignoring it",
              file=sys.stderr)
        return empty
    return data


def flag_queue_flags(flag_data: dict) -> list[dict]:
    """Turn quarantine entries into review-queue flags (reason 'flagged-issue').

    merge_queue keys by (id, reason), so these integrate idempotently with
    the scan-produced flags.
    """
    flags: list[dict] = []
    for entry in flag_data.get("flagged", []):
        if not isinstance(entry, dict):
            continue
        item_id = entry.get("item_id") or entry.get("id")
        if not item_id:
            continue
        flags.append({
            "id": str(item_id),
            "title": entry.get("title"),
            "source_uri": None,
            "reason": FLAG_REASON,
            "evidence": (
                f"{entry.get('description') or ''} "
                f"(severity={entry.get('severity')}, "
                f"reporter={entry.get('reporter') or 'unknown'}, "
                f"issue={entry.get('id')})"
            ),
        })
    return flags


def backfill_flag_queue_decisions(path: Path | None, merged: dict, now: datetime) -> int:
    """Back-fill DECIDED review-queue decisions into the quarantine flag queue.

    For every merged queue entry (id, reason='flagged-issue') whose status is
    approved/dismissed, the matching quarantine entry's status is set to that
    decision and the flag queue is written under an exclusive lock
    (audit.locked_json_update — audit-A finding 5), so a concurrent bridge
    flag_issue writer can neither tear the file nor lose the back-fill.
    Returns the number of quarantine entries matched (0 when nothing was
    decided). The caller must not invoke this in --dry-run.
    """
    if path is None or not path.exists():
        return 0
    decided = {
        str(e.get("id")): e.get("status")
        for e in merged.get("flagged", [])
        if isinstance(e, dict) and e.get("reason") == FLAG_REASON
        and e.get("status") in DECIDED_STATUSES
    }
    if not decided:
        return 0
    matched = 0

    def _apply_decisions(data: dict) -> None:
        nonlocal matched
        for entry in data.get("flagged", []):
            if not isinstance(entry, dict):
                continue
            key = str(entry.get("item_id") or entry.get("id"))
            if key in decided and entry.get("status") != decided[key]:
                entry["status"] = decided[key]  # idempotent: only count changes
                matched += 1
        if matched:
            data["updated_at"] = now.isoformat()

    try:
        with _locked_json_update(path, _apply_decisions,
                                 lambda: {"updated_at": None, "flagged": []}) as data:
            if data is not None:
                _apply_decisions(data)  # idempotent — safe if the CM already applied it
    except RuntimeError as exc:
        # corrupt/unlockable flag queue: warn and skip (never overwrite blindly)
        print(f"WARNING: cannot back-fill flag queue {path!r}: {exc}", file=sys.stderr)
        return 0
    return matched


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(argv: list[str]) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as e:
        if e.code == 0:
            raise
        if isinstance(e.code, str):
            print(e.code, file=sys.stderr)
        else:
            print("ERROR: invalid arguments (see --help)", file=sys.stderr)
        return 1

    queue_path = Path(args.queue)

    try:
        url = resolve_url()
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    db_desc = describe_conninfo(url)

    try:
        conn = psycopg.connect(url, row_factory=dict_row, connect_timeout=10)
    except Exception as exc:
        print(f"ERROR: cannot connect to {db_desc} (credentials redacted): "
              f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    now = datetime.now(timezone.utc)

    # Phase 5: quarantine flag queue (audit.FLAG_QUEUE) — loaded up front so
    # its entries merge into the review queue as reason "flagged-issue".
    flag_path = flag_queue_path()
    flag_data = load_flag_queue(flag_path)
    fq_flags = flag_queue_flags(flag_data)

    try:
        with conn:
            items = fetch_items(conn)
            n_rels = conn.execute("SELECT count(*) AS n FROM relationships").fetchone()["n"]
            flags, contradiction_edges, version_flags = scan(conn, now)
            flags = flags + fq_flags

            # decisions already recorded in the existing queue govern invalidation
            existing_queue = load_queue(queue_path)
            decided = {
                (str(e.get("id")), str(e.get("reason")))
                for e in existing_queue.get("flagged", [])
                if isinstance(e, dict) and e.get("status") in DECIDED_STATUSES
            }
            invalidations = decide_invalidations(contradiction_edges, items, decided)

            if args.dry_run:
                applied = 0  # nothing is written in dry-run
            else:
                applied = apply_invalidations(conn, invalidations)

            merged = merge_queue(existing_queue, flags, now)
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"ERROR: curator scan failed on {db_desc}: {type(exc).__name__}: {exc}",
              file=sys.stderr)
        return 1
    finally:
        conn.close()

    by_reason: dict[str, int] = {}
    for f in flags:
        by_reason[f["reason"]] = by_reason.get(f["reason"], 0) + 1

    if args.dry_run:
        write_ok = "not written (dry-run)"
        n_backfill = 0  # --dry-run writes neither the queue nor the flag queue
    else:
        # Locked load-merge-write (audit-A finding 5): audit.locked_json_update
        # holds an exclusive lock across load -> merge -> atomic write, so a
        # concurrent bridge/curator writer can neither tear the queue file nor
        # lose updates. merge_queue is idempotent (keyed by (id, reason)), so
        # applying the mutator twice is harmless.
        def _merge_mutator(data: dict) -> None:
            m = merge_queue(data, flags, now)
            data.clear()
            data.update(m)

        try:
            with _locked_json_update(queue_path, _merge_mutator,
                                     lambda: {"updated_at": None, "flagged": []}) as data:
                if data is not None:
                    _merge_mutator(data)  # safe if the CM already applied it
            merged = load_queue(queue_path)  # post-write state (merged under the lock)
            write_ok = f"written to {queue_path}"
        except (OSError, RuntimeError) as exc:
            print(f"ERROR: cannot write queue {queue_path!r}: {exc}", file=sys.stderr)
            return 1
        try:
            n_backfill = backfill_flag_queue_decisions(flag_path, merged, now)
        except (OSError, RuntimeError) as exc:
            print(f"WARNING: cannot back-fill flag queue {flag_path!r}: {exc}",
                  file=sys.stderr)
            n_backfill = 0

    n_decided = sum(1 for e in merged["flagged"] if e.get("status") in DECIDED_STATUSES)
    n_pending = sum(1 for e in merged["flagged"] if e.get("status") == "pending")

    print(f"curator: db        = {db_desc} (host/dbname only — credentials redacted)")
    print(f"curator: dry_run   = {str(args.dry_run).lower()}")
    print(f"curator: scanned   = {len(items)} item(s), {int(n_rels)} relationship(s)")
    print(f"curator: queue     = {write_ok}")
    print(f"summary: stale={by_reason.get('stale', 0)} "
          f"contradiction={by_reason.get('contradiction', 0)} "
          f"superseded={by_reason.get('superseded', 0)} "
          f"isolated={by_reason.get('isolated', 0)} "
          f"flagged_issue={by_reason.get(FLAG_REASON, 0)} "
          f"total_flags={len(flags)}")
    if args.dry_run:
        print(f"summary: would_invalidate={len(invalidations)} "
              f"({'; '.join(f'{l} (vs {w}, {why})' for l, w, why in invalidations) or 'none'})")
        print("summary: queue NOT written (dry-run); flag queue NOT written (dry-run)")
    else:
        print(f"summary: invalidated={applied} "
              f"({'; '.join(f'{l} (vs {w}, {why})' for l, w, why in invalidations) or 'none'})")
        print(f"summary: flag_queue_backfilled={n_backfill}"
              + (f" (flag queue {flag_path})" if flag_path is not None else ""))
    print(f"summary: queue_pending={n_pending} queue_decided={n_decided} "
          f"queue_total={len(merged['flagged'])}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
