"""bridge.py — JSON-lines CLI bridge between frontends (MCP server, dropbox UI)
and the Python pipeline/retrieval code.

LOCKED CONTRACT (PM_LEDGER.md, Batch 2 bridge contract — do not change):

    python -m bridge <command> '<json-args>'      # run from the repo root

Prints exactly ONE JSON line to stdout:

    {"ok": true, "result": ...}    exit code 0
    {"ok": false, "error": "..."}  exit code 1

All diagnostics go to stderr only. Library chatter is redirected to stderr at
the file-descriptor level, so stdout always carries exactly the one envelope.

Commands (json-args in parentheses):

    ingest        {"source_type": "text"|"url"|"pdf"|"image"|"youtube"|"audio"|"github", "payload": str,
                   ("youtube": payload is the video URL; captions preferred, metadata fallback),
                   ("pdf"|"image"|"audio": payload is a path to a file; the file is
                    read into bytes before ingest — a missing file fails here with
                    a clean error, and store_raw then stores the real file bytes),
                   ("github": payload is a repo URL https://github.com/<owner>/<repo>(/tree/<branch>);
                    the repo's text files are imported as MULTIPLE knowledge items, each
                    inserted through the same per-item path as single ingests),
                   "source_uri"?: str|null, "title"?: str|null,
                   "store_raw"?: bool = false,
                   "max_files"?: int = 50, "max_file_bytes"?: int = 200000  (github only)}
                   github result: {status: "inserted"|"partial"|"duplicate-mix",
                   node_ids, file_count, errors, source_type, repo_url}
    search        {"query": str, "methods"?: ["vector","fts","graph"],
                   "hops"?: int = 1 (clamped to 1..5), "top_k"?: int = 10 (1..100),
                   "candidate_k"?: int = 20 (1..200), "token_budget"?: int = 8000}
                   token_budget is caller-overridable up to 100000 (clamped 1..100000)
                   but defaults to the locked 8000 (retrieval.search.DEFAULT_TOKEN_BUDGET);
                   the shipped frontends (MCP server, dropbox UI) never override it
    get_related   {"item_id": str, "hops"?: int = 1}
    link_items    {"from_id": str, "to_id": str, "type": str,
                   "strength"?: number 0-1, "direction"?: "out"|"in"|"both",
                   "context"?: str|null}
    update_item   {"item_id": str, "title"?, "content"?, "distillation_score"?,
                   "update_period"?, "metadata"?}
    delete_item   {"item_id": str}
    list_recent   {"limit"?: int = 20}
    curator       {"dry_run"?: bool = false}
                  Phase 4 consolidation report (backs the MCP `dream` tool):
                  staleness counts (update_period != -1 split stale/not-stale via
                  retrieval.merge.is_stale), contradiction edge count
                  (relationships.type = 'contradicts'), isolated item count (no
                  relationships in either direction) — all via parameterized SQL.
                  When scripts/curator.py is importable (lazy, by file path from the
                  repo root), its main logic is called to write/merge the review
                  queue and queue_written/queue_path land in the report; otherwise
                  the report stands alone with queue_written=false, queue_path=null.
                  result: {report: {total_items, staleness_checked, stale, not_stale,
                  contradictions, isolated, queue_written, queue_path, dry_run}, ...}
    review_decision {"id": str, "reason": str, "decision": "approved"|"dismissed"}
                  Record a human review decision atomically in BOTH places when
                  "id" is a uuid: the repo-root review_queue.json entry keyed
                  by (id, reason) gets status=decision and updated_at=now
                  (written to a tmp file then os.replace — ValueError on a
                  missing queue file, no matching entry, or bad input), AND
                  the DB is updated —
                  "dismissed": UPDATE knowledge_items SET metadata =
                  metadata - 'review_status' WHERE id=%s;
                  "approved": only when metadata->>'review_status' LIKE
                  'invalidated%' -> metadata || {"review_status":
                  "invalidated-approved"} (otherwise a no-op).
                  A NON-uuid "id" (e.g. one of the 16-hex flag_issue ids) is a
                  QUEUE-ONLY decision: the queue entry is updated and the DB
                  is never touched (db_updated=0).
                  result: {queue_updated, db_updated, id, reason, decision,
                  queue_path}
    flag_issue    {"item_id"?: str(uuid), "title": str, "description": str,
                   "severity"?: "low"|"medium"|"high" = "medium", "reporter"?: str}
                  Quarantine an issue in the FLAG_QUEUE file ONLY (never the
                  DB — knowledge_items/relationships are never touched).
                  Entry id = sha256((item_id or "") + "\n" + description)[:16];
                  an existing id returns the stored entry unchanged (status
                  preserved, idempotent). The queue is loaded, mutated and
                  written under the f"{FLAG_QUEUE}.lock" lockfile via
                  audit.locked_json_update (atomic tmp + os.replace), so
                  concurrent writers cannot tear the file; a corrupt existing
                  queue file is refused (RuntimeError).
                  result: the entry {id, item_id, title, description, severity,
                  reporter, status: "pending", created_at}
    project_context {"project": str, "token_budget"?: int = 4000}
                  Read-only context package for a project: looks up the
                  knowledge_items row with source_uri "project://<slug>"
                  (slug = lowercase [a-z0-9-] chars of project) and gathers
                  graph (page_rank_expand, hops=2, top_k=20) + vector
                  (top 10 for "<project> decision lesson risk failed approach")
                  related items, capped by a greedy token budget (len//4).
                  Unknown project -> ValueError. Logs NOTHING (read-only).
                  result: {project, profile (row minus embedding),
                  graph_related, vector_related, decisions, estimated_tokens}
    discover_relevant {"query": str, "hops"?: int = 2, "top_k"?: int = 10}
                  Read-only hybrid retrieval (all methods, candidate_k=30,
                  hops clamped 1..5, top_k clamped 1..100) plus suggested
                  decision/lesson titles. Logs NOTHING (read-only).
                  result: {query, results, by_method_counts, suggested}
    log_audit     {"event": {"type": str, ...}}
                  Append one event to the audit log (audit.log_event pops
                  "type" and logs the rest). The MCP write server calls this
                  at startup and after mutating tool calls.
                  result: {"logged": true}

Audit trail (Phase 5): every mutating command (ingest, update_item,
delete_item, link_items, review_decision, flag_issue) appends a
{"event": "bridge_write", "command": ..., "args_sha256": ..., "outcome":
"ok"|"error", ...} line to the audit log (audit.py — SECONDBRAIN_AUDIT_LOG or
<repo_root>/audit_log.jsonl) — with outcome="ok" on success and
outcome="error" (plus a sanitized error text carrying the exception type
name) even when the command fails; serve mode logs "bridge_serve_start"
with its pid at startup. Audit output goes to the log FILE only, never
stdout — the one-JSON-line stdout contract above is unchanged.

Serve mode (persistent process — heavy imports/models paid once, not per call):

    python -m bridge serve

Reads stdin LINE BY LINE. Each line is one request JSON object:

    {"id": <any json value>, "command": "<cmd>", "args": {...}}

and writes exactly ONE stdout line per request:

    {"id": <same>, "ok": true,  "result": ...}
    {"id": <same>, "ok": false, "error": "..."}

A malformed line gets {"id": null, "ok": false, "error": "..."}. All
diagnostics go to stderr. stdin EOF -> exit 0. At serve start the pipeline +
retrieval modules (including the sentence_transformers import chain) are
imported eagerly so the cost is paid once per process; model weights stay
lazy (loaded on first embed/rerank call, then cached in-process).

All DB access goes through pipeline.db.get_connection() (which also registers the
numpy pgvector loader on every connection); every SQL statement is parameterized.
DATABASE_URL comes from the environment (see pipeline/db.py).
"""

from __future__ import annotations

import hashlib
import importlib
import json
import os
import re
import sys
import traceback
import uuid
from datetime import date, datetime, time, timezone
from decimal import Decimal

import audit

USAGE = ("usage: python -m bridge <command> '<json-args>' | python -m bridge serve\n"
         "  commands: ingest, search, get_related, link_items, update_item, delete_item, list_recent, curator, review_decision,\n"
         "            flag_issue, project_context, discover_relevant, log_audit\n"
         "  serve: persistent JSON-lines mode, one request per stdin line, one response line each")

VALID_SOURCE_TYPES = ("text", "url", "pdf", "image", "youtube", "audio", "github")
VALID_DIRECTIONS = ("out", "in", "both")


# ---------------------------------------------------------------------------
# JSON serialization of DB rows / pipeline results
# ---------------------------------------------------------------------------

def _jsonable(obj):
    """Convert arbitrary Python/DB values into JSON-serializable structures."""
    if obj is None or isinstance(obj, (bool, int, float, str)):
        return obj
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (datetime, date, time)):
        return obj.isoformat()
    if isinstance(obj, uuid.UUID):
        return str(obj)
    if isinstance(obj, dict):
        # Raw 1024-dim embedding vectors are stripped from bridge payloads —
        # MCP/UI consumers never need them and they would be 1024-float blobs.
        return {str(k): _jsonable(v) for k, v in obj.items() if k != "embedding"}
    if isinstance(obj, (list, tuple, set, frozenset)):
        return [_jsonable(v) for v in obj]
    if isinstance(obj, (bytes, bytearray, memoryview)):
        raw = bytes(obj)
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError:
            return raw.hex()
    if hasattr(obj, "tolist"):  # numpy arrays
        return _jsonable(obj.tolist())
    if hasattr(obj, "item"):  # numpy scalars
        return _jsonable(obj.item())
    return str(obj)


# ---------------------------------------------------------------------------
# Argument validation helpers
# ---------------------------------------------------------------------------

def _req_str(args: dict, name: str) -> str:
    v = args.get(name)
    if not isinstance(v, str) or not v:
        raise ValueError(f"missing or invalid required arg {name!r} (non-empty string expected)")
    return v


def _opt_str(args: dict, name: str) -> str | None:
    v = args.get(name)
    if v is not None and not isinstance(v, str):
        raise ValueError(f"arg {name!r} must be a string or null")
    return v


def _opt_int(args: dict, name: str, default: int) -> int:
    v = args.get(name)
    if v is None:
        return default
    if isinstance(v, bool) or not isinstance(v, (int, float)) or int(v) != v:
        raise ValueError(f"arg {name!r} must be an integer")
    return int(v)


def _req_uuid(args: dict, name: str) -> uuid.UUID:
    v = args.get(name)
    if not isinstance(v, str):
        raise ValueError(f"missing or invalid required arg {name!r} (uuid string expected)")
    try:
        return uuid.UUID(v)
    except (ValueError, AttributeError, TypeError):
        raise ValueError(f"arg {name!r} is not a valid uuid: {v!r}") from None


def _sanitize_error(text) -> str:
    """Strip secrets from an error string before it lands in an envelope.

    Replaces the value of every environment variable whose NAME contains
    KEY/TOKEN/PASSWORD/SECRET with <redacted> (so a leaked API key, DB
    password, etc. can never appear in an error message), redacts URL
    credentials (scheme://user:pass@ -> scheme://user:<redacted>@), and
    redacts URL query strings / fragments (https://host/path?secret#x ->
    https://host/path?<redacted>).
    """
    import re

    s = str(text)
    for name, val in os.environ.items():
        if val and any(tag in name.upper() for tag in ("KEY", "TOKEN", "PASSWORD", "SECRET")):
            s = s.replace(val, "<redacted>")
    s = re.sub(
        r"(?i)(\b[a-z][a-z0-9+.\-]*://[^/\s:@]+:)[^@\s/]+(@)",
        r"\1<redacted>\2",
        s,
    )
    # query string / fragment of an http(s) URL never lands in an envelope
    # (lazy match: everything from the FIRST ? or # on is redacted)
    s = re.sub(
        r"(?i)(https?://[^\s]+?)[?#][^\s]*",
        r"\1?<redacted>",
        s,
    )
    return s


# ---------------------------------------------------------------------------
# Server-side path confinement (path-traversal guard)
# ---------------------------------------------------------------------------
# For source_type pdf/image/audio the payload is a SERVER-SIDE PATH (the
# dropbox UI uploads into dropbox/inbox/ first and passes that path). Confine
# it: realpath(payload) must live under dropbox/inbox/ OR the staging dir
# <system temp dir>/secondbrain-staging (tests/scripts stage files there);
# extra roots via BRIDGE_PATH_ALLOWLIST (os.pathsep-separated). Anything else
# is refused before any file access.


def _staging_root() -> str:
    """Dedicated staging dir under the system temp dir (created on demand)."""
    import tempfile

    staging = os.path.join(tempfile.gettempdir(), "secondbrain-staging")
    os.makedirs(staging, exist_ok=True)
    return staging



def _allowed_path_roots() -> list[str]:
    repo_root = os.path.dirname(os.path.abspath(__file__))
    roots = [
        os.path.realpath(os.path.join(repo_root, "dropbox", "inbox")),
        os.path.realpath(_staging_root()),
    ]
    extra = os.environ.get("BRIDGE_PATH_ALLOWLIST", "")
    for part in extra.split(os.pathsep):
        part = part.strip()
        if part:
            roots.append(os.path.realpath(part))
    return roots


def _assert_allowed_path(payload: str) -> None:
    real = os.path.realpath(payload)
    for root in _allowed_path_roots():
        if real == root or real.startswith(root + os.sep):
            return
    raise ValueError(
        f"path outside allowed roots: {payload!r} (allowed: dropbox/inbox/, "
        f"the secondbrain-staging dir under the system temp dir, plus "
        f"BRIDGE_PATH_ALLOWLIST entries)"
    )


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

# Mutating commands — every one of these appends a bridge_write audit event
# (outcome "ok" on success, "error" on failure, see _audit_bridge_write).
MUTATING_COMMANDS = frozenset({
    "ingest", "update_item", "delete_item", "link_items",
    "review_decision", "flag_issue",
})


def _audit_bridge_write(command: str, args: dict, outcome: str = "ok",
                        error: str | None = None, **extra) -> None:
    """Append a bridge_write audit event for a mutating command.

    Fields: command, args_sha256 (audit.args_fingerprint), outcome
    ("ok" | "error"), the sanitized error text (only when outcome == "error"),
    plus item_ids (item_id/from_id/to_id/id string args) when present and any
    extra fields. log_event never raises, so this is always safe to call —
    on the success path AND in the failure path of a mutating command.
    """
    item_ids = [args[k] for k in ("item_id", "from_id", "to_id", "id")
                if isinstance(args.get(k), str)]
    fields: dict = {"command": command, "args_sha256": audit.args_fingerprint(args),
                    "outcome": outcome}
    if error is not None:
        fields["error"] = error
    if item_ids:
        fields["item_ids"] = item_ids
    fields.update(extra)
    audit.log_event("bridge_write", **fields)


def _audit_bridge_write_error(command: str, args: dict, exc: BaseException) -> None:
    """Log bridge_write with outcome="error" for a FAILED mutating command.

    The error text carries the exception type name and goes through
    _sanitize_error, so no secret/URL material leaks into the audit log.
    """
    if command in MUTATING_COMMANDS:
        _audit_bridge_write(command, args, outcome="error",
                            error=_sanitize_error(f"{type(exc).__name__}: {exc}"))


def _ingest_one(source_type: str, payload, source_uri: str | None = None,
                title: str | None = None, store_raw: bool = False,
                distillation_override: float | None = None) -> dict:
    """Single-item insert path shared by every source type.

    Delegates to pipeline.ingest.ingest — dedup per item, insert-only,
    parameterized SQL — so multi-item imports (github) reuse exactly the
    same insert code as single ingests; no SQL lives in this file.
    """
    from pipeline.ingest import ingest
    return ingest(source_type, payload, source_uri, title, store_raw=store_raw,
                  distillation_override=distillation_override)


def _github_file_title(source_uri: str | None) -> str | None:
    """Derive a repo-relative file path (used as title) from a github blob URI."""
    if not source_uri or "/blob/" not in source_uri:
        return None
    rest = source_uri.split("/blob/", 1)[1]
    parts = rest.split("/", 1)
    return parts[1] if len(parts) == 2 else rest


def _cmd_ingest_github(repo_url: str, max_files: int, max_file_bytes: int,
                       store_raw: bool = False) -> dict:
    """Import a GitHub repo, inserting each file via the shared per-item path."""
    from pipeline.github_import import import_github_repo

    items = import_github_repo(repo_url, max_files=max_files, max_file_bytes=max_file_bytes)
    if not items:
        raise ValueError(f"github import of {repo_url!r} returned no files")
    node_ids: list[str] = []
    errors: list[str] = []
    statuses: list[str] = []
    for item in items:
        try:
            res = _ingest_one("text", item.text, source_uri=item.source_uri,
                              title=_github_file_title(item.source_uri),
                              store_raw=store_raw,
                              distillation_override=0.1)
        except Exception as exc:  # one bad item must not abort the batch
            errors.append(f"{item.source_uri}: {type(exc).__name__}: {exc}")
            continue
        statuses.append(res.get("status", ""))
        node_ids.extend(res.get("node_ids") or [])
    if not node_ids and errors:
        raise RuntimeError(
            f"github import of {repo_url!r}: all {len(errors)} item inserts failed; "
            f"first error: {errors[0]}"
        )
    if errors:
        status = "partial"
    elif statuses and all(s == "inserted" for s in statuses):
        status = "inserted"
    else:
        status = "duplicate-mix"
    return {
        "status": status,
        "node_ids": node_ids,
        "file_count": len(items),
        "errors": errors,
        "source_type": "github",
        "repo_url": repo_url,
    }


def cmd_ingest(args: dict) -> dict:
    """Ingest content via the pipeline. Single items return pipeline.ingest.ingest(...);
    source_type "github" imports a whole repo as multiple items (_cmd_ingest_github)."""
    source_type = _req_str(args, "source_type")
    if source_type not in VALID_SOURCE_TYPES:
        raise ValueError(f"source_type must be one of {VALID_SOURCE_TYPES}, got {source_type!r}")
    payload = args.get("payload")
    if not isinstance(payload, str):
        raise ValueError("missing or invalid required arg 'payload' (string expected)")
    source_uri = _opt_str(args, "source_uri")
    title = _opt_str(args, "title")
    store_raw = args.get("store_raw", False)
    if not isinstance(store_raw, bool):
        raise ValueError("arg 'store_raw' must be a boolean")
    if source_type == "github":
        max_files = min(max(_opt_int(args, "max_files", 50), 1), 500)
        max_file_bytes = min(max(_opt_int(args, "max_file_bytes", 200_000), 1), 5_000_000)
        result = _cmd_ingest_github(payload, max_files, max_file_bytes, store_raw=store_raw)
        _audit_bridge_write("ingest", args)
        return result
    if source_type in ("pdf", "image", "audio"):
        # payload is a server-side path for these types — confine it to the
        # allowed roots BEFORE any file access or DB work, then read the file
        # BYTES so a missing/unreadable file fails here (clean error, before
        # any DB access) and store_raw stores the real bytes, not the path.
        _assert_allowed_path(payload)
        if not os.path.isfile(payload):
            raise ValueError(f"{source_type} payload is not an existing file: {payload!r}")
        with open(payload, "rb") as fh:
            file_bytes = fh.read()
        if source_uri is None:
            source_uri = payload
        payload = file_bytes
    result = _ingest_one(source_type, payload, source_uri, title, store_raw=store_raw)
    _audit_bridge_write("ingest", args)
    return result


def cmd_search(args: dict) -> dict:
    """Hybrid retrieval via retrieval.search.search(...)."""
    from retrieval.search import DEFAULT_TOKEN_BUDGET, search

    query = _req_str(args, "query")
    if not query.strip():
        raise ValueError("arg 'query' must be a non-empty string")
    methods = args.get("methods")
    if methods is not None:
        if not isinstance(methods, list) or not all(isinstance(m, str) for m in methods):
            raise ValueError("arg 'methods' must be a list of strings ('vector'|'fts'|'graph')")
        methods = list(methods)
    hops = _opt_int(args, "hops", 1)
    top_k = _opt_int(args, "top_k", 10)
    candidate_k = _opt_int(args, "candidate_k", 20)
    token_budget = _opt_int(args, "token_budget", DEFAULT_TOKEN_BUDGET)
    # Clamp unbounded inputs (audit finding: hops/top_k/candidate_k were unbounded).
    hops = min(max(hops, 1), 5)
    top_k = min(max(top_k, 1), 100)
    candidate_k = min(max(candidate_k, 1), 200)
    token_budget = min(max(token_budget, 1), 100_000)
    return search(query, methods=methods, hops=hops, top_k=top_k,
                  candidate_k=candidate_k, token_budget=token_budget)


def cmd_get_related(args: dict) -> dict:
    """Return the item row plus its graph neighborhood (retrieval.graph.graph_expand)."""
    from pipeline.db import get_connection
    from retrieval.graph import graph_expand

    item_id = _req_uuid(args, "item_id")
    hops = _opt_int(args, "hops", 1)
    # Same clamp as cmd_search (audit finding: hops was unbounded here).
    hops = min(max(hops, 1), 5)
    with get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM knowledge_items WHERE id = %s", (item_id,)
        ).fetchone()
        if row is None:
            raise ValueError(f"knowledge item {item_id} not found")
        related = graph_expand(conn, [row["id"]], hops=hops)
    return {"item": dict(row), "related": related, "params": {"hops": hops}}


def cmd_link_items(args: dict) -> dict:
    """INSERT a relationship between two items; returns the created row."""
    from pipeline.db import get_connection

    from_id = _req_uuid(args, "from_id")
    to_id = _req_uuid(args, "to_id")
    rel_type = _req_str(args, "type")
    strength = args.get("strength")
    if strength is not None:
        if isinstance(strength, bool) or not isinstance(strength, (int, float)):
            raise ValueError("arg 'strength' must be a number between 0 and 1")
        strength = float(strength)
        if not 0.0 <= strength <= 1.0:
            raise ValueError("arg 'strength' must be between 0 and 1")
    direction = args.get("direction", "out")
    if direction is None:
        direction = "out"
    if direction not in VALID_DIRECTIONS:
        raise ValueError(f"arg 'direction' must be one of {VALID_DIRECTIONS}, got {direction!r}")
    context = _opt_str(args, "context")

    # Map the API direction onto the schema's one_way/bidirectional:
    #   "out"  -> from_id -> to_id (one_way)
    #   "in"   -> to_id -> from_id (one_way, i.e. incoming to from_id)
    #   "both" -> from_id <-> to_id (bidirectional)
    if direction == "in":
        edge_from, edge_to, edge_dir = to_id, from_id, "one_way"
    elif direction == "both":
        edge_from, edge_to, edge_dir = from_id, to_id, "bidirectional"
    else:
        edge_from, edge_to, edge_dir = from_id, to_id, "one_way"

    with get_connection() as conn:
        for nid, label in ((from_id, "from_id"), (to_id, "to_id")):
            exists = conn.execute(
                "SELECT 1 FROM knowledge_items WHERE id = %s", (nid,)
            ).fetchone()
            if exists is None:
                raise ValueError(f"{label} {nid} does not reference an existing knowledge item")
        row = conn.execute(
            """
            INSERT INTO relationships (from_id, to_id, type, strength, direction, context)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (from_id, to_id, type) DO NOTHING
            RETURNING *
            """,
            (edge_from, edge_to, rel_type, strength, edge_dir, context),
        ).fetchone()
        if row is None:
            # Relationship already exists — return the existing row.
            row = conn.execute(
                """
                SELECT * FROM relationships
                WHERE from_id = %s AND to_id = %s AND type = %s
                """,
                (edge_from, edge_to, rel_type),
            ).fetchone()
    _audit_bridge_write("link_items", args)
    return dict(row)


def cmd_update_item(args: dict) -> dict:
    """Explicit user-requested UPDATE of a knowledge item; re-embeds on content change."""
    import json as _json

    from pipeline.db import get_connection

    item_id = _req_uuid(args, "item_id")
    updatable = ("title", "content", "distillation_score", "update_period", "metadata")
    provided = {k: args[k] for k in updatable if args.get(k) is not None}
    if not provided:
        raise ValueError(f"no updatable fields provided (choose from {updatable})")

    sets: list[str] = []
    params: list = []

    if "title" in provided:
        if not isinstance(provided["title"], str):
            raise ValueError("arg 'title' must be a string")
        sets.append("title = %s")
        params.append(provided["title"])

    new_metadata = None
    if "metadata" in provided:
        if not isinstance(provided["metadata"], dict):
            raise ValueError("arg 'metadata' must be a JSON object")
        new_metadata = dict(provided["metadata"])

    if "content" in provided:
        content = provided["content"]
        if not isinstance(content, str) or not content.strip():
            raise ValueError("arg 'content' must be a non-empty string")
        sets.append("content = %s")
        params.append(content)

    if "distillation_score" in provided:
        s = provided["distillation_score"]
        if isinstance(s, bool) or not isinstance(s, (int, float)):
            raise ValueError("arg 'distillation_score' must be a number between 0 and 1")
        s = float(s)
        if not 0.0 <= s <= 1.0:
            raise ValueError("arg 'distillation_score' must be between 0 and 1")
        sets.append("distillation_score = %s")
        params.append(s)

    if "update_period" in provided:
        p = provided["update_period"]
        if isinstance(p, bool) or not isinstance(p, (int, float)) or int(p) != p:
            raise ValueError("arg 'update_period' must be an integer (seconds, -1 = never)")
        sets.append("update_period = %s")
        params.append(int(p))

    with get_connection() as conn:
        current = conn.execute(
            "SELECT * FROM knowledge_items WHERE id = %s", (item_id,)
        ).fetchone()
        if current is None:
            raise ValueError(f"knowledge item {item_id} not found")

        content_changed = "content" in provided and provided["content"] != current["content"]
        if content_changed:
            # Re-embed the new content and keep the dedup hashes in sync:
            # metadata is MERGED (not replaced), and the whole-document
            # metadata.document_hash version key is recomputed from the new
            # full content alongside the per-node content_hash.
            from pipeline.dedup import content_hash
            from pipeline.embed import embed_text

            emb = embed_text(provided["content"])
            sets.append("embedding = %s::vector")
            params.append(emb.tolist())
            new_hash = content_hash(provided["content"])
            if new_metadata is None:
                new_metadata = {}
            new_metadata["content_hash"] = new_hash
            new_metadata["document_hash"] = new_hash

        if new_metadata is not None:
            # Merge into the existing jsonb instead of replacing it, so
            # pre-existing metadata keys survive an update. Parameterized.
            sets.append("metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb")
            params.append(_json.dumps(new_metadata))

        params.append(item_id)
        # Column names in SET come only from the fixed `updatable` whitelist;
        # every value is a parameterized %s.
        row = conn.execute(
            f"UPDATE knowledge_items SET {', '.join(sets)} WHERE id = %s RETURNING *",
            params,
        ).fetchone()
    _audit_bridge_write("update_item", args)
    return dict(row)


def cmd_delete_item(args: dict) -> dict:
    """User-requested removal of an item and all of its relationships."""
    from pipeline.db import get_connection

    item_id = _req_uuid(args, "item_id")
    with get_connection() as conn:
        conn.execute(
            "DELETE FROM relationships WHERE from_id = %s OR to_id = %s",
            (item_id, item_id),
        )
        row = conn.execute(
            "DELETE FROM knowledge_items WHERE id = %s RETURNING id", (item_id,)
        ).fetchone()
        if row is None:
            raise ValueError(f"knowledge item {item_id} not found")
    _audit_bridge_write("delete_item", args)
    return {"deleted": str(item_id)}


def cmd_list_recent(args: dict) -> dict:
    """Most recent knowledge_items rows."""
    from pipeline.db import get_connection

    limit = _opt_int(args, "limit", 20)
    if limit < 1:
        raise ValueError("arg 'limit' must be >= 1")
    limit = min(limit, 1000)
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM knowledge_items ORDER BY created_at DESC LIMIT %s",
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# curator — Phase 4 consolidation report (backs the MCP `dream` tool)
# ---------------------------------------------------------------------------

# Entry-point names tried, in order, inside scripts/curator.py (that file is
# built by a parallel worker; the dream path must work standalone without it).
_CURATOR_ENTRY_NAMES = ("run_curator", "run", "consolidate", "curator_pass", "main")


def _curator_script_path() -> str:
    """Absolute path of scripts/curator.py (repo root = this file's directory)."""
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "scripts", "curator.py")



def _resolve_db_url() -> str:
    """Connection URL for the curator report SQL: TEST_DATABASE_URL first
    (tests point it at a throwaway test DB), then DATABASE_URL — the same
    convention as scripts/curator.py resolve_url()."""
    url = (os.environ.get("TEST_DATABASE_URL") or "").strip()
    if not url:
        url = (os.environ.get("DATABASE_URL") or "").strip()
    if not url:
        raise RuntimeError(
            "no database URL — set TEST_DATABASE_URL (tests) or DATABASE_URL "
            "(ops), e.g. postgresql://postgres@127.0.0.1:5433/secondbrain"
        )
    return url


def _import_curator_module(path: str):
    """Lazily import scripts/curator.py by file location via importlib."""
    import importlib.util

    spec = importlib.util.spec_from_file_location("secondbrain_scripts_curator", path)
    if spec is None or spec.loader is None:
        return None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _call_curator_entry(fn, report: dict, dry_run: bool):
    """Call a curator entry point, passing report/dry_run only when its
    signature accepts them (the parallel worker owns that file's API)."""
    import inspect

    try:
        params = inspect.signature(fn).parameters
    except (TypeError, ValueError):
        return fn()
    kwargs = {}
    if "report" in params:
        kwargs["report"] = report
    if "dry_run" in params:
        kwargs["dry_run"] = dry_run
    return fn(**kwargs)


def cmd_curator(args: dict) -> dict:
    """Phase 4 curator consolidation report (backs the MCP `dream` tool).

    Computes the health report directly with parameterized SQL:
      - staleness: items with update_period != -1, split stale/not-stale via
        retrieval.merge.is_stale
      - contradictions: relationship edges with type = 'contradicts'
      - isolated: knowledge_items with no relationship in either direction
    If scripts/curator.py exists, its main logic is imported lazily (by file
    path from the repo root) and called to write/merge the review queue: a
    CLI-style main(argv) gets ["--dry-run"] on dry runs, a function entry gets
    report=/dry_run= kwargs when its signature accepts them. queue_written /
    queue_path are merged into the report. Without the script the report still
    stands alone (queue_written=false, queue_path=null), so dream works even
    before scripts/curator.py merges.

    args: {"dry_run"?: bool = false} — a dry run runs the full scan but writes
    neither the review queue nor any invalidation UPDATE.
    """
    import psycopg
    from psycopg.rows import dict_row

    from pipeline.db import _register_vector_loader
    from retrieval.merge import is_stale

    dry_run = args.get("dry_run", False)
    if not isinstance(dry_run, bool):
        raise ValueError("arg 'dry_run' must be a boolean")

    # Resolve the URL TEST_DATABASE_URL-first (same convention as
    # scripts/curator.py resolve_url()) and connect explicitly with it —
    # get_connection() would only see DATABASE_URL.
    conn = psycopg.connect(_resolve_db_url(), row_factory=dict_row)
    _register_vector_loader(conn)
    try:
        total_items = int(conn.execute(
            "SELECT COUNT(*) AS n FROM knowledge_items"
        ).fetchone()["n"])
        # Staleness only applies to items with an update_period (-1 = never stale).
        candidates = conn.execute(
            "SELECT id, update_period, created_at FROM knowledge_items WHERE update_period <> %s",
            (-1,),
        ).fetchall()
        stale = sum(1 for row in candidates if is_stale(dict(row)))
        contradictions = int(conn.execute(
            "SELECT COUNT(*) AS n FROM relationships WHERE type = %s",
            ("contradicts",),
        ).fetchone()["n"])
        isolated = int(conn.execute(
            """
            SELECT COUNT(*) AS n
            FROM knowledge_items ki
            WHERE NOT EXISTS (SELECT 1 FROM relationships r WHERE r.from_id = ki.id)
              AND NOT EXISTS (SELECT 1 FROM relationships r WHERE r.to_id = ki.id)
            """
        ).fetchone()["n"])
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    report = {
        "total_items": total_items,
        "staleness_checked": len(candidates),
        "stale": stale,
        "not_stale": len(candidates) - stale,
        "contradictions": contradictions,
        "isolated": isolated,
        "queue_written": False,
        "queue_path": None,
        "dry_run": dry_run,
    }

    result = {"report": report, "dry_run": dry_run,
              "curator_script": None, "delegated": False}

    curator_path = _curator_script_path()
    if os.path.isfile(curator_path):
        result["curator_script"] = curator_path
        try:
            module = _import_curator_module(curator_path)
            entry, entry_name = None, None
            if module is not None:
                for name in _CURATOR_ENTRY_NAMES:
                    cand = getattr(module, name, None)
                    if callable(cand):
                        entry, entry_name = cand, name
                        break
            if entry is not None:
                if entry_name == "main":
                    # CLI-style entry: main(argv) with argparse --queue/--dry-run.
                    # Its prints land on bridge stderr (fd 1 is redirected onto
                    # fd 2 while a command runs), so the one-JSON-line stdout
                    # contract of the bridge is preserved.
                    rc = entry(["--dry-run"] if dry_run else [])
                    out = {"exit_code": rc}
                    if rc == 0 and not dry_run:
                        report["queue_written"] = True
                        q = str(getattr(module, "DEFAULT_QUEUE", "review_queue.json"))
                        report["queue_path"] = os.path.abspath(q)
                else:
                    out = _call_curator_entry(entry, report, dry_run)
                    if isinstance(out, dict):
                        if out.get("queue_written") is not None:
                            report["queue_written"] = bool(out["queue_written"])
                        qp = out.get("queue_path", out.get("path"))
                        if qp:
                            report["queue_path"] = str(qp)
                result["delegated"] = True
                result["curator_result"] = _jsonable(out)
        except Exception as e:
            # The optional curator script must never break the report itself.
            result["curator_error"] = _sanitize_error(f"{type(e).__name__}: {e}")

    return result


# ---------------------------------------------------------------------------
# review_decision — record a human review decision (queue + DB)
# ---------------------------------------------------------------------------

_REVIEW_QUEUE_NAME = "review_queue.json"
_REVIEW_DECISIONS = ("approved", "dismissed")


def _review_queue_path() -> str:
    """Repo-root review_queue.json (repo root = this file's directory)."""
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), _REVIEW_QUEUE_NAME)


def cmd_review_decision(args: dict) -> dict:
    """Record a review decision atomically in the queue file (and the DB for uuid ids).

    args: {"id": str, "reason": str, "decision": "approved"|"dismissed"}

    "id" may be either:
      * a uuid — the repo-root review_queue.json entry keyed by (id, reason)
        gets status=decision and updated_at=now AND the DB row is updated
        (see below); or
      * a non-uuid id (e.g. one of the 16-hex flag-queue ids from flag_issue)
        — a QUEUE-ONLY decision: only the queue file is updated, the DB is
        never touched (db_updated is 0).

    Queue: the file is written to a tmp file and os.replace'd into place.
    ValueError on a missing/unreadable queue, no matching entry, or bad input.

    DB (uuid ids only; the only sanctioned non-insert write besides the
    curator merge — all parameterized):
      "dismissed" -> UPDATE knowledge_items SET metadata = metadata -
                     'review_status' WHERE id = %s
      "approved"  -> only when (metadata->>'review_status') LIKE 'invalidated%':
                     SET metadata = metadata ||
                     '{"review_status": "invalidated-approved"}'::jsonb
                     (otherwise a no-op).

    Returns {"queue_updated": bool, "db_updated": int, "id", "reason",
    "decision", "queue_path"}.
    """
    raw_id = _req_str(args, "id")
    reason = _req_str(args, "reason")
    decision = _req_str(args, "decision")
    if decision not in _REVIEW_DECISIONS:
        raise ValueError(f"arg 'decision' must be one of {_REVIEW_DECISIONS}, got {decision!r}")
    item_id: uuid.UUID | None
    try:
        item_id = uuid.UUID(raw_id)
    except (ValueError, AttributeError, TypeError):
        item_id = None  # non-uuid id (e.g. a 16-hex flag id) -> QUEUE-ONLY decision
    id_str = str(item_id) if item_id is not None else raw_id

    # --- queue file (atomic tmp + os.replace) ---
    queue_path = _review_queue_path()
    if not os.path.isfile(queue_path):
        raise ValueError(f"review queue not found: {queue_path}")
    try:
        with open(queue_path, encoding="utf-8") as fh:
            queue = json.load(fh)
    except (json.JSONDecodeError, OSError) as e:
        raise ValueError(f"review queue {queue_path} is not readable JSON: {e}") from None
    if not isinstance(queue, dict) or not isinstance(queue.get("flagged"), list):
        raise ValueError(
            f"review queue {queue_path} has an unexpected shape "
            '(expected {"updated_at": ..., "flagged": [...]})'
        )
    entry = None
    for e in queue["flagged"]:
        if (isinstance(e, dict)
                and str(e.get("id")) == id_str
                and str(e.get("reason")) == reason):
            entry = e
            break
    if entry is None:
        raise ValueError(
            f"no review queue entry in {queue_path} for id={id_str} reason={reason!r}"
        )
    # --- DB (metadata-only jsonb write, parameterized) — uuid ids ONLY ---
    # Commit the DB update BEFORE replacing review_queue.json so a DB error
    # never leaves the queue file mutated out of sync with the database.
    # A non-uuid (flag) id is a queue-only decision: the DB is never touched.
    db_updated = 0
    if item_id is not None:
        from pipeline.db import get_connection

        with get_connection() as conn:
            if decision == "dismissed":
                cur = conn.execute(
                    "UPDATE knowledge_items SET metadata = metadata - 'review_status' WHERE id = %s",
                    (item_id,),
                )
            else:  # approved — only an existing invalidated* marker is rewritten
                # (%% is psycopg placeholder escaping for the literal % in LIKE)
                cur = conn.execute(
                    """
                    UPDATE knowledge_items
                    SET metadata = metadata || '{"review_status": "invalidated-approved"}'::jsonb
                    WHERE id = %s AND (metadata->>'review_status') LIKE 'invalidated%%'
                    """,
                    (item_id,),
                )
            db_updated = cur.rowcount if getattr(cur, "rowcount", None) is not None else 0

    now = datetime.now(timezone.utc).isoformat()
    entry["status"] = decision
    entry["updated_at"] = now
    queue["updated_at"] = now
    tmp_path = queue_path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as fh:
        fh.write(json.dumps(queue, indent=2, default=str, ensure_ascii=False) + "\n")
    os.replace(tmp_path, queue_path)

    _audit_bridge_write("review_decision", args)
    return {
        "queue_updated": True,
        "db_updated": int(db_updated),
        "id": id_str,
        "reason": reason,
        "decision": decision,
        "queue_path": queue_path,
    }


# ---------------------------------------------------------------------------
# Phase 5 — quarantine flag queue, read-only project context, audit log
# ---------------------------------------------------------------------------

_FLAG_SEVERITIES = ("low", "medium", "high")
_DECISION_RE = re.compile(r"decision|lesson|risk", re.IGNORECASE)


def cmd_flag_issue(args: dict) -> dict:
    """Quarantine an issue in the flag queue FILE ONLY — never touches the DB.

    args: {"item_id"?: str(uuid), "title": str, "description": str,
           "severity"?: "low"|"medium"|"high" = "medium", "reporter"?: str}

    Entry id = sha256((item_id or "") + "\n" + description)[:16]. The queue
    (audit.FLAG_QUEUE, default {"updated_at": ..., "flagged": [...]}) is
    loaded, mutated and written under an exclusive lockfile via
    audit.locked_json_update — a corrupt existing file raises RuntimeError
    and is never overwritten, and concurrent writers cannot tear the file.
    An existing id returns the stored entry unchanged (status preserved —
    idempotent); otherwise the new
    {id, item_id, title, description, severity, reporter, status: "pending",
    created_at} entry is appended. Every call logs a bridge_write audit
    event (outcome "ok"; a failure is logged by the dispatcher with
    outcome "error").
    """
    item_id = args.get("item_id")
    if item_id is not None:
        if not isinstance(item_id, str):
            raise ValueError("arg 'item_id' must be a uuid string or null")
        try:
            uuid.UUID(item_id)
        except (ValueError, AttributeError, TypeError):
            raise ValueError(f"arg 'item_id' is not a valid uuid: {item_id!r}") from None
    title = _req_str(args, "title")
    if not title.strip():
        raise ValueError("arg 'title' must be a non-empty string")
    description = _req_str(args, "description")
    if not description.strip():
        raise ValueError("arg 'description' must be a non-empty string")
    severity = args.get("severity")
    if severity is None:
        severity = "medium"
    if severity not in _FLAG_SEVERITIES:
        raise ValueError(f"arg 'severity' must be one of {_FLAG_SEVERITIES}, got {severity!r}")
    reporter = _opt_str(args, "reporter")

    issue_id = hashlib.sha256(
        ((item_id or "") + "\n" + description).encode("utf-8")
    ).hexdigest()[:16]

    def _mutate_queue(queue):
        if not isinstance(queue, dict) or not isinstance(queue.get("flagged"), list):
            raise RuntimeError(
                f"existing flag queue {audit.FLAG_QUEUE} has an unexpected shape "
                '(expected {"updated_at": ..., "flagged": [...]}) — refusing to overwrite'
            )
        for e in queue["flagged"]:
            if isinstance(e, dict) and e.get("id") == issue_id:
                return e  # idempotent: the stored entry comes back unchanged
        now = datetime.now(timezone.utc).isoformat()
        entry = {
            "id": issue_id,
            "item_id": item_id,
            "title": title,
            "description": description,
            "severity": severity,
            "reporter": reporter,
            "status": "pending",
            "created_at": now,
        }
        queue["flagged"].append(entry)
        queue["updated_at"] = now
        return entry

    # load -> mutate -> atomic write under the f"{FLAG_QUEUE}.lock" lockfile,
    # so concurrent flag_issue writers can never tear the queue file
    with audit.locked_json_update(
        audit.FLAG_QUEUE, _mutate_queue, lambda: {"updated_at": None, "flagged": []}
    ) as entry:
        pass
    _audit_bridge_write("flag_issue", args, issue_id=issue_id)
    return entry


def _project_slug(project: str) -> str:
    """slug = the lowercase [a-z0-9-] characters kept from the project name."""
    return "".join(
        ch for ch in project.lower() if ch in "abcdefghijklmnopqrstuvwxyz0123456789-"
    )


def _greedy_token_fill(items: list[dict], token_budget: int) -> tuple[list[dict], int]:
    """Local copy of the greedy token-budget fill (retrieval.search._apply_token_budget),
    estimating tokens as len(content)//4: keep items while they fit the budget,
    truncate the first non-fitting item to the remaining budget (marked
    "truncated"), and always keep at least one item. Returns (fitted prefix,
    estimated tokens used).
    """
    budget = max(1, int(token_budget))
    out: list[dict] = []
    used = 0
    for it in items:
        content = it.get("content") or ""
        t = len(content) // 4
        if used + t <= budget:
            out.append(it)
            used += t
            continue
        remaining = budget - used
        truncated = dict(it)
        truncated["content"] = content[: max(remaining, 1) * 4]
        truncated["truncated"] = True
        out.append(truncated)
        used += len(truncated["content"]) // 4
        break
    return out, used


def cmd_project_context(args: dict) -> dict:
    """Read-only per-project context package (logs NOTHING).

    args: {"project": str, "token_budget"?: int = 4000}

    Looks up the knowledge_items row with source_uri "project://<slug>"
    (slug = lowercase [a-z0-9-] chars of project); unknown project raises
    ValueError. Then gathers, read-only: page_rank_expand(conn, [proj_id],
    hops=2, top_k=20) and vector_search on "<project> decision lesson risk
    failed approach" (top 10). Returned content is capped by a greedy token
    budget (len//4 estimate, local copy of the fill logic).
    """
    from pipeline.db import get_connection
    from pipeline.embed import embed_text
    from retrieval.graph import page_rank_expand
    from retrieval.vector_search import vector_search

    project = _req_str(args, "project")
    slug = _project_slug(project)
    if not slug:
        raise ValueError(
            f"project {project!r} has no slug characters ([a-z0-9-] after lowercasing)"
        )
    token_budget = min(max(_opt_int(args, "token_budget", 4000), 1), 100_000)
    uri = f"project://{slug}"

    with get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM knowledge_items WHERE source_uri = %s", (uri,)
        ).fetchone()
        if row is None:
            raise ValueError(
                f"unknown project {project!r} — ingest a project://{slug} profile first, "
                "or use discover_relevant"
            )
        profile = {k: v for k, v in dict(row).items() if k != "embedding"}
        proj_id = row["id"]
        graph_related = page_rank_expand(conn, [proj_id], hops=2, top_k=20)
        query_emb = embed_text(f"{project} decision lesson risk failed approach")
        vector_related = vector_search(conn, query_emb, top_k=10)

    # Greedy budget over [profile, *graph, *vector] — the fill keeps a prefix,
    # so zip(pool, fitted) realigns each kept item with its section.
    graph_ids = {str(r.get("id")) for r in graph_related}
    pool: list[tuple[str, dict]] = [("profile", profile)]
    for r in graph_related:
        pool.append(("graph", r))
    for r in vector_related:
        if str(r.get("id")) in graph_ids or str(r.get("id")) == str(proj_id):
            continue
        pool.append(("vector", r))
    fitted, used = _greedy_token_fill([item for _, item in pool], token_budget)

    profile_out = None
    sections: dict[str, list[dict]] = {"graph": [], "vector": []}
    for (section, _), item in zip(pool, fitted):
        if section == "profile":
            profile_out = item
        else:
            sections[section].append(item)

    related = sections["graph"] + sections["vector"]
    decisions = [
        r for r in related
        if _DECISION_RE.search(f"{r.get('type') or ''} {r.get('title') or ''}")
    ]
    return {
        "project": project,
        "profile": profile_out,
        "graph_related": sections["graph"],
        "vector_related": sections["vector"],
        "decisions": decisions,
        "estimated_tokens": used,
    }


def cmd_discover_relevant(args: dict) -> dict:
    """Read-only auto-retrieval for new projects (logs NOTHING).

    args: {"query": str, "hops"?: int = 2 (clamped 1..5), "top_k"?: int = 10 (1..100)}

    Runs retrieval.search.search with all methods (candidate_k=30) and adds
    per-method counts plus suggested decision/lesson titles.
    """
    from retrieval.search import search

    query = _req_str(args, "query")
    if not query.strip():
        raise ValueError("arg 'query' must be a non-empty string")
    hops = min(max(_opt_int(args, "hops", 2), 1), 5)
    top_k = min(max(_opt_int(args, "top_k", 10), 1), 100)
    result = search(query, methods=None, hops=hops, top_k=top_k, candidate_k=30)
    results = result["results"]
    by_method_counts = {m: len(rows) for m, rows in result["by_method"].items()}

    def _titles(pattern: str) -> list[str]:
        rx = re.compile(pattern, re.IGNORECASE)
        return [
            r["title"] for r in results
            if r.get("title") and rx.search(f"{r.get('type') or ''} {r['title']}")
        ]

    return {
        "query": query,
        "results": results,
        "by_method_counts": by_method_counts,
        "suggested": {
            "decisions": _titles(r"decision|lesson"),
            "lessons": _titles(r"lesson"),
        },
    }


def cmd_log_audit(args: dict) -> dict:
    """Append one event to the audit log (audit.log_event).

    args: {"event": {"type": str, ...}} — "type" is popped and becomes the
    event name, the remaining keys are the event fields. The reserved
    envelope keys ts/actor/event are dropped from the caller fields, so they
    can never be spoofed. The MCP write server calls this at startup and
    after mutating tool calls.
    """
    event = args.get("event")
    if not isinstance(event, dict):
        raise ValueError("arg 'event' must be a JSON object with a 'type' key")
    event = dict(event)
    event_type = event.pop("type")
    if not isinstance(event_type, str) or not event_type:
        raise ValueError("arg 'event' must contain a non-empty string 'type'")
    # reserved envelope keys can never be injected by the caller
    for reserved in ("ts", "actor", "event"):
        event.pop(reserved, None)
    audit.log_event(event_type, **event)
    return {"logged": True}


COMMANDS = {
    "ingest": cmd_ingest,
    "search": cmd_search,
    "get_related": cmd_get_related,
    "link_items": cmd_link_items,
    "update_item": cmd_update_item,
    "delete_item": cmd_delete_item,
    "list_recent": cmd_list_recent,
    "curator": cmd_curator,
    "review_decision": cmd_review_decision,
    "flag_issue": cmd_flag_issue,
    "project_context": cmd_project_context,
    "discover_relevant": cmd_discover_relevant,
    "log_audit": cmd_log_audit,
}


# ---------------------------------------------------------------------------
# serve mode — persistent JSON-lines process (heavy imports paid once)
# ---------------------------------------------------------------------------

_SERVE_MODULES = (
    "pipeline.db",
    "pipeline.dedup",
    "pipeline.embed",
    "pipeline.chunk",
    "pipeline.extractors",
    "pipeline.github_import",
    "pipeline.raw_store",
    "pipeline.ingest",
    "retrieval.db",
    "retrieval.vector_search",
    "retrieval.fts",
    "retrieval.graph",
    "retrieval.merge",
    "retrieval.rerank",
    "retrieval.search",
)


def _eager_imports() -> None:
    """Import the pipeline + retrieval modules once at serve start.

    The dominant per-process cost is the sentence_transformers/torch import
    chain (~8-10s); importing it here means it is paid once per serve process
    instead of once per command. Model weights stay lazy — they load on the
    first embed/rerank call and are then cached in-process. Import failures
    are logged to stderr and are never fatal: the affected commands simply
    return error envelopes when invoked.
    """
    for mod in _SERVE_MODULES:
        try:
            importlib.import_module(mod)
        except Exception as e:
            print(f"[bridge serve] eager import of {mod} failed: {type(e).__name__}: {e}",
                  file=sys.stderr)
    try:
        import sentence_transformers  # noqa: F401 — pay the heavy import once
    except Exception as e:
        print(f"[bridge serve] sentence_transformers import failed "
              f"(models will lazy-load or fail per command): {type(e).__name__}: {e}",
              file=sys.stderr)


def _write_all(fd: int, data: bytes) -> None:
    """os.write loop — a single response line can exceed PIPE_BUF, so writes
    to a pipe may be partial."""
    view = memoryview(data)
    while view:
        n = os.write(fd, view)
        view = view[n:]


def _serve_handle_line(line: str) -> dict:
    """Parse and dispatch ONE serve-mode request line; return the response payload.

    Never raises: every failure (bad JSON, wrong shape, unknown command,
    handler exception) becomes an {"ok": false, "error": ...} payload.
    """
    try:
        req = json.loads(line)
        if not isinstance(req, dict):
            raise ValueError("request line must be a JSON object")
    except (json.JSONDecodeError, ValueError) as e:
        return {"id": None, "ok": False,
                "error": _sanitize_error(f"malformed request line: {e}")}
    rid = req.get("id")
    command = req.get("command")
    args = req.get("args", {})
    if not isinstance(command, str) or not command or not isinstance(args, dict):
        return {"id": None, "ok": False,
                "error": _sanitize_error(
                    "request must look like "
                    '{"id": <any>, "command": <string>, "args": {<object>}}')}
    handler = COMMANDS.get(command)
    if handler is None:
        return {"id": rid, "ok": False,
                "error": _sanitize_error(
                    f"unknown command {command!r}; choose from {sorted(COMMANDS)}")}
    try:
        result = handler(args)
        return {"id": rid, "ok": True, "result": _jsonable(result)}
    except Exception as e:
        print(f"[bridge serve] command {command!r} failed:\n"
              f"{traceback.format_exc()}", file=sys.stderr)
        # a failed mutating command still logs bridge_write, with outcome="error"
        _audit_bridge_write_error(command, args, e)
        return {"id": rid, "ok": False,
                "error": _sanitize_error(f"{type(e).__name__}: {e}")}


def serve() -> int:
    """Persistent JSON-lines command loop. Returns the process exit code.

    Protocol (one request per stdin line, exactly one response line each):

        request:  {"id": <any json value>, "command": "<cmd>", "args": {...}}
        response: {"id": <same>, "ok": true,  "result": ...}
                  {"id": <same>, "ok": false, "error": "..."}
        malformed request line: {"id": null, "ok": false, "error": "..."}

    fd 1 is redirected onto fd 2 for the whole loop so library chatter can
    never pollute the response stream; responses go to the saved original
    stdout fd via os.write. stdin EOF -> exit 0. A single undecodable or
    unreadable stdin line (UnicodeDecodeError / OSError) gets an error
    envelope and the loop CONTINUES — one bad line never kills the process.
    """
    _eager_imports()
    audit.log_event("bridge_serve_start", pid=os.getpid())
    print(f"[bridge serve] ready (commands: {', '.join(sorted(COMMANDS))})", file=sys.stderr)

    saved_fd = os.dup(1)
    os.dup2(2, 1)  # from here on, anything printing to stdout lands on stderr
    # Read stdin as BYTES and decode each line manually: a text-mode readline()
    # that hits a UnicodeDecodeError can leave the stream at EOF (swallowing
    # buffered lines), so the byte level is the only robust way to guarantee
    # one bad line never kills the loop.
    stdin_buffer = getattr(sys.stdin, "buffer", None)
    try:
        while True:
            try:
                raw = stdin_buffer.readline() if stdin_buffer is not None else sys.stdin.readline()
            except OSError as e:
                # Unreadable line: emit {"id": null, ok: false} and keep serving.
                payload = {"id": None, "ok": False,
                           "error": _sanitize_error(
                               f"unreadable request line: {type(e).__name__}: {e}")}
            else:
                if raw == b"" or raw == "":  # EOF
                    break
                if isinstance(raw, bytes):
                    try:
                        line = raw.decode("utf-8")
                    except UnicodeDecodeError as e:
                        payload = {"id": None, "ok": False,
                                   "error": _sanitize_error(
                                       f"unreadable request line: {type(e).__name__}: {e}")}
                        line = None
                    else:
                        payload = _serve_handle_line(line)
                else:
                    payload = _serve_handle_line(raw)
            try:
                _write_all(saved_fd,
                           (json.dumps(payload, default=_jsonable, ensure_ascii=False)
                            + "\n").encode("utf-8"))
            except BrokenPipeError:
                break  # parent closed our stdout — nothing left to do
    finally:
        try:
            os.dup2(saved_fd, 1)
        except OSError:
            pass
        os.close(saved_fd)
    print("[bridge serve] stdin EOF — exiting 0", file=sys.stderr)
    return 0


# ---------------------------------------------------------------------------
# Entry point — enforce the one-JSON-line stdout contract
# ---------------------------------------------------------------------------

def _emit(payload: dict, ok: bool) -> int:
    line = json.dumps(payload, default=_jsonable, ensure_ascii=False)
    sys.stdout.write(line + "\n")
    sys.stdout.flush()
    return 0 if ok else 1


def main(argv: list[str]) -> int:
    if len(argv) < 2 or argv[1] in ("-h", "--help"):
        print(USAGE, file=sys.stderr)
        return _emit({"ok": False, "error": USAGE}, False)

    command = argv[1]
    if command == "serve":
        # Persistent mode: ignore any extra argv, run the JSON-lines loop.
        return serve()
    raw_args = argv[2] if len(argv) > 2 else "{}"
    try:
        args = json.loads(raw_args)
        if not isinstance(args, dict):
            raise ValueError("json-args must be a JSON object")
    except (json.JSONDecodeError, ValueError) as e:
        return _emit({"ok": False, "error": _sanitize_error(f"invalid json-args: {e}")}, False)

    handler = COMMANDS.get(command)
    if handler is None:
        return _emit(
            {"ok": False, "error": _sanitize_error(
                f"unknown command {command!r}; choose from {sorted(COMMANDS)}")},
            False,
        )

    # Redirect fd 1 onto fd 2 while the command runs so that any library
    # chatter (progress bars, warnings) can never pollute stdout. The single
    # JSON envelope is written only after fd 1 is restored.
    saved_fd = os.dup(1)
    os.dup2(2, 1)
    try:
        result = handler(args)
        payload, ok = {"ok": True, "result": _jsonable(result)}, True
    except Exception as e:
        print(f"[bridge] command {command!r} failed:\n{traceback.format_exc()}", file=sys.stderr)
        # a failed mutating command still logs bridge_write, with outcome="error"
        _audit_bridge_write_error(command, args, e)
        payload = {"ok": False, "error": _sanitize_error(f"{type(e).__name__}: {e}")}
        ok = False
    finally:
        try:
            sys.stdout.flush()
        except Exception:
            pass
        os.dup2(saved_fd, 1)
        os.close(saved_fd)

    return _emit(payload, ok)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
