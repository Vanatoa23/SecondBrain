#!/usr/bin/env python
"""extract_lessons.py — Phase 4 lesson extraction from postmortems / decision docs.

Usage (from the repo root):

    python scripts/extract_lessons.py <file-or-dir> [--glob '*.md']

Reads Markdown/text files (postmortems, decision logs, retrospectives) and
detects lesson/decision content HEURISTICALLY:

  * sections headed "Lesson", "Lessons learned", "Takeaways", "Retrospective",
    "Postmortem", "After action", "What went wrong/right", "Decision(s)"
    (any ATX heading level) — every bullet or prose paragraph in such a
    section becomes a candidate;
  * bullet lines starting with "Lesson:" / "Decision:" anywhere in the doc.

Each candidate becomes ONE knowledge item, ingested through ONE persistent
`python -m bridge serve` child (the locked frontend protocol — the heavy
model load is paid once per run, not once per candidate):

    source_uri = f"lesson://<slug(file)>#<n>"     (n = 1-based candidate index)
    title      = f"[{kind}] {first line}"          (kind = lesson | decision)
    source_type= "text", payload = the candidate text

The kind travels in the title (ingest has no metadata arg on the insert
path); type stays 'note' and the curator/extraction layer can refine later.
Dedup is handled by the pipeline itself: re-running the same input produces
the same candidate texts, so the whole-document content hash marks every row
"skipped-duplicate" and no new rows are created (idempotent re-runs).

Optional LLM refinement (env-gated, default OFF): when EXTRACT_ENABLED=1 and
EXTRACT_API_URL is set, pipeline.extract.extract_knowledge is called lazily
on each document and extra lesson/decision ENTITIES it finds are appended as
candidates. Any failure there is sanitized and swallowed — the heuristic
candidates are always ingested regardless.

Connection URL comes from the environment ONLY (never a CLI flag, so no
credential can end up in a process list / shell history):

    TEST_DATABASE_URL first (tests point it at a throwaway test DB),
    then DATABASE_URL (real ops use — same convention as scripts/curator.py
    and scripts/audit_edges.py).

Per-candidate actions are printed as lines starting with one of:

    ingested | skipped-duplicate | skipped-empty | error

followed by a "summary: files=F candidates=C ingested=N skipped-duplicate=M
skipped-empty=K errors=E" line.

Exit codes: 0 = run completed with no errors (duplicates and zero-candidate
files are fine), 1 = fatal (bad/missing input path, unreadable file, no
database URL, or any per-candidate ingest error).

Security: all output is passed through _redact() — API keys (EXTRACT_API_KEY
/ VISION_API_KEY / GITHUB_TOKEN values), Bearer tokens, Authorization headers
and api_key= URL parameters are stripped from every printed error; the
connection URL itself is never printed (host/dbname only). All SQL happens
inside bridge/pipeline — every statement there is parameterized.

Note for tests: when this tool is run BY TESTS, the harness only ever points
TEST_DATABASE_URL at a database whose name matches (^|_)test(_|$).
"""

from __future__ import annotations

import argparse
import json
import os
import queue
import re
import subprocess
import sys
import threading
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))  # for the lazy `pipeline` import (llm refine)

DEFAULT_GLOB = "*.md"
LESSON_URI_PREFIX = "lesson://"
MAX_CANDIDATES_PER_FILE = 200
MAX_TITLE_LEN = 120
INGEST_TIMEOUT_S = 600  # per-candidate timeout on the serve child

# ---------------------------------------------------------------------------
# Heuristic detection patterns
# ---------------------------------------------------------------------------

HEADING_RE = re.compile(r"^(#{1,6})\s+(.*?)\s*#*\s*$")
FENCE_RE = re.compile(r"^\s*(```+|~~~+)")
# Bullets (unordered -/*/+) AND numbered-list items (1. / 1) ).
BULLET_RE = re.compile(r"^(\s*)(?:[-*+]|\d+[.)])\s+(.*)$")
BULLET_KIND_RE = re.compile(r"^(lesson|decision)\s*:\s*(.+)$", re.IGNORECASE)
# Section headings that mark lesson-bearing content.
LESSON_HEADING_RE = re.compile(
    r"lessons?|learnings|takeaways?|retrospective|post-?mortem|after[- ]action"
    r"|what went (wrong|right)",
    re.IGNORECASE,
)
DECISION_HEADING_RE = re.compile(r"\bdecisions?\b", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Redaction / credential hygiene
# ---------------------------------------------------------------------------


def _redact(message) -> str:
    """Strip secrets from an error/log string before it is printed.

    Removes EXTRACT_API_KEY / VISION_API_KEY / GITHUB_TOKEN values wherever
    they occur, plus Bearer tokens, Authorization headers, and URL-embedded
    api_key= parameters — so a failing provider call can never leak
    credentials into stdout/stderr.
    """
    s = str(message)
    s = re.sub(r"(?i)\bbearer\s+[^\s,;\"']+", "bearer <redacted>", s)
    s = re.sub(r"(?i)(authorization\s*[:=]\s*)[^\s,;\"']+", r"\1<redacted>", s)
    s = re.sub(r"(?i)([?&]api_key=)[^&#\s]+", r"\1<redacted>", s)
    for var in ("EXTRACT_API_KEY", "VISION_API_KEY", "GITHUB_TOKEN"):
        val = os.environ.get(var)
        if val:
            s = s.replace(val, "<redacted-api-key>")
    return s


def _db_desc(url: str) -> str:
    """Credential-free label for the connection URL: host/dbname only."""
    try:
        from psycopg import conninfo

        d = conninfo.conninfo_to_dict(url)
        d.pop("password", None)
        return f"{d.get('host') or ''}/{d.get('dbname') or ''}"
    except Exception:
        m = re.search(r"://(?:[^@/]*@)?([^/:?]+)(?::\d+)?/([^/?]+)", url)
        if m:
            return f"{m.group(1)}/{m.group(2)}"
        return "<unparseable>"


def resolve_db_url() -> str | None:
    """TEST_DATABASE_URL first (tests), then DATABASE_URL (ops)."""
    url = (os.environ.get("TEST_DATABASE_URL") or "").strip()
    if url:
        return url
    return (os.environ.get("DATABASE_URL") or "").strip() or None


# ---------------------------------------------------------------------------
# File reading + heuristic parsing (pure — no DB, no subprocess)
# ---------------------------------------------------------------------------


def read_text_file(path: str | Path) -> str:
    """Read a text/markdown file. Raises ValueError with a clean message on
    any OS-level failure (missing file, is-a-directory, permission)."""
    path = Path(path)
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except OSError as e:
        raise ValueError(f"cannot read file {path}: {e}") from None


def slugify(name: str, max_len: int = 80) -> str:
    """URL/path-friendly slug for the lesson:// source URI."""
    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    slug = re.sub(r"-{2,}", "-", slug)[:max_len].strip("-")
    return slug or "doc"


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", text.lower()).strip()


def _make_title(text: str) -> str:
    first = text.splitlines()[0].strip() if text.splitlines() else ""
    first = first[:MAX_TITLE_LEN]
    return first


def _candidate(kind: str, text: str, seen: set) -> dict | None:
    text = text.strip()
    if len(text) < 3:
        return None
    key = _norm(text)
    if key in seen:
        return None
    seen.add(key)
    return {"kind": kind, "title": _make_title(text), "text": text}


def parse_lessons(text: str) -> list[dict]:
    """Extract lesson/decision candidates from markdown-ish text. Pure.

    Returns a list of {"kind": "lesson"|"decision", "title": str, "text": str}
    in document order, deduplicated by normalized text, capped at
    MAX_CANDIDATES_PER_FILE. Never raises on arbitrary input.

    Heading detection ignores '#' lines inside fenced code blocks (```/~~~);
    both bullet (-/*/+) and numbered-list (1./1)) items count as bullets.
    """
    candidates: list[dict] = []
    seen: set = set()

    section: dict | None = None  # {"kind": str} — the open lesson/decision section
    open_bullet: list[str] | None = None  # lines of the bullet being accumulated
    para: list[str] | None = None  # lines of the prose paragraph being accumulated
    in_fence: bool = False  # inside a fenced code block (``` / ~~~)

    def flush_bullet() -> None:
        nonlocal open_bullet
        if open_bullet:
            lines = open_bullet
            open_bullet = None
            head = lines[0].strip()
            m = BULLET_KIND_RE.match(head)
            if m:
                kind, body = m.group(1).lower(), m.group(2)
                lines = [body] + lines[1:]
            else:
                kind = section["kind"] if section else "lesson"
            cand = _candidate(kind, "\n".join(lines), seen)
            if cand and len(candidates) < MAX_CANDIDATES_PER_FILE:
                candidates.append(cand)

    def flush_para() -> None:
        nonlocal para
        if para:
            lines = para
            para = None
            kind = section["kind"] if section else "lesson"
            cand = _candidate(kind, "\n".join(lines), seen)
            if cand and len(candidates) < MAX_CANDIDATES_PER_FILE:
                candidates.append(cand)

    for raw_line in text.splitlines():
        line = raw_line.rstrip()
        # Fenced code blocks: a '#' line inside a fence is CODE, not a heading
        # (track the fence state so it can never open/close a lesson section),
        # and no fence content is parsed as bullet/prose either.
        if FENCE_RE.match(line):
            in_fence = not in_fence
            flush_bullet()
            flush_para()
            continue
        if in_fence:
            continue
        hm = HEADING_RE.match(line)
        if hm:
            flush_bullet()
            flush_para()
            # ANY heading closes the open section; only a heading that itself
            # names lessons/decisions opens a new one (so "## Timeline" inside
            # a postmortem does not leak into the candidates).
            section = None
            heading = hm.group(2)
            if LESSON_HEADING_RE.search(heading) or DECISION_HEADING_RE.search(heading):
                kind = "decision" if DECISION_HEADING_RE.search(heading) else "lesson"
                section = {"kind": kind}
            continue
        if section is not None:
            bm = BULLET_RE.match(line)
            if bm and bm.group(2).strip():
                flush_para()
                flush_bullet()
                open_bullet = [bm.group(2)]
                continue
            if not line.strip():
                flush_bullet()
                flush_para()
                continue
            # continuation of the open bullet (indented) vs a prose paragraph
            if open_bullet is not None and raw_line[:1] in (" ", "\t"):
                open_bullet.append(line.strip())
            else:
                flush_bullet()
                if para is None:
                    para = [line.strip()]
                else:
                    para.append(line.strip())
            continue
        # outside any lesson/decision section: only explicit "Lesson:"/"Decision:" bullets
        bm = BULLET_RE.match(line)
        if bm:
            m = BULLET_KIND_RE.match(bm.group(2).strip())
            if m:
                cand = _candidate(m.group(1).lower(), m.group(2), seen)
                if cand and len(candidates) < MAX_CANDIDATES_PER_FILE:
                    candidates.append(cand)

    flush_bullet()
    flush_para()
    return candidates


def llm_refine_candidates(doc_text: str, existing: list[dict]) -> list[dict]:
    """Optional LLM refinement (EXTRACT_ENABLED=1 + EXTRACT_API_URL set).

    Lazily calls pipeline.extract.extract_knowledge on the document and
    appends lesson/decision ENTITIES not already covered by the heuristic
    candidates. Catch-all: any failure is sanitized, logged to stderr, and
    swallowed — heuristic candidates are unaffected.
    """
    if os.environ.get("EXTRACT_ENABLED") != "1":
        return []
    if not (os.environ.get("EXTRACT_API_URL") or "").strip():
        return []
    try:
        from pipeline import extract as llm_extract  # lazy — default run pays nothing

        knowledge = llm_extract.extract_knowledge(doc_text[:20000])
    except Exception as exc:
        try:
            from pipeline.extract import _sanitize

            detail = _sanitize(f"{type(exc).__name__}: {exc}")
        except Exception:
            detail = type(exc).__name__  # never leak raw exception text
        print(f"[extract_lessons] llm refinement failed (ignored): {detail}",
              file=sys.stderr)
        return []
    have = {_norm(c["title"]) for c in existing}
    out: list[dict] = []
    for ent in knowledge.get("entities", []):
        etype = str(ent.get("type", "")).strip().lower()
        if etype not in ("lesson", "decision"):
            continue
        name = str(ent.get("name", "")).strip()
        if not name:
            continue
        key = _norm(name)
        if key in have:
            continue
        have.add(key)
        out.append({
            "kind": etype,
            "title": name[:MAX_TITLE_LEN],
            # The ACTUAL lesson text extracted by the LLM (the entity name),
            # not boilerplate — this is the payload that gets ingested.
            "text": name,
        })
    return out


def collect_files(target: str, pattern: str) -> list[Path]:
    """Resolve the CLI target to a sorted list of files. Raises ValueError
    with a clean message when the target does not exist."""
    p = Path(target)
    if p.is_file():
        return [p]
    if p.is_dir():
        return sorted(f for f in p.glob(pattern) if f.is_file())
    raise ValueError(f"no such file or directory: {target}")


# ---------------------------------------------------------------------------
# Ingest via ONE persistent `bridge serve` child (locked frontend protocol)
# ---------------------------------------------------------------------------


class BridgeServe:
    """One long-lived `python -m bridge serve` child speaking the JSON-lines
    protocol: {"id", "command", "args"} on one stdin line -> exactly one
    response line with the same id on stdout.

    The torch/model load is paid once per serve process, not once per
    candidate. A daemon reader thread feeds a queue so each request can
    time out. If the child dies it is respawned once and the request
    retried once (mirrors the MCP server's respawn-once behavior); reader
    lines are tagged with a generation counter so the stale reader queue of
    the dead child (including its EOF sentinel) can never feed the new
    process.
    """

    def __init__(self, database_url: str):
        self._database_url = database_url
        self._next_id = 1
        self._lines: queue.Queue = queue.Queue()
        self._gen = 0  # generation counter — stale readers of a dead child are ignored
        self.proc: subprocess.Popen | None = None
        self._spawn()

    def _spawn(self) -> None:
        env = dict(os.environ)
        env["DATABASE_URL"] = self._database_url  # bridge reads DATABASE_URL
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "bridge", "serve"],
            cwd=str(REPO_ROOT),
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            bufsize=1,
        )
        self._gen += 1
        gen = self._gen
        reader = threading.Thread(target=self._read_loop, args=(gen,), daemon=True)
        reader.start()

    def _read_loop(self, gen: int) -> None:
        try:
            for line in self.proc.stdout:
                self._lines.put((gen, line))
        except Exception:
            pass
        finally:
            self._lines.put((gen, None))  # EOF sentinel: this serve child is gone

    def request(self, command: str, args: dict, timeout: float = INGEST_TIMEOUT_S) -> dict:
        """Send one request and return the id-matched response envelope.

        Respawns the child once (and retries the request once) if it died.
        Raises TimeoutError on timeout, EOFError if a respawn also fails.
        """
        try:
            return self._request_once(command, args, timeout)
        except EOFError:
            self._spawn()
            return self._request_once(command, args, timeout)

    def _request_once(self, command: str, args: dict, timeout: float) -> dict:
        rid = self._next_id
        self._next_id += 1
        line = json.dumps({"id": rid, "command": command, "args": args})
        try:
            self.proc.stdin.write(line + "\n")
            self.proc.stdin.flush()
        except (BrokenPipeError, OSError, ValueError) as e:
            raise EOFError(f"bridge serve stdin write failed: {e}") from None
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"no response to {command!r} within {timeout}s")
            try:
                item = self._lines.get(timeout=remaining)
            except queue.Empty:
                raise TimeoutError(f"no response to {command!r} within {timeout}s") from None
            gen, raw = item
            if gen != self._gen:
                # Stale line/sentinel from the PREVIOUS (dead) child — never
                # let the old reader queue feed the new process.
                continue
            if raw is None:
                raise EOFError("bridge serve process exited")
            item = raw.strip()
            if not item:
                continue
            try:
                msg = json.loads(item)
            except json.JSONDecodeError:
                continue  # non-JSON noise on stdout — ignore
            if isinstance(msg, dict) and msg.get("id") == rid:
                return msg
            # a response for a different id (requests are serial, so this
            # should not happen) — ignore it

    def close(self) -> None:
        proc = self.proc
        if proc is None:
            return
        try:
            proc.stdin.close()
            proc.wait(timeout=60)
        except Exception:
            try:
                proc.kill()
                proc.wait(timeout=10)
            except Exception:
                pass


def ingest_candidate(serve: BridgeServe, cand: dict, source_uri: str, title: str) -> str:
    """Ingest one candidate through the persistent serve child.

    Returns the action string: "ingested", "skipped-duplicate",
    "skipped-empty" or "error: ..." (redacted).
    """
    args = {
        "source_type": "text",
        "payload": cand["text"],
        "source_uri": source_uri,
        "title": title,
    }
    try:
        resp = serve.request("ingest", args)
    except Exception as e:
        return f"error: cannot run bridge ingest: {_redact(f'{type(e).__name__}: {e}')}"
    if resp.get("ok"):
        status = (resp.get("result") or {}).get("status")
        if status == "inserted":
            return "ingested"
        if status == "duplicate":
            return "skipped-duplicate"
        if status == "empty":
            return "skipped-empty"
        return f"error: unexpected ingest status {_redact(status)!r}"
    err = _redact(resp.get("error", ""))
    return f"error: bridge ingest failed: {err or 'no output'}"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Extract lesson/decision candidates from postmortem/decision "
                    "markdown files and ingest them via one persistent bridge serve child."
    )
    parser.add_argument("target", help="a markdown/text file, or a directory to scan with --glob")
    parser.add_argument("--glob", default=DEFAULT_GLOB,
                        help=f"file pattern for directory targets (default: {DEFAULT_GLOB!r}; "
                             f"use '**/*.md' to recurse)")
    args = parser.parse_args(argv)

    # Input validation BEFORE any DB access — a bad path is a clean exit 1.
    try:
        files = collect_files(args.target, args.glob)
    except ValueError as e:
        print(f"error: {_redact(e)}", file=sys.stderr)
        return 1

    if not files:
        print(f"no files matched {args.glob!r} under {args.target}")
        print("summary: files=0 candidates=0 ingested=0 skipped-duplicate=0 "
              "skipped-empty=0 errors=0")
        return 0

    # Read + parse every file first; an unreadable file is fatal (exit 1).
    work: list[tuple[Path, list[dict]]] = []
    for path in files:
        try:
            text = read_text_file(path)
        except ValueError as e:
            print(f"error: {_redact(e)}", file=sys.stderr)
            return 1
        candidates = parse_lessons(text)
        candidates.extend(llm_refine_candidates(text, candidates))
        work.append((path, candidates))

    total_candidates = sum(len(c) for _, c in work)
    for path, candidates in work:
        print(f"file {path}: candidates={len(candidates)}")

    counts = {"ingested": 0, "skipped-duplicate": 0, "skipped-empty": 0, "errors": 0}
    if total_candidates:
        database_url = resolve_db_url()
        if not database_url:
            print(
                "error: no database URL — set TEST_DATABASE_URL (tests) or "
                "DATABASE_URL (ops). Example: "
                "DATABASE_URL=postgresql://postgres@127.0.0.1:5433/secondbrain "
                "python scripts/extract_lessons.py postmortems/",
                file=sys.stderr,
            )
            return 1
        serve = BridgeServe(database_url)
        try:
            for path, candidates in work:
                slug = slugify(str(path))
                for n, cand in enumerate(candidates, start=1):
                    source_uri = f"{LESSON_URI_PREFIX}{slug}#{n}"
                    title = f"[{cand['kind']}] {cand['title']}"
                    action = ingest_candidate(serve, cand, source_uri, title)
                    if action == "ingested":
                        counts["ingested"] += 1
                    elif action in ("skipped-duplicate", "skipped-empty"):
                        counts[action] += 1
                    else:
                        counts["errors"] += 1
                    print(f"{action} {source_uri} ({title})")
        finally:
            serve.close()
    else:
        print("no lesson/decision candidates found in any input file")

    print(
        f"summary: files={len(files)} candidates={total_candidates}"
        f" ingested={counts['ingested']}"
        f" skipped-duplicate={counts['skipped-duplicate']}"
        f" skipped-empty={counts['skipped-empty']}"
        f" errors={counts['errors']}"
    )
    return 1 if counts["errors"] else 0


if __name__ == "__main__":
    sys.exit(main())
