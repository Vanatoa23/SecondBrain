"""AI-chat export importer — ingest conversations from an AI chat export JSON
file into the Second Brain knowledge base (single-shot).

Usage (from the repo root):

    python scripts/export_chat.py <export.json> [--source-uri-prefix chat://] [--once]

Detected export shapes (generic, not vendor-locked):

  * a JSON list of conversations:  [{"title": ..., "messages": [...]}, ...]
  * an object wrapping them:       {"conversations": [{"title": ..., "messages": [...]}, ...]}
  * a ChatGPT conversations.json export: a list (or {"conversations": [...]})
    where each conversation has a "mapping" {node_id: {"message": {"author":
    {"role": ...}, "content": {"parts": [...]}}, "children": [...]}} — the
    messages are walked from the root node(s) (no parent) following
    "children" in order; title = conv.get("title")
  * a Claude export: a list of conversations with "chat_messages":
    [{"sender": "human"|"assistant", "text": ...}] — sender human->user,
    assistant->assistant; title = conv.get("name") or the uuid

For the legacy "messages" shape each message is an object with a "role" and
either "content" or "text"; the content may be a plain string OR a list of
parts like [{"type": "text", "text": "..."}, ...] (non-text parts are
skipped, text parts are concatenated). The ChatGPT/Claude adapters normalize
into the same {"role", "content"} messages.

Every conversation becomes ONE knowledge item:

    text       = "# <title>\n\n" + "<role>: <content>" lines joined by "\n"
    source_uri = f"{prefix}{slug(title)}-{i}"     (i = 0-based conversation index)
    title      = <title>

Ingestion goes through ONE persistent `python -m bridge serve` child for the
whole export (line-delimited JSON protocol: {"id", "command": "ingest",
"args": {...}} in, one response line out per request) — the heavy model load
is paid once per export instead of once per conversation. The child reads
DATABASE_URL from the environment — this tool is DB-backed, so DATABASE_URL
MUST be set:

    DATABASE_URL=postgresql://postgres@127.0.0.1:5433/secondbrain \
        python scripts/export_chat.py export.json

Per-conversation actions are printed as lines starting with one of:

    ingested | skipped-duplicate | skipped-empty | error

followed by a "summary: ingested=N skipped-duplicate=M skipped-empty=K errors=E"
line.

Exit codes: 0 = run completed with no errors (duplicates are fine), 1 = fatal
(DATABASE_URL unset, unreadable/invalid export file, or any per-conversation
error in --once mode).

Note for tests: when this tool is run BY TESTS, the harness only ever points
DATABASE_URL at a database whose name matches (^|_)test(_|$); the tool itself
just uses DATABASE_URL and has no test-specific guard.

All SQL happens inside bridge/pipeline — every statement there is parameterized.
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

DEFAULT_PREFIX = "chat://"


# ---------------------------------------------------------------------------
# Parsing (pure — no DB, no subprocess)
# ---------------------------------------------------------------------------


def slugify(title: str, max_len: int = 60) -> str:
    """URL/path-friendly slug from a conversation title."""
    slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")
    slug = re.sub(r"-{2,}", "-", slug)[:max_len].strip("-")
    return slug or "conversation"


def _message_content(msg: dict) -> str:
    """Extract the text of one message.

    "content" (or "text") may be a plain string or a list of parts such as
    [{"type": "text", "text": "..."}] — text parts are concatenated, other
    part types are skipped.
    """
    raw = msg.get("content")
    if raw is None:
        raw = msg.get("text")
    if raw is None:
        return ""
    if isinstance(raw, str):
        return raw
    if isinstance(raw, list):
        parts: list[str] = []
        for part in raw:
            if isinstance(part, str):
                parts.append(part)
            elif isinstance(part, dict):
                # Only text parts carry content; skip images/tool calls/etc.
                if part.get("type") in (None, "text") and isinstance(part.get("text"), str):
                    parts.append(part["text"])
                elif part.get("type") == "text" and isinstance(part.get("content"), str):
                    parts.append(part["content"])
        return "".join(parts)
    return str(raw)


def _conversation_text(title: str, messages: list) -> str:
    lines = [f"# {title}", ""]
    for msg in messages:
        if not isinstance(msg, dict):
            continue
        role = str(msg.get("role") or "unknown")
        content = _message_content(msg)
        lines.append(f"{role}: {content}")
    return "\n".join(lines)


def _chatgpt_messages(conv: dict) -> list[dict]:
    """ChatGPT conversations.json adapter: walk the "mapping" tree from the
    root node(s) (no parent) following "children" in order and emit the
    non-empty user/assistant messages as {"role", "content"} dicts."""
    mapping = conv.get("mapping")
    if not isinstance(mapping, dict) or not mapping:
        raise ValueError('ChatGPT conversation has no non-empty "mapping" object')
    children_of = {}
    roots = []
    for node_id, node in mapping.items():
        if not isinstance(node, dict):
            continue
        children_of[node_id] = node.get("children") or []
        if not node.get("parent"):
            roots.append(node_id)
    messages: list[dict] = []
    seen_nodes: set = set()

    def walk(node_id):
        if node_id in seen_nodes:
            return
        seen_nodes.add(node_id)
        node = mapping.get(node_id)
        if not isinstance(node, dict):
            return
        msg = node.get("message")
        if isinstance(msg, dict):
            author = msg.get("author") or {}
            role = author.get("role") if isinstance(author, dict) else None
            content = msg.get("content") or {}
            parts = content.get("parts") if isinstance(content, dict) else None
            if role in ("user", "assistant") and isinstance(parts, list):
                text_parts = [
                    p if isinstance(p, str) else (p.get("text") or "")
                    for p in parts
                    if isinstance(p, str) or isinstance(p, dict)
                ]
                text = "".join(text_parts).strip()
                if text:
                    messages.append({"role": role, "content": text})
        for child in children_of.get(node_id, []):
            walk(child)

    for root in sorted(roots):
        walk(root)
    return messages


def _claude_messages(conv: dict) -> list[dict]:
    """Claude export adapter: "chat_messages" [{"sender": "human"|"assistant",
    "text": ...}] -> [{"role": "user"|"assistant", "content": ...}]."""
    raw = conv.get("chat_messages")
    if not isinstance(raw, list):
        raise ValueError('Claude conversation has no "chat_messages" list')
    messages: list[dict] = []
    for m in raw:
        if not isinstance(m, dict):
            continue
        sender = str(m.get("sender") or "").lower()
        text = m.get("text")
        if text is None:
            text = m.get("content")
        if isinstance(text, list):  # tolerate part-lists like the legacy shape
            text = _message_content({"content": text})
        if sender == "human":
            role = "user"
        elif sender == "assistant":
            role = "assistant"
        else:
            continue
        if isinstance(text, str) and text.strip():
            messages.append({"role": role, "content": text})
    return messages


def parse_export(path: str | Path) -> list[dict]:
    """Parse an AI-chat export file into a list of conversation dicts.

    Returns a list of {"index", "title", "text", "n_messages"} — one entry per
    conversation, in file order. "text" is the knowledge-item body:
    "# <title>\n\n" + "<role>: <content>" lines.

    Raises ValueError with a clear message for unreadable files, malformed
    JSON, or a top-level shape that is neither a list of conversations nor an
    object with a "conversations" list (per-conversation shapes: legacy
    "messages", ChatGPT "mapping", Claude "chat_messages").
    """
    path = Path(path)
    try:
        raw = path.read_text(encoding="utf-8", errors="replace")
    except OSError as e:
        raise ValueError(f"cannot read export file {path}: {e}") from None
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"export file {path} is not valid JSON: {e}") from None

    if isinstance(data, dict) and isinstance(data.get("conversations"), list):
        conversations = data["conversations"]
    elif isinstance(data, list):
        conversations = data
    else:
        raise ValueError(
            f"export file {path} has an unsupported shape: expected a JSON list of "
            'conversations or an object like {"conversations": [...]}'
        )

    out: list[dict] = []
    for i, conv in enumerate(conversations):
        if not isinstance(conv, dict):
            raise ValueError(f"conversation #{i} in {path} is not a JSON object")

        messages = conv.get("messages")
        if isinstance(messages, list):
            # legacy flat shape: {"messages": [{"role", "content"|"text"}, ...]}
            title = conv.get("title") or conv.get("name") or f"conversation-{i}"
        elif isinstance(conv.get("mapping"), dict):
            # ChatGPT conversations.json: {"title"?, "mapping": {node_id: ...}}
            messages = _chatgpt_messages(conv)
            title = conv.get("title") or f"conversation-{i}"
        elif isinstance(conv.get("chat_messages"), list):
            # Claude export: {"name"?, "uuid"?, "chat_messages": [...]}
            messages = _claude_messages(conv)
            title = conv.get("name") or conv.get("uuid") or f"conversation-{i}"
        else:
            raise ValueError(
                f'conversation #{i} in {path} has an unsupported shape: expected '
                '"messages" (list), "mapping" (ChatGPT), or "chat_messages" (Claude)'
            )

        title = str(title)
        out.append({
            "index": i,
            "title": title,
            "text": _conversation_text(title, messages),
            "n_messages": len(messages),
        })
    return out


# ---------------------------------------------------------------------------
# Ingest via ONE persistent `bridge serve` child
# ---------------------------------------------------------------------------

INGEST_TIMEOUT_S = 600  # per-conversation timeout on the serve child


class BridgeServe:
    """One long-lived `python -m bridge serve` child speaking the JSON-lines
    protocol: {"id", "command", "args"} on one stdin line -> exactly one
    response line with the same id on stdout.

    The torch/model load is paid once per serve process, not once per
    conversation. A daemon reader thread feeds a queue so each request can
    time out. If the child dies it is respawned once and the request retried
    once (mirrors the MCP server's respawn-once behavior); reader lines are
    tagged with a generation counter so the stale reader queue of the dead
    child (including its EOF sentinel) can never feed the new process.
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
        env["DATABASE_URL"] = self._database_url
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


def ingest_conversation(conv: dict, prefix: str, serve: BridgeServe) -> str:
    """Ingest one conversation through the persistent serve child.

    Returns the action string: "ingested", "skipped-duplicate", "skipped-empty"
    or "error: ...".
    """
    source_uri = f"{prefix}{slugify(conv['title'])}-{conv['index']}"
    args = {
        "source_type": "text",
        "payload": conv["text"],
        "source_uri": source_uri,
        "title": conv["title"],
    }
    try:
        resp = serve.request("ingest", args)
    except Exception as e:
        return f"error: cannot run bridge ingest: {type(e).__name__}: {e}"
    if resp.get("ok"):
        status = (resp.get("result") or {}).get("status")
        if status == "inserted":
            return "ingested"
        if status == "duplicate":
            return "skipped-duplicate"
        if status == "empty":
            return "skipped-empty"
        return f"error: unexpected ingest status {status!r}"
    err = str(resp.get("error", ""))
    return f"error: bridge ingest failed: {err or 'no output'}"


def export_once(export_path: str | Path, prefix: str, database_url: str) -> dict:
    conversations = parse_export(export_path)
    if not conversations:
        raise ValueError(f"export file {export_path} contains no conversations")
    print(f"export: {len(conversations)} conversation(s) from {export_path} (prefix {prefix!r})")
    counts = {"ingested": 0, "skipped-duplicate": 0, "skipped-empty": 0, "errors": 0}
    serve = BridgeServe(database_url)
    try:
        for conv in conversations:
            action = ingest_conversation(conv, prefix, serve)
            source_uri = f"{prefix}{slugify(conv['title'])}-{conv['index']}"
            if action == "ingested":
                counts["ingested"] += 1
            elif action in ("skipped-duplicate", "skipped-empty"):
                counts[action] += 1
            else:
                counts["errors"] += 1
            print(f"{action} {source_uri} ({conv['title']})")
    finally:
        serve.close()
    print(
        f"summary: ingested={counts['ingested']}"
        f" skipped-duplicate={counts['skipped-duplicate']}"
        f" skipped-empty={counts['skipped-empty']}"
        f" errors={counts['errors']}"
    )
    return counts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Import an AI-chat export JSON file into the Second Brain knowledge base."
    )
    parser.add_argument("export_file", help="path to the chat export JSON file")
    parser.add_argument("--source-uri-prefix", default=DEFAULT_PREFIX,
                        help=f"source_uri prefix for ingested conversations (default: {DEFAULT_PREFIX!r})")
    parser.add_argument("--once", action="store_true",
                        help="single pass and exit (this is the default behavior)")
    args = parser.parse_args(argv)

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print(
            "error: DATABASE_URL is not set — export_chat is DB-backed and ingests via "
            "a persistent `python -m bridge serve` child, which reads DATABASE_URL. Example: "
            "DATABASE_URL=postgresql://postgres@127.0.0.1:5433/secondbrain "
            "python scripts/export_chat.py export.json",
            file=sys.stderr,
        )
        return 1

    try:
        counts = export_once(args.export_file, args.source_uri_prefix, database_url)
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    return 1 if counts["errors"] else 0


if __name__ == "__main__":
    sys.exit(main())
