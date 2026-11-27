"""audit.py — Phase 5 append-only audit log, external-send gate, and quarantine
flag-queue helpers. Stdlib-only companion module for bridge.py (and the MCP
write server, which calls bridge `log_audit` at startup and after mutating
tool calls).

Paths (env-overridable, repo root = this file's directory):

    AUDIT_LOG   = env SECONDBRAIN_AUDIT_LOG   or <repo_root>/audit_log.jsonl
    FLAG_QUEUE  = env SECONDBRAIN_FLAG_QUEUE  or <repo_root>/flagged_issues.json

Audit events are appended as ONE JSON line each to AUDIT_LOG — never to
stdout, so the bridge's one-JSON-line stdout contract is untouched:

    {"ts": <iso-utc>, "actor": env SECONDBRAIN_ACTOR or "local",
     "event": <event_type>, ...fields}

log_event NEVER raises: any failure is printed to stderr and the event dict
is returned anyway; the envelope keys ts/actor/event always win over caller
fields. check_external_send gates outbound HTTP to a host[:port] allowlist
when SECONDBRAIN_REQUIRE_EXTERNAL_APPROVAL is set to any value other than
"", "0", "false", "no", "off" (unrecognized values fail closed — the gate is
ON); when the gate is on, a backslash in the raw netloc or a disagreement
between urlsplit and urllib3 about the host is refused, plain http is only
allowed to loopback hosts, and every refusal is a PermissionError carrying
host-only text (never the full URL). W3 calls it before every external POST.
load_json_file / write_json_atomic / locked_json_update back the flag queue:
a corrupt existing queue file is refused (RuntimeError), never overwritten —
same policy as scripts/curator.py load_queue — and locked_json_update runs
load -> mutate -> atomic write under an exclusive lockfile so concurrent
writers cannot tear the file.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Callable
from urllib.parse import urlsplit

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))

AUDIT_LOG = os.environ.get("SECONDBRAIN_AUDIT_LOG") or os.path.join(REPO_ROOT, "audit_log.jsonl")
FLAG_QUEUE = os.environ.get("SECONDBRAIN_FLAG_QUEUE") or os.path.join(REPO_ROOT, "flagged_issues.json")


def log_event(event_type: str, **fields: Any) -> dict:
    """Append ONE JSON line for `event_type` to AUDIT_LOG and return the event.

    The line is {"ts": <iso-utc>, "actor": <SECONDBRAIN_ACTOR or "local">,
    "event": event_type, ...fields}, opened in append mode, written, flushed.
    The envelope keys ts/actor/event ALWAYS win — caller fields with those
    names are overridden, never the other way round. NEVER raises: on any
    failure the error goes to stderr and the event dict is still returned.
    """
    ev = {
        **fields,
        "ts": datetime.now(timezone.utc).isoformat(),
        "actor": os.environ.get("SECONDBRAIN_ACTOR") or "local",
        "event": event_type,
    }
    try:
        log_path = os.path.abspath(AUDIT_LOG)
        parent = os.path.dirname(log_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(log_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(ev, default=str, ensure_ascii=False) + "\n")
            fh.flush()
    except Exception as e:  # never let audit logging break the caller
        print(f"[audit] failed to append event {event_type!r} to {AUDIT_LOG}: "
              f"{type(e).__name__}: {e}", file=sys.stderr)
    return ev


def args_fingerprint(args: dict) -> str:
    """Stable 16-hex-char fingerprint of an args dict (sorted keys, str fallback)."""
    return hashlib.sha256(
        json.dumps(args, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()[:16]


# Values of SECONDBRAIN_REQUIRE_EXTERNAL_APPROVAL that turn the gate OFF.
# Any OTHER set value enables the gate — unrecognized values fail closed.
_GATE_OFF_VALUES = ("", "0", "false", "no", "off")

# Hosts that may be reached over plain http even when the gate is on
# (local Ollama-style endpoints such as http://127.0.0.1:11434).
_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "::1", "localhost"})

# A lockfile older than this many seconds is stale and removed.
_LOCK_STALE_AFTER = 10.0


def _gate_enabled() -> bool:
    """True when SECONDBRAIN_REQUIRE_EXTERNAL_APPROVAL is set to any value NOT
    in ("", "0", "false", "no", "off"). Unset -> False (no-op). Unrecognized
    values -> True (fail closed)."""
    value = os.environ.get("SECONDBRAIN_REQUIRE_EXTERNAL_APPROVAL")
    if value is None:
        return False
    return value not in _GATE_OFF_VALUES


def _split_allowlist_entry(entry: str) -> tuple[str, int | None]:
    """Split a lowercased/stripped allowlist entry into (host, port|None).

    Entries are "host" or "host:port"; bracketed IPv6 ("[::1]:8443") is
    handled too. A bare IPv6 host ("::1") is never mistaken for host:port.
    """
    if entry.startswith("[") and "]" in entry:
        host_part, _, rest = entry.partition("]")
        host = host_part[1:]
        if rest.startswith(":") and rest[1:].isdigit():
            return host, int(rest[1:])
        return host, None
    if entry.count(":") == 1:
        host_part, _, port_part = entry.partition(":")
        if port_part.isdigit():
            return host_part, int(port_part)
    return entry, None


def check_external_send(url: str) -> None:
    """Gate an outbound send to `url`.

    No-op unless SECONDBRAIN_REQUIRE_EXTERNAL_APPROVAL is set to a value other
    than "", "0", "false", "no", "off" (any other value enables the gate —
    fail closed on unrecognized values). When the gate is enabled:

      * a backslash anywhere in the raw netloc is refused outright
        (parser-confusion bypass, e.g. https://evil.com\\@api.example.com);
      * urllib3's parse (urllib3.util.url.parse_url, lazy import) must agree
        with urlsplit on the host — any disagreement is refused (when
        urllib3 is unavailable the backslash rule still applies);
      * the host must match one of the comma-separated allowlist entries in
        SECONDBRAIN_EXTERNAL_ALLOWLIST — entries are "host" or "host:port"
        (lowercase, stripped); an entry without a port matches any port, an
        entry with a port matches only that port;
      * the scheme must be https, unless the host is loopback (127.0.0.1,
        ::1, localhost) — plain http to a non-loopback host is refused, so
        local http://127.0.0.1:11434 stays allowed.

    Every refusal raises PermissionError with HOST-ONLY text — the full URL
    is never included. Called before every external POST.
    """
    if not _gate_enabled():
        return
    raw = url if isinstance(url, str) else str(url)
    netloc = raw
    scheme = ""
    host = ""
    port: int | None = None
    unparsable = False
    try:
        parts = urlsplit(raw)
    except ValueError:
        parts = None
    if parts is not None:
        netloc = parts.netloc
        scheme = (parts.scheme or "").lower()
        try:
            host = (parts.hostname or "").lower()
            port = parts.port
        except ValueError:  # invalid bracketed host / bad port — fail closed
            host, port, unparsable = "", None, True
    else:
        unparsable = True

    def _refuse(detail: str) -> PermissionError:
        # host-only text — never the full URL
        return PermissionError(
            f"external send to host {host!r} is not approved ({detail}) — set "
            "SECONDBRAIN_EXTERNAL_ALLOWLIST or unset SECONDBRAIN_REQUIRE_EXTERNAL_APPROVAL"
        )

    # 1. backslash in the raw netloc -> parser-confusion bypass
    if "\\" in netloc:
        raise _refuse("backslash in netloc")
    # 2. urllib3's parser must agree with urlsplit on the host
    try:
        from urllib3.util.url import parse_url as _u3_parse_url
    except Exception:
        _u3_parse_url = None  # urllib3 unavailable — the backslash rule still applies
    if _u3_parse_url is not None:
        try:
            u3_host = (_u3_parse_url(raw).host or "").lower()
        except Exception:
            u3_host = None  # unparsable for urllib3 counts as disagreement
        if u3_host is not None and u3_host.startswith("[") and u3_host.endswith("]"):
            u3_host = u3_host[1:-1]  # bracketed IPv6
        if u3_host != host:
            raise _refuse("host parser mismatch")
    # 3. allowlist match ("host" or "host:port" entries)
    if unparsable:
        raise _refuse("unparsable URL")
    allowed = False
    for raw_entry in os.environ.get("SECONDBRAIN_EXTERNAL_ALLOWLIST", "").split(","):
        entry = raw_entry.strip().lower()
        if not entry:
            continue
        e_host, e_port = _split_allowlist_entry(entry)
        if host == e_host and (e_port is None or (port is not None and port == e_port)):
            allowed = True
            break
    if not allowed:
        raise _refuse("host not in allowlist")
    # 4. scheme must be https unless the host is loopback
    if scheme != "https" and host not in _LOOPBACK_HOSTS:
        raise _refuse("plain http to non-loopback host (https required)")


def load_json_file(path: str | os.PathLike, default: Any) -> Any:
    """Load JSON from `path`; return `default` when the file does not exist.

    A corrupt (unreadable / non-JSON) EXISTING file raises RuntimeError — the
    caller must never overwrite it blindly (same policy as curator.load_queue).
    """
    p = os.fspath(path)
    if not os.path.exists(p):
        return default
    try:
        with open(p, encoding="utf-8") as fh:
            return json.load(fh)
    except (json.JSONDecodeError, OSError) as exc:
        raise RuntimeError(
            f"existing file {p} is not readable JSON ({exc}) — refusing to "
            "overwrite it; fix or move it manually"
        ) from None


def write_json_atomic(path: str | os.PathLike, obj: Any) -> None:
    """Write `obj` as pretty JSON to a uniquely named tmp file next to `path`
    (write + flush + fsync, then chmod 0600), then os.replace it into place.

    The tmp name is f"{path}.{pid}.{uuid4hex}.tmp" so concurrent writers never
    collide on one tmp name. On any exception the tmp file is unlinked
    (try/finally) and the original file is left untouched.
    """
    p = os.fspath(path)
    tmp = f"{p}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.write(json.dumps(obj, indent=2, default=str, ensure_ascii=False) + "\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.chmod(tmp, 0o600)
        os.replace(tmp, p)
    finally:
        if os.path.exists(tmp):
            try:
                os.unlink(tmp)
            except OSError:
                pass


def _acquire_lockfile(lock_path: str) -> int:
    """Acquire the lockfile via os.open(O_CREAT|O_EXCL|O_WRONLY, 0o600).

    A lockfile older than 10s is stale and removed; otherwise the caller
    retries until the 10s acquisition timeout, then TimeoutError is raised.
    Returns the lock file descriptor.
    """
    start = time.monotonic()
    while True:
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            try:
                os.write(fd, f"pid={os.getpid()}\n".encode("utf-8"))
            except OSError:
                pass
            return fd
        except FileExistsError:
            try:
                age = time.time() - os.path.getmtime(lock_path)
            except OSError:
                age = 0.0  # lock vanished between open and stat — just retry
            if age >= _LOCK_STALE_AFTER:
                try:
                    os.unlink(lock_path)  # stale lock — remove it
                except OSError:
                    pass
                continue
            if time.monotonic() - start >= _LOCK_STALE_AFTER:
                raise TimeoutError(
                    f"could not acquire lock {lock_path!r} within "
                    f"{_LOCK_STALE_AFTER:.0f}s"
                ) from None
            time.sleep(0.05)


@contextlib.contextmanager
def locked_json_update(path: str | os.PathLike,
                       mutator: Callable[[Any], Any],
                       default_factory: Callable[[], Any]):
    """Atomically load -> mutate -> write a JSON file under an exclusive lock.

        with locked_json_update(path, mutator, default_factory) as result:
            ...

    The lockfile f"{path}.lock" is acquired with O_CREAT|O_EXCL|O_WRONLY
    (mode 0600); a lock older than 10s is stale and removed. The JSON is
    loaded with load_json_file semantics — a missing file yields
    default_factory(), a corrupt EXISTING file raises RuntimeError and is
    never overwritten — then mutator(obj) runs, then the file is written with
    write_json_atomic. The lock is released (fd closed + lockfile unlinked)
    in a finally, so a failing mutator never leaves the lock behind.
    Yields whatever mutator returned.
    """
    p = os.fspath(path)
    lock_path = f"{p}.lock"
    fd = _acquire_lockfile(lock_path)
    try:
        obj = load_json_file(p, default_factory())
        result = mutator(obj)
        write_json_atomic(p, obj)
        yield result
    finally:
        try:
            os.close(fd)
        except OSError:
            pass
        try:
            os.unlink(lock_path)
        except OSError:
            pass
