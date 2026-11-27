"""Content-addressed raw document store — the portability layer of the Second Brain.

Layout (see README.md portability principles: raw originals + Postgres schema are
the system of record, and `raw/` is gitignored so originals never pollute git):

    <repo root>/raw/<sha256[:2]>/<sha256>

e.g. `raw/9f/9f2c...` — the file name IS the sha256 of its bytes, so the store is
self-verifying: `sha256(read_raw(h)) == h` always holds, and identical content
always lands on the same path (natural dedup at the storage layer).

Guarantees:
  - content-addressed: path derived only from the sha256 hex digest
  - idempotent: an existing object is never rewritten
  - atomic: bytes go to a tmp file in the same directory, then `os.replace`
  - path-traversal safe: the hash is validated as 64 lowercase hex chars before
    any path is built, so no input can escape `raw/`

Usage:
    from pipeline.raw_store import store_raw_bytes, read_raw, raw_path_for
    info = store_raw_bytes(b"original bytes")   # {"path": ..., "sha256": ...}
    data = read_raw(info["sha256"])             # b"original bytes"
    p    = raw_path_for(info["sha256"])         # Path(".../raw/ab/abcd...")

`pipeline/ingest.py` calls `store_raw_bytes` when invoked with
`store_raw=True` and records the hash in `knowledge_items.metadata.raw_sha256`
plus a `raw://<sha256>` source_uri when the caller gave none.
"""

from __future__ import annotations

import hashlib
import os
import re
from pathlib import Path

# Repo root = parent of pipeline/
REPO_ROOT = Path(__file__).resolve().parent.parent
RAW_ROOT = REPO_ROOT / "raw"

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def _validate_sha256(sha256: str) -> str:
    """Reject anything that is not a 64-char lowercase hex digest.

    This is the path-traversal guard: a hash can only ever address
    `raw/<2 hex chars>/<64 hex chars>` — nothing else.
    """
    if not isinstance(sha256, str) or not _SHA256_RE.match(sha256):
        raise ValueError(f"invalid sha256 (expected 64 lowercase hex chars): {sha256!r}")
    return sha256


def raw_path_for(sha256: str, raw_root: Path | None = None) -> Path:
    """Return the on-disk path for a content hash: raw/<sha[:2]>/<sha>."""
    root = Path(raw_root) if raw_root is not None else RAW_ROOT
    return root / _validate_sha256(sha256)[:2] / sha256


def store_raw_bytes(data: bytes, raw_root: Path | None = None) -> dict:
    """Store raw bytes in the content-addressed store.

    Returns {"path": <absolute path str>, "sha256": <hex digest>}.
    Idempotent: if the object already exists it is NOT rewritten.
    Atomic: written to a tmp file in the target directory, then os.replace.
    """
    if not isinstance(data, (bytes, bytearray, memoryview)):
        raise TypeError(f"store_raw_bytes expects bytes, got {type(data).__name__}")
    data = bytes(data)
    sha256 = hashlib.sha256(data).hexdigest()
    path = raw_path_for(sha256, raw_root)

    if path.exists():
        return {"path": str(path), "sha256": sha256}

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.parent / f".{sha256}.tmp-{os.getpid()}"
    try:
        with open(tmp, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)  # atomic rename on the same filesystem
    finally:
        if tmp.exists():
            tmp.unlink()  # only on failure — os.replace moved it on success
    return {"path": str(path), "sha256": sha256}


def read_raw(sha256: str, raw_root: Path | None = None) -> bytes:
    """Read back the raw bytes for a content hash."""
    path = raw_path_for(sha256, raw_root)
    if not path.is_file():
        raise FileNotFoundError(f"no raw object for sha256 {sha256!r} at {path}")
    return path.read_bytes()
