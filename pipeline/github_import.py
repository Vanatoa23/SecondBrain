"""GitHub repository import — fetch a public repo's text files via the REST API.

    import_github_repo(repo_url, max_files=50, max_file_bytes=200_000)
        -> list[ExtractionResult]

Each text-ish file in the repo becomes one ExtractionResult whose source_uri
points at the file on github.com, so every ingested knowledge item keeps full
provenance (owner/repo/branch/path).

Security model:

  * Only https://github.com/<owner>/<repo>(/tree/<branch>) URLs are accepted.
    The host must be exactly github.com (case-insensitive) — anything else is
    a ValueError, so no SSRF via a crafted host.
  * Every path segment (owner, repo, branch) must match ^[A-Za-z0-9_.-]+$ and
    must not be "." or "..", so no path traversal / injection into the API
    URLs built from them.
  * All HTTP requests go to the fixed host api.github.com only — the final
    URL is re-checked before every request (_assert_api_url).
  * Unauthenticated GitHub API calls are rate-limited (60/hr per IP); a 403
    raises a RuntimeError that tells the user to set GITHUB_TOKEN. When
    GITHUB_TOKEN is set in the environment it is sent as a Bearer token (never
    logged, never included in error messages).

Network: all calls use requests.get(..., timeout=30).
"""

from __future__ import annotations

import base64
import binascii
import os
import re
from urllib.parse import urlsplit

import requests

from .extractors import ExtractionResult

API_HOST = "api.github.com"
WEB_HOST = "github.com"
API_BASE = f"https://{API_HOST}"
REQUEST_TIMEOUT = 30

# owner / repo / branch segment validation — prevents path injection into
# the API URLs built from these values.
_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
_FORBIDDEN_SEGMENTS = {".", ".."}

# Text-ish extensions we import; everything else (binaries, images, archives,
# lockfiles-by-extension, ...) is skipped.
TEXT_EXTENSIONS = {
    ".md", ".txt", ".py", ".ts", ".js", ".json", ".yaml", ".yml", ".toml",
    ".rst", ".c", ".cpp", ".h", ".go", ".rs", ".sql", ".html",
}

# Lockfiles are text but useless as knowledge (huge, machine-generated).
SKIP_FILENAMES = {
    "package-lock.json", "yarn.lock", "pnpm-lock.yaml", "poetry.lock",
    "cargo.lock", "gemfile.lock", "composer.lock", "pipfile.lock", "pdm.lock",
}


def _valid_segment(segment: str) -> bool:
    return segment not in _FORBIDDEN_SEGMENTS and bool(_NAME_RE.match(segment))


def parse_github_url(repo_url: str) -> tuple[str, str, str | None]:
    """Parse a github.com repo URL into (owner, repo, branch).

    Accepted forms:
        https://github.com/<owner>/<repo>
        https://github.com/<owner>/<repo>/tree/<branch>
    (http scheme and a trailing slash are tolerated; query/fragment ignored.)

    branch is None when the URL carries no /tree/<branch> — the caller then
    resolves the repo's default branch from the repo metadata API.

    Raises ValueError for anything else: non-github host, wrong scheme,
    missing segments, unsupported subpaths, or any segment that fails
    ^[A-Za-z0-9_.-]+$ (this rejects "." / ".." path-injection attempts).
    """
    if not isinstance(repo_url, str) or not repo_url.strip():
        raise ValueError("repo_url must be a non-empty string")
    parts = urlsplit(repo_url.strip())
    if parts.scheme.lower() not in ("http", "https"):
        raise ValueError(
            f"repo_url must be an http(s) URL, got scheme {parts.scheme!r}: {repo_url!r}"
        )
    host = (parts.hostname or "").lower()
    if host != WEB_HOST:
        # SSRF guard: only the public github.com web host is accepted.
        raise ValueError(
            f"repo_url host must be {WEB_HOST!r}, got {host!r}: {repo_url!r}"
        )
    segments = [s for s in parts.path.split("/") if s != ""]
    if len(segments) < 2:
        raise ValueError(
            f"repo_url must look like https://github.com/<owner>/<repo>, got {repo_url!r}"
        )
    owner, repo = segments[0], segments[1]
    branch: str | None = None
    if len(segments) > 2:
        if segments[2] != "tree" or len(segments) < 4:
            raise ValueError(
                "only https://github.com/<owner>/<repo>(/tree/<branch>) URLs are "
                f"supported, got {repo_url!r}"
            )
        if len(segments) > 4:
            raise ValueError(
                "only https://github.com/<owner>/<repo>(/tree/<branch>) URLs are "
                f"supported (no deeper subpaths), got {repo_url!r}"
            )
        branch = segments[3]
    for segment in (owner, repo, branch):
        if segment is not None and not _valid_segment(segment):
            raise ValueError(
                f"invalid path segment {segment!r} in repo_url {repo_url!r} "
                "(must match ^[A-Za-z0-9_.-]+$ and not be '.'/'..')"
            )
    return owner, repo, branch


def _assert_api_url(url: str) -> None:
    """SSRF guard: every outgoing request URL must target api.github.com."""
    host = (urlsplit(url).hostname or "").lower()
    if host != API_HOST:
        raise ValueError(f"refusing to request non-GitHub-API host {host!r}: {url!r}")


def _api_get(url: str, params: dict | None = None) -> requests.Response:
    """GET a GitHub REST API URL with timeout, optional Bearer token, clean errors."""
    _assert_api_url(url)
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "SecondBrain/1.0",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    resp = requests.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
    if resp.status_code == 403:
        # Rate limit (unauthenticated: 60 req/hr per IP) or abuse detection.
        # Never include the token or full headers in the message.
        raise RuntimeError(
            "GitHub API returned 403 (rate limit exceeded or access forbidden) for "
            f"{url}. Set the GITHUB_TOKEN environment variable to a GitHub personal "
            "access token to authenticate and raise the rate limit."
        )
    if resp.status_code != 200:
        raise RuntimeError(
            f"GitHub API request failed with HTTP {resp.status_code} for {url}: "
            f"{resp.text[:200]}"
        )
    return resp


# Git blob shas: 40 hex chars (SHA-1) or 64 (SHA-256 repos). Validated BEFORE
# any URL is built from the sha, so a malformed sha can never inject into the
# API URL path. fullmatch (not ^...$) so a trailing newline cannot sneak past.
_BLOB_SHA_RE = re.compile(r"[0-9a-f]{40,64}")


def _fetch_blob_text(api_repo_base: str, sha: str) -> str:
    """Fetch one git blob by sha and decode its base64 content to text.

    Raises ValueError for a sha that does not match ^[0-9a-f]{40,64}$ (before
    any request is made).
    """
    if not isinstance(sha, str) or not _BLOB_SHA_RE.fullmatch(sha):
        raise ValueError(
            f"invalid blob sha {sha!r}: must match ^[0-9a-f]{{40,64}}$ "
            "(lowercase hex, 40 or 64 chars)"
        )
    url = f"{api_repo_base}/git/blobs/{sha}"
    _assert_api_url(url)
    payload = _api_get(url).json()
    if payload.get("encoding") != "base64" or "content" not in payload:
        raise RuntimeError(f"unexpected blob payload for sha {sha!r}: {str(payload)[:200]}")
    try:
        raw = base64.b64decode(payload["content"])
    except (binascii.Error, ValueError) as exc:
        raise RuntimeError(f"could not base64-decode blob {sha!r}: {exc}") from exc
    return raw.decode("utf-8", errors="replace")


def import_github_repo(
    repo_url: str,
    max_files: int = 50,
    max_file_bytes: int = 200_000,
) -> list[ExtractionResult]:
    """Import a public GitHub repo's text files as ExtractionResults.

    Args:
        repo_url: https://github.com/<owner>/<repo>(/tree/<branch>)
        max_files: cap on the number of files imported.
        max_file_bytes: per-file size cap (checked against the git tree size
            before fetching, so large files are never downloaded).

    Returns one ExtractionResult per imported file:
        text       — decoded file content
        mime_type  — "text/plain"
        source_uri — https://github.com/<owner>/<repo>/blob/<branch>/<path>

    Raises ValueError on a malformed/non-github URL and RuntimeError on API
    errors (including the 403 rate limit, with a GITHUB_TOKEN hint).
    """
    if isinstance(max_files, bool) or not isinstance(max_files, int) or max_files < 1:
        raise ValueError(f"max_files must be a positive integer, got {max_files!r}")
    if (
        isinstance(max_file_bytes, bool)
        or not isinstance(max_file_bytes, int)
        or max_file_bytes < 1
    ):
        raise ValueError(f"max_file_bytes must be a positive integer, got {max_file_bytes!r}")

    owner, repo, branch = parse_github_url(repo_url)
    repo_base = f"{API_BASE}/repos/{owner}/{repo}"
    _assert_api_url(repo_base)

    meta = _api_get(repo_base).json()
    if branch is None:
        branch = meta.get("default_branch") or "main"
        if not _valid_segment(branch):
            raise ValueError(f"repo returned an invalid default branch {branch!r}")

    tree_payload = _api_get(
        f"{repo_base}/git/trees/{branch}", params={"recursive": "1"}
    ).json()
    entries = tree_payload.get("tree") or []

    # Pick text-ish files, skipping binaries (by extension allowlist),
    # lockfiles (by name) and oversized files (by tree size) — before any
    # content download, capped at max_files.
    candidates: list[tuple[str, str]] = []  # (path, sha)
    for entry in entries:
        if len(candidates) >= max_files:
            break
        if entry.get("type") != "blob":
            continue
        path = entry.get("path") or ""
        if not path or path.startswith("/"):
            continue
        ext = os.path.splitext(path)[1].lower()
        if ext not in TEXT_EXTENSIONS:
            continue
        if os.path.basename(path).lower() in SKIP_FILENAMES:
            continue
        size = entry.get("size") or 0
        if size > max_file_bytes:
            continue
        sha = entry.get("sha")
        if not sha:
            continue
        candidates.append((path, sha))

    results: list[ExtractionResult] = []
    errors: list[str] = []
    for path, sha in candidates:
        if len(results) >= max_files:
            break
        try:
            text = _fetch_blob_text(repo_base, sha)
        except Exception as exc:  # skip a bad file, keep importing the rest
            errors.append(f"{path}: {type(exc).__name__}: {exc}")
            continue
        results.append(
            ExtractionResult(
                text=text,
                mime_type="text/plain",
                source_uri=(
                    f"https://{WEB_HOST}/{owner}/{repo}/blob/{branch}/{path}"
                ),
            )
        )

    if not results and errors:
        raise RuntimeError(
            f"github import of {repo_url!r}: all {len(errors)} file fetches failed; "
            f"first error: {errors[0]}"
        )
    return results
