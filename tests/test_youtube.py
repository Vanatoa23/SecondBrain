"""YouTube caption extractor tests — no DB, no network, no yt-dlp required.

Covers pipeline/extractors.py extract_youtube:
  (a) dispatch: extract("youtube", url) reaches extract_youtube (monkeypatched)
  (b) fake runner returning a small .vtt file -> caption lines in text,
      mime_type + source_uri preserved
  (c) URL validation: non-YouTube host raises ValueError; YouTube host that
      resolves to a private/loopback IP raises via _assert_public_url;
      non-http(s) scheme raises; youtu.be short links are accepted
  (d) no-captions fallback: runner returns metadata only -> text includes
      title/description, no crash, no fabricated transcript
  (e) bridge envelope: `python -m bridge ingest` with source_type "youtube"
      and an evil (non-YouTube) URL -> {"ok": false} exit 1 without DATABASE_URL

All DNS is monkeypatched (socket.getaddrinfo) so tests are deterministic and
offline. Style follows tests/test_url_guard.py: plain asserts + __main__ runner.
"""

import json
import os
import socket
import subprocess
import sys
import tempfile
import unittest.mock as mock
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

REPO_ROOT = Path(__file__).resolve().parent.parent

import pipeline.extractors as extractors
from pipeline.extractors import ExtractionResult, extract, extract_youtube

YOUTUBE_URL = "https://www.youtube.com/watch?v=abc123"

PUBLIC_DNS = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443))]
LOOPBACK_DNS = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 443))]


def _public_dns():
    return mock.patch("socket.getaddrinfo", return_value=PUBLIC_DNS)


# --- (a) dispatch -------------------------------------------------------------

def test_dispatch_reaches_extract_youtube():
    called = {}

    def fake_extract_youtube(url, runner=None):
        called["url"] = url
        return ExtractionResult(text="fake", mime_type="text/x-youtube-transcript", source_uri=url)

    with mock.patch.object(extractors, "extract_youtube", fake_extract_youtube):
        result = extract("youtube", YOUTUBE_URL)
    assert called.get("url") == YOUTUBE_URL, called
    assert result.text == "fake"
    assert result.mime_type == "text/x-youtube-transcript"


# --- (b) captions via fake runner ----------------------------------------------

FAKE_VTT = (
    "WEBVTT\n"
    "Kind: captions\n"
    "Language: en\n"
    "\n"
    "00:00:01.000 --> 00:00:03.000\n"
    "Hello from the caption track\n"
    "\n"
    "00:00:03.000 --> 00:00:05.500\n"
    "Second caption line here\n"
    "\n"
    "00:00:05.500 --> 00:00:06.000\n"
    "Second caption line here\n"  # auto-captions overlap/repeat — must be deduped
)


def test_caption_subtitles_become_transcript_text():
    def fake_runner(url, workdir):
        path = os.path.join(workdir, "abc123.en.vtt")
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(FAKE_VTT)
        return {"subtitle_path": path, "info": {"title": "Fake Video"}}

    with _public_dns():
        result = extract_youtube(YOUTUBE_URL, runner=fake_runner)
    assert isinstance(result, ExtractionResult)
    assert "Hello from the caption track" in result.text, result.text
    assert "Second caption line here" in result.text, result.text
    assert "-->" not in result.text, "timestamp lines must be stripped"
    assert "WEBVTT" not in result.text
    assert result.text.count("Second caption line here") == 1, "consecutive dupes collapsed"
    assert result.mime_type == "text/x-youtube-transcript"
    assert result.source_uri == YOUTUBE_URL


def test_youtu_be_short_link_accepted():
    def fake_runner(url, workdir):
        return {"subtitle_path": None, "info": {"title": "Short link video", "description": "desc"}}

    with _public_dns():
        result = extract_youtube("https://youtu.be/abc123", runner=fake_runner)
    assert result.source_uri == "https://youtu.be/abc123"
    assert "Short link video" in result.text


# --- (c) URL validation ---------------------------------------------------------

def _assert_value_error(fn, label):
    try:
        fn()
    except ValueError:
        return
    raise AssertionError(f"{label} did not raise ValueError")


def test_non_youtube_host_refused():
    with _public_dns():
        _assert_value_error(
            lambda: extract_youtube("https://evil.example.com/x", runner=lambda u, w: {}),
            "extract_youtube('https://evil.example.com/x')",
        )


def test_lookalike_host_refused():
    # "notyoutube.com" ends with "youtube.com" as a string but is NOT a subdomain.
    with _public_dns():
        _assert_value_error(
            lambda: extract_youtube("https://notyoutube.com/watch?v=x", runner=lambda u, w: {}),
            "extract_youtube('https://notyoutube.com/...')",
        )


def test_non_http_scheme_refused():
    _assert_value_error(
        lambda: extract_youtube("file:///etc/passwd", runner=lambda u, w: {}),
        "extract_youtube('file:///etc/passwd')",
    )


def test_youtube_host_on_loopback_ip_refused():
    # _assert_public_url integration: even a youtube.com host that resolves to
    # 127.0.0.1 must be refused before any fetch.
    with mock.patch("socket.getaddrinfo", return_value=LOOPBACK_DNS):
        _assert_value_error(
            lambda: extract_youtube(YOUTUBE_URL, runner=lambda u, w: {}),
            f"extract_youtube({YOUTUBE_URL!r}) with loopback DNS",
        )


def test_dispatch_validates_youtube_url():
    with _public_dns():
        _assert_value_error(
            lambda: extract("youtube", "https://evil.example.com/x"),
            "extract('youtube', 'https://evil.example.com/x')",
        )


# --- (d) no-captions fallback ----------------------------------------------------

def test_metadata_fallback_when_no_captions():
    def fake_runner(url, workdir):
        return {
            "subtitle_path": None,
            "info": {"title": "My Great Video", "description": "An excellent description of the video."},
        }

    with _public_dns():
        result = extract_youtube(YOUTUBE_URL, runner=fake_runner)
    assert "My Great Video" in result.text, result.text
    assert "An excellent description of the video." in result.text, result.text
    assert YOUTUBE_URL in result.text
    assert result.mime_type == "text/x-youtube-transcript"
    assert result.source_uri == YOUTUBE_URL


def test_empty_everything_still_returns_result():
    def fake_runner(url, workdir):
        return {"subtitle_path": None, "info": {}}

    with _public_dns():
        result = extract_youtube(YOUTUBE_URL, runner=fake_runner)
    assert isinstance(result, ExtractionResult)
    assert result.source_uri == YOUTUBE_URL  # URL alone is still provenance


# --- (e) bridge envelope ----------------------------------------------------------

def test_bridge_ingest_youtube_rejects_evil_url_without_db():
    env = dict(os.environ)
    env.pop("DATABASE_URL", None)
    env.pop("TEST_DATABASE_URL", None)
    proc = subprocess.run(
        [sys.executable, "-m", "bridge", "ingest",
         json.dumps({"source_type": "youtube", "payload": "https://evil.example.com/x"})],
        cwd=str(REPO_ROOT), env=env, capture_output=True, text=True, timeout=180,
    )
    assert proc.returncode == 1, f"expected exit 1, got {proc.returncode}; stderr: {proc.stderr[-500:]}"
    lines = [ln for ln in proc.stdout.strip().splitlines() if ln.strip()]
    assert lines, f"bridge must print one JSON envelope line; stdout: {proc.stdout!r}"
    payload = json.loads(lines[-1])
    assert payload.get("ok") is False, payload
    assert "ValueError" in payload.get("error", ""), payload


if __name__ == "__main__":
    failed = 0
    for name, fn in sorted(list(globals().items())):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
            except Exception as exc:  # noqa: BLE001 — report and continue
                failed += 1
                print(f"FAIL {name}: {type(exc).__name__}: {exc}")
            else:
                print(f"PASS {name}")
    if failed:
        print(f"{failed} test(s) FAILED.")
        sys.exit(1)
    print("All tests passed.")
