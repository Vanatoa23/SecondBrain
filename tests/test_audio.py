"""Audio transcription extractor tests — no DB required.

Covers pipeline/extractors.py source_type "audio":
  (a) extract("audio", <tiny generated wav>) returns a well-formed
      ExtractionResult (model-dependent parts SKIP cleanly when
      faster-whisper or the model weights are unavailable),
  (b) dispatch wiring: extract("audio", ...) reaches extract_audio
      (verified with a monkeypatched fake, no model needed),
  (c) unknown extension errors cleanly with ValueError,
  (d) nonexistent file raises a clean error, and the bridge single-shot
      envelope stays clean: `python -m bridge ingest '{"source_type":"audio",
      "payload":"/nonexistent.mp3"}'` -> one JSON line, ok:false, exit 1,
      with no DATABASE_URL (the file read fails before any DB access).

Plain-assert style like tests/test_url_guard.py: run with `python tests/test_audio.py`.
"""

import json
import math
import os
import struct
import subprocess
import sys
import tempfile
import wave
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pipeline.extractors as extractors
from pipeline.extractors import ExtractionResult, extract

REPO_ROOT = Path(__file__).resolve().parent.parent

SKIPPED: list[str] = []


def _make_tiny_wav(path: str, seconds: float = 0.5, freq: float = 440.0, rate: int = 16000) -> str:
    """Generate a tiny 16-bit mono PCM WAV (a sine wave) in pure python."""
    n = int(seconds * rate)
    frames = b"".join(
        struct.pack("<h", int(32767 * 0.3 * math.sin(2.0 * math.pi * freq * i / rate)))
        for i in range(n)
    )
    with wave.open(path, "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(rate)
        w.writeframes(frames)
    return path


# --- (a) real extract() on a generated wav: ExtractionResult structure -------

def test_extract_audio_wav_structure():
    with tempfile.TemporaryDirectory(prefix="sb-audio-") as td:
        wav_path = _make_tiny_wav(os.path.join(td, "tiny.wav"))
        try:
            result = extract("audio", wav_path)
        except (ImportError, OSError) as exc:
            # faster-whisper missing or model weights not downloadable in this
            # sandbox — the model-dependent assert is SKIPped; dispatch wiring
            # is still proven by test_dispatch_audio_monkeypatched below.
            SKIPPED.append(f"test_extract_audio_wav_structure: {type(exc).__name__}: {exc}")
            print(f"SKIP model-dependent transcription "
                  f"(faster-whisper/model unavailable): {type(exc).__name__}: {exc}")
            return
    assert isinstance(result, ExtractionResult), type(result)
    assert isinstance(result.text, str), type(result.text)
    assert result.mime_type.startswith("audio/"), result.mime_type
    assert result.mime_type == "audio/wav", result.mime_type
    assert result.source_uri == wav_path, result.source_uri
    print(f"  transcribed text of the 0.5s sine wav: {result.text!r}")


# --- (b) dispatch wiring: extract("audio", ...) reaches extract_audio -------

def test_dispatch_audio_monkeypatched():
    calls = {}

    def fake_extract_audio(path_or_bytes, source_uri=None):
        calls["payload"] = path_or_bytes
        calls["source_uri"] = source_uri
        return ExtractionResult(text="fake transcript",
                                mime_type="audio/wav",
                                source_uri=source_uri)

    orig = extractors.extract_audio
    extractors.extract_audio = fake_extract_audio
    try:
        res = extract("audio", b"RIFFfake-bytes", source_uri="/tmp/x.wav")
        assert calls.get("payload") == b"RIFFfake-bytes", calls
        assert calls.get("source_uri") == "/tmp/x.wav", calls
        assert res.text == "fake transcript"
        assert res.mime_type == "audio/wav"
        # path payload form too
        calls.clear()
        res2 = extract("audio", "/tmp/y.mp3")
        assert calls.get("payload") == "/tmp/y.mp3", calls
        assert res2.source_uri is None  # fake echoes source_uri=None for path form
    finally:
        extractors.extract_audio = orig


# --- (c) unknown extension errors cleanly ------------------------------------

def test_unknown_extension_errors_cleanly():
    for payload, uri in (("/tmp/whatever.xyz", None), (b"raw-bytes", None)):
        try:
            extract("audio", payload, source_uri=uri)
        except ValueError as exc:
            assert "extension" in str(exc), str(exc)
        else:
            raise AssertionError(f"extract('audio', {payload!r}) did not raise ValueError")


# --- (d) nonexistent audio file: clean error, and clean bridge envelope ------

def test_nonexistent_audio_file_clean_error():
    try:
        extract("audio", "/nonexistent.mp3")
    except (FileNotFoundError, ValueError, OSError):
        return  # clean, typed error — no dependency needed to get here
    raise AssertionError("extract('audio', '/nonexistent.mp3') did not raise a clean error")


def _run_bridge(args_json: str) -> subprocess.CompletedProcess:
    env = {k: v for k, v in os.environ.items() if k != "DATABASE_URL"}
    return subprocess.run(
        [sys.executable, "-m", "bridge", "ingest", args_json],
        capture_output=True, text=True, cwd=str(REPO_ROOT), env=env, timeout=180,
    )


def test_bridge_ingest_audio_nonexistent_envelope():
    proc = _run_bridge('{"source_type":"audio","payload":"/nonexistent.mp3"}')
    assert proc.returncode == 1, f"exit={proc.returncode} stdout={proc.stdout!r} stderr={proc.stderr[-400:]!r}"
    lines = [l for l in proc.stdout.splitlines() if l.strip()]
    assert len(lines) == 1, f"stdout must be exactly one JSON line, got {proc.stdout!r}"
    envelope = json.loads(lines[0])
    assert envelope.get("ok") is False, envelope
    err = envelope.get("error")
    assert isinstance(err, str) and err, envelope
    # the failure must be the file read, not source_type validation
    assert "source_type" not in err, err
    assert "/nonexistent.mp3" in err, err


def test_bridge_ingest_unknown_source_type_still_rejected():
    proc = _run_bridge('{"source_type":"video","payload":"x"}')
    assert proc.returncode == 1, f"exit={proc.returncode} stdout={proc.stdout!r}"
    lines = [l for l in proc.stdout.splitlines() if l.strip()]
    assert len(lines) == 1, f"stdout must be exactly one JSON line, got {proc.stdout!r}"
    envelope = json.loads(lines[0])
    assert envelope.get("ok") is False, envelope
    assert "source_type" in envelope.get("error", ""), envelope


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
    if SKIPPED:
        print(f"{len(SKIPPED)} model-dependent assert(s) SKIPPED:")
        for s in SKIPPED:
            print(f"  SKIP {s}")
    if failed:
        print(f"{failed} test(s) FAILED.")
        sys.exit(1)
    print("All tests passed.")
