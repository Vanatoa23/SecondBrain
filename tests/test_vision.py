"""Vision-LLM image interpretation tests — pipeline/extractors.py source_type "image".

No DB required for the main section. Covers:

  (a) dispatch wiring: extract("image", ...) reaches extract_image
      (monkeypatched, no deps needed),
  (b) vision path: VISION_API_URL + VISION_MODEL set, requests.post mocked to
      return a fake chat completion -> the description text is returned,
      mime_type stays "image/*", source_uri is kept, and the request payload
      carries the prompt text + a base64 image_url data URL (POST to
      {VISION_API_URL}/chat/completions, timeout=120),
  (c) env vars unset -> falls back to OCR (_extract_image_ocr monkeypatched
      to return "OCR TEXT"),
  (d) vision endpoint down (requests.post raises) -> OCR fallback still works,
  (e) every test sets/unsets the VISION_* env vars explicitly,
  (f) bridge envelope without DATABASE_URL: image payload "/nonexistent.png"
      -> exactly one stdout JSON line, ok:false, exit 1,
  (g) a small PNG generated with PIL (solid color + drawn text) exercises the
      bytes path with the model/OCR mocked, so no heavy deps load.

Guarded DB smoke (runs last, name test_zz_db_smoke_guarded):
  * TEST_DATABASE_URL is the ONLY accepted variable — no DATABASE_URL fallback.
  * dbname must match (^|_)test(_|$) case-insensitively, checked BEFORE any
    connection; otherwise REFUSING + exit 1.
  * Unset or unreachable -> "SKIP: no test database available" + exit 0.
  * With a test DB: schema applied to that DB only, one image ingested
    in-process via pipeline.ingest.ingest("image", png_bytes, ...) with the
    vision env unset and pytesseract faked in sys.modules (no tesseract binary
    needed) -> assert inserted + FTS/vector searchable, then cleanup.

Plain-assert style like tests/test_url_guard.py: run `python tests/test_vision.py`.
"""

import base64
import contextlib
import io
import json
import os
import re
import subprocess
import sys
import tempfile
import unittest.mock as mock
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pipeline.extractors as extractors  # noqa: E402
from pipeline.extractors import ExtractionResult, extract, extract_image  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent

SKIPPED: list[str] = []

SKIP_MSG = "SKIP: no test database available (set TEST_DATABASE_URL)"
TEST_DBNAME_RE = re.compile(r"(?:^|_)test(?:_|$)", re.IGNORECASE)

VISION_ENV = {
    "VISION_API_URL": "http://vision.test/v1",
    "VISION_MODEL": "test-vision-model",
    "VISION_API_KEY": "test-key",
}


@contextlib.contextmanager
def _vision_env(**overrides):
    """Set the VISION_* env vars explicitly for the duration of the block."""
    env = dict(VISION_ENV)
    env.update(overrides)
    with mock.patch.dict(os.environ, env):
        yield env


@contextlib.contextmanager
def _clear_vision_env():
    """Remove any VISION_* env vars for the duration of the block."""
    saved = {k: os.environ.pop(k) for k in list(os.environ) if k.startswith("VISION_")}
    try:
        yield
    finally:
        os.environ.update(saved)


def _make_png_bytes(size=(64, 32), color=(30, 60, 120), text="HELLO SB") -> bytes:
    """Generate a small PNG with PIL: solid background + drawn text (default font)."""
    from PIL import Image, ImageDraw

    img = Image.new("RGB", size, color)
    draw = ImageDraw.Draw(img)
    draw.text((4, 4), text, fill=(255, 255, 255))  # PIL default font
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


class _FakeVisionResponse:
    def __init__(self, text):
        self._text = text
        self.status_code = 200

    def raise_for_status(self):
        return None

    def json(self):
        return {"choices": [{"message": {"role": "assistant", "content": self._text}}]}


# --- (a) dispatch wiring: extract("image", ...) reaches extract_image --------


def test_dispatch_image_monkeypatched():
    calls = {}

    def fake_extract_image(path_or_bytes, source_uri=None):
        calls["payload"] = path_or_bytes
        calls["source_uri"] = source_uri
        return ExtractionResult(text="fake image text", mime_type="image/*", source_uri=source_uri)

    with mock.patch.object(extractors, "extract_image", fake_extract_image):
        res = extract("image", b"PNGDATA", source_uri="vision://dispatch")
    assert calls.get("payload") == b"PNGDATA", calls
    assert calls.get("source_uri") == "vision://dispatch", calls
    assert res.text == "fake image text", res
    assert res.mime_type == "image/*", res


# --- (b) vision path: mocked chat completion ----------------------------------


def test_vision_path_returns_description_and_payload_shape():
    png = _make_png_bytes()
    captured = {}

    def fake_post(url, headers=None, json=None, timeout=None, **kwargs):
        captured.update(url=url, headers=headers, json=json, timeout=timeout)
        return _FakeVisionResponse("VISION DESC: a blue rectangle with the text HELLO SB")

    with _vision_env(), mock.patch("requests.post", side_effect=fake_post):
        res = extract_image(png, source_uri="vision://t1")

    assert isinstance(res, ExtractionResult), type(res)
    assert res.text == "VISION DESC: a blue rectangle with the text HELLO SB", res.text
    assert res.mime_type == "image/*", res.mime_type
    assert res.source_uri == "vision://t1", res.source_uri

    # request shape: POST {VISION_API_URL}/chat/completions, timeout=120, auth header
    assert captured["url"] == "http://vision.test/v1/chat/completions", captured["url"]
    assert captured["timeout"] == 120, captured["timeout"]
    assert captured["headers"].get("Authorization") == "Bearer test-key", captured["headers"]
    payload = captured["json"]
    assert payload["model"] == "test-vision-model", payload.get("model")
    messages = payload["messages"]
    assert messages[0]["role"] == "user", messages
    content = messages[0]["content"]
    assert isinstance(content, list) and len(content) == 2, content
    text_part = next(p for p in content if p.get("type") == "text")
    img_part = next(p for p in content if p.get("type") == "image_url")
    assert "Describe this image/screenshot" in text_part["text"], text_part
    assert "Transcribe ALL visible text" in text_part["text"], text_part
    data_url = img_part["image_url"]["url"]
    assert data_url.startswith("data:image/"), data_url[:40]
    mime, b64 = data_url[len("data:"):].split(";base64,", 1)
    assert mime.startswith("image/"), mime
    raw = base64.b64decode(b64)
    assert raw[:8] == b"\x89PNG\r\n\x1a\n", raw[:8]  # decodes back to our PNG


def test_vision_path_path_form_keeps_path_as_source_uri():
    png = _make_png_bytes()
    with tempfile.TemporaryDirectory(prefix="sb-vision-") as td:
        path = os.path.join(td, "shot.png")
        with open(path, "wb") as fh:
            fh.write(png)
        with _vision_env(), mock.patch(
            "requests.post", return_value=_FakeVisionResponse("desc from path form")
        ):
            res = extract_image(path)
    assert res.text == "desc from path form", res.text
    assert res.source_uri == path, res.source_uri
    assert res.mime_type == "image/*", res.mime_type


def test_vision_api_key_optional():
    png = _make_png_bytes()
    captured = {}

    def fake_post(url, headers=None, json=None, timeout=None, **kwargs):
        captured.update(headers=headers)
        return _FakeVisionResponse("no-key desc")

    env = {k: v for k, v in VISION_ENV.items() if k != "VISION_API_KEY"}
    with mock.patch.dict(os.environ, env), mock.patch("requests.post", side_effect=fake_post):
        os.environ.pop("VISION_API_KEY", None)
        res = extract_image(png, source_uri="vision://nokey")
    assert res.text == "no-key desc", res.text
    assert "Authorization" not in (captured.get("headers") or {}), captured["headers"]


# --- (c) env unset -> OCR fallback --------------------------------------------


def test_env_unset_falls_back_to_ocr():
    png = _make_png_bytes()
    ocr_calls = {}

    def fake_ocr(path_or_bytes, source_uri=None):
        ocr_calls["payload"] = path_or_bytes
        ocr_calls["source_uri"] = source_uri
        assert not os.environ.get("VISION_API_URL"), "VISION_API_URL must be unset here"
        assert not os.environ.get("VISION_MODEL"), "VISION_MODEL must be unset here"
        return ExtractionResult(text="OCR TEXT", mime_type="image/*", source_uri=source_uri)

    with _clear_vision_env(), mock.patch.object(extractors, "_extract_image_ocr", fake_ocr):
        res = extract_image(png, source_uri="vision://t2")
    assert res.text == "OCR TEXT", res.text
    assert res.mime_type == "image/*", res.mime_type
    assert res.source_uri == "vision://t2", res.source_uri
    assert ocr_calls.get("payload") == png, ocr_calls
    assert ocr_calls.get("source_uri") == "vision://t2", ocr_calls


# --- (d) vision endpoint down -> OCR fallback ---------------------------------


def test_vision_endpoint_down_falls_back_to_ocr():
    import requests

    png = _make_png_bytes()
    ocr_calls = {}

    def fake_ocr(path_or_bytes, source_uri=None):
        ocr_calls["payload"] = path_or_bytes
        ocr_calls["source_uri"] = source_uri
        return ExtractionResult(text="OCR TEXT", mime_type="image/*", source_uri=source_uri)

    def boom(*a, **k):
        raise requests.ConnectionError("vision endpoint down")

    with _vision_env(), mock.patch("requests.post", side_effect=boom), \
            mock.patch.object(extractors, "_extract_image_ocr", fake_ocr):
        res = extract_image(png, source_uri="vision://t3")
    assert res.text == "OCR TEXT", res.text
    assert res.mime_type == "image/*", res.mime_type
    assert res.source_uri == "vision://t3", res.source_uri
    assert ocr_calls.get("payload") == png, ocr_calls


def test_vision_bad_response_falls_back_to_ocr():
    png = _make_png_bytes()

    class _BadResponse:
        status_code = 200

        def raise_for_status(self):
            return None

        def json(self):
            return {"choices": [{"message": {"content": ""}}]}  # empty content

    with _vision_env(), mock.patch("requests.post", return_value=_BadResponse()), \
            mock.patch.object(
                extractors, "_extract_image_ocr",
                lambda p, source_uri=None: ExtractionResult("OCR TEXT", "image/*", source_uri),
            ):
        res = extract_image(png, source_uri="vision://t3b")
    assert res.text == "OCR TEXT", res.text


# --- (g) PNG bytes path with everything mocked (no heavy deps) ----------------


def test_png_bytes_path_vision_and_ocr():
    png = _make_png_bytes()
    assert png[:8] == b"\x89PNG\r\n\x1a\n"

    # bytes path, vision mocked: the data URL carries our exact PNG re-encoded
    captured = {}

    def fake_post(url, headers=None, json=None, timeout=None, **kwargs):
        captured["json"] = json
        return _FakeVisionResponse("bytes-path vision desc")

    with _vision_env(), mock.patch("requests.post", side_effect=fake_post):
        res = extract_image(png, source_uri="vision://bytes")
    assert res.text == "bytes-path vision desc", res.text
    img_part = next(
        p for p in captured["json"]["messages"][0]["content"] if p.get("type") == "image_url"
    )
    b64 = img_part["image_url"]["url"].split(";base64,", 1)[1]
    assert base64.b64decode(b64)[:8] == b"\x89PNG\r\n\x1a\n"

    # bytes path, OCR mocked: the ORIGINAL bytes reach the OCR function
    seen = {}

    def fake_ocr(path_or_bytes, source_uri=None):
        seen["payload"] = path_or_bytes
        return ExtractionResult(text="OCR TEXT", mime_type="image/*", source_uri=source_uri)

    with _clear_vision_env(), mock.patch.object(extractors, "_extract_image_ocr", fake_ocr):
        res2 = extract_image(png, source_uri="vision://bytes-ocr")
    assert res2.text == "OCR TEXT", res2.text
    assert seen.get("payload") == png, "OCR must receive the original bytes"
    assert res2.source_uri == "vision://bytes-ocr", res2.source_uri


# --- (f) bridge envelope without DATABASE_URL ---------------------------------


def _run_bridge(args_json: str) -> subprocess.CompletedProcess:
    env = {
        k: v for k, v in os.environ.items()
        if k != "DATABASE_URL" and not k.startswith("VISION_")
    }
    return subprocess.run(
        [sys.executable, "-m", "bridge", "ingest", args_json],
        capture_output=True, text=True, cwd=str(REPO_ROOT), env=env, timeout=180,
    )


def test_bridge_ingest_image_nonexistent_envelope():
    proc = _run_bridge('{"source_type":"image","payload":"/nonexistent.png"}')
    assert proc.returncode == 1, (
        f"exit={proc.returncode} stdout={proc.stdout!r} stderr={proc.stderr[-400:]!r}"
    )
    lines = [l for l in proc.stdout.splitlines() if l.strip()]
    assert len(lines) == 1, f"stdout must be exactly one JSON line, got {proc.stdout!r}"
    envelope = json.loads(lines[0])
    assert envelope.get("ok") is False, envelope
    err = envelope.get("error")
    assert isinstance(err, str) and err, envelope
    # the failure must come from the image itself, not source_type validation
    assert "source_type" not in err, err


# --- guarded DB smoke (runs last) ----------------------------------------------


def _safe_db_label(url: str) -> str:
    """Credential-free label for SKIP/connect-failed messages (host:port/dbname).

    Parsed with psycopg.conninfo.conninfo_to_dict and stripped of the password:
    keyword-form conninfo has no '@' at all, so a naive split on the '@'
    separator would echo the whole conninfo — credentials included.
    """
    try:
        from psycopg import conninfo
        d = conninfo.conninfo_to_dict(url)
        d.pop("password", None)
        host = d.get("host") or "localhost"
        port = d.get("port") or "5432"
        dbname = d.get("dbname") or ""
        return f"{host}:{port}/{dbname}"
    except Exception:
        return "<unparseable conninfo>"


def _db_name(url):
    try:
        from psycopg import conninfo
        return conninfo.conninfo_to_dict(url).get("dbname", "") or ""
    except Exception:
        m = re.search(r"/([^/?]+)", url)
        return m.group(1) if m else ""


def test_zz_db_smoke_guarded():
    url = os.environ.get("TEST_DATABASE_URL")  # TEST_DATABASE_URL only, no fallback
    if not url:
        print(SKIP_MSG)
        return
    name = _db_name(url)
    if not TEST_DBNAME_RE.search(name):
        print(f"REFUSING to run against database {name!r}: TEST_DATABASE_URL must "
              "point at a test database (delimited 'test' component) — the smoke "
              "applies/drops the knowledge tables.")
        sys.exit(1)
    import psycopg
    try:
        conn = psycopg.connect(url, connect_timeout=5)
        conn.close()
    except Exception:
        print(SKIP_MSG)
        print(f"(connect to {_safe_db_label(url)} failed)")
        return

    # apply schema to the TEST db only (drops the two tables there first)
    schema_sql = (REPO_ROOT / "db" / "schema.sql").read_text()
    with psycopg.connect(url) as conn:
        conn.execute("DROP TABLE IF EXISTS relationships CASCADE")
        conn.execute("DROP TABLE IF EXISTS knowledge_items CASCADE")
        conn.execute(schema_sql)
    print("PASS db_smoke setup_schema")

    png = _make_png_bytes()
    fake_tess = mock.Mock()
    fake_tess.image_to_string.return_value = (
        "OCR TEXT vision smoke marker: blue rectangle reading HELLO SB"
    )

    os.environ["DATABASE_URL"] = url  # pipeline reads DATABASE_URL
    try:
        with _clear_vision_env(), mock.patch.dict(sys.modules, {"pytesseract": fake_tess}):
            # embedding model check — ingest embeds via bge-m3
            try:
                from pipeline.embed import embed_text
                embed_text("warmup")
                model_ok = True
            except Exception as e:
                model_ok = False
                print(f"SKIP: embedding model unavailable ({type(e).__name__}: {str(e)[:120]}) "
                      "— model-dependent smoke steps skipped")
                SKIPPED.append(f"test_zz_db_smoke_guarded: embed model unavailable: {e}")

            if not model_ok:
                return

            from pipeline.db import get_connection
            from pipeline.ingest import ingest

            res = ingest("image", png, source_uri="vision://t1")
            assert res["status"] == "inserted", res
            assert res["node_ids"], res
            print(f"PASS db_smoke ingest image via OCR fallback (nodes={len(res['node_ids'])})")

            # searchable: FTS hit + vector hit on the OCR'd text
            with get_connection() as conn:
                fts = conn.execute(
                    "SELECT id FROM knowledge_items "
                    "WHERE source_uri = %s AND to_tsvector('english', content) "
                    "@@ plainto_tsquery('english', %s)",
                    ("vision://t1", "vision smoke marker"),
                ).fetchall()
                assert fts, "FTS query found no row for the ingested image"
                from pipeline.embed import embed_text
                q_emb = embed_text("vision smoke marker blue rectangle").tolist()
                vec = conn.execute(
                    "SELECT id FROM knowledge_items WHERE source_uri = %s "
                    "ORDER BY embedding <=> %s::vector LIMIT 1",
                    ("vision://t1", q_emb),
                ).fetchone()
                assert vec is not None, "vector query found no row for the ingested image"

                # cleanup: remove only what this smoke inserted
                conn.execute(
                    "DELETE FROM relationships WHERE from_id IN "
                    "(SELECT id FROM knowledge_items WHERE source_uri = %s) "
                    "OR to_id IN (SELECT id FROM knowledge_items WHERE source_uri = %s)",
                    ("vision://t1", "vision://t1"),
                )
                conn.execute(
                    "DELETE FROM knowledge_items WHERE source_uri = %s", ("vision://t1",)
                )
            print("PASS db_smoke image inserted + FTS/vector searchable + cleanup")
    finally:
        os.environ.pop("DATABASE_URL", None)


if __name__ == "__main__":
    failed = 0
    for name, fn in sorted(list(globals().items())):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
            except SystemExit:
                raise  # guarded refusal inside test_zz_db_smoke_guarded must exit 1
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
