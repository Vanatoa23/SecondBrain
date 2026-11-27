"""Tests for the Docling-first PDF extractor (Phase 2) — pipeline/extractors.py
extract_pdf / _extract_pdf_pypdf, the "pdf" dispatch entry, and the bridge
ingest envelope for PDFs.

No-DB sections always run and never touch a database: a tiny valid 1-page PDF
is built from RAW bytes (minimal template with a Tj text operator — no
reportlab dependency), the pypdf fallback path is forced by making the
docling import raise ImportError, the real Docling path runs when docling is
installed and its models load (otherwise SKIP), dispatch wiring is verified
by monkeypatching, and bridge subprocesses run with DATABASE_URL removed
from the environment.

Guarded DB section (test_zz_db_smoke_guarded, runs last):
  * TEST_DATABASE_URL is the ONLY accepted variable — no DATABASE_URL fallback.
  * The dbname must match (^|_)test(_|$) case-insensitively, checked BEFORE any
    connection or destructive op; otherwise REFUSING + exit 1.
  * Unset or unreachable -> "SKIP: no test database available (set
    TEST_DATABASE_URL)" + exit 0.
  * With a test DB: schema applied to that DB only, the generated PDF ingested
    via `python -m bridge ingest`, asserted inserted + searchable via
    retrieval.search, then cleaned up. Model-dependent steps SKIP when the
    embedding/reranker models cannot be downloaded.

Run one suite at a time with OMP_NUM_THREADS=2 (16GB box: os error 1455 with
2+ torch processes).
"""

import builtins
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest.mock as mock
from pathlib import Path

os.environ.setdefault("OMP_NUM_THREADS", "2")  # before any torch import

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pipeline.extractors import ExtractionResult, extract, extract_pdf  # noqa: E402

SKIP_MSG = "SKIP: no test database available (set TEST_DATABASE_URL)"
TEST_DBNAME_RE = re.compile(r"(?:^|_)test(?:_|$)", re.IGNORECASE)

PDF_TEXT = "SecondBrain PDF smoke marker zebra quadrant 7749"


# --- tiny valid 1-page PDF built from raw bytes (no reportlab) ----------------

def _pdf_escape(s: str) -> str:
    return s.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def build_pdf_bytes(text: str = PDF_TEXT) -> bytes:
    """Minimal but valid 1-page PDF with one Tj text operator, correct xref."""
    # 12pt keeps the line inside the 612pt MediaBox (24pt would overflow the
    # right edge and pdfium/docling clip glyphs outside the page box).
    content = f"BT /F1 12 Tf 72 720 Td ({_pdf_escape(text)}) Tj ET".encode("latin-1")
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R "
        b"/Resources << /Font << /F1 5 0 R >> >> >>",
        b"<< /Length " + str(len(content)).encode() + b" >>\nstream\n" + content + b"\nendstream",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]
    out = bytearray(b"%PDF-1.4\n")
    offsets = []
    for i, body in enumerate(objects, start=1):
        offsets.append(len(out))
        out += f"{i} 0 obj\n".encode() + body + b"\nendobj\n"
    xref_pos = len(out)
    out += f"xref\n0 {len(objects) + 1}\n".encode()
    out += b"0000000000 65535 f \n"
    for off in offsets:
        out += f"{off:010d} 00000 n \n".encode()
    out += (f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\n"
            f"startxref\n{xref_pos}\n%%EOF\n").encode()
    return bytes(out)


def _force_docling_import_error():
    """Patch builtins.__import__ so any docling import raises ImportError."""
    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "docling" or name.startswith("docling."):
            raise ImportError(f"No module named {name!r} (forced for test)")
        return real_import(name, *args, **kwargs)

    return mock.patch("builtins.__import__", side_effect=fake_import)


# --- (a) pypdf fallback path (docling import forced to fail) -------------------

def test_fallback_pypdf_extracts_text_from_bytes():
    pdf = build_pdf_bytes()
    with _force_docling_import_error():
        res = extract_pdf(pdf)
    assert isinstance(res, ExtractionResult), type(res)
    assert PDF_TEXT in res.text, res.text
    assert res.mime_type == "application/pdf", res.mime_type
    assert res.source_uri is None, res.source_uri  # bytes input -> None


def test_fallback_pypdf_extracts_text_from_path():
    pdf = build_pdf_bytes()
    with tempfile.TemporaryDirectory(prefix="sb-pdf-") as d:
        p = os.path.join(d, "tiny.pdf")
        with open(p, "wb") as fh:
            fh.write(pdf)
        with _force_docling_import_error():
            res = extract_pdf(p)
        assert PDF_TEXT in res.text, res.text
        assert res.source_uri == p, res.source_uri
        assert res.mime_type == "application/pdf"


def test_fallback_used_when_docling_conversion_fails():
    # docling imports fine but convert() blows up -> fallback must still work.
    pdf = build_pdf_bytes()

    class _Boom:
        def convert(self, source):
            raise RuntimeError("forced conversion failure")

    with mock.patch("docling.document_converter.DocumentConverter", return_value=_Boom()):
        res = extract_pdf(pdf)
    assert PDF_TEXT in res.text, res.text
    assert res.mime_type == "application/pdf"


# --- (b) real Docling path (SKIP when docling/models unavailable) --------------

def test_docling_path_extracts_markdown_or_skip():
    try:
        from docling.datamodel.base_models import DocumentStream
        from docling.document_converter import DocumentConverter
    except Exception as e:
        print(f"SKIP docling path (docling not importable: "
              f"{type(e).__name__}: {str(e)[:120]})")
        return
    pdf = build_pdf_bytes()
    try:
        converter = DocumentConverter()
        doc = converter.convert(DocumentStream(name="tiny.pdf",
                                               stream=io.BytesIO(pdf))).document
        md = doc.export_to_markdown()
    except Exception as e:
        print(f"SKIP docling path (conversion unavailable: "
              f"{type(e).__name__}: {str(e)[:200]})")
        return
    assert PDF_TEXT in md, md[:400]
    # and extract_pdf itself must surface the same text (via docling here)
    res = extract_pdf(pdf)
    assert PDF_TEXT in res.text, res.text[:400]
    assert res.mime_type == "application/pdf"
    print("PASS docling path (real DocumentConverter used, markdown extracted)")


# --- (c) dispatch wiring --------------------------------------------------------

def test_dispatch_pdf_reaches_extract_pdf():
    sentinel = ExtractionResult(text="sentinel", mime_type="application/pdf",
                                source_uri="/tmp/x.pdf")
    with mock.patch("pipeline.extractors.extract_pdf", return_value=sentinel) as m:
        out = extract("pdf", "/tmp/x.pdf")
    assert out is sentinel
    m.assert_called_once_with("/tmp/x.pdf")
    with mock.patch("pipeline.extractors.extract_pdf", return_value=sentinel) as m2:
        extract("pdf", b"%PDF-bytes")
    m2.assert_called_once_with(b"%PDF-bytes")


# --- dbname guard regex (delimited (^|_)test(_|$) refusal semantics) ------------

def test_dbname_guard_regex():
    assert TEST_DBNAME_RE.search("pdf_test")
    assert TEST_DBNAME_RE.search("test")
    assert TEST_DBNAME_RE.search("db_test_2")
    assert TEST_DBNAME_RE.search("TEST")
    assert not TEST_DBNAME_RE.search("latest")
    assert not TEST_DBNAME_RE.search("postgres")
    assert not TEST_DBNAME_RE.search("mytestdb")     # not delimited
    assert not TEST_DBNAME_RE.search("contest")       # not delimited
    assert not TEST_DBNAME_RE.search("")


# --- (d)/(e) bridge envelopes without DATABASE_URL ------------------------------

def _no_db_env():
    return {k: v for k, v in os.environ.items() if k != "DATABASE_URL"}


def _run_bridge_ingest(args, timeout=900):
    proc = subprocess.run(
        [sys.executable, "-m", "bridge", "ingest", json.dumps(args)],
        cwd=ROOT, capture_output=True, text=True, timeout=timeout,
        env=_no_db_env(),
    )
    lines = [ln for ln in proc.stdout.splitlines() if ln.strip()]
    assert len(lines) == 1, f"expected exactly 1 stdout line, got {proc.stdout[:400]!r}"
    env = json.loads(lines[0])
    assert isinstance(env, dict) and isinstance(env.get("ok"), bool), env
    assert "Traceback" not in proc.stdout, proc.stdout[:400]
    return proc.returncode, env


def test_bridge_ingest_nonexistent_pdf_is_clean_error_envelope():
    code, env = _run_bridge_ingest({"source_type": "pdf", "payload": "/nonexistent.pdf"})
    assert code == 1, (code, env)
    assert env["ok"] is False, env
    assert isinstance(env.get("error"), str) and env["error"], env
    assert "result" not in env, env
    assert "nonexistent" in env["error"], env["error"]


def test_bridge_ingest_corrupt_pdf_is_clean_error_envelope():
    with tempfile.TemporaryDirectory(prefix="sb-pdf-") as d:
        p = os.path.join(d, "corrupt.pdf")
        with open(p, "wb") as fh:
            fh.write(b"%PDF-1.4 this is not a real pdf, no xref, garbage bytes \x00\x01\x02")
        code, env = _run_bridge_ingest({"source_type": "pdf", "payload": p})
    assert code == 1, (code, env)
    assert env["ok"] is False and env.get("error"), env
    assert "result" not in env, env


def test_extract_pdf_corrupt_bytes_raises_cleanly():
    for bad in (b"", b"%PDF-1.4 garbage not a pdf \x00\xff"):
        try:
            extract_pdf(bad)
        except Exception:
            continue
        raise AssertionError(f"extract_pdf({bad[:20]!r}...) did not raise")


# --- guarded DB smoke ------------------------------------------------------------

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

    from psycopg.rows import dict_row

    # apply schema to the TEST db only (drops the two tables there first)
    schema_sql = (ROOT / "db" / "schema.sql").read_text()
    with psycopg.connect(url) as conn:
        conn.execute("DROP TABLE IF EXISTS relationships CASCADE")
        conn.execute("DROP TABLE IF EXISTS knowledge_items CASCADE")
    psql = shutil.which("psql")
    if psql:
        subprocess.run([psql, url, "-v", "ON_ERROR_STOP=1", "-f", str(ROOT / "db" / "schema.sql")],
                       check=True, capture_output=True, text=True)
    else:
        with psycopg.connect(url) as conn:
            conn.execute(schema_sql)
    print("PASS db_smoke setup_schema")

    os.environ["DATABASE_URL"] = url  # pipeline reads DATABASE_URL
    tmpdir = tempfile.mkdtemp(prefix="sb-pdf-smoke-")
    pdf_path = os.path.join(tmpdir, "smoke.pdf")
    with open(pdf_path, "wb") as fh:
        fh.write(build_pdf_bytes())
    try:
        # model check — ingest embeds via bge-m3, search reranks via bge-reranker
        model_ok = False
        try:
            from pipeline.embed import embed_text
            embed_text("warmup")
            from retrieval.rerank import rerank
            rerank("warmup", [{"content": "warmup"}])
            model_ok = True
        except Exception as e:
            print(f"SKIP: embedding/reranker models unavailable "
                  f"({type(e).__name__}: {str(e)[:120]}) — searchable-check skipped")

        if not model_ok:
            return

        # ingest the generated PDF via the real bridge subprocess
        proc = subprocess.run(
            [sys.executable, "-m", "bridge", "ingest",
             json.dumps({"source_type": "pdf", "payload": pdf_path})],
            cwd=ROOT, capture_output=True, text=True, timeout=900,
            env={**os.environ, "DATABASE_URL": url, "OMP_NUM_THREADS": "2"},
        )
        lines = [ln for ln in proc.stdout.splitlines() if ln.strip()]
        assert len(lines) == 1, proc.stdout[:400]
        env = json.loads(lines[0])
        assert env["ok"] is True, (proc.returncode, env, proc.stderr[-400:])
        res = env["result"]
        assert res["status"] == "inserted", res
        assert res["node_ids"], res
        print(f"PASS db_smoke bridge ingest pdf (status={res['status']} "
              f"nodes={len(res['node_ids'])})")

        # row present and carries the extracted text + source_uri
        with psycopg.connect(url, row_factory=dict_row) as conn:
            rows = conn.execute(
                "SELECT id, content, mime_type, source_uri FROM knowledge_items "
                "WHERE source_uri = %s",
                (pdf_path,),
            ).fetchall()
        assert rows, "no rows for the ingested pdf source_uri"
        assert any(PDF_TEXT in (r["content"] or "") for r in rows), \
            [r["content"][:200] for r in rows]
        assert all(r["mime_type"] == "application/pdf" for r in rows), \
            [r["mime_type"] for r in rows]
        print("PASS db_smoke row verified (text + mime_type + source_uri)")

        # searchable via retrieval.search
        from retrieval.search import search
        out = search(PDF_TEXT, top_k=5)
        assert out["results"], out
        assert any(r.get("source_uri") == pdf_path for r in out["results"]), \
            [r.get("source_uri") for r in out["results"]]
        print("PASS db_smoke searchable via retrieval.search")
    finally:
        with psycopg.connect(url) as conn:
            conn.execute(
                "DELETE FROM relationships WHERE from_id IN "
                "(SELECT id FROM knowledge_items WHERE source_uri = %s) "
                "OR to_id IN (SELECT id FROM knowledge_items WHERE source_uri = %s)",
                (pdf_path, pdf_path),
            )
            conn.execute("DELETE FROM knowledge_items WHERE source_uri = %s", (pdf_path,))
        shutil.rmtree(tmpdir, ignore_errors=True)
        print("PASS db_smoke cleanup")


if __name__ == "__main__":
    failed = 0
    for name, fn in sorted(list(globals().items())):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
            except SystemExit:
                raise
            except Exception as exc:  # noqa: BLE001 — report and continue
                failed += 1
                print(f"FAIL {name}: {type(exc).__name__}: {exc}")
            else:
                print(f"PASS {name}")
    if failed:
        print(f"{failed} test(s) FAILED.")
        sys.exit(1)
    print("All tests passed.")
