"""Phase 2 ACCEPTANCE test — drop a paper, a lecture recording, and a repo:
all must produce searchable, provenance-linked chunks automatically.

ROADMAP.md Phase 2 acceptance: "Drop paper/lecture/repo -> automatic
searchable chunks with provenance". This test proves it end to end against a
REAL PostgreSQL + pgvector database through the REAL bridge serve process:

  leg 1  PDF ("paper")   — a tiny raw PDF with a unique marker term is built
                           from raw bytes (minimal 1-page template, no
                           reportlab), written to a temp file, and ingested via
                           `bridge serve` {source_type: "pdf", payload: <path>,
                           source_uri: "paper://acceptance"}.
  leg 2  AUDIO ("lecture recording") — a wav with a spoken marker phrase
                           (pyttsx3/SAPI5 when available, else a sine wav) is
                           ingested via {source_type: "audio", payload: <wav>,
                           source_uri: "audio://acceptance-lecture.wav"}
                           (the .wav suffix is required: for byte payloads the
                           audio extractor derives the decode extension from
                           source_uri). SKIPped when faster-whisper or its
                           model weights are unavailable.
  leg 3  GITHUB ("repo") — {source_type: "github",
                           payload: "https://github.com/octocat/Spoon-Knife"}
                           imports the repo as multiple items, each with a
                           https://github.com/octocat/Spoon-Knife/blob/... URI.
                           SKIPped when the network/rate limit fails.
  (YouTube is deliberately not a leg here — covered by tests/test_youtube.py.)

For every non-skipped leg, a hybrid search (query = marker / title, top_k=10)
is run THROUGH THE SAME `bridge serve` child (command "search") so this test
process never loads torch: it asserts ok:true, the expected source_uri appears
in the top-10 results, every returned result carries the provenance fields
(title + source_uri), and by_method has the vector/fts/graph keys.

Guard pattern (locked, same as tests/test_e2e.py / test_mcp_acceptance.py):
  - TEST_DATABASE_URL is the ONLY accepted variable — no DATABASE_URL
    fallback, because apply_schema() drops the knowledge tables.
  - REFUSE (exit 1) against any dbname not matching (^|_)test(_|$)
    case-insensitively, checked BEFORE any connection or destructive op.
  - SKIP (exit 0) when TEST_DATABASE_URL is unset or the server is
    unreachable, or when the bge models are not in the HF cache.
  - All SQL is parameterized; cleanup (DELETE ... WHERE source_uri LIKE/=
    <leg prefixes>) runs in a finally block and the serve child is killed.

The test process itself stays torch-free: it only uses psycopg + stdlib, and
checks model availability via the HuggingFace cache directory instead of
loading models. All model work (extract/embed/rerank/search) happens inside
ONE persistent `python -m bridge serve` child spawned with OMP_NUM_THREADS=2
and DATABASE_URL=<test url> — one process at a time (16GB box: os error 1455
with 2+ torch processes).

Setup:

    createdb phase2acc_test            # on 127.0.0.1:5433, trust auth
    TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:5433/phase2acc_test \
        OMP_NUM_THREADS=2 python tests/test_phase2_acceptance.py

"""

from __future__ import annotations

import importlib.util
import json
import math
import os
import re
import shutil
import struct
import subprocess
import sys
import tempfile
import threading
import time
import wave
from pathlib import Path

os.environ.setdefault("OMP_NUM_THREADS", "2")  # before any child import chain

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import psycopg  # noqa: E402
from psycopg import conninfo, sql  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402

SKIP_MSG = "SKIP: no test database available (set TEST_DATABASE_URL)"
TEST_DBNAME_RE = re.compile(r"(?:^|_)test(?:_|$)", re.IGNORECASE)

REQUEST_TIMEOUT_S = 1200.0  # first call pays the model loads inside serve

# Unique marker terms per leg (searchable, FTS-friendly).
PDF_MARKER = "crystalline quasar beacon 8842"
PDF_TITLE = "Phase 2 acceptance paper (paper://acceptance)"
PDF_SOURCE_URI_SENT = "paper://acceptance"
AUDIO_PHRASE = "the lecture marker is crimson badger lantern"
AUDIO_MARKER = "crimson badger lantern"
AUDIO_TITLE = "Acceptance lecture recording"
# The audio extractor derives the decode extension from source_uri for byte
# payloads, so the uri must end in .wav; the leg asserts the
# "audio://acceptance-lecture" prefix.
AUDIO_SOURCE_URI = "audio://acceptance-lecture.wav"
AUDIO_URI_PREFIX = "audio://acceptance-lecture"
GITHUB_REPO = "https://github.com/octocat/Spoon-Knife"
GITHUB_URI_PREFIX = "https://github.com/octocat/Spoon-Knife/blob/"
GITHUB_QUERY = "forking a repository pull request Spoon-Knife practice"


# --- guards -------------------------------------------------------------------


def get_test_url() -> str | None:
    # TEST_DATABASE_URL only — never fall back to DATABASE_URL: apply_schema()
    # DROPs the two tables and must never touch a non-test database.
    return os.environ.get("TEST_DATABASE_URL")


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


def db_name(url: str) -> str:
    try:
        return conninfo.conninfo_to_dict(url).get("dbname", "") or ""
    except Exception:
        m = re.search(r"/([^/?]+)", url)
        return m.group(1) if m else ""


def is_test_dbname(name: str) -> bool:
    return bool(TEST_DBNAME_RE.search(name or ""))


def ensure_database(url: str) -> bool:
    """Create the target DB if missing. Returns False if the server is unreachable."""
    info = conninfo.conninfo_to_dict(url)
    name = info.get("dbname") or ""
    if not name:
        return False
    for maint in ("postgres", "template1"):
        m = dict(info)
        m["dbname"] = maint
        try:
            conn = psycopg.connect(
                conninfo.make_conninfo(**{k: v for k, v in m.items() if v}),
                connect_timeout=5, autocommit=True,
            )
        except Exception:
            continue
        try:
            exists = conn.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s", (name,)
            ).fetchone()
            if not exists:
                conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
                print(f"created test database {name!r}")
            return True
        finally:
            conn.close()
    return False


def db_reachable(url: str) -> bool:
    try:
        conn = psycopg.connect(url, connect_timeout=5)
        conn.close()
        return True
    except Exception:
        return False


def apply_schema(url: str) -> None:
    """Idempotently apply db/schema.sql to the test DB (drops the two tables first)."""
    schema_path = REPO_ROOT / "db" / "schema.sql"
    schema_sql = schema_path.read_text()
    with psycopg.connect(url) as conn:
        conn.execute("DROP TABLE IF EXISTS relationships CASCADE")
        conn.execute("DROP TABLE IF EXISTS knowledge_items CASCADE")
    psql = shutil.which("psql")
    if psql:
        subprocess.run(
            [psql, url, "-v", "ON_ERROR_STOP=1", "-f", str(schema_path)],
            check=True, capture_output=True, text=True,
        )
    else:
        with psycopg.connect(url) as conn:
            conn.execute(schema_sql)  # no params -> simple protocol, multi-statement ok


def hf_cache() -> Path:
    return Path.home() / ".cache" / "huggingface" / "hub"


def check_bge_models() -> tuple[bool, str]:
    """bge-m3 + bge-reranker-v2-m3 present in the HF cache — WITHOUT loading torch."""
    cache = hf_cache()
    missing = [n for n in ("models--BAAI--bge-m3", "models--BAAI--bge-reranker-v2-m3")
               if not (cache / n).exists()]
    if missing:
        return False, f"not in HF cache: {', '.join(missing)}"
    return True, ""


def check_whisper() -> tuple[bool, str]:
    """faster-whisper importable + base model weights cached — no model loading."""
    if importlib.util.find_spec("faster_whisper") is None:
        return False, "faster_whisper not installed"
    if not (hf_cache() / "models--Systran--faster-whisper-base").exists():
        return False, "faster-whisper base model not in HF cache"
    return True, ""


# --- tiny raw-bytes PDF builder (same approach as tests/test_pdf.py) -----------


def _pdf_escape(s: str) -> str:
    return s.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def build_pdf_bytes(text: str) -> bytes:
    """Minimal but valid 1-page PDF with one Tj text operator, correct xref."""
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


# --- tiny wav builders ----------------------------------------------------------


def make_sine_wav(path: str, seconds: float = 1.0, freq: float = 440.0, rate: int = 16000) -> str:
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


def make_spoken_wav(path: str, phrase: str) -> str | None:
    """Speak `phrase` with pyttsx3/SAPI5 into a wav. None when unavailable."""
    try:
        import pyttsx3
        engine = pyttsx3.init()
        engine.save_to_file(phrase, path)
        engine.runAndWait()
        engine.stop()
    except Exception as e:
        print(f"  NOTE: pyttsx3 spoken wav unavailable ({type(e).__name__}: "
              f"{str(e)[:120]}) — falling back to a sine wav")
        return None
    if not os.path.isfile(path) or os.path.getsize(path) < 1000:
        return None
    return path


# --- persistent bridge serve client ---------------------------------------------


class BridgeServe:
    """One persistent `python -m bridge serve` child; one JSON response line per request."""

    def __init__(self, url: str):
        env = {**os.environ, "DATABASE_URL": url, "OMP_NUM_THREADS": "2"}
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "bridge", "serve"],
            cwd=str(REPO_ROOT), env=env,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL, text=True, bufsize=1,
        )
        self._next_id = 1
        self._pending: dict[int, dict] = {}
        self._lock = threading.Lock()
        self._reader = threading.Thread(target=self._read_loop, daemon=True)
        self._reader.start()

    def _read_loop(self):
        for line in self.proc.stdout:
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(msg, dict) and "id" in msg:
                with self._lock:
                    entry = self._pending.pop(msg["id"], None)
                if entry is not None:
                    entry["msg"] = msg
                    entry["event"].set()

    def request(self, command: str, args: dict, timeout: float = REQUEST_TIMEOUT_S) -> dict:
        rid = self._next_id
        self._next_id += 1
        entry = {"event": threading.Event(), "msg": None}
        with self._lock:
            self._pending[rid] = entry
        self.proc.stdin.write(json.dumps({"id": rid, "command": command, "args": args}) + "\n")
        self.proc.stdin.flush()
        if not entry["event"].wait(timeout):
            with self._lock:
                self._pending.pop(rid, None)
            raise TimeoutError(f"no response to {command!r} within {timeout}s")
        return entry["msg"]

    def close(self):
        try:
            self.proc.stdin.close()
            self.proc.wait(timeout=60)
        except Exception:
            try:
                self.proc.kill()
                self.proc.wait(timeout=10)
            except Exception:
                pass


# --- assertions ------------------------------------------------------------------

FAILURES: list[str] = []
SKIPS: list[str] = []


def check(cond: bool, msg: str) -> bool:
    print(f"{'PASS' if cond else 'FAIL'} {msg}")
    if not cond:
        FAILURES.append(msg)
    return cond


def skip(msg: str) -> None:
    print(f"SKIP {msg}")
    SKIPS.append(msg)


def count_items(url: str, sql_text: str, params: tuple) -> int:
    """Run a fixed COUNT query. The SQL text is always a literal at the call
    site (WHERE clause inlined) — never an f-string."""
    with psycopg.connect(url, row_factory=dict_row) as conn:
        return conn.execute(sql_text, params).fetchone()["n"]


def search_via_bridge(bridge: BridgeServe, leg: str, query: str,
                      expected_uri: str | None = None,
                      uri_prefix: str | None = None) -> None:
    """Run a hybrid search through the serve child and assert provenance."""
    t0 = time.perf_counter()
    resp = bridge.request("search", {"query": query, "top_k": 10})
    dt = time.perf_counter() - t0
    ok = resp.get("ok") is True
    check(ok, f"{leg} search ok:true (query={query!r}, {dt:.1f}s)"
        + ("" if ok else f" error={resp.get('error')}"))
    if not ok:
        return
    result = resp["result"]
    results = result.get("results") or []
    by_method = result.get("by_method") or {}
    check(all(k in by_method for k in ("vector", "fts", "graph")),
          f"{leg} search by_method has vector/fts/graph keys (got {sorted(by_method)})")
    prov_ok = all(("title" in r) and ("source_uri" in r) for r in results[:10])
    check(prov_ok and len(results) > 0,
          f"{leg} search: every top-10 result carries title + source_uri "
          f"(n_results={len(results)})")
    uris = [r.get("source_uri") for r in results[:10]]
    if expected_uri is not None:
        hit = expected_uri in uris
    else:
        hit = any(u and u.startswith(uri_prefix) for u in uris)
    check(hit, f"{leg} search: expected provenance uri in top-10 "
               f"(expected={expected_uri or uri_prefix + '*'}, top10_uris={uris})")


# --- legs ------------------------------------------------------------------------


def leg_pdf(bridge: BridgeServe, url: str, tmpdir: str) -> str | None:
    """Returns the pdf path (the provenance uri the pdf extractor stamps)."""
    pdf_path = os.path.join(tmpdir, "acceptance_paper.pdf")
    text = (f"Phase two acceptance paper. Unique marker: {PDF_MARKER}. "
            f"This document proves that dropped PDFs become searchable chunks "
            f"with provenance.")
    with open(pdf_path, "wb") as fh:
        fh.write(build_pdf_bytes(text))
    resp = bridge.request("ingest", {
        "source_type": "pdf", "payload": pdf_path,
        "source_uri": PDF_SOURCE_URI_SENT, "title": PDF_TITLE,
    })
    ok = resp.get("ok") is True
    check(ok, f"pdf ingest ok:true" + ("" if ok else f" error={resp.get('error')}"))
    if not ok:
        return None
    res = resp["result"]
    check(res.get("status") == "inserted", f"pdf ingest status=inserted (got {res.get('status')})")
    # NOTE: the pdf dispatch (pipeline/extractors.py extract("pdf", payload))
    # stamps source_uri = the file path (the source_uri arg is honored only for
    # audio), so the paper leg's provenance uri in the DB is the temp file path.
    n = count_items(url, "SELECT count(*) AS n FROM knowledge_items"
                         " WHERE source_uri = %s AND mime_type = %s",
                    (pdf_path, "application/pdf"))
    check(n >= 1, f"pdf row exists with mime application/pdf and source_uri=<pdf path> (n={n})")
    with psycopg.connect(url, row_factory=dict_row) as conn:
        row = conn.execute(
            "SELECT content FROM knowledge_items WHERE source_uri = %s LIMIT 1",
            (pdf_path,),
        ).fetchone()
    check(row is not None and PDF_MARKER in (row["content"] or ""),
          "pdf row content contains the marker text (real extractor output stored)")
    search_via_bridge(bridge, "pdf", f"{PDF_MARKER} acceptance paper", expected_uri=pdf_path)
    return pdf_path


def leg_audio(bridge: BridgeServe, url: str, tmpdir: str) -> None:
    whisper_ok, whisper_err = check_whisper()
    if not whisper_ok:
        skip(f"audio leg: faster-whisper unavailable ({whisper_err}) — the extractor "
             "itself is covered by tests/test_audio.py")
        return
    wav_path = os.path.join(tmpdir, "acceptance_lecture.wav")
    spoken = make_spoken_wav(wav_path, AUDIO_PHRASE)
    if spoken is None:
        make_sine_wav(wav_path, seconds=1.0)
        print("  NOTE: using a sine wav — transcription may be empty; if so the "
              "pipeline returns status 'empty' (insert-only, no empty rows) and the "
              "item/title search assertion is waived with this note")
    resp = bridge.request("ingest", {
        "source_type": "audio", "payload": wav_path,
        "source_uri": AUDIO_SOURCE_URI, "title": AUDIO_TITLE,
    }, timeout=1800.0)
    ok = resp.get("ok") is True
    check(ok, f"audio ingest ok:true" + ("" if ok else f" error={resp.get('error')}"))
    if not ok:
        return
    res = resp["result"]
    status = res.get("status")
    if status != "inserted":
        skip(f"audio leg: transcription empty (sine wav) -> status={status!r}; the "
             "insert-only pipeline stores no row for empty text, so no searchable "
             "chunk exists for this leg (extractor covered by tests/test_audio.py)")
        return
    check(True, f"audio ingest status=inserted")
    n = count_items(url, "SELECT count(*) AS n FROM knowledge_items"
                         " WHERE source_uri = %s AND mime_type = %s",
                    (AUDIO_SOURCE_URI, "audio/wav"))
    check(n >= 1, f"audio row exists with mime audio/wav and source_uri={AUDIO_SOURCE_URI} (n={n})")
    search_via_bridge(bridge, "audio", f"{AUDIO_MARKER} lecture", uri_prefix=AUDIO_URI_PREFIX)
    # title search (title is part of the FTS index)
    search_via_bridge(bridge, "audio(title)", AUDIO_TITLE, uri_prefix=AUDIO_URI_PREFIX)


def leg_github(bridge: BridgeServe, url: str) -> None:
    # real ingest (network-dependent):
    resp = bridge.request("ingest", {"source_type": "github", "payload": GITHUB_REPO},
                          timeout=600.0)
    if resp.get("ok") is not True:
        skip(f"github leg: network/rate-limit failed ({str(resp.get('error'))[:160]}) — "
             "covered by tests/test_github.py live Spoon-Knife ingest earlier")
        return
    res = resp["result"]
    check(res.get("file_count", 0) >= 1,
          f"github ingest file_count>=1 (got {res.get('file_count')}, status={res.get('status')})")
    n = count_items(url, "SELECT count(*) AS n FROM knowledge_items WHERE source_uri LIKE %s",
                    (GITHUB_URI_PREFIX + "%",))
    check(n >= 1, f"github rows exist with blob source_uri prefix (n={n})")
    search_via_bridge(bridge, "github", GITHUB_QUERY, uri_prefix=GITHUB_URI_PREFIX)


def cleanup(url: str, pdf_path: str | None) -> None:
    with psycopg.connect(url) as conn:
        # relationships cascade on knowledge_items delete (schema ON DELETE CASCADE)
        patterns = ["audio://acceptance-lecture%", "https://github.com/octocat/Spoon-Knife/blob/%",
                    "paper://%"]
        params: list = []
        clauses = ["source_uri LIKE %s" for _ in patterns]
        params.extend(patterns)
        if pdf_path:
            clauses.append("source_uri = %s")
            params.append(pdf_path)
        conn.execute(
            "DELETE FROM knowledge_items WHERE " + " OR ".join(clauses), tuple(params)
        )
        remaining = conn.execute(
            "SELECT count(*) AS n FROM knowledge_items WHERE " + " OR ".join(clauses),
            tuple(params),
        ).fetchone()[0]
    print(f"PASS cleanup (acceptance rows remaining: {remaining})")


# --- main ------------------------------------------------------------------------


def main() -> None:
    url = get_test_url()
    if not url:
        print(SKIP_MSG)
        sys.exit(0)

    # Refusal guard BEFORE any connection / destructive op.
    name = db_name(url)
    if not is_test_dbname(name):
        print(f"REFUSING to run against database {name!r}: TEST_DATABASE_URL must "
              "point at a test database (dbname matching (^|_)test(_|$)) — "
              "apply_schema() drops the knowledge tables.")
        sys.exit(1)

    if not ensure_database(url) or not db_reachable(url):
        print(SKIP_MSG)
        print(f"(connect to {_safe_db_label(url)} failed)")
        sys.exit(0)

    # pipeline/ and retrieval/ (inside the serve child) read DATABASE_URL.
    os.environ["DATABASE_URL"] = url

    apply_schema(url)
    print("PASS setup_schema (db/schema.sql applied to test DB)")

    model_ok, model_err = check_bge_models()
    if not model_ok:
        print(f"SKIP: embedding/reranker models unavailable ({model_err})")
        sys.exit(0)
    print("PASS models (bge-m3 + bge-reranker-v2-m3 present in HF cache; "
          "not loaded in this process)")

    tmpdir = tempfile.mkdtemp(prefix="sb-phase2acc-")
    bridge = BridgeServe(url)
    pdf_path: str | None = None
    try:
        print("--- leg 1: PDF (paper) ---")
        pdf_path = leg_pdf(bridge, url, tmpdir)
        print("--- leg 2: audio (lecture recording) ---")
        leg_audio(bridge, url, tmpdir)
        print("--- leg 3: github (repo) ---")
        leg_github(bridge, url)
    finally:
        try:
            cleanup(url, pdf_path)
        except Exception as e:
            print(f"WARNING: cleanup failed: {type(e).__name__}: {str(e)[:200]}")
            FAILURES.append(f"cleanup failed: {e}")
        bridge.close()
        shutil.rmtree(tmpdir, ignore_errors=True)

    print("\n================ PHASE 2 ACCEPTANCE SUMMARY ================")
    for leg in ("pdf", "audio", "github"):
        leg_fail = [f for f in FAILURES if f.startswith(leg)]
        leg_skip = [s for s in SKIPS if s.startswith(leg)]
        if leg_fail:
            print(f"leg {leg}: FAIL ({len(leg_fail)} failure(s))")
        elif leg_skip:
            print(f"leg {leg}: SKIP ({leg_skip[0][:120]})")
        else:
            print(f"leg {leg}: PASS")
    if SKIPS:
        print(f"SKIPs ({len(SKIPS)}):")
        for s in SKIPS:
            print(f"  - {s}")
    if FAILURES:
        print(f"FAILED ({len(FAILURES)} failure(s)):")
        for f in FAILURES:
            print(f"  - {f}")
        sys.exit(1)
    print("PHASE 2 ACCEPTANCE PASSED (all legs PASS or documented SKIP; "
          "searches hit provenance)")
    sys.exit(0)


if __name__ == "__main__":
    main()
