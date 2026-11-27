"""Unit tests for the ingestion pipeline — no live DB or model required."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline.chunk import CHUNK_THRESHOLD, HARD_CAP, chunk_text, estimate_tokens
from pipeline.dedup import SIMILARITY_THRESHOLD, content_hash, normalize_text
from pipeline.ingest import estimate_distillation_score, estimate_update_period
from retrieval.merge import is_stale, merge_results


# --- chunking ---------------------------------------------------------------

def test_short_text_not_chunked():
    text = "Short note about sleep."
    assert chunk_text(text) == [text]


def test_long_text_chunked():
    para = "This is a sentence. " * 600  # ~3000 tokens
    text = para + "\n\n" + para
    chunks = chunk_text(text)
    assert len(chunks) > 1
    for c in chunks:
        assert estimate_tokens(c) <= HARD_CAP


def test_chunk_threshold_respected():
    # ~520 tokens per paragraph, 20 paragraphs -> ~10k tokens total: chunking
    # MUST occur, and every produced chunk must respect CHUNK_THRESHOLD.
    para = "This is a test sentence about chunking thresholds. " * 40
    text = "\n\n".join([para] * 20)
    assert estimate_tokens(text) > CHUNK_THRESHOLD
    chunks = chunk_text(text)
    assert len(chunks) > 1, "text above CHUNK_THRESHOLD must be split into >1 chunk"
    for c in chunks:
        assert estimate_tokens(c) <= CHUNK_THRESHOLD, (
            f"chunk of {estimate_tokens(c)} tokens exceeds CHUNK_THRESHOLD={CHUNK_THRESHOLD}"
        )


# --- dedup helpers ----------------------------------------------------------

def test_content_hash_stable():
    a = content_hash("Hello   World")
    b = content_hash("hello world")
    assert a == b  # normalization makes them equal


def test_normalize():
    assert normalize_text("  Hello\n\n World  ") == "hello world"


# --- estimators -------------------------------------------------------------

def test_update_period_book_is_permanent():
    assert estimate_update_period("A classic book on philosophy", None) == -1


def test_update_period_event_is_short():
    assert estimate_update_period("Hackathon starts 24.12.2026", None) == 86400


def test_distillation_score():
    assert estimate_distillation_score("pdf") < 0.5   # raw source
    assert estimate_distillation_score("text") > 0.5  # own note
    # ALL raw third-party content scores LOW (0.1) like url/pdf/image —
    # including the Phase-2 multimodal source types (audit fix).
    for st in ("url", "pdf", "image", "youtube", "audio", "github"):
        assert estimate_distillation_score(st) == 0.1, st
    assert estimate_distillation_score("text") == 0.8


# --- auto_link ----------------------------------------------------------------


def test_auto_link_selects_embedding_in_single_query():
    """auto_link must fetch candidate embeddings in the SAME query as the
    candidates (no per-candidate re-SELECT — the old N+1), and link only
    candidates with cosine similarity >= 0.75."""
    import numpy as np
    # NB: `pipeline.ingest` the attribute is shadowed by the ingest() function
    # re-exported in pipeline/__init__.py — grab the module from sys.modules.
    ingest_mod = sys.modules["pipeline.ingest"]

    q_emb = np.array([1.0, 0.0], dtype=np.float32)
    candidates = [
        {"id": "cand-similar", "content": "x",
         "embedding": np.array([1.0, 0.0], dtype=np.float32)},   # cosine 1.0 -> link
        {"id": "cand-dissimilar", "content": "y",
         "embedding": np.array([0.0, 1.0], dtype=np.float32)},   # cosine 0.0 -> skip
    ]
    queries: list[str] = []

    class _Rows:
        def __init__(self, rows):
            self._rows = rows

        def fetchall(self):
            return self._rows

        def fetchone(self):
            return self._rows[0] if self._rows else None

    class FakeConn:
        def execute(self, sql, params=None):
            queries.append(sql)
            if "ORDER BY embedding <=>" in sql:
                return _Rows(candidates)
            if sql.strip().upper().startswith("INSERT INTO RELATIONSHIPS"):
                return _Rows([{"id": "rel-1"}])
            raise AssertionError(f"unexpected query: {sql!r}")

    orig_embed = ingest_mod.embed_texts
    ingest_mod.embed_texts = lambda texts: [q_emb]
    try:
        rels = ingest_mod.auto_link("new-node", "some new text", FakeConn())
    finally:
        ingest_mod.embed_texts = orig_embed

    assert rels == ["rel-1"], rels  # only the similar candidate was linked
    selects = [s for s in queries if s.strip().upper().startswith("SELECT")]
    assert len(selects) == 1, f"auto_link must issue exactly ONE SELECT, got {len(selects)}: {selects}"
    assert "embedding" in selects[0], "the single SELECT must return the embedding column too"


# --- merge / staleness ------------------------------------------------------

from datetime import datetime, timedelta, timezone


def _node(node_id, score, update_period=-1, age_days=0):
    return {
        "id": node_id, "score": score, "update_period": update_period,
        "created_at": datetime.now(timezone.utc) - timedelta(days=age_days),
        "content": "x", "found_by": set(),
    }


def test_merge_dedups_by_id():
    by_method = {
        "vector": [_node("a", 0.9), _node("b", 0.5)],
        "fts": [_node("a", 0.7), _node("c", 0.6)],
    }
    merged = merge_results(by_method)
    ids = [m["id"] for m in merged]
    assert sorted(ids) == ["a", "b", "c"]
    a = next(m for m in merged if m["id"] == "a")
    assert a["score"] == 0.9  # max across methods
    assert set(a["found_by"]) == {"vector", "fts"}


def test_stale_item_penalized_not_filtered():
    stale = _node("old", 1.0, update_period=86400, age_days=10)
    fresh = _node("new", 0.8, update_period=-1)
    merged = merge_results({"vector": [stale, fresh]})
    by_id = {m["id"]: m for m in merged}
    assert by_id["old"]["stale"] is True
    assert abs(by_id["old"]["score"] - 0.5) < 1e-6  # 1.0 * 0.5 penalty
    assert by_id["new"]["stale"] is False
    assert by_id["old"]["id"] in by_id  # still present — soft penalty only


def test_permanent_item_never_stale():
    n = _node("book", 1.0, update_period=-1, age_days=3650)
    assert is_stale(n) is False


if __name__ == "__main__":
    for name, fn in sorted(list(globals().items())):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"PASS {name}")
    print("All tests passed.")
