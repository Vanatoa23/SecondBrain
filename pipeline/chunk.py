"""Semantic chunking.

Rule (locked spec): keep the document whole if it is <= CHUNK_THRESHOLD tokens.
Above that, split at paragraph/sentence boundaries into semantic chunks.
HARD_CAP is bge-m3's maximum sequence length — never produce a chunk larger.
"""

from __future__ import annotations

import re

CHUNK_THRESHOLD = 2000   # tokens — below this, keep whole
HARD_CAP = 8192          # bge-m3 max sequence length


def estimate_tokens(text: str) -> int:
    """Rough token estimate: ~4 chars per token for English text."""
    return max(1, len(text) // 4)


def _split_paragraphs(text: str) -> list[str]:
    """Split on blank lines, keeping the delimiter attached to the preceding chunk."""
    return [p for p in re.split(r"(?<=\n)\s*\n", text) if p.strip()]


def _split_sentences(paragraph: str) -> list[str]:
    """Split a paragraph into sentences (naive but effective for chunking)."""
    return [s for s in re.split(r"(?<=[.!?])\s+", paragraph) if s.strip()]


def _hard_split(text: str) -> list[str]:
    """Hard-split by characters (~4 chars/token) so no piece exceeds HARD_CAP
    tokens. Used when a single sentence is itself larger than HARD_CAP (e.g.
    punctuation-free text) and cannot be split at sentence boundaries."""
    max_chars = HARD_CAP * 4
    return [text[i:i + max_chars] for i in range(0, len(text), max_chars)]


def chunk_text(text: str) -> list[str]:
    """Split text into semantic chunks.

    Returns a list with a single element if the text fits under CHUNK_THRESHOLD.
    """
    if estimate_tokens(text) <= CHUNK_THRESHOLD:
        return [text]

    chunks: list[str] = []
    current = ""

    for para in _split_paragraphs(text):
        # A paragraph larger than CHUNK_THRESHOLD is split at sentence
        # boundaries — not only when it exceeds HARD_CAP (a 2000-8192-token
        # single-paragraph transcript must not stay one giant chunk).
        if estimate_tokens(para) > CHUNK_THRESHOLD:
            for sent in _split_sentences(para):
                if estimate_tokens(sent) > HARD_CAP:
                    # Oversized single sentence (no usable punctuation):
                    # flush what we have, then hard-split by characters so no
                    # chunk can exceed HARD_CAP tokens.
                    if current.strip():
                        chunks.append(current.strip())
                        current = ""
                    chunks.extend(_hard_split(sent))
                    continue
                if estimate_tokens(current + sent) > CHUNK_THRESHOLD and current:
                    chunks.append(current.strip())
                    current = sent
                else:
                    current += sent + " "
            continue

        if estimate_tokens(current + para) > CHUNK_THRESHOLD and current:
            chunks.append(current.strip())
            current = para
        else:
            current += para + "\n\n"

    if current.strip():
        chunks.append(current.strip())

    return chunks
