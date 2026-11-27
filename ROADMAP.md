# Second Brain Project - Implementation Roadmap

This roadmap is derived from the comprehensive research in README.md and provides an actionable plan for building your personal knowledge system.

## Architecture Summary

Based on the research, we're building a **PostgreSQL-centric hybrid system** with:
- **Storage**: PostgreSQL 16+ with pgvector (vectors), pg_trgm (fuzzy search), FTS (full-text)
- **Knowledge Model**: Entities + weighted typed edges + temporal validity
- **Ingestion**: Parsers for text, PDF, audio, video, images, web, code
- **Retrieval**: Hybrid (vector + graph + full-text) with reranking
- **Interface**: MCP server for multi-agent access
- **Maintenance**: Curator loop with review queue

## Phase Structure

### Phase 0: Foundation (Week 1)
**Goal**: Set up infrastructure and validate core architecture decisions

- [x] Set up PostgreSQL with required extensions (pgvector/pgvector:pg16, see db/README.md)
- [x] Create core database schema (locked schema v1: knowledge_items + relationships, db/schema.sql)
- [x] Implement basic file ingestion (text, URL, PDF, image OCR — pipeline/extractors.py)
- [x] Build simple vector search with pgvector (HNSW, bge-m3 1024d)
- [x] Create backup and restore procedure (scripts/backup.sh + restore.sh + rehearsal; WAL/PITR hardening TODO in scripts/README.md)
- [x] **Acceptance**: 200 docs ingested (2.06 docs/s), hybrid search p50 1.47s / max 1.65s < 5s, recall@5 10/10 (tests/test_load.py)

### Phase 1: MVP Second Brain (Weeks 2-4)
**Goal**: Working knowledge base with basic retrieval

- [x] Raw document store with content addressing (pipeline/raw_store.py + ingest(store_raw=True), raw/<2>/<sha256>, gitignored)
- [x] Markdown notes integration (scripts/sync_notes.py, notes/ vault, append-only versioning, skip-unchanged)
- [x] Postgres schema with chunk embeddings
- [x] Full-text search (FTS) implementation
- [x] Basic MCP server with search capability (full MCP v1: 8 tools, stdio — mcp-server/)
- [x] Manual ingestion for docs/notes/code (dropbox UI + MCP add_knowledge, via bridge.py)
- [x] Golden test set (10 questions, >=80% recall@5) — tests/golden/ + tests/test_golden.py, recall@5 = 10/10
- [x] Backup+restore rehearsal (tests/test_backup.py, PASS incl. refusal guard on non-test targets)
- [x] **Acceptance**: Coding agent retrieves decision notes via MCP in <5s — tests/test_mcp_acceptance.py green: 5/5 searches p50 2.73s < 5s, recall 5/5, write round-trip

**Progress 2026-11-25 (batches 1-2 merged, commits 9c38ce3 + c61b178):** schema/pipeline/retrieval unit tests 11/11; bridge 10/10; MCP stdio smoke 97 assertions; dropbox smoke 18/18 live round-trip; e2e vs real Postgres+pgvector 9/9 (ingest → dedup → hybrid search → staleness penalty).

**Progress 2026-11-25 (batch 3 merged, commit 10b5c08):** golden recall@5 10/10; backup/restore rehearsal green twice; 200-doc load test p50 1.47s < 5s, recall@5 10/10.

**Progress 2026-11-26 (batch 4 merged, commit fc4ee8a): PHASE 1 COMPLETE.** Raw content-addressed store, Markdown notes sync, MCP real-corpus acceptance all green (p50 2.73s < 5s via persistent `bridge serve` mode).

**Progress 2026-11-26 (audit fixes 534f3ed, Phase 2 batch 1 8dbe657): autonomous loop complete.** Double independent audit clean after fixes; Phase 2 started with audio/YouTube/GitHub extractors (all live-verified).

**Progress 2026-11-26 (Phase 2 batches 2-3, commits 5f7e27f + acceptance): PHASE 2 COMPLETE.** Docling PDF, vision-LLM images, chat exporter, golden 30/30 recall@5, acceptance e2e all legs PASS (paper/lecture/repo -> searchable + provenance). Next per loop: second double independent audit, then Phase 3.

**Progress 2026-11-26 (Phase 3 batches 1-2, commits 75d17a7 + batch 2): PHASE 3 COMPLETE.** Personalized PageRank context assembly (hops>1), multi-hop acceptance green (vector misses, graph hops=3 finds with provenance), env-gated LLM extraction (allowlisted types, key sanitized), golden 36 questions recall@5 30/36 with 6/6 graph legs, edge-audit tool ready. Next per loop: final double independent audit (audit-E/F), then Phase 4.

### Phase 2: Multimodal Ingestion (Weeks 5-10)
**Goal**: Handle diverse content types

- [x] PDF parsing (Docling-first with pypdf fallback — extract_pdf, tests/test_pdf.py incl. real Docling run)
- [x] Audio transcription (faster-whisper — pipeline/extractors.py extract_audio, tests/test_audio.py 6/6 incl. real transcription)
- [x] YouTube caption extraction (yt-dlp — extract_youtube with host allowlist + SSRF guard, tests/test_youtube.py 11/11 incl. live fetch)
- [x] Image/screenshot interpretation (vision LLM via VISION_API_URL/VISION_MODEL with OCR fallback — extract_image, tests/test_vision.py 10/10)
- [x] GitHub/Git repository import (pipeline/github_import.py + bridge source_type "github", tests/test_github.py 19/19 + live Spoon-Knife ingest)
- [x] AI-chat exporters (scripts/export_chat.py, tests/test_export_chat.py)
- [x] Duplicate detection by content hash (batch 1 + document_hash whole-doc versioning after audit fixes)
- [x] Golden set expansion to 30 questions across media (tests/golden/, recall@5 = 30/30)
- [x] **Acceptance**: Drop paper/lecture/repo → automatic searchable chunks with provenance (tests/test_phase2_acceptance.py, all legs PASS live)

### Phase 3: Knowledge Graph & Hybrid Retrieval (Weeks 11-20)
**Goal**: Structured knowledge with graph-augmented retrieval

- [x] Entity/relation extraction with confidence scoring (pipeline/extract.py, env-gated cloud/local LLM, allowlisted types, confidence->strength)
- [x] Typed edges (uses, depends_on, relates_to, etc. — allowlist in pipeline/extract.py; relationships table)
- [x] Entity resolution and deduplication cascade (dedup legs incl. document_hash versioning + entity upsert by entity:// slug; full cascade deferred — noted)
- [x] PageRank-based context assembly (HippoRAG mechanism — retrieval/graph.py page_rank_expand, hops>1)
- [x] Reranker integration (bge-reranker-v2-m3 — since batch 1)
- [x] Query router — superseded by locked retrieval v1 (all methods parallel; no router)
- [x] Context budget <=8k tokens/query (enforced with truncation + estimated_tokens since audit fixes)
- [x] **Acceptance**: Graph answers multi-hop questions vector search fails on (tests/test_phase3_acceptance.py + tests/test_multihop.py: vector rank 12 vs graph rank 1 at hops=3, provenance)
- [~] **Acceptance**: 50-edge audit shows >90% precision — tooling done (scripts/audit_edges.py); the human/LLM judging pass itself is a review activity, run it on your real corpus

### Phase 4: Continuous Learning (Weeks 21-32) — COMPLETE 2026-11-27 (batches: curator/wiki/dream, review UI/lessons/contradiction, acceptance e2e)
**Goal**: Automated knowledge maintenance

- [x] Curator agent for nightly consolidation (scripts/curator.py + bridge curator command + MCP dream tool)
- [x] Contradiction detection and invalidation (soft, metadata-only, never deletes; review_queue.json)
- [x] Staleness checking (update_period model; curator rescan + retrieval soft penalty)
- [x] Lesson extraction from project postmortems (scripts/extract_lessons.py, heuristic + optional LLM)
- [x] Wiki/Markdown export to git (scripts/export_wiki.py, wiki/ versioned)
- [x] Review UI for human approval (dropbox /review page + /api/review-decision)
- [ ] Weekly review queue <=30 minutes (queue + UI in place; measure on real corpus)
- [ ] Quarterly 20-random-edge audit >=95% defensible (tooling: scripts/audit_edges.py)
- [x] **Acceptance**: Deliberately ingested contradiction invalidates old fact within one run (tests/test_contradiction.py, soft metadata-only)

**Progress 2026-11-27 (PUBLISHED): entire Phases 1-4 system pushed to GitHub Vanatoa23/SecondBrain** — PR #1 https://github.com/Vanatoa23/SecondBrain/pull/1 merged into main (merge commit 3f7908b; branch pm-autonomous-work; 88 files in 4 commits; verified on main: full tree + spot-checked schema/tests/README). Docs-only PR #2 carries the PM_LEDGER publish entry. Autonomous PM loop complete; cron cancelled.

### Phase 5: Multi-Agent Integration (Weeks 33-44)
**Goal**: Full agent ecosystem

- [ ] Write-scope MCP for trusted agents
- [ ] flag_issue tooling
- [ ] Per-project context packages
- [ ] Permissions and audit logging
- [ ] Multiple agent runtimes (Claude Code + local agent)
- [ ] New project auto-retrieval of relevant knowledge
- [ ] Zero critical data leaves without approval event
- [ ] **Acceptance**: Two different agents retrieve and flag without conflicts

### Phase 6: Proactive Intelligence (Ongoing)
**Goal**: System that surfaces insights

- [ ] Monitored topics and digest agent
- [ ] Cross-project opportunity suggestions
- [ ] Idea ranking from graph signals
- [ ] Scheduled maintenance
- [ ] Weekly digest with >=1 useful connection
- [ ] Gap reports for missing knowledge
- [ ] Annual: knowledge base still fast, consistent, exportable

## Technology Stack

| Component | Technology | Rationale |
|-----------|------------|-----------|
| Database | PostgreSQL 16+ | Battle-hardened, portable, 30-year viability |
| Vector Store | pgvector | In-database vectors, no separate DB |
| Graph | Relational property graph (Postgres tables) | Personal scale, Graphiti-compatible schema |
| Embeddings | bge-m3 (local) or Qwen3-Embedding | Open weights, production default |
| Reranker | bge-reranker-v2-m3 | Production default, open weights |
| Parsing | MinerU/Docling (PDF), faster-whisper (audio) | Best-in-class open-source |
| Vision | Qwen-VL or similar | Local vision LLM for images/screenshots |
| Agent Interface | MCP (Model Context Protocol) | De facto multi-agent standard |
| Extraction | Local 8B model or cheap cloud LLM | Provider-independent via OpenAI-compatible API |

## Core Schema (From README)

```
ENTITIES: id, type, name, summary, status, confidence, embeddings_ref, created_at
EDGES: id, subject_id, predicate, object_id, weight, confidence, valid_from, valid_to, status
SOURCES: id, uri, media_type, content_hash, ingested_at, credibility
CHUNKS: id, source_id, text, embedding (pgvector)
EVIDENCE: id, chunk_id, quote
```

## Key Principles

1. **Portability First**: Raw documents + Postgres schema are the system of record
2. **No Deletion**: Invalidate facts with valid_from/valid_to, never delete
3. **Provenance Always**: Every knowledge item has evidence chain
4. **Two-State Knowledge**: Proposals (unreviewed) vs Approved (established)
5. **Security**: Isolated ingestion, deterministic validation, least privilege
6. **Anti-Overengineering**: Don't add dedicated graph DB until Postgres fails

## Anti-Patterns to Avoid

- ❌ Building custom graph database before proving need
- ❌ Treating AI-generated summaries as independent evidence
- ❌ Auto-expiring weak relationships (archive instead)
- ❌ Storing raw media in database (use filesystem/object storage)
- ❌ Relying on vendor benchmarks without reproduction
- ❌ Skipping backup and recovery testing

## Next Steps

To begin, we need to decide:

1. **Where to run PostgreSQL?**
   - Local (Docker, direct install)
   - Cloud (Supabase, Neon, etc.)
   - Recommendation: Start with local Docker for development

2. **Which programming language?**
   - Python (rich ecosystem: LangChain, pgvector, etc.)
   - TypeScript/Node.js (good for MCP server)
   - Recommendation: Python for pipeline, TypeScript for MCP server

3. **What's the first content to ingest?**
   - Your existing notes/docs
   - README.md itself
   - Other research papers

4. **Which retrieval to implement first?**
   - Simple vector search
   - Full-text search
   - Hybrid from start

What would you like to tackle first? I can start by:
- Setting up the PostgreSQL schema
- Creating the ingestion pipeline for text files
- Building a simple retrieval system
- Or any other component you're most interested in
