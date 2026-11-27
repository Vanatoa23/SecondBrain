# Project Ledger - Second Brain

**Status**: Append-Only Document  
**Purpose**: Permanent record of all major decisions, architectural choices, and high-level project steps  
**Maintainer**: PM Agent (with User oversight)  
**Format**: Newest entries at top, never delete or modify existing entries

---

## Template for New Entries

```markdown
## [YYYY-MM-DD HH:MM] [Category]: [Brief description]
- **Decided by**: PM | User | Reviewer
- **Rationale**: [Why this decision was made]
- **Impact**: [What this means for the project]
- **Related**: [Files, tasks, other decisions, links]
- **Status**: Active | Superseded | Deprecated
```

**Categories**: ARCHITECTURE | FEATURE | PROCESS | SCOPE | PRIORITY | RESOURCE | QUALITY | SECURITY

---

## [2026-11-27 03:26] PROCESS: PUBLISHED to GitHub Vanatoa23/SecondBrain — PR #1 merged into main (merge commit 3f7908b)
- **Decided by**: PM, per User standing authorization [2026-11-26 23:48] ("push and merge everything to the secondbrain github repo")
- **Rationale**: Phase 4 complete and reviewer-approved (APPROVE 1/3, zero blockers, all batches); user asked for the full publish. Local checkout had no origin remote, so the connector_github_app tools were used: branch `pm-autonomous-work` created from `main` (0a0432f); 88 tracked files (manifest from `git ls-files`, all strict UTF-8, sensitive-path grep clean) pushed as 4 commits — dd19de8 (23 files), f5aeb90 (35), 1dee794 (15), 8c75ae6 (15); PR #1 https://github.com/Vanatoa23/SecondBrain/pull/1 (base=main, head=8c75ae6631e11afd47772ab60e75ed427254eef5) merged with merge_method=merge and matching expectedHeadSha; no force-push. Verified on main @ 3f7908b2d237dd9063f8ecf1e7b975406ee6f670: 7 commits, root tree complete (db/ pipeline/ retrieval/ bridge.py mcp-server/ dropbox/ scripts/ tests/ PM_*.md README.md ROADMAP.md .gitignore requirements.txt), spot-checked db/schema.sql (incl. partial entity:// unique index), tests/test_phase4_acceptance.py, README.md. This entry + the ROADMAP publish line are carried to GitHub by docs-only PR #2.
- **Impact**: Full Phases 1-4 system now public at https://github.com/Vanatoa23/SecondBrain. Local master stays the working checkout (no origin configured locally; future pushes go through the same connector route).
- **Related**: PR #1 (merged), merge commit 3f7908b, branch pm-autonomous-work, PR #2 (docs: this entry), cron loop 4e0eac40 (cancelled after publish)
- **Status**: Active

## [2026-11-27 03:30] FEATURE: PHASE 4 COMPLETE — all batches merged incl. acceptance e2e; next: publish to GitHub Vanatoa23/SecondBrain
- **Decided by**: PM (merges); Reviewers APPROVE at iteration 1/3 for every batch, zero blockers; User standing authorization [2026-11-26 23:48]
- **Rationale**: Whole Phase 4 delivered in the loop format: B1 curator core (staleness/contradiction/gap scan, review_queue.json with preserved human decisions, soft metadata-only invalidation) + wiki export + MCP dream wired to curator; B2 dropbox review UI (/review + decision API, CSRF-guarded) + lesson extraction from postmortems + contradiction e2e (ROADMAP acceptance: one curator run invalidates the older fact, soft, never deleted, still searchable); B3 phase4 acceptance e2e (curator + invalidation + search visibility + wiki + dream via MCP in one guarded run).
- **Impact**: Phase 4 acceptance met. All suites green at every merge (pipeline 12/12, golden recall@5 30/36 with 6/6 graph legs, MCP acceptance p50 ~2.6-2.9s < 5s, smokes 98/33, contradiction + phase4 acceptance PASS, guards REFUSING/SKIP verified). Remaining Phase 4 checkboxes are measurement activities on the real corpus (weekly queue time, quarterly edge audit) — tooling ready (scripts/audit_edges.py). NEXT per user: publish everything to GitHub Vanatoa23/SecondBrain (connector route, PR into main, no force-push), log URLs here, final summary, cancel cron.
- **Related**: ROADMAP.md Phase 4, commits 9df77d4 + batch2 + batch3, tests/test_phase4_acceptance.py, tests/test_contradiction.py
- **Status**: Active

## [2026-11-27 03:03] FEATURE: Phase 4 batch 2 merged — review UI, lessons, contradiction acceptance e2e
- **Decided by**: PM (merge); Reviewer APPROVE iteration 1/3, zero blockers; User standing authorization [2026-11-26 23:48]
- **Rationale**: Phase 4 batch 2: human review UI for the curator queue, lesson extraction from postmortems, and the Phase 4 headline acceptance (contradiction invalidated within one curator run).
- **Impact**: dropbox /review page + /api/review-queue + /api/review-decision (atomic writes, CSRF-guarded, textContent-only; smoke 30/30 no-DB + 33/33 DB); scripts/extract_lessons.py (postmortem section parser -> lesson:// items, idempotent dedup, optional env-gated LLM refinement); tests/test_contradiction.py proves the ROADMAP Phase 4 acceptance live: A + contradicting A' linked -> one curator run flags both in review_queue.json and soft-invalidates the older (metadata.review_status only, contents byte-unchanged, both still searchable), idempotent re-run. All suites green. Next: P4-B3 phase4 acceptance e2e, then publish everything to GitHub Vanatoa23/SecondBrain per [2026-11-26 23:48].
- **Related**: ROADMAP.md Phase 4, commit in git log, dropbox review page, scripts/extract_lessons.py
- **Status**: Active

## [2026-11-27 01:50] FEATURE: Phase 4 batch 1 merged — curator core + wiki export + dream tool
- **Decided by**: PM (merge); Reviewer APPROVE iteration 1/3, zero blockers; User standing authorization [2026-11-26 23:48]
- **Rationale**: Phase 4 batch 1 per ROADMAP: curator (staleness rescan, contradiction detection, gap detection, review queue with preserved human decisions, soft metadata-only invalidation), wiki/Markdown export as the versioned portability view, and the MCP dream tool now runs the curator and returns a structured report.
- **Impact**: scripts/curator.py + tests/test_curator.py (9 PASS incl. dry-run, merge-idempotency, decisions preserved, content byte-unchanged on invalidation); scripts/export_wiki.py + tests/test_export_wiki.py (byte-identical reruns, fence widening, bidirectional links, redaction); bridge.py curator command + mcp-server dream (98/98 smoke both modes; delegation to scripts/curator.py when present, standalone report otherwise). review_queue.json gitignored; wiki/ stays versioned per README. All 20 suites green (golden 30/36, MCP p50 2.65s, multihop, phase2/3 acceptance). Next: P4-B2 (review UI in dropbox, lesson extraction, contradiction e2e), P4-B3 (phase4 acceptance), then publish to GitHub Vanatoa23/SecondBrain.
- **Related**: ROADMAP.md Phase 4, commit in git log, scripts/curator.py, scripts/export_wiki.py
- **Status**: Active

## [2026-11-27 00:52] QUALITY: PHASE 3 LOOP COMPLETE — final double audit (audit-E + audit-F) CLEAN, H fixes merged (45fe202)
- **Decided by**: PM; Reviewer APPROVE iteration 1/3 zero blockers; audits E/F both CLEAN (no HIGH/MEDIUM); User standing loop [2026-11-26 16:23]
- **Rationale**: The Phase 3 loop finished: batch 1 (PageRank + multihop + LLM extraction, 75d17a7), batch 2 (edge audit tool, golden 36 w/ 6/6 graph legs, phase3 acceptance, ingest LOWs), final double audit E/F (both CLEAN; re-ran all 24 suites green: golden recall@5 30/36, MCP p50 < 5s, load p50 ~1.4-2.2s, smokes 98/23), H fix batch for the audit LOWs (45fe202: CGNAT/NAT64 SSRF rejection, vision/key sanitization, github sha validation, entity-upsert partial UNIQUE + savepoint, _safe_db_label in all 19 test files incl. PM one-liner in test_extract, smokes auto-apply schema, doc fixes, README sandbox wording aligned to v1).
- **Impact**: Phase 3 acceptance met: graph answers multi-hop questions vector search fails on (vector rank 12 vs graph rank 1 at hops=3, hop provenance), staleness/contradiction machinery in, extraction env-gated and key-safe. Accepted residuals logged: DNS-rebind TOCTOU IP pinning, python lockfile, pagefile/thread guidance. Next per [2026-11-26 23:48]: ALL of Phase 4 in the same loop format, then publish everything to GitHub Vanatoa23/SecondBrain.
- **Related**: ROADMAP.md Phase 3, commits 75d17a7/45fe202, audit-E/F reports, reviewer-H report
- **Status**: Active

## [2026-11-26 23:48] PROCESS: User expanded authorization — complete the WHOLE of Phase 4 in the same loop format, then publish everything to GitHub Vanatoa23/SecondBrain
- **Decided by**: User ("Complete the whole phase 4 with this loop format and then put on github")
- **Rationale**: Supersedes the batch-1-only scope in [2026-11-26 22:05]: the loop now covers ALL Phase 4 batches (curator core: staleness rescan + contradiction detection/invalidation + review queue; wiki/Markdown export; dream tool wiring; review UI; lesson extraction; Phase 4 acceptance e2e) with per-batch workers -> Reviewer (large/high, max 3 iterations) -> PM merge + commit + ledger + ROADMAP ticks, then publish.
- **Impact**: Publish step unchanged: after the last Phase 4 batch is merged and logged, push+merge ALL tracked content to Vanatoa23/SecondBrain (default branch main) via connector_github_app (no local origin; never force-push; PR+merge route; STOP+log if blocked). No extra double audit requested for Phase 4 — per-batch reviews only. INVARIANTS unchanged. 
- **Related**: ROADMAP.md Phase 4, PM_LEDGER [2026-11-26 22:05], repo Vanatoa23/SecondBrain (id 1470708571)
- **Status**: Active

## [2026-11-26 22:05] PROCESS: User instruction — after the H batch merges, run the NEXT batch (Phase 4 batch 1) in the same loop format; when it finishes, push+merge everything to GitHub Vanatoa23/SecondBrain
- **Decided by**: User (moving away for the night)
- **Rationale**: User: "When this batch finishes please continue on with exact same loop format with the next batch... When the next batch also finishes please push and merge everything to the secondbrain github repo."
- **Impact**: Loop continues: reviewer-H verdict -> PM merges H batch -> ledger entry (final audit E/F + H fixes + Phase 3 loop complete) -> spawn Phase 4 batch 1 (curator: staleness/contradiction scan + review queue + wiki export + dream tool wiring) with per-batch reviewer+merge+ledger -> THEN publish: target repo confirmed as Vanatoa23/SecondBrain (public, default branch main, currently empty of our work; local repo has no origin remote). Use connector_github_app: inspect remote main state first; push all content and create+merge a PR into main if direct push is not possible (never force-push; if branch protection/permissions block, STOP, log blocked, report with URLs). Log the PR/commit URLs in the ledger. Then final summary + cancel cron. INVARIANTS unchanged ([2026-11-26 16:23]). Anything ambiguous about the push (existing content on main, permissions) -> STOP and log, don't guess.
- **Related**: ROADMAP.md Phase 4, PM_LEDGER [2026-11-26 16:23], GitHub repo Vanatoa23/SecondBrain (id 1470708571)
- **Status**: Active

## [2026-11-26 21:03] FEATURE: PHASE 3 COMPLETE — batch 2 merged (edge audit tool, multi-hop golden, phase3 acceptance)
- **Decided by**: PM (merge); Reviewer APPROVE iteration 1/3, zero blockers; User standing loop [2026-11-26 16:23]
- **Rationale**: Phase 3 done: batch 1 (PageRank depth, multihop e2e, LLM extraction) + batch 2 (50-edge audit tooling, multi-hop golden questions, Phase 3 acceptance e2e, reviewer LOW fixes in ingest.py).
- **Impact**: scripts/audit_edges.py (deterministic seeded read-only sampling, endpoint export, credential redaction proven live); golden set now 36 questions — recall@5 = 30/36 (83.3% >= 80%), 6/6 require_graph questions found via PageRank graph legs while vector-only misses 6/6; tests/test_phase3_acceptance.py 55 PASS: vector-only top-5 misses the far chain node, graph hops=3 finds it with hop provenance, graph_ranking expand/pagerank controls correct. Phase 3 acceptance met ("graph answers multi-hop questions vector search fails on"). The 50-edge precision judging pass is a review activity on the real corpus — tool ready, run later. Next per loop: FINAL double independent audit (audit-E + audit-F), fixes, merge, log, final summary, cancel cron.
- **Related**: ROADMAP.md Phase 3, commits 75d17a7 + batch-2 commit, tests/test_phase3_acceptance.py, tests/test_multihop.py, scripts/audit_edges.py
- **Status**: Active

## [2026-11-26 17:51] FEATURE: Phase 3 batch 1 merged — PageRank graph depth, multi-hop proof, LLM extraction
- **Decided by**: PM (merge); Reviewer APPROVE iteration 1/3, zero blockers; User standing loop [2026-11-26 16:23]
- **Rationale**: Phase 3 batch 1: graph context assembly (HippoRAG-style personalized PageRank), the multi-hop acceptance proof, and env-gated LLM entity/relation extraction (cloud allowed per [2026-11-26 16:23]).
- **Impact**: Commit (see git log). retrieval/graph.py: personalized_pagerank + page_rank_expand (direction-aware, strength weights, staleness decay exactly halving PR — verified 0.103454 -> 0.051727); search() graph_ranking auto: hops=1 stays byte-identical v1 expand, hops>1 uses pagerank. tests/test_multihop.py: planted chain A->B->C->D, vector-only rank of D = 12 (missed in top-5), graph hops=3 rank 1 with via_relationship provenance, control hops=1 clean. pipeline/extract.py + ingest hook: EXTRACT_ENABLED/EXTRACT_API_URL/EXTRACT_MODEL/EXTRACT_API_KEY env-only (key sanitized from all errors), data-only system prompt (injection text lands in user message only — tested), 9-type relation allowlist, strict validation, insert-only entity/relationship storage. Evidence: graph_pr all steps, multihop ALL PASS, extract 12/12, golden 30/30, MCP acceptance p50 2.568s, mcp smoke 98/98, dropbox 23/23, all regressions green. Reviewer LOWs folded into batch 2: ingest.py outer catch must _sanitize, rel_count overcounts ON CONFLICT skips, entity upsert lacks UNIQUE. Next: batch 2 (edge-audit tooling + multi-hop golden + LOW fixes + phase3 acceptance), then final double audit E/F.
- **Related**: ROADMAP.md Phase 3, README.md HippoRAG, commit in git log
- **Status**: Active

## [2026-11-26 16:23] PROCESS: Standing Phase 3 loop authorized (same workflow as Phase 1/2 loops + final double audit)
- **Decided by**: User (answered PM's two questions: cloud API allowed for extraction; standing loop for all of Phase 3)
- **Rationale**: Continue into Phase 3 (knowledge graph depth + hybrid retrieval quality: PageRank context assembly, multi-hop acceptance, LLM entity/relation extraction, 50-edge precision audit) with the exact workflow used before: per-batch worker spawn -> Reviewer (large/high, max 3 iterations) -> PM merge + commit + ledger entry, then a FINAL double independent audit (audit-E + audit-F) with fixes merged, cron-driven while the session is live, final report at the end.
- **Impact**: Phase 3 batches: (1) graph PageRank depth in retrieval/ + multi-hop e2e + LLM extraction worker; (2) 50-edge audit tooling + multi-hop golden questions; (3) Phase 3 acceptance e2e. Then audit-E/F, fixes, merge, summary. INVARIANTS unchanged from [2026-11-26 09:46] plus: cloud API keys live ONLY in env vars (EXTRACT_API_KEY etc.), never logged, never committed, stripped from error messages; run suites one at a time, latency-budget suites with default threads on an idle box; anything out of Phase 3 scope requires asking the User.
- **Related**: ROADMAP.md Phase 3, PM_LEDGER [2026-11-26 09:46] invariants, audit backend decision below
- **Status**: Active

## [2026-11-26 16:23] ARCHITECTURE: Phase 3 LLM extraction backend — cloud API allowed, env-keyed, provider-independent
- **Decided by**: User
- **Rationale**: Entity/relation extraction (Phase 3) needs an LLM; user allows a cheap cloud API via an OpenAI-compatible endpoint, with the key only in env (EXTRACT_API_KEY), never logged or committed, stripped from errors. A local endpoint (Ollama-style) remains possible through the same EXTRACT_API_URL interface. Extraction is env-gated (EXTRACT_ENABLED=1 + EXTRACT_API_URL set), so the default pipeline stays fully local and free.
- **Impact**: pipeline/extract.py: POST {EXTRACT_API_URL}/chat/completions, model EXTRACT_MODEL, JSON-only schema-constrained output, relation types from a small allowlist (relates_to, uses, depends_on, supersedes, contradicts, produced_by, taught_in, evaluated_in, applies_to), strict validation (invalid dropped), caps on counts, clean retry/timeout/429 handling. System prompt treats ingested text as DATA (never follow instructions inside it). Results become entity knowledge_items + relationships rows (strength=confidence), wired into ingest as an optional post-step. Local-only remains the default.
- **Related**: README.md extraction row (local 8B or cheap cloud), ROADMAP Phase 3, PM_INSTRUCTIONS security standards
- **Status**: Active

## [2026-11-26 15:41] QUALITY+SECURITY: Second double audit (audit-C + audit-D) done; all findings fixed and merged (a2eae32) — LOOP COMPLETE
- **Decided by**: PM (reconciliation + merge); Reviewer APPROVE at iteration 1/3 with every fix verified live; User loop authorization [2026-11-26 09:46]
- **Rationale**: User asked to loop until Phase 2 finished, then double-audited, fixed, merged. Phase 2 completed (5f7e27f, c007b13); two fresh independent auditors (audit-C, audit-D, large/high, no shared output) then audited the whole repo and re-ran all suites.
- **Impact**: Commit a2eae32 fixes everything the audits found. HIGH fixed: redirect-SSRF bypass in extract_url (allow_redirects=False + per-hop _assert_public_url + 10MB stream cap; regression tests prove 302->169.254.169.254 refused). MEDIUMs fixed: bridge path confinement for pdf/image/audio payloads (realpath under dropbox/inbox or tempdir, BRIDGE_PATH_ALLOWLIST; /etc/passwd refused live), get_related hops clamp 1..5 (was a proven hang), restore.sh dbname now parsed via psycopg conninfo last-wins fail-closed (duplicate-dbname and ?dbname= regressions). LOWs fixed: token_budget clamp, serve-mode undecodable-line robustness, update_item metadata merge + document_hash re-stamp, backup/restore credential redaction (live-proven), dropbox CSRF Origin/Sec-Fetch-Site 403s, vision URL scheme check, MCP add_knowledge enum now all 7 source types, distillation 0.1 for raw media, auto_link N+1, export_chat persistent serve child (5.5x faster), no f-string SQL in tests, smoke spawns node directly. ACCEPTED RESIDUALS (documented, not fixed): DNS-rebind TOCTOU window in _assert_public_url, CGNAT 100.64/10 not rejected, README sandboxing wording vs v1 reality, no Python lockfile, OMP_NUM_THREADS=2 makes the <5s latency budgets fail (run latency suites with default threads, one torch process at a time; enlarge the Windows pagefile — os error 1455 under 2+ torch processes), dropbox smoke leaves ignored files in inbox/. Final evidence (reviewer live runs): golden recall@5 30/30, load p50 1.874s, MCP acceptance p50 3.757s < 5s, pipeline 12/12, bridge 13/13, url_guard 12/12, security_g1 9/9, e2e 9/9, phase2 acceptance all legs, mcp smoke 98/98, dropbox smoke 23/23, backup/rawstore/notes_sync/pdf/vision/export_chat/github all green; guards: postgres/latest REFUSING exit 1, unset SKIP exit 0, postgres DB untouched. Cron loop afbb3fed cancelled. PHASE 2 DONE + DOUBLE AUDITED + FIXED + MERGED. Next (needs fresh user approval): Phase 3 (graph depth: PageRank context assembly, query router; reranker budget already in), Phase 4 curator loop, dream tool, README security wording alignment, Python lockfile.
- **Related**: audit-C/audit-D reports, reviewer-G report, commit a2eae32, ROADMAP.md Phase 2
- **Status**: Active

## [2026-11-26 11:57] FEATURE: PHASE 2 COMPLETE — multimodal ingestion done + acceptance green (batches 2-3: 5f7e27f, c007b13)
- **Decided by**: PM (merges); Reviewers APPROVE at iteration 1/3 for every batch, zero blockers; User loop authorization [2026-11-26 09:46]
- **Rationale**: Phase 2 remaining scope from ROADMAP delivered: Docling PDF (pypdf fallback), vision-LLM images (OCR fallback), AI-chat exporter, golden set 30 questions across media (recall@5 = 30/30), and the Phase 2 acceptance run.
- **Impact**: Commits 5f7e27f (batch 2) + c007b13 (batch 3). Acceptance tests/test_phase2_acceptance.py: real extractor output — raw PDF (marker text found), spoken wav via faster-whisper, live GitHub Spoon-Knife (2 blob-provenance rows) — all ingested via one bridge serve child and hybrid-searched back with title+source_uri provenance, by_method vector/fts/graph. Golden recall@5 = 30/30. All regression suites green (pipeline 11/11, bridge 11/11, e2e 9/9, url_guard 7/7, audio 6/6, youtube 11/11, github 19/19, pdf, vision 10/10, export_chat, load, backup, mcp acceptance). Reviewer-assessed deviations accepted: pdf stamps file path as source_uri (path IS the original location per schema spec); audio derives decode ext from source_uri for byte payloads. Next per loop: SECOND double independent audit (audit-C/audit-D) over the full repo, then fixes+merge, then final summary.
- **Related**: ROADMAP.md Phase 2, commits 5f7e27f/c007b13, tests/test_phase2_acceptance.py, tests/golden/
- **Status**: Active

## [2026-11-26 09:46] PROCESS: New autonomous loop authorized — finish Phase 2, then double audit + fixes + merge
- **Decided by**: User ("yes, loop until phase 2 is finished, double audited fixed and merged")
- **Rationale**: Phase 2 batch 1 (audio/youtube/github) is merged (8dbe657). Remaining Phase 2 scope per ROADMAP: Docling/MinerU PDF upgrade, vision-LLM image interpretation, AI-chat exporters, golden set expansion to 30 questions across media, plus the Phase 2 acceptance run (drop paper/lecture/repo -> searchable provenance-linked chunks).
- **Impact**: Standing approval covers exactly: (1) Phase 2 batch 2 — three workers: PDF/Docling upgrade, vision-LLM image upgrade, chat exporter + golden-30; (2) Phase 2 batch 3 — acceptance e2e test (tests/test_phase2_acceptance.py); (3) per-batch Reviewer (large/high, max 3 iterations) + PM merge + commits + ledger entries + ROADMAP ticks; (4) a SECOND double independent audit — two fresh auditor sessions (audit-C, audit-D, large/high, no shared output) over the full repo, PM reconciles findings, fixes via worker/PM + reviewer pass, merge, log; (5) final summary. INVARIANTS identical to [2026-11-26 00:08]: never modify/delete user data; all SQL parameterized; DB-touching tests only via TEST_DATABASE_URL with the delimited (^|_)test(_|$) refusal before any connection and SKIP exit 0 without DB; no secrets; localhost-only servers; workers never commit; disjoint file paths; tasks 1-2h; run suites ONE at a time with OMP_NUM_THREADS=2 (16GB commit limit, os error 1455); anything ambiguous that could harm data or exceed this scope -> STOP, log a blocked entry, report. Anything outside Phase 2 + its audit requires asking the User. A cron tick drives the loop while the session is live.
- **Related**: ROADMAP.md Phase 2, PM_LEDGER [2026-11-26 00:08] invariants, commits dd1b032/8dbe657
- **Status**: Active

## [2026-11-26 04:31] FEATURE: Phase 2 batch 1 merged (commit 8dbe657) — multimodal extractors live; AUTONOMOUS LOOP COMPLETE
- **Decided by**: PM (merge); Reviewer APPROVE at iteration 1/3, zero blockers; User standing loop authorization [2026-11-26 00:08]
- **Rationale**: Phase 2 kickoff per ROADMAP: audio transcription, YouTube captions, GitHub repo import — the three highest-value multimodal paths. PDF (pypdf) and image OCR (pytesseract) already existed from batch 1.
- **Impact**: Commit 8dbe657. pipeline/extractors.py: extract_audio (faster-whisper, lazy import, cached model, CUDA-probe fallback to CPU int8), extract_youtube (yt-dlp, host allowlist on top of SSRF guard, VTT/SRT->text, metadata-only fallback never fabricating transcripts). pipeline/github_import.py: public repo import via REST with host/segment regex validation, caps before download, optional GITHUB_TOKEN, per-file provenance URLs. bridge.py source_types += youtube/audio/github; github inserts each file via the shared parameterized insert-only path (_ingest_one). Evidence: test_audio 6/6 incl. real transcription; test_youtube 11/11 incl. live "Me at the zoo" caption fetch; test_github 19/19 no-DB + DB smoke with live Spoon-Knife ingest (2 files -> 2 nodes -> 2 rows); all regression suites green (pipeline 11/11, url_guard 7/7, bridge 11/11, e2e 9/9, fixes_f1 all); bridge error envelopes clean for bad audio/youtube/github input; extractors import stays 0.4s (lazy heavy deps). Residual LOWs logged: uncapped audio file read into RAM at bridge (optional max_audio_bytes), DNS-rebind TOCTOU in _assert_public_url (pre-existing). LOOP COMPLETE per [2026-11-26 00:08]: Phase 1 done, double independent audit done + all findings fixed and merged (534f3ed), Phase 2 started (8dbe657). Cron loop 187541ae cancelled by PM. Next candidates (need fresh user approval): Phase 2 remainder (MinerU/Docling PDF upgrade, vision-LLM screenshots, AI-chat exporters, golden set to 30 questions), Phase 3 graph/rerank depth (PageRank context assembly, query router), curator loop (Phase 4), dream tool.
- **Related**: ROADMAP.md Phase 2, commit 8dbe657, tests/test_audio.py, tests/test_youtube.py, tests/test_github.py, pipeline/github_import.py
- **Status**: Active

## [2026-11-26 03:55] QUALITY: Audit fix batch merged (commit 534f3ed) — all double-audit findings closed; review iteration 1/3 + PM fix for the one blocker
- **Decided by**: PM; Reviewer verified every fix live; User standing loop authorization [2026-11-26 00:08]
- **Rationale**: Both independent audits converged; the fix batch (F1 core / F2 test guards / F3 shell+docs+hygiene) implemented all findings. Reviewer verdict at iteration 1/3: every fix FIXED and green live, with ONE HIGH blocker (N1: test_e2e.py + test_fixes_f1.py still used the substring "test" guard and connected before refusing — dbname "latest" passed, proven live). PM applied the delimited (^|_)test(_|$) regex + name-check-before-connect to both files and verified: "latest" refused exit 1 with 0 tables created, "postgres" refused, unset SKIP exit 0.
- **Impact**: Commit 534f3ed. New locked semantics: whole-document metadata.document_hash is the version key (same source_uri + same hash = duplicate; same uri + different hash = append-only NEW version; excerpts excluded from the uri leg) — this also fixed silent dedup failure for multi-chunk docs and sync_notes dropping >=0.95-similar edits. Staleness penalty now applies in FINAL ranking (final_score = rerank_score * 0.5 if stale; stale never filtered). 8k-token output budget enforced (greedy fill + truncation + estimated_tokens). Retrieval now truly parallel (separate connections, shared vector seeds). Bridge clamps hops<=5/top_k<=100/candidate_k<=200, passes token_budget, exposes store_raw. All 10 test entry points on TEST_DATABASE_URL + delimited refusal + SKIP. restore.sh delimited guard first. extract_url SSRF guard (scheme + private/loopback/link-local/reserved IP rejection; residual DNS-rebind TOCTOU noted LOW). dropbox refuses non-loopback HOST without ALLOW_NONLOCAL_HOST=1. .gitignore: backups/, notes/*.md. READMEs document serve mode. Residual LOWs logged: SSRF TOCTOU, MCP <5s budget is thread-cap sensitive (OMP_NUM_THREADS=2 -> ~6s; default threads -> p50 2.98s), load p50 1.47s -> 2.82s (still under budget). ENV: 16GB box repeatedly hits os error 1455 with 2+ torch processes — run suites one at a time; recommend enlarging the Windows pagefile. Next: Phase 2 batch 1 (multimodal extractors).
- **Related**: audit-A/audit-B reports, reviewer-fixes report, commit 534f3ed, tests/test_fixes_f1.py, tests/test_url_guard.py
- **Status**: Active

## [2026-11-26 02:04] QUALITY: Double independent audit done (audit-A + audit-B, both large/high) — FIXES-NEEDED, fix batch spawned
- **Decided by**: PM (reconciliation of two independent read-only audits); User standing loop authorization [2026-11-26 00:08]
- **Rationale**: Two independent auditors (separate sessions, no shared output) both re-ran all 10 suites live (all green) and converged on the same defects. No SQLi/command-injection/path-traversal/real-secrets/non-localhost exposure found. Convergent findings the PM will fix: (1) tests/test_bridge.py + both node smokes read DATABASE_URL without the locked TEST_DATABASE_URL+dbname-refusal guard (audit-B rated HIGH); (2) staleness x0.5 penalty never affects final ranking because rerank re-sorts purely by rerank_score; (3) content-hash dedup silently fails for multi-chunk docs (probe: 4-chunk doc inserted twice); (4) sync_notes can silently drop edits that stay >=0.95 cosine-similar; (5) <=8k-token retrieval budget unenforced on output; plus LOWs: backups/ not gitignored, stale serve-mode READMEs, HOST env override unwarned, extract_url has no scheme/private-IP (SSRF) guard, substring "test" guard passes dbname "latest", HARD_CAP sentence edge in chunk.py, unbounded hops/top_k/candidate_k, retrieval "parallel" runs on one shared connection + duplicate vector query, e2e step_2 non-falsifiable branch. Environmental: 16GB box hits Windows os error 1455 (commit limit) when 2+ torch processes load — recommend enlarging the pagefile.
- **Impact**: Three fix workers spawned with disjoint file ownership (F1 python core: dedup/ingest/chunk/retrieval/bridge/sync_notes + pipeline/e2e tests; F2 test guards + node smokes; F3 shell/exposure/docs/hygiene). Interface locked between F1 and F2: ingest() gains whole-document metadata.document_hash; find_duplicate treats same source_uri + same document_hash as duplicate, same source_uri + different hash as a NEW VERSION (insert, append-only); sync_notes passes source_uri=abs path directly (stamp hack removed) and skips when document_hash matches. After fixes: reviewer pass, PM merge, ledger entry. Then Phase 2 batch 1.
- **Related**: audit-A/audit-B session reports, PM_LEDGER invariants, tests/, commit 46cb71d
- **Status**: Active

## [2026-11-26 01:26] FEATURE: Batch 4 merged — PHASE 1 COMPLETE (raw store, notes sync, MCP acceptance green)
- **Decided by**: PM (merge); Reviewer APPROVE at iteration 1/3, zero blockers; User standing loop authorization [2026-11-26 00:08]
- **Rationale**: Phase 1 remainder from ROADMAP: content-addressed raw document store (portability), Markdown vault sync, and the real-corpus MCP acceptance run. Reviewer verified all guards live (REFUSING exit 1 on non-test DBs, SKIP exit 0 without DB, postgres DB untouched, raw/ gitignored) and assessed the sync_notes content_hash stamp as a non-issue (it strengthens whole-file hash dedup).
- **Impact**: Commit fc4ee8a. tests/test_mcp_acceptance.py fully green: 8 locked tools, 5/5 searches recall+title correct, latency min 2.63s / p50 2.73s / max 2.91s vs 5s budget, add_knowledge write round-trip 0.53s, search-after-write 2.6s. scripts/sync_notes.py: append-only versioning (changed file -> new row, old row kept), skip-unchanged by content hash, --once/--watch. pipeline/raw_store.py + ingest(store_raw=True): atomic content-addressed raw/<2>/<sha> store. PHASE 1 ACCEPTANCE MET. Next per loop: double independent audit, then Phase 2 batch 1.
- **Related**: ROADMAP.md Phase 1, commit fc4ee8a, tests/test_mcp_acceptance.py, scripts/sync_notes.py, pipeline/raw_store.py
- **Status**: Active

## [2026-11-26 01:26] ARCHITECTURE: Persistent bridge `serve` mode locked as the frontend protocol
- **Decided by**: PM (after worker finding + reviewer verification)
- **Rationale**: MCP acceptance initially failed the <5s latency budget (~22-38s per search) because mcp-server spawned a fresh `python -m bridge <cmd>` per tool call, paying ~16s of torch/model loading every call. Fix: `python -m bridge serve` — one long-lived process, one JSON request per stdin line ({"id", "command", "args"}), exactly one JSON response line per request on stdout, models loaded once. Both frontends (mcp-server/src/index.ts BridgeClient, dropbox/server.js) now keep one persistent child with respawn-once and per-call timeouts. Measured: MCP search 2.6-2.9s warm, dropbox API ~0.6s warm. Single-shot mode remains for scripts/tests.
- **Impact**: LOCKED SPEC ADDITION: all long-lived frontends (MCP server, dropbox UI) MUST talk to Python via `bridge serve`, never per-call spawn. Related operational note: loading torch in 3+ processes at once exhausted the Windows commit limit (os error 1455) on the 16GB dev box — test_mcp_acceptance now seeds via one bridge serve process; recommend the user enlarge the pagefile or keep model-loading processes to one.
- **Related**: bridge.py serve command, mcp-server/src/index.ts, dropbox/server.js, tests/test_bridge.py test_serve_mode, commit fc4ee8a
- **Status**: Active

## [2026-11-26 00:08] PROCESS: Autonomous loop authorized by User — finish Phase 1, double independent audit + fixes, then start Phase 2
- **Decided by**: User (standing approval, given before sleeping)
- **Rationale**: User asked the PM to loop autonomously: complete all Phase 1 batches, run a double independent code audit and fix findings, then start Phase 2. A recurring cron tick drives the loop while the session is live.
- **Impact**: Standing approval covers exactly: (1) Phase 1 remainder batch — raw content-addressed store, Markdown notes sync, real-corpus MCP acceptance run; (2) per-batch Reviewer (large/high, max 3 iterations) + PM merge + commits + ledger entries; (3) DOUBLE INDEPENDENT AUDIT — two separate auditor sessions (large/high) with identical self-contained audit prompts, findings reconciled by PM, fixes via worker/reviewer, merged and logged; (4) Phase 2 start — batch 1 multimodal extractors (audio transcription, YouTube captions, GitHub import) wired into pipeline/extractors.py + bridge.py with tests. INVARIANTS (non-negotiable): never modify or delete user data; all SQL parameterized; DB-touching tests only on databases whose name contains "test" via TEST_DATABASE_URL with the refusal guard, SKIP exit 0 without DB; no secrets; servers localhost-only; workers never commit; tasks 1-2h in disjoint paths; if anything ambiguous could harm data or exceed scope — STOP and log a blocked entry instead of guessing. Anything outside this list requires asking the User.
- **Related**: PM_INSTRUCTIONS.md workflow, ROADMAP.md Phase 1/2, cron loop schedule, commits 9c38ce3/c61b178/10b5c08
- **Status**: Active

## [2026-11-25 23:56] FEATURE: Batch 3 merged — golden set, backup/restore rehearsal, 200-doc load test (review iteration 1/3, APPROVE)
- **Decided by**: PM (merge); Reviewer verdict APPROVE at iteration 1 of max 3, zero blocking issues; User approved the spawn
- **Rationale**: Closes the remaining Phase 0/1 acceptance gaps from ROADMAP.md: golden test set (10 questions, recall@5 >= 80%), backup+restore rehearsal, 200-doc ingest with retrieval < 5s.
- **Impact**: Merged as commit 10b5c08. Evidence (all re-run live by the reviewer on PG 16.16+pgvector @ 127.0.0.1:5433): pipeline 11/11, bridge 10/10, e2e 9/9, golden recall@5 = 10/10 on a 15-doc corpus, backup/restore rehearsal PASS twice (dump 20.7KB -> restore -> counts + FTS hit verified; refusal exit 1 on non-test target with target verified unchanged), load test 200/200 inserted at 2.06 docs/s with per-query hybrid latency min 1.377s / p50 1.467s / max 1.645s vs 5s budget and recall@5 10/10, MCP smoke 97 assertions, dropbox smoke 18/18. Guard pattern verified in all three new tests (TEST_DATABASE_URL only, refuse dbname without "test" before any destructive op, SKIP exit 0 without DB; postgres DB confirmed untouched). PERF NOTE: bge-reranker-v2-m3 on CPU dominates per-query cost and scales with merged-candidate count x doc length (one 5.7s outlier with ~25-word docs at candidate_k=20) — candidate_k tuning is the lever if latency grows with corpus size. scripts/README.md:43 documents that pg_dump/pg_restore client major must match server major (pg_dump 18 archive fails on PG16). Next: Phase 1 remainder (raw document store, Markdown notes integration) or Phase 2 multimodal items.
- **Related**: ROADMAP.md Phase 0/1 acceptance, README.md Part 3 backup register, commit 10b5c08, tests/test_e2e.py guard pattern
- **Status**: Active

## [2026-11-25 23:14] PROCESS: Batch 3 approved — spawning 3 workers (golden test set, backup/restore rehearsal, 200-doc load check)
- **Decided by**: User (approved PM's batch 3 proposal)
- **Rationale**: These are the remaining Phase 0/1 acceptance gaps in ROADMAP.md: golden test set (10 questions, recall@5 >= 80%), backup+restore rehearsal, and 200-doc ingest with retrieval < 5s. All three need a real Postgres; the local pgvector server on 127.0.0.1:5433 is up.
- **Impact**: Workers add tests/test_golden.py + tests/golden/ (seed corpus + questions), scripts/backup.sh + scripts/restore.sh + scripts/README.md + tests/test_backup.py, tests/test_load.py. Data-safety pattern locked from batch 2: TEST_DATABASE_URL only (never DATABASE_URL fallback), refuse any dbname without "test" before destructive ops, SKIP+exit 0 when no DB. Each worker self-tests against its OWN throwaway DB (golden_test / backup_test + restore_test / load_test on 127.0.0.1:5433) so parallel workers don't collide. After submissions: Reviewer (large, high, max 3 iterations), then PM merges and logs.
- **Related**: ROADMAP.md Phase 0/1 acceptance criteria, README.md Part 3 (backup: pg_dump logical vs WAL/PITR), tests/test_e2e.py guard pattern, commits 9c38ce3 + c61b178
- **Status**: Active

## [2026-11-25 22:53] FEATURE: Batch 2 merged — bridge + MCP server + dropbox UI + e2e tests (review iteration 1/3)
- **Decided by**: PM (merge decision); Reviewer verdict APPROVE-WITH-FIXES-APPLIED at iteration 1 of max 3; User approved the spawn
- **Rationale**: All three workers delivered against the locked bridge contract (`python -m bridge <command> '<json>'`, one JSON line out). Reviewer re-ran every suite on a live Postgres 16 + pgvector and found no blocking issues. PM applied final data-safety fixes before merging.
- **Impact**: Merged as commits 9c38ce3 (batch 1) and c61b178 (batch 2). Evidence: tests/test_pipeline.py 11/11; tests/test_bridge.py 10/10; mcp-server smoke 97 assertions; dropbox smoke 18/18 live (twice, idempotent) + 15/15 no-DB; tests/test_e2e.py 9/9 twice on a throwaway DB, SKIP+exit 0 without DB, REFUSE+exit 1 on a non-test dbname. PM/reviewer fixes: pgvector numpy loader now lives in pipeline/db.py + retrieval/db.py get_connection() (single mechanism; worker runtime shims removed); bridge strips raw embeddings from JSON payloads; test_e2e.py uses TEST_DATABASE_URL only and refuses dbnames without "test"; dropbox smoke deletes its own item via bridge delete_item; package-lock.json files now tracked. Next batch candidates: golden test set (10 questions, recall@5), backup+restore rehearsal, 200-doc load check, dream tool (Phase 2).
- **Related**: PM_INSTRUCTIONS.md workflow, locked MCP v1 / Retrieval v1 / Ingestion v1 specs, ROADMAP.md Phase 1, commits 9c38ce3 + c61b178
- **Status**: Active

## [2026-11-25 21:52] PROCESS: Batch 2 approved — spawning 3 workers (MCP server, dropbox UI, e2e test)
- **Decided by**: User (approved PM's proposed breakdown)
- **Rationale**: Batch 1 (db/, pipeline/, retrieval/, tests/test_pipeline.py — 11/11 tests pass) is complete. Batch 2 builds the two access surfaces plus end-to-end proof. To keep parallel workers from colliding, PM locked a bridge contract: `python -m bridge <command> '<json-args>'` prints one JSON line (`{"ok": true, "result": ...}` / `{"ok": false, "error": ...}`); commands: ingest, search, get_related, link_items, update_item, delete_item, list_recent. All logic stays in Python; TypeScript/UI shell out to the bridge.
- **Impact**: Three attached workers spawned in current checkout: (1) `mcp-server/` TypeScript stdio MCP server (8 tools, dream = Phase-2 stub) + `bridge.py` at repo root; (2) `dropbox/` localhost-only insert + retrieve UI coded against the bridge contract; (3) `tests/test_e2e.py` against real Postgres via DATABASE_URL (docker per db/README.md). After submissions: one Reviewer (large model, high effort, max 3 iterations), then PM reviews, merges, logs. Workers touch disjoint paths and do not commit.
- **Related**: PM_INSTRUCTIONS.md workflow, MCP spec v1, Retrieval spec v1, db/README.md, tests/test_pipeline.py
- **Status**: Active

## [2026-11-25 18:25] PROCESS: PM handover — new PM session to spawn workers for batch 2+
- **Decided by**: User + PM
- **Rationale**: This PM session was started by another session (depth limit) and cannot spawn child worker sessions. A fresh PM session started directly by the user will have full spawn capability. Handover prompt written to PM_HANDOVER.md.
- **Impact**: Next PM session picks up at batch 2: TypeScript MCP server (stdio, 8 tools) + dropbox UI (insert/retrieve pages), then end-to-end integration test. Batch 1 code (db/, pipeline/, retrieval/, tests/) is complete, tested, and self-reviewed.
- **Related**: PM_HANDOVER.md, all locked specs above
- **Status**: Active

## [2026-11-25 18:20] FEATURE: Batch 1 implemented — DB schema + ingestion pipeline + retrieval engine
- **Decided by**: PM (worker spawn blocked by session depth; implemented directly, self-reviewed)
- **Rationale**: PM session cannot start child sessions (depth limit), so batch 1 was implemented in-session. All locked specs followed. 11 unit tests pass. All SQL parameterized (no injection). Pipeline is insert-only — never modifies existing rows (data safety).
- **Impact**: Files created: db/schema.sql, db/README.md, requirements.txt, pipeline/ (db, extractors, embed, chunk, dedup, ingest), retrieval/ (db, vector_search, fts, graph, merge, rerank, search), tests/test_pipeline.py. Next: batch 2 (MCP server in TypeScript + dropbox UI).
- **Related**: All four locked specs, tests/test_pipeline.py
- **Status**: Active

## [2026-11-25 18:10] ARCHITECTURE: MCP server spec v1 locked
- **Decided by**: PM + User
- **Rationale**: Local stdio transport keeps everything private and simple; HTTP + API key deferred. Tools: search (hybrid, configurable methods/hops), add_knowledge, get_related, link_items, update_item, delete_item, list_recent. `dream` tool (reweight/reconnect/consolidate) acknowledged as Phase 2 — not stupid, aligns with Proactive Intelligence roadmap phase.
- **Impact**: MCP worker builds TypeScript stdio server exposing all tools over the locked schema and retrieval spec.
- **Related**: All prior specs, ROADMAP.md Phase 6
- **Status**: Active

## [2026-11-25 18:05] ARCHITECTURE: Retrieval spec v1 locked
- **Decided by**: PM + User
- **Rationale**: All three retrieval methods (vector, FTS, graph) run in parallel and merge — no query router needed, simpler and more robust. Graph default 1 hop (configurable). Stale items soft-deprioritized via score penalty, never hidden. bge-reranker-v2-m3 reranks merged results within ≤8k token context budget. Two interfaces: dropbox retrieve page shows raw result sets, MCP tool packages for AI.
- **Impact**: Retrieval worker builds: parallel query executor, result merger/deduper, staleness penalty scorer, bge-reranker integration, MCP search tool, dropbox retrieve UI.
- **Related**: Database schema v1, Ingestion spec v1, README.md Section C
- **Status**: Active

## [2026-11-25 18:00] ARCHITECTURE: Ingestion pipeline spec v1 locked
- **Decided by**: PM + User
- **Rationale**: Two entry points (dropbox UI + MCP add_knowledge tool) cover both deliberate capture and ambient AI-assisted capture. Documents kept whole under 2000 tokens for focused retrieval; semantic chunking above that (8192 hard cap = bge-m3 limit). LLM auto-estimates update_period, distillation_score, and relationships on insert. Dedup via URL/content hash + semantic similarity.
- **Impact**: Ingestion worker builds: content extractors (PDF, image OCR, URL fetch, video description), semantic chunker, bge-m3 embedder, dedup check, auto-relationship extractor, dropbox UI trigger, MCP tool.
- **Related**: Database schema v1, ROADMAP.md Phase 1
- **Status**: Active

## [2026-11-25 17:56] ARCHITECTURE: Database schema v1 locked
- **Decided by**: PM + User
- **Rationale**: Universal node model covers all content types. `knowledge_items` stores searchable text in `content` + original location in `source_uri`; non-text formats (video, large PDFs) get a detailed description and a reference, not inline bytes. `distillation_score` (0-1) replaces binary canonical flag. `update_period` (-1 = never, else seconds) lets the system compute staleness from age instead of manual status. Relationships support both discrete `type` and flexible `context`/`strength`/`direction`. pgvector with HNSW index, bge-m3 embeddings (1024 dims).
- **Impact**: All ingestion and retrieval code builds on this schema. Two tables: `knowledge_items`, `relationships`.
- **Related**: Database Schema Spec (PM session), README.md v3
- **Status**: Active

## [2026-11-25 17:56] ARCHITECTURE: Tech stack — Python pipeline + TypeScript MCP + PostgreSQL
- **Decided by**: User (after PM comparison of Python vs Rust vs Java)
- **Rationale**: Python owns the ML/embedding ecosystem (sentence-transformers, langchain, unstructured) needed for chunking, embeddings, and reranking. TypeScript is the native language of the MCP protocol. Postgres does the heavy lifting (vector, FTS, graph). Rust and Java rejected — no comparable ML ecosystem, more boilerplate, slower iteration for a solo personal system.
- **Impact**: Ingestion/embedding pipeline in Python; MCP server in TypeScript; all storage and retrieval in PostgreSQL + pgvector.
- **Related**: ROADMAP.md open questions, PM session discussion
- **Status**: Active

---

## Initial Project Decisions

### [2026-11-25 16:45] ARCHITECTURE: PostgreSQL-centric hybrid system
- **Decided by**: User (from README.md research)
- **Rationale**: Single battle-hardened database for relational facts, property graph, pgvector embeddings, and full-text search. Avoids multi-database operational overhead and licensing issues (Neo4j GPL, FalkorDB SSPL).
- **Impact**: All knowledge (entities, edges, chunks, sources) stored in PostgreSQL. Graph traversal via relational property graph pattern (Graphiti-compatible).
- **Related**: README.md Section E, ROADMAP.md
- **Status**: Active

### [2026-11-25 16:45] ARCHITECTURE: Hybrid retrieval (vector + graph + full-text)
- **Decided by**: User (from README.md research)
- **Rationale**: Vector RAG wins on direct factoid retrieval; GraphRAG wins on multi-hop, associative, and corpus-level "themes" questions. Systematic evaluations confirm hybrid approach is superior.
- **Impact**: Query router selects retrieval method based on query type. Results merged and reranked (bge-reranker-v2-m3). Context budget target: ≤8k tokens/query.
- **Related**: README.md Section C, ROADMAP.md Phase 3
- **Status**: Active

### [2026-11-25 16:45] PROCESS: Multi-agent workflow with PM/Worker/Reviewer roles
- **Decided by**: User
- **Rationale**: Scalable, quality-focused development process. Separation of concerns: PM coordinates, Workers implement, Reviewer ensures quality.
- **Impact**: All code goes through Reviewer (max 3 iterations). PM makes final merge decisions. User provides direction to PM.
- **Related**: PM_INSTRUCTIONS.md
- **Status**: Active

### [2026-11-25 16:45] SCOPE: Start with Phase 1 (MVP Second Brain)
- **Decided by**: User (implied by ROADMAP.md)
- **Rationale**: Build foundation before advanced features. Phase 1 delivers working knowledge base with basic ingestion and retrieval.
- **Impact**: Focus on: raw document store, Markdown integration, PostgreSQL schema, chunk embeddings, FTS, basic MCP server, manual ingestion.
- **Related**: ROADMAP.md Phase 1, README.md Section G
- **Status**: Active

### [2026-11-25 16:45] QUALITY: Max 3 review iterations per task
- **Decided by**: User
- **Rationale**: Prevent infinite loops while allowing reasonable iteration. Forces workers to understand requirements before starting.
- **Impact**: After 3 failed reviews: hard reset, worker must try different approach. PM can override in exceptional cases.
- **Related**: PM_INSTRUCTIONS.md Workflow
- **Status**: Active

### [2026-11-25 16:45] PROCESS: Append-only ledger for all decisions
- **Decided by**: User
- **Rationale**: Maintain permanent, auditable record of project history. Prevents knowledge loss and provides context for future decisions.
- **Impact**: This file (PM_LEDGER.md) is the source of truth for "why" questions. Never delete or modify entries, only append.
- **Related**: PM_INSTRUCTIONS.md Decision Documentation
- **Status**: Active

---

## How to Add New Entries

1. **Create new entry at TOP** of this file (below this header, above existing entries)
2. **Use ISO timestamp**: `[YYYY-MM-DD HH:MM]` 
3. **Choose category** from: ARCHITECTURE, FEATURE, PROCESS, SCOPE, PRIORITY, RESOURCE, QUALITY, SECURITY
4. **Fill all fields**: Decided by, Rationale, Impact, Related, Status
5. **Never edit existing entries** - only append new ones
6. **Commit changes** to git with descriptive message

---

## Decision Index

| Date | Category | Decision | Status |
|------|----------|----------|--------|
| 2026-11-27 03:03 | FEATURE | Phase 4 batch 2 merged: review UI + lessons + contradiction acceptance | Active |
| 2026-11-27 01:50 | FEATURE | Phase 4 batch 1 merged: curator + wiki + dream | Active |
| 2026-11-27 00:52 | QUALITY | Phase 3 loop complete: audit E/F CLEAN, H fixes merged (45fe202) | Active |
| 2026-11-26 22:05 | PROCESS | Next batch (Phase 4 b1) in same loop; then push+merge all to Vanatoa23/SecondBrain | Active |
| 2026-11-26 17:51 | FEATURE | Phase 3 batch 1 merged: PageRank depth + multi-hop proof + LLM extraction | Active |
| 2026-11-26 16:23 | PROCESS | Standing Phase 3 loop authorized (workers+reviewer+merge per batch, final double audit) | Active |
| 2026-11-26 16:23 | ARCHITECTURE | LLM extraction: cloud API allowed, env-keyed; local still default | Active |
| 2026-11-26 15:41 | QUALITY | 2nd double audit fixed+merged (a2eae32); Phase 2 loop complete | Active |
| 2026-11-26 11:57 | FEATURE | Phase 2 complete (5f7e27f, c007b13); acceptance green; next: second double audit | Active |
| 2026-11-26 09:46 | PROCESS | New loop authorized: finish Phase 2, then double audit + fixes + merge | Active |
| 2026-11-26 04:31 | FEATURE | Phase 2 batch 1 merged (8dbe657); autonomous loop complete | Active |
| 2026-11-26 03:55 | QUALITY | Audit fix batch merged (534f3ed); all findings closed; N1 guard blocker PM-fixed + verified | Active |
| 2026-11-26 02:04 | QUALITY | Double audit done; fix batch spawned (guards, staleness ranking, doc-hash dedup, 8k budget, SSRF guard, hygiene) | Active |
| 2026-11-26 01:26 | FEATURE | Batch 4 merged — Phase 1 complete | Active |
| 2026-11-26 01:26 | ARCHITECTURE | Persistent bridge serve mode locked for frontends | Active |
| 2026-11-26 00:08 | PROCESS | Autonomous loop authorized: Phase 1 finish + double audit + Phase 2 start | Active |
| 2026-11-25 23:56 | FEATURE | Batch 3 merged after review (iteration 1/3, APPROVE) | Active |
| 2026-11-25 23:14 | PROCESS | Batch 3 approved — golden set, backup/restore, load test workers | Active |
| 2026-11-25 22:53 | FEATURE | Batch 2 merged after review (iteration 1/3) | Active |
| 2026-11-25 21:52 | PROCESS | Batch 2 approved — 3 workers spawned | Active |
| 2026-11-25 18:25 | PROCESS | PM handover to fresh session for worker spawning | Active |
| 2026-11-25 18:20 | FEATURE | Batch 1: DB + pipeline + retrieval implemented | Active |
| 2026-11-25 18:10 | ARCHITECTURE | MCP server spec v1 locked | Active |
| 2026-11-25 18:05 | ARCHITECTURE | Retrieval spec v1 locked | Active |
| 2026-11-25 18:00 | ARCHITECTURE | Ingestion pipeline spec v1 locked | Active |
| 2026-11-25 17:56 | ARCHITECTURE | Database schema v1 locked | Active |
| 2026-11-25 17:56 | ARCHITECTURE | Tech stack: Python + TypeScript + Postgres | Active |
| 2026-11-25 16:45 | ARCHITECTURE | PostgreSQL-centric hybrid system | Active |
| 2026-11-25 16:45 | ARCHITECTURE | Hybrid retrieval | Active |
| 2026-11-25 16:45 | PROCESS | Multi-agent workflow | Active |
| 2026-11-25 16:45 | SCOPE | Start with Phase 1 | Active |
| 2026-11-25 16:45 | QUALITY | Max 3 review iterations | Active |
| 2026-11-25 16:45 | PROCESS | Append-only ledger | Active |

---

*Ledger initialized. All future decisions will be appended here.*
