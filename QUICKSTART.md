# Second Brain — Local Quickstart & Dogfooding Guide

Everything needed to run your local Second Brain (Phases 0–5) as a daily driver and validate it on your real corpus before Phase 6.

## 1. Start PostgreSQL + pgvector (persistent volume)

```bash
cp .env.example .env          # optional: customize password/port/options
docker compose up -d          # starts pgvector/pgvector:pg16 on 127.0.0.1:5432
docker compose ps             # wait for (healthy)
```

`docker-compose.yml` binds `127.0.0.1` only, mounts a persistent named volume (`secondbrain_pgdata`), and auto-applies `db/schema.sql` on first boot.

## 2. Install Python & Node dependencies

```bash
# Python environment (Python 3.10–3.12 recommended)
python -m venv .venv
source .venv/bin/activate      # Windows PowerShell: .venv\Scripts\Activate.ps1
pip install -r requirements.txt

# MCP server (TypeScript -> dist/)
cd mcp-server && npm install && npm run build && cd ..

# Dropbox web UI
cd dropbox && npm install && cd ..
```

## 3. Verify readiness (read-only)

```bash
export DATABASE_URL="postgresql://postgres:postgres@127.0.0.1:5432/secondbrain"
export SECONDBRAIN_MCP_SCOPE="read"
python scripts/doctor.py
```

All required checks should report `[OK]` (and `database_connection` will show extensions + row counts).

## 4. Launch the Dropbox UI (Insert / Retrieve / Review)

```bash
cd dropbox
PYTHON="../.venv/bin/python" npm start   # http://127.0.0.1:3000
```

- `http://127.0.0.1:3000/` — ingest text, URLs, YouTube, GitHub repos, PDFs, images, audio
- `http://127.0.0.1:3000/retrieve` — hybrid search (vector + FTS + graph + reranker)
- `http://127.0.0.1:3000/review` — approve/dismiss curator flags

## 5. Connect your MCP client(s)

Copy and adapt one of the ready-made configs in `mcp-server/examples/`:
- **Read-only scope** (recommended default for coding agents): `mcp-server/examples/claude_code.mcp.json` or `claude_desktop.config.json` (`SECONDBRAIN_MCP_SCOPE=read`).
- **Write scope** (trusted agents only): `mcp-server/examples/write-scope.snippet.json` (`SECONDBRAIN_MCP_SCOPE=write` + strong `SECONDBRAIN_WRITE_TOKEN`).

## 6. Real-Corpus Dogfooding Checklist (before Phase 6)

Run this once on your actual knowledge so Phase 6 (digests, cross-project suggestions, gap reports) can be tuned against real graph signals:

1. **Sync your notes vault:**
   ```bash
   NOTES_DIR=/path/to/your/notes python scripts/sync_notes.py
   ```
2. **Ingest a mix of real sources** via Dropbox UI (`/`): 2–3 PDFs/papers, 1–2 GitHub repos, a couple of URLs/YouTube videos, and optionally AI chat exports (`python scripts/export_chat.py`).
3. **Test 10 real queries** via `/retrieve` and your MCP client (`search`, `get_project_context`, `discover_relevant`). Note any bad chunks, missing links, or noisy rankings.
4. **Run the maintenance loop once:**
   ```bash
   python scripts/curator.py
   python scripts/export_wiki.py
   ```
   Open `http://127.0.0.1:3000/review` and time how long it takes to clear the queue (Phase 4 operational target: ≤30 min/week).
5. **Spot-check graph quality:**
   ```bash
   python scripts/audit_edges.py --sample 20
   ```
   Target: ≥95% defensible edges.
6. **Take your first real backup:**
   ```bash
   BACKUP_SOURCE_URL="$DATABASE_URL" BACKUP_DIR=./backups ./scripts/backup.sh
   ```
7. *(Optional)* Wire nightly curator + weekly backup using the templates in `scripts/schedule/`.
