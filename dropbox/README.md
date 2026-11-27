# Dropbox UI — local-only insert/retrieve/review for Second Brain

A minimal Node + Express web UI with three pages:

- **`/` (insert)** — all 7 bridge source types: paste raw **text**, submit a
  **URL** / **YouTube** / **GitHub** link, or upload a **PDF** / **image** /
  **audio** file. File modes use upload-then-ingest: the file is first saved to
  `dropbox/inbox/` via `POST /api/upload`, then `POST /api/ingest` is called
  with the returned inbox path as `payload` (plus an optional title). Submits
  show status (`inserted` / `duplicate`), `node_ids`, `relationship_ids`,
  `chunks`.
- **`/retrieve`** — query + method checkboxes (vector / fts / graph), hops (default 1),
  top_k (default 10). Submits to `POST /api/search` and renders **four panels**: the raw
  per-method result sets (`by_method.vector` / `.fts` / `.graph`) plus the merged +
  reranked `results` list. Each result shows title, score, `source_uri`, and a stale flag.
- **`/review`** — the curator review queue for human approval (Phase 4). Reads
  `review_queue.json` at the repo root (written by `python scripts/curator.py`, gitignored)
  and lists **pending** flags grouped by reason (stale / contradiction / superseded /
  isolated) with title, `source_uri`, evidence, and flagged age. **Approve** / **Dismiss**
  buttons POST a decision and refresh the list; decided items are shown greyed at the
  bottom. Empty state: "no pending flags — run python scripts/curator.py". The page is
  self-contained — its header nav (Insert / Retrieve / Review) is rendered by `app.js`.

All knowledge-base access goes through the Python bridge at the repo root as
`bridge.py` — this package never touches SQL or the DB directly. The server keeps
**ONE persistent `python -m bridge serve` child process** (spawned lazily on the
first call, respawned once if it dies) and speaks the JSON-lines serve protocol:
one request line `{"id": n, "command": ..., "args": {...}}` per call on the child's
stdin, exactly one id-matched JSON response line per request on stdout. The
single-shot form

```
python -m bridge <command> '<json-args>'   # cwd = repo root, prints one JSON line
```

remains available for scripts and tests only — the UI must not spawn a fresh bridge
per request (the model cold start per spawn is what the persistent serve mode avoids).

The server runs the child with the `PYTHON` env var (default `python`) and returns the
bridge envelope verbatim: HTTP 200 with `{"ok": true, "result": ...}`, HTTP 500 with
`{"ok": false, "error": "..."}`.

## Run

Prerequisites: Python deps installed (`pip install -r requirements.txt`), Postgres with
pgvector running and schema applied (see `db/README.md` — `docker run ... pgvector/pgvector:pg16`,
then `psql "$DATABASE_URL" -f db/schema.sql`), `DATABASE_URL` set, and `bridge.py` present at
the repo root.

```bash
cd dropbox
npm install
npm start          # http://127.0.0.1:3000
```

Environment variables:

| Var | Default | Meaning |
|-----|---------|---------|
| `HOST` | `127.0.0.1` | bind address — local only by default; a non-loopback value (`127.0.0.1`/`::1`/`localhost` are loopback) prints a loud WARNING at boot and requires `ALLOW_NONLOCAL_HOST=1`, else the server exits 1 |
| `PORT` | `3000` | listen port |
| `PYTHON` | `python` | Python binary used for the persistent `python -m bridge serve` child |
| `BRIDGE_TIMEOUT_MS` | `180000` | per-call bridge timeout |
| `ALLOW_NONLOCAL_HOST` | unset | set to `1` to deliberately bind a non-loopback `HOST` |

## API

- `POST /api/ingest` — body `{source_type: "text"|"url"|"pdf"|"image"|"youtube"|"audio"|"github", payload, source_uri?, title?}`
  → bridge `ingest` envelope. `text` takes the text itself; `url`/`youtube`/`github`
  take a URL string; `pdf`/`image`/`audio` take a `dropbox/inbox/` file path
  (upload-then-ingest via `POST /api/upload`).
- `POST /api/search` — body `{query, methods?, hops?, top_k?, candidate_k?}`
  → bridge `search` envelope `{"ok":true,"result":{"results":[...],"by_method":{...}}}`.
- `POST /api/upload?filename=<name>` — raw file body (octet-stream), text/markdown/pdf
  plus image (`.png .jpg .jpeg .gif .webp .bmp`) and audio (`.wav .mp3 .m4a .ogg .flac`)
  extensions, filename sanitized, saved into `dropbox/inbox/` → `{ok, result: {path, ...}}`.
- `GET /review` — serves `public/review.html`.
- `GET /api/review-queue` — reads `review_queue.json` from the repo root →
  `{ok:true, queue:{updated_at, flagged:[...]}}` when present, `{ok:true, queue:null}`
  when the curator has not run yet.
- `POST /api/review-decision` — body `{id, reason, decision: "approved"|"dismissed"}`.
  Local 400 validation first (bad input), then it **delegates to the bridge
  `review_decision` command** (locked interface) via the persistent serve child and
  returns the bridge envelope verbatim: HTTP 200 `{"ok":true,"result":{...}}` on
  success, HTTP 500 `{"ok":false,"error":"..."}` when the bridge call fails. The
  bridge atomically updates `review_queue.json` AND the DB `metadata.review_status`;
  this server never writes the queue file itself. Mutating route → CSRF
  Origin/Sec-Fetch-Site guard applies.

## Smoke tests

```bash
cd dropbox
node test/smoke.mjs           # or: npm test
node test/review_ui.test.mjs  # review-page render test (dependency-free DOM stub)
```

`test/smoke.mjs` boots the server on a test port and checks: all pages serve (`/`,
`/retrieve`, `/review`), validation error envelopes are well-formed, upload lands in
`inbox/` safely (all allowed extensions accepted, bad ones rejected), the review
queue API works (queue:null without a file, pending items listed with a temporary
`review_queue.json` at the repo root that is removed in `finally`, review-decision
local 400 validation + 403 CSRF guard + delegation to the bridge `review_decision`
command — 200 `ok:true` when the bridge implements it, 500 `ok:false` error envelope
otherwise), and — when `bridge.py` + `DATABASE_URL`
are available — the full round-trip (insert → duplicate → search → `by_method` keys).
Without a DB it verifies the pages, the boot, and that errors come back as HTTP 500
`{"ok": false, "error": "..."}`; the round-trip then needs a live Postgres.

`test/review_ui.test.mjs` loads `public/app.js` into a minimal DOM stub (no deps),
renders a sample review queue, and asserts every reason section has an `h2` child and
no text node anywhere equals `"undefined"` (the old chained-`append` heading bug).

## Security notes

Binds `127.0.0.1` only by default (a non-loopback `HOST` needs `ALLOW_NONLOCAL_HOST=1`
and prints a loud warning, otherwise the server exits 1 at boot) and rejects non-local
`Host` headers (DNS-rebinding guard; bracketed IPv6 literals like `[::1]:3000` parse to
hostname `::1`); every response carries `X-Frame-Options: DENY` and
`Content-Security-Policy: default-src 'self'; frame-ancestors 'none'` (clickjacking
guard); no
credentials or secrets anywhere; no outbound network calls from the server; uploads are
restricted to text/markdown/pdf/image/audio extensions with sanitized filenames inside
`dropbox/inbox/`; all
dynamic data is rendered in the browser with `textContent` only (never `innerHTML`); all
SQL happens in Python.
