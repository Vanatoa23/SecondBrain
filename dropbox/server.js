#!/usr/bin/env node
/**
 * Second Brain — dropbox UI server. LOCAL ONLY.
 *
 * Serves the insert page (/) and retrieve page (/retrieve) from ./public and
 * exposes two JSON endpoints that shell out to the Python bridge CLI:
 *
 *   POST /api/ingest   {source_type, payload, source_uri?, title?}
 *   POST /api/search   {query, methods?, hops?, top_k?, candidate_k?}
 *   POST /api/upload   raw file body + ?filename=...  -> saves into ./inbox
 *   GET  /review       serves public/review.html (curator review queue UI)
 *   GET  /api/review-queue  -> {ok:true, queue:{...}} or {ok:true, queue:null}
 *                            (reads review_queue.json from the repo root)
 *   POST /api/review-decision {id, reason, decision:"approved"|"dismissed"}
 *                            -> sets status on the matching flagged entry
 *                            (keyed by id+reason), atomic tmp+rename write,
 *                            returns the updated queue
 *
 * Bridge contract (locked by PM — bridge.py is implemented by another worker):
 *   `python -m bridge <command> '<json-args>'` run with cwd = repo root prints
 *   exactly ONE JSON line to stdout: {"ok": true, "result": ...} or
 *   {"ok": false, "error": "..."} (exit 0/1). This server returns that JSON
 *   verbatim: HTTP 200 when ok:true, HTTP 500 when ok:false.
 *
 * Persistent mode: instead of paying the model cold start per request, this
 * server keeps ONE long-lived `python -m bridge serve` child (spawned lazily
 * on the first call, respawned once if it dies) and speaks the JSON-lines
 * serve protocol: one request line {"id": n, "command": ..., "args": {...}}
 * per call, one id-matched response line back. The HTTP envelope behavior is
 * unchanged: 200 ok:true / 500 ok:false / 400 validation.
 *
 * Security: binds 127.0.0.1 only (HOST env to override, but a non-loopback
 * HOST prints a loud warning and requires ALLOW_NONLOCAL_HOST=1, else exit 1),
 * rejects non-local
 * Host headers (DNS-rebinding guard), requires same-origin Origin (or
 * Sec-Fetch-Site same-origin/none) on mutating /api routes (CSRF guard;
 * header-less local tooling like curl is still allowed), makes no outbound
 * network calls, writes
 * no SQL, holds no credentials. All rendering happens client-side with
 * textContent only.
 *
 * Env: HOST (default 127.0.0.1), PORT (default 3000), PYTHON (default python),
 *      BRIDGE_TIMEOUT_MS (default 180000), ALLOW_NONLOCAL_HOST (default unset —
 *      required to bind a non-loopback HOST).
 */
'use strict';

const path = require('node:path');
const fs = require('node:fs');
const { spawn } = require('node:child_process');
const readline = require('node:readline');
const express = require('express');

const REPO_ROOT = path.resolve(__dirname, '..');
const PUBLIC_DIR = path.join(__dirname, 'public');
const INBOX_DIR = path.join(__dirname, 'inbox');
// Curator review queue (written by scripts/curator.py, gitignored runtime
// artifact) lives at the repo root — fixed path, per the Phase 4 spec.
const QUEUE_PATH = path.join(REPO_ROOT, 'review_queue.json');

const HOST = process.env.HOST || '127.0.0.1';

// --- Local-only exposure guard -------------------------------------------
// HOST may override the bind address, but anything that is not a loopback
// address (127.0.0.1, ::1, localhost) would expose this UI to the network:
// print a loud WARNING at boot and require ALLOW_NONLOCAL_HOST=1, else exit 1.
const HOST_NORMALIZED = HOST.toLowerCase().replace(/^\[|\]$/g, '');
const LOOPBACK_HOSTS = new Set(['127.0.0.1', '::1', 'localhost']);
if (!LOOPBACK_HOSTS.has(HOST_NORMALIZED)) {
  console.error('WARNING: ============================================================');
  console.error(`WARNING: HOST=${HOST} is NOT a loopback address — this server would be`);
  console.error('WARNING: reachable from the network. Set ALLOW_NONLOCAL_HOST=1 to allow');
  console.error('WARNING: this deliberately; otherwise the server refuses to start.');
  console.error('WARNING: ============================================================');
  if (process.env.ALLOW_NONLOCAL_HOST !== '1') {
    console.error('REFUSING to bind non-local HOST without ALLOW_NONLOCAL_HOST=1.');
    process.exit(1);
  }
}
const PORT = Number.parseInt(process.env.PORT || '3000', 10);
const PYTHON = process.env.PYTHON || 'python';
const BRIDGE_TIMEOUT_MS = Number.parseInt(process.env.BRIDGE_TIMEOUT_MS || '180000', 10);

const SEARCH_METHODS = ['vector', 'fts', 'graph'];
const INGEST_SOURCE_TYPES = ['text', 'url', 'pdf'];
const ALLOWED_UPLOAD_EXT = new Set(['.txt', '.md', '.text', '.pdf']);
const MAX_UPLOAD_BYTES = 25 * 1024 * 1024;
const REVIEW_DECISIONS = new Set(['approved', 'dismissed']);

fs.mkdirSync(INBOX_DIR, { recursive: true });

const app = express();
app.disable('x-powered-by');

// ---------------------------------------------------------------------------
// Middleware: security headers + local-only Host check (DNS-rebinding guard)
// ---------------------------------------------------------------------------
app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('Cache-Control', 'no-store');
  const hostHeader = String(req.headers.host || '').toLowerCase();
  if (hostHeader) {
    const hostname = hostHeader.replace(/^\[|\]$/g, '').split(':')[0];
    const allowed = new Set(['127.0.0.1', 'localhost', '::1', HOST.toLowerCase()]);
    if (!allowed.has(hostHeader) && !allowed.has(hostname)) {
      return res.status(403).json({ ok: false, error: 'forbidden: this server is local-only' });
    }
  }
  next();
});

app.use(express.json({ limit: '10mb' }));

// ---------------------------------------------------------------------------
// Middleware: CSRF guard on mutating /api routes
// Browsers attach an Origin header to cross-site POSTs; when present it must
// be same-origin (matching this server's Host or HOST:PORT). When Origin is
// absent but Sec-Fetch-Site is present, it must be same-origin or none.
// Requests with NEITHER header (curl, local tooling, node fetch) are allowed.
// The Host-header check above has already run by the time we get here.
// ---------------------------------------------------------------------------
const MUTATING_API_PATHS = new Set(['/api/ingest', '/api/search', '/api/upload', '/api/review-decision']);
app.use((req, res, next) => {
  if (req.method !== 'POST' || !MUTATING_API_PATHS.has(req.path)) return next();
  const origin = req.headers.origin;
  const secFetchSite = String(req.headers['sec-fetch-site'] || '').toLowerCase();
  if (origin) {
    let originHost = null;
    try { originHost = new URL(String(origin)).host.toLowerCase(); } catch { originHost = null; }
    const allowedHosts = new Set([
      String(req.headers.host || '').toLowerCase(),
      `${HOST.toLowerCase()}:${PORT}`,
    ]);
    if (!originHost || !allowedHosts.has(originHost)) {
      return res.status(403).json({
        ok: false,
        error: `forbidden: cross-site Origin header (${String(origin)})`,
      });
    }
  } else if (secFetchSite && secFetchSite !== 'same-origin' && secFetchSite !== 'none') {
    return res.status(403).json({
      ok: false,
      error: `forbidden: cross-site request (Sec-Fetch-Site: ${secFetchSite})`,
    });
  }
  next();
});

// ---------------------------------------------------------------------------
// Bridge client — the ONLY way this server touches the knowledge base.
// One persistent `python -m bridge serve` child; each call writes one JSON
// request line and resolves with the id-matched response line. The child is
// spawned lazily on the first call and respawned once if it dies; a stuck
// call is killed after BRIDGE_TIMEOUT_MS and reported as an error envelope.
// ---------------------------------------------------------------------------
class BridgeClient {
  constructor() {
    this.child = null;
    this.pending = new Map(); // id -> { resolve, timer }
    this.nextId = 1;
  }

  get alive() {
    return this.child !== null && !this.child.killed && this.child.exitCode === null;
  }

  failAll(error) {
    for (const [, p] of this.pending) {
      clearTimeout(p.timer);
      p.resolve({ ok: false, error });
    }
    this.pending.clear();
  }

  killChild() {
    const c = this.child;
    this.child = null;
    if (c) {
      c.removeAllListeners();
      try { c.kill('SIGKILL'); } catch { /* already gone */ }
    }
  }

  spawnChild() {
    this.killChild();
    const child = spawn(PYTHON, ['-m', 'bridge', 'serve'], { cwd: REPO_ROOT });
    this.child = child;
    child.stderr.on('data', (d) => {
      process.stderr.write(`[bridge] ${d}`); // bridge diagnostics stay on stderr
    });
    const rl = readline.createInterface({ input: child.stdout });
    rl.on('line', (line) => this.onLine(line));
    const onDead = (why) => {
      if (this.child !== child) return; // superseded by a respawn
      this.child = null;
      this.failAll(why);
    };
    child.on('error', (err) => onDead(`bridge serve process error: ${err.message}`));
    child.on('close', (code, signal) =>
      onDead(`bridge serve process exited (code ${code}, signal ${signal})`));
  }

  onLine(line) {
    const trimmed = line.trim();
    if (!trimmed) return;
    let msg;
    try { msg = JSON.parse(trimmed); } catch { return; } // non-JSON noise
    if (msg && typeof msg === 'object' && typeof msg.id === 'number') {
      const entry = this.pending.get(msg.id);
      if (entry) {
        clearTimeout(entry.timer);
        this.pending.delete(msg.id);
        entry.resolve(msg);
      }
    }
  }

  call(command, args) {
    if (!this.alive) {
      try {
        this.spawnChild();
      } catch (err) {
        return Promise.resolve({ ok: false, error: `failed to start ${PYTHON}: ${err.message}` });
      }
    }
    const child = this.child;
    const id = this.nextId++;
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        this.killChild(); // stuck child: drop it so the next call respawns fresh
        resolve({ ok: false, error: `bridge ${command} timed out after ${BRIDGE_TIMEOUT_MS} ms` });
      }, BRIDGE_TIMEOUT_MS);
      this.pending.set(id, {
        resolve: (r) => resolve({ ok: r.ok, result: r.result, error: r.error }),
        timer,
      });
      child.stdin.write(JSON.stringify({ id, command, args }) + '\n', (err) => {
        if (err) {
          clearTimeout(timer);
          this.pending.delete(id);
          this.killChild();
          resolve({ ok: false, error: `failed to write to bridge: ${err.message}` });
        }
      });
    });
  }

  // Best-effort warm-up so the first real API call is already warm.
  warmup() {
    this.call('search', { query: 'bridge warmup ping', methods: ['vector', 'fts'], top_k: 1, candidate_k: 5 })
      .catch(() => { /* best effort */ });
  }

  dispose() {
    this.failAll('bridge client disposed');
    this.killChild();
  }
}

const bridge = new BridgeClient();

function runBridge(command, args) {
  return bridge.call(command, args);
}

// ---------------------------------------------------------------------------
// POST /api/ingest — forward to bridge `ingest`
// ---------------------------------------------------------------------------
app.post('/api/ingest', async (req, res) => {
  const body = req.body || {};
  const { source_type, payload, source_uri, title } = body;
  const errors = [];
  if (!INGEST_SOURCE_TYPES.includes(source_type)) {
    errors.push(`source_type must be one of ${INGEST_SOURCE_TYPES.join('|')}`);
  }
  if (typeof payload !== 'string' || payload.length === 0) {
    errors.push('payload must be a non-empty string');
  }
  if (source_uri !== undefined && source_uri !== null && typeof source_uri !== 'string') {
    errors.push('source_uri must be a string or null');
  }
  if (title !== undefined && title !== null && typeof title !== 'string') {
    errors.push('title must be a string or null');
  }
  if (errors.length) {
    return res.status(400).json({ ok: false, error: errors.join('; ') });
  }

  const args = { source_type, payload };
  if (source_uri !== undefined) args.source_uri = source_uri;
  if (title !== undefined) args.title = title;

  const envelope = await runBridge('ingest', args);
  return res.status(envelope.ok ? 200 : 500).json(envelope);
});

// ---------------------------------------------------------------------------
// POST /api/search — forward to bridge `search`
// ---------------------------------------------------------------------------
app.post('/api/search', async (req, res) => {
  const body = req.body || {};
  const { query, methods, hops, top_k, candidate_k } = body;
  const errors = [];
  if (typeof query !== 'string' || query.trim().length === 0) {
    errors.push('query must be a non-empty string');
  }
  if (methods !== undefined &&
      (!Array.isArray(methods) || methods.length === 0 ||
       methods.some((m) => !SEARCH_METHODS.includes(m)))) {
    errors.push(`methods must be a non-empty array subset of [${SEARCH_METHODS.join(', ')}]`);
  }
  for (const [key, value] of [['hops', hops], ['top_k', top_k], ['candidate_k', candidate_k]]) {
    if (value !== undefined && (!Number.isInteger(value) || value < 1)) {
      errors.push(`${key} must be a positive integer`);
    }
  }
  if (errors.length) {
    return res.status(400).json({ ok: false, error: errors.join('; ') });
  }

  const args = { query };
  if (methods !== undefined) args.methods = methods;
  if (hops !== undefined) args.hops = hops;
  if (top_k !== undefined) args.top_k = top_k;
  if (candidate_k !== undefined) args.candidate_k = candidate_k;

  const envelope = await runBridge('search', args);
  return res.status(envelope.ok ? 200 : 500).json(envelope);
});

// ---------------------------------------------------------------------------
// POST /api/upload — save one file into dropbox/inbox/ with a sanitized name.
// Raw request body (application/octet-stream), original name in ?filename=.
// ---------------------------------------------------------------------------
function sanitizeFilename(name) {
  const base = String(name || 'upload')
    .split(/[\\/]/)                       // strip any path components
    .pop()
    .replace(/[^a-zA-Z0-9._-]/g, '_')      // keep only safe chars
    .replace(/^\.+/, '')                   // no leading dots
    .replace(/\.{2,}/g, '.')               // no ".." sequences
    .slice(0, 100);
  return base || 'upload';
}

app.post('/api/upload', express.raw({ type: '*/*', limit: MAX_UPLOAD_BYTES }), (req, res) => {
  const original = sanitizeFilename(req.query.filename);
  const ext = path.extname(original).toLowerCase();
  if (!ALLOWED_UPLOAD_EXT.has(ext)) {
    return res.status(400).json({
      ok: false,
      error: `unsupported file type "${ext || '(none)'}"; allowed: ${[...ALLOWED_UPLOAD_EXT].join(', ')}`,
    });
  }
  const body = req.body;
  if (!Buffer.isBuffer(body) || body.length === 0) {
    return res.status(400).json({ ok: false, error: 'empty upload body' });
  }
  const stored = `${Date.now()}-${Math.random().toString(36).slice(2, 10)}-${original}`;
  const dest = path.join(INBOX_DIR, stored);
  if (!dest.startsWith(INBOX_DIR + path.sep)) {
    return res.status(400).json({ ok: false, error: 'invalid filename' });
  }
  fs.writeFileSync(dest, body);
  return res.status(200).json({
    ok: true,
    result: { path: dest, filename: stored, original_name: original, size: body.length },
  });
});

// ---------------------------------------------------------------------------
// Review queue (curator output) — read + human decision.
// The queue file is review_queue.json at the repo root, written by
// scripts/curator.py: {"updated_at": <iso>, "flagged": [{id, title,
// source_uri, reason, evidence, status}, ...]}. This server never creates it;
// it only reads it and records human decisions (status) on flagged entries.
// ---------------------------------------------------------------------------
function readQueueFile() {
  // Returns {queue} on success, {missing:true} when the file does not exist,
  // {error} when it exists but cannot be read/parsed/validated.
  let raw;
  try {
    raw = fs.readFileSync(QUEUE_PATH, 'utf8');
  } catch (err) {
    if (err && err.code === 'ENOENT') return { missing: true };
    return { error: `cannot read review_queue.json: ${err.message}` };
  }
  let queue;
  try {
    queue = JSON.parse(raw);
  } catch {
    return { error: 'review_queue.json is not valid JSON — refusing to touch it; fix or move it manually' };
  }
  if (!queue || typeof queue !== 'object' || Array.isArray(queue) || !Array.isArray(queue.flagged)) {
    return { error: 'review_queue.json has an unexpected shape (expected {"updated_at": ..., "flagged": [...]})' };
  }
  return { queue };
}

function writeQueueFileAtomic(queue) {
  const tmp = QUEUE_PATH + '.tmp'; // same tmp name scripts/curator.py uses
  fs.writeFileSync(tmp, JSON.stringify(queue, null, 2) + '\n');
  fs.renameSync(tmp, QUEUE_PATH); // atomic on POSIX and Windows (same volume)
}

// GET /api/review-queue — {ok:true, queue:{...}} when the file exists,
// {ok:true, queue:null} when it does not (curator has not run yet).
app.get('/api/review-queue', (req, res) => {
  const r = readQueueFile();
  if (r.missing) return res.status(200).json({ ok: true, queue: null });
  if (r.error) return res.status(500).json({ ok: false, error: r.error });
  return res.status(200).json({ ok: true, queue: r.queue });
});

// POST /api/review-decision — {id, reason, decision} records a human decision
// on the flagged entry keyed by (id, reason). 400 on bad input, missing queue,
// corrupt queue, or no matching entry. Mutating -> CSRF middleware applies.
app.post('/api/review-decision', (req, res) => {
  const body = req.body || {};
  const { id, reason, decision } = body;
  const errors = [];
  if (typeof id !== 'string' || id.trim().length === 0) {
    errors.push('id must be a non-empty string');
  }
  if (typeof reason !== 'string' || reason.trim().length === 0) {
    errors.push('reason must be a non-empty string');
  }
  if (!REVIEW_DECISIONS.has(decision)) {
    errors.push('decision must be "approved" or "dismissed"');
  }
  if (errors.length) {
    return res.status(400).json({ ok: false, error: errors.join('; ') });
  }

  const r = readQueueFile();
  if (r.missing) {
    return res.status(400).json({
      ok: false,
      error: 'no review queue yet — run python scripts/curator.py first (review_queue.json is missing)',
    });
  }
  if (r.error) return res.status(400).json({ ok: false, error: r.error });

  const queue = r.queue;
  const entry = queue.flagged.find(
    (e) => e && typeof e === 'object' && String(e.id) === id && String(e.reason) === reason,
  );
  if (!entry) {
    return res.status(400).json({
      ok: false,
      error: `no flagged entry matching id=${JSON.stringify(id)} reason=${JSON.stringify(reason)}`,
    });
  }

  entry.status = decision;
  queue.updated_at = new Date().toISOString();
  try {
    writeQueueFileAtomic(queue);
  } catch (err) {
    return res.status(500).json({ ok: false, error: `failed to write review_queue.json: ${err.message}` });
  }
  return res.status(200).json({ ok: true, queue });
});

// ---------------------------------------------------------------------------
// Static pages
// ---------------------------------------------------------------------------
app.get('/retrieve', (req, res) => res.sendFile(path.join(PUBLIC_DIR, 'retrieve.html')));
app.get('/review', (req, res) => res.sendFile(path.join(PUBLIC_DIR, 'review.html')));
app.use(express.static(PUBLIC_DIR));

// Unknown /api routes -> JSON 404 (never HTML)
app.use('/api', (req, res) => {
  res.status(404).json({ ok: false, error: `unknown endpoint: ${req.method} ${req.path}` });
});

// Error handler — always a JSON envelope
app.use((err, req, res, next) => { // eslint-disable-line no-unused-vars
  if (err && err.type === 'entity.parse.failed') {
    return res.status(400).json({ ok: false, error: 'invalid JSON body' });
  }
  if (err && err.type === 'entity.too.large') {
    return res.status(413).json({ ok: false, error: 'request body too large' });
  }
  console.error(err);
  return res.status(500).json({ ok: false, error: 'internal server error' });
});

const server = app.listen(PORT, HOST, () => {
  console.log(`dropbox UI listening on http://${HOST}:${PORT} (local only; repo root: ${REPO_ROOT})`);
  bridge.warmup(); // load the models once inside the persistent bridge child
});
server.on('error', (err) => {
  console.error(`failed to bind ${HOST}:${PORT}: ${err.message}`);
  process.exit(1);
});
for (const sig of ['SIGINT', 'SIGTERM']) {
  process.on(sig, () => {
    bridge.dispose();
    server.close(() => process.exit(0));
    setTimeout(() => process.exit(0), 500).unref();
  });
}
