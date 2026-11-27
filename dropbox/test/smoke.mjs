#!/usr/bin/env node
/**
 * Smoke test for the dropbox UI. No external deps — starts server.js on a
 * test port, then checks:
 *
 *   1. GET / and GET /retrieve serve HTML; GET /app.js serves JS.
 *   2. POST /api/ingest and /api/search reject invalid input with a
 *      well-formed {ok:false} envelope (HTTP 400).
 *   3. POST /api/upload saves a file into dropbox/inbox/ and rejects bad
 *      extensions (no DB needed).
 *   3c. Review queue UI: GET /review serves the page, GET /api/review-queue
 *      returns queue:null with no file and the pending items with a temp
 *      review_queue.json at the repo root (created for the test, removed in
 *      finally — gitignored). POST /api/review-decision validates locally
 *      (400 on bad input, 403 on foreign Origin) and then DELEGATES to the
 *      bridge `review_decision` command (locked interface — the bridge
 *      updates review_queue.json AND DB metadata.review_status): 200 ok:true
 *      when the bridge implements it, 500 ok:false error envelope when it
 *      does not (this checkout may predate the bridge command).
 *   4. Round-trip (needs bridge.py + Postgres selected via TEST_DATABASE_URL —
 *      the only accepted DB variable; the dbname must match (^|_)test(_|$)
 *      case-insensitively or the script refuses with exit 1 before starting
 *      anything; unset/unreachable -> "SKIP: no test database available
 *      (set TEST_DATABASE_URL)" and exit 0. The spawned server receives
 *      DATABASE_URL=<guarded test url>):
 *      (a) ingest sample text -> ok:true, status "inserted"
 *      (b) ingest same text again -> status "duplicate"
 *      (c) search a distinctive term -> ok:true and the item's title in results
 *      (d) response has by_method with vector/fts/graph keys
 *      With a test DB, db/schema.sql is first applied to it (idempotent —
 *      fresh test databases work as-is).
 *      If no DB/bridge is available in this environment, the round-trip is
 *      skipped and instead the error envelopes are asserted well-formed
 *      (HTTP 500, ok:false, string error) — see acceptance criteria.
 *
 * Run: node test/smoke.mjs   (env: SMOKE_PORT, default 3210)
 */
import { spawn, spawnSync } from 'node:child_process';
import net from 'node:net';
import path from 'node:path';
import fs from 'node:fs';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DROPBOX_DIR = path.resolve(__dirname, '..');
const REPO_ROOT = path.resolve(DROPBOX_DIR, '..');
const PORT = Number.parseInt(process.env.SMOKE_PORT || '3210', 10);
const HOST = '127.0.0.1';
const BASE = `http://${HOST}:${PORT}`;

// --- locked test-DB guard (same invariant as tests/test_e2e.py) --------------
// TEST_DATABASE_URL is the ONLY accepted variable — DATABASE_URL is never used
// to select the test DB. The dbname must match (^|_)test(_|$) case-insensitively
// (stricter than a substring check, so e.g. "latest" is refused); otherwise the
// script refuses with exit 1 BEFORE starting anything. If unset/unreachable it
// prints SKIP and exits 0. The spawned server gets DATABASE_URL=<test url>.
const SKIP_MSG = 'SKIP: no test database available (set TEST_DATABASE_URL)';
const TEST_DBNAME_RE = /(?:^|_)test(?:_|$)/i;

function dbNameFromUrl(url) {
  try {
    const u = new URL(url);
    return decodeURIComponent(u.pathname.replace(/^\//, '').split('/')[0] || '');
  } catch {
    return '';
  }
}

function isDbReachable(url) {
  return new Promise((resolve) => {
    let u;
    try { u = new URL(url); } catch { resolve(false); return; }
    const port = Number(u.port || 5432);
    const host = u.hostname || '127.0.0.1';
    const sock = net.connect({ host, port });
    const finish = (ok) => { try { sock.destroy(); } catch { /* ignore */ } resolve(ok); };
    sock.setTimeout(5000, () => finish(false));
    sock.once('connect', () => finish(true));
    sock.once('error', () => finish(false));
  });
}

// Apply db/schema.sql to the guarded test DB before any DB-mode assertion, so a
// FRESH test database works without a manual schema step. Runs a small python
// subprocess from the repo root (autocommit so the DDL persists). schema.sql is
// idempotent — every CREATE uses IF NOT EXISTS — so "already exists" errors are
// tolerated; anything else fails the smoke.
function applySchemaToTestDb(url) {
  const py = 'import psycopg,sys; sql=open("db/schema.sql").read(); ' +
    'c=psycopg.connect(sys.argv[1], autocommit=True); c.execute(sql); c.close()';
  const res = spawnSync(process.env.PYTHON || 'python', ['-c', py, url], { cwd: REPO_ROOT, encoding: 'utf8' });
  if (res.error) throw res.error;
  if (res.status !== 0) {
    const out = `${res.stdout || ''}${res.stderr || ''}`;
    if (/already exists/i.test(out)) {
      console.log("note - schema apply reported 'already exists' (db/schema.sql is idempotent) — continuing");
      return;
    }
    throw new Error(`failed to apply db/schema.sql to the test DB: ${out.trim()}`);
  }
  console.log('note - db/schema.sql applied to the test DB (idempotent; fresh test DBs supported)');
}

const testDbUrl = process.env.TEST_DATABASE_URL || '';
let dbUrl = null; // DATABASE_URL handed to the spawned server (guarded test DB only)
if (testDbUrl) {
  const name = dbNameFromUrl(testDbUrl);
  if (!TEST_DBNAME_RE.test(name)) {
    console.error(`REFUSING to run against database '${name}': TEST_DATABASE_URL must ` +
      'point at a test database (dbname matching (^|_)test(_|$)) — this smoke deletes knowledge_items rows.');
    process.exit(1);
  }
  dbUrl = testDbUrl;
}

let passed = 0;
let failed = 0;
const failures = [];
let ingestedNodeId = null; // cleaned up via bridge delete_item in finally

function check(name, cond, extra = '') {
  if (cond) {
    passed += 1;
    console.log(`PASS ${name}`);
  } else {
    failed += 1;
    failures.push(name);
    console.log(`FAIL ${name}${extra ? ` — ${extra}` : ''}`);
  }
}

async function waitReady(timeoutMs = 20000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const r = await fetch(`${BASE}/`);
      if (r.ok) return;
    } catch { /* not up yet */ }
    await new Promise((r) => setTimeout(r, 200));
  }
  throw new Error('server did not become ready in time');
}

async function postJson(url, body) {
  const resp = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  let json = null;
  try { json = await resp.json(); } catch { /* leave null */ }
  return { status: resp.status, json };
}

const RUN_ID = Date.now().toString(36); // unique per run -> re-runs stay idempotent
const SAMPLE_TITLE = `Smoke Test Quixotic Pangolin ${RUN_ID}`;
const SAMPLE_TEXT =
  `The quixotic pangolin of smoke-testing: a distinctive marker term (zxqwv pangolin arboretum ${RUN_ID}) ` +
  'used only by dropbox/test/smoke.mjs. It describes how a local dropbox UI ingests text into ' +
  'the Second Brain knowledge base and retrieves it again via hybrid search.';

async function main() {
  if (dbUrl && !(await isDbReachable(dbUrl))) {
    console.log(SKIP_MSG);
    console.log(`(connect to ${dbUrl.split('@').slice(-1)[0]} failed)`);
    process.exit(0);
  }

  if (dbUrl) applySchemaToTestDb(dbUrl); // fresh test DBs get the schema automatically

  // --- start the server -----------------------------------------------------
  // TEST_DATABASE_URL only: the server (and its bridge child) get
  // DATABASE_URL=<guarded test url>; a stray DATABASE_URL is stripped.
  const serverEnv = { ...process.env, HOST, PORT: String(PORT) };
  if (dbUrl) serverEnv.DATABASE_URL = dbUrl;
  else delete serverEnv.DATABASE_URL;
  const server = spawn(process.execPath, [path.join(DROPBOX_DIR, 'server.js')], {
    env: serverEnv,
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let serverLog = '';
  server.stdout.on('data', (d) => { serverLog += d.toString(); });
  server.stderr.on('data', (d) => { serverLog += d.toString(); });

  try {
    await waitReady();
    check('server boots and listens on 127.0.0.1', serverLog.includes('listening'), serverLog.trim());

    // --- 1. pages -------------------------------------------------------------
    const home = await fetch(`${BASE}/`);
    const homeBody = await home.text();
    check('GET / returns 200 HTML', home.status === 200 && /text\/html/.test(home.headers.get('content-type') || ''));
    check('GET / contains insert form', homeBody.includes('id="ingest-form"') && homeBody.includes('id="raw-text"'));

    const ret = await fetch(`${BASE}/retrieve`);
    const retBody = await ret.text();
    check('GET /retrieve returns 200 HTML', ret.status === 200 && /text\/html/.test(ret.headers.get('content-type') || ''));
    check('GET /retrieve contains search form + 4 panels',
      retBody.includes('id="search-form"') &&
      retBody.includes('id="results-vector"') && retBody.includes('id="results-fts"') &&
      retBody.includes('id="results-graph"') && retBody.includes('id="results-merged"'));

    const js = await fetch(`${BASE}/app.js`);
    check('GET /app.js returns 200 JS', js.status === 200 && /javascript/.test(js.headers.get('content-type') || ''));

    // --- 2. validation envelopes ----------------------------------------------
    const badIngest = await postJson(`${BASE}/api/ingest`, { source_type: 'text' });
    check('POST /api/ingest invalid -> 400 ok:false envelope',
      badIngest.status === 400 && badIngest.json && badIngest.json.ok === false &&
      typeof badIngest.json.error === 'string');

    const badSearch = await postJson(`${BASE}/api/search`, { query: '  ' });
    check('POST /api/search empty query -> 400 ok:false envelope',
      badSearch.status === 400 && badSearch.json && badSearch.json.ok === false &&
      typeof badSearch.json.error === 'string');

    const badMethod = await postJson(`${BASE}/api/search`, { query: 'x', methods: ['nope'] });
    check('POST /api/search bad method -> 400 ok:false envelope',
      badMethod.status === 400 && badMethod.json && badMethod.json.ok === false);

    // --- 3. upload (no DB needed) ---------------------------------------------
    const uploadResp = await fetch(`${BASE}/api/upload?filename=${encodeURIComponent('smoke note.md')}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/octet-stream' },
      body: Buffer.from('# smoke upload\nhello from smoke.mjs\n'),
    });
    const uploadJson = await uploadResp.json();
    check('POST /api/upload saves file -> ok:true with path',
      uploadResp.status === 200 && uploadJson.ok === true && typeof uploadJson.result.path === 'string');
    if (uploadJson.ok) {
      const p = uploadJson.result.path;
      check('uploaded file lands inside dropbox/inbox/ with sanitized name',
        p.startsWith(path.join(DROPBOX_DIR, 'inbox') + path.sep) &&
        !path.basename(p).includes(' ') && fs.existsSync(p) &&
        fs.readFileSync(p, 'utf8').includes('hello from smoke.mjs'));
    }
    const badUpload = await fetch(`${BASE}/api/upload?filename=evil.exe`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/octet-stream' },
      body: Buffer.from('MZ'),
    });
    const badUploadJson = await badUpload.json();
    check('POST /api/upload rejects .exe -> 400 ok:false',
      badUpload.status === 400 && badUploadJson.ok === false && typeof badUploadJson.error === 'string');

    const traversal = await fetch(`${BASE}/api/upload?filename=${encodeURIComponent('..\\..\\evil.txt')}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/octet-stream' },
      body: Buffer.from('x'),
    });
    const travJson = await traversal.json();
    check('POST /api/upload path-traversal name stays inside inbox/',
      traversal.status === 200 && travJson.ok === true &&
      travJson.result.path.startsWith(path.join(DROPBOX_DIR, 'inbox') + path.sep));

    // --- 3b. CSRF guard on mutating routes (no DB needed) ---------------------
    const evilUpload = await fetch(`${BASE}/api/upload?filename=csrf-evil.md`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/octet-stream', Origin: 'http://evil.example' },
      body: Buffer.from('x'),
    });
    const evilUploadJson = await evilUpload.json();
    check('POST /api/upload with foreign Origin -> 403 ok:false',
      evilUpload.status === 403 && evilUploadJson.ok === false &&
      typeof evilUploadJson.error === 'string',
      `status=${evilUpload.status} body=${JSON.stringify(evilUploadJson).slice(0, 200)}`);

    const goodUpload = await fetch(`${BASE}/api/upload?filename=csrf-ok.md`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/octet-stream', Origin: BASE },
      body: Buffer.from('# csrf same-origin upload\n'),
    });
    const goodUploadJson = await goodUpload.json();
    check('POST /api/upload with same-origin Origin -> 200 ok:true',
      goodUpload.status === 200 && goodUploadJson.ok === true &&
      typeof goodUploadJson.result.path === 'string',
      `status=${goodUpload.status} body=${JSON.stringify(goodUploadJson).slice(0, 200)}`);

    const evilIngest = await fetch(`${BASE}/api/ingest`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Origin: 'https://attacker.example' },
      body: JSON.stringify({ source_type: 'text', payload: 'csrf probe' }),
    });
    const evilIngestJson = await evilIngest.json();
    check('POST /api/ingest with foreign Origin -> 403 ok:false',
      evilIngest.status === 403 && evilIngestJson.ok === false,
      `status=${evilIngest.status} body=${JSON.stringify(evilIngestJson).slice(0, 200)}`);

    const crossSiteSfs = await fetch(`${BASE}/api/search`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Sec-Fetch-Site': 'cross-site' },
      body: JSON.stringify({ query: 'csrf probe' }),
    });
    const crossSiteSfsJson = await crossSiteSfs.json();
    check('POST /api/search with Sec-Fetch-Site: cross-site (no Origin) -> 403 ok:false',
      crossSiteSfs.status === 403 && crossSiteSfsJson.ok === false,
      `status=${crossSiteSfs.status} body=${JSON.stringify(crossSiteSfsJson).slice(0, 200)}`);

    const noOriginIngest = await fetch(`${BASE}/api/ingest`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' }, // no Origin, no Sec-Fetch-Site (curl/local)
      body: JSON.stringify({ source_type: 'text' }),
    });
    const noOriginJson = await noOriginIngest.json();
    check('POST /api/ingest without Origin (local tool) still reaches validation -> 400',
      noOriginIngest.status === 400 && noOriginJson.ok === false,
      `status=${noOriginIngest.status} body=${JSON.stringify(noOriginJson).slice(0, 200)}`);

    // --- 3c. review queue page + decision API (no DB needed) -------------------
    const reviewPage = await fetch(`${BASE}/review`);
    const reviewBody = await reviewPage.text();
    check('GET /review returns 200 HTML with id="review-queue"',
      reviewPage.status === 200 &&
      /text\/html/.test(reviewPage.headers.get('content-type') || '') &&
      reviewBody.includes('id="review-queue"'),
      `status=${reviewPage.status}`);

    // The queue path is fixed at the repo root (gitignored runtime artifact).
    // A pre-existing real queue is backed up and restored byte-identical in
    // finally; otherwise the temp file created here is deleted in finally.
    const QUEUE_PATH = path.join(REPO_ROOT, 'review_queue.json');
    const preexistingQueue = fs.existsSync(QUEUE_PATH) ? fs.readFileSync(QUEUE_PATH) : null;
    let wroteTempQueue = false;
    // The bridge review_decision command keys entries by (id, reason) and
    // requires id to be a UUID — use UUIDs so the 200-path works when a test
    // DB is available.
    const SMOKE_FLAG = {
      id: crypto.randomUUID(),
      title: 'Smoke Flag Item',
      source_uri: 'smoke://flag',
      reason: 'stale',
      evidence: 'smoke evidence: update_period exceeded (dropbox/test/smoke.mjs)',
      status: 'pending',
    };
    const SMOKE_FLAG2 = {
      id: crypto.randomUUID(),
      title: 'Smoke Flag Item 2',
      source_uri: 'smoke://flag2',
      reason: 'isolated',
      evidence: 'zero relationships',
      status: 'pending',
    };
    try {
      if (preexistingQueue === null) {
        const noQueue = await fetch(`${BASE}/api/review-queue`);
        const noQueueJson = await noQueue.json();
        check('GET /api/review-queue with no queue file -> 200 ok:true queue:null',
          noQueue.status === 200 && noQueueJson.ok === true && noQueueJson.queue === null,
          `status=${noQueue.status} body=${JSON.stringify(noQueueJson).slice(0, 200)}`);
      }

      const tempQueue = { updated_at: new Date().toISOString(), flagged: [SMOKE_FLAG, SMOKE_FLAG2] };
      fs.writeFileSync(QUEUE_PATH, JSON.stringify(tempQueue, null, 2) + '\n');
      wroteTempQueue = true;

      const q1 = await fetch(`${BASE}/api/review-queue`);
      const q1Json = await q1.json();
      check('GET /api/review-queue with temp queue -> ok:true, pending item listed',
        q1.status === 200 && q1Json.ok === true && q1Json.queue && Array.isArray(q1Json.queue.flagged) &&
        q1Json.queue.flagged.some((e) => e && e.id === SMOKE_FLAG.id && e.status === 'pending'),
        `status=${q1.status} body=${JSON.stringify(q1Json).slice(0, 300)}`);

      const badDecision = await postJson(`${BASE}/api/review-decision`,
        { id: SMOKE_FLAG.id, reason: 'stale', decision: 'maybe' });
      check('POST /api/review-decision bad decision -> 400 ok:false envelope (local validation)',
        badDecision.status === 400 && badDecision.json && badDecision.json.ok === false &&
        typeof badDecision.json.error === 'string',
        `status=${badDecision.status} body=${JSON.stringify(badDecision.json).slice(0, 200)}`);

      const emptyId = await postJson(`${BASE}/api/review-decision`,
        { id: '  ', reason: 'stale', decision: 'approved' });
      check('POST /api/review-decision blank id -> 400 ok:false envelope (local validation)',
        emptyId.status === 400 && emptyId.json && emptyId.json.ok === false &&
        typeof emptyId.json.error === 'string',
        `status=${emptyId.status} body=${JSON.stringify(emptyId.json).slice(0, 200)}`);

      fs.writeFileSync(QUEUE_PATH, '{not valid json');
      const corruptGet = await fetch(`${BASE}/api/review-queue`);
      const corruptGetJson = await corruptGet.json();
      check('GET /api/review-queue with corrupt queue -> 500 ok:false envelope',
        corruptGet.status === 500 && corruptGetJson.ok === false &&
        typeof corruptGetJson.error === 'string',
        `status=${corruptGet.status} body=${JSON.stringify(corruptGetJson).slice(0, 200)}`);
      const corrupt = await postJson(`${BASE}/api/review-decision`,
        { id: SMOKE_FLAG.id, reason: 'stale', decision: 'approved' });
      check('POST /api/review-decision with corrupt queue -> well-formed error envelope, never 200',
        corrupt.status !== 200 && corrupt.json && corrupt.json.ok === false &&
        typeof corrupt.json.error === 'string',
        `status=${corrupt.status} body=${JSON.stringify(corrupt.json).slice(0, 200)}`);
      fs.writeFileSync(QUEUE_PATH, JSON.stringify(tempQueue, null, 2) + '\n'); // restore temp for the decision legs

      // The endpoint delegates to the bridge `review_decision` command (locked
      // interface) and returns its envelope verbatim — it never writes the
      // queue file itself. When the bridge implements the command: 200 ok:true
      // and the bridge updates review_queue.json. When it does not (yet):
      // 500 with a well-formed ok:false error envelope. Both are valid.
      const approve = await postJson(`${BASE}/api/review-decision`,
        { id: SMOKE_FLAG.id, reason: 'stale', decision: 'approved' });
      if (approve.status === 200 && approve.json && approve.json.ok === true) {
        check('POST /api/review-decision approve -> 200 ok:true via bridge review_decision', true);
        const fileAfter = JSON.parse(fs.readFileSync(QUEUE_PATH, 'utf8'));
        const fileEntry = Array.isArray(fileAfter.flagged)
          ? fileAfter.flagged.find((e) => e && e.id === SMOKE_FLAG.id)
          : null;
        check('bridge persisted the decision to review_queue.json (status approved)',
          !!fileEntry && fileEntry.status === 'approved',
          JSON.stringify(fileAfter).slice(0, 300));

        const q2 = await fetch(`${BASE}/api/review-queue`);
        const q2Json = await q2.json();
        const persistedEntry = q2Json && q2Json.queue && Array.isArray(q2Json.queue.flagged)
          ? q2Json.queue.flagged.find((e) => e && e.id === SMOKE_FLAG.id)
          : null;
        check('GET /api/review-queue shows the approved status',
          q2.status === 200 && q2Json.ok === true && persistedEntry && persistedEntry.status === 'approved',
          `status=${q2.status} body=${JSON.stringify(q2Json).slice(0, 300)}`);

        const dismiss = await postJson(`${BASE}/api/review-decision`,
          { id: SMOKE_FLAG2.id, reason: 'isolated', decision: 'dismissed' });
        check('POST /api/review-decision dismiss (keyed by id+reason) -> 200 ok:true via bridge',
          dismiss.status === 200 && dismiss.json && dismiss.json.ok === true,
          `status=${dismiss.status} body=${JSON.stringify(dismiss.json).slice(0, 300)}`);
      } else {
        check('POST /api/review-decision approve -> well-formed delegation envelope ' +
          '(200 ok:true via bridge review_decision, or 500 ok:false when the bridge lacks the command)',
          (approve.status === 200 && approve.json && approve.json.ok === true) ||
          (approve.status === 500 && approve.json && approve.json.ok === false &&
            typeof approve.json.error === 'string'),
          `status=${approve.status} body=${JSON.stringify(approve.json).slice(0, 300)}`);
        console.log('note - bridge returned an error envelope for review_decision ' +
          `(status=${approve.status}, body=${JSON.stringify(approve.json).slice(0, 300)}) — ` +
          'the HTTP layer delegated correctly and never wrote the queue itself');
      }

      const evilDecision = await fetch(`${BASE}/api/review-decision`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Origin: 'http://evil.example' },
        body: JSON.stringify({ id: SMOKE_FLAG.id, reason: 'stale', decision: 'approved' }),
      });
      const evilDecisionJson = await evilDecision.json();
      check('POST /api/review-decision with foreign Origin -> 403 ok:false',
        evilDecision.status === 403 && evilDecisionJson.ok === false &&
        typeof evilDecisionJson.error === 'string',
        `status=${evilDecision.status} body=${JSON.stringify(evilDecisionJson).slice(0, 200)}`);
    } finally {
      if (wroteTempQueue) {
        try { fs.unlinkSync(QUEUE_PATH); } catch { /* already gone */ }
      }
      if (preexistingQueue !== null) {
        fs.writeFileSync(QUEUE_PATH, preexistingQueue); // restore byte-identical
      }
    }

    // --- 4. round-trip (needs bridge.py + DATABASE_URL) ------------------------
    const ingest1 = await postJson(`${BASE}/api/ingest`, {
      source_type: 'text', payload: SAMPLE_TEXT, source_uri: null, title: SAMPLE_TITLE,
    });

    if (ingest1.status === 200 && ingest1.json && ingest1.json.ok === true) {
      console.log('DB mode: bridge + Postgres reachable, running full round-trip assertions.');
      const r1 = ingest1.json.result || {};
      check('(a) ingest sample text -> ok:true, status inserted',
        r1.status === 'inserted' && Array.isArray(r1.node_ids) && r1.node_ids.length > 0 &&
        typeof r1.chunks === 'number' && r1.chunks >= 1,
        JSON.stringify(r1).slice(0, 300));
      if (r1.status === 'inserted' && Array.isArray(r1.node_ids)) ingestedNodeId = r1.node_ids[0];

      const ingest2 = await postJson(`${BASE}/api/ingest`, {
        source_type: 'text', payload: SAMPLE_TEXT, source_uri: null, title: SAMPLE_TITLE,
      });
      check('(b) same text again -> status duplicate',
        ingest2.status === 200 && ingest2.json.ok === true &&
        (ingest2.json.result || {}).status === 'duplicate',
        JSON.stringify(ingest2.json).slice(0, 300));

      const search = await postJson(`${BASE}/api/search`, {
        query: 'quixotic pangolin zxqwv arboretum', methods: ['vector', 'fts', 'graph'], hops: 1, top_k: 10,
      });
      check('(c) search distinctive term -> ok:true',
        search.status === 200 && search.json.ok === true && Array.isArray((search.json.result || {}).results),
        JSON.stringify(search.json).slice(0, 300));
      if (search.json && search.json.ok) {
        const titles = (search.json.result.results || []).map((n) => n.title || '');
        check('(c) inserted item title appears in merged results', titles.includes(SAMPLE_TITLE),
          `titles: ${JSON.stringify(titles)}`);
        const byMethod = search.json.result.by_method || {};
        check('(d) by_method has vector/fts/graph keys',
          ['vector', 'fts', 'graph'].every((k) => Array.isArray(byMethod[k])),
          `keys: ${JSON.stringify(Object.keys(byMethod))}`);
      } else {
        check('(c) inserted item title appears in merged results', false, 'search failed');
        check('(d) by_method has vector/fts/graph keys', false, 'search failed');
      }
    } else {
      console.log('No-DB mode: no test DB configured (TEST_DATABASE_URL unset/unreachable).');
      console.log('Round-trip (inserted/duplicate/search/by_method) requires a live Postgres — skipped here.');
      check('ingest without DB -> HTTP 500 with well-formed ok:false envelope',
        ingest1.status === 500 && ingest1.json && ingest1.json.ok === false &&
        typeof ingest1.json.error === 'string' && ingest1.json.error.length > 0,
        `status=${ingest1.status} body=${JSON.stringify(ingest1.json).slice(0, 300)}`);

      const search = await postJson(`${BASE}/api/search`, {
        query: 'quixotic pangolin', methods: ['vector', 'fts', 'graph'], hops: 1, top_k: 10,
      });
      check('search without DB -> HTTP 500 with well-formed ok:false envelope',
        search.status === 500 && search.json && search.json.ok === false &&
        typeof search.json.error === 'string' && search.json.error.length > 0,
        `status=${search.status} body=${JSON.stringify(search.json).slice(0, 300)}`);
    }
  } catch (err) {
    failed += 1;
    failures.push(`fatal: ${err.message}`);
    console.error(`FATAL ${err.message}`);
    console.error(serverLog);
  } finally {
    // Remove the smoke-test item so re-runs against the same DB stay idempotent
    // (semantic dedup would otherwise flag the near-identical sample text).
    if (ingestedNodeId) {
      try {
        const delEnv = { ...process.env };
        if (dbUrl) delEnv.DATABASE_URL = dbUrl;
        else delete delEnv.DATABASE_URL;
        spawnSync(process.env.PYTHON || 'python',
          ['-m', 'bridge', 'delete_item', JSON.stringify({ item_id: ingestedNodeId })],
          { cwd: REPO_ROOT, stdio: 'ignore', env: delEnv });
      } catch { /* best effort */ }
    }
    server.kill('SIGTERM');
  }

  console.log(`\n${passed} passed, ${failed} failed`);
  if (failures.length) console.log(`failures: ${failures.join('; ')}`);
  process.exit(failed === 0 ? 0 : 1);
}

main();
