/**
 * Smoke test for the Second Brain MCP server.
 *
 * Spawns the built server (`node dist/index.js` — exactly what `npm start`
 * runs) over stdio, speaks newline-delimited
 * JSON-RPC (the MCP stdio framing), and asserts:
 *
 *   - initialize + notifications/initialized + tools/list return well-formed
 *     JSON-RPC responses
 *   - tools/list contains all 8 tools: search, add_knowledge, get_related,
 *     link_items, update_item, delete_item, list_recent, dream
 *   - tools/call works for each of the 8 tools (with a test DB, dream runs the
 *     bridge curator pass and returns the structured report; without a DB,
 *     dream returns isError without killing the server)
 *   - an erroring call (search with empty query, delete_item with a random
 *     uuid) returns isError:true without killing the server (a follow-up
 *     list_recent still succeeds)
 *   - fast-failure cases (no DB needed): with PYTHON=/nonexistent the server
 *     still answers initialize immediately (transport connects before the
 *     background warm-up) and tool calls fail fast as isError; a child that
 *     dies mid-request has its pending JSON-RPC requests REJECTED on
 *     exit/close instead of hanging until the ~240 s request timeout
 *
 * Guard pattern (locked — same invariant as tests/test_e2e.py): the DB is
 * selected from TEST_DATABASE_URL ONLY (never DATABASE_URL). The dbname must
 * match (^|_)test(_|$) case-insensitively, otherwise the script refuses with
 * exit 1 BEFORE spawning anything; if TEST_DATABASE_URL is unset/unreachable
 * it prints "SKIP: no test database available (set TEST_DATABASE_URL)" and
 * exits 0. The spawned server receives DATABASE_URL=<guarded test url>.
 *
 * With a test DB, db/schema.sql is first applied to it (idempotent — fresh test
 * databases work as-is), then the DB-backed tools are additionally exercised
 * end-to-end (add_knowledge -> get_related -> link_items -> update_item ->
 * delete_item round-trip, cleaned up afterwards). Without a DB those calls return
 * isError, which is still a well-formed response; the script notes that DB
 * checks were skipped.
 *
 * Run: node test/smoke.mjs   (after `npm install && npm run build`)
 */

import { spawn, execSync, spawnSync } from "node:child_process";
import net from "node:net";
import * as path from "node:path";
import * as crypto from "node:crypto";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const serverDir = path.resolve(here, "..");
const isWin = process.platform === "win32";

// --- locked test-DB guard (same invariant as tests/test_e2e.py) --------------
// TEST_DATABASE_URL is the ONLY accepted variable — DATABASE_URL is never used
// to select the test DB. The dbname must match (^|_)test(_|$) case-insensitively
// (stricter than a substring check, so e.g. "latest" is refused); otherwise the
// script refuses with exit 1 BEFORE spawning anything. If unset/unreachable it
// prints SKIP and exits 0. The spawned server gets DATABASE_URL=<test url>.
const SKIP_MSG = "SKIP: no test database available (set TEST_DATABASE_URL)";
const TEST_DBNAME_RE = /(?:^|_)test(?:_|$)/i;

function dbNameFromUrl(url) {
  try {
    const u = new URL(url);
    return decodeURIComponent(u.pathname.replace(/^\//, "").split("/")[0] || "");
  } catch {
    return "";
  }
}

function isDbReachable(url) {
  return new Promise((resolve) => {
    let u;
    try { u = new URL(url); } catch { resolve(false); return; }
    const port = Number(u.port || 5432);
    const host = u.hostname || "127.0.0.1";
    const sock = net.connect({ host, port });
    const finish = (ok) => { try { sock.destroy(); } catch { /* ignore */ } resolve(ok); };
    sock.setTimeout(5000, () => finish(false));
    sock.once("connect", () => finish(true));
    sock.once("error", () => finish(false));
  });
}

// Apply db/schema.sql to the guarded test DB before any DB-mode assertion, so a
// FRESH test database works without a manual schema step. Runs a small python
// subprocess from the repo root (autocommit so the DDL persists). schema.sql is
// idempotent — every CREATE uses IF NOT EXISTS — so "already exists" errors are
// tolerated; anything else fails the smoke.
function applySchemaToTestDb(url) {
  const repoRoot = path.resolve(serverDir, "..");
  const py = 'import psycopg,sys; sql=open("db/schema.sql").read(); ' +
    'c=psycopg.connect(sys.argv[1], autocommit=True); c.execute(sql); c.close()';
  const res = spawnSync(process.env.PYTHON || "python", ["-c", py, url], { cwd: repoRoot, encoding: "utf8" });
  if (res.error) throw res.error;
  if (res.status !== 0) {
    const out = `${res.stdout || ""}${res.stderr || ""}`;
    if (/already exists/i.test(out)) {
      console.log("note - schema apply reported 'already exists' (db/schema.sql is idempotent) — continuing");
      return;
    }
    throw new Error(`failed to apply db/schema.sql to the test DB: ${out.trim()}`);
  }
  console.log("note - db/schema.sql applied to the test DB (idempotent; fresh test DBs supported)");
}

const testDbUrl = process.env.TEST_DATABASE_URL || "";
let dbUrl = null; // DATABASE_URL handed to the spawned server (guarded test DB only)
if (testDbUrl) {
  const name = dbNameFromUrl(testDbUrl);
  if (!TEST_DBNAME_RE.test(name)) {
    console.error(`REFUSING to run against database '${name}': TEST_DATABASE_URL must ` +
      "point at a test database (dbname matching (^|_)test(_|$)) — this smoke deletes knowledge_items rows.");
    process.exit(1);
  }
  dbUrl = testDbUrl;
}

const childEnv = { ...process.env };
if (dbUrl) childEnv.DATABASE_URL = dbUrl;
else delete childEnv.DATABASE_URL; // never let a stray DATABASE_URL select the DB

let passed = 0;
function assert(cond, msg) {
  if (!cond) {
    console.error(`FAIL: ${msg}`);
    process.exitCode = 1;
    throw new Error(msg);
  }
  passed++;
  console.log(`ok - ${msg}`);
}

// Spawn the built server directly with node — no npm, no shell on any platform.
// (Spawning npm.cmd with shell:false is impossible on modern Node: batch files
// require a shell since the CVE-2024-27980 fix and throw EINVAL; shell:true is
// what triggers DEP0190. `npm start` is just `node dist/index.js`.)
const serverEntry = path.join(serverDir, "dist", "index.js");
const child = spawn(process.execPath, [serverEntry], {
  cwd: serverDir,
  shell: false,
  env: childEnv,
  stdio: ["pipe", "pipe", "pipe"],
});

let buffer = "";
const pending = new Map();
let nextId = 1;
const REQUEST_TIMEOUT_MS = 240_000;

child.stdout.on("data", (chunk) => {
  buffer += chunk.toString("utf8");
  let idx;
  while ((idx = buffer.indexOf("\n")) >= 0) {
    const line = buffer.slice(0, idx).trim();
    buffer = buffer.slice(idx + 1);
    if (!line) continue;
    let msg;
    try {
      msg = JSON.parse(line);
    } catch {
      continue; // npm banner or any non-JSON noise
    }
    if (msg && msg.id !== undefined && pending.has(msg.id)) {
      const { resolve, timer } = pending.get(msg.id);
      clearTimeout(timer);
      pending.delete(msg.id);
      resolve(msg);
    }
  }
});

child.stderr.on("data", (d) => {
  const s = d.toString("utf8");
  if (s.trim()) console.error(`[server stderr] ${s.trim()}`);
});

// A crashed/exited server child must REJECT all pending JSON-RPC requests
// immediately — otherwise a dead child hangs the smoke for the full
// REQUEST_TIMEOUT_MS (~240 s) per pending request.
function rejectAllPending(reason) {
  if (pending.size === 0) return;
  console.error(`[smoke] ${reason} — rejecting ${pending.size} pending request(s)`);
  for (const [, p] of pending) {
    clearTimeout(p.timer);
    p.reject(new Error(reason));
  }
  pending.clear();
}

child.on("exit", (code) => {
  console.error(`[smoke] server process exited with code ${code}`);
  rejectAllPending(`server process exited (code ${code})`);
});

child.on("close", (code, signal) => {
  rejectAllPending(`server process closed (code ${code}, signal ${signal})`);
});

function send(obj) {
  child.stdin.write(JSON.stringify(obj) + "\n");
}

function request(method, params) {
  const id = nextId++;
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      pending.delete(id);
      reject(new Error(`timeout waiting for response to ${method}`));
    }, REQUEST_TIMEOUT_MS);
    pending.set(id, { resolve, timer });
    send({ jsonrpc: "2.0", id, method, params });
  });
}

/** Call a tool; returns { result, error } from the JSON-RPC response. */
async function callTool(name, args) {
  const res = await request("tools/call", { name, arguments: args });
  assert(res.jsonrpc === "2.0" && res.id !== undefined, `tools/call ${name}: well-formed JSON-RPC response`);
  return res;
}

/** Assert a tools/call response is a well-formed MCP tool result. */
function assertToolResult(res, name) {
  const r = res.result;
  assert(r && typeof r === "object", `tools/call ${name}: has result object`);
  assert(Array.isArray(r.content) && r.content.length > 0, `tools/call ${name}: content is a non-empty array`);
  assert(r.content[0].type === "text" && typeof r.content[0].text === "string", `tools/call ${name}: first content block is text`);
  return r;
}

function parseText(r) {
  return JSON.parse(r.content[0].text);
}

async function main() {
  // --- fast-failure cases (no DB needed) --------------------------------------
  // (a) PYTHON=/nonexistent: the server must still answer initialize quickly
  //     (the transport connects before the background bridge warm-up) and a
  //     tool call must come back fast as isError — never a 240 s hang.
  {
    const badEnv = { ...childEnv, PYTHON: "/nonexistent/python-w4-smoke" };
    const bad = spawn(process.execPath, [serverEntry], {
      cwd: serverDir, shell: false, env: badEnv, stdio: ["pipe", "pipe", "pipe"],
    });
    bad.stdin.on("error", () => { /* child died mid-write — the exit handler rejects pending */ });
    let badBuf = "";
    const badPending = new Map();
    let badNextId = 1;
    bad.stdout.on("data", (chunk) => {
      badBuf += chunk.toString("utf8");
      let idx;
      while ((idx = badBuf.indexOf("\n")) >= 0) {
        const line = badBuf.slice(0, idx).trim();
        badBuf = badBuf.slice(idx + 1);
        if (!line) continue;
        let msg;
        try { msg = JSON.parse(line); } catch { continue; }
        if (msg && msg.id !== undefined && badPending.has(msg.id)) {
          const { resolve, timer } = badPending.get(msg.id);
          clearTimeout(timer);
          badPending.delete(msg.id);
          resolve(msg);
        }
      }
    });
    bad.on("exit", (code) => {
      for (const [, p] of badPending) { clearTimeout(p.timer); p.reject(new Error(`server exited (code ${code})`)); }
      badPending.clear();
    });
    bad.on("close", (code, signal) => {
      for (const [, p] of badPending) { clearTimeout(p.timer); p.reject(new Error(`server closed (code ${code}, signal ${signal})`)); }
      badPending.clear();
    });
    const badRequest = (method, params) => {
      const id = badNextId++;
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
          badPending.delete(id);
          reject(new Error(`timeout waiting for response to ${method}`));
        }, REQUEST_TIMEOUT_MS);
        badPending.set(id, { resolve, timer });
        bad.stdin.write(JSON.stringify({ jsonrpc: "2.0", id, method, params }) + "\n");
      });
    };
    try {
      const t0 = Date.now();
      const init = await badRequest("initialize", {
        protocolVersion: "2025-06-18",
        capabilities: {},
        clientInfo: { name: "smoke-bad-python", version: "0.1.0" },
      });
      const initMs = Date.now() - t0;
      assert(init.result && init.result.protocolVersion,
        "PYTHON=/nonexistent: initialize still answered (transport connects before warm-up)");
      assert(initMs < 30_000,
        `PYTHON=/nonexistent: initialize answered fast (${initMs} ms < 30000 ms — not blocked on model loading)`);
      bad.stdin.write(JSON.stringify({ jsonrpc: "2.0", method: "notifications/initialized" }) + "\n");
      const t1 = Date.now();
      const res = await badRequest("tools/call", { name: "search", arguments: { query: "fast failure probe" } });
      const callMs = Date.now() - t1;
      assert(res.result && res.result.isError === true,
        "PYTHON=/nonexistent: tools/call returns isError (bridge unspawnable) instead of hanging");
      assert(callMs < 60_000,
        `PYTHON=/nonexistent: tool call failed fast (${callMs} ms < 60000 ms — not the 240 s timeout)`);
    } finally {
      try {
        bad.kill();
        if (isWin && bad.pid) {
          try { execSync(`taskkill /pid ${bad.pid} /T /F`, { stdio: "ignore" }); } catch { /* already gone */ }
        }
      } catch { /* ignore */ }
    }
  }

  // (b) a child that dies mid-request: the pending JSON-RPC request must be
  //     REJECTED immediately on exit/close — not resolved by the 240 s timer.
  {
    const dying = spawn(process.execPath, ["-e", "setTimeout(() => process.exit(7), 150)"], {
      cwd: serverDir, shell: false, stdio: ["pipe", "pipe", "pipe"],
    });
    dying.stdin.on("error", () => { /* already dead — the exit handler rejects pending */ });
    const dPending = new Map();
    const dRequest = (method) => new Promise((resolve, reject) => {
      const id = 1;
      const timer = setTimeout(() => {
        dPending.delete(id);
        reject(new Error(`timeout waiting for response to ${method}`));
      }, REQUEST_TIMEOUT_MS);
      dPending.set(id, { resolve, reject, timer });
      dying.stdin.write(JSON.stringify({ jsonrpc: "2.0", id, method, params: {} }) + "\n");
    });
    const onDead = (why) => {
      for (const [, p] of dPending) { clearTimeout(p.timer); p.reject(new Error(why)); }
      dPending.clear();
    };
    dying.on("exit", (code) => onDead(`server process exited (code ${code})`));
    dying.on("close", (code, signal) => onDead(`server process closed (code ${code}, signal ${signal})`));
    const t0 = Date.now();
    let rejected = null;
    try {
      await dRequest("initialize");
    } catch (e) {
      rejected = e;
    }
    const ms = Date.now() - t0;
    assert(rejected !== null, "crashed child: pending request REJECTED (not left hanging)");
    assert(/exited|closed/.test(String(rejected && rejected.message)),
      `crashed child: rejection names the exit (${rejected && rejected.message})`);
    assert(ms < 30_000,
      `crashed child: rejection is fast (${ms} ms < 30000 ms — not the 240 s timeout)`);
    assert(dPending.size === 0, "crashed child: pending map drained on exit/close");
  }

  if (dbUrl && !(await isDbReachable(dbUrl))) {
    console.log(SKIP_MSG);
    console.log(`(connect to ${dbUrl.split("@").slice(-1)[0]} failed)`);
    return; // .finally kills the child and exits 0
  }
  const hasDb = Boolean(dbUrl);
  if (hasDb) applySchemaToTestDb(dbUrl); // fresh test DBs get the schema automatically
  if (!hasDb) {
    console.log("NOTE: TEST_DATABASE_URL is not set — DB-backed tool calls (incl. dream) will " +
      "return isError; tools/list and error-path checks still run fully.");
  }

  // --- initialize -----------------------------------------------------------
  const init = await request("initialize", {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "smoke-test", version: "0.1.0" },
  });
  assert(init.result && init.result.protocolVersion, "initialize: result with protocolVersion");
  assert(init.result.serverInfo && init.result.serverInfo.name === "secondbrain", "initialize: serverInfo.name == secondbrain");
  send({ jsonrpc: "2.0", method: "notifications/initialized" });

  // --- tools/list -------------------------------------------------------------
  const list = await request("tools/list", {});
  assert(list.result && Array.isArray(list.result.tools), "tools/list: result.tools is an array");
  const names = list.result.tools.map((t) => t.name).sort();
  const expected = ["add_knowledge", "delete_item", "dream", "get_related", "link_items", "list_recent", "search", "update_item"];
  assert(
    expected.every((n) => names.includes(n)),
    `tools/list contains all 8 tools (got: ${names.join(", ")})`,
  );
  for (const t of list.result.tools) {
    assert(typeof t.description === "string" && t.description.length > 0, `tool ${t.name}: has description`);
    assert(t.inputSchema && typeof t.inputSchema === "object", `tool ${t.name}: has inputSchema`);
  }

  // add_knowledge source_type enum must cover every bridge source_type.
  const addKnowledgeTool = list.result.tools.find((t) => t.name === "add_knowledge");
  const stEnum = addKnowledgeTool?.inputSchema?.properties?.source_type?.enum;
  const expectedSourceTypes = ["text", "url", "pdf", "image", "youtube", "audio", "github"];
  assert(
    Array.isArray(stEnum) && expectedSourceTypes.every((s) => stEnum.includes(s)),
    `add_knowledge source_type enum covers all 7 bridge source types (got: ${JSON.stringify(stEnum)})`,
  );

  // --- dream: Phase 4 — bridge 'curator' consolidation report -------------------
  // With a test DB: ok (not isError) and content[0].text parses to the structured
  // curator report. Without a DB: the bridge curator call fails (no DATABASE_URL),
  // so dream returns isError — but the server must stay alive (checked below).
  const dreamRes = await callTool("dream", {});
  const dreamResult = assertToolResult(dreamRes, "dream");
  if (hasDb) {
    assert(!dreamResult.isError, "dream: ok with a test DB (runs the bridge curator pass)");
    const dreamPayload = parseText(dreamResult);
    assert(
      dreamPayload && dreamPayload.report &&
        typeof dreamPayload.report.stale === "number" &&
        typeof dreamPayload.report.contradictions === "number" &&
        typeof dreamPayload.report.isolated === "number" &&
        typeof dreamPayload.report.queue_written === "boolean" &&
        (dreamPayload.report.queue_path === null || typeof dreamPayload.report.queue_path === "string"),
      "dream: returns the curator report { report: { stale, contradictions, isolated, queue_written, queue_path } }",
    );
  } else {
    assert(dreamResult.isError === true, "dream without a test DB: isError (bridge curator fails) — server must not crash");
    console.log("NOTE: dream returned isError because no test DB is configured (TEST_DATABASE_URL unset/unreachable) (expected)");
  }

  // --- list_recent --------------------------------------------------------------
  const recentRes = await callTool("list_recent", { limit: 5 });
  const recentResult = assertToolResult(recentRes, "list_recent");
  if (hasDb) {
    assert(!recentResult.isError, "list_recent: ok with a test DB (TEST_DATABASE_URL)");
    const rows = parseText(recentResult);
    assert(Array.isArray(rows) && rows.length <= 5, "list_recent: result parses to an array of rows");
  } else {
    console.log("NOTE: list_recent returned isError because no test DB is configured (TEST_DATABASE_URL unset/unreachable) (expected)");
  }

  // --- search -------------------------------------------------------------------
  const searchRes = await callTool("search", { query: "hybrid retrieval vector graph full-text" });
  const searchResult = assertToolResult(searchRes, "search");
  if (hasDb) {
    assert(!searchResult.isError, "search: ok with a test DB (TEST_DATABASE_URL)");
    const payload = parseText(searchResult);
    assert(Array.isArray(payload.results) && payload.by_method, "search: result has { results, by_method }");
  } else {
    console.log("NOTE: search returned isError because no test DB is configured (TEST_DATABASE_URL unset/unreachable) (expected)");
  }

  // --- DB round-trip via the remaining tools (only when a DB is available) ------
  let itemA = null;
  let itemB = null;
  if (hasDb) {
    const uniq = crypto.randomUUID().slice(0, 8);

    const addA = assertToolResult(await callTool("add_knowledge", {
      source_type: "text",
      payload: `MCP smoke test note alpha ${uniq}: the Second Brain merges vector, FTS and graph retrieval.`,
      title: `mcp smoke alpha ${uniq}`,
    }), "add_knowledge (A)");
    assert(!addA.isError, "add_knowledge (A): ok");
    const resA = parseText(addA);
    assert(resA.status === "inserted" && Array.isArray(resA.node_ids) && resA.node_ids.length > 0, "add_knowledge (A): inserted with node_ids");
    itemA = resA.node_ids[0];

    const addB = assertToolResult(await callTool("add_knowledge", {
      source_type: "text",
      payload: `MCP smoke test note beta ${uniq}: graph traversal expands from vector seeds.`,
      title: `mcp smoke beta ${uniq}`,
    }), "add_knowledge (B)");
    assert(!addB.isError, "add_knowledge (B): ok");
    itemB = parseText(addB).node_ids[0];

    const rel = assertToolResult(await callTool("get_related", { item_id: itemA, hops: 1 }), "get_related");
    assert(!rel.isError, "get_related: ok");
    const relPayload = parseText(rel);
    assert(relPayload.item && relPayload.item.id === itemA && Array.isArray(relPayload.related), "get_related: { item, related } shape");

    const link = assertToolResult(await callTool("link_items", {
      from_id: itemA, to_id: itemB, type: "related", strength: 0.9, direction: "both", context: `mcp smoke link ${uniq}`,
    }), "link_items");
    assert(!link.isError, "link_items: ok");
    const linkRow = parseText(link);
    assert(linkRow.from_id === itemA && linkRow.to_id === itemB && linkRow.type === "related", "link_items: created row matches");

    const upd = assertToolResult(await callTool("update_item", { item_id: itemA, title: `mcp smoke alpha UPDATED ${uniq}` }), "update_item");
    assert(!upd.isError, "update_item: ok");
    assert(parseText(upd).title === `mcp smoke alpha UPDATED ${uniq}`, "update_item: updated row returned");

    const rel2 = assertToolResult(await callTool("get_related", { item_id: itemA, hops: 1 }), "get_related (after link)");
    const rel2Payload = parseText(rel2);
    assert(rel2Payload.related.some((r) => r.id === itemB), "get_related: sees the linked item");
  } else {
    console.log("NOTE: skipping add_knowledge/get_related/link_items/update_item round-trip (no TEST_DATABASE_URL)");
  }

  // --- error paths: isError without killing the server ---------------------------
  const emptySearch = await callTool("search", { query: "" });
  const emptySearchResult = assertToolResult(emptySearch, "search (empty query)");
  assert(emptySearchResult.isError === true, "search with empty query: isError:true");

  const delRandom = await callTool("delete_item", { item_id: crypto.randomUUID() });
  const delRandomResult = assertToolResult(delRandom, "delete_item (random uuid)");
  assert(delRandomResult.isError === true, "delete_item with random uuid: isError:true");

  // server must still be alive and answering
  const after = assertToolResult(await callTool("list_recent", { limit: 3 }), "list_recent (after errors)");
  assert(Array.isArray(after.content), "server still responds after tool errors");

  // --- cleanup of smoke items ------------------------------------------------------
  if (itemA) {
    const delA = assertToolResult(await callTool("delete_item", { item_id: itemA }), "delete_item (cleanup A)");
    assert(!delA.isError && parseText(delA).deleted === itemA, "delete_item (A): { deleted: item_id }");
  }
  if (itemB) {
    const delB = assertToolResult(await callTool("delete_item", { item_id: itemB }), "delete_item (cleanup B)");
    assert(!delB.isError && parseText(delB).deleted === itemB, "delete_item (B): { deleted: item_id }");
  }

  console.log(`\nSMOKE PASSED (${passed} assertions)`);
}

main()
  .catch((e) => {
    console.error(`SMOKE FAILED: ${e && e.stack ? e.stack : e}`);
    process.exitCode = 1;
  })
  .finally(() => {
    try {
      child.kill();
      if (isWin && child.pid) {
        try { execSync(`taskkill /pid ${child.pid} /T /F`, { stdio: "ignore" }); } catch { /* already gone */ }
      }
    } catch { /* ignore */ }
    setTimeout(() => process.exit(process.exitCode ?? 0), 500);
  });
