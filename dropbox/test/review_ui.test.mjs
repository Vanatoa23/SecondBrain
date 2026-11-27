#!/usr/bin/env node
/**
 * Review-page UI test for the dropbox frontend. No external deps — loads
 * public/app.js into a minimal dependency-free DOM stub and exercises the
 * review render path with a sample queue, asserting:
 *
 *   1. every reason section has an h2 child (the old code chained native
 *      append()'s undefined return into section.append(...), which rendered
 *      the literal text "undefined" instead of the heading)
 *   2. NO text node anywhere in the rendered tree equals "undefined"
 *   3. reason groups, heading counts, pending/decided split all render
 *
 * Run: node test/review_ui.test.mjs
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const APP_JS = path.resolve(__dirname, '..', 'public', 'app.js');

// --- minimal DOM stub ---------------------------------------------------------
// createElement returns node objects with append/textContent/className/
// replaceChildren/addEventListener/hidden. Native append() converts non-node
// arguments to text nodes and returns undefined — the stub reproduces both,
// so a chained-append bug (section.append(h2.append(x))) surfaces as a literal
// "undefined" text node with no h2 child, exactly like in a browser.
function makeText(text) {
  return {
    tagName: '#text',
    text: String(text),
    children: [],
    get textContent() { return this.text; },
    set textContent(v) { this.text = String(v); },
  };
}

function makeNode(tag) {
  return {
    tagName: String(tag).toUpperCase(),
    children: [],
    text: '',
    className: '',
    hidden: false,
    disabled: false,
    href: '',
    accept: '',
    listeners: {},
    get textContent() {
      return this.text + this.children.map((c) => c.textContent).join('');
    },
    set textContent(v) {
      this.text = String(v);
      this.children = [];
    },
    append(...nodes) {
      for (const n of nodes) {
        if (n && typeof n === 'object' && typeof n.tagName === 'string') this.children.push(n);
        else this.children.push(makeText(n));
      }
    },
    appendChild(n) { this.append(n); return n; },
    replaceChildren(...nodes) { this.children = []; this.text = ''; this.append(...nodes); },
    addEventListener(type, fn) { (this.listeners[type] = this.listeners[type] || []).push(fn); },
  };
}

const elements = new Map();
function reg(id, tag = 'div') {
  const n = makeNode(tag);
  n.id = id;
  elements.set(id, n);
  return n;
}
for (const id of ['site-nav', 'review-summary', 'review-queue', 'review-decided', 'review-decided-list', 'review-decided-count']) {
  reg(id);
}

const body = makeNode('body');
body.dataset = { page: 'review' };

let domReady = null;
const documentStub = {
  body,
  createElement: (tag) => makeNode(tag),
  getElementById: (id) => elements.get(id) || null,
  querySelectorAll: () => [],
  addEventListener: (type, fn) => { if (type === 'DOMContentLoaded') domReady = fn; },
};

const SAMPLE_QUEUE = {
  updated_at: new Date().toISOString(),
  flagged: [
    { id: 'item-a', title: 'Alpha note', source_uri: 'note://a', reason: 'stale', evidence: 'update_period exceeded', status: 'pending' },
    { id: 'item-b', title: 'Beta note', source_uri: 'note://b', reason: 'stale', evidence: 'older than its update_period', status: 'pending' },
    { id: 'item-c', title: 'Gamma note', source_uri: 'note://c', reason: 'contradiction', evidence: 'conflicts with item-d', status: 'pending' },
    { id: 'item-e', title: 'Epsilon note', source_uri: 'note://e', reason: 'mystery', evidence: 'reason outside the known groups', status: 'pending' },
    { id: 'item-f', title: 'Zeta note', source_uri: 'note://f', reason: 'isolated', evidence: 'no relationships', status: 'approved' },
  ],
};

const fetchStub = async (url) => {
  if (url === '/api/review-queue') {
    return { ok: true, status: 200, json: async () => ({ ok: true, queue: SAMPLE_QUEUE }) };
  }
  throw new Error(`unexpected fetch in stub: ${url}`);
};

// --- load app.js into the stub -------------------------------------------------
const code = fs.readFileSync(APP_JS, 'utf8');
const runApp = new Function('document', 'fetch', code);
runApp(documentStub, fetchStub);

// --- assertions -----------------------------------------------------------------
let passed = 0;
let failed = 0;
const failures = [];

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

function allText(node, out = []) {
  if (node.text) out.push(node.text);
  for (const c of node.children || []) allText(c, out);
  return out;
}

async function main() {
  check('app.js loaded and registered a DOMContentLoaded handler', typeof domReady === 'function');
  domReady();
  await new Promise((r) => setTimeout(r, 20)); // let the async init (fetch + render) settle

  const queueBox = elements.get('review-queue');
  const summary = elements.get('review-summary');
  const decidedBox = elements.get('review-decided');
  const decidedList = elements.get('review-decided-list');
  const decidedCount = elements.get('review-decided-count');

  const sections = queueBox.children.filter((c) => c.tagName === 'SECTION');
  check('pending reasons render as sections (stale, contradiction, unknown group)',
    sections.length === 3, `got ${sections.length}`);

  check('every reason section has an h2 child',
    sections.length > 0 && sections.every((s) => s.children.some((c) => c.tagName === 'H2')));

  const h2s = sections.map((s) => s.children.find((c) => c.tagName === 'H2'));
  check('h2 texts carry the reason label + count',
    h2s.map((h) => (h ? h.textContent : '<no h2>')).join('|') === 'Stale (2)|Contradiction (1)|mystery (1)',
    `got ${JSON.stringify(h2s.map((h) => (h ? h.textContent : '<no h2>')))}`);
  check('each h2 carries the count span',
    h2s.every((h) => h && h.children.some((c) => c.tagName === 'SPAN' && c.className === 'count' && /^\(\d+\)$/.test(c.textContent))));

  const wholeTree = [
    ...allText(queueBox),
    ...allText(summary),
    ...allText(decidedBox),
    ...allText(decidedList),
    ...allText(elements.get('site-nav')),
  ];
  check('NO text node equals "undefined" anywhere in the rendered tree',
    !wholeTree.some((t) => t === 'undefined'),
    `texts: ${JSON.stringify(wholeTree)}`);

  const flagItems = sections.flatMap(
    (s) => ((s.children.find((c) => c.tagName === 'OL') || { children: [] }).children));
  check('all 4 pending flags render as list items with approve/dismiss buttons',
    flagItems.length === 4 && flagItems.every((li) => li.tagName === 'LI' &&
      li.children.some((c) => c.tagName === 'DIV' && c.className === 'flag-actions' &&
        c.children.filter((b) => b.tagName === 'BUTTON').length === 2)),
    `got ${flagItems.length} items`);

  check('decided box shown with exactly 1 decided item and count (1)',
    decidedBox.hidden === false && decidedList.children.length === 1 && decidedCount.textContent === '(1)',
    `hidden=${decidedBox.hidden} items=${decidedList.children.length} count=${decidedCount.textContent}`);

  check('summary line shows pending/decided counts',
    /pending 4 · decided 1/.test(summary.textContent), summary.textContent);

  console.log(`\n${passed} passed, ${failed} failed`);
  if (failures.length) console.log(`failures: ${failures.join('; ')}`);
  process.exit(failed === 0 ? 0 : 1);
}

main();
