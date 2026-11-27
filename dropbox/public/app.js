/**
 * Second Brain — dropbox UI frontend. Plain DOM, no framework.
 *
 * Security: every piece of server/DB data is rendered with textContent or
 * createElement only — never innerHTML — so titles, URIs, content and error
 * strings can never inject markup.
 */
'use strict';

// ---------------------------------------------------------------------------
// DOM helpers
// ---------------------------------------------------------------------------
function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined && text !== null) node.textContent = String(text);
  return node;
}

function clear(node) {
  node.replaceChildren();
}

function formatScore(score) {
  const n = Number(score);
  if (!Number.isFinite(n)) return '—';
  return n.toFixed(4);
}

function snippet(text, max) {
  const s = String(text).replace(/\s+/g, ' ').trim();
  return s.length > max ? `${s.slice(0, max)}…` : s;
}

// Staleness: trust the server-computed flag when present (merged results),
// otherwise compute it locally from update_period + created_at (raw panels).
function isStale(node) {
  if (typeof node.stale === 'boolean') return node.stale;
  const period = Number(node.update_period);
  if (!Number.isFinite(period) || period === -1) return false;
  if (!node.created_at) return false;
  const created = new Date(node.created_at);
  if (Number.isNaN(created.getTime())) return false;
  return (Date.now() - created.getTime()) / 1000 > period;
}

// ---------------------------------------------------------------------------
// Result rendering (used by all four panels on /retrieve)
// ---------------------------------------------------------------------------
function resultItem(node) {
  const li = el('li', 'result');

  const head = el('div', 'result-head');
  const title = node.title || node.source_uri || node.id || '(untitled)';
  head.append(el('span', 'result-title', title));
  const stale = isStale(node);
  head.append(el('span', stale ? 'badge badge-stale' : 'badge', stale ? 'stale' : 'fresh'));
  li.append(head);

  const meta = el('div', 'result-meta');
  let score = `score ${formatScore(node.score)}`;
  if (node.rerank_score !== undefined) score += ` · rerank ${formatScore(node.rerank_score)}`;
  if (Array.isArray(node.found_by) && node.found_by.length) score += ` · found_by ${node.found_by.join(', ')}`;
  meta.append(el('span', 'result-score', score));
  meta.append(el('span', 'result-uri', node.source_uri || '(no source_uri)'));
  li.append(meta);

  if (node.content) li.append(el('p', 'result-content', snippet(node.content, 280)));
  if (node.via_relationship) {
    li.append(el('p', 'result-rel',
      `via ${node.via_relationship.type} · hop ${node.via_relationship.hop}` +
      (node.via_relationship.context ? ` · ${node.via_relationship.context}` : '')));
  }
  return li;
}

function renderResultList(listEl, countEl, results) {
  clear(listEl);
  const items = Array.isArray(results) ? results : [];
  countEl.textContent = `(${items.length})`;
  for (const node of items) listEl.append(resultItem(node));
}

// ---------------------------------------------------------------------------
// Insert page (/)
// ---------------------------------------------------------------------------
function initInsertPage() {
  const form = document.getElementById('ingest-form');
  const response = document.getElementById('response');
  const submitBtn = document.getElementById('submit-btn');
  const rawText = document.getElementById('raw-text');
  const urlInput = document.getElementById('url-input');
  const fileInput = document.getElementById('file-input');
  const titleInput = document.getElementById('title-input');

  // All 7 bridge source types. Panels are grouped: text gets the textarea;
  // url/youtube/github share the URL input; pdf/image/audio share the file
  // input (upload-then-ingest: POST /api/upload, then /api/ingest the path).
  const panels = {
    text: document.getElementById('panel-text'),
    url: document.getElementById('panel-url'),
    youtube: document.getElementById('panel-url'),
    github: document.getElementById('panel-url'),
    pdf: document.getElementById('panel-file'),
    image: document.getElementById('panel-file'),
    audio: document.getElementById('panel-file'),
  };
  const FILE_ACCEPT = {
    pdf: '.pdf',
    image: '.png,.jpg,.jpeg,.gif,.webp,.bmp',
    audio: '.wav,.mp3,.m4a,.ogg,.flac',
  };
  for (const input of form.elements.mode) {
    input.addEventListener('change', () => {
      const mode = form.elements.mode.value;
      for (const [key, panel] of Object.entries(panels)) panel.hidden = key !== mode;
      if (FILE_ACCEPT[mode]) fileInput.accept = FILE_ACCEPT[mode];
    });
  }

  function setBusy(busy, message) {
    submitBtn.disabled = busy;
    if (message !== undefined) {
      clear(response);
      response.append(el('p', 'status', message));
    }
  }

  function renderIngestResponse(httpStatus, json) {
    clear(response);
    const ok = json && json.ok === true;
    const head = el('div', ok ? 'status status-ok' : 'status status-error');
    head.append(el('strong', null, ok ? `OK (HTTP ${httpStatus})` : `FAILED (HTTP ${httpStatus})`));
    response.append(head);

    if (!ok) {
      response.append(el('p', 'error-text', String((json && json.error) || 'unknown error')));
      return;
    }
    const r = json.result || {};
    const summary = el('p', 'status');
    summary.append(el('span', 'badge', String(r.status || 'unknown')));
    summary.append(` nodes: ${(r.node_ids || []).length} · relationships: ${(r.relationship_ids || []).length} · chunks: ${r.chunks ?? 0}`);
    response.append(summary);

    const pre = el('pre', 'json-block', JSON.stringify(json.result, null, 2));
    response.append(pre);
  }

  async function uploadFile(file) {
    const resp = await fetch(`/api/upload?filename=${encodeURIComponent(file.name)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/octet-stream' },
      body: file,
    });
    const json = await resp.json();
    if (!resp.ok || !json.ok) {
      throw new Error(json.error || `upload failed (HTTP ${resp.status})`);
    }
    return json.result.path;
  }

  form.addEventListener('submit', async (ev) => {
    ev.preventDefault();
    const mode = form.elements.mode.value;
    const title = titleInput.value.trim();
    setBusy(true, 'Working…');
    try {
      let args;
      if (mode === 'text') {
        if (!rawText.value.trim()) throw new Error('text is empty');
        args = { source_type: 'text', payload: rawText.value, source_uri: null, title: title || null };
      } else if (mode === 'url' || mode === 'youtube' || mode === 'github') {
        const url = urlInput.value.trim();
        if (!url) throw new Error('URL is empty');
        args = { source_type: mode, payload: url, source_uri: url, title: title || null };
      } else {
        // pdf | image | audio: the payload is the dropbox/inbox path of the
        // uploaded file — upload first, then ingest with the returned path.
        const file = fileInput.files[0];
        if (!file) throw new Error('no file selected');
        const filePath = await uploadFile(file); // saved under dropbox/inbox/
        args = { source_type: mode, payload: filePath, source_uri: filePath, title: title || null };
      }

      const resp = await fetch('/api/ingest', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(args),
      });
      const json = await resp.json();
      renderIngestResponse(resp.status, json);
    } catch (err) {
      clear(response);
      response.append(el('p', 'status status-error', `Error: ${err.message}`));
    } finally {
      setBusy(false);
    }
  });
}

// ---------------------------------------------------------------------------
// Review page (/review) — curator review queue for human approval.
// Lists pending flags grouped by reason; Approve/Dismiss POST a decision and
// refresh; decided items are shown greyed at the bottom; empty state points
// at the curator command.
// ---------------------------------------------------------------------------
const REVIEW_REASONS = ['stale', 'contradiction', 'superseded', 'isolated'];
const REASON_LABELS = {
  stale: 'Stale',
  contradiction: 'Contradiction',
  superseded: 'Superseded',
  isolated: 'Isolated',
};

function formatAge(iso) {
  if (!iso) return '—';
  const t = new Date(iso).getTime();
  if (Number.isNaN(t)) return '—';
  const s = Math.max(0, Math.floor((Date.now() - t) / 1000));
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m`;
  const h = Math.floor(m / 60);
  if (h < 48) return `${h}h ${m % 60}m`;
  const d = Math.floor(h / 24);
  return `${d}d ${h % 24}h`;
}

function renderSiteNav() {
  // Only the review page has an app.js-rendered header nav (index.html and
  // retrieve.html carry their own static nav and are not edited).
  const nav = document.getElementById('site-nav');
  if (!nav) return;
  const links = [['/', 'Insert'], ['/retrieve', 'Retrieve'], ['/review', 'Review']];
  links.forEach(([href, label], i) => {
    if (i > 0) nav.append(el('span', null, ' · '));
    if (href === '/review') {
      nav.append(el('strong', null, label)); // current page
    } else {
      const a = el('a', null, label);
      a.href = href;
      nav.append(a);
    }
  });
}

function initReviewPage() {
  renderSiteNav();
  const summary = document.getElementById('review-summary');
  const queueBox = document.getElementById('review-queue');
  const decidedBox = document.getElementById('review-decided');
  const decidedList = document.getElementById('review-decided-list');
  const decidedCount = document.getElementById('review-decided-count');

  async function decide(entry, decision, btn) {
    if (btn) btn.disabled = true;
    try {
      const resp = await fetch('/api/review-decision', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ id: entry.id, reason: entry.reason, decision }),
      });
      const json = await resp.json();
      if (!resp.ok || !json.ok) throw new Error(json.error || `HTTP ${resp.status}`);
      // /api/review-decision delegates to the bridge and returns its envelope
      // ({"ok":true,"result":{...}}) — re-read the queue so rendering does not
      // depend on the exact result shape.
      const q = await fetch('/api/review-queue');
      const qj = await q.json();
      if (!q.ok || !qj.ok) throw new Error(qj.error || `HTTP ${q.status}`);
      renderQueue(qj.queue);
    } catch (err) {
      clear(summary);
      summary.append(el('p', 'status status-error', `Decision failed: ${err.message}`));
      if (btn) btn.disabled = false;
    }
  }

  function flagItem(entry, queue, isDecided) {
    const status = String(entry.status || 'pending');
    const li = el('li', isDecided ? 'flag flag-decided' : 'flag');

    const head = el('div', 'flag-head');
    head.append(el('span', 'flag-title', entry.title || entry.id || '(untitled)'));
    head.append(el('span', 'badge', String(entry.reason || 'other')));
    head.append(el('span', `badge badge-${status}`, status));
    li.append(head);

    const meta = el('div', 'flag-meta');
    meta.append(el('span', 'flag-uri', entry.source_uri || '(no source_uri)'));
    meta.append(el('span', 'flag-age', `flagged age ${formatAge(queue && queue.updated_at)}`));
    li.append(meta);

    if (entry.evidence) li.append(el('p', 'flag-evidence', String(entry.evidence)));

    if (!isDecided) {
      const actions = el('div', 'flag-actions');
      const approve = el('button', 'btn-approve', 'Approve');
      const dismiss = el('button', 'btn-dismiss', 'Dismiss');
      approve.addEventListener('click', () => decide(entry, 'approved', approve));
      dismiss.addEventListener('click', () => decide(entry, 'dismissed', dismiss));
      actions.append(approve, dismiss);
      li.append(actions);
    }
    return li;
  }

  function renderQueue(queue) {
    clear(queueBox);
    clear(summary);
    if (!queue || !Array.isArray(queue.flagged)) {
      summary.append(el('p', 'status', 'No review queue file yet (review_queue.json at the repo root).'));
      queueBox.append(el('p', 'empty-state', 'no pending flags — run python scripts/curator.py'));
      decidedBox.hidden = true;
      return;
    }
    const flagged = queue.flagged.filter((e) => e && typeof e === 'object');
    const pending = flagged.filter((e) => String(e.status || 'pending') === 'pending');
    const decided = flagged.filter((e) => e.status === 'approved' || e.status === 'dismissed');

    summary.append(el('p', 'status',
      `queue updated ${queue.updated_at || '—'} · pending ${pending.length} · decided ${decided.length}`));

    if (!pending.length) {
      queueBox.append(el('p', 'empty-state', 'no pending flags — run python scripts/curator.py'));
    } else {
      const groups = new Map();
      for (const e of pending) {
        const reason = String(e.reason || 'other');
        if (!groups.has(reason)) groups.set(reason, []);
        groups.get(reason).push(e);
      }
      const order = [
        ...REVIEW_REASONS.filter((r) => groups.has(r)),
        ...[...groups.keys()].filter((r) => !REVIEW_REASONS.includes(r)),
      ];
      for (const reason of order) {
        const entries = groups.get(reason);
        const section = el('section', 'panel reason-group');
        // Build the h2 first (native append() returns undefined — chaining it
        // into section.append used to render the literal text "undefined").
        const heading = el('h2', null, `${REASON_LABELS[reason] || reason} `);
        heading.append(el('span', 'count', `(${entries.length})`));
        section.append(heading);
        const list = el('ol', 'flags');
        for (const e of entries) list.append(flagItem(e, queue, false));
        section.append(list);
        queueBox.append(section);
      }
    }

    if (decided.length) {
      decidedBox.hidden = false;
      decidedCount.textContent = `(${decided.length})`;
      clear(decidedList);
      for (const e of decided) decidedList.append(flagItem(e, queue, true));
    } else {
      decidedBox.hidden = true;
    }
  }

  (async () => {
    try {
      const resp = await fetch('/api/review-queue');
      const json = await resp.json();
      if (!resp.ok || !json.ok) throw new Error(json.error || `HTTP ${resp.status}`);
      renderQueue(json.queue);
    } catch (err) {
      clear(summary);
      summary.append(el('p', 'status status-error', `Could not load review queue: ${err.message}`));
    }
  })();
}

// ---------------------------------------------------------------------------
// Retrieve page (/retrieve)
// ---------------------------------------------------------------------------
function initRetrievePage() {
  const form = document.getElementById('search-form');
  const response = document.getElementById('response');
  const submitBtn = document.getElementById('submit-btn');
  const queryInput = document.getElementById('query-input');
  const hopsInput = document.getElementById('hops-input');
  const topkInput = document.getElementById('topk-input');
  const panelsBox = document.getElementById('panels');
  const methodBoxes = Array.from(document.querySelectorAll('.method-box'));

  const lists = {
    merged: document.getElementById('results-merged'),
    vector: document.getElementById('results-vector'),
    fts: document.getElementById('results-fts'),
    graph: document.getElementById('results-graph'),
  };
  const counts = {
    merged: document.getElementById('count-merged'),
    vector: document.getElementById('count-vector'),
    fts: document.getElementById('count-fts'),
    graph: document.getElementById('count-graph'),
  };

  form.addEventListener('submit', async (ev) => {
    ev.preventDefault();
    const query = queryInput.value.trim();
    if (!query) {
      clear(response);
      response.append(el('p', 'status status-error', 'Query is empty.'));
      return;
    }
    const methods = methodBoxes.filter((b) => b.checked).map((b) => b.value);
    if (!methods.length) {
      clear(response);
      response.append(el('p', 'status status-error', 'Select at least one method.'));
      return;
    }
    const args = {
      query,
      methods,
      hops: Number.parseInt(hopsInput.value, 10) || 1,
      top_k: Number.parseInt(topkInput.value, 10) || 10,
    };

    submitBtn.disabled = true;
    clear(response);
    response.append(el('p', 'status', 'Searching…'));
    try {
      const resp = await fetch('/api/search', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(args),
      });
      const json = await resp.json();
      clear(response);
      if (!json.ok) {
        response.append(el('p', 'status status-error',
          `Search failed (HTTP ${resp.status}): ${json.error || 'unknown error'}`));
        return;
      }
      const result = json.result || {};
      const byMethod = result.by_method || {};
      panelsBox.hidden = false;
      // Locked spec: show the RAW per-method sets plus the merged view.
      renderResultList(lists.merged, counts.merged, result.results);
      renderResultList(lists.vector, counts.vector, byMethod.vector);
      renderResultList(lists.fts, counts.fts, byMethod.fts);
      renderResultList(lists.graph, counts.graph, byMethod.graph);
      response.append(el('p', 'status status-ok',
        `Done (HTTP ${resp.status}). Merged: ${(result.results || []).length} · ` +
        `vector: ${(byMethod.vector || []).length} · fts: ${(byMethod.fts || []).length} · ` +
        `graph: ${(byMethod.graph || []).length}`));
    } catch (err) {
      clear(response);
      response.append(el('p', 'status status-error', `Error: ${err.message}`));
    } finally {
      submitBtn.disabled = false;
    }
  });
}

// ---------------------------------------------------------------------------
document.addEventListener('DOMContentLoaded', () => {
  const page = document.body.dataset.page;
  if (page === 'insert') initInsertPage();
  else if (page === 'retrieve') initRetrievePage();
  else if (page === 'review') initReviewPage();
});
