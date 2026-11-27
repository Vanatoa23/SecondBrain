"""Graph traversal over the relationships table.

Default 1 hop (direct neighbors), configurable up to N hops.
Bidirectional relationships are followed in both directions;
one_way relationships only from from_id -> to_id.

Phase 3 (HippoRAG mechanism, PM_LEDGER.md [2026-11-26 16:23]): page_rank_expand
builds the h-hop neighborhood from seed nodes and then ranks that subgraph with
weighted personalized PageRank, so corroborated / high-strength paths dominate
context assembly instead of raw hop distance.
"""

from __future__ import annotations

from .merge import is_stale

DEFAULT_STRENGTH = 0.5  # edge weight when relationships.strength is NULL
STALE_FACTOR = 0.5      # edge weight multiplier per stale endpoint (merge.STALENESS_PENALTY)


def _directed_endpoints(from_id, to_id, direction):
    """Yield (src, dst) pairs an edge can be traversed as, per its direction.

    one_way       : from_id -> to_id
    bidirectional : both directions
    in            : reversed, to_id -> from_id (defensive; the schema CHECK only
                    allows one_way/bidirectional — bridge link_items "in" stores a
                    swapped one_way row — but raw 'in' rows are handled correctly)
    """
    d = (direction or "one_way").strip().lower()
    if d == "bidirectional":
        return ((from_id, to_id), (to_id, from_id))
    if d == "in":
        return ((to_id, from_id),)
    return ((from_id, to_id),)


def personalized_pagerank(
    node_ids,
    edges,
    seed_ids,
    damping: float = 0.85,
    iterations: int = 30,
    node_factor: dict | None = None,
    tol: float = 1e-12,
) -> dict:
    """Weighted personalized PageRank over an explicit edge list (pure function).

    Args:
        node_ids: all nodes of the subgraph.
        edges: iterable of (src, dst, weight) with weight > 0 (raw edge weights,
            typically relationships.strength; NULL strength handled by the caller).
        seed_ids: teleport set — the random walk restarts on these nodes.
        damping: standard PageRank damping factor.
        iterations: max power-iteration steps (early stop when max delta < tol).
        node_factor: optional {node: factor}; each edge's effective weight is
            weight * factor(src) * factor(dst) while out-degree normalization uses
            the RAW weights, so a decayed (e.g. stale) node genuinely passes on /
            receives less rank mass instead of just redistributing it.

    Returns {node_id: pr_score}. Dangling nodes (no outgoing edges) redistribute
    their mass along the teleport vector (personalized dangling handling), so
    leaf nodes are not inflated.
    """
    nodes = list(dict.fromkeys(node_ids))
    if not nodes:
        return {}
    n = len(nodes)
    idx = {node: i for i, node in enumerate(nodes)}
    out_raw = [0.0] * n
    in_adj: list[list[tuple[int, float]]] = [[] for _ in range(n)]
    factor = node_factor or {}
    for src, dst, w in edges:
        if src not in idx or dst not in idx or w is None or w <= 0:
            continue
        s, d = idx[src], idx[dst]
        eff = w * factor.get(src, 1.0) * factor.get(dst, 1.0)
        in_adj[d].append((s, eff))
        out_raw[s] += w
    seeds = [idx[s] for s in dict.fromkeys(seed_ids) if s in idx]
    if seeds:
        teleport = [0.0] * n
        for s in seeds:
            teleport[s] = 1.0 / len(seeds)
    else:
        teleport = [1.0 / n] * n
    pr = teleport[:]
    for _ in range(max(1, int(iterations))):
        dangling = sum(pr[i] for i in range(n) if out_raw[i] == 0.0)
        new = [
            (1.0 - damping) * teleport[v]
            + damping * dangling * teleport[v]
            + sum(
                damping * pr[u] * eff / out_raw[u]
                for (u, eff) in in_adj[v]
                if out_raw[u] > 0
            )
            for v in range(n)
        ]
        delta = max(abs(new[v] - pr[v]) for v in range(n))
        pr = new
        if delta < tol:
            break
    return {nodes[i]: pr[i] for i in range(n)}


def graph_expand(conn, seed_ids: list[str], hops: int = 1, top_k: int = 20) -> list[dict]:
    """Expand from seed node ids through relationships up to `hops` levels.

    Returns connected nodes with their relationship metadata and a score
    that decays with hop distance (1.0 at hop 1, 0.5 at hop 2, ...).
    """
    if not seed_ids or hops < 1:
        return []

    found: dict[str, dict] = {}
    frontier = set(seed_ids)
    expanded = set(seed_ids)  # visited: every node is expanded at most once

    for hop in range(1, hops + 1):
        if not frontier:
            break
        decay = 1.0 / hop
        placeholders = ",".join(["%s"] * len(frontier))
        rows = conn.execute(
            f"""
            SELECT r.from_id, r.to_id, r.type, r.strength, r.direction, r.context,
                   k.id, k.type AS node_type, k.title, k.content, k.mime_type,
                   k.source_uri, k.distillation_score, k.created_at,
                   k.update_period, k.metadata
            FROM relationships r
            JOIN knowledge_items k
              ON k.id = CASE WHEN r.direction = 'bidirectional'
                            THEN CASE WHEN r.from_id = ANY(ARRAY[{placeholders}])
                                      THEN r.to_id ELSE r.from_id END
                            ELSE r.to_id END
            WHERE (r.from_id = ANY(ARRAY[{placeholders}])
                   OR (r.direction = 'bidirectional' AND r.to_id = ANY(ARRAY[{placeholders}])))
            """,
            (*frontier, *frontier, *frontier),
        ).fetchall()

        next_frontier: set[str] = set()
        for r in rows:
            nid = r["id"]
            if nid in seed_ids:
                continue
            score = decay * (r["strength"] if r["strength"] is not None else DEFAULT_STRENGTH)
            if nid not in found or found[nid]["score"] < score:
                found[nid] = {
                    "id": nid,
                    "type": r["node_type"],
                    "title": r["title"],
                    "content": r["content"],
                    "mime_type": r["mime_type"],
                    "source_uri": r["source_uri"],
                    "distillation_score": r["distillation_score"],
                    "created_at": r["created_at"],
                    "update_period": r["update_period"],
                    "metadata": r["metadata"],
                    "score": score,
                    "via_relationship": {
                        "type": r["type"],
                        "strength": r["strength"],
                        "context": r["context"],
                        "hop": hop,
                    },
                }
            next_frontier.add(nid)
        # Visited set: expand each node at most once (seeds included), so
        # cycles terminate. Multi-hop still works because next_frontier holds
        # the newly discovered, not-yet-expanded nodes (found may contain
        # them already — subtracting `found` here would empty the frontier
        # after hop 1 and silently degrade to 1-hop expansion).
        frontier = next_frontier - expanded
        expanded |= next_frontier

    return sorted(found.values(), key=lambda x: x["score"], reverse=True)[:top_k]


def _load_nodes(conn, node_ids: list) -> dict:
    """Fetch knowledge_items rows for the given ids -> {id: row}."""
    if not node_ids:
        return {}
    placeholders = ",".join(["%s"] * len(node_ids))
    rows = conn.execute(
        f"""
        SELECT id, type, title, content, mime_type, source_uri,
               distillation_score, created_at, update_period, metadata
        FROM knowledge_items
        WHERE id = ANY(ARRAY[{placeholders}])
        """,
        tuple(node_ids),
    ).fetchall()
    return {r["id"]: r for r in rows}


def page_rank_expand(
    conn,
    seed_ids: list[str],
    hops: int = 1,
    top_k: int = 20,
    damping: float = 0.85,
    iterations: int = 30,
) -> list[dict]:
    """h-hop neighborhood from seeds, ranked by personalized PageRank (HippoRAG).

    1. Build the h-hop directed neighborhood from `seed_ids` (same traversal
       semantics as graph_expand: one_way from->to, bidirectional both ways,
       'in' reversed), tracking the best edge per reached node.
    2. Load the full subgraph induced by seeds + reached nodes.
    3. Run weighted personalized PageRank on that subgraph:
       edge weight = strength (0.5 when NULL), direction-aware, and halved per
       stale endpoint (retrieval.merge.is_stale, factor 0.5) so stale nodes — and
       everything reachable only through them — rank lower. Out-degree
       normalization uses the raw weights, so the decay is a real mass loss.
       Teleport set = the seeds.
    4. Return reached nodes (seeds excluded, like graph_expand) with
       score = pr_score and the same field shape graph_expand returns
       (id/type/title/content/mime_type/source_uri/distillation_score/
       created_at/update_period/metadata/score/via_relationship), so
       retrieval.merge.merge_results consumes them unchanged.
    """
    if not seed_ids or hops < 1:
        return []

    seeds = list(dict.fromkeys(seed_ids))
    seed_set = set(seeds)
    found: dict[str, dict] = {}
    frontier = set(seed_set)
    subgraph_nodes = set(seed_set)

    # --- 1) h-hop BFS over directed edges (same expansion logic as graph_expand)
    for hop in range(1, hops + 1):
        if not frontier:
            break
        decay = 1.0 / hop
        placeholders = ",".join(["%s"] * len(frontier))
        edges = conn.execute(
            f"""
            SELECT from_id, to_id, type, strength, direction, context
            FROM relationships
            WHERE from_id = ANY(ARRAY[{placeholders}])
               OR to_id = ANY(ARRAY[{placeholders}])
            """,
            (*frontier, *frontier),
        ).fetchall()
        targets: dict[str, dict] = {}
        for e in edges:
            w = e["strength"] if e["strength"] is not None else DEFAULT_STRENGTH
            for src, dst in _directed_endpoints(e["from_id"], e["to_id"], e["direction"]):
                if src not in frontier or dst in seed_set:
                    continue
                score = decay * w
                if dst not in targets or targets[dst]["_score"] < score:
                    t = dict(e)
                    t["_score"] = score
                    t["_hop"] = hop
                    targets[dst] = t
        if not targets:
            break
        node_rows = _load_nodes(conn, list(targets.keys()))
        next_frontier: set[str] = set()
        for nid, e in targets.items():
            row = node_rows.get(nid)
            if row is None:
                continue
            next_frontier.add(nid)
            subgraph_nodes.add(nid)
            if nid not in found or found[nid]["score"] < e["_score"]:
                found[nid] = {
                    "id": nid,
                    "type": row["type"],
                    "title": row["title"],
                    "content": row["content"],
                    "mime_type": row["mime_type"],
                    "source_uri": row["source_uri"],
                    "distillation_score": row["distillation_score"],
                    "created_at": row["created_at"],
                    "update_period": row["update_period"],
                    "metadata": row["metadata"],
                    "score": e["_score"],
                    "via_relationship": {
                        "type": e["type"],
                        "strength": e["strength"],
                        "context": e["context"],
                        "hop": e["_hop"],
                    },
                }
        frontier = next_frontier - seed_set

    if not found:
        return []

    # --- 2) full subgraph induced by seeds + reached nodes
    all_nodes = list(subgraph_nodes | seed_set)
    node_rows = _load_nodes(conn, all_nodes)
    placeholders = ",".join(["%s"] * len(all_nodes))
    sub_edges = conn.execute(
        f"""
        SELECT from_id, to_id, strength, direction
        FROM relationships
        WHERE from_id = ANY(ARRAY[{placeholders}])
          AND to_id = ANY(ARRAY[{placeholders}])
        """,
        (*all_nodes, *all_nodes),
    ).fetchall()

    # --- 3) weighted personalized PageRank with staleness decay
    stale_factor = {
        nid: (STALE_FACTOR if is_stale(dict(row)) else 1.0)
        for nid, row in node_rows.items()
    }
    edges = []
    for e in sub_edges:
        w = e["strength"] if e["strength"] is not None else DEFAULT_STRENGTH
        for src, dst in _directed_endpoints(e["from_id"], e["to_id"], e["direction"]):
            edges.append((src, dst, w))
    pr = personalized_pagerank(
        all_nodes, edges, seed_set, damping=damping, iterations=iterations,
        node_factor=stale_factor,
    )

    # --- 4) score = pr_score, same shape as graph_expand output
    out = []
    for nid, entry in found.items():
        if nid not in pr:
            continue
        e = dict(entry)
        e["score"] = pr[nid]
        out.append(e)
    return sorted(out, key=lambda x: x["score"], reverse=True)[:top_k]
