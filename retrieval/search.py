"""Main retrieval entry point — hybrid search over vector + FTS + graph.

Locked spec (PM_LEDGER.md): all selected methods run in parallel (each on its
own DB connection), results are merged, deduplicated, staleness-penalized,
reranked with bge-reranker-v2-m3, then re-ordered by the staleness-aware final
score and truncated to a token output budget.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from pipeline.chunk import estimate_tokens  # ~4 chars/token estimate
from pipeline.embed import embed_text  # reuse the same bge-m3 embedder (run from project root)
from .db import get_connection
from .fts import fts_search
from .graph import graph_expand, page_rank_expand
from .merge import REVIEW_STATUS_PENALTY, STALENESS_PENALTY, merge_results
from .rerank import rerank
from .vector_search import vector_search

ALL_METHODS = ("vector", "fts", "graph")

REVIEW_PENALTY = REVIEW_STATUS_PENALTY  # 0.5 when review_status startswith "invalidated"

# Graph ranking modes: "expand" = hop-decay scoring (graph_expand, the v1
# behavior), "pagerank" = personalized PageRank over the h-hop subgraph
# (page_rank_expand, HippoRAG mechanism, Phase 3). "auto" keeps hops=1 on the
# v1 path (no regression for existing callers) and uses PageRank for hops>1.
GRAPH_RANKINGS = ("auto", "expand", "pagerank")

DEFAULT_TOKEN_BUDGET = 8000  # max estimated tokens of returned result content


def _apply_token_budget(results: list[dict], token_budget: int) -> list[dict]:
    """Greedily keep results while their estimated tokens fit the budget.

    The last included result is truncated (character cut at ~4 chars/token) so
    the total fits ~budget. At least one result is always returned (when the
    input is non-empty).
    """
    budget = max(1, int(token_budget))
    out: list[dict] = []
    used = 0
    for r in results:
        content = r.get("content") or ""
        t = estimate_tokens(content)
        if used + t <= budget:
            out.append(r)
            used += t
            continue
        # Does not fit whole — include it truncated to the remaining budget.
        remaining = budget - used
        truncated = dict(r)
        truncated["content"] = content[: max(remaining, 1) * 4]
        truncated["truncated"] = True
        out.append(truncated)
        used += estimate_tokens(truncated["content"])
        break
    return out


def search(
    query: str,
    methods: list[str] | None = None,
    hops: int = 1,
    top_k: int = 10,
    candidate_k: int = 20,
    token_budget: int = DEFAULT_TOKEN_BUDGET,
    graph_ranking: str = "auto",
) -> dict:
    """Run a hybrid search.

    Args:
        query: natural-language query string.
        methods: subset of ['vector', 'fts', 'graph']. Default: all three.
        hops: graph traversal depth (default 1 = direct neighbors).
        top_k: final number of results after reranking + staleness ordering.
        candidate_k: per-method candidate pool size before merging.
        token_budget: max estimated tokens of returned result content
            (greedy fill; the last result may be truncated). Default 8000.
        graph_ranking: how the graph method ranks its neighborhood —
            "auto" (default) = "expand" for hops=1 (v1 behavior unchanged),
            "pagerank" for hops>1; or force "expand" / "pagerank" explicitly.

    Returns:
        {
            "results": [...],                      # ranked + budget-truncated
            "by_method": {"vector": [...], ...},   # raw per-method results
            "params": {...},                       # effective parameters used
            "estimated_tokens": int,               # tokens of returned results
        }
    """
    methods = methods or list(ALL_METHODS)
    for m in methods:
        if m not in ALL_METHODS:
            raise ValueError(f"Unknown method {m!r}. Choose from {ALL_METHODS}")
    if graph_ranking not in GRAPH_RANKINGS:
        raise ValueError(f"Unknown graph_ranking {graph_ranking!r}. Choose from {GRAPH_RANKINGS}")
    ranking = graph_ranking
    if ranking == "auto":
        ranking = "pagerank" if hops > 1 else "expand"

    # Embed the query only when a method needs it: vector search uses it
    # directly and graph seeds come from the vector result. A pure FTS query
    # never loads the bge-m3 embedder.
    query_emb = None
    if "vector" in methods or "graph" in methods:
        query_emb = embed_text(query)

    # Each method runs on its OWN connection inside the thread pool — psycopg
    # connections serialize their own wire protocol, so separate connections
    # make the three queries genuinely parallel.
    def run_vector(query_emb):
        with get_connection() as conn:
            return vector_search(conn, query_emb, top_k=candidate_k)

    def run_fts():
        with get_connection() as conn:
            return fts_search(conn, query, top_k=candidate_k)

    def run_graph(query_emb, vector_future=None):
        if vector_future is not None:
            # Reuse the vector result as graph seeds (no re-query).
            seeds = vector_future.result()[:5]
        else:
            with get_connection() as conn:
                seeds = vector_search(conn, query_emb, top_k=5)
        seed_ids = [s["id"] for s in seeds]
        with get_connection() as conn:
            if ranking == "pagerank":
                return page_rank_expand(conn, seed_ids, hops=hops, top_k=candidate_k)
            return graph_expand(conn, seed_ids, hops=hops, top_k=candidate_k)

    with ThreadPoolExecutor(max_workers=len(methods)) as pool:
        futures = {}
        if "vector" in methods:
            futures["vector"] = pool.submit(run_vector, query_emb)
        if "fts" in methods:
            futures["fts"] = pool.submit(run_fts)
        if "graph" in methods:
            futures["graph"] = pool.submit(run_graph, query_emb, futures.get("vector"))
        by_method: dict[str, list[dict]] = {m: futures[m].result() for m in methods}

    merged = merge_results(by_method)
    ranked = rerank(query, merged)  # score ALL merged candidates

    # Final ranking: rerank score with the staleness penalty and the
    # review-status penalty applied, so a stale or invalidated item can no
    # longer outrank a fresh one on rerank score alone. The stale /
    # review_status flags and the penalized final score stay on each result;
    # nothing is ever filtered out here.
    for r in ranked:
        penalty = STALENESS_PENALTY if r.get("stale") else 1.0
        review_status = r.get("review_status") or ""
        if not isinstance(review_status, str):
            review_status = ""
        review_penalty = REVIEW_PENALTY if review_status.startswith("invalidated") else 1.0
        r["review_status"] = review_status
        r["final_score"] = r["rerank_score"] * penalty * review_penalty
    ranked = sorted(ranked, key=lambda x: x["final_score"], reverse=True)[:top_k]

    results = _apply_token_budget(ranked, token_budget)

    return {
        "results": results,
        "by_method": by_method,
        "params": {
            "hops": hops,
            "top_k": top_k,
            "candidate_k": candidate_k,
            "token_budget": max(1, int(token_budget)),
            "graph_ranking": ranking,
        },
        "estimated_tokens": sum(estimate_tokens(r.get("content") or "") for r in results),
    }
