"""Merge results from vector / FTS / graph, dedup by node id, apply staleness penalty."""

from __future__ import annotations

from datetime import datetime, timezone

STALENESS_PENALTY = 0.5  # stale items keep 50% of their score — never hard-filtered
REVIEW_STATUS_PENALTY = 0.5  # review_status "invalidated..." keeps 50% — never hard-filtered


def is_stale(node: dict, now: datetime | None = None) -> bool:
    """A node is stale if update_period != -1 and its age exceeds update_period."""
    period = node.get("update_period")
    if period is None or period == -1:
        return False
    created = node.get("created_at")
    if created is None:
        return False
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    now = now or datetime.now(timezone.utc)
    age_seconds = (now - created).total_seconds()
    return age_seconds > period


def merge_results(
    by_method: dict[str, list[dict]],
    now: datetime | None = None,
) -> list[dict]:
    """Merge per-method result lists into one deduplicated, scored list.

    Score combination: max score across methods (a node found by both vector
    and FTS keeps its best score), then staleness and review-status penalties
    applied (soft penalties only — nothing is ever filtered out).
    """
    merged: dict[str, dict] = {}
    for method, results in by_method.items():
        for node in results:
            nid = node["id"]
            score = float(node.get("score", 0.0))
            if nid in merged:
                merged[nid]["score"] = max(merged[nid]["score"], score)
                merged[nid]["found_by"].add(method)
            else:
                entry = dict(node)
                entry["score"] = score
                entry["found_by"] = {method}
                merged[nid] = entry

    for entry in merged.values():
        if is_stale(entry, now):
            entry["score"] *= STALENESS_PENALTY
            entry["stale"] = True
        else:
            entry["stale"] = False
        meta = entry.get("metadata")
        review_status = meta.get("review_status") if isinstance(meta, dict) else ""
        if not isinstance(review_status, str):
            review_status = ""  # non-string jsonb value must not crash startswith
        entry["review_status"] = review_status
        if review_status.startswith("invalidated"):
            entry["score"] *= REVIEW_STATUS_PENALTY
        entry["found_by"] = sorted(entry["found_by"])

    return sorted(merged.values(), key=lambda x: x["score"], reverse=True)
