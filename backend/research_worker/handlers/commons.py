"""
Commons imagery handler on the durable queue.

Registered:

  ('commons', 'enrich_performer_imagery'), target_type='performer'
    For one performer, gather freely-licensed images from Wikimedia Commons
    (+ Flickr Commons if a key is set), run the visual-analysis pipeline
    (resolution/sharpness/face/identity gate, perceptual + ORB de-dup, and a
    cost-bounded Claude vision rerank), and link the good ones into
    images + artist_images. Group photos are never made primary.

Why a dedicated 'commons' source: the binding limits here are the Wikimedia
APIs and the Anthropic vision spend, both distinct from MB/Wikipedia. A
separate source gives it its own worker thread and its own quota row.

Cost control: the Claude rerank is the only paid step. Before reranking, the
handler reserves one 'commons' daily quota unit per image it will rerank
(bounded by rerank_cap). If the budget is spent the job is released until the
window resets (QuotaExhausted) — the standard mechanism — so daily spend has a
hard ceiling set by source_quotas.

Idempotency / re-runs: images are deduped by (source, source_identifier) / URL
and links by (performer_id, image_id), so re-running only ever adds genuinely
new photos. Every run stamps performers.last_imagery_check, which is how the
producer (core.performer_commons_imagery) decides who is due for a (re)sweep.

Producer: core.performer_commons_imagery. It enqueues performers whose
last_imagery_check is NULL or older than the staleness window, so newly added
artists get covered and existing ones get periodically re-examined for new
Commons uploads.
"""

from __future__ import annotations

from typing import Any

from db_utils import get_db_connection
from core import commons_imagery as ci

from research_worker.errors import PermanentError, QuotaExhausted
from research_worker.registry import handler

# Worker-side defaults. The rerank cap bounds paid vision calls per performer;
# the daily 'commons' quota (source_quotas) bounds them across all performers.
_DEFAULT_LIMIT = 8
_DEFAULT_RERANK_CAP = 12

_LOAD_PERFORMER_SQL = """
    SELECT
        p.id,
        p.name,
        p.wikipedia_url,
        p.external_links,
        EXISTS (
            SELECT 1 FROM artist_images ai WHERE ai.performer_id = p.id
        ) AS has_any_image,
        (
            SELECT i.url
            FROM artist_images ai
            JOIN images i ON i.id = ai.image_id
            WHERE ai.performer_id = p.id
            ORDER BY ai.is_primary DESC, ai.display_order
            LIMIT 1
        ) AS reference_url
    FROM performers p
    WHERE p.id = %s
"""


def _wikipedia_url(row) -> str | None:
    direct = (row.get("wikipedia_url") or "").strip()
    if direct:
        return direct
    links = row.get("external_links") or {}
    return (links.get("wikipedia") or "").strip() or None


@handler("commons", "enrich_performer_imagery")
def enrich_performer_imagery(payload: dict[str, Any], ctx) -> dict[str, Any]:
    """Gather + analyze + link freely-licensed imagery for one performer."""
    performer_id = ctx.target_id

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(_LOAD_PERFORMER_SQL, (performer_id,))
            row = cur.fetchone()
    if row is None:
        raise PermanentError(f"performer {performer_id} not found")

    name = row["name"]
    config = ci.GatherConfig(
        limit=int(payload.get("limit", _DEFAULT_LIMIT)),
        rerank_cap=int(payload.get("rerank_cap", _DEFAULT_RERANK_CAP)),
        visual=True, do_rerank=True, do_gate=True, identity=True,
    )

    # Reserve one quota unit per image we'll rerank; QuotaExhausted releases the
    # job until the window resets. A missing quota row (migration not applied)
    # degrades to "no cap" rather than blocking enrichment.
    def rerank_budget(n: int) -> None:
        try:
            ctx.consume_quota(n, "day")
        except QuotaExhausted:
            raise
        except RuntimeError as e:
            ctx.log.warning("commons quota row missing (%s); proceeding "
                            "without a rerank cap", e)

    session = ci.make_session()
    candidates = ci.gather_candidates(
        name, _wikipedia_url(row), session=session, config=config)

    reference_urls = [row["reference_url"]] if row.get("reference_url") else []
    ranked = ci.analyze_and_rank(
        candidates, session=session, config=config,
        reference_urls=reference_urls, performer_name=name,
        rerank_budget=rerank_budget,
    )
    ranked = ranked[: config.limit]

    result = ci.persist_images(
        performer_id, ranked, had_any_image=row["has_any_image"])

    # Stamp the check time on every completion (even a no-op) so the producer
    # doesn't re-enqueue this performer until the staleness window elapses.
    _stamp_checked(performer_id)

    return {
        "updated": result["saved"] > 0,
        "name": name,
        "candidates": len(candidates),
        "kept": len(ranked),
        "images_added": result["saved"],
        "primary_set": result["primary_set"],
    }


def _stamp_checked(performer_id: str) -> None:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE performers "
                "SET last_imagery_check = now(), updated_at = now() "
                "WHERE id = %s",
                (performer_id,),
            )
        conn.commit()
