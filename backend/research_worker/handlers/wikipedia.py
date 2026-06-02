"""
Wikipedia handlers on the durable queue.

Currently registered:

  ('wikipedia', 'enrich_performer_from_wikipedia'), target_type='performer'
    For one performer that already has a Wikipedia URL, fetch the page and
    populate {birth_date, death_date, biography, images}. Birth and death dates
    are only-new (written only when the DB lacks them). Images are harvested in
    full on every run: we walk the page for every content photo (not just the
    lead) and link any we don't already hold, deduped by URL — a re-found image
    is left untouched while genuinely-new ones get added. Biography is refreshed
    on every run — it's overwritten whenever Wikipedia carries one that differs
    from the stored blurb — so an edited Wikipedia bio propagates on the next
    sweep.

Why a dedicated 'wikipedia' source (rather than folding this into the
musicbrainz handler that also touches Wikipedia for reference discovery):

  - This job is Wikipedia-only — no MusicBrainz call — so the binding rate
    limit is Wikipedia's, not MB's. A separate source gives it its own worker
    thread, isolated from the (slow, 1-req/sec) MB backfills, and makes the
    admin job-type rollup read truthfully.
  - WikipediaSearcher rate-limits its own live requests internally, and this
    source has a single worker thread, so jobs of this type are serialised.

Producer: core.performer_wikipedia_enrichment. It enqueues one job per
performer that has a Wikipedia URL — deliberately NOT gated on missing fields,
because Wikipedia content evolves and a future sweep should re-examine
everyone. The handler is the thing that decides, per-field, whether there is
anything new to write, so re-running the producer is cheap and safe.

Persistence outcomes: a fruitless fetch — or one where Wikipedia has nothing
the DB needs (no missing date/image and an unchanged biography) — records a
{updated: False} no-op rather than a RetryableError. The searcher swallows its
own network errors and returns None for both "page missing" and "transient
blip", so we can't distinguish them; re-running the producer re-enqueues a
performer that was missed to a transient outage (the dedup index only collapses
in-flight jobs). Same self-healing property the MB backfills rely on.
"""

from __future__ import annotations

from typing import Any

from db_utils import get_db_connection
from integrations.wikipedia.performer_data import fetch_performer_data
from integrations.wikipedia.utils import WikipediaSearcher

from research_worker.errors import PermanentError
from research_worker.registry import handler


# Pull the performer's current state plus a presence flag so the handler can
# decide, per field, whether there's anything to fetch/write. `has_any_image`
# drives whether a freshly-harvested image becomes the performer's primary —
# we only auto-promote a primary when the performer has none at all. We no
# longer gate image harvesting on an existing Wikipedia image: every run walks
# the page for new photos and relies on URL dedup to skip ones we already hold.
_LOAD_PERFORMER_SQL = """
    SELECT
        p.id,
        p.name,
        p.birth_date,
        p.death_date,
        p.biography,
        p.wikipedia_url,
        p.external_links,
        EXISTS (
            SELECT 1 FROM artist_images ai WHERE ai.performer_id = p.id
        ) AS has_any_image
    FROM performers p
    WHERE p.id = %s
"""


def _wikipedia_url(row) -> str | None:
    """Performer's Wikipedia URL from the dedicated column or external_links,
    treating empty strings as absent."""
    direct = (row.get('wikipedia_url') or '').strip()
    if direct:
        return direct
    links = row['external_links'] or {}
    fallback = (links.get('wikipedia') or '').strip()
    return fallback or None


@handler('wikipedia', 'enrich_performer_from_wikipedia')
def enrich_performer_from_wikipedia(payload: dict[str, Any], ctx) -> dict[str, Any]:
    """Fill a performer's missing biographical fields + harvest images from
    Wikipedia.

    Dates are only-new (written only when the DB lacks them); biography is
    refreshed every run; images are harvested in full and linked when new
    (deduped by URL). Returns a result dict describing what (if anything) was
    added.
    """
    performer_id = ctx.target_id
    force_refresh = bool(payload.get('force_refresh', False))

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(_LOAD_PERFORMER_SQL, (performer_id,))
            row = cur.fetchone()

    if row is None:
        raise PermanentError(f"performer {performer_id} not found")

    name = row['name']
    wiki_url = _wikipedia_url(row)
    if not wiki_url:
        # The producer only enqueues performers with a Wikipedia URL; a row
        # that lost it since enqueue has nothing for this handler to do.
        raise PermanentError(
            f"performer {performer_id} has no Wikipedia URL"
        )

    # Birth and death dates are only-new: we only fetch/write them when the DB
    # lacks them. Biography is refreshed from Wikipedia on every run
    # (overwriting any existing blurb). Images are harvested in full on every
    # run — we walk the page for every content photo and let URL dedup leave
    # already-held images untouched. That means a wiki-URL performer is always
    # worth a (cache-served) page fetch; there's no "already populated"
    # short-circuit.
    want_birth = row['birth_date'] is None
    want_death = row['death_date'] is None

    searcher = WikipediaSearcher(cache_days=7, force_refresh=force_refresh)
    data = fetch_performer_data(
        searcher,
        wiki_url,
        want_dates=(want_birth or want_death),
        want_biography=True,
        want_image=True,
    )

    field_updates: dict[str, str] = {}
    # Only-new for dates: write only what the DB was missing.
    if want_birth and data.birth_date:
        field_updates['birth_date'] = data.birth_date
    if want_death and data.death_date:
        field_updates['death_date'] = data.death_date
    # Refresh biography whenever Wikipedia has one that differs from what we
    # hold — including replacing an existing blurb. Skip the write when it's
    # unchanged so re-runs don't churn rows or report spurious updates.
    if data.biography and data.biography != (row['biography'] or '').strip():
        field_updates['biography'] = data.biography

    images_added = _save_wikipedia_images(
        performer_id, data.images, had_any_image=row['has_any_image'],
    )

    if not field_updates and not images_added:
        return {'updated': False, 'reason': 'nothing_new', 'name': name}

    if field_updates:
        _update_performer_fields(performer_id, field_updates)

    return {
        'updated': True,
        'name': name,
        'birth_date_added': field_updates.get('birth_date'),
        'death_date_added': field_updates.get('death_date'),
        'biography_updated': 'biography' in field_updates,
        'images_added': images_added,
    }


def _update_performer_fields(performer_id: str, fields: dict[str, str]) -> None:
    """UPDATE the named columns. Column names come from a fixed allowlist
    (the keys this handler builds), never from external input."""
    set_clause = ", ".join(f"{col} = %s" for col in fields)
    sql = (
        f"UPDATE performers SET {set_clause}, updated_at = CURRENT_TIMESTAMP "
        f"WHERE id = %s"
    )
    params = list(fields.values()) + [performer_id]
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()


# Re-fetch a known image URL into images (sharing the row across performers if
# it already exists) and link it to the performer. Mirrors the dedup logic in
# the old scripts/fetch_artist_images.py save path.
_INSERT_IMAGE_SQL = """
    INSERT INTO images (
        url, source, source_identifier, license_type, license_url,
        attribution, width, height, thumbnail_url, source_page_url
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    RETURNING id
"""


def _save_wikipedia_images(performer_id: str, images, had_any_image: bool) -> int:
    """Link every harvested image to the performer, deduped by URL.

    `images` arrives lead-first. The lead is promoted to the performer's
    primary only when the performer started with no images at all; everything
    else is appended after the current highest display_order, preserving page
    order. Returns the count of newly-created performer<->image links.
    """
    if not images:
        return 0

    next_order = _next_display_order(performer_id)
    added = 0
    have_image = had_any_image
    for image in images:
        # First newly-linked image on a performer with none becomes primary.
        is_primary = not have_image and added == 0
        if _save_wikipedia_image(
            performer_id, image, is_primary=is_primary, display_order=next_order,
        ):
            added += 1
            next_order += 1
            have_image = True
    return added


def _next_display_order(performer_id: str) -> int:
    """Next free display_order slot for a performer's images (0 when none)."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(MAX(display_order), -1) AS m "
                "FROM artist_images WHERE performer_id = %s",
                (performer_id,),
            )
            return cur.fetchone()['m'] + 1


def _save_wikipedia_image(
    performer_id: str, image, is_primary: bool, display_order: int = 0,
) -> bool:
    """Insert the image (or reuse an existing row by URL) and link it to the
    performer. Returns True if a new performer<->image link was created."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM images WHERE url = %s", (image.url,))
            existing = cur.fetchone()
            if existing:
                image_id = existing['id']
            else:
                cur.execute(_INSERT_IMAGE_SQL, (
                    image.url, image.source, image.source_identifier,
                    image.license_type, image.license_url, image.attribution,
                    image.width, image.height, image.thumbnail_url,
                    image.source_page_url,
                ))
                image_id = cur.fetchone()['id']

            cur.execute(
                "SELECT 1 FROM artist_images "
                "WHERE performer_id = %s AND image_id = %s",
                (performer_id, image_id),
            )
            if cur.fetchone():
                conn.commit()  # image row may have been freshly inserted
                return False

            cur.execute(
                "INSERT INTO artist_images "
                "(performer_id, image_id, is_primary, display_order) "
                "VALUES (%s, %s, %s, %s)",
                (performer_id, image_id, is_primary, display_order),
            )
        conn.commit()
    return True
