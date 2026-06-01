"""
Wikipedia handlers on the durable queue.

Currently registered:

  ('wikipedia', 'enrich_performer_from_wikipedia'), target_type='performer'
    For one performer that already has a Wikipedia URL, fetch the page and
    fill any of {birth_date, death_date, biography, primary image} that
    Wikipedia carries and the DB lacks. Only-new: existing values are never
    overwritten.

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

Only-new persistence (matching the producer's intent): a fruitless fetch — or
one where Wikipedia simply has nothing the DB is missing — records a
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


# Pull the performer's current state plus two presence flags so the handler
# can decide, per field, whether there's anything to fetch/write. The image
# flag is wikipedia-source-specific: a performer with a Spotify image but no
# Wikipedia one is still a candidate for a Wikipedia image.
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
            SELECT 1 FROM artist_images ai
            JOIN images im ON im.id = ai.image_id
            WHERE ai.performer_id = p.id AND im.source = 'wikipedia'
        ) AS has_wikipedia_image,
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
    """Fill a performer's missing biographical fields + image from Wikipedia.

    Only-new: searches only for fields the DB lacks and writes only those.
    Returns a result dict describing what (if anything) was added.
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

    # Per-field "is this missing?" — drives which work we do and what we write.
    want_birth = row['birth_date'] is None
    want_death = row['death_date'] is None
    want_biography = not (row['biography'] or '').strip()
    want_image = not row['has_wikipedia_image']

    # Nothing missing -> skip the network entirely. (Rare: needs a deceased,
    # fully-documented performer with a Wikipedia image already saved.)
    if not (want_birth or want_death or want_biography or want_image):
        return {'updated': False, 'reason': 'already_populated', 'name': name}

    searcher = WikipediaSearcher(cache_days=7, force_refresh=force_refresh)
    data = fetch_performer_data(
        searcher,
        wiki_url,
        want_dates=(want_birth or want_death),
        want_biography=want_biography,
        want_image=want_image,
    )

    # Only-new: collect the fields Wikipedia supplied that the DB was missing.
    field_updates: dict[str, str] = {}
    if want_birth and data.birth_date:
        field_updates['birth_date'] = data.birth_date
    if want_death and data.death_date:
        field_updates['death_date'] = data.death_date
    if want_biography and data.biography:
        field_updates['biography'] = data.biography

    image_saved = False
    if want_image and data.image:
        image_saved = _save_wikipedia_image(
            performer_id, data.image, is_primary=not row['has_any_image'],
        )

    if not field_updates and not image_saved:
        return {'updated': False, 'reason': 'nothing_new', 'name': name}

    if field_updates:
        _update_performer_fields(performer_id, field_updates)

    return {
        'updated': True,
        'name': name,
        'birth_date_added': field_updates.get('birth_date'),
        'death_date_added': field_updates.get('death_date'),
        'biography_added': bool(field_updates.get('biography')),
        'image_added': image_saved,
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


def _save_wikipedia_image(performer_id: str, image, is_primary: bool) -> bool:
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
                "VALUES (%s, %s, %s, 0)",
                (performer_id, image_id, is_primary),
            )
        conn.commit()
    return True
