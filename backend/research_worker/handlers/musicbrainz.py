"""
MusicBrainz handlers on the durable queue.

This module is the template for long-running MB tasks — backfills, walks,
re-imports — that need to survive worker restarts and span hours or days
of wall time. Each handler maps one `target_id` (release/recording/
artist/work UUID) to one MB API fetch, a parse, and a DB update. The
worker queue gives us dedup, retry-with-backoff, crash recovery via the
janitor, and admin visibility via the research_jobs table for free.

Currently registered:

  ('musicbrainz', 'backfill_release_label'), target_type='release'
    Re-fetches one MB release and writes `label` / `catalog_number`
    back. Covers the ~71k pre-`+labels` releases (issue #195).

  ('musicbrainz', 'verify_performer_references'), target_type='performer'
    Fills a performer's missing Wikipedia / MusicBrainz references
    (only-new mode). Durable-queue replacement for the old in-process
    scripts/verify_performer_references.py. See that handler's own
    docstring for the only-new / transient-handling rationale.

Conventions for future MB handlers in this module:

  1. Instantiate `MusicBrainzSearcher(force_refresh=True)` per job. The
     30-day disk cache holds responses written before whatever field
     we're after was added to the `inc` string, so trusting it would
     silently re-write the same NULL. `force_refresh=True` is fine
     because the handler runs once per target row and the dedup index
     collapses re-enqueues.

  2. MB's 1-req/sec rate limit is enforced inside the client. With one
     worker thread per (musicbrainz, *) job_type the queue is
     naturally serialised under the limit, so no source_quotas row.

  3. `get_release_details` returns None for *both* 404 and "transient
     failure after the client's internal retries". The client records
     the HTTP status on `last_release_status`, so split on it: a 404
     (deleted/merged MBID) is a PermanentError — straight to 'dead', no
     wasted retries — while None with any other status (timeout/5xx,
     where last_release_status stays None) is a RetryableError.

  4. Idempotency guard at the top of the handler: re-read the target
     row and short-circuit if the field we'd write is already set.
     Lets stale claims and dedup re-enqueues be safe no-ops.

  5. Outcomes:
       target row missing             -> PermanentError
       target.musicbrainz_id is NULL  -> PermanentError
       MB returned None (404)         -> PermanentError
       MB returned None (transient)   -> RetryableError
       MB data, field absent          -> done with {updated: False}
       MB data, field present         -> UPDATE (values clamped) + done
"""

from __future__ import annotations

import json
from typing import Any

from db_utils import get_db_connection
from integrations.musicbrainz.client import MusicBrainzSearcher
from integrations.musicbrainz.parsing import parse_release_data
from integrations.wikipedia.utils import WikipediaSearcher

from research_worker.errors import PermanentError, RetryableError
from research_worker.registry import handler


# Column widths in the releases table (sql/jazz-db-schema.sql). MB can return
# overlong values — typically a concatenated catalog_number — which Postgres
# rejects with StringDataRightTruncation. Clamp before the UPDATE so a long
# value writes a usable prefix instead of crashing the job.
_LABEL_MAX = 255
_CATALOG_NUMBER_MAX = 100

_LOAD_RELEASE_SQL = """
    SELECT id, musicbrainz_release_id, label
    FROM releases
    WHERE id = %s
"""

# Preserve any existing catalog_number — if a human set it manually we
# don't want the backfill to clobber it. label is guaranteed NULL when
# we reach this UPDATE (the idempotency guard returns early otherwise).
_UPDATE_LABEL_SQL = """
    UPDATE releases
    SET label = %s,
        catalog_number = COALESCE(catalog_number, %s),
        updated_at = NOW()
    WHERE id = %s
"""


@handler('musicbrainz', 'backfill_release_label')
def backfill_release_label(payload: dict[str, Any], ctx) -> dict[str, Any]:
    """Re-fetch one MB release and populate `label` / `catalog_number`.

    Issue #195: rows imported before commit a019176 added `+labels` to
    the release-detail `inc` string have label IS NULL. This handler
    fixes the historic rows; the forward fix is in place for new ones.
    """
    release_id = ctx.target_id

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(_LOAD_RELEASE_SQL, (release_id,))
            row = cur.fetchone()

    if row is None:
        raise PermanentError(f"release {release_id} not found")

    mbid = row['musicbrainz_release_id']
    if not mbid:
        raise PermanentError(
            f"release {release_id} has no musicbrainz_release_id"
        )

    if row['label']:
        return {
            'updated': False,
            'reason': 'already_populated',
            'label': row['label'],
        }

    mb_client = MusicBrainzSearcher(force_refresh=True)
    mb_release = mb_client.get_release_details(mbid)

    if mb_release is None:
        # A 404 means the MBID is gone (deleted/merged) — retrying can't fix
        # it. Anything else (timeout, 5xx) is transient and worth a retry.
        if mb_client.last_release_status == 404:
            raise PermanentError(
                f"MB has no release {release_id} (mbid={mbid}): 404 "
                f"deleted/merged"
            )
        raise RetryableError(
            f"MB returned no data for release {release_id} (mbid={mbid})"
        )

    parsed = parse_release_data(mb_release)
    label = parsed.get('label')
    catalog_number = parsed.get('catalog_number')

    if not label:
        return {
            'updated': False,
            'reason': 'no_label_info',
            'mbid': mbid,
        }

    label = label[:_LABEL_MAX]
    if catalog_number:
        catalog_number = catalog_number[:_CATALOG_NUMBER_MAX]

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(_UPDATE_LABEL_SQL, (label, catalog_number, release_id))
        conn.commit()

    return {
        'updated': True,
        'label': label,
        'catalog_number': catalog_number,
        'mbid': mbid,
    }


# ---------------------------------------------------------------------------
# verify_performer_references — fill missing Wikipedia / MusicBrainz refs
# ---------------------------------------------------------------------------
#
# This is the durable-queue replacement for the old in-process
# scripts/verify_performer_references.py. It runs in *only-new* mode: for one
# performer it searches for whichever of {wikipedia_url, musicbrainz_id} is
# missing and writes what it finds. It deliberately does NOT re-verify or
# remove existing references — the producer
# (core.performer_reference_verification) only enqueues performers that are
# already missing at least one ref, and the idempotency guard below
# short-circuits anything that filled in since enqueue.
#
# Scope is controlled by payload['reftypes'] — a subset of
# {'wikipedia', 'musicbrainz'}. The producer sets it from the CLI --reftype
# flag so you can run a Wikipedia-only sweep without the (slow, 1-req/sec)
# MusicBrainz lookups. Absent/empty payload defaults to both, preserving the
# original behaviour. reftype is carried in the payload rather than the
# job_type because research_jobs dedups on (source, job_type, target_type,
# target_id): a single job per performer never collides with itself, but a
# wiki-only and an mb-only job for the SAME performer would. Run scopes
# sequentially (let one drain before enqueuing the other) to avoid that.
#
# Source is 'musicbrainz' (not a new worker source) because MB's 1-req/sec
# limit is the binding constraint and the single (musicbrainz, *) thread
# serialises both the Wikipedia and MB calls under it. Both client classes
# rate-limit their own live requests internally.
#
# Transient-vs-permanent: the underlying searchers swallow their own
# network/HTTP errors and return None / [] for *both* "no match" and "API
# blip". We can't distinguish the two without status plumbing the clients
# don't expose, so a fruitless search records a {updated: False} no-op rather
# than a RetryableError. That's safe for a one-off sweep: a performer missed
# to a transient outage still has a NULL ref, so re-running the producer
# re-enqueues it (the dedup index only collapses in-flight jobs). This is the
# same self-healing property the label backfill relies on.

_VALID_REFTYPES = ('wikipedia', 'musicbrainz')


def _payload_reftypes(payload: dict[str, Any]) -> tuple[bool, bool]:
    """Resolve which references this job should fill from payload['reftypes'].

    Returns (want_wikipedia, want_musicbrainz). Absent/empty -> both, which
    preserves the pre-flag behaviour.
    """
    reftypes = payload.get('reftypes') or list(_VALID_REFTYPES)
    return ('wikipedia' in reftypes, 'musicbrainz' in reftypes)


# Pull a few sample song titles for the same verification context the old
# script built, so WikipediaSearcher can disambiguate common names.
_LOAD_PERFORMER_SQL = """
    SELECT
        p.id,
        p.name,
        p.external_links,
        p.wikipedia_url,
        p.musicbrainz_id,
        p.birth_date,
        p.death_date,
        ARRAY_AGG(DISTINCT s.title) FILTER (WHERE s.title IS NOT NULL)
            AS sample_songs
    FROM performers p
    LEFT JOIN recording_performers rp ON p.id = rp.performer_id
    LEFT JOIN recordings r ON rp.recording_id = r.id
    LEFT JOIN songs s ON r.song_id = s.id
    WHERE p.id = %s
    GROUP BY p.id, p.name, p.external_links, p.wikipedia_url,
             p.musicbrainz_id, p.birth_date, p.death_date
"""


def _wikipedia_ref(row) -> str | None:
    """Existing Wikipedia URL from the dedicated column or external_links."""
    external_links = row['external_links'] or {}
    return row.get('wikipedia_url') or external_links.get('wikipedia')


def _musicbrainz_ref(row) -> str | None:
    """Existing MB artist id from the dedicated column or external_links."""
    external_links = row['external_links'] or {}
    return row.get('musicbrainz_id') or external_links.get('musicbrainz')


def _search_musicbrainz_artist_id(mb_client, performer_name, context):
    """Return a verified MB artist id for an exact name match, or None.

    Ported from the old script's search_musicbrainz(): take the search
    hits, keep the first exact (case-insensitive) name match that
    verify_musicbrainz_reference() confirms.
    """
    artists = mb_client.search_musicbrainz_artist(performer_name)
    for artist in artists:
        if artist.get('name', '').lower() != performer_name.lower():
            continue
        mb_id = artist.get('id')
        if not mb_id:
            continue
        verification = mb_client.verify_musicbrainz_reference(
            performer_name, mb_id, context,
        )
        if verification.get('valid'):
            return mb_id
    return None


@handler('musicbrainz', 'verify_performer_references')
def verify_performer_references(payload: dict[str, Any], ctx) -> dict[str, Any]:
    """Fill in a performer's missing Wikipedia / MusicBrainz references.

    Only-new semantics: searches for whichever ref is absent and writes it.
    Existing references are left untouched. Returns a result dict describing
    what (if anything) was added.
    """
    performer_id = ctx.target_id
    force_refresh = bool(payload.get('force_refresh', False))
    want_wikipedia, want_musicbrainz = _payload_reftypes(payload)

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(_LOAD_PERFORMER_SQL, (performer_id,))
            row = cur.fetchone()

    if row is None:
        raise PermanentError(f"performer {performer_id} not found")

    name = row['name']
    old_wikipedia = _wikipedia_ref(row)
    old_musicbrainz = _musicbrainz_ref(row)

    # Idempotency guard: nothing to do once every *requested* ref is present.
    # Covers stale claims and re-enqueues that raced an earlier fill, and
    # makes a wiki-only job a fast no-op for a performer that already has a
    # Wikipedia ref (no MusicBrainz call regardless of its MB state).
    wikipedia_satisfied = (not want_wikipedia) or bool(old_wikipedia)
    musicbrainz_satisfied = (not want_musicbrainz) or bool(old_musicbrainz)
    if wikipedia_satisfied and musicbrainz_satisfied:
        return {
            'updated': False,
            'reason': 'already_populated',
            'name': name,
        }

    context = {
        'birth_date': row['birth_date'],
        'death_date': row['death_date'],
        'sample_songs': (row['sample_songs'] or [])[:5],
    }

    new_refs: dict[str, str] = {}

    if want_wikipedia and not old_wikipedia:
        wiki_searcher = WikipediaSearcher(
            cache_days=7, force_refresh=force_refresh,
        )
        found_url = wiki_searcher.search_wikipedia(name, context)
        if found_url:
            new_refs['wikipedia'] = found_url

    if want_musicbrainz and not old_musicbrainz:
        mb_client = MusicBrainzSearcher(force_refresh=force_refresh)
        found_id = _search_musicbrainz_artist_id(mb_client, name, context)
        if found_id:
            new_refs['musicbrainz'] = found_id

    if not new_refs:
        return {
            'updated': False,
            'reason': 'no_refs_found',
            'name': name,
        }

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                _build_performer_update_sql(new_refs),
                _build_performer_update_params(new_refs, performer_id),
            )
        conn.commit()

    return {
        'updated': True,
        'name': name,
        'wikipedia_added': new_refs.get('wikipedia'),
        'musicbrainz_added': new_refs.get('musicbrainz'),
    }


def _build_performer_update_sql(new_refs: dict[str, str]) -> str:
    """Compose the UPDATE for whichever refs were found.

    Wikipedia and MusicBrainz write their dedicated columns; any other key
    (none today, but kept for parity with the old script) merges into the
    external_links JSONB.
    """
    set_parts: list[str] = []
    if 'wikipedia' in new_refs:
        set_parts.append("wikipedia_url = %s")
    if 'musicbrainz' in new_refs:
        set_parts.append("musicbrainz_id = %s")
    other = {k: v for k, v in new_refs.items()
             if k not in ('wikipedia', 'musicbrainz')}
    if other:
        set_parts.append(
            "external_links = COALESCE(external_links, '{}'::jsonb) || %s::jsonb"
        )
    set_parts.append("updated_at = CURRENT_TIMESTAMP")
    return f"UPDATE performers SET {', '.join(set_parts)} WHERE id = %s"


def _build_performer_update_params(new_refs: dict[str, str],
                                   performer_id: str) -> list:
    params: list = []
    if 'wikipedia' in new_refs:
        params.append(new_refs['wikipedia'])
    if 'musicbrainz' in new_refs:
        params.append(new_refs['musicbrainz'])
    other = {k: v for k, v in new_refs.items()
             if k not in ('wikipedia', 'musicbrainz')}
    if other:
        params.append(json.dumps(other))
    params.append(performer_id)
    return params
