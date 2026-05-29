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

from typing import Any

from db_utils import get_db_connection
from integrations.musicbrainz.client import MusicBrainzSearcher
from integrations.musicbrainz.parsing import parse_release_data

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
