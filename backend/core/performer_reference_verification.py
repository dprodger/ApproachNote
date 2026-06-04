"""
Performer reference-verification producers (only-new mode).

Producer side of the durable-queue replacement for the old in-process
scripts/verify_performer_references.py. It scans for performers missing a
Wikipedia and/or MusicBrainz reference and enqueues one
('musicbrainz', 'verify_performer_references') job per row onto the research
queue. The handler in research_worker/handlers/musicbrainz.py does the
actual Wikipedia + MB search and DB UPDATE — see that file for the
only-new / transient-handling semantics.

Two producers share that candidate logic and the same job/handler:

  - enqueue_sweep()    — global backfill, run by the batch script.
  - enqueue_for_song() — ingestion seam, called from
    core.song_research._enqueue_downstream_jobs after a MusicBrainz import so
    a freshly-ingested performer gains its Wikipedia URL automatically (GH
    #208) without an out-of-band batch run.

Per-performer was chosen over a single mega-job for the same reasons as the
release-label backfill (see core.release_label_backfill):

  - Each job is bounded: at most one Wikipedia search + one MB search + one
    UPDATE, naturally serialised under MB's 1-req/sec limit.
  - The research_jobs unique index dedups on
    (musicbrainz, verify_performer_references, performer, <id>), so
    re-running the producer mid-sweep is a no-op for in-flight rows.
  - A worker crash mid-row loses at most one job; the janitor reaps the
    stuck 'running' row and the next worker re-claims it.
  - Admin can watch progress via research_jobs without a side table.

"Missing a reference" honours the dedicated columns AND the external_links
JSONB fallback, matching how the handler reads existing refs: a performer is
a candidate when it lacks a Wikipedia ref in BOTH wikipedia_url and
external_links->>'wikipedia', OR lacks an MB ref in BOTH musicbrainz_id and
external_links->>'musicbrainz'.
"""

from __future__ import annotations

import logging
from typing import Optional, Sequence

from core import research_jobs
from db_utils import get_db_connection


logger = logging.getLogger(__name__)


VALID_REFTYPES = ('wikipedia', 'musicbrainz')

# "Missing" predicates per reference type, honouring the external_links
# fallback the handler also reads.
_MISSING_WIKIPEDIA = (
    "(wikipedia_url IS NULL AND external_links->>'wikipedia' IS NULL)"
)
_MISSING_MUSICBRAINZ = (
    "(musicbrainz_id IS NULL AND external_links->>'musicbrainz' IS NULL)"
)


def _normalise_reftypes(reftypes: Optional[Sequence[str]]) -> list[str]:
    """Return the requested reftypes (both if None/empty), validated."""
    if not reftypes:
        return list(VALID_REFTYPES)
    invalid = [r for r in reftypes if r not in VALID_REFTYPES]
    if invalid:
        raise ValueError(f"unknown reftype(s): {invalid}")
    # Preserve canonical order / dedupe.
    return [r for r in VALID_REFTYPES if r in reftypes]


def _candidate_where(reftypes: list[str]) -> str:
    """Build the WHERE predicate: performer is a candidate if it's missing
    ANY of the requested reference types."""
    clauses = []
    if 'wikipedia' in reftypes:
        clauses.append(_MISSING_WIKIPEDIA)
    if 'musicbrainz' in reftypes:
        clauses.append(_MISSING_MUSICBRAINZ)
    return " OR ".join(clauses)


def find_candidate_performer_ids(
    limit: Optional[int] = None,
    reftypes: Optional[Sequence[str]] = None,
) -> list[str]:
    """Return performer UUIDs missing one of the requested references.

    reftypes defaults to both Wikipedia and MusicBrainz. ORDER BY created_at
    DESC so a partial sweep covers the most-recently-added performers first —
    same convention as the release-label and Spotify-duration backfills.
    """
    reftypes = _normalise_reftypes(reftypes)
    limit_clause = "LIMIT %s" if limit is not None else ""
    sql = f"""
        SELECT id
        FROM performers
        WHERE {_candidate_where(reftypes)}
        ORDER BY created_at DESC
        {limit_clause}
    """
    params: tuple = (limit,) if limit is not None else ()

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    return [str(row['id']) for row in rows]


def find_song_performer_ids(
    song_id: str,
    reftypes: Optional[Sequence[str]] = None,
) -> list[str]:
    """Return UUIDs of performers credited on a song's recordings that are
    missing one of the requested references.

    Scopes the same "missing ref" predicate as the global sweep to a single
    song, via recording_performers -> recordings. Used by the ingestion seam
    so only this song's performers get enqueued, not the whole catalogue.
    """
    reftypes = _normalise_reftypes(reftypes)
    sql = f"""
        SELECT DISTINCT p.id
        FROM performers p
        JOIN recording_performers rp ON rp.performer_id = p.id
        JOIN recordings r ON r.id = rp.recording_id
        WHERE r.song_id = %s
          AND ({_candidate_where(reftypes)})
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (song_id,))
            rows = cur.fetchall()
    return [str(row['id']) for row in rows]


def enqueue_for_song(song_id: str,
                     reftypes: Optional[Sequence[str]] = None,
                     priority: int = 100,
                     batch_size: int = 1000) -> dict[str, int]:
    """Enqueue verify jobs for one song's performers missing a reference.

    The ingestion-time counterpart to enqueue_sweep: instead of scanning the
    whole catalogue it targets just the performers credited on `song_id`'s
    recordings. Shares the candidate predicate, job type, handler, and dedup
    index with the sweep, so it inherits only-new semantics and idempotency —
    re-ingesting a song never re-runs a performer that already has the ref.

    Default priority 100 (the normal research-pipeline default) sits behind
    user-driven work (50) so it won't delay interactive matching, and ahead of
    the bulk sweep (110).

    Returns the same {'candidates', 'enqueued', 'skipped'} shape as
    enqueue_sweep.
    """
    reftypes = _normalise_reftypes(reftypes)
    performer_ids = find_song_performer_ids(song_id, reftypes=reftypes)
    if not performer_ids:
        return {'candidates': 0, 'enqueued': 0, 'skipped': 0}

    result = research_jobs.enqueue_many_for_targets(
        source=research_jobs.SOURCE_MUSICBRAINZ,
        job_type='verify_performer_references',
        target_type=research_jobs.TARGET_PERFORMER,
        target_ids=performer_ids,
        payload={'reftypes': reftypes},
        priority=priority,
        batch_size=batch_size,
    )

    logger.info(
        "performer_reference_verification[song=%s]: reftypes=%s candidates=%d "
        "enqueued=%d skipped=%d",
        song_id, ",".join(reftypes),
        result['requested'], result['inserted'], result['skipped'],
    )
    return {
        'candidates': result['requested'],
        'enqueued': result['inserted'],
        'skipped': result['skipped'],
    }


def enqueue_sweep(limit: Optional[int] = None,
                  reftypes: Optional[Sequence[str]] = None,
                  priority: int = 110,
                  batch_size: int = 1000) -> dict[str, int]:
    """Find candidate performers and enqueue one verify job per row.

    reftypes (default both) scopes BOTH the candidate query and the job
    payload, so a Wikipedia-only sweep enqueues only Wikipedia-missing
    performers and the handler skips the (slow) MusicBrainz lookup.

    Priority 110 sits behind user-driven work (50) and the normal
    research-pipeline default (100), so the sweep won't starve interactive
    jobs.

    Returns:
        {'candidates': N, 'enqueued': M, 'skipped': S}
        - candidates: how many performers the SELECT found.
        - enqueued:   how many new research_jobs rows were inserted.
        - skipped:    how many collapsed against an existing in-flight job
                      via the dedup index — safe and expected on re-runs.
    """
    reftypes = _normalise_reftypes(reftypes)
    performer_ids = find_candidate_performer_ids(limit=limit, reftypes=reftypes)
    if not performer_ids:
        return {'candidates': 0, 'enqueued': 0, 'skipped': 0}

    result = research_jobs.enqueue_many_for_targets(
        source=research_jobs.SOURCE_MUSICBRAINZ,
        job_type='verify_performer_references',
        target_type=research_jobs.TARGET_PERFORMER,
        target_ids=performer_ids,
        payload={'reftypes': reftypes},
        priority=priority,
        batch_size=batch_size,
    )

    logger.info(
        "performer_reference_verification: reftypes=%s candidates=%d "
        "enqueued=%d skipped=%d",
        ",".join(reftypes),
        result['requested'], result['inserted'], result['skipped'],
    )
    return {
        'candidates': result['requested'],
        'enqueued': result['inserted'],
        'skipped': result['skipped'],
    }
