#!/usr/bin/env python3
"""
Load performer biographical data + imagery from Wikipedia — enqueue onto the
research queue.

Enqueues one ('wikipedia', 'enrich_performer_from_wikipedia') job per
performer that has a Wikipedia URL. The actual page fetch and the only-new DB
writes (birth_date, death_date, biography, primary image) happen on the
durable research-queue worker — see
research_worker/handlers/wikipedia.py.

Unlike verify_performer_references.py, this does NOT search for a Wikipedia
URL: it only walks performers that already have one. Finding the URL is a
separate concern (that's what the reference verifier does); this populates the
data behind a URL we already trust.

It runs in only-new mode: it fills fields Wikipedia has and the DB lacks, but
never overwrites an existing value. Safe to re-run — the research_jobs unique
index dedups against in-flight jobs, and re-running re-examines every
Wikipedia-URL performer (Wikipedia content evolves).

Usage:
    python load_performer_wikipedia_data.py                 # all wiki-URL performers
    python load_performer_wikipedia_data.py --limit 500
    python load_performer_wikipedia_data.py --dry-run
    python load_performer_wikipedia_data.py --name "Miles Davis"
    python load_performer_wikipedia_data.py --id "561d854a-6a28-4aa7-8c99-323e6ce46c2a"
    python load_performer_wikipedia_data.py --force-refresh  # bypass the 7-day Wikipedia cache
"""

from script_base import ScriptBase, run_script
from core.performer_wikipedia_enrichment import (
    enqueue_one,
    enqueue_sweep,
    find_candidate_performer_ids,
)
from db_utils import get_db_connection


def _resolve_performer_id(script, name=None, performer_id=None):
    """Look up a single performer's UUID by --id or --name. Returns the UUID
    string, or None if not found (caller logs and exits)."""
    if performer_id:
        sql = "SELECT id, name FROM performers WHERE id = %s"
        param = performer_id
    else:
        sql = "SELECT id, name FROM performers WHERE LOWER(name) = LOWER(%s)"
        param = name

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (param,))
            row = cur.fetchone()

    if row is None:
        script.logger.error(
            "No performer found for %s",
            f"id={performer_id}" if performer_id else f"name={name!r}",
        )
        return None
    script.logger.info("Resolved performer: %s (%s)", row['name'], row['id'])
    return str(row['id'])


def main():
    script = ScriptBase(
        name="load_performer_wikipedia_data",
        description=(
            "Enqueue per-performer Wikipedia-enrichment jobs (birth/death "
            "dates, biography, image) onto the research-queue worker "
            "(only-new mode). Walks every performer that has a Wikipedia URL."
        ),
        epilog="""
Examples:
  python load_performer_wikipedia_data.py
  python load_performer_wikipedia_data.py --limit 500
  python load_performer_wikipedia_data.py --dry-run
  python load_performer_wikipedia_data.py --name "Miles Davis"
  python load_performer_wikipedia_data.py --id "561d854a-6a28-4aa7-8c99-323e6ce46c2a"
  python load_performer_wikipedia_data.py --force-refresh
        """
    )

    group = script.parser.add_mutually_exclusive_group()
    group.add_argument('--name', help='Enqueue only the performer with this name')
    group.add_argument('--id', help='Enqueue only the performer with this UUID')

    script.add_dry_run_arg()
    script.add_debug_arg()
    script.add_force_refresh_arg()
    script.add_limit_arg(default=None)

    args = script.parse_args()

    script.print_header({
        "DRY RUN": args.dry_run,
        "FORCE REFRESH": args.force_refresh,
        "LIMIT": args.limit if args.limit is not None else 'all candidates',
        "PERFORMER": args.name or args.id or 'all Wikipedia-URL performers',
    })

    # Single-performer path (--name / --id): resolve and enqueue directly.
    # Note: the worker still no-ops if the resolved performer has no Wikipedia
    # URL — this CLI doesn't pre-filter the single case.
    if args.name or args.id:
        performer_uuid = _resolve_performer_id(
            script, name=args.name, performer_id=args.id,
        )
        if performer_uuid is None:
            return False
        if args.dry_run:
            script.logger.info(
                "Would enqueue 1 performer (%s) for Wikipedia enrichment",
                performer_uuid,
            )
            script.print_summary({'candidates': 1, 'enqueued': 0})
            return True
        job_id = enqueue_one(performer_uuid, force_refresh=args.force_refresh)
        script.print_summary({
            'candidates': 1,
            'enqueued': 1 if job_id is not None else 0,
            'job_id': job_id,
        })
        return True

    # Sweep path: enqueue every performer that has a Wikipedia URL.
    if args.dry_run:
        performer_ids = find_candidate_performer_ids(limit=args.limit)
        script.logger.info(
            "Would enqueue %d performer(s) for Wikipedia enrichment",
            len(performer_ids),
        )
        for pid in performer_ids[:25]:
            script.logger.debug("  candidate: %s", pid)
        if len(performer_ids) > 25:
            script.logger.debug("  ... %d more", len(performer_ids) - 25)
        script.print_summary({
            'candidates': len(performer_ids),
            'enqueued': 0,
            'skipped': 0,
        })
        return True

    stats = enqueue_sweep(limit=args.limit, force_refresh=args.force_refresh)
    script.print_summary(stats)
    return True


if __name__ == "__main__":
    run_script(main)
