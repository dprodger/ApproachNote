#!/usr/bin/env python3
"""
Verify performer external references — enqueue onto the research queue.

Enqueues one ('musicbrainz', 'verify_performer_references') job per
performer that is missing a Wikipedia and/or MusicBrainz reference. The
actual Wikipedia + MB search and DB UPDATE happen on the durable
research-queue worker (see research_worker/handlers/musicbrainz.py).

This is the durable-queue replacement for the old in-process verifier. It
runs in only-new mode: it fills missing references but does not re-verify or
remove existing ones. Safe to re-run; the research_jobs unique index dedups
against in-flight jobs.

Usage:
    python verify_performer_references.py
    python verify_performer_references.py --limit 500
    python verify_performer_references.py --dry-run
    python verify_performer_references.py --name "Miles Davis"
    python verify_performer_references.py --id "561d854a-6a28-4aa7-8c99-323e6ce46c2a"
"""

from script_base import ScriptBase, run_script
from core import research_jobs
from core.performer_reference_verification import (
    enqueue_sweep,
    find_candidate_performer_ids,
)
from db_utils import get_db_connection


def _resolve_performer_id(script, name=None, performer_id=None):
    """Look up a single performer's UUID by --id or --name. Returns the
    UUID string, or None if not found (caller logs and exits)."""
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


def _enqueue_one(script, performer_uuid):
    """Enqueue a single performer (used by --name / --id)."""
    job_id = research_jobs.enqueue(
        source=research_jobs.SOURCE_MUSICBRAINZ,
        job_type='verify_performer_references',
        target_type=research_jobs.TARGET_PERFORMER,
        target_id=performer_uuid,
        priority=110,
    )
    script.print_summary({
        'candidates': 1,
        'enqueued': 1 if job_id is not None else 0,
        'job_id': job_id,
    })


def main():
    script = ScriptBase(
        name="verify_performer_references",
        description=(
            "Enqueue per-performer reference-verification jobs onto the "
            "research-queue worker (only-new mode)."
        ),
        epilog="""
Examples:
  python verify_performer_references.py
  python verify_performer_references.py --limit 500
  python verify_performer_references.py --dry-run
  python verify_performer_references.py --name "Miles Davis"
  python verify_performer_references.py --id "561d854a-6a28-4aa7-8c99-323e6ce46c2a"
        """
    )

    group = script.parser.add_mutually_exclusive_group()
    group.add_argument('--name', help='Enqueue only the performer with this name')
    group.add_argument('--id', help='Enqueue only the performer with this UUID')

    script.add_dry_run_arg()
    script.add_debug_arg()
    script.add_limit_arg(default=None)

    args = script.parse_args()

    script.print_header({
        "DRY RUN": args.dry_run,
        "LIMIT": args.limit if args.limit is not None else 'all candidates',
        "PERFORMER": args.name or args.id or 'all missing-ref candidates',
    })

    # Single-performer path (--name / --id): resolve and enqueue directly.
    if args.name or args.id:
        performer_uuid = _resolve_performer_id(
            script, name=args.name, performer_id=args.id,
        )
        if performer_uuid is None:
            return False
        if args.dry_run:
            script.logger.info(
                "Would enqueue 1 performer (%s) for reference verification",
                performer_uuid,
            )
            script.print_summary({'candidates': 1, 'enqueued': 0})
            return True
        _enqueue_one(script, performer_uuid)
        return True

    # Sweep path: enqueue all candidates missing a reference.
    if args.dry_run:
        performer_ids = find_candidate_performer_ids(limit=args.limit)
        script.logger.info(
            "Would enqueue %d performer(s) for reference verification",
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

    stats = enqueue_sweep(limit=args.limit)
    script.print_summary(stats)
    return True


if __name__ == "__main__":
    run_script(main)
