#!/usr/bin/env python3
"""
Enrich performer imagery — enqueue onto the durable research queue.

Enqueues one ('commons', 'enrich_performer_imagery') job per performer that is
due for an imagery (re)check (never checked, or last checked more than
--stale-days ago). The actual Commons gathering + visual analysis + DB writes
happen on the research-worker (see research_worker/handlers/commons.py).

Safe to re-run: the research_jobs unique index dedups against in-flight jobs.

Usage:
    python enrich_performer_imagery.py                       # full due-sweep
    python enrich_performer_imagery.py --limit 200           # cap the sweep
    python enrich_performer_imagery.py --stale-days 30       # tighter window
    python enrich_performer_imagery.py --dry-run             # count only
    python enrich_performer_imagery.py --name "Sonny Rollins"
    python enrich_performer_imagery.py --id <performer-uuid>
"""

from script_base import ScriptBase, run_script
from db_utils import get_db_connection
from core.performer_commons_imagery import (
    DEFAULT_STALE_DAYS,
    enqueue_one,
    enqueue_sweep,
    find_candidate_performer_ids,
)


def _resolve_performer_id(script, name=None, performer_id=None):
    if performer_id:
        sql, param = "SELECT id, name FROM performers WHERE id = %s", performer_id
    else:
        sql = "SELECT id, name FROM performers WHERE LOWER(name) = LOWER(%s)"
        param = name
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (param,))
            row = cur.fetchone()
    if row is None:
        script.logger.error("No performer found for %s",
                            f"id={performer_id}" if performer_id else f"name={name!r}")
        return None
    script.logger.info("Resolved performer: %s (%s)", row["name"], row["id"])
    return str(row["id"])


def main() -> bool:
    script = ScriptBase(
        name="enrich_performer_imagery",
        description="Enqueue Commons-imagery enrichment jobs for performers",
        epilog=__doc__,
    )
    group = script.parser.add_mutually_exclusive_group(required=False)
    group.add_argument("--name", help="Enqueue a single performer by name")
    group.add_argument("--id", help="Enqueue a single performer by UUID")
    script.parser.add_argument("--stale-days", type=int, default=DEFAULT_STALE_DAYS,
                               help=f"Re-check performers not checked in this many "
                                    f"days (default {DEFAULT_STALE_DAYS})")
    script.add_limit_arg(default=None)
    script.add_dry_run_arg()
    args = script.parse_args()

    script.print_header({"DRY RUN": args.dry_run})

    # Single performer
    if args.name or args.id:
        performer_id = _resolve_performer_id(script, name=args.name,
                                             performer_id=args.id)
        if performer_id is None:
            return False
        if args.dry_run:
            script.logger.info("[DRY RUN] would enqueue performer %s", performer_id)
            return True
        job_id = enqueue_one(performer_id)
        script.logger.info("Enqueued job %s for performer %s", job_id, performer_id)
        return True

    # Sweep
    if args.dry_run:
        ids = find_candidate_performer_ids(stale_days=args.stale_days, limit=args.limit)
        script.logger.info("[DRY RUN] %d performer(s) due (stale_days=%d, limit=%s)",
                           len(ids), args.stale_days, args.limit)
        return True

    stats = enqueue_sweep(stale_days=args.stale_days, limit=args.limit)
    script.print_summary({
        "candidates": stats["candidates"],
        "enqueued": stats["enqueued"],
        "skipped_dedup": stats["skipped"],
    })
    return True


if __name__ == "__main__":
    run_script(main)
