#!/usr/bin/env python3
"""
Backfill MusicBrainz release labels — issue #195.

Enqueues one ('musicbrainz', 'backfill_release_label') job per release
that has a `musicbrainz_release_id` but no `label`. The actual MB fetch
+ DB UPDATE happens on the durable research-queue worker (see
research_worker/handlers/musicbrainz.py).

Run as a one-off after the forward fix (commit a019176) lands — the ~71k
historic rows won't refill themselves. Safe to re-run; the research_jobs
unique index dedups against in-flight jobs.

Usage:
    python backfill_release_labels.py
    python backfill_release_labels.py --limit 500
    python backfill_release_labels.py --dry-run
"""

from script_base import ScriptBase, run_script
from core.release_label_backfill import (
    enqueue_sweep,
    find_candidate_release_ids,
)


def main():
    script = ScriptBase(
        name="backfill_release_labels",
        description=(
            "Enqueue per-release MB label-backfill jobs onto the "
            "research-queue worker."
        ),
        epilog="""
Examples:
  python backfill_release_labels.py
  python backfill_release_labels.py --limit 500
  python backfill_release_labels.py --dry-run
        """
    )

    script.add_dry_run_arg()
    script.add_debug_arg()
    script.add_limit_arg(default=None)

    args = script.parse_args()

    script.print_header({
        "DRY RUN": args.dry_run,
        "LIMIT": args.limit if args.limit is not None else 'all candidates',
    })

    if args.dry_run:
        release_ids = find_candidate_release_ids(limit=args.limit)
        script.logger.info(
            "Would enqueue %d release(s) for MB label backfill",
            len(release_ids),
        )
        for rid in release_ids[:25]:
            script.logger.debug("  candidate: %s", rid)
        if len(release_ids) > 25:
            script.logger.debug("  ... %d more", len(release_ids) - 25)
        script.print_summary({
            'candidates': len(release_ids),
            'enqueued': 0,
            'skipped': 0,
        })
        return True

    stats = enqueue_sweep(limit=args.limit)
    script.print_summary(stats)
    # No per-row error count — enqueue_many propagates DB failures and
    # the script exits non-zero via the uncaught exception.
    return True


if __name__ == "__main__":
    run_script(main)
