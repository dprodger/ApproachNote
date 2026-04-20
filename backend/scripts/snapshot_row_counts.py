#!/usr/bin/env python3
"""
Snapshot row counts for every public-schema table.

Persists snapshots to a git-committed JSONL file (default:
`snapshots/row_counts.jsonl` at the repo root). After writing, compares
each table's new count to its most recent previous entry and logs a
WARNING (plus exits non-zero) if any table shrank by more than the
configured threshold. Designed for cron/scheduled use — wrap it in
whatever alerting surface you want.

Why JSONL in git instead of a DB table:
    The watchdog can't live in the thing it's watching. If we lost the DB
    (or restored it from a backup predating the wipe we were trying to
    detect), a DB-resident snapshot history would vanish at exactly the
    moment we need it. A JSONL file committed to the repo gives us:
      - history that survives DB wipes and restores
      - full audit trail via git log
      - greppable, diff-able, readable without psql
      - no new schema to maintain

Why this exists:
    April 2026 incident — `recording_release_streaming_links` and
    `release_streaming_links` went from heavily populated to empty via
    some out-of-band operation we couldn't reconstruct because nothing
    was watching row counts. This script is the watchdog.

File format:
    One JSON object per line (JSONL). Each entry:
      {"captured_at": "2026-04-20T23:04:00+00:00",
       "schema": "public",
       "table": "recordings",
       "rows": 107203}
    Append-only. "Previous count" for a table = the most recent line
    whose "table" matches, excluding any lines written in the current
    run.

What counts as an alarming drop (all configurable via env vars):
    ROW_DROP_ALERT_PCT       default 10   — percent drop threshold
    ROW_DROP_ALERT_MIN_ABS   default 100  — minimum absolute drop to alert
                                            on (prevents noise on tiny
                                            tables where a 50% drop is 2
                                            rows)
    ROW_DROP_ALERT_ALLOWLIST comma-separated table names that are expected
                             to shrink and should never trigger an alert.
                             Defaults to refresh_tokens and
                             password_reset_tokens; anything passed here
                             is added to that set.
    ROW_SNAPSHOT_HISTORY_FILE
                             override the default history file path.

Usage:
    # One-shot run (writes snapshot + compares to previous)
    python scripts/snapshot_row_counts.py

    # Read-only — print counts but don't append to the history file
    python scripts/snapshot_row_counts.py --dry-run

    # Show which tables would be monitored, then exit
    python scripts/snapshot_row_counts.py --list-tables

Scheduling — pick one:

1. **GitHub Actions (recommended, zero infra)** — see
   `.github/workflows/row-snapshot.yml`. Runs daily, commits the updated
   history file back to the repo, and fails the workflow if any alarming
   drop is detected (which triggers a standard GitHub notification email).

2. **Local crontab on a dev machine** — fine for personal monitoring.
   Remember to pull before / push after so the shared history file stays
   coherent across machines.

Exit codes:
    0  — ran successfully, no alarming drops
    1  — unexpected error (DB connection, file I/O, etc.)
    2  — ran successfully but one or more tables had an alarming drop
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Make backend/ importable when this script is run directly from cron.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

load_dotenv()

from db_utils import get_db_connection  # noqa: E402

logger = logging.getLogger(__name__)


# Tables we never want to alert on even if they shrink a lot.
# Tokens churn naturally as they expire or get rotated.
DEFAULT_ALLOWLIST = {
    'refresh_tokens',
    'password_reset_tokens',
}


# Default path is repo-relative: {repo_root}/snapshots/row_counts.jsonl
_SCRIPT_DIR = Path(__file__).resolve().parent         # backend/scripts
_REPO_ROOT = _SCRIPT_DIR.parent.parent                # backend/.. = repo root
DEFAULT_HISTORY_PATH = _REPO_ROOT / 'snapshots' / 'row_counts.jsonl'


def _parse_allowlist(env_value: str | None) -> set[str]:
    extra = {
        t.strip()
        for t in (env_value or '').split(',')
        if t.strip()
    }
    return DEFAULT_ALLOWLIST | extra


def _parse_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == '':
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning(
            f"Env var {name}={raw!r} isn't an int; falling back to default {default}"
        )
        return default


def _history_path() -> Path:
    override = os.getenv('ROW_SNAPSHOT_HISTORY_FILE')
    return Path(override) if override else DEFAULT_HISTORY_PATH


def load_previous_counts(history_file: Path) -> dict[str, int]:
    """Return {table_name -> most recent row_count} from the history file.

    Missing file returns an empty dict — that's the "first run" case and
    the caller will log "no comparison" for every table.
    """
    if not history_file.exists():
        return {}

    latest: dict[str, int] = {}
    with history_file.open() as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                logger.warning(
                    f"{history_file}:{line_no} is not valid JSON — skipping"
                )
                continue
            table = entry.get('table')
            rows = entry.get('rows')
            if table is None or rows is None:
                continue
            # JSONL is append-only and chronologically ordered; last write wins.
            latest[table] = rows
    return latest


def list_public_tables(conn) -> list[str]:
    """Every user table in the public schema, alphabetized.

    Excludes views, partitions, and system catalogs. Uses
    information_schema so we don't need pg_catalog privileges.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """
        )
        return [row['table_name'] for row in cur.fetchall()]


def count_rows(conn, table_name: str) -> int:
    """Exact COUNT(*) for a single table.

    All tables in this DB are well under a million rows so COUNT(*) is
    fine. If we ever grow past that, swap to pg_class.reltuples (estimate
    that's accurate post-ANALYZE and much faster).
    """
    with conn.cursor() as cur:
        # table_name is a Postgres identifier, not a value — can't bind
        # it as a parameter. We've already filtered it through
        # information_schema so it's guaranteed to be a real table name,
        # but double-quote it regardless for belt-and-braces.
        cur.execute(f'SELECT COUNT(*) AS n FROM "{table_name}"')
        return cur.fetchone()['n']


def append_snapshot_batch(history_file: Path, snapshots: list[dict]) -> None:
    """Append a batch of snapshot entries as JSONL.

    Creates parent dirs and file if needed. Opens in append mode so
    concurrent writers from different machines don't clobber each other —
    though the git commit step downstream will serialize via merge
    anyway.
    """
    history_file.parent.mkdir(parents=True, exist_ok=True)
    with history_file.open('a') as f:
        for entry in snapshots:
            f.write(json.dumps(entry, separators=(',', ':')) + '\n')


def evaluate_drop(
    prev: int | None,
    current: int,
    drop_pct: float,
    drop_min_abs: int,
) -> tuple[bool, str]:
    """True if current is alarmingly smaller than prev.

    Returns (is_alarming, reason_message).
    """
    if prev is None:
        return False, "first snapshot, no comparison"
    if prev == 0:
        # Can't compute percent from zero; just note the state.
        return False, f"prev=0, current={current}"
    delta = current - prev
    pct_change = 100.0 * delta / prev
    if delta >= 0:
        return False, f"steady/growing ({delta:+d}, {pct_change:+.1f}%)"
    abs_drop = -delta
    pct_drop = -pct_change
    if abs_drop < drop_min_abs:
        return False, (
            f"drop {abs_drop} < min-abs {drop_min_abs}, "
            f"ignoring ({pct_change:+.1f}%)"
        )
    if pct_drop < drop_pct:
        return False, (
            f"drop {pct_drop:.1f}% < threshold {drop_pct}%, "
            f"ignoring (abs {delta:+d})"
        )
    return True, (
        f"DROP {abs_drop} rows ({pct_change:+.1f}%): {prev} → {current}"
    )


def main():
    parser = argparse.ArgumentParser(
        description='Snapshot public-schema row counts and alert on drops'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help="Print counts and comparison but don't append to the history file",
    )
    parser.add_argument(
        '--list-tables',
        action='store_true',
        help='List tables that would be monitored and exit',
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Include INFO-level lines for every table, not just alarming ones',
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format='%(asctime)s %(levelname)s %(message)s',
    )
    # The summary line at the end is always useful, regardless of level.
    summary_logger = logging.getLogger(f'{__name__}.summary')
    summary_logger.setLevel(logging.INFO)

    drop_pct = float(_parse_int_env('ROW_DROP_ALERT_PCT', 10))
    drop_min_abs = _parse_int_env('ROW_DROP_ALERT_MIN_ABS', 100)
    allowlist = _parse_allowlist(os.getenv('ROW_DROP_ALERT_ALLOWLIST'))
    history_file = _history_path()

    logger.info(
        f"thresholds: drop_pct>={drop_pct}%  drop_min_abs>={drop_min_abs}  "
        f"allowlist={sorted(allowlist)}  history_file={history_file}"
    )

    try:
        previous = load_previous_counts(history_file)

        with get_db_connection() as conn:
            tables = list_public_tables(conn)

            if args.list_tables:
                for t in tables:
                    suffix = "  (allowlisted)" if t in allowlist else ""
                    print(f"{t}{suffix}")
                return 0

            alarming: list[tuple[str, int, int, str]] = []  # (name, prev, curr, reason)
            new_entries: list[dict] = []
            captured_at = datetime.now(timezone.utc).isoformat(timespec='seconds')

            for table in tables:
                try:
                    current = count_rows(conn, table)
                except Exception as e:
                    logger.error(f"  {table}: failed to count: {e}")
                    continue

                prev = previous.get(table)
                is_alarming, reason = evaluate_drop(
                    prev, current, drop_pct, drop_min_abs
                )

                if table in allowlist and is_alarming:
                    logger.info(
                        f"  {table}: {reason} [allowlisted — not alerting]"
                    )
                    is_alarming = False

                if is_alarming:
                    logger.warning(f"  {table}: {reason}")
                    alarming.append((table, prev, current, reason))
                else:
                    logger.info(f"  {table}: {current} rows — {reason}")

                new_entries.append({
                    'captured_at': captured_at,
                    'schema': 'public',
                    'table': table,
                    'rows': current,
                })

            if not args.dry_run:
                append_snapshot_batch(history_file, new_entries)

            summary_logger.info(
                f"snapshot complete: {len(tables)} tables, "
                f"{len(alarming)} alarming drop(s). history={history_file}"
            )

            if alarming:
                # Print a compact summary to stderr so cron systems that
                # capture output (like GitHub Actions) surface it without
                # digging through the full log.
                print(
                    f"\nALARMING DROPS ({len(alarming)}):",
                    file=sys.stderr,
                )
                for name, prev, curr, reason in alarming:
                    print(f"  {name}: {reason}", file=sys.stderr)
                return 2

            return 0

    except Exception:
        logger.exception("Snapshot run failed")
        return 1


if __name__ == '__main__':
    sys.exit(main())
