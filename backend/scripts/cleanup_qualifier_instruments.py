#!/usr/bin/env python3
"""
One-time cleanup of bogus "instruments" created from MusicBrainz credit
qualifiers (GH #213).

Context
-------
MusicBrainz ``instrument`` performance relationships carry an ``attributes``
array that mixes real instrument names with credit *qualifiers* — "guest",
"solo", "additional", "minor" — that describe *how* an instrument was played
rather than naming an instrument. The pre-fix importer
(``parse_artist_relationships``) appended every attribute indiscriminately, so
qualifiers were stored as if they were instruments. They then surfaced in the
Artist Detail page's instruments list (e.g. "tenor saxophone, guest, solo").

The importer no longer does this (see ``INSTRUMENT_QUALIFIER_ATTRIBUTES`` in
``integrations/musicbrainz/performer_importer.py``). This script repairs the
data already written before that fix.

What it cleans
--------------
For every row in ``instruments`` whose name is one of the qualifiers
(case-insensitive):

  1. ``performer_instruments`` — DELETE all links to the qualifier instrument.
     This is what drives the Artist Detail page, so this is the core fix.

  2. ``recording_performers`` — these rows record a performer's participation
     in a recording. We never want to drop the participation itself, only the
     bogus instrument:
       - If another row already credits the same performer on the same
         recording (real instrument, or a NULL-instrument leader/role row),
         the qualifier row is a redundant duplicate → DELETE it.
       - If the qualifier row is the *only* evidence of the performer on that
         recording → keep the row but NULL out ``instrument_id``.

  3. ``instruments`` — DELETE the now-unreferenced qualifier rows themselves.

All work for a run happens in a single transaction so a failure rolls back
cleanly.

Examples
--------
    # Dry run: report what would change, touch nothing
    python cleanup_qualifier_instruments.py --dry-run

    # Apply the cleanup
    python cleanup_qualifier_instruments.py
"""

from typing import Any, Dict, List

from script_base import ScriptBase, run_script
from db_utils import get_db_connection
from integrations.musicbrainz.performer_importer import (
    INSTRUMENT_QUALIFIER_ATTRIBUTES,
)


# Lowercased list for case-insensitive matching against instruments.name.
_QUALIFIERS_LC = sorted(q.lower() for q in INSTRUMENT_QUALIFIER_ATTRIBUTES)


def _fetch_qualifier_instruments(conn) -> List[Dict[str, Any]]:
    """Return the instruments rows whose name is a credit qualifier."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, name
            FROM instruments
            WHERE lower(name) = ANY(%s)
            ORDER BY name
            """,
            (_QUALIFIERS_LC,),
        )
        return [dict(row) for row in cur.fetchall()]


def _count_links(conn, instrument_ids: List[str]) -> Dict[str, int]:
    """Count rows that reference the qualifier instruments, for reporting."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) AS n FROM performer_instruments "
            "WHERE instrument_id = ANY(%s)",
            (instrument_ids,),
        )
        perf_links = cur.fetchone()['n']

        # Recording rows split into "redundant" (a sibling credit exists) and
        # "sole" (the only credit for that performer on that recording).
        cur.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE has_sibling)     AS redundant,
                COUNT(*) FILTER (WHERE NOT has_sibling) AS sole
            FROM (
                SELECT rp_q.id,
                       EXISTS (
                           SELECT 1 FROM recording_performers rp_o
                           WHERE rp_o.recording_id = rp_q.recording_id
                             AND rp_o.performer_id = rp_q.performer_id
                             AND rp_o.id <> rp_q.id
                             AND (
                                 rp_o.instrument_id IS NULL
                                 OR rp_o.instrument_id <> ALL(%s)
                             )
                       ) AS has_sibling
                FROM recording_performers rp_q
                WHERE rp_q.instrument_id = ANY(%s)
            ) classified
            """,
            (instrument_ids, instrument_ids),
        )
        row = cur.fetchone()

    return {
        'performer_links': perf_links,
        'recording_redundant': row['redundant'] or 0,
        'recording_sole': row['sole'] or 0,
    }


def _apply_cleanup(conn, instrument_ids: List[str]) -> Dict[str, int]:
    """Delete the qualifier links/instruments inside a single transaction."""
    deleted = {
        'performer_links_deleted': 0,
        'recording_rows_deleted': 0,
        'recording_rows_nulled': 0,
        'instruments_deleted': 0,
    }

    with conn.cursor() as cur:
        # 1. performer_instruments — drop every qualifier link (core #213 fix).
        cur.execute(
            "DELETE FROM performer_instruments WHERE instrument_id = ANY(%s)",
            (instrument_ids,),
        )
        deleted['performer_links_deleted'] = cur.rowcount

        # 2a. recording_performers — delete qualifier rows that duplicate an
        #     existing credit for the same (recording, performer).
        cur.execute(
            """
            DELETE FROM recording_performers rp_q
            WHERE rp_q.instrument_id = ANY(%s)
              AND EXISTS (
                  SELECT 1 FROM recording_performers rp_o
                  WHERE rp_o.recording_id = rp_q.recording_id
                    AND rp_o.performer_id = rp_q.performer_id
                    AND rp_o.id <> rp_q.id
                    AND (
                        rp_o.instrument_id IS NULL
                        OR rp_o.instrument_id <> ALL(%s)
                    )
              )
            """,
            (instrument_ids, instrument_ids),
        )
        deleted['recording_rows_deleted'] = cur.rowcount

        # 2b. recording_performers — for remaining qualifier rows (the only
        #     credit for that performer on the recording), keep the row but
        #     drop the bogus instrument.
        cur.execute(
            "UPDATE recording_performers SET instrument_id = NULL "
            "WHERE instrument_id = ANY(%s)",
            (instrument_ids,),
        )
        deleted['recording_rows_nulled'] = cur.rowcount

        # 3. instruments — the qualifier rows are now unreferenced.
        cur.execute(
            "DELETE FROM instruments WHERE id = ANY(%s)",
            (instrument_ids,),
        )
        deleted['instruments_deleted'] = cur.rowcount

    conn.commit()
    return deleted


def main() -> bool:
    script = ScriptBase(
        name="cleanup_qualifier_instruments",
        description=(
            "Remove bogus instruments (guest/solo/additional/minor) imported "
            "from MusicBrainz credit qualifiers. See GH #213."
        ),
        epilog=__doc__ or "",
    )
    script.add_dry_run_arg()
    script.add_debug_arg()
    args = script.parse_args()

    script.print_header({"DRY RUN": args.dry_run})
    script.logger.info(f"Qualifiers: {', '.join(_QUALIFIERS_LC)}")
    script.logger.info("")

    with get_db_connection() as conn:
        instruments = _fetch_qualifier_instruments(conn)

        if not instruments:
            script.logger.info(
                "No qualifier instruments found. Nothing to clean up."
            )
            return True

        instrument_ids = [row['id'] for row in instruments]

        script.logger.info(f"Found {len(instruments)} qualifier instrument(s):")
        for row in instruments:
            script.logger.info(f"  - {row['name']} ({row['id']})")
        script.logger.info("")

        counts = _count_links(conn, instrument_ids)
        script.print_section("Affected rows", {
            "performer_instruments links": counts['performer_links'],
            "recording_performers (duplicate, delete)": counts['recording_redundant'],
            "recording_performers (sole credit, NULL instrument)": counts['recording_sole'],
        })

        if args.dry_run:
            script.logger.info("")
            script.logger.info("[DRY RUN] No changes made.")
            return True

        deleted = _apply_cleanup(conn, instrument_ids)

    script.print_section("Results", {
        "performer_instruments links deleted": deleted['performer_links_deleted'],
        "recording_performers rows deleted": deleted['recording_rows_deleted'],
        "recording_performers instruments nulled": deleted['recording_rows_nulled'],
        "instruments deleted": deleted['instruments_deleted'],
    })

    return True


if __name__ == "__main__":
    run_script(main)
