#!/usr/bin/env python3
"""
Migration: Drop legacy cover_art columns from releases table

This migration drops the deprecated cover_art_small, cover_art_medium, and
cover_art_large columns from the releases table. All artwork is now stored
in the release_imagery table.

IMPORTANT: This is a destructive migration. Run the verification query first
to ensure all data has been migrated to release_imagery.

Usage:
    # Verify data is migrated (dry run shows what would happen)
    python scripts/drop_legacy_cover_art_columns.py --dry-run

    # Actually drop the columns (requires --confirm flag)
    python scripts/drop_legacy_cover_art_columns.py --confirm
"""

from script_base import ScriptBase, run_script
from db_utils import get_db_connection


def main():
    script = ScriptBase(
        name="drop_legacy_cover_art_columns",
        description="Drop legacy cover_art columns from releases table",
        epilog="""
Examples:
  python scripts/drop_legacy_cover_art_columns.py --dry-run
  python scripts/drop_legacy_cover_art_columns.py --confirm
        """
    )

    script.add_dry_run_arg()
    script.add_debug_arg()
    script.parser.add_argument(
        '--confirm',
        action='store_true',
        help='Required flag to actually drop the columns (destructive operation)'
    )

    args = script.parse_args()

    script.print_header({
        "DRY RUN": args.dry_run,
    })

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Check if columns exist
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'releases'
                  AND column_name IN ('cover_art_small', 'cover_art_medium', 'cover_art_large')
            """)
            existing_columns = [row['column_name'] for row in cur.fetchall()]

            if not existing_columns:
                script.logger.info("Legacy columns already dropped - nothing to do")
                script.print_summary({
                    'status': 'Already clean',
                    'columns_dropped': 0,
                })
                return True

            script.logger.info(f"Found legacy columns to drop: {', '.join(existing_columns)}")

            # Verify all Spotify data has been migrated
            cur.execute("""
                SELECT COUNT(*) as count
                FROM releases r
                JOIN release_streaming_links rsl
                    ON rsl.release_id = r.id AND rsl.service = 'spotify'
                WHERE rsl.service_id IS NOT NULL
                  AND (r.cover_art_small IS NOT NULL
                       OR r.cover_art_medium IS NOT NULL
                       OR r.cover_art_large IS NOT NULL)
                  AND NOT EXISTS (
                      SELECT 1 FROM release_imagery ri
                      WHERE ri.release_id = r.id
                        AND ri.source = 'Spotify'
                  )
            """)
            unmigrated_count = cur.fetchone()['count']

            if unmigrated_count > 0:
                script.logger.error(f"DANGER: {unmigrated_count} releases have artwork in legacy columns but NOT in release_imagery!")
                script.logger.error("Run migrate_spotify_imagery.py first to migrate this data.")
                script.print_summary({
                    'status': 'BLOCKED - unmigrated data exists',
                    'unmigrated_releases': unmigrated_count,
                })
                return False

            script.logger.info("All Spotify artwork has been migrated to release_imagery")

            # Show current counts
            cur.execute("SELECT COUNT(*) as count FROM release_imagery WHERE source = 'Spotify'")
            spotify_imagery_count = cur.fetchone()['count']
            script.logger.info(f"Spotify entries in release_imagery: {spotify_imagery_count}")

            cur.execute("""
                SELECT COUNT(*) as count FROM releases
                WHERE cover_art_small IS NOT NULL
                   OR cover_art_medium IS NOT NULL
                   OR cover_art_large IS NOT NULL
            """)
            legacy_count = cur.fetchone()['count']
            script.logger.info(f"Releases with legacy artwork columns populated: {legacy_count}")

            if args.dry_run:
                script.logger.info("[DRY RUN] Would drop columns: cover_art_small, cover_art_medium, cover_art_large")
                script.print_summary({
                    'status': 'Dry run - no changes made',
                    'columns_to_drop': len(existing_columns),
                    'spotify_imagery_count': spotify_imagery_count,
                })
                return True

            if not args.confirm:
                script.logger.warning("This is a DESTRUCTIVE operation.")
                script.logger.warning("Run with --confirm to actually drop the columns.")
                script.print_summary({
                    'status': 'Requires --confirm flag',
                    'columns_to_drop': len(existing_columns),
                })
                return False

            # Drop the columns
            script.logger.info("Dropping legacy columns...")
            for column in ['cover_art_small', 'cover_art_medium', 'cover_art_large']:
                if column in existing_columns:
                    script.logger.info(f"  Dropping {column}...")
                    cur.execute(f"ALTER TABLE releases DROP COLUMN {column}")

            conn.commit()
            script.logger.info("Columns dropped successfully")

    script.print_summary({
        'status': 'SUCCESS',
        'columns_dropped': len(existing_columns),
        'spotify_imagery_preserved': spotify_imagery_count,
    })

    return True


if __name__ == "__main__":
    run_script(main)
