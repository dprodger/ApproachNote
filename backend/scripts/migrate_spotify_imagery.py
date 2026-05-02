#!/usr/bin/env python3
"""
Migration: Migrate Spotify artwork from legacy releases columns to release_imagery table

This migration copies existing Spotify album artwork from the legacy columns
(releases.cover_art_small/medium/large) to the normalized release_imagery table.

The legacy columns are retained for backwards compatibility. New Spotify artwork
will be written to both locations until a future migration removes the legacy columns.

Usage:
    # Dry run - see what would be migrated
    python scripts/migrate_spotify_imagery.py --dry-run

    # Run the migration
    python scripts/migrate_spotify_imagery.py

    # Run with limit for testing
    python scripts/migrate_spotify_imagery.py --limit 100
"""

from script_base import ScriptBase, run_script
from db_utils import get_db_connection


def main():
    script = ScriptBase(
        name="migrate_spotify_imagery",
        description="Migrate Spotify artwork from releases columns to release_imagery table",
        epilog="""
Examples:
  python scripts/migrate_spotify_imagery.py --dry-run
  python scripts/migrate_spotify_imagery.py
  python scripts/migrate_spotify_imagery.py --limit 100
        """
    )

    script.add_dry_run_arg()
    script.add_debug_arg()
    script.add_limit_arg(default=0)  # 0 = no limit

    args = script.parse_args()

    script.print_header({
        "DRY RUN": args.dry_run,
    })

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Count releases that need migration
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
            total_to_migrate = cur.fetchone()['count']
            script.logger.info(f"Found {total_to_migrate} releases with Spotify artwork needing migration")

            # Count existing Spotify entries in release_imagery
            cur.execute("""
                SELECT COUNT(*) as count
                FROM release_imagery
                WHERE source = 'Spotify'
            """)
            existing_count = cur.fetchone()['count']
            script.logger.info(f"Existing Spotify entries in release_imagery: {existing_count}")

            if total_to_migrate == 0:
                script.logger.info("No releases need migration")
                script.print_summary({
                    'already_migrated': existing_count,
                    'migrated_this_run': 0,
                })
                return True

            # Build the migration query
            limit_clause = f"LIMIT {args.limit}" if args.limit > 0 else ""

            migration_sql = f"""
                INSERT INTO release_imagery (
                    release_id, source, source_id, type,
                    image_url_small, image_url_medium, image_url_large
                )
                SELECT
                    r.id,
                    'Spotify'::imagery_source,
                    rsl.service_id,
                    'Front'::imagery_type,
                    r.cover_art_small,
                    r.cover_art_medium,
                    r.cover_art_large
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
                {limit_clause}
                ON CONFLICT ON CONSTRAINT release_imagery_unique DO NOTHING
            """

            if args.dry_run:
                script.logger.info(f"[DRY RUN] Would migrate up to {total_to_migrate if args.limit == 0 else min(total_to_migrate, args.limit)} releases")
                script.logger.debug(f"Migration SQL:\n{migration_sql}")
                migrated_count = 0
            else:
                script.logger.info("Running migration...")
                cur.execute(migration_sql)
                migrated_count = cur.rowcount
                conn.commit()
                script.logger.info(f"Migrated {migrated_count} releases")

            # Verify the migration
            cur.execute("""
                SELECT COUNT(*) as count
                FROM release_imagery
                WHERE source = 'Spotify'
            """)
            final_count = cur.fetchone()['count']

    script.print_summary({
        'releases_needing_migration': total_to_migrate,
        'already_in_release_imagery': existing_count,
        'migrated_this_run': migrated_count if not args.dry_run else f"{min(total_to_migrate, args.limit) if args.limit > 0 else total_to_migrate} (dry run)",
        'total_spotify_imagery_after': final_count if not args.dry_run else f"{existing_count} (dry run)",
    })

    return True


if __name__ == "__main__":
    run_script(main)
