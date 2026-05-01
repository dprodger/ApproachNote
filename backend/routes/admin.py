"""
Admin Routes - Orphan Recording Review

ARCHITECTURE NOTE:
This module handles orphan recording import. To avoid code duplication with
the main MusicBrainz import flow, we reuse:
- MBReleaseImporter: For release creation and CAA cover art import
- PerformerImporter: For linking performers to recordings
- MusicBrainzSearcher: For fetching release details from MB API

This ensures consistent behavior between:
1. Regular song research flow (song_research.py → mb_release_importer.py)
2. Orphan recording import (this module)
"""

from flask import Blueprint, render_template, request, jsonify, redirect, url_for, g
import json
import logging
import secrets

from core.auth_utils import hash_password
from db_utils import get_db_connection
from integrations.musicbrainz.release_importer import MBReleaseImporter
from integrations.musicbrainz.parsing import parse_release_data
from integrations.musicbrainz.performer_importer import PerformerImporter
from integrations.musicbrainz.utils import MusicBrainzSearcher
from integrations.spotify.db import (
    block_streaming_track,
    is_track_manual_override,
    set_track_link_manual_override,
)
from core.spotify_rematch import (
    run_spotify_rematch_for_song,
    save_run,
    list_runs_for_song,
    list_all_runs,
    load_run,
)

logger = logging.getLogger(__name__)

admin_bp = Blueprint('admin', __name__, url_prefix='/admin')


@admin_bp.route('/')
def admin_index():
    """Admin dashboard with links to all admin services"""
    return render_template('admin/index.html')


@admin_bp.route('/orphans')
def orphans_list():
    """List songs with orphan recordings for review"""
    with get_db_connection() as db:
        with db.cursor() as cur:
            # Get songs that have orphan recordings
            cur.execute("""
                SELECT
                    s.id,
                    s.title,
                    s.composer,
                    s.musicbrainz_id,
                    COUNT(o.id) as orphan_count,
                    COUNT(CASE WHEN o.status = 'pending' THEN 1 END) as pending_count,
                    COUNT(CASE WHEN o.status = 'approved' THEN 1 END) as approved_count,
                    COUNT(CASE WHEN o.status = 'rejected' THEN 1 END) as rejected_count,
                    COUNT(CASE WHEN o.spotify_track_id IS NOT NULL THEN 1 END) as with_spotify
                FROM songs s
                JOIN orphan_recordings o ON s.id = o.song_id
                GROUP BY s.id, s.title, s.composer, s.musicbrainz_id
                ORDER BY s.title
            """)
            songs = [dict(row) for row in cur.fetchall()]

    return render_template('admin/orphans_list.html', songs=songs)


@admin_bp.route('/orphans/<song_id>')
def orphans_review(song_id):
    """Review orphan recordings for a specific song"""
    with get_db_connection() as db:
        with db.cursor() as cur:
            # Get song info
            cur.execute("""
                SELECT id, title, composer, musicbrainz_id
                FROM songs WHERE id = %s
            """, (song_id,))
            song = cur.fetchone()

            if not song:
                return "Song not found", 404

            song = dict(song)

            # Get orphan recordings for this song
            cur.execute("""
                SELECT
                    id,
                    mb_recording_id,
                    mb_recording_title,
                    mb_artist_credit,
                    mb_first_release_date,
                    mb_length_ms,
                    mb_disambiguation,
                    mb_releases,
                    issue_type,
                    spotify_track_id,
                    spotify_track_name,
                    spotify_artist_name,
                    spotify_album_name,
                    spotify_preview_url,
                    spotify_external_url,
                    spotify_album_art_url,
                    spotify_match_confidence,
                    spotify_match_score,
                    spotify_matched_mb_release_id,
                    status,
                    review_notes,
                    reviewed_at,
                    imported_recording_id
                FROM orphan_recordings
                WHERE song_id = %s
                ORDER BY
                    CASE status
                        WHEN 'pending' THEN 1
                        WHEN 'approved' THEN 2
                        WHEN 'imported' THEN 3
                        WHEN 'rejected' THEN 4
                    END,
                    CASE spotify_match_confidence
                        WHEN 'high' THEN 1
                        WHEN 'medium' THEN 2
                        WHEN 'low' THEN 3
                        ELSE 4
                    END,
                    mb_artist_credit
            """, (song_id,))
            orphans = [dict(row) for row in cur.fetchall()]

            # Backfill empty mb_releases from MusicBrainz API
            mb = MusicBrainzSearcher()
            for orphan in orphans:
                if not orphan.get('mb_releases'):
                    try:
                        mb.rate_limit()
                        url = f"https://musicbrainz.org/ws/2/recording/{orphan['mb_recording_id']}"
                        params = {'inc': 'releases', 'fmt': 'json'}
                        response = mb.session.get(url, params=params, timeout=15)
                        if response.status_code == 200:
                            data = response.json()
                            releases = []
                            for release in data.get('releases', []):
                                releases.append({
                                    'id': release.get('id'),
                                    'title': release.get('title'),
                                    'date': release.get('date', ''),
                                    'status': release.get('status', ''),
                                    'country': release.get('country', '')
                                })
                            releases.sort(key=lambda r: r.get('date', 'zzzz'))
                            if releases:
                                orphan['mb_releases'] = releases
                                # Persist to DB so we don't re-fetch next time
                                cur.execute("""
                                    UPDATE orphan_recordings
                                    SET mb_releases = %s::jsonb, updated_at = CURRENT_TIMESTAMP
                                    WHERE id = %s
                                """, (json.dumps(releases), orphan['id']))
                    except Exception as e:
                        logger.debug(f"Error backfilling releases for {orphan['mb_recording_id']}: {e}")

            db.commit()

    return render_template('admin/orphans_review.html', song=song, orphans=orphans)


@admin_bp.route('/orphans/<orphan_id>/status', methods=['POST'])
def update_orphan_status(orphan_id):
    """Update the status of an orphan recording"""
    data = request.get_json()
    new_status = data.get('status')
    notes = data.get('notes', '')
    include_spotify = data.get('include_spotify')  # True/False/None

    if new_status not in ['pending', 'approved', 'rejected']:
        return jsonify({'error': 'Invalid status'}), 400

    try:
        with get_db_connection() as db:
            with db.cursor() as cur:
                # If approving and explicitly NOT including Spotify, clear the Spotify fields
                # so the import logic won't use them
                if new_status == 'approved' and include_spotify is False:
                    cur.execute("""
                        UPDATE orphan_recordings
                        SET status = %s,
                            review_notes = %s,
                            reviewed_at = CURRENT_TIMESTAMP,
                            updated_at = CURRENT_TIMESTAMP,
                            spotify_track_id = NULL,
                            spotify_external_url = NULL,
                            spotify_matched_mb_release_id = NULL
                        WHERE id = %s
                        RETURNING id, status
                    """, (new_status, notes or 'Approved without Spotify link', orphan_id))
                else:
                    cur.execute("""
                        UPDATE orphan_recordings
                        SET status = %s,
                            review_notes = %s,
                            reviewed_at = CURRENT_TIMESTAMP,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                        RETURNING id, status
                    """, (new_status, notes, orphan_id))
                result = cur.fetchone()
                db.commit()

        if result:
            return jsonify({'success': True, 'id': str(result['id']), 'status': result['status']})
        else:
            return jsonify({'error': 'Orphan not found'}), 404

    except Exception as e:
        logger.error(f"Error updating orphan status: {e}")
        return jsonify({'error': str(e)}), 500


@admin_bp.route('/orphans/<song_id>/bulk-reject', methods=['POST'])
def bulk_reject_by_artist(song_id):
    """Bulk reject all pending orphans by a specific artist"""
    data = request.get_json()
    artist_credit = data.get('artist_credit')
    notes = data.get('notes', 'Bulk rejected')

    if not artist_credit:
        return jsonify({'error': 'artist_credit is required'}), 400

    try:
        with get_db_connection() as db:
            with db.cursor() as cur:
                cur.execute("""
                    UPDATE orphan_recordings
                    SET status = 'rejected',
                        review_notes = %s,
                        reviewed_at = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE song_id = %s
                      AND mb_artist_credit = %s
                      AND status = 'pending'
                    RETURNING id
                """, (notes, song_id, artist_credit))
                results = cur.fetchall()
                db.commit()

        return jsonify({
            'success': True,
            'rejected_count': len(results)
        })

    except Exception as e:
        logger.error(f"Error bulk rejecting orphans: {e}")
        return jsonify({'error': str(e)}), 500


@admin_bp.route('/orphans/<song_id>/json')
def api_orphans_for_song(song_id):
    """API endpoint to get orphan recordings for a song"""
    with get_db_connection() as db:
        with db.cursor() as cur:
            cur.execute("""
                SELECT
                    id,
                    mb_recording_id,
                    mb_recording_title,
                    mb_artist_credit,
                    mb_first_release_date,
                    issue_type,
                    spotify_track_id,
                    spotify_track_name,
                    spotify_artist_name,
                    spotify_album_name,
                    spotify_preview_url,
                    spotify_external_url,
                    spotify_album_art_url,
                    spotify_match_confidence,
                    status,
                    review_notes
                FROM orphan_recordings
                WHERE song_id = %s
                ORDER BY mb_artist_credit
            """, (song_id,))
            orphans = [dict(row) for row in cur.fetchall()]

    # Convert UUIDs to strings for JSON
    for o in orphans:
        o['id'] = str(o['id'])

    return jsonify(orphans)


@admin_bp.route('/orphans/<song_id>/import', methods=['POST'])
def import_approved_orphans(song_id):
    """Import all approved orphan recordings for a song"""
    imported_count = 0
    errors = []

    try:
        with get_db_connection() as db:
            with db.cursor() as cur:
                # Get song info including work ID
                cur.execute("""
                    SELECT id, title, musicbrainz_id
                    FROM songs WHERE id = %s
                """, (song_id,))
                song = cur.fetchone()

                if not song:
                    return jsonify({'error': 'Song not found'}), 404

                # Get all approved orphans with Spotify data
                cur.execute("""
                    SELECT id, mb_recording_id, mb_recording_title, mb_artist_credit,
                           mb_artist_ids, mb_first_release_date, mb_releases,
                           spotify_track_id, spotify_track_name, spotify_album_name,
                           spotify_external_url, spotify_matched_mb_release_id
                    FROM orphan_recordings
                    WHERE song_id = %s AND status = 'approved'
                """, (song_id,))
                approved_orphans = [dict(row) for row in cur.fetchall()]

                if not approved_orphans:
                    return jsonify({'error': 'No approved orphans to import'}), 400

                for orphan in approved_orphans:
                    try:
                        # Pass db connection (not cursor) for MBReleaseImporter calls
                        recording_id = _import_single_orphan(db, song, orphan)
                        if recording_id:
                            imported_count += 1
                    except Exception as e:
                        logger.error(f"Error importing orphan {orphan['id']}: {e}")
                        errors.append(f"{orphan['mb_artist_credit']}: {str(e)}")

                db.commit()

        return jsonify({
            'success': True,
            'imported': imported_count,
            'errors': errors
        })

    except Exception as e:
        logger.error(f"Error in bulk import: {e}")
        return jsonify({'error': str(e)}), 500


def _import_single_orphan(db, song, orphan):
    """
    Import a single orphan recording into the recordings table.

    Uses shared code from MBReleaseImporter for release creation and CAA import
    to ensure consistent behavior with the main song research flow.

    Args:
        db: Database connection (not cursor) - needed for MBReleaseImporter calls
        song: Song dict with id, musicbrainz_id
        orphan: Orphan recording dict with all fields
    """
    song_id = song['id']
    work_id = song['musicbrainz_id']

    # Create cursor for this function's queries
    cur = db.cursor()

    # Parse year from release date
    recording_year = None
    release_date = orphan.get('mb_first_release_date')
    if release_date and len(release_date) >= 4:
        try:
            recording_year = int(release_date[:4])
        except ValueError:
            pass

    # Check if recording already exists with this MB recording ID
    cur.execute("""
        SELECT id FROM recordings
        WHERE musicbrainz_id = %s AND song_id = %s
    """, (orphan['mb_recording_id'], song_id))

    existing = cur.fetchone()
    if existing:
        # Recording already exists, just update orphan status
        recording_id = existing['id']
        logger.info(f"Recording already exists: {orphan['mb_artist_credit']}")
    else:
        # Create new recording
        cur.execute("""
            INSERT INTO recordings (
                song_id, recording_year,
                mb_first_release_date, is_canonical,
                musicbrainz_id, source_mb_work_id
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            song_id,
            recording_year,
            release_date,
            False,  # Not canonical by default
            orphan['mb_recording_id'],
            work_id
        ))
        result = cur.fetchone()
        recording_id = result['id']
        logger.info(f"Created recording: {orphan['mb_artist_credit']} -> {recording_id}")

    # Link performers if we have MusicBrainz artist IDs
    artist_ids = orphan.get('mb_artist_ids') or []
    artist_names = (orphan.get('mb_artist_credit') or '').split(' / ')

    for i, mbid in enumerate(artist_ids):
        if not mbid:
            continue

        # Get performer name (if available)
        name = artist_names[i] if i < len(artist_names) else None

        # Find or create performer
        performer_id = _find_or_create_performer(cur, mbid, name)

        if performer_id:
            # Link performer to recording (as leader for first artist)
            role = 'leader' if i == 0 else 'sideman'
            cur.execute("""
                INSERT INTO recording_performers (recording_id, performer_id, role)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (recording_id, performer_id, role))

    # Link to release - always create a release from MB data, with Spotify if available
    matched_release_id = orphan.get('spotify_matched_mb_release_id')
    spotify_track_id = orphan.get('spotify_track_id')

    # Determine which MB release to use:
    # 1. If we have a Spotify-matched release, use that
    # 2. Otherwise, use the first release from mb_releases
    mb_release_id = matched_release_id
    if not mb_release_id and orphan.get('mb_releases'):
        mb_releases = orphan['mb_releases']
        if mb_releases and len(mb_releases) > 0:
            mb_release_id = mb_releases[0].get('id')

    if mb_release_id:
        # Find or create the release using full MBReleaseImporter flow
        # This ensures:
        # - Full release metadata from MusicBrainz API
        # - Cover Art Archive images imported
        # - Same code path as regular song research
        release_id, is_new = _find_or_create_release_with_caa(db, mb_release_id, orphan)

        if release_id:
            # Create recording_releases entry
            cur.execute("""
                INSERT INTO recording_releases (
                    recording_id, release_id, track_title, track_artist_credit
                )
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (recording_id, release_id) DO NOTHING
            """, (
                recording_id,
                release_id,
                orphan.get('mb_recording_title'),
                orphan.get('mb_artist_credit')
            ))

            # If we have a Spotify track ID, add it to the streaming links table
            if spotify_track_id:
                # Get the recording_release_id
                cur.execute("""
                    SELECT id FROM recording_releases
                    WHERE recording_id = %s AND release_id = %s
                """, (recording_id, release_id))
                rr_row = cur.fetchone()
                if rr_row:
                    # Check for manual override - don't overwrite manually added links
                    if is_track_manual_override(db, rr_row['id'], 'spotify'):
                        logger.info(f"Skipping Spotify update - manual override exists for recording_release {rr_row['id']}")
                    else:
                        service_url = f'https://open.spotify.com/track/{spotify_track_id}'
                        cur.execute("""
                            INSERT INTO recording_release_streaming_links (
                                recording_release_id, service, service_id, service_url,
                                match_method, matched_at
                            )
                            VALUES (%s, 'spotify', %s, %s, 'orphan_import', CURRENT_TIMESTAMP)
                            ON CONFLICT (recording_release_id, service) DO UPDATE
                            SET service_id = EXCLUDED.service_id,
                                service_url = EXCLUDED.service_url,
                                match_method = EXCLUDED.match_method,
                                matched_at = CURRENT_TIMESTAMP,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE recording_release_streaming_links.match_method != 'manual'
                               OR recording_release_streaming_links.match_method IS NULL
                        """, (rr_row['id'], spotify_track_id, service_url))
                        logger.info(f"Linked recording to release with Spotify: {spotify_track_id}"
                                   f"{' (new release with CAA)' if is_new else ''}")
            else:
                logger.info(f"Linked recording to release (no Spotify): {mb_release_id}"
                           f"{' (new release with CAA)' if is_new else ''}")

            # Set default_release_id if recording doesn't have one
            cur.execute("""
                UPDATE recordings
                SET default_release_id = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                  AND default_release_id IS NULL
            """, (release_id, recording_id))

    # Update orphan status to imported
    cur.execute("""
        UPDATE orphan_recordings
        SET status = 'imported',
            imported_recording_id = %s,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
    """, (recording_id, orphan['id']))

    # Close the cursor we created
    cur.close()

    return recording_id


def _find_or_create_performer(cur, mbid, name):
    """Find performer by MusicBrainz ID or create if not exists"""
    # Try to find by MBID
    cur.execute("""
        SELECT id FROM performers WHERE musicbrainz_id = %s
    """, (mbid,))
    result = cur.fetchone()
    if result:
        return result['id']

    # Try to find by name (case-insensitive)
    if name:
        cur.execute("""
            SELECT id FROM performers WHERE LOWER(name) = LOWER(%s)
        """, (name,))
        result = cur.fetchone()
        if result:
            # Update the existing performer with the MBID
            cur.execute("""
                UPDATE performers SET musicbrainz_id = %s WHERE id = %s
            """, (mbid, result['id']))
            return result['id']

        # Create new performer
        cur.execute("""
            INSERT INTO performers (name, musicbrainz_id)
            VALUES (%s, %s)
            RETURNING id
        """, (name, mbid))
        result = cur.fetchone()
        if result:
            logger.info(f"Created performer: {name}")
            return result['id']

    return None


def _find_or_create_release_with_caa(conn, mb_release_id: str, orphan: dict) -> tuple:
    """
    Find or create a release using the full MBReleaseImporter flow.

    This ensures:
    1. Release is created with full metadata from MusicBrainz API
    2. Cover art is fetched from Cover Art Archive
    3. Same code path as regular song research import

    Args:
        conn: Database connection
        mb_release_id: MusicBrainz release ID
        orphan: Orphan recording dict (for fallback artist credit)

    Returns:
        Tuple of (release_id, is_new) where is_new indicates if release was created
    """
    if not mb_release_id:
        return None, False

    with conn.cursor() as cur:
        # Try to find existing release
        cur.execute("""
            SELECT id FROM releases WHERE musicbrainz_release_id = %s
        """, (mb_release_id,))
        result = cur.fetchone()
        if result:
            return result['id'], False

    # Release doesn't exist - fetch full details from MusicBrainz and create
    # Use MBReleaseImporter which handles:
    # - Full release metadata parsing
    # - Lookup table management (formats, statuses, packaging)
    # - Cover Art Archive import

    mb_searcher = MusicBrainzSearcher()
    release_details = mb_searcher.get_release_details(mb_release_id)

    if not release_details:
        logger.warning(f"Could not fetch release details from MusicBrainz: {mb_release_id}")
        # Fall back to minimal release creation
        return _create_minimal_release(conn, mb_release_id, orphan), True

    # Use MBReleaseImporter for consistent release creation
    importer = MBReleaseImporter(dry_run=False, import_cover_art=True, logger=logger)

    # Load lookup table caches (formats, statuses, packaging)
    importer._load_lookup_caches(conn)

    # Parse release data using the same logic as regular import
    release_data = parse_release_data(release_details)

    # Create the release
    release_id = importer._create_release(conn, release_data)

    if release_id:
        logger.info(f"Created release via MBReleaseImporter: {release_data.get('title')}")

        # Import cover art from Cover Art Archive
        # This is the same code path used during regular song research
        importer._import_cover_art_for_release(conn, release_id, mb_release_id)

    return release_id, True


def _create_minimal_release(conn, mb_release_id: str, orphan: dict):
    """
    Create a minimal release when MusicBrainz API fetch fails.

    This is a fallback for when we can't get full release details.
    The release can be enriched later via the CAA importer.
    """
    # Get basic info from the orphan's mb_releases data
    mb_releases = orphan.get('mb_releases') or []
    release_info = next(
        (r for r in mb_releases if r.get('id') == mb_release_id),
        {}
    )

    release_year = None
    release_date = release_info.get('date')
    if release_date and len(release_date) >= 4:
        try:
            release_year = int(release_date[:4])
        except ValueError:
            pass

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO releases (
                musicbrainz_release_id, title, artist_credit,
                release_year, country
            )
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
        """, (
            mb_release_id,
            release_info.get('title'),
            orphan.get('mb_artist_credit'),
            release_year,
            release_info.get('country')
        ))
        result = cur.fetchone()
        if result:
            logger.info(f"Created minimal release (fallback): {release_info.get('title')}")
            return result['id']

    return None


@admin_bp.route('/orphans/<song_id>/existing-recordings')
def get_existing_recordings_for_song(song_id):
    """
    Get existing recordings for a song that an orphan could be linked to.

    Returns recordings with their releases and Spotify info to help identify
    if an orphan is the same performance as an existing recording.
    """
    with get_db_connection() as db:
        with db.cursor() as cur:
            # Get song info
            cur.execute("""
                SELECT id, title, musicbrainz_id FROM songs WHERE id = %s
            """, (song_id,))
            song = cur.fetchone()

            if not song:
                return jsonify({'error': 'Song not found'}), 404

            # Get existing recordings with their releases and Spotify data
            cur.execute("""
                SELECT
                    rec.id,
                    rec.musicbrainz_id as mb_recording_id,
                    def_rel.title as album_title,
                    rec.recording_year,
                    rec.mb_first_release_date,
                    p.name as leader_name,
                    p.id as leader_id,
                    -- Get releases for this recording
                    (
                        SELECT json_agg(json_build_object(
                            'release_id', rel.id,
                            'title', rel.title,
                            'year', rel.release_year,
                            'mb_release_id', rel.musicbrainz_release_id,
                            'spotify_album_id', rel.spotify_album_id,
                            'spotify_album_url', CASE WHEN rel.spotify_album_id IS NOT NULL
                                THEN 'https://open.spotify.com/album/' || rel.spotify_album_id END,
                            'spotify_track_id', rrsl.service_id,
                            'spotify_track_url', rrsl.service_url,
                            'album_art_small', (SELECT ri.image_url_small FROM release_imagery ri
                                 WHERE ri.release_id = rel.id AND ri.type = 'Front' LIMIT 1)
                        ) ORDER BY rel.release_year)
                        FROM recording_releases rr
                        JOIN releases rel ON rr.release_id = rel.id
                        LEFT JOIN recording_release_streaming_links rrsl
                            ON rrsl.recording_release_id = rr.id AND rrsl.service = 'spotify'
                        WHERE rr.recording_id = rec.id
                    ) as releases
                FROM recordings rec
                LEFT JOIN releases def_rel ON rec.default_release_id = def_rel.id
                LEFT JOIN recording_performers rp ON rec.id = rp.recording_id AND rp.role = 'leader'
                LEFT JOIN performers p ON rp.performer_id = p.id
                WHERE rec.song_id = %s
                ORDER BY rec.recording_year, COALESCE(p.sort_name, p.name)
            """, (song_id,))

            recordings = []
            for row in cur.fetchall():
                rec = dict(row)
                # Parse releases JSON
                rec['releases'] = rec['releases'] or []
                # Add a flag for whether any release has a Spotify track match
                rec['has_spotify'] = any(
                    r.get('spotify_track_id')
                    for r in rec['releases']
                )
                recordings.append(rec)

            return jsonify({
                'song': dict(song),
                'recordings': recordings
            })


@admin_bp.route('/orphans/<orphan_id>/link-to-recording', methods=['POST'])
def link_orphan_to_existing_recording(orphan_id):
    """
    Link an orphan recording to an existing recording instead of creating a new one.

    This is used when the orphan is the same performance as an existing recording,
    just appearing on a different release (compilation, reissue, etc.).

    The orphan's MB release will be added as a new recording_releases entry
    for the existing recording.
    """
    data = request.get_json() or {}
    recording_id = data.get('recording_id')

    if not recording_id:
        return jsonify({'error': 'recording_id is required'}), 400

    try:
        with get_db_connection() as db:
            with db.cursor() as cur:
                # Get the orphan
                cur.execute("""
                    SELECT id, song_id, mb_recording_id, mb_recording_title, mb_artist_credit,
                           mb_releases, spotify_track_id, spotify_external_url,
                           spotify_matched_mb_release_id, status
                    FROM orphan_recordings
                    WHERE id = %s
                """, (orphan_id,))
                orphan = cur.fetchone()

                if not orphan:
                    return jsonify({'error': 'Orphan not found'}), 404

                orphan = dict(orphan)

                # Verify the recording exists and belongs to the same song
                cur.execute("""
                    SELECT id, song_id, musicbrainz_id FROM recordings WHERE id = %s
                """, (recording_id,))
                recording = cur.fetchone()

                if not recording:
                    return jsonify({'error': 'Recording not found'}), 404

                if str(recording['song_id']) != str(orphan['song_id']):
                    return jsonify({'error': 'Recording belongs to a different song'}), 400

                # Get the MB release to link (prefer Spotify-matched release, fall back to first release)
                mb_release_id = orphan.get('spotify_matched_mb_release_id')
                if not mb_release_id and orphan.get('mb_releases'):
                    mb_releases = orphan['mb_releases']
                    if mb_releases and len(mb_releases) > 0:
                        mb_release_id = mb_releases[0].get('id')

                if not mb_release_id:
                    return jsonify({'error': 'No MB release found for orphan'}), 400

                # Find or create the release
                release_id, is_new = _find_or_create_release_with_caa(db, mb_release_id, orphan)

                if not release_id:
                    return jsonify({'error': 'Could not find or create release'}), 500

                # Create recording_releases entry linking existing recording to this release
                cur.execute("""
                    INSERT INTO recording_releases (
                        recording_id, release_id, track_title, track_artist_credit
                    )
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (recording_id, release_id) DO NOTHING
                    RETURNING id
                """, (
                    recording_id,
                    release_id,
                    orphan.get('mb_recording_title'),
                    orphan.get('mb_artist_credit')
                ))
                rr_result = cur.fetchone()

                # If we have a Spotify track ID, add it to the streaming links table
                spotify_track_id = orphan.get('spotify_track_id')
                if spotify_track_id:
                    # Get the recording_release_id (might not have been returned if it already existed)
                    if not rr_result:
                        cur.execute("""
                            SELECT id FROM recording_releases
                            WHERE recording_id = %s AND release_id = %s
                        """, (recording_id, release_id))
                        rr_result = cur.fetchone()
                    if rr_result:
                        # Check for manual override - don't overwrite manually added links
                        if is_track_manual_override(db, rr_result['id'], 'spotify'):
                            logger.debug(f"Skipping Spotify update - manual override exists")
                        else:
                            service_url = f'https://open.spotify.com/track/{spotify_track_id}'
                            cur.execute("""
                                INSERT INTO recording_release_streaming_links (
                                    recording_release_id, service, service_id, service_url,
                                    match_method, matched_at
                                )
                                VALUES (%s, 'spotify', %s, %s, 'orphan_import', CURRENT_TIMESTAMP)
                                ON CONFLICT (recording_release_id, service) DO UPDATE
                                SET service_id = EXCLUDED.service_id,
                                    service_url = EXCLUDED.service_url,
                                    match_method = EXCLUDED.match_method,
                                    matched_at = CURRENT_TIMESTAMP,
                                    updated_at = CURRENT_TIMESTAMP
                                WHERE recording_release_streaming_links.match_method != 'manual'
                                   OR recording_release_streaming_links.match_method IS NULL
                            """, (rr_result['id'], spotify_track_id, service_url))

                # Set default_release_id if recording doesn't have one, or if this release
                # has Spotify data and the current default doesn't
                has_spotify = bool(orphan.get('spotify_track_id'))
                cur.execute("""
                    UPDATE recordings
                    SET default_release_id = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                      AND (
                          default_release_id IS NULL
                          OR (%s AND NOT EXISTS (
                              SELECT 1 FROM releases rel
                              WHERE rel.id = default_release_id
                                AND rel.spotify_album_id IS NOT NULL
                          ))
                      )
                """, (release_id, recording_id, has_spotify))

                # Update orphan status to 'linked' (a new status) or 'imported'
                cur.execute("""
                    UPDATE orphan_recordings
                    SET status = 'linked',
                        imported_recording_id = %s,
                        imported_at = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP,
                        review_notes = COALESCE(review_notes, '') || ' Linked to existing recording.'
                    WHERE id = %s
                """, (recording_id, orphan_id))

                db.commit()

                logger.info(f"Linked orphan {orphan_id} to existing recording {recording_id} via release {release_id}")

                return jsonify({
                    'success': True,
                    'recording_id': str(recording_id),
                    'release_id': str(release_id),
                    'release_is_new': is_new,
                    'recording_release_id': str(rr_result['id']) if rr_result else None
                })

    except Exception as e:
        logger.error(f"Error linking orphan to recording: {e}")
        return jsonify({'error': str(e)}), 500


# =============================================================================
# AUTHORITY RECOMMENDATIONS REVIEWER
# =============================================================================

@admin_bp.route('/recommendations')
def recommendations_list():
    """
    List songs with authority recommendations and their completion status.
    Shows which songs have unmatched recommendations that need attention.
    Supports filtering by repertoire via query parameter.
    """
    from flask import request

    repertoire_id = request.args.get('repertoire_id', '')

    with get_db_connection() as db:
        with db.cursor() as cur:
            # Fetch all repertoires for the filter dropdown
            cur.execute("""
                SELECT
                    r.id,
                    r.name AS repertoire_name,
                    COALESCE(u.display_name, u.email) AS user_name,
                    COUNT(rs.song_id) AS song_count
                FROM repertoires r
                JOIN users u ON r.user_id = u.id
                LEFT JOIN repertoire_songs rs ON r.id = rs.repertoire_id
                GROUP BY r.id, r.name, u.display_name, u.email
                ORDER BY u.display_name, u.email, r.name
            """)
            repertoires = [dict(row) for row in cur.fetchall()]

            # Build the main query - filter by repertoire if selected
            if repertoire_id:
                cur.execute("""
                    SELECT
                        s.id,
                        s.title,
                        s.composer,
                        s.musicbrainz_id,
                        COUNT(*) AS total_recs,
                        COUNT(sar.recording_id) AS matched_recs,
                        ROUND(COUNT(sar.recording_id)::decimal / COUNT(*) * 100, 1) AS perc_complete,
                        COUNT(*) FILTER (WHERE sar.artist_name IS NULL OR sar.artist_name = '') AS missing_artist,
                        COUNT(*) FILTER (WHERE sar.album_title IS NULL OR sar.album_title = '') AS missing_album,
                        array_agg(DISTINCT sar.source) AS sources
                    FROM songs s
                    JOIN song_authority_recommendations sar ON s.id = sar.song_id
                    JOIN repertoire_songs rs ON s.id = rs.song_id AND rs.repertoire_id = %s
                    GROUP BY s.id, s.title, s.composer, s.musicbrainz_id
                    ORDER BY perc_complete ASC, total_recs DESC
                """, (repertoire_id,))
            else:
                cur.execute("""
                    SELECT
                        s.id,
                        s.title,
                        s.composer,
                        s.musicbrainz_id,
                        COUNT(*) AS total_recs,
                        COUNT(sar.recording_id) AS matched_recs,
                        ROUND(COUNT(sar.recording_id)::decimal / COUNT(*) * 100, 1) AS perc_complete,
                        COUNT(*) FILTER (WHERE sar.artist_name IS NULL OR sar.artist_name = '') AS missing_artist,
                        COUNT(*) FILTER (WHERE sar.album_title IS NULL OR sar.album_title = '') AS missing_album,
                        array_agg(DISTINCT sar.source) AS sources
                    FROM songs s
                    JOIN song_authority_recommendations sar ON s.id = sar.song_id
                    GROUP BY s.id, s.title, s.composer, s.musicbrainz_id
                    ORDER BY perc_complete ASC, total_recs DESC
                """)
            songs = [dict(row) for row in cur.fetchall()]

    return render_template('admin/recommendations_list.html',
                          songs=songs,
                          repertoires=repertoires,
                          selected_repertoire_id=repertoire_id)


@admin_bp.route('/recommendations/<song_id>')
def recommendations_review(song_id):
    """
    Review unmatched authority recommendations for a specific song.
    Shows detailed diagnostic information for each recommendation.
    """
    from flask import request
    repertoire_id = request.args.get('repertoire_id', '')

    with get_db_connection() as db:
        with db.cursor() as cur:
            # Get song info
            cur.execute("""
                SELECT id, title, composer, musicbrainz_id, second_mb_id
                FROM songs WHERE id = %s
            """, (song_id,))
            song = cur.fetchone()

            if not song:
                return "Song not found", 404

            song = dict(song)

            # Get all recommendations for this song
            # Use subquery for performer to avoid duplicates when recording has multiple leaders
            cur.execute("""
                SELECT
                    sar.id,
                    sar.song_id,
                    sar.recording_id,
                    sar.source,
                    sar.recommendation_text,
                    sar.source_url,
                    sar.artist_name,
                    sar.album_title,
                    sar.recording_year,
                    sar.itunes_album_id,
                    sar.itunes_track_id,
                    sar.created_at,
                    -- If matched, get recording info from default release
                    def_rel.title AS matched_album,
                    (
                        SELECT p.name
                        FROM recording_performers rp
                        JOIN performers p ON rp.performer_id = p.id
                        WHERE rp.recording_id = r.id AND rp.role = 'leader'
                        LIMIT 1
                    ) AS matched_performer
                FROM song_authority_recommendations sar
                LEFT JOIN recordings r ON sar.recording_id = r.id
                LEFT JOIN releases def_rel ON r.default_release_id = def_rel.id
                WHERE sar.song_id = %s
                ORDER BY
                    CASE WHEN sar.recording_id IS NULL THEN 0 ELSE 1 END,
                    sar.source,
                    sar.artist_name
            """, (song_id,))
            recommendations = [dict(row) for row in cur.fetchall()]

            # Calculate stats
            total = len(recommendations)
            matched = sum(1 for r in recommendations if r['recording_id'])
            unmatched = total - matched
            missing_artist = sum(1 for r in recommendations
                               if not r['recording_id'] and (not r['artist_name'] or r['artist_name'].strip() == ''))
            missing_album = sum(1 for r in recommendations
                              if not r['recording_id'] and (not r['album_title'] or r['album_title'].strip() == ''))

            stats = {
                'total': total,
                'matched': matched,
                'unmatched': unmatched,
                'missing_artist': missing_artist,
                'missing_album': missing_album,
                'perc_complete': round(matched / total * 100, 1) if total > 0 else 0
            }

    return render_template('admin/recommendations_review.html',
                          song=song,
                          recommendations=recommendations,
                          stats=stats,
                          repertoire_id=repertoire_id)


@admin_bp.route('/recommendations/<song_id>/potential-matches/<rec_id>')
def get_potential_matches(song_id, rec_id):
    """
    Find potential release matches for an unmatched recommendation.
    Searches releases by artist name and album title similarity.
    """
    with get_db_connection() as db:
        with db.cursor() as cur:
            # Get the recommendation
            cur.execute("""
                SELECT id, artist_name, album_title, recording_year
                FROM song_authority_recommendations
                WHERE id = %s AND song_id = %s
            """, (rec_id, song_id))
            rec = cur.fetchone()

            if not rec:
                return jsonify({'error': 'Recommendation not found'}), 404

            rec = dict(rec)
            artist_name = rec.get('artist_name') or ''
            album_title = rec.get('album_title') or ''

            # Search for potential matches in releases
            # Using ILIKE for case-insensitive partial matching
            cur.execute("""
                SELECT DISTINCT
                    rel.id AS release_id,
                    rel.title AS release_title,
                    rel.artist_credit,
                    rel.release_year,
                    rel.musicbrainz_release_id,
                    rel.spotify_album_id,
                    (SELECT ri.image_url_small FROM release_imagery ri
                     WHERE ri.release_id = rel.id AND ri.type = 'Front' LIMIT 1) AS cover_art,
                    r.id AS recording_id,
                    def_rel.title AS recording_album
                FROM releases rel
                LEFT JOIN recording_releases rr ON rel.id = rr.release_id
                LEFT JOIN recordings r ON rr.recording_id = r.id AND r.song_id = %s
                LEFT JOIN releases def_rel ON r.default_release_id = def_rel.id
                WHERE (
                    rel.artist_credit ILIKE %s
                    OR rel.title ILIKE %s
                )
                ORDER BY
                    CASE WHEN r.id IS NOT NULL THEN 0 ELSE 1 END,
                    rel.release_year DESC
                LIMIT 20
            """, (
                song_id,
                f'%{artist_name}%' if artist_name else '%',
                f'%{album_title}%' if album_title else '%'
            ))

            matches = [dict(row) for row in cur.fetchall()]

            # Convert UUIDs to strings
            for m in matches:
                if m.get('release_id'):
                    m['release_id'] = str(m['release_id'])
                if m.get('recording_id'):
                    m['recording_id'] = str(m['recording_id'])

    return jsonify({
        'recommendation': {
            'id': str(rec['id']),
            'artist_name': rec['artist_name'],
            'album_title': rec['album_title'],
            'recording_year': rec['recording_year']
        },
        'matches': matches
    })


@admin_bp.route('/recommendations/<rec_id>/link', methods=['POST'])
def link_recommendation_to_recording(rec_id):
    """
    Manually link an authority recommendation to a recording.
    """
    data = request.get_json() or {}
    recording_id = data.get('recording_id')

    if not recording_id:
        return jsonify({'error': 'recording_id is required'}), 400

    try:
        with get_db_connection() as db:
            with db.cursor() as cur:
                cur.execute("""
                    UPDATE song_authority_recommendations
                    SET recording_id = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                    RETURNING id, recording_id
                """, (recording_id, rec_id))
                result = cur.fetchone()
                db.commit()

                if not result:
                    return jsonify({'error': 'Recommendation not found'}), 404

                return jsonify({
                    'success': True,
                    'recommendation_id': str(result['id']),
                    'recording_id': str(result['recording_id'])
                })

    except Exception as e:
        logger.error(f"Error linking recommendation: {e}")
        return jsonify({'error': str(e)}), 500


@admin_bp.route('/recommendations/<rec_id>/unlink', methods=['POST'])
def unlink_recommendation(rec_id):
    """
    Remove the recording link from an authority recommendation.
    """
    try:
        with get_db_connection() as db:
            with db.cursor() as cur:
                cur.execute("""
                    UPDATE song_authority_recommendations
                    SET recording_id = NULL,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                    RETURNING id
                """, (rec_id,))
                result = cur.fetchone()
                db.commit()

                if not result:
                    return jsonify({'error': 'Recommendation not found'}), 404

                return jsonify({
                    'success': True,
                    'recommendation_id': str(result['id'])
                })

    except Exception as e:
        logger.error(f"Error unlinking recommendation: {e}")
        return jsonify({'error': str(e)}), 500


@admin_bp.route('/recommendations/<song_id>/diagnose', methods=['POST'])
def diagnose_mb_recording(song_id):
    """
    Diagnose why a MusicBrainz recording isn't matching.

    Takes a MusicBrainz recording URL and returns:
    - Is the recording linked to this song's Work in MusicBrainz?
    - Do we have this recording in our database?
    - Do we have its releases?
    - Where does the matching logic fail?
    """
    import re
    import requests
    from rapidfuzz import fuzz

    data = request.get_json() or {}
    mb_url = data.get('url', '').strip()
    rec_id = data.get('recommendation_id')  # Optional: to compare against specific rec

    # Parse recording ID from URL
    # Supports: https://musicbrainz.org/recording/UUID or just UUID
    mb_recording_id = None
    uuid_pattern = r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'

    match = re.search(uuid_pattern, mb_url, re.IGNORECASE)
    if match:
        mb_recording_id = match.group(0).lower()
    else:
        return jsonify({'error': 'Could not parse MusicBrainz recording ID from URL'}), 400

    diagnosis = {
        'mb_recording_id': mb_recording_id,
        'mb_url': f'https://musicbrainz.org/recording/{mb_recording_id}',
        'checks': [],
        'recommendation': None,
        'mb_data': None,
        'our_data': None,
        'issues': [],
        'suggestions': []
    }

    try:
        with get_db_connection() as db:
            with db.cursor() as cur:
                # Get song info including Work IDs (primary and secondary)
                cur.execute("""
                    SELECT id, title, musicbrainz_id, second_mb_id
                    FROM songs WHERE id = %s
                """, (song_id,))
                song = cur.fetchone()

                if not song:
                    return jsonify({'error': 'Song not found'}), 404

                song = dict(song)
                # Collect all valid work IDs for this song
                our_work_ids = [song['musicbrainz_id']] if song['musicbrainz_id'] else []
                if song.get('second_mb_id'):
                    our_work_ids.append(song['second_mb_id'])

                diagnosis['song'] = {
                    'id': str(song['id']),
                    'title': song['title'],
                    'work_id': song['musicbrainz_id'],
                    'second_work_id': song.get('second_mb_id'),
                    'all_work_ids': our_work_ids
                }

                # Get the recommendation if provided
                if rec_id:
                    cur.execute("""
                        SELECT id, artist_name, album_title, recording_year, source
                        FROM song_authority_recommendations
                        WHERE id = %s
                    """, (rec_id,))
                    rec = cur.fetchone()
                    if rec:
                        diagnosis['recommendation'] = dict(rec)
                        diagnosis['recommendation']['id'] = str(rec['id'])

                # ===== CHECK 1: Fetch from MusicBrainz =====
                mb_session = requests.Session()
                mb_session.headers.update({
                    'User-Agent': 'ApproachNote/1.0 (+support@approachnote.com)'
                })

                # Fetch recording with work-rels and releases
                mb_response = mb_session.get(
                    f'https://musicbrainz.org/ws/2/recording/{mb_recording_id}',
                    params={
                        'inc': 'releases+artist-credits+work-rels',
                        'fmt': 'json'
                    },
                    timeout=15
                )

                if mb_response.status_code == 404:
                    diagnosis['checks'].append({
                        'name': 'MB Recording Exists',
                        'passed': False,
                        'detail': 'Recording not found in MusicBrainz'
                    })
                    diagnosis['issues'].append('Recording does not exist in MusicBrainz')
                    return jsonify(diagnosis)

                if mb_response.status_code != 200:
                    diagnosis['checks'].append({
                        'name': 'MB API Call',
                        'passed': False,
                        'detail': f'MusicBrainz API error: {mb_response.status_code}'
                    })
                    return jsonify(diagnosis)

                mb_data = mb_response.json()
                diagnosis['checks'].append({
                    'name': 'MB Recording Exists',
                    'passed': True,
                    'detail': f"Found: {mb_data.get('title')}"
                })

                # Extract MB data
                mb_artist_credit = ' / '.join([
                    ac.get('name', ac.get('artist', {}).get('name', ''))
                    for ac in mb_data.get('artist-credit', [])
                ])
                mb_releases = mb_data.get('releases', [])

                diagnosis['mb_data'] = {
                    'title': mb_data.get('title'),
                    'artist_credit': mb_artist_credit,
                    'releases': [
                        {
                            'id': r.get('id'),
                            'title': r.get('title'),
                            'date': r.get('date'),
                            'country': r.get('country')
                        }
                        for r in mb_releases[:10]  # Limit to first 10
                    ],
                    'total_releases': len(mb_releases)
                }

                # ===== CHECK 2: Is it linked to the Work? =====
                work_relations = mb_data.get('relations', [])
                work_links = [
                    rel for rel in work_relations
                    if rel.get('type') == 'performance' and rel.get('work')
                ]

                linked_to_our_work = False
                matched_work_title = None
                linked_works = []
                for rel in work_links:
                    work = rel.get('work', {})
                    work_id = work.get('id')
                    work_title = work.get('title')
                    # Check against both primary and secondary work IDs
                    is_ours = work_id in our_work_ids
                    linked_works.append({
                        'id': work_id,
                        'title': work_title,
                        'is_ours': is_ours
                    })
                    if is_ours:
                        linked_to_our_work = True
                        matched_work_title = work_title

                diagnosis['mb_data']['linked_works'] = linked_works

                if linked_to_our_work:
                    # Show which work ID matched (primary or secondary)
                    work_note = ""
                    if matched_work_title and matched_work_title.lower() != song['title'].lower():
                        work_note = f" (via alternate title: {matched_work_title})"
                    diagnosis['checks'].append({
                        'name': 'Linked to Work',
                        'passed': True,
                        'detail': f"Recording IS linked to '{song['title']}' in MusicBrainz{work_note}"
                    })
                elif linked_works:
                    diagnosis['checks'].append({
                        'name': 'Linked to Work',
                        'passed': False,
                        'detail': f"Recording is linked to OTHER works: {', '.join([w['title'] for w in linked_works])}"
                    })
                    diagnosis['issues'].append(f"Recording is linked to wrong Work(s) in MusicBrainz: {', '.join([w['title'] for w in linked_works])}")
                    diagnosis['suggestions'].append("Edit MusicBrainz to add a 'performance of' relationship to the correct Work")
                else:
                    diagnosis['checks'].append({
                        'name': 'Linked to Work',
                        'passed': False,
                        'detail': "Recording has NO work relationships in MusicBrainz"
                    })
                    diagnosis['issues'].append("Recording is not linked to any Work in MusicBrainz")
                    diagnosis['suggestions'].append("Edit MusicBrainz to add a 'performance of' relationship to this Work")

                # ===== CHECK 3: Do we have this recording? =====
                cur.execute("""
                    SELECT r.id, def_rel.title as album_title, r.recording_year, r.musicbrainz_id,
                           p.name as leader_name
                    FROM recordings r
                    LEFT JOIN releases def_rel ON r.default_release_id = def_rel.id
                    LEFT JOIN recording_performers rp ON r.id = rp.recording_id AND rp.role = 'leader'
                    LEFT JOIN performers p ON rp.performer_id = p.id
                    WHERE r.musicbrainz_id = %s
                """, (mb_recording_id,))
                our_recording = cur.fetchone()

                if our_recording:
                    our_recording = dict(our_recording)
                    diagnosis['checks'].append({
                        'name': 'In Our Database',
                        'passed': True,
                        'detail': f"We have this recording: {our_recording['leader_name'] or 'Unknown'} - {our_recording['album_title'] or 'Unknown Album'}"
                    })
                    diagnosis['our_data'] = {
                        'recording_id': str(our_recording['id']),
                        'album_title': our_recording['album_title'],
                        'recording_year': our_recording['recording_year'],
                        'leader_name': our_recording['leader_name']
                    }
                else:
                    diagnosis['checks'].append({
                        'name': 'In Our Database',
                        'passed': False,
                        'detail': "We do NOT have this recording in our database"
                    })
                    if linked_to_our_work:
                        diagnosis['issues'].append("Recording is linked to Work but we haven't imported it")
                        diagnosis['suggestions'].append("Re-run the MusicBrainz import for this song to pick up this recording")
                    else:
                        diagnosis['issues'].append("Recording not imported (not linked to Work in MB)")

                # ===== CHECK 4: Do we have the releases? =====
                if mb_releases:
                    mb_release_ids = [r.get('id') for r in mb_releases]
                    placeholders = ','.join(['%s'] * len(mb_release_ids))
                    cur.execute(f"""
                        SELECT musicbrainz_release_id, title, artist_credit
                        FROM releases
                        WHERE musicbrainz_release_id IN ({placeholders})
                    """, mb_release_ids)
                    our_releases = {r['musicbrainz_release_id']: dict(r) for r in cur.fetchall()}

                    matched_releases = []
                    missing_releases = []
                    for mb_rel in mb_releases[:5]:  # Check first 5
                        if mb_rel.get('id') in our_releases:
                            matched_releases.append(mb_rel.get('title'))
                        else:
                            missing_releases.append({
                                'id': mb_rel.get('id'),
                                'title': mb_rel.get('title')
                            })

                    if matched_releases:
                        diagnosis['checks'].append({
                            'name': 'Have Releases',
                            'passed': True,
                            'detail': f"We have {len(matched_releases)} of the releases: {', '.join(matched_releases[:3])}"
                        })
                    else:
                        diagnosis['checks'].append({
                            'name': 'Have Releases',
                            'passed': False,
                            'detail': "We don't have any of this recording's releases"
                        })
                        diagnosis['issues'].append("None of the recording's releases are in our database")

                    diagnosis['our_data'] = diagnosis.get('our_data') or {}
                    diagnosis['our_data']['matched_releases'] = matched_releases
                    diagnosis['our_data']['missing_releases'] = missing_releases[:5]

                # ===== CHECK 5: Compare with recommendation =====
                if diagnosis.get('recommendation'):
                    rec = diagnosis['recommendation']
                    rec_artist = rec.get('artist_name') or ''
                    rec_album = rec.get('album_title') or ''

                    # Artist comparison
                    artist_score = fuzz.ratio(rec_artist.lower(), mb_artist_credit.lower())
                    artist_partial = fuzz.partial_ratio(rec_artist.lower(), mb_artist_credit.lower())

                    diagnosis['checks'].append({
                        'name': 'Artist Match',
                        'passed': artist_score >= 80 or artist_partial >= 90,
                        'detail': f"Rec artist: '{rec_artist}' vs MB: '{mb_artist_credit}' (score: {artist_score}%, partial: {artist_partial}%)"
                    })

                    if artist_score < 80 and artist_partial < 90:
                        diagnosis['issues'].append(f"Artist name mismatch: '{rec_artist}' vs '{mb_artist_credit}'")
                        diagnosis['suggestions'].append("Check if the matcher needs to handle this artist name variation")

                    # Album comparison (against all MB releases)
                    # Use same matching logic as the actual matcher
                    best_album_score = 0
                    best_album_match = None
                    best_album_method = None
                    for mb_rel in mb_releases:
                        rel_title = mb_rel.get('title', '')
                        rec_lower = rec_album.lower()
                        rel_lower = rel_title.lower()

                        # Try multiple fuzzy matching approaches (same as matcher)
                        ratio = fuzz.ratio(rec_lower, rel_lower)
                        token_sort = fuzz.token_sort_ratio(rec_lower, rel_lower)
                        partial = fuzz.partial_ratio(rec_lower, rel_lower)
                        token_set = fuzz.token_set_ratio(rec_lower, rel_lower)

                        # Find best method and score
                        scores = [
                            (ratio, 'ratio'),
                            (token_sort, 'token_sort'),
                            (partial, 'partial'),
                            (token_set, 'token_set')
                        ]
                        best_for_this = max(scores, key=lambda x: x[0])

                        if best_for_this[0] > best_album_score:
                            best_album_score = best_for_this[0]
                            best_album_match = rel_title
                            best_album_method = best_for_this[1]

                    diagnosis['checks'].append({
                        'name': 'Album Match',
                        'passed': best_album_score >= 80,
                        'detail': f"Rec album: '{rec_album}' vs best MB match: '{best_album_match}' (score: {best_album_score:.1f}% via {best_album_method})"
                    })

                    if best_album_score < 80:
                        diagnosis['issues'].append(f"Album title mismatch: '{rec_album}' doesn't match any MB release (best: {best_album_score:.1f}%)")
                        diagnosis['suggestions'].append("The recommendation's album title may be different from MB release titles")

                # ===== Summary =====
                if not diagnosis['issues']:
                    if our_recording:
                        diagnosis['summary'] = "This recording exists in our database. The matcher may need to be re-run or there's a logic issue."
                    else:
                        diagnosis['summary'] = "All checks passed but recording not imported. Try re-importing from MusicBrainz."
                else:
                    diagnosis['summary'] = f"Found {len(diagnosis['issues'])} issue(s) preventing the match."

                return jsonify(diagnosis)

    except Exception as e:
        logger.error(f"Error in diagnosis: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@admin_bp.route('/recommendations/<song_id>/run-matcher', methods=['POST'])
def run_matcher_for_song(song_id):
    """
    Run the authority recommendation matcher for a specific song.
    This re-attempts to match unmatched recommendations to recordings.
    """
    try:
        # Import the matcher class
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).parent.parent / 'scripts'))
        from jazzs_match_authorityrecs import AuthorityRecommendationMatcher

        with get_db_connection() as db:
            with db.cursor() as cur:
                # Get song name
                cur.execute("SELECT title FROM songs WHERE id = %s", (song_id,))
                song = cur.fetchone()

                if not song:
                    return jsonify({'error': 'Song not found'}), 404

                song_name = song['title']

        # Run the matcher for this song
        matcher = AuthorityRecommendationMatcher(
            dry_run=False,
            min_confidence='medium',
            song_name=song_name,
            strategy='performer'
        )
        matcher.run()

        return jsonify({
            'success': True,
            'song_name': song_name,
            'stats': matcher.stats
        })

    except Exception as e:
        logger.error(f"Error running matcher for song: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@admin_bp.route('/recommendations/run-matcher-all', methods=['POST'])
def run_matcher_all():
    """
    Run the authority recommendation matcher for all songs with unmatched recommendations.
    """
    try:
        # Import the matcher class
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).parent.parent / 'scripts'))
        from jazzs_match_authorityrecs import AuthorityRecommendationMatcher

        # Run the matcher for all songs
        matcher = AuthorityRecommendationMatcher(
            dry_run=False,
            min_confidence='medium',
            song_name=None,  # No filter = all songs
            strategy='performer'
        )
        matcher.run()

        return jsonify({
            'success': True,
            'stats': matcher.stats
        })

    except Exception as e:
        logger.error(f"Error running matcher for all songs: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


# ============================================================================
# Apple Music DuckDB Catalog Status
# ============================================================================

@admin_bp.route('/apple-music-catalog')
def apple_music_catalog_status():
    """Read-only status snapshot of the Apple Music DuckDB catalog
    (typically hosted on MotherDuck). Reports backing-store mode,
    SELECT 1 round-trip latency, per-feed export freshness, per-table
    row counts, and recent refresh-chain activity. See
    core/apple_catalog_status.py for the section-by-section gather
    implementation."""
    from core.apple_catalog_status import get_catalog_status
    status = get_catalog_status()
    return render_template(
        'admin/apple_music_catalog.html',
        status=status,
    )


@admin_bp.route('/apple-music-catalog/refresh', methods=['POST'])
def apple_music_catalog_refresh():
    """Enqueue the chained refresh job (albums → songs → artists →
    rebuild_index). The dedup index on research_jobs ensures only one
    chain can be in flight at a time; a duplicate POST returns the
    existing job's id and reports its current status so the caller
    can tell a new chain from a dedup hit."""
    from integrations.apple_music.refresh import enqueue_refresh_chain
    from core import research_jobs
    try:
        job_id = enqueue_refresh_chain()
    except Exception as e:
        logger.exception("Failed to enqueue Apple catalog refresh")
        return jsonify({'error': str(e)}), 500

    if job_id is None:
        return jsonify({'error': 'enqueue collapsed without an id'}), 500

    job = research_jobs.get_job(job_id) or {}
    job_status = job.get('status') or 'unknown'
    # If the job is already running/queued (dedup hit), the user just
    # observed an existing chain; otherwise this call kicked one off.
    already_in_flight = job_status in ('queued', 'running')
    logger.info(
        "Apple catalog refresh request; job id=%s status=%s already_in_flight=%s",
        job_id, job_status, already_in_flight,
    )
    return jsonify({
        'job_id': job_id,
        'started': 'albums',
        'status': job_status,
        'already_in_flight': already_in_flight,
    }), 202


# ============================================================================
# Apple Music Match Admin
# ============================================================================

@admin_bp.route('/apple-matches')
def apple_matches_list():
    """List songs with Apple Music match statistics."""
    with get_db_connection() as db:
        with db.cursor() as cur:
            # Get songs with releases and their Apple Music match status
            cur.execute("""
                SELECT
                    s.id,
                    s.title,
                    s.composer,
                    COUNT(DISTINCT rel.id) as total_releases,
                    COUNT(DISTINCT CASE WHEN rsl.id IS NOT NULL THEN rel.id END) as matched_releases,
                    COUNT(DISTINCT CASE
                        WHEN rsl.id IS NULL AND rel.apple_music_searched_at IS NOT NULL
                        THEN rel.id
                    END) as searched_no_match,
                    COUNT(DISTINCT CASE
                        WHEN rsl.id IS NULL AND rel.apple_music_searched_at IS NULL
                        THEN rel.id
                    END) as not_searched,
                    COUNT(DISTINCT rr.recording_id) as total_recordings,
                    COUNT(DISTINCT CASE WHEN rrsl.id IS NOT NULL THEN rr.id END) as matched_tracks
                FROM songs s
                JOIN recordings rec ON rec.song_id = s.id
                JOIN recording_releases rr ON rr.recording_id = rec.id
                JOIN releases rel ON rr.release_id = rel.id
                LEFT JOIN release_streaming_links rsl
                    ON rel.id = rsl.release_id AND rsl.service = 'apple_music'
                LEFT JOIN recording_release_streaming_links rrsl
                    ON rr.id = rrsl.recording_release_id AND rrsl.service = 'apple_music'
                GROUP BY s.id, s.title, s.composer
                HAVING COUNT(DISTINCT rel.id) > 0
                ORDER BY s.title
            """)
            songs = cur.fetchall()

            # Calculate summary stats
            total_songs = len(songs)
            songs_complete = sum(1 for s in songs if s['matched_releases'] == s['total_releases'])
            songs_partial = sum(1 for s in songs if 0 < s['matched_releases'] < s['total_releases'])
            songs_none = sum(1 for s in songs if s['matched_releases'] == 0)

            summary = {
                'total_songs': total_songs,
                'songs_complete': songs_complete,
                'songs_partial': songs_partial,
                'songs_none': songs_none,
                'total_releases': sum(s['total_releases'] for s in songs),
                'matched_releases': sum(s['matched_releases'] for s in songs),
                'searched_no_match': sum(s['searched_no_match'] for s in songs),
                'not_searched': sum(s['not_searched'] for s in songs),
            }

    return render_template('admin/apple_matches_list.html',
                          songs=songs,
                          summary=summary)


@admin_bp.route('/apple-matches/<song_id>')
def apple_matches_review(song_id):
    """Review Apple Music matches for a specific song."""
    with get_db_connection() as db:
        with db.cursor() as cur:
            # Get song info
            cur.execute("""
                SELECT id, title, composer, musicbrainz_id
                FROM songs WHERE id = %s
            """, (song_id,))
            song = cur.fetchone()

            if not song:
                return "Song not found", 404

            # Get releases with Apple Music status and track details
            cur.execute("""
                SELECT
                    rel.id,
                    rel.title,
                    rel.artist_credit,
                    rel.release_year,
                    rel.musicbrainz_release_id,
                    rel.apple_music_searched_at,
                    rsl.service_id as apple_music_album_id,
                    rsl.service_url as apple_music_url,
                    rsl.id IS NOT NULL as has_apple_music,
                    -- Get cover art
                    (SELECT ri.image_url_small FROM release_imagery ri
                     WHERE ri.release_id = rel.id AND ri.type = 'Front' LIMIT 1) as cover_art,
                    -- Get recordings for this release
                    (SELECT json_agg(
                        json_build_object(
                            'recording_release_id', rr_sub.id,
                            'track_number', rr_sub.track_number,
                            'disc_number', rr_sub.disc_number,
                            'title', COALESCE(rr_sub.track_title, s_sub.title),
                            'has_apple_music', rrsl_sub.id IS NOT NULL,
                            'apple_music_track_id', rrsl_sub.service_id,
                            'apple_music_url', rrsl_sub.service_url
                        ) ORDER BY rr_sub.disc_number, rr_sub.track_number
                    )
                    FROM recording_releases rr_sub
                    JOIN recordings rec_sub ON rr_sub.recording_id = rec_sub.id
                    JOIN songs s_sub ON rec_sub.song_id = s_sub.id
                    LEFT JOIN recording_release_streaming_links rrsl_sub
                        ON rr_sub.id = rrsl_sub.recording_release_id
                        AND rrsl_sub.service = 'apple_music'
                    WHERE rr_sub.release_id = rel.id
                      AND rec_sub.song_id = %s
                    ) as tracks
                FROM releases rel
                JOIN recording_releases rr ON rel.id = rr.release_id
                JOIN recordings rec ON rr.recording_id = rec.id
                LEFT JOIN release_streaming_links rsl
                    ON rel.id = rsl.release_id AND rsl.service = 'apple_music'
                WHERE rec.song_id = %s
                GROUP BY rel.id, rel.title, rel.artist_credit, rel.release_year,
                         rel.musicbrainz_release_id, rel.apple_music_searched_at,
                         rsl.service_id, rsl.service_url, rsl.id
                ORDER BY rel.release_year, rel.title
            """, (song_id, song_id))
            releases = cur.fetchall()

            # Calculate stats
            stats = {
                'total_releases': len(releases),
                'matched_releases': sum(1 for r in releases if r['has_apple_music']),
                'searched_no_match': sum(1 for r in releases
                    if not r['has_apple_music'] and r['apple_music_searched_at']),
                'not_searched': sum(1 for r in releases
                    if not r['has_apple_music'] and not r['apple_music_searched_at']),
                'total_tracks': sum(len(r['tracks'] or []) for r in releases),
                'matched_tracks': sum(
                    sum(1 for t in (r['tracks'] or []) if t['has_apple_music'])
                    for r in releases
                ),
            }

    return render_template('admin/apple_matches_review.html',
                          song=song,
                          releases=releases,
                          stats=stats)


@admin_bp.route('/apple-matches/<song_id>/run-matcher', methods=['POST'])
def run_apple_matcher_for_song(song_id):
    """Run the Apple Music matcher for a specific song."""
    try:
        from integrations.apple_music.matcher import AppleMusicMatcher

        with get_db_connection() as db:
            with db.cursor() as cur:
                cur.execute("SELECT title FROM songs WHERE id = %s", (song_id,))
                song = cur.fetchone()
                if not song:
                    return jsonify({'error': 'Song not found'}), 404
                song_name = song['title']

        # Check for local-only mode from request
        local_only = request.json.get('local_only', False) if request.is_json else False

        matcher = AppleMusicMatcher(
            dry_run=False,
            strict_mode=True,
            rematch=False,
            local_catalog_only=local_only,
            logger=logger
        )

        result = matcher.match_song(song_name)

        return jsonify({
            'success': result.get('success', False),
            'song_name': song_name,
            'stats': result.get('stats', {})
        })

    except Exception as e:
        logger.error(f"Error running Apple matcher for song: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@admin_bp.route('/apple-matches/<song_id>/diagnose', methods=['POST'])
def diagnose_apple_match(song_id):
    """
    Diagnose why an Apple Music album didn't match a release.

    Takes an Apple Music URL and compares it against our releases.
    """
    import re
    from rapidfuzz import fuzz

    try:
        data = request.get_json()
        apple_url = data.get('url', '').strip()
        release_id = data.get('release_id')

        if not apple_url:
            return jsonify({'error': 'Apple Music URL is required'}), 400

        # Parse Apple Music URL to extract album ID
        # Formats:
        # https://music.apple.com/us/album/kind-of-blue/268443092
        # https://music.apple.com/us/album/1440851918
        album_id_match = re.search(r'/album/[^/]*/(\d+)|/album/(\d+)', apple_url)
        if not album_id_match:
            return jsonify({'error': 'Could not parse Apple Music album ID from URL'}), 400

        album_id = album_id_match.group(1) or album_id_match.group(2)

        diagnosis = {
            'url': apple_url,
            'album_id': album_id,
            'checks': [],
            'apple_music_data': None,
            'our_release': None,
            'comparison': None,
            'suggestions': []
        }

        # Try iTunes API first (returns English names, more reliable for comparison)
        try:
            from integrations.apple_music.client import AppleMusicClient
            client = AppleMusicClient()

            album_data = client.lookup_album(album_id)
            if album_data:
                diagnosis['checks'].append({
                    'name': 'Album via iTunes API',
                    'passed': True,
                    'message': 'Found via iTunes API lookup'
                })
                diagnosis['apple_music_data'] = {
                    'id': album_data.get('id'),
                    'name': album_data.get('name'),
                    'artist': album_data.get('artist'),
                    'release_date': album_data.get('release_date'),
                    'track_count': album_data.get('track_count'),
                }

                # Get album tracks via iTunes API
                tracks = client.lookup_album_tracks(album_id)
                if tracks:
                    diagnosis['apple_music_data']['tracks'] = [
                        {'name': t.get('name'), 'track_number': t.get('track_number')}
                        for t in tracks[:10]
                    ]
                    if len(tracks) > 10:
                        diagnosis['apple_music_data']['tracks_truncated'] = True
            else:
                diagnosis['checks'].append({
                    'name': 'Album via iTunes API',
                    'passed': False,
                    'message': 'Not found via iTunes API'
                })
        except Exception as e:
            diagnosis['checks'].append({
                'name': 'Album via iTunes API',
                'passed': False,
                'message': f'API lookup failed: {str(e)}'
            })

        # If iTunes API failed, try local catalog as fallback
        if not diagnosis['apple_music_data']:
            try:
                from integrations.apple_music.feed import AppleMusicCatalog
                catalog = AppleMusicCatalog()

                album_data = catalog.get_album_by_id(album_id)

                if album_data:
                    diagnosis['checks'].append({
                        'name': 'Album in local catalog',
                        'passed': True,
                        'message': 'Found in local catalog (note: may have localized names)'
                    })
                    diagnosis['apple_music_data'] = {
                        'id': album_data.get('id'),
                        'name': album_data.get('name'),
                        'artist': album_data.get('artistName'),
                        'release_date': album_data.get('releaseDate'),
                        'track_count': album_data.get('trackCount'),
                    }
                    # Warn about potential localization
                    diagnosis['suggestions'].append(
                        'Data from local catalog may have localized artist names. '
                        'iTunes API lookup was not available.'
                    )

                    # Get tracks
                    tracks = catalog.get_songs_for_album(album_id)
                    if tracks:
                        diagnosis['apple_music_data']['tracks'] = [
                            {'name': t.get('name'), 'track_number': t.get('trackNumber')}
                            for t in tracks[:10]
                        ]
                        if len(tracks) > 10:
                            diagnosis['apple_music_data']['tracks_truncated'] = True
                else:
                    diagnosis['checks'].append({
                        'name': 'Album in local catalog',
                        'passed': False,
                        'message': 'Not found in local catalog either'
                    })
                    diagnosis['suggestions'].append('Album not found in iTunes API or local catalog.')
            except Exception as e:
                diagnosis['checks'].append({
                    'name': 'Album in local catalog',
                    'passed': False,
                    'message': f'Error accessing catalog: {str(e)}'
                })

        # Get our release data for comparison
        if release_id:
            with get_db_connection() as db:
                with db.cursor() as cur:
                    cur.execute("""
                        SELECT
                            rel.id, rel.title, rel.artist_credit, rel.release_year,
                            rel.apple_music_searched_at
                        FROM releases rel
                        WHERE rel.id = %s
                    """, (release_id,))
                    release = cur.fetchone()

                    if release:
                        diagnosis['our_release'] = {
                            'id': str(release['id']),
                            'title': release['title'],
                            'artist': release['artist_credit'],
                            'year': release['release_year'],
                            'searched_at': str(release['apple_music_searched_at']) if release['apple_music_searched_at'] else None
                        }

        # Compare if we have both
        if diagnosis['apple_music_data'] and diagnosis['our_release']:
            from integrations.spotify.matching import normalize_for_comparison, is_substring_title_match

            am = diagnosis['apple_music_data']
            our = diagnosis['our_release']

            # Normalize names (strips feat., remastered, live annotations, etc.)
            am_artist_norm = normalize_for_comparison(am.get('artist') or '')
            our_artist_norm = normalize_for_comparison(our.get('artist') or '')
            am_album_norm = normalize_for_comparison(am.get('name') or '')
            our_album_norm = normalize_for_comparison(our.get('title') or '')

            # Calculate similarities on normalized names
            artist_sim = fuzz.ratio(am_artist_norm, our_artist_norm)
            album_sim = fuzz.ratio(am_album_norm, our_album_norm)

            # Partial ratio (handles substrings better)
            artist_partial = fuzz.partial_ratio(am_artist_norm, our_artist_norm)
            album_partial = fuzz.partial_ratio(am_album_norm, our_album_norm)

            # Check substring matching (fallback used by actual matcher)
            artist_substring = is_substring_title_match(am.get('artist') or '', our.get('artist') or '')
            album_substring = is_substring_title_match(am.get('name') or '', our.get('title') or '')

            diagnosis['comparison'] = {
                'artist': {
                    'apple_music': am.get('artist'),
                    'our_release': our.get('artist'),
                    'normalized_apple': am_artist_norm,
                    'normalized_ours': our_artist_norm,
                    'similarity': artist_sim,
                    'partial_similarity': artist_partial,
                    'substring_match': artist_substring,
                },
                'album': {
                    'apple_music': am.get('name'),
                    'our_release': our.get('title'),
                    'normalized_apple': am_album_norm,
                    'normalized_ours': our_album_norm,
                    'similarity': album_sim,
                    'partial_similarity': album_partial,
                    'substring_match': album_substring,
                },
                'year': {
                    'apple_music': am.get('release_date', '')[:4] if am.get('release_date') else None,
                    'our_release': our.get('year'),
                }
            }

            # Add diagnosis based on similarities
            # Default thresholds: artist >= 65%, album >= 65% (with substring fallback)
            artist_passes = artist_sim >= 65 or artist_substring
            album_passes = album_sim >= 65 or album_substring

            if artist_sim >= 65:
                diagnosis['checks'].append({
                    'name': 'Artist name match',
                    'passed': True,
                    'message': f'Artist similarity {artist_sim}% (normalized)'
                })
            elif artist_substring:
                diagnosis['checks'].append({
                    'name': 'Artist name match',
                    'passed': True,
                    'message': f'Artist similarity {artist_sim}% but substring match passes'
                })
            else:
                diagnosis['checks'].append({
                    'name': 'Artist name match',
                    'passed': False,
                    'message': f'Artist similarity {artist_sim}% is below threshold (65%) and no substring match'
                })
                diagnosis['suggestions'].append(
                    f'Artist names differ significantly: "{am.get("artist")}" vs "{our.get("artist")}"'
                )

            if album_sim >= 65:
                diagnosis['checks'].append({
                    'name': 'Album name match',
                    'passed': True,
                    'message': f'Album similarity {album_sim}% (normalized)'
                })
            elif album_substring:
                diagnosis['checks'].append({
                    'name': 'Album name match',
                    'passed': True,
                    'message': f'Album similarity {album_sim}% but substring match passes'
                })
            else:
                diagnosis['checks'].append({
                    'name': 'Album name match',
                    'passed': False,
                    'message': f'Album similarity {album_sim}% is below threshold (65%) and no substring match'
                })
                diagnosis['suggestions'].append(
                    f'Album names differ: "{am.get("name")}" vs "{our.get("title")}"'
                )

            # Check if it would match with current thresholds (including substring fallback)
            would_match = artist_passes and album_passes
            diagnosis['would_match'] = would_match

            if not would_match:
                diagnosis['suggestions'].append(
                    'This album would not match with current thresholds. Consider manual linking.'
                )

        return jsonify(diagnosis)

    except Exception as e:
        logger.error(f"Error diagnosing Apple match: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


# =============================================================================
# STREAMING AVAILABILITY
# =============================================================================

@admin_bp.route('/streaming-availability')
def streaming_availability():
    """
    Get streaming availability statistics for songs.

    Shows for each song:
    - Total recordings
    - Recordings with Spotify track links
    - Recordings with Apple Music track links
    - Recordings with both
    - Recordings with neither

    Query params:
    - repertoire_id: Filter to songs in a specific repertoire
    - filter: 'no_playable' | 'spotify_only' | 'apple_only' | 'catalog_diff' | 'all' (default: all)
    - sort: 'title' | 'total' | 'playable' | 'spotify' | 'apple' | 'missing' (default: title)
    - order: 'asc' | 'desc' (default: asc)
    """
    repertoire_id = request.args.get('repertoire_id')
    filter_type = request.args.get('filter', 'all')
    sort_by = request.args.get('sort', 'title')
    order = request.args.get('order', 'asc')

    with get_db_connection() as db:
        with db.cursor() as cur:
            # Build the base query
            # We need to count recordings that have track-level streaming links
            #
            # DATA MODELS:
            # - Both Spotify and Apple Music use recording_release_streaming_links
            # - Spotify also has legacy spotify_track_id column (checked for backwards compatibility)
            #
            # First aggregate streaming availability at the recording level,
            # then count recordings per song. This avoids double-counting when
            # a recording has multiple releases with different streaming status.
            query = """
                WITH recording_streaming AS (
                    -- Determine if each recording has Spotify/Apple across ANY of its releases
                    SELECT
                        r.id as recording_id,
                        r.song_id,
                        -- Has Spotify: check streaming_links table
                        BOOL_OR(rrsl_spotify.id IS NOT NULL) as has_spotify,
                        -- Has Apple if ANY release has apple music link
                        BOOL_OR(rrsl_apple.id IS NOT NULL) as has_apple
                    FROM recordings r
                    LEFT JOIN recording_releases rr ON rr.recording_id = r.id
                    LEFT JOIN recording_release_streaming_links rrsl_spotify
                        ON rrsl_spotify.recording_release_id = rr.id
                        AND rrsl_spotify.service = 'spotify'
                    LEFT JOIN recording_release_streaming_links rrsl_apple
                        ON rrsl_apple.recording_release_id = rr.id
                        AND rrsl_apple.service = 'apple_music'
                    GROUP BY r.id, r.song_id
                ),
                song_recording_counts AS (
                    SELECT
                        s.id as song_id,
                        s.title,
                        s.composer,
                        COUNT(rs.recording_id) as total_recordings,
                        COUNT(CASE WHEN rs.has_spotify THEN 1 END) as spotify_recordings,
                        COUNT(CASE WHEN rs.has_apple THEN 1 END) as apple_recordings,
                        COUNT(CASE WHEN rs.has_spotify AND rs.has_apple THEN 1 END) as both_recordings,
                        COUNT(CASE WHEN rs.has_spotify OR rs.has_apple THEN 1 END) as any_playable_recordings,
                        COUNT(CASE WHEN NOT rs.has_spotify AND NOT rs.has_apple THEN 1 END) as no_streaming_recordings,
                        COUNT(CASE WHEN rs.has_spotify AND NOT rs.has_apple THEN 1 END) as spotify_only_recordings,
                        COUNT(CASE WHEN NOT rs.has_spotify AND rs.has_apple THEN 1 END) as apple_only_recordings
                    FROM songs s
                    LEFT JOIN recording_streaming rs ON rs.song_id = s.id
            """

            # Add repertoire join if filtering
            params = []
            if repertoire_id:
                query += """
                    INNER JOIN repertoire_songs repsongs ON repsongs.song_id = s.id
                    WHERE repsongs.repertoire_id = %s
                """
                params.append(repertoire_id)

            query += """
                    GROUP BY s.id, s.title, s.composer
                )
                SELECT * FROM song_recording_counts
            """

            # Add filter conditions
            if filter_type == 'no_playable':
                query += " WHERE any_playable_recordings = 0 AND total_recordings > 0"
            elif filter_type == 'spotify_only':
                query += " WHERE spotify_only_recordings > 0"
            elif filter_type == 'apple_only':
                query += " WHERE apple_only_recordings > 0"
            elif filter_type == 'catalog_diff':
                query += " WHERE spotify_only_recordings > 0 OR apple_only_recordings > 0"
            # 'all' has no filter

            # Add sorting
            sort_column = {
                'title': 'title',
                'total': 'total_recordings',
                'playable': 'any_playable_recordings',
                'spotify': 'spotify_recordings',
                'apple': 'apple_recordings',
                'missing': 'no_streaming_recordings',
            }.get(sort_by, 'title')

            order_dir = 'DESC' if order == 'desc' else 'ASC'
            query += f" ORDER BY {sort_column} {order_dir}"

            cur.execute(query, params)
            songs = [dict(row) for row in cur.fetchall()]

            # Calculate summary stats
            summary = {
                'total_songs': len(songs),
                'songs_with_no_playable': sum(1 for s in songs if s['any_playable_recordings'] == 0 and s['total_recordings'] > 0),
                'songs_with_catalog_diff': sum(1 for s in songs if s['spotify_only_recordings'] > 0 or s['apple_only_recordings'] > 0),
                'total_recordings': sum(s['total_recordings'] for s in songs),
                'total_spotify': sum(s['spotify_recordings'] for s in songs),
                'total_apple': sum(s['apple_recordings'] for s in songs),
                'total_both': sum(s['both_recordings'] for s in songs),
                'total_neither': sum(s['no_streaming_recordings'] for s in songs),
            }

            # Get available repertoires for the filter dropdown
            cur.execute("""
                SELECT r.id, r.name, COUNT(rs.song_id) as song_count
                FROM repertoires r
                LEFT JOIN repertoire_songs rs ON r.id = rs.repertoire_id
                GROUP BY r.id, r.name
                ORDER BY r.name
            """)
            repertoires = [dict(row) for row in cur.fetchall()]

    # Check if JSON requested
    if request.headers.get('Accept') == 'application/json' or request.args.get('format') == 'json':
        return jsonify({
            'songs': songs,
            'summary': summary,
            'repertoires': repertoires,
            'filters': {
                'repertoire_id': repertoire_id,
                'filter': filter_type,
                'sort': sort_by,
                'order': order
            }
        })

    # Otherwise return HTML template
    return render_template(
        'admin/streaming_availability.html',
        songs=songs,
        summary=summary,
        repertoires=repertoires,
        current_repertoire=repertoire_id,
        current_filter=filter_type,
        current_sort=sort_by,
        current_order=order
    )


# =============================================================================
# SPOTIFY DIAGNOSTICS
# =============================================================================

@admin_bp.route('/streaming-diagnostics/<song_id>')
def streaming_diagnostics(song_id):
    """
    Diagnostic page showing streaming availability for all recordings of a song.
    Shows recordings with their releases and Spotify/Apple Music status.

    Query params:
        filter: 'all' | 'spotify' | 'apple' | 'both' | 'neither' (default: all)
    """
    filter_type = request.args.get('filter', 'all')

    with get_db_connection() as db:
        with db.cursor() as cur:
            # Get song info
            cur.execute("""
                SELECT id, title, composer, musicbrainz_id
                FROM songs
                WHERE id = %s
            """, (song_id,))
            song = cur.fetchone()

            if not song:
                return "Song not found", 404

            # Get all recordings with streaming status aggregated across releases
            cur.execute("""
                SELECT
                    r.id as recording_id,
                    def_rel.title as album_title,
                    r.recording_year,
                    r.musicbrainz_id as recording_mb_id,
                    r.default_release_id,
                    def_rel.title as default_release_title,
                    def_rel.artist_credit as default_release_artist,
                    def_rel.musicbrainz_release_id as default_release_mb_id,
                    -- Count releases
                    (SELECT COUNT(*) FROM recording_releases rr WHERE rr.recording_id = r.id) as release_count,
                    -- Spotify: has track if ANY release has spotify link
                    EXISTS(SELECT 1 FROM recording_releases rr
                           JOIN recording_release_streaming_links rrsl ON rrsl.recording_release_id = rr.id
                           WHERE rr.recording_id = r.id AND rrsl.service = 'spotify') as has_spotify,
                    -- Apple: has track if ANY release has apple music link
                    EXISTS(SELECT 1 FROM recording_releases rr
                           JOIN recording_release_streaming_links rrsl ON rrsl.recording_release_id = rr.id
                           WHERE rr.recording_id = r.id AND rrsl.service = 'apple_music') as has_apple
                FROM recordings r
                LEFT JOIN releases def_rel ON r.default_release_id = def_rel.id
                WHERE r.song_id = %s
                ORDER BY r.recording_year, def_rel.title
            """, (song_id,))
            all_recordings = cur.fetchall()

            # Apply filter
            if filter_type == 'spotify':
                recordings = [r for r in all_recordings if r['has_spotify']]
            elif filter_type == 'apple':
                recordings = [r for r in all_recordings if r['has_apple']]
            elif filter_type == 'both':
                recordings = [r for r in all_recordings if r['has_spotify'] and r['has_apple']]
            elif filter_type == 'neither':
                recordings = [r for r in all_recordings if not r['has_spotify'] and not r['has_apple']]
            else:
                recordings = all_recordings

            # For each recording, get all releases with streaming info
            recordings_with_releases = []
            for rec in recordings:
                cur.execute("""
                    SELECT
                        rel.id as release_id,
                        rr.id as recording_release_id,
                        rel.title,
                        rel.artist_credit,
                        rel.release_year,
                        rel.musicbrainz_release_id as release_mb_id,
                        -- Spotify album (on release)
                        rel.spotify_album_id,
                        -- Spotify track from streaming links table
                        rrsl_spotify.service_id as spotify_track_id,
                        rrsl_spotify.service_url as spotify_track_url,
                        rr.disc_number,
                        rr.track_number,
                        CASE WHEN rel.id = %s THEN true ELSE false END as is_default,
                        -- Apple album (from release_streaming_links)
                        rsl_apple.service_id as apple_album_id,
                        rsl_apple.service_url as apple_album_url,
                        -- Apple track (from recording_release_streaming_links)
                        rrsl_apple.service_id as apple_track_id,
                        rrsl_apple.service_url as apple_track_url
                    FROM recording_releases rr
                    JOIN releases rel ON rr.release_id = rel.id
                    LEFT JOIN recording_release_streaming_links rrsl_spotify
                        ON rrsl_spotify.recording_release_id = rr.id AND rrsl_spotify.service = 'spotify'
                    LEFT JOIN release_streaming_links rsl_apple
                        ON rsl_apple.release_id = rel.id AND rsl_apple.service = 'apple_music'
                    LEFT JOIN recording_release_streaming_links rrsl_apple
                        ON rrsl_apple.recording_release_id = rr.id AND rrsl_apple.service = 'apple_music'
                    WHERE rr.recording_id = %s
                    ORDER BY
                        CASE WHEN rel.id = %s THEN 0 ELSE 1 END,
                        rel.release_year,
                        rel.title
                """, (rec['default_release_id'], rec['recording_id'], rec['default_release_id']))
                releases = cur.fetchall()

                recordings_with_releases.append({
                    'recording': rec,
                    'releases': releases
                })

            # Summary stats (from all recordings, not filtered)
            total_recordings = len(all_recordings)
            with_spotify = sum(1 for r in all_recordings if r['has_spotify'])
            with_apple = sum(1 for r in all_recordings if r['has_apple'])
            with_both = sum(1 for r in all_recordings if r['has_spotify'] and r['has_apple'])
            with_neither = sum(1 for r in all_recordings if not r['has_spotify'] and not r['has_apple'])

            summary = {
                'total_recordings': total_recordings,
                'with_spotify': with_spotify,
                'with_apple': with_apple,
                'with_both': with_both,
                'with_neither': with_neither,
                'filtered_count': len(recordings)
            }

    return render_template(
        'admin/streaming_diagnostics.html',
        song=song,
        recordings=recordings_with_releases,
        summary=summary,
        current_filter=filter_type
    )


# Keep old route for backwards compatibility
@admin_bp.route('/spotify-diagnostics/<song_id>')
def spotify_diagnostics_redirect(song_id):
    """Redirect old URL to new streaming diagnostics page."""
    return redirect(url_for('admin.streaming_diagnostics', song_id=song_id, **request.args))


# =============================================================================
# SONG RECORDINGS RESET
# =============================================================================

@admin_bp.route('/song-reset')
def song_reset_list():
    """
    List all songs with search capability for resetting their recordings.
    Shows recording count and related data counts for each song.
    """
    search_query = request.args.get('q', '').strip()

    with get_db_connection() as db:
        with db.cursor() as cur:
            if search_query:
                # Search by song title (case-insensitive, partial match)
                cur.execute("""
                    SELECT
                        s.id,
                        s.title,
                        s.composer,
                        s.musicbrainz_id,
                        COUNT(DISTINCT r.id) as recording_count,
                        COUNT(DISTINCT rr.release_id) as release_count,
                        COUNT(DISTINCT sar.id) as authority_rec_count
                    FROM songs s
                    LEFT JOIN recordings r ON r.song_id = s.id
                    LEFT JOIN recording_releases rr ON rr.recording_id = r.id
                    LEFT JOIN song_authority_recommendations sar ON sar.recording_id = r.id
                    WHERE LOWER(s.title) LIKE LOWER(%s)
                    GROUP BY s.id, s.title, s.composer, s.musicbrainz_id
                    ORDER BY s.title
                    LIMIT 100
                """, (f'%{search_query}%',))
            else:
                # Show songs with recordings, ordered by recording count
                cur.execute("""
                    SELECT
                        s.id,
                        s.title,
                        s.composer,
                        s.musicbrainz_id,
                        COUNT(DISTINCT r.id) as recording_count,
                        COUNT(DISTINCT rr.release_id) as release_count,
                        COUNT(DISTINCT sar.id) as authority_rec_count
                    FROM songs s
                    LEFT JOIN recordings r ON r.song_id = s.id
                    LEFT JOIN recording_releases rr ON rr.recording_id = r.id
                    LEFT JOIN song_authority_recommendations sar ON sar.recording_id = r.id
                    GROUP BY s.id, s.title, s.composer, s.musicbrainz_id
                    HAVING COUNT(DISTINCT r.id) > 0
                    ORDER BY s.title
                    LIMIT 200
                """)
            songs = [dict(row) for row in cur.fetchall()]

    return render_template('admin/song_reset_list.html',
                          songs=songs,
                          search_query=search_query)


@admin_bp.route('/song-reset/<song_id>')
def song_reset_detail(song_id):
    """
    Show details of what will be deleted for a song before confirmation.
    """
    with get_db_connection() as db:
        with db.cursor() as cur:
            # Get song info
            cur.execute("""
                SELECT id, title, composer, musicbrainz_id
                FROM songs WHERE id = %s
            """, (song_id,))
            song = cur.fetchone()

            if not song:
                return "Song not found", 404

            song = dict(song)

            # Get recordings with their release info
            cur.execute("""
                SELECT
                    r.id,
                    r.musicbrainz_id,
                    r.recording_year,
                    def_rel.title as album_title,
                    def_rel.artist_credit,
                    (SELECT COUNT(*) FROM recording_releases rr WHERE rr.recording_id = r.id) as release_count,
                    (SELECT COUNT(*) FROM recording_performers rp WHERE rp.recording_id = r.id) as performer_count
                FROM recordings r
                LEFT JOIN releases def_rel ON r.default_release_id = def_rel.id
                WHERE r.song_id = %s
                ORDER BY r.recording_year, def_rel.title
            """, (song_id,))
            recordings = [dict(row) for row in cur.fetchall()]

            # Count releases that will become orphaned
            cur.execute("""
                SELECT COUNT(DISTINCT rel.id) as orphan_release_count
                FROM releases rel
                JOIN recording_releases rr ON rr.release_id = rel.id
                JOIN recordings r ON rr.recording_id = r.id
                WHERE r.song_id = %s
                  AND NOT EXISTS (
                      SELECT 1 FROM recording_releases rr2
                      JOIN recordings r2 ON rr2.recording_id = r2.id
                      WHERE rr2.release_id = rel.id
                        AND r2.song_id != %s
                  )
            """, (song_id, song_id))
            orphan_releases = cur.fetchone()['orphan_release_count']

            # Count authority recommendations that will be unlinked
            cur.execute("""
                SELECT COUNT(*) as rec_count
                FROM song_authority_recommendations sar
                JOIN recordings r ON sar.recording_id = r.id
                WHERE r.song_id = %s
            """, (song_id,))
            authority_recs = cur.fetchone()['rec_count']

            stats = {
                'recording_count': len(recordings),
                'orphan_release_count': orphan_releases,
                'authority_rec_count': authority_recs
            }

    return render_template('admin/song_reset_detail.html',
                          song=song,
                          recordings=recordings,
                          stats=stats)


@admin_bp.route('/song-reset/<song_id>/execute', methods=['POST'])
def song_reset_execute(song_id):
    """
    Execute the reset: remove all recordings and related data for a song.

    This will:
    1. Unlink authority recommendations from recordings (set recording_id = NULL)
    2. Delete recording_releases entries for this song's recordings
    3. Delete orphaned releases (releases with no remaining recording links)
    4. Delete recording_performers entries (cascades from recording delete)
    5. Delete recordings for this song
    """
    try:
        with get_db_connection() as db:
            with db.cursor() as cur:
                # Get song info for logging
                cur.execute("""
                    SELECT id, title FROM songs WHERE id = %s
                """, (song_id,))
                song = cur.fetchone()

                if not song:
                    return jsonify({'error': 'Song not found'}), 404

                song_title = song['title']

                # Get recording IDs for this song
                cur.execute("""
                    SELECT id FROM recordings WHERE song_id = %s
                """, (song_id,))
                recording_ids = [row['id'] for row in cur.fetchall()]

                if not recording_ids:
                    return jsonify({
                        'success': True,
                        'message': 'No recordings to delete',
                        'deleted': {
                            'recordings': 0,
                            'releases': 0,
                            'authority_recs_unlinked': 0
                        }
                    })

                # 1. Unlink authority recommendations (don't delete, just unlink)
                cur.execute("""
                    UPDATE song_authority_recommendations
                    SET recording_id = NULL, updated_at = CURRENT_TIMESTAMP
                    WHERE recording_id = ANY(%s)
                    RETURNING id
                """, (recording_ids,))
                unlinked_recs = len(cur.fetchall())
                logger.info(f"Unlinked {unlinked_recs} authority recommendations for '{song_title}'")

                # 2. Find releases that will become orphaned after we delete recording_releases
                cur.execute("""
                    SELECT DISTINCT rel.id
                    FROM releases rel
                    JOIN recording_releases rr ON rr.release_id = rel.id
                    WHERE rr.recording_id = ANY(%s)
                      AND NOT EXISTS (
                          SELECT 1 FROM recording_releases rr2
                          WHERE rr2.release_id = rel.id
                            AND rr2.recording_id != ALL(%s)
                      )
                """, (recording_ids, recording_ids))
                orphan_release_ids = [row['id'] for row in cur.fetchall()]

                # 3. Delete recording_releases entries
                cur.execute("""
                    DELETE FROM recording_releases
                    WHERE recording_id = ANY(%s)
                    RETURNING id
                """, (recording_ids,))
                deleted_rr = len(cur.fetchall())
                logger.info(f"Deleted {deleted_rr} recording_releases entries for '{song_title}'")

                # 4. Delete orphaned releases
                deleted_releases = 0
                if orphan_release_ids:
                    cur.execute("""
                        DELETE FROM releases
                        WHERE id = ANY(%s)
                        RETURNING id
                    """, (orphan_release_ids,))
                    deleted_releases = len(cur.fetchall())
                    logger.info(f"Deleted {deleted_releases} orphaned releases for '{song_title}'")

                # 5. Delete recordings (recording_performers cascade deletes automatically)
                cur.execute("""
                    DELETE FROM recordings
                    WHERE song_id = %s
                    RETURNING id
                """, (song_id,))
                deleted_recordings = len(cur.fetchall())
                logger.info(f"Deleted {deleted_recordings} recordings for '{song_title}'")

                db.commit()

                return jsonify({
                    'success': True,
                    'message': f"Successfully reset recordings for '{song_title}'",
                    'deleted': {
                        'recordings': deleted_recordings,
                        'releases': deleted_releases,
                        'authority_recs_unlinked': unlinked_recs
                    }
                })

    except Exception as e:
        logger.error(f"Error resetting song recordings: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


# ---------------------------------------------------------------------------
# Duration Mismatch Review
# ---------------------------------------------------------------------------

def _format_duration(ms):
    """Format milliseconds as M:SS"""
    if ms is None:
        return '—'
    total_seconds = int(ms / 1000)
    minutes = total_seconds // 60
    seconds = total_seconds % 60
    return f"{minutes}:{seconds:02d}"


def _format_diff(diff_ms):
    """Format a duration difference as +M:SS or -M:SS"""
    sign = '+' if diff_ms >= 0 else '-'
    abs_ms = abs(diff_ms)
    total_seconds = int(abs_ms / 1000)
    minutes = total_seconds // 60
    seconds = total_seconds % 60
    return f"{sign}{minutes}:{seconds:02d}"


def _compute_album_fits_for_rows(conn, rows, log=None):
    """For each unique (release_id, spotify_album_id) pair in `rows`,
    compute the album-context fit ratio between the MB release tracklist
    and the Spotify album tracklist.

    Returns: {release_id: {mb_track_count, spotify_track_count,
                            matched_count, match_ratio, matched_titles}}
    Releases without a Spotify album link or with fetch failures simply
    aren't keys in the returned dict — the template will render '—' for
    those.

    Cached by release_id within the call so multiple links on the same
    release pay the upstream API cost only once. SpotifyClient.get_album_tracks
    and MusicBrainzSearcher both keep on-disk caches with multi-day
    TTLs, so first visit is the only one that pays the network round trip.
    """
    # Local imports — keeps the module import graph clean (these pull in
    # spotify-client + MB-searcher transitively).
    from integrations.spotify.client import SpotifyClient
    from integrations.spotify.matching import check_album_context_via_tracklist

    log = log or logger
    fits = {}
    spotify_client = None  # Lazy: only construct if we have any candidates.

    seen_releases = set()
    for row in rows:
        release_id = str(row['release_id'])
        if release_id in seen_releases:
            continue
        seen_releases.add(release_id)

        spotify_album_id = row.get('spotify_album_id')
        if not spotify_album_id:
            # No Spotify album linked at all — no album context to compute.
            continue

        if spotify_client is None:
            spotify_client = SpotifyClient(logger=log)

        try:
            tracks = spotify_client.get_album_tracks(spotify_album_id)
        except Exception:
            log.exception(
                "album-fit: failed to fetch Spotify album tracks for %s",
                spotify_album_id,
            )
            continue
        if not tracks:
            continue

        try:
            fit = check_album_context_via_tracklist(conn, release_id, tracks)
        except Exception:
            log.exception(
                "album-fit: check_album_context_via_tracklist failed for "
                "release=%s spotify_album=%s",
                release_id, spotify_album_id,
            )
            continue

        # Only surface the result when we actually got both sides — an
        # MB release with zero tracks back from MB just renders as '—'.
        if fit.get('mb_track_count'):
            fits[release_id] = fit

    return fits


@admin_bp.route('/duration-mismatches')
def duration_mismatches_list():
    """List songs that have Spotify links with duration mismatches vs MusicBrainz"""
    threshold = request.args.get('threshold', 60, type=int)
    current_sort = request.args.get('sort', 'mismatch_count')
    current_order = request.args.get('order', 'desc')
    # ?include_verified=1 to also count rows the admin has manually
    # verified (match_method='manual'). Default-off so the page is a
    # signal of work-still-to-do, not a rehash of accepted overrides.
    include_verified = request.args.get('include_verified') in ('1', 'true', 'yes')
    threshold_ms = threshold * 1000

    sort_map = {
        'title': 's.title',
        'mismatch_count': 'mismatch_count',
        'max_diff': 'max_diff_ms',
    }
    order_col = sort_map.get(current_sort, 'mismatch_count')
    order_dir = 'ASC' if current_order == 'asc' else 'DESC'

    manual_filter = (
        '' if include_verified
        else "AND (rrsl.match_method IS NULL OR rrsl.match_method != 'manual')"
    )

    with get_db_connection() as db:
        with db.cursor() as cur:
            # Prefer rr.track_length_ms (the per-release track duration)
            # when MB provided it, falling back to r.duration_ms (the
            # canonical recording length). MB allows the same recording_id
            # to ship on multiple releases with different track lengths —
            # e.g. a 9:26 live recording on a compilation as a 5:50 edit.
            # Comparing Spotify's edit-length to the canonical length
            # would falsely flag every such case as a mismatch.
            cur.execute(f"""
                SELECT
                    s.id AS song_id,
                    s.title,
                    s.composer,
                    COUNT(DISTINCT r.id) AS total_recordings,
                    COUNT(rrsl.id) AS mismatch_count,
                    MAX(ABS(COALESCE(rr.track_length_ms, r.duration_ms) - rrsl.duration_ms)) AS max_diff_ms
                FROM songs s
                JOIN recordings r ON r.song_id = s.id
                JOIN recording_releases rr ON rr.recording_id = r.id
                JOIN recording_release_streaming_links rrsl
                    ON rrsl.recording_release_id = rr.id
                    AND rrsl.service = 'spotify'
                WHERE COALESCE(rr.track_length_ms, r.duration_ms) IS NOT NULL
                  AND rrsl.duration_ms IS NOT NULL
                  AND ABS(COALESCE(rr.track_length_ms, r.duration_ms) - rrsl.duration_ms) > %s
                  {manual_filter}
                GROUP BY s.id, s.title, s.composer
                ORDER BY {order_col} {order_dir}, s.title ASC
            """, (threshold_ms,))
            songs = [dict(row) for row in cur.fetchall()]

            for song in songs:
                song['max_diff_display'] = _format_diff(song['max_diff_ms'])

            # Summary stats
            cur.execute("""
                SELECT COUNT(*) AS cnt
                FROM recording_release_streaming_links
                WHERE service = 'spotify'
            """)
            total_spotify = cur.fetchone()['cnt']

            cur.execute("""
                SELECT COUNT(*) AS cnt
                FROM recording_release_streaming_links
                WHERE service = 'spotify' AND match_method = 'manual'
            """)
            total_manual = cur.fetchone()['cnt']

            total_mismatched = sum(s['mismatch_count'] for s in songs)

    summary = {
        'total_songs': len(songs),
        'total_mismatched_links': total_mismatched,
        'total_spotify_links': total_spotify,
        'total_manual_links': total_manual,
        'include_verified': include_verified,
    }

    return render_template('admin/duration_mismatches_list.html',
                           songs=songs,
                           summary=summary,
                           threshold=threshold,
                           current_sort=current_sort,
                           current_order=current_order)


@admin_bp.route('/duration-mismatches/<song_id>')
def duration_mismatches_review(song_id):
    """Review duration mismatches for a specific song"""
    threshold = request.args.get('threshold', 60, type=int)
    include_verified = request.args.get('include_verified') in ('1', 'true', 'yes')
    threshold_ms = threshold * 1000

    manual_filter = (
        '' if include_verified
        else "AND (rrsl.match_method IS NULL OR rrsl.match_method != 'manual')"
    )

    with get_db_connection() as db:
        with db.cursor() as cur:
            cur.execute("""
                SELECT id, title, composer, musicbrainz_id
                FROM songs WHERE id = %s
            """, (song_id,))
            song = cur.fetchone()
            if not song:
                return "Song not found", 404
            song = dict(song)

            # COALESCE strategy mirrors the list page above: prefer the
            # release-specific track_length_ms when MB has it, fall back
            # to the recording's canonical duration. We also surface the
            # raw track_length_ms separately so the template can hint to
            # the admin when these two diverge (i.e. MB knows the release
            # uses an edited cut).
            cur.execute(f"""
                SELECT
                    r.id AS recording_id,
                    r.title,
                    r.recording_year,
                    r.musicbrainz_id,
                    r.duration_ms,
                    rrsl.id AS streaming_link_id,
                    rrsl.service_id,
                    rrsl.service_url,
                    rrsl.duration_ms AS spotify_duration_ms,
                    rrsl.match_confidence,
                    rrsl.match_method,
                    rr.id AS recording_release_id,
                    rr.track_number,
                    rr.disc_number,
                    rr.track_length_ms,
                    COALESCE(rr.track_length_ms, r.duration_ms) AS effective_mb_duration_ms,
                    rrsl.service_title AS spotify_track_title,
                    rel.id AS release_id,
                    rel.title AS release_title,
                    rel.artist_credit,
                    rel.musicbrainz_release_id AS release_mb_id,
                    rel.spotify_album_id,
                    rel.release_year,
                    ABS(COALESCE(rr.track_length_ms, r.duration_ms) - rrsl.duration_ms) AS diff_ms
                FROM recordings r
                JOIN recording_releases rr ON rr.recording_id = r.id
                JOIN recording_release_streaming_links rrsl
                    ON rrsl.recording_release_id = rr.id
                    AND rrsl.service = 'spotify'
                JOIN releases rel ON rel.id = rr.release_id
                WHERE r.song_id = %s
                  AND COALESCE(rr.track_length_ms, r.duration_ms) IS NOT NULL
                  AND rrsl.duration_ms IS NOT NULL
                  AND ABS(COALESCE(rr.track_length_ms, r.duration_ms) - rrsl.duration_ms) > %s
                  {manual_filter}
                ORDER BY r.recording_year NULLS LAST, rel.title
            """, (song_id, threshold_ms))
            rows = [dict(row) for row in cur.fetchall()]

        # Album-fit calculation — compare each row's MB release tracklist
        # against the linked Spotify album's tracklist. Strong fit (high
        # ratio + many matched tracks) is a signal that the duration delta
        # is just an "edit/version difference" rather than a wrong-album
        # match. We compute this at display time so the admin reviewing a
        # mismatch can decide quickly whether to verify or unlink.
        #
        # Cached by spotify_album_id within this request so multiple links
        # on the same album don't trigger duplicate API calls. Underlying
        # client (SpotifyClient.get_album_tracks) and MusicBrainzSearcher
        # both have on-disk caches with multi-day TTLs, so first visit per
        # album is the only one that pays the network cost.
        album_fit_cache = _compute_album_fits_for_rows(db, rows, log=logger)

    # Group by recording
    recordings_map = {}
    for row in rows:
        rec_id = row['recording_id']
        if rec_id not in recordings_map:
            recordings_map[rec_id] = {
                'recording': {
                    'id': rec_id,
                    'title': row['title'],
                    'recording_year': row['recording_year'],
                    'musicbrainz_id': row['musicbrainz_id'],
                    'duration_ms': row['duration_ms'],
                    'duration_display': _format_duration(row['duration_ms']),
                },
                'links': []
            }
        diff_ms = row['diff_ms']
        album_fit = album_fit_cache.get(str(row['release_id']))
        # MB Duration shown in the admin table is now the *effective*
        # duration for this release — track_length_ms when MB provided
        # one, recording.duration_ms otherwise. Both raw values are kept
        # so the template can flag when they diverge ("recording is
        # 9:26 but this release uses a 5:50 edit").
        recording_duration_ms = row['duration_ms']
        track_length_ms = row.get('track_length_ms')
        effective_mb_duration_ms = row['effective_mb_duration_ms']
        recordings_map[rec_id]['links'].append({
            'streaming_link_id': str(row['streaming_link_id']),
            'service_id': row['service_id'],
            'service_url': row['service_url'],
            'spotify_duration_ms': row['spotify_duration_ms'],
            'spotify_duration_display': _format_duration(row['spotify_duration_ms']),
            'mb_duration_display': _format_duration(effective_mb_duration_ms),
            'recording_duration_display': _format_duration(recording_duration_ms),
            'track_length_ms': track_length_ms,
            'has_release_specific_length': (
                track_length_ms is not None
                and track_length_ms != recording_duration_ms
            ),
            'diff_ms': diff_ms,
            'diff_seconds': int(diff_ms / 1000),
            'diff_display': _format_diff(diff_ms),
            'match_confidence': float(row['match_confidence']) if row['match_confidence'] is not None else None,
            'match_method': row['match_method'],
            'is_manual': row['match_method'] == 'manual',
            'release_title': row['release_title'],
            'artist_credit': row['artist_credit'],
            'release_mb_id': row['release_mb_id'],
            'release_id': str(row['release_id']),
            'spotify_album_id': row['spotify_album_id'],
            'mb_track_title': row['title'],
            'spotify_track_title': row['spotify_track_title'],
            'album_fit': album_fit,
        })

    recordings = list(recordings_map.values())

    return render_template('admin/duration_mismatches_review.html',
                           song=song,
                           recordings=recordings,
                           threshold=threshold,
                           include_verified=include_verified)


@admin_bp.route('/duration-mismatches/delete-links', methods=['POST'])
def duration_mismatches_delete():
    """Delete selected Spotify streaming links"""
    data = request.get_json()
    link_ids = data.get('link_ids', [])

    if not link_ids:
        return jsonify({'error': 'No link IDs provided'}), 400

    try:
        with get_db_connection() as db:
            with db.cursor() as cur:
                cur.execute("""
                    DELETE FROM recording_release_streaming_links
                    WHERE id = ANY(%s)
                      AND service = 'spotify'
                    RETURNING id
                """, (link_ids,))
                deleted = cur.fetchall()
                db.commit()

                logger.info(f"Admin deleted {len(deleted)} Spotify streaming links for duration mismatch cleanup")

                return jsonify({
                    'success': True,
                    'deleted_count': len(deleted)
                })

    except Exception as e:
        logger.error(f"Error deleting streaming links: {e}")
        return jsonify({'error': str(e)}), 500


@admin_bp.route('/duration-mismatches/links/<link_id>/verify', methods=['POST'])
def duration_mismatches_verify_link(link_id):
    """Mark a Spotify streaming link as manually verified (or un-verify it).

    Flips `recording_release_streaming_links.match_method` to 'manual',
    which is the magic value the matcher already honours as a "do not
    touch" flag in update_recording_release_track_id and
    clear_recording_release_track. The duration-mismatch admin queries
    also exclude rows with match_method='manual' so the UI stops nagging
    about a match the admin has already accepted.

    Body: optional JSON {"manual": false} to revert. Default is to set
    manual=true.
    """
    data = request.get_json(silent=True) or {}
    set_manual = bool(data.get('manual', True))
    try:
        with get_db_connection() as db:
            updated = set_track_link_manual_override(
                db, link_id, manual=set_manual, log=logger,
            )
            db.commit()
    except Exception as e:
        logger.error(f"Error toggling manual override on link {link_id}: {e}")
        return jsonify({'error': str(e)}), 500

    if not updated:
        return jsonify({'error': 'Streaming link not found'}), 404

    return jsonify({
        'success': True,
        'link_id': link_id,
        'match_method': 'manual' if set_manual else 'fuzzy_search',
    })


@admin_bp.route('/duration-mismatches/links/<link_id>/reject', methods=['POST'])
def duration_mismatches_reject_link(link_id):
    """Block + delete a wrong Spotify streaming link.

    Two-step operation in a single transaction:

      1. Insert into bad_streaming_matches at block_level='track', so the
         matcher's get_blocked_tracks_for_song lookup will skip this
         (song_id, spotify_track_id) pair on every future match attempt.
      2. Delete the streaming link itself. The page reloads to confirm.

    Idempotent against repeated invocations: the bad_streaming_matches
    unique constraint collapses duplicate blocks, and the DELETE
    no-ops on a missing link.

    Body: optional JSON {"reason": "..."} captured into bad_streaming_matches.reason
    for human review. The default is "rejected via admin UI".
    """
    data = request.get_json(silent=True) or {}
    reason = (data.get('reason') or 'rejected via admin UI').strip()

    try:
        with get_db_connection() as db:
            with db.cursor() as cur:
                # Pull (song_id, service_id) from the link we're about to nuke.
                # Going through recording_releases → recordings → songs.id is
                # the only reachable path; rrsl itself has no song_id.
                cur.execute(
                    """
                    SELECT rec.song_id AS song_id, rrsl.service_id AS spotify_track_id
                    FROM recording_release_streaming_links rrsl
                    JOIN recording_releases rr ON rr.id = rrsl.recording_release_id
                    JOIN recordings rec ON rec.id = rr.recording_id
                    WHERE rrsl.id = %s
                      AND rrsl.service = 'spotify'
                    """,
                    (link_id,),
                )
                row = cur.fetchone()
            if not row:
                return jsonify({'error': 'Streaming link not found'}), 404
            song_id = str(row['song_id'])
            spotify_track_id = row['spotify_track_id']

            # Step 1 — block the (song, track) pair so the matcher skips it.
            # Skipped silently if there's no service_id to block (defensive;
            # in practice every spotify link has one).
            if spotify_track_id:
                block_streaming_track(
                    db, song_id, spotify_track_id,
                    service='spotify', reason=reason, log=logger,
                )

            # Step 2 — delete the link itself.
            with db.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM recording_release_streaming_links
                    WHERE id = %s AND service = 'spotify'
                    RETURNING id
                    """,
                    (link_id,),
                )
                deleted = cur.fetchone()

            db.commit()
    except Exception as e:
        logger.error(f"Error rejecting link {link_id}: {e}")
        return jsonify({'error': str(e)}), 500

    return jsonify({
        'success': True,
        'link_id': link_id,
        'song_id': song_id,
        'spotify_track_id': spotify_track_id,
        'deleted': bool(deleted),
        'blocked': bool(spotify_track_id),
    })


@admin_bp.route('/duration-mismatches/links/<link_id>/tracklists', methods=['GET'])
def duration_mismatches_link_tracklists(link_id):
    """Return the MB release tracklist + Spotify album tracklist paired
    up for side-by-side comparison in the admin UI.

    Click-to-expand panel on /admin/duration-mismatches/<song> calls this
    so the admin can see exactly which MB track lines up to which
    Spotify track without leaving the page. The data underneath powers
    the existing Album Fit column too — same MB and Spotify API hits,
    same on-disk caches.

    Both upstream calls are cached on disk (multi-day TTL on the
    SpotifyClient cache, MB cache via MusicBrainzSearcher), so first
    expand per release pays the network round-trip and subsequent
    expands are instant.
    """
    from integrations.spotify.client import SpotifyClient
    from integrations.musicbrainz.utils import MusicBrainzSearcher

    try:
        with get_db_connection() as db:
            with db.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        rel.id           AS release_id,
                        rel.title        AS release_title,
                        rel.artist_credit,
                        rel.musicbrainz_release_id,
                        rel.spotify_album_id
                    FROM recording_release_streaming_links rrsl
                    JOIN recording_releases rr ON rr.id = rrsl.recording_release_id
                    JOIN releases rel ON rel.id = rr.release_id
                    WHERE rrsl.id = %s
                      AND rrsl.service = 'spotify'
                    """,
                    (link_id,),
                )
                row = cur.fetchone()
    except Exception as e:
        logger.error(f"Error loading tracklists for link {link_id}: {e}")
        return jsonify({'error': str(e)}), 500

    if not row:
        return jsonify({'error': 'Streaming link not found'}), 404

    mb_release_id = row['musicbrainz_release_id']
    spotify_album_id = row['spotify_album_id']
    release_title = row['release_title']
    release_artist_credit = row['artist_credit']

    # MB side. Build a flat list of {disc, position, title, duration_ms}
    # ordered by (disc, position). MB's `length` field is already in ms.
    mb_tracks = []
    if mb_release_id:
        try:
            mb_searcher = MusicBrainzSearcher()
            mb_release = mb_searcher.get_release_details(mb_release_id)
        except Exception:
            logger.exception(
                "tracklists: MB get_release_details failed for %s", mb_release_id,
            )
            mb_release = None

        if mb_release:
            for medium in mb_release.get('media', []):
                disc_number = medium.get('position', 1)
                for track in medium.get('tracks', []):
                    raw_length = track.get('length')
                    try:
                        duration_ms = int(raw_length) if raw_length is not None else None
                    except (TypeError, ValueError):
                        duration_ms = None
                    mb_tracks.append({
                        'disc_number': disc_number,
                        'position': track.get('position'),
                        'title': track.get('title'),
                        'duration_ms': duration_ms,
                        'duration_display': _format_duration(duration_ms),
                    })

    # Spotify side. Album header (name + artists) + the full paginated
    # tracklist (which already includes per-track artists, durations, and
    # disc/track numbers thanks to the artists field we started capturing
    # in 4f06d18).
    spotify_album_title = None
    spotify_album_artists = []
    spotify_tracks = []
    if spotify_album_id:
        try:
            spotify_client = SpotifyClient(logger=logger)
            details = spotify_client.get_album_details(spotify_album_id)
            tracks = spotify_client.get_album_tracks(spotify_album_id)
        except Exception:
            logger.exception(
                "tracklists: Spotify fetch failed for album %s", spotify_album_id,
            )
            details = None
            tracks = None

        if details:
            spotify_album_title = details.get('name')
            spotify_album_artists = [
                a.get('name') for a in (details.get('artists') or []) if a
            ]
        for track in tracks or []:
            duration_ms = track.get('duration_ms')
            spotify_tracks.append({
                'disc_number': track.get('disc_number', 1),
                'position': track.get('track_number'),
                'name': track.get('name'),
                'artists': track.get('artists') or [],
                'duration_ms': duration_ms,
                'duration_display': _format_duration(duration_ms),
                'spotify_track_id': track.get('id'),
            })

    return jsonify({
        'success': True,
        'link_id': link_id,
        'mb_release': {
            'title': release_title,
            'artist_credit': release_artist_credit,
            'mb_release_id': mb_release_id,
            'tracks': mb_tracks,
        },
        'spotify_album': {
            'title': spotify_album_title,
            'artists': spotify_album_artists,
            'spotify_album_id': spotify_album_id,
            'tracks': spotify_tracks,
        },
    })


@admin_bp.route('/users')
def users_list():
    """List user accounts with email search and pagination."""
    search = request.args.get('search', '').strip()

    try:
        page = max(1, int(request.args.get('page', 1)))
    except ValueError:
        page = 1

    try:
        per_page = int(request.args.get('per_page', 50))
    except ValueError:
        per_page = 50
    per_page = max(10, min(per_page, 200))

    offset = (page - 1) * per_page

    where_sql = ''
    params = []
    if search:
        where_sql = 'WHERE email ILIKE %s'
        params.append(f'%{search}%')

    with get_db_connection() as db:
        with db.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) AS total FROM users {where_sql}', params)
            total = cur.fetchone()['total']

            cur.execute(
                f"""
                SELECT
                    id,
                    email,
                    display_name,
                    is_admin,
                    is_active,
                    account_locked,
                    email_verified,
                    google_id IS NOT NULL AS has_google,
                    apple_id IS NOT NULL AS has_apple,
                    last_login_at,
                    created_at
                FROM users
                {where_sql}
                ORDER BY last_login_at DESC NULLS LAST, created_at DESC
                LIMIT %s OFFSET %s
                """,
                params + [per_page, offset],
            )
            users = [dict(row) for row in cur.fetchall()]

    total_pages = max(1, (total + per_page - 1) // per_page)

    return render_template(
        'admin/users_list.html',
        users=users,
        search=search,
        page=page,
        per_page=per_page,
        total=total,
        total_pages=total_pages,
    )


@admin_bp.route('/users/<user_id>/reset-password', methods=['POST'])
def users_reset_password(user_id):
    """
    Reset a user's password from the /admin/users page.

    JSON body (all optional):
        password: str — use this value. If omitted, a random one is generated.

    Returns the new password in the response so the admin can copy it once
    and hand it to the user out-of-band. There is no flag forcing a change
    on next login; admins are expected to share the password and let the
    user change it via the normal flow.
    """
    payload = request.get_json(silent=True) or {}
    new_password = (payload.get('password') or '').strip()
    generated = False
    if not new_password:
        new_password = secrets.token_urlsafe(12)
        generated = True

    if len(new_password) < 8:
        return jsonify({'error': 'Password must be at least 8 characters'}), 400

    with get_db_connection() as db:
        with db.cursor() as cur:
            cur.execute(
                "SELECT id, email FROM users WHERE id = %s",
                (user_id,),
            )
            user = cur.fetchone()
            if not user:
                return jsonify({'error': 'User not found'}), 404

            cur.execute(
                "UPDATE users SET password_hash = %s, updated_at = NOW() "
                "WHERE id = %s",
                (hash_password(new_password), user_id),
            )
            db.commit()

    actor = getattr(g, 'current_user', None)
    actor_email = actor['email'] if actor else 'unknown'
    logger.info(
        "admin %s reset password for user %s (%s)",
        actor_email, user['email'], user_id,
    )

    return jsonify({
        'success': True,
        'email': user['email'],
        'password': new_password,
        'generated': generated,
    })


# ============================================================================
# Spotify Rematch Diagnostics
#
# Admin tool that runs the Spotify track matcher (rematch_tracks=True) for a
# single song and shows a before/after diff of Spotify state. The shared
# logic lives in core.spotify_rematch; the backfill script uses it too.
# ============================================================================

@admin_bp.route('/spotify-rematch')
def spotify_rematch_list():
    """
    Landing page: song picker + recent runs across all songs.

    Matches the admin "stuck release" query from
    scripts/backfill_spotify_track_links.py so the admin can see which songs
    have releases with an album-level Spotify mapping but missing track-level
    links.
    """
    search = (request.args.get('q') or '').strip()

    with get_db_connection() as db:
        with db.cursor() as cur:
            if search:
                cur.execute("""
                    SELECT
                        s.id,
                        s.title,
                        s.composer,
                        COUNT(DISTINCT r.id) FILTER (
                            WHERE r.spotify_album_id IS NOT NULL
                              AND NOT EXISTS (
                                  SELECT 1 FROM recording_release_streaming_links rrsl
                                  WHERE rrsl.recording_release_id = rr.id
                                    AND rrsl.service = 'spotify'
                              )
                        ) AS stuck_releases
                    FROM songs s
                    JOIN recordings rec ON rec.song_id = s.id
                    JOIN recording_releases rr ON rr.recording_id = rec.id
                    JOIN releases r ON r.id = rr.release_id
                    WHERE LOWER(s.title) LIKE LOWER(%s)
                    GROUP BY s.id, s.title, s.composer
                    ORDER BY s.title
                    LIMIT 100
                """, (f'%{search}%',))
            else:
                # Default view: songs with the most stuck releases, as defined
                # in backfill_spotify_track_links.py
                cur.execute("""
                    SELECT s.id, s.title, s.composer,
                           COUNT(DISTINCT r.id) AS stuck_releases
                    FROM songs s
                    JOIN recordings rec ON rec.song_id = s.id
                    JOIN recording_releases rr ON rr.recording_id = rec.id
                    JOIN releases r ON r.id = rr.release_id
                    WHERE r.spotify_album_id IS NOT NULL
                      AND NOT EXISTS (
                          SELECT 1 FROM recording_release_streaming_links rrsl
                          WHERE rrsl.recording_release_id = rr.id
                            AND rrsl.service = 'spotify'
                      )
                    GROUP BY s.id, s.title, s.composer
                    ORDER BY COUNT(DISTINCT r.id) DESC, s.title
                    LIMIT 100
                """)
            songs = [dict(row) for row in cur.fetchall()]

    recent_runs = list_all_runs(limit=25)

    return render_template(
        'admin/spotify_rematch_list.html',
        songs=songs,
        search=search,
        recent_runs=recent_runs,
    )


@admin_bp.route('/spotify-rematch/<song_id>')
def spotify_rematch_detail(song_id):
    """Song-specific page: current Spotify state summary + run history + run button."""
    with get_db_connection() as db:
        with db.cursor() as cur:
            cur.execute("""
                SELECT id, title, composer
                FROM songs
                WHERE id = %s
            """, (song_id,))
            song = cur.fetchone()
            if not song:
                return render_template(
                    'admin/spotify_rematch_detail.html',
                    song=None,
                    summary=None,
                    runs=[],
                ), 404

            # Current state summary — no snapshot, just aggregate counts.
            cur.execute("""
                SELECT
                    COUNT(DISTINCT r.id) AS total_releases,
                    COUNT(DISTINCT r.id) FILTER (
                        WHERE r.spotify_album_id IS NOT NULL
                    ) AS releases_with_album_id,
                    COUNT(DISTINCT r.id) FILTER (
                        WHERE r.spotify_album_id IS NOT NULL
                          AND NOT EXISTS (
                              SELECT 1 FROM recording_release_streaming_links rrsl
                              WHERE rrsl.recording_release_id = rr.id
                                AND rrsl.service = 'spotify'
                          )
                    ) AS stuck_releases,
                    COUNT(DISTINCT rr.id) AS total_recording_releases,
                    COUNT(DISTINCT rr.id) FILTER (
                        WHERE EXISTS (
                            SELECT 1 FROM recording_release_streaming_links rrsl
                            WHERE rrsl.recording_release_id = rr.id
                              AND rrsl.service = 'spotify'
                        )
                    ) AS recording_releases_with_spotify_track
                FROM recordings rec
                JOIN recording_releases rr ON rr.recording_id = rec.id
                JOIN releases r ON r.id = rr.release_id
                WHERE rec.song_id = %s
            """, (song_id,))
            summary = dict(cur.fetchone() or {})

    runs = list_runs_for_song(song_id)

    return render_template(
        'admin/spotify_rematch_detail.html',
        song=dict(song),
        summary=summary,
        runs=runs,
    )


@admin_bp.route('/spotify-rematch/<song_id>/run', methods=['POST'])
def spotify_rematch_run(song_id):
    """
    Run the Spotify matcher for one song. Blocking — may take minutes for
    songs with many releases. Persists a run record and returns a JSON
    payload with the URL to view it.
    """
    try:
        run_record = run_spotify_rematch_for_song(song_id, logger=logger)
        save_run(run_record)
        return jsonify({
            'success': True,
            'run_id': run_record['run_id'],
            'run_url': url_for(
                'admin.spotify_rematch_run_detail',
                song_id=song_id,
                run_id=run_record['run_id'],
            ),
            'stats': run_record['stats'],
            'change_count': len(run_record['changes']),
        })
    except ValueError as e:
        return jsonify({'success': False, 'error': str(e)}), 404
    except Exception as e:
        logger.error(f"Spotify rematch failed for song {song_id}: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


@admin_bp.route('/spotify-rematch/<song_id>/run/<run_id>')
def spotify_rematch_run_detail(song_id, run_id):
    """Render a persisted run as a report."""
    run = load_run(run_id)
    if not run or run.get('song', {}).get('id') != song_id:
        return render_template(
            'admin/spotify_rematch_run.html',
            run=None,
            song_id=song_id,
        ), 404

    # Group changes by action for display
    grouped = {}
    for change in run.get('changes', []):
        grouped.setdefault(change['action'], []).append(change)

    return render_template(
        'admin/spotify_rematch_run.html',
        run=run,
        grouped_changes=grouped,
        song_id=song_id,
    )


# ============================================================================
# Browse: Song / Recording / (later) Release detail pages
#
# Read-only admin pages for tracing data end-to-end. Issue #176 covers
# expansion to release detail (recording_releases + streaming links). For
# now: songs list/search → song detail → recording detail.
# ============================================================================

@admin_bp.route('/songs')
def songs_browse_list():
    """Searchable song list. ?q= filters by title or alt_titles, case- and
    accent-insensitive (Postgres `unaccent`). No pagination yet — capped
    at 200 rows."""
    from db_utils import normalize_apostrophes
    q = (request.args.get('q') or '').strip()
    limit = 200

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            if q:
                # Normalize apostrophes on the input; `unaccent` strips
                # diacritics on both sides of the LIKE so "naima" finds
                # "Naïma" and "si tu vois ma mere" finds "...mère".
                like = f"%{normalize_apostrophes(q).lower()}%"
                cur.execute(
                    """
                    SELECT s.id, s.title, s.composer, s.musicbrainz_id, s.second_mb_id,
                           (SELECT COUNT(*) FROM recordings r WHERE r.song_id = s.id) AS recording_count
                    FROM songs s
                    WHERE LOWER(unaccent(s.title)) LIKE LOWER(unaccent(%s))
                       OR EXISTS (
                           SELECT 1 FROM unnest(COALESCE(s.alt_titles, ARRAY[]::text[])) AS t
                           WHERE LOWER(unaccent(t)) LIKE LOWER(unaccent(%s))
                       )
                    ORDER BY s.title
                    LIMIT %s
                    """,
                    (like, like, limit),
                )
            else:
                cur.execute(
                    """
                    SELECT s.id, s.title, s.composer, s.musicbrainz_id, s.second_mb_id,
                           (SELECT COUNT(*) FROM recordings r WHERE r.song_id = s.id) AS recording_count
                    FROM songs s
                    ORDER BY s.title
                    LIMIT %s
                    """,
                    (limit,),
                )
            songs = cur.fetchall()

    return render_template(
        'admin/browse_songs_list.html',
        songs=songs,
        q=q,
        limit=limit,
        truncated=len(songs) >= limit,
    )


@admin_bp.route('/songs/<song_id>')
def songs_browse_detail(song_id):
    """Song detail with sortable recording list.

    The recordings list is fetched via the SAME helper the iOS/Mac app
    consumes from /api/songs/<id>/recordings (`fetch_song_recordings_listing`
    in routes/songs.py). The admin layer adds a small supplementary SELECT
    for diagnostic-only fields (MB recording ID, release count, default
    release ID, primary release year) that the API deliberately omits from
    its list payload — they're displayed alongside the canonical fields,
    never substituted in.
    """
    from routes.songs import fetch_song_recordings_listing

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, title, composer, musicbrainz_id, second_mb_id,
                       composed_year, composed_key, wikipedia_url, alt_titles
                FROM songs WHERE id = %s
                """,
                (song_id,),
            )
            song = cur.fetchone()
            if not song:
                return ('Song not found', 404)

            # Diagnostic-only extras keyed by recording id. Kept separate
            # from the canonical helper output so the admin can never
            # disagree with the app on cover art / album title / artist
            # credit — those come from fetch_song_recordings_listing only.
            cur.execute(
                """
                SELECT r.id,
                       r.musicbrainz_id AS mb_recording_id,
                       r.default_release_id,
                       (SELECT COUNT(*) FROM recording_releases rr WHERE rr.recording_id = r.id) AS release_count,
                       (SELECT release_year FROM releases WHERE id = r.default_release_id) AS primary_release_year
                FROM recordings r
                WHERE r.song_id = %s
                """,
                (song_id,),
            )
            extras_by_id = {str(row['id']): row for row in cur.fetchall()}

    # Canonical app payload (same SQL as /api/songs/<id>/recordings).
    recordings = fetch_song_recordings_listing(song_id, sort_by='year')

    # Merge diagnostic extras onto each row, and project performers (a
    # JSON list of dicts in the canonical payload) into a comma-joined
    # display string for the table.
    for r in recordings:
        rid = str(r['id'])
        extra = extras_by_id.get(rid, {})
        r['mb_recording_id'] = extra.get('mb_recording_id')
        r['default_release_id'] = extra.get('default_release_id')
        r['release_count'] = extra.get('release_count', 0)
        r['primary_release_year'] = extra.get('primary_release_year')
        # Canonical performers shape: [{name, sort_name, instrument, role}, ...]
        # Already ordered leader → sideman → other in SQL.
        r['performers_display'] = ', '.join(
            p['name'] for p in (r.get('performers') or []) if p.get('name')
        )

    return render_template(
        'admin/browse_song_detail.html',
        song=song,
        recordings=recordings,
    )


@admin_bp.route('/releases/<release_id>')
def releases_browse_detail(release_id):
    """Release detail.

    Diagnostic-only page (no app-facing /releases endpoint to mirror, since
    releases aren't exposed as first-class objects in the public API; the
    apps only see the album_title/artist_credit/cover-art fields the song
    payload pre-joins). The page exposes everything we know about the
    release row, all imagery rows from every source, all release-level
    streaming links, and the per-track table with each recording_release's
    streaming-link rows attached.

    Optional ?via=<recording_id> query param threads the breadcrumb chain
    back through the recording (and its song) the user came from. The
    recording must actually be linked to this release; if it isn't, the
    param is silently ignored and the breadcrumb falls back to the
    no-context shape.
    """
    import uuid as _uuid
    via_recording_id_raw = (request.args.get('via') or '').strip() or None
    via_recording_id = None
    if via_recording_id_raw:
        # Reject non-UUID input early — passing a non-UUID string into a
        # uuid-typed WHERE clause raises an error that aborts the rest of
        # the transaction, breaking the whole page. Silent-drop is fine
        # here; the param is decorative.
        try:
            _uuid.UUID(via_recording_id_raw)
            via_recording_id = via_recording_id_raw
        except (ValueError, AttributeError):
            pass
    via_context = None  # {'recording_id', 'recording_title', 'song_id', 'song_title'}

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # 1) Release row + lookup-table joins for human-readable
            #    format/packaging/status names.
            cur.execute(
                """
                SELECT rel.*,
                       fmt.name AS format_name,
                       pkg.name AS packaging_name,
                       sts.name AS status_name
                FROM releases rel
                LEFT JOIN release_formats fmt ON fmt.id = rel.format_id
                LEFT JOIN release_packaging pkg ON pkg.id = rel.packaging_id
                LEFT JOIN release_statuses sts ON sts.id = rel.status_id
                WHERE rel.id = %s
                """,
                (release_id,),
            )
            release = cur.fetchone()
            if not release:
                return ('Release not found', 404)

            # 1a) If ?via=<recording_id>, pull the song chain for the
            #     breadcrumb. The JOIN on recording_releases ensures the
            #     recording is *actually* on this release — drops the
            #     param silently if a stale URL is followed to an unrelated
            #     release.
            if via_recording_id:
                try:
                    cur.execute(
                        """
                        SELECT rec.id AS recording_id,
                               rec.title AS recording_title,
                               s.id AS song_id,
                               s.title AS song_title
                        FROM recording_releases rr
                        JOIN recordings rec ON rec.id = rr.recording_id
                        JOIN songs s ON s.id = rec.song_id
                        WHERE rr.recording_id = %s AND rr.release_id = %s
                        LIMIT 1
                        """,
                        (via_recording_id, release_id),
                    )
                    row = cur.fetchone()
                    if row:
                        via_context = {
                            'recording_id': str(row['recording_id']),
                            'recording_title': row['recording_title'],
                            'song_id': str(row['song_id']),
                            'song_title': row['song_title'],
                        }
                except Exception:
                    # Silently fall back to no-context breadcrumb on any
                    # malformed UUID, etc. — the page is still useful.
                    via_context = None

            # 2) Release events (per-country release dates)
            cur.execute(
                """
                SELECT country, release_date
                FROM release_events
                WHERE release_id = %s
                ORDER BY release_date NULLS LAST, country
                """,
                (release_id,),
            )
            release_events = cur.fetchall()

            # 3) Release labels (label name + catalog number per label)
            cur.execute(
                """
                SELECT label_name, catalog_number, musicbrainz_label_id
                FROM release_labels
                WHERE release_id = %s
                ORDER BY label_name
                """,
                (release_id,),
            )
            release_labels = cur.fetchall()

            # 4) All imagery, every source, every type — show what we have
            cur.execute(
                """
                SELECT id, source::text AS source, source_id, source_url,
                       type::text AS type,
                       image_url_small, image_url_medium, image_url_large,
                       approved, comment, updated_at
                FROM release_imagery
                WHERE release_id = %s
                ORDER BY (type = 'Front') DESC, type, source
                """,
                (release_id,),
            )
            imagery = cur.fetchall()

            # 5) Release-level streaming links (album-level Apple/Spotify/etc.)
            cur.execute(
                """
                SELECT id, service, service_id, service_url,
                       match_confidence, match_method,
                       matched_at, last_verified_at, notes
                FROM release_streaming_links
                WHERE release_id = %s
                ORDER BY service
                """,
                (release_id,),
            )
            release_streaming_links = cur.fetchall()

            # 6) Tracks on this release (recording_releases joined to recordings)
            cur.execute(
                """
                SELECT rr.id AS recording_release_id,
                       rr.recording_id,
                       rr.disc_number, rr.track_number, rr.track_position,
                       rr.track_title, rr.track_artist_credit, rr.track_length_ms,
                       rec.title AS recording_title,
                       rec.musicbrainz_id AS mb_recording_id,
                       rec.recording_year,
                       rec.song_id,
                       s.title AS song_title
                FROM recording_releases rr
                JOIN recordings rec ON rec.id = rr.recording_id
                LEFT JOIN songs s ON s.id = rec.song_id
                WHERE rr.release_id = %s
                ORDER BY rr.disc_number NULLS LAST, rr.track_number NULLS LAST
                """,
                (release_id,),
            )
            tracks = cur.fetchall()

            # 7) All track-level streaming links for this release in one shot;
            #    grouped per recording_release_id below.
            cur.execute(
                """
                SELECT rrsl.id, rrsl.recording_release_id,
                       rrsl.service, rrsl.service_id, rrsl.service_url,
                       rrsl.service_title, rrsl.duration_ms, rrsl.preview_url,
                       rrsl.isrc, rrsl.match_confidence, rrsl.match_method,
                       rrsl.matched_at, rrsl.last_verified_at, rrsl.notes
                FROM recording_release_streaming_links rrsl
                JOIN recording_releases rr ON rr.id = rrsl.recording_release_id
                WHERE rr.release_id = %s
                ORDER BY rr.disc_number, rr.track_number, rrsl.service
                """,
                (release_id,),
            )
            track_links_rows = cur.fetchall()

    # Service → release_imagery source mapping. Used to surface the
    # streaming-service-side artwork next to its track-level link
    # ("imagery for this Apple Music track" = the matching release-level
    # 'Apple' imagery, since per-track artwork isn't stored separately).
    service_to_source = {
        'apple_music': 'Apple',
        'spotify': 'Spotify',
        'youtube': None,
    }
    imagery_by_source = {}
    for im in imagery:
        # First match wins; ORDER BY above prefers Front covers.
        imagery_by_source.setdefault(im['source'], im)

    # Group track streaming links by recording_release_id and attach the
    # service-matching imagery thumb URL to each link.
    links_by_rr = {}
    for link in track_links_rows:
        link['imagery_url'] = None
        link['imagery_source_url'] = None
        src = service_to_source.get(link['service'])
        if src and src in imagery_by_source:
            im = imagery_by_source[src]
            link['imagery_url'] = im['image_url_small'] or im['image_url_medium']
            link['imagery_source_url'] = im['source_url']
        links_by_rr.setdefault(str(link['recording_release_id']), []).append(link)

    for t in tracks:
        t['streaming_links'] = links_by_rr.get(str(t['recording_release_id']), [])

    return render_template(
        'admin/browse_release_detail.html',
        release=release,
        release_events=release_events,
        release_labels=release_labels,
        imagery=imagery,
        release_streaming_links=release_streaming_links,
        tracks=tracks,
        via_context=via_context,
    )


@admin_bp.route('/releases/<release_id>/rematch-spotify', methods=['POST'])
def rematch_release_spotify(release_id):
    """Run the live Spotify matcher against this single release and persist
    the result (writes to the DB).

    Calls the same SpotifyMatcher.match_releases() the worker calls, with
    rematch_all=True so the album is re-searched even when there's already
    a stale match, force_refresh=True so the search cache doesn't dominate,
    and the new release_ids filter so only THIS release is processed. A
    release can hold tracks for multiple songs (e.g. a Various Artists
    compilation), so we loop over each unique song on the release and
    call the matcher once per song — that's how match_releases is
    structured (a song-scoped helper).

    Snapshots release_streaming_links and recording_release_streaming_links
    rows for this release before and after, and returns a diff so the UI
    can show what actually changed.
    """
    import logging
    import uuid as _uuid_mod
    from io import StringIO
    from integrations.spotify.matcher import SpotifyMatcher

    def _snapshot(cur):
        cur.execute(
            """
            SELECT 'album' AS scope, NULL::uuid AS recording_release_id,
                   service, service_id, service_url, match_method, match_confidence
            FROM release_streaming_links WHERE release_id = %s
            UNION ALL
            SELECT 'track' AS scope, rrsl.recording_release_id,
                   rrsl.service, rrsl.service_id, rrsl.service_url,
                   rrsl.match_method, rrsl.match_confidence
            FROM recording_release_streaming_links rrsl
            JOIN recording_releases rr ON rr.id = rrsl.recording_release_id
            WHERE rr.release_id = %s
            ORDER BY scope, service, recording_release_id
            """,
            (release_id, release_id),
        )
        return [dict(r) for r in cur.fetchall()]

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM releases WHERE id = %s", (release_id,))
            if not cur.fetchone():
                return jsonify({'error': 'Release not found'}), 404

            # Distinct songs that have a track on this release.
            cur.execute(
                """
                SELECT DISTINCT rec.song_id, s.title AS song_title
                FROM recording_releases rr
                JOIN recordings rec ON rec.id = rr.recording_id
                JOIN songs s ON s.id = rec.song_id
                WHERE rr.release_id = %s
                ORDER BY s.title
                """,
                (release_id,),
            )
            songs = [dict(r) for r in cur.fetchall()]
            before = _snapshot(cur)

    if not songs:
        return jsonify({'error': 'No songs linked to this release'}), 400

    # Per-request isolated logger → buffer (same pattern as diagnose).
    log_buffer = StringIO()
    diag_logger = logging.getLogger(f'admin.spotify_rematch.{_uuid_mod.uuid4().hex}')
    diag_logger.setLevel(logging.DEBUG)
    diag_logger.propagate = False
    handler = logging.StreamHandler(log_buffer)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter('%(message)s'))
    diag_logger.addHandler(handler)

    error = None
    per_song_results = []
    matcher = None
    try:
        # ONE matcher instance across all songs so stats accumulate. The
        # cache and HTTP client are shared too, which avoids re-auth.
        matcher = SpotifyMatcher(
            dry_run=False,
            strict_mode=True,
            force_refresh=True,
            rematch_all=True,        # full re-match — re-search album AND tracks
            logger=diag_logger,
        )
        for song in songs:
            song_id_str = str(song['song_id'])
            diag_logger.info(f"=== Processing song {song['song_title']!r} ({song_id_str}) ===")
            res = matcher.match_releases(song_id_str, release_ids=[release_id])
            per_song_results.append({
                'song_id': song_id_str,
                'song_title': song['song_title'],
                'success': res.get('success', False),
                'error': res.get('error'),
            })
    except Exception as e:
        logger.exception("Spotify rematch failed for release %s", release_id)
        error = str(e)
    finally:
        diag_logger.removeHandler(handler)
        handler.close()

    # Snapshot AFTER and compute diff.
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            after = _snapshot(cur)

    def _key(row):
        # rows with same scope+service+recording_release_id are "the same row";
        # anything else is a different identity.
        return (row['scope'], row['service'], str(row.get('recording_release_id') or ''))
    before_by = {_key(r): r for r in before}
    after_by = {_key(r): r for r in after}
    added = [after_by[k] for k in after_by.keys() - before_by.keys()]
    removed = [before_by[k] for k in before_by.keys() - after_by.keys()]
    changed = []
    for k in before_by.keys() & after_by.keys():
        b, a = before_by[k], after_by[k]
        if b.get('service_id') != a.get('service_id') or b.get('match_method') != a.get('match_method'):
            changed.append({'before': b, 'after': a})

    stats = matcher.stats if matcher else {}
    return jsonify({
        'songs_processed': per_song_results,
        'stats': {
            'releases_processed': stats.get('releases_processed', 0),
            'releases_with_spotify': stats.get('releases_with_spotify', 0),
            'releases_no_match': stats.get('releases_no_match', 0),
            'releases_cleared': stats.get('releases_cleared', 0),
            'tracks_matched': stats.get('tracks_matched', 0),
            'tracks_no_match': stats.get('tracks_no_match', 0),
            'api_calls': stats.get('api_calls', 0),
        },
        'changes': {
            'added': added,
            'removed': removed,
            'changed': changed,
        },
        'log': log_buffer.getvalue(),
        'error': error,
    }), (500 if error else 200)


@admin_bp.route('/releases/<release_id>/diagnose-spotify', methods=['POST'])
def diagnose_release_spotify(release_id):
    """Simulate the Spotify matcher's album-search ladder against this release.

    Builds a real SpotifyMatcher (dry_run=True so nothing writes), pipes
    its DEBUG-level reasoning into a per-request buffer, and calls the
    same search_spotify_album() the worker calls. Returns the matcher's
    result plus the captured log so the admin can see exactly which
    queries were tried, which candidates came back, and why each was
    accepted or rejected — using the live matcher rules rather than a
    parallel implementation.

    Body params (JSON):
        force_refresh: bool (default False) — bypass the Spotify search
            cache. Useful when the cache has a stale "no match" entry.
    """
    import logging
    import uuid
    from io import StringIO
    from integrations.spotify.matcher import SpotifyMatcher
    from integrations.spotify.search import search_spotify_album

    body = request.get_json(silent=True) or {}
    force_refresh = bool(body.get('force_refresh', False))

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, title, artist_credit, release_year
                FROM releases WHERE id = %s
                """,
                (release_id,),
            )
            release = cur.fetchone()
            if not release:
                return jsonify({'error': 'Release not found'}), 404

            # Pick the first track's song title for validate_album_match's
            # track-presence fallback (used when artist similarity is low
            # but the album does contain the song we expected).
            cur.execute(
                """
                SELECT s.title AS song_title
                FROM recording_releases rr
                JOIN recordings rec ON rec.id = rr.recording_id
                JOIN songs s ON s.id = rec.song_id
                WHERE rr.release_id = %s
                ORDER BY rr.disc_number NULLS LAST, rr.track_number NULLS LAST
                LIMIT 1
                """,
                (release_id,),
            )
            song_row = cur.fetchone()
            song_title = song_row['song_title'] if song_row else None

    # Per-request isolated logger → buffer, so we capture only this run's
    # output and don't bleed handlers across requests.
    log_buffer = StringIO()
    diag_logger = logging.getLogger(f'admin.spotify_diag.{uuid.uuid4().hex}')
    diag_logger.setLevel(logging.DEBUG)
    diag_logger.propagate = False
    handler = logging.StreamHandler(log_buffer)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter('%(message)s'))
    diag_logger.addHandler(handler)

    error = None
    result = None
    try:
        matcher = SpotifyMatcher(
            dry_run=True,
            strict_mode=True,        # match the worker's default
            force_refresh=force_refresh,
            logger=diag_logger,
        )
        result = search_spotify_album(
            matcher,
            album_title=release['title'],
            artist_name=release['artist_credit'],
            song_title=song_title,
        )
    except Exception as e:
        logger.exception("Spotify diagnosis failed for release %s", release_id)
        error = str(e)
    finally:
        diag_logger.removeHandler(handler)
        handler.close()

    # Strip ANSI / non-essentials, keep raw text.
    log_text = log_buffer.getvalue()

    return jsonify({
        'input': {
            'album_title': release['title'],
            'artist_name': release['artist_credit'],
            'release_year': release['release_year'],
            'song_title_used_for_verify': song_title,
            'force_refresh': force_refresh,
            'thresholds': {
                'min_artist_similarity': 75,
                'min_album_similarity': 65,
                'min_track_similarity': 85,
            },
        },
        'matched': result is not None,
        'result': result,
        'log': log_text,
        'error': error,
    }), (500 if error else 200)


@admin_bp.route('/releases/<release_id>/diagnose-apple', methods=['POST'])
def diagnose_release_apple(release_id):
    """Simulate the Apple Music matcher's album search against this release.

    Mirrors what the Apple worker does at album-match time: builds a real
    AppleMusicMatcher (dry_run=True so nothing writes), captures its
    DEBUG-level reasoning into a buffer, and runs search_and_validate_album
    against the release's artist + title + year. Returns the matcher's
    result, the captured log, the inputs, and the thresholds — using the
    matcher's actual code, not a parallel implementation, so any future
    tuning shows up automatically.

    Body params (JSON):
        use_api_fallback: bool (default False) — if True, allow the
            matcher to fall back to the iTunes Search API after the local
            catalog misses. The worker runs with local_catalog_only=True
            so the default here matches that behavior; flip the flag to
            see what would happen if the API fallback were enabled.
        force_refresh: bool (default False) — bypass the iTunes API
            response cache when the API fallback runs. Only meaningful
            when use_api_fallback is also True.
    """
    import logging
    import uuid as _uuid_mod
    from io import StringIO
    from integrations.apple_music.matcher import AppleMusicMatcher
    from integrations.apple_music.search import search_and_validate_album

    body = request.get_json(silent=True) or {}
    use_api_fallback = bool(body.get('use_api_fallback', False))
    force_refresh = bool(body.get('force_refresh', False))

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, title, artist_credit, release_year
                FROM releases WHERE id = %s
                """,
                (release_id,),
            )
            release = cur.fetchone()
            if not release:
                return jsonify({'error': 'Release not found'}), 404

    log_buffer = StringIO()
    diag_logger = logging.getLogger(f'admin.apple_diag.{_uuid_mod.uuid4().hex}')
    diag_logger.setLevel(logging.DEBUG)
    diag_logger.propagate = False
    handler = logging.StreamHandler(log_buffer)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter('%(message)s'))
    diag_logger.addHandler(handler)

    error = None
    result = None
    try:
        matcher = AppleMusicMatcher(
            dry_run=True,
            strict_mode=True,
            force_refresh=force_refresh,
            # Worker default is local_catalog_only=True; mirror that
            # unless the admin explicitly opts into API fallback.
            local_catalog_only=not use_api_fallback,
            logger=diag_logger,
        )
        result = search_and_validate_album(
            matcher,
            artist_name=release['artist_credit'] or '',
            album_title=release['title'],
            release_year=release['release_year'],
        )
    except Exception as e:
        logger.exception("Apple Music diagnosis failed for release %s", release_id)
        error = str(e)
    finally:
        diag_logger.removeHandler(handler)
        handler.close()

    return jsonify({
        'input': {
            'album_title': release['title'],
            'artist_name': release['artist_credit'],
            'release_year': release['release_year'],
            'use_api_fallback': use_api_fallback,
            'force_refresh': force_refresh,
            'thresholds': {
                'min_artist_similarity': 75,
                'min_album_similarity': 65,
                'min_track_similarity': 85,
            },
        },
        'matched': result is not None,
        'result': result,
        'log': log_buffer.getvalue(),
        'error': error,
    }), (500 if error else 200)


@admin_bp.route('/releases/<release_id>/rematch-apple', methods=['POST'])
def rematch_release_apple(release_id):
    """Run the live Apple Music matcher against this single release and
    persist the result (writes to the DB).

    Calls the same AppleMusicMatcher.match_releases() the worker calls,
    with rematch=True so the album is re-searched even when there's
    already a stale "searched / no match" timestamp, and the new
    release_ids filter so only THIS release is processed. A release can
    hold tracks for multiple songs, so we loop over each unique song on
    the release and call the matcher once per song on a single shared
    matcher instance.

    Snapshots release_streaming_links, recording_release_streaming_links,
    and release_imagery for this release before and after, returns a diff.

    Body params (JSON):
        use_api_fallback: bool (default False) — same meaning as on the
            diagnose endpoint. Worker default is local-only.
    """
    import logging
    import uuid as _uuid_mod
    from io import StringIO
    from integrations.apple_music.matcher import AppleMusicMatcher

    body = request.get_json(silent=True) or {}
    use_api_fallback = bool(body.get('use_api_fallback', False))

    def _snapshot(cur):
        cur.execute(
            """
            SELECT 'album'  AS scope, NULL::uuid AS recording_release_id,
                   service, service_id, service_url,
                   match_method, match_confidence,
                   NULL::text AS img_source, NULL::text AS img_type
            FROM release_streaming_links WHERE release_id = %s
            UNION ALL
            SELECT 'track'  AS scope, rrsl.recording_release_id,
                   rrsl.service, rrsl.service_id, rrsl.service_url,
                   rrsl.match_method, rrsl.match_confidence,
                   NULL, NULL
            FROM recording_release_streaming_links rrsl
            JOIN recording_releases rr ON rr.id = rrsl.recording_release_id
            WHERE rr.release_id = %s
            UNION ALL
            SELECT 'imagery' AS scope, NULL,
                   ri.source::text AS service, ri.source_id AS service_id,
                   ri.source_url AS service_url,
                   NULL, NULL,
                   ri.source::text, ri.type::text
            FROM release_imagery ri WHERE ri.release_id = %s
            ORDER BY scope, service, recording_release_id
            """,
            (release_id, release_id, release_id),
        )
        return [dict(r) for r in cur.fetchall()]

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM releases WHERE id = %s", (release_id,))
            if not cur.fetchone():
                return jsonify({'error': 'Release not found'}), 404

            cur.execute(
                """
                SELECT DISTINCT rec.song_id, s.title AS song_title
                FROM recording_releases rr
                JOIN recordings rec ON rec.id = rr.recording_id
                JOIN songs s ON s.id = rec.song_id
                WHERE rr.release_id = %s
                ORDER BY s.title
                """,
                (release_id,),
            )
            songs = [dict(r) for r in cur.fetchall()]
            before = _snapshot(cur)

    if not songs:
        return jsonify({'error': 'No songs linked to this release'}), 400

    log_buffer = StringIO()
    diag_logger = logging.getLogger(f'admin.apple_rematch.{_uuid_mod.uuid4().hex}')
    diag_logger.setLevel(logging.DEBUG)
    diag_logger.propagate = False
    handler = logging.StreamHandler(log_buffer)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter('%(message)s'))
    diag_logger.addHandler(handler)

    error = None
    per_song_results = []
    matcher = None
    try:
        matcher = AppleMusicMatcher(
            dry_run=False,
            strict_mode=True,
            force_refresh=True,
            rematch=True,                              # full re-match — re-search album
            local_catalog_only=not use_api_fallback,   # worker default: local only
            logger=diag_logger,
        )
        for song in songs:
            song_id_str = str(song['song_id'])
            diag_logger.info(f"=== Processing song {song['song_title']!r} ({song_id_str}) ===")
            res = matcher.match_releases(song_id_str, release_ids=[release_id])
            per_song_results.append({
                'song_id': song_id_str,
                'song_title': song['song_title'],
                'success': res.get('success', False),
                'message': res.get('message'),
            })
    except Exception as e:
        logger.exception("Apple Music rematch failed for release %s", release_id)
        error = str(e)
    finally:
        diag_logger.removeHandler(handler)
        handler.close()

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            after = _snapshot(cur)

    def _key(row):
        # imagery rows are keyed by (scope, source, type); streaming rows
        # by (scope, service, recording_release_id). Identity is just
        # "what counts as the same row in the table" — service-id changes
        # show up as a 'changed' row, not added+removed.
        if row['scope'] == 'imagery':
            return ('imagery', row.get('img_source') or '', row.get('img_type') or '')
        return (row['scope'], row['service'], str(row.get('recording_release_id') or ''))
    before_by = {_key(r): r for r in before}
    after_by = {_key(r): r for r in after}
    added = [after_by[k] for k in after_by.keys() - before_by.keys()]
    removed = [before_by[k] for k in before_by.keys() - after_by.keys()]
    changed = []
    for k in before_by.keys() & after_by.keys():
        b, a = before_by[k], after_by[k]
        if b.get('service_id') != a.get('service_id') or b.get('match_method') != a.get('match_method'):
            changed.append({'before': b, 'after': a})

    stats = matcher.stats if matcher else {}
    return jsonify({
        'songs_processed': per_song_results,
        'stats': {
            'releases_processed': stats.get('releases_processed', 0),
            'releases_matched': stats.get('releases_matched', 0),
            'releases_with_apple_music': stats.get('releases_with_apple_music', 0),
            'releases_no_match': stats.get('releases_no_match', 0),
            'releases_skipped': stats.get('releases_skipped', 0),
            'tracks_matched': stats.get('tracks_matched', 0),
            'tracks_no_match': stats.get('tracks_no_match', 0),
            'artwork_added': stats.get('artwork_added', 0),
            'local_catalog_hits': stats.get('local_catalog_hits', 0),
            'api_calls': stats.get('api_calls', 0),
        },
        'changes': {
            'added': added,
            'removed': removed,
            'changed': changed,
        },
        'log': log_buffer.getvalue(),
        'error': error,
    }), (500 if error else 200)


@admin_bp.route('/recordings/<recording_id>')
def recordings_browse_detail(recording_id):
    """Recording detail with release list."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT r.id, r.title, r.musicbrainz_id AS mb_recording_id,
                       r.recording_year, r.recording_date, r.label, r.is_canonical,
                       r.song_id, s.title AS song_title,
                       r.source_mb_work_id, r.duration_ms
                FROM recordings r
                JOIN songs s ON s.id = r.song_id
                WHERE r.id = %s
                """,
                (recording_id,),
            )
            recording = cur.fetchone()
            if not recording:
                return ('Recording not found', 404)

            cur.execute(
                """
                SELECT rr.id AS recording_release_id,
                       rr.release_id,
                       rr.disc_number,
                       rr.track_number,
                       rel.title AS release_title,
                       rel.artist_credit,
                       rel.release_year,
                       rel.musicbrainz_release_id AS mb_release_id,
                       rel.spotify_album_id,
                       (SELECT service_id FROM release_streaming_links rsl
                          WHERE rsl.release_id = rel.id AND rsl.service = 'apple_music') AS apple_music_album_id
                FROM recording_releases rr
                JOIN releases rel ON rel.id = rr.release_id
                WHERE rr.recording_id = %s
                ORDER BY rel.release_year NULLS LAST, rel.title
                """,
                (recording_id,),
            )
            releases = cur.fetchall()

    return render_template(
        'admin/browse_recording_detail.html',
        recording=recording,
        releases=releases,
    )

