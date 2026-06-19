# routes/musicbrainz.py
"""
MusicBrainz API routes for searching and importing works
"""

from flask import Blueprint, jsonify, request, g
import logging
import db_utils as db_tools
from core import research_queue
from core.song_research import create_song_and_queue_research
from integrations.musicbrainz.utils import MusicBrainzSearcher
from middleware.auth_middleware import require_auth

logger = logging.getLogger(__name__)
musicbrainz_bp = Blueprint('musicbrainz', __name__)

# Shared MusicBrainz searcher instance
_mb_searcher = None


def get_mb_searcher():
    """Get or create a shared MusicBrainzSearcher instance"""
    global _mb_searcher
    if _mb_searcher is None:
        _mb_searcher = MusicBrainzSearcher()
    return _mb_searcher


@musicbrainz_bp.route('/musicbrainz/works/search', methods=['GET'])
def search_musicbrainz_works():
    """
    Search MusicBrainz for works (songs) by title.

    Query Parameters:
        q (str, required): Search query (song title)
        limit (int, optional): Maximum results to return (default 5, max 10)

    Returns:
        JSON with results array containing:
        - id: MusicBrainz work UUID
        - title: Work title
        - composers: Array of composer names (may be null)
        - score: Match score (0-100)
        - type: Work type (e.g., "Song")
        - musicbrainz_url: URL to MusicBrainz page
    """
    query = request.args.get('q', '').strip()

    if not query:
        return jsonify({
            'error': 'Missing required parameter: q',
            'results': []
        }), 400

    # Get and validate limit
    limit = request.args.get('limit', 5, type=int)
    limit = max(1, min(limit, 10))  # Clamp between 1 and 10

    try:
        searcher = get_mb_searcher()
        results = searcher.search_works_multi(query, limit=limit)

        return jsonify({
            'query': query,
            'results': results
        }), 200

    except Exception as e:
        logger.error(f"Error searching MusicBrainz: {e}", exc_info=True)
        return jsonify({
            'error': 'Failed to search MusicBrainz',
            'detail': str(e),
            'results': []
        }), 500


@musicbrainz_bp.route('/musicbrainz/request', methods=['POST'])
@require_auth
def request_song_from_musicbrainz():
    """
    Submit a request to add a song from MusicBrainz. Requires authentication.

    Unlike /musicbrainz/import (admin-only, immediate), this records a pending
    request in song_requests for an admin to review and approve. Nothing is
    added to the catalog until approval.

    Request Body (JSON):
        musicbrainz_id (str, required): MusicBrainz work UUID
        title (str, required): Song title
        composer (str, optional): Composer name(s)

    Returns:
        201 with the created request, or 409 if the song already exists or a
        pending request for the same work is already on file.
    """
    try:
        data = request.get_json()

        if not data:
            return jsonify({'error': 'No data provided'}), 400

        musicbrainz_id = data.get('musicbrainz_id', '').strip()
        title = data.get('title', '').strip()
        composer = data.get('composer', '').strip() if data.get('composer') else None

        requested_by = str(g.current_user['id'])

        if not musicbrainz_id:
            return jsonify({'error': 'musicbrainz_id is required'}), 400

        if not title:
            return jsonify({'error': 'title is required'}), 400

        # Already in the catalog? Nothing to request.
        existing = db_tools.execute_query(
            "SELECT id, title FROM songs WHERE musicbrainz_id = %s",
            (musicbrainz_id,),
            fetch_one=True,
        )
        if existing:
            return jsonify({
                'error': 'This song is already in the catalog',
                'existing_song': {
                    'id': str(existing['id']),
                    'title': existing['title'],
                },
            }), 409

        # Already requested and awaiting review?
        pending = db_tools.execute_query(
            "SELECT id FROM song_requests WHERE musicbrainz_id = %s AND status = 'pending'",
            (musicbrainz_id,),
            fetch_one=True,
        )
        if pending:
            return jsonify({
                'error': 'This song has already been requested and is awaiting review',
                'request_id': str(pending['id']),
            }), 409

        result = db_tools.execute_query(
            """
            INSERT INTO song_requests (musicbrainz_id, title, composer, requested_by)
            VALUES (%s, %s, %s, %s)
            RETURNING id, musicbrainz_id, title, composer, status, created_at
            """,
            (musicbrainz_id, title, composer, requested_by),
            fetch_one=True,
        )

        logger.info(
            f"Song request submitted: {title} (request_id: {result['id']}, "
            f"MB: {musicbrainz_id}, requested_by: {requested_by})"
        )

        return jsonify({
            'success': True,
            'message': 'Song request submitted for review',
            'request': {
                'id': str(result['id']),
                'musicbrainz_id': result['musicbrainz_id'],
                'title': result['title'],
                'composer': result['composer'],
                'status': result['status'],
                'created_at': result['created_at'].isoformat() if result['created_at'] else None,
            },
        }), 201

    except Exception as e:
        logger.error(f"Error submitting song request: {e}", exc_info=True)
        return jsonify({
            'error': 'Failed to submit song request',
            'detail': str(e),
        }), 500


@musicbrainz_bp.route('/musicbrainz/import', methods=['POST'])
@require_auth
def import_from_musicbrainz():
    """
    Import a song from MusicBrainz into the database and queue for research.
    Admin-only — a direct "add it now" shortcut that bypasses the song-request
    approval queue. Regular users go through /musicbrainz/request instead.

    Request Body (JSON):
        musicbrainz_id (str, required): MusicBrainz work UUID
        title (str, required): Song title
        composer (str, optional): Composer name(s)

    Returns:
        JSON with created song data and queue status
    """
    if not g.current_user.get('is_admin'):
        return jsonify({'error': 'Admin access required'}), 403

    try:
        data = request.get_json()

        if not data:
            return jsonify({'error': 'No data provided'}), 400

        musicbrainz_id = data.get('musicbrainz_id', '').strip()
        title = data.get('title', '').strip()
        composer = data.get('composer', '').strip() if data.get('composer') else None

        # Set created_by to the authenticated user's ID
        created_by = str(g.current_user['id'])

        if not musicbrainz_id:
            return jsonify({'error': 'musicbrainz_id is required'}), 400

        if not title:
            return jsonify({'error': 'title is required'}), 400

        # Check if song with this MusicBrainz ID already exists
        existing_query = "SELECT id, title FROM songs WHERE musicbrainz_id = %s"
        existing = db_tools.execute_query(existing_query, (musicbrainz_id,), fetch_one=True)

        if existing:
            return jsonify({
                'error': 'Song with this MusicBrainz ID already exists',
                'existing_song': {
                    'id': str(existing['id']),
                    'title': existing['title']
                }
            }), 409  # Conflict

        result, queued = create_song_and_queue_research(
            musicbrainz_id=musicbrainz_id,
            title=title,
            composer=composer,
            created_by=created_by,
        )

        return jsonify({
            'success': True,
            'message': 'Song imported and queued for research',
            'song': {
                'id': str(result['id']),
                'title': result['title'],
                'composer': result['composer'],
                'musicbrainz_id': result['musicbrainz_id'],
                'created_at': result['created_at'].isoformat() if result['created_at'] else None,
                'updated_at': result['updated_at'].isoformat() if result['updated_at'] else None,
                'created_by': result['created_by']
            },
            'research_queued': queued,
            'queue_size': research_queue.get_queue_size()
        }), 201

    except Exception as e:
        logger.error(f"Error importing from MusicBrainz: {e}", exc_info=True)
        return jsonify({
            'error': 'Failed to import song',
            'detail': str(e)
        }), 500
