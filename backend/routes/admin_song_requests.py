# routes/admin_song_requests.py
"""
Admin review of user-submitted song requests.

The iOS/Mac apps' "Search MusicBrainz" flow POSTs to /v1/musicbrainz/request,
which records a pending row in song_requests. This blueprint lets an admin
review those requests and either approve them (creating the song + queuing
research via the same helper the import endpoint uses) or reject them.

All routes live under /admin/song-requests and are gated by the admin
before_request hook in app.py (see middleware/admin_middleware.py).
"""

import logging
from datetime import datetime, timezone

from flask import Blueprint, render_template, request, jsonify, g

import db_utils as db_tools
from core.song_research import create_song_and_queue_research

logger = logging.getLogger(__name__)

admin_song_requests_bp = Blueprint(
    'admin_song_requests', __name__, url_prefix='/admin/song-requests'
)


@admin_song_requests_bp.route('', methods=['GET'])
def song_requests_list():
    """List song requests (pending first), with requester info for review."""
    rows = db_tools.execute_query(
        """
        SELECT
            sr.id,
            sr.musicbrainz_id,
            sr.title,
            sr.composer,
            sr.status,
            sr.review_note,
            sr.created_at,
            sr.reviewed_at,
            sr.created_song_id,
            requester.email        AS requester_email,
            requester.display_name AS requester_name,
            reviewer.email         AS reviewer_email
        FROM song_requests sr
        LEFT JOIN users requester ON requester.id = sr.requested_by
        LEFT JOIN users reviewer  ON reviewer.id  = sr.reviewed_by
        ORDER BY
            CASE sr.status
                WHEN 'pending'  THEN 1
                WHEN 'approved' THEN 2
                WHEN 'rejected' THEN 3
                ELSE 4
            END,
            sr.created_at DESC
        """,
        fetch_all=True,
    ) or []

    requests = [dict(row) for row in rows]
    pending_count = sum(1 for r in requests if r['status'] == 'pending')

    return render_template(
        'admin/song_requests_list.html',
        requests=requests,
        pending_count=pending_count,
    )


@admin_song_requests_bp.route('/<request_id>/approve', methods=['POST'])
def approve_song_request(request_id):
    """Approve a pending request: create the song + queue research, then stamp
    the request as approved. If the song already exists (e.g. it was added by
    another path since the request came in), link to it without re-creating."""
    try:
        req = db_tools.execute_query(
            "SELECT id, musicbrainz_id, title, composer, status, requested_by "
            "FROM song_requests WHERE id = %s",
            (request_id,),
            fetch_one=True,
        )
        if not req:
            return jsonify({'error': 'Request not found'}), 404
        if req['status'] != 'pending':
            return jsonify({'error': f"Request is already {req['status']}"}), 409

        reviewer_id = str(g.current_user['id'])
        now = datetime.now(timezone.utc)

        # Did the song get added in the meantime?
        existing = db_tools.execute_query(
            "SELECT id, title FROM songs WHERE musicbrainz_id = %s",
            (req['musicbrainz_id'],),
            fetch_one=True,
        )

        if existing:
            song_id = str(existing['id'])
            queued = False
            logger.info(
                f"Approving song request {request_id}: song already exists "
                f"({song_id}); linking without re-creating."
            )
        else:
            song_row, queued = create_song_and_queue_research(
                musicbrainz_id=req['musicbrainz_id'],
                title=req['title'],
                composer=req['composer'],
                created_by=str(req['requested_by']) if req['requested_by'] else None,
            )
            song_id = str(song_row['id'])

        db_tools.execute_query(
            """
            UPDATE song_requests
            SET status = 'approved',
                reviewed_by = %s,
                reviewed_at = %s,
                created_song_id = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
            """,
            (reviewer_id, now, song_id, request_id),
            fetch_all=False,
        )

        logger.info(
            f"Song request {request_id} approved by {reviewer_id} "
            f"(song_id: {song_id}, research_queued: {queued})"
        )

        return jsonify({
            'success': True,
            'status': 'approved',
            'song_id': song_id,
            'research_queued': queued,
        })

    except Exception as e:
        logger.error(f"Error approving song request {request_id}: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500


@admin_song_requests_bp.route('/<request_id>/reject', methods=['POST'])
def reject_song_request(request_id):
    """Reject a pending request, optionally recording a reason."""
    try:
        data = request.get_json(silent=True) or {}
        review_note = (data.get('review_note') or '').strip() or None

        req = db_tools.execute_query(
            "SELECT id, status FROM song_requests WHERE id = %s",
            (request_id,),
            fetch_one=True,
        )
        if not req:
            return jsonify({'error': 'Request not found'}), 404
        if req['status'] != 'pending':
            return jsonify({'error': f"Request is already {req['status']}"}), 409

        reviewer_id = str(g.current_user['id'])
        now = datetime.now(timezone.utc)

        db_tools.execute_query(
            """
            UPDATE song_requests
            SET status = 'rejected',
                review_note = %s,
                reviewed_by = %s,
                reviewed_at = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
            """,
            (review_note, reviewer_id, now, request_id),
            fetch_all=False,
        )

        logger.info(f"Song request {request_id} rejected by {reviewer_id}")

        return jsonify({'success': True, 'status': 'rejected'})

    except Exception as e:
        logger.error(f"Error rejecting song request {request_id}: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500
