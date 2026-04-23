# routes/research.py
from flask import Blueprint, jsonify, request
import logging
import db_utils as db_tools
from core import research_queue, research_jobs
logger = logging.getLogger(__name__)
research_bp = Blueprint('research', __name__)

@research_bp.route('/songs/<song_id>/refresh', methods=['POST'])
def refresh_song_data(song_id):
    """
    Queue a song for background research and data refresh

    This endpoint accepts a song ID and adds it to the background processing
    queue. The actual research happens asynchronously in a worker thread.

    Args:
        song_id: UUID of the song to research

    Query Parameters:
        force_refresh: If true (default), bypass cache and re-fetch all data.
                      If false, use cached data where available ("simple refresh").

    Returns:
        JSON response with queue status
    """
    try:
        # Parse force_refresh parameter (default: true for "deep refresh")
        force_refresh_param = request.args.get('force_refresh', 'true').lower()
        force_refresh = force_refresh_param in ('true', '1', 'yes')

        # First verify the song exists and get its name
        query = "SELECT id, title FROM songs WHERE id = %s"
        song = db_tools.execute_query(query, (song_id,), fetch_one=True)

        if not song:
            return jsonify({
                'error': 'Song not found',
                'song_id': song_id
            }), 404

        # Add to research queue
        success = research_queue.add_song_to_queue(song['id'], song['title'], force_refresh=force_refresh)
        
        if success:
            return jsonify({
                'success': True,
                'message': 'Song queued for research',
                'song_id': song['id'],
                'song_title': song['title'],
                'force_refresh': force_refresh,
                'queue_size': research_queue.get_queue_size()
            }), 202  # 202 Accepted - processing will happen asynchronously
        else:
            return jsonify({
                'error': 'Failed to queue song',
                'song_id': song_id
            }), 500
            
    except Exception as e:
        logger.error(f"Error queueing song {song_id} for research: {e}")
        return jsonify({
            'error': 'Internal server error',
            'detail': str(e)
        }), 500

@research_bp.route('/research/queue', methods=['GET'])
def get_queue_status():
    """Get the current status of the research queue"""
    current_song = research_queue.get_current_song()
    current_progress = research_queue.get_current_progress()
    
    response = {
        'queue_size': research_queue.get_queue_size(),
        'worker_active': research_queue._worker_running,
        'current_song': current_song,
        'progress': current_progress
    }
    
    return jsonify(response)


@research_bp.route('/research/queue/items', methods=['GET'])
def get_queue_items():
    """
    Get a list of all songs currently in the research queue
    
    Returns:
        JSON response with list of queued songs in order
    """
    try:
        queued_songs = research_queue.get_queued_songs()
        
        return jsonify({
            'success': True,
            'queue_size': len(queued_songs),
            'queued_songs': queued_songs
        }), 200
        
    except Exception as e:
        logger.error(f"Error retrieving queued songs: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': 'Internal server error',
            'detail': str(e)
        }), 500
  
# Add this route to routes/research.py

@research_bp.route('/admin/research/queue-all-songs', methods=['POST'])
def queue_all_songs_for_research():
    """
    Admin endpoint to queue songs for research

    Query Parameters:
        batch_size (int, optional): Limit number of songs to queue
        repertoire_id (uuid, optional): Only queue songs from this repertoire
        force_refresh (bool, optional): If true (default), bypass cache ("deep refresh").
                                       If false, use cached data ("simple refresh").
    """
    try:
        batch_size = request.args.get('batch_size', type=int)
        repertoire_id = request.args.get('repertoire_id')
        force_refresh_param = request.args.get('force_refresh', 'true').lower()
        force_refresh = force_refresh_param in ('true', '1', 'yes')
        
        # Build query based on parameters
        if repertoire_id:
            query = """
                SELECT s.id, s.title 
                FROM songs s
                INNER JOIN repertoire_songs rs ON s.id = rs.song_id
                WHERE rs.repertoire_id = %s
                ORDER BY s.title
            """
            params = (repertoire_id,)
        else:
            query = "SELECT id, title FROM songs ORDER BY title"
            params = None
        
        if batch_size and batch_size > 0:
            query += f" LIMIT {batch_size}"
        
        songs = db_tools.execute_query(query, params) if params else db_tools.execute_query(query)
        
        if not songs:
            return jsonify({
                'success': True,
                'message': 'No songs found in database',
                'songs_queued': 0,
                'queue_size': research_queue.get_queue_size()
            }), 200
        
        # Queue each song for research
        queued_count = 0
        failed_songs = []

        for song in songs:
            success = research_queue.add_song_to_queue(song['id'], song['title'], force_refresh=force_refresh)
            if success:
                queued_count += 1
            else:
                failed_songs.append({
                    'id': song['id'],
                    'title': song['title']
                })
        
        # Prepare response
        response_data = {
            'success': True,
            'message': f'Queued {queued_count} songs for research',
            'total_songs': len(songs),
            'songs_queued': queued_count,
            'songs_failed': len(failed_songs),
            'force_refresh': force_refresh,
            'queue_size': research_queue.get_queue_size()
        }
        
        # Include failed songs if any
        if failed_songs:
            response_data['failed_songs'] = failed_songs
            logger.warning(f"Failed to queue {len(failed_songs)} songs")
        
        logger.info(f"Admin: Queued {queued_count}/{len(songs)} songs for research")
        
        return jsonify(response_data), 202  # 202 Accepted - processing will happen asynchronously
        
    except Exception as e:
        logger.error(f"Error queuing all songs for research: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': 'Internal server error',
            'detail': str(e)
        }), 500

# ============================================================================
# Per-target research status (read-only, client-facing)
# ----------------------------------------------------------------------------
# Backed by core.research_jobs.status_for_target — returns the latest job per
# source for a given (target_type, target_id). Used by iOS/Mac clients to
# show fine-grained crawl status on song / recording / performer detail pages.
# ============================================================================


def _serialize_status_row(row: dict) -> dict:
    """Render one research_jobs row for the status response (no payload/result)."""
    return {
        'job_id': row['id'],
        'job_type': row['job_type'],
        'status': row['status'],
        'attempts': row['attempts'],
        'max_attempts': row['max_attempts'],
        'queued_at': row['created_at'].isoformat() if row.get('created_at') else None,
        'claimed_at': row['claimed_at'].isoformat() if row.get('claimed_at') else None,
        'finished_at': row['finished_at'].isoformat() if row.get('finished_at') else None,
        'next_run_at': row['run_after'].isoformat() if row.get('run_after') else None,
        'last_error': row.get('last_error'),
    }


def _status_response(target_type: str, target_id: str):
    rows = research_jobs.status_for_target(target_type, target_id)
    return jsonify({
        'target_type': target_type,
        'target_id': str(target_id),
        'sources': {row['source']: _serialize_status_row(row) for row in rows},
    })


@research_bp.route('/songs/<song_id>/research_status', methods=['GET'])
def song_research_status(song_id):
    return _status_response(research_jobs.TARGET_SONG, song_id)


@research_bp.route('/recordings/<recording_id>/research_status', methods=['GET'])
def recording_research_status(recording_id):
    return _status_response(research_jobs.TARGET_RECORDING, recording_id)


@research_bp.route('/performers/<performer_id>/research_status', methods=['GET'])
def performer_research_status(performer_id):
    return _status_response(research_jobs.TARGET_PERFORMER, performer_id)
