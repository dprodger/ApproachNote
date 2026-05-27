# routes/transcriptions.py
from flask import Blueprint, jsonify, request
import logging
import db_utils as db_tools

logger = logging.getLogger(__name__)
transcriptions_bp = Blueprint('transcriptions', __name__)

# Transcription endpoints:
# - GET /songs/<song_id>/transcriptions
# - GET /recordings/<recording_id>/transcriptions
# - GET /transcriptions/<transcription_id>





# Solo Transcriptions API Endpoints
# Add these to backend/app.py

@transcriptions_bp.route('/songs/<song_id>/transcriptions', methods=['GET'])
def get_song_transcriptions(song_id):
    """Get all solo transcriptions for a specific song"""
    try:
        query = """
            SELECT
                st.id,
                st.song_id,
                st.recording_id,
                st.youtube_url,
                st.created_at,
                st.updated_at,
                def_rel.title as album_title,
                r.recording_year,
                s.title as song_title
            FROM solo_transcriptions st
            JOIN songs s ON st.song_id = s.id
            LEFT JOIN recordings r ON st.recording_id = r.id
            LEFT JOIN releases def_rel ON r.default_release_id = def_rel.id
            WHERE st.song_id = %s
            ORDER BY r.recording_year DESC NULLS LAST
        """
        
        transcriptions = db_tools.execute_query(query, (song_id,), fetch_all=True)
        
        if not transcriptions:
            return jsonify([])
        
        return jsonify(transcriptions)
        
    except Exception as e:
        logger.error(f"Error fetching song transcriptions: {e}")
        return jsonify({'error': 'Failed to fetch transcriptions', 'detail': str(e)}), 500


@transcriptions_bp.route('/recordings/<recording_id>/transcriptions', methods=['GET'])
def get_recording_transcriptions(recording_id):
    """Get all solo transcriptions for a specific recording"""
    try:
        query = """
            SELECT
                st.id,
                st.song_id,
                st.recording_id,
                st.youtube_url,
                st.created_at,
                st.updated_at,
                s.title as song_title,
                def_rel.title as album_title,
                r.recording_year
            FROM solo_transcriptions st
            JOIN songs s ON st.song_id = s.id
            JOIN recordings r ON st.recording_id = r.id
            LEFT JOIN releases def_rel ON r.default_release_id = def_rel.id
            WHERE st.recording_id = %s
            ORDER BY st.created_at DESC
        """
        
        transcriptions = db_tools.execute_query(query, (recording_id,), fetch_all=True)
        
        if not transcriptions:
            return jsonify([])
        
        return jsonify(transcriptions)
        
    except Exception as e:
        logger.error(f"Error fetching recording transcriptions: {e}")
        return jsonify({'error': 'Failed to fetch transcriptions', 'detail': str(e)}), 500


@transcriptions_bp.route('/transcriptions/<transcription_id>', methods=['GET'])
def get_transcription_detail(transcription_id):
    """Get detailed information about a specific solo transcription"""
    try:
        query = """
            SELECT
                st.id,
                st.song_id,
                st.recording_id,
                st.youtube_url,
                st.created_at,
                st.updated_at,
                s.title as song_title,
                s.composer,
                def_rel.title as album_title,
                r.recording_year
            FROM solo_transcriptions st
            JOIN songs s ON st.song_id = s.id
            LEFT JOIN recordings r ON st.recording_id = r.id
            LEFT JOIN releases def_rel ON r.default_release_id = def_rel.id
            WHERE st.id = %s
        """
        
        transcription = db_tools.execute_query(query, (transcription_id,), fetch_one=True)
        
        if not transcription:
            return jsonify({'error': 'Transcription not found'}), 404

        return jsonify(transcription)

    except Exception as e:
        logger.error(f"Error fetching transcription detail: {e}")
        return jsonify({'error': 'Failed to fetch transcription detail', 'detail': str(e)}), 500


@transcriptions_bp.route('/transcriptions', methods=['POST'])
def create_transcription():
    """Create a new solo transcription"""
    try:
        data = request.get_json()

        if not data:
            return jsonify({'error': 'No data provided'}), 400

        song_id = data.get('song_id')
        recording_id = data.get('recording_id')  # Optional - can be None
        youtube_url = data.get('youtube_url')
        created_by = data.get('created_by')  # Optional - user ID

        if not song_id:
            return jsonify({'error': 'song_id is required'}), 400
        if not youtube_url:
            return jsonify({'error': 'youtube_url is required'}), 400

        # Validate that the song exists
        song_check = db_tools.execute_query(
            "SELECT id FROM songs WHERE id = %s",
            (song_id,),
            fetch_one=True
        )
        if not song_check:
            return jsonify({'error': 'Song not found'}), 404

        # If recording_id is provided, validate it exists and belongs to the song
        if recording_id:
            recording_check = db_tools.execute_query(
                "SELECT id FROM recordings WHERE id = %s AND song_id = %s",
                (recording_id, song_id),
                fetch_one=True
            )
            if not recording_check:
                return jsonify({'error': 'Recording not found or does not belong to this song'}), 404

            # Check for duplicate transcription (same recording + youtube URL)
            duplicate_check = db_tools.execute_query(
                "SELECT id FROM solo_transcriptions WHERE recording_id = %s AND youtube_url = %s",
                (recording_id, youtube_url),
                fetch_one=True
            )
            if duplicate_check:
                return jsonify({'error': 'A transcription with this YouTube URL already exists for this recording'}), 409
        else:
            # Check for duplicate transcription without recording (same song + youtube URL with no recording)
            duplicate_check = db_tools.execute_query(
                "SELECT id FROM solo_transcriptions WHERE song_id = %s AND recording_id IS NULL AND youtube_url = %s",
                (song_id, youtube_url),
                fetch_one=True
            )
            if duplicate_check:
                return jsonify({'error': 'A transcription with this YouTube URL already exists for this song'}), 409

        # Create the transcription using direct connection for INSERT
        with db_tools.get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO solo_transcriptions (song_id, recording_id, youtube_url, created_by)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id, song_id, recording_id, youtube_url, created_at, created_by
                """, (song_id, recording_id, youtube_url, created_by))

                result = cur.fetchone()
                conn.commit()

        if not result:
            return jsonify({'error': 'Failed to create transcription'}), 500

        logger.info(f"Created transcription {result['id']} for song {song_id}, recording {recording_id}, created_by {created_by}")

        return jsonify({
            'message': 'Transcription created successfully',
            'transcription': dict(result)
        }), 201

    except Exception as e:
        logger.error(f"Error creating transcription: {e}")
        return jsonify({'error': 'Failed to create transcription', 'detail': str(e)}), 500
