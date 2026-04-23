"""
User Repertoire Management Routes (Updated for Phase 5)

This module handles repertoire operations with user authentication:
- GET /repertoires - List user's repertoires (requires auth)
- GET /repertoires/<id> - Get repertoire details (requires auth, must be owner)
- POST /repertoires - Create new repertoire (requires auth)
- PUT /repertoires/<id> - Update repertoire (requires auth, must be owner)
- DELETE /repertoires/<id> - Delete repertoire (requires auth, must be owner)
- GET /repertoires/<id>/songs - Get songs in repertoire (requires auth, must be owner)
- POST /repertoires/<id>/songs/<song_id> - Add song to repertoire (requires auth, must be owner)
- DELETE /repertoires/<id>/songs/<song_id> - Remove song from repertoire (requires auth, must be owner)
"""

from flask import Blueprint, jsonify, request, g
import logging
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_utils import get_db_connection
from middleware.auth_middleware import require_auth

logger = logging.getLogger(__name__)
repertoires_bp = Blueprint('repertoires', __name__, url_prefix='/repertoires')


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def verify_repertoire_ownership(repertoire_id, user_id):
    """
    Verify that the user owns the specified repertoire
    
    Returns:
        (bool, dict or None): (is_owner, repertoire_data)
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, name, description, user_id, created_at, updated_at
                    FROM repertoires
                    WHERE id = %s
                """, (repertoire_id,))
                
                repertoire = cur.fetchone()
                
                if not repertoire:
                    return False, None
                
                # Check ownership
                if repertoire['user_id'] != user_id:
                    return False, repertoire
                
                return True, repertoire
                
    except Exception as e:
        logger.error(f"Error verifying repertoire ownership: {e}", exc_info=True)
        return False, None


# =============================================================================
# REPERTOIRE CRUD ENDPOINTS
# =============================================================================

@repertoires_bp.route('/', methods=['GET'])
@require_auth
def get_user_repertoires():
    """
    Get all repertoires for the authenticated user
    
    Returns:
        200: [
            {
                "id": "uuid",
                "name": "Gig Standards",
                "description": "Essential standards for gigs",
                "song_count": 42,
                "created_at": "2025-01-15T10:30:00Z",
                "updated_at": "2025-01-20T14:22:00Z"
            },
            ...
        ]
        500: Server error
    """
    user_id = g.current_user['id']
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        r.id,
                        r.name,
                        r.description,
                        r.created_at,
                        r.updated_at,
                        COUNT(rs.song_id) as song_count
                    FROM repertoires r
                    LEFT JOIN repertoire_songs rs ON r.id = rs.repertoire_id
                    WHERE r.user_id = %s
                    GROUP BY r.id, r.name, r.description, r.created_at, r.updated_at
                    ORDER BY r.updated_at DESC
                """, (user_id,))
                
                repertoires = cur.fetchall()
                
                return jsonify([dict(rep) for rep in repertoires]), 200
                
    except Exception as e:
        logger.error(f"Error fetching repertoires: {e}", exc_info=True)
        return jsonify({'error': 'Failed to fetch repertoires'}), 500


@repertoires_bp.route('/<repertoire_id>', methods=['GET'])
@require_auth
def get_repertoire_detail(repertoire_id):
    """
    Get detailed information about a specific repertoire
    
    Returns:
        200: Repertoire with full song details
        403: User doesn't own this repertoire
        404: Repertoire not found
        500: Server error
    """
    user_id = g.current_user['id']
    
    # Verify ownership
    is_owner, repertoire = verify_repertoire_ownership(repertoire_id, user_id)
    
    if not repertoire:
        return jsonify({'error': 'Repertoire not found'}), 404
    
    if not is_owner:
        return jsonify({'error': 'You do not have access to this repertoire'}), 403
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Get songs in this repertoire
                cur.execute("""
                    SELECT 
                        s.id,
                        s.title,
                        s.composer,
                        s.structure,
                        s.external_references,
                        rs.created_at as added_at
                    FROM songs s
                    INNER JOIN repertoire_songs rs ON s.id = rs.song_id
                    WHERE rs.repertoire_id = %s
                    ORDER BY s.title
                """, (repertoire_id,))
                
                songs = cur.fetchall()
                
                # Build response
                result = dict(repertoire)
                result['songs'] = [dict(song) for song in songs]
                result['song_count'] = len(songs)
                
                return jsonify(result), 200
                
    except Exception as e:
        logger.error(f"Error fetching repertoire detail: {e}", exc_info=True)
        return jsonify({'error': 'Failed to fetch repertoire details'}), 500


@repertoires_bp.route('/', methods=['POST'])
@require_auth
def create_repertoire():
    """
    Create a new repertoire for the authenticated user
    
    Request body:
        {
            "name": "My Gig Standards",
            "description": "Songs I play at gigs" // optional
        }
    
    Returns:
        201: {"id": "uuid", "name": "...", "description": "...", ...}
        400: Invalid input
        500: Server error
    """
    user_id = g.current_user['id']
    data = request.get_json()
    
    name = data.get('name', '').strip()
    description = data.get('description', '').strip()
    
    if not name:
        return jsonify({'error': 'Repertoire name is required'}), 400
    
    if len(name) > 255:
        return jsonify({'error': 'Name must be 255 characters or less'}), 400
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO repertoires (name, description, user_id)
                    VALUES (%s, %s, %s)
                    RETURNING id, name, description, created_at, updated_at
                """, (name, description if description else None, user_id))
                
                repertoire = cur.fetchone()
                conn.commit()
                
                logger.info(f"User {user_id} created repertoire: {name}")
                
                return jsonify(dict(repertoire)), 201
                
    except Exception as e:
        logger.error(f"Error creating repertoire: {e}", exc_info=True)
        return jsonify({'error': 'Failed to create repertoire'}), 500


@repertoires_bp.route('/<repertoire_id>', methods=['PUT'])
@require_auth
def update_repertoire(repertoire_id):
    """
    Update a repertoire (name/description)
    
    Request body (all optional):
        {
            "name": "Updated Name",
            "description": "Updated description"
        }
    
    Returns:
        200: Updated repertoire
        400: Invalid input
        403: User doesn't own this repertoire
        404: Repertoire not found
        500: Server error
    """
    user_id = g.current_user['id']
    data = request.get_json()
    
    # Verify ownership
    is_owner, _ = verify_repertoire_ownership(repertoire_id, user_id)
    
    if not is_owner:
        return jsonify({'error': 'Repertoire not found or access denied'}), 404
    
    name = data.get('name', '').strip() if 'name' in data else None
    description = data.get('description', '').strip() if 'description' in data else None
    
    if name is not None and not name:
        return jsonify({'error': 'Name cannot be empty'}), 400
    
    if name and len(name) > 255:
        return jsonify({'error': 'Name must be 255 characters or less'}), 400
    
    # Build update query
    updates = []
    values = []
    
    if name is not None:
        updates.append("name = %s")
        values.append(name)
    
    if description is not None:
        updates.append("description = %s")
        values.append(description)
    
    if not updates:
        return jsonify({'error': 'No fields to update'}), 400
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                values.append(repertoire_id)
                
                query = f"""
                    UPDATE repertoires
                    SET {', '.join(updates)}, updated_at = NOW()
                    WHERE id = %s
                    RETURNING id, name, description, created_at, updated_at
                """
                
                cur.execute(query, values)
                repertoire = cur.fetchone()
                conn.commit()
                
                logger.info(f"User {user_id} updated repertoire {repertoire_id}")
                
                return jsonify(dict(repertoire)), 200
                
    except Exception as e:
        logger.error(f"Error updating repertoire: {e}", exc_info=True)
        return jsonify({'error': 'Failed to update repertoire'}), 500


@repertoires_bp.route('/<repertoire_id>', methods=['DELETE'])
@require_auth
def delete_repertoire(repertoire_id):
    """
    Delete a repertoire (and all its song associations)
    
    Returns:
        200: {"message": "Repertoire deleted"}
        403: User doesn't own this repertoire
        404: Repertoire not found
        500: Server error
    """
    user_id = g.current_user['id']
    
    # Verify ownership
    is_owner, _ = verify_repertoire_ownership(repertoire_id, user_id)
    
    if not is_owner:
        return jsonify({'error': 'Repertoire not found or access denied'}), 404
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Delete repertoire (cascade will delete repertoire_songs)
                cur.execute("""
                    DELETE FROM repertoires
                    WHERE id = %s
                    RETURNING id
                """, (repertoire_id,))
                
                result = cur.fetchone()
                conn.commit()
                
                if result:
                    logger.info(f"User {user_id} deleted repertoire {repertoire_id}")
                    return jsonify({'message': 'Repertoire deleted'}), 200
                else:
                    return jsonify({'error': 'Repertoire not found'}), 404
                
    except Exception as e:
        logger.error(f"Error deleting repertoire: {e}", exc_info=True)
        return jsonify({'error': 'Failed to delete repertoire'}), 500


# =============================================================================
# REPERTOIRE SONGS ENDPOINTS
# =============================================================================

@repertoires_bp.route('/<repertoire_id>/songs', methods=['GET'])
@require_auth
def get_repertoire_songs(repertoire_id):
    """
    Get all songs in a repertoire (with optional search/filter)
    
    Query params:
        ?search=<query> - Filter songs by title or composer
    
    Returns:
        200: List of songs
        403: User doesn't own this repertoire
        404: Repertoire not found
        500: Server error
    """
    user_id = g.current_user['id']
    search_query = request.args.get('search', '').strip()
    
    # Verify ownership
    is_owner, _ = verify_repertoire_ownership(repertoire_id, user_id)
    
    if not is_owner:
        return jsonify({'error': 'Repertoire not found or access denied'}), 404
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                if search_query:
                    cur.execute("""
                        SELECT
                            s.id,
                            s.title,
                            s.composer,
                            s.structure,
                            s.external_references,
                            rs.created_at as added_at
                        FROM songs s
                        INNER JOIN repertoire_songs rs ON s.id = rs.song_id
                        WHERE rs.repertoire_id = %s
                          AND (unaccent(s.title) ILIKE unaccent(%s)
                               OR unaccent(s.composer) ILIKE unaccent(%s)
                               OR EXISTS (SELECT 1 FROM unnest(s.alt_titles) AS alt WHERE unaccent(alt) ILIKE unaccent(%s)))
                        ORDER BY s.title
                    """, (repertoire_id, f'%{search_query}%', f'%{search_query}%', f'%{search_query}%'))
                else:
                    cur.execute("""
                        SELECT 
                            s.id,
                            s.title,
                            s.composer,
                            s.structure,
                            s.external_references,
                            rs.created_at as added_at
                        FROM songs s
                        INNER JOIN repertoire_songs rs ON s.id = rs.song_id
                        WHERE rs.repertoire_id = %s
                        ORDER BY s.title
                    """, (repertoire_id,))
                
                songs = cur.fetchall()
                
                return jsonify([dict(song) for song in songs]), 200
                
    except Exception as e:
        logger.error(f"Error fetching repertoire songs: {e}", exc_info=True)
        return jsonify({'error': 'Failed to fetch songs'}), 500


@repertoires_bp.route('/<repertoire_id>/songs/<song_id>', methods=['POST'])
@require_auth
def add_song_to_repertoire(repertoire_id, song_id):
    """
    Add a song to a repertoire
    
    Returns:
        201: {"message": "Song added to repertoire"}
        403: User doesn't own this repertoire
        404: Repertoire or song not found
        409: Song already in repertoire
        500: Server error
    """
    user_id = g.current_user['id']
    
    # Verify ownership
    is_owner, _ = verify_repertoire_ownership(repertoire_id, user_id)
    
    if not is_owner:
        return jsonify({'error': 'Repertoire not found or access denied'}), 404
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Verify song exists
                cur.execute("SELECT id FROM songs WHERE id = %s", (song_id,))
                if not cur.fetchone():
                    return jsonify({'error': 'Song not found'}), 404
                
                # Check if already in repertoire
                cur.execute("""
                    SELECT id FROM repertoire_songs
                    WHERE repertoire_id = %s AND song_id = %s
                """, (repertoire_id, song_id))
                
                if cur.fetchone():
                    return jsonify({'error': 'Song already in repertoire'}), 409
                
                # Add to repertoire
                cur.execute("""
                    INSERT INTO repertoire_songs (repertoire_id, song_id)
                    VALUES (%s, %s)
                """, (repertoire_id, song_id))
                
                conn.commit()
                
                logger.info(f"User {user_id} added song {song_id} to repertoire {repertoire_id}")
                
                return jsonify({'message': 'Song added to repertoire'}), 201
                
    except Exception as e:
        logger.error(f"Error adding song to repertoire: {e}", exc_info=True)
        return jsonify({'error': 'Failed to add song'}), 500


@repertoires_bp.route('/<repertoire_id>/songs/<song_id>', methods=['DELETE'])
@require_auth
def remove_song_from_repertoire(repertoire_id, song_id):
    """
    Remove a song from a repertoire
    
    Returns:
        200: {"message": "Song removed from repertoire"}
        403: User doesn't own this repertoire
        404: Repertoire, song, or association not found
        500: Server error
    """
    user_id = g.current_user['id']
    
    # Verify ownership
    is_owner, _ = verify_repertoire_ownership(repertoire_id, user_id)
    
    if not is_owner:
        return jsonify({'error': 'Repertoire not found or access denied'}), 404
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Remove from repertoire
                cur.execute("""
                    DELETE FROM repertoire_songs
                    WHERE repertoire_id = %s AND song_id = %s
                    RETURNING id
                """, (repertoire_id, song_id))
                
                result = cur.fetchone()
                conn.commit()
                
                if result:
                    logger.info(f"User {user_id} removed song {song_id} from repertoire {repertoire_id}")
                    return jsonify({'message': 'Song removed from repertoire'}), 200
                else:
                    return jsonify({'error': 'Song not found in repertoire'}), 404
                
    except Exception as e:
        logger.error(f"Error removing song from repertoire: {e}", exc_info=True)
        return jsonify({'error': 'Failed to remove song'}), 500