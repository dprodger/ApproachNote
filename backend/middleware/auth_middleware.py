"""
Authentication middleware for protecting Flask routes

This module provides decorators for:
- require_auth: Require valid JWT access token
- optional_auth: Load user if token present, but don't require it
"""

from functools import wraps
from flask import request, jsonify, g
import sys
import os

# Add parent directory to path to import auth_utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.auth_utils import decode_token
from db_utils import get_db_connection


def require_auth(f):
    """
    Decorator to require valid JWT access token
    
    Usage:
        @app.route('/protected')
        @require_auth
        def protected_route():
            user = g.current_user
            return jsonify({'user': user})
    
    The decorated function will have access to g.current_user containing:
    - id: User UUID
    - email: User email
    - display_name: User display name
    - is_active: Account active status
    - account_locked: Account locked status
    - is_admin: Whether the user has admin privileges
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Get token from Authorization header
        auth_header = request.headers.get('Authorization')
        
        if not auth_header:
            return jsonify({'error': 'No authorization header'}), 401
        
        try:
            # Expected format: "Bearer <token>"
            parts = auth_header.split(' ')
            if len(parts) != 2 or parts[0] != 'Bearer':
                return jsonify({'error': 'Invalid authorization header format'}), 401
            
            token = parts[1]
            payload = decode_token(token)
            
            # Verify token type
            if payload.get('type') != 'access':
                return jsonify({'error': 'Invalid token type'}), 401
            
            # Load user from database
            user_id = payload['user_id']
            
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT id, email, display_name, is_active, account_locked, is_admin
                        FROM users
                        WHERE id = %s
                    """, (user_id,))
                    
                    user = cur.fetchone()
                    
                    if not user:
                        return jsonify({'error': 'User not found'}), 401
                    
                    if not user['is_active']:
                        return jsonify({'error': 'Account is inactive'}), 401
                    
                    if user['account_locked']:
                        return jsonify({'error': 'Account is locked'}), 401
                    
                    # Store user in Flask's g object for use in route
                    g.current_user = user
            
            return f(*args, **kwargs)
            
        except (IndexError, ValueError) as e:
            return jsonify({'error': f'Invalid token: {str(e)}'}), 401
        except Exception as e:
            return jsonify({'error': 'Authentication failed'}), 500
    
    return decorated_function


def optional_auth(f):
    """
    Decorator that loads user if token is present, but doesn't require it
    
    Usage:
        @app.route('/public-or-protected')
        @optional_auth
        def mixed_route():
            if hasattr(g, 'current_user'):
                # User is authenticated
                return jsonify({'message': f'Hello {g.current_user["email"]}'})
            else:
                # User is not authenticated
                return jsonify({'message': 'Hello anonymous user'})
    
    If a valid token is present, g.current_user will be set.
    If no token or invalid token, g.current_user will not be set.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        auth_header = request.headers.get('Authorization')
        
        if auth_header:
            try:
                parts = auth_header.split(' ')
                if len(parts) == 2 and parts[0] == 'Bearer':
                    token = parts[1]
                    payload = decode_token(token)
                    
                    if payload.get('type') == 'access':
                        user_id = payload['user_id']
                        
                        with get_db_connection() as conn:
                            with conn.cursor() as cur:
                                cur.execute("""
                                    SELECT id, email, display_name
                                    FROM users
                                    WHERE id = %s AND is_active = true
                                """, (user_id,))
                                
                                user = cur.fetchone()
                                if user:
                                    g.current_user = user
            except:
                pass  # Invalid token, but that's okay for optional auth
        
        # If no token or invalid token, current_user will not be set
        return f(*args, **kwargs)
    
    return decorated_function