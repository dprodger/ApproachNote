"""
Authentication routes: register, login, token refresh, logout

This module handles core authentication operations including:
- User registration with email/password
- User login and token generation
- Access token refresh using refresh tokens
- User logout and token revocation
- Current user information retrieval
"""

from flask import Blueprint, request, jsonify, g
import sys
import os
import logging

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_utils import get_db_connection
from core.auth_utils import (
    hash_password,
    verify_password,
    generate_access_token,
    generate_refresh_token,
    decode_token
)
from middleware.auth_middleware import require_auth
from core.email_service import send_welcome_email
from rate_limit import (
    limiter,
    LOGIN_LIMIT,
    REGISTER_LIMIT,
    GOOGLE_LOGIN_LIMIT,
    APPLE_LOGIN_LIMIT,
    REFRESH_TOKEN_LIMIT,
)

# Google OAuth imports (ADD THESE LINES)
from google.auth.transport import requests as google_requests
from google.oauth2 import id_token as google_id_token

# Sign in with Apple: Apple identity tokens are RS256 JWTs signed with
# rotating public keys published at Apple's JWKS endpoint. PyJWT handles
# fetching/caching the JWKS and verifying the signature.
import jwt
from jwt import PyJWKClient, InvalidTokenError, PyJWKClientError

logger = logging.getLogger(__name__)
auth_bp = Blueprint('auth', __name__, url_prefix='/auth')

# Google OAuth configuration (NEW - ADD THIS)
GOOGLE_CLIENT_ID = os.getenv('GOOGLE_CLIENT_ID')

# Sign in with Apple configuration.
# APPLE_BUNDLE_IDS is a comma-separated list of bundle IDs / Services IDs we
# accept as the `aud` claim on Apple identity tokens — typically the iOS app
# bundle and the Mac app bundle.
APPLE_ISSUER = 'https://appleid.apple.com'
APPLE_JWKS_URL = 'https://appleid.apple.com/auth/keys'
APPLE_BUNDLE_IDS = [
    bid.strip() for bid in os.getenv('APPLE_BUNDLE_IDS', '').split(',') if bid.strip()
]
# The JWKS client caches keys in-process; safe to instantiate once.
_apple_jwk_client = PyJWKClient(APPLE_JWKS_URL, cache_keys=True)

@auth_bp.route('/register', methods=['POST'])
@limiter.limit(REGISTER_LIMIT)
def register():
    """
    Register new user with email and password
    
    Request body:
        {
            "email": "user@example.com",
            "password": "password123",
            "display_name": "John Doe" (optional)
        }
    
    Returns:
        201: {
            "user": {"id": "...", "email": "...", "display_name": "..."},
            "access_token": "...",
            "refresh_token": "..."
        }
        400: Invalid input
        409: Email already registered
        500: Server error
    """
    data = request.get_json()
    
    email = data.get('email')
    password = data.get('password')
    display_name = data.get('display_name')

    # Validation
    if not email or not password:
        return jsonify({'error': 'Email and password required'}), 400
    
    if len(password) < 8:
        return jsonify({'error': 'Password must be at least 8 characters'}), 400
    
    # Basic email validation
    if '@' not in email or '.' not in email:
        return jsonify({'error': 'Invalid email format'}), 400
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Check if email already exists
                cur.execute("SELECT id FROM users WHERE email = %s", (email,))
                if cur.fetchone():
                    return jsonify({'error': 'Email already registered'}), 409
                
                # Create user
                password_hash = hash_password(password)
                
                cur.execute("""
                    INSERT INTO users (email, password_hash, display_name)
                    VALUES (%s, %s, %s)
                    RETURNING id, email, display_name, created_at
                """, (email, password_hash, display_name))
                
                user = cur.fetchone()
                conn.commit()
                
                # Generate tokens
                access_token = generate_access_token(user['id'])
                refresh_token = generate_refresh_token(user['id'])
                
                # Store refresh token
                cur.execute("""
                    INSERT INTO refresh_tokens (user_id, token, expires_at)
                    VALUES (%s, %s, NOW() + INTERVAL '30 days')
                """, (user['id'], refresh_token))
                conn.commit()
                
                logger.info(f"User registered: {email}")
                
                # Send welcome email (non-blocking - don't fail registration if email fails)
                try:
                    send_welcome_email(email, display_name)
                except Exception as email_error:
                    logger.warning(f"Welcome email failed for {email}: {email_error}")
                
                return jsonify({
                    'user': {
                        'id': str(user['id']),
                        'email': user['email'],
                        'display_name': user['display_name']
                    },
                    'access_token': access_token,
                    'refresh_token': refresh_token
                }), 201
                
    except Exception as e:
        logger.error(f"Registration error: {e}", exc_info=True)
        return jsonify({'error': 'Registration failed'}), 500


@auth_bp.route('/login', methods=['POST'])
@limiter.limit(LOGIN_LIMIT)
def login():
    """
    Login with email and password
    
    Request body:
        {
            "email": "user@example.com",
            "password": "password123"
        }
    
    Returns:
        200: {
            "user": {"id": "...", "email": "...", "display_name": "..."},
            "access_token": "...",
            "refresh_token": "..."
        }
        400: Invalid input
        401: Invalid credentials or account locked
        500: Server error
    """
    data = request.get_json()
    
    email = data.get('email')
    password = data.get('password')

    if not email or not password:
        return jsonify({'error': 'Email and password required'}), 400
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Get user
                cur.execute("""
                    SELECT id, email, password_hash, display_name, 
                           is_active, account_locked, failed_login_attempts
                    FROM users
                    WHERE email = %s
                """, (email,))
                
                user = cur.fetchone()
                
                if not user:
                    return jsonify({'error': 'Invalid credentials'}), 401
                
                # Check account status
                if not user['is_active']:
                    return jsonify({'error': 'Account is inactive'}), 401
                
                if user['account_locked']:
                    return jsonify({'error': 'Account is locked. Please contact support.'}), 401
                
                # Verify password
                if not verify_password(password, user['password_hash']):
                    # Increment failed attempts
                    cur.execute("""
                        UPDATE users
                        SET failed_login_attempts = failed_login_attempts + 1,
                            last_failed_login_at = NOW(),
                            account_locked = CASE 
                                WHEN failed_login_attempts >= 4 THEN true 
                                ELSE false 
                            END
                        WHERE id = %s
                    """, (user['id'],))
                    conn.commit()
                    
                    return jsonify({'error': 'Invalid credentials'}), 401
                
                # Reset failed attempts on successful login
                cur.execute("""
                    UPDATE users
                    SET failed_login_attempts = 0,
                        last_login_at = NOW()
                    WHERE id = %s
                """, (user['id'],))
                conn.commit()
                
                # Generate tokens
                access_token = generate_access_token(user['id'])
                refresh_token = generate_refresh_token(user['id'])
                
                # Store refresh token
                cur.execute("""
                    INSERT INTO refresh_tokens (user_id, token, expires_at)
                    VALUES (%s, %s, NOW() + INTERVAL '30 days')
                """, (user['id'], refresh_token))
                conn.commit()
                
                logger.info(f"User logged in: {email}")
                
                return jsonify({
                    'user': {
                        'id': str(user['id']),
                        'email': user['email'],
                        'display_name': user['display_name']
                    },
                    'access_token': access_token,
                    'refresh_token': refresh_token
                }), 200
                
    except Exception as e:
        logger.error(f"Login error: {e}", exc_info=True)
        return jsonify({'error': 'Login failed'}), 500


@auth_bp.route('/refresh-token', methods=['POST'])
@limiter.limit(REFRESH_TOKEN_LIMIT)
def refresh_token():
    """
    Get new access token using refresh token
    
    Request body:
        {
            "refresh_token": "..."
        }
    
    Returns:
        200: {
            "access_token": "...",
            "refresh_token": "..." (new refresh token)
        }
        400: Missing refresh token
        401: Invalid or expired refresh token
        500: Server error
    """
    data = request.get_json()
    refresh_token_value = data.get('refresh_token')
    
    if not refresh_token_value:
        return jsonify({'error': 'Refresh token required'}), 400
    
    try:
        # Decode token
        payload = decode_token(refresh_token_value)
        
        if payload.get('type') != 'refresh':
            return jsonify({'error': 'Invalid token type'}), 401
        
        user_id = payload['user_id']
        
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Verify refresh token exists and is not revoked
                cur.execute("""
                    SELECT id FROM refresh_tokens
                    WHERE token = %s 
                    AND user_id = %s
                    AND revoked_at IS NULL
                    AND expires_at > NOW()
                """, (refresh_token_value, user_id))
                
                if not cur.fetchone():
                    return jsonify({'error': 'Invalid or expired refresh token'}), 401
                
                # Generate new access token
                access_token = generate_access_token(user_id)
                
                # Rotate refresh token (more secure)
                new_refresh_token = generate_refresh_token(user_id)
                
                # Revoke old refresh token
                cur.execute("""
                    UPDATE refresh_tokens
                    SET revoked_at = NOW()
                    WHERE token = %s
                """, (refresh_token_value,))
                
                # Store new refresh token
                cur.execute("""
                    INSERT INTO refresh_tokens (user_id, token, expires_at)
                    VALUES (%s, %s, NOW() + INTERVAL '30 days')
                """, (user_id, new_refresh_token))
                
                conn.commit()
                
                return jsonify({
                    'access_token': access_token,
                    'refresh_token': new_refresh_token
                }), 200
                
    except ValueError as e:
        return jsonify({'error': str(e)}), 401
    except Exception as e:
        logger.error(f"Token refresh error: {e}", exc_info=True)
        return jsonify({'error': 'Token refresh failed'}), 500


@auth_bp.route('/me', methods=['GET'])
@require_auth
def get_current_user():
    """
    Get current user info (requires valid access token)
    
    Headers:
        Authorization: Bearer <access_token>
    
    Returns:
        200: {
            "id": "...",
            "email": "...",
            "display_name": "..."
        }
        401: Invalid or missing token
    """
    user = g.current_user
    
    return jsonify({
        'id': str(user['id']),
        'email': user['email'],
        'display_name': user['display_name']
    }), 200


@auth_bp.route('/logout', methods=['POST'])
@require_auth
def logout():
    """
    Logout and revoke refresh token
    
    Headers:
        Authorization: Bearer <access_token>
    
    Request body:
        {
            "refresh_token": "..." (optional)
        }
    
    Returns:
        200: {"message": "Logged out successfully"}
        401: Invalid or missing token
    """
    data = request.get_json() or {}
    refresh_token_value = data.get('refresh_token')
    
    if refresh_token_value:
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    # Revoke refresh token
                    cur.execute("""
                        UPDATE refresh_tokens
                        SET revoked_at = NOW()
                        WHERE token = %s AND user_id = %s
                    """, (refresh_token_value, g.current_user['id']))
                    conn.commit()
        except Exception as e:
            logger.error(f"Logout error: {e}", exc_info=True)
    
    return jsonify({'message': 'Logged out successfully'}), 200
    
@auth_bp.route('/google', methods=['POST'])
@limiter.limit(GOOGLE_LOGIN_LIMIT)
def google_login():
    """
    Authenticate with Google ID token
    
    Request body:
        {
            "id_token": "eyJhbGciOiJSUzI1NiIsImtpZCI6..."
        }
    
    Returns:
        200: {
            "user": {"id": "...", "email": "...", "display_name": "..."},
            "access_token": "...",
            "refresh_token": "..."
        }
        400: Invalid input
        401: Invalid token
        500: Server error
    """
    data = request.get_json()
    id_token_str = data.get('id_token')
    
    if not id_token_str:
        return jsonify({'error': 'ID token required'}), 400
    
    if not GOOGLE_CLIENT_ID:
        logger.error("GOOGLE_CLIENT_ID not configured")
        return jsonify({'error': 'Google authentication not configured'}), 500
    
    try:
        # Verify Google ID token
        idinfo = google_id_token.verify_oauth2_token(
            id_token_str, 
            google_requests.Request(), 
            GOOGLE_CLIENT_ID
        )
        
        # Extract user information from token
        google_id = idinfo['sub']
        email = idinfo['email']
        display_name = idinfo.get('name')
        profile_image = idinfo.get('picture')
        
        logger.info(f"🔐 Google login attempt for: {email}")
        
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Check if user exists with this Google ID or email
                cur.execute("""
                    SELECT id, email, display_name, google_id
                    FROM users
                    WHERE google_id = %s OR email = %s
                """, (google_id, email))
                
                user = cur.fetchone()
                
                if user:
                    # User exists
                    user_id = user['id']

                    # Update Google ID if not set (linking existing email account)
                    if not user.get('google_id'):
                        logger.info(f"🔗 Linking Google account to existing user: {email}")
                        cur.execute("""
                            UPDATE users
                            SET google_id = %s,
                                email_verified = true,
                                profile_image_url = %s,
                                last_login_at = NOW(),
                                updated_at = CURRENT_TIMESTAMP
                            WHERE id = %s
                        """, (google_id, profile_image, user_id))
                        conn.commit()
                    else:
                        # Existing Google user - just update last_login_at
                        cur.execute("""
                            UPDATE users
                            SET last_login_at = NOW()
                            WHERE id = %s
                        """, (user_id,))
                        conn.commit()
                else:
                    # Create new user
                    logger.info(f"✨ Creating new user via Google: {email}")
                    cur.execute("""
                        INSERT INTO users (
                            email, google_id, display_name,
                            profile_image_url, email_verified, last_login_at
                        )
                        VALUES (%s, %s, %s, %s, true, NOW())
                        RETURNING id
                    """, (email, google_id, display_name, profile_image))
                    
                    result = cur.fetchone()
                    user_id = result['id']
                    conn.commit()
                
                # Generate tokens
                access_token = generate_access_token(user_id)
                refresh_token = generate_refresh_token(user_id)
                
                # Store refresh token
                cur.execute("""
                    INSERT INTO refresh_tokens (user_id, token, expires_at)
                    VALUES (%s, %s, NOW() + INTERVAL '30 days')
                """, (user_id, refresh_token))
                conn.commit()
                
                # Get complete user details
                cur.execute("""
                    SELECT id, email, display_name, profile_image_url, email_verified
                    FROM users WHERE id = %s
                """, (user_id,))
                
                user_data = cur.fetchone()
                
                logger.info(f"✅ Google login successful for: {email}")
                
                return jsonify({
                    'user': {
                        'id': str(user_data['id']),
                        'email': user_data['email'],
                        'display_name': user_data['display_name'],
                        'profile_image_url': user_data.get('profile_image_url'),
                        'email_verified': user_data.get('email_verified', False)
                    },
                    'access_token': access_token,
                    'refresh_token': refresh_token
                }), 200
                
    except ValueError as e:
        logger.warning(f"Invalid Google token: {e}")
        return jsonify({'error': f'Invalid token: {str(e)}'}), 401
    except Exception as e:
        logger.error(f"Google login error: {e}", exc_info=True)
        return jsonify({'error': 'Authentication failed'}), 500


@auth_bp.route('/apple', methods=['POST'])
@limiter.limit(APPLE_LOGIN_LIMIT)
def apple_login():
    """
    Authenticate with a Sign in with Apple identity token.

    Request body:
        {
            "identity_token": "eyJraWQiOiJ...",    # required, Apple's JWT
            "full_name": "Ada Lovelace",            # optional; only sent on first
                                                    # auth, since Apple only
                                                    # returns fullName once
            "email": "ada@example.com",             # optional; first-auth only,
                                                    # from credential.email.
                                                    # Used only when the token
                                                    # has no email claim.
            "authorization_code": "..."             # optional; reserved for
                                                    # future server-side refresh
        }

    Returns:
        200: {user, access_token, refresh_token}  — same shape as /auth/google
        400: identity_token missing
        401: token invalid (signature, iss/aud/exp)
        500: server misconfigured or DB error

    Notes on Apple's quirks:
    - `email` is only present in the token on the FIRST sign-in. Subsequent
      tokens for the same user omit it. We look up by `apple_id` primarily.
    - If a user opts to hide their email we get a `...@privaterelay.appleid.com`
      relay address; we store it verbatim.
    - `fullName` is never in the identity token itself; it's in the
      authorization credential and only present on first auth. The client is
      responsible for forwarding it via the `full_name` field below.
    """
    data = request.get_json() or {}
    identity_token = data.get('identity_token')
    client_full_name = data.get('full_name')
    # Client-supplied fallback from ASAuthorizationAppleIDCredential.email.
    # Apple sometimes omits email from the JWT even on first auth (notably
    # with Hide My Email), but the client-side credential is authoritative
    # on first auth. We prefer the token's email when both are present.
    client_email = data.get('email')

    if not identity_token:
        return jsonify({'error': 'identity_token required'}), 400

    if not APPLE_BUNDLE_IDS:
        logger.error("APPLE_BUNDLE_IDS not configured")
        return jsonify({'error': 'Apple authentication not configured'}), 500

    try:
        # Fetch the signing key matching the token's `kid` header.
        signing_key = _apple_jwk_client.get_signing_key_from_jwt(identity_token)

        # PyJWT verifies signature, iss, aud, and exp in one pass.
        claims = jwt.decode(
            identity_token,
            signing_key.key,
            algorithms=['RS256'],
            audience=APPLE_BUNDLE_IDS,
            issuer=APPLE_ISSUER,
        )
    except (InvalidTokenError, PyJWKClientError) as e:
        # Includes malformed tokens, bad signatures, expired, wrong iss/aud,
        # and unknown `kid` (which happens when a forged/malformed token
        # references a signing key Apple never published).
        logger.warning(f"Invalid Apple token: {e}")
        return jsonify({'error': f'Invalid token: {str(e)}'}), 401
    except Exception as e:
        logger.error(f"Apple token verification error: {e}", exc_info=True)
        return jsonify({'error': 'Authentication failed'}), 500

    apple_id = claims.get('sub')
    token_email = claims.get('email')  # may be None on re-auth
    email_verified_claim = claims.get('email_verified')
    # Apple returns email_verified as a bool or the string "true"
    email_verified = (
        email_verified_claim is True
        or str(email_verified_claim).lower() == 'true'
    )

    if not apple_id:
        logger.warning("Apple token missing `sub` claim")
        return jsonify({'error': 'Invalid token: missing subject'}), 401

    # Prefer token email (verified by Apple's signature); fall back to the
    # client-supplied credential.email only when the token has none. If we
    # fall back, email_verified isn't asserted by the token, so leave it
    # false unless the token said otherwise.
    effective_email = token_email or client_email

    logger.info(f"🔐 Apple login attempt for sub: {apple_id} (email: {effective_email or 'not provided'})")

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Primary match on apple_id; fall back to email for linking.
                # We query on email only if we actually have one (first-auth),
                # otherwise the OR branch would match NULL = NULL (never).
                if effective_email:
                    cur.execute(
                        """
                        SELECT id, email, display_name, apple_id
                        FROM users
                        WHERE apple_id = %s OR email = %s
                        """,
                        (apple_id, effective_email),
                    )
                else:
                    cur.execute(
                        """
                        SELECT id, email, display_name, apple_id
                        FROM users
                        WHERE apple_id = %s
                        """,
                        (apple_id,),
                    )

                user = cur.fetchone()

                if user:
                    user_id = user['id']
                    stored_apple_id = user.get('apple_id')

                    if not stored_apple_id:
                        # Linking an existing email/password or Google account.
                        logger.info(f"🔗 Linking Apple account to existing user: {user.get('email')}")
                        cur.execute(
                            """
                            UPDATE users
                            SET apple_id = %s,
                                email_verified = COALESCE(email_verified, %s),
                                last_login_at = NOW(),
                                updated_at = CURRENT_TIMESTAMP
                            WHERE id = %s
                            """,
                            (apple_id, email_verified, user_id),
                        )
                    elif stored_apple_id != apple_id:
                        # Same human, new `sub`. This happens when the app's
                        # bundle ID changes and Apple issues a fresh subject
                        # claim for the same user (#145). We only reach this
                        # branch via the email-match arm of the lookup query,
                        # so the user's identity by email is intact — re-link
                        # the row to the new sub so future re-auths (which
                        # send only `sub`, no email) still find this user
                        # instead of spawning a duplicate account.
                        logger.warning(
                            "🔄 Apple sub changed for user %s "
                            "(stored=%s, token=%s, email_verified=%s). "
                            "Re-linking via email match.",
                            user_id, stored_apple_id, apple_id, email_verified,
                        )
                        cur.execute(
                            """
                            UPDATE users
                            SET apple_id = %s,
                                last_login_at = NOW(),
                                updated_at = CURRENT_TIMESTAMP
                            WHERE id = %s
                            """,
                            (apple_id, user_id),
                        )
                    else:
                        cur.execute(
                            """
                            UPDATE users
                            SET last_login_at = NOW()
                            WHERE id = %s
                            """,
                            (user_id,),
                        )
                    conn.commit()
                else:
                    # Brand-new user. We require an email to create the row
                    # (users.email is NOT NULL). Apple sends email in the
                    # token on first auth, or — when that's missing — the
                    # client forwards credential.email from the authorization
                    # response. If we have neither, refuse cleanly and tell
                    # the user how to recover: revoking the grant in Apple ID
                    # settings forces Apple to re-send email scope next time.
                    if not effective_email:
                        logger.warning(
                            "Apple login: new sub %s with no email from "
                            "token or client; refusing to create row. User "
                            "must revoke the grant in Settings → Apple ID "
                            "→ Sign-In & Security and retry.",
                            apple_id,
                        )
                        return jsonify({
                            'error': (
                                'Apple sign-in needs to be re-authorized. '
                                'Open Settings → your name → Sign-In & '
                                'Security → Sign in with Apple, remove this '
                                'app, then sign in again.'
                            )
                        }), 401
                    logger.info(f"✨ Creating new user via Apple: {effective_email}")
                    cur.execute(
                        """
                        INSERT INTO users (
                            email, apple_id, display_name,
                            email_verified, last_login_at
                        )
                        VALUES (%s, %s, %s, %s, NOW())
                        RETURNING id
                        """,
                        (
                            effective_email,
                            apple_id,
                            client_full_name,
                            email_verified,
                        ),
                    )
                    user_id = cur.fetchone()['id']
                    conn.commit()

                # Issue our own tokens (identical to Google flow)
                access_token = generate_access_token(user_id)
                refresh_token = generate_refresh_token(user_id)

                cur.execute(
                    """
                    INSERT INTO refresh_tokens (user_id, token, expires_at)
                    VALUES (%s, %s, NOW() + INTERVAL '30 days')
                    """,
                    (user_id, refresh_token),
                )
                conn.commit()

                cur.execute(
                    """
                    SELECT id, email, display_name, profile_image_url, email_verified
                    FROM users WHERE id = %s
                    """,
                    (user_id,),
                )
                user_data = cur.fetchone()

                logger.info(f"✅ Apple login successful for: {user_data.get('email') or apple_id}")

                return jsonify({
                    'user': {
                        'id': str(user_data['id']),
                        'email': user_data['email'],
                        'display_name': user_data['display_name'],
                        'profile_image_url': user_data.get('profile_image_url'),
                        'email_verified': user_data.get('email_verified', False),
                    },
                    'access_token': access_token,
                    'refresh_token': refresh_token,
                }), 200

    except Exception as e:
        logger.error(f"Apple login error: {e}", exc_info=True)
        return jsonify({'error': 'Authentication failed'}), 500
