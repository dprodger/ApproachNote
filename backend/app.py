"""
Jazz Reference API Backend - Improved Version
A Flask API with robust database connection handling
"""

from flask import Flask, render_template, request, jsonify, send_from_directory
from werkzeug.middleware.proxy_fix import ProxyFix
import logging
import os

from dotenv import load_dotenv  # ADD THIS LINE

# Load environment variables from .env file
load_dotenv()  # ADD THIS LINE

# Configuration
from config import configure_logging, init_app_config

# Set pooling mode BEFORE importing db_utils
os.environ['DB_USE_POOLING'] = 'true'

# Import database tools
import db_utils as db_tools
from core import research_queue
from core import song_research
logger = configure_logging()

# Create Flask app
app = Flask(__name__)
init_app_config(app)

# Install ProxyFix so `request.remote_addr` and `request.url` reflect the
# real client's IP and scheme rather than Render's reverse proxy. Rate
# limiting (below) depends on this: without ProxyFix, Flask-Limiter would
# see every request as coming from Render's outbound IP and lump all
# users together. `x_for=1` tells Werkzeug to trust exactly one hop of
# X-Forwarded-For (Render). If another proxy is ever added in front
# (e.g. Cloudflare), bump this to 2.
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)

# Map `admin.approachnote.com/<path>` onto the internal `/admin/<path>` route
# surface, install the matching `Location:` header rewriter, and expose
# `admin_url()` to Jinja. Must come AFTER ProxyFix so X-Forwarded-Host has
# already been folded into HTTP_HOST.
from middleware.admin_subdomain import install as install_admin_subdomain
install_admin_subdomain(app)

# Initialize the rate limiter against the Flask app. MUST come after
# ProxyFix is installed, so the key function sees the real client IP
# from the very first request.
from rate_limit import init_rate_limiter
init_rate_limiter(app)

logger.info(f"Spotify credentials present: {bool(os.environ.get('SPOTIFY_CLIENT_ID'))}")
logger.info(f"Flask app initialized in PID {os.getpid()}")

# Import authentication blueprints
from routes.auth import auth_bp
from routes.password import password_bp

# Register authentication blueprints under /v1/auth/. Both blueprints set
# url_prefix='/auth' on their Blueprint() definition; passing url_prefix
# here overrides that, yielding final paths like /v1/auth/login.
app.register_blueprint(auth_bp, url_prefix='/v1/auth')
app.register_blueprint(password_bp, url_prefix='/v1/auth')

# Register all route blueprints
from routes import register_blueprints
register_blueprints(app)


# ============================================================================
# LANDING PAGE
# ============================================================================

# PostHog analytics config for the public marketing pages. The public project
# key is read from the environment (not hardcoded) and is intentionally exposed
# in client JS; the block in templates/_analytics.html no-ops when it's unset,
# so local dev stays clean. US Cloud host by default; override via POSTHOG_HOST.
@app.context_processor
def inject_analytics():
    return {
        'posthog_api_key': os.environ.get('POSTHOG_API_KEY', ''),
        'posthog_host': os.environ.get('POSTHOG_HOST', 'https://us.i.posthog.com'),
    }


@app.route('/')
def landing_page():
    """Serve the main landing page"""
    return render_template('index.html')

# Root-level icon probes. Browsers and iOS Safari request these fixed paths
# automatically (regardless of the <link rel="icon"> tags in index.html), so
# without explicit routes they fall through to the host-routing 404. Served
# from any host; see ROOT_ICON_PATHS in enforce_host_routing.
@app.route('/favicon.ico')
def favicon():
    return send_from_directory(app.static_folder, 'images/favicon.ico',
                               mimetype='image/x-icon')

@app.route('/apple-touch-icon.png')
@app.route('/apple-touch-icon-precomposed.png')
def apple_touch_icon():
    return send_from_directory(app.static_folder, 'images/apple-touch-icon.png',
                               mimetype='image/png')

@app.route('/terms')
def terms_page():
    return render_template('terms.html')

@app.route('/privacy')
def privacy_page():
    return render_template('privacy.html')

# Account self-service pages. These must be plain https URLs (not the app's
# custom URL scheme) so the links in auth emails are clickable in every mail
# client and work on desktop. They POST to the existing /auth/* JSON API and
# are served from any host (see ACCOUNT_PATHS in the host-routing hook).
@app.route('/reset-password')
def reset_password_page():
    return render_template('reset_password.html')

@app.route('/forgot-password')
def forgot_password_page():
    return render_template('forgot_password.html')

# ============================================================================
# HOST-BASED ROUTING
# ============================================================================

# Define which hosts serve which content
API_HOSTS = ['api.approachnote.com', 'localhost:5001', '127.0.0.1:5001']
ADMIN_HOSTS = ['admin.approachnote.com']
WEB_HOSTS = ['approachnote.com', 'www.approachnote.com']

# Routes that should only be served from the website (not API subdomain)
WEB_ONLY_PATHS = ['/', '/terms', '/privacy']

# Account self-service pages served from ANY host (API, web, or localhost).
# Auth emails link to these on the API host (same origin as the /auth/* API,
# so no CORS), but they should also work if reached via the web host.
ACCOUNT_PATHS = ['/reset-password', '/forgot-password']

# Fixed root-level icon paths browsers/iOS request automatically. Served from
# any host (web, API, or localhost) so the probes never 404.
ROOT_ICON_PATHS = ['/favicon.ico', '/apple-touch-icon.png',
                   '/apple-touch-icon-precomposed.png']

# Routes that should only be served from the API subdomain
# (everything except web-only paths and static files)

@app.before_request
def enforce_host_routing():
    """
    Enforce that API, admin, and website routes are only served from their
    respective hosts.

    Admin is served from admin.approachnote.com (and localhost for dev).
    Browser-facing URLs on the admin host don't carry a `/admin` prefix —
    `middleware.admin_subdomain.AdminSubdomainMiddleware` rewrites incoming
    PATH_INFO to `/admin/...` before Flask routes the request, so by the
    time we run here the path always looks like `/admin/...` for admin
    traffic. Hitting `api.approachnote.com/admin/...` is an explicit 404:
    the only sanctioned admin entry point is the dedicated subdomain.
    """
    # Check X-Forwarded-Host first (set by reverse proxies like Render)
    # Fall back to request.host
    host = request.headers.get('X-Forwarded-Host', request.host)
    path = request.path

    # Normalize host (lowercase, strip port if present)
    host_normalized = host.lower().split(':')[0] if host else ''

    # Log for debugging (remove after confirming it works)
    logger.debug(f"Host routing: host_normalized={host_normalized}, path={path}, raw_host={host}, request.host={request.host}")

    # Allow static files from any host
    if path.startswith('/static/'):
        return None

    # Allow account self-service pages (password reset/forgot) from any host
    if path in ACCOUNT_PATHS:
        return None

    # Allow root-level icon probes (favicon, apple-touch-icon) from any host
    if path in ROOT_ICON_PATHS:
        return None

    is_admin_host = host_normalized in ['admin.approachnote.com']
    is_localhost = host_normalized in ['localhost', '127.0.0.1']

    # Admin routes are only served from the dedicated admin subdomain or
    # localhost (dev). Any /admin/* hit on the API or web host is a hard 404.
    if path.startswith('/admin'):
        if is_admin_host or is_localhost:
            return None
        return jsonify({'error': 'Not found'}), 404

    # On the admin host, only /admin/* (already rewritten by the WSGI
    # middleware) and /static/* are valid. Anything else has slipped past the
    # rewrite — bail out.
    if is_admin_host:
        return jsonify({'error': 'Not found'}), 404

    # Allow all routes on localhost (development)
    if is_localhost:
        return None

    # Check if this is an API host (check if host contains 'api.')
    is_api_host = 'api.' in host_normalized

    # Check if this is a web host (www or root domain, but not api/admin)
    is_web_host = (
        'approachnote.com' in host_normalized
        and 'api.' not in host_normalized
        and not is_admin_host
    )

    logger.debug(f"Host check: is_api_host={is_api_host}, is_web_host={is_web_host}")

    # On API host: block web-only paths
    if is_api_host and path in WEB_ONLY_PATHS:
        return jsonify({'error': 'Not found', 'message': 'Use approachnote.com for website'}), 404

    # On web host: only allow web-only paths and static files
    if is_web_host and path not in WEB_ONLY_PATHS:
        return jsonify({'error': 'Not found', 'message': 'Use api.approachnote.com for API'}), 404

    return None

# Admin-auth gate. Runs for every URL under /admin/ regardless of which
# blueprint registered the route (notably routes/research.py also registers
# an /admin/* path). The hook is a no-op for /admin/login and /admin/logout
# so the login page stays reachable.
from middleware.admin_middleware import check_admin_or_respond  # noqa: E402

@app.before_request
def gate_admin_paths():
    # Match only real admin paths. /adminfoo or /admin= must fall through to
    # Flask's routing (which will 404), otherwise they'd loop through login.
    path = request.path
    if path == '/admin' or path.startswith('/admin/'):
        return check_admin_or_respond()
    return None


# Request/response logging
@app.before_request
def log_request():
    """Log incoming requests"""
    logger.info(f"{request.method} {request.path} (Host: {request.host})")

@app.after_request
def log_response(response):
    """Log response status"""
    logger.info(f"{request.method} {request.path} - {response.status_code}")
    return response



if __name__ == '__main__':
    # Running directly with 'python app.py' (not gunicorn)
    logger.info("Starting Flask application directly (not gunicorn)...")
    logger.info("Database connection pool will initialize on first request")
    
    # Start keepalive thread
    db_tools.start_keepalive_thread()
    
    # Start research worker thread (only when running directly)
    if not research_queue._worker_running:
        research_queue.start_worker(song_research.research_song)
        logger.info("Research worker thread initialized")
        
    try:
        app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 5001)))
    finally:
        # Cleanup
        logger.info("Shutting down...")
        research_queue.stop_worker()
        db_tools.stop_keepalive_thread()
        db_tools.close_connection_pool()
        logger.info("Shutdown complete")
        
import atexit

def cleanup_connections():
    """Close the connection pool on shutdown"""
    logger.info("Shutting down connection pool...")
    db_tools.close_connection_pool()
    logger.info("Connection pool closed")

atexit.register(cleanup_connections)