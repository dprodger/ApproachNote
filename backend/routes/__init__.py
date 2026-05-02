# routes/__init__.py
"""
Blueprint registration helper
"""

def register_blueprints(app):
    """Register all application blueprints"""
    from routes.health import health_bp
    from routes.research import research_bp
    from routes.songs import songs_bp
    from routes.recordings import recordings_bp
    from routes.performers import performers_bp
    from routes.images import images_bp
    from routes.repertoires import repertoires_bp
    from routes.transcriptions import transcriptions_bp
    from routes.reports import reports_bp
    from routes.authority import authorities_bp
    from routes.admin import admin_bp
    from routes.admin_research import admin_research_bp
    # Registers /admin/login and /admin/logout onto admin_bp. Import for
    # side effects — the module has no public symbols we need here.
    import routes.admin_auth  # noqa: F401
    from routes.videos import videos_bp
    from routes.favorites import favorites_bp
    from routes.contributions import contributions_bp
    from routes.musicbrainz import musicbrainz_bp

    # Unversioned: load-balancer probe + admin web UI.
    app.register_blueprint(health_bp)
    app.register_blueprint(admin_bp)
    app.register_blueprint(admin_research_bp)

    # Public client API — every client-facing route lives under /v1/.
    # See docs/api-versioning.md for policy. The url_prefix passed here
    # overrides any url_prefix set on the Blueprint() itself.
    app.register_blueprint(research_bp, url_prefix='/v1')
    app.register_blueprint(songs_bp, url_prefix='/v1')
    app.register_blueprint(recordings_bp, url_prefix='/v1')
    app.register_blueprint(performers_bp, url_prefix='/v1')
    app.register_blueprint(images_bp, url_prefix='/v1')
    app.register_blueprint(repertoires_bp, url_prefix='/v1/repertoires')
    app.register_blueprint(transcriptions_bp, url_prefix='/v1')
    app.register_blueprint(reports_bp, url_prefix='/v1')
    app.register_blueprint(authorities_bp, url_prefix='/v1')
    app.register_blueprint(videos_bp, url_prefix='/v1')
    app.register_blueprint(favorites_bp, url_prefix='/v1')
    app.register_blueprint(contributions_bp, url_prefix='/v1')
    app.register_blueprint(musicbrainz_bp, url_prefix='/v1')