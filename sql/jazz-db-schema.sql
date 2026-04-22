-- Approach Note - PostgreSQL Database Schema
-- Generated from live Supabase database on 2026-03-19

-- Enable UUID extension for generating unique IDs
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Enable unaccent extension for accent-insensitive text matching
-- (e.g., "Mel Torme" matches "Mel Torme")
CREATE EXTENSION IF NOT EXISTS unaccent;

-- ============================================================================
-- CUSTOM TYPES
-- ============================================================================

CREATE TYPE imagery_source AS ENUM ('MusicBrainz', 'Spotify', 'Wikipedia', 'Apple', 'Amazon');
CREATE TYPE imagery_type AS ENUM ('Front', 'Back');

-- ============================================================================
-- CORE TABLES
-- ============================================================================

-- Songs table
CREATE TABLE songs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    title VARCHAR(255) NOT NULL,
    composer VARCHAR(500),
    composed_year INTEGER,
    composed_key VARCHAR(10),
    song_reference TEXT,
    structure TEXT,
    external_references JSONB,
    musicbrainz_id VARCHAR(36) UNIQUE,
    second_mb_id VARCHAR(36),
    wikipedia_url VARCHAR(500),
    alt_titles TEXT[],
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    updated_by VARCHAR(100)
);

-- Performers table
CREATE TABLE performers (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL,
    sort_name VARCHAR(255),
    artist_type VARCHAR(50),
    disambiguation VARCHAR(500),
    biography TEXT,
    birth_date DATE,
    death_date DATE,
    external_links JSONB,
    wikipedia_url VARCHAR(500),
    musicbrainz_id VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    updated_by VARCHAR(100)
);

-- Instruments table
CREATE TABLE instruments (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(100) NOT NULL UNIQUE,
    category VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- RELEASE REFERENCE TABLES
-- ============================================================================

CREATE TABLE release_formats (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    category VARCHAR(50),
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE release_packaging (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE release_statuses (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- RELEASES
-- ============================================================================

CREATE TABLE releases (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    musicbrainz_release_id VARCHAR(36) UNIQUE,
    musicbrainz_release_group_id VARCHAR(36),
    title VARCHAR(500) NOT NULL,
    artist_credit VARCHAR(500),
    disambiguation VARCHAR(500),
    release_date DATE,
    release_year INTEGER,
    country VARCHAR(2),
    label VARCHAR(255),
    catalog_number VARCHAR(100),
    barcode VARCHAR(50),
    format_id INTEGER REFERENCES release_formats(id),
    packaging_id INTEGER REFERENCES release_packaging(id),
    status_id INTEGER REFERENCES release_statuses(id),
    language VARCHAR(10),
    script VARCHAR(10),
    total_tracks INTEGER,
    total_discs INTEGER DEFAULT 1,
    cover_art_url VARCHAR(500),
    cover_art_checked_at TIMESTAMP WITH TIME ZONE,
    spotify_album_id VARCHAR(50),
    apple_music_searched_at TIMESTAMP WITH TIME ZONE,
    amazon_url VARCHAR(500),
    discogs_url VARCHAR(500),
    data_quality VARCHAR(50),
    annotation TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    updated_by VARCHAR(100)
);

-- ============================================================================
-- RECORDINGS
-- ============================================================================

CREATE TABLE recordings (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    song_id UUID NOT NULL REFERENCES songs(id) ON DELETE CASCADE,
    title VARCHAR(500),                         -- MusicBrainz recording title (may differ from song title)
    recording_date DATE,
    recording_year INTEGER,
    recording_date_source VARCHAR(50),          -- 'mb_performer_relation', 'mb_first_release', 'earliest_release', 'manual'
    recording_date_precision VARCHAR(10),       -- 'day', 'month', 'year'
    mb_first_release_date VARCHAR(10),          -- MusicBrainz first-release-date (YYYY, YYYY-MM, or YYYY-MM-DD)
    label VARCHAR(255),
    is_canonical BOOLEAN DEFAULT false,
    notes TEXT,
    default_release_id UUID REFERENCES releases(id) ON DELETE SET NULL,
    musicbrainz_id VARCHAR(255),
    source_mb_work_id VARCHAR(36),              -- MusicBrainz work ID this recording was imported from
    duration_ms INTEGER,                        -- Recording duration from MusicBrainz
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    updated_by VARCHAR(100)
);

-- ============================================================================
-- JUNCTION TABLES
-- ============================================================================

-- Recording <-> Release junction
CREATE TABLE recording_releases (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    recording_id UUID NOT NULL REFERENCES recordings(id) ON DELETE CASCADE,
    release_id UUID NOT NULL REFERENCES releases(id) ON DELETE CASCADE,
    disc_number INTEGER DEFAULT 1,
    track_number INTEGER,
    track_position VARCHAR(20),                 -- e.g., A1, B2
    track_title VARCHAR(500),                   -- May differ from recording title
    track_artist_credit VARCHAR(500),
    track_length_ms INTEGER,                    -- Duration on this specific release
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (recording_id, release_id)
    -- Track/disc lookup is served by idx_recording_releases_disc_track below;
    -- a separate UNIQUE on (recording_id, release_id, disc_number, track_number)
    -- would be redundant with the (recording_id, release_id) constraint above.
);

-- Recording <-> Performer junction
CREATE TABLE recording_performers (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    recording_id UUID NOT NULL REFERENCES recordings(id) ON DELETE CASCADE,
    performer_id UUID NOT NULL REFERENCES performers(id) ON DELETE CASCADE,
    instrument_id UUID REFERENCES instruments(id) ON DELETE SET NULL,
    role VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (recording_id, performer_id, instrument_id)
);

-- Performer <-> Instrument junction
CREATE TABLE performer_instruments (
    performer_id UUID NOT NULL REFERENCES performers(id) ON DELETE CASCADE,
    instrument_id UUID NOT NULL REFERENCES instruments(id) ON DELETE CASCADE,
    is_primary BOOLEAN DEFAULT false,
    proficiency_level VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (performer_id, instrument_id)
);

-- Release <-> Performer junction
CREATE TABLE release_performers (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    release_id UUID NOT NULL REFERENCES releases(id) ON DELETE CASCADE,
    performer_id UUID NOT NULL REFERENCES performers(id) ON DELETE CASCADE,
    instrument_id UUID REFERENCES instruments(id) ON DELETE SET NULL,
    role VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (release_id, performer_id, instrument_id)
);

-- ============================================================================
-- CORE USER TABLES
-- (defined here because streaming-links tables below reference users.id;
-- refresh / password-reset / verification tables follow further down.)
-- ============================================================================

CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email VARCHAR(255) NOT NULL UNIQUE,
    email_verified BOOLEAN DEFAULT false,
    password_hash VARCHAR(255),
    display_name VARCHAR(255),
    profile_image_url VARCHAR(500),
    google_id VARCHAR(255) UNIQUE,
    apple_id VARCHAR(255) UNIQUE,
    is_active BOOLEAN DEFAULT true,
    account_locked BOOLEAN DEFAULT false,
    failed_login_attempts INTEGER DEFAULT 0,
    last_failed_login_at TIMESTAMP WITH TIME ZONE,
    last_login_at TIMESTAMP WITH TIME ZONE,
    is_admin BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON COLUMN users.is_admin IS
    'True if user may access /admin web pages. Granted via backend/scripts/grant_admin.py.';

CREATE INDEX IF NOT EXISTS idx_users_is_admin
    ON users(is_admin)
    WHERE is_admin = true;

CREATE TABLE admin_users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email VARCHAR(255) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    full_name VARCHAR(255),
    role VARCHAR(50) DEFAULT 'editor',
    is_active BOOLEAN DEFAULT true,
    last_login_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- STREAMING LINKS
-- ============================================================================

-- Track-level streaming links (per recording-release pair)
CREATE TABLE recording_release_streaming_links (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    recording_release_id UUID NOT NULL REFERENCES recording_releases(id) ON DELETE CASCADE,
    service VARCHAR(50) NOT NULL,               -- 'spotify', 'apple_music', 'youtube', 'youtube_music', 'tidal', etc.
    service_id VARCHAR(100),                    -- Track ID on this service
    service_title VARCHAR(500),                 -- Track title as it appears on this service
    service_url VARCHAR(500),
    duration_ms INTEGER,
    popularity INTEGER,                         -- Spotify popularity score
    preview_url VARCHAR(500),
    isrc VARCHAR(20),                           -- International Standard Recording Code
    match_confidence NUMERIC,                   -- 0.00-1.00
    match_method VARCHAR(100),                  -- 'fuzzy_search', 'isrc_lookup', 'upc_lookup', 'manual', etc.
    matched_at TIMESTAMP WITH TIME ZONE,
    last_verified_at TIMESTAMP WITH TIME ZONE,
    added_by_user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (recording_release_id, service)
);

-- Album-level streaming links (per release)
CREATE TABLE release_streaming_links (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    release_id UUID NOT NULL REFERENCES releases(id) ON DELETE CASCADE,
    service VARCHAR(50) NOT NULL,
    service_id VARCHAR(100),
    service_url VARCHAR(500),
    match_confidence NUMERIC,
    match_method VARCHAR(100),
    matched_at TIMESTAMP WITH TIME ZONE,
    last_verified_at TIMESTAMP WITH TIME ZONE,
    added_by_user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (release_id, service)
);

-- ============================================================================
-- RELEASE METADATA
-- ============================================================================

CREATE TABLE release_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    release_id UUID NOT NULL REFERENCES releases(id) ON DELETE CASCADE,
    country VARCHAR(2),
    release_date DATE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (release_id, country, release_date)
);

CREATE TABLE release_labels (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    release_id UUID NOT NULL REFERENCES releases(id) ON DELETE CASCADE,
    label_name VARCHAR(255) NOT NULL,
    catalog_number VARCHAR(100),
    musicbrainz_label_id VARCHAR(36),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (release_id, label_name, catalog_number)
);

CREATE TABLE release_imagery (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    release_id UUID NOT NULL REFERENCES releases(id) ON DELETE CASCADE,
    source imagery_source NOT NULL,
    source_id VARCHAR(255),
    source_url VARCHAR(1000),
    type imagery_type NOT NULL,
    image_url_small VARCHAR(1000),
    image_url_medium VARCHAR(1000),
    image_url_large VARCHAR(1000),
    checksum VARCHAR(64),
    comment TEXT,
    approved BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (release_id, source, type)
);

-- ============================================================================
-- IMAGES
-- ============================================================================

CREATE TABLE images (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    url VARCHAR(1000) NOT NULL UNIQUE,
    source VARCHAR(100) NOT NULL,
    source_identifier VARCHAR(255),
    license_type VARCHAR(100),
    license_url VARCHAR(500),
    attribution TEXT,
    width INTEGER,
    height INTEGER,
    thumbnail_url VARCHAR(1000),
    source_page_url VARCHAR(1000),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (source, source_identifier, url)
);

CREATE TABLE artist_images (
    performer_id UUID NOT NULL REFERENCES performers(id) ON DELETE CASCADE,
    image_id UUID NOT NULL REFERENCES images(id) ON DELETE CASCADE,
    is_primary BOOLEAN DEFAULT false,
    display_order INTEGER DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (performer_id, image_id)
);

-- ============================================================================
-- AUTH SESSION & TOKEN TABLES
-- (users and admin_users are defined earlier, before STREAMING LINKS)
-- ============================================================================

CREATE TABLE refresh_tokens (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token VARCHAR(500) NOT NULL UNIQUE,
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    revoked_at TIMESTAMP WITH TIME ZONE,
    device_info JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE password_reset_tokens (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token VARCHAR(255) NOT NULL UNIQUE,
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    used_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE email_verification_tokens (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token VARCHAR(255) NOT NULL UNIQUE,
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    verified_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- USER FEATURES
-- ============================================================================

CREATE TABLE repertoires (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID REFERENCES users(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE repertoire_songs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    repertoire_id UUID NOT NULL REFERENCES repertoires(id) ON DELETE CASCADE,
    song_id UUID NOT NULL REFERENCES songs(id) ON DELETE CASCADE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (repertoire_id, song_id)
);

CREATE TABLE recording_favorites (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    recording_id UUID NOT NULL REFERENCES recordings(id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (recording_id, user_id)
);

CREATE TABLE recording_contributions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    recording_id UUID NOT NULL REFERENCES recordings(id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    performance_key VARCHAR(3),
    is_instrumental BOOLEAN,
    tempo_marking VARCHAR(20),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (user_id, recording_id)
);

-- ============================================================================
-- CONTENT TABLES
-- ============================================================================

CREATE TABLE videos (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    song_id UUID REFERENCES songs(id) ON DELETE SET NULL,
    recording_id UUID REFERENCES recordings(id) ON DELETE SET NULL,
    youtube_url VARCHAR(500) NOT NULL,
    title VARCHAR(255),
    description TEXT,
    video_type VARCHAR(50) NOT NULL,
    duration_seconds INTEGER,
    tempo INTEGER,
    key_signature VARCHAR(20),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100) NOT NULL
);

CREATE TABLE video_performers (
    video_id UUID NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
    performer_id UUID NOT NULL REFERENCES performers(id) ON DELETE CASCADE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (video_id, performer_id)
);

CREATE TABLE solo_transcriptions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    song_id UUID NOT NULL REFERENCES songs(id) ON DELETE CASCADE,
    recording_id UUID REFERENCES recordings(id) ON DELETE CASCADE,
    youtube_url VARCHAR(500),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    updated_by VARCHAR(100)
);

-- ============================================================================
-- DATA QUALITY & ADMIN
-- ============================================================================

CREATE TABLE orphan_recordings (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    song_id UUID NOT NULL REFERENCES songs(id) ON DELETE CASCADE,
    mb_recording_id VARCHAR(36) NOT NULL,
    mb_recording_title VARCHAR(500),
    mb_artist_credit VARCHAR(500),
    mb_artist_ids TEXT[],
    mb_first_release_date VARCHAR(20),
    mb_release_count INTEGER,
    mb_length_ms INTEGER,
    mb_disambiguation VARCHAR(500),
    mb_releases JSONB,
    spotify_track_id VARCHAR(100),
    spotify_track_name VARCHAR(500),
    spotify_artist_name VARCHAR(500),
    spotify_album_name VARCHAR(500),
    spotify_album_id VARCHAR(100),
    spotify_preview_url VARCHAR(500),
    spotify_external_url VARCHAR(500),
    spotify_album_art_url VARCHAR(500),
    spotify_match_confidence VARCHAR(20),
    spotify_match_score DOUBLE PRECISION,
    spotify_matched_at TIMESTAMP WITH TIME ZONE,
    spotify_matched_mb_release_id VARCHAR(36),
    issue_type VARCHAR(50) NOT NULL,
    linked_work_ids TEXT[],
    status VARCHAR(20) DEFAULT 'pending',
    reviewed_at TIMESTAMP WITH TIME ZONE,
    reviewed_by VARCHAR(100),
    review_notes TEXT,
    imported_recording_id UUID REFERENCES recordings(id) ON DELETE SET NULL,
    imported_at TIMESTAMP WITH TIME ZONE,
    discovered_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (song_id, mb_recording_id)
);

CREATE TABLE song_authority_recommendations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    song_id UUID NOT NULL REFERENCES songs(id) ON DELETE CASCADE,
    recording_id UUID REFERENCES recordings(id) ON DELETE SET NULL,
    source VARCHAR(100) NOT NULL,
    recommendation_text TEXT,
    source_url TEXT NOT NULL,
    artist_name VARCHAR(255),
    album_title VARCHAR(255),
    recording_year INTEGER,
    itunes_album_id BIGINT,
    itunes_track_id BIGINT,
    captured_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE bad_streaming_matches (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    service VARCHAR(50) NOT NULL,
    block_level VARCHAR(20) NOT NULL,
    service_id VARCHAR(100) NOT NULL,
    song_id UUID NOT NULL REFERENCES songs(id) ON DELETE CASCADE,
    reason TEXT,
    reported_by UUID REFERENCES users(id) ON DELETE SET NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (service, block_level, service_id, song_id)
);

CREATE TABLE content_reports (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_type VARCHAR(50) NOT NULL,
    entity_id UUID NOT NULL,
    entity_name VARCHAR(255) NOT NULL,
    report_category VARCHAR(50) NOT NULL DEFAULT 'link_issue',
    external_source VARCHAR(100) NOT NULL,
    external_url VARCHAR(1000) NOT NULL,
    explanation TEXT NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    priority VARCHAR(20) DEFAULT 'normal',
    resolution_notes TEXT,
    resolution_action VARCHAR(100),
    reporter_ip VARCHAR(45),
    reporter_user_agent TEXT,
    reporter_platform VARCHAR(50),
    reporter_app_version VARCHAR(50),
    reviewed_by UUID REFERENCES admin_users(id) ON DELETE SET NULL,
    reviewed_at TIMESTAMP WITH TIME ZONE,
    resolved_by UUID REFERENCES admin_users(id) ON DELETE SET NULL,
    resolved_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE audit_log (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    table_name VARCHAR(100) NOT NULL,
    record_id UUID NOT NULL,
    action VARCHAR(20) NOT NULL,
    old_values JSONB,
    new_values JSONB,
    changed_by VARCHAR(100),
    changed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Quick-lookup table for pending orphan count (maintained by application)
CREATE TABLE orphan_count (
    count BIGINT
);

-- ============================================================================
-- VIEWS
-- ============================================================================

CREATE VIEW songs_with_authority_recs AS
    SELECT s.id AS song_id,
        s.title,
        count(sar.id) AS recommendation_count,
        count(DISTINCT sar.source) AS source_count,
        array_agg(DISTINCT sar.source) FILTER (WHERE sar.source IS NOT NULL) AS sources
    FROM songs s
        LEFT JOIN song_authority_recommendations sar ON s.id = sar.song_id
    GROUP BY s.id, s.title;

-- ============================================================================
-- INDEXES
-- ============================================================================

-- Songs
CREATE INDEX idx_songs_title ON songs (title);
CREATE INDEX idx_songs_composer ON songs (composer);
CREATE INDEX idx_songs_musicbrainz_id ON songs (musicbrainz_id);
CREATE INDEX idx_songs_second_mb_id ON songs (second_mb_id) WHERE second_mb_id IS NOT NULL;
CREATE INDEX idx_songs_alt_titles ON songs USING gin (alt_titles);

-- Performers
CREATE INDEX idx_performers_name ON performers (name);
CREATE INDEX idx_performers_sort_name ON performers (sort_name) WHERE sort_name IS NOT NULL;
CREATE INDEX idx_performers_sort_order ON performers (COALESCE(sort_name, name));
CREATE INDEX idx_performers_musicbrainz_id ON performers (musicbrainz_id) WHERE musicbrainz_id IS NOT NULL;
CREATE INDEX idx_performers_wikipedia_url ON performers (wikipedia_url) WHERE wikipedia_url IS NOT NULL;
CREATE INDEX idx_performers_external_links ON performers USING gin (external_links);

-- Instruments
CREATE INDEX idx_instruments_name ON instruments (name);
CREATE INDEX idx_instruments_category ON instruments (category);

-- Releases
CREATE INDEX idx_releases_musicbrainz_release_id ON releases (musicbrainz_release_id);
CREATE INDEX idx_releases_musicbrainz_release_group_id ON releases (musicbrainz_release_group_id);
CREATE INDEX idx_releases_title ON releases (title);
CREATE INDEX idx_releases_artist_credit ON releases (artist_credit);
CREATE INDEX idx_releases_release_date ON releases (release_date);
CREATE INDEX idx_releases_release_year ON releases (release_year);
CREATE INDEX idx_releases_label ON releases (label);
CREATE INDEX idx_releases_barcode ON releases (barcode);
CREATE INDEX idx_releases_spotify_album_id ON releases (spotify_album_id);
CREATE INDEX idx_releases_cover_art_checked_at ON releases (cover_art_checked_at) WHERE cover_art_checked_at IS NULL;

-- Recordings
CREATE INDEX idx_recordings_song_id ON recordings (song_id);
CREATE INDEX idx_recordings_year ON recordings (recording_year);
CREATE INDEX idx_recordings_is_canonical ON recordings (is_canonical);
CREATE INDEX idx_recordings_default_release_id ON recordings (default_release_id) WHERE default_release_id IS NOT NULL;
CREATE INDEX idx_recordings_source_mb_work_id ON recordings (source_mb_work_id) WHERE source_mb_work_id IS NOT NULL;
CREATE INDEX idx_recordings_recording_date_source ON recordings (recording_date_source) WHERE recording_date_source IS NOT NULL;
CREATE INDEX idx_recordings_mb_first_release_date ON recordings (mb_first_release_date) WHERE mb_first_release_date IS NOT NULL;
CREATE INDEX idx_recordings_title ON recordings (title);
CREATE UNIQUE INDEX idx_recordings_mb_id_song_id ON recordings (musicbrainz_id, song_id) WHERE musicbrainz_id IS NOT NULL;

-- Recording Releases
CREATE INDEX idx_recording_releases_recording_id ON recording_releases (recording_id);
CREATE INDEX idx_recording_releases_release_id ON recording_releases (release_id);
CREATE INDEX idx_recording_releases_disc_track ON recording_releases (release_id, disc_number, track_number);

-- Recording Performers
CREATE INDEX idx_recording_performers_recording_id ON recording_performers (recording_id);
CREATE INDEX idx_recording_performers_performer_id ON recording_performers (performer_id);
CREATE INDEX idx_recording_performers_instrument_id ON recording_performers (instrument_id);

-- Recording Release Streaming Links
CREATE INDEX idx_rr_streaming_links_recording_release_id ON recording_release_streaming_links (recording_release_id);
CREATE INDEX idx_rr_streaming_links_service ON recording_release_streaming_links (service);
CREATE INDEX idx_rr_streaming_links_rr_service ON recording_release_streaming_links (recording_release_id, service);
CREATE INDEX idx_rr_streaming_links_service_id ON recording_release_streaming_links (service_id) WHERE service_id IS NOT NULL;
CREATE INDEX idx_rr_streaming_links_isrc ON recording_release_streaming_links (isrc) WHERE isrc IS NOT NULL;
CREATE INDEX idx_rr_streaming_links_match_method ON recording_release_streaming_links (match_method) WHERE match_method IS NOT NULL;
CREATE INDEX idx_rr_streaming_links_added_by_user ON recording_release_streaming_links (added_by_user_id) WHERE added_by_user_id IS NOT NULL;

-- Release Streaming Links
CREATE INDEX idx_release_streaming_links_release_id ON release_streaming_links (release_id);
CREATE INDEX idx_release_streaming_links_service ON release_streaming_links (service);
CREATE INDEX idx_release_streaming_links_service_id ON release_streaming_links (service_id) WHERE service_id IS NOT NULL;
CREATE INDEX idx_release_streaming_links_match_method ON release_streaming_links (match_method) WHERE match_method IS NOT NULL;
CREATE INDEX idx_release_streaming_links_added_by_user ON release_streaming_links (added_by_user_id) WHERE added_by_user_id IS NOT NULL;

-- Release Events
CREATE INDEX idx_release_events_release_id ON release_events (release_id);
CREATE INDEX idx_release_events_country ON release_events (country);

-- Release Labels
CREATE INDEX idx_release_labels_release_id ON release_labels (release_id);
CREATE INDEX idx_release_labels_label_name ON release_labels (label_name);

-- Release Imagery
CREATE INDEX idx_release_imagery_release_id ON release_imagery (release_id);
CREATE INDEX idx_release_imagery_source ON release_imagery (source);
CREATE INDEX idx_release_imagery_type ON release_imagery (type);
CREATE INDEX idx_release_imagery_release_source ON release_imagery (release_id, source);
CREATE INDEX idx_release_imagery_release_type ON release_imagery (release_id, type);

-- Release Performers
CREATE INDEX idx_release_performers_release_id ON release_performers (release_id);
CREATE INDEX idx_release_performers_performer_id ON release_performers (performer_id);
CREATE INDEX idx_release_performers_role ON release_performers (role);

-- Images
CREATE INDEX idx_images_source ON images (source);
CREATE INDEX idx_images_source_identifier ON images (source_identifier);

-- Artist Images
CREATE INDEX idx_artist_images_performer ON artist_images (performer_id);
CREATE INDEX idx_artist_images_image ON artist_images (image_id);
CREATE INDEX idx_artist_images_primary ON artist_images (is_primary) WHERE is_primary = true;

-- Users
CREATE INDEX idx_users_email ON users (email);
CREATE INDEX idx_users_google_id ON users (google_id);
CREATE INDEX idx_users_apple_id ON users (apple_id);

-- Auth Tokens
CREATE INDEX idx_refresh_tokens_user_id ON refresh_tokens (user_id);
CREATE INDEX idx_refresh_tokens_token ON refresh_tokens (token);
CREATE INDEX idx_password_reset_tokens_user_id ON password_reset_tokens (user_id);
CREATE INDEX idx_password_reset_tokens_token ON password_reset_tokens (token);
CREATE INDEX idx_email_verification_tokens_user_id ON email_verification_tokens (user_id);
CREATE INDEX idx_email_verification_tokens_token ON email_verification_tokens (token);

-- Repertoires
CREATE INDEX idx_repertoires_user_id ON repertoires (user_id);
CREATE INDEX idx_repertoires_name ON repertoires (name);
CREATE INDEX idx_repertoire_songs_repertoire_id ON repertoire_songs (repertoire_id);
CREATE INDEX idx_repertoire_songs_song_id ON repertoire_songs (song_id);

-- Recording Favorites
CREATE INDEX idx_recording_favorites_recording_id ON recording_favorites (recording_id);
CREATE INDEX idx_recording_favorites_user_id ON recording_favorites (user_id);

-- Recording Contributions
CREATE INDEX idx_recording_contributions_recording_id ON recording_contributions (recording_id);
CREATE INDEX idx_recording_contributions_user_id ON recording_contributions (user_id);
CREATE INDEX idx_recording_contributions_key ON recording_contributions (recording_id, performance_key) WHERE performance_key IS NOT NULL;
CREATE INDEX idx_recording_contributions_instrumental ON recording_contributions (recording_id, is_instrumental) WHERE is_instrumental IS NOT NULL;

-- Videos
CREATE INDEX idx_videos_song_id ON videos (song_id);
CREATE INDEX idx_videos_recording_id ON videos (recording_id);
CREATE INDEX idx_videos_type ON videos (video_type);

-- Solo Transcriptions
CREATE INDEX idx_solo_transcriptions_song_id ON solo_transcriptions (song_id);
CREATE INDEX idx_solo_transcriptions_recording_id ON solo_transcriptions (recording_id);

-- Orphan Recordings
CREATE INDEX idx_orphan_recordings_song_id ON orphan_recordings (song_id);
CREATE INDEX idx_orphan_recordings_mb_recording_id ON orphan_recordings (mb_recording_id);
CREATE INDEX idx_orphan_recordings_status ON orphan_recordings (status);
CREATE INDEX idx_orphan_recordings_spotify_match ON orphan_recordings (spotify_match_confidence);

-- Song Authority Recommendations
CREATE INDEX idx_authority_recs_song_id ON song_authority_recommendations (song_id);
CREATE INDEX idx_authority_recs_recording_id ON song_authority_recommendations (recording_id);
CREATE INDEX idx_authority_recs_source ON song_authority_recommendations (source);
CREATE INDEX idx_authority_recs_captured_at ON song_authority_recommendations (captured_at);
CREATE INDEX idx_song_authority_recs_itunes_album ON song_authority_recommendations (itunes_album_id) WHERE itunes_album_id IS NOT NULL;
CREATE INDEX idx_song_authority_recs_itunes_track ON song_authority_recommendations (itunes_track_id) WHERE itunes_track_id IS NOT NULL;

-- Bad Streaming Matches
CREATE INDEX idx_bad_streaming_matches_song_id ON bad_streaming_matches (song_id);
CREATE INDEX idx_bad_streaming_matches_service_id ON bad_streaming_matches (service, service_id);
CREATE INDEX idx_bad_streaming_matches_lookup ON bad_streaming_matches (service, block_level, service_id, song_id);

-- Content Reports
CREATE INDEX idx_content_reports_entity ON content_reports (entity_type, entity_id);
CREATE INDEX idx_content_reports_entity_id ON content_reports (entity_id);
CREATE INDEX idx_content_reports_status ON content_reports (status) WHERE status IN ('pending', 'reviewing');
CREATE INDEX idx_content_reports_external_source ON content_reports (external_source);
CREATE INDEX idx_content_reports_created_at ON content_reports (created_at DESC);

-- ============================================================================
-- COLUMN DOCUMENTATION
-- ============================================================================

COMMENT ON COLUMN songs.second_mb_id IS
    'Secondary MusicBrainz work ID for songs that exist as multiple works in MB '
    '(e.g., a standard split across two work entries, or an alternate version '
    'merged into a separate work). When set, recordings can be imported from '
    'either MB work and tracked via recordings.source_mb_work_id.';

COMMENT ON COLUMN recordings.source_mb_work_id IS
    'The MusicBrainz work ID this recording was imported from — either '
    'songs.musicbrainz_id or songs.second_mb_id. Used to assess which '
    'recordings came from the secondary MB work for review.';
