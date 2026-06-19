-- sql/migrations/022_add_song_requests.sql
--
-- Add the song_requests table: user-submitted requests to add a song,
-- gated behind manual admin approval.
--
-- Previously the iOS/Mac apps' "Search MusicBrainz" flow POSTed to
-- /v1/musicbrainz/import, which immediately inserted into `songs` and queued
-- background research — the song went live the instant a user imported it.
-- Now that the catalog is opening up to more users, app submissions instead
-- land here as `pending` requests. An admin reviews them at
-- /admin/song-requests and approves (which creates the song + queues research,
-- reusing the existing import logic) or rejects them.
--
-- The /v1/musicbrainz/import endpoint is retained but is now admin-only, as a
-- direct "add it now" shortcut that bypasses the request queue.
--
-- Run: psql $DATABASE_URL -f sql/migrations/022_add_song_requests.sql

CREATE TABLE IF NOT EXISTS song_requests (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    musicbrainz_id  VARCHAR(36) NOT NULL,
    title           VARCHAR(255) NOT NULL,
    composer        VARCHAR(500),
    requested_by    UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    status          VARCHAR(20) NOT NULL DEFAULT 'pending',
    review_note     TEXT,
    reviewed_by     UUID REFERENCES users(id) ON DELETE SET NULL,
    reviewed_at     TIMESTAMP WITH TIME ZONE,
    created_song_id UUID REFERENCES songs(id) ON DELETE SET NULL,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT song_requests_status_check
        CHECK (status IN ('pending', 'approved', 'rejected'))
);

COMMENT ON TABLE song_requests IS
    'User-submitted requests to add a song from MusicBrainz, awaiting manual admin approval. Approval creates the songs row + queues research; see routes/admin_song_requests.py.';
COMMENT ON COLUMN song_requests.musicbrainz_id IS 'MusicBrainz work UUID the user picked from the in-app search.';
COMMENT ON COLUMN song_requests.requested_by IS 'User who submitted the request.';
COMMENT ON COLUMN song_requests.status IS 'pending | approved | rejected.';
COMMENT ON COLUMN song_requests.review_note IS 'Optional admin note, typically the reason a request was rejected.';
COMMENT ON COLUMN song_requests.created_song_id IS 'The songs.id created when this request was approved (NULL until/unless approved).';

-- Only one OPEN request per work; approved/rejected rows don't block a later
-- re-request of the same work.
CREATE UNIQUE INDEX IF NOT EXISTS idx_song_requests_pending_mbid
    ON song_requests(musicbrainz_id)
    WHERE status = 'pending';

-- Admin list view orders by status then recency.
CREATE INDEX IF NOT EXISTS idx_song_requests_status_created
    ON song_requests(status, created_at DESC);

-- Rollback:
--   DROP TABLE IF EXISTS song_requests;
