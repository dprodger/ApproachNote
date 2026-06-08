-- ============================================================================
-- ILLUSTRATIVE FORMAT ONLY -- shows the exact SQL emitted by
--   fetch_commons_images.py --emit-sql
-- The structure (idempotent CTE upsert into images + link into artist_images)
-- is final and schema-correct. The literal VALUES are placeholders: run the
-- script against the live Wikimedia API to emit authoritative rows.
-- DO NOT execute this file as-is.
-- ============================================================================

-- Idempotent ingest of PD/CC0 imagery into images + artist_images
-- Performer: Dave Brubeck (REPLACE-WITH-REAL-PERFORMER-UUID)
BEGIN;

-- [0] Dave Brubeck, 1954  (public_domain)  https://commons.wikimedia.org/wiki/File:Dave_Brubeck_1954.jpg
WITH img AS (
    INSERT INTO images (
        url, source, source_identifier, license_type, license_url,
        attribution, width, height, thumbnail_url, source_page_url
    ) VALUES (
        'https://upload.wikimedia.org/wikipedia/commons/<path>/Dave_Brubeck_1954.jpg',
        'wikimedia_commons', '<commons_pageid>', 'public_domain', NULL,
        '<author>', NULL, NULL,
        'https://upload.wikimedia.org/.../400px-Dave_Brubeck_1954.jpg',
        'https://commons.wikimedia.org/wiki/File:Dave_Brubeck_1954.jpg'
    )
    ON CONFLICT (url) DO UPDATE SET updated_at = CURRENT_TIMESTAMP
    RETURNING id
)
INSERT INTO artist_images (performer_id, image_id, is_primary, display_order)
SELECT 'REPLACE-WITH-REAL-PERFORMER-UUID', id, TRUE, 0 FROM img
ON CONFLICT (performer_id, image_id) DO NOTHING;

-- [1] Dave Brubeck in performance  (cc0)  https://commons.wikimedia.org/wiki/File:DaveBrubeck18.JPG
WITH img AS (
    INSERT INTO images (
        url, source, source_identifier, license_type, license_url,
        attribution, width, height, thumbnail_url, source_page_url
    ) VALUES (
        'https://upload.wikimedia.org/wikipedia/commons/<path>/DaveBrubeck18.JPG',
        'wikimedia_commons', '<commons_pageid>', 'cc0',
        'https://creativecommons.org/publicdomain/zero/1.0/',
        '<author>', NULL, NULL,
        'https://upload.wikimedia.org/.../400px-DaveBrubeck18.JPG',
        'https://commons.wikimedia.org/wiki/File:DaveBrubeck18.JPG'
    )
    ON CONFLICT (url) DO UPDATE SET updated_at = CURRENT_TIMESTAMP
    RETURNING id
)
INSERT INTO artist_images (performer_id, image_id, is_primary, display_order)
SELECT 'REPLACE-WITH-REAL-PERFORMER-UUID', id, FALSE, 1 FROM img
ON CONFLICT (performer_id, image_id) DO NOTHING;

COMMIT;
