# Layered Data Architecture Design

## Problem Statement

Approach Note ingests data from multiple automated sources (MusicBrainz, Wikipedia, Spotify, etc.) and also allows manual curation. Currently, these can conflict:

1. **Automated imports can overwrite curated data** - When re-running crawlers, manually corrected data may be lost
2. **No way to assess crawler improvements** - Can't tell if a crawler change made things better or worse
3. **No provenance tracking** - Don't know where a piece of data came from or when it was last updated

## Proposed Solution: Source-of-Truth Layers

### Core Concept

Every enrichable field has three layers:

```
┌─────────────────────────────────────────┐
│         COMPUTED / DISPLAYED            │  ← What users see (API responses)
├─────────────────────────────────────────┤
│         CURATED (manual)                │  ← Human overrides (highest priority)
├─────────────────────────────────────────┤
│         CRAWLED (automated)             │  ← Automated ingestion (can always update)
└─────────────────────────────────────────┘
```

**Business Logic:** `computed = COALESCE(curated, crawled)`

This means:
- Crawlers can always run and update their values without fear of overwriting manual work
- Manual curation always wins
- Users always see the best available data

### Schema Pattern

For each enrichable field, store both layers plus metadata:

```sql
-- Example: performers table
performers (
    id UUID PRIMARY KEY,
    name VARCHAR NOT NULL,

    -- Wikipedia URL
    wikipedia_url_crawled VARCHAR,
    wikipedia_url_crawled_at TIMESTAMPTZ,
    wikipedia_url_crawled_source VARCHAR,      -- e.g., 'wiki_crawler_v2', 'musicbrainz_relation'

    wikipedia_url_curated VARCHAR,
    wikipedia_url_curated_at TIMESTAMPTZ,
    wikipedia_url_curated_by VARCHAR,          -- User or admin ID

    wikipedia_url GENERATED ALWAYS AS (
        COALESCE(wikipedia_url_curated, wikipedia_url_crawled)
    ) STORED,

    -- MusicBrainz ID
    musicbrainz_id_crawled VARCHAR,
    musicbrainz_id_crawled_at TIMESTAMPTZ,
    musicbrainz_id_crawled_source VARCHAR,

    musicbrainz_id_curated VARCHAR,
    musicbrainz_id_curated_at TIMESTAMPTZ,
    musicbrainz_id_curated_by VARCHAR,

    musicbrainz_id GENERATED ALWAYS AS (
        COALESCE(musicbrainz_id_curated, musicbrainz_id_crawled)
    ) STORED,

    -- Birth/death dates, bio, etc. follow same pattern...
)
```

### Handling Relational Data (e.g., Performer Roles)

For junction tables like `recording_performers`, the pattern applies to the `role` field:

```sql
recording_performers (
    id UUID PRIMARY KEY,
    recording_id UUID REFERENCES recordings(id),
    performer_id UUID REFERENCES performers(id),
    instrument_id UUID REFERENCES instruments(id),

    -- Role (leader/sideman/other)
    role_crawled VARCHAR,
    role_crawled_at TIMESTAMPTZ,
    role_crawled_source VARCHAR,               -- e.g., 'mb_artist_credit', 'mb_relation', 'fallback_first'

    role_curated VARCHAR,
    role_curated_at TIMESTAMPTZ,
    role_curated_by VARCHAR,

    role GENERATED ALWAYS AS (
        COALESCE(role_curated, role_crawled)
    ) STORED,

    UNIQUE(recording_id, performer_id)
)
```

Now the import logic can always do:
```sql
INSERT INTO recording_performers (recording_id, performer_id, role_crawled, role_crawled_source)
VALUES ($1, $2, $3, 'mb_artist_credit')
ON CONFLICT (recording_id, performer_id)
DO UPDATE SET
    role_crawled = EXCLUDED.role_crawled,
    role_crawled_at = NOW(),
    role_crawled_source = EXCLUDED.role_crawled_source;
```

This updates the crawled value but never touches the curated value.

## Crawler Scoring & Assessment

When updating a crawler, run a comparison before deploying:

### 1. Coverage Improvement

```sql
-- New values found (was NULL, now populated)
SELECT COUNT(*) as new_values
FROM performers
WHERE wikipedia_url_crawled_new IS NOT NULL
  AND wikipedia_url_crawled_old IS NULL;
```

### 2. Value Changes

```sql
-- Values that changed (for manual review)
SELECT
    name,
    wikipedia_url_crawled AS old_value,
    wikipedia_url_crawled_new AS new_value,
    wikipedia_url_curated AS curated_value
FROM performers_comparison
WHERE wikipedia_url_crawled_new != wikipedia_url_crawled
ORDER BY name;
```

### 3. Agreement with Curated Data

```sql
-- How often does new crawler agree with manual curation?
SELECT
    COUNT(*) FILTER (WHERE crawled_new = curated) as matches_curated,
    COUNT(*) FILTER (WHERE crawled_new != curated) as differs_from_curated,
    COUNT(*) FILTER (WHERE curated IS NOT NULL) as total_curated
FROM performers_comparison;
```

### 4. Regression Detection

```sql
-- Cases where old crawler matched curation but new crawler doesn't
SELECT *
FROM performers_comparison
WHERE wikipedia_url_crawled_old = wikipedia_url_curated
  AND wikipedia_url_crawled_new != wikipedia_url_curated;
```

## Implementation Approach

### Phase 1: Add Layered Columns (Non-Breaking)

1. Add `_crawled` and `_curated` columns alongside existing columns
2. Migrate existing data: copy current values to `_crawled` columns
3. Add generated `_computed` columns (or rename existing columns)
4. Update API queries to use computed columns (should already work if column names match)

### Phase 2: Update Crawlers

1. Modify crawlers to write to `_crawled` columns only
2. Add `_crawled_source` tracking to identify which crawler/version produced the data
3. Crawlers can now use `ON CONFLICT DO UPDATE` safely

### Phase 3: Add Curation UI/Tools

1. Admin interface to set `_curated` values
2. Logging of who curated what and when
3. Ability to "clear" curation (set to NULL) to revert to crawled value

### Phase 4: Crawler Comparison Tools

1. Script to run new crawler in "dry run" mode and capture proposed changes
2. Comparison reports showing coverage, changes, and agreement with curation
3. Dashboard for crawler health metrics over time

## Fields to Consider for This Pattern

### performers
- `wikipedia_url`
- `musicbrainz_id`
- `birth_date`, `death_date`
- `bio` / `biography`
- Primary image selection

### recordings
- `recording_date`
- `recording_year`
- `is_canonical`

### recording_performers
- `role` (leader/sideman/other)
- `instrument_id`

### songs
- `composer`
- `musicbrainz_id`, `second_mb_id`
- `year_written`

### releases
- `spotify_album_id`
- `cover_art_url`

## Trade-offs

### Pros
- Crawlers can run freely without data loss risk
- Full provenance tracking
- Can assess crawler quality quantitatively
- Manual curation is preserved and prioritized
- Can "undo" curation by setting to NULL

### Cons
- Schema complexity (3x columns per enrichable field)
- Migration effort for existing data
- Slightly more complex queries (though generated columns hide this)
- Storage increase (though mostly NULL values)

## Alternative: Separate Provenance Table

Instead of adding columns, use a separate table:

```sql
data_values (
    id UUID PRIMARY KEY,
    entity_type VARCHAR,      -- 'performer', 'recording', etc.
    entity_id UUID,
    field_name VARCHAR,       -- 'wikipedia_url', 'role', etc.

    crawled_value TEXT,
    crawled_at TIMESTAMPTZ,
    crawled_source VARCHAR,

    curated_value TEXT,
    curated_at TIMESTAMPTZ,
    curated_by VARCHAR,

    UNIQUE(entity_type, entity_id, field_name)
)
```

This is more flexible but requires JOINs everywhere and loses type safety.

## Next Steps

1. Pick a pilot table/field to test the pattern (suggest: `performers.wikipedia_url`)
2. Write migration to add layered columns
3. Update one crawler to use the new pattern
4. Build comparison tooling
5. Evaluate and expand to other fields

---

*Document created: December 2024*
*Status: Design proposal - not yet implemented*
