# Potential Recording Merge Ideas

## Background: GitHub Issue #38

Issue: Data duplication for "By the River Sainte Marie" - Paul Desmond recordings appearing multiple times.

## The Problem

MusicBrainz treats each release's recording as a separate entity, even when it's the same audio. This results in duplicate recordings in Approach Note when:
- The same performance appears on multiple compilations/box sets
- Different releases have slightly different masters (length variations)
- MusicBrainz contributors haven't merged the recordings

## Case Study: Paul Desmond "By the River Sainte Marie"

Three separate recordings in the database, all from the same 1964-09-04 session:

| DB Recording ID | MB Recording ID | Length | First Release | ISRC | Album Title |
|-----------------|-----------------|--------|---------------|------|-------------|
| `79d82cc3-d12a-4704-abd6-6b7ca9b65867` | `3c164a48-038d-4676-9bbc-f79d41eb4d8e` | 377,226ms | 2000 | USBB16401024 | The Best of the Complete Paul Desmond RCA Victor Recordings |
| `85dbd40d-8a78-467b-9a14-191841c1cb35` | `23d7cd12-16b6-4ed0-9dae-871417e15bda` | 377,306ms | 1995 | none | All Across the City |
| `9aa52b99-ae9c-48eb-882d-dd77f58b79c9` | `d7f6e4cb-35ee-4c32-bb87-8920f46627ce` | 375,693ms | 1995 | none | Paul Desmond: The Best of the Complete RCA Victor Recordings |

**Evidence they're the same performance:**
- All from RCA Victor sessions
- First two have nearly identical lengths (~377 sec)
- Third is ~1.5 sec shorter (likely different master/edit)
- Recording date is 1964-09-04 (on the one that has it)
- Same performers: Paul Desmond, Jim Hall, Connie Kay, Eugene Wright

## Recommended Merge for This Case

**Keep:** `79d82cc3-d12a-4704-abd6-6b7ca9b65867` (MB: `3c164a48...`)
- Has ISRC code
- Has correct recording date (1964-09-04)
- Has complete performer credits with instruments

**Merge into it:**
- `85dbd40d-8a78-467b-9a14-191841c1cb35` - has 7 releases to migrate
- `9aa52b99-ae9c-48eb-882d-dd77f58b79c9` - has 1 release to migrate

## Implementation Ideas

### Option 1: One-off Script for This Case
```python
# Pseudocode
master_recording_id = '79d82cc3-d12a-4704-abd6-6b7ca9b65867'
duplicate_ids = [
    '85dbd40d-8a78-467b-9a14-191841c1cb35',
    '9aa52b99-ae9c-48eb-882d-dd77f58b79c9'
]

for dup_id in duplicate_ids:
    # Move recording_releases to master
    UPDATE recording_releases SET recording_id = master_recording_id WHERE recording_id = dup_id

    # Move any authority recommendations
    UPDATE song_authority_recommendations SET recording_id = master_recording_id WHERE recording_id = dup_id

    # Aggregate performers (skip duplicates)
    INSERT INTO recording_performers ... ON CONFLICT DO NOTHING

    # Delete duplicate recording
    DELETE FROM recordings WHERE id = dup_id
```

### Option 2: General `merge_recordings.py` Script
Similar to existing `merge_songs.py`:
- Takes master_id and list of duplicate_ids
- Validates all recordings belong to same song
- Migrates all foreign key references
- Aggregates performer credits
- Updates is_canonical flag on master
- Logs all changes

### Option 3: Duplicate Detection Query
Find potential duplicates by matching:
- Same song_id
- Same or similar recording_year
- Similar track length (within 5 seconds)
- Overlapping performers

```sql
-- Find potential duplicates within same song
SELECT
    s.title as song_title,
    r1.id as rec1_id,
    r2.id as rec2_id,
    r1.album_title as rec1_album,
    r2.album_title as rec2_album,
    r1.recording_year,
    r2.recording_year
FROM recordings r1
JOIN recordings r2 ON r1.song_id = r2.song_id AND r1.id < r2.id
JOIN songs s ON s.id = r1.song_id
WHERE r1.recording_year = r2.recording_year
  AND EXISTS (
    SELECT 1 FROM recording_performers rp1
    JOIN recording_performers rp2 ON rp1.performer_id = rp2.performer_id
    WHERE rp1.recording_id = r1.id AND rp2.recording_id = r2.id
    AND rp1.role = 'leader' AND rp2.role = 'leader'
  )
ORDER BY s.title, r1.recording_year;
```

## Database Schema Notes

Relevant tables for merge:
- `recordings` - main table, has `is_canonical` flag (currently unused)
- `recording_releases` - links recordings to releases (many-to-many)
- `recording_performers` - links recordings to performers with roles
- `song_authority_recommendations` - may reference specific recordings

## Scope of Problem

For this one song ("By the River Sainte Marie"), there are 49 recordings total. Most are legitimately different performances by different artists over the decades (Jimmie Lunceford 1938, Nat King Cole 1940, etc.). The Paul Desmond case is specifically 3 recordings that should be 1.

A broader audit could identify similar cases across other songs.
