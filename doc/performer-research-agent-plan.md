# Performer Research Agent

## Context

Many recordings in our database have incomplete performer data because MusicBrainz lacks artist relationship data for them — it only has the credited artist (e.g., "João Donato") with no sidemen, instruments, or session details. Currently, filling in this data requires tedious manual research across multiple web sources. This agent automates that process using Claude's tool-use API to orchestrate searches across structured APIs (Discogs) and the open web, producing a research report for human review before any DB changes.

## Architecture Overview

The agent is a Python script that:
1. Gathers everything we already know about a recording from our DB and MusicBrainz
2. Queries the Discogs API for release credits (structured data, most reliable)
3. If Discogs lacks data, hands off to Claude with web search tools to find personnel from blogs, liner notes, Amazon, etc.
4. Outputs a structured markdown research file (same format as `backend/research/donatural-performers.md`)

```
Script invocation
  → Gather context (DB + MusicBrainz)
  → Try Discogs API for release credits
  → If insufficient: Claude agent loop with web search tools
  → Write research markdown file
```

## Files to Create

### 1. `backend/discogs_client.py` — Discogs API client

Lightweight client for the Discogs API v2. Follows existing patterns from `spotify_client.py` and `mb_utils.py`.

- **Auth**: Personal access token via `DISCOGS_PERSONAL_ACCESS_TOKEN` env var
- **Rate limiting**: 60 req/min authenticated, 25 unauthenticated. Use response headers (`X-Discogs-Ratelimit-Remaining`) and sleep when needed
- **Caching**: 30-day file cache matching `mb_utils.py` pattern
- **Key methods**:
  - `search_release(artist, title, year=None) → list[dict]` — search for releases
  - `get_release(release_id) → dict` — full release with `extraartists` credits
  - `get_master(master_id) → dict` — master release info
  - `extract_personnel(release_data, track_position=None) → list[dict]` — parse `extraartists` and `tracklist[n].extraartists` into `[{name, instrument, role, discogs_artist_id}]`

Discogs release `extraartists` contain the personnel credits with roles like "Drums", "Bass", "Piano", "Trumpet", etc. Track-level `extraartists` provide per-track credits when available.

### 2. `backend/performer_research_agent.py` — Main agent script

Entry point and orchestration. Run as:
```bash
python performer_research_agent.py <recording_db_id>
# or
python performer_research_agent.py --mb-id <musicbrainz_recording_id>
```

**Step-by-step flow:**

#### Phase 1: Context Gathering (no LLM needed)
1. Look up recording in our DB → title, MB recording ID, existing performers, song title
2. Fetch recording details from MusicBrainz API (`mb_utils.get_recording_details`) → artist credits, relations, releases
3. Fetch release details for each linked release → track listing, release date, label
4. Check if MusicBrainz release has a Discogs URL in its `url-rels` (need to add `url-rels` to the MB release fetch `inc` parameter)
5. Compile context object with all known info

#### Phase 2: Discogs Lookup (structured, no LLM)
1. If we have a Discogs URL from MB → extract release ID, call `discogs_client.get_release()`
2. Otherwise → `discogs_client.search_release(artist_credit, release_title, year)`
3. Parse `extraartists` credits from Discogs release data
4. If the recording is on a specific track, filter to track-level credits + album-level credits
5. If Discogs returns good personnel data (3+ performers with instruments) → skip to Phase 4

#### Phase 3: Claude Web Research (LLM agent loop)
Only runs if Discogs didn't produce sufficient results.

Uses Anthropic Python SDK (`anthropic` package) with tool use. Claude is given:
- Full context from Phase 1 (recording title, artist, album, year, label, track position)
- What we already know (existing performers, Discogs partial results)
- Tools to search the web and fetch pages

**Tools provided to Claude:**
- `web_search(query) → list[{title, url, snippet}]` — wraps a web search API (SerpAPI, Brave Search, or Google Custom Search)
- `fetch_page(url) → str` — fetches a URL and returns cleaned text (using `requests` + `BeautifulSoup`, similar to `wiki_utils.py` patterns)
- `search_musicbrainz_artist(name) → dict` — look up an artist name on MusicBrainz to get their MBID
- `done(personnel: list[dict], sources: list[str], notes: str)` — signal completion with results

**Claude system prompt** instructs it to:
- Search for the album/release name + "credits" or "personnel" or "liner notes"
- Check Discogs web pages, AllMusic, Amazon listings, music blogs
- For live albums: identify the core band vs per-track guests
- Cross-reference multiple sources when possible
- Return structured personnel with confidence levels
- Always cite sources

**Agent loop**: Max 15 tool-use turns. Claude calls tools, gets results, reasons, calls more tools, eventually calls `done()`.

#### Phase 4: Output Research File
Write a markdown file to `backend/research/<slug>.md` using the same format as `donatural-performers.md`:
- Recording metadata (DB ID, MB ID, Spotify ID, etc.)
- Context paragraph explaining what was found
- Personnel table: Performer | Instrument | MusicBrainz Artist ID | Source | Confidence | Notes
- Guest artists on other tracks (if applicable)
- Sources list with URLs
- TODO section noting prerequisites for DB insertion (e.g., `source` column on `recording_performers`)

### 3. `backend/web_search_utils.py` — Web search wrapper

Simple utility that wraps a web search provider for use as a Claude tool.

- Support multiple backends: **Brave Search API** (recommended, $0 for 2000 queries/mo free tier), or SerpAPI, or Google Custom Search
- `SEARCH_API_KEY` and `SEARCH_API_PROVIDER` env vars
- `search(query, num_results=10) → list[{title, url, snippet}]`
- `fetch_and_clean(url) → str` — fetch URL, convert HTML to clean text using BeautifulSoup (reuse patterns from `wiki_utils.py`), truncate to ~8000 chars for LLM context

## Files to Modify

### `backend/mb_utils.py`
- In `get_release_details()`: add `url-rels` to the `inc` parameter so we can get Discogs links from MusicBrainz releases

### `backend/requirements.txt`
- Add `anthropic` (Claude SDK)
- Add `beautifulsoup4` if not already present (`wiki_utils.py` uses it so likely present)

## Environment Variables (new)

| Variable | Required | Description |
|----------|----------|-------------|
| `ANTHROPIC_API_KEY` | Yes | Claude API key for agent reasoning |
| `DISCOGS_PERSONAL_ACCESS_TOKEN` | No | Discogs API token (25 req/min without, 60 with) |
| `SEARCH_API_KEY` | Only if Phase 3 needed | Brave Search (or SerpAPI) API key |
| `SEARCH_API_PROVIDER` | No | `brave` (default), `serpapi`, or `google` |

## Cost Estimate

Per recording research:
- Discogs API: free (within rate limits)
- Claude (if needed): ~$0.05–0.20 per recording (depends on number of search iterations, using Haiku for cost efficiency or Sonnet for quality)
- Web search API: free tier covers ~2000 searches/month

## Prerequisite: `recording_performers.source` Column

Before inserting agent-researched performers into the DB, add a `source` column to `recording_performers` to distinguish MusicBrainz-imported vs agent-researched vs manually-added performers. This mirrors the existing `recording_date_source` pattern on the `recordings` table. Values: `'musicbrainz'`, `'discogs'`, `'web_research'`, `'manual'`.

## Verification

1. Run the agent on recording `add71fe0-4bbf-4102-af55-d2e2b3178c38` (João Donato - Minha Saudade) and compare output against the manually-researched `backend/research/donatural-performers.md`
2. Test with a recording that has good Discogs data to verify the Discogs-only path works without invoking Claude
3. Test with a recording that has complete MusicBrainz relationships to verify it reports "already has performer data" gracefully

## Existing Code to Reuse

| Utility | File | What to reuse |
|---------|------|---------------|
| MusicBrainz API client | `backend/mb_utils.py` | `MusicBrainzSearcher.get_recording_details()`, `get_release_details()`, caching pattern |
| Performer DB operations | `backend/mb_performer_importer.py` | `_batch_get_performers()`, instrument lookup patterns |
| Wikipedia HTML parsing | `backend/wiki_utils.py` | BeautifulSoup patterns for cleaning HTML to text |
| Spotify client patterns | `backend/spotify_client.py` | Rate limiting, auth token, caching patterns to mirror for Discogs |
| DB connection | `backend/db_utils.py` | `get_db_connection()` context manager |
