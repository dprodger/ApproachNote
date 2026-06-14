"""Wikipedia song-intro fetcher + updater.

Pulls the lead-section extract for a song's Wikipedia article — the
plain-text intro shown in the app's song detail — and writes it into
songs.structure.

This is the reusable, pipeline-wired version of the one-time backfill in
scripts/onetime_scripts/one_time_song_wiki_intro.py. The backfill populated
existing rows once; this module is called from core.song_research so every
newly imported / refreshed song with a wikipedia_url gets its intro pulled
in too (the import path previously set wikipedia_url but never the intro).

It lives under integrations/wikipedia (not the MusicBrainz updaters in
integrations/musicbrainz/song_updates.py) because it talks to the MediaWiki
extracts API, not MusicBrainz — it consumes the wikipedia_url that the MB
updater has already resolved onto the song.
"""

import logging
from urllib.parse import unquote, urlparse

import requests

logger = logging.getLogger(__name__)

# MediaWiki asks API clients to send a descriptive User-Agent.
USER_AGENT = "ApproachNote/1.0 (+support@approachnote.com)"
DEFAULT_SENTENCES = 4
REQUEST_TIMEOUT = 15


def parse_wikipedia_url(wikipedia_url: str):
    """Return (api_url, page_title) for a Wikipedia article URL, or (None, None).

    Honors the language subdomain so de.wikipedia.org URLs hit the right API.
    """
    try:
        parsed = urlparse(wikipedia_url)
        if not parsed.netloc or '/wiki/' not in parsed.path:
            return None, None
        title = parsed.path.split('/wiki/', 1)[1]
        title = title.split('#', 1)[0]
        title = unquote(title)
        if not title:
            return None, None
        api_url = f"{parsed.scheme}://{parsed.netloc}/w/api.php"
        return api_url, title
    except Exception:
        return None, None


def fetch_wikipedia_intro(page_title: str, api_url: str,
                          sentences: int = DEFAULT_SENTENCES,
                          session: requests.Session = None):
    """Fetch the lead-section plain-text extract for a Wikipedia page.

    Returns the extract string, or None if the page is missing / empty / the
    request fails. Raises nothing for HTTP-level non-200s (logs + returns
    None); network exceptions propagate to the caller.
    """
    sess = session or requests.Session()
    params = {
        'action': 'query',
        'format': 'json',
        'prop': 'extracts',
        'titles': page_title,
        'redirects': 1,
        'exintro': 1,
        'explaintext': 1,
        'exsentences': sentences,
    }
    headers = {'User-Agent': USER_AGENT, 'Accept': 'application/json'}
    resp = sess.get(api_url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
    if resp.status_code != 200:
        logger.warning("Wikipedia returned status %s for %s", resp.status_code, page_title)
        return None
    pages = resp.json().get('query', {}).get('pages', {})
    if not pages:
        return None
    page = next(iter(pages.values()))
    if 'missing' in page:
        logger.warning("Wikipedia page missing: %s", page_title)
        return None
    extract = (page.get('extract') or '').strip()
    return extract or None


def update_song_wikipedia_intro(song_id: str,
                                sentences: int = DEFAULT_SENTENCES,
                                force_refresh: bool = False,
                                dry_run: bool = False) -> bool:
    """Populate songs.structure with the song's Wikipedia intro.

    Reads the wikipedia_url already on the song (set earlier in the research
    pipeline by update_song_wikipedia_url), fetches the lead-section extract,
    and stores it in songs.structure.

    Idempotent like the sibling MB updaters: skips a song that already has
    structure text, UNLESS force_refresh is set — a deep refresh re-pulls
    the intro so edits/expansions on Wikipedia flow through.

    Args:
        song_id: UUID of the song
        sentences: Number of intro sentences to request from MediaWiki
        force_refresh: Overwrite existing structure text if True
        dry_run: Log what would happen without writing to the DB

    Returns:
        bool: True if structure was updated (or would be, in dry-run).
    """
    from db_utils import get_db_connection

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT wikipedia_url, structure, title FROM songs WHERE id = %s",
                    (song_id,),
                )
                row = cur.fetchone()

        if not row:
            return False

        wikipedia_url = row['wikipedia_url']
        current_structure = row['structure']
        song_title = row['title']

        if not wikipedia_url:
            logger.debug("Song has no Wikipedia URL, skipping intro update")
            return False

        # Don't clobber an existing intro unless explicitly refreshing.
        if (current_structure or '').strip() and not force_refresh:
            logger.debug("Song '%s' already has intro text, skipping", song_title)
            return False

        api_url, page_title = parse_wikipedia_url(wikipedia_url)
        if not api_url:
            logger.warning("Could not parse Wikipedia URL for '%s': %s",
                           song_title, wikipedia_url)
            return False

        intro = fetch_wikipedia_intro(page_title, api_url, sentences=sentences)
        if not intro:
            logger.debug("No Wikipedia intro returned for '%s'", song_title)
            return False

        if dry_run:
            logger.info("[DRY RUN] Would update intro for '%s' (%d chars)",
                        song_title, len(intro))
            return True

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE songs SET structure = %s, updated_at = CURRENT_TIMESTAMP "
                    "WHERE id = %s",
                    (intro, song_id),
                )
                conn.commit()

        logger.info("✓ Updated Wikipedia intro for '%s' (%d chars)",
                    song_title, len(intro))
        return True

    except requests.RequestException as e:
        logger.error("Wikipedia request error updating intro for song %s: %s", song_id, e)
        return False
    except Exception as e:
        logger.error("Error updating Wikipedia intro for song %s: %s", song_id, e)
        return False
