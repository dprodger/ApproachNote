#!/usr/bin/env python3
"""
One-Time Script: Backfill songs.structure with Wikipedia intro text.

Walks songs that have a wikipedia_url, fetches the first few sentences of
the lead section from Wikipedia, and writes the text into songs.structure.

This is temporary scaffolding so the user can preview Wikipedia intros
inside the app. The structure column will eventually be renamed or replaced
with a dedicated column for this content.
"""

import sys
import argparse
import logging
import time
from pathlib import Path
from urllib.parse import unquote, urlparse

import requests

# Resolve backend/ on sys.path so `db_utils` imports work from this nested dir.
BACKEND_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(BACKEND_DIR))
sys.path.insert(0, str(BACKEND_DIR / 'scripts'))

# Load .env from backend/
try:
    from dotenv import load_dotenv
    load_dotenv(BACKEND_DIR / '.env')
except ImportError:
    pass

from db_utils import get_db_connection  # noqa: E402

LOG_DIR = Path(__file__).resolve().parent.parent / 'log'
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / 'one_time_song_wiki_intro.log'),
    ],
)
logger = logging.getLogger(__name__)

USER_AGENT = "ApproachNote/1.0 (+support@approachnote.com)"
RATE_LIMIT_SECONDS = 1.0


def parse_wikipedia_url(wikipedia_url: str):
    """
    Return (api_url, page_title) for a Wikipedia article URL, or (None, None).

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


def fetch_wikipedia_intro(session, api_url, page_title, sentences):
    """Fetch the lead-section extract for a Wikipedia page."""
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
    resp = session.get(api_url, params=params, timeout=15)
    if resp.status_code != 200:
        logger.warning(f"  Wikipedia returned status {resp.status_code} for {page_title}")
        return None
    pages = resp.json().get('query', {}).get('pages', {})
    if not pages:
        return None
    page = next(iter(pages.values()))
    if 'missing' in page:
        logger.warning(f"  Wikipedia page missing: {page_title}")
        return None
    extract = (page.get('extract') or '').strip()
    return extract or None


def get_songs_with_wikipedia():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, title, wikipedia_url, structure
                FROM songs
                WHERE wikipedia_url IS NOT NULL
                  AND wikipedia_url <> ''
                ORDER BY title
                """
            )
            return cur.fetchall()


def update_song_structure(song_id, structure_text):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE songs SET structure = %s WHERE id = %s",
                (structure_text, song_id),
            )
        conn.commit()


def main():
    parser = argparse.ArgumentParser(
        description='Backfill songs.structure with Wikipedia intro text.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python one_time_song_wiki_intro.py --dry-run --limit 5
  python one_time_song_wiki_intro.py --sentences 3
  python one_time_song_wiki_intro.py --only-empty
        """,
    )
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would happen without writing to the DB')
    parser.add_argument('--limit', type=int,
                        help='Max number of songs to process')
    parser.add_argument('--sentences', type=int, default=4,
                        help='Number of intro sentences to request (default: 4)')
    parser.add_argument('--only-empty', action='store_true',
                        help='Skip songs whose structure column is already populated')
    parser.add_argument('--debug', action='store_true', help='Verbose logging')
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    songs = get_songs_with_wikipedia()
    if args.only_empty:
        songs = [s for s in songs if not (s.get('structure') or '').strip()]
    if args.limit:
        songs = songs[: args.limit]

    logger.info("=" * 80)
    logger.info("BACKFILL songs.structure WITH WIKIPEDIA INTRO")
    logger.info("=" * 80)
    if args.dry_run:
        logger.info("*** DRY RUN — no DB writes ***")
    logger.info(
        f"Songs to process: {len(songs)} "
        f"(sentences={args.sentences}, only_empty={args.only_empty})"
    )
    logger.info("")

    session = requests.Session()
    session.headers.update({'User-Agent': USER_AGENT, 'Accept': 'application/json'})

    stats = {'processed': 0, 'updated': 0, 'no_intro': 0, 'bad_url': 0, 'errors': 0}
    last_request = 0.0
    total = len(songs)

    for song in songs:
        stats['processed'] += 1
        title = song['title']
        url = song['wikipedia_url']

        api_url, page_title = parse_wikipedia_url(url)
        if not api_url:
            logger.warning(f"[{stats['processed']}/{total}] {title}: cannot parse {url}")
            stats['bad_url'] += 1
            continue

        elapsed = time.time() - last_request
        if elapsed < RATE_LIMIT_SECONDS:
            time.sleep(RATE_LIMIT_SECONDS - elapsed)
        last_request = time.time()

        try:
            intro = fetch_wikipedia_intro(session, api_url, page_title, args.sentences)
        except requests.RequestException as e:
            logger.error(f"[{stats['processed']}/{total}] {title}: request error {e}")
            stats['errors'] += 1
            continue
        except Exception as e:
            logger.error(f"[{stats['processed']}/{total}] {title}: unexpected error {e}",
                         exc_info=True)
            stats['errors'] += 1
            continue

        if not intro:
            logger.info(f"[{stats['processed']}/{total}] {title}: no intro returned")
            stats['no_intro'] += 1
            continue

        snippet = intro.replace('\n', ' / ')
        if len(snippet) > 120:
            snippet = snippet[:117] + '…'
        logger.info(f"[{stats['processed']}/{total}] {title}: {snippet}")

        if args.dry_run:
            stats['updated'] += 1
            continue

        try:
            update_song_structure(song['id'], intro)
            stats['updated'] += 1
        except Exception as e:
            logger.error(f"  DB update failed for {title}: {e}", exc_info=True)
            stats['errors'] += 1

    logger.info("")
    logger.info("=" * 80)
    logger.info("SUMMARY")
    logger.info("=" * 80)
    for k, v in stats.items():
        logger.info(f"  {k:<10} {v}")
    logger.info("=" * 80)
    return stats['errors'] == 0


if __name__ == "__main__":
    try:
        ok = main()
        sys.exit(0 if ok else 1)
    except KeyboardInterrupt:
        print("\nCancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)
