"""
Extract biographical data and imagery for a performer from their Wikipedia
page.

This is the parsing/fetching layer behind the
('wikipedia', 'enrich_performer_from_wikipedia') worker handler. It is
deliberately free of any database access: callers pass in a performer's
Wikipedia URL and get back a `PerformerWikipediaData` describing whatever the
page yields. The handler decides what (if anything) to persist.

Two source paths feed the four fields:

  - birth_date / death_date / biography come from the rendered article HTML
    (fetched via WikipediaSearcher, honouring its 7-day disk cache), parsed
    out of the infobox and lead paragraphs.

  - image: only the page's lead image is fetched, via the MediaWiki
    `pageimages` API (the chosen lead image at full resolution), with an
    infobox-HTML scrape as a fallback when the API returns no image. Body /
    extra-infobox photos are intentionally not harvested — in practice they
    carry too many off-target shots to be worth keeping. License/attribution
    metadata is pulled from the `imageinfo` API. The handler dedups by URL, so
    a re-found lead is left untouched and only a genuinely-new one gets linked.
    (`fetch_all_images` still implements the full multi-image harvest for
    callers that want it, e.g. the offline scripts.)

The date and biography extraction is ported from the old
scripts/load_artists_from_wikipedia.py; the image fetching is a streamlined
port of scripts/fetch_artist_images.py (trimmed to the Wikipedia source and
the cases we actually hit).
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import quote, unquote

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

_API_URL = "https://en.wikipedia.org/w/api.php"

# How many images we'll harvest from a single performer page per run. Keeps the
# lead/infobox photo plus a handful of body photos without bloating performers
# who have very long articles.
_DEFAULT_IMAGE_LIMIT = 6

# Minimum rendered width (px) for a body image to count as content. Filters out
# inline icons, flag thumbnails, and pictograms, which render small.
_MIN_CONTENT_IMAGE_WIDTH = 100

# Raster extensions we treat as photographs. SVGs (logos, signatures, icons) and
# media stills (audio/video) are excluded.
_PHOTO_EXTENSIONS = ('.jpg', '.jpeg', '.png')

# Substrings in a Commons filename that mark non-photo clutter surviving the
# raster + size filters (e.g. a wide PNG logo or signature). Lower-cased match.
_CLUTTER_FILENAME_TERMS = (
    'logo', 'signature', 'commons-logo', 'magnify', 'speaker', 'loudspeaker',
    'sound-icon', 'audio', 'wiktionary', 'wikiquote', 'wikisource',
    'wikimedia', 'wikidata', 'ambox', 'question_book', 'padlock', 'edit-icon',
)

_MONTHS = {
    'january': '01', 'jan': '01',
    'february': '02', 'feb': '02',
    'march': '03', 'mar': '03',
    'april': '04', 'apr': '04',
    'may': '05',
    'june': '06', 'jun': '06',
    'july': '07', 'jul': '07',
    'august': '08', 'aug': '08',
    'september': '09', 'sep': '09', 'sept': '09',
    'october': '10', 'oct': '10',
    'november': '11', 'nov': '11',
    'december': '12', 'dec': '12',
}


@dataclass
class WikipediaImage:
    """A single image lifted from a Wikipedia page, with enough metadata to
    populate the `images` table."""
    url: str
    thumbnail_url: Optional[str] = None
    source_identifier: Optional[str] = None  # the page title
    source_page_url: Optional[str] = None
    license_type: str = 'unknown'
    license_url: Optional[str] = None
    attribution: Optional[str] = None
    width: Optional[int] = None
    height: Optional[int] = None
    source: str = 'wikipedia'


@dataclass
class PerformerWikipediaData:
    """Everything we could pull from one performer's Wikipedia page. Any field
    may be None (or, for images, an empty list) when the page doesn't carry it
    (or wasn't requested)."""
    birth_date: Optional[str] = None
    death_date: Optional[str] = None
    biography: Optional[str] = None
    # All harvested images, lead/primary candidate first, in page order.
    images: list[WikipediaImage] = field(default_factory=list)
    page_fetched: bool = False  # False when the article HTML couldn't be loaded


# ---------------------------------------------------------------------------
# Date parsing (ported from load_artists_from_wikipedia.py)
# ---------------------------------------------------------------------------

def _month_to_num(month_str: str) -> str:
    return _MONTHS.get(month_str.lower(), '01')


def parse_date(date_text: str) -> Optional[str]:
    """Parse a Wikipedia date string into YYYY-MM-DD, or None.

    Wikipedia infoboxes often embed a hidden ISO date in parentheses, e.g.
    "(1941-06-12)June 12, 1941" — we prefer that when present, then fall back
    to a handful of human-readable formats.
    """
    if not date_text:
        return None

    # Hidden ISO date in parentheses is the most reliable signal.
    paren_iso = re.search(r'\((\d{4})-(\d{2})-(\d{2})\)', date_text)
    if paren_iso:
        candidate = _valid_iso_date(
            f"{paren_iso.group(1)}-{paren_iso.group(2)}-{paren_iso.group(3)}"
        )
        if candidate:
            return candidate
        # An impossible hidden date (e.g. Feb 30) — fall through and try the
        # human-readable forms in the cleaned text.

    cleaned = re.sub(r'\([^)]*\)', '', date_text).strip()

    patterns = [
        # ISO: "1926-05-26"
        (r'(\d{4})-(\d{2})-(\d{2})',
         lambda m: f"{m.group(1)}-{m.group(2)}-{m.group(3)}"),
        # "May 26, 1926"
        (r'(\w+)\s+(\d{1,2}),?\s+(\d{4})',
         lambda m: f"{m.group(3)}-{_month_to_num(m.group(1))}-{int(m.group(2)):02d}"),
        # "26 May 1926"
        (r'(\d{1,2})\s+(\w+)\s+(\d{4})',
         lambda m: f"{m.group(3)}-{_month_to_num(m.group(2))}-{int(m.group(1)):02d}"),
        # "May 1926"
        (r'(\w+)\s+(\d{4})',
         lambda m: f"{m.group(2)}-{_month_to_num(m.group(1))}-01"),
        # bare year
        (r'^(\d{4})$', lambda m: f"{m.group(1)}-01-01"),
    ]

    for pattern, formatter in patterns:
        match = re.search(pattern, cleaned)
        if not match:
            continue
        try:
            formatted = formatter(match)
        except Exception:  # noqa: BLE001 - bad capture, try next pattern
            continue
        # The formatter can emit an impossible calendar date (e.g. a captured
        # day-of-month that doesn't exist, or a stray number read as the day),
        # which Postgres rejects with DatetimeFieldOverflow. Validate against
        # the real calendar and try the next, looser pattern if it fails.
        candidate = _valid_iso_date(formatted)
        if candidate:
            return candidate
    return None


def _valid_iso_date(s: str) -> Optional[str]:
    """Return `s` iff it is a real YYYY-MM-DD calendar date, else None."""
    try:
        datetime.strptime(s, '%Y-%m-%d')
        return s
    except ValueError:
        return None


def extract_birth_death_dates(soup: BeautifulSoup) -> tuple[Optional[str], Optional[str]]:
    """Pull (birth_date, death_date) as YYYY-MM-DD strings from the infobox."""
    infobox = soup.find('table', {'class': 'infobox'})
    if not infobox:
        return (None, None)

    birth_date = None
    born_row = infobox.find('th', string=re.compile(r'Born', re.IGNORECASE))
    if born_row:
        born_cell = born_row.find_next_sibling('td')
        if born_cell:
            birth_date = parse_date(born_cell.get_text())

    death_date = None
    died_row = infobox.find('th', string=re.compile(r'Died', re.IGNORECASE))
    if died_row:
        died_cell = died_row.find_next_sibling('td')
        if died_cell:
            death_date = parse_date(died_cell.get_text())

    return (birth_date, death_date)


def extract_biography(soup: BeautifulSoup) -> Optional[str]:
    """Return the first few substantial lead paragraphs as a biography blurb."""
    content_div = None
    for div in soup.find_all('div', class_='mw-parser-output'):
        if div.find('p'):
            content_div = div
            break
    if not content_div:
        return None

    paragraphs: list[str] = []
    for p in content_div.find_all('p'):
        text = p.get_text().strip()
        if (len(text) > 50
                and 'coordinates' not in text.lower()
                and 'disambiguation' not in text.lower()
                and not text.startswith('This article')
                and not text.startswith('For other uses')):
            paragraphs.append(text)
            if len(paragraphs) >= 3:
                break

    if not paragraphs:
        return None

    # Clean each paragraph independently — strip citation markers and collapse
    # runaway intra-paragraph whitespace — but keep the paragraphs separated by
    # a blank line so the stored blurb retains its source paragraph breaks. The
    # app renders the '\n\n' separators as discrete paragraphs (ExpandableProse);
    # a single re.sub(r'\s+', ' ') over the joined text would erase them.
    cleaned: list[str] = []
    for paragraph in paragraphs:
        paragraph = re.sub(r'\[\d+\]', '', paragraph)     # citation markers
        paragraph = re.sub(r'\s+', ' ', paragraph).strip()
        if paragraph:
            cleaned.append(paragraph)

    if not cleaned:
        return None
    return '\n\n'.join(cleaned)


# ---------------------------------------------------------------------------
# Image fetching (streamlined port of fetch_artist_images.py)
# ---------------------------------------------------------------------------

def _page_title_from_url(wikipedia_url: str) -> Optional[str]:
    """Extract the (decoded) page title from a /wiki/<Title> URL."""
    match = re.search(r'/wiki/(.+)$', wikipedia_url)
    if not match:
        return None
    return unquote(match.group(1))


def _absolute_src(src: str) -> str:
    """Resolve a protocol-relative or root-relative <img src> to an absolute
    https URL."""
    if src.startswith('//'):
        return 'https:' + src
    if src.startswith('/'):
        return 'https://en.wikipedia.org' + src
    return src


def _full_res_from_thumb(img_src: str) -> str:
    """Recover the original-resolution file URL from a Wikipedia thumbnail URL.

    A thumbnail is .../thumb/a/ab/Foo.jpg/220px-Foo.jpg, where the original is
    .../a/ab/Foo.jpg — i.e. the file name appears as the directory before a
    size-prefixed leaf. Drop the /thumb/ segment and strip that trailing leaf
    to recover the original. A URL that isn't a thumbnail is returned unchanged.
    """
    if '/thumb/' not in img_src:
        return img_src
    full = img_src.replace('/thumb/', '/')
    return re.sub(r'/[^/]+$', '', full)


def _image_filename(url: Optional[str]) -> Optional[str]:
    """Return the canonical Commons file name from a thumbnail or original URL,
    used to dedup the same underlying image across thumb sizes.

    Thumbnail: .../thumb/a/ab/Foo.jpg/220px-Foo.jpg -> 'Foo.jpg'
    Original:  .../commons/a/ab/Foo.jpg             -> 'Foo.jpg'
    """
    if not url:
        return None
    if '/thumb/' in url:
        # The file title is the segment before the size-prefixed leaf.
        parts = url.split('/')
        if len(parts) >= 2:
            return unquote(parts[-2])
    return unquote(url.split('/')[-1].split('?')[0])


def _normalize_license(license_str: Optional[str]) -> str:
    if not license_str or license_str == 'unknown':
        return 'unknown'
    s = license_str.lower()
    if 'public domain' in s:
        return 'public_domain'
    if 'cc0' in s:
        return 'cc0'
    if 'cc-by-sa' in s or 'cc by-sa' in s:
        return 'cc_by_sa'
    if 'cc-by' in s or 'cc by' in s:
        return 'cc_by'
    if 'fair use' in s:
        return 'fair_use'
    return 'other'


def _fetch_image_license(searcher, image_url: str) -> dict:
    """Look up license/attribution/size for a Commons file via imageinfo.

    Returns a dict with keys license_type, license_url, attribution, width,
    height, url (the canonical file URL, which may differ from image_url).
    Best-effort: returns sensible defaults on any failure.
    """
    out = {
        'license_type': 'unknown', 'license_url': None, 'attribution': None,
        'width': None, 'height': None, 'url': image_url,
    }
    image_filename = image_url.split('/')[-1]
    try:
        searcher.rate_limit()
        resp = searcher.session.get(_API_URL, params={
            'action': 'query', 'format': 'json',
            'titles': f'File:{image_filename}',
            'prop': 'imageinfo', 'iiprop': 'extmetadata|size|url',
        }, timeout=10)
        resp.raise_for_status()
        pages = resp.json().get('query', {}).get('pages', {})
        if not pages:
            return out
        file_page = next(iter(pages.values()))
        info = (file_page.get('imageinfo') or [{}])[0]
        if info.get('url'):
            out['url'] = info['url']
        meta = info.get('extmetadata', {})
        if 'License' in meta:
            out['license_type'] = _normalize_license(meta['License'].get('value'))
        if 'LicenseUrl' in meta:
            out['license_url'] = meta['LicenseUrl'].get('value')
        if 'Artist' in meta:
            out['attribution'] = meta['Artist'].get('value')
        elif 'Credit' in meta:
            out['attribution'] = meta['Credit'].get('value')
        if info.get('width'):
            out['width'] = info['width']
        if info.get('height'):
            out['height'] = info['height']
    except Exception as e:  # noqa: BLE001 - license is best-effort metadata
        logger.debug("Could not fetch image license for %s: %s", image_filename, e)
    return out


def _scrape_infobox_image(searcher, page_title: str, page_url: str) -> Optional[WikipediaImage]:
    """Fallback: scrape the first infobox <img> when pageimages returns none."""
    html = searcher._fetch_wikipedia_page(page_url)
    if not html:
        return None

    soup = BeautifulSoup(html, 'html.parser')
    infobox = soup.find('table', {'class': 'infobox'})
    if not infobox:
        return None
    img_tag = infobox.find('img')
    if not img_tag or not img_tag.get('src'):
        return None

    img_src = _absolute_src(img_tag.get('src'))
    full_img_url = _full_res_from_thumb(img_src)

    lic = _fetch_image_license(searcher, full_img_url)
    return WikipediaImage(
        url=lic['url'],
        thumbnail_url=img_src,
        source_identifier=page_title,
        source_page_url=page_url,
        license_type=lic['license_type'],
        license_url=lic['license_url'],
        attribution=lic['attribution'],
        width=lic['width'] or img_tag.get('width'),
        height=lic['height'] or img_tag.get('height'),
    )


def fetch_main_image(searcher, wikipedia_url: str) -> Optional[WikipediaImage]:
    """Return the page's lead image (full resolution) with license metadata.

    Tries the MediaWiki pageimages API first, then falls back to scraping the
    infobox. Returns None when the page has no usable image.
    """
    page_title = _page_title_from_url(wikipedia_url)
    if not page_title:
        logger.debug("Could not extract page title from URL: %s", wikipedia_url)
        return None

    try:
        searcher.rate_limit()
        resp = searcher.session.get(_API_URL, params={
            'action': 'query', 'format': 'json', 'titles': page_title,
            'prop': 'pageimages|info', 'piprop': 'original|thumbnail',
            'pithumbsize': 500, 'inprop': 'url',
        }, timeout=10)
        resp.raise_for_status()
        pages = resp.json().get('query', {}).get('pages', {})
    except Exception as e:  # noqa: BLE001 - treat as "no image", caller no-ops
        logger.debug("pageimages lookup failed for %s: %s", page_title, e)
        return None

    if not pages:
        return None
    page = next(iter(pages.values()))
    page_url = page.get('fullurl') or wikipedia_url or (
        f"https://en.wikipedia.org/wiki/{quote(page_title)}"
    )

    if 'original' not in page:
        # No API image — try the infobox scrape fallback.
        return _scrape_infobox_image(searcher, page_title, page_url)

    image_url = page['original']['source']
    thumbnail_url = page.get('thumbnail', {}).get('source')
    lic = _fetch_image_license(searcher, image_url)
    return WikipediaImage(
        url=lic['url'] or image_url,
        thumbnail_url=thumbnail_url,
        source_identifier=page_title,
        source_page_url=page_url,
        license_type=lic['license_type'],
        license_url=lic['license_url'],
        attribution=lic['attribution'],
        width=lic['width'] or page['original'].get('width'),
        height=lic['height'] or page['original'].get('height'),
    )


def _is_content_photo(img_tag, full_url: str, filename: Optional[str]) -> bool:
    """Decide whether a rendered <img> is a real content photograph worth
    harvesting (vs an icon, logo, flag, signature, or media still).

    Cheap, network-free checks only — runs over every <img> on the page.
    """
    if 'upload.wikimedia.org' not in full_url:
        return False
    if not filename:
        return False
    lower = filename.lower()
    if not lower.endswith(_PHOTO_EXTENSIONS):
        return False
    if any(term in lower for term in _CLUTTER_FILENAME_TERMS):
        return False
    # Rendered width filters out inline pictograms; absent width is rare for
    # real content thumbnails, so treat "no width" as too small.
    try:
        width = int(img_tag.get('width'))
    except (TypeError, ValueError):
        return False
    return width >= _MIN_CONTENT_IMAGE_WIDTH


def _gather_content_image_candidates(searcher, wikipedia_url: str) -> list[dict]:
    """Scrape the rendered article HTML for content-photo candidates.

    Returns a list of dicts (filename, full_url, thumb_url, width, height) in
    page order, deduped by canonical filename. Network-free beyond the (cached)
    page fetch — license lookups are left to the caller so we only pay them for
    images we actually keep.
    """
    html = searcher._fetch_wikipedia_page(wikipedia_url)
    if not html:
        return []

    soup = BeautifulSoup(html, 'html.parser')
    content = (soup.find('div', {'id': 'mw-content-text'})
               or soup.find('div', class_='mw-parser-output'))
    if not content:
        return []

    candidates: list[dict] = []
    seen: set[str] = set()
    for img_tag in content.find_all('img'):
        src = img_tag.get('src')
        if not src:
            continue
        thumb_url = _absolute_src(src)
        full_url = _full_res_from_thumb(thumb_url)
        filename = _image_filename(full_url)
        if not _is_content_photo(img_tag, full_url, filename):
            continue
        key = filename.lower()
        if key in seen:
            continue
        seen.add(key)

        def _dim(attr):
            try:
                return int(img_tag.get(attr))
            except (TypeError, ValueError):
                return None

        candidates.append({
            'filename': filename,
            'full_url': full_url,
            'thumb_url': thumb_url,
            'width': _dim('width'),
            'height': _dim('height'),
        })
    return candidates


def fetch_all_images(
    searcher, wikipedia_url: str, *, limit: int = _DEFAULT_IMAGE_LIMIT,
) -> list[WikipediaImage]:
    """Harvest up to `limit` content images from a performer's Wikipedia page.

    The page's canonical lead image (via `fetch_main_image`) comes first, then
    additional body/infobox photographs scraped from the rendered article,
    deduped against the lead and each other by canonical filename. License
    metadata is fetched only for the images we keep.
    """
    page_title = _page_title_from_url(wikipedia_url) or ''
    results: list[WikipediaImage] = []
    seen: set[str] = set()

    lead = fetch_main_image(searcher, wikipedia_url)
    if lead:
        results.append(lead)
        lead_fn = _image_filename(lead.url)
        if lead_fn:
            seen.add(lead_fn.lower())

    for cand in _gather_content_image_candidates(searcher, wikipedia_url):
        if len(results) >= limit:
            break
        key = cand['filename'].lower()
        if key in seen:
            continue
        seen.add(key)
        lic = _fetch_image_license(searcher, cand['full_url'])
        results.append(WikipediaImage(
            url=lic['url'] or cand['full_url'],
            thumbnail_url=cand['thumb_url'],
            source_identifier=page_title or None,
            source_page_url=wikipedia_url,
            license_type=lic['license_type'],
            license_url=lic['license_url'],
            attribution=lic['attribution'],
            width=lic['width'] or cand['width'],
            height=lic['height'] or cand['height'],
        ))

    return results[:limit]


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def fetch_performer_data(
    searcher,
    wikipedia_url: str,
    *,
    want_dates: bool = True,
    want_biography: bool = True,
    want_image: bool = True,
) -> PerformerWikipediaData:
    """Fetch the requested fields from a performer's Wikipedia page.

    `searcher` is a WikipediaSearcher (page fetches honour its disk cache and
    rate limiting). Each `want_*` flag lets the caller skip work for fields
    already present in the DB — e.g. skip date parsing once both dates are
    stored. `want_image` fetches only the page's lead image (the first image);
    additional body/infobox photos are deliberately not harvested, since in
    practice they include enough off-target shots that we keep just the lead.
    Returns a PerformerWikipediaData; fields not requested (or not found) are
    left at their empty default.
    """
    data = PerformerWikipediaData()

    if want_dates or want_biography:
        html = searcher._fetch_wikipedia_page(wikipedia_url)
        if html:
            data.page_fetched = True
            soup = BeautifulSoup(html, 'html.parser')
            if want_dates:
                data.birth_date, data.death_date = extract_birth_death_dates(soup)
            if want_biography:
                data.biography = extract_biography(soup)

    if want_image:
        lead = fetch_main_image(searcher, wikipedia_url)
        data.images = [lead] if lead else []

    return data
