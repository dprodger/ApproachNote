"""
MusicBrainz response parsing.

Pure functions that turn raw MusicBrainz JSON into the dicts our DB layer
expects. No HTTP, no DB — just shape conversion and date-string cleanup.

- parse_mb_date: MB's YYYY / YYYY-MM / YYYY-MM-DD strings (with '??'
  placeholders) → (formatted_date, year, precision).
- extract_recording_date_from_mb: best recording date off an MB recording,
  preferring performer-relation session dates over first-release-date.
- parse_release_data: MB release JSON → our release-row dict (artist credit,
  release date, country, label, format, track counts, etc.).
- log_release_info: small debug-log helper that pairs with parse_release_data
  for dry-run output.
"""

import logging
from typing import Any, Dict, Optional, Tuple

_logger = logging.getLogger(__name__)


def parse_mb_date(date_str: str) -> Tuple[Optional[str], Optional[int], Optional[str]]:
    """
    Parse a MusicBrainz date string (YYYY, YYYY-MM, or YYYY-MM-DD).

    MusicBrainz uses '??' for unknown parts, e.g.:
    - 2013-??-26 (year and day known, month unknown)
    - 2013-05-?? (year and month known, day unknown)

    Returns:
        Tuple of (formatted_date, year, precision)
        - formatted_date: Full date string for DB (YYYY-MM-DD, using 01 for unknown parts)
        - year: Integer year
        - precision: 'day', 'month', or 'year'
    """
    if not date_str:
        return (None, None, None)

    try:
        # Extract parts
        parts = date_str.split('-')
        year_str = parts[0] if len(parts) > 0 else None
        month_str = parts[1] if len(parts) > 1 else None
        day_str = parts[2] if len(parts) > 2 else None

        # Check if year is valid (not ????)
        if not year_str or '?' in year_str:
            return (None, None, None)

        year = int(year_str)

        # Determine precision and build formatted date
        if day_str and '?' not in day_str and month_str and '?' not in month_str:
            # Full date known: YYYY-MM-DD
            return (f"{year:04d}-{month_str}-{day_str}", year, 'day')
        elif month_str and '?' not in month_str:
            # Month known: YYYY-MM
            return (f"{year:04d}-{month_str}-01", year, 'month')
        else:
            # Year only
            return (f"{year:04d}-01-01", year, 'year')

    except (ValueError, TypeError, IndexError):
        pass

    return (None, None, None)


def extract_recording_date_from_mb(mb_recording: Dict[str, Any],
                                    logger: Optional[logging.Logger] = None) -> Dict[str, Any]:
    """
    Extract the best recording date from MusicBrainz recording data.

    Priority:
    1. Performer relation dates (actual session dates when all/most match)
    2. MusicBrainz first-release-date (upper bound)

    Args:
        mb_recording: MusicBrainz recording data dict
        logger: Optional logger for detailed diagnostics

    Returns:
        Dict with keys:
        - recording_date: Formatted date string (YYYY-MM-DD)
        - recording_year: Integer year
        - recording_date_precision: 'day', 'month', or 'year'
        - recording_date_source: 'mb_performer_relation' or 'mb_first_release'
        - mb_first_release_date: Raw first-release-date from MB (for caching)
    """
    log = logger or _logger
    recording_id = mb_recording.get('id', 'unknown')
    recording_title = mb_recording.get('title', 'Unknown')

    result = {
        'recording_date': None,
        'recording_year': None,
        'recording_date_precision': None,
        'recording_date_source': None,
        'mb_first_release_date': None,
    }

    # Cache the first-release-date regardless of what we use
    first_release_date = mb_recording.get('first-release-date')
    if first_release_date:
        result['mb_first_release_date'] = first_release_date

    # Priority 1: Check performer relation dates
    relations = mb_recording.get('relations', [])

    # Count performers with and without dates
    performers_with_dates = []
    performers_without_dates = []

    for rel in relations:
        if rel.get('type') == 'instrument':
            artist_name = rel.get('artist', {}).get('name', 'Unknown')
            if rel.get('begin'):
                performers_with_dates.append({
                    'name': artist_name,
                    'date': rel['begin']
                })
            else:
                performers_without_dates.append(artist_name)

    total_performers = len(performers_with_dates) + len(performers_without_dates)

    if performers_with_dates:
        session_dates = set(p['date'] for p in performers_with_dates)
        session_years = set(d[:4] for d in session_dates if len(d) >= 4)

        # Case 1: All performers with dates have the same date
        if len(session_dates) == 1:
            date_str = session_dates.pop()
            formatted_date, year, precision = parse_mb_date(date_str)

            if formatted_date:
                # Log if some performers lack dates
                if performers_without_dates:
                    log.debug(
                        f"  PARTIAL_SESSION_DATES: {len(performers_with_dates)}/{total_performers} "
                        f"performers have date {date_str} for recording '{recording_title}' "
                        f"[{recording_id}]. Missing: {performers_without_dates[:3]}{'...' if len(performers_without_dates) > 3 else ''}"
                    )

                result['recording_date'] = formatted_date
                result['recording_year'] = year
                result['recording_date_precision'] = precision
                result['recording_date_source'] = 'mb_performer_relation'
                return result

        # Case 2: Multiple dates but all same year - use year only
        elif len(session_years) == 1 and len(session_dates) > 1:
            year = int(session_years.pop())
            log.info(
                f"  MULTI_SESSION_SAME_YEAR: Recording '{recording_title}' [{recording_id}] "
                f"has {len(session_dates)} different dates in {year}: {sorted(session_dates)}"
            )
            result['recording_date'] = f"{year}-01-01"
            result['recording_year'] = year
            result['recording_date_precision'] = 'year'
            result['recording_date_source'] = 'mb_performer_relation'
            return result

        # Case 3: Multiple dates across different years - log and use earliest
        elif len(session_years) > 1:
            log.warning(
                f"  MULTI_YEAR_SESSION_DATES: Recording '{recording_title}' [{recording_id}] "
                f"has dates spanning multiple years: {sorted(session_dates)}. Using earliest."
            )
            date_str = min(session_dates)
            formatted_date, year, precision = parse_mb_date(date_str)

            if formatted_date:
                result['recording_date'] = formatted_date
                result['recording_year'] = year
                result['recording_date_precision'] = precision
                result['recording_date_source'] = 'mb_performer_relation'
                return result

    # Priority 2: Use first-release-date as fallback
    if first_release_date:
        formatted_date, year, precision = parse_mb_date(first_release_date)

        if formatted_date:
            result['recording_date'] = formatted_date
            result['recording_year'] = year
            result['recording_date_precision'] = precision
            result['recording_date_source'] = 'mb_first_release'
            return result

    return result


def parse_release_data(mb_release: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse a MusicBrainz release JSON blob into our releases-row dict.

    Handles MB's messy date formats (YYYY / YYYY-MM / YYYY-MM-DD with '??'
    placeholders), pulls country from release-events or the top-level
    country field, takes the first medium's format and sums across media
    for track/disc counts.
    """
    # Extract artist credit
    artist_credit = ''
    artist_credits = mb_release.get('artist-credit') or []
    for credit in artist_credits:
        if isinstance(credit, dict):
            artist = credit.get('artist', {})
            artist_credit += artist.get('name', '')
            artist_credit += credit.get('joinphrase', '')
        elif isinstance(credit, str):
            artist_credit += credit

    # Extract release date
    # MusicBrainz returns dates in various formats: "2004-05-17", "2004-05", "2004"
    # Also can have unknown parts: "2017-??-29", "2004-??"
    # PostgreSQL DATE type requires full YYYY-MM-DD format with valid values
    release_date_raw = mb_release.get('date', '')
    release_date = None
    release_year = None

    if release_date_raw and len(release_date_raw) >= 4:
        try:
            # Check for unknown date markers (??) - if present, only use year
            if '?' in release_date_raw:
                release_year = int(release_date_raw[:4])
                release_date = f"{release_year}-01-01"
            elif len(release_date_raw) == 4:
                # Year only: "2004" -> "2004-01-01"
                release_year = int(release_date_raw)
                release_date = f"{release_date_raw}-01-01"
            elif len(release_date_raw) == 7:
                # Year-month: "2004-05" -> "2004-05-01"
                release_year = int(release_date_raw[:4])
                release_date = f"{release_date_raw}-01"
            elif len(release_date_raw) >= 10:
                # Full date: "2004-05-17" (may have time component, truncate)
                release_year = int(release_date_raw[:4])
                release_date = release_date_raw[:10]
            else:
                # Unknown format, just extract year if possible
                release_year = int(release_date_raw[:4])
                release_date = None
        except (ValueError, TypeError):
            pass

    # Get country (prefer release-events, fall back to country)
    country = None
    release_events = mb_release.get('release-events') or []
    if release_events:
        area = release_events[0].get('area') or {}  # Handle None explicitly
        country = area.get('iso-3166-1-codes', [None])[0] if area.get('iso-3166-1-codes') else area.get('name')
    if not country:
        country = mb_release.get('country')

    # Get label and catalog number
    label = None
    catalog_number = None
    label_info = mb_release.get('label-info') or []
    if label_info:
        label_entry = label_info[0]
        label_obj = label_entry.get('label') or {}  # Handle None explicitly
        label = label_obj.get('name') if label_obj else None
        catalog_number = label_entry.get('catalog-number')

    # Get format from first medium
    format_name = None
    total_tracks = 0
    total_discs = 0
    media = mb_release.get('media') or []  # Handle None explicitly
    if media:
        format_name = media[0].get('format')
        total_discs = len(media)
        for medium in media:
            total_tracks += medium.get('track-count', 0) or 0  # Handle None

    return {
        'musicbrainz_release_id': mb_release.get('id'),
        'musicbrainz_release_group_id': mb_release.get('release-group', {}).get('id'),
        'title': mb_release.get('title'),
        'artist_credit': artist_credit.strip() or None,
        'disambiguation': mb_release.get('disambiguation') or None,
        'release_date': release_date,  # Already normalized to YYYY-MM-DD or None
        'release_year': release_year,
        'country': country or None,
        'label': label,
        'catalog_number': catalog_number,
        'barcode': mb_release.get('barcode') or None,
        'format_name': format_name,
        'packaging_name': mb_release.get('packaging'),
        'status_name': mb_release.get('status'),
        'language': mb_release.get('text-representation', {}).get('language'),
        'script': mb_release.get('text-representation', {}).get('script'),
        'total_tracks': total_tracks or None,
        'total_discs': total_discs or None,
        'data_quality': mb_release.get('quality'),
    }


def log_release_info(release_data: Dict[str, Any], log: logging.Logger) -> None:
    """Dry-run debug dump of a parsed release dict."""
    log.info(f"    [DRY RUN] Release details:")
    log.info(f"      Title: {release_data['title']}")
    log.info(f"      Artist: {release_data.get('artist_credit', 'Unknown')}")
    log.info(f"      Year: {release_data.get('release_year', 'Unknown')}")
    log.info(f"      Country: {release_data.get('country', 'Unknown')}")
    log.info(f"      Format: {release_data.get('format_name', 'Unknown')}")
    log.info(f"      Label: {release_data.get('label', 'Unknown')}")
