"""
Spotify Matching Utilities

Text normalization, fuzzy matching, and validation logic for matching
our database records to Spotify API results.

Functions in this module are stateless and can be used independently.
"""

import re
import logging
from typing import List, Optional
from rapidfuzz import fuzz

logger = logging.getLogger(__name__)


# Common jazz ensemble suffixes that may not appear in Spotify artist names
# e.g., "Bill Evans Trio" in our DB might be just "Bill Evans" on Spotify
ENSEMBLE_SUFFIXES = [
    'Trio', 'Quartet', 'Quintet', 'Sextet', 'Septet', 'Octet', 'Nonet',
    'Orchestra', 'Big Band', 'Band', 'Ensemble', 'Group'
]

# Common first name nicknames/variants - map to canonical form
# This handles cases like "Dave Liebman" vs "David Liebman"
NAME_VARIANTS = {
    # David variants
    'dave': 'david',
    'davey': 'david',
    'davy': 'david',
    # William variants
    'bill': 'william',
    'billy': 'william',
    'will': 'william',
    'willy': 'william',
    'willie': 'william',
    # Robert variants
    'bob': 'robert',
    'bobby': 'robert',
    'rob': 'robert',
    'robbie': 'robert',
    # Richard variants
    'dick': 'richard',
    'rick': 'richard',
    'ricky': 'richard',
    'richie': 'richard',
    # James variants
    'jim': 'james',
    'jimmy': 'james',
    'jamie': 'james',
    # Thomas variants
    'tom': 'thomas',
    'tommy': 'thomas',
    # Charles variants
    'charlie': 'charles',
    'chuck': 'charles',
    'chas': 'charles',
    # Edward variants
    'ed': 'edward',
    'eddie': 'edward',
    'ted': 'edward',
    'teddy': 'edward',
    # Michael variants
    'mike': 'michael',
    'mikey': 'michael',
    'mick': 'michael',
    # Joseph variants
    'joe': 'joseph',
    'joey': 'joseph',
    # Anthony variants
    'tony': 'anthony',
    # Benjamin variants
    'ben': 'benjamin',
    'benny': 'benjamin',
    # Daniel variants
    'dan': 'daniel',
    'danny': 'daniel',
    # Donald variants
    'don': 'donald',
    'donnie': 'donald',
    # Gerald variants
    'gerry': 'gerald',
    'jerry': 'gerald',
    # Kenneth variants
    'ken': 'kenneth',
    'kenny': 'kenneth',
    # Lawrence variants
    'larry': 'lawrence',
    # Matthew variants
    'matt': 'matthew',
    # Nicholas variants
    'nick': 'nicholas',
    'nicky': 'nicholas',
    # Patrick variants
    'pat': 'patrick',
    'paddy': 'patrick',
    # Peter variants
    'pete': 'peter',
    # Philip variants
    'phil': 'philip',
    # Raymond variants
    'ray': 'raymond',
    # Ronald variants
    'ron': 'ronald',
    'ronnie': 'ronald',
    # Samuel variants
    'sam': 'samuel',
    'sammy': 'samuel',
    # Stephen/Steven variants
    'steve': 'steven',
    'stevie': 'steven',
    # Theodore variants
    'theo': 'theodore',
    # Timothy variants
    'tim': 'timothy',
    'timmy': 'timothy',
    # Walter variants
    'walt': 'walter',
    'wally': 'walter',
    # Alexander variants
    'alex': 'alexander',
    # Frederick variants
    'fred': 'frederick',
    'freddy': 'frederick',
    'freddie': 'frederick',
    # Harold variants
    'hal': 'harold',
    'harry': 'harold',
    # Leonard variants
    'len': 'leonard',
    'lenny': 'leonard',
    # Nathaniel variants
    'nat': 'nathaniel',
    'nate': 'nathaniel',
}

# Artist names that indicate a compilation rather than a specific artist
# For these, we allow lenient track verification since artist matching is meaningless
# Includes common translations from Apple Music catalogs
COMPILATION_ARTIST_PATTERNS = [
    'various artists',
    'various',
    'va',
    'multiple artists',
    'compilation',
    'assorted artists',
    'diverse artists',
    # Translations found in Apple Music catalog
    '群星',                    # Chinese
    'varios artistas',        # Spanish
    'vários artistas',        # Portuguese
    'artistes variés',        # French
    'artistes divers',        # French alt
    'verschiedene interpreten',  # German
    'artisti vari',           # Italian
    'さまざまなアーティスト',      # Japanese
    '여러 아티스트',            # Korean
]


def is_compilation_artist(artist_name: str) -> bool:
    """
    Check if an artist name indicates a compilation/various artists release.

    Args:
        artist_name: The artist name to check

    Returns:
        True if the artist name suggests a compilation
    """
    if not artist_name:
        return False

    normalized = artist_name.lower().strip()
    return normalized in COMPILATION_ARTIST_PATTERNS


# Annotations on a track title that signal a recording-level *version*
# difference — not just remaster / stereo-mix / "from [show]" cosmetic
# noise. If one side of a candidate match has any of these and the
# other side has none, the two sides are recordings of the same SONG
# but different PERFORMANCES, and the matcher should not link them
# even when title-strip-rescue lifts the score to 100%.
#
# Each entry is a substring matched case-insensitively after
# normalize_for_comparison, with word boundaries enforced where it
# matters (e.g. avoid matching "live" inside "olive" — see
# has_version_keyword for the regex).
_VERSION_KEYWORDS = (
    'live',
    'demo',
    'alternate',
    'alt take',
    'rehearsal',
    'instrumental',
    'acoustic',
    'unplugged',
    'session',
)

_VERSION_KEYWORD_RE = re.compile(
    r'\b(?:' + '|'.join(re.escape(kw) for kw in _VERSION_KEYWORDS) + r')\b',
    re.IGNORECASE,
)


def has_version_keyword(title: str) -> bool:
    """True when `title` contains a recording-level version annotation
    (live, demo, alternate take, instrumental, acoustic, etc.).

    Used by match_tracks_for_release to reject asymmetric matches —
    e.g. an MB recording titled "Peace (live at Newport)" matched to a
    Spotify track titled just "Peace" — because the title-strip rescue
    in calculate_similarity will happily lift such pairs to 100% even
    though they're different recordings.

    Word-boundary matching avoids false positives on substrings like
    "olive" / "alive" containing "live".
    """
    if not title:
        return False
    return bool(_VERSION_KEYWORD_RE.search(title))


def track_artist_matches_recording_leader(
    recording_leader: str,
    track_artists: list,
    min_similarity: int = 50,
) -> tuple[bool, float]:
    """Decide whether a Spotify track's artists are plausibly the same as
    a recording's leader performer.

    Used by match_tracks_for_release on compilation albums (Various
    Artists) where the album-level artist check carries no signal —
    multiple Spotify compilations can share an album title yet credit
    completely different artists for the same track title (e.g. two
    "Watermelon Man"s on two "The Very Best of Latin Jazz" compilations,
    one by Mongo Santamaría and one by Recife All Stars).

    Match rules — accept if any of:
      - either name (normalized) is a substring of the other (handles
        "Mongo Santamaría" vs "Mongo Santamaría & His Orchestra"-style
        variations)
      - fuzz similarity to any track artist meets `min_similarity`

    Returns (matches, best_similarity). When no recording_leader or no
    track_artists are available, returns (True, 100.0) — missing data
    is not grounds for rejection.
    """
    if not recording_leader or not track_artists:
        return True, 100.0

    leader_norm = normalize_for_comparison(recording_leader)
    if not leader_norm:
        return True, 100.0

    best_sim = 0.0
    for ta in track_artists:
        if not ta:
            continue
        ta_norm = normalize_for_comparison(ta)
        if not ta_norm:
            continue
        if leader_norm in ta_norm or ta_norm in leader_norm:
            return True, 100.0
        sim = calculate_similarity(recording_leader, ta)
        if sim > best_sim:
            best_sim = sim

    return best_sim >= min_similarity, best_sim


def strip_ensemble_suffix(artist_name: str) -> str:
    """
    Strip common ensemble suffixes from artist names.

    Examples:
        "Lynne Arriale Trio" -> "Lynne Arriale"
        "Bill Evans Trio" -> "Bill Evans"
        "Duke Ellington Orchestra" -> "Duke Ellington"
        "Miles Davis" -> "Miles Davis" (unchanged)

    Returns:
        Artist name with suffix stripped, or original if no suffix found
    """
    if not artist_name:
        return artist_name

    for suffix in ENSEMBLE_SUFFIXES:
        # Check for suffix at end of string (case-insensitive)
        pattern = rf'\s+{re.escape(suffix)}$'
        if re.search(pattern, artist_name, re.IGNORECASE):
            return re.sub(pattern, '', artist_name, flags=re.IGNORECASE).strip()

    return artist_name


def normalize_name_variants(text: str) -> str:
    """
    Normalize common first name nicknames/variants to their canonical form.

    This handles cases like "Dave Liebman" -> "David Liebman" to improve
    artist matching when the same person uses different name forms.

    Args:
        text: Text that may contain name variants

    Returns:
        Text with common nickname variants normalized
    """
    if not text:
        return text

    words = text.lower().split()
    normalized_words = []

    for word in words:
        # Check if this word is a known nickname
        if word in NAME_VARIANTS:
            normalized_words.append(NAME_VARIANTS[word])
        else:
            normalized_words.append(word)

    return ' '.join(normalized_words)


# Common album title suffixes that may differ between MusicBrainz and Spotify
# These are stripped for search queries to improve matching
ALBUM_LIVE_SUFFIXES = [
    r'\s*:\s*live$',      # "Solo: Live" -> "Solo"
    r'\s*-\s*live$',      # "Album - Live" -> "Album"
    r'\s*\(live\)$',      # "Album (Live)" -> "Album"
]


def normalize_for_search(text: str) -> str:
    """
    Normalize text for use in search queries.

    This is lighter than normalize_for_comparison - it only standardizes
    characters that might cause search mismatches without altering the
    semantic content.

    Examples:
        "New Faces – New Sounds" -> "New Faces - New Sounds"
        "Köln Concert" -> "Koln Concert" (if unidecode available)
    """
    if not text:
        return text

    # Normalize various dash characters to regular hyphen
    text = text.replace('–', '-')  # en-dash
    text = text.replace('—', '-')  # em-dash
    text = text.replace('‐', '-')  # Unicode hyphen
    text = text.replace('−', '-')  # minus sign

    # Normalize quotes
    text = text.replace('"', '"').replace('"', '"')
    text = text.replace(''', "'").replace(''', "'")

    return text


def strip_live_suffix(album_title: str) -> str:
    """
    Strip common live recording suffixes from album titles for search queries.

    MusicBrainz often includes ": Live" or "- Live" in album titles, but Spotify
    may have the album without this suffix.

    Examples:
        "Solo: Live" -> "Solo"
        "At The Philharmonic - Live" -> "At The Philharmonic"
        "Concert (Live)" -> "Concert"
        "Night Train" -> "Night Train" (unchanged)

    Returns:
        Album title with live suffix stripped, or original if no suffix found
    """
    if not album_title:
        return album_title

    for pattern in ALBUM_LIVE_SUFFIXES:
        if re.search(pattern, album_title, re.IGNORECASE):
            return re.sub(pattern, '', album_title, flags=re.IGNORECASE).strip()

    return album_title


# MusicBrainz frequently disambiguates compilation series by appending the
# year (or year range) of the source recordings inside tilde-delimited
# pads — e.g. "It's Up to You ~ 1946 ~ Volume 2" or
# "The Chronological Classics: Lunceford ~ 1937–1939 ~". Spotify carries the
# same content under the bare title (or a slightly different volume marker),
# which means our literal `album:"..."` queries miss every time.
#
# The regex anchors at end-of-string and requires the full
# whitespace+tilde+year+optional-tilde envelope so a year that happens to
# appear elsewhere in a title (e.g. Prince's "1999") is left intact. An
# optional second-tilde plus an optional trailing chunk (volume marker) are
# eaten too; the separator before the year is required to be a literal tilde
# so we don't strip parenthesized years like "(2001)" — those usually ARE
# load-bearing.
_MB_YEAR_DISAMBIGUATOR_RE = re.compile(
    r'\s*~\s*\d{4}(?:\s*[-–]\s*\d{2,4})?\s*(?:~\s*.*)?$',
    re.UNICODE,
)


def strip_mb_year_disambiguator(album_title: str) -> str:
    """
    Strip the MusicBrainz-style ``~ YYYY ~`` (or ``~ YYYY–YYYY ~ Vol N``)
    disambiguator suffix from an album title for search queries.

    Used the same way as ``strip_live_suffix``: applied only to the search
    query, never to the validation comparison. A wrong candidate still has
    to clear the standard album/artist similarity thresholds against the
    full original title.

    Examples:
        "It's Up to You ~ 1946 ~ Volume 2" -> "It's Up to You"
        "The Chronological Classics ~ 1937–1939 ~" -> "The Chronological Classics"
        "Live in Paris ~ 1958 ~"            -> "Live in Paris"
        "Songs in A Minor (2001)"           -> "Songs in A Minor (2001)"   (untouched)
        "1999"                              -> "1999"                       (untouched)
        "Greatest Hits"                     -> "Greatest Hits"              (untouched)

    Returns:
        Album title with the MB year-disambiguator stripped, or the original
        if no such suffix is present.
    """
    if not album_title:
        return album_title

    return _MB_YEAR_DISAMBIGUATOR_RE.sub('', album_title).strip()


def normalize_for_comparison(text: str) -> str:
    """
    Normalize text for fuzzy comparison
    Removes common variations that shouldn't affect matching
    """
    if not text:
        return ""
    
    text = text.lower()

    # Replace apostrophes with spaces
    # Handles: "Don'cha" vs "Don Cha", "'Way" vs "Way", etc.
    # Using space instead of removal so "don'cha" → "don cha" matches Spotify's "Don Cha"
    text = text.replace("'", " ")     # U+0027 Standard apostrophe
    text = text.replace("\u2019", " ") # U+2019 Right single quote (curly apostrophe)
    text = text.replace("\u2018", " ") # U+2018 Left single quote
    text = text.replace("`", " ")     # Backtick

    # Normalize curly double quotes to straight quotes (then remove below)
    text = text.replace('"', '"')  # U+201C Left double quote
    text = text.replace('"', '"')  # U+201D Right double quote
    # Remove double quotes entirely (they add noise to comparisons)
    text = text.replace('"', '')

    # Remove live recording annotations
    text = re.sub(r'\s*-\s*live\s+(at|in|from)\s+.*$', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\s*\(live\s+(at|in|from)\s+[^)]*\).*$', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\s*-\s*live$', '', text, flags=re.IGNORECASE)  # Simple "- Live" suffix
    text = re.sub(r'\s*\(live\)$', '', text, flags=re.IGNORECASE)  # Simple "(Live)" suffix
    text = re.sub(r'\s*:\s*live$', '', text, flags=re.IGNORECASE)  # Simple ": Live" suffix (e.g., "Solo: Live")
    
    # Remove recorded at annotations
    text = re.sub(r'\s*-\s*recorded\s+(at|in)\s+.*$', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\s*\(recorded\s+(at|in)\s+[^)]*\).*$', '', text, flags=re.IGNORECASE)
    
    # Remove remastered annotations (various formats)
    # "- Remastered", "- Remastered 2025", "- 2025 Remaster", "(Remastered)", etc.
    text = re.sub(r'\s*-\s*remaster(ed)?(\s+\d{4})?.*$', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\s*-\s*\d{4}\s+remaster(ed)?.*$', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\s*\(remaster(ed)?(\s+\d{4})?\).*$', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\s*\(\d{4}\s+remaster(ed)?\).*$', '', text, flags=re.IGNORECASE)
    # Handle "- Instrumental/Remastered" and similar compound suffixes
    text = re.sub(r'\s*-\s*instrumental(/remaster(ed)?)?.*$', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\s*\(instrumental(/remaster(ed)?)?\).*$', '', text, flags=re.IGNORECASE)

    # Remove featured artist annotations (common in streaming services)
    # Handles: (feat. Artist), (featuring Artist), (ft. Artist), (with Artist)
    text = re.sub(r'\s*\(feat\.?\s+[^)]+\)', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\s*\(featuring\s+[^)]+\)', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\s*\(ft\.?\s+[^)]+\)', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\s*\(with\s+[^)]+\)', '', text, flags=re.IGNORECASE)
    # Also handle dash variants: - feat. Artist, - featuring Artist
    text = re.sub(r'\s*-\s*feat\.?\s+.*$', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\s*-\s*featuring\s+.*$', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\s*-\s*ft\.?\s+.*$', '', text, flags=re.IGNORECASE)

    # Remove film/show/musical source annotations (common in streaming services)
    # Handles: "- From the 20th Century-Fox Film, ..." or "(From the Broadway Musical...)"
    # These annotations indicate the source but shouldn't affect matching
    text = re.sub(r'\s*-\s*from\s+(the\s+)?([\w\s\-\.]+\s+)?(film|movie|musical|show|motion picture|broadway|soundtrack|production).*$', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\s*\(from\s+(the\s+)?([\w\s\-\.]+\s+)?(film|movie|musical|show|motion picture|broadway|soundtrack|production)[^)]*\)', '', text, flags=re.IGNORECASE)

    # Remove date/venue at end
    text = re.sub(r'\s*/\s+[a-z]+\s+\d+.*$', '', text, flags=re.IGNORECASE)
    
    # Remove tempo/arrangement annotations (common in jazz)
    text = re.sub(r'\s*-\s*(slow|fast|up tempo|medium|ballad)(\s+version)?.*$', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\s*\((slow|fast|up tempo|medium|ballad)(\s+version)?\).*$', '', text, flags=re.IGNORECASE)
    
    # Remove take numbers and alternate versions
    text = re.sub(r'\s*-\s*(take|version|alternate|alt\.?)\s*\d*.*$', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\s*\((take|version|alternate|alt\.?)\s*\d*\).*$', '', text, flags=re.IGNORECASE)
    
    # Remove ensemble suffixes
    text = text.replace(' trio', '')
    text = text.replace(' quartet', '')
    text = text.replace(' quintet', '')
    text = text.replace(' sextet', '')
    text = text.replace(' orchestra', '')
    text = text.replace(' band', '')
    text = text.replace(' ensemble', '')

    # Normalize volume / part markers so MB's verbose form matches Spotify's
    # abbreviated form (and vice versa). The trailing digit is required so we
    # never collapse "Volume" / "Part" when they're load-bearing nouns
    # ("Turn the Volume Up", "Part of Me"). Both tokens stay distinct from
    # each other — "vol 2" and "pt 2" don't merge, only their abbreviations.
    #   "volume 2", "vol. 2", "Vol.2"  -> "vol 2"
    #   "part 2",   "pt. 2",  "Pt.2"   -> "pt 2"
    text = re.sub(r'\b(?:volume|vol)\.?\s*(\d+)', r'vol \1', text)
    text = re.sub(r'\b(?:part|pt)\.?\s*(\d+)', r'pt \1', text)

    # Normalize "and" vs "&"
    text = text.replace(' & ', ' and ')

    # Normalize slashes to spaces (e.g., "Strasbourg/St. Denis" → "Strasbourg St. Denis")
    # This handles title variations where "/" is used as a separator
    text = re.sub(r'\s*/\s*', ' ', text)
    text = text.replace('/', ' ')

    # Normalize various dash characters to regular dash
    # en-dash (–), em-dash (—), and other Unicode dashes → regular dash (-)
    text = text.replace('–', '-')  # en-dash
    text = text.replace('—', '-')  # em-dash
    text = text.replace('‐', '-')  # Unicode hyphen
    text = text.replace('−', '-')  # minus sign

    # Normalize spacing around dashes (e.g., "St. - Denis" → "St.-Denis")
    text = re.sub(r'\s*-\s*', '-', text)
    
    # Remove extra whitespace
    text = ' '.join(text.split())
    
    return text


def calculate_similarity(text1: str, text2: str) -> float:
    """
    Calculate similarity between two strings using fuzzy matching.
    
    Handles common variations like parenthetical additions:
    - "Who Cares?" vs "Who Cares (As Long As You Care For Me)"
    - "Stella By Starlight" vs "Stella By Starlight (From 'The Uninvited')"
    
    Returns a score from 0-100
    """
    if not text1 or not text2:
        return 0
    
    norm1 = normalize_for_comparison(text1)
    norm2 = normalize_for_comparison(text2)
    
    # Primary comparison using token_sort_ratio
    score = fuzz.token_sort_ratio(norm1, norm2)
    
    # If score is below threshold, try comparing without parenthetical
    # AND bracketed content. Brackets carry the same kind of annotation
    # as parens — "[Live]", "[Remastered]", "[Ampico Piano Roll Recording]" —
    # and need to be stripped for the rescue to fire on titles that mix
    # both styles ("My Heart Stood Still (From 'A Connecticut Yankee')
    # [Ampico Piano Roll Recording]" was getting filtered out of the
    # candidate pool because only the parens were stripped).
    if score < 80:
        def _strip_annotations(s):
            s = re.sub(r'\s*\([^)]*\)\s*', ' ', s)
            s = re.sub(r'\s*\[[^\]]*\]\s*', ' ', s)
            return ' '.join(s.split()).strip()

        stripped1 = _strip_annotations(norm1)
        stripped2 = _strip_annotations(norm2)

        # Only use stripped comparison if something was actually removed
        if stripped1 != norm1 or stripped2 != norm2:
            stripped_score = fuzz.token_sort_ratio(stripped1, stripped2)
            if stripped_score > score:
                logger.debug(f"      Parenthetical fallback: {score}% → {stripped_score}%")
                score = stripped_score
    
    return score


def split_title_qualifier(title: str) -> tuple:
    """Split a track/song title into (base, qualifier).

    A qualifier is a trailing annotation that disambiguates a variant of
    the same underlying tune — e.g. "(Take 2)", "[Live]", or
    " - From the 1957 Riverside Sessions". Three syntaxes are accepted:

        "Foo (qualifier)"   -> ("Foo", "qualifier")
        "Foo [qualifier]"   -> ("Foo", "qualifier")
        "Foo - qualifier"   -> ("Foo", "qualifier")  (whitespace around the
                                                       dash is required, so
                                                       hyphenated names like
                                                       "Saint-Saëns" survive)

    Returns (title, None) when no qualifier is present. Strings come back
    NOT yet normalized — caller should pass each through
    normalize_for_comparison() before comparing.
    """
    if not title:
        return ('', None)
    t = title.strip()

    m = re.search(r'\s*\(([^)]+)\)\s*$', t)
    if m:
        return (t[:m.start()].rstrip(), m.group(1).strip())

    m = re.search(r'\s*\[([^\]]+)\]\s*$', t)
    if m:
        return (t[:m.start()].rstrip(), m.group(1).strip())

    # Whitespace REQUIRED on both sides so we don't split "Saint-Saëns" or
    # "Tin Pan Alley"-style names.
    m = re.search(r'\s+-\s+(.+)$', t)
    if m:
        return (t[:m.start()].rstrip(), m.group(1).strip())

    return (t, None)


def _normalize_for_structural_match(s: str) -> str:
    """Stricter-than-`normalize_for_comparison` normalization for the
    structural title match. Strips ornamental punctuation (commas,
    semicolons, colons) that doesn't change meaning but DOES break exact
    string equality — real case: MB "Well, You Needn't" vs Spotify
    "Well You Needn't" only differ by a comma.

    Kept private because it's tuned for structural-match comparisons,
    not the broader fuzzy-similarity scoring.
    """
    s = normalize_for_comparison(s)
    s = re.sub(r'[,;:]', ' ', s)
    return ' '.join(s.split())


def is_structural_title_match(t1: str, t2: str) -> bool:
    """True when two titles share the same base AND same qualifier (or
    both lack a qualifier), regardless of which annotation syntax is used.

        "Take Five (Live)" matches "Take Five - Live"
        "Well You Needn't (opening)" matches "Well You Needn't - Opening"

    But:
        "Take Five (Live)" does NOT match "Take Five (Studio)"
        "Take Five (Live)" does NOT match "Take Five" — having vs missing a
            qualifier makes them different recordings, not the same one.

    This is the cross-syntax equivalent of "exact normalized match" — used
    by match_track_to_recording to break ties between Spotify candidates
    that all score 100% via different paths in calculate_similarity.
    """
    base1, qual1 = split_title_qualifier(t1)
    base2, qual2 = split_title_qualifier(t2)
    if _normalize_for_structural_match(base1) != _normalize_for_structural_match(base2):
        return False
    if qual1 is None and qual2 is None:
        return True
    if qual1 is None or qual2 is None:
        return False
    return _normalize_for_structural_match(qual1) == _normalize_for_structural_match(qual2)


def is_substring_title_match(title1: str, title2: str) -> bool:
    """
    Check if one normalized title is a complete substring of the other.
    
    This is a fallback matching strategy used when track positions match
    but fuzzy matching doesn't meet the threshold. This handles cases like:
    - "An Affair to Remember" vs "An Affair to Remember - From the 20th Century-Fox Film"
    - "Stella By Starlight" vs "Stella By Starlight (From 'The Uninvited')"
    
    To minimize false positives, we require:
    - The shorter title is at least 4 characters
    - The shorter title appears as a complete substring in the longer one
    
    Args:
        title1: First title
        title2: Second title
        
    Returns:
        True if one title is fully contained in the other
    """
    if not title1 or not title2:
        return False
    
    norm1 = normalize_for_comparison(title1)
    norm2 = normalize_for_comparison(title2)
    
    # Determine shorter and longer
    shorter = norm1 if len(norm1) <= len(norm2) else norm2
    longer = norm2 if len(norm1) <= len(norm2) else norm1
    
    # Require minimum length to avoid false positives with very short titles
    if len(shorter) < 4:
        return False
    
    # Check if shorter is a complete substring of longer
    return shorter in longer


def extract_primary_artist(artist_credit: str) -> str:
    """
    Extract the primary artist from a MusicBrainz artist_credit string.
    
    MusicBrainz artist_credit can contain multiple artists joined by various
    separators (', ', '; ', '/', ' & '). For Spotify searches, we typically
    only need the primary (first) artist to get a good match.
    
    This prevents issues with long artist strings like:
    "Dave Brubeck, Claude Debussy, João Donato/João Gilberto, Bill Evans..."
    
    Args:
        artist_credit: Full artist credit string from MusicBrainz
        
    Returns:
        Primary artist name (first artist in the credit)
    """
    if not artist_credit:
        return None
    
    # Common separators in MusicBrainz artist credits
    # Order matters - check multi-char separators first
    separators = [', ', '; ', ' / ', '/', ' & ']
    
    result = artist_credit
    for sep in separators:
        if sep in result:
            result = result.split(sep)[0]
            break
    
    return result.strip() if result else None


def validate_track_match(spotify_track: dict, expected_song: str, 
                         expected_artist: str, expected_album: str,
                         min_track_similarity: int, min_artist_similarity: int,
                         min_album_similarity: int) -> tuple:
    """
    Validate that a Spotify track result actually matches what we're looking for
    
    Args:
        spotify_track: Track dict from Spotify API
        expected_song: Song title we're searching for
        expected_artist: Artist name we're searching for
        expected_album: Album title we're searching for (can be None)
        min_track_similarity: Minimum track title similarity threshold
        min_artist_similarity: Minimum artist similarity threshold
        min_album_similarity: Minimum album similarity threshold
        
    Returns:
        tuple: (is_valid, reason, scores_dict)
    """
    # Extract Spotify track info
    spotify_song = spotify_track['name']
    spotify_artist_list = [a['name'] for a in spotify_track['artists']]
    spotify_artists = ', '.join(spotify_artist_list)
    spotify_album = spotify_track['album']['name']
    
    # Calculate track title similarity
    song_similarity = calculate_similarity(expected_song, spotify_song)
    
    # Debug: Show normalized versions if similarity is surprisingly low
    if song_similarity < 70:
        norm_expected = normalize_for_comparison(expected_song)
        norm_spotify = normalize_for_comparison(spotify_song)
        if norm_expected != expected_song.lower() or norm_spotify != spotify_song.lower():
            logger.debug(f"       [Normalization] Expected: '{expected_song}' → '{norm_expected}'")
            logger.debug(f"       [Normalization] Spotify:  '{spotify_song}' → '{norm_spotify}'")
    
    # Calculate artist similarity - handle multi-artist tracks
    individual_artist_scores = [
        calculate_similarity(expected_artist, spotify_artist)
        for spotify_artist in spotify_artist_list
    ]
    best_individual_match = max(individual_artist_scores) if individual_artist_scores else 0
    
    full_artist_similarity = calculate_similarity(expected_artist, spotify_artists)
    
    artist_similarity = max(best_individual_match, full_artist_similarity)
    
    # Calculate album similarity
    album_similarity = calculate_similarity(expected_album, spotify_album) if expected_album else None
    
    scores = {
        'song': song_similarity,
        'artist': artist_similarity,
        'artist_best_individual': best_individual_match,
        'artist_full_string': full_artist_similarity,
        'album': album_similarity,
        'spotify_song': spotify_song,
        'spotify_artist': spotify_artists,
        'spotify_album': spotify_album
    }
    
    # Validation logic
    if song_similarity < min_track_similarity:
        return False, f"Track title similarity too low ({song_similarity}% < {min_track_similarity}%)", scores
    
    if artist_similarity < min_artist_similarity:
        return False, f"Artist similarity too low ({artist_similarity}% < {min_artist_similarity}%)", scores
    
    if expected_album and album_similarity and album_similarity < min_album_similarity:
        return False, f"Album similarity too low ({album_similarity}% < {min_album_similarity}%)", scores
    
    # Passed all validation checks
    return True, "Valid match", scores


def validate_album_match(spotify_album: dict, expected_album: str,
                         expected_artist: str, min_album_similarity: int,
                         min_artist_similarity: int,
                         song_title: str = None,
                         verify_track_callback=None,
                         verify_tracklist_callback=None) -> tuple:
    """
    Validate that a Spotify album result actually matches what we're looking for

    Args:
        spotify_album: Spotify album dict from search results
        expected_album: Album title we're searching for
        expected_artist: Artist name we're searching for
        min_album_similarity: Minimum album similarity threshold
        min_artist_similarity: Minimum artist similarity threshold
        song_title: Optional song title for track verification fallback.
                   When album similarity is high (>=80%) but artist fails,
                   we can still accept the match if the album contains
                   a track matching this title.
        verify_track_callback: Optional callback function(album_id, song_title) -> bool
                              for verifying track presence
        verify_tracklist_callback: Optional callback function(album_id) -> bool|None
                              that compares the full MB tracklist against
                              this Spotify album's tracklist. Consulted only
                              when artist similarity is below threshold —
                              the case where the substring/track-presence
                              fallbacks below tend to admit compilations
                              that share an album title and one track but
                              are otherwise unrelated (issue #184). True
                              forces accept, False forces reject, None
                              falls through to the existing fallback logic.

    Returns:
        tuple: (is_valid, reason, scores_dict)
    """
    spotify_album_name = spotify_album['name']
    spotify_artist_list = [a['name'] for a in spotify_album['artists']]
    spotify_artists = ', '.join(spotify_artist_list)
    
    # Calculate album similarity
    album_similarity = calculate_similarity(expected_album, spotify_album_name)
    
    # Check for substring containment (e.g., "Live at Montreux" in "Live At The Montreux Jazz Festival")
    # This is a strong signal even if fuzzy similarity is below threshold
    # Strip articles (the, a, an) for more flexible matching
    def strip_articles(text):
        return re.sub(r'\b(the|a|an)\b', '', text, flags=re.IGNORECASE).strip()
    
    normalized_expected = strip_articles(normalize_for_comparison(expected_album))
    normalized_spotify = strip_articles(normalize_for_comparison(spotify_album_name))
    # Also remove extra spaces that may result from stripping articles
    normalized_expected = ' '.join(normalized_expected.split())
    normalized_spotify = ' '.join(normalized_spotify.split())
    
    album_is_substring = (
        normalized_expected in normalized_spotify or 
        normalized_spotify in normalized_expected
    )
    
    # Calculate artist similarity
    individual_artist_scores = [
        calculate_similarity(expected_artist, spotify_artist)
        for spotify_artist in spotify_artist_list
    ]
    best_individual_match = max(individual_artist_scores) if individual_artist_scores else 0
    full_artist_similarity = calculate_similarity(expected_artist, spotify_artists)
    artist_similarity = max(best_individual_match, full_artist_similarity)
    
    # Check for artist substring containment (e.g., "Lynne Arriale" in "Lynne Arriale Trio")
    normalized_expected_artist = normalize_for_comparison(expected_artist)
    artist_is_substring = any(
        normalized_expected_artist in normalize_for_comparison(sa) or
        normalize_for_comparison(sa) in normalized_expected_artist
        for sa in spotify_artist_list
    )
    
    scores = {
        'album': album_similarity,
        'album_is_substring': album_is_substring,
        'artist': artist_similarity,
        'artist_is_substring': artist_is_substring,
        'artist_best_individual': best_individual_match,
        'artist_full_string': full_artist_similarity,
        'spotify_album': spotify_album_name,
        'spotify_artist': spotify_artists
    }
    
    # Validation logic
    # Accept if: fuzzy similarity meets threshold OR album title is a substring (with reasonable similarity)
    album_valid = (
        album_similarity >= min_album_similarity or
        (album_is_substring and album_similarity >= 50)  # Substring match with at least 50% similarity
    )

    # Special case: Spotify sometimes prepends artist name to album title, e.g.:
    # "Ryan Porter (Live at New Morning, Paris)" where our album is "Live at New Morning, Paris"
    # If the expected album is contained in Spotify album and the extra text is the artist name, accept it
    # NOTE: We use raw lowercased names here, not normalized ones, because normalize_for_comparison
    # strips out "(Live at ...)" annotations which we need to preserve for this check
    if not album_valid and expected_artist:
        raw_expected = expected_album.lower().strip()
        raw_spotify = spotify_album_name.lower().strip()

        # Check if expected album is contained in Spotify album name
        if raw_expected in raw_spotify:
            # Check if Spotify album is "Artist (Album)" or "Artist - Album" pattern
            extra_text = raw_spotify.replace(raw_expected, '').strip()
            # Remove common separators
            extra_text = extra_text.strip('()-–—:').strip()

            if extra_text:
                raw_expected_artist = expected_artist.lower().strip()
                extra_similarity = calculate_similarity(extra_text, raw_expected_artist)
                if extra_similarity >= 75:
                    album_valid = True
                    logger.debug(f"      Album accepted: Spotify prepended artist name to album title ({extra_similarity}% match)")
    
    if not album_valid:
        return False, f"Album similarity too low ({album_similarity}% < {min_album_similarity}%)", scores
    
    if album_is_substring and album_similarity < min_album_similarity:
        logger.debug(f"      Album accepted via substring containment ({album_similarity}%)")
    
    if expected_artist and artist_similarity < min_artist_similarity:
        # Tracklist verification first — when the artist signal is weak,
        # the substring/track-presence fallbacks below will happily admit
        # a compilation that shares the album title and contains the song
        # but is otherwise unrelated (issue #184: MB "Djangology" by Django
        # Reinhardt & Stéphane Grappelli matched to a Spotify compilation
        # by Django Reinhardt + Quintette du Hot Club de France because
        # one credited Spotify artist is a substring of the MB credit).
        # Comparing the full MB tracklist against the candidate's Spotify
        # tracklist is the strongest signal we have for "is this actually
        # the same album."
        if verify_tracklist_callback:
            tl_album_id = spotify_album.get('id')
            tracklist_result = (
                verify_tracklist_callback(tl_album_id) if tl_album_id else None
            )
            if tracklist_result is False:
                return (
                    False,
                    "Tracklist mismatch (Spotify album appears to be a different release)",
                    scores,
                )
            if tracklist_result is True:
                scores['verified_by_tracklist'] = True
                logger.debug(
                    f"      Album accepted via tracklist verification (artist {artist_similarity}%)")
                return True, "Valid match (verified by tracklist)", scores
            # tracklist_result is None — couldn't verify (no MB ID,
            # MB unreachable, etc.). Fall through to the existing
            # substring / track-presence fallback so an outage in MB
            # doesn't block all weak-artist matches.

        # Check if artist is accepted via substring containment
        # (e.g., "Lynne Arriale" contained in "Lynne Arriale Trio")
        artist_valid_by_substring = artist_is_substring and artist_similarity >= 50

        if artist_valid_by_substring:
            logger.debug(f"      Artist accepted via substring containment ({artist_similarity}%)")
        else:
            # Artist validation failed - try track verification fallback
            # This handles "Various Artists" compilations where artist matching is meaningless
            # Only attempt if album similarity is high (>=80%) and we have a song title
            #
            # IMPORTANT: Skip this fallback when the album title is essentially the song title
            # (common for singles). In that case, the track will trivially be found on ANY
            # album with that name, regardless of artist, leading to false positive matches.
            album_is_song_title = (
                song_title and
                calculate_similarity(expected_album, song_title) >= 85
            )
            if album_is_song_title:
                logger.debug(f"      Track verification skipped (album title matches song title - likely a single)")

            if song_title and album_similarity >= 80 and verify_track_callback and not album_is_song_title:
                # For compilation artists (Various Artists, etc.), allow lenient track verification
                # For real artists, require at least 50% artist similarity to use track verification
                # This prevents matching completely unrelated artists who happen to share
                # album/track names (e.g., Charlie Parker → Stephane Grappelli at 43.8%,
                # Art Tatum → Art Pepper at 42.1%) — common with jazz standards.
                # Ensemble variations (Bill Evans → Bill Evans Trio) are already handled
                # by the substring check above, so this threshold only affects non-substring cases.
                is_compilation = is_compilation_artist(expected_artist)
                min_artist_for_track_verify = 0 if is_compilation else 50

                if artist_similarity >= min_artist_for_track_verify:
                    album_id = spotify_album.get('id')
                    if album_id and verify_track_callback(album_id, song_title):
                        scores['verified_by_track'] = True
                        if is_compilation:
                            logger.debug(f"      Album accepted via track verification (compilation artist)")
                        else:
                            logger.debug(f"      Album accepted via track verification (artist {artist_similarity}% >= {min_artist_for_track_verify}%)")
                        return True, "Valid match (verified by track presence)", scores
                else:
                    logger.debug(f"      Track verification skipped (artist {artist_similarity}% < {min_artist_for_track_verify}% minimum for non-compilation)")

            return False, f"Artist similarity too low ({artist_similarity}% < {min_artist_similarity}%)", scores

    return True, "Valid match", scores


# ---------------------------------------------------------------------------
# Track verification + duration scoring + single-track matching
#
# Moved out of SpotifyMatcher in #115 step 3. These are the scoring paths the
# matcher uses once candidate Spotify data is in hand. They take their
# dependencies (client, logger, thresholds, stats dict) as explicit args so
# the module stays stateless and unit-testable without instantiating a full
# matcher.
# ---------------------------------------------------------------------------


def verify_album_contains_track(client, log: logging.Logger,
                                min_track_similarity: int,
                                album_id: str, song_title: str) -> bool:
    """
    Verify that a Spotify album contains a track matching the song title.

    Used as a fallback validation when artist matching fails but album
    similarity is high — handles compilation albums, "Various Artists",
    and artist name variations.

    Args:
        client: SpotifyClient providing get_album_tracks()
        log: Logger for debug output
        min_track_similarity: Title-similarity threshold (0-100) that a track
            on the album must meet for verification to pass.
        album_id: Spotify album ID
        song_title: Song title to search for in the album

    Returns:
        True if at least one track on the album matches the title above the
        threshold, False otherwise.
    """
    tracks = client.get_album_tracks(album_id)
    if not tracks:
        return False

    for track in tracks:
        similarity = calculate_similarity(song_title, track['name'])
        if similarity >= min_track_similarity:
            log.debug(f"      Track verification passed: '{track['name']}' ({similarity}%)")
            return True

    return False


def duration_confidence(expected_ms: int, actual_ms: int) -> float:
    """
    Confidence score (0.0-1.0) based on absolute duration difference.

    Thresholds:
      < 5s:      1.0  (perfect — encoding/rounding difference)
      5-30s:     0.9  (remaster or slight edit)
      30s-2min:  0.7  (different edit/version, worth flagging)
      2-5min:    0.4  (likely wrong performance)
      > 5min:    0.2  (almost certainly wrong)
    """
    diff = abs(expected_ms - actual_ms)
    if diff <= 5000:
        return 1.0
    elif diff <= 30000:
        return 0.9
    elif diff <= 120000:
        return 0.7
    elif diff <= 300000:
        return 0.4
    else:
        return 0.2


def duration_adjusted_score(title_score: float, expected_ms: int,
                            track_duration_ms: int) -> float:
    """
    Adjust a title similarity score using duration proximity.

    Duration acts as a soft tie-breaker: when two tracks have similar title
    scores, the one with closer duration wins. The adjustment is small
    (+/- up to 5 points) — a clearly better title match still wins, but
    it's enough to break ties between identical titles (Take 1 vs Take 2,
    live vs studio, etc).

    If either duration is unknown, returns the title score unchanged.
    """
    if expected_ms is None or track_duration_ms is None:
        return title_score

    confidence = duration_confidence(expected_ms, track_duration_ms)
    # Map confidence (0.2-1.0) to adjustment (-4 to +5 points)
    # 1.0 → +5, 0.9 → +3.75, 0.7 → +1.25, 0.4 → -2.5, 0.2 → -5
    adjustment = (confidence - 0.5) * 10
    return title_score + adjustment


def fetch_mb_tracks_for_release(conn, release_id: str) -> list:
    """Fetch the MusicBrainz tracklist for one of our internal releases.

    Returns a list of {title, position, normalized} dicts ordered by
    position across all media. Returns [] if the release has no MB ID
    or the MB lookup failed — callers must treat empty as "unable to
    verify" rather than "tracklist is empty".
    """
    # Local import — avoid a module-level dependency from spotify.matching
    # on integrations.musicbrainz.
    from integrations.musicbrainz.utils import MusicBrainzSearcher

    with conn.cursor() as cur:
        cur.execute(
            "SELECT musicbrainz_release_id FROM releases WHERE id = %s",
            (release_id,),
        )
        row = cur.fetchone()

    if not row or not row['musicbrainz_release_id']:
        return []

    mb_searcher = MusicBrainzSearcher()
    release_data = mb_searcher.get_release_details(row['musicbrainz_release_id'])
    if not release_data:
        return []

    mb_tracks = []
    position = 0
    for medium in release_data.get('media', []):
        for track in medium.get('tracks', []):
            position += 1
            mb_tracks.append({
                'title': track.get('title', ''),
                'position': position,
                'normalized': normalize_for_comparison(track.get('title', '')),
            })
    return mb_tracks


def compare_mb_to_spotify_tracks(mb_tracks: list, spotify_tracks: list) -> dict:
    """Match MB tracks to Spotify tracks by title similarity (with a small
    same-position bonus) and report coverage stats.

    Returns a dict with mb_track_count, spotify_track_count, matched_count,
    match_ratio (0.0-1.0), and matched_titles (list of (mb_title, sp_title,
    similarity) tuples). The match-acceptance threshold is 75 (token-sort).
    """
    from rapidfuzz import fuzz

    result = {
        'mb_track_count': len(mb_tracks),
        'spotify_track_count': len(spotify_tracks),
        'matched_count': 0,
        'match_ratio': 0.0,
        'matched_titles': [],
    }

    if not mb_tracks or not spotify_tracks:
        return result

    sp_normalized = [
        normalize_for_comparison(t['name']) for t in spotify_tracks
    ]

    used_sp_indices = set()
    for mb_track in mb_tracks:
        best_score = 0
        best_idx = -1
        best_sp_title = ''

        for idx, sp_norm in enumerate(sp_normalized):
            if idx in used_sp_indices:
                continue
            score = fuzz.token_sort_ratio(mb_track['normalized'], sp_norm)
            if abs(mb_track['position'] - (idx + 1)) <= 2 and score >= 70:
                score = min(100, score + 5)
            if score > best_score:
                best_score = score
                best_idx = idx
                best_sp_title = spotify_tracks[idx]['name']

        if best_score >= 75:
            result['matched_titles'].append(
                (mb_track['title'], best_sp_title, best_score))
            used_sp_indices.add(best_idx)

    result['matched_count'] = len(result['matched_titles'])
    result['match_ratio'] = (
        result['matched_count'] / result['mb_track_count']
        if result['mb_track_count'] > 0 else 0.0
    )
    return result


def check_album_context_via_tracklist(conn, release_id: str,
                                      spotify_tracks: list) -> dict:
    """
    Compare the full MusicBrainz release tracklist against the Spotify album
    tracklist to assess whether this is genuinely the same album.

    Used as a rescue signal for tracks that would otherwise be rejected on
    duration confidence alone — if the surrounding album clearly lines up
    with Spotify, we trust the match more.

    Returns:
        Dict with mb_track_count, spotify_track_count, matched_count,
        match_ratio (float 0.0-1.0), and matched_titles (list of
        (mb_title, sp_title, similarity) tuples).
    """
    mb_tracks = fetch_mb_tracks_for_release(conn, release_id)
    info = compare_mb_to_spotify_tracks(mb_tracks, spotify_tracks)
    # Preserve the original return shape — callers expect mb_track_count=0
    # both for "no MB data" and "MB returned an empty release".
    return info


def match_track_to_recording(log: logging.Logger, stats: dict,
                             min_track_similarity: int,
                             song_title: str,
                             spotify_tracks: List[dict],
                             expected_disc: int = None,
                             expected_track: int = None,
                             alt_titles: List[str] = None,
                             song_id: str = None,
                             conn=None,
                             expected_duration_ms: int = None) -> Optional[dict]:
    """
    Find the best matching Spotify track for a song title.

    Args:
        log: Logger for debug messages
        stats: Mutable counters dict — `stats['tracks_blocked']` is incremented
            when a candidate is skipped because it's on the blocklist for
            `song_id`. The caller usually passes matcher.stats.
        min_track_similarity: Title similarity threshold (0-100) for accepting
            a candidate.
        song_title: The song title to match
        spotify_tracks: List of track dicts from get_album_tracks()
        expected_disc: Expected disc number (optional, for position-based fallback)
        expected_track: Expected track number (optional, for position-based fallback)
        alt_titles: Alternative titles to try if primary title doesn't match
        song_id: Our database song ID (for blocklist checking)
        conn: Optional existing database connection. If provided, uses it
              instead of opening a new one (avoids idle-connection timeouts
              when called from within a transaction).
        expected_duration_ms: MusicBrainz recording duration (optional).
              Used as a soft signal to prefer tracks with closer duration.

    Returns:
        Best matching track dict or None if no good match.
    """
    best_match = None
    best_score = 0
    # An exact-normalized-match candidate always beats a non-exact one,
    # regardless of duration tiebreaker. Issue #100: when an album carries
    # multiple variations of the same song (e.g. both "Well You Needn't"
    # and "Well You Needn't (opening)"), the parenthetical-strip rescue
    # in calculate_similarity makes both candidates score 100% no matter
    # which side of the variation we're querying for. Without this hard
    # preference, only the duration tiebreaker decides — and small
    # duration ambiguities pick the wrong track.
    best_was_exact = False
    # Position match — same (disc, track) on MB and Spotify — is a
    # stronger signal than duration when title scores tie. Real case: a
    # release with TWO recordings of the same song titled "My Heart
    # Stood Still" at positions 1-7 and 1-20 (different durations) had
    # its assignments crossed because duration is a ±5-point soft
    # tiebreaker that flips on small differences. Position info is
    # authoritative when both sides agree on it, so we use it as the
    # next tier after exact-title and before duration.
    best_was_position_match = False

    # Build set of blocked track IDs for this song (more efficient than per-track DB calls)
    blocked_track_ids = set()
    if song_id:
        from integrations.spotify.db import get_blocked_tracks_for_song
        blocked_track_ids = set(get_blocked_tracks_for_song(song_id, conn=conn))
        if blocked_track_ids:
            log.debug(f"      Found {len(blocked_track_ids)} blocked track(s) for this song")

    def _consider(track, query_title: str) -> bool:
        """Score `track` against `query_title`; update best_match in place
        if it wins. Returns True when this candidate became the new best.

        Tiebreaker tiers, in priority order:
          1. structural-exact-title match (a candidate exactly matches
             the query in normalised structural terms — see
             is_structural_title_match)
          2. position match — disc and track both equal the MB row's
             expected position
          3. duration-adjusted title score
        """
        nonlocal best_match, best_score, best_was_exact, best_was_position_match

        is_exact = is_structural_title_match(query_title, track['name'])

        # Structural-match candidates are guaranteed to clear the threshold,
        # even when their fuzzy score doesn't. Real case (issue #100): an
        # MB recording titled "Well, You Needn't (opening)" against a
        # Spotify track titled "Well You Needn't - Opening" only fuzz-scores
        # 78% — below the 85% threshold — because the parenthetical-strip
        # rescue in calculate_similarity doesn't normalize across paren-vs-
        # dash syntax. Without this floor the variant Spotify track gets
        # filtered out before the exact-preference logic below ever sees
        # it, leaving only the long "Well You Needn't" track as a candidate.
        title_score = calculate_similarity(query_title, track['name'])
        if is_exact and title_score < 100:
            title_score = 100.0

        if title_score < min_track_similarity:
            return False

        adjusted_score = duration_adjusted_score(
            title_score, expected_duration_ms, track.get('duration_ms'))

        is_position_match = (
            expected_disc is not None
            and expected_track is not None
            and track.get('disc_number') == expected_disc
            and track.get('track_number') == expected_track
        )

        # Tier 1 — structural exact beats inexact unconditionally.
        if is_exact != best_was_exact:
            if is_exact:
                best_match = track
                best_score = adjusted_score
                best_was_exact = True
                best_was_position_match = is_position_match
                return True
            return False

        # Tier 2 — within an exactness group, position match wins.
        if is_position_match != best_was_position_match:
            if is_position_match:
                best_match = track
                best_score = adjusted_score
                best_was_position_match = True
                return True
            return False

        # Tier 3 — same exactness AND same position-match status:
        # higher duration-adjusted score wins.
        if adjusted_score > best_score:
            best_match = track
            best_score = adjusted_score
            return True
        return False

    # First pass: standard fuzzy matching with primary title, duration-adjusted
    for track in spotify_tracks:
        if track['id'] in blocked_track_ids:
            log.debug(f"      Skipping blocked track: {track['id']} ('{track['name']}')")
            stats['tracks_blocked'] = stats.get('tracks_blocked', 0) + 1
            continue
        _consider(track, song_title)

    if best_match:
        duration_info = ""
        if expected_duration_ms and best_match.get('duration_ms'):
            diff = abs(expected_duration_ms - best_match['duration_ms']) / 1000
            duration_info = f", duration diff {diff:.0f}s"
        exact_info = " exact" if best_was_exact else ""
        log.debug(f"      Track match: '{song_title}' → '{best_match['name']}' ({best_score:.0f}%{duration_info}{exact_info})")
        return best_match

    # Second pass: try alternative titles
    if alt_titles:
        for alt_title in alt_titles:
            for track in spotify_tracks:
                if track['id'] in blocked_track_ids:
                    continue
                _consider(track, alt_title)

            if best_match:
                duration_info = ""
                if expected_duration_ms and best_match.get('duration_ms'):
                    diff = abs(expected_duration_ms - best_match['duration_ms']) / 1000
                    duration_info = f", duration diff {diff:.0f}s"
                exact_info = " exact" if best_was_exact else ""
                log.debug(f"      Track match via alt title: '{alt_title}' → '{best_match['name']}' ({best_score:.0f}%{duration_info}{exact_info})")
                return best_match

    # Fallback: if positions provided and no fuzzy match, try position-based substring match.
    # Handles cases like "An Affair to Remember" vs
    # "An Affair to Remember - From the 20th Century-Fox Film, An Affair To Remember"
    if expected_disc is not None and expected_track is not None:
        for track in spotify_tracks:
            if track['id'] in blocked_track_ids:
                continue

            if track.get('disc_number') == expected_disc and track.get('track_number') == expected_track:
                if is_substring_title_match(song_title, track['name']):
                    log.debug(f"      Position+substring match: '{song_title}' → '{track['name']}' "
                              f"(disc {expected_disc}, track {expected_track})")
                    return track

                if alt_titles:
                    for alt_title in alt_titles:
                        if is_substring_title_match(alt_title, track['name']):
                            log.debug(f"      Position+substring match via alt title: '{alt_title}' → '{track['name']}' "
                                      f"(disc {expected_disc}, track {expected_track})")
                            return track

    return best_match