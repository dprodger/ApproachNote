#!/usr/bin/env python3
"""
Wikipedia Utilities
Shared utilities for searching and interacting with Wikipedia API
"""

import time
import logging
import requests
from bs4 import BeautifulSoup
import re
import os
import json
import hashlib
from datetime import datetime, timedelta
from pathlib import Path

from core.cache_utils import get_cache_dir
from core.http_client import make_session

logger = logging.getLogger(__name__)

# Terms that mark a page's primary subject as a non-musician. Used to reject
# pages whose infobox/lead clearly describe an actor, athlete, politician, etc.
# Kept focused to avoid false rejects; only applied when NO music term is also
# present in the infobox/lead.
_NON_MUSICIAN_TERMS = [
    'actor', 'actress', 'filmmaker', 'screenwriter', 'comedian',
    'basketball', 'footballer', 'baseball', 'quarterback', 'athlete',
    'politician', 'senator', 'congressman', 'governor', 'mayor', 'president',
    'novelist', 'painter', 'sculptor', 'economist', 'physicist', 'philosopher',
]

# Music terms whose presence in the infobox/lead protects a genuine musician
# from the non-musician guard above (e.g. 'jazz organist').
_MUSICIAN_TERMS = [
    'musician', 'singer', 'vocalist', 'pianist', 'organist', 'guitarist',
    'bassist', 'drummer', 'saxophonist', 'trumpeter', 'trombonist',
    'composer', 'bandleader', 'jazz', 'blues', 'bebop', 'swing',
]


class WikipediaSearcher:
    """Shared Wikipedia search functionality with caching"""
    
    def __init__(self, cache_days=7, force_refresh=False):  # Remove cache_dir parameter
        """
        Initialize Wikipedia searcher with caching support
        
        Args:
            cache_dir: Directory to store cached Wikipedia pages
            cache_days: Number of days before cache is considered stale
            force_refresh: If True, always fetch fresh data ignoring cache
        """
        self.session = make_session()
        
        # Rate limiting
        self.last_request_time = 0
        self.min_request_interval = 1.0
        
        # Store cache settings as instance variables
        self.cache_days = cache_days
        self.force_refresh = force_refresh        

        # Get cache directories using the shared utility
        # This ensures we use the persistent disk mount on Render
        self.cache_dir = get_cache_dir('wikipedia')
        self.search_cache_dir = self.cache_dir / 'searches'
        
        # Create subdirectories
        self.search_cache_dir.mkdir(parents=True, exist_ok=True)
        
        logger.debug(f"Wikipedia cache directory: {self.cache_dir}")        
        
        # Track whether last operation made an API call
        self.last_made_api_call = False
        
        # Create cache directories if they don't exist
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.search_cache_dir = self.cache_dir / 'searches'
        self.search_cache_dir.mkdir(parents=True, exist_ok=True)
        
        logger.debug(f"Wikipedia cache: {self.cache_dir} (expires after {cache_days} days, force_refresh={force_refresh})")
    
    def _get_cache_path(self, url):
        """
        Get the cache file path for a Wikipedia URL
        
        Args:
            url: Wikipedia URL
            
        Returns:
            Path object for the cache file
        """
        # Create a safe filename from the URL using hash
        url_hash = hashlib.md5(url.encode()).hexdigest()
        # Also include a human-readable part from the URL
        url_part = url.split('/')[-1][:50]  # Last part of URL, max 50 chars
        filename = f"{url_part}_{url_hash}.json"
        return self.cache_dir / filename
    
    def _get_search_cache_path(self, search_query):
        """
        Get the cache file path for a search query
        
        Args:
            search_query: Search query string
            
        Returns:
            Path object for the cache file
        """
        query_hash = hashlib.md5(search_query.encode()).hexdigest()
        safe_query = re.sub(r'[^a-zA-Z0-9_-]', '_', search_query.lower())[:50]
        filename = f"search_{safe_query}_{query_hash}.json"
        return self.search_cache_dir / filename
    
    def _is_cache_valid(self, cache_path):
        """
        Check if cache file exists and is not expired
        
        Args:
            cache_path: Path to cache file
            
        Returns:
            bool: True if cache is valid and not expired
        """
        if not cache_path.exists():
            return False
        
        # Check file modification time
        mtime = datetime.fromtimestamp(cache_path.stat().st_mtime)
        age = datetime.now() - mtime
        
        is_valid = age.days < self.cache_days
        if is_valid:
            logger.debug(f"Cache valid (age: {age.days} days): {cache_path.name}")
        else:
            logger.debug(f"Cache expired (age: {age.days} days): {cache_path.name}")
        
        return is_valid
    
    def _load_from_cache(self, url):
        """
        Load Wikipedia page content from cache
        
        Args:
            url: Wikipedia URL
            
        Returns:
            dict with 'html' and 'fetched_at', or None if not in cache
        """
        cache_path = self._get_cache_path(url)
        
        if not self._is_cache_valid(cache_path):
            return None
        
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
                logger.debug(f"Loaded from cache: {url}")
                return cache_data
        except Exception as e:
            logger.warning(f"Failed to load cache file {cache_path}: {e}")
            return None
    
    def _save_to_cache(self, url, html_content):
        """
        Save Wikipedia page content to cache
        
        Args:
            url: Wikipedia URL
            html_content: HTML content to cache
        """
        cache_path = self._get_cache_path(url)
        
        try:
            cache_data = {
                'url': url,
                'html': html_content,
                'fetched_at': datetime.now().isoformat()
            }
            
            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
            
            logger.debug(f"Saved to cache: {url}")
        except Exception as e:
            logger.warning(f"Failed to save cache file {cache_path}: {e}")
    
    def _load_search_from_cache(self, search_query):
        """Load search results from cache"""
        cache_path = self._get_search_cache_path(search_query)
        
        if not self._is_cache_valid(cache_path):
            return None
        
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
                logger.debug(f"Loaded search from cache: {search_query}")
                return cache_data.get('results')
        except Exception as e:
            logger.warning(f"Failed to load search cache: {e}")
            return None
    
    def _save_search_to_cache(self, search_query, search_results):
        """Save search results to cache"""
        cache_path = self._get_search_cache_path(search_query)
        
        try:
            cache_data = {
                'query': search_query,
                'results': search_results,
                'cached_at': datetime.now().isoformat()
            }
            
            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
            
            logger.debug(f"Saved search to cache: {search_query}")
        except Exception as e:
            logger.warning(f"Failed to save search cache: {e}")
    
    def _fetch_wikipedia_page(self, url):
        """
        Fetch Wikipedia page, using cache if available
        
        Args:
            url: Wikipedia URL to fetch
            
        Returns:
            HTML content as string, or None if fetch failed
        """
        # Check cache first (unless force_refresh is enabled)
        if not self.force_refresh:
            cached = self._load_from_cache(url)
            if cached:
                logger.debug(f"  Using cached Wikipedia page (fetched: {cached['fetched_at'][:10]})")
                self.last_made_api_call = False
                return cached['html']
        
        # Fetch from Wikipedia
        logger.debug(f"  Fetching from Wikipedia...")
        self.last_made_api_call = True
        self.rate_limit()
        
        try:
            response = self.session.get(url, timeout=10)
            
            if response.status_code != 200:
                logger.warning(f"Wikipedia returned status code {response.status_code}")
                return None
            
            # Save to cache
            self._save_to_cache(url, response.text)
            
            return response.text
            
        except requests.RequestException as e:
            logger.error(f"Error fetching Wikipedia URL {url}: {e}")
            return None
    
    def rate_limit(self):
        """Enforce rate limiting for Wikipedia API"""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_request_interval:
            sleep_time = self.min_request_interval - elapsed
            logger.debug("sleeping in rate_limit")
            time.sleep(sleep_time)
        self.last_request_time = time.time()

    def _strip_nickname(self, name):
        """Remove a decorative quoted nickname and normalize smart quotes.

        Performer names sometimes embed a nickname in quotes, e.g.
        '“Brother” Jack McDuff' or '‘Papa’ John DeFrancesco'. The quoted part is
        decorative; the legal name ('Jack McDuff') is what Wikipedia titles and
        our matching want. Only paired double quotes ("..." / “...”) and paired
        smart single quotes (‘...’) are stripped — a lone straight apostrophe
        (O'Brien, 'Night Sweet Pea) has no opener, so such names are untouched.

        The stripped form is only returned when it still has at least two
        tokens (a plausible first + last name). If stripping collapses the
        name to a single bare surname (e.g. '‘Doc’ West' -> 'West'), the
        original is returned instead: a lone surname is too generic — it
        fuzzy-matches unrelated famous people ('West' -> Kanye West) and
        partial-matches any 'First Surname' page.
        """
        s = name.replace('“', '"').replace('”', '"')
        s = re.sub(r'"[^"]*"', ' ', s)                  # "nickname"
        s = re.sub(r'‘[^’]*’', ' ', s)   # ‘nickname’
        s = re.sub(r'\s+', ' ', s).strip()
        if s and len(s.split()) >= 2:
            return s
        return name.strip()

    def verify_wikipedia_reference(self, performer_name, wikipedia_url, context):
        """
        Verify that a Wikipedia URL is valid and refers to the correct performer
        
        Args:
            performer_name: Name of the performer
            wikipedia_url: Wikipedia URL to verify
            context: Dict with birth_date, death_date, sample_songs for verification
            
        Returns:
            Dict with 'valid' (bool), 'confidence' (str), 'reason' (str)
        """
        try:
            logger.debug(f"Verifying Wikipedia URL: {wikipedia_url}")
            
            # Fetch page (from cache if available)
            html_content = self._fetch_wikipedia_page(wikipedia_url)
            
            if not html_content:
                return {
                    'valid': False,
                    'confidence': 'certain',
                    'reason': 'Failed to fetch Wikipedia page'
                }
            
            # Parse the page
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Get the main content area (skip navigation/menus)
            content_div = soup.find('div', {'id': 'mw-content-text'}) or soup.find('div', {'class': 'mw-parser-output'})
            if content_div:
                # Drop hatnotes ("For the musician, see ...") before reading the
                # text: they are cross-references to OTHER subjects, and letting
                # their keywords leak in mis-scores the page (e.g. the actor Kirk
                # Douglas hatnote mentions "musician" and points at the real one).
                for hatnote in content_div.select('div.hatnote, .hatnote'):
                    hatnote.decompose()
                page_text = content_div.get_text().lower()
            else:
                page_text = soup.get_text().lower()
            
            # Check if this is a disambiguation or redirect to wrong page
            # Method 1: Check if page title explicitly ends with "(disambiguation)"
            page_title = soup.find('h1', {'id': 'firstHeading'})
            if page_title:
                page_title_text = page_title.get_text().strip()
                if page_title_text.endswith('(disambiguation)'):
                    logger.debug(f"Page title ends with '(disambiguation)' - rejecting page")
                    return {
                        'valid': False,
                        'confidence': 'high',
                        'reason': 'Page is a disambiguation page',
                        'score': 0
                    }
            
            # Method 1b: Check for actual disambiguation page indicators
            # Look for patterns like "may refer to" at the start, which indicates a real disambiguation page
            # Note: We ignore hatnotes like "For other uses, see X (disambiguation)" which just reference disambiguation pages
            logger.debug(f"Checking for disambiguation page indicators...")
            first_paragraph = page_text[:800]
            # Real disambiguation pages typically start with "[Name] may refer to:"
            if re.search(r'^[^.]*?\bmay refer to\b', first_paragraph):
                logger.debug(f"Found 'may refer to' pattern indicating disambiguation page")
                return {
                    'valid': False,
                    'confidence': 'high',
                    'reason': 'Page is a disambiguation page',
                    'score': 0
                }
            
            # Method 2: Check if page has many bullet points with birth/death dates
            # which suggests it's listing multiple people
            logger.debug(f"Checking for multiple birth year patterns...")
            ul_lists = soup.find_all('ul', limit=3)
            if ul_lists:
                list_text = ' '.join([ul.get_text() for ul in ul_lists[:2]])
                # Count how many birth year patterns like "(1942–2020)" or "(born 1974)"
                birth_patterns = re.findall(r'\((?:born\s+)?\d{4}', list_text)
                logger.debug(f"Found {len(birth_patterns)} birth year patterns: {birth_patterns[:5]}")
                if len(birth_patterns) >= 3:
                    logger.debug(f"Multiple birth patterns found - rejecting as disambiguation page")
                    return {
                        'valid': False,
                        'confidence': 'high',
                        'reason': f'Page appears to be a disambiguation page (lists {len(birth_patterns)} different people)',
                        'score': 0
                    }
            
            # Calculate confidence based on multiple factors
            confidence_score = 0
            reasons = []
            
            # Check name similarity
            page_title_text = ''
            page_title = soup.find('h1', {'id': 'firstHeading'})
            if page_title:
                page_title_text = page_title.get_text().strip()

                # Check if the title disambiguation clearly indicates a NON-musician
                # Extract the disambiguation term in parentheses (e.g., "(basketball)" from "Sam Jones (basketball)")
                disambiguation_match = re.search(r'\(([^)]+)\)$', page_title_text)
                if disambiguation_match:
                    disambiguation_term = disambiguation_match.group(1).lower()
                    
                    # Non-musician professions/fields
                    non_musician_terms = [
                        'basketball', 'football', 'baseball', 'hockey', 'soccer', 'cricket',
                        'athlete', 'sports', 'player', 'coach',
                        'politician', 'politics', 'senator', 'congressman', 'mayor',
                        'businessman', 'business', 'entrepreneur', 'ceo', 'executive',
                        'actor', 'actress', 'film', 'television',
                        'writer', 'author', 'journalist', 'poet',
                        'scientist', 'physicist', 'chemist', 'biologist',
                        'military', 'general', 'admiral', 'colonel'
                    ]
                    
                    # Musician-related terms that should NOT reject
                    musician_terms = [
                        'musician', 'singer', 'vocalist', 'pianist', 'guitarist', 'bassist',
                        'drummer', 'saxophonist', 'trumpeter', 'composer', 'conductor',
                        'bandleader', 'jazz', 'blues', 'rock', 'folk', 'country'
                    ]
                    
                    # Check if disambiguation term indicates non-musician
                    is_non_musician = any(term in disambiguation_term for term in non_musician_terms)
                    is_musician = any(term in disambiguation_term for term in musician_terms)
                    
                    if is_non_musician and not is_musician:
                        logger.debug(f"Page title indicates non-musician: '{page_title_text}'")
                        return {
                            'valid': False,
                            'confidence': 'high',
                            'reason': f'Page is about a {disambiguation_term}, not a musician',
                            'score': 0
                        }
                
                # Remove disambiguation parentheses like "(saxophonist)" and
                # strip decorative quoted nicknames from both sides so e.g.
                # '“Brother” Jack McDuff' matches the page titled 'Jack McDuff'
                # as an exact (not merely partial) name match.
                page_name = re.sub(r'\s*\([^)]*\)\s*$', '', page_title_text).strip()
                page_name = self._strip_nickname(page_name).lower()
                performer_name_lower = self._strip_nickname(performer_name).lower()

                name_match = False
                if page_name == performer_name_lower:
                    confidence_score += 30
                    reasons.append(f"Exact name match")
                    name_match = True
                elif performer_name_lower in page_name or page_name in performer_name_lower:
                    confidence_score += 15
                    reasons.append(f"Partial name match")
                    name_match = True
                else:
                    # Name doesn't match - check how different the names are
                    # Split into parts and compare
                    performer_parts = set(performer_name_lower.split())
                    page_parts = set(page_name.split())

                    # Check if last names match (most important for identification)
                    performer_last = performer_name_lower.split()[-1] if performer_name_lower.split() else ""
                    page_last = page_name.split()[-1] if page_name.split() else ""

                    # If last names are different and not similar, this is likely wrong person
                    if performer_last and page_last and performer_last != page_last:
                        # Use Levenshtein-like similarity: how many edits to transform one to other?
                        # Simple approximation: longest common substring ratio
                        similarity = self._string_similarity(performer_last, page_last)

                        if similarity < 0.8:
                            # Last names are clearly different - reject this page
                            logger.debug(f"Last name mismatch: '{performer_last}' vs '{page_last}' (similarity: {similarity:.2f})")
                            return {
                                'valid': False,
                                'confidence': 'high',
                                'reason': f"Name mismatch: expected '{performer_name}', page is about '{page_title_text}'",
                                'score': 0
                            }

                    # Names don't match but might be related (e.g., stage name vs birth name)
                    # Apply a penalty but don't reject outright
                    confidence_score -= 20
                    reasons.append(f"Name mismatch: expected '{performer_name}', page is '{page_title_text}'")
            
            # Look for infobox (strong signal this is a musician page)
            infobox_text = ''
            infobox = soup.find('table', {'class': 'infobox'})
            if infobox:
                infobox_text = infobox.get_text().lower()
                
                # Check for SPECIFIC music-related terms in infobox (not just "occupation")
                specific_music_terms = [
                    'jazz', 'musician', 'singer', 'vocalist', 'pianist', 'composer',
                    'saxophonist', 'trumpeter', 'bassist', 'drummer', 'guitarist',
                    'bandleader', 'blues', 'soul', 'r&b', 'gospel', 'folk',
                    'instruments', 'genres', 'labels'
                ]
                found_specific_terms = [term for term in specific_music_terms if self._word_in_text(term, infobox_text)]
                if found_specific_terms:
                    confidence_score += 40  # Strong signal
                    reasons.append(f"Infobox contains music terms: {', '.join(found_specific_terms[:3])}")
                elif 'occupation' in infobox_text:
                    # Has occupation but no music-specific terms - only give small boost
                    confidence_score += 10
                    reasons.append(f"Infobox present but no specific music terms")
            
            # Check for jazz musician keywords in main content
            # Use more specific terms that are clearly music-related
            specific_music_keywords = [
                'jazz', 'musician', 'singer', 'vocalist', 'pianist', 
                'saxophonist', 'trumpeter', 'bassist', 'drummer', 
                'guitarist', 'composer', 'bandleader',
                'album', 'recording', 'blues', 'soul', 'r&b', 
                'gospel', 'folk', 'orchestra', 'symphony',
                'concerto', 'sonata', 'opera'
            ]
            # More generic terms that need context (could be sports, business, etc)
            generic_music_keywords = [
                'music', 'song', 'performance', 'concert', 'stage'
            ]
            
            # Search in first 2000 characters using word boundary matching
            # FIXED: Use word boundaries to avoid matching "opera" in "operating"
            found_specific = [kw for kw in specific_music_keywords if self._word_in_text(kw, page_text[:2000])]
            found_generic = [kw for kw in generic_music_keywords if self._word_in_text(kw, page_text[:2000])]
            
            if found_specific:
                # Specific music terms get full points
                confidence_score += 20
                reasons.append(f"Found music keywords: {', '.join(found_specific[:3])}")
            elif found_generic:
                # Generic terms only get partial credit and only if we have other signals
                confidence_score += 5
                reasons.append(f"Found generic music keywords: {', '.join(found_generic[:2])}")

            # Guard: reject pages whose primary subject is clearly a non-musician
            # (actor, athlete, politician, ...). We look only at the infobox and
            # the lead sentence — an incidental "musician" mention later in the
            # body must not rescue e.g. the actor Kirk Douglas. A music term in
            # the infobox/lead protects genuine musicians ('jazz organist').
            lead_text = page_text[:600]
            subject_text = f"{infobox_text} {lead_text}"
            non_musician_hits = [t for t in _NON_MUSICIAN_TERMS if self._word_in_text(t, subject_text)]
            music_hits = [t for t in _MUSICIAN_TERMS if self._word_in_text(t, subject_text)]
            if non_musician_hits and not music_hits:
                logger.debug(f"Primary subject looks non-musician ({non_musician_hits[:2]}), no music signal - rejecting")
                return {
                    'valid': False,
                    'confidence': 'high',
                    'reason': f"Page subject appears to be a {non_musician_hits[0]}, not a musician",
                    'score': 0
                }

            # Check birth/death dates if available
            has_corroboration = False
            if context.get('birth_date'):
                birth_year = str(context['birth_date'].year) if hasattr(context['birth_date'], 'year') else str(context['birth_date'])[:4]
                if birth_year in page_text[:2000]:
                    confidence_score += 25
                    has_corroboration = True
                    reasons.append(f"Birth year {birth_year} found on page")

            if context.get('death_date'):
                death_year = str(context['death_date'].year) if hasattr(context['death_date'], 'year') else str(context['death_date'])[:4]
                if death_year in page_text[:2000]:
                    confidence_score += 20
                    has_corroboration = True
                    reasons.append(f"Death year {death_year} found on page")

            # Check if any of the performer's songs are mentioned
            if context.get('sample_songs'):
                song_mentions = [song for song in context['sample_songs']
                               if song and song.lower() in page_text]
                if song_mentions:
                    confidence_score += 25
                    has_corroboration = True
                    reasons.append(f"Found song references: {', '.join(song_mentions[:2])}")

            # Guard: a parenthetically disambiguated title (e.g.
            # "Joe Jones (Fluxus musician)") means several same-named people
            # exist, so a bare name + generic music keywords isn't enough to
            # know which one this is. Require corroboration (birth/death year or
            # a song on the page) before accepting such a page.
            title_disambiguated = bool(re.search(r'\([^)]+\)\s*$', page_title_text))
            if title_disambiguated and not has_corroboration:
                logger.debug(f"Disambiguated title '{page_title_text}' without corroboration - not accepting")
                return {
                    'valid': False,
                    'confidence': 'low',
                    'reason': f"Disambiguated page '{page_title_text}' needs birth/death or song corroboration (score: {confidence_score})",
                    'score': confidence_score
                }

            # Determine validity based on confidence score
            # Require at least 50 points (medium confidence) to accept
            if confidence_score >= 50:
                return {
                    'valid': True,
                    'confidence': 'high' if confidence_score >= 70 else 'medium',
                    'reason': '; '.join(reasons) if reasons else 'Page appears valid (score: {})'.format(confidence_score),
                    'score': confidence_score
                }
            else:
                return {
                    'valid': False,
                    'confidence': 'low' if confidence_score >= 30 else 'very_low',
                    'reason': 'Insufficient evidence of correct performer (score: {}): {}'.format(confidence_score, '; '.join(reasons)),
                    'score': confidence_score
                }
                
        except requests.RequestException as e:
            logger.error(f"Error verifying Wikipedia URL {wikipedia_url}: {e}")
            return {
                'valid': False,
                'confidence': 'uncertain',
                'reason': f'Request failed: {str(e)}'
            }
        except Exception as e:
            logger.error(f"Unexpected error verifying Wikipedia: {e}", exc_info=True)
            return {
                'valid': False,
                'confidence': 'uncertain',
                'reason': f'Verification error: {str(e)}'
            }
    
    def _word_in_text(self, word, text):
        """
        Check if a word exists in text as a complete word (not as part of another word)

        Args:
            word: The word to search for (case-insensitive)
            text: The text to search in (should already be lowercased)

        Returns:
            bool: True if word is found as a complete word
        """
        # Use word boundary regex to match only complete words
        # \b ensures we match whole words only
        pattern = r'\b' + re.escape(word.lower()) + r'\b'
        return bool(re.search(pattern, text.lower()))

    def _string_similarity(self, s1, s2):
        """
        Calculate similarity ratio between two strings for name matching.
        Returns a value between 0 and 1, where 1 means identical.

        Uses Levenshtein edit distance - counts minimum insertions, deletions,
        and substitutions needed to transform one string to another.

        This catches cases like "Catney" vs "McArtney" which share characters
        but are clearly different names (edit distance = 2: insert 'm', substitute 'c'->'r').

        Args:
            s1: First string
            s2: Second string

        Returns:
            float: Similarity ratio between 0 and 1
        """
        if not s1 or not s2:
            return 0.0
        if s1 == s2:
            return 1.0

        # Levenshtein distance using dynamic programming
        len1, len2 = len(s1), len(s2)

        # Create distance matrix
        dp = [[0] * (len2 + 1) for _ in range(len1 + 1)]

        # Initialize base cases
        for i in range(len1 + 1):
            dp[i][0] = i
        for j in range(len2 + 1):
            dp[0][j] = j

        # Fill in the matrix
        for i in range(1, len1 + 1):
            for j in range(1, len2 + 1):
                if s1[i-1] == s2[j-1]:
                    dp[i][j] = dp[i-1][j-1]  # No operation needed
                else:
                    dp[i][j] = 1 + min(
                        dp[i-1][j],      # Deletion
                        dp[i][j-1],      # Insertion
                        dp[i-1][j-1]     # Substitution
                    )

        edit_distance = dp[len1][len2]
        # Similarity is 1 - (edit_distance / max_length)
        max_len = max(len1, len2)
        return 1.0 - (edit_distance / max_len)


    def _opensearch(self, query):
        """Return candidate Wikipedia article URLs for a query via the
        OpenSearch API, honoring the 7-day search cache.

        Sets self.last_made_api_call. Returns a list of URLs (possibly empty);
        a genuine empty result is cached so bulk re-runs skip it. Returns None
        on a transient request failure so the caller can tell 'no results'
        apart from 'lookup failed' (and we avoid caching the failure).
        """
        if not self.force_refresh:
            cached = self._load_search_from_cache(query)
            if cached is not None:
                self.last_made_api_call = False
                return cached

        self.last_made_api_call = True
        self.rate_limit()
        try:
            response = self.session.get(
                "https://en.wikipedia.org/w/api.php",
                params={'action': 'opensearch', 'search': query,
                        'limit': 5, 'namespace': 0, 'format': 'json'},
                timeout=10)
        except requests.RequestException as e:
            logger.warning(f"  OpenSearch request failed for {query!r}: {e}")
            return None

        if response.status_code != 200:
            return None

        data = response.json()
        urls = data[3] if len(data) >= 4 and data[3] else []
        if not self.force_refresh:
            self._save_search_to_cache(query, urls)
        return urls

    def search_wikipedia(self, performer_name, context):
        """
        Search Wikipedia for a performer

        Args:
            performer_name: Name to search for
            context: Dict with additional info for verification

        Returns:
            Wikipedia URL if found with reasonable confidence, None otherwise
        """
        try:
            # Search the nickname-stripped (more canonical) form first, then
            # the name as stored. e.g. '“Brother” Jack McDuff' searches
            # 'Jack McDuff' first so the canonical article is found and
            # preferred over an album/redirect that the decorated name returns.
            queries = []
            stripped = self._strip_nickname(performer_name)
            if stripped.lower() != performer_name.lower():
                queries.append(stripped)
            queries.append(performer_name)

            candidate_urls = []
            any_api_call = False
            for query in queries:
                urls = self._opensearch(query)
                any_api_call = any_api_call or self.last_made_api_call
                for url in (urls or []):
                    if url not in candidate_urls:
                        candidate_urls.append(url)
            self.last_made_api_call = any_api_call

            if not candidate_urls:
                logger.debug("  No Wikipedia search results")
                return None

            # Verify each candidate until we find a good match
            # Note: verify_wikipedia_reference will also set last_made_api_call
            for url in candidate_urls[:8]:
                verification = self.verify_wikipedia_reference(performer_name, url, context)
                logger.debug(f"  Checked {url}: valid={verification['valid']}, confidence={verification['confidence']}, score={verification.get('score', 0)}, reason={verification['reason']}")
                if verification['valid']:
                    logger.debug(f"  Found Wikipedia: {url} (confidence: {verification['confidence']}, score: {verification.get('score', 0)})")
                    logger.debug(f"    Reason: {verification['reason']}")
                    return url

            # No candidate verified - cache empty under the stored name so a
            # re-run skips re-verifying (preserves bulk-run speed).
            self._save_search_to_cache(performer_name, [])
            return None

        except Exception as e:
            logger.error(f"Error searching Wikipedia for {performer_name}: {e}")
            return None