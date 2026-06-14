#!/usr/bin/env python3
"""
Fetch Artist Images Script
Fetches images for jazz artists from Wikipedia and Discogs, storing them in the database.

ARCHITECTURE:
This script separates database operations from slow API calls to prevent connection timeouts:
1. Fetch performer data (short DB connection)
2. Fetch images from external APIs (no DB connection)
3. Save results to database (short DB connection)

Usage:
    python fetch_artist_images.py --name "Miles Davis"
    python fetch_artist_images.py --id <uuid>
    python fetch_artist_images.py --name "John Coltrane" --dry-run
    python fetch_artist_images.py --name "Thelonious Monk" --debug
"""

import sys
import argparse
import logging
import requests
import json
import re
from typing import Optional, Dict, Any, List
from urllib.parse import quote, urljoin, unquote
from dataclasses import dataclass
import time
from pathlib import Path
from datetime import datetime

# Add the backend directory to the path so imports work from any directory
backend_dir = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(backend_dir))

# Third-party imports
from bs4 import BeautifulSoup

# Import our database utilities
from db_utils import (
    get_db_connection,
    find_performer_by_name,
    find_performer_by_id,
    update_performer_external_references,
    normalize_apostrophes,
    get_performer_images
)

# Import Wikipedia utilities with caching
from integrations.wikipedia.utils import WikipediaSearcher
from core.http_client import make_session

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('log/fetch_artist_images.log')
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class ImageData:
    """Data class for image information."""
    url: str
    source: str
    thumbnail_url: Optional[str] = None
    source_identifier: Optional[str] = None
    source_page_url: Optional[str] = None
    license_type: Optional[str] = None
    license_url: Optional[str] = None
    attribution: Optional[str] = None
    width: Optional[int] = None
    height: Optional[int] = None


class ImageFetcher:
    """Handles fetching images from various sources."""
    
    def __init__(self, dry_run: bool = False, debug: bool = False, force_refresh: bool = False):
        self.dry_run = dry_run
        self.debug = debug
        self.force_refresh = force_refresh
        self.session = make_session(accept_json=False)

        # Initialize Wikipedia searcher with caching
        self.wiki_searcher = WikipediaSearcher(
            cache_days=7,
            force_refresh=force_refresh
        )
        
        # Track whether the last fetch made API calls
        self.last_fetch_made_api_call = False
        
        if debug:
            logger.setLevel(logging.DEBUG)
    
    def fetch_wikipedia_image(self, artist_name: str, wikipedia_url: Optional[str] = None) -> Optional[ImageData]:
        """
        Fetch image from Wikipedia for an artist.
        Uses WikipediaSearcher for page lookup and rate limiting.
        
        Args:
            artist_name: Name of the artist to search for
            wikipedia_url: Optional Wikipedia URL from database (skips search if provided)
        
        Returns:
            ImageData object or None
        """
        # Reset API call tracking
        self.last_fetch_made_api_call = False
        
        try:
            page_title = None
            page_url = wikipedia_url
            
            # If we have a Wikipedia URL from the database, extract the page title
            if wikipedia_url:
                logger.debug(f"Using Wikipedia URL from database: {wikipedia_url}")
                # Extract page title from URL like https://en.wikipedia.org/wiki/Miles_Davis
                match = re.search(r'/wiki/(.+)$', wikipedia_url)
                if match:
                    page_title = unquote(match.group(1))
                    logger.debug(f"Extracted page title: {page_title}")
                else:
                    logger.warning(f"Could not extract page title from URL: {wikipedia_url}")
                    wikipedia_url = None  # Fall back to search
            
            # If no Wikipedia URL provided, use WikipediaSearcher to find the page
            if not wikipedia_url:
                logger.debug(f"Searching Wikipedia for {artist_name}...")
                
                # Use WikipediaSearcher which handles caching and rate limiting
                # Provide minimal context for verification
                context = {
                    'birth_date': None,
                    'death_date': None,
                    'sample_songs': []
                }
                
                page_url = self.wiki_searcher.search_wikipedia(artist_name, context)
                
                # Track if API call was made (WikipediaSearcher sets this)
                if self.wiki_searcher.last_made_api_call:
                    self.last_fetch_made_api_call = True
                
                if not page_url:
                    logger.debug(f"No Wikipedia page found for {artist_name}")
                    return None
                
                # Extract page title from the URL we found
                match = re.search(r'/wiki/(.+)$', page_url)
                if match:
                    page_title = unquote(match.group(1))
                    logger.debug(f"Found Wikipedia page: {page_title}")
                else:
                    logger.warning(f"Could not extract page title from found URL: {page_url}")
                    return None
            
            # Step 2: Get the main image from the page using Wikipedia API
            # Note: This must be an API call - there's no way around it to get the image
            self.last_fetch_made_api_call = True
            
            # Use WikipediaSearcher's rate limiting
            self.wiki_searcher.rate_limit()
            
            api_url = "https://en.wikipedia.org/w/api.php"
            image_params = {
                'action': 'query',
                'format': 'json',
                'titles': page_title,
                'prop': 'pageimages|pageterms|info',
                'piprop': 'original|thumbnail',
                'pithumbsize': 500,
                'inprop': 'url'
            }
            
            response = self.wiki_searcher.session.get(api_url, params=image_params, timeout=10)
            response.raise_for_status()
            page_data = response.json()
            
            pages = page_data.get('query', {}).get('pages', {})
            if not pages:
                logger.debug(f"No page data found for {page_title}")
                return None
            
            page = next(iter(pages.values()))
            
            # Get image URL
            if 'original' not in page:
                # API didn't return image - try HTML scraping fallback
                logger.debug(f"No image from pageimages API, trying HTML scraping fallback...")
                page_url = page.get('fullurl', page_url or f"https://en.wikipedia.org/wiki/{quote(page_title)}")
                return self._scrape_wikipedia_page_for_image(page_title, page_url, artist_name)
            
            image_url = page['original']['source']
            thumbnail_url = page.get('thumbnail', {}).get('source')
            page_url = page.get('fullurl', page_url or f"https://en.wikipedia.org/wiki/{quote(page_title)}")
            
            # Step 3: Get image details for licensing (another required API call)
            # Use WikipediaSearcher's rate limiting
            self.wiki_searcher.rate_limit()
            
            image_filename = image_url.split('/')[-1]
            license_params = {
                'action': 'query',
                'format': 'json',
                'titles': f'File:{image_filename}',
                'prop': 'imageinfo',
                'iiprop': 'extmetadata|size'
            }
            
            response = self.wiki_searcher.session.get(api_url, params=license_params, timeout=10)
            response.raise_for_status()
            license_data = response.json()
            
            # Extract license information
            license_type = 'unknown'
            license_url = None
            attribution = None
            width = page['original'].get('width')
            height = page['original'].get('height')
            
            if license_data.get('query', {}).get('pages'):
                file_page = next(iter(license_data['query']['pages'].values()))
                if 'imageinfo' in file_page and file_page['imageinfo']:
                    imageinfo = file_page['imageinfo'][0]
                    extmetadata = imageinfo.get('extmetadata', {})
                    
                    # Get license
                    if 'License' in extmetadata:
                        license_type = extmetadata['License'].get('value', 'unknown')
                    if 'LicenseUrl' in extmetadata:
                        license_url = extmetadata['LicenseUrl'].get('value')
                    
                    # Get attribution
                    if 'Artist' in extmetadata:
                        attribution = extmetadata['Artist'].get('value')
                    elif 'Credit' in extmetadata:
                        attribution = extmetadata['Credit'].get('value')
                    
                    # Get actual dimensions
                    if 'size' in imageinfo:
                        width = imageinfo.get('width', width)
                        height = imageinfo.get('height', height)
            
            # Normalize license type
            license_type_normalized = self._normalize_license(license_type)
            
            image_data = ImageData(
                url=image_url,
                thumbnail_url=thumbnail_url,
                source='wikipedia',
                source_identifier=page_title,
                source_page_url=page_url,
                license_type=license_type_normalized,
                license_url=license_url,
                attribution=attribution,
                width=width,
                height=height
            )
            
            logger.debug(f"✓ Found Wikipedia image: {image_url}")
            logger.debug(f"Image details: {image_data}")
            
            return image_data
            
        except requests.RequestException as e:
            logger.error(f"Error fetching from Wikipedia: {e}")
            if self.debug:
                logger.exception(e)
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching Wikipedia image: {e}")
            if self.debug:
                logger.exception(e)
            return None
    
    def fetch_discogs_image(self, artist_name: str) -> Optional[ImageData]:
        """
        Fetch image from Discogs for an artist.
        
        Args:
            artist_name: Name of the artist to search for
        
        Returns:
            ImageData object or None
        """
        # Placeholder - Discogs requires authentication
        logger.debug(f"Discogs image fetch not yet implemented for {artist_name}")
        return None
    
    def _normalize_license(self, license_str: str) -> str:
        """
        Normalize license type to standard values.
        
        Args:
            license_str: Raw license string
        
        Returns:
            Normalized license type
        """
        if not license_str or license_str == 'unknown':
            return 'unknown'
        
        license_lower = license_str.lower()
        
        if 'public domain' in license_lower or 'pd' in license_lower:
            return 'public_domain'
        elif 'cc0' in license_lower:
            return 'cc0'
        elif 'cc-by-sa' in license_lower or 'cc by-sa' in license_lower:
            return 'cc_by_sa'
        elif 'cc-by' in license_lower or 'cc by' in license_lower:
            return 'cc_by'
        elif 'fair use' in license_lower:
            return 'fair_use'
        else:
            return 'other'
    
    def _scrape_wikipedia_page_for_image(self, page_title: str, page_url: str, 
                                         artist_name: str) -> Optional[ImageData]:
        """
        Scrape Wikipedia page HTML to find images in the infobox.
        This is a fallback when the pageimages API doesn't return an image.
        
        Args:
            page_title: Wikipedia page title
            page_url: Full Wikipedia page URL
            artist_name: Artist name (for logging)
        
        Returns:
            ImageData object or None
        """
        try:
            logger.debug(f"Scraping page HTML: {page_url}")
            
            # Fetch the page HTML (using cache if available)
            html_content = self.wiki_searcher._fetch_wikipedia_page(page_url)
            
            if not html_content:
                logger.info(f"Failed to fetch Wikipedia page for {artist_name}")
                return None
            
            # Parse with BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Look for image in the infobox
            infobox = soup.find('table', {'class': 'infobox'})
            if not infobox:
                logger.debug("No infobox found on page")
                logger.debug(f"No image found on Wikipedia page for {artist_name}")
                return None
            
            # Find the first image in the infobox
            img_tag = infobox.find('img')
            if not img_tag or not img_tag.get('src'):
                logger.debug("No image found in infobox")
                logger.debug(f"No image found on Wikipedia page for {artist_name}")
                return None
            
            # Extract image URL
            img_src = img_tag.get('src')
            if img_src.startswith('//'):
                img_src = 'https:' + img_src
            elif img_src.startswith('/'):
                img_src = 'https://en.wikipedia.org' + img_src
            
            logger.debug(f"Found image in infobox: {img_src}")
            
            # Get the full-resolution image URL
            # Wikipedia thumbnail URLs: .../thumb/.../filename.jpg/220px-filename.jpg
            # We want the original: .../filename.jpg
            full_img_url = img_src
            if '/thumb/' in img_src:
                # Remove /thumb/ and size suffix to get original
                full_img_url = re.sub(r'/thumb/', '/', img_src)
                # Extract actual filename and rebuild URL
                match = re.search(r'/([^/]+)$', img_src)
                if match:
                    filename = match.group(1)
                    # Remove size prefix like "220px-"
                    filename = re.sub(r'^\d+px-', '', filename)
                    full_img_url = re.sub(r'/[^/]+$', '/' + filename, full_img_url)
            
            logger.debug(f"Full resolution URL: {full_img_url}")
            
            # Extract image filename for license lookup
            image_filename = full_img_url.split('/')[-1]
            
            # Try to get license information via API
            api_url = "https://en.wikipedia.org/w/api.php"
            license_params = {
                'action': 'query',
                'format': 'json',
                'titles': f'File:{image_filename}',
                'prop': 'imageinfo',
                'iiprop': 'extmetadata|size|url'
            }
            
            license_type = 'unknown'
            license_url = None
            attribution = None
            width = img_tag.get('width')
            height = img_tag.get('height')
            
            try:
                response = self.session.get(api_url, params=license_params, timeout=10)
                response.raise_for_status()
                license_data = response.json()
                
                if license_data.get('query', {}).get('pages'):
                    file_page = next(iter(license_data['query']['pages'].values()))
                    if 'imageinfo' in file_page and file_page['imageinfo']:
                        imageinfo = file_page['imageinfo'][0]
                        
                        # Use the full URL from the API if available
                        if 'url' in imageinfo:
                            full_img_url = imageinfo['url']
                            logger.debug(f"Updated to API URL: {full_img_url}")
                        
                        # Extract metadata
                        extmetadata = imageinfo.get('extmetadata', {})
                        
                        if 'License' in extmetadata:
                            license_type = extmetadata['License'].get('value', 'unknown')
                        if 'LicenseUrl' in extmetadata:
                            license_url = extmetadata['LicenseUrl'].get('value')
                        if 'Artist' in extmetadata:
                            attribution = extmetadata['Artist'].get('value')
                        elif 'Credit' in extmetadata:
                            attribution = extmetadata['Credit'].get('value')
                        
                        # Get dimensions
                        if 'width' in imageinfo:
                            width = imageinfo['width']
                        if 'height' in imageinfo:
                            height = imageinfo['height']
            except Exception as e:
                logger.debug(f"Could not fetch license info: {e}")
            
            # Normalize license type
            license_type_normalized = self._normalize_license(license_type)
            
            image_data = ImageData(
                url=full_img_url,
                thumbnail_url=img_src,
                source='wikipedia',
                source_identifier=page_title,
                source_page_url=page_url,
                license_type=license_type_normalized,
                license_url=license_url,
                attribution=attribution,
                width=width,
                height=height
            )
            
            logger.debug(f"✓ Found Wikipedia image via HTML scraping: {full_img_url}")
            logger.debug(f"Image details: {image_data}")
            
            return image_data
            
        except Exception as e:
            logger.error(f"Error scraping Wikipedia page for image: {e}")
            if self.debug:
                logger.exception(e)
            logger.debug(f"No image found on Wikipedia page for {artist_name}")
            return None


class ImageDatabaseManager:
    """Handles database operations for images - separated from API fetching."""
    
    def __init__(self, dry_run: bool = False, debug: bool = False):
        self.dry_run = dry_run
        self.debug = debug
    
    def get_existing_images(self, performer_id: str) -> List[Dict[str, Any]]:
        """
        Get existing images for a performer.
        Uses a short-lived database connection.
        
        Args:
            performer_id: UUID of the performer
        
        Returns:
            List of existing image records
        """
        try:
            existing_images = get_performer_images(performer_id)
            if existing_images:
                logger.debug(f"Performer already has {len(existing_images)} image(s)")
                for img in existing_images:
                    logger.debug(f"  - {img['source']}: {img['url']}")
            return existing_images or []
        except Exception as e:
            logger.error(f"Error getting existing images: {e}")
            if self.debug:
                logger.exception(e)
            return []
    

    def is_duplicate_image(self, performer_id: str, image_url: str) -> bool:
        """
        Check if an image URL is already associated with this performer.
        
        Args:
            performer_id: UUID of the performer
            image_url: URL of the image to check
        
        Returns:
            True if this image is already linked to this performer, False otherwise
        """
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    # Check if this URL is already linked to this performer
                    query = """
                        SELECT 1 
                        FROM images i
                        JOIN artist_images ai ON i.id = ai.image_id
                        WHERE ai.performer_id = %s AND i.url = %s
                        LIMIT 1
                    """
                    cur.execute(query, (performer_id, image_url))
                    result = cur.fetchone()
                    return result is not None
        except Exception as e:
            logger.error(f"Error checking for duplicate image: {e}")
            if self.debug:
                logger.exception(e)
            # On error, assume it's not a duplicate to avoid blocking new images
            return False
    
    def save_image(self, performer_id: str, image_data: ImageData, 
                   is_primary: bool = False, display_order: int = 0) -> bool:
        """
        Save an image to the database with a short-lived connection.
        Skips saving if the image is already linked to this performer.
        
        Args:
            performer_id: UUID of the performer
            image_data: ImageData object containing image information
            is_primary: Whether this is the primary image
            display_order: Display order for the image
        
        Returns:
            True if saved successfully, False if duplicate or error
        """
        # First check if this is a duplicate
        if self.is_duplicate_image(performer_id, image_data.url):
            logger.debug(f"⊘ Image already linked to performer, skipping: {image_data.url}")
            return False
        
        if self.dry_run:
            logger.debug(f"[DRY RUN] Would save image: {image_data.url}")
            logger.debug(f"  Source: {image_data.source}")
            logger.debug(f"  Primary: {is_primary}, Order: {display_order}")
            return True
        
        try:
            # Use a short-lived connection just for this operation
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    # Check if image URL already exists in images table
                    check_query = "SELECT id FROM images WHERE url = %s"
                    cur.execute(check_query, (image_data.url,))
                    existing = cur.fetchone()
                    
                    if existing:
                        image_id = existing['id']
                        logger.debug(f"Image already exists in database: {image_id}")
                    else:
                        # Insert new image
                        insert_image_query = """
                            INSERT INTO images (
                                url, source, source_identifier, license_type, license_url,
                                attribution, width, height, thumbnail_url, source_page_url
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                            )
                            RETURNING id
                        """
                        cur.execute(insert_image_query, (
                            image_data.url,
                            image_data.source,
                            image_data.source_identifier,
                            image_data.license_type,
                            image_data.license_url,
                            image_data.attribution,
                            image_data.width,
                            image_data.height,
                            image_data.thumbnail_url,
                            image_data.source_page_url
                        ))
                        image_id = cur.fetchone()['id']
                        logger.debug(f"✓ Inserted new image: {image_id}")
                    
                    # Check if relationship already exists (extra safety check)
                    check_rel_query = """
                        SELECT 1 FROM artist_images 
                        WHERE performer_id = %s AND image_id = %s
                    """
                    cur.execute(check_rel_query, (performer_id, image_id))
                    
                    if cur.fetchone():
                        logger.warning(f"⊘ Image already linked to performer (shouldn't happen - duplicate check failed)")
                        return False
                    else:
                        # Link image to performer
                        insert_rel_query = """
                            INSERT INTO artist_images (
                                performer_id, image_id, is_primary, display_order
                            ) VALUES (%s, %s, %s, %s)
                        """
                        cur.execute(insert_rel_query, (
                            performer_id, image_id, is_primary, display_order
                        ))
                        logger.debug(f"✓ Linked image to performer")
                    
                    # Commit happens automatically when exiting context
                    return True
            
            # Connection is now closed
                    
        except Exception as e:
            logger.error(f"Error saving image to database: {e}")
            if self.debug:
                logger.exception(e)
            return False
    
    def update_external_references(self, performer_id: str, 
                                   external_refs: Dict[str, str]) -> bool:
        """
        Update performer external references.
        Uses a short-lived database connection.
        
        Args:
            performer_id: UUID of the performer
            external_refs: Dictionary of external reference data
        
        Returns:
            True if updated successfully, False otherwise
        """
        try:
            update_performer_external_references(performer_id, external_refs, self.dry_run)
            return True
        except Exception as e:
            logger.error(f"Error updating external references: {e}")
            if self.debug:
                logger.exception(e)
            return False


def process_performer(performer: Dict[str, Any], fetcher: ImageFetcher, 
                     db_manager: ImageDatabaseManager) -> Dict[str, Any]:
    """
    Process a single performer: fetch images and save to database.
    This function clearly separates API calls from database operations.
    
    Args:
        performer: Performer record from database
        fetcher: ImageFetcher instance for API calls
        db_manager: ImageDatabaseManager for database operations
    
    Returns:
        Dict with processing results including images_added, made_api_calls, sources, etc.
    """
    performer_id = str(performer['id'])
    performer_name = performer['name']
    images_added = 0
    made_api_calls = False
    sources = []
    
    logger.debug(f"\n{'='*60}")
    logger.debug(f"Fetching images for: {performer_name}")
    logger.debug(f"{'='*60}")
    
    # STEP 1: Get existing images ONCE (single short DB connection)
    existing_images = db_manager.get_existing_images(performer_id)
    logger.debug(f"  Found {len(existing_images)} existing image(s)")
    
    # Check in memory if we already have images from each source
    has_wikipedia_image = any(img['source'] == 'wikipedia' for img in existing_images)
    has_discogs_image = any(img['source'] == 'discogs' for img in existing_images)
    
    if has_wikipedia_image and has_discogs_image:
        logger.debug(f"  Performer already has images from both sources (skipping all API calls)")
        return {
            'performer_id': performer_id,
            'performer_name': performer_name,
            'images_added': 0,
            'made_api_calls': False,
            'disposition': 'no new images (already exist)',
            'sources': []
        }
    
    # STEP 2: Fetch images from external APIs (NO database connection, only if needed)
    wikipedia_url = performer.get('wikipedia_url')
    
    # Fetch Wikipedia image only if we don't already have one
    wiki_image = None
    if not has_wikipedia_image:
        logger.debug(f"  Fetching Wikipedia image...")
        wiki_image = fetcher.fetch_wikipedia_image(performer_name, wikipedia_url=wikipedia_url)
        if fetcher.last_fetch_made_api_call:
            made_api_calls = True
            logger.debug(f"  Wikipedia API call was made")
    else:
        logger.debug(f"  Skipping Wikipedia fetch (already have Wikipedia image)")
    
    # Fetch Discogs image only if we don't already have one
    discogs_image = None
    if not has_discogs_image:
        logger.debug(f"  Fetching Discogs image...")
        discogs_image = fetcher.fetch_discogs_image(performer_name)
        if discogs_image:
            made_api_calls = True
            logger.debug(f"  Discogs API call was made")
    else:
        logger.debug(f"  Skipping Discogs fetch (already have Discogs image)")
    
    # STEP 3: Save results to database (short DB connections for each save operation)
    if wiki_image:
        # Save the image (will skip if duplicate)
        if db_manager.save_image(performer_id, wiki_image, 
                                is_primary=not existing_images, display_order=0):
            images_added += 1
            sources.append('wikipedia')
            logger.debug(f"  ✓ Added Wikipedia image")
        else:
            logger.debug(f"  - Wikipedia image already exists (skipped)")
    elif not has_wikipedia_image:
        logger.debug(f"  - No Wikipedia image found")
    
    if discogs_image:
        if db_manager.save_image(performer_id, discogs_image, 
                                is_primary=False, display_order=1):
            images_added += 1
            sources.append('discogs')
            logger.debug(f"  ✓ Added Discogs image")
        else:
            logger.debug(f"  - Discogs image already exists (skipped)")
    elif not has_discogs_image:
        logger.debug(f"  - No Discogs image found")
    
    logger.debug(f"\n{'='*60}")
    logger.debug(f"✓ Added {images_added} new image(s) for {performer_name}")
    logger.debug(f"{'='*60}\n")
    
    # Determine disposition for single-line logging
    if images_added > 0:
        disposition = f"added {images_added} ({', '.join(sources)})"
    elif has_wikipedia_image or has_discogs_image or wiki_image or discogs_image:
        disposition = "no new images (already exist)"
    else:
        disposition = "no images found"
    
    return {
        'performer_id': performer_id,
        'performer_name': performer_name,
        'images_added': images_added,
        'made_api_calls': made_api_calls,
        'disposition': disposition,
        'sources': sources
    }


def get_all_performers() -> List[Dict[str, Any]]:
    """
    Get all performers from the database.
    Uses a short-lived connection.
    
    Returns:
        List of performer records
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, biography, birth_date, death_date, wikipedia_url
                FROM performers
                ORDER BY name
            """)
            performers = cur.fetchall()
    # Connection is now closed
    return performers


def save_images_added_log(images_added_list: List[Dict[str, Any]]):
    """Save list of performers with newly added images to JSON log file"""
    if not images_added_list:
        return
    
    log_dir = Path('log')
    log_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f'images_added_{timestamp}.json'
    
    try:
        with open(log_file, 'w', encoding='utf-8') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'total_performers': len(images_added_list),
                'total_images': sum(item['images_added'] for item in images_added_list),
                'performers': images_added_list
            }, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Images added logged to: {log_file}")
    except Exception as e:
        logger.error(f"Failed to save images added log: {e}")


def print_summary(stats: Dict[str, int]):
    """Print processing summary"""
    logger.info("")
    logger.info("="*80)
    logger.info("PROCESSING SUMMARY")
    logger.info("="*80)
    logger.info(f"Performers processed:   {stats['performers_processed']}")
    logger.info(f"Images added:           {stats['images_added']}")
    logger.info(f"  - Wikipedia:          {stats['wikipedia_images']}")
    logger.info(f"  - Discogs:            {stats['discogs_images']}")
    logger.info(f"No new images:          {stats['no_new_images']}")
    logger.info(f"No images found:        {stats['no_images_found']}")
    logger.info(f"Errors:                 {stats['errors']}")
    logger.info("="*80)


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description='Fetch images for jazz artists from Wikipedia and Discogs',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Fetch images by artist name
    python fetch_artist_images.py --name "Miles Davis"
    
    # Fetch by performer ID
    python fetch_artist_images.py --id 123e4567-e89b-12d3-a456-426614174000
    
    # Dry run (don't save to database)
    python fetch_artist_images.py --name "John Coltrane" --dry-run
    
    # Force refresh Wikipedia pages (bypass cache)
    python fetch_artist_images.py --name "Miles Davis" --force-refresh
    
    # Enable debug logging
    python fetch_artist_images.py --name "Thelonious Monk" --debug
        """
    )
    
    # Required arguments (one of)
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument('--name', help='Artist name to search for')
    group.add_argument('--id', help='Performer UUID')
    
    # Optional arguments
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be done without making changes')
    parser.add_argument('--debug', action='store_true',
                       help='Enable debug logging')
    parser.add_argument('--force-refresh', action='store_true',
                       help='Bypass Wikipedia cache and fetch fresh data from Wikipedia')
    
    args = parser.parse_args()
    
    # Set logging level
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Initialize stats tracking
    stats = {
        'performers_processed': 0,
        'images_added': 0,
        'wikipedia_images': 0,
        'discogs_images': 0,
        'no_new_images': 0,
        'no_images_found': 0,
        'errors': 0
    }
    
    # Track performers with newly added images
    images_added_list = []
    
    logger.info("="*80)
    logger.info("FETCH ARTIST IMAGES")
    logger.info("="*80)
    
    if args.dry_run:
        logger.info("*** DRY RUN MODE - No database changes will be made ***")
    
    if args.force_refresh:
        logger.info("*** FORCE REFRESH MODE - Bypassing Wikipedia cache ***")
    
    # Create fetcher and database manager
    fetcher = ImageFetcher(dry_run=args.dry_run, debug=args.debug, force_refresh=args.force_refresh)
    db_manager = ImageDatabaseManager(dry_run=args.dry_run, debug=args.debug)
    
    # Determine which performers to process
    performers = []
    
    if args.name:
        artist_name = normalize_apostrophes(args.name)
        logger.debug(f"Searching for performer: {artist_name}")
        # Short DB connection to find performer
        performer = find_performer_by_name(artist_name)
        if not performer:
            logger.error(f"Performer not found: {artist_name}")
            sys.exit(1)
        performers = [performer]
        logger.debug(f"Found performer: {performer['name']} (ID: {performer['id']})")
    elif args.id:
        logger.debug(f"Looking up performer ID: {args.id}")
        # Short DB connection to find performer
        performer = find_performer_by_id(args.id)
        if not performer:
            logger.error(f"Performer not found with ID: {args.id}")
            sys.exit(1)
        performers = [performer]
        logger.debug(f"Found performer: {performer['name']} (ID: {performer['id']})")
    else:
        # Process all performers - short DB connection to get list
        logger.debug("No specific performer specified - processing all performers")
        performers = get_all_performers()
    
    logger.info(f"Found {len(performers)} performer(s) to process")
    logger.info("")
    
    # Process each performer
    # Note: Each call to process_performer() uses separate short-lived connections
    for i, performer in enumerate(performers, 1):
        logger.debug(f"Processing performer {i}/{len(performers)}")
        
        try:
            result = process_performer(performer, fetcher, db_manager)
            stats['performers_processed'] += 1
            
            # Update stats based on result
            stats['images_added'] += result['images_added']
            if 'wikipedia' in result['sources']:
                stats['wikipedia_images'] += 1
            if 'discogs' in result['sources']:
                stats['discogs_images'] += 1
            
            if result['images_added'] > 0:
                images_added_list.append({
                    'performer_id': result['performer_id'],
                    'performer_name': result['performer_name'],
                    'images_added': result['images_added'],
                    'sources': result['sources']
                })
            elif 'already exist' in result['disposition']:
                stats['no_new_images'] += 1
            elif 'no images found' in result['disposition']:
                stats['no_images_found'] += 1
            
            # Single-line INFO logging
            logger.info(f"Processing: {result['performer_name']} - {result['disposition']}")
            
            # Add delay between performers only if we made API calls
            if len(performers) > 1 and i < len(performers) and result['made_api_calls']:
                logger.debug(f"Waiting 2 seconds before next performer (API calls were made)...")
                time.sleep(2.0)
                
        except Exception as e:
            stats['errors'] += 1
            logger.error(f"Error processing {performer['name']}: {e}")
            if args.debug:
                logger.exception(e)
    
    # Save log file if we added images
    if images_added_list:
        save_images_added_log(images_added_list)
    
    # Print summary
    print_summary(stats)
    
    sys.exit(0 if stats['errors'] == 0 else 1)


if __name__ == '__main__':
    main()