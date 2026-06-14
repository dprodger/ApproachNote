"""
Add Wikipedia Image Script
Takes a Wikipedia image page URL, identifies the artist, and adds the image to the database.

Usage:
    python add_wikipedia_image.py "https://en.wikipedia.org/wiki/File:Miles_Davis_by_Palumbo_cropped.jpg"
    python add_wikipedia_image.py "https://en.wikipedia.org/wiki/File:Nina_Simone_1965.jpg" --dry-run
    python add_wikipedia_image.py "https://en.wikipedia.org/wiki/File:John_Coltrane_1963.jpg" --debug
"""

import sys
import argparse
import logging
import requests
import re
from typing import Optional, Dict, Any
from urllib.parse import quote, unquote, urlparse
import time

# Import our database utilities
from db_utils import (
    get_db_connection,
    find_performer_by_name,
    find_performer_by_id,
    get_performer_images
)
from core.http_client import make_session

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class WikipediaImageProcessor:
    """Processes Wikipedia image pages and links them to artists."""
    
    def __init__(self, dry_run: bool = False, debug: bool = False):
        self.dry_run = dry_run
        self.debug = debug
        self.session = make_session(accept_json=False)
        
        if debug:
            logger.setLevel(logging.DEBUG)
    
    def _normalize_license(self, license_text: str) -> str:
        """
        Normalize license type from Wikipedia metadata.
        
        Args:
            license_text: Raw license text from Wikipedia
            
        Returns:
            Normalized license type
        """
        if not license_text:
            return 'unknown'
        
        license_lower = license_text.lower()
        
        if 'cc-by-sa' in license_lower or 'cc by-sa' in license_lower:
            return 'CC-BY-SA'
        elif 'cc-by' in license_lower or 'cc by' in license_lower:
            return 'CC-BY'
        elif 'public domain' in license_lower or 'pd' in license_lower:
            return 'Public Domain'
        elif 'cc0' in license_lower:
            return 'CC0'
        elif 'gfdl' in license_lower:
            return 'GFDL'
        else:
            return license_text
    
    def extract_filename_from_url(self, url: str) -> Optional[str]:
        """
        Extract the filename from a Wikipedia image page URL.
        
        Args:
            url: Wikipedia image page URL
            
        Returns:
            Filename or None if not a valid Wikipedia image URL
        """
        try:
            # Handle multiple formats:
            # https://en.wikipedia.org/wiki/File:Name.jpg
            # https://commons.wikimedia.org/wiki/File:Name.jpg
            # https://en.wikipedia.org/wiki/Article#/media/File:Name.jpg (media viewer)
            
            # Check for #/media/File: format first (media viewer)
            if '#/media/File:' in url or '#/media/Image:' in url:
                match = re.search(r'#/media/(?:File|Image):(.+)$', url)
                if match:
                    return unquote(match.group(1))
            
            # Standard /wiki/File: format
            parsed = urlparse(url)
            path = parsed.path
            
            if '/wiki/File:' in path or '/wiki/Image:' in path:
                # Extract everything after File: or Image:
                match = re.search(r'/wiki/(?:File|Image):(.+)$', path)
                if match:
                    return unquote(match.group(1))
            
            logger.error(f"URL does not appear to be a Wikipedia image page: {url}")
            return None
            
        except Exception as e:
            logger.error(f"Error parsing URL: {e}")
            return None
    
    def fetch_image_metadata(self, filename: str) -> Optional[Dict[str, Any]]:
        """
        Fetch metadata for a Wikipedia/Commons image file.
        
        Args:
            filename: Image filename (e.g., "Miles_Davis_1955.jpg")
            
        Returns:
            Dictionary with image metadata or None
        """
        try:
            logger.info(f"Fetching metadata for: {filename}")
            
            # Try Commons first, then Wikipedia
            for api_url in [
                "https://commons.wikimedia.org/w/api.php",
                "https://en.wikipedia.org/w/api.php"
            ]:
                params = {
                    'action': 'query',
                    'format': 'json',
                    'titles': f'File:{filename}',
                    'prop': 'imageinfo|categories',
                    'iiprop': 'extmetadata|size|url',
                    'iiurlwidth': 500
                }
                
                response = self.session.get(api_url, params=params, timeout=10)
                response.raise_for_status()
                data = response.json()
                
                pages = data.get('query', {}).get('pages', {})
                if not pages:
                    continue
                
                page = next(iter(pages.values()))
                
                # Check if page exists (not missing)
                if 'missing' in page:
                    continue
                
                # Get image info
                if 'imageinfo' not in page or not page['imageinfo']:
                    continue
                
                imageinfo = page['imageinfo'][0]
                extmetadata = imageinfo.get('extmetadata', {})
                
                # Extract metadata
                image_url = imageinfo.get('url')
                thumbnail_url = imageinfo.get('thumburl')
                width = imageinfo.get('width')
                height = imageinfo.get('height')
                
                # Get license
                license_type = 'unknown'
                license_url = None
                if 'License' in extmetadata:
                    license_type = extmetadata['License'].get('value', 'unknown')
                if 'LicenseUrl' in extmetadata:
                    license_url = extmetadata['LicenseUrl'].get('value')
                
                # Get attribution
                attribution = None
                if 'Artist' in extmetadata:
                    attribution = extmetadata['Artist'].get('value')
                elif 'Credit' in extmetadata:
                    attribution = extmetadata['Credit'].get('value')
                
                # Get categories to help identify the subject
                categories = []
                if 'categories' in page:
                    categories = [cat['title'].replace('Category:', '') 
                                for cat in page['categories']]
                
                # Get page URL
                source_page_url = f"https://commons.wikimedia.org/wiki/File:{quote(filename)}" if 'commons' in api_url else f"https://en.wikipedia.org/wiki/File:{quote(filename)}"
                
                metadata = {
                    'url': image_url,
                    'thumbnail_url': thumbnail_url,
                    'source': 'wikipedia',
                    'source_identifier': filename,
                    'source_page_url': source_page_url,
                    'license_type': self._normalize_license(license_type),
                    'license_url': license_url,
                    'attribution': attribution,
                    'width': width,
                    'height': height,
                    'categories': categories
                }
                
                logger.info(f"✓ Found image metadata")
                logger.debug(f"Metadata: {metadata}")
                
                return metadata
            
            logger.error(f"Image not found on Commons or Wikipedia: {filename}")
            return None
            
        except requests.RequestException as e:
            logger.error(f"Error fetching image metadata: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching metadata: {e}")
            if self.debug:
                logger.exception(e)
            return None
    
    def identify_artist_from_metadata(self, filename: str, metadata: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Try to identify the artist from the image filename and metadata.
        
        Args:
            filename: Image filename
            metadata: Image metadata including categories
            
        Returns:
            Performer record from database or None
        """
        try:
            logger.info("Attempting to identify artist...")
            
            # Strategy 1: Parse artist name from filename
            # Common patterns:
            # "Miles_Davis_1955.jpg"
            # "Nina_Simone_by_Photographer.jpg"
            # "John_Coltrane_performing.jpg"
            
            # Remove file extension
            name_part = filename.rsplit('.', 1)[0]
            
            # Remove common suffixes
            name_part = re.sub(r'_by_\w+', '', name_part)
            name_part = re.sub(r'_\d{4}', '', name_part)  # Remove years
            name_part = re.sub(r'_performing', '', name_part, flags=re.IGNORECASE)
            name_part = re.sub(r'_cropped', '', name_part, flags=re.IGNORECASE)
            name_part = re.sub(r'_portrait', '', name_part, flags=re.IGNORECASE)
            
            # Convert underscores to spaces
            artist_name = name_part.replace('_', ' ').strip()
            
            logger.debug(f"Extracted potential artist name from filename: {artist_name}")
            
            # Try to find in database
            performer = find_performer_by_name(artist_name)
            if performer:
                logger.info(f"✓ Found artist in database: {performer['name']}")
                return performer
            
            # Strategy 2: Look at categories for hints
            if metadata.get('categories'):
                logger.debug(f"Checking categories: {metadata['categories'][:5]}")
                
                # Look for categories that might contain artist names
                for category in metadata['categories']:
                    # Categories like "Miles Davis" or "Photographs of Miles Davis"
                    if 'photograph' in category.lower() or 'portrait' in category.lower():
                        # Extract name
                        cat_name = re.sub(r'photographs? of\s+', '', category, flags=re.IGNORECASE)
                        cat_name = re.sub(r'portraits? of\s+', '', cat_name, flags=re.IGNORECASE)
                        cat_name = cat_name.strip()
                        
                        if cat_name:
                            performer = find_performer_by_name(cat_name)
                            if performer:
                                logger.info(f"✓ Found artist from category '{category}': {performer['name']}")
                                return performer
            
            logger.warning(f"Could not identify artist from filename: {artist_name}")
            logger.info("Possible artist name extracted: " + artist_name)
            logger.info("Please verify and add manually if needed")
            
            return None
            
        except Exception as e:
            logger.error(f"Error identifying artist: {e}")
            if self.debug:
                logger.exception(e)
            return None
    
    def save_image_to_db(
        self,
        performer_id: str,
        image_data: Dict[str, Any],
        is_primary: bool = False,
        display_order: int = 0
    ) -> bool:
        """
        Save an image to the database and link it to a performer.
        
        Args:
            performer_id: UUID of the performer
            image_data: Dictionary containing image metadata
            is_primary: Whether this should be the primary image
            display_order: Display order for this image
            
        Returns:
            True if successful, False otherwise
        """
        if self.dry_run:
            logger.info(f"[DRY RUN] Would save image to database:")
            logger.info(f"  URL: {image_data.get('url')}")
            logger.info(f"  Source: {image_data.get('source')}")
            logger.info(f"  License: {image_data.get('license_type')}")
            logger.info(f"  Primary: {is_primary}")
            return True
        
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    # First, check if image already exists
                    check_query = """
                        SELECT id FROM images 
                        WHERE source = %s AND source_identifier = %s AND url = %s
                    """
                    cur.execute(check_query, (
                        image_data['source'],
                        image_data.get('source_identifier'),
                        image_data['url']
                    ))
                    existing = cur.fetchone()
                    
                    if existing:
                        image_id = existing['id']
                        logger.info(f"Image already exists in database: {image_id}")
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
                            image_data['url'],
                            image_data['source'],
                            image_data.get('source_identifier'),
                            image_data.get('license_type'),
                            image_data.get('license_url'),
                            image_data.get('attribution'),
                            image_data.get('width'),
                            image_data.get('height'),
                            image_data.get('thumbnail_url'),
                            image_data.get('source_page_url')
                        ))
                        image_id = cur.fetchone()['id']
                        logger.info(f"✓ Inserted new image: {image_id}")
                    
                    # Check if relationship already exists
                    check_rel_query = """
                        SELECT 1 FROM artist_images 
                        WHERE performer_id = %s AND image_id = %s
                    """
                    cur.execute(check_rel_query, (performer_id, image_id))
                    
                    if cur.fetchone():
                        logger.info(f"Image already linked to performer")
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
                        logger.info(f"✓ Linked image to performer")
                    
                    conn.commit()
                    return True
                    
        except Exception as e:
            logger.error(f"Error saving image to database: {e}")
            if self.debug:
                logger.exception(e)
            return False
    
    def process_image_url(self, url: str, performer: Optional[Dict[str, Any]] = None) -> bool:
        """
        Process a Wikipedia image URL, identify the artist, and add to database.
        
        Args:
            url: Wikipedia image page URL
            performer: Optional performer record (skips identification if provided)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"\n{'='*60}")
            logger.info(f"Processing image URL: {url}")
            logger.info(f"{'='*60}\n")
            
            # Extract filename
            filename = self.extract_filename_from_url(url)
            if not filename:
                return False
            
            # Fetch image metadata
            metadata = self.fetch_image_metadata(filename)
            if not metadata:
                return False
            
            # Identify artist (or use provided performer)
            if performer:
                logger.info(f"Using specified artist: {performer['name']}")
            else:
                performer = self.identify_artist_from_metadata(filename, metadata)
                if not performer:
                    logger.error("Could not identify artist. Please add image manually or use --artist flag.")
                    return False
            
            performer_id = str(performer['id'])
            
            # Check existing images
            existing_images = get_performer_images(performer_id)
            is_primary = len(existing_images) == 0
            
            if existing_images:
                logger.info(f"Performer already has {len(existing_images)} image(s)")
                for img in existing_images:
                    logger.info(f"  - {img['source']}: {img['url']}")
            
            # Save image
            success = self.save_image_to_db(
                performer_id,
                metadata,
                is_primary=is_primary,
                display_order=len(existing_images)
            )
            
            if success:
                logger.info(f"\n{'='*60}")
                logger.info(f"✓ Successfully added image for {performer['name']}")
                logger.info(f"{'='*60}\n")
                return True
            else:
                return False
            
        except Exception as e:
            logger.error(f"Error processing image URL: {e}")
            if self.debug:
                logger.exception(e)
            return False


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description='Add a Wikipedia image to the database by identifying the artist',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Add an image from Wikipedia (auto-identify artist)
    python add_wikipedia_image.py "https://en.wikipedia.org/wiki/File:Miles_Davis_1955.jpg"
    
    # Specify the artist by name
    python add_wikipedia_image.py "https://en.wikipedia.org/wiki/File:Group_Photo.jpg" --name "Miles Davis"
    
    # Specify the artist by ID
    python add_wikipedia_image.py "https://en.wikipedia.org/wiki/File:Concert.jpg" --id "123e4567-e89b-12d3-a456-426614174000"
    
    # Add from Wikimedia Commons
    python add_wikipedia_image.py "https://commons.wikimedia.org/wiki/File:Nina_Simone.jpg"
    
    # Dry run (don't save to database)
    python add_wikipedia_image.py "https://en.wikipedia.org/wiki/File:John_Coltrane.jpg" --dry-run
    
    # Enable debug logging
    python add_wikipedia_image.py "https://en.wikipedia.org/wiki/File:Artist.jpg" --debug

The script will:
1. Extract the filename from the URL
2. Fetch image metadata (license, size, etc.)
3. Attempt to identify the artist (unless --name or --id is specified)
4. Add the image to the database and link it to the artist
        """
    )
    
    # Required argument
    parser.add_argument('url', help='Wikipedia or Wikimedia Commons image page URL')
    
    # Artist identification options
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--name', help='Artist name (skips auto-identification)')
    group.add_argument('--id', help='Artist UUID (skips auto-identification)')
    
    # Optional arguments
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be done without making changes')
    parser.add_argument('--debug', action='store_true',
                       help='Enable debug logging')
    
    args = parser.parse_args()
    
    # Validate URL
    if not ('wikipedia.org' in args.url or 'wikimedia.org' in args.url):
        logger.error("URL must be from wikipedia.org or wikimedia.org")
        sys.exit(1)
    
    if '/File:' not in args.url and '/Image:' not in args.url:
        logger.error("URL must point to a File: or Image: page")
        sys.exit(1)
    
    # Find performer if specified
    performer = None
    if args.name:
        logger.info(f"Looking up artist: {args.name}")
        performer = find_performer_by_name(args.name)
        if not performer:
            logger.error(f"Artist not found: {args.name}")
            sys.exit(1)
        logger.info(f"Found artist: {performer['name']} (ID: {performer['id']})")
    elif args.id:
        logger.info(f"Looking up artist ID: {args.id}")
        performer = find_performer_by_id(args.id)
        if not performer:
            logger.error(f"Artist not found with ID: {args.id}")
            sys.exit(1)
        logger.info(f"Found artist: {performer['name']} (ID: {performer['id']})")
    
    # Process the image
    processor = WikipediaImageProcessor(dry_run=args.dry_run, debug=args.debug)
    success = processor.process_image_url(args.url, performer=performer)
    
    if success:
        logger.info("✓ Image successfully added")
        sys.exit(0)
    else:
        logger.error("✗ Failed to add image")
        sys.exit(1)


if __name__ == '__main__':
    main()