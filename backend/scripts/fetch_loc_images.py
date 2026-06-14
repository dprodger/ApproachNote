#!/usr/bin/env python3
"""
Fetch Library of Congress Images Script
Fetches images for jazz artists from the Library of Congress digital collections,
storing them in the database.

The Library of Congress has an excellent collection of jazz photographs, particularly
from the William P. Gottlieb Collection, which entered the public domain in 2010.

Usage:
    python fetch_loc_images.py --name "Miles Davis"
    python fetch_loc_images.py --id <uuid>
    python fetch_loc_images.py --name "John Coltrane" --dry-run
    python fetch_loc_images.py --name "Ella Fitzgerald" --debug
"""

import sys
import argparse
import logging
import requests
import json
from typing import Optional, Dict, Any, List
from urllib.parse import quote, urljoin
import time

# Import our database utilities
from db_utils import (
    get_db_connection,
    find_performer_by_name,
    find_performer_by_id,
    update_performer_external_references,
    normalize_apostrophes,
    get_performer_images
)
from core.http_client import make_session

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('log/fetch_loc_images.log')
    ]
)
logger = logging.getLogger(__name__)


class LOCImageFetcher:
    """Handles fetching images from the Library of Congress."""
    
    # LOC API endpoints
    LOC_SEARCH_URL = "https://www.loc.gov/collections/"
    LOC_ITEM_URL = "https://www.loc.gov/item/"
    
    # Collections to search
    COLLECTIONS = [
        "gottlieb",  # William P. Gottlieb Collection (jazz photos, public domain)
        "world-telegram",  # New York World-Telegram & Sun Collection
    ]
    
    def __init__(self, dry_run: bool = False, debug: bool = False):
        """
        Initialize LOC image fetcher
        
        Args:
            dry_run: If True, show what would be done without making changes
            debug: Enable debug logging
        """
        self.dry_run = dry_run
        self.debug = debug
        self.session = make_session()
        
        if debug:
            logger.setLevel(logging.DEBUG)
        
        self.stats = {
            'searches_performed': 0,
            'images_found': 0,
            'images_saved': 0,
            'errors': 0
        }
    
    def search_loc_collection(self, artist_name: str, collection: str = None) -> List[Dict[str, Any]]:
        """
        Search the Library of Congress for images of an artist.
        
        Args:
            artist_name: Name of the artist to search for
            collection: Specific collection to search (optional)
        
        Returns:
            List of image dictionaries from LOC
        """
        try:
            self.stats['searches_performed'] += 1
            
            # Build search URL
            # LOC uses a specific URL format for searches with JSON output
            if collection:
                search_url = f"https://www.loc.gov/collections/{collection}/"
                logger.info(f"Searching LOC {collection} collection for {artist_name}...")
            else:
                search_url = "https://www.loc.gov/search/"
                logger.info(f"Searching LOC for {artist_name}...")
            
            params = {
                'q': artist_name,
                'fo': 'json',  # Request JSON format
                'c': 100,  # Number of results
                'at': 'results'  # Return results
                # Note: Not filtering by 'partof:photographs' as it's too restrictive
                # We'll filter by validating image_url presence instead
            }
            
            response = self.session.get(search_url, params=params, timeout=15)
            
            # Rate limiting - be respectful to LOC servers
            time.sleep(1.0)
            
            if response.status_code != 200:
                logger.warning(f"LOC search returned status {response.status_code}")
                return []
            
            # Parse JSON response
            try:
                data = response.json()
                
                # Check if we got valid results
                if not isinstance(data, dict):
                    logger.warning(f"LOC returned unexpected data type: {type(data)}")
                    logger.debug(f"Response content: {str(data)[:200]}")
                    return []
                
                results = data.get('results', [])
                
                if not results:
                    logger.info(f"No results found in LOC for {artist_name}")
                    logger.debug(f"Response keys: {data.keys()}")
                    return []
                
                logger.info(f"Found {len(results)} potential results from LOC")
                if self.debug and results:
                    logger.debug(f"Sample result keys: {results[0].keys() if results else 'none'}")
                    logger.debug(f"Sample title: {results[0].get('title', 'no title')}")
                
                # Filter and process results
                images = []
                for idx, result in enumerate(results):
                    logger.debug(f"Processing result {idx + 1}/{len(results)}: {result.get('title', 'N/A')[:60]}")
                    image_data = self._process_loc_result(result, artist_name)
                    if image_data:
                        images.append(image_data)
                        self.stats['images_found'] += 1
                        logger.debug(f"  ✓ Image data extracted")
                    else:
                        logger.debug(f"  ✗ No image data extracted")
                
                logger.info(f"Extracted {len(images)} usable images from {len(results)} results")
                return images
                
            except json.JSONDecodeError as e:
                logger.error(f"Could not parse JSON from LOC response: {e}")
                logger.debug(f"Response text: {response.text[:500]}")
                return []
            except Exception as e:
                logger.error(f"Error processing LOC response: {e}")
                if self.debug:
                    logger.exception(e)
                return []
                
        except requests.RequestException as e:
            logger.error(f"Error searching LOC: {e}")
            self.stats['errors'] += 1
            return []
        except Exception as e:
            logger.error(f"Unexpected error searching LOC: {e}")
            if self.debug:
                logger.exception(e)
            self.stats['errors'] += 1
            return []
    
    def _process_loc_result(self, result: Dict[str, Any], artist_name: str) -> Optional[Dict[str, Any]]:
        """
        Process a single LOC search result to extract image data.
        
        Args:
            result: Result dictionary from LOC API
            artist_name: Name being searched for (for validation)
        
        Returns:
            Image data dictionary or None
        """
        try:
            # Extract basic information
            item_id = result.get('id')
            title = result.get('title', '')
            
            # Validate that this result is actually about the artist
            if not self._validate_result(result, artist_name):
                logger.debug(f"Skipping result - doesn't match artist: {title}")
                return None
            
            # Get image URLs - LOC provides multiple resolutions
            image_url_list = result.get('image_url', [])
            if not image_url_list:
                # Try to get from resources
                resources = result.get('resources', [])
                if resources and len(resources) > 0:
                    image_url_list = [resources[0].get('image', '')]
            
            # If image_url_list is a string, convert to list
            if isinstance(image_url_list, str):
                image_url_list = [image_url_list]
            
            if not image_url_list or len(image_url_list) == 0:
                logger.debug(f"No image URL found for result: {title}")
                return None
            
            # Choose the best resolution (LOC provides multiple sizes)
            # The format is typically: pct:6.25, pct:12.5, pct:25.0, pct:50.0, pct:100.0
            # We want a good balance - 50% is usually perfect for display
            image_url = None
            thumbnail_url = None
            
            for url in image_url_list:
                if 'pct:50.0' in url or 'pct:25.0' in url:
                    image_url = url.split('#')[0]  # Remove the #h=xxx&w=xxx part
                    break
            
            # If we didn't find a medium size, just use the last (usually full size)
            if not image_url and len(image_url_list) > 0:
                image_url = image_url_list[-1].split('#')[0]
            
            # Use smallest for thumbnail
            if len(image_url_list) > 0:
                thumbnail_url = image_url_list[0].split('#')[0]
            
            if not image_url:
                logger.debug(f"Could not determine image URL from: {image_url_list}")
                return None
            
            # Build full URLs
            item_url = result.get('url', '')
            if not item_url and item_id:
                item_url = f"https://www.loc.gov/item/{item_id.split('/')[-2]}/"
            
            # Get collection information
            partof = result.get('partof', [])
            collection_name = partof[0] if partof and len(partof) > 0 else 'Library of Congress'
            
            # Determine license based on collection
            license_info = self._determine_license(result)
            
            # Extract dimensions from image_url if available (they're in the #h=xxx&w=xxx format)
            width = None
            height = None
            if image_url_list and len(image_url_list) > 0:
                # Try to get dimensions from the full-size image URL
                full_size_url = image_url_list[-1]
                if '#h=' in full_size_url:
                    try:
                        dims = full_size_url.split('#')[1]
                        for param in dims.split('&'):
                            if param.startswith('h='):
                                height = int(param.split('=')[1])
                            elif param.startswith('w='):
                                width = int(param.split('=')[1])
                    except (IndexError, ValueError):
                        pass
            
            # Build image data
            image_data = {
                'url': image_url,
                'thumbnail_url': thumbnail_url,
                'source': 'loc',
                'source_identifier': item_id,
                'source_page_url': item_url,
                'license_type': license_info['license_type'],
                'license_url': license_info.get('license_url'),
                'attribution': license_info.get('attribution'),
                'width': width,
                'height': height,
                'title': title,
                'collection': collection_name
            }
            
            logger.debug(f"Successfully processed LOC image: {title[:60]}")
            return image_data
            
        except Exception as e:
            logger.error(f"Error processing LOC result: {e}")
            if self.debug:
                logger.exception(e)
            return None
    
    def _validate_result(self, result: Dict[str, Any], artist_name: str) -> bool:
        """
        Validate that a search result is actually about the specified artist.
        
        Args:
            result: LOC search result
            artist_name: Name of the artist being searched
        
        Returns:
            True if result appears to be about the artist
        """
        # Check title
        title = result.get('title', '').lower()
        artist_lower = artist_name.lower()
        
        # Split artist name into parts for better matching
        name_parts = [part.lower() for part in artist_name.split() if len(part) > 2]
        
        # Check if all significant name parts appear in title
        matches_in_title = sum(1 for part in name_parts if part in title)
        
        if matches_in_title >= len(name_parts):
            logger.debug(f"  Validation: All name parts in title")
            return True
        
        # Also check description/subject fields (LOC uses subject for artist names)
        description = str(result.get('description', '')).lower()
        subjects = result.get('subject', [])
        
        # Subjects can be a list or string
        if isinstance(subjects, list):
            subjects_text = ' '.join(str(s) for s in subjects).lower()
        else:
            subjects_text = str(subjects).lower()
        
        # Check subjects field (this is where LOC typically has artist names)
        if artist_lower in subjects_text:
            logger.debug(f"  Validation: Artist name in subjects")
            return True
        
        # Check for name parts in combined fields
        combined_text = f"{title} {description} {subjects_text}"
        matches = sum(1 for part in name_parts if part in combined_text)
        
        # More lenient - accept if we find at least half the name parts (rounded up)
        required_matches = (len(name_parts) + 1) // 2
        if matches >= required_matches:
            logger.debug(f"  Validation: {matches}/{len(name_parts)} name parts found")
            return True
        
        logger.debug(f"  Validation failed: only {matches}/{len(name_parts)} name parts found")
        return False
    
    def _determine_license(self, result: Dict[str, Any]) -> Dict[str, str]:
        """
        Determine license information for a LOC image.
        
        Args:
            result: LOC search result
        
        Returns:
            Dictionary with license_type, license_url, and attribution
        """
        rights = result.get('rights', '').lower()
        partof = result.get('partof', [])
        
        # Check if this is from the Gottlieb Collection (public domain)
        is_gottlieb = any('gottlieb' in str(p).lower() for p in partof)
        
        if is_gottlieb:
            # Gottlieb Collection entered public domain in 2010
            return {
                'license_type': 'public-domain',
                'license_url': 'https://www.loc.gov/rr/print/res/717_gott.html',
                'attribution': 'William P. Gottlieb/Library of Congress'
            }
        
        # Check for explicit public domain markers
        if 'no known copyright' in rights or 'public domain' in rights:
            return {
                'license_type': 'public-domain',
                'license_url': 'https://www.loc.gov/rr/print/195_copr.html',
                'attribution': 'Library of Congress'
            }
        
        # Check for restrictions
        if 'restricted' in rights or 'rights reserved' in rights:
            return {
                'license_type': 'all-rights-reserved',
                'license_url': None,
                'attribution': result.get('contributors', ['Library of Congress'])[0]
            }
        
        # Default to requiring rights evaluation
        return {
            'license_type': 'unknown',
            'license_url': 'https://www.loc.gov/rr/print/195_copr.html',
            'attribution': 'Library of Congress'
        }
    
    def save_image_to_db(self, performer_id: str, image_data: Dict[str, Any], 
                        is_primary: bool = False, display_order: int = 0) -> bool:
        """
        Save an image to the database and link it to a performer.
        
        Args:
            performer_id: UUID of the performer
            image_data: Dictionary containing image information
            is_primary: Whether this is the primary/profile image
            display_order: Order for displaying in carousel
        
        Returns:
            True if successful, False otherwise
        """
        if self.dry_run:
            logger.info(f"[DRY RUN] Would save image to database:")
            logger.info(f"  Title: {image_data.get('title')}")
            logger.info(f"  URL: {image_data.get('url')}")
            logger.info(f"  Source: {image_data.get('source')}")
            logger.info(f"  Collection: {image_data.get('collection')}")
            logger.info(f"  License: {image_data.get('license_type')}")
            logger.info(f"  Primary: {is_primary}")
            return True
        
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    # First, check if image already exists
                    check_query = """
                        SELECT id FROM images 
                        WHERE source = %s AND source_identifier = %s
                    """
                    cur.execute(check_query, (
                        image_data['source'],
                        image_data.get('source_identifier')
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
                    self.stats['images_saved'] += 1
                    return True
                    
        except Exception as e:
            logger.error(f"Error saving image to database: {e}")
            if self.debug:
                logger.exception(e)
            self.stats['errors'] += 1
            return False
    
    def fetch_and_save_images(self, performer: Dict[str, Any], limit: int = 3) -> int:
        """
        Fetch images from Library of Congress and save them.
        
        Args:
            performer: Performer record from database
            limit: Maximum number of images to save
        
        Returns:
            Number of new images added
        """
        performer_id = str(performer['id'])
        performer_name = performer['name']
        images_added = 0
        
        logger.info(f"\n{'='*80}")
        logger.info(f"FETCHING LOC IMAGES FOR: {performer_name}")
        logger.info(f"{'='*80}")
        
        # Check existing images
        existing_images = get_performer_images(performer_id)
        if existing_images:
            logger.info(f"Performer already has {len(existing_images)} image(s)")
            for img in existing_images:
                logger.info(f"  - {img['source']}: {img.get('url', 'N/A')[:80]}")
        
        # Search LOC for images
        all_images = []
        
        # Try searching specific collections first
        for collection in self.COLLECTIONS:
            images = self.search_loc_collection(performer_name, collection)
            all_images.extend(images)
            
            # If we found good images in Gottlieb collection, prioritize those
            if collection == 'gottlieb' and images:
                logger.info(f"Found {len(images)} images in Gottlieb collection")
                break
        
        # If no images found in specific collections, try general search
        if not all_images:
            images = self.search_loc_collection(performer_name)
            all_images.extend(images)
        
        if not all_images:
            logger.info(f"No images found at Library of Congress for {performer_name}")
            return 0
        
        logger.info(f"\nProcessing {min(len(all_images), limit)} of {len(all_images)} found images...")
        
        # Save images (up to limit)
        for idx, image_data in enumerate(all_images[:limit]):
            logger.info(f"\nImage {idx + 1}/{min(len(all_images), limit)}:")
            logger.info(f"  Title: {image_data.get('title', 'N/A')}")
            logger.info(f"  Collection: {image_data.get('collection', 'N/A')}")
            logger.info(f"  License: {image_data.get('license_type', 'N/A')}")
            
            # Determine if this should be primary
            is_primary = (not existing_images and idx == 0)
            
            # Save the image
            if self.save_image_to_db(performer_id, image_data, 
                                    is_primary=is_primary, 
                                    display_order=idx):
                images_added += 1
        
        # Update external references with LOC info
        if images_added > 0 and not self.dry_run:
            external_refs = {
                'loc_search_url': f"https://www.loc.gov/search/?q={quote(performer_name)}&fo=json"
            }
            update_performer_external_references(performer_id, external_refs, self.dry_run)
        
        logger.info(f"\n{'='*80}")
        logger.info(f"✓ Added {images_added} new image(s) for {performer_name}")
        logger.info(f"{'='*80}\n")
        
        return images_added
    
    def print_summary(self):
        """Print processing summary"""
        logger.info("")
        logger.info("="*80)
        logger.info("PROCESSING SUMMARY")
        logger.info("="*80)
        logger.info(f"Searches performed: {self.stats['searches_performed']}")
        logger.info(f"Images found:       {self.stats['images_found']}")
        logger.info(f"Images saved:       {self.stats['images_saved']}")
        logger.info(f"Errors:             {self.stats['errors']}")
        logger.info("="*80)


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description='Fetch images for jazz artists from the Library of Congress',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Fetch images by artist name
    python fetch_loc_images.py --name "Miles Davis"
    
    # Fetch by performer ID
    python fetch_loc_images.py --id 123e4567-e89b-12d3-a456-426614174000
    
    # Limit number of images
    python fetch_loc_images.py --name "Ella Fitzgerald" --limit 5
    
    # Dry run (don't save to database)
    python fetch_loc_images.py --name "John Coltrane" --dry-run
    
    # Enable debug logging
    python fetch_loc_images.py --name "Art Tatum" --debug
    
    # Combination
    python fetch_loc_images.py --name "Thelonious Monk" --limit 3 --dry-run --debug

Notes:
    The Library of Congress has excellent jazz collections, particularly:
    - William P. Gottlieb Collection (public domain since 2010)
    - New York World-Telegram & Sun Collection
    
    This script prioritizes public domain images and provides proper attribution.
        """
    )
    
    # Required arguments (one of)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--name', help='Artist name to search for')
    group.add_argument('--id', help='Performer UUID')
    
    # Optional arguments
    parser.add_argument(
        '--limit',
        type=int,
        default=3,
        help='Maximum number of images to save (default: 3)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be done without making changes'
    )
    
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
    )
    
    args = parser.parse_args()
    
    # Set logging level
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Find the performer
    if args.name:
        artist_name = normalize_apostrophes(args.name)
        logger.info(f"Searching for performer: {artist_name}")
        performer = find_performer_by_name(artist_name)
        if not performer:
            logger.error(f"Performer not found: {artist_name}")
            sys.exit(1)
    else:
        logger.info(f"Looking up performer ID: {args.id}")
        performer = find_performer_by_id(args.id)
        if not performer:
            logger.error(f"Performer not found with ID: {args.id}")
            sys.exit(1)
    
    logger.info(f"Found performer: {performer['name']} (ID: {performer['id']})")
    
    # Fetch and save images
    fetcher = LOCImageFetcher(dry_run=args.dry_run, debug=args.debug)
    
    try:
        images_added = fetcher.fetch_and_save_images(performer, limit=args.limit)
        
        fetcher.print_summary()
        
        if images_added > 0:
            logger.info(f"\n✓ Success! Added {images_added} image(s)")
        else:
            logger.info(f"\n✓ No new images added (performer may already have images or none found at LOC)")
        
        sys.exit(0)
        
    except KeyboardInterrupt:
        logger.info("\nProcess cancelled by user")
        fetcher.print_summary()
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        fetcher.print_summary()
        sys.exit(1)


if __name__ == '__main__':
    main()
