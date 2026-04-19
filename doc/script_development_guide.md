# Jazz Reference Script Development Guide

This guide documents common patterns and best practices for building new scripts in the Jazz Reference application, extracted from analysis of existing scripts.

## Table of Contents
1. [Script Structure](#script-structure)
2. [Command-Line Interface](#command-line-interface)
3. [Logging Configuration](#logging-configuration)
4. [Database Connections](#database-connections)
5. [Error Handling](#error-handling)
6. [External API Integration](#external-api-integration)
7. [Statistics Tracking](#statistics-tracking)
8. [Complete Template](#complete-template)

---

## Script Structure

### Standard File Organization

```python
#!/usr/bin/env python3
"""
Script Name
Brief description of what the script does
"""

# Standard library imports
import sys
import argparse
import logging
import os
from datetime import datetime

# Third-party imports
import requests

# Local imports
from db_utils import get_db_connection

# Configure logging (see Logging section)
# Define classes
# Define main() function
# Standard if __name__ == "__main__" block
```

### Naming Conventions
- Script files: `action_noun.py` (e.g., `fetch_artist_images.py`, `import_mb_releases.py`)
- Classes: PascalCase with descriptive suffixes (e.g., `ImageFetcher`, `SpotifyMatcher`, `MusicBrainzImporter`)
- Methods: snake_case describing the action

---

## Command-Line Interface

### Standard Argument Parser Setup

All scripts use `argparse.ArgumentParser` with:
- A clear description
- `RawDescriptionHelpFormatter` for multi-line examples
- An `epilog` with usage examples
- Consistent flag naming

```python
def main():
    parser = argparse.ArgumentParser(
        description='Brief description of what this script does',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage
  python script_name.py "argument"
  
  # With dry-run
  python script_name.py "argument" --dry-run
  
  # With debug logging
  python script_name.py "argument" --debug
  
  # Combination
  python script_name.py "argument" --dry-run --debug
        """
    )
```

### Standard Arguments

#### Required Arguments
```python
# Positional argument (when there's a primary input)
parser.add_argument('song', help='Song name or database ID')

# Or mutually exclusive options
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--name', help='Artist name to search for')
group.add_argument('--id', help='Performer UUID')
```

#### Standard Optional Flags

**Always include these two flags:**

```python
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
```

**Additional optional arguments as needed:**

```python
parser.add_argument(
    '--limit',
    type=int,
    default=100,
    help='Maximum number of items to process (default: 100)'
)
```

### Parsing and Using Arguments

```python
args = parser.parse_args()

# Set logging level based on debug flag
if args.debug:
    logging.getLogger().setLevel(logging.DEBUG)

# Create main class instance with dry_run flag
processor = MyProcessor(dry_run=args.dry_run)

# Wrap execution in try-except
try:
    success = processor.process(args.song)
    sys.exit(0 if success else 1)
except KeyboardInterrupt:
    logger.info("\nProcess cancelled by user")
    sys.exit(1)
except Exception as e:
    logger.error(f"Unexpected error: {e}", exc_info=True)
    sys.exit(1)
```

---

## Logging Configuration

### Standard Logging Setup

**At the top of the file (after imports):**

```python
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('log/script_name.log')
    ]
)
logger = logging.getLogger(__name__)
```

### Log Directory Structure
- Create a `log/` directory in your scripts folder
- Each script should have its own log file: `log/script_name.log`
- Log files persist across runs for debugging history

### Logging Patterns

**Information Messages:**
```python
logger.info(f"Processing: {item_name}")
logger.info(f"Found {count} items to process")
logger.info("")  # Blank line for readability
```

**Success Messages:**
```python
logger.info(f"✓ Successfully imported: {item}")
logger.info(f"✓ Updated with ID: {id}")
```

**Warning Messages:**
```python
logger.warning(f"No results found for: {query}")
logger.warning("Skipping item due to missing data")
```

**Error Messages:**
```python
logger.error(f"Failed to process {item}: {error}")
logger.error(f"API error: {e}", exc_info=True)
```

**Debug Messages:**
```python
logger.debug(f"API response: {response.json()}")
logger.debug(f"Query parameters: {params}")
```

**Visual Separators:**
```python
logger.info("="*80)
logger.info("SCRIPT NAME")
logger.info("="*80)
```

---

## Database Connections

### Using the Shared db_utils Module

**Import:**
```python
from db_utils import get_db_connection
```

### Context Manager Pattern

**Always use the context manager for automatic connection handling:**

```python
with get_db_connection() as conn:
    with conn.cursor() as cur:
        # Execute queries
        cur.execute("SELECT * FROM table WHERE id = %s", (id,))
        result = cur.fetchone()
        
        # Commit is automatic on context exit if no exceptions
        conn.commit()
```

### Common Query Patterns

**Fetch One:**
```python
with get_db_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, name, composer
            FROM songs
            WHERE title = %s
        """, (song_title,))
        
        song = cur.fetchone()
        if not song:
            logger.warning(f"Song not found: {song_title}")
            return None
```

**Fetch All:**
```python
with get_db_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, title, composer
            FROM songs
            WHERE musicbrainz_id IS NULL
            ORDER BY title
        """)
        
        songs = cur.fetchall()
```

**Insert with RETURNING:**
```python
with get_db_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO recordings (song_id, album_title, recording_year)
            VALUES (%s, %s, %s)
            RETURNING id
        """, (song_id, album, year))
        
        result = cur.fetchone()
        recording_id = result['id']
        
        conn.commit()
```

**Update:**
```python
with get_db_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE songs
            SET musicbrainz_id = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (mb_id, song_id))
        
        conn.commit()
```

**Conditional Insert (Avoid Duplicates):**
```python
cur.execute("""
    INSERT INTO performers (name, instrument)
    SELECT %s, %s
    WHERE NOT EXISTS (
        SELECT 1 FROM performers
        WHERE name = %s
    )
    RETURNING id
""", (name, instrument, name))
```

### Dry-Run Pattern

**Check dry-run flag before committing changes:**

```python
def update_record(self, conn, record_id, value):
    """Update record with new value"""
    if self.dry_run:
        logger.info(f"    [DRY RUN] Would update record {record_id} with: {value}")
        return
    
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE table_name
            SET field = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (value, record_id))
        
        conn.commit()
        logger.info(f"    ✓ Updated record: {record_id}")
```

---

## Error Handling

### Class-Level Error Tracking

```python
class MyProcessor:
    def __init__(self, dry_run=False):
        self.dry_run = dry_run
        self.stats = {
            'items_processed': 0,
            'items_succeeded': 0,
            'items_failed': 0,
            'errors': 0
        }
```

### Try-Except in Processing Loops

```python
for item in items:
    try:
        self.stats['items_processed'] += 1
        result = self.process_item(item)
        
        if result:
            self.stats['items_succeeded'] += 1
        else:
            self.stats['items_failed'] += 1
            
    except Exception as e:
        logger.error(f"Error processing {item}: {e}", exc_info=True)
        self.stats['errors'] += 1
        continue  # Continue with next item
```

### Main Function Error Handling

```python
def main():
    parser = argparse.ArgumentParser(...)
    args = parser.parse_args()
    
    processor = MyProcessor(dry_run=args.dry_run)
    
    try:
        success = processor.run()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("\nProcess cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)
```

### Exit Codes
- `0`: Success
- `1`: Error or failure
- Also `1` for user cancellation (Ctrl+C)

---

## External API Integration

### Session Management

**Create a session object for reusable connections:**

```python
class APIIntegration:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'ApproachNote/1.0 (+support@approachnote.com)',
            'Accept': 'application/json'
        })
```

### Rate Limiting

**Respect API rate limits:**

```python
def make_api_call(self, url, params=None):
    """Make API call with rate limiting"""
    try:
        response = self.session.get(url, params=params)
        
        # MusicBrainz requires 1 second between requests
        time.sleep(1.0)
        
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"API error {response.status_code}: {response.text}")
            return None
            
    except Exception as e:
        logger.error(f"API request failed: {e}")
        return None
```

### Environment Variables for Credentials

**Use environment variables for API keys:**

```python
import os
from dotenv import load_dotenv

load_dotenv()

client_id = os.environ.get('SPOTIFY_CLIENT_ID')
client_secret = os.environ.get('SPOTIFY_CLIENT_SECRET')

if not client_id or not client_secret:
    logger.error("API credentials not found!")
    logger.error("Set SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET environment variables")
    sys.exit(1)
```

### Handling API Responses

```python
def search_api(self, query):
    """Search external API"""
    url = "https://api.example.com/search"
    params = {'q': query, 'limit': 10}
    
    try:
        response = self.session.get(url, params=params)
        time.sleep(1.0)  # Rate limiting
        
        if response.status_code == 200:
            data = response.json()
            
            # Validate response structure
            if 'results' not in data:
                logger.warning("Unexpected API response format")
                return []
            
            results = []
            for item in data['results']:
                # Extract and validate required fields
                if 'id' in item and 'name' in item:
                    results.append({
                        'id': item['id'],
                        'name': item['name'],
                        'url': item.get('url')  # Optional field
                    })
            
            return results
            
        elif response.status_code == 429:
            logger.error("API rate limit exceeded")
            return []
        else:
            logger.error(f"API error {response.status_code}")
            return []
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error: {e}")
        return []
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON response: {e}")
        return []
```

---

## Statistics Tracking

### Initialize Statistics Dictionary

```python
class MyProcessor:
    def __init__(self, dry_run=False):
        self.dry_run = dry_run
        self.stats = {
            'items_processed': 0,
            'items_found': 0,
            'items_created': 0,
            'items_updated': 0,
            'items_skipped': 0,
            'no_match_found': 0,
            'errors': 0
        }
```

### Update Stats During Processing

```python
def process_items(self):
    items = self.get_items()
    
    for item in items:
        self.stats['items_processed'] += 1
        
        if item.already_processed:
            self.stats['items_skipped'] += 1
            continue
        
        result = self.do_work(item)
        
        if result:
            self.stats['items_updated'] += 1
        else:
            self.stats['no_match_found'] += 1
```

### Print Summary

**Always print a summary at the end:**

```python
def print_summary(self):
    """Print processing summary"""
    logger.info("")
    logger.info("="*80)
    logger.info("PROCESSING SUMMARY")
    logger.info("="*80)
    logger.info(f"Items processed:     {self.stats['items_processed']}")
    logger.info(f"Items found:         {self.stats['items_found']}")
    logger.info(f"Items created:       {self.stats['items_created']}")
    logger.info(f"Items updated:       {self.stats['items_updated']}")
    logger.info(f"Items skipped:       {self.stats['items_skipped']}")
    logger.info(f"No match found:      {self.stats['no_match_found']}")
    logger.info(f"Errors:              {self.stats['errors']}")
    logger.info("="*80)
```

---

## Complete Template

Here's a complete template that incorporates all the patterns:

```python
#!/usr/bin/env python3
"""
Script Name
Brief description of what this script does and its purpose
"""

import sys
import argparse
import logging
import os
import time
from datetime import datetime
import requests

# Import shared database utilities
from db_utils import get_db_connection

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('log/script_name.log')
    ]
)
logger = logging.getLogger(__name__)


class MyProcessor:
    """Main processing class"""
    
    def __init__(self, dry_run=False):
        """
        Initialize processor
        
        Args:
            dry_run: If True, show what would be done without making changes
        """
        self.dry_run = dry_run
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'ApproachNote/1.0 (+support@approachnote.com)',
            'Accept': 'application/json'
        })
        self.stats = {
            'items_processed': 0,
            'items_succeeded': 0,
            'items_failed': 0,
            'errors': 0
        }
    
    def get_items_to_process(self):
        """Get items that need processing from database"""
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, name
                    FROM items
                    WHERE needs_processing = true
                    ORDER BY name
                """)
                return cur.fetchall()
    
    def process_item(self, item):
        """
        Process a single item
        
        Args:
            item: Item dict with id and name
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Processing: {item['name']}")
            
            # Do external API work if needed
            result = self.fetch_external_data(item['name'])
            
            if not result:
                logger.warning(f"No data found for: {item['name']}")
                return False
            
            # Update database
            self.update_database(item['id'], result)
            
            logger.info(f"✓ Successfully processed: {item['name']}")
            return True
            
        except Exception as e:
            logger.error(f"Error processing {item['name']}: {e}", exc_info=True)
            self.stats['errors'] += 1
            return False
    
    def fetch_external_data(self, name):
        """Fetch data from external API"""
        try:
            url = f"https://api.example.com/search"
            params = {'q': name}
            
            response = self.session.get(url, params=params)
            time.sleep(1.0)  # Rate limiting
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"API error {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"API request failed: {e}")
            return None
    
    def update_database(self, item_id, data):
        """Update database with fetched data"""
        if self.dry_run:
            logger.info(f"    [DRY RUN] Would update item {item_id}")
            return
        
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE items
                    SET data = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (data, item_id))
                
                conn.commit()
    
    def run(self):
        """Main processing method"""
        logger.info("="*80)
        logger.info("SCRIPT NAME")
        logger.info("="*80)
        
        if self.dry_run:
            logger.info("*** DRY RUN MODE - No database changes will be made ***")
            logger.info("")
        
        # Get items to process
        items = self.get_items_to_process()
        
        if not items:
            logger.info("No items to process!")
            return True
        
        logger.info(f"Found {len(items)} items to process")
        logger.info("")
        
        # Process each item
        for item in items:
            self.stats['items_processed'] += 1
            
            success = self.process_item(item)
            
            if success:
                self.stats['items_succeeded'] += 1
            else:
                self.stats['items_failed'] += 1
        
        # Print summary
        self.print_summary()
        
        return True
    
    def print_summary(self):
        """Print processing summary"""
        logger.info("")
        logger.info("="*80)
        logger.info("PROCESSING SUMMARY")
        logger.info("="*80)
        logger.info(f"Items processed:  {self.stats['items_processed']}")
        logger.info(f"Items succeeded:  {self.stats['items_succeeded']}")
        logger.info(f"Items failed:     {self.stats['items_failed']}")
        logger.info(f"Errors:           {self.stats['errors']}")
        logger.info("="*80)


def main():
    parser = argparse.ArgumentParser(
        description='Process items and update database',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run in normal mode
  python script_name.py
  
  # Dry run to see what would be done
  python script_name.py --dry-run
  
  # Enable debug logging
  python script_name.py --debug
  
  # Combine flags
  python script_name.py --dry-run --debug
        """
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
    
    # Create processor and run
    processor = MyProcessor(dry_run=args.dry_run)
    
    try:
        success = processor.run()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("\nProcess cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
```

---

## Additional Best Practices

### Code Organization
- One class per script for the main processing logic
- Keep methods focused and single-purpose
- Use descriptive method names that explain what they do
- Separate data fetching, processing, and database operations

### Documentation
- Module docstring at the top explaining purpose
- Class docstrings explaining the class's role
- Method docstrings for complex methods
- Inline comments for non-obvious logic

### Testing Workflow
1. Always start with `--dry-run --debug` to validate logic
2. Test with a small dataset first
3. Check log files for any warnings or errors
4. Validate database changes manually before running on full dataset

### Database Safety
- Always use parameterized queries (never string formatting)
- Use transactions for multi-step operations
- Include `updated_at = CURRENT_TIMESTAMP` in updates
- Check for existing records before inserting to avoid duplicates

### User Experience
- Provide clear progress indicators
- Show what's happening at each step
- Always print a summary at the end
- Use visual separators for readability
- Handle Ctrl+C gracefully

---

## Quick Checklist

When creating a new script, ensure you have:

- [ ] Shebang line (`#!/usr/bin/env python3`)
- [ ] Module docstring
- [ ] Standard imports organized
- [ ] Logging configuration with file and console handlers
- [ ] Import from `db_utils` for database access
- [ ] Main processing class with `__init__` taking `dry_run` parameter
- [ ] Statistics dictionary in `__init__`
- [ ] `argparse` setup with description and epilog examples
- [ ] `--dry-run` flag
- [ ] `--debug` flag
- [ ] Error handling with try-except in loops
- [ ] KeyboardInterrupt handling in main()
- [ ] Summary printing at end
- [ ] Proper exit codes
- [ ] Log file in `log/` directory
- [ ] Context managers for database connections
- [ ] Rate limiting for external APIs
- [ ] Validation of API responses
- [ ] Clear logging at each step

---

## Script-Specific Patterns

### Scripts that Process One Item
```python
parser.add_argument('song', help='Song name or database ID')

# In main():
success = processor.process_song(args.song)
```

### Scripts that Process All Items
```python
# No required arguments, just flags

# In class:
def run(self):
    items = self.get_all_items()
    for item in items:
        self.process_item(item)
```

### Scripts with Multiple Input Methods
```python
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--name', help='Search by name')
group.add_argument('--id', help='Search by ID')
```

# Standard Script Configuration Block

Add this section to the script development guide for all new scripts in `backend/scripts/`.

---

## Path Configuration and Cache Location Standards

### Required Path Setup

**All scripts in `backend/scripts/` must include this at the top** (after standard library imports, before local imports):

```python
#!/usr/bin/env python3
"""
Script description
"""

# Standard library imports
import sys
import argparse
import logging
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

# Third-party imports
import requests

# Local imports (now these will work correctly)
from db_utils import get_db_connection
```

**Why this is needed:**
- Scripts are run from `backend/scripts/` directory
- Utility modules (`db_utils.py`, `mb_utils.py`, etc.) are in `backend/`
- Without this, Python can't find the utility modules

### Cache Directory Standard

**All caching scripts must use the standardized cache location:**

```python
# Cache configuration - peer to backend directory
CACHE_DIR = Path(__file__).parent.parent.parent / 'cache' / 'subsystem_name'
CACHE_DAYS = 30  # Default cache expiration

# Examples:
# For MusicBrainz: Path(__file__).parent.parent.parent / 'cache' / 'musicbrainz'
# For Spotify: Path(__file__).parent.parent.parent / 'cache' / 'spotify'
# For JazzStandards: Path(__file__).parent.parent.parent / 'cache' / 'jazzstandards'
# For Wikipedia: Path(__file__).parent.parent.parent / 'cache' / 'wikipedia'
```

**Directory structure:**
```
ApproachNote/
├── backend/
│   ├── scripts/
│   │   ├── my_script.py          # Script runs from here
│   │   └── ...
│   ├── db_utils.py                # Imported via sys.path.insert
│   └── ...
└── cache/                         # Cache peer to backend
    ├── musicbrainz/
    ├── spotify/
    ├── jazzstandards/
    └── wikipedia/
```

**Path resolution:**
- `Path(__file__)` → `/path/to/ApproachNote/backend/scripts/my_script.py`
- `.parent` → `/path/to/ApproachNote/backend/scripts/`
- `.parent.parent` → `/path/to/ApproachNote/backend/`
- `.parent.parent.parent` → `/path/to/ApproachNote/`
- `.parent.parent.parent / 'cache' / 'subsystem'` → `/path/to/ApproachNote/cache/subsystem/`

### Cache Directory Setup

Ensure cache directory is created:

```python
# In __init__ or at module level
CACHE_DIR.mkdir(parents=True, exist_ok=True)
```

### Complete Template Header

```python
#!/usr/bin/env python3
"""
Script Name
Brief description of what the script does
"""

# Standard library imports
import sys
import argparse
import logging
import json
import os
from datetime import datetime, timedelta
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

# Third-party imports
import requests
from bs4 import BeautifulSoup

# Local imports
from db_utils import get_db_connection

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('log/script_name.log')
    ]
)
logger = logging.getLogger(__name__)

# Cache configuration (if needed)
CACHE_DIR = Path(__file__).parent.parent.parent / 'cache' / 'subsystem_name'
CACHE_DAYS = 30

# Ensure cache directory exists
CACHE_DIR.mkdir(parents=True, exist_ok=True)
```

---

## Checklist for New Scripts

- [ ] Add `sys.path.insert(0, str(Path(__file__).parent.parent))` before local imports
- [ ] If caching: Use `Path(__file__).parent.parent.parent / 'cache' / 'subsystem_name'`
- [ ] If caching: Create cache directory with `CACHE_DIR.mkdir(parents=True, exist_ok=True)`
- [ ] Scripts run from `backend/scripts/` directory
- [ ] Import `db_utils`, `mb_utils`, etc. work correctly
- [ ] Cache files stored in `ApproachNote/cache/` (peer to backend)

---

## Summary

Following these patterns will ensure:
- **Consistency** across all scripts
- **Reliability** with proper error handling
- **Maintainability** with clear structure
- **User-friendliness** with helpful output
- **Safety** with dry-run mode
- **Debuggability** with detailed logging

Every new script should be recognizable as part of the Jazz Reference project through these shared patterns and conventions.
