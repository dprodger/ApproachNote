"""
Apple Music Feed Client

Downloads and processes Apple Music catalog data via the Feed API.
This provides bulk access to Apple Music's catalog (albums, artists, songs)
without the aggressive rate limiting of the iTunes Search API.

Requires:
- Apple Developer Program membership
- Media ID (from Apple Developer portal)
- Private key (.p8 file) for JWT signing

Environment variables:
- APPLE_MEDIA_ID: Your Media ID from Apple Developer portal
- APPLE_PRIVATE_KEY_PATH: Path to your .p8 private key file
- APPLE_KEY_ID: The key ID associated with your private key
- APPLE_TEAM_ID: Your Apple Developer Team ID
"""

import os
import jwt
import time
import logging
import requests
import hashlib
from pathlib import Path
from typing import Dict, List, Optional, Any, Generator
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import pyarrow.parquet as pq
    import pyarrow
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False
    pyarrow = None  # type: ignore

try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False
    duckdb = None  # type: ignore

logger = logging.getLogger(__name__)

# Apple Music Feed API base URL
FEED_API_BASE = "https://api.media.apple.com"

# Available feeds
FEEDS = {
    'albums': 'album',
    'artists': 'artist',
    'songs': 'song',
}

# Default storage location for downloaded catalog data. Override with
# APPLE_MUSIC_CATALOG_DIR — points the worker at the persistent disk on
# Render (e.g. /data/apple_music_catalog).
_FALLBACK_CATALOG_DIR = Path(__file__).parent / "data" / "apple_music_catalog"


def _resolve_catalog_dir() -> Path:
    env_override = os.environ.get('APPLE_MUSIC_CATALOG_DIR')
    return Path(env_override) if env_override else _FALLBACK_CATALOG_DIR


# Resolved at import time. Tests patch the env var before import; runtime
# code that needs the live value should call _resolve_catalog_dir() instead.
DEFAULT_CATALOG_DIR = _resolve_catalog_dir()


class AppleMusicFeedError(Exception):
    """Base exception for Apple Music Feed errors"""
    pass


class AuthenticationError(AppleMusicFeedError):
    """JWT authentication failed"""
    pass


class FeedDownloadError(AppleMusicFeedError):
    """Failed to download feed data"""
    pass


class AppleMusicFeedClient:
    """
    Client for Apple Music Feed API.

    Handles JWT authentication and downloading parquet files containing
    the Apple Music catalog.
    """

    def __init__(
        self,
        media_id: str = None,
        private_key_path: str = None,
        key_id: str = None,
        team_id: str = None,
        catalog_dir: Path = None,
        logger: logging.Logger = None
    ):
        """
        Initialize the Feed client.

        Args:
            media_id: Apple Media ID (or set APPLE_MEDIA_ID env var)
            private_key_path: Path to .p8 private key (or set APPLE_PRIVATE_KEY_PATH)
            key_id: Key ID for the private key (or set APPLE_KEY_ID)
            team_id: Apple Developer Team ID (or set APPLE_TEAM_ID)
            catalog_dir: Directory to store downloaded catalog files
            logger: Logger instance
        """
        self.media_id = media_id or os.environ.get('APPLE_MEDIA_ID')
        self.private_key_path = private_key_path or os.environ.get('APPLE_PRIVATE_KEY_PATH')
        self.key_id = key_id or os.environ.get('APPLE_KEY_ID')
        self.team_id = team_id or os.environ.get('APPLE_TEAM_ID')
        self.catalog_dir = catalog_dir or _resolve_catalog_dir()
        self.log = logger or logging.getLogger(__name__)

        self._private_key = None
        self._token = None
        self._token_expiry = None

        # Validate configuration
        self._validate_config()

    def _validate_config(self):
        """Validate required configuration is present."""
        missing = []
        if not self.media_id:
            missing.append('APPLE_MEDIA_ID')
        if not self.private_key_path:
            missing.append('APPLE_PRIVATE_KEY_PATH')
        if not self.key_id:
            missing.append('APPLE_KEY_ID')
        if not self.team_id:
            missing.append('APPLE_TEAM_ID')

        if missing:
            self.log.warning(f"Apple Music Feed not configured. Missing: {', '.join(missing)}")

    def is_configured(self) -> bool:
        """Check if all required configuration is present."""
        return all([
            self.media_id,
            self.private_key_path,
            self.key_id,
            self.team_id,
            os.path.exists(self.private_key_path) if self.private_key_path else False
        ])

    def _load_private_key(self) -> str:
        """Load the private key from file."""
        if self._private_key is None:
            try:
                with open(self.private_key_path, 'r') as f:
                    self._private_key = f.read()
            except Exception as e:
                raise AuthenticationError(f"Failed to load private key: {e}")
        return self._private_key

    def _generate_token(self) -> str:
        """
        Generate a JWT token for Apple Music Feed API.

        The token is valid for 1 hour but we'll refresh it after 50 minutes.
        """
        now = datetime.utcnow()
        expiry = now + timedelta(hours=1)

        headers = {
            'alg': 'ES256',
            'kid': self.key_id,
            'typ': 'JWT'
        }

        payload = {
            'iss': self.team_id,
            'iat': int(now.timestamp()),
            'exp': int(expiry.timestamp()),
            'sub': self.media_id
        }

        private_key = self._load_private_key()

        try:
            token = jwt.encode(
                payload,
                private_key,
                algorithm='ES256',
                headers=headers
            )
            self._token = token
            self._token_expiry = expiry - timedelta(minutes=10)  # Refresh 10 min early
            return token
        except Exception as e:
            raise AuthenticationError(f"Failed to generate JWT token: {e}")

    def _get_token(self) -> str:
        """Get a valid token, generating a new one if needed."""
        if self._token is None or datetime.utcnow() >= self._token_expiry:
            return self._generate_token()
        return self._token

    def _make_request(self, endpoint: str, params: Dict = None) -> Dict:
        """Make an authenticated request to the Feed API."""
        if not self.is_configured():
            raise AuthenticationError("Apple Music Feed is not configured")

        url = f"{FEED_API_BASE}{endpoint}"
        headers = {
            'Authorization': f'Bearer {self._get_token()}',
            'Content-Type': 'application/json'
        }

        self.log.debug(f"Making request to: {url}")

        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            self.log.debug(f"Response status: {response.status_code}")
            self.log.debug(f"Response body: {response.text[:500] if response.text else 'empty'}")
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            self.log.error(f"HTTP error: {e}")
            self.log.error(f"Response body: {e.response.text[:500] if e.response.text else 'empty'}")
            if e.response.status_code == 401:
                # Token may have expired, try refreshing
                self._token = None
                headers['Authorization'] = f'Bearer {self._get_token()}'
                response = requests.get(url, headers=headers, params=params, timeout=30)
                response.raise_for_status()
                return response.json()
            raise FeedDownloadError(f"API request failed: {e}")
        except Exception as e:
            raise FeedDownloadError(f"API request failed: {e}")

    def get_latest_export(self, feed_name: str) -> Dict:
        """
        Get metadata about the latest export for a feed.

        Args:
            feed_name: One of 'albums', 'artists', 'songs'

        Returns:
            Export metadata including ID, date, and part count
        """
        if feed_name not in FEEDS:
            raise ValueError(f"Unknown feed: {feed_name}. Must be one of: {list(FEEDS.keys())}")

        feed_type = FEEDS[feed_name]
        endpoint = f"/v1/feed/{feed_type}/latest"

        response = self._make_request(endpoint)

        # Parse the nested response format
        # Response structure: {"data": [{"id": "...", ...}], "resources": {"exports": {...}}}
        data_list = response.get('data', [])
        if not data_list:
            raise FeedDownloadError(f"No export data found for {feed_name}")

        export_ref = data_list[0]
        export_id = export_ref.get('id')

        # Get full export details from resources
        resources = response.get('resources', {})
        exports = resources.get('exports', {})
        export_data = exports.get(export_id, {})
        attributes = export_data.get('attributes', {})

        self.log.debug(f"Export ID: {export_id}")
        self.log.debug(f"Attributes: {attributes}")

        return {
            'id': export_id,
            'feed_type': feed_type,
            'created_date': attributes.get('dateGenerated'),
            'part_count': attributes.get('partCount', 0),
            'total_records': attributes.get('totalRecords', 0),
            'file_size_bytes': attributes.get('fileSizeBytes', 0),
        }

    def get_export_parts(self, export_id: str) -> List[Dict]:
        """
        Get the list of parts (files) for an export.

        Args:
            export_id: Export ID from get_latest_export()

        Returns:
            List of part metadata with download URLs
        """
        all_parts = []
        endpoint = f"/v1/feed/exports/{export_id}/parts"

        while endpoint:
            response = self._make_request(endpoint)

            # Parse nested response format
            data_list = response.get('data', [])
            resources = response.get('resources', {})
            parts_resources = resources.get('parts', {})

            self.log.debug(f"Found {len(data_list)} parts in this page")
            self.log.debug(f"Resources keys: {list(resources.keys())}")

            if parts_resources:
                # Log first part's structure for debugging
                first_key = list(parts_resources.keys())[0] if parts_resources else None
                if first_key:
                    self.log.debug(f"Sample part data: {parts_resources[first_key]}")

            for part_ref in data_list:
                part_id = part_ref.get('id')
                part_href = part_ref.get('href')

                # Get full part details from resources
                part_data = parts_resources.get(part_id, {})
                attributes = part_data.get('attributes', {})

                # Extract part number from ID if not in attributes (e.g., "0_part_album_..." -> 0)
                part_number = attributes.get('partNumber') or attributes.get('offset')
                if part_number is None and part_id:
                    try:
                        part_number = int(part_id.split('_')[0])
                    except (ValueError, IndexError):
                        part_number = 0

                # Download URL is in 'exportLocation' field
                download_url = (attributes.get('exportLocation') or
                               attributes.get('downloadUrl') or
                               part_ref.get('exportLocation') or
                               part_ref.get('downloadUrl'))

                all_parts.append({
                    'id': part_id,
                    'href': part_href,
                    'part_number': part_number,
                    'download_url': download_url,
                    'file_size_bytes': attributes.get('fileSizeBytes') or part_ref.get('fileSizeBytes', 0),
                    'checksum': attributes.get('checksum') or part_ref.get('checksum'),
                })

            # Check for pagination
            next_page = response.get('next')
            if next_page:
                self.log.debug(f"Fetching next page: {next_page}")
                endpoint = next_page
            else:
                endpoint = None

        self.log.info(f"Total parts found: {len(all_parts)}")

        # Sort by part number
        return sorted(all_parts, key=lambda p: p['part_number'] or 0)

    def get_part_download_url(self, part: Dict) -> Optional[str]:
        """
        Get the download URL for a part by fetching its details.

        Args:
            part: Part dict from get_export_parts()

        Returns:
            Download URL string or None
        """
        if part.get('download_url'):
            return part['download_url']

        href = part.get('href')
        if not href:
            return None

        try:
            response = self._make_request(href)
            data_list = response.get('data', [])
            if data_list:
                attributes = data_list[0].get('attributes', {})
                return attributes.get('exportLocation') or attributes.get('downloadUrl')

            # Also check resources
            resources = response.get('resources', {})
            parts = resources.get('parts', {})
            part_data = parts.get(part['id'], {})
            attrs = part_data.get('attributes', {})
            return attrs.get('exportLocation') or attrs.get('downloadUrl')
        except Exception as e:
            self.log.error(f"Failed to get download URL for part {part['id']}: {e}")
            return None

    def download_part(
        self,
        part: Dict,
        output_dir: Path,
        verify_checksum: bool = True
    ) -> Path:
        """
        Download a single part file.

        Args:
            part: Part metadata from get_export_parts()
            output_dir: Directory to save the file
            verify_checksum: Whether to verify the MD5 checksum

        Returns:
            Path to the downloaded file
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        part_number = part.get('part_number', 0)
        filename = f"part_{part_number:04d}.parquet"
        output_path = output_dir / filename

        # Check if file already exists
        if output_path.exists():
            # If we have a checksum, verify it
            if verify_checksum and part.get('checksum'):
                existing_checksum = self._calculate_md5(output_path)
                if existing_checksum == part['checksum']:
                    self.log.debug(f"Part {part_number} already downloaded and verified")
                    return output_path
                else:
                    self.log.warning(f"Part {part_number} checksum mismatch, re-downloading")
            else:
                # No checksum available, skip if file exists and has reasonable size
                file_size = output_path.stat().st_size
                if file_size > 1000000:  # > 1MB, assume it's valid
                    self.log.debug(f"Part {part_number} already exists ({file_size / 1024 / 1024:.1f} MB), skipping")
                    return output_path
                else:
                    self.log.warning(f"Part {part_number} exists but is too small ({file_size} bytes), re-downloading")

        # Get download URL - fetch it if not already present
        download_url = part.get('download_url')
        if not download_url:
            self.log.debug(f"Fetching download URL for part {part_number}...")
            download_url = self.get_part_download_url(part)

        if not download_url:
            raise FeedDownloadError(f"No download URL available for part {part_number}")

        self.log.info(f"Downloading part {part_number}...")

        try:
            response = requests.get(
                download_url,
                headers={'Authorization': f'Bearer {self._get_token()}'},
                stream=True,
                timeout=300
            )
            response.raise_for_status()

            # Write to temp file first
            temp_path = output_path.with_suffix('.tmp')
            with open(temp_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            # Verify checksum
            if verify_checksum and part.get('checksum'):
                actual_checksum = self._calculate_md5(temp_path)
                if actual_checksum != part['checksum']:
                    temp_path.unlink()
                    raise FeedDownloadError(
                        f"Checksum mismatch for part {part_number}: "
                        f"expected {part['checksum']}, got {actual_checksum}"
                    )

            # Move to final location
            temp_path.rename(output_path)
            self.log.info(f"Downloaded part {part_number} ({output_path.stat().st_size / 1024 / 1024:.1f} MB)")

            return output_path

        except Exception as e:
            if isinstance(e, FeedDownloadError):
                raise
            raise FeedDownloadError(f"Failed to download part {part_number}: {e}")

    def _calculate_md5(self, path: Path) -> str:
        """Calculate MD5 checksum of a file."""
        md5 = hashlib.md5()
        with open(path, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                md5.update(chunk)
        return md5.hexdigest()

    def download_feed(
        self,
        feed_name: str,
        max_workers: int = 4,
        verify_checksum: bool = True
    ) -> Path:
        """
        Download all parts of a feed.

        Args:
            feed_name: One of 'albums', 'artists', 'songs'
            max_workers: Number of parallel download threads
            verify_checksum: Whether to verify checksums

        Returns:
            Path to the directory containing downloaded files
        """
        if not PYARROW_AVAILABLE:
            raise ImportError("pyarrow is required for feed processing. Install with: pip install pyarrow")

        export = self.get_latest_export(feed_name)
        self.log.info(f"Latest {feed_name} export: {export['id']}")
        self.log.info(f"  Created: {export['created_date']}")
        self.log.info(f"  Parts: {export['part_count']}")
        self.log.info(f"  Records: {export['total_records']:,}")
        self.log.info(f"  Size: {export['file_size_bytes'] / 1024 / 1024 / 1024:.1f} GB")

        parts = self.get_export_parts(export['id'])

        # Create output directory with export date
        export_date = export['created_date'][:10]  # YYYY-MM-DD
        output_dir = self.catalog_dir / feed_name / export_date
        output_dir.mkdir(parents=True, exist_ok=True)

        # Save export metadata
        metadata_path = output_dir / 'metadata.txt'
        with open(metadata_path, 'w') as f:
            f.write(f"export_id={export['id']}\n")
            f.write(f"created_date={export['created_date']}\n")
            f.write(f"part_count={export['part_count']}\n")
            f.write(f"total_records={export['total_records']}\n")
            f.write(f"downloaded_at={datetime.utcnow().isoformat()}\n")

        # Download parts in parallel
        self.log.info(f"Downloading {len(parts)} parts to {output_dir}...")

        downloaded = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    self.download_part, part, output_dir, verify_checksum
                ): part
                for part in parts
            }

            for future in as_completed(futures):
                part = futures[future]
                try:
                    path = future.result()
                    downloaded.append(path)
                except Exception as e:
                    self.log.error(f"Failed to download part {part['part_number']}: {e}")

        self.log.info(f"Downloaded {len(downloaded)}/{len(parts)} parts")

        return output_dir


class AppleMusicCatalog:
    """
    Query interface for downloaded Apple Music catalog data.

    Uses an indexed DuckDB database for fast queries if available,
    otherwise falls back to scanning parquet files directly.

    Supports MotherDuck cloud database via APPLE_MUSIC_CATALOG_DB env var:
        export APPLE_MUSIC_CATALOG_DB="md:apple_music_feed"
    """

    # Default indexed database path. Sits next to the catalog dir so a
    # persistent-disk mount holds parquet + duckdb together.
    DEFAULT_DB_PATH = _resolve_catalog_dir().parent / "apple_music_catalog.duckdb"

    def __init__(
        self,
        catalog_dir: Path = None,
        db_path: str = None,
        logger: logging.Logger = None
    ):
        """
        Initialize the catalog reader.

        Args:
            catalog_dir: Directory containing downloaded catalog files
            db_path: Path to indexed DuckDB database, or MotherDuck connection string (e.g., "md:apple_music_feed")
            logger: Logger instance
        """
        if not DUCKDB_AVAILABLE:
            raise ImportError("duckdb is required. Install with: pip install duckdb")

        self.catalog_dir = Path(catalog_dir or _resolve_catalog_dir())
        self.log = logger or logging.getLogger(__name__)

        # Check for MotherDuck or custom DB path via env var or parameter
        self.db_path = db_path or os.environ.get('APPLE_MUSIC_CATALOG_DB')
        self._use_motherduck = self.db_path and str(self.db_path).startswith('md:')

        if not self.db_path:
            self.db_path = self.DEFAULT_DB_PATH

        # Check if indexed database exists (skip check for MotherDuck)
        if self._use_motherduck:
            self._use_indexed_db = True
            self.log.debug(f"Using MotherDuck database: {self.db_path}")
        else:
            self.db_path = Path(self.db_path)
            self._use_indexed_db = self.db_path.exists()
            if self._use_indexed_db:
                self.log.debug(f"Using indexed database: {self.db_path}")
            else:
                self.log.debug(f"Indexed database not found, will scan parquet files (slower)")
                self.log.debug(f"Run: python scripts/build_apple_catalog_index.py to create index")

        self._conn = None
        self._query_count = 0  # Track number of queries for debugging

    def _get_latest_export_dir(self, feed_name: str) -> Optional[Path]:
        """Get the most recent export directory for a feed."""
        feed_dir = self.catalog_dir / feed_name
        if not feed_dir.exists():
            return None

        # Find most recent dated directory
        export_dirs = sorted(
            [d for d in feed_dir.iterdir() if d.is_dir()],
            reverse=True
        )

        return export_dirs[0] if export_dirs else None

    def _get_parquet_glob(self, feed_name: str) -> str:
        """Get glob pattern for parquet files."""
        export_dir = self._get_latest_export_dir(feed_name)
        if not export_dir:
            raise FileNotFoundError(f"No {feed_name} catalog data found in {self.catalog_dir}")
        return str(export_dir / '*.parquet')

    def _get_conn(self):
        """Get DuckDB connection."""
        if self._conn is None:
            self._create_conn()
        return self._conn

    def _create_conn(self):
        """Create a new DuckDB connection."""
        if self._use_motherduck:
            # MotherDuck connection - db_path is like "md:apple_music_feed"
            self._conn = duckdb.connect(str(self.db_path))
        elif self._use_indexed_db:
            self._conn = duckdb.connect(str(self.db_path), read_only=True)
        else:
            self._conn = duckdb.connect(':memory:')

    def _refresh_conn(self):
        """Close and recreate the DuckDB connection."""
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
        self._create_conn()

    def get_query_count(self) -> int:
        """Get the total number of queries executed."""
        return self._query_count

    def reset_query_count(self):
        """Reset the query counter."""
        self._query_count = 0

    def search_albums(
        self,
        artist_name: str = None,
        album_title: str = None,
        limit: int = 100
    ) -> List[Dict]:
        """
        Search for albums by artist and/or title.

        Uses indexed DuckDB database if available (fast), otherwise
        falls back to scanning parquet files (slow).

        Args:
            artist_name: Artist name to search for
            album_title: Album title to search for
            limit: Maximum results to return

        Returns:
            List of matching album dicts
        """
        conn = self._get_conn()

        if self._use_indexed_db:
            return self._search_albums_indexed(conn, artist_name, album_title, limit)
        else:
            return self._search_albums_parquet(conn, artist_name, album_title, limit)

    def _search_albums_indexed(
        self,
        conn,
        artist_name: str = None,
        album_title: str = None,
        limit: int = 100
    ) -> List[Dict]:
        """Search albums using indexed database (fast)."""
        conditions = []
        params = []

        if artist_name:
            conditions.append("LOWER(artist_name) LIKE ?")
            params.append(f"%{artist_name.lower()}%")

        if album_title:
            conditions.append("LOWER(name) LIKE ?")
            params.append(f"%{album_title.lower()}%")

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        # Order by relevance - shorter album names that match are likely more relevant
        # Also prioritize exact-ish matches by checking length similarity
        order_clause = ""
        if album_title:
            # Order by how close the album name length is to the search term
            # This helps rank "Midnight in Paris (Soundtrack)" above "Jazz Midnight Paris: The Best..."
            order_clause = f"ORDER BY ABS(LENGTH(name) - {len(album_title)})"

        query = f"""
            SELECT
                id,
                name,
                artist_name as artistName,
                release_date as releaseDate,
                track_count as trackCount,
                upc,
                url_template as urlTemplate
            FROM albums
            WHERE {where_clause}
            {order_clause}
            LIMIT {limit}
        """

        try:
            self._query_count += 1
            self.log.debug(f"Album search query: artist={artist_name}, album={album_title}")

            # Refresh connection periodically to avoid stale connection issues
            if self._query_count % 500 == 0:
                self.log.debug("Refreshing DuckDB connection (periodic)")
                self._refresh_conn()
                conn = self._get_conn()

            result = conn.execute(query, params).fetchall()
            self.log.debug(f"Album search returned {len(result)} results")
            columns = ['id', 'name', 'artistName', 'releaseDate', 'trackCount', 'upc', 'urlTemplate']
            return [dict(zip(columns, row)) for row in result]
        except Exception as e:
            self.log.error(f"Album search error: {e}")
            # Refresh connection on error and retry once
            try:
                self.log.debug("Refreshing DuckDB connection after error")
                self._refresh_conn()
                conn = self._get_conn()
                result = conn.execute(query, params).fetchall()
                columns = ['id', 'name', 'artistName', 'releaseDate', 'trackCount', 'upc', 'urlTemplate']
                return [dict(zip(columns, row)) for row in result]
            except Exception as retry_error:
                self.log.error(f"Album search retry failed: {retry_error}")
                return []

    def _search_albums_parquet(
        self,
        conn,
        artist_name: str = None,
        album_title: str = None,
        limit: int = 100
    ) -> List[Dict]:
        """Search albums by scanning parquet files (slow fallback)."""
        parquet_glob = self._get_parquet_glob('albums')

        conditions = []
        params = []

        if artist_name:
            # Search in primaryArtists array - cast the whole array to string for LIKE search
            conditions.append("LOWER(CAST(primaryArtists AS VARCHAR)) LIKE ?")
            params.append(f"%{artist_name.lower()}%")

        if album_title:
            # Use nameDefault which is a simple VARCHAR
            conditions.append("LOWER(nameDefault) LIKE ?")
            params.append(f"%{album_title.lower()}%")

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        query = f"""
            SELECT
                id,
                nameDefault as name,
                primaryArtists[1].name as artistName,
                CAST(releaseDate AS VARCHAR) as releaseDate,
                len(songs) as trackCount,
                upc,
                urlTemplate
            FROM read_parquet('{parquet_glob}')
            WHERE {where_clause}
            LIMIT {limit}
        """

        try:
            self._query_count += 1
            self.log.debug(f"Album search (parquet): artist={artist_name}, album={album_title}")
            result = conn.execute(query, params).fetchall()
            self.log.debug(f"Album search (parquet) returned {len(result)} results")
            columns = ['id', 'name', 'artistName', 'releaseDate', 'trackCount', 'upc', 'urlTemplate']
            return [dict(zip(columns, row)) for row in result]
        except Exception as e:
            self.log.error(f"Album search error: {e}")
            return []

    def search_songs(
        self,
        artist_name: str = None,
        song_title: str = None,
        album_id: str = None,
        limit: int = 100
    ) -> List[Dict]:
        """
        Search for songs by artist, title, or album.

        Uses indexed DuckDB database if available (fast), otherwise
        falls back to scanning parquet files (slow).

        Args:
            artist_name: Artist name to search for
            song_title: Song title to search for
            album_id: Apple Music album ID to filter by
            limit: Maximum results to return

        Returns:
            List of matching song dicts
        """
        conn = self._get_conn()

        if self._use_indexed_db:
            return self._search_songs_indexed(conn, artist_name, song_title, album_id, limit)
        else:
            return self._search_songs_parquet(conn, artist_name, song_title, album_id, limit)

    def _search_songs_indexed(
        self,
        conn,
        artist_name: str = None,
        song_title: str = None,
        album_id: str = None,
        limit: int = 100
    ) -> List[Dict]:
        """Search songs using indexed database (fast)."""
        conditions = []
        params = []

        if artist_name:
            conditions.append("LOWER(artist_name) LIKE ?")
            params.append(f"%{artist_name.lower()}%")

        if song_title:
            conditions.append("LOWER(name) LIKE ?")
            params.append(f"%{song_title.lower()}%")

        if album_id:
            conditions.append("album_id = ?")
            params.append(album_id)

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        query = f"""
            SELECT
                id,
                name,
                artist_name as artistName,
                album_id as albumId,
                album_name as albumName,
                disc_number as discNumber,
                track_number as trackNumber,
                duration_ms as durationInMillis,
                isrc,
                preview_url as previewUrl
            FROM songs
            WHERE {where_clause}
            LIMIT {limit}
        """

        try:
            self._query_count += 1
            self.log.debug(f"Song search query: artist={artist_name}, song={song_title}, album_id={album_id}")
            result = conn.execute(query, params).fetchall()
            self.log.debug(f"Song search returned {len(result)} results")
            columns = ['id', 'name', 'artistName', 'albumId', 'albumName', 'discNumber', 'trackNumber', 'durationInMillis', 'isrc', 'previewUrl']
            return [dict(zip(columns, row)) for row in result]
        except Exception as e:
            self.log.error(f"Song search error: {e}")
            return []

    def _search_songs_parquet(
        self,
        conn,
        artist_name: str = None,
        song_title: str = None,
        album_id: str = None,
        limit: int = 100
    ) -> List[Dict]:
        """Search songs by scanning parquet files (slow fallback)."""
        parquet_glob = self._get_parquet_glob('songs')

        conditions = []
        params = []

        if artist_name:
            # Search in primaryArtists array
            conditions.append("LOWER(CAST(primaryArtists AS VARCHAR)) LIKE ?")
            params.append(f"%{artist_name.lower()}%")

        if song_title:
            # Use nameDefault which is a simple VARCHAR
            conditions.append("LOWER(nameDefault) LIKE ?")
            params.append(f"%{song_title.lower()}%")

        if album_id:
            # album is a struct with id and name
            conditions.append("album.id = ?")
            params.append(album_id)

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        query = f"""
            SELECT
                id,
                nameDefault as name,
                primaryArtists[1].name as artistName,
                album.id as albumId,
                album.name as albumName,
                volumeNumber as discNumber,
                trackNumber,
                durationInMillis,
                isrc,
                shortPreview as previewUrl
            FROM read_parquet('{parquet_glob}')
            WHERE {where_clause}
            LIMIT {limit}
        """

        try:
            self._query_count += 1
            self.log.debug(f"Song search (parquet): artist={artist_name}, song={song_title}, album_id={album_id}")
            result = conn.execute(query, params).fetchall()
            self.log.debug(f"Song search (parquet) returned {len(result)} results")
            columns = ['id', 'name', 'artistName', 'albumId', 'albumName', 'discNumber', 'trackNumber', 'durationInMillis', 'isrc', 'previewUrl']
            return [dict(zip(columns, row)) for row in result]
        except Exception as e:
            self.log.error(f"Song search error: {e}")
            return []

    def get_album_by_id(self, album_id: str) -> Optional[Dict]:
        """Get a specific album by its Apple Music ID."""
        conn = self._get_conn()

        if self._use_indexed_db:
            query = """
                SELECT
                    id,
                    name,
                    artist_name as artistName,
                    release_date as releaseDate,
                    track_count as trackCount,
                    upc,
                    url_template as urlTemplate
                FROM albums
                WHERE id = ?
                LIMIT 1
            """
        else:
            parquet_glob = self._get_parquet_glob('albums')
            query = f"""
                SELECT
                    id,
                    nameDefault as name,
                    primaryArtists[1].name as artistName,
                    CAST(releaseDate AS VARCHAR) as releaseDate,
                    len(songs) as trackCount,
                    upc,
                    urlTemplate
                FROM read_parquet('{parquet_glob}')
                WHERE id = ?
                LIMIT 1
            """

        try:
            self._query_count += 1
            self.log.debug(f"Album lookup by ID: {album_id}")
            result = conn.execute(query, [album_id]).fetchall()
            self.log.debug(f"Album lookup returned {len(result)} results")
            if result:
                columns = ['id', 'name', 'artistName', 'releaseDate', 'trackCount', 'upc', 'urlTemplate']
                return dict(zip(columns, result[0]))
            return None
        except Exception as e:
            self.log.error(f"Album lookup error: {e}")
            return None

    def get_songs_for_album(self, album_id: str) -> List[Dict]:
        """Get all songs for a specific album."""
        conn = self._get_conn()

        if self._use_indexed_db:
            # Check if songs table exists (might be albums-only mode)
            try:
                table_check = conn.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_name = 'songs'"
                ).fetchone()
                if not table_check:
                    self.log.debug("Songs table not available (albums-only mode)")
                    return []
            except Exception:
                return []

            query = """
                SELECT
                    id,
                    name,
                    artist_name as artistName,
                    album_id as albumId,
                    album_name as albumName,
                    disc_number as discNumber,
                    track_number as trackNumber,
                    duration_ms as durationInMillis,
                    isrc,
                    preview_url as previewUrl
                FROM songs
                WHERE album_id = ?
                ORDER BY disc_number, track_number
            """
        else:
            try:
                parquet_glob = self._get_parquet_glob('songs')
            except FileNotFoundError:
                self.log.debug("Songs parquet files not available")
                return []
            query = f"""
                SELECT
                    id,
                    nameDefault as name,
                    primaryArtists[1].name as artistName,
                    album.id as albumId,
                    album.name as albumName,
                    volumeNumber as discNumber,
                    trackNumber,
                    durationInMillis,
                    isrc,
                    shortPreview as previewUrl
                FROM read_parquet('{parquet_glob}')
                WHERE album.id = ?
                ORDER BY volumeNumber, trackNumber
            """

        try:
            self._query_count += 1
            self.log.debug(f"Get songs for album: {album_id}")
            result = conn.execute(query, [album_id]).fetchall()
            self.log.debug(f"Get songs for album returned {len(result)} tracks")
            columns = ['id', 'name', 'artistName', 'albumId', 'albumName', 'discNumber', 'trackNumber', 'durationInMillis', 'isrc', 'previewUrl']
            return [dict(zip(columns, row)) for row in result]
        except Exception as e:
            self.log.debug(f"Songs for album error: {e}")
            return []

    def get_catalog_stats(self) -> Dict[str, Any]:
        """Get statistics about the loaded catalog."""
        stats = {}

        for feed_name in ['albums', 'artists', 'songs']:
            export_dir = self._get_latest_export_dir(feed_name)
            if export_dir:
                metadata_path = export_dir / 'metadata.txt'
                if metadata_path.exists():
                    with open(metadata_path, 'r') as f:
                        meta = dict(line.strip().split('=', 1) for line in f if '=' in line)
                    stats[feed_name] = {
                        'export_date': meta.get('created_date', 'unknown'),
                        'total_records': int(meta.get('total_records', 0)),
                        'downloaded_at': meta.get('downloaded_at', 'unknown'),
                    }
                else:
                    parquet_files = list(export_dir.glob('*.parquet'))
                    stats[feed_name] = {
                        'export_date': export_dir.name,
                        'part_count': len(parquet_files),
                    }
            else:
                stats[feed_name] = None

        return stats


def is_feed_configured() -> bool:
    """Check if Apple Music Feed is configured (all env vars present)."""
    client = AppleMusicFeedClient()
    return client.is_configured()


def get_catalog_stats() -> Dict[str, Any]:
    """Get stats about downloaded catalog data."""
    catalog = AppleMusicCatalog()
    return catalog.get_catalog_stats()
