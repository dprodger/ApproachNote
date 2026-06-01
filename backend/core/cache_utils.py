#!/usr/bin/env python3
"""
Cache Utilities
Provides shared functionality for locating the persistent cache directory
"""

import os
from pathlib import Path


def get_cache_root():
    """
    Get the absolute path to the cache root directory.

    Resolution order:
    - If the CACHE_ROOT env var is set, use it verbatim. The research worker
      sets this to a path on its persistent disk (e.g. /data/cache) so the
      MusicBrainz / Wikipedia caches survive deploys instead of being wiped
      with the ephemeral repo checkout.
    - Otherwise fall back to <backend>/cache, i.e.
      /opt/render/project/src/backend/cache on Render and
      <repo>/backend/cache locally. This file lives in backend/core/, so its
      grandparent is the backend/ directory.

    Returns:
        Path: Absolute path to cache root directory
    """
    env_root = os.environ.get('CACHE_ROOT')
    if env_root:
        cache_root = Path(env_root)
    else:
        # __file__ is backend/core/cache_utils.py -> parent is backend/core,
        # grandparent (backend_dir) is backend/.
        backend_dir = Path(__file__).resolve().parent.parent
        cache_root = backend_dir / 'cache'

    # Ensure the cache directory exists
    cache_root.mkdir(parents=True, exist_ok=True)

    return cache_root


def get_cache_dir(service_name):
    """
    Get the cache directory for a specific service (e.g., 'musicbrainz', 'spotify').
    
    Args:
        service_name: Name of the service (e.g., 'musicbrainz', 'spotify', 'wikipedia')
        
    Returns:
        Path: Absolute path to the service's cache directory
    """
    cache_dir = get_cache_root() / service_name
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir