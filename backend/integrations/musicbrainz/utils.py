"""
Backward-compat facade for integrations.musicbrainz.

MusicBrainzSearcher moved to `integrations.musicbrainz.client` (#160 step 1).
The three song-metadata updaters moved to
`integrations.musicbrainz.song_updates` (#160 step 4).

Existing callers that do `from integrations.musicbrainz.utils import X` keep
working — new code should import from the real module.
"""

from integrations.musicbrainz.client import MusicBrainzSearcher
from integrations.musicbrainz.song_updates import (
    update_song_composed_year,
    update_song_composer,
    update_song_wikipedia_url,
)

__all__ = [
    'MusicBrainzSearcher',
    'update_song_composer',
    'update_song_wikipedia_url',
    'update_song_composed_year',
]
