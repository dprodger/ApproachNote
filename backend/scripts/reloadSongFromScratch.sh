#!/bin/bash
#
# Run the full song-research pipeline (MB import, Spotify, Apple,
# JazzStandards.com, authority recs) for a single song. Accepts EITHER
# --id <uuid> or --name <song_name> (or a bare positional name for
# backwards compatibility with the original shape).
#
# Most maintenance ops want --id — uniquely identifies the row, no
# escaping pain. Falls through to a DB lookup for the two children
# (jazzs_extract.py, jazzs_match_authorityrecs.py) that only accept
# --name today.
#
# Equivalent Python entrypoint: research_song.py — see that script for
# additional flags (--mb-only, --skip <child>).

set -euo pipefail

usage() {
    echo "Usage: $0 (--id <uuid> | --name <song_name> | <song_name>)" >&2
    echo "Examples:" >&2
    echo "  $0 'All The Things You Are'" >&2
    echo "  $0 --name 'All The Things You Are'" >&2
    echo "  $0 --id 1a9897bc-0194-4611-a7d5-0396003a29b3" >&2
    exit 1
}

if [ "$#" -eq 0 ]; then
    usage
fi

SONG_ID=""
SONG_NAME=""

case "$1" in
    --id)
        [ "$#" -ge 2 ] || usage
        SONG_ID="$2"
        ;;
    --name)
        [ "$#" -ge 2 ] || usage
        SONG_NAME="$2"
        ;;
    -*)
        usage
        ;;
    *)
        # Bare positional → song name (back-compat with the original shell).
        SONG_NAME="$1"
        ;;
esac

# Resolve whichever arg is missing via psql so the children that take
# only --name still work when we got an --id, and vice versa.
# Requires psql + DATABASE_URL set in the env (the same setup the rest
# of the maintenance scripts assume).
if [ -z "$SONG_ID" ] && [ -n "$SONG_NAME" ]; then
    SONG_ID="$(psql "$DATABASE_URL" -At -c "
        SELECT id FROM songs
        WHERE LOWER(title) = LOWER('$SONG_NAME')
        ORDER BY length(title) ASC
        LIMIT 1
    ")"
    if [ -z "$SONG_ID" ]; then
        echo "ERROR: no song found with name '$SONG_NAME'" >&2
        exit 2
    fi
fi
if [ -z "$SONG_NAME" ] && [ -n "$SONG_ID" ]; then
    SONG_NAME="$(psql "$DATABASE_URL" -At -c "
        SELECT title FROM songs WHERE id = '$SONG_ID'
    ")"
    if [ -z "$SONG_NAME" ]; then
        echo "ERROR: no song found with id $SONG_ID" >&2
        exit 2
    fi
fi

echo "================================================================================"
echo "Song: $SONG_NAME ($SONG_ID)"
echo "================================================================================"

# Children that accept --id are passed the UUID (unambiguous). The two
# JazzStandards.com children only accept --name, so they get the
# resolved title.
python import_mb_releases.py        --id "$SONG_ID"   --force-refresh --limit 2000
python match_spotify_tracks.py      --id "$SONG_ID"   --force-refresh --rematch-all
python match_apple_tracks.py        --id "$SONG_ID"   --force-refresh
python jazzs_extract.py             --name "$SONG_NAME" --force-refresh
python jazzs_match_authorityrecs.py --name "$SONG_NAME"
