"""
Pure-function tests for PerformerImporter.parse_artist_relationships.

Covers the instrument-attribute parsing fix for GH #213: MusicBrainz
`instrument` relationships carry an `attributes` array that mixes real
instrument names with credit qualifiers like "guest"/"solo"/"additional".
Only the instrument names should be stored; the qualifiers must be dropped.

These tests construct relation dicts directly — no DB, no MusicBrainz API.
"""

from __future__ import annotations

import pytest

from integrations.musicbrainz.performer_importer import (
    INSTRUMENT_QUALIFIER_ATTRIBUTES,
    PerformerImporter,
)


@pytest.fixture(scope="module")
def importer():
    return PerformerImporter(dry_run=True)


def _instrument_relation(artist_name, attributes):
    return {
        'type': 'instrument',
        'target-type': 'artist',
        'artist': {'name': artist_name, 'id': 'mbid-123', 'sort-name': artist_name},
        'attributes': attributes,
    }


def test_qualifier_attributes_are_dropped(importer):
    relation = _instrument_relation('John Coltrane', ['guest', 'tenor saxophone'])
    result = importer.parse_artist_relationships([relation])

    assert len(result) == 1
    assert result[0]['instruments'] == ['tenor saxophone']


def test_solo_qualifier_is_dropped(importer):
    relation = _instrument_relation('Miles Davis', ['solo', 'trumpet'])
    result = importer.parse_artist_relationships([relation])

    assert result[0]['instruments'] == ['trumpet']


def test_all_known_qualifiers_dropped_but_instrument_kept(importer):
    attrs = list(INSTRUMENT_QUALIFIER_ATTRIBUTES) + ['piano']
    relation = _instrument_relation('Bill Evans', attrs)
    result = importer.parse_artist_relationships([relation])

    assert result[0]['instruments'] == ['piano']


def test_qualifier_match_is_case_insensitive(importer):
    relation = _instrument_relation('Art Blakey', ['Guest', 'SOLO', 'drums'])
    result = importer.parse_artist_relationships([relation])

    assert result[0]['instruments'] == ['drums']


def test_dict_form_attributes_filtered(importer):
    relation = _instrument_relation(
        'Charlie Parker',
        [{'name': 'guest'}, {'name': 'alto saxophone'}],
    )
    result = importer.parse_artist_relationships([relation])

    assert result[0]['instruments'] == ['alto saxophone']


def test_plain_instrument_unaffected(importer):
    relation = _instrument_relation('Paul Chambers', ['double bass'])
    result = importer.parse_artist_relationships([relation])

    assert result[0]['instruments'] == ['double bass']
