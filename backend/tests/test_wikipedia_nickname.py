"""
Tests for WikipediaSearcher._strip_nickname.

Performer names sometimes carry a decorative nickname in quotes
(e.g. '“Brother” Jack McDuff'). The searcher strips that quoted segment so
the lookup and name-matching use the legal name ('Jack McDuff') — which is
what Wikipedia article titles use. Crucially, lone apostrophes in real names
(O'Brien, D'Angelo, leading-apostrophe titles) must be left untouched.

_strip_nickname is pure (no DB/network), so these run without fixtures.
"""

import pytest

from integrations.wikipedia.utils import WikipediaSearcher


@pytest.fixture(scope="module")
def searcher():
    # Construction only sets up cache dirs + an HTTP session; no DB/network.
    return WikipediaSearcher()


@pytest.mark.parametrize(
    "name, expected",
    [
        # Smart double-quote nickname (the Jack McDuff case)
        ("“Brother” Jack McDuff", "Jack McDuff"),
        # Straight double-quote nickname
        ('"Brother" Jack McDuff', "Jack McDuff"),
        # Smart single-quote nickname
        ("‘Papa’ John DeFrancesco", "John DeFrancesco"),
        # Plain names are unchanged
        ("Miles Davis", "Miles Davis"),
        ("John Coltrane", "John Coltrane"),
        # Lone apostrophes must NOT be treated as nickname delimiters
        ("Jack O'Brien", "Jack O'Brien"),
        ("D'Angelo", "D'Angelo"),
        ("'Night, Sweet Pea", "'Night, Sweet Pea"),
        # Trailing quoted segment (album-style title) still strips the quotes
        ("“Brother” Jack McDuff Live!", "Jack McDuff Live!"),
        # Whitespace left by stripping is collapsed
        ("“Brother”  Jack   McDuff", "Jack McDuff"),
        # Guard: stripping that leaves a single bare surname is rejected and
        # the original is kept (a lone surname fuzzy-matches famous people:
        # 'West' -> Kanye West, 'Bower' -> Kris Bowers).
        ("‘Doc’ West", "‘Doc’ West"),
        ("“Bumps” Myers", "“Bumps” Myers"),
        ("“Bugs” Bower", "“Bugs” Bower"),
        ('"Dizzy" Gillespie', '"Dizzy" Gillespie'),
    ],
)
def test_strip_nickname(searcher, name, expected):
    assert searcher._strip_nickname(name) == expected


def test_strip_nickname_never_empties(searcher):
    """A name that is *only* a quoted nickname falls back to the trimmed
    original rather than returning an empty string."""
    assert searcher._strip_nickname("“Brother”") == "“Brother”"
    assert searcher._strip_nickname("   ") == ""
