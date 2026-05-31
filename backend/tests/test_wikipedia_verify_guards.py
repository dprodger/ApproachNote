"""
Tests for the precision guards in WikipediaSearcher.verify_wikipedia_reference.

These guard against nickname-stripped names colliding with a famous *different*
person:

- Non-musician guard: a page whose infobox/lead describes an actor/athlete/etc.
  (and has no music signal) is rejected — catches the actor Kirk Douglas.
- Disambiguation-corroboration guard: a parenthetically disambiguated title
  ("Joe Jones (Fluxus musician)") needs a birth/death-year or song match before
  it's accepted — catches the wrong Joe Jones.
- Hatnote stripping: cross-reference hatnotes ("For the musician, see ...") are
  removed before scoring so the *other* subject's keywords don't leak in.

The page fetch is monkeypatched with crafted HTML, so these run offline.
"""

import pytest

from integrations.wikipedia.utils import WikipediaSearcher


@pytest.fixture(scope="module")
def searcher():
    return WikipediaSearcher()


def _page(title, lead, occupation=None, hatnote=None):
    """Minimal Wikipedia-shaped HTML: h1 heading, optional hatnote, optional
    infobox with an Occupation row, and a lead paragraph."""
    infobox = (
        f'<table class="infobox"><tr><th>Occupation</th>'
        f'<td>{occupation}</td></tr></table>' if occupation else ''
    )
    hat = f'<div class="hatnote">{hatnote}</div>' if hatnote else ''
    return (
        '<html><body>'
        f'<h1 id="firstHeading">{title}</h1>'
        '<div id="mw-content-text"><div class="mw-parser-output">'
        f'{hat}{infobox}<p>{lead}</p>'
        '</div></div></body></html>'
    )


def _verify(searcher, monkeypatch, performer, html, context=None):
    monkeypatch.setattr(searcher, "_fetch_wikipedia_page", lambda url: html)
    ctx = context or {"birth_date": None, "death_date": None, "sample_songs": []}
    return searcher.verify_wikipedia_reference(
        performer, "https://en.wikipedia.org/wiki/X", ctx
    )


def test_non_musician_subject_rejected(searcher, monkeypatch):
    html = _page("John Smith",
                 "John Smith was an American actor and filmmaker.",
                 occupation="Actor, filmmaker")
    result = _verify(searcher, monkeypatch, "John Smith", html)
    assert result["valid"] is False
    assert result["score"] == 0


def test_musician_with_incidental_non_music_word_kept(searcher, monkeypatch):
    # A music term in the lead protects a genuine musician even if a
    # non-musician word also appears.
    html = _page("Jane Doe",
                 "Jane Doe was an American jazz organist and occasional actor.",
                 occupation="Musician")
    result = _verify(searcher, monkeypatch, "Jane Doe", html)
    assert result["valid"] is True


def test_disambiguated_title_without_corroboration_rejected(searcher, monkeypatch):
    html = _page("Joe Test (musician)",
                 "Joe Test was an American jazz drummer.",
                 occupation="Musician")
    result = _verify(searcher, monkeypatch, "Joe Test", html)
    assert result["valid"] is False


def test_disambiguated_title_with_song_corroboration_accepted(searcher, monkeypatch):
    html = _page("Joe Test (musician)",
                 "Joe Test was an American jazz drummer known for Blue Moon.",
                 occupation="Musician")
    ctx = {"birth_date": None, "death_date": None, "sample_songs": ["Blue Moon"]}
    result = _verify(searcher, monkeypatch, "Joe Test", html, ctx)
    assert result["valid"] is True


def test_hatnote_keywords_do_not_rescue_non_musician(searcher, monkeypatch):
    # The hatnote mentions "musician" and points elsewhere; it must be stripped
    # so the actual (actor) subject is still rejected.
    html = _page("Bob Star",
                 "Bob Star was an American actor.",
                 occupation="Actor",
                 hatnote="For the musician, see Bob Star (bandleader).")
    result = _verify(searcher, monkeypatch, "Bob Star", html)
    assert result["valid"] is False
    assert result["score"] == 0
