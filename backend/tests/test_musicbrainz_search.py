"""
Tests for /v1/musicbrainz/works/search.

The case that matters here is the difference between "MusicBrainz answered
and had nothing" and "MusicBrainz never answered". MusicBrainz sheds load
with a 503 often enough that the old behaviour — swallow the error, return
an empty list — told users a standard didn't exist in MusicBrainz whenever
the service hiccuped.

No database is involved; the search route only talks to MusicBrainz.
"""

import pytest
import requests

from integrations.musicbrainz.client import MusicBrainzSearcher, MusicBrainzUnavailable


class FakeResponse:
    def __init__(self, status_code, payload=None):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}

    def json(self):
        return self._payload


@pytest.fixture
def searcher(monkeypatch):
    """A searcher with rate-limit sleeps and retry backoff stubbed out."""
    s = MusicBrainzSearcher()
    monkeypatch.setattr(s, "rate_limit", lambda: None)
    monkeypatch.setattr("integrations.musicbrainz.client.time.sleep", lambda _: None)
    return s


def _work_payload(title="Body and Soul"):
    return {
        "works": [
            {
                "id": "98fb7d1f-12f5-3878-9fad-73266fedbec8",
                "title": title,
                "type": "Song",
                "score": 100,
            }
        ]
    }


def test_search_raises_when_musicbrainz_keeps_returning_503(searcher):
    """A persistent 503 must not be reported to the caller as "no matches"."""
    calls = []

    def always_busy(*args, **kwargs):
        calls.append(kwargs.get("params"))
        return FakeResponse(503)

    searcher.session.get = always_busy

    with pytest.raises(MusicBrainzUnavailable):
        searcher.search_works_multi("Body and Soul")

    assert len(calls) == 3, "should exhaust the three-attempt retry budget"


def test_search_retries_past_a_transient_503(searcher):
    """One 503 followed by a good response yields results, not an empty list."""
    responses = [FakeResponse(503), FakeResponse(200, _work_payload())]

    def flaky(*args, **kwargs):
        return responses.pop(0)

    searcher.session.get = flaky

    results = searcher.search_works_multi("Body and Soul")

    assert [r["title"] for r in results] == ["Body and Soul"]
    assert responses == [], "both queued responses should have been consumed"


def test_search_retries_on_timeout(searcher):
    """Timeouts are transient too, and get the same retry treatment."""
    responses = [requests.exceptions.Timeout(), FakeResponse(200, _work_payload())]

    def flaky(*args, **kwargs):
        item = responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    searcher.session.get = flaky

    results = searcher.search_works_multi("Body and Soul")

    assert len(results) == 1


def test_empty_result_set_is_not_an_error(searcher):
    """A genuine no-match answer still returns an empty list."""
    searcher.session.get = lambda *a, **k: FakeResponse(200, {"works": []})

    assert searcher.search_works_multi("zzzz no such work") == []


def test_client_error_is_not_retried(searcher):
    """A 400 is our bad query, not their outage — retrying just repeats it."""
    calls = []

    def bad_request(*args, **kwargs):
        calls.append(1)
        return FakeResponse(400)

    searcher.session.get = bad_request

    with pytest.raises(MusicBrainzUnavailable):
        searcher.search_works_multi("Body and Soul")

    assert len(calls) == 1


def test_apostrophe_variants_are_normalized(searcher):
    """The curly-apostrophe normalization is a real substitution, not a no-op.

    This line was once flattened by an editor autocorrect into
    ``replace("'", "'")``, silently doing nothing.
    """
    sent = {}

    def capture(*args, **kwargs):
        sent["query"] = kwargs["params"]["query"]
        return FakeResponse(200, _work_payload("It’s a Blue World"))

    searcher.session.get = capture

    searcher.search_works_multi("It’s a Blue World")

    assert sent["query"] == 'work:"It\'s a Blue World"'


def test_route_reports_503_when_musicbrainz_is_down(client, monkeypatch):
    """The endpoint surfaces an outage as 503, not as a successful empty search."""
    def boom(self, title, limit=5):
        raise MusicBrainzUnavailable("MusicBrainz work search failed (HTTP 503)")

    monkeypatch.setattr(MusicBrainzSearcher, "search_works_multi", boom)

    response = client.get("/v1/musicbrainz/works/search?q=Body+and+Soul")

    assert response.status_code == 503
    assert response.get_json()["results"] == []


def test_route_reports_200_when_musicbrainz_has_no_matches(client, monkeypatch):
    """An honest empty answer stays a 200 so the UI can say "no results"."""
    monkeypatch.setattr(
        MusicBrainzSearcher, "search_works_multi", lambda self, title, limit=5: []
    )

    response = client.get("/v1/musicbrainz/works/search?q=zzzz+no+such+work")

    assert response.status_code == 200
    assert response.get_json()["results"] == []
