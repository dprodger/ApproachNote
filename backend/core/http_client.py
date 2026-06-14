"""Shared HTTP client configuration.

A single home for the outbound User-Agent and a `requests.Session` factory,
so every crawler/integration identifies us the same way and a version bump
is a one-line change rather than a sweep across ~20 files.

Most external services (MusicBrainz, Wikipedia/MediaWiki, Cover Art Archive,
Wikimedia Commons, etc.) expect — and in some cases require — a descriptive
User-Agent. Use `make_session()` to get a session that already carries it.

Note: this only handles identification/headers, not per-service rate
limiting. Clients that must throttle (e.g. MusicBrainz) keep their own
rate-limit logic on top of the session.
"""

import requests

# Outbound identity sent on every API/crawl request. Bump the version here.
HTTP_USER_AGENT = "ApproachNote/1.0 (+support@approachnote.com)"


def make_session(accept_json: bool = True) -> requests.Session:
    """Return a requests.Session preconfigured with our User-Agent.

    Args:
        accept_json: Also set ``Accept: application/json`` (the common case
            for the JSON APIs we call). Pass False for HTML/binary fetches.
    """
    session = requests.Session()
    headers = {'User-Agent': HTTP_USER_AGENT}
    if accept_json:
        headers['Accept'] = 'application/json'
    session.headers.update(headers)
    return session
