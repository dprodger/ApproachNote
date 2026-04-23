"""
Research worker package.

Standalone process (separate Render service) that drains the research_jobs
table. One thread per source so each source's rate limit is isolated.

Entrypoint: `python -m research_worker.run` (see Procfile).

Pieces:
- errors.py     — exception types handlers raise to communicate outcome.
- registry.py   — handler registration decorator + lookup.
- claim.py      — SELECT FOR UPDATE SKIP LOCKED claim/release/finalize.
- quota.py      — atomic quota consume + reset + 403-driven exhaustion.
- loop.py       — per-source thread loop (claim, dispatch, finalize).
- janitor.py    — periodic sweep for stuck jobs and expired done rows.
- run.py        — process entrypoint: wires it all up, handles SIGTERM.
- handlers/     — one module per source; each registers its job_types.
"""
