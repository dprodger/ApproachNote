"""
Handler registration. Each source's handler module imports `handler` and
decorates its functions; importing the module is enough to register.

Each handler receives (payload: dict, ctx: JobContext) and returns a
JSON-serialisable dict that's stored in research_jobs.result. Raise one
of the errors in research_worker.errors to signal a non-success outcome.
"""

from __future__ import annotations

from typing import Any, Callable

# (source, job_type) -> callable(payload, ctx) -> result dict
HandlerFn = Callable[[dict[str, Any], 'JobContext'], dict[str, Any]]  # noqa: F821
HANDLERS: dict[tuple[str, str], HandlerFn] = {}


def handler(source: str, job_type: str) -> Callable[[HandlerFn], HandlerFn]:
    """Decorator: register a handler for (source, job_type)."""
    def deco(fn: HandlerFn) -> HandlerFn:
        key = (source, job_type)
        if key in HANDLERS:
            raise RuntimeError(
                f"Duplicate handler registration for {key}: "
                f"{HANDLERS[key].__qualname__} vs {fn.__qualname__}"
            )
        HANDLERS[key] = fn
        return fn
    return deco


def get_handler(source: str, job_type: str) -> HandlerFn:
    try:
        return HANDLERS[(source, job_type)]
    except KeyError:
        raise LookupError(
            f"No handler registered for source={source!r} job_type={job_type!r}"
        )


def registered_sources() -> set[str]:
    """Sources that have at least one registered handler. The runner spawns
    one worker thread per element."""
    return {source for (source, _job_type) in HANDLERS}
