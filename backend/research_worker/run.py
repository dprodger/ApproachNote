"""
Worker process entrypoint. Run as `python -m research_worker.run`.

Wires up:
- Path setup so `core.*` and `integrations.*` resolve (matches script_base).
- Logging to stdout (Render captures it).
- One thread per registered (source, job_type), plus a janitor thread.
- Clean shutdown on SIGTERM/SIGINT.

Each handler module must be imported here so its @handler() decorators run
and populate the registry before threads start.
"""

from __future__ import annotations

import logging
import os
import signal
import sys
import threading
from pathlib import Path

# Make `core.*` and `integrations.*` importable when launched from anywhere.
# Mirrors backend/scripts/script_base.py.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Load .env if python-dotenv is available (parity with the Flask app).
try:
    from dotenv import load_dotenv
    env_path = Path(__file__).resolve().parent.parent / '.env'
    if env_path.exists():
        load_dotenv(env_path)
except ImportError:
    pass

# Worker uses pooling — same DB story as the web service.
os.environ.setdefault('DB_USE_POOLING', 'true')

from research_worker import janitor, loop  # noqa: E402
from research_worker.registry import HANDLERS  # noqa: E402

# Import handler modules for side effects (registration). Add a line per
# source as we move them onto the new queue.
from research_worker.handlers import youtube as _youtube_handler  # noqa: E402, F401
from research_worker.handlers import spotify as _spotify_handler  # noqa: E402, F401
from research_worker.handlers import apple as _apple_handler      # noqa: E402, F401


logger = logging.getLogger(__name__)


def _configure_logging() -> None:
    """Stdout-only structured-ish log format. Render aggregates stdout."""
    level_name = os.environ.get('RESEARCH_WORKER_LOG_LEVEL', 'INFO').upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        stream=sys.stdout,
        force=True,
    )


def main() -> int:
    _configure_logging()

    if not HANDLERS:
        logger.error("No handlers registered — refusing to start.")
        return 1

    shutdown = threading.Event()

    def _on_signal(signum, _frame):  # type: ignore[no-untyped-def]
        logger.info("received signal %s; initiating shutdown", signum)
        shutdown.set()

    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    threads: list[threading.Thread] = []

    for (source, job_type) in sorted(HANDLERS):
        t = threading.Thread(
            target=loop.run_source,
            name=f"worker-{source}-{job_type}",
            args=(source, job_type, shutdown),
            daemon=False,
        )
        t.start()
        threads.append(t)

    janitor_thread = threading.Thread(
        target=janitor.run_janitor,
        name='worker-janitor',
        args=(shutdown,),
        daemon=False,
    )
    janitor_thread.start()
    threads.append(janitor_thread)

    logger.info(
        "research_worker started; %d worker thread(s), 1 janitor",
        len(threads) - 1,
    )

    # Block until all threads exit (which happens after `shutdown` is set).
    for t in threads:
        t.join()

    logger.info("research_worker stopped cleanly")
    return 0


if __name__ == '__main__':
    sys.exit(main())
