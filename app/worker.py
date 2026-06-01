"""Standalone worker process: ``python -m app.worker``.

Runs two jobs in one container so ops only has to deploy a single worker:

1. **Automation dispatcher loop** (always on) — polls the ``automation_email_jobs``
   table, claims due rows with ``FOR UPDATE SKIP LOCKED``, materializes the email
   draft, sends via Brevo, persists the outcome. Independent of REDIS_URL so the
   automation engine works in any deployment shape.
2. **RQ worker** (when REDIS_URL + USE_RQ_LONG_JOBS are set) — runs the existing
   ``sweep_long`` queue used by Fathom follow-ups, content studio bundle regen,
   etc. Lives in a daemon thread so a Redis hiccup can't take down the dispatcher.

Crash recovery: on boot we sweep stale ``sending`` rows back to ``scheduled``,
so a SIGKILL never strands an email. SIGTERM/SIGINT trigger a graceful drain.
"""
from __future__ import annotations

import logging
import os
import signal
import sys
import threading
import time
from typing import Optional

from app.core.config import settings
from app.db.session import SessionLocal

LOG = logging.getLogger("app.worker")

_SHUTDOWN = False


def _handle_signal(_signum, _frame) -> None:
    global _SHUTDOWN
    _SHUTDOWN = True
    LOG.info("received shutdown signal; draining...")


def _start_rq_worker_thread() -> Optional[threading.Thread]:
    """Run RQ in a daemon thread when configured. Returns the Thread or None."""
    if not (settings.REDIS_URL and settings.USE_RQ_LONG_JOBS):
        LOG.info("RQ worker disabled (REDIS_URL/USE_RQ_LONG_JOBS unset)")
        return None
    try:
        from redis import Redis
        from rq import Queue, Worker
    except Exception as e:  # noqa: BLE001 - rq optional
        LOG.warning("RQ deps missing (%s); automation dispatcher will still run", e)
        return None

    def _run_rq() -> None:
        try:
            conn = Redis.from_url(settings.REDIS_URL)
            queues = [Queue("sweep_long", connection=conn)]
            w = Worker(queues, connection=conn)
            LOG.info("RQ worker listening on queue sweep_long")
            w.work(with_scheduler=False, burst=False)
        except Exception:
            LOG.exception("RQ worker thread crashed; dispatcher continues")

    t = threading.Thread(target=_run_rq, name="rq-worker", daemon=True)
    t.start()
    return t


def _dispatcher_loop() -> None:
    """Tick the automation dispatcher every TICK_INTERVAL seconds."""
    from app.services.automation_dispatcher import (
        recover_in_flight,
        tick,
        write_heartbeat,
    )

    TICK_INTERVAL = float(os.environ.get("AUTOMATION_TICK_INTERVAL_SEC", "5"))
    HEARTBEAT_INTERVAL = float(os.environ.get("AUTOMATION_HEARTBEAT_INTERVAL_SEC", "15"))

    with SessionLocal() as db:
        try:
            n = recover_in_flight(db)
            if n:
                LOG.info("recovered %d stuck 'sending' jobs on boot", n)
        except Exception:
            LOG.exception("recovery sweep failed on boot")

    last_heartbeat = 0.0
    while not _SHUTDOWN:
        loop_started = time.time()
        try:
            with SessionLocal() as db:
                attempted = tick(db)
                now = time.time()
                if now - last_heartbeat >= HEARTBEAT_INTERVAL:
                    write_heartbeat(db)
                    last_heartbeat = now
                if attempted:
                    LOG.info("dispatcher: processed %d job(s)", attempted)
        except Exception:
            LOG.exception("dispatcher tick failed; backing off briefly")
            time.sleep(2.0)
            continue

        # Hold the loop interval even if tick() ran fast
        elapsed = time.time() - loop_started
        if elapsed < TICK_INTERVAL and not _SHUTDOWN:
            time.sleep(TICK_INTERVAL - elapsed)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    LOG.info(
        "starting worker pid=%s redis=%s rq=%s",
        os.getpid(),
        bool(settings.REDIS_URL),
        bool(settings.REDIS_URL and settings.USE_RQ_LONG_JOBS),
    )

    rq_thread = _start_rq_worker_thread()

    try:
        _dispatcher_loop()
    finally:
        LOG.info("worker shutting down")
        if rq_thread is not None and rq_thread.is_alive():
            # rq worker is daemon, will exit with the process
            pass


if __name__ == "__main__":
    main()
    sys.exit(0)
