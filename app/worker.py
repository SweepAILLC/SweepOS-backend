"""Standalone worker process: ``python -m app.worker``.

Runs two jobs in one container so ops only has to deploy a single worker:

1. **RQ worker** (main thread, when REDIS_URL + USE_RQ_LONG_JOBS) — processes the
   ``sweep_long`` queue (Fathom follow-ups, Call Library LLM batches, etc.).
   Must run on the main thread: RQ uses SIGALRM for job timeouts and crashes in
   a side thread with ``ValueError: signal only works in main thread``.
2. **Automation dispatcher loop** (daemon thread) — polls ``automation_email_jobs``,
   claims due rows, sends via Brevo. Independent of REDIS_URL.

Crash recovery: on boot we sweep all ``sending`` rows back to ``scheduled``.
SIGTERM/SIGINT trigger a graceful drain.
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
_rq_worker: Optional[object] = None


def _handle_signal(_signum, _frame) -> None:
    global _SHUTDOWN
    _SHUTDOWN = True
    LOG.info("received shutdown signal; draining...")
    w = _rq_worker
    if w is not None:
        try:
            w.request_stop()
        except Exception:
            pass


def _run_rq_worker_main() -> None:
    """Run RQ on the main thread (required for SIGALRM-based job monitoring)."""
    global _rq_worker
    if not (settings.REDIS_URL and settings.USE_RQ_LONG_JOBS):
        LOG.info("RQ worker disabled (REDIS_URL/USE_RQ_LONG_JOBS unset)")
        return
    try:
        from redis import Redis
        from rq import Queue, Worker
    except Exception as e:  # noqa: BLE001 - rq optional
        LOG.warning("RQ deps missing (%s); automation dispatcher will still run", e)
        return

    try:
        conn = Redis.from_url(settings.REDIS_URL)
        from app.long_jobs import CALL_LIBRARY_RQ_QUEUE, DEFAULT_RQ_QUEUE

        queues = [
            Queue(CALL_LIBRARY_RQ_QUEUE, connection=conn),
            Queue(DEFAULT_RQ_QUEUE, connection=conn),
        ]
        w = Worker(queues, connection=conn)
        _rq_worker = w
        LOG.info(
            "RQ worker listening on %s (priority) then %s (main thread)",
            CALL_LIBRARY_RQ_QUEUE,
            DEFAULT_RQ_QUEUE,
        )
        w.work(with_scheduler=True, burst=False)
    except Exception:
        LOG.exception("RQ worker crashed")
    finally:
        _rq_worker = None


def _dispatcher_loop() -> None:
    """Tick the automation dispatcher every TICK_INTERVAL seconds."""
    from app.services.automation_dispatcher import (
        recover_all_sending_on_boot,
        tick,
        write_heartbeat,
    )

    TICK_INTERVAL = float(os.environ.get("AUTOMATION_TICK_INTERVAL_SEC", "5"))
    HEARTBEAT_INTERVAL = float(os.environ.get("AUTOMATION_HEARTBEAT_INTERVAL_SEC", "15"))

    with SessionLocal() as db:
        try:
            n = recover_all_sending_on_boot(db)
            if n:
                LOG.info("recovered %d in-flight 'sending' jobs on boot", n)
        except Exception:
            LOG.exception("recovery sweep failed on boot")

    try:
        from app.services.call_library_queue import drain_stuck_pending_all_orgs

        n = drain_stuck_pending_all_orgs()
        if n:
            LOG.info("call_library boot drain requeued=%s", n)
    except Exception:
        LOG.exception("call_library boot drain failed on startup")

    last_heartbeat = 0.0
    last_call_library_drain = 0.0
    last_stripe_catchup = 0.0
    last_instagram_sync = 0.0
    call_library_drain_interval = float(
        getattr(settings, "CALL_LIBRARY_WORKER_DRAIN_INTERVAL_SEC", 180) or 180
    )
    stripe_catchup_interval = float(
        getattr(settings, "STRIPE_CATCHUP_INTERVAL_SEC", 600) or 600
    )
    # Check hourly for due orgs; per-org freshness still uses INSTAGRAM_SYNC_INTERVAL_SEC.
    instagram_sync_check_interval = float(
        getattr(settings, "INSTAGRAM_SYNC_CHECK_INTERVAL_SEC", 3600) or 3600
    )
    if instagram_sync_check_interval <= 0:
        instagram_sync_check_interval = float(
            getattr(settings, "INSTAGRAM_SYNC_INTERVAL_SEC", 86400) or 86400
        )
    while not _SHUTDOWN:
        loop_started = time.time()
        try:
            with SessionLocal() as db:
                attempted = tick(db)
                try:
                    from app.services.funnel_lead_notifications import (
                        flush_due_funnel_lead_digests,
                    )

                    digest_n = flush_due_funnel_lead_digests(db)
                    if digest_n:
                        LOG.info("funnel lead digests: flushed %d org(s)", digest_n)
                except Exception:
                    LOG.exception("funnel lead digest flush failed")
                    try:
                        db.rollback()
                    except Exception:
                        pass
                now = time.time()
                if now - last_heartbeat >= HEARTBEAT_INTERVAL:
                    try:
                        write_heartbeat(db)
                    except Exception:
                        LOG.exception("dispatcher heartbeat failed")
                        db.rollback()
                        try:
                            from app.db.session import engine
                            engine.dispose()
                        except Exception:
                            pass
                    else:
                        last_heartbeat = now
                if now - last_call_library_drain >= call_library_drain_interval:
                    try:
                        from app.services.call_library_queue import drain_stuck_pending_all_orgs

                        n = drain_stuck_pending_all_orgs()
                        if n:
                            LOG.info("call_library worker drain requeued=%s", n)
                    except Exception:
                        LOG.exception("call_library worker drain failed")
                    last_call_library_drain = now
                if stripe_catchup_interval > 0 and now - last_stripe_catchup >= stripe_catchup_interval:
                    try:
                        from app.services.stripe_webhook_onboard import catchup_stripe_recent_for_all_orgs

                        stats = catchup_stripe_recent_for_all_orgs()
                        if stats.get("synced") or stats.get("failed"):
                            LOG.info("stripe catch-up %s", stats)
                    except Exception:
                        LOG.exception("stripe catch-up failed")
                    last_stripe_catchup = now
                if (
                    instagram_sync_check_interval > 0
                    and now - last_instagram_sync >= instagram_sync_check_interval
                ):
                    try:
                        from app.long_jobs import schedule_background_work
                        from app.services.instagram_sync_service import sync_instagram_all_orgs_job

                        # Offload so the automation dispatcher tick is never blocked by Composio.
                        schedule_background_work(
                            sync_instagram_all_orgs_job,
                            None,
                            prefer_rq=True,
                            job_timeout=max(
                                900,
                                int(getattr(settings, "INSTAGRAM_SYNC_BUDGET_SEC", 90) or 90) * 20,
                            ),
                        )
                        LOG.info(
                            "instagram sync check enqueued (check_interval=%ss)",
                            int(instagram_sync_check_interval),
                        )
                    except Exception:
                        LOG.exception("instagram sync enqueue failed")
                    last_instagram_sync = now
                if attempted:
                    LOG.info("dispatcher: processed %d job(s)", attempted)
        except Exception:
            LOG.exception("dispatcher tick failed; backing off briefly")
            time.sleep(2.0)
            continue

        elapsed = time.time() - loop_started
        if elapsed < TICK_INTERVAL and not _SHUTDOWN:
            time.sleep(TICK_INTERVAL - elapsed)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    rq_enabled = bool(settings.REDIS_URL and settings.USE_RQ_LONG_JOBS)
    LOG.info(
        "starting worker pid=%s redis=%s rq=%s",
        os.getpid(),
        bool(settings.REDIS_URL),
        rq_enabled,
    )

    dispatcher_thread = threading.Thread(
        target=_dispatcher_loop,
        name="automation-dispatcher",
        daemon=True,
    )
    dispatcher_thread.start()

    try:
        if rq_enabled:
            _run_rq_worker_main()
        else:
            while not _SHUTDOWN:
                time.sleep(1.0)
    finally:
        LOG.info("worker shutting down")


if __name__ == "__main__":
    main()
    sys.exit(0)
