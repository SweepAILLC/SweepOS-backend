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
from typing import List, Optional

# Must be set before first SessionLocal import in this process (and spawn children).
os.environ.setdefault("SWEEP_PROCESS_ROLE", "worker")

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
    last_calendar_catchup = 0.0
    last_whop_catchup = 0.0
    last_ghl_lead_catchup = 0.0
    last_funnel_webhook_prune = 0.0
    # Catch-ups run off-thread (calendar sync can take a while); never overlap one with itself.
    catchup_running: dict = {}

    def _run_catchup_async(name: str, fn) -> None:
        if catchup_running.get(name):
            return
        catchup_running[name] = True

        def _target() -> None:
            try:
                stats = fn()
                if stats.get("synced") or stats.get("failed"):
                    LOG.info("%s catch-up %s", name, stats)
            except Exception:
                LOG.exception("%s catch-up failed", name)
            finally:
                catchup_running[name] = False

        threading.Thread(target=_target, daemon=True, name=f"{name}-catchup").start()

    last_instagram_sync = 0.0
    # 0.0 so the first tick after boot runs it — a deploy applies the rule immediately.
    last_follow_up_sweep = 0.0
    follow_up_sweep_interval = float(
        getattr(settings, "FOLLOW_UP_SWEEP_INTERVAL_SEC", 900) or 900
    )
    call_library_drain_interval = float(
        getattr(settings, "CALL_LIBRARY_WORKER_DRAIN_INTERVAL_SEC", 180) or 180
    )
    stripe_catchup_interval = float(
        getattr(settings, "STRIPE_CATCHUP_INTERVAL_SEC", 600) or 600
    )
    calendar_catchup_interval = float(getattr(settings, "CALENDAR_CATCHUP_INTERVAL_SEC", 300) or 0)
    whop_catchup_interval = float(getattr(settings, "WHOP_CATCHUP_INTERVAL_SEC", 300) or 0)
    # Checks which orgs are due; per-org cadence (15 min, or daily with a live webhook)
    # lives in ghl_lead_sync.is_due.
    ghl_lead_check_interval = 60.0
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
                try:
                    from app.services.inbound_webhook_inbox import (
                        flush_due_inbound_webhooks,
                        retry_unprocessed_stripe_events,
                    )

                    inbound_n = flush_due_inbound_webhooks(db)
                    if inbound_n:
                        LOG.info("inbound webhook retries: processed %d row(s)", inbound_n)
                    stripe_n = retry_unprocessed_stripe_events(db)
                    if stripe_n:
                        LOG.info("stripe event retries: processed %d row(s)", stripe_n)
                except Exception:
                    LOG.exception("inbound webhook retry flush failed")
                    try:
                        db.rollback()
                    except Exception:
                        pass
                if time.time() - last_funnel_webhook_prune >= 3600:
                    try:
                        from app.services.funnel_webhooks import prune_done_deliveries

                        pruned = prune_done_deliveries(db)
                        if pruned:
                            LOG.info("funnel webhook inbox: pruned %d done row(s)", pruned)
                    except Exception:
                        LOG.exception("funnel webhook inbox prune failed")
                        try:
                            db.rollback()
                        except Exception:
                            pass
                    last_funnel_webhook_prune = time.time()
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
                if now - last_follow_up_sweep >= follow_up_sweep_interval:
                    try:
                        from app.services.client_automation import sweep_expired_follow_ups_all_orgs

                        n = sweep_expired_follow_ups_all_orgs(db)
                        if n:
                            LOG.info("follow-up expiry sweep moved %d card(s)", n)
                    except Exception:
                        LOG.exception("follow-up expiry sweep failed")
                        try:
                            db.rollback()
                        except Exception:
                            pass
                    last_follow_up_sweep = now
                if stripe_catchup_interval > 0 and now - last_stripe_catchup >= stripe_catchup_interval:
                    try:
                        from app.services.stripe_webhook_onboard import catchup_stripe_recent_for_all_orgs

                        stats = catchup_stripe_recent_for_all_orgs()
                        if stats.get("synced") or stats.get("failed"):
                            LOG.info("stripe catch-up %s", stats)
                    except Exception:
                        LOG.exception("stripe catch-up failed")
                    last_stripe_catchup = now
                if calendar_catchup_interval > 0 and now - last_calendar_catchup >= calendar_catchup_interval:
                    from app.services.integration_catchup import catchup_calendar_for_all_orgs

                    _run_catchup_async("calendar", catchup_calendar_for_all_orgs)
                    last_calendar_catchup = now
                if whop_catchup_interval > 0 and now - last_whop_catchup >= whop_catchup_interval:
                    from app.services.integration_catchup import catchup_whop_for_all_orgs

                    _run_catchup_async("whop", catchup_whop_for_all_orgs)
                    last_whop_catchup = now
                if now - last_ghl_lead_catchup >= ghl_lead_check_interval:
                    from app.services.ghl_lead_sync import catchup_ghl_leads_for_all_orgs

                    _run_catchup_async("ghl_leads", catchup_ghl_leads_for_all_orgs)
                    last_ghl_lead_catchup = now
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


def _funnel_webhook_drain_loop() -> None:
    """Drain custom funnel webhook deliveries (app.services.funnel_webhooks).

    Claims use FOR UPDATE SKIP LOCKED, so several threads here and in other worker
    instances drain in parallel. Polls every second when idle, immediately when a
    full batch came back (backlog).
    """
    from app.services.funnel_webhooks import drain_due

    batch = 25
    while not _SHUTDOWN:
        claimed = 0
        try:
            with SessionLocal() as db:
                claimed = drain_due(db, limit=batch)
        except Exception:
            LOG.exception("funnel webhook drain failed; backing off briefly")
            time.sleep(2.0)
            continue
        if claimed:
            LOG.info("funnel webhooks: processed %d delivery(ies)", claimed)
        if claimed < batch and not _SHUTDOWN:
            time.sleep(1.0)


def _rq_child_entry() -> None:
    """Spawned RQ process: one job at a time, no automation dispatcher."""
    os.environ["SWEEP_PROCESS_ROLE"] = "worker"
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    _run_rq_worker_main()


def _spawn_extra_rq_workers(extra: int) -> List[object]:
    if extra <= 0:
        return []
    import multiprocessing

    try:
        multiprocessing.set_start_method("spawn", force=True)
    except RuntimeError:
        pass
    procs: List[object] = []
    for i in range(extra):
        p = multiprocessing.Process(
            target=_rq_child_entry,
            name=f"rq-worker-{i + 2}",
            daemon=True,
        )
        p.start()
        procs.append(p)
        LOG.info("spawned extra RQ worker pid=%s", p.pid)
    return procs


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    rq_enabled = bool(settings.REDIS_URL and settings.USE_RQ_LONG_JOBS)
    rq_count = max(1, int(getattr(settings, "RQ_WORKER_COUNT", 1) or 1))
    LOG.info(
        "starting worker pid=%s redis=%s rq=%s rq_workers=%s",
        os.getpid(),
        bool(settings.REDIS_URL),
        rq_enabled,
        rq_count if rq_enabled else 0,
    )

    dispatcher_thread = threading.Thread(
        target=_dispatcher_loop,
        name="automation-dispatcher",
        daemon=True,
    )
    dispatcher_thread.start()

    for i in range(max(0, int(getattr(settings, "FUNNEL_WEBHOOK_DRAIN_THREADS", 2) or 0))):
        threading.Thread(target=_funnel_webhook_drain_loop, name=f"funnel-webhook-drain-{i + 1}", daemon=True).start()

    extra_procs: List[object] = []
    try:
        if rq_enabled:
            extra_procs = _spawn_extra_rq_workers(rq_count - 1)
            _run_rq_worker_main()
        else:
            while not _SHUTDOWN:
                time.sleep(1.0)
    finally:
        for p in extra_procs:
            try:
                p.terminate()
            except Exception:
                pass
        LOG.info("worker shutting down")


if __name__ == "__main__":
    main()
    sys.exit(0)
