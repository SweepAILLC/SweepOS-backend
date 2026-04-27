"""RQ worker: `python -m app.worker` (requires REDIS_URL). Listens on queue `sweep_long`."""
from __future__ import annotations

import logging
import sys

from redis import Redis
from rq import Queue, Worker

from app.core.config import settings

logger = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    if not settings.REDIS_URL:
        logger.error("REDIS_URL is not set; worker cannot start.")
        sys.exit(1)
    conn = Redis.from_url(settings.REDIS_URL)
    queues = [Queue("sweep_long", connection=conn)]
    w = Worker(queues, connection=conn)
    logger.info("RQ worker listening on queue sweep_long")
    w.work(with_scheduler=False)


if __name__ == "__main__":
    main()
