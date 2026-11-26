"""Ponto de entrada: uv run python -m btc_market (Ctrl+C para parar)."""

import asyncio
import logging
from contextlib import suppress

from btc_market.app import run
from btc_market.config import Config
from btc_market.logs import setup_logging

logger = logging.getLogger("btc_market")


def main() -> None:
    config = Config()
    setup_logging(config.log_dir)
    try:
        with suppress(KeyboardInterrupt):
            asyncio.run(run(config))
    except Exception:
        logger.exception("a coleta parou por um erro")
        raise


if __name__ == "__main__":
    main()
