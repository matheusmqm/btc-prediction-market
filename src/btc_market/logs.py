"""Logs no terminal e em arquivo, com horário em UTC."""

import logging
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path

LOG_FILE_NAME = "btc_market.log"


def setup_logging(log_dir: Path, level: int = logging.INFO) -> None:
    """Configura o logger raiz. Pode ser chamada mais de uma vez."""
    log_dir.mkdir(parents=True, exist_ok=True)

    formatter = logging.Formatter(
        "%(asctime)sZ %(levelname)s %(name)s: %(message)s", datefmt="%Y-%m-%dT%H:%M:%S"
    )
    formatter.converter = time.gmtime

    console = logging.StreamHandler()
    file = RotatingFileHandler(
        log_dir / LOG_FILE_NAME, maxBytes=10_000_000, backupCount=5, encoding="utf-8"
    )
    for handler in (console, file):
        handler.setFormatter(formatter)

    root = logging.getLogger()
    for handler in root.handlers[:]:
        if getattr(handler, "_btc_market", False):
            root.removeHandler(handler)
            handler.close()
    for handler in (console, file):
        handler._btc_market = True
        root.addHandler(handler)
    root.setLevel(level)
    logging.getLogger("httpx").setLevel(logging.WARNING)  # uma linha por requisição é ruído
