import logging

import pytest

from btc_market.logs import setup_logging


@pytest.fixture(autouse=True)
def restore_root_logger():
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    yield
    for handler in root.handlers:
        if handler not in handlers:
            handler.close()
    root.handlers[:] = handlers
    root.setLevel(level)


def test_writes_to_a_file_in_log_dir(tmp_path):
    setup_logging(tmp_path / "logs")

    logging.getLogger("test").info("coleta começou")

    text = (tmp_path / "logs" / "btc_market.log").read_text(encoding="utf-8")
    assert "INFO test: coleta começou" in text
    assert "Z " in text  # horário em UTC


def test_calling_twice_does_not_duplicate_lines(tmp_path):
    setup_logging(tmp_path)
    setup_logging(tmp_path)

    logging.getLogger("test").info("uma vez")

    text = (tmp_path / "btc_market.log").read_text(encoding="utf-8")
    assert text.count("uma vez") == 1
