import sqlite3
from contextlib import closing

import pytest

from btc_market.gaps import GapTracker
from btc_market.models import BtcTrade, Market, Snapshot
from btc_market.storage.database import connect
from btc_market.storage.writer import Writer


@pytest.fixture
def path(tmp_path):
    return tmp_path / "test.db"


@pytest.fixture
def writer(path):
    with closing(connect(path)) as connection:
        yield Writer(connection, flush_interval=1)


def gaps(path):
    with closing(sqlite3.connect(path)) as reader:
        return reader.execute(
            "SELECT source, start_ms, end_ms, reason FROM collector_gap ORDER BY id"
        ).fetchall()


def test_first_run_has_no_gap(writer, path):
    tracker = GapTracker("binance", writer, last_activity_ms=None)

    tracker.data(100)

    assert gaps(path) == []


def test_a_loss_is_recorded_at_once_and_closed_by_the_next_data(writer, path):
    tracker = GapTracker("binance", writer, last_activity_ms=None)
    tracker.data(100)

    tracker.lost("caiu", now_ms=500)
    tracker.lost("caiu de novo", now_ms=900)  # continua a mesma lacuna

    assert gaps(path) == [("binance", 100, None, "caiu")]

    tracker.data(1_000)

    assert gaps(path) == [("binance", 100, 1_000, "caiu")]


def test_heartbeat_moves_the_start_but_does_not_close(writer, path):
    tracker = GapTracker("polymarket", writer, last_activity_ms=None)
    tracker.data(100)
    tracker.heartbeat(200)
    tracker.lost("caiu", now_ms=500)

    tracker.heartbeat(600)  # PONG: conexão viva, mas ainda sem dados

    assert gaps(path) == [("polymarket", 200, None, "caiu")]


def test_loss_before_any_data_starts_now(writer, path):
    GapTracker("binance", writer, last_activity_ms=None).lost("recusada", now_ms=700)

    assert gaps(path) == [("binance", 700, None, "recusada")]


def test_time_the_program_was_off_is_a_gap(writer, path):
    tracker = GapTracker("binance", writer, last_activity_ms=100)

    assert gaps(path) == [("binance", 100, None, "programa parado")]
    tracker.data(5_000)
    assert gaps(path) == [("binance", 100, 5_000, "programa parado")]


def test_a_gap_left_open_is_continued_after_restart(writer, path):
    GapTracker("binance", writer, last_activity_ms=None).lost("caiu", now_ms=100)

    GapTracker("binance", writer, last_activity_ms=400).data(900)

    assert gaps(path) == [("binance", 100, 900, "caiu")]


def test_clock_going_back_does_not_break_the_gap(writer, path):
    tracker = GapTracker("binance", writer, last_activity_ms=1_000)

    tracker.data(900)  # relógio ajustado para trás

    assert gaps(path)[0][1:3] == (1_000, 1_000)


def test_last_activity_comes_from_the_newest_record_of_any_source(writer):
    assert writer.last_activity_ms() is None
    market_id = writer.save_market(Market("s", "c", "1", "2", 0, 900_000, 0))

    writer.put(BtcTrade(1, 1.0, trade_ms=10, received_ms=300))
    writer.put(Snapshot(market_id, "up", 0.4, 0.5, 1.0, source_ms=10, received_ms=200))
    writer.flush()

    assert writer.last_activity_ms() == 300
