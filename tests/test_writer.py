import asyncio
import sqlite3
from contextlib import closing, suppress

import pytest

from btc_market.models import BtcTrade, Market, Snapshot
from btc_market.storage.database import connect, save_market
from btc_market.storage.writer import Writer

NEVER = 3600.0  # intervalo longo: nada é gravado por tempo durante o teste


@pytest.fixture
def path(tmp_path):
    return tmp_path / "test.db"


@pytest.fixture
def db(path):
    with closing(connect(path)) as connection:
        yield connection


def trade(trade_id, price=60_000.0):
    return BtcTrade(agg_trade_id=trade_id, price=price, trade_ms=trade_id, received_ms=trade_id)


def saved_ids(path):
    """Lê por outra conexão: só enxerga o que teve commit."""
    with closing(sqlite3.connect(path)) as reader:
        return [row[0] for row in reader.execute("SELECT agg_trade_id FROM btc_price ORDER BY 1")]


async def started(writer):
    task = asyncio.create_task(writer.run())
    await asyncio.sleep(0)
    return task


def test_writes_a_batch_after_the_interval(db, path):
    async def scenario():
        writer = Writer(db, flush_interval=0.01)
        task = await started(writer)
        writer.put(trade(1))
        await asyncio.sleep(0.1)
        assert saved_ids(path) == [1]
        writer.stop()
        await task

    asyncio.run(scenario())


def test_stop_writes_everything_still_queued(db, path):
    async def scenario():
        writer = Writer(db, flush_interval=NEVER)
        task = await started(writer)
        for trade_id in range(1, 501):
            writer.put(trade(trade_id))
        assert saved_ids(path) == []
        writer.stop()
        await asyncio.wait_for(task, timeout=1)

    asyncio.run(scenario())

    assert saved_ids(path) == list(range(1, 501))


def test_cancelling_also_writes_what_is_queued(db, path):
    async def scenario():
        writer = Writer(db, flush_interval=NEVER)
        task = await started(writer)
        writer.put(trade(1))
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    asyncio.run(scenario())

    assert saved_ids(path) == [1]


def test_writes_snapshots_and_trades_together(db, path):
    market_id = save_market(db, Market("slug", "0xabc", "1", "2", 0, 900_000, 0))
    db.commit()
    writer = Writer(db, flush_interval=NEVER)

    writer.put(Snapshot(market_id, "up", 0.40, 0.42, 10.0, 100, 101))
    writer.put(trade(1))
    writer.flush()

    assert db.execute("SELECT count(*) FROM market_snapshot").fetchone()[0] == 1
    assert saved_ids(path) == [1]


def test_one_bad_record_does_not_lose_the_batch(db, path, caplog):
    writer = Writer(db, flush_interval=NEVER)

    writer.put(trade(1))
    writer.put(trade(2, price=None))
    writer.put(trade(3))
    writer.flush()

    assert saved_ids(path) == [1, 3]
    assert "descartado" in caplog.text


def test_keeps_records_while_the_database_is_locked(db, path, caplog):
    db.execute("PRAGMA busy_timeout = 0")
    writer = Writer(db, flush_interval=NEVER)
    writer.put(trade(1))

    with closing(sqlite3.connect(path)) as other:
        other.execute("BEGIN IMMEDIATE")
        writer.flush()
        other.rollback()
    assert saved_ids(path) == []
    assert "tentar de novo" in caplog.text

    writer.flush()
    assert saved_ids(path) == [1]


def test_saving_a_market_is_committed_immediately(db, path):
    market = Market("slug", "0xabc", "1", "2", 0, 900_000, 0)

    market_id = Writer(db, flush_interval=NEVER).save_market(market)

    with closing(sqlite3.connect(path)) as reader:
        assert reader.execute("SELECT id FROM market").fetchall() == [(market_id,)]


def test_refuses_unknown_records(db):
    with pytest.raises(TypeError):
        Writer(db, flush_interval=NEVER).put("texto")
