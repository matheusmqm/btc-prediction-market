import sqlite3
from contextlib import closing

import pytest

from btc_market.models import BtcTrade, Market, Snapshot
from btc_market.storage.database import connect, insert_snapshots, insert_trades, save_market

MARKET = Market(
    slug="btc-updown-15m-1795701600",
    condition_id="0xabc",
    up_token_id="111",
    down_token_id="222",
    start_ms=1_795_701_600_000,
    end_ms=1_795_702_500_000,
    discovered_ms=1_795_701_540_000,
)
TRADE = BtcTrade(agg_trade_id=1, price=60_000.5, trade_ms=10, received_ms=12)


@pytest.fixture
def db(tmp_path):
    with closing(connect(tmp_path / "data" / "test.db")) as connection:
        yield connection


def count(db, table):
    return db.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


def columns(db, table):
    return [row[1] for row in db.execute(f"PRAGMA table_info({table})")]


def test_uses_wal_and_enforces_foreign_keys(db):
    assert db.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    assert db.execute("PRAGMA foreign_keys").fetchone()[0] == 1


def test_creates_the_approved_tables(db):
    tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}

    assert tables == {"market", "market_snapshot", "btc_price", "collector_gap"}
    assert columns(db, "btc_price") == ["agg_trade_id", "price", "trade_ms", "received_ms"]


def test_reopening_keeps_the_data(tmp_path):
    path = tmp_path / "test.db"
    with closing(connect(path)) as first, first:
        insert_trades(first, [TRADE])

    with closing(connect(path)) as second:
        assert count(second, "btc_price") == 1


def test_same_trade_twice_gives_one_row(db):
    insert_trades(db, [TRADE])
    insert_trades(db, [TRADE])

    assert count(db, "btc_price") == 1


def test_invalid_trade_is_not_silently_ignored(db):
    broken = BtcTrade(agg_trade_id=2, price=None, trade_ms=10, received_ms=12)

    with pytest.raises(sqlite3.IntegrityError):
        insert_trades(db, [broken])


def test_saving_a_market_twice_returns_the_same_id(db):
    assert save_market(db, MARKET) == save_market(db, MARKET)
    assert count(db, "market") == 1


def test_snapshot_is_linked_to_its_market(db):
    market_id = save_market(db, MARKET)

    insert_snapshots(db, [Snapshot(market_id, "up", 0.40, 0.42, 10.0, 100, 101)])

    row = db.execute("SELECT * FROM market_snapshot").fetchone()
    assert row[1:] == (market_id, "up", 0.40, 0.42, 10.0, 100, 101)


@pytest.mark.parametrize("market_id, side", [(999, "up"), (1, "sideways")])
def test_rejects_unknown_market_or_side(db, market_id, side):
    save_market(db, MARKET)

    with pytest.raises(sqlite3.IntegrityError):
        insert_snapshots(db, [Snapshot(market_id, side, 0.4, 0.42, 1.0, 100, 101)])
