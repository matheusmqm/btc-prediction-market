"""O programa inteiro contra servidores locais."""

import asyncio
import json
import sqlite3
from contextlib import closing, suppress
from datetime import UTC, datetime

import httpx
from websockets.asyncio.server import serve

from btc_market.app import run
from btc_market.config import Config


def iso(timestamp):
    return datetime.fromtimestamp(timestamp, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def gamma(request):
    start = int(request.url.params["slug"].rsplit("-", 1)[1])
    market = {
        "conditionId": f"0x{start}",
        "outcomes": '["Up", "Down"]',
        "clobTokenIds": json.dumps([f"{start}1", f"{start}2"]),
        "eventStartTime": iso(start),
        "endDate": iso(start + 900),
    }
    return httpx.Response(200, json=[{"markets": [market]}])


trade_ids = iter(range(1, 10**9))


async def binance(websocket):
    while True:
        trade = {"e": "aggTrade", "a": next(trade_ids), "p": "60000", "T": 1}
        await websocket.send(json.dumps(trade))
        await asyncio.sleep(0.01)


async def polymarket(websocket):
    subscription = json.loads(await websocket.recv())
    for token in subscription["assets_ids"]:
        book = {
            "event_type": "book",
            "asset_id": token,
            "timestamp": "1",
            "bids": [{"price": "0.4", "size": "1"}],
            "asks": [{"price": "0.5", "size": "1"}],
        }
        await websocket.send(json.dumps(book))
    await websocket.wait_closed()


def run_for(config_dir, seconds):
    async def scenario():
        async with (
            serve(binance, "127.0.0.1", 0) as first,
            serve(polymarket, "127.0.0.1", 0) as second,
        ):
            url = "ws://127.0.0.1:{}".format
            config = Config(
                gamma_url="https://gamma.test",
                binance_ws_url=url(first.sockets[0].getsockname()[1]),
                polymarket_ws_url=url(second.sockets[0].getsockname()[1]),
                data_dir=config_dir,
                flush_interval_seconds=0.05,
            )
            client = httpx.AsyncClient(transport=httpx.MockTransport(gamma))
            task = asyncio.create_task(run(config, client=client))
            await asyncio.sleep(seconds)
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    asyncio.run(scenario())


def query(path, sql):
    with closing(sqlite3.connect(path)) as reader:
        return reader.execute(sql).fetchall()


def test_collects_everything_and_records_the_time_it_was_off(tmp_path):
    run_for(tmp_path, 0.7)
    database = tmp_path / "btc_market.db"

    assert query(database, "SELECT count(*) FROM market")[0][0] >= 2
    assert query(database, "SELECT count(*) FROM btc_price")[0][0] > 10
    assert query(database, "SELECT DISTINCT side FROM market_snapshot ORDER BY 1") == [
        ("down",),
        ("up",),
    ]
    assert query(database, "SELECT count(*) FROM collector_gap")[0][0] == 0

    run_for(tmp_path, 0.7)

    closed = query(
        database,
        "SELECT source, reason FROM collector_gap WHERE end_ms IS NOT NULL ORDER BY source",
    )
    assert closed == [("binance", "programa parado"), ("polymarket", "programa parado")]
