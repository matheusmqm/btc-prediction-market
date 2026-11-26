import asyncio
import json
from contextlib import suppress

from websockets.asyncio.server import serve

from btc_market.models import BtcTrade
from btc_market.sources.binance import collect, parse_agg_trade


def message(trade_id=1):
    """Formato real de uma mensagem aggTrade."""
    return json.dumps(
        {
            "e": "aggTrade",
            "E": 1795701898763,
            "s": "BTCUSDT",
            "a": trade_id,
            "p": "60882.85000000",
            "q": "0.00326000",
            "f": 10,
            "l": 10,
            "T": 1795701898760,
            "m": False,
            "M": True,
        }
    )


def test_parses_a_trade_using_the_trade_time():
    assert parse_agg_trade(message(7), received_ms=99) == BtcTrade(
        agg_trade_id=7, price=60882.85, trade_ms=1795701898760, received_ms=99
    )


def test_ignores_and_logs_unexpected_messages(caplog):
    for raw in ['{"result": null, "id": 1}', "não é json", '{"e": "aggTrade"}', "[1]"]:
        assert parse_agg_trade(raw, received_ms=1) is None

    assert caplog.text.count("inesperada") == 4


def run_against(handler, expected):
    """Roda o coletor contra um servidor local até receber `expected` negócios."""

    async def scenario():
        trades, enough = [], asyncio.Event()

        def put(trade):
            trades.append(trade)
            if len(trades) == expected:
                enough.set()

        async with serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            task = asyncio.create_task(collect(f"ws://127.0.0.1:{port}", put, initial_delay=0.01))
            await asyncio.wait_for(enough.wait(), timeout=5)
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        return trades

    return asyncio.run(scenario())


def test_reconnects_after_the_server_closes():
    connections = []

    async def handler(websocket):
        connections.append(websocket)
        await websocket.send(message(len(connections)))

    trades = run_against(handler, expected=3)

    assert [t.agg_trade_id for t in trades] == [1, 2, 3]
    assert all(t.received_ms > 0 for t in trades)


def test_bad_message_does_not_drop_the_connection():
    async def handler(websocket):
        await websocket.send("lixo")
        await websocket.send(message(5))
        await websocket.wait_closed()

    assert [t.agg_trade_id for t in run_against(handler, expected=1)] == [5]


def test_keeps_trying_while_the_server_is_unreachable(caplog):
    async def scenario():
        task = asyncio.create_task(
            collect("ws://127.0.0.1:1", lambda trade: None, initial_delay=0.01, max_delay=0.02)
        )
        await asyncio.sleep(5)
        assert not task.done()
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    asyncio.run(scenario())

    assert caplog.text.count("nova tentativa") >= 2


def test_server_shutdown_warning_opens_a_new_connection():
    connections = []

    async def handler(websocket):
        connections.append(websocket)
        if len(connections) == 1:
            await websocket.send(json.dumps({"e": "serverShutdown", "E": 1, "T": 2}))
        else:
            await websocket.send(message(9))
        await websocket.wait_closed()  # o servidor não fecha: o coletor precisa sair

    assert [t.agg_trade_id for t in run_against(handler, expected=1)] == [9]
    assert len(connections) == 2
