import asyncio
import json
from contextlib import suppress

from websockets.asyncio.server import serve

from btc_market.models import Market
from btc_market.sources.polymarket import MarketStream

FIRST = Market("first", "0xa", "111", "222", 0, 900_000, 0)
SECOND = Market("second", "0xb", "333", "444", 900_000, 1_800_000, 0)


def book(token, bid="0.40", ask="0.42", ts="1000"):
    return json.dumps(
        {
            "event_type": "book",
            "asset_id": token,
            "timestamp": ts,
            "bids": [{"price": bid, "size": "5"}],
            "asks": [{"price": ask, "size": "10"}],
        }
    )


class Session:
    """Servidor local + MarketStream, para observar mensagens e snapshots."""

    def __init__(self, **options):
        self.options = options
        self.messages = asyncio.Queue()  # o que o coletor enviou
        self.snapshots = asyncio.Queue()  # o que o coletor entregou
        self.websocket = None

    async def _handler(self, websocket):
        self.websocket = websocket
        async for raw in websocket:
            self.messages.put_nowait(raw if raw == "PING" else json.loads(raw))

    async def __aenter__(self):
        self._server = await serve(self._handler, "127.0.0.1", 0)
        port = self._server.sockets[0].getsockname()[1]
        self.stream = MarketStream(
            f"ws://127.0.0.1:{port}",
            self.snapshots.put_nowait,
            initial_delay=0.01,
            **self.options,
        )
        self._task = asyncio.create_task(self.stream.run())
        return self

    async def __aexit__(self, *exc):
        self._task.cancel()
        with suppress(asyncio.CancelledError):
            await self._task
        self._server.close()
        await self._server.wait_closed()

    async def sent(self):
        return await asyncio.wait_for(self.messages.get(), timeout=5)

    async def snapshot(self):
        return await asyncio.wait_for(self.snapshots.get(), timeout=5)


def test_subscribes_both_tokens_and_labels_each_side():
    async def scenario():
        async with Session() as s:
            await s.stream.subscribe(7, FIRST)
            assert await s.sent() == {"assets_ids": ["111", "222"], "type": "market"}
            await s.websocket.send(book("111"))
            await s.websocket.send(book("222", "0.58", "0.60"))

            up, down = await s.snapshot(), await s.snapshot()

            assert (up.market_id, up.side, up.bid, up.ask, up.ask_size) == (
                7,
                "up",
                0.40,
                0.42,
                10.0,
            )
            assert (down.side, down.ask, down.source_ms) == ("down", 0.60, 1000)
            assert up.received_ms > 0

    asyncio.run(scenario())


def test_adds_and_removes_markets_on_the_same_connection():
    async def scenario():
        async with Session() as s:
            await s.stream.subscribe(1, FIRST)
            await s.sent()
            connection = s.websocket

            await s.stream.subscribe(2, SECOND)
            await s.stream.subscribe(2, SECOND)  # repetido: não reenvia
            assert await s.sent() == {"assets_ids": ["333", "444"], "operation": "subscribe"}
            await s.websocket.send(book("333"))
            assert (await s.snapshot()).market_id == 2

            await s.stream.unsubscribe(1)
            assert await s.sent() == {"assets_ids": ["111", "222"], "operation": "unsubscribe"}
            await s.websocket.send(book("111"))  # atrasada: deve ser ignorada
            await s.websocket.send(book("444"))
            assert (await s.snapshot()).side == "down"
            assert s.snapshots.empty()
            assert s.websocket is connection

    asyncio.run(scenario())


def test_reconnecting_resubscribes_and_records_the_book_again():
    async def scenario():
        async with Session() as s:
            await s.stream.subscribe(1, FIRST)
            await s.stream.subscribe(2, SECOND)
            await s.sent()
            await s.websocket.send(book("111", ts="1"))
            first = await s.snapshot()

            await s.websocket.close()

            assert await s.sent() == {
                "assets_ids": ["111", "222", "333", "444"],
                "type": "market",
            }
            await s.websocket.send(book("111", ts="2"))
            second = await s.snapshot()
            assert (first.ask, second.ask) == (0.42, 0.42)
            assert second.source_ms == 2

    asyncio.run(scenario())


def test_does_not_connect_without_markets():
    async def scenario():
        async with Session() as s:
            await asyncio.sleep(0.1)
            assert s.websocket is None

    asyncio.run(scenario())


def test_sends_ping_and_ignores_pong():
    async def scenario():
        async with Session(ping_interval=0.01) as s:
            await s.stream.subscribe(1, FIRST)
            await s.sent()
            assert await s.sent() == "PING"
            await s.websocket.send("PONG")
            await s.websocket.send(book("111"))
            assert (await s.snapshot()).side == "up"

    asyncio.run(scenario())


def test_bad_messages_are_logged_without_dropping_the_connection(caplog):
    async def scenario():
        async with Session() as s:
            await s.stream.subscribe(1, FIRST)
            await s.sent()
            await s.websocket.send("lixo")
            await s.websocket.send(json.dumps({"event_type": "book", "asset_id": "111"}))
            await s.websocket.send(json.dumps([1]))
            await s.websocket.send(book("111"))
            await s.snapshot()

    asyncio.run(scenario())

    assert caplog.text.count("inesperad") == 3
