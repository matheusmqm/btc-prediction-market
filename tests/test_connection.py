import asyncio
from contextlib import suppress

from websockets.asyncio.server import serve

from btc_market.sources.connection import keep_connected


def test_a_silent_connection_is_dropped_and_reopened():
    connections, losses = [], []

    async def handler(websocket):
        connections.append(websocket)
        await websocket.send("oi")
        await websocket.wait_closed()  # fica aberta, sem mandar mais nada

    async def scenario():
        async with serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            task = asyncio.create_task(
                keep_connected(
                    "teste",
                    f"ws://127.0.0.1:{port}",
                    on_message=lambda raw: True,
                    on_lost=losses.append,
                    silence_timeout=0.05,
                    initial_delay=0.01,
                )
            )
            while len(connections) < 3:
                await asyncio.sleep(0.01)
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    asyncio.run(asyncio.wait_for(scenario(), timeout=10))

    assert "sem mensagens" in losses[0]
