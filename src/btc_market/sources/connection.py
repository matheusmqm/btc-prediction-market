"""Conexão WebSocket persistente: reconecta com espera crescente e detecta silêncio."""

import asyncio
import logging
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager, nullcontext

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import WebSocketException

logger = logging.getLogger(__name__)


class Reconnect(Exception):
    """Levantada por on_message para pedir uma conexão nova."""


async def keep_connected(
    name: str,
    url: str,
    *,
    on_message: Callable[[str], bool],
    on_lost: Callable[[str], None],
    on_open: Callable[[ClientConnection], AbstractAsyncContextManager] = nullcontext,
    wait_until_ready: Callable[[], object] | None = None,
    silence_timeout: float,
    initial_delay: float = 1.0,
    max_delay: float = 30.0,
) -> None:
    """Roda até ser cancelada.

    on_message devolve True quando a mensagem trouxe dados; isso zera a espera.
    on_open envolve a conexão (assinatura, ping). Sem nenhuma mensagem por
    silence_timeout segundos, a conexão é considerada morta e refeita.
    """
    delay = initial_delay
    while True:
        if wait_until_ready:
            await wait_until_ready()
        try:
            # max_size=None: a foto inicial do livro da Polymarket pode ser grande.
            async with connect(url, max_size=None) as websocket, on_open(websocket):
                logger.info("%s conectada", name)
                while True:
                    try:
                        raw = await asyncio.wait_for(websocket.recv(), silence_timeout)
                    except TimeoutError:
                        raise Reconnect(f"sem mensagens há {silence_timeout:g}s") from None
                    if on_message(raw):
                        delay = initial_delay
        except Reconnect as error:
            reason = str(error)
        except (OSError, TimeoutError, WebSocketException) as error:
            reason = repr(error)
        logger.warning("%s: %s; nova tentativa em %.2fs", name, reason, delay)
        on_lost(reason)
        await asyncio.sleep(delay)
        delay = min(delay * 2, max_delay)
