"""WebSocket de mercado da Polymarket (CLOB) e troca de mercado.

O servidor envia todas as mudanças do livro; o filtro de bid/ask é feito aqui.
Os mercados são trocados na conexão aberta, sem reconectar.
"""

import asyncio
import json
import logging
import time
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager, suppress

from websockets.asyncio.client import ClientConnection
from websockets.exceptions import WebSocketException

from btc_market.gaps import Gaps, NoGaps
from btc_market.models import Market, Side, Snapshot
from btc_market.sources.connection import keep_connected
from btc_market.sources.gamma import WINDOW_SECONDS, slug_for, window_start
from btc_market.sources.top_of_book import TopOfBook

logger = logging.getLogger(__name__)

Tokens = dict[str, tuple[int, Side]]  # token -> (id do mercado, lado)

# Devolve (id, mercado) da janela que começa em `start`, ou None se ainda não existe.
Fetch = Callable[[int], Awaitable[tuple[int, Market] | None]]


def _handle(
    raw: str, top: TopOfBook, tokens: Tokens, put: Callable[[Snapshot], None], received_ms: int
) -> bool:
    """Interpreta uma mensagem e diz se ela trouxe dados do livro.

    Formato inesperado vira aviso no log.
    """
    try:
        payload = json.loads(raw)
        quotes = [
            quote
            for event in (payload if isinstance(payload, list) else [payload])
            for quote in top.update(event)
        ]
    except ValueError, KeyError, TypeError, AttributeError:
        logger.warning("mensagem inesperada da Polymarket: %.200s", raw)
        return False
    for quote in quotes:
        market_id, side = tokens[quote.token]
        put(
            Snapshot(
                market_id,
                side,
                quote.bid,
                quote.ask,
                quote.ask_size,
                quote.source_ms,
                received_ms,
            )
        )
    return True


async def _ping(websocket: ClientConnection, interval: float) -> None:
    with suppress(WebSocketException):  # a queda é tratada por quem está lendo
        while True:
            await asyncio.sleep(interval)
            await websocket.send("PING")


class MarketStream:
    """Conexão única com os mercados assinados. Reconecta até run() ser cancelada."""

    def __init__(
        self,
        url: str,
        put: Callable[[Snapshot], None],
        *,
        gaps: Gaps | None = None,
        ping_interval: float = 10.0,
        silence_timeout: float = 25.0,
        initial_delay: float = 1.0,
        max_delay: float = 30.0,
    ) -> None:
        self._url = url
        self._put = put
        self._gaps = gaps or NoGaps()
        self._ping_interval = ping_interval
        self._silence_timeout = silence_timeout
        self._initial_delay = initial_delay
        self._max_delay = max_delay
        self._tokens: Tokens = {}
        self._has_tokens = asyncio.Event()
        # Só existem enquanto há conexão.
        self._websocket: ClientConnection | None = None
        self._top: TopOfBook | None = None

    async def subscribe(self, market_id: int, market: Market) -> None:
        """Assina Up e Down do mercado. Repetir a chamada não faz nada."""
        new: Tokens = {
            market.up_token_id: (market_id, "up"),
            market.down_token_id: (market_id, "down"),
        }
        if new.keys() <= self._tokens.keys():
            return
        self._tokens.update(new)
        self._has_tokens.set()
        if self._top:
            self._top.add(set(new))
        logger.info("assinando %s", market.slug)
        await self._send_operation("subscribe", list(new))

    async def unsubscribe(self, market_id: int) -> None:
        old = [token for token, (owner, _) in self._tokens.items() if owner == market_id]
        if not old:
            return
        for token in old:
            del self._tokens[token]
        if not self._tokens:
            self._has_tokens.clear()
        if self._top:
            self._top.remove(set(old))
        logger.info("cancelando o mercado de id %d", market_id)
        await self._send_operation("unsubscribe", old)

    async def _send_operation(self, operation: str, tokens: list[str]) -> None:
        if self._websocket:
            # Se a conexão caiu, a reconexão assina a lista atualizada.
            message = json.dumps({"assets_ids": tokens, "operation": operation})
            with suppress(WebSocketException):
                await self._websocket.send(message)

    async def run(self) -> None:
        await keep_connected(
            "Polymarket",
            self._url,
            on_open=self._session,
            on_message=self._on_message,
            on_lost=self._gaps.lost,
            wait_until_ready=self._has_tokens.wait,
            # O PONG chega a cada ping mesmo com o preço parado.
            silence_timeout=self._silence_timeout,
            initial_delay=self._initial_delay,
            max_delay=self._max_delay,
        )

    @asynccontextmanager
    async def _session(self, websocket: ClientConnection):
        """Assina o que está ativo e mantém o ping enquanto a conexão durar."""
        tokens = list(self._tokens)
        self._top = TopOfBook(set(tokens))  # livro novo a cada conexão
        self._websocket = websocket
        pinging = None
        try:
            await websocket.send(json.dumps({"assets_ids": tokens, "type": "market"}))
            pinging = asyncio.create_task(_ping(websocket, self._ping_interval))
            yield
        finally:
            if pinging:
                pinging.cancel()
            self._websocket = self._top = None

    def _on_message(self, raw: str) -> bool:
        received_ms = time.time_ns() // 1_000_000
        if raw == "PONG":
            self._gaps.heartbeat(received_ms)
            return False
        has_data = _handle(raw, self._top, self._tokens, self._put, received_ms)
        if has_data:
            self._gaps.data(received_ms)
        return has_data


class Rotation:
    """Decide quando buscar, assinar e cancelar cada mercado de 15 minutos.

    O próximo é buscado assim que a janela atual começa (sobra tempo para
    falhas) e assinado `lead_seconds` antes de começar.
    """

    def __init__(
        self,
        stream: MarketStream,
        fetch: Fetch,
        *,
        lead_seconds: float,
        linger_seconds: float,
        retry_seconds: float = 10.0,
        gaps: Gaps | None = None,
    ) -> None:
        self._stream = stream
        self._gaps = gaps or NoGaps()
        self._fetch = fetch
        self._lead = lead_seconds
        self._linger = linger_seconds
        self._retry = retry_seconds
        self._known: dict[int, tuple[int, Market]] = {}  # início -> (id, mercado)
        self._retry_at: dict[int, float] = {}

    async def run(self, interval: float = 1.0) -> None:
        while True:
            await self.step(time.time())
            await asyncio.sleep(interval)

    async def step(self, now: float) -> None:
        current = window_start(now)
        upcoming = current + WINDOW_SECONDS
        for start in (current, upcoming):
            await self._ensure_known(start, now)
        if current in self._known:
            await self._stream.subscribe(*self._known[current])
        elif now >= current + self._linger:  # o anterior já foi cancelado
            self._gaps.lost("mercado atual não obtido")
        if upcoming in self._known and upcoming - now <= self._lead:
            await self._stream.subscribe(*self._known[upcoming])
        ended = [s for s in self._known if now >= s + WINDOW_SECONDS + self._linger]
        for start in ended:
            market_id, _ = self._known.pop(start)
            await self._stream.unsubscribe(market_id)

    async def _ensure_known(self, start: int, now: float) -> None:
        if start in self._known or now < self._retry_at.get(start, 0):
            return
        try:
            found = await self._fetch(start)
        except Exception as error:  # nenhuma falha aqui pode parar a coleta
            logger.error("não foi possível obter %s: %r", slug_for(start), error)
            found = None
        if found is None:
            self._retry_at[start] = now + self._retry
        else:
            self._known[start] = found
            self._retry_at.pop(start, None)
