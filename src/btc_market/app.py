"""Liga as fontes ao escritor e roda a coleta até ser cancelada."""

import asyncio
import logging
from contextlib import closing

import httpx

from btc_market.config import Config
from btc_market.gaps import GapTracker
from btc_market.models import Market
from btc_market.sources import binance, gamma
from btc_market.sources.polymarket import MarketStream, Rotation
from btc_market.storage.database import connect
from btc_market.storage.writer import Writer

logger = logging.getLogger(__name__)


async def run(config: Config, *, client: httpx.AsyncClient | None = None) -> None:
    with closing(connect(config.database_path)) as connection:
        async with client or httpx.AsyncClient(timeout=15) as http:
            await _collect(config, Writer(connection, config.flush_interval_seconds), http)


async def _collect(config: Config, writer: Writer, http: httpx.AsyncClient) -> None:
    # Lido antes de qualquer coleta: marca quando o programa parou da última vez.
    last_activity = writer.last_activity_ms()
    binance_gaps = GapTracker("binance", writer, last_activity)
    polymarket_gaps = GapTracker("polymarket", writer, last_activity)

    async def fetch(start: int) -> tuple[int, Market] | None:
        market = await gamma.fetch_market(http, config.gamma_url, start)
        return None if market is None else (writer.save_market(market), market)

    stream = MarketStream(
        config.polymarket_ws_url,
        writer.put,
        gaps=polymarket_gaps,
        ping_interval=config.ping_interval_seconds,
        silence_timeout=config.polymarket_silence_seconds,
    )
    rotation = Rotation(
        stream,
        fetch,
        lead_seconds=config.subscribe_lead_seconds,
        linger_seconds=config.unsubscribe_delay_seconds,
        gaps=polymarket_gaps,
    )
    sources = [
        asyncio.create_task(
            binance.collect(
                config.binance_ws_url,
                writer.put,
                gaps=binance_gaps,
                silence_timeout=config.binance_silence_seconds,
            )
        ),
        asyncio.create_task(stream.run()),
        asyncio.create_task(rotation.run()),
        asyncio.create_task(
            gamma.run_resolution(
                http,
                config.gamma_url,
                writer.pending_resolutions,
                writer.save_resolution,
                interval=config.resolution_interval_seconds,
                final_price_wait_ms=config.final_price_wait_hours * 3_600_000,
            )
        ),
    ]
    writing = asyncio.create_task(writer.run())
    logger.info("coleta iniciada; banco em %s", config.database_path)
    try:
        # Nenhuma tarefa deveria terminar: se terminar, é um erro e o programa para.
        done, _ = await asyncio.wait([*sources, writing], return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            task.result()
        raise RuntimeError("uma tarefa da coleta terminou sem motivo")
    finally:
        # Primeiro param as fontes, depois o escritor grava o que sobrou.
        for task in sources:
            task.cancel()
        await asyncio.gather(*sources, return_exceptions=True)
        writer.stop()
        await writing
        logger.info("coleta encerrada")
