"""Descoberta de mercados e resultados na Gamma API (REST) da Polymarket.

O slug é btc-updown-15m-<início da janela, em segundos>. O início vem de
eventStartTime; o campo startDate é a data de criação do mercado.
"""

import asyncio
import json
import logging
import time
from collections.abc import Callable
from datetime import datetime

import httpx

from btc_market.models import Market, Resolution

logger = logging.getLogger(__name__)

WINDOW_SECONDS = 900


def window_start(timestamp: float) -> int:
    """Início, em segundos, da janela de 15 minutos que contém o instante."""
    return int(timestamp // WINDOW_SECONDS) * WINDOW_SECONDS


def slug_for(start: int) -> str:
    return f"btc-updown-15m-{start}"


def _epoch_seconds(iso: str) -> int:
    return int(datetime.fromisoformat(iso).timestamp())


def parse_market(event: dict, start: int, discovered_ms: int) -> Market:
    """Converte um evento da Gamma. Levanta ValueError se o formato não for o esperado."""
    try:
        data = event["markets"][0]
        tokens = dict(
            zip(json.loads(data["outcomes"]), json.loads(data["clobTokenIds"]), strict=True)
        )
        if _epoch_seconds(data["eventStartTime"]) != start:
            raise ValueError(f"eventStartTime {data['eventStartTime']} não bate com o slug")
        if not data["conditionId"]:
            raise ValueError("conditionId vazio")
        return Market(
            slug=slug_for(start),
            condition_id=data["conditionId"],
            up_token_id=tokens["Up"],
            down_token_id=tokens["Down"],
            start_ms=start * 1000,
            end_ms=_epoch_seconds(data["endDate"]) * 1000,
            discovered_ms=discovered_ms,
        )
    except (KeyError, IndexError, TypeError, ValueError) as error:
        raise ValueError(f"resposta inesperada para {slug_for(start)}: {error!r}") from error


async def _get_events(
    client: httpx.AsyncClient, base_url: str, slug: str, attempts: int, retry_delay: float
) -> list:
    """GET com novas tentativas para quedas de conexão, erro 5xx e limite de uso (429)."""
    for attempt in range(1, attempts + 1):
        try:
            response = await client.get(f"{base_url}/events", params={"slug": slug})
            response.raise_for_status()
            return response.json()
        except (httpx.TransportError, httpx.HTTPStatusError) as error:
            if isinstance(error, httpx.HTTPStatusError):
                status = error.response.status_code
                if status != 429 and status < 500:
                    raise
            if attempt == attempts:
                raise
            logger.info("Gamma falhou (%r); tentativa %d de %d", error, attempt, attempts)
            await asyncio.sleep(retry_delay)
    raise ValueError("attempts precisa ser pelo menos 1")


async def fetch_market(
    client: httpx.AsyncClient,
    base_url: str,
    start: int,
    *,
    attempts: int = 6,
    retry_delay: float = 1.0,
) -> Market | None:
    """Mercado da janela que começa em `start`, ou None se ainda não foi publicado."""
    events = await _get_events(client, base_url, slug_for(start), attempts, retry_delay)
    if not events:
        return None
    return parse_market(events[0], start, discovered_ms=time.time_ns() // 1_000_000)


async def discover(
    client: httpx.AsyncClient,
    base_url: str,
    now: float,
    save: Callable[[Market], int],
    *,
    retry_delay: float = 1.0,
) -> list[tuple[int, Market]]:
    """Grava o mercado atual e o próximo; devolve (id, mercado) dos que conseguiu.

    A falha em um não impede o outro, e quem chama pode tentar de novo depois.
    """
    found = []
    current = window_start(now)
    for start in (current, current + WINDOW_SECONDS):
        try:
            market = await fetch_market(client, base_url, start, retry_delay=retry_delay)
        except (httpx.HTTPError, ValueError) as error:
            logger.error("não foi possível obter %s: %r", slug_for(start), error)
            continue
        if market is None:
            logger.warning("mercado %s ainda não publicado", slug_for(start))
            continue
        found.append((save(market), market))
    return found


def parse_resolution(event: dict) -> Resolution | None:
    """Resultado oficial, ou None enquanto não houver.

    Só vale com status "resolved": antes disso os preços podem estar em 0,9995.
    """
    try:
        data = event["markets"][0]
        if data.get("umaResolutionStatus") != "resolved":
            return None
        prices = [float(price) for price in json.loads(data["outcomePrices"])]
        if sorted(prices) != [0.0, 1.0]:
            return None
        winner = json.loads(data["outcomes"])[prices.index(1.0)]
        metadata = event.get("eventMetadata") or {}
        return Resolution(
            outcome={"Up": "up", "Down": "down"}[winner],
            price_to_beat=metadata.get("priceToBeat"),
            final_price=metadata.get("finalPrice"),
        )
    except (KeyError, IndexError, TypeError, ValueError) as error:
        logger.warning("resultado em formato inesperado: %r", error)
        return None


SaveResolution = Callable[[str, Resolution, int], None]  # slug, resultado, quando soubemos


async def resolve_pending(
    client: httpx.AsyncClient,
    base_url: str,
    slugs: list[str],
    save: SaveResolution,
    *,
    retry_delay: float = 1.0,
) -> None:
    """Consulta uma vez cada mercado; a falha em um não impede os outros."""
    for slug in slugs:
        try:
            events = await _get_events(client, base_url, slug, 6, retry_delay)
            resolution = parse_resolution(events[0]) if events else None
            if resolution:
                save(slug, resolution, time.time_ns() // 1_000_000)
        except Exception as error:  # tenta de novo na próxima rodada
            logger.error("não foi possível resolver %s: %r", slug, error)


async def run_resolution(
    client: httpx.AsyncClient,
    base_url: str,
    pending: Callable[[int, int], list[str]],
    save: SaveResolution,
    *,
    interval: float,
    final_price_wait_ms: int,
) -> None:
    """A cada intervalo, consulta os pendentes. A primeira rodada é imediata,
    o que recupera o que ficou para trás antes de um reinício.
    """
    while True:
        try:
            slugs = pending(time.time_ns() // 1_000_000, final_price_wait_ms)
            await resolve_pending(client, base_url, slugs, save)
        except Exception as error:  # ex.: banco travado
            logger.error("rodada de resolução falhou: %r", error)
        await asyncio.sleep(interval)
