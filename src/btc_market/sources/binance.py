"""Stream aggTrade da Binance.

A conexão vale no máximo 24h e o servidor envia ping a cada 20s (a biblioteca
websockets responde sozinha). Quedas são tratadas reconectando.
"""

import json
import logging
import time
from collections.abc import Callable

from btc_market.gaps import Gaps, NoGaps
from btc_market.models import BtcTrade
from btc_market.sources.connection import Reconnect, keep_connected

logger = logging.getLogger(__name__)


class ServerShutdown(Reconnect):
    """A Binance avisou que vai desligar o servidor desta conexão."""


def parse_agg_trade(raw: str, received_ms: int) -> BtcTrade | None:
    """Converte uma mensagem em negócio. Mensagem inesperada vira aviso no log e None."""
    try:
        data = json.loads(raw)
        if data.get("e") == "serverShutdown":
            raise ServerShutdown("aviso de desligamento do servidor")
        if data["e"] != "aggTrade":
            raise KeyError("e")
        return BtcTrade(
            agg_trade_id=int(data["a"]),
            price=float(data["p"]),
            trade_ms=int(data["T"]),
            received_ms=received_ms,
        )
    except ValueError, KeyError, TypeError, AttributeError:
        logger.warning("mensagem inesperada da Binance: %.200s", raw)
        return None


async def collect(
    url: str,
    put: Callable[[BtcTrade], None],
    *,
    gaps: Gaps | None = None,
    silence_timeout: float = 15.0,
    initial_delay: float = 1.0,
    max_delay: float = 30.0,
) -> None:
    """Entrega cada negócio a put() e reconecta para sempre, até ser cancelada."""
    gaps = gaps or NoGaps()

    def on_message(raw: str) -> bool:
        trade = parse_agg_trade(raw, received_ms=time.time_ns() // 1_000_000)
        if trade is None:
            return False
        put(trade)
        gaps.data(trade.received_ms)
        return True

    await keep_connected(
        "Binance",
        url,
        on_message=on_message,
        on_lost=gaps.lost,
        silence_timeout=silence_timeout,
        initial_delay=initial_delay,
        max_delay=max_delay,
    )
