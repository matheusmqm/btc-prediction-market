"""Registros que passam das fontes para o armazenamento. Horários: UTC em milissegundos."""

from dataclasses import dataclass
from typing import Literal

Side = Literal["up", "down"]


@dataclass(frozen=True, slots=True)
class Market:
    slug: str
    condition_id: str
    up_token_id: str
    down_token_id: str
    start_ms: int
    end_ms: int
    discovered_ms: int


@dataclass(frozen=True, slots=True)
class Snapshot:
    market_id: int
    side: Side
    bid: float | None
    ask: float | None
    ask_size: float | None
    source_ms: int
    received_ms: int


@dataclass(frozen=True, slots=True)
class BtcTrade:
    agg_trade_id: int
    price: float
    trade_ms: int
    received_ms: int


@dataclass(frozen=True, slots=True)
class Resolution:
    outcome: Side
    price_to_beat: float | None
    final_price: float | None  # costuma sair depois do vencedor
