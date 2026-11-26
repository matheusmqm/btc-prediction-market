"""Registro das interrupções de cada fonte em collector_gap.

A lacuna é gravada assim que começa (sobrevive a um processo morto) e fechada
quando os dados voltam. Começa no último sinal de vida, não na detecção.
"""

import logging
import time
from typing import Literal, Protocol

logger = logging.getLogger(__name__)

Source = Literal["binance", "polymarket"]


class GapStore(Protocol):
    def find_open_gap(self, source: Source) -> int | None: ...

    def open_gap(self, source: Source, start_ms: int, reason: str) -> int: ...

    def close_gap(self, gap_id: int, end_ms: int) -> None: ...


class Gaps(Protocol):
    """O que as fontes informam."""

    def heartbeat(self, now_ms: int) -> None: ...

    def data(self, now_ms: int) -> None: ...

    def lost(self, reason: str) -> None: ...


class NoGaps:
    """Para quem não precisa registrar lacunas (testes e experimentos)."""

    def heartbeat(self, now_ms: int) -> None:
        pass

    def data(self, now_ms: int) -> None:
        pass

    def lost(self, reason: str) -> None:
        pass


class GapTracker:
    def __init__(self, source: Source, store: GapStore, last_activity_ms: int | None) -> None:
        """last_activity_ms: último registro gravado antes deste início, se houver."""
        self._source = source
        self._store = store
        self._alive_ms = last_activity_ms
        self._open = store.find_open_gap(source)  # lacuna deixada por uma execução anterior
        if self._open is None and last_activity_ms is not None:
            self._start(last_activity_ms, "programa parado")

    def heartbeat(self, now_ms: int) -> None:
        """A conexão está viva (ex.: PONG), mas isso não traz dados."""
        self._alive_ms = now_ms

    def data(self, now_ms: int) -> None:
        self._alive_ms = now_ms
        if self._open is None:
            return
        try:
            self._store.close_gap(self._open, now_ms)
        except Exception as error:  # tenta de novo no próximo dado
            logger.error("não foi possível fechar a lacuna de %s: %r", self._source, error)
            return
        logger.info("%s: dados voltaram", self._source)
        self._open = None

    def lost(self, reason: str, now_ms: int | None = None) -> None:
        if self._open is not None:
            return
        now_ms = now_ms if now_ms is not None else time.time_ns() // 1_000_000
        self._start(self._alive_ms if self._alive_ms is not None else now_ms, reason)

    def _start(self, start_ms: int, reason: str) -> None:
        try:
            self._open = self._store.open_gap(self._source, start_ms, reason)
        except Exception as error:  # o registro de lacuna não pode parar a coleta
            logger.error("não foi possível registrar lacuna de %s: %r", self._source, error)
