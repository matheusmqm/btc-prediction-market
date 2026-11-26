"""Escritor único: recebe registros em uma fila e grava em lotes.

As fontes só chamam put(). Para encerrar sem perder nada: pare as fontes,
chame stop() e aguarde run() terminar.
"""

import asyncio
import logging
import sqlite3

from btc_market.gaps import Source
from btc_market.models import BtcTrade, Market, Resolution, Snapshot
from btc_market.storage import database

logger = logging.getLogger(__name__)

Record = Snapshot | BtcTrade


class Writer:
    def __init__(self, connection: sqlite3.Connection, flush_interval: float) -> None:
        self._connection = connection
        self._flush_interval = flush_interval
        self._queue: asyncio.Queue[Record] = asyncio.Queue()
        self._pending: list[Record] = []  # tirados da fila e ainda sem commit
        self._stopping = asyncio.Event()

    def put(self, record: Record) -> None:
        if not isinstance(record, Record):
            raise TypeError(f"registro desconhecido: {type(record).__name__}")
        self._queue.put_nowait(record)

    def save_market(self, market: Market) -> int:
        """Grava na hora, sem fila, porque quem chama precisa do id."""
        with self._connection:
            return database.save_market(self._connection, market)

    def pending_resolutions(self, now_ms: int, final_price_wait_ms: int) -> list[str]:
        return database.pending_resolutions(self._connection, now_ms, final_price_wait_ms)

    def save_resolution(self, slug: str, resolution: Resolution, resolved_ms: int) -> None:
        with self._connection:
            database.save_resolution(self._connection, slug, resolution, resolved_ms)

    def last_activity_ms(self) -> int | None:
        return database.last_activity_ms(self._connection)

    def find_open_gap(self, source: Source) -> int | None:
        return database.find_open_gap(self._connection, source)

    def open_gap(self, source: Source, start_ms: int, reason: str) -> int:
        with self._connection:
            return database.open_gap(self._connection, source, start_ms, reason)

    def close_gap(self, gap_id: int, end_ms: int) -> None:
        with self._connection:
            database.close_gap(self._connection, gap_id, end_ms)

    def stop(self) -> None:
        self._stopping.set()

    async def run(self) -> None:
        """Grava a cada intervalo até stop(); sempre grava o que sobrou ao sair."""
        try:
            while not self._stopping.is_set():
                try:
                    await asyncio.wait_for(self._stopping.wait(), self._flush_interval)
                except TimeoutError:
                    pass
                self.flush()
        finally:
            self.flush()
            if self._pending:
                logger.error("encerrando com %d registros sem gravar", len(self._pending))

    def flush(self) -> None:
        while not self._queue.empty():
            self._pending.append(self._queue.get_nowait())
        if not self._pending:
            return
        try:
            self._write(self._pending)
        except sqlite3.OperationalError as error:  # banco travado, disco cheio...
            logger.error(
                "falha ao gravar %d registros, vai tentar de novo: %s", len(self._pending), error
            )
            return
        except sqlite3.IntegrityError:  # algum registro inválido no lote
            self._write_one_by_one()
            return
        self._pending.clear()

    def _write(self, records: list[Record]) -> None:
        with self._connection:  # uma transação: commit no fim ou rollback no erro
            snapshots = [r for r in records if isinstance(r, Snapshot)]
            trades = [r for r in records if isinstance(r, BtcTrade)]
            database.insert_snapshots(self._connection, snapshots)
            database.insert_trades(self._connection, trades)

    def _write_one_by_one(self) -> None:
        while self._pending:
            record = self._pending[0]
            try:
                self._write([record])
            except sqlite3.IntegrityError as error:
                logger.error("registro inválido descartado: %s (%s)", record, error)
            except sqlite3.OperationalError as error:
                logger.error("falha ao gravar, vai tentar de novo: %s", error)
                return
            self._pending.pop(0)
