"""Conexão e consultas. Nenhuma função faz commit: quem chama controla a transação."""

import sqlite3
from collections.abc import Iterable
from dataclasses import astuple
from importlib.resources import files
from pathlib import Path

from btc_market.gaps import Source
from btc_market.models import BtcTrade, Market, Resolution, Snapshot


def connect(path: Path) -> sqlite3.Connection:
    """Abre o banco, criando a pasta e as tabelas se preciso."""
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA journal_mode = WAL")  # permite ler enquanto se grava
    connection.execute("PRAGMA synchronous = NORMAL")
    connection.execute("PRAGMA foreign_keys = ON")
    schema = files("btc_market.storage").joinpath("schema.sql").read_text(encoding="utf-8")
    connection.executescript(schema)
    return connection


def save_market(connection: sqlite3.Connection, market: Market) -> int:
    """Grava o mercado, se ainda não existir, e devolve o id."""
    connection.execute(
        """
        INSERT INTO market (slug, condition_id, up_token_id, down_token_id,
                            start_ms, end_ms, discovered_ms)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (slug) DO NOTHING
        """,
        astuple(market),
    )
    row = connection.execute("SELECT id FROM market WHERE slug = ?", (market.slug,)).fetchone()
    return row[0]


def insert_snapshots(connection: sqlite3.Connection, snapshots: Iterable[Snapshot]) -> None:
    connection.executemany(
        """
        INSERT INTO market_snapshot (market_id, side, bid, ask, ask_size, source_ms, received_ms)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        map(astuple, snapshots),
    )


def insert_trades(connection: sqlite3.Connection, trades: Iterable[BtcTrade]) -> None:
    """Ignora apenas negócios repetidos; outros erros continuam aparecendo."""
    connection.executemany(
        """
        INSERT INTO btc_price (agg_trade_id, price, trade_ms, received_ms)
        VALUES (?, ?, ?, ?)
        ON CONFLICT (agg_trade_id) DO NOTHING
        """,
        map(astuple, trades),
    )


def pending_resolutions(
    connection: sqlite3.Connection, now_ms: int, final_price_wait_ms: int
) -> list[str]:
    """Slugs de mercados encerrados sem vencedor, ou ainda esperando o preço final.

    O vencedor é buscado para sempre; o preço final, só por um tempo após o fim.
    """
    rows = connection.execute(
        """
        SELECT slug FROM market
        WHERE end_ms <= :now
          AND (outcome IS NULL OR (final_price IS NULL AND end_ms > :now - :wait))
        ORDER BY end_ms
        """,
        {"now": now_ms, "wait": final_price_wait_ms},
    )
    return [slug for (slug,) in rows]


def save_resolution(
    connection: sqlite3.Connection, slug: str, resolution: Resolution, resolved_ms: int
) -> None:
    """Preenche o que ainda está vazio; nunca apaga nem troca um valor já gravado."""
    connection.execute(
        """
        UPDATE market SET
            outcome = coalesce(outcome, ?),
            price_to_beat = coalesce(price_to_beat, ?),
            final_price = coalesce(final_price, ?),
            resolved_ms = coalesce(resolved_ms, ?)
        WHERE slug = ?
        """,
        (
            resolution.outcome,
            resolution.price_to_beat,
            resolution.final_price,
            resolved_ms,
            slug,
        ),
    )


def last_activity_ms(connection: sqlite3.Connection) -> int | None:
    """Quando o programa gravou algo pela última vez.

    Usa a última linha de cada tabela (barato mesmo com o banco grande).
    """
    rows = [
        connection.execute(
            "SELECT received_ms FROM btc_price ORDER BY agg_trade_id DESC LIMIT 1"
        ).fetchone(),
        connection.execute(
            "SELECT received_ms FROM market_snapshot ORDER BY id DESC LIMIT 1"
        ).fetchone(),
    ]
    return max((row[0] for row in rows if row), default=None)


def find_open_gap(connection: sqlite3.Connection, source: Source) -> int | None:
    row = connection.execute(
        "SELECT id FROM collector_gap WHERE source = ? AND end_ms IS NULL ORDER BY id DESC",
        (source,),
    ).fetchone()
    return row[0] if row else None


def open_gap(connection: sqlite3.Connection, source: Source, start_ms: int, reason: str) -> int:
    cursor = connection.execute(
        "INSERT INTO collector_gap (source, start_ms, reason) VALUES (?, ?, ?)",
        (source, start_ms, reason),
    )
    return cursor.lastrowid


def close_gap(connection: sqlite3.Connection, gap_id: int, end_ms: int) -> None:
    # max(): se o relógio for ajustado para trás, o fim não fica antes do início.
    connection.execute(
        "UPDATE collector_gap SET end_ms = max(?, start_ms) WHERE id = ?", (end_ms, gap_id)
    )
