import asyncio
import sqlite3
from contextlib import closing, suppress

import httpx
import pytest

from btc_market.models import Market, Resolution
from btc_market.sources.gamma import parse_resolution, resolve_pending, run_resolution
from btc_market.storage.database import connect
from btc_market.storage.writer import Writer

HOUR = 3_600_000
END = 900_000


def event(prices='["0", "1"]', status="resolved", metadata=None, outcomes='["Up", "Down"]'):
    """Recorte de uma resposta real de um mercado encerrado."""
    market = {"outcomes": outcomes, "outcomePrices": prices, "umaResolutionStatus": status}
    data = {"markets": [market]}
    if metadata is not None:
        data["eventMetadata"] = metadata
    return data


@pytest.mark.parametrize(
    "prices, outcomes, expected",
    [
        ('["1", "0"]', '["Up", "Down"]', "up"),
        ('["0", "1"]', '["Up", "Down"]', "down"),
        ('["1", "0"]', '["Down", "Up"]', "down"),  # pelo nome, não pela posição
    ],
)
def test_reads_the_winner(prices, outcomes, expected):
    assert parse_resolution(event(prices, outcomes=outcomes)).outcome == expected


@pytest.mark.parametrize(
    "response",
    [
        event('["0.9995", "0.0005"]', status=None),  # visto ao vivo: preço quase 1, sem resultado
        event('["1", "0"]', status="proposed"),
        event('["0.5", "0.5"]'),
        {"markets": [{}]},
    ],
)
def test_no_winner_before_the_official_resolution(response):
    assert parse_resolution(response) is None


def test_reads_the_official_prices_when_present():
    response = event(metadata={"priceToBeat": 60787.1, "finalPrice": 60755.0})

    assert parse_resolution(response) == Resolution("down", 60787.1, 60755.0)
    assert parse_resolution(event(metadata={"priceToBeat": 1.5})) == Resolution("down", 1.5, None)
    assert parse_resolution(event()) == Resolution("down", None, None)


@pytest.fixture
def path(tmp_path):
    return tmp_path / "test.db"


@pytest.fixture
def writer(path):
    with closing(connect(path)) as connection:
        yield Writer(connection, flush_interval=1)


def save_market(writer, slug="first", end_ms=END):
    writer.save_market(Market(slug, f"c-{slug}", "1", "2", end_ms - END, end_ms, 0))


def row(path, slug="first"):
    with closing(sqlite3.connect(path)) as reader:
        return reader.execute(
            "SELECT outcome, price_to_beat, final_price, resolved_ms FROM market WHERE slug = ?",
            (slug,),
        ).fetchone()


def test_pending_only_includes_markets_that_ended(writer):
    save_market(writer, "ended", end_ms=END)
    save_market(writer, "running", end_ms=3 * END)

    assert writer.pending_resolutions(now_ms=END, final_price_wait_ms=HOUR) == ["ended"]


def test_stays_pending_until_the_final_price_arrives(writer, path):
    save_market(writer)
    writer.save_resolution("first", Resolution("up", 10.0, None), resolved_ms=111)

    assert writer.pending_resolutions(END + 1, HOUR) == ["first"]

    writer.save_resolution("first", Resolution("up", None, 12.0), resolved_ms=222)

    assert row(path) == ("up", 10.0, 12.0, 111)  # nada é apagado; vale o primeiro horário
    assert writer.pending_resolutions(END + 1, HOUR) == []


def test_gives_up_on_the_final_price_but_never_on_the_winner(writer):
    save_market(writer, "no_final_price")
    save_market(writer, "no_winner")
    writer.save_resolution("no_final_price", Resolution("up", 10.0, None), resolved_ms=1)

    assert writer.pending_resolutions(END + 2 * HOUR, HOUR) == ["no_winner"]


def run_pass(handler, slugs):
    saved = []

    async def scenario():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            await resolve_pending(
                client,
                "https://gamma.test",
                slugs,
                lambda slug, resolution, ms: saved.append((slug, resolution)),
                retry_delay=0,
            )

    asyncio.run(scenario())
    return saved


def test_one_failure_does_not_block_the_other_markets(caplog):
    def handler(request):
        slug = request.url.params["slug"]
        if slug == "broken":
            return httpx.Response(404)
        if slug == "open":
            return httpx.Response(200, json=[event(status=None)])
        return httpx.Response(200, json=[event()])

    saved = run_pass(handler, ["broken", "open", "closed"])

    assert saved == [("closed", Resolution("down", None, None))]
    assert "broken" in caplog.text


def test_keeps_asking_until_the_result_comes_out(writer, path):
    save_market(writer)
    calls = []

    def handler(request):
        calls.append(request)
        if len(calls) < 3:
            return httpx.Response(200, json=[event(status=None)])
        return httpx.Response(200, json=[event(metadata={"priceToBeat": 2.0, "finalPrice": 1.0})])

    async def scenario():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            task = asyncio.create_task(
                run_resolution(
                    client,
                    "https://gamma.test",
                    writer.pending_resolutions,
                    writer.save_resolution,
                    interval=0.01,
                    final_price_wait_ms=10**15,
                )
            )
            await asyncio.sleep(0.3)
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    asyncio.run(scenario())

    assert row(path)[:3] == ("down", 2.0, 1.0)
    assert len(calls) == 3  # depois de resolvido, para de consultar
