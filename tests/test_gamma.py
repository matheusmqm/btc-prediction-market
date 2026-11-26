import asyncio
import json
from datetime import UTC, datetime

import httpx
import pytest

from btc_market.models import Market
from btc_market.sources.gamma import (
    discover,
    fetch_market,
    parse_market,
    slug_for,
    window_start,
)

START = 1795701600  # 2026-11-26T14:00:00Z
URL = "https://gamma.test"


def event(start=START, **changes):
    """Recorte de uma resposta real de /events?slug=..."""
    iso = lambda ts: datetime.fromtimestamp(ts, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")  # noqa: E731
    market = {
        "conditionId": f"0x{start}",
        "outcomes": '["Up", "Down"]',
        "clobTokenIds": '["111", "222"]',
        "startDate": "2026-11-25T14:08:02.694983Z",  # criação, não o início da janela
        "eventStartTime": iso(start),
        "endDate": iso(start + 900),
    } | changes
    return {"slug": slug_for(start), "markets": [market]}


def ts(text):
    return datetime.fromisoformat(text).timestamp()


@pytest.mark.parametrize(
    "moment, expected",
    [
        ("2026-11-26T14:00:00+00:00", "2026-11-26T14:00:00+00:00"),
        ("2026-11-26T14:14:59.999+00:00", "2026-11-26T14:00:00+00:00"),
        ("2026-11-26T14:15:00+00:00", "2026-11-26T14:15:00+00:00"),
        ("2026-11-26T23:59:59+00:00", "2026-11-26T23:45:00+00:00"),
        ("2026-11-27T00:00:00+00:00", "2026-11-27T00:00:00+00:00"),
    ],
)
def test_window_start(moment, expected):
    assert window_start(ts(moment)) == ts(expected)


def test_slug_uses_the_window_start():
    assert slug_for(START) == "btc-updown-15m-1795701600"


def test_parses_a_market_using_the_event_start_time():
    assert parse_market(event(), START, discovered_ms=5) == Market(
        slug="btc-updown-15m-1795701600",
        condition_id=f"0x{START}",
        up_token_id="111",
        down_token_id="222",
        start_ms=START * 1000,
        end_ms=(START + 900) * 1000,
        discovered_ms=5,
    )


def test_matches_tokens_by_outcome_name_not_position():
    reversed_event = event(outcomes='["Down", "Up"]')

    market = parse_market(reversed_event, START, discovered_ms=5)

    assert (market.up_token_id, market.down_token_id) == ("222", "111")


@pytest.mark.parametrize(
    "changes",
    [
        {"eventStartTime": "2026-11-26T15:00:00Z"},  # não bate com o slug
        {"outcomes": '["Yes", "No"]'},
        {"conditionId": None},
        {"clobTokenIds": "não é json"},
    ],
)
def test_refuses_an_unexpected_response(changes):
    with pytest.raises(ValueError):
        parse_market(event(**changes), START, discovered_ms=5)


def run(handler, call):
    async def scenario():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await call(client)

    return asyncio.run(scenario())


def fetch(handler, attempts=6):
    return run(
        handler,
        lambda client: fetch_market(client, URL, START, attempts=attempts, retry_delay=0),
    )


def test_retries_after_connection_resets_and_server_errors():
    calls = []

    def handler(request):
        calls.append(request)
        if len(calls) == 1:
            raise httpx.ConnectError("reset", request=request)
        if len(calls) == 2:
            return httpx.Response(503)
        return httpx.Response(200, json=[event()])

    assert fetch(handler).slug == slug_for(START)
    assert len(calls) == 3
    assert calls[0].url.params["slug"] == slug_for(START)


def test_gives_up_after_the_last_attempt():
    calls = []

    def handler(request):
        calls.append(request)
        raise httpx.ConnectError("reset", request=request)

    with pytest.raises(httpx.ConnectError):
        fetch(handler, attempts=3)
    assert len(calls) == 3


def test_market_not_published_yet_is_none():
    assert fetch(lambda request: httpx.Response(200, json=[])) is None


def test_discovers_current_and_next_market():
    def handler(request):
        start = int(request.url.params["slug"].rsplit("-", 1)[1])
        return httpx.Response(200, content=json.dumps([event(start)]))

    saved = []

    def save(market):
        saved.append(market)
        return len(saved)

    found = run(handler, lambda c: discover(c, URL, START + 840, save, retry_delay=0))

    assert [m.start_ms // 1000 for m in saved] == [START, START + 900]
    assert found == [(1, saved[0]), (2, saved[1])]


def test_failure_on_one_market_does_not_block_the_other(caplog):
    def handler(request):
        if request.url.params["slug"] == slug_for(START):
            return httpx.Response(404)
        return httpx.Response(200, json=[event(START + 900)])

    found = run(handler, lambda c: discover(c, URL, START, lambda m: 1, retry_delay=0))

    assert [market.slug for _, market in found] == [slug_for(START + 900)]
    assert slug_for(START) in caplog.text
