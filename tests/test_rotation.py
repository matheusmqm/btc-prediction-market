import asyncio

from btc_market.models import Market
from btc_market.sources.polymarket import Rotation

W0 = 1795701600  # início de uma janela
W1, W2 = W0 + 900, W0 + 1800


def market(start):
    return Market(
        f"m{start}", f"c{start}", "u", "d", start * 1000, (start + 900) * 1000, discovered_ms=0
    )


class FakeStream:
    def __init__(self):
        self.active = set()

    async def subscribe(self, market_id, market):
        self.active.add(market_id)

    async def unsubscribe(self, market_id):
        self.active.discard(market_id)


class FakeSource:
    """Faz o papel de Gamma + banco: o id do mercado é o próprio início."""

    def __init__(self):
        self.calls = []
        self.failures = {}  # início -> quantas vezes ainda falhar

    async def __call__(self, start):
        self.calls.append(start)
        if self.failures.get(start, 0):
            self.failures[start] -= 1
            raise ValueError("falha simulada")
        return start, market(start)


def rotation():
    stream, source = FakeStream(), FakeSource()
    rotating = Rotation(stream, source, lead_seconds=60, linger_seconds=5, retry_seconds=10)
    return rotating, stream, source


def run(rotating, stream, *moments):
    """Executa um passo em cada instante e devolve os mercados ativos depois de cada um."""

    async def scenario():
        states = []
        for moment in moments:
            await rotating.step(moment)
            states.append(set(stream.active))
        return states

    return asyncio.run(scenario())


def test_fetches_the_next_market_early_but_subscribes_only_near_the_end():
    rotating, stream, source = rotation()

    states = run(rotating, stream, W0 + 10, W0 + 839, W0 + 840)

    assert source.calls == [W0, W1]  # buscados uma única vez, 14 min antes
    assert states == [{W0}, {W0}, {W0, W1}]


def test_drops_the_old_market_shortly_after_it_ends():
    rotating, stream, source = rotation()

    states = run(rotating, stream, W0 + 850, W1, W1 + 4, W1 + 5)

    assert states == [{W0, W1}, {W0, W1}, {W0, W1}, {W1}]
    assert source.calls == [W0, W1, W2]


def test_retries_a_failed_fetch_after_a_pause(caplog):
    rotating, stream, source = rotation()
    source.failures[W1] = 2

    run(rotating, stream, W0 + 10, W0 + 11, W0 + 20, W0 + 30, W0 + 850)

    assert source.calls == [W0, W1, W1, W1]  # em 11 ainda não tentou de novo
    assert stream.active == {W0, W1}
    assert "falha simulada" in caplog.text


def test_unpublished_market_is_retried():
    stream, calls = FakeStream(), []

    async def fetch(start):
        calls.append(start)
        return None

    rotating = Rotation(stream, fetch, lead_seconds=60, linger_seconds=5, retry_seconds=10)

    run(rotating, stream, W0, W0 + 10)

    assert calls == [W0, W1, W0, W1]
    assert stream.active == set()


def test_a_current_market_that_cannot_be_fetched_is_a_gap():
    losses = []

    class Gaps:
        def lost(self, reason):
            losses.append(reason)

    async def fetch(start):
        return None

    rotating = Rotation(FakeStream(), fetch, lead_seconds=60, linger_seconds=5, gaps=Gaps())

    run(rotating, FakeStream(), W0 + 4)
    assert losses == []  # logo após a virada o mercado anterior ainda está ativo

    run(rotating, FakeStream(), W0 + 6)
    assert losses == ["mercado atual não obtido"]
