import pytest

from btc_market.sources.top_of_book import Quote, TopOfBook


def book(bid="0.40", ask="0.42", size="10", ts="1000", token="up"):
    return {
        "event_type": "book",
        "asset_id": token,
        "timestamp": ts,
        "bids": [{"price": "0.10", "size": "1"}, {"price": bid, "size": "5"}],
        "asks": [{"price": "0.90", "size": "1"}, {"price": ask, "size": size}],
    }


def change(*items, ts="2000"):
    keys = ("side", "price", "size", "best_bid", "best_ask")
    return {
        "event_type": "price_change",
        "timestamp": ts,
        "price_changes": [
            {"asset_id": "up", **dict(zip(keys, item, strict=True))} for item in items
        ],
    }


@pytest.fixture
def top():
    top = TopOfBook({"up"})
    top.update(book())
    return top


def test_first_book_is_recorded():
    assert TopOfBook({"up"}).update(book()) == [Quote("up", 0.40, 0.42, 10.0, 1000)]


def test_size_only_change_is_ignored(top):
    assert top.update(change(("SELL", "0.42", "99", "0.40", "0.42"))) == []


def test_ask_change_is_recorded_with_its_size(top):
    event = change(("SELL", "0.41", "7", "0.40", "0.41"))

    assert top.update(event) == [Quote("up", 0.40, 0.41, 7.0, 2000)]


def test_bid_change_is_recorded(top):
    [quote] = top.update(change(("BUY", "0.41", "3", "0.41", "0.42")))

    assert (quote.bid, quote.ask, quote.ask_size) == (0.41, 0.42, 10.0)


def test_one_message_gives_one_quote_with_the_final_state(top):
    event = change(
        ("SELL", "0.42", "0", "0.40", "0.90"),
        ("SELL", "0.43", "4", "0.40", "0.43"),
    )

    assert top.update(event) == [Quote("up", 0.40, 0.43, 4.0, 2000)]


def test_other_tokens_and_events_are_ignored(top):
    assert top.update(book(token="other")) == []
    assert top.update({"event_type": "last_trade_price", "asset_id": "up"}) == []


def test_change_without_best_prices_only_updates_the_book(top):
    level_only = {
        "event_type": "price_change",
        "timestamp": "2000",
        "price_changes": [{"asset_id": "up", "side": "SELL", "price": "0.41", "size": "7"}],
    }
    assert top.update(level_only) == []

    [quote] = top.update(change(("BUY", "0.41", "1", "0.41", "0.41")))
    assert quote.ask_size == 7.0


def test_empty_side_is_recorded_as_missing(top):
    [quote] = top.update(change(("SELL", "0.42", "0", "0.40", "0")))

    assert (quote.ask, quote.ask_size) == (None, None)


def test_ask_of_one_means_nobody_is_selling(top):
    # Visto ao vivo no fim do mercado: o WebSocket manda "1" e o livro REST não tem ask.
    [quote] = top.update(change(("SELL", "0.42", "0", "0.999", "1")))

    assert (quote.bid, quote.ask, quote.ask_size) == (0.999, None, None)
