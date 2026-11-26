"""uv run --no-project python -m unittest discover -s spike"""

import unittest

from top_of_book import TopOfBook


def book(bid="0.40", ask="0.42", size="10", ts="1000"):
    return {
        "event_type": "book",
        "asset_id": "up",
        "timestamp": ts,
        "bids": [{"price": "0.10", "size": "1"}, {"price": bid, "size": "5"}],
        "asks": [{"price": "0.90", "size": "1"}, {"price": ask, "size": size}],
    }


def change(*items, ts="2000"):
    keys = ("side", "price", "size", "best_bid", "best_ask")
    return {
        "event_type": "price_change",
        "timestamp": ts,
        "price_changes": [{"asset_id": "up", **dict(zip(keys, item))} for item in items],
    }


class TopOfBookTest(unittest.TestCase):
    def setUp(self):
        self.top = TopOfBook()
        self.first = self.top.update(book(), {"up"})

    def test_first_book_is_recorded(self):
        self.assertEqual(
            self.first,
            [{"token": "up", "bid": 0.40, "ask": 0.42, "ask_size": 10.0, "src_ms": 1000}],
        )

    def test_size_only_change_is_ignored(self):
        event = change(("SELL", "0.42", "99", "0.40", "0.42"))
        self.assertEqual(self.top.update(event, {"up"}), [])

    def test_ask_change_is_recorded_with_its_size(self):
        event = change(("SELL", "0.41", "7", "0.40", "0.41"))
        [row] = self.top.update(event, {"up"})
        self.assertEqual((row["bid"], row["ask"], row["ask_size"]), (0.40, 0.41, 7.0))

    def test_bid_change_is_recorded(self):
        event = change(("BUY", "0.41", "3", "0.41", "0.42"))
        [row] = self.top.update(event, {"up"})
        self.assertEqual((row["bid"], row["ask"]), (0.41, 0.42))

    def test_one_message_gives_one_row_with_the_final_state(self):
        event = change(
            ("SELL", "0.42", "0", "0.40", "0.90"),
            ("SELL", "0.43", "4", "0.40", "0.43"),
        )
        [row] = self.top.update(event, {"up"})
        self.assertEqual((row["ask"], row["ask_size"]), (0.43, 4.0))

    def test_unsubscribed_token_and_other_events_are_ignored(self):
        self.assertEqual(self.top.update(book(ask="0.50"), {"down"}), [])
        self.assertEqual(self.top.update({"event_type": "new_market"}, {"up"}), [])


if __name__ == "__main__":
    unittest.main()
