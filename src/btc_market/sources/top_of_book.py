"""Regra de gravação: uma cotação só quando o bid ou o ask de um token mudar."""

from typing import NamedTuple


class Quote(NamedTuple):
    token: str
    bid: float | None
    ask: float | None
    ask_size: float | None
    source_ms: int


def _price(value: str | None) -> float | None:
    """Lado vazio do livro vira None.

    Preços válidos ficam entre 0 e 1. Sem comprador a fonte manda "0" e sem
    vendedor manda "1"; ausente ou "" também contam como vazio.
    """
    price = float(value) if value else 0.0
    return price if 0 < price < 1 else None


class TopOfBook:
    """Guarda o livro de asks e o último (bid, ask) entregue de cada token.

    Criar uma instância nova a cada conexão, para o livro ser refeito.
    """

    def __init__(self, tokens: set[str]) -> None:
        self._tokens = set(tokens)
        self._asks: dict[str, dict[float, float]] = {}  # token -> {preço: tamanho}
        self._last: dict[str, tuple] = {}  # token -> (bid, ask)

    def add(self, tokens: set[str]) -> None:
        self._tokens |= tokens

    def remove(self, tokens: set[str]) -> None:
        self._tokens -= tokens
        for token in tokens:
            self._asks.pop(token, None)
            self._last.pop(token, None)

    def update(self, event: dict) -> list[Quote]:
        """Aplica um evento e devolve as cotações que mudaram (zero ou mais)."""
        top: dict[str, tuple] = {}  # estado final de cada token nesta mensagem
        # O evento "book" inicial pode vir sem event_type.
        kind = event.get("event_type") or ("book" if "bids" in event else None)

        if kind == "book" and event["asset_id"] in self._tokens:
            token = event["asset_id"]
            asks = {float(a["price"]): float(a["size"]) for a in event["asks"]}
            self._asks[token] = asks
            bid = max((float(b["price"]) for b in event["bids"]), default=None)
            top[token] = (bid, min(asks, default=None))
        elif kind == "price_change":
            for change in event["price_changes"]:
                token = change["asset_id"]
                if token not in self._tokens:
                    continue
                if change["side"] == "SELL":
                    self._set_ask_level(token, float(change["price"]), float(change["size"]))
                if "best_bid" in change or "best_ask" in change:
                    top[token] = (_price(change.get("best_bid")), _price(change.get("best_ask")))

        quotes = []
        for token, (bid, ask) in top.items():
            if self._last.get(token) == (bid, ask):
                continue
            self._last[token] = (bid, ask)
            ask_size = self._asks.get(token, {}).get(ask)
            quotes.append(Quote(token, bid, ask, ask_size, int(event["timestamp"])))
        return quotes

    def _set_ask_level(self, token: str, price: float, size: float) -> None:
        asks = self._asks.setdefault(token, {})
        if size:
            asks[price] = size
        else:
            asks.pop(price, None)
