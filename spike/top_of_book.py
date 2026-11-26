"""Regra de gravação: uma linha só quando o bid ou o ask de um token mudar."""


def _number(value) -> float | None:
    return None if value in (None, "") else float(value)


class TopOfBook:
    """Guarda o livro de asks e o último (bid, ask) gravado de cada token.

    Criar uma instância nova a cada conexão, para refazer o livro.
    """

    def __init__(self) -> None:
        self._asks: dict[str, dict[float, float]] = {}  # token -> {preço: tamanho}
        self._last: dict[str, tuple] = {}  # token -> (bid, ask) gravado

    def update(self, event: dict, tokens: set[str]) -> list[dict]:
        """Aplica um evento e devolve as linhas a gravar (zero ou mais)."""
        top: dict[str, tuple] = {}  # estado final de cada token nesta mensagem
        # O evento "book" inicial pode vir sem event_type.
        kind = event.get("event_type") or ("book" if "bids" in event else None)

        if kind == "book" and event["asset_id"] in tokens:
            token = event["asset_id"]
            asks = {float(a["price"]): float(a["size"]) for a in event["asks"]}
            self._asks[token] = asks
            bid = max((float(b["price"]) for b in event["bids"]), default=None)
            top[token] = (bid, min(asks, default=None))
        elif kind == "price_change":
            for change in event["price_changes"]:
                token = change["asset_id"]
                if token not in tokens:
                    continue
                if change["side"] == "SELL":
                    asks = self._asks.setdefault(token, {})
                    price, size = float(change["price"]), float(change["size"])
                    if size:
                        asks[price] = size
                    else:
                        asks.pop(price, None)
                top[token] = (_number(change.get("best_bid")), _number(change.get("best_ask")))

        rows = []
        for token, (bid, ask) in top.items():
            if self._last.get(token) == (bid, ask):
                continue
            self._last[token] = (bid, ask)
            rows.append({
                "token": token,
                "bid": bid,
                "ask": ask,
                "ask_size": self._asks.get(token, {}).get(ask),
                "src_ms": int(event["timestamp"]),
            })
        return rows
