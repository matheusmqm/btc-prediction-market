"""Fase 0 (descartável): quantas linhas sobrariam gravando só quando o topo muda?

Uso: uv run --no-project python spike/filter_sim.py [pasta]
"""

import json
import sqlite3
import sys
import tempfile
from collections import Counter
from pathlib import Path

from analyze import lines


def main() -> None:
    data = Path(__file__).parent / "data"
    # Precisa de mensagens cruas (capture.py --raw).
    folder = Path(sys.argv[1]) if len(sys.argv) > 1 else sorted(data.glob("*/polymarket.jsonl"))[-1].parent

    books: dict[str, dict[float, float]] = {}  # token -> {preço do ask: tamanho}
    best: dict[str, float] = {}  # token -> best_ask informado pela fonte
    previous: dict[str, dict] = {}
    rows: dict[str, list] = {"ask": [], "ask_bid": [], "ask_size": [], "ask_1s": {}}
    total = Counter()
    first = last = None

    for record in lines(folder / "polymarket.jsonl"):
        if record["raw"] == "PONG":
            continue
        payload = json.loads(record["raw"])
        for event in payload if isinstance(payload, list) else [payload]:
            kind = event.get("event_type") or ("book" if "bids" in event else None)
            updates = []  # (token, best_bid, best_ask)
            if kind == "book":
                token = event["asset_id"]
                books[token] = {float(a["price"]): float(a["size"]) for a in event["asks"]}
                bid = max((float(b["price"]) for b in event["bids"]), default=None)
                updates.append((token, bid, min(books[token], default=None)))
            elif kind == "price_change":
                for change in event["price_changes"]:
                    token = change["asset_id"]
                    if change["side"] == "SELL":
                        price, size = float(change["price"]), float(change["size"])
                        if size:
                            books.setdefault(token, {})[price] = size
                        else:
                            books.setdefault(token, {}).pop(price, None)
                    updates.append((token, float(change["best_bid"]), float(change["best_ask"])))
            else:
                continue

            first = first or record["recv_ms"]
            last = record["recv_ms"]
            stamp = int(event["timestamp"])
            for token, bid, ask in updates:
                total["updates"] += 1
                size = books.get(token, {}).get(ask)
                if ask in books.get(token, {}):
                    total["size_known"] += 1
                total["checked"] += 1
                row = (token, stamp, record["recv_ms"], bid, ask, size)
                old = previous.get(token, {})
                if old.get("ask") != ask:
                    rows["ask"].append(row)
                if old.get("ask_bid") != (ask, bid):
                    rows["ask_bid"].append(row)
                if old.get("ask_size") != (ask, size):
                    rows["ask_size"].append(row)
                rows["ask_1s"][(token, stamp // 1000)] = row
                previous[token] = {"ask": ask, "ask_bid": (ask, bid), "ask_size": (ask, size)}

    seconds = (last - first) / 1000
    print(f"{folder.name}: {seconds / 60:.1f} min, {total['updates']} atualizações")
    print(f"tamanho no best ask reconstruído em {total['size_known'] / total['checked']:.1%}")
    for name, selected in rows.items():
        selected = list(selected.values()) if isinstance(selected, dict) else selected
        with tempfile.TemporaryDirectory() as tmp:
            db = sqlite3.connect(Path(tmp) / "t.db")
            db.execute("create table s(token int, src_ms int, recv_ms int, bid real, ask real, size real)")
            db.execute("create index i on s(token, src_ms)")
            ids = {token: n for n, token in enumerate({row[0] for row in selected})}
            db.executemany("insert into s values(?,?,?,?,?,?)", [(ids[r[0]], *r[1:]) for r in selected])
            db.commit()
            db.close()
            mb = (Path(tmp) / "t.db").stat().st_size / 1e6
        print(
            f"  {name:9} {len(selected):>8} linhas ({len(selected) / total['updates']:.2%})"
            f"  ~{mb / seconds * 86400:7.1f} MB/dia"
        )


if __name__ == "__main__":
    main()
