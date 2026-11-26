"""Fase 0 (descartável): resume uma pasta gravada por capture.py.

Uso: uv run --no-project python spike/analyze.py [pasta]   (padrão: a mais recente)
"""

import gzip
import json
import sys
from collections import Counter
from pathlib import Path


def lines(path: Path):
    with path.open(encoding="utf-8") as file:
        for line in file:
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue  # última linha ainda sendo escrita


def report(name: str, path: Path) -> None:
    counts, sizes, keys, delays, ids = Counter(), Counter(), {}, [], []
    first = last = None
    for record in lines(path):
        first = first or record["recv_ms"]
        last = record["recv_ms"]
        raw = record["raw"]
        if raw == "PONG":
            counts["PONG"] += 1
            continue
        payload = json.loads(raw)
        events = payload if isinstance(payload, list) else [payload]
        for event in events:
            kind = event.get("event_type") or event.get("e") or ("book" if "bids" in event else "?")
            counts[kind] += 1
            sizes[kind] += len(raw) // len(events)
            keys.setdefault(kind, set()).update(event)
            for change in event.get("price_changes", []):
                keys.setdefault("price_change.item", set()).update(change)
            stamp = event.get("E") or event.get("timestamp")
            if stamp:
                delays.append(record["recv_ms"] - int(stamp))
            if kind == "aggTrade":
                ids.append(event["a"])

    seconds = (last - first) / 1000
    raw_bytes = path.stat().st_size
    with path.open("rb") as file:
        sample = file.read(5_000_000)
    ratio = len(sample) / len(gzip.compress(sample))
    per_day = raw_bytes / seconds * 86400 / 1e9

    print(f"\n== {name}: {seconds / 60:.1f} min, {sum(counts.values())} eventos")
    print(f"bruto: {raw_bytes / 1e6:.1f} MB -> {per_day:.2f} GB/dia (gzip reduz {ratio:.1f}x)")
    for kind, count in counts.most_common():
        print(f"  {kind:20} {count:>9}  {count / seconds:8.1f}/s  {sizes[kind] / 1e6:8.1f} MB")
    for kind, fields in keys.items():
        print(f"  campos {kind}: {sorted(fields)}")
    if delays:
        delays.sort()
        print(
            "recebimento - horário da fonte (ms): "
            f"p50={delays[len(delays) // 2]} p99={delays[int(len(delays) * 0.99)]} max={delays[-1]}"
        )
    if ids:
        missing = ids[-1] - ids[0] + 1 - len(set(ids))
        print(f"IDs aggTrade faltando: {missing}; repetidos: {len(ids) - len(set(ids))}")


def main() -> None:
    data = Path(__file__).parent / "data"
    folder = Path(sys.argv[1]) if len(sys.argv) > 1 else sorted(data.iterdir())[-1]
    print(folder)
    report("binance", folder / "binance.jsonl")
    if (folder / "polymarket.jsonl").exists():  # só com capture.py --raw
        report("polymarket", folder / "polymarket.jsonl")
    snapshots = list(lines(folder / "market_snapshot.jsonl")) if (
        folder / "market_snapshot.jsonl"
    ).exists() else []
    if len(snapshots) > 1:
        seconds = (snapshots[-1]["recv_ms"] - snapshots[0]["recv_ms"]) / 1000
        no_size = sum(row["ask_size"] is None for row in snapshots)
        print(f"\n== market_snapshot: {len(snapshots)} linhas, {len(snapshots) / seconds:.1f}/s")
        print(f"linhas sem volume no ask: {no_size}")


if __name__ == "__main__":
    main()
