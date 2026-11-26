"""Fase 0 (descartável): grava os dados das fontes para conhecê-los.

Uso, a partir da raiz do projeto:
    uv run --no-project --with websockets --with httpx python spike/capture.py --minutes 65

Polymarket: market_snapshot.jsonl recebe uma linha só quando o bid ou o ask muda.
Com --raw, grava também todas as mensagens em polymarket.jsonl (cerca de 1 GB/hora).
Binance e --raw usam: {"recv_ms": <relógio local>, "raw": "<mensagem>"}.
JSONL serve só para inspeção; o MVP grava em SQLite.
"""

import argparse
import asyncio
import json
import time
from pathlib import Path

import httpx
import websockets

from top_of_book import TopOfBook

GAMMA_URL = "https://gamma-api.polymarket.com/events"
POLY_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
BINANCE_WS = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"
WINDOW = 900  # segundos


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def window_start(ts: float) -> int:
    return int(ts) - int(ts) % WINDOW


def append(path: Path, record: dict) -> None:
    with path.open("a", encoding="utf-8") as file:
        file.write(json.dumps(record) + "\n")


def log(out: Path, message: str) -> None:
    line = f"{time.strftime('%H:%M:%S')} {message}"
    print(line, flush=True)
    with (out / "capture.log").open("a", encoding="utf-8") as file:
        file.write(line + "\n")


async def fetch_event(client: httpx.AsyncClient, start: int) -> dict | None:
    for attempt in range(1, 7):  # nesta rede a conexão com a Gamma cai com frequência
        try:
            response = await client.get(GAMMA_URL, params={"slug": f"btc-updown-15m-{start}"})
            break
        except httpx.TransportError:
            if attempt == 6:
                raise
            await asyncio.sleep(1)
    response.raise_for_status()
    events = response.json()
    return events[0] if events else None


async def binance(out: Path) -> None:
    while True:
        try:
            async with websockets.connect(BINANCE_WS) as ws:
                log(out, "binance: conectado")
                with (out / "binance.jsonl").open("a", encoding="utf-8") as file:
                    async for raw in ws:
                        file.write(json.dumps({"recv_ms": now_ms(), "raw": raw}) + "\n")
        except Exception as error:  # spike: registrar e reconectar
            log(out, f"binance: caiu ({error!r})")
            await asyncio.sleep(2)


async def polymarket_session(out: Path, client: httpx.AsyncClient, keep_raw: bool) -> None:
    subscribed: dict[int, list[str]] = {}  # início da janela -> token IDs
    top = TopOfBook()  # novo a cada conexão

    async def tokens_for(start: int) -> list[str]:
        event = await fetch_event(client, start)
        append(out / "gamma.jsonl", {"recv_ms": now_ms(), "start": start, "event": event})
        return json.loads(event["markets"][0]["clobTokenIds"])

    async with websockets.connect(POLY_WS, max_size=None) as ws:
        current = window_start(time.time())
        subscribed[current] = await tokens_for(current)
        await ws.send(json.dumps({
            "assets_ids": subscribed[current],
            "type": "market",
            "custom_feature_enabled": True,
        }))
        log(out, f"polymarket: assinado {current}")

        async def read() -> None:
            async for raw in ws:
                recv_ms = now_ms()
                if keep_raw:
                    append(out / "polymarket.jsonl", {"recv_ms": recv_ms, "raw": raw})
                if raw == "PONG":
                    continue
                payload = json.loads(raw)
                tokens = {token for pair in subscribed.values() for token in pair}
                for event in payload if isinstance(payload, list) else [payload]:
                    for row in top.update(event, tokens):
                        append(out / "market_snapshot.jsonl", {**row, "recv_ms": recv_ms})

        async def ping() -> None:
            while True:
                await asyncio.sleep(10)
                await ws.send("PING")

        async def rollover() -> None:
            while True:
                await asyncio.sleep(5)
                now = time.time()
                upcoming = window_start(now) + WINDOW
                if upcoming - now <= 60 and upcoming not in subscribed:
                    subscribed[upcoming] = await tokens_for(upcoming)
                    await ws.send(json.dumps({
                        "assets_ids": subscribed[upcoming],
                        "operation": "subscribe",
                        "custom_feature_enabled": True,
                    }))
                    log(out, f"polymarket: assinado {upcoming} (antecipado)")
                # Mantém o antigo por 3 min após o fim, para ver o market_resolved.
                for start in [s for s in subscribed if now > s + WINDOW + 180]:
                    await ws.send(json.dumps({
                        "assets_ids": subscribed.pop(start),
                        "operation": "unsubscribe",
                    }))
                    log(out, f"polymarket: cancelado {start}")

        async with asyncio.TaskGroup() as group:
            group.create_task(read())
            group.create_task(ping())
            group.create_task(rollover())


async def polymarket(out: Path, client: httpx.AsyncClient, keep_raw: bool) -> None:
    while True:
        try:
            await polymarket_session(out, client, keep_raw)
        except Exception as error:  # spike: registrar e reconectar
            log(out, f"polymarket: caiu ({error!r})")
            await asyncio.sleep(2)


async def watch_gamma(out: Path, client: httpx.AsyncClient) -> None:
    """Passo 0.4: quando aparecem o price to beat e o resultado?"""
    pending: set[int] = set()
    while True:
        pending.add(window_start(time.time()))
        for start in sorted(pending):
            try:
                event = await fetch_event(client, start)
                market = event["markets"][0]
                record = {
                    "recv_ms": now_ms(),
                    "start": start,
                    "closed": market.get("closed"),
                    "outcomePrices": market.get("outcomePrices"),
                    "umaResolutionStatus": market.get("umaResolutionStatus"),
                    "eventMetadata": event.get("eventMetadata"),
                }
                append(out / "resolution.jsonl", record)
                if market.get("umaResolutionStatus") == "resolved" and (
                    event.get("eventMetadata") or {}
                ).get("finalPrice"):
                    pending.discard(start)
            except Exception as error:
                log(out, f"gamma: falhou {start} ({error!r})")
        await asyncio.sleep(20)


async def main(minutes: float, keep_raw: bool) -> None:
    out = Path(__file__).parent / "data" / time.strftime("%Y%m%d-%H%M%S")
    out.mkdir(parents=True)
    log(out, f"gravando em {out} por {minutes} min")
    async with httpx.AsyncClient(timeout=15) as client:
        tasks = [
            asyncio.create_task(binance(out)),
            asyncio.create_task(polymarket(out, client, keep_raw)),
            asyncio.create_task(watch_gamma(out, client)),
        ]
        await asyncio.sleep(minutes * 60)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    log(out, "fim")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--minutes", type=float, default=65)
    parser.add_argument("--raw", action="store_true", help="grava também as mensagens cruas")
    args = parser.parse_args()
    asyncio.run(main(args.minutes, args.raw))
