import asyncio
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import websockets
import aiohttp

SYMBOLS = [
    "btcusdt", "ethusdt", "bnbusdt", "solusdt", "xrpusdt",
    "dogeusdt", "adausdt", "avaxusdt", "dotusdt", "linkusdt",
]

SPOT_WS_URL = "wss://data-stream.binance.vision/stream?streams=" + "/".join(
    f"{s}@aggTrade" for s in SYMBOLS
)
FUT_REST_BASE = "https://fapi.binance.com"

COLLECT_SECONDS = 240
FUT_POLL_INTERVAL = 10.0    # секунд между REST-опросами фьючерсов
DATA_FILE = Path("data/history.json")
MAX_MINUTES_PER_PAIR = 8000

spot_buckets = {s: {"buy": 0.0, "sell": 0.0} for s in SYMBOLS}
fut_buckets  = {s: {"buy": 0.0, "sell": 0.0} for s in SYMBOLS}
fut_last_id  = {s: None for s in SYMBOLS}
last_minute = int(time.time() // 60)
flush_lock = asyncio.Lock()


def load_history():
    if DATA_FILE.exists():
        try:
            return json.loads(DATA_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def save_history(data):
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    DATA_FILE.write_text(
        json.dumps(data, separators=(",", ":"), ensure_ascii=False),
        encoding="utf-8",
    )


async def flush_minute(minute_ts):
    async with flush_lock:
        history = load_history()
        saved_any = False

        for symbol in SYMBOLS:
            sym_upper = symbol.upper()
            if sym_upper not in history:
                history[sym_upper] = {"spot": [], "fut": []}

            sb = spot_buckets[symbol]
            total = sb["buy"] + sb["sell"]
            if total > 0:
                delta_ratio = round((sb["buy"] - sb["sell"]) / total, 5)
                arr = history[sym_upper]["spot"]
                if not arr or arr[-1][0] != minute_ts:
                    arr.append([minute_ts, delta_ratio])
                    if len(arr) > MAX_MINUTES_PER_PAIR:
                        history[sym_upper]["spot"] = arr[-MAX_MINUTES_PER_PAIR:]
                    saved_any = True
                sb["buy"] = 0.0
                sb["sell"] = 0.0

            fb = fut_buckets[symbol]
            total_f = fb["buy"] + fb["sell"]
            if total_f > 0:
                delta_ratio_f = round((fb["buy"] - fb["sell"]) / total_f, 5)
                arr_f = history[sym_upper]["fut"]
                if not arr_f or arr_f[-1][0] != minute_ts:
                    arr_f.append([minute_ts, delta_ratio_f])
                    if len(arr_f) > MAX_MINUTES_PER_PAIR:
                        history[sym_upper]["fut"] = arr_f[-MAX_MINUTES_PER_PAIR:]
                    saved_any = True
                fb["buy"] = 0.0
                fb["sell"] = 0.0

        if saved_any:
            save_history(history)
            print(f"[{datetime.now(timezone.utc).isoformat()}] flushed minute {minute_ts}")
        return saved_any


async def process_spot_ws(ws_url, buckets, label):
    try:
        async with websockets.connect(ws_url, ping_interval=20, ping_timeout=10) as ws:
            print(f"{label} connected to {ws_url[:70]}...")
            msg_count = 0
            while True:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=15)
                except asyncio.TimeoutError:
                    print(f"{label} timeout (15s no message)")
                    continue
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                stream = msg.get("stream", "")
                data = msg.get("data", {})
                if not stream or not data:
                    continue

                symbol = stream.split("@")[0].lower()
                if symbol not in buckets:
                    continue

                try:
                    qty = float(data.get("q", 0))
                except (ValueError, TypeError):
                    continue
                if qty <= 0:
                    continue

                if data.get("m", False):
                    buckets[symbol]["sell"] += qty
                else:
                    buckets[symbol]["buy"] += qty

                msg_count += 1
                if msg_count % 500 == 0:
                    print(f"{label} received {msg_count} trades, last={symbol} qty={qty:.4f}")
    except Exception as e:
        print(f"{label} WS error: {type(e).__name__}: {e}")


async def poll_futures_once(session, symbol, buckets, last_id_map, label):
    """Один цикл REST-поллинга aggTrades для одной пары. Возвращает число новых сделок."""
    url = FUT_REST_BASE + "/fapi/v1/aggTrades?symbol=" + symbol.upper() + "&limit=1000"
    last_id = last_id_map.get(symbol)
    if last_id is not None:
        url += "&fromId=" + str(last_id + 1)

    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                return 0
            trades = await resp.json()
    except Exception as e:
        print(f"{label} {symbol} poll error: {type(e).__name__}: {e}")
        return 0

    if not isinstance(trades, list) or not trades:
        return 0

    added = 0
    b = buckets[symbol]
    for t in trades:
        try:
            qty = float(t.get("q", 0))
        except (ValueError, TypeError):
            continue
        if qty <= 0:
            continue
        if t.get("m", False):
            b["sell"] += qty
        else:
            b["buy"] += qty
        added += 1

    last_id_map[symbol] = trades[-1].get("a", last_id)
    return added


async def process_fut_rest_poller(buckets, label):
    """Поллинг фьючерсных сделок каждые FUT_POLL_INTERVAL секунд."""
    print(f"{label} REST poller started (interval {FUT_POLL_INTERVAL}s)")
    total_added = 0
    async with aiohttp.ClientSession() as session:
        while True:
            cycle_start = time.time()
            results = await asyncio.gather(*[
                poll_futures_once(session, s, buckets, fut_last_id, label)
                for s in SYMBOLS
            ], return_exceptions=True)
            added = sum(r for r in results if isinstance(r, int))
            total_added += added
            if added > 0:
                print(f"{label} polled {added} trades (total {total_added})")
            elapsed = time.time() - cycle_start
            await asyncio.sleep(max(0.5, FUT_POLL_INTERVAL - elapsed))


async def minute_timer():
    global last_minute
    while True:
        now_minute = int(time.time() // 60)
        if now_minute > last_minute:
            prev = last_minute
            last_minute = now_minute
            await flush_minute(prev * 60)
        await asyncio.sleep(1)


async def main():
    start = time.time()
    print(f"Collector started (spot WS + fut REST), will run for {COLLECT_SECONDS}s")

    tasks = [
        asyncio.create_task(process_spot_ws(SPOT_WS_URL, spot_buckets, "SPOT")),
        asyncio.create_task(process_fut_rest_poller(fut_buckets, "FUT")),
        asyncio.create_task(minute_timer()),
    ]

    try:
        await asyncio.sleep(COLLECT_SECONDS)
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await flush_minute(last_minute * 60)
        print(f"Done in {time.time() - start:.1f}s")


if __name__ == "__main__":
    asyncio.run(main())
