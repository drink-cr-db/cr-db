import asyncio
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import websockets

# --- Конфигурация ---
SYMBOLS = [
    "btcusdt", "ethusdt", "bnbusdt", "solusdt", "xrpusdt",
    "dogeusdt", "adausdt", "avaxusdt", "dotusdt", "linkusdt",
]

# ИСПОЛЬЗУЕМ data-stream.binance.vision — он не блокируется для US IP
SPOT_WS_URL = "wss://data-stream.binance.vision/stream?streams=" + "/".join(
    f"{s}@depth20@100ms" for s in SYMBOLS
)
# Фьючерсы временно отключаем, чтобы не тратить время на заведомо блокируемый эндпоинт
FUT_WS_URL = None  # было: "wss://fstream.binance.com/stream?streams=..."

COLLECT_SECONDS = 270
DATA_FILE = Path("data/history.json")
MAX_MINUTES_PER_PAIR = 8000

spot_buckets = {s: {"sum": 0.0, "count": 0} for s in SYMBOLS}
fut_buckets  = {s: {"sum": 0.0, "count": 0} for s in SYMBOLS}
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
            if sb["count"] > 0:
                avg = round(sb["sum"] / sb["count"], 5)
                arr = history[sym_upper]["spot"]
                if not arr or arr[-1][0] != minute_ts:
                    arr.append([minute_ts, avg])
                    if len(arr) > MAX_MINUTES_PER_PAIR:
                        history[sym_upper]["spot"] = arr[-MAX_MINUTES_PER_PAIR:]
                    saved_any = True
                sb["sum"] = 0.0
                sb["count"] = 0

            # Фьючерсы пока не собираем
            # fb = fut_buckets[symbol]
            # ...

        if saved_any:
            save_history(history)
            print(f"[{datetime.now(timezone.utc).isoformat()}] flushed minute {minute_ts}")
        return saved_any


async def process_stream(ws_url, buckets, label):
    try:
        async with websockets.connect(ws_url, ping_interval=20, ping_timeout=10) as ws:
            print(f"{label} connected to {ws_url[:60]}...")
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

                bids = data.get("bids") or data.get("b") or []
                asks = data.get("asks") or data.get("a") or []
                try:
                    bid_vol = sum(float(lvl[1]) for lvl in bids)
                    ask_vol = sum(float(lvl[1]) for lvl in asks)
                except (ValueError, IndexError, TypeError):
                    continue

                total = bid_vol + ask_vol
                if total <= 0:
                    continue
                ratio = (bid_vol - ask_vol) / total

                buckets[symbol]["sum"] += ratio
                buckets[symbol]["count"] += 1
                msg_count += 1
                if msg_count % 500 == 0:
                    print(f"{label} received {msg_count} msgs, last={symbol} ratio={ratio:.4f}")
    except Exception as e:
        print(f"{label} WS error: {type(e).__name__}: {e}")


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
    print(f"Collector started, will run for {COLLECT_SECONDS}s")

    tasks = [
        asyncio.create_task(process_stream(SPOT_WS_URL, spot_buckets, "SPOT")),
        asyncio.create_task(minute_timer()),
    ]
    # Фьючерсный поток не запускаем

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
