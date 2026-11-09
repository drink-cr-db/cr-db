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
FUT_RATIO_URL = "https://fapi.binance.com/futures/data/takerlongshortRatio"

COLLECT_SECONDS = 240
FUT_POLL_INTERVAL = 30
DATA_FILE = Path("data/history.json")
MAX_MINUTES_PER_PAIR = 8000

spot_buckets = {s: {"buy": 0.0, "sell": 0.0} for s in SYMBOLS}
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


async def flush_spot(minute_ts):
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
        if saved_any:
            save_history(history)
            print(f"[{datetime.now(timezone.utc).isoformat()}] flushed spot {minute_ts}")


async def process_spot_ws(ws_url, buckets, label):
    try:
        async with websockets.connect(ws_url, ping_interval=20, ping_timeout=10) as ws:
            print(f"{label} connected to {ws_url[:70]}...")
            msg_count = 0
            while True:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=15)
                except asyncio.TimeoutError:
                    print(f"{label} timeout")
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
                    print(f"{label} received {msg_count} trades")
    except Exception as e:
        print(f"{label} WS error: {type(e).__name__}: {e}")


async def poll_futures_ratio(session, label):
    async with flush_lock:
        history = load_history()
        total_new = 0

        for symbol in SYMBOLS:
            url = FUT_RATIO_URL + "?symbol=" + symbol.upper() + "&period=5m&limit=30"
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        print(f"{label} {symbol} HTTP {resp.status}")
                        continue
                    data = await resp.json()
            except Exception as e:
                print(f"{label} {symbol} error: {type(e).__name__}: {e}")
                continue
            if not isinstance(data, list) or not data:
                continue

            sym_upper = symbol.upper()
            if sym_upper not in history:
                history[sym_upper] = {"spot": [], "fut": []}
            arr = history[sym_upper]["fut"]
            existing_ts = set(t[0] for t in arr) if arr else set()

            added = 0
            for item in data:
                try:
                    ts_ms = int(item.get("timestamp", 0))
                    buy_vol = float(item.get("buyVol", 0))
                    sell_vol = float(item.get("sellVol", 0))
                except (ValueError, TypeError):
                    continue
                ts_sec = ts_ms // 1000
                if ts_sec in existing_ts:
                    continue
                total = buy_vol + sell_vol
                if total <= 0:
                    continue
                ratio = (buy_vol - sell_vol) / total
                arr.append([ts_sec, round(ratio, 5)])
                existing_ts.add(ts_sec)
                added += 1

            if added > 0:
                arr.sort(key=lambda x: x[0])
                if len(arr) > MAX_MINUTES_PER_PAIR:
                    history[sym_upper]["fut"] = arr[-MAX_MINUTES_PER_PAIR:]
                total_new += added
                print(f"{label} {sym_upper} +{added} fut points (total {len(arr)})")

        if total_new > 0:
            save_history(history)
        return total_new


async def process_fut_ratio_poller(label):
    print(f"{label} ratio poller started (interval {FUT_POLL_INTERVAL}s)")
    async with aiohttp.ClientSession() as session:
        while True:
            cycle_start = time.time()
            try:
                await poll_futures_ratio(session, label)
            except Exception as e:
                print(f"{label} poller error: {e}")
            elapsed = time.time() - cycle_start
            await asyncio.sleep(max(1.0, FUT_POLL_INTERVAL - elapsed))


async def minute_timer():
    global last_minute
    while True:
        now_minute = int(time.time() // 60)
        if now_minute > last_minute:
            prev = last_minute
            last_minute = now_minute
            await flush_spot(prev * 60)
        await asyncio.sleep(1)


async def main():
    start = time.time()
    print(f"Collector started, will run for {COLLECT_SECONDS}s")
    tasks = [
        asyncio.create_task(process_spot_ws(SPOT_WS_URL, spot_buckets, "SPOT")),
        asyncio.create_task(process_fut_ratio_poller("FUT")),
        asyncio.create_task(minute_timer()),
    ]
    try:
        await asyncio.sleep(COLLECT_SECONDS)
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await flush_spot(last_minute * 60)
        print(f"Done in {time.time() - start:.1f}s")


if __name__ == "__main__":
    asyncio.run(main())
