import asyncio
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import websockets
import aiohttp

# ---------- Конфигурация ----------
SYMBOLS = [
    "btcusdt", "ethusdt", "bnbusdt", "solusdt", "xrpusdt",
    "dogeusdt", "adausdt", "avaxusdt", "dotusdt", "linkusdt",
]

# Binance
BINANCE_SPOT_WS = "wss://data-stream.binance.vision/stream?streams=" + "/".join(
    f"{s}@aggTrade" for s in SYMBOLS
)
BINANCE_FUT_RATIO_URL = "https://fapi.binance.com/futures/data/takerlongshortRatio"

# Bybit (Spot)
BYBIT_SPOT_WS = "wss://stream.bybit.com/v5/public/spot"

# OKX (Spot)
OKX_SPOT_WS = "wss://ws.okx.com:8443/ws/v5/public"

# Coinbase (Advanced Trade)
COINBASE_SPOT_WS = "wss://advanced-trade-ws.coinbase.com"

# Kraken (WebSocket v2)
KRAKEN_SPOT_WS = "wss://ws.kraken.com/v2"

COLLECT_SECONDS = 240
FUT_POLL_INTERVAL = 30
DATA_FILE = Path("data/history.json")
MAX_MINUTES_PER_PAIR = 8000

# Хранилище текущей минуты (суммарные buy/sell по всем биржам)
spot_buckets = {s: {"buy": 0.0, "sell": 0.0} for s in SYMBOLS}
last_minute = int(time.time() // 60)
flush_lock = asyncio.Lock()


# ---------- Работа с историей ----------
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


# ---------- Универсальный обработчик сделок ----------
def add_trade(symbol: str, side: str, qty: float):
    """side: 'buy' или 'sell'"""
    if symbol not in spot_buckets:
        return
    if side == "buy":
        spot_buckets[symbol]["buy"] += qty
    elif side == "sell":
        spot_buckets[symbol]["sell"] += qty


# ---------- Binance Spot ----------
async def process_binance_spot():
    try:
        async with websockets.connect(BINANCE_SPOT_WS, ping_interval=20, ping_timeout=10) as ws:
            print("BINANCE SPOT connected")
            msg_count = 0
            while True:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=15)
                except asyncio.TimeoutError:
                    print("BINANCE SPOT timeout")
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
                try:
                    qty = float(data.get("q", 0))
                except (ValueError, TypeError):
                    continue
                if qty <= 0:
                    continue
                side = "sell" if data.get("m", False) else "buy"
                add_trade(symbol, side, qty)
                msg_count += 1
                if msg_count % 500 == 0:
                    print(f"BINANCE SPOT received {msg_count} trades")
    except Exception as e:
        print(f"BINANCE SPOT WS error: {type(e).__name__}: {e}")


# ---------- Bybit Spot ----------
async def process_bybit_spot():
    # Bybit использует те же тикеры, что и Binance (BTCUSDT)
    try:
        async with websockets.connect(BYBIT_SPOT_WS, ping_interval=20, ping_timeout=10) as ws:
            print("BYBIT SPOT connected")
            # Подписка на топики publicTrade.{symbol}
            args = [f"publicTrade.{s.upper()}" for s in SYMBOLS]
            sub_msg = {"op": "subscribe", "args": args}
            await ws.send(json.dumps(sub_msg))
            msg_count = 0
            while True:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=15)
                except asyncio.TimeoutError:
                    print("BYBIT SPOT timeout")
                    continue
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                # Формат: { "topic": "publicTrade.BTCUSDT", "data": [ { "S": "Buy", "v": "0.1", ... } ] }
                topic = msg.get("topic", "")
                data = msg.get("data", [])
                if not topic.startswith("publicTrade.") or not data:
                    continue
                symbol = topic.split(".")[1].lower()  # btcusdt
                for trade in data:
                    try:
                        qty = float(trade.get("v", 0))
                    except (ValueError, TypeError):
                        continue
                    if qty <= 0:
                        continue
                    side = "buy" if trade.get("S") == "Buy" else "sell"
                    add_trade(symbol, side, qty)
                    msg_count += 1
                if msg_count % 500 == 0 and msg_count > 0:
                    print(f"BYBIT SPOT received {msg_count} trades")
    except Exception as e:
        print(f"BYBIT SPOT WS error: {type(e).__name__}: {e}")


# ---------- OKX Spot ----------
async def process_okx_spot():
    # OKX использует формат BTC-USDT
    try:
        async with websockets.connect(OKX_SPOT_WS, ping_interval=20, ping_timeout=10) as ws:
            print("OKX SPOT connected")
            args = []
            for s in SYMBOLS:
                base = s.replace("usdt", "").upper()
                args.append({"channel": "trades", "instId": f"{base}-USDT"})
            sub_msg = {"op": "subscribe", "args": args}
            await ws.send(json.dumps(sub_msg))
            msg_count = 0
            while True:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=15)
                except asyncio.TimeoutError:
                    print("OKX SPOT timeout")
                    continue
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                # Формат: { "arg": { "channel": "trades", "instId": "BTC-USDT" }, "data": [ { "side": "buy", "sz": "0.1", ... } ] }
                arg = msg.get("arg", {})
                if arg.get("channel") != "trades":
                    continue
                data = msg.get("data", [])
                if not data:
                    continue
                inst_id = arg.get("instId", "")  # BTC-USDT
                symbol = inst_id.replace("-", "").lower()  # btcusdt
                for trade in data:
                    try:
                        qty = float(trade.get("sz", 0))
                    except (ValueError, TypeError):
                        continue
                    if qty <= 0:
                        continue
                    side = trade.get("side", "").lower()  # 'buy' или 'sell'
                    if side in ("buy", "sell"):
                        add_trade(symbol, side, qty)
                        msg_count += 1
                if msg_count % 500 == 0 and msg_count > 0:
                    print(f"OKX SPOT received {msg_count} trades")
    except Exception as e:
        print(f"OKX SPOT WS error: {type(e).__name__}: {e}")


# ---------- Coinbase Spot ----------
async def process_coinbase_spot():
    # Coinbase использует формат BTC-USD
    try:
        async with websockets.connect(COINBASE_SPOT_WS, ping_interval=20, ping_timeout=10) as ws:
            print("COINBASE SPOT connected")
            product_ids = []
            for s in SYMBOLS:
                base = s.replace("usdt", "").upper()
                product_ids.append(f"{base}-USD")
            sub_msg = {
                "type": "subscribe",
                "product_ids": product_ids,
                "channel": "market_trades",
            }
            await ws.send(json.dumps(sub_msg))
            msg_count = 0
            while True:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=15)
                except asyncio.TimeoutError:
                    print("COINBASE SPOT timeout")
                    continue
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                # Формат: { "channel": "market_trades", "events": [ { "trades": [ { "side": "BUY", "size": "0.1", "product_id": "BTC-USD" } ] } ] }
                if msg.get("channel") != "market_trades":
                    continue
                events = msg.get("events", [])
                for event in events:
                    trades = event.get("trades", [])
                    for trade in trades:
                        product_id = trade.get("product_id", "")  # BTC-USD
                        symbol = product_id.replace("-", "").lower()  # btcusd -> btcusdt?
                        # Приводим к нашему формату: BTC-USD -> btcusdt (предполагая USDT)
                        # В реальности BTC-USD и BTC-USDT — разные пары, но для агрегации давления это допустимо
                        base = product_id.replace("-USD", "").lower()
                        symbol = base + "usdt"
                        try:
                            qty = float(trade.get("size", 0))
                        except (ValueError, TypeError):
                            continue
                        if qty <= 0:
                            continue
                        side = trade.get("side", "").lower()  # 'buy' или 'sell'
                        if side in ("buy", "sell"):
                            add_trade(symbol, side, qty)
                            msg_count += 1
                if msg_count % 500 == 0 and msg_count > 0:
                    print(f"COINBASE SPOT received {msg_count} trades")
    except Exception as e:
        print(f"COINBASE SPOT WS error: {type(e).__name__}: {e}")


# ---------- Kraken Spot (WebSocket v2) ----------
async def process_kraken_spot():
    # Kraken использует формат BTC/USD
    try:
        async with websockets.connect(KRAKEN_SPOT_WS, ping_interval=20, ping_timeout=10) as ws:
            print("KRAKEN SPOT connected")
            pairs = []
            for s in SYMBOLS:
                base = s.replace("usdt", "").upper()
                pairs.append(f"{base}/USD")
            sub_msg = {
                "method": "subscribe",
                "params": {
                    "channel": "trade",
                    "symbol": pairs,
                },
            }
            await ws.send(json.dumps(sub_msg))
            msg_count = 0
            while True:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=15)
                except asyncio.TimeoutError:
                    print("KRAKEN SPOT timeout")
                    continue
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                # Формат: { "channel": "trade", "type": "update", "data": [ { "symbol": "BTC/USD", "side": "buy", "qty": 0.1, ... } ] }
                if msg.get("channel") != "trade":
                    continue
                data = msg.get("data", [])
                if not data:
                    continue
                for trade in data:
                    symbol_raw = trade.get("symbol", "")  # BTC/USD
                    base = symbol_raw.replace("/USD", "").lower()
                    symbol = base + "usdt"
                    try:
                        qty = float(trade.get("qty", 0))
                    except (ValueError, TypeError):
                        continue
                    if qty <= 0:
                        continue
                    side = trade.get("side", "").lower()  # 'buy' или 'sell'
                    if side in ("buy", "sell"):
                        add_trade(symbol, side, qty)
                        msg_count += 1
                if msg_count % 500 == 0 and msg_count > 0:
                    print(f"KRAKEN SPOT received {msg_count} trades")
    except Exception as e:
        print(f"KRAKEN SPOT WS error: {type(e).__name__}: {e}")


# ---------- Фьючерсы (Binance REST) ----------
async def poll_futures_ratio(session):
    async with flush_lock:
        history = load_history()
        total_new = 0
        for symbol in SYMBOLS:
            url = BINANCE_FUT_RATIO_URL + "?symbol=" + symbol.upper() + "&period=5m&limit=30"
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        print(f"FUT {symbol} HTTP {resp.status}")
                        continue
                    data = await resp.json()
            except Exception as e:
                print(f"FUT {symbol} error: {type(e).__name__}: {e}")
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
                print(f"FUT {sym_upper} +{added} points (total {len(arr)})")
        if total_new > 0:
            save_history(history)
        return total_new


async def process_fut_ratio_poller():
    print(f"FUT ratio poller started (interval {FUT_POLL_INTERVAL}s)")
    async with aiohttp.ClientSession() as session:
        while True:
            cycle_start = time.time()
            try:
                await poll_futures_ratio(session)
            except Exception as e:
                print(f"FUT poller error: {e}")
            elapsed = time.time() - cycle_start
            await asyncio.sleep(max(1.0, FUT_POLL_INTERVAL - elapsed))


# ---------- Минутный таймер ----------
async def minute_timer():
    global last_minute
    while True:
        now_minute = int(time.time() // 60)
        if now_minute > last_minute:
            prev = last_minute
            last_minute = now_minute
            await flush_spot(prev * 60)
        await asyncio.sleep(1)


# ---------- Main ----------
async def main():
    start = time.time()
    print(f"Multi-exchange collector started, will run for {COLLECT_SECONDS}s")
    tasks = [
        asyncio.create_task(process_binance_spot()),
        asyncio.create_task(process_bybit_spot()),
        asyncio.create_task(process_okx_spot()),
        asyncio.create_task(process_coinbase_spot()),
        asyncio.create_task(process_kraken_spot()),
        asyncio.create_task(process_fut_ratio_poller()),
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
