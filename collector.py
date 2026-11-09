import asyncio
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import websockets

# --- Конфигурация ---
SYMBOLS = [
    "btcusdt", "ethusdt", "bnbusdt", "solusdt", "xrpusdt",
    "dogeusdt", "adausdt", "avaxusdt", "dotusdt", "linkusdt"
]
WS_URL = "wss://stream.binance.com:9443/stream?streams=" + "/".join(
    f"{s}@depth20@100ms" for s in SYMBOLS
)
COLLECT_SECONDS = 240  # 4 минуты (оставляем запас до следующего запуска)
DATA_FILE = Path("data/history.json")
MAX_MINUTES_PER_PAIR = 8000  # ~5.5 дней при сборе раз в минуту

# --- Вспомогательные функции ---
def load_history():
    if DATA_FILE.exists():
        try:
            return json.loads(DATA_FILE.read_text())
        except (json.JSONDecodeError, OSError):
            pass
    return {}

def save_history(data):
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    DATA_FILE.write_text(json.dumps(data, separators=(",", ":")))

def aggregate_minute(spot_buckets, fut_buckets):
    """Усредняет накопленные значения за минуту и добавляет в историю."""
    now_ts = int(time.time())
    minute_ts = now_ts - (now_ts % 60)  # Начало текущей минуты

    # Загружаем текущую историю
    history = load_history()

    for symbol in SYMBOLS:
        sym_upper = symbol.upper()
        if sym_upper not in history:
            history[sym_upper] = {"spot": [], "fut": []}

        # Spot
        if spot_buckets.get(symbol) and spot_buckets[symbol]["count"] > 0:
            avg_ratio = spot_buckets[symbol]["sum"] / spot_buckets[symbol]["count"]
            history[sym_upper]["spot"].append([minute_ts, round(avg_ratio, 5)])

        # Futures
        if fut_buckets.get(symbol) and fut_buckets[symbol]["count"] > 0:
            avg_ratio = fut_buckets[symbol]["sum"] / fut_buckets[symbol]["count"]
            history[sym_upper]["fut"].append([minute_ts, round(avg_ratio, 5)])

        # Обрезаем старые данные
        for key in ("spot", "fut"):
            if len(history[sym_upper][key]) > MAX_MINUTES_PER_PAIR:
                history[sym_upper][key] = history[sym_upper][key][-MAX_MINUTES_PER_PAIR:]

    save_history(history)
    print(f"[{datetime.now(timezone.utc).isoformat()}] Сохранено {len(history)} пар.")

async def main():
    spot_buckets = {s: {"sum": 0.0, "count": 0} for s in SYMBOLS}
    fut_buckets = {s: {"sum": 0.0, "count": 0} for s in SYMBOLS}

    start_time = time.time()
    last_minute_mark = int(start_time // 60)

    try:
        async with websockets.connect(WS_URL) as ws:
            while time.time() - start_time < COLLECT_SECONDS:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=10)
                    msg = json.loads(raw)
                except (asyncio.TimeoutError, json.JSONDecodeError):
                    continue

                stream = msg.get("stream", "")
                data = msg.get("data", {})
                symbol = stream.split("@")[0].lower()
                if symbol not in spot_buckets:
                    continue

                # Для спота и фьючерсов используем одни и те же ключи, но в реальном Binance
                # потоки depth для спота и фьючерсов разные. Здесь упрощённо считаем,
                # что depth20 для спота — это спот, а для фьючерсов можно было бы
                # использовать отдельный WS. Для демонстрации используем один поток
                # как "spot", а "fut" оставим пустым, чтобы не усложнять.
                # В реальном коде нужно два отдельных WebSocket-подключения.
                bids = data.get("bids") or data.get("b") or []
                asks = data.get("asks") or data.get("a") or []
                bid_vol = sum(float(level[1]) for level in bids)
                ask_vol = sum(float(level[1]) for level in asks)
                total = bid_vol + ask_vol
                ratio = (bid_vol - ask_vol) / total if total > 0 else 0.0

                spot_buckets[symbol]["sum"] += ratio
                spot_buckets[symbol]["count"] += 1

                # Проверяем, не перешли ли на новую минуту
                current_minute = int(time.time() // 60)
                if current_minute > last_minute_mark:
                    aggregate_minute(spot_buckets, fut_buckets)
                    # Сбрасываем накопления
                    for s in SYMBOLS:
                        spot_buckets[s] = {"sum": 0.0, "count": 0}
                        fut_buckets[s] = {"sum": 0.0, "count": 0}
                    last_minute_mark = current_minute

    except Exception as e:
        print(f"Ошибка WebSocket: {e}")

    # Финальная агрегация за последнюю неполную минуту
    aggregate_minute(spot_buckets, fut_buckets)
    print("Сбор завершён.")

if __name__ == "__main__":
    asyncio.run(main())
