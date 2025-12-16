# delta_paper_bot.py
"""
Paper trading bot using Delta Exchange WebSocket candles.
NO REAL ORDERS. SAFE TO RUN.

Requirements:
  pip install websockets aiohttp

Environment variables:
  DELTA_WS_URL (optional)
"""

import asyncio
import json
import time
from collections import deque
import websockets

# =========================
# CONFIG
# =========================
WS_URL = "wss://socket.india.delta.exchange"
SYMBOL = "ETHUSD"
WINDOW = 5
LONG_THRESH = 0.1
SHORT_THRESH = -0.1
POSITION_SIZE = 1
START_BALANCE = 100_000

MAX_TRADES = 50
MAX_DRAWDOWN = -5_000  # stop if loss exceeds this

# =========================
# STATE
# =========================
closes = deque(maxlen=WINDOW + 1)
returns = deque(maxlen=WINDOW)

position = "FLAT"   # FLAT / LONG / SHORT
entry_price = None
balance = START_BALANCE
trades = 0
last_candle_ts = None

# =========================
# UTILS
# =========================
def compute_return(new, prev):
    return (new - prev) / prev if prev != 0 else 0.0

def rolling_mean():
    return sum(returns) / len(returns) if returns else 0.0

def signal_from_return(ret_pct):
    if ret_pct > LONG_THRESH:
        return 1
    if ret_pct < SHORT_THRESH:
        return -1
    return 0

def log(msg):
    print(time.strftime("%H:%M:%S"), msg)

# =========================
# PAPER EXECUTION
# =========================
def enter_long(price):
    global position, entry_price, trades
    position = "LONG"
    entry_price = price
    trades += 1
    log(f"ENTER LONG @ {price}")

def enter_short(price):
    global position, entry_price, trades
    position = "SHORT"
    entry_price = price
    trades += 1
    log(f"ENTER SHORT @ {price}")

def exit_position(price):
    global position, entry_price, balance
    if position == "LONG":
        pnl = (price - entry_price) * POSITION_SIZE
    elif position == "SHORT":
        pnl = (entry_price - price) * POSITION_SIZE
    else:
        return

    balance += pnl
    log(f"EXIT {position} @ {price} | PnL={pnl:.2f} | Balance={balance:.2f}")
    position = "FLAT"
    entry_price = None

# =========================
# STATE MACHINE
# =========================
def handle_signal(signal, price):
    global position

    if position == "FLAT":
        if signal == 1:
            enter_long(price)
        elif signal == -1:
            enter_short(price)

    elif position == "LONG":
        if signal == -1:
            exit_position(price)
            enter_short(price)
        elif signal == 0:
            exit_position(price)

    elif position == "SHORT":
        if signal == 1:
            exit_position(price)
            enter_long(price)
        elif signal == 0:
            exit_position(price)

# =========================
# MAIN LOOP
# =========================
async def run():
    global last_candle_ts

    async with websockets.connect(WS_URL) as ws:
        sub = {
            "type": "subscribe",
            "payload": {
                "channels": [
                    {"name": "candlesticks_1m", "symbols": [SYMBOL]}
                ]
            }
        }
        await ws.send(json.dumps(sub))
        log("Subscribed to candles")

        async for msg in ws:
            data = json.loads(msg)
            payload = data.get("payload")
            if not payload:
                continue

            candle = payload[-1]
            close = float(candle["close"])
            ts = candle["start_timestamp"]

            # ---- Candle gating (prevents double fire)
            if ts == last_candle_ts:
                continue
            last_candle_ts = ts

            if closes:
                returns.append(compute_return(close, closes[-1]))
            closes.append(close)

            ret_pct = rolling_mean() * 100
            signal = signal_from_return(ret_pct)

            log(f"Close={close} Ret%={ret_pct:.4f} Signal={signal}")

            handle_signal(signal, close)

            # ---- Kill switches
            if trades >= MAX_TRADES:
                log("MAX TRADES HIT — STOPPING")
                break

            if balance - START_BALANCE <= MAX_DRAWDOWN:
                log("MAX DRAWDOWN HIT — STOPPING")
                break

# =========================
# ENTRY
# =========================
if __name__ == "__main__":
    asyncio.run(run())
