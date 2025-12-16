"""
Delta Exchange Paper Trading Bot (1m candles)
NO REAL ORDERS
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

LONG_THRESH = 0.005     # 0.005%
SHORT_THRESH = -0.005

EXIT_DEADBAND = 0.005

POSITION_SIZE = 1
START_BALANCE = 100_000

MAX_TRADES = 100
MAX_DRAWDOWN = -5_000

# =========================
# STATE
# =========================
closes = deque(maxlen=10)

position = "FLAT"
entry_price = None
balance = START_BALANCE
trades = 0
last_ts = None

# =========================
# UTILS
# =========================
def log(msg):
    print(time.strftime("%H:%M:%S"), msg, flush=True)

def compute_return(new, prev):
    return (new - prev) / prev if prev else 0.0

def signal_from_return(ret_pct):
    if ret_pct > LONG_THRESH:
        return 1
    if ret_pct < SHORT_THRESH:
        return -1
    return 0

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
    log(f"EXIT {position} @ {price} | PnL={pnl:.2f} | Bal={balance:.2f}")
    position = "FLAT"
    entry_price = None

# =========================
# STRATEGY
# =========================
def handle(signal, price, ret_pct):
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
        elif signal == 0 and abs(ret_pct) < EXIT_DEADBAND:
            exit_position(price)

    elif position == "SHORT":
        if signal == 1:
            exit_position(price)
            enter_long(price)
        elif signal == 0 and abs(ret_pct) < EXIT_DEADBAND:
            exit_position(price)

# =========================
# MAIN LOOP
# =========================
async def run():
    global last_ts

    async with websockets.connect(
        WS_URL,
        ping_interval=20,
        ping_timeout=10
    ) as ws:

        await ws.send(json.dumps({
            "type": "subscribe",
            "payload": {
                "channels": [
                    {
                        "name": "candlestick_1m",
                        "symbols": [SYMBOL]
                    }
                ]
            }
        }))

        log("Subscribed to Delta 1m candle CLOSE")

        async for msg in ws:
            data = json.loads(msg)
            payload = data.get("payload")
            if not payload:
                continue

            candle = payload[-1]

            # Only act on closed candles
            if not candle.get("is_final", False):
                continue

            ts = candle["start_timestamp"]
            if ts == last_ts:
                continue
            last_ts = ts

            close = float(candle["close"])

            if closes:
                ret_pct = compute_return(close, closes[-1]) * 100
            else:
                ret_pct = 0.0

            closes.append(close)

            signal = signal_from_return(ret_pct)

            log(f"[CLOSE] {close} Ret%={ret_pct:.4f} Signal={signal}")

            handle(signal, close, ret_pct)

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
