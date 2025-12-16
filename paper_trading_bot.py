# delta_paper_bot_fixed.py
"""
Paper trading bot using Delta Exchange WebSocket candles.
NO REAL ORDERS. SAFE TO RUN.
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

# Realistic for 1-min ETH
LONG_THRESH = 0.01     # 0.01 %
SHORT_THRESH = -0.01

POSITION_SIZE = 1
START_BALANCE = 100_000

MAX_TRADES = 50
MAX_DRAWDOWN = -5_000

EXIT_DEADBAND = 0.002   # do not exit on tiny noise

# =========================
# STATE
# =========================
closes = deque(maxlen=WINDOW + 1)
returns = deque(maxlen=WINDOW)

signal_buf = deque(maxlen=2)  # signal persistence

position = "FLAT"
entry_price = None
balance = START_BALANCE
trades = 0
last_candle_ts = None

# =========================
# UTILS
# =========================
def compute_return(new, prev):
    return (new - prev) / prev if prev else 0.0

def signal_from_return(ret_pct):
    if ret_pct > LONG_THRESH:
        return 1
    if ret_pct < SHORT_THRESH:
        return -1
    return 0

def log(msg):
    print(time.strftime("%H:%M:%S"), msg, flush=True)

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
def handle_signal(signal, price, ret_pct):
    global position

    # require signal persistence (2 candles)
    if len(signal_buf) < 2 or not all(s == signal for s in signal_buf):
        return

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
    global last_candle_ts

    async with websockets.connect(WS_URL) as ws:
        await ws.send(json.dumps({
            "type": "subscribe",
            "payload": {
                "channels": [
                    {"name": "candlesticks_1m", "symbols": [SYMBOL]}
                ]
            }
        }))
        log("Subscribed to candles")

        async for msg in ws:
            data = json.loads(msg)
            payload = data.get("payload")
            if not payload:
                continue

            candle = payload[-1]
            close = float(candle["close"])
            ts = candle["start_timestamp"]

            # ---- Candle gating
            if ts == last_candle_ts:
                continue
            last_candle_ts = ts

            if closes:
                r = compute_return(close, closes[-1])
                returns.append(r)
                ret_pct = r * 100
            else:
                ret_pct = 0.0

            closes.append(close)

            signal = signal_from_return(ret_pct)
            signal_buf.append(signal)

            log(f"Close={close} Ret%={ret_pct:.4f} Signal={signal}")

            handle_signal(signal, close, ret_pct)

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
