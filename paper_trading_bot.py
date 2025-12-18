"""
Delta Exchange Paper Trading Bot
EC2-safe | Auto-reconnect | Persistent logs | Clean architecture

NO REAL ORDERS
"""

import asyncio
import json
import time
from collections import deque
import websockets
import traceback

# =========================
# CONFIG
# =========================
WS_URL = "wss://socket.india.delta.exchange"
SYMBOL = "ETHUSD"

WINDOW = 2
LONG_THRESH = 0.5
SHORT_THRESH = -0.5

POSITION_SIZE = 1
START_BALANCE = 100_000

MAX_TRADES = 50
MAX_DRAWDOWN = -5_000

PING_INTERVAL = 20
RECONNECT_DELAY = 5

LOG_FILE = "trades.log"
EQUITY_FILE = "equity.json"

# =========================
# STATE
# =========================
class BotState:
    def __init__(self):
        self.closes = deque(maxlen=WINDOW + 1)
        self.returns = deque(maxlen=WINDOW)
        self.position = "FLAT"
        self.entry_price = None
        self.balance = START_BALANCE
        self.trades = 0
        self.last_candle_ts = None
        self.equity_curve = []

state = BotState()

# =========================
# UTILS
# =========================
def log(msg):
    line = f"{time.strftime('%H:%M:%S')} {msg}"
    print(line)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")

def compute_return(new, prev):
    return (new - prev) / prev if prev else 0.0

# =========================
# STRATEGY
# =========================
def rolling_mean():
    return sum(state.returns) / len(state.returns) if state.returns else 0.0

def signal_from_return(ret_pct):
    if ret_pct > LONG_THRESH:
        return 1
    if ret_pct < SHORT_THRESH:
        return -1
    return 0

# =========================
# EXECUTION (PAPER)
# =========================
def enter_long(price):
    state.position = "LONG"
    state.entry_price = price
    state.trades += 1
    log(f"ENTER LONG @ {price}")

def enter_short(price):
    state.position = "SHORT"
    state.entry_price = price
    state.trades += 1
    log(f"ENTER SHORT @ {price}")

def exit_position(price):
    if state.position == "LONG":
        pnl = (price - state.entry_price) * POSITION_SIZE
    elif state.position == "SHORT":
        pnl = (state.entry_price - price) * POSITION_SIZE
    else:
        return

    state.balance += pnl
    log(f"EXIT {state.position} @ {price} | PnL={pnl:.2f} | Balance={state.balance:.2f}")
    state.position = "FLAT"
    state.entry_price = None

def handle_signal(signal, price):
    if state.position == "FLAT":
        if signal == 1:
            enter_long(price)
        elif signal == -1:
            enter_short(price)

    elif state.position == "LONG":
        if signal <= 0:
            exit_position(price)
            if signal == -1:
                enter_short(price)

    elif state.position == "SHORT":
        if signal >= 0:
            exit_position(price)
            if signal == 1:
                enter_long(price)

# =========================
# METRICS
# =========================
def record_equity(ts, price):
    state.equity_curve.append({
        "timestamp": ts,
        "balance": state.balance,
        "position": state.position,
        "price": price
    })

    if len(state.equity_curve) % 100 == 0:
        with open(EQUITY_FILE, "w") as f:
            json.dump(state.equity_curve, f, indent=2)

# =========================
# DATA FEED
# =========================
async def consume_ws():
    async with websockets.connect(
        WS_URL,
        ping_interval=PING_INTERVAL,
        ping_timeout=PING_INTERVAL,
        close_timeout=5
    ) as ws:

        sub = {
            "type": "subscribe",
            "payload": {
                "channels": [{"name": "candlestick_1m", "symbols": [SYMBOL]}]
            }
        }

        await ws.send(json.dumps(sub))
        log(f"Subscribed to {SYMBOL}. Waiting for first candle...")

        async for msg in ws:
            #print("RAW WS MESSAGE:", msg)
            data = json.loads(msg)

            parsed = parse_delta_candlestick(data)
            if parsed is None:
                continue

            ts, close = parsed

            if ts == state.last_candle_ts:
                continue

            state.last_candle_ts = ts

            if state.closes:
                state.returns.append(compute_return(close, state.closes[-1]))

            state.closes.append(close)

            ret_pct = rolling_mean() * 100
            signal = signal_from_return(ret_pct)

            log(f"NEW CANDLE | Close={close} | Ret%={ret_pct:.4f} | Signal={signal}")
            handle_signal(signal, close)
            record_equity(ts, close)


def parse_delta_candlestick(msg: dict):
    """
    Parse a Delta Exchange candlestick update.
    Returns (timestamp, close_price) or None if not a candle.
    """

    msg_type = msg.get("type", "")
    if not msg_type.startswith("candlestick_"):
        return None

    try:
        ts = msg["candle_start_time"]
        close = float(msg["close"])
        return ts, close
    except (KeyError, TypeError, ValueError):
        return None



# =========================
# ORCHESTRATOR (RECONNECT SAFE)
# =========================
async def run_forever():
    while True:
        try:
            await consume_ws()
        except Exception as e:
            log("ERROR — reconnecting")
            log(traceback.format_exc())
            await asyncio.sleep(RECONNECT_DELAY)

# =========================
# ENTRY
# =========================
if __name__ == "__main__":
    asyncio.run(run_forever())
