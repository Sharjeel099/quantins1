# delta_bot.py
"""
Functional, minimal-latency trading bot for Delta Exchange.
"""

import os
import time
import hmac
import hashlib
import json
import asyncio
from collections import deque
from typing import Deque, Tuple, Dict, Any
import aiohttp
import websockets

# -----------------------------
# Configuration (set via env)
# -----------------------------
API_KEY = os.getenv("DELTA_API_KEY", "")
API_SECRET = os.getenv("DELTA_API_SECRET", "")
BASE_URL = os.getenv("DELTA_BASE_URL", "https://api.india.delta.exchange")
WS_URL = os.getenv("DELTA_WS_URL", "wss://socket.india.delta.exchange")

PRODUCT_SYMBOL = os.getenv("DELTA_PRODUCT_SYMBOL", "ETHUSD")
PRODUCT_ID = os.getenv("DELTA_PRODUCT_ID", "")

WINDOW = int(os.getenv("ROLLING_WINDOW", "5"))
LONG_THRESH = float(os.getenv("LONG_THRESHOLD", "0.1"))
SHORT_THRESH = float(os.getenv("SHORT_THRESHOLD", "-0.1"))
ORDER_SIZE = float(os.getenv("ORDER_SIZE", "1"))

# -----------------------------
# Utility / Signing
# -----------------------------
def now_ts() -> str:
    return str(int(time.time()))

def sign_request(api_secret: str, method: str, timestamp: str,
                 path: str, query_string: str, payload_str: str) -> str:
    data = (method + timestamp + path + (query_string or "") + (payload_str or "")).encode()
    return hmac.new(api_secret.encode(), data, hashlib.sha256).hexdigest()

# -----------------------------
# Rolling State
# -----------------------------
def make_rolling_state(window: int) -> Dict[str, Any]:
    return {
        "closes": deque(maxlen=window + 1),
        "pct_deque": deque(maxlen=window),
        "last_signal": 0
    }

def update_returns_state(state, new_close, window):
    closes = state["closes"]
    pct_deque = state["pct_deque"]

    if closes:
        prev = closes[-1]
        pct = (new_close - prev) / prev if prev else 0.0
        pct_deque.append(pct)

    closes.append(new_close)
    rolling_mean = sum(pct_deque) / len(pct_deque) if pct_deque else 0.0
    return state, rolling_mean * 100.0

def compute_signal(ret_percent, long_th, short_th):
    if ret_percent > long_th:
        return 1
    if ret_percent < short_th:
        return -1
    return 0

# -----------------------------
# REST helpers
# -----------------------------
def build_signed_headers(api_key, api_secret, method, path, payload=None):
    ts = now_ts()
    payload_str = json.dumps(payload) if payload else ""
    sig = sign_request(api_secret, method.upper(), ts, path, "", payload_str)
    return {
        "Content-Type": "application/json",
        "api-key": api_key,
        "timestamp": ts,
        "signature": sig
    }

async def place_market_order(session, product_id, side, size):
    path = "/v2/orders"
    url = BASE_URL + path
    payload = {
        "product_id": int(product_id),
        "side": side,
        "order_type": "market_order",
        "size": size
    }
    headers = build_signed_headers(API_KEY, API_SECRET, "POST", path, payload)
    async with session.post(url, json=payload, headers=headers) as r:
        return await r.json()

# -----------------------------
# WebSocket helpers
# -----------------------------
def subscribe_msg():
    return {
        "type": "subscribe",
        "payload": {
            "channels": [{
                "name": "candlesticks_1m",
                "symbols": [PRODUCT_SYMBOL]
            }]
        }
    }

def parse_candle(msg):
    if not msg.get("type", "").startswith("candlestick_"):
        return None
    try:
        return float(msg["close"]), msg.get("timestamp")
    except:
        return None

# -----------------------------
# Main Bot Loop with Reconnect
# -----------------------------
async def run_bot():
    state = make_rolling_state(WINDOW)
    backoff = 1

    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as session:

        while True:
            try:
                print("Connecting WebSocket...")
                async with websockets.connect(
                    WS_URL,
                    ping_interval=20,
                    ping_timeout=20,
                    max_size=None
                ) as ws:

                    print("WebSocket connected")
                    backoff = 1  # reset on success

                    await ws.send(json.dumps(subscribe_msg()))
                    print(f"Subscribed to {PRODUCT_SYMBOL}")

                    async for raw in ws:
                        msg = json.loads(raw)
                        parsed = parse_candle(msg)
                        if not parsed:
                            continue

                        close, ts = parsed
                        _, ret_pct = update_returns_state(state, close, WINDOW)

                        signal = compute_signal(ret_pct, LONG_THRESH, SHORT_THRESH)
                        last_signal = state["last_signal"]

                        if abs(signal - last_signal) == 2 and PRODUCT_ID:
                            side = "buy" if signal == 1 else "sell"
                            print(f"Signal flip {last_signal}->{signal} | ret={ret_pct:.5f}")
                            asyncio.create_task(
                                place_market_order(session, PRODUCT_ID, side, ORDER_SIZE)
                            )

                        state["last_signal"] = signal

            except Exception as e:
                print(f"WebSocket error: {e}")
                print(f"Reconnecting in {backoff}s...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

# -----------------------------
# Entrypoint
# -----------------------------
def main():
    if not API_KEY or not API_SECRET:
        raise RuntimeError("Missing API credentials")

    asyncio.run(run_bot())

if __name__ == "__main__":
    main()
