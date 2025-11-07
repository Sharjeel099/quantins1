# delta_bot.py
"""
Functional, minimal-latency trading bot for Delta Exchange.
Requirements:
  pip install aiohttp websockets

Environment variables required:
  DELTA_API_KEY    - your API key
  DELTA_API_SECRET - your API secret
  DELTA_BASE_URL   - e.g. https://api.india.delta.exchange (default used if not set)
  DELTA_WS_URL     - e.g. wss://socket.india.delta.exchange (default used if not set)
Notes:
  - Test thoroughly on testnet before using real funds.
  - Respect rate limits. Implement additional safety (size checks, cancel logic) as needed.
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
PRODUCT_SYMBOL = os.getenv("DELTA_PRODUCT_SYMBOL", "ETHUSD")   # replace with desired symbol
PRODUCT_ID = os.getenv("DELTA_PRODUCT_ID", "")  # optional: using symbol for subscription; REST needs product_id
WINDOW = int(os.getenv("ROLLING_WINDOW", "5"))  # 'average' parameter from your function
LONG_THRESH = float(os.getenv("LONG_THRESHOLD", "0.1"))   # example thresholds (percent)
SHORT_THRESH = float(os.getenv("SHORT_THRESHOLD", "-0.1"))
ORDER_SIZE = float(os.getenv("ORDER_SIZE", "1"))  # contract size per order
USE_TESTNET = os.getenv("DELTA_USE_TESTNET", "0") in ("1", "true", "True")

if USE_TESTNET:
    WS_URL = os.getenv("DELTA_WS_URL", "wss://socket-ind.testnet.deltaex.org")
    BASE_URL = os.getenv("DELTA_BASE_URL", "https://api.india.delta.exch-demo")  # replace with actual testnet REST base if provided


# -----------------------------
# Utility / Signing (pure fns)
# -----------------------------
def now_ts() -> str:
    """Return current unix timestamp in seconds as a string."""
    return str(int(time.time()))

def sign_request(api_secret: str, method: str, timestamp: str, path: str,
                 query_string: str, payload_str: str) -> str:
    """
    Signature = HMAC_SHA256(api_secret, method + timestamp + path + query_string + payload)
    Matches docs: signature_data = method + timestamp + path + query_string + payload.
    """
    data = (method + timestamp + path + (query_string or "") + (payload_str or "")).encode()
    return hmac.new(api_secret.encode(), data, hashlib.sha256).hexdigest()

# -----------------------------
# Rolling return calculator
# -----------------------------
def make_rolling_state(window: int) -> Dict[str, Any]:
    """Create a small mutable state container; functional-style functions operate on it."""
    return {
        "closes": deque(maxlen=window + 1),   # we store window+1 closes to compute pct changes
        "pct_deque": deque(maxlen=window),    # last `window` percentage changes
        "last_signal": 0                      # previous signal
    }

def update_returns_state(state: Dict[str, Any], new_close: float, window: int) -> Tuple[Dict[str, Any], float]:
    """
    Feed a new close price; returns (new_state, rolling_mean_pct*100).
    rolling_mean_pct is percent (like your function multiplied by 100).
    """
    closes: Deque[float] = state["closes"]
    pct_deque: Deque[float] = state["pct_deque"]

    if len(closes) > 0:
        prev = closes[-1]
        if prev != 0:
            pct = (new_close - prev) / prev
        else:
            pct = 0.0
        pct_deque.append(pct)
    closes.append(new_close)

    # compute rolling mean of pct changes over available window
    if len(pct_deque) == 0:
        rolling_mean = 0.0
    else:
        rolling_mean = sum(pct_deque) / len(pct_deque)

    # match your formula: (pct_change().rolling(average).mean()) * 100
    return state, rolling_mean * 100.0

def compute_signal(ret_percent: float, long: float, short: float) -> int:
    """Return 1 for long, -1 for short, 0 for neutral."""
    if ret_percent > long:
        return 1
    if ret_percent < short:
        return -1
    return 0

# -----------------------------
# REST helpers (pure & async)
# -----------------------------
def build_signed_headers(api_key: str, api_secret: str,
                         method: str, path: str, payload: dict = None, query_string: str = "") -> Dict[str, str]:
    ts = now_ts()
    payload_str = json.dumps(payload) if payload else ""
    signature = sign_request(api_secret, method.upper(), ts, path, query_string, payload_str)
    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "api-key": api_key,
        "timestamp": ts,
        "signature": signature
    }

async def place_market_order(session: aiohttp.ClientSession, api_key: str, api_secret: str,
                             product_id: int, side: str, size: float) -> Dict[str, Any]:
    """
    Place a market order on /v2/orders.
    Example payload fields: product_id, side ('buy'/'sell'), order_type ('market_order'), size.
    NOTE: adjust payload fields per your account settings (leverage, margin mode) and test thoroughly.
    """
    path = "/v2/orders"
    url = BASE_URL.rstrip("/") + path
    payload = {
        "product_id": int(product_id),
        "side": side,
        "order_type": "market_order",
        "size": size
    }
    headers = build_signed_headers(api_key, api_secret, "POST", path, payload, "")
    async with session.post(url, json=payload, headers=headers) as resp:
        try:
            return await resp.json()
        except Exception:
            text = await resp.text()
            return {"success": False, "http_status": resp.status, "text": text}

# -----------------------------
# Websocket: subscribe & handle messages
# -----------------------------
def make_subscribe_message(channel: str, symbols):
    """Create subscribe payload (docs show this pattern)."""
    return {
        "type": "subscribe",
        "payload": {
            "channels": [{ "name": channel, "symbols": symbols }]
        }
    }

def parse_candlestick_message(msg: dict) -> dict:
    """
    Parse a candlestick message—structure varies; adapt as needed.
    This function expects an object with 'type' and 'payload' and payload containing a candlestick
    or ticks with keys like 'close' and 'start_timestamp'.
    """
    # This is intentionally defensive: adapt to exact delta payload if different.
    # Example payload (from docs): { "type":"candlesticks", "channel":"candlestick_1m", "payload":[{...}] }
    payload = msg.get("payload")
    if not payload:
        return {}
    # If payload is a list of candles, take latest
    if isinstance(payload, list) and len(payload) > 0:
        last = payload[-1]
    elif isinstance(payload, dict):
        # sometimes payload may contain `candles` or `data`
        last = payload.get("data") or payload.get("candles") or payload
        if isinstance(last, list) and last:
            last = last[-1]
    else:
        last = {}
    # Try common keys
    close = last.get("close") or last.get("c") or last.get("close_price")
    ts = last.get("start_timestamp") or last.get("timestamp") or last.get("t")
    # normalize numeric types
    try:
        close = float(close) if close is not None else None
    except Exception:
        close = None
    return {"close": close, "timestamp": ts, "raw": last}

async def run_bot(
    product_symbol: str,
    product_id: int,
    window: int,
    long_th: float,
    short_th: float,
    order_size: float,
    api_key: str,
    api_secret: str,
    ws_url: str,
):
    """Main orchestration loop: connect to WS, update rolling state, place orders on flips."""
    state = make_rolling_state(window)
    session_timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=session_timeout) as session:
        async with websockets.connect(ws_url, ping_interval=20, max_size=None) as ws:
            # subscribe to 1m candlesticks for the chosen symbol
            subscribe_msg = make_subscribe_message("candlesticks_1m", [product_symbol])
            await ws.send(json.dumps(subscribe_msg))
            print(f"Subscribed to candlesticks_1m for {product_symbol} at {ws_url}")

            async def do_place_order(side: str, size: float):
                if not product_id:
                    print("ERROR: PRODUCT_ID not set. Set PRODUCT_ID env var for placing REST orders.")
                    return
                print(f"Placing {side} market order size={size}")
                resp = await place_market_order(session, api_key, api_secret, product_id, side, size)
                print("Order response:", resp)

            # We'll store last_direction to detect entry/exit pairs
            try:
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue
                    # Filter for candlestick events; adapt to actual message format
                    if (msg.get("type") and "candlestick" in msg.get("type")) or msg.get("channel","").startswith("candlestick"):
                        parsed = parse_candlestick_message(msg)
                        close = parsed.get("close")
                        if close is None:
                            # ignore non-numeric or incomplete messages
                            continue

                        # update rolling state & compute return in percent
                        _, ret_pct = update_returns_state(state, close, window)

                        signal = compute_signal(ret_pct, long_th, short_th)
                        last_signal = state.get("last_signal", 0)

                        # detect flips of magnitude 2 (1 <-> -1)
                        if abs(signal - last_signal) == 2:
                            # flip from long->short or short->long
                            # close previous side, enter new side.
                            # Example flow: if last_signal==1 and signal==-1:
                            #   - place sell to close previous long (depends on position mgmt)
                            #   - place sell to open short (market)
                            # For simplicity: place single market order on new side.
                            side = "buy" if signal == 1 else "sell"
                            # We place a market order to go in direction of signal
                            asyncio.create_task(do_place_order(side, order_size))
                            print(f"Signal flip {last_signal} -> {signal} at {parsed.get('timestamp')} close={close} ret%={ret_pct:.6f}")

                        # update saved signal
                        state["last_signal"] = signal

                    # handle ping/pong or other event types as necessary
            except websockets.ConnectionClosed as e:
                print("Websocket closed:", e)
            except Exception as e:
                print("Websocket loop error:", e)


# -----------------------------
# Entrypoint
# -----------------------------
def _env_or_fail(name: str) -> str:
    val = os.getenv(name, "")
    if val == "":
        raise RuntimeError(f"missing env var {name}; set it before running")
    return val

def main():
    if not API_KEY or not API_SECRET:
        raise RuntimeError("Set DELTA_API_KEY and DELTA_API_SECRET as environment variables before running.")

    # PRODUCT_ID required for REST orders. If not provided, we will refuse to place orders.
    pid = int(PRODUCT_ID) if PRODUCT_ID else 0

    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_bot(
        product_symbol=PRODUCT_SYMBOL,
        product_id=pid,
        window=WINDOW,
        long_th=LONG_THRESH,
        short_th=SHORT_THRESH,
        order_size=ORDER_SIZE,
        api_key=API_KEY,
        api_secret=API_SECRET,
        ws_url=WS_URL
    ))

if __name__ == "__main__":
    main()
