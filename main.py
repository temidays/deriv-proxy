from flask import Flask, request, jsonify
import websocket
import json
import os
import time
from threading import Thread, Event

app = Flask(__name__)

# ==================== CONFIGURATION ====================
DERIV_APP_ID = os.environ.get("DERIV_APP_ID", "YOUR_APP_ID_HERE")
DERIV_WS_URL = f"wss://ws.derivws.com/websockets/v3?app_id={DERIV_APP_ID}"

TIMEFRAME_MAP = {
    "M1": 60, "M5": 300, "M15": 900, "M30": 1800,
    "H1": 3600, "H4": 14400, "D1": 86400
}

SYMBOL_MAP = {
    "VOLATILITY25": "R_25", "VOLATILITY10": "R_10",
    "VOLATILITY50": "R_50", "VOLATILITY75": "R_75", "VOLATILITY100": "R_100",
    "BOOM500": "BOOM500", "BOOM1000": "BOOM1000",
    "CRASH500": "CRASH500", "CRASH1000": "CRASH1000",
}
# =======================================================

def get_deriv_candles(symbol, granularity, count=100):
    result = []
    error_msg = None
    done = Event()

    def on_message(ws, message):
        nonlocal result, error_msg
        try:
            data = json.loads(message)
            if "candles" in data:
                result = data["candles"]
                done.set()
            elif "error" in data:
                error_msg = data["error"]
                print(f"[Deriv Error] {error_msg}")
                done.set()
            else:
                print(f"[Deriv Response] {data}")
        except Exception as e:
            error_msg = str(e)
            print(f"[Parse Error] {e}")
            done.set()

    def on_error(ws, error):
        nonlocal error_msg
        error_msg = str(error)
        print(f"[WebSocket Error] {error}")
        done.set()

    def on_open(ws):
        payload = {
            "ticks_history": symbol,
            "adjust_start_time": 1,
            "count": count,
            "granularity": granularity,
            "style": "candles",
            "end": "latest"
        }
        ws.send(json.dumps(payload))

    try:
        ws = websocket.WebSocketApp(
            DERIV_WS_URL,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error
        )
        thread = Thread(target=ws.run_forever)
        thread.daemon = True
        thread.start()

        done.wait(timeout=25)  # Increased timeout
        ws.close()

        if error_msg:
            print(f"[Final Error] {error_msg}")
            return []

        return result

    except Exception as e:
        print(f"[Critical Error] {e}")
        return []


@app.route("/")
def home():
    return "Deriv Proxy v2 is running ✅"


@app.route("/ohlc")
def get_ohlc():
    pair = request.args.get("pair", "").upper()
    timeframe = request.args.get("timeframe", "M15").upper()
    count = int(request.args.get("count", 100))

    deriv_symbol = SYMBOL_MAP.get(pair, pair)

    if timeframe not in TIMEFRAME_MAP:
        return jsonify({"error": f"Unsupported timeframe: {timeframe}"}), 400

    granularity = TIMEFRAME_MAP[timeframe]

    print(f"[Request] pair={pair} → {deriv_symbol}, tf={timeframe}, count={count}")

    candles = get_deriv_candles(deriv_symbol, granularity, count)

    if not candles:
        print("[Warning] No candles returned")
        return jsonify([])

    formatted = [{
        "open": float(c["open"]),
        "high": float(c["high"]),
        "low": float(c["low"]),
        "close": float(c["close"]),
        "volume": float(c.get("volume", 0))
    } for c in candles]

    print(f"[Success] Returned {len(formatted)} candles")
    return jsonify(formatted)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
