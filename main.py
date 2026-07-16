from flask import Flask, request, jsonify
import websocket
import json
import os
from threading import Thread, Event

app = Flask(__name__)

# ==================== CONFIGURATION ====================
DERIV_APP_ID = os.environ.get("DERIV_APP_ID", "YOUR_APP_ID_HERE")
DERIV_WS_URL = f"wss://ws.derivws.com/websockets/v3?app_id={DERIV_APP_ID}"

TIMEFRAME_MAP = {
    "M1": 60,
    "M5": 300,
    "M15": 900,
    "M30": 1800,
    "H1": 3600,
    "H4": 14400,
    "D1": 86400
}

SYMBOL_MAP = {
    "VOLATILITY25": "R_25",
    "VOLATILITY10": "R_10",
    "VOLATILITY50": "R_50",
    "VOLATILITY75": "R_75",
    "VOLATILITY100": "R_100",
    "BOOM500": "BOOM500",
    "BOOM1000": "BOOM1000",
    "CRASH500": "CRASH500",
    "CRASH1000": "CRASH1000",
}
# =======================================================

def get_deriv_candles(symbol, granularity, count=160):
    result = []
    done = Event()

    def on_message(ws, message):
        nonlocal result
        data = json.loads(message)
        if "candles" in data:
            result = data["candles"]
            done.set()
        elif "error" in data:
            print("Deriv Error:", data)
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

    ws = websocket.WebSocketApp(
        DERIV_WS_URL,
        on_open=on_open,
        on_message=on_message
    )
    thread = Thread(target=ws.run_forever)
    thread.daemon = True
    thread.start()
    done.wait(timeout=20)
    ws.close()
    return result


@app.route("/")
def home():
    return "Deriv Proxy is running ✅"


@app.route("/ohlc")
def get_ohlc():
    pair = request.args.get("pair", "").upper()
    timeframe = request.args.get("timeframe", "M15").upper()
    count = int(request.args.get("count", 160))

    deriv_symbol = SYMBOL_MAP.get(pair, pair)

    if timeframe not in TIMEFRAME_MAP:
        return jsonify({"error": f"Unsupported timeframe: {timeframe}"}), 400

    granularity = TIMEFRAME_MAP[timeframe]

    try:
        candles = get_deriv_candles(deriv_symbol, granularity, count)

        if not candles:
            return jsonify([])

        formatted = []
        for c in candles:
            formatted.append({
                "open": float(c["open"]),
                "high": float(c["high"]),
                "low": float(c["low"]),
                "close": float(c["close"]),
                "volume": float(c.get("volume", 0))
            })

        return jsonify(formatted)

    except Exception as e:
        print("Error:", str(e))
        return jsonify([]), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
