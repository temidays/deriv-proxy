from flask import Flask, request, jsonify
import websocket
import json
import os
import time
import sqlite3
import threading
import urllib.request
import urllib.parse

app = Flask(__name__)

DERIV_APP_ID = os.environ.get("DERIV_APP_ID", "YOUR_APP_ID_HERE")
DERIV_WS_URL = f"wss://ws.derivws.com/websockets/v3?app_id={DERIV_APP_ID}"
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
ADMIN_KEY = os.environ.get("ADMIN_KEY", "")
DB_PATH = os.environ.get("DB_PATH", "structure_memory.db")

TIMEFRAME_MAP = {
    "M1": 60, "M5": 300, "M15": 900, "M30": 1800,
    "H1": 3600, "H4": 14400, "D1": 86400
}

SYMBOL_MAP = {
    "VOLATILITY10": "R_10", "VOLATILITY25": "R_25",
    "VOLATILITY50": "R_50", "VOLATILITY75": "R_75",
    "VOLATILITY100": "R_100",
    "BOOM500": "BOOM500", "BOOM1000": "BOOM1000",
    "CRASH500": "CRASH500", "CRASH1000": "CRASH1000"
}

db_lock = threading.Lock()


def db():
    con = sqlite3.connect(DB_PATH, timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("""CREATE TABLE IF NOT EXISTS structures (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        structure_key TEXT UNIQUE,
        pair TEXT NOT NULL,
        timeframe TEXT NOT NULL,
        direction TEXT NOT NULL,
        state TEXT NOT NULL,
        a_json TEXT, b_json TEXT, c_json TEXT, d_json TEXT, e_json TEXT, f_json TEXT,
        fib50 REAL,
        valid INTEGER DEFAULT 0,
        telegram_sent INTEGER DEFAULT 0,
        created_epoch INTEGER,
        updated_epoch INTEGER
    )""")
    con.execute("""CREATE TABLE IF NOT EXISTS telegram_users (
        chat_id TEXT PRIMARY KEY,
        username TEXT,
        active INTEGER DEFAULT 1,
        created_epoch INTEGER
    )""")
    con.commit()
    return con


def deriv_candles(symbol, granularity, count=500):
    result, error = [], None
    done = threading.Event()

    def on_message(ws, message):
        nonlocal result, error
        try:
            data = json.loads(message)
            if "candles" in data:
                result = data["candles"]
                done.set()
            elif "error" in data:
                error = data["error"].get("message", str(data["error"]))
                done.set()
        except Exception as exc:
            error = str(exc)
            done.set()

    def on_error(ws, exc):
        nonlocal error
        error = str(exc)
        done.set()

    def on_open(ws):
        ws.send(json.dumps({
            "ticks_history": symbol,
            "adjust_start_time": 1,
            "count": max(50, min(int(count), 1000)),
            "granularity": granularity,
            "style": "candles",
            "end": "latest"
        }))

    ws = websocket.WebSocketApp(
        DERIV_WS_URL, on_open=on_open,
        on_message=on_message, on_error=on_error
    )
    t = threading.Thread(target=ws.run_forever, daemon=True)
    t.start()
    done.wait(25)
    ws.close()
    if error:
        print("[Deriv]", error)
    return sorted(result, key=lambda x: int(x.get("epoch", 0)))


def normalize(rows):
    return [{
        "epoch": int(x.get("epoch", 0)),
        "open": float(x["open"]),
        "high": float(x["high"]),
        "low": float(x["low"]),
        "close": float(x["close"]),
        "volume": float(x.get("volume", 0))
    } for x in rows]


def atr(candles, period=14):
    if len(candles) < period + 1:
        return 0.0
    trs = []
    start = len(candles) - period
    for i in range(start, len(candles)):
        cur, prev = candles[i], candles[i-1]
        trs.append(max(
            cur["high"] - cur["low"],
            abs(cur["high"] - prev["close"]),
            abs(cur["low"] - prev["close"])
        ))
    return sum(trs) / len(trs)


def confirmed_swings(candles, strength=3):
    points = []
    for i in range(strength, len(candles) - strength):
        x = candles[i]
        left = candles[i-strength:i]
        right = candles[i+1:i+strength+1]
        if all(x["low"] < y["low"] for y in left) and all(x["low"] <= y["low"] for y in right):
            points.append(("L", i, x))
        if all(x["high"] > y["high"] for y in left) and all(x["high"] >= y["high"] for y in right):
            points.append(("H", i, x))
    return sorted(points, key=lambda p: p[1])


def candle_body(c):
    return abs(c["close"] - c["open"])


def is_displacement(c, atr_value, direction, multiplier=1.0):
    if atr_value <= 0:
        return False
    rng = c["high"] - c["low"]
    body = candle_body(c)
    if body < atr_value * multiplier or rng < atr_value * 1.2:
        return False
    return (c["close"] > c["open"]) if direction == "BULLISH" else (c["close"] < c["open"])


def point(c, label, role, price):
    return {
        "label": label,
        "role": role,
        "price": float(price),
        "epoch": int(c["epoch"]),
        "open": c["open"],
        "high": c["high"],
        "low": c["low"],
        "close": c["close"]
    }


def detect_new_candidates(candles, pair, timeframe, strength=3, bos_mode="body"):
    """
    Historical discovery only. It creates partial A/B/C candidates and
    completed candidates where the historical sequence already contains
    D/E/F. Existing candidates are persisted separately.
    """
    swings = confirmed_swings(candles, strength)
    lows = [x for x in swings if x[0] == "L"]
    highs = [x for x in swings if x[0] == "H"]
    found = []

    # Bullish: A downside sweep, B lower structural swing, C previous high.
    for a in lows:
        for b in lows:
            if b[1] <= a[1] or b[2]["low"] >= a[2]["low"]:
                continue
            c = next((x for x in highs if x[1] > b[1]), None)
            if not c:
                continue
            found.append(build_sequence(
                candles, pair, timeframe, "BULLISH", a, b, c, bos_mode
            ))
            break

    # Bearish mirror.
    for a in highs:
        for b in highs:
            if b[1] <= a[1] or b[2]["high"] <= a[2]["high"]:
                continue
            c = next((x for x in lows if x[1] > b[1]), None)
            if not c:
                continue
            found.append(build_sequence(
                candles, pair, timeframe, "BEARISH", a, b, c, bos_mode
            ))
            break

    return [x for x in found if x]


def build_sequence(candles, pair, timeframe, direction, a, b, c, bos_mode):
    atrv = atr(candles)
    after_c = range(c[1] + 1, len(candles))
    d = None
    for i in after_c:
        x = candles[i]
        broken = (
            x["close"] > c[2]["high"] if direction == "BULLISH"
            else x["close"] < c[2]["low"]
        ) if bos_mode == "body" else (
            x["high"] > c[2]["high"] if direction == "BULLISH"
            else x["low"] < c[2]["low"]
        )
        if broken:
            d = (i, x)
            break

    obj = {
        "pair": pair, "timeframe": timeframe, "direction": direction,
        "a": point(a[2], "A", "SWEEP", a[2]["low"] if direction == "BULLISH" else a[2]["high"]),
        "b": point(b[2], "B", "SWING", b[2]["low"] if direction == "BULLISH" else b[2]["high"]),
        "c": point(c[2], "C", "STRUCTURE", c[2]["high"] if direction == "BULLISH" else c[2]["low"]),
        "d": None, "e": None, "f": None, "fib50": None,
        "state": "WAITING_FOR_BOS", "valid": False
    }
    if not d:
        return obj

    obj["d"] = point(d[1], "D", "BOS", d[1]["high"] if direction == "BULLISH" else d[1]["low"])
    obj["state"] = "WAITING_FOR_EXPANSION"

    e = None
    if direction == "BULLISH":
        e = max(((i, candles[i]) for i in range(d[0]+1, len(candles))),
                key=lambda z: z[1]["high"], default=None)
    else:
        e = min(((i, candles[i]) for i in range(d[0]+1, len(candles))),
                key=lambda z: z[1]["low"], default=None)

    if not e:
        return obj

    obj["e"] = point(e[1], "E", "EXPANSION", e[1]["high"] if direction == "BULLISH" else e[1]["low"])
    obj["state"] = "WAITING_FOR_DISPLACEMENT"

    for i in range(e[0]+1, len(candles)):
        x = candles[i]
        if is_displacement(x, atrv, direction):
            obj["f"] = point(x, "F", "DISPLACEMENT", x["close"])
            if direction == "BULLISH":
                obj["fib50"] = obj["b"]["price"] + (obj["e"]["price"] - obj["b"]["price"]) * 0.5
            else:
                obj["fib50"] = obj["b"]["price"] - (obj["b"]["price"] - obj["e"]["price"]) * 0.5
            obj["valid"] = True
            obj["state"] = "COMPLETE"
            break
    return obj


def structure_key(s):
    return f"{s['pair']}|{s['timeframe']}|{s['direction']}|{s['a']['epoch']}|{s['b']['epoch']}|{s['c']['epoch']}"


def save_candidate(s):
    now = int(time.time())
    key = structure_key(s)
    def js(x): return json.dumps(x) if x else None
    with db_lock:
        con = db()
        con.execute("""INSERT INTO structures
            (structure_key,pair,timeframe,direction,state,a_json,b_json,c_json,d_json,e_json,f_json,fib50,valid,created_epoch,updated_epoch)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(structure_key) DO UPDATE SET
              state=excluded.state,d_json=excluded.d_json,e_json=excluded.e_json,
              f_json=excluded.f_json,fib50=excluded.fib50,valid=excluded.valid,
              updated_epoch=excluded.updated_epoch""",
            (key,s["pair"],s["timeframe"],s["direction"],s["state"],
             js(s["a"]),js(s["b"]),js(s["c"]),js(s["d"]),js(s["e"]),js(s["f"]),
             s["fib50"],int(s["valid"]),s["a"]["epoch"],now))
        con.commit()
        con.close()
    return key


def telegram_send(chat_id, text):
    if not TELEGRAM_BOT_TOKEN:
        return False
    data = urllib.parse.urlencode({
        "chat_id": str(chat_id), "text": text, "parse_mode": "HTML"
    }).encode()
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
        data=data
    )
    try:
        urllib.request.urlopen(req, timeout=10).read()
        return True
    except Exception as exc:
        print("[Telegram]", exc)
        return False


def format_signal(s):
    icon = "🟢" if s["direction"] == "BULLISH" else "🔴"
    lines = [
        f"{icon} <b>{s['direction']} A→F STRUCTURE COMPLETED</b>",
        "",
        f"<b>Symbol:</b> {s['pair']}",
        f"<b>Timeframe:</b> {s['timeframe']}",
        "",
        "━━━━━━━━━━━━━━━━━━",
        "",
    ]
    for key in ("a","b","c","d","e","f"):
        p = s[key]
        lines.append(f"<b>{key.upper()} — {p['role']}</b>")
        lines.append(f"Price: <code>{p['price']}</code>")
        lines.append(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(p['epoch']))}")
        lines.append("")
    lines += [
        "━━━━━━━━━━━━━━━━━━",
        f"<b>BOS:</b> {'✅' if s['d'] else '❌'}",
        f"<b>DISPLACEMENT:</b> {'✅' if s['f'] else '❌'}",
        f"<b>FIB 50%:</b> <code>{s['fib50']}</code>" if s["fib50"] is not None else "<b>FIB 50%:</b> —",
        "",
        "<b>ENTRY / TP / SL:</b> Not calculated in this version.",
        "<b>Signal:</b> Structure information only — manually trace on chart.",
    ]
    return "\n".join(lines)


def broadcast_completed(s):
    with db_lock:
        con = db()
        users = con.execute("SELECT chat_id FROM telegram_users WHERE active=1").fetchall()
        con.close()
    sent = 0
    for row in users:
        if telegram_send(row["chat_id"], format_signal(s)):
            sent += 1
    return sent


def persist_and_alert(s):
    key = save_candidate(s)
    if s["state"] != "COMPLETE":
        return key, False

    with db_lock:
        con = db()
        row = con.execute("SELECT telegram_sent FROM structures WHERE structure_key=?", (key,)).fetchone()
        already = bool(row["telegram_sent"]) if row else False
        con.close()
    if already:
        return key, False

    sent = broadcast_completed(s)
    with db_lock:
        con = db()
        con.execute("UPDATE structures SET telegram_sent=1, updated_epoch=? WHERE structure_key=?",
                    (int(time.time()), key))
        con.commit()
        con.close()
    return key, sent > 0


@app.route("/")
def home():
    return "Deriv Proxy v4 — Persistent A-F Structure Engine is running ✅"


@app.route("/ohlc")
def ohlc():
    pair = request.args.get("pair", "").upper()
    timeframe = request.args.get("timeframe", "M15").upper()
    count = int(request.args.get("count", 500))
    if timeframe not in TIMEFRAME_MAP:
        return jsonify({"error": f"Unsupported timeframe: {timeframe}"}), 400
    candles = normalize(deriv_candles(SYMBOL_MAP.get(pair, pair), TIMEFRAME_MAP[timeframe], count))
    return jsonify(candles)


@app.route("/scan")
def scan():
    pair = request.args.get("pair", "").upper()
    timeframe = request.args.get("timeframe", "M15").upper()
    strength = int(request.args.get("strength", 3))
    bos = request.args.get("bos", "body").lower()
    count = int(request.args.get("count", 500))
    if timeframe not in TIMEFRAME_MAP:
        return jsonify({"error": f"Unsupported timeframe: {timeframe}"}), 400

    candles = normalize(deriv_candles(SYMBOL_MAP.get(pair, pair), TIMEFRAME_MAP[timeframe], count))
    candidates = detect_new_candidates(candles, pair, timeframe, strength, bos)

    stored = []
    for s in candidates:
        key, sent = persist_and_alert(s)
        stored.append({
            "structure_key": key,
            "state": s["state"],
            "direction": s["direction"],
            "telegram_sent_now": sent,
            **{k: s[k] for k in ("a","b","c","d","e","f","fib50","valid")}
        })

    return jsonify({
        "pair": pair, "timeframe": timeframe,
        "scan_mode": "DUAL_DIRECTION",
        "memory_enabled": True,
        "signals": stored
    })


@app.route("/structures")
def structures():
    pair = request.args.get("pair")
    timeframe = request.args.get("timeframe")
    sql = "SELECT * FROM structures"
    args = []
    where = []
    if pair:
        where.append("pair=?"); args.append(pair.upper())
    if timeframe:
        where.append("timeframe=?"); args.append(timeframe.upper())
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY updated_epoch DESC LIMIT 200"
    with db_lock:
        con = db()
        rows = con.execute(sql, args).fetchall()
        con.close()

    out = []
    for r in rows:
        item = dict(r)
        for k in ("a_json","b_json","c_json","d_json","e_json","f_json"):
            item[k[:-5]] = json.loads(item.pop(k)) if item[k] else None
        out.append(item)
    return jsonify(out)


@app.route("/telegram/register", methods=["POST"])
def telegram_register():
    data = request.get_json(force=True)
    chat_id = data.get("chat_id")
    if chat_id is None:
        return jsonify({"error": "chat_id required"}), 400
    with db_lock:
        con = db()
        con.execute("""INSERT INTO telegram_users(chat_id,username,active,created_epoch)
                       VALUES(?,?,1,?)
                       ON CONFLICT(chat_id) DO UPDATE SET username=excluded.username,active=1""",
                    (str(chat_id), data.get("username",""), int(time.time())))
        con.commit()
        con.close()
    return jsonify({"ok": True})


@app.route("/telegram/broadcast", methods=["POST"])
def telegram_broadcast():
    if ADMIN_KEY and request.headers.get("X-Admin-Key") != ADMIN_KEY:
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json(force=True)
    text = data.get("text","")
    with db_lock:
        con = db()
        users = con.execute("SELECT chat_id FROM telegram_users WHERE active=1").fetchall()
        con.close()
    sent = sum(1 for r in users if telegram_send(r["chat_id"], text))
    return jsonify({"targeted": len(users), "sent": sent})


@app.route("/telegram/users")
def telegram_users():
    if ADMIN_KEY and request.headers.get("X-Admin-Key") != ADMIN_KEY:
        return jsonify({"error": "unauthorized"}), 401
    with db_lock:
        con = db()
        rows = con.execute("SELECT chat_id,username,active,created_epoch FROM telegram_users ORDER BY created_epoch DESC").fetchall()
        con.close()
    return jsonify([dict(r) for r in rows])


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
