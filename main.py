import codecs
import encodings

# Restore/register Python's standard codec search function.
codecs.register(encodings.search_function)

from flask import Flask, request, jsonify
import websocket
import json
import os
import time
import sqlite3
import threading
import urllib.request
import urllib.parse

# Verify the standard IDNA codec.
try:
    codecs.lookup("idna")
    print("[Startup] IDNA codec: OK")
except LookupError as e:
    print(f"[Startup] IDNA codec ERROR: {e}")

app = Flask(__name__)

# ========================= CONFIG =========================
DERIV_APP_ID = os.environ.get("DERIV_APP_ID", "YOUR_APP_ID_HERE")
DERIV_WS_URL = f"wss://ws.derivws.com/websockets/v3?app_id={DERIV_APP_ID}"
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
ADMIN_KEY = os.environ.get("ADMIN_KEY", "")
DB_PATH = os.environ.get("DB_PATH", "structure_memory.db")

TIMEFRAME_MAP = {
    "M1": 60, "M5": 300, "M15": 900, "M30": 1800,
    "H1": 3600, "H4": 14400, "D1": 86400,
}
SYMBOL_MAP = {
    "VOLATILITY10": "R_10", "VOLATILITY25": "R_25", "VOLATILITY50": "R_50",
    "VOLATILITY75": "R_75", "VOLATILITY100": "R_100",
    "BOOM500": "BOOM500", "BOOM1000": "BOOM1000",
    "CRASH500": "CRASH500", "CRASH1000": "CRASH1000",
}
DB_LOCK = threading.Lock()
SCAN_LOCK = threading.Lock()
SCANNER_THREAD = None
SCANNER_STOP = threading.Event()
SCANNER_LAST = {}

# ==========================================================
# DATABASE / MEMORY
# ==========================================================
def db():
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("""CREATE TABLE IF NOT EXISTS structures(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        structure_key TEXT UNIQUE,
        pair TEXT, timeframe TEXT, direction TEXT, state TEXT,
        a_json TEXT, b_json TEXT, c_json TEXT, d_json TEXT, e_json TEXT, f_json TEXT,
        fib50 REAL, valid INTEGER DEFAULT 0, telegram_sent INTEGER DEFAULT 0,
        created_epoch INTEGER, updated_epoch INTEGER,
        live_from_epoch INTEGER DEFAULT 0
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS telegram_users(
        chat_id TEXT PRIMARY KEY, username TEXT, active INTEGER DEFAULT 1, created_epoch INTEGER
    )""")
    # Upgrade databases created by V2.
    cols = {r[1] for r in c.execute("PRAGMA table_info(structures)").fetchall()}
    if "live_from_epoch" not in cols:
        c.execute("ALTER TABLE structures ADD COLUMN live_from_epoch INTEGER DEFAULT 0")
    c.commit()
    return c


def j(x):
    return json.dumps(x, separators=(",", ":")) if x else None


def point(label, role, candle, price):
    return {
        "label": label, "role": role, "price": float(price), "epoch": int(candle["epoch"]),
        "open": candle["open"], "high": candle["high"], "low": candle["low"], "close": candle["close"]
    }


def structure_key(s):
    return f"{s['pair']}|{s['timeframe']}|{s['direction']}|{s['a']['epoch']}|{s['b']['epoch']}|{s['c']['epoch']}"


def fib50(b, e, direction):
    return (b + e) / 2.0


def save(s):
    now = int(time.time())
    k = structure_key(s)
    with DB_LOCK:
        c = db()
        c.execute("""INSERT INTO structures(
            structure_key,pair,timeframe,direction,state,a_json,b_json,c_json,d_json,e_json,f_json,
            fib50,valid,telegram_sent,created_epoch,updated_epoch,live_from_epoch
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(structure_key) DO UPDATE SET
            state=excluded.state,d_json=excluded.d_json,e_json=excluded.e_json,f_json=excluded.f_json,
            fib50=excluded.fib50,valid=excluded.valid,updated_epoch=excluded.updated_epoch,
            live_from_epoch=excluded.live_from_epoch""",
            (k, s["pair"], s["timeframe"], s["direction"], s["state"],
             j(s.get("a")), j(s.get("b")), j(s.get("c")), j(s.get("d")), j(s.get("e")), j(s.get("f")),
             s.get("fib50"), int(bool(s.get("valid"))), int(bool(s.get("telegram_sent"))),
             s.get("created_epoch", s["a"]["epoch"]), now, int(s.get("live_from_epoch", 0))))
        c.commit(); c.close()
    return k


def row_to_s(r):
    def p(name): return json.loads(r[name]) if r[name] else None
    return {
        "id": r["id"], "structure_key": r["structure_key"], "pair": r["pair"],
        "timeframe": r["timeframe"], "direction": r["direction"], "state": r["state"],
        "a": p("a_json"), "b": p("b_json"), "c": p("c_json"), "d": p("d_json"),
        "e": p("e_json"), "f": p("f_json"), "fib50": r["fib50"], "valid": bool(r["valid"]),
        "telegram_sent": bool(r["telegram_sent"]), "live_from_epoch": int(r["live_from_epoch"] or 0)
    }


def structures_for(pair, tf):
    with DB_LOCK:
        c = db(); rows = c.execute(
            "SELECT * FROM structures WHERE pair=? AND timeframe=? ORDER BY created_epoch",
            (pair, tf)).fetchall(); c.close()
    return [row_to_s(r) for r in rows]

# ==========================================================
# DERIV DATA
# ==========================================================
def get_candles(symbol, granularity, count=500):
    result, err = [], None
    done = threading.Event()

    def on_message(ws, message):
        nonlocal result, err
        try:
            data = json.loads(message)
            if "candles" in data:
                result = data["candles"]; done.set()
            elif "error" in data:
                err = data["error"].get("message", str(data["error"])); done.set()
        except Exception as e:
            err = str(e); done.set()

    def on_error(ws, error):
        nonlocal err
        err = str(error); done.set()

    def on_open(ws):
        ws.send(json.dumps({
            "ticks_history": symbol, "adjust_start_time": 1,
            "count": max(100, min(int(count), 1000)),
            "granularity": granularity, "style": "candles", "end": "latest"
        }))

    ws = websocket.WebSocketApp(DERIV_WS_URL, on_open=on_open, on_message=on_message, on_error=on_error)
    threading.Thread(target=ws.run_forever, daemon=True).start()
    done.wait(25)
    try: ws.close()
    except Exception: pass
    if err: print("[Deriv]", err)
    return sorted(result, key=lambda x: int(x.get("epoch", 0)))


def normalize(rows):
    return [{
        "epoch": int(x.get("epoch", 0)), "open": float(x["open"]), "high": float(x["high"]),
        "low": float(x["low"]), "close": float(x["close"]), "volume": float(x.get("volume", 0))
    } for x in rows]


def closed_candles(rows, granularity):
    """Return only fully closed candles. The currently forming candle is ignored."""
    now = int(time.time())
    return [x for x in rows if x["epoch"] + int(granularity) <= now]

# ==========================================================
# STRUCTURE TOOLS
# ==========================================================
def atrs(cs, period=14):
    tr = []
    for i, x in enumerate(cs):
        if i == 0: tr.append(x["high"] - x["low"])
        else:
            p = cs[i-1]
            tr.append(max(x["high"] - x["low"], abs(x["high"] - p["close"]), abs(x["low"] - p["close"])))
    out = [0.0] * len(cs)
    for i in range(period, len(cs)):
        out[i] = sum(tr[i-period+1:i+1]) / period
    return out


def swings(cs, strength=3):
    s = max(1, int(strength)); out = []
    for i in range(s, len(cs)-s):
        x = cs[i]
        is_low = all(x["low"] < y["low"] for y in cs[i-s:i]) and all(x["low"] <= y["low"] for y in cs[i+1:i+s+1])
        is_high = all(x["high"] > y["high"] for y in cs[i-s:i]) and all(x["high"] >= y["high"] for y in cs[i+1:i+s+1])
        if is_low: out.append(("L", i, x))
        if is_high: out.append(("H", i, x))
    return sorted(out, key=lambda z: z[1])

# Build only chronological A-B-C candidates. This prevents the V2 combinatorial explosion.
def discover_abc(cs, pair, tf, direction, strength, min_atr):
    sw = swings(cs, strength)
    av = atrs(cs)
    out = []
    wanted = "L" if direction == "BULLISH" else "H"
    opposite = "H" if direction == "BULLISH" else "L"

    same = [x for x in sw if x[0] == wanted]
    opp = [x for x in sw if x[0] == opposite]

    for idx in range(len(same)-1):
        a = same[idx]; b = same[idx+1]
        # A -> B must be the next significant swing of the same type.
        if direction == "BULLISH" and b[2]["low"] >= a[2]["low"]: continue
        if direction == "BEARISH" and b[2]["high"] <= a[2]["high"]: continue
        if min_atr and av[b[1]]:
            displacement = (a[2]["low"] - b[2]["low"]) if direction == "BULLISH" else (b[2]["high"] - a[2]["high"])
            if displacement < av[b[1]] * min_atr: continue
        c = next((x for x in opp if x[1] > b[1]), None)
        if not c: continue
        s = {
            "pair": pair, "timeframe": tf, "direction": direction,
            "a": point("A", "SWEEP", a[2], a[2]["low"] if direction == "BULLISH" else a[2]["high"]),
            "b": point("B", "SWING", b[2], b[2]["low"] if direction == "BULLISH" else b[2]["high"]),
            "c": point("C", "STRUCTURE", c[2], c[2]["high"] if direction == "BULLISH" else c[2]["low"]),
            "d": None, "e": None, "f": None, "fib50": None,
            "state": "WAITING_FOR_BOS", "valid": False, "telegram_sent": False,
        }
        out.append(s)
    return out


def advance(s, cs, bos="body", exp_atr=0.5, disp_atr=1.0, allow_historical_f=False):
    """Advance one stored structure. A/B/C/D/E are locked once written.
    F is allowed only from live_from_epoch onward for a backfilled structure.
    """
    ix = {x["epoch"]: i for i, x in enumerate(cs)}
    av = atrs(cs)
    d = s["direction"]
    ci = ix.get(s["c"]["epoch"])
    if ci is None: return s
    level = s["c"]["price"]

    # D: first confirmed BOS after C. Once locked, never replace it.
    if s["d"] is None:
        for i in range(ci+1, len(cs)):
            x = cs[i]
            broken = (x["close"] > level if d == "BULLISH" else x["close"] < level) if bos == "body" else (x["high"] > level if d == "BULLISH" else x["low"] < level)
            if broken:
                s["d"] = point("D", "BOS", x, x["high"] if d == "BULLISH" else x["low"])
                s["state"] = "WAITING_FOR_EXPANSION"
                break
    if not s["d"]: return s

    di = ix.get(s["d"]["epoch"])
    if di is None: return s

    # E: first significant expansion swing after D. Once locked, never replace it.
    if s["e"] is None:
        sw = swings(cs, 2)
        for kind, i, x in sw:
            if i <= di: continue
            move = x["high"] - level if d == "BULLISH" else level - x["low"]
            if move <= 0: continue
            base_atr = av[di] or av[i]
            if base_atr and move < base_atr * exp_atr: continue
            if (d == "BULLISH" and kind != "H") or (d == "BEARISH" and kind != "L"): continue
            s["e"] = point("E", "EXPANSION", x, x["high"] if d == "BULLISH" else x["low"])
            s["fib50"] = fib50(s["b"]["price"], s["e"]["price"], d)
            s["state"] = "WAITING_FOR_DISPLACEMENT"
            break
    if not s["e"]: return s

    ei = ix.get(s["e"]["epoch"])
    if ei is None: return s
    mid = s["fib50"]
    live_from = int(s.get("live_from_epoch", 0))

    # F: ONLY a new candle after the structure's live boundary can trigger an alert.
    # Historical F is deliberately ignored during backfill.
    if s["f"] is None:
        for i in range(ei+1, len(cs)):
            x = cs[i]
            if not allow_historical_f and live_from and x["epoch"] <= live_from:
                continue
            fprice = x["low"] if d == "BULLISH" else x["high"]
            reaches = fprice <= mid if d == "BULLISH" else fprice >= mid
            rng = x["high"] - x["low"]
            body = abs(x["close"] - x["open"])
            if not reaches: continue
            if not av[i] or rng < av[i] * 1.1 or body < av[i] * disp_atr: continue
            s["f"] = point("F", "DISPLACEMENT", x, fprice)
            s["valid"] = True
            s["state"] = "COMPLETE"
            break
    return s

# ==========================================================
# TELEGRAM
# ==========================================================
def tg_send(chat_id, text):
    if not TELEGRAM_BOT_TOKEN: return False
    data = urllib.parse.urlencode({"chat_id": str(chat_id), "text": text, "parse_mode": "HTML"}).encode()
    req = urllib.request.Request(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage", data=data)
    try:
        urllib.request.urlopen(req, timeout=10).read(); return True
    except Exception as e:
        print("[Telegram]", e); return False


def signal_text(s):
    icon = "🟢" if s["direction"] == "BULLISH" else "🔴"
    lines = [f'{icon} <b>{s["direction"]} A→F STRUCTURE CONFIRMED</b>', "",
             f'<b>Symbol:</b> {s["pair"]}', f'<b>Timeframe:</b> {s["timeframe"]}', ""]
    for k in ("a", "b", "c", "d", "e", "f"):
        p = s[k]
        lines += [f'<b>{k} — {p["role"]}</b>', f'Price: <code>{p["price"]}</code>',
                  f'Time: {time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(p["epoch"]))}', ""]
    lines += ["━━━━━━━━━━━━━━━━━━", f'<b>50% Fibonacci:</b> <code>{s["fib50"]}</code>',
              "", "<b>ENTRY:</b> Manual", "<b>TP:</b> Manual", "<b>SL:</b> Manual",
              "", "A→F is confirmed. The engine does not decide entry, TP or SL."]
    return "\n".join(lines)


def alert(s):
    k = structure_key(s)
    with DB_LOCK:
        c = db(); r = c.execute("SELECT telegram_sent FROM structures WHERE structure_key=?", (k,)).fetchone()
        if r and r["telegram_sent"]: c.close(); return 0
        users = c.execute("SELECT chat_id FROM telegram_users WHERE active=1").fetchall(); c.close()
    sent = sum(1 for u in users if tg_send(u["chat_id"], signal_text(s)))
    if sent:
        with DB_LOCK:
            c = db(); c.execute("UPDATE structures SET telegram_sent=1,updated_epoch=? WHERE structure_key=?", (int(time.time()), k)); c.commit(); c.close()
    return sent

# ==========================================================
# SCAN ENGINE
# ==========================================================
def run_scan(pair, tf, cs, strength, bos, min_atr, exp_atr, disp_atr):
    latest_epoch = cs[-1]["epoch"]
    mem = structures_for(pair, tf)
    keys = {x["structure_key"] for x in mem}

    # Discover historical A-B-C-D-E candidates. Historical F is intentionally suppressed.
    for direction in ("BULLISH", "BEARISH"):
        for s in discover_abc(cs, pair, tf, direction, strength, min_atr):
            k = structure_key(s)
            if k in keys: continue
            # The candidate becomes live only AFTER this backfill scan.
            s["live_from_epoch"] = latest_epoch
            s = advance(s, cs, bos, exp_atr, disp_atr, allow_historical_f=False)
            save(s)
            mem.append(s); keys.add(k)

    completed = []
    # Existing structures are advanced. F is only permitted after their live boundary.
    for s in mem:
        old = s["state"]
        # V2 compatibility: if an old structure has no live boundary, establish
        # the boundary now so V3 never turns an already-existing historical F
        # into a fresh Telegram alert.
        if not s.get("live_from_epoch"):
            s["live_from_epoch"] = latest_epoch
            save(s)
        if s["state"] != "COMPLETE":
            s = advance(s, cs, bos, exp_atr, disp_atr, allow_historical_f=False)
            save(s)
        if old != "COMPLETE" and s["state"] == "COMPLETE":
            sent = alert(s)
            s["telegram_sent_now"] = bool(sent)
            completed.append(s)

    return {
        "pair": pair, "timeframe": tf, "scan_mode": "DUAL_DIRECTION",
        "memory_enabled": True, "chronological_locking": True,
        "historical_f_alerts_suppressed": True,
        "active_structures": sum(1 for x in mem if x["state"] != "COMPLETE"),
        "stored_structures": len(mem), "completed_now": len(completed),
        "signals": mem, "completed_signals": completed,
    }

# ==========================================================
# CONTINUOUS LIVE SCANNER
# ==========================================================
def env_list(name, default):
    raw = os.environ.get(name, default)
    return [x.strip().upper() for x in raw.split(",") if x.strip()]


def scanner_settings():
    pairs = env_list("SCANNER_PAIRS", "VOLATILITY100")
    tfs = [x for x in env_list("SCANNER_TIMEFRAMES", "M15") if x in TIMEFRAME_MAP]
    return {
        "enabled": os.environ.get("SCANNER_ENABLED", "true").lower() in ("1", "true", "yes", "on"),
        "pairs": pairs,
        "timeframes": tfs or ["M15"],
        "interval": max(5, int(os.environ.get("SCANNER_INTERVAL_SECONDS", "10"))),
        "count": max(100, min(1000, int(os.environ.get("SCANNER_CANDLE_COUNT", "500")))),
        "strength": int(os.environ.get("SCANNER_STRENGTH", "3")),
        "bos": os.environ.get("SCANNER_BOS", "body").lower(),
        "min_atr": float(os.environ.get("SCANNER_MIN_ATR_MOVE", ".25")),
        "exp_atr": float(os.environ.get("SCANNER_MIN_EXPANSION_ATR", ".5")),
        "disp_atr": float(os.environ.get("SCANNER_DISPLACEMENT_ATR", "1.0")),
    }


def continuous_scan_once(pair, tf, cfg):
    gran = TIMEFRAME_MAP[tf]
    cs = closed_candles(normalize(get_candles(SYMBOL_MAP.get(pair, pair), gran, cfg["count"])), gran)
    if len(cs) < max(50, cfg["strength"] * 4):
        return {"ok": False, "reason": "not_enough_closed_candles", "pair": pair, "timeframe": tf}
    latest = cs[-1]["epoch"]
    cache_key = f"{pair}|{tf}"
    # A structure only changes when a new closed candle arrives. This prevents
    # hammering the engine and avoids duplicate Telegram sends.
    if SCANNER_LAST.get(cache_key) == latest:
        return {"ok": True, "pair": pair, "timeframe": tf, "skipped": True, "latest_closed_epoch": latest}
    result = run_scan(pair, tf, cs, cfg["strength"], cfg["bos"], cfg["min_atr"], cfg["exp_atr"], cfg["disp_atr"])
    SCANNER_LAST[cache_key] = latest
    return {"ok": True, "pair": pair, "timeframe": tf, "skipped": False, "latest_closed_epoch": latest,
            "completed_now": result["completed_now"], "active_structures": result["active_structures"]}


def scanner_loop():
    print("[Scanner] Continuous live scanner started")
    while not SCANNER_STOP.is_set():
        try:
            cfg = scanner_settings()
            if cfg["enabled"]:
                # One worker thread owns live scanning, so Telegram alerts cannot
                # be duplicated by multiple concurrent scan requests.
                if SCAN_LOCK.acquire(blocking=False):
                    try:
                        for pair in cfg["pairs"]:
                            for tf in cfg["timeframes"]:
                                if SCANNER_STOP.is_set(): break
                                try:
                                    r = continuous_scan_once(pair, tf, cfg)
                                    if r.get("completed_now"):
                                        print(f"[Scanner] {pair} {tf}: {r['completed_now']} new A→F signal(s)")
                                except Exception as e:
                                    print(f"[Scanner] {pair} {tf} error: {e}")
                    finally:
                        SCAN_LOCK.release()
        except Exception as e:
            print("[Scanner] loop error:", e)
        SCANNER_STOP.wait(cfg.get("interval", 10) if 'cfg' in locals() else 10)
    print("[Scanner] Continuous live scanner stopped")


def start_scanner():
    global SCANNER_THREAD
    if SCANNER_THREAD and SCANNER_THREAD.is_alive():
        return False
    SCANNER_STOP.clear()
    SCANNER_THREAD = threading.Thread(target=scanner_loop, name="freedom-live-scanner", daemon=True)
    SCANNER_THREAD.start()
    return True


def stop_scanner():
    SCANNER_STOP.set()
    return True

# ==========================================================
# API
# ==========================================================
@app.route("/")
def home():
   return "Freedom Structure Scanner V4 is running ✅"

@app.route("/health")
def health():
    return jsonify({
        "ok": True, "engine": "Freedom Structure Scanner V4",
        "memory": True, "chronological_locking": True,
        "telegram_configured": bool(TELEGRAM_BOT_TOKEN),
        "continuous_scanner": bool(SCANNER_THREAD and SCANNER_THREAD.is_alive()) and not SCANNER_STOP.is_set(),
        "scanner_pairs": scanner_settings()["pairs"],
        "scanner_timeframes": scanner_settings()["timeframes"],
        "timeframes": list(TIMEFRAME_MAP)
    })

@app.route("/ohlc")
def ohlc():
    pair = request.args.get("pair", "").upper(); tf = request.args.get("timeframe", "M15").upper()
    count = int(request.args.get("count", 500))
    if tf not in TIMEFRAME_MAP: return jsonify({"error": "Unsupported timeframe", "supported": list(TIMEFRAME_MAP)}), 400
    return jsonify(closed_candles(normalize(get_candles(SYMBOL_MAP.get(pair, pair), TIMEFRAME_MAP[tf], count)), TIMEFRAME_MAP[tf]))

@app.route("/scan")
def scan():
    pair = request.args.get("pair", "").upper(); tf = request.args.get("timeframe", "M15").upper()
    count = int(request.args.get("count", 500)); strength = int(request.args.get("strength", 3))
    bos = request.args.get("bos", "body").lower(); bos = bos if bos in ("body", "wick") else "body"
    min_atr = float(request.args.get("min_atr_move", .25)); exp_atr = float(request.args.get("min_expansion_atr", .5)); disp_atr = float(request.args.get("displacement_atr", 1.0))
    if tf not in TIMEFRAME_MAP: return jsonify({"error": "Unsupported timeframe", "supported": list(TIMEFRAME_MAP)}), 400
    cs = closed_candles(normalize(get_candles(SYMBOL_MAP.get(pair, pair), TIMEFRAME_MAP[tf], count)), TIMEFRAME_MAP[tf])
    if len(cs) < max(50, strength * 4): return jsonify({"error": "Not enough candles returned", "candles": len(cs)}), 502
    return jsonify(run_scan(pair, tf, cs, strength, bos, min_atr, exp_atr, disp_atr))

@app.route("/scanner/status")
def scanner_status():
    cfg = scanner_settings()
    return jsonify({
        "running": bool(SCANNER_THREAD and SCANNER_THREAD.is_alive()) and not SCANNER_STOP.is_set(),
        "enabled": cfg["enabled"], "pairs": cfg["pairs"], "timeframes": cfg["timeframes"],
        "interval_seconds": cfg["interval"], "candle_count": cfg["count"],
        "last_processed": SCANNER_LAST
    })


@app.route("/scanner/start", methods=["POST"])
def scanner_start():
    if ADMIN_KEY and request.headers.get("X-Admin-Key") != ADMIN_KEY:
        return jsonify({"error": "unauthorized"}), 401
    start_scanner()
    return jsonify({"ok": True, "message": "Continuous scanner started"})


@app.route("/scanner/stop", methods=["POST"])
def scanner_stop():
    if not ADMIN_KEY or request.headers.get("X-Admin-Key") != ADMIN_KEY:
        return jsonify({"error": "unauthorized"}), 401
    stop_scanner()
    return jsonify({"ok": True, "message": "Continuous scanner stopping"})


@app.route("/structures")
def structures():
    pair = request.args.get("pair"); tf = request.args.get("timeframe")
    sql = "SELECT * FROM structures"; args = []; where = []
    if pair: where.append("pair=?"); args.append(pair.upper())
    if tf: where.append("timeframe=?"); args.append(tf.upper())
    if where: sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY updated_epoch DESC LIMIT 300"
    with DB_LOCK:
        c = db(); rows = c.execute(sql, args).fetchall(); c.close()
    return jsonify([row_to_s(r) for r in rows])

@app.route("/telegram/register", methods=["POST"])
def register():
    d = request.get_json(force=True); chat = d.get("chat_id")
    if chat is None: return jsonify({"error": "chat_id required"}), 400
    with DB_LOCK:
        c = db(); c.execute("INSERT INTO telegram_users(chat_id,username,active,created_epoch) VALUES(?,?,1,?) ON CONFLICT(chat_id) DO UPDATE SET username=excluded.username,active=1", (str(chat), d.get("username", ""), int(time.time()))); c.commit(); c.close()
    return jsonify({"ok": True, "chat_id": str(chat)})

@app.route("/telegram/users")
def users():
    if ADMIN_KEY and request.headers.get("X-Admin-Key") != ADMIN_KEY: return jsonify({"error": "unauthorized"}), 401
    with DB_LOCK:
        c = db(); rows = c.execute("SELECT chat_id,username,active,created_epoch FROM telegram_users ORDER BY created_epoch DESC").fetchall(); c.close()
    return jsonify([dict(r) for r in rows])

@app.route("/telegram/broadcast", methods=["POST"])
def broadcast():
    if ADMIN_KEY and request.headers.get("X-Admin-Key") != ADMIN_KEY: return jsonify({"error": "unauthorized"}), 401
    text = request.get_json(force=True).get("text", "")
    with DB_LOCK:
        c = db(); users = c.execute("SELECT chat_id FROM telegram_users WHERE active=1").fetchall(); c.close()
    sent = sum(1 for u in users if tg_send(u["chat_id"], text))
    return jsonify({"targeted": len(users), "sent": sent})

@app.route("/admin/reset", methods=["POST"])
def reset():
    if not ADMIN_KEY or request.headers.get("X-Admin-Key") != ADMIN_KEY:
        return jsonify({"error": "unauthorized"}), 401
    pair = request.args.get("pair"); tf = request.args.get("timeframe")
    with DB_LOCK:
        c = db()
        if pair and tf:
            c.execute("DELETE FROM structures WHERE pair=? AND timeframe=?", (pair.upper(), tf.upper()))
        elif pair:
            c.execute("DELETE FROM structures WHERE pair=?", (pair.upper(),))
        else:
            c.execute("DELETE FROM structures")
        c.commit(); c.close()
    return jsonify({"ok": True, "message": "Structure memory reset"})

# Start once when the Gunicorn worker imports the application.
# Render should run a single web worker for this in-process scanner.
if os.environ.get("SCANNER_ENABLED", "true").lower() in ("1", "true", "yes", "on"):
    start_scanner()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

