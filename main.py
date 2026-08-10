import codecs
import encodings
import json
import os
import sqlite3
import threading
import time
import urllib.parse
import urllib.request

# Restore/register Python's standard codec search function.
try:
    codecs.register(encodings.search_function)
except Exception:
    pass

from flask import Flask, jsonify, request
import websocket

# ============================================================
# STARTUP / CODEC FIX
# ============================================================
try:
    codecs.lookup("idna")
    print("[Startup] IDNA codec: OK")
except LookupError as e:
    print(f"[Startup] IDNA codec ERROR: {e}")

app = Flask(__name__)

# ============================================================
# CONFIG
# ============================================================
DERIV_APP_ID = os.environ.get("DERIV_APP_ID", "YOUR_APP_ID_HERE")
DERIV_WS_URL = f"wss://ws.derivws.com/websockets/v3?app_id={DERIV_APP_ID}"

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
ADMIN_KEY = os.environ.get("ADMIN_KEY", "")
DB_PATH = os.environ.get("DB_PATH", "structure_memory.db")

DEFAULT_SCANNER_INTERVAL = 120  # seconds — fixed scanner interval

TIMEFRAME_MAP = {
    "M1": 60,
    "M5": 300,
    "M15": 900,
    "M30": 1800,
    "H1": 3600,
    "H4": 14400,
    "D1": 86400,
}

SYMBOL_MAP = {
    "VOLATILITY10": "R_10",
    "VOLATILITY25": "R_25",
    "VOLATILITY50": "R_50",
    "VOLATILITY75": "R_75",
    "VOLATILITY100": "R_100",
    "BOOM500": "BOOM500",
    "BOOM1000": "BOOM1000",
    "CRASH500": "CRASH500",
    "CRASH1000": "CRASH1000",
    # Real Forex pairs via Deriv
    "EURUSD": "frxEURUSD",
    "GBPUSD": "frxGBPUSD",
    "USDJPY": "frxUSDJPY",
    "AUDUSD": "frxAUDUSD",
    "USDCAD": "frxUSDCAD",
    "EURGBP": "frxEURGBP",
    "GBPJPY": "frxGBPJPY",
    "EURJPY": "frxEURJPY",
}

DB_LOCK = threading.RLock()
SCAN_LOCK = threading.Lock()
SCANNER_THREAD = None
SCANNER_STOP = threading.Event()
SCANNER_LAST = {}

SCANNER_DIAGNOSTICS = {
    "last_check": {},
    "last_success": {},
    "last_error": {},
    "scan_count": {},
}

# ============================================================
# DATABASE / MEMORY
# ============================================================

def db():
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("""
        CREATE TABLE IF NOT EXISTS structures(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            structure_key TEXT UNIQUE,
            pair TEXT,
            timeframe TEXT,
            direction TEXT,
            state TEXT,
            a_json TEXT,
            b_json TEXT,
            c_json TEXT,
            d_json TEXT,
            e_json TEXT,
            f_json TEXT,
            fib50 REAL,
            valid INTEGER DEFAULT 0,
            telegram_sent INTEGER DEFAULT 0,
            created_epoch INTEGER,
            updated_epoch INTEGER,
            live_from_epoch INTEGER DEFAULT 0,
            discard_reason TEXT DEFAULT ''
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS telegram_users(
            chat_id TEXT PRIMARY KEY,
            username TEXT,
            active INTEGER DEFAULT 1,
            created_epoch INTEGER
        )
    """)
    # Ensure column exists
    cols = {r[1] for r in c.execute("PRAGMA table_info(structures)").fetchall()}
    if "discard_reason" not in cols:
        try:
            c.execute("ALTER TABLE structures ADD COLUMN discard_reason TEXT DEFAULT ''")
        except Exception:
            pass
    if "live_from_epoch" not in cols:
        try:
            c.execute("ALTER TABLE structures ADD COLUMN live_from_epoch INTEGER DEFAULT 0")
        except Exception:
            pass
    c.commit()
    return c


def j(x):
    return json.dumps(x, separators=(",", ":")) if x else None


def point(label, role, candle, price):
    return {
        "label": label,
        "role": role,
        "price": float(price),
        "epoch": int(candle["epoch"]),
        "open": float(candle["open"]),
        "high": float(candle["high"]),
        "low": float(candle["low"]),
        "close": float(candle["close"]),
    }


def structure_key(s):
    return f"{s['pair']}|{s['timeframe']}|{s['direction']}|{s['a']['epoch']}|{s['c']['epoch']}|{s['b']['epoch']}"


def fib50(b_price, e_price, direction):
    # Simple midpoint
    return (b_price + e_price) / 2.0


def save(s):
    now = int(time.time())
    k = structure_key(s)
    with DB_LOCK:
        c = db()
        c.execute("""
            INSERT INTO structures(
                structure_key, pair, timeframe, direction, state,
                a_json, b_json, c_json, d_json, e_json, f_json,
                fib50, valid, telegram_sent, discard_reason,
                created_epoch, updated_epoch, live_from_epoch
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(structure_key) DO UPDATE SET
                state=excluded.state,
                d_json=excluded.d_json,
                e_json=excluded.e_json,
                f_json=excluded.f_json,
                fib50=excluded.fib50,
                valid=excluded.valid,
                telegram_sent=excluded.telegram_sent,
                discard_reason=excluded.discard_reason,
                updated_epoch=excluded.updated_epoch,
                live_from_epoch=excluded.live_from_epoch
        """, (
            k, s["pair"], s["timeframe"], s["direction"], s["state"],
            j(s.get("a")), j(s.get("b")), j(s.get("c")), j(s.get("d")), j(s.get("e")), j(s.get("f")),
            s.get("fib50"), int(bool(s.get("valid"))), int(bool(s.get("telegram_sent"))),
            s.get("discard_reason", ""),
            s.get("created_epoch", s["a"]["epoch"] if s.get("a") else now),
            now,
            int(s.get("live_from_epoch", 0))
        ))
        c.commit()
        c.close()
    return k


def row_to_s(r):
    def p(name):
        return json.loads(r[name]) if r[name] else None
    return {
        "id": r["id"],
        "structure_key": r["structure_key"],
        "pair": r["pair"],
        "timeframe": r["timeframe"],
        "direction": r["direction"],
        "state": r["state"],
        "a": p("a_json"),
        "b": p("b_json"),
        "c": p("c_json"),
        "d": p("d_json"),
        "e": p("e_json"),
        "f": p("f_json"),
        "fib50": r["fib50"],
        "valid": bool(r["valid"]),
        "telegram_sent": bool(r["telegram_sent"]),
        "live_from_epoch": int(r["live_from_epoch"] or 0),
        "discard_reason": r["discard_reason"] or "",
    }


# ============================================================
# DERIV DATA
# ============================================================

def get_candles(symbol, granularity, count=500):
    result = []
    err = None
    done = threading.Event()
    nonlocal_err = [None]

    def on_message(ws, message):
        nonlocal result, err
        try:
            data = json.loads(message)
            if "candles" in data:
                result = data["candles"]
                done.set()
            elif "error" in data:
                err = data["error"].get("message", str(data["error"]))
                done.set()
        except Exception as e:
            err = str(e)
            done.set()

    def on_error(ws, error):
        nonlocal err
        err = str(error)
        done.set()

    def on_open(ws):
        try:
            ws.send(json.dumps({
                "ticks_history": symbol,
                "adjust_start_time": 1,
                "count": max(100, min(int(count), 1000)),
                "granularity": granularity,
                "style": "candles",
                "end": "latest",
            }))
        except Exception as e:
            nonlocal_err[0] = str(e)
            done.set()

    ws = websocket.WebSocketApp(
        DERIV_WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
    )
    worker = threading.Thread(target=ws.run_forever, name=f"deriv-ws-{symbol}-{granularity}", daemon=True)
    worker.start()
    done.wait(25)
    try:
        ws.close()
    except Exception:
        pass
    if nonlocal_err[0]:
        err = nonlocal_err[0]
    if err:
        print(f"[Deriv] {symbol} error: {err}")
    return sorted(result, key=lambda x: int(x.get("epoch", 0)))


def normalize(rows):
    return [
        {
            "epoch": int(x.get("epoch", 0)),
            "open": float(x["open"]),
            "high": float(x["high"]),
            "low": float(x["low"]),
            "close": float(x["close"]),
            "volume": float(x.get("volume", 0)),
        } for x in rows
    ]


def closed_candles(rows, granularity):
    now = int(time.time())
    return [x for x in rows if x["epoch"] + int(granularity) <= now]


# ============================================================
# STRUCTURE TOOLS
# ============================================================

def atrs(cs, period=14):
    tr = []
    for i, x in enumerate(cs):
        if i == 0:
            tr.append(x["high"] - x["low"])
        else:
            p = cs[i - 1]
            tr.append(max(x["high"] - x["low"], abs(x["high"] - p["close"]), abs(x["low"] - p["close"])))
    out = [0.0] * len(cs)
    for i in range(period, len(cs)):
        out[i] = sum(tr[i - period + 1:i + 1]) / period
    return out


def swings(cs, strength=3):
    s = max(1, int(strength))
    out = []
    for i in range(s, len(cs) - s):
        x = cs[i]
        is_low = all(x["low"] < y["low"] for y in cs[i - s:i]) and all(x["low"] <= y["low"] for y in cs[i + 1:i + s + 1])
        is_high = all(x["high"] > y["high"] for y in cs[i - s:i]) and all(x["high"] >= y["high"] for y in cs[i + 1:i + s + 1])
        if is_low:
            out.append(("L", i, x))
        if is_high:
            out.append(("H", i, x))
    return sorted(out, key=lambda z: z[1])


def significant_swings(cs, strength=3, min_atr_move=0.25):
    """
    Confirmed pivots only, with ATR noise filter.
    A pivot is only available after `strength` candles have fully closed after it.
    """
    raw = swings(cs, strength)
    av = atrs(cs)
    reduced = []
    for kind, i, candle in raw:
        price = candle["low"] if kind == "L" else candle["high"]
        if not reduced:
            reduced.append((kind, i, candle))
            continue
        last_kind, last_i, last_c = reduced[-1]
        last_price = last_c["low"] if last_kind == "L" else last_c["high"]
        # Same direction: keep extreme
        if kind == last_kind:
            replace = (kind == "L" and price < last_price) or (kind == "H" and price > last_price)
            if replace:
                reduced[-1] = (kind, i, candle)
            continue
        # Different direction: check ATR movement significance
        local_atr = av[i] if av[i] else (av[last_i] if av[last_i] else 0)
        movement = abs(price - last_price)
        if local_atr and movement < local_atr * min_atr_move:
            continue
        reduced.append((kind, i, candle))
    return reduced


# ============================================================
# DISCOVERY: A -> C -> B  (Corrected Order)
# ============================================================

def pivot_price(kind, candle):
    return candle["low"] if kind == "L" else candle["high"]


def discover_abc(
    cs,
    pair,
    tf,
    direction,
    strength,
    min_atr,
):
    """
    Bullish chart sequence: A(low) -> C(high) -> B(lower low)
    Bearish chart sequence: A(high) -> C(low) -> B(higher high)
    B is the structural extreme that must be confirmed before any BOS/D.
    """
    pivots = significant_swings(cs, strength=strength, min_atr_move=min_atr)
    av = atrs(cs)
    out = []

    if direction == "BULLISH":
        first_kind, middle_kind, final_kind = "L", "H", "L"
    else:
        first_kind, middle_kind, final_kind = "H", "L", "H"

    # Need at least 3 pivots: A, C, B
    for n in range(len(pivots) - 2):
        a_kind, ai, a_c = pivots[n]
        c_kind, ci, c_c = pivots[n + 1]
        b_kind, bi, b_c = pivots[n + 2]

        if a_kind != first_kind or c_kind != middle_kind or b_kind != final_kind:
            continue

        if direction == "BULLISH":
            a_price = pivot_price("L", a_c)
            c_price = pivot_price("H", c_c)
            b_price = pivot_price("L", b_c)
            # B must sweep below A (protected extreme low)
            if b_price >= a_price:
                continue
            sweep_size = a_price - b_price
        else:
            a_price = pivot_price("H", a_c)
            c_price = pivot_price("L", c_c)
            b_price = pivot_price("H", b_c)
            if b_price <= a_price:
                continue
            sweep_size = b_price - a_price

        # ATR filter on the sweep itself
        local_atr = av[bi] if av[bi] else av[ai]
        if local_atr and sweep_size < local_atr * min_atr:
            continue

        # C must represent a meaningful structural move from A
        c_move = abs(c_price - a_price)
        if local_atr and c_move < local_atr * min_atr:
            continue

        s = {
            "pair": pair,
            "timeframe": tf,
            "direction": direction,
            "a": point("A", "INITIAL_SWING", a_c, a_price),
            "b": point("B", "STRUCTURAL_EXTREME", b_c, b_price),
            "c": point("C", "PREVIOUS_STRUCTURE", c_c, c_price),
            "d": None,
            "e": None,
            "f": None,
            "fib50": None,
            "state": "WAITING_FOR_BOS",
            "valid": False,
            "telegram_sent": False,
            "live_from_epoch": 0,
        }
        out.append(s)

    return out


# ============================================================
# ADVANCE: D, E, F with invalidation / discard logic
# ============================================================

def candle_breaks_level(candle, level, direction, bos="body"):
    if direction == "BULLISH":
        return (candle["close"] > level) if bos == "body" else (candle["high"] > level)
    else:
        return (candle["close"] < level) if bos == "body" else (candle["low"] < level)


def discard_structure(s, reason, pair, tf):
    s["state"] = "DISCARDED"
    s["valid"] = False
    s["discard_reason"] = reason
    s["telegram_sent"] = False
    k = structure_key(s)
    with DB_LOCK:
        c = db()
        # Delete invalidated candidates so memory doesn't bloat
        # but keep complete ones; only delete non-complete.
        if s.get("state") != "COMPLETE":
            c.execute("DELETE FROM structures WHERE structure_key=?", (k,))
        else:
            # Update discard reason only if needed (shouldn't happen for complete)
            c.execute("UPDATE structures SET state=?, discard_reason=?, updated_epoch=? WHERE structure_key=?",
                      (s["state"], reason, int(time.time()), k))
        c.commit()
        c.close()
    return s


def cleanup_active_structures(pair, tf, direction):
    """
    For a given pair/timeframe/direction, keep only the newest active (non-complete, non-discarded) candidate.
    This prevents old A->C->B candidates from piling up when B is replaced.
    """
    with DB_LOCK:
        c = db()
        rows = c.execute("""
            SELECT id FROM structures
            WHERE pair=? AND timeframe=? AND direction=? AND state NOT IN ('COMPLETE','DISCARDED')
            ORDER BY updated_epoch DESC
        """, (pair, tf, direction)).fetchall()
        # Keep only the most recent active
        keep_ids = [r["id"] for r in rows[:1]]
        delete_ids = [r["id"] for r in rows[1:]]
        for did in delete_ids:
            c.execute("DELETE FROM structures WHERE id=?", (did,))
        c.commit()
        c.close()


def advance(
    s,
    cs,
    bos="body",
    exp_atr=0.5,
    disp_atr=1.0,
    allow_historical_f=False,
    swing_strength=3,
):
    ix = {x["epoch"]: i for i, x in enumerate(cs)}
    av = atrs(cs)
    pivots = significant_swings(cs, strength=swing_strength, min_atr_move=0.25)

    direction = s["direction"]
    bi = ix.get(s["b"]["epoch"])
    ci = ix.get(s["c"]["epoch"])
    if bi is None or ci is None:
        s = discard_structure(s, "pivot_index_missing", s["pair"], s["timeframe"])
        return s

    b_level = s["b"]["price"]
    c_level = s["c"]["price"]

    # ---------------------------------------------------------
    # INVALIDATION: A new structural extreme replaces B.
    # Bullish: new low lower than B -> old B becomes A, rebuild.
    # Bearish: new high higher than B -> old B becomes A, rebuild.
    # ---------------------------------------------------------
    for i in range(bi + 1, len(cs)):
        candle = cs[i]
        if direction == "BULLISH":
            if candle["low"] < b_level:
                # New protected extreme low found. This invalidates current B.
                s = discard_structure(s, "replaced_by_new_lower_low", s["pair"], s["timeframe"])
                return s
        else:
            if candle["high"] > b_level:
                s = discard_structure(s, "replaced_by_new_higher_high", s["pair"], s["timeframe"])
                return s

    # ---------------------------------------------------------
    # D = Confirmed BOS after C (using B as structural reference)
    # ---------------------------------------------------------
    if s["d"] is None:
        for i in range(bi + 1, len(cs)):
            candle = cs[i]
            broken = candle_breaks_level(candle, c_level, direction, bos)
            if broken:
                d_price = candle["high"] if direction == "BULLISH" else candle["low"]
                s["d"] = point("D", "BOS_BREAK", candle, d_price)
                s["state"] = "WAITING_FOR_E"
                break

    if s["d"] is None:
        return s

    di = ix.get(s["d"]["epoch"])
    if di is None:
        s = discard_structure(s, "d_index_missing", s["pair"], s["timeframe"])
        return s

    # ---------------------------------------------------------
    # E = Confirmed expansion swing after D
    # ---------------------------------------------------------
    if s["e"] is None:
        wanted = "H" if direction == "BULLISH" else "L"
        for kind, i, candle in pivots:
            if i <= di or kind != wanted:
                continue
            e_price = candle["high"] if direction == "BULLISH" else candle["low"]
            expansion = (e_price - c_level) if direction == "BULLISH" else (c_level - e_price)
            local_atr = av[i] if av[i] else av[di]
            if expansion <= 0:
                continue
            if local_atr and expansion < local_atr * exp_atr:
                continue
            s["e"] = point("E", "POST_BOS_EXPANSION", candle, e_price)
            s["fib50"] = fib50(s["b"]["price"], s["e"]["price"], direction)
            s["state"] = "WAITING_FOR_F"
            break

    if s["e"] is None:
        return s

    ei = ix.get(s["e"]["epoch"])
    if ei is None:
        s = discard_structure(s, "e_index_missing", s["pair"], s["timeframe"])
        return s

    mid = s["fib50"]
    live_from = int(s.get("live_from_epoch", 0))

    # ---------------------------------------------------------
    # F = Confirmed retracement swing after E beyond Fib 50%
    # ---------------------------------------------------------
    if s["f"] is None:
        wanted_f = "L" if direction == "BULLISH" else "H"
        for kind, i, candle in pivots:
            if i <= ei or kind != wanted_f:
                continue

            # Do not allow historical F for backfilled structures unless permitted
            if (not allow_historical_f) and live_from and candle["epoch"] <= live_from:
                continue

            f_price = candle["low"] if direction == "BULLISH" else candle["high"]

            reaches_50 = (f_price <= mid) if direction == "BULLISH" else (f_price >= mid)
            if not reaches_50:
                continue

            # F must not break the structural extreme B
            if direction == "BULLISH" and f_price < b_level:
                s = discard_structure(s, "retracement_broke_B", s["pair"], s["timeframe"])
                return s
            if direction == "BEARISH" and f_price > b_level:
                s = discard_structure(s, "retracement_broke_B", s["pair"], s["timeframe"])
                return s

            # Confirm F only if the candle range/body is significant relative to ATR
            rng = candle["high"] - candle["low"]
            body = abs(candle["close"] - candle["open"])
            local_atr_f = av[i]
            if not local_atr_f or rng < local_atr_f * 1.1:
                continue
            if body < local_atr_f * disp_atr:
                continue

            s["f"] = point("F", "FIB50_RETRACEMENT", candle, f_price)
            s["valid"] = True
            s["state"] = "COMPLETE"
            break

    return s


# ============================================================
# TELEGRAM
# ============================================================

def tg_send(chat_id, text):
    if not TELEGRAM_BOT_TOKEN:
        return False
    data = urllib.parse.urlencode({"chat_id": str(chat_id), "text": text, "parse_mode": "HTML"}).encode()
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    req = urllib.request.Request(url, data=data)
    try:
        urllib.request.urlopen(req, timeout=10).read()
        return True
    except Exception as e:
        print(f"[Telegram] {e}")
        return False


def signal_text(s):
    icon = "🟢" if s["direction"] == "BULLISH" else "🔴"
    lines = [
        f'{icon} {s["direction"]} A→F STRUCTURE CONFIRMED',
        "",
        f'Symbol: {s["pair"]}',
        f'Timeframe: {s["timeframe"]}',
        "",
    ]
    for k in ("a", "b", "c", "d", "e", "f"):
        p = s[k]
        if p:
            lines += [
                f'{k.upper()} — {p["role"]}',
                f'Price: {p["price"]}',
                f'Time: {time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(p["epoch"]))}',
                "",
            ]
    lines += [
        "━━━━━━━━━━━━━━━━━━",
        f'Fib 50%: {s["fib50"]}',
        "",
        "ENTRY: Manual decision",
        "TP / SL: Manual decision",
        "",
        "Engine does not provide trade execution. Pattern confirmed.",
    ]
    return "\n".join(lines)


def alert(s):
    k = structure_key(s)
    with DB_LOCK:
        c = db()
        r = c.execute("SELECT telegram_sent FROM structures WHERE structure_key=?", (k,)).fetchone()
        if r and r["telegram_sent"]:
            c.close()
            return 0
        users = c.execute("SELECT chat_id FROM telegram_users WHERE active=1").fetchall()
        c.close()
    sent = sum(1 for u in users if tg_send(u["chat_id"], signal_text(s)))
    if sent:
        with DB_LOCK:
            c = db()
            c.execute("UPDATE structures SET telegram_sent=1, updated_epoch=? WHERE structure_key=?", (int(time.time()), k))
            c.commit()
            c.close()
    return sent


# ============================================================
# DIAGNOSTICS
# ============================================================

def diag_key(pair, tf):
    return f"{pair}|{tf}"


def diag_started(pair, tf):
    key = diag_key(pair, tf)
    SCANNER_DIAGNOSTICS["last_check"][key] = {"status": "started", "time": int(time.time())}


def diag_success(pair, tf, result):
    key = diag_key(pair, tf)
    SCANNER_DIAGNOSTICS["last_success"][key] = {
        "time": int(time.time()),
        "latest_closed_epoch": result.get("latest_closed_epoch"),
        "completed_now": result.get("completed_now", 0),
        "active_structures": result.get("active_structures", 0),
        "skipped": result.get("skipped", False),
    }
    SCANNER_DIAGNOSTICS["scan_count"][key] = SCANNER_DIAGNOSTICS["scan_count"].get(key, 0) + 1
    SCANNER_DIAGNOSTICS["last_error"].pop(key, None)


def diag_error(pair, tf, error):
    key = diag_key(pair, tf)
    SCANNER_DIAGNOSTICS["last_error"][key] = {"time": int(time.time()), "error": str(error)}


# ============================================================
# SCAN ENGINE
# ============================================================

def run_scan(pair, tf, cs, strength, bos, min_atr, exp_atr, disp_atr):
    latest_epoch = cs[-1]["epoch"]
    mem = structures_for(pair, tf)
    keys = {x["structure_key"] for x in mem}

    # Process each direction independently
    for direction in ("BULLISH", "BEARISH"):
        # Discover new A-C-B candidates from full history
        candidates = discover_abc(cs, pair, tf, direction, strength, min_atr)

        for s in candidates:
            k = structure_key(s)
            if k in keys:
                continue
            # New candidate becomes active only after this scan point
            s["live_from_epoch"] = latest_epoch
            s = advance(s, cs, bos, exp_atr, disp_atr, allow_historical_f=False, swing_strength=strength)
            save(s)
            mem.append(s)
            keys.add(k)

            # Clean up older active candidates for this pair/tf/direction
            cleanup_active_structures(pair, tf, direction)

        # Advance existing structures
        completed = []
        new_mem = []
        for s in mem:
            old_state = s["state"]

            # Ensure live_from_epoch exists (backward compat)
            if not s.get("live_from_epoch"):
                s["live_from_epoch"] = latest_epoch
                save(s)

            # Skip already completed or discarded for memory management
            if old_state in ("COMPLETE", "DISCARDED"):
                new_mem.append(s)
                if old_state == "COMPLETE" and not s.get("telegram_sent"):
                    # Just in case, don't alert again
                    pass
                continue

            s = advance(s, cs, bos, exp_atr, disp_atr, allow_historical_f=False, swing_strength=strength)

            # If advance invalidated it, delete from DB for this non-complete state
            if s["state"] == "DISCARDED":
                discard_structure(s, s.get("discard_reason", "invalidated"), pair, tf)
                # Do not keep in active memory list
                continue

            save(s)
            new_mem.append(s)

            if old_state != "COMPLETE" and s["state"] == "COMPLETE":
                sent = alert(s)
                s["telegram_sent_now"] = bool(sent)
                completed.append(s)

        # Replace memory list
        # (Not strictly necessary for DB-backed logic, but keeps variables consistent)
        pass  # DB is source of truth

    # Count active structures for diagnostics
    with DB_LOCK:
        c = db()
        active_rows = c.execute("SELECT COUNT(*) FROM structures WHERE pair=? AND timeframe=? AND state NOT IN ('COMPLETE','DISCARDED')", (pair, tf)).fetchone()
        active_structures = active_rows[0] if active_rows else 0
        c.close()

    return {
        "pair": pair,
        "timeframe": tf,
        "scan_mode": "DUAL_DIRECTION",
        "memory_enabled": True,
        "chronological_locking": True,
        "historical_f_alerts_suppressed": True,
        "active_structures": active_structures,
        "stored_structures": len(structures_for(pair, tf)),
        "completed_now": len(completed),
        "signals": structures_for(pair, tf),
        "completed_signals": completed,
    }


# ============================================================
# SCANNER SETTINGS
# ============================================================

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
        "interval": max(5, int(os.environ.get("SCANNER_INTERVAL_SECONDS", str(DEFAULT_SCANNER_INTERVAL)))),
        "count": max(100, min(1000, int(os.environ.get("SCANNER_CANDLE_COUNT", "500")))),
        "strength": int(os.environ.get("SCANNER_STRENGTH", "3")),
        "bos": os.environ.get("SCANNER_BOS", "body").lower(),
        "min_atr": float(os.environ.get("SCANNER_MIN_ATR_MOVE", ".25")),
        "exp_atr": float(os.environ.get("SCANNER_MIN_EXPANSION_ATR", ".5")),
        "disp_atr": float(os.environ.get("SCANNER_DISPLACEMENT_ATR", "1.0")),
    }


# ============================================================
# CONTINUOUS LIVE SCANNER (120s interval)
# ============================================================

def continuous_scan_once(pair, tf, cfg):
    key = diag_key(pair, tf)
    diag_started(pair, tf)

    gran = TIMEFRAME_MAP[tf]
    raw = get_candles(SYMBOL_MAP.get(pair, pair), gran, cfg["count"])
    cs = closed_candles(normalize(raw), gran)

    if len(cs) < max(50, cfg["strength"] * 4):
        result = {
            "ok": False,
            "reason": "not_enough_closed_candles",
            "pair": pair,
            "timeframe": tf,
            "candles": len(cs),
        }
        diag_error(pair, tf, f"Not enough closed candles: {len(cs)}")
        return result

    latest = cs[-1]["epoch"]
    if SCANNER_LAST.get(key) == latest:
        result = {
            "ok": True,
            "pair": pair,
            "timeframe": tf,
            "skipped": True,
            "latest_closed_epoch": latest,
            "completed_now": 0,
            "active_structures": None,
        }
        SCANNER_DIAGNOSTICS["last_success"][key] = {
            "time": int(time.time()),
            "latest_closed_epoch": latest,
            "completed_now": 0,
            "active_structures": None,
            "skipped": True,
        }
        return result

    result = run_scan(
        pair, tf, cs,
        cfg["strength"], cfg["bos"], cfg["min_atr"], cfg["exp_atr"], cfg["disp_atr"]
    )
    SCANNER_LAST[key] = latest
    result.update({"ok": True, "skipped": False, "latest_closed_epoch": latest})
    diag_success(pair, tf, result)
    return result


def scanner_loop():
    print(f"[Scanner] Continuous live scanner started (interval={scanner_settings()['interval']}s)")
    while not SCANNER_STOP.is_set():
        cycle_started = time.time()
        try:
            cfg = scanner_settings()
            if not cfg["enabled"]:
                print("[Scanner] Disabled by SCANNER_ENABLED.")
            elif not SCAN_LOCK.acquire(blocking=False):
                print("[Scanner] Previous cycle still running; skipping.")
            else:
                try:
                    for pair in cfg["pairs"]:
                        if SCANNER_STOP.is_set():
                            break
                        for tf in cfg["timeframes"]:
                            if SCANNER_STOP.is_set():
                                break
                            try:
                                result = continuous_scan_once(pair, tf, cfg)
                                if result.get("completed_now", 0):
                                    print(f"[Scanner] {pair} {tf}: {result['completed_now']} new A→F signal(s)")
                                else:
                                    print(f"[Scanner] {pair} {tf}: OK (epoch={result.get('latest_closed_epoch')}, skipped={result.get('skipped', False)})")
                            except Exception as e:
                                diag_error(pair, tf, e)
                                print(f"[Scanner] {pair} {tf} ERROR: {e}")
                finally:
                    SCAN_LOCK.release()
        except Exception as e:
            print(f"[Scanner] OUTER LOOP ERROR: {e}")
        elapsed = time.time() - cycle_started
        cfg = scanner_settings()
        wait_seconds = max(1, cfg["interval"] - int(elapsed))
        print(f"[Scanner] Next cycle in {wait_seconds}s.")
        SCANNER_STOP.wait(wait_seconds)
    print("[Scanner] Continuous live scanner stopped.")


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


# ============================================================
# API ROUTES
# ============================================================

@app.route("/")
def home():
    return "Freedom Structure Scanner V5 (Corrected A→C→B→D→E→F) is running ✅"

@app.route("/health")
def health():
    cfg = scanner_settings()
    return jsonify({
        "ok": True,
        "engine": "Freedom Structure Scanner V5",
        "chronological_order": "A -> C -> B -> D -> E -> F",
        "memory_discard": True,
        "continuous_scanner": bool(SCANNER_THREAD and SCANNER_THREAD.is_alive()) and not SCANNER_STOP.is_set(),
        "scanner_interval_seconds": cfg["interval"],
    })

@app.route("/ohlc")
def ohlc():
    pair = request.args.get("pair", "").upper()
    tf = request.args.get("timeframe", "M15").upper()
    try:
        count = int(request.args.get("count", 500))
    except ValueError:
        return jsonify({"error": "Invalid count"}), 400
    if tf not in TIMEFRAME_MAP:
        return jsonify({"error": "Unsupported timeframe", "supported": list(TIMEFRAME_MAP)}), 400

    rows = get_candles(SYMBOL_MAP.get(pair, pair), TIMEFRAME_MAP[tf], count)
    return jsonify(closed_candles(normalize(rows), TIMEFRAME_MAP[tf]))

@app.route("/scan")
def scan():
    pair = request.args.get("pair", "").upper()
    tf = request.args.get("timeframe", "M15").upper()
    try:
        count = int(request.args.get("count", 500))
        strength = int(request.args.get("strength", 3))
        min_atr = float(request.args.get("min_atr_move", ".25"))
        exp_atr = float(request.args.get("min_expansion_atr", ".5"))
        disp_atr = float(request.args.get("displacement_atr", "1.0"))
    except ValueError:
        return jsonify({"error": "Invalid numeric parameter"}), 400
    bos = request.args.get("bos", "body").lower()
    if bos not in ("body", "wick"):
        bos = "body"
    if tf not in TIMEFRAME_MAP:
        return jsonify({"error": "Unsupported timeframe", "supported": list(TIMEFRAME_MAP)}), 400

    rows = get_candles(SYMBOL_MAP.get(pair, pair), TIMEFRAME_MAP[tf], count)
    cs = closed_candles(normalize(rows), TIMEFRAME_MAP[tf])
    if len(cs) < max(50, strength * 4):
        return jsonify({"error": "Not enough candles", "candles": len(cs)}), 502
    return jsonify(run_scan(pair, tf, cs, strength, bos, min_atr, exp_atr, disp_atr))

@app.route("/scanner/status")
def scanner_status():
    cfg = scanner_settings()
    return jsonify({
        "running": bool(SCANNER_THREAD and SCANNER_THREAD.is_alive()) and not SCANNER_STOP.is_set(),
        "enabled": cfg["enabled"],
        "pairs": cfg["pairs"],
        "timeframes": cfg["timeframes"],
        "interval_seconds": cfg["interval"],
        "last_processed": dict(SCANNER_LAST),
        "diagnostics": {
            "last_check": dict(SCANNER_DIAGNOSTICS["last_check"]),
            "last_success": dict(SCANNER_DIAGNOSTICS["last_success"]),
            "last_error": dict(SCANNER_DIAGNOSTICS["last_error"]),
            "scan_count": dict(SCANNER_DIAGNOSTICS["scan_count"]),
        }
    })

@app.route("/scanner/start", methods=["POST"])
def scanner_start():
    if ADMIN_KEY and request.headers.get("X-Admin-Key") != ADMIN_KEY:
        return jsonify({"error": "unauthorized"}), 401
    started = start_scanner()
    return jsonify({"ok": True, "started": started, "message": "Started" if started else "Already running"})

@app.route("/scanner/stop", methods=["POST"])
def scanner_stop():
    if not ADMIN_KEY or request.headers.get("X-Admin-Key") != ADMIN_KEY:
        return jsonify({"error": "unauthorized"}), 401
    stop_scanner()
    return jsonify({"ok": True, "message": "Stopping"})

@app.route("/structures")
def structures():
    pair = request.args.get("pair")
    tf = request.args.get("timeframe")

    sql = "SELECT * FROM structures"
    args = []
    where = []

    if pair:
        where.append("pair=?")
        args.append(pair.upper())

    if tf:
        where.append("timeframe=?")
        args.append(tf.upper())

    if where:
        sql += " WHERE " + " AND ".join(where)

    sql += " ORDER BY updated_epoch DESC LIMIT 300"

    with DB_LOCK:
        c = db()
        rows = c.execute(sql, args).fetchall()
        c.close()

    return jsonify([row_to_s(r) for r in rows])


@app.route("/telegram/register", methods=["POST"])
def register():
    d = request.get_json(force=True, silent=True) or {}
    chat = d.get("chat_id")

    if chat is None:
        return jsonify({"error": "chat_id required"}), 400

    with DB_LOCK:
        c = db()

        c.execute("""
            INSERT INTO telegram_users(
                chat_id,
                username,
                active,
                created_epoch
            )
            VALUES(?,?,1,?)
            ON CONFLICT(chat_id)
            DO UPDATE SET
                username=excluded.username,
                active=1
            """,
            (
                str(chat),
                d.get("username", ""),
                int(time.time()),
            ),
        )

        c.commit()
        c.close()

    return jsonify({
        "ok": True,
        "chat_id": str(chat),
    })


@app.route("/telegram/users")
def users():
    if ADMIN_KEY and request.headers.get("X-Admin-Key") != ADMIN_KEY:
        return jsonify({"error": "unauthorized"}), 401

    with DB_LOCK:
        c = db()

        rows = c.execute("""
            SELECT
                chat_id,
                username,
                active,
                created_epoch
            FROM telegram_users
            ORDER BY created_epoch DESC
            """
        ).fetchall()

        c.close()

    return jsonify([dict(r) for r in rows])


@app.route("/telegram/broadcast", methods=["POST"])
def broadcast():
    if ADMIN_KEY and request.headers.get("X-Admin-Key") != ADMIN_KEY:
        return jsonify({"error": "unauthorized"}), 401

    body = request.get_json(force=True, silent=True) or {}

    text = body.get("text", "")

    with DB_LOCK:
        c = db()

        users = c.execute("""
            SELECT chat_id
            FROM telegram_users
            WHERE active=1
            """
        ).fetchall()

        c.close()

    sent = sum(
        1
        for u in users
        if tg_send(
            u["chat_id"],
            text,
        )
    )

    return jsonify({
        "targeted": len(users),
        "sent": sent,
    })


@app.route("/admin/reset", methods=["POST"])
def reset():
    if ADMIN_KEY and request.headers.get("X-Admin-Key") != ADMIN_KEY:
        return jsonify({"error": "unauthorized"}), 401

    pair = request.args.get("pair")
    tf = request.args.get("timeframe")

    with DB_LOCK:
        c = db()

        if pair and tf:
            c.execute(
                """
                DELETE FROM structures
                WHERE pair=? AND timeframe=?
                """,
                (
                    pair.upper(),
                    tf.upper(),
                ),
            )

        elif pair:
            c.execute(
                """
                DELETE FROM structures
                WHERE pair=?
                """,
                (pair.upper(),),
            )

        else:
            c.execute(
                "DELETE FROM structures"
            )

        c.commit()
        c.close()

    return jsonify({
        "ok": True,
        "message": "Structure memory reset",
    })


# ============================================================
# AUTO START
# ============================================================

if (
    os.environ.get(
        "SCANNER_ENABLED",
        "true",
    ).lower()
    in ("1", "true", "yes", "on")
):
    start_scanner()


# ============================================================
# LOCAL DEVELOPMENT
# ============================================================

if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(
            os.environ.get(
                "PORT",
                8080,
            )
        ),
    )
