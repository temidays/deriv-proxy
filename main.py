import codecs
import encodings
import json
import os
import sqlite3
import threading
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

from flask import Flask, jsonify, request
import websocket

# ============================================================
# STARTUP
# ============================================================

try:
    codecs.register(encodings.search_function)
except Exception:
    pass

try:
    codecs.lookup("idna")
    print("[Startup] IDNA codec: OK")
except LookupError as exc:
    print(f"[Startup] IDNA codec ERROR: {exc}")

app = Flask(__name__)

# ============================================================
# CONFIGURATION
# ============================================================

DERIV_APP_ID = os.environ.get("DERIV_APP_ID", "YOUR_APP_ID_HERE")
DERIV_WS_URL = f"wss://ws.derivws.com/websockets/v3?app_id={DERIV_APP_ID}"
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
ADMIN_KEY = os.environ.get("ADMIN_KEY", "")

# Persistent disk path on Render.
# Set DB_PATH=/var/data/structure_memory.db in Render environment
DB_PATH = os.environ.get("DB_PATH", "structure_memory.db")

DEFAULT_SCANNER_INTERVAL = 120
DEFAULT_CANDLE_COUNT = 1000

TIMEFRAME_MAP = {
    "M1": 60,
    "M5": 300,
    "M15": 900,
    "M30": 1800,
    "H1": 3600,
    "H4": 14400,
    "D1": 86400,
}

# Curated set of supported symbols.
SYMBOL_MAP = {
    # Volatility indices (Deriv native)
    "R_10": "R_10",
    "R_25": "R_25",
    "R_50": "R_50",
    "R_75": "R_75",
    "R_100": "R_100",
    "V10": "R_10",
    "V25": "R_25",
    "V50": "R_50",
    "V75": "R_75",
    "V100": "R_100",
    "VOLATILITY10": "R_10",
    "VOLATILITY25": "R_25",
    "VOLATILITY50": "R_50",
    "VOLATILITY75": "R_75",
    "VOLATILITY100": "R_100",
    # Boom / Crash
    "BOOM500": "BOOM500",
    "BOOM1000": "BOOM1000",
    "CRASH500": "CRASH500",
    "CRASH1000": "CRASH1000",
    # Forex
    "EURUSD": "frxEURUSD",
    "GBPUSD": "frxGBPUSD",
    "USDJPY": "frxUSDJPY",
    "AUDUSD": "frxAUDUSD",
    "USDCAD": "frxUSDCAD",
    "USDCHF": "frxUSDCHF",
    "NZDUSD": "frxNZDUSD",
    "EURGBP": "frxEURGBP",
    "GBPJPY": "frxGBPJPY",
    "EURJPY": "frxEURJPY",
    "AUDJPY": "frxAUDJPY",
    "FRXEURUSD": "frxEURUSD",
    "FRXGBPUSD": "frxGBPUSD",
    "FRXUSDJPY": "frxUSDJPY",
    "FRXAUDUSD": "frxAUDUSD",
    "FRXUSDCAD": "frxUSDCAD",
    "FRXUSDCHF": "frxUSDCHF",
    "FRXNZDUSD": "frxNZDUSD",
    "FRXEURGBP": "frxEURGBP",
    "FRXGBPJPY": "frxGBPJPY",
    "FRXEURJPY": "frxEURJPY",
    "FRXAUDJPY": "frxAUDJPY",
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
# SYMBOL HELPERS
# ============================================================

def canonical_symbol(value):
    """
    Map user input to a supported Deriv symbol.

    Reject anything that is not in SYMBOL_MAP.
    """
    if value is None:
        return ""

    raw = str(value).strip()
    if not raw:
        return ""

    upper = raw.upper().strip()

    compact = (
        upper
        .replace(" ", "")
        .replace("/", "")
        .replace("-", "")
        .replace("_INDEX", "")
    )

    if upper in SYMBOL_MAP:
        return SYMBOL_MAP[upper]

    if compact in SYMBOL_MAP:
        return SYMBOL_MAP[compact]

    if compact.startswith("FRX") and len(compact) > 3:
        candidate = "frx" + compact[3:]
        if candidate in SYMBOL_MAP.values():
            return candidate

    return ""


def is_supported_symbol(value):
    return canonical_symbol(value) != ""


def unique_values(values):
    output = []
    seen = set()
    for value in values:
        if value and value not in seen:
            seen.add(value)
            output.append(value)
    return output


# ============================================================
# DATABASE
# ============================================================

def ensure_database_directory():
    absolute = os.path.abspath(DB_PATH)
    directory = os.path.dirname(absolute)
    if directory and not os.path.exists(directory):
        try:
            os.makedirs(directory, exist_ok=True)
            print(f"[DB] Created directory: {directory}")
        except Exception as exc:
            print(f"[DB] Could not create directory: {exc}")


def is_database_corrupt():
    """
    Quick corruption check.

    Returns True if the database is malformed.
    """
    if not os.path.exists(DB_PATH):
        return False

    try:
        check = sqlite3.connect(DB_PATH, timeout=5)
        check.row_factory = None
        check.execute("PRAGMA quick_check")
        check.close()
        return False
    except sqlite3.DatabaseError:
        return True
    except Exception:
        return False


def quarantine_corrupt_database():
    """
    Rename a corrupt database file so a fresh one can be created.
    """
    if not os.path.exists(DB_PATH):
        return

    corrupt_path = f"{DB_PATH}.corrupt.{int(time.time())}"

    for suffix in ("", "-wal", "-shm"):
        source = f"{DB_PATH}{suffix}"
        destination = f"{corrupt_path}{suffix}"

        if os.path.exists(source):
            try:
                os.rename(source, destination)
                print(f"[DB] Quarantined: {source} -> {destination}")
            except Exception as exc:
                print(f"[DB] Could not move {source}: {exc}")
                try:
                    os.remove(source)
                except Exception:
                    pass


def create_schema(connection):
    """
    Create all tables and indexes. Idempotent.
    """
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS structures(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            structure_key TEXT UNIQUE NOT NULL,
            pair TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            direction TEXT NOT NULL,
            state TEXT NOT NULL,
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
        """
    )

    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS telegram_users(
            chat_id TEXT PRIMARY KEY,
            username TEXT DEFAULT '',
            active INTEGER DEFAULT 1,
            created_epoch INTEGER
        )
        """
    )

    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS scanner_targets(
            pair TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            active INTEGER DEFAULT 1,
            created_epoch INTEGER,
            updated_epoch INTEGER,
            PRIMARY KEY(pair, timeframe)
        )
        """
    )

    # Migrate old schemas
    try:
        existing_columns = {
            row[1]
            for row in connection.execute(
                "PRAGMA table_info(structures)"
            ).fetchall()
        }

        if "live_from_epoch" not in existing_columns:
            connection.execute(
                "ALTER TABLE structures ADD COLUMN live_from_epoch INTEGER DEFAULT 0"
            )

        if "discard_reason" not in existing_columns:
            connection.execute(
                "ALTER TABLE structures ADD COLUMN discard_reason TEXT DEFAULT ''"
            )
    except sqlite3.DatabaseError as exc:
        print(f"[DB] Migration warning: {exc}")

    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_structures_pair_timeframe ON structures(pair, timeframe)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_structures_state ON structures(state)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_structures_updated ON structures(updated_epoch)"
    )

    connection.commit()


def db():
    """
    Open (or create) the SQLite database with WAL mode.

    Recovers automatically if the database is corrupt.
    """
    ensure_database_directory()

    # Recovery: if database is corrupt, quarantine it and start fresh.
    if is_database_corrupt():
        print("[DB] Corruption detected. Recovering...")
        quarantine_corrupt_database()

    connection = sqlite3.connect(
        DB_PATH,
        timeout=30,
        check_same_thread=False,
    )

    connection.row_factory = sqlite3.Row

    try:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA foreign_keys=ON")
    except sqlite3.DatabaseError as exc:
        print(f"[DB] PRAGMA warning: {exc}")

    create_schema(connection)

    return connection


# ============================================================
# TIME HELPERS
# ============================================================

def epoch_to_utc(epoch):
    return datetime.fromtimestamp(
        int(epoch), tz=timezone.utc
    ).strftime("%Y-%m-%d %H:%M:%S UTC")


def now_utc_string():
    return epoch_to_utc(int(time.time()))


# ============================================================
# STRUCTURE SERIALIZATION
# ============================================================

def j(value):
    if value is None:
        return None
    return json.dumps(value, separators=(",", ":"))


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


def structure_key(structure):
    return (
        f"{structure['pair']}|"
        f"{structure['timeframe']}|"
        f"{structure['direction']}|"
        f"{structure['a']['epoch']}|"
        f"{structure['c']['epoch']}|"
        f"{structure['b']['epoch']}"
    )


def fibonacci_level(b_price, e_price, ratio=0.5):
    ratio = max(0.0, min(1.0, float(ratio)))
    return float(b_price + ((e_price - b_price) * ratio))


def save(structure):
    now = int(time.time())
    key = structure_key(structure)

    values = {
        "structure_key": key,
        "pair": structure["pair"],
        "timeframe": structure["timeframe"],
        "direction": structure["direction"],
        "state": structure["state"],
        "a_json": j(structure.get("a")),
        "b_json": j(structure.get("b")),
        "c_json": j(structure.get("c")),
        "d_json": j(structure.get("d")),
        "e_json": j(structure.get("e")),
        "f_json": j(structure.get("f")),
        "fib50": structure.get("fib50"),
        "valid": int(bool(structure.get("valid"))),
        "telegram_sent": int(bool(structure.get("telegram_sent"))),
        "discard_reason": structure.get("discard_reason", ""),
        "created_epoch": int(structure.get("created_epoch", structure["a"]["epoch"])),
        "updated_epoch": now,
        "live_from_epoch": int(structure.get("live_from_epoch", 0)),
    }

    with DB_LOCK:
        connection = db()
        try:
            connection.execute(
                """
                INSERT INTO structures(
                    structure_key, pair, timeframe, direction, state,
                    a_json, b_json, c_json, d_json, e_json, f_json,
                    fib50, valid, telegram_sent, discard_reason,
                    created_epoch, updated_epoch, live_from_epoch
                )
                VALUES(
                    :structure_key, :pair, :timeframe, :direction, :state,
                    :a_json, :b_json, :c_json, :d_json, :e_json, :f_json,
                    :fib50, :valid, :telegram_sent, :discard_reason,
                    :created_epoch, :updated_epoch, :live_from_epoch
                )
                ON CONFLICT(structure_key) DO UPDATE SET
                    state=excluded.state,
                    a_json=excluded.a_json,
                    b_json=excluded.b_json,
                    c_json=excluded.c_json,
                    d_json=excluded.d_json,
                    e_json=excluded.e_json,
                    f_json=excluded.f_json,
                    fib50=excluded.fib50,
                    valid=excluded.valid,
                    telegram_sent=CASE
                        WHEN structures.telegram_sent=1 THEN 1
                        ELSE excluded.telegram_sent
                    END,
                    discard_reason=excluded.discard_reason,
                    updated_epoch=excluded.updated_epoch,
                    live_from_epoch=excluded.live_from_epoch
                """,
                values,
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
    return key


def row_to_s(row):
    def parse_point(column):
        value = row[column]
        if not value:
            return None
        return json.loads(value)

    structure = {
        "id": row["id"],
        "structure_key": row["structure_key"],
        "pair": row["pair"],
        "timeframe": row["timeframe"],
        "direction": row["direction"],
        "state": row["state"],
        "a": parse_point("a_json"),
        "b": parse_point("b_json"),
        "c": parse_point("c_json"),
        "d": parse_point("d_json"),
        "e": parse_point("e_json"),
        "f": parse_point("f_json"),
        "fib50": row["fib50"],
        "valid": bool(row["valid"]),
        "telegram_sent": bool(row["telegram_sent"]),
        "created_epoch": int(row["created_epoch"] or 0),
        "updated_epoch": int(row["updated_epoch"] or 0),
        "live_from_epoch": int(row["live_from_epoch"] or 0),
        "discard_reason": row["discard_reason"] or "",
    }

    b_point = structure.get("b")
    e_point = structure.get("e")
    if b_point and e_point:
        structure["fib"] = {
            "0": b_point["price"],
            "50": structure.get("fib50"),
            "100": e_point["price"],
        }
    else:
        structure["fib"] = None
    return structure


def structures_for(pair, timeframe):
    canonical_pair = canonical_symbol(pair)
    canonical_tf = str(timeframe).upper()
    with DB_LOCK:
        connection = db()
        try:
            rows = connection.execute(
                "SELECT * FROM structures WHERE pair=? AND timeframe=? ORDER BY created_epoch ASC",
                (canonical_pair, canonical_tf),
            ).fetchall()
        finally:
            connection.close()
    return [row_to_s(row) for row in rows]


def delete_structure(structure):
    key = structure_key(structure)
    with DB_LOCK:
        connection = db()
        try:
            connection.execute("DELETE FROM structures WHERE structure_key=?", (key,))
            connection.commit()
        finally:
            connection.close()


# ============================================================
# SCANNER TARGET MEMORY
# ============================================================

def database_scanner_targets():
    with DB_LOCK:
        connection = db()
        try:
            rows = connection.execute(
                "SELECT pair, timeframe FROM scanner_targets WHERE active=1 ORDER BY pair, timeframe"
            ).fetchall()
        finally:
            connection.close()
    return [{"pair": row["pair"], "timeframe": row["timeframe"]} for row in rows]


def replace_scanner_targets(pairs, timeframes):
    canonical_pairs = unique_values(
        [canonical_symbol(p) for p in pairs if is_supported_symbol(p)]
    )
    canonical_timeframes = unique_values(
        [tf.upper() for tf in timeframes if tf.upper() in TIMEFRAME_MAP]
    )

    if not canonical_pairs:
        raise ValueError("At least one valid pair is required")
    if not canonical_timeframes:
        raise ValueError("At least one supported timeframe is required")

    targets = [
        {"pair": pair, "timeframe": tf}
        for pair in canonical_pairs
        for tf in canonical_timeframes
    ]
    if len(targets) > 50:
        raise ValueError("Maximum 50 pair/timeframe combinations")

    now = int(time.time())
    with DB_LOCK:
        connection = db()
        try:
            connection.execute("DELETE FROM scanner_targets")
            for target in targets:
                connection.execute(
                    "INSERT INTO scanner_targets(pair, timeframe, active, created_epoch, updated_epoch) VALUES(?,?,1,?,?)",
                    (target["pair"], target["timeframe"], now, now),
                )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
    return targets


# ============================================================
# DERIV DATA
# ============================================================

def get_candles(symbol, granularity, count=500):
    deriv_symbol = canonical_symbol(symbol)
    if not deriv_symbol:
        raise ValueError(f"Unsupported symbol: {symbol}")

    result = []
    error_message = None
    done = threading.Event()
    open_error = [None]

    def on_message(ws, message):
        nonlocal result, error_message
        try:
            data = json.loads(message)
            if "candles" in data:
                result = data["candles"]
                done.set()
            elif "error" in data:
                error_message = data["error"].get("message", str(data["error"]))
                done.set()
        except Exception as exc:
            error_message = str(exc)
            done.set()

    def on_error(ws, error):
        nonlocal error_message
        error_message = str(error)
        done.set()

    def on_open(ws):
        try:
            ws.send(json.dumps({
                "ticks_history": deriv_symbol,
                "adjust_start_time": 1,
                "count": max(100, min(int(count), 1000)),
                "granularity": int(granularity),
                "style": "candles",
                "end": "latest",
            }))
        except Exception as exc:
            open_error[0] = str(exc)
            done.set()

    socket = websocket.WebSocketApp(
        DERIV_WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
    )

    worker = threading.Thread(
        target=socket.run_forever,
        name=f"deriv-ws-{deriv_symbol}-{granularity}",
        daemon=True,
    )
    worker.start()
    completed = done.wait(30)

    try:
        socket.close()
    except Exception:
        pass

    if open_error[0]:
        error_message = open_error[0]
    if not completed and not result:
        raise TimeoutError(f"Deriv request timed out for {deriv_symbol}")
    if error_message:
        raise RuntimeError(f"Deriv error for {deriv_symbol}: {error_message}")
    if not result:
        raise RuntimeError(f"Deriv returned no candles for {deriv_symbol}")

    return sorted(result, key=lambda item: int(item.get("epoch", 0)))


def normalize(rows):
    return [
        {
            "epoch": int(row.get("epoch", 0)),
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "volume": float(row.get("volume", 0)),
        }
        for row in rows
    ]


def closed_candles(rows, granularity):
    now = int(time.time())
    return [
        candle for candle in rows
        if candle["epoch"] + int(granularity) <= now
    ]


# ============================================================
# TECHNICAL TOOLS
# ============================================================

def atrs(candles, period=14):
    if not candles:
        return []

    true_ranges = []
    for index, candle in enumerate(candles):
        if index == 0:
            true_range = candle["high"] - candle["low"]
        else:
            previous = candles[index - 1]
            true_range = max(
                candle["high"] - candle["low"],
                abs(candle["high"] - previous["close"]),
                abs(candle["low"] - previous["close"]),
            )
        true_ranges.append(true_range)

    output = [0.0] * len(candles)
    first_atr_index = max(0, int(period) - 1)
    for index in range(first_atr_index, len(candles)):
        start = max(0, index - int(period) + 1)
        values = true_ranges[start:index + 1]
        output[index] = sum(values) / len(values)
    return output


def swings(candles, strength=3):
    strength = max(1, int(strength))
    output = []
    for index in range(strength, len(candles) - strength):
        candle = candles[index]
        left = candles[index - strength:index]
        right = candles[index + 1:index + strength + 1]

        is_low = (
            all(candle["low"] < item["low"] for item in left) and
            all(candle["low"] <= item["low"] for item in right)
        )
        is_high = (
            all(candle["high"] > item["high"] for item in left) and
            all(candle["high"] >= item["high"] for item in right)
        )

        if is_low:
            output.append(("L", index, candle))
        if is_high:
            output.append(("H", index, candle))

    return sorted(output, key=lambda item: item[1])


def pivot_price(kind, candle):
    return candle["low"] if kind == "L" else candle["high"]


def significant_swings(candles, strength=3, min_atr_move=0.25):
    raw = swings(candles, strength)
    atr_values = atrs(candles)
    reduced = []

    for kind, index, candle in raw:
        price = pivot_price(kind, candle)
        if not reduced:
            reduced.append((kind, index, candle))
            continue

        previous_kind, previous_index, previous_candle = reduced[-1]
        previous_price = pivot_price(previous_kind, previous_candle)

        if kind == previous_kind:
            more_extreme = (
                (kind == "L" and price < previous_price) or
                (kind == "H" and price > previous_price)
            )
            if more_extreme:
                reduced[-1] = (kind, index, candle)
            continue

        local_atr = atr_values[index] or atr_values[previous_index]
        movement = abs(price - previous_price)
        if local_atr and movement < local_atr * float(min_atr_move):
            continue
        reduced.append((kind, index, candle))
    return reduced


# ============================================================
# A -> C -> B DISCOVERY
# ============================================================

def discover_abc(candles, pair, timeframe, direction, strength, min_atr, min_bars=1):
    pivots = significant_swings(candles, strength=strength, min_atr_move=min_atr)
    atr_values = atrs(candles)
    output = []

    if direction == "BULLISH":
        expected = ("L", "H", "L")
    else:
        expected = ("H", "L", "H")

    for pivot_index in range(len(pivots) - 2):
        a_kind, a_index, a_candle = pivots[pivot_index]
        c_kind, c_index, c_candle = pivots[pivot_index + 1]
        b_kind, b_index, b_candle = pivots[pivot_index + 2]

        if (a_kind, c_kind, b_kind) != expected:
            continue
        if c_index - a_index < int(min_bars):
            continue
        if b_index - c_index < int(min_bars):
            continue

        a_price = pivot_price(a_kind, a_candle)
        c_price = pivot_price(c_kind, c_candle)
        b_price = pivot_price(b_kind, b_candle)

        if direction == "BULLISH":
            if b_price >= a_price:
                continue
            sweep_size = a_price - b_price
        else:
            if b_price <= a_price:
                continue
            sweep_size = b_price - a_price

        local_atr = atr_values[b_index] or atr_values[a_index]
        if local_atr and sweep_size < local_atr * float(min_atr):
            continue

        c_displacement = abs(c_price - a_price)
        if local_atr and c_displacement < local_atr * float(min_atr):
            continue

        structure = {
            "pair": canonical_symbol(pair),
            "timeframe": str(timeframe).upper(),
            "direction": direction,
            "a": point("A", "INITIAL_SWING", a_candle, a_price),
            "b": point("B", "STRUCTURAL_EXTREME", b_candle, b_price),
            "c": point("C", "PREVIOUS_STRUCTURE", c_candle, c_price),
            "d": None, "e": None, "f": None,
            "fib50": None,
            "state": "WAITING_FOR_BOS",
            "valid": False,
            "telegram_sent": False,
            "discard_reason": "",
            "live_from_epoch": 0,
        }
        output.append(structure)
    return output


# ============================================================
# STRUCTURE ADVANCEMENT
# ============================================================

def candle_breaks_level(candle, level, direction, bos_mode="body"):
    if direction == "BULLISH":
        if bos_mode == "wick":
            return candle["high"] > level
        return candle["close"] > level
    if bos_mode == "wick":
        return candle["low"] < level
    return candle["close"] < level


def mark_discarded(structure, reason):
    structure["state"] = "DISCARDED"
    structure["valid"] = False
    structure["discard_reason"] = reason
    return structure


def advance(structure, candles, bos_mode="body", expansion_atr=0.5, displacement_atr=1.0,
            allow_historical_f=False, swing_strength=3, min_atr_move=0.25, fib_ratio=0.5):

    if structure.get("state") == "COMPLETE":
        return structure

    index_by_epoch = {candle["epoch"]: index for index, candle in enumerate(candles)}
    atr_values = atrs(candles)
    pivots = significant_swings(candles, strength=swing_strength, min_atr_move=min_atr_move)

    direction = structure["direction"]
    b_index = index_by_epoch.get(structure["b"]["epoch"])
    c_index = index_by_epoch.get(structure["c"]["epoch"])

    if b_index is None or c_index is None:
        return mark_discarded(structure, "pivot_missing_from_candle_window")

    b_level = structure["b"]["price"]
    c_level = structure["c"]["price"]

    if structure.get("d") is None:
        replacement_kind = "L" if direction == "BULLISH" else "H"
        for kind, index, candle in pivots:
            if index <= b_index:
                continue
            if kind != replacement_kind:
                continue
            replacement_price = pivot_price(kind, candle)
            replaced = (
                replacement_price < b_level if direction == "BULLISH"
                else replacement_price > b_level
            )
            if replaced:
                return mark_discarded(
                    structure,
                    "confirmed_new_lower_low_replaced_B" if direction == "BULLISH"
                    else "confirmed_new_higher_high_replaced_B"
                )

    if structure.get("d") is None:
        for index in range(b_index + 1, len(candles)):
            candle = candles[index]
            if candle_breaks_level(candle, c_level, direction, bos_mode):
                d_price = candle["high"] if direction == "BULLISH" else candle["low"]
                structure["d"] = point(
                    "D",
                    "BODY_BOS" if bos_mode == "body" else "WICK_BOS",
                    candle,
                    d_price
                )
                structure["state"] = "WAITING_FOR_E"
                break

    if structure.get("d") is None:
        return structure

    d_index = index_by_epoch.get(structure["d"]["epoch"])
    if d_index is None:
        return mark_discarded(structure, "D_missing_from_candle_window")

    for index in range(b_index + 1, len(candles)):
        candle = candles[index]
        broke_b = (
            candle["low"] < b_level if direction == "BULLISH"
            else candle["high"] > b_level
        )
        if broke_b:
            return mark_discarded(structure, "protected_B_was_broken")

    if structure.get("e") is None:
        wanted_kind = "H" if direction == "BULLISH" else "L"
        for kind, index, candle in pivots:
            if index <= d_index:
                continue
            if kind != wanted_kind:
                continue
            e_price = pivot_price(kind, candle)
            expansion = (e_price - c_level) if direction == "BULLISH" else (c_level - e_price)
            local_atr = atr_values[index] or atr_values[d_index]
            if expansion <= 0:
                continue
            if local_atr and expansion < local_atr * float(expansion_atr):
                continue
            structure["e"] = point("E", "POST_BOS_EXPANSION", candle, e_price)
            structure["fib50"] = fibonacci_level(structure["b"]["price"], structure["e"]["price"], fib_ratio)
            structure["state"] = "WAITING_FOR_F"
            break

    if structure.get("e") is None:
        return structure

    e_index = index_by_epoch.get(structure["e"]["epoch"])
    if e_index is None:
        return mark_discarded(structure, "E_missing_from_candle_window")

    fib_threshold = structure["fib50"]
    live_from = int(structure.get("live_from_epoch", 0))

    if structure.get("f") is None:
        wanted_kind = "L" if direction == "BULLISH" else "H"
        for kind, index, candle in pivots:
            if index <= e_index:
                continue
            if kind != wanted_kind:
                continue
            if not allow_historical_f and live_from and candle["epoch"] <= live_from:
                continue
            f_price = pivot_price(kind, candle)
            reaches_threshold = (f_price <= fib_threshold) if direction == "BULLISH" else (f_price >= fib_threshold)
            if not reaches_threshold:
                continue
            invalidates_b = (f_price < b_level) if direction == "BULLISH" else (f_price > b_level)
            if invalidates_b:
                return mark_discarded(structure, "retracement_invalidated_B")
            retracement_distance = abs(structure["e"]["price"] - f_price)
            local_atr = atr_values[index]
            if local_atr and retracement_distance < local_atr * float(displacement_atr):
                continue
            structure["f"] = point("F", "FIB_RETRACEMENT", candle, f_price)
            structure["valid"] = True
            structure["state"] = "COMPLETE"
            structure["discard_reason"] = ""
            break

    return structure


# ============================================================
# TELEGRAM
# ============================================================

def tg_send(chat_id, text):
    if not TELEGRAM_BOT_TOKEN:
        return False, "TELEGRAM_BOT_TOKEN not configured"

    data = urllib.parse.urlencode({
        "chat_id": str(chat_id),
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": "true",
    }).encode()

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    telegram_request = urllib.request.Request(url, data=data)

    try:
        with urllib.request.urlopen(telegram_request, timeout=15) as response:
            body = response.read().decode("utf-8", errors="ignore")
            payload = json.loads(body)
            if not payload.get("ok"):
                return False, payload.get("description", "Telegram API error")
            return True, "ok"
    except urllib.error.HTTPError as exc:
        try:
            detail = exc.read().decode("utf-8", errors="ignore")
            payload = json.loads(detail)
            return False, payload.get("description", f"HTTP {exc.code}")
        except Exception:
            return False, f"HTTP {exc.code}"
    except Exception as exc:
        return False, str(exc)


def signal_text(structure):
    icon = "🟢" if structure["direction"] == "BULLISH" else "🔴"

    lines = [
        f"{icon} <b>{structure['direction']} A→F STRUCTURE CONFIRMED</b>",
        "",
        f"Symbol: <b>{structure['pair']}</b>",
        f"Timeframe: <b>{structure['timeframe']}</b>",
        f"Detected: <b>{now_utc_string()}</b>",
        "",
        "━━━━━━━━━━━━━━━━━━",
        "",
    ]

    for key in ("a", "c", "b", "d", "e", "f"):
        point_value = structure.get(key)
        if not point_value:
            continue
        lines.extend([
            f"<b>{point_value['label']}</b> — {point_value['role']}",
            f"Price: {point_value['price']}",
            f"Candle time: {epoch_to_utc(point_value['epoch'])}",
            "",
        ])

    lines.extend([
        "━━━━━━━━━━━━━━━━━━",
        f"Fibonacci 50%: {structure['fib50']}",
        "",
        "ENTRY: Manual",
        "TP: Manual",
        "SL: Manual",
        "",
        "Scanner confirmed the structure. It does not execute trades.",
    ])
    return "\n".join(lines)


def alert(structure):
    key = structure_key(structure)
    with DB_LOCK:
        connection = db()
        try:
            existing = connection.execute(
                "SELECT telegram_sent FROM structures WHERE structure_key=?", (key,)
            ).fetchone()
            if existing and existing["telegram_sent"]:
                return 0
            users = connection.execute(
                "SELECT chat_id FROM telegram_users WHERE active=1"
            ).fetchall()
        finally:
            connection.close()

    if not users:
        print("[Telegram] No active users registered")
        return 0

    message = signal_text(structure)
    sent = 0
    for user in users:
        ok, _detail = tg_send(user["chat_id"], message)
        if ok:
            sent += 1

    if sent:
        with DB_LOCK:
            connection = db()
            try:
                connection.execute(
                    "UPDATE structures SET telegram_sent=1, updated_epoch=? WHERE structure_key=?",
                    (int(time.time()), key)
                )
                connection.commit()
            finally:
                connection.close()
    return sent


# ============================================================
# DIAGNOSTICS
# ============================================================

def diagnostic_key(pair, timeframe):
    return f"{canonical_symbol(pair)}|{str(timeframe).upper()}"


def diagnostic_started(pair, timeframe):
    key = diagnostic_key(pair, timeframe)
    SCANNER_DIAGNOSTICS["last_check"][key] = {
        "status": "started",
        "time": int(time.time()),
        "time_utc": now_utc_string(),
    }


def diagnostic_success(pair, timeframe, result):
    key = diagnostic_key(pair, timeframe)
    SCANNER_DIAGNOSTICS["last_success"][key] = {
        "time": int(time.time()),
        "time_utc": now_utc_string(),
        "latest_closed_epoch": result.get("latest_closed_epoch"),
        "completed_now": result.get("completed_now", 0),
        "active_structures": result.get("active_structures", 0),
        "skipped": result.get("skipped", False),
    }
    SCANNER_DIAGNOSTICS["scan_count"][key] = SCANNER_DIAGNOSTICS["scan_count"].get(key, 0) + 1
    SCANNER_DIAGNOSTICS["last_error"].pop(key, None)


def diagnostic_error(pair, timeframe, error):
    key = diagnostic_key(pair, timeframe)
    SCANNER_DIAGNOSTICS["last_error"][key] = {
        "time": int(time.time()),
        "time_utc": now_utc_string(),
        "error": str(error),
    }


# ============================================================
# SCAN ENGINE
# ============================================================

def run_scan(pair, timeframe, candles, strength, bos_mode, min_atr, expansion_atr,
             displacement_atr, min_bars=1, fib_ratio=0.5):

    pair = canonical_symbol(pair)
    if not pair:
        raise ValueError("Unsupported pair")

    timeframe = str(timeframe).upper()

    latest_epoch = candles[-1]["epoch"]
    completed_now = []

    memory = structures_for(pair, timeframe)

    for direction in ("BULLISH", "BEARISH"):
        active = [
            s for s in memory
            if s["direction"] == direction and s["state"] not in ("COMPLETE", "DISCARDED")
        ]
        active.sort(key=lambda s: s["b"]["epoch"], reverse=True)
        for obsolete in active[1:]:
            delete_structure(obsolete)

    memory = structures_for(pair, timeframe)

    for structure in list(memory):
        if structure["state"] in ("COMPLETE", "DISCARDED"):
            continue
        previous_state = structure["state"]
        if not structure.get("live_from_epoch"):
            structure["live_from_epoch"] = latest_epoch
        structure = advance(
            structure=structure, candles=candles, bos_mode=bos_mode,
            expansion_atr=expansion_atr, displacement_atr=displacement_atr,
            allow_historical_f=False, swing_strength=strength,
            min_atr_move=min_atr, fib_ratio=fib_ratio
        )
        if structure["state"] == "DISCARDED":
            delete_structure(structure)
            continue
        save(structure)
        if previous_state != "COMPLETE" and structure["state"] == "COMPLETE":
            sent = alert(structure)
            structure["telegram_sent_now"] = bool(sent)
            completed_now.append(structure)

    memory = structures_for(pair, timeframe)
    known_keys = {s["structure_key"] for s in memory}

    for direction in ("BULLISH", "BEARISH"):
        has_active = any(
            s["direction"] == direction and s["state"] not in ("COMPLETE", "DISCARDED")
            for s in memory
        )
        if has_active:
            continue
        candidates = discover_abc(
            candles=candles, pair=pair, timeframe=timeframe,
            direction=direction, strength=strength, min_atr=min_atr, min_bars=min_bars
        )
        candidates.sort(key=lambda s: s["b"]["epoch"], reverse=True)
        for candidate in candidates:
            key = structure_key(candidate)
            if key in known_keys:
                continue
            candidate["live_from_epoch"] = latest_epoch
            candidate = advance(
                structure=candidate, candles=candles, bos_mode=bos_mode,
                expansion_atr=expansion_atr, displacement_atr=displacement_atr,
                allow_historical_f=False, swing_strength=strength,
                min_atr_move=min_atr, fib_ratio=fib_ratio
            )
            if candidate["state"] == "DISCARDED":
                continue
            save(candidate)
            known_keys.add(key)
            memory.append(candidate)
            break

    final_signals = structures_for(pair, timeframe)
    active_count = sum(1 for s in final_signals if s["state"] not in ("COMPLETE", "DISCARDED"))

    return {
        "pair": pair,
        "timeframe": timeframe,
        "scan_mode": "DUAL_DIRECTION",
        "memory_enabled": True,
        "chronological_order": "A -> C -> B -> D -> E -> F",
        "chronological_locking": True,
        "historical_f_alerts_suppressed": True,
        "active_structures": active_count,
        "stored_structures": len(final_signals),
        "completed_now": len(completed_now),
        "signals": final_signals,
        "completed_signals": completed_now,
    }


# ============================================================
# SCANNER SETTINGS
# ============================================================

def environment_list(name, default):
    raw = os.environ.get(name, default)
    return [v.strip() for v in raw.split(",") if v.strip()]


def scanner_settings():
    environment_pairs = unique_values([
        canonical_symbol(p) for p in environment_list("SCANNER_PAIRS", "R_100")
    ])
    environment_timeframes = unique_values([
        tf.upper() for tf in environment_list("SCANNER_TIMEFRAMES", "M15")
        if tf.upper() in TIMEFRAME_MAP
    ])
    bos_mode = os.environ.get("SCANNER_BOS", "body").lower()
    if bos_mode not in ("body", "wick"):
        bos_mode = "body"

    return {
        "enabled": os.environ.get("SCANNER_ENABLED", "true").lower() in ("1", "true", "yes", "on"),
        "pairs": environment_pairs or ["R_100"],
        "timeframes": environment_timeframes or ["M15"],
        "interval": max(5, int(os.environ.get("SCANNER_INTERVAL_SECONDS", str(DEFAULT_SCANNER_INTERVAL)))),
        "count": max(100, min(1000, int(os.environ.get("SCANNER_CANDLE_COUNT", str(DEFAULT_CANDLE_COUNT))))),
        "strength": max(1, int(os.environ.get("SCANNER_STRENGTH", "3"))),
        "bos": bos_mode,
        "min_atr": max(0.0, float(os.environ.get("SCANNER_MIN_ATR_MOVE", "0.25"))),
        "expansion_atr": max(0.0, float(os.environ.get("SCANNER_MIN_EXPANSION_ATR", "0.5"))),
        "displacement_atr": max(0.0, float(os.environ.get("SCANNER_DISPLACEMENT_ATR", "1.0"))),
        "min_bars": max(1, int(os.environ.get("SCANNER_MIN_BARS", "1"))),
        "fib_ratio": max(0.0, min(1.0, float(os.environ.get("SCANNER_FIB_THRESHOLD", "0.5")))),
    }


def effective_scanner_targets(config=None):
    stored_targets = database_scanner_targets()
    if stored_targets:
        return stored_targets
    config = config or scanner_settings()
    return [
        {"pair": pair, "timeframe": tf}
        for pair in config["pairs"]
        for tf in config["timeframes"]
    ]


# ============================================================
# CONTINUOUS SCANNER
# ============================================================

def continuous_scan_once(pair, timeframe, config):
    pair = canonical_symbol(pair)
    timeframe = str(timeframe).upper()
    key = diagnostic_key(pair, timeframe)
    diagnostic_started(pair, timeframe)

    if timeframe not in TIMEFRAME_MAP:
        raise ValueError(f"Unsupported timeframe: {timeframe}")

    granularity = TIMEFRAME_MAP[timeframe]
    raw = get_candles(pair, granularity, config["count"])
    candles = closed_candles(normalize(raw), granularity)

    minimum_required = max(50, config["strength"] * 4)
    if len(candles) < minimum_required:
        result = {
            "ok": False, "reason": "not_enough_closed_candles",
            "pair": pair, "timeframe": timeframe, "candles": len(candles),
        }
        diagnostic_error(pair, timeframe, f"Not enough closed candles: {len(candles)}")
        return result

    latest_epoch = candles[-1]["epoch"]
    if SCANNER_LAST.get(key) == latest_epoch:
        result = {
            "ok": True, "pair": pair, "timeframe": timeframe, "skipped": True,
            "latest_closed_epoch": latest_epoch, "completed_now": 0, "active_structures": None,
        }
        diagnostic_success(pair, timeframe, result)
        return result

    result = run_scan(
        pair=pair, timeframe=timeframe, candles=candles,
        strength=config["strength"], bos_mode=config["bos"],
        min_atr=config["min_atr"], expansion_atr=config["expansion_atr"],
        displacement_atr=config["displacement_atr"], min_bars=config["min_bars"],
        fib_ratio=config["fib_ratio"],
    )
    SCANNER_LAST[key] = latest_epoch
    result.update({"ok": True, "skipped": False, "latest_closed_epoch": latest_epoch})
    diagnostic_success(pair, timeframe, result)
    return result


def scanner_loop():
    initial_config = scanner_settings()
    print(f"[Scanner] Continuous live scanner started (interval={initial_config['interval']}s)")

    while not SCANNER_STOP.is_set():
        cycle_started = time.time()
        try:
            config = scanner_settings()
            if not config["enabled"]:
                print("[Scanner] Disabled by SCANNER_ENABLED.")
            elif not SCAN_LOCK.acquire(blocking=False):
                print("[Scanner] Previous cycle still running; skipping.")
            else:
                try:
                    targets = effective_scanner_targets(config)
                    for target in targets:
                        if SCANNER_STOP.is_set():
                            break
                        pair = target["pair"]
                        timeframe = target["timeframe"]
                        try:
                            result = continuous_scan_once(pair, timeframe, config)
                            if result.get("completed_now", 0):
                                print(f"[Scanner] {pair} {timeframe}: {result['completed_now']} new signal(s)")
                            else:
                                print(f"[Scanner] {pair} {timeframe}: OK (epoch={result.get('latest_closed_epoch')}, skipped={result.get('skipped', False)})")
                        except Exception as exc:
                            diagnostic_error(pair, timeframe, exc)
                            print(f"[Scanner] {pair} {timeframe} ERROR: {exc}")
                finally:
                    SCAN_LOCK.release()
        except Exception as exc:
            print(f"[Scanner] OUTER LOOP ERROR: {exc}")

        elapsed = time.time() - cycle_started
        config = scanner_settings()
        wait_seconds = max(1, config["interval"] - int(elapsed))
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
# ADMIN HELPERS
# ============================================================

def optional_admin_authorized():
    if not ADMIN_KEY:
        return True
    return request.headers.get("X-Admin-Key") == ADMIN_KEY


def strict_admin_authorized():
    if not ADMIN_KEY:
        return True
    return request.headers.get("X-Admin-Key") == ADMIN_KEY


# ============================================================
# API ROUTES
# ============================================================

@app.get("/")
def home_api():
    return "Freedom Structure Scanner is running. Order: A → C → B → D → E → F ✅"


@app.get("/health")
def health_api():
    config = scanner_settings()
    try:
        targets = effective_scanner_targets(config)
    except Exception:
        targets = []

    try:
        with DB_LOCK:
            connection = db()
            try:
                telegram_users = connection.execute(
                    "SELECT COUNT(*) FROM telegram_users WHERE active=1"
                ).fetchone()[0]
                stored_structures = connection.execute(
                    "SELECT COUNT(*) FROM structures"
                ).fetchone()[0]
            finally:
                connection.close()
    except Exception as exc:
        print(f"[Health] database read failed: {exc}")
        telegram_users = 0
        stored_structures = 0

    return jsonify({
        "ok": True,
        "engine": "Freedom Structure Scanner V7",
        "server_time_utc": now_utc_string(),
        "chronological_order": "A -> C -> B -> D -> E -> F",
        "memory_discard": True,
        "database_path": os.path.abspath(DB_PATH),
        "database_persistent": os.path.abspath(DB_PATH).startswith("/var/data"),
        "stored_structures": stored_structures,
        "telegram_users": telegram_users,
        "telegram_configured": bool(TELEGRAM_BOT_TOKEN),
        "admin_key_configured": bool(ADMIN_KEY),
        "continuous_scanner": bool(SCANNER_THREAD and SCANNER_THREAD.is_alive() and not SCANNER_STOP.is_set()),
        "scanner_interval_seconds": config["interval"],
        "scanner_targets": targets,
    })


@app.get("/symbols/resolve")
def resolve_symbol_api():
    raw = request.args.get("pair", "")
    resolved = canonical_symbol(raw)
    return jsonify({
        "input": raw,
        "deriv_symbol": resolved,
        "supported": bool(resolved),
    })


@app.get("/symbols")
def list_symbols_api():
    return jsonify({
        "ok": True,
        "symbols": sorted(set(SYMBOL_MAP.values())),
    })


@app.get("/ohlc")
def ohlc_api():
    raw_pair = request.args.get("pair", "")
    pair = canonical_symbol(raw_pair)
    timeframe = request.args.get("timeframe", "M15").upper()

    if not pair:
        return jsonify({
            "error": f"Unsupported symbol: {raw_pair}",
            "hint": "Use /symbols to see supported symbols"
        }), 400

    if timeframe not in TIMEFRAME_MAP:
        return jsonify({
            "error": "Unsupported timeframe",
            "supported": list(TIMEFRAME_MAP)
        }), 400

    try:
        count = max(100, min(int(request.args.get("count", DEFAULT_CANDLE_COUNT)), 1000))
    except ValueError:
        return jsonify({"error": "Invalid count"}), 400

    try:
        rows = get_candles(pair, TIMEFRAME_MAP[timeframe], count)
        candles = closed_candles(normalize(rows), TIMEFRAME_MAP[timeframe])
        return jsonify(candles)
    except Exception as exc:
        return jsonify({"error": str(exc), "pair": pair, "timeframe": timeframe}), 502


@app.get("/scan")
def scan_api():
    raw_pair = request.args.get("pair", "")
    pair = canonical_symbol(raw_pair)
    timeframe = request.args.get("timeframe", "M15").upper()

    if not pair:
        return jsonify({
            "error": f"Unsupported symbol: {raw_pair}",
            "hint": "Use /symbols to see supported symbols"
        }), 400

    if timeframe not in TIMEFRAME_MAP:
        return jsonify({
            "error": "Unsupported timeframe",
            "supported": list(TIMEFRAME_MAP)
        }), 400

    try:
        count = max(100, min(int(request.args.get("count", DEFAULT_CANDLE_COUNT)), 1000))
        strength = max(1, int(request.args.get("strength", 3)))
        min_atr = max(0.0, float(request.args.get("min_atr_move", 0.25)))
        expansion_atr = max(0.0, float(request.args.get("min_expansion_atr", 0.5)))
        displacement_atr = max(0.0, float(request.args.get("displacement_atr", 1.0)))
        min_bars = max(1, int(request.args.get("min_bars", 1)))
        fib_ratio = max(0.0, min(1.0, float(request.args.get("fib_threshold", 0.5))))
    except ValueError:
        return jsonify({"error": "Invalid numeric parameter"}), 400

    bos_mode = request.args.get("bos", "body").lower()
    if bos_mode not in ("body", "wick"):
        bos_mode = "body"

    if not SCAN_LOCK.acquire(blocking=False):
        return jsonify({"error": "Scanner is already processing another request"}), 409

    try:
        rows = get_candles(pair, TIMEFRAME_MAP[timeframe], count)
        candles = closed_candles(normalize(rows), TIMEFRAME_MAP[timeframe])
        if len(candles) < max(50, strength * 4):
            return jsonify({"error": "Not enough closed candles", "candles": len(candles)}), 502
        result = run_scan(
            pair=pair, timeframe=timeframe, candles=candles,
            strength=strength, bos_mode=bos_mode, min_atr=min_atr,
            expansion_atr=expansion_atr, displacement_atr=displacement_atr,
            min_bars=min_bars, fib_ratio=fib_ratio,
        )
        return jsonify(result)
    except Exception as exc:
        return jsonify({"error": str(exc), "pair": pair, "timeframe": timeframe}), 502
    finally:
        SCAN_LOCK.release()


@app.get("/structures")
def structures_api():
    raw_pair = request.args.get("pair")
    timeframe = request.args.get("timeframe")

    sql = "SELECT * FROM structures"
    arguments = []
    where = []

    if raw_pair:
        pair = canonical_symbol(raw_pair)
        if not pair:
            return jsonify({"error": f"Unsupported symbol: {raw_pair}"}), 400
        where.append("pair=?")
        arguments.append(pair)

    if timeframe:
        where.append("timeframe=?")
        arguments.append(timeframe.upper())

    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY updated_epoch DESC LIMIT 300"

    with DB_LOCK:
        connection = db()
        try:
            rows = connection.execute(sql, arguments).fetchall()
        finally:
            connection.close()
    return jsonify([row_to_s(row) for row in rows])


@app.route("/scanner/targets", methods=["GET", "PUT", "POST"])
def scanner_targets_api():
    if request.method == "GET":
        config = scanner_settings()
        targets = effective_scanner_targets(config)
        return jsonify({
            "ok": True, "targets": targets,
            "pairs": unique_values([t["pair"] for t in targets]),
            "timeframes": unique_values([t["timeframe"] for t in targets]),
            "supported_timeframes": list(TIMEFRAME_MAP),
            "source": "database" if database_scanner_targets() else "environment",
        })

    if not optional_admin_authorized():
        return jsonify({"error": "unauthorized"}), 401

    body = request.get_json(force=True, silent=True) or {}
    pairs = body.get("pairs", [])
    timeframes = body.get("timeframes", [])

    if not isinstance(pairs, list):
        return jsonify({"error": "pairs must be a JSON list"}), 400
    if not isinstance(timeframes, list):
        return jsonify({"error": "timeframes must be a JSON list"}), 400

    # Reject unsupported symbols
    invalid = [p for p in pairs if not is_supported_symbol(p)]
    if invalid:
        return jsonify({
            "error": "Unsupported symbols detected",
            "invalid": invalid,
            "supported": sorted(set(SYMBOL_MAP.values())),
        }), 400

    try:
        targets = replace_scanner_targets(pairs, timeframes)
        return jsonify({
            "ok": True, "message": "Scanner targets updated", "targets": targets,
            "pairs": unique_values([t["pair"] for t in targets]),
            "timeframes": unique_values([t["timeframe"] for t in targets]),
        })
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400


@app.get("/scanner/status")
def scanner_status_api():
    config = scanner_settings()
    return jsonify({
        "running": bool(SCANNER_THREAD and SCANNER_THREAD.is_alive() and not SCANNER_STOP.is_set()),
        "enabled": config["enabled"],
        "server_time_utc": now_utc_string(),
        "interval_seconds": config["interval"],
        "candle_count": config["count"],
        "targets": effective_scanner_targets(config),
        "last_processed": dict(SCANNER_LAST),
        "diagnostics": {
            "last_check": dict(SCANNER_DIAGNOSTICS["last_check"]),
            "last_success": dict(SCANNER_DIAGNOSTICS["last_success"]),
            "last_error": dict(SCANNER_DIAGNOSTICS["last_error"]),
            "scan_count": dict(SCANNER_DIAGNOSTICS["scan_count"]),
        },
    })


@app.post("/scanner/start")
def scanner_start_api():
    if not optional_admin_authorized():
        return jsonify({"error": "unauthorized"}), 401
    started = start_scanner()
    return jsonify({
        "ok": True, "started": started,
        "message": "Scanner started" if started else "Scanner already running",
    })


@app.post("/scanner/stop")
def scanner_stop_api():
    if not optional_admin_authorized():
        return jsonify({"error": "unauthorized"}), 401
    stop_scanner()
    return jsonify({"ok": True, "message": "Scanner is stopping"})


# ============================================================
# TELEGRAM ROUTES
# ============================================================

@app.post("/telegram/register")
def telegram_register_api():
    body = request.get_json(force=True, silent=True) or {}
    chat_id = body.get("chat_id")
    if not chat_id:
        return jsonify({"error": "chat_id required"}), 400
    chat_id = str(chat_id).strip()
    with DB_LOCK:
        connection = db()
        try:
            connection.execute(
                "INSERT INTO telegram_users(chat_id, username, active, created_epoch) VALUES(?,?,1,?) ON CONFLICT(chat_id) DO UPDATE SET username=excluded.username, active=1",
                (chat_id, body.get("username", ""), int(time.time()))
            )
            connection.commit()
            total = connection.execute(
                "SELECT COUNT(*) FROM telegram_users WHERE active=1"
            ).fetchone()[0]
        finally:
            connection.close()
    print(f"[Telegram] Registered chat_id={chat_id}")
    return jsonify({
        "ok": True, "chat_id": chat_id, "active_users": total,
        "telegram_configured": bool(TELEGRAM_BOT_TOKEN),
    })


@app.post("/telegram/unregister")
def telegram_unregister_api():
    body = request.get_json(force=True, silent=True) or {}
    chat_id = body.get("chat_id")
    if not chat_id:
        return jsonify({"error": "chat_id required"}), 400
    with DB_LOCK:
        connection = db()
        try:
            connection.execute("UPDATE telegram_users SET active=0 WHERE chat_id=?", (str(chat_id),))
            connection.commit()
        finally:
            connection.close()
    return jsonify({"ok": True, "chat_id": str(chat_id), "active": False})


@app.get("/telegram/users")
def telegram_users_api():
    if not strict_admin_authorized():
        return jsonify({"error": "unauthorized"}), 401
    with DB_LOCK:
        connection = db()
        try:
            rows = connection.execute(
                "SELECT chat_id, username, active, created_epoch FROM telegram_users ORDER BY created_epoch DESC"
            ).fetchall()
        finally:
            connection.close()
    return jsonify([dict(row) for row in rows])


@app.post("/telegram/test")
def telegram_test_api():
    if not strict_admin_authorized():
        return jsonify({"error": "unauthorized"}), 401
    if not TELEGRAM_BOT_TOKEN:
        return jsonify({"ok": False, "error": "TELEGRAM_BOT_TOKEN not configured"}), 400

    body = request.get_json(force=True, silent=True) or {}
    chat_id = body.get("chat_id")

    message = (
        "✅ <b>Freedom Structure Scanner</b>\n\n"
        f"Test time: <b>{now_utc_string()}</b>\n\n"
        "Telegram delivery is working. You will receive A→F alerts here."
    )

    if chat_id:
        ok, detail = tg_send(str(chat_id).strip(), message)
        return jsonify({
            "ok": ok, "sent": int(ok), "detail": detail,
            "chat_id": str(chat_id).strip(),
        })

    with DB_LOCK:
        connection = db()
        try:
            users = connection.execute(
                "SELECT chat_id FROM telegram_users WHERE active=1"
            ).fetchall()
        finally:
            connection.close()

    if not users:
        return jsonify({"ok": False, "error": "No active users", "sent": 0}), 400

    sent = 0
    failed = []
    for user in users:
        ok, _detail = tg_send(user["chat_id"], message)
        if ok:
            sent += 1
        else:
            failed.append(user["chat_id"])
    return jsonify({
        "ok": sent > 0, "targeted": len(users), "sent": sent,
        "failed": failed, "time_utc": now_utc_string(),
    })


@app.post("/telegram/broadcast")
def telegram_broadcast_api():
    if not strict_admin_authorized():
        return jsonify({"error": "unauthorized"}), 401
    body = request.get_json(force=True, silent=True) or {}
    text = str(body.get("text", "")).strip()
    if not text:
        return jsonify({"error": "text is required"}), 400
    with DB_LOCK:
        connection = db()
        try:
            users = connection.execute(
                "SELECT chat_id FROM telegram_users WHERE active=1"
            ).fetchall()
        finally:
            connection.close()
    sent = sum(1 for u in users if tg_send(u["chat_id"], text)[0])
    return jsonify({"targeted": len(users), "sent": sent})


# ============================================================
# ADMIN ROUTES
# ============================================================

@app.post("/admin/reset")
def admin_reset_api():
    if not strict_admin_authorized():
        return jsonify({"error": "unauthorized"}), 401
    raw_pair = request.args.get("pair")
    timeframe = request.args.get("timeframe")
    with DB_LOCK:
        connection = db()
        try:
            if raw_pair and timeframe:
                connection.execute(
                    "DELETE FROM structures WHERE pair=? AND timeframe=?",
                    (canonical_symbol(raw_pair), timeframe.upper()),
                )
            elif raw_pair:
                connection.execute(
                    "DELETE FROM structures WHERE pair=?",
                    (canonical_symbol(raw_pair),),
                )
            else:
                connection.execute("DELETE FROM structures")
            connection.commit()
        finally:
            connection.close()
    SCANNER_LAST.clear()
    return jsonify({"ok": True, "message": "Structure memory reset"})


@app.post("/admin/repair-database")
def admin_repair_database_api():
    if not strict_admin_authorized():
        return jsonify({"error": "unauthorized"}), 401
    quarantine_corrupt_database()
    return jsonify({"ok": True, "message": "Database quarantined. New DB will be created on next request."})


@app.get("/admin/database")
def admin_database_api():
    if not strict_admin_authorized():
        return jsonify({"error": "unauthorized"}), 401
    absolute_path = os.path.abspath(DB_PATH)
    file_size = os.path.getsize(absolute_path) if os.path.exists(absolute_path) else 0
    with DB_LOCK:
        connection = db()
        try:
            structures_total = connection.execute("SELECT COUNT(*) FROM structures").fetchone()[0]
            complete_total = connection.execute(
                "SELECT COUNT(*) FROM structures WHERE state='COMPLETE'"
            ).fetchone()[0]
            telegram_total = connection.execute(
                "SELECT COUNT(*) FROM telegram_users WHERE active=1"
            ).fetchone()[0]
            targets_total = connection.execute(
                "SELECT COUNT(*) FROM scanner_targets WHERE active=1"
            ).fetchone()[0]
        finally:
            connection.close()
    return jsonify({
        "ok": True,
        "path": absolute_path,
        "persistent": absolute_path.startswith("/var/data"),
        "size_bytes": file_size,
        "structures_total": structures_total,
        "structures_complete": complete_total,
        "telegram_users": telegram_total,
        "scanner_targets": targets_total,
        "time_utc": now_utc_string(),
    })


# ============================================================
# START SCANNER
# ============================================================

if os.environ.get("SCANNER_ENABLED", "true").lower() in ("1", "true", "yes", "on"):
    start_scanner()

# ============================================================
# LOCAL DEVELOPMENT
# ============================================================

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)), debug=False)
