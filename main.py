import codecs
import encodings
import json
import os
import sqlite3
import threading
import time
import urllib.parse
import urllib.request

import websocket
from flask import Flask, jsonify, request


# ============================================================
# PYTHON CODEC INITIALIZATION
# ============================================================

# Restore/register Python's standard codec search function.
codecs.register(encodings.search_function)

try:
    codecs.lookup("idna")
    print("[Startup] IDNA codec: OK", flush=True)
except LookupError as e:
    print(f"[Startup] IDNA codec ERROR: {e}", flush=True)


# ============================================================
# FLASK APP
# ============================================================

app = Flask(__name__)


# ============================================================
# CONFIGURATION
# ============================================================

DERIV_APP_ID = os.environ.get(
    "DERIV_APP_ID",
    "YOUR_APP_ID_HERE"
)

DERIV_WS_URL = (
    f"wss://ws.derivws.com/websockets/v3"
    f"?app_id={DERIV_APP_ID}"
)

TELEGRAM_BOT_TOKEN = os.environ.get(
    "TELEGRAM_BOT_TOKEN",
    ""
)

ADMIN_KEY = os.environ.get(
    "ADMIN_KEY",
    ""
)

DB_PATH = os.environ.get(
    "DB_PATH",
    "structure_memory.db"
)


# ============================================================
# TIMEFRAME / SYMBOL MAPS
# ============================================================

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
}


# ============================================================
# GLOBAL LOCKS / SCANNER STATE
# ============================================================

DB_LOCK = threading.Lock()
SCAN_LOCK = threading.Lock()
DIAGNOSTIC_LOCK = threading.Lock()

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
# DATABASE
# ============================================================

def db():
    """
    Open SQLite database and ensure required tables exist.
    """

    connection = sqlite3.connect(
        DB_PATH,
        timeout=30
    )

    connection.row_factory = sqlite3.Row

    connection.execute(
        """
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

            live_from_epoch INTEGER DEFAULT 0
        )
        """
    )

    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS telegram_users(
            chat_id TEXT PRIMARY KEY,
            username TEXT,
            active INTEGER DEFAULT 1,
            created_epoch INTEGER
        )
        """
    )

    # Upgrade databases created by earlier versions.
    columns = {
        row[1]
        for row in connection.execute(
            "PRAGMA table_info(structures)"
        ).fetchall()
    }

    if "live_from_epoch" not in columns:
        connection.execute(
            """
            ALTER TABLE structures
            ADD COLUMN live_from_epoch INTEGER DEFAULT 0
            """
        )

    connection.commit()

    return connection


# ============================================================
# JSON HELPERS
# ============================================================

def j(value):
    if value is None:
        return None

    return json.dumps(
        value,
        separators=(",", ":")
    )


# ============================================================
# STRUCTURE HELPERS
# ============================================================

def point(label, role, candle, price):
    return {
        "label": label,
        "role": role,
        "price": float(price),
        "epoch": int(candle["epoch"]),

        "open": candle["open"],
        "high": candle["high"],
        "low": candle["low"],
        "close": candle["close"],
    }


def structure_key(structure):
    return (
        f"{structure['pair']}|"
        f"{structure['timeframe']}|"
        f"{structure['direction']}|"
        f"{structure['a']['epoch']}|"
        f"{structure['b']['epoch']}|"
        f"{structure['c']['epoch']}"
    )


def fib50(b, e, direction):
    # The midpoint is the same for both directions.
    return (b + e) / 2.0


# ============================================================
# SAVE STRUCTURE
# ============================================================

def save(structure):
    now = int(time.time())
    key = structure_key(structure)

    with DB_LOCK:
        connection = db()

        connection.execute(
            """
            INSERT INTO structures(
                structure_key,
                pair,
                timeframe,
                direction,
                state,

                a_json,
                b_json,
                c_json,
                d_json,
                e_json,
                f_json,

                fib50,
                valid,
                telegram_sent,

                created_epoch,
                updated_epoch,

                live_from_epoch
            )
            VALUES(
                ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?
            )

            ON CONFLICT(structure_key)
            DO UPDATE SET
                state = excluded.state,

                d_json = excluded.d_json,
                e_json = excluded.e_json,
                f_json = excluded.f_json,

                fib50 = excluded.fib50,
                valid = excluded.valid,

                telegram_sent = excluded.telegram_sent,

                updated_epoch = excluded.updated_epoch,

                live_from_epoch = excluded.live_from_epoch
            """,
            (
                key,

                structure["pair"],
                structure["timeframe"],
                structure["direction"],
                structure["state"],

                j(structure.get("a")),
                j(structure.get("b")),
                j(structure.get("c")),
                j(structure.get("d")),
                j(structure.get("e")),
                j(structure.get("f")),

                structure.get("fib50"),

                int(bool(structure.get("valid"))),

                int(
                    bool(
                        structure.get(
                            "telegram_sent",
                            False
                        )
                    )
                ),

                structure.get(
                    "created_epoch",
                    structure["a"]["epoch"]
                ),

                now,

                int(
                    structure.get(
                        "live_from_epoch",
                        0
                    )
                ),
            ),
        )

        connection.commit()
        connection.close()

    return key


# ============================================================
# DATABASE ROW → STRUCTURE
# ============================================================

def row_to_structure(row):

    def parse(name):
        if not row[name]:
            return None

        return json.loads(row[name])

    return {
        "id": row["id"],
        "structure_key": row["structure_key"],

        "pair": row["pair"],
        "timeframe": row["timeframe"],
        "direction": row["direction"],
        "state": row["state"],

        "a": parse("a_json"),
        "b": parse("b_json"),
        "c": parse("c_json"),
        "d": parse("d_json"),
        "e": parse("e_json"),
        "f": parse("f_json"),

        "fib50": row["fib50"],

        "valid": bool(row["valid"]),
        "telegram_sent": bool(row["telegram_sent"]),

        "live_from_epoch": int(
            row["live_from_epoch"] or 0
        ),
    }


# ============================================================
# STRUCTURE MEMORY
# ============================================================

def structures_for(pair, timeframe):

    with DB_LOCK:
        connection = db()

        rows = connection.execute(
            """
            SELECT *
            FROM structures
            WHERE pair = ?
              AND timeframe = ?
            ORDER BY created_epoch
            """,
            (
                pair,
                timeframe
            )
        ).fetchall()

        connection.close()

    return [
        row_to_structure(row)
        for row in rows
    ]


# ============================================================
# DERIV DATA
# ============================================================

def get_candles(symbol, granularity, count=500):
    """
    Fetch candles from Deriv using WebSocket.

    Returns:
        sorted list of candle dictionaries
    """

    result = []
    error_message = None

    completed = threading.Event()

    def on_message(ws, message):
        nonlocal result
        nonlocal error_message

        try:
            data = json.loads(message)

            if "candles" in data:
                result = data["candles"]
                completed.set()
                return

            if "error" in data:
                error_message = data["error"].get(
                    "message",
                    str(data["error"])
                )
                completed.set()

        except Exception as exc:
            error_message = str(exc)
            completed.set()

    def on_error(ws, error):
        nonlocal error_message

        error_message = str(error)
        completed.set()

    def on_open(ws):
        request_data = {
            "ticks_history": symbol,
            "adjust_start_time": 1,
            "count": max(
                100,
                min(int(count), 1000)
            ),
            "granularity": int(granularity),
            "style": "candles",
            "end": "latest",
        }

        ws.send(
            json.dumps(request_data)
        )

    websocket_app = websocket.WebSocketApp(
        DERIV_WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
    )

    websocket_thread = threading.Thread(
        target=websocket_app.run_forever,
        daemon=True
    )

    websocket_thread.start()

    # Maximum wait for Deriv.
    completed.wait(30)

    try:
        websocket_app.close()
    except Exception:
        pass

    if error_message:
        print(
            f"[Deriv] {symbol} error: {error_message}",
            flush=True
        )

        raise RuntimeError(
            f"Deriv WebSocket error: {error_message}"
        )

    if not result:
        raise RuntimeError(
            f"Deriv returned no candles for {symbol}"
        )

    result = sorted(
        result,
        key=lambda x: int(
            x.get("epoch", 0)
        )
    )

    return result


# ============================================================
# NORMALIZE CANDLES
# ============================================================

def normalize(rows):

    normalized = []

    for row in rows:
        normalized.append(
            {
                "epoch": int(
                    row.get("epoch", 0)
                ),

                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),

                "volume": float(
                    row.get("volume", 0)
                ),
            }
        )

    return normalized


# ============================================================
# CLOSED CANDLES
# ============================================================

def closed_candles(rows, granularity):
    """
    Return only fully closed candles.

    The currently forming candle is ignored.
    """

    now = int(time.time())

    return [
        candle
        for candle in rows
        if (
            candle["epoch"]
            + int(granularity)
            <= now
        )
    ]


# ============================================================
# ATR
# ============================================================

def atrs(candles, period=14):

    if not candles:
        return []

    true_ranges = []

    for index, candle in enumerate(candles):

        if index == 0:
            true_ranges.append(
                candle["high"]
                - candle["low"]
            )

        else:
            previous = candles[index - 1]

            true_ranges.append(
                max(
                    candle["high"]
                    - candle["low"],

                    abs(
                        candle["high"]
                        - previous["close"]
                    ),

                    abs(
                        candle["low"]
                        - previous["close"]
                    ),
                )
            )

    output = [0.0] * len(candles)

    for index in range(
        period,
        len(candles)
    ):
        output[index] = (
            sum(
                true_ranges[
                    index - period + 1:
                    index + 1
                ]
            )
            / period
        )

    return output


# ============================================================
# SWINGS
# ============================================================

def swings(candles, strength=3):

    strength = max(
        1,
        int(strength)
    )

    output = []

    for index in range(
        strength,
        len(candles) - strength
    ):

        candle = candles[index]

        is_low = (
            all(
                candle["low"] < other["low"]
                for other in candles[
                    index - strength:
                    index
                ]
            )
            and
            all(
                candle["low"] <= other["low"]
                for other in candles[
                    index + 1:
                    index + strength + 1
                ]
            )
        )

        is_high = (
            all(
                candle["high"] > other["high"]
                for other in candles[
                    index - strength:
                    index
                ]
            )
            and
            all(
                candle["high"] >= other["high"]
                for other in candles[
                    index + 1:
                    index + strength + 1
                ]
            )
        )

        if is_low:
            output.append(
                ("L", index, candle)
            )

        if is_high:
            output.append(
                ("H", index, candle)
            )

    return sorted(
        output,
        key=lambda item: item[1]
    )


# ============================================================
# DISCOVER A-B-C
# ============================================================

def discover_abc(
    candles,
    pair,
    timeframe,
    direction,
    strength,
    min_atr
):
    """
    Build chronological A-B-C candidates.

    B must extend A.

    C is the first opposite swing
    after B.
    """

    swing_points = swings(
        candles,
        strength
    )

    atr_values = atrs(candles)

    output = []

    wanted = (
        "L"
        if direction == "BULLISH"
        else "H"
    )

    opposite = (
        "H"
        if direction == "BULLISH"
        else "L"
    )

    same_type = [
        item
        for item in swing_points
        if item[0] == wanted
    ]

    opposite_type = [
        item
        for item in swing_points
        if item[0] == opposite
    ]

    for index in range(
        len(same_type) - 1
    ):

        a = same_type[index]
        b = same_type[index + 1]

        # Bullish:
        # B must sweep lower than A.
        if (
            direction == "BULLISH"
            and b[2]["low"] >= a[2]["low"]
        ):
            continue

        # Bearish:
        # B must sweep higher than A.
        if (
            direction == "BEARISH"
            and b[2]["high"] <= a[2]["high"]
        ):
            continue

        if (
            min_atr
            and atr_values[b[1]]
        ):

            displacement = (
                a[2]["low"]
                - b[2]["low"]
                if direction == "BULLISH"
                else
                b[2]["high"]
                - a[2]["high"]
            )

            if (
                displacement
                < atr_values[b[1]]
                * min_atr
            ):
                continue

        c = next(
            (
                item
                for item in opposite_type
                if item[1] > b[1]
            ),
            None
        )

        if not c:
            continue

        structure = {
            "pair": pair,
            "timeframe": timeframe,
            "direction": direction,

            "a": point(
                "A",
                "SWEEP",
                a[2],
                (
                    a[2]["low"]
                    if direction == "BULLISH"
                    else a[2]["high"]
                )
            ),

            "b": point(
                "B",
                "SWING",
                b[2],
                (
                    b[2]["low"]
                    if direction == "BULLISH"
                    else b[2]["high"]
                )
            ),

            "c": point(
                "C",
                "STRUCTURE",
                c[2],
                (
                    c[2]["high"]
                    if direction == "BULLISH"
                    else c[2]["low"]
                )
            ),

            "d": None,
            "e": None,
            "f": None,

            "fib50": None,

            "state": "WAITING_FOR_BOS",

            "valid": False,

            "telegram_sent": False,
        }

        output.append(structure)

    return output


# ============================================================
# ADVANCE A STRUCTURE
# ============================================================

def advance(
    structure,
    candles,
    bos="body",
    exp_atr=0.5,
    disp_atr=1.0,
    allow_historical_f=False
):
    """
    Advance one stored structure.

    A/B/C/D/E are locked once written.

    F is only allowed from live_from_epoch
    onward for backfilled structures.
    """

    index_by_epoch = {
        candle["epoch"]: index
        for index, candle in enumerate(candles)
    }

    atr_values = atrs(candles)

    direction = structure["direction"]

    c_index = index_by_epoch.get(
        structure["c"]["epoch"]
    )

    if c_index is None:
        return structure

    level = structure["c"]["price"]

    # ========================================================
    # D - FIRST CONFIRMED BOS AFTER C
    # ========================================================

    if structure["d"] is None:

        for index in range(
            c_index + 1,
            len(candles)
        ):

            candle = candles[index]

            if bos == "body":

                broken = (
                    candle["close"] > level
                    if direction == "BULLISH"
                    else candle["close"] < level
                )

            else:

                broken = (
                    candle["high"] > level
                    if direction == "BULLISH"
                    else candle["low"] < level
                )

            if broken:

                structure["d"] = point(
                    "D",
                    "BOS",
                    candle,
                    (
                        candle["high"]
                        if direction == "BULLISH"
                        else candle["low"]
                    )
                )

                structure["state"] = (
                    "WAITING_FOR_EXPANSION"
                )

                break

    if not structure["d"]:
        return structure

    d_index = index_by_epoch.get(
        structure["d"]["epoch"]
    )

    if d_index is None:
        return structure

    # ========================================================
    # E - FIRST SIGNIFICANT EXPANSION SWING
    # ========================================================

    if structure["e"] is None:

        swing_points = swings(
            candles,
            2
        )

        for kind, index, candle in swing_points:

            if index <= d_index:
                continue

            move = (
                candle["high"] - level
                if direction == "BULLISH"
                else level - candle["low"]
            )

            if move <= 0:
                continue

            base_atr = (
                atr_values[d_index]
                or atr_values[index]
            )

            if (
                base_atr
                and move
                < base_atr * exp_atr
            ):
                continue

            if (
                direction == "BULLISH"
                and kind != "H"
            ):
                continue

            if (
                direction == "BEARISH"
                and kind != "L"
            ):
                continue

            structure["e"] = point(
                "E",
                "EXPANSION",
                candle,
                (
                    candle["high"]
                    if direction == "BULLISH"
                    else candle["low"]
                )
            )

            structure["fib50"] = fib50(
                structure["b"]["price"],
                structure["e"]["price"],
                direction
            )

            structure["state"] = (
                "WAITING_FOR_DISPLACEMENT"
            )

            break

    if not structure["e"]:
        return structure

    e_index = index_by_epoch.get(
        structure["e"]["epoch"]
    )

    if e_index is None:
        return structure

    # ========================================================
    # F - LIVE DISPLACEMENT
    # ========================================================

    midpoint = structure["fib50"]

    live_from_epoch = int(
        structure.get(
            "live_from_epoch",
            0
        )
    )

    if structure["f"] is None:

        for index in range(
            e_index + 1,
            len(candles)
        ):

            candle = candles[index]

            # Never generate an historical F
            # for a newly discovered/backfilled structure.
            if (
                not allow_historical_f
                and live_from_epoch
                and candle["epoch"]
                <= live_from_epoch
            ):
                continue

            f_price = (
                candle["low"]
                if direction == "BULLISH"
                else candle["high"]
            )

            reaches_midpoint = (
                f_price <= midpoint
                if direction == "BULLISH"
                else f_price >= midpoint
            )

            if not reaches_midpoint:
                continue

            candle_range = (
                candle["high"]
                - candle["low"]
            )

            candle_body = abs(
                candle["close"]
                - candle["open"]
            )

            if (
                not atr_values[index]
                or candle_range
                < atr_values[index] * 1.1
                or candle_body
                < atr_values[index] * disp_atr
            ):
                continue

            structure["f"] = point(
                "F",
                "DISPLACEMENT",
                candle,
                f_price
            )

            structure["valid"] = True

            structure["state"] = "COMPLETE"

            break

    return structure


# ============================================================
# TELEGRAM
# ============================================================

def tg_send(chat_id, text):

    if not TELEGRAM_BOT_TOKEN:
        return False

    data = urllib.parse.urlencode(
        {
            "chat_id": str(chat_id),
            "text": text,
            "parse_mode": "HTML",
        }
    ).encode()

    url = (
        f"https://api.telegram.org/"
        f"bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    )

    request_object = urllib.request.Request(
        url,
        data=data
    )

    try:
        urllib.request.urlopen(
            request_object,
            timeout=10
        ).read()

        return True

    except Exception as exc:

        print(
            f"[Telegram] Error: {exc}",
            flush=True
        )

        return False


# ============================================================
# TELEGRAM SIGNAL TEXT
# ============================================================

def signal_text(structure):

    icon = (
        "🟢"
        if structure["direction"] == "BULLISH"
        else "🔴"
    )

    lines = [
        (
            f'{icon} '
            f'{structure["direction"]} '
            f'A→F STRUCTURE CONFIRMED'
        ),
        "",

        f'Symbol: {structure["pair"]}',
        f'Timeframe: {structure["timeframe"]}',
        "",
    ]

    for label in (
        "a",
        "b",
        "c",
        "d",
        "e",
        "f"
    ):

        point_data = structure[label]

        if not point_data:
            continue

        lines.extend(
            [
                (
                    f'{label.upper()} — '
                    f'{point_data["role"]}'
                ),

                (
                    f'Price: '
                    f'{point_data["price"]}'
                ),

                (
                    f'Time: '
                    f'{time.strftime(
                        "%Y-%m-%d %H:%M:%S UTC",
                        time.gmtime(
                            point_data["epoch"]
                        )
                    )}'
                ),

                "",
            ]
        )

    lines.extend(
        [
            "━━━━━━━━━━━━━━━━━━",

            (
                f'50% Fibonacci: '
                f'{structure["fib50"]}'
            ),

            "",

            "ENTRY: Manual",
            "TP: Manual",
            "SL: Manual",

            "",

            (
                "A→F is confirmed. "
                "The engine does not decide "
                "entry, TP or SL."
            ),
        ]
    )

    return "\n".join(lines)


# ============================================================
# SEND ALERT
# ============================================================

def alert(structure):

    key = structure_key(structure)

    with DB_LOCK:

        connection = db()

        row = connection.execute(
            """
            SELECT telegram_sent
            FROM structures
            WHERE structure_key = ?
            """,
            (key,)
        ).fetchone()

        if (
            row
            and row["telegram_sent"]
        ):
            connection.close()
            return 0

        users = connection.execute(
            """
            SELECT chat_id
            FROM telegram_users
            WHERE active = 1
            """
        ).fetchall()

        connection.close()

    sent = 0

    for user in users:

        if tg_send(
            user["chat_id"],
            signal_text(structure)
        ):
            sent += 1

    if sent:

        with DB_LOCK:

            connection = db()

            connection.execute(
                """
                UPDATE structures
                SET telegram_sent = 1,
                    updated_epoch = ?
                WHERE structure_key = ?
                """,
                (
                    int(time.time()),
                    key
                )
            )

            connection.commit()
            connection.close()

    return sent


# ============================================================
# RUN STRUCTURE SCAN
# ============================================================

def run_scan(
    pair,
    timeframe,
    candles,
    strength,
    bos,
    min_atr,
    exp_atr,
    disp_atr
):
    """
    Main A→F structure engine.
    """

    if not candles:
        return {
            "pair": pair,
            "timeframe": timeframe,
            "completed_now": 0,
            "active_structures": 0,
            "stored_structures": 0,
            "signals": [],
            "completed_signals": [],
        }

    latest_epoch = candles[-1]["epoch"]

    memory = structures_for(
        pair,
        timeframe
    )

    existing_keys = {
        item["structure_key"]
        for item in memory
    }

    # ========================================================
    # HISTORICAL A-B-C DISCOVERY
    # ========================================================

    for direction in (
        "BULLISH",
        "BEARISH"
    ):

        candidates = discover_abc(
            candles,
            pair,
            timeframe,
            direction,
            strength,
            min_atr
        )

        for structure in candidates:

            key = structure_key(
                structure
            )

            if key in existing_keys:
                continue

            # Candidate becomes live only
            # after this backfill scan.
            structure["live_from_epoch"] = (
                latest_epoch
            )

            structure = advance(
                structure,
                candles,
                bos,
                exp_atr,
                disp_atr,
                allow_historical_f=False
            )

            save(structure)

            memory.append(structure)

            existing_keys.add(key)

    # ========================================================
    # ADVANCE EXISTING STRUCTURES
    # ========================================================

    completed_now = []

    for structure in memory:

        old_state = structure["state"]

        # V2/V3 compatibility.
        if not structure.get(
            "live_from_epoch"
        ):

            structure["live_from_epoch"] = (
                latest_epoch
            )

            save(structure)

        if structure["state"] != "COMPLETE":

            structure = advance(
                structure,
                candles,
                bos,
                exp_atr,
                disp_atr,
                allow_historical_f=False
            )

            save(structure)

        # New completion detected.
        if (
            old_state != "COMPLETE"
            and structure["state"] == "COMPLETE"
        ):

            sent = alert(
                structure
            )

            structure[
                "telegram_sent_now"
            ] = bool(sent)

            completed_now.append(
                structure
            )

    return {
        "pair": pair,
        "timeframe": timeframe,

        "scan_mode": "DUAL_DIRECTION",

        "memory_enabled": True,

        "chronological_locking": True,

        "historical_f_alerts_suppressed": True,

        "active_structures": sum(
            1
            for item in memory
            if item["state"] != "COMPLETE"
        ),

        "stored_structures": len(memory),

        "completed_now": len(
            completed_now
        ),

        "signals": memory,

        "completed_signals": completed_now,
    }


# ============================================================
# ENVIRONMENT HELPERS
# ============================================================

def env_list(name, default):

    raw = os.environ.get(
        name,
        default
    )

    return [
        item.strip().upper()
        for item in raw.split(",")
        if item.strip()
    ]


# ============================================================
# SCANNER SETTINGS
# ============================================================

def scanner_settings():

    pairs = env_list(
        "SCANNER_PAIRS",
        "VOLATILITY100"
    )

    timeframes = [
        item
        for item in env_list(
            "SCANNER_TIMEFRAMES",
            "M15"
        )
        if item in TIMEFRAME_MAP
    ]

    return {
        "enabled": (
            os.environ.get(
                "SCANNER_ENABLED",
                "true"
            ).lower()
            in (
                "1",
                "true",
                "yes",
                "on",
            )
        ),

        "pairs": pairs,

        "timeframes": (
            timeframes
            or ["M15"]
        ),

        # IMPORTANT:
        # Default scanner interval is 120 seconds.
        "interval": max(
            5,
            int(
                os.environ.get(
                    "SCANNER_INTERVAL_SECONDS",
                    "120"
                )
            )
        ),

        "count": max(
            100,
            min(
                1000,
                int(
                    os.environ.get(
                        "SCANNER_CANDLE_COUNT",
                        "500"
                    )
                )
            )
        ),

        "strength": int(
            os.environ.get(
                "SCANNER_STRENGTH",
                "3"
            )
        ),

        "bos": (
            os.environ.get(
                "SCANNER_BOS",
                "body"
            ).lower()
        ),

        "min_atr": float(
            os.environ.get(
                "SCANNER_MIN_ATR_MOVE",
                "0.25"
            )
        ),

        "exp_atr": float(
            os.environ.get(
                "SCANNER_MIN_EXPANSION_ATR",
                "0.5"
            )
        ),

        "disp_atr": float(
            os.environ.get(
                "SCANNER_DISPLACEMENT_ATR",
                "1.0"
            )
        ),
    }


# ============================================================
# DIAGNOSTIC HELPERS
# ============================================================

def diagnostic_key(pair, timeframe):
    return f"{pair}|{timeframe}"


def set_last_check(key, data):

    with DIAGNOSTIC_LOCK:
        SCANNER_DIAGNOSTICS[
            "last_check"
        ][key] = data


def set_last_success(key, data):

    with DIAGNOSTIC_LOCK:
        SCANNER_DIAGNOSTICS[
            "last_success"
        ][key] = data


def set_last_error(key, data):

    with DIAGNOSTIC_LOCK:
        SCANNER_DIAGNOSTICS[
            "last_error"
        ][key] = data


def clear_last_error(key):

    with DIAGNOSTIC_LOCK:
        SCANNER_DIAGNOSTICS[
            "last_error"
        ].pop(
            key,
            None
        )


def increment_scan_count(key):

    with DIAGNOSTIC_LOCK:

        current = (
            SCANNER_DIAGNOSTICS[
                "scan_count"
            ].get(
                key,
                0
            )
        )

        SCANNER_DIAGNOSTICS[
            "scan_count"
        ][key] = current + 1

        return current + 1


# ============================================================
# ONE CONTINUOUS SCAN
# ============================================================

def continuous_scan_once(
    pair,
    timeframe,
    config
):
    """
    Perform one scanner cycle.

    Diagnostics are updated regardless of
    whether the scan succeeds or fails.
    """

    key = diagnostic_key(
        pair,
        timeframe
    )

    check_time = int(time.time())

    set_last_check(
        key,
        {
            "time": check_time,
            "status": "started",
        }
    )

    scan_number = increment_scan_count(
        key
    )

    print(
        (
            f"[Scanner] CHECK #{scan_number} "
            f"{pair} {timeframe} "
            f"started"
        ),
        flush=True
    )

    try:

        granularity = TIMEFRAME_MAP[
            timeframe
        ]

        deriv_symbol = SYMBOL_MAP.get(
            pair,
            pair
        )

        # ====================================================
        # GET CANDLES
        # ====================================================

        raw_candles = get_candles(
            deriv_symbol,
            granularity,
            config["count"]
        )

        normalized = normalize(
            raw_candles
        )

        candles = closed_candles(
            normalized,
            granularity
        )

        print(
            (
                f"[Scanner] {pair} {timeframe} "
                f"received {len(normalized)} "
                f"candles / "
                f"{len(candles)} closed"
            ),
            flush=True
        )

        # ====================================================
        # ENOUGH DATA?
        # ====================================================

        minimum_required = max(
            50,
            config["strength"] * 4
        )

        if len(candles) < minimum_required:

            error = (
                "not_enough_closed_candles"
            )

            set_last_error(
                key,
                {
                    "time": int(time.time()),
                    "error": error,
                    "candles": len(candles),
                }
            )

            set_last_check(
                key,
                {
                    "time": int(time.time()),
                    "status": "insufficient_data",
                    "candles": len(candles),
                }
            )

            print(
                (
                    f"[Scanner] {pair} "
                    f"{timeframe}: "
                    f"not enough closed candles "
                    f"({len(candles)})"
                ),
                flush=True
            )

            return {
                "ok": False,
                "reason": error,
                "pair": pair,
                "timeframe": timeframe,
            }

        # ====================================================
        # LATEST CLOSED CANDLE
        # ====================================================

        latest_epoch = candles[-1][
            "epoch"
        ]

        previous_epoch = SCANNER_LAST.get(
            key
        )

        # ====================================================
        # SAME CLOSED CANDLE
        # ====================================================

        if (
            previous_epoch is not None
            and previous_epoch
            == latest_epoch
        ):

            SCANNER_LAST[key] = latest_epoch

            set_last_success(
                key,
                {
                    "time": int(time.time()),
                    "latest_closed_epoch":
                        latest_epoch,
                    "candles":
                        len(candles),
                    "status":
                        "no_new_closed_candle",
                }
            )

            set_last_check(
                key,
                {
                    "time": int(time.time()),
                    "status":
                        "complete",
                    "latest_closed_epoch":
                        latest_epoch,
                }
            )

            clear_last_error(
                key
            )

            print(
                (
                    f"[Scanner] {pair} "
                    f"{timeframe}: "
                    f"no new closed candle "
                    f"(latest={latest_epoch})"
                ),
                flush=True
            )

            return {
                "ok": True,
                "pair": pair,
                "timeframe": timeframe,
                "skipped": True,
                "latest_closed_epoch":
                    latest_epoch,
            }

        # ====================================================
        # RUN STRUCTURE ENGINE
        # ====================================================

        result = run_scan(
            pair,
            timeframe,
            candles,

            config["strength"],
            config["bos"],
            config["min_atr"],
            config["exp_atr"],
            config["disp_atr"],
        )

        # Update last processed only
        # after successful scan.
        SCANNER_LAST[key] = (
            latest_epoch
        )

        completed_count = result[
            "completed_now"
        ]

        # ====================================================
        # SUCCESS DIAGNOSTICS
        # ====================================================

        set_last_success(
            key,
            {
                "time": int(time.time()),

                "latest_closed_epoch":
                    latest_epoch,

                "candles":
                    len(candles),

                "active_structures":
                    result[
                        "active_structures"
                    ],

                "stored_structures":
                    result[
                        "stored_structures"
                    ],

                "completed_now":
                    completed_count,

                "status":
                    "scan_complete",
            }
        )

        set_last_check(
            key,
            {
                "time": int(time.time()),

                "status":
                    "complete",

                "latest_closed_epoch":
                    latest_epoch,
            }
        )

        clear_last_error(
            key
        )

        print(
            (
                f"[Scanner] {pair} "
                f"{timeframe}: "
                f"SCAN COMPLETE | "
                f"latest={latest_epoch} | "
                f"active="
                f"{result['active_structures']} | "
                f"stored="
                f"{result['stored_structures']} | "
                f"new_signals="
                f"{completed_count}"
            ),
            flush=True
        )

        return {
            "ok": True,

            "pair": pair,
            "timeframe": timeframe,

            "skipped": False,

            "latest_closed_epoch":
                latest_epoch,

            "completed_now":
                completed_count,

            "active_structures":
                result[
                    "active_structures"
                ],

            "stored_structures":
                result[
                    "stored_structures"
                ],
        }

    except Exception as exc:

        # ====================================================
        # IMPORTANT:
        # An individual scanner error must NOT
        # terminate the scanner thread.
        # ====================================================

        error_text = str(exc)

        set_last_error(
            key,
            {
                "time": int(time.time()),
                "error": error_text,
            }
        )

        set_last_check(
            key,
            {
                "time": int(time.time()),
                "status": "error",
            }
        )

        print(
            (
                f"[Scanner] ERROR "
                f"{pair} {timeframe}: "
                f"{error_text}"
            ),
            flush=True
        )

        return {
            "ok": False,

            "pair": pair,
            "timeframe": timeframe,

            "error": error_text,
        }


# ============================================================
# SCANNER LOOP
# ============================================================

def scanner_loop():

    print(
        (
            "[Scanner] Continuous live scanner "
            "started (interval=120 seconds)"
        ),
        flush=True
    )

    # ========================================================
    # IMPORTANT:
    # Run immediately instead of sleeping 120 seconds first.
    # ========================================================

    first_cycle = True

    while not SCANNER_STOP.is_set():

        cycle_started = int(
            time.time()
        )

        try:

            config = scanner_settings()

            if not config["enabled"]:

                print(
                    (
                        "[Scanner] Scanner is "
                        "disabled by SCANNER_ENABLED"
                    ),
                    flush=True
                )

            else:

                # ====================================================
                # ONE THREAD OWNS LIVE SCANNING
                # ====================================================

                if SCAN_LOCK.acquire(
                    blocking=False
                ):

                    try:

                        print(
                            (
                                "[Scanner] "
                                f"Starting scan cycle "
                                f"at "
                                f"{time.strftime(
                                    '%Y-%m-%d %H:%M:%S UTC',
                                    time.gmtime()
                                )}"
                            ),
                            flush=True
                        )

                        for pair in config[
                            "pairs"
                        ]:

                            if SCANNER_STOP.is_set():
                                break

                            for timeframe in config[
                                "timeframes"
                            ]:

                                if SCANNER_STOP.is_set():
                                    break

                                result = (
                                    continuous_scan_once(
                                        pair,
                                        timeframe,
                                        config
                                    )
                                )

                                if result.get(
                                    "completed_now",
                                    0
                                ):

                                    print(
                                        (
                                            "[Scanner] "
                                            f"{pair} "
                                            f"{timeframe}: "
                                            f"{result['completed_now']} "
                                            "NEW A→F "
                                            "signal(s)"
                                        ),
                                        flush=True
                                    )

                    finally:

                        SCAN_LOCK.release()

                else:

                    print(
                        (
                            "[Scanner] "
                            "Scan lock busy; "
                            "skipping this cycle"
                        ),
                        flush=True
                    )

        except Exception as exc:

            # Absolute last line of defence.
            print(
                (
                    "[Scanner] "
                    f"LOOP ERROR: {exc}"
                ),
                flush=True
            )

        first_cycle = False

        cycle_finished = int(
            time.time()
        )

        elapsed = (
            cycle_finished
            - cycle_started
        )

        config = scanner_settings()

        wait_seconds = max(
            1,
            config["interval"]
            - elapsed
        )

        print(
            (
                f"[Scanner] Cycle finished. "
                f"Next scan in "
                f"{wait_seconds} seconds."
            ),
            flush=True
        )

        # Wait interruptibly so /scanner/stop
        # can stop the thread quickly.
        SCANNER_STOP.wait(
            wait_seconds
        )

    print(
        "[Scanner] Continuous live scanner stopped",
        flush=True
    )


# ============================================================
# START SCANNER
# ============================================================

def start_scanner():

    global SCANNER_THREAD

    if (
        SCANNER_THREAD
        and SCANNER_THREAD.is_alive()
    ):

        print(
            "[Scanner] Already running",
            flush=True
        )

        return False

    SCANNER_STOP.clear()

    SCANNER_THREAD = threading.Thread(
        target=scanner_loop,
        name="freedom-live-scanner",
        daemon=True
    )

    SCANNER_THREAD.start()

    return True


# ============================================================
# STOP SCANNER
# ============================================================

def stop_scanner():

    SCANNER_STOP.set()

    return True


# ============================================================
# API - HOME
# ============================================================

@app.route("/")
def home():

    return (
        "Freedom Structure Scanner V4 "
        "is running ✅"
    )


# ============================================================
# API - HEALTH
# ============================================================

@app.route("/health")
def health():

    config = scanner_settings()

    return jsonify(
        {
            "ok": True,

            "engine":
                "Freedom Structure Scanner V4",

            "memory":
                True,

            "chronological_locking":
                True,

            "telegram_configured":
                bool(
                    TELEGRAM_BOT_TOKEN
                ),

            "continuous_scanner":
                bool(
                    SCANNER_THREAD
                    and
                    SCANNER_THREAD.is_alive()
                )
                and not SCANNER_STOP.is_set(),

            "scanner_pairs":
                config["pairs"],

            "scanner_timeframes":
                config["timeframes"],

            "scanner_interval_seconds":
                config["interval"],

            "timeframes":
                list(TIMEFRAME_MAP),
        }
    )


# ============================================================
# API - OHLC
# ============================================================

@app.route("/ohlc")
def ohlc():

    pair = request.args.get(
        "pair",
        ""
    ).upper()

    timeframe = request.args.get(
        "timeframe",
        "M15"
    ).upper()

    try:
        count = int(
            request.args.get(
                "count",
                500
            )
        )
    except ValueError:
        return jsonify(
            {
                "error":
                    "Invalid count"
            }
        ), 400

    if timeframe not in TIMEFRAME_MAP:

        return jsonify(
            {
                "error":
                    "Unsupported timeframe",

                "supported":
                    list(TIMEFRAME_MAP),
            }
        ), 400

    try:

        raw = get_candles(
            SYMBOL_MAP.get(
                pair,
                pair
            ),

            TIMEFRAME_MAP[
                timeframe
            ],

            count
        )

        candles = closed_candles(
            normalize(raw),
            TIMEFRAME_MAP[
                timeframe
            ]
        )

        return jsonify(
            candles
        )

    except Exception as exc:

        return jsonify(
            {
                "error":
                    str(exc)
            }
        ), 502


# ============================================================
# API - MANUAL SCAN
# ============================================================

@app.route("/scan")
def scan():

    pair = request.args.get(
        "pair",
        ""
    ).upper()

    timeframe = request.args.get(
        "timeframe",
        "M15"
    ).upper()

    try:
        count = int(
            request.args.get(
                "count",
                500
            )
        )

        strength = int(
            request.args.get(
                "strength",
                3
            )
        )

        min_atr = float(
            request.args.get(
                "min_atr_move",
                0.25
            )
        )

        exp_atr = float(
            request.args.get(
                "min_expansion_atr",
                0.5
            )
        )

        disp_atr = float(
            request.args.get(
                "displacement_atr",
                1.0
            )
        )

    except ValueError:

        return jsonify(
            {
                "error":
                    "Invalid numeric parameter"
            }
        ), 400

    bos = request.args.get(
        "bos",
        "body"
    ).lower()

    if bos not in (
        "body",
        "wick"
    ):
        bos = "body"

    if timeframe not in TIMEFRAME_MAP:

        return jsonify(
            {
                "error":
                    "Unsupported timeframe",

                "supported":
                    list(TIMEFRAME_MAP),
            }
        ), 400

    try:

        raw = get_candles(
            SYMBOL_MAP.get(
                pair,
                pair
            ),

            TIMEFRAME_MAP[
                timeframe
            ],

            count
        )

        candles = closed_candles(
            normalize(raw),
            TIMEFRAME_MAP[
                timeframe
            ]
        )

        minimum_required = max(
            50,
            strength * 4
        )

        if len(candles) < minimum_required:

            return jsonify(
                {
                    "error":
                        "Not enough candles returned",

                    "candles":
                        len(candles),
                }
            ), 502

        result = run_scan(
            pair,
            timeframe,
            candles,

            strength,
            bos,
            min_atr,
            exp_atr,
            disp_atr
        )

        return jsonify(
            result
        )

    except Exception as exc:

        return jsonify(
            {
                "error":
                    str(exc)
            }
        ), 502


# ============================================================
# API - SCANNER STATUS
# ============================================================

@app.route("/scanner/status")
def scanner_status():

    config = scanner_settings()

    with DIAGNOSTIC_LOCK:

        diagnostics = {
            "last_check":
                dict(
                    SCANNER_DIAGNOSTICS[
                        "last_check"
                    ]
                ),

            "last_success":
                dict(
                    SCANNER_DIAGNOSTICS[
                        "last_success"
                    ]
                ),

            "last_error":
                dict(
                    SCANNER_DIAGNOSTICS[
                        "last_error"
                    ]
                ),

            "scan_count":
                dict(
                    SCANNER_DIAGNOSTICS[
                        "scan_count"
                    ]
                ),
        }

    return jsonify(
        {
            "running":
                bool(
                    SCANNER_THREAD
                    and
                    SCANNER_THREAD.is_alive()
                )
                and not SCANNER_STOP.is_set(),

            "enabled":
                config["enabled"],

            "pairs":
                config["pairs"],

            "timeframes":
                config["timeframes"],

            "interval_seconds":
                config["interval"],

            "candle_count":
                config["count"],

            "last_processed":
                dict(SCANNER_LAST),

            "diagnostics":
                diagnostics,
        }
    )


# ============================================================
# API - START SCANNER
# ============================================================

@app.route(
    "/scanner/start",
    methods=["POST"]
)
def scanner_start():

    if (
        ADMIN_KEY
        and
        request.headers.get(
            "X-Admin-Key"
        ) != ADMIN_KEY
    ):

        return jsonify(
            {
                "error":
                    "unauthorized"
            }
        ), 401

    started = start_scanner()

    return jsonify(
        {
            "ok": True,

            "started":
                started,

            "message":
                (
                    "Continuous scanner started"
                    if started
                    else
                    "Continuous scanner was already running"
                ),
        }
    )


# ============================================================
# API - STOP SCANNER
# ============================================================

@app.route(
    "/scanner/stop",
    methods=["POST"]
)
def scanner_stop():

    if (
        not ADMIN_KEY
        or
        request.headers.get(
            "X-Admin-Key"
        ) != ADMIN_KEY
    ):

        return jsonify(
            {
                "error":
                    "unauthorized"
            }
        ), 401

    stop_scanner()

    return jsonify(
        {
            "ok": True,

            "message":
                "Continuous scanner stopping",
        }
    )


# ============================================================
# API - STRUCTURES
# ============================================================

@app.route("/structures")
def structures():

    pair = request.args.get(
        "pair"
    )

    timeframe = request.args.get(
        "timeframe"
    )

    sql = (
        "SELECT * FROM structures"
    )

    arguments = []

    where = []

    if pair:

        where.append(
            "pair = ?"
        )

        arguments.append(
            pair.upper()
        )

    if timeframe:

        where.append(
            "timeframe = ?"
        )

        arguments.append(
            timeframe.upper()
        )

    if where:

        sql += (
            " WHERE "
            + " AND ".join(where)
        )

    sql += (
        " ORDER BY updated_epoch DESC "
        "LIMIT 300"
    )

    with DB_LOCK:

        connection = db()

        rows = connection.execute(
            sql,
            arguments
        ).fetchall()

        connection.close()

    return jsonify(
        [
            row_to_structure(row)
            for row in rows
        ]
    )


# ============================================================
# TELEGRAM REGISTER
# ============================================================

@app.route(
    "/telegram/register",
    methods=["POST"]
)
def telegram_register():

    data = request.get_json(
        force=True
    )

    chat_id = data.get(
        "chat_id"
    )

    if chat_id is None:

        return jsonify(
            {
                "error":
                    "chat_id required"
            }
        ), 400

    username = data.get(
        "username",
        ""
    )

    with DB_LOCK:

        connection = db()

        connection.execute(
            """
            INSERT INTO telegram_users(
                chat_id,
                username,
                active,
                created_epoch
            )
            VALUES(?, ?, 1, ?)

            ON CONFLICT(chat_id)
            DO UPDATE SET
                username = excluded.username,
                active = 1
            """,
            (
                str(chat_id),
                username,
                int(time.time())
            )
        )

        connection.commit()
        connection.close()

    return jsonify(
        {
            "ok": True,
            "chat_id": str(chat_id),
        }
    )


# ============================================================
# TELEGRAM USERS
# ============================================================

@app.route("/telegram/users")
def telegram_users():

    if (
        ADMIN_KEY
        and
        request.headers.get(
            "X-Admin-Key"
        ) != ADMIN_KEY
    ):

        return jsonify(
            {
                "error":
                    "unauthorized"
            }
        ), 401

    with DB_LOCK:

        connection = db()

        rows = connection.execute(
            """
            SELECT
                chat_id,
                username,
                active,
                created_epoch
            FROM telegram_users
            ORDER BY created_epoch DESC
            """
        ).fetchall()

        connection.close()

    return jsonify(
        [
            dict(row)
            for row in rows
        ]
    )


# ============================================================
# TELEGRAM BROADCAST
# ============================================================

@app.route(
    "/telegram/broadcast",
    methods=["POST"]
)
def telegram_broadcast():

    if (
        ADMIN_KEY
        and
        request.headers.get(
            "X-Admin-Key"
        ) != ADMIN_KEY
    ):

        return jsonify(
            {
                "error":
                    "unauthorized"
            }
        ), 401

    data = request.get_json(
        force=True
    )

    text = data.get(
        "text",
        ""
    )

    with DB_LOCK:

        connection = db()

        users = connection.execute(
            """
            SELECT chat_id
            FROM telegram_users
            WHERE active = 1
            """
        ).fetchall()

        connection.close()

    sent = 0

    for user in users:

        if tg_send(
            user["chat_id"],
            text
        ):
            sent += 1

    return jsonify(
        {
            "targeted":
                len(users),

            "sent":
                sent,
        }
    )


# ============================================================
# ADMIN RESET
# ============================================================

@app.route(
    "/admin/reset",
    methods=["POST"]
)
def admin_reset():

    if (
        not ADMIN_KEY
        or
        request.headers.get(
            "X-Admin-Key"
        ) != ADMIN_KEY
    ):

        return jsonify(
            {
                "error":
                    "unauthorized"
            }
        ), 401

    pair = request.args.get(
        "pair"
    )

    timeframe = request.args.get(
        "timeframe"
    )

    with DB_LOCK:

        connection = db()

        if pair and timeframe:

            connection.execute(
                """
                DELETE FROM structures
                WHERE pair = ?
                  AND timeframe = ?
                """,
                (
                    pair.upper(),
                    timeframe.upper()
                )
            )

        elif pair:

            connection.execute(
                """
                DELETE FROM structures
                WHERE pair = ?
                """,
                (
                    pair.upper(),
                )
            )

        else:

            connection.execute(
                "DELETE FROM structures"
            )

        connection.commit()
        connection.close()

    # Clear scanner's last candle state too.
    if pair and timeframe:

        SCANNER_LAST.pop(
            diagnostic_key(
                pair.upper(),
                timeframe.upper()
            ),
            None
        )

    elif pair:

        prefix = (
            pair.upper()
            + "|"
        )

        for key in list(
            SCANNER_LAST.keys()
        ):

            if key.startswith(prefix):

                SCANNER_LAST.pop(
                    key,
                    None
                )

    else:

        SCANNER_LAST.clear()

    return jsonify(
        {
            "ok": True,

            "message":
                "Structure memory reset",
        }
    )


# ============================================================
# START SCANNER AUTOMATICALLY
# ============================================================

if (
    os.environ.get(
        "SCANNER_ENABLED",
        "true"
    ).lower()
    in (
        "1",
        "true",
        "yes",
        "on",
    )
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
                8080
            )
        )
    )
