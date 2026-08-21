import codecs
import encodings
import json
import os
import psycopg2
import psycopg2.extras
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

def now_utc_string():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

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

DERIV_APP_ID = os.environ.get("DERIV_APP_ID", "1089")
DERIV_WS_URL = (
    "wss://ws.derivws.com/websockets/v3"
    f"?app_id={DERIV_APP_ID}"
)

TELEGRAM_BOT_TOKEN = os.environ.get(
    "TELEGRAM_BOT_TOKEN",
    "",
)

ADMIN_KEY = os.environ.get(
    "ADMIN_KEY",
    "",
)

DEFAULT_SCANNER_INTERVAL = 120
DEFAULT_CANDLE_COUNT = 1000
# Maximum active structures tracked per direction per pair/timeframe
MAX_STRUCTURES_PER_DIRECTION = int(
    os.environ.get("MAX_STRUCTURES_PER_DIRECTION", "3")
)
# G must reach at least this percentage of the B -> E range.
# 0.90 means 90%, while touching or exceeding E is stronger.
G_MIN_REACH = max(
    0.50,
    min(
        1.20,
        float(
            os.environ.get(
                "SCANNER_G_MIN_REACH",
                "0.90",
            )
        ),
    ),
)

TRADE_SL_ATR_MULTIPLIER = max(
    0.0,
    float(
        os.environ.get(
            "TRADE_SL_ATR_MULTIPLIER",
            "1.0",
        )
    ),
)

TRADE_TP3_EXTENSION = max(
    0.0,
    float(
        os.environ.get(
            "TRADE_TP3_EXTENSION",
            "0.15",
        )
    ),
)

# A-zone uses A candle low/body for bullish and body/high for bearish.
# Optional ATR expansion around the zone.
A_ZONE_ATR_BUFFER = max(
    0.0,
    float(
        os.environ.get(
            "A_ZONE_ATR_BUFFER",
            "0.0",
        )
    ),
)


TIMEFRAME_MAP = {
    "M1": 60,
    "M5": 300,
    "M15": 900,
    "M30": 1800,
    "H1": 3600,
    "H2": 7200,
    "H4": 14400,
    "H8": 28800,
    "H24": 86400,
}


SYMBOL_MAP = {
    # Volatility indices
    "R_10": "R_10",
    "R_25": "R_25",
    "R_50": "R_50",
    "R_75": "R_75",
    "R_100": "R_100",
    "1HZ10V": "1HZ10V",
    "1HZ25V": "1HZ25V",
    "1HZ50V": "1HZ50V",
    "1HZ75V": "1HZ75V",
    "1HZ100V": "1HZ100V",
    "1HZ200V": "1HZ200V",
    "1HZ300V": "1HZ300V",

    "V10": "R_10",
    "V25": "R_25",
    "V50": "R_50",
    "V75": "R_75",
    "V100": "R_100",
    "1HZ10V": "1HZ10V",
    "1HZ25V": "1HZ25V",
    "1HZ50V": "1HZ50V",
    "1HZ75V": "1HZ75V",
    "1HZ100V": "1HZ100V",
    "1HZ200V": "1HZ200V",
    "1HZ300V": "1HZ300V",

    "VOLATILITY10": "R_10",
    "VOLATILITY25": "R_25",
    "VOLATILITY50": "R_50",
    "VOLATILITY75": "R_75",
    "VOLATILITY100": "R_100",
    "VOLATILITY101S": "1HZ10V",
    "VOLATILITY251S": "1HZ25V",
    "VOLATILITY501S": "1HZ50V",
    "VOLATILITY751S": "1HZ75V",
    "VOLATILITY1001S": "1HZ100V",
    "VOLATILITY2001S": "1HZ200V",
    "VOLATILITY3001S": "1HZ300V",

    # Boom and Crash
    "BOOM50": "BOOM50",
    "BOOM150N": "BOOM150N",
    "BOOM300N": "BOOM300N",
    "BOOM500": "BOOM500",
    "BOOM600": "BOOM600",
    "BOOM900": "BOOM900",
    "BOOM1000": "BOOM1000",
    "CRASH50": "CRASH50",
    "CRASH150N": "CRASH150N",
    "CRASH300N": "CRASH300N",
    "CRASH500": "CRASH500",
    "CRASH600": "CRASH600",
    "CRASH900": "CRASH900",
    "CRASH1000": "CRASH1000",


    # Step and Jump Indices
    "STPRNG": "STPRNG",
    "STPRNG2": "STPRNG2",
    "STPRNG3": "STPRNG3",
    "STPRNG4": "STPRNG4",
    "STPRNG5": "STPRNG5",
    "JD10": "JD10",
    "JD25": "JD25",
    "JD50": "JD50",
    "JD75": "JD75",
    "JD100": "JD100",
    
    # Forex
    "NZDJPY": "frxNZDJPY",
    "NZDUSD": "frxNZDUSD",
    "GBPCHF": "frxGBPCHF",
    "GBPCAD": "frxGBPCAD",
    "EURNZD": "frxEURNZD",
    "AUDNZD": "frxAUDNZD",
    "AUDCHF": "frxAUDCHF",
    "AUDCAD": "frxAUDCAD",
    "GBPJPY": "frxGBPJPY",
    "GBPAUD": "frxGBPAUD",
    "EURAUD": "frxEURAUD",
    "EURUSD": "frxEURUSD",
    "GBPUSD": "frxGBPUSD",
    "USDJPY": "frxUSDJPY",
    "AUDUSD": "frxAUDUSD",
    "USDCAD": "frxUSDCAD",
    "USDCHF": "frxUSDCHF",
    "EURGBP": "frxEURGBP",
    "EURCHF": "frxEURCHF",
    "AUDJPY": "frxAUDJPY",
    "EURCAD": "frxEURCAD",
    "CHFJPY": "frxCHFJPY",
    "EURJPY": "frxEURJPY",
    "USDZAR": "frxUSDZAR",
    "USDTRY": "frxUSDTRY",
    "USDMXN": "frxUSDMXN",
    "USDSGD": "frxUSDSGD",

    "FRXNZDJPY": "frxNZDJPY",
    "FRXNZDUSD": "frxNZDUSD",
    "FRXGBPCHF": "frxGBPCHF",
    "FRXGBPCAD": "frxGBPCAD",
    "FRXEURNZD": "frxEURNZD",
    "FRXAUDNZD": "frxAUDNZD",
    "FRXAUDCHF": "frxAUDCHF",
    "FRXAUDCAD": "frxAUDCAD",
    "FRXGBPJPY": "frxGBPJPY",
    "FRXGBPAUD": "frxGBPAUD",
    "FRXEURAUD": "frxEURAUD",
    "FRXEURUSD": "frxEURUSD",
    "FRXGBPUSD": "frxGBPUSD",
    "FRXUSDJPY": "frxUSDJPY",
    "FRXAUDUSD": "frxAUDUSD",
    "FRXUSDCAD": "frxUSDCAD",
    "FRXUSDCHF": "frxUSDCHF",
    "FRXEURGBP": "frxEURGBP",
    "FRXEURCHF": "frxEURCHF",
    "FRXAUDJPY": "frxAUDJPY",
    "FRXEURCAD": "frxEURCAD",
    "FRXCHFJPY": "frxCHFJPY",
    "FRXEURJPY": "frxEURJPY",
    "FRXUSDZAR": "frxUSDZAR",
    "FRXUSDTRY": "frxUSDTRY",
    "FRXUSDMXN": "frxUSDMXN",
    "FRXUSDSGD": "frxUSDSGD",

    # Metals
    "XAUUSD": "frxXAUUSD",
    "GOLD": "frxXAUUSD",
    "FRXXAUUSD": "frxXAUUSD",
    "XPDUSD": "FRXXPDUSD",
    "XPTUSD": "FRXXPTUSD",
    "XAGUSD": "FRXXAGUSD",
    "SILVER": "FRXXAGUSD",
    "USOIL": "FRXUSOIL",
    "CRUDEOIL": "FRXUSOIL",
    "UKBRENT": "FRXUKBRENT",
    "BRENT": "FRXUKBRENT",
    "NGAS": "FRXNGAS",

    "XAGUSD": "frxXAGUSD",
    "SILVER": "frxXAGUSD",
    "FRXXAGUSD": "frxXAGUSD",
    "XPDUSD": "frxXPDUSD",
    "XPTUSD": "frxXPTUSD",
    "XAGUSD": "frxXAGUSD",
    "SILVER": "frxXAGUSD",
    "USOIL": "frxUSOIL",
    "CRUDEOIL": "frxUSOIL",
    "UKBRENT": "frxUKBRENT",
    "BRENT": "frxUKBRENT",
    "NGAS": "frxNGAS",

    # Stock Indices
    "OTC_NDX": "OTC_NDX",
    "OTC_HSI": "OTC_HSI",
    "OTC_SX5E": "OTC_SX5E",
    "OTC_FCHI": "OTC_FCHI",
    "OTC_AEX": "OTC_AEX",
    "OTC_SSMI": "OTC_SSMI",
    "OTC_DJI": "OTC_DJI",
    "OTC_SPC": "OTC_SPC",
    "OTC_FTSE": "OTC_FTSE",
    "OTC_GDAXI": "OTC_GDAXI",
    "OTC_N225": "OTC_N225",
    "OTC_AS51": "OTC_AS51",

    # Cryptocurrencies
    "cryBTCUSD": "cryBTCUSD",
    "cryETHUSD": "cryETHUSD",

    "CRYBTCUSD": "cryBTCUSD",
    "CRYETHUSD": "cryETHUSD",
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

# Require confluence on another timeframe before sending G alert.
# Set to False to disable confluence requirement.
REQUIRE_CONFLUENCE = os.environ.get(
    "REQUIRE_CONFLUENCE", "true"
).lower() in ("1", "true", "yes", "on")

# Minimum stage rank on another timeframe to count as confluence.
# 1 = has D (BOS confirmed)
# 2 = has E (expansion confirmed)
# 3 = has F (retracement confirmed)
CONFLUENCE_MIN_RANK = max(
    0,
    int(os.environ.get("CONFLUENCE_MIN_RANK", "1")),
)


# ============================================================
# DATABASE PATH (optimized for free Render)
# ============================================================

DATABASE_URL = os.environ.get("DATABASE_URL", "")
DB_IS_PERSISTENT = bool(DATABASE_URL)

if DB_IS_PERSISTENT:
    print(f"[DB] Using PostgreSQL database")
else:
    print("[DB] WARNING: No DATABASE_URL set. Using in-memory fallback.")


# ============================================================
# SYMBOL HELPERS
# ============================================================

def canonical_symbol(value):
    if value is None:
        return ""

    raw = str(value).strip()

    if not raw:
        return ""

    upper = raw.upper()

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


def unique_values(values):
    result = []
    seen = set()

    for value in values:
        if value and value not in seen:
            seen.add(value)
            result.append(value)

    return result


def split_csv(value):
    if not value:
        return []

    return [
        item.strip()
        for item in str(value).split(",")
        if item.strip()
    ]


# ============================================================
# TIME HELPERS
# ============================================================

from zoneinfo import ZoneInfo   # add this import at the top with the other imports

def epoch_to_local(epoch):
    """Convert epoch to the timezone set in TELEGRAM_TIMEZONE"""
    try:
        tz = ZoneInfo(TELEGRAM_TIMEZONE)
    except Exception:
        tz = timezone.utc

    moment = datetime.fromtimestamp(int(epoch), tz=tz)
    return moment.strftime("%Y-%m-%d %H:%M:%S %Z")


# ============================================================
# DATABASE (hardened for free Render)
# ============================================================

def delete_database_files():
    # PostgreSQL — nothing to delete locally.
    # Tables are dropped and recreated via create_schema.
    print("[DB] PostgreSQL in use — no local files to delete.")


def create_schema(connection):
    cursor = connection.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS structures(
            id SERIAL PRIMARY KEY,
            structure_key TEXT UNIQUE NOT NULL,
            pair TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            direction TEXT NOT NULL,
            state TEXT NOT NULL,
            stage TEXT DEFAULT '',
            context_only INTEGER DEFAULT 0,
            a_json TEXT,
            b_json TEXT,
            c_json TEXT,
            d_json TEXT,
            e_json TEXT,
            f_json TEXT,
            g_json TEXT,
            fib50 REAL,
            a_zone_low REAL,
            a_zone_high REAL,
            entry_price REAL,
            entry_epoch INTEGER DEFAULT 0,
            valid INTEGER DEFAULT 0,
            telegram_sent INTEGER DEFAULT 0,
            created_epoch INTEGER,
            updated_epoch INTEGER,
            live_from_epoch INTEGER DEFAULT 0,
            discard_reason TEXT DEFAULT ''
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS telegram_users(
            chat_id TEXT PRIMARY KEY,
            username TEXT DEFAULT '',
            active INTEGER DEFAULT 1,
            created_epoch INTEGER
        )
        """
    )

    cursor.execute(
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

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS telegram_trade_alerts(
            id SERIAL PRIMARY KEY,
            structure_key TEXT NOT NULL,
            chat_id TEXT NOT NULL,
            pair TEXT,
            timeframe TEXT,
            direction TEXT,
            f_message_id INTEGER DEFAULT 0,
            g_message_id INTEGER DEFAULT 0,
            active_message_id INTEGER DEFAULT 0,
            plan_json TEXT,
            trade_state TEXT DEFAULT 'WAITING_FOR_G',
            last_event TEXT DEFAULT '',
            created_epoch INTEGER,
            updated_epoch INTEGER,
            UNIQUE(structure_key, chat_id)
        )
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS
        idx_structures_pair_timeframe
        ON structures(pair, timeframe)
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS
        idx_structures_state
        ON structures(state)
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS
        idx_trade_alerts_state
        ON telegram_trade_alerts(trade_state)
        """
    )

    connection.commit()
    cursor.close()


def open_database():
    if not DATABASE_URL:
        raise RuntimeError(
            "No DATABASE_URL configured."
        )

    connection = psycopg2.connect(
        DATABASE_URL,
        connect_timeout=30,
        cursor_factory=psycopg2.extras.RealDictCursor,
    )

    connection.autocommit = False
    create_schema(connection)
    return connection


def db():
    connection = None
    try:
        connection = open_database()
        return connection
    except Exception as exc:
        print(f"[DB] Database error: {exc}")
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass
        raise


# ============================================================
# SERIALIZATION
# ============================================================

def json_value(value):
    if value is None:
        return None

    return json.dumps(
        value,
        separators=(",", ":"),
    )


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


def set_stage(structure, stage):
    structure["stage"] = stage
    structure["state"] = stage
    return structure


def structure_a_zone(structure, candles=None):
    """
    Bullish:
        A low to A candle body high

    Bearish:
        A candle body low to A high
    """

    if structure.get("a_zone_low") is not None:
        return (
            float(structure["a_zone_low"]),
            float(structure["a_zone_high"]),
        )

    a = structure["a"]

    candle_low = float(a["low"])
    candle_high = float(a["high"])
    candle_body_low = min(
        float(a["open"]),
        float(a["close"]),
    )
    candle_body_high = max(
        float(a["open"]),
        float(a["close"]),
    )

    if structure["direction"] == "BULLISH":
        zone_low = candle_low
        zone_high = candle_body_high
    else:
        zone_low = candle_body_low
        zone_high = candle_high

    # Optional ATR zone buffer.
    if candles and A_ZONE_ATR_BUFFER > 0:
        index = None

        for position, item in enumerate(candles):
            if int(item["epoch"]) == int(a["epoch"]):
                index = position
                break

        if index is not None:
            atr_values = atrs(candles)
            local_atr = atr_values[index]

            if local_atr:
                buffer = local_atr * A_ZONE_ATR_BUFFER

                if structure["direction"] == "BULLISH":
                    zone_low -= buffer
                else:
                    zone_high += buffer

    return (
        float(zone_low),
        float(zone_high),
    )


def apply_zone_to_structure(structure, candles=None):
    zone_low, zone_high = structure_a_zone(
        structure,
        candles,
    )

    structure["a_zone_low"] = zone_low
    structure["a_zone_high"] = zone_high

    return structure


def structure_to_row_values(structure):
    now = int(time.time())

    return {
        "structure_key": structure_key(structure),
        "pair": structure["pair"],
        "timeframe": structure["timeframe"],
        "direction": structure["direction"],
        "state": structure.get(
            "state",
            structure.get("stage", "WAITING_FOR_BOS"),
        ),
        "stage": structure.get(
            "stage",
            structure.get("state", "WAITING_FOR_BOS"),
        ),
        "context_only": int(
            bool(structure.get("context_only", False))
        ),
        "a_json": json_value(structure.get("a")),
        "b_json": json_value(structure.get("b")),
        "c_json": json_value(structure.get("c")),
        "d_json": json_value(structure.get("d")),
        "e_json": json_value(structure.get("e")),
        "f_json": json_value(structure.get("f")),
        "g_json": json_value(structure.get("g")),
        "fib50": structure.get("fib50"),
        "a_zone_low": structure.get("a_zone_low"),
        "a_zone_high": structure.get("a_zone_high"),
        "entry_price": structure.get("entry_price"),
        "entry_epoch": int(
            structure.get("entry_epoch", 0)
        ),
        "valid": int(bool(structure.get("valid"))),
        "telegram_sent": int(
            bool(structure.get("telegram_sent"))
        ),
        "created_epoch": int(
            structure.get(
                "created_epoch",
                structure["a"]["epoch"],
            )
        ),
        "updated_epoch": now,
        "live_from_epoch": int(
            structure.get("live_from_epoch", 0)
        ),
        "discard_reason": structure.get(
            "discard_reason",
            "",
        ),
    }


def save(structure):
    apply_zone_to_structure(structure)

    values = structure_to_row_values(structure)

    with DB_LOCK:
        connection = db()

        try:
            cursor = connection.cursor()
            cursor.execute(
                """
                INSERT INTO structures(
                    structure_key,
                    pair,
                    timeframe,
                    direction,
                    state,
                    stage,
                    context_only,
                    a_json,
                    b_json,
                    c_json,
                    d_json,
                    e_json,
                    f_json,
                    g_json,
                    fib50,
                    a_zone_low,
                    a_zone_high,
                    entry_price,
                    entry_epoch,
                    valid,
                    telegram_sent,
                    created_epoch,
                    updated_epoch,
                    live_from_epoch,
                    discard_reason
                )
                VALUES(
                    %(structure_key)s,
                    %(pair)s,
                    %(timeframe)s,
                    %(direction)s,
                    %(state)s,
                    %(stage)s,
                    %(context_only)s,
                    %(a_json)s,
                    %(b_json)s,
                    %(c_json)s,
                    %(d_json)s,
                    %(e_json)s,
                    %(f_json)s,
                    %(g_json)s,
                    %(fib50)s,
                    %(a_zone_low)s,
                    %(a_zone_high)s,
                    %(entry_price)s,
                    %(entry_epoch)s,
                    %(valid)s,
                    %(telegram_sent)s,
                    %(created_epoch)s,
                    %(updated_epoch)s,
                    %(live_from_epoch)s,
                    %(discard_reason)s
                )
                ON CONFLICT(structure_key) DO UPDATE SET
                    state=excluded.state,
                    stage=excluded.stage,
                    context_only=excluded.context_only,
                    a_json=excluded.a_json,
                    b_json=excluded.b_json,
                    c_json=excluded.c_json,
                    d_json=excluded.d_json,
                    e_json=excluded.e_json,
                    f_json=excluded.f_json,
                    g_json=excluded.g_json,
                    fib50=excluded.fib50,
                    a_zone_low=excluded.a_zone_low,
                    a_zone_high=excluded.a_zone_high,
                    entry_price=excluded.entry_price,
                    entry_epoch=excluded.entry_epoch,
                    valid=excluded.valid,
                    telegram_sent=CASE
                        WHEN structures.telegram_sent=1
                        THEN 1
                        ELSE excluded.telegram_sent
                    END,
                    updated_epoch=excluded.updated_epoch,
                    live_from_epoch=excluded.live_from_epoch,
                    discard_reason=excluded.discard_reason
                """,
                values,
            )

            connection.commit()
            cursor.close()

        except Exception:
            connection.rollback()
            raise

        finally:
            connection.close()


def parse_json_point(row, column):
    value = row[column]
    if not value:
        return None
    if isinstance(value, dict):
        return value
    return json.loads(value)


def row_to_structure(row):
    try:
        stage = row["stage"] or row["state"]
    except Exception:
        stage = row["state"]

    structure = {
        "id": row["id"],
        "structure_key": row["structure_key"],
        "pair": row["pair"],
        "timeframe": row["timeframe"],
        "direction": row["direction"],
        "state": row["state"],
        "stage": stage,
        "context_only": bool(row.get("context_only", 0)),
        "a": parse_json_point(row, "a_json"),
        "b": parse_json_point(row, "b_json"),
        "c": parse_json_point(row, "c_json"),
        "d": parse_json_point(row, "d_json"),
        "e": parse_json_point(row, "e_json"),
        "f": parse_json_point(row, "f_json"),
        "g": parse_json_point(row, "g_json"),
        "fib50": row["fib50"],
        "a_zone_low": row["a_zone_low"],
        "a_zone_high": row["a_zone_high"],
        "entry_price": row["entry_price"],
        "entry_epoch": int(row["entry_epoch"] or 0),
        "valid": bool(row["valid"]),
        "telegram_sent": bool(row["telegram_sent"]),
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
            cursor = connection.cursor()
            cursor.execute(
                """
                SELECT *
                FROM structures
                WHERE pair=%s
                AND timeframe=%s
                AND state != 'INVALID'
                ORDER BY created_epoch ASC
                """,
                (
                canonical_pair,
                canonical_tf,
                ),
        )
            rows = cursor.fetchall()
            cursor.close()

        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    return [
        row_to_structure(row)
        for row in rows
    ]


def delete_structure(structure):
    key = structure_key(structure)

    with DB_LOCK:
        connection = db()
        try:
            cursor = connection.cursor()
            cursor.execute(
                """
                DELETE FROM structures
                WHERE structure_key=%s
                """,
                (key,),
            )
            connection.commit()
            cursor.close()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()


def delete_structure_by_key(structure_key_value):
    with DB_LOCK:
        connection = db()
        try:
            cursor = connection.cursor()
            cursor.execute(
                "DELETE FROM structures WHERE structure_key=%s",
                (structure_key_value,),
            )
            connection.commit()
            cursor.close()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

def structure_for_key(structure_key_value):
    with DB_LOCK:
        connection = db()
        try:
            cursor = connection.cursor()
            cursor.execute(
                """
                SELECT *
                FROM structures
                WHERE structure_key=%s
                """,
                (structure_key_value,),
            )
            row = cursor.fetchone()
            cursor.close()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    if not row:
        return None

    return row_to_structure(row)


def close_structure_by_key(structure_key_value, reason):
    """
    Keep the row as CLOSED instead of deleting it.

    This is important: the scanner remembers the structure key,
    so it does not rediscover the same historical A -> G setup.
    """
    with DB_LOCK:
        connection = db()
        try:
            cursor = connection.cursor()
            cursor.execute(
                """
                UPDATE structures
                SET state='CLOSED',
                    stage='CLOSED',
                    discard_reason=%s,
                    updated_epoch=%s
                WHERE structure_key=%s
                """,
                (
                    reason,
                    int(time.time()),
                    structure_key_value,
                ),
            )
            connection.commit()
            cursor.close()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()


def close_trade_alerts_for_structure(
    structure_key_value,
    trade_state,
    reason,
):
    """
    Stop Telegram monitoring too.

    Without this, an old ACTIVE Telegram plan could still be
    checked for TP/SL after you manually close the setup.
    """
    with DB_LOCK:
        connection = db()
        try:
            cursor = connection.cursor()
            cursor.execute(
                """
                UPDATE telegram_trade_alerts
                SET trade_state=%s,
                    last_event=%s,
                    updated_epoch=%s
                WHERE structure_key=%s
                """,
                (
                    trade_state,
                    reason,
                    int(time.time()),
                    structure_key_value,
                ),
            )
            connection.commit()
            cursor.close()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()


# ============================================================
# DERIV CANDLES
# ============================================================

def get_candles(symbol, granularity, count=500):
    deriv_symbol = canonical_symbol(symbol)

    if not deriv_symbol:
        raise ValueError(
            f"Unsupported symbol: {symbol}"
        )

    result = []
    error_message = None
    done = threading.Event()
    open_error = [None]

    def on_message(ws, message):
        nonlocal result
        nonlocal error_message

        try:
            data = json.loads(message)

            if "candles" in data:
                result = data["candles"]
                done.set()

            elif "error" in data:
                error_message = data["error"].get(
                    "message",
                    str(data["error"]),
                )
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
            ws.send(
                json.dumps(
                    {
                        "ticks_history": deriv_symbol,
                        "adjust_start_time": 1,
                        "count": max(
                            100,
                            min(int(count), 1000),
                        ),
                        "granularity": int(granularity),
                        "style": "candles",
                        "end": "latest",
                    }
                )
            )

        except Exception as exc:
            open_error[0] = str(exc)
            done.set()

    socket = websocket.WebSocketApp(
        DERIV_WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        header={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Origin": "https://app.deriv.com",
        }
    )

    worker = threading.Thread(
        target=socket.run_forever,
        name=(
            f"deriv-ws-{deriv_symbol}-"
            f"{granularity}"
        ),
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
        raise TimeoutError(
            f"Deriv request timed out for {deriv_symbol}"
        )

    if error_message:
        raise RuntimeError(
            f"Deriv error for {deriv_symbol}: "
            f"{error_message}"
        )

    if not result:
        raise RuntimeError(
            f"Deriv returned no candles for {deriv_symbol}"
        )

    return sorted(
        result,
        key=lambda item: int(
            item.get("epoch", 0)
        ),
    )


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
        candle
        for candle in rows
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
                abs(
                    candle["high"]
                    - previous["close"]
                ),
                abs(
                    candle["low"]
                    - previous["close"]
                ),
            )

        true_ranges.append(true_range)

    output = [0.0] * len(candles)
    first_index = max(0, int(period) - 1)

    for index in range(first_index, len(candles)):
        start = max(
            0,
            index - int(period) + 1,
        )

        values = true_ranges[
            start:index + 1
        ]

        output[index] = sum(values) / len(values)

    return output


def swings(candles, strength=3):
    strength = max(1, int(strength))
    output = []

    for index in range(
        strength,
        len(candles) - strength,
    ):
        candle = candles[index]

        left = candles[
            index - strength:index
        ]

        right = candles[
            index + 1:index + strength + 1
        ]

        is_low = (
            all(
                candle["low"] < item["low"]
                for item in left
            )
            and all(
                candle["low"] <= item["low"]
                for item in right
            )
        )

        is_high = (
            all(
                candle["high"] > item["high"]
                for item in left
            )
            and all(
                candle["high"] >= item["high"]
                for item in right
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
        key=lambda item: item[1],
    )


def pivot_price(kind, candle):
    if kind == "L":
        return candle["low"]

    return candle["high"]


def significant_swings(
    candles,
    strength=3,
    min_atr_move=0.25,
):
    raw = swings(candles, strength)
    atr_values = atrs(candles)
    reduced = []

    for kind, index, candle in raw:
        price = pivot_price(kind, candle)

        if not reduced:
            reduced.append(
                (kind, index, candle)
            )
            continue

        (
            previous_kind,
            previous_index,
            previous_candle,
        ) = reduced[-1]

        previous_price = pivot_price(
            previous_kind,
            previous_candle,
        )

        if kind == previous_kind:
            more_extreme = (
                (
                    kind == "L"
                    and price < previous_price
                )
                or (
                    kind == "H"
                    and price > previous_price
                )
            )

            if more_extreme:
                reduced[-1] = (
                    kind,
                    index,
                    candle,
                )

            continue

        local_atr = (
            atr_values[index]
            or atr_values[previous_index]
        )

        movement = abs(
            price - previous_price
        )

        if (
            local_atr
            and movement
            < local_atr * float(min_atr_move)
        ):
            continue

        reduced.append(
            (kind, index, candle)
        )

    return reduced


def fibonacci_level(start, end, ratio=0.5):
    return float(start) + (float(end) - float(start)) * float(ratio)


# ============================================================
# A -> C -> B DISCOVERY
# ============================================================

def discover_abc(
    candles,
    pair,
    timeframe,
    direction,
    strength,
    min_atr,
    min_bars=1,
):
    pivots = significant_swings(
        candles,
        strength=strength,
        min_atr_move=min_atr,
    )

    atr_values = atrs(candles)
    output = []

    expected = (
        ("L", "H", "L")
        if direction == "BULLISH"
        else ("H", "L", "H")
    )

    for pivot_index in range(
        len(pivots) - 2
    ):
        (
            a_kind,
            a_index,
            a_candle,
        ) = pivots[pivot_index]

        (
            c_kind,
            c_index,
            c_candle,
        ) = pivots[pivot_index + 1]

        (
            b_kind,
            b_index,
            b_candle,
        ) = pivots[pivot_index + 2]

        if (
            a_kind,
            c_kind,
            b_kind,
        ) != expected:
            continue

        if (
            c_index - a_index
            < int(min_bars)
        ):
            continue

        if (
            b_index - c_index
            < int(min_bars)
        ):
            continue

        a_price = pivot_price(
            a_kind,
            a_candle,
        )

        c_price = pivot_price(
            c_kind,
            c_candle,
        )

        b_price = pivot_price(
            b_kind,
            b_candle,
        )

        if direction == "BULLISH":
            if b_price >= a_price:
                continue

            sweep_size = a_price - b_price

        else:
            if b_price <= a_price:
                continue

            sweep_size = b_price - a_price

        local_atr = (
            atr_values[b_index]
            or atr_values[a_index]
        )

        if (
            local_atr
            and sweep_size
            < local_atr * float(min_atr)
        ):
            continue

        if (
            local_atr
            and abs(c_price - a_price)
            < local_atr * float(min_atr)
        ):
            continue

        structure = {
            "pair": canonical_symbol(pair),
            "timeframe": str(timeframe).upper(),
            "direction": direction,

            "a": point(
                "A",
                "INITIAL_SWING",
                a_candle,
                a_price,
            ),

            "b": point(
                "B",
                "STRUCTURAL_EXTREME",
                b_candle,
                b_price,
            ),

            "c": point(
                "C",
                "PREVIOUS_STRUCTURE",
                c_candle,
                c_price,
            ),

            "d": None,
            "e": None,
            "f": None,
            "g": None,

            "fib50": None,
            "a_zone_low": None,
            "a_zone_high": None,
            "entry_price": None,
            "entry_epoch": 0,

            "stage": "WAITING_FOR_BOS",
            "state": "WAITING_FOR_BOS",
            "context_only": True,
            "valid": False,
            "telegram_sent": False,
            "discard_reason": "",
            "live_from_epoch": 0,
        }

        apply_zone_to_structure(
            structure,
            candles,
        )

        output.append(structure)

    return output


# ============================================================
# ADVANCEMENT: D -> E -> F -> G
# ============================================================

def candle_breaks_level(
    candle,
    level,
    direction,
    bos_mode="body",
):
    if direction == "BULLISH":
        if bos_mode == "wick":
            return candle["high"] > level

        return candle["close"] > level

    if bos_mode == "wick":
        return candle["low"] < level

    return candle["close"] < level


def mark_invalid(structure, reason):
    structure["stage"] = "INVALID"
    structure["state"] = "INVALID"
    structure["valid"] = False
    structure["discard_reason"] = reason
    return structure


def advance_structure(
    structure,
    candles,
    bos_mode="body",
    expansion_atr=0.5,
    displacement_atr=1.0,
    swing_strength=3,
    min_atr_move=0.25,
    fib_ratio=0.5,
):
    if structure.get("stage") in (
        "ACTIVE",
        "CLOSED",
        "INVALID",
    ):
        return structure

    index_by_epoch = {
        candle["epoch"]: index
        for index, candle in enumerate(candles)
    }

    atr_values = atrs(candles)

    pivots = significant_swings(
        candles,
        strength=swing_strength,
        min_atr_move=min_atr_move,
    )

    direction = structure["direction"]

    b_index = index_by_epoch.get(
        structure["b"]["epoch"]
    )

    c_index = index_by_epoch.get(
        structure["c"]["epoch"]
    )

    if b_index is None or c_index is None:
        return mark_invalid(
            structure,
            "pivot_missing_from_candle_window",
        )

    b_level = structure["b"]["price"]
    c_level = structure["c"]["price"]

    # --------------------------------------------------------
    # D = BOS
    # --------------------------------------------------------

    if structure.get("d") is None:
        replacement_kind = (
            "L"
            if direction == "BULLISH"
            else "H"
        )

        for kind, index, candle in pivots:
            if index <= b_index:
                continue

            if kind != replacement_kind:
                continue

            replacement_price = pivot_price(
                kind,
                candle,
            )

            replaced = (
                replacement_price < b_level
                if direction == "BULLISH"
                else replacement_price > b_level
            )

            if replaced:
                return mark_invalid(
                    structure,
                    "new_structural_extreme_replaced_B",
                )

        for index in range(
            b_index + 1,
            len(candles),
        ):
            candle = candles[index]

            if candle_breaks_level(
                candle,
                c_level,
                direction,
                bos_mode,
            ):
                d_price = (
                    candle["high"]
                    if direction == "BULLISH"
                    else candle["low"]
                )

                structure["d"] = point(
                    "D",
                    (
                        "BODY_BOS"
                        if bos_mode == "body"
                        else "WICK_BOS"
                    ),
                    candle,
                    d_price,
                )

                set_stage(
                    structure,
                    "WAITING_FOR_E",
                )

                break

    if structure.get("d") is None:
        return structure

    d_index = index_by_epoch.get(
        structure["d"]["epoch"]
    )

    if d_index is None:
        return mark_invalid(
            structure,
            "D_missing_from_candle_window",
        )

    # --------------------------------------------------------
    # Protected B validation
    # --------------------------------------------------------

    for index in range(
        b_index + 1,
        len(candles),
    ):
        candle = candles[index]

        broke_b = (
            candle["low"] <= b_level
            if direction == "BULLISH"
            else candle["high"] >= b_level
        )

        if broke_b:
            return mark_invalid(
                structure,
                "protected_B_was_broken",
            )

    # --------------------------------------------------------
    # E = expansion
    # --------------------------------------------------------

    if structure.get("e") is None:
        wanted_kind = (
            "H"
            if direction == "BULLISH"
            else "L"
        )

        for kind, index, candle in pivots:
            if index <= d_index:
                continue

            if kind != wanted_kind:
                continue

            e_price = pivot_price(
                kind,
                candle,
            )

            expansion = (
                e_price - c_level
                if direction == "BULLISH"
                else c_level - e_price
            )

            local_atr = (
                atr_values[index]
                or atr_values[d_index]
            )

            if expansion <= 0:
                continue

            if (
                local_atr
                and expansion
                < local_atr
                * float(expansion_atr)
            ):
                continue

            structure["e"] = point(
                "E",
                "POST_BOS_EXPANSION",
                candle,
                e_price,
            )

            structure["fib50"] = fibonacci_level(
                structure["b"]["price"],
                structure["e"]["price"],
                fib_ratio,
            )

            set_stage(
                structure,
                "WAITING_FOR_F",
            )

            break

    if structure.get("e") is None:
        return structure

    e_index = index_by_epoch.get(
        structure["e"]["epoch"]
    )

    if e_index is None:
        return mark_invalid(
            structure,
            "E_missing_from_candle_window",
        )

    fib50 = structure["fib50"]

    # --------------------------------------------------------
    # F = first confirmed retracement beyond Fib 50
    #
    # F does not need to touch A-zone.
    # It must remain above B for bullish or below B for bearish.
    # --------------------------------------------------------

    if structure.get("f") is None:
        wanted_kind = (
            "L"
            if direction == "BULLISH"
            else "H"
        )

        for kind, index, candle in pivots:
            if index <= e_index:
                continue

            if kind != wanted_kind:
                continue

            f_price = pivot_price(
                kind,
                candle,
            )

            reaches_fib = (
                f_price <= fib50
                if direction == "BULLISH"
                else f_price >= fib50
            )

            if not reaches_fib:
                continue

            breaks_protected_b = (
                f_price <= b_level
                if direction == "BULLISH"
                else f_price >= b_level
            )

            if breaks_protected_b:
                return mark_invalid(
                    structure,
                    "F_broke_protected_B",
                )

            structure["f"] = point(
                "F",
                "FIB_RETRACEMENT",
                candle,
                f_price,
            )

            set_stage(
                structure,
                "WAITING_FOR_G",
            )

            break

    if structure.get("f") is None:
        return structure

    f_index = index_by_epoch.get(
        structure["f"]["epoch"]
    )

    if f_index is None:
        return mark_invalid(
            structure,
            "F_missing_from_candle_window",
        )

    # --------------------------------------------------------
    # G = strength push near or beyond 100%
    # --------------------------------------------------------
    if structure.get("g") is None:
        total_range = abs(
            structure["e"]["price"]
            - structure["b"]["price"]
        )

        if total_range <= 0:
            return mark_invalid(
                structure,
                "invalid_B_E_range",
            )

        if direction == "BULLISH":
            g_threshold = (
                structure["b"]["price"]
                + total_range * G_MIN_REACH
            )
            wanted_kind = "H"
        else:
            g_threshold = (
                structure["b"]["price"]
                - total_range * G_MIN_REACH
            )
            wanted_kind = "L"

        for kind, index, candle in pivots:
            if index <= f_index:
                continue

            if kind != wanted_kind:
                continue

            g_price = pivot_price(
                kind,
                candle,
            )

            reaches_g = (
                g_price >= g_threshold
                if direction == "BULLISH"
                else g_price <= g_threshold
            )

            if not reaches_g:
                continue

            structure["g"] = point(
                "G",
                "STRENGTH_PUSH",
                candle,
                g_price,
            )

            # G is confirmed. Now wait for a closed A-zone entry.
            set_stage(
                structure,
                "WAITING_FOR_ENTRY",
            )

            break

    return structure


# ============================================================
# TRADE PLAN CALCULATIONS
# ============================================================

def find_candle_index(candles, epoch):
    for index, candle in enumerate(candles):
        if int(candle["epoch"]) == int(epoch):
            return index

    return None


def sl_buffer_for(structure, candles):
    b_index = find_candle_index(
        candles,
        structure["b"]["epoch"],
    )

    atr_values = atrs(candles)

    if (
        b_index is not None
        and b_index < len(atr_values)
        and atr_values[b_index] > 0
    ):
        return (
            atr_values[b_index]
            * TRADE_SL_ATR_MULTIPLIER
        )

    return abs(
        structure["e"]["price"]
        - structure["b"]["price"]
    ) * 0.05


def pending_trade_plan(structure, candles):
    zone_low, zone_high = structure_a_zone(
        structure,
        candles,
    )

    buffer = sl_buffer_for(
        structure,
        candles,
    )

    if structure["direction"] == "BULLISH":
        stop_loss = structure["b"]["price"] - buffer
    else:
        stop_loss = structure["b"]["price"] + buffer

    return {
        "structure_key": structure_key(structure),
        "pair": structure["pair"],
        "timeframe": structure["timeframe"],
        "direction": structure["direction"],
        "status": "WAITING_FOR_PULLBACK",
        "entry_zone_low": round(zone_low, 5),
        "entry_zone_high": round(zone_high, 5),
        "stop_loss": round(stop_loss, 5),
        "protected_B": round(
            structure["b"]["price"],
            5,
        ),
        "f_price": round(
            structure["f"]["price"],
            5,
        ),
        "g_price": round(
            structure["g"]["price"],
            5,
        ),
        "created_utc": now_utc_string(),
    }


def active_trade_plan(structure, candles):
    entry = float(
        structure["entry_price"]
    )

    zone_low, zone_high = structure_a_zone(
        structure,
        candles,
    )

    buffer = sl_buffer_for(
        structure,
        candles,
    )

    if structure["direction"] == "BULLISH":
        stop_loss = structure["b"]["price"] - buffer
    else:
        stop_loss = structure["b"]["price"] + buffer

    candidates = [
        (
            "D",
            float(structure["d"]["price"]),
        ),
        (
            "E",
            float(structure["e"]["price"]),
        ),
        (
            "G",
            float(structure["g"]["price"]),
        ),
    ]

    if structure["direction"] == "BULLISH":
        valid_targets = [
            item
            for item in candidates
            if item[1] > entry
        ]

        valid_targets.sort(
            key=lambda item: item[1]
        )

    else:
        valid_targets = [
            item
            for item in candidates
            if item[1] < entry
        ]

        valid_targets.sort(
            key=lambda item: item[1],
            reverse=True,
        )

    target_prices = []

    for name, price in valid_targets:
        if not target_prices:
            target_prices.append(price)
            continue

        if abs(price - target_prices[-1]) > 0.000001:
            target_prices.append(price)

    range_be = abs(
        structure["e"]["price"]
        - structure["b"]["price"]
    )

    if not target_prices:
        if structure["direction"] == "BULLISH":
            target_prices = [
                entry + range_be * 0.5,
                entry + range_be,
            ]
        else:
            target_prices = [
                entry - range_be * 0.5,
                entry - range_be,
            ]

    tp1 = target_prices[0]

    if len(target_prices) >= 2:
        tp2 = target_prices[1]
    else:
        if structure["direction"] == "BULLISH":
            tp2 = tp1 + range_be * 0.5
        else:
            tp2 = tp1 - range_be * 0.5

    if structure["direction"] == "BULLISH":
        tp3 = tp2 + (
            range_be * TRADE_TP3_EXTENSION
        )
    else:
        tp3 = tp2 - (
            range_be * TRADE_TP3_EXTENSION
        )

    risk = abs(entry - stop_loss)

    rr1 = (
        abs(tp1 - entry) / risk
        if risk > 0
        else 0.0
    )

    rr2 = (
        abs(tp2 - entry) / risk
        if risk > 0
        else 0.0
    )

    rr3 = (
        abs(tp3 - entry) / risk
        if risk > 0
        else 0.0
    )

    return {
        "structure_key": structure_key(structure),
        "pair": structure["pair"],
        "timeframe": structure["timeframe"],
        "direction": structure["direction"],
        "status": "ACTIVE",
        "entry_zone_low": round(zone_low, 5),
        "entry_zone_high": round(zone_high, 5),
        "entry": round(entry, 5),
        "stop_loss": round(stop_loss, 5),
        "protected_B": round(
            structure["b"]["price"],
            5,
        ),
        "tp1": round(tp1, 5),
        "tp2": round(tp2, 5),
        "tp3": round(tp3, 5),
        "rr_tp1": round(rr1, 2),
        "rr_tp2": round(rr2, 2),
        "rr_tp3": round(rr3, 2),
        "entry_epoch": int(
            structure["entry_epoch"]
        ),
        "entry_time_utc": epoch_to_local(
            structure["entry_epoch"]
        ),
        "created_utc": now_utc_string(),
        "tp1_hit": False,
        "tp2_hit": False,
        "tp3_hit": False,
        "sl_hit": False,
        "trade_state": "ACTIVE",
    }


# ============================================================
# CLOSED A-ZONE ENTRY CONFIRMATION
# ============================================================

def confirm_closed_entry(structure, candles):
    """
    After G:
    - confirm a live A-zone entry
    - or discard the setup if history already used it
    """
    if structure.get("stage") != "WAITING_FOR_ENTRY":
        return False

    if not structure.get("g"):
        return False

    zone_low, zone_high = structure_a_zone(
        structure,
        candles,
    )
    g_epoch = int(structure["g"]["epoch"])
    live_from = int(structure.get("live_from_epoch", 0) or 0)
    protected_b = float(structure["b"]["price"])
    direction = structure["direction"]

    def touches_zone(candle):
        return (
            float(candle["low"]) <= zone_high
            and float(candle["high"]) >= zone_low
        )

    def is_entry_candle(candle):
        if not touches_zone(candle):
            return False

        close = float(candle["close"])
        opened = float(candle["open"])

        # Wick into the zone is enough.
        # Candle must close in the expected direction.
        if direction == "BULLISH":
            return close > opened

        return close < opened

    def broke_protected_b(candle):
        if direction == "BULLISH":
            return float(candle["low"]) <= protected_b

        return float(candle["high"]) >= protected_b

    entry_candle = None
    touched_zone = False

    for candle in candles:
        candle_epoch = int(candle["epoch"])

        if candle_epoch <= g_epoch:
            continue

        if broke_protected_b(candle) and entry_candle is None:
            mark_invalid(
                structure,
                "protected_B_broken_before_entry",
            )
            return False

        if entry_candle is None:
            if touches_zone(candle):
                touched_zone = True

            if is_entry_candle(candle):
                entry_candle = candle

    if entry_candle is None:
        if touched_zone and setup_already_ran_to_targets(
            structure,
            candles,
            g_epoch,
            zone_low,
            zone_high,
        ):
            mark_invalid(
                structure,
                "missed_entry_targets_already_hit",
            )
            return False

        return False

    structure["entry_price"] = float(entry_candle["close"])
    structure["entry_epoch"] = int(entry_candle["epoch"])

    finished = False
    finish_reason = ""

    try:
        finished, finish_reason = trade_already_finished(
            structure,
            candles,
        )
    except Exception as exc:
        print(
            f"[Scanner] trade_already_finished error "
            f"in confirm_closed_entry: {exc}"
        )

    if finished:
        mark_invalid(
            structure,
            finish_reason,
        )
        return False

    set_stage(
        structure,
        "ACTIVE",
    )
    return True

    
def setup_already_ran_to_targets(
    structure,
    candles,
    g_epoch,
    zone_low,
    zone_high,
):
    """
    Price touched the A-zone after G, then already reached
    a target without producing a valid entry candle.
    """
    direction = structure["direction"]
    targets = []

    for key in ("d", "e", "g"):
        point_data = structure.get(key)

        if not point_data:
            continue

        price = float(point_data["price"])

        if direction == "BULLISH" and price > zone_high:
            targets.append(price)

        elif direction == "BEARISH" and price < zone_low:
            targets.append(price)

    if not targets:
        return False

    target = (
        min(targets)
        if direction == "BULLISH"
        else max(targets)
    )

    zone_was_touched = False

    for candle in candles:
        candle_epoch = int(candle["epoch"])

        if candle_epoch <= g_epoch:
            continue

        candle_low = float(candle["low"])
        candle_high = float(candle["high"])

        if (
            candle_low <= zone_high
            and candle_high >= zone_low
        ):
            zone_was_touched = True

        if not zone_was_touched:
            continue

        if (
            direction == "BULLISH"
            and candle_high >= target
        ):
            return True

        if (
            direction == "BEARISH"
            and candle_low <= target
        ):
            return True

    return False


def trade_already_finished(structure, candles):
    """
    Replay candles after entry and detect historical TP3 or SL.
    Returns: (finished, reason)
    """
    if not structure.get("entry_epoch"):
        return False, ""

    if not structure.get("g"):
        return False, ""

    if not structure.get("entry_price"):
        return False, ""

    try:
        plan = active_trade_plan(
            structure,
            candles,
        )
    except Exception:
        return False, ""

    entry_epoch = int(structure["entry_epoch"])
    direction = structure["direction"]

    try:
        stop_loss = float(plan["stop_loss"])
        tp3 = float(plan["tp3"])
    except (KeyError, TypeError, ValueError):
        return False, ""

    for candle in candles:
        candle_epoch = int(candle["epoch"])

        if candle_epoch <= entry_epoch:
            continue

        high = float(candle["high"])
        low = float(candle["low"])

        if direction == "BULLISH":
            if low <= stop_loss:
                return True, "historical_sl_already_hit"

            if high >= tp3:
                return True, "historical_tp_already_hit"

        else:
            if high >= stop_loss:
                return True, "historical_sl_already_hit"

            if low <= tp3:
                return True, "historical_tp_already_hit"

    return False, ""

# ============================================================
# TELEGRAM TEXT
# ============================================================

# Telegram time zone (use your local timezone name)
# Examples: "Europe/London", "Africa/Lagos", "America/New_York", "UTC"
TELEGRAM_TIMEZONE = os.environ.get("TELEGRAM_TIMEZONE", "UTC")

def f_message_text(structure):
    icon = "🔴" if structure["direction"] == "BEARISH" else "🟢"

    def fmt(point):
        if not point:
            return "—"
        return (
            f"{point['label']} — {point['role']}\n"
            f"Price: {point['price']}\n"
            f"Candle time: {epoch_to_local(point['epoch'])}"
        )

    lines = [
        f"{icon} <b>{structure['direction']} A→F STRUCTURE CONFIRMED</b>",
        "",
        f"Symbol: <b>{structure['pair']}</b>",
        f"Timeframe: <b>{structure['timeframe']}</b>",
        f"Detected: <b>{epoch_to_local(int(time.time()))}</b>",
        "",
        "────────────────────",
        fmt(structure.get("a")),
        "",
        fmt(structure.get("c")),
        "",
        fmt(structure.get("b")),
        "",
        fmt(structure.get("d")),
        "",
        fmt(structure.get("e")),
        "",
        fmt(structure.get("f")),
        "────────────────────",
        "",
        f"Fibonacci 50%: <b>{structure.get('fib50')}</b>",
        "",
        "You can now draw the lines on your chart.",
        "Waiting for G strength push...",
        "",
        f"Message time: <b>{epoch_to_local(int(time.time()))}</b>",
    ]

    return "\n".join(lines)


def g_message_text(structure, candles, confluent_tf=None, confluent_stage=None):
    plan = pending_trade_plan(
        structure,
        candles,
    )

    icon = (
        "🟢"
        if structure["direction"] == "BULLISH"
        else "🔴"
    )

    confluence_line = (
        f"✅ Confluence: <b>{confluent_tf}</b> also at "
        f"<b>{confluent_stage}</b>"
        if confluent_tf
        else "✅ Confluence confirmed"
    )

    return "\n".join(
        [
            f"{icon} <b>A→G SETUP CONFIRMED</b>",
            "",
            f"Symbol: <b>{plan['pair']}</b>",
            f"Timeframe: <b>{plan['timeframe']}</b>",
            f"Direction: <b>{plan['direction']}</b>",
            f"G time: <b>{epoch_to_local(structure['g']['epoch'])}</b>",
            f"G price: <b>{plan['g_price']}</b>",
            "",
            confluence_line,
            "",
            f"Entry Zone: <b>{plan['entry_zone_low']} - {plan['entry_zone_high']}</b>",
            f"Stop Loss: <b>{plan['stop_loss']}</b>",
            f"Protected B: <b>{plan['protected_B']}</b>",
            "",
            "Status: WAITING FOR CLOSED PULLBACK",
            "",
            "A candle must touch the projected A-zone "
            "and close in the expected direction.",
            "TP will be calculated when the trade becomes active.",
            "",
            f"Message time: <b>{epoch_to_local(int(time.time()))}</b>",
        ]
    )

    

def active_message_text(structure, candles):
    plan = active_trade_plan(
        structure,
        candles,
    )

    icon = (
        "🟢"
        if structure["direction"] == "BULLISH"
        else "🔴"
    )

    return "\n".join(
        [
            f"{icon} <b>TRADE ACTIVE — ENTRY CONFIRMED</b>",
            "",
            f"Symbol: <b>{plan['pair']}</b>",
            f"Timeframe: <b>{plan['timeframe']}</b>",
            "",
            f"Entry: <b>{plan['entry']}</b>",
            f"Entry candle: <b>{plan['entry_time_utc']}</b>",
            f"Stop Loss: <b>{plan['stop_loss']}</b>",
            "",
            f"TP1: <b>{plan['tp1']}</b> | RR {plan['rr_tp1']}",
            f"TP2: <b>{plan['tp2']}</b> | RR {plan['rr_tp2']}",
            f"TP3: <b>{plan['tp3']}</b> | RR {plan['rr_tp3']}",
            "",
            f"Entry zone: {plan['entry_zone_low']} - {plan['entry_zone_high']}",
            "",
            f"Message time: <b>{epoch_to_local(int(time.time()))}</b>",
        ]
    )


def trade_event_text(plan, event, price, candle_epoch):
    if event == "SL":
        icon = "❌"          # red X
        title = "STOP LOSS HIT"
    else:
        icon = "✔️"          # green pass mark
        title = f"{event} HIT"

    return "\n".join(
        [
            f"{icon} <b>{title}</b>",
            "",
            f"Symbol: <b>{plan['pair']}</b>",
            f"Timeframe: <b>{plan['timeframe']}</b>",
            f"Direction: <b>{plan['direction']}</b>",
            "",
            f"Price: <b>{price}</b>",
            f"Candle time: <b>{epoch_to_local(candle_epoch)}</b>",
            f"Message time: <b>{epoch_to_local(int(time.time()))}</b>",
        ]
    )

def breakeven_message_text(plan, tp1_price, candle_epoch):
    icon = (
        "🟢"
        if plan["direction"] == "BULLISH"
        else "🔴"
    )
    return "\n".join(
        [
            f"{icon} <b>TP1 HIT — MOVE SL TO BREAK EVEN</b>",
            "",
            f"Symbol: <b>{plan['pair']}</b>",
            f"Timeframe: <b>{plan['timeframe']}</b>",
            f"Direction: <b>{plan['direction']}</b>",
            "",
            f"TP1 reached: <b>{tp1_price}</b>",
            f"Candle time: <b>{epoch_to_local(candle_epoch)}</b>",
            "",
            "────────────────────",
            "🛡 Action required:",
            f"Move your Stop Loss to <b>{plan['entry']}</b> (your entry price)",
            "This locks in a risk-free trade.",
            "────────────────────",
            "",
            f"TP2 target: <b>{plan['tp2']}</b>",
            f"TP3 target: <b>{plan['tp3']}</b>",
            "",
            f"Message time: <b>{epoch_to_local(int(time.time()))}</b>",
        ]
    )


def cancel_message_text(structure, reason):
    return "\n".join(
        [
            "⚪ <b>SETUP / TRADE CLOSED</b>",
            "",
            f"Symbol: <b>{structure['pair']}</b>",
            f"Timeframe: <b>{structure['timeframe']}</b>",
            f"Direction: <b>{structure['direction']}</b>",
            f"Reason: <b>{reason}</b>",
            "",
            "Scanner monitoring has stopped for this setup.",
            "The engine can now search for a fresh structure.",
            "",
            f"Time: <b>{epoch_to_local(int(time.time()))}</b>",
        ]
    )

# ============================================================
# TELEGRAM API
# ============================================================

def tg_send(chat_id, text, reply_to_message_id=None):
    if not TELEGRAM_BOT_TOKEN:
        return (
            False,
            0,
            "TELEGRAM_BOT_TOKEN not configured",
        )

    payload = {
        "chat_id": str(chat_id),
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": "true",
    }

    if reply_to_message_id:
        payload["reply_to_message_id"] = int(
            reply_to_message_id
        )

    data = urllib.parse.urlencode(
        payload
    ).encode()

    url = (
        "https://api.telegram.org/bot"
        f"{TELEGRAM_BOT_TOKEN}/sendMessage"
    )

    telegram_request = urllib.request.Request(
        url,
        data=data,
    )

    try:
        with urllib.request.urlopen(
            telegram_request,
            timeout=15,
        ) as response:
            body = response.read().decode(
                "utf-8",
                errors="ignore",
            )

            result = json.loads(body)

            if not result.get("ok"):
                return (
                    False,
                    0,
                    result.get(
                        "description",
                        "Telegram rejected message",
                    ),
                )

            message_id = int(
                result["result"]["message_id"]
            )

            return True, message_id, "ok"

    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(
            "utf-8",
            errors="ignore",
        )

        return False, 0, detail

    except Exception as exc:
        return False, 0, str(exc)


# ============================================================
# TELEGRAM PLAN RECORDS
# ============================================================

def active_telegram_users():
    with DB_LOCK:
        connection = db()

        try:
            cursor = connection.cursor()
            cursor.execute(
                """
                SELECT chat_id
                FROM telegram_users
                WHERE active=1
                """
            )
            result_rows = cursor.fetchall()
            cursor.close()
            return result_rows

        finally:
            connection.close()


def get_trade_alert(structure_key_value, chat_id):
    with DB_LOCK:
        connection = db()
        try:
            cursor = connection.cursor()
            cursor.execute(
                """
                SELECT *
                FROM telegram_trade_alerts
                WHERE structure_key=%s
                AND chat_id=%s
                """,
                (
                    structure_key_value,
                    str(chat_id),
                ),
            )
            result_row = cursor.fetchone()
            cursor.close()
            return result_row
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()



def save_trade_alert(
    structure,
    chat_id,
    f_message_id=0,
    g_message_id=0,
    active_message_id=0,
    plan=None,
    trade_state="WAITING_FOR_G",
    last_event="",
):
    key = structure_key(structure)
    now = int(time.time())

    with DB_LOCK:
        connection = db()

        try:
            cursor = connection.cursor()
            cursor.execute(
                """
                INSERT INTO telegram_trade_alerts(
                    structure_key,
                    chat_id,
                    pair,
                    timeframe,
                    direction,
                    f_message_id,
                    g_message_id,
                    active_message_id,
                    plan_json,
                    trade_state,
                    last_event,
                    created_epoch,
                    updated_epoch
                )
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT(structure_key, chat_id)
                DO UPDATE SET
                    f_message_id=CASE
                        WHEN excluded.f_message_id > 0
                        THEN excluded.f_message_id
                        ELSE telegram_trade_alerts.f_message_id
                    END,
                    g_message_id=CASE
                        WHEN excluded.g_message_id > 0
                        THEN excluded.g_message_id
                        ELSE telegram_trade_alerts.g_message_id
                    END,
                    active_message_id=CASE
                        WHEN excluded.active_message_id > 0
                        THEN excluded.active_message_id
                        ELSE telegram_trade_alerts.active_message_id
                    END,
                    plan_json=CASE
                        WHEN excluded.plan_json IS NOT NULL
                        THEN excluded.plan_json
                        ELSE telegram_trade_alerts.plan_json
                    END,
                    trade_state=excluded.trade_state,
                    last_event=excluded.last_event,
                    updated_epoch=excluded.updated_epoch
                """,
                (
                    key,
                    str(chat_id),
                    structure["pair"],
                    structure["timeframe"],
                    structure["direction"],
                    int(f_message_id or 0),
                    int(g_message_id or 0),
                    int(active_message_id or 0),
                    (
                        json.dumps(
                            plan,
                            separators=(",", ":"),
                        )
                        if plan is not None
                        else None
                    ),
                    trade_state,
                    last_event,
                    now,
                    now,
                ),
            )

            connection.commit()

        finally:
            connection.close()


def send_f_notifications(structure):
    users = active_telegram_users()

    if not users:
        return 0

    sent = 0
    text = f_message_text(structure)

    for user in users:
        chat_id = user["chat_id"]
        existing = get_trade_alert(
            structure_key(structure),
            chat_id,
        )

        if existing and existing["f_message_id"]:
            continue

        ok, message_id, detail = tg_send(
            chat_id,
            text,
        )

        if not ok:
            print(
                f"[Telegram] F message failed "
                f"for {chat_id}: {detail}"
            )
            continue

        save_trade_alert(
            structure,
            chat_id,
            f_message_id=message_id,
            trade_state="WAITING_FOR_G",
        )

        sent += 1

    return sent


def send_g_notifications(
    structure,
    candles,
    confluent_tf=None,
    confluent_stage=None,
):
    users = active_telegram_users()
    if not users:
        return 0

    sent = 0
    text = g_message_text(
        structure,
        candles,
        confluent_tf=confluent_tf,
        confluent_stage=confluent_stage,
    )

    pending_plan = pending_trade_plan(
        structure,
        candles,
    )

    for user in users:
        chat_id = user["chat_id"]

        existing = get_trade_alert(
            structure_key(structure),
            chat_id,
        )

        f_message_id = (
            existing["f_message_id"]
            if existing
            else 0
        )

        g_message_id = (
            existing["g_message_id"]
            if existing
            else 0
        )

        if g_message_id:
            continue

        # If F was historical or missed, create a context
        # message first so G still has a reply chain.
        if not f_message_id:
            ok_f, f_message_id, detail_f = tg_send(
                chat_id,
                f_message_text(structure),
            )

            if not ok_f:
                print(
                    f"[Telegram] Context F failed "
                    f"for {chat_id}: {detail_f}"
                )
                continue

        ok_g, g_message_id, detail_g = tg_send(
            chat_id,
            text,
            reply_to_message_id=f_message_id,
        )

        if not ok_g:
            print(
                f"[Telegram] G message failed "
                f"for {chat_id}: {detail_g}"
            )
            continue

        save_trade_alert(
            structure,
            chat_id,
            f_message_id=f_message_id,
            g_message_id=g_message_id,
            plan=pending_plan,
            trade_state="WAITING_FOR_ENTRY",
        )

        sent += 1

    return sent


def send_active_notifications(structure, candles):
    users = active_telegram_users()

    if not users:
        return 0

    sent = 0
    plan = active_trade_plan(
        structure,
        candles,
    )

    text = active_message_text(
        structure,
        candles,
    )

    for user in users:
        chat_id = user["chat_id"]

        existing = get_trade_alert(
            structure_key(structure),
            chat_id,
        )

        f_message_id = (
            existing["f_message_id"]
            if existing
            else 0
        )

        g_message_id = (
            existing["g_message_id"]
            if existing
            else 0
        )

        active_message_id = (
            existing["active_message_id"]
            if existing
            else 0
        )

        if active_message_id:
            continue

        # Ensure the reply chain exists.
        if not g_message_id:
            g_sent = send_g_notifications(
                structure,
                candles,
            )

            if g_sent:
                existing = get_trade_alert(
                    structure_key(structure),
                    chat_id,
                )

                if existing:
                    f_message_id = existing["f_message_id"]
                    g_message_id = existing["g_message_id"]

        reply_id = (
            g_message_id
            or f_message_id
            or None
        )

        ok, active_id, detail = tg_send(
            chat_id,
            text,
            reply_to_message_id=reply_id,
        )

        if not ok:
            print(
                f"[Telegram] Active entry failed "
                f"for {chat_id}: {detail}"
            )
            continue

        save_trade_alert(
            structure,
            chat_id,
            f_message_id=f_message_id,
            g_message_id=g_message_id,
            active_message_id=active_id,
            plan=plan,
            trade_state="ACTIVE",
        )

        sent += 1

    return sent


def send_cancel_notifications(structure, reason):
    users = active_telegram_users()
    sent = 0

    for user in users:
        chat_id = user["chat_id"]
        existing = get_trade_alert(
            structure_key(structure),
            chat_id,
        )

        if not existing:
            continue

        if existing["trade_state"] in (
            "CANCELLED",
            "CLOSED",
            "SL_HIT",
        ):
            continue

        reply_id = (
            existing["active_message_id"]
            or existing["g_message_id"]
            or existing["f_message_id"]
            or None
        )

        ok, _message_id, detail = tg_send(
            chat_id,
            cancel_message_text(
                structure,
                reason,
            ),
            reply_to_message_id=reply_id,
        )

        if not ok:
            print(
                f"[Telegram] Cancellation failed "
                f"for {chat_id}: {detail}"
            )
            continue

        save_trade_alert(
            structure,
            chat_id,
            trade_state="CANCELLED",
            last_event=reason,
        )
        sent += 1

    return sent


# ============================================================
# ENTRY AND TP/SL MONITORING
# ============================================================

def monitor_trade_alerts(pair, timeframe, candles):
    pair = canonical_symbol(pair)
    timeframe = str(timeframe).upper()

    with DB_LOCK:
        connection = db()
        try:
            cursor = connection.cursor()
            cursor.execute(
                """
                SELECT *
                FROM telegram_trade_alerts
                WHERE pair=%s
                  AND timeframe=%s
                  AND trade_state='ACTIVE'
                """,
                (
                    pair,
                    timeframe,
                ),
            )
            result_rows = cursor.fetchall()
            rows = cursor.fetchall()
            cursor.close()
            cursor.close()
            return result_rows
        finally:
            connection.close()

    if not rows:
        return 0

    total_events = 0

    for row in rows:
        if not row["plan_json"]:
            continue

        try:
            plan = json.loads(row["plan_json"])
        except Exception:
            continue

        entry_epoch = int(plan.get("entry_epoch", 0))
        direction = plan["direction"]
        events = []
        close_trade = False

        for candle in candles:
            candle_epoch = int(candle["epoch"])

            if candle_epoch <= entry_epoch:
                continue

            high = float(candle["high"])
            low = float(candle["low"])

            if direction == "BULLISH":
                # SL first — conservative
                if (
                    not plan.get("sl_hit")
                    and low <= float(plan["stop_loss"])
                ):
                    plan["sl_hit"] = True
                    plan["trade_state"] = "SL_HIT"
                    events.append(
                        (
                            "SL",
                            plan["stop_loss"],
                            candle_epoch,
                        )
                    )
                    close_trade = True
                    break

                if (
                    not plan.get("tp1_hit")
                    and high >= float(plan["tp1"])
                ):
                    plan["tp1_hit"] = True
                    events.append(
                        (
                            "TP1",
                            plan["tp1"],
                            candle_epoch,
                        )
                    )
                    # Break-even suggestion
                    try:
                        tg_send(
                            row["chat_id"],
                            breakeven_message_text(
                                plan,
                                plan["tp1"],
                                candle_epoch,
                            ),
                            reply_to_message_id=row["active_message_id"],
                        )
                    except Exception as exc:
                        print(
                            f"[Telegram] Break-even message failed: {exc}"
                        )

                if (
                    not plan.get("tp2_hit")
                    and high >= float(plan["tp2"])
                ):
                    plan["tp2_hit"] = True
                    events.append(
                        (
                            "TP2",
                            plan["tp2"],
                            candle_epoch,
                        )
                    )

                if (
                    not plan.get("tp3_hit")
                    and high >= float(plan["tp3"])
                ):
                    plan["tp3_hit"] = True
                    plan["trade_state"] = "TP3_HIT"
                    events.append(
                        (
                            "TP3",
                            plan["tp3"],
                            candle_epoch,
                        )
                    )
                    close_trade = True
                    break

            else:
                # BEARISH
                # SL first — conservative
                if (
                    not plan.get("sl_hit")
                    and high >= float(plan["stop_loss"])
                ):
                    plan["sl_hit"] = True
                    plan["trade_state"] = "SL_HIT"
                    events.append(
                        (
                            "SL",
                            plan["stop_loss"],
                            candle_epoch,
                        )
                    )
                    close_trade = True
                    break

                if (
                    not plan.get("tp1_hit")
                    and low <= float(plan["tp1"])
                ):
                    plan["tp1_hit"] = True
                    events.append(
                        (
                            "TP1",
                            plan["tp1"],
                            candle_epoch,
                        )
                    )
                    # Break-even suggestion
                    try:
                        tg_send(
                            row["chat_id"],
                            breakeven_message_text(
                                plan,
                                plan["tp1"],
                                candle_epoch,
                            ),
                            reply_to_message_id=row["active_message_id"],
                        )
                    except Exception as exc:
                        print(
                            f"[Telegram] Break-even message failed: {exc}"
                        )

                if (
                    not plan.get("tp2_hit")
                    and low <= float(plan["tp2"])
                ):
                    plan["tp2_hit"] = True
                    events.append(
                        (
                            "TP2",
                            plan["tp2"],
                            candle_epoch,
                        )
                    )

                if (
                    not plan.get("tp3_hit")
                    and low <= float(plan["tp3"])
                ):
                    plan["tp3_hit"] = True
                    plan["trade_state"] = "TP3_HIT"
                    events.append(
                        (
                            "TP3",
                            plan["tp3"],
                            candle_epoch,
                        )
                    )
                    close_trade = True
                    break

        if not events:
            continue

        active_message_id = row["active_message_id"]

        for event, price, candle_epoch in events:
            ok, _message_id, detail = tg_send(
                row["chat_id"],
                trade_event_text(
                    plan,
                    event,
                    price,
                    candle_epoch,
                ),
                reply_to_message_id=active_message_id,
            )

            if ok:
                total_events += 1
            else:
                print(
                    f"[Telegram] Trade event failed: {detail}"
                )

        if close_trade:
            plan["trade_state"] = (
                "CLOSED"
                if plan.get("tp3_hit")
                else "SL_HIT"
            )

            with DB_LOCK:
                connection = db()
                try:
                    cursor = connection.cursor()
                    cursor.execute(
                        """
                        UPDATE telegram_trade_alerts
                        SET plan_json=%s,
                            trade_state=%s,
                            last_event=%s,
                            updated_epoch=%s
                        WHERE id=%s
                        """,
                        (
                            json.dumps(
                                plan,
                                separators=(",", ":"),
                            ),
                            plan["trade_state"],
                            events[-1][0],
                            int(time.time()),
                            row["id"],
                        ),
                    )
                    connection.commit()
                    cursor.close()
                except Exception:
                    connection.rollback()
                    raise
                finally:
                    connection.close()

            # Free this pattern slot
            delete_structure_by_key(row["structure_key"])

    return total_events


# ============================================================
# NOTIFICATION PROCESSING
# ============================================================

def check_timeframe_confluence(
    pair,
    timeframe,
    direction,
    min_stage_rank=1,
):
    """
    Check if the same pair has a structure in the same direction
    on at least one OTHER timeframe that is at least at stage D.

    min_stage_rank:
        0 = WAITING_FOR_BOS
        1 = WAITING_FOR_E  (has D)
        2 = WAITING_FOR_F  (has E)
        3 = WAITING_FOR_G  (has F)
        4 = WAITING_FOR_ENTRY (has G)
        5 = ACTIVE
    """
    pair = canonical_symbol(pair)
    timeframe = str(timeframe).upper()

    with DB_LOCK:
        connection = db()
        try:
            cursor = connection.cursor()
            cursor.execute(
                """
                SELECT timeframe, state, stage,
                       d_json, e_json, f_json, g_json
                FROM structures
                WHERE pair=%s
                  AND direction=%s
                  AND timeframe != %s
                  AND state NOT IN ('INVALID', 'CLOSED')
                ORDER BY updated_epoch DESC
                """,
                (pair, direction, timeframe),
            )
            rows = cursor.fetchall()
            cursor.close()
        finally:
            connection.close()

    for row in rows:
        stage = row["stage"] or row["state"]
        rank = {
            "WAITING_FOR_BOS": 0,
            "WAITING_FOR_E": 1,
            "WAITING_FOR_F": 2,
            "WAITING_FOR_G": 3,
            "WAITING_FOR_ENTRY": 4,
            "ACTIVE": 5,
            "COMPLETE": 6,
        }.get(stage, -1)

        if rank >= min_stage_rank:
            return True, row["timeframe"], stage

    return False, None, None
    
def process_structure_events(
    structure,
    previous_stage,
    previous_f_epoch,
    previous_g_epoch,
    candles,
):
    notifications = 0

    live_from = int(
        structure.get("live_from_epoch", 0)
    )

    f_epoch = int(
        structure["f"]["epoch"]
    ) if structure.get("f") else 0

    g_epoch = int(
        structure["g"]["epoch"]
    ) if structure.get("g") else 0

    # F message only when F is newly live.
    if (
        structure.get("stage") in (
            "WAITING_FOR_G",
            "WAITING_FOR_ENTRY",
            "ACTIVE",
        )
        and f_epoch > live_from
        and f_epoch != previous_f_epoch
    ):
        notifications += send_f_notifications(
            structure
        )

    # G message contains Entry Zone + SL.
    # Only send if confluence is confirmed on another timeframe
    # (or if confluence check is disabled).
    if (
        structure.get("stage") in (
            "WAITING_FOR_ENTRY",
            "ACTIVE",
        )
        and g_epoch > live_from
        and g_epoch != previous_g_epoch
    ):
        confluent = True
        confluent_tf = None
        confluent_stage = None

        if REQUIRE_CONFLUENCE:
            confluent, confluent_tf, confluent_stage = (
                check_timeframe_confluence(
                    structure["pair"],
                    structure["timeframe"],
                    structure["direction"],
                    min_stage_rank=CONFLUENCE_MIN_RANK,
                )
            )

        if confluent:
            if confluent_tf:
                print(
                    f"[Confluence] {structure['pair']} "
                    f"{structure['timeframe']} "
                    f"{structure['direction']} confirmed by "
                    f"{confluent_tf} at {confluent_stage}"
                )
            notifications += send_g_notifications(
                structure,
                candles,
                confluent_tf=confluent_tf,
                confluent_stage=confluent_stage,
            )
        else:
            print(
                f"[Confluence] {structure['pair']} "
                f"{structure['timeframe']} "
                f"{structure['direction']} "
                f"G formed but no confluence yet — "
                f"holding Telegram alert"
            )

    # If an entry was confirmed by a closed A-zone rejection,
    # send actual Entry + TP1/TP2/TP3.
    if (
        structure.get("stage") == "ACTIVE"
        and structure.get("entry_epoch", 0) > live_from
    ):
        notifications += send_active_notifications(
            structure,
            candles,
        )

    return notifications


# ============================================================
# SCAN ENGINE
# ============================================================

def stage_rank(structure):
    """Higher number = more advanced in the A → G pipeline."""
    stage = structure.get(
        "stage",
        structure.get("state", ""),
    )
    return {
        "WAITING_FOR_BOS": 0,
        "WAITING_FOR_E": 1,
        "WAITING_FOR_F": 2,
        "WAITING_FOR_G": 3,
        "WAITING_FOR_ENTRY": 4,
        "ACTIVE": 5,
        "COMPLETE": 6,
    }.get(stage, -1)


def latest_point_epoch(structure):
    """Return the epoch of the most recent point in the structure."""
    for key in ("g", "f", "e", "d", "b", "a"):
        pt = structure.get(key)
        if pt:
            return int(pt.get("epoch", 0))
    return 0


def run_scan(
    pair,
    timeframe,
    candles,
    strength,
    bos_mode,
    min_atr,
    expansion_atr,
    displacement_atr,
    min_bars=1,
    fib_ratio=0.5,
):
    pair = canonical_symbol(pair)
    timeframe = str(timeframe).upper()

    latest_epoch = candles[-1]["epoch"]
    completed_now = []
    trade_events_now = 0

    memory = structures_for(
        pair,
        timeframe,
    )

    # Keep the most advanced candidates per direction.
    # Allow up to MAX_STRUCTURES_PER_DIRECTION active at once.
    for direction in (
        "BULLISH",
        "BEARISH",
    ):
        active = [
            item
            for item in memory
            if item["direction"] == direction
            and item["state"] not in (
                "COMPLETE",
                "INVALID",
                "CLOSED",
            )
        ]
        active.sort(
            key=lambda item: (
                stage_rank(item),
                latest_point_epoch(item),
            ),
            reverse=True,
        )
        # Delete anything beyond the allowed maximum
        for obsolete in active[MAX_STRUCTURES_PER_DIRECTION:]:
            delete_structure(obsolete)

    memory = structures_for(
        pair,
        timeframe,
    )



    # Advance existing candidates.
    for structure in list(memory):
        if structure["state"] in (
            "INVALID",
            "CLOSED",
        ):
            continue

        previous_stage = structure.get(
            "stage",
            structure["state"],
        )

        previous_f_epoch = int(
            structure["f"]["epoch"]
        ) if structure.get("f") else 0

        previous_g_epoch = int(
            structure["g"]["epoch"]
        ) if structure.get("g") else 0

        if not structure.get("live_from_epoch"):
            structure["live_from_epoch"] = latest_epoch

        structure = advance_structure(
            structure=structure,
            candles=candles,
            bos_mode=bos_mode,
            expansion_atr=expansion_atr,
            displacement_atr=displacement_atr,
            swing_strength=strength,
            min_atr_move=min_atr,
            fib_ratio=fib_ratio,
        )

        # After G, wait for a CLOSED A-zone rejection candle.
        if structure.get("stage") == "WAITING_FOR_ENTRY":
            confirm_closed_entry(
                structure,
                candles,
            )

        if structure.get("stage") == "ACTIVE":
            finished = False
            finish_reason = ""

            try:
                finished, finish_reason = trade_already_finished(
                    structure,
                    candles,
                )
            except Exception as exc:
                print(
                    f"[Scanner] trade_already_finished error "
                    f"for {structure.get('pair')} "
                    f"{structure.get('timeframe')}: {exc}"
                )

            if finished:
                mark_invalid(
                    structure,
                    finish_reason,
                )
                
        if structure["state"] == "INVALID":
            # Send cancellation only for a live plan.
            if (
                structure.get("g")
                and structure.get("g", {}).get("epoch", 0)
                > structure.get("live_from_epoch", 0)
            ):
                send_cancel_notifications(
                    structure,
                    structure.get(
                        "discard_reason",
                        "structure invalidated",
                    ),
                )
            delete_structure(structure)
            continue

        g_epoch_now = int(
            structure["g"]["epoch"]
        ) if structure.get("g") else 0

        if g_epoch_now and g_epoch_now != previous_g_epoch:
            completed_now.append(structure)

        save(structure)

        trade_events_now += process_structure_events(
            structure,
            previous_stage,
            previous_f_epoch,
            previous_g_epoch,
            candles,
        )

        save(structure)

    memory = structures_for(
        pair,
        timeframe,
    )

    known_keys = {
        item["structure_key"]
        for item in memory
    }

    # Discover new candidates if we have room for more.
    for direction in (
        "BULLISH",
        "BEARISH",
    ):
        active_for_direction = [
            item
            for item in memory
            if item["direction"] == direction
            and item["state"] not in (
                "COMPLETE",
                "INVALID",
                "CLOSED",
            )
        ]

        # How many slots are still available
        available_slots = (
            MAX_STRUCTURES_PER_DIRECTION
            - len(active_for_direction)
        )

        if available_slots <= 0:
            continue

        best_rank = max(
            (
                stage_rank(item)
                for item in active_for_direction
            ),
            default=-1,
        )

        # If ALL slots are filled with G-or-beyond structures
        # do not bother discovering basic patterns
        if best_rank >= 4 and len(active_for_direction) >= MAX_STRUCTURES_PER_DIRECTION:
            continue

        candidates = discover_abc(
            candles=candles,
            pair=pair,
            timeframe=timeframe,
            direction=direction,
            strength=strength,
            min_atr=min_atr,
            min_bars=min_bars,
        )

        candidates.sort(
            key=lambda item: item["b"]["epoch"],
            reverse=True,
        )

        for candidate in candidates:
            key = structure_key(candidate)

            if key in known_keys:
                continue

            # Historical context can be used to find F and G,
            # but live notifications require epochs after this boundary.
            candidate["live_from_epoch"] = latest_epoch
            candidate["context_only"] = True

            previous_stage = candidate["stage"]

            candidate = advance_structure(
                structure=candidate,
                candles=candles,
                bos_mode=bos_mode,
                expansion_atr=expansion_atr,
                displacement_atr=displacement_atr,
                swing_strength=strength,
                min_atr_move=min_atr,
                fib_ratio=fib_ratio,
            )

            if candidate.get("stage") == "WAITING_FOR_ENTRY":
                confirm_closed_entry(
                    candidate,
                    candles,
                )

            if candidate["state"] == "INVALID":
                continue

            save(candidate)
            known_keys.add(key)
            memory.append(candidate)

            # Historical F/G are context only and do not create an
            # old alert immediately.
        discovered = 0

        for candidate in candidates:
            if discovered >= available_slots:
                break

            key = structure_key(candidate)

            if key in known_keys:
                continue

            candidate["live_from_epoch"] = latest_epoch
            candidate["context_only"] = True

            previous_stage = candidate["stage"]

            candidate = advance_structure(
                structure=candidate,
                candles=candles,
                bos_mode=bos_mode,
                expansion_atr=expansion_atr,
                displacement_atr=displacement_atr,
                swing_strength=strength,
                min_atr_move=min_atr,
                fib_ratio=fib_ratio,
            )

            if candidate.get("stage") == "WAITING_FOR_ENTRY":
                confirm_closed_entry(
                    candidate,
                    candles,
                )

            if candidate["state"] == "INVALID":
                continue

            save(candidate)
            known_keys.add(key)
            memory.append(candidate)
            discovered += 1

    # Monitor active trade plans for TP/SL.
    for target in (
        database_scanner_targets()
    ):
        target_pair = target["pair"]
        target_tf = target["timeframe"]

        if (
            target_pair != pair
            or target_tf != timeframe
        ):
            continue

        trade_events_now += monitor_trade_alerts(
            pair,
            timeframe,
            candles,
        )

    final_signals = structures_for(
        pair,
        timeframe,
    )

    active_count = sum(
        1
        for item in final_signals
        if item["state"] not in (
            "COMPLETE",
            "INVALID",
            "CLOSED",
        )
    )

    return {
        "pair": pair,
        "timeframe": timeframe,
        "scan_mode": "DUAL_DIRECTION",
        "chronological_order": "A -> C -> B -> D -> E -> F -> G",
        "memory_enabled": True,
        "active_structures": active_count,
        "stored_structures": len(final_signals),
        "completed_now": len(completed_now),
        "trade_events_now": trade_events_now,
        "signals": final_signals,
        "completed_signals": completed_now,
    }


# ============================================================
# SCANNER SETTINGS
# ============================================================

def environment_list(name, default):
    raw = os.environ.get(name, default)

    return [
        value.strip()
        for value in raw.split(",")
        if value.strip()
    ]


def scanner_settings():
    pairs = unique_values(
        [
            canonical_symbol(pair)
            for pair in environment_list(
                "SCANNER_PAIRS",
                "",
            )
            if canonical_symbol(pair)
        ]
    )

    timeframes = unique_values(
        [
            timeframe.upper()
            for timeframe in environment_list(
                "SCANNER_TIMEFRAMES",
                "",
            )
            if timeframe.upper() in TIMEFRAME_MAP
        ]
    )

    bos = os.environ.get(
        "SCANNER_BOS",
        "body",
    ).lower()

    if bos not in (
        "body",
        "wick",
    ):
        bos = "body"

    return {
        "enabled": os.environ.get(
            "SCANNER_ENABLED",
            "true",
        ).lower() in (
            "1",
            "true",
            "yes",
            "on",
        ),
        "pairs": pairs or ["R_100"],
        "timeframes": timeframes or ["M15"],
        "interval": max(
            5,
            int(
                os.environ.get(
                    "SCANNER_INTERVAL_SECONDS",
                    str(DEFAULT_SCANNER_INTERVAL),
                )
            ),
        ),
        "count": max(
            100,
            min(
                1000,
                int(
                    os.environ.get(
                        "SCANNER_CANDLE_COUNT",
                        str(DEFAULT_CANDLE_COUNT),
                    )
                ),
            ),
        ),
        "strength": max(
            1,
            int(
                os.environ.get(
                    "SCANNER_STRENGTH",
                    "3",
                )
            ),
        ),
        "bos": bos,
        "min_atr": max(
            0.0,
            float(
                os.environ.get(
                    "SCANNER_MIN_ATR_MOVE",
                    "0.25",
                )
            ),
        ),
        "expansion_atr": max(
            0.0,
            float(
                os.environ.get(
                    "SCANNER_MIN_EXPANSION_ATR",
                    "0.5",
                )
            ),
        ),
        "displacement_atr": max(
            0.0,
            float(
                os.environ.get(
                    "SCANNER_DISPLACEMENT_ATR",
                    "1.0",
                )
            ),
        ),
        "min_bars": max(
            1,
            int(
                os.environ.get(
                    "SCANNER_MIN_BARS",
                    "1",
                )
            ),
        ),
        "fib_ratio": max(
            0.0,
            min(
                1.0,
                float(
                    os.environ.get(
                        "SCANNER_FIB_THRESHOLD",
                        "0.5",
                    )
                ),
            ),
        ),
    }


def database_scanner_targets():
    try:
        with DB_LOCK:
            connection = db()
            try:
                cursor = connection.cursor()
                cursor.execute(
                    """
                    SELECT pair, timeframe
                    FROM scanner_targets
                    WHERE active=1
                    ORDER BY pair, timeframe
                    """
                )
                rows = cursor.fetchall()
                cursor.close()
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()

        return [
            {
                "pair": row["pair"],
                "timeframe": row["timeframe"],
            }
            for row in rows
        ]

    except Exception as exc:
        print(
            f"[Scanner] Target lookup failed: {exc}"
        )
        return []

def effective_scanner_targets(config=None):
    stored = database_scanner_targets()
    if stored:
        return stored

    config = config or scanner_settings()

    if not config["pairs"] or not config["timeframes"]:
        return []

    return [
        {
            "pair": pair,
            "timeframe": timeframe,
        }
        for pair in config["pairs"]
        for timeframe in config["timeframes"]
    ]


def replace_scanner_targets(pairs, timeframes):
    canonical_pairs = unique_values(
        [
            canonical_symbol(pair)
            for pair in pairs
            if canonical_symbol(pair)
        ]
    )

    canonical_timeframes = unique_values(
        [
            timeframe.upper()
            for timeframe in timeframes
            if timeframe.upper() in TIMEFRAME_MAP
        ]
    )

    if not canonical_pairs:
        raise ValueError(
            "At least one valid pair is required"
        )

    if not canonical_timeframes:
        raise ValueError(
            "At least one supported timeframe is required"
        )

    targets = [
        {
            "pair": pair,
            "timeframe": timeframe,
        }
        for pair in canonical_pairs
        for timeframe in canonical_timeframes
    ]

    if len(targets) > 50:
        raise ValueError(
            "Maximum 50 pair/timeframe combinations"
        )

    now = int(time.time())

    with DB_LOCK:
        connection = db()

        try:
            cursor = connection.cursor()
            cursor.execute(
                "DELETE FROM scanner_targets"
            )

            for target in targets:
                cursor = connection.cursor()
                cursor.execute(
                    """
                    INSERT INTO scanner_targets(
                        pair,
                        timeframe,
                        active,
                        created_epoch,
                        updated_epoch
                    )
                    VALUES(%s,%s,1,%s,%s)
                    """,
                    (
                        target["pair"],
                        target["timeframe"],
                        now,
                        now,
                    ),
                )

            connection.commit()
            cursor.close()

        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    return targets


# ============================================================
# CONTINUOUS SCANNER
# ============================================================

def continuous_scan_once(pair, timeframe, config):
    pair = canonical_symbol(pair)
    timeframe = str(timeframe).upper()

    diagnostic_started(
        pair,
        timeframe,
    )

    if timeframe not in TIMEFRAME_MAP:
        raise ValueError(
            f"Unsupported timeframe: {timeframe}"
        )

    granularity = TIMEFRAME_MAP[timeframe]

    raw = get_candles(
        pair,
        granularity,
        config["count"],
    )

    candles = closed_candles(
        normalize(raw),
        granularity,
    )

    minimum_required = max(
        50,
        config["strength"] * 4,
    )

    if len(candles) < minimum_required:
        result = {
            "ok": False,
            "reason": "not_enough_closed_candles",
            "pair": pair,
            "timeframe": timeframe,
            "candles": len(candles),
        }

        diagnostic_error(
            pair,
            timeframe,
            f"Not enough closed candles: {len(candles)}",
        )

        return result

    latest_epoch = candles[-1]["epoch"]
    key = f"{pair}|{timeframe}"

    if SCANNER_LAST.get(key) == latest_epoch:
        events = monitor_trade_alerts(
            pair,
            timeframe,
            candles,
        )

        result = {
            "ok": True,
            "pair": pair,
            "timeframe": timeframe,
            "skipped": True,
            "latest_closed_epoch": latest_epoch,
            "completed_now": 0,
            "trade_events_now": events,
            "active_structures": None,
        }

        diagnostic_success(
            pair,
            timeframe,
            result,
        )

        return result

    result = run_scan(
        pair=pair,
        timeframe=timeframe,
        candles=candles,
        strength=config["strength"],
        bos_mode=config["bos"],
        min_atr=config["min_atr"],
        expansion_atr=config["expansion_atr"],
        displacement_atr=config["displacement_atr"],
        min_bars=config["min_bars"],
        fib_ratio=config["fib_ratio"],
    )

    SCANNER_LAST[key] = latest_epoch

    result.update(
        {
            "ok": True,
            "skipped": False,
            "latest_closed_epoch": latest_epoch,
        }
    )

    diagnostic_success(
        pair,
        timeframe,
        result,
    )

    return result


def scanner_loop():
    config = scanner_settings()

    print(
        "[Scanner] Started "
        f"(interval={config['interval']} seconds)"
    )

    while not SCANNER_STOP.is_set():
        cycle_started = time.time()

        try:
            config = scanner_settings()

            if not config["enabled"]:
                print("[Scanner] Disabled")

            elif not SCAN_LOCK.acquire(blocking=False):
                print("[Scanner] Previous cycle still running")

            else:
                try:
                    targets = effective_scanner_targets(config)

                    if not targets:
                        print(
                            "[Scanner] No targets configured. "
                            "Set pairs in the app or SCANNER_PAIRS env var."
                        )
                    else:
                        for target in targets:
                            if SCANNER_STOP.is_set():
                                break

                            pair = target["pair"]
                            timeframe = target["timeframe"]

                            try:
                                result = continuous_scan_once(
                                    pair,
                                    timeframe,
                                    config,
                                )

                                if result.get("completed_now", 0):
                                    print(
                                        f"[Scanner] {pair} "
                                        f"{timeframe}: "
                                        f"{result['completed_now']} "
                                        "new structure(s)"
                                    )
                                elif result.get("trade_events_now", 0):
                                    print(
                                        f"[Scanner] {pair} "
                                        f"{timeframe}: "
                                        f"{result['trade_events_now']} "
                                        "trade event(s)"
                                    )
                                else:
                                    print(
                                        f"[Scanner] {pair} "
                                        f"{timeframe}: OK "
                                        f"(skipped="
                                        f"{result.get('skipped')})"
                                    )

                            except Exception as exc:
                                diagnostic_error(
                                    pair,
                                    timeframe,
                                    exc,
                                )
                                print(
                                    f"[Scanner] {pair} "
                                    f"{timeframe} ERROR: "
                                    f"{exc}"
                                )

                finally:
                    SCAN_LOCK.release()

        except Exception as exc:
            print(f"[Scanner] OUTER ERROR: {exc}")

        elapsed = time.time() - cycle_started
        config = scanner_settings()

        wait_seconds = max(
            1,
            config["interval"] - int(elapsed),
        )

        print(f"[Scanner] Next cycle in {wait_seconds}s")

        SCANNER_STOP.wait(wait_seconds)

    print("[Scanner] Stopped")


def start_scanner():
    global SCANNER_THREAD

    if (
        SCANNER_THREAD
        and SCANNER_THREAD.is_alive()
    ):
        return False

    SCANNER_STOP.clear()

    SCANNER_THREAD = threading.Thread(
        target=scanner_loop,
        name="trade-signal-scanner",
        daemon=True,
    )

    SCANNER_THREAD.start()
    return True


def stop_scanner():
    SCANNER_STOP.set()
    return True


# ============================================================
# AUTH
# ============================================================

def optional_admin_authorized():
    if not ADMIN_KEY:
        return True

    return (
        request.headers.get(
            "X-Admin-Key"
        )
        == ADMIN_KEY
    )


def strict_admin_authorized():
    if not ADMIN_KEY:
        return True

    return (
        request.headers.get(
            "X-Admin-Key"
        )
        == ADMIN_KEY
    )


# ============================================================
# API ROUTES
# ============================================================

@app.get("/")
def home_api():
    return (
        "TradeSignal scanner is running. "
        "A → C → B → D → E → F → G"
    )


@app.get("/health")
def health_api():
    config = scanner_settings()

    try:
        targets = effective_scanner_targets(config)
    except Exception:
        targets = []

    structures_count = 0
    users_count = 0
    plans_count = 0

    try:
        with DB_LOCK:
            connection = db()
            try:
                cursor = connection.cursor()

                cursor.execute(
                    "SELECT COUNT(*) AS cnt FROM structures"
                )
                structures_count = cursor.fetchone()["cnt"]

                cursor.execute(
                    """
                    SELECT COUNT(*) AS cnt
                    FROM telegram_users
                    WHERE active=1
                    """
                )
                users_count = cursor.fetchone()["cnt"]

                cursor.execute(
                    """
                    SELECT COUNT(*) AS cnt
                    FROM telegram_trade_alerts
                    WHERE trade_state='ACTIVE'
                    """
                )
                plans_count = cursor.fetchone()["cnt"]

                cursor.close()
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()

    except Exception as exc:
        print(f"[Health] DB error: {exc}")

    return jsonify(
        {
            "ok": True,
            "engine": "TradeSignal Structure Scanner V9",
            "server_time_utc": datetime.now(timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S UTC"
            ),
            "chronological_order": (
                "A -> C -> B -> D -> E -> F -> G"
            ),
            "database_type": (
                "postgresql" if DB_IS_PERSISTENT else "none"
            ),
            "database_persistent": DB_IS_PERSISTENT,
            "stored_structures": structures_count,
            "telegram_users": users_count,
            "active_trade_plans": plans_count,
            "telegram_configured": bool(TELEGRAM_BOT_TOKEN),
            "continuous_scanner": bool(
                SCANNER_THREAD
                and SCANNER_THREAD.is_alive()
                and not SCANNER_STOP.is_set()
            ),
            "scanner_interval_seconds": config["interval"],
            "g_min_reach": G_MIN_REACH,
            "targets": targets,
        }
    )


@app.get("/symbols")
def symbols_api():
    return jsonify(
        {
            "ok": True,
            "symbols": sorted(
                set(SYMBOL_MAP.values())
            ),
        }
    )


@app.get("/symbols/resolve")
def resolve_symbol_api():
    raw = request.args.get(
        "pair",
        "",
    )

    resolved = canonical_symbol(raw)

    return jsonify(
        {
            "input": raw,
            "deriv_symbol": resolved,
            "supported": bool(resolved),
        }
    )


@app.get("/ohlc")
def ohlc_api():
    raw_pair = request.args.get(
        "pair",
        "",
    )

    pair = canonical_symbol(raw_pair)

    timeframe = request.args.get(
        "timeframe",
        "M15",
    ).upper()

    if not pair:
        return jsonify(
            {
                "error": f"Unsupported symbol: {raw_pair}"
            }
        ), 400

    if timeframe not in TIMEFRAME_MAP:
        return jsonify(
            {
                "error": "Unsupported timeframe",
                "supported": list(
                    TIMEFRAME_MAP
                ),
            }
        ), 400

    try:
        count = max(
            100,
            min(
                int(
                    request.args.get(
                        "count",
                        DEFAULT_CANDLE_COUNT,
                    )
                ),
                1000,
            ),
        )
    except ValueError:
        return jsonify(
            {
                "error": "Invalid count"
            }
        ), 400

    try:
        rows = get_candles(
            pair,
            TIMEFRAME_MAP[timeframe],
            count,
        )

        candles = closed_candles(
            normalize(rows),
            TIMEFRAME_MAP[timeframe],
        )

        return jsonify(candles)

    except Exception as exc:
        return jsonify(
            {
                "error": str(exc),
                "pair": pair,
                "timeframe": timeframe,
            }
        ), 502


@app.get("/scan")
def scan_api():
    raw_pair = request.args.get(
        "pair",
        "",
    )

    pair = canonical_symbol(raw_pair)

    timeframe = request.args.get(
        "timeframe",
        "M15",
    ).upper()

    if not pair:
        return jsonify(
            {
                "error": f"Unsupported symbol: {raw_pair}"
            }
        ), 400

    if timeframe not in TIMEFRAME_MAP:
        return jsonify(
            {
                "error": "Unsupported timeframe",
                "supported": list(
                    TIMEFRAME_MAP
                ),
            }
        ), 400

    try:
        count = max(
            100,
            min(
                int(
                    request.args.get(
                        "count",
                        DEFAULT_CANDLE_COUNT,
                    )
                ),
                1000,
            ),
        )

        strength = max(
            1,
            int(
                request.args.get(
                    "strength",
                    "3",
                )
            ),
        )

        min_atr = max(
            0.0,
            float(
                request.args.get(
                    "min_atr_move",
                    "0.25",
                )
            ),
        )

        expansion_atr = max(
            0.0,
            float(
                request.args.get(
                    "min_expansion_atr",
                    "0.5",
                )
            ),
        )

        displacement_atr = max(
            0.0,
            float(
                request.args.get(
                    "displacement_atr",
                    "1.0",
                )
            ),
        )

        min_bars = max(
            1,
            int(
                request.args.get(
                    "min_bars",
                    "1",
                )
            ),
        )

        fib_ratio = max(
            0.0,
            min(
                1.0,
                float(
                    request.args.get(
                        "fib_threshold",
                        "0.5",
                    )
                ),
            ),
        )

    except ValueError:
        return jsonify(
            {
                "error": "Invalid numeric parameter"
            }
        ), 400

    bos_mode = request.args.get(
        "bos",
        "body",
    ).lower()

    if bos_mode not in (
        "body",
        "wick",
    ):
        bos_mode = "body"

    if not SCAN_LOCK.acquire(
        blocking=False
    ):
        return jsonify(
            {
                "error": (
                    "Scanner is already processing "
                    "another request"
                )
            }
        ), 409

    try:
        rows = get_candles(
            pair,
            TIMEFRAME_MAP[timeframe],
            count,
        )

        candles = closed_candles(
            normalize(rows),
            TIMEFRAME_MAP[timeframe],
        )

        if len(candles) < max(
            50,
            strength * 4,
        ):
            return jsonify(
                {
                    "error": (
                        "Not enough closed candles"
                    ),
                    "candles": len(candles),
                }
            ), 502

        result = run_scan(
            pair=pair,
            timeframe=timeframe,
            candles=candles,
            strength=strength,
            bos_mode=bos_mode,
            min_atr=min_atr,
            expansion_atr=expansion_atr,
            displacement_atr=displacement_atr,
            min_bars=min_bars,
            fib_ratio=fib_ratio,
        )

        return jsonify(result)

    except Exception as exc:
        return jsonify(
            {
                "error": str(exc),
                "pair": pair,
                "timeframe": timeframe,
            }
        ), 502

    finally:
        SCAN_LOCK.release()


@app.get("/structures")
def structures_api():
    raw_pair = request.args.get(
        "pair",
        "",
    )

    raw_timeframe = request.args.get(
        "timeframe",
        "",
    )

    sql = (
        "SELECT * FROM structures "
        "WHERE state != 'INVALID'"
    )

    arguments = []
    clauses = []

    pairs = unique_values(
        [
            canonical_symbol(item)
            for item in split_csv(raw_pair)
            if canonical_symbol(item)
        ]
    )

    timeframes = unique_values(
        [
            item.upper()
            for item in split_csv(raw_timeframe)
            if item.upper() in TIMEFRAME_MAP
        ]
    )

    if pairs:
        clauses.append(
            "pair IN ("
            + ",".join("%s" for _ in pairs)
            + ")"
        )
        arguments.extend(pairs)

    if timeframes:
        clauses.append(
            "timeframe IN ("
            + ",".join("%s" for _ in timeframes)
            + ")"
        )
        arguments.extend(timeframes)

    if clauses:
        sql += " AND " + " AND ".join(clauses)

    sql += (
        " ORDER BY updated_epoch DESC "
        "LIMIT 500"
    )

    with DB_LOCK:
        connection = db()

        try:
            cursor = connection.cursor()
            cursor.execute(sql, arguments)
            rows = cursor.fetchall()
            cursor.close()

        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    return jsonify(
        [
            row_to_structure(row)
            for row in rows
        ]
    )


@app.post("/structures/close")
def close_structure_api():
    if not strict_admin_authorized():
        return jsonify(
            {
                "ok": False,
                "error": "unauthorized",
            }
        ), 401

    body = request.get_json(
        force=True,
        silent=True,
    ) or {}

    structure_key_value = str(
        body.get("structure_key", "")
    ).strip()

    outcome = str(
        body.get("outcome", "DISCARDED")
    ).strip().upper()

    if not structure_key_value:
        return jsonify(
            {
                "ok": False,
                "error": "structure_key is required",
            }
        ), 400

    outcomes = {
        "DISCARDED": (
            "CANCELLED",
            "Manually discarded from Android app",
        ),
        "SL_HIT": (
            "SL_HIT",
            "Manually marked as Stop Loss hit from Android app",
        ),
        "TP_HIT": (
            "CLOSED",
            "Manually marked as final Take Profit hit from Android app",
        ),
    }

    if outcome not in outcomes:
        return jsonify(
            {
                "ok": False,
                "error": (
                    "outcome must be DISCARDED, "
                    "SL_HIT, or TP_HIT"
                ),
            }
        ), 400

    if not SCAN_LOCK.acquire(blocking=False):
        return jsonify(
            {
                "ok": False,
                "error": (
                    "Scanner is busy. "
                    "Try again in a few seconds."
                ),
            }
        ), 409

    try:
        structure = structure_for_key(
            structure_key_value
        )

        if not structure:
            return jsonify(
                {
                    "ok": False,
                    "error": "Structure not found",
                }
            ), 404

        trade_state, default_reason = outcomes[outcome]

        reason = str(
            body.get("reason", "")
        ).strip() or default_reason

        send_cancel_notifications(
            structure,
            reason,
        )

        close_trade_alerts_for_structure(
            structure_key_value,
            trade_state,
            reason,
        )

        close_structure_by_key(
            structure_key_value,
            reason,
        )

        return jsonify(
            {
                "ok": True,
                "structure_key": structure_key_value,
                "state": "CLOSED",
                "outcome": outcome,
                "message": (
                    "Setup closed. Scanner can now "
                    "search for another position."
                ),
            }
        )

    finally:
        SCAN_LOCK.release()


@app.route(
    "/scanner/targets",
    methods=["GET", "PUT", "POST"],
)
def scanner_targets_api():
    if request.method == "GET":
        config = scanner_settings()
        targets = effective_scanner_targets(config)

        return jsonify(
            {
                "ok": True,
                "targets": targets,
                "pairs": unique_values(
                    [
                        target["pair"]
                        for target in targets
                    ]
                ),
                "timeframes": unique_values(
                    [
                        target["timeframe"]
                        for target in targets
                    ]
                ),
                "supported_timeframes": list(
                    TIMEFRAME_MAP
                ),
            }
        )

    if not optional_admin_authorized():
        return jsonify(
            {
                "error": "unauthorized"
            }
        ), 401

    body = request.get_json(
        force=True,
        silent=True,
    ) or {}

    pairs = body.get(
        "pairs",
        [],
    )

    timeframes = body.get(
        "timeframes",
        [],
    )

    if not isinstance(pairs, list):
        return jsonify(
            {
                "error": "pairs must be a JSON list"
            }
        ), 400

    if not isinstance(timeframes, list):
        return jsonify(
            {
                "error": (
                    "timeframes must be a JSON list"
                )
            }
        ), 400

    invalid_pairs = [
        pair
        for pair in pairs
        if not canonical_symbol(pair)
    ]

    if invalid_pairs:
        return jsonify(
            {
                "error": "Unsupported symbols",
                "invalid": invalid_pairs,
            }
        ), 400

    try:
        targets = replace_scanner_targets(
            pairs,
            timeframes,
        )

        return jsonify(
            {
                "ok": True,
                "message": (
                    "Scanner targets updated"
                ),
                "targets": targets,
                "pairs": unique_values(
                    [
                        target["pair"]
                        for target in targets
                    ]
                ),
                "timeframes": unique_values(
                    [
                        target["timeframe"]
                        for target in targets
                    ]
                ),
            }
        )

    except ValueError as exc:
        return jsonify(
            {
                "error": str(exc)
            }
        ), 400


@app.get("/scanner/status")
def scanner_status_api():
    config = scanner_settings()

    return jsonify(
        {
            "running": bool(
                SCANNER_THREAD
                and SCANNER_THREAD.is_alive()
                and not SCANNER_STOP.is_set()
            ),
            "enabled": config["enabled"],
            "server_time_utc": now_utc_string(),
            "interval_seconds": config["interval"],
            "candle_count": config["count"],
            "g_min_reach": G_MIN_REACH,
            "targets": effective_scanner_targets(config),
            "last_processed": dict(SCANNER_LAST),
            "diagnostics": {
                "last_check": dict(
                    SCANNER_DIAGNOSTICS[
                        "last_check"
                    ]
                ),
                "last_success": dict(
                    SCANNER_DIAGNOSTICS[
                        "last_success"
                    ]
                ),
                "last_error": dict(
                    SCANNER_DIAGNOSTICS[
                        "last_error"
                    ]
                ),
                "scan_count": dict(
                    SCANNER_DIAGNOSTICS[
                        "scan_count"
                    ]
                ),
            },
        }
    )


@app.post("/scanner/start")
def scanner_start_api():
    if not optional_admin_authorized():
        return jsonify(
            {
                "error": "unauthorized"
            }
        ), 401

    started = start_scanner()

    return jsonify(
        {
            "ok": True,
            "started": started,
        }
    )


@app.post("/scanner/stop")
def scanner_stop_api():
    if not optional_admin_authorized():
        return jsonify(
            {
                "error": "unauthorized"
            }
        ), 401

    stop_scanner()

    return jsonify(
        {
            "ok": True,
            "message": "Scanner stopping",
        }
    )


# ============================================================
# TELEGRAM ROUTES
# ============================================================

@app.route(
    "/telegram/register",
    methods=["GET", "POST"],
)

@app.route(
    "/telegram/register",
    methods=["GET", "POST"],
)
def telegram_register_api():
    if request.method == "GET":
        chat_id = request.args.get("chat_id")
        username = request.args.get("username", "")
    else:
        body = request.get_json(
            force=True,
            silent=True,
        ) or {}
        chat_id = body.get("chat_id")
        username = body.get("username", "")

    if not chat_id:
        return jsonify(
            {
                "error": "chat_id required"
            }
        ), 400

    with DB_LOCK:
        connection = db()
        try:
            cursor = connection.cursor()
            cursor.execute(
                """
                INSERT INTO telegram_users(
                    chat_id,
                    username,
                    active,
                    created_epoch
                )
                VALUES(%s,%s,1,%s)
                ON CONFLICT(chat_id)
                DO UPDATE SET
                    username=EXCLUDED.username,
                    active=1
                """,
                (
                    str(chat_id),
                    username,
                    int(time.time()),
                ),
            )
            connection.commit()
            cursor.close()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    return jsonify(
        {
            "ok": True,
            "chat_id": str(chat_id),
            "telegram_configured": bool(
                TELEGRAM_BOT_TOKEN
            ),
        }
    )


@app.post("/telegram/unregister")
def telegram_unregister_api():
    body = request.get_json(
        force=True,
        silent=True,
    ) or {}

    chat_id = body.get("chat_id")

    if not chat_id:
        return jsonify(
            {
                "error": "chat_id required"
            }
        ), 400

    with DB_LOCK:
        connection = db()

        try:
            cursor = connection.cursor()
            cursor.execute(
            """
            UPDATE telegram_users
            SET active=0
            WHERE chat_id=%s
            """,
            (str(chat_id),),
        )
            connection.commit()
            cursor.close()

        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    return jsonify(
        {
            "ok": True,
            "chat_id": str(chat_id),
            "active": False,
        }
    )


@app.route(
    "/telegram/test",
    methods=["GET", "POST"],
)
def telegram_test_api():
    if not TELEGRAM_BOT_TOKEN:
        return jsonify(
            {
                "ok": False,
                "error": (
                    "TELEGRAM_BOT_TOKEN is not configured"
                ),
            }
        ), 400

    if request.method == "GET":
        chat_id = request.args.get(
            "chat_id"
        )
    else:
        body = request.get_json(
            force=True,
            silent=True,
        ) or {}

        chat_id = body.get("chat_id")

    text = (
        "✅ <b>TradeSignal Test Message</b>\n\n"
        "Telegram delivery is working.\n"
        "You will receive F, G, Entry, TP and SL updates here.\n\n"
        f"🕒 Sent: <b>{now_utc_string()}</b>"
    )

    if chat_id:
        ok, message_id, detail = tg_send(
            str(chat_id),
            text,
        )

        return jsonify(
            {
                "ok": ok,
                "sent": int(ok),
                "message_id": message_id,
                "detail": detail,
            }
        )

    if not strict_admin_authorized():
        return jsonify(
            {
                "error": "unauthorized"
            }
        ), 401

    users = active_telegram_users()
    sent = 0

    for user in users:
        ok, _message_id, _detail = tg_send(
            user["chat_id"],
            text,
        )

        if ok:
            sent += 1

    return jsonify(
        {
            "ok": sent > 0,
            "targeted": len(users),
            "sent": sent,
        }
    )


@app.get("/telegram/plans")
def telegram_plans_api():
    if not strict_admin_authorized():
        return jsonify(
            {
                "error": "unauthorized"
            }
        ), 401

    with DB_LOCK:
        connection = db()

        try:
            cursor = connection.cursor()
            cursor.execute(
            """
            SELECT *
            FROM telegram_trade_alerts
            ORDER BY updated_epoch DESC
            LIMIT 300
            """
        )
            rows = cursor.fetchall()
            cursor.close()

        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    result = []

    for row in rows:
        item = dict(row)

        if item.get("plan_json"):
            try:
                item["plan"] = json.loads(
                    item["plan_json"]
                )
            except Exception:
                item["plan"] = None

        result.append(item)

    return jsonify(result)


# ============================================================
# ADMIN ROUTES
# ============================================================

@app.post("/admin/reset")
def admin_reset_api():
    if not strict_admin_authorized():
        return jsonify(
            {
                "error": "unauthorized"
            }
        ), 401

    raw_pair = request.args.get("pair")
    timeframe = request.args.get("timeframe")

    with DB_LOCK:
        connection = db()
        try:
            cursor = connection.cursor()

            if raw_pair and timeframe:
                cursor.execute(
                    """
                    DELETE FROM structures
                    WHERE pair=%s AND timeframe=%s
                    """,
                    (
                        canonical_symbol(raw_pair),
                        timeframe.upper(),
                    ),
                )
            elif raw_pair:
                cursor.execute(
                    """
                    DELETE FROM structures
                    WHERE pair=%s
                    """,
                    (
                        canonical_symbol(raw_pair),
                    ),
                )
            else:
                cursor.execute(
                    "DELETE FROM structures"
                )

            connection.commit()
            cursor.close()

        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    SCANNER_LAST.clear()

    return jsonify(
        {
            "ok": True,
            "message": "Structure memory reset",
        }
    )


@app.route(
    "/admin/db_reset",
    methods=["GET", "POST"],
)
def admin_db_reset_api():
    if not strict_admin_authorized():
        return jsonify({"error": "unauthorized"}), 401

    with DB_LOCK:
        connection = db()
        try:
            cursor = connection.cursor()
            cursor.execute("DROP TABLE IF EXISTS structures CASCADE")
            cursor.execute("DROP TABLE IF EXISTS telegram_users CASCADE")
            cursor.execute("DROP TABLE IF EXISTS scanner_targets CASCADE")
            cursor.execute("DROP TABLE IF EXISTS telegram_trade_alerts CASCADE")
            connection.commit()
            cursor.close()
            create_schema(connection)
        finally:
            connection.close()

    SCANNER_LAST.clear()

    return jsonify(
        {
            "ok": True,
            "message": "Database rebuilt",
            "type": "postgresql",
        }
    )


@app.get("/admin/database")
def admin_database_api():
    if not strict_admin_authorized():
        return jsonify(
            {
                "error": "unauthorized"
            }
        ), 401

    with DB_LOCK:
        connection = db()
        try:
            cursor = connection.cursor()

            cursor.execute("SELECT COUNT(*) FROM structures")
            structures = cursor.fetchone()["cnt"]

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM structures
                WHERE state='COMPLETE'
                """
            )
            complete = cursor.fetchone()["cnt"]

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM telegram_users
                WHERE active=1
                """
            )
            users = cursor.fetchone()["cnt"]

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM telegram_trade_alerts
                """
            )
            plans = cursor.fetchone()["cnt"]

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM telegram_trade_alerts
                WHERE trade_state='ACTIVE'
                """
            )
            active_plans = cursor.fetchone()["cnt"]

            cursor.close()
        finally:
            connection.close()

    return jsonify(
        {
            "ok": True,
            "path": DB_PATH,
            "persistent": DB_IS_PERSISTENT,
            "structures": structures,
            "complete_structures": complete,
            "telegram_users": users,
            "trade_plans": plans,
            "active_trade_plans": active_plans,
            "time_utc": now_utc_string(),
        }
    )


# ============================================================
# DIAGNOSTICS (kept for compatibility)
# ============================================================

def diagnostic_started(pair, timeframe):
    key = f"{pair}|{timeframe}"
    SCANNER_DIAGNOSTICS["last_check"][key] = now_utc_string()
    SCANNER_DIAGNOSTICS["scan_count"][key] = (
        SCANNER_DIAGNOSTICS["scan_count"].get(key, 0) + 1
    )


def diagnostic_success(pair, timeframe, result):
    key = f"{pair}|{timeframe}"
    SCANNER_DIAGNOSTICS["last_success"][key] = {
        "time": now_utc_string(),
        "skipped": result.get("skipped"),
        "completed_now": result.get("completed_now"),
        "trade_events_now": result.get("trade_events_now"),
    }


def diagnostic_error(pair, timeframe, error):
    key = f"{pair}|{timeframe}"
    SCANNER_DIAGNOSTICS["last_error"][key] = {
        "time": now_utc_string(),
        "error": str(error),
    }


# ============================================================
# START SCANNER
# ============================================================

if os.environ.get(
    "SCANNER_ENABLED",
    "true",
).lower() in (
    "1",
    "true",
    "yes",
    "on",
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
                "8080",
            )
        ),
        debug=False,
    )
