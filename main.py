import codecs
import encodings
import json
import os
import psycopg2
import psycopg2.extras
import psycopg2.pool
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
# G must reach 100% of the B -> E range, or go beyond it.
# 1.00 = G must touch E.
# Values above 1.00 require G to extend past E.
G_MIN_REACH = max(
    0.50,
    min(
        1.20,
        float(
            os.environ.get(
                "SCANNER_G_MIN_REACH",
                "1.00",
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

# Risk:Reward multiple.
# 2.0 means 1:2 — TP is twice the SL distance from entry.
TRADE_RR_MULTIPLIER = max(
    0.1,
    float(
        os.environ.get(
            "TRADE_RR_MULTIPLIER",
            "2.0",
        )
    ),
)

# ── FINAL TP SELECTION ───────────────────────────────────────
# Which TP level closes the trade and frees the slot.
#
# Valid values: "tp1", "tp2", "tp3"
#
# Historically the engine treated TP3 as the "final" target.
# The new default treats TP2 as the final target — the same price
# as the original structural "E-level" target before the TP3
# extension was added.
FINAL_TP_KEY = os.environ.get(
    "FINAL_TP_KEY",
    "tp2",
).lower()

if FINAL_TP_KEY not in ("tp1", "tp2", "tp3"):
    FINAL_TP_KEY = "tp2"

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

# ------------------------------------------------------------------
# A-ZONE RECTANGLE (Pine parity)
#
# Pine:
#   box.new(
#       f_bi(s.a.k),            <- left   = the A candle
#       s.zHigh,                <- top
#       bar_index + extendBars, <- right  = live bar projected forward
#       s.zLow                  <- bottom
#   )
#
# extendBars = input.int(25, "Extend lines (bars)")
# ------------------------------------------------------------------
A_ZONE_EXTEND_BARS = max(
    1,
    int(
        os.environ.get(
            "A_ZONE_EXTEND_BARS",
            "25",
        )
    ),
)

# ── TRADE QUALITY FILTERS ────────────────────────────────────
# These reject low-probability setups before entry.

# Minimum risk-reward ratio for TP1.
# If TP1 distance / SL distance < this value, skip the trade.
MIN_RR_TP1 = max(
    0.0,
    float(
        os.environ.get(
            "MIN_RR_TP1",
            "1.0",
        )
    ),
)

# G must reach at least this fraction of the B→E range.
# 1.0 = G must touch or exceed E. Values > 1.0 require extension.
# This is separate from G_MIN_REACH which controls G detection.
# This controls trade ENTRY quality.
G_MIN_STRENGTH_FOR_ENTRY = max(
    0.0,
    float(
        os.environ.get(
            "G_MIN_STRENGTH_FOR_ENTRY",
            "0.95",
        )
    ),
)

# Reject entry if the zone was already tapped between E and G.
# When True, a zone that was visited before G confirmed is
# considered "mitigated" and the entry is skipped.
REJECT_MITIGATED_ZONE = os.environ.get(
    "REJECT_MITIGATED_ZONE",
    "true",
).lower() in (
    "1",
    "true",
    "yes",
    "on",
)

# Minimum distance from entry zone to SL, as a multiple of ATR.
# If the zone-to-SL gap is smaller than ATR × this value,
# the SL is considered too tight and the trade is skipped.
MIN_SL_ATR_DISTANCE = max(
    0.0,
    float(
        os.environ.get(
            "MIN_SL_ATR_DISTANCE",
            "0.3",
        )
    ),
)

# Simple trend filter: use an EMA on the candle data.
# Bullish entries only when price is above this EMA.
# Bearish entries only when price is below this EMA.
# Set to 0 to disable the trend filter.
TREND_EMA_PERIOD = max(
    0,
    int(
        os.environ.get(
            "TREND_EMA_PERIOD",
            "200",
        )
    ),
)

# Pine's `bar_index` is the LIVE (still forming) bar.
# main.py works on CLOSED candles only, so the last closed candle is
# one bar behind Pine's bar_index. Setting this True adds that bar back
# so the rectangle's right edge lands on the identical chart timestamp.
A_ZONE_ANCHOR_LIVE_BAR = os.environ.get(
    "A_ZONE_ANCHOR_LIVE_BAR",
    "true",
).lower() in (
    "1",
    "true",
    "yes",
    "on",
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

    # Step Index
    "STPRNG": "STPRNG",
    "STPRNG2": "STPRNG2",
    "STPRNG3": "STPRNG3",
    "STPRNG4": "STPRNG4",
    "STPRNG5": "STPRNG5",

    # Jump Diffusion
    "JD10": "JD10",
    "JD25": "JD25",
    "JD50": "JD50",
    "JD75": "JD75",
    "JD100": "JD100",

    # Boom extended
    "BOOM50": "BOOM50",
    "BOOM150N": "BOOM150N",
    "BOOM300N": "BOOM300N",
    "BOOM600": "BOOM600",
    "BOOM900": "BOOM900",

    # Crash extended
    "CRASH50": "CRASH50",
    "CRASH150N": "CRASH150N",
    "CRASH300N": "CRASH300N",
    "CRASH600": "CRASH600",
    "CRASH900": "CRASH900",

    # 1Hz Volatility
    "1HZ10V": "1HZ10V",
    "1HZ25V": "1HZ25V",
    "1HZ50V": "1HZ50V",
    "1HZ75V": "1HZ75V",
    "1HZ100V": "1HZ100V",


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
    "BTCUSD": "cryBTCUSD",
    "ETHUSD": "cryETHUSD",

    "CRYBTCUSD": "cryBTCUSD",
    "CRYETHUSD": "cryETHUSD",
}

# ============================================================
# MARKET HOURS
# ============================================================

# Pairs that trade 24/7 — always scan
ALWAYS_ON_PREFIXES = (
    "R_",
    "1HZ",
    "BOOM",
    "CRASH",
    "STEP",
    "STPRNG",
    "JD",
    "cry",
)

# Forex and metals — closed on weekends
FOREX_PREFIXES = (
    "frx",
)

# Stock indices — complex hours, skip for now
INDICES_PREFIXES = (
    "OTC_",
)


def is_market_open(pair):
    """
    Return True if this pair is currently tradeable.

    Synthetics and crypto: always open.
    Forex and metals: closed Saturday and Sunday.
    Stock indices: skipped entirely for now.
    """
    pair = str(pair).strip()

    # Stock indices — skip entirely
    if any(pair.startswith(p) for p in INDICES_PREFIXES):
        return False

    # Synthetics and crypto — always open
    if any(pair.startswith(p) for p in ALWAYS_ON_PREFIXES):
        return True

    # Forex and metals — check if it is a weekday
    if any(pair.startswith(p) for p in FOREX_PREFIXES):
        now_utc = datetime.now(timezone.utc)
        weekday = now_utc.weekday()

        # Monday=0, Tuesday=1, ..., Friday=4,
        # Saturday=5, Sunday=6

        # Forex closes Friday 22:00 UTC
        # Forex opens Sunday 22:00 UTC
        if weekday == 5:
            # Saturday — always closed
            return False

        if weekday == 6:
            # Sunday — closed until 22:00 UTC
            return now_utc.hour >= 22

        if weekday == 4:
            # Friday — closed after 22:00 UTC
            return now_utc.hour < 22

        # Monday to Thursday — always open
        return True

    # Unknown pair type — allow scanning
    return True


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

# Connection pool for PostgreSQL
_DB_POOL = None

def get_pool():
    global _DB_POOL
    if _DB_POOL is None and DATABASE_URL:
        try:
            _DB_POOL = psycopg2.pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=8,
                dsn=DATABASE_URL,
                connect_timeout=30,
                cursor_factory=psycopg2.extras.RealDictCursor,
            )
            print("[DB] Connection pool created (max 8 connections)")
        except Exception as exc:
            print(f"[DB] Pool creation failed: {exc}")
            _DB_POOL = None
    return _DB_POOL

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

    # Pass through unknown symbols that look like
    # valid Deriv synthetic indices
    SYNTHETIC_PREFIXES = (
        "BOOM", "CRASH", "STPRNG", "JD",
        "1HZ", "R_", "STEP",
    )
    if any(raw.upper().startswith(p) for p in SYNTHETIC_PREFIXES):
        return raw.strip()

    # Pass through valid Deriv synthetic symbols
    clean = raw.strip()
    if clean and all(c.isalnum() or c == '_' for c in clean):
        return clean

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
            a_zone_left_epoch INTEGER DEFAULT 0,
            a_zone_right_epoch INTEGER DEFAULT 0,
            a_zone_anchor_epoch INTEGER DEFAULT 0,
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

    # ------------------------------------------------------------------
    # Migration: add A-zone rectangle geometry to existing databases
    # ------------------------------------------------------------------
    for column_name, column_type in (
        ("a_zone_left_epoch", "INTEGER DEFAULT 0"),
        ("a_zone_right_epoch", "INTEGER DEFAULT 0"),
        ("a_zone_anchor_epoch", "INTEGER DEFAULT 0"),
    ):
        try:
            cursor.execute(
                "ALTER TABLE structures "
                "ADD COLUMN IF NOT EXISTS "
                f"{column_name} {column_type}"
            )
        except Exception as exc:
            print(
                f"[DB] Migration skipped for {column_name}: {exc}"
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


def release_connection(connection):
    """Return connection to pool or close it."""
    if connection is None:
        return
    try:
        pool = get_pool()
        if pool:
            pool.putconn(connection)
        else:
            connection.close()
    except Exception:
        try:
            connection.close()
        except Exception:
            pass


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


# ============================================================
# A-ZONE  (port of Pine f_zone + box.new)
# ============================================================

def timeframe_seconds(timeframe):
    """Bar duration in seconds. Pine's implicit bar width."""
    return int(
        TIMEFRAME_MAP.get(
            str(timeframe).upper(),
            900,
        )
    )


def structure_a_zone(structure, candles=None, recompute=False):
    """
    Exact port of Pine:

        f_zone(Str s) =>
            float bodyLo = math.min(s.a.o, s.a.c)
            float bodyHi = math.max(s.a.o, s.a.c)
            float zl = s.dir == "BULLISH" ? s.a.l  : bodyLo
            float zh = s.dir == "BULLISH" ? bodyHi : s.a.h
            if aZoneBuf > 0
                float la = array.get(aW, s.a.k)
                if la > 0
                    if s.dir == "BULLISH"
                        zl := zl - la * aZoneBuf
                    else
                        zh := zh + la * aZoneBuf
            s.zLow  := zl
            s.zHigh := zh

    Bullish:  A candle low        -> A candle body high
    Bearish:  A candle body low   -> A candle high
    """
    if (
        not recompute
        and structure.get("a_zone_low") is not None
        and structure.get("a_zone_high") is not None
    ):
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

    # Optional ATR zone buffer (one-sided, exactly like Pine).
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


def structure_a_zone_box(structure, candles=None):
    """
    Exact port of the Pine rectangle:

        if showZone and not na(s.a)
            box.new(
                f_bi(s.a.k),            // left   = A candle bar
                s.zHigh,                // top
                bar_index + extendBars, // right  = live bar + N bars
                s.zLow                  // bottom
            )

    The box is anchored on the A candle and projected forward past the
    most recent bar. Pine redraws it on every `barstate.islast`, so the
    right edge slides forward with the market. This function reproduces
    that: pass `candles` and the projection re-anchors to the latest bar.

    Returns a chart-ready rectangle dict.
    """
    zone_low, zone_high = structure_a_zone(
        structure,
        candles,
    )

    step = timeframe_seconds(structure["timeframe"])
    left_epoch = int(structure["a"]["epoch"])

    # Pine's `bar_index` = right-most bar on the chart.
    if candles:
        anchor_epoch = int(candles[-1]["epoch"])
    elif structure.get("a_zone_anchor_epoch"):
        anchor_epoch = int(structure["a_zone_anchor_epoch"])
    else:
        anchor_epoch = left_epoch

    # Pine counts the live bar; we only hold closed bars.
    forward_bars = A_ZONE_EXTEND_BARS + (
        1 if A_ZONE_ANCHOR_LIVE_BAR else 0
    )

    right_epoch = anchor_epoch + forward_bars * step

    # A rectangle must always open to the right of A.
    if right_epoch <= left_epoch:
        right_epoch = left_epoch + forward_bars * step

    return {
        "left_epoch": left_epoch,
        "right_epoch": right_epoch,
        "anchor_epoch": anchor_epoch,
        "top": float(zone_high),
        "bottom": float(zone_low),
        "height": float(zone_high - zone_low),
        "bars_wide": int(
            (right_epoch - left_epoch) // step
        ),
        "extend_bars": A_ZONE_EXTEND_BARS,
        "bar_seconds": step,
        "left_time_utc": epoch_to_local(left_epoch),
        "right_time_utc": epoch_to_local(right_epoch),
        "anchor_time_utc": epoch_to_local(anchor_epoch),
        "direction": structure["direction"],
        "pair": structure.get("pair"),
        "timeframe": structure["timeframe"],
    }


def apply_zone_to_structure(structure, candles=None):
    """
    Writes both the price band and the rectangle geometry onto the
    structure.

    Pine calls f_zone(s) once when the candidate is created, then
    redraws the box every bar with a fresh right edge. This mirrors
    both behaviours:

      * zLow / zHigh are computed once and then cached (frozen)
      * left_epoch is permanently the A candle
      * right_epoch advances whenever fresh candles are supplied
      * with no candles, the last stored projection is reused
        (so save() never collapses the box)
    """
    box = structure_a_zone_box(
        structure,
        candles,
    )

    structure["a_zone_low"] = box["bottom"]
    structure["a_zone_high"] = box["top"]
    structure["a_zone_left_epoch"] = box["left_epoch"]
    structure["a_zone_right_epoch"] = box["right_epoch"]
    structure["a_zone_anchor_epoch"] = box["anchor_epoch"]
    structure["a_zone"] = box

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
        "a_zone_left_epoch": int(
            structure.get("a_zone_left_epoch", 0) or 0
        ),
        "a_zone_right_epoch": int(
            structure.get("a_zone_right_epoch", 0) or 0
        ),
        "a_zone_anchor_epoch": int(
            structure.get("a_zone_anchor_epoch", 0) or 0
        ),
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
                    a_zone_left_epoch,
                    a_zone_right_epoch,
                    a_zone_anchor_epoch,
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
                    %(a_zone_left_epoch)s,
                    %(a_zone_right_epoch)s,
                    %(a_zone_anchor_epoch)s,
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
                    a_zone_left_epoch=excluded.a_zone_left_epoch,
                    a_zone_right_epoch=excluded.a_zone_right_epoch,
                    a_zone_anchor_epoch=excluded.a_zone_anchor_epoch,
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
        "a_zone_left_epoch": int(
            row.get("a_zone_left_epoch") or 0
        ),
        "a_zone_right_epoch": int(
            row.get("a_zone_right_epoch") or 0
        ),
        "a_zone_anchor_epoch": int(
            row.get("a_zone_anchor_epoch") or 0
        ),
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

    # Rebuild the drawable A-zone rectangle from stored geometry.
    if (
        structure.get("a")
        and structure.get("a_zone_low") is not None
    ):
        try:
            structure["a_zone"] = structure_a_zone_box(
                structure
            )
        except Exception:
            structure["a_zone"] = None
    else:
        structure["a_zone"] = None

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

        except Exception as save_exc:
            print(f"[DB] Save failed for {values.get('pair')} {values.get('timeframe')}: {save_exc}")
            connection.rollback()
            raise
        finally:
            try:
                connection.close()
            except Exception:
                pass

    return [
        s for s in (
            row_to_structure(row)
            for row in rows
        )
        if s is not None
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
            try:
                connection.close()
            except Exception:
                pass


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
            try:
                connection.close()
            except Exception:
                pass

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
            try:
                connection.close()
            except Exception:
                pass

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
            try:
                connection.close()
            except Exception:
                pass


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
            try:
                connection.close()
            except Exception:
                pass


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

def ema_values(candles, period=200):
    """Simple EMA over close prices."""
    if not candles or period <= 0:
        return []

    output = [0.0] * len(candles)
    multiplier = 2.0 / (period + 1)

    output[0] = float(candles[0]["close"])

    for i in range(1, len(candles)):
        output[i] = (
            (float(candles[i]["close"]) - output[i - 1])
            * multiplier
            + output[i - 1]
        )

    return output


def zone_was_mitigated_before_g(structure, candles):
    """
    Check if price already tapped the A-zone between E and G.

    If so, the zone's liquidity is already consumed and a
    post-G entry is lower probability.
    """
    if not structure.get("e") or not structure.get("g"):
        return False

    zone_low, zone_high = structure_a_zone(
        structure,
        candles,
    )

    e_epoch = int(structure["e"]["epoch"])
    g_epoch = int(structure["g"]["epoch"])

    for candle in candles:
        candle_epoch = int(candle["epoch"])

        if candle_epoch <= e_epoch:
            continue

        if candle_epoch >= g_epoch:
            break

        candle_low = float(candle["low"])
        candle_high = float(candle["high"])

        if candle_low <= zone_high and candle_high >= zone_low:
            return True

    return False


def trade_quality_check(structure, candles):
    """
    Run all quality filters on a structure that is ready for entry.

    Returns: (passes: bool, reason: str)
    """
    if not structure.get("g"):
        return True, ""

    if not structure.get("e"):
        return True, ""

    if not structure.get("b"):
        return True, ""

    direction = structure["direction"]
    b_price = float(structure["b"]["price"])
    e_price = float(structure["e"]["price"])
    g_price = float(structure["g"]["price"])

    zone_low, zone_high = structure_a_zone(
        structure,
        candles,
    )

    # ── G strength check ──
    total_range = abs(e_price - b_price)

    if total_range > 0 and G_MIN_STRENGTH_FOR_ENTRY > 0:
        g_reach = abs(g_price - b_price) / total_range

        if g_reach < G_MIN_STRENGTH_FOR_ENTRY:
            return False, "G_too_weak_for_entry"

    # ── Zone mitigated before G ──
    if REJECT_MITIGATED_ZONE:
        if zone_was_mitigated_before_g(structure, candles):
            return False, "zone_mitigated_before_G"

    # ── Minimum SL distance ──
    if MIN_SL_ATR_DISTANCE > 0 and candles:
        atr_values = atrs(candles)

        b_index = None

        for i, c in enumerate(candles):
            if int(c["epoch"]) == int(structure["b"]["epoch"]):
                b_index = i
                break

        if b_index is not None and b_index < len(atr_values):
            local_atr = atr_values[b_index]

            if local_atr > 0:
                buffer = sl_buffer_for(structure, candles)

                if direction == "BULLISH":
                    sl_price = b_price - buffer
                    entry_estimate = zone_high
                else:
                    sl_price = b_price + buffer
                    entry_estimate = zone_low

                sl_distance = abs(entry_estimate - sl_price)

                if sl_distance < local_atr * MIN_SL_ATR_DISTANCE:
                    return False, "SL_too_tight"

    # ── Minimum risk-reward ──
    if MIN_RR_TP1 > 0 and candles:
        try:
            # Temporarily set a synthetic entry to compute plan.
            temp_entry = (
                zone_high
                if direction == "BULLISH"
                else zone_low
            )

            buffer = sl_buffer_for(structure, candles)

            if direction == "BULLISH":
                sl_price = b_price - buffer
            else:
                sl_price = b_price + buffer

            risk = abs(temp_entry - sl_price)

            if risk > 0:
                # Use the nearest structural target as TP1.
                targets = []

                for key in ("d", "e", "g"):
                    pt = structure.get(key)

                    if not pt:
                        continue

                    p = float(pt["price"])

                    if direction == "BULLISH" and p > temp_entry:
                        targets.append(p)
                    elif direction == "BEARISH" and p < temp_entry:
                        targets.append(p)

                if targets:
                    nearest_tp = (
                        min(targets)
                        if direction == "BULLISH"
                        else max(targets)
                    )

                    reward = abs(nearest_tp - temp_entry)
                    rr = reward / risk

                    if rr < MIN_RR_TP1:
                        return False, "risk_reward_too_low"

        except Exception:
            pass

    # ── Trend filter ──
    if TREND_EMA_PERIOD > 0 and candles:
        ema = ema_values(
            candles,
            TREND_EMA_PERIOD,
        )

        if len(ema) > 0:
            current_ema = ema[-1]
            current_close = float(candles[-1]["close"])

            if direction == "BULLISH" and current_close < current_ema:
                return False, "against_trend_ema"

            if direction == "BEARISH" and current_close > current_ema:
                return False, "against_trend_ema"

    return True, ""


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
    """
    Advance one structure through:

        D -> E -> F -> G

    IMPORTANT E LOGIC:

        Old behaviour:
            E = first valid expansion pivot after D.

        New behaviour:
            E keeps extending until a real F retracement begins.

            Bullish:
                E = highest expansion high after D
                    before price retraces to/beyond fib50.

            Bearish:
                E = lowest expansion low after D
                    before price retraces to/beyond fib50.

    F LOGIC:

        F is then selected from the final E.

        Bullish:
            F = deepest swing low after E that reaches fib50.
            F must be strictly above A-zone:
                F > a_zone_high

        Bearish:
            F = deepest swing high after E that reaches fib50.
            F must be strictly below A-zone:
                F < a_zone_low

    G LOGIC:
        G is searched only after the final F.
    """
    if structure.get("stage") in (
        "ACTIVE",
        "CLOSED",
        "INVALID",
    ):
        return structure

    index_by_epoch = {
        int(candle["epoch"]): index
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
        int(structure["b"]["epoch"])
    )
    c_index = index_by_epoch.get(
        int(structure["c"]["epoch"])
    )

    if b_index is None or c_index is None:
        return mark_invalid(
            structure,
            "pivot_missing_from_candle_window",
        )

    b_level = float(structure["b"]["price"])
    c_level = float(structure["c"]["price"])

    zone_low, zone_high = structure_a_zone(
        structure,
        candles,
    )

    def f_a_zone_violation_reason(f_price):
        """
        Strict A-zone rule.

        Bullish:
            valid only if F > zone_high

        Bearish:
            valid only if F < zone_low
        """
        f_price = float(f_price)

        if direction == "BULLISH":
            if f_price < zone_low:
                return "bullish_F_below_A_zone"

            if f_price <= zone_high:
                return "bullish_F_inside_A_zone"

            return ""

        # BEARISH
        if f_price > zone_high:
            return "bearish_F_above_A_zone"

        if f_price >= zone_low:
            return "bearish_F_inside_A_zone"

        return ""

    # --------------------------------------------------------
    # D = break of structure
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

            replacement_price = float(
                pivot_price(
                    kind,
                    candle,
                )
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
                    float(candle["high"])
                    if direction == "BULLISH"
                    else float(candle["low"])
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
        int(structure["d"]["epoch"])
    )

    if d_index is None:
        return mark_invalid(
            structure,
            "D_missing_from_candle_window",
        )

    # --------------------------------------------------------
    # Protected-B validation
    # --------------------------------------------------------
    for index in range(
        b_index + 1,
        len(candles),
    ):
        candle = candles[index]

        broke_b = (
            float(candle["low"]) <= b_level
            if direction == "BULLISH"
            else float(candle["high"]) >= b_level
        )

        if broke_b:
            return mark_invalid(
                structure,
                "protected_B_was_broken",
            )

    # --------------------------------------------------------
    # E = final expansion extreme after D
    #
    # This is the key upgrade.
    #
    # Old:
    #   first valid E pivot after D.
    #
    # New:
    #   keep updating E while the expansion extends.
    #   stop updating E only when the first real F retracement
    #   reaches fib50 using the latest E.
    # --------------------------------------------------------
    if structure.get("f") is None:
        wanted_e_kind = (
            "H"
            if direction == "BULLISH"
            else "L"
        )

        wanted_f_kind = (
            "L"
            if direction == "BULLISH"
            else "H"
        )

        best_e_data = None
        best_e_price = None
        best_e_index = None

        for kind, index, candle in pivots:
            if index <= d_index:
                continue

            # Keep extending E while same-direction expansion continues.
            if kind == wanted_e_kind:
                e_price = float(
                    pivot_price(
                        kind,
                        candle,
                    )
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
                    < local_atr * float(expansion_atr)
                ):
                    continue

                is_more_extreme_e = (
                    best_e_price is None
                    or (
                        direction == "BULLISH"
                        and e_price > best_e_price
                    )
                    or (
                        direction == "BEARISH"
                        and e_price < best_e_price
                    )
                )

                if is_more_extreme_e:
                    best_e_data = (
                        kind,
                        index,
                        candle,
                    )
                    best_e_price = e_price
                    best_e_index = index

            # Once a real F retracement appears, E is finished.
            if (
                best_e_price is not None
                and best_e_index is not None
                and kind == wanted_f_kind
                and index > best_e_index
            ):
                trial_fib50 = fibonacci_level(
                    b_level,
                    best_e_price,
                    fib_ratio,
                )

                f_test_price = float(
                    pivot_price(
                        kind,
                        candle,
                    )
                )

                reaches_trial_fib = (
                    f_test_price <= trial_fib50
                    if direction == "BULLISH"
                    else f_test_price >= trial_fib50
                )

                if reaches_trial_fib:
                    break

        if best_e_data is not None:
            _kind, _index, _candle = best_e_data

            structure["e"] = point(
                "E",
                "POST_BOS_EXPANSION",
                _candle,
                best_e_price,
            )

            structure["fib50"] = fibonacci_level(
                b_level,
                best_e_price,
                fib_ratio,
            )

            set_stage(
                structure,
                "WAITING_FOR_F",
            )

    if structure.get("e") is None:
        return structure

    e_index = index_by_epoch.get(
        int(structure["e"]["epoch"])
    )

    if e_index is None:
        return mark_invalid(
            structure,
            "E_missing_from_candle_window",
        )

    fib50 = float(structure["fib50"])

    # --------------------------------------------------------
    # F = deepest retracement after final E
    #
    # Bullish:
    #   choose lowest valid swing low after E.
    #
    # Bearish:
    #   choose highest valid swing high after E.
    #
    # Scan stops when opposite pivot reaches G territory.
    # --------------------------------------------------------
    if structure.get("f") is None:
        wanted_f_kind = (
            "L"
            if direction == "BULLISH"
            else "H"
        )

        opposite_kind = (
            "H"
            if direction == "BULLISH"
            else "L"
        )

        total_range = abs(
            float(structure["e"]["price"])
            - b_level
        )

        if total_range <= 0:
            return mark_invalid(
                structure,
                "invalid_B_E_range",
            )

        if direction == "BULLISH":
            g_threshold = (
                b_level
                + total_range * G_MIN_REACH
            )
        else:
            g_threshold = (
                b_level
                - total_range * G_MIN_REACH
            )

        best_f_data = None
        best_f_price = None

        for kind, index, candle in pivots:
            if index <= e_index:
                continue

            # Retracement ends once price pushes back to G territory.
            if kind == opposite_kind:
                opposite_price = float(
                    pivot_price(
                        kind,
                        candle,
                    )
                )

                reaches_g_area = (
                    opposite_price >= g_threshold
                    if direction == "BULLISH"
                    else opposite_price <= g_threshold
                )

                if reaches_g_area:
                    break

            if kind != wanted_f_kind:
                continue

            f_price = float(
                pivot_price(
                    kind,
                    candle,
                )
            )

            reaches_fib = (
                f_price <= fib50
                if direction == "BULLISH"
                else f_price >= fib50
            )

            if not reaches_fib:
                continue

            is_deeper_f = (
                best_f_price is None
                or (
                    direction == "BULLISH"
                    and f_price < best_f_price
                )
                or (
                    direction == "BEARISH"
                    and f_price > best_f_price
                )
            )

            if is_deeper_f:
                best_f_data = (
                    kind,
                    index,
                    candle,
                )
                best_f_price = f_price

        if best_f_price is not None:
            zone_reason = f_a_zone_violation_reason(
                best_f_price
            )

            if zone_reason:
                return mark_invalid(
                    structure,
                    zone_reason,
                )

            f_broke_protected_b = (
                best_f_price <= b_level
                if direction == "BULLISH"
                else best_f_price >= b_level
            )

            if f_broke_protected_b:
                return mark_invalid(
                    structure,
                    "F_broke_protected_B",
                )

            _kind, _index, _candle = best_f_data

            structure["f"] = point(
                "F",
                "FIB_RETRACEMENT",
                _candle,
                best_f_price,
            )

            set_stage(
                structure,
                "WAITING_FOR_G",
            )

    if structure.get("f") is None:
        return structure

    f_index = index_by_epoch.get(
        int(structure["f"]["epoch"])
    )

    if f_index is None:
        return mark_invalid(
            structure,
            "F_missing_from_candle_window",
        )

    # --------------------------------------------------------
    # Validate stored F from older database records
    # --------------------------------------------------------
    stored_f_price = float(
        structure["f"]["price"]
    )

    stored_zone_reason = f_a_zone_violation_reason(
        stored_f_price
    )

    if stored_zone_reason:
        return mark_invalid(
            structure,
            stored_zone_reason,
        )

    stored_f_broke_b = (
        stored_f_price <= b_level
        if direction == "BULLISH"
        else stored_f_price >= b_level
    )

    if stored_f_broke_b:
        return mark_invalid(
            structure,
            "F_broke_protected_B",
        )

    # --------------------------------------------------------
    # G = strength push at or beyond 100% of B -> E
    #
    # Fib mapping:
    #   0%   = B
    #   50%  = F threshold
    #   100% = E
    #
    # Bullish:
    #   valid only if G >= E
    #
    # Bearish:
    #   valid only if G <= E
    # --------------------------------------------------------
    if structure.get("g") is None:
        total_range = abs(
            float(structure["e"]["price"])
            - b_level
        )

        if total_range <= 0:
            return mark_invalid(
                structure,
                "invalid_B_E_range",
            )

        e_price = float(structure["e"]["price"])

        if direction == "BULLISH":
            g_threshold = (
                b_level
                + total_range * G_MIN_REACH
            )
            wanted_g_kind = "H"
        else:
            g_threshold = (
                b_level
                - total_range * G_MIN_REACH
            )
            wanted_g_kind = "L"

        for kind, index, candle in pivots:
            if index <= f_index:
                continue

            if kind != wanted_g_kind:
                continue

            g_price = float(
                pivot_price(
                    kind,
                    candle,
                )
            )

            # 100% and above.
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
    box = structure_a_zone_box(
        structure,
        candles,
    )

    zone_low = box["bottom"]
    zone_high = box["top"]

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
        "a_zone_box": box,
        "entry_zone_from_utc": box["left_time_utc"],
        "entry_zone_to_utc": box["right_time_utc"],
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
    """
    SL: current method (protected B +/- ATR buffer).

    TP: fixed risk:reward from that SL.

        Risk = |entry - SL|
        TP1  = 1R
        TP2  = TRADE_RR_MULTIPLIER R   (default 2R, the final target)
        TP3  = 3R                      (kept for display only)
    """
    entry = float(structure["entry_price"])

    box = structure_a_zone_box(structure, candles)
    zone_low = box["bottom"]
    zone_high = box["top"]

    buffer = sl_buffer_for(structure, candles)

    if structure["direction"] == "BULLISH":
        stop_loss = structure["b"]["price"] - buffer
    else:
        stop_loss = structure["b"]["price"] + buffer

    risk = abs(entry - stop_loss)

    if risk <= 0:
        risk = abs(
            structure["e"]["price"]
            - structure["b"]["price"]
        ) * 0.05

    rr = TRADE_RR_MULTIPLIER

    if structure["direction"] == "BULLISH":
        tp1 = entry + risk * 1.0
        tp2 = entry + risk * rr
        tp3 = entry + risk * 3.0
    else:
        tp1 = entry - risk * 1.0
        tp2 = entry - risk * rr
        tp3 = entry - risk * 3.0

    rr1 = 1.0 if risk > 0 else 0.0
    rr2 = rr if risk > 0 else 0.0
    rr3 = 3.0 if risk > 0 else 0.0

    return {
        "structure_key": structure_key(structure),
        "pair": structure["pair"],
        "timeframe": structure["timeframe"],
        "direction": structure["direction"],
        "status": "ACTIVE",
        "entry_zone_low": round(zone_low, 5),
        "entry_zone_high": round(zone_high, 5),
        "a_zone_box": box,
        "entry_zone_from_utc": box["left_time_utc"],
        "entry_zone_to_utc": box["right_time_utc"],
        "entry": round(entry, 5),
        "stop_loss": round(stop_loss, 5),
        "protected_B": round(structure["b"]["price"], 5),
        "tp1": round(tp1, 5),
        "tp2": round(tp2, 5),
        "tp3": round(tp3, 5),
        "rr_tp1": round(rr1, 2),
        "rr_tp2": round(rr2, 2),
        "rr_tp3": round(rr3, 2),
        "entry_epoch": int(structure["entry_epoch"]),
        "entry_time_utc": epoch_to_local(structure["entry_epoch"]),
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
    After G, activate the trade on the first A-zone tap.

    ENTRY RULE:
        - Candle after G must overlap the A-zone.
        - Entry price is anchored DIRECTLY on the rectangle edge:
            * BULLISH: Top edge of A-zone (zone_high)
            * BEARISH: Bottom edge of A-zone (zone_low)
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

    def broke_protected_b(candle):
        if direction == "BULLISH":
            return float(candle["low"]) <= protected_b
        return float(candle["high"]) >= protected_b

    entry_candle = None

    for candle in candles:
        candle_epoch = int(candle["epoch"])

        if candle_epoch <= g_epoch:
            continue

        # Protected B broken before entry tap -> invalidate
        if entry_candle is None and broke_protected_b(candle):
            mark_invalid(
                structure,
                "protected_B_broken_before_entry",
            )
            return False

        if entry_candle is None and touches_zone(candle):
            entry_candle = candle
            break

    # No tap yet
    if entry_candle is None:
        if setup_already_ran_to_targets(
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

    # --------------------------------------------------------
    # Zone tapped -> set entry directly ON the rectangle edge
    # --------------------------------------------------------
    structure["entry_price"] = float(
        zone_high if direction == "BULLISH" else zone_low
    )
    structure["entry_epoch"] = int(entry_candle["epoch"])

    # If this trade already hit SL or TP3 historically, clean it up
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

    set_stage(structure, "ACTIVE")
    return True

    
def setup_already_ran_to_targets(
    structure,
    candles,
    g_epoch,
    zone_low,
    zone_high,
):
    """
    After G, if the zone was touched but no valid entry candle
    formed, check whether price already reached the nearest
    structural target (D, E, or G). If so, the entry was missed
    and the slot should be freed.
    """
    direction = structure["direction"]

    # Collect targets beyond the zone.
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

    # Use the nearest target (easiest to reach).
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
    Replay candles after entry and detect historical SL or final TP.

    The final TP is controlled by FINAL_TP_KEY.
    Default: "tp2" — matches the original structural E-target,
    before the TP3 extension was added.

    Returns: (finished: bool, reason: str)
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
    except Exception as exc:
        print(
            f"[Scanner] active_trade_plan failed "
            f"in trade_already_finished: {exc}"
        )
        return False, ""

    entry_epoch = int(structure["entry_epoch"])
    direction = structure["direction"]

    try:
        stop_loss = float(plan["stop_loss"])
        final_tp = float(plan[FINAL_TP_KEY])
    except (KeyError, TypeError, ValueError) as exc:
        print(
            f"[Scanner] Plan key error in "
            f"trade_already_finished: {exc}"
        )
        return False, ""

    for candle in candles:
        candle_epoch = int(candle["epoch"])

        if candle_epoch <= entry_epoch:
            continue

        high = float(candle["high"])
        low = float(candle["low"])

        if direction == "BULLISH":
            # SL first — conservative.
            if low <= stop_loss:
                return True, "historical_sl_already_hit"

            if high >= final_tp:
                return True, f"historical_{FINAL_TP_KEY}_already_hit"

        else:
            if high >= stop_loss:
                return True, "historical_sl_already_hit"

            if low <= final_tp:
                return True, f"historical_{FINAL_TP_KEY}_already_hit"

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

    box = plan["a_zone_box"]

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
            "──────── PROJECTED A-ZONE ────────",
            f"Top: <b>{plan['entry_zone_high']}</b>",
            f"Bottom: <b>{plan['entry_zone_low']}</b>",
            f"Anchored at A: <b>{box['left_time_utc']}</b>",
            f"Projected to: <b>{box['right_time_utc']}</b>",
            f"Width: <b>{box['bars_wide']} bars</b> "
            f"(+{box['extend_bars']} forward)",
            "──────────────────────────────",
            "",
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
            try:
                connection.close()
            except Exception:
                pass


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
            try:
                connection.close()
            except Exception:
                pass



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
            try:
                connection.close()
            except Exception:
                pass


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
    """
    1. Process Telegram TP/SL for all rows with trade_state=ACTIVE.
    2. Audit all ACTIVE structures that have no Telegram row and
       close them if TP3 or SL has been hit. This ensures the
       engine frees the slot even when no Telegram user is registered.
    """
    pair = canonical_symbol(pair)
    timeframe = str(timeframe).upper()

    # ----------------------------------------------------------
    # Step 1 — Telegram plan TP/SL processing
    # ----------------------------------------------------------
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
                (pair, timeframe),
            )
            rows = cursor.fetchall()
            cursor.close()

        except Exception:
            connection.rollback()
            raise

        finally:
            connection.close()

    total_events = 0
    closed_structure_keys = set()

    for row in rows:
        if not row["plan_json"]:
            continue

        try:
            plan = json.loads(row["plan_json"])
        except Exception:
            continue

        entry_epoch = int(plan.get("entry_epoch", 0))

        if entry_epoch <= 0:
            continue

        direction = plan.get("direction")

        if direction not in ("BULLISH", "BEARISH"):
            continue

        events = []
        close_trade = False

        for candle in candles:
            candle_epoch = int(candle["epoch"])

            if candle_epoch <= entry_epoch:
                continue

            high = float(candle["high"])
            low = float(candle["low"])

            if direction == "BULLISH":
                # SL first — conservative.
                if (
                    not plan.get("sl_hit")
                    and low <= float(plan["stop_loss"])
                ):
                    plan["sl_hit"] = True
                    plan["trade_state"] = "SL_HIT"
                    events.append(
                        ("SL", plan["stop_loss"], candle_epoch)
                    )
                    close_trade = True
                    break

                if (
                    not plan.get("tp1_hit")
                    and high >= float(plan["tp1"])
                ):
                    plan["tp1_hit"] = True
                    events.append(
                        ("TP1", plan["tp1"], candle_epoch)
                    )

                    try:
                        tg_send(
                            row["chat_id"],
                            breakeven_message_text(
                                plan,
                                plan["tp1"],
                                candle_epoch,
                            ),
                            reply_to_message_id=(
                                row["active_message_id"] or None
                            ),
                        )
                    except Exception as exc:
                        print(
                            "[Telegram] Break-even "
                            f"message failed: {exc}"
                        )

                if (
                    not plan.get("tp2_hit")
                    and high >= float(plan["tp2"])
                ):
                    plan["tp2_hit"] = True
                    events.append(
                        ("TP2", plan["tp2"], candle_epoch)
                    )

                # Final TP (configurable — default is tp2).
                if (
                    not plan.get(f"{FINAL_TP_KEY}_hit")
                    and high >= float(plan[FINAL_TP_KEY])
                ):
                    plan[f"{FINAL_TP_KEY}_hit"] = True
                    plan["trade_state"] = "TP_HIT"

                    events.append(
                        (
                            FINAL_TP_KEY.upper(),
                            plan[FINAL_TP_KEY],
                            candle_epoch,
                        )
                    )

                    close_trade = True
                    break
                    events.append(
                        ("TP3", plan["tp3"], candle_epoch)
                    )
                    close_trade = True
                    break

            else:
                # BEARISH — SL first.
                if (
                    not plan.get("sl_hit")
                    and high >= float(plan["stop_loss"])
                ):
                    plan["sl_hit"] = True
                    plan["trade_state"] = "SL_HIT"
                    events.append(
                        ("SL", plan["stop_loss"], candle_epoch)
                    )
                    close_trade = True
                    break

                if (
                    not plan.get("tp1_hit")
                    and low <= float(plan["tp1"])
                ):
                    plan["tp1_hit"] = True
                    events.append(
                        ("TP1", plan["tp1"], candle_epoch)
                    )

                    try:
                        tg_send(
                            row["chat_id"],
                            breakeven_message_text(
                                plan,
                                plan["tp1"],
                                candle_epoch,
                            ),
                            reply_to_message_id=(
                                row["active_message_id"] or None
                            ),
                        )
                    except Exception as exc:
                        print(
                            "[Telegram] Break-even "
                            f"message failed: {exc}"
                        )

                if (
                    not plan.get("tp2_hit")
                    and low <= float(plan["tp2"])
                ):
                    plan["tp2_hit"] = True
                    events.append(
                        ("TP2", plan["tp2"], candle_epoch)
                    )

                # Final TP (configurable — default is tp2).
                if (
                    not plan.get(f"{FINAL_TP_KEY}_hit")
                    and low <= float(plan[FINAL_TP_KEY])
                ):
                    plan[f"{FINAL_TP_KEY}_hit"] = True
                    plan["trade_state"] = "TP_HIT"

                    events.append(
                        (
                            FINAL_TP_KEY.upper(),
                            plan[FINAL_TP_KEY],
                            candle_epoch,
                        )
                    )

                    close_trade = True
                    break
                    events.append(
                        ("TP3", plan["tp3"], candle_epoch)
                    )
                    close_trade = True
                    break

        if not events:
            continue

        active_message_id = row["active_message_id"] or None

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
                if plan.get(f"{FINAL_TP_KEY}_hit")
                else "SL_HIT"
            )

            closed_structure_keys.add(row["structure_key"])

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
                        plan.get("trade_state", "ACTIVE"),
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

    # Delete closed structures after all users are processed.
    for structure_key_value in closed_structure_keys:
        try:
            delete_structure_by_key(structure_key_value)
        except Exception as exc:
            print(
                f"[Scanner] delete_structure_by_key error: {exc}"
            )

    # ----------------------------------------------------------
    # Step 2 — Structural audit
    #
    # Find all ACTIVE structures for this pair/timeframe that
    # have no Telegram row. These were never tracked by
    # monitor_trade_alerts but still need TP/SL enforcement.
    # Without this, structures with no registered Telegram users
    # stay ACTIVE forever even after TP3 or SL is hit.
    # ----------------------------------------------------------
    active_structures = structures_for(pair, timeframe)

    for structure in active_structures:
        if structure.get("stage") != "ACTIVE":
            continue

        skey = structure_key(structure)

        # Skip structures that already had Telegram processing above.
        if skey in closed_structure_keys:
            continue

        if not structure.get("entry_epoch"):
            continue

        if not structure.get("entry_price"):
            continue

        try:
            finished, finish_reason = trade_already_finished(
                structure,
                candles,
            )
        except Exception as exc:
            print(
                f"[Scanner] Structural audit "
                f"trade_already_finished error: {exc}"
            )
            continue

        if not finished:
            continue

        print(
            f"[Scanner] Structural audit closing "
            f"{pair} {timeframe}: {finish_reason}"
        )

        trade_state = (
            "CLOSED"
            if "tp" in finish_reason.lower()
            else "SL_HIT"
        )

        try:
            close_trade_alerts_for_structure(
                skey,
                trade_state,
                finish_reason,
            )
        except Exception as exc:
            print(
                f"[Scanner] close_trade_alerts error "
                f"in structural audit: {exc}"
            )

        mark_invalid(structure, finish_reason)

        # Notify only if the plan was live, then remove the row.
        live_from = int(
            structure.get("live_from_epoch", 0) or 0
        )
        entry_epoch = int(
            structure.get("entry_epoch", 0) or 0
        )

        if entry_epoch > live_from:
            try:
                send_cancel_notifications(
                    structure,
                    finish_reason,
                )
            except Exception as exc:
                print(
                    f"[Scanner] send_cancel_notifications "
                    f"error in structural audit: {exc}"
                )

        try:
            delete_structure_by_key(skey)
        except Exception as exc:
            print(
                f"[Scanner] delete_structure_by_key error "
                f"in structural audit: {exc}"
            )

        total_events += 1

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
            try:
                connection.close()
            except Exception:
                pass

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
    """
    Process F, G and active-entry notifications.

    Important:
    G confluence is checked again on later scans when it was not
    available on the original G-confirmation scan. Telegram database
    records prevent duplicate delivery.
    """
    notifications = 0

    live_from = int(
        structure.get("live_from_epoch", 0) or 0
    )

    f_epoch = (
        int(structure["f"]["epoch"])
        if structure.get("f")
        else 0
    )

    g_epoch = (
        int(structure["g"]["epoch"])
        if structure.get("g")
        else 0
    )

    # --------------------------------------------------------
    # F notification
    # --------------------------------------------------------
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

    # --------------------------------------------------------
    # G notification
    #
    # Do not require g_epoch != previous_g_epoch here.
    # If confluence was missing when G first formed, this block
    # must retry on subsequent scans.
    # --------------------------------------------------------
    if (
        structure.get("stage") in (
            "WAITING_FOR_ENTRY",
            "ACTIVE",
        )
        and g_epoch > live_from
    ):
        confluent = True
        confluent_tf = None
        confluent_stage = None

        if REQUIRE_CONFLUENCE:
            (
                confluent,
                confluent_tf,
                confluent_stage,
            ) = check_timeframe_confluence(
                structure["pair"],
                structure["timeframe"],
                structure["direction"],
                min_stage_rank=CONFLUENCE_MIN_RANK,
            )

        if confluent:
            sent_now = send_g_notifications(
                structure,
                candles,
                confluent_tf=confluent_tf,
                confluent_stage=confluent_stage,
            )

            notifications += sent_now

            if sent_now and confluent_tf:
                print(
                    f"[Confluence] {structure['pair']} "
                    f"{structure['timeframe']} "
                    f"{structure['direction']} confirmed by "
                    f"{confluent_tf} at {confluent_stage}"
                )

        elif g_epoch != previous_g_epoch:
            # Print only on initial G formation. The confluence
            # check still retries silently on future scans.
            print(
                f"[Confluence] {structure['pair']} "
                f"{structure['timeframe']} "
                f"{structure['direction']} "
                f"G formed but no confluence yet — "
                f"holding Telegram alert"
            )

    # --------------------------------------------------------
    # Active entry notification
    #
    # This remains retry-safe because send_active_notifications()
    # checks active_message_id before sending.
    # --------------------------------------------------------
    if (
        structure.get("stage") == "ACTIVE"
        and int(structure.get("entry_epoch", 0)) > live_from
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

        if structure is None:
            continue

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
                print(
                    f"[Scanner] {structure.get('pair')} "
                    f"{structure.get('timeframe')} "
                    f"ACTIVE structure closed: {finish_reason}"
                )

                # Close the Telegram trade alert row so
                # monitor_trade_alerts does not re-process it.
                trade_state = (
                    "CLOSED"
                    if "tp" in finish_reason
                    else "SL_HIT"
                )

                try:
                    close_trade_alerts_for_structure(
                        structure_key(structure),
                        trade_state,
                        finish_reason,
                    )
                except Exception as exc:
                    print(
                        f"[Scanner] close_trade_alerts error: {exc}"
                    )

                mark_invalid(
                    structure,
                    finish_reason,
                )
                
        if structure["state"] == "INVALID":
            reason = structure.get(
                "discard_reason",
                "structure invalidated",
            )

            live_from = int(
                structure.get(
                    "live_from_epoch",
                    0,
                )
                or 0
            )

            f_epoch = (
                int(structure["f"]["epoch"])
                if structure.get("f")
                else 0
            )

            g_epoch = (
                int(structure["g"]["epoch"])
                if structure.get("g")
                else 0
            )

            entry_epoch = int(
                structure.get("entry_epoch", 0) or 0
            )

            had_live_notification_stage = (
                f_epoch > live_from
                or g_epoch > live_from
                or entry_epoch > live_from
            )

            if had_live_notification_stage:
                try:
                    send_cancel_notifications(
                        structure,
                        reason,
                    )
                except Exception as exc:
                    print(
                        f"[Scanner] send_cancel_notifications "
                        f"error: {exc}"
                    )

                try:
                    close_trade_alerts_for_structure(
                        structure_key(structure),
                        "CANCELLED",
                        reason,
                    )
                except Exception as exc:
                    print(
                        f"[Scanner] close_trade_alerts error "
                        f"on invalidation: {exc}"
                    )

            try:
                delete_structure(structure)
            except Exception as exc:
                print(
                    f"[Scanner] delete_structure error: {exc}"
                )

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

            if candidate is None:
                continue

            if candidate.get("stage") == "WAITING_FOR_ENTRY":
                confirm_closed_entry(
                    candidate,
                    candles,
                )

            if candidate.get("state") == "INVALID":
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

            if candidate is None:
                continue

            if candidate.get("stage") == "WAITING_FOR_ENTRY":
                confirm_closed_entry(
                    candidate,
                    candles,
                )

            if candidate.get("state") == "INVALID":
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
    """
    Always use Render ENV configuration.

    Database scanner_targets are ignored.

    This guarantees that every pair and every timeframe
    defined in:

        SCANNER_PAIRS
        SCANNER_TIMEFRAMES

    will be scanned every cycle.
    """

    config = config or scanner_settings()

    pairs = config.get("pairs", [])
    timeframes = config.get("timeframes", [])

    if not pairs:
        return []

    if not timeframes:
        return []

    targets = []

    for pair in pairs:
        for timeframe in timeframes:

            targets.append(
                {
                    "pair": pair,
                    "timeframe": timeframe,
                }
            )

    return targets


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

    if len(targets) > 2000:
        raise ValueError(
            "Maximum 2000 pair/timeframe combinations"
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
            try:
                connection.close()
            except Exception:
                pass

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

    # Skip only when candles unchanged AND we have structures saved
    # This ensures empty pairs always get a full scan
    if False and SCANNER_LAST.get(key) == latest_epoch:  # Skip disabled - always scan
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

    try:
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
    except Exception as exc:
        diagnostic_error(pair, timeframe, exc)
        print(f"[Scanner] {pair} {timeframe} run_scan ERROR: {exc}")
        return {
            "ok": False,
            "pair": pair,
            "timeframe": timeframe,
            "skipped": False,
            "completed_now": 0,
            "trade_events_now": 0,
            "error": str(exc),
        }

    if not result:
        return {
            "ok": False,
            "pair": pair,
            "timeframe": timeframe,
            "skipped": False,
            "completed_now": 0,
            "trade_events_now": 0,
        }

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

    consecutive_errors = 0

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

                                if not result:
                                    result = {
                                        "ok": False,
                                        "skipped": True,
                                        "completed_now": 0,
                                        "trade_events_now": 0,
                                        "reason": "no_result",
                                    }

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
                                    reason = result.get("reason", "")
                                    if reason != "market_closed":
                                        print(
                                            f"[Scanner] {pair} "
                                            f"{timeframe}: OK "
                                            f"(skipped="
                                            f"{result.get('skipped')})"
                                        )

                            except Exception as exc:
                                import traceback
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
                                print(
                                    f"[Scanner] TRACEBACK: "
                                    f"{traceback.format_exc()}"
                                )

                finally:
                    SCAN_LOCK.release()

        except Exception as exc:
            consecutive_errors += 1
            print(f"[Scanner] OUTER ERROR ({consecutive_errors}): {exc}")
            if consecutive_errors > 10:
                print("[Scanner] Too many errors - restarting scanner thread")
                consecutive_errors = 0

        else:
            consecutive_errors = 0

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
            "scanner_thread_alive": bool(
            SCANNER_THREAD
            and SCANNER_THREAD.is_alive()
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
            try:
                connection.close()
            except Exception:
                pass

    return jsonify(
        [
            row_to_structure(row)
            for row in rows
        ]
    )

@app.get("/structures/zones")
def structure_zones_api():
    """
    Returns every live A-zone rectangle, ready to draw.

    Each item mirrors Pine:
        box.new(left_epoch, top, right_epoch, bottom)
    """
    raw_pair = request.args.get("pair", "")
    raw_timeframe = request.args.get("timeframe", "")

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

    sql = (
        "SELECT * FROM structures "
        "WHERE state NOT IN ('INVALID', 'CLOSED')"
    )

    arguments = []
    clauses = []

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

    sql += " ORDER BY updated_epoch DESC LIMIT 500"

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

    zones = []

    for row in rows:
        structure = row_to_structure(row)
        box = structure.get("a_zone")

        if not box:
            continue

        zones.append(
            {
                "structure_key": structure["structure_key"],
                "pair": structure["pair"],
                "timeframe": structure["timeframe"],
                "direction": structure["direction"],
                "stage": structure["stage"],
                "box": box,
            }
        )

    return jsonify(
        {
            "ok": True,
            "count": len(zones),
            "extend_bars": A_ZONE_EXTEND_BARS,
            "zones": zones,
        }
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

        # Send Telegram cancellation notification
        try:
            send_cancel_notifications(
                structure,
                reason,
            )
        except Exception as exc:
            print(
                f"[Close] Telegram notification failed: {exc}"
            )

        # Stop Telegram TP/SL monitoring
        try:
            close_trade_alerts_for_structure(
                structure_key_value,
                trade_state,
                reason,
            )
        except Exception as exc:
            print(
                f"[Close] Trade alert update failed: {exc}"
            )

        # Mark structure as CLOSED in database
        close_structure_by_key(
            structure_key_value,
            reason,
        )

        # Clear scanner cache so next cycle
        # picks up the change immediately
        key_to_clear = None
        for k in list(SCANNER_LAST.keys()):
            if structure["pair"] in k and structure["timeframe"] in k:
                key_to_clear = k
                break

        if key_to_clear:
            SCANNER_LAST.pop(key_to_clear, None)

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

    except Exception as exc:
        print(f"[Close] Error: {exc}")
        return jsonify(
            {
                "ok": False,
                "error": str(exc),
            }
        ), 500


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
            try:
                connection.close()
            except Exception:
                pass

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
            try:
                connection.close()
            except Exception:
                pass

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
            try:
                connection.close()
            except Exception:
                pass

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
            try:
                connection.close()
            except Exception:
                pass

    SCANNER_LAST.clear()
    print("[Scanner] SCANNER_LAST cleared - next cycle will do full rescan")

    return jsonify(
        {
            "ok": True,
            "message": "Structure memory reset - full rescan will happen next cycle",
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
            try:
                connection.close()
            except Exception:
                pass

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
            try:
                connection.close()
            except Exception:
                pass

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
