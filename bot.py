import os
import re
import io
import gc
import json
import base64
import html
import time
import logging
import threading
import urllib.parse
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from typing import Dict, List, Optional, Tuple, Any

import requests
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
try:
    import mplfinance as mpf
except ImportError:
    mpf = None

try:
    from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
    from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes
    TELEGRAM_AVAILABLE = True
except ImportError:
    Update = Any  # type: ignore
    class _ContextTypesStub:
        DEFAULT_TYPE = Any
    ContextTypes = _ContextTypesStub  # type: ignore
    ApplicationBuilder = CommandHandler = MessageHandler = CallbackQueryHandler = filters = None
    InlineKeyboardButton = InlineKeyboardMarkup = None
    TELEGRAM_AVAILABLE = False

# =============================================================================
# Configuration / security
# =============================================================================

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("gaemiGTP")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
FMP_API_KEY = os.getenv("FMP_API_KEY", "").strip()
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.7-flash").strip()
PORT = int(os.getenv("PORT", "10000"))
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "20"))
MAX_CACHE_ITEMS = int(os.getenv("MAX_CACHE_ITEMS", "256"))
API_KEY_WEB = os.getenv("WEB_API_KEY", "").strip()
ALLOW_PUBLIC_WEB_API = os.getenv("ALLOW_PUBLIC_WEB_API", "false").lower() == "true"
ANALYSIS_YEARS = max(1, int(os.getenv("ANALYSIS_YEARS", "1")))
TRANSACTION_COST = float(os.getenv("TRANSACTION_COST", "0.001"))  # 0.10% per round-ish leg; applied on entry+exit.
MAX_HOLDING_BARS = max(5, int(os.getenv("MAX_HOLDING_BARS", "15")))

if not TELEGRAM_TOKEN:
    logger.warning("TELEGRAM_TOKEN is not set. Telegram bot will not start until it is configured.")

# Thread-safe bounded TTL cache.
_CACHE: Dict[str, Tuple[float, Any]] = {}
_CACHE_LOCK = threading.Lock()
_DETAIL_CACHE: Dict[str, Tuple[float, str]] = {}
_DETAIL_LOCK = threading.Lock()


def cache_get(cache: Dict, lock: threading.Lock, key: str, ttl: int = CACHE_TTL_SECONDS):
    now = time.time()
    with lock:
        item = cache.get(key)
        if not item:
            return None
        ts, value = item
        if now - ts > ttl:
            cache.pop(key, None)
            return None
        return value


def cache_set(cache: Dict, lock: threading.Lock, key: str, value: Any, max_items: int = MAX_CACHE_ITEMS):
    with lock:
        cache[key] = (time.time(), value)
        while len(cache) > max_items:
            oldest = min(cache.items(), key=lambda kv: kv[1][0])[0]
            cache.pop(oldest, None)


# =============================================================================
# Static symbol maps
# =============================================================================

CRYPTO_MAP = {
    "BTC": "BTCUSDT", "비트코인": "BTCUSDT", "비트": "BTCUSDT",
    "ETH": "ETHUSDT", "이더리움": "ETHUSDT", "이더": "ETHUSDT",
    "XRP": "XRPUSDT", "리플": "XRPUSDT", "DOGE": "DOGEUSDT", "도지코인": "DOGEUSDT",
    "SOL": "SOLUSDT", "솔라나": "SOLUSDT", "ADA": "ADAUSDT", "에이다": "ADAUSDT",
    "AVAX": "AVAXUSDT", "아발란체": "AVAXUSDT", "SKY": "SKYUSDT", "스카이": "SKYUSDT",
}

US_STOCK_MAP = {
    "엔비디아": "NVDA", "NVDA": "NVDA", "테슬라": "TSLA", "TSLA": "TSLA",
    "애플": "AAPL", "AAPL": "AAPL", "마이크로소프트": "MSFT", "마소": "MSFT", "MSFT": "MSFT",
    "아마존": "AMZN", "AMZN": "AMZN", "구글": "GOOGL", "알파벳": "GOOGL", "GOOGL": "GOOGL",
    "메타": "META", "META": "META", "AMD": "AMD", "아이온큐": "IONQ", "IONQ": "IONQ",
    "팔란티어": "PLTR", "PLTR": "PLTR", "대만반도체": "TSM", "TSM": "TSM", "TSMC": "TSM",
    "QQQ": "QQQ", "SPY": "SPY", "SOXL": "SOXL", "TQQQ": "TQQQ",
}

DOMESTIC_FAST_MAP = {
    "SK하이닉스": "000660", "하이닉스": "000660", "삼성전자": "005930", "삼전": "005930",
    "삼성전기": "009150", "삼성중공업": "010140", "SK증권": "001510", "SK스퀘어": "402340",
    "솔트룩스": "304100", "알테오젠": "196170", "에코프로": "086520", "에코프로비엠": "247540",
    "레인보우로보틱스": "277810", "카카오": "035720", "네이버": "035420", "NAVER": "035420",
    "현대차": "005380", "기아": "000270", "포스코홀딩스": "005490", "POSCO홀딩스": "005490",
    "LG에너지솔루션": "373220", "한화오션": "042660", "두산에너빌리티": "034020",
    "한화에어로스페이스": "012450", "현대모비스": "012330", "셀트리온": "068270",
    "한미반도체": "042700", "씨젠": "096530", "HLB": "028300", "에코프로머티": "450080",
    "리가켐바이오": "141080", "엔켐": "348370", "삼천당제약": "000250", "휴젤": "145020",
    "클래시스": "214150", "리노공업": "058470",
}


# =============================================================================
# HTTP helpers
# =============================================================================

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "gaemiGTP/2.0 (+https://example.invalid)"})


def http_get_json(url: str, timeout: float = 4.0, **kwargs) -> Optional[Any]:
    try:
        r = SESSION.get(url, timeout=timeout, **kwargs)
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        logger.warning("GET failed: %s | %s", url.split("?")[0], exc)
        return None


def http_post_json(url: str, payload: dict, timeout: float = 7.0, **kwargs) -> Optional[Any]:
    try:
        r = SESSION.post(url, json=payload, timeout=timeout, **kwargs)
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        logger.warning("POST failed: %s | %s", url.split("?")[0], exc)
        return None


# =============================================================================
# Data lookup
# =============================================================================


def find_stock_code(query: str) -> Tuple[Optional[str], str]:
    clean_q = re.sub(r"\s+", "", query or "").upper()
    if clean_q.isdigit() and len(clean_q) == 6:
        return clean_q, clean_q

    for name, code in DOMESTIC_FAST_MAP.items():
        if re.sub(r"\s+", "", name).upper() == clean_q:
            return code, name

    encoded = urllib.parse.quote((query or "").strip())
    url = f"https://m.stock.naver.com/front-api/search/autoComplete?query={encoded}&target=stock"
    res = http_get_json(url, timeout=3.0)
    if not isinstance(res, dict):
        return None, (query or "").strip()
    items = res.get("result", {}).get("items", []) or []
    for item in items:
        name = str(item.get("name", "")).strip()
        code = str(item.get("code", "")).strip()
        if name and code and re.sub(r"\s+", "", name).upper() == clean_q:
            return code, name

    # Do not silently choose an ambiguous first result in a financial app.
    if len(items) == 1:
        item = items[0]
        return str(item.get("code", "")).strip() or None, str(item.get("name", "")).strip()
    return None, (query or "").strip()


def get_foreign_rss_headlines(ticker: str) -> List[str]:
    ticker = (ticker or "").strip().upper()
    if not ticker:
        return []
    cached = cache_get(_CACHE, _CACHE_LOCK, f"rss:{ticker}", ttl=60)
    if cached is not None:
        return cached

    url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={urllib.parse.quote(ticker)}&region=US&lang=en-US"
    r = None
    headlines: List[str] = []
    try:
        r = SESSION.get(url, timeout=3.0)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        for item in root.findall(".//item")[:8]:
            title = item.findtext("title", default="")
            title = re.sub(r"<.*?>", "", html.unescape(title)).strip()
            if title and len(title) > 10:
                headlines.append(title)
            if len(headlines) >= 4:
                break
    except Exception as exc:
        logger.warning("RSS failed for %s: %s", ticker, exc)

    cache_set(_CACHE, _CACHE_LOCK, f"rss:{ticker}", headlines, max_items=MAX_CACHE_ITEMS)
    return headlines


def get_naver_realtime_price(code: str) -> Tuple[Optional[float], Optional[str]]:
    url = f"https://polling.finance.naver.com/api/realtime/domestic/stock/{urllib.parse.quote(code)}"
    res = http_get_json(url, timeout=3.0)
    if not isinstance(res, dict):
        return None, None
    datas = res.get("datas", []) or []
    if not datas:
        return None, None
    raw = str(datas[0].get("closePrice", "")).replace(",", "").strip()
    try:
        p = float(raw)
        return p, f"{p:,.0f} 원"
    except ValueError:
        return None, None


def _yahoo_records(symbol: str, range_value: str = "1y", interval: str = "1d", include_prepost: bool = False) -> Optional[pd.DataFrame]:
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(symbol)}"
        f"?range={urllib.parse.quote(range_value)}&interval={urllib.parse.quote(interval)}"
        f"&includePrePost={'true' if include_prepost else 'false'}&events=div%2Csplits"
    )
    res = http_get_json(url, timeout=4.0)
    try:
        result = (res or {}).get("chart", {}).get("result", [])
        if not result:
            return None
        root = result[0]
        ts = root.get("timestamp", [])
        quote = root.get("indicators", {}).get("quote", [{}])[0]
        rows = []
        for i, t in enumerate(ts):
            o = quote.get("open", [None] * len(ts))[i]
            h = quote.get("high", [None] * len(ts))[i]
            l = quote.get("low", [None] * len(ts))[i]
            c = quote.get("close", [None] * len(ts))[i]
            v = quote.get("volume", [0] * len(ts))[i]
            if None in (o, h, l, c):
                continue
            rows.append({
                "Date": pd.to_datetime(t, unit="s", utc=True).tz_convert("Asia/Seoul").tz_localize(None),
                "Open": float(o), "High": float(h), "Low": float(l), "Close": float(c), "Volume": float(v or 0.0)
            })
        if not rows:
            return None
        return pd.DataFrame(rows).set_index("Date").sort_index()
    except Exception as exc:
        logger.warning("Yahoo parse failed for %s: %s", symbol, exc)
        return None


def get_crypto_data(symbol: str, limit: int = 500) -> Tuple[Optional[float], Optional[str], Optional[pd.DataFrame]]:
    clean = symbol.replace("-", "").upper()
    if not clean.endswith("USDT"):
        clean += "USDT"
    cached = cache_get(_CACHE, _CACHE_LOCK, f"binance:{clean}:{limit}", ttl=10)
    if cached is not None:
        return cached
    url = f"https://api.binance.com/api/v3/klines?symbol={clean}&interval=1d&limit={limit}"
    res = http_get_json(url, timeout=4.0)
    if not isinstance(res, list) or len(res) < 30:
        return None, None, None
    try:
        rows = [{
            "Date": pd.to_datetime(int(x[0]), unit="ms"),
            "Open": float(x[1]), "High": float(x[2]), "Low": float(x[3]),
            "Close": float(x[4]), "Volume": float(x[5]),
        } for x in res]
        df = pd.DataFrame(rows).set_index("Date").sort_index()
        price = float(df["Close"].iloc[-1])
        out = (price, f"{price:,.2f} 달러 (USDT)", df)
        cache_set(_CACHE, _CACHE_LOCK, f"binance:{clean}:{limit}", out, max_items=MAX_CACHE_ITEMS)
        return out
    except Exception as exc:
        logger.warning("Binance parse failed for %s: %s", clean, exc)
        return None, None, None


def get_us_stock_data(symbol: str, years: int = 1) -> Tuple[Optional[float], Optional[str], Optional[pd.DataFrame]]:
    days = max(365, min(365 * years + 30, 3650))
    range_value = f"{days}d"
    cached = cache_get(_CACHE, _CACHE_LOCK, f"us:{symbol}:{range_value}", ttl=20)
    if cached is not None:
        return cached
    df = _yahoo_records(symbol.upper().strip(), range_value=range_value, interval="1d", include_prepost=False)
    if df is None or len(df) < 60:
        return None, None, None
    price = float(df["Close"].iloc[-1])
    out = (price, f"{price:,.2f} 달러", df)
    cache_set(_CACHE, _CACHE_LOCK, f"us:{symbol}:{range_value}", out, max_items=MAX_CACHE_ITEMS)
    return out


def get_domestic_stock_data(code: str, years: int = 1) -> Tuple[Optional[float], Optional[str], Optional[pd.DataFrame]]:
    cache_key = f"kr:{code}:{years}"
    cached = cache_get(_CACHE, _CACHE_LOCK, cache_key, ttl=20)
    if cached is not None:
        return cached
    realtime_price, realtime_text = get_naver_realtime_price(code)
    df = _yahoo_records(f"{code}.KS", range_value=f"{max(365, 365*years+30)}d", interval="1d", include_prepost=False)
    if df is None or len(df) < 60:
        df = _yahoo_records(f"{code}.KQ", range_value=f"{max(365, 365*years+30)}d", interval="1d", include_prepost=False)
    if df is None or len(df) < 60:
        return None, None, None

    # Never rescale historical candles to make them match a different source.
    # Use the same historical source for technical calculations, while Naver is
    # used only as the displayed current snapshot when available.
    if realtime_price is None:
        realtime_price = float(df["Close"].iloc[-1])
        realtime_text = f"{realtime_price:,.0f} 원"
    out = (float(realtime_price), realtime_text, df)
    cache_set(_CACHE, _CACHE_LOCK, cache_key, out, max_items=MAX_CACHE_ITEMS)
    return out


# =============================================================================
# Technical / ICT-style structure analysis
# =============================================================================


def wilder_rsi(close: pd.Series, period: int = 14) -> float:
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - 100 / (1 + rs)
    val = rsi.iloc[-1]
    if np.isnan(val):
        return 50.0
    if np.isinf(val):
        return 100.0
    return float(val)


def atr_wilder(df: pd.DataFrame, period: int = 14) -> pd.Series:
    prev_close = df["Close"].shift(1)
    tr = pd.concat([
        df["High"] - df["Low"],
        (df["High"] - prev_close).abs(),
        (df["Low"] - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()


def swing_points(df: pd.DataFrame, left: int = 2, right: int = 2) -> Tuple[List[Tuple[int, float]], List[Tuple[int, float]]]:
    highs: List[Tuple[int, float]] = []
    lows: List[Tuple[int, float]] = []
    h = df["High"].to_numpy()
    l = df["Low"].to_numpy()
    for i in range(left, len(df) - right):
        if h[i] == np.max(h[i-left:i+right+1]) and h[i] > np.max(h[i-left:i]) and h[i] >= np.max(h[i+1:i+right+1]):
            highs.append((i, float(h[i])))
        if l[i] == np.min(l[i-left:i+right+1]) and l[i] < np.min(l[i-left:i]) and l[i] <= np.min(l[i+1:i+right+1]):
            lows.append((i, float(l[i])))
    return highs, lows


def find_recent_fvg(df: pd.DataFrame, lookback: int = 30) -> Optional[Dict[str, Any]]:
    start = max(2, len(df) - lookback)
    for i in range(len(df) - 2, start - 1, -1):
        # Bullish FVG: current low > high two candles back.
        if float(df["Low"].iloc[i]) > float(df["High"].iloc[i-2]):
            return {
                "type": "bullish", "index": i,
                "low": float(df["High"].iloc[i-2]), "high": float(df["Low"].iloc[i])
            }
        # Bearish FVG: current high < low two candles back.
        if float(df["High"].iloc[i]) < float(df["Low"].iloc[i-2]):
            return {
                "type": "bearish", "index": i,
                "low": float(df["High"].iloc[i]), "high": float(df["Low"].iloc[i-2])
            }
    return None


def find_recent_order_block(df: pd.DataFrame, direction: str, lookback: int = 30) -> Optional[Dict[str, Any]]:
    start = max(1, len(df) - lookback)
    for i in range(len(df) - 2, start - 1, -1):
        o, h, l, c = map(float, [df["Open"].iloc[i], df["High"].iloc[i], df["Low"].iloc[i], df["Close"].iloc[i]])
        n_c = float(df["Close"].iloc[i+1])
        atr = float(df["ATR"].iloc[i+1]) if "ATR" in df.columns and not np.isnan(df["ATR"].iloc[i+1]) else float(df["High"].iloc[i] - df["Low"].iloc[i])
        body = abs(c - o)
        # A simple displacement confirmation rather than a single candle reversal.
        if direction == "LONG" and c < o and n_c > h and abs(n_c - o) >= max(atr * 0.5, body * 1.2):
            return {"type": "bullish", "index": i, "low": l, "high": h}
        if direction == "SHORT" and c > o and n_c < l and abs(n_c - o) >= max(atr * 0.5, body * 1.2):
            return {"type": "bearish", "index": i, "low": l, "high": h}
    return None


def classify_structure(df: pd.DataFrame) -> Dict[str, Any]:
    highs, lows = swing_points(df)
    curr = float(df["Close"].iloc[-1])
    prev = float(df["Close"].iloc[-2])
    recent_highs = highs[-3:]
    recent_lows = lows[-3:]

    last_high = recent_highs[-1][1] if recent_highs else np.nan
    prev_high = recent_highs[-2][1] if len(recent_highs) >= 2 else np.nan
    last_low = recent_lows[-1][1] if recent_lows else np.nan
    prev_low = recent_lows[-2][1] if len(recent_lows) >= 2 else np.nan

    bullish_structure = bool(not np.isnan(last_high) and not np.isnan(prev_high) and not np.isnan(last_low) and not np.isnan(prev_low)
                             and last_high > prev_high and last_low > prev_low)
    bearish_structure = bool(not np.isnan(last_high) and not np.isnan(prev_high) and not np.isnan(last_low) and not np.isnan(prev_low)
                             and last_high < prev_high and last_low < prev_low)

    bullish_break = bool(not np.isnan(last_high) and curr > last_high)
    bearish_break = bool(not np.isnan(last_low) and curr < last_low)

    # Liquidity sweep: current/previous bar traded beyond a recent swing then closed back inside.
    bullish_sweep = bool(not np.isnan(last_low) and df["Low"].iloc[-1] < last_low and curr > last_low)
    bearish_sweep = bool(not np.isnan(last_high) and df["High"].iloc[-1] > last_high and curr < last_high)

    if bullish_break or bullish_structure or bullish_sweep:
        bias = "BULLISH"
    elif bearish_break or bearish_structure or bearish_sweep:
        bias = "BEARISH"
    else:
        bias = "NEUTRAL"

    return {
        "highs": recent_highs, "lows": recent_lows,
        "last_high": last_high, "prev_high": prev_high,
        "last_low": last_low, "prev_low": prev_low,
        "bullish_structure": bullish_structure, "bearish_structure": bearish_structure,
        "bullish_break": bullish_break, "bearish_break": bearish_break,
        "bullish_sweep": bullish_sweep, "bearish_sweep": bearish_sweep,
        "bias": bias, "prev_close": prev, "curr_close": curr,
    }


def _round_price(v: float, is_krw: bool) -> float:
    return round(float(v), 0 if is_krw else 2)


def build_signal(df: pd.DataFrame, raw_price: float, asset_type: str) -> Dict[str, Any]:
    if df is None or len(df) < 80:
        raise ValueError("기술적 분석에는 최소 80개 봉이 필요합니다.")

    work = df.copy()
    work["ATR"] = atr_wilder(work, 14)
    work["MA20"] = work["Close"].rolling(20).mean()
    work["MA60"] = work["Close"].rolling(60).mean()
    work["VolAvg5Prev"] = work["Volume"].shift(1).rolling(5).mean()

    atr = float(work["ATR"].iloc[-1])
    if not np.isfinite(atr) or atr <= 0:
        atr = float((work["High"] - work["Low"]).tail(14).mean())
    ma20 = float(work["MA20"].iloc[-1])
    ma60 = float(work["MA60"].iloc[-1])
    rsi = wilder_rsi(work["Close"], 14)
    vol_avg_prev = float(work["VolAvg5Prev"].iloc[-1]) if np.isfinite(work["VolAvg5Prev"].iloc[-1]) else float(work["Volume"].tail(5).mean())
    vol_ratio = float(work["Volume"].iloc[-1] / vol_avg_prev * 100) if vol_avg_prev > 0 else 100.0

    structure = classify_structure(work)
    fvg = find_recent_fvg(work)

    long_ob = find_recent_order_block(work, "LONG")
    short_ob = find_recent_order_block(work, "SHORT")

    is_krw = asset_type == "domestic_stock"
    current = float(raw_price)
    candidates: List[Dict[str, Any]] = []

    # Score-based decision: require structural alignment; do not force a long in a box.
    long_score = 0.0
    if structure["bullish_break"]: long_score += 2.5
    elif structure["bullish_structure"]: long_score += 1.5
    if structure["bullish_sweep"]: long_score += 1.5
    if current >= ma20: long_score += 0.8
    if ma20 >= ma60: long_score += 0.8
    if 45 <= rsi <= 68: long_score += 0.5
    if vol_ratio >= 120: long_score += 0.5
    if fvg and fvg["type"] == "bullish": long_score += 0.5
    if long_ob: long_score += 0.5

    short_score = 0.0
    if structure["bearish_break"]: short_score += 2.5
    elif structure["bearish_structure"]: short_score += 1.5
    if structure["bearish_sweep"]: short_score += 1.5
    if current <= ma20: short_score += 0.8
    if ma20 <= ma60: short_score += 0.8
    if 32 <= rsi <= 55: short_score += 0.5
    if vol_ratio >= 120: short_score += 0.5
    if fvg and fvg["type"] == "bearish": short_score += 0.5
    if short_ob: short_score += 0.5

    if long_score >= 5.0 and long_score > short_score + 0.75:
        direction = "LONG"
    elif short_score >= 5.0 and short_score > long_score + 0.75:
        direction = "SHORT"
    else:
        direction = "WAIT"

    # Structural entry zone.
    if direction == "LONG":
        if long_ob:
            entry_low = max(long_ob["low"], current - 0.9 * atr)
            entry_high = min(long_ob["high"], current)
        elif fvg and fvg["type"] == "bullish":
            entry_low, entry_high = fvg["low"], min(fvg["high"], current)
        else:
            entry_low, entry_high = current - 0.6 * atr, current
        if entry_low >= entry_high:
            entry_low, entry_high = current - 0.5 * atr, current

        invalidation = (long_ob["low"] if long_ob else (structure["last_low"] if np.isfinite(structure["last_low"]) else current - atr)) - 0.15 * atr
        reference_targets = [x[1] for x in structure["highs"] if x[1] > current]
        target1 = min(reference_targets) if reference_targets else current + 2.0 * max(current - invalidation, 0.5 * atr)
        target2_base = current + 3.0 * max(current - invalidation, 0.75 * atr)
        target2 = max(target1 * 1.03, target2_base)
        risk_price = max(current - invalidation, 0.5 * atr)
        rr1 = (target1 - current) / risk_price if risk_price > 0 else 0
        rr2 = (target2 - current) / risk_price if risk_price > 0 else 0
        candidates = [{"score": long_score, "direction": direction, "entry_low": entry_low, "entry_high": entry_high,
                       "stop": invalidation, "target1": target1, "target2": target2, "rr1": rr1, "rr2": rr2}]
    elif direction == "SHORT":
        if short_ob:
            entry_low = max(short_ob["low"], current)
            entry_high = min(short_ob["high"], current + 0.9 * atr)
        elif fvg and fvg["type"] == "bearish":
            entry_low, entry_high = max(fvg["low"], current), fvg["high"]
        else:
            entry_low, entry_high = current, current + 0.6 * atr
        if entry_low >= entry_high:
            entry_low, entry_high = current, current + 0.5 * atr

        invalidation = (short_ob["high"] if short_ob else (structure["last_high"] if np.isfinite(structure["last_high"]) else current + atr)) + 0.15 * atr
        reference_targets = [x[1] for x in structure["lows"] if x[1] < current]
        target1 = max(reference_targets) if reference_targets else current - 2.0 * max(invalidation - current, 0.5 * atr)
        target2_base = current - 3.0 * max(invalidation - current, 0.75 * atr)
        target2 = min(target1 * 0.97, target2_base)
        risk_price = max(invalidation - current, 0.5 * atr)
        rr1 = (current - target1) / risk_price if risk_price > 0 else 0
        rr2 = (current - target2) / risk_price if risk_price > 0 else 0
        candidates = [{"score": short_score, "direction": direction, "entry_low": entry_low, "entry_high": entry_high,
                       "stop": invalidation, "target1": target1, "target2": target2, "rr1": rr1, "rr2": rr2}]

    quality = max(long_score, short_score) / 7.1 * 5.0
    quality = float(max(1.0, min(5.0, quality)))
    stars = "★★★★★" if quality >= 4.5 else "★★★★☆" if quality >= 3.8 else "★★★☆☆" if quality >= 2.8 else "★★☆☆☆" if quality >= 1.8 else "★☆☆☆☆"

    ma_desc = (
        f"현재가가 20일선({ma20:,.0f}{'원' if is_krw else '달러'}) 및 60일선({ma60:,.0f}{'원' if is_krw else '달러'}) 상단입니다."
        if current >= ma20 and current >= ma60 else
        f"현재가가 20일선({ma20:,.0f}{'원' if is_krw else '달러'})과 60일선({ma60:,.0f}{'원' if is_krw else '달러'}) 사이입니다."
        if current >= ma20 else
        f"현재가가 20일선({ma20:,.0f}{'원' if is_krw else '달러'}) 및 60일선({ma60:,.0f}{'원' if is_krw else '달러'}) 하단입니다."
    )

    def fp(v: float) -> float:
        return _round_price(v, is_krw)

    result = {
        "direction": direction,
        "score": round(quality, 1),
        "stars": stars,
        "pattern_name": {
            "LONG": "상승 구조 + 유동성 회수형",
            "SHORT": "하락 구조 + 유동성 회수형",
            "WAIT": "구조 불일치 / 관망",
        }[direction],
        "rsi": round(rsi, 1),
        "atr": fp(atr),
        "ma20": fp(ma20), "ma60": fp(ma60),
        "vol_ratio": int(round(vol_ratio)),
        "structure": structure,
        "fvg": fvg,
        "long_ob": long_ob,
        "short_ob": short_ob,
        "ma_fact_desc": ma_desc,
        "long_score": round(long_score, 2),
        "short_score": round(short_score, 2),
    }
    if candidates:
        c = candidates[0]
        for k, v in c.items():
            if k in {"entry_low", "entry_high", "stop", "target1", "target2"}:
                result[k] = fp(float(v))
            elif k in {"score", "rr1", "rr2"}:
                result[k] = round(float(v), 2)
            else:
                result[k] = v
    else:
        result.update({"entry_low": None, "entry_high": None, "stop": None, "target1": None, "target2": None, "rr1": 0.0, "rr2": 0.0, "score_raw": max(long_score, short_score)})
    return result


# =============================================================================
# Backtest: same signal family, no look-ahead, costs, compounding
# =============================================================================


def _signal_direction_for_bar(work: pd.DataFrame, i: int) -> str:
    # Use only information through bar i.
    sub = work.iloc[:i+1].copy()
    if len(sub) < 80:
        return "WAIT"
    sig = build_signal(sub, float(sub["Close"].iloc[-1]), "overseas_stock")
    return sig["direction"]


def backtest_strategy(df: pd.DataFrame, years: int = 1) -> Optional[Dict[str, Any]]:
    if df is None or len(df) < 120:
        return None
    work = df.copy()
    atrs = atr_wilder(work, 14)
    work["ATR"] = atrs
    equity = 1.0
    peak = 1.0
    max_dd = 0.0
    trades: List[Dict[str, Any]] = []
    i = 80

    while i < len(work) - 2:
        try:
            sig = build_signal(work.iloc[:i+1], float(work["Close"].iloc[i]), "overseas_stock")
        except Exception:
            i += 1
            continue
        direction = sig["direction"]
        if direction == "WAIT" or sig.get("stop") is None or sig.get("target2") is None:
            i += 1
            continue

        entry_idx = i + 1
        entry = float(work["Open"].iloc[entry_idx])
        stop = float(sig["stop"])
        target = float(sig["target2"])
        if direction == "LONG" and (entry <= stop or entry >= target):
            i += 1
            continue
        if direction == "SHORT" and (entry >= stop or entry <= target):
            i += 1
            continue

        entry_cost = 1 - TRANSACTION_COST
        exit_cost = 1 - TRANSACTION_COST
        exit_price = None
        outcome = "TIME"
        exit_idx = min(entry_idx + MAX_HOLDING_BARS, len(work)-1)

        for j in range(entry_idx, exit_idx + 1):
            high = float(work["High"].iloc[j])
            low = float(work["Low"].iloc[j])
            if direction == "LONG":
                # Conservative assumption when both are touched in the same bar: stop first.
                if low <= stop:
                    exit_price, outcome, exit_idx = stop, "LOSS", j
                    break
                if high >= target:
                    exit_price, outcome, exit_idx = target, "WIN", j
                    break
            else:
                if high >= stop:
                    exit_price, outcome, exit_idx = stop, "LOSS", j
                    break
                if low <= target:
                    exit_price, outcome, exit_idx = target, "WIN", j
                    break
        if exit_price is None:
            exit_price = float(work["Close"].iloc[exit_idx])

        if direction == "LONG":
            gross = (exit_price / entry) - 1
        else:
            gross = (entry / exit_price) - 1
        net = ((1 + gross) * entry_cost * exit_cost) - 1
        equity *= (1 + net)
        peak = max(peak, equity)
        max_dd = max(max_dd, (peak - equity) / peak)
        trades.append({"direction": direction, "outcome": outcome, "net": net, "entry": entry, "exit": exit_price, "entry_idx": entry_idx, "exit_idx": exit_idx})
        i = exit_idx + 1

    if len(trades) < 5:
        return None

    wins = [t["net"] for t in trades if t["net"] > 0]
    losses = [abs(t["net"]) for t in trades if t["net"] < 0]
    win_rate = len(wins) / len(trades) * 100
    profit_factor = sum(wins) / sum(losses) if losses else None
    expectancy = float(np.mean([t["net"] for t in trades]))
    return {
        "total": len(trades), "wins": len(wins), "losses": len(losses),
        "win_rate": round(win_rate, 1), "compound_return": round((equity - 1) * 100, 1),
        "max_drawdown": round(max_dd * 100, 1),
        "profit_factor": round(profit_factor, 2) if profit_factor is not None else None,
        "expectancy": round(expectancy * 100, 3),
        "years": years,
    }


def format_backtest_text(name: str, symbol: str, bt: Optional[Dict[str, Any]]) -> str:
    if not bt:
        return f"🏆 {name} ({symbol})\n\n검증 데이터가 충분하지 않거나 유효한 거래가 5건 미만입니다."
    pf = f"{bt['profit_factor']:.2f}" if bt["profit_factor"] is not None else "N/A"
    return (
        f"🏆 {name} ({symbol}) {bt['years']}년 검증\n\n"
        f"거래 수: {bt['total']}건\n"
        f"승률: {bt['win_rate']:.1f}%\n"
        f"복리 누적수익: {bt['compound_return']:+.1f}%\n"
        f"최대낙폭: {bt['max_drawdown']:.1f}%\n"
        f"Profit Factor: {pf}\n"
        f"거래당 기대값: {bt['expectancy']:+.3f}%"
    )


# =============================================================================
# Fundamentals / news / Gemini
# =============================================================================


def synthesize_issue_headline_gemini(stock_name: str, raw_issues: List[str]) -> str:
    if not GEMINI_API_KEY or not raw_issues:
        return raw_issues[0] if raw_issues else ""
    valid = []
    for x in raw_issues:
        s = re.sub(r"<.*?>", "", html.unescape(str(x))).strip()
        if s and not s.startswith("http") and s not in valid:
            valid.append(s)
    if not valid:
        return ""
    prompt = (
        f"종목 {stock_name}에 대한 아래 뉴스 제목만 사용하세요. "
        "사실을 추가하지 말고 핵심 재료를 한국어 1문장으로 요약하세요.\n- " + "\n- ".join(valid[:4])
    )
    text = gemini_generate(prompt, max_output_tokens=120)
    return re.sub(r"[\"'\n]", "", text).strip() if text else valid[0]


def fetch_extra_fundamentals(symbol: str, asset_type: str, stock_name: str = "") -> Dict[str, str]:
    info = {"supply_status": "", "financial_status": "", "recent_news": ""}
    raw_issues: List[str] = []

    if asset_type == "overseas_stock":
        raw_issues.extend(get_foreign_rss_headlines(symbol))
        if FMP_API_KEY:
            url = f"https://financialmodelingprep.com/api/v3/key-metrics-ttm/{urllib.parse.quote(symbol)}?apikey={urllib.parse.quote(FMP_API_KEY)}"
            res = http_get_json(url, timeout=3.0)
            if isinstance(res, list) and res:
                m = res[0] or {}
                parts = []
                if m.get("peRatioTTM") is not None: parts.append(f"PER {float(m['peRatioTTM']):.2f}배")
                if m.get("pbRatioTTM") is not None: parts.append(f"PBR {float(m['pbRatioTTM']):.2f}배")
                if m.get("roeTTM") is not None: parts.append(f"ROE {float(m['roeTTM'])*100:.2f}%")
                info["financial_status"] = " / ".join(parts)
    elif asset_type == "crypto":
        base = symbol.replace("USDT", "").upper()
        raw_issues.extend(get_foreign_rss_headlines(f"{base}-USD"))
    else:
        raw_issues_url = f"https://m.stock.naver.com/api/news/stock/{urllib.parse.quote(symbol)}?pageSize=6&page=1"
        res = http_get_json(raw_issues_url, timeout=3.0)
        if isinstance(res, dict):
            items = res.get("items") or res.get("newsList") or res.get("result") or []
            if isinstance(items, list):
                for n in items:
                    title = n.get("title") or n.get("tit") or n.get("itemTitle") or ""
                    title = re.sub(r"<.*?>", "", html.unescape(str(title))).strip()
                    if title and title not in raw_issues:
                        raw_issues.append(title)
        integ = http_get_json(f"https://m.stock.naver.com/api/stock/{urllib.parse.quote(symbol)}/integration", timeout=3.0)
        if isinstance(integ, dict):
            items = integ.get("totalInfos", []) or []
            per = pbr = foreign = ""
            for item in items:
                code, value = item.get("code"), item.get("value")
                if code == "per" and value: per = value
                elif code == "pbr" and value: pbr = value
                elif code == "foreignRate" and value: foreign = value
            if foreign: info["supply_status"] = f"외국인 지분율 {foreign}%"
            parts = []
            if per: parts.append(f"PER {per}배")
            if pbr: parts.append(f"PBR {pbr}배")
            info["financial_status"] = " / ".join(parts)

    info["recent_news"] = synthesize_issue_headline_gemini(stock_name, raw_issues)
    return info


def gemini_generate(prompt: str, max_output_tokens: int = 1000) -> str:
    if not GEMINI_API_KEY:
        return ""
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{urllib.parse.quote(GEMINI_MODEL)}:generateContent"
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.15, "maxOutputTokens": max_output_tokens},
    }
    res = http_post_json(
        url,
        payload,
        timeout=8.0,
        headers={"x-goog-api-key": GEMINI_API_KEY},
    )
    try:
        return res["candidates"][0]["content"]["parts"][0]["text"].strip()
    except Exception:
        return ""


def generate_deep_report_gemini(stock_name: str, symbol: str, price: str, an: dict, extra_info: dict) -> str:
    if not GEMINI_API_KEY:
        return (
            f"{an['ma_fact_desc']}\n\n"
            f"구조 방향: {an['direction']} / RSI {an['rsi']} / 거래량비 {an['vol_ratio']}%.\n"
            "AI 해설은 GEMINI_API_KEY가 설정되면 활성화됩니다."
        )
    prompt = f"""
당신은 금융 분석 보조 모델입니다. 아래의 수치와 구조 데이터만 사용해 한국어로 5~8문장 분석을 작성하세요.
추측하거나 존재하지 않는 기관수급/호가창/체결강도/뉴스 사실을 만들어내지 마세요.
매수·매도 확정 표현 대신 조건부 시나리오로 설명하세요.

종목: {stock_name} ({symbol})
현재가: {price}
방향: {an['direction']}
패턴: {an['pattern_name']}
RSI: {an['rsi']}
ATR: {an['atr']}
20일선: {an['ma20']}
60일선: {an['ma60']}
5일 선행평균 대비 거래량: {an['vol_ratio']}%
구조 바이어스: {an['structure']['bias']}
상승 구조: {an['structure']['bullish_structure']}
하락 구조: {an['structure']['bearish_structure']}
상승 돌파: {an['structure']['bullish_break']}
하락 돌파: {an['structure']['bearish_break']}
상승 유동성 스윕: {an['structure']['bullish_sweep']}
하락 유동성 스윕: {an['structure']['bearish_sweep']}
최근 FVG: {an['fvg']}
이평 팩트: {an['ma_fact_desc']}
뉴스: {extra_info.get('recent_news', '')}
재무: {extra_info.get('financial_status', '')}
수급: {extra_info.get('supply_status', '')}

반드시 구조 데이터와 일치하게 작성하고, 데이터가 없는 내용은 쓰지 마세요.
"""
    ai_text = gemini_generate(prompt, max_output_tokens=1000)
    if ai_text:
        return ai_text
    return (
        f"{an['ma_fact_desc']}\n\n"
        f"현재 구조는 {an['direction']}이며 RSI {an['rsi']:.1f}, 거래량은 직전 5개 봉 평균 대비 {an['vol_ratio']}%입니다.\n"
        f"상승 구조/하락 구조: {an['structure']['bullish_structure']} / {an['structure']['bearish_structure']}, "
        f"상승 돌파/하락 돌파: {an['structure']['bullish_break']} / {an['structure']['bearish_break']}입니다.\n"
        "뉴스·재무 데이터는 확보된 값만 참고하며, 신호는 수익을 보장하지 않습니다."
    )


# =============================================================================
# Chart
# =============================================================================


def generate_chart_image(df: pd.DataFrame, title_text: str = "") -> Optional[bytes]:
    if df is None or df.empty or len(df) < 20:
        return None
    try:
        plot_df = df.tail(60).copy()
        buf = io.BytesIO()
        if mpf is not None:
            mc = mpf.make_marketcolors(up="#ef4444", down="#3b82f6", edge="inherit", wick="inherit", volume="inherit")
            style = mpf.make_mpf_style(base_mpf_style="nightclouds", marketcolors=mc, gridcolor="#2a2a2a", facecolor="#1e1e1e", edgecolor="#1e1e1e", figcolor="#1e1e1e")
            mpf.plot(
                plot_df, type="candle", mav=(20, 60), volume=True, style=style,
                title=title_text[:60], savefig=dict(fname=buf, dpi=110, bbox_inches="tight", pad_inches=0.08), figsize=(8, 4.8)
            )
        else:
            fig, ax = plt.subplots(figsize=(8, 4.8))
            ax.plot(plot_df.index, plot_df["Close"], linewidth=1.2, label="Close")
            ax.plot(plot_df.index, plot_df["Close"].rolling(20).mean(), linewidth=1.0, label="MA20")
            ax.plot(plot_df.index, plot_df["Close"].rolling(60).mean(), linewidth=1.0, label="MA60")
            ax.set_title(title_text[:60])
            ax.grid(alpha=0.2)
            ax.legend()
            fig.tight_layout()
            fig.savefig(buf, format="png", dpi=110)
            plt.close(fig)
        return buf.getvalue()
    except Exception as exc:
        logger.warning("Chart generation failed: %s", exc)
        return None
    finally:
        plt.close("all")
        gc.collect()


# =============================================================================
# Full analysis
# =============================================================================


def resolve_symbol(query: str) -> Tuple[str, str, str]:
    raw = (query or "").strip()
    clean = re.sub(r"\s+", "", raw).upper()
    if clean.endswith("USDT") or clean in CRYPTO_MAP:
        return "crypto", CRYPTO_MAP.get(clean, clean), raw
    if clean in US_STOCK_MAP:
        return "overseas_stock", US_STOCK_MAP[clean], raw
    if re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,7}", clean) and clean not in DOMESTIC_FAST_MAP:
        return "overseas_stock", clean, raw
    code, name = find_stock_code(raw)
    if not code:
        raise ValueError(f"'{raw}' 종목을 찾지 못했습니다.")
    return "domestic_stock", code, name


def fetch_technical_data(symbol: str, asset_type: str, stock_name: str):
    if asset_type == "crypto":
        return get_crypto_data(symbol, 500)
    if asset_type == "overseas_stock":
        return get_us_stock_data(symbol, ANALYSIS_YEARS)
    return get_domestic_stock_data(symbol, ANALYSIS_YEARS)


def run_full_analysis_process(query: str) -> Dict[str, Any]:
    asset_type, symbol, formal_name = resolve_symbol(query)
    curr_price, price_text, df = fetch_technical_data(symbol, asset_type, formal_name)
    if curr_price is None or df is None or len(df) < 80:
        raise ValueError("가격/봉 데이터가 충분하지 않습니다.")

    with ThreadPoolExecutor(max_workers=3) as pool:
        f_extra = pool.submit(fetch_extra_fundamentals, symbol, asset_type, formal_name)
        f_bt = pool.submit(backtest_strategy, df.copy(), ANALYSIS_YEARS)
        f_chart = pool.submit(generate_chart_image, df.copy(), f"{formal_name} ({symbol})")
        extra = f_extra.result()
        bt = f_bt.result()
        chart_bytes = f_chart.result()

    analysis = build_signal(df, float(curr_price), asset_type)

    detail = generate_deep_report_gemini(formal_name, symbol, price_text, analysis, extra)
    direction = analysis["direction"]
    if direction in {"LONG", "SHORT"}:
        if direction == "LONG":
            ret1 = (analysis["target1"] / curr_price - 1) * 100
            ret2 = (analysis["target2"] / curr_price - 1) * 100
            risk = (1 - analysis["stop"] / curr_price) * 100
        else:
            ret1 = (curr_price / analysis["target1"] - 1) * 100
            ret2 = (curr_price / analysis["target2"] - 1) * 100
            risk = (analysis["stop"] / curr_price - 1) * 100
        signal_text = (
            f"스마트머니 구조 신호\n\n"
            f"방향성: {direction}\n"
            f"진입 구간: {analysis['entry_low']:,.0f} ~ {analysis['entry_high']:,.0f}\n"
            f"1차 목표: {analysis['target1']:,.0f} (+{ret1:.1f}%)\n"
            f"2차 목표: {analysis['target2']:,.0f} (+{ret2:.1f}%)\n"
            f"손절 무효화: {analysis['stop']:,.0f} (-{risk:.1f}%)\n"
            f"목표 R/R: {analysis['rr1']:.2f}R / {analysis['rr2']:.2f}R\n\n"
            f"구조: {analysis['structure']['bias']}\n"
            f"상승 돌파: {analysis['structure']['bullish_break']} / 하락 돌파: {analysis['structure']['bearish_break']}\n"
            f"상승 스윕: {analysis['structure']['bullish_sweep']} / 하락 스윕: {analysis['structure']['bearish_sweep']}"
        )
    else:
        signal_text = (
            "스마트머니 구조 신호\n\n"
            "방향성: WAIT\n"
            "현재 구조가 롱/숏 어느 한쪽으로 충분히 정렬되지 않아 관망 구간으로 판정합니다.\n\n"
            f"상승 점수: {analysis['long_score']:.2f} / 하락 점수: {analysis['short_score']:.2f}\n"
            f"구조: {analysis['structure']['bias']}"
        )

    summary = (
        f"현재가 {price_text}\n\n"
        f"현재 트레이딩 적합도 {analysis['stars']} (점수 {analysis['score']:.1f})\n\n"
        f"패턴: {analysis['pattern_name']}\n"
        f"방향: {direction}\n"
        f"RSI: {analysis['rsi']:.1f} / 거래량비: {analysis['vol_ratio']}%\n"
        f"{analysis['ma_fact_desc']}\n\n"
        f"※ 이 시스템은 데이터 기반 조건 신호이며 수익을 보장하지 않습니다."
    )

    return {
        "formal_name": formal_name, "symbol": symbol, "asset_type": asset_type,
        "price": price_text, "summary": summary, "detail": detail,
        "signal": signal_text, "news_raw": extra.get("recent_news", ""),
        "chart_bytes": chart_bytes, "chart_b64": base64.b64encode(chart_bytes).decode("ascii") if chart_bytes else "",
        "bt_info": bt, "analysis": analysis,
    }


# =============================================================================
# Telegram
# =============================================================================


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message:
        await update.message.reply_text(
            "gaemiGTP 가동 중입니다. 종목명을 입력하세요.\n"
            "예: 하이닉스, 엔비디아, BTC\n"
            "백테스트: 검증 하이닉스"
        )


async def analyze(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return
    query = update.message.text.strip()
    if re.match(r"^(?:/verify|/검증|검증)", query, flags=re.IGNORECASE):
        clean_sub = re.sub(r"^(?:/verify|/검증|검증)\s*", "", query, flags=re.IGNORECASE).strip()
        if not clean_sub:
            await update.message.reply_text("검증할 종목명을 입력해주세요.\n예: 검증 하이닉스")
            return
        status = await update.message.reply_text(f"🔍 {clean_sub} 백테스트 검증 중...")
        try:
            res = await __import__("asyncio").to_thread(run_full_analysis_process, clean_sub)
            await status.edit_text(format_backtest_text(res["formal_name"], res["symbol"], res["bt_info"]))
        except Exception as exc:
            logger.exception("Telegram backtest error")
            await status.edit_text(f"❌ 검증 실패: {str(exc)[:120]}")
        return

    status = await update.message.reply_text(f"{query} 분석 중...")
    try:
        res = await __import__("asyncio").to_thread(run_full_analysis_process, query)
        if res.get("chart_bytes"):
            await update.message.reply_photo(photo=io.BytesIO(res["chart_bytes"]))
        await status.delete()
        keyboard = [[InlineKeyboardButton("gaemiGTP 시그널", callback_data=f"detail:{res['symbol']}")]]
        cache_set(_DETAIL_CACHE, _DETAIL_LOCK, res["symbol"], res["signal"], max_items=MAX_CACHE_ITEMS)
        await update.message.reply_text(res["summary"], reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as exc:
        logger.exception("Telegram analysis error")
        await status.edit_text(f"❌ 분석 실패: {str(exc)[:160]}")


async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    data = q.data or ""
    if data.startswith("detail:"):
        symbol = data.split(":", 1)[1]
        detail = cache_get(_DETAIL_CACHE, _DETAIL_LOCK, symbol, ttl=3600) or "시그널을 불러올 수 없습니다."
        await q.message.reply_text(f"🎯 gaemiGTP 시그널\n\n{detail}")


# =============================================================================
# Web UI/API
# =============================================================================


HTML_PAGE = r'''<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>gaemiGTP - 구조 기반 퀀트 분석</title>
<style>
*{box-sizing:border-box;margin:0;padding:0;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif}
html,body{height:100%;background:#1e1f20;color:#e3e3e3}
body{overflow:hidden}
header{height:56px;display:flex;align-items:center;justify-content:space-between;padding:0 20px}
.logo{font-weight:700;font-size:1.15rem;cursor:pointer}.new{background:#282a2c;color:#ccc;border:0;border-radius:20px;padding:7px 14px;cursor:pointer}
#home,#results{width:100%;height:calc(100% - 56px)}
#home{display:flex;align-items:center;justify-content:center;flex-direction:column;padding:16px}
.hero{font-size:3rem;font-weight:700;margin-bottom:8px}.sub{color:#8e918f;margin-bottom:24px;text-align:center}
.search{display:flex;width:100%;max-width:720px;background:#282a2c;border-radius:32px;padding:6px 8px 6px 16px}
.search input{flex:1;background:transparent;border:0;outline:0;color:#fff;padding:8px;font-size:.98rem}.search button{width:38px;height:38px;border:0;border-radius:50%;background:#3a3c3e;color:#fff;cursor:pointer}
.tags{display:flex;gap:8px;flex-wrap:wrap;justify-content:center;margin-top:18px}.tag{background:#282a2c;border:0;color:#ccc;border-radius:16px;padding:7px 12px;cursor:pointer}
#results{display:none;overflow:auto;padding:16px}.inner{max-width:720px;margin:auto}.loading{text-align:center;color:#8e918f;padding:50px 0}.card{background:#282a2c;border-radius:16px;padding:20px;margin-bottom:16px}.chart{width:100%;border-radius:12px;overflow:hidden;margin-bottom:16px}.chart img{width:100%;display:block}.title{font-size:1.15rem;font-weight:700;margin-bottom:14px}.issue{color:#fef08a;font-weight:600;margin-bottom:14px}.summary{white-space:pre-wrap;line-height:1.7}.signal{display:none;white-space:pre-wrap;border-top:1px solid #3a3c3e;margin-top:16px;padding-top:16px;line-height:1.7;color:#ccc}.detail{margin-top:14px;width:100%;background:#3a3c3e;color:#fff;border:0;border-radius:10px;padding:11px;cursor:pointer}
</style>
</head><body>
<header><div class="logo" onclick="home()">gaemiGTP</div><button class="new" onclick="home()">+ 새 검색</button></header>
<section id="home"><div class="hero">gaemiGTP</div><div class="sub">구조·유동성·변동성 기반 분석</div>
<div class="search"><input id="q" placeholder="SK하이닉스, NVDA, BTC" onkeydown="if(event.key==='Enter')search()"><button onclick="search()">↑</button></div>
<div class="tags"><button class="tag" onclick="search('SK하이닉스')">🔥 SK하이닉스</button><button class="tag" onclick="search('엔비디아')">🚀 엔비디아</button><button class="tag" onclick="search('비트코인')">₿ 비트코인</button></div>
</section>
<section id="results"><div class="inner"><div id="loading" class="loading">분석 중...</div><div id="box" style="display:none"><div class="chart" id="chartBox"><img id="chart" alt="캔들 차트"></div><div class="card"><div id="title" class="title"></div><div id="issue" class="issue"></div><div id="summary" class="summary"></div><button class="detail" onclick="toggleSignal()">gaemiGTP 시그널</button><div id="signal" class="signal"></div></div></div></div></section>
<script>
let lastRequest=0;
function home(){document.getElementById('home').style.display='flex';document.getElementById('results').style.display='none';}
async function search(value){const q=(value||document.getElementById('q').value).trim();if(!q)return;const req=++lastRequest;document.getElementById('home').style.display='none';document.getElementById('results').style.display='block';document.getElementById('loading').style.display='block';document.getElementById('box').style.display='none';try{const r=await fetch('/api/analyze?q='+encodeURIComponent(q),{headers:{'X-API-Key':window.__API_KEY||''}});const d=await r.json();if(req!==lastRequest)return;if(!r.ok)throw new Error(d.error||'분석 실패');document.getElementById('title').innerText=d.name+' ('+d.symbol+')';document.getElementById('issue').innerText=d.news_raw?('👉 '+d.news_raw):'';document.getElementById('issue').style.display=d.news_raw?'block':'none';document.getElementById('summary').innerText=d.summary;document.getElementById('signal').innerText=d.signal||'';if(d.chart){document.getElementById('chart').src='data:image/png;base64,'+d.chart;document.getElementById('chartBox').style.display='block'}else document.getElementById('chartBox').style.display='none';document.getElementById('loading').style.display='none';document.getElementById('box').style.display='block'}catch(e){document.getElementById('loading').innerText='❌ '+e.message}}
function toggleSignal(){const e=document.getElementById('signal');e.style.display=e.style.display==='block'?'none':'block'}
</script></body></html>'''


def _authorized_web_request(handler: BaseHTTPRequestHandler) -> bool:
    if ALLOW_PUBLIC_WEB_API:
        return True
    if not API_KEY_WEB:
        return False
    provided = handler.headers.get("X-API-Key", "")
    return provided and provided == API_KEY_WEB


class ApiAndWebServer(BaseHTTPRequestHandler):
    server_version = "gaemiGTP/2.0"

    def _json(self, code: int, body: dict):
        raw = json.dumps(body, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(raw)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Allow", "GET, OPTIONS")
        self.end_headers()

    def do_GET(self):
        try:
            parsed = urllib.parse.urlparse(self.path)
            if parsed.path == "/api/analyze":
                if not _authorized_web_request(self):
                    self._json(401, {"error": "웹 API 인증이 필요합니다."})
                    return
                params = urllib.parse.parse_qs(parsed.query)
                q = (params.get("q", [""])[0] or "").strip()
                if not q or len(q) > 50:
                    self._json(400, {"error": "종목명을 확인해주세요."})
                    return
                try:
                    res = run_full_analysis_process(q)
                    self._json(200, {
                        "success": True, "name": res["formal_name"], "symbol": res["symbol"], "price": res["price"],
                        "summary": res["summary"], "signal": res["signal"], "detail": res["detail"],
                        "news_raw": res["news_raw"], "chart": res["chart_b64"], "backtest": res["bt_info"],
                    })
                except Exception as exc:
                    logger.exception("Web analysis error")
                    self._json(422, {"error": str(exc)[:200]})
                return

            raw = HTML_PAGE.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
        except Exception as exc:
            logger.exception("HTTP handler error: %s", exc)
            try:
                self._json(500, {"error": "서버 처리 중 오류가 발생했습니다."})
            except Exception:
                pass

    def log_message(self, fmt, *args):
        logger.info("HTTP %s", fmt % args)


def run_web_server():
    server = ThreadingHTTPServer(("0.0.0.0", PORT), ApiAndWebServer)
    logger.info("Web server listening on %s", PORT)
    server.serve_forever()


# =============================================================================
# Entrypoint
# =============================================================================


def main():
    threading.Thread(target=run_web_server, daemon=True, name="web-server").start()
    if not TELEGRAM_AVAILABLE or not TELEGRAM_TOKEN:
        if not TELEGRAM_AVAILABLE:
            logger.warning("Telegram disabled: python-telegram-bot is not installed.")
        else:
            logger.warning("Telegram disabled: TELEGRAM_TOKEN is missing. Web server remains available if WEB_API_KEY/public access is configured.")
        while True:
            time.sleep(3600)
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, analyze))
    app.add_handler(CallbackQueryHandler(button_callback))
    logger.info("Telegram bot starting")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
