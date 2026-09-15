import threading
import time
from concurrent.futures import Future
import urllib.parse

from performance import HTTP_SESSION


DOMESTIC_MARKET_TYPE_CACHE = {}
DOMESTIC_MARKET_TYPE_CACHE_TTL = 24 * 60 * 60
DOMESTIC_MARKET_TYPE_INFLIGHT = {}
DOMESTIC_MARKET_TYPE_LOCK = threading.Lock()


def get_naver_stock_market_type(code):
    """Naver basic 정보에서 종목의 실제 KOSPI/KOSDAQ 구분을 읽는다."""
    try:
        url = f"https://m.stock.naver.com/api/stock/{code}/basic"
        res = HTTP_SESSION.get(url, timeout=1.8).json()
        exchange = res.get("stockExchangeType") or {}
        name = exchange.get("name") if isinstance(exchange, dict) else None
        if name in {"KOSPI", "KOSDAQ"}:
            return name
    except Exception:
        pass
    return None


def _get_cached_domestic_market_type(code):
    key = str(code or "").strip().upper()
    if not key:
        return None
    item = DOMESTIC_MARKET_TYPE_CACHE.get(key)
    if item and time.time() - item["time"] < DOMESTIC_MARKET_TYPE_CACHE_TTL:
        return item.get("value")
    return None


def _refresh_domestic_market_type(code):
    key = str(code or "").strip().upper()
    if not key:
        return None
    with DOMESTIC_MARKET_TYPE_LOCK:
        future = DOMESTIC_MARKET_TYPE_INFLIGHT.get(key)
        if future is None:
            future = Future()
            DOMESTIC_MARKET_TYPE_INFLIGHT[key] = future
            owner = True
        else:
            owner = False
    if not owner:
        try:
            return future.result(timeout=3.0)
        except Exception:
            return None
    try:
        value = get_naver_stock_market_type(key)
        DOMESTIC_MARKET_TYPE_CACHE[key] = {"time": time.time(), "value": value}
        future.set_result(value)
        return value
    except Exception as exc:
        try:
            future.set_result(None)
        except Exception:
            pass
        print(f"[DOMESTIC MARKET TYPE] {key} refresh failed: {type(exc).__name__}: {exc}")
        return None
    finally:
        with DOMESTIC_MARKET_TYPE_LOCK:
            DOMESTIC_MARKET_TYPE_INFLIGHT.pop(key, None)
