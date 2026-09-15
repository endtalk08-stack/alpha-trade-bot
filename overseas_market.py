"""Overseas stock candle data helpers.

Behavior-preserving extraction from bot.py.
"""
import time
import threading
import urllib.parse
from concurrent.futures import Future

import pandas as pd

from performance import HTTP_SESSION


# ===== SPEED OPTIMIZATION 2: overseas candle cache =====
# 5년치 일봉을 매 검색마다 Yahoo에서 다시 받지 않는다.
# 기본 TTL은 10분이며, 캐시가 없거나 만료된 경우에만 갱신한다.
OVERSEAS_CANDLE_CACHE = {}
OVERSEAS_CANDLE_CACHE_TTL = 10 * 60
# 동일 미국 종목에 /api/analyze와 다른 요청이 동시에 들어와도
# Yahoo 5년 일봉을 한 번만 가져오도록 single-flight를 적용한다.
OVERSEAS_CANDLE_INFLIGHT = {}
OVERSEAS_CANDLE_INFLIGHT_LOCK = threading.Lock()

def fetch_overseas_candles(symbol):
    """
    해외주식 차트 데이터 조회 + 서버 메모리 TTL 캐시 + single-flight.

    같은 종목을 여러 요청이 동시에 요구하면 첫 요청만 Yahoo를 호출하고
    나머지는 그 결과를 기다린다. 기존 반환 계약은 그대로 유지한다:
      (chart_close, price_text, df)
    """
    key = str(symbol or "").strip().upper()
    if not key:
        return None, None, None

    now = time.time()
    cached = OVERSEAS_CANDLE_CACHE.get(key)
    if cached and (now - cached.get("time", 0)) < OVERSEAS_CANDLE_CACHE_TTL:
        try:
            df = cached["df"].copy()
            chart_close = float(df["Close"].iloc[-1])
            return chart_close, f"{chart_close:,.2f} 달러", df
        except Exception:
            OVERSEAS_CANDLE_CACHE.pop(key, None)

    # 같은 티커의 동시 Yahoo 요청을 하나로 합친다.
    with OVERSEAS_CANDLE_INFLIGHT_LOCK:
        inflight = OVERSEAS_CANDLE_INFLIGHT.get(key)
        if inflight is None:
            inflight = Future()
            OVERSEAS_CANDLE_INFLIGHT[key] = inflight
            owner = True
        else:
            owner = False

    if not owner:
        try:
            value = inflight.result(timeout=7.0)
            if not value or value[2] is None:
                return None, None, None
            chart_close, price_text, df = value
            return chart_close, price_text, df.copy()
        except Exception as exc:
            print(f"[YAHOO CHART WAIT ERROR] {key}: {type(exc).__name__}: {exc}")
            return None, None, None

    try:
        encoded_sym = urllib.parse.quote(key)
        url = (
            f"https://query1.finance.yahoo.com/v8/finance/chart/"
            f"{encoded_sym}?range=5y&interval=1d&includePrePost=false"
        )
        response = HTTP_SESSION.get(
            url,
            timeout=5.0,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        response.raise_for_status()

        res = response.json()
        chart = res.get("chart") or {}
        if chart.get("error"):
            raise RuntimeError(f"Yahoo chart error: {chart.get('error')}")

        result = chart.get("result") or []
        if not result:
            raise RuntimeError("Yahoo chart result가 비어 있습니다.")

        item = result[0]
        ts = item.get("timestamp") or []
        quote = (item.get("indicators") or {}).get("quote") or [{}]
        quote = quote[0] if quote else {}

        if not ts or not quote:
            raise RuntimeError("Yahoo chart에 timestamp/quote 데이터가 없습니다.")

        opens = quote.get("open") or []
        highs = quote.get("high") or []
        lows = quote.get("low") or []
        closes = quote.get("close") or []
        volumes = quote.get("volume") or []

        n = min(len(ts), len(opens), len(highs), len(lows), len(closes), len(volumes))
        records = []
        for i in range(n):
            if any(v is None for v in (opens[i], highs[i], lows[i], closes[i])):
                continue
            records.append({
                "Date": pd.to_datetime(ts[i], unit="s"),
                "Open": float(opens[i]),
                "High": float(highs[i]),
                "Low": float(lows[i]),
                "Close": float(closes[i]),
                "Volume": float(volumes[i] or 0.0),
            })

        if len(records) < 15:
            raise RuntimeError(f"유효한 일봉 데이터가 부족합니다: {len(records)}개")

        df = pd.DataFrame(records).set_index("Date").sort_index()
        chart_close = float(df["Close"].iloc[-1])
        result_value = (chart_close, f"{chart_close:,.2f} 달러", df)

        OVERSEAS_CANDLE_CACHE[key] = {
            "time": time.time(),
            "df": df.copy(),
        }
        try:
            inflight.set_result(result_value)
        except Exception:
            pass
        return chart_close, f"{chart_close:,.2f} 달러", df

    except Exception as exc:
        print(f"[YAHOO CHART ERROR] {key}: {type(exc).__name__}: {exc}")
        try:
            inflight.set_result((None, None, None))
        except Exception:
            pass
        return None, None, None
    finally:
        with OVERSEAS_CANDLE_INFLIGHT_LOCK:
            OVERSEAS_CANDLE_INFLIGHT.pop(key, None)


def _fetch_overseas_quote_snapshot_uncached(symbol):
    """
    해외주식 현재가/세션 데이터 조회.
    Yahoo quote의 세션 정보를 기준으로 현재 표시 가격을 결정한다.
    반환값은 차트용 일봉 데이터와 분리한다.
    """
    try:
        encoded_sym = urllib.parse.quote(symbol.upper())
        url = (
            f"https://query1.finance.yahoo.com/v7/finance/quote"
            f"?symbols={encoded_sym}"
        )
        response = HTTP_SESSION.get(
            url,
            timeout=5.0,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        response.raise_for_status()
        payload = response.json()

        result = ((payload.get("quoteResponse") or {}).get("result") or [])
        if not result:
            raise RuntimeError("Yahoo quote result가 비어 있습니다.")

        q = result[0]
        state = str(q.get("marketState") or "CLOSED").upper()
        regular_price = q.get("regularMarketPrice")
        previous_close = q.get("regularMarketPreviousClose")

        # 정규장 가격을 기본값으로 사용하고,
        # PRE/POST에서는 확장시장 가격이 있으면 그 값을 사용한다.
        price = regular_price
        price_source = "regular"
        change_pct = q.get("regularMarketChangePercent")

        if state in ("PRE", "PREPRE"):
            pre_price = q.get("preMarketPrice")
            if pre_price is not None:
                price = pre_price
                price_source = "pre"
                change_pct = q.get("preMarketChangePercent")

        elif state in ("POST", "POSTPOST"):
            post_price = q.get("postMarketPrice")
            if post_price is not None:
                price = post_price
                price_source = "post"
                change_pct = q.get("postMarketChangePercent")

        if price is None:
            raise RuntimeError("현재 가격 필드가 없습니다.")

        # 공급자가 등락률을 주지 않는 경우에만 직접 계산한다.
        if change_pct is None and previous_close:
            change_pct = ((float(price) - float(previous_close)) / float(previous_close)) * 100.0

        return {
            "symbol": q.get("symbol") or symbol.upper(),
            "price": float(price),
            "previous_close": float(previous_close) if previous_close is not None else None,
            "change_pct": float(change_pct) if change_pct is not None else None,
            "market_state": state,
            "price_source": price_source,
            "currency": q.get("currency") or "USD",
            "regular_market_price": float(regular_price) if regular_price is not None else None,
            "pre_market_price": float(q["preMarketPrice"]) if q.get("preMarketPrice") is not None else None,
            "post_market_price": float(q["postMarketPrice"]) if q.get("postMarketPrice") is not None else None,
            "timestamp": q.get("regularMarketTime"),
        }

    except Exception as exc:
        print(f"[YAHOO QUOTE ERROR] {symbol}: {type(exc).__name__}: {exc}")
        return None


# Shared overseas quote cache (moved from bot.py; behavior preserved)
OVERSEAS_QUOTE_CACHE = {}
OVERSEAS_QUOTE_CACHE_TTL = 30
MARKET_DATA_CACHE_LOCK = threading.Lock()

def fetch_overseas_quote_snapshot(symbol):
    """해외주식 현재가 공용 TTL 캐시 래퍼.

    /api/analyze, /api/chart, 미국 주도종목 상세분석에서 동일 종목의
    Yahoo quote를 반복 호출하지 않도록 STEP 3 공용 캐시를 사용한다.
    기존 호출부의 함수명/반환 형식은 그대로 유지한다.
    """
    key = str(symbol or "").strip().upper()
    if not key:
        return None

    now = time.time()
    with MARKET_DATA_CACHE_LOCK:
        cached = OVERSEAS_QUOTE_CACHE.get(key)
        if cached and (now - cached.get("time", 0.0)) < OVERSEAS_QUOTE_CACHE_TTL:
            value = cached.get("value")
            return dict(value) if isinstance(value, dict) else value

    value = _fetch_overseas_quote_snapshot_uncached(key)

    with MARKET_DATA_CACHE_LOCK:
        OVERSEAS_QUOTE_CACHE[key] = {
            "time": time.time(),
            "value": dict(value) if isinstance(value, dict) else value,
        }

    return dict(value) if isinstance(value, dict) else value
