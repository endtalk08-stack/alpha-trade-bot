import os
import re
import json
import time
import html
import threading
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

import requests
import pandas as pd
import numpy as np

# ----------------------------------------------------
# 0. 공용 HTTP 세션
# ----------------------------------------------------
HTTP_SESSION = requests.Session()
HTTP_SESSION.headers.update({
    "User-Agent": "gaemiGTP/4.0 (+https://www.gaemigtp.com)",
    "Accept": "*/*",
    "Connection": "keep-alive",
})

HTTP_TIMEOUT = (3.0, 8.0)

# ----------------------------------------------------
# 1. 기본 설정 / 종목 매핑
# ----------------------------------------------------
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
TELEGRAM_BOT_USERNAME = os.environ.get("TELEGRAM_BOT_USERNAME", "gaemigtp_bot").strip().lstrip("@")
DART_API_KEY = os.environ.get("DART_API_KEY", "").strip()

GLOBAL_INDEX_MAP = {
    "코스피": "^KS11", "KOSPI": "^KS11",
    "코스닥": "^KQ11", "KOSDAQ": "^KQ11",
    "VIX": "^VIX", "빅스": "^VIX", "변동성": "^VIX",
    "WTI유가": "CL=F", "WTI": "CL=F", "유가": "CL=F", "원유": "CL=F",
    "금": "GC=F", "GOLD": "GC=F", "골드": "GC=F",
    "환율": "KRW=X", "USD/KRW": "KRW=X", "원달러": "KRW=X",
}

CRYPTO_MAP = {
    "BTC": "BTCUSDT", "비트코인": "BTCUSDT", "비트": "BTCUSDT",
    "ETH": "ETHUSDT", "이더리움": "ETHUSDT", "이더": "ETHUSDT",
    "XRP": "XRPUSDT", "리플": "XRPUSDT",
    "DOGE": "DOGEUSDT", "도지코인": "DOGEUSDT", "도지": "DOGEUSDT",
    "SOL": "SOLUSDT", "솔라나": "SOLUSDT",
    "ADA": "ADAUSDT", "에이다": "ADAUSDT",
    "AVAX": "AVAXUSDT", "아발란체": "AVAXUSDT",
    "SKY": "SKYUSDT", "스카이": "SKYUSDT",
}

US_STOCK_MAP = {
    "엔비디아": "NVDA", "NVDA": "NVDA",
    "테슬라": "TSLA", "TSLA": "TSLA",
    "애플": "AAPL", "AAPL": "AAPL",
    "마이크로소프트": "MSFT", "마소": "MSFT", "MSFT": "MSFT",
    "아마존": "AMZN", "AMZN": "AMZN",
    "구글": "GOOGL", "알파벳": "GOOGL", "GOOGL": "GOOGL",
    "메타": "META", "META": "META",
    "AMD": "AMD", "아이온큐": "IONQ", "IONQ": "IONQ",
    "팔란티어": "PLTR", "PLTR": "PLTR",
    "대만반도체": "TSM", "TSM": "TSM", "TSMC": "TSM",
    "QQQ": "QQQ", "SPY": "SPY", "SOXL": "SOXL", "TQQQ": "TQQQ",
    "DIA": "DIA", "SOXX": "SOXX",
}

DOMESTIC_FAST_MAP = {
    "SK하이닉스": "000660", "하이닉스": "000660",
    "삼성전자": "005930", "삼전": "005930", "삼성전기": "009150", "삼성중공업": "010140",
    "SK증권": "001510", "SK스퀘어": "402340", "솔트룩스": "304100",
    "알테오젠": "196170", "에코프로": "086520", "에코프로비엠": "247540",
    "레인보우로보틱스": "277810", "카카오": "035720", "네이버": "035420", "NAVER": "035420",
    "현대차": "005380", "기아": "000270", "포스코홀딩스": "005490", "POSCO홀딩스": "005490",
    "LG에너지솔루션": "373220", "한화오션": "042660", "두산에너빌리티": "034020",
    "한화에어로스페이스": "012450", "현대모비스": "012330", "셀트리온": "068270",
    "한미반도체": "042700", "씨젠": "096530", "HLB": "028300", "에코프로머티": "450080",
    "리가켐바이오": "141080", "엔켐": "348370", "삼천당제약": "000250", "휴젤": "145020",
    "클래시스": "214150", "리노공업": "058470",
}

WORLD_BOARD_CONFIG = [
    {"name": "코스피", "sym": "^KS11", "tag": "국내"},
    {"name": "코스닥", "sym": "^KQ11", "tag": "국내"},
    {"name": "QQQ", "sym": "QQQ", "tag": "나스닥100"},
    {"name": "SPY", "sym": "SPY", "tag": "S&P500"},
    {"name": "SOXX", "sym": "SOXX", "tag": "반도체"},
    {"name": "DIA", "sym": "DIA", "tag": "다우존스"},
    {"name": "VIX", "sym": "^VIX", "tag": "변동성"},
    {"name": "USD/KRW", "sym": "KRW=X", "tag": "환율"},
    {"name": "WTI 유가", "sym": "CL=F", "tag": "원유"},
    {"name": "금", "sym": "GC=F", "tag": "안전자산"},
]

# ----------------------------------------------------
# 2. 캐시 / 텔레그램 구독 상태
# ----------------------------------------------------
CACHE_TTL = 30
WORLD_TTL = 25
ANALYSIS_CACHE = {}
CACHED_WORLD_BOARD = []

# In-memory subscriptions. Token is never hard-coded here.
TELEGRAM_SUBSCRIPTIONS = {}  # chat_id -> {symbol, last_price, alert_state}
TELEGRAM_LOCK = threading.Lock()


def safe_float(v):
    try:
        f = float(v)
        return f if np.isfinite(f) else None
    except Exception:
        return None


def yahoo_chart(symbol, period="5y", interval="1d"):
    encoded = urllib.parse.quote(symbol.upper(), safe="=^")
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{encoded}?range={period}&interval={interval}&events=history"
    res = HTTP_SESSION.get(url, timeout=HTTP_TIMEOUT)
    res.raise_for_status()
    payload = res.json()
    result = (payload.get("chart") or {}).get("result") or []
    if not result:
        raise ValueError("Yahoo Finance returned no data")
    return result[0]


def yahoo_df(symbol, period="5y", interval="1d"):
    result = yahoo_chart(symbol, period, interval)
    timestamps = result.get("timestamp") or []
    quote = ((result.get("indicators") or {}).get("quote") or [{}])[0]
    rows = []
    for i, ts in enumerate(timestamps):
        try:
            o = safe_float((quote.get("open") or [])[i])
            h = safe_float((quote.get("high") or [])[i])
            l = safe_float((quote.get("low") or [])[i])
            c = safe_float((quote.get("close") or [])[i])
            v = safe_float((quote.get("volume") or [])[i]) or 0.0
            if None in (o, h, l, c):
                continue
            rows.append({
                "Date": pd.to_datetime(ts, unit="s", utc=True).tz_convert("Asia/Seoul").tz_localize(None),
                "Open": o, "High": h, "Low": l, "Close": c, "Volume": v,
            })
        except Exception:
            continue
    if not rows:
        raise ValueError("No valid OHLC rows")
    return pd.DataFrame(rows).set_index("Date").sort_index()


def fetch_single_world_item(item):
    name, sym, tag = item["name"], item["sym"], item["tag"]
    try:
        result = yahoo_chart(sym, "7d", "1d")
        meta = result.get("meta") or {}
        curr = safe_float(meta.get("regularMarketPrice"))
        prev = safe_float(meta.get("chartPreviousClose")) or safe_float(meta.get("previousClose"))
        if curr is None:
            return {"name": name, "tag": tag, "price": "-", "rate": "-", "up": True, "sparkline": []}
        change = ((curr - prev) / prev * 100.0) if prev else None
        closes = (((result.get("indicators") or {}).get("quote") or [{}])[0].get("close") or [])
        spark = [round(float(c), 4) for c in closes if safe_float(c) is not None][-7:]
        if sym == "KRW=X":
            price_str = f"{curr:,.1f}원"
        elif sym in ("^KS11", "^KQ11", "^VIX"):
            price_str = f"{curr:,.2f}"
        else:
            price_str = f"{curr:,.2f}"
        return {
            "name": name,
            "tag": tag,
            "price": price_str,
            "rate": f"{change:+.2f}%" if change is not None else "-",
            "up": change >= 0 if change is not None else True,
            "sparkline": spark,
        }
    except Exception:
        return {"name": name, "tag": tag, "price": "-", "rate": "-", "up": True, "sparkline": []}


def background_world_board_worker():
    global CACHED_WORLD_BOARD
    while True:
        try:
            with ThreadPoolExecutor(max_workers=10) as ex:
                results = list(ex.map(fetch_single_world_item, WORLD_BOARD_CONFIG))
            if results:
                CACHED_WORLD_BOARD = results
        except Exception:
            pass
        time.sleep(WORLD_TTL)


def fetch_all_world_board_parallel():
    with ThreadPoolExecutor(max_workers=10) as ex:
        return list(ex.map(fetch_single_world_item, WORLD_BOARD_CONFIG))


def fetch_rss_news(query, limit=5):
    try:
        q = urllib.parse.quote_plus(query)
        url = f"https://news.google.com/rss/search?q={q}&hl=ko&gl=KR&ceid=KR:ko"
        res = HTTP_SESSION.get(url, timeout=HTTP_TIMEOUT)
        res.raise_for_status()
        root = ET.fromstring(res.text)
        items = []
        for item in root.findall(".//item")[:limit]:
            title = html.unescape(item.findtext("title") or "").strip()
            link = (item.findtext("link") or "#").strip()
            pub = html.unescape(item.findtext("pubDate") or "").strip()
            source = item.findtext("source") or "Google News"
            items.append({"title": title, "link": link, "pubDate": pub, "media": source.strip()})
        return items
    except Exception:
        return []


def fetch_dart_disclosures(stock_code, is_krw=True, limit=5):
    if not is_krw or not stock_code or not str(stock_code).isdigit():
        return []
    try:
        url = f"https://m.stock.naver.com/api/stock/{stock_code}/disclosure?page=1&pageSize={limit}"
        res = HTTP_SESSION.get(url, timeout=HTTP_TIMEOUT)
        res.raise_for_status()
        obj = res.json()
        items = obj.get("items", []) or obj.get("disclosures", [])
        out = []
        for it in items[:limit]:
            title = str(it.get("reportTitle") or it.get("title") or "").strip()
            date_raw = str(it.get("rceptDt") or it.get("date") or "")[:10].replace("-", ".")
            art_id = str(it.get("articleId") or it.get("rceptNo") or "").strip()
            tag_type, tag_name = "neutral", "공시"
            if any(k in title for k in ["공급계약", "수주", "자기주식취득", "무상증자", "영업실적", "주주우대", "특허"]):
                tag_type, tag_name = "positive", "호재"
            elif any(k in title for k in ["유상증자", "전환사채", "신주인수권", "감자", "횡령", "배임", "관리종목", "상장폐지", "불성실공시"]):
                tag_type, tag_name = "negative", "악재주의"
            elif any(k in title for k in ["최대주주변경", "주식매수선택권", "주요사항보고서"]):
                tag_type, tag_name = "warning", "변동"
            out.append({
                "title": title, "date": date_raw,
                "link": f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={art_id}" if art_id else "#",
                "tag_type": tag_type, "tag_name": tag_name,
                "flr_nm": it.get("submitter") or "DART",
            })
        return out
    except Exception:
        return []


def find_stock_code(query):
    clean = query.strip().replace(" ", "").upper()
    if clean.isdigit() and len(clean) == 6:
        return clean, clean
    for name, code in DOMESTIC_FAST_MAP.items():
        if name.replace(" ", "").upper() == clean:
            return code, name
    try:
        url = f"https://m.stock.naver.com/front-api/search/autoComplete?query={urllib.parse.quote(query.strip())}&target=stock"
        res = HTTP_SESSION.get(url, timeout=HTTP_TIMEOUT)
        res.raise_for_status()
        items = ((res.json().get("result") or {}).get("items") or [])
        exact = next((it for it in items if str(it.get("name", "")).replace(" ", "").upper() == clean), None)
        item = exact or (items[0] if items else None)
        if item:
            return str(item.get("code") or "").strip(), str(item.get("name") or query).strip()
    except Exception:
        pass
    return None, query.strip()


def get_naver_realtime_price(code):
    try:
        url = f"https://polling.finance.naver.com/api/realtime/domestic/stock/{code}"
        res = HTTP_SESSION.get(url, timeout=HTTP_TIMEOUT)
        res.raise_for_status()
        datas = res.json().get("datas") or []
        if datas:
            p = str(datas[0].get("closePrice") or "").replace(",", "").strip()
            val = safe_float(p)
            if val is not None:
                return val, f"{int(val):,} 원"
    except Exception:
        pass
    return None, None


def fetch_domestic_candles(code):
    try:
        url = f"https://fchart.stock.naver.com/sise.nhn?symbol={code}&timeframe=day&count=1200&requestType=0"
        res = HTTP_SESSION.get(url, timeout=HTTP_TIMEOUT)
        res.raise_for_status()
        root = ET.fromstring(res.text)
        rows = []
        for it in root.findall(".//item"):
            parts = str(it.get("data") or "").split("|")
            if len(parts) < 6:
                continue
            try:
                rows.append({
                    "Date": pd.to_datetime(parts[0]),
                    "Open": float(parts[1]), "High": float(parts[2]), "Low": float(parts[3]),
                    "Close": float(parts[4]), "Volume": float(parts[5]),
                })
            except Exception:
                continue
        if len(rows) >= 20:
            return pd.DataFrame(rows).set_index("Date").sort_index()
    except Exception:
        pass

    # Yahoo backup for Korean-listed symbols.
    for suffix in (".KS", ".KQ"):
        try:
            return yahoo_df(f"{code}{suffix}", "5y", "1d")
        except Exception:
            continue
    return None


def fetch_overseas_candles(symbol):
    try:
        df = yahoo_df(symbol, "5y", "1d")
        curr = safe_float(df["Close"].iloc[-1])
        if curr is None:
            return None, None, None
        return curr, f"{curr:,.2f} 달러", df
    except Exception:
        return None, None, None


def fetch_crypto_candles(symbol):
    sym = symbol.upper().replace("-", "")
    if not sym.endswith("USDT"):
        sym += "USDT"
    try:
        url = f"https://api.binance.com/api/v3/klines?symbol={sym}&interval=1d&limit=1000"
        res = HTTP_SESSION.get(url, timeout=HTTP_TIMEOUT)
        res.raise_for_status()
        arr = res.json()
        rows = []
        for x in arr:
            rows.append({
                "Date": pd.to_datetime(int(x[0]), unit="ms"),
                "Open": float(x[1]), "High": float(x[2]), "Low": float(x[3]),
                "Close": float(x[4]), "Volume": float(x[5]),
            })
        df = pd.DataFrame(rows).set_index("Date").sort_index()
        curr = safe_float(df["Close"].iloc[-1])
        if curr is None:
            return None, None, None
        return curr, f"{curr:,.2f} 달러 (USDT)", df
    except Exception:
        return None, None, None


def fetch_crypto_ticker(symbol):
    sym = symbol.upper().replace("-", "")
    if not sym.endswith("USDT"):
        sym += "USDT"
    try:
        url = f"https://api.binance.com/api/v3/ticker/24hr?symbol={sym}"
        res = HTTP_SESSION.get(url, timeout=HTTP_TIMEOUT)
        res.raise_for_status()
        return res.json()
    except Exception:
        return None


def get_trading_val_rank(symbol, is_krw):
    # Rank is not fabricated. It is intentionally blank unless a real ranking source is wired.
    return ""

# ----------------------------------------------------
# 3. 실제 백테스트
# ----------------------------------------------------
def run_real_backtest(df):
    if df is None or len(df) < 80:
        return None
    closes = df["Close"].to_numpy(dtype=float)
    highs = df["High"].to_numpy(dtype=float)
    lows = df["Low"].to_numpy(dtype=float)
    ma20 = pd.Series(closes).rolling(20).mean().to_numpy()
    signals = np.where((closes[:-1] <= ma20[:-1]) & (closes[1:] > ma20[1:]))[0] + 1
    if len(signals) == 0:
        return None

    outcomes = []
    win_pnl, loss_pnl = [], []
    max_horizon = 14
    for s in signals:
        if s >= len(closes) - 1:
            continue
        entry = closes[s]
        if not np.isfinite(entry) or entry <= 0:
            continue
        # Backtest rule is explicitly defined, never replaced by a fallback constant.
        target = entry * 1.05
        stop = entry * 0.96
        outcome = None
        for k in range(1, min(max_horizon, len(closes) - s - 1) + 1):
            if highs[s + k] >= target:
                outcome = True
                win_pnl.append(0.05)
                break
            if lows[s + k] <= stop:
                outcome = False
                loss_pnl.append(0.04)
                break
        if outcome is not None:
            outcomes.append((s, outcome))

    if not outcomes:
        return None

    n = len(closes)
    recent_1y = [v for idx, v in outcomes if idx >= max(0, n - 250)]
    recent_2y = [v for idx, v in outcomes if idx >= max(0, n - 500)]
    all_vals = [v for _, v in outcomes]
    vals_1y = recent_1y or all_vals
    vals_2y = recent_2y or all_vals
    win_1y = sum(vals_1y) / len(vals_1y) * 100.0
    win_2y = sum(vals_2y) / len(vals_2y) * 100.0
    rr = None
    if win_pnl and loss_pnl:
        rr = float(np.mean(win_pnl) / max(np.mean(loss_pnl), 1e-9))
    return {
        "win_1y": round(win_1y, 1),
        "win_2y": round(win_2y, 1),
        "rr": round(rr, 1) if rr is not None else None,
        "sample": len(outcomes),
    }

# ----------------------------------------------------
# 4. 실제 퀀트 지표
# ----------------------------------------------------
def calculate_quant_factors(raw_price, df, symbol="", is_krw=True):
    if df is None or len(df) < 20 or raw_price is None:
        return None
    close = pd.to_numeric(df["Close"], errors="coerce")
    high = pd.to_numeric(df["High"], errors="coerce")
    low = pd.to_numeric(df["Low"], errors="coerce")
    volume = pd.to_numeric(df["Volume"], errors="coerce").fillna(0)
    close = close.dropna()
    df2 = df.loc[close.index].copy()
    high = high.loc[close.index]
    low = low.loc[close.index]
    volume = volume.loc[close.index]
    n = len(close)
    fmt = (lambda v: f"{int(round(v)):,} 원") if is_krw else (lambda v: f"{float(v):,.2f} 달러")

    ma5_v = float(close.tail(5).mean()) if n >= 5 else None
    ma20_v = float(close.tail(20).mean()) if n >= 20 else None
    ma60_v = float(close.tail(60).mean()) if n >= 60 else None
    ma120_v = float(close.tail(120).mean()) if n >= 120 else None
    ma600_v = float(close.tail(600).mean()) if n >= 600 else None
    ma1200_v = float(close.tail(1200).mean()) if n >= 1200 else None

    std20 = float(close.tail(20).std()) if n >= 20 else None
    z = ((raw_price - ma20_v) / std20) if ma20_v and std20 and std20 > 0 else None

    returns = close.pct_change().dropna().tail(20)
    ann_vol = float(returns.std() * np.sqrt(250) * 100.0) if len(returns) >= 5 else None

    bb_n = min(60, n)
    bb_mid = float(close.tail(bb_n).mean()) if bb_n >= 20 else None
    bb_std = float(close.tail(bb_n).std()) if bb_n >= 20 else None
    bb_up = bb_mid + 2 * bb_std if bb_mid is not None and bb_std is not None else None
    bb_dn = bb_mid - 2 * bb_std if bb_mid is not None and bb_std is not None else None

    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    macd_sig = macd.ewm(span=9, adjust=False).mean()
    macd_v = float(macd.iloc[-1]) if n >= 35 else None
    macd_s = float(macd_sig.iloc[-1]) if n >= 35 else None

    delta = close.diff()
    gain = delta.where(delta > 0, 0.0).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0.0)).rolling(14).mean()
    rs = gain / loss.replace(0, np.nan)
    rsi_series = 100 - (100 / (1 + rs))
    rsi_v = safe_float(rsi_series.iloc[-1])

    tr = pd.concat([high - low, (high - close.shift(1)).abs(), (low - close.shift(1)).abs()], axis=1).max(axis=1)
    atr_v = float(tr.tail(14).mean()) if n >= 20 else None

    v_mean5 = float(volume.tail(5).mean()) if n >= 5 else None
    v_curr = float(volume.iloc[-1]) if n else None
    vol_pct = (v_curr / v_mean5 * 100.0) if v_mean5 and v_mean5 > 0 else None

    df_1y = df2.tail(250)
    high_52 = float(df_1y["High"].max()) if not df_1y.empty else None
    low_52 = float(df_1y["Low"].min()) if not df_1y.empty else None
    pos_52 = ((raw_price - low_52) / (high_52 - low_52) * 100.0) if high_52 is not None and low_52 is not None and high_52 > low_52 else None

    disp20 = raw_price / ma20_v * 100.0 if ma20_v else None
    disp60 = raw_price / ma60_v * 100.0 if ma60_v else None

    val20 = float((close.tail(20) * volume.tail(20)).mean()) if n >= 20 else None
    if val20 is None:
        val20_str = "-"
    elif is_krw:
        val20_str = f"{val20/1e8:.1f}억원"
    else:
        val20_str = f"{val20/1e6:.1f}M 달러" if val20 >= 1e6 else f"{val20:,.0f} 달러"

    bullish = 0
    total = 0
    for cond in [
        ma5_v is not None and raw_price >= ma5_v,
        ma20_v is not None and raw_price >= ma20_v,
        ma60_v is not None and raw_price >= ma60_v,
        macd_v is not None and macd_s is not None and macd_v >= macd_s,
        rsi_v is not None and 40 <= rsi_v <= 65,
        vol_pct is not None and vol_pct >= 100,
    ]:
        total += 1
        bullish += int(cond)
    score = int(round(bullish / total * 100.0)) if total else None

    if ma60_v is not None and raw_price > ma20_v > ma60_v:
        trend = "정배열 상승 추세"
        guide = "20일선과 60일선 위에서 추세가 유지되는지 확인하고, ATR 범위의 눌림과 거래량 회복을 함께 확인하세요."
    elif ma60_v is not None and raw_price < ma20_v < ma60_v:
        trend = "역배열 하락 추세"
        guide = "20일선 회복과 거래량 개선이 확인되기 전에는 보수적으로 접근하세요."
    else:
        trend = "박스권 혼조세"
        guide = "20일선·60일선 방향과 변동성 확대 여부를 함께 확인하세요."

    # Dynamic signal levels: ATR derived, with no fixed placeholder fallback.
    entry1_v = raw_price - 0.5 * atr_v if atr_v is not None and atr_v > 0 else None
    entry2_v = raw_price - 1.2 * atr_v if atr_v is not None and atr_v > 0 else None
    target1_v = raw_price + 1.5 * atr_v if atr_v is not None and atr_v > 0 else None
    target2_v = raw_price + 2.8 * atr_v if atr_v is not None and atr_v > 0 else None
    stop_v = raw_price - 1.8 * atr_v if atr_v is not None and atr_v > 0 else None
    signal_pct = int(max(8, min(92, 45))) if all(v is not None for v in [entry1_v, stop_v, target2_v]) else None

    bt = run_real_backtest(df2)

    # 최근 60거래일 상승 시그널 흐름(실제 지표 기반 0~100)
    signal_history = []
    hist = df2.tail(60).copy()
    h_close = hist["Close"]
    h_vol = hist["Volume"]
    h_ma5 = h_close.rolling(5).mean()
    h_ma20 = h_close.rolling(20).mean()
    h_ma60 = h_close.rolling(60).mean()
    h_ema12 = h_close.ewm(span=12, adjust=False).mean()
    h_ema26 = h_close.ewm(span=26, adjust=False).mean()
    h_macd = h_ema12 - h_ema26
    h_macd_sig = h_macd.ewm(span=9, adjust=False).mean()
    h_delta = h_close.diff()
    h_gain = h_delta.where(h_delta > 0, 0.0).rolling(14).mean()
    h_loss = (-h_delta.where(h_delta < 0, 0.0)).rolling(14).mean()
    h_rsi = 100 - (100 / (1 + (h_gain / h_loss.replace(0, np.nan))))
    h_vmean5 = h_vol.rolling(5).mean()

    for idx in hist.index:
        vals = [
            safe_float(h_close.loc[idx] >= h_ma5.loc[idx]) if pd.notna(h_ma5.loc[idx]) else None,
            safe_float(h_close.loc[idx] >= h_ma20.loc[idx]) if pd.notna(h_ma20.loc[idx]) else None,
            safe_float(h_close.loc[idx] >= h_ma60.loc[idx]) if pd.notna(h_ma60.loc[idx]) else None,
            safe_float(h_macd.loc[idx] >= h_macd_sig.loc[idx]) if pd.notna(h_macd_sig.loc[idx]) else None,
            safe_float(40 <= h_rsi.loc[idx] <= 65) if pd.notna(h_rsi.loc[idx]) else None,
            safe_float(h_vol.loc[idx] >= h_vmean5.loc[idx]) if pd.notna(h_vmean5.loc[idx]) else None,
        ]
        valid = [v for v in vals if v is not None]
        score_hist = (sum(valid) / len(valid) * 100.0) if valid else None
        if score_hist is not None:
            signal_history.append({
                "time": idx.strftime("%Y-%m-%d"),
                "score": round(score_hist, 1),
            })

    return {
        "quant_score": f"{score}%" if score is not None else "-",
        "score_badge": ("모멘텀 최강세" if score is not None and score >= 80 else "상승 모멘텀 우세" if score is not None and score >= 60 else "중립" if score is not None else "데이터 부족"),
        "score_dots": f"<span style='color:#10b981; font-weight:700;'>상승 시그널 {bullish}</span> &nbsp; <span style='color:#38bdf8; font-weight:700;'>중립 {max(0, total-bullish)}</span>",
        "trend": trend,
        "z_score": f"{z:+.2f} σ" if z is not None else "-",
        "z_state": ("과열 밴드" if z is not None and z >= 2 else "과매도 반등구간" if z is not None and z <= -2 else "정규 분포 밴드 내" if z is not None else "데이터 부족"),
        "z_color": "warning" if z is not None and z >= 2 else "positive" if z is not None and z <= -2 else "neutral",
        "ann_vol": f"{ann_vol:.1f}%" if ann_vol is not None else "-",
        "vol_risk_state": ("고변동성" if ann_vol is not None and ann_vol >= 40 else "적정 변동성" if ann_vol is not None and ann_vol >= 18 else "저변동성" if ann_vol is not None else "데이터 부족"),
        "vol_risk_color": "warning" if ann_vol is not None and ann_vol >= 40 else "positive" if ann_vol is not None and ann_vol >= 18 else "neutral",
        "ma5": fmt(ma5_v) if ma5_v is not None else "-", "ma5_state": "5일선 상회" if ma5_v is not None and raw_price >= ma5_v else "5일선 하회" if ma5_v is not None else "데이터 부족", "ma5_color": "positive" if ma5_v is not None and raw_price >= ma5_v else "negative",
        "ma20": fmt(ma20_v) if ma20_v is not None else "-", "ma20_state": "20일선 지지" if ma20_v is not None and raw_price >= ma20_v else "20일선 하회" if ma20_v is not None else "데이터 부족", "ma20_color": "positive" if ma20_v is not None and raw_price >= ma20_v else "negative",
        "ma60": fmt(ma60_v) if ma60_v is not None else "-", "ma60_state": "60일선 상회" if ma60_v is not None and raw_price >= ma60_v else "60일선 하회" if ma60_v is not None else "데이터 부족", "ma60_color": "positive" if ma60_v is not None and raw_price >= ma60_v else "warning",
        "ma120": fmt(ma120_v) if ma120_v is not None else "-", "ma120_state": "120일선 상회" if ma120_v is not None and raw_price >= ma120_v else "120일선 하회" if ma120_v is not None else "데이터 부족", "ma120_color": "positive" if ma120_v is not None and raw_price >= ma120_v else "negative",
        "ma600": fmt(ma600_v) if ma600_v is not None else "-", "ma1200": fmt(ma1200_v) if ma1200_v is not None else "-",
        "bollinger": f"{fmt(bb_up)} / {fmt(bb_dn)}" if bb_up is not None and bb_dn is not None else "-", "bb_state": "상단 돌파" if bb_up is not None and raw_price >= bb_up else "하단 지지" if bb_dn is not None and raw_price <= bb_dn else "밴드 내" if bb_up is not None else "데이터 부족", "bb_color": "positive" if bb_up is not None and raw_price >= bb_up else "warning" if bb_dn is not None and raw_price <= bb_dn else "neutral",
        "macd": f"{macd_v:+.2f} (시그널: {macd_s:+.2f})" if macd_v is not None and macd_s is not None else "-", "macd_state": "골든크로스" if macd_v is not None and macd_s is not None and macd_v >= macd_s else "데드크로스" if macd_v is not None and macd_s is not None else "데이터 부족", "macd_color": "positive" if macd_v is not None and macd_s is not None and macd_v >= macd_s else "negative",
        "pos_52w": f"{pos_52:.1f}% (고:{fmt(high_52)}/저:{fmt(low_52)})" if pos_52 is not None else "-", "slider_fill_pct": int(max(8, min(92, pos_52))) if pos_52 is not None else 50,
        "high_52w": fmt(high_52) if high_52 is not None else "-", "low_52w": fmt(low_52) if low_52 is not None else "-",
        "signal_history": signal_history,
        "disp": f"20일: {disp20:.1f}% | 60일: {disp60:.1f}%" if disp20 is not None and disp60 is not None else f"20일: {disp20:.1f}%" if disp20 is not None else "-",
        "disp_state": "적정 이격 유지" if disp20 is not None and 98 <= disp20 <= 104 else "단기 과열 경계" if disp20 is not None and disp20 > 104 else "단기 과매도 구간" if disp20 is not None else "데이터 부족",
        "disp_color": "positive" if disp20 is not None and 98 <= disp20 <= 104 else "warning" if disp20 is not None and disp20 > 104 else "neutral",
        "disp20_num": f"{disp20:+.1f}%" if disp20 is not None else "-",
        "rsi": f"{rsi_v:.1f}" if rsi_v is not None else "-", "rsi_state": "과열권" if rsi_v is not None and rsi_v >= 70 else "침체권" if rsi_v is not None and rsi_v <= 30 else "중립" if rsi_v is not None else "데이터 부족", "rsi_color": "warning" if rsi_v is not None and rsi_v >= 70 else "positive" if rsi_v is not None and rsi_v <= 30 else "neutral",
        "atr": fmt(atr_v) if atr_v is not None else "-", "atr_ratio": f"{atr_v / raw_price * 100:.2f}%" if atr_v is not None and raw_price else "-",
        "vol_ratio": f"{vol_pct:.1f}%" if vol_pct is not None else "-", "vol_state": "거래량 급증" if vol_pct is not None and vol_pct >= 150 else "거래량 보통" if vol_pct is not None and vol_pct >= 70 else "거래량 침체" if vol_pct is not None else "데이터 부족", "vol_color": "positive" if vol_pct is not None and vol_pct >= 150 else "neutral",
        "tval_20d": val20_str,
        "entry1": fmt(entry1_v) if entry1_v is not None else "-", "entry2": fmt(entry2_v) if entry2_v is not None else "-",
        "target1": fmt(target1_v) if target1_v is not None else "-", "target2": fmt(target2_v) if target2_v is not None else "-", "stop_p": fmt(stop_v) if stop_v is not None else "-",
        "entry1_raw": entry1_v, "entry2_raw": entry2_v, "target1_raw": target1_v, "target2_raw": target2_v, "stop_raw": stop_v,
        "sig_fill_pct": signal_pct,
        "guide_text": guide,
        "bt_1y": f"1년 실측 {bt['win_1y']:.1f}%" if bt else "1년 실측 -",
        "bt_2y": f"2년 실측 {bt['win_2y']:.1f}%" if bt else "2년 실측 -",
        "win_rate_num": f"{bt['win_2y']:.1f}%" if bt else "-",
        "rr_str": f"실측 손익비 1:{bt['rr']:.1f}" if bt and bt.get("rr") is not None else "실측 손익비 -",
    }


def fetch_market_consensus_unified(symbol, market_name, raw_price, formal_name):
    # Never fabricate consensus. Only return a card when a source provides real values.
    if market_name == "암호화폐":
        ticker = fetch_crypto_ticker(symbol)
        if not ticker:
            return None
        try:
            pct = float(ticker.get("priceChangePercent"))
            quote_vol = float(ticker.get("quoteVolume"))
            high = float(ticker.get("highPrice"))
            low = float(ticker.get("lowPrice"))
            return {
                "title": "암호화폐 24시간 시장 데이터",
                "gauge_val": f"{pct:+.2f}%", "gauge_label": "24H",
                "status_label": "실시간 시장 데이터", "badge_text": "Binance",
                "dots": "가격 변화와 거래대금 기반",
                "primary_label": "24시간 고가/저가", "primary_val": f"{high:,.2f} / {low:,.2f}",
                "secondary_label": "24시간 거래대금", "secondary_val": f"{quote_vol/1e6:,.1f}M USDT",
                "slider_left": f"{low:,.2f}", "slider_mid": f"{raw_price:,.2f}", "slider_right": f"{high:,.2f}",
                "slider_bottom_left": "24H Low", "slider_bottom_right": "24H High",
                "fill_pct": max(5, min(95, ((raw_price - low) / max(1e-9, high - low)) * 100)),
                "list_title": "실시간 시장값", "source_tag": "Binance 공개 API", "items": [],
            }
        except Exception:
            return None
    return None


def run_full_pipeline(query):
    global ANALYSIS_CACHE
    original = query.strip()
    clean = original.replace(" ", "").upper()
    if not clean:
        return None
    now = time.time()

    cached = ANALYSIS_CACHE.get(clean)
    if cached and now - cached[0] < CACHE_TTL:
        return cached[1]

    market_name = "국내주식"
    is_krw = True
    symbol = None
    formal_name = original
    raw_price = None
    price_str = None
    df = None

    if clean in GLOBAL_INDEX_MAP:
        symbol = GLOBAL_INDEX_MAP[clean]
        curr, ps, d = fetch_overseas_candles(symbol)
        raw_price, price_str, df = curr, ps, d
        if symbol in ("^KS11", "^KQ11"):
            market_name, is_krw = "국내증시", True
            price_str = f"{raw_price:,.2f} pt" if raw_price is not None else None
        elif symbol == "KRW=X":
            market_name, is_krw = "외환", False
            price_str = f"{raw_price:,.2f} 원" if raw_price is not None else None
        elif symbol in ("CL=F", "GC=F"):
            market_name, is_krw = "원자재", False
            price_str = f"${raw_price:,.2f}" if raw_price is not None else None
        else:
            market_name, is_krw = "글로벌증시", False
            price_str = f"${raw_price:,.2f}" if raw_price is not None else None
    elif clean.endswith("USDT") or clean in CRYPTO_MAP or any(k in clean for k in ("BTC", "ETH", "코인", "비트")):
        symbol = CRYPTO_MAP.get(clean, clean if clean.endswith("USDT") else f"{clean}USDT")
        formal_name = original
        raw_price, price_str, df = fetch_crypto_candles(symbol)
        market_name, is_krw = "암호화폐", False
    elif clean in US_STOCK_MAP:
        symbol = US_STOCK_MAP[clean]
        formal_name = original
        raw_price, price_str, df = fetch_overseas_candles(symbol)
        market_name, is_krw = "미국주식", False
    elif any(c.isupper() for c in original) and not any("가" <= c <= "힣" for c in original):
        symbol = original.upper().strip()
        formal_name = symbol
        raw_price, price_str, df = fetch_overseas_candles(symbol)
        market_name, is_krw = "미국주식", False
    else:
        code, name = find_stock_code(original)
        if not code:
            return None
        symbol, formal_name = code, name
        raw_price, price_str = get_naver_realtime_price(code)
        df = fetch_domestic_candles(code)
        market_name, is_krw = "국내주식", True
        if raw_price is None and df is not None and not df.empty:
            raw_price = safe_float(df["Close"].iloc[-1])
            price_str = f"{int(raw_price):,} 원" if raw_price is not None else None

    if raw_price is None or df is None or len(df) < 20:
        return None

    with ThreadPoolExecutor(max_workers=4) as ex:
        f_news = ex.submit(fetch_rss_news, formal_name, 5)
        f_global = ex.submit(fetch_rss_news, "뉴욕증시 나스닥 미국증시", 4)
        f_consensus = ex.submit(fetch_market_consensus_unified, symbol, market_name, raw_price, formal_name)
        f_dart = ex.submit(fetch_dart_disclosures, symbol, is_krw, 5)
        stock_news = f_news.result()
        global_news = f_global.result()
        consensus = f_consensus.result()
        dart = f_dart.result()

    world_board = CACHED_WORLD_BOARD or fetch_all_world_board_parallel()
    quant = calculate_quant_factors(raw_price, df, symbol=symbol, is_krw=is_krw)
    if not quant:
        return None

    chart_df = df.copy()
    chart_df["MA20"] = chart_df["Close"].rolling(20).mean()
    chart_df["MA60"] = chart_df["Close"].rolling(60).mean()
    chart_df["STD60"] = chart_df["Close"].rolling(60).std()
    chart_df["BB_UP"] = chart_df["MA60"] + chart_df["STD60"] * 2
    chart_df["BB_DN"] = chart_df["MA60"] - chart_df["STD60"] * 2

    candles, ma20, ma60, bb_up, bb_dn = [], [], [], [], []
    for idx, row in chart_df.tail(65).iterrows():
        t = idx.strftime("%Y-%m-%d")
        candles.append({"time": t, "open": float(row["Open"]), "high": float(row["High"]), "low": float(row["Low"]), "close": float(row["Close"])})
        for key, arr in (("MA20", ma20), ("MA60", ma60), ("BB_UP", bb_up), ("BB_DN", bb_dn)):
            val = safe_float(row[key])
            if val is not None:
                arr.append({"time": t, "value": val})

    data = {
        "name": formal_name,
        "formal_name": formal_name,
        "symbol": symbol,
        "market": market_name,
        "price": price_str,
        "price_str": price_str,
        "time": datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S"),
        "now_str": datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S"),
        "quant": quant,
        "consensus": consensus,
        "stock_news": stock_news,
        "global_news": global_news,
        "world_board": world_board,
        "dart_disclosures": dart,
        "candles": candles,
        "ma20_line": ma20,
        "ma60_line": ma60,
        "bb_up_line": bb_up,
        "bb_dn_line": bb_dn,
    }
    ANALYSIS_CACHE[clean] = (now, data)
    return data

# ----------------------------------------------------
# 5. Telegram: 명령 + 구독 + 실시간 가격 경보
# ----------------------------------------------------
def telegram_api(method, payload=None, timeout=10):
    if not TELEGRAM_TOKEN:
        return None
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}"
    try:
        r = HTTP_SESSION.post(url, json=payload or {}, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


def send_telegram_msg(chat_id, text):
    return telegram_api("sendMessage", {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}, timeout=8)


def format_tg_result(data):
    q = data["quant"]
    name = html.escape(str(data["name"]))
    return (
        f"<b>gaemiGTP 실시간 분석: {name}</b> ({html.escape(str(data['symbol']))})\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"현재가: <b>{html.escape(str(data['price']))}</b>\n"
        f"추세: <b>{html.escape(str(q['trend']))}</b>\n"
        f"백테스트 승률: <b>{html.escape(str(q['win_rate_num']))}</b> ({html.escape(str(q['rr_str']))})\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>gaemiGTP 시그널</b>\n"
        f"• 1차 진입: {html.escape(str(q['entry1']))}\n"
        f"• 2차 진입: {html.escape(str(q['entry2']))}\n"
        f"• 1차 목표: {html.escape(str(q['target1']))}\n"
        f"• 2차 목표: {html.escape(str(q['target2']))}\n"
        f"• 손절: {html.escape(str(q['stop_p']))}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>{html.escape(str(q['guide_text']))}</i>"
    )


def subscribe_chat(chat_id, query):
    data = run_full_pipeline(query)
    if not data:
        return False, "종목 데이터를 가져오지 못했습니다. 잠시 후 다시 시도해 주세요."
    with TELEGRAM_LOCK:
        TELEGRAM_SUBSCRIPTIONS[chat_id] = {
            "symbol": data["symbol"],
            "display": data["name"],
            "last_price": safe_float(str(data["price"]).replace(",", "").split()[0]),
            "alert_state": {"entry": False, "target1": False, "target2": False, "stop": False},
        }
    return True, format_tg_result(data) + "\n\n✅ 이 종목의 가격 경보를 켰습니다."


def unsubscribe_chat(chat_id):
    with TELEGRAM_LOCK:
        existed = TELEGRAM_SUBSCRIPTIONS.pop(chat_id, None)
    return existed is not None


def telegram_update_worker():
    if not TELEGRAM_TOKEN:
        return
    offset = 0
    while True:
        try:
            data = telegram_api("getUpdates", {"offset": offset, "timeout": 20}, timeout=25)
            if not data or not data.get("ok"):
                time.sleep(3)
                continue
            for upd in data.get("result", []):
                offset = int(upd.get("update_id", 0)) + 1
                msg = upd.get("message") or {}
                chat = msg.get("chat") or {}
                chat_id = chat.get("id")
                text = str(msg.get("text") or "").strip()
                if not chat_id or not text:
                    continue

                if text.startswith("/start"):
                    parts = text.split(maxsplit=1)
                    if len(parts) == 2 and parts[1].strip():
                        ok, reply = subscribe_chat(chat_id, parts[1].strip())
                        send_telegram_msg(chat_id, reply)
                    else:
                        send_telegram_msg(chat_id, "gaemiGTP 알림봇입니다.\n\n/삼성전자\n/NVDA\n/BTC 처럼 입력하면 해당 종목의 가격 경보를 켭니다.\n\n/stop : 알림 해제\n/status : 현재 알림 종목")
                elif text == "/stop":
                    send_telegram_msg(chat_id, "✅ 알림을 해제했습니다." if unsubscribe_chat(chat_id) else "현재 등록된 알림이 없습니다.")
                elif text == "/status":
                    with TELEGRAM_LOCK:
                        sub = TELEGRAM_SUBSCRIPTIONS.get(chat_id)
                    if sub:
                        send_telegram_msg(chat_id, f"현재 알림 종목: <b>{html.escape(sub['display'])}</b> ({html.escape(sub['symbol'])})")
                    else:
                        send_telegram_msg(chat_id, "현재 등록된 알림이 없습니다.")
                else:
                    query = text.lstrip("/").strip()
                    ok, reply = subscribe_chat(chat_id, query)
                    send_telegram_msg(chat_id, reply if ok else reply)
        except Exception:
            time.sleep(3)


def telegram_alert_worker():
    if not TELEGRAM_TOKEN:
        return
    while True:
        with TELEGRAM_LOCK:
            items = list(TELEGRAM_SUBSCRIPTIONS.items())
        for chat_id, sub in items:
            try:
                data = run_full_pipeline(sub["symbol"])
                if not data:
                    continue
                q = data["quant"]
                curr = safe_float(str(data["price"]).replace(",", "").split()[0])
                if curr is None:
                    continue
                levels = {
                    "entry": q.get("entry1_raw"),
                    "target1": q.get("target1_raw"),
                    "target2": q.get("target2_raw"),
                    "stop": q.get("stop_raw"),
                }
                # Reset alert state if price moves meaningfully back below/above a level.
                state = sub["alert_state"]
                messages = []
                if levels["entry"] is not None and curr <= levels["entry"] and not state["entry"]:
                    messages.append(f"🟢 <b>{html.escape(sub['display'])}</b> 1차 진입 구간 도달\n현재가: {html.escape(str(data['price']))}\n기준가: {html.escape(str(q['entry1']))}")
                    state["entry"] = True
                if levels["target1"] is not None and curr >= levels["target1"] and not state["target1"]:
                    messages.append(f"🎯 <b>{html.escape(sub['display'])}</b> 1차 목표가 도달\n현재가: {html.escape(str(data['price']))}\n목표가: {html.escape(str(q['target1']))}")
                    state["target1"] = True
                if levels["target2"] is not None and curr >= levels["target2"] and not state["target2"]:
                    messages.append(f"🚀 <b>{html.escape(sub['display'])}</b> 2차 목표가 도달\n현재가: {html.escape(str(data['price']))}\n목표가: {html.escape(str(q['target2']))}")
                    state["target2"] = True
                if levels["stop"] is not None and curr <= levels["stop"] and not state["stop"]:
                    messages.append(f"🔵 <b>{html.escape(sub['display'])}</b> 손절 방어선 도달\n현재가: {html.escape(str(data['price']))}\n방어선: {html.escape(str(q['stop_p']))}")
                    state["stop"] = True

                # Re-arm when price moves back through the band after an alert.
                if levels["entry"] is not None and curr > levels["entry"] * 1.01:
                    state["entry"] = False
                if levels["target1"] is not None and curr < levels["target1"] * 0.99:
                    state["target1"] = False
                if levels["target2"] is not None and curr < levels["target2"] * 0.99:
                    state["target2"] = False
                if levels["stop"] is not None and curr > levels["stop"] * 1.01:
                    state["stop"] = False

                sub["last_price"] = curr
                for message in messages:
                    send_telegram_msg(chat_id, message)
            except Exception:
                continue
        time.sleep(30)

# ----------------------------------------------------
# 6. 시작 작업자
# ----------------------------------------------------
threading.Thread(target=background_world_board_worker, daemon=True).start()
if TELEGRAM_TOKEN:
    threading.Thread(target=telegram_update_worker, daemon=True).start()
    threading.Thread(target=telegram_alert_worker, daemon=True).start()


HTML_PAGE = """<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>gaemiGTP</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Google+Sans:wght@400;500;600;700&family=Roboto:wght@400;500;700&display=swap" rel="stylesheet">
    <script src="https://unpkg.com/lightweight-charts@4.1.1/dist/lightweight-charts.standalone.production.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2"></script>
    <style>
        :root {
            --bg-body: #131314;
            --bg-card: #1e1f20;
            --bg-sidebar: #1e1f20;
            --bg-subcard: #131314;
            --text-title: #f2f2f2;
            --text-main: #e3e3e3;
            --text-muted: #8e918f;
            --text-sub: #c4c7c5;
            --border-line: #282a2c;
            --border-focus: #333538;
            --btn-hover: rgba(255, 255, 255, 0.08);
            --signal-border: rgba(16, 185, 129, 0.55);
            --signal-glow: rgba(16, 185, 129, 0.12);
            --bottom-fade: linear-gradient(180deg, rgba(19,19,20,0) 0%, rgba(19,19,20,1) 40%);
        }

        body.light-theme {
            --bg-body: #f8f9fa;
            --bg-card: #ffffff;
            --bg-sidebar: #f1f3f4;
            --bg-subcard: #f8f9fa;
            --text-title: #1f1f1f;
            --text-main: #202124;
            --text-muted: #5f6368;
            --text-sub: #3c4043;
            --border-line: #dadce0;
            --border-focus: #bdc1c6;
            --btn-hover: rgba(0, 0, 0, 0.05);
            --signal-border: rgba(16, 185, 129, 0.65);
            --signal-glow: rgba(16, 185, 129, 0.08);
            --bottom-fade: linear-gradient(180deg, rgba(248,249,250,0) 0%, rgba(248,249,250,1) 40%);
        }

        * { box-sizing: border-box; margin: 0; padding: 0; font-family: 'Google Sans', 'Roboto', -apple-system, BlinkMacSystemFont, "Apple SD Gothic Neo", sans-serif; }
        body { background-color: var(--bg-body); color: var(--text-main); min-height: 100vh; overflow-x: hidden; display: flex; transition: background-color 0.2s, color 0.2s; }
        
        #sidebar-wrapper {
            width: 260px;
            background-color: var(--bg-sidebar);
            height: 100vh;
            position: fixed;
            top: 0;
            left: 0;
            display: flex;
            flex-direction: column;
            padding: 12px 14px 16px;
            z-index: 100;
            border-right: 1px solid var(--border-line);
            transform: translateX(-100%);
            transition: transform 0.25s ease, background-color 0.2s;
            box-shadow: none;
        }
        #sidebar-wrapper.open {
            transform: translateX(0);
            box-shadow: 10px 0 30px rgba(0,0,0,0.25);
        }

        .side-top-action {
            height: 44px;
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 12px;
            padding: 0 4px;
        }

        .gemini-menu-icon {
            background: transparent;
            border: none;
            color: var(--text-sub);
            cursor: pointer;
            padding: 6px;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            transition: background 0.15s, color 0.15s;
        }
        .gemini-menu-icon:hover { background: var(--btn-hover); color: var(--text-title); }

        .gemini-new-btn {
            display: flex;
            align-items: center;
            gap: 12px;
            background: var(--bg-body);
            border: 1px solid var(--border-line);
            color: var(--text-main);
            font-size: 0.875rem;
            font-weight: 500;
            padding: 10px 16px;
            border-radius: 24px;
            cursor: pointer;
            width: 100%;
            transition: all 0.2s;
            margin-bottom: 16px;
        }
        .gemini-new-btn:hover { background: var(--btn-hover); border-color: var(--border-focus); color: var(--text-title); }

        .sidebar-scroll-area {
            flex: 1;
            overflow-y: auto;
        }

        .sidebar-section-title {
            font-size: 0.75rem;
            color: var(--text-muted);
            font-weight: 600;
            padding: 8px 12px;
            letter-spacing: -0.2px;
        }

        .sidebar-item {
            display: flex;
            align-items: center;
            padding: 9px 14px;
            border-radius: 20px;
            color: var(--text-sub);
            font-size: 0.875rem;
            cursor: pointer;
            margin-bottom: 2px;
            transition: background 0.15s, color 0.15s;
        }
        .sidebar-item:hover { background: var(--btn-hover); color: var(--text-title); }

        #content-wrapper {
            margin-left: 0;
            width: 100%;
            min-height: 100vh;
            display: flex;
            flex-direction: column;
            position: relative;
        }

        header {
            height: 56px;
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 0 16px;
            background: var(--bg-body);
            border-bottom: none;
            position: sticky;
            top: 0;
            z-index: 40;
            transition: background-color 0.2s;
        }

        .header-left { display: flex; align-items: center; gap: 12px; }
        .logo-top { font-weight: 700; font-size: 1.18rem; color: var(--text-title); letter-spacing: -0.5px; cursor: pointer; }

        #main-view { min-height: calc(100vh - 56px); display: flex; flex-direction: column; align-items: center; justify-content: center; padding: 16px; }
        .hero-title { font-size: 2.8rem; font-weight: 700; margin-bottom: 6px; letter-spacing: -1px; color: var(--text-title); text-align: center; }
        .hero-sub { font-size: 0.95rem; color: var(--text-muted); margin-bottom: 24px; font-weight: 400; text-align: center; }

        .search-container { width: 100%; max-width: 832px; position: relative; }
        .search-box { width: 100%; min-height: 60px; background: var(--bg-card); border: 1px solid var(--border-line); border-radius: 30px; padding: 8px 12px 8px 18px; display: flex; align-items: center; box-shadow: 0 4px 20px rgba(0,0,0,0.1); position: relative; }
        .search-box input { flex: 1; min-width: 0; background: transparent; border: none; outline: none; color: var(--text-title); font-size: 0.95rem; }

        .model-selector-wrap { position: relative; margin-right: 4px; flex-shrink: 0; }
        .model-pill-btn { background: transparent; border: none; border-radius: 20px; padding: 6px 8px; display: flex; align-items: center; gap: 4px; cursor: pointer; user-select: none; }
        .model-pill-btn:hover { background: var(--btn-hover); }
        .model-pill-name { font-size: 0.82rem; font-weight: 600; color: var(--text-main); }
        .model-pill-arrow { font-size: 0.6rem; color: var(--text-muted); }

        .model-dropdown-menu { display: none; position: absolute; bottom: calc(100% + 10px); right: 0; width: 210px; max-width: 80vw; background: var(--bg-card); border: 1px solid var(--border-line); border-radius: 16px; padding: 6px; box-shadow: 0 10px 30px rgba(0,0,0,0.15); z-index: 100; }
        .model-dropdown-item { padding: 9px 12px; border-radius: 10px; cursor: pointer; }
        .model-dropdown-item:hover { background: var(--btn-hover); }
        .model-dropdown-item.active { background: var(--btn-hover); }
        .item-title { font-size: 0.88rem; font-weight: 600; color: var(--text-title); display: flex; justify-content: space-between; }
        .item-sub { font-size: 0.73rem; color: var(--text-muted); margin-top: 2px; }
        .item-check { color: #38bdf8; font-weight: bold; display: none; }
        .model-dropdown-item.active .item-check { display: inline; }

        .send-btn { width: 38px; height: 38px; border-radius: 50%; background: var(--border-line); border: none; color: var(--text-title); display: flex; align-items: center; justify-content: center; cursor: pointer; flex-shrink: 0; }
        .send-btn:hover { background: var(--border-focus); }

        .popular-tags { display: flex; gap: 8px; margin-top: 20px; flex-wrap: wrap; justify-content: center; }
        .tag-btn { background: var(--bg-card); border: 1px solid var(--border-line); border-radius: 16px; padding: 6px 13px; font-size: 0.82rem; color: var(--text-sub); cursor: pointer; }
        .tag-btn:hover { background: var(--btn-hover); color: var(--text-title); }

        #result-scroll-view { display: none; width: 100%; min-height: 100vh; padding: 16px 12px 100px; }
        .result-inner { max-width: 1220px; margin: 0 auto; width: 100%; }
        
        .card { background: var(--bg-card); border-radius: 16px; padding: 18px 16px; margin-bottom: 14px; border: 1px solid var(--border-line); box-shadow: 0 2px 10px rgba(0,0,0,0.03); max-width: 832px; margin-left: auto; margin-right: auto; }
        
        .chart-dual-grid {
            display: grid;
            grid-template-columns: 832px 348px;
            gap: 16px;
            margin: 0 auto 14px;
            max-width: 1196px;
            justify-content: center;
            align-items: stretch;
        }

        .chart-main-card {
            background: var(--bg-card);
            border-radius: 16px;
            padding: 18px 16px;
            border: 1px solid var(--border-line);
            box-shadow: 0 2px 10px rgba(0,0,0,0.03);
            display: flex;
            flex-direction: column;
            justify-content: space-between;
            width: 100%;
        }

        .price-title { font-size: 1.25rem; font-weight: 700; color: var(--text-title); word-break: keep-all; }
        .price-title span { color: var(--text-muted); font-size: 0.85rem; font-weight: 400; margin-left: 4px; }
        .price-val { font-size: 2.1rem; font-weight: 800; color: var(--text-title); margin: 6px 0; }
        .price-meta { color: var(--text-muted); font-size: 0.78rem; line-height: 1.4; word-break: break-all; }

        .chart-legend { display: flex; flex-wrap: wrap; gap: 8px 12px; font-size: 0.75rem; color: var(--text-muted); margin-top: 10px; }
        .legend-tag { display: flex; align-items: center; gap: 4px; }
        .legend-dot { width: 7px; height: 7px; border-radius: 50%; }
        .chart-box-wrap { width: 100%; height: 300px; margin-top: 10px; position: relative; background: var(--bg-card); border-radius: 8px; overflow: hidden; }

        .global-board-card {
            background: var(--bg-card);
            border: 1px solid var(--border-line);
            border-radius: 16px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.03);
            padding: 16px 14px;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
            width: 100%;
        }

        .board-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 8px;
            padding-bottom: 6px;
            border-bottom: 1px solid var(--border-line);
        }
        .board-header-title { font-size: 0.95rem; font-weight: 800; color: var(--text-title); }
        .board-header-sub { font-size: 0.7rem; color: var(--text-muted); }

        .board-2col-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 6px;
            flex: 1;
        }

        .mini-index-card {
            background: var(--bg-subcard);
            border: 1px solid var(--border-line);
            border-radius: 10px;
            padding: 7px 8px;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
            transition: all 0.2s ease;
            cursor: pointer;
            min-height: 58px;
        }
        .mini-index-card:hover {
            border-color: var(--border-focus);
            background: var(--btn-hover);
            transform: translateY(-1px);
        }
        .mini-top { display: flex; justify-content: space-between; align-items: center; }
        .mini-name { font-size: 0.76rem; font-weight: 800; color: var(--text-title); }
        .mini-tag { 
            font-size: 0.6rem; 
            font-weight: 700;
            color: var(--text-muted); 
            background: var(--btn-hover); 
            border: 1px solid var(--border-line);
            padding: 1px 4px; 
            border-radius: 5px; 
        }
        
        .mini-mid-row {
            display: flex;
            justify-content: space-between;
            align-items: flex-end;
            margin-top: 3px;
        }
        .mini-price { font-size: 0.88rem; font-weight: 800; color: var(--text-title); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; line-height: 1.1; }
        .mini-rate { font-size: 0.72rem; font-weight: 700; line-height: 1.1; margin-top: 1px; }
        .mini-spark { flex-shrink: 0; }

        .section-title { font-size: 1rem; font-weight: 700; color: var(--text-title); margin-bottom: 12px; display: flex; justify-content: space-between; align-items: center; }
        
        .val-up { color: #ef4444; }
        .val-down { color: #38bdf8; }

        .quant-row-item {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 10px 0;
            border-bottom: 1px solid var(--border-line);
            font-size: 0.84rem;
        }
        .quant-row-item:last-child {
            border-bottom: none;
        }
        .quant-row-left {
            display: flex;
            flex-direction: column;
            gap: 3px;
            min-width: 0;
        }
        .quant-row-name {
            font-weight: 700;
            color: var(--text-title);
        }
        .quant-row-status {
            font-size: 0.72rem;
            font-weight: 700;
        }
        .quant-row-right {
            text-align: right;
            flex-shrink: 0;
        }
        .quant-row-val {
            font-weight: 800;
            color: var(--text-title);
        }
        .quant-row-subval {
            font-size: 0.72rem;
            color: var(--text-muted);
            margin-top: 2px;
        }

        .status-pill-positive { color: #10b981; }
        .status-pill-negative { color: #ef4444; }
        .status-pill-warning { color: #d97706; }
        .status-pill-neutral { color: var(--text-muted); }

        .dart-item { padding: 9px 0; border-bottom: 1px solid var(--border-line); text-decoration: none; display: flex; align-items: center; justify-content: space-between; gap: 8px; }
        .dart-item:last-child { border-bottom: none; }
        .dart-title { color: var(--text-main); font-size: 0.83rem; font-weight: 500; line-height: 1.4; flex: 1; min-width: 0; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
        .dart-title:hover { color: #0284c7; }
        .dart-tag { font-size: 0.68rem; font-weight: 700; padding: 2px 6px; border-radius: 6px; flex-shrink: 0; }
        .tag-positive { background: rgba(16, 185, 129, 0.15); color: #10b981; border: 1px solid rgba(16, 185, 129, 0.3); }
        .tag-negative { background: rgba(239, 68, 68, 0.15); color: #ef4444; border: 1px solid rgba(239, 68, 68, 0.3); }
        .tag-warning { background: rgba(249, 115, 22, 0.15); color: #f97316; border: 1px solid rgba(249, 115, 22, 0.3); }
        .tag-neutral { background: var(--btn-hover); color: var(--text-muted); border: 1px solid var(--border-line); }

        .vote-btn-wrap { display: flex; gap: 10px; margin-top: 10px; }
        .vote-btn { flex: 1; padding: 10px; border-radius: 12px; border: 1px solid var(--border-line); background: var(--bg-subcard); color: var(--text-title); font-size: 0.88rem; font-weight: 700; cursor: pointer; display: flex; align-items: center; justify-content: center; gap: 6px; transition: all 0.2s; }
        .vote-btn:hover { background: var(--btn-hover); border-color: var(--border-focus); }
        .vote-btn.active-long { background: rgba(16, 185, 129, 0.15); border-color: #10b981; color: #10b981; }
        .vote-btn.active-short { background: rgba(56, 189, 248, 0.15); border-color: #38bdf8; color: #38bdf8; }

        .comment-input-box { display: flex; gap: 8px; margin-top: 12px; }
        .comment-input-box input { flex: 1; min-width: 0; background: var(--bg-subcard); border: 1px solid var(--border-line); border-radius: 20px; padding: 9px 14px; color: var(--text-title); font-size: 0.82rem; outline: none; }
        .comment-input-box button { background: #10b981; border: none; color: #ffffff; font-size: 0.8rem; font-weight: 700; padding: 0 16px; border-radius: 20px; cursor: pointer; flex-shrink: 0; }
        .comment-item { padding: 9px 0; border-bottom: 1px solid var(--border-line); font-size: 0.82rem; }
        .comment-item:last-child { border-bottom: none; }
        .comment-head { display: flex; justify-content: space-between; color: var(--text-muted); font-size: 0.7rem; margin-bottom: 3px; }
        .comment-nick { font-weight: 700; color: #10b981; }
        .comment-body { color: var(--text-main); word-break: break-all; }

        .signal-premium-card {
            background: var(--bg-card) !important;
            border: 1.8px solid #10b981 !important;
            border-radius: 18px !important;
            padding: 18px 20px !important;
            margin-bottom: 16px !important;
            box-shadow: 0 6px 24px var(--signal-glow) !important;
            transition: all 0.25s ease;
            max-width: 832px;
            margin-left: auto;
            margin-right: auto;
        }

        .signal-card-header {
            display: flex;
            align-items: center;
            justify-content: center;
            position: relative;
            cursor: pointer;
            user-select: none;
            padding: 6px 0;
        }
        .signal-card-title {
            font-size: 1.15rem !important;
            font-weight: 800 !important;
            color: #10b981 !important;
            text-align: center;
            letter-spacing: -0.4px;
        }
        .signal-arrow-icon {
            width: 22px;
            height: 22px;
            fill: #10b981 !important;
            position: absolute;
            right: 4px;
            transition: transform 0.25s ease;
        }

        .signal-premium-btn {
            width: 100%;
            max-width: 832px;
            margin-left: auto;
            margin-right: auto;
            background: var(--bg-card);
            border: 1.8px solid #10b981;
            border-radius: 16px;
            padding: 15px 18px;
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 10px;
            color: #10b981;
            font-size: 0.95rem;
            font-weight: 800;
            text-decoration: none;
            cursor: pointer;
            box-shadow: 0 4px 18px var(--signal-glow);
            margin-bottom: 16px;
            transition: all 0.2s ease;
            word-break: keep-all;
            text-align: center;
        }
        .signal-premium-btn:hover {
            background: rgba(16, 185, 129, 0.1);
            border-color: #059669;
            color: #059669;
        }
        .signal-premium-btn svg {
            fill: #10b981;
            flex-shrink: 0;
            transition: fill 0.2s;
        }
        .signal-premium-btn:hover svg {
            fill: #059669;
        }

        .gauge-inner-box {
            width: 62px;
            height: 62px;
            border-radius: 50%;
            border: 4px solid #10b981;
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            background: var(--bg-subcard);
            flex-shrink: 0;
        }

        .guide-box { padding: 14px 0 0 0; margin-top: 14px; font-size: 0.85rem; line-height: 1.6; color: var(--text-sub); border-top: 1px solid rgba(16, 185, 129, 0.2); }
        .guide-title { font-weight: 800; color: #10b981; margin-bottom: 6px; display: flex; align-items: center; gap: 4px; font-size: 0.92rem; }

        .news-item { padding: 10px 0; border-bottom: 1px solid var(--border-line); text-decoration: none; display: block; }
        .news-item:last-child { border-bottom: none; }
        .news-title { color: var(--text-main); font-size: 0.85rem; font-weight: 500; line-height: 1.4; }
        .news-title:hover { color: #0284c7; }
        .news-meta { color: var(--text-muted); font-size: 0.72rem; margin-top: 4px; }

        #bottom-bar { display: none; position: fixed; bottom: 0; left: 0; right: 0; height: 74px; background: var(--bottom-fade); align-items: center; justify-content: center; padding: 0 12px 6px; z-index: 50; }
        .loading { display: none; text-align: center; color: #0284c7; margin: 40px 0; font-size: 0.9rem; }

        .dot-marker-style {
            position: absolute;
            top: -4px;
            width: 14px;
            height: 14px;
            border-radius: 50%;
            background: #10b981;
            border: 2px solid #ffffff;
            box-shadow: 0 1px 4px rgba(0,0,0,0.3);
            transform: translateX(-50%);
        }

        @media (max-width: 1200px) {
            .chart-dual-grid {
                grid-template-columns: 1fr;
                max-width: 832px;
            }
            .global-board-card {
                margin-top: 4px;
            }
        }
    </style>
</head>
<body>

    <div id="sidebar-wrapper">
        <div class="side-top-action">
            <span style="font-weight:700; font-size:1.15rem; color:var(--text-title); cursor:pointer;" onclick="resetToHome()">gaemiGTP</span>
            
            <button class="gemini-menu-icon" onclick="toggleSidebar()" title="사이드 패널 닫기">
                <svg width="22" height="22" viewBox="0 0 24 24" fill="currentColor">
                    <path d="M4 19V5C4 4.45 4.19583 3.97917 4.5875 3.5875C4.97917 3.19583 5.45 3 6 3H18C18.55 3 19.0208 3.19583 19.4125 3.5875C19.8042 3.97917 20 4.45 20 5V19C20 19.55 19.8042 20.0208 19.4125 20.4125C19.0208 20.8042 18.55 21 18 21H6C5.45 21 4.97917 20.8042 4.5875 20.4125C4.19583 20.0208 4 19.55 4 19ZM9 19H18V5H9V19ZM6 5V19V5Z"/>
                </svg>
            </button>
        </div>

        <button class="gemini-new-btn" onclick="resetToHome()">
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <path d="M12 4v16m8-8H4" stroke-linecap="round" stroke-linejoin="round"/>
            </svg>
            <span>새 종목</span>
        </button>

        <div class="sidebar-scroll-area">
            <div class="sidebar-section-title">최근 종목</div>
            <div class="sidebar-item" onclick="doSearch('SK하이닉스');">SK하이닉스</div>
            <div class="sidebar-item" onclick="doSearch('엔비디아');">엔비디아</div>
            <div class="sidebar-item" onclick="doSearch('삼성전자');">삼성전자</div>
            <div class="sidebar-item" onclick="doSearch('비트코인');">비트코인</div>
        </div>
    </div>

    <div id="content-wrapper">
        <header>
            <div class="header-left">
                <button class="gemini-menu-icon" id="headerToggleBtn" onclick="toggleSidebar()" title="사이드 패널 열기">
                    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="3" y1="12" x2="21" y2="12"></line><line x1="3" y1="6" x2="21" y2="6"></line><line x1="3" y1="18" x2="21" y2="18"></line></svg>
                </button>
                <div class="logo-top" id="headerLogo" onclick="resetToHome()">gaemiGTP</div>
            </div>

            <button class="gemini-menu-icon" id="themeToggleBtn" onclick="toggleTheme()" title="화면 테마 변경">
                <span id="themeIconArea"></span>
            </button>
        </header>

        <div id="main-view">
            <div class="hero-title">gaemiGTP</div>
            <div class="hero-sub">종목명 하나면 10초 만에 끝납니다</div>
            <div class="search-container">
                <div class="search-box">
                    <input type="text" id="centerInput" placeholder="종목명 입력 (예: SK하이닉스, 삼성전자, NVDA, BTC)" onkeypress="if(event.keyCode==13) doSearch(this.value);" autocomplete="off">
                    
                    <div class="model-selector-wrap">
                        <div class="model-pill-btn" onclick="toggleModelDropdown('center')">
                            <span class="model-pill-name" id="centerCurrentModel">gaemi</span>
                            <span class="model-pill-arrow">▲</span>
                        </div>
                        <div class="model-dropdown-menu" id="centerDropdown">
                            <div class="model-dropdown-item active" onclick="selectModel('gaemi', 'gaemi')">
                                <div class="item-title">gaemi <span class="item-check">✓</span></div>
                                <div class="item-sub">도움을 받으세요</div>
                            </div>
                            <div class="model-dropdown-item" onclick="selectModel('info', 'info')">
                                <div class="item-title">info <span class="item-check">✓</span></div>
                                <div class="item-sub">정보를 확인하세요</div>
                            </div>
                            <div class="model-dropdown-item" onclick="selectModel('pro', 'pro')">
                                <div class="item-title">pro <span class="item-check">✓</span></div>
                                <div class="item-sub">실력을 키우세요</div>
                            </div>
                        </div>
                    </div>

                    <button class="send-btn" onclick="doSearch(document.getElementById('centerInput').value)">
                        <svg width="18" height="18" viewBox="0 0 24 24"><path d="M12 4L12 20M12 4L5 11M12 4L19 11" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" fill="none"/></svg>
                    </button>
                </div>
            </div>
            <div class="popular-tags">
                <button class="tag-btn" onclick="doSearch('SK하이닉스')">SK하이닉스</button>
                <button class="tag-btn" onclick="doSearch('삼성전자')">삼성전자</button>
                <button class="tag-btn" onclick="doSearch('알테오젠')">알테오젠</button>
                <button class="tag-btn" onclick="doSearch('엔비디아')">엔비디아</button>
                <button class="tag-btn" onclick="doSearch('비트코인')">비트코인</button>
            </div>
        </div>

        <div id="result-scroll-view">
            <div class="result-inner">
                <div class="loading" id="loading">세계 증시, 전자공시 및 실시간 퀀트 데이터를 연산 중입니다...</div>
                <div id="result-card-wrap" style="display:none;">
                    
                    <!-- 1. 메인 차트 + 우측 '증시는 지금' -->
                    <div class="chart-dual-grid">
                        <div class="chart-main-card">
                            <div>
                                <div class="price-title" id="rTitle"></div>
                                <div class="price-val" id="rPrice"></div>
                                <div class="price-meta" id="rMeta"></div>
                                <div class="chart-legend">
                                    <div class="legend-tag"><span class="legend-dot" style="background:#00b4d8;"></span> 20일선 (슬림)</div>
                                    <div class="legend-tag"><span class="legend-dot" style="background:#f97316;"></span> 볼린저 60선 (중심)</div>
                                    <div class="legend-tag"><span class="legend-dot" style="background:#f87171;"></span> 볼린저 상단 (저항)</div>
                                    <div class="legend-tag"><span class="legend-dot" style="background:#38bdf8;"></span> 볼린저 하단 (지지)</div>
                                </div>
                            </div>
                            <div class="chart-box-wrap" id="tvChart"></div>
                        </div>

                        <!-- 우측 10대 지표 보드 -->
                        <div class="global-board-card">
                            <div class="board-header">
                                <span class="board-header-title">증시는 지금</span>
                                <span class="board-header-sub">실시간 연동</span>
                            </div>
                            <div class="board-2col-grid" id="worldBoardGrid"></div>
                        </div>
                    </div>

                    <!-- 2. 차트 바로 아래: 퀀트 분석 카드 -->
                    <div class="card">
                        <div style="margin-bottom:16px; display:flex; justify-content:space-between; align-items:center;">
                            <span style="font-size:1.05rem; font-weight:800; color:var(--text-title);">퀀트 분석</span>
                            <span id="qTrendBadge" style="font-size:0.75rem; color:#0284c7; font-weight:700;"></span>
                        </div>

                        <!-- 상단 요약 게이지 & 안전한 모멘텀 상태 뱃지 -->
                        <div style="display:flex; align-items:center; gap:14px; padding-bottom:16px; border-bottom:1px solid var(--border-line);">
                            <div class="gauge-inner-box">
                                <span style="font-size:1.05rem; font-weight:800; color:var(--text-title); line-height:1;" id="qScoreVal"></span>
                                <span style="font-size:0.55rem; color:var(--text-muted); margin-top:3px; font-weight:700;">모멘텀</span>
                            </div>
                            <div style="min-width:0;">
                                <div style="display:flex; align-items:center; gap:6px; flex-wrap:wrap;">
                                    <span style="font-size:0.78rem; color:var(--text-muted);">계량 상태</span>
                                    <span style="background:rgba(16,185,129,0.15); color:#10b981; font-size:0.72rem; font-weight:800; padding:2px 7px; border-radius:10px;" id="qScoreBadge"></span>
                                </div>
                                <div style="font-size:0.74rem; color:var(--text-muted); margin-top:4px; word-break:keep-all;" id="qScoreDots"></div>
                            </div>
                        </div>

                        <!-- 2개 과학적 핵심 통계 지표 -->
                        <div style="display:flex; justify-content:space-between; align-items:flex-end; margin-top:14px;">
                            <div>
                                <div style="font-size:0.72rem; color:var(--text-muted); margin-bottom:2px;">통계적 Z-Score (20D 표준편차)</div>
                                <div style="font-size:1.25rem; font-weight:800; color:#10b981;" id="qZScoreVal"></div>
                            </div>
                            <div style="text-align:right;">
                                <div style="font-size:0.72rem; color:var(--text-muted); margin-bottom:2px;">연환산 변동성 (20D σ)</div>
                                <div style="font-size:1.15rem; font-weight:700; color:var(--text-title);" id="qVolVal"></div>
                            </div>
                        </div>

                        <!-- 52주 가격 위치 + 상승 시그널 흐름 -->
                        <div style="margin:20px 0 14px; position:relative;">
                            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:6px;">
                                <span style="font-size:0.72rem; color:var(--text-muted);">최근 60거래일 상승 시그널 흐름</span>
                                <span style="font-size:0.68rem; color:#10b981; font-weight:700;" id="qSignalNow">현재 -</span>
                            </div>
                            <div style="width:100%; height:54px; background:var(--bg-subcard); border-radius:8px; position:relative; border:1px solid var(--border-line); overflow:hidden;">
                                <svg id="qSignalChart" viewBox="0 0 600 120" preserveAspectRatio="none" style="width:100%; height:100%; display:block;"></svg>
                            </div>
                        </div>

                        <!-- 52주 가격 위치 슬라이더 바 -->
                        <div style="margin:20px 0 14px; position:relative;">
                            <div style="display:flex; justify-content:space-between; font-size:0.7rem; margin-bottom:6px; white-space:nowrap;">
                                <span style="color:var(--text-muted);" id="qLow52w"></span>
                                <span style="color:#10b981; font-weight:700;" id="qPos52wLabel">52주 가격 위치</span>
                                <span style="color:var(--text-muted);" id="qHigh52w"></span>
                            </div>
                            <div style="width:100%; height:6px; background:var(--bg-subcard); border-radius:4px; position:relative; border:1px solid var(--border-line);">
                                <div style="height:100%; background:linear-gradient(90deg, #38bdf8, #10b981); border-radius:4px;" id="qSliderFill"></div>
                                <div class="dot-marker-style" id="qDotMarker"></div>
                            </div>
                            <div style="display:flex; justify-content:space-between; font-size:0.68rem; color:var(--text-muted); margin-top:6px; white-space:nowrap;">
                                <span>52주 최저</span>
                                <span>52주 최고</span>
                            </div>
                        </div>

                        <!-- 세부 지표 & 과학적 통계 라벨 리스트 -->
                        <div style="margin-top:16px; border-top:1px solid var(--border-line); padding-top:12px;">
                            <div style="font-size:0.82rem; font-weight:700; color:var(--text-sub); margin-bottom:8px; display:flex; justify-content:space-between;">
                                <span>계량 기술 지표 세부 판정</span>
                                <span style="font-size:0.68rem; color:var(--text-muted); font-weight:normal;">14대 퀀트 알고리즘</span>
                            </div>
                            <div id="quantDetailList"></div>
                        </div>
                    </div>

                    <!-- 3. 기술 지표 바로 아래: 종목 뉴스 -->
                    <div class="card">
                        <div class="section-title">종목 뉴스</div>
                        <div id="stockNewsList"></div>
                    </div>

                    <!-- 4. 3대 시장 통합 컨센서스 카드 -->
                    <div class="card" id="unifiedCard" style="display:none;">
                        <div style="margin-bottom:16px;">
                            <span style="font-size:1.05rem; font-weight:700; color:var(--text-title);" id="uTitle"></span>
                        </div>

                        <div style="display:flex; align-items:center; gap:14px; padding-bottom:16px; border-bottom:1px solid var(--border-line);">
                            <div class="gauge-inner-box">
                                <span style="font-size:1.0rem; font-weight:800; color:var(--text-title); line-height:1;" id="uGaugeVal"></span>
                                <span style="font-size:0.55rem; color:var(--text-muted); margin-top:3px;" id="uGaugeLabel"></span>
                            </div>
                            <div style="min-width:0;">
                                <div style="display:flex; align-items:center; gap:6px; flex-wrap:wrap;">
                                    <span style="font-size:0.78rem; color:var(--text-muted);" id="uStatusLabel"></span>
                                    <span style="background:rgba(16,185,129,0.15); color:#10b981; font-size:0.72rem; font-weight:700; padding:2px 7px; border-radius:10px;" id="uBadge"></span>
                                </div>
                                <div style="font-size:0.74rem; color:var(--text-muted); margin-top:4px; word-break:keep-all;" id="uDots"></div>
                            </div>
                        </div>

                        <div style="display:flex; justify-content:space-between; align-items:flex-end; margin-top:14px;">
                            <div>
                                <div style="font-size:0.72rem; color:var(--text-muted); margin-bottom:2px;" id="uPrimaryLabel"></div>
                                <div style="font-size:1.25rem; font-weight:800; color:#10b981;" id="uPrimaryVal"></div>
                            </div>
                            <div style="text-align:right;">
                                <div style="font-size:0.72rem; color:var(--text-muted); margin-bottom:2px;" id="uSecondaryLabel"></div>
                                <div style="font-size:1.15rem; font-weight:700; color:var(--text-title);" id="uSecondaryVal"></div>
                            </div>
                        </div>

                        <div style="margin:20px 0 14px; position:relative;">
                            <div style="display:flex; justify-content:space-between; font-size:0.7rem; margin-bottom:6px; white-space:nowrap;">
                                <span style="color:var(--text-muted);" id="uSliderLeft"></span>
                                <span style="color:#10b981; font-weight:700;" id="uSliderMid"></span>
                                <span style="color:var(--text-muted);" id="uSliderRight"></span>
                            </div>
                            <div style="width:100%; height:6px; background:var(--bg-subcard); border-radius:4px; position:relative; border:1px solid var(--border-line);">
                                <div style="height:100%; background:linear-gradient(90deg, #38bdf8, #10b981); border-radius:4px;" id="uBarFill"></div>
                                <div class="dot-marker-style" id="uDotMarker"></div>
                            </div>
                            <div style="display:flex; justify-content:space-between; font-size:0.68rem; color:var(--text-muted); margin-top:6px; white-space:nowrap;">
                                <span>52주 최저</span>
                                <span>52주 최고</span>
                            </div>
                        </div>

                        <div style="margin-top:16px; border-top:1px solid var(--border-line); padding-top:12px;">
                            <div style="font-size:0.82rem; font-weight:700; color:var(--text-sub); margin-bottom:8px; display:flex; justify-content:space-between;">
                                <span id="uListTitle"></span>
                                <span style="font-size:0.68rem; color:var(--text-muted); font-weight:normal;" id="uSourceTag"></span>
                            </div>
                            <div id="uItemList"></div>
                        </div>
                    </div>

                    <!-- 5. 개미 투표 카드 -->
                    <div class="card">
                        <div class="section-title">
                            <span>개미 투표</span>
                            <span style="font-size:0.72rem; color:var(--text-muted); font-weight:normal;">내일 주가 전망</span>
                        </div>
                        <div style="margin:8px 0;">
                            <div style="display:flex; justify-content:space-between; font-size:0.75rem; font-weight:700; margin-bottom:6px;">
                                <span style="color:#10b981;" id="voteLongPct">상승 50%</span>
                                <span style="color:#38bdf8;" id="voteShortPct">하락 50%</span>
                            </div>
                            <div style="width:100%; height:8px; background:var(--bg-subcard); border-radius:4px; overflow:hidden; display:flex; border:1px solid var(--border-line);">
                                <div style="height:100%; background:#10b981; width:50%; transition:width 0.3s;" id="voteLongBar"></div>
                                <div style="height:100%; background:#38bdf8; width:50%; transition:width 0.3s;" id="voteShortBar"></div>
                            </div>
                            <div style="display:flex; justify-content:space-between; font-size:0.68rem; color:var(--text-muted); margin-top:4px;">
                                <span id="voteLongCount">0명</span>
                                <span id="voteShortCount">0명</span>
                            </div>
                        </div>
                        <div class="vote-btn-wrap">
                            <button class="vote-btn" id="btnVoteLong" onclick="castVote('long')">상승 전망</button>
                            <button class="vote-btn" id="btnVoteShort" onclick="castVote('short')">하락 전망</button>
                        </div>
                    </div>

                    <!-- 6. 개미 토론 카드 -->
                    <div class="card">
                        <div class="section-title">
                            <span>개미 토론</span>
                            <span style="font-size:0.72rem; color:var(--text-muted); font-weight:normal;" id="commentCountTag">0개의 의견</span>
                        </div>
                        <div id="commentList" style="margin-top:8px;">
                            <div style="color:var(--text-muted); font-size:0.8rem; padding:10px 0; text-align:center;">첫 번째 한마디를 남겨보세요!</div>
                        </div>
                        <div class="comment-input-box">
                            <input type="text" id="commentInput" placeholder="이 종목에 대한 생각을 자유롭게 적어주세요" onkeypress="if(event.keyCode==13) postComment();" autocomplete="off" maxlength="80">
                            <button onclick="postComment()">등록</button>
                        </div>
                    </div>

                    <!-- 7. DART 전자공시 카드 -->
                    <div class="card" id="dartCard" style="display:none;">
                        <div class="section-title">
                            <span>DART 전자공시</span>
                            <span style="font-size:0.72rem; color:var(--text-muted); font-weight:normal;">최근 핵심 공시</span>
                        </div>
                        <div id="dartList"></div>
                    </div>

                    <!-- 8. 해외 뉴스 -->
                    <div class="card">
                        <div class="section-title">해외 뉴스</div>
                        <div id="globalNewsList"></div>
                    </div>

                    <!-- 9. gaemiGTP 시그널 박스 -->
                    <div class="signal-premium-card">
                        <div class="signal-card-header" onclick="toggleSignal()">
                            <span class="signal-card-title">gaemiGTP 시그널</span>
                            <svg class="signal-arrow-icon" id="signalArrow" viewBox="0 0 24 24"><path d="M7 10l5 5 5-5z"/></svg>
                        </div>
                        
                        <div id="signalBody" style="display:none; margin-top:16px;">
                            <div style="display:flex; align-items:center; gap:14px; padding-bottom:16px; border-bottom:1px solid rgba(16,185,129,0.2);">
                                <div class="gauge-inner-box">
                                    <span style="font-size:1.05rem; font-weight:800; color:var(--text-title); line-height:1;" id="rWinRateGauge"></span>
                                    <span style="font-size:0.6rem; color:var(--text-muted); margin-top:3px; font-weight:700;">승률</span>
                                </div>
                                <div style="min-width:0;">
                                    <div style="display:flex; align-items:center; gap:6px; flex-wrap:wrap;">
                                        <span style="font-size:0.82rem; color:var(--text-muted);">시그널 종합</span>
                                        <span style="background:rgba(16,185,129,0.15); color:#10b981; font-size:0.75rem; font-weight:800; padding:3px 8px; border-radius:10px;">분할 대응 유효</span>
                                    </div>
                                    <div style="font-size:0.78rem; color:var(--text-muted); margin-top:5px; word-break:keep-all;">
                                        <span style="color:#10b981; font-weight:800;" id="rBt1yTag"></span> &nbsp; 
                                        <span style="color:#10b981; font-weight:800;" id="rBt2yTag"></span> &nbsp; 
                                        <span style="color:#38bdf8; font-weight:800;" id="rRrTag"></span>
                                    </div>
                                </div>
                            </div>

                            <div style="margin:20px 0 14px; position:relative;">
                                <div style="display:flex; justify-content:space-between; font-size:0.75rem; margin-bottom:6px; white-space:nowrap;">
                                    <span style="color:#38bdf8; font-weight:800;" id="rSliderStop"></span>
                                    <span style="color:var(--text-title); font-weight:800;" id="rSliderEntry"></span>
                                    <span style="color:#10b981; font-weight:800;" id="rSliderTarget"></span>
                                </div>
                                <div style="width:100%; height:7px; background:var(--bg-subcard); border-radius:4px; position:relative; border:1px solid var(--border-line);">
                                    <div style="height:100%; background:linear-gradient(90deg, #38bdf8, #10b981); border-radius:4px;" id="rSigBarFill"></div>
                                    <div class="dot-marker-style" id="rSigDotMarker"></div>
                                </div>
                                <div style="display:flex; justify-content:space-between; font-size:0.7rem; color:var(--text-muted); margin-top:6px; white-space:nowrap; font-weight:600;">
                                    <span>방어선</span>
                                    <span>목표선</span>
                                </div>
                            </div>

                            <div style="margin-top:16px; border-top:1px solid rgba(16,185,129,0.2); padding-top:10px;">
                                <div style="display:flex; justify-content:space-between; align-items:center; padding:8px 0; border-bottom:1px solid rgba(16,185,129,0.15); font-size:0.86rem;">
                                    <div><span style="color:var(--text-title); font-weight:700;">1차 진입 기준가</span></div>
                                    <div style="color:var(--text-title); font-weight:800; white-space:nowrap;" id="rEntry1"></div>
                                </div>
                                <div style="display:flex; justify-content:space-between; align-items:center; padding:8px 0; border-bottom:1px solid rgba(16,185,129,0.15); font-size:0.86rem;">
                                    <div><span style="color:var(--text-title); font-weight:700;">2차 분할 기준가</span></div>
                                    <div style="color:var(--text-title); font-weight:800; white-space:nowrap;" id="rEntry2"></div>
                                </div>
                                <div style="display:flex; justify-content:space-between; align-items:center; padding:8px 0; border-bottom:1px solid rgba(16,185,129,0.15); font-size:0.86rem;">
                                    <div><span style="color:#10b981; font-weight:700;">1차 목표가</span></div>
                                    <div style="color:#10b981; font-weight:800; white-space:nowrap;" id="rTarget1"></div>
                                </div>
                                <div style="display:flex; justify-content:space-between; align-items:center; padding:8px 0; border-bottom:1px solid rgba(16,185,129,0.15); font-size:0.86rem;">
                                    <div><span style="color:#10b981; font-weight:700;">2차 목표가</span></div>
                                    <div style="color:#10b981; font-weight:800; white-space:nowrap;" id="rTarget2"></div>
                                </div>
                                <div style="display:flex; justify-content:space-between; align-items:center; padding:8px 0; font-size:0.86rem;">
                                    <div><span style="color:#38bdf8; font-weight:700;">리스크 방어선</span></div>
                                    <div style="color:#38bdf8; font-weight:800; white-space:nowrap;" id="rStop"></div>
                                </div>
                            </div>

                            <div class="guide-box">
                                <div class="guide-title">gaemiGTP 시그널 가이드</div>
                                <div id="rGuideText" style="margin-top:4px; color:var(--text-sub); word-break:keep-all;"></div>
                            </div>
                        </div>
                    </div>

                    <!-- 10. 텔레그램 실시간 알림 받기 버튼 -->
                    <a id="tgAlertLink" href="#" target="_blank" class="signal-premium-btn">
                        <svg width="20" height="20" viewBox="0 0 24 24"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm4.64 6.8c-.15 1.58-.8 5.42-1.13 7.19-.14.75-.42 1-.68 1.03-.58.05-1.02-.38-1.58-.75-.88-.58-1.38-.94-2.23-1.5-.99-.65-.35-1.01.22-1.59.15-.15 2.71-2.48 2.76-2.69a.2.2 0 00-.05-.18c-.06-.05-.14-.03-.21-.02-.09.02-1.49.95-4.22 2.79-.4.27-.76.41-1.08.4-.36-.01-1.04-.2-1.55-.37-.63-.2-1.12-.31-1.08-.66.02-.18.27-.36.74-.55 2.92-1.27 4.86-2.11 5.83-2.51 2.78-1.16 3.35-1.36 3.73-1.36.08 0 .27.02.39.12.1.08.13.19.14.27-.01.06.01.24 0 .38z"/></svg>
                        <span id="tgAlertBtnText">gaemiGTP 시그널 텔레그램 실시간 알림 받기</span>
                    </a>

                </div>
            </div>
        </div>

        <div id="bottom-bar">
            <div class="search-container">
                <div class="search-box">
                    <input type="text" id="bottomInput" placeholder="다른 종목명 입력 (예: 테슬라, 카카오, BTC)" onkeypress="if(event.keyCode==13) doSearch(this.value);" autocomplete="off">
                    
                    <div class="model-selector-wrap">
                        <div class="model-pill-btn" onclick="toggleModelDropdown('bottom')">
                            <span class="model-pill-name" id="bottomCurrentModel">gaemi</span>
                            <span class="model-pill-arrow">▲</span>
                        </div>
                        <div class="model-dropdown-menu" id="bottomDropdown">
                            <div class="model-dropdown-item active" onclick="selectModel('gaemi', 'gaemi')">
                                <div class="item-title">gaemi <span class="item-check">✓</span></div>
                                <div class="item-sub">도움을 받으세요</div>
                            </div>
                            <div class="model-dropdown-item" onclick="selectModel('info', 'info')">
                                <div class="item-title">info <span class="item-check">✓</span></div>
                                <div class="item-sub">정보를 확인하세요</div>
                            </div>
                            <div class="model-dropdown-item" onclick="selectModel('pro', 'pro')">
                                <div class="item-title">pro <span class="item-check">✓</span></div>
                                <div class="item-sub">실력을 키우세요</div>
                            </div>
                        </div>
                    </div>

                    <button class="send-btn" onclick="doSearch(document.getElementById('bottomInput').value)">
                        <svg width="18" height="18" viewBox="0 0 24 24"><path d="M12 4L12 20M12 4L5 11M12 4L19 11" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" fill="none"/></svg>
                    </button>
                </div>
            </div>
        </div>
    </div>

    <script>
        let chart = null;
        let selectedMode = 'gaemi';
        let currentSymbol = '';

        function setTxt(id, val) {
            const el = document.getElementById(id);
            if (el) el.innerText = (val !== undefined && val !== null) ? String(val) : '';
        }
        function setHtml(id, val) {
            const el = document.getElementById(id);
            if (el) el.innerHTML = (val !== undefined && val !== null) ? String(val) : '';
        }

        const SUPABASE_URL = "https://uaqvmsubzgzvxbdcwcad.supabase.co";
        const SUPABASE_KEY = "sb_publishable_bcxDofsnPXgBICDDgQ8Ijw_fNjPcsRW";
        let supabaseClient = null;
        try {
            if (window.supabase) {
                supabaseClient = window.supabase.createClient(SUPABASE_URL, SUPABASE_KEY);
            }
        } catch(e) {
            console.warn("Supabase fallback:", e);
        }

        function initTheme() {
            const saved = localStorage.getItem('gaemi_theme') || 'dark';
            if (saved === 'light') {
                document.body.classList.add('light-theme');
                updateThemeIcon(true);
            } else {
                document.body.classList.remove('light-theme');
                updateThemeIcon(false);
            }
        }

        function toggleTheme() {
            const isLight = document.body.classList.toggle('light-theme');
            localStorage.setItem('gaemi_theme', isLight ? 'light' : 'dark');
            updateThemeIcon(isLight);

            if (chart) {
                chart.applyOptions({
                    layout: {
                        background: { color: isLight ? '#ffffff' : '#1e1f20' },
                        textColor: isLight ? '#5f6368' : '#8e918f'
                    },
                    timeScale: { borderColor: isLight ? '#dadce0' : '#282a2c' },
                    rightPriceScale: { borderColor: isLight ? '#dadce0' : '#282a2c' }
                });
            }
        }

        function updateThemeIcon(isLight) {
            const area = document.getElementById('themeIconArea');
            if (!area) return;
            area.innerHTML = isLight 
                ? `<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"></path></svg>`
                : `<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="5"></circle><line x1="12" y1="1" x2="12" y2="3"></line><line x1="12" y1="21" x2="12" y2="23"></line><line x1="4.22" y1="4.22" x2="5.64" y2="5.64"></line><line x1="18.36" y1="18.36" x2="19.78" y2="19.78"></line><line x1="1" y1="12" x2="3" y2="12"></line><line x1="21" y1="12" x2="23" y2="12"></line><line x1="4.22" y1="19.78" x2="5.64" y2="18.36"></line><line x1="18.36" y1="5.64" x2="19.78" y2="4.22"></line></svg>`;
        }

        function resizeChart() {
            const container = document.getElementById('tvChart');
            if (chart && container && container.clientWidth > 0) {
                chart.applyOptions({ width: container.clientWidth });
            }
        }

        function toggleSidebar() {
            const side = document.getElementById('sidebar-wrapper');
            if (side) side.classList.toggle('open');
        }

        function toggleModelDropdown(target) {
            const centerD = document.getElementById('centerDropdown');
            const bottomD = document.getElementById('bottomDropdown');
            const targetD = target === 'center' ? centerD : bottomD;
            const otherD = target === 'center' ? bottomD : centerD;
            if (otherD) otherD.style.display = 'none';
            if (targetD) targetD.style.display = targetD.style.display === 'block' ? 'none' : 'block';
        }

        function selectModel(modeKey, modeName) {
            selectedMode = modeKey;
            setTxt('centerCurrentModel', modeName);
            setTxt('bottomCurrentModel', modeName);

            ['centerDropdown', 'bottomDropdown'].forEach(id => {
                const menu = document.getElementById(id);
                if (menu) {
                    const items = menu.querySelectorAll('.model-dropdown-item');
                    items.forEach(it => {
                        if (it.innerText.includes(modeName)) {
                            it.classList.add('active');
                        } else {
                            it.classList.remove('active');
                        }
                    });
                    menu.style.display = 'none';
                }
            });
        }

        document.addEventListener('click', function(e) {
            if (!e.target.closest('.model-selector-wrap')) {
                const cd = document.getElementById('centerDropdown');
                const bd = document.getElementById('bottomDropdown');
                if (cd) cd.style.display = 'none';
                if (bd) bd.style.display = 'none';
            }
            if (!e.target.closest('#sidebar-wrapper') && !e.target.closest('#headerToggleBtn')) {
                const side = document.getElementById('sidebar-wrapper');
                if (side && side.classList.contains('open')) {
                    side.classList.remove('open');
                }
            }
        });

        function resetToHome() {
            document.getElementById('main-view').style.display = 'flex';
            document.getElementById('result-scroll-view').style.display = 'none';
            document.getElementById('bottom-bar').style.display = 'none';
            document.getElementById('result-card-wrap').style.display = 'none';
            document.getElementById('loading').style.display = 'none';
            
            const uCard = document.getElementById('unifiedCard');
            if (uCard) uCard.style.display = 'none';
            const dCard = document.getElementById('dartCard');
            if (dCard) dCard.style.display = 'none';
            
            document.getElementById('centerInput').value = '';
            document.getElementById('bottomInput').value = '';
            currentSymbol = '';
            
            const side = document.getElementById('sidebar-wrapper');
            if (side) side.classList.remove('open');
        }

        function toggleSignal() {
            const body = document.getElementById('signalBody');
            const arrow = document.getElementById('signalArrow');
            if (!body || !arrow) return;
            if (body.style.display === 'block') {
                body.style.display = 'none';
                arrow.style.transform = 'rotate(0deg)';
            } else {
                body.style.display = 'block';
                arrow.style.transform = 'rotate(180deg)';
            }
        }

        function renderSparklineSVG(points, isUp) {
            if (!points || points.length < 2) return '';
            const min = Math.min(...points);
            const max = Math.max(...points);
            const range = (max - min) === 0 ? 1 : (max - min);
            const width = 44;
            const height = 16;
            const padY = 2;
            
            const coords = points.map((p, i) => {
                const x = (i / (points.length - 1)) * width;
                const y = height - padY - ((p - min) / range) * (height - padY * 2);
                return `${x.toFixed(1)},${y.toFixed(1)}`;
            });
            
            const pathD = 'M ' + coords.join(' L ');
            const strokeColor = isUp ? '#ef4444' : '#38bdf8';
            
            return `<svg width="${width}" height="${height}" viewBox="0 0 ${width} ${height}" class="mini-spark">
                <path d="${pathD}" fill="none" stroke="${strokeColor}" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/>
            </svg>`;
        }

        async function loadCommunityData(symbol, displayName) {
            currentSymbol = symbol;
            const targetName = displayName || symbol;
            
            const tgLink = document.getElementById('tgAlertLink');
            const tgBtnText = document.getElementById('tgAlertBtnText');
            if (tgLink && tgBtnText) {
                tgLink.href = 'https://t.me/gaemigtp_bot?start=' + encodeURIComponent(symbol);
                tgBtnText.innerText = '[' + targetName + '] gaemiGTP 시그널 텔레그램 실시간 알림 받기';
            }

            const voteKey = 'vote_' + symbol;
            const rawVote = localStorage.getItem(voteKey) || JSON.stringify({ long: 0, short: 0, myVote: null });
            let vData = JSON.parse(rawVote);

            if (supabaseClient) {
                try {
                    const { data, error } = await supabaseClient
                        .from('votes')
                        .select('*')
                        .eq('symbol', symbol)
                        .single();
                    if (data && !error) {
                        vData.long = data.long_count || vData.long;
                        vData.short = data.short_count || vData.short;
                    }
                } catch(e) {}
            }
            updateVoteUI(vData);

            let comments = [];
            if (supabaseClient) {
                try {
                    const { data, error } = await supabaseClient
                        .from('comments')
                        .select('*')
                        .eq('symbol', symbol)
                        .order('created_at', { ascending: false })
                        .limit(8);
                    if (data && !error && data.length > 0) {
                        comments = data.map(d => ({
                            nick: d.nickname,
                            text: d.text,
                            time: formatCommentTime(d.created_at)
                        }));
                    }
                } catch(e) {}
            }

            if (comments.length === 0) {
                const cKey = 'comments_' + symbol;
                const rawComm = localStorage.getItem(cKey) || JSON.stringify([]);
                comments = JSON.parse(rawComm);
            }
            renderComments(comments);
        }

        function formatCommentTime(ts) {
            if (!ts) return "방금 전";
            try {
                const diff = (new Date() - new Date(ts)) / 1000;
                if (diff < 60) return "방금 전";
                if (diff < 3600) return `${Math.floor(diff/60)}분 전`;
                if (diff < 86400) return `${Math.floor(diff/3600)}시간 전`;
                return `${Math.floor(diff/86400)}일 전`;
            } catch(e) {
                return "방금 전";
            }
        }

        function updateVoteUI(vData) {
            const total = vData.long + vData.short;
            const lPct = total > 0 ? Math.round((vData.long / total) * 100) : 50;
            const sPct = 100 - lPct;

            setTxt('voteLongPct', `상승 ${lPct}%`);
            setTxt('voteShortPct', `하락 ${sPct}%`);
            
            const lb = document.getElementById('voteLongBar');
            if (lb) lb.style.width = lPct + '%';
            const sb = document.getElementById('voteShortBar');
            if (sb) sb.style.width = sPct + '%';
            
            setTxt('voteLongCount', `${vData.long}명`);
            setTxt('voteShortCount', `${vData.short}명`);

            const btnL = document.getElementById('btnVoteLong');
            const btnS = document.getElementById('btnVoteShort');
            if (btnL && btnS) {
                btnL.classList.remove('active-long');
                btnS.classList.remove('active-short');
                if (vData.myVote === 'long') btnL.classList.add('active-long');
                if (vData.myVote === 'short') btnS.classList.add('active-short');
            }
        }

        async function castVote(type) {
            if (!currentSymbol) return;
            const voteKey = 'vote_' + currentSymbol;
            const rawVote = localStorage.getItem(voteKey) || JSON.stringify({ long: 0, short: 0, myVote: null });
            const vData = JSON.parse(rawVote);

            if (vData.myVote === type) return;

            if (vData.myVote === 'long') vData.long = Math.max(0, vData.long - 1);
            if (vData.myVote === 'short') vData.short = Math.max(0, vData.short - 1);

            if (type === 'long') vData.long += 1;
            if (type === 'short') vData.short += 1;

            vData.myVote = type;
            localStorage.setItem(voteKey, JSON.stringify(vData));
            updateVoteUI(vData);

            if (supabaseClient) {
                try {
                    await supabaseClient
                        .from('votes')
                        .upsert({
                            symbol: currentSymbol,
                            long_count: vData.long,
                            short_count: vData.short
                        });
                } catch(e) {}
            }
        }

        function renderComments(list) {
            const wrap = document.getElementById('commentList');
            if (!wrap) return;
            setTxt('commentCountTag', `${list.length}개의 의견`);
            wrap.innerHTML = '';
            if (list.length === 0) {
                wrap.innerHTML = '<div style="color:var(--text-muted); font-size:0.8rem; padding:10px 0; text-align:center;">첫 번째 한마디를 남겨보세요!</div>';
                return;
            }
            list.slice(0, 8).forEach(c => {
                wrap.innerHTML += `
                    <div class="comment-item">
                        <div class="comment-head">
                            <span class="comment-nick">${c.nick}</span>
                            <span>${c.time}</span>
                        </div>
                        <div class="comment-body">${c.text}</div>
                    </div>
                `;
            });
        }

        async function postComment() {
            const input = document.getElementById('commentInput');
            if (!input) return;
            const txt = input.value.trim();
            if (!txt || !currentSymbol) return;

            const nicks = ["익명개미", "불꽃슈퍼개미", "성투개미", "황금손개미", "존버대장", "퀀트장인"];
            const randomNick = nicks[Math.floor(Math.random() * nicks.length)] + "#" + Math.floor(100 + Math.random() * 900);

            const cKey = 'comments_' + currentSymbol;
            const rawComm = localStorage.getItem(cKey) || '[]';
            const comments = JSON.parse(rawComm);

            const newC = {
                nick: randomNick,
                text: txt,
                time: "방금 전"
            };

            comments.unshift(newC);
            localStorage.setItem(cKey, JSON.stringify(comments));
            renderComments(comments);
            input.value = '';

            if (supabaseClient) {
                try {
                    await supabaseClient
                        .from('comments')
                        .insert({
                            symbol: currentSymbol,
                            nickname: randomNick,
                            text: txt
                        });
                } catch(e) {}
            }
        }

        async function doSearch(query) {
            const cleanQ = query.trim();
            if (!cleanQ) return;

            document.getElementById('centerInput').value = cleanQ;
            document.getElementById('bottomInput').value = cleanQ;

            document.getElementById('main-view').style.display = 'none';
            document.getElementById('result-scroll-view').style.display = 'block';
            document.getElementById('bottom-bar').style.display = 'flex';
            document.getElementById('loading').style.display = 'block';
            document.getElementById('result-card-wrap').style.display = 'none';

            if (window.innerWidth <= 900) {
                const side = document.getElementById('sidebar-wrapper');
                if (side) side.classList.remove('open');
            }

            try {
                const url = '/api/analyze?q=' + encodeURIComponent(cleanQ);
                const response = await fetch(url);
                const data = await response.json();
                if (!response.ok || data.error) {
                    alert(data.error || '종목을 찾을 수 없거나 분석 중 오류가 발생했습니다.');
                    document.getElementById('loading').style.display = 'none';
                    return;
                }

                const isKrw = data.market === '국내주식' || data.market === '국내증시';
                const displayName = data.name || data.formal_name;
                const displayPrice = data.price || data.price_str;
                const displayTime = data.time || data.now_str;

                setHtml('rTitle', `${displayName} <span>(${data.symbol})</span>`);
                setTxt('rPrice', displayPrice);
                setHtml('rMeta', `시장: ${data.market} · 출처: ${isKrw ? 'Naver/KRX' : 'Yahoo/Binance'} · 시각: ${displayTime}`);

                loadCommunityData(data.symbol, displayName);

                const wBoardGrid = document.getElementById('worldBoardGrid');
                if (wBoardGrid) {
                    wBoardGrid.innerHTML = '';
                    if (data.world_board) {
                        data.world_board.forEach(w => {
                            const cls = w.up ? 'val-up' : 'val-down';
                            const sparkSvg = renderSparklineSVG(w.sparkline, w.up);
                            wBoardGrid.innerHTML += `
                                <div class="mini-index-card" onclick="doSearch('${w.name === 'USD/KRW' ? '환율' : w.name}')">
                                    <div class="mini-top">
                                        <span class="mini-name">${w.name}</span>
                                        <span class="mini-tag">${w.tag}</span>
                                    </div>
                                    <div class="mini-mid-row">
                                        <div>
                                            <div class="mini-price">${w.price}</div>
                                            <div class="mini-rate ${cls}">${w.rate}</div>
                                        </div>
                                        ${sparkSvg}
                                    </div>
                                </div>
                            `;
                        });
                    }
                }

                const q = data.quant;
                if (q) {
                    setTxt('qTrendBadge', q.trend);
                    setTxt('qScoreVal', q.quant_score);
                    setTxt('qScoreBadge', q.score_badge);
                    setHtml('qScoreDots', q.score_dots);

                    setTxt('qZScoreVal', q.z_score);
                    setTxt('qVolVal', `${q.ann_vol} (${q.vol_risk_state ? q.vol_risk_state.split(' ')[0] : ''})`);

                    setTxt('qLow52w', `최저 ${q.low_52w}`);
                    setTxt('qHigh52w', `최고 ${q.high_52w}`);

                    const qSignalChart = document.getElementById('qSignalChart');
                    const qSignalNow = document.getElementById('qSignalNow');
                    if (qSignalChart && Array.isArray(q.signal_history) && q.signal_history.length > 1) {
                        const pts = q.signal_history.filter(x => Number.isFinite(Number(x.score)));
                        const scores = pts.map(x => Number(x.score));
                        const minScore = 0;
                        const maxScore = 100;
                        const w = 600;
                        const h = 120;
                        const coords = scores.map((score, i) => {
                            const x = (i / Math.max(1, scores.length - 1)) * w;
                            const y = h - ((score - minScore) / (maxScore - minScore)) * h;
                            return `${x.toFixed(1)},${Math.max(4, Math.min(h-4, y)).toFixed(1)}`;
                        });
                        const linePath = 'M ' + coords.join(' L ');
                        const areaPath = `M ${coords[0].split(',')[0]},${h} L ${coords.join(' L ')} L ${coords[coords.length-1].split(',')[0]},${h} Z`;
                        const lastScore = scores[scores.length - 1];
                        qSignalChart.innerHTML = `
                            <defs>
                                <linearGradient id="qSigFill" x1="0" x2="0" y1="0" y2="1">
                                    <stop offset="0%" stop-color="#10b981" stop-opacity="0.28"/>
                                    <stop offset="100%" stop-color="#10b981" stop-opacity="0.02"/>
                                </linearGradient>
                            </defs>
                            <line x1="0" y1="36" x2="600" y2="36" stroke="rgba(16,185,129,0.18)" stroke-width="1" stroke-dasharray="5 5"/>
                            <line x1="0" y1="72" x2="600" y2="72" stroke="rgba(255,255,255,0.06)" stroke-width="1"/>
                            <path d="${areaPath}" fill="url(#qSigFill)"/>
                            <path d="${linePath}" fill="none" stroke="#10b981" stroke-width="3" vector-effect="non-scaling-stroke"/>
                            <circle cx="${coords[coords.length-1].split(',')[0]}" cy="${coords[coords.length-1].split(',')[1]}" r="5" fill="#10b981" stroke="#ffffff" stroke-width="2" vector-effect="non-scaling-stroke"/>
                        `;
                        if (qSignalNow) qSignalNow.innerText = `현재 ${lastScore.toFixed(0)}점`;
                    } else if (qSignalNow) {
                        qSignalNow.innerText = '현재 -';
                        if (qSignalChart) qSignalChart.innerHTML = '';
                    }
                    
                    const qFill = document.getElementById('qSliderFill');
                    if (qFill) qFill.style.width = q.slider_fill_pct + '%';
                    const qDot = document.getElementById('qDotMarker');
                    if (qDot) qDot.style.left = q.slider_fill_pct + '%';

                    const qList = document.getElementById('quantDetailList');
                    if (qList) {
                        qList.innerHTML = `
                            <div class="quant-row-item">
                                <div class="quant-row-left">
                                    <span class="quant-row-name">통계적 Z-Score</span>
                                    <span class="quant-row-status status-pill-${q.z_color}">${q.z_state}</span>
                                </div>
                                <div class="quant-row-right">
                                    <div class="quant-row-val">${q.z_score}</div>
                                    <div class="quant-row-subval">20D 표준점수</div>
                                </div>
                            </div>
                            <div class="quant-row-item">
                                <div class="quant-row-left">
                                    <span class="quant-row-name">20일 연환산 변동성</span>
                                    <span class="quant-row-status status-pill-${q.vol_risk_color}">${q.vol_risk_state}</span>
                                </div>
                                <div class="quant-row-right">
                                    <div class="quant-row-val">${q.ann_vol}</div>
                                    <div class="quant-row-subval">통계적 변동률</div>
                                </div>
                            </div>
                            <div class="quant-row-item">
                                <div class="quant-row-left">
                                    <span class="quant-row-name">5일 이동평균선</span>
                                    <span class="quant-row-status status-pill-${q.ma5_color}">${q.ma5_state}</span>
                                </div>
                                <div class="quant-row-right">
                                    <div class="quant-row-val">${q.ma5}</div>
                                    <div class="quant-row-subval">단기 모멘텀선</div>
                                </div>
                            </div>
                            <div class="quant-row-item">
                                <div class="quant-row-left">
                                    <span class="quant-row-name">20일 이동평균선</span>
                                    <span class="quant-row-status status-pill-${q.ma20_color}">${q.ma20_state}</span>
                                </div>
                                <div class="quant-row-right">
                                    <div class="quant-row-val">${q.ma20}</div>
                                    <div class="quant-row-subval">중기 추세선</div>
                                </div>
                            </div>
                            <div class="quant-row-item">
                                <div class="quant-row-left">
                                    <span class="quant-row-name">60일 이동평균선</span>
                                    <span class="quant-row-status status-pill-${q.ma60_color}">${q.ma60_state}</span>
                                </div>
                                <div class="quant-row-right">
                                    <div class="quant-row-val">${q.ma60}</div>
                                    <div class="quant-row-subval">중장기 수급선</div>
                                </div>
                            </div>
                            <div class="quant-row-item">
                                <div class="quant-row-left">
                                    <span class="quant-row-name">볼린저 밴드 (60, 2)</span>
                                    <span class="quant-row-status status-pill-${q.bb_color}">${q.bb_state}</span>
                                </div>
                                <div class="quant-row-right">
                                    <div class="quant-row-val">${q.bollinger}</div>
                                    <div class="quant-row-subval">상단 / 하단</div>
                                </div>
                            </div>
                            <div class="quant-row-item">
                                <div class="quant-row-left">
                                    <span class="quant-row-name">MACD (12, 26, 9)</span>
                                    <span class="quant-row-status status-pill-${q.macd_color}">${q.macd_state}</span>
                                </div>
                                <div class="quant-row-right">
                                    <div class="quant-row-val">${q.macd}</div>
                                    <div class="quant-row-subval">모멘텀 지표</div>
                                </div>
                            </div>
                            <div class="quant-row-item">
                                <div class="quant-row-left">
                                    <span class="quant-row-name">RSI (14)</span>
                                    <span class="quant-row-status status-pill-${q.rsi_color}">${q.rsi_state}</span>
                                </div>
                                <div class="quant-row-right">
                                    <div class="quant-row-val">${q.rsi} pt</div>
                                    <div class="quant-row-subval">상대강도지수</div>
                                </div>
                            </div>
                            <div class="quant-row-item">
                                <div class="quant-row-left">
                                    <span class="quant-row-name">거래량 / 5일평균</span>
                                    <span class="quant-row-status status-pill-${q.vol_color}">${q.vol_state}</span>
                                </div>
                                <div class="quant-row-right">
                                    <div class="quant-row-val">${q.vol_ratio}</div>
                                    <div class="quant-row-subval">유동성 비율</div>
                                </div>
                            </div>
                            <div class="quant-row-item">
                                <div class="quant-row-left">
                                    <span class="quant-row-name">20일 평균 거래대금</span>
                                    <span class="quant-row-status status-pill-neutral">시장 유동성</span>
                                </div>
                                <div class="quant-row-right">
                                    <div class="quant-row-val">${q.tval_20d}</div>
                                    <div class="quant-row-subval">실시간 집계</div>
                                </div>
                            </div>
                        `;
                    }

                    setTxt('rWinRateGauge', q.win_rate_num);
                    setTxt('rBt1yTag', q.bt_1y);
                    setTxt('rBt2yTag', q.bt_2y);
                    setTxt('rRrTag', q.rr_str);

                    setTxt('rSliderStop', q.stop_p);
                    setTxt('rSliderEntry', `기준 ${q.entry1}`);
                    setTxt('rSliderTarget', q.target2);

                    const sFill = document.getElementById('rSigBarFill');
                    if (sFill) sFill.style.width = q.sig_fill_pct + '%';
                    const sDot = document.getElementById('rSigDotMarker');
                    if (sDot) sDot.style.left = q.sig_fill_pct + '%';

                    setTxt('rEntry1', q.entry1);
                    setTxt('rEntry2', q.entry2);
                    setTxt('rTarget1', q.target1);
                    setTxt('rTarget2', q.target2);
                    setTxt('rStop', q.stop_p);
                    setTxt('rGuideText', q.guide_text);
                }

                const sNews = document.getElementById('stockNewsList');
                if (sNews) {
                    sNews.innerHTML = '';
                    if (data.stock_news && data.stock_news.length > 0) {
                        data.stock_news.forEach(n => {
                            sNews.innerHTML += `
                                <a href="${n.link}" target="_blank" class="news-item">
                                    <div class="news-title">${n.title}</div>
                                    <div class="news-meta">${n.media} · ${n.pubDate}</div>
                                </a>
                            `;
                        });
                    } else {
                        sNews.innerHTML = '<div style="color:var(--text-muted); font-size:0.85rem;">관련 뉴스가 없습니다.</div>';
                    }
                }

                const c = data.consensus;
                const uCard = document.getElementById('unifiedCard');
                if (c && uCard) {
                    setTxt('uTitle', c.title);
                    setTxt('uGaugeVal', c.gauge_val);
                    setTxt('uGaugeLabel', c.gauge_label);
                    setTxt('uStatusLabel', c.status_label);
                    setTxt('uBadge', c.badge_text);
                    setHtml('uDots', c.dots);

                    setTxt('uPrimaryLabel', c.primary_label);
                    setTxt('uPrimaryVal', c.primary_val);
                    setTxt('uSecondaryLabel', c.secondary_label);
                    setTxt('uSecondaryVal', c.secondary_val);

                    setTxt('uSliderLeft', c.slider_left);
                    setTxt('uSliderMid', c.slider_mid);
                    setTxt('uSliderRight', c.slider_right);
                    setTxt('uSliderBottomLeft', c.slider_bottom_left);
                    setTxt('uSliderBottomRight', c.slider_bottom_right);

                    const uFill = document.getElementById('uBarFill');
                    if (uFill) uFill.style.width = c.fill_pct + '%';
                    const uDot = document.getElementById('uDotMarker');
                    if (uDot) uDot.style.left = c.fill_pct + '%';

                    setTxt('uListTitle', c.list_title);
                    setTxt('uSourceTag', c.source_tag);

                    const iList = document.getElementById('uItemList');
                    if (iList) {
                        iList.innerHTML = '';
                        if (c.items && c.items.length > 0) {
                            c.items.forEach((it, idx) => {
                                const isLast = idx === c.items.length - 1;
                                iList.innerHTML += `
                                    <div style="display:flex; justify-content:space-between; align-items:center; padding:8px 0; ${isLast ? '' : 'border-bottom:1px solid var(--border-line);'} font-size:0.82rem;">
                                        <div>
                                            <div style="color:var(--text-title); font-weight:700;">${it.main}</div>
                                            <div style="font-size:0.7rem; color:var(--text-muted); margin-top:2px;">${it.sub}</div>
                                        </div>
                                        <div style="text-align:right;">
                                            <div style="color:var(--text-title); font-weight:700;">${it.val}</div>
                                            <div style="color:#10b981; font-size:0.72rem; font-weight:700;">${it.sub_val}</div>
                                        </div>
                                    </div>
                                `;
                            });
                        }
                    }
                    uCard.style.display = 'block';
                } else if (uCard) {
                    uCard.style.display = 'none';
                }

                const dCard = document.getElementById('dartCard');
                const dList = document.getElementById('dartList');
                if (dCard && dList) {
                    if (data.dart_disclosures && data.dart_disclosures.length > 0) {
                        dList.innerHTML = '';
                        data.dart_disclosures.forEach(d => {
                            dList.innerHTML += `
                                <a href="${d.link}" target="_blank" class="dart-item">
                                    <span class="dart-tag tag-${d.tag_type}">${d.tag_name}</span>
                                    <span class="dart-title">${d.title}</span>
                                    <span style="color:var(--text-muted); font-size:0.72rem; flex-shrink:0;">${d.date}</span>
                                </a>
                            `;
                        });
                        dCard.style.display = 'block';
                    } else {
                        dCard.style.display = 'none';
                    }
                }

                const gNews = document.getElementById('globalNewsList');
                if (gNews) {
                    gNews.innerHTML = '';
                    if (data.global_news && data.global_news.length > 0) {
                        data.global_news.forEach(n => {
                            gNews.innerHTML += `
                                <a href="${n.link}" target="_blank" class="news-item">
                                    <div class="news-title">${n.title}</div>
                                    <div class="news-meta">${n.media} · ${n.pubDate}</div>
                                </a>
                            `;
                        });
                    } else {
                        gNews.innerHTML = '<div style="color:var(--text-muted); font-size:0.85rem;">해외 증시 뉴스를 불러오는 중입니다.</div>';
                    }
                }

                document.getElementById('loading').style.display = 'none';
                document.getElementById('result-card-wrap').style.display = 'block';

                setTimeout(() => {
                    initTvChart(data.candles, data.ma20_line, data.ma60_line, data.bb_up_line, data.bb_dn_line);
                }, 50);

                window.scrollTo({top: 0, behavior: 'smooth'});

            } catch (err) {
                console.error("Analysis Render Error:", err);
                alert('데이터를 처리하는 중 오류가 발생했습니다: ' + err.message);
                document.getElementById('loading').style.display = 'none';
            }
        }

        function initTvChart(candles, ma20, ma60, bbUp, bbDn) {
            const container = document.getElementById('tvChart');
            if (!container) return;
            container.innerHTML = '';

            const isLight = document.body.classList.contains('light-theme');

            chart = LightweightCharts.createChart(container, {
                width: container.clientWidth || 700,
                height: 300,
                layout: { 
                    background: { color: isLight ? '#ffffff' : '#1e1f20' }, 
                    textColor: isLight ? '#5f6368' : '#8e918f' 
                },
                grid: {
                    vertLines: { color: 'rgba(0,0,0,0)' },
                    horzLines: { color: 'rgba(0,0,0,0)' }
                },
                crosshair: {
                    vertLine: { visible: false },
                    horzLine: { visible: false }
                },
                timeScale: { borderColor: isLight ? '#dadce0' : '#282a2c', timeVisible: true },
                rightPriceScale: { borderColor: isLight ? '#dadce0' : '#282a2c' }
            });

            const candleSeries = chart.addCandlestickSeries({
                upColor: '#ef4444',
                downColor: '#38bdf8',
                borderDownColor: '#38bdf8',
                borderUpColor: '#ef4444',
                wickDownColor: '#38bdf8',
                wickUpColor: '#ef4444',
                priceLineVisible: false,
                lastValueVisible: true
            });
            candleSeries.setData(candles || []);

            const lineMa20 = chart.addLineSeries({ color: '#00b4d8', lineWidth: 1, priceLineVisible: false, lastValueVisible: false });
            lineMa20.setData(ma20 || []);

            const lineMa60 = chart.addLineSeries({ color: '#f97316', lineWidth: 1.5, priceLineVisible: false, lastValueVisible: false });
            lineMa60.setData(ma60 || []);

            const lineBbUp = chart.addLineSeries({ color: '#f87171', lineWidth: 1.2, priceLineVisible: false, lastValueVisible: false });
            lineBbUp.setData(bbUp || []);

            const lineBbDn = chart.addLineSeries({ color: '#38bdf8', lineWidth: 1.2, priceLineVisible: false, lastValueVisible: false });
            lineBbDn.setData(bbDn || []);

            chart.timeScale().fitContent();

            window.addEventListener('resize', resizeChart);
        }

        window.addEventListener('DOMContentLoaded', initTheme);
    </script>
</body>
</html>
"""

# ----------------------------------------------------
# 8. 멀티스레드 웹 서버
# ----------------------------------------------------
# ----------------------------------------------------
# 8. 멀티스레드 웹 서버
# ----------------------------------------------------
class WebHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        # Render logs stay readable.
        print("WEB", fmt % args)

    def _json(self, status, payload):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)

        if parsed.path == "/health":
            self._json(200, {"ok": True, "gemini": False, "telegram_configured": bool(TELEGRAM_TOKEN)})
            return

        if parsed.path == "/api/analyze":
            q = str(params.get("q", [""])[0]).strip()
            if not q:
                self._json(400, {"error": "종목명을 입력해주세요."})
                return
            try:
                data = run_full_pipeline(q)
                if not data:
                    self._json(404, {"error": "실제 데이터를 가져오지 못했습니다. 종목명 또는 데이터 제공처를 확인해주세요."})
                    return
                self._json(200, data)
            except Exception as exc:
                # Never return HTML to the frontend. This prevents Unexpected token '<'.
                print("API ERROR:", repr(exc))
                self._json(500, {"error": "서버 처리 중 오류가 발생했습니다.", "detail": str(exc)[:500]})
            return

        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(HTML_PAGE.encode("utf-8"))


def run_web_server():
    port = int(os.environ.get("PORT", "10000"))
    server = ThreadingHTTPServer(("0.0.0.0", port), WebHandler)
    print(f"gaemiGTP listening on :{port}")
    server.serve_forever()


if __name__ == "__main__":
    run_web_server()
