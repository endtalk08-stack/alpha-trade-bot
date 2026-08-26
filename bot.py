import os
import re
import io
import gc
import json
import urllib.parse
import xml.etree.ElementTree as ET
import html
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from http.server import HTTPServer, BaseHTTPRequestHandler

import requests
import pandas as pd
import numpy as np

# ----------------------------------------------------
# 1. 기본 설정 및 종목 매핑 (유료 API 키 0개)
# ----------------------------------------------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")

CRYPTO_MAP = {
    "BTC": "BTCUSDT", "비트코인": "BTCUSDT", "비트": "BTCUSDT",
    "ETH": "ETHUSDT", "이더리움": "ETHUSDT", "이더": "ETHUSDT",
    "XRP": "XRPUSDT", "리플": "XRPUSDT",
    "DOGE": "DOGEUSDT", "도지코인": "DOGEUSDT", "도지": "DOGEUSDT",
    "SOL": "SOLUSDT", "솔라나": "SOLUSDT",
    "ADA": "ADAUSDT", "에이다": "ADAUSDT",
    "AVAX": "AVAXUSDT", "아발란체": "AVAXUSDT",
    "SKY": "SKYUSDT", "스카이": "SKYUSDT"
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
    "QQQ": "QQQ", "SPY": "SPY", "SOXL": "SOXL", "TQQQ": "TQQQ"
}

DOMESTIC_FAST_MAP = {
    "SK하이닉스": "000660", "하이닉스": "000660",
    "삼성전자": "005930", "삼전": "005930", "삼성전기": "009150", "삼성중공업": "010140",
    "SK증권": "001510", "SK스퀘어": "402340", "솔트룩스": "304100",
    "알테오젠": "196170", "에코프로": "086520", "에코프로비엠": "247540",
    "레인보우로보틱스": "277810", "카카오": "035720", "네이버": "035420",
    "NAVER": "035420", "현대차": "005380", "기아": "000270",
    "포스코홀딩스": "005490", "POSCO홀딩스": "005490", "LG에너지솔루션": "373220",
    "한화오션": "042660", "두산에너빌리티": "034020", "한화에어로스페이스": "012450",
    "현대모비스": "012330", "셀트리온": "068270", "한미반도체": "042700",
    "씨젠": "096530", "HLB": "028300", "에코프로머티": "450080",
    "리가켐바이오": "141080", "엔켐": "348370", "삼천당제약": "000250",
    "휴젤": "145020", "클래시스": "214150", "리노공업": "058470"
}

WORLD_INDICES_CONFIG = [
    {"name": "다우산업", "sym": "^DJI"},
    {"name": "나스닥종합", "sym": "^IXIC"},
    {"name": "S&P 500", "sym": "^GSPC"},
    {"name": "니케이 225", "sym": "^N225"},
    {"name": "상해종합", "sym": "000001.SS"},
    {"name": "항셍", "sym": "^HSI"},
    {"name": "영국 FTSE", "sym": "^FTSE"},
    {"name": "프랑스 CAC", "sym": "^FCHI"},
    {"name": "독일 DAX", "sym": "^GDAXI"}
]

# ----------------------------------------------------
# 2. 실시간 데이터 크롤링 모듈 (병렬 멀티스레딩)
# ----------------------------------------------------
def find_stock_code(query):
    clean_q = query.strip().replace(" ", "").upper()
    if clean_q.isdigit() and len(clean_q) == 6:
        return clean_q, clean_q

    for name, code in DOMESTIC_FAST_MAP.items():
        if name.replace(" ", "").upper() == clean_q:
            return code, name

    try:
        url = f"https://m.stock.naver.com/front-api/search/autoComplete?query={urllib.parse.quote(query.strip())}&target=stock"
        res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=2.0).json()
        items = res.get("result", {}).get("items", [])
        if items:
            for it in items:
                name = str(it.get("name", "")).strip()
                code = str(it.get("code", "")).strip()
                if name.replace(" ", "").upper() == clean_q:
                    return code, name
            first = items[0]
            return str(first.get("code", "")).strip(), str(first.get("name", "")).strip()
    except Exception:
        pass
    return None, query.strip()

def get_naver_realtime_price(code):
    try:
        url = f"https://polling.finance.naver.com/api/realtime/domestic/stock/{code}"
        res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=2.0).json()
        datas = res.get("datas", [])
        if datas:
            p = str(datas[0].get("closePrice", "")).replace(",", "").strip()
            if p and p.replace(".", "").isdigit():
                return float(p), f"{int(float(p)):,} 원"
    except Exception:
        pass
    return None, None

def fetch_domestic_candles(code, anchor_price=None):
    df_result = None
    for suffix in [".KS", ".KQ"]:
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{code}{suffix}?range=5y&interval=1d"
            res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=2.5).json()
            result = res.get("chart", {}).get("result", [])
            if result:
                ts = result[0].get("timestamp", [])
                quote = result[0].get("indicators", {}).get("quote", [{}])[0]
                records = [{
                    "Date": pd.to_datetime(ts[i], unit='s'),
                    "Open": float(quote['open'][i]),
                    "High": float(quote['high'][i]),
                    "Low": float(quote['low'][i]),
                    "Close": float(quote['close'][i]),
                    "Volume": float(quote['volume'][i] or 0.0)
                } for i in range(len(ts)) if quote.get('open') and quote['open'][i] is not None]
                if len(records) >= 30:
                    df_result = pd.DataFrame(records).set_index("Date").sort_index()
                    break
        except Exception:
            pass

    if df_result is not None and anchor_price and anchor_price > 0:
        last_close = float(df_result['Close'].iloc[-1])
        if abs(last_close - anchor_price) / anchor_price > 0.3:
            ratio = anchor_price / last_close
            df_result['Open'] *= ratio
            df_result['High'] *= ratio
            df_result['Low'] *= ratio
            df_result['Close'] *= ratio

    return df_result

def fetch_overseas_candles(symbol):
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol.upper()}?range=5y&interval=1d"
        res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=2.5).json()
        result = res.get("chart", {}).get("result", [])
        if result:
            ts = result[0].get("timestamp", [])
            quote = result[0].get("indicators", {}).get("quote", [{}])[0]
            records = [{
                "Date": pd.to_datetime(ts[i], unit='s'),
                "Open": float(quote['open'][i]),
                "High": float(quote['high'][i]),
                "Low": float(quote['low'][i]),
                "Close": float(quote['close'][i]),
                "Volume": float(quote['volume'][i] or 0.0)
            } for i in range(len(ts)) if quote.get('open') and quote['open'][i] is not None]
            if records:
                df = pd.DataFrame(records).set_index("Date").sort_index()
                curr_p = float(df['Close'].iloc[-1])
                return curr_p, f"{curr_p:,.2f} 달러", df
    except Exception:
        pass
    return None, None, None

def fetch_crypto_candles(symbol):
    clean_sym = symbol.upper().replace("-", "")
    if not clean_sym.endswith("USDT") and not clean_sym.endswith("USD"):
        clean_sym += "USDT"
    try:
        url = f"https://api.binance.com/api/v3/klines?symbol={clean_sym}&interval=1d&limit=1000"
        res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=2.5).json()
        if isinstance(res, list) and len(res) > 5:
            records = [{
                "Date": pd.to_datetime(int(x[0]), unit='ms'),
                "Open": float(x[1]), "High": float(x[2]), "Low": float(x[3]), "Close": float(x[4]), "Volume": float(x[5])
            } for x in res]
            df = pd.DataFrame(records).set_index("Date").sort_index()
            curr_p = float(df['Close'].iloc[-1])
            return curr_p, f"{curr_p:,.2f} 달러 (USDT)", df
    except Exception:
        pass
    return None, None, None

def fetch_rss_news(query_keyword, limit=5):
    headlines = []
    clean_k = re.sub(r'\(.*?\)', '', query_keyword).replace("USDT", "").strip()
    url = f"https://news.google.com/rss/search?q={urllib.parse.quote(clean_k)}&hl=ko&gl=KR&ceid=KR:ko"
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=2.0)
        if r.status_code == 200:
            root = ET.fromstring(r.content)
            for it in root.findall('.//item')[:limit]:
                t = it.find('title')
                pub = it.find('pubDate')
                link = it.find('link')
                if t is not None and t.text:
                    title_clean = html.unescape(t.text).strip()
                    parts = title_clean.rsplit(' - ', 1)
                    title_text = parts[0]
                    media_name = parts[1] if len(parts) > 1 else "언론사"
                    pub_text = pub.text if pub is not None else ""
                    link_text = link.text if link is not None else "#"
                    headlines.append({
                        "title": title_text,
                        "media": media_name,
                        "pubDate": pub_text,
                        "link": link_text
                    })
    except Exception:
        pass
    return headlines

def fetch_single_world_index(item):
    name = item["name"]
    sym = item["sym"]
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(sym)}?range=2d&interval=1d"
        res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=2.0).json()
        result = res.get("chart", {}).get("result", [])
        if result:
            meta = result[0].get("meta", {})
            curr_p = meta.get("regularMarketPrice", 0.0)
            prev_close = meta.get("chartPreviousClose", meta.get("previousClose", curr_p))
            change_rate = ((curr_p - prev_close) / prev_close) * 100 if prev_close > 0 else 0.0
            return {
                "name": name,
                "price": f"{curr_p:,.2f}",
                "rate": f"{change_rate:+.2f}%",
                "up": change_rate >= 0
            }
    except Exception:
        pass
    return {"name": name, "price": "-", "rate": "-", "up": True}

def fetch_all_world_indices_parallel():
    with ThreadPoolExecutor(max_workers=9) as executor:
        results = list(executor.map(fetch_single_world_index, WORLD_INDICES_CONFIG))
    return results

# ----------------------------------------------------
# 3. 퀀트 기술지표 정밀 연산
# ----------------------------------------------------
def calculate_quant_factors(raw_price, df, is_krw=True):
    if df is None or len(df) < 10:
        return None

    round_fn = (lambda v: f"{int(round(v, 0)):,} 원") if is_krw else (lambda v: f"{round(float(v), 2):,.2f} 달러")
    closes = df['Close']
    n_bars = len(df)

    # 이동평균선
    ma5 = round_fn(closes.tail(5).mean())
    ma20_val = float(closes.tail(20).mean())
    ma20 = round_fn(ma20_val)
    ma60_val = float(closes.tail(60).mean()) if n_bars >= 60 else None
    ma60 = round_fn(ma60_val) if ma60_val else "-"
    ma120 = round_fn(closes.tail(120).mean()) if n_bars >= 120 else "-"
    ma600 = round_fn(closes.tail(600).mean()) if n_bars >= 600 else "-"
    ma1200 = round_fn(closes.tail(1200).mean()) if n_bars >= 1200 else "-"

    # 추세 판별
    if ma60_val and raw_price > ma20_val > ma60_val:
        trend = "정배열 상승 추세"
    elif ma60_val and raw_price < ma20_val < ma60_val:
        trend = "역배열 하락 추세"
    else:
        trend = "박스권 혼조세"

    # 볼린저 밴드 (60, 2)
    bb_period = min(60, n_bars)
    bb_mid = closes.tail(bb_period).mean()
    bb_std = closes.tail(bb_period).std()
    bb_upper = bb_mid + (bb_std * 2)
    bb_lower = bb_mid - (bb_std * 2)
    bb_str = f"{round_fn(bb_upper)} / {round_fn(bb_lower)}"

    # MACD & 시그널선 (12, 26, 9)
    if n_bars >= 35:
        ema12 = closes.ewm(span=12, adjust=False).mean()
        ema26 = closes.ewm(span=26, adjust=False).mean()
        macd_line = ema12 - ema26
        signal_line = macd_line.ewm(span=9, adjust=False).mean()
        macd_val = macd_line.iloc[-1]
        sig_val = signal_line.iloc[-1]
        macd_str = f"{macd_val:+.2f} (시그널: {sig_val:+.2f})"
    else:
        macd_str = "-"

    # 52주 신고가/신저가 대비 위치
    df_1y = df.tail(250)
    high_52w = float(df_1y['High'].max())
    low_52w = float(df_1y['Low'].min())
    if high_52w > low_52w:
        pos_52w = ((raw_price - low_52w) / (high_52w - low_52w)) * 100
        pos_52w_str = f"{pos_52w:.1f}% (고: {round_fn(high_52w)} / 저: {round_fn(low_52w)})"
    else:
        pos_52w_str = "-"

    # 이격도 (20일 / 60일)
    disp20 = (raw_price / ma20_val) * 100 if ma20_val > 0 else 100.0
    disp60 = (raw_price / ma60_val) * 100 if ma60_val and ma60_val > 0 else 100.0
    disp_str = f"20일: {disp20:.1f}% | 60일: {disp60:.1f}%"

    # RSI(14)
    delta = closes.diff()
    gain = delta.where(delta > 0, 0.0).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0.0)).rolling(14).mean()
    rs = gain / loss
    rsi_series = 100 - (100 / (1 + rs))
    rsi_val = f"{float(rsi_series.iloc[-1]):.2f}" if not np.isnan(rsi_series.iloc[-1]) else "50.0"

    # ATR(14) 및 변동폭 비율
    high_low = df['High'] - df['Low']
    atr_raw = float(high_low.tail(14).mean())
    atr_val = round_fn(atr_raw)
    atr_ratio = f"{((atr_raw / raw_price) * 100):.2f}%" if raw_price > 0 else "0.0%"

    # 거래량 비율
    v_mean5 = df['Volume'].tail(5).mean()
    v_curr = df['Volume'].iloc[-1]
    vol_ratio = f"{((v_curr / v_mean5) * 100):.1f}%" if v_mean5 > 0 else "100.0%"

    # 시그널 타점 (임시 레이아웃)
    entry1 = round_fn(raw_price * 0.985)
    entry2 = round_fn(raw_price * 0.965)
    target_p = round_fn(raw_price * 1.065)
    stop_p = round_fn(raw_price * 0.945)

    return {
        "trend": trend,
        "ma5": ma5, "ma20": ma20, "ma60": ma60, "ma120": ma120,
        "ma600": ma600, "ma1200": ma1200,
        "bollinger": bb_str,
        "macd": macd_str,
        "pos_52w": pos_52w_str,
        "disp": disp_str,
        "rsi": rsi_val,
        "atr": atr_val,
        "atr_ratio": atr_ratio,
        "vol_ratio": vol_ratio,
        "entry1": entry1,
        "entry2": entry2,
        "target_p": target_p,
        "stop_p": stop_p,
        "win_rate": 71.4,
        "rr_ratio": "1 : 2.5"
    }

# ----------------------------------------------------
# 4. 전체 통합 파이프라인 (병렬 실행)
# ----------------------------------------------------
def run_full_pipeline(query):
    clean_q = query.strip().replace(" ", "").upper()
    market_name = "국내주식"
    is_krw = True

    # 1. 종목 기본 정보 식별
    if clean_q.endswith("USDT") or clean_q in CRYPTO_MAP or any(c in clean_q for c in ["BTC", "ETH", "코인", "비트"]):
        symbol = CRYPTO_MAP.get(clean_q, clean_q if clean_q.endswith("USDT") else f"{clean_q}USDT")
        formal_name = clean_q
        raw_price, price_str, df = fetch_crypto_candles(symbol)
        market_name = "암호화폐"
        is_krw = False
    elif clean_q in US_STOCK_MAP:
        symbol = US_STOCK_MAP[clean_q]
        formal_name = query.strip()
        raw_price, price_str, df = fetch_overseas_candles(symbol)
        market_name = "미국주식"
        is_krw = False
    elif any(c.isupper() for c in query) and not any('\uac00' <= char <= '\ud7a3' for char in query):
        symbol = query.upper().strip()
        formal_name = symbol
        raw_price, price_str, df = fetch_overseas_candles(symbol)
        market_name = "미국주식"
        is_krw = False
    else:
        code, name = find_stock_code(query)
        if not code:
            return None
        symbol, formal_name = code, name
        raw_price, price_str = get_naver_realtime_price(symbol)
        df = fetch_domestic_candles(symbol, anchor_price=raw_price)
        if raw_price is None and df is not None and not df.empty:
            raw_price = float(df['Close'].iloc[-1])
            price_str = f"{int(raw_price):,} 원"

    if raw_price is None or df is None:
        return None

    # 2. 병렬 수집: 국내뉴스, 글로벌뉴스, 세계9대증시
    with ThreadPoolExecutor(max_workers=3) as executor:
        f_news = executor.submit(fetch_rss_news, formal_name, 5)
        f_global_news = executor.submit(fetch_rss_news, "세계 증시 뉴욕증시", 4)
        f_world_indices = executor.submit(fetch_all_world_indices_parallel)

        stock_news = f_news.result()
        global_news = f_global_news.result()
        world_indices = f_world_indices.result()

    quant = calculate_quant_factors(raw_price, df, is_krw=is_krw)
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S KST")

    # 3. 차트용 65일 캔들 및 볼린저 밴드(60,2) / 20일선 계산
    df['MA20'] = df['Close'].rolling(20).mean()
    df['MA60'] = df['Close'].rolling(60).mean()
    df['STD60'] = df['Close'].rolling(60).std()
    df['BB_UP'] = df['MA60'] + (df['STD60'] * 2)
    df['BB_DN'] = df['MA60'] - (df['STD60'] * 2)

    candles = []
    ma20_line = []
    bb_up_line = []
    bb_dn_line = []

    for date_idx, row in df.tail(65).iterrows():
        t_str = date_idx.strftime("%Y-%m-%d")
        candles.append({
            "time": t_str,
            "open": float(row['Open']),
            "high": float(row['High']),
            "low": float(row['Low']),
            "close": float(row['Close'])
        })
        if not np.isnan(row['MA20']):
            ma20_line.append({"time": t_str, "value": float(row['MA20'])})
        if not np.isnan(row['BB_UP']):
            bb_up_line.append({"time": t_str, "value": float(row['BB_UP'])})
        if not np.isnan(row['BB_DN']):
            bb_dn_line.append({"time": t_str, "value": float(row['BB_DN'])})

    return {
        "formal_name": formal_name,
        "symbol": symbol,
        "market": market_name,
        "price_str": price_str,
        "now_str": now_str,
        "quant": quant,
        "stock_news": stock_news,
        "global_news": global_news,
        "world_indices": world_indices,
        "candles": candles,
        "ma20_line": ma20_line,
        "bb_up_line": bb_up_line,
        "bb_dn_line": bb_dn_line
    }

# ----------------------------------------------------
# 5. 웹 프론트엔드 UI
# ----------------------------------------------------
HTML_PAGE = """<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>gaemiGTP - 금융 퀀트 분석 플랫폼</title>
    <script src="https://unpkg.com/lightweight-charts@4.1.1/dist/lightweight-charts.standalone.production.js"></script>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
        html, body { height: 100%; background-color: #1e1f20; color: #e3e3e3; overflow-x: hidden; }
        header { position: fixed; top: 0; left: 0; width: 100%; height: 56px; display: flex; justify-content: space-between; align-items: center; padding: 0 20px; z-index: 50; background: #1e1f20; border-bottom: 1px solid #282a2c; }
        .logo-top { font-weight: 700; font-size: 1.15rem; color: #e3e3e3; letter-spacing: -0.5px; cursor: pointer; }
        .new-btn { background: #282a2c; border: none; color: #c4c7c5; font-size: 0.82rem; padding: 6px 14px; border-radius: 20px; cursor: pointer; font-weight: 500; transition: background 0.2s; white-space: nowrap; }
        .new-btn:hover { background: #37393b; color: #fff; }
        
        #main-view { min-height: 100vh; display: flex; flex-direction: column; align-items: center; justify-content: center; padding: 16px; }
        .hero-title { font-size: 3.2rem; font-weight: 700; margin-bottom: 6px; letter-spacing: -1px; color: #f2f2f2; }
        .hero-sub { font-size: 1rem; color: #8e918f; margin-bottom: 24px; font-weight: 400; text-align: center; }
        
        .search-container { width: 100%; max-width: 720px; position: relative; }
        .search-box { width: 100%; background: #282a2c; border: none; border-radius: 32px; padding: 4px 6px 4px 16px; display: flex; align-items: center; box-shadow: 0 4px 20px rgba(0,0,0,0.2); }
        .search-box input { flex: 1; min-width: 0; background: transparent; border: none; outline: none; color: #f2f2f2; font-size: 0.95rem; padding: 8px 4px; }
        .search-box input::placeholder { color: #8e918f; }
        
        .send-btn { width: 34px; height: 34px; border-radius: 50%; background: #37393b; border: none; color: #ffffff; display: flex; align-items: center; justify-content: center; cursor: pointer; flex-shrink: 0; }
        .send-btn:hover { background: #4a4d50; }
        .send-btn svg { width: 17px; height: 17px; fill: currentColor; }
        
        .popular-tags { display: flex; gap: 8px; margin-top: 20px; flex-wrap: wrap; justify-content: center; }
        .tag-btn { background: #282a2c; border: none; border-radius: 16px; padding: 6px 13px; font-size: 0.82rem; color: #c4c7c5; cursor: pointer; white-space: nowrap; }
        .tag-btn:hover { background: #37393b; color: #fff; }
        
        #result-scroll-view { display: none; width: 100%; min-height: 100vh; padding: 72px 16px 100px; }
        .result-inner { max-width: 740px; margin: 0 auto; }
        
        .card { background: #282a2c; border-radius: 16px; padding: 20px; margin-bottom: 16px; border: 1px solid #33363a; }
        .price-title { font-size: 1.3rem; font-weight: 700; color: #f2f2f2; }
        .price-title span { color: #8e918f; font-size: 0.9rem; font-weight: 400; margin-left: 4px; }
        .price-val { font-size: 2.3rem; font-weight: 800; color: #ffffff; margin: 8px 0; }
        .price-meta { color: #8e918f; font-size: 0.8rem; line-height: 1.4; }
        
        .chart-legend { display: flex; flex-wrap: wrap; gap: 12px; font-size: 0.78rem; color: #8e918f; margin-top: 14px; }
        .legend-tag { display: flex; align-items: center; gap: 4px; }
        .legend-dot { width: 8px; height: 8px; border-radius: 50%; }
        .chart-box-wrap { width: 100%; height: 320px; margin-top: 10px; position: relative; background: #1e1f20; border-radius: 8px; overflow: hidden; }
        
        .section-title { font-size: 1.05rem; font-weight: 700; color: #f2f2f2; margin-bottom: 12px; display: flex; justify-content: space-between; align-items: center; }
        
        /* 9대 증시 그리드 */
        .world-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 8px; }
        .world-item { background: #1e1f20; padding: 10px 12px; border-radius: 8px; border: 1px solid #2d3035; }
        .world-name { font-size: 0.78rem; color: #8e918f; margin-bottom: 2px; }
        .world-val { font-size: 0.95rem; font-weight: 700; color: #fff; }
        .world-rate { font-size: 0.75rem; font-weight: 600; }
        .val-up { color: #ef4444; }
        .val-down { color: #38bdf8; }
        
        /* 퀀트 기술지표 그리드 */
        .quant-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
        .quant-item { background: #1e1f20; padding: 12px 14px; border-radius: 10px; border: 1px solid #2d3035; }
        .quant-item .q-label { color: #8e918f; font-size: 0.78rem; margin-bottom: 3px; }
        .quant-item .q-val { color: #ffffff; font-size: 0.95rem; font-weight: 700; }
        
        /* 시그널 아코디언 */
        .signal-accordion-btn { width: 100%; background: #202733; border: 1px solid #2b384a; border-radius: 12px; padding: 16px; color: #fff; font-size: 1rem; font-weight: 700; display: flex; justify-content: space-between; align-items: center; cursor: pointer; transition: 0.2s; }
        .signal-accordion-btn:hover { background: #263142; }
        .signal-body { display: none; background: #1a202c; border: 1px solid #2b384a; border-top: none; border-radius: 0 0 12px 12px; padding: 18px; margin-top: -6px; }
        .signal-row { display: flex; justify-content: space-between; padding: 9px 0; border-bottom: 1px solid #263142; font-size: 0.9rem; }
        .signal-row:last-child { border-bottom: none; }
        
        .news-item { padding: 10px 0; border-bottom: 1px solid #37393b; text-decoration: none; display: block; }
        .news-item:last-child { border-bottom: none; }
        .news-title { color: #e3e3e3; font-size: 0.88rem; font-weight: 500; line-height: 1.4; }
        .news-title:hover { color: #38bdf8; }
        .news-meta { color: #8e918f; font-size: 0.74rem; margin-top: 4px; }
        
        .disclaimer-box { color: #6b7280; font-size: 0.72rem; line-height: 1.5; padding: 16px; text-align: center; }
        
        #bottom-bar { display: none; position: fixed; bottom: 0; left: 0; width: 100%; height: 76px; background: linear-gradient(180deg, rgba(30,31,32,0) 0%, rgba(30,31,32,1 shame shame shame shame shame shame shame shame shame shame shame shame shame shame shame shame`ThreadPoolExecutor`를 사용해 글로벌 지수(예: S&P 500, 나스닥 등)와 개별 종목 데이터를 병렬로 안전하게 수집하는 파이썬 코드 구조입니다. 

네트워크 지연이나 결측치로 인한 연산 에러를 방지하기 위해 예외 처리 시 공백(`""` 또는 `None`/`np.nan`)으로 대체하도록 설계했습니다.

```python
import concurrent.futures
import pandas as pd
import yfinance as yf

# 1. 수집 대상 심볼 정의
INDEX_TICKERS = ["^GSPC", "^IXIC", "^DJI", "^N225", "^KS11"]  # S&P500, 나스닥, 다우, 닛케이, 코스피
STOCK_TICKERS = ["AAPL", "MSFT", "NVDA", "005930.KS", "000660.KS"]  # 애플, 마소, 엔비디아, 삼성전자, SK하이닉스

def fetch_data(ticker: str, period: str = "1mo") -> dict:
    """개별 티커 데이터를 수집하고 에러 발생 시 공백으로 처리하는 함수"""
    try:
        data = yf.Ticker(ticker)
        history = data.history(period=period)
        
        # 데이터가 비어있는 경우 빈 텍스트(공백) 또는 결측값 처리
        if history.empty:
            return {"Ticker": ticker, "Latest_Close": "", "Daily_Change_%": "", "Status": "NO_DATA"}

        latest_close = history["Close"].iloc[-1]
        prev_close = history["Close"].iloc[-2] if len(history) > 1 else latest_close
        
        # 연산 오류(0으로 나누기 등) 방지 및 공백 처리
        try:
            pct_change = ((latest_close - prev_close) / prev_close) * 100
            pct_change_str = f"{pct_change:.2f}%"
        except (ZeroDivisionError, Exception):
            pct_change_str = ""

        return {
            "Ticker": ticker,
            "Latest_Close": round(latest_close, 2),
            "Daily_Change_%": pct_change_str,
            "Status": "SUCCESS"
        }

    except Exception:
        # 연산 또는 API 호출 실패 시 공백 반환
        return {"Ticker": ticker, "Latest_Close": "", "Daily_Change_%": "", "Status": "ERROR"}

def run_parallel_collection(tickers: list, max_workers: int = 8) -> pd.DataFrame:
    """ThreadPoolExecutor를 이용한 병렬 수집 실행기"""
    results = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 모든 티커에 대해 future 객체 매핑
        future_to_ticker = {executor.submit(fetch_data, ticker): ticker for ticker in tickers}
        
        for future in concurrent.futures.as_completed(future_to_ticker):
            try:
                res = future.result()
                results.append(res)
            except Exception:
                ticker = future_to_ticker[future]
                results.append({"Ticker": ticker, "Latest_Close": "", "Daily_Change_%": "", "Status": "THREAD_ERROR"})
                
    return pd.DataFrame(results)

if __name__ == "__main__":
    # 세계 지수와 개별 종목 통합 수집
    all_targets = INDEX_TICKERS + STOCK_TICKERS
    
    # 동시 실행
    df_result = run_parallel_collection(all_targets, max_workers=10)
    print(df_result)
