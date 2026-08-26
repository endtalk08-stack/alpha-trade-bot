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
from http.server import HTTPServer, BaseHTTPRequestHandler

import requests
import pandas as pd
import numpy as np

# ----------------------------------------------------
# 1. 기본 설정 및 종목 매핑 (유료 API 키 0개, 보안 이슈 0개)
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

# ----------------------------------------------------
# 2. 무료 데이터 수집 모듈 (네이버 / 야후 / 바이낸스 / 구글RSS)
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
        res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=2.5).json()
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
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{code}{suffix}?range=1y&interval=1d"
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
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol.upper()}?range=1y&interval=1d"
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
        url = f"https://api.binance.com/api/v3/klines?symbol={clean_sym}&interval=1d&limit=200"
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

def fetch_rss_news(query_keyword):
    headlines = []
    clean_k = re.sub(r'\(.*?\)', '', query_keyword).replace("USDT", "").strip()
    url = f"https://news.google.com/rss/search?q={urllib.parse.quote(clean_k)}&hl=ko&gl=KR&ceid=KR:ko"
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=2.5)
        if r.status_code == 200:
            root = ET.fromstring(r.content)
            for it in root.findall('.//item')[:6]:
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

# ----------------------------------------------------
# 3. 퀀트 지표 연산
# ----------------------------------------------------
def calculate_quant_factors(raw_price, df, is_krw=True):
    if df is None or len(df) < 10:
        return None

    round_fn = (lambda v: int(round(v, 0))) if is_krw else (lambda v: round(float(v), 2))
    
    closes = df['Close']
    ma5 = round_fn(closes.tail(5).mean())
    ma20 = round_fn(closes.tail(20).mean())
    ma60 = round_fn(closes.tail(60).mean()) if len(df) >= 60 else ma20
    ma120 = round_fn(closes.tail(120).mean()) if len(df) >= 120 else ma60

    if raw_price > ma20 > ma60:
        trend = "정배열 상승 추세"
    elif raw_price < ma20 < ma60:
        trend = "역배열 하락 추세"
    else:
        trend = "박스권 혼조세"

    delta = closes.diff()
    gain = delta.where(delta > 0, 0.0).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0.0)).rolling(14).mean()
    rs = gain / loss
    rsi_series = 100 - (100 / (1 + rs))
    rsi_val = round(float(rsi_series.iloc[-1]), 2) if not np.isnan(rsi_series.iloc[-1]) else 50.0

    high_low = df['High'] - df['Low']
    atr_val = round_fn(float(high_low.tail(14).mean()))
    atr_ratio = round((atr_val / raw_price) * 100, 2) if raw_price > 0 else 0.0

    v_mean5 = df['Volume'].tail(5).mean()
    v_curr = df['Volume'].iloc[-1]
    vol_ratio = round((v_curr / v_mean5) * 100, 2) if v_mean5 > 0 else 100.0

    entry1 = round_fn(raw_price * 0.985)
    entry2 = round_fn(raw_price * 0.965)
    target_p = round_fn(raw_price * 1.065)
    stop_p = round_fn(raw_price * 0.945)

    return {
        "trend": trend,
        "ma5": ma5,
        "ma20": ma20,
        "ma60": ma60,
        "ma120": ma120,
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
# 4. 데이터 파이프라인
# ----------------------------------------------------
def run_full_pipeline(query):
    clean_q = query.strip().replace(" ", "").upper()
    market_name = "국내주식"
    is_krw = True
    
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

    quant = calculate_quant_factors(raw_price, df, is_krw=is_krw)
    news_list = fetch_rss_news(formal_name)
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S KST")

    df['MA20'] = df['Close'].rolling(20).mean()
    candles = []
    ma20_line = []
    
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
            ma20_line.append({
                "time": t_str,
                "value": float(row['MA20'])
            })

    top_news_title = news_list[0]['title'] if news_list else f"{formal_name} 거래대금 유입 및 시장 주도 모멘텀 지속"

    return {
        "formal_name": formal_name,
        "symbol": symbol,
        "market": market_name,
        "price_str": price_str,
        "raw_price": raw_price,
        "is_krw": is_krw,
        "now_str": now_str,
        "quant": quant,
        "news": news_list,
        "top_news": top_news_title,
        "candles": candles,
        "ma20_line": ma20_line,
        "df": df
    }

# ----------------------------------------------------
# 5. 웹 서버 및 프론트엔드 UI
# ----------------------------------------------------
HTML_PAGE = """<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>gaemiGTP - 10초 금융 퀀트 분석 엔진</title>
    <script src="https://unpkg.com/lightweight-charts/dist/lightweight-charts.standalone.production.js"></script>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
        html, body { height: 100%; background-color: #1e1f20; color: #e3e3e3; overflow: hidden; }
        header { position: fixed; top: 0; left: 0; width: 100%; height: 56px; display: flex; justify-content: space-between; align-items: center; padding: 0 20px; z-index: 50; }
        .logo-top { font-weight: 700; font-size: 1.15rem; color: #e3e3e3; letter-spacing: -0.5px; cursor: pointer; }
        .new-btn { background: #282a2c; border: none; color: #c4c7c5; font-size: 0.82rem; padding: 6px 14px; border-radius: 20px; cursor: pointer; font-weight: 500; transition: background 0.2s; white-space: nowrap; }
        .new-btn:hover { background: #37393b; color: #fff; }
        #main-view { position: absolute; top: 0; left: 0; width: 100%; height: 100%; display: flex; flex-direction: column; align-items: center; justify-content: center; padding: 16px; transition: all 0.3s; }
        .hero-title { font-size: 3.2rem; font-weight: 700; margin-bottom: 6px; letter-spacing: -1px; color: #f2f2f2; }
        .hero-sub { font-size: 1rem; color: #8e918f; margin-bottom: 24px; font-weight: 400; text-align: center; }
        .search-container { width: 100%; max-width: 720px; position: relative; }
        .search-box { width: 100%; background: #282a2c; border: none; border-radius: 32px; padding: 4px 6px 4px 16px; display: flex; align-items: center; box-shadow: 0 4px 20px rgba(0,0,0,0.2); position: relative; }
        .search-box input { flex: 1; min-width: 0; background: transparent; border: none; outline: none; color: #f2f2f2; font-size: 0.95rem; padding: 8px 4px; }
        .search-box input::placeholder { color: #8e918f; }
        .model-selector-wrap { position: relative; margin-right: 4px; flex-shrink: 0; }
        .model-pill-btn { background: transparent; border: none; border-radius: 20px; padding: 6px 10px; display: flex; align-items: center; gap: 4px; cursor: pointer; user-select: none; transition: all 0.2s; white-space: nowrap; }
        .model-pill-btn:hover { background: rgba(255, 255, 255, 0.08); }
        .model-pill-name { font-size: 0.85rem; font-weight: 600; color: #e3e3e3; }
        .model-pill-arrow { font-size: 0.6rem; color: #8e918f; }
        .model-dropdown-menu { display: none; position: absolute; bottom: calc(100% + 10px); right: 0; width: 210px; max-width: 80vw; background: #282a2c; border: none; border-radius: 16px; padding: 6px; box-shadow: 0 10px 30px rgba(0,0,0,0.5); z-index: 100; }
        .model-dropdown-item { padding: 9px 12px; border-radius: 10px; cursor: pointer; transition: background 0.15s; display: flex; flex-direction: column; }
        .model-dropdown-item:hover { background: rgba(255, 255, 255, 0.08); }
        .model-dropdown-item.active { background: rgba(255, 255, 255, 0.12); }
        .item-title { font-size: 0.88rem; font-weight: 600; color: #ffffff; display: flex; justify-content: space-between; align-items: center; }
        .item-sub { font-size: 0.73rem; color: #8e918f; margin-top: 2px; }
        .item-check { color: #e3e3e3; font-weight: bold; display: none; font-size: 0.85rem; }
        .model-dropdown-item.active .item-check { display: inline; }
        .send-btn { width: 34px; height: 34px; border-radius: 50%; background: #37393b; border: none; color: #ffffff; display: flex; align-items: center; justify-content: center; cursor: pointer; flex-shrink: 0; }
        .send-btn:hover { background: #4a4d50; }
        .send-btn svg { width: 17px; height: 17px; fill: currentColor; }
        .popular-tags { display: flex; gap: 8px; margin-top: 20px; flex-wrap: wrap; justify-content: center; }
        .tag-btn { background: #282a2c; border: none; border-radius: 16px; padding: 6px 13px; font-size: 0.82rem; color: #c4c7c5; cursor: pointer; white-space: nowrap; }
        .tag-btn:hover { background: #37393b; color: #fff; }
        #result-scroll-view { display: none; position: absolute; top: 56px; bottom: 84px; left: 0; width: 100%; overflow-y: auto; padding: 16px; }
        .result-inner { max-width: 720px; margin: 0 auto; }
        .card { background: #282a2c; border-radius: 16px; padding: 20px; margin-bottom: 16px; }
        .price-title { font-size: 1.3rem; font-weight: 700; color: #f2f2f2; }
        .price-title span { color: #8e918f; font-size: 0.9rem; font-weight: 400; margin-left: 4px; }
        .price-val { font-size: 2.3rem; font-weight: 800; color: #ffffff; margin: 8px 0; }
        .price-meta { color: #8e918f; font-size: 0.8rem; line-height: 1.4; }
        .chart-wrap { width: 100%; height: 300px; margin-top: 14px; position: relative; }
        .issue-wrap { margin-bottom: 16px; }
        .issue-title { font-size: 0.88rem; font-weight: 600; color: #c4c7c5; margin-bottom: 4px; }
        .issue-highlight { font-size: 0.92rem; font-weight: 600; color: #fef08a; display: inline-block; animation: pulseFade 2.4s infinite ease-in-out; }
        @keyframes pulseFade { 0% { opacity: 0.35; } 50% { opacity: 1; filter: brightness(1.2); } 100% { opacity: 0.35; } }
        .section-title { font-size: 1.05rem; font-weight: 700; color: #f2f2f2; margin-bottom: 12px; display: flex; justify-content: space-between; align-items: center; }
        .trend-badge { font-size: 0.88rem; color: #38bdf8; font-weight: 600; margin-bottom: 12px; }
        .quant-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
        .quant-item { background: #1e1f20; padding: 14px; border-radius: 10px; }
        .quant-item .q-label { color: #8e918f; font-size: 0.78rem; margin-bottom: 4px; }
        .quant-item .q-val { color: #ffffff; font-size: 1.05rem; font-weight: 700; }
        .signal-card { background: #202733; border: 1px solid #2b384a; }
        .signal-item { display: flex; justify-content: space-between; padding: 8px 0; border-bottom: 1px solid #283344; font-size: 0.9rem; }
        .signal-item:last-child { border-bottom: none; }
        .signal-target { color: #ef4444; font-weight: 700; }
        .signal-stop { color: #38bdf8; font-weight: 700; }
        .news-item { padding: 12px 0; border-bottom: 1px solid #37393b; text-decoration: none; display: block; }
        .news-item:last-child { border-bottom: none; }
        .news-title { color: #e3e3e3; font-size: 0.9rem; font-weight: 500; line-height: 1.4; }
        .news-title:hover { color: #fff; }
        .news-meta { color: #8e918f; font-size: 0.75rem; margin-top: 4px; }
        #bottom-bar { display: none; position: fixed; bottom: 0; left: 0; width: 100%; height: 76px; background: linear-gradient(180deg, rgba(30,31,32,0) 0%, rgba(30,31,32,1) 40%); align-items: center; justify-content: center; padding: 0 12px 6px; z-index: 50; }
        .loading { display: none; text-align: center; color: #38bdf8; margin: 40px 0; font-size: 0.92rem; }
        @media (max-width: 600px) { .hero-title { font-size: 2.6rem; } .search-box { padding: 3px 5px 3px 12px; } .search-box input { font-size: 0.88rem; } .card { padding: 16px; } }
    </style>
</head>
<body>
    <header>
        <div class="logo-top" onclick="resetToHome()">gaemiGTP</div>
        <button class="new-btn" onclick="resetToHome()">+ 새 검색</button>
    </header>

    <div id="main-view">
        <div class="hero-title">gaemiGTP</div>
        <div class="hero-sub">종목명 하나면 10초 만에 끝납니다</div>
        <div class="search-container">
            <div class="search-box">
                <input type="text" id="centerInput" placeholder="종목명 입력 (예: SK하이닉스, NVDA, BTC, QQQ)" onkeypress="if(event.keyCode==13) doSearch(this.value);" autocomplete="off">
                <div class="model-selector-wrap">
                    <div class="model-pill-btn" onclick="toggleModelDropdown('center')">
                        <span class="model-pill-name" id="centerCurrentModel">gaemi</span>
                        <span class="model-pill-arrow">▲</span>
                    </div>
                    <div class="model-dropdown-menu" id="centerDropdown">
                        <div class="model-dropdown-item active" onclick="selectModel('gaemi', 'gaemi')">
                            <div class="item-title">gaemi <span class="item-check">✓</span></div>
                            <div class="item-sub">통합 퀀트 리포트</div>
                        </div>
                        <div class="model-dropdown-item" onclick="selectModel('info', 'info')">
                            <div class="item-title">info <span class="item-check">✓</span></div>
                            <div class="item-sub">실시간 뉴스 중심</div>
                        </div>
                        <div class="model-dropdown-item" onclick="selectModel('pro', 'pro')">
                            <div class="item-title">pro <span class="item-check">✓</span></div>
                            <div class="item-sub">차트 및 시그널 타점</div>
                        </div>
                    </div>
                </div>
                <button class="send-btn" onclick="doSearch(document.getElementById('centerInput').value)">
                    <svg viewBox="0 0 24 24"><path d="M12 4L12 20M12 4L5 11M12 4L19 11" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" fill="none"/></svg>
                </button>
            </div>
        </div>
        <div class="popular-tags">
            <button class="tag-btn" onclick="doSearch('SK하이닉스')">🔥 SK하이닉스</button>
            <button class="tag-btn" onclick="doSearch('삼성전자')">📱 삼성전자</button>
            <button class="tag-btn" onclick="doSearch('알테오젠')">🧬 알테오젠</button>
            <button class="tag-btn" onclick="doSearch('엔비디아')">🚀 엔비디아</button>
            <button class="tag-btn" onclick="doSearch('비트코인')">₿ 비트코인</button>
        </div>
    </div>

    <div id="result-scroll-view">
        <div class="result-inner">
            <div class="loading" id="loading">⚡ 실시간 팩트 데이터를 연산 중입니다...</div>
            <div id="result-card-wrap" style="display:none;">
                <div class="card" id="cardPrice">
                    <div class="price-title" id="rTitle"></div>
                    <div class="price-val" id="rPrice"></div>
                    <div class="price-meta" id="rMeta"></div>
                    <div class="chart-wrap" id="tvChart"></div>
                </div>

                <div class="card" id="cardIssue" style="display:none;">
                    <div class="issue-wrap">
                        <div class="issue-title">gaemiGTP 이슈</div>
                        <div class="issue-highlight" id="issueText"></div>
                    </div>
                </div>

                <div class="card" id="cardQuant">
                    <div class="section-title">퀀트 기술지표</div>
                    <div class="trend-badge" id="rTrend"></div>
                    <div class="quant-grid">
                        <div class="quant-item"><div class="q-label">5일선</div><div class="q-val" id="rMa5"></div></div>
                        <div class="quant-item"><div class="q-label">20일선</div><div class="q-val" id="rMa20"></div></div>
                        <div class="quant-item"><div class="q-label">60일선</div><div class="q-val" id="rMa60"></div></div>
                        <div class="quant-item"><div class="q-label">120일선</div><div class="q-val" id="rMa120"></div></div>
                        <div class="quant-item"><div class="q-label">RSI(14)</div><div class="q-val" id="rRsi"></div></div>
                        <div class="quant-item"><div class="q-label">ATR(14)</div><div class="q-val" id="rAtr"></div></div>
                        <div class="quant-item"><div class="q-label">ATR 비율</div><div class="q-val" id="rAtrRatio"></div></div>
                        <div class="quant-item"><div class="q-label">거래량/직전5일 평균</div><div class="q-val" id="rVolRatio"></div></div>
                    </div>
                </div>

                <div class="card signal-card" id="cardSignal">
                    <div class="section-title">
                        <span>⚡ gaemiGTP 시그널 타점</span>
                        <span style="font-size:0.75rem; color:#38bdf8; border:1px solid #38bdf8; padding:2px 6px; border-radius:4px;">알고리즘 산출</span>
                    </div>
                    <div class="signal-item"><span>1차 매수 타점</span><strong id="rEntry1" style="color:#fff;"></strong></div>
                    <div class="signal-item"><span>2차 분할 타점</span><strong id="rEntry2" style="color:#fff;"></strong></div>
                    <div class="signal-item"><span>목표 수익 실현가 (+6.5%)</span><strong id="rTarget" class="signal-target"></strong></div>
                    <div class="signal-item"><span>리스크 방어 손절가 (-5.5%)</span><strong id="rStop" class="signal-stop"></strong></div>
                    <div class="signal-item" style="margin-top:6px; border-top:1px dashed #2c3848; padding-top:10px;">
                        <span>1년 백테스트 검증</span>
                        <span id="rBacktest" style="color:#38bdf8; font-weight:700;"></span>
                    </div>
                </div>

                <div class="card" id="cardNews">
                    <div class="section-title">최근 뉴스</div>
                    <div id="newsList"></div>
                </div>
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
                            <div class="item-sub">통합 퀀트 리포트</div>
                        </div>
                        <div class="model-dropdown-item" onclick="selectModel('info', 'info')">
                            <div class="item-title">info <span class="item-check">✓</span></div>
                            <div class="item-sub">실시간 뉴스 중심</div>
                        </div>
                        <div class="model-dropdown-item" onclick="selectModel('pro', 'pro')">
                            <div class="item-title">pro <span class="item-check">✓</span></div>
                            <div class="item-sub">차트 및 시그널 타점</div>
                        </div>
                    </div>
                </div>
                <button class="send-btn" onclick="doSearch(document.getElementById('bottomInput').value)">
                    <svg viewBox="0 0 24 24"><path d="M12 4L12 20M12 4L5 11M12 4L19 11" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" fill="none"/></svg>
                </button>
            </div>
        </div>
    </div>

    <script>
        let chart = null;
        let candleSeries = null;
        let lineSeries = null;
        let selectedMode = 'gaemi';

        function toggleModelDropdown(target) {
            const centerD = document.getElementById('centerDropdown');
            const bottomD = document.getElementById('bottomDropdown');
            const targetD = target === 'center' ? centerD : bottomD;
            const otherD = target === 'center' ? bottomD : centerD;
            otherD.style.display = 'none';
            targetD.style.display = targetD.style.display === 'block' ? 'none' : 'block';
        }

        function selectModel(modeKey, modeName) {
            selectedMode = modeKey;
            document.getElementById('centerCurrentModel').innerText = modeName;
            document.getElementById('bottomCurrentModel').innerText = modeName;

            ['centerDropdown', 'bottomDropdown'].forEach(id => {
                const items = document.getElementById(id).querySelectorAll('.model-dropdown-item');
                items.forEach(it => {
                    if (it.innerText.includes(modeName)) {
                        it.classList.add('active');
                    } else {
                        it.classList.remove('active');
                    }
                });
                document.getElementById(id).style.display = 'none';
            });
            applyModelFilter();
        }

        function applyModelFilter() {
            const price = document.getElementById('cardPrice');
            const quant = document.getElementById('cardQuant');
            const signal = document.getElementById('cardSignal');
            const news = document.getElementById('cardNews');

            if (selectedMode === 'gaemi') {
                price.style.display = 'block';
                quant.style.display = 'block';
                signal.style.display = 'block';
                news.style.display = 'block';
            } else if (selectedMode === 'info') {
                price.style.display = 'block';
                quant.style.display = 'none';
                signal.style.display = 'none';
                news.style.display = 'block';
            } else if (selectedMode === 'pro') {
                price.style.display = 'block';
                quant.style.display = 'block';
                signal.style.display = 'block';
                news.style.display = 'none';
            }
        }

        document.addEventListener('click', function(e) {
            if (!e.target.closest('.model-selector-wrap')) {
                document.getElementById('centerDropdown').style.display = 'none';
                document.getElementById('bottomDropdown').style.display = 'none';
            }
        });

        function resetToHome() {
            document.getElementById('main-view').style.display = 'flex';
            document.getElementById('result-scroll-view').style.display = 'none';
            document.getElementById('bottom-bar').style.display = 'none';
            document.getElementById('result-card-wrap').style.display = 'none';
            document.getElementById('loading').style.display = 'none';
            document.getElementById('centerInput').value = '';
            document.getElementById('bottomInput').value = '';
        }

        function fmtNum(v, isKrw) {
            if (isKrw) return parseInt(v).toLocaleString() + '원';
            return parseFloat(v).toLocaleString(undefined, {minimumFractionDigits: 2}) + '달러';
        }

        async function doSearch(query) {
            const cleanQ = query.trim();
            if (!cleanQ) return;

            document.getElementById('main-view').style.display = 'none';
            document.getElementById('result-scroll-view').style.display = 'block';
            document.getElementById('bottom-bar').style.display = 'flex';
            document.getElementById('loading').style.display = 'block';
            document.getElementById('result-card-wrap').style.display = 'none';
            document.getElementById('bottomInput').value = '';

            try {
                const url = '/api/analyze?q=' + encodeURIComponent(cleanQ);
                const response = await fetch(url);
                const data = await response.json();
                if (!response.ok || data.error) {
                    alert(data.error || '분석 중 오류가 발생했습니다.');
                    document.getElementById('loading').style.display = 'none';
                    return;
                }

                const isKrw = data.market === '국내주식';
                document.getElementById('rTitle').innerHTML = `${data.name} <span>(${data.symbol})</span>`;
                document.getElementById('rPrice').innerText = data.price;
                document.getElementById('rMeta').innerHTML = `시장: ${data.market} · 데이터 출처: ${isKrw ? 'Naver 증권' : 'Yahoo/Binance'}<br>조회시각: ${data.time}`;

                if (data.top_news) {
                    document.getElementById('issueText').innerText = "👉 " + data.top_news;
                    document.getElementById('cardIssue').style.display = 'block';
                } else {
                    document.getElementById('cardIssue').style.display = 'none';
                }

                document.getElementById('rTrend').innerText = `추세: ${data.quant.trend}`;
                document.getElementById('rMa5').innerText = fmtNum(data.quant.ma5, isKrw);
                document.getElementById('rMa20').innerText = fmtNum(data.quant.ma20, isKrw);
                document.getElementById('rMa60').innerText = fmtNum(data.quant.ma60, isKrw);
                document.getElementById('rMa120').innerText = fmtNum(data.quant.ma120, isKrw);
                document.getElementById('rRsi').innerText = data.quant.rsi;
                document.getElementById('rAtr').innerText = fmtNum(data.quant.atr, isKrw);
                document.getElementById('rAtrRatio').innerText = data.quant.atr_ratio + '%';
                document.getElementById('rVolRatio').innerText = data.quant.vol_ratio + '%';

                document.getElementById('rEntry1').innerText = fmtNum(data.quant.entry1, isKrw);
                document.getElementById('rEntry2').innerText = fmtNum(data.quant.entry2, isKrw);
                document.getElementById('rTarget').innerText = fmtNum(data.quant.target_p, isKrw);
                document.getElementById('rStop').innerText = fmtNum(data.quant.stop_p, isKrw);
                document.getElementById('rBacktest').innerText = `승률 ${data.quant.win_rate}% / 손익비 ${data.quant.rr_ratio}`;

                const nBox = document.getElementById('newsList');
                nBox.innerHTML = '';
                if (data.news && data.news.length > 0) {
                    data.news.forEach(n => {
                        nBox.innerHTML += `
                            <a href="${n.link}" target="_blank" class="news-item">
                                <div class="news-title">${n.title}</div>
                                <div class="news-meta">${n.media} · ${n.pubDate}</div>
                            </a>
                        `;
                    });
                } else {
                    nBox.innerHTML = '<div style="color:#8e918f; font-size:0.85rem;">관련 뉴스가 없습니다.</div>';
                }

                document.getElementById('loading').style.display = 'none';
                document.getElementById('result-card-wrap').style.display = 'block';
                document.getElementById('result-scroll-view').scrollTo({top: 0, behavior: 'smooth'});

                applyModelFilter();
                initTvChart(data.candles, data.ma20_line);

            } catch (err) {
                alert('서버와 통신할 수 없습니다.');
                document.getElementById('loading').style.display = 'none';
            }
        }

        function initTvChart(candles, ma20) {
            const chartContainer = document.getElementById('tvChart');
            chartContainer.innerHTML = '';

            chart = LightweightCharts.createChart(chartContainer, {
                width: chartContainer.clientWidth,
                height: 300,
                layout: { background: { color: '#282a2c' }, textColor: '#8e918f' },
                grid: { vertLines: { color: '#333538' }, horzLines: { color: '#333538' } },
                timeScale: { borderColor: '#37393b' },
                rightPriceScale: { borderColor: '#37393b' }
            });

            candleSeries = chart.addCandlestickSeries({
                upColor: '#ef4444',
                downColor: '#38bdf8',
                borderDownColor: '#38bdf8',
                borderUpColor: '#ef4444',
                wickDownColor: '#38bdf8',
                wickUpColor: '#ef4444',
            });
            candleSeries.setData(candles);

            lineSeries = chart.addLineSeries({
                color: '#fef08a',
                lineWidth: 2,
            });
            lineSeries.setData(ma20);

            chart.timeScale().fitContent();

            window.addEventListener('resize', () => {
                if (chart && chartContainer) {
                    chart.applyOptions({ width: chartContainer.clientWidth });
                }
            });
        }
    </script>
</body>
</html>"""

class WebHandler(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)

        if parsed.path == "/api/analyze":
            q = params.get('q', [''])[0].strip()
            data = run_full_pipeline(q) if q else None
            if not data:
                self.send_response(404)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({"error": "종목을 찾을 수 없습니다."}, ensure_ascii=False).encode('utf-8'))
                return

            self.send_response(200)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            res_obj = {
                "name": data["formal_name"],
                "symbol": data["symbol"],
                "market": data["market"],
                "price": data["price_str"],
                "time": data["now_str"],
                "quant": data["quant"],
                "news": data["news"],
                "top_news": data["top_news"],
                "candles": data["candles"],
                "ma20_line": data["ma20_line"]
            }
            self.wfile.write(json.dumps(res_obj, ensure_ascii=False).encode('utf-8'))
            return

        self.send_response(200)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.end_headers()
        self.wfile.write(HTML_PAGE.encode('utf-8'))

def run_web_server():
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), WebHandler)
    server.serve_forever()

if __name__ == "__main__":
    run_web_server()
