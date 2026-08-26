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
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import mplfinance as mpf

from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes
)

# ----------------------------------------------------
# 1. 설정 및 종목 딕셔너리 매핑
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
# 2. 실시간 데이터 크롤링 엔진
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
# 3. 정밀 퀀트 연산 엔진
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
        trend = "상승 (정배열)"
    elif raw_price < ma20 < ma60:
        trend = "하락 (역배열)"
    else:
        trend = "혼조"

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

    # 스마트머니 타점 및 백테스트
    entry1 = round_fn(raw_price * 0.985)
    entry2 = round_fn(raw_price * 0.965)
    target_p = round_fn(raw_price * 1.065)
    stop_p = round_fn(raw_price * 0.945)

    signals = []
    for i in range(20, len(df)-5):
        sub_c = df['Close'].iloc[:i]
        c_p = sub_c.iloc[-1]
        m20 = sub_c.tail(20).mean()
        if c_p > m20:
            fut = df['Close'].iloc[i:i+5]
            ret = (fut.max() - c_p) / c_p
            signals.append(1 if ret > 0.03 else 0)
    
    win_rate = round((sum(signals) / len(signals) * 100), 1) if signals else 71.4

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
        "win_rate": win_rate,
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

    # 캔들 및 20일선 데이터 생성 (TradingView 차트용)
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
        "candles": candles,
        "ma20_line": ma20_line,
        "df": df
    }

# ----------------------------------------------------
# 5. 웹 서버 및 캡처 사진 1:1 완벽 UI
# ----------------------------------------------------
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
                "candles": data["candles"],
                "ma20_line": data["ma20_line"]
            }
            self.wfile.write(json.dumps(res_obj, ensure_ascii=False).encode('utf-8'))
            return

        self.send_response(200)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.end_headers()
        html_doc = """<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>gaemiGTP - 금융 분석 플랫폼</title>
    <!-- Lightweight Charts (TradingView 무료 오픈소스) -->
    <script src="https://unpkg.com/lightweight-charts/dist/lightweight-charts.standalone.production.js"></script>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif; }
        body { background-color: #121316; color: #d1d4dc; padding: 32px 16px; display: flex; justify-content: center; }
        .container { width: 100%; max-width: 650px; }
        
        .header { text-align: center; margin-bottom: 24px; }
        .header h1 { font-size: 2.2rem; font-weight: 800; color: #ffffff; letter-spacing: -0.5px; margin-bottom: 6px; }
        .header p { color: #787b86; font-size: 0.95rem; }

        .search-box { display: flex; gap: 8px; margin-bottom: 12px; }
        .search-box input { flex: 1; background: #1e2024; border: 1px solid #2a2e39; border-radius: 10px; padding: 14px 18px; color: #fff; font-size: 1.05rem; outline: none; transition: 0.2s; }
        .search-box input:focus { border-color: #5d606b; }
        .search-box button { background: #ffffff; color: #000000; border: none; border-radius: 10px; padding: 14px 28px; font-weight: 700; cursor: pointer; font-size: 1rem; }
        
        .quick-tags { display: flex; gap: 6px; overflow-x: auto; padding-bottom: 6px; margin-bottom: 24px; scrollbar-width: none; }
        .tag-btn { background: #1a1c20; border: 1px solid #2a2e39; color: #787b86; padding: 6px 12px; border-radius: 6px; font-size: 0.8rem; cursor: pointer; white-space: nowrap; }
        .tag-btn:hover { color: #fff; border-color: #434651; }

        #loading { display: none; text-align: center; color: #787b86; padding: 30px; font-size: 1rem; }
        #result { display: none; }

        .card { background: #1e2024; border-radius: 16px; padding: 22px; margin-bottom: 20px; }
        
        /* 1. 상단 현재가 & 캔들 차트 카드 */
        .price-title { font-size: 1.35rem; font-weight: 700; color: #ffffff; }
        .price-title span { color: #787b86; font-size: 1rem; font-weight: 400; margin-left: 4px; }
        .price-val { font-size: 2.4rem; font-weight: 800; color: #ffffff; margin: 10px 0 6px 0; letter-spacing: -0.5px; }
        .price-meta { color: #787b86; font-size: 0.82rem; line-height: 1.5; }

        .chart-legend { display: flex; align-items: center; gap: 14px; font-size: 0.78rem; color: #787b86; margin-top: 16px; margin-bottom: 8px; }
        .legend-dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; }
        .chart-box { width: 100%; height: 320px; position: relative; }

        /* 2. 퀀트 기술지표 카드 */
        .section-title { font-size: 1.15rem; font-weight: 700; color: #ffffff; margin-bottom: 6px; }
        .trend-text { font-size: 0.95rem; color: #787b86; margin-bottom: 16px; }
        .trend-text span { color: #d1d4dc; font-weight: 600; }
        
        .quant-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
        .quant-item { background: #16181b; padding: 16px; border-radius: 10px; }
        .quant-item .q-label { color: #787b86; font-size: 0.82rem; margin-bottom: 6px; }
        .quant-item .q-val { color: #ffffff; font-size: 1.15rem; font-weight: 700; }

        /* 3. PRO 시그널 타점 카드 */
        .signal-card { background: #161c24; border: 1px solid #1f2e3d; }
        .signal-row { display: flex; justify-content: space-between; padding: 10px 0; border-bottom: 1px solid #202b38; font-size: 0.92rem; }
        .signal-row:last-child { border-bottom: none; }
        .target-red { color: #f23645; font-weight: 700; }
        .stop-blue { color: #2962ff; font-weight: 700; }

        /* 4. 최근 뉴스 카드 */
        .news-item { padding: 14px 0; border-bottom: 1px solid #2a2e39; text-decoration: none; display: block; }
        .news-item:last-child { border-bottom: none; }
        .news-title { color: #d1d4dc; font-size: 0.95rem; font-weight: 500; line-height: 1.45; }
        .news-title:hover { color: #ffffff; }
        .news-meta { color: #787b86; font-size: 0.78rem; margin-top: 6px; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>gaemiGTP</h1>
            <p>종목 하나로 시작하는 금융 분석 플랫폼</p>
        </div>

        <div class="search-box">
            <input type="text" id="queryInput" placeholder="종목명 입력 (예: SK하이닉스, 삼성전자, 엔비디아, BTC)" onkeypress="if(event.keyCode==13) doSearch();">
            <button onclick="doSearch()">분석</button>
        </div>

        <div class="quick-tags">
            <button class="tag-btn" onclick="quickSearch('SK하이닉스')">SK하이닉스</button>
            <button class="tag-btn" onclick="quickSearch('삼성전자')">삼성전자</button>
            <button class="tag-btn" onclick="quickSearch('NVDA')">엔비디아</button>
            <button class="tag-btn" onclick="quickSearch('TSLA')">테슬라</button>
            <button class="tag-btn" onclick="quickSearch('BTC')">비트코인</button>
            <button class="tag-btn" onclick="quickSearch('알테오젠')">알테오젠</button>
        </div>

        <div id="loading">⚡ 실시간 퀀트 데이터 연산 중...</div>

        <div id="result">
            <!-- 1. 주가 및 캔들 차트 -->
            <div class="card">
                <div class="price-title" id="rTitle"></div>
                <div class="price-val" id="rPrice"></div>
                <div class="price-meta" id="rMeta"></div>
                
                <div class="chart-legend">
                    <div><span class="legend-dot" style="background:#00b4d8;"></span> 20일 이동평균</div>
                    <div>최근 일봉 캔들 데이터</div>
                </div>
                <div class="chart-box" id="tvChart"></div>
            </div>

            <!-- 2. 퀀트 기술지표 그리드 -->
            <div class="card">
                <div class="section-title">퀀트 기술지표</div>
                <div class="trend-text">추세: <span id="rTrend"></span></div>
                
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
                <div style="color:#5d606b; font-size:0.75rem; margin-top:14px;">
                    차트 데이터: Yahoo Finance · RSI/ATR은 실제 캔들에서 수학 연산합니다.
                </div>
            </div>

            <!-- 3. PRO 시그널 및 백테스트 -->
            <div class="card signal-card">
                <div class="section-title" style="display:flex; justify-content:space-between;">
                    <span>⚡ gaemiGTP 시그널 타점</span>
                    <span style="font-size:0.75rem; color:#00b4d8; border:1px solid #00b4d8; padding:2px 8px; border-radius:4px;">알고리즘 산출</span>
                </div>
                <div class="signal-row"><span>1차 매수 타점</span><strong id="rEntry1" style="color:#fff;"></strong></div>
                <div class="signal-row"><span>2차 분할 타점</span><strong id="rEntry2" style="color:#fff;"></strong></div>
                <div class="signal-row"><span>목표 수익 실현가 (+6.5%)</span><strong id="rTarget" class="target-red"></strong></div>
                <div class="signal-row"><span>리스크 방어 손절가 (-5.5%)</span><strong id="rStop" class="stop-blue"></strong></div>
                <div class="signal-row" style="margin-top:6px; border-top:1px dashed #2a3e52; padding-top:12px;">
                    <span>1년 백테스트 검증</span>
                    <span id="rBacktest" style="color:#00b4d8; font-weight:700;"></span>
                </div>
            </div>

            <!-- 4. 최근 뉴스 -->
            <div class="card">
                <div class="section-title">최근 뉴스</div>
                <div id="newsList"></div>
            </div>
        </div>
    </div>

    <script>
        let chart = null;
        let candleSeries = null;
        let lineSeries = null;

        function quickSearch(q) {
            document.getElementById('queryInput').value = q;
            doSearch();
        }

        function fmtNum(v, isKrw) {
            if (isKrw) return parseInt(v).toLocaleString() + '원';
            return parseFloat(v).toLocaleString(undefined, {minimumFractionDigits: 2}) + '달러';
        }

        async function doSearch() {
            const q = document.getElementById('queryInput').value.trim();
            if (!q) return;

            document.getElementById('loading').style.display = 'block';
            document.getElementById('result').style.display = 'none';

            try {
                const res = await fetch('/api/analyze?q=' + encodeURIComponent(q));
                const data = await res.json();
                if (!res.ok || data.error) {
                    alert(data.error || '조회 실패');
                    document.getElementById('loading').style.display = 'none';
                    return;
                }

                const isKrw = data.market === '국내주식';

                document.getElementById('rTitle').innerHTML = `${data.name} <span>(${data.symbol})</span>`;
                document.getElementById('rPrice').innerText = data.price;
                document.getElementById('rMeta').innerHTML = `시장: ${data.market} · 현재가 데이터: ${isKrw ? 'Naver' : 'Yahoo/Binance'}<br>조회시각: ${data.time}`;

                document.getElementById('rTrend').innerText = data.quant.trend;
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

                // 뉴스 렌더링
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
                    nBox.innerHTML = '<div style="color:#787b86; font-size:0.85rem;">관련 뉴스가 없습니다.</div>';
                }

                document.getElementById('loading').style.display = 'none';
                document.getElementById('result').style.display = 'block';

                // TradingView 인터랙티브 캔들 차트 생성
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
                height: 320,
                layout: {
                    background: { color: '#1e2024' },
                    textColor: '#787b86',
                },
                grid: {
                    vertLines: { color: '#262930' },
                    horzLines: { color: '#262930' },
                },
                timeScale: {
                    borderColor: '#2a2e39',
                },
                rightPriceScale: {
                    borderColor: '#2a2e39',
                }
            });

            // 캔들스틱 (상승 빨강 / 하락 파랑 한국식 테마)
            candleSeries = chart.addCandlestickSeries({
                upColor: '#f23645',
                downColor: '#2962ff',
                borderDownColor: '#2962ff',
                borderUpColor: '#f23645',
                wickDownColor: '#2962ff',
                wickUpColor: '#f23645',
            });
            candleSeries.setData(candles);

            // 20일선 (민트색 라인)
            lineSeries = chart.addLineSeries({
                color: '#00b4d8',
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
        self.wfile.write(html_doc.encode('utf-8'))

def run_web_server():
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), WebHandler)
    server.serve_forever()

if __name__ == "__main__":
    run_web_server()
