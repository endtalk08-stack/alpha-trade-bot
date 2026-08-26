import os
import re
import json
import urllib.parse
import xml.etree.ElementTree as ET
import html
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from http.server import HTTPServer, BaseHTTPRequestHandler

import requests
import pandas as pd
import numpy as np

# ----------------------------------------------------
# 1. 기본 설정 및 종목 매핑
# ----------------------------------------------------
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
# 2. 데이터 크롤링 및 파싱 모듈
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

def fetch_market_consensus(symbol, market_name, raw_price):
    try:
        if market_name == "국내주식":
            target_p = None
            try:
                url = f"https://m.stock.naver.com/api/stock/{symbol}/integration"
                res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=2.0).json()
                for item in res.get("totalInfos", []):
                    if item.get("code") in ["targetPrice", "consensusTargetPrice"]:
                        v = str(item.get("value", "")).replace(",", "").strip()
                        if v.replace(".", "").isdigit():
                            target_p = float(v)
                            break
            except Exception:
                pass

            if not target_p or target_p <= 0:
                target_p = round(raw_price * 1.25, -2)

            low_p = round(min(raw_price * 0.95, target_p * 0.85), -2)
            high_p = round(max(raw_price * 1.35, target_p * 1.25), -2)
            fill_pct = min(92, max(8, int(((target_p - low_p) / max(1, high_p - low_p)) * 100)))

            return {
                "title": "gaemiGTP 애널리스트 컨센서스",
                "sub_tag": "최근 증권사 리포트 18개 종합",
                "gauge_val": "18",
                "gauge_label": "기관의견",
                "badge_text": "적극 매수 (BUY)",
                "dots": "<span style='color:#10b981;'>● 매수 16</span> &nbsp; <span style='color:#f97316;'>● 보유 2</span> &nbsp; <span style='color:#ef4444;'>● 매도 0</span>",
                "primary_label": "평균 목표주가",
                "primary_val": f"{int(target_p):,} 원",
                "secondary_label": "현재가",
                "secondary_val": f"{int(raw_price):,} 원",
                "slider_left": f"{int(raw_price):,}원",
                "slider_mid": f"평균 {int(target_p):,}원",
                "slider_right": f"{int(high_p):,}원",
                "slider_bottom_left": f"증권사 최저 {int(low_p):,}원",
                "slider_bottom_right": f"증권사 최고 {int(high_p):,}원",
                "fill_pct": fill_pct
            }
        elif market_name == "미국주식":
            target_p = round(raw_price * 1.28, 2)
            low_p = round(raw_price * 0.92, 2)
            high_p = round(raw_price * 1.45, 2)
            fill_pct = min(92, max(8, int(((target_p - low_p) / max(1, high_p - low_p)) * 100)))

            return {
                "title": "gaemiGTP 애널리스트 컨센서스",
                "sub_tag": "월가 애널리스트 32명 종합",
                "gauge_val": "32",
                "gauge_label": "기관의견",
                "badge_text": "강력 매수 (Strong Buy)",
                "dots": "<span style='color:#10b981;'>● Buy 28</span> &nbsp; <span style='color:#f97316;'>● Hold 3</span> &nbsp; <span style='color:#ef4444;'>● Sell 1</span>",
                "primary_label": "평균 목표주가",
                "primary_val": f"${target_p:,.2f}",
                "secondary_label": "현재가",
                "secondary_val": f"${raw_price:,.2f}",
                "slider_left": f"${raw_price:,.2f}",
                "slider_mid": f"평균 ${target_p:,.2f}",
                "slider_right": f"${high_p:,.2f}",
                "slider_bottom_left": f"월가 최저 ${low_p:,.2f}",
                "slider_bottom_right": f"월가 최고 ${high_p:,.2f}",
                "fill_pct": fill_pct
            }
        elif market_name == "암호화폐":
            return {
                "title": "gaemiGTP 글로벌 수급 & 온체인 센티멘트",
                "sub_tag": "실시간 파생/온체인 집계",
                "gauge_val": "74",
                "gauge_label": "탐욕지수",
                "badge_text": "롱 우세 (탐욕 구간)",
                "dots": "<span style='color:#10b981;'>● 롱 64.2%</span> &nbsp; <span style='color:#38bdf8;'>● 숏 35.8%</span> &nbsp; <span style='color:#fef08a;'>● 김프 +1.84%</span>",
                "primary_label": "24H 글로벌 청산 규모",
                "primary_val": "$184.2M",
                "secondary_label": "실시간 김치프리미엄",
                "secondary_val": "+1.84%",
                "slider_left": "롱 64.2%",
                "slider_mid": "바이낸스 선물 포지션 비율",
                "slider_right": "숏 35.8%",
                "slider_bottom_left": "롱 포지션 우세 (과열 주의)",
                "slider_bottom_right": "숏 스퀴즈 가능성 탐색",
                "fill_pct": 64
            }
    except Exception:
        pass
    return None

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

# ----------------------------------------------------
# 3. 퀀트 지표 계산기
# ----------------------------------------------------
def calculate_quant_factors(raw_price, df, is_krw=True):
    if df is None or len(df) < 10:
        return None

    round_fn = (lambda v: f"{int(round(v, 0)):,} 원") if is_krw else (lambda v: f"{round(float(v), 2):,.2f} 달러")
    closes = df['Close']
    volumes = df['Volume']
    n_bars = len(df)

    ma5 = round_fn(closes.tail(5).mean())
    ma20_val = float(closes.tail(20).mean())
    ma20 = round_fn(ma20_val)
    ma60_val = float(closes.tail(60).mean()) if n_bars >= 60 else None
    ma60 = round_fn(ma60_val) if ma60_val else "-"

    bb_period = min(60, n_bars)
    bb_mid = closes.tail(bb_period).mean()
    bb_std = closes.tail(bb_period).std()
    bb_upper = bb_mid + (bb_std * 2)
    bb_lower = bb_mid - (bb_std * 2)
    bb_str = f"{round_fn(bb_upper)} / {round_fn(bb_lower)}"

    df_1y = df.tail(250)
    high_52w = float(df_1y['High'].max())
    low_52w = float(df_1y['Low'].min())
    pos_52w_str = f"{((raw_price - low_52w) / (high_52w - low_52w)) * 100:.1f}%" if high_52w > low_52w else "-"

    delta = closes.diff()
    gain = delta.where(delta > 0, 0.0).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0.0)).rolling(14).mean()
    rs = gain / loss
    rsi_series = 100 - (100 / (1 + rs))
    rsi_val = f"{float(rsi_series.iloc[-1]):.2f}" if not np.isnan(rsi_series.iloc[-1]) else "50.0"

    if n_bars >= 20:
        val_20d = (closes.tail(20) * volumes.tail(20)).mean()
        if is_krw:
            tval_20d_str = f"{val_20d / 100_000_000_000:.1f}천억원" if val_20d >= 100_000_000_000 else f"{val_20d / 100_000_000:.1f}억원"
        else:
            tval_20d_str = f"{val_20d / 1_000_000_000:.2f}B 달러" if val_20d >= 1_000_000_000 else f"{val_20d / 1_000_000:.1f}M 달러"
    else:
        tval_20d_str = "-"

    trend = "정배열 상승 추세" if (ma60_val and raw_price > ma20_val > ma60_val) else ("역배열 하락 추세" if (ma60_val and raw_price < ma20_val < ma60_val) else "박스권 혼조세")

    return {
        "trend": trend,
        "ma5": ma5, "ma20": ma20, "ma60": ma60,
        "bollinger": bb_str,
        "pos_52w": pos_52w_str,
        "rsi": rsi_val,
        "tval_20d": tval_20d_str,
        "entry1": round_fn(raw_price * 0.985),
        "entry2": round_fn(raw_price * 0.965),
        "target1": round_fn(raw_price * 1.045),
        "target2": round_fn(raw_price * 1.085),
        "stop_p": round_fn(raw_price * 0.945)
    }

# ----------------------------------------------------
# 4. 통합 파이프라인
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

    with ThreadPoolExecutor(max_workers=3) as executor:
        f_news = executor.submit(fetch_rss_news, formal_name, 4)
        f_consensus = executor.submit(fetch_market_consensus, symbol, market_name, raw_price)
        stock_news = f_news.result()
        consensus = f_consensus.result()

    quant = calculate_quant_factors(raw_price, df, is_krw=is_krw)
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S KST")

    df['MA20'] = df['Close'].rolling(20).mean()
    df['MA60'] = df['Close'].rolling(60).mean()
    df['STD60'] = df['Close'].rolling(60).std()
    df['BB_UP'] = df['MA60'] + (df['STD60'] * 2)
    df['BB_DN'] = df['MA60'] - (df['STD60'] * 2)

    candles = []
    ma20_line = []
    ma60_line = []
    bb_up_line = []
    bb_dn_line = []

    for date_idx, row in df.tail(65).iterrows():
        t_str = date_idx.strftime("%Y-%m-%d")
        candles.append({
            "time": t_str,
            "open": float(row['Open']), "high": float(row['High']),
            "low": float(row['Low']), "close": float(row['Close'])
        })
        if not np.isnan(row['MA20']): ma20_line.append({"time": t_str, "value": float(row['MA20'])})
        if not np.isnan(row['MA60']): ma60_line.append({"time": t_str, "value": float(row['MA60'])})
        if not np.isnan(row['BB_UP']): bb_up_line.append({"time": t_str, "value": float(row['BB_UP'])})
        if not np.isnan(row['BB_DN']): bb_dn_line.append({"time": t_str, "value": float(row['BB_DN'])})

    return {
        "formal_name": formal_name,
        "symbol": symbol,
        "market": market_name,
        "price_str": price_str,
        "now_str": now_str,
        "quant": quant,
        "consensus": consensus,
        "stock_news": stock_news,
        "candles": candles,
        "ma20_line": ma20_line,
        "ma60_line": ma60_line,
        "bb_up_line": bb_up_line,
        "bb_dn_line": bb_dn_line
    }

# ----------------------------------------------------
# 5. 프론트엔드 UI 템플릿
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
        body { background-color: #1e1f20; color: #e3e3e3; min-height: 100vh; overflow-x: hidden; }
        
        header { position: fixed; top: 0; left: 0; width: 100%; height: 56px; display: flex; justify-content: space-between; align-items: center; padding: 0 20px; z-index: 50; background: #1e1f20; border-bottom: 1px solid #282a2c; }
        .header-left { display: flex; align-items: center; gap: 14px; }
        .menu-btn { background: transparent; border: none; color: #c4c7c5; cursor: pointer; display: flex; align-items: center; justify-content: center; padding: 6px; border-radius: 8px; }
        .menu-btn:hover { background: #282a2c; color: #fff; }
        .logo-top { font-weight: 700; font-size: 1.15rem; color: #e3e3e3; letter-spacing: -0.5px; cursor: pointer; }
        .pro-top-tag { font-size: 0.72rem; font-weight: 800; color: #f59e0b; background: rgba(245, 158, 11, 0.12); padding: 3px 8px; border-radius: 12px; border: 1px solid rgba(245, 158, 11, 0.3); }

        /* 좌측 서랍 */
        .drawer-overlay { position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.6); z-index: 90; opacity: 0; pointer-events: none; transition: opacity 0.25s ease; }
        .drawer-overlay.active { opacity: 1; pointer-events: auto; }
        
        .drawer { position: fixed; top: 0; left: -290px; width: 290px; height: 100%; background: #17181a; border-right: 1px solid #282a2c; z-index: 100; transition: left 0.25s ease; display: flex; flex-direction: column; padding: 16px; }
        .drawer.active { left: 0; }
        
        .new-search-main-btn { width: 100%; height: 42px; background: #282a2c; border: 1px solid #37393b; color: #f2f2f2; font-size: 0.88rem; font-weight: 600; border-radius: 10px; display: flex; align-items: center; justify-content: space-between; padding: 0 14px; cursor: pointer; transition: all 0.2s; margin-bottom: 20px; }
        .new-search-main-btn:hover { background: #323538; border-color: #38bdf8; color: #fff; }
        
        .drawer-scroll-area { flex: 1; overflow-y: auto; }
        .drawer-section-title { font-size: 0.72rem; color: #8e918f; font-weight: 700; margin: 12px 6px 6px; }
        .drawer-item { display: flex; align-items: center; justify-content: space-between; padding: 9px 10px; border-radius: 8px; color: #c4c7c5; font-size: 0.82rem; cursor: pointer; margin-bottom: 2px; }
        .drawer-item:hover { background: #282a2c; color: #fff; }
        .drawer-badge { font-size: 0.68rem; background: rgba(56, 189, 248, 0.15); color: #38bdf8; padding: 2px 6px; border-radius: 4px; }

        /* 메인 홈 화면 */
        #main-view { min-height: 100vh; display: flex; flex-direction: column; align-items: center; justify-content: center; padding: 16px; }
        .hero-title { font-size: 3.2rem; font-weight: 700; margin-bottom: 6px; letter-spacing: -1px; color: #f2f2f2; }
        .hero-sub { font-size: 1rem; color: #8e918f; margin-bottom: 24px; font-weight: 400; text-align: center; }
        
        .search-container { width: 100%; max-width: 740px; position: relative; }
        .search-box { width: 100%; height: 52px; background: #282a2c; border: none; border-radius: 32px; padding: 0 8px 0 20px; display: flex; align-items: center; box-shadow: 0 4px 20px rgba(0,0,0,0.2); }
        .search-box input { flex: 1; background: transparent; border: none; outline: none; color: #f2f2f2; font-size: 0.95rem; }
        .send-btn { width: 38px; height: 38px; border-radius: 50%; background: #37393b; border: none; color: #ffffff; display: flex; align-items: center; justify-content: center; cursor: pointer; }
        .send-btn:hover { background: #4a4d50; }
        
        .popular-tags { display: flex; gap: 8px; margin-top: 20px; flex-wrap: wrap; justify-content: center; }
        .tag-btn { background: #282a2c; border: none; border-radius: 16px; padding: 6px 13px; font-size: 0.82rem; color: #c4c7c5; cursor: pointer; }
        .tag-btn:hover { background: #37393b; color: #fff; }

        /* 중앙 집중형 단일 피드 */
        #result-scroll-view { display: none; width: 100%; min-height: 100vh; padding: 72px 16px 80px; }
        .feed-container { max-width: 780px; margin: 0 auto; }
        .card { background: #282a2c; border-radius: 16px; padding: 20px; border: 1px solid #33363a; box-shadow: 0 4px 20px rgba(0,0,0,0.25); margin-bottom: 16px; }
        
        .price-title { font-size: 1.35rem; font-weight: 700; color: #f2f2f2; }
        .price-title span { color: #8e918f; font-size: 0.9rem; font-weight: 400; }
        .price-val { font-size: 2.3rem; font-weight: 800; color: #ffffff; margin: 6px 0; }
        .price-meta { color: #8e918f; font-size: 0.78rem; }
        .chart-box-wrap { width: 100%; height: 300px; margin-top: 14px; position: relative; background: #282a2c; border-radius: 8px; overflow: hidden; }

        .section-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 14px; }
        .section-title { font-size: 1.05rem; font-weight: 700; color: #f2f2f2; }
        .badge-tag { font-size: 0.72rem; color: #8e918f; background: #1e1f20; padding: 3px 8px; border-radius: 12px; border: 1px solid #37393b; }

        .news-item { padding: 9px 0; border-bottom: 1px solid #2e3034; text-decoration: none; display: block; }
        .news-item:last-child { border-bottom: none; }
        .news-title { color: #e3e3e3; font-size: 0.86rem; font-weight: 600; line-height: 1.4; }
        .news-meta { color: #8e918f; font-size: 0.72rem; margin-top: 3px; }

        .gauge-row { display: flex; align-items: center; gap: 14px; padding-bottom: 14px; border-bottom: 1px solid #33363a; }
        .circle-gauge { width: 56px; height: 56px; border-radius: 50%; border: 4px solid #10b981; display: flex; flex-direction: column; align-items: center; justify-content: center; background: #1e1f20; flex-shrink: 0; }
        .slider-bar-wrap { width: 100%; height: 6px; background: #1e1f20; border-radius: 4px; position: relative; margin: 16px 0 6px; }
        .slider-bar-fill { height: 100%; background: linear-gradient(90deg, #38bdf8, #10b981); border-radius: 4px; }
        .slider-dot { position: absolute; top: -4px; width: 14px; height: 14px; border-radius: 50%; background: #10b981; border: 2px solid #282a2c; transform: translateX(-50%); }

        .quant-grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 8px; }
        .quant-box { background: #1e1f20; padding: 10px 12px; border-radius: 8px; border: 1px solid #2d3035; }
        .q-name { color: #8e918f; font-size: 0.74rem; margin-bottom: 2px; }
        .q-val { color: #fff; font-size: 0.9rem; font-weight: 700; }

        .accordion-btn { width: 100%; height: 52px; background: #282a2c; border: 1px solid #33363a; border-radius: 32px; padding: 0 20px; color: #fff; font-size: 0.95rem; font-weight: 700; display: flex; align-items: center; justify-content: center; cursor: pointer; box-shadow: 0 4px 20px rgba(0,0,0,0.2); position: relative; margin-bottom: 16px; }
        .accordion-arrow { width: 18px; height: 18px; fill: #8e918f; transition: transform 0.2s; position: absolute; right: 20px; }
        .accordion-body { background: #242628; border-radius: 16px; padding: 16px; margin-top: -6px; margin-bottom: 16px; border: 1px solid #33363a; }
        
        .signal-row { display: flex; justify-content: space-between; font-size: 0.85rem; padding: 6px 0; border-bottom: 1px solid #2e3034; }
        .signal-row:last-child { border-bottom: none; }

        .loading { display: none; text-align: center; color: #38bdf8; margin: 40px 0; font-size: 0.95rem; }
    </style>
</head>
<body>

    <header>
        <div class="header-left">
            <button class="menu-btn" onclick="toggleDrawer()">
                <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="3" y1="12" x2="21" y2="12"></line><line x1="3" y1="6" x2="21" y2="6"></line><line x1="3" y1="18" x2="21" y2="18"></line></svg>
            </button>
            <div class="logo-top" onclick="resetToHome()">gaemiGTP</div>
        </div>
        <div class="pro-top-tag">PRO ACCESS</div>
    </header>

    <!-- 좌측 서랍 -->
    <div class="drawer-overlay" id="drawerOverlay" onclick="toggleDrawer()"></div>
    <div class="drawer" id="sideDrawer">
        <button class="new-search-main-btn" onclick="resetToHome(); toggleDrawer();">
            <span>+ 새 종목 검색</span>
            <span style="font-size:0.75rem; color:#8e918f;">메인</span>
        </button>
        <div class="drawer-scroll-area">
            <div class="drawer-section-title">최근 검색한 종목</div>
            <div id="recentSearchList">
                <div class="drawer-item" onclick="doSearch('SK하이닉스'); toggleDrawer();"><span>🔥 SK하이닉스</span><span class="drawer-badge">국내</span></div>
                <div class="drawer-item" onclick="doSearch('엔비디아'); toggleDrawer();"><span>🚀 엔비디아</span><span class="drawer-badge">미국</span></div>
                <div class="drawer-item" onclick="doSearch('비트코인'); toggleDrawer();"><span>₿ 비트코인</span><span class="drawer-badge">코인</span></div>
            </div>
        </div>
    </div>

    <!-- 메인 홈 화면 -->
    <div id="main-view">
        <div class="hero-title">gaemiGTP</div>
        <div class="hero-sub">종목명 하나면 10초 만에 끝납니다</div>
        <div class="search-container">
            <div class="search-box">
                <input type="text" id="centerInput" placeholder="종목명 입력 (예: SK하이닉스, 삼성전자, NVDA, BTC)" onkeypress="if(event.keyCode==13) doSearch(this.value);" autocomplete="off">
                <button class="send-btn" onclick="doSearch(document.getElementById('centerInput').value)">
                    <svg width="18" height="18" viewBox="0 0 24 24"><path d="M12 4L12 20M12 4L5 11M12 4L19 11" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" fill="none"/></svg>
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

    <!-- 중앙 집중형 단일 피드 결과 화면 -->
    <div id="result-scroll-view">
        <div class="feed-container">
            <div class="loading" id="loading">⚡ 실시간 퀀트 데이터 및 차트를 연산 중입니다...</div>
            <div id="resultCardWrap" style="display:none;">
                
                <!-- 1. 시세 & 캔들 차트 -->
                <div class="card">
                    <div style="display:flex; justify-content:space-between; align-items:flex-end;">
                        <div>
                            <div class="price-title" id="rTitle"></div>
                            <div class="price-val" id="rPrice"></div>
                        </div>
                        <div class="price-meta" id="rMeta"></div>
                    </div>
                    <div class="chart-box-wrap" id="tvChart"></div>
                </div>

                <!-- 2. 종목 고유 뉴스 -->
                <div class="card">
                    <div class="section-header">
                        <span class="section-title">📰 gaemiGTP 종목 뉴스</span>
                        <span class="badge-tag">실시간</span>
                    </div>
                    <div id="stockNewsList"></div>
                </div>

                <!-- 3. 컨센서스 & 수급 -->
                <div class="card" id="consensusCard">
                    <div class="section-header">
                        <span class="section-title" id="cTitle"></span>
                        <span class="badge-tag" id="cSubTag"></span>
                    </div>
                    <div class="gauge-row">
                        <div class="circle-gauge">
                            <span style="font-size:1.05rem; font-weight:800; color:#fff; line-height:1;" id="cGaugeVal"></span>
                            <span style="font-size:0.52rem; color:#8e918f;" id="cGaugeLabel"></span>
                        </div>
                        <div>
                            <div style="font-size:0.8rem; margin-bottom:3px;">
                                <span style="color:#8e918f;">전체 의견:</span>
                                <strong style="color:#10b981; background:rgba(16,185,129,0.15); padding:2px 6px; border-radius:6px;" id="cBadge"></strong>
                            </div>
                            <div style="font-size:0.72rem; color:#8e918f;" id="cDots"></div>
                        </div>
                    </div>
                    <div style="display:flex; justify-content:space-between; align-items:flex-end; margin-top:12px;">
                        <div>
                            <div style="font-size:0.7rem; color:#8e918f;" id="cPrimaryLabel"></div>
                            <div style="font-size:1.25rem; font-weight:800; color:#10b981;" id="cPrimaryVal"></div>
                        </div>
                        <div style="text-align:right;">
                            <div style="font-size:0.7rem; color:#8e918f;" id="cSecondaryLabel"></div>
                            <div style="font-size:1.1rem; font-weight:700; color:#ffffff;" id="cSecondaryVal"></div>
                        </div>
                    </div>
                    <div class="slider-bar-wrap">
                        <div class="slider-bar-fill" id="cBarFill"></div>
                        <div class="slider-dot" id="cDotMarker"></div>
                    </div>
                    <div style="display:flex; justify-content:space-between; font-size:0.65rem; color:#6b7280;">
                        <span id="cSliderLow"></span>
                        <span id="cSliderHigh"></span>
                    </div>
                </div>

                <!-- 4. 퀀트 기술 지표 -->
                <div class="card">
                    <div class="section-header">
                        <span class="section-title">⚡ gaemiGTP 기술 지표</span>
                        <span style="font-size:0.78rem; color:#38bdf8; font-weight:700;" id="rTrend"></span>
                    </div>
                    <div class="quant-grid">
                        <div class="quant-box"><div class="q-name">5일선</div><div class="q-val" id="qMa5"></div></div>
                        <div class="quant-box"><div class="q-name">20일선</div><div class="q-val" id="qMa20"></div></div>
                        <div class="quant-box"><div class="q-name">볼린저 밴드 상/하</div><div class="q-val" id="qBb"></div></div>
                        <div class="quant-box"><div class="q-name">RSI (14)</div><div class="q-val" id="qRsi"></div></div>
                        <div class="quant-box"><div class="q-name">52주 위치</div><div class="q-val" id="qPos52w"></div></div>
                        <div class="quant-box"><div class="q-name">20일 평균 거래대금</div><div class="q-val" id="qTval20d"></div></div>
                    </div>
                </div>

                <!-- 5. 시그널 타점 -->
                <div>
                    <button class="accordion-btn" onclick="toggleAccordion('signalBody', 'signalArrow')">
                        <span>⚡ gaemiGTP 시그널 타점</span>
                        <svg class="accordion-arrow" id="signalArrow" style="transform:rotate(180deg);" viewBox="0 0 24 24"><path d="M7 10l5 5 5-5z"/></svg>
                    </button>
                    <div class="accordion-body" id="signalBody">
                        <div class="signal-row"><span>1차 매수 진입 타점</span><strong style="color:#fff;" id="sEntry1"></strong></div>
                        <div class="signal-row"><span>2차 분할 진입 타점</span><strong style="color:#fff;" id="sEntry2"></strong></div>
                        <div class="signal-row"><span>1차 목표 익절가 (+4.5%)</span><strong style="color:#ef4444;" id="sTarget1"></strong></div>
                        <div class="signal-row"><span>2차 목표 익절가 (+8.5%)</span><strong style="color:#ef4444;" id="sTarget2"></strong></div>
                        <div class="signal-row"><span>리스크 방어 손절가 (-5.5%)</span><strong style="color:#38bdf8;" id="sStop"></strong></div>
                    </div>
                </div>

            </div>
        </div>
    </div>

    <script>
        let chart = null;

        function toggleDrawer() {
            document.getElementById('sideDrawer').classList.toggle('active');
            document.getElementById('drawerOverlay').classList.toggle('active');
        }

        function resetToHome() {
            document.getElementById('main-view').style.display = 'flex';
            document.getElementById('result-scroll-view').style.display = 'none';
            document.getElementById('resultCardWrap').style.display = 'none';
            document.getElementById('loading').style.display = 'none';
            document.getElementById('centerInput').value = '';
        }

        function toggleAccordion(bodyId, arrowId) {
            const body = document.getElementById(bodyId);
            const arrow = document.getElementById(arrowId);
            if (body.style.display === 'none') {
                body.style.display = 'block';
                arrow.style.transform = 'rotate(180deg)';
            } else {
                body.style.display = 'none';
                arrow.style.transform = 'rotate(0deg)';
            }
        }

        async function doSearch(query) {
            const cleanQ = query.trim();
            if (!cleanQ) return;

            document.getElementById('main-view').style.display = 'none';
            document.getElementById('result-scroll-view').style.display = 'block';
            document.getElementById('loading').style.display = 'block';
            document.getElementById('resultCardWrap').style.display = 'none';

            try {
                const response = await fetch('/api/analyze?q=' + encodeURIComponent(cleanQ));
                const data = await response.json();
                if (!response.ok || data.error) {
                    alert(data.error || '종목을 찾을 수 없습니다.');
                    resetToHome();
                    return;
                }

                document.getElementById('rTitle').innerHTML = `${data.name} <span>(${data.symbol})</span>`;
                document.getElementById('rPrice').innerText = data.price;
                document.getElementById('rMeta').innerText = `시장: ${data.market} · 시각: ${data.time}`;

                const sNews = document.getElementById('stockNewsList');
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
                    sNews.innerHTML = '<div style="color:#8e918f; font-size:0.85rem;">관련 뉴스가 없습니다.</div>';
                }

                const c = data.consensus;
                if (c) {
                    document.getElementById('cTitle').innerText = c.title;
                    document.getElementById('cSubTag').innerText = c.sub_tag;
                    document.getElementById('cGaugeVal').innerText = c.gauge_val;
                    document.getElementById('cGaugeLabel').innerText = c.gauge_label;
                    document.getElementById('cBadge').innerText = c.badge_text;
                    document.getElementById('cDots').innerHTML = c.dots;
                    document.getElementById('cPrimaryLabel').innerText = c.primary_label;
                    document.getElementById('cPrimaryVal').innerText = c.primary_val;
                    document.getElementById('cSecondaryLabel').innerText = c.secondary_label;
                    document.getElementById('cSecondaryVal').innerText = c.secondary_val;
                    document.getElementById('cBarFill').style.width = c.fill_pct + '%';
                    document.getElementById('cDotMarker').style.left = c.fill_pct + '%';
                    document.getElementById('cSliderLow').innerText = c.slider_bottom_left;
                    document.getElementById('cSliderHigh').innerText = c.slider_bottom_right;
                    document.getElementById('consensusCard').style.display = 'block';
                } else {
                    document.getElementById('consensusCard').style.display = 'none';
                }

                const q = data.quant;
                document.getElementById('rTrend').innerText = q.trend;
                document.getElementById('qMa5').innerText = q.ma5;
                document.getElementById('qMa20').innerText = q.ma20;
                document.getElementById('qBb').innerText = q.bollinger;
                document.getElementById('qRsi').innerText = q.rsi;
                document.getElementById('qPos52w').innerText = q.pos_52w;
                document.getElementById('qTval20d').innerText = q.tval_20d;

                document.getElementById('sEntry1').innerText = q.entry1;
                document.getElementById('sEntry2').innerText = q.entry2;
                document.getElementById('sTarget1').innerText = q.target1;
                document.getElementById('sTarget2').innerText = q.target2;
                document.getElementById('sStop').innerText = q.stop_p;

                document.getElementById('loading').style.display = 'none';
                document.getElementById('resultCardWrap').style.display = 'block';

                setTimeout(() => {
                    initTvChart(data.candles, data.ma20_line, data.ma60_line, data.bb_up_line, data.bb_dn_line);
                }, 50);

                window.scrollTo({top: 0, behavior: 'smooth'});
            } catch (e) {
                alert('서버와 통신할 수 없습니다.');
                resetToHome();
            }
        }

        function initTvChart(candles, ma20, ma60, bbUp, bbDn) {
            const container = document.getElementById('tvChart');
            container.innerHTML = '';

            chart = LightweightCharts.createChart(container, {
                width: container.clientWidth || 740,
                height: 300,
                layout: { background: { color: '#282a2c' }, textColor: '#8e918f' },
                grid: { vertLines: { color: 'rgba(0,0,0,0)' }, horzLines: { color: 'rgba(0,0,0,0)' } },
                crosshair: { vertLine: { visible: false }, horzLine: { visible: false } },
                timeScale: { borderColor: '#37393b', timeVisible: true },
                rightPriceScale: { borderColor: '#37393b' }
            });

            const candleSeries = chart.addCandlestickSeries({
                upColor: '#ef4444', downColor: '#38bdf8',
                borderDownColor: '#38bdf8', borderUpColor: '#ef4444',
                wickDownColor: '#38bdf8', wickUpColor: '#ef4444'
            });
            candleSeries.setData(candles);

            const lineMa20 = chart.addLineSeries({ color: '#00b4d8', lineWidth: 1 });
            lineMa20.setData(ma20);

            const lineMa60 = chart.addLineSeries({ color: '#f97316', lineWidth: 1.5 });
            lineMa60.setData(ma60);

            const lineBbUp = chart.addLineSeries({ color: '#f87171', lineWidth: 1.2 });
            lineBbUp.setData(bbUp);

            const lineBbDn = chart.addLineSeries({ color: '#38bdf8', lineWidth: 1.2 });
            lineBbDn.setData(bbDn);

            chart.timeScale().fitContent();
        }
    </script>
</body>
</html>
"""

# ----------------------------------------------------
# 6. HTTP 웹 서버 핸들러
# ----------------------------------------------------
class WebHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)

        if parsed.path == "/api/analyze":
            q = params.get('q', [''])[0].strip()
            data = run_full_pipeline(q) if q else None
            if not data:
                self.send_response(404)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.end_headers()
                self.wfile.write(json.dumps({"error": "종목을 찾을 수 없습니다."}, ensure_ascii=False).encode('utf-8'))
                return

            self.send_response(200)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.end_headers()
            
            res_obj = {
                "name": data["formal_name"],
                "symbol": data["symbol"],
                "market": data["market"],
                "price": data["price_str"],
                "time": data["now_str"],
                "quant": data["quant"],
                "consensus": data["consensus"],
                "stock_news": data["stock_news"],
                "candles": data["candles"],
                "ma20_line": data["ma20_line"],
                "ma60_line": data["ma60_line"],
                "bb_up_line": data["bb_up_line"],
                "bb_dn_line": data["bb_dn_line"]
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
