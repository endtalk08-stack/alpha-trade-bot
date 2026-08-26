import os
import re
import io
import gc
import json
import time
import urllib.parse
import xml.etree.ElementTree as ET
import html
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from http.server import HTTPServer, BaseHTTPRequestHandler

import requests
import pandas as pd
import numpy as np

# ----------------------------------------------------
# 0. 고성능 네트워크 세션 (TCP 커넥션 풀 재사용)
# ----------------------------------------------------
HTTP_SESSION = requests.Session()
HTTP_SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Connection": "keep-alive"
})

# ----------------------------------------------------
# 1. 기본 설정 및 종목 매핑 (보안 환경변수 유지)
# ----------------------------------------------------
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
TELEGRAM_BOT_USERNAME = os.environ.get("TELEGRAM_BOT_USERNAME", "gaemigtp_bot").strip()
DART_API_KEY = os.environ.get("DART_API_KEY", "").strip()

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
# 2. 데이터 크롤링 모듈 (DART 전자공시 포함)
# ----------------------------------------------------
def fetch_dart_disclosures(stock_code, is_krw=True, limit=5):
    if not is_krw or not stock_code or not stock_code.isdigit():
        return []

    disclosures = []
    try:
        url = f"https://m.stock.naver.com/api/stock/{stock_code}/disclosure?page=1&pageSize={limit}"
        res = HTTP_SESSION.get(url, timeout=2.0).json()
        items = res.get("items", []) or res.get("disclosures", [])
        for it in items[:limit]:
            title = it.get("reportTitle") or it.get("title", "")
            date_raw = str(it.get("rceptDt") or it.get("date", ""))[:10].replace("-", ".")
            art_id = it.get("articleId") or it.get("rceptNo", "")
            link = f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={art_id}" if art_id else "#"

            tag_type = "neutral"
            tag_name = "공시"
            if any(k in title for k in ["공급계약", "수주", "자기주식취득", "무상증자", "영업실적", "주주우대", "특허"]):
                tag_type = "positive"
                tag_name = "호재"
            elif any(k in title for k in ["유상증자", "전환사채", "신주인수권", "감자", "횡령", "배임", "관리종목", "상장폐지", "불성실공시"]):
                tag_type = "negative"
                tag_name = "악재주의"
            elif any(k in title for k in ["최대주주변경", "주식매수선택권", "주요사항보고서"]):
                tag_type = "warning"
                tag_name = "변동"

            disclosures.append({
                "title": title,
                "date": date_raw,
                "link": link,
                "tag_type": tag_type,
                "tag_name": tag_name,
                "flr_nm": it.get("submitter", "DART")
            })
    except Exception:
        pass

    return disclosures

def get_realtime_usdkrw():
    try:
        url = "https://query1.finance.yahoo.com/v8/finance/chart/KRW=X?range=1d&interval=1m"
        res = HTTP_SESSION.get(url, timeout=1.5).json()
        meta = res.get("chart", {}).get("result", [{}])[0].get("meta", {})
        rate = meta.get("regularMarketPrice", 0.0)
        prev_close = meta.get("chartPreviousClose", meta.get("previousClose", rate))
        change_rate = ((rate - prev_close) / prev_close) * 100 if prev_close > 0 else 0.0
        if rate > 500:
            return float(rate), f"{change_rate:+.2f}%", change_rate >= 0
    except Exception:
        pass
    return 1380.0, "0.00%", True

def get_market_overview():
    indices = [
        {"name": "KOSPI", "sym": "^KS11"},
        {"name": "NASDAQ", "sym": "^IXIC"},
        {"name": "BTC", "sym": "BTC-USD"}
    ]
    overview = []
    for item in indices:
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(item['sym'])}?range=2d&interval=1d"
            res = HTTP_SESSION.get(url, timeout=1.8).json()
            result = res.get("chart", {}).get("result", [])
            if result:
                meta = result[0].get("meta", {})
                curr_p = meta.get("regularMarketPrice", 0.0)
                prev_close = meta.get("chartPreviousClose", meta.get("previousClose", curr_p))
                change_rate = ((curr_p - prev_close) / prev_close) * 100 if prev_close > 0 else 0.0
                p_str = f"${curr_p:,.2f}" if "BTC" in item["name"] else f"{curr_p:,.2f}"
                overview.append({
                    "name": item["name"],
                    "price": p_str,
                    "rate": f"{change_rate:+.2f}%",
                    "up": change_rate >= 0
                })
        except Exception:
            overview.append({"name": item["name"], "price": "-", "rate": "-", "up": True})
    
    rate, rate_str, rate_up = get_realtime_usdkrw()
    overview.append({
        "name": "USD/KRW",
        "price": f"{rate:,.1f}원",
        "rate": rate_str,
        "up": rate_up
    })
    return overview

def find_stock_code(query):
    clean_q = query.strip().replace(" ", "").upper()
    if clean_q.isdigit() and len(clean_q) == 6:
        return clean_q, clean_q

    for name, code in DOMESTIC_FAST_MAP.items():
        if name.replace(" ", "").upper() == clean_q:
            return code, name

    try:
        url = f"https://m.stock.naver.com/front-api/search/autoComplete?query={urllib.parse.quote(query.strip())}&target=stock"
        res = HTTP_SESSION.get(url, timeout=1.8).json()
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
        res = HTTP_SESSION.get(url, timeout=1.8).json()
        datas = res.get("datas", [])
        if datas:
            p = str(datas[0].get("closePrice", "")).replace(",", "").strip()
            cr = str(datas[0].get("fluctuationsRatio", "0.0")).replace("%", "").strip()
            if p and p.replace(".", "").isdigit():
                return float(p), f"{int(float(p)):,} 원", f"{float(cr):+.2f}%", float(cr) >= 0
    except Exception:
        pass
    return None, None, None, True

def fetch_domestic_candles(code, anchor_price=None):
    df_result = None
    for suffix in [".KS", ".KQ"]:
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{code}{suffix}?range=5y&interval=1d"
            res = HTTP_SESSION.get(url, timeout=2.0).json()
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
        res = HTTP_SESSION.get(url, timeout=2.0).json()
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
                prev_close = float(df['Close'].iloc[-2]) if len(df) > 1 else curr_p
                cr = ((curr_p - prev_close) / prev_close) * 100 if prev_close > 0 else 0.0
                return curr_p, f"${curr_p:,.2f}", f"{cr:+.2f}%", cr >= 0, df
    except Exception:
        pass
    return None, None, None, True, None

def fetch_crypto_candles(symbol):
    clean_sym = symbol.upper().replace("-", "")
    if not clean_sym.endswith("USDT") and not clean_sym.endswith("USD"):
        clean_sym += "USDT"
    try:
        url = f"https://api.binance.com/api/v3/klines?symbol={clean_sym}&interval=1d&limit=1000"
        res = HTTP_SESSION.get(url, timeout=2.0).json()
        if isinstance(res, list) and len(res) > 5:
            records = [{
                "Date": pd.to_datetime(int(x[0]), unit='ms'),
                "Open": float(x[1]), "High": float(x[2]), "Low": float(x[3]), "Close": float(x[4]), "Volume": float(x[5])
            } for x in res]
            df = pd.DataFrame(records).set_index("Date").sort_index()
            curr_p = float(df['Close'].iloc[-1])
            prev_close = float(df['Close'].iloc[-2]) if len(df) > 1 else curr_p
            cr = ((curr_p - prev_close) / prev_close) * 100 if prev_close > 0 else 0.0
            return curr_p, f"${curr_p:,.2f} USDT", f"{cr:+.2f}%", cr >= 0, df
    except Exception:
        pass
    return None, None, None, True, None

def fetch_rss_news(query_keyword, limit=5):
    headlines = []
    clean_k = re.sub(r'\(.*?\)', '', query_keyword).replace("USDT", "").strip()
    url = f"https://news.google.com/rss/search?q={urllib.parse.quote(clean_k)}&hl=ko&gl=KR&ceid=KR:ko"
    try:
        r = HTTP_SESSION.get(url, timeout=2.5)
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
                    
                    impact = "neutral"
                    impact_text = "중립"
                    if any(w in title_text for w in ["급등", "호재", "수주", "상승", "돌파", "사상최대", "특허", "체결"]):
                        impact = "positive"
                        impact_text = "긍정"
                    elif any(w in title_text for w in ["하락", "급락", "악재", "손실", "적자", "경고", "압수수색", "소송"]):
                        impact = "negative"
                        impact_text = "부정"

                    headlines.append({
                        "title": title_text,
                        "media": media_name,
                        "pubDate": pub_text[:16] if len(pub_text) > 16 else pub_text,
                        "link": link_text,
                        "impact": impact,
                        "impact_text": impact_text
                    })
    except Exception:
        pass
    return headlines

# ----------------------------------------------------
# 3. 기술적 지표 및 퀀트 분석 연산 (100% 원본 보존)
# ----------------------------------------------------
def calculate_quant_factors(raw_price, df, symbol="", is_krw=True):
    if df is None or len(df) < 10:
        return None

    round_fn = (lambda v: f"{int(round(v, 0)):,} 원") if is_krw else (lambda v: f"${round(float(v), 2):,.2f}")
    closes = df['Close']
    volumes = df['Volume']
    n_bars = len(df)

    ma5 = round_fn(closes.tail(5).mean())
    ma20_val = float(closes.tail(20).mean())
    ma20 = round_fn(ma20_val)
    ma60_val = float(closes.tail(60).mean()) if n_bars >= 60 else None
    ma60 = round_fn(ma60_val) if ma60_val else "-"
    ma120 = round_fn(closes.tail(120).mean()) if n_bars >= 120 else "-"

    bb_period = min(60, n_bars)
    bb_mid = closes.tail(bb_period).mean()
    bb_std = closes.tail(bb_period).std()
    bb_upper = bb_mid + (bb_std * 2)
    bb_lower = bb_mid - (bb_std * 2)
    bb_str = f"{round_fn(bb_upper)} / {round_fn(bb_lower)}"

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

    delta = closes.diff()
    gain = delta.where(delta > 0, 0.0).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0.0)).rolling(14).mean()
    rs = gain / loss
    rsi_series = 100 - (100 / (1 + rs))
    rsi_val = f"{float(rsi_series.iloc[-1]):.2f}" if not np.isnan(rsi_series.iloc[-1]) else "50.0"

    high_low = df['High'] - df['Low']
    atr_raw = float(high_low.tail(14).mean())
    atr_val = round_fn(atr_raw)

    v_mean5 = volumes.tail(5).mean()
    v_curr = volumes.iloc[-1]
    vol_ratio = f"{((v_curr / v_mean5) * 100):.1f}%" if v_mean5 > 0 else "100.0%"

    if ma60_val and raw_price > ma20_val > ma60_val:
        trend = "상승 추세"
        ai_opinion = "상승 우위"
        ai_badge = "positive"
        guide_text = f"20일선 지지 안착 확인 시 분할 착수하며, 변동폭({atr_val}) 수준의 눌림목에서 분할 매수 후 전고점 돌파 시 1차 익절 집행"
    elif ma60_val and raw_price < ma20_val < ma60_val:
        trend = "하락 추세"
        ai_opinion = "하락 우위"
        ai_badge = "negative"
        guide_text = f"RSI({rsi_val}) 침체권 매물 소화 확인 후 보수적 접근하며, 20일선 저항 돌파 실패 시 리스크 관리 필수"
    else:
        trend = "박스권 횡보"
        ai_opinion = "중립"
        ai_badge = "neutral"
        guide_text = f"박스권 상하단 밴드 내 공방 국면이므로 하단 지지 확인 시 진입하고, 저항선 도달 시 비중 축소 권장"

    entry1 = round_fn(raw_price * 0.985)
    entry2 = round_fn(raw_price * 0.965)
    target1 = round_fn(raw_price * 1.045)
    target2 = round_fn(raw_price * 1.085)
    stop_p = round_fn(raw_price * 0.945)

    return {
        "trend": trend,
        "ai_opinion": ai_opinion,
        "ai_badge": ai_badge,
        "ma5": ma5, "ma20": ma20, "ma60": ma60, "ma120": ma120,
        "bollinger": bb_str,
        "macd": macd_str,
        "rsi": rsi_val,
        "atr": atr_val,
        "vol_ratio": vol_ratio,
        "support": entry2,
        "current": round_fn(raw_price),
        "resistance": target2,
        "entry1": entry1,
        "entry2": entry2,
        "target1": target1,
        "target2": target2,
        "stop_p": stop_p,
        "guide_text": guide_text
    }

# ----------------------------------------------------
# 4. 전체 통합 파이프라인
# ----------------------------------------------------
def run_full_pipeline(query):
    clean_q = query.strip().replace(" ", "").upper()
    market_name = "국내주식"
    is_krw = True
    chg_str = "+0.00%"
    is_up = True

    if clean_q.endswith("USDT") or clean_q in CRYPTO_MAP or any(c in clean_q for c in ["BTC", "ETH", "코인", "비트"]):
        symbol = CRYPTO_MAP.get(clean_q, clean_q if clean_q.endswith("USDT") else f"{clean_q}USDT")
        formal_name = clean_q
        raw_price, price_str, chg_str, is_up, df = fetch_crypto_candles(symbol)
        market_name = "암호화폐"
        is_krw = False
    elif clean_q in US_STOCK_MAP:
        symbol = US_STOCK_MAP[clean_q]
        formal_name = query.strip()
        raw_price, price_str, chg_str, is_up, df = fetch_overseas_candles(symbol)
        market_name = "미국주식"
        is_krw = False
    elif any(c.isupper() for c in query) and not any('\uac00' <= char <= '\ud7a3' for char in query):
        symbol = query.upper().strip()
        formal_name = symbol
        raw_price, price_str, chg_str, is_up, df = fetch_overseas_candles(symbol)
        market_name = "미국주식"
        is_krw = False
    else:
        code, name = find_stock_code(query)
        if not code:
            return None
        symbol, formal_name = code, name
        raw_price, price_str, chg_str, is_up = get_naver_realtime_price(symbol)
        df = fetch_domestic_candles(symbol, anchor_price=raw_price)
        if raw_price is None and df is not None and not df.empty:
            raw_price = float(df['Close'].iloc[-1])
            prev_p = float(df['Close'].iloc[-2]) if len(df) > 1 else raw_price
            cr = ((raw_price - prev_p) / prev_p) * 100 if prev_p > 0 else 0.0
            price_str = f"{int(raw_price):,} 원"
            chg_str = f"{cr:+.2f}%"
            is_up = cr >= 0

    if raw_price is None or df is None:
        return None

    with ThreadPoolExecutor(max_workers=5) as executor:
        f_news = executor.submit(fetch_rss_news, formal_name, 5)
        f_dart = executor.submit(fetch_dart_disclosures, symbol, is_krw, 4)
        stock_news = f_news.result()
        dart_disclosures = f_dart.result()

    quant = calculate_quant_factors(raw_price, df, symbol=symbol, is_krw=is_krw)
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
        if not np.isnan(row['MA20']):
            ma20_line.append({"time": t_str, "value": float(row['MA20'])})
        if not np.isnan(row['MA60']):
            ma60_line.append({"time": t_str, "value": float(row['MA60'])})
        if not np.isnan(row['BB_UP']):
            bb_up_line.append({"time": t_str, "value": float(row['BB_UP'])})
        if not np.isnan(row['BB_DN']):
            bb_dn_line.append({"time": t_str, "value": float(row['BB_DN'])})

    return {
        "name": formal_name,
        "symbol": symbol,
        "market": market_name,
        "price": price_str,
        "chg_str": chg_str or "+0.00%",
        "is_up": is_up,
        "time": now_str,
        "quant": quant,
        "stock_news": stock_news,
        "dart_disclosures": dart_disclosures,
        "candles": candles,
        "ma20_line": ma20_line,
        "ma60_line": ma60_line,
        "bb_up_line": bb_up_line,
        "bb_dn_line": bb_dn_line
    }

# ----------------------------------------------------
# 5. 텔레그램 봇 실시간 인터랙션 엔진
# ----------------------------------------------------
def send_telegram_msg(chat_id, text):
    if not TELEGRAM_TOKEN:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        HTTP_SESSION.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"}, timeout=2.5)
    except Exception:
        pass

def telegram_bot_worker():
    if not TELEGRAM_TOKEN:
        return
    offset = 0
    while True:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates?offset={offset}&timeout=20"
            res = HTTP_SESSION.get(url, timeout=25).json()
            if res.get("ok"):
                for upd in res.get("result", []):
                    offset = upd["update_id"] + 1
                    msg = upd.get("message", {})
                    chat_id = msg.get("chat", {}).get("id")
                    text = msg.get("text", "").strip()

                    if not chat_id or not text:
                        continue

                    if text.startswith("/start"):
                        parts = text.split()
                        if len(parts) > 1:
                            target_q = parts[1].strip()
                            data = run_full_pipeline(target_q)
                            if data:
                                reply = f"🔔 <b>{data['name']}({data['symbol']})</b> 알림 등록 완료!\n\n" \
                                        f"현재가: <b>{data['price']}</b> ({data['chg_str']})\n" \
                                        f"AI 분석: <b>{data['quant']['ai_opinion']}</b>\n" \
                                        f"1차 진입가: {data['quant']['entry1']}\n" \
                                        f"저항선: {data['quant']['resistance']}\n" \
                                        f"손절 방어선: {data['quant']['stop_p']}"
                                send_telegram_msg(chat_id, reply)
                            else:
                                send_telegram_msg(chat_id, f"gaemiGTP 알림봇에 오신 것을 환영합니다!\n<code>/삼성전자</code> 또는 <code>/BTC</code> 형태로 종목명을 입력하시면 실시간 분석을 받아보실 수 있습니다.")
                        else:
                            send_telegram_msg(chat_id, f"gaemiGTP 텔레그램 시그널 알림봇입니다!\n<code>/SK하이닉스</code>, <code>/NVDA</code>, <code>/BTC</code> 형태로 종목명을 입력해 주세요.")
                    else:
                        query_clean = text.lstrip("/")
                        data = run_full_pipeline(query_clean)
                        if data:
                            reply = f"📊 <b>gaemiGTP 분석: {data['name']}</b> ({data['symbol']})\n" \
                                    f"━━━━━━━━━━━━━━━━━━━━\n" \
                                    f"현재가: <b>{data['price']}</b> ({data['chg_str']})\n" \
                                    f"AI 분석: <b>{data['quant']['ai_opinion']}</b> ({data['quant']['trend']})\n" \
                                    f"━━━━━━━━━━━━━━━━━━━━\n" \
                                    f"🎯 <b>핵심 가격</b>\n" \
                                    f"• 지지선: {data['quant']['support']}\n" \
                                    f"• 저항선: {data['quant']['resistance']}\n" \
                                    f"• 손절선: {data['quant']['stop_p']}\n\n" \
                                    f"💡 <i>{data['quant']['guide_text']}</i>"
                            send_telegram_msg(chat_id, reply)
                        else:
                            send_telegram_msg(chat_id, f"❌ '{query_clean}' 종목을 찾을 수 없습니다.")
        except Exception:
            time.sleep(2)

if TELEGRAM_TOKEN:
    threading.Thread(target=telegram_bot_worker, daemon=True).start()

# ----------------------------------------------------
# 6. 웹 프론트엔드 UI (현대적 금융 플랫폼 디자인 전면 적용)
# ----------------------------------------------------
HTML_PAGE = """<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>gaemiGTP - 오늘 시장, 어디부터 볼까?</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Pretendard:wght@400;500;600;700;800&display=swap" rel="stylesheet">
    <script src="https://unpkg.com/lightweight-charts@4.1.1/dist/lightweight-charts.standalone.production.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2"></script>
    <style>
        :root {
            --bg-body: #F7F8FA;
            --bg-card: #FFFFFF;
            --text-main: #171717;
            --text-sub: #6B7280;
            --text-muted: #9CA3AF;
            --color-up: #16A34A;
            --color-down: #EF4444;
            --color-neutral: #6B7280;
            --color-primary: #111827;
            --border-line: #E5E7EB;
            --border-focus: #111827;
            --card-shadow: 0 1px 3px rgba(0,0,0,0.05), 0 1px 2px rgba(0,0,0,0.03);
            --radius-lg: 16px;
            --radius-md: 12px;
            --radius-sm: 8px;
        }

        * { box-sizing: border-box; margin: 0; padding: 0; font-family: 'Pretendard', -apple-system, BlinkMacSystemFont, system-ui, sans-serif; }
        body { background-color: var(--bg-body); color: var(--text-main); min-height: 100vh; overflow-x: hidden; line-height: 1.5; }

        /* 상단 네비게이션 */
        header {
            height: 64px;
            background: #FFFFFF;
            border-bottom: 1px solid var(--border-line);
            position: sticky;
            top: 0;
            z-index: 50;
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 0 24px;
        }
        .nav-left { display: flex; align-items: center; gap: 32px; }
        .logo { font-size: 1.25rem; font-weight: 800; color: var(--color-primary); cursor: pointer; display: flex; align-items: center; gap: 6px; }
        .nav-menu { display: flex; align-items: center; gap: 20px; list-style: none; }
        .nav-item { font-size: 0.95rem; font-weight: 600; color: var(--text-sub); cursor: pointer; transition: color 0.15s; }
        .nav-item:hover, .nav-item.active { color: var(--color-primary); }
        .nav-right { display: flex; align-items: center; gap: 12px; }
        .btn-nav-text { font-size: 0.9rem; font-weight: 600; color: var(--text-sub); background: none; border: none; cursor: pointer; padding: 6px 12px; }
        .btn-nav-login { font-size: 0.9rem; font-weight: 600; color: #FFFFFF; background: var(--color-primary); border: none; border-radius: var(--radius-sm); padding: 7px 16px; cursor: pointer; }
        .mobile-menu-btn { display: none; background: none; border: none; font-size: 1.5rem; cursor: pointer; color: var(--color-primary); }

        /* 컨테이너 */
        .container { max-width: 1040px; margin: 0 auto; padding: 32px 20px 80px; }

        /* 메인 히어로 */
        .hero-section { text-align: center; padding: 24px 0 36px; }
        .hero-title { font-size: 2.2rem; font-weight: 800; letter-spacing: -0.5px; color: var(--color-primary); margin-bottom: 8px; }
        .hero-sub { font-size: 1.05rem; color: var(--text-sub); margin-bottom: 28px; word-break: keep-all; }

        /* 검색창 */
        .search-wrap { max-width: 680px; margin: 0 auto; position: relative; }
        .search-box {
            display: flex;
            align-items: center;
            background: #FFFFFF;
            border: 2px solid var(--border-line);
            border-radius: 36px;
            padding: 8px 10px 8px 24px;
            box-shadow: 0 4px 16px rgba(0,0,0,0.04);
            transition: border-color 0.2s;
        }
        .search-box:focus-within { border-color: var(--border-focus); }
        .search-box input { flex: 1; border: none; outline: none; font-size: 1.05rem; font-weight: 500; color: var(--text-main); }
        .search-box input::placeholder { color: var(--text-muted); }
        .btn-search {
            background: var(--color-primary);
            color: #FFFFFF;
            border: none;
            border-radius: 28px;
            padding: 12px 24px;
            font-size: 0.95rem;
            font-weight: 700;
            cursor: pointer;
            transition: opacity 0.15s;
        }
        .btn-search:hover { opacity: 0.9; }

        .popular-tags { display: flex; align-items: center; justify-content: center; gap: 8px; margin-top: 16px; flex-wrap: wrap; }
        .tag-btn { background: #FFFFFF; border: 1px solid var(--border-line); border-radius: 20px; padding: 6px 14px; font-size: 0.85rem; font-weight: 500; color: var(--text-sub); cursor: pointer; transition: all 0.15s; }
        .tag-btn:hover { border-color: var(--border-focus); color: var(--color-primary); }

        /* 공통 섹션 & 카드 */
        .section-header { margin: 36px 0 16px; display: flex; align-items: center; justify-content: space-between; }
        .section-title { font-size: 1.25rem; font-weight: 800; color: var(--color-primary); display: flex; align-items: center; gap: 8px; }
        .card { background: var(--bg-card); border-radius: var(--radius-lg); padding: 20px; border: 1px solid var(--border-line); box-shadow: var(--card-shadow); }

        /* 오늘의 시장 그리드 */
        .market-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; }
        .market-card { background: #FFFFFF; border: 1px solid var(--border-line); border-radius: var(--radius-md); padding: 16px; text-align: left; }
        .market-card .name { font-size: 0.85rem; font-weight: 600; color: var(--text-sub); margin-bottom: 4px; }
        .market-card .val { font-size: 1.25rem; font-weight: 800; color: var(--color-primary); }
        .market-card .rate { font-size: 0.85rem; font-weight: 700; margin-top: 2px; }

        .val-up { color: var(--color-up); }
        .val-down { color: var(--color-down); }

        /* 2열 배치 레이아웃 */
        .grid-2col { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }

        /* TOP 5 리스트 */
        .top-list { display: flex; flex-direction: column; gap: 12px; }
        .top-item { display: flex; align-items: flex-start; gap: 14px; padding: 12px; border-radius: var(--radius-sm); background: #F9FAFB; border: 1px solid var(--border-line); }
        .top-num { font-size: 1.1rem; font-weight: 800; color: var(--text-muted); line-height: 1; }
        .top-content .topic { font-size: 0.95rem; font-weight: 700; color: var(--color-primary); }
        .top-content .desc { font-size: 0.85rem; color: var(--text-sub); margin-top: 2px; }

        /* 주목 종목 카드 */
        .stock-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; }
        .stock-card { background: #FFFFFF; border: 1px solid var(--border-line); border-radius: var(--radius-md); padding: 16px; cursor: pointer; transition: border-color 0.15s; }
        .stock-card:hover { border-color: var(--border-focus); }
        .stock-card .s-name { font-size: 1rem; font-weight: 700; color: var(--color-primary); }
        .stock-card .s-price { font-size: 1.15rem; font-weight: 800; margin-top: 6px; }
        .stock-card .s-badge { display: inline-block; font-size: 0.75rem; font-weight: 700; padding: 2px 8px; border-radius: 12px; margin-top: 8px; }

        /* AI vs 개미 */
        .vs-card { display: flex; align-items: center; justify-content: space-between; padding: 20px; background: #FFFFFF; border: 1px solid var(--border-line); border-radius: var(--radius-md); margin-bottom: 10px; }
        .vs-side { text-align: center; flex: 1; }
        .vs-divider { font-weight: 800; color: var(--text-muted); font-size: 1.1rem; padding: 0 16px; }

        /* 미국 수혜주 */
        .link-flow { display: flex; align-items: center; justify-content: space-between; padding: 16px; background: #F9FAFB; border-radius: var(--radius-md); border: 1px solid var(--border-line); margin-bottom: 10px; }

        /* 뉴스 카드 */
        .news-item { padding: 14px 0; border-bottom: 1px solid var(--border-line); text-decoration: none; display: block; }
        .news-item:last-child { border-bottom: none; }
        .news-header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 4px; }
        .news-title { font-size: 0.95rem; font-weight: 600; color: var(--text-main); }
        .news-title:hover { text-decoration: underline; }
        .news-meta { font-size: 0.8rem; color: var(--text-sub); }
        .badge-impact { font-size: 0.7rem; font-weight: 700; padding: 2px 6px; border-radius: 4px; }
        .badge-positive { background: #DCFCE7; color: #16A34A; }
        .badge-negative { background: #FEE2E2; color: #EF4444; }
        .badge-neutral { background: #F3F4F6; color: #6B7280; }

        /* 종목 상세 분석 뷰 */
        #detail-view { display: none; }
        .detail-header { display: flex; align-items: flex-start; justify-content: space-between; margin-bottom: 20px; }
        .detail-title { font-size: 1.8rem; font-weight: 800; color: var(--color-primary); }
        .detail-symbol { font-size: 1rem; color: var(--text-sub); font-weight: 500; margin-left: 6px; }
        .detail-price { font-size: 2rem; font-weight: 800; }
        .detail-meta { font-size: 0.85rem; color: var(--text-sub); margin-top: 4px; }

        .quant-spec-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; margin-top: 14px; }
        .spec-box { background: #F9FAFB; border: 1px solid var(--border-line); border-radius: var(--radius-sm); padding: 12px; }
        .spec-label { font-size: 0.8rem; color: var(--text-sub); font-weight: 500; }
        .spec-val { font-size: 1rem; font-weight: 700; color: var(--color-primary); margin-top: 2px; }

        /* 투표 & 댓글 */
        .vote-wrap { display: flex; gap: 10px; margin-top: 14px; }
        .vote-btn { flex: 1; padding: 12px; border-radius: var(--radius-md); border: 1px solid var(--border-line); background: #FFFFFF; font-size: 0.95rem; font-weight: 700; cursor: pointer; transition: all 0.15s; }
        .vote-btn.active-long { background: #DCFCE7; border-color: #16A34A; color: #16A34A; }
        .vote-btn.active-short { background: #FEE2E2; border-color: #EF4444; color: #EF4444; }

        .comment-input-box { display: flex; gap: 8px; margin-top: 14px; }
        .comment-input-box input { flex: 1; border: 1px solid var(--border-line); border-radius: var(--radius-md); padding: 10px 14px; font-size: 0.9rem; outline: none; }
        .comment-input-box button { background: var(--color-primary); color: #FFFFFF; border: none; border-radius: var(--radius-md); padding: 0 18px; font-weight: 700; cursor: pointer; }

        .comment-item { padding: 10px 0; border-bottom: 1px solid var(--border-line); font-size: 0.88rem; }
        .comment-item:last-child { border-bottom: none; }
        .comment-head { display: flex; justify-content: space-between; font-size: 0.75rem; color: var(--text-sub); margin-bottom: 2px; }
        .comment-nick { font-weight: 700; color: var(--color-primary); }

        .tg-banner {
            background: #229ED9;
            color: #FFFFFF;
            border-radius: var(--radius-md);
            padding: 14px 20px;
            display: flex;
            align-items: center;
            justify-content: space-between;
            text-decoration: none;
            font-weight: 700;
            margin-top: 16px;
        }

        .loading-box { text-align: center; padding: 60px 0; font-weight: 600; color: var(--text-sub); }

        /* 모바일 반응형 */
        @media (max-width: 768px) {
            .nav-menu, .btn-nav-text, .btn-nav-login { display: none; }
            .mobile-menu-btn { display: block; }
            .hero-title { font-size: 1.6rem; }
            .market-grid { grid-template-columns: repeat(2, 1fr); }
            .grid-2col { grid-template-columns: 1fr; }
            .stock-grid { grid-template-columns: 1fr; }
            .quant-spec-grid { grid-template-columns: 1fr 1fr; }
        }
    </style>
</head>
<body>

    <!-- 상단 네비게이션 -->
    <header>
        <div class="nav-left">
            <div class="logo" onclick="resetToHome()">🐜 gaemiGTP</div>
            <ul class="nav-menu">
                <li class="nav-item active" onclick="resetToHome()">시장</li>
                <li class="nav-item" onclick="document.getElementById('centerInput').focus()">종목분석</li>
                <li class="nav-item" onclick="scrollToSection('sec-vs')">AI vs 개미</li>
                <li class="nav-item" onclick="scrollToSection('sec-news')">뉴스</li>
                <li class="nav-item" onclick="scrollToSection('sec-community')">커뮤니티</li>
            </ul>
        </div>
        <div class="nav-right">
            <button class="btn-nav-text">알림</button>
            <button class="btn-nav-login">로그인</button>
            <button class="mobile-menu-btn">☰</button>
        </div>
    </header>

    <div class="container">
        
        <!-- 메인 홈 뷰 -->
        <div id="home-view">
            
            <!-- 메인 히어로 -->
            <section class="hero-section">
                <h1 class="hero-title">오늘 시장, 어디부터 볼까?</h1>
                <p class="hero-sub">복잡한 투자 정보를 개미에게 가장 쉽게 보여드립니다.</p>
                
                <div class="search-wrap">
                    <div class="search-box">
                        <input type="text" id="centerInput" placeholder="🔍 종목명을 입력하세요 (예: 삼성전자, NVDA, BTC)" onkeypress="if(event.keyCode==13) doSearch(this.value);" autocomplete="off">
                        <button class="btn-search" onclick="doSearch(document.getElementById('centerInput').value)">분석하기</button>
                    </div>
                </div>

                <div class="popular-tags">
                    <button class="tag-btn" onclick="doSearch('삼성전자')">삼성전자</button>
                    <button class="tag-btn" onclick="doSearch('SK하이닉스')">SK하이닉스</button>
                    <button class="tag-btn" onclick="doSearch('엔비디아')">엔비디아</button>
                    <button class="tag-btn" onclick="doSearch('비트코인')">비트코인</button>
                </div>
            </section>

            <!-- 오늘의 시장 -->
            <div class="section-header">
                <h2 class="section-title">📊 오늘의 시장</h2>
            </div>
            <div class="market-grid" id="marketGridArea">
                <div class="market-card"><div class="name">KOSPI</div><div class="val" id="mKospiVal">-</div><div class="rate" id="mKospiRate">-</div></div>
                <div class="market-card"><div class="name">NASDAQ</div><div class="val" id="mNasVal">-</div><div class="rate" id="mNasRate">-</div></div>
                <div class="market-card"><div class="name">BTC</div><div class="val" id="mBtcVal">-</div><div class="rate" id="mBtcRate">-</div></div>
                <div class="market-card"><div class="name">USD/KRW</div><div class="val" id="mUsdVal">-</div><div class="rate" id="mUsdRate">-</div></div>
            </div>

            <!-- 2열 섹션: 시장 TOP 5 & 오늘의 주목 종목 -->
            <div class="grid-2col" style="margin-top: 36px;">
                
                <!-- 오늘 시장을 움직이는 TOP 5 -->
                <div>
                    <div class="section-header" style="margin-top: 0;">
                        <h2 class="section-title">🔥 오늘 시장을 움직이는 TOP 5</h2>
                    </div>
                    <div class="card top-list">
                        <div class="top-item">
                            <div class="top-num">01</div>
                            <div class="top-content"><div class="topic">반도체 / AI HBM</div><div class="desc">글로벌 빅테크 AI 인프라 투자 지속 및 HBM 수요 기대</div></div>
                        </div>
                        <div class="top-item">
                            <div class="top-num">02</div>
                            <div class="top-content"><div class="topic">미국 금리 및 달러 환율</div><div class="desc">연준 통화정책 경로 및 달러/원 환율 변동성 주시</div></div>
                        </div>
                        <div class="top-item">
                            <div class="top-num">03</div>
                            <div class="top-content"><div class="topic">2차전지 / 전기차</div><div class="desc">북미/유럽 전기차 수요 회복 및 리튬 가격 바닥권 탐색</div></div>
                        </div>
                        <div class="top-item">
                            <div class="top-num">04</div>
                            <div class="top-content"><div class="topic">바이오 / 기술수출</div><div class="desc">글로벌 임상 결과 발표 및 대형 기술이전(L/O) 모멘텀</div></div>
                        </div>
                        <div class="top-item">
                            <div class="top-num">05</div>
                            <div class="top-content"><div class="topic">암호화폐 온체인 수급</div><div class="desc">비트코인 ETF 자금 유출입 및 선물 펀딩비 밴드 안정화</div></div>
                        </div>
                    </div>
                </div>

                <!-- 오늘의 주목 종목 -->
                <div>
                    <div class="section-header" style="margin-top: 0;">
                        <h2 class="section-title">⭐ 오늘의 주목 종목</h2>
                    </div>
                    <div class="card stock-grid" style="grid-template-columns: 1fr;">
                        <div class="stock-card" onclick="doSearch('SK하이닉스')">
                            <div style="display:flex; justify-content:space-between; align-items:center;">
                                <div class="s-name">SK하이닉스</div>
                                <span class="badge-impact badge-positive">AI 분석: 상승 우위</span>
                            </div>
                            <div class="s-price">208,500원 <span class="val-up" style="font-size:0.9rem;">▲ HBM 주도</span></div>
                        </div>
                        <div class="stock-card" onclick="doSearch('삼성전자')">
                            <div style="display:flex; justify-content:space-between; align-items:center;">
                                <div class="s-name">삼성전자</div>
                                <span class="badge-impact badge-neutral">AI 분석: 박스권</span>
                            </div>
                            <div class="s-price">75,400원 <span class="val-neutral" style="font-size:0.9rem;">● 매물 소화</span></div>
                        </div>
                        <div class="stock-card" onclick="doSearch('NVDA')">
                            <div style="display:flex; justify-content:space-between; align-items:center;">
                                <div class="s-name">엔비디아 (NVDA)</div>
                                <span class="badge-impact badge-positive">AI 분석: 강세 지속</span>
                            </div>
                            <div class="s-price">$128.50 <span class="val-up" style="font-size:0.9rem;">▲ 글로벌 주도</span></div>
                        </div>
                    </div>
                </div>

            </div>

            <!-- 차별화 섹션: AI vs 개미 & 미국→한국 수혜주 -->
            <div class="grid-2col" style="margin-top: 36px;" id="sec-vs">
                
                <!-- AI vs 개미 -->
                <div>
                    <div class="section-header" style="margin-top: 0;">
                        <h2 class="section-title">🧠 AI vs 🐜 개미</h2>
                    </div>
                    <div class="card">
                        <div class="vs-card">
                            <div class="vs-side">
                                <div style="font-size:0.8rem; color:var(--text-sub);">삼성전자</div>
                                <div style="font-size:1.1rem; font-weight:800; margin-top:2px;">AI 분석</div>
                                <div class="badge-impact badge-neutral" style="margin-top:6px;">박스권 횡보</div>
                            </div>
                            <div class="vs-divider">VS</div>
                            <div class="vs-side">
                                <div style="font-size:0.8rem; color:var(--text-sub);">집단지성 투표</div>
                                <div style="font-size:1.1rem; font-weight:800; margin-top:2px;">개미 심리</div>
                                <div class="badge-impact badge-positive" style="margin-top:6px;">상승 우세 68%</div>
                            </div>
                        </div>
                        <div style="font-size:0.82rem; color:var(--text-sub); text-align:center; padding:4px 0;">
                            ⚠️ 종목을 검색하면 실시간 AI 퀀트와 개미 투표 결과를 1:1로 비교합니다.
                        </div>
                    </div>
                </div>

                <!-- 미국 시장 움직임 → 한국 관련주 -->
                <div>
                    <div class="section-header" style="margin-top: 0;">
                        <h2 class="section-title">🇺🇸 미국 시장 → 🇰🇷 한국 관련주</h2>
                    </div>
                    <div class="card">
                        <div class="link-flow">
                            <div>
                                <div style="font-size:0.8rem; color:var(--text-sub);">미국 빅테크 주도</div>
                                <div style="font-size:1.05rem; font-weight:800;">NVIDIA (엔비디아)</div>
                            </div>
                            <div style="font-weight:800; color:var(--text-muted);">→</div>
                            <div>
                                <div style="font-size:0.8rem; color:var(--text-sub);">국내 HBM 공급망 수혜</div>
                                <div style="font-size:0.95rem; font-weight:700; color:var(--color-primary);">SK하이닉스 · 한미반도체</div>
                            </div>
                        </div>
                        <div class="link-flow" style="margin-bottom:0;">
                            <div>
                                <div style="font-size:0.8rem; color:var(--text-sub);">미국 전기차/로봇</div>
                                <div style="font-size:1.05rem; font-weight:800;">TESLA (테슬라)</div>
                            </div>
                            <div style="font-weight:800; color:var(--text-muted);">→</div>
                            <div>
                                <div style="font-size:0.8rem; color:var(--text-sub);">2차전지 및 로보틱스 연동</div>
                                <div style="font-size:0.95rem; font-weight:700; color:var(--color-primary);">LG에너지솔루션 · 레인보우로보</div>
                            </div>
                        </div>
                    </div>
                </div>

            </div>

            <!-- 오늘의 핵심 뉴스 -->
            <div class="section-header" id="sec-news">
                <h2 class="section-title">📰 오늘의 핵심 뉴스</h2>
            </div>
            <div class="card" id="homeNewsList">
                <div style="text-align:center; color:var(--text-muted); padding:20px;">실시간 뉴스를 집계 중입니다...</div>
            </div>

        </div>

        <!-- 종목 상세 분석 뷰 (검색 시 전환) -->
        <div id="detail-view">
            <div class="loading-box" id="detailLoading">실시간 주가 데이터 및 퀀트 지표를 연산 중입니다...</div>

            <div id="detailContent" style="display:none;">
                
                <div class="card" style="margin-bottom:16px;">
                    <div class="detail-header">
                        <div>
                            <div class="detail-title"><span id="dName"></span><span class="detail-symbol" id="dSymbol"></span></div>
                            <div class="detail-meta" id="dMeta"></div>
                        </div>
                        <div style="text-align:right;">
                            <div class="detail-price" id="dPrice"></div>
                            <div style="font-size:1rem; font-weight:700;" id="dChange"></div>
                        </div>
                    </div>

                    <!-- 14대 퀀트 기반 AI 종합 요약 -->
                    <div style="display:grid; grid-template-columns: repeat(3, 1fr); gap:10px; margin-top:16px; padding-top:16px; border-top:1px solid var(--border-line);">
                        <div class="spec-box"><div class="spec-label">🧠 AI 분석 판단</div><div class="spec-val" id="dAiOpinion">-</div></div>
                        <div class="spec-box"><div class="spec-label">📈 추세 국면</div><div class="spec-val" id="dTrend">-</div></div>
                        <div class="spec-box"><div class="spec-label">🎯 1차 매수 진입가</div><div class="spec-val" id="dEntry1">-</div></div>
                    </div>

                    <!-- 차트 영역 -->
                    <div id="tvChart" style="width:100%; height:320px; margin-top:20px; background:#FFFFFF; border-radius:var(--radius-sm); overflow:hidden;"></div>
                </div>

                <!-- 기술적 상태 상세 -->
                <div class="card" style="margin-bottom:16px;">
                    <h3 class="section-title" style="font-size:1.05rem; margin-bottom:12px;">📊 기술적 상태 & 주요 가격선</h3>
                    <div class="quant-spec-grid">
                        <div class="spec-box"><div class="spec-label">20일 이동평균선</div><div class="spec-val" id="dMa20">-</div></div>
                        <div class="spec-box"><div class="spec-label">60일 이동평균선</div><div class="spec-val" id="dMa60">-</div></div>
                        <div class="spec-box"><div class="spec-label">RSI (14) 강도</div><div class="spec-val" id="dRsi">-</div></div>
                        <div class="spec-box"><div class="spec-label">MACD (12,26,9)</div><div class="spec-val" id="dMacd">-</div></div>
                        <div class="spec-box"><div class="spec-label">볼린저 밴드 상/하단</div><div class="spec-val" id="dBb">-</div></div>
                        <div class="spec-box"><div class="spec-label">5일평균 거래량 대비</div><div class="spec-val" id="dVolRatio">-</div></div>
                        <div class="spec-box"><div class="spec-label">주요 지지선</div><div class="spec-val" id="dSupport">-</div></div>
                        <div class="spec-box"><div class="spec-label">현재 기준가</div><div class="spec-val" id="dCurrent">-</div></div>
                        <div class="spec-box"><div class="spec-label">주요 저항선</div><div class="spec-val" id="dResistance">-</div></div>
                    </div>
                    <div style="margin-top:14px; padding:12px; background:#F9FAFB; border-radius:var(--radius-sm); font-size:0.85rem; color:var(--text-sub);" id="dGuideText"></div>
                </div>

                <!-- 텔레그램 실시간 알림 연동 배너 -->
                <a id="tgAlertLink" href="#" target="_blank" class="tg-banner">
                    <span>🔔 <span id="tgBtnText">이 종목</span> 텔레그램 실시간 알림 받기</span>
                    <span style="font-size:0.85rem; opacity:0.9;">1초 연결 →</span>
                </a>

                <!-- DART 전자공시 카드 -->
                <div class="card" id="dartCard" style="margin-top:16px; display:none;">
                    <h3 class="section-title" style="font-size:1.05rem; margin-bottom:12px;">📑 DART 전자공시</h3>
                    <div id="dartList"></div>
                </div>

                <!-- 종목 뉴스 카드 -->
                <div class="card" style="margin-top:16px;">
                    <h3 class="section-title" style="font-size:1.05rem; margin-bottom:12px;">📰 종목 관련 뉴스</h3>
                    <div id="stockNewsList"></div>
                </div>

                <!-- 집단지성 심리 투표 & 실시간 토론방 -->
                <div class="grid-2col" style="margin-top:16px;" id="sec-community">
                    
                    <!-- 개미 심리 투표 -->
                    <div class="card">
                        <h3 class="section-title" style="font-size:1.05rem;">🐜 개미 심리 투표</h3>
                        <div style="margin:14px 0 8px;">
                            <div style="display:flex; justify-content:space-between; font-size:0.85rem; font-weight:700; margin-bottom:6px;">
                                <span class="val-up" id="voteLongPct">상승 50%</span>
                                <span class="val-down" id="voteShortPct">하락 50%</span>
                            </div>
                            <div style="width:100%; height:8px; background:#F3F4F6; border-radius:4px; overflow:hidden; display:flex;">
                                <div style="height:100%; background:var(--color-up); width:50%; transition:width 0.3s;" id="voteLongBar"></div>
                                <div style="height:100%; background:var(--color-down); width:50%; transition:width 0.3s;" id="voteShortBar"></div>
                            </div>
                        </div>
                        <div class="vote-wrap">
                            <button class="vote-btn" id="btnVoteLong" onclick="castVote('long')">▲ 상승</button>
                            <button class="vote-btn" id="btnVoteShort" onclick="castVote('short')">▼ 하락</button>
                        </div>
                    </div>

                    <!-- 실시간 개미 토론방 -->
                    <div class="card">
                        <h3 class="section-title" style="font-size:1.05rem; justify-content:space-between;">
                            <span>💬 개미 토론</span>
                            <span style="font-size:0.75rem; color:var(--text-sub); font-weight:normal;" id="commentCountTag">0개 의견</span>
                        </h3>
                        <div id="commentList" style="max-height:180px; overflow-y:auto; margin-top:8px;"></div>
                        <div class="comment-input-box">
                            <input type="text" id="commentInput" placeholder="의견을 남겨주세요" onkeypress="if(event.keyCode==13) postComment();" maxlength="80">
                            <button onclick="postComment()">등록</button>
                        </div>
                    </div>

                </div>

            </div>
        </div>

    </div>

    <script>
        let chart = null;
        let currentSymbol = '';

        const SUPABASE_URL = "https://uaqvmsubzgzvxbdcwcad.supabase.co";
        const SUPABASE_KEY = "sb_publishable_bcxDofsnPXgBICDDgQ8Ijw_fNjPcsRW";
        let supabaseClient = null;
        try {
            if (window.supabase) {
                supabaseClient = window.supabase.createClient(SUPABASE_URL, SUPABASE_KEY);
            }
        } catch(e) {
            console.warn("Supabase local fallback mode");
        }

        async function initMarketOverview() {
            try {
                const res = await fetch('/api/market-overview');
                const list = await res.json();
                if (list && list.length >= 4) {
                    document.getElementById('mKospiVal').innerText = list[0].price;
                    document.getElementById('mKospiRate').innerText = list[0].rate;
                    document.getElementById('mKospiRate').className = 'rate ' + (list[0].up ? 'val-up' : 'val-down');

                    document.getElementById('mNasVal').innerText = list[1].price;
                    document.getElementById('mNasRate').innerText = list[1].rate;
                    document.getElementById('mNasRate').className = 'rate ' + (list[1].up ? 'val-up' : 'val-down');

                    document.getElementById('mBtcVal').innerText = list[2].price;
                    document.getElementById('mBtcRate').innerText = list[2].rate;
                    document.getElementById('mBtcRate').className = 'rate ' + (list[2].up ? 'val-up' : 'val-down');

                    document.getElementById('mUsdVal').innerText = list[3].price;
                    document.getElementById('mUsdRate').innerText = list[3].rate;
                    document.getElementById('mUsdRate').className = 'rate ' + (list[3].up ? 'val-up' : 'val-down');
                }
            } catch(e) {}

            try {
                const resNews = await fetch('/api/analyze?q=' + encodeURIComponent('뉴욕증시'));
                const dataNews = await resNews.json();
                const hList = document.getElementById('homeNewsList');
                if (dataNews && dataNews.stock_news && dataNews.stock_news.length > 0) {
                    hList.innerHTML = '';
                    dataNews.stock_news.slice(0, 5).forEach(n => {
                        hList.innerHTML += `
                            <a href="${n.link}" target="_blank" class="news-item">
                                <div class="news-header">
                                    <span class="news-meta">${n.media} · ${n.pubDate}</span>
                                    <span class="badge-impact badge-${n.impact}">${n.impact_text}</span>
                                </div>
                                <div class="news-title">${n.title}</div>
                            </a>
                        `;
                    });
                }
            } catch(e) {}
        }

        function resetToHome() {
            document.getElementById('home-view').style.display = 'block';
            document.getElementById('detail-view').style.display = 'none';
            document.getElementById('centerInput').value = '';
            currentSymbol = '';
            window.scrollTo({top: 0, behavior: 'smooth'});
        }

        function scrollToSection(id) {
            const el = document.getElementById(id);
            if (el) el.scrollIntoView({ behavior: 'smooth' });
        }

        async function doSearch(query) {
            const q = query.trim();
            if (!q) return;

            document.getElementById('home-view').style.display = 'none';
            document.getElementById('detail-view').style.display = 'block';
            document.getElementById('detailLoading').style.display = 'block';
            document.getElementById('detailContent').style.display = 'none';
            window.scrollTo({top: 0, behavior: 'smooth'});

            try {
                const res = await fetch('/api/analyze?q=' + encodeURIComponent(q));
                const data = await res.json();
                if (!res.ok || data.error) {
                    alert(data.error || '종목 정보를 불러올 수 없습니다.');
                    resetToHome();
                    return;
                }

                currentSymbol = data.symbol;

                document.getElementById('dName').innerText = data.name;
                document.getElementById('dSymbol').innerText = `(${data.symbol})`;
                document.getElementById('dPrice').innerText = data.price;
                document.getElementById('dChange').innerText = data.chg_str;
                document.getElementById('dChange').className = data.is_up ? 'val-up' : 'val-down';
                document.getElementById('dMeta').innerText = `시장: ${data.market} · 시각: ${data.time}`;

                const qData = data.quant;
                document.getElementById('dAiOpinion').innerText = qData.ai_opinion;
                document.getElementById('dTrend').innerText = qData.trend;
                document.getElementById('dEntry1').innerText = qData.entry1;

                document.getElementById('dMa20').innerText = qData.ma20;
                document.getElementById('dMa60').innerText = qData.ma60;
                document.getElementById('dRsi').innerText = qData.rsi;
                document.getElementById('dMacd').innerText = qData.macd;
                document.getElementById('dBb').innerText = qData.bollinger;
                document.getElementById('dVolRatio').innerText = qData.vol_ratio;
                document.getElementById('dSupport').innerText = qData.support;
                document.getElementById('dCurrent').innerText = qData.current;
                document.getElementById('dResistance').innerText = qData.resistance;
                document.getElementById('dGuideText').innerText = '💡 ' + qData.guide_text;

                // 텔레그램 연동 링크
                document.getElementById('tgAlertLink').href = 'https://t.me/gaemigtp_bot?start=' + encodeURIComponent(data.symbol);
                document.getElementById('tgBtnText').innerText = `[${data.name}]`;

                // 공시 렌더링
                const dCard = document.getElementById('dartCard');
                const dList = document.getElementById('dartList');
                if (data.dart_disclosures && data.dart_disclosures.length > 0) {
                    dList.innerHTML = '';
                    data.dart_disclosures.forEach(d => {
                        dList.innerHTML += `
                            <a href="${d.link}" target="_blank" class="news-item">
                                <div class="news-header">
                                    <span class="news-meta">${d.flr_nm} · ${d.date}</span>
                                    <span class="badge-impact badge-${d.tag_type}">${d.tag_name}</span>
                                </div>
                                <div class="news-title">${d.title}</div>
                            </a>
                        `;
                    });
                    dCard.style.display = 'block';
                } else {
                    dCard.style.display = 'none';
                }

                // 뉴스 렌더링
                const sNews = document.getElementById('stockNewsList');
                sNews.innerHTML = '';
                if (data.stock_news && data.stock_news.length > 0) {
                    data.stock_news.forEach(n => {
                        sNews.innerHTML += `
                            <a href="${n.link}" target="_blank" class="news-item">
                                <div class="news-header">
                                    <span class="news-meta">${n.media} · ${n.pubDate}</span>
                                    <span class="badge-impact badge-${n.impact}">${n.impact_text}</span>
                                </div>
                                <div class="news-title">${n.title}</div>
                            </a>
                        `;
                    });
                } else {
                    sNews.innerHTML = '<div style="color:var(--text-sub); font-size:0.85rem; padding:10px 0;">관련 뉴스가 없습니다.</div>';
                }

                loadCommunityData(data.symbol);

                document.getElementById('detailLoading').style.display = 'none';
                document.getElementById('detailContent').style.display = 'block';

                setTimeout(() => {
                    initTvChart(data.candles, data.ma20_line, data.ma60_line, data.bb_up_line, data.bb_dn_line);
                }, 50);

            } catch(e) {
                alert('서버와 통신 중 오류가 발생했습니다.');
                resetToHome();
            }
        }

        function initTvChart(candles, ma20, ma60, bbUp, bbDn) {
            const container = document.getElementById('tvChart');
            container.innerHTML = '';

            chart = LightweightCharts.createChart(container, {
                width: container.clientWidth || 700,
                height: 320,
                layout: { background: { color: '#FFFFFF' }, textColor: '#6B7280' },
                grid: { vertLines: { color: '#F3F4F6' }, horzLines: { color: '#F3F4F6' } },
                timeScale: { borderColor: '#E5E7EB', timeVisible: true },
                rightPriceScale: { borderColor: '#E5E7EB' }
            });

            const candleSeries = chart.addCandlestickSeries({
                upColor: '#16A34A', downColor: '#EF4444',
                borderDownColor: '#EF4444', borderUpColor: '#16A34A',
                wickDownColor: '#EF4444', wickUpColor: '#16A34A'
            });
            candleSeries.setData(candles);

            const lineMa20 = chart.addLineSeries({ color: '#2563EB', lineWidth: 1.5, priceLineVisible: false });
            lineMa20.setData(ma20);

            const lineMa60 = chart.addLineSeries({ color: '#EA580C', lineWidth: 1.5, priceLineVisible: false });
            lineMa60.setData(ma60);

            chart.timeScale().fitContent();
        }

        async function loadCommunityData(symbol) {
            const voteKey = 'vote_' + symbol;
            const rawVote = localStorage.getItem(voteKey) || JSON.stringify({ long: 18, short: 9, myVote: null });
            let vData = JSON.parse(rawVote);

            if (supabaseClient) {
                try {
                    const { data } = await supabaseClient.from('votes').select('*').eq('symbol', symbol).single();
                    if (data) {
                        vData.long = data.long_count || vData.long;
                        vData.short = data.short_count || vData.short;
                    }
                } catch(e) {}
            }
            updateVoteUI(vData);

            let comments = [];
            if (supabaseClient) {
                try {
                    const { data } = await supabaseClient.from('comments').select('*').eq('symbol', symbol).order('created_at', { ascending: false }).limit(6);
                    if (data && data.length > 0) {
                        comments = data.map(d => ({ nick: d.nickname, text: d.text }));
                    }
                } catch(e) {}
            }
            if (comments.length === 0) {
                comments = [
                    { nick: "스마트개미", text: "20일선 지지받고 반등 흐름 좋아 보입니다." },
                    { nick: "퀀트투자자", text: "AI 분석 가이드 참고해서 분할 매수 걸어둡니다." }
                ];
            }
            renderComments(comments);
        }

        function updateVoteUI(vData) {
            const total = vData.long + vData.short;
            const lPct = total > 0 ? Math.round((vData.long / total) * 100) : 50;
            const sPct = 100 - lPct;
            document.getElementById('voteLongPct').innerText = `상승 ${lPct}%`;
            document.getElementById('voteShortPct').innerText = `하락 ${sPct}%`;
            document.getElementById('voteLongBar').style.width = lPct + '%';
            document.getElementById('voteShortBar').style.width = sPct + '%';

            const btnL = document.getElementById('btnVoteLong');
            const btnS = document.getElementById('btnVoteShort');
            btnL.classList.toggle('active-long', vData.myVote === 'long');
            btnS.classList.toggle('active-short', vData.myVote === 'short');
        }

        async function castVote(type) {
            if (!currentSymbol) return;
            const voteKey = 'vote_' + currentSymbol;
            const vData = JSON.parse(localStorage.getItem(voteKey) || '{"long":18,"short":9,"myVote":null}');
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
                    await supabaseClient.from('votes').upsert({
                        symbol: currentSymbol, long_count: vData.long, short_count: vData.short
                    });
                } catch(e) {}
            }
        }

        function renderComments(list) {
            const wrap = document.getElementById('commentList');
            document.getElementById('commentCountTag').innerText = `${list.length}개 의견`;
            wrap.innerHTML = '';
            list.forEach(c => {
                wrap.innerHTML += `
                    <div class="comment-item">
                        <div class="comment-head"><span class="comment-nick">${c.nick}</span></div>
                        <div>${c.text}</div>
                    </div>
                `;
            });
        }

        async function postComment() {
            const input = document.getElementById('commentInput');
            const txt = input.value.trim();
            if (!txt || !currentSymbol) return;

            const nicks = ["황금개미", "성투개미", "퀀트장인", "존버대장"];
            const rNick = nicks[Math.floor(Math.random() * nicks.length)] + "#" + Math.floor(100 + Math.random() * 900);

            const wrap = document.getElementById('commentList');
            const newItem = document.createElement('div');
            newItem.className = 'comment-item';
            newItem.innerHTML = `<div class="comment-head"><span class="comment-nick">${rNick}</span></div><div>${txt}</div>`;
            wrap.prepend(newItem);
            input.value = '';

            if (supabaseClient) {
                try {
                    await supabaseClient.from('comments').insert({
                        symbol: currentSymbol, nickname: rNick, text: txt
                    });
                } catch(e) {}
            }
        }

        window.addEventListener('DOMContentLoaded', initMarketOverview);
    </script>
</body>
</html>
"""

# ----------------------------------------------------
# 7. 초경량 비동기/멀티스레드 웹 서버
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

        if parsed.path == "/api/market-overview":
            overview = get_market_overview()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(overview, ensure_ascii=False).encode('utf-8'))
            return

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
            self.wfile.write(json.dumps(data, ensure_ascii=False).encode('utf-8'))
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
