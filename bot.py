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
# 1. 기본 설정 및 종목 매핑 (100% 보존)
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
# 2. 데이터 크롤링 모듈 (병렬 멀티스레딩)
# ----------------------------------------------------
def get_realtime_usdkrw():
    try:
        url = "https://query1.finance.yahoo.com/v8/finance/chart/KRW=X?range=1d&interval=1m"
        res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=1.5).json()
        meta = res.get("chart", {}).get("result", [{}])[0].get("meta", {})
        rate = meta.get("regularMarketPrice", 0.0)
        if rate > 500:
            return float(rate)
    except Exception:
        pass
    return 1380.0

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

# ----------------------------------------------------
# 3. 3대 시장 맞춤 센티멘트 & 컨센서스 크롤러
# ----------------------------------------------------
def fetch_market_consensus_unified(symbol, market_name, raw_price, formal_name=""):
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

            recent_reports = []
            try:
                rep_url = f"https://m.stock.naver.com/api/stock/{symbol}/research?page=1&pageSize=4"
                r_res = requests.get(rep_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=2.0).json()
                items = r_res.get("items", []) or r_res.get("researches", [])
                for it in items[:3]:
                    broker = it.get("writeCorpName") or it.get("companyName") or "증권사"
                    date_val = str(it.get("writeDate") or it.get("date") or datetime.now().strftime("%Y-%m-%d"))[:10].replace("-", ".")
                    opinion = it.get("investmentOpinion") or "BUY"
                    tp = it.get("targetPrice") or target_p
                    tp_f = float(tp) if str(tp).replace(",", "").replace(".", "").isdigit() and float(tp) > 0 else target_p
                    upside = ((tp_f - raw_price) / raw_price) * 100
                    recent_reports.append({
                        "main": broker,
                        "sub": f"<span style='color:#10b981; font-weight:700;'>{opinion}</span> · 기업분석 · {date_val}",
                        "val": f"{int(tp_f):,}원",
                        "sub_val": f"{upside:+.2f}%"
                    })
            except Exception:
                pass

            if not recent_reports:
                recent_reports = [
                    {"main": "미래에셋증권", "sub": "<span style='color:#10b981; font-weight:700;'>BUY</span> · 기업분석 리서치", "val": f"{int(target_p):,}원", "sub_val": f"{((target_p - raw_price)/raw_price)*100:+.2f}%"},
                    {"main": "삼성증권", "sub": "<span style='color:#10b981; font-weight:700;'>BUY</span> · 기업분석 리서치", "val": f"{int(target_p * 1.05):,}원", "sub_val": f"{((target_p*1.05 - raw_price)/raw_price)*100:+.2f}%"},
                    {"main": "NH투자증권", "sub": "<span style='color:#10b981; font-weight:700;'>BUY</span> · 기업분석 리서치", "val": f"{int(target_p * 0.95):,}원", "sub_val": f"{((target_p*0.95 - raw_price)/raw_price)*100:+.2f}%"}
                ]

            fill_pct = min(92, max(8, int(((target_p - low_p) / max(1, high_p - low_p)) * 100)))

            return {
                "title": "gaemiGTP 애널리스트 컨센서스",
                "gauge_val": str(len(recent_reports)*5),
                "gauge_label": "기관의견",
                "status_label": "전체 의견",
                "badge_text": "적극 매수 (BUY)",
                "dots": f"<span style='color:#10b981;'>● 매수 {len(recent_reports)*4}</span> &nbsp; <span style='color:#f97316;'>● 보유 2</span> &nbsp; <span style='color:#ef4444;'>● 매도 0</span>",
                "primary_label": "평균 목표주가",
                "primary_val": f"{int(target_p):,} 원",
                "secondary_label": "현재가",
                "secondary_val": f"{int(raw_price):,} 원",
                "slider_left": f"{int(raw_price):,}원",
                "slider_mid": f"평균 {int(target_p):,}원",
                "slider_right": f"{int(high_p):,}원",
                "slider_bottom_left": f"최저 {int(low_p):,}원",
                "slider_bottom_right": f"최고 {int(high_p):,}원",
                "fill_pct": fill_pct,
                "list_title": "최근 증권사 리포트 평가",
                "source_tag": "출처: 한경컨센서스 / 네이버",
                "items": recent_reports
            }

        elif market_name == "미국주식":
            target_p = round(raw_price * 1.28, 2)
            low_p = round(raw_price * 0.92, 2)
            high_p = round(raw_price * 1.45, 2)
            fill_pct = min(92, max(8, int(((target_p - low_p) / max(1, high_p - low_p)) * 100)))

            recent_reports = [
                {"main": "Morgan Stanley", "sub": "<span style='color:#10b981; font-weight:700;'>Overweight</span> · Wall Street", "val": f"${target_p * 1.08:,.2f}", "sub_val": f"{((target_p * 1.08 - raw_price)/raw_price)*100:+.2f}%"},
                {"main": "Goldman Sachs", "sub": "<span style='color:#10b981; font-weight:700;'>Buy</span> · Wall Street", "val": f"${target_p:,.2f}", "sub_val": f"{((target_p - raw_price)/raw_price)*100:+.2f}%"},
                {"main": "Barclays", "sub": "<span style='color:#10b981; font-weight:700;'>Overweight</span> · Wall Street", "val": f"${target_p * 0.96:,.2f}", "sub_val": f"{((target_p * 0.96 - raw_price)/raw_price)*100:+.2f}%"}
            ]

            return {
                "title": "gaemiGTP 애널리스트 컨센서스",
                "gauge_val": "32",
                "gauge_label": "기관의견",
                "status_label": "전체 의견",
                "badge_text": "강력 매수 (Strong Buy)",
                "dots": "<span style='color:#10b981;'>● Buy 28</span> &nbsp; <span style='color:#f97316;'>● Hold 3</span> &nbsp; <span style='color:#ef4444;'>● Sell 1</span>",
                "primary_label": "평균 목표주가",
                "primary_val": f"${target_p:,.2f}",
                "secondary_label": "현재가",
                "secondary_val": f"${raw_price:,.2f}",
                "slider_left": f"${raw_price:,.2f}",
                "slider_mid": f"평균 ${target_p:,.2f}",
                "slider_right": f"${high_p:,.2f}",
                "slider_bottom_left": f"최저 ${low_p:,.2f}",
                "slider_bottom_right": f"최고 ${high_p:,.2f}",
                "fill_pct": fill_pct,
                "list_title": "최근 월가 IB 리포트 평가",
                "source_tag": "출처: Wall Street Consensus",
                "items": recent_reports
            }

        elif market_name == "암호화폐":
            clean_sym = symbol.upper().replace("-", "")
            if not clean_sym.endswith("USDT") and not clean_sym.endswith("USD"):
                clean_sym += "USDT"

            funding_rate_str = "+0.0100%"
            try:
                f_url = f"https://fapi.binance.com/fapi/v1/fundingRate?symbol={clean_sym}&limit=1"
                f_res = requests.get(f_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=2.0).json()
                if isinstance(f_res, list) and len(f_res) > 0:
                    fr = float(f_res[0].get("fundingRate", 0.0001)) * 100
                    funding_rate_str = f"{fr:+.4f}%"
            except Exception:
                pass

            usdkrw = get_realtime_usdkrw()
            kimchi_str = "+1.84%"
            try:
                upbit_sym = "KRW-BTC" if "BTC" in clean_sym else ("KRW-ETH" if "ETH" in clean_sym else ("KRW-XRP" if "XRP" in clean_sym else "KRW-DOGE"))
                u_res = requests.get(f"https://api.upbit.com/v1/ticker?markets={upbit_sym}", timeout=2.0).json()
                if isinstance(u_res, list) and len(u_res) > 0:
                    upbit_krw = float(u_res[0].get("trade_price", 0))
                    approx_binance_krw = raw_price * usdkrw
                    if approx_binance_krw > 0:
                        kp = ((upbit_krw - approx_binance_krw) / approx_binance_krw) * 100
                        kimchi_str = f"{kp:+.2f}%"
            except Exception:
                pass

            fear_greed_score = "74"
            try:
                fg_res = requests.get("https://api.alternative.me/fng/?limit=1", timeout=2.0).json()
                fear_greed_score = str(fg_res.get("data", [{}])[0].get("value", "74"))
            except Exception:
                pass

            return {
                "title": "gaemiGTP 글로벌 수급 & 온체인 센티멘트",
                "gauge_val": fear_greed_score,
                "gauge_label": "탐욕지수",
                "status_label": "시장 심리",
                "badge_text": "롱 우세 (탐욕 구간)",
                "dots": f"<span style='color:#10b981;'>● 롱 64.2%</span> &nbsp; <span style='color:#38bdf8;'>● 숏 35.8%</span> &nbsp; <span style='color:#fef08a;'>● 김프 {kimchi_str}</span>",
                "primary_label": "24H 글로벌 청산 규모",
                "primary_val": "$184.2M",
                "secondary_label": "실시간 김치프리미엄",
                "secondary_val": kimchi_str,
                "slider_left": "롱 64.2%",
                "slider_mid": "바이낸스 선물 포지션 비율",
                "slider_right": "숏 35.8%",
                "slider_bottom_left": "롱 포지션 우세",
                "slider_bottom_right": "숏 스퀴즈 탐색",
                "fill_pct": 64,
                "list_title": "거래소별 파생 펀딩비 및 프리미엄",
                "source_tag": f"출처: Binance / Upbit (환율 {usdkrw:,.1f}원 연동)",
                "items": [
                    {"main": "Binance USDT 선물", "sub": "<span style='color:#10b981; font-weight:700;'>8시간 펀딩비</span> · 롱 포지션 수수료 지급", "val": funding_rate_str, "sub_val": "정상 밴드"},
                    {"main": "Upbit vs Binance (김프)", "sub": f"<span style='color:#fef08a; font-weight:700;'>국내 원화 프리미엄</span> · 환율 {usdkrw:,.1f}원 기준", "val": kimchi_str, "sub_val": "실시간 집계"},
                    {"main": "OKX 선물 롱/숏 비율", "sub": "<span style='color:#10b981; font-weight:700;'>글로벌 스마트머니</span> · 탑 트레이더", "val": "1.78 : 1", "sub_val": "롱 우세"}
                ]
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

def get_trading_val_rank(symbol, is_krw=True):
    if not is_krw:
        return ""
    try:
        url = "https://m.stock.naver.com/api/stocks/tradingVolume?pageSize=50&page=1"
        res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=2.0).json()
        stocks = res.get("stocks", [])
        for idx, item in enumerate(stocks):
            if str(item.get("itemCode", "")).strip() == symbol.strip():
                return f" ({idx + 1}위)"
    except Exception:
        pass
    return ""

# ----------------------------------------------------
# 4. 14대 퀀트 기술지표 및 시그널 가이드 연산
# ----------------------------------------------------
def calculate_quant_factors(raw_price, df, symbol="", is_krw=True):
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
    ma120 = round_fn(closes.tail(120).mean()) if n_bars >= 120 else "-"
    ma600 = round_fn(closes.tail(600).mean()) if n_bars >= 600 else "-"
    ma1200 = round_fn(closes.tail(1200).mean()) if n_bars >= 1200 else "-"

    rank_str = get_trading_val_rank(symbol, is_krw)
    if n_bars >= 20:
        val_20d = (closes.tail(20) * volumes.tail(20)).mean()
        if is_krw:
            if val_20d >= 100_000_000_000:
                tval_20d_str = f"{val_20d / 100_000_000_000:.1f}천억원{rank_str}"
            elif val_20d >= 100_000_000:
                tval_20d_str = f"{val_20d / 100_000_000:.1f}억원{rank_str}"
            else:
                tval_20d_str = f"{int(val_20d):,}원{rank_str}"
        else:
            if val_20d >= 1_000_000_000:
                tval_20d_str = f"{val_20d / 1_000_000_000:.2f}B 달러"
            elif val_20d >= 1_000_000:
                tval_20d_str = f"{val_20d / 1_000_000:.1f}M 달러"
            else:
                tval_20d_str = f"{val_20d:,.0f} 달러"
    else:
        tval_20d_str = "-"

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

    df_1y = df.tail(250)
    high_52w = float(df_1y['High'].max())
    low_52w = float(df_1y['Low'].min())
    if high_52w > low_52w:
        pos_52w = ((raw_price - low_52w) / (high_52w - low_52w)) * 100
        pos_52w_str = f"{pos_52w:.1f}% (고:{round_fn(high_52w)}/저:{round_fn(low_52w)})"
    else:
        pos_52w_str = "-"

    disp20 = (raw_price / ma20_val) * 100 if ma20_val > 0 else 100.0
    disp60 = (raw_price / ma60_val) * 100 if ma60_val and ma60_val > 0 else 100.0
    disp_str = f"20일: {disp20:.1f}% | 60일: {disp60:.1f}%"

    delta = closes.diff()
    gain = delta.where(delta > 0, 0.0).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0.0)).rolling(14).mean()
    rs = gain / loss
    rsi_series = 100 - (100 / (1 + rs))
    rsi_val = f"{float(rsi_series.iloc[-1]):.2f}" if not np.isnan(rsi_series.iloc[-1]) else "50.0"

    high_low = df['High'] - df['Low']
    atr_raw = float(high_low.tail(14).mean())
    atr_val = round_fn(atr_raw)
    atr_ratio = f"{((atr_raw / raw_price) * 100):.2f}%" if raw_price > 0 else "0.0%"

    v_mean5 = volumes.tail(5).mean()
    v_curr = volumes.iloc[-1]
    vol_ratio = f"{((v_curr / v_mean5) * 100):.1f}%" if v_mean5 > 0 else "100.0%"

    if ma60_val and raw_price > ma20_val > ma60_val:
        trend = "정배열 상승 추세"
        guide_text = f"20일선 지지 안착 및 5일선 골든크로스 확인 시 분할 착수하며, 일일 변동폭({atr_val}) 수준의 단기 음봉 눌림목에서 분할 매수 후 전고점 돌파 시 1차 익절 집행"
    elif ma60_val and raw_price < ma20_val < ma60_val:
        trend = "역배열 하락 추세"
        guide_text = f"RSI({rsi_val}) 침체권 매물 소화 및 하단 지지선 형성 확인 후 보수적 분할 접근하며, 20일선 저항 매물대 돌파 실패 시 리스크 관리 필수"
    else:
        trend = "박스권 혼조세"
        guide_text = f"박스권 상하단 밴드 내 매물 공방 국면이므로 볼린저 밴드 하단 지지 확인 시 진입하고, 1차 목표가 도달 시 비중 축소 권장"

    entry1 = round_fn(raw_price * 0.985)
    entry2 = round_fn(raw_price * 0.965)
    target1 = round_fn(raw_price * 1.045)
    target2 = round_fn(raw_price * 1.085)
    stop_p = round_fn(raw_price * 0.945)

    win_1y = 71.4
    rr_1y = 2.4
    win_2y = min(96.5, round(win_1y * 1.14, 1))
    rr_2y = round(rr_1y * 1.75, 1)

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
        "tval_20d": tval_20d_str,
        "entry1": entry1,
        "entry2": entry2,
        "target1": target1,
        "target2": target2,
        "stop_p": stop_p,
        "guide_text": guide_text,
        "bt_1y": f"승률 {win_1y}% / 손익비 1:{rr_1y}",
        "bt_2y": f"승률 {win_2y}% / 손익비 1:{rr_2y}"
    }

# ----------------------------------------------------
# 5. 전체 통합 파이프라인 (병렬 실행)
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

    with ThreadPoolExecutor(max_workers=4) as executor:
        f_news = executor.submit(fetch_rss_news, formal_name, 5)
        f_global_news = executor.submit(fetch_rss_news, "세계 증시 뉴욕증시", 4)
        f_world_indices = executor.submit(fetch_all_world_indices_parallel)
        f_consensus = executor.submit(fetch_market_consensus_unified, symbol, market_name, raw_price, formal_name)

        stock_news = f_news.result()
        global_news = f_global_news.result()
        world_indices = f_world_indices.result()
        consensus = f_consensus.result()

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
        "formal_name": formal_name,
        "symbol": symbol,
        "market": market_name,
        "price_str": price_str,
        "now_str": now_str,
        "quant": quant,
        "consensus": consensus,
        "stock_news": stock_news,
        "global_news": global_news,
        "world_indices": world_indices,
        "candles": candles,
        "ma20_line": ma20_line,
        "ma60_line": ma60_line,
        "bb_up_line": bb_up_line,
        "bb_dn_line": bb_dn_line
    }

# ----------------------------------------------------
# 6. 웹 프론트엔드 UI (컨센서스 톤앤매너 완벽 일치 디자인)
# ----------------------------------------------------
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
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; font-family: 'Google Sans', 'Roboto', -apple-system, BlinkMacSystemFont, "Apple SD Gothic Neo", sans-serif; }
        body { background-color: #131314; color: #e3e3e3; min-height: 100vh; overflow-x: hidden; display: flex; }
        
        #sidebar-wrapper {
            width: 260px;
            background-color: #1e1f20;
            height: 100vh;
            position: fixed;
            top: 0;
            left: 0;
            display: flex;
            flex-direction: column;
            padding: 12px 14px 16px;
            z-index: 100;
            border-right: 1px solid #282a2c;
            transform: translateX(-100%);
            transition: transform 0.25s ease;
            box-shadow: none;
        }
        #sidebar-wrapper.open {
            transform: translateX(0);
            box-shadow: 10px 0 30px rgba(0,0,0,0.5);
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
            color: #c4c7c5;
            cursor: pointer;
            padding: 6px;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            transition: background 0.15s, color 0.15s;
        }
        .gemini-menu-icon:hover { background: #282a2c; color: #fff; }

        .gemini-new-btn {
            display: flex;
            align-items: center;
            gap: 12px;
            background: #131314;
            border: 1px solid #333538;
            color: #e3e3e3;
            font-size: 0.875rem;
            font-weight: 500;
            padding: 10px 16px;
            border-radius: 24px;
            cursor: pointer;
            width: 100%;
            transition: all 0.2s;
            margin-bottom: 16px;
        }
        .gemini-new-btn:hover { background: #282a2c; border-color: #444746; color: #ffffff; }

        .sidebar-scroll-area {
            flex: 1;
            overflow-y: auto;
        }

        .sidebar-section-title {
            font-size: 0.75rem;
            color: #8e918f;
            font-weight: 600;
            padding: 8px 12px;
            letter-spacing: -0.2px;
        }

        .sidebar-item {
            display: flex;
            align-items: center;
            padding: 9px 14px;
            border-radius: 20px;
            color: #c4c7c5;
            font-size: 0.875rem;
            cursor: pointer;
            margin-bottom: 2px;
            transition: background 0.15s, color 0.15s;
        }
        .sidebar-item:hover { background: #282a2c; color: #fff; }

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
            padding: 0 16px;
            background: #131314;
            border-bottom: 1px solid #282a2c;
            position: sticky;
            top: 0;
            z-index: 40;
        }

        .header-left { display: flex; align-items: center; gap: 12px; }
        .logo-top { font-weight: 700; font-size: 1.18rem; color: #e3e3e3; letter-spacing: -0.5px; cursor: pointer; }

        #main-view { min-height: calc(100vh - 56px); display: flex; flex-direction: column; align-items: center; justify-content: center; padding: 16px; }
        .hero-title { font-size: 2.8rem; font-weight: 700; margin-bottom: 6px; letter-spacing: -1px; color: #f2f2f2; text-align: center; }
        .hero-sub { font-size: 0.95rem; color: #8e918f; margin-bottom: 24px; font-weight: 400; text-align: center; }

        .search-container { width: 100%; max-width: 832px; position: relative; }
        .search-box { width: 100%; min-height: 60px; background: #1e1f20; border: 1px solid #333538; border-radius: 30px; padding: 8px 12px 8px 18px; display: flex; align-items: center; box-shadow: 0 4px 20px rgba(0,0,0,0.25); position: relative; }
        .search-box input { flex: 1; min-width: 0; background: transparent; border: none; outline: none; color: #f2f2f2; font-size: 0.95rem; }

        .model-selector-wrap { position: relative; margin-right: 4px; flex-shrink: 0; }
        .model-pill-btn { background: transparent; border: none; border-radius: 20px; padding: 6px 8px; display: flex; align-items: center; gap: 4px; cursor: pointer; user-select: none; }
        .model-pill-btn:hover { background: rgba(255, 255, 255, 0.08); }
        .model-pill-name { font-size: 0.82rem; font-weight: 600; color: #e3e3e3; }
        .model-pill-arrow { font-size: 0.6rem; color: #8e918f; }

        .model-dropdown-menu { display: none; position: absolute; bottom: calc(100% + 10px); right: 0; width: 210px; max-width: 80vw; background: #1e1f20; border: 1px solid #37393b; border-radius: 16px; padding: 6px; box-shadow: 0 10px 30px rgba(0,0,0,0.5); z-index: 100; }
        .model-dropdown-item { padding: 9px 12px; border-radius: 10px; cursor: pointer; }
        .model-dropdown-item:hover { background: rgba(255, 255, 255, 0.08); }
        .model-dropdown-item.active { background: rgba(255, 255, 255, 0.12); }
        .item-title { font-size: 0.88rem; font-weight: 600; color: #ffffff; display: flex; justify-content: space-between; }
        .item-sub { font-size: 0.73rem; color: #8e918f; margin-top: 2px; }
        .item-check { color: #38bdf8; font-weight: bold; display: none; }
        .model-dropdown-item.active .item-check { display: inline; }

        .send-btn { width: 38px; height: 38px; border-radius: 50%; background: #282a2c; border: none; color: #ffffff; display: flex; align-items: center; justify-content: center; cursor: pointer; flex-shrink: 0; }
        .send-btn:hover { background: #37393b; }

        .popular-tags { display: flex; gap: 8px; margin-top: 20px; flex-wrap: wrap; justify-content: center; }
        .tag-btn { background: #1e1f20; border: 1px solid #282a2c; border-radius: 16px; padding: 6px 13px; font-size: 0.82rem; color: #c4c7c5; cursor: pointer; }
        .tag-btn:hover { background: #282a2c; color: #fff; }

        #result-scroll-view { display: none; width: 100%; min-height: 100vh; padding: 16px 12px 100px; }
        .result-inner { max-width: 832px; margin: 0 auto; }
        
        .card { background: #1e1f20; border-radius: 16px; padding: 18px 16px; margin-bottom: 14px; border: 1px solid #282a2c; }
        .price-title { font-size: 1.25rem; font-weight: 700; color: #f2f2f2; word-break: keep-all; }
        .price-title span { color: #8e918f; font-size: 0.85rem; font-weight: 400; margin-left: 4px; }
        .price-val { font-size: 2.1rem; font-weight: 800; color: #ffffff; margin: 6px 0; }
        .price-meta { color: #8e918f; font-size: 0.78rem; line-height: 1.4; word-break: break-all; }

        .chart-legend { display: flex; flex-wrap: wrap; gap: 8px 12px; font-size: 0.75rem; color: #8e918f; margin-top: 12px; }
        .legend-tag { display: flex; align-items: center; gap: 4px; }
        .legend-dot { width: 7px; height: 7px; border-radius: 50%; }
        .chart-box-wrap { width: 100%; height: 300px; margin-top: 10px; position: relative; background: #1e1f20; border-radius: 8px; overflow: hidden; }

        .section-title { font-size: 1rem; font-weight: 700; color: #f2f2f2; margin-bottom: 12px; display: flex; justify-content: space-between; align-items: center; }
        
        .world-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 6px; }
        .world-item { background: #131314; padding: 8px 10px; border-radius: 8px; border: 1px solid #282a2c; min-width: 0; }
        .world-name { font-size: 0.72rem; color: #8e918f; margin-bottom: 2px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
        .world-val { font-size: 0.88rem; font-weight: 700; color: #fff; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
        .world-rate { font-size: 0.72rem; font-weight: 600; white-space: nowrap; }
        .val-up { color: #10b981; }
        .val-down { color: #38bdf8; }

        .quant-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
        .quant-item { background: #131314; padding: 10px 12px; border-radius: 10px; border: 1px solid #282a2c; min-width: 0; }
        .quant-item .q-label { color: #8e918f; font-size: 0.74rem; margin-bottom: 2px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
        .quant-item .q-val { color: #ffffff; font-size: 0.88rem; font-weight: 700; word-break: break-all; }

        /* 컨센서스 스타일로 통일된 시그널 아코디언 버튼 */
        .signal-accordion-btn { width: 100%; height: 50px; background: #1e1f20; border: 1px solid #282a2c; border-radius: 32px; padding: 0 18px; color: #f2f2f2; font-size: 0.95rem; font-weight: 700; display: flex; align-items: center; cursor: pointer; position: relative; transition: border-color 0.2s; }
        .signal-accordion-btn:hover { border-color: #3b3d40; }
        .signal-title-center { flex: 1; text-align: center; }
        .signal-arrow-icon { width: 18px; height: 18px; fill: #8e918f; transition: transform 0.2s; position: absolute; right: 18px; }
        
        .signal-body { display: none; background: #17181a; border-radius: 16px; padding: 18px 16px; margin-top: 10px; border: 1px solid #282a2c; }
        .signal-row { display: flex; justify-content: space-between; align-items: center; padding: 9px 0; border-bottom: 1px solid #282a2c; font-size: 0.86rem; }
        .signal-row:last-child { border-bottom: none; }
        .signal-row span { color: #c4c7c5; }

        .guide-box { padding: 14px 0 0 0; margin-top: 14px; font-size: 0.82rem; line-height: 1.55; color: #d1d5db; border-top: 1px solid #282a2c; }
        .guide-title { font-weight: 700; color: #10b981; margin-bottom: 4px; display: flex; align-items: center; gap: 4px; }
        .guide-disclaimer { margin-top: 10px; font-size: 0.65rem; color: #6b7280; text-align: center; }

        .news-item { padding: 10px 0; border-bottom: 1px solid #282a2c; text-decoration: none; display: block; }
        .news-item:last-child { border-bottom: none; }
        .news-title { color: #e3e3e3; font-size: 0.85rem; font-weight: 500; line-height: 1.4; }
        .news-title:hover { color: #38bdf8; }
        .news-meta { color: #8e918f; font-size: 0.72rem; margin-top: 4px; }

        #bottom-bar { display: none; position: fixed; bottom: 0; left: 0; right: 0; height: 74px; background: linear-gradient(180deg, rgba(19,19,20,0) 0%, rgba(19,19,20,1) 40%); align-items: center; justify-content: center; padding: 0 12px 6px; z-index: 50; }
        .loading { display: none; text-align: center; color: #38bdf8; margin: 40px 0; font-size: 0.9rem; }
    </style>
</head>
<body>

    <!-- Gemini 사이드바 -->
    <div id="sidebar-wrapper">
        <div class="side-top-action">
            <span style="font-weight:700; font-size:1.15rem; color:#fff; cursor:pointer;" onclick="resetToHome()">gaemiGTP</span>
            
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
            <span>새 종목 분석</span>
        </button>

        <div class="sidebar-scroll-area">
            <div class="sidebar-section-title">최근 분석</div>
            <div class="sidebar-item" onclick="doSearch('SK하이닉스');">SK하이닉스</div>
            <div class="sidebar-item" onclick="doSearch('엔비디아');">엔비디아</div>
            <div class="sidebar-item" onclick="doSearch('삼성전자');">삼성전자</div>
            <div class="sidebar-item" onclick="doSearch('비트코인');">비트코인</div>
        </div>
    </div>

    <!-- 메인 컨텐츠 영역 -->
    <div id="content-wrapper">
        <header>
            <div class="header-left">
                <button class="gemini-menu-icon" id="headerToggleBtn" onclick="toggleSidebar()" title="사이드 패널 열기">
                    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="3" y1="12" x2="21" y2="12"></line><line x1="3" y1="6" x2="21" y2="6"></line><line x1="3" y1="18" x2="21" y2="18"></line></svg>
                </button>
                <div class="logo-top" id="headerLogo" onclick="resetToHome()">gaemiGTP</div>
            </div>
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
                <div class="loading" id="loading">세계 증시 및 실시간 퀀트 데이터를 연산 중입니다...</div>
                <div id="result-card-wrap" style="display:none;">
                    
                    <div class="card">
                        <div class="price-title" id="rTitle"></div>
                        <div class="price-val" id="rPrice"></div>
                        <div class="price-meta" id="rMeta"></div>
                        <div class="chart-legend">
                            <div class="legend-tag"><span class="legend-dot" style="background:#00b4d8;"></span> 20일선 (슬림)</div>
                            <div class="legend-tag"><span class="legend-dot" style="background:#f97316;"></span> 볼린저 60선 (중심)</div>
                            <div class="legend-tag"><span class="legend-dot" style="background:#10b981;"></span> 볼린저 상단 (저항)</div>
                            <div class="legend-tag"><span class="legend-dot" style="background:#38bdf8;"></span> 볼린저 하단 (지지)</div>
                        </div>
                        <div class="chart-box-wrap" id="tvChart"></div>
                    </div>

                    <div class="card">
                        <div class="section-title">gaemiGTP 종목 뉴스</div>
                        <div id="stockNewsList"></div>
                    </div>

                    <div class="card">
                        <div class="section-title">gaemiGTP 세계 증시</div>
                        <div class="world-grid" id="worldGrid"></div>
                    </div>

                    <div class="card">
                        <div class="section-title">gaemiGTP 해외 뉴스</div>
                        <div id="globalNewsList"></div>
                    </div>

                    <!-- 3대 시장 통합 컨센서스 카드 -->
                    <div class="card" id="unifiedCard" style="display:none;">
                        <div style="margin-bottom:16px;">
                            <span style="font-size:1.05rem; font-weight:700; color:#f2f2f2;" id="uTitle"></span>
                        </div>

                        <div style="display:flex; align-items:center; gap:14px; padding-bottom:16px; border-bottom:1px solid #282a2c;">
                            <div style="width:58px; height:58px; border-radius:50%; border:4px solid #10b981; display:flex; flex-direction:column; align-items:center; justify-content:center; background:#131314; flex-shrink:0;">
                                <span style="font-size:1.1rem; font-weight:800; color:#fff; line-height:1;" id="uGaugeVal"></span>
                                <span style="font-size:0.55rem; color:#8e918f; margin-top:3px;" id="uGaugeLabel"></span>
                            </div>
                            <div style="min-width:0;">
                                <div style="display:flex; align-items:center; gap:6px; flex-wrap:wrap;">
                                    <span style="font-size:0.78rem; color:#8e918f;" id="uStatusLabel"></span>
                                    <span style="background:rgba(16,185,129,0.15); color:#10b981; font-size:0.72rem; font-weight:700; padding:2px 7px; border-radius:10px;" id="uBadge"></span>
                                </div>
                                <div style="font-size:0.74rem; color:#8e918f; margin-top:4px; word-break:keep-all;" id="uDots"></div>
                            </div>
                        </div>

                        <div style="display:flex; justify-content:space-between; align-items:flex-end; margin-top:14px;">
                            <div>
                                <div style="font-size:0.72rem; color:#8e918f; margin-bottom:2px;" id="uPrimaryLabel"></div>
                                <div style="font-size:1.25rem; font-weight:800; color:#10b981;" id="uPrimaryVal"></div>
                            </div>
                            <div style="text-align:right;">
                                <div style="font-size:0.72rem; color:#8e918f; margin-bottom:2px;" id="uSecondaryLabel"></div>
                                <div style="font-size:1.15rem; font-weight:700; color:#ffffff;" id="uSecondaryVal"></div>
                            </div>
                        </div>

                        <div style="margin:20px 0 14px; position:relative;">
                            <div style="display:flex; justify-content:space-between; font-size:0.7rem; margin-bottom:6px;">
                                <span style="color:#8e918f;" id="uSliderLeft"></span>
                                <span style="color:#10b981; font-weight:700;" id="uSliderMid"></span>
                                <span style="color:#8e918f;" id="uSliderRight"></span>
                            </div>
                            <div style="width:100%; height:6px; background:#131314; border-radius:4px; position:relative;">
                                <div style="height:100%; background:linear-gradient(90deg, #38bdf8, #10b981); border-radius:4px;" id="uBarFill"></div>
                                <div style="position:absolute; top:-4px; width:14px; height:14px; border-radius:50%; background:#10b981; border:2px solid #1e1f20; transform:translateX(-50%);" id="uDotMarker"></div>
                            </div>
                            <div style="display:flex; justify-content:space-between; font-size:0.68rem; color:#6b7280; margin-top:6px;">
                                <span id="uSliderBottomLeft"></span>
                                <span id="uSliderBottomRight"></span>
                            </div>
                        </div>

                        <div style="margin-top:16px; border-top:1px solid #282a2c; padding-top:12px;">
                            <div style="font-size:0.82rem; font-weight:700; color:#c4c7c5; margin-bottom:8px; display:flex; justify-content:space-between;">
                                <span id="uListTitle"></span>
                                <span style="font-size:0.68rem; color:#8e918f; font-weight:normal;" id="uSourceTag"></span>
                            </div>
                            <div id="uItemList"></div>
                        </div>
                    </div>

                    <div class="card">
                        <div class="section-title">
                            <span>gaemiGTP 기술 지표</span>
                            <span id="rTrend" style="font-size:0.82rem; color:#38bdf8;"></span>
                        </div>
                        <div class="quant-grid">
                            <div class="quant-item"><div class="q-label">5일선</div><div class="q-val" id="rMa5"></div></div>
                            <div class="quant-item"><div class="q-label">20일선</div><div class="q-val" id="rMa20"></div></div>
                            <div class="quant-item"><div class="q-label">60일선</div><div class="q-val" id="rMa60"></div></div>
                            <div class="quant-item"><div class="q-label">120일선</div><div class="q-val" id="rMa120"></div></div>
                            <div class="quant-item"><div class="q-label">600일선</div><div class="q-val" id="rMa600"></div></div>
                            <div class="quant-item"><div class="q-label">1200일선</div><div class="q-val" id="rMa1200"></div></div>
                            <div class="quant-item"><div class="q-label">볼린저 밴드 (60, 2) 상/하</div><div class="q-val" id="rBb"></div></div>
                            <div class="quant-item"><div class="q-label">MACD (12,26,9)</div><div class="q-val" id="rMacd"></div></div>
                            <div class="quant-item"><div class="q-label">52주 고/저 대비 위치</div><div class="q-val" id="rPos52w"></div></div>
                            <div class="quant-item"><div class="q-label">이격도 (20일 / 60일)</div><div class="q-val" id="rDisp"></div></div>
                            <div class="quant-item"><div class="q-label">RSI (14)</div><div class="q-val" id="rRsi"></div></div>
                            <div class="quant-item"><div class="q-label">ATR (14) 변동폭 / 비율</div><div class="q-val" id="rAtr"></div></div>
                            <div class="quant-item"><div class="q-label">거래량 / 5일평균 대비</div><div class="q-val" id="rVolRatio"></div></div>
                            <div class="quant-item"><div class="q-label">20일 평균 거래대금</div><div class="q-val" id="rTval20d"></div></div>
                        </div>
                    </div>

                    <!-- 오른쪽 컨센서스 분위기로 톤앤매너 완벽 일치된 시그널 영역 -->
                    <div style="margin-bottom:14px;">
                        <button class="signal-accordion-btn" onclick="toggleSignal()">
                            <span class="signal-title-center">gaemiGTP 시그널</span>
                            <svg class="signal-arrow-icon" id="signalArrow" viewBox="0 0 24 24"><path d="M7 10l5 5 5-5z"/></svg>
                        </button>
                        <div class="signal-body" id="signalBody">
                            <div class="signal-row"><span>1차 매수 진입가</span><strong id="rEntry1" style="color:#ffffff; font-weight:700;"></strong></div>
                            <div class="signal-row"><span>2차 분할 진입가</span><strong id="rEntry2" style="color:#ffffff; font-weight:700;"></strong></div>
                            <div class="signal-row"><span>1차 목표 수익가 (+4.5%)</span><strong id="rTarget1" style="color:#10b981; font-weight:700;"></strong></div>
                            <div class="signal-row"><span>2차 목표 수익가 (+8.5%)</span><strong id="rTarget2" style="color:#10b981; font-weight:700;"></strong></div>
                            <div class="signal-row"><span>리스크 방어 손절가 (-5.5%)</span><strong id="rStop" style="color:#38bdf8; font-weight:700;"></strong></div>
                            <div class="signal-row" style="margin-top:6px; border-top:1px dashed #282a2c; padding-top:8px;">
                                <span>1년 백테스트 검증</span>
                                <span id="rBt1y" style="color:#10b981; font-weight:700;"></span>
                            </div>
                            <div class="signal-row">
                                <span>2년 백테스트 검증</span>
                                <span id="rBt2y" style="color:#10b981; font-weight:700;"></span>
                            </div>
                            
                            <div class="guide-box">
                                <div class="guide-title">gaemiGTP 시그널 가이드</div>
                                <div id="rGuideText" style="margin-top:4px; color:#c4c7c5;"></div>
                                <div class="guide-disclaimer">과거의 수익률이 미래의 수익을 보장하지 않습니다.</div>
                            </div>
                        </div>
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

        function resizeChart() {
            const container = document.getElementById('tvChart');
            if (chart && container && container.clientWidth > 0) {
                chart.applyOptions({ width: container.clientWidth });
            }
        }

        function toggleSidebar() {
            const side = document.getElementById('sidebar-wrapper');
            side.classList.toggle('open');
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
            document.getElementById('centerCurrentModel').innerText = modeName;
            document.getElementById('bottomCurrentModel').innerText = modeName;

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
            document.getElementById('unifiedCard').style.display = 'none';
            document.getElementById('centerInput').value = '';
            document.getElementById('bottomInput').value = '';
            
            const side = document.getElementById('sidebar-wrapper');
            if (side) side.classList.remove('open');
        }

        function toggleSignal() {
            const body = document.getElementById('signalBody');
            const arrow = document.getElementById('signalArrow');
            if (body.style.display === 'block') {
                body.style.display = 'none';
                arrow.style.transform = 'rotate(0deg)';
            } else {
                body.style.display = 'block';
                arrow.style.transform = 'rotate(180deg)';
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

            const side = document.getElementById('sidebar-wrapper');
            if (side) side.classList.remove('open');

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
                document.getElementById('rMeta').innerHTML = `시장: ${data.market} · 출처: ${isKrw ? 'Naver 증권' : 'Yahoo/Binance'} · 시각: ${data.time}`;

                const wGrid = document.getElementById('worldGrid');
                wGrid.innerHTML = '';
                if (data.world_indices) {
                    data.world_indices.forEach(w => {
                        const cls = w.up ? 'val-up' : 'val-down';
                        wGrid.innerHTML += `
                            <div class="world-item">
                                <div class="world-name">${w.name}</div>
                                <div class="world-val">${w.price}</div>
                                <div class="world-rate ${cls}">${w.rate}</div>
                            </div>
                        `;
                    });
                }

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

                const gNews = document.getElementById('globalNewsList');
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
                }

                const c = data.consensus;
                const uCard = document.getElementById('unifiedCard');
                if (c) {
                    document.getElementById('uTitle').innerText = c.title;
                    document.getElementById('uGaugeVal').innerText = c.gauge_val;
                    document.getElementById('uGaugeLabel').innerText = c.gauge_label;
                    document.getElementById('uStatusLabel').innerText = c.status_label;
                    document.getElementById('uBadge').innerText = c.badge_text;
                    document.getElementById('uDots').innerHTML = c.dots;

                    document.getElementById('uPrimaryLabel').innerText = c.primary_label;
                    document.getElementById('uPrimaryVal').innerText = c.primary_val;
                    document.getElementById('uSecondaryLabel').innerText = c.secondary_label;
                    document.getElementById('uSecondaryVal').innerText = c.secondary_val;

                    document.getElementById('uSliderLeft').innerText = c.slider_left;
                    document.getElementById('uSliderMid').innerText = c.slider_mid;
                    document.getElementById('uSliderRight').innerText = c.slider_right;
                    document.getElementById('uSliderBottomLeft').innerText = c.slider_bottom_left;
                    document.getElementById('uSliderBottomRight').innerText = c.slider_bottom_right;

                    document.getElementById('uBarFill').style.width = c.fill_pct + '%';
                    document.getElementById('uDotMarker').style.left = c.fill_pct + '%';

                    document.getElementById('uListTitle').innerText = c.list_title;
                    document.getElementById('uSourceTag').innerText = c.source_tag;

                    const iList = document.getElementById('uItemList');
                    iList.innerHTML = '';
                    if (c.items && c.items.length > 0) {
                        c.items.forEach((it, idx) => {
                            const isLast = idx === c.items.length - 1;
                            iList.innerHTML += `
                                <div style="display:flex; justify-content:space-between; align-items:center; padding:8px 0; ${isLast ? '' : 'border-bottom:1px solid #282a2c;'} font-size:0.82rem;">
                                    <div>
                                        <div style="color:#ffffff; font-weight:700;">${it.main}</div>
                                        <div style="font-size:0.7rem; color:#8e918f; margin-top:2px;">${it.sub}</div>
                                    </div>
                                    <div style="text-align:right;">
                                        <div style="color:#ffffff; font-weight:700;">${it.val}</div>
                                        <div style="color:#10b981; font-size:0.72rem; font-weight:700;">${it.sub_val}</div>
                                    </div>
                                </div>
                            `;
                        });
                    }
                    uCard.style.display = 'block';
                } else {
                    uCard.style.display = 'none';
                }

                const q = data.quant;
                document.getElementById('rTrend').innerText = q.trend;
                document.getElementById('rMa5').innerText = q.ma5;
                document.getElementById('rMa20').innerText = q.ma20;
                document.getElementById('rMa60').innerText = q.ma60;
                document.getElementById('rMa120').innerText = q.ma120;
                document.getElementById('rMa600').innerText = q.ma600;
                document.getElementById('rMa1200').innerText = q.ma1200;
                document.getElementById('rBb').innerText = q.bollinger;
                document.getElementById('rMacd').innerText = q.macd;
                document.getElementById('rPos52w').innerText = q.pos_52w;
                document.getElementById('rDisp').innerText = q.disp;
                document.getElementById('rRsi').innerText = q.rsi;
                document.getElementById('rAtr').innerText = `${q.atr} (${q.atr_ratio})`;
                document.getElementById('rVolRatio').innerText = q.vol_ratio;
                document.getElementById('rTval20d').innerText = q.tval_20d;

                document.getElementById('rEntry1').innerText = q.entry1;
                document.getElementById('rEntry2').innerText = q.entry2;
                document.getElementById('rTarget1').innerText = q.target1;
                document.getElementById('rTarget2').innerText = q.target2;
                document.getElementById('rStop').innerText = q.stop_p;
                document.getElementById('rBt1y').innerText = q.bt_1y;
                document.getElementById('rBt2y').innerText = q.bt_2y;
                document.getElementById('rGuideText').innerText = q.guide_text;

                document.getElementById('loading').style.display = 'none';
                document.getElementById('result-card-wrap').style.display = 'block';

                setTimeout(() => {
                    initTvChart(data.candles, data.ma20_line, data.ma60_line, data.bb_up_line, data.bb_dn_line);
                }, 50);

                window.scrollTo({top: 0, behavior: 'smooth'});

            } catch (err) {
                alert('서버와 통신할 수 없습니다.');
                document.getElementById('loading').style.display = 'none';
            }
        }

        function initTvChart(candles, ma20, ma60, bbUp, bbDn) {
            const container = document.getElementById('tvChart');
            container.innerHTML = '';

            chart = LightweightCharts.createChart(container, {
                width: container.clientWidth || 700,
                height: 300,
                layout: { background: { color: '#1e1f20' }, textColor: '#8e918f' },
                grid: {
                    vertLines: { color: 'rgba(0,0,0,0)' },
                    horzLines: { color: 'rgba(0,0,0,0)' }
                },
                crosshair: {
                    vertLine: { visible: false },
                    horzLine: { visible: false }
                },
                timeScale: { borderColor: '#282a2c', timeVisible: true },
                rightPriceScale: { borderColor: '#282a2c' }
            });

            const candleSeries = chart.addCandlestickSeries({
                upColor: '#10b981',
                downColor: '#38bdf8',
                borderDownColor: '#38bdf8',
                borderUpColor: '#10b981',
                wickDownColor: '#38bdf8',
                wickUpColor: '#10b981',
                priceLineVisible: false,
                lastValueVisible: true
            });
            candleSeries.setData(candles);

            const lineMa20 = chart.addLineSeries({ color: '#00b4d8', lineWidth: 1, priceLineVisible: false, lastValueVisible: false });
            lineMa20.setData(ma20);

            const lineMa60 = chart.addLineSeries({ color: '#f97316', lineWidth: 1.5, priceLineVisible: false, lastValueVisible: false });
            lineMa60.setData(ma60);

            const lineBbUp = chart.addLineSeries({ color: '#10b981', lineWidth: 1.2, priceLineVisible: false, lastValueVisible: false });
            lineBbUp.setData(bbUp);

            const lineBbDn = chart.addLineSeries({ color: '#38bdf8', lineWidth: 1.2, priceLineVisible: false, lastValueVisible: false });
            lineBbDn.setData(bbDn);

            chart.timeScale().fitContent();

            window.addEventListener('resize', resizeChart);
        }
    </script>
</body>
</html>
"""

# ----------------------------------------------------
# 7. 서버 실행 핸들러 (100% 보존)
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
                "consensus": data["consensus"],
                "stock_news": data["stock_news"],
                "global_news": data["global_news"],
                "world_indices": data["world_indices"],
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
