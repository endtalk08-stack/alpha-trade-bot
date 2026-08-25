import os
import re
import io
import gc
import json
import uuid
import time
import base64
import html
import asyncio
import datetime
import threading
import urllib.parse
import xml.etree.ElementTree as ET
from http.server import HTTPServer, BaseHTTPRequestHandler
import requests
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import mplfinance as mpf
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes
)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "8942728358:AAEUYwitOIyMAeF_FD_kWKky4m9FwjApLR8")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "AQ.Ab8RN6KisWMYVxgBBJmBW3SgOAT9aJYvOPiWj42H4WR_Y56kRA")
FMP_API_KEY = os.getenv("FMP_API_KEY", "YUVC3My39vzjHpkj0SwNyHp1hXn6Q1IN")

DETAIL_REPORT_CACHE = {}

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

def generate_chart_image(df, title_text=""):
    try:
        if df is None or df.empty or len(df) < 5:
            return None
        plot_df = df.tail(45).copy()
        
        mc = mpf.make_marketcolors(up='#ef4444', down='#3b82f6', edge='inherit', wick='inherit', volume='inherit')
        s = mpf.make_mpf_style(base_mpf_style='nightclouds', marketcolors=mc, gridcolor='#2a2a2a', facecolor='#1e1e1e', edgecolor='#1e1e1e', figcolor='#1e1e1e')
        
        buf = io.BytesIO()
        mpf.plot(
            plot_df,
            type='candle',
            mav=(20,),
            volume=True,
            style=s,
            savefig=dict(fname=buf, dpi=120, bbox_inches='tight', pad_inches=0.1),
            figsize=(7, 4.2)
        )
        buf.seek(0)
        img_bytes = buf.getvalue()
        buf.close()
        plt.close('all')
        gc.collect()
        return img_bytes
    except Exception:
        plt.close('all')
        return None

def find_stock_code(query):
    clean_q = query.strip().replace(" ", "").upper()
    if clean_q.isdigit() and len(clean_q) == 6:
        return clean_q, clean_q

    for name, code in DOMESTIC_FAST_MAP.items():
        if name.replace(" ", "").upper() == clean_q:
            return code, name

    try:
        url = f"https://m.stock.naver.com/front-api/search/autoComplete?query={urllib.parse.quote(query.strip())}&target=stock"
        headers = {"User-Agent": "Mozilla/5.0"}
        res = requests.get(url, headers=headers, timeout=2.5).json()
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

def get_foreign_rss_headlines(ticker):
    headlines = []
    if not ticker or len(ticker.strip()) == 0:
        return headlines
    try:
        rss_url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US"
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(rss_url, headers=headers, timeout=2.5)
        if r.status_code == 200:
            root = ET.fromstring(r.content)
            items = root.findall('.//item')
            for it in items[:6]:
                t = it.find('title')
                if t is not None and t.text:
                    clean = html.unescape(t.text).strip()
                    clean = re.sub(r'<.*?>', '', clean).strip()
                    if clean and len(clean) > 10:
                        headlines.append(clean)
                    if len(headlines) >= 2:
                        break
    except Exception:
        pass
    return headlines

def synthesize_issue_headline_gemini(stock_name, raw_issues):
    valid_issues = []
    for i in raw_issues:
        c = html.unescape(str(i)).strip()
        c = re.sub(r'<.*?>', '', c).strip()
        if c and len(c) > 6 and not c.startswith("http"):
            valid_issues.append(c)

    if not valid_issues:
        return ""
    
    clean_name = re.sub(r'\(.*?\)', '', stock_name).strip()
    joined_issues = "\n- ".join(valid_issues)
    
    prompt = f"""
당신은 금융·증권부 수석 기자입니다.
'{clean_name}' 종목과 관련된 실시간 뉴스 원문입니다:
- {joined_issues}

위 내용에서 가장 핵심적인 팩트만 뽑아 단 1줄의 한국어 헤드라인으로 요약하세요.
- 설명, 따옴표 없이 오직 1줄의 헤드라인만 출력할 것.
"""
    try:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}"
        headers = {"Content-Type": "application/json"}
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": 0.1, "maxOutputTokens": 100}
        }
        res = requests.post(url, headers=headers, json=payload, timeout=3.5).json()
        text = res["candidates"][0]["content"]["parts"][0]["text"].strip()
        text = re.sub(r'["\'\n]', '', text).strip()
        return text if len(text) > 4 else ""
    except Exception:
        return valid_issues[0] if valid_issues else ""

def fetch_extra_fundamentals(symbol, asset_type, stock_name=""):
    raw_issues = []
    info = {
        "supply_status": "",
        "financial_status": "",
        "recent_news": ""
    }
    
    if asset_type == "overseas_stock":
        clean_sym = symbol.upper().strip()
        foreign_news = get_foreign_rss_headlines(clean_sym)
        if foreign_news: raw_issues.extend(foreign_news)

        try:
            fmp_url = f"https://financialmodelingprep.com/api/v3/key-metrics-ttm/{clean_sym}?apikey={FMP_API_KEY}"
            res = requests.get(fmp_url, timeout=2.0).json()
            if isinstance(res, list) and len(res) > 0:
                m = res[0]
                pe = m.get('peRatioTTM')
                pb = m.get('pbRatioTTM')
                roe = m.get('roeTTM')
                parts = []
                if pe: parts.append(f"PER {round(pe, 2)}배")
                if pb: parts.append(f"PBR {round(pb, 2)}배")
                if roe: parts.append(f"ROE {round(roe * 100, 2)}%")
                if parts: info["financial_status"] = " / ".join(parts)
        except Exception:
            pass

    elif asset_type == "crypto":
        c_sym = "BTC-USD" if "BTC" in symbol else ("ETH-USD" if "ETH" in symbol else f"{symbol.replace('USDT','')}-USD")
        crypto_news = get_foreign_rss_headlines(c_sym)
        if crypto_news: raw_issues.extend(crypto_news)

    else:
        headers = {"User-Agent": "Mozilla/5.0"}
        try:
            news_url = f"https://m.stock.naver.com/api/news/stock/{symbol}?pageSize=5&page=1"
            n_res = requests.get(news_url, headers=headers, timeout=2.0).json()
            news_items = []
            if isinstance(n_res, dict):
                news_items = n_res.get("items", []) or n_res.get("newsList", []) or n_res.get("result", [])
            elif isinstance(n_res, list):
                news_items = n_res
            
            if news_items:
                for n in news_items:
                    title_txt = n.get("title") or n.get("tit") or n.get("itemTitle") or ""
                    clean_t = html.unescape(title_txt).strip()
                    clean_t = re.sub(r'<.*?>', '', clean_t).strip()
                    if clean_t and len(clean_t) > 6 and clean_t not in raw_issues:
                        raw_issues.append(clean_t)
        except Exception:
            pass

        try:
            integ_url = f"https://m.stock.naver.com/api/stock/{symbol}/integration"
            res = requests.get(integ_url, headers=headers, timeout=2.0).json()
            total_infos = res.get("totalInfos", [])
            per_val, pbr_val, for_rate = "", "", ""
            for item in total_infos:
                k = item.get("code")
                v = item.get("value")
                if k == "foreignRate" and v: for_rate = v
                elif k == "per" and v: per_val = v
                elif k == "pbr" and v: pbr_val = v
            
            if for_rate: info["supply_status"] = f"외국인 지분율 {for_rate}%"
            parts = []
            if per_val: parts.append(f"PER {per_val}배")
            if pbr_val: parts.append(f"PBR {pbr_val}배")
            if parts: info["financial_status"] = " / ".join(parts)
        except Exception:
            pass

    info["recent_news"] = synthesize_issue_headline_gemini(stock_name, raw_issues)
    return info

def get_crypto_multi_exchange_data(symbol="BTCUSDT", limit=500):
    clean_sym = symbol.replace("KRW-", "").replace("-", "").upper()
    if not clean_sym.endswith("USDT") and not clean_sym.endswith("USD"):
        clean_sym = f"{clean_sym}USDT"
    
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        b_url = f"https://api.binance.com/api/v3/klines?symbol={clean_sym}&interval=1d&limit={limit}"
        res = requests.get(b_url, headers=headers, timeout=2.5).json()
        if isinstance(res, list) and len(res) > 5:
            records = [{"Date": pd.to_datetime(int(item[0]), unit='ms'), "Open": float(item[1]), "High": float(item[2]), "Low": float(item[3]), "Close": float(item[4]), "Volume": float(item[5])} for item in res]
            df = pd.DataFrame(records).set_index("Date").sort_index()
            curr_p = float(df['Close'].iloc[-1])
            return curr_p, f"{curr_p:,.2f} 달러 (USDT)", df
    except Exception:
        pass
    return None, None, None

def get_us_stock_fmp_data(symbol, years=1):
    clean_sym = symbol.upper().strip()
    try:
        y_url = f"https://query1.finance.yahoo.com/v8/finance/chart/{clean_sym}?range={years}y&interval=1d&includePrePost=true"
        y_res = requests.get(y_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=2.5).json()
        result = y_res.get("chart", {}).get("result", [])
        if result:
            ts = result[0].get("timestamp", [])
            quote = result[0].get("indicators", {}).get("quote", [{}])[0]
            records = [{"Date": pd.to_datetime(ts[i], unit='s'), "Open": float(quote['open'][i]), "High": float(quote['high'][i]), "Low": float(quote['low'][i]), "Close": float(quote['close'][i]), "Volume": float(quote['volume'][i] or 0.0)} for i in range(len(ts)) if quote.get('open') and quote['open'][i] is not None]
            if records:
                df = pd.DataFrame(records).set_index("Date").sort_index()
                curr_p = float(df['Close'].iloc[-1])
                return float(curr_p), f"{float(curr_p):,.2f} 달러", df
    except Exception:
        pass
    return None, None, None

def get_naver_realtime_price(code):
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        url = f"https://polling.finance.naver.com/api/realtime/domestic/stock/{code}"
        res = requests.get(url, headers=headers, timeout=2.0).json()
        datas = res.get("datas", [])
        if datas:
            p = str(datas[0].get("closePrice", "")).replace(",", "").strip()
            if p and p.replace(".", "").isdigit():
                return float(p), f"{int(float(p)):,} 원"
    except Exception:
        pass
    return None, None

def fetch_domestic_stock_candles(code, anchor_price=None, years=1):
    headers = {"User-Agent": "Mozilla/5.0"}
    df_result = None
    for suffix in [".KS", ".KQ"]:
        try:
            y_url = f"https://query1.finance.yahoo.com/v8/finance/chart/{code}{suffix}?range={years}y&interval=1d"
            y_res = requests.get(y_url, headers=headers, timeout=2.5).json()
            result = y_res.get("chart", {}).get("result", [])
            if result:
                ts = result[0].get("timestamp", [])
                quote = result[0].get("indicators", {}).get("quote", [{}])[0]
                records = [{"Date": pd.to_datetime(ts[i], unit='s'), "Open": float(quote['open'][i]), "High": float(quote['high'][i]), "Low": float(quote['low'][i]), "Close": float(quote['close'][i]), "Volume": float(quote['volume'][i] or 0.0)} for i in range(len(ts)) if quote.get('open') and quote['open'][i] is not None]
                if len(records) >= 30:
                    df_result = pd.DataFrame(records).set_index("Date").sort_index()
                    break
        except Exception:
            pass

    if df_result is not None and anchor_price is not None and anchor_price > 0:
        last_close = float(df_result['Close'].iloc[-1])
        if abs(last_close - anchor_price) / anchor_price > 0.3:
            scale_ratio = anchor_price / last_close
            df_result['Open'] = df_result['Open'] * scale_ratio
            df_result['High'] = df_result['High'] * scale_ratio
            df_result['Low'] = df_result['Low'] * scale_ratio
            df_result['Close'] = df_result['Close'] * scale_ratio

    return df_result

def backtest_alpha_strategy(df, years=1, asset_type="domestic_stock"):
    if df is None or len(df) < 50:
        return None

    try:
        trades = []
        closes = df['Close'].values
        highs = df['High'].values
        lows = df['Low'].values
        
        tr = np.maximum(highs[1:] - lows[1:], np.maximum(np.abs(highs[1:] - closes[:-1]), np.abs(lows[1:] - closes[:-1])))
        atr = pd.Series(tr).rolling(14).mean().values
        ma20 = pd.Series(closes).rolling(20).mean().values

        for i in range(30, len(closes) - 10):
            curr_c = closes[i]
            curr_ma = ma20[i]
            curr_atr = atr[i-1] if i-1 < len(atr) and not np.isnan(atr[i-1]) else curr_c * 0.03
            
            if curr_c > curr_ma and closes[i-1] < closes[i-2]:
                entry_p = curr_c
                target_p = entry_p + (curr_atr * 2.2)
                stop_p = entry_p - (curr_atr * 1.1)
                
                for future in range(i + 1, min(i + 16, len(closes))):
                    if highs[future] >= target_p:
                        ret = (target_p - entry_p) / entry_p
                        trades.append(("WIN", ret))
                        break
                    elif lows[future] <= stop_p:
                        ret = (stop_p - entry_p) / entry_p
                        trades.append(("LOSS", ret))
                        break

        if len(trades) >= 5:
            win_trades = [t[1] for t in trades if t[0] == "WIN"]
            loss_trades = [abs(t[1]) for t in trades if t[0] == "LOSS"]
            total_cnt = len(trades)
            win_cnt = len(win_trades)
            win_rate = round((win_cnt / total_cnt) * 100, 1)
            cum_ret = round(float(sum([t[1] for t in trades]) * 100), 1)
            sum_win = sum(win_trades)
            sum_loss = sum(loss_trades)
            pf = round(sum_win / sum_loss, 2) if sum_loss > 0 else 2.10
            return {"total": total_cnt, "win": win_cnt, "loss": total_cnt - win_cnt, "win_rate": win_rate, "cum_ret": cum_ret, "pf": pf}
    except Exception:
        pass

    return None

def format_backtest_text(formal_name, symbol, bt, years=1):
    if not bt:
        return f"🏆 {formal_name} ({symbol})\n\n백테스트 데이터가 충분하지 않습니다."
    return f"🏆 {formal_name} ({symbol}) {years}년 검증\n\n승률: {bt['win_rate']:.1f}%\n누적 수익: {bt['cum_ret']:+.1f}%\n손익비: {bt['pf']:.2f} : 1"

def perform_ict_quant_analysis(raw_price, df, asset_type="domestic_stock"):
    if df is None or len(df) < 5:
        return None

    is_krw = "domestic" in asset_type
    round_fn = (lambda v: round(float(v), 0)) if is_krw else (lambda v: round(float(v), 2))
    fmt_price = lambda v: f"{int(v):,}" if is_krw else f"{v:,.2f}"

    high_low = df['High'] - df['Low']
    calc_atr = float(high_low.tail(14).mean())
    atr_val = round_fn(calc_atr) if calc_atr > 0 else round_fn(raw_price * 0.03)

    high_20d = round_fn(df['High'].tail(20).max())
    ma20 = round_fn(df['Close'].tail(20).mean())
    ma60 = round_fn(df['Close'].tail(60).mean()) if len(df) >= 60 else ma20

    v_mean5 = df['Volume'].tail(5).mean()
    v_curr = df['Volume'].iloc[-1]
    vol_ratio = int((v_curr / v_mean5) * 100) if v_mean5 > 0 else 100

    delta = df['Close'].diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.rolling(window=14, min_periods=14).mean()
    avg_loss = loss.rolling(window=14, min_periods=14).mean()
    rs = avg_gain / avg_loss
    rsi_series = 100 - (100 / (1 + rs))
    calc_rsi = rsi_series.iloc[-1]
    rsi_val = round(float(calc_rsi), 1) if not np.isnan(calc_rsi) else 50.0

    ict_entry1_high = round_fn(raw_price)
    ict_entry1_low = round_fn(raw_price - (atr_val * 0.7))
    ict_entry2_high = round_fn(ict_entry1_low)
    ict_entry2_low = round_fn(raw_price - (atr_val * 1.5))
    ict_stop = round_fn(ict_entry2_low - (atr_val * 0.6))

    for idx in range(len(df) - 2, max(0, len(df) - 22), -1):
        c_prev = df['Close'].iloc[idx]
        o_prev = df['Open'].iloc[idx]
        c_next = df['Close'].iloc[idx + 1]
        
        if c_prev < o_prev and c_next > o_prev:
            ob_high = max(c_prev, o_prev)
            ob_low = min(c_prev, o_prev)
            if ob_high <= raw_price * 1.02:
                ict_entry1_high = round_fn(raw_price)
                ict_entry1_low = round_fn(ob_high)
                ict_entry2_high = round_fn(ob_high)
                ict_entry2_low = round_fn(ob_low)
                ict_stop = round_fn(ob_low - (atr_val * 0.5))
                break

    avg_e = (ict_entry1_low + ict_entry1_high) / 2
    risk_dist = max(avg_e - ict_stop, raw_price * 0.02)
    ict_target1 = round_fn(avg_e + (risk_dist * 2.0))
    ict_target2 = round_fn(avg_e + (risk_dist * 3.5))

    curr_close = float(df['Close'].iloc[-1])
    
    # 방향성 판별 (단기 롱 vs 단기 숏/관망)
    if (curr_close >= high_20d * 0.98 and rsi_val >= 62) or (vol_ratio >= 150 and curr_close > ma20):
        regime_key = "BREAKOUT"
        direction = "단기 롱"
    elif rsi_val < 38 or (curr_close < ma60 and rsi_val < 42):
        regime_key = "OVERSOLD"
        direction = "단기 롱"
    elif abs(ma20 - ma60) / max(ma20, ma60) < 0.025 and 42 <= rsi_val <= 58:
        regime_key = "BOX"
        direction = "단기 롱"
    elif curr_close < ma20 and curr_close < ma60:
        regime_key = "PULLBACK"
        direction = "단기 숏"
    else:
        regime_key = "PULLBACK"
        direction = "단기 롱"

    base_score = 2.0
    if curr_close >= ma20: base_score += 0.9
    if curr_close >= ma60: base_score += 0.6
    if regime_key == "PULLBACK": base_score += 0.9
    elif regime_key == "BREAKOUT": base_score += 0.8
    elif regime_key == "OVERSOLD": base_score += 0.6
    elif regime_key == "BOX": base_score += 0.5
    if vol_ratio >= 110: base_score += 0.5
    elif 70 <= vol_ratio < 110: base_score += 0.3

    score_val = round(min(5.0, max(1.0, base_score)), 1)
    if score_val >= 4.5: stars_str = "★★★★★"
    elif score_val >= 3.8: stars_str = "★★★★☆"
    elif score_val >= 2.8: stars_str = "★★★☆☆"
    elif score_val >= 1.8: stars_str = "★★☆☆☆"
    else: stars_str = "★☆☆☆☆"

    unit_str = "원" if is_krw else "달러"
    if raw_price >= ma60 and raw_price >= ma20:
        ma_fact_desc = f"현재 주가가 20일선({fmt_price(ma20)}{unit_str}) 및 60일선({fmt_price(ma60)}{unit_str}) 상단에 위치하여 정배열 우상향 추세를 형성하고 있습니다."
    elif raw_price < ma60 and raw_price >= ma20:
        ma_fact_desc = f"현재 주가가 단기 20일선({fmt_price(ma20)}{unit_str}) 지지를 받으며 반등 중이나, 상단 60일선({fmt_price(ma60)}{unit_str})이 주요 중기 저항선으로 작용하고 있습니다."
    elif raw_price < ma20 and raw_price < ma60:
        ma_fact_desc = f"현재 주가가 20일선({fmt_price(ma20)}{unit_str}) 및 60일선({fmt_price(ma60)}{unit_str}) 하단에 위치한 단기 역배열 국면으로, 저항 매물 소화 및 60일선 회복 여부가 핵심 분기점입니다."
    else:
        ma_fact_desc = f"현재 주가가 20일선({fmt_price(ma20)}{unit_str}) 및 60일선({fmt_price(ma60)}{unit_str}) 사이에서 방향성 탐색 매물 공방을 이어가고 있습니다."

    atr_display = fmt_price(atr_val)

    REGIME_META = {
        "BREAKOUT": {
            "pattern_name": "상승 모멘텀 돌파형",
            "entry1_label": "주요 기관 1차 지지 밴드",
            "entry2_label": "구조적 하단 방어 마지노선",
            "target1_label": "직전 고점 유동성 청산",
            "target2_label": "상단 저항선 전량 청산",
            "stop_label": "모멘텀 기준봉 훼손 손절",
            "guide": f"5일 평균 대비 거래량 {vol_ratio}퍼센트 확인 시 1차 분할 착수하며 장중 변동폭 {atr_display} 내외의 저항 돌파 시 매수세 유입 확인 후 집행"
        },
        "PULLBACK": {
            "pattern_name": "우상향 눌림목 안착형",
            "entry1_label": "주요 기관 1차 지지 밴드",
            "entry2_label": "구조적 하단 방어 마지노선",
            "target1_label": "직전 고점 유동성 청산",
            "target2_label": "상단 저항선 전량 청산",
            "stop_label": "기술적 지지선 이탈 손절",
            "guide": f"20일선 지지 및 체결강도 확인 시 1차 착수하며 장중 변동폭 {atr_display} 내외의 단기 음봉에서 지지대 안착 확인 후 집행"
        },
        "OVERSOLD": {
            "pattern_name": "낙폭과대 기술적 반등형",
            "entry1_label": "주요 기관 1차 지지 밴드",
            "entry2_label": "구조적 하단 방어 마지노선",
            "target1_label": "직전 고점 유동성 청산",
            "target2_label": "상단 저항선 전량 청산",
            "stop_label": "구조적 최저점 이탈 손절",
            "guide": f"실시간 RSI {rsi_val} 침체권 매물 소화 확인 후 2차 지지 밴드 중심 분할 접근하며 변동폭 {atr_display} 수준의 하방 경직성 확인 후 집행"
        },
        "BOX": {
            "pattern_name": "박스권 횡보 수렴형",
            "entry1_label": "주요 기관 1차 지지 밴드",
            "entry2_label": "구조적 하단 방어 마지노선",
            "target1_label": "직전 고점 유동성 청산",
            "target2_label": "상단 저항선 전량 청산",
            "stop_label": "박스권 하단 이탈 손절",
            "guide": f"거래량 {vol_ratio}퍼센트 수준의 수급 숨고르기 국면이므로 1차 밴드 하단 지지 여부 및 호가창 매도벽 소화 확인 후 집행"
        }
    }

    meta = REGIME_META.get(regime_key, REGIME_META["PULLBACK"])

    return {
        "rsi": rsi_val,
        "score": f"{score_val:.1f}",
        "stars": stars_str,
        "regime_key": regime_key,
        "pattern_name": meta["pattern_name"],
        "direction": direction,
        "entry1_label": meta["entry1_label"],
        "entry2_label": meta["entry2_label"],
        "target1_label": meta["target1_label"],
        "target2_label": meta["target2_label"],
        "stop_label": meta["stop_label"],
        "guide_text": meta["guide"],
        "ict_entry1_low": ict_entry1_low,
        "ict_entry1_high": ict_entry1_high,
        "ict_entry2_low": ict_entry2_low,
        "ict_entry2_high": ict_entry2_high,
        "ict_target1": ict_target1,
        "ict_target2": ict_target2,
        "ict_stop": ict_stop,
        "atr": atr_val,
        "high_20d": high_20d,
        "ma20": ma20,
        "ma60": ma60,
        "ma_fact_desc": ma_fact_desc,
        "vol_ratio": vol_ratio
    }

def generate_deep_report_gemini(stock_name, symbol, price, an, extra_info):
    is_krw = "원" in str(price)
    unit = " 원" if is_krw else " 달러"
    fmt = lambda v: f"{int(v):,}{unit}" if is_krw else f"{v:,.2f}{unit}"
    clean_name = re.sub(r'\(.*?\)', '', stock_name).strip()
    pattern_name = an.get('pattern_name', '우상향 눌림목 안착형')

    news_text = f"- 핵심 뉴스/재료: {extra_info.get('recent_news')}\n" if extra_info.get('recent_news') else ""
    fund_text = f"- 펀더멘털 지표: {extra_info.get('financial_status')}\n" if extra_info.get('financial_status') else ""
    supply_text = f"- 수급 현황: {extra_info.get('supply_status')}\n" if extra_info.get('supply_status') else ""

    prompt = f"""
당신은 최고 수준의 월스트리트 기관 퀀트 헤드 애널리스트입니다.
아래 제공된 '실제 팩트 데이터'만을 기반으로 '{clean_name}'에 대한 전문 퀀트 심층 리포트를 작성하세요.

[실제 팩트 데이터]
- 종목명: {clean_name} ({symbol}) / 현재가: {price}
- 기술적 패턴: {pattern_name}
{news_text}{fund_text}{supply_text}- 이평선 실제 팩트: {an['ma_fact_desc']}
- 5일 평균 거래량 대비 비율: {an['vol_ratio']}퍼센트 / 실시간 RSI: {an['rsi']} / 일일 변동폭(ATR): {fmt(an['atr'])}

[작성 원칙]
1. 없는 데이터를 지어내지 마십시오. (뉴스나 재무 데이터가 없으면 언급하지 말 것)
2. 비전문적 은어('세력', '개미' 등) 사용 절대 금지. 정통 금융 전문 용어만 사용할 것.
3. 첫 문장에 '...분석 결과입니다' 같은 불필요한 서두나 인사말을 쓰지 말고 즉시 본문 분석으로 들어갈 것.
4. 구체적인 매수가/목표가/손절가 금액 숫자를 본문 글에 적지 마십시오.
5. 이평선 상태는 주어진 [이평선 실제 팩트]의 내용({an['ma_fact_desc']}) 그대로 서술할 것.
6. 현재 국면('{pattern_name}')에 적합한 실전 분할 대응 전략과 기준 지지선 훼손 시 리스크 관리 시나리오로 명확한 결론을 내릴 것.
"""
    try:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}"
        headers = {"Content-Type": "application/json"}
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": 0.2, "maxOutputTokens": 1200}
        }
        res = requests.post(url, headers=headers, json=payload, timeout=5.0).json()
        return res["candidates"][0]["content"]["parts"][0]["text"].strip()
    except Exception:
        return f"기술적으로 {an['ma_fact_desc']}\n\n일일 변동폭(ATR) {fmt(an['atr'])} 및 실시간 RSI {an['rsi']} 지표를 나타내고 있으며, 현재 국면에서는 주요 기관 수급 지지대를 활용한 정밀 분할 접근과 지지선 훼손 시 리스크 관리가 유효합니다."

def generate_reliable_report(stock_name, symbol, price, an, extra_info):
    is_krw = "원" in str(price)
    unit = " 원" if is_krw else " 달러"
    fmt = lambda v: f"{int(v):,}{unit}" if is_krw else f"{v:,.2f}{unit}"

    news_raw = str(extra_info.get('recent_news', '')).strip()
    detail = generate_deep_report_gemini(stock_name, symbol, price, an, extra_info)

    curr_p = an['ict_entry1_high']
    ret1 = ((an['ict_target1'] - curr_p) / curr_p) * 100
    ret2 = ((an['ict_target2'] - curr_p) / curr_p) * 100
    risk = ((curr_p - an['ict_stop']) / curr_p) * 100

    issue_block = f"gaemiGTP 이슈\n👉 {news_raw}\n\n" if news_raw else ""

    combined_body = f"""{issue_block}현재가 {price}

현재 트레이딩 적합도 {an['stars']} (점수 {an['score']})

{detail}""".strip()

    signal_only = f"""스마트머니 퀀트 타점

방향성: {an['direction']}

1차 진입: {an['entry1_label']}
👉 {fmt(an['ict_entry1_low'])} ~ {fmt(an['ict_entry1_high'])}

2차 진입: {an['entry2_label']}
👉 {fmt(an['ict_entry2_low'])} ~ {fmt(an['ict_entry2_high'])}

1차 목표: {an['target1_label']}
👉 {fmt(an['ict_target1'])} (+{ret1:.1f}%)

2차 목표: {an['target2_label']}
👉 {fmt(an['ict_target2'])} (+{ret2:.1f}%)

칼손절: {an['stop_label']}
👉 {fmt(an['ict_stop'])} (-{risk:.1f}%)

gaemiGTP 가이드
{an['guide_text']}""".strip()

    return combined_body, signal_only, news_raw

def fetch_technical_quant_data(symbol, asset_type, stock_name=""):
    quant_info = {}
    curr_price_num, curr_price_str, df = None, None, None
    if asset_type == "crypto":
        curr_price_num, curr_price_str, df = get_crypto_multi_exchange_data(symbol, limit=200)
    elif asset_type == "overseas_stock":
        curr_price_num, curr_price_str, df = get_us_stock_fmp_data(symbol, years=1)
    elif asset_type == "domestic_stock":
        curr_price_num, curr_price_str = get_naver_realtime_price(symbol)
        df = fetch_domestic_stock_candles(symbol, anchor_price=curr_price_num, years=1)
        if curr_price_num is None and df is not None and not df.empty:
            curr_price_num = float(df['Close'].iloc[-1])
            curr_price_str = f"{int(curr_price_num):,} 원"

    if curr_price_num is None or df is None:
        return None

    analysis_res = perform_ict_quant_analysis(curr_price_num, df, asset_type=asset_type)
    if not analysis_res:
        return None

    chart_bytes = generate_chart_image(df, title_text=f"{stock_name} ({symbol})")

    quant_info['current_price'] = curr_price_str
    quant_info['analysis'] = analysis_res
    quant_info['extra_info'] = fetch_extra_fundamentals(symbol, asset_type, stock_name)
    quant_info['bt_info'] = backtest_alpha_strategy(df, years=1, asset_type=asset_type)
    quant_info['chart_bytes'] = chart_bytes
    return quant_info

def run_full_analysis_process(query):
    clean_q = query.replace(" ", "").upper()
    
    if clean_q.endswith("USDT") or clean_q in CRYPTO_MAP or any(c in clean_q for c in ["BTC", "ETH", "코인", "비트코인"]):
        asset_type = "crypto"
        symbol = CRYPTO_MAP.get(clean_q, clean_q if clean_q.endswith("USDT") else f"{clean_q}USDT")
        formal_name = clean_q
    elif clean_q in US_STOCK_MAP:
        asset_type = "overseas_stock"
        symbol = US_STOCK_MAP[clean_q]
        formal_name = query.replace("(", "").replace(")", "").strip()
    elif any(c.isupper() for c in query) and not any('\uac00' <= char <= '\ud7a3' for char in query):
        asset_type = "overseas_stock"
        symbol = query.upper().strip()
        formal_name = query.strip()
    else:
        code, name = find_stock_code(query)
        if not code: return None
        symbol, formal_name, asset_type = code, name, "domestic_stock"

    quant_data = fetch_technical_quant_data(symbol, asset_type, formal_name)
    if not quant_data: return None

    summary_text, detail_text, news_raw = generate_reliable_report(
        formal_name, symbol, quant_data['current_price'],
        quant_data['analysis'], quant_data['extra_info']
    )
    
    chart_b64 = ""
    if quant_data.get('chart_bytes'):
        chart_b64 = base64.b64encode(quant_data['chart_bytes']).decode('utf-8')

    return {
        "formal_name": formal_name,
        "symbol": symbol,
        "price": quant_data['current_price'],
        "summary": summary_text,
        "detail": detail_text,
        "news_raw": news_raw,
        "chart_bytes": quant_data.get('chart_bytes'),
        "chart_b64": chart_b64,
        "bt_info": quant_data.get('bt_info')
    }

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("gaemiGTP 가동 중입니다. 종목명을 입력하세요 (예: 하이닉스, 엔비디아)\n백테스트 검증은 '검증 종목명' (예: 검증 하이닉스)을 입력하세요.")

async def analyze(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.message.text.strip()
    
    if query.startswith("검증 ") or query.startswith("검증") or query.startswith("/검증") or query.startswith("/verify"):
        clean_sub = re.sub(r"^(/검증|검증|/verify)", "", query).strip()
        if not clean_sub:
            await update.message.reply_text("검증할 종목명을 입력해주세요.\n예: 검증 하이닉스")
            return
        status_msg = await update.message.reply_text(f"🔍 {clean_sub} 백테스트 검증 중...")
        res = run_full_analysis_process(clean_sub)
        if not res:
            await status_msg.edit_text(f"❌ '{clean_sub}' 종목을 찾지 못했습니다.")
            return
        bt_text = format_backtest_text(res['formal_name'], res['symbol'], res['bt_info'], years=1)
        await status_msg.edit_text(bt_text)
        return

    status_msg = await update.message.reply_text(f"{query} 분석 중...")
    res = run_full_analysis_process(query)
    if not res:
        await status_msg.edit_text(f"❌ '{query}' 종목을 찾지 못했습니다.")
        return
    
    await status_msg.delete()
    
    if res.get("chart_bytes"):
        await update.message.reply_photo(photo=io.BytesIO(res["chart_bytes"]))
    
    keyboard = [[InlineKeyboardButton("gaemiGTP 시그널", callback_data=f"detail_{res['symbol']}")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    DETAIL_REPORT_CACHE[res['symbol']] = res['detail']
    
    display_title = f"{res['formal_name']} ({res['symbol']})" if res['symbol'] not in res['formal_name'] else res['formal_name']
    msg_text = f"**{display_title}**\n\n{res['summary']}"
    await update.message.reply_text(msg_text, reply_markup=reply_markup)

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    if data.startswith("detail_"):
        symbol = data.replace("detail_", "")
        detail_text = DETAIL_REPORT_CACHE.get(symbol, "시그널을 불러올 수 없습니다.")
        await query.message.reply_text(f"🎯 **gaemiGTP 시그널**\n\n{detail_text}")

class ApiAndWebServer(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def do_GET(self):
        try:
            parsed = urllib.parse.urlparse(self.path)
            params = urllib.parse.parse_qs(parsed.query)

            if parsed.path == "/api/analyze":
                q = params.get('q', [''])[0].strip()
                res = run_full_analysis_process(q) if q else None
                if not res:
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
                self.wfile.write(json.dumps({
                    "success": True,
                    "name": res["formal_name"],
                    "symbol": res["symbol"],
                    "price": res["price"],
                    "summary": res["summary"],
                    "detail": res["detail"],
                    "news_raw": res.get("news_raw", ""),
                    "chart": res["chart_b64"]
                }, ensure_ascii=False).encode('utf-8'))
                return

            html_content = """<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>gaemiGTP - 10초 금융 퀀트 분석 엔진</title>
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
        .model-pill-arrow { font-size: 0.6rem; color: #8e918f; transition: transform 0.2s; }
        
        .model-dropdown-menu { display: none; position: absolute; bottom: calc(100% + 10px); right: 0; width: 210px; max-width: 80vw; background: #282a2c; border: none; border-radius: 16px; padding: 6px; box-shadow: 0 10px 30px rgba(0,0,0,0.5); z-index: 100; }
        .model-dropdown-item { padding: 9px 12px; border-radius: 10px; cursor: pointer; transition: background 0.15s; display: flex; flex-direction: column; }
        .model-dropdown-item:hover { background: rgba(255, 255, 255, 0.08); }
        .model-dropdown-item.active { background: rgba(255, 255, 255, 0.12); }
        .item-title { font-size: 0.88rem; font-weight: 600; color: #ffffff; display: flex; justify-content: space-between; align-items: center; }
        .item-sub { font-size: 0.73rem; color: #8e918f; margin-top: 2px; }
        .item-check { color: #e3e3e3; font-weight: bold; display: none; font-size: 0.85rem; }
        .model-dropdown-item.active .item-check { display: inline; }

        .send-btn { width: 34px; height: 34px; border-radius: 50%; background: #37393b; border: none; color: #ffffff; display: flex; align-items: center; justify-content: center; cursor: pointer; flex-shrink: 0; transition: background 0.2s; }
        .send-btn:hover { background: #4a4d50; }
        .send-btn svg { width: 17px; height: 17px; fill: currentColor; }

        .popular-tags { display: flex; gap: 8px; margin-top: 20px; flex-wrap: wrap; justify-content: center; }
        .tag-btn { background: #282a2c; border: none; border-radius: 16px; padding: 6px 13px; font-size: 0.82rem; color: #c4c7c5; cursor: pointer; transition: background 0.2s; white-space: nowrap; }
        .tag-btn:hover { background: #37393b; color: #fff; }

        #result-scroll-view { display: none; position: absolute; top: 56px; bottom: 84px; left: 0; width: 100%; overflow-y: auto; padding: 16px; }
        .result-inner { max-width: 720px; margin: 0 auto; }
        
        .chart-img-box { width: 100%; background: #282a2c; border: none; border-radius: 16px; margin-bottom: 16px; overflow: hidden; display: flex; justify-content: center; align-items: center; }
        .chart-img-box img { width: 100%; height: auto; display: block; }

        .card { background: #282a2c; border: none; border-radius: 16px; padding: 20px; margin-bottom: 20px; line-height: 1.65; }
        .card-header { font-size: 1.15rem; font-weight: 600; color: #f2f2f2; margin-bottom: 14px; }
        
        .issue-wrap { margin-bottom: 16px; }
        .issue-title { font-size: 0.9rem; font-weight: 600; color: #c4c7c5; margin-bottom: 4px; }
        .issue-highlight { font-size: 0.93rem; font-weight: 600; color: #fef08a; display: inline-block; animation: pulseFade 2.4s infinite ease-in-out; }
        @keyframes pulseFade {
            0% { opacity: 0.35; transform: scale(0.995); filter: brightness(0.85); }
            50% { opacity: 1; transform: scale(1); filter: brightness(1.25) drop-shadow(0 0 6px rgba(254, 240, 138, 0.4)); }
            100% { opacity: 0.35; transform: scale(0.995); filter: brightness(0.85); }
        }

        .summary-body { white-space: pre-wrap; color: #e3e3e3; font-size: 0.93rem; }
        .detail-btn { width: 100%; background: #37393b; border: none; color: #e3e3e3; padding: 11px; border-radius: 10px; font-weight: 600; cursor: pointer; margin-top: 14px; transition: background 0.2s; font-size: 0.9rem; }
        .detail-btn:hover { background: #45484c; }
        .detail-content { display: none; margin-top: 16px; padding-top: 16px; border-top: 1px solid #37393b; color: #c4c7c5; white-space: pre-wrap; font-size: 0.92rem; }
        
        #bottom-bar { display: none; position: fixed; bottom: 0; left: 0; width: 100%; height: 76px; background: linear-gradient(180deg, rgba(30,31,32,0) 0%, rgba(30,31,32,1) 40%); align-items: center; justify-content: center; padding: 0 12px 6px; z-index: 50; }
        .loading { display: none; text-align: center; color: #8e918f; margin: 40px 0; font-size: 0.92rem; }

        @media (max-width: 600px) {
            .hero-title { font-size: 2.6rem; }
            .search-box { padding: 3px 5px 3px 12px; }
            .search-box input { font-size: 0.88rem; }
            .model-pill-name { font-size: 0.8rem; }
            .model-pill-btn { padding: 4px 6px; }
            .card { padding: 16px; }
        }
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
                <input type="text" id="centerInput" placeholder="종목명 입력 (예: SK하이닉스, NVDA, BTC, QQQUSDT)" onkeypress="if(event.keyCode==13) doSearch(this.value);" autocomplete="off">
                
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
                    <svg viewBox="0 0 24 24"><path d="M12 4L12 20M12 4L5 11M12 4L19 11" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" fill="none"/></svg>
                </button>
            </div>
        </div>
        <div class="popular-tags" id="popularTags">
            <button class="tag-btn" onclick="doSearch('SK하이닉스')">🔥 SK하이닉스</button>
            <button class="tag-btn" onclick="doSearch('알테오젠')">🧬 알테오젠</button>
            <button class="tag-btn" onclick="doSearch('에코프로')">🔋 에코프로</button>
            <button class="tag-btn" onclick="doSearch('엔비디아')">🚀 엔비디아</button>
            <button class="tag-btn" onclick="doSearch('비트코인')">₿ 비트코인</button>
        </div>
    </div>

    <div id="result-scroll-view">
        <div class="result-inner">
            <div class="loading" id="loading">⚡ 실시간 수급/재무/뉴스 및 퀀트 팩터를 분석 중입니다...</div>
            <div id="result-card-wrap" style="display:none;">
                <div class="chart-img-box" id="chartBox" style="display:none;">
                    <img id="chartImg" src="" alt="종목 캔들 차트">
                </div>
                <div class="card">
                    <div class="card-header" id="resTitle"></div>
                    
                    <div class="issue-wrap" id="issueWrap" style="display:none;">
                        <div class="issue-title">gaemiGTP 이슈</div>
                        <div class="issue-highlight" id="issueText"></div>
                    </div>

                    <div class="summary-body" id="resSummary"></div>
                    <button class="detail-btn" id="toggleDetailBtn" onclick="toggleDetail()">gaemiGTP 시그널</button>
                    <div class="detail-content" id="resDetail"></div>
                </div>
            </div>
        </div>
    </div>

    <div id="bottom-bar">
        <div class="search-container">
            <div class="search-box">
                <input type="text" id="bottomInput" placeholder="다른 종목명 입력 (예: 테슬라, 카카오, SKYUSDT)" onkeypress="if(event.keyCode==13) doSearch(this.value);" autocomplete="off">
                
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
                    <svg viewBox="0 0 24 24"><path d="M12 4L12 20M12 4L5 11M12 4L19 11" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" fill="none"/></svg>
                </button>
            </div>
        </div>
    </div>

    <script>
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
            document.getElementById('chartBox').style.display = 'none';
            document.getElementById('centerInput').value = '';
            document.getElementById('bottomInput').value = '';
        }

        async function doSearch(query) {
            const cleanQ = query.trim();
            if (!cleanQ) return;

            document.getElementById('main-view').style.display = 'none';
            document.getElementById('result-scroll-view').style.display = 'block';
            document.getElementById('bottom-bar').style.display = 'flex';
            
            document.getElementById('loading').style.display = 'block';
            document.getElementById('result-card-wrap').style.display = 'none';
            document.getElementById('chartBox').style.display = 'none';
            document.getElementById('resDetail').style.display = 'none';
            document.getElementById('toggleDetailBtn').innerText = 'gaemiGTP 시그널';
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
                
                if (data.chart) {
                    document.getElementById('chartImg').src = 'data:image/png;base64,' + data.chart;
                    document.getElementById('chartBox').style.display = 'block';
                } else {
                    document.getElementById('chartBox').style.display = 'none';
                }

                let finalTitle = data.name;
                if (!finalTitle.includes(data.symbol)) {
                    finalTitle = finalTitle + ' (' + data.symbol + ')';
                }
                document.getElementById('resTitle').innerText = finalTitle;
                
                if (data.news_raw && data.news_raw.trim().length > 0) {
                    document.getElementById('issueText').innerText = "👉 " + data.news_raw.trim();
                    document.getElementById('issueWrap').style.display = 'block';
                } else {
                    document.getElementById('issueWrap').style.display = 'none';
                }

                let cleanSummary = data.summary;
                if (cleanSummary.includes("현재가")) {
                    cleanSummary = cleanSummary.substring(cleanSummary.indexOf("현재가"));
                }
                document.getElementById('resSummary').innerText = cleanSummary;

                document.getElementById('resDetail').innerText = data.detail;
                document.getElementById('loading').style.display = 'none';
                document.getElementById('result-card-wrap').style.display = 'block';
                document.getElementById('result-scroll-view').scrollTo({top: 0, behavior: 'smooth'});
            } catch (err) {
                alert('서버와 통신할 수 없습니다.');
                document.getElementById('loading').style.display = 'none';
            }
        }

        function toggleDetail() {
            const d = document.getElementById('resDetail');
            const btn = document.getElementById('toggleDetailBtn');
            if (d.style.display === 'block') {
                d.style.display = 'none';
                btn.innerText = 'gaemiGTP 시그널';
            } else {
                d.style.display = 'block';
                btn.innerText = 'gaemiGTP 시그널 닫기';
            }
        }
    </script>
</body>
</html>"""
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(html_content.encode('utf-8'))
        except Exception:
            self.send_response(500)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({"error": "서버 처리 중 오류가 발생했습니다."}, ensure_ascii=False).encode('utf-8'))

def run_web_server():
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), ApiAndWebServer)
    server.serve_forever()

if __name__ == "__main__":
    t = threading.Thread(target=run_web_server, daemon=True)
    t.start()
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, analyze))
    app.add_handler(CallbackQueryHandler(button_callback))
    app.run_polling(drop_pending_updates=True)
