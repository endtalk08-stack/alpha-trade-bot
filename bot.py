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

def synthesize_issue_headline_local(stock_name, raw_issues):
    """Return the strongest real headline without AI or invented text."""
    valid = []
    seen = set()
    for issue in raw_issues:
        c = html.unescape(str(issue)).strip()
        c = re.sub(r'<.*?>', '', c).strip()
        if not c or len(c) < 7 or c.startswith('http'):
            continue
        key = re.sub(r'\s+', ' ', c).lower()
        if key in seen:
            continue
        seen.add(key)
        valid.append(c)
    return valid[0] if valid else ""

def fetch_extra_fundamentals(symbol, asset_type, stock_name=""):
    """Collect only data that is actually returned by free/public endpoints."""
    raw_issues = []
    info = {
        "supply_status": "",
        "financial_status": "",
        "recent_news": ""
    }

    if asset_type == "overseas_stock":
        clean_sym = symbol.upper().strip()
        raw_issues.extend(get_foreign_rss_headlines(clean_sym))

        # Yahoo public quote endpoint; if unavailable, leave fundamentals blank.
        try:
            url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={urllib.parse.quote(clean_sym)}"
            res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=2.5).json()
            result = res.get("quoteResponse", {}).get("result", [])
            if result:
                q = result[0]
                parts = []
                pe = q.get("trailingPE")
                pb = q.get("priceToBook")
                if isinstance(pe, (int, float)):
                    parts.append(f"PER {pe:.2f}배")
                if isinstance(pb, (int, float)):
                    parts.append(f"PBR {pb:.2f}배")
                if parts:
                    info["financial_status"] = " / ".join(parts)
        except Exception:
            pass

    elif asset_type == "crypto":
        c_sym = symbol.replace("USDT", "-USD").replace("USD", "-USD")
        if not c_sym.endswith("-USD"):
            c_sym += "-USD"
        raw_issues.extend(get_foreign_rss_headlines(c_sym))

    else:
        headers = {"User-Agent": "Mozilla/5.0"}
        try:
            news_url = f"https://m.stock.naver.com/api/news/stock/{symbol}?pageSize=10&page=1"
            n_res = requests.get(news_url, headers=headers, timeout=2.5).json()
            news_items = []
            if isinstance(n_res, dict):
                news_items = n_res.get("items", []) or n_res.get("newsList", []) or n_res.get("result", [])
            elif isinstance(n_res, list):
                news_items = n_res
            for n in news_items:
                title_txt = n.get("title") or n.get("tit") or n.get("itemTitle") or ""
                clean_t = html.unescape(str(title_txt)).strip()
                clean_t = re.sub(r'<.*?>', '', clean_t).strip()
                if clean_t and len(clean_t) > 6 and clean_t not in raw_issues:
                    raw_issues.append(clean_t)
        except Exception:
            pass

        try:
            integ_url = f"https://m.stock.naver.com/api/stock/{symbol}/integration"
            res = requests.get(integ_url, headers=headers, timeout=2.5).json()
            total_infos = res.get("totalInfos", [])
            per_val, pbr_val, for_rate = "", "", ""
            for item in total_infos:
                k = item.get("code")
                v = item.get("value")
                if k == "foreignRate" and v:
                    for_rate = str(v)
                elif k == "per" and v:
                    per_val = str(v)
                elif k == "pbr" and v:
                    pbr_val = str(v)
            if for_rate:
                info["supply_status"] = f"외국인 지분율 {for_rate}%"
            parts = []
            if per_val:
                parts.append(f"PER {per_val}배")
            if pbr_val:
                parts.append(f"PBR {pbr_val}배")
            if parts:
                info["financial_status"] = " / ".join(parts)
        except Exception:
            pass

    info["recent_news"] = synthesize_issue_headline_local(stock_name, raw_issues)
    return info

def generate_deep_report_local(stock_name, symbol, price, an, extra_info, bt_info):
    """Deterministic report generated from observed data only; no Gemini call."""
    unit = "원" if "원" in str(price) else "달러"
    fmt = lambda v: f"{int(v):,}{unit}" if unit == "원" else f"{float(v):,.2f}{unit}"

    trend_sentence = an.get("ma_fact_desc", "")
    news = str(extra_info.get("recent_news", "")).strip()
    fund = str(extra_info.get("financial_status", "")).strip()
    supply = str(extra_info.get("supply_status", "")).strip()

    parts = [trend_sentence]
    parts.append(f"RSI {an['rsi']}, ATR {fmt(an['atr'])}, 최근 5일 평균 대비 거래량 {an['vol_ratio']}퍼센트입니다.")
    if news:
        parts.append(f"최근 확인 뉴스: {news}")
    if fund:
        parts.append(f"확인된 밸류에이션 지표: {fund}.")
    if supply:
        parts.append(f"확인된 수급 지표: {supply}.")
    parts.append(f"현재 국면은 {an['pattern_name']}이며, {an['guide_text']}.")
    if bt_info:
        parts.append(
            f"과거 동일 규칙을 적용한 백테스트는 {bt_info['total']}건 중 "
            f"승리 {bt_info['win']}건, 패배 {bt_info['loss']}건, 승률 {bt_info['win_rate']:.1f}%였습니다."
        )
    else:
        parts.append("현재 데이터로 백테스트를 계산할 표본이 충분하지 않습니다.")
    return "\n\n".join(p for p in parts if p)

def generate_reliable_report(stock_name, symbol, price, an, extra_info, bt_info=None):
    is_krw = "원" in str(price)
    unit = " 원" if is_krw else " 달러"
    fmt = lambda v: f"{int(v):,}{unit}" if is_krw else f"{v:,.2f}{unit}"

    news_raw = str(extra_info.get('recent_news', '')).strip()
    detail = generate_deep_report_local(stock_name, symbol, price, an, extra_info, bt_info)

    curr_p = an['ict_entry1_high']
    ret1 = ((an['ict_target1'] - curr_p) / curr_p) * 100 if curr_p else 0.0
    ret2 = ((an['ict_target2'] - curr_p) / curr_p) * 100 if curr_p else 0.0
    risk = ((curr_p - an['ict_stop']) / curr_p) * 100 if curr_p else 0.0

    issue_block = f"gaemiGTP 이슈\n👉 {news_raw}\n\n" if news_raw else ""
    combined_body = f"""{issue_block}현재가 {price}\n\n현재 트레이딩 적합도 {an['stars']} (점수 {an['score']})\n\n{detail}""".strip()

    signal_only = f"""스마트머니 퀀트 타점\n\n방향성: {an['direction']}\n\n1차 진입: {an['entry1_label']}\n👉 {fmt(an['ict_entry1_low'])} ~ {fmt(an['ict_entry1_high'])}\n\n2차 진입: {an['entry2_label']}\n👉 {fmt(an['ict_entry2_low'])} ~ {fmt(an['ict_entry2_high'])}\n\n1차 목표: {an['target1_label']}\n👉 {fmt(an['ict_target1'])} (+{ret1:.1f}%)\n\n2차 목표: {an['target2_label']}\n👉 {fmt(an['ict_target2'])} (+{ret2:.1f}%)\n\n칼손절: {an['stop_label']}\n👉 {fmt(an['ict_stop'])} (-{risk:.1f}%)\n\ngaemiGTP 가이드\n{an['guide_text']}""".strip()

    return combined_body, signal_only, news_raw

def fetch_technical_quant_data(symbol, asset_type, stock_name=""):
    quant_info = {}
    curr_price_num, curr_price_str, df = None, None, None
    if asset_type == "crypto":
        curr_price_num, curr_price_str, df = get_crypto_multi_exchange_data(symbol, limit=200)
    elif asset_type == "overseas_stock":
        curr_price_num, curr_price_str, df = get_us_stock_yahoo_data(symbol, years=1)
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
        quant_data['analysis'], quant_data['extra_info'], quant_data.get('bt_info')
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
    run_web_server()
