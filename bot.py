import os
import re
import io
import gc
import json
import uuid
import asyncio
import sqlite3
import datetime
import threading
import urllib.parse
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

# 인증 정보
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "8980819571:AAHyzm6KjZ0rKocObWoS28ThBbVGhJ5TfUs")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "AQ.Ab8RN6KisWMYVxgBBJmBW3SgOAT9aJYvOPiWj42H4WR_Y56kRA")
FMP_API_KEY = os.getenv("FMP_API_KEY", "YUVC3My39vzjHpkj0SwNyHp1hXn6Q1IN")

REPORT_CACHE = {}
DB_FILE = "trades.db"

def init_db():
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute('''
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                name TEXT,
                asset_type TEXT,
                direction TEXT,
                entry_price REAL,
                target_price REAL,
                stop_loss REAL,
                status TEXT DEFAULT 'PENDING',
                ret REAL DEFAULT 0.0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                closed_at TIMESTAMP
            )
        ''')
        conn.commit()
        conn.close()
    except Exception:
        pass

init_db()

CRYPTO_MAP = {
    "BTC": "BTCUSDT", "비트코인": "BTCUSDT", "비트": "BTCUSDT",
    "ETH": "ETHUSDT", "이더리움": "ETHUSDT", "이더": "ETHUSDT",
    "XRP": "XRPUSDT", "리플": "XRPUSDT",
    "DOGE": "DOGEUSDT", "도지코인": "DOGEUSDT", "도지": "DOGEUSDT",
    "SOL": "SOLUSDT", "솔라나": "SOLUSDT",
    "ADA": "ADAUSDT", "에이다": "ADAUSDT",
    "AVAX": "AVAXUSDT", "아발란체": "AVAXUSDT"
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
    "삼성중공업": "010140", "삼성전자": "005930", "삼성전기": "009150",
    "SK하이닉스": "000660", "SK스퀘어": "402340", "SK증권": "001510",
    "솔트룩스": "304100", "알테오젠": "196170", "에코프로": "086520",
    "에코프로비엠": "247540", "레인보우로보틱스": "277810", "카카오": "035720",
    "네이버": "035420", "NAVER": "035420", "현대차": "005380",
    "기아": "000270", "포스코홀딩스": "005490", "POSCO홀딩스": "005490",
    "LG에너지솔루션": "373220", "한화오션": "042660", "두산에너빌리티": "034020",
    "한화에어로스페이스": "012450", "현대모비스": "012330", "셀트리온": "068270"
}

def ask_gemini_api(prompt_text):
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}"
    headers = {"Content-Type": "application/json"}
    payload = {"contents": [{"parts": [{"text": prompt_text}]}]}
    try:
        res = requests.post(url, headers=headers, json=payload, timeout=8.0)
        data = res.json()
        return data["candidates"][0]["content"]["parts"][0]["text"]
    except Exception:
        return "타점 전략 분석 완료 SPLIT 지표 기반 안전 구간 진입 권장"

async def delete_messages_later(bot, chat_id, message_ids, delay=900):
    try:
        await asyncio.sleep(delay)
        for msg_id in message_ids:
            try:
                await bot.delete_message(chat_id=chat_id, message_id=msg_id)
            except Exception:
                pass
    except Exception:
        pass

def find_stock_code(query):
    clean_q = query.strip().replace(" ", "").upper()
    if clean_q.isdigit() and len(clean_q) == 6:
        return clean_q, clean_q

    for name, code in DOMESTIC_FAST_MAP.items():
        if name.replace(" ", "").upper() == clean_q:
            return code, name

    try:
        url = f"https://ac.finance.naver.com/ac?q={urllib.parse.quote(query.strip())}&target=stock"
        headers = {"User-Agent": "Mozilla/5.0"}
        res = requests.get(url, headers=headers, timeout=2.5).json()
        items_list = res.get("items", [])
        if items_list and isinstance(items_list[0], list) and len(items_list[0]) > 0:
            for item in items_list[0]:
                code = str(item[0]).strip()
                name = str(item[1]).strip()
                if name.replace(" ", "").upper() == clean_q:
                    return code, name
            return str(items_list[0][0][0]).strip(), str(items_list[0][0][1]).strip()
    except Exception:
        pass
    return None, query.strip()

def get_crypto_bybit_data(symbol="BTCUSDT"):
    clean_sym = symbol.replace("KRW-", "").replace("-", "").upper()
    if not clean_sym.endswith("USDT") and not clean_sym.endswith("USD"):
        clean_sym = f"{clean_sym}USDT"

    try:
        url = f"https://api.bybit.com/v5/market/kline?category=linear&symbol={clean_sym}&interval=D&limit=40"
        headers = {"User-Agent": "Mozilla/5.0"}
        res = requests.get(url, headers=headers, timeout=2.5).json()
        candle_list = res.get("result", {}).get("list", [])
        
        if candle_list:
            records = []
            for item in candle_list:
                records.append({
                    "Date": pd.to_datetime(int(item[0]), unit='ms'),
                    "Open": float(item[1]),
                    "High": float(item[2]),
                    "Low": float(item[3]),
                    "Close": float(item[4]),
                    "Volume": float(item[5])
                })
            df = pd.DataFrame(records).set_index("Date").sort_index()
            curr_price = float(df['Close'].iloc[-1])
            return curr_price, f"{curr_price:,.2f} 달러 (USDT)", df
    except Exception:
        pass
    return None, None, None

def get_us_stock_fmp_data(symbol):
    clean_sym = symbol.upper().strip()

    try:
        stooq_url = f"https://stooq.com/q/d/l/?s={clean_sym.lower()}.us&i=d"
        stooq_res = requests.get(stooq_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=2.5)
        if stooq_res.status_code == 200 and "Date,Open,High,Low,Close" in stooq_res.text:
            csv_data = io.StringIO(stooq_res.text)
            df = pd.read_csv(csv_data)
            if not df.empty and len(df) >= 5:
                df['Date'] = pd.to_datetime(df['Date'])
                df = df.set_index('Date').sort_index()
                df = df.tail(40)
                curr_p = float(df['Close'].iloc[-1])
                return curr_p, f"{curr_p:,.2f} 달러", df
    except Exception:
        pass

    try:
        y_url = f"https://query1.finance.yahoo.com/v8/finance/chart/{clean_sym}?range=1mo&interval=1d"
        headers = {"User-Agent": "Mozilla/5.0"}
        y_res = requests.get(y_url, headers=headers, timeout=2.5).json()
        result = y_res.get("chart", {}).get("result", [])
        if result:
            ts = result[0].get("timestamp", [])
            quote = result[0].get("indicators", {}).get("quote", [{}])[0]
            opens = quote.get("open", [])
            highs = quote.get("high", [])
            lows = quote.get("low", [])
            closes = quote.get("close", [])
            volumes = quote.get("volume", [])

            records = []
            for i in range(len(ts)):
                if opens[i] is not None and closes[i] is not None:
                    records.append({
                        "Date": pd.to_datetime(ts[i], unit='s'),
                        "Open": float(opens[i]),
                        "High": float(highs[i]),
                        "Low": float(lows[i]),
                        "Close": float(closes[i]),
                        "Volume": float(volumes[i]) if volumes[i] is not None else 0.0
                    })
            if records:
                df = pd.DataFrame(records).set_index("Date").sort_index()
                curr_p = float(df['Close'].iloc[-1])
                return curr_p, f"{curr_p:,.2f} 달러", df
    except Exception:
        pass

    return None, None, None

def get_naver_realtime_price(code):
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        url = f"https://m.stock.naver.com/api/stock/{code}/basic"
        res = requests.get(url, headers=headers, timeout=2.0).json()
        p = str(res.get("closePrice", "")).replace(",", "").strip()
        if p and p.isdigit():
            return float(p), f"{int(p):,} 원"
    except Exception:
        pass
    return None, None

def fetch_domestic_stock_candles(code):
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        url = f"https://api.stock.naver.com/chart/domestic/market/stock/{code}?periodType=dayCandle&range=40"
        res = requests.get(url, headers=headers, timeout=2.0).json()
        price_infos = res.get("priceInfos", [])
        if price_infos:
            records = []
            for item in price_infos:
                records.append({
                    "Date": pd.to_datetime(str(item["localDate"]), format="%Y%m%d"),
                    "Open": float(item["openPrice"]),
                    "High": float(item["highPrice"]),
                    "Low": float(item["lowPrice"]),
                    "Close": float(item["closePrice"]),
                    "Volume": float(item["accumulatedTradingVolume"])
                })
            if records:
                return pd.DataFrame(records).set_index("Date").sort_index()
    except Exception:
        pass
    return None

def generate_candlestick_chart(df, symbol_name):
    fig = None
    try:
        if df is None or len(df) < 5:
            return None

        chart_df = df.tail(30).copy()
        chart_df.index = pd.to_datetime(chart_df.index)
        
        mc = mpf.make_marketcolors(
            up='#ef5350', down='#26a69a',
            wick={'up':'#ef5350', 'down':'#26a69a'},
            edge={'up':'#ef5350', 'down':'#26a69a'},
            volume={'up':'#ef5350', 'down':'#26a69a'}
        )
        s = mpf.make_mpf_style(marketcolors=mc, gridstyle=':', gridcolor='#2b2b36', facecolor='#131722', figcolor='#131722')
        
        buf = io.BytesIO()
        fig, axlist = mpf.plot(
            chart_df,
            type='candle',
            mav=(20,),
            volume=True,
            style=s,
            figsize=(7, 3.8),
            returnfig=True,
            savefig=dict(fname=buf, format='png', bbox_inches='tight', facecolor='#131722', dpi=80)
        )
        buf.seek(0)
        return buf
    except Exception:
        return None
    finally:
        if fig is not None:
            plt.close(fig)
        plt.close('all')
        gc.collect()

def perform_ict_quant_analysis(raw_price, df):
    if df is None or len(df) < 15:
        atr = round(raw_price * 0.03, 2)
        return {
            "trend_status": "중립 레인지",
            "rsi": 50.0,
            "ma20_status": "20일선 수렴",
            "atr": atr,
            "ict_zone": "Equilibrium",
            "entry_p": raw_price,
            "target_p": round(raw_price + atr * 2.0, 2),
            "stop_p": round(raw_price - atr * 1.2, 2)
        }

    col_map = {c: c.capitalize() for c in df.columns}
    df_c = df.rename(columns=col_map)
    closes = df_c['Close']
    highs = df_c['High']
    lows = df_c['Low']
    curr_p = closes.iloc[-1]

    recent_high = highs.tail(20).max()
    recent_low = lows.tail(20).min()
    eq = (recent_high + recent_low) / 2.0
    ict_zone = "Discount Zone (할인 / 롱 유리)" if curr_p <= eq else "Premium Zone (할증 / 숏 유리)"

    ma20 = closes.rolling(20, min_periods=5).mean().iloc[-1]
    disparity = ((curr_p - ma20) / ma20) * 100
    ma20_status = f"20일선 대비 {disparity:+.1f}%"

    delta = closes.diff()
    gain = (delta.where(delta > 0, 0)).rolling(14, min_periods=5).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14, min_periods=5).mean()
    rs = gain / (loss + 1e-9)
    rsi_val = float((100 - (100 / (1 + rs))).iloc[-1])
    if np.isnan(rsi_val):
        rsi_val = 50.0

    atr_val = round(float((highs - lows).tail(14).mean()), 2)
    if atr_val <= 0:
        atr_val = round(curr_p * 0.03, 2)

    entry_p = curr_p
    target_p = round(curr_p + (atr_val * 2.0), 2)
    stop_p = round(curr_p - (atr_val * 1.2), 2)

    return {
        "trend_status": "상승 모멘텀" if curr_p >= ma20 else "조정 국면",
        "rsi": round(rsi_val, 1),
        "ma20_status": ma20_status,
        "atr": atr_val,
        "ict_zone": ict_zone,
        "entry_p": entry_p,
        "target_p": target_p,
        "stop_p": stop_p
    }

def fetch_technical_quant_data(symbol, asset_type):
    quant_info = {}
    curr_price_num = None
    curr_price_str = None
    df = None

    try:
        if asset_type == "crypto":
            curr_price_num, curr_price_str, df = get_crypto_bybit_data(symbol)
        elif asset_type == "overseas_stock":
            curr_price_num, curr_price_str, df = get_us_stock_fmp_data(symbol)
        elif asset_type == "domestic_stock":
            curr_price_num, curr_price_str = get_naver_realtime_price(symbol)
            df = fetch_domestic_stock_candles(symbol)
            if curr_price_num is None and df is not None and not df.empty:
                curr_price_num = float(df['Close'].iloc[-1])
                curr_price_str = f"{int(curr_price_num):,} 원"

        if curr_price_num is None:
            return None

        quant_info['raw_price'] = curr_price_num
        quant_info['current_price'] = curr_price_str
        quant_info['analysis'] = perform_ict_quant_analysis(curr_price_num, df)
        quant_info['chart_df'] = df

        return quant_info
    except Exception:
        return None

def record_trade_signal(symbol, name, asset_type, an):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute('''
            INSERT INTO trades (symbol, name, asset_type, direction, entry_price, target_price, stop_loss, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'PENDING')
        ''', (symbol, name, asset_type, 'LONG', an.get('entry_p', 0), an.get('target_p', 0), an.get('stop_p', 0)))
        conn.commit()
        conn.close()
    except Exception:
        pass

def update_and_calculate_performance():
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT id, symbol, asset_type, entry_price, target_price, stop_loss FROM trades WHERE status = 'PENDING'")
        pending_rows = c.fetchall()

        for row in pending_rows:
            tid, sym, atype, entry_p, target_p, stop_p = row
            curr_p = None

            if atype == "crypto":
                curr_p, _, _ = get_crypto_bybit_data(sym)
            elif atype == "overseas_stock":
                curr_p, _, _ = get_us_stock_fmp_data(sym)
            elif atype == "domestic_stock":
                curr_p, _ = get_naver_realtime_price(sym)

            if curr_p is not None and entry_p > 0:
                if curr_p >= target_p:
                    ret = ((target_p - entry_p) / entry_p) * 100
                    c.execute("UPDATE trades SET status = 'WIN', ret = ?, closed_at = CURRENT_TIMESTAMP WHERE id = ?", (round(ret, 2), tid))
                elif curr_p <= stop_p:
                    ret = ((stop_p - entry_p) / entry_p) * 100
                    c.execute("UPDATE trades SET status = 'LOSS', ret = ?, closed_at = CURRENT_TIMESTAMP WHERE id = ?", (round(ret, 2), tid))

        conn.commit()

        c.execute("SELECT COUNT(*) FROM trades")
        total_trades = c.fetchone()[0]

        c.execute("SELECT COUNT(*), SUM(ret) FROM trades WHERE status = 'WIN'")
        win_row = c.fetchone()
        win_count = win_row[0] or 0
        win_return = win_row[1] or 0.0

        c.execute("SELECT COUNT(*), SUM(ret) FROM trades WHERE status = 'LOSS'")
        loss_row = c.fetchone()
        loss_count = loss_row[0] or 0
        loss_return = loss_row[1] or 0.0

        c.execute("SELECT COUNT(*) FROM trades WHERE status = 'PENDING'")
        pending_count = c.fetchone()[0]

        c.execute("SELECT name, entry_price, target_price, status, ret, created_at FROM trades ORDER BY id DESC LIMIT 5")
        recent_history = c.fetchall()
        conn.close()

        finished = win_count + loss_count
        win_rate = (win_count / finished * 100) if finished > 0 else 0.0
        cum_ret = win_return + loss_return

        return {
            "total": total_trades,
            "finished": finished,
            "win": win_count,
            "loss": loss_count,
            "pending": pending_count,
            "win_rate": win_rate,
            "cum_ret": cum_ret,
            "history": recent_history
        }
    except Exception:
        return {"total": 0, "finished": 0, "win": 0, "loss": 0, "pending": 0, "win_rate": 0.0, "cum_ret": 0.0, "history": []}

def generate_full_quant_report(stock_name, symbol, price, quant_data):
    an = quant_data.get('analysis', {}) if quant_data else {}
    tech_context = f"""
    [ICT & 퀀트 정밀 연산치]
    - 현재가: {price}
    - ICT 마켓 프레임: {an.get('ict_zone', 'Equilibrium')}
    - 진입 기준가: {an.get('entry_p', 0)}
    - 1차 BSL 목표가: {an.get('target_p', 0)}
    - SSL 칼손절가: {an.get('stop_p', 0)}
    - 이평선 구조: {an.get('ma20_status', '산출중')}
    - RSI 강도: {an.get('rsi', 50.0)}
    - ATR 변동폭: {an.get('atr', '산출중')}
    """

    prompt = f"""
    당신은 ICT(Inner Circle Trader) 전문 퀀트 트레이더입니다.
    제공된 ICT 타점 수치를 바탕으로 명확한 타점 전략 카드를 작성하세요.

    대상 종목: {stock_name} ({symbol})
    {tech_context}

    [★ 필수 출력 포맷 (특수기호, 하이픈, 번호매기기 없이 작성) ★]
    
    {stock_name} ({symbol})
    현재가 {price}
    ICT 마켓 프레임 {an.get('ict_zone', 'Equilibrium')}

    ICT Alpha Action Plan

    방향성 롱 (Bullish Order Block 되돌림)
    1차 진입 {an.get('entry_p', 0)} (FVG 지지 확인 진입권)
    2차 진입 {round(float(an.get('entry_p', 0)) * 0.985, 2)} (오더블록 하단 2차 분할)
    1차 목표 {an.get('target_p', 0)} (단기 Buy-side Liquidity 1차 분할 익절)
    2차 목표 {round(float(an.get('target_p', 0)) * 1.03, 2)} (상단 주요 유동성 풀 최종 익절)
    칼손절 {an.get('stop_p', 0)} (Sell-side Liquidity 이탈 무효화)

    트리거 조건 및 행동 요령
    킬존 시간대 체결 강도 및 분할 매수 대응 전략 서술

    SPLIT

    ICT 시각에서 분석한 기관 세력의 오더블록(OB)과 FVG 구조

    Buy-side 및 Sell-side 유동성 풀 배치 상태

    예상 손익비 분석 및 실전 트레이딩 코멘트
    """
    response_text = ask_gemini_api(prompt)
    parts = response_text.split("SPLIT")
    summary = parts[0].strip()
    detail = parts[1].strip() if len(parts) > 1 else "심층 분석 데이터를 불러올 수 없습니다"
    return summary, detail

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Alpha Signal ICT 퀀트 봇 가동 중입니다\n\n"
        "• 종목 분석: 종목명 입력 (예: 엔비디아, 테슬라, 비트코인)\n"
        "• 실전 성적표 조회: '성적표' 또는 '승률' 입력\n"
        "(분석 메시지는 15분 후 자동 정리됩니다)"
    )

async def show_performance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    status_msg = await update.message.reply_text("📊 실시간 시세를 대조하여 타점 승률을 계산 중입니다...")
    stats = update_and_calculate_performance()

    hist_lines = ""
    for item in stats['history']:
        name, entry_p, target_p, st, ret, dt = item
        st_icon = "🟢 익절" if st == "WIN" else ("🔴 손절" if st == "LOSS" else "⏳ 추적중")
        ret_str = f"({ret:+.2f}%)" if st in ["WIN", "LOSS"] else ""
        hist_lines += f"• {name} ➜ {st_icon} {ret_str}\n"

    if not hist_lines:
        hist_lines = "아직 기록된 타점이 없습니다 종목을 먼저 검색해 보세요\n"

    report = f"""
🏆 Alpha Signal 실전 타점 누적 성적표

• 총 타점 발행 {stats['total']}건
• 완료된 매매 {stats['finished']}건 (현재 진행중 {stats['pending']}건)
• 익절(승리) {stats['win']}건 / 손절(패배) {stats['loss']}건
• 실전 승률 {stats['win_rate']:.1f}%
• 누적 확정 수익률 {stats['cum_ret']:+.2f}%

최근 타점 진행 현황
{hist_lines}
성적표는 실시간 시세가 변동될 때마다 자동 갱신됩니다
"""
    await status_msg.edit_text(report.strip())

async def analyze(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.message.text.strip()
    chat_id = update.effective_chat.id

    # 성적표/승률 키워드 감지
    if query in ["성적표", "승률", "수익률", "검증", "/score", "/report"]:
        await show_performance(update, context)
        return

    if query in ["대장", "대장주", "종목추천", "추천"]:
        query = "삼성전기"

    status_msg = await update.message.reply_text(f"{query} ICT 스마트 머니 분석 중입니다...")
    new_msg_ids = []

    try:
        clean_q = query.replace(" ", "").upper()
        
        # 1. 암호화폐
        if clean_q in CRYPTO_MAP or any(c in clean_q for c in ["BTC", "ETH", "USDT", "코인", "비트코인", "이더리움", "리플", "도지"]):
            asset_type = "crypto"
            symbol = CRYPTO_MAP.get(clean_q, clean_q.replace("코인", "") + "USDT")
            formal_name = query
        # 2. 미국 주식
        elif clean_q in US_STOCK_MAP:
            asset_type = "overseas_stock"
            symbol = US_STOCK_MAP[clean_q]
            formal_name = f"{query} ({symbol})"
        elif any(c.isupper() for c in query) and not any('\uac00' <= char <= '\ud7a3' for char in query):
            asset_type = "overseas_stock"
            symbol = query.upper()
            formal_name = query
        # 3. 국내 주식
        else:
            code, name = find_stock_code(query)
            if not code:
                await status_msg.edit_text(f"❌ '{query}' 종목을 찾지 못했습니다. 종목명을 확인해 주세요.")
                return
            symbol = code
            formal_name = name
            asset_type = "domestic_stock"

        quant_data = fetch_technical_quant_data(symbol, asset_type)
        if not quant_data or not quant_data.get('current_price'):
            await status_msg.edit_text(f"❌ {formal_name}의 실시간 시세를 가져오지 못했습니다. 잠시 후 다시 시도해 주세요.")
            return

        current_price = quant_data['current_price']
        summary_text, detail_text = generate_full_quant_report(formal_name, symbol, current_price, quant_data)

        record_trade_signal(symbol, formal_name, asset_type, quant_data['analysis'])

        report_id = str(uuid.uuid4())[:8]
        if len(REPORT_CACHE) > 30:
            REPORT_CACHE.clear()
        REPORT_CACHE[report_id] = {"summary": summary_text, "detail": detail_text}

        keyboard = [[InlineKeyboardButton("ICT 심층 분석 및 근거 보기", callback_data=f"show_detail:{report_id}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        chart_buf = generate_candlestick_chart(quant_data.get('chart_df'), formal_name)
        
        try:
            await status_msg.delete()
        except Exception:
            pass

        if chart_buf is not None:
            try:
                sent_photo = await context.bot.send_photo(chat_id=chat_id, photo=chart_buf.getvalue())
                new_msg_ids.append(sent_photo.message_id)
            except Exception:
                pass

        sent_msg = await context.bot.send_message(chat_id=chat_id, text=summary_text, reply_markup=reply_markup)
        new_msg_ids.append(sent_msg.message_id)

        if new_msg_ids:
            asyncio.create_task(delete_messages_later(context.bot, chat_id, new_msg_ids, delay=900))

    except Exception:
        try:
            await status_msg.edit_text("일시적 처리 지연이 발생했습니다. 다시 입력해 주세요.")
        except Exception:
            pass
    finally:
        gc.collect()

async def handle_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    action, report_id = query.data.split(":")
    data = REPORT_CACHE.get(report_id)

    if not data:
        await query.message.reply_text("만료된 세션입니다 종목명을 다시 입력해 주세요")
        return

    if action == "show_detail":
        full_text = f"{data['summary']}\n\n{data['detail']}"
        keyboard = [[InlineKeyboardButton("타점 카드만 보기", callback_data=f"show_summary:{report_id}")]]
        await query.message.edit_text(text=full_text, reply_markup=InlineKeyboardMarkup(keyboard))
    elif action == "show_summary":
        keyboard = [[InlineKeyboardButton("ICT 심층 분석 및 근거 보기", callback_data=f"show_detail:{report_id}")]]
        await query.message.edit_text(text=data['summary'], reply_markup=InlineKeyboardMarkup(keyboard))

class HealthServer(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")
    def log_message(self, format, *args):
        return

def run_health_server():
    try:
        port = int(os.environ.get("PORT", 10000))
        server = HTTPServer(("0.0.0.0", port), HealthServer)
        server.serve_forever()
    except Exception:
        pass

if __name__ == "__main__":
    t = threading.Thread(target=run_health_server, daemon=True)
    t.start()

    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("score", show_performance))
    app.add_handler(CommandHandler("report", show_performance))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, analyze))
    app.add_handler(CallbackQueryHandler(handle_button))
    app.run_polling(drop_pending_updates=True)
