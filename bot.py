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
import traceback
from http.server import HTTPServer, BaseHTTPRequestHandler
import google.generativeai as genai
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

# 1. 인증 정보
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "8980819571:AAHyzm6KjZ0rKocObWoS28ThBbVGhJ5TfUs")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "AQ.Ab8RN6KisWMYVxgBBJmBW3SgOAT9aJYvOPiWj42H4WR_Y56kRA")
FMP_API_KEY = os.getenv("FMP_API_KEY", "YUVC3My39vzjHpkj0SwNyHp1hXn6Q1IN")

try:
    genai.configure(api_key=GEMINI_API_KEY)
    ai_model = genai.GenerativeModel('gemini-1.5-flash')
except Exception:
    ai_model = None

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
    "SK하이닉스": "000660", "하이닉스": "000660",
    "삼성전자": "005930", "삼성전기": "009150", "삼성중공업": "010140",
    "SK증권": "001510", "SK스퀘어": "402340", "솔트룩스": "304100",
    "알테오젠": "196170", "에코프로": "086520", "에코프로비엠": "247540",
    "레인보우로보틱스": "277810", "카카오": "035720", "네이버": "035420",
    "NAVER": "035420", "현대차": "005380", "기아": "000270",
    "포스코홀딩스": "005490", "POSCO홀딩스": "005490", "LG에너지솔루션": "373220",
    "한화오션": "042660", "두산에너빌리티": "034020", "한화에어로스페이스": "012450",
    "현대모비스": "012330", "셀트리온": "068270"
}

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

    try:
        url = f"https://financialmodelingprep.com/api/v3/quote/{clean_sym}?apikey={FMP_API_KEY}"
        res = requests.get(url, timeout=2.5).json()
        if isinstance(res, list) and len(res) > 0:
            curr_p = float(res[0].get("price", 0))
            if curr_p > 0:
                return curr_p, f"{curr_p:,.2f} 달러", None
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
            "ma20": raw_price,
            "ma20_status": "20일선 수렴",
            "vol_status": "평균 수준",
            "atr": atr,
            "ict_zone": "Discount Zone (롱 OTE 유리)",
            "raw_price": raw_price
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
    ict_zone = "Discount Zone (할인 / 롱 OTE 유리)" if curr_p <= eq else "Premium Zone (할증 / 숏 유리)"

    ma20 = closes.rolling(20, min_periods=5).mean().iloc[-1]
    ma60 = closes.rolling(60, min_periods=10).mean().iloc[-1] if len(df_c) >= 10 else ma20
    disparity = ((curr_p - ma20) / ma20) * 100
    ma20_status = f"20일선({ma20:,.1f}) 대비 {disparity:+.1f}% / 60일선({ma60:,.1f})"

    delta = closes.diff()
    gain = (delta.where(delta > 0, 0)).rolling(14, min_periods=5).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14, min_periods=5).mean()
    rs = gain / (loss + 1e-9)
    rsi_val = float((100 - (100 / (1 + rs))).iloc[-1])
    if np.isnan(rsi_val):
        rsi_val = 50.0

    v_col = 'Volume' if 'Volume' in df_c.columns else 'Vol'
    curr_v = df_c[v_col].iloc[-1]
    avg_v5 = df_c[v_col].iloc[-6:-1].mean() if len(df_c) >= 6 else curr_v
    vol_ratio = (curr_v / (avg_v5 + 1e-9)) * 100
    vol_status = f"5일 평균 대비 {vol_ratio:.0f}% 수준"

    atr_val = round(float((highs - lows).tail(14).mean()), 2)
    if atr_val <= 0:
        atr_val = round(curr_p * 0.03, 2)

    return {
        "trend_status": "상승 모멘텀 가속" if curr_p >= ma20 else "단기 매물 소화 조정",
        "rsi": round(rsi_val, 1),
        "ma20": round(ma20, 2),
        "ma20_status": ma20_status,
        "vol_status": vol_status,
        "atr": atr_val,
        "ict_zone": ict_zone,
        "raw_price": curr_p
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

def generate_full_quant_report(stock_name, symbol, price, quant_data):
    an = quant_data.get('analysis', {}) if quant_data else {}
    tech_context = f"""
    [다중 팩터 정밀 기술적 연산 데이터]
    - 현재가: {price}
    - ICT 존: {an.get('ict_zone', 'Discount Zone')}
    - 이평선 구조: {an.get('ma20_status', '산출중')}
    - RSI 강도: {an.get('rsi', 50.0)}
    - 거래량 흐름: {an.get('vol_status', '보통')}
    - 일일 평균 변동폭(ATR): {an.get('atr', '산출중')}
    """

    prompt = f"""
    당신은 대한민국 최고의 실전 퀀트 및 ICT 스마트머니 트레이더입니다.
    제공된 정밀 기술적 데이터와 일봉 차트를 기반으로 풍부하고 객관적인 실전 타점 카드와 심층 해설을 작성하세요.

    종목: {stock_name} ({symbol})
    {tech_context}

    [★ 작성 지침 ★]
    1. 콜론(:), 하이픈(-), 번호매기기, 마크다운 볼드(**) 없이 정갈하게 작성하세요.
    2. 영문 대문자 SPLIT 으로 1부 타점 카드와 2부 심층 해설을 반드시 나누세요.
    3. 트레이딩 적합도는 점수(예: 4.2)와 별점(★★★★☆)으로 명시하세요.
    4. 1차 및 2차 진입가는 과거 매물대와 ATR을 고려해 현실적인 '구간(범위)'으로 제시하세요.
    5. 2부 심층 분석은 (1)과거 시계열 및 캔들 매물대 흐름, (2)매크로 및 업종 수급 집중도, (3)손익비 및 트레이딩 총평을 매우 풍부하고 전문적으로 작성하세요.

    [★ 출력 형식 예시 ★]
    {stock_name} ({symbol})

    현재가 {price}

    현재 트레이딩 적합도 ★★★★☆ (점수 4.2)

    Alpha Signal 액션 플랜

    방향성 롱 (단기 눌림목 매수)
    1차 진입 OOO원에서 OOO원 (단기 매물 소화 및 지지 확인 구간 1차 분할)
    2차 진입 OOO원에서 OOO원 (주요 구조적 지지대 및 하단 반등 마지노선 2차 분할)
    1차 목표 OOO원 (직전 단기 매물벽 도달 시 50퍼센트 분할 익절)
    2차 목표 OOO원 (상단 저항 밴드 돌파 시 최종 전량 익절)
    칼손절 OOO원 (지지선 붕괴 및 시나리오 무효화 기준가)

    트리거 조건 및 행동 요령
    당일 체결강도 및 호가창 수급 비대칭성을 확인하며 진입하는 행동 요령 서술

    SPLIT

    최근 주가 흐름과 일봉 캔들 매물대 소화 과정 및 과거 시계열 지표(20일선, RSI, 거래량) 정밀 진단

    미국 연준 금리 등 매크로 환경 및 해당 섹터로의 외국인/기관 수급 유입 모멘텀 심층 분석

    예상 진입가 대비 목표가 도달 시 수익률과 손절 시 리스크를 비교한 손익비 분석 및 최종 매매 코멘트
    """
    response = ai_model.generate_content(prompt).text
    if "SPLIT" in response:
        parts = response.split("SPLIT")
        summary = parts[0].strip()
        detail = parts[1].strip()
    else:
        summary = response.strip()
        detail = "심층 분석 데이터를 불러오는 중입니다"
    return summary, detail

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Alpha Signal ICT 퀀트 봇 가동 중입니다\n\n"
        "• 종목 분석: 종목명 입력 (예: SK증권, SK하이닉스, 엔비디아, 비트코인)\n"
        "(분석 메시지는 15분 후 자동 정리됩니다)"
    )

async def analyze(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.message.text.strip()
    chat_id = update.effective_chat.id

    if query in ["대장", "대장주", "종목추천", "추천", "/대장"]:
        query = "삼성전기"

    status_msg = await update.message.reply_text(f"{query} 다중 팩터 종합 분석 중입니다...")
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

        report_id = str(uuid.uuid4())[:8]
        if len(REPORT_CACHE) > 30:
            REPORT_CACHE.clear()
        REPORT_CACHE[report_id] = {"summary": summary_text, "detail": detail_text}

        keyboard = [[InlineKeyboardButton("심층 분석 및 근거 보기", callback_data=f"show_detail:{report_id}")]]
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

    except Exception as e:
        # 실제 발생한 에러의 구체적인 스택트레이스를 채팅창으로 즉시 확인
        err_msg = traceback.format_exc()
        try:
            await status_msg.edit_text(f"⚠️ 시스템 디버그 에러:\n{err_msg[:400]}")
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
        keyboard = [[InlineKeyboardButton("심층 분석 및 근거 보기", callback_data=f"show_detail:{report_id}")]]
        await query.message.edit_text(text=data['summary'], reply_markup=InlineKeyboardMarkup(keyboard))

class SimpleServer(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Bot is Running Alive!")

def run_web_server():
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), SimpleServer)
    server.serve_forever()

if __name__ == "__main__":
    threading.Thread(target=run_web_server, daemon=True).start()
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, analyze))
    app.add_handler(CallbackQueryHandler(handle_button))
    
    app.run_polling(drop_pending_updates=True)
