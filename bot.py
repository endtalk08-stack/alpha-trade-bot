import os
import re
import io
import json
import uuid
import datetime
import threading
import urllib.parse
from http.server import HTTPServer, BaseHTTPRequestHandler
import google.generativeai as genai
import yfinance as yf
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

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "8980819571:AAHyzm6KjZ0rKocObWoS28ThBbVGhJ5TfUs")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "AQ.Ab8RN6KisWMYVxgBBJmBW3SgOAT9aJYvOPiWj42H4WR_Y56kRA")

genai.configure(api_key=GEMINI_API_KEY)
ai_model = genai.GenerativeModel('gemini-3.6-flash')

REPORT_CACHE = {}

COMMON_SYMBOLS = {
    "BTC": ("crypto", "BTCUSDT"),
    "ETH": ("crypto", "ETHUSDT"),
    "XRP": ("crypto", "XRPUSDT"),
    "DOGE": ("crypto", "DOGEUSDT"),
    "SOL": ("crypto", "SOLUSDT"),
    "비트코인": ("crypto", "BTCUSDT"),
    "이더리움": ("crypto", "ETHUSDT"),
    "리플": ("crypto", "XRPUSDT"),
    "도지코인": ("crypto", "DOGEUSDT"),
    "솔라나": ("crypto", "SOLUSDT"),
    "삼성전자": ("domestic_stock", "005930"),
    "삼성전기": ("domestic_stock", "009150"),
    "SK하이닉스": ("domestic_stock", "000660"),
    "SK스퀘어": ("domestic_stock", "402340"),
    "SK증권": ("domestic_stock", "001510"),
    "솔트룩스": ("domestic_stock", "304100"),
    "알테오젠": ("domestic_stock", "196170"),
    "에코프로": ("domestic_stock", "086520"),
    "에코프로비엠": ("domestic_stock", "247540"),
    "레인보우로보틱스": ("domestic_stock", "277810"),
    "카카오": ("domestic_stock", "035720"),
    "네이버": ("domestic_stock", "035420"),
    "NAVER": ("domestic_stock", "035420"),
    "현대차": ("domestic_stock", "005380"),
    "기아": ("domestic_stock", "000270"),
    "포스코홀딩스": ("domestic_stock", "005490"),
    "POSCO홀딩스": ("domestic_stock", "005490"),
    "LG에너지솔루션": ("domestic_stock", "373220"),
    "테슬라": ("overseas_stock", "TSLA"),
    "TSLA": ("overseas_stock", "TSLA"),
    "엔비디아": ("overseas_stock", "NVDA"),
    "NVDA": ("overseas_stock", "NVDA"),
    "애플": ("overseas_stock", "AAPL"),
    "AAPL": ("overseas_stock", "AAPL"),
    "마이크로소프트": ("overseas_stock", "MSFT"),
    "MSFT": ("overseas_stock", "MSFT"),
    "QQQ": ("overseas_stock", "QQQ")
}

def get_crypto_bybit_data(symbol="BTCUSDT"):
    clean_sym = symbol.replace("KRW-", "").replace("-", "").upper()
    if not clean_sym.endswith("USDT") and not clean_sym.endswith("USD"):
        clean_sym = f"{clean_sym}USDT"

    try:
        url = f"https://api.bybit.com/v5/market/kline?category=linear&symbol={clean_sym}&interval=D&limit=60"
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
            return f"{curr_price:,.2f} 달러 (USDT)", df
    except Exception:
        pass
    return None, None

def get_stock_code_by_name(query):
    clean_q = query.strip()
    target_comp = clean_q.replace(" ", "").upper()

    for k, v in COMMON_SYMBOLS.items():
        if k.replace(" ", "").upper() == target_comp:
            return v[1], k

    try:
        d_url = f"https://suggest-baro.daum.net/suggest?id=stock&v=4&code=utf-8&q={requests.utils.quote(clean_q)}"
        headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://finance.daum.net"}
        res = requests.get(d_url, headers=headers, timeout=2).json()
        sub_items = res.get("sub", [])
        if sub_items:
            for item in sub_items:
                raw_code = str(item[0]).replace("A", "").strip()
                name = str(item[1]).strip()
                if name.replace(" ", "").upper() == target_comp:
                    return raw_code, name
            return str(sub_items[0][0]).replace("A", "").strip(), str(sub_items[0][1]).strip()
    except Exception:
        pass

    try:
        url = f"https://m.stock.naver.com/api/search/autoComplete?keyword={requests.utils.quote(clean_q)}"
        headers = {"User-Agent": "Mozilla/5.0"}
        res = requests.get(url, headers=headers, timeout=2).json()
        for key in ["stocks", "items"]:
            if key in res and isinstance(res[key], list):
                for item in res[key]:
                    c = item.get("itemCode") or item.get("code")
                    n = item.get("stockName") or item.get("name")
                    if c and n:
                        if str(n).replace(" ", "").upper() == target_comp:
                            return str(c).strip(), str(n).strip()
                if res[key]:
                    first = res[key][0]
                    c = first.get("itemCode") or first.get("code")
                    n = first.get("stockName") or first.get("name")
                    if c and n:
                        return str(c).strip(), str(n).strip()
    except Exception:
        pass

    return None, clean_q

def get_naver_realtime_price(code):
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        url = f"https://m.stock.naver.com/api/stock/{code}/basic"
        res = requests.get(url, headers=headers, timeout=2).json()
        p = str(res.get("closePrice", "")).replace(",", "").strip()
        if p and p.isdigit():
            return f"{int(p):,} 원"
    except Exception:
        pass
    return None

def fetch_domestic_stock_candles(code):
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        url = f"https://api.stock.naver.com/chart/domestic/market/stock/{code}?periodType=dayCandle&range=60"
        res = requests.get(url, headers=headers, timeout=2.5).json()
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
    try:
        if df is None or len(df) < 5:
            return None

        chart_df = df.tail(40).copy()
        chart_df.index = pd.to_datetime(chart_df.index)
        
        mc = mpf.make_marketcolors(
            up='#ef5350', down='#26a69a',
            wick={'up':'#ef5350', 'down':'#26a69a'},
            edge={'up':'#ef5350', 'down':'#26a69a'},
            volume={'up':'#ef5350', 'down':'#26a69a'}
        )
        s = mpf.make_mpf_style(marketcolors=mc, gridstyle=':', gridcolor='#2b2b36', facecolor='#131722', figcolor='#131722')
        
        buf = io.BytesIO()
        mpf.plot(
            chart_df,
            type='candle',
            mav=(20, 60),
            volume=True,
            style=s,
            figsize=(8, 4.5),
            savefig=dict(fname=buf, format='png', bbox_inches='tight', facecolor='#131722')
        )
        buf.seek(0)
        return buf
    except Exception:
        return None

def detect_chart_pattern(df):
    if df is None or len(df) < 25:
        return "정상 추세 탐색 구간"
    
    closes = df['Close'].values
    highs = df['High'].values
    lows = df['Low'].values
    
    recent_high = np.max(highs[-10:])
    prev_high = np.max(highs[-30:-10])
    curr_close = closes[-1]
    
    if abs(recent_high - prev_high) / prev_high < 0.035:
        if curr_close < recent_high * 0.96:
            return "일봉상 쌍봉(더블탑) 저항 패턴 감지 (상단 매물벽 돌파 실패로 하방 압력 가중)"
            
    recent_low = np.min(lows[-10:])
    prev_low = np.min(lows[-30:-10])
    
    if abs(recent_low - prev_low) / prev_low < 0.035:
        if curr_close > recent_low * 1.025:
            return "일봉상 쌍바닥(더블바텀 W자) 지지 패턴 감지 (강력한 저가 매수세 유입 및 추세 반전 성공)"
            
    return "이동평균선 수렴 및 방향성 탐색 구간"

def fetch_technical_quant_data(symbol, asset_type):
    quant_info = {}
    curr_price_str = None
    df = None

    try:
        if asset_type == "crypto":
            curr_price_str, df = get_crypto_bybit_data(symbol)
        elif asset_type == "domestic_stock":
            curr_price_str = get_naver_realtime_price(symbol)
            df = fetch_domestic_stock_candles(symbol)
        else:
            stock = yf.Ticker(symbol)
            df = stock.history(period="60d")
            if not df.empty:
                curr_p = float(df['Close'].iloc[-1])
                curr_price_str = f"{curr_p:,.2f} 달러"

        if not curr_price_str:
            return None

        quant_info['current_price'] = curr_price_str
        quant_info['pattern'] = detect_chart_pattern(df)

        if df is not None and len(df) >= 10:
            col_map = {c: c.lower() for c in df.columns}
            df_calc = df.rename(columns=col_map)
            quant_info['chart_df'] = df
            quant_info['rsi'] = 50.0
            quant_info['ma20'] = round(float(df_calc['close'].rolling(20).mean().iloc[-1]), 2) if len(df_calc) >= 20 else "산출중"
        else:
            quant_info['chart_df'] = None
            quant_info['rsi'] = 50.0
            quant_info['ma20'] = "산출중"

        return quant_info
    except Exception:
        return None

def generate_full_quant_report(stock_name, symbol, price, quant_data):
    pattern_info = quant_data.get('pattern', '정상 추세') if quant_data else '정상 추세'
    prompt = f"""
    당신은 실전 퀀트 트레이더입니다.
    종목: {stock_name} ({symbol})
    현재가: {price}
    차트 패턴 진단: {pattern_info}

    [작성 규칙]
    - 쌍봉 감지 시 적합도 2.0점 이하 부여 및 숏/관망 권고.
    - 쌍바닥 감지 시 적합도 4.7점 이상 부여 및 W자 롱 매수 타점 제시.
    - 콜론, 특수기호 없이 작성하고 SPLIT으로 1부 타점 카드와 2부 해설을 나누세요.

    {stock_name} ({symbol})
    
    현재가 {price}

    현재 트레이딩 적합도 ★★★★☆ (점수 4.5)

    Alpha Signal 액션 플랜

    방향성 롱 또는 숏
    1차 진입 1차진입가구간 (지지 확인 1차 분할)
    2차 진입 2차진입가구간 (마지노선 2차 분할)
    1차 목표 1차목표가 (50퍼센트 분할 익절)
    2차 목표 2차목표가 (전량 최종 익절)
    칼손절 칼손절가 (시나리오 이탈 기준가)

    트리거 조건 및 행동 요령
    장중 눌림목 체결 강도 대응 요령 서술

    SPLIT

    일봉상 차트 패턴과 핵심 수급 흐름 심층 분석 서술
    """
    response = ai_model.generate_content(prompt).text
    parts = response.split("SPLIT")
    summary = parts[0].strip()
    detail = parts[1].strip() if len(parts) > 1 else "심층 분석 데이터를 불러올 수 없습니다"
    return summary, detail

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Alpha Signal Quant Bot 가동 중입니다\n\n"
        "종목명을 입력하시면 실시간 차트와 퀀트 액션 플랜을 전송합니다\n"
        "예시 삼성전기 sk하이닉스 BTC 알테오젠 대장"
    )

async def analyze(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.message.text.strip()
    chat_id = update.effective_chat.id

    if query in ["대장", "대장주", "종목추천", "추천", "/대장"]:
        query = "삼성전기"

    status_msg = await update.message.reply_text(f"{query} 실시간 데이터 분석 중입니다")

    try:
        # 종목코드 식별
        if any(c in query.upper() for c in ["BTC", "ETH", "코인", "비트"]):
            asset_type, symbol = "crypto", query.upper()
            formal_name = query
        elif any(c.isupper() for c in query) and not any('\uac00' <= char <= '\ud7a3' for char in query):
            asset_type, symbol = "overseas_stock", query.upper()
            formal_name = query
        else:
            code, name = get_stock_code_by_name(query)
            symbol = code if code else query
            formal_name = name if code else query
            asset_type = "domestic_stock"

        quant_data = fetch_technical_quant_data(symbol, asset_type)
        if not quant_data or not quant_data.get('current_price'):
            await status_msg.edit_text(f"❌ {query}의 시세를 조회하지 못했습니다. 종목명을 다시 확인해 주세요.")
            return

        current_price = quant_data['current_price']
        summary_text, detail_text = generate_full_quant_report(formal_name, symbol, current_price, quant_data)

        report_id = str(uuid.uuid4())[:8]
        REPORT_CACHE[report_id] = {"summary": summary_text, "detail": detail_text}

        keyboard = [[InlineKeyboardButton("심층 분석 및 근거 보기", callback_data=f"show_detail:{report_id}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        # 차트 이미지
        chart_buf = generate_candlestick_chart(quant_data.get('chart_df'), formal_name)
        
        try:
            await status_msg.delete()
        except Exception:
            pass

        if chart_buf is not None:
            try:
                await context.bot.send_photo(chat_id=chat_id, photo=chart_buf.getvalue())
            except Exception:
                pass

        await context.bot.send_message(chat_id=chat_id, text=summary_text, reply_markup=reply_markup)

    except Exception as e:
        try:
            await status_msg.edit_text(f"일시적인 네트워크 지연이 발생했습니다. 10초 후 다시 입력해 주세요.")
        except Exception:
            pass

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
    app.run_polling()
