import os
import re
import io
import gc
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

# 1. 암호화폐 맵
CRYPTO_MAP = {
    "BTC": "BTCUSDT", "비트코인": "BTCUSDT", "비트": "BTCUSDT",
    "ETH": "ETHUSDT", "이더리움": "ETHUSDT", "이더": "ETHUSDT",
    "XRP": "XRPUSDT", "리플": "XRPUSDT",
    "DOGE": "DOGEUSDT", "도지코인": "DOGEUSDT", "도지": "DOGEUSDT",
    "SOL": "SOLUSDT", "솔라나": "SOLUSDT",
    "ADA": "ADAUSDT", "에이다": "ADAUSDT",
    "AVAX": "AVAXUSDT", "아발란체": "AVAXUSDT"
}

# 2. 미국 주식 맵
US_STOCK_MAP = {
    "엔비디아": "NVDA", "NVDA": "NVDA",
    "테슬라": "TSLA", "TSLA": "TSLA",
    "애플": "AAPL", "AAPL": "AAPL",
    "마이크로소프트": "MSFT", "MSFT": "MSFT",
    "아마존": "AMZN", "AMZN": "AMZN",
    "구글": "GOOGL", "알파벳": "GOOGL", "GOOGL": "GOOGL",
    "메타": "META", "META": "META",
    "AMD": "AMD", "아이온큐": "IONQ", "IONQ": "IONQ",
    "팔란티어": "PLTR", "PLTR": "PLTR",
    "QQQ": "QQQ", "SPY": "SPY", "SOXL": "SOXL", "TQQQ": "TQQQ"
}

# 3. 국내 주요 대형주 맵
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
        url = f"https://api.bybit.com/v5/market/kline?category=linear&symbol={clean_sym}&interval=D&limit=50"
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

def get_us_stock_data(symbol):
    try:
        headers = {"User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X)"}
        url = f"https://api.stock.naver.com/chart/foreign/item/{symbol}?periodType=dayCandle&range=50"
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
                    "Volume": float(item.get("accumulatedTradingVolume", 0))
                })
            if records:
                df = pd.DataFrame(records).set_index("Date").sort_index()
                curr_p = float(df['Close'].iloc[-1])
                return f"{curr_p:,.2f} 달러", df
    except Exception:
        pass
    return None, None

def get_naver_realtime_price(code):
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        url = f"https://m.stock.naver.com/api/stock/{code}/basic"
        res = requests.get(url, headers=headers, timeout=2.0).json()
        p = str(res.get("closePrice", "")).replace(",", "").strip()
        if p and p.isdigit():
            return f"{int(p):,} 원"
    except Exception:
        pass
    return None

def fetch_domestic_stock_candles(code):
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        url = f"https://api.stock.naver.com/chart/domestic/market/stock/{code}?periodType=dayCandle&range=50"
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
    fig = None
    try:
        if df is None or len(df) < 5:
            return None

        chart_df = df.tail(35).copy()
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

def perform_comprehensive_quant_analysis(df):
    """과거 시계열 데이터를 통한 다중 팩터 종합 연산"""
    if df is None or len(df) < 20:
        return {
            "trend_status": "중립 추세",
            "rsi": 50.0,
            "ma20_status": "20일선 수렴",
            "vol_status": "평균 수준",
            "atr": "산출중"
        }

    col_map = {c: c.capitalize() for c in df.columns}
    df_c = df.rename(columns=col_map)
    closes = df_c['Close']
    curr_p = closes.iloc[-1]

    # 1. 20일선 추세 이격도
    ma20 = closes.rolling(20).mean().iloc[-1]
    disparity = ((curr_p - ma20) / ma20) * 100
    if disparity > 3.0:
        ma20_status = f"20일선 상회 (+{disparity:.1f}% 이격 / 단기 상승 우세)"
    elif disparity < -3.0:
        ma20_status = f"20일선 하회 ({disparity:.1f}% 이격 / 단기 저항 구간)"
    else:
        ma20_status = f"20일선 밀착 지지 테스트 ({disparity:.1f}%)"

    # 2. RSI (14)
    delta = closes.diff()
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rs = gain / (loss + 1e-9)
    rsi_val = float((100 - (100 / (1 + rs))).iloc[-1])
    if np.isnan(rsi_val):
        rsi_val = 50.0

    # 3. 거래량 모멘텀
    v_col = 'Volume' if 'Volume' in df_c.columns else 'Vol'
    curr_v = df_c[v_col].iloc[-1]
    avg_v5 = df_c[v_col].iloc[-6:-1].mean() if len(df_c) >= 6 else curr_v
    vol_ratio = (curr_v / (avg_v5 + 1e-9)) * 100
    if vol_ratio >= 150:
        vol_status = f"5일 평균 대비 {vol_ratio:.0f}% 급증 (유의미한 수급 유입)"
    elif vol_ratio <= 70:
        vol_status = f"5일 평균 대비 {vol_ratio:.0f}% 감소 (거래량 마름/관망세)"
    else:
        vol_status = f"5일 평균 대비 {vol_ratio:.0f}% (정상 거래 범위)"

    # 4. 일일 변동폭 ATR (14)
    high_low = df_c['High'] - df_c['Low']
    atr_val = round(float(high_low.tail(14).mean()), 2)

    # 5. 종합 시장 국면 판정
    if curr_p >= ma20 and rsi_val >= 55 and vol_ratio >= 120:
        trend_status = "상승 모멘텀 가속 국면 (추세 추종 및 돌파 유효)"
    elif curr_p < ma20 and rsi_val <= 40:
        trend_status = "하락 추세 및 과매도 국면 (단기 낙폭과대 반등 탐색)"
    elif curr_p >= ma20 and (45 <= rsi_val <= 55):
        trend_status = "20일선 눌림목 지지 국면 (손익비 우수 분할 진입권)"
    else:
        trend_status = "박스권 횡보 및 매물 소화 국면 (변동성 축소)"

    return {
        "trend_status": trend_status,
        "rsi": round(rsi_val, 1),
        "ma20_status": ma20_status,
        "vol_status": vol_status,
        "atr": atr_val
    }

def fetch_technical_quant_data(symbol, asset_type):
    quant_info = {}
    curr_price_str = None
    df = None

    try:
        if asset_type == "crypto":
            curr_price_str, df = get_crypto_bybit_data(symbol)
        elif asset_type == "overseas_stock":
            curr_price_str, df = get_us_stock_data(symbol)
        elif asset_type == "domestic_stock":
            curr_price_str = get_naver_realtime_price(symbol)
            df = fetch_domestic_stock_candles(symbol)
            if not curr_price_str and df is not None and not df.empty:
                c_val = float(df['Close'].iloc[-1])
                curr_price_str = f"{int(c_val):,} 원"

        if not curr_price_str:
            return None

        quant_info['current_price'] = curr_price_str
        quant_info['analysis'] = perform_comprehensive_quant_analysis(df)
        quant_info['chart_df'] = df

        return quant_info
    except Exception:
        return None

def generate_full_quant_report(stock_name, symbol, price, quant_data):
    an = quant_data.get('analysis', {}) if quant_data else {}
    tech_context = f"""
    [과거 시계열 기반 다중 팩터 정밀 연산치]
    - 현재가: {price}
    - 종합 시장 국면: {an.get('trend_status', '중립 추세')}
    - 이동평균선 위치: {an.get('ma20_status', '산출중')}
    - RSI 강도: {an.get('rsi', 50.0)}
    - 거래량 흐름: {an.get('vol_status', '보통')}
    - 일일 평균 변동폭(ATR): {an.get('atr', '산출중')}
    """

    prompt = f"""
    당신은 대한민국 최고의 실전 퀀트 트레이더입니다.
    특정 캔들 패턴 하나에 치우치지 말고, 제공된 과거 시계열 종합 연산 팩터(추세, RSI, 거래량, 변동폭)를 입체적으로 교차 검증하여 냉철한 실전 전략을 작성하세요.

    대상 종목: {stock_name} ({symbol})
    {tech_context}

    [★ 다중 팩터 기반 트레이딩 평가 규칙 ★]
    1. 이평선 추세, RSI, 거래량의 종합 정합성을 따져 트레이딩 적합도(1.0 ~ 5.0)를 산정하세요.
       - 역배열/거래량 없는 하락: 2.0점 이하 (숏/관망 권고)
       - 박스권 횡보: 3점대 (박스권 하단 분할 대응)
       - 20일선 눌림목 지지/거래량 폭발 돌파: 4.5점 이상 (롱 적극 대응)
    2. 타점(진입가, 목표가, 손절가)은 제공된 ATR(일일 변동폭)과 지지·저항대를 계산하여 손익비 1:2 이상의 현실적 가격을 제시하세요.
    3. 콜론, 하이픈, 번호매기기, 특수기호 없이 작성하고 영문 SPLIT 으로 1부 타점 카드와 2부 해설을 나누세요.

    {stock_name} ({symbol})
    
    현재가 {price}

    현재 트레이딩 적합도 ★★★★☆ (점수 4.5)

    Alpha Signal 액션 플랜

    방향성 롱 (눌림목 매수 또는 돌파) 또는 숏 (관망 및 비중 축소)
    1차 진입 1차진입가구간 (과거 지지대 확인 1차 분할)
    2차 진입 2차진입가구간 (하단 마지노선 2차 분할)
    1차 목표 1차목표가 (직전 매물벽 도달 시 50퍼센트 분할 익절)
    2차 목표 2차목표가 (상단 목표 밴드 최종 익절)
    칼손절 칼손절가 (구조적 지지선 이탈 시 손익비 기준가)

    트리거 조건 및 행동 요령
    당일 호가창 체결 강도 및 실전 진입 타이밍 서술

    SPLIT

    과거 시계열 데이터(20일선 이격도, RSI, 거래량 변화)가 보여주는 종합 추세 진단

    현재 시장 매크로 환경 및 섹터 수급 집중도 심층 분석

    예상 수익률 대비 감수할 리스크의 손익비 분석 및 최종 매매 코멘트
    """
    response = ai_model.generate_content(prompt).text
    parts = response.split("SPLIT")
    summary = parts[0].strip()
    detail = parts[1].strip() if len(parts) > 1 else "심층 분석 데이터를 불러올 수 없습니다"
    return summary, detail

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Alpha Signal Quant Bot 가동 중입니다\n\n"
        "종목명을 입력하시면 과거 시계열 다중 팩터 분석과 실시간 퀀트 액션 플랜을 전송합니다\n"
        "예시 삼성중공업 엔비디아 테슬라 비트코인 sk하이닉스 대장"
    )

async def analyze(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.message.text.strip()
    chat_id = update.effective_chat.id

    if query in ["대장", "대장주", "종목추천", "추천", "/대장"]:
        query = "삼성전기"

    status_msg = await update.message.reply_text(f"{query} 다중 팩터 종합 분석 중입니다...")

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
            formal_name = query
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
        if len(REPORT_CACHE) > 50:
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
                await context.bot.send_photo(chat_id=chat_id, photo=chart_buf.getvalue())
            except Exception:
                pass

        await context.bot.send_message(chat_id=chat_id, text=summary_text, reply_markup=reply_markup)

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
