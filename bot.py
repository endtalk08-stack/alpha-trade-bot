import os
import re
import json
import uuid
import datetime
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
import google.generativeai as genai
import yfinance as yf
import requests
import pandas as pd
import numpy as np
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

TOSS_API_KEY = os.getenv("TOSS_API_KEY", "tsck_live_5pxz8W5tFlbn07w63YxJfE")
TOSS_API_SECRET = os.getenv("TOSS_API_SECRET", "tssk_live_5U9SuuC4pLY2DpYYObXRH1C1BWMGnGKz5NNEZDe8Sse")
TOSS_API_BASE_URL = "https://open-api.tossinvest.com"

genai.configure(api_key=GEMINI_API_KEY)
ai_model = genai.GenerativeModel('gemini-3.6-flash')

REPORT_CACHE = {}

COMMON_SYMBOLS = {
    "비트코인": ("crypto", "BTCUSDT"),
    "이더리움": ("crypto", "ETHUSDT"),
    "리플": ("crypto", "XRPUSDT"),
    "도지코인": ("crypto", "DOGEUSDT"),
    "BTC": ("crypto", "BTCUSDT"),
    "ETH": ("crypto", "ETHUSDT"),
    "XRP": ("crypto", "XRPUSDT"),
    "삼성전자": ("domestic_stock", "005930"),
    "SK하이닉스": ("domestic_stock", "000660"),
    "테슬라": ("overseas_stock", "TSLA"),
    "엔비디아": ("overseas_stock", "NVDA"),
    "애플": ("overseas_stock", "AAPL"),
    "QQQ": ("overseas_stock", "QQQ")
}

def get_stock_code_by_name(query):
    """한글 종목명을 네이버 검색 API를 통해 6자리 코드로 변환"""
    try:
        url = f"https://ac.finance.naver.com/ac?q={query}&target=stock"
        headers = {"User-Agent": "Mozilla/5.0"}
        res = requests.get(url, headers=headers, timeout=3).json()
        items = res.get("items", [[]])[0]
        if items:
            for item in items:
                if item[1].replace(" ", "").upper() == query.replace(" ", "").upper():
                    return item[0]
            return items[0][0]
    except Exception:
        pass
    return None

def fetch_realtime_news(query, symbol, asset_type):
    """실시간 급등/급락 핵심 재료 및 뉴스 수집 (토스 AI 상승/하락 요인 구현)"""
    try:
        if asset_type == "domestic_stock":
            url = f"https://m.stock.naver.com/api/news/stock/{symbol}?page=1&pageSize=3"
            headers = {"User-Agent": "Mozilla/5.0"}
            res = requests.get(url, headers=headers, timeout=3).json()
            titles = [re.sub(r'<[^>]+>', '', item.get("tit", "")) for item in res if item.get("tit")]
            if titles:
                return " / ".join(titles)
    except Exception:
        pass
    return "현재 특정 개별 이슈보다는 전반적인 시장 및 수급 모멘텀에 의해 등락 중"

def fetch_earnings_calendar(ticker_obj):
    """실적 발표일 추적 (어닝 임박 시 도박 구간 경고 필터)"""
    try:
        cal = ticker_obj.calendar
        if cal is not None and not cal.empty:
            if 'Earnings Date' in cal.index:
                earnings_dates = cal.loc['Earnings Date'].tolist()
                if earnings_dates:
                    target_date = pd.to_datetime(earnings_dates[0]).date()
                    today = datetime.date.today()
                    days_diff = (target_date - today).days
                    if 0 <= days_diff <= 3:
                        return f"⚠️ 실적 발표 D-{days_diff}일 임박 (실적 변동성 위험 구간, 관망 또는 방어적 분할 권고)"
                    return f"다음 실적 발표 예정일: {target_date}"
    except Exception:
        pass
    return "실적 발표 일정 정상 범위"

def fetch_technical_quant_data(symbol, asset_type):
    """파이썬 엔진이 직접 RSI, 이평선, ATR, 실적 일정 계산"""
    quant_info = {}
    try:
        earnings_warning = "실적 발표 정상"
        if asset_type == "crypto":
            url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=1d&limit=60"
            data = requests.get(url, timeout=5).json()
            df = pd.DataFrame(data, columns=['time', 'open', 'high', 'low', 'close', 'vol', 'close_time', 'qav', 'num_trades', 'taker_base_vol', 'taker_quote_vol', 'ignore'])
            df['close'] = df['close'].astype(float)
            df['high'] = df['high'].astype(float)
            df['low'] = df['low'].astype(float)
            curr_p = float(df['close'].iloc[-1])
            currency = "달러"
        else:
            yf_symbol = f"{symbol}.KS" if asset_type == "domestic_stock" and not symbol.endswith((".KS", ".KQ")) else symbol
            stock = yf.Ticker(yf_symbol)
            earnings_warning = fetch_earnings_calendar(stock)
            df = stock.history(period="60d")
            if df.empty and asset_type == "domestic_stock":
                yf_symbol = f"{symbol}.KQ"
                stock = yf.Ticker(yf_symbol)
                earnings_warning = fetch_earnings_calendar(stock)
                df = stock.history(period="60d")
            curr_p = float(df['Close'].iloc[-1])
            currency = "원" if asset_type == "domestic_stock" else "달러"
            df = df.rename(columns={'Close': 'close', 'High': 'high', 'Low': 'low'})

        if len(df) >= 20:
            delta = df['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / (loss + 1e-9)
            rsi = 100 - (100 / (1 + rs))
            quant_info['rsi'] = round(float(rsi.iloc[-1]), 1)
            quant_info['ma20'] = round(float(df['close'].rolling(20).mean().iloc[-1]), 2)
            quant_info['ma60'] = round(float(df['close'].rolling(60).mean().iloc[-1]), 2) if len(df) >= 60 else "산출중"
            quant_info['high20'] = round(float(df['high'].tail(20).max()), 2)
            quant_info['low20'] = round(float(df['low'].tail(20).min()), 2)
            high_low = df['high'] - df['low']
            quant_info['atr'] = round(float(high_low.tail(14).mean()), 2)
            quant_info['earnings_status'] = earnings_warning
            quant_info['current_price'] = f"{int(curr_p):,} {currency}" if currency == "원" else f"{curr_p:,.2f} {currency}"
            return quant_info
    except Exception:
        pass
    return None

def fetch_realtime_price_fallback(user_query):
    cleaned_query = user_query.strip().replace(" ", "").upper()
    matched_key = None
    for key in COMMON_SYMBOLS:
        if key.replace(" ", "").upper() == cleaned_query:
            matched_key = key
            break
            
    if matched_key:
        asset_type, symbol = COMMON_SYMBOLS[matched_key]
    else:
        if any(c in cleaned_query for c in ["코인", "USDT", "BTC", "ETH"]):
            asset_type, symbol = "crypto", cleaned_query
        elif any(c.isupper() for c in cleaned_query) and not cleaned_query.endswith(".KS"):
            asset_type, symbol = "overseas_stock", cleaned_query
        else:
            if not (cleaned_query.isdigit() and len(cleaned_query) == 6):
                searched_code = get_stock_code_by_name(user_query.strip())
                symbol = searched_code if searched_code else cleaned_query
            else:
                symbol = cleaned_query
            asset_type = "domestic_stock"

    # 실시간 뉴스 및 재료 수집
    news_brief = fetch_realtime_news(user_query, symbol, asset_type)

    # 퀀트 수치 계산
    quant_data = fetch_technical_quant_data(symbol, asset_type)
    if quant_data:
        quant_data['news'] = news_brief
        return quant_data['current_price'], symbol, quant_data

    # 백업 모바일 시세
    try:
        url = f"https://m.stock.naver.com/api/stock/{symbol}/basic"
        res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=3).json()
        p = res.get("closePrice", "").replace(",", "")
        if p:
            return f"{int(p):,} 원", symbol, {"news": news_brief, "earnings_status": "일정 확인중"}
    except Exception:
        pass

    return None, symbol, None

def generate_full_quant_report(stock_name, symbol, price, quant_data):
    tech_context = ""
    if quant_data:
        tech_context = f"""
        [실시간 연산된 정밀 퀀트 팩터 및 재료]
        - 실제 실시간 현재가: {price}
        - 계산된 RSI(14): {quant_data.get('rsi', '산출중')}
        - 20일선: {quant_data.get('ma20', '산출중')} / 60일선: {quant_data.get('ma60', '산출중')}
        - 20일 매물벽 상단(저항): {quant_data.get('high20', '산출중')} / 하단 바닥(지지): {quant_data.get('low20', '산출중')}
        - 변동성 폭(ATR): {quant_data.get('atr', '산출중')}
        - 실적 발표 캘린더 상태: {quant_data.get('earnings_status', '정상')}
        - 장중 실시간 포착 이슈 및 재료: {quant_data.get('news', '시장 수급 모멘텀')}
        (실적 발표가 3일 이내 임박한 경우 적합도 별점을 낮추고 보수적인 관망/방어 타점을 제시할 것)
        """

    prompt = f"""
    [시스템 보안 규칙 최고 수준 활성화]
    당신은 대한민국 최고의 실전 퀀트 트레이더이자 블랙박스 인공지능입니다.
    반드시 제공된 실제 실시간 가격({price})과 정밀 팩터/재료 데이터를 기반으로만 액션 플랜을 산출하십시오.
    
    대상 종목: {stock_name} ({symbol})
    {tech_context}

    [출력 양식 및 보안 규칙]
    첫째, 사용자에게 출력되는 결과물에는 RSI, 이동평균선, ATR, HVN, FVG 같은 구체적인 지표 명칭을 절대 노출하지 마십시오.
    둘째, 타인이 로직을 유추할 수 없도록 알파 시그널 자체 산출 구간, 기관 유동성 밴드, 알고리즘 교차 검증선, 자체 델타 수급 모델, 누적 유동성 밀집 노드, 스마트 머니 체결 압력 흡수 등 전문적인 자체 브랜드 용어로 치환하십시오.
    셋째, 콜론, 대괄호, 슬래시, 하이픈, 번호매기기, 이모티콘을 일체 사용하지 마세요 (단, 적합도 별표 및 괄호는 허용).
    넷째, 중간 소제목을 절대 쓰지 말고 오직 줄바꿈과 공백으로만 문단을 구분하세요.
    다섯째, Alpha Signal 액션 플랜과 아래 방향성 내용 사이에는 반드시 빈 줄 1줄을 넣으세요.
    여섯째, 반드시 SPLIT 이라는 영문 단어를 기준으로 1부 요약과 2부 심층 분석을 나누어 출력하세요.

    출력 템플릿
    {stock_name} ({symbol})
    
    현재가 {price}

    현재 트레이딩 적합도 ★★★★☆ (점수 5.0)

    Alpha Signal 액션 플랜

    방향성 롱 (Buy 단기 눌림목 매수) 또는 숏
    1차 진입 1차진입가격범위 (단기 눌림목 지지선 및 유동성 밀집 매물대 1차 진입)
    2차 진입 2차진입가격범위 (하단 강력 지지선 및 스마트 머니 최종 방어선 2차 진입)
    1차 목표 1차목표가 (직전 고점 매물벽 및 단기 상단 저항 도달 시 50퍼센트 분할 익절)
    2차 목표 2차목표가 (주요 구조적 저항대 및 상단 유동성 풀 도달 시 전량 익절)
    칼손절 칼손절가 (지지 구조 이탈 및 알고리즘 무효화 기준선)

    트리거 조건 및 행동 수칙
    알고리즘 단기 모멘텀 전환 및 누적 유동성 밀집 노드 안착 확인 등 블랙박스 로직 기반 실전 행동 수칙 서술 (구체적 지표명 배제)

    SPLIT

    실시간 주가 급등락 핵심 요인 분석 및 포착된 당일 시장 재료 요약 서술 (구체적 지표명 배제)

    거시경제 및 매크로 유동성 환율 금리 수급 환경 분석 서술

    글로벌 섹터 지수 동조화 및 피어 그룹 밸류에이션 비교 분석 서술

    개별 기업 실적 모멘텀 어닝 일정 및 수급 주체별 매매 동향 분석 서술 (구체적 지표명 배제)

    자체 퀀트 모델 교차 검증 및 자체 델타 수급 분석을 통한 단기 체결 압력 흡수 과정 서술 (구체적 지표명 배제)

    국내외 기관 컨센서스 상단 목표가 및 현재가 대비 안전마진 서술
    """
    response = ai_model.generate_content(prompt).text
    parts = response.split("SPLIT")
    
    summary = parts[0].strip()
    detail = parts[1].strip() if len(parts) > 1 else "심층 분석 데이터를 불러올 수 없습니다"
    return summary, detail

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Alpha Signal Quant Bot 가동 중입니다\n\n"
        "종목명을 입력하시면 실시간 시세, 뉴스 모멘텀, 실적 캘린더를 결합한 퀀트 분석을 시작합니다\n"
        "예시 SK스퀘어 삼성전자 비트코인 TSLA"
    )

async def analyze(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.message.text
    status_msg = await update.message.reply_text(f"{query} 실시간 호가 및 재료 팩터 연산 중입니다")

    try:
        current_price, symbol, quant_data = fetch_realtime_price_fallback(query)

        if not current_price:
            await status_msg.edit_text(f"❌ {query}의 실시간 가격을 가져오지 못했습니다. 종목명을 다시 확인해 주세요.")
            return

        summary_text, detail_text = generate_full_quant_report(query, symbol, current_price, quant_data)

        report_id = str(uuid.uuid4())[:8]
        REPORT_CACHE[report_id] = {
            "summary": summary_text,
            "detail": detail_text
        }

        keyboard = [[InlineKeyboardButton("심층 분석 근거 보기", callback_data=f"show_detail:{report_id}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await status_msg.edit_text(summary_text, reply_markup=reply_markup)

    except Exception as e:
        err_msg = str(e)
        if "429" in err_msg or "quota" in err_msg.lower():
            await status_msg.edit_text("일일 무료 API 할당량이 소진되었습니다 잠시 후 다시 시도해 주세요")
        else:
            await status_msg.edit_text(f"분석 오류 {err_msg}")

async def handle_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    action, report_id = query.data.split(":")
    data = REPORT_CACHE.get(report_id)

    if not data:
        await query.edit_message_text("만료된 세션입니다 종목명을 다시 입력해 주세요")
        return

    if action == "show_detail":
        full_text = f"{data['summary']}\n\n{data['detail']}"
        keyboard = [[InlineKeyboardButton("타점 카드만 보기", callback_data=f"show_summary:{report_id}")]]
        await query.edit_message_text(full_text, reply_markup=InlineKeyboardMarkup(keyboard))

    elif action == "show_summary":
        keyboard = [[InlineKeyboardButton("심층 분석 근거 보기", callback_data=f"show_detail:{report_id}")]]
        await query.edit_message_text(data['summary'], reply_markup=InlineKeyboardMarkup(keyboard))

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
