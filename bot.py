import os
import re
import json
import uuid
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
import google.generativeai as genai
import yfinance as yf
import requests
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
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "여기에_GEMINI_API_KEY_입력")

genai.configure(api_key=GEMINI_API_KEY)
ai_model = genai.GenerativeModel('gemini-2.5-flash')

# 1회 호출 데이터 임시 저장소 (버튼 클릭 시 추가 요금 0원 보장)
REPORT_CACHE = {}

COMMON_SYMBOLS = {
    "비트코인": ("crypto", "BTCUSDT"),
    "이더리움": ("crypto", "ETHUSDT"),
    "리플": ("crypto", "XRPUSDT"),
    "도지코인": ("crypto", "DOGEUSDT"),
    "삼성전자": ("stock", "005930.KS"),
    "SK하이닉스": ("stock", "000660.KS"),
    "테슬라": ("stock", "TSLA"),
    "엔비디아": ("stock", "NVDA"),
    "애플": ("stock", "AAPL")
}

def fetch_realtime_price(user_query):
    query = user_query.strip()
    if query in COMMON_SYMBOLS:
        asset_type, symbol = COMMON_SYMBOLS[query]
    else:
        if any(c in query for c in ["코인", "USDT", "BTC", "ETH"]):
            asset_type, symbol = "crypto", query.upper()
        else:
            asset_type, symbol = "stock", query

    try:
        if asset_type == "crypto":
            url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
            r = requests.get(url, timeout=5).json()
            return f"{float(r['price']):,.2f} 달러", symbol
        else:
            stock = yf.Ticker(symbol)
            price = stock.history(period="1d")['Close'].iloc[-1]
            if symbol.endswith(".KS") or symbol.endswith(".KQ"):
                return f"{int(price):,} 원", symbol
            return f"{price:,.2f} 달러", symbol
    except Exception:
        return "실시간 호가 집계중", symbol

def generate_full_quant_report(stock_name, symbol, price):
    prompt = f"""
    당신은 대한민국 최고의 실전 퀀트 트레이더이자 데이터 기반 기술적 분석과 ICT 스마트 머니 콘셉트를 교차 검증하는 통합 분석 전문가입니다
    대상 종목 {stock_name} ({symbol})
    현재 실시간 가격 {price}

    아래 제시된 출력 양식 규칙을 글자 하나 어김없이 엄격하게 준수하여 리포트를 작성하세요

    출력 양식 규칙
    첫째 콜론 대괄호 슬래시 하이픈 번호매기기 이모티콘을 일체 사용하지 마세요 (단 적합도 별표 및 괄호는 허용)
    둘째 중간 소제목을 절대 쓰지 말고 오직 줄바꿈과 공백으로만 문단을 구분하세요
    셋째 Alpha Signal 액션 플랜과 아래 방향성 내용 사이에는 반드시 빈 줄 1줄을 넣으세요
    넷째 반드시 SPLIT 이라는 영문 단어를 기준으로 1부 타점 요약과 2부 심층 분석을 나누어 출력하세요
    다섯째 매수 매도 타점 도출 시 전통적 기술적 지표와 ICT 오더블럭 공정가치갭 유동성 스윕을 교차 검증하십시오 두 관점이 상충하면 별점을 낮추고 보수적 타점을 제시하며 완벽히 일치하는 구간에서만 강력한 진입 타점을 설정하십시오

    출력 템플릿
    {stock_name} ({symbol})
    
    현재가 {price}

    현재 트레이딩 적합도 ★★★★☆ (점수 5.0)

    Alpha Signal 액션 플랜

    방향성 롱 (Buy on Dip) 또는 숏
    1차 진입 1차진입가격범위 (전통적 지표 및 ICT 겹치는 합류점 지지 구간)
    2차 진입 2차진입가격범위 (추가 유동성 확보 구간 및 극한의 지지선)
    1차 목표 1차목표가 (도달 시 50퍼센트 분할 익절)
    칼손절 칼손절가 (시장 구조 붕괴 및 주요 추세선 이탈 기준선)

    트리거 조건 및 행동 수칙
    보조지표와 하위 프레임 시장 구조 전환 등 교차 검증에 기반한 행동 수칙 서술

    SPLIT

    핵심 상승 하락 모멘텀 요약 및 전통적 지표와 스마트 머니 유동성 관점의 통합 시나리오 서술

    거시경제 및 매크로 유동성 환율 금리 수급 환경 분석 서술

    글로벌 섹터 지수 동조화 및 피어 그룹 밸류에이션 비교 분석 서술

    개별 기업 펀더멘털 실적 모멘텀 및 하방 경직성 요인 분석 서술

    전통 지표 이동평균선 보조지표 와 ICT 관점 오더블럭 공정가치갭 의 교차 검증을 통한 단기 수급 흐름 및 매물 소화 과정 서술

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
        "종목명을 입력하시면 실시간 시세와 교차 검증 알고리즘을 반영한 분석을 시작합니다\n"
        "예시 비트코인 삼성전자 테슬라"
    )

async def analyze(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.message.text
    status_msg = await update.message.reply_text(f"{query} 실시간 데이터 수집 및 퀀트 연산 중입니다")

    try:
        current_price, symbol = fetch_realtime_price(query)
        summary_text, detail_text = generate_full_quant_report(query, symbol, current_price)

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
