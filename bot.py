import os
import re
import json
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
import pandas as pd
from datetime import datetime
import google.generativeai as genai
import yfinance as yf
import requests
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "8980819571:AAHyzm6KjZ0rKocObWoS28ThBbVGhJ5TfUs")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "AQ.Ab8RN6KisWMYVxgBBJmBW3SgOAT9aJYvOPiWj42H4WR_Y56kRA")

# Pro 버전으로 변경 시
ticker_model = genai.GenerativeModel('gemini-2.5-flash')  # 단순 티커 변환은 빠른 Flash 추천
report_model = genai.GenerativeModel('gemini-2.5-pro', system_instruction=SYSTEM_PROMPT)  # 리포트 생성만 Pro로 격상

# 엄격하게 고정된 무특수문자/무소제목 9대 레이어 트레이딩 리포트 템플릿
SYSTEM_PROMPT = """
당신은 대한민국 최고의 실전 퀀트 트레이더이자 리스크 관리 전문가입니다.
사용자가 요청한 종목과 실시간 가격을 바탕으로, 아래에 제시된 [출력 양식 규칙]을 글자 하나 어김없이 엄격하게 준수하여 리포트를 작성하세요.

[출력 양식 규칙]
1. 콜론(:), 대괄호([]), 슬래시(/), 하이픈(-), 번호매기기(1., 2.), 이모티콘을 일체 사용하지 마세요. (단, 적합도 별표 ★, ☆ 및 괄호 ()는 허용)
2. 중간 소제목(적합도 평가 사유, 거시 매크로 등)을 절대 쓰지 말고 오직 줄바꿈과 공백으로만 문단을 구분하세요.
3. Alpha Signal 액션 플랜과 아래 방향성 내용 사이에는 반드시 빈 줄 1줄을 넣으세요.

[출력 템플릿]
{종목명} ({심볼})

현재 트레이딩 적합도 ★★★★☆ (점수 / 5.0)

핵심 상승 모멘텀 요약 및 리스크 감점 사유와 눌림목 돌파 대응 방향성 서술

거시경제 및 매크로 유동성 환율 금리 수급 환경 분석 서술

글로벌 섹터 지수 동조화 및 피어 그룹 밸류에이션 비교 분석 서술

개별 기업 펀더멘털 실적 모멘텀 및 하방 경직성 요인 분석 서술

단기 수급 흐름 거래량 및 시간외 동시호가 매물 소화 흐름 서술

국내외 기관 컨센서스 상단 목표가 및 현재가 대비 안전마진 서술

Alpha Signal 액션 플랜

방향성 롱 (Buy on Dip)
1차 진입 1차진입가격범위 (진입시점조건)
2차 진입 2차진입가격범위 (추가매수기준)
1차 목표 1차목표가 (도달 시 50% 분할 익절)
칼손절 칼손절가 (종가 이탈 시 상승 구조 무효화)

트리거 조건 및 행동 수칙
보조지표 수치와 차트 반등 패턴 조건 및 추격 매수 금지 등 실전 행동 수칙 서술
"""
report_model = genai.GenerativeModel('gemini-3.6-flash', system_instruction=SYSTEM_PROMPT)

def fetch_realtime_price_universal(user_query):
    prompt = f"""
    사용자가 입력한 검색어: '{user_query}'
    이 자산의 성격과 시세 조회를 위한 심볼을 JSON 형태로만 답해줘.
    - 한국 주식이면: {{"type": "stock", "symbol": "6자리종목코드.KS 또는 .KQ"}} (예: 삼성전자는 "005930.KS")
    - 해외 주식이면: {{"type": "stock", "symbol": "티커"}} (예: 테슬라는 "TSLA")
    - 암호화폐이면: {{"type": "crypto", "symbol": "바이낸스페어"}} (예: 비트코인은 "BTCUSDT", 도지코인은 "DOGEUSDT")
    - 판별 불가면: {{"type": "unknown", "symbol": ""}}
    오직 JSON 포맷만 출력해.
    """
    try:
        res = ticker_model.generate_content(prompt).text
        clean_json = re.search(r"\{.*\}", res, re.DOTALL).group()
        info = json.loads(clean_json)

        asset_type = info.get("type")
        symbol = info.get("symbol")

        if asset_type == "crypto":
            url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
            r = requests.get(url, timeout=5).json()
            return f"${float(r['price']):,.2f}", symbol
        elif asset_type == "stock":
            stock = yf.Ticker(symbol)
            price = stock.history(period="1d")['Close'].iloc[-1]
            if symbol.endswith(".KS") or symbol.endswith(".KQ"):
                return f"{int(price):,}원", symbol
            else:
                return f"${price:,.2f}", symbol
        else:
            return "시세 조회 불가", "UNKNOWN"
    except Exception:
        return "실시간 시세 조회 지연", user_query

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    welcome_text = (
        "Alpha Trade Bot 가동 중입니다.\n\n"
        "종목명을 입력하시면 실시간 시세 기반 Alpha Signal 리포트를 제공합니다.\n"
        "예시: 비트코인, 삼성전자, SK하이닉스, 테슬라"
    )
    await update.message.reply_text(welcome_text)

async def analyze(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_query = update.message.text
    status_msg = await update.message.reply_text(f"'{user_query}' 실시간 시세 수집 및 퀀트 분석 중입니다.")

    try:
        current_price, identified_symbol = fetch_realtime_price_universal(user_query)
        prompt = f"종목명 {user_query}\n심볼코드 {identified_symbol}\n현재 실시간 가격 {current_price}\n\n위 데이터를 바탕으로 규칙을 준수하여 리포트를 작성해줘."
        response = report_model.generate_content(prompt)
        await status_msg.edit_text(response.text)
    except Exception as e:
        err_msg = str(e)
        if "429" in err_msg:
            await status_msg.edit_text("무료 API 호출 한도에 일시 도달했습니다. 약 20초 뒤에 다시 시도해 주세요.")
        else:
            await status_msg.edit_text(f"분석 오류: {err_msg}")

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
    app.run_polling()
