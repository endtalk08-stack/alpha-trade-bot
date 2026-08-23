
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

genai.configure(api_key=GEMINI_API_KEY)
ticker_model = genai.GenerativeModel('gemini-3.6-flash')

SYSTEM_PROMPT = """
당신은 최고 수준의 퀀트 트레이더이자 리스크 관리 전문가 'Alpha AI'입니다.
제공된 [현재 실시간 가격]을 반드시 기준으로 삼아 오차 없는 실전 트레이딩 타점을 제시하세요.

[필수 리포트 양식]
1. 트레이딩 적합도 별점 (★ 1~5점)
2. 현재 실시간 가격 및 핵심 모멘텀/차트 지표 해석
3. 포지션 방향성 (롱/매수 또는 관망/숏)
4. 1차 진입가, 2차 진입가, 1차 목표가, 2차 목표가, 칼손절가 (명확한 수치)
5. 실전 진입 트리거 조건
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
            return f"${float(r['price']):,.2f} (Binance 실시간)", symbol
        elif asset_type == "stock":
            stock = yf.Ticker(symbol)
            price = stock.history(period="1d")['Close'].iloc[-1]
            if symbol.endswith(".KS") or symbol.endswith(".KQ"):
                return f"{int(price):,}원 (한국거래소)", symbol
            else:
                return f"${price:,.2f} (미국/해외거래소)", symbol
        else:
            return "시세 조회 불가", "UNKNOWN"
    except Exception:
        return "실시간 시세 조회 일시적 지연", user_query

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    welcome_text = (
        "🚀 **Alpha Trade Bot 가동 완료**\n\n"
        "국내/미국 주식, 암호화폐 상관없이 아무 종목이나 편하게 입력하세요!\n"
        "(예: `삼전`, `테슬라`, `도지코인`, `엔비디아`, `SOL` 등)"
    )
    await update.message.reply_text(welcome_text, parse_mode="Markdown")

async def analyze(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_query = update.message.text
    status_msg = await update.message.reply_text(f"🔍 '{user_query}' 실시간 데이터 수집 및 퀀트 분석 중...")

    try:
        current_price, identified_symbol = fetch_realtime_price_universal(user_query)
        prompt = f"종목명/심볼: {user_query} ({identified_symbol})\n현재 실시간 가격: {current_price}\n\n위 실시간 가격을 바탕으로 실전 매매 리포트를 작성해줘."
        response = report_model.generate_content(prompt)
        await status_msg.edit_text(response.text, parse_mode="Markdown")
    except Exception as e:
        await status_msg.edit_text(f"❌ 분석 오류: {str(e)}")

# Render Web Service 헬스체크용 경량 더미 웹 서버
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
    # 포트 감지용 백그라운드 웹서버 실행
    threading.Thread(target=run_web_server, daemon=True).start()
    
    print("⚡ Render 상시 배포 모드로 봇을 가동합니다.")
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, analyze))
    app.run_polling()
