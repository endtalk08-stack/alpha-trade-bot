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

# 이모티콘, 마크다운 기호(**, ---, |)를 배제한 순수 텍스트 양식
SYSTEM_PROMPT = """
당신은 대한민국 최고의 실전 퀀트 트레이더입니다.
가독성을 위해 이모티콘(이모지), 별표(★), 표(Table), 마크다운 기호(**, ---, | 등)를 일체 사용하지 마세요.
오직 아래의 순수 텍스트 양식으로만 답변하세요.

[답변 양식]
[종목명 실전 타점 브리핑]
- 현재가: [실시간가격]
- 포지션: [매수/관망/매도] (적합도: X.X / 5.0)

[매매 기준가]
- 진입 타점: [1차 진입가] ~ [2차 진입가]
- 1차 목표가: [1차 목표가] (수익률%)
- 2차 목표가: [2차 목표가] (수익률%)
- 칼손절 기준: [손절가] 이탈 시 정리

[핵심 코멘트]
[차트, 수급 핵심 요약 및 진입 타이밍 조건 1~2문장]
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
            return f"${float(r['price']):,.2f} (Binance)", symbol
        elif asset_type == "stock":
            stock = yf.Ticker(symbol)
            price = stock.history(period="1d")['Close'].iloc[-1]
            if symbol.endswith(".KS") or symbol.endswith(".KQ"):
                return f"{int(price):,}원 (한국거래소)", symbol
            else:
                return f"${price:,.2f} (해외거래소)", symbol
        else:
            return "시세 조회 불가", "UNKNOWN"
    except Exception:
        return "실시간 시세 조회 지연", user_query

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    welcome_text = (
        "[Alpha Trade Bot]\n"
        "종목명을 입력하시면 실시간 가격 기반 타점을 분석해 드립니다.\n"
        "(예: 삼성전자, SK증권, 테슬라, 비트코인)"
    )
    await update.message.reply_text(welcome_text)

async def analyze(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_query = update.message.text
    status_msg = await update.message.reply_text(f"'{user_query}' 분석 중...")

    try:
        current_price, identified_symbol = fetch_realtime_price_universal(user_query)
        prompt = f"종목명: {user_query} ({identified_symbol})\n현재 실시간 가격: {current_price}\n\n위 가격을 바탕으로 순수 텍스트 양식에 맞춰 작성해줘."
        response = report_model.generate_content(prompt)
        await status_msg.edit_text(response.text)
    except Exception as e:
        await status_msg.edit_text(f"분석 오류: {str(e)}")

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
