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

# 9단계 정밀 퀀트 트레이딩 프롬프트
SYSTEM_PROMPT = """
당신은 대한민국 최고의 실전 퀀트 트레이더이자 리스크 관리 전문가입니다.
사용자가 요청한 종목과 실시간 가격을 바탕으로, 아래의 [9단계 정밀 퀀트 분석 양식]에 맞춰 명확하고 실전적인 수치 위주로 분석을 제공하세요.
지저분한 마크다운 표나 깨지는 특수기호 없이, 모바일에서 완벽히 읽히도록 깔끔한 줄바꿈과 텍스트로만 작성하세요.

[답변 양식]
[Alpha AI] [종목명] 실전 퀀트 9단계 정밀 리포트

1. 트레이딩 적합도
- 별점: [★ 1~5개] (X.X / 5.0)
- 성격: [초단타 / 데이트레이딩 / 스윙 / 중장기 / 관망]

2. 실시간 가격 및 지표 해석
- 기준 가격: [실시간 가격]
- 보조지표: RSI, MACD, 주요 이동평균선 정배열/역배열 상태 분석 요약

3. 핵심 지지 및 저항 레벨
- 1차 지지선 / 2차 지지선: [수치]
- 1차 저항선 / 2차 저항선: [수치]

4. 수급 및 모멘텀 분석
- 거래량/거래대금 흐름 및 매수 주체(외인/기관/세력) 수급 강도

5. 포지션 방향성
- 최종 포지션: [롱(매수) / 숏(매도) / 관망]
- 매매 전략: [눌림목 분할매수 / 돌파추종 / 박스권 매매 등]

6. 실전 분할 진입 타점
- 1차 진입가: [구체적 가격] (비중 10~15%)
- 2차 진입가: [구체적 가격] (비중 15~20%)

7. 수익 실현 목표가
- 1차 목표가: [가격] (+수익률%) -> 물량 50% 분할 익절
- 2차 목표가: [가격] (+수익률%) -> 잔량 전량 익절

8. 리스크 관리 (칼손절 및 손익비)
- 칼손절가: [가격] (-손실률%) -> 이탈 시 무조건 전량 정리
- 예상 손익비 (Risk/Reward): 1 : [손익비 수치]

9. 실전 진입 트리거 및 행동 수칙
- 분봉/일봉 차트상 진입 신호 조건 및 매매 시 주의사항 2~3줄 요약
"""
report_model = genai.GenerativeModel('gemini-3.6-flash', system_instruction=SYSTEM_PROMPT)

# 주요 종목 빠른 조회용 맵 (API 호출 없이 즉시 파싱)
QUICK_MAP = {
    "삼성전자": "005930.KS", "삼전": "005930.KS",
    "sk하이닉스": "000660.KS", "하이닉스": "000660.KS",
    "sk증권": "001510.KS",
    "현대차": "005380.KS", "기아": "000270.KS",
    "카카오": "035720.KS", "네이버": "035420.KS",
    "비트코인": "BTCUSDT", "btc": "BTCUSDT",
    "이더리움": "ETHUSDT", "eth": "ETHUSDT",
    "리플": "XRPUSDT", "xrp": "XRPUSDT",
    "솔라나": "SOLUSDT", "sol": "SOLUSDT",
    "도지코인": "DOGEUSDT", "doge": "DOGEUSDT",
    "테슬라": "TSLA", "엔비디아": "NVDA", "애플": "AAPL", "마이크로소프트": "MSFT"
}

def fetch_realtime_price_universal(user_query):
    query_clean = user_query.strip().lower()
    symbol = QUICK_MAP.get(query_clean, None)

    # 1. 코인 직접 조회
    if symbol and symbol.endswith("USDT"):
        try:
            url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
            r = requests.get(url, timeout=4).json()
            return f"${float(r['price']):,.2f}", symbol
        except:
            pass

    # 2. 주식 직접 조회
    if symbol:
        try:
            stock = yf.Ticker(symbol)
            price = stock.history(period="1d")['Close'].iloc[-1]
            if symbol.endswith(".KS") or symbol.endswith(".KQ"):
                return f"{int(price):,}원", symbol
            return f"${price:,.2f}", symbol
        except:
            pass

    # 3. 맵에 없는 경우 일반 티커로 yfinance 시도
    try:
        stock = yf.Ticker(user_query.strip())
        price = stock.history(period="1d")['Close'].iloc[-1]
        return f"{price:,.2f}", user_query
    except:
        return "최신 거래소 시세", user_query

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    welcome_text = (
        "[Alpha Trade Bot] 가동 중\n"
        "종목명을 입력하시면 실시간 시세 기반 9단계 정밀 분석을 제공합니다.\n"
        "(예: 삼성전자, SK증권, SK하이닉스, 테슬라, 비트코인)"
    )
    await update.message.reply_text(welcome_text)

async def analyze(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_query = update.message.text
    status_msg = await update.message.reply_text(f"🔍 '{user_query}' 실시간 데이터 수집 및 9단계 분석 중...")

    try:
        current_price, identified_symbol = fetch_realtime_price_universal(user_query)
        prompt = f"종목명: {user_query} ({identified_symbol})\n현재 실시간 가격: {current_price}\n\n위 실시간 가격을 기준으로 9단계 정밀 리포트를 작성해줘."
        response = report_model.generate_content(prompt)
        await status_msg.edit_text(response.text)
    except Exception as e:
        err_msg = str(e)
        if "429" in err_msg:
            await status_msg.edit_text("⏳ 무료 API 호출 한도에 잠시 도달했습니다. 약 20초 뒤에 다시 시도해 주세요.")
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
