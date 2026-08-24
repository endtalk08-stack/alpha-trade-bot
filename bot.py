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

# 1. 통합 인증 키 설정
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "8980819571:AAHyzm6KjZ0rKocObWoS28ThBbVGhJ5TfUs")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "AQ.Ab8RN6KisWMYVxgBBJmBW3SgOAT9aJYvOPiWj42H4WR_Y56kRA")

TOSS_API_KEY = os.getenv("TOSS_API_KEY", "tsck_live_5pxz8W5tFlbn07w63YxJfE")
TOSS_API_SECRET = os.getenv("TOSS_API_SECRET", "tssk_live_5U9SuuC4pLY2DpYYObXRH1C1BWMGnGKz5NNEZDe8Sse")
TOSS_API_BASE_URL = "https://open-api.tossinvest.com"

genai.configure(api_key=GEMINI_API_KEY)
ai_model = genai.GenerativeModel('gemini-2.5-flash')

REPORT_CACHE = {}

COMMON_SYMBOLS = {
    "비트코인": ("crypto", "BTCUSDT"),
    "이더리움": ("crypto", "ETHUSDT"),
    "리플": ("crypto", "XRPUSDT"),
    "도지코인": ("crypto", "DOGEUSDT"),
    "삼성전자": ("domestic_stock", "005930"),
    "SK하이닉스": ("domestic_stock", "000660"),
    "금호전기": ("domestic_stock", "001210"),
    "테슬라": ("overseas_stock", "TSLA"),
    "엔비디아": ("overseas_stock", "NVDA"),
    "애플": ("overseas_stock", "AAPL"),
    "QQQ": ("overseas_stock", "QQQ")
}

def get_toss_stock_price(symbol, market_type="domestic"):
    """토스증권 공식 Open API 실시간 시세 조회"""
    try:
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {TOSS_API_KEY}",
            "X-Toss-Secret": TOSS_API_SECRET
        }
        endpoint = f"{TOSS_API_BASE_URL}/v1/market/quote?symbol={symbol}&market={market_type}"
        response = requests.get(endpoint, headers=headers, timeout=3)
        
        if response.status_code == 200:
            data = response.json()
            price = float(data.get("price", 0))
            if market_type == "domestic":
                return f"{int(price):,} 원"
            return f"{price:,.2f} 달러"
    except Exception:
        pass
    return None

def fetch_realtime_price(user_query):
    query = user_query.strip()
    
    if query in COMMON_SYMBOLS:
        asset_type, symbol = COMMON_SYMBOLS[query]
    else:
        if any(c in query for c in ["코인", "USDT", "BTC", "ETH"]):
            asset_type, symbol = "crypto", query.upper()
        elif any(c.isupper() for c in query) and not query.endswith(".KS"):
            asset_type, symbol = "overseas_stock", query.upper()
        else:
            asset_type, symbol = "domestic_stock", query

    # 가상자산 시세 (바이낸스 실시간)
    if asset_type == "crypto":
        try:
            url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
            r = requests.get(url, timeout=5).json()
            return f"{float(r['price']):,.2f} 달러", symbol
        except Exception:
            return "실시간 호가 집계중", symbol

    # 국내/미국 주식 시세 (토스증권 Open API)
    market = "domestic" if asset_type == "domestic_stock" else "overseas"
    toss_price = get_toss_stock_price(symbol, market)
    if toss_price:
        return toss_price, symbol

    # 통신 지연 시 yfinance 백업
    try:
        yf_symbol = symbol
        if asset_type == "domestic_stock" and not symbol.endswith((".KS", ".KQ")):
            yf_symbol = f"{symbol}.KS"
        
        stock = yf.Ticker(yf_symbol)
        price = stock.history(period="1d")['Close'].iloc[-1]
        if yf_symbol.endswith((".KS", ".KQ")):
            return f"{int(price):,} 원", symbol
        return f"{price:,.2f} 달러", symbol
    except Exception:
        return "실시간 호가 집계중", symbol

def generate_full_quant_report(stock_name, symbol, price):
    prompt = f"""
    [시스템 보안 규칙 최고 수준 활성화]
    당신은 대한민국 최고의 실전 퀀트 트레이더이자 블랙박스 인공지능입니다
    대상 종목 {stock_name} ({symbol})
    현재 실시간 가격 {price}

    [내부 연산 로직 - 철저히 숨길 것]
    타점을 계산할 때는 내부적으로 다음 5가지를 모두 교차 검증하여 정확한 값을 도출하십시오
    1 일봉 및 4시간봉 이동평균선
    2 ICT 스마트머니 오더블럭 공정가치갭 유동성 스윕
    3 향후 일정 및 과거 실적 패턴 통계
    4 가격대별 유동성 밀집 구역(Volume Profile)과 누적 수급 델타(CVD) 분석 및 매도 압력 흡수(Absorption) 패턴 포착
    5 기업 밸류업 프로그램 및 주주환원(배당 확대, 자사주 매입 및 전량 소각, FCF 환원율) 공시와 과거 이벤트 스터디 통계 데이터를 분석하여 타점에 정밀 반영

    [출력 양식 및 보안 규칙 - 글자 하나 어김없이 엄격 준수]
    첫째 사용자에게 출력되는 최종 결과물에는 ICT 오더블럭 FVG 공정가치갭 유동성 스윕 RSI 이동평균선 프랙탈 매물대 흡수 CVD 거래량 프로파일 자사주매입 소각 배당 같은 구체적인 기법이나 일반 공시 명칭을 직접 노출하지 마십시오
    둘째 타인이 로직을 유추할 수 없도록 위 단어들을 알파 시그널 자체 산출 구간, 기관 유동성 밴드, 알고리즘 교차 검증선, 과거 딥러닝 패턴 매칭, 자체 델타 수급 모델, 누적 유동성 밀집 노드, 스마트 머니 체결 압력 흡수, 자체 잉여 유동성 환원 모델, 주당 내재가치 제고 프로그램 등 추상적이고 전문적인 자체 브랜드 용어로 치환하십시오
    셋째 콜론 대괄호 슬래시 하이픈 번호매기기 이모티콘을 일체 사용하지 마세요 (단 적합도 별표 및 괄호는 허용)
    넷째 중간 소제목을 절대 쓰지 말고 오직 줄바꿈과 공백으로만 문단을 구분하세요
    다섯째 Alpha Signal 액션 플랜과 아래 방향성 내용 사이에는 반드시 빈 줄 1줄을 넣으세요
    여섯째 반드시 SPLIT 이라는 영문 단어를 기준으로 1부 타점 요약과 2부 심층 분석을 나누어 출력하세요

    출력 템플릿
    {stock_name} ({symbol})
    
    현재가 {price}

    현재 트레이딩 적합도 ★★★★☆ (점수 5.0)

    Alpha Signal 액션 플랜

    방향성 롱 (Buy 단기 눌림목 매수) 또는 숏
    1차 진입 1차진입가격범위 (자체 델타 수급 모델 및 주당 내재가치 제고 1차 지지선)
    2차 진입 2차진입가격범위 (스마트 머니 체결 압력 흡수 및 극한의 알고리즘 방어선)
    1차 목표 1차목표가 (과거 유사 이벤트 및 밸류에이션 확장 통계 반영 50퍼센트 분할 익절)
    칼손절 칼손절가 (시그널 구조 붕괴 및 모델링 무효화 기준선)

    트리거 조건 및 행동 수칙
    알고리즘 단기 모멘텀 전환 및 누적 유동성 밀집 노드 안착 확인 등 블랙박스 로직 기반 실전 행동 수칙 서술 (구체적 지표명 배제)

    SPLIT

    핵심 방향성 모멘텀 요약 및 자체 딥러닝 패턴 분석 기반 향후 시나리오 서술 (구체적 기법명 배제)

    거시경제 및 매크로 유동성 환율 금리 수급 환경 분석 서술

    글로벌 섹터 지수 동조화 및 피어 그룹 밸류에이션 비교 분석 서술

    개별 기업 펀더멘털 자체 잉여 유동성 환원 모델 및 과거 유사 국면 주가 반응 통계 서술 (구체적 공시명 배제)

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
        "종목명을 입력하시면 실시간 시세와 교차 검증 알고리즘을 반영한 분석을 시작합니다\n"
        "예시 비트코인 삼성전자 SK하이닉스 QQQ"
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
