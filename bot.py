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
    "SK스퀘어": ("domestic_stock", "402340"),
    "테슬라": ("overseas_stock", "TSLA"),
    "엔비디아": ("overseas_stock", "NVDA"),
    "애플": ("overseas_stock", "AAPL"),
    "QQQ": ("overseas_stock", "QQQ")
}

def get_stock_code_by_name(query):
    """한글/영문 혼용 종목명을 네이버 증권 검색 API를 통해 6자리 코드로 완벽 변환"""
    try:
        # 네이버 자동완성 API
        url = f"https://ac.finance.naver.com/ac?q={requests.utils.quote(query)}&target=stock"
        headers = {"User-Agent": "Mozilla/5.0"}
        res = requests.get(url, headers=headers, timeout=3).json()
        items = res.get("items", [[]])[0]
        if items:
            q_clean = query.replace(" ", "").upper()
            for item in items:
                name_clean = item[1].replace(" ", "").upper()
                if q_clean == name_clean or q_clean in name_clean:
                    return item[0], item[1]
            return items[0][0], items[0][1]
    except Exception:
        pass
    return None, query

def get_naver_realtime_price(code):
    """네이버 모바일 증권에서 실시간 현재가 즉시 추출"""
    try:
        url = f"https://m.stock.naver.com/api/stock/{code}/basic"
        headers = {"User-Agent": "Mozilla/5.0"}
        res = requests.get(url, headers=headers, timeout=3).json()
        p = res.get("closePrice", "").replace(",", "")
        if p:
            return f"{int(p):,} 원"
    except Exception:
        pass
    return None

def fetch_orderbook_and_sentiment(symbol, asset_type):
    """10호가 잔량 델타 및 시장 심리 분석"""
    sentiment_data = {
        "orderbook_ratio": "중립 델타",
        "retail_sentiment": "정상 범위",
        "action_guide": "정석 분할 진입 유효"
    }
    
    if asset_type == "crypto":
        try:
            url = f"https://api.binance.com/api/v3/depth?symbol={symbol}&limit=10"
            ob = requests.get(url, timeout=3).json()
            bids = sum([float(x[1]) for x in ob.get("bids", [])])
            asks = sum([float(x[1]) for x in ob.get("asks", [])])
            if asks > 0:
                ratio = bids / asks
                if ratio > 1.8:
                    sentiment_data["orderbook_ratio"] = f"매수잔량 우세 ({ratio:.2f}배, 단기 지지 탄탄)"
                elif ratio < 0.6:
                    sentiment_data["orderbook_ratio"] = f"매도잔량 우세 ({1/ratio:.2f}배, 상단 매도벽 두터움)"
                else:
                    sentiment_data["orderbook_ratio"] = f"균형 오더북 (체결 델타 {ratio:.2f})"
        except Exception:
            pass

    elif asset_type == "domestic_stock":
        try:
            url = f"https://m.stock.naver.com/api/stock/{symbol}/market"
            headers = {"User-Agent": "Mozilla/5.0"}
            res = requests.get(url, headers=headers, timeout=3).json()
            total_ask = int(res.get("totalAskQty", 0))
            total_bid = int(res.get("totalBidQty", 0))
            if total_ask > 0 and total_bid > 0:
                imbalance = total_ask / total_bid
                if imbalance >= 2.0:
                    sentiment_data["orderbook_ratio"] = f"상승형 매도벽 흡수 패턴 (매도잔량/매수잔량 {imbalance:.1f}배 - 세력 돌파형 호가)"
                    sentiment_data["retail_sentiment"] = "스마트 머니 체결 흡수 가동"
                elif imbalance <= 0.5:
                    sentiment_data["orderbook_ratio"] = f"허매수벽 트랩 주의 (매수잔량/매도잔량 {1/imbalance:.1f}배 - 지지선 이탈 리스크)"
                    sentiment_data["retail_sentiment"] = "개미 매수 쏠림 과열"
                    sentiment_data["action_guide"] = "허매수 트랩 회피를 위해 1차 진입가 보수적 하향"
        except Exception:
            pass

    return sentiment_data

def fetch_realtime_news(query, symbol, asset_type):
    """실시간 급등/급락 핵심 재료 및 뉴스 수집"""
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
    """실적 발표일 추적"""
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

def calculate_relative_strength(df, asset_type):
    """시장 지수 대비 상대 강도(RS) 계산"""
    try:
        bench_symbol = "^KS11" if asset_type == "domestic_stock" else ("^IXIC" if asset_type == "overseas_stock" else "BTC-USD")
        bench_df = yf.Ticker(bench_symbol).history(period="20d")
        if not bench_df.empty and len(bench_df) >= 5 and len(df) >= 5:
            stock_ret = (df['close'].iloc[-1] - df['close'].iloc[-5]) / df['close'].iloc[-5] * 100
            bench_ret = (bench_df['Close'].iloc[-1] - bench_df['Close'].iloc[-5]) / bench_df['Close'].iloc[-5] * 100
            rs_score = stock_ret - bench_ret
            if rs_score > 3.0:
                return f"🔥 시장 대비 압도적 주도주 (RS +{rs_score:.1f}% 초과 탄력)"
            elif rs_score < -3.0:
                return f"❄️ 시장 대비 약세 소외주 (RS {rs_score:.1f}% 부진)"
            return f"시장 동조화 흐름 (RS {rs_score:+.1f}%)"
    except Exception:
        pass
    return "상대 강도 중립"

def fetch_technical_quant_data(symbol, asset_type):
    """RSI, 이평선, ATR, 거래량, RS 지표 정밀 계산"""
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
            df['vol'] = df['vol'].astype(float)
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
            
            # 네이버 실시간 호가로 현재가 보정
            naver_p = get_naver_realtime_price(symbol)
            if naver_p:
                curr_price_str = naver_p
            else:
                curr_p = float(df['Close'].iloc[-1])
                curr_price_str = f"{int(curr_p):,} 원" if asset_type == "domestic_stock" else f"{curr_p:,.2f} 달러"

            currency = "원" if asset_type == "domestic_stock" else "달러"
            df = df.rename(columns={'Close': 'close', 'High': 'high', 'Low': 'low', 'Volume': 'vol'})

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
            
            avg_vol5 = df['vol'].iloc[-6:-1].mean() if len(df) >= 6 else df['vol'].mean()
            curr_vol = df['vol'].iloc[-1]
            vol_ratio = (curr_vol / (avg_vol5 + 1e-9)) * 100
            quant_info['vol_status'] = f"5일 평균 대비 {vol_ratio:.0f}% 수준"

            quant_info['rs_signal'] = calculate_relative_strength(df, asset_type)
            quant_info['earnings_status'] = earnings_warning
            quant_info['current_price'] = curr_price_str if 'curr_price_str' in locals() else f"{curr_p:,.2f} {currency}"
            quant_info['sentiment'] = fetch_orderbook_and_sentiment(symbol, asset_type)
            return quant_info
    except Exception:
        pass
    return None

def fetch_realtime_price_fallback(user_query):
    cleaned_query = user_query.strip().replace(" ", "").upper()
    display_name = user_query.strip()
    
    # 1. 고정 딕셔너리 매핑 확인 (대소문자/공백 무시)
    matched_key = None
    for key in COMMON_SYMBOLS:
        if key.replace(" ", "").upper() == cleaned_query:
            matched_key = key
            break
            
    if matched_key:
        asset_type, symbol = COMMON_SYMBOLS[matched_key]
        display_name = matched_key
    else:
        if any(c in cleaned_query for c in ["코인", "USDT", "BTC", "ETH"]):
            asset_type, symbol = "crypto", cleaned_query
        elif any(c.isupper() for c in cleaned_query) and not cleaned_query.endswith(".KS") and not any('\uac00' <= char <= '\ud7a3' for char in cleaned_query):
            asset_type, symbol = "overseas_stock", cleaned_query
        else:
            # 6자리 숫자가 아니면 네이버 검색으로 6자리 종목코드 자동 추출
            if not (cleaned_query.isdigit() and len(cleaned_query) == 6):
                searched_code, formal_name = get_stock_code_by_name(user_query.strip())
                symbol = searched_code if searched_code else cleaned_query
                display_name = formal_name if searched_code else user_query.strip()
            else:
                symbol = cleaned_query
            asset_type = "domestic_stock"

    news_brief = fetch_realtime_news(user_query, symbol, asset_type)
    quant_data = fetch_technical_quant_data(symbol, asset_type)
    
    if quant_data and quant_data.get('current_price'):
        quant_data['news'] = news_brief
        return quant_data['current_price'], symbol, display_name, quant_data

    # 네이버 모바일 실시간 가격 2차 백업
    if asset_type == "domestic_stock":
        p = get_naver_realtime_price(symbol)
        if p:
            sent = fetch_orderbook_and_sentiment(symbol, asset_type)
            fallback_quant = {
                "news": news_brief,
                "earnings_status": "정상 범위",
                "sentiment": sent,
                "vol_status": "정상",
                "rs_signal": "시장 동조화"
            }
            return p, symbol, display_name, fallback_quant

    return None, symbol, display_name, None

def generate_full_quant_report(stock_name, symbol, price, quant_data):
    tech_context = ""
    if quant_data:
        sent = quant_data.get('sentiment', {})
        tech_context = f"""
        [실시간 연산된 정밀 퀀트 팩터 및 고도화 지표]
        - 실제 실시간 현재가: {price}
        - 계산된 RSI(14): {quant_data.get('rsi', '산출중')}
        - 20일선: {quant_data.get('ma20', '산출중')} / 60일선: {quant_data.get('ma60', '산출중')}
        - 20일 매물벽 상단: {quant_data.get('high20', '산출중')} / 하단 바닥: {quant_data.get('low20', '산출중')}
        - 일일 변동폭(ATR): {quant_data.get('atr', '산출중')}
        - 거래량 폭발 모멘텀: {quant_data.get('vol_status', '보통')}
        - 시장 대비 상대강도(RS): {quant_data.get('rs_signal', '중립')}
        - 실적 발표 일정: {quant_data.get('earnings_status', '정상')}
        - 포착된 당일 뉴스 재료: {quant_data.get('news', '시장 수급 모멘텀')}
        - 10호가 오더북 체결 델타: {sent.get('orderbook_ratio', '중립')}
        - 개미 군중 심리 포지션: {sent.get('retail_sentiment', '정상')}
        - 알고리즘 권고 행동: {sent.get('action_guide', '정석 대응')}
        """

    prompt = f"""
    [시스템 보안 규칙 최고 수준 활성화]
    당신은 대한민국 최고의 실전 퀀트 트레이더이자 블랙박스 인공지능입니다.
    반드시 제공된 실제 실시간 가격({price})과 상대강도(RS), 거래량 폭발 모멘텀, 오더북 델타를 기반으로만 액션 플랜을 산출하십시오.
    
    대상 종목: {stock_name} ({symbol})
    {tech_context}

    [출력 양식 및 보안 규칙]
    첫째, 사용자에게 출력되는 결과물에는 RSI, 이동평균선, ATR, HVN, FVG, 오더북, 상대강도 같은 구체적인 지표 명칭을 절대 노출하지 마십시오.
    둘째, 타인이 로직을 유추할 수 없도록 알파 시그널 자체 산출 구간, 기관 유동성 밴드, 알고리즘 교차 검증선, 자체 델타 수급 모델, 누적 유동성 밀집 노드, 스마트 머니 체결 압력 흡수, 시장 초과 모멘텀 주도 강도 등 전문적인 자체 브랜드 용어로 치환하십시오.
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

    글로벌 섹터 지수 동조화 및 시장 대비 초과 상대 강도 분석 서술 (구체적 지표명 배제)

    개별 기업 실적 모멘텀 어닝 일정 및 대량 거래량 유입 강도 분석 서술 (구체적 지표명 배제)

    자체 퀀트 모델 교차 검증 및 자체 델타 수급 분석을 통한 호가 잔량 체결 압력 흡수 과정 서술 (구체적 지표명 배제)

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
        "종목명을 입력하시면 실시간 시세, 오더북 델타, 상대 강도(RS), 거래량 모멘텀을 결합한 퀀트 분석을 시작합니다\n"
        "예시 sk스퀘어 삼성전자 비트코인 TSLA"
    )

async def analyze(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.message.text
    status_msg = await update.message.reply_text(f"{query} 실시간 호가 및 다차원 퀀트 팩터 연산 중입니다")

    try:
        current_price, symbol, formal_name, quant_data = fetch_realtime_price_fallback(query)

        if not current_price:
            await status_msg.edit_text(f"❌ {query}의 실시간 가격을 가져오지 못했습니다. 종목명을 다시 확인해 주세요.")
            return

        summary_text, detail_text = generate_full_quant_report(formal_name, symbol, current_price, quant_data)

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
