import os
import re
import io
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
    "솔트룩스": ("domestic_stock", "304100"),
    "알테오젠": ("domestic_stock", "196170"),
    "에코프로": ("domestic_stock", "086520"),
    "테슬라": ("overseas_stock", "TSLA"),
    "엔비디아": ("overseas_stock", "NVDA"),
    "애플": ("overseas_stock", "AAPL"),
    "QQQ": ("overseas_stock", "QQQ")
}

def get_stock_code_by_name(query):
    """네이버 / 다음 실시간 검색으로 6자리 종목코드 추출"""
    clean_q = query.strip()
    
    try:
        url = f"https://ac.finance.naver.com/ac?q={requests.utils.quote(clean_q)}&target=stock"
        headers = {"User-Agent": "Mozilla/5.0"}
        res = requests.get(url, headers=headers, timeout=2).json()
        items = res.get("items", [[]])[0]
        if items:
            q_comp = clean_q.replace(" ", "").upper()
            for item in items:
                name_comp = item[1].replace(" ", "").upper()
                if q_comp == name_comp:
                    return item[0], item[1]
            return items[0][0], items[0][1]
    except Exception:
        pass

    try:
        d_url = f"https://suggest-baro.daum.net/suggest?id=stock&v=4&code=utf-8&q={requests.utils.quote(clean_q)}"
        headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://finance.daum.net"}
        res = requests.get(d_url, headers=headers, timeout=2).json()
        sub_items = res.get("sub", [])
        if sub_items:
            raw_code = sub_items[0][0].replace("A", "")
            name = sub_items[0][1]
            return raw_code, name
    except Exception:
        pass

    return None, clean_q

def get_naver_realtime_price(code):
    """네이버 모바일 증권에서 실시간 가격 추출"""
    try:
        url = f"https://m.stock.naver.com/api/stock/{code}/basic"
        headers = {"User-Agent": "Mozilla/5.0"}
        res = requests.get(url, headers=headers, timeout=2).json()
        p = str(res.get("closePrice", "")).replace(",", "")
        if p and p != "None":
            return f"{int(p):,} 원"
    except Exception:
        pass
    return None

def fetch_naver_stock_candles(code):
    """네이버 증권 일봉 캔들 API (국내 전종목 100% 지원)"""
    try:
        url = f"https://fchart.stock.naver.com/sise.nhn?symbol={code}&timeframe=day&count=60&requestType=0"
        headers = {"User-Agent": "Mozilla/5.0"}
        res = requests.get(url, headers=headers, timeout=3)
        matches = re.findall(r'<item data="([^"]+)"', res.text)
        
        records = []
        for m in matches:
            parts = m.split("|")
            if len(parts) >= 6:
                date_str, o, h, l, c, v = parts[0], parts[1], parts[2], parts[3], parts[4], parts[5]
                records.append({
                    "Date": pd.to_datetime(date_str, format="%Y%m%d"),
                    "Open": float(o),
                    "High": float(h),
                    "Low": float(l),
                    "Close": float(c),
                    "Volume": float(v)
                })
        if records:
            df = pd.DataFrame(records).set_index("Date")
            return df
    except Exception:
        pass
    return None

def generate_candlestick_chart(df, symbol_name):
    """트레이딩뷰 다크테마 캔들 차트 이미지 버퍼 생성 (텔레그램 전송 안정화)"""
    try:
        chart_df = df.tail(40).copy()
        chart_df.index = pd.to_datetime(chart_df.index)
        
        # 다크 테마 컬러 설정
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
        buf.name = "chart.png"  # 텔레그램 전송용 가상 파일명 부여
        return buf
    except Exception as e:
        print(f"Chart Render Error: {e}")
        return None

def fetch_orderbook_and_sentiment(symbol, asset_type):
    sentiment_data = {
        "orderbook_ratio": "중립 델타",
        "retail_sentiment": "정상 범위",
        "action_guide": "정석 분할 진입 유효"
    }
    if asset_type == "crypto":
        try:
            url = f"https://api.binance.com/api/v3/depth?symbol={symbol}&limit=10"
            ob = requests.get(url, timeout=2).json()
            bids = sum([float(x[1]) for x in ob.get("bids", [])])
            asks = sum([float(x[1]) for x in ob.get("asks", [])])
            if asks > 0:
                ratio = bids / asks
                if ratio > 1.8:
                    sentiment_data["orderbook_ratio"] = f"매수잔량 우세 ({ratio:.2f}배)"
                elif ratio < 0.6:
                    sentiment_data["orderbook_ratio"] = f"매도잔량 우세 ({1/ratio:.2f}배)"
                else:
                    sentiment_data["orderbook_ratio"] = f"균형 오더북 ({ratio:.2f})"
        except Exception:
            pass
    elif asset_type == "domestic_stock":
        try:
            url = f"https://m.stock.naver.com/api/stock/{symbol}/market"
            headers = {"User-Agent": "Mozilla/5.0"}
            res = requests.get(url, headers=headers, timeout=2).json()
            total_ask = int(res.get("totalAskQty", 0))
            total_bid = int(res.get("totalBidQty", 0))
            if total_ask > 0 and total_bid > 0:
                imbalance = total_ask / total_bid
                if imbalance >= 2.0:
                    sentiment_data["orderbook_ratio"] = f"상승형 매도벽 흡수 패턴 (매도/매수 {imbalance:.1f}배)"
                    sentiment_data["retail_sentiment"] = "스마트 머니 체결 가동"
                elif imbalance <= 0.5:
                    sentiment_data["orderbook_ratio"] = f"허매수벽 트랩 주의 (매수/매도 {1/imbalance:.1f}배)"
                    sentiment_data["retail_sentiment"] = "개미 매수 쏠림"
                    sentiment_data["action_guide"] = "진입가 보수적 하향"
        except Exception:
            pass
    return sentiment_data

def fetch_realtime_news(symbol, asset_type):
    try:
        if asset_type == "domestic_stock":
            url = f"https://m.stock.naver.com/api/news/stock/{symbol}?page=1&pageSize=3"
            headers = {"User-Agent": "Mozilla/5.0"}
            res = requests.get(url, headers=headers, timeout=2).json()
            titles = [re.sub(r'<[^>]+>', '', item.get("tit", "")) for item in res if item.get("tit")]
            if titles:
                return " / ".join(titles)
    except Exception:
        pass
    return "현재 시장 전반의 수급 모멘텀에 의해 등락 중"

def fetch_technical_quant_data(symbol, asset_type):
    quant_info = {}
    curr_price_str = None
    df = None

    try:
        if asset_type == "crypto":
            url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=1d&limit=60"
            data = requests.get(url, timeout=3).json()
            df = pd.DataFrame(data, columns=['time', 'open', 'high', 'low', 'close', 'vol', 'close_time', 'qav', 'num_trades', 'taker_base_vol', 'taker_quote_vol', 'ignore'])
            df['close'] = df['close'].astype(float)
            df['high'] = df['high'].astype(float)
            df['low'] = df['low'].astype(float)
            df['open'] = df['open'].astype(float)
            df['vol'] = df['vol'].astype(float)
            df.index = pd.to_datetime(df['time'], unit='ms')
            curr_p = float(df['close'].iloc[-1])
            curr_price_str = f"{curr_p:,.2f} 달러"

        elif asset_type == "domestic_stock":
            curr_price_str = get_naver_realtime_price(symbol)
            df = fetch_naver_stock_candles(symbol)
            if df is None or len(df) < 10:
                try:
                    df = yf.Ticker(f"{symbol}.KQ").history(period="60d")
                    if df.empty:
                        df = yf.Ticker(f"{symbol}.KS").history(period="60d")
                except Exception:
                    pass
            if not curr_price_str and df is not None and not df.empty:
                c_val = float(df['Close'].iloc[-1])
                curr_price_str = f"{int(c_val):,} 원"

        else:
            stock = yf.Ticker(symbol)
            df = stock.history(period="60d")
            if not df.empty:
                curr_p = float(df['Close'].iloc[-1])
                curr_price_str = f"{curr_p:,.2f} 달러"

        if not curr_price_str:
            return None

        quant_info['current_price'] = curr_price_str
        quant_info['sentiment'] = fetch_orderbook_and_sentiment(symbol, asset_type)
        quant_info['earnings_status'] = "정상 범위"

        if df is not None and len(df) >= 10:
            col_map = {c: c.lower() for c in df.columns}
            df_calc = df.rename(columns=col_map)

            delta = df_calc['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / (loss + 1e-9)
            rsi = 100 - (100 / (1 + rs))
            quant_info['rsi'] = round(float(rsi.iloc[-1]), 1) if not pd.isna(rsi.iloc[-1]) else 50.0
            quant_info['ma20'] = round(float(df_calc['close'].rolling(20).mean().iloc[-1]), 2) if len(df_calc) >= 20 else "산출중"
            quant_info['ma60'] = round(float(df_calc['close'].rolling(60).mean().iloc[-1]), 2) if len(df_calc) >= 60 else "산출중"
            quant_info['high20'] = round(float(df_calc['high'].tail(20).max()), 2)
            quant_info['low20'] = round(float(df_calc['low'].tail(20).min()), 2)
            
            high_low = df_calc['high'] - df_calc['low']
            quant_info['atr'] = round(float(high_low.tail(14).mean()), 2)
            
            v_col = 'vol' if 'vol' in df_calc.columns else 'volume'
            avg_vol5 = df_calc[v_col].iloc[-6:-1].mean() if len(df_calc) >= 6 else df_calc[v_col].mean()
            curr_vol = df_calc[v_col].iloc[-1]
            vol_ratio = (curr_vol / (avg_vol5 + 1e-9)) * 100
            quant_info['vol_status'] = f"5일 평균 대비 {vol_ratio:.0f}%"

            standard_df = df_calc[['open', 'high', 'low', 'close', v_col]].rename(columns={
                'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', v_col: 'Volume'
            })
            quant_info['chart_df'] = standard_df
        else:
            quant_info['chart_df'] = None
            quant_info['rsi'] = 50.0
            quant_info['ma20'] = "산출중"
            quant_info['ma60'] = "산출중"
            quant_info['high20'] = "산출중"
            quant_info['low20'] = "산출중"
            quant_info['atr'] = "산출중"
            quant_info['vol_status'] = "정상"

        return quant_info

    except Exception:
        if curr_price_str:
            return {
                "current_price": curr_price_str,
                "sentiment": fetch_orderbook_and_sentiment(symbol, asset_type),
                "earnings_status": "정상",
                "vol_status": "정상",
                "chart_df": None,
                "rsi": 50.0
            }
    return None

def fetch_realtime_price_fallback(user_query):
    cleaned_query = user_query.strip().replace(" ", "").upper()
    display_name = user_query.strip()
    
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
        elif any(c.isupper() for c in cleaned_query) and not any('\uac00' <= char <= '\ud7a3' for char in cleaned_query):
            asset_type, symbol = "overseas_stock", cleaned_query
        else:
            if not (cleaned_query.isdigit() and len(cleaned_query) == 6):
                searched_code, formal_name = get_stock_code_by_name(user_query.strip())
                symbol = searched_code if searched_code else cleaned_query
                display_name = formal_name if searched_code else user_query.strip()
            else:
                symbol = cleaned_query
            asset_type = "domestic_stock"

    news_brief = fetch_realtime_news(symbol, asset_type)
    quant_data = fetch_technical_quant_data(symbol, asset_type)
    
    if quant_data and quant_data.get('current_price'):
        quant_data['news'] = news_brief
        return quant_data['current_price'], symbol, display_name, quant_data

    return None, symbol, display_name, None

def generate_full_quant_report(stock_name, symbol, price, quant_data):
    tech_context = ""
    if quant_data:
        sent = quant_data.get('sentiment', {})
        tech_context = f"""
        [실시간 연산된 정밀 퀀트 팩터]
        - 실제 실시간 현재가: {price}
        - 계산된 RSI(14): {quant_data.get('rsi', '산출중')}
        - 20일선: {quant_data.get('ma20', '산출중')} / 60일선: {quant_data.get('ma60', '산출중')}
        - 20일 매물벽 상단: {quant_data.get('high20', '산출중')} / 하단 바닥: {quant_data.get('low20', '산출중')}
        - 일일 변동폭(ATR): {quant_data.get('atr', '산출중')}
        - 거래량 폭발 모멘텀: {quant_data.get('vol_status', '보통')}
        - 장중 뉴스 재료: {quant_data.get('news', '시장 수급 모멘텀')}
        - 10호가 오더북 체결 델타: {sent.get('orderbook_ratio', '중립')}
        - 개미 군중 심리 포지션: {sent.get('retail_sentiment', '정상')}
        """

    prompt = f"""
    [시스템 보안 규칙 최고 수준 활성화]
    당신은 대한민국 최고의 실전 퀀트 트레이더이자 블랙박스 인공지능입니다.
    반드시 제공된 실제 실시간 가격({price})과 정밀 팩터를 기반으로만 액션 플랜을 산출하십시오.
    
    대상 종목: {stock_name} ({symbol})
    {tech_context}

    [출력 양식 및 보안 규칙]
    첫째, 사용자에게 출력되는 결과물에는 RSI, 이동평균선, ATR, HVN, FVG, 오더북 같은 구체적인 지표 명칭을 절대 노출하지 마십시오.
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
        "종목명을 입력하시면 실시간 차트 렌더링 및 퀀트 액션 플랜을 함께 전송합니다\n"
        "예시 솔트룩스 알테오젠 SK스퀘어 삼성전자 비트코인 TSLA"
    )

async def analyze(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.message.text
    chat_id = update.effective_chat.id
    status_msg = await update.message.reply_text(f"{query} 실시간 차트 렌더링 및 퀀트 연산 중입니다")

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

        # 1. 차트 이미지 생성
        chart_buffer = None
        if quant_data and quant_data.get('chart_df') is not None and not quant_data['chart_df'].empty:
            chart_buffer = generate_candlestick_chart(quant_data['chart_df'], formal_name)

        # 2. 로딩 메시지 삭제
        await status_msg.delete()

        # 3. 차트 사진 전송
        if chart_buffer is not None:
            try:
                await context.bot.send_photo(
                    chat_id=chat_id,
                    photo=chart_buffer.getvalue(),
                    filename="chart.png"
                )
            except Exception as pe:
                print(f"Photo send error: {pe}")

        # 4. 타점 요약 카드 전송
        await context.bot.send_message(
            chat_id=chat_id,
            text=summary_text,
            reply_markup=reply_markup
        )

    except Exception as e:
        err_msg = str(e)
        if "429" in err_msg or "quota" in err_msg.lower():
            await update.message.reply_text("일일 API 할당량이 소진되었습니다 잠시 후 다시 시도해 주세요")
        else:
            await update.message.reply_text(f"분석 오류 {err_msg}")

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
        keyboard = [[InlineKeyboardButton("심층 분석 근거 보기", callback_data=f"show_detail:{report_id}")]]
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
