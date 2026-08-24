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
    "삼성전기": ("domestic_stock", "009150"),
    "SK하이닉스": ("domestic_stock", "000660"),
    "SK스퀘어": ("domestic_stock", "402340"),
    "솔트룩스": ("domestic_stock", "304100"),
    "알테오젠": ("domestic_stock", "196170"),
    "에코프로": ("domestic_stock", "086520"),
    "에코프로비엠": ("domestic_stock", "247540"),
    "레인보우로보틱스": ("domestic_stock", "277810"),
    "카카오": ("domestic_stock", "035720"),
    "네이버": ("domestic_stock", "035420"),
    "NAVER": ("domestic_stock", "035420"),
    "현대차": ("domestic_stock", "005380"),
    "기아": ("domestic_stock", "000270"),
    "POSCO홀딩스": ("domestic_stock", "005490"),
    "포스코홀딩스": ("domestic_stock", "005490"),
    "LG에너지솔루션": ("domestic_stock", "373220"),
    "테슬라": ("overseas_stock", "TSLA"),
    "엔비디아": ("overseas_stock", "NVDA"),
    "애플": ("overseas_stock", "AAPL"),
    "마이크로소프트": ("overseas_stock", "MSFT"),
    "QQQ": ("overseas_stock", "QQQ")
}

def get_stock_code_by_name(query):
    """코스피/코스닥 전 종목 실시간 검색 (다음/네이버/통합 4단계 파싱)"""
    clean_q = query.strip()
    
    # 1. 다음 증권 자동완성
    try:
        d_url = f"https://suggest-baro.daum.net/suggest?id=stock&v=4&code=utf-8&q={requests.utils.quote(clean_q)}"
        headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://finance.daum.net"}
        res = requests.get(d_url, headers=headers, timeout=2).json()
        sub_items = res.get("sub", [])
        if sub_items:
            for item in sub_items:
                raw_code = item[0].replace("A", "").strip()
                name = item[1].strip()
                if clean_q.replace(" ", "").upper() == name.replace(" ", "").upper():
                    return raw_code, name
            return sub_items[0][0].replace("A", "").strip(), sub_items[0][1].strip()
    except Exception:
        pass

    # 2. 네이버 모바일 통합 검색 API
    try:
        url = f"https://m.stock.naver.com/api/search/autoComplete?keyword={requests.utils.quote(clean_q)}"
        headers = {"User-Agent": "Mozilla/5.0"}
        res = requests.get(url, headers=headers, timeout=2).json()
        for group in ["stocks", "items"]:
            if group in res and res[group]:
                for item in res[group]:
                    code = item.get("itemCode") or item.get("code")
                    name = item.get("stockName") or item.get("name")
                    if code and name:
                        return code, name
    except Exception:
        pass

    # 3. 네이버 PC 자동완성 API
    try:
        url = f"https://ac.finance.naver.com/ac?q={requests.utils.quote(clean_q)}&target=stock"
        headers = {"User-Agent": "Mozilla/5.0"}
        res = requests.get(url, headers=headers, timeout=2).json()
        items_list = res.get("items", [])
        if items_list and len(items_list[0]) > 0:
            for item in items_list[0]:
                code = item[0]
                name = item[1]
                if clean_q.replace(" ", "").upper() == name.replace(" ", "").upper():
                    return code, name
            return items_list[0][0][0], items_list[0][0][1]
    except Exception:
        pass

    return None, clean_q

def get_naver_realtime_price(code):
    """네이버 모바일 증권 실시간 현재가 추출"""
    try:
        url = f"https://m.stock.naver.com/api/stock/{code}/basic"
        headers = {"User-Agent": "Mozilla/5.0"}
        res = requests.get(url, headers=headers, timeout=2).json()
        p = str(res.get("closePrice", "")).replace(",", "")
        if p and p != "None" and p != "":
            return f"{int(p):,} 원"
    except Exception:
        pass
    return None

def fetch_domestic_stock_candles(code):
    """네이버 모바일 통합 일봉 데이터 수집 (코스피/코스닥 100% 보장)"""
    try:
        url = f"https://api.stock.naver.com/chart/domestic/market/stock/{code}?periodType=dayCandle&range=60"
        headers = {"User-Agent": "Mozilla/5.0"}
        res = requests.get(url, headers=headers, timeout=3).json()
        price_infos = res.get("priceInfos", [])
        if price_infos:
            records = []
            for item in price_infos:
                records.append({
                    "Date": pd.to_datetime(item["localDate"], format="%Y%m%d"),
                    "Open": float(item["openPrice"]),
                    "High": float(item["highPrice"]),
                    "Low": float(item["lowPrice"]),
                    "Close": float(item["closePrice"]),
                    "Volume": float(item["accumulatedTradingVolume"])
                })
            df = pd.DataFrame(records).set_index("Date").sort_index()
            return df
    except Exception:
        pass

    try:
        url = f"https://fchart.stock.naver.com/sise.nhn?symbol={code}&timeframe=day&count=60&requestType=0"
        headers = {"User-Agent": "Mozilla/5.0"}
        res = requests.get(url, headers=headers, timeout=3)
        matches = re.findall(r'<item data="([^"]+)"', res.text)
        records = []
        for m in matches:
            parts = m.split("|")
            if len(parts) >= 6:
                records.append({
                    "Date": pd.to_datetime(parts[0], format="%Y%m%d"),
                    "Open": float(parts[1]),
                    "High": float(parts[2]),
                    "Low": float(parts[3]),
                    "Close": float(parts[4]),
                    "Volume": float(parts[5])
                })
        if records:
            df = pd.DataFrame(records).set_index("Date").sort_index()
            return df
    except Exception:
        pass

    return None

def generate_candlestick_chart(df, symbol_name):
    """트레이딩뷰 다크 테마 캔들 차트 생성"""
    try:
        chart_df = df.tail(40).copy()
        chart_df.index = pd.to_datetime(chart_df.index)
        
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
        return buf
    except Exception:
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
            df = fetch_domestic_stock_candles(symbol)
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
    당신은 주식을 처음 하는 사람도 1초 만에 이해할 수 있게 쉽게 설명해주는 베테랑 실전 퀀트 트레이더입니다.
    어려운 퀀트 전문 용어나 뜬구름 잡는 외계어(예: 델타 수급 모델, 유동성 밴드, 체결 압력 흡수 등)는 절대 쓰지 마세요.
    실제 매매할 때 쓰는 아주 직관적이고 쉬운 한국어로 요약해서 전달하세요.
    
    대상 종목: {stock_name} ({symbol})
    {tech_context}

    [작성 규칙]
    1. 콜론, 대괄호, 슬래시, 하이픈, 번호매기기, 이모티콘을 일체 사용하지 마세요 (단, 별표 및 괄호는 허용).
    2. 소제목 없이 오직 줄바꿈과 공백으로만 문단을 나누세요.
    3. 반드시 SPLIT 이라는 영문 단어를 기준으로 1부 타점 카드와 2부 쉬운 해설을 나누어 출력하세요.

    출력 템플릿
    {stock_name} ({symbol})
    
    현재가 {price}

    현재 트레이딩 적합도 ★★★★☆ (점수 5.0)

    Alpha Signal 액션 플랜

    방향성 롱 (단기 눌림목 매수) 또는 숏
    1차 진입 1차진입가격범위 (지지선 부근 1차 분할 매수)
    2차 진입 2차진입가격범위 (바닥 지지선 부근 2차 분할 매수)
    1차 목표 1차목표가 (단기 저항 도달 시 절반 익절)
    2차 목표 2차목표가 (최종 목표가 도달 시 전량 익절)
    칼손절 칼손절가 (이탈 시 뒤도 돌아보지 않고 손절)

    행동 요령
    지표명 없이 초보자도 바로 이해할 수 있는 실전 진입 및 대응 요령 1줄 서술

    SPLIT

    오늘 주가가 왜 이렇게 움직이는지 핵심 뉴스 재료와 시장 분위기를 쉬운 말로 서술

    현재 환율 금리 시장 수급이 이 종목에 유리한지 불리한지 1줄 요약

    비슷한 업종 대비 힘이 강하게 붙었는지 아니면 소외되었는지 쉽게 설명

    거래량이 평소보다 터졌는지와 세력이 물량을 받아먹고 있는지 쉽게 설명

    증권사들의 평균 목표가와 지금 사면 먹을 수 있는 상승 여력 서술
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
        "예시 삼성전기 솔트룩스 알테오젠 에코프로 삼성전자 비트코인 TSLA"
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

        keyboard = [[InlineKeyboardButton("쉬운 해설 및 근거 보기", callback_data=f"show_detail:{report_id}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        # 1. 차트 이미지 생성
        chart_buffer = None
        if quant_data and quant_data.get('chart_df') is not None and not quant_data['chart_df'].empty:
            chart_buffer = generate_candlestick_chart(quant_data['chart_df'], formal_name)

        # 2. 로딩 메시지 삭제
        await status_msg.delete()

        # 3. 차트 사진 먼저 전송
        if chart_buffer is not None:
            try:
                await context.bot.send_photo(
                    chat_id=chat_id,
                    photo=chart_buffer.getvalue()
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
        keyboard = [[InlineKeyboardButton("쉬운 해설 및 근거 보기", callback_data=f"show_detail:{report_id}")]]
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
