import os
import re
import io
import json
import uuid
import datetime
import threading
import urllib.parse
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

# 주요 종목 마스터 사전 (영문 대소문자 무관 탐색)
COMMON_SYMBOLS = {
    "BTC": ("crypto", "BTCUSDT"),
    "ETH": ("crypto", "ETHUSDT"),
    "XRP": ("crypto", "XRPUSDT"),
    "DOGE": ("crypto", "DOGEUSDT"),
    "비트코인": ("crypto", "BTCUSDT"),
    "이더리움": ("crypto", "ETHUSDT"),
    "리플": ("crypto", "XRPUSDT"),
    "도지코인": ("crypto", "DOGEUSDT"),
    "삼성전자": ("domestic_stock", "005930"),
    "삼성전기": ("domestic_stock", "009150"),
    "SK하이닉스": ("domestic_stock", "000660"),
    "SK스퀘어": ("domestic_stock", "402340"),
    "SK증권": ("domestic_stock", "001510"),
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
    "포스코홀딩스": ("domestic_stock", "005490"),
    "POSCO홀딩스": ("domestic_stock", "005490"),
    "LG에너지솔루션": ("domestic_stock", "373220"),
    "테슬라": ("overseas_stock", "TSLA"),
    "TSLA": ("overseas_stock", "TSLA"),
    "엔비디아": ("overseas_stock", "NVDA"),
    "NVDA": ("overseas_stock", "NVDA"),
    "애플": ("overseas_stock", "AAPL"),
    "AAPL": ("overseas_stock", "AAPL"),
    "마이크로소프트": ("overseas_stock", "MSFT"),
    "MSFT": ("overseas_stock", "MSFT"),
    "QQQ": ("overseas_stock", "QQQ")
}

def get_stock_code_by_name(query):
    """대소문자/특수문자 무관 100% 종목코드 추출기"""
    clean_q = query.strip()
    target_comp = clean_q.replace(" ", "").upper()

    # 1. 마스터 사전 매칭 (대소문자 무시)
    for k, v in COMMON_SYMBOLS.items():
        if k.replace(" ", "").upper() == target_comp:
            return v[1], k

    # 2. 다음 금융 검색 (코스피/코스닥 대소문자 무관 매칭)
    try:
        d_url = f"https://suggest-baro.daum.net/suggest?id=stock&v=4&code=utf-8&q={requests.utils.quote(clean_q)}"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)", "Referer": "https://finance.daum.net"}
        res = requests.get(d_url, headers=headers, timeout=2).json()
        sub_items = res.get("sub", [])
        if sub_items:
            for item in sub_items:
                raw_code = str(item[0]).replace("A", "").strip()
                name = str(item[1]).strip()
                if name.replace(" ", "").upper() == target_comp:
                    return raw_code, name
            # 정확 일치가 없어도 1순위 반환
            return str(sub_items[0][0]).replace("A", "").strip(), str(sub_items[0][1]).strip()
    except Exception:
        pass

    # 3. 네이버 모바일 통합 검색
    try:
        url = f"https://m.stock.naver.com/api/search/autoComplete?keyword={requests.utils.quote(clean_q)}"
        headers = {"User-Agent": "Mozilla/5.0"}
        res = requests.get(url, headers=headers, timeout=2).json()
        for key in ["stocks", "items"]:
            if key in res and isinstance(res[key], list):
                for item in res[key]:
                    c = item.get("itemCode") or item.get("code")
                    n = item.get("stockName") or item.get("name")
                    if c and n:
                        if str(n).replace(" ", "").upper() == target_comp:
                            return str(c).strip(), str(n).strip()
                if res[key]:
                    first = res[key][0]
                    c = first.get("itemCode") or first.get("code")
                    n = first.get("stockName") or first.get("name")
                    if c and n:
                        return str(c).strip(), str(n).strip()
    except Exception:
        pass

    # 4. 네이버 PC 검색
    try:
        url = f"https://ac.finance.naver.com/ac?q={requests.utils.quote(clean_q)}&target=stock"
        headers = {"User-Agent": "Mozilla/5.0"}
        res = requests.get(url, headers=headers, timeout=2).json()
        items_list = res.get("items", [])
        if items_list and isinstance(items_list[0], list) and len(items_list[0]) > 0:
            for item in items_list[0]:
                c = str(item[0]).strip()
                n = str(item[1]).strip()
                if n.replace(" ", "").upper() == target_comp:
                    return c, n
            return str(items_list[0][0][0]).strip(), str(items_list[0][0][1]).strip()
    except Exception:
        pass

    return None, clean_q

def get_naver_realtime_price(code):
    """실시간 현재가 추출 (모바일 API + 실시간 폴링)"""
    headers = {"User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X)"}
    try:
        url = f"https://m.stock.naver.com/api/stock/{code}/basic"
        res = requests.get(url, headers=headers, timeout=2).json()
        p = str(res.get("closePrice", "")).replace(",", "").strip()
        if p and p.isdigit():
            return f"{int(p):,} 원"
    except Exception:
        pass

    try:
        url = f"https://polling.finance.naver.com/api/realtime/domestic/stock/{code}"
        res = requests.get(url, headers=headers, timeout=2).json()
        datas = res.get("result", {}).get("areas", [{}])[0].get("datas", [])
        if datas:
            nv = datas[0].get("nv")
            if nv:
                return f"{int(nv):,} 원"
    except Exception:
        pass
    return None

def fetch_domestic_stock_candles(code):
    """일봉 데이터 수집 (네이버 모바일 JSON + XML 2중 파싱)"""
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        url = f"https://api.stock.naver.com/chart/domestic/market/stock/{code}?periodType=dayCandle&range=60"
        res = requests.get(url, headers=headers, timeout=3).json()
        price_infos = res.get("priceInfos", [])
        if price_infos:
            records = []
            for item in price_infos:
                records.append({
                    "Date": pd.to_datetime(str(item["localDate"]), format="%Y%m%d"),
                    "Open": float(item["openPrice"]),
                    "High": float(item["highPrice"]),
                    "Low": float(item["lowPrice"]),
                    "Close": float(item["closePrice"]),
                    "Volume": float(item["accumulatedTradingVolume"])
                })
            if records:
                return pd.DataFrame(records).set_index("Date").sort_index()
    except Exception:
        pass

    try:
        url = f"https://fchart.stock.naver.com/sise.nhn?symbol={code}&timeframe=day&count=60&requestType=0"
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
            return pd.DataFrame(records).set_index("Date").sort_index()
    except Exception:
        pass

    return None

def generate_candlestick_chart(df, symbol_name):
    """트레이딩뷰 다크 테마 캔들 차트 생성"""
    try:
        if df is None or len(df) < 5:
            return None

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
        "orderbook_ratio": "중립",
        "retail_sentiment": "정상 범위",
        "action_guide": "분할 매수 유효"
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
                    sentiment_data["orderbook_ratio"] = f"매도벽 흡수 (매도/매수 {imbalance:.1f}배)"
                    sentiment_data["retail_sentiment"] = "세력 매수 유입"
                elif imbalance <= 0.5:
                    sentiment_data["orderbook_ratio"] = f"허매수벽 주의 (매수/매도 {1/imbalance:.1f}배)"
                    sentiment_data["retail_sentiment"] = "개미 쏠림"
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
    return "시장 전반의 수급 모멘텀에 의해 등락 중"

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
    
    # 1. 고정 마스터 매핑
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
            # 6자리 종목코드 자동 검색 (대소문자 완전 무시)
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

    # 실시간 시세 단독 백업
    if asset_type == "domestic_stock":
        p = get_naver_realtime_price(symbol)
        if p:
            sent = fetch_orderbook_and_sentiment(symbol, asset_type)
            fallback_quant = {
                "current_price": p,
                "news": news_brief,
                "earnings_status": "정상",
                "sentiment": sent,
                "vol_status": "정상",
                "chart_df": None,
                "rsi": 50.0
            }
            return p, symbol, display_name, fallback_quant

    return None, symbol, display_name, None

def generate_full_quant_report(stock_name, symbol, price, quant_data):
    tech_context = ""
    if quant_data:
        sent = quant_data.get('sentiment', {})
        tech_context = f"""
        [실시간 연산된 정밀 팩터]
        - 실제 실시간 현재가: {price}
        - 계산된 RSI: {quant_data.get('rsi', '50.0')}
        - 20일선: {quant_data.get('ma20', '산출중')} / 60일선: {quant_data.get('ma60', '산출중')}
        - 20일 고점: {quant_data.get('high20', '산출중')} / 20일 저점: {quant_data.get('low20', '산출중')}
        - 일일 변동폭(ATR): {quant_data.get('atr', '산출중')}
        - 거래량 흐름: {quant_data.get('vol_status', '보통')}
        - 장중 뉴스 재료: {quant_data.get('news', '시장 수급 모멘텀')}
        - 호가창 수급 델타: {sent.get('orderbook_ratio', '중립')}
        - 개미 매수세: {sent.get('retail_sentiment', '정상')}
        """

    prompt = f"""
    당신은 여의도 최고의 프랍트레이딩 데스크 수석 퀀트 트레이더입니다.
    두루뭉술하거나 뻔한 말(예: '세력이 받아먹는다', '수급이 좋다') 같은 영혼 없는 템플릿형 문장은 엄격히 금지합니다.
    제공된 실제 팩터 데이터와 해당 기업의 실제 최근 이슈/업황을 직접 결합하여, 이 종목만의 독보적이고 날카로운 실전 매매 전략을 작성하세요.

    대상 종목: {stock_name} ({symbol})
    {tech_context}

    [작성 보안 및 양식 규칙]
    1. 콜론, 대괄호, 슬래시, 하이픈, 번호매기기, 이모티콘을 일체 사용하지 마세요 (단, 별표 및 괄호는 허용).
    2. 소제목 없이 오직 줄바꿈과 공백으로만 문단을 나누세요.
    3. 타점 수치는 제공된 실제 현재가({price})와 20일 고점/저점, 변동폭 데이터를 계산하여 현실적이고 정밀하게 제시하세요.
    4. 반드시 SPLIT 이라는 영문 단어를 기준으로 1부 타점 카드와 2부 심층 실전 해설을 나누어 출력하세요.

    출력 템플릿
    {stock_name} ({symbol})
    
    현재가 {price}

    현재 트레이딩 적합도 ★★★★☆ (점수 5.0)

    Alpha Signal 액션 플랜

    방향성 롱 (단기 눌림목 매수) 또는 숏
    1차 진입 1차진입가격범위 (단기 매물 소화 및 지지 확인 구간 1차 분할)
    2차 진입 2차진입가격범위 (주요 구조적 지지대 및 하단 반등 마지노선 2차 분할)
    1차 목표 1차목표가 (직전 단기 매물벽 도달 시 50퍼센트 분할 익절)
    2차 목표 2차목표가 (상단 저항 밴드 돌파 시 최종 전량 익절)
    칼손절 칼손절가 (지지선 붕괴 및 시나리오 무효화 기준가)

    트리거 조건 및 행동 요령
    당일 호가창 체결 강도와 장중 눌림 확인 등 구체적인 실전 진입 타이밍 서술

    SPLIT

    이 종목의 오늘 등락을 만든 실제 핵심 재료와 테마 모멘텀을 팩트 위주로 구체적 서술

    환율 금리 시장 매크로 환경이 이 종목의 실적 및 주가에 미치는 구체적 영향 분석

    동일 섹터 내 타 종목 대비 수급 강도와 주도주 여부를 시장 상대강도 기반으로 날카롭게 서술

    당일 거래량 급증 수준과 호가창 매수 매도 잔량 비율을 바탕으로 한 실제 손바뀜 및 매물 흡수 상태 서술

    목표가 도달 시 예상 수익률과 칼손절가 기준 감수할 리스크의 손익비 분석 및 최종 매매 코멘트
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
        "예시 sk증권 솔트룩스 알테오젠 에코프로 삼성전자 비트코인 TSLA"
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

        keyboard = [[InlineKeyboardButton("심층 분석 및 근거 보기", callback_data=f"show_detail:{report_id}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        # 차트 이미지 생성 (독립 try-except로 안정성 100% 보장)
        chart_buffer = None
        if quant_data and quant_data.get('chart_df') is not None and not quant_data['chart_df'].empty:
            try:
                chart_buffer = generate_candlestick_chart(quant_data['chart_df'], formal_name)
            except Exception:
                chart_buffer = None

        # 로딩 메시지 삭제
        try:
            await status_msg.delete()
        except Exception:
            pass

        # 1. 차트 이미지 전송
        if chart_buffer is not None:
            try:
                await context.bot.send_photo(
                    chat_id=chat_id,
                    photo=chart_buffer.getvalue()
                )
            except Exception as pe:
                print(f"Photo send error: {pe}")

        # 2. 타점 요약 카드 전송 (무조건 보장)
        await context.bot.send_message(
            chat_id=chat_id,
            text=summary_text,
            reply_markup=reply_markup
        )

    except Exception as e:
        err_msg = str(e)
        if "429" in err_msg or "quota" in err_msg.lower():
            await context.bot.send_message(chat_id=chat_id, text="일일 API 할당량이 소진되었습니다 잠시 후 다시 시도해 주세요")
        else:
            await context.bot.send_message(chat_id=chat_id, text=f"분석 처리 중 일시적 지연이 발생했습니다. 다시 입력해 주세요.")

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
        keyboard = [[InlineKeyboardButton("심층 분석 및 근거 보기", callback_data=f"show_detail:{report_id}")]]
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
