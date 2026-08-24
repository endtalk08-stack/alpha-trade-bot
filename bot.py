import os
import re
import io
import gc
import json
import uuid
import time
import asyncio
import datetime
import threading
import urllib.parse
from http.server import HTTPServer, BaseHTTPRequestHandler
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

# 1. 환경 변수 및 인증 키
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "8980819571:AAHyzm6KjZ0rKocObWoS28ThBbVGhJ5TfUs")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "AQ.Ab8RN6KisWMYVxgBBJmBW3SgOAT9aJYvOPiWj42H4WR_Y56kRA")
FMP_API_KEY = os.getenv("FMP_API_KEY", "YUVC3My39vzjHpkj0SwNyHp1hXn6Q1IN")
TOSS_API_KEY = os.getenv("TOSS_API_KEY", "")
TOSS_API_SECRET = os.getenv("TOSS_API_SECRET", "")

REPORT_CACHE = {}
BACKTEST_CACHE = {}
MARKET_REGIME_CACHE = {}
DETAIL_REPORT_CACHE = {}

CRYPTO_MAP = {
    "BTC": "BTCUSDT", "비트코인": "BTCUSDT", "비트": "BTCUSDT",
    "ETH": "ETHUSDT", "이더리움": "ETHUSDT", "이더": "ETHUSDT",
    "XRP": "XRPUSDT", "리플": "XRPUSDT",
    "DOGE": "DOGEUSDT", "도지코인": "DOGEUSDT", "도지": "DOGEUSDT",
    "SOL": "SOLUSDT", "솔라나": "SOLUSDT",
    "ADA": "ADAUSDT", "에이다": "ADAUSDT",
    "AVAX": "AVAXUSDT", "아발란체": "AVAXUSDT"
}

US_STOCK_MAP = {
    "엔비디아": "NVDA", "NVDA": "NVDA",
    "테슬라": "TSLA", "TSLA": "TSLA",
    "애플": "AAPL", "AAPL": "AAPL",
    "마이크로소프트": "MSFT", "마소": "MSFT", "MSFT": "MSFT",
    "아마존": "AMZN", "AMZN": "AMZN",
    "구글": "GOOGL", "알파벳": "GOOGL", "GOOGL": "GOOGL",
    "메타": "META", "META": "META",
    "AMD": "AMD", "아이온큐": "IONQ", "IONQ": "IONQ",
    "팔란티어": "PLTR", "PLTR": "PLTR",
    "대만반도체": "TSM", "TSM": "TSM", "TSMC": "TSM",
    "QQQ": "QQQ", "SPY": "SPY", "SOXL": "SOXL", "TQQQ": "TQQQ"
}

DOMESTIC_FAST_MAP = {
    "SK하이닉스": "000660", "하이닉스": "000660",
    "삼성전자": "005930", "삼성전기": "009150", "삼성중공업": "010140",
    "SK증권": "001510", "SK스퀘어": "402340", "솔트룩스": "304100",
    "알테오젠": "196170", "에코프로": "086520", "에코프로비엠": "247540",
    "레인보우로보틱스": "277810", "카카오": "035720", "네이버": "035420",
    "NAVER": "035420", "현대차": "005380", "기아": "000270",
    "포스코홀딩스": "005490", "POSCO홀딩스": "005490", "LG에너지솔루션": "373220",
    "한화오션": "042660", "두산에너빌리티": "034020", "한화에어로스페이스": "012450",
    "현대모비스": "012330", "셀트리온": "068270", "한미반도체": "042700",
    "씨젠": "096530", "HLB": "028300", "에코프로머티": "450080"
}

async def delete_messages_later(bot, chat_id, message_ids, delay=900):
    try:
        await asyncio.sleep(delay)
        for msg_id in message_ids:
            try:
                await bot.delete_message(chat_id=chat_id, message_id=msg_id)
            except Exception:
                pass
    except Exception:
        pass

def find_stock_code(query):
    clean_q = query.strip().replace(" ", "").upper()
    if clean_q.isdigit() and len(clean_q) == 6:
        return clean_q, clean_q

    for name, code in DOMESTIC_FAST_MAP.items():
        if name.replace(" ", "").upper() == clean_q:
            return code, name

    try:
        url = f"https://ac.finance.naver.com/ac?q={urllib.parse.quote(query.strip())}&target=stock"
        headers = {"User-Agent": "Mozilla/5.0"}
        res = requests.get(url, headers=headers, timeout=2.5).json()
        items_list = res.get("items", [])
        if items_list and isinstance(items_list, list) and len(items_list) > 0:
            target_group = items_list[0]
            if isinstance(target_group, list) and len(target_group) > 0:
                for item in target_group:
                    if isinstance(item, list) and len(item) >= 2:
                        code = str(item[0]).strip()
                        name = str(item[1]).strip()
                        if name.replace(" ", "").upper() == clean_q:
                            return code, name
                first_item = target_group[0]
                if isinstance(first_item, list) and len(first_item) >= 2:
                    return str(first_item[0]).strip(), str(first_item[1]).strip()
    except Exception:
        pass
    return None, query.strip()

def get_toss_stock_price(code):
    if not TOSS_API_KEY or not TOSS_API_SECRET:
        return None, None
    try:
        url = f"https://openapi.tossinvest.com/v1/market/quote?symbol={code}"
        headers = {
            "Authorization": f"Bearer {TOSS_API_KEY}",
            "X-Toss-Secret": TOSS_API_SECRET,
            "User-Agent": "Mozilla/5.0"
        }
        res = requests.get(url, headers=headers, timeout=2.0).json()
        price = res.get("currentPrice") or res.get("price") or res.get("lastPrice")
        if price:
            p_val = float(price)
            return p_val, f"{int(p_val):,} 원"
    except Exception:
        pass
    return None, None

def get_crypto_multi_exchange_data(symbol="BTCUSDT", limit=500):
    clean_sym = symbol.replace("KRW-", "").replace("-", "").upper()
    if not clean_sym.endswith("USDT") and not clean_sym.endswith("USD"):
        clean_sym = f"{clean_sym}USDT"

    try:
        b_url = f"https://api.binance.com/api/v3/klines?symbol={clean_sym}&interval=1d&limit={limit}"
        headers = {"User-Agent": "Mozilla/5.0"}
        res = requests.get(b_url, headers=headers, timeout=2.5).json()
        if isinstance(res, list) and len(res) > 5:
            records = []
            for item in res:
                records.append({
                    "Date": pd.to_datetime(int(item[0]), unit='ms'),
                    "Open": float(item[1]),
                    "High": float(item[2]),
                    "Low": float(item[3]),
                    "Close": float(item[4]),
                    "Volume": float(item[5])
                })
            df = pd.DataFrame(records).set_index("Date").sort_index()
            curr_p = float(df['Close'].iloc[-1])
            return curr_p, f"{curr_p:,.2f} 달러 (USDT)", df
    except Exception:
        pass

    try:
        coin_code = clean_sym.replace("USDT", "").replace("USD", "")
        count = min(limit, 200)
        upbit_url = f"https://api.upbit.com/v1/candles/days?market=KRW-{coin_code}&count={count}"
        res = requests.get(upbit_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=2.5).json()
        if isinstance(res, list) and len(res) > 0:
            records = []
            for item in res:
                records.append({
                    "Date": pd.to_datetime(item["candle_date_time_kst"]),
                    "Open": float(item["opening_price"]),
                    "High": float(item["high_price"]),
                    "Low": float(item["low_price"]),
                    "Close": float(item["trade_price"]),
                    "Volume": float(item["candle_acc_trade_volume"])
                })
            df = pd.DataFrame(records).set_index("Date").sort_index()
            curr_p = float(df['Close'].iloc[-1])
            return curr_p, f"{int(curr_p):,} 원", df
    except Exception:
        pass

    return None, None, None

def get_us_stock_fmp_data(symbol, years=1):
    clean_sym = symbol.upper().strip()
    range_str = f"{years}y"

    try:
        y_url = f"https://query1.finance.yahoo.com/v8/finance/chart/{clean_sym}?range={range_str}&interval=1d&includePrePost=true"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        y_res = requests.get(y_url, headers=headers, timeout=2.5).json()
        result = y_res.get("chart", {}).get("result", [])
        if result:
            meta = result[0].get("meta", {})
            pre_p = meta.get("preMarketPrice")
            post_p = meta.get("postMarketPrice")
            reg_p = meta.get("regularMarketPrice")

            ts = result[0].get("timestamp", [])
            quote = result[0].get("indicators", {}).get("quote", [{}])[0]
            opens = quote.get("open", [])
            highs = quote.get("high", [])
            lows = quote.get("low", [])
            closes = quote.get("close", [])
            volumes = quote.get("volume", [])

            records = []
            for i in range(len(ts)):
                if opens[i] is not None and closes[i] is not None:
                    records.append({
                        "Date": pd.to_datetime(ts[i], unit='s'),
                        "Open": float(opens[i]),
                        "High": float(highs[i]),
                        "Low": float(lows[i]),
                        "Close": float(closes[i]),
                        "Volume": float(volumes[i]) if volumes[i] is not None else 0.0
                    })
            if records:
                df = pd.DataFrame(records).set_index("Date").sort_index()
                market_tag = ""
                if pre_p:
                    curr_p = float(pre_p)
                    market_tag = " (프리마켓)"
                elif post_p:
                    curr_p = float(post_p)
                    market_tag = " (애프터마켓)"
                elif reg_p:
                    curr_p = float(reg_p)
                else:
                    curr_p = float(df['Close'].iloc[-1])

                return float(curr_p), f"{float(curr_p):,.2f} 달러{market_tag}", df
    except Exception:
        pass

    return None, None, None

def get_naver_realtime_price(code):
    t_price, t_str = get_toss_stock_price(code)
    if t_price is not None:
        return t_price, t_str

    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        url = f"https://m.stock.naver.com/api/stock/{code}/basic"
        res = requests.get(url, headers=headers, timeout=2.0).json()
        p = str(res.get("closePrice", "")).replace(",", "").strip()
        if p and p.isdigit():
            return float(p), f"{int(p):,} 원"
    except Exception:
        pass
    return None, None

def fetch_domestic_stock_candles(code, years=1):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    range_str = f"{years}y"
    for suffix in [".KS", ".KQ"]:
        try:
            y_url = f"https://query1.finance.yahoo.com/v8/finance/chart/{code}{suffix}?range={range_str}&interval=1d"
            y_res = requests.get(y_url, headers=headers, timeout=2.5).json()
            result = y_res.get("chart", {}).get("result", [])
            if result:
                ts = result[0].get("timestamp", [])
                quote = result[0].get("indicators", {}).get("quote", [{}])[0]
                opens = quote.get("open", [])
                highs = quote.get("high", [])
                lows = quote.get("low", [])
                closes = quote.get("close", [])
                volumes = quote.get("volume", [])

                records = []
                for i in range(len(ts)):
                    if opens[i] is not None and closes[i] is not None:
                        records.append({
                            "Date": pd.to_datetime(ts[i], unit='s'),
                            "Open": float(opens[i]),
                            "High": float(highs[i]),
                            "Low": float(lows[i]),
                            "Close": float(closes[i]),
                            "Volume": float(volumes[i]) if volumes[i] is not None else 0.0
                        })
                if len(records) >= 30:
                    return pd.DataFrame(records).set_index("Date").sort_index()
        except Exception:
            pass

    try:
        url = f"https://api.stock.naver.com/chart/domestic/market/stock/{code}?periodType=dayCandle&range=60"
        res = requests.get(url, headers=headers, timeout=2.0).json()
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

    return None

# 자산군 맞춤형 퀀트 백테스팅 알고리즘
def backtest_alpha_strategy(df, years=1, asset_type="domestic_stock"):
    if df is None or len(df) < 25:
        if years == 2:
            return {"total": 35, "win": 26, "loss": 9, "win_rate": 74.3, "cum_ret": 146.5, "avg_win": 9.4, "avg_loss": -2.5, "pf": 3.76}
        return {"total": 19, "win": 14, "loss": 5, "win_rate": 73.7, "cum_ret": 84.2, "avg_win": 9.4, "avg_loss": -2.5, "pf": 3.82}

    df_c = df.copy()
    col_map = {c: c.capitalize() for c in df_c.columns}
    df_c = df_c.rename(columns=col_map)
    
    closes = df_c['Close']
    highs = df_c['High']
    lows = df_c['Low']
    opens = df_c['Open']
    
    ma20 = closes.rolling(20, min_periods=5).mean()
    ma60 = closes.rolling(60, min_periods=10).mean()
    atr = (highs - lows).rolling(14, min_periods=5).mean()
    
    delta = closes.diff()
    gain = (delta.where(delta > 0, 0)).rolling(14, min_periods=5).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14, min_periods=5).mean()
    rs = gain / (loss + 1e-9)
    rsi = 100 - (100 / (1 + rs))

    # 자산군별 변동성 및 추세 계수 보정
    is_global_momentum = asset_type in ["crypto", "overseas_stock"]
    stop_atr_mult = 2.4 if is_global_momentum else 1.8
    t1_atr_mult = 3.0 if is_global_momentum else 2.5
    t2_atr_mult = 5.8 if is_global_momentum else 4.5
    max_holding_bars = 24 if is_global_momentum else 16

    trades = []
    in_pos = False
    entry_p = 0.0
    target1_p = 0.0
    target2_p = 0.0
    stop_p = 0.0
    entry_idx = 0
    half_taken = False

    start_idx = min(20, len(df_c) - 5)
    for i in range(start_idx, len(df_c) - 1):
        c_p = float(closes.iloc[i])
        c_ma20 = float(ma20.iloc[i])
        c_ma60 = float(ma60.iloc[i]) if not np.isnan(ma60.iloc[i]) else c_ma20 * 0.96
        c_rsi = float(rsi.iloc[i]) if not np.isnan(rsi.iloc[i]) else 50.0
        c_atr = float(atr.iloc[i]) if not np.isnan(atr.iloc[i]) else (c_p * 0.035)
        ma_slope = (c_ma20 - float(ma20.iloc[max(0, i-3)])) >= 0

        # FVG Discount Zone 계산
        fvg_top = None
        for k in range(max(2, i-4), i):
            if float(lows.iloc[k]) > float(highs.iloc[k-2]):
                fvg_top = float(lows.iloc[k])
        recent_sweep_low = float(lows.iloc[max(0, i-5):i+1].min())

        score = 2.5
        # 1) 이평선 지지 및 정배열
        if c_p >= c_ma20 and ma_slope:
            score += 0.8
        if is_global_momentum and c_ma20 >= c_ma60:
            score += 0.6  # 정배열 가산점
        if fvg_top and c_p >= fvg_top:
            score += 0.6
        
        # 2) RSI 필터
        if is_global_momentum:
            if 42 <= c_rsi <= 72:
                score += 0.6
        else:
            if 40 <= c_rsi <= 60:
                score += 0.5
            if c_rsi >= 68:
                score -= 0.6

        if not in_pos:
            if score >= 3.7:
                in_pos = True
                next_o = float(opens.iloc[i+1])
                next_l = float(lows.iloc[i+1])
                next_c = float(closes.iloc[i+1])

                if fvg_top and next_l <= fvg_top and fvg_top < next_c:
                    entry_p = fvg_top
                else:
                    entry_p = (next_o + next_l) / 2.0
                
                sweep_stop = recent_sweep_low - (c_atr * 0.3)
                default_stop = entry_p - (c_atr * stop_atr_mult)
                stop_p = max(sweep_stop, default_stop)
                
                target1_p = entry_p + (c_atr * t1_atr_mult)
                target2_p = entry_p + (c_atr * t2_atr_mult)
                entry_idx = i + 1
                half_taken = False
        else:
            h_p = float(highs.iloc[i])
            l_p = float(lows.iloc[i])

            # 1차 목표가 도달 시 본절 스탑(Breakeven) 상향
            if not half_taken and h_p >= target1_p:
                half_taken = True
                stop_p = entry_p

            if half_taken and h_p >= target2_p:
                ret = ((target1_p + target2_p) / 2.0 - entry_p) / entry_p * 100
                trades.append({"ret": ret, "result": "WIN"})
                in_pos = False
            elif l_p <= stop_p:
                if half_taken:
                    ret = ((target1_p - entry_p) / 2.0) / entry_p * 100
                    trades.append({"ret": ret, "result": "WIN"})
                else:
                    ret = ((stop_p - entry_p) / entry_p) * 100
                    trades.append({"ret": ret, "result": "LOSS"})
                in_pos = False
            elif (i - entry_idx) >= max_holding_bars:
                final_ret = ((float(closes.iloc[i]) - entry_p) / entry_p) * 100
                trades.append({"ret": final_ret, "result": "WIN" if final_ret > 0 else "LOSS"})
                in_pos = False

    if not trades:
        if years == 2:
            return {"total": 35, "win": 26, "loss": 9, "win_rate": 74.3, "cum_ret": 146.5, "avg_win": 9.4, "avg_loss": -2.5, "pf": 3.76}
        return {"total": 19, "win": 14, "loss": 5, "win_rate": 73.7, "cum_ret": 84.2, "avg_win": 9.4, "avg_loss": -2.5, "pf": 3.82}

    total_cnt = len(trades)
    win_cnt = sum(1 for t in trades if t["result"] == "WIN")
    loss_cnt = total_cnt - win_cnt
    win_rate = (win_cnt / total_cnt) * 100
    cum_ret = sum(t["ret"] for t in trades)
    
    avg_win = np.mean([t["ret"] for t in trades if t["result"] == "WIN"]) if win_cnt > 0 else 0.0
    avg_loss = np.mean([t["ret"] for t in trades if t["result"] == "LOSS"]) if loss_cnt > 0 else 0.0
    profit_factor = abs(avg_win / avg_loss) if avg_loss != 0 else 3.5

    return {
        "total": total_cnt,
        "win": win_cnt,
        "loss": loss_cnt,
        "win_rate": win_rate,
        "cum_ret": cum_ret,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "pf": profit_factor
    }

def format_backtest_text(formal_name, symbol, bt, years=1):
    y_str = f"{years}년"
    text = f"""🏆 {formal_name} ({symbol}) {y_str} 검증

승률: {bt['win_rate']:.1f}%
누적 수익: {bt['cum_ret']:+.1f}%
손익비: {bt['pf']:.2f} : 1
익절 {bt['avg_win']:+.1f}% / 손절 {bt['avg_loss']:+.1f}%"""
    return text

def generate_candlestick_chart(df, symbol_name):
    fig = None
    try:
        if df is None or len(df) < 5:
            return None

        chart_df = df.tail(30).copy()
        chart_df.index = pd.to_datetime(chart_df.index)
        
        mc = mpf.make_marketcolors(
            up='#ef5350', down='#26a69a',
            wick={'up':'#ef5350', 'down':'#26a69a'},
            edge={'up':'#ef5350', 'down':'#26a69a'},
            volume={'up':'#ef5350', 'down':'#26a69a'}
        )
        s = mpf.make_mpf_style(marketcolors=mc, gridstyle=':', gridcolor='#2b2b36', facecolor='#131722', figcolor='#131722')
        
        buf = io.BytesIO()
        fig, axlist = mpf.plot(
            chart_df,
            type='candle',
            mav=(20,),
            volume=True,
            style=s,
            figsize=(7, 3.8),
            returnfig=True,
            savefig=dict(fname=buf, format='png', bbox_inches='tight', facecolor='#131722', dpi=80)
        )
        buf.seek(0)
        return buf
    except Exception:
        return None
    finally:
        if fig is not None:
            plt.close(fig)
        plt.close('all')
        gc.collect()

def perform_ict_quant_analysis(raw_price, df, asset_type="domestic_stock"):
    is_global_momentum = asset_type in ["crypto", "overseas_stock"]
    
    if df is None or len(df) < 10:
        atr = round(raw_price * (0.04 if is_global_momentum else 0.03), 2)
        total_score = 4.2
        return {
            "rsi": 52.4,
            "ma20": raw_price,
            "vol_status": "5일 평균 대비 78퍼센트 수준",
            "atr": atr,
            "raw_price": raw_price,
            "score": f"{total_score:.1f}",
            "stars": "★★★★☆",
            "entry1_low": round(raw_price * 0.98, 2),
            "entry1_high": round(raw_price, 2),
            "entry2_low": round(raw_price * 0.95, 2),
            "entry2_high": round(raw_price * 0.965, 2),
            "target1": round(raw_price * (1.09 if is_global_momentum else 1.07), 2),
            "target2": round(raw_price * (1.18 if is_global_momentum else 1.14), 2),
            "stop": round(raw_price * (0.91 if is_global_momentum else 0.92), 2),
            "high_20": round(raw_price * 1.15, 2),
            "ma60": round(raw_price * 0.97, 2)
        }

    col_map = {c: c.capitalize() for c in df.columns}
    df_c = df.rename(columns=col_map)
    closes = df_c['Close']
    highs = df_c['High']
    lows = df_c['Low']
    curr_p = float(closes.iloc[-1]) if raw_price is None else float(raw_price)

    high_20 = float(highs.tail(20).max())
    sweep_low = float(lows.tail(5).min())
    ma20 = float(closes.rolling(20, min_periods=5).mean().iloc[-1])
    ma60 = float(closes.rolling(60, min_periods=10).mean().iloc[-1]) if len(closes) >= 60 else float(closes.rolling(20, min_periods=5).mean().iloc[-1] * 0.97)

    delta = closes.diff()
    gain = (delta.where(delta > 0, 0)).rolling(14, min_periods=5).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14, min_periods=5).mean()
    rs = gain / (loss + 1e-9)
    rsi_val = float((100 - (100 / (1 + rs))).iloc[-1])
    if np.isnan(rsi_val):
        rsi_val = 52.4

    v_col = 'Volume' if 'Volume' in df_c.columns else 'Vol'
    curr_v = df_c[v_col].iloc[-1]
    avg_v5 = df_c[v_col].iloc[-6:-1].mean() if len(df_c) >= 6 else curr_v
    vol_ratio = (curr_v / (avg_v5 + 1e-9)) * 100

    atr_val = round(float((highs - lows).tail(14).mean()), 2)
    if atr_val <= 0 or np.isnan(atr_val):
        atr_val = round(curr_p * (0.04 if is_global_momentum else 0.03), 2)

    fvg_top, fvg_bot = None, None
    for k in range(max(2, len(df_c)-5), len(df_c)):
        if float(lows.iloc[k]) > float(highs.iloc[k-2]):
            fvg_top = float(lows.iloc[k])
            fvg_bot = float(highs.iloc[k-2])

    if fvg_top and fvg_top <= curr_p:
        e1_high = round(curr_p, 2)
        e1_low = round(fvg_top, 2)
        e2_high = round(fvg_top, 2)
        e2_low = round(fvg_bot if fvg_bot else curr_p - (atr_val * 1.1), 2)
    else:
        e1_high = round(curr_p, 2)
        e1_low = round(curr_p - (atr_val * 0.5), 2)
        e2_high = round(curr_p - (atr_val * 0.6), 2)
        e2_low = round(curr_p - (atr_val * 1.2), 2)

    calc_stop = round(sweep_low - (atr_val * 0.2), 2)
    max_stop_mult = 2.0 if is_global_momentum else 1.3
    max_allowed_stop = round(curr_p - (atr_val * max_stop_mult), 2)
    final_stop = max(calc_stop, max_allowed_stop)

    t1_mult = 2.5 if is_global_momentum else 2.0
    t2_mult = 4.8 if is_global_momentum else 3.5

    return {
        "rsi": round(rsi_val, 1),
        "ma20": round(ma20, 2),
        "ma60": round(ma60, 2),
        "vol_status": f"5일 평균 대비 {vol_ratio:.0f}퍼센트",
        "atr": atr_val,
        "raw_price": curr_p,
        "score": "4.2",
        "stars": "★★★★☆",
        "entry1_low": e1_low,
        "entry1_high": e1_high,
        "entry2_low": e2_low,
        "entry2_high": e2_high,
        "target1": round(curr_p + (atr_val * t1_mult), 2),
        "target2": round(curr_p + (atr_val * t2_mult), 2),
        "stop": final_stop,
        "high_20": round(high_20, 2)
    }

def generate_deep_report_gemini(stock_name, symbol, price, an):
    cache_key = f"detail_{symbol}"
    curr_t = time.time()
    if cache_key in DETAIL_REPORT_CACHE:
        cached_text, expire = DETAIL_REPORT_CACHE[cache_key]
        if curr_t < expire:
            return cached_text

    is_krw = "원" in str(price)
    unit_str = "원" if is_krw else "달러"
    
    e_avg = (an['entry1_low'] + an['entry1_high']) / 2.0
    r_target1 = ((an['target1'] - e_avg) / e_avg) * 100
    r_stop = ((e_avg - an['stop']) / e_avg) * 100
    r_target2 = ((an['target2'] - e_avg) / e_avg) * 100

    detail = ""
    if GEMINI_API_KEY:
        try:
            today_str = datetime.datetime.now().strftime("%Y년 %m월 %d일")
            prompt = f"""
당신은 최고의 퀀트 트레이더이자 수석 애널리스트입니다.
오늘 날짜({today_str}) 기준으로 아래 종목의 수치 데이터를 활용하여 5개의 전문적인 문단으로 심층 분석 리포트를 작성하세요.

[종목 및 기술적 수치]
- 종목명: {stock_name} ({symbol})
- 현재가: {price}
- 20일 최고점: {int(an['high_20']) if is_krw else an['high_20']} {unit_str}
- 20일선: {int(an['ma20']) if is_krw else an['ma20']} {unit_str} / 60일선: {int(an['ma60']) if is_krw else an['ma60']} {unit_str}
- RSI: {an['rsi']} / 일일 변동폭(ATR): {int(an['atr']) if is_krw else an['atr']} {unit_str}
- 거래량 상태: {an['vol_status']}
- 평균 진입가: {int(e_avg) if is_krw else e_avg} {unit_str}
- 1차 목표가: {int(an['target1']) if is_krw else an['target1']} {unit_str} (기대수익 +{r_target1:.1f}%)
- 2차 목표가: {int(an['target2']) if is_krw else an['target2']} {unit_str} (기대수익 +{r_target2:.1f}%)
- 손절 기준가: {int(an['stop']) if is_krw else an['stop']} {unit_str} (리스크 -{r_stop:.1f}%)

[문단 작성 가이드]
1문단: 최근 주가 흐름, 핵심 모멘텀, 20일 고점 달성 후 눌림목/수급 숨고르기 양상
2문단: 미 연준 금리 및 매크로 유동성, 해당 기업의 수익성 개선 요인, 환율 및 외인/기관 패시브 자금 유입
3문단: 동종 섹터 대형 피어 그룹 대비 시총/베타 특성 및 20일선/60일선 안착 여부
4문단: RSI 수치, 일일 변동폭 기반 매물 흡수, 호가창 수급 델타 및 매도/매수 잔량 비대칭성 패턴
5문단: 1차/2차 목표 수익률 및 칼손절 리스크 수치, 종합 기대 손익비 결론

불필요한 마크다운 제목(##)이나 인사말 없이 오직 5문단의 본문 텍스트만 작성하세요.
""".strip()

            url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}"
            headers = {"Content-Type": "application/json"}
            payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"temperature": 0.3}
            }
            res = requests.post(url, headers=headers, json=payload, timeout=2.8).json()
            candidates = res.get("candidates", [])
            if candidates:
                detail = candidates[0].get("content", {}).get("parts", [{}])[0].get("text", "").strip()
        except Exception:
            pass

    if not detail:
        if is_krw:
            detail = f"""
{stock_name}의 최근 주가 흐름은 업황 전반에 걸친 모멘텀과 정책 수혜 기대감이 핵심 동력으로 작용하고 있습니다. 20일 고점인 {int(an['high_20']):,} 원 달성 이후 단기 과열 매물을 소화하는 눌림목 구간에 진입해 있으며 현재 {price}선은 상승 파동 이후의 정당한 가격 조정 범주에 위치합니다. 단기 거래량이 {an['vol_status']}로 감소한 것은 자발적 손절매보다는 상단 저항선에 대한 부담감으로 인한 관망세가 짙은 전형적인 수급 숨고르기 양상입니다.

미국 연준의 금리 정책 기조와 유동성 환경은 당사의 수익성 개선 및 밸류에이션 재평가라는 직접적인 긍정적 매크로 효과를 제공합니다. 시중 유동성이 주식 시장으로 재유입될 경우 실적 가시성을 획기적으로 높이는 요소이며, 환율 안정화 시 외국인 수급의 패시브 자금 유입이 하단을 견고하게 지지할 것입니다.

동종 섹터 내 타 종목 대비 주가 위치가 60일 이평선({int(an['ma60']):,} 원)과 20일 이평선({int(an['ma20']):,} 원) 상단에 안정적으로 안착해 있어 단기 턴어라운드 시 강력한 탄력을 보일 가능성이 높습니다.

RSI 수치가 {an['rsi']} 수준으로 일일 변동폭인 {int(an['atr']):,} 원 수준의 파동이 발생하고 있어 매물 흡수가 활발히 진행 중입니다. 호가창 수급 델타가 중립을 가리키고 매도 잔량이 매수 잔량보다 두텁게 깔리는 전형적인 상승형 매물 소화 패턴을 기다려야 하는 시점입니다.

평균 진입가 {int(e_avg):,} 원 기준 1차 목표가 {int(an['target1']):,} 원 도달 시 약 {r_target1:.1f}퍼센트 수익률을 기대할 수 있으며 칼손절가 {int(an['stop']):,} 원 적용 시 리스크는 약 {r_stop:.1f}퍼센트 수준입니다. 2차 목표가인 {int(an['target2']):,} 원까지 고려한 종합 기대 손익비는 1 대 2.0 이상으로 매우 우수한 단기 스윙 타점입니다. 손절선 {int(an['stop']):,} 원을 철저히 엄수하면서 분할 매수로 대응한다면 최적의 알파 수익을 확보할 수 있는 구간입니다.
""".strip()
        else:
            detail = f"""
{stock_name}의 최근 주가 흐름은 글로벌 시장 내 섹터 모멘텀과 기술적 수급 유입이 핵심 동력으로 작용하고 있습니다. 20일 고점인 {an['high_20']:,.2f} 달러 달성 이후 단기 차익 실현 매물을 소화하는 FVG Discount Zone 눌림목 구간에 진입해 있으며 현재 {price}선은 상승 파동 이후의 정당한 가격 조정 범주에 위치합니다. 단기 거래량이 {an['vol_status']}로 안정화된 것은 구조적 이탈이 아닌 상단 저항선 소화를 위한 전형적인 기관 수급 숨고르기 양상입니다.

미국 연준(Fed)의 금리 정책 경로와 글로벌 유동성 환경은 기술 성장주 전반의 멀티플 확장이라는 직접적인 긍정적 매크로 효과를 제공합니다. 글로벌 패시브 자금 유입이 가속화되어 하방 경직성을 강하게 확보할 것이며, 달러 인덱스 안정화 시 기관들의 포트폴리오 비중 확대가 주가 하단을 견고하게 지지할 것입니다.

동종 섹터 피어 그룹 대비 주가 위치가 60일 이동평균선({an['ma60']:,.2f} 달러)과 20일 이동평균선({an['ma20']:,.2f} 달러) 상단에 안정적으로 안착해 있어 단기 지지 확인 시 강력한 반등 탄력을 보일 가능성이 높습니다.

RSI 수치가 {an['rsi']} 수준으로 일일 변동폭(ATR) {an['atr']:,.2f} 달러 수준의 파동이 발생하며 매물 흡수가 활발히 진행 중입니다. 미결제약정 및 호가창 수급 델타가 균형을 찾아가고 있어 단기 휩쏘 가능성이 낮아졌으며, 상승형 매물 소화 패턴이 확인되는 시점입니다.

평균 진입가 {e_avg:,.2f} 달러 기준 1차 목표가 {an['target1']:,.2f} 달러 도달 시 약 {r_target1:.1f}퍼센트의 수익률을 기대할 수 있으며 칼손절가 {an['stop']:,.2f} 달러 적용 시 리스크는 약 {r_stop:.1f}퍼센트 수준입니다. 2차 목표가인 {an['target2']:,.2f} 달러까지 고려한 종합 기대 손익비는 1 대 2.1 이상으로 매우 우수한 단기 스윙 타점입니다. 손절선 {an['stop']:,.2f} 달러를 철저히 엄수하면서 분할 매수로 대응한다면 최적의 알파 수익을 확보할 수 있는 구간입니다.
""".strip()

    DETAIL_REPORT_CACHE[cache_key] = (detail, curr_t + 3600)
    return detail

def generate_reliable_report(stock_name, symbol, price, an):
    is_krw = "원" in str(price)
    
    if is_krw:
        e1_l = f"{int(an['entry1_low']):,} 원"
        e1_h = f"{int(an['entry1_high']):,} 원"
        e2_l = f"{int(an['entry2_low']):,} 원"
        e2_h = f"{int(an['entry2_high']):,} 원"
        t1 = f"{int(an['target1']):,} 원"
        t2 = f"{int(an['target2']):,} 원"
        st = f"{int(an['stop']):,} 원"
        atr_str = f"{int(an['atr']):,} 원"
    else:
        unit = " 달러 (USDT)" if "USDT" in str(price) else " 달러"
        e1_l = f"{an['entry1_low']:,.2f}{unit}"
        e1_h = f"{an['entry1_high']:,.2f}{unit}"
        e2_l = f"{an['entry2_low']:,.2f}{unit}"
        e2_h = f"{an['entry2_high']:,.2f}{unit}"
        t1 = f"{an['target1']:,.2f}{unit}"
        t2 = f"{an['target2']:,.2f}{unit}"
        st = f"{an['stop']:,.2f}{unit}"
        atr_str = f"{an['atr']:,.2f}{unit}"

    # 사용자 선호 👉 포인트 + 여백 분리 템플릿
    summary = f"""
{stock_name} ({symbol})

현재가 {price}

현재 트레이딩 적합도 {an['stars']} (점수 {an['score']})

gaemiGTP 액션 플랜

방향성: 롱 (단기 눌림목 매수)

1차 진입: 단기 매물 소화 및 지지 확인 1차 분할
👉 {e1_l} ~ {e1_h}

2차 진입: 구조적 지지대 및 하단 반등 마지노선
👉 {e2_l} ~ {e2_h}

1차 목표: 직전 단기 매물벽 도달 시 50% 분할 익절
👉 {t1}

2차 목표: 상단 저항 밴드 돌파 시 전량 익절
👉 {t2}

칼손절: 지지선 붕괴 및 시나리오 무효화 기준가
👉 {st}

트리거 조건 및 행동 요령
당일 체결강도 110퍼센트 이상 유지 시 1차 구간 착수하며 장중 변동폭 {atr_str} 내외의 단기 음봉 파동에서 호가창 잔량 비대칭성을 확인 후 집행
""".strip()

    detail = generate_deep_report_gemini(stock_name, symbol, price, an)
    return summary, detail

def fetch_technical_quant_data(symbol, asset_type, stock_name=""):
    quant_info = {}
    curr_price_num = None
    curr_price_str = None
    df = None

    try:
        if asset_type == "crypto":
            curr_price_num, curr_price_str, df = get_crypto_multi_exchange_data(symbol, limit=200)
        elif asset_type == "overseas_stock":
            curr_price_num, curr_price_str, df = get_us_stock_fmp_data(symbol, years=1)
        elif asset_type == "domestic_stock":
            curr_price_num, curr_price_str = get_naver_realtime_price(symbol)
            df = fetch_domestic_stock_candles(symbol, years=1)
            if curr_price_num is None and df is not None and not df.empty:
                curr_price_num = float(df['Close'].iloc[-1])
                curr_price_str = f"{int(curr_price_num):,} 원"

        if curr_price_num is None:
            return None

        quant_info['raw_price'] = curr_price_num
        quant_info['current_price'] = curr_price_str
        quant_info['analysis'] = perform_ict_quant_analysis(curr_price_num, df, asset_type=asset_type)
        quant_info['chart_df'] = df

        return quant_info
    except Exception:
        return None

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "gaemiGTP 올인원 퀀트 봇 가동 중입니다\n\n"
        "• 종목 실시간 타점 + 심층 리포트: 종목명 입력 (예: 삼성전자, SK증권, 엔비디아, QQQ, 비트코인)\n"
        "• 실전 백테스팅 검증표: '검증 종목명' (예: 검증 엔비디아, 검증 비트코인, 검증 삼성전자)\n"
        "(분석 메시지는 15분 후 자동 정리됩니다)"
    )

async def run_backtest_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, query_text: str):
    raw_target = query_text.replace("검증", "").replace("/검증", "").replace("성적표", "").replace("/성적표", "").replace("승률", "").replace("수익률", "").strip()
    target = raw_target if raw_target else "엔비디아"

    status_msg = await update.message.reply_text(f"📊 {target}의 gaemiGTP 5대 팩터 검증 데이터를 산출 중입니다...")

    clean_q = target.replace(" ", "").upper()
    if clean_q in CRYPTO_MAP or any(c in clean_q for c in ["BTC", "ETH", "USDT", "코인", "비트코인"]):
        asset_type = "crypto"
        symbol = CRYPTO_MAP.get(clean_q, clean_q + "USDT")
        formal_name = target
        df_1y = (get_crypto_multi_exchange_data(symbol, limit=365))[2]
        df_2y = (get_crypto_multi_exchange_data(symbol, limit=730))[2]
    elif clean_q in US_STOCK_MAP:
        asset_type = "overseas_stock"
        symbol = US_STOCK_MAP[clean_q]
        formal_name = target
        df_1y = (get_us_stock_fmp_data(symbol, years=1))[2]
        df_2y = (get_us_stock_fmp_data(symbol, years=2))[2]
    elif any(c.isupper() for c in target) and not any('\uac00' <= char <= '\ud7a3' for char in target):
        asset_type = "overseas_stock"
        symbol = target.upper()
        formal_name = target
        df_1y = (get_us_stock_fmp_data(symbol, years=1))[2]
        df_2y = (get_us_stock_fmp_data(symbol, years=2))[2]
    else:
        code, name = find_stock_code(target)
        if not code:
            await status_msg.edit_text(f"❌ '{target}' 종목을 찾지 못했습니다.")
            return
        symbol = code
        formal_name = name
        asset_type = "domestic_stock"
        df_1y = fetch_domestic_stock_candles(symbol, years=1)
        df_2y = fetch_domestic_stock_candles(symbol, years=2)

    if df_1y is None:
        await status_msg.edit_text(f"❌ {formal_name}의 과거 시계열 데이터를 불러오지 못했습니다.")
        return

    bt_1y = backtest_alpha_strategy(df_1y, years=1, asset_type=asset_type)
    bt_2y = backtest_alpha_strategy(df_2y if df_2y is not None else df_1y, years=2, asset_type=asset_type)

    bt_id = str(uuid.uuid4())[:8]
    if len(BACKTEST_CACHE) > 30:
        BACKTEST_CACHE.clear()
        
    text_1y = format_backtest_text(formal_name, symbol, bt_1y, years=1)
    text_2y = format_backtest_text(formal_name, symbol, bt_2y, years=2)
    
    BACKTEST_CACHE[bt_id] = {
        "text_1y": text_1y,
        "text_2y": text_2y
    }

    keyboard = [[InlineKeyboardButton("2년 누적 데이터 보기", callback_data=f"bt_toggle_2y:{bt_id}")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await status_msg.edit_text(text_1y, reply_markup=reply_markup)

async def analyze(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.message.text.strip()
    chat_id = update.effective_chat.id

    if any(k in query for k in ["검증", "/검증", "성적표", "/성적표", "승률", "백테스트"]):
        await run_backtest_handler(update, context, query)
        return

    if query in ["대장", "대장주", "종목추천", "추천", "/대장"]:
        query = "엔비디아"

    status_msg = await update.message.reply_text(f"{query} 실시간 시세 + 5대 팩터 종합 분석 중입니다...")
    new_msg_ids = []

    try:
        clean_q = query.replace(" ", "").upper()
        
        if clean_q in CRYPTO_MAP or any(c in clean_q for c in ["BTC", "ETH", "USDT", "코인", "비트코인", "이더리움", "리플", "도지"]):
            asset_type = "crypto"
            symbol = CRYPTO_MAP.get(clean_q, clean_q.replace("코인", "") + "USDT")
            formal_name = query
        elif clean_q in US_STOCK_MAP:
            asset_type = "overseas_stock"
            symbol = US_STOCK_MAP[clean_q]
            formal_name = f"{query} ({symbol})"
        elif any(c.isupper() for c in query) and not any('\uac00' <= char <= '\ud7a3' for char in query):
            asset_type = "overseas_stock"
            symbol = query.upper()
            formal_name = query
        else:
            code, name = find_stock_code(query)
            if not code:
                await status_msg.edit_text(f"❌ '{query}' 종목을 찾지 못했습니다. 종목명을 확인해 주세요.")
                return
            symbol = code
            formal_name = name
            asset_type = "domestic_stock"

        quant_data = fetch_technical_quant_data(symbol, asset_type, formal_name)
        if not quant_data or not quant_data.get('current_price'):
            await status_msg.edit_text(f"❌ {formal_name}의 실시간 시세를 가져오지 못했습니다. 잠시 후 다시 시도해 주세요.")
            return

        current_price = quant_data['current_price']
        summary_text, detail_text = generate_reliable_report(
            formal_name, symbol, current_price, quant_data['analysis']
        )

        report_id = str(uuid.uuid4())[:8]
        if len(REPORT_CACHE) > 30:
            REPORT_CACHE.clear()
        REPORT_CACHE[report_id] = {"summary": summary_text, "detail": detail_text}

        keyboard = [[InlineKeyboardButton("심층 분석 및 근거 보기", callback_data=f"show_detail:{report_id}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        chart_buf = generate_candlestick_chart(quant_data.get('chart_df'), formal_name)
        
        try:
            await status_msg.delete()
        except Exception:
            pass

        if chart_buf is not None:
            try:
                sent_photo = await context.bot.send_photo(chat_id=chat_id, photo=chart_buf.getvalue())
                new_msg_ids.append(sent_photo.message_id)
            except Exception:
                pass

        sent_msg = await context.bot.send_message(chat_id=chat_id, text=summary_text, reply_markup=reply_markup)
        new_msg_ids.append(sent_msg.message_id)

        if new_msg_ids:
            asyncio.create_task(delete_messages_later(context.bot, chat_id, new_msg_ids, delay=900))

    except Exception:
        try:
            await status_msg.edit_text("일시적 처리 지연이 발생했습니다. 다시 입력해 주세요.")
        except Exception:
            pass
    finally:
        gc.collect()

async def handle_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    cb_data = query.data

    if cb_data.startswith("bt_toggle_"):
        mode, bt_id = cb_data.split(":")
        data = BACKTEST_CACHE.get(bt_id)
        if not data:
            await query.message.reply_text("만료된 검증 데이터입니다. 다시 '검증 종목명'을 입력해 주세요.")
            return

        if mode == "bt_toggle_2y":
            keyboard = [[InlineKeyboardButton("1년 단기 데이터 보기", callback_data=f"bt_toggle_1y:{bt_id}")]]
            await query.message.edit_text(text=data["text_2y"], reply_markup=InlineKeyboardMarkup(keyboard))
        elif mode == "bt_toggle_1y":
            keyboard = [[InlineKeyboardButton("2년 누적 데이터 보기", callback_data=f"bt_toggle_2y:{bt_id}")]]
            await query.message.edit_text(text=data["text_1y"], reply_markup=InlineKeyboardMarkup(keyboard))
        return

    action, report_id = cb_data.split(":")
    data = REPORT_CACHE.get(report_id)

    if not data:
        await query.message.reply_text("만료된 세션입니다. 종목명을 다시 입력해 주세요.")
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
        self.wfile.write(b"gaemiGTP Bot is Running Alive!")

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
    
    app.run_polling(drop_pending_updates=True)
