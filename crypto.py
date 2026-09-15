import time
import pandas as pd
from performance import HTTP_SESSION

def _fetch_crypto_candles_uncached(symbol):
    clean_sym = symbol.upper().replace("-", "")
    if not clean_sym.endswith("USDT") and not clean_sym.endswith("USD"):
        clean_sym += "USDT"
    try:
        url = f"https://api.binance.com/api/v3/klines?symbol={clean_sym}&interval=1d&limit=1000"
        res = HTTP_SESSION.get(url, timeout=2.0).json()
        if isinstance(res, list) and len(res) > 5:
            records = [{
                "Date": pd.to_datetime(int(x[0]), unit='ms'),
                "Open": float(x[1]), "High": float(x[2]), "Low": float(x[3]), "Close": float(x[4]), "Volume": float(x[5])
            } for x in res]
            df = pd.DataFrame(records).set_index("Date").sort_index()
            curr_p = float(df['Close'].iloc[-1])
            return curr_p, f"{curr_p:,.2f} 달러 (USDT)", df
    except Exception:
        pass
    return None, None, None


# Shared crypto candle cache (moved from bot.py; behavior preserved)
CRYPTO_CANDLE_CACHE = {}
CRYPTO_CANDLE_CACHE_TTL = 10 * 60

def fetch_crypto_candles(symbol):
    clean_sym = str(symbol or "").upper().replace("-", "")
    if not clean_sym.endswith("USDT") and not clean_sym.endswith("USD"):
        clean_sym += "USDT"
    now = time.time()
    cached = CRYPTO_CANDLE_CACHE.get(clean_sym)
    if cached and now - cached["time"] < CRYPTO_CANDLE_CACHE_TTL:
        try:
            df = cached["df"].copy(deep=True)
            curr_p = float(df["Close"].iloc[-1])
            return curr_p, f"{curr_p:,.2f} 달러 (USDT)", df
        except Exception:
            CRYPTO_CANDLE_CACHE.pop(clean_sym, None)
    value = _fetch_crypto_candles_uncached(clean_sym)
    if value and value[2] is not None:
        CRYPTO_CANDLE_CACHE[clean_sym] = {"time": now, "df": value[2].copy(deep=True)}
    return value
