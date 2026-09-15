"""Naver/KRX real-time domestic index helpers."""

from performance import HTTP_SESSION

def fetch_naver_realtime_index(index_code):
    """국내 지수 전용 실시간/최신 시세. Yahoo의 regularMarketPrice를 사용하지 않는다."""
    try:
        url = f"https://polling.finance.naver.com/api/realtime/domestic/index/{index_code}"
        res = HTTP_SESSION.get(
            url,
            timeout=2.0,
            headers={"Accept-Language": "ko-KR,ko;q=0.9", "User-Agent": "Mozilla/5.0"}
        )
        res.raise_for_status()
        payload = res.json()
        datas = payload.get("datas") or []
        if not datas:
            return None

        d = datas[0]
        price_raw = str(d.get("closePrice", "")).replace(",", "").strip()
        rate_raw = str(d.get("fluctuationsRatio", "")).replace(",", "").strip()
        change_raw = str(d.get("compareToPreviousClosePrice", "")).replace(",", "").strip()

        if not price_raw or not rate_raw:
            return None

        price = float(price_raw)
        rate = float(rate_raw)
        change = float(change_raw) if change_raw not in {"", "-", "None"} else None

        return {
            "price": price,
            "rate": rate,
            "change": change,
            "market_status": d.get("marketStatus"),
            "traded_at": d.get("localTradedAt"),
        }
    except Exception as exc:
        print(f"[NAVER INDEX] {index_code} failed: {exc}")
        return None


import pandas as pd

def fetch_market_5day_returns_from_df(df, index_name):
    """주어진 일봉 데이터에서 최근 5거래일 수익률 흐름을 계산합니다."""
    try:
        if df is None or df.empty or "Close" not in df.columns:
            return None
        closes = [float(x) for x in df["Close"].dropna().tail(6).tolist()]
        if len(closes) < 6:
            return None
        points = []
        base = closes[0]
        for i in range(1, 6):
            prev_close, close = closes[i - 1], closes[i]
            daily = ((close / prev_close) - 1.0) * 100.0 if prev_close else 0.0
            cumulative = ((close / base) - 1.0) * 100.0 if base else 0.0
            points.append({
                "date": f"D-{6-i}",
                "close": round(close, 8),
                "daily_return": round(daily, 2),
                "cumulative_return": round(cumulative, 2),
            })
        total = points[-1]["cumulative_return"]
        score = 2 if total >= 1.0 else 1 if total >= 0.3 else -2 if total <= -1.0 else -1 if total <= -0.3 else 0
        return {
            "index_name": index_name, "score": score,
            "detail": f"{index_name} 최근 5거래일 {total:+.2f}%",
            "total_return": round(total, 2), "points": points,
            "source": "실제 일봉 데이터"
        }
    except Exception as exc:
        print(f"[MARKET 5D DF] {index_name} failed: {exc}")
        return None



from overseas_market import fetch_overseas_candles

def fetch_market_5day_returns_from_symbol(symbol, index_name, world_item=None):
    """Yahoo의 실제 일봉으로 미국 시장 등 지수의 최근 5거래일 흐름을 계산합니다."""
    try:
        if isinstance(world_item, dict):
            closes = [float(x) for x in (world_item.get("sparkline") or []) if x is not None]
            if len(closes) >= 6:
                return fetch_market_5day_returns_from_df(
                    pd.DataFrame({"Close": closes}), index_name
                )
        _, _, df = fetch_overseas_candles(symbol)
        return fetch_market_5day_returns_from_df(df, index_name)
    except Exception as exc:
        print(f"[MARKET 5D SYMBOL] {index_name} failed: {exc}")
        return None

def fetch_naver_index_5day_returns(index_code="KOSPI", world_item=None):
    """실제 시장 지수의 최근 5거래일 흐름을 계산한다.
    우선 이미 /api/analyze에서 정상 수집된 world_board의 실제 종가 history를 사용하고,
    부족하면 동일한 Yahoo 지수 차트 경로를 보조로 사용한다. 별도 API 키는 필요 없다.
    """
    index_name = "KOSPI" if str(index_code).upper() == "KOSPI" else "KOSDAQ"
    try:
        closes = []
        if isinstance(world_item, dict):
            closes = [float(x) for x in (world_item.get("sparkline") or []) if x is not None]

        if len(closes) < 6:
            symbol = "^KS11" if index_name == "KOSPI" else "^KQ11"
            _, _, df = fetch_overseas_candles(symbol)
            if df is not None and not df.empty and "Close" in df.columns:
                closes = [float(x) for x in df["Close"].dropna().tail(7).tolist()]

        if len(closes) < 6:
            return None

        closes = closes[-6:]
        points = []
        for i in range(1, len(closes)):
            prev_close = closes[i - 1]
            close = closes[i]
            daily_return = ((close / prev_close) - 1.0) * 100.0 if prev_close else 0.0
            cumulative_return = ((close / closes[0]) - 1.0) * 100.0 if closes[0] else 0.0
            points.append({
                "date": f"D-{6-i}",
                "close": round(close, 2),
                "daily_return": round(daily_return, 2),
                "cumulative_return": round(cumulative_return, 2),
            })

        total_return = points[-1]["cumulative_return"]
        if total_return >= 1.0:
            score = 2
        elif total_return >= 0.3:
            score = 1
        elif total_return <= -1.0:
            score = -2
        elif total_return <= -0.3:
            score = -1
        else:
            score = 0

        return {
            "index_name": index_name,
            "score": score,
            "detail": f"{index_name} 최근 5거래일 {total_return:+.2f}%",
            "total_return": round(total_return, 2),
            "points": points,
            "source": "Naver/KRX 현재값 + 실제 지수 일봉 흐름",
        }
    except Exception as exc:
        print(f"[MARKET 5D] {index_name} failed: {exc}")
        return None
