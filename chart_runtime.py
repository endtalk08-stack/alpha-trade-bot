"""Fast chart fetch wrappers."""

from overseas_market import fetch_overseas_candles

def _fetch_fast_domestic_chart(code):
    # STEP 3: 전체 분석에서 사용하는 국내 일봉 공용 캐시를 그대로 사용한다.
    # 기존의 별도 120일 Naver 호출을 제거한다.
    try:
        from bot import fetch_domestic_candles
        df = fetch_domestic_candles(code)
        return df.tail(120).copy() if df is not None else None
    except Exception as exc:
        print(f"[FAST DOMESTIC CHART] {code} failed: {type(exc).__name__}: {exc}")
        return None

def _fetch_fast_overseas_chart(symbol):
    # STEP 3: 전체 분석에서 사용하는 5년 Yahoo 일봉 공용 캐시를 그대로 사용한다.
    # 별도의 1년 Yahoo 호출을 제거해 /api/chart ↔ /api/analyze 중복 요청을 없앤다.
    key = str(symbol or "").strip().upper()
    if not key:
        return None
    try:
        _, _, df = fetch_overseas_candles(key)
        return df
    except Exception as exc:
        print(f"[FAST OVERSEAS CHART] {key} failed: {type(exc).__name__}: {exc}")
        return None
