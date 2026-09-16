"""Right market board - US rows.
Separated from bot.py without changing row-building logic.
"""

try:
    from .performance import HTTP_SESSION
except ImportError:
    from performance import HTTP_SESSION

def _right_sidebar_us_rows(limit=15):
    """해외주식 실시간 목록.
    Yahoo의 most_actives screener 응답 자체에 포함된 현재가/등락률/거래량을 사용한다.
    별도의 v7 quote 호출은 현재 Yahoo의 인증(crumb) 정책 때문에 실패할 수 있어 사용하지 않는다.
    """
    screener_urls = [
        "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?scrIds=most_actives&count=50",
        "https://query2.finance.yahoo.com/v1/finance/screener/predefined/saved?scrIds=most_actives&count=50",
    ]
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/151.0 Safari/537.36",
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://finance.yahoo.com/research-hub/screener/most-active/",
    }

    quotes = []
    last_error = None
    for screener_url in screener_urls:
        try:
            sr = HTTP_SESSION.get(screener_url, timeout=4.0, headers=headers)
            sr.raise_for_status()
            sp = sr.json()
            result = ((sp.get("finance") or {}).get("result") or [])
            if result and isinstance(result[0], dict):
                quotes = result[0].get("quotes") or []
            if quotes:
                break
        except Exception as exc:
            last_error = exc

    if not quotes:
        if last_error:
            print(f"[RIGHT MARKET BOARD] US screener failed: {type(last_error).__name__}: {last_error}")
        return []

    rows = []
    for q in quotes:
        if not isinstance(q, dict):
            continue
        symbol = str(q.get("symbol") or "").upper().strip()
        if not symbol:
            continue

        # Screener 응답의 실시간 필드를 우선 사용한다.
        price = q.get("regularMarketPrice")
        prev = q.get("regularMarketPreviousClose")
        pct = q.get("regularMarketChangePercent")
        change = q.get("regularMarketChange")
        volume = q.get("regularMarketVolume")

        # 응답 버전에 따라 현재가가 marketPrice 형태로 내려오는 경우도 지원한다.
        if price is None:
            price = q.get("marketPrice")
        if prev is None:
            prev = q.get("previousClose")
        if volume is None:
            volume = q.get("dayVolume")
        if pct is None and price is not None and prev:
            try:
                pct = (float(price) - float(prev)) / float(prev) * 100.0
            except Exception:
                pct = None
        if change is None and price is not None and prev:
            try:
                change = float(price) - float(prev)
            except Exception:
                change = None

        try:
            price_f = float(price)
        except Exception:
            continue
        try:
            volume_f = float(volume or 0)
        except Exception:
            volume_f = 0.0

        # Yahoo screener의 most_actives는 거래량 기준이다.
        # 우측 메뉴의 표시 기준은 거래대금이므로 현재가 × 거래량으로 정렬한다.
        notional = price_f * volume_f
        try:
            pct_f = float(pct) if pct is not None else None
        except Exception:
            pct_f = None
        try:
            change_f = float(change) if change is not None else None
        except Exception:
            change_f = None

        rows.append({
            "rank": 0,
            "code": symbol,
            "symbol": symbol,
            "name": q.get("shortName") or q.get("longName") or symbol,
            "price": price_f,
            "change_pct": pct_f,
            "change": change_f,
            "trading_value": notional,
            "currency": q.get("currency") or "USD",
            "market": "해외",
            "source": "Yahoo Finance",
        })

    rows.sort(key=lambda r: r.get("trading_value") or 0, reverse=True)
    for i, row in enumerate(rows[:limit], 1):
        row["rank"] = i
    return rows[:limit]
