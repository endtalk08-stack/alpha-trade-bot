import urllib.parse

import numpy as np

from performance import HTTP_SESSION
from market_index import fetch_naver_realtime_index


def fetch_single_world_item(item):
    name = item["name"]
    sym = item["sym"]
    tag = item["tag"]
    sparkline = []

    # 국내 지수는 Naver/KRX 계열 endpoint를 사용한다.
    # Yahoo의 regularMarketPrice/chartPreviousClose는 국내 장중/마감 시점과
    # 데이터 갱신 시점이 어긋날 수 있어 KOSPI/KOSDAQ의 현재 등락률 계산에 쓰지 않는다.
    if sym in {"^KS11", "^KQ11"}:
        index_code = "KOSPI" if sym == "^KS11" else "KOSDAQ"
        live = fetch_naver_realtime_index(index_code)
        if live:
            curr_p = live["price"]
            change_rate = live["rate"]

            # 보조 미니차트는 기존 Yahoo historical close를 사용한다.
            # 현재값/등락률의 기준은 위 Naver/KRX 데이터로 고정한다.
            try:
                encoded_sym = urllib.parse.quote(sym)
                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{encoded_sym}?range=14d&interval=1d"
                yres = HTTP_SESSION.get(url, timeout=2.0).json()
                yresult = yres.get("chart", {}).get("result", [])
                if yresult:
                    raw_closes = yresult[0].get("indicators", {}).get("quote", [{}])[0].get("close", [])
                    sparkline = [
                        round(float(c), 2)
                        for c in raw_closes
                        if c is not None and not np.isnan(float(c))
                    ][-7:]
            except Exception:
                sparkline = []

            return {
                "name": name,
                "tag": tag,
                "price": f"{curr_p:,.2f}",
                "rate": f"{change_rate:+.2f}%",
                "up": bool(change_rate >= 0),
                "sparkline": sparkline,
                "source": "Naver/KRX",
                "market_status": live.get("market_status"),
                "traded_at": live.get("traded_at"),
            }

    # 해외 지수/원자재/환율은 기존 Yahoo 흐름을 유지한다.
    try:
        encoded_sym = urllib.parse.quote(sym)
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{encoded_sym}?range=7d&interval=1d"
        res = HTTP_SESSION.get(url, timeout=2.0).json()
        result = res.get("chart", {}).get("result", [])
        if result:
            meta = result[0].get("meta", {})
            curr_p = meta.get("regularMarketPrice", 0.0)
            prev_close = meta.get("chartPreviousClose", meta.get("previousClose", curr_p))
            change_rate = ((curr_p - prev_close) / prev_close) * 100 if prev_close > 0 else 0.0

            raw_closes = result[0].get("indicators", {}).get("quote", [{}])[0].get("close", [])
            sparkline = [
                round(float(c), 2)
                for c in raw_closes
                if c is not None and not np.isnan(float(c))
            ][-7:]

            price_str = f"{curr_p:,.1f}원" if "KRW" in sym else f"{curr_p:,.2f}"

            return {
                "name": name,
                "tag": tag,
                "price": price_str,
                "rate": f"{change_rate:+.2f}%",
                "up": bool(change_rate >= 0),
                "sparkline": sparkline,
                "source": "Yahoo Finance",
            }
    except Exception as exc:
        print(f"[WORLD BOARD] {sym} failed: {exc}")

    return {"name": name, "tag": tag, "price": "-", "rate": "-", "up": True, "sparkline": []}

