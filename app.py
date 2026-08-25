import json
import os
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse
import xml.etree.ElementTree as ET

import requests

HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", "10000"))
TIMEOUT = 8

DOMESTIC_STOCKS = {
    "SK하이닉스": ("000660", "SK하이닉스"),
    "하이닉스": ("000660", "SK하이닉스"),
    "삼성전자": ("005930", "삼성전자"),
    "삼전": ("005930", "삼성전자"),
}
US_STOCKS = {
    "엔비디아": ("NVDA", "엔비디아"),
    "NVDA": ("NVDA", "엔비디아"),
    "테슬라": ("TSLA", "테슬라"),
    "TSLA": ("TSLA", "테슬라"),
    "애플": ("AAPL", "애플"),
    "AAPL": ("AAPL", "애플"),
    "마이크로소프트": ("MSFT", "마이크로소프트"),
    "MSFT": ("MSFT", "마이크로소프트"),
}
CRYPTO = {
    "비트코인": ("BTCUSDT", "비트코인"),
    "BTC": ("BTCUSDT", "비트코인"),
    "BTCUSDT": ("BTCUSDT", "비트코인"),
    "이더리움": ("ETHUSDT", "이더리움"),
    "ETH": ("ETHUSDT", "이더리움"),
    "ETHUSDT": ("ETHUSDT", "이더리움"),
}


def clean_query(text):
    return str(text or "").strip().replace(" ", "").upper()


def now_kst():
    from datetime import timedelta
    return datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=9))).strftime("%Y-%m-%d %H:%M:%S KST")


def find_asset(query):
    key = clean_query(query)
    for name, (symbol, formal) in DOMESTIC_STOCKS.items():
        if clean_query(name) == key:
            return {"kind": "domestic", "market": "국내주식", "symbol": symbol, "name": formal}
    for name, (symbol, formal) in US_STOCKS.items():
        if clean_query(name) == key:
            return {"kind": "us", "market": "미국주식", "symbol": symbol, "name": formal}
    for name, (symbol, formal) in CRYPTO.items():
        if clean_query(name) == key:
            return {"kind": "crypto", "market": "암호화폐", "symbol": symbol, "name": formal}
    return None


def get_naver_price(code):
    url = f"https://polling.finance.naver.com/api/realtime/domestic/stock/{code}"
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=TIMEOUT)
    r.raise_for_status()
    rows = r.json().get("datas", [])
    if not rows:
        raise ValueError("국내주식 가격 데이터가 없습니다.")
    raw = str(rows[0].get("closePrice", "")).replace(",", "").strip()
    if not raw:
        raise ValueError("현재가 데이터가 없습니다.")
    return f"{int(float(raw)):,} 원", "Naver"


def get_yahoo_price(ticker):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1d&interval=1m&includePrePost=true"
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=TIMEOUT)
    r.raise_for_status()
    result = (r.json().get("chart") or {}).get("result") or []
    if not result:
        raise ValueError("미국주식 가격 데이터가 없습니다.")
    meta = result[0].get("meta") or {}
    price = meta.get("regularMarketPrice") or meta.get("previousClose")
    if price is None:
        closes = (((result[0].get("indicators") or {}).get("quote") or [{}])[0].get("close") or [])
        closes = [x for x in closes if x is not None]
        if not closes:
            raise ValueError("현재가 데이터가 없습니다.")
        price = closes[-1]
    return f"{float(price):,.2f} USD", "Yahoo Finance"


def get_binance_price(symbol):
    url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=TIMEOUT)
    r.raise_for_status()
    price = r.json().get("price")
    if price is None:
        raise ValueError("암호화폐 가격 데이터가 없습니다.")
    return f"{float(price):,.2f} USDT", "Binance"


def get_price(asset):
    if asset["kind"] == "domestic":
        return get_naver_price(asset["symbol"])
    if asset["kind"] == "us":
        return get_yahoo_price(asset["symbol"])
    return get_binance_price(asset["symbol"])


def get_yahoo_history(ticker, range_value="1y", interval="1d"):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range={range_value}&interval={interval}&includePrePost=false"
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=TIMEOUT)
    r.raise_for_status()
    result = (r.json().get("chart") or {}).get("result") or []
    if not result:
        raise ValueError("과거 가격 데이터가 없습니다.")
    row = result[0]
    ts = row.get("timestamp") or []
    quote = ((row.get("indicators") or {}).get("quote") or [{}])[0]
    o = quote.get("open") or []
    h = quote.get("high") or []
    l = quote.get("low") or []
    c = quote.get("close") or []
    v = quote.get("volume") or []
    candles = []
    for i, t in enumerate(ts):
        try:
            vals = [float(o[i]), float(h[i]), float(l[i]), float(c[i])]
        except (IndexError, TypeError, ValueError):
            continue
        if not all(v == v for v in vals):
            continue
        vol = 0.0
        try:
            vol = float(v[i]) if v[i] is not None else 0.0
        except (IndexError, TypeError, ValueError):
            vol = 0.0
        candles.append({"t": int(t), "o": vals[0], "h": vals[1], "l": vals[2], "c": vals[3], "v": vol})
    if len(candles) < 10:
        raise ValueError("차트용 과거 데이터가 부족합니다.")
    return candles, "Yahoo Finance"


def get_crypto_history(symbol, limit=90):
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=1d&limit={limit}"
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=TIMEOUT)
    r.raise_for_status()
    rows = r.json()
    candles = []
    for row in rows:
        try:
            candles.append({"t": int(row[0] // 1000), "o": float(row[1]), "h": float(row[2]), "l": float(row[3]), "c": float(row[4]), "v": float(row[5])})
        except (IndexError, TypeError, ValueError):
            continue
    if len(candles) < 10:
        raise ValueError("차트용 과거 데이터가 부족합니다.")
    return candles, "Binance"


def get_history(asset):
    if asset["kind"] == "crypto":
        return get_crypto_history(asset["symbol"])
    ticker = asset["symbol"] if asset["kind"] == "us" else f'{asset["symbol"]}.KS'
    return get_yahoo_history(ticker)



def _finite_float(value):
    try:
        v = float(value)
        return v if v == v and abs(v) < 1e100 else None
    except (TypeError, ValueError):
        return None


def sma(values, period):
    if len(values) < period:
        return None
    vals = [_finite_float(v) for v in values[-period:]]
    if any(v is None for v in vals):
        return None
    return sum(vals) / period


def rsi_wilder(candles, period=14):
    closes = [_finite_float(c["c"]) for c in candles]
    if len(closes) < period + 1 or any(v is None for v in closes[-(period + 1):]):
        return None

    gains = []
    losses = []
    for i in range(1, len(closes)):
        delta = closes[i] - closes[i - 1]
        gains.append(max(delta, 0.0))
        losses.append(max(-delta, 0.0))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        avg_gain = ((avg_gain * (period - 1)) + gains[i]) / period
        avg_loss = ((avg_loss * (period - 1)) + losses[i]) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def atr_wilder(candles, period=14):
    if len(candles) < period + 1:
        return None

    trs = []
    prev_close = None
    for c in candles:
        h = _finite_float(c["h"])
        l = _finite_float(c["l"])
        cl = _finite_float(c["c"])
        if h is None or l is None or cl is None:
            continue
        if prev_close is None:
            tr = h - l
        else:
            tr = max(h - l, abs(h - prev_close), abs(l - prev_close))
        trs.append(tr)
        prev_close = cl

    if len(trs) < period:
        return None

    atr = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr = ((atr * (period - 1)) + tr) / period
    return atr


def volume_ratio(candles, lookback=5):
    if len(candles) < lookback + 1:
        return None
    current = _finite_float(candles[-1]["v"])
    prev = [_finite_float(c["v"]) for c in candles[-lookback-1:-1]]
    if current is None or any(v is None for v in prev):
        return None
    base = sum(prev) / lookback
    if base <= 0:
        return None
    return current / base * 100.0


def build_indicators(candles):
    closes = [c["c"] for c in candles]
    current = _finite_float(closes[-1]) if closes else None
    ma5 = sma(closes, 5)
    ma20 = sma(closes, 20)
    ma60 = sma(closes, 60)
    ma120 = sma(closes, 120)
    rsi14 = rsi_wilder(candles, 14)
    atr14 = atr_wilder(candles, 14)
    vol_ratio = volume_ratio(candles, 5)

    if current is None:
        trend = ""
    elif ma20 is not None and ma60 is not None:
        if current > ma20 > ma60:
            trend = "상승"
        elif current < ma20 < ma60:
            trend = "하락"
        else:
            trend = "혼조"
    else:
        trend = ""

    atr_pct = None
    if current and atr14 is not None and current != 0:
        atr_pct = atr14 / current * 100.0

    return {
        "ma5": ma5,
        "ma20": ma20,
        "ma60": ma60,
        "ma120": ma120,
        "rsi14": rsi14,
        "atr14": atr14,
        "atr_pct": atr_pct,
        "volume_ratio": vol_ratio,
        "trend": trend,
    }


def html_escape(value):
    return (str(value).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;").replace("'", "&#39;"))


def format_axis_price(value, market):
    if market == "국내주식":
        return f"{int(round(value)):,}"
    return f"{value:,.2f}"


def build_svg_chart(candles, market, width=760, height=390):
    data = candles[-60:]
    left, right, top, bottom = 62, 18, 18, 52
    cw, ch = width - left - right, height - top - bottom
    max_p = max(c["h"] for c in data)
    min_p = min(c["l"] for c in data)
    span = max(max_p - min_p, max_p * 0.01, 1.0)
    slot = cw / len(data)
    body_w = max(3, slot * 0.56)

    def xp(i):
        return left + slot * i + slot / 2

    def yp(v):
        return top + (max_p - v) / span * ch

    svg = [
        f'<svg viewBox="0 0 {width} {height}" width="100%" height="{height}" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="최근 3개월 일봉 캔들 차트">',
        '<rect width="100%" height="100%" rx="16" fill="#191a1c"/>'
    ]

    for frac in (0, 0.25, 0.5, 0.75, 1):
        yy = top + ch * frac
        val = max_p - span * frac
        svg.append(f'<line x1="{left}" y1="{yy:.1f}" x2="{width-right}" y2="{yy:.1f}" stroke="#34363a" stroke-width="1"/>')
        svg.append(f'<text x="8" y="{yy+4:.1f}" fill="#8e9297" font-size="11">{html_escape(format_axis_price(val, market))}</text>')

    closes = [c["c"] for c in data]
    ma20 = []
    for i in range(len(closes)):
        s = max(0, i - 19)
        ma20.append(sum(closes[s:i+1]) / (i - s + 1))

    for i, c in enumerate(data):
        xx = xp(i)
        up = c["c"] >= c["o"]
        color = "#3b82f6" if up else "#ef4444"
        svg.append(f'<line x1="{xx:.1f}" y1="{yp(c["h"]):.1f}" x2="{xx:.1f}" y2="{yp(c["l"]):.1f}" stroke="{color}" stroke-width="1.2"/>')
        body_top = min(yp(c["o"]), yp(c["c"]))
        body_h = max(abs(yp(c["c"]) - yp(c["o"])), 1.8)
        svg.append(f'<rect x="{xx-body_w/2:.1f}" y="{body_top:.1f}" width="{body_w:.1f}" height="{body_h:.1f}" rx="1" fill="{color}"/>')

    ma_points = " ".join(f"{xp(i):.1f},{yp(v):.1f}" for i, v in enumerate(ma20))
    svg.append(f'<polyline fill="none" stroke="#49d6c9" stroke-width="2.3" points="{ma_points}"/>')

    last_y = yp(data[-1]["c"])
    svg.append(f'<line x1="{left}" y1="{last_y:.1f}" x2="{width-right}" y2="{last_y:.1f}" stroke="#d0d3d6" stroke-dasharray="4 4" stroke-width="1"/>')
    svg.append(f'<text x="{width-right-4}" y="{last_y-7:.1f}" text-anchor="end" fill="#d0d3d6" font-size="12">{html_escape(format_axis_price(data[-1]["c"], market))}</text>')

    for idx in sorted({0, len(data)//3, (2*len(data))//3, len(data)-1}):
        dt = datetime.fromtimestamp(data[idx]["t"], tz=timezone.utc)
        svg.append(f'<text x="{xp(idx):.1f}" y="{height-16}" text-anchor="middle" fill="#777c82" font-size="10">{dt.strftime("%m/%d")}</text>')

    svg.append('</svg>')
    return ''.join(svg)


HTML = """<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>gaemiGTP</title>
<style>
*{box-sizing:border-box}
body{margin:0;min-height:100vh;background:#1f2022;color:#f3f3f3;font-family:Arial,"Noto Sans KR",sans-serif}
.page{min-height:100vh;display:flex;flex-direction:column;align-items:center;padding:42px 20px 60px}
.logo{font-size:34px;font-weight:800;letter-spacing:-1px;margin-top:10px}.sub{margin-top:8px;color:#96999d;font-size:14px;text-align:center}
.search{width:min(760px,100%);display:flex;gap:10px;margin-top:26px} input{flex:1;background:#2a2c2f;color:#fff;border:1px solid #3b3d40;border-radius:14px;padding:16px 18px;font-size:16px;outline:none}
button{border:0;border-radius:14px;background:#f1f1f1;color:#111;padding:0 24px;font-size:16px;font-weight:700;cursor:pointer}
.card{width:min(760px,100%);margin-top:22px;background:#2a2c2f;border-radius:18px;padding:24px}.title{font-size:22px;font-weight:800;margin-bottom:14px}.price{font-size:34px;font-weight:800;margin-bottom:8px}.meta{margin-top:8px;color:#a2a5aa;font-size:14px}.error{color:#ffb4b4}.section-title{font-size:17px;font-weight:800;margin-bottom:14px}.trend{margin-bottom:14px;color:#a5a9ae}.metrics{display:grid;grid-template-columns:repeat(2,1fr);gap:10px;margin-top:10px}.metric{background:#222427;border-radius:12px;padding:12px}.metric span{display:block;color:#8d9298;font-size:12px;margin-bottom:6px}.metric strong{font-size:15px}.news-item{padding:14px 0;border-top:1px solid #3a3c40}.news-item:first-child{border-top:0;padding-top:0}.news-title{font-size:15px;line-height:1.55}.news-meta{margin-top:7px;color:#8c9095;font-size:12px;line-height:1.45}.news-meta a{color:#b9c0c6;text-decoration:none;margin-left:8px}.small{margin-top:18px;color:#777b80;font-size:12px;line-height:1.6}.chart-wrap{width:min(760px,100%);margin-top:18px;background:#191a1c;border-radius:18px;padding:10px;overflow:hidden}.legend{width:min(760px,100%);display:flex;gap:16px;align-items:center;margin:14px 2px 0;color:#8f9398;font-size:12px}.dot{display:inline-block;width:9px;height:9px;border-radius:50%;margin-right:6px}
</style>
</head><body><div class="page">
<div class="logo">gaemiGTP</div><div class="sub">종목 하나로 시작하는 금융 분석 플랫폼</div>
<form class="search" action="/search" method="get"><input name="q" value="{QUERY}" placeholder="예: SK하이닉스, NVDA, BTC" autocomplete="off"><button type="submit">분석</button></form>
{RESULT}</div></body></html>"""



def make_news_queries(asset):
    name = asset["name"]
    symbol = asset["symbol"]
    market = asset["market"]

    queries = [name, f"{name} {symbol}"]
    if market == "미국주식":
        queries.extend([symbol, f"{symbol} stock", f"{name} stock"])
    elif market == "암호화폐":
        queries.extend([symbol, f"{name} crypto", f"{symbol} crypto"])

    # preserve order / remove duplicates
    result = []
    seen = set()
    for q in queries:
        q = q.strip()
        if q and q.lower() not in seen:
            seen.add(q.lower())
            result.append(q)
    return result


def parse_rss_items(xml_bytes, limit=5):
    root = ET.fromstring(xml_bytes)
    items = []
    for item in root.findall(".//item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        pub_date = (item.findtext("pubDate") or "").strip()
        source_node = item.find("source")
        source = (source_node.text or "").strip() if source_node is not None else ""

        if not title:
            continue

        items.append({
            "title": title,
            "link": link,
            "published": pub_date,
            "source": source,
        })

        if len(items) >= limit:
            break
    return items


def fetch_google_news_rss(query, limit=5):
    url = (
        "https://news.google.com/rss/search?"
        f"q={requests.utils.quote(query)}&hl=ko&gl=KR&ceid=KR:ko"
    )
    response = requests.get(
        url,
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=TIMEOUT,
    )
    response.raise_for_status()
    return parse_rss_items(response.content, limit=limit)


def fetch_google_news_rss_us(query, limit=5):
    url = (
        "https://news.google.com/rss/search?"
        f"q={requests.utils.quote(query)}&hl=en-US&gl=US&ceid=US:en"
    )
    response = requests.get(
        url,
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=TIMEOUT,
    )
    response.raise_for_status()
    return parse_rss_items(response.content, limit=limit)


def fetch_yahoo_news_rss(ticker, limit=5):
    url = (
        "https://feeds.finance.yahoo.com/rss/2.0/headline?"
        f"s={requests.utils.quote(ticker)}&region=US&lang=en-US"
    )
    response = requests.get(
        url,
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=TIMEOUT,
    )
    response.raise_for_status()
    return parse_rss_items(response.content, limit=limit)


def normalize_news(items):
    cleaned = []
    seen = set()

    for item in items:
        title = re.sub(r"\s+", " ", item.get("title", "")).strip()
        if not title:
            continue

        # Google News titles can contain "Title - Source".
        source = (item.get("source") or "").strip()
        if not source and " - " in title:
            left, right = title.rsplit(" - ", 1)
            if len(right) <= 80:
                title, source = left.strip(), right.strip()

        key = re.sub(r"[^0-9a-zA-Z가-힣]", "", title).lower()
        if not key or key in seen:
            continue

        seen.add(key)
        cleaned.append({
            "title": title,
            "source": source,
            "published": item.get("published", ""),
            "link": item.get("link", ""),
        })

    return cleaned[:5]


def collect_free_news(asset, limit=5):
    """
    Free/no-key news collection.
    Priority:
      - US stocks: Yahoo Finance RSS first, Google News US fallback
      - Korean stocks: Google News Korean RSS
      - Crypto: Google News Korean RSS + US fallback
    If all sources fail, return [] rather than inventing a headline.
    """
    all_items = []

    try:
        if asset["market"] == "미국주식":
            all_items.extend(fetch_yahoo_news_rss(asset["symbol"], limit=limit))
            if len(all_items) < limit:
                all_items.extend(
                    fetch_google_news_rss_us(
                        f"{asset['symbol']} stock",
                        limit=limit,
                    )
                )
        else:
            queries = make_news_queries(asset)
            for q in queries:
                try:
                    all_items.extend(fetch_google_news_rss(q, limit=limit))
                except Exception as exc:
                    print(f"[NEWS SOURCE ERROR] query={q}: {exc}")
                if len(normalize_news(all_items)) >= limit:
                    break

            # Extra English fallback for crypto / popular symbols.
            if len(all_items) < limit and asset["market"] == "암호화폐":
                try:
                    all_items.extend(
                        fetch_google_news_rss_us(
                            f"{asset['symbol']} crypto",
                            limit=limit,
                        )
                    )
                except Exception as exc:
                    print(f"[NEWS US FALLBACK ERROR] {asset['symbol']}: {exc}")

        final_items = normalize_news(all_items)
        print(f"[NEWS] {asset['symbol']}: {len(final_items)} items")
        return final_items[:limit]

    except Exception as exc:
        print(f"[NEWS ERROR] {asset['symbol']}: {exc}")
        return []


def render_news_block(items):
    if not items:
        return (
            '<div class="card">'
            '<div class="section-title">최근 뉴스</div>'
            '<div class="meta">현재 무료 뉴스 소스에서 확인되는 뉴스가 없습니다.</div>'
            '</div>'
        )

    cards = []
    for item in items:
        title = html_escape(item["title"])
        source = html_escape(item.get("source") or "출처 미상")
        published = html_escape(item.get("published") or "")
        link = item.get("link") or ""
        link_html = (
            f'<a href="{html_escape(link)}" target="_blank" rel="noopener noreferrer">원문</a>'
            if link else ""
        )

        time_part = f' · {published}' if published else ""
        cards.append(
            '<div class="news-item">'
            f'<div class="news-title">{title}</div>'
            f'<div class="news-meta">{source}{time_part} {link_html}</div>'
            '</div>'
        )

    return (
        '<div class="card">'
        '<div class="section-title">최근 뉴스</div>'
        + "".join(cards)
        + '</div>'
    )



def render_page(query="", result=None):
    result_html = ""
    if result:
        if result.get("success"):
            ind = result.get("indicators", {})

            def fmt_indicator(v, suffix=""):
                if v is None:
                    return ""
                return f"{v:,.2f}{suffix}"

            ma_rows = []
            for label, key in (("5일선", "ma5"), ("20일선", "ma20"), ("60일선", "ma60"), ("120일선", "ma120")):
                value = ind.get(key)
                if value is None:
                    display = ""
                elif result["market"] == "국내주식":
                    display = f"{int(round(value)):,}원"
                elif result["market"] == "암호화폐":
                    display = f"{value:,.2f} USDT"
                else:
                    display = f"{value:,.2f} USD"
                ma_rows.append(f'<div class="metric"><span>{label}</span><strong>{html_escape(display)}</strong></div>')

            rsi_text = fmt_indicator(ind.get("rsi14"))
            atr_text = ""
            if ind.get("atr14") is not None:
                if result["market"] == "국내주식":
                    atr_text = f'{int(round(ind["atr14"])):,}원'
                elif result["market"] == "암호화폐":
                    atr_text = f'{ind["atr14"]:,.2f} USDT'
                else:
                    atr_text = f'{ind["atr14"]:,.2f} USD'
            atr_pct_text = fmt_indicator(ind.get("atr_pct"), "%")
            vol_text = fmt_indicator(ind.get("volume_ratio"), "%")
            trend_text = html_escape(ind.get("trend") or "")

            result_html = (
                '<div class="card">'
                f'<div class="title">{html_escape(result["name"])} <span style="color:#777;font-weight:500">({html_escape(result["symbol"])})</span></div>'
                f'<div class="price">{html_escape(result["price_text"])}</div>'
                f'<div class="meta">시장: {html_escape(result["market"])} · 현재가 데이터: {html_escape(result["price_source"])}</div>'
                f'<div class="meta">조회시각: {html_escape(result["as_of"])}</div>'
                '</div>'
                f'<div class="chart-wrap">{result["chart_svg"]}</div>'
                '<div class="legend"><span><i class="dot" style="background:#49d6c9"></i>20일 이동평균</span><span>최근 1년 일봉 데이터</span></div>'
                '<div class="card">'
                '<div class="section-title">퀀트 기술지표</div>'
                f'<div class="trend">추세: <strong>{trend_text}</strong></div>'
                '<div class="metrics">'
                + "".join(ma_rows)
                + '</div>'
                '<div class="metrics">'
                f'<div class="metric"><span>RSI(14)</span><strong>{html_escape(rsi_text)}</strong></div>'
                f'<div class="metric"><span>ATR(14)</span><strong>{html_escape(atr_text)}</strong></div>'
                f'<div class="metric"><span>ATR 비율</span><strong>{html_escape(atr_pct_text)}</strong></div>'
                f'<div class="metric"><span>거래량/직전5일 평균</span><strong>{html_escape(vol_text)}</strong></div>'
                '</div>'
                f'<div class="small">차트 데이터: {html_escape(result["chart_source"])} · RSI/ATR은 실제 캔들에서 계산합니다.</div>'
                '</div>'
            )
            result_html += render_news_block(result.get("news", []))

        else:
            result_html = f'<div class="card"><div class="title error">데이터 조회 실패</div><div class="meta">{html_escape(result.get("error", "시장 데이터를 가져오지 못했습니다."))}</div></div>'
    return HTML.replace("{QUERY}", html_escape(query)).replace("{RESULT}", result_html)


def analyze_query(query):
    q = (query or "").strip()
    if not q:
        return {"success": False, "error": "종목명을 입력해주세요."}
    asset = find_asset(q)
    if not asset:
        return {"success": False, "error": "현재 개발 단계에서 지원하지 않는 종목입니다."}
    try:
        price_text, price_source = get_price(asset)
        candles, chart_source = get_history(asset)
        indicators = build_indicators(candles)
        return {
            "success": True,
            "name": asset["name"], "symbol": asset["symbol"], "market": asset["market"],
            "price_text": price_text, "price_source": price_source,
            "as_of": now_kst(), "chart_source": chart_source,
            "chart_svg": build_svg_chart(candles, asset["market"]),
            "indicators": indicators,
            "news": collect_free_news(asset, limit=5),
        }
    except Exception as exc:
        print(f"[ANALYSIS ERROR] {asset['symbol']}: {exc}")
        return {"success": False, "error": "차트 또는 시장 데이터를 가져오지 못했습니다. 잠시 후 다시 시도해주세요."}


class Handler(BaseHTTPRequestHandler):
    def send_bytes(self, status, content_type, body):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def send_json(self, status, payload):
        self.send_bytes(status, "application/json; charset=utf-8", json.dumps(payload, ensure_ascii=False).encode("utf-8"))

    def do_GET(self):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)
        if parsed.path == "/":
            self.send_bytes(200, "text/html; charset=utf-8", render_page().encode("utf-8")); return
        if parsed.path == "/search":
            q = params.get("q", [""])[0].strip()
            result = analyze_query(q)
            self.send_bytes(200 if result.get("success") else 502, "text/html; charset=utf-8", render_page(q, result).encode("utf-8")); return
        if parsed.path == "/health":
            self.send_json(200, {"status":"ok", "service":"gaemiGTP-development"}); return

        if parsed.path == "/api/news":
            q = params.get("q", [""])[0].strip()
            asset = find_asset(q) if q else None
            if not asset:
                self.send_json(404, {"success": False, "news": []})
                return
            news = collect_free_news(asset, limit=5)
            self.send_json(200, {
                "success": True,
                "name": asset["name"],
                "symbol": asset["symbol"],
                "news": news,
            })
            return

        if parsed.path == "/api/price":
            q = params.get("q", [""])[0].strip()
            result = analyze_query(q)
            self.send_json(200 if result.get("success") else 502, result); return
        self.send_json(404, {"error":"요청을 찾을 수 없습니다."})

    def log_message(self, fmt, *args):
        print("[WEB]", fmt % args)


def main():
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"gaemiGTP development server started on port {PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
