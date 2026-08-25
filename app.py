import json
import os
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

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


def get_yahoo_history(ticker, range_value="3mo", interval="1d"):
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
    candles = []
    for i, t in enumerate(ts):
        try:
            vals = [float(o[i]), float(h[i]), float(l[i]), float(c[i])]
        except (IndexError, TypeError, ValueError):
            continue
        if not all(v == v for v in vals):
            continue
        candles.append({"t": int(t), "o": vals[0], "h": vals[1], "l": vals[2], "c": vals[3]})
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
            candles.append({"t": int(row[0] // 1000), "o": float(row[1]), "h": float(row[2]), "l": float(row[3]), "c": float(row[4])})
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
.card{width:min(760px,100%);margin-top:22px;background:#2a2c2f;border-radius:18px;padding:24px}.title{font-size:22px;font-weight:800;margin-bottom:14px}.price{font-size:34px;font-weight:800;margin-bottom:8px}.meta{margin-top:8px;color:#a2a5aa;font-size:14px}.error{color:#ffb4b4}.small{margin-top:18px;color:#777b80;font-size:12px;line-height:1.6}.chart-wrap{width:min(760px,100%);margin-top:18px;background:#191a1c;border-radius:18px;padding:10px;overflow:hidden}.legend{width:min(760px,100%);display:flex;gap:16px;align-items:center;margin:14px 2px 0;color:#8f9398;font-size:12px}.dot{display:inline-block;width:9px;height:9px;border-radius:50%;margin-right:6px}
</style>
</head><body><div class="page">
<div class="logo">gaemiGTP</div><div class="sub">종목 하나로 시작하는 금융 분석 플랫폼</div>
<form class="search" action="/search" method="get"><input name="q" value="{QUERY}" placeholder="예: SK하이닉스, NVDA, BTC" autocomplete="off"><button type="submit">분석</button></form>
{RESULT}</div></body></html>"""


def render_page(query="", result=None):
    result_html = ""
    if result:
        if result.get("success"):
            result_html = (
                '<div class="card">'
                f'<div class="title">{html_escape(result["name"])} <span style="color:#777;font-weight:500">({html_escape(result["symbol"])})</span></div>'
                f'<div class="price">{html_escape(result["price_text"])}</div>'
                f'<div class="meta">시장: {html_escape(result["market"])} · 현재가 데이터: {html_escape(result["price_source"])}</div>'
                f'<div class="meta">조회시각: {html_escape(result["as_of"])}</div>'
                '</div>'
                f'<div class="chart-wrap">{result["chart_svg"]}</div>'
                '<div class="legend"><span><i class="dot" style="background:#49d6c9"></i>20일 이동평균</span><span>최근 3개월 일봉</span></div>'
                '<div class="card"><div class="small">현재 단계는 실제 시장 가격과 일봉 차트를 연결하는 테스트입니다. RSI·ATR·뉴스·ICT 분석은 다음 단계에서 연결합니다.</div>'
                f'<div class="meta">차트 데이터: {html_escape(result["chart_source"])}</div></div>'
            )
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
        return {
            "success": True,
            "name": asset["name"], "symbol": asset["symbol"], "market": asset["market"],
            "price_text": price_text, "price_source": price_source,
            "as_of": now_kst(), "chart_source": chart_source,
            "chart_svg": build_svg_chart(candles, asset["market"]),
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
