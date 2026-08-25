import json
import os
import re
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import requests

HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", "10000"))
TIMEOUT = 8

DOMESTIC_STOCKS = {
    "SK하이닉스": "000660",
    "하이닉스": "000660",
    "삼성전자": "005930",
    "삼전": "005930",
}

US_STOCKS = {
    "엔비디아": "NVDA",
    "NVDA": "NVDA",
    "테슬라": "TSLA",
    "TSLA": "TSLA",
    "애플": "AAPL",
    "AAPL": "AAPL",
    "마이크로소프트": "MSFT",
    "MSFT": "MSFT",
}

CRYPTO = {
    "비트코인": "BTCUSDT",
    "BTC": "BTCUSDT",
    "BTCUSDT": "BTCUSDT",
    "이더리움": "ETHUSDT",
    "ETH": "ETHUSDT",
    "ETHUSDT": "ETHUSDT",
}

HTML = """<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>gaemiGTP</title>
<style>
*{box-sizing:border-box}
body{margin:0;min-height:100vh;background:#1f2022;color:#f3f3f3;font-family:Arial,"Noto Sans KR",sans-serif}
.page{min-height:100vh;display:flex;flex-direction:column;align-items:center;justify-content:center;padding:24px}
.logo{font-size:42px;font-weight:800;letter-spacing:-1px}
.sub{margin-top:10px;color:#96999d;font-size:15px;text-align:center}
.search{width:min(760px,100%);display:flex;gap:10px;margin-top:28px}
input{flex:1;background:#2a2c2f;color:#fff;border:1px solid #3b3d40;border-radius:14px;padding:17px 18px;font-size:16px;outline:none}
input:focus{border-color:#666}
button{border:0;border-radius:14px;background:#f1f1f1;color:#111;padding:0 24px;font-size:16px;font-weight:700;cursor:pointer}
.status{margin-top:18px;color:#888c91;font-size:14px;text-align:center;min-height:20px}
.card{width:min(760px,100%);margin-top:22px;background:#2a2c2f;border-radius:18px;padding:24px}
.title{font-size:22px;font-weight:800;margin-bottom:18px}
.price{font-size:34px;font-weight:800}
.meta{margin-top:10px;color:#a2a5aa;font-size:14px}
.error{color:#ffb4b4}
.small{margin-top:20px;color:#777b80;font-size:12px}
.link{margin-top:18px;display:inline-block;color:#bbb;text-decoration:none}
</style>
</head>
<body>
<div class="page">
  <div class="logo">gaemiGTP</div>
  <div class="sub">종목 하나로 시작하는 금융 분석 플랫폼</div>

  <form class="search" action="/search" method="get">
    <input id="q" name="q" value="{QUERY}" placeholder="예: SK하이닉스, NVDA, BTC" autocomplete="off">
    <button type="submit">분석</button>
  </form>

  {RESULT}

</div>
</body>
</html>
"""


def now_kst():
    return datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=9))).strftime("%Y-%m-%d %H:%M:%S KST")


def clean_query(q: str) -> str:
    return re.sub(r"\s+", "", q or "").upper()


def find_asset(q: str):
    raw = (q or "").strip()
    key = clean_query(raw)

    # Domestic stock code input
    if key.isdigit() and len(key) == 6:
        return {"market": "국내주식", "symbol": key, "name": key, "kind": "domestic"}

    for name, code in DOMESTIC_STOCKS.items():
        if clean_query(name) == key:
            return {"market": "국내주식", "symbol": code, "name": name, "kind": "domestic"}

    for name, ticker in US_STOCKS.items():
        if clean_query(name) == key:
            return {"market": "미국주식", "symbol": ticker, "name": name, "kind": "us"}

    for name, ticker in CRYPTO.items():
        if clean_query(name) == key:
            return {"market": "암호화폐", "symbol": ticker, "name": name, "kind": "crypto"}

    return None


def get_naver_price(code: str):
    url = f"https://polling.finance.naver.com/api/realtime/domestic/stock/{code}"
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(url, headers=headers, timeout=TIMEOUT)
    r.raise_for_status()
    data = r.json()
    rows = data.get("datas", [])
    if not rows:
        raise ValueError("국내주식 가격 데이터가 없습니다.")
    row = rows[0]
    price_raw = str(row.get("closePrice", "")).replace(",", "").strip()
    if not price_raw:
        raise ValueError("현재가 데이터가 없습니다.")
    price = float(price_raw)
    return f"{int(price):,} 원", "Naver"


def get_yahoo_price(ticker: str):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1d&interval=1m&includePrePost=true"
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(url, headers=headers, timeout=TIMEOUT)
    r.raise_for_status()
    data = r.json()
    result = (data.get("chart") or {}).get("result") or []
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
    currency = meta.get("currency") or "USD"
    return f"{float(price):,.2f} {currency}", "Yahoo Finance"


def get_binance_price(symbol: str):
    url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(url, headers=headers, timeout=TIMEOUT)
    r.raise_for_status()
    data = r.json()
    price = data.get("price")
    if price is None:
        raise ValueError("암호화폐 가격 데이터가 없습니다.")
    return f"{float(price):,.2f} USDT", "Binance"


def get_price(asset):
    if asset["kind"] == "domestic":
        return get_naver_price(asset["symbol"])
    if asset["kind"] == "us":
        return get_yahoo_price(asset["symbol"])
    if asset["kind"] == "crypto":
        return get_binance_price(asset["symbol"])
    raise ValueError("지원하지 않는 자산입니다.")




def html_escape(value: str) -> str:
    return (
        str(value)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def render_page(query="", result=None):
    q_safe = html_escape(query)
    result_html = ""

    if result:
        if result.get("success"):
            result_html = f"""
  <div class="card">
    <div class="title">{html_escape(result["name"])} <span style="color:#777;font-weight:500">({html_escape(result["symbol"])})</span></div>
    <div class="price">{html_escape(result["price_text"])}</div>
    <div class="meta">시장: {html_escape(result["market"])} · 데이터: {html_escape(result["source"])}</div>
    <div class="meta">조회시각: {html_escape(result["as_of"])}</div>
    <div class="small">현재 단계는 실제 가격 데이터 연결 테스트입니다. 기술지표·차트·뉴스·ICT 분석은 다음 단계에서 연결합니다.</div>
  </div>
"""
        else:
            result_html = f"""
  <div class="card">
    <div class="title error">데이터 조회 실패</div>
    <div class="meta">{html_escape(result.get("error", "가격 데이터를 가져오지 못했습니다."))}</div>
  </div>
"""

    return HTML.replace("{QUERY}", q_safe).replace("{RESULT}", result_html)


def analyze_query(query):
    q = (query or "").strip()
    if not q:
        return {"success": False, "error": "종목명을 입력해주세요."}

    asset = find_asset(q)
    if not asset:
        return {"success": False, "error": "현재 개발 단계에서 지원하지 않는 종목입니다."}

    try:
        price_text, source_name = get_price(asset)
        return {
            "success": True,
            "name": asset["name"],
            "symbol": asset["symbol"],
            "market": asset["market"],
            "price_text": price_text,
            "source": source_name,
            "as_of": now_kst(),
        }
    except Exception as exc:
        print(f"[PRICE ERROR] {asset['symbol']}: {exc}")
        return {
            "success": False,
            "error": "가격 데이터를 가져오지 못했습니다. 잠시 후 다시 시도해주세요.",
        }




class Handler(BaseHTTPRequestHandler):
    def send_bytes(self, status, content_type, body: bytes):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def send_json(self, status, payload):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_bytes(status, "application/json; charset=utf-8", body)

    def do_GET(self):
        parsed = urlparse(self.path)
        query_params = parse_qs(parsed.query)

        if parsed.path == "/":
            self.send_bytes(200, "text/html; charset=utf-8", render_page().encode("utf-8"))
            return

        if parsed.path == "/search":
            q = query_params.get("q", [""])[0].strip()
            result = analyze_query(q)
            status = 200 if result.get("success") else 502
            self.send_bytes(status, "text/html; charset=utf-8", render_page(q, result).encode("utf-8"))
            return

        if parsed.path == "/health":
            self.send_json(200, {"status": "ok", "service": "gaemiGTP-development"})
            return

        if parsed.path == "/api/price":
            q = query_params.get("q", [""])[0].strip()
            result = analyze_query(q)
            self.send_json(200 if result.get("success") else 502, result)
            return

        self.send_json(404, {"error": "요청을 찾을 수 없습니다."})

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
