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
.card{width:min(760px,100%);margin-top:22px;background:#2a2c2f;border-radius:18px;padding:24px;display:none}
.title{font-size:22px;font-weight:800;margin-bottom:18px}
.price{font-size:34px;font-weight:800}
.meta{margin-top:10px;color:#a2a5aa;font-size:14px}
.error{color:#ffb4b4}
.small{margin-top:20px;color:#777b80;font-size:12px}
</style>
</head>
<body>
<div class="page">
  <div class="logo">gaemiGTP</div>
  <div class="sub">종목 하나로 시작하는 금융 분석 플랫폼</div>

  <div class="search">
    <input id="q" placeholder="예: SK하이닉스, NVDA, BTC" onkeydown="if(event.key==='Enter') analyze()">
    <button onclick="analyze()">분석</button>
  </div>

  <div class="status" id="status">실제 시장 데이터 연결 대기 중</div>
  <div class="card" id="card"></div>
</div>

<script>
async function analyze(){
  const q=document.getElementById('q').value.trim();
  const status=document.getElementById('status');
  const card=document.getElementById('card');
  if(!q){status.textContent='종목명을 입력해주세요.';return;}

  status.textContent='실제 가격 데이터를 가져오는 중...';
  card.style.display='none';
  card.className='card';

  try{
    const r=await fetch('/api/price?q='+encodeURIComponent(q),{cache:'no-store'});
    const data=await r.json();
    if(!r.ok || !data.success){
      status.textContent=data.error || '데이터를 가져오지 못했습니다.';
      return;
    }

    card.innerHTML=`
      <div class="title">${escapeHtml(data.name)} <span style="color:#777;font-weight:500">(${escapeHtml(data.symbol)})</span></div>
      <div class="price">${escapeHtml(data.price_text)}</div>
      <div class="meta">시장: ${escapeHtml(data.market)} · 데이터: ${escapeHtml(data.source)}</div>
      <div class="meta">조회시각: ${escapeHtml(data.as_of)}</div>
      <div class="small">현재 단계는 가격 데이터 연결 테스트입니다. 기술지표·차트·뉴스·ICT 분석은 다음 단계에서 연결합니다.</div>
    `;
    card.style.display='block';
    status.textContent='가격 데이터 연결 성공';
  }catch(e){
    status.textContent='서버 통신 오류가 발생했습니다.';
    card.className='card error';
  }
}

function escapeHtml(v){
  return String(v).replace(/[&<>'"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;","'":"&#39;","\"":"&quot;"}[c]));
}
</script>
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

        if parsed.path == "/":
            self.send_bytes(200, "text/html; charset=utf-8", HTML.encode("utf-8"))
            return

        if parsed.path == "/health":
            self.send_json(200, {"status": "ok", "service": "gaemiGTP-development"})
            return

        if parsed.path == "/api/price":
            q = parse_qs(parsed.query).get("q", [""])[0].strip()
            if not q:
                self.send_json(400, {"success": False, "error": "종목명을 입력해주세요."})
                return

            asset = find_asset(q)
            if not asset:
                self.send_json(404, {"success": False, "error": "현재 개발 단계에서 지원하지 않는 종목입니다."})
                return

            try:
                price_text, source = get_price(asset)
                self.send_json(200, {
                    "success": True,
                    "name": asset["name"],
                    "symbol": asset["symbol"],
                    "market": asset["market"],
                    "price_text": price_text,
                    "source": source,
                    "as_of": now_kst(),
                })
            except Exception as exc:
                print(f"[PRICE ERROR] {asset['symbol']}: {exc}")
                self.send_json(502, {
                    "success": False,
                    "error": "가격 데이터를 가져오지 못했습니다. 잠시 후 다시 시도해주세요."
                })
            return

        self.send_json(404, {"success": False, "error": "요청을 찾을 수 없습니다."})

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
