import os
import re
import io
import gc
import json
import time
import urllib.parse
import xml.etree.ElementTree as ET
import html
import threading
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

import requests
import pandas as pd
import numpy as np

# ----------------------------------------------------
# 0. 고성능 네트워크 세션 (TCP 커넥션 풀 재사용)
# ----------------------------------------------------
HTTP_SESSION = requests.Session()
HTTP_SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Connection": "keep-alive"
})

# ----------------------------------------------------
# 1. 기본 설정 및 종목/지수 매핑
# ----------------------------------------------------
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
TELEGRAM_BOT_USERNAME = os.environ.get("TELEGRAM_BOT_USERNAME", "gaemigtp_bot").strip()
DART_API_KEY = os.environ.get("DART_API_KEY", "").strip()

GLOBAL_INDEX_MAP = {
    "코스피": "^KS11", "KOSPI": "^KS11",
    "코스닥": "^KQ11", "KOSDAQ": "^KQ11",
    "VIX": "^VIX", "빅스": "^VIX", "변동성": "^VIX",
    "WTI유가": "CL=F", "WTI": "CL=F", "유가": "CL=F", "원유": "CL=F",
    "금": "GC=F", "GOLD": "GC=F", "골드": "GC=F",
    "환율": "KRW=X", "USD/KRW": "KRW=X", "원달러": "KRW=X"
}

CRYPTO_MAP = {
    "BTC": "BTCUSDT", "비트코인": "BTCUSDT", "비트": "BTCUSDT",
    "ETH": "ETHUSDT", "이더리움": "ETHUSDT", "이더": "ETHUSDT",
    "XRP": "XRPUSDT", "리플": "XRPUSDT",
    "DOGE": "DOGEUSDT", "도지코인": "DOGEUSDT", "도지": "DOGEUSDT",
    "SOL": "SOLUSDT", "솔라나": "SOLUSDT",
    "ADA": "ADAUSDT", "에이다": "ADAUSDT",
    "AVAX": "AVAXUSDT", "아발란체": "AVAXUSDT",
    "SKY": "SKYUSDT", "스카이": "SKYUSDT"
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
    "QQQ": "QQQ", "SPY": "SPY", "SOXL": "SOXL", "TQQQ": "TQQQ",
    "DIA": "DIA", "SOXX": "SOXX"
}

DOMESTIC_FAST_MAP = {
    "SK하이닉스": "000660", "하이닉스": "000660",
    "삼성전자": "005930", "삼전": "005930", "삼성전기": "009150", "삼성중공업": "010140",
    "SK증권": "001510", "SK스퀘어": "402340", "솔트룩스": "304100",
    "알테오젠": "196170", "에코프로": "086520", "에코프로비엠": "247540",
    "레인보우로보틱스": "277810", "카카오": "035720", "네이버": "035420",
    "NAVER": "035420", "현대차": "005380", "기아": "000270",
    "포스코홀딩스": "005490", "POSCO홀딩스": "005490", "LG에너지솔루션": "373220",
    "한화오션": "042660", "두산에너빌리티": "034020", "한화에어로스페이스": "012450",
    "현대모비스": "012330", "셀트리온": "068270", "한미반도체": "042700",
    "씨젠": "096530", "HLB": "028300", "에코프로머티": "450080",
    "리가켐바이오": "141080", "엔켐": "348370", "삼천당제약": "000250",
    "휴젤": "145020", "클래시스": "214150", "리노공업": "058470"
}

WORLD_BOARD_CONFIG = [
    {"name": "코스피", "sym": "^KS11", "tag": "국내"},
    {"name": "코스닥", "sym": "^KQ11", "tag": "국내"},
    {"name": "QQQ", "sym": "QQQ", "tag": "나스닥100"},
    {"name": "SPY", "sym": "SPY", "tag": "S&P500"},
    {"name": "SOXX", "sym": "SOXX", "tag": "반도체"},
    {"name": "DIA", "sym": "DIA", "tag": "다우존스"},
    {"name": "VIX", "sym": "^VIX", "tag": "변동성"},
    {"name": "USD/KRW", "sym": "KRW=X", "tag": "환율"},
    {"name": "WTI 유가", "sym": "CL=F", "tag": "원유"},
    {"name": "금", "sym": "GC=F", "tag": "안전자산"}
]

# ----------------------------------------------------
# 2. 초고속 인메모리 캐시 & 백그라운드 워커
# ----------------------------------------------------
CACHED_WORLD_BOARD = []
ANALYSIS_CACHE = {}
MARKET_SCHEDULE_CACHE = {"ts": 0.0, "data": None}
SCHEDULE_LOCK = threading.Lock()

MAJOR_US_EARNINGS = [("NVDA","엔비디아"),("MSFT","마이크로소프트"),("AAPL","애플"),("AMZN","아마존"),("GOOGL","알파벳"),("META","메타"),("TSLA","테슬라"),("AVGO","브로드컴"),("AMD","AMD"),("TSM","TSMC"),("MU","마이크론"),("ORCL","오라클"),("NFLX","넷플릭스"),("JPM","JP모건"),("WMT","월마트"),("COST","코스트코"),("V","비자"),("MA","마스터카드")]

def _kst_now(): return datetime.now(timezone.utc).astimezone(ZoneInfo("Asia/Seoul"))

def _importance(event_name, country):
    name=(event_name or "").lower()
    high=["cpi","consumer price","pce","non-farm","payroll","unemployment rate","fed","fomc","interest rate","policy int","gdp","retail sales","ppi"]
    mid=["jobless","claims","pmi","ism","industrial production","consumer confidence","housing starts","durable goods","existing home","new home"]
    return 3 if any(k in name for k in high) else 2 if any(k in name for k in mid) else 1

def _stars(n): return "★"*int(n)+"☆"*(3-int(n))

def _norm(v):
    x=str(v).strip()
    return "-" if x.lower() in {"nan","none","-",""} else x

def _d_day(date_iso):
    try:
        d=datetime.fromisoformat(date_iso).date()
        delta=(d-_kst_now().date()).days
        if delta==0: return "TODAY"
        if delta==1: return "D-1"
        if delta>1: return f"D-{delta}"
        if delta==-1: return "D+1"
        return f"D+{abs(delta)}"
    except Exception:
        return "-"

def fetch_yahoo_economic_calendar(days=14):
    today=_kst_now().date(); rows=[]
    for i in range(max(1,min(days,14))):
        d=today+timedelta(days=i)
        url=f"https://finance.yahoo.com/calendar/economic/?day={d.isoformat()}"
        try:
            r=HTTP_SESSION.get(url,timeout=6,headers={"Accept-Language":"en-US,en;q=0.9"}); r.raise_for_status()
            tables=pd.read_html(io.StringIO(r.text))
            for df in tables:
                cols=[str(c).strip() for c in df.columns]
                if "Event" not in cols or "Country" not in cols: continue
                for _,row in df.iterrows():
                    event=_norm(row.get("Event","")); country=_norm(row.get("Country",""))
                    if event=="-": continue
                    imp=_importance(event,country)
                    # 핵심국(미국/한국/일본/중국/유럽/영국) 또는 중대 영향 이벤트만 표시
                    keep=country in {"US","KR","JP","CN","EU","DE","GB"} or imp>=2
                    if not keep: continue
                    t=_norm(row.get("Event Time","-"))
                    rows.append({
                        "date":d.isoformat(),
                        "d_day":_d_day(d.isoformat()),
                        "time_utc":t,
                        "country":country,
                        "event":event.replace(" *","").strip(),
                        "actual":_norm(row.get("Actual","-")),
                        "forecast":_norm(row.get("Market Expectation","-")),
                        "previous":_norm(row.get("Prior to This","-")),
                        "importance":imp,
                        "importance_label":_stars(imp),
                        "source":"Yahoo Finance"
                    })
                break
        except Exception as e:
            print(f"[ECON] {d} failed: {e}")
    rows.sort(key=lambda x:(x["date"],x["time_utc"],-x["importance"],x["country"],x["event"]))
    return rows[:180]

def _cap(v):
    m=re.search(r"([0-9,.]+)\s*([TtBbMm])",str(v))
    if not m: return 0.0
    try: return float(m.group(1).replace(',',''))*({"T":1e12,"B":1e9,"M":1e6}[m.group(2).upper()])
    except Exception: return 0.0

def fetch_yahoo_weekly_earnings(days=14):
    today=_kst_now().date(); out=[]
    for i in range(max(1,min(days,14))):
        d=today+timedelta(days=i); url=f"https://finance.yahoo.com/calendar/earnings/?day={d.isoformat()}"
        try:
            r=HTTP_SESSION.get(url,timeout=6,headers={"Accept-Language":"en-US,en;q=0.9"}); r.raise_for_status()
            tables=pd.read_html(io.StringIO(r.text))
            for df in tables:
                cols=[str(c).strip() for c in df.columns]
                if "Symbol" not in cols or "Company" not in cols: continue
                for _,row in df.iterrows():
                    sym=_norm(row.get("Symbol","")); company=_norm(row.get("Company",""))
                    if sym=="-": continue
                    out.append({
                        "date":d.isoformat(),
                        "d_day":_d_day(d.isoformat()),
                        "time":_norm(row.get("Earnings Call Time","-")),
                        "symbol":sym,
                        "company":company,
                        "event":_norm(row.get("Event Name","실적발표")),
                        "eps_est":_norm(row.get("EPS Estimate","-")),
                        "market_cap":_norm(row.get("Market Cap","-")),
                        "source":"Yahoo Finance",
                        "major": sym in {x[0] for x in MAJOR_US_EARNINGS}
                    })
                break
        except Exception as e:
            print(f"[EARNINGS] {d} failed: {e}")
    return out

def fetch_yahoo_major_earnings(days=30):
    today=_kst_now().date(); end=today+timedelta(days=days); out=[]
    def one(pair):
        sym,name=pair
        try:
            url=f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{sym}?modules=calendarEvents,price"
            r=HTTP_SESSION.get(url,timeout=4,headers={"Accept-Language":"en-US,en;q=0.9"}); r.raise_for_status()
            obj=(r.json().get("quoteSummary",{}).get("result") or [None])[0]
            ce=(obj or {}).get("calendarEvents") or {}
            dates=(ce.get("earnings") or {}).get("earningsDate",[])
            for dv in dates:
                raw=dv.get("raw") if isinstance(dv,dict) else dv
                if raw:
                    dt=datetime.fromtimestamp(float(raw),tz=timezone.utc).astimezone(ZoneInfo("Asia/Seoul"))
                    if today<=dt.date()<=end:
                        return {
                            "date":dt.date().isoformat(), "d_day":_d_day(dt.date().isoformat()),
                            "time":dt.strftime("%H:%M"), "symbol":sym, "company":name,
                            "event":"주요기업 실적발표", "eps_est":"-", "market_cap":"-",
                            "source":"Yahoo Finance", "major":True
                        }
        except Exception as e:
            print(f"[MAJOR EARNINGS] {sym} failed: {e}")
        return None
    with ThreadPoolExecutor(max_workers=10) as ex:
        for x in ex.map(one,MAJOR_US_EARNINGS):
            if x: out.append(x)
    return out

def get_market_schedule():
    with SCHEDULE_LOCK:
        if MARKET_SCHEDULE_CACHE["data"] is not None and time.time()-MARKET_SCHEDULE_CACHE["ts"]<300:
            return MARKET_SCHEDULE_CACHE["data"]
    economic=fetch_yahoo_economic_calendar(14)
    earnings=fetch_yahoo_weekly_earnings(14)
    # 주요 기업은 Yahoo 캘린더가 비어 있어도 개별 calendarEvents로 보강
    major=fetch_yahoo_major_earnings(30)
    merged={}
    for x in earnings+major:
        key=(x.get("date"),x.get("symbol"))
        old=merged.get(key)
        if old is None or (x.get("major") and not old.get("major")):
            merged[key]=x
    earnings=list(merged.values())
    earnings.sort(key=lambda x:(x["date"],0 if x.get("major") else 1,x.get("symbol","")))
    data={
        "updated_at":_kst_now().strftime("%Y-%m-%d %H:%M KST"),
        "economic":economic,
        "earnings":earnings[:80]
    }
    with SCHEDULE_LOCK:
        MARKET_SCHEDULE_CACHE.update({"ts":time.time(),"data":data})
    return data

def schedule_background_worker():
    while True:
        try: get_market_schedule()
        except Exception as e: print(f"[SCHEDULE] failed: {e}")
        time.sleep(300)
threading.Thread(target=schedule_background_worker,daemon=True).start()

def fetch_single_world_item(item):
    name = item["name"]
    sym = item["sym"]
    tag = item["tag"]
    sparkline = []
    try:
        encoded_sym = urllib.parse.quote(sym)
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{encoded_sym}?range=7d&interval=1d"
        res = HTTP_SESSION.get(url, timeout=1.8).json()
        result = res.get("chart", {}).get("result", [])
        if result:
            meta = result[0].get("meta", {})
            curr_p = meta.get("regularMarketPrice", 0.0)
            prev_close = meta.get("chartPreviousClose", meta.get("previousClose", curr_p))
            change_rate = ((curr_p - prev_close) / prev_close) * 100 if prev_close > 0 else 0.0
            
            raw_closes = result[0].get("indicators", {}).get("quote", [{}])[0].get("close", [])
            sparkline = [round(float(c), 2) for c in raw_closes if c is not None and not np.isnan(c)][-7:]
            
            if "KRW" in sym:
                price_str = f"{curr_p:,.1f}원"
            elif curr_p > 1000:
                price_str = f"{curr_p:,.2f}"
            else:
                price_str = f"{curr_p:,.2f}"

            return {
                "name": name,
                "tag": tag,
                "price": price_str,
                "rate": f"{change_rate:+.2f}%",
                "up": change_rate >= 0,
                "sparkline": sparkline
            }
    except Exception:
        pass
    return {"name": name, "tag": tag, "price": "-", "rate": "-", "up": True, "sparkline": []}

def background_world_board_worker():
    global CACHED_WORLD_BOARD
    while True:
        try:
            with ThreadPoolExecutor(max_workers=10) as executor:
                results = list(executor.map(fetch_single_world_item, WORLD_BOARD_CONFIG))
            if results and any(r["price"] != "-" for r in results):
                CACHED_WORLD_BOARD = results
        except Exception:
            pass
        time.sleep(25)

threading.Thread(target=background_world_board_worker, daemon=True).start()

# ----------------------------------------------------
# 3. 데이터 크롤러 모듈
# ----------------------------------------------------
def fetch_rss_news(query, limit=5):
    """Fetch real news headlines from Google News RSS. No fabricated fallback."""
    q = (query or "").strip()
    if not q:
        return []
    cache_key = f"news:{q}:{limit}"
    cached = ANALYSIS_CACHE.get(cache_key)
    if cached and time.time() - cached[0] < 60:
        return cached[1]
    items_out = []
    try:
        rss_url = "https://news.google.com/rss/search?q=" + urllib.parse.quote(q) + "&hl=ko&gl=KR&ceid=KR:ko"
        res = HTTP_SESSION.get(rss_url, timeout=3.5)
        res.raise_for_status()
        root = ET.fromstring(res.content)
        for item in root.findall("./channel/item")[:max(1, min(limit, 10))]:
            title = (item.findtext("title") or "").strip()
            link = (item.findtext("link") or "").strip()
            pub_date = (item.findtext("pubDate") or "").strip()
            source_el = item.find("source")
            media = source_el.text.strip() if source_el is not None and source_el.text else "Google News"
            if title:
                items_out.append({"title": html.unescape(title), "link": link, "pubDate": pub_date, "media": media})
    except Exception as exc:
        print(f"[NEWS] fetch failed: {exc}")
    ANALYSIS_CACHE[cache_key] = (time.time(), items_out)
    return items_out


def fetch_market_consensus_unified(symbol, market_name, raw_price, stock_name=""):
    """Best-effort real consensus data. Returns None rather than inventing numbers."""
    sym = (symbol or "").upper().strip()
    if market_name != "미국주식":
        return None
    try:
        url = f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{urllib.parse.quote(sym)}?modules=financialData,recommendationTrend,price"
        res = HTTP_SESSION.get(url, timeout=3.5)
        res.raise_for_status()
        root = res.json().get("quoteSummary", {}).get("result") or []
        if not root:
            return None
        obj = root[0]
        fd = obj.get("financialData") or {}
        trend = obj.get("recommendationTrend") or {}
        recs = (trend.get("trend") or [{}])
        rec = recs[0] if recs else {}
        target_mean = (fd.get("targetMeanPrice") or {}).get("raw")
        target_high = (fd.get("targetHighPrice") or {}).get("raw")
        target_low = (fd.get("targetLowPrice") or {}).get("raw")
        buy = rec.get("strongBuy", 0) + rec.get("buy", 0)
        hold = rec.get("hold", 0)
        sell = rec.get("sell", 0) + rec.get("strongSell", 0)
        total = buy + hold + sell
        if target_mean is None and total == 0:
            return None
        labels = []
        if target_mean is not None:
            labels.append(f"목표주가 평균 ${float(target_mean):,.2f}")
        if target_low is not None and target_high is not None:
            labels.append(f"범위 ${float(target_low):,.2f} ~ ${float(target_high):,.2f}")
        gauge = "-"
        badge = "실제 컨센서스"
        if total > 0:
            bull = (buy / total) * 100
            gauge = f"{bull:.0f}%"
            if bull >= 60:
                badge = "매수 의견 우세"
            elif bull <= 40:
                badge = "보수적 의견"
            else:
                badge = "혼합 의견"
        return {
            "title": f"{stock_name or sym} 월가 컨센서스",
            "gauge_val": gauge,
            "gauge_label": "Buy 비중" if total > 0 else "실측값",
            "status_label": "실제 Yahoo Finance 컨센서스 데이터",
            "badge_text": badge,
            "dots": f"매수 {buy} / 보유 {hold} / 매도 {sell}" if total > 0 else "애널리스트 의견 집계 없음",
            "primary_label": "목표주가 평균",
            "primary_val": f"${float(target_mean):,.2f}" if target_mean is not None else "-",
            "secondary_label": "현재가 대비 평균",
            "secondary_val": f"{((float(target_mean)/float(raw_price)-1)*100):+.1f}%" if target_mean is not None and raw_price else "-",
            "slider_left": f"저점 ${float(target_low):,.2f}" if target_low is not None else "저점 -",
            "slider_mid": "평균 목표",
            "slider_right": f"고점 ${float(target_high):,.2f}" if target_high is not None else "고점 -",
            "slider_bottom_left": "",
            "slider_bottom_right": "",
            "fill_pct": min(92, max(8, round(((float(raw_price)-float(target_low))/(float(target_high)-float(target_low)))*100))) if target_mean is not None and target_low is not None and target_high is not None and float(target_high)>float(target_low) else 50,
            "list_title": "실제 애널리스트 의견",
            "source_tag": "Yahoo Finance",
            "items": [
                {"main":"Buy","sub":"Strong Buy + Buy","val":str(buy),"sub_val":""},
                {"main":"Hold","sub":"Hold","val":str(hold),"sub_val":""},
                {"main":"Sell","sub":"Sell + Strong Sell","val":str(sell),"sub_val":""},
            ],
        }
    except Exception:
        return None


def fetch_all_world_board_parallel():
    try:
        with ThreadPoolExecutor(max_workers=len(WORLD_BOARD_CONFIG)) as executor:
            results = list(executor.map(fetch_single_world_item, WORLD_BOARD_CONFIG))
        return results
    except Exception:
        return []


def get_trading_val_rank(symbol, is_krw):
    """Rank is not available from the free data source; never invent one."""
    return ""

def fetch_dart_disclosures(stock_code, is_krw=True, limit=5):
    if not is_krw or not stock_code or not stock_code.isdigit():
        return None

    disclosures = []
    try:
        url = f"https://m.stock.naver.com/api/stock/{stock_code}/disclosure?page=1&pageSize={limit}"
        res = HTTP_SESSION.get(url, timeout=1.8).json()
        items = res.get("items", []) or res.get("disclosures", [])
        for it in items[:limit]:
            title = it.get("reportTitle") or it.get("title", "")
            date_raw = str(it.get("rceptDt") or it.get("date", ""))[:10].replace("-", ".")
            art_id = it.get("articleId") or it.get("rceptNo", "")
            link = f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={art_id}" if art_id else "#"

            tag_type = "neutral"
            tag_name = "공시"
            if any(k in title for k in ["공급계약", "수주", "자기주식취득", "무상증자", "영업실적", "주주우대", "특허"]):
                tag_type = "positive"
                tag_name = "호재"
            elif any(k in title for k in ["유상증자", "전환사채", "신주인수권", "감자", "횡령", "배임", "관리종목", "상장폐지", "불성실공시"]):
                tag_type = "negative"
                tag_name = "악재주의"
            elif any(k in title for k in ["최대주주변경", "주식매수선택권", "주요사항보고서"]):
                tag_type = "warning"
                tag_name = "변동"

            disclosures.append({
                "title": title,
                "date": date_raw,
                "link": link,
                "tag_type": tag_type,
                "tag_name": tag_name,
                "flr_nm": it.get("submitter", "DART")
            })
    except Exception:
        pass

    return disclosures

def get_realtime_usdkrw():
    try:
        url = "https://query1.finance.yahoo.com/v8/finance/chart/KRW=X?range=1d&interval=1m"
        res = HTTP_SESSION.get(url, timeout=1.5).json()
        meta = res.get("chart", {}).get("result", [{}])[0].get("meta", {})
        rate = meta.get("regularMarketPrice", 0.0)
        if rate and float(rate) > 500:
            return float(rate)
    except Exception:
        pass
    return None

def find_stock_code(query):
    clean_q = query.strip().replace(" ", "").upper()
    if clean_q.isdigit() and len(clean_q) == 6:
        return clean_q, clean_q

    for name, code in DOMESTIC_FAST_MAP.items():
        if name.replace(" ", "").upper() == clean_q:
            return code, name

    try:
        url = f"https://m.stock.naver.com/front-api/search/autoComplete?query={urllib.parse.quote(query.strip())}&target=stock"
        res = HTTP_SESSION.get(url, timeout=1.8).json()
        items = res.get("result", {}).get("items", [])
        if items:
            for it in items:
                name = str(it.get("name", "")).strip()
                code = str(it.get("code", "")).strip()
                if name.replace(" ", "").upper() == clean_q:
                    return code, name
            first = items[0]
            return str(first.get("code", "")).strip(), str(first.get("name", "")).strip()
    except Exception:
        pass
    return None, query.strip()

def get_naver_realtime_price(code):
    try:
        url = f"https://polling.finance.naver.com/api/realtime/domestic/stock/{code}"
        res = HTTP_SESSION.get(url, timeout=1.8).json()
        datas = res.get("datas", [])
        if datas:
            p = str(datas[0].get("closePrice", "")).replace(",", "").strip()
            if p and p.replace(".", "").isdigit():
                return float(p), f"{int(float(p)):,} 원"
    except Exception:
        pass
    return None, None

def _fetch_yahoo_chart(symbol, range_value="5y", interval="1d", timeout=3.0):
    """Yahoo Chart API with host fallback. Returns raw result dict or None."""
    encoded = urllib.parse.quote(str(symbol).upper())
    for host in ("query1.finance.yahoo.com", "query2.finance.yahoo.com"):
        try:
            url = f"https://{host}/v8/finance/chart/{encoded}?range={range_value}&interval={interval}"
            res = HTTP_SESSION.get(url, timeout=timeout)
            res.raise_for_status()
            payload = res.json()
            result = payload.get("chart", {}).get("result") or []
            if result:
                return result[0]
        except Exception as exc:
            print(f"[YAHOO] {symbol} via {host} failed: {exc}")
    return None

def _yahoo_result_to_df(result):
    if not result:
        return None
    try:
        ts = result.get("timestamp") or []
        quote = (result.get("indicators") or {}).get("quote", [{}])[0]
        opens, highs, lows, closes, vols = quote.get("open", []), quote.get("high", []), quote.get("low", []), quote.get("close", []), quote.get("volume", [])
        records = []
        for i, t in enumerate(ts):
            if i >= len(opens) or opens[i] is None or closes[i] is None:
                continue
            records.append({
                "Date": pd.to_datetime(t, unit="s"),
                "Open": float(opens[i]),
                "High": float(highs[i]),
                "Low": float(lows[i]),
                "Close": float(closes[i]),
                "Volume": float(vols[i] or 0.0) if i < len(vols) else 0.0
            })
        if not records:
            return None
        return pd.DataFrame(records).set_index("Date").sort_index()
    except Exception as exc:
        print(f"[YAHOO] dataframe parse failed: {exc}")
        return None

def fetch_domestic_candles(code, anchor_price=None):
    # 1) Naver daily chart (primary for Korean equities)
    try:
        url = f"https://fchart.stock.naver.com/sise.nhn?symbol={code}&timeframe=day&count=1200&requestType=0"
        res = HTTP_SESSION.get(url, timeout=3.0)
        if res.status_code == 200:
            root = ET.fromstring(res.text)
            records = []
            for it in root.findall(".//item"):
                parts = (it.get("data", "") or "").split("|")
                if len(parts) >= 6:
                    records.append({"Date": pd.to_datetime(parts[0]), "Open": float(parts[1]), "High": float(parts[2]), "Low": float(parts[3]), "Close": float(parts[4]), "Volume": float(parts[5])})
            if len(records) >= 20:
                df = pd.DataFrame(records).set_index("Date").sort_index()
                if anchor_price and anchor_price > 0:
                    last_close = float(df["Close"].iloc[-1])
                    if last_close > 0 and abs(last_close-anchor_price)/anchor_price > 0.30:
                        ratio = anchor_price / last_close
                        for col in ("Open","High","Low","Close"):
                            df[col] = df[col] * ratio
                return df
    except Exception as exc:
        print(f"[NAVER] candles failed for {code}: {exc}")

    # 2) Yahoo KR ticker fallback
    for suffix in (".KS", ".KQ"):
        df = _yahoo_result_to_df(_fetch_yahoo_chart(f"{code}{suffix}"))
        if df is not None and len(df) >= 20:
            return df
    return None

def fetch_overseas_candles(symbol):
    result = _fetch_yahoo_chart(symbol, range_value="5y", interval="1d")
    df = _yahoo_result_to_df(result)
    if df is not None and not df.empty:
        curr_p = float(df["Close"].iloc[-1])
        return curr_p, f"{curr_p:,.2f} 달러", df
    return None, None, None

def fetch_crypto_candles(symbol):
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

# ----------------------------------------------------
# 4. 100% 실시간 퀀트 백테스팅 & 동적 시그널 연산
# ----------------------------------------------------
def run_real_vectorized_backtest(df):
    """Historical MA20-cross test. Returns None when evidence is insufficient."""
    if df is None or len(df) < 120:
        return None
    try:
        work = df.copy()
        close = work["Close"].astype(float)
        high = work["High"].astype(float)
        low = work["Low"].astype(float)
        ma20 = close.rolling(20).mean()
        signals = []
        for i in range(21, len(work) - 15):
            if pd.isna(ma20.iloc[i-1]) or pd.isna(ma20.iloc[i]):
                continue
            if close.iloc[i-1] <= ma20.iloc[i-1] and close.iloc[i] > ma20.iloc[i]:
                signals.append(i)
        results=[]; wins=[]; losses=[]
        for sidx in signals:
            entry=float(close.iloc[sidx])
            target=entry*1.05; stop=entry*0.96
            outcome=None
            for k in range(1,16):
                if sidx+k>=len(work): break
                if high.iloc[sidx+k] >= target:
                    outcome=True; wins.append(0.05); break
                if low.iloc[sidx+k] <= stop:
                    outcome=False; losses.append(0.04); break
            if outcome is not None: results.append((sidx,outcome))
        if len(results) < 5:
            return None
        n=len(work)
        recent1=[r[1] for r in results if r[0]>=n-250]
        recent2=[r[1] for r in results if r[0]>=n-500]
        win1=sum(recent1)/len(recent1)*100 if recent1 else None
        win2=sum(recent2)/len(recent2)*100 if recent2 else None
        if win1 is None and win2 is None:
            return None
        avg_w=float(np.mean(wins)) if wins else None
        avg_l=float(np.mean(losses)) if losses else None
        rr=(avg_w/avg_l) if avg_w is not None and avg_l and avg_l>0 else None
        return {"win_1y": round(win1,1) if win1 is not None else None,
                "win_2y": round(win2,1) if win2 is not None else None,
                "rr": round(rr,2) if rr is not None else None,
                "trades": len(results)}
    except Exception:
        return None


def _component_score_from_return(v):
    if v is None or not np.isfinite(v):
        return None
    return float(max(0.0, min(100.0, 50.0 + 500.0 * np.tanh(v))))


def calculate_quant_factors(raw_price, df, symbol="", is_krw=True, benchmark_df=None):
    if df is None or len(df) < 30:
        return None
    closes=df["Close"].astype(float); highs=df["High"].astype(float); lows=df["Low"].astype(float); volumes=df["Volume"].astype(float)
    n=len(df); round_fn=(lambda v:f"{int(round(v)):,} 원") if is_krw else (lambda v:f"{float(v):,.2f} 달러")
    def val(s, default=None):
        try:
            x=float(s.iloc[-1] if hasattr(s,'iloc') else s)
            return x if np.isfinite(x) else default
        except Exception: return default
    ma5v=closes.rolling(5).mean().iloc[-1]; ma20v=closes.rolling(20).mean().iloc[-1]
    ma60v=closes.rolling(60).mean().iloc[-1] if n>=60 else None
    ma120v=closes.rolling(120).mean().iloc[-1] if n>=120 else None
    ma600v=closes.rolling(600).mean().iloc[-1] if n>=600 else None
    ma1200v=closes.rolling(1200).mean().iloc[-1] if n>=1200 else None
    ma5=round_fn(ma5v) if np.isfinite(ma5v) else "-"; ma20=round_fn(ma20v) if np.isfinite(ma20v) else "-"
    ma60=round_fn(ma60v) if ma60v is not None and np.isfinite(ma60v) else "-"; ma120=round_fn(ma120v) if ma120v is not None and np.isfinite(ma120v) else "-"
    ma600=round_fn(ma600v) if ma600v is not None and np.isfinite(ma600v) else "-"; ma1200=round_fn(ma1200v) if ma1200v is not None and np.isfinite(ma1200v) else "-"
    rsi_series=100-(100/(1+(closes.diff().clip(lower=0).rolling(14).mean()/(-closes.diff().clip(upper=0).rolling(14).mean()))))
    rsi_num=val(rsi_series)
    atr_series=(highs-lows).rolling(14).mean(); atr_raw=val(atr_series)
    pct=closes.pct_change(); ann_vol=val(pct.rolling(20).std()*np.sqrt(250)*100)
    vol_avg5=volumes.shift(1).rolling(5).mean(); vol_ref=val(vol_avg5); vol_pct=(float(volumes.iloc[-1])/vol_ref*100) if vol_ref and vol_ref>0 else None
    std20=val(closes.rolling(20).std()); z_score=(float(raw_price)-ma20v)/std20 if std20 and std20>0 else None
    bb_mid=val(closes.rolling(60).mean()) if n>=60 else None; bb_std=val(closes.rolling(60).std()) if n>=60 else None
    bb_upper=bb_mid+2*bb_std if bb_mid is not None and bb_std is not None else None; bb_lower=bb_mid-2*bb_std if bb_mid is not None and bb_std is not None else None
    ema12=closes.ewm(span=12,adjust=False).mean(); ema26=closes.ewm(span=26,adjust=False).mean(); macd=ema12-ema26; macd_sig=macd.ewm(span=9,adjust=False).mean(); macd_v=val(macd); macd_s=val(macd_sig)
    # ADX / directional strength
    up=highs.diff(); dn=-lows.diff(); tr=pd.concat([highs-lows,(highs-closes.shift()).abs(),(lows-closes.shift()).abs()],axis=1).max(axis=1)
    plus_dm=up.where((up>dn)&(up>0),0.0); minus_dm=dn.where((dn>up)&(dn>0),0.0)
    atr14=tr.rolling(14).mean(); pdi=100*(plus_dm.rolling(14).mean()/atr14); mdi=100*(minus_dm.rolling(14).mean()/atr14); dx=100*((pdi-mdi).abs()/(pdi+mdi)); adx_s=dx.rolling(14).mean(); adx=val(adx_s); pdi_v=val(pdi); mdi_v=val(mdi)
    # OBV direction and slope
    obv=(np.sign(closes.diff()).fillna(0)*volumes).cumsum(); obv20_slope=None
    if n>=20:
        x=np.arange(20); y=obv.tail(20).to_numpy(dtype=float); obv20_slope=float(np.polyfit(x,y,1)[0])
    # momentum horizons
    ret1=closes.iloc[-1]/closes.iloc[-21]-1 if n>=22 else None
    ret3=closes.iloc[-1]/closes.iloc[-64]-1 if n>=65 else None
    ret6=closes.iloc[-1]/closes.iloc[-127]-1 if n>=128 else None
    ret12=closes.iloc[-1]/closes.iloc[-253]-1 if n>=254 else None
    # relative strength to benchmark
    rs1=rs3=None
    if benchmark_df is not None and len(benchmark_df)>65:
        b=benchmark_df["Close"].astype(float)
        try:
            br1=b.iloc[-1]/b.iloc[-21]-1 if len(b)>=22 else None
            br3=b.iloc[-1]/b.iloc[-64]-1 if len(b)>=65 else None
            rs1=(ret1-br1) if ret1 is not None and br1 is not None else None
            rs3=(ret3-br3) if ret3 is not None and br3 is not None else None
        except Exception: pass
    high20=closes.tail(20).max() if n>=20 else None; high60=closes.tail(60).max() if n>=60 else None; high120=closes.tail(120).max() if n>=120 else None
    breakout_strength=0.0; breakout_parts=0
    for h in (high20,high60,high120):
        if h is not None and h>0:
            breakout_strength += max(0.0,min(100.0,(float(raw_price)/float(h)-1)*1000+50)); breakout_parts+=1
    breakout_score=breakout_strength/breakout_parts if breakout_parts else None
    trend_comp=[]
    if ma20v and ma60v:
        trend_comp.append(100 if float(raw_price)>ma20v>ma60v else 75 if float(raw_price)>ma20v else 25 if float(raw_price)<ma20v<ma60v else 50)
    if macd_v is not None and macd_s is not None: trend_comp.append(75 if macd_v>macd_s else 25)
    if adx is not None and pdi_v is not None and mdi_v is not None: trend_comp.append(min(100,max(0,50+(adx-20)*1.5 + (pdi_v-mdi_v)*1.5)))
    if vol_pct is not None: trend_comp.append(min(100,max(0,50+(vol_pct-100)*0.25)))
    if obv20_slope is not None: trend_comp.append(75 if obv20_slope>0 else 25)
    if rs1 is not None: trend_comp.append(_component_score_from_return(rs1))
    mom_parts=[]
    for v,w in ((ret1,0.20),(ret3,0.20),(ret6,0.15),(ret12,0.10),(rs1,0.08),(rs3,0.05)):
        c=_component_score_from_return(v)
        if c is not None: mom_parts.append((c,w))
    trend_score=float(np.mean(trend_comp)) if trend_comp else None
    if trend_score is not None: mom_parts.append((trend_score,0.15))
    if adx is not None: mom_parts.append((min(100,max(0,adx*2.0)),0.07))
    if obv20_slope is not None: mom_parts.append((75 if obv20_slope>0 else 25,0.05))
    if breakout_score is not None: mom_parts.append((breakout_score,0.05))
    if mom_parts:
        wsum=sum(w for _,w in mom_parts); momentum_score=sum(c*w for c,w in mom_parts)/wsum
    else: momentum_score=None
    # history for the graph (last 90 bars)
    hist=[]; hist_dates=[]
    start_idx=max(1,n-90)
    for i in range(start_idx,n):
        if i<22: continue
        c=float(closes.iloc[i]); r1=float(closes.iloc[i]/closes.iloc[i-21]-1) if i>=21 else None
        r3=float(closes.iloc[i]/closes.iloc[i-63]-1) if i>=63 else None
        ma20_i=float(closes.iloc[max(0,i-19):i+1].mean())
        ma60_i=float(closes.iloc[max(0,i-59):i+1].mean()) if i>=59 else None
        parts=[]
        for v,w in ((r1,0.35),(r3,0.25)):
            sc=_component_score_from_return(v)
            if sc is not None: parts.append((sc,w))
        if ma60_i is not None: parts.append((75 if c>ma20_i>ma60_i else 25 if c<ma20_i<ma60_i else 50,0.25))
        parts.append((75 if i>0 and closes.iloc[i]>closes.iloc[i-1] else 25,0.15))
        hs=sum(w for _,w in parts); score=sum(c*w for c,w in parts)/hs if hs else None
        if score is not None:
            hist.append(round(float(score),1)); hist_dates.append(pd.Timestamp(df.index[i]).strftime('%m-%d'))
    high52=df.tail(min(252,n))["High"].max(); low52=df.tail(min(252,n))["Low"].min(); pos=((float(raw_price)-low52)/(high52-low52)*100) if high52>low52 else None
    disp20=(float(raw_price)/ma20v*100) if ma20v else None; disp60=(float(raw_price)/ma60v*100) if ma60v else None
    bullish_count=0; checks=0
    for cond in [float(raw_price)>=ma5v, float(raw_price)>=ma20v, ma60v is not None and float(raw_price)>=ma60v, macd_v is not None and macd_s is not None and macd_v>=macd_s, rsi_num is not None and 40<=rsi_num<=65, vol_pct is not None and vol_pct>=100]:
        checks+=1
        if cond: bullish_count+=1
    quant_score=round((bullish_count/checks)*100,1) if checks else None
    trend="정배열 상승 추세" if ma60v is not None and float(raw_price)>ma20v>ma60v else "역배열 하락 추세" if ma60v is not None and float(raw_price)<ma20v<ma60v else "박스권 혼조세"
    if trend=="정배열 상승 추세": guide="20일선과 60일선의 방향이 모두 상승인지 확인하고, 거래량과 OBV가 동반되는 눌림을 우선 관찰하세요."
    elif trend=="역배열 하락 추세": guide="20일선·60일선 아래에서는 반등보다 추세 회복 여부를 먼저 확인하세요."
    else: guide="추세가 혼재되어 있어 돌파와 거래량 동반 여부를 확인한 뒤 대응하세요."
    if momentum_score is None: momentum_state="데이터 부족"
    elif momentum_score>=75: momentum_state="강한 상승 모멘텀"
    elif momentum_score>=60: momentum_state="상승 모멘텀 우세"
    elif momentum_score>=40: momentum_state="중립"
    else: momentum_state="하락 모멘텀 우세"
    sig=run_real_vectorized_backtest(df)
    # signal prices: only create when ATR + structure are available
    entry1=entry2=target1=target2=stop=None; rr1=rr2=None
    if atr_raw is not None and atr_raw>0:
        recent_low=float(lows.tail(20).min()); recent_high=float(highs.tail(20).max())
        entry1=float(raw_price)-0.5*atr_raw; entry2=float(raw_price)-1.0*atr_raw
        stop=float(recent_low-0.25*atr_raw)
        target1=float(raw_price)+1.5*atr_raw; target2=float(raw_price)+2.5*atr_raw
        if stop>=float(raw_price): stop=float(raw_price)-1.5*atr_raw
        risk=float(raw_price)-stop
        rr1=(target1-float(raw_price))/risk if risk>0 else None; rr2=(target2-float(raw_price))/risk if risk>0 else None
    return {
        "quant_score": f"{quant_score:.1f}" if quant_score is not None else "-",
        "score_badge": "모멘텀 최강세" if momentum_score is not None and momentum_score>=80 else "상승 모멘텀 우세" if momentum_score is not None and momentum_score>=65 else "중립/관망",
        "score_dots": f"상승 조건 {bullish_count}/{checks}" if checks else "데이터 부족",
        "trend": trend,
        "z_score": f"{z_score:+.2f} σ" if z_score is not None else "-", "z_state": "과열 경계" if z_score is not None and z_score>=2 else "과매도 반등구간" if z_score is not None and z_score<=-2 else "정규 범위" if z_score is not None else "데이터 부족", "z_color":"warning" if z_score is not None and z_score>=2 else "positive" if z_score is not None and z_score<=-2 else "neutral",
        "ann_vol": f"{ann_vol:.1f}%" if ann_vol is not None else "-", "vol_risk_state":"고변동성" if ann_vol is not None and ann_vol>=40 else "적정 변동성" if ann_vol is not None and ann_vol>=18 else "저변동성" if ann_vol is not None else "데이터 부족", "vol_risk_color":"warning" if ann_vol is not None and ann_vol>=40 else "positive" if ann_vol is not None and ann_vol>=18 else "neutral",
        "ma5":ma5,"ma5_state":"5일선 상회" if np.isfinite(ma5v) and float(raw_price)>=ma5v else "5일선 하회","ma5_color":"positive" if np.isfinite(ma5v) and float(raw_price)>=ma5v else "negative",
        "ma20":ma20,"ma20_state":"20일선 지지" if np.isfinite(ma20v) and float(raw_price)>=ma20v else "20일선 하회","ma20_color":"positive" if np.isfinite(ma20v) and float(raw_price)>=ma20v else "negative",
        "ma60":ma60,"ma60_state":"60일선 상회" if ma60v is not None and float(raw_price)>=ma60v else "60일선 하회","ma60_color":"positive" if ma60v is not None and float(raw_price)>=ma60v else "warning",
        "ma120":ma120,"ma120_state":"120일선 상회" if ma120v is not None and float(raw_price)>=ma120v else "120일선 하회","ma120_color":"positive" if ma120v is not None and float(raw_price)>=ma120v else "negative",
        "ma600":ma600,"ma1200":ma1200,
        "bollinger": f"{round_fn(bb_upper)} / {round_fn(bb_lower)}" if bb_upper is not None and bb_lower is not None else "-", "bb_state":"상단 돌파" if bb_upper is not None and float(raw_price)>=bb_upper else "하단 접근" if bb_lower is not None and float(raw_price)<=bb_lower else "밴드 내" if bb_upper is not None else "데이터 부족", "bb_color":"positive" if bb_upper is not None and float(raw_price)>=bb_upper else "warning" if bb_lower is not None and float(raw_price)<=bb_lower else "neutral",
        "macd": f"{macd_v:+.2f} (시그널: {macd_s:+.2f})" if macd_v is not None and macd_s is not None else "-", "macd_state":"상승 모멘텀" if macd_v is not None and macd_s is not None and macd_v>=macd_s else "하락 압력" if macd_v is not None and macd_s is not None else "데이터 부족", "macd_color":"positive" if macd_v is not None and macd_s is not None and macd_v>=macd_s else "negative" if macd_v is not None else "neutral",
        "rsi": f"{rsi_num:.1f}" if rsi_num is not None else "-", "rsi_state":"과열권" if rsi_num is not None and rsi_num>=70 else "침체권" if rsi_num is not None and rsi_num<=30 else "중립" if rsi_num is not None else "데이터 부족", "rsi_color":"warning" if rsi_num is not None and rsi_num>=70 else "positive" if rsi_num is not None and rsi_num<=30 else "neutral",
        "vol_ratio": f"{vol_pct:.1f}%" if vol_pct is not None else "-", "vol_state":"거래량 급증" if vol_pct is not None and vol_pct>=150 else "거래량 보통" if vol_pct is not None and vol_pct>=70 else "거래량 침체" if vol_pct is not None else "데이터 부족", "vol_color":"positive" if vol_pct is not None and vol_pct>=150 else "neutral",
        "tval_20d": round_fn(float((closes.tail(20)*volumes.tail(20)).mean())) if n>=20 else "-",
        "low_52w": round_fn(low52), "high_52w":round_fn(high52), "slider_fill_pct": min(92,max(8,int(pos))) if pos is not None else 50,
        "disp": f"20일: {disp20:.1f}% | 60일: {disp60:.1f}%" if disp20 is not None and disp60 is not None else "-", "disp_state":"적정 이격" if disp20 is not None and 98<=disp20<=104 else "과열 경계" if disp20 is not None and disp20>104 else "과매도 구간" if disp20 is not None else "데이터 부족", "disp_color":"positive" if disp20 is not None and 98<=disp20<=104 else "warning" if disp20 is not None and disp20>104 else "neutral", "disp20_num":f"{disp20:+.1f}%" if disp20 is not None else "-",
        "atr": round_fn(atr_raw) if atr_raw is not None else "-", "atr_ratio": f"{atr_raw/float(raw_price)*100:.2f}%" if atr_raw is not None and raw_price else "-",
        "adx": f"{adx:.1f}" if adx is not None else "-", "adx_state":"강한 추세" if adx is not None and adx>=25 else "추세 약함" if adx is not None else "데이터 부족", "adx_color":"positive" if adx is not None and adx>=25 else "warning" if adx is not None else "neutral",
        "obv_state":"누적 매수 우세" if obv20_slope is not None and obv20_slope>0 else "누적 매도 우세" if obv20_slope is not None else "데이터 부족", "obv_color":"positive" if obv20_slope is not None and obv20_slope>0 else "negative" if obv20_slope is not None else "neutral",
        "momentum_1m": f"{ret1*100:+.1f}%" if ret1 is not None else "-", "momentum_3m":f"{ret3*100:+.1f}%" if ret3 is not None else "-", "momentum_6m":f"{ret6*100:+.1f}%" if ret6 is not None else "-", "momentum_12m":f"{ret12*100:+.1f}%" if ret12 is not None else "-",
        "rs_1m": f"{rs1*100:+.1f}%p" if rs1 is not None else "-", "rs_3m":f"{rs3*100:+.1f}%p" if rs3 is not None else "-", "breakout_strength":f"{breakout_score:.1f}" if breakout_score is not None else "-",
        "momentum_score": f"{momentum_score:.1f}" if momentum_score is not None else "-", "momentum_state": momentum_state, "momentum_history":hist, "momentum_dates":hist_dates,
        "entry1": round_fn(entry1) if entry1 is not None else "-", "entry2": round_fn(entry2) if entry2 is not None else "-", "target1":round_fn(target1) if target1 is not None else "-", "target2":round_fn(target2) if target2 is not None else "-", "stop_p":round_fn(stop) if stop is not None else "-",
        "entry1_num": entry1, "entry2_num": entry2, "target1_num": target1, "target2_num": target2, "stop_num": stop,
        "sig_fill_pct": int(min(92,max(8,50))) if entry1 is None else 50, "guide_text":guide,
        "bt_1y": f"1년 실측 {sig['win_1y']:.1f}%" if sig and sig.get('win_1y') is not None else "1년 실측 데이터 부족", "bt_2y": f"2년 실측 {sig['win_2y']:.1f}%" if sig and sig.get('win_2y') is not None else "2년 실측 데이터 부족", "win_rate_num": f"{sig['win_2y']:.1f}%" if sig and sig.get('win_2y') is not None else "-", "rr_str": f"실측 손익비 1:{sig['rr']:.2f}" if sig and sig.get('rr') is not None else "실측 손익비 -",
        "rr1":rr1,"rr2":rr2,
    }


def run_full_pipeline(query):
    global ANALYSIS_CACHE, CACHED_WORLD_BOARD
    clean_q = query.strip().replace(" ", "").upper()
    now_ts = time.time()

    if clean_q in ANALYSIS_CACHE:
        cached_time, cached_data = ANALYSIS_CACHE[clean_q]
        if now_ts - cached_time < 30:
            if CACHED_WORLD_BOARD:
                cached_data["world_board"] = CACHED_WORLD_BOARD
            return cached_data

    market_name = "국내주식"
    is_krw = True

    if clean_q in GLOBAL_INDEX_MAP:
        symbol = GLOBAL_INDEX_MAP[clean_q]
        formal_name = query.strip()
        curr_p, price_str, df = fetch_overseas_candles(symbol)
        
        if symbol in ["^KS11", "^KQ11"]:
            market_name = "국내증시"
            is_krw = True
            price_str = f"{curr_p:,.2f} pt" if curr_p else "-"
        elif symbol == "KRW=X":
            market_name = "외환"
            is_krw = False
            price_str = f"{curr_p:,.1f} 원" if curr_p else "-"
        elif symbol in ["CL=F", "GC=F"]:
            market_name = "원자재"
            is_krw = False
            price_str = f"${curr_p:,.2f}" if curr_p else "-"
        else:
            market_name = "글로벌증시"
            is_krw = False
            price_str = f"${curr_p:,.2f}" if curr_p else "-"
        raw_price = curr_p

    elif clean_q.endswith("USDT") or clean_q in CRYPTO_MAP or any(c in clean_q for c in ["BTC", "ETH", "코인", "비트"]):
        symbol = CRYPTO_MAP.get(clean_q, clean_q if clean_q.endswith("USDT") else f"{clean_q}USDT")
        formal_name = clean_q
        raw_price, price_str, df = fetch_crypto_candles(symbol)
        market_name = "암호화폐"
        is_krw = False

    elif clean_q in US_STOCK_MAP:
        symbol = US_STOCK_MAP[clean_q]
        formal_name = query.strip()
        raw_price, price_str, df = fetch_overseas_candles(symbol)
        market_name = "미국주식"
        is_krw = False
    elif any(c.isupper() for c in query) and not any('\uac00' <= char <= '\ud7a3' for char in query):
        symbol = query.upper().strip()
        formal_name = symbol
        raw_price, price_str, df = fetch_overseas_candles(symbol)
        market_name = "미국주식"
        is_krw = False

    else:
        code, name = find_stock_code(query)
        if not code:
            return None
        symbol, formal_name = code, name
        raw_price, price_str = get_naver_realtime_price(symbol)
        df = fetch_domestic_candles(symbol, anchor_price=raw_price)
        if raw_price is None and df is not None and not df.empty:
            raw_price = float(df['Close'].iloc[-1])
            price_str = f"{int(raw_price):,} 원"

    if raw_price is None or df is None or len(df) < 30:
        print(f"[PIPELINE] core market data unavailable: query={query!r}, symbol={locals().get('symbol','?')}, raw_price={raw_price}, bars={len(df) if df is not None else 0}")
        return {"__error__": "핵심 시세 데이터를 가져오지 못했습니다. 잠시 후 다시 시도해 주세요."}

    with ThreadPoolExecutor(max_workers=8) as executor:
        f_news = executor.submit(fetch_rss_news, formal_name, 5)
        f_global_news = executor.submit(fetch_rss_news, "뉴욕증시 OR 미국증시 OR 나스닥", 4)
        f_consensus = executor.submit(fetch_market_consensus_unified, symbol, market_name, raw_price, formal_name)
        f_dart = executor.submit(fetch_dart_disclosures, symbol, is_krw, 5)

        stock_news = f_news.result()
        global_news = f_global_news.result()
        consensus = f_consensus.result()
        dart_disclosures = f_dart.result()

    world_board = CACHED_WORLD_BOARD if CACHED_WORLD_BOARD else fetch_all_world_board_parallel()
    benchmark_df = None
    try:
        if market_name in ("국내주식", "국내증시"):
            benchmark_df = fetch_overseas_candles("^KS11")[2]
        elif market_name == "미국주식":
            benchmark_df = fetch_overseas_candles("SPY")[2]
        elif market_name == "암호화폐" and symbol != "BTCUSDT":
            benchmark_df = fetch_crypto_candles("BTCUSDT")[2]
    except Exception:
        benchmark_df = None
    quant = calculate_quant_factors(raw_price, df, symbol=symbol, is_krw=is_krw, benchmark_df=benchmark_df)
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S KST")

    df['MA20'] = df['Close'].rolling(20).mean()
    df['MA60'] = df['Close'].rolling(60).mean()
    df['STD60'] = df['Close'].rolling(60).std()
    df['BB_UP'] = df['MA60'] + (df['STD60'] * 2)
    df['BB_DN'] = df['MA60'] - (df['STD60'] * 2)

    candles = []
    ma20_line = []
    ma60_line = []
    bb_up_line = []
    bb_dn_line = []

    for date_idx, row in df.tail(65).iterrows():
        t_str = date_idx.strftime("%Y-%m-%d")
        candles.append({
            "time": t_str,
            "open": float(row['Open']), "high": float(row['High']),
            "low": float(row['Low']), "close": float(row['Close'])
        })
        if not np.isnan(row['MA20']):
            ma20_line.append({"time": t_str, "value": float(row['MA20'])})
        if not np.isnan(row['MA60']):
            ma60_line.append({"time": t_str, "value": float(row['MA60'])})
        if not np.isnan(row['BB_UP']):
            bb_up_line.append({"time": t_str, "value": float(row['BB_UP'])})
        if not np.isnan(row['BB_DN']):
            bb_dn_line.append({"time": t_str, "value": float(row['BB_DN'])})

    result_data = {
        "name": formal_name,
        "formal_name": formal_name,
        "symbol": symbol,
        "market": market_name,
        "price": price_str,
        "price_str": price_str,
        "time": now_str,
        "now_str": now_str,
        "quant": quant,
        "consensus": consensus,
        "stock_news": stock_news,
        "global_news": global_news,
        "world_board": world_board,
        "dart_disclosures": dart_disclosures,
        "candles": candles,
        "ma20_line": ma20_line,
        "ma60_line": ma60_line,
        "bb_up_line": bb_up_line,
        "bb_dn_line": bb_dn_line
    }

    ANALYSIS_CACHE[clean_q] = (now_ts, result_data)
    return result_data

# ----------------------------------------------------
# 6. 텔레그램 봇 실시간 인터랙션 엔진
# ----------------------------------------------------
TELEGRAM_ALERTS = {}
TELEGRAM_ALERT_LOCK = threading.Lock()


def telegram_send(chat_id, text):
    if not TELEGRAM_TOKEN:
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
        r = HTTP_SESSION.post(url, json=payload, timeout=5)
        return bool(r.ok and r.json().get("ok"))
    except Exception as exc:
        print(f"[TELEGRAM] send failed: {exc}")
        return False


def _resolve_data_for_alert(symbol):
    clean = symbol.upper().replace("-", "")
    if clean.endswith("USDT"):
        p, _, _ = fetch_crypto_candles(clean)
        return p
    if clean in CRYPTO_MAP:
        p, _, _ = fetch_crypto_candles(CRYPTO_MAP[clean])
        return p
    # US stock ticker or mapped symbol
    p, _, _ = fetch_overseas_candles(clean)
    if p is not None:
        return p
    code, _ = find_stock_code(symbol)
    if code:
        p, _ = get_naver_realtime_price(code)
        return p
    return None


def register_telegram_alert(chat_id, query):
    res = run_full_pipeline(query)
    if not res:
        return None, "종목 데이터를 가져오지 못했습니다."
    analysis = res.get("quant") or {}
    direction = analysis.get("momentum_state", "관망")
    direction = "LONG" if analysis.get("momentum_score") not in (None, "-") and float(analysis.get("momentum_score")) >= 65 else "WAIT"
    with TELEGRAM_ALERT_LOCK:
        bucket = TELEGRAM_ALERTS.setdefault(str(chat_id), {})
        bucket[res["symbol"]] = {
            "name": res["formal_name"],
            "symbol": res["symbol"],
            "direction": direction,
            "target1": analysis.get("target1_num"),
            "target2": analysis.get("target2_num"),
            "stop": analysis.get("stop_num"),
            "last_price": None,
            "fired": set(),
        }
    return res, None


def remove_telegram_alert(chat_id, symbol=None):
    with TELEGRAM_ALERT_LOCK:
        if str(chat_id) not in TELEGRAM_ALERTS:
            return 0
        if symbol:
            return 1 if TELEGRAM_ALERTS[str(chat_id)].pop(symbol.upper(), None) else 0
        count = len(TELEGRAM_ALERTS[str(chat_id)])
        TELEGRAM_ALERTS[str(chat_id)] = {}
        return count


def list_telegram_alerts(chat_id):
    with TELEGRAM_ALERT_LOCK:
        return list(TELEGRAM_ALERTS.get(str(chat_id), {}).values())


def telegram_alert_monitor():
    while True:
        try:
            snapshot=[]
            with TELEGRAM_ALERT_LOCK:
                for chat_id, bucket in TELEGRAM_ALERTS.items():
                    for symbol, sub in bucket.items():
                        snapshot.append((chat_id, symbol, dict(sub)))
            for chat_id, symbol, sub in snapshot:
                price = _resolve_data_for_alert(symbol)
                if price is None:
                    continue
                sub["last_price"] = price
                direction=sub.get("direction")
                checks=[]
                if direction=="LONG":
                    if sub.get("target1") is not None and price>=sub["target1"]: checks.append(("target1", "1차 목표가 도달"))
                    if sub.get("target2") is not None and price>=sub["target2"]: checks.append(("target2", "2차 목표가 도달"))
                    if sub.get("stop") is not None and price<=sub["stop"]: checks.append(("stop", "리스크 방어선 도달"))
                elif direction=="SHORT":
                    if sub.get("target1") is not None and price<=sub["target1"]: checks.append(("target1", "1차 목표가 도달"))
                    if sub.get("target2") is not None and price<=sub["target2"]: checks.append(("target2", "2차 목표가 도달"))
                    if sub.get("stop") is not None and price>=sub["stop"]: checks.append(("stop", "리스크 방어선 도달"))
                for key, label in checks:
                    with TELEGRAM_ALERT_LOCK:
                        current=TELEGRAM_ALERTS.get(str(chat_id),{}).get(symbol)
                        already=(current and key in current.get("fired", set()))
                        if current and not already:
                            current.setdefault("fired", set()).add(key)
                    if not already:
                        telegram_send(chat_id, f"🚨 <b>{html.escape(sub['name'])}</b>\n{html.escape(label)}\n현재가: <b>{price:,.2f}</b>\n\ngaemiGTP는 데이터 기반 조건을 알립니다. 수익을 보장하지 않습니다.")
        except Exception as exc:
            print(f"[TELEGRAM] monitor error: {exc}")
        time.sleep(30)


def telegram_bot_worker():
    if not TELEGRAM_TOKEN:
        return
    offset=0
    while True:
        try:
            url=f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
            params={"offset":offset,"timeout":20,"allowed_updates":json.dumps(["message"])}
            res=HTTP_SESSION.get(url,params=params,timeout=25).json()
            if not res.get("ok"):
                time.sleep(3); continue
            for upd in res.get("result",[]):
                offset=upd["update_id"]+1
                msg=upd.get("message",{}); chat_id=msg.get("chat",{}).get("id"); text=(msg.get("text") or "").strip()
                if not chat_id or not text: continue
                if text.startswith("/start"):
                    parts=text.split(maxsplit=1)
                    if len(parts)==2 and parts[1].strip():
                        target=parts[1].strip(); res_data,err=register_telegram_alert(chat_id,target)
                        if err:
                            telegram_send(chat_id,f"❌ {html.escape(err)}")
                        else:
                            a=res_data.get("quant", {})
                            telegram_send(chat_id,
                                f"✅ <b>{html.escape(res_data['formal_name'])}</b> 알림 등록\n\n"
                                f"현재가: <b>{res_data['price']}</b>\n"
                                f"상승 모멘텀: <b>{a.get('momentum_score','-')}</b> ({a.get('momentum_state','-')})\n"
                                f"1차 목표: {a.get('target1','-')}\n2차 목표: {a.get('target2','-')}\n방어선: {a.get('stop_p','-')}\n\n"
                                f"목표/방어선 도달을 30초 간격으로 확인합니다.\n/stop {res_data['symbol']} 로 해제할 수 있습니다.")
                    else:
                        telegram_send(chat_id,"gaemiGTP 알림봇입니다.\n/하이닉스 또는 /NVDA처럼 입력하세요.\n/stop 전체 해제\n/status 등록 목록")
                    continue
                if text.startswith("/stop"):
                    parts=text.split(maxsplit=1)
                    if len(parts)==1 or parts[1].strip()=="전체":
                        removed=remove_telegram_alert(chat_id)
                    else:
                        removed=remove_telegram_alert(chat_id, parts[1].strip())
                    telegram_send(chat_id, f"✅ 알림 {removed}건을 해제했습니다.")
                    continue
                if text.startswith("/status"):
                    items=list_telegram_alerts(chat_id)
                    if not items: telegram_send(chat_id,"현재 등록된 알림이 없습니다.")
                    else:
                        body="\n".join([f"• {html.escape(x['name'])} ({x['symbol']}) / {x['direction']}" for x in items])
                        telegram_send(chat_id,"📋 등록된 알림\n"+body)
                    continue
                query_clean=text.lstrip("/")
                if query_clean:
                    res_data,err=register_telegram_alert(chat_id,query_clean)
                    if err: telegram_send(chat_id,f"❌ {html.escape(err)}")
                    else: telegram_send(chat_id,f"✅ {html.escape(res_data['formal_name'])} 알림 등록 완료\n현재가: {res_data['price']}\n/status 로 확인")
        except Exception as exc:
            print(f"[TELEGRAM] polling error: {exc}")
            time.sleep(3)

if TELEGRAM_TOKEN:
    threading.Thread(target=telegram_bot_worker, daemon=True, name="telegram-worker").start()
    threading.Thread(target=telegram_alert_monitor, daemon=True, name="telegram-monitor").start()

HTML_PAGE = """<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>gaemiGTP</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Google+Sans:wght@400;500;600;700&family=Roboto:wght@400;500;700&display=swap" rel="stylesheet">
    <script src="https://unpkg.com/lightweight-charts@4.1.1/dist/lightweight-charts.standalone.production.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2"></script>
    <style>
        :root {
            --bg-body: #131314;
            --bg-card: #1e1f20;
            --bg-sidebar: #1e1f20;
            --bg-subcard: #131314;
            --text-title: #f2f2f2;
            --text-main: #e3e3e3;
            --text-muted: #8e918f;
            --text-sub: #c4c7c5;
            --border-line: #282a2c;
            --border-focus: #333538;
            --btn-hover: rgba(255, 255, 255, 0.08);
            --signal-border: rgba(16, 185, 129, 0.55);
            --signal-glow: rgba(16, 185, 129, 0.12);
            --bottom-fade: linear-gradient(180deg, rgba(19,19,20,0) 0%, rgba(19,19,20,1) 40%);
        }

        body.light-theme {
            --bg-body: #f8f9fa;
            --bg-card: #ffffff;
            --bg-sidebar: #f1f3f4;
            --bg-subcard: #f8f9fa;
            --text-title: #1f1f1f;
            --text-main: #202124;
            --text-muted: #5f6368;
            --text-sub: #3c4043;
            --border-line: #dadce0;
            --border-focus: #bdc1c6;
            --btn-hover: rgba(0, 0, 0, 0.05);
            --signal-border: rgba(16, 185, 129, 0.65);
            --signal-glow: rgba(16, 185, 129, 0.08);
            --bottom-fade: linear-gradient(180deg, rgba(248,249,250,0) 0%, rgba(248,249,250,1) 40%);
        }

        * { box-sizing: border-box; margin: 0; padding: 0; font-family: 'Google Sans', 'Roboto', -apple-system, BlinkMacSystemFont, "Apple SD Gothic Neo", sans-serif; }
        body { background-color: var(--bg-body); color: var(--text-main); min-height: 100vh; overflow-x: hidden; display: flex; transition: background-color 0.2s, color 0.2s; }
        
        #sidebar-wrapper {
            width: 260px;
            background-color: var(--bg-sidebar);
            height: 100vh;
            position: fixed;
            top: 0;
            left: 0;
            display: flex;
            flex-direction: column;
            padding: 12px 14px 16px;
            z-index: 100;
            border-right: 1px solid var(--border-line);
            transform: translateX(-100%);
            transition: transform 0.25s ease, background-color 0.2s;
            box-shadow: none;
        }
        #sidebar-wrapper.open {
            transform: translateX(0);
            box-shadow: 10px 0 30px rgba(0,0,0,0.25);
        }

        .side-top-action {
            height: 44px;
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 12px;
            padding: 0 4px;
        }

        .gemini-menu-icon {
            background: transparent;
            border: none;
            color: var(--text-sub);
            cursor: pointer;
            padding: 6px;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            transition: background 0.15s, color 0.15s;
        }
        .gemini-menu-icon:hover { background: var(--btn-hover); color: var(--text-title); }

        .gemini-new-btn {
            display: flex;
            align-items: center;
            gap: 12px;
            background: var(--bg-body);
            border: 1px solid var(--border-line);
            color: var(--text-main);
            font-size: 0.875rem;
            font-weight: 500;
            padding: 10px 16px;
            border-radius: 24px;
            cursor: pointer;
            width: 100%;
            transition: all 0.2s;
            margin-bottom: 16px;
        }
        .gemini-new-btn:hover { background: var(--btn-hover); border-color: var(--border-focus); color: var(--text-title); }

        .sidebar-scroll-area {
            flex: 1;
            overflow-y: auto;
        }

        .sidebar-section-title {
            font-size: 0.75rem;
            color: var(--text-muted);
            font-weight: 600;
            padding: 8px 12px;
            letter-spacing: -0.2px;
        }

        .sidebar-item {
            display: flex;
            align-items: center;
            padding: 9px 14px;
            border-radius: 20px;
            color: var(--text-sub);
            font-size: 0.875rem;
            cursor: pointer;
            margin-bottom: 2px;
            transition: background 0.15s, color 0.15s;
        }
        .sidebar-item:hover { background: var(--btn-hover); color: var(--text-title); }

        #content-wrapper {
            margin-left: 0;
            width: 100%;
            min-height: 100vh;
            display: flex;
            flex-direction: column;
            position: relative;
        }

        header {
            height: 56px;
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 0 16px;
            background: var(--bg-body);
            border-bottom: none;
            position: sticky;
            top: 0;
            z-index: 40;
            transition: background-color 0.2s;
        }

        .header-left { display: flex; align-items: center; gap: 12px; }
        .logo-top { font-weight: 700; font-size: 1.18rem; color: var(--text-title); letter-spacing: -0.5px; cursor: pointer; }

        #main-view { min-height: calc(100vh - 56px); display: flex; flex-direction: column; align-items: center; justify-content: center; padding: 16px; }
        .hero-title { font-size: 2.8rem; font-weight: 700; margin-bottom: 6px; letter-spacing: -1px; color: var(--text-title); text-align: center; }
        .hero-sub { font-size: 0.95rem; color: var(--text-muted); margin-bottom: 24px; font-weight: 400; text-align: center; }

        .search-container { width: 100%; max-width: 832px; position: relative; }
        .search-box { width: 100%; min-height: 60px; background: var(--bg-card); border: 1px solid var(--border-line); border-radius: 30px; padding: 8px 12px 8px 18px; display: flex; align-items: center; box-shadow: 0 4px 20px rgba(0,0,0,0.1); position: relative; }
        .search-box input { flex: 1; min-width: 0; background: transparent; border: none; outline: none; color: var(--text-title); font-size: 0.95rem; }

        .model-selector-wrap { position: relative; margin-right: 4px; flex-shrink: 0; }
        .model-pill-btn { background: transparent; border: none; border-radius: 20px; padding: 6px 8px; display: flex; align-items: center; gap: 4px; cursor: pointer; user-select: none; }
        .model-pill-btn:hover { background: var(--btn-hover); }
        .model-pill-name { font-size: 0.82rem; font-weight: 600; color: var(--text-main); }
        .model-pill-arrow { font-size: 0.6rem; color: var(--text-muted); }

        .model-dropdown-menu { display: none; position: absolute; bottom: calc(100% + 10px); right: 0; width: 210px; max-width: 80vw; background: var(--bg-card); border: 1px solid var(--border-line); border-radius: 16px; padding: 6px; box-shadow: 0 10px 30px rgba(0,0,0,0.15); z-index: 100; }
        .model-dropdown-item { padding: 9px 12px; border-radius: 10px; cursor: pointer; }
        .model-dropdown-item:hover { background: var(--btn-hover); }
        .model-dropdown-item.active { background: var(--btn-hover); }
        .item-title { font-size: 0.88rem; font-weight: 600; color: var(--text-title); display: flex; justify-content: space-between; }
        .item-sub { font-size: 0.73rem; color: var(--text-muted); margin-top: 2px; }
        .item-check { color: #38bdf8; font-weight: bold; display: none; }
        .model-dropdown-item.active .item-check { display: inline; }

        .send-btn { width: 38px; height: 38px; border-radius: 50%; background: var(--border-line); border: none; color: var(--text-title); display: flex; align-items: center; justify-content: center; cursor: pointer; flex-shrink: 0; }
        .send-btn:hover { background: var(--border-focus); }

        .popular-tags { display: flex; gap: 8px; margin-top: 20px; flex-wrap: wrap; justify-content: center; }
        .tag-btn { background: var(--bg-card); border: 1px solid var(--border-line); border-radius: 16px; padding: 6px 13px; font-size: 0.82rem; color: var(--text-sub); cursor: pointer; }
        .tag-btn:hover { background: var(--btn-hover); color: var(--text-title); }

        #result-scroll-view { display: none; width: 100%; min-height: 100vh; padding: 16px 12px 100px; }
        .result-inner { max-width: 1220px; margin: 0 auto; width: 100%; }
        
        .card { background: var(--bg-card); border-radius: 16px; padding: 18px 16px; margin-bottom: 14px; border: 1px solid var(--border-line); box-shadow: 0 2px 10px rgba(0,0,0,0.03); max-width: 832px; margin-left: auto; margin-right: auto; }
        
        .chart-dual-grid {
            display: grid; grid-template-columns: 832px 348px; gap: 16px; margin: 0 auto 14px; max-width: 1196px; justify-content: center; align-items: stretch;
        }
        .quant-and-schedule-grid { display:grid; grid-template-columns:minmax(0,832px) 348px; gap:16px; margin:0 auto 14px; max-width:1196px; align-items:stretch; }
        .quant-card-shell { min-width:0; }
        .quant-card-shell > .card { max-width:none; height:100%; margin-bottom:0; }
        .schedule-count { font-size:.60rem; color:var(--text-muted); margin-top:2px; }
        .schedule-dday { font-size:.60rem; font-weight:900; color:#10b981; border:1px solid rgba(16,185,129,.35); background:rgba(16,185,129,.06); border-radius:7px; padding:2px 5px; white-space:nowrap; }
        .schedule-dday.today { color:#ef4444; border-color:rgba(239,68,68,.35); background:rgba(239,68,68,.06); }
        .schedule-item.major { border-color:rgba(16,185,129,.35); }
        .schedule-major-badge { font-size:.56rem; font-weight:900; color:#10b981; margin-left:5px; }
        .schedule-source { font-size:.56rem; color:var(--text-muted); margin-left:auto; }
        .schedule-date-label { display:flex; align-items:center; justify-content:space-between; padding:8px 0 5px; }
        .schedule-date-label span:first-child { font-size:.72rem; font-weight:900; color:var(--text-title); }
        .schedule-date-label span:last-child { font-size:.58rem; color:var(--text-muted); }
        .schedule-card { background:var(--bg-card); border:1px solid var(--border-line); border-radius:16px; padding:16px 14px; height:100%; min-width:0; display:flex; flex-direction:column; overflow:hidden; }
        .schedule-header { display:flex; justify-content:space-between; align-items:flex-start; gap:10px; padding-bottom:10px; border-bottom:1px solid var(--border-line); }
        .schedule-title { font-size:1rem; font-weight:800; color:var(--text-title); }
        .schedule-sub { font-size:.66rem; color:var(--text-muted); margin-top:3px; line-height:1.4; }
        .schedule-refresh { font-size:.62rem; color:#10b981; font-weight:700; white-space:nowrap; }
        .schedule-tabs { display:flex; gap:6px; margin:10px 0; }
        .schedule-tab { flex:1; border:1px solid var(--border-line); background:var(--bg-subcard); color:var(--text-muted); border-radius:9px; padding:7px 4px; font-size:.72rem; font-weight:800; cursor:pointer; }
        .schedule-tab.active { color:var(--text-title); border-color:#10b981; background:rgba(16,185,129,.08); }
        .schedule-list { flex:1; overflow:auto; padding-right:3px; }
        .schedule-day { font-size:.72rem; font-weight:800; color:var(--text-title); padding:8px 0 6px; }
        .schedule-item { border:1px solid var(--border-line); background:var(--bg-subcard); border-radius:10px; padding:8px; margin-bottom:7px; }
        .schedule-item-top { display:flex; align-items:center; gap:6px; }
        .schedule-time { font-size:.62rem; color:var(--text-muted); min-width:53px; }
        .schedule-stars { font-size:.62rem; letter-spacing:-1px; }
        .schedule-country { font-size:.6rem; color:#10b981; font-weight:800; margin-left:auto; }
        .schedule-event { margin-top:4px; font-size:.76rem; font-weight:700; color:var(--text-title); line-height:1.35; word-break:keep-all; }
        .schedule-values { display:grid; grid-template-columns:repeat(3,1fr); gap:5px; margin-top:6px; }
        .schedule-value-box { padding-top:5px; border-top:1px solid var(--border-line); }
        .schedule-value-label { font-size:.58rem; color:var(--text-muted); }
        .schedule-value { font-size:.68rem; color:var(--text-sub); font-weight:700; margin-top:2px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
        .earnings-symbol { color:#10b981; font-weight:900; font-size:.7rem; }
        .earnings-company { font-size:.74rem; font-weight:800; color:var(--text-title); margin-top:3px; line-height:1.3; }
        .earnings-meta { font-size:.62rem; color:var(--text-muted); margin-top:4px; line-height:1.4; }
        .schedule-empty { color:var(--text-muted); font-size:.76rem; padding:16px 4px; text-align:center; }
        @media (max-width:1200px) { .quant-and-schedule-grid{grid-template-columns:1fr;max-width:832px;} .schedule-card{height:520px;} }

        .chart-main-card {
            background: var(--bg-card);
            border-radius: 16px;
            padding: 18px 16px;
            border: 1px solid var(--border-line);
            box-shadow: 0 2px 10px rgba(0,0,0,0.03);
            display: flex;
            flex-direction: column;
            justify-content: space-between;
            width: 100%;
        }

        .price-title { font-size: 1.25rem; font-weight: 700; color: var(--text-title); word-break: keep-all; }
        .price-title span { color: var(--text-muted); font-size: 0.85rem; font-weight: 400; margin-left: 4px; }
        .price-val { font-size: 2.1rem; font-weight: 800; color: var(--text-title); margin: 6px 0; }
        .price-meta { color: var(--text-muted); font-size: 0.78rem; line-height: 1.4; word-break: break-all; }

        .chart-legend { display: flex; flex-wrap: wrap; gap: 8px 12px; font-size: 0.75rem; color: var(--text-muted); margin-top: 10px; }
        .legend-tag { display: flex; align-items: center; gap: 4px; }
        .legend-dot { width: 7px; height: 7px; border-radius: 50%; }
        .chart-box-wrap { width: 100%; height: 300px; margin-top: 10px; position: relative; background: var(--bg-card); border-radius: 8px; overflow: hidden; }

        .global-board-card {
            background: var(--bg-card);
            border: 1px solid var(--border-line);
            border-radius: 16px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.03);
            padding: 16px 14px;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
            width: 100%;
        }

        .board-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 8px;
            padding-bottom: 6px;
            border-bottom: 1px solid var(--border-line);
        }
        .board-header-title { font-size: 0.95rem; font-weight: 800; color: var(--text-title); }
        .board-header-sub { font-size: 0.7rem; color: var(--text-muted); }

        .board-2col-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 6px;
            flex: 1;
        }

        .mini-index-card {
            background: var(--bg-subcard);
            border: 1px solid var(--border-line);
            border-radius: 10px;
            padding: 7px 8px;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
            transition: all 0.2s ease;
            cursor: pointer;
            min-height: 58px;
        }
        .mini-index-card:hover {
            border-color: var(--border-focus);
            background: var(--btn-hover);
            transform: translateY(-1px);
        }
        .mini-top { display: flex; justify-content: space-between; align-items: center; }
        .mini-name { font-size: 0.76rem; font-weight: 800; color: var(--text-title); }
        .mini-tag { 
            font-size: 0.6rem; 
            font-weight: 700;
            color: var(--text-muted); 
            background: var(--btn-hover); 
            border: 1px solid var(--border-line);
            padding: 1px 4px; 
            border-radius: 5px; 
        }
        
        .mini-mid-row {
            display: flex;
            justify-content: space-between;
            align-items: flex-end;
            margin-top: 3px;
        }
        .mini-price { font-size: 0.88rem; font-weight: 800; color: var(--text-title); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; line-height: 1.1; }
        .mini-rate { font-size: 0.72rem; font-weight: 700; line-height: 1.1; margin-top: 1px; }
        .mini-spark { flex-shrink: 0; }

        .section-title { font-size: 1rem; font-weight: 700; color: var(--text-title); margin-bottom: 12px; display: flex; justify-content: space-between; align-items: center; }
        
        .val-up { color: #ef4444; }
        .val-down { color: #38bdf8; }

        .quant-row-item {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 10px 0;
            border-bottom: 1px solid var(--border-line);
            font-size: 0.84rem;
        }
        .quant-row-item:last-child {
            border-bottom: none;
        }
        .quant-row-left {
            display: flex;
            flex-direction: column;
            gap: 3px;
            min-width: 0;
        }
        .quant-row-name {
            font-weight: 700;
            color: var(--text-title);
        }
        .quant-row-status {
            font-size: 0.72rem;
            font-weight: 700;
        }
        .quant-row-right {
            text-align: right;
            flex-shrink: 0;
        }
        .quant-row-val {
            font-weight: 800;
            color: var(--text-title);
        }
        .quant-row-subval {
            font-size: 0.72rem;
            color: var(--text-muted);
            margin-top: 2px;
        }

        .status-pill-positive { color: #10b981; }
        .status-pill-negative { color: #ef4444; }
        .status-pill-warning { color: #d97706; }
        .status-pill-neutral { color: var(--text-muted); }

        .dart-item { padding: 9px 0; border-bottom: 1px solid var(--border-line); text-decoration: none; display: flex; align-items: center; justify-content: space-between; gap: 8px; }
        .dart-item:last-child { border-bottom: none; }
        .dart-title { color: var(--text-main); font-size: 0.83rem; font-weight: 500; line-height: 1.4; flex: 1; min-width: 0; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
        .dart-title:hover { color: #0284c7; }
        .dart-tag { font-size: 0.68rem; font-weight: 700; padding: 2px 6px; border-radius: 6px; flex-shrink: 0; }
        .tag-positive { background: rgba(16, 185, 129, 0.15); color: #10b981; border: 1px solid rgba(16, 185, 129, 0.3); }
        .tag-negative { background: rgba(239, 68, 68, 0.15); color: #ef4444; border: 1px solid rgba(239, 68, 68, 0.3); }
        .tag-warning { background: rgba(249, 115, 22, 0.15); color: #f97316; border: 1px solid rgba(249, 115, 22, 0.3); }
        .tag-neutral { background: var(--btn-hover); color: var(--text-muted); border: 1px solid var(--border-line); }

        .vote-btn-wrap { display: flex; gap: 10px; margin-top: 10px; }
        .vote-btn { flex: 1; padding: 10px; border-radius: 12px; border: 1px solid var(--border-line); background: var(--bg-subcard); color: var(--text-title); font-size: 0.88rem; font-weight: 700; cursor: pointer; display: flex; align-items: center; justify-content: center; gap: 6px; transition: all 0.2s; }
        .vote-btn:hover { background: var(--btn-hover); border-color: var(--border-focus); }
        .vote-btn.active-long { background: rgba(16, 185, 129, 0.15); border-color: #10b981; color: #10b981; }
        .vote-btn.active-short { background: rgba(56, 189, 248, 0.15); border-color: #38bdf8; color: #38bdf8; }

        .comment-input-box { display: flex; gap: 8px; margin-top: 12px; }
        .comment-input-box input { flex: 1; min-width: 0; background: var(--bg-subcard); border: 1px solid var(--border-line); border-radius: 20px; padding: 9px 14px; color: var(--text-title); font-size: 0.82rem; outline: none; }
        .comment-input-box button { background: #10b981; border: none; color: #ffffff; font-size: 0.8rem; font-weight: 700; padding: 0 16px; border-radius: 20px; cursor: pointer; flex-shrink: 0; }
        .comment-item { padding: 9px 0; border-bottom: 1px solid var(--border-line); font-size: 0.82rem; }
        .comment-item:last-child { border-bottom: none; }
        .comment-head { display: flex; justify-content: space-between; color: var(--text-muted); font-size: 0.7rem; margin-bottom: 3px; }
        .comment-nick { font-weight: 700; color: #10b981; }
        .comment-body { color: var(--text-main); word-break: break-all; }

        .signal-premium-card {
            background: var(--bg-card) !important;
            border: 1.8px solid #10b981 !important;
            border-radius: 18px !important;
            padding: 18px 20px !important;
            margin-bottom: 16px !important;
            box-shadow: 0 6px 24px var(--signal-glow) !important;
            transition: all 0.25s ease;
            max-width: 832px;
            margin-left: auto;
            margin-right: auto;
        }

        .signal-card-header {
            display: flex;
            align-items: center;
            justify-content: center;
            position: relative;
            cursor: pointer;
            user-select: none;
            padding: 6px 0;
        }
        .signal-card-title {
            font-size: 1.15rem !important;
            font-weight: 800 !important;
            color: #10b981 !important;
            text-align: center;
            letter-spacing: -0.4px;
        }
        .signal-arrow-icon {
            width: 22px;
            height: 22px;
            fill: #10b981 !important;
            position: absolute;
            right: 4px;
            transition: transform 0.25s ease;
        }

        .signal-premium-btn {
            width: 100%;
            max-width: 832px;
            margin-left: auto;
            margin-right: auto;
            background: var(--bg-card);
            border: 1.8px solid #10b981;
            border-radius: 16px;
            padding: 15px 18px;
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 10px;
            color: #10b981;
            font-size: 0.95rem;
            font-weight: 800;
            text-decoration: none;
            cursor: pointer;
            box-shadow: 0 4px 18px var(--signal-glow);
            margin-bottom: 16px;
            transition: all 0.2s ease;
            word-break: keep-all;
            text-align: center;
        }
        .signal-premium-btn:hover {
            background: rgba(16, 185, 129, 0.1);
            border-color: #059669;
            color: #059669;
        }
        .signal-premium-btn svg {
            fill: #10b981;
            flex-shrink: 0;
            transition: fill 0.2s;
        }
        .signal-premium-btn:hover svg {
            fill: #059669;
        }

        .gauge-inner-box {
            width: 62px;
            height: 62px;
            border-radius: 50%;
            border: 4px solid #10b981;
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            background: var(--bg-subcard);
            flex-shrink: 0;
        }

        .guide-box { padding: 14px 0 0 0; margin-top: 14px; font-size: 0.85rem; line-height: 1.6; color: var(--text-sub); border-top: 1px solid rgba(16, 185, 129, 0.2); }
        .guide-title { font-weight: 800; color: #10b981; margin-bottom: 6px; display: flex; align-items: center; gap: 4px; font-size: 0.92rem; }

        .news-item { padding: 10px 0; border-bottom: 1px solid var(--border-line); text-decoration: none; display: block; }
        .news-item:last-child { border-bottom: none; }
        .news-title { color: var(--text-main); font-size: 0.85rem; font-weight: 500; line-height: 1.4; }
        .news-title:hover { color: #0284c7; }
        .news-meta { color: var(--text-muted); font-size: 0.72rem; margin-top: 4px; }

        #bottom-bar { display: none; position: fixed; bottom: 0; left: 0; right: 0; height: 74px; background: var(--bottom-fade); align-items: center; justify-content: center; padding: 0 12px 6px; z-index: 50; }
        .loading { display: none; text-align: center; color: #0284c7; margin: 40px 0; font-size: 0.9rem; }

        .dot-marker-style {
            position: absolute;
            top: -4px;
            width: 14px;
            height: 14px;
            border-radius: 50%;
            background: #10b981;
            border: 2px solid #ffffff;
            box-shadow: 0 1px 4px rgba(0,0,0,0.3);
            transform: translateX(-50%);
        }

        @media (max-width: 1200px) {
            .chart-dual-grid {
                grid-template-columns: 1fr;
                max-width: 832px;
            }
            .global-board-card {
                margin-top: 4px;
            }
        }

        /* v9: 좌측=종목 분석 / 우측=해외·시장 정보 */
        .dashboard-row { display:grid; grid-template-columns:minmax(0,832px) 348px; gap:16px; width:100%; max-width:1196px; margin:0 auto 14px; align-items:stretch; }
        .dashboard-left,.dashboard-right{min-width:0;width:100%;}
        .dashboard-left{display:flex;flex-direction:column;align-items:center;}
        .dashboard-right{display:flex;flex-direction:column;align-items:stretch;}
        .dashboard-left>.card,.dashboard-left>.signal-premium-card,.dashboard-left>.signal-premium-btn,.dashboard-left>.chart-main-card,.dashboard-left>.quant-card-shell{width:100%;max-width:832px;margin-left:auto;margin-right:auto;}
        .dashboard-right>.global-board-card,.dashboard-right>.schedule-card,.dashboard-right>.card{width:100%;max-width:348px;margin-left:auto;margin-right:auto;}
        .dashboard-row-quant>.dashboard-left>.quant-card-shell,.dashboard-row-quant>.dashboard-right>.schedule-card{height:100%;}
        .dashboard-row-quant>.dashboard-right>.schedule-card{min-height:100%;}
        .dashboard-row-stock-only>.dashboard-right:empty{min-height:1px;}
        @media (max-width:1200px){.dashboard-row{grid-template-columns:1fr;max-width:832px}.dashboard-right>.global-board-card,.dashboard-right>.schedule-card,.dashboard-right>.card{max-width:832px}.dashboard-row-quant>.dashboard-left>.quant-card-shell,.dashboard-row-quant>.dashboard-right>.schedule-card{height:auto}.dashboard-row-stock-only>.dashboard-right:empty{display:none}}
        .dashboard-left .section-title,.dashboard-left .quant-card .section-title{text-align:center;}
    </style>
</head>
<body>

    <div id="sidebar-wrapper">
        <div class="side-top-action">
            <span style="font-weight:700; font-size:1.15rem; color:var(--text-title); cursor:pointer;" onclick="resetToHome()">gaemiGTP</span>
            
            <button class="gemini-menu-icon" onclick="toggleSidebar()" title="사이드 패널 닫기">
                <svg width="22" height="22" viewBox="0 0 24 24" fill="currentColor">
                    <path d="M4 19V5C4 4.45 4.19583 3.97917 4.5875 3.5875C4.97917 3.19583 5.45 3 6 3H18C18.55 3 19.0208 3.19583 19.4125 3.5875C19.8042 3.97917 20 4.45 20 5V19C20 19.55 19.8042 20.0208 19.4125 20.4125C19.0208 20.8042 18.55 21 18 21H6C5.45 21 4.97917 20.8042 4.5875 20.4125C4.19583 20.0208 4 19.55 4 19ZM9 19H18V5H9V19ZM6 5V19V5Z"/>
                </svg>
            </button>
        </div>

        <button class="gemini-new-btn" onclick="resetToHome()">
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <path d="M12 4v16m8-8H4" stroke-linecap="round" stroke-linejoin="round"/>
            </svg>
            <span>새 종목</span>
        </button>

        <div class="sidebar-scroll-area">
            <div class="sidebar-section-title">최근 종목</div>
            <div class="sidebar-item" onclick="doSearch('SK하이닉스');">SK하이닉스</div>
            <div class="sidebar-item" onclick="doSearch('엔비디아');">엔비디아</div>
            <div class="sidebar-item" onclick="doSearch('삼성전자');">삼성전자</div>
            <div class="sidebar-item" onclick="doSearch('비트코인');">비트코인</div>
        </div>
    </div>

    <div id="content-wrapper">
        <header>
            <div class="header-left">
                <button class="gemini-menu-icon" id="headerToggleBtn" onclick="toggleSidebar()" title="사이드 패널 열기">
                    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="3" y1="12" x2="21" y2="12"></line><line x1="3" y1="6" x2="21" y2="6"></line><line x1="3" y1="18" x2="21" y2="18"></line></svg>
                </button>
                <div class="logo-top" id="headerLogo" onclick="resetToHome()">gaemiGTP</div>
            </div>

            <button class="gemini-menu-icon" id="themeToggleBtn" onclick="toggleTheme()" title="화면 테마 변경">
                <span id="themeIconArea"></span>
            </button>
        </header>

        <div id="main-view">
            <div class="hero-title">gaemiGTP</div>
            <div class="hero-sub">종목명 하나면 10초 만에 끝납니다</div>
            <div class="search-container">
                <div class="search-box">
                    <input type="text" id="centerInput" placeholder="종목명 입력 (예: SK하이닉스, 삼성전자, NVDA, BTC)" onkeypress="if(event.keyCode==13) doSearch(this.value);" autocomplete="off">
                    
                    <div class="model-selector-wrap">
                        <div class="model-pill-btn" onclick="toggleModelDropdown('center')">
                            <span class="model-pill-name" id="centerCurrentModel">gaemi</span>
                            <span class="model-pill-arrow">▲</span>
                        </div>
                        <div class="model-dropdown-menu" id="centerDropdown">
                            <div class="model-dropdown-item active" onclick="selectModel('gaemi', 'gaemi')">
                                <div class="item-title">gaemi <span class="item-check">✓</span></div>
                                <div class="item-sub">도움을 받으세요</div>
                            </div>
                            <div class="model-dropdown-item" onclick="selectModel('info', 'info')">
                                <div class="item-title">info <span class="item-check">✓</span></div>
                                <div class="item-sub">정보를 확인하세요</div>
                            </div>
                            <div class="model-dropdown-item" onclick="selectModel('pro', 'pro')">
                                <div class="item-title">pro <span class="item-check">✓</span></div>
                                <div class="item-sub">실력을 키우세요</div>
                            </div>
                        </div>
                    </div>

                    <button class="send-btn" onclick="doSearch(document.getElementById('centerInput').value)">
                        <svg width="18" height="18" viewBox="0 0 24 24"><path d="M12 4L12 20M12 4L5 11M12 4L19 11" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" fill="none"/></svg>
                    </button>
                </div>
            </div>
            <div class="popular-tags">
                <button class="tag-btn" onclick="doSearch('SK하이닉스')">SK하이닉스</button>
                <button class="tag-btn" onclick="doSearch('삼성전자')">삼성전자</button>
                <button class="tag-btn" onclick="doSearch('알테오젠')">알테오젠</button>
                <button class="tag-btn" onclick="doSearch('엔비디아')">엔비디아</button>
                <button class="tag-btn" onclick="doSearch('비트코인')">비트코인</button>
            </div>
        </div>

        <div id="result-scroll-view">
            <div class="result-inner">
                <div class="loading" id="loading">세계 증시, 전자공시 및 실시간 퀀트 데이터를 연산 중입니다...</div>
                <div id="result-card-wrap" style="display:none;">
                    
                    <!-- 1. 메인 차트 + 우측 '증시는 지금' -->
                    <div class="chart-dual-grid">
                        <div class="chart-main-card">
                            <div>
                                <div class="price-title" id="rTitle"></div>
                                <div class="price-val" id="rPrice"></div>
                                <div class="price-meta" id="rMeta"></div>
                                <div class="chart-legend">
                                    <div class="legend-tag"><span class="legend-dot" style="background:#00b4d8;"></span> 20일선 (슬림)</div>
                                    <div class="legend-tag"><span class="legend-dot" style="background:#f97316;"></span> 볼린저 60선 (중심)</div>
                                    <div class="legend-tag"><span class="legend-dot" style="background:#f87171;"></span> 볼린저 상단 (저항)</div>
                                    <div class="legend-tag"><span class="legend-dot" style="background:#38bdf8;"></span> 볼린저 하단 (지지)</div>
                                </div>
                            </div>
                            <div class="chart-box-wrap" id="tvChart"></div>
                        </div>

                        <!-- 우측 10대 지표 보드 -->
                        <div class="global-board-card">
                            <div class="board-header">
                                <span class="board-header-title">증시는 지금</span>
                                <span class="board-header-sub">실시간 연동</span>
                            </div>
                            <div class="board-2col-grid" id="worldBoardGrid"></div>
                        </div>
                    </div>

                    <!-- 2. 차트 바로 아래: 퀀트 분석 + 주요 증시 일정 -->
                    <div class="quant-and-schedule-grid">
                        <div class="quant-card-shell">
                            <div class="card quant-card">
                        <div style="margin-bottom:16px; display:flex; justify-content:space-between; align-items:center;">
                            <span style="font-size:1.05rem; font-weight:800; color:var(--text-title);">퀀트 분석</span>
                            <span id="qTrendBadge" style="font-size:0.75rem; color:#0284c7; font-weight:700;"></span>
                        </div>

                        <div style="display:grid; grid-template-columns:repeat(4,1fr); gap:8px; margin-bottom:12px;">
                            <div style="background:var(--bg-subcard);border:1px solid var(--border-line);border-radius:10px;padding:9px 8px;"><div style="font-size:.62rem;color:var(--text-muted);">상승 모멘텀</div><div style="font-size:.95rem;font-weight:800;color:#10b981;" id="qProMomentum">-</div></div>
                            <div style="background:var(--bg-subcard);border:1px solid var(--border-line);border-radius:10px;padding:9px 8px;"><div style="font-size:.62rem;color:var(--text-muted);">ADX 추세강도</div><div style="font-size:.95rem;font-weight:800;color:var(--text-title);" id="qProAdx">-</div></div>
                            <div style="background:var(--bg-subcard);border:1px solid var(--border-line);border-radius:10px;padding:9px 8px;"><div style="font-size:.62rem;color:var(--text-muted);">시장 대비 RS</div><div style="font-size:.95rem;font-weight:800;color:var(--text-title);" id="qProRs">-</div></div>
                            <div style="background:var(--bg-subcard);border:1px solid var(--border-line);border-radius:10px;padding:9px 8px;"><div style="font-size:.62rem;color:var(--text-muted);">돌파 강도</div><div style="font-size:.95rem;font-weight:800;color:var(--text-title);" id="qProBreakout">-</div></div>
                        </div>

                        <!-- 상단 요약 게이지 & 안전한 모멘텀 상태 뱃지 -->
                        <div style="display:flex; align-items:center; gap:14px; padding-bottom:16px; border-bottom:1px solid var(--border-line);">
                            <div class="gauge-inner-box">
                                <span style="font-size:1.05rem; font-weight:800; color:var(--text-title); line-height:1;" id="qScoreVal"></span>
                                <span style="font-size:0.55rem; color:var(--text-muted); margin-top:3px; font-weight:700;">모멘텀</span>
                            </div>
                            <div style="min-width:0;">
                                <div style="display:flex; align-items:center; gap:6px; flex-wrap:wrap;">
                                    <span style="font-size:0.78rem; color:var(--text-muted);">계량 상태</span>
                                    <span style="background:rgba(16,185,129,0.15); color:#10b981; font-size:0.72rem; font-weight:800; padding:2px 7px; border-radius:10px;" id="qScoreBadge"></span>
                                </div>
                                <div style="font-size:0.74rem; color:var(--text-muted); margin-top:4px; word-break:keep-all;" id="qScoreDots"></div>
                            </div>
                        </div>

                        <!-- 2개 과학적 핵심 통계 지표 -->
                        <div style="display:flex; justify-content:space-between; align-items:flex-end; margin-top:14px;">
                            <div>
                                <div style="font-size:0.72rem; color:var(--text-muted); margin-bottom:2px;">통계적 Z-Score (20D 표준편차)</div>
                                <div style="font-size:1.25rem; font-weight:800; color:#10b981;" id="qZScoreVal"></div>
                            </div>
                            <div style="text-align:right;">
                                <div style="font-size:0.72rem; color:var(--text-muted); margin-bottom:2px;">연환산 변동성 (20D σ)</div>
                                <div style="font-size:1.15rem; font-weight:700; color:var(--text-title);" id="qVolVal"></div>
                            </div>
                        </div>

                        <!-- 52주 가격 위치 슬라이더 바 -->
                        <div style="margin:20px 0 14px; position:relative;">
                            <div style="display:flex; justify-content:space-between; font-size:0.7rem; margin-bottom:6px; white-space:nowrap;">
                                <span style="color:var(--text-muted);" id="qLow52w"></span>
                                <span style="color:#10b981; font-weight:700;" id="qPos52wLabel">52주 가격 위치</span>
                                <span style="color:var(--text-muted);" id="qHigh52w"></span>
                            </div>
                            <div style="width:100%; height:6px; background:var(--bg-subcard); border-radius:4px; position:relative; border:1px solid var(--border-line);">
                                <div style="height:100%; background:linear-gradient(90deg, #38bdf8, #10b981); border-radius:4px;" id="qSliderFill"></div>
                                <div class="dot-marker-style" id="qDotMarker"></div>
                            </div>
                            <div style="display:flex; justify-content:space-between; font-size:0.68rem; color:var(--text-muted); margin-top:6px; white-space:nowrap;">
                                <span>52주 최저</span>
                                <span>52주 최고</span>
                            </div>
                        </div>

                        <!-- 상승 모멘텀 흐름 그래프 -->
                        <div style="margin:18px 0 8px; padding-top:14px; border-top:1px solid var(--border-line);">
                            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;">
                                <span style="font-size:0.82rem; font-weight:700; color:var(--text-sub);">상승 모멘텀 흐름</span>
                                <span style="font-size:0.9rem; font-weight:800; color:#10b981;" id="qMomentumScore">-</span>
                            </div>
                            <div style="font-size:0.68rem; color:var(--text-muted); margin-bottom:6px;" id="qMomentumState">실제 가격·추세·수익률·ADX·OBV·상대강도 기반</div>
                            <div id="qMomentumGraph" style="width:100%; height:150px; overflow:hidden; border:1px solid var(--border-line); border-radius:10px; background:var(--bg-subcard);"></div>
                        </div>

                        <!-- 세부 지표 & 과학적 통계 라벨 리스트 -->
                        <div style="margin-top:16px; border-top:1px solid var(--border-line); padding-top:12px;">
                            <div style="font-size:0.82rem; font-weight:700; color:var(--text-sub); margin-bottom:8px; display:flex; justify-content:space-between;">
                                <span>계량 기술 지표 세부 판정</span>
                                <span style="font-size:0.68rem; color:var(--text-muted); font-weight:normal;">14대 퀀트 알고리즘</span>
                            </div>
                            <div id="quantDetailList"></div>
                        </div>
                            </div>
                        </div>
                        <aside class="schedule-card">
                            <div class="schedule-header">
                                <div><div class="schedule-title">주요 증시 일정</div><div class="schedule-sub">미국·한국 핵심 지표와 주요 기업 실적을 5분 단위 갱신</div></div>
                                <div class="schedule-refresh" id="scheduleUpdated">업데이트 대기</div>
                            </div>
                            <div class="schedule-tabs">
                                <button class="schedule-tab active" id="scheduleTabMacro" onclick="switchScheduleTab('macro')">경제지표</button>
                                <button class="schedule-tab" id="scheduleTabEarnings" onclick="switchScheduleTab('earnings')">기업 실적</button>
                            </div>
                            <div class="schedule-list" id="marketScheduleList"><div class="schedule-empty">일정 데이터를 불러오는 중입니다.</div></div>
                        </aside>
                    </div>


                    <!-- 3. 기술 지표 바로 아래: 종목 뉴스 -->
                    <div class="card">
                        <div class="section-title">종목 뉴스</div>
                        <div id="stockNewsList"></div>
                    </div>

                    <!-- 4. 3대 시장 통합 컨센서스 카드 -->
                    <div class="card" id="unifiedCard" style="display:none;">
                        <div style="margin-bottom:16px;">
                            <span style="font-size:1.05rem; font-weight:700; color:var(--text-title);" id="uTitle"></span>
                        </div>

                        <div style="display:flex; align-items:center; gap:14px; padding-bottom:16px; border-bottom:1px solid var(--border-line);">
                            <div class="gauge-inner-box">
                                <span style="font-size:1.0rem; font-weight:800; color:var(--text-title); line-height:1;" id="uGaugeVal"></span>
                                <span style="font-size:0.55rem; color:var(--text-muted); margin-top:3px;" id="uGaugeLabel"></span>
                            </div>
                            <div style="min-width:0;">
                                <div style="display:flex; align-items:center; gap:6px; flex-wrap:wrap;">
                                    <span style="font-size:0.78rem; color:var(--text-muted);" id="uStatusLabel"></span>
                                    <span style="background:rgba(16,185,129,0.15); color:#10b981; font-size:0.72rem; font-weight:700; padding:2px 7px; border-radius:10px;" id="uBadge"></span>
                                </div>
                                <div style="font-size:0.74rem; color:var(--text-muted); margin-top:4px; word-break:keep-all;" id="uDots"></div>
                            </div>
                        </div>

                        <div style="display:flex; justify-content:space-between; align-items:flex-end; margin-top:14px;">
                            <div>
                                <div style="font-size:0.72rem; color:var(--text-muted); margin-bottom:2px;" id="uPrimaryLabel"></div>
                                <div style="font-size:1.25rem; font-weight:800; color:#10b981;" id="uPrimaryVal"></div>
                            </div>
                            <div style="text-align:right;">
                                <div style="font-size:0.72rem; color:var(--text-muted); margin-bottom:2px;" id="uSecondaryLabel"></div>
                                <div style="font-size:1.15rem; font-weight:700; color:var(--text-title);" id="uSecondaryVal"></div>
                            </div>
                        </div>

                        <div style="margin:20px 0 14px; position:relative;">
                            <div style="display:flex; justify-content:space-between; font-size:0.7rem; margin-bottom:6px; white-space:nowrap;">
                                <span style="color:var(--text-muted);" id="uSliderLeft"></span>
                                <span style="color:#10b981; font-weight:700;" id="uSliderMid"></span>
                                <span style="color:var(--text-muted);" id="uSliderRight"></span>
                            </div>
                            <div style="width:100%; height:6px; background:var(--bg-subcard); border-radius:4px; position:relative; border:1px solid var(--border-line);">
                                <div style="height:100%; background:linear-gradient(90deg, #38bdf8, #10b981); border-radius:4px;" id="uBarFill"></div>
                                <div class="dot-marker-style" id="uDotMarker"></div>
                            </div>
                            <div style="display:flex; justify-content:space-between; font-size:0.68rem; color:var(--text-muted); margin-top:6px; white-space:nowrap;">
                                <span>52주 최저</span>
                                <span>52주 최고</span>
                            </div>
                        </div>

                        <div style="margin-top:16px; border-top:1px solid var(--border-line); padding-top:12px;">
                            <div style="font-size:0.82rem; font-weight:700; color:var(--text-sub); margin-bottom:8px; display:flex; justify-content:space-between;">
                                <span id="uListTitle"></span>
                                <span style="font-size:0.68rem; color:var(--text-muted); font-weight:normal;" id="uSourceTag"></span>
                            </div>
                            <div id="uItemList"></div>
                        </div>
                    </div>

                    <!-- 5. 개미 투표 카드 -->
                    <div class="card">
                        <div class="section-title">
                            <span>개미 투표</span>
                            <span style="font-size:0.72rem; color:var(--text-muted); font-weight:normal;">내일 주가 전망</span>
                        </div>
                        <div style="margin:8px 0;">
                            <div style="display:flex; justify-content:space-between; font-size:0.75rem; font-weight:700; margin-bottom:6px;">
                                <span style="color:#10b981;" id="voteLongPct">상승 50%</span>
                                <span style="color:#38bdf8;" id="voteShortPct">하락 50%</span>
                            </div>
                            <div style="width:100%; height:8px; background:var(--bg-subcard); border-radius:4px; overflow:hidden; display:flex; border:1px solid var(--border-line);">
                                <div style="height:100%; background:#10b981; width:50%; transition:width 0.3s;" id="voteLongBar"></div>
                                <div style="height:100%; background:#38bdf8; width:50%; transition:width 0.3s;" id="voteShortBar"></div>
                            </div>
                            <div style="display:flex; justify-content:space-between; font-size:0.68rem; color:var(--text-muted); margin-top:4px;">
                                <span id="voteLongCount">0명</span>
                                <span id="voteShortCount">0명</span>
                            </div>
                        </div>
                        <div class="vote-btn-wrap">
                            <button class="vote-btn" id="btnVoteLong" onclick="castVote('long')">상승 전망</button>
                            <button class="vote-btn" id="btnVoteShort" onclick="castVote('short')">하락 전망</button>
                        </div>
                    </div>

                    <!-- 6. 개미 토론 카드 -->
                    <div class="card">
                        <div class="section-title">
                            <span>개미 토론</span>
                            <span style="font-size:0.72rem; color:var(--text-muted); font-weight:normal;" id="commentCountTag">0개의 의견</span>
                        </div>
                        <div id="commentList" style="margin-top:8px;">
                            <div style="color:var(--text-muted); font-size:0.8rem; padding:10px 0; text-align:center;">첫 번째 한마디를 남겨보세요!</div>
                        </div>
                        <div class="comment-input-box">
                            <input type="text" id="commentInput" placeholder="이 종목에 대한 생각을 자유롭게 적어주세요" onkeypress="if(event.keyCode==13) postComment();" autocomplete="off" maxlength="80">
                            <button onclick="postComment()">등록</button>
                        </div>
                    </div>

                    <!-- 7. DART 전자공시 카드 -->
                    <div class="card" id="dartCard" style="display:none;">
                        <div class="section-title">
                            <span>DART 전자공시</span>
                            <span style="font-size:0.72rem; color:var(--text-muted); font-weight:normal;">최근 핵심 공시</span>
                        </div>
                        <div id="dartList"></div>
                    </div>

                    <!-- 8. 해외 뉴스 -->
                    <div class="card">
                        <div class="section-title">해외 뉴스</div>
                        <div id="globalNewsList"></div>
                    </div>

                    <!-- 9. gaemiGTP 시그널 박스 -->
                    <div class="signal-premium-card">
                        <div class="signal-card-header" onclick="toggleSignal()">
                            <span class="signal-card-title">gaemiGTP 시그널</span>
                            <svg class="signal-arrow-icon" id="signalArrow" viewBox="0 0 24 24"><path d="M7 10l5 5 5-5z"/></svg>
                        </div>
                        
                        <div id="signalBody" style="display:none; margin-top:16px;">
                            <div style="display:flex; align-items:center; gap:14px; padding-bottom:16px; border-bottom:1px solid rgba(16,185,129,0.2);">
                                <div class="gauge-inner-box">
                                    <span style="font-size:1.05rem; font-weight:800; color:var(--text-title); line-height:1;" id="rWinRateGauge"></span>
                                    <span style="font-size:0.6rem; color:var(--text-muted); margin-top:3px; font-weight:700;">승률</span>
                                </div>
                                <div style="min-width:0;">
                                    <div style="display:flex; align-items:center; gap:6px; flex-wrap:wrap;">
                                        <span style="font-size:0.82rem; color:var(--text-muted);">시그널 종합</span>
                                        <span style="background:rgba(16,185,129,0.15); color:#10b981; font-size:0.75rem; font-weight:800; padding:3px 8px; border-radius:10px;">분할 대응 유효</span>
                                    </div>
                                    <div style="font-size:0.78rem; color:var(--text-muted); margin-top:5px; word-break:keep-all;">
                                        <span style="color:#10b981; font-weight:800;" id="rBt1yTag"></span> &nbsp; 
                                        <span style="color:#10b981; font-weight:800;" id="rBt2yTag"></span> &nbsp; 
                                        <span style="color:#38bdf8; font-weight:800;" id="rRrTag"></span>
                                    </div>
                                </div>
                            </div>

                            <div style="margin:20px 0 14px; position:relative;">
                                <div style="display:flex; justify-content:space-between; font-size:0.75rem; margin-bottom:6px; white-space:nowrap;">
                                    <span style="color:#38bdf8; font-weight:800;" id="rSliderStop"></span>
                                    <span style="color:var(--text-title); font-weight:800;" id="rSliderEntry"></span>
                                    <span style="color:#10b981; font-weight:800;" id="rSliderTarget"></span>
                                </div>
                                <div style="width:100%; height:7px; background:var(--bg-subcard); border-radius:4px; position:relative; border:1px solid var(--border-line);">
                                    <div style="height:100%; background:linear-gradient(90deg, #38bdf8, #10b981); border-radius:4px;" id="rSigBarFill"></div>
                                    <div class="dot-marker-style" id="rSigDotMarker"></div>
                                </div>
                                <div style="display:flex; justify-content:space-between; font-size:0.7rem; color:var(--text-muted); margin-top:6px; white-space:nowrap; font-weight:600;">
                                    <span>방어선</span>
                                    <span>목표선</span>
                                </div>
                            </div>

                            <div style="margin-top:16px; border-top:1px solid rgba(16,185,129,0.2); padding-top:10px;">
                                <div style="display:flex; justify-content:space-between; align-items:center; padding:8px 0; border-bottom:1px solid rgba(16,185,129,0.15); font-size:0.86rem;">
                                    <div><span style="color:var(--text-title); font-weight:700;">1차 진입 기준가</span></div>
                                    <div style="color:var(--text-title); font-weight:800; white-space:nowrap;" id="rEntry1"></div>
                                </div>
                                <div style="display:flex; justify-content:space-between; align-items:center; padding:8px 0; border-bottom:1px solid rgba(16,185,129,0.15); font-size:0.86rem;">
                                    <div><span style="color:var(--text-title); font-weight:700;">2차 분할 기준가</span></div>
                                    <div style="color:var(--text-title); font-weight:800; white-space:nowrap;" id="rEntry2"></div>
                                </div>
                                <div style="display:flex; justify-content:space-between; align-items:center; padding:8px 0; border-bottom:1px solid rgba(16,185,129,0.15); font-size:0.86rem;">
                                    <div><span style="color:#10b981; font-weight:700;">1차 목표가</span></div>
                                    <div style="color:#10b981; font-weight:800; white-space:nowrap;" id="rTarget1"></div>
                                </div>
                                <div style="display:flex; justify-content:space-between; align-items:center; padding:8px 0; border-bottom:1px solid rgba(16,185,129,0.15); font-size:0.86rem;">
                                    <div><span style="color:#10b981; font-weight:700;">2차 목표가</span></div>
                                    <div style="color:#10b981; font-weight:800; white-space:nowrap;" id="rTarget2"></div>
                                </div>
                                <div style="display:flex; justify-content:space-between; align-items:center; padding:8px 0; font-size:0.86rem;">
                                    <div><span style="color:#38bdf8; font-weight:700;">리스크 방어선</span></div>
                                    <div style="color:#38bdf8; font-weight:800; white-space:nowrap;" id="rStop"></div>
                                </div>
                            </div>

                            <div class="guide-box">
                                <div class="guide-title">gaemiGTP 시그널 가이드</div>
                                <div id="rGuideText" style="margin-top:4px; color:var(--text-sub); word-break:keep-all;"></div>
                            </div>
                        </div>
                    </div>

                    <!-- 10. 텔레그램 실시간 알림 받기 버튼 -->
                    <a id="tgAlertLink" href="#" target="_blank" class="signal-premium-btn">
                        <svg width="20" height="20" viewBox="0 0 24 24"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm4.64 6.8c-.15 1.58-.8 5.42-1.13 7.19-.14.75-.42 1-.68 1.03-.58.05-1.02-.38-1.58-.75-.88-.58-1.38-.94-2.23-1.5-.99-.65-.35-1.01.22-1.59.15-.15 2.71-2.48 2.76-2.69a.2.2 0 00-.05-.18c-.06-.05-.14-.03-.21-.02-.09.02-1.49.95-4.22 2.79-.4.27-.76.41-1.08.4-.36-.01-1.04-.2-1.55-.37-.63-.2-1.12-.31-1.08-.66.02-.18.27-.36.74-.55 2.92-1.27 4.86-2.11 5.83-2.51 2.78-1.16 3.35-1.36 3.73-1.36.08 0 .27.02.39.12.1.08.13.19.14.27-.01.06.01.24 0 .38z"/></svg>
                        <span id="tgAlertBtnText">gaemiGTP 시그널 텔레그램 실시간 알림 받기</span>
                    </a>

                </div>
            </div>
        </div>

        <div id="bottom-bar">
            <div class="search-container">
                <div class="search-box">
                    <input type="text" id="bottomInput" placeholder="다른 종목명 입력 (예: 테슬라, 카카오, BTC)" onkeypress="if(event.keyCode==13) doSearch(this.value);" autocomplete="off">
                    
                    <div class="model-selector-wrap">
                        <div class="model-pill-btn" onclick="toggleModelDropdown('bottom')">
                            <span class="model-pill-name" id="bottomCurrentModel">gaemi</span>
                            <span class="model-pill-arrow">▲</span>
                        </div>
                        <div class="model-dropdown-menu" id="bottomDropdown">
                            <div class="model-dropdown-item active" onclick="selectModel('gaemi', 'gaemi')">
                                <div class="item-title">gaemi <span class="item-check">✓</span></div>
                                <div class="item-sub">도움을 받으세요</div>
                            </div>
                            <div class="model-dropdown-item" onclick="selectModel('info', 'info')">
                                <div class="item-title">info <span class="item-check">✓</span></div>
                                <div class="item-sub">정보를 확인하세요</div>
                            </div>
                            <div class="model-dropdown-item" onclick="selectModel('pro', 'pro')">
                                <div class="item-title">pro <span class="item-check">✓</span></div>
                                <div class="item-sub">실력을 키우세요</div>
                            </div>
                        </div>
                    </div>

                    <button class="send-btn" onclick="doSearch(document.getElementById('bottomInput').value)">
                        <svg width="18" height="18" viewBox="0 0 24 24"><path d="M12 4L12 20M12 4L5 11M12 4L19 11" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" fill="none"/></svg>
                    </button>
                </div>
            </div>
        </div>
    </div>

    <script>
        let chart = null;
        let selectedMode = 'gaemi';
        let currentSymbol = '';

        function setTxt(id, val) {
            const el = document.getElementById(id);
            if (el) el.innerText = (val !== undefined && val !== null) ? String(val) : '';
        }
        function setHtml(id, val) {
            const el = document.getElementById(id);
            if (el) el.innerHTML = (val !== undefined && val !== null) ? String(val) : '';
        }

        const SUPABASE_URL = "https://uaqvmsubzgzvxbdcwcad.supabase.co";
        const SUPABASE_KEY = "sb_publishable_bcxDofsnPXgBICDDgQ8Ijw_fNjPcsRW";
        let supabaseClient = null;
        try {
            if (window.supabase) {
                supabaseClient = window.supabase.createClient(SUPABASE_URL, SUPABASE_KEY);
            }
        } catch(e) {
            console.warn("Supabase fallback:", e);
        }

        function initTheme() {
            const saved = localStorage.getItem('gaemi_theme') || 'dark';
            if (saved === 'light') {
                document.body.classList.add('light-theme');
                updateThemeIcon(true);
            } else {
                document.body.classList.remove('light-theme');
                updateThemeIcon(false);
            }
        }

        function toggleTheme() {
            const isLight = document.body.classList.toggle('light-theme');
            localStorage.setItem('gaemi_theme', isLight ? 'light' : 'dark');
            updateThemeIcon(isLight);

            if (chart) {
                chart.applyOptions({
                    layout: {
                        background: { color: isLight ? '#ffffff' : '#1e1f20' },
                        textColor: isLight ? '#5f6368' : '#8e918f'
                    },
                    timeScale: { borderColor: isLight ? '#dadce0' : '#282a2c' },
                    rightPriceScale: { borderColor: isLight ? '#dadce0' : '#282a2c' }
                });
            }
        }

        function updateThemeIcon(isLight) {
            const area = document.getElementById('themeIconArea');
            if (!area) return;
            area.innerHTML = isLight 
                ? `<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"></path></svg>`
                : `<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="5"></circle><line x1="12" y1="1" x2="12" y2="3"></line><line x1="12" y1="21" x2="12" y2="23"></line><line x1="4.22" y1="4.22" x2="5.64" y2="5.64"></line><line x1="18.36" y1="18.36" x2="19.78" y2="19.78"></line><line x1="1" y1="12" x2="3" y2="12"></line><line x1="21" y1="12" x2="23" y2="12"></line><line x1="4.22" y1="19.78" x2="5.64" y2="18.36"></line><line x1="18.36" y1="5.64" x2="19.78" y2="4.22"></line></svg>`;
        }

        function resizeChart() {
            const container = document.getElementById('tvChart');
            if (chart && container && container.clientWidth > 0) {
                chart.applyOptions({ width: container.clientWidth });
            }
        }

        function toggleSidebar() {
            const side = document.getElementById('sidebar-wrapper');
            if (side) side.classList.toggle('open');
        }

        function toggleModelDropdown(target) {
            const centerD = document.getElementById('centerDropdown');
            const bottomD = document.getElementById('bottomDropdown');
            const targetD = target === 'center' ? centerD : bottomD;
            const otherD = target === 'center' ? bottomD : centerD;
            if (otherD) otherD.style.display = 'none';
            if (targetD) targetD.style.display = targetD.style.display === 'block' ? 'none' : 'block';
        }

        function selectModel(modeKey, modeName) {
            selectedMode = modeKey;
            setTxt('centerCurrentModel', modeName);
            setTxt('bottomCurrentModel', modeName);

            ['centerDropdown', 'bottomDropdown'].forEach(id => {
                const menu = document.getElementById(id);
                if (menu) {
                    const items = menu.querySelectorAll('.model-dropdown-item');
                    items.forEach(it => {
                        if (it.innerText.includes(modeName)) {
                            it.classList.add('active');
                        } else {
                            it.classList.remove('active');
                        }
                    });
                    menu.style.display = 'none';
                }
            });
        }

        document.addEventListener('click', function(e) {
            if (!e.target.closest('.model-selector-wrap')) {
                const cd = document.getElementById('centerDropdown');
                const bd = document.getElementById('bottomDropdown');
                if (cd) cd.style.display = 'none';
                if (bd) bd.style.display = 'none';
            }
            if (!e.target.closest('#sidebar-wrapper') && !e.target.closest('#headerToggleBtn')) {
                const side = document.getElementById('sidebar-wrapper');
                if (side && side.classList.contains('open')) {
                    side.classList.remove('open');
                }
            }
        });

        function resetToHome() {
            document.getElementById('main-view').style.display = 'flex';
            document.getElementById('result-scroll-view').style.display = 'none';
            document.getElementById('bottom-bar').style.display = 'none';
            document.getElementById('result-card-wrap').style.display = 'none';
            document.getElementById('loading').style.display = 'none';
            
            const uCard = document.getElementById('unifiedCard');
            if (uCard) uCard.style.display = 'none';
            const dCard = document.getElementById('dartCard');
            if (dCard) dCard.style.display = 'none';
            
            document.getElementById('centerInput').value = '';
            document.getElementById('bottomInput').value = '';
            currentSymbol = '';
            
            const side = document.getElementById('sidebar-wrapper');
            if (side) side.classList.remove('open');
        }

        function toggleSignal() {
            const body = document.getElementById('signalBody');
            const arrow = document.getElementById('signalArrow');
            if (!body || !arrow) return;
            if (body.style.display === 'block') {
                body.style.display = 'none';
                arrow.style.transform = 'rotate(0deg)';
            } else {
                body.style.display = 'block';
                arrow.style.transform = 'rotate(180deg)';
            }
        }

        function renderSparklineSVG(points, isUp) {
            if (!points || points.length < 2) return '';
            const min = Math.min(...points);
            const max = Math.max(...points);
            const range = (max - min) === 0 ? 1 : (max - min);
            const width = 44;
            const height = 16;
            const padY = 2;
            
            const coords = points.map((p, i) => {
                const x = (i / (points.length - 1)) * width;
                const y = height - padY - ((p - min) / range) * (height - padY * 2);
                return `${x.toFixed(1)},${y.toFixed(1)}`;
            });
            
            const pathD = 'M ' + coords.join(' L ');
            const strokeColor = isUp ? '#ef4444' : '#38bdf8';
            
            return `<svg width="${width}" height="${height}" viewBox="0 0 ${width} ${height}" class="mini-spark">
                <path d="${pathD}" fill="none" stroke="${strokeColor}" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/>
            </svg>`;
        }

        async function loadCommunityData(symbol, displayName) {
            currentSymbol = symbol;
            const targetName = displayName || symbol;
            
            const tgLink = document.getElementById('tgAlertLink');
            const tgBtnText = document.getElementById('tgAlertBtnText');
            if (tgLink && tgBtnText) {
                tgLink.href = 'https://t.me/gaemigtp_bot?start=' + encodeURIComponent(symbol);
                tgBtnText.innerText = '[' + targetName + '] gaemiGTP 시그널 텔레그램 실시간 알림 받기';
            }

            const voteKey = 'vote_' + symbol;
            const rawVote = localStorage.getItem(voteKey) || JSON.stringify({ long: 15, short: 8, myVote: null });
            let vData = JSON.parse(rawVote);

            if (supabaseClient) {
                try {
                    const { data, error } = await supabaseClient
                        .from('votes')
                        .select('*')
                        .eq('symbol', symbol)
                        .single();
                    if (data && !error) {
                        vData.long = data.long_count || vData.long;
                        vData.short = data.short_count || vData.short;
                    }
                } catch(e) {}
            }
            updateVoteUI(vData);

            let comments = [];
            if (supabaseClient) {
                try {
                    const { data, error } = await supabaseClient
                        .from('comments')
                        .select('*')
                        .eq('symbol', symbol)
                        .order('created_at', { ascending: false })
                        .limit(8);
                    if (data && !error && data.length > 0) {
                        comments = data.map(d => ({
                            nick: d.nickname,
                            text: d.text,
                            time: formatCommentTime(d.created_at)
                        }));
                    }
                } catch(e) {}
            }

            if (comments.length === 0) {
                const cKey = 'comments_' + symbol;
                const rawComm = localStorage.getItem(cKey) || JSON.stringify([
                    { nick: "수익왕개미", text: "20일선 지지받고 올라갈 차례네요.", time: "방금 전" },
                    { nick: "단타고수", text: "시그널 진입가에 1차 분할 매수 걸어둡니다.", time: "10분 전" }
                ]);
                comments = JSON.parse(rawComm);
            }
            renderComments(comments);
        }

        function formatCommentTime(ts) {
            if (!ts) return "방금 전";
            try {
                const diff = (new Date() - new Date(ts)) / 1000;
                if (diff < 60) return "방금 전";
                if (diff < 3600) return `${Math.floor(diff/60)}분 전`;
                if (diff < 86400) return `${Math.floor(diff/3600)}시간 전`;
                return `${Math.floor(diff/86400)}일 전`;
            } catch(e) {
                return "방금 전";
            }
        }

        function updateVoteUI(vData) {
            const total = vData.long + vData.short;
            const lPct = total > 0 ? Math.round((vData.long / total) * 100) : 50;
            const sPct = 100 - lPct;

            setTxt('voteLongPct', `상승 ${lPct}%`);
            setTxt('voteShortPct', `하락 ${sPct}%`);
            
            const lb = document.getElementById('voteLongBar');
            if (lb) lb.style.width = lPct + '%';
            const sb = document.getElementById('voteShortBar');
            if (sb) sb.style.width = sPct + '%';
            
            setTxt('voteLongCount', `${vData.long}명`);
            setTxt('voteShortCount', `${vData.short}명`);

            const btnL = document.getElementById('btnVoteLong');
            const btnS = document.getElementById('btnVoteShort');
            if (btnL && btnS) {
                btnL.classList.remove('active-long');
                btnS.classList.remove('active-short');
                if (vData.myVote === 'long') btnL.classList.add('active-long');
                if (vData.myVote === 'short') btnS.classList.add('active-short');
            }
        }

        async function castVote(type) {
            if (!currentSymbol) return;
            const voteKey = 'vote_' + currentSymbol;
            const rawVote = localStorage.getItem(voteKey) || JSON.stringify({ long: 15, short: 8, myVote: null });
            const vData = JSON.parse(rawVote);

            if (vData.myVote === type) return;

            if (vData.myVote === 'long') vData.long = Math.max(0, vData.long - 1);
            if (vData.myVote === 'short') vData.short = Math.max(0, vData.short - 1);

            if (type === 'long') vData.long += 1;
            if (type === 'short') vData.short += 1;

            vData.myVote = type;
            localStorage.setItem(voteKey, JSON.stringify(vData));
            updateVoteUI(vData);

            if (supabaseClient) {
                try {
                    await supabaseClient
                        .from('votes')
                        .upsert({
                            symbol: currentSymbol,
                            long_count: vData.long,
                            short_count: vData.short
                        });
                } catch(e) {}
            }
        }

        function renderComments(list) {
            const wrap = document.getElementById('commentList');
            if (!wrap) return;
            setTxt('commentCountTag', `${list.length}개의 의견`);
            wrap.innerHTML = '';
            if (list.length === 0) {
                wrap.innerHTML = '<div style="color:var(--text-muted); font-size:0.8rem; padding:10px 0; text-align:center;">첫 번째 한마디를 남겨보세요!</div>';
                return;
            }
            list.slice(0, 8).forEach(c => {
                wrap.innerHTML += `
                    <div class="comment-item">
                        <div class="comment-head">
                            <span class="comment-nick">${c.nick}</span>
                            <span>${c.time}</span>
                        </div>
                        <div class="comment-body">${c.text}</div>
                    </div>
                `;
            });
        }

        async function postComment() {
            const input = document.getElementById('commentInput');
            if (!input) return;
            const txt = input.value.trim();
            if (!txt || !currentSymbol) return;

            const nicks = ["익명개미", "불꽃슈퍼개미", "성투개미", "황금손개미", "존버대장", "퀀트장인"];
            const randomNick = nicks[Math.floor(Math.random() * nicks.length)] + "#" + Math.floor(100 + Math.random() * 900);

            const cKey = 'comments_' + currentSymbol;
            const rawComm = localStorage.getItem(cKey) || '[]';
            const comments = JSON.parse(rawComm);

            const newC = {
                nick: randomNick,
                text: txt,
                time: "방금 전"
            };

            comments.unshift(newC);
            localStorage.setItem(cKey, JSON.stringify(comments));
            renderComments(comments);
            input.value = '';

            if (supabaseClient) {
                try {
                    await supabaseClient
                        .from('comments')
                        .insert({
                            symbol: currentSymbol,
                            nickname: randomNick,
                            text: txt
                        });
                } catch(e) {}
            }
        }

        function renderMomentumGraph(values, dates) {
            const wrap = document.getElementById('qMomentumGraph');
            if (!wrap) return;
            if (!values || values.length < 2) {
                wrap.innerHTML = '<div style="height:100%;display:flex;align-items:center;justify-content:center;color:var(--text-muted);font-size:0.78rem;">상승 모멘텀을 계산할 실제 데이터가 부족합니다.</div>';
                return;
            }
            const w = Math.max(300, wrap.clientWidth || 700), h = 150, pad = {l:10,r:10,t:12,b:22};
            const minV=0,maxV=100, innerW=w-pad.l-pad.r, innerH=h-pad.t-pad.b;
            const pts=values.map((v,i)=>{const x=pad.l+(i/(values.length-1))*innerW; const y=pad.t+(1-((v-minV)/(maxV-minV)))*innerH; return [x,y,v];});
            const line=pts.map((p)=>`${p[0].toFixed(1)},${p[1].toFixed(1)}`).join(' ');
            const last=pts[pts.length-1];
            const themeLight=document.body.classList.contains('light-theme');
            const grid=themeLight?'#dadce0':'#282a2c'; const text=themeLight?'#5f6368':'#8e918f';
            wrap.innerHTML=`<svg width="100%" height="100%" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none">
                <line x1="${pad.l}" y1="${pad.t+innerH*0.2}" x2="${w-pad.r}" y2="${pad.t+innerH*0.2}" stroke="${grid}" stroke-width="1"/>
                <line x1="${pad.l}" y1="${pad.t+innerH*0.5}" x2="${w-pad.r}" y2="${pad.t+innerH*0.5}" stroke="${grid}" stroke-width="1"/>
                <line x1="${pad.l}" y1="${pad.t+innerH*0.8}" x2="${w-pad.r}" y2="${pad.t+innerH*0.8}" stroke="${grid}" stroke-width="1"/>
                <polyline points="${line}" fill="none" stroke="#10b981" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/>
                <circle cx="${last[0]}" cy="${last[1]}" r="4.5" fill="#10b981"/>
                <text x="${pad.l}" y="${h-5}" fill="${text}" font-size="10">${dates[0]||''}</text>
                <text x="${w-pad.r}" y="${h-5}" fill="${text}" font-size="10" text-anchor="end">${dates[dates.length-1]||'현재'}</text>
                <text x="${w-pad.r}" y="${pad.t+8}" fill="#10b981" font-size="10" text-anchor="end">100</text>
                <text x="${w-pad.r}" y="${pad.t+innerH/2+4}" fill="${text}" font-size="10" text-anchor="end">50</text>
                <text x="${w-pad.r}" y="${pad.t+innerH+2}" fill="${text}" font-size="10" text-anchor="end">0</text>
            </svg>`;
        }

        window.addEventListener('resize', ()=>renderMomentumGraph(window.__momentumValues||[],window.__momentumDates||[]));

        let currentScheduleTab='macro'; let lastSchedule=null;
        function switchScheduleTab(tab){ currentScheduleTab=tab; const a=document.getElementById('scheduleTabMacro'),b=document.getElementById('scheduleTabEarnings'); if(a)a.classList.toggle('active',tab==='macro'); if(b)b.classList.toggle('active',tab==='earnings'); if(lastSchedule)renderMarketSchedule(lastSchedule); }
        function escText(v){return String(v??'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;');}
        function renderMarketSchedule(data){ lastSchedule=data||{economic:[],earnings:[]}; setTxt('scheduleUpdated',lastSchedule.updated_at||'데이터 없음'); const wrap=document.getElementById('marketScheduleList'); if(!wrap)return; const arr=currentScheduleTab==='earnings'?(lastSchedule.earnings||[]):(lastSchedule.economic||[]); if(!arr.length){wrap.innerHTML='<div class="schedule-empty">현재 표시할 일정이 없습니다.</div>';return;} let out='',last=''; arr.forEach(x=>{if(x.date!==last){const d=new Date(x.date+'T00:00:00'); const ds=isNaN(d.getTime())?x.date:d.toLocaleDateString('ko-KR',{month:'numeric',day:'numeric',weekday:'short'}); out+=`<div class="schedule-date-label"><span>${escText(ds)}</span><span>${escText(x.d_day||'')}</span></div>`;last=x.date;} if(currentScheduleTab==='earnings'){const major=x.major?' major':''; const today=x.d_day==='TODAY'?' today':''; out+=`<div class="schedule-item${major}"><div class="schedule-item-top"><span class="earnings-symbol">${escText(x.symbol||'-')}</span>${x.major?'<span class="schedule-major-badge">핵심</span>':''}<span class="schedule-country">${escText(x.d_day||'-')}</span></div><div class="earnings-company">${escText(x.company||'-')}</div><div class="earnings-meta">${escText(x.event||'실적발표')} · ${escText(x.time||'-')} · EPS 예상 ${escText(x.eps_est||'-')} · 시총 ${escText(x.market_cap||'-')}</div></div>`;}else{const today=x.d_day==='TODAY'?' today':''; out+=`<div class="schedule-item"><div class="schedule-item-top"><span class="schedule-time">${escText(x.time_utc||'-')}</span><span class="schedule-stars" style="color:#f59e0b">${escText(x.importance_label||'★☆☆')}</span><span class="schedule-country">${escText(x.country||'-')}</span><span class="schedule-dday${today}">${escText(x.d_day||'')}</span></div><div class="schedule-event">${escText(x.event||'-')}</div><div class="schedule-values"><div class="schedule-value-box"><div class="schedule-value-label">실제</div><div class="schedule-value">${escText(x.actual||'-')}</div></div><div class="schedule-value-box"><div class="schedule-value-label">예상</div><div class="schedule-value">${escText(x.forecast||'-')}</div></div><div class="schedule-value-box"><div class="schedule-value-label">이전</div><div class="schedule-value">${escText(x.previous||'-')}</div></div></div></div>`;}}); wrap.innerHTML=out;}
        async function loadMarketSchedule(){ try{const r=await fetch('/api/schedule'); const d=await r.json(); if(!r.ok||d.error)throw new Error(d.error||'일정 조회 실패'); renderMarketSchedule(d);}catch(e){console.warn('schedule',e);const w=document.getElementById('marketScheduleList');if(w)w.innerHTML='<div class="schedule-empty">일정 데이터를 불러오지 못했습니다.<br>잠시 후 다시 시도합니다.</div>';}}

        async function doSearch(query) {
            const cleanQ = query.trim();
            if (!cleanQ) return;

            document.getElementById('centerInput').value = cleanQ;
            document.getElementById('bottomInput').value = cleanQ;

            document.getElementById('main-view').style.display = 'none';
            document.getElementById('result-scroll-view').style.display = 'block';
            document.getElementById('bottom-bar').style.display = 'flex';
            document.getElementById('loading').style.display = 'block';
            document.getElementById('result-card-wrap').style.display = 'none';

            if (window.innerWidth <= 900) {
                const side = document.getElementById('sidebar-wrapper');
                if (side) side.classList.remove('open');
            }

            try {
                const url = '/api/analyze?q=' + encodeURIComponent(cleanQ);
                const response = await fetch(url);
                const data = await response.json();
                if (!response.ok || data.error) {
                    alert(data.error || '종목을 찾을 수 없거나 분석 중 오류가 발생했습니다.');
                    document.getElementById('loading').style.display = 'none';
                    return;
                }

                const isKrw = data.market === '국내주식' || data.market === '국내증시';
                const displayName = data.name || data.formal_name;
                const displayPrice = data.price || data.price_str;
                const displayTime = data.time || data.now_str;

                setHtml('rTitle', `${displayName} <span>(${data.symbol})</span>`);
                setTxt('rPrice', displayPrice);
                setHtml('rMeta', `시장: ${data.market} · 출처: ${isKrw ? 'Naver/KRX' : 'Yahoo/Binance'} · 시각: ${displayTime}`);

                loadCommunityData(data.symbol, displayName);
                loadMarketSchedule();

                const wBoardGrid = document.getElementById('worldBoardGrid');
                if (wBoardGrid) {
                    wBoardGrid.innerHTML = '';
                    if (data.world_board) {
                        data.world_board.forEach(w => {
                            const cls = w.up ? 'val-up' : 'val-down';
                            const sparkSvg = renderSparklineSVG(w.sparkline, w.up);
                            wBoardGrid.innerHTML += `
                                <div class="mini-index-card" onclick="doSearch('${w.name === 'USD/KRW' ? '환율' : w.name}')">
                                    <div class="mini-top">
                                        <span class="mini-name">${w.name}</span>
                                        <span class="mini-tag">${w.tag}</span>
                                    </div>
                                    <div class="mini-mid-row">
                                        <div>
                                            <div class="mini-price">${w.price}</div>
                                            <div class="mini-rate ${cls}">${w.rate}</div>
                                        </div>
                                        ${sparkSvg}
                                    </div>
                                </div>
                            `;
                        });
                    }
                }

                const q = data.quant;
                if (q) {
                    setTxt('qTrendBadge', q.trend);
                    setTxt('qScoreVal', q.quant_score);
                    setTxt('qScoreBadge', q.score_badge);
                    setHtml('qScoreDots', q.score_dots);

                    setTxt('qZScoreVal', q.z_score);
                    setTxt('qVolVal', `${q.ann_vol} (${q.vol_risk_state ? q.vol_risk_state.split(' ')[0] : ''})`);

                    setTxt('qLow52w', `최저 ${q.low_52w}`);
                    setTxt('qHigh52w', `최고 ${q.high_52w}`);
                    
                    const qFill = document.getElementById('qSliderFill');
                    if (qFill) qFill.style.width = q.slider_fill_pct + '%';
                    const qDot = document.getElementById('qDotMarker');
                    if (qDot) qDot.style.left = q.slider_fill_pct + '%';

                    setTxt('qMomentumScore', q.momentum_score === '-' ? '-' : `모멘텀 ${q.momentum_score}`);
                    setTxt('qProMomentum', q.momentum_score || '-');
                    setTxt('qProAdx', q.adx || '-');
                    setTxt('qProRs', q.rs_1m || '-');
                    setTxt('qProBreakout', q.breakout_strength || '-');
                    setTxt('qMomentumState', q.momentum_state || '데이터 부족');
                    window.__momentumValues = q.momentum_history || []; window.__momentumDates = q.momentum_dates || []; renderMomentumGraph(window.__momentumValues, window.__momentumDates);

                    const qList = document.getElementById('quantDetailList');
                    if (qList) {
                        qList.innerHTML = `
                            <div class="quant-row-item">
                                <div class="quant-row-left">
                                    <span class="quant-row-name">통계적 Z-Score</span>
                                    <span class="quant-row-status status-pill-${q.z_color}">${q.z_state}</span>
                                </div>
                                <div class="quant-row-right">
                                    <div class="quant-row-val">${q.z_score}</div>
                                    <div class="quant-row-subval">20D 표준점수</div>
                                </div>
                            </div>
                            <div class="quant-row-item">
                                <div class="quant-row-left">
                                    <span class="quant-row-name">20일 연환산 변동성</span>
                                    <span class="quant-row-status status-pill-${q.vol_risk_color}">${q.vol_risk_state}</span>
                                </div>
                                <div class="quant-row-right">
                                    <div class="quant-row-val">${q.ann_vol}</div>
                                    <div class="quant-row-subval">통계적 변동률</div>
                                </div>
                            </div>
                            <div class="quant-row-item">
                                <div class="quant-row-left">
                                    <span class="quant-row-name">5일 이동평균선</span>
                                    <span class="quant-row-status status-pill-${q.ma5_color}">${q.ma5_state}</span>
                                </div>
                                <div class="quant-row-right">
                                    <div class="quant-row-val">${q.ma5}</div>
                                    <div class="quant-row-subval">단기 모멘텀선</div>
                                </div>
                            </div>
                            <div class="quant-row-item">
                                <div class="quant-row-left">
                                    <span class="quant-row-name">20일 이동평균선</span>
                                    <span class="quant-row-status status-pill-${q.ma20_color}">${q.ma20_state}</span>
                                </div>
                                <div class="quant-row-right">
                                    <div class="quant-row-val">${q.ma20}</div>
                                    <div class="quant-row-subval">중기 추세선</div>
                                </div>
                            </div>
                            <div class="quant-row-item">
                                <div class="quant-row-left">
                                    <span class="quant-row-name">60일 이동평균선</span>
                                    <span class="quant-row-status status-pill-${q.ma60_color}">${q.ma60_state}</span>
                                </div>
                                <div class="quant-row-right">
                                    <div class="quant-row-val">${q.ma60}</div>
                                    <div class="quant-row-subval">중장기 수급선</div>
                                </div>
                            </div>
                            <div class="quant-row-item">
                                <div class="quant-row-left">
                                    <span class="quant-row-name">볼린저 밴드 (60, 2)</span>
                                    <span class="quant-row-status status-pill-${q.bb_color}">${q.bb_state}</span>
                                </div>
                                <div class="quant-row-right">
                                    <div class="quant-row-val">${q.bollinger}</div>
                                    <div class="quant-row-subval">상단 / 하단</div>
                                </div>
                            </div>
                            <div class="quant-row-item">
                                <div class="quant-row-left">
                                    <span class="quant-row-name">MACD (12, 26, 9)</span>
                                    <span class="quant-row-status status-pill-${q.macd_color}">${q.macd_state}</span>
                                </div>
                                <div class="quant-row-right">
                                    <div class="quant-row-val">${q.macd}</div>
                                    <div class="quant-row-subval">모멘텀 지표</div>
                                </div>
                            </div>
                            <div class="quant-row-item">
                                <div class="quant-row-left">
                                    <span class="quant-row-name">RSI (14)</span>
                                    <span class="quant-row-status status-pill-${q.rsi_color}">${q.rsi_state}</span>
                                </div>
                                <div class="quant-row-right">
                                    <div class="quant-row-val">${q.rsi} pt</div>
                                    <div class="quant-row-subval">상대강도지수</div>
                                </div>
                            </div>
                            <div class="quant-row-item">
                                <div class="quant-row-left">
                                    <span class="quant-row-name">거래량 / 5일평균</span>
                                    <span class="quant-row-status status-pill-${q.vol_color}">${q.vol_state}</span>
                                </div>
                                <div class="quant-row-right">
                                    <div class="quant-row-val">${q.vol_ratio}</div>
                                    <div class="quant-row-subval">유동성 비율</div>
                                </div>
                            </div>
                            <div class="quant-row-item"><div class="quant-row-left"><span class="quant-row-name">상승 모멘텀 점수</span><span class="quant-row-status status-pill-positive">${q.momentum_state}</span></div><div class="quant-row-right"><div class="quant-row-val">${q.momentum_score}</div><div class="quant-row-subval">1·3·6·12M + 추세 + ADX + OBV + RS</div></div></div>
                            <div class="quant-row-item"><div class="quant-row-left"><span class="quant-row-name">ADX</span><span class="quant-row-status status-pill-${q.adx_color}">${q.adx_state}</span></div><div class="quant-row-right"><div class="quant-row-val">${q.adx}</div><div class="quant-row-subval">추세 강도</div></div></div>
                            <div class="quant-row-item"><div class="quant-row-left"><span class="quant-row-name">OBV 흐름</span><span class="quant-row-status status-pill-${q.obv_color}">${q.obv_state}</span></div><div class="quant-row-right"><div class="quant-row-val">${q.obv_state}</div><div class="quant-row-subval">가격·거래량 누적</div></div></div>
                            <div class="quant-row-item"><div class="quant-row-left"><span class="quant-row-name">수익률 모멘텀</span><span class="quant-row-status status-pill-neutral">실측</span></div><div class="quant-row-right"><div class="quant-row-val">1M ${q.momentum_1m} · 3M ${q.momentum_3m}</div><div class="quant-row-subval">6M ${q.momentum_6m} · 12M ${q.momentum_12m}</div></div></div>
                            <div class="quant-row-item"><div class="quant-row-left"><span class="quant-row-name">시장 대비 상대강도</span><span class="quant-row-status status-pill-neutral">실측</span></div><div class="quant-row-right"><div class="quant-row-val">1M ${q.rs_1m}</div><div class="quant-row-subval">3M ${q.rs_3m}</div></div></div>
                            <div class="quant-row-item"><div class="quant-row-left"><span class="quant-row-name">고점 돌파 강도</span><span class="quant-row-status status-pill-neutral">실측</span></div><div class="quant-row-right"><div class="quant-row-val">${q.breakout_strength}</div><div class="quant-row-subval">20·60·120일 고점</div></div></div>
                        `;
                    }

                    setTxt('rWinRateGauge', q.win_rate_num);
                    setTxt('rBt1yTag', q.bt_1y);
                    setTxt('rBt2yTag', q.bt_2y);
                    setTxt('rRrTag', q.rr_str);

                    setTxt('rSliderStop', q.stop_p);
                    setTxt('rSliderEntry', `기준 ${q.entry1}`);
                    setTxt('rSliderTarget', q.target2);

                    const sFill = document.getElementById('rSigBarFill');
                    if (sFill) sFill.style.width = q.sig_fill_pct + '%';
                    const sDot = document.getElementById('rSigDotMarker');
                    if (sDot) sDot.style.left = q.sig_fill_pct + '%';

                    setTxt('rEntry1', q.entry1);
                    setTxt('rEntry2', q.entry2);
                    setTxt('rTarget1', q.target1);
                    setTxt('rTarget2', q.target2);
                    setTxt('rStop', q.stop_p);
                    setTxt('rGuideText', q.guide_text);
                }

                const sNews = document.getElementById('stockNewsList');
                if (sNews) {
                    sNews.innerHTML = '';
                    if (data.stock_news && data.stock_news.length > 0) {
                        data.stock_news.forEach(n => {
                            sNews.innerHTML += `
                                <a href="${n.link}" target="_blank" class="news-item">
                                    <div class="news-title">${n.title}</div>
                                    <div class="news-meta">${n.media} · ${n.pubDate}</div>
                                </a>
                            `;
                        });
                    } else {
                        sNews.innerHTML = '<div style="color:var(--text-muted); font-size:0.85rem;">관련 뉴스가 없습니다.</div>';
                    }
                }

                const c = data.consensus;
                const uCard = document.getElementById('unifiedCard');
                if (c && uCard) {
                    setTxt('uTitle', c.title);
                    setTxt('uGaugeVal', c.gauge_val);
                    setTxt('uGaugeLabel', c.gauge_label);
                    setTxt('uStatusLabel', c.status_label);
                    setTxt('uBadge', c.badge_text);
                    setHtml('uDots', c.dots);

                    setTxt('uPrimaryLabel', c.primary_label);
                    setTxt('uPrimaryVal', c.primary_val);
                    setTxt('uSecondaryLabel', c.secondary_label);
                    setTxt('uSecondaryVal', c.secondary_val);

                    setTxt('uSliderLeft', c.slider_left);
                    setTxt('uSliderMid', c.slider_mid);
                    setTxt('uSliderRight', c.slider_right);
                    setTxt('uSliderBottomLeft', c.slider_bottom_left);
                    setTxt('uSliderBottomRight', c.slider_bottom_right);

                    const uFill = document.getElementById('uBarFill');
                    if (uFill) uFill.style.width = c.fill_pct + '%';
                    const uDot = document.getElementById('uDotMarker');
                    if (uDot) uDot.style.left = c.fill_pct + '%';

                    setTxt('uListTitle', c.list_title);
                    setTxt('uSourceTag', c.source_tag);

                    const iList = document.getElementById('uItemList');
                    if (iList) {
                        iList.innerHTML = '';
                        if (c.items && c.items.length > 0) {
                            c.items.forEach((it, idx) => {
                                const isLast = idx === c.items.length - 1;
                                iList.innerHTML += `
                                    <div style="display:flex; justify-content:space-between; align-items:center; padding:8px 0; ${isLast ? '' : 'border-bottom:1px solid var(--border-line);'} font-size:0.82rem;">
                                        <div>
                                            <div style="color:var(--text-title); font-weight:700;">${it.main}</div>
                                            <div style="font-size:0.7rem; color:var(--text-muted); margin-top:2px;">${it.sub}</div>
                                        </div>
                                        <div style="text-align:right;">
                                            <div style="color:var(--text-title); font-weight:700;">${it.val}</div>
                                            <div style="color:#10b981; font-size:0.72rem; font-weight:700;">${it.sub_val}</div>
                                        </div>
                                    </div>
                                `;
                            });
                        }
                    }
                    uCard.style.display = 'block';
                } else if (uCard) {
                    uCard.style.display = 'none';
                }

                const dCard = document.getElementById('dartCard');
                const dList = document.getElementById('dartList');
                if (dCard && dList) {
                    if (data.dart_disclosures && data.dart_disclosures.length > 0) {
                        dList.innerHTML = '';
                        data.dart_disclosures.forEach(d => {
                            dList.innerHTML += `
                                <a href="${d.link}" target="_blank" class="dart-item">
                                    <span class="dart-tag tag-${d.tag_type}">${d.tag_name}</span>
                                    <span class="dart-title">${d.title}</span>
                                    <span style="color:var(--text-muted); font-size:0.72rem; flex-shrink:0;">${d.date}</span>
                                </a>
                            `;
                        });
                        dCard.style.display = 'block';
                    } else {
                        dCard.style.display = 'none';
                    }
                }

                const gNews = document.getElementById('globalNewsList');
                if (gNews) {
                    gNews.innerHTML = '';
                    if (data.global_news && data.global_news.length > 0) {
                        data.global_news.forEach(n => {
                            gNews.innerHTML += `
                                <a href="${n.link}" target="_blank" class="news-item">
                                    <div class="news-title">${n.title}</div>
                                    <div class="news-meta">${n.media} · ${n.pubDate}</div>
                                </a>
                            `;
                        });
                    } else {
                        gNews.innerHTML = '<div style="color:var(--text-muted); font-size:0.85rem;">해외 증시 뉴스를 불러오는 중입니다.</div>';
                    }
                }

                document.getElementById('loading').style.display = 'none';
                document.getElementById('result-card-wrap').style.display = 'block';

                setTimeout(() => {
                    initTvChart(data.candles, data.ma20_line, data.ma60_line, data.bb_up_line, data.bb_dn_line);
                }, 50);

                window.scrollTo({top: 0, behavior: 'smooth'});

            } catch (err) {
                console.error("Analysis Render Error:", err);
                alert('데이터를 처리하는 중 오류가 발생했습니다: ' + err.message);
                document.getElementById('loading').style.display = 'none';
            }
        }

        function initTvChart(candles, ma20, ma60, bbUp, bbDn) {
            const container = document.getElementById('tvChart');
            if (!container) return;
            container.innerHTML = '';

            const isLight = document.body.classList.contains('light-theme');

            chart = LightweightCharts.createChart(container, {
                width: container.clientWidth || 700,
                height: 300,
                layout: { 
                    background: { color: isLight ? '#ffffff' : '#1e1f20' }, 
                    textColor: isLight ? '#5f6368' : '#8e918f' 
                },
                grid: {
                    vertLines: { color: 'rgba(0,0,0,0)' },
                    horzLines: { color: 'rgba(0,0,0,0)' }
                },
                crosshair: {
                    vertLine: { visible: false },
                    horzLine: { visible: false }
                },
                timeScale: { borderColor: isLight ? '#dadce0' : '#282a2c', timeVisible: true },
                rightPriceScale: { borderColor: isLight ? '#dadce0' : '#282a2c' }
            });

            const candleSeries = chart.addCandlestickSeries({
                upColor: '#ef4444',
                downColor: '#38bdf8',
                borderDownColor: '#38bdf8',
                borderUpColor: '#ef4444',
                wickDownColor: '#38bdf8',
                wickUpColor: '#ef4444',
                priceLineVisible: false,
                lastValueVisible: true
            });
            candleSeries.setData(candles || []);

            const lineMa20 = chart.addLineSeries({ color: '#00b4d8', lineWidth: 1, priceLineVisible: false, lastValueVisible: false });
            lineMa20.setData(ma20 || []);

            const lineMa60 = chart.addLineSeries({ color: '#f97316', lineWidth: 1.5, priceLineVisible: false, lastValueVisible: false });
            lineMa60.setData(ma60 || []);

            const lineBbUp = chart.addLineSeries({ color: '#f87171', lineWidth: 1.2, priceLineVisible: false, lastValueVisible: false });
            lineBbUp.setData(bbUp || []);

            const lineBbDn = chart.addLineSeries({ color: '#38bdf8', lineWidth: 1.2, priceLineVisible: false, lastValueVisible: false });
            lineBbDn.setData(bbDn || []);

            chart.timeScale().fitContent();

            window.addEventListener('resize', resizeChart);
        }

        function buildV9DashboardLayout() {
            const wrap = document.getElementById('result-card-wrap');
            if (!wrap || wrap.dataset.v9Built === '1') return;
            const chartGrid = wrap.querySelector('.chart-dual-grid');
            const quantGrid = wrap.querySelector('.quant-and-schedule-grid');
            if (!chartGrid || !quantGrid) return;
            const chartMain = chartGrid.querySelector('.chart-main-card');
            const worldBoard = chartGrid.querySelector('.global-board-card');
            const quantShell = quantGrid.querySelector('.quant-card-shell');
            const scheduleCard = quantGrid.querySelector('.schedule-card');
            const stockNews = wrap.querySelector('#stockNewsList')?.closest('.card');
            const globalNews = wrap.querySelector('#globalNewsList')?.closest('.card');
            const unifiedCard = wrap.querySelector('#unifiedCard');
            const dartCard = wrap.querySelector('#dartCard');
            const signalCard = wrap.querySelector('.signal-premium-card');
            const tgButton = wrap.querySelector('#tgAlertLink');
            const voteCard = Array.from(wrap.children).find(el => el.querySelector?.('#voteLongPct')) || null;
            const commentCard = Array.from(wrap.children).find(el => el.querySelector?.('#commentList')) || null;
            if (!chartMain || !worldBoard || !quantShell || !scheduleCard) return;
            const rows=[];
            const makeRow=(name='')=>{ const row=document.createElement('div'); row.className='dashboard-row '+name; const left=document.createElement('div'); left.className='dashboard-left'; const right=document.createElement('div'); right.className='dashboard-right'; row.append(left,right); rows.push(row); return {row,left,right}; };
            let r=makeRow('dashboard-row-top'); r.left.appendChild(chartMain); r.right.appendChild(worldBoard);
            r=makeRow('dashboard-row-quant'); r.left.appendChild(quantShell); r.right.appendChild(scheduleCard);
            r=makeRow('dashboard-row-lower'); if(stockNews)r.left.appendChild(stockNews); if(globalNews)r.right.appendChild(globalNews);
            [unifiedCard,voteCard,commentCard,dartCard,signalCard,tgButton].forEach(el=>{ if(!el)return; const x=makeRow('dashboard-row-stock-only'); x.left.appendChild(el); });
            wrap.innerHTML=''; rows.forEach(x=>wrap.appendChild(x.row)); wrap.dataset.v9Built='1';
        }
        window.addEventListener('DOMContentLoaded', () => { initTheme(); buildV9DashboardLayout(); });
    </script>
</body>
</html>
"""

# ----------------------------------------------------
# 8. 멀티스레드 웹 서버
# ----------------------------------------------------
class WebHandler(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)

        if parsed.path == "/api/schedule":
            try:
                data=get_market_schedule()
                self.send_response(200); self.send_header('Content-Type','application/json; charset=utf-8'); self.send_header('Access-Control-Allow-Origin','*'); self.end_headers(); self.wfile.write(json.dumps(data,ensure_ascii=False).encode('utf-8'))
            except Exception as exc:
                self.send_response(500); self.send_header('Content-Type','application/json; charset=utf-8'); self.send_header('Access-Control-Allow-Origin','*'); self.end_headers(); self.wfile.write(json.dumps({"error":"주요 일정 데이터를 불러오지 못했습니다.","detail":str(exc)},ensure_ascii=False).encode('utf-8'))
            return

        if parsed.path == "/api/analyze":
            q = params.get('q', [''])[0].strip()
            try:
                data = run_full_pipeline(q) if q else None
            except Exception as exc:
                print(f"[API] analyze failed: {exc}")
                self.send_response(500); self.send_header('Content-Type','application/json; charset=utf-8'); self.send_header('Access-Control-Allow-Origin','*'); self.end_headers(); self.wfile.write(json.dumps({"error":"서버 처리 중 오류가 발생했습니다.","detail":str(exc)},ensure_ascii=False).encode('utf-8')); return
            if not data:
                self.send_response(404)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({"error": "종목을 찾을 수 없습니다."}, ensure_ascii=False).encode('utf-8'))
                return
            if isinstance(data, dict) and data.get("__error__"):
                self.send_response(503)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({"error": data["__error__"]}, ensure_ascii=False).encode('utf-8'))
                return

            self.send_response(200)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(data, ensure_ascii=False).encode('utf-8'))
            return

        self.send_response(200)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.end_headers()
        self.wfile.write(HTML_PAGE.encode('utf-8'))

def run_web_server():
    port = int(os.environ.get("PORT", 10000))
    server = ThreadingHTTPServer(("0.0.0.0", port), WebHandler)
    server.serve_forever()

if __name__ == "__main__":
    run_web_server()
