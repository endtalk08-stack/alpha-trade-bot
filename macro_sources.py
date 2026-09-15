import os
import requests

from cache import FRED_MIN_INTERVAL, _rate_limit_wait, _cache_get, _cache_set
from performance import HTTP_SESSION

FRED_API_KEY = os.environ.get("FRED_API_KEY", "").strip()
ALPHA_VANTAGE_API_KEY = os.environ.get("ALPHA_VANTAGE_API_KEY", "").strip()

FRED_CACHE = {}
FRED_CACHE_TTL = 900  # 15분
ALPHA_NEWS_CACHE = {}
ALPHA_NEWS_CACHE_TTL = 600  # 10분
ALPHA_EARNINGS_CACHE = {}
ALPHA_EARNINGS_CACHE_TTL = 1800  # 30분

def _safe_float(value):
    try:
        return float(value)
    except Exception:
        return None

def _fred_series(series_id, limit=8):
    """FRED 실제 관측값. 캐시와 요청 간격을 적용해 429를 예방한다."""
    if not FRED_API_KEY:
        return []
    key=f"{series_id}:{limit}"
    cached=_cache_get(FRED_CACHE,key,FRED_CACHE_TTL)
    if cached is not None:
        return cached
    try:
        _rate_limit_wait(FRED_MIN_INTERVAL)
        url = "https://api.stlouisfed.org/fred/series/observations"
        params = {
            "series_id": series_id, "api_key": FRED_API_KEY, "file_type": "json",
            "sort_order": "desc", "limit": int(limit)
        }
        res = HTTP_SESSION.get(url, params=params, timeout=4.0)
        if res.status_code == 429:
            print(f"[FRED] rate limited: {series_id}")
            return []
        res.raise_for_status()
        obs = res.json().get("observations", [])
        out=[]
        for x in obs:
            v=_safe_float(x.get("value"))
            if v is not None:
                out.append({"date":x.get("date"),"value":v})
        return _cache_set(FRED_CACHE,key,out)
    except Exception as exc:
        print(f"[FRED] {series_id} fetch failed: {exc}")
        return []
def fetch_fred_macro_snapshot():
    """FRED 실제 관측값 스냅샷. 15분 캐시로 불필요한 반복 호출을 막는다."""
    cache_key="macro_snapshot"
    cached=_cache_get(FRED_CACHE,cache_key,FRED_CACHE_TTL)
    if cached is not None:
        return cached
    series={
        "us10y":"DGS10","us2y":"DGS2","fedfunds":"FEDFUNDS",
        "cpi":"CPIAUCSL","unemployment":"UNRATE","payroll":"PAYEMS",
    }
    result={}
    for name,sid in series.items():
        obs=_fred_series(sid,8)
        if obs:
            result[name]={"series_id":sid,"observations":obs}
    return _cache_set(FRED_CACHE,cache_key,result)
def fetch_alpha_vantage_news(symbol="", topics=""):
    """Alpha Vantage 뉴스. 캐시를 사용하며 키가 없으면 빈 결과를 반환."""
    if not ALPHA_VANTAGE_API_KEY:
        return []
    key=(symbol or "").upper()+"|"+(topics or "")
    cached=_cache_get(ALPHA_NEWS_CACHE,key,ALPHA_NEWS_CACHE_TTL)
    if cached is not None:
        return cached
    try:
        params={"function":"NEWS_SENTIMENT","apikey":ALPHA_VANTAGE_API_KEY,"limit":20}
        if symbol: params["tickers"]=symbol
        if topics: params["topics"]=topics
        res=HTTP_SESSION.get("https://www.alphavantage.co/query",params=params,timeout=4.0).json()
        feed=res.get("feed",[])
        out=[]
        for item in feed:
            out.append({"title":item.get("title",""),"description":item.get("summary",""),
                        "url":item.get("url",""),"published":item.get("time_published",""),
                        "source":item.get("source","Alpha Vantage"),
                        "sentiment":item.get("overall_sentiment_label",""),
                        "sentiment_score":_safe_float(item.get("overall_sentiment_score"))})
        return _cache_set(ALPHA_NEWS_CACHE,key,out)
    except Exception as exc:
        print(f"[ALPHA NEWS] fetch failed: {exc}")
        return []
def fetch_alpha_vantage_earnings(symbol):
    """실제 EPS actual/estimate/surprise를 반환. 추정 목표가는 절대 생성하지 않는다."""
    if not ALPHA_VANTAGE_API_KEY or not symbol:
        return {}
    key=symbol.upper()
    cached=_cache_get(ALPHA_EARNINGS_CACHE,key,ALPHA_EARNINGS_CACHE_TTL)
    if cached is not None:
        return cached
    try:
        params={"function":"EARNINGS","symbol":key,"apikey":ALPHA_VANTAGE_API_KEY}
        res=HTTP_SESSION.get("https://www.alphavantage.co/query",params=params,timeout=5.0).json()
        q=res.get("quarterlyEarnings",[])
        rows=[]
        for x in q[:8]:
            actual=_safe_float(x.get("reportedEPS"))
            estimate=_safe_float(x.get("estimatedEPS"))
            surprise=_safe_float(x.get("surprisePercentage"))
            if actual is None and estimate is None:
                continue
            rows.append({"fiscalDateEnding":x.get("fiscalDateEnding"),
                         "reportedDate":x.get("reportedDate"),
                         "actual_eps":actual,"expected_eps":estimate,
                         "surprise_pct":surprise,"source":"Alpha Vantage EARNINGS"})
        return _cache_set(ALPHA_EARNINGS_CACHE,key,{"quarterly":rows})
    except Exception as exc:
        print(f"[ALPHA EARNINGS] {symbol} fetch failed: {exc}")
        return {}
