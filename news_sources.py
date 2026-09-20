"""gaemiGTP 뉴스 수집기 — Google News 단일 기준."""
from __future__ import annotations
import html
import time
import urllib.parse
import xml.etree.ElementTree as ET
from typing import Any
from performance import HTTP_SESSION
_GOOGLE_CACHE = {}
_GOOGLE_CACHE_TTL = 300

def _clean_text(value: Any) -> str:
    return html.unescape(str(value or "")).strip()

def _google_rss(query: str, limit: int = 10):
    query = _clean_text(query)
    limit = max(1, min(int(limit or 10), 20))
    cache_key=(query,limit)
    now=time.time()
    cached=_GOOGLE_CACHE.get(cache_key)
    if cached and now-cached['time'] < _GOOGLE_CACHE_TTL:
        return list(cached['data'])
    url='https://news.google.com/rss/search?' + urllib.parse.urlencode({'q':query,'hl':'ko','gl':'KR','ceid':'KR:ko'})
    rows=[]
    try:
        r=HTTP_SESSION.get(url,timeout=4.0,headers={'User-Agent':'Mozilla/5.0 gaemiGTP/1.0'})
        r.raise_for_status()
        root=ET.fromstring(r.content)
        seen=set()
        for item in root.findall('./channel/item'):
            title=_clean_text(item.findtext('title')); link=_clean_text(item.findtext('link')); pub=_clean_text(item.findtext('pubDate'))
            source=item.find('source'); media=_clean_text(source.text if source is not None else 'Google News') or 'Google News'
            key=(title,link)
            if not title or key in seen: continue
            seen.add(key)
            rows.append({'title':title,'link':link,'media':media,'pubDate':pub,'source':'Google News'})
            if len(rows)>=limit: break
    except Exception as exc:
        print(f'[GOOGLE NEWS] fetch failed: {type(exc).__name__}: {exc}')
    _GOOGLE_CACHE[cache_key]={'time':now,'data':list(rows)}
    return rows

def fetch_google_stock_news(symbol='', market_name='', formal_name='', limit=10):
    parts=[]
    if _clean_text(formal_name): parts.append(f'"{_clean_text(formal_name)}"')
    if _clean_text(symbol): parts.append(f'"{_clean_text(symbol).upper()}"')
    return _google_rss(' OR '.join(parts),limit) if parts else []

def fetch_google_economic_news(market_name='국내주식', limit=10):
    market=_clean_text(market_name) or '국내주식'
    if '미국' in market:
        q='미국 증시 OR 뉴욕증시 OR 나스닥 OR 연준 OR 금리 OR 달러 OR 미국 경제'
    elif '암호' in market or '코인' in market:
        q='가상자산 OR 비트코인 OR 이더리움 OR 암호화폐 OR 코인시장'
    else:
        q='한국 증시 OR 코스피 OR 코스닥 OR 금리 OR 환율 OR 한국 경제 OR 금융시장'
    return _google_rss(q,limit)
