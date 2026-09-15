import os
import re
import json
import html
import urllib.parse
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None
from concurrent.futures import ThreadPoolExecutor

from performance import HTTP_SESSION
from cache import _cache_get, _cache_set

FINLIGHT_API_KEY = os.environ.get("FINLIGHT_API_KEY", "").strip()
NEWS_CACHE = {}
NEWS_CACHE_TTL = 600
NEWS_TRANSLATION_CACHE = {}
NEWS_TRANSLATION_CACHE_TTL = 21600
NEWS_TRANSLATION_EXECUTOR = ThreadPoolExecutor(
    max_workers=6,
    thread_name_prefix="gaemi-news-translate",
)

def fetch_naver_news(query_keyword, limit=5):
    """Google News RSS 실패/빈 결과 시 사용하는 Naver News API fallback."""
    headlines = []
    client_id = os.environ.get("NAVER_CLIENT_ID", "").strip()
    client_secret = os.environ.get("NAVER_CLIENT_SECRET", "").strip()

    if not client_id or not client_secret:
        print("[NAVER NEWS] API credentials not configured; fallback skipped")
        return headlines

    clean_k = re.sub(r'\(.*?\)', '', str(query_keyword or "")).replace("USDT", "").strip()
    if not clean_k:
        return headlines

    try:
        r = HTTP_SESSION.get(
            "https://openapi.naver.com/v1/search/news.json",
            params={
                "query": clean_k,
                "display": max(1, min(int(limit), 100)),
                "start": 1,
                "sort": "date",
            },
            headers={
                "X-Naver-Client-Id": client_id,
                "X-Naver-Client-Secret": client_secret,
                "Accept": "application/json",
                "User-Agent": "Mozilla/5.0",
            },
            timeout=3.0,
        )
        print(f"[NAVER NEWS] query={clean_k} HTTP {r.status_code}")
        if r.status_code != 200:
            return headlines

        data = r.json()
        for item in (data.get("items") or [])[:limit]:
            title = html.unescape(
                re.sub(r"<[^>]+>", "", str(item.get("title") or ""))
            ).strip()
            description = html.unescape(
                re.sub(r"<[^>]+>", "", str(item.get("description") or ""))
            ).strip()

            if not title:
                continue

            parts = title.rsplit(" - ", 1)
            if len(parts) > 1 and len(parts[1]) <= 80:
                title_text = parts[0].strip()
                media_name = parts[1].strip()
            else:
                title_text = title
                media_name = "Naver 뉴스"

            headlines.append({
                "title": title_text,
                "media": media_name,
                "pubDate": str(item.get("pubDate") or "").strip(),
                "link": str(item.get("originallink") or item.get("link") or "#").strip(),
                "description": description,
                "source": "Naver",
            })
    except Exception as exc:
        print(f"[NAVER NEWS] fallback failed: {type(exc).__name__}: {exc}")

    return headlines


def _format_finlight_pubdate(value):
    """Finlight ISO 시간을 기존 뉴스 UI용 한국시간 문자열로 변환한다."""
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if ZoneInfo is not None:
            dt = dt.astimezone(ZoneInfo("Asia/Seoul"))
        else:
            dt = dt.astimezone(timezone(timedelta(hours=9)))
        return dt.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return raw


def _finlight_relevance_score(item, target_name=""):
    """
    감성(호재/악재)이 아니라 '투자에 쓸모 있는가'를 기준으로 순위를 매긴다.

    핵심 원칙:
    1) 종목명이 제목에 직접 등장하는 뉴스는 강하게 우선한다.
    2) 실적/수주/투자/규제/정책/업황처럼 주가에 영향을 줄 가능성이 높은 단어를 가산한다.
    3) 셰프/연예/행사/제품 체험처럼 주가 영향이 약한 일반 뉴스는 감점한다.
    4) 호재/악재 자체를 판단하지 않는다. 중요한 '재료'를 위로 올리는 용도다.
    """
    categories = {str(x).lower() for x in (item.get("categories") or [])}
    title = re.sub(r"\s+", " ", str(item.get("title") or "")).strip().lower()
    summary = re.sub(r"\s+", " ", str(item.get("summary") or "")).strip().lower()
    text = f"{title} {summary}"

    score = 0

    # Finlight의 구조화 카테고리: 시장/경제/정책성 뉴스에 높은 기본점수.
    high_value = {"markets", "economy", "regulation", "geopolitics", "commodities", "energy"}
    medium_value = {"technology", "business", "crypto", "politics", "security"}
    score += 3 * len(categories & high_value)
    score += 2 * len(categories & medium_value)

    # 선택 종목과 직접 관련된 제목을 가장 강하게 우선한다.
    target = str(target_name or "").strip().lower()
    target_terms = {target} if target else set()
    # 국내 주요 종목의 실제 기사 표기 약칭도 함께 본다.
    if "삼성전자" in target:
        target_terms.update({"삼성전자", "삼성", "삼전", "삼전우"})
    if "sk하이닉스" in target or "하이닉스" in target:
        target_terms.update({"sk하이닉스", "하이닉스", "sk하닉"})

    if any(term and term in title for term in target_terms):
        score += 8
    elif any(term and term in summary for term in target_terms):
        score += 3

    # 국내/미국 주식에서 실제 가격 재료가 되기 쉬운 핵심 키워드.
    market_keywords = [
        "실적", "영업익", "영업이익", "순이익", "매출", "가이던스", "전망", "컨센서스",
        "수주", "계약", "공급", "납품", "투자", "증설", "생산", "공장", "capex",
        "hbм", "hbm", "메모리", "반도체", "ai", "인공지능", "데이터센터",
        "인수", "합병", "지분", "자사주", "배당", "목표주가", "투자의견",
        "외국인", "기관", "순매수", "순매도", "관세", "수출규제", "수입규제",
        "제재", "규제", "정책", "금리", "연준", "fed", "cpi", "pce", "고용",
        "환율", "달러", "유가", "원유", "전력", "전력기기", "업황", "가격",
        "공급망", "특허", "기술", "협력", "파트너십", "mou", "ceo", "경영진",
    ]
    score += sum(2 for kw in market_keywords if kw in text)

    # 투자 가치가 낮은 일반 기사/홍보성 제목은 감점한다.
    low_value_keywords = [
        "셰프", "맛집", "먹방", "연예", "배우", "가수", "축제", "전시",
        "캠페인", "봉사", "사회공헌", "팝업", "체험", "콘서트", "팬미팅",
        "갤러리", "화보", "패션", "브랜드 행사", "신제품 출시", "출시 행사",
        "광고", "모델", "홍보대사", "이벤트", "사은품", "프로모션",
        "쿠킹쇼", "셰프와", "클래스 열린다", "클래스", "요리", "배워요",
    ]
    score -= sum(5 for kw in low_value_keywords if kw in title)

    return score


def fetch_finlight_news(query="", symbol="", market_name="", formal_name="", limit=10,
                        language="en", countries=None, categories=None, cache_tag=""):
    """Finlight REST API를 기존 gaemiGTP 뉴스 포맷으로 변환한다.

    API Key는 Render 환경변수 FINLIGHT_API_KEY에서만 읽는다.
    실패/빈 결과는 호출부에서 기존 뉴스 소스로 fallback할 수 있도록 []을 반환한다.
    """
    if not FINLIGHT_API_KEY:
        print("[FINLIGHT NEWS] FINLIGHT_API_KEY not configured")
        return []

    try:
        lim = max(1, min(int(limit), 20))
    except Exception:
        lim = 10

    payload = {
        "query": str(query or "").strip(),
        "language": language or "en",
        "pageSize": lim,
        "orderBy": "publishDate",
        "order": "DESC",
    }
    if countries:
        payload["countries"] = list(countries)
    if categories:
        payload["categories"] = list(categories)

    # 빈 query 키는 보내지 않는다.
    if not payload["query"]:
        payload.pop("query", None)

    cache_key = "finlight|{}|{}|{}|{}|{}|{}".format(
        cache_tag or "news",
        symbol or "",
        market_name or "",
        language or "en",
        json.dumps(countries or [], ensure_ascii=False, sort_keys=True),
        json.dumps(categories or [], ensure_ascii=False, sort_keys=True),
    )
    cache_key += "|" + (query or "") + f"|{lim}"
    cached = _cache_get(NEWS_CACHE, cache_key, NEWS_CACHE_TTL)
    if cached is not None:
        return list(cached) if isinstance(cached, list) else []

    try:
        res = HTTP_SESSION.post(
            "https://api.finlight.me/v2/articles",
            headers={
                "accept": "application/json",
                "Content-Type": "application/json",
                "X-API-KEY": FINLIGHT_API_KEY,
            },
            json=payload,
            timeout=4.5,
        )
        if res.status_code != 200:
            print(f"[FINLIGHT NEWS] HTTP {res.status_code} query={query!r}")
            return []

        data = res.json() if res.content else {}
        articles = data.get("articles") or []
        out = []
        seen = set()
        target = str(formal_name or "").strip()

        for item in articles:
            if not isinstance(item, dict):
                continue
            title = html.unescape(str(item.get("title") or "")).strip()
            link = str(item.get("link") or "").strip()
            if not title or not link:
                continue
            key = re.sub(r"\s+", " ", title).lower()
            if key in seen:
                continue
            seen.add(key)

            source = str(item.get("source") or "Finlight").strip()
            pub = _format_finlight_pubdate(item.get("publishDate"))
            categories_item = item.get("categories") or []
            summary = html.unescape(str(item.get("summary") or "")).strip()

            out.append({
                "title": title,
                "media": source,
                "pubDate": pub,
                "link": link,
                "description": summary,
                "source": "Finlight",
                "categories": categories_item,
                "countries": item.get("countries") or [],
                "language": item.get("language") or language,
                "relevance_score": _finlight_relevance_score(item, target),
            })

        # 감성은 사용하지 않는다. 투자 관련 카테고리와 최신성을 우선한다.
        out.sort(key=lambda x: (int(x.get("relevance_score") or 0), x.get("pubDate") or ""), reverse=True)
        out = out[:lim]
        print(f"[FINLIGHT NEWS] query={query!r} lang={language} 결과={len(out)}건")
        return _cache_set(NEWS_CACHE, cache_key, out)
    except Exception as exc:
        print(f"[FINLIGHT NEWS] fetch failed: {type(exc).__name__}: {exc}")
        return []


def _translate_news_title_en_to_ko(title):
    """영문 뉴스 제목을 한국어로 번역한다. 실패하면 원문을 그대로 반환한다."""
    raw = html.unescape(str(title or "")).strip()
    if not raw:
        return raw
    ko_count = len(re.findall(r"[가-힣]", raw))
    alpha_count = len(re.findall(r"[A-Za-z]", raw))
    if ko_count >= 2 or alpha_count < 4:
        return raw
    cache_key = f"en>ko:{raw}"
    cached = _cache_get(NEWS_TRANSLATION_CACHE, cache_key, NEWS_TRANSLATION_CACHE_TTL)
    if cached:
        return str(cached)
    try:
        response = HTTP_SESSION.get(
            "https://translate.googleapis.com/translate_a/single",
            params=[
                ("client", "gtx"), ("sl", "en"), ("tl", "ko"),
                ("dt", "t"), ("q", raw),
            ],
            timeout=3.0,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        if response.status_code != 200:
            return raw
        payload = response.json()
        translated = "".join(
            str(part[0]) for part in (payload[0] or [])
            if isinstance(part, list) and part and part[0]
        ).strip()
        if not translated:
            return raw
        return _cache_set(NEWS_TRANSLATION_CACHE, cache_key, translated)
    except Exception as exc:
        print(f"[NEWS TRANSLATION] failed: {type(exc).__name__}: {exc}")
        return raw


def translate_news_titles_to_korean(items, market_name=""):
    """미국주식 화면의 영문 기사 제목을 병렬 번역한다. 원문 링크/출처는 유지한다."""
    if market_name != "미국주식" or not items:
        return items or []
    news_items = [dict(x) for x in items if isinstance(x, dict)]
    if not news_items:
        return []
    futures = {}
    for idx, item in enumerate(news_items):
        title = str(item.get("title") or "").strip()
        if title:
            futures[idx] = NEWS_TRANSLATION_EXECUTOR.submit(
                _translate_news_title_en_to_ko, title
            )
    for idx, future in futures.items():
        try:
            translated = future.result(timeout=3.5)
            if translated and translated != news_items[idx].get("title"):
                news_items[idx]["title"] = translated
                news_items[idx]["title_original"] = str(news_items[idx].get("title_original") or "") or str(items[idx].get("title") or "")
        except Exception as exc:
            print(f"[NEWS TRANSLATION] result failed: {type(exc).__name__}: {exc}")
    return news_items


def fetch_finlight_stock_news(symbol="", market_name="", formal_name="", limit=20):
    """
    종목 뉴스 공용 수집기.
    기존 Finlight 자리를 유지하되 실제 뉴스 공급원은 Naver News API를 사용한다.
    미국/국내 모두 한국어 뉴스로 받아 번역 API를 사용하지 않는다.
    """
    limit = max(1, min(int(limit), 20))
    name = str(formal_name or "").strip()
    ticker = str(symbol or "").strip().upper()

    if not name and ticker:
        name = ticker

    queries = []
    if name:
        queries.append(name)
    if market_name == "미국주식" and ticker and ticker != name:
        queries.append(f"{name} {ticker}")

    merged = []
    seen = set()
    for query in queries or [ticker]:
        rows = fetch_naver_news(query, min(limit, 10))
        for row in rows:
            if not isinstance(row, dict):
                continue
            title_key = re.sub(r"\s+", " ", str(row.get("title") or "")).strip().lower()
            if not title_key or title_key in seen:
                continue
            seen.add(title_key)
            item = dict(row)
            item["source"] = "Naver"
            item["relevance_score"] = _finlight_relevance_score(
                {
                    "title": item.get("title", ""),
                    "summary": item.get("description", ""),
                    "categories": [],
                },
                name,
            )
            merged.append(item)

    merged.sort(
        key=lambda x: (
            int(x.get("relevance_score") or 0),
            str(x.get("pubDate") or ""),
        ),
        reverse=True,
    )
    return merged[:limit]


def fetch_finlight_economic_news(market_name="", limit=10):
    """
    경제/시장 뉴스 공용 수집기.
    기존 Finlight 자리를 유지하되 실제 뉴스 공급원은 Naver News API를 사용한다.
    미국주식도 한국어 뉴스로 받아 번역 API를 사용하지 않는다.
    """
    limit = max(1, min(int(limit), 20))

    if market_name in {"국내주식", "국내증시"}:
        queries = [
            "미국증시 나스닥 연준 금리 반도체 AI 원유 환율",
            "미국증시 마감 나스닥",
        ]
    else:
        queries = [
            "미국증시 나스닥 연준 금리 반도체 AI 원유",
            "미국증시 마감",
        ]

    merged = []
    seen = set()
    for query in queries:
        rows = fetch_naver_news(query, min(limit, 10))
        for row in rows:
            if not isinstance(row, dict):
                continue
            title_key = re.sub(r"\s+", " ", str(row.get("title") or "")).strip().lower()
            if not title_key or title_key in seen:
                continue
            seen.add(title_key)
            item = dict(row)
            item["source"] = "Naver"
            item["relevance_score"] = _economic_news_priority(item, market_name)
            merged.append(item)

    merged.sort(
        key=lambda x: (
            int(x.get("relevance_score") or 0),
            str(x.get("pubDate") or ""),
        ),
        reverse=True,
    )

    final = merged[:limit]
    if final:
        return final

    return fetch_naver_news("미국증시 나스닥", limit)


def _fetch_rss_news_uncached(query_keyword, limit=5):
    """Google News RSS 우선. 실패/빈 결과이면 Naver News API로 fallback."""
    headlines = []
    clean_k = re.sub(r'\(.*?\)', '', query_keyword).replace("USDT", "").strip()
    url = f"https://news.google.com/rss/search?q={urllib.parse.quote(clean_k)}&hl=ko&gl=KR&ceid=KR:ko"

    try:
        r = HTTP_SESSION.get(url, timeout=2.5)
        if r.status_code == 200:
            root = ET.fromstring(r.content)
            for it in root.findall('.//item')[:limit]:
                t = it.find('title')
                pub = it.find('pubDate')
                link = it.find('link')
                if t is not None and t.text:
                    title_clean = html.unescape(t.text).strip()
                    parts = title_clean.rsplit(' - ', 1)
                    title_text = parts[0]
                    media_name = parts[1] if len(parts) > 1 else "언론사"
                    pub_text = pub.text if pub is not None else ""
                    link_text = link.text if link is not None else "#"
                    headlines.append({
                        "title": title_text,
                        "media": media_name,
                        "pubDate": pub_text,
                        "link": link_text
                    })
    except Exception as exc:
        print(f"[GOOGLE NEWS] RSS failed: {type(exc).__name__}: {exc}")

    if not headlines:
        print(f"[NEWS FALLBACK] Google News empty -> Naver News: {clean_k}")
        headlines = fetch_naver_news(clean_k, limit)

    return headlines


def fetch_rss_news(query_keyword, limit=5):
    """뉴스 공용 캐시를 사용한다. 빈 결과도 짧게 캐시해 반복 실패를 막는다."""
    clean_k = re.sub(r'\(.*?\)', '', str(query_keyword or '')).replace('USDT', '').strip()
    if not clean_k:
        return []
    try:
        lim = max(1, min(int(limit), 20))
    except Exception:
        lim = 5
    key = f"{clean_k.lower()}|{lim}"
    cached = _cache_get(NEWS_CACHE, key, NEWS_CACHE_TTL)
    if cached is not None:
        return list(cached) if isinstance(cached, list) else cached
    result = _fetch_rss_news_uncached(clean_k, lim)
    return _cache_set(NEWS_CACHE, key, list(result or []))

