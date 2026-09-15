import os
import re
import json
import html
import urllib.parse
import time
import xml.etree.ElementTree as ET
import threading
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


NEWS_TRANSLATION_BATCH_LOCK = threading.Lock()
NEWS_TRANSLATION_LAST_REQUEST_TS = 0.0
NEWS_TRANSLATION_MIN_INTERVAL = 0.6


NEWS_TRANSLATION_BATCH_LOCK = threading.Lock()
NEWS_TRANSLATION_LAST_REQUEST_TS = 0.0
NEWS_TRANSLATION_MIN_INTERVAL = 1.0
NEWS_TRANSLATION_BATCH_SIZE = 4
NEWS_TRANSLATION_BACKOFF_UNTIL = 0.0


def _translation_market_enabled(market_name):
    normalized = str(market_name or "").strip()
    return normalized in {"미국주식", "미국증시", "해외주식"}


def _translate_news_title_en_to_ko(title):
    """영문 뉴스 제목 1건을 한국어로 번역한다. 실패 시 원문을 유지한다."""
    raw = html.unescape(str(title or "")).strip()
    if not raw:
        return raw

    ko_count = len(re.findall(r"[가-힣]", raw))
    alpha_count = len(re.findall(r"[A-Za-z]", raw))
    if ko_count >= 2 or alpha_count < 4:
        return raw

    cache_key = f"en>ko:{raw}"
    cached = _cache_get(
        NEWS_TRANSLATION_CACHE,
        cache_key,
        NEWS_TRANSLATION_CACHE_TTL,
    )
    if cached:
        return str(cached)

    result = _translate_news_titles_batch_to_korean([raw])
    return str(result.get(0) or raw)


def _translate_news_titles_batch_to_korean(titles):
    """여러 영문 뉴스 제목을 작은 배치로 묶어 한국어로 번역한다."""
    if not titles:
        return {}

    result = {}
    pending = []

    for idx, title in enumerate(titles):
        raw = html.unescape(str(title or "")).strip()
        if not raw:
            result[idx] = raw
            continue

        ko_count = len(re.findall(r"[가-힣]", raw))
        alpha_count = len(re.findall(r"[A-Za-z]", raw))
        if ko_count >= 2 or alpha_count < 4:
            result[idx] = raw
            continue

        cache_key = f"en>ko:{raw}"
        cached = _cache_get(
            NEWS_TRANSLATION_CACHE,
            cache_key,
            NEWS_TRANSLATION_CACHE_TTL,
        )
        if cached:
            result[idx] = str(cached)
        else:
            pending.append((idx, raw, cache_key))

    if not pending:
        return result

    with NEWS_TRANSLATION_BATCH_LOCK:
        global NEWS_TRANSLATION_LAST_REQUEST_TS
        global NEWS_TRANSLATION_BACKOFF_UNTIL

        # 같은 요청이 연달아 들어오면 외부 번역 서버를 두드리지 않는다.
        now = time.time()
        if now < NEWS_TRANSLATION_BACKOFF_UNTIL:
            for idx, raw, _ in pending:
                result.setdefault(idx, raw)
            return result

        still_pending = []
        for idx, raw, cache_key in pending:
            cached = _cache_get(
                NEWS_TRANSLATION_CACHE,
                cache_key,
                NEWS_TRANSLATION_CACHE_TTL,
            )
            if cached:
                result[idx] = str(cached)
            else:
                still_pending.append((idx, raw, cache_key))

        if not still_pending:
            return result

        # URL 길이와 배치 파싱 위험을 줄이기 위해 소량씩 처리한다.
        for batch_start in range(0, len(still_pending), NEWS_TRANSLATION_BATCH_SIZE):
            batch = still_pending[
                batch_start:batch_start + NEWS_TRANSLATION_BATCH_SIZE
            ]

            lines = [
                f"{pos}\t{raw}"
                for pos, (_, raw, _) in enumerate(batch, start=1)
            ]
            batch_text = "\n".join(lines)

            last_error = None
            translated_ok = False

            for attempt in range(2):
                try:
                    now = time.time()
                    wait_sec = NEWS_TRANSLATION_MIN_INTERVAL - (
                        now - NEWS_TRANSLATION_LAST_REQUEST_TS
                    )
                    if wait_sec > 0:
                        time.sleep(wait_sec)

                    # GET 대신 POST body로 q를 전달해 URL 길이 제한을 피한다.
                    response = HTTP_SESSION.post(
                        "https://translate.googleapis.com/translate_a/single",
                        params=[
                            ("client", "gtx"),
                            ("sl", "en"),
                            ("tl", "ko"),
                            ("dt", "t"),
                        ],
                        data={"q": batch_text},
                        timeout=8.0,
                        headers={
                            "User-Agent": "Mozilla/5.0",
                            "Content-Type": "application/x-www-form-urlencoded",
                        },
                    )
                    NEWS_TRANSLATION_LAST_REQUEST_TS = time.time()

                    if response.status_code == 429:
                        last_error = "HTTP 429"
                        # 429가 반복되면 사이트를 계속 붙잡지 않고
                        # 일정 시간 번역 요청을 쉬게 한다.
                        if attempt == 0:
                            time.sleep(2.0)
                        else:
                            NEWS_TRANSLATION_BACKOFF_UNTIL = time.time() + 30.0
                        continue

                    if response.status_code != 200:
                        last_error = f"HTTP {response.status_code}"
                        continue

                    payload = response.json()
                    translated_text = "".join(
                        str(part[0])
                        for part in (payload[0] or [])
                        if isinstance(part, list) and part and part[0]
                    ).strip()

                    if not translated_text:
                        last_error = "empty translation"
                        continue

                    # 번역기가 탭/번호/줄바꿈을 조금 바꿔도 최대한 복구한다.
                    parsed = {}
                    for line in translated_text.splitlines():
                        match = re.match(
                            r"^\s*(\d+)\s*(?:\t|\|\s*\|\s*\|)\s*(.+?)\s*$",
                            line,
                        )
                        if not match:
                            continue

                        pos = int(match.group(1))
                        value = match.group(2).strip()

                        # 한글이 없는 결과도 버리지 않는다.
                        # 예: FOMC/NVDA/Apple 같은 고유명사는 번역 결과가
                        # 영어로 남을 수 있지만, 해당 뉴스는 정상 결과일 수 있다.
                        if value:
                            parsed[pos] = value

                    # 부분 성공을 그대로 적용한다. 한 건 실패해도 다른 건 살린다.
                    if parsed:
                        for pos, (idx, raw, cache_key) in enumerate(
                            batch,
                            start=1,
                        ):
                            translated = str(parsed.get(pos) or "").strip()

                            # 결과가 원문과 완전히 같으면 번역 실패로 간주하고
                            # 캐시하지 않는다.
                            if translated and translated != raw:
                                result[idx] = _cache_set(
                                    NEWS_TRANSLATION_CACHE,
                                    cache_key,
                                    translated,
                                )
                            else:
                                result.setdefault(idx, raw)

                        translated_ok = True
                        break

                    last_error = "batch parse failed"

                except Exception as exc:
                    last_error = f"{type(exc).__name__}: {exc}"

            if not translated_ok:
                if last_error:
                    print(
                        f"[NEWS TRANSLATION BATCH] failed: "
                        f"{last_error}"
                    )
                for idx, raw, _ in batch:
                    result.setdefault(idx, raw)

    return result


def translate_news_titles_to_korean(items, market_name=""):
    """미국주식/미국증시/해외주식 뉴스 제목을 한국어로 번역한다."""
    if not _translation_market_enabled(market_name) or not items:
        return items or []

    news_items = [dict(x) for x in items if isinstance(x, dict)]
    if not news_items:
        return []

    titles = [str(item.get("title") or "").strip() for item in news_items]
    translated_map = _translate_news_titles_batch_to_korean(titles)

    for idx, item in enumerate(news_items):
        original = str(item.get("title") or "").strip()
        translated = str(translated_map.get(idx) or original).strip()

        if translated and translated != original:
            item["title"] = translated
            item["title_original"] = (
                str(item.get("title_original") or "")
                or original
            )

    return news_items


def fetch_finlight_stock_news(symbol="", market_name="", formal_name="", limit=20):
    """선택 종목의 직접 관련 금융뉴스. Finlight 우선, 기존 RSS를 fallback으로 사용한다."""
    if market_name == "국내주식":
        language = "ko"
        countries = ["KR"]
        query = f"ticker:{symbol}" if symbol else formal_name
        categories = ["markets", "economy", "business", "technology", "regulation", "geopolitics"]
    elif market_name == "미국주식":
        language = "en"
        countries = ["US"]
        query = f"ticker:{symbol}" if symbol else formal_name
        categories = ["markets", "economy", "business", "technology", "regulation", "geopolitics"]
    elif market_name == "암호화폐":
        language = "en"
        countries = None
        query = formal_name or symbol
        categories = ["crypto", "markets", "economy", "regulation", "technology"]
    else:
        language = "en"
        countries = None
        query = formal_name or symbol
        categories = ["markets", "economy", "business", "technology", "regulation", "geopolitics", "energy", "commodities"]

    result = fetch_finlight_news(
        query=query,
        symbol=symbol,
        market_name=market_name,
        formal_name=formal_name,
        limit=limit,
        language=language,
        countries=countries,
        categories=categories,
        cache_tag="stock",
    )
    if result:
        return result

    # Finlight 장애/일시적 빈 결과에도 기존 뉴스 기능은 유지한다.
    return fetch_rss_news(formal_name or symbol, limit)


def _economic_news_priority(item, market_name=""):
    """경제뉴스 안에서 미국 증시 마감 시황처럼 시장 방향 판단에 직접 쓰이는 기사를 우선한다."""
    title = str(item.get("title") or "").lower()
    score = int(item.get("relevance_score") or 0)

    # 국내 종목의 "왜 움직였을까"에서 전일 미국 증시 흐름을 첫 번째 재료로
    # 사용할 수 있도록 미국 증시 마감/지수/반도체 시황에 추가 가중치를 준다.
    us_close_terms = [
        "미국증시", "뉴욕증시", "뉴욕 증시", "미국 증시", "나스닥",
        "s&p500", "s&p 500", "다우", "필라델피아반도체", "반도체지수",
        "뉴욕증시 마감", "미국증시 마감", "마감 시황", "증시 마감",
        "wall street", "nasdaq", "s&p 500", "dow", "phlx semiconductor",
    ]
    market_move_terms = [
        "상승", "하락", "급등", "급락", "혼조", "마감", "지수",
        "금리", "국채금리", "유가", "cpi", "ppi", "연준", "fomc",
    ]

    if any(term in title for term in us_close_terms):
        score += 6
    if any(term in title for term in market_move_terms):
        score += 2
    if "필라델피아반도체" in title or "반도체지수" in title or "phlx semiconductor" in title:
        score += 3
    if "마감" in title and any(term in title for term in ["미국", "뉴욕", "나스닥", "다우"]):
        score += 3

    # 미국 증시의 '왜 움직였는가'를 설명하는 원인 기사에 추가 가중치.
    # 지정학 → 원유 → 물가 → 금리의 연결고리가 다음 단계 분석의 핵심 원재료다.
    driver_terms = [
        "이란", "이란전쟁", "중동", "호르무즈", "전쟁 장기화",
        "유가", "원유", "wti", "브렌트", "국채금리", "미국채금리",
        "인플레이션", "연준", "fomc", "금리인상", "공급차질",
        "iran", "iran war", "middle east", "strait of hormuz", "oil",
        "crude", "wti", "brent", "treasury yields", "inflation",
        "federal reserve", "fomc", "rate hike", "supply disruption",
    ]
    driver_hits = sum(1 for term in driver_terms if term in title)
    if driver_hits:
        score += min(10, 4 + driver_hits * 2)
    if any(term in title for term in ["유가", "원유", "wti", "브렌트", "oil", "crude"]):
        score += 2
    if any(term in title for term in ["국채금리", "미국채금리", "treasury yields"]):
        score += 2
    return score


def fetch_finlight_economic_news(market_name="", limit=10):
    """경제/시장/정책/산업 뉴스 + 미국 증시 마감 시황을 별도로 확보한다."""
    if market_name == "국내주식" or market_name == "국내증시":
        language = "ko"
        query = "연준 OR 금리 OR 인플레이션 OR 관세 OR 반도체 OR AI OR 원유 OR 환율"
        countries = None
    elif market_name == "암호화폐":
        language = "en"
        query = "Federal Reserve OR interest rates OR inflation OR crypto OR Bitcoin OR tariffs OR stock market"
        countries = None
    else:
        language = "en"
        query = "Federal Reserve OR interest rates OR inflation OR tariffs OR semiconductor OR AI OR oil OR stock market"
        # 경제뉴스는 미국에만 가두지 않는다. 한국/중국/유럽 등 글로벌 정책·산업 뉴스도 필요하다.
        countries = None

    categories = ["markets", "economy", "regulation", "geopolitics", "technology", "energy", "commodities", "business"]

    # 1) 미국 증시 마감 시황을 별도 수집한다.
    # 국내주식 화면에서는 한국어 기사로 받아 사용자가 바로 읽을 수 있게 한다.
    us_close_limit = max(6, min(10, int(limit)))
    us_close = fetch_finlight_news(
        query=(
            "미국증시 OR 뉴욕증시 OR 나스닥 OR 다우 OR S&P500 "
            "OR 필라델피아반도체 OR 미국증시 마감 OR 뉴욕증시 마감"
        ) if language == "ko" else (
            "US stock market OR Wall Street OR Nasdaq OR Dow OR S&P 500 "
            "OR Philadelphia Semiconductor OR market close"
        ),
        market_name=market_name,
        limit=us_close_limit,
        language=language,
        countries=(None if language == "ko" else ["US"]),
        categories=["markets", "economy", "technology"],
        cache_tag="economic_us_close",
    )

    # 2) 미국 증시를 움직인 '원인'도 별도 수집한다.
    # 예: 이란/중동 전쟁 → 유가 → 인플레이션 → 국채금리 → Fed → 주가
    driver_limit = max(5, min(10, int(limit) // 2 if int(limit) > 1 else 5))
    us_driver = fetch_finlight_news(
        query=(
            "이란 OR 이란전쟁 OR 중동 OR 호르무즈 OR 유가 OR 원유 OR WTI "
            "OR 브렌트 OR 국채금리 OR 미국채금리 OR 인플레이션 OR 연준 "
            "OR FOMC OR 금리인상 OR 공급차질"
        ) if language == "ko" else (
            "Iran OR Iran war OR Middle East OR Strait of Hormuz OR oil OR crude "
            "OR WTI OR Brent OR Treasury yields OR inflation OR Federal Reserve "
            "OR FOMC OR rate hike OR supply disruption"
        ),
        market_name=market_name,
        limit=driver_limit,
        language=language,
        countries=(None if language == "ko" else ["US"]),
        categories=["geopolitics", "commodities", "economy", "markets", "energy", "regulation"],
        cache_tag="economic_us_driver",
    )

    # 3) 기존 경제/정책/산업 뉴스도 유지한다.
    general_limit = max(10, int(limit))
    general = fetch_finlight_news(
        query=query,
        market_name=market_name,
        limit=general_limit,
        language=language,
        countries=countries,
        categories=categories,
        cache_tag="economic",
    )

    merged = []
    seen = set()
    for item in (us_close or []) + (us_driver or []) + (general or []):
        if not isinstance(item, dict):
            continue
        title_key = re.sub(r"\s+", " ", str(item.get("title") or "")).strip().lower()
        if not title_key or title_key in seen:
            continue
        seen.add(title_key)
        item = dict(item)
        item["relevance_score"] = _economic_news_priority(item, market_name)
        merged.append(item)

    merged.sort(
        key=lambda x: (int(x.get("relevance_score") or 0), x.get("pubDate") or ""),
        reverse=True,
    )
    pre_limit_count = len(merged)
    final_limit = max(1, int(limit))
    merged = merged[:final_limit]

    print(
        f"[FINLIGHT ECONOMIC SUMMARY] market={market_name!r} "
        f"us_close={len(us_close or [])} "
        f"us_driver={len(us_driver or [])} "
        f"general={len(general or [])} "
        f"deduped={pre_limit_count} final={len(merged)} limit={final_limit}"
    )

    if merged:
        return merged

    return fetch_rss_news("뉴욕증시 OR 미국증시 OR 나스닥", limit)


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

