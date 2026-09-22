import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

NEWS_CLUSTER_STOPWORDS = {
    "sk하이닉스","하이닉스","삼성전자","엔비디아","nvidia","nvda","tesla","tsla",
    "오늘","최근","관련","주가","주식","종목","시장","증시","상승","하락","급등","급락",
    "전망","분석","소식","뉴스","관련주","주목","관심","이유","영향","장중","마감",
    "the","and","for","with","from","stock","shares","market","today","latest","news"
}

NEWS_GENERIC_TOKENS = {
    "실적","ai","인공지능","gpu","hbm","hbm4","데이터센터","수요","매출","영업이익","순이익",
    "공급","공급망","수주","계약","증설","관세","금리","인플레이션","물가","연준","fomc",
    "금리인하","금리인상","고용","경기","cpi","pce","반도체","메모리","dram","nand","aws",
    "cloud","클라우드","데이터센터","invest","investment","earnings","revenue","profit","guidance"
}

NEWS_POLARITY = {
    "positive": ["호조","개선","증가","성장","상회","확대","수요 증가","강세","수주","계약","돌파","흑자","record","beat","beats","growth","strong demand","upgrade","surge","expansion"],
    "negative": ["부진","감소","하회","둔화","악화","축소","약세","우려","관세 부담","금리 부담","공급 차질","적자","downgrade","miss","decline","weak demand","cut","risk","concern"]
}

def _news_tokens(text):
    text = re.sub(r"[^0-9A-Za-z가-힣]+", " ", str(text or "").lower())
    raw = [x for x in text.split() if len(x) >= 2]
    return [x for x in raw if x not in NEWS_CLUSTER_STOPWORDS]

def _news_cluster_key(news):
    title = str(news.get("title", ""))
    desc = str(news.get("description", news.get("summary", "")))
    tokens = _news_tokens(title + " " + desc)
    # 핵심 후보 단어만 남겨 동일 재료를 안정적으로 묶는다.
    core = [x for x in tokens if x in NEWS_GENERIC_TOKENS]
    return set(core)

def _news_polarity(news):
    text = (str(news.get("title", "")) + " " + str(news.get("description", news.get("summary", "")))).lower()
    pos = sum(1 for k in NEWS_POLARITY["positive"] if k.lower() in text)
    neg = sum(1 for k in NEWS_POLARITY["negative"] if k.lower() in text)
    if pos > neg: return 1
    if neg > pos: return -1
    return 0

def analyze_news_clusters(news_list, target_name="", symbol=""):
    """여러 출처의 뉴스를 공통 주제로 묶고 출처 다양성/반복성을 계산한다."""
    items = [x for x in (news_list or []) if isinstance(x, dict) and str(x.get("title", "")).strip()]
    clusters = []
    for item in items:
        keys = _news_cluster_key(item)
        if not keys:
            continue
        best = None
        best_overlap = 0
        for c in clusters:
            overlap = len(keys & c["keywords"])
            union = len(keys | c["keywords"]) or 1
            similarity = overlap / union
            if overlap >= 1 and similarity >= 0.25 and overlap > best_overlap:
                best, best_overlap = c, overlap
        if best is None:
            best = {"keywords": set(keys), "items": []}
            clusters.append(best)
        else:
            best["keywords"].update(keys)
        best["items"].append(item)

    scored = []
    for c in clusters:
        items_c = c["items"]
        sources = {str(x.get("media") or x.get("source") or "").strip().lower() for x in items_c}
        sources.discard("")
        polarity = sum(_news_polarity(x) for x in items_c)
        pos = sum(1 for x in items_c if _news_polarity(x) > 0)
        neg = sum(1 for x in items_c if _news_polarity(x) < 0)
        source_count = len(sources)
        repeat_count = len(items_c)
        # 출처 수 + 반복 횟수 + 방향 일치 + 핵심주제 단어 수
        score = repeat_count * 1.0 + source_count * 2.0 + min(3, len(c["keywords"])) * 0.7
        if polarity > 0 and pos >= max(2, neg + 1): score += 2.0
        elif polarity < 0 and neg >= max(2, pos + 1): score += 2.0
        scored.append({
            "keywords": sorted(c["keywords"], key=lambda x: (-len(x), x))[:5],
            "items": items_c,
            "repeat_count": repeat_count,
            "source_count": source_count,
            "polarity": 1 if polarity > 0 else -1 if polarity < 0 else 0,
            "positive_count": pos,
            "negative_count": neg,
            "score": round(score, 2),
        })
    scored.sort(key=lambda x: x["score"], reverse=True)
    return scored

def build_clustered_material_evidence(news_list, target_name="", symbol=""):
    clusters = analyze_news_clusters(news_list, target_name, symbol)
    if not clusters:
        return {"score": 0, "detail": "반복적으로 확인되는 핵심 뉴스 재료 없음", "evidence": [], "clusters": []}
    top = clusters[0]
    # 출처가 하나뿐이고 기사도 1개면 강한 재료로 승격하지 않는다.
    reliable = top["repeat_count"] >= 2 and top["source_count"] >= 2
    direction = top["polarity"]
    if reliable and direction > 0: score = 2
    elif reliable and direction < 0: score = -2
    elif top["repeat_count"] >= 2 and direction != 0: score = direction
    else: score = 0
    topic = "·".join(top["keywords"][:3]) or "관련 재료"
    detail = f"{topic} 관련 내용이 {top['repeat_count']}건"
    if direction > 0: detail += " · 긍정 방향"
    elif direction < 0: detail += " · 부정 방향"
    return {"score": score, "detail": detail, "evidence": top["items"][:5], "clusters": clusters[:5]}

# ---------------------------------------------------------------------------
# 핵심뉴스 선별 (1차: 제목/출처 기반, AI/본문 미사용)
# ---------------------------------------------------------------------------
NEWS_NOISE_SOURCE_PATTERNS = (
    "네이버 블로그", "blog.naver", "티스토리", "tistory", "브런치",
    "주달", "씽크풀", "thinkpool",
)
NEWS_NOISE_TITLE_PATTERNS = (
    r"투자분석", r"투자 분석", r"주식\s*초보", r"매수할", r"매도할",
    r"올릴\s*종목", r"추천", r"추천주", r"유망주", r"대응전략",
    r"대응 전략", r"투자전략", r"투자 전략", r"전망 및 대응",
    r"밀릴때마다", r"물량\s*모아", r"지금\s*사야", r"지금\s*팔아",
    r"여의도\s*클래스", r"종목\s*분석", r"종목분석", r"주가\s*전망",
)
NEWS_MARKET_EVENT_PATTERNS = (
    r"\bVI\b", r"VI\s*발동", r"상한가", r"하한가",
    r"거래대금.{0,12}(돌파|급증|증가)", r"거래량.{0,12}(급증|폭증|돌파)",
    r"(급등|급락).{0,8}\d+(?:\.\d+)?\s*%", r"\d+(?:\.\d+)?\s*%.*(급등|급락)",
    r"52주.{0,8}(신고가|신저가)", r"신고가", r"신저가",
)
NEWS_MATERIAL_PATTERNS = (
    r"계약", r"수주", r"공급", r"납품", r"증설", r"공장",
    r"투자", r"합작", r"파트너", r"신제품", r"신규\s*(고객|사업|수주)",
    r"실적", r"매출", r"영업이익", r"순이익", r"배당",
    r"인수", r"합병", r"M&A", r"승인", r"허가", r"특허", r"임상",
    r"FDA", r"정책", r"규제", r"관세", r"공급\s*부족",
    r"수요\s*(증가|확대|감소|둔화)", r"가격\s*(상승|하락|인상|인하)",
    r"조회공시", r"공시요구",
)

def _news_matches_any(text, patterns):
    return any(re.search(p, str(text or ""), flags=re.IGNORECASE) for p in patterns)

def _news_recency_bonus(news):
    raw = str((news or {}).get("pubDate") or (news or {}).get("publishedAt") or "").strip()
    if not raw:
        return 0
    dt = None
    try:
        dt = parsedate_to_datetime(raw)
    except Exception:
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except Exception:
            pass
    if dt is None:
        return 0
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    age_hours = max(0.0, (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds() / 3600.0)
    if age_hours <= 6: return 22
    if age_hours <= 24: return 18
    if age_hours <= 72: return 13
    if age_hours <= 7 * 24: return 8
    if age_hours <= 30 * 24: return 3
    return 0

def classify_key_news_item(news):
    item = dict(news or {})
    title = str(item.get("title") or "").strip()
    source = str(item.get("media") or item.get("source") or "").strip()
    if not title:
        return None
    if any(p.lower() in source.lower() for p in NEWS_NOISE_SOURCE_PATTERNS):
        return {"include": False, "type": "noise", "label": "제외", "score": -100}
    if _news_matches_any(title, NEWS_NOISE_TITLE_PATTERNS):
        return {"include": False, "type": "noise", "label": "제외", "score": -100}

    is_market = _news_matches_any(title, NEWS_MARKET_EVENT_PATTERNS)
    is_disclosure = bool(re.search(r"조회공시|공시요구|공시답변", title, re.I))
    is_material = _news_matches_any(title, NEWS_MATERIAL_PATTERNS)

    if is_market:
        event_type, label, base = "market_event", "시장", 46
    elif is_disclosure:
        event_type, label, base = "disclosure", "공시", 40
    elif is_material:
        event_type, label, base = "material", "재료", 48
    else:
        event_type, label, base = "other", "중립", 8

    polarity = _news_polarity(item)
    if event_type in {"market_event", "disclosure"}:
        sentiment, sentiment_label = "neutral", label
    elif polarity > 0:
        sentiment, sentiment_label = "positive", "호재"
        base += 8
    elif polarity < 0:
        sentiment, sentiment_label = "negative", "악재"
        base += 8
    else:
        sentiment, sentiment_label = "neutral", "중립"

    if re.search(r"속보|단독", title, re.I):
        base += 5
    if re.search(r"\d+(?:\.\d+)?\s*(?:억|조|만|%|원)", title, re.I):
        base += 3
    base += _news_recency_bonus(item)

    item.update({
        "event_type": event_type,
        "sentiment": sentiment,
        "sentiment_label": sentiment_label,
        "importance_score": base,
    })
    return {"include": event_type != "other", "type": event_type, "label": sentiment_label, "score": base, "item": item}

def select_key_news(news_list, limit=2):
    """현재 10개 종목뉴스에서 주요뉴스 최대 2개를 선별한다."""
    candidates, seen = [], set()
    for news in news_list or []:
        if not isinstance(news, dict):
            continue
        title = re.sub(r"\s+", " ", str(news.get("title") or "").strip())
        key = title.lower()
        if not key or key in seen:
            continue
        seen.add(key)
        result = classify_key_news_item(news)
        if result and result.get("include"):
            candidates.append(result["item"])

    candidates.sort(key=lambda x: int(x.get("importance_score", 0)), reverse=True)
    if not candidates:
        return []

    def token_set(title):
        return set(_news_tokens(title))

    selected = [candidates[0]]
    # 2번째는 다른 이슈/유형을 우선하되, 없으면 점수순으로 채운다.
    for item in candidates[1:]:
        if len(selected) >= min(2, max(1, int(limit))):
            break
        a, b = token_set(selected[0].get("title")), token_set(item.get("title"))
        similarity = len(a & b) / max(1, len(a | b))
        if similarity >= 0.65:
            continue
        selected.append(item)
    if len(selected) < min(2, max(1, int(limit))):
        for item in candidates:
            if item not in selected:
                selected.append(item)
                if len(selected) >= 2:
                    break
    return selected[:2]
