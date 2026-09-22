import re

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
