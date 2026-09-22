"""Standard news data adapter for gaemiGTP's existing news collectors.

This module does not replace the existing collectors. It converts their
existing rows into a stable, reusable shape for the next frontend/data layer.
"""
from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone

from news_analysis import NEWS_GENERIC_TOKENS, NEWS_POLARITY, _news_polarity

# High-value material terms already used by the site's news logic.
MATERIAL_TERMS = {
    "실적", "수주", "계약", "투자", "증설", "감산", "공급", "규제", "관세",
    "제재", "승인", "소송", "특허", "배당", "인수", "합병", "신제품", "신규 고객",
    "점유율", "공장", "라인 증설", "납품", "마진", "전망 상향", "전망 하향",
    "hbm", "hbm4", "gddr", "gpu", "데이터센터", "ai", "인공지능", "반도체",
    "메모리", "dram", "nand", "파운드리", "전기차", "배터리", "금리", "인플레이션",
    "환율", "유가", "연준", "fomc",
}

# Content that should not become a primary material label.
NOISE_TERMS = {
    "인터뷰", "화보", "칼럼", "리뷰", "가이드", "주식 초보", "투자 팁",
    "맛집", "여행", "연예", "채용", "부고", "추천주", "종목추천", "투자분석",
}


def _stable_id(symbol: str, item: dict) -> str:
    raw = "|".join([
        str(symbol or "").strip().upper(),
        str(item.get("title") or "").strip(),
        str(item.get("pubDate") or item.get("published_at") or "").strip(),
        str(item.get("link") or item.get("original_url") or "").strip(),
    ])
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:20]


def _clean_source(item: dict) -> str:
    return str(item.get("media") or item.get("source") or "출처").strip() or "출처"


def _keywords(title: str, description: str) -> list[str]:
    text = f"{title} {description}".lower()
    found = []
    # Prefer multi-word terms first, then single tokens.
    candidates = sorted(MATERIAL_TERMS | NEWS_GENERIC_TOKENS, key=lambda x: (-len(x), x))
    for term in candidates:
        if term.lower() in text and term.lower() not in {x.lower() for x in found}:
            found.append(term)
        if len(found) >= 6:
            break
    return [f"#{x}" for x in found[:5]]


def _sentiment(item: dict) -> tuple[str, int]:
    score = _news_polarity(item)
    if score > 0:
        return "호재", score
    if score < 0:
        return "악재", score
    # Market-event language is not forced into positive/negative sentiment.
    text = f"{item.get('title', '')} {item.get('description', '')}".lower()
    event_terms = ("vi", "거래대금", "거래량", "급등", "급락", "52주", "상한가", "하한가")
    if any(x in text for x in event_terms):
        return "정보", 0
    return "중립", 0


def _issues(title: str, description: str) -> list[str]:
    text = f"{title} {description}".lower()
    terms = []
    for term in sorted(MATERIAL_TERMS, key=lambda x: (-len(x), x)):
        if term.lower() in text and term.lower() not in {x.lower() for x in terms}:
            terms.append(term)
        if len(terms) >= 3:
            break
    return [f"#{x}" for x in terms]


def normalize_news(item: dict, *, symbol: str = "", market: str = "", stock_name: str = "") -> dict:
    """Convert one existing collector row into the gaemiGTP standard news shape."""
    title = str(item.get("title") or "").strip()
    description = str(item.get("description") or item.get("summary") or "").strip()
    original_url = str(item.get("originallink") or item.get("link") or "").strip()
    published_at = str(item.get("pubDate") or item.get("published_at") or "").strip()
    sentiment, sentiment_score = _sentiment(item)
    keywords = _keywords(title, description)
    issues = _issues(title, description)

    return {
        "id": _stable_id(symbol, item),
        "symbol": str(symbol or "").strip().upper(),
        "market": str(market or "").strip(),
        "stock_name": str(stock_name or "").strip(),
        "title": title,
        "source": _clean_source(item),
        "published_at": published_at,
        "original_url": original_url or "#",
        "description": description,
        "sentiment": sentiment,
        "sentiment_score": sentiment_score,
        "keywords": keywords,
        # Do not invent related companies yet. The selected stock is the only
        # confirmed relation at this stage.
        "related_stocks": ([str(symbol).strip().upper()] if symbol else []),
        "related_issues": issues,
    }


def normalize_news_list(items: list[dict] | None, *, symbol: str = "", market: str = "", stock_name: str = "") -> list[dict]:
    result = []
    seen = set()
    for item in items or []:
        if not isinstance(item, dict):
            continue
        row = normalize_news(item, symbol=symbol, market=market, stock_name=stock_name)
        if not row["title"] or row["id"] in seen:
            continue
        seen.add(row["id"])
        result.append(row)
    return result
