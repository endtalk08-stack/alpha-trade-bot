import re


def merge_news_sources(*sources):
    """뉴스 중복을 줄이고 실제 출처가 있는 뉴스만 합친다."""
    seen = set()
    merged = []
    for source in sources:
        for item in (source or []):
            if not isinstance(item, dict):
                continue
            title = str(item.get("title", "")).strip()
            key = re.sub(r"\s+", " ", title).lower()
            if not key or key in seen:
                continue
            seen.add(key)
            merged.append(item)
    return merged
