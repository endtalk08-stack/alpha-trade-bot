"""DART disclosure widget data."""

from performance import HTTP_SESSION

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
