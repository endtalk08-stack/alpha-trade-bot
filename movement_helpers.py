"""Pure helpers used by the movement-analysis engine."""

from numeric_utils import _safe_float

def _classify_direction(value, positive=0.3, strong=1.0):
    v = _safe_float(value)
    if v is None:
        return 0
    if v >= strong:
        return 2
    if v >= positive:
        return 1
    if v <= -strong:
        return -2
    if v <= -positive:
        return -1
    return 0

def _extract_news_text(news):
    out = []
    for item in (news or []):
        if isinstance(item, dict):
            out.append(str(item.get("title", "")))
            out.append(str(item.get("description", "")))
            out.append(str(item.get("summary", "")))
        else:
            out.append(str(item))
    return " ".join(out)

def _detect_macro_signals(stock_news, global_news=None):
    """뉴스가 '발표/결과'인지 단순 언급인지 구분하기 위한 최소한의 신호 추출."""
    blob = (_extract_news_text(stock_news) + " " + _extract_news_text(global_news)).lower()

    groups = {
        "CPI": ["cpi", "소비자물가", "소비자 물가"],
        "PPI": ["ppi", "생산자물가", "생산자 물가"],
        "고용": ["고용", "실업률", "비농업", "nfp", "payroll"],
        "FOMC": ["fomc", "fed", "연준", "기준금리"],
        "미국채": ["10년물", "10년 국채", "2년물", "미국채"],
        "유가": ["wti", "brent", "유가", "원유"],
        "중국": ["중국", "상하이", "홍콩", "위안화"],
        "관세·정책": ["관세", "수출규제", "수출 규제", "제재", "정책"],
        "지정학": ["전쟁", "휴전", "분쟁", "중동", "지정학"],
    }
    event_words = ["발표", "발표됐다", "발표했다", "예상", "전망", "결과", "전월", "전년", "전망치", "시장 예상"]
    signals = []
    for name, kws in groups.items():
        hits = [k for k in kws if k in blob]
        if hits:
            is_event = any(w in blob for w in event_words)
            signals.append({"name": name, "hits": hits[:4], "is_event": is_event})
    return signals

def _build_theme_breadth(formal_name, stock_news):
    """
    현재 코드가 보유한 뉴스/테마 데이터만으로 과장하지 않도록
    '테마 동반 상승'은 실제 관련 종목 수치가 없으면 주장하지 않는다.
    """
    theme, hits = _detect_theme(formal_name, stock_news)
    if not theme:
        return {"theme": None, "hits": 0, "score": 0, "detail": "테마 근거 없음", "verified_breadth": False}

    if theme in ("반도체", "AI"):
        # SOXX가 별도 제공되면 analyze_global_impact에서 보강한다.
        return {"theme": theme, "hits": hits, "score": 1 if hits >= 2 else 0,
                "detail": f"{theme} 관련 키워드 {hits}건 감지", "verified_breadth": False}

    return {"theme": theme, "hits": hits, "score": 1 if hits >= 2 else 0,
            "detail": f"{theme} 관련 키워드 {hits}건 감지", "verified_breadth": False}

def _find_earnings_event(formal_name, symbol, market_calendar):
    """현재 종목의 실적 일정에서 실제 actual/estimate가 함께 있는 이벤트를 찾는다."""
    target={str(symbol).upper(),str(formal_name).upper()}
    candidates=[]
    for ev in (market_calendar or []):
        if ev.get("type")!="earn": continue
        vals={str(ev.get("ticker","")).upper(),str(ev.get("name","")).upper()}
        if target & vals or str(formal_name).upper() in str(ev.get("name","")).upper():
            candidates.append(ev)
    return candidates[0] if candidates else None

def _verified_macro_event_snapshot(market_calendar):
    """경제 캘린더의 actual/forecast/previous를 숫자로 비교. 문자열 언급만으로 이벤트를 확정하지 않는다."""
    out=[]
    for ev in (market_calendar or []):
        if ev.get("type")!="eco": continue
        actual=_safe_float(ev.get("val")); forecast=_safe_float(ev.get("exp")); previous=_safe_float(ev.get("prev"))
        if actual is not None and forecast is not None:
            surprise=(actual-forecast)/abs(forecast)*100 if forecast!=0 else None
            out.append({"name":ev.get("name","경제지표"),"actual":actual,"expected":forecast,"previous":previous,
                        "surprise_pct":surprise,"date":ev.get("date_key"),"time":ev.get("time")})
    return out

def _movement_rate_to_float(value):
    try:
        if value is None:
            return None
        s = str(value).replace("%", "").replace(",", "").strip()
        if s in ("", "-", "--"):
            return None
        return float(s)
    except Exception:
        return None

def _find_world_item(world_board, names):
    if not world_board:
        return None
    name_set = {str(x).upper() for x in names}
    for item in world_board:
        if str(item.get("name", "")).upper() in name_set:
            return item
    return None

def _news_sentiment_score(news_list):
    positive = 0
    negative = 0
    evidence = []
    if not news_list:
        return positive, negative, evidence

    for news in news_list:
        title = str(news.get("title", "")).lower()
        pos_hits = [k for k in POSITIVE_NEWS_KEYWORDS if k.lower() in title]
        neg_hits = [k for k in NEGATIVE_NEWS_KEYWORDS if k.lower() in title]

        if pos_hits:
            positive += min(2, len(pos_hits))
            evidence.append({
                "title": str(news.get("title", "")),
                "type": "positive",
                "keywords": pos_hits[:3]
            })
        if neg_hits:
            negative += min(2, len(neg_hits))
            evidence.append({
                "title": str(news.get("title", "")),
                "type": "negative",
                "keywords": neg_hits[:3]
            })

    return positive, negative, evidence

def _detect_theme(formal_name, stock_news):
    text_parts = [str(formal_name)]
    for news in (stock_news or []):
        text_parts.append(str(news.get("title", "")))
    text = " ".join(text_parts).lower()

    theme_scores = {}
    for theme, keywords in MOVEMENT_THEME_KEYWORDS.items():
        score = sum(1 for keyword in keywords if keyword.lower() in text)
        if score > 0:
            theme_scores[theme] = score

    if not theme_scores:
        return None, 0
    theme = max(theme_scores, key=theme_scores.get)
    return theme, theme_scores[theme]

MOVEMENT_THEME_KEYWORDS = {
    "반도체": ["삼성전자", "sk하이닉스", "하이닉스", "마이크론", "엔비디아", "nvidia", "amd", "asml", "tsmc", "반도체", "hbm", "dram", "nand", "ai"],
    "AI": ["ai", "인공지능", "엔비디아", "nvidia", "데이터센터", "gpu", "hbm", "클라우드"],
    "로봇": ["로봇", "robot", "robotics", "휴머노이드", "자동화", "협동로봇"],
    "바이오": ["바이오", "제약", "신약", "임상", "hlb", "셀트리온", "리가켐"],
    "2차전지": ["배터리", "2차전지", "이차전지", "전기차", "리튬", "양극재", "음극재", "에코프로", "lg에너지"],
    "원전": ["원전", "원자력", "두산에너빌리티", "smr", "원자로", "nuclear"],
    "방산": ["방산", "방위산업", "미사일", "전투기", "한화에어로", "lignex1", "l3harris"],
    "우주": ["우주", "위성", "스페이스x", "spacex", "로켓", "satellite"]
}

NEGATIVE_NEWS_KEYWORDS = [
    "적자", "감소", "하락", "급락", "손실", "부진", "하향", "목표가 하향", "유상증자",
    "전환사채", "소송", "리콜", "취소", "철회", "지연", "악재", "우려", "규제", "횡령", "배임"
]

POSITIVE_NEWS_KEYWORDS = [
    "수주", "계약", "공급", "실적 개선", "어닝 서프라이즈", "호실적", "흑자전환",
    "증가", "확대", "돌파", "신제품", "개발", "투자", "증설", "승인", "특허",
    "협력", "파트너십", "수혜", "목표가 상향", "상향", "강세", "급등", "사상 최대", "최대 실적"
]
