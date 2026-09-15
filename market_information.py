MARKET_INFO_GROUPS = {
    "fomc": ["fomc", "연준", "연방준비", "파월", "금리인하", "금리 인하", "금리인상", "금리 인상"],
    "inflation": ["cpi", "pce", "물가", "인플레이션", "소비자물가"],
    "jobs": ["고용", "실업률", "비농업", "고용보고서", "jobs", "payroll"],
    "rates": ["국채금리", "채권금리", "10년물", "2년물", "금리"],
    "tariff": ["관세", "tariff", "무역"],
    "market": ["나스닥", "s&p", "s&p500", "다우", "미국증시", "뉴욕증시", "증시"],
}

MARKET_INFO_STOP = set("미국 뉴욕 시장 증시 주가 주식 관련 소식 전해 전망 투자 오늘 어제 최근 상승 하락 영향 기대".split())

from news_analysis import _news_polarity

def _market_info_topic(text):
    low=str(text or "").lower()
    hits=[]
    for topic, words in MARKET_INFO_GROUPS.items():
        if any(w.lower() in low for w in words):
            hits.append(topic)
    return hits

def build_market_information(global_news):
    """시장 뉴스는 종목 원인으로 확정하지 않고, 여러 출처에서 반복되는 시장 이슈를 정보로만 요약한다."""
    items=[x for x in (global_news or []) if isinstance(x,dict) and str(x.get('title','')).strip()]
    if not items:
        return {"topic":"", "text":"", "repeat_count":0, "source_count":0, "evidence":[]}
    groups=[]
    for item in items:
        text=str(item.get('title',''))+' '+str(item.get('description',item.get('summary','')))
        topics=_market_info_topic(text)
        for topic in topics:
            g=next((x for x in groups if x['topic']==topic),None)
            if g is None:
                g={'topic':topic,'items':[]}
                groups.append(g)
            g['items'].append(item)
    if not groups:
        # 일반 시장 뉴스도 있으면 가장 많은 출처의 제목을 근거로만 보존
        return {"topic":"", "text":"", "repeat_count":0, "source_count":0, "evidence":[]}
    groups.sort(key=lambda g:(len(g['items']), len({str(i.get('media') or i.get('source') or '').lower() for i in g['items']})), reverse=True)
    g=groups[0]
    sources={str(i.get('media') or i.get('source') or '').strip().lower() for i in g['items'] if str(i.get('media') or i.get('source') or '').strip()}
    n=len(g['items']); sc=len(sources)
    # 방향은 시장 뉴스 자체의 제목에서만 읽고, 종목 원인으로 사용하지 않는다.
    pos=sum(1 for i in g['items'] if _news_polarity(i)>0)
    neg=sum(1 for i in g['items'] if _news_polarity(i)<0)
    direction = '상승' if pos>neg else '하락' if neg>pos else ''
    phrases={
        'fomc': 'FOMC·연준 발언 관련 소식이 전해졌어',
        'inflation': '미국 물가 관련 소식이 전해졌어',
        'jobs': '미국 고용 관련 소식이 전해졌어',
        'rates': '미국 금리 관련 소식이 전해졌어',
        'tariff': '관세·무역 관련 소식이 전해졌어',
        'market': '미국 증시 관련 소식이 전해졌어',
    }
    base=phrases.get(g['topic'],'시장 관련 소식이 전해졌어')
    if direction:
        if g['topic']=='fomc':
            text=base+f' 시장은 {direction} 흐름을 보였어.'
        else:
            text=base+f' 시장은 {direction} 흐름을 보였어.'
    else:
        text=base+'.'
    return {"topic":g['topic'],"text":text,"repeat_count":n,"source_count":sc,"evidence":g['items'][:6]}
