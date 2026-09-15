import re
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor

import numpy as np

from performance import HTTP_SESSION

US_SECTOR_STOCK_UNIVERSE = {
    "SOXX": [
        ("NVDA", "엔비디아"), ("AVGO", "브로드컴"), ("AMD", "AMD"),
        ("MU", "마이크론"), ("MRVL", "마벨"), ("QCOM", "퀄컴"),
        ("TSM", "TSMC"), ("LRCX", "램리서치"), ("AMAT", "어플라이드"), ("KLAC", "KLA"),
    ],
    "AIQ": [
        ("NVDA", "엔비디아"), ("AVGO", "브로드컴"), ("ANET", "아리스타"),
        ("VRT", "버티브"), ("DELL", "델"), ("PLTR", "팔란티어"),
        ("SMCI", "슈퍼마이크로"), ("ARM", "ARM"), ("MRVL", "마벨"), ("CDNS", "케이던스"),
    ],
    "IGV": [
        ("MSFT", "마이크로소프트"), ("ORCL", "오라클"), ("CRM", "세일즈포스"),
        ("ADBE", "어도비"), ("NOW", "서비스나우"), ("INTU", "인튜이트"),
        ("PANW", "팔로알토"), ("PLTR", "팔란티어"), ("SNPS", "시놉시스"), ("CDNS", "케이던스"),
    ],
    "FDN": [
        ("META", "메타"), ("GOOGL", "알파벳"), ("AMZN", "아마존"),
        ("NFLX", "넷플릭스"), ("UBER", "우버"), ("ABNB", "에어비앤비"),
        ("DASH", "도어대시"), ("PINS", "핀터레스트"), ("RDDT", "레딧"), ("SNAP", "스냅"),
    ],
    "XLF": [
        ("JPM", "JP모건"), ("BAC", "뱅크오브아메리카"), ("GS", "골드만삭스"),
        ("MS", "모건스탠리"), ("C", "씨티그룹"), ("WFC", "웰스파고"),
        ("BLK", "블랙록"), ("SCHW", "찰스슈왑"), ("AXP", "아메리칸익스프레스"), ("V", "비자"),
    ],
    "XBI": [
        ("REGN", "리제네론"), ("VRTX", "버텍스"), ("AMGN", "암젠"),
        ("GILD", "길리어드"), ("BIIB", "바이오젠"), ("MRNA", "모더나"),
        ("ILMN", "일루미나"), ("INCY", "인사이트"), ("BMRN", "바이오마린"), ("ALNY", "앨나일람"),
    ],
    "XLV": [
        ("LLY", "일라이릴리"), ("UNH", "유나이티드헬스"), ("JNJ", "존슨앤드존슨"),
        ("ABBV", "애브비"), ("MRK", "머크"), ("TMO", "써모피셔"),
        ("ABT", "애보트"), ("ISRG", "인튜이티브서지컬"), ("DHR", "다나허"), ("PFE", "화이자"),
    ],
    "DRIV": [
        ("TSLA", "테슬라"), ("GM", "GM"), ("F", "포드"), ("RIVN", "리비안"),
        ("LCID", "루시드"), ("LI", "리오토"), ("NIO", "니오"), ("XPEV", "샤오펑"),
        ("UBER", "우버"), ("APTV", "앱티브"),
    ],
    "ITA": [
        ("RTX", "RTX"), ("LMT", "록히드마틴"), ("NOC", "노스럽그루먼"),
        ("GD", "제너럴다이내믹스"), ("HII", "헌팅턴잉걸스"), ("LHX", "L3해리스"),
        ("GE", "GE에어로스페이스"), ("BA", "보잉"), ("TDG", "트랜스다임"), ("HEI", "하이코"),
    ],
    "XLE": [
        ("XOM", "엑슨모빌"), ("CVX", "셰브론"), ("COP", "코노코필립스"), ("EOG", "EOG리소스"),
        ("SLB", "SLB"), ("OXY", "옥시덴탈"), ("MPC", "마라톤페트롤리엄"),
        ("PSX", "필립스66"), ("VLO", "발레로"), ("HAL", "할리버튼"),
    ],
    "NLR": [
        ("CEG", "컨스텔레이션"), ("VST", "비스트라"), ("GEV", "GE버노바"),
        ("NEE", "넥스트에라"), ("DUK", "듀크에너지"), ("SO", "서던컴퍼니"),
        ("AEP", "아메리칸일렉트릭"), ("ETN", "이튼"), ("CARR", "캐리어"), ("BWXT", "BWX테크놀로지"),
    ],
    "XLB": [
        ("LIN", "린데"), ("SHW", "셔윈윌리엄스"), ("APD", "에어프로덕츠"),
        ("ECL", "에코랩"), ("FCX", "프리포트맥모란"), ("NEM", "뉴몬트"),
        ("NUE", "뉴코"), ("DD", "듀폰"), ("ALB", "앨버말"), ("DOW", "다우"),
    ],
    "XLI": [
        ("GE", "GE에어로스페이스"), ("CAT", "캐터필러"), ("DE", "디어"), ("HON", "허니웰"),
        ("UNP", "유니언퍼시픽"), ("UPS", "UPS"), ("WM", "웨이스트매니지먼트"),
        ("RTX", "RTX"), ("ETN", "이튼"), ("MMM", "3M"),
    ],
    "XLY": [
        ("AMZN", "아마존"), ("TSLA", "테슬라"), ("HD", "홈디포"), ("MCD", "맥도날드"),
        ("TJX", "TJX"), ("LOW", "로우스"), ("NKE", "나이키"), ("BKNG", "부킹홀딩스"),
        ("SBUX", "스타벅스"), ("TGT", "타깃"),
    ],
    "XLRE": [
        ("PLD", "프로로지스"), ("AMT", "아메리칸타워"), ("EQIX", "에퀴닉스"),
        ("CCI", "크라운캐슬"), ("WELL", "웰타워"), ("SPG", "사이먼프로퍼티"),
        ("O", "리얼티인컴"), ("VICI", "VICI프로퍼티"), ("DLR", "디지털리얼티"), ("PSA", "퍼블릭스토리지"),
    ],
}

US_TICKER_PATTERN = re.compile(r"^[A-Z]{1,6}$")

CACHED_US_LEADING_STOCKS = {}
LAST_US_LEADING_STOCKS_FETCH = {}
US_LEADING_STOCKS_TTL = 300

def _fetch_us_stock_snapshot(item):
    """업종별 핵심 후보의 실제 현재가/당일 등락률/시가총액을 조회한다."""
    symbol, name = item
    try:
        encoded = urllib.parse.quote(symbol)
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{encoded}?range=3mo&interval=1d"
        res = HTTP_SESSION.get(
            url,
            timeout=5.0,
            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json,text/plain,*/*"}
        )
        res.raise_for_status()
        result = (res.json().get("chart", {}).get("result") or [None])[0]
        if not result:
            return None

        quote = (result.get("indicators", {}).get("quote") or [{}])[0]
        closes = quote.get("close") or []
        clean = []
        for value in closes:
            try:
                v = float(value)
                if np.isfinite(v):
                    clean.append(v)
            except Exception:
                continue
        if len(clean) < 2:
            return None

        latest = clean[-1]
        prev = clean[-2]
        current = ((latest / prev) - 1.0) * 100.0 if prev else 0.0

        # 기존 카드의 최근 20거래일 미니차트 유지
        spark_src = clean[-20:]
        lo, hi = min(spark_src), max(spark_src)
        if hi - lo < 1e-12:
            spark = [17.0] * len(spark_src)
        else:
            spark = [
                round(31.0 - ((v - lo) / (hi - lo)) * 28.0, 1)
                for v in spark_src
            ]

        # 시가총액은 후보군의 "핵심 10개" 검증/구성용 실제 데이터
        market_cap = None
        try:
            qurl = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={encoded}"
            qres = HTTP_SESSION.get(
                qurl,
                timeout=5.0,
                headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json,text/plain,*/*"}
            )
            if qres.ok:
                rows = qres.json().get("quoteResponse", {}).get("result") or []
                if rows and rows[0].get("marketCap") is not None:
                    market_cap = float(rows[0]["marketCap"])
        except Exception:
            pass

        return {
            "symbol": symbol,
            "name": name,
            "price": latest,
            "current": current,
            "market_cap": market_cap,
            "sparkline": spark,
        }
    except Exception as exc:
        print(f"[US STOCK] {symbol} failed: {type(exc).__name__}: {exc}")
        return None

def fetch_us_leading_stocks(sector_symbol, force=False):
    """
    업종별 핵심 종목군 10개 중 당일 상승률이 높은 TOP 5를 선정한다.

    - 후보군: 업종별 핵심 종목 10개
    - 선정: 당일 상승률 내림차순
    - 거래대금/1W/1M/20일 평균 거래대금은 선정에 사용하지 않음
    - 시가총액은 후보군 자체를 핵심 종목으로 구성하는 기준이며,
      TOP 5의 당일 순위에는 직접 가중하지 않음
    """
    sector_symbol = str(sector_symbol or "").strip().upper()
    candidates = US_SECTOR_STOCK_UNIVERSE.get(sector_symbol)
    if not candidates:
        return []

    now = time.time()
    if (
        not force
        and sector_symbol in CACHED_US_LEADING_STOCKS
        and now - LAST_US_LEADING_STOCKS_FETCH.get(sector_symbol, 0.0) < US_LEADING_STOCKS_TTL
    ):
        return CACHED_US_LEADING_STOCKS[sector_symbol]

    with ThreadPoolExecutor(max_workers=min(10, max(1, len(candidates)))) as executor:
        rows = list(executor.map(_fetch_us_stock_snapshot, candidates))

    rows = [r for r in rows if r and r.get("price") is not None]
    if not rows:
        return []

    # 핵심 10개 안에서 "오늘 가장 많이 오른 종목" 순.
    # 동률일 때만 시가총액을 보조 기준으로 사용한다.
    rows.sort(
        key=lambda r: (
            r.get("current", float("-inf")),
            r.get("market_cap") or 0.0,
        ),
        reverse=True,
    )

    top = [
        {
            "symbol": r["symbol"],
            "name": r["name"],
            "price": r["price"],
            "current": r["current"],
            "sparkline": r["sparkline"],
        }
        for r in rows[:5]
    ]

    CACHED_US_LEADING_STOCKS[sector_symbol] = top
    LAST_US_LEADING_STOCKS_FETCH[sector_symbol] = now

    print(
        f"[US LEADING STOCKS] {sector_symbol} TOP5: "
        + ", ".join(f"{r['symbol']}({r['current']:+.2f}%)" for r in top)
    )
    return top
