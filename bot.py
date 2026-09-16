from earnings_calendar import _classify_earnings_hour, _normalize_earnings_timestamp, _fetch_yahoo_earnings_timestamps
from world_board import fetch_single_world_item
from crypto import _fetch_crypto_candles_uncached, fetch_crypto_candles
from overseas_market import fetch_overseas_candles
from us_leading_sectors import fetch_us_leading_sectors
from cache import EXTERNAL_CACHE_LOCK, FRED_LAST_REQUEST_TS, FRED_MIN_INTERVAL, API_LOCK, _rate_limit_wait, _cache_get, _cache_set
import os
import ast
from serialization import clean_for_json
from chart_payload import _build_chart_payload
from news_merge import merge_news_sources
from macro_sources import (FRED_CACHE, FRED_CACHE_TTL, ALPHA_NEWS_CACHE, ALPHA_NEWS_CACHE_TTL, ALPHA_EARNINGS_CACHE, ALPHA_EARNINGS_CACHE_TTL, fetch_fred_macro_snapshot, fetch_alpha_vantage_news, fetch_alpha_vantage_earnings)

# Shared HTTP/performance infrastructure
from performance import HTTP_SESSION, _PERF_HISTORY, _PERF_HISTORY_MAX, _PERF_LOCAL, _PERF_REQUESTS, _PERF_REQUESTS_LOCK, _perf_record_http, _perf_run_with_context, _perf_submit, _perf_stage, _perf_store_history, _perf_history_snapshot, _timed_http_get, _perf_begin, _perf_snapshot
from chart_runtime import _fetch_fast_domestic_chart, _fetch_fast_overseas_chart

# 실적 캘린더 첫 화면에 보여줄 주요 예정 실적 수
EARNINGS_PREVIEW_COUNT = 4
import re
import io
import gzip
import gc
import json
import time
import urllib.parse
import xml.etree.ElementTree as ET
import html
import threading
from datetime import datetime, timedelta, timezone
try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None
from concurrent.futures import ThreadPoolExecutor, wait, Future, FIRST_COMPLETED
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

import requests
import pandas as pd
import numpy as np



# Quant/Trading-value module
from market_index import fetch_naver_realtime_index, fetch_market_5day_returns_from_df, fetch_market_5day_returns_from_symbol, fetch_naver_index_5day_returns
from quant import (
    get_trading_val_rank,
    run_real_vectorized_backtest,
    _cached_quant_backtest,
    calculate_quant_factors,
)
from numeric_utils import _safe_float


from economic_calendar import (
    _calendar_event_common, _impact_to_stars, _parse_calendar_timestamp,
    _filter_kr_us_economic_events, _is_allowed_economic_country,
    ECONOMIC_CALENDAR_ALLOWED_COUNTRIES, build_earnings_event,
)

# ----------------------------------------------------
# 1. 기본 설정 및 종목/지수 매핑
# ----------------------------------------------------
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
TELEGRAM_BOT_USERNAME = os.environ.get("TELEGRAM_BOT_USERNAME", "gaemigtp_bot").strip()
DART_API_KEY = os.environ.get("DART_API_KEY", "").strip()
FINNHUB_API_KEY = os.environ.get("FINNHUB_API_KEY", "").strip()
FRED_API_KEY = os.environ.get("FRED_API_KEY", "").strip()
TRADING_ECONOMICS_API_KEY = os.environ.get("TRADING_ECONOMICS_API_KEY", "").strip()
ALPHA_VANTAGE_API_KEY = os.environ.get("ALPHA_VANTAGE_API_KEY", "").strip()

FRED_API_KEY = os.environ.get("FRED_API_KEY", "").strip()
TRADING_ECONOMICS_API_KEY = os.environ.get("TRADING_ECONOMICS_API_KEY", "").strip()

NEWS_API_KEY = os.environ.get("NEWS_API_KEY", "").strip()
FINLIGHT_API_KEY = os.environ.get("FINLIGHT_API_KEY", "").strip()
import consensus as _consensus
from consensus import fetch_market_consensus_unified
_consensus.FINNHUB_API_KEY = FINNHUB_API_KEY
_consensus.ALPHA_VANTAGE_API_KEY = ALPHA_VANTAGE_API_KEY

GLOBAL_INDEX_MAP = {
    "코스피": "^KS11", "KOSPI": "^KS11",
    "코스닥": "^KQ11", "KOSDAQ": "^KQ11",
    "나스닥": "^IXIC", "NASDAQ": "^IXIC",
    "나스닥종합": "^IXIC", "NASDAQCOMPOSITE": "^IXIC",
    "나스닥100": "^NDX", "NASDAQ100": "^NDX",
    "VIX": "^VIX", "빅스": "^VIX", "변동성": "^VIX",
    "WTI유가": "CL=F", "WTI": "CL=F", "유가": "CL=F", "원유": "CL=F",
    "금": "GC=F", "GOLD": "GC=F", "골드": "GC=F",
    "환율": "KRW=X", "USD/KRW": "KRW=X", "원달러": "KRW=X"
}

CRYPTO_MAP = {
    "BTC": "BTCUSDT", "비트코인": "BTCUSDT", "비트": "BTCUSDT",
    "ETH": "ETHUSDT", "이더리움": "ETHUSDT", "이더": "ETHUSDT",
    "XRP": "XRPUSDT", "리플": "XRPUSDT",
    "DOGE": "DOGEUSDT", "도지코인": "DOGEUSDT", "도지": "DOGEUSDT",
    "SOL": "SOLUSDT", "솔라나": "SOLUSDT",
    "ADA": "ADAUSDT", "에이다": "ADAUSDT",
    "AVAX": "AVAXUSDT", "아발란체": "AVAXUSDT",
    "SKY": "SKYUSDT", "스카이": "SKYUSDT"
}

US_STOCK_MAP = {
    "엔비디아": "NVDA", "NVDA": "NVDA",
    "테슬라": "TSLA", "TSLA": "TSLA",
    "애플": "AAPL", "AAPL": "AAPL",
    "마이크로소프트": "MSFT", "마소": "MSFT", "MSFT": "MSFT",
    "아마존": "AMZN", "AMZN": "AMZN",
    "구글": "GOOGL", "알파벳": "GOOGL", "GOOGL": "GOOGL",
    "메타": "META", "META": "META",
    "AMD": "AMD", "아이온큐": "IONQ", "IONQ": "IONQ",
    "팔란티어": "PLTR", "PLTR": "PLTR",
    "대만반도체": "TSM", "TSM": "TSM", "TSMC": "TSM",
    "QQQ": "QQQ", "SPY": "SPY", "SOXL": "SOXL", "TQQQ": "TQQQ",
    "DIA": "DIA", "SOXX": "SOXX"
}
from us_leading_stocks import US_SECTOR_STOCK_UNIVERSE, US_TICKER_PATTERN, fetch_us_leading_stocks
from news_sources import (fetch_google_stock_news, fetch_google_economic_news, fetch_rss_news)
try:
    from .right_market_us import _right_sidebar_us_rows
except ImportError:
    from right_market_us import _right_sidebar_us_rows


# 주도종목 티커도 일반 미국주식 검색 경로에서 동일하게 처리
for _sector_candidates in US_SECTOR_STOCK_UNIVERSE.values():
    for _ticker, _ko_name in _sector_candidates:
        US_STOCK_MAP.setdefault(_ticker, _ticker)

DOMESTIC_FAST_MAP = {
    "SK하이닉스": "000660", "하이닉스": "000660",
    "삼성전자": "005930", "삼전": "005930", "삼성전기": "009150", "삼성중공업": "010140",
    "SK증권": "001510", "SK스퀘어": "402340", "솔트룩스": "304100",
    "알테오젠": "196170", "에코프로": "086520", "에코프로비엠": "247540",
    "레인보우로보틱스": "277810", "카카오": "035720", "네이버": "035420",
    "NAVER": "035420", "현대차": "005380", "기아": "000270",
    "포스코홀딩스": "005490", "POSCO홀딩스": "005490", "LG에너지솔루션": "373220",
    "한화오션": "042660", "두산에너빌리티": "034020", "한화에어로스페이스": "012450",
    "현대모비스": "012330", "셀트리온": "068270", "한미반도체": "042700",
    "씨젠": "096530", "HLB": "028300", "에코프로머티": "450080",
    "리가켐바이오": "141080", "엔켐": "348370", "삼천당제약": "000250",
    "휴젤": "145020", "클래시스": "214150", "리노공업": "058470"
}

WORLD_BOARD_CONFIG = [
    {"name": "코스피", "sym": "^KS11", "tag": "국내"},
    {"name": "코스닥", "sym": "^KQ11", "tag": "국내"},
    {"name": "QQQ", "sym": "QQQ", "tag": "나스닥100"},
    {"name": "SPY", "sym": "SPY", "tag": "S&P500"},
    {"name": "SOXX", "sym": "SOXX", "tag": "반도체"},
    {"name": "DIA", "sym": "DIA", "tag": "다우존스"},
    {"name": "VIX", "sym": "^VIX", "tag": "변동성"},
    {"name": "USD/KRW", "sym": "KRW=X", "tag": "환율"},
    {"name": "WTI 유가", "sym": "CL=F", "tag": "원유"},
    {"name": "금", "sym": "GC=F", "tag": "안전자산"}
]

# ----------------------------------------------------
# 2. JSON 정제 헬퍼 & 초고속 인메모리 캐시
# ----------------------------------------------------
CACHED_WORLD_BOARD = []
CACHED_WORLD_BOARD_FETCH_TIME = 0
WORLD_BOARD_TTL = 300
WORLD_BOARD_CACHE_LOCK = threading.Lock()
CACHED_MARKET_CALENDAR = []
LAST_CALENDAR_FETCH_TIME = 0
ANALYSIS_CACHE = {}
ANALYSIS_ENRICHING = set()
ANALYSIS_ENRICH_CONTEXT = {}
ANALYSIS_ENRICH_LOCK = threading.Lock()
ANALYSIS_CORE_TTL = 180  # core→details 전환에 충분한 시간 확보

# ===== External API cache / rate-limit protection =====



EARNINGS_RESULT_CACHE = {}
EARNINGS_RESULT_CACHE_TTL = 1800  # 30분
OPTIONAL_API_EXECUTOR = ThreadPoolExecutor(
    max_workers=18,
    thread_name_prefix="gaemi-optional",
)
ANALYSIS_ENRICH_EXECUTOR = ThreadPoolExecutor(
    max_workers=6,
    thread_name_prefix="gaemi-enrich",
)
OPTIONAL_API_SOFT_TIMEOUT = 6.0  # 첫 화면에서 뉴스/컨센서스 등 실제 데이터를 기다리는 최대 대기시간
ANALYSIS_ENRICH_WAIT = 10.0  # 백그라운드 details 단계에서 외부 데이터가 준비될 최대 대기시간

# 통합 컨센서스 결과 캐시. 컨센서스는 초단위 데이터가 아니므로
# 매 종목 검색마다 Naver/Finnhub/Yahoo fallback 체인을 다시 타지 않는다.
# Step 1에서는 기존 데이터 수집 로직은 그대로 유지하고, 중복 호출만 차단한다.








# ===== 미국 주도종목 위젯 (2단계: 실제 데이터) =====
# 업종별 후보군은 명시적으로 관리하고, 실제 순위는 Yahoo Finance 가격 데이터로 매번 계산한다.
# 즉, 후보군 자체를 고정 TOP5로 박아두지 않고 현재/1주/1개월 모멘텀을 합산해 TOP5를 자동 선정한다.



ECO_TITLE_KO_MAP = {
    "Non-Farm Employment Change": "비농업 고용보고서 (NFP)",
    "Unemployment Rate": "실업률",
    "CPI m/m": "소비자물가지수(CPI) MoM",
    "CPI y/y": "소비자물가지수(CPI) YoY",
    "Core CPI m/m": "근원 소비자물가지수(CPI) MoM",
    "Core CPI y/y": "근원 소비자물가지수(CPI) YoY",
    "Federal Funds Rate": "FOMC 기준금리 결정",
    "FOMC Statement": "FOMC 성명서 발표",
    "FOMC Press Conference": "FOMC 파월 의장 기자회견",
    "Core PCE Price Index m/m": "근원 PCE 물가지수 MoM",
    "Core PCE Price Index y/y": "근원 PCE 물가지수 YoY",
    "Unemployment Claims": "신규 실업수당청구건수",
    "Prelim GDP q/q": "GDP 성장률 (잠정치)",
    "Final GDP q/q": "GDP 성장률 (확정치)",
    "Retail Sales m/m": "소매판매 MoM",
    "Core Retail Sales m/m": "근원 소매판매 MoM",
    "ISM Manufacturing PMI": "ISM 제조업 구매관리자지수",
    "ISM Services PMI": "ISM 서비스업 구매관리자지수",
    "CB Consumer Confidence": "컨퍼런스보드 소비자신뢰지수",
    "Existing Home Sales": "기존주택판매",
    "New Home Sales": "신규주택판매",
    "Building Permits": "건축승인건수",
    "Crude Oil Inventories": "원유재고",
    "PPI m/m": "생산자물가지수(PPI) MoM",
    "PPI y/y": "생산자물가지수(PPI) YoY",
    "Core PPI m/m": "근원 생산자물가지수(PPI) MoM",
    "Richmond Manufacturing Index": "리치몬드 제조업지수",
    "Housing Starts": "주택착공건수",
    "Trade Balance": "무역수지",
    "Flash Manufacturing PMI": "제조업 PMI (속보치)",
    "Flash Services PMI": "서비스업 PMI (속보치)",
    "FOMC Member Barr Speaks": "미 연준 바 이사 발언"
}

COUNTRY_PREFIX_MAP = {
    "USD": "미 ", "EUR": "유로존 ", "GBP": "영국 ", "JPY": "일본 ",
    "CNY": "중국 ", "KRW": "한국 ", "CAD": "캐나다 ", "AUD": "호주 "
}

# 실적 캘린더에서 추적할 "주요 미국 기업" 목록
# - 단순히 10개 종목만 조회하지 않고, 시장 영향도가 큰 주요 기업을 넓게 포함
# - 실제 표시 여부는 실적 데이터가 존재하는 날짜와 별표 중요도 필터로 결정
MAJOR_US_TECH_TICKERS = [
    ("NVDA", "엔비디아"),
    ("AVGO", "브로드컴"),
    ("TSLA", "테슬라"),
    ("AAPL", "애플"),
    ("MSFT", "마이크로소프트"),
    ("AMZN", "아마존"),
    ("GOOGL", "구글"),
    ("META", "메타"),
    ("AMD", "AMD"),
    ("ORCL", "오라클"),
    ("MU", "마이크론"),
    ("COST", "코스트코"),
    ("ADBE", "어도비"),
    ("CRM", "세일즈포스"),
    ("NFLX", "넷플릭스"),
    ("INTC", "인텔"),
    ("QCOM", "퀄컴"),
    ("AMAT", "어플라이드 머티어리얼즈"),
    ("CSCO", "시스코"),
    ("PANW", "팔로알토 네트웍스"),
    ("SNOW", "스노우플레이크"),
    ("DELL", "델 테크놀로지스"),
    ("HPE", "휴렛팩커드 엔터프라이즈"),
    ("FDX", "페덱스"),
    ("NKE", "나이키"),
    ("ACN", "액센츄어"),
    ("CTAS", "신타스"),
    ("CIEN", "시에나"),
    ("ADSK", "오토데스크"),
    ("MRVL", "마벨 테크놀로지"),
]

MAJOR_KR_EARNINGS = {
    "005930": "삼성전자", "005930.KS": "삼성전자",
    "000660": "SK하이닉스", "000660.KS": "SK하이닉스",
    "005380": "현대차", "005380.KS": "현대차",
    "000270": "기아", "000270.KS": "기아",
    "005490": "POSCO홀딩스", "005490.KS": "POSCO홀딩스",
    "012450": "한화에어로스페이스", "012450.KS": "한화에어로스페이스",
    "042660": "한화오션", "042660.KS": "한화오션",
    "034020": "두산에너빌리티", "034020.KS": "두산에너빌리티",
    "012330": "현대모비스", "012330.KS": "현대모비스",
    "009150": "삼성전기", "009150.KS": "삼성전기",
    "373220": "LG에너지솔루션", "373220.KS": "LG에너지솔루션",
    "035420": "NAVER", "035420.KS": "NAVER",
    "035720": "카카오", "035720.KS": "카카오",
    "068270": "셀트리온", "068270.KS": "셀트리온",
    "042700": "한미반도체", "042700.KS": "한미반도체"
}

# ----------------------------------------------------
# 3. 3단계 하이브리드 실적 일정 수집기 (한국/미국 완벽 대응)
# ----------------------------------------------------




def _fetch_all_real_earnings_uncached():
    """
    국내 + 미국 주요기업 실적 캘린더 수집기.

    설계 원칙
    - 과거 30일 + 오늘 + 앞으로 30일의 실적을 KST 기준으로 표시한다.
    - Finnhub의 국제 실적 캘린더를 사용하되, 무작위 소형/해외 종목이
      화면에 섞이지 않도록 사이트에서 추적하는 주요 미국/한국 기업만 허용한다.
    - 미국: Finnhub + Yahoo timestamp 보정.
    - 한국: Finnhub international=true + Yahoo timestamp 보정.
    - DART는 '미래 발표 예정일'을 제공하는 API가 아니므로 예정일 생성에
      가상 날짜를 만들지 않는다. DART는 기존 공시 기능에서 실제 발표/공시 확인용으로 사용한다.
    - API가 주는 날짜가 과거 30일~미래 30일 범위를 벗어나면 표시하지 않는다.
    - 같은 기업/같은 날짜/같은 시간은 1건으로 합친다.
    """

    events = []
    now_kst = datetime.now(timezone(timedelta(hours=9)))
    today_kst = now_kst.date()
    history_start = today_kst - timedelta(days=30)
    future_end = today_kst + timedelta(days=30)

    # ----------------------------------------------------
    # 추적 대상
    # ----------------------------------------------------
    us_name_map = {
        str(sym).upper(): ko
        for sym, ko in MAJOR_US_TECH_TICKERS
    }

    # 사이트 성격상 시장 영향력이 큰 미국 기업을 조금 더 보강한다.
    extra_us = {
        "PLTR": "팔란티어",
        "CRWD": "크라우드스트라이크",
        "INTU": "인튜이트",
        "NOW": "서비스나우",
        "AMGN": "암젠",
        "GILD": "길리어드 사이언스",
        "V": "비자",
        "MA": "마스터카드",
        "JPM": "JP모건",
        "BAC": "뱅크오브아메리카",
        "WMT": "월마트",
        "HD": "홈디포",
        "LOW": "로우스",
        "PEP": "펩시코",
        "KO": "코카콜라",
        "MCD": "맥도날드",
        "DIS": "디즈니",
        "UBER": "우버",
        "ABNB": "에어비앤비",
        "SNPS": "시놉시스",
        "CDNS": "케이던스",
        "LRCX": "램리서치",
        "KLAC": "KLA",
        "TXN": "텍사스 인스트루먼트",
        "ARM": "ARM",
        "MRNA": "모더나",
        "LLY": "일라이 릴리",
    }
    us_name_map.update(extra_us)

    kr_name_map = {}
    for k_code, k_name in MAJOR_KR_EARNINGS.items():
        key = str(k_code).upper().strip()
        kr_name_map[key] = k_name
        if key.isdigit() and len(key) == 6:
            kr_name_map[f"{key}.KS"] = k_name
            kr_name_map[f"{key}.KQ"] = k_name

    # Finnhub/Yahoo가 국내 종목을 6자리 또는 .KS 형태로 줄 수 있으므로
    # 두 형태를 모두 canonical 6자리로 맞춘다.
    def normalize_kr_symbol(sym):
        value = str(sym or "").upper().strip()
        value = value.replace(".KS", "").replace(".KQ", "")
        if value.isdigit() and len(value) == 6:
            return value
        return value

    allowed_us = set(us_name_map.keys())
    allowed_kr = set()
    for key in kr_name_map:
        canonical = normalize_kr_symbol(key)
        if canonical.isdigit() and len(canonical) == 6:
            allowed_kr.add(canonical)

    # 중요도
    us_tier3 = {
        "NVDA", "AAPL", "MSFT", "AMZN", "GOOGL", "META",
        "TSLA", "AVGO", "AMD", "ORCL", "MU", "PLTR",
        "JPM", "LLY", "WMT"
    }
    us_tier2 = allowed_us - us_tier3

    kr_tier3 = {
        "005930", "000660", "005380", "000270",
        "012450", "042660", "034020", "373220"
    }
    kr_tier2 = allowed_kr - kr_tier3

    def star_info(sym, market):
        if market == "US":
            s = str(sym).upper()
            if s in us_tier3:
                return 3, "★★★"
            if s in us_tier2:
                return 2, "★★☆"
            return 1, "★☆☆"

        s = normalize_kr_symbol(sym)
        if s in kr_tier3:
            return 3, "★★★"
        if s in kr_tier2:
            return 2, "★★☆"
        return 1, "★☆☆"

    # ----------------------------------------------------
    # Yahoo 실제 발표시각 조회
    # ----------------------------------------------------
    yahoo_symbols = list(allowed_us)
    yahoo_symbols += [f"{code}.KS" for code in sorted(allowed_kr)]
    yahoo_times = _fetch_yahoo_earnings_timestamps(yahoo_symbols)

    def get_yahoo_time(sym, market):
        candidates = [str(sym).upper().strip()]
        if market == "KR":
            canonical = normalize_kr_symbol(sym)
            candidates.extend([canonical, f"{canonical}.KS", f"{canonical}.KQ"])
        for candidate in candidates:
            if candidate in yahoo_times:
                return yahoo_times[candidate]
        return None

    # ----------------------------------------------------
    # 날짜/시간 → UTC
    # ----------------------------------------------------
    def make_utc_datetime(raw_date, hour, market, sym):
        try:
            base_date = datetime.strptime(
                str(raw_date).strip(), "%Y-%m-%d"
            ).date()
        except Exception:
            return None, hour

        hour = str(hour or "amc").lower().strip()

        # Yahoo의 실제 timestamp가 있으면 우선 사용한다.
        yahoo_dt = get_yahoo_time(sym, market)
        if yahoo_dt is not None:
            try:
                if abs((yahoo_dt.date() - base_date).days) <= 1:
                    normalized_dt, normalized_hour = _normalize_earnings_timestamp(
                        str(sym).upper(), yahoo_dt
                    )
                    if normalized_dt is not None:
                        return normalized_dt, normalized_hour
            except Exception:
                pass

        # 미국: Finnhub amc/bmo/dmh → 미국 동부시간
        if market == "US":
            if ZoneInfo is not None:
                try:
                    et = ZoneInfo("America/New_York")
                    if hour == "bmo":
                        local_text = f"{raw_date} 06:00"
                    elif hour == "dmh":
                        local_text = f"{raw_date} 09:30"
                    else:
                        local_text = f"{raw_date} 16:00"
                    local_dt = datetime.strptime(
                        local_text, "%Y-%m-%d %H:%M"
                    ).replace(tzinfo=et)
                    return local_dt.astimezone(timezone.utc), hour
                except Exception:
                    pass

            base_utc = datetime.combine(
                base_date,
                datetime.min.time(),
                tzinfo=timezone.utc
            )
            if hour == "bmo":
                return base_utc + timedelta(hours=10), hour
            return base_utc + timedelta(hours=20), hour

        # 한국: Finnhub에 시간이 없으면 한국시간 16:00을 안전한 표시 기준으로 사용.
        # '확정된 실제 발표시각'이라고 주장하지 않고 예정일만 정확히 표시한다.
        kst = timezone(timedelta(hours=9))
        local_dt = datetime.combine(
            base_date,
            datetime.min.time().replace(hour=16),
            tzinfo=kst
        )
        return local_dt.astimezone(timezone.utc), "amc"

    # ----------------------------------------------------
    # 이벤트 추가
    # ----------------------------------------------------
    def append_event(it, market):
        if not isinstance(it, dict):
            return

        raw_date = str(it.get("date", "")).strip()
        if not raw_date:
            return

        try:
            source_date = datetime.strptime(raw_date, "%Y-%m-%d").date()
        except Exception:
            return

        # 과거 30일 + 오늘 + 앞으로 30일의 실적을 표시한다.
        # 과거 날짜는 실제 발표가 끝난 실적으로 표시할 수 있다.
        if source_date < history_start or source_date > future_end:
            return

        raw_sym = str(it.get("symbol", "")).upper().strip()
        if not raw_sym:
            return

        if market == "US":
            sym = raw_sym
            if sym not in allowed_us:
                return
            ko_name = us_name_map.get(sym, sym)
        else:
            sym = normalize_kr_symbol(raw_sym)
            if sym not in allowed_kr:
                return
            ko_name = kr_name_map.get(sym, sym)

        hour = str(it.get("hour", "amc")).lower().strip()
        dt_utc, hour = make_utc_datetime(raw_date, hour, market, raw_sym)
        if dt_utc is None:
            return

        # KST 날짜가 실제 화면 기준 범위를 벗어나면 제외.
        kst_dt = dt_utc.astimezone(timezone(timedelta(hours=9)))
        if kst_dt.date() < history_start or kst_dt.date() > future_end:
            return

        eps_est = it.get("epsEstimate")
        rev_est = it.get("revenueEstimate")
        eps_actual = it.get("epsActual")
        rev_actual = it.get("revenueActual")

        try:
            eps_str = (
                f"예상 EPS ${float(eps_est):.2f}"
                if eps_est is not None else "예상치 집계중"
            )
        except Exception:
            eps_str = "예상치 집계중"

        try:
            rev_str = (
                f"예상 매출 ${float(rev_est) / 1e9:.1f}B"
                if rev_est is not None else "-"
            )
        except Exception:
            rev_str = "-"

        try:
            built = build_earnings_event(
                raw_sym if market == "US" else sym,
                ko_name,
                dt_utc,
                eps_str,
                rev_str,
                hour,
                actual_eps=eps_actual,
                actual_rev=rev_actual,
                expected_eps=eps_est,
                expected_rev=rev_est
            )
        except Exception as exc:
            print(
                f"[CALENDAR][EARN] 이벤트 생성 실패 "
                f"{market}/{raw_sym}/{raw_date}: {repr(exc)}"
            )
            return

        stars, star_str = star_info(sym, market)
        for ev in built:
            ev["stars"] = stars
            ev["star_str"] = star_str
            ev["market"] = market
            ev["source"] = "Finnhub"
            ev["confirmed"] = False
            ev["expected_revenue"] = rev_est
            ev["expected_revenue_str"] = rev_str
            events.append(ev)

    # ----------------------------------------------------
    # Finnhub
    #
    # 미국 + 국제시장(KR)을 한 번에 조회한다.
    # international=true가 중요하다.
    # 기존 코드의 기본값(false)은 미국 중심 데이터만 반환한다.
    # ----------------------------------------------------
    if FINNHUB_API_KEY:
        from_d = history_start.strftime("%Y-%m-%d")
        to_d = future_end.strftime("%Y-%m-%d")

        for international in (False, True):
            try:
                url = (
                    "https://finnhub.io/api/v1/calendar/earnings"
                    f"?from={from_d}"
                    f"&to={to_d}"
                    f"&international={'true' if international else 'false'}"
                    f"&token={FINNHUB_API_KEY}"
                )

                res = HTTP_SESSION.get(url, timeout=5.0).json()
                items = res.get("earningsCalendar", []) or []

                print(
                    f"[EARNINGS DEBUG] Finnhub "
                    f"international={international}: {len(items)}건"
                )

                for it in items:
                    sym = str(it.get("symbol", "")).upper().strip()

                    # 미국 주요 기업
                    if sym in allowed_us:
                        append_event(it, "US")
                        continue

                    # 한국 주요 기업
                    if normalize_kr_symbol(sym) in allowed_kr:
                        append_event(it, "KR")

            except Exception as exc:
                print(
                    f"[CALENDAR][EARN] Finnhub "
                    f"international={international} 오류: {repr(exc)}"
                )

    else:
        print("[CALENDAR][EARN] FINNHUB_API_KEY가 없어 Finnhub 조회를 건너뜁니다.")

    # ----------------------------------------------------
    # Yahoo 보조 수집
    # Finnhub이 놓친 '추적 대상' 종목만 보완한다.
    # ----------------------------------------------------
    captured = set()
    for ev in events:
        ticker = str(ev.get("ticker", "")).upper().strip()
        date_key = str(ev.get("date_key", "")).strip()
        captured.add((ticker, date_key))

    # Yahoo quote endpoint에서 이미 가져온 timestamp를 활용한다.
    # Yahoo quote timestamp는 현재/다음 실적용이므로 미래 보완에만 사용한다.
    for us_sym in sorted(allowed_us):
        dt_utc = get_yahoo_time(us_sym, "US")
        if dt_utc is None:
            continue

        try:
            kst_dt = dt_utc.astimezone(timezone(timedelta(hours=9)))
            if not (today_kst <= kst_dt.date() <= future_end):
                continue

            date_key = kst_dt.strftime("%Y.%m.%d")
            if (us_sym, date_key) in captured:
                continue

            normalized_dt, hour = _normalize_earnings_timestamp(us_sym, dt_utc)
            if normalized_dt is None:
                continue

            built = build_earnings_event(
                us_sym,
                us_name_map.get(us_sym, us_sym),
                normalized_dt,
                "예상치 집계중",
                "-",
                hour
            )

            stars, star_str = star_info(us_sym, "US")
            for ev in built:
                ev["stars"] = stars
                ev["star_str"] = star_str
                ev["market"] = "US"
                ev["source"] = "Yahoo"
                ev["confirmed"] = False
                events.append(ev)

        except Exception as exc:
            print(
                f"[CALENDAR][EARN] Yahoo US 보완 실패 {us_sym}: {repr(exc)}"
            )

    for kr_code in sorted(allowed_kr):
        yahoo_candidates = [
            f"{kr_code}.KS",
            f"{kr_code}.KQ",
            kr_code,
        ]
        dt_utc = None
        for candidate in yahoo_candidates:
            if candidate in yahoo_times:
                dt_utc = yahoo_times[candidate]
                break

        if dt_utc is None:
            continue

        try:
            kst_dt = dt_utc.astimezone(timezone(timedelta(hours=9)))
            if not (today_kst <= kst_dt.date() <= future_end):
                continue

            date_key = kst_dt.strftime("%Y.%m.%d")
            if (kr_code, date_key) in captured:
                continue

            normalized_dt, hour = _normalize_earnings_timestamp(
                f"{kr_code}.KS",
                dt_utc
            )
            if normalized_dt is None:
                continue

            built = build_earnings_event(
                kr_code,
                kr_name_map.get(kr_code, kr_code),
                normalized_dt,
                "예상치 집계중",
                "-",
                hour
            )

            stars, star_str = star_info(kr_code, "KR")
            for ev in built:
                ev["ticker"] = kr_code
                ev["stars"] = stars
                ev["star_str"] = star_str
                ev["market"] = "KR"
                ev["source"] = "Yahoo"
                ev["confirmed"] = False
                events.append(ev)

        except Exception as exc:
            print(
                f"[CALENDAR][EARN] Yahoo KR 보완 실패 {kr_code}: {repr(exc)}"
            )

    # ----------------------------------------------------
    # 중복 제거
    # ----------------------------------------------------
    unique = {}
    source_priority = {
        "Finnhub": 2,
        "Yahoo": 1,
    }

    for ev in events:
        ticker = str(ev.get("ticker", "")).upper().strip()
        date_key = str(ev.get("date_key", "")).strip()
        event_time = str(ev.get("time", "")).strip()
        market = str(ev.get("market", "")).upper().strip()

        if not ticker or not date_key:
            continue

        key = (
            market,
            ticker,
            date_key,
            event_time,
        )

        old = unique.get(key)
        if old is None:
            unique[key] = ev
            continue

        old_priority = source_priority.get(
            str(old.get("source", "")), 0
        )
        new_priority = source_priority.get(
            str(ev.get("source", "")), 0
        )

        if new_priority > old_priority:
            unique[key] = ev

    final_events = sorted(
        unique.values(),
        key=lambda x: (
            str(x.get("date_key", "")),
            str(x.get("time", "")),
            0 if str(x.get("market", "")) == "KR" else 1,
            str(x.get("ticker", "")),
        )
    )

    # ----------------------------------------------------
    # 디버그: 과거 30일 + 오늘 + 앞으로 30일 날짜별 건수
    # ----------------------------------------------------
    date_counts = {}
    market_counts = {"KR": 0, "US": 0}

    for ev in final_events:
        dk = str(ev.get("date_key", ""))
        if dk:
            date_counts[dk] = date_counts.get(dk, 0) + 1

        market = str(ev.get("market", "")).upper()
        if market in market_counts:
            market_counts[market] += 1

    print(
        f"[EARNINGS FINAL] 과거30일~미래30일: {len(final_events)}건 / "
        f"국내={market_counts['KR']}건 / 미국={market_counts['US']}건 / "
        f"일정있는날={len(date_counts)}일"
    )
    print(
        f"[EARNINGS FINAL] 날짜별: {date_counts}"
    )

    return final_events




def fetch_all_real_earnings():
    """실적 캘린더 공용 캐시. 실제 발표일 데이터만 저장한다."""
    cache_key = "all_real_earnings"
    cached = _cache_get(EARNINGS_RESULT_CACHE, cache_key, EARNINGS_RESULT_CACHE_TTL)
    if cached is not None:
        return list(cached) if isinstance(cached, list) else cached
    result = _fetch_all_real_earnings_uncached()
    if result:
        return _cache_set(EARNINGS_RESULT_CACHE, cache_key, list(result))
    return result or []

def fetch_real_market_calendar_live():
    """실적 + 경제 캘린더를 실제 API에서 가져와 화면용 단일 배열로 합칩니다."""
    global CACHED_MARKET_CALENDAR, LAST_CALENDAR_FETCH_TIME

    now = time.time()

    # 실적 데이터만 캐시되어 있는 경우에는 그대로 반환하면 안 됩니다.
    # 경제 캘린더도 반드시 함께 확인해야 합니다.
    # 기존 코드에서는 실적이 먼저 들어오면 CACHED_MARKET_CALENDAR가 truthy가 되어
    # 경제 API 호출 자체가 건너뛰어지는 문제가 있었습니다.
    if CACHED_MARKET_CALENDAR and (now - LAST_CALENDAR_FETCH_TIME) < 600:
        has_earnings = any(
            isinstance(ev, dict) and ev.get("type") == "earn"
            for ev in CACHED_MARKET_CALENDAR
        )
        has_economic = any(
            isinstance(ev, dict) and str(ev.get("type", "")).lower() in {"eco", "economic"}
            for ev in CACHED_MARKET_CALENDAR
        )
        if has_earnings and has_economic:
            return CACHED_MARKET_CALENDAR

    earnings = []
    economic = []

    try:
        earnings = fetch_all_real_earnings() or []
    except Exception as exc:
        print("[MARKET CALENDAR] 실적 캘린더 조회 실패:", repr(exc))

    try:
        economic = fetch_economic_calendar_history_future(
            days_back=180,
            days_forward=90,
        ) or []
    except Exception as exc:
        print("[MARKET CALENDAR] 경제 캘린더 조회 실패:", repr(exc))

    # 경제 캘린더는 한국/미국만 허용합니다.
    raw_economic_count = len(economic)
    economic = _filter_kr_us_economic_events(economic)

    print(
        f"[MARKET CALENDAR][ECO FILTER] "
        f"raw={raw_economic_count}, kr_us={len(economic)}"
    )

    combined = [
        ev for ev in (earnings + economic)
        if isinstance(ev, dict) and ev.get("date_key")
    ]

    unique = {}
    for ev in combined:
        key = (
            ev.get("type", ""),
            ev.get("ticker", ""),
            ev.get("date_key", ""),
            ev.get("time", ""),
            ev.get("name", ""),
        )
        unique[key] = ev

    combined = sorted(unique.values(), key=lambda x: x.get("ts", 0))

    # 빈 결과는 캐시하지 않습니다.
    if combined:
        CACHED_MARKET_CALENDAR = combined
        LAST_CALENDAR_FETCH_TIME = now
        print(
            f"[MARKET CALENDAR] 정상 수집: 전체 {len(combined)}건 / "
            f"실적 {len(earnings)}건 / 경제 {len(economic)}건"
        )
        return combined

    print("[MARKET CALENDAR] 실적/경제 캘린더 데이터가 없습니다.")
    return CACHED_MARKET_CALENDAR or []


def fetch_economic_calendar_history_future(days_back=180, days_forward=90):
    """경제 캘린더를 과거/현재/미래까지 안정적으로 수집한다.

    우선순위
    1) Finnhub (키가 있을 때)
    2) Trading Economics (키가 있을 때)
    3) ForexFactory 이번 주
    4) ForexFactory 다음 주

    중요:
    - 빈 결과는 캐시하지 않는다.
    - 한 소스가 실패해도 다른 소스로 계속 시도한다.
    - 오늘 일정이 없을 때의 '가장 최근 발표일 이동'은 프론트에서 처리한다.
    """
    now_kst = datetime.now(timezone(timedelta(hours=9)))
    from_d = (now_kst - timedelta(days=days_back)).strftime("%Y-%m-%d")
    to_d = (now_kst + timedelta(days=days_forward)).strftime("%Y-%m-%d")
    cache_key = f"eco:{from_d}:{to_d}"

    cached = _cache_get(ALPHA_NEWS_CACHE, cache_key, 1800)
    if cached:
        cached = _filter_kr_us_economic_events(cached)
        if cached:
            return cached

    events = []

    def _dedupe_and_sort(items):
        unique = {}
        for ev in items:
            key = (
                ev.get("type"),
                ev.get("date_key"),
                ev.get("time"),
                ev.get("name"),
                ev.get("country", ""),
            )
            unique[key] = ev
        return sorted(unique.values(), key=lambda x: x.get("ts", 0))

    # --------------------------------------------------------
    # 1. Finnhub
    # --------------------------------------------------------
    if FINNHUB_API_KEY:
        try:
            url = "https://finnhub.io/api/v1/calendar/economic"
            res = HTTP_SESSION.get(
                url,
                params={"from": from_d, "to": to_d, "token": FINNHUB_API_KEY},
                timeout=6.0,
            )
            if res.ok:
                payload = res.json()
                rows = payload.get("economicCalendar", []) if isinstance(payload, dict) else []

                for it in rows:
                    dt = _parse_calendar_timestamp(
                        it.get("time") or it.get("timestamp") or it.get("date")
                    )

                    if dt is None:
                        raw_date = str(it.get("date") or "").strip()
                        if raw_date:
                            try:
                                dt = datetime.strptime(
                                    raw_date[:10], "%Y-%m-%d"
                                ).replace(tzinfo=timezone.utc)
                            except Exception:
                                continue

                    if dt is None:
                        continue

                    stars, star_str = _impact_to_stars(
                        it.get("impact") or it.get("importance")
                    )
                    country = str(it.get("country") or "").strip()
                    raw_title = str(
                        it.get("event") or it.get("title") or ""
                    ).strip()

                    if not raw_title:
                        continue

                    prefix = COUNTRY_PREFIX_MAP.get(country, "")
                    title_ko = ECO_TITLE_KO_MAP.get(raw_title, raw_title)
                    full_name = f"{prefix}{title_ko}" if prefix else title_ko

                    actual = str(it.get("actual") or "").strip()
                    estimate = str(
                        it.get("estimate") or it.get("forecast") or ""
                    ).strip()
                    previous = str(
                        it.get("prev") or it.get("previous") or ""
                    ).strip()

                    if not actual:
                        actual = (
                            "발표완료"
                            if dt <= datetime.now(timezone.utc)
                            else "발표대기"
                        )

                    events.append(
                        _calendar_event_common(
                            dt,
                            full_name,
                            country,
                            stars,
                            star_str,
                            "eco",
                            actual,
                            estimate,
                            previous,
                            extra={
                                "unit": it.get("unit") or "",
                                "source": "Finnhub",
                            },
                        )
                    )

                finnhub_out = _filter_kr_us_economic_events(_dedupe_and_sort(events))
                if finnhub_out:
                    print(f"[CALENDAR][ECO] Finnhub 성공: {len(finnhub_out)}건")
                    return _cache_set(
                        ALPHA_NEWS_CACHE, cache_key, finnhub_out
                    )

            else:
                print(
                    f"[CALENDAR][ECO] Finnhub HTTP {res.status_code}"
                )

        except Exception as exc:
            print("[CALENDAR][ECO] Finnhub 실패:", repr(exc))

    # --------------------------------------------------------
    # 2. Trading Economics (선택형)
    # --------------------------------------------------------
    if TRADING_ECONOMICS_API_KEY:
        try:
            url = (
                "https://api.tradingeconomics.com/"
                f"calendar/country/united%20states/{from_d}/{to_d}"
            )
            res = HTTP_SESSION.get(
                url,
                params={
                    "c": TRADING_ECONOMICS_API_KEY,
                    "f": "json",
                },
                timeout=7.0,
            )

            if res.ok:
                rows = res.json()
                if isinstance(rows, list):
                    for it in rows:
                        dt = _parse_calendar_timestamp(
                            it.get("Date") or it.get("date")
                        )
                        if dt is None:
                            continue

                        stars, star_str = _impact_to_stars(
                            it.get("Importance") or it.get("importance")
                        )

                        raw_title = str(
                            it.get("Event")
                            or it.get("Category")
                            or it.get("event")
                            or ""
                        ).strip()

                        if not raw_title:
                            continue

                        country = str(
                            it.get("Country") or "USD"
                        ).strip()

                        prefix = COUNTRY_PREFIX_MAP.get(
                            "USD" if country.lower() == "united states" else country,
                            "미 " if country.lower() == "united states" else "",
                        )

                        title_ko = ECO_TITLE_KO_MAP.get(
                            raw_title,
                            raw_title,
                        )

                        full_name = f"{prefix}{title_ko}" if prefix else title_ko

                        actual = str(
                            it.get("Actual") or it.get("actual") or ""
                        ).strip()

                        forecast = str(
                            it.get("Forecast")
                            or it.get("TEForecast")
                            or it.get("forecast")
                            or ""
                        ).strip()

                        previous = str(
                            it.get("Previous")
                            or it.get("previous")
                            or ""
                        ).strip()

                        if not actual:
                            actual = (
                                "발표완료"
                                if dt <= datetime.now(timezone.utc)
                                else "발표대기"
                            )

                        events.append(
                            _calendar_event_common(
                                dt,
                                full_name,
                                country,
                                stars,
                                star_str,
                                "eco",
                                actual,
                                forecast,
                                previous,
                                extra={
                                    "unit": it.get("Unit") or "",
                                    "source": "TradingEconomics",
                                },
                            )
                        )

                    te_out = _filter_kr_us_economic_events(_dedupe_and_sort(events))
                    if te_out:
                        print(
                            f"[CALENDAR][ECO] Trading Economics 성공: {len(te_out)}건"
                        )
                        return _cache_set(
                            ALPHA_NEWS_CACHE, cache_key, te_out
                        )

            else:
                print(
                    f"[CALENDAR][ECO] Trading Economics HTTP {res.status_code}"
                )

        except Exception as exc:
            print("[CALENDAR][ECO] Trading Economics 실패:", repr(exc))

    # --------------------------------------------------------
    # 3~4. ForexFactory fallback
    # 이번 주 + 다음 주를 각각 요청하여 일요일/주말에도
    # 경제 일정 데이터가 끊기지 않도록 한다.
    # --------------------------------------------------------
    ff_urls = [
        "https://nfs.faireconomy.media/ff_calendar_thisweek.json",
        "https://nfs.faireconomy.media/ff_calendar_nextweek.json",
    ]

    for url in ff_urls:
        try:
            res = HTTP_SESSION.get(url, timeout=5.0)
            if not res.ok:
                print(
                    f"[CALENDAR][ECO] ForexFactory HTTP "
                    f"{res.status_code}: {url}"
                )
                continue

            payload = res.json()
            if not isinstance(payload, list):
                continue

            for it in payload:
                dt = _parse_calendar_timestamp(it.get("date", ""))
                if dt is None:
                    continue

                # 요청한 기간 밖이면 제외
                if dt < datetime.now(timezone.utc) - timedelta(days=days_back):
                    continue
                if dt > datetime.now(timezone.utc) + timedelta(days=days_forward):
                    continue

                stars, star_str = _impact_to_stars(it.get("impact"))
                country = str(it.get("country", "")).strip()
                prefix = COUNTRY_PREFIX_MAP.get(country, "")

                raw_title = str(it.get("title", "")).strip()
                if not raw_title:
                    continue

                title_ko = ECO_TITLE_KO_MAP.get(raw_title, raw_title)
                full_name = f"{prefix}{title_ko}" if prefix else title_ko

                actual_raw = str(it.get("actual", "")).strip()
                actual = actual_raw or (
                    "발표완료"
                    if dt <= datetime.now(timezone.utc)
                    else "발표대기"
                )

                events.append(
                    _calendar_event_common(
                        dt,
                        full_name,
                        country,
                        stars,
                        star_str,
                        "eco",
                        actual,
                        str(it.get("forecast", "")).strip() or "-",
                        str(it.get("previous", "")).strip() or "-",
                        extra={"source": "ForexFactory"},
                    )
                )

        except Exception as exc:
            print(
                f"[CALENDAR][ECO] ForexFactory 실패 "
                f"({url}): {repr(exc)}"
            )

    out = _filter_kr_us_economic_events(_dedupe_and_sort(events))

    # 절대 빈 결과를 30분 캐시하지 않는다.
    if out:
        print(f"[CALENDAR][ECO] ForexFactory fallback 성공: {len(out)}건")
        return _cache_set(ALPHA_NEWS_CACHE, cache_key, out)

    print("[CALENDAR][ECO] 모든 경제 일정 소스에서 데이터가 없습니다.")
    return []


# ----------------------------------------------------
# 4. 크롤러 및 유틸리티 함수
# ----------------------------------------------------























def find_stock_code(query):
    clean_q = query.strip().replace(" ", "").upper()
    if clean_q.isdigit() and len(clean_q) == 6:
        return clean_q, clean_q

    for name, code in DOMESTIC_FAST_MAP.items():
        if name.replace(" ", "").upper() == clean_q:
            return code, name

    try:
        url = f"https://m.stock.naver.com/front-api/search/autoComplete?query={urllib.parse.quote(query.strip())}&target=stock"
        res = HTTP_SESSION.get(url, timeout=1.8).json()
        items = res.get("result", {}).get("items", [])
        if items:
            for it in items:
                name = str(it.get("name", "")).strip()
                code = str(it.get("code", "")).strip()
                if name.replace(" ", "").upper() == clean_q:
                    return code, name
            first = items[0]
            return str(first.get("code", "")).strip(), str(first.get("name", "")).strip()
    except Exception:
        pass
    return None, query.strip()

from domestic_market import _fetch_domestic_candles_naver_uncached
from domestic_market_type import (
    get_naver_stock_market_type,
    _get_cached_domestic_market_type,
    _refresh_domestic_market_type,
    DOMESTIC_MARKET_TYPE_CACHE,
    DOMESTIC_MARKET_TYPE_CACHE_TTL,
    DOMESTIC_MARKET_TYPE_INFLIGHT,
    DOMESTIC_MARKET_TYPE_LOCK,
)

# ===== SPEED OPTIMIZATION 3: 공용 시세/차트 캐시 =====
# 같은 종목을 /api/analyze 와 /api/chart 에서 연속 조회할 때
# Naver/Yahoo/Binance를 다시 호출하지 않도록 공용 TTL 캐시를 사용한다.
MARKET_DATA_CACHE_LOCK = threading.Lock()
NAVER_REALTIME_CACHE = {}
NAVER_REALTIME_CACHE_TTL = 15
DOMESTIC_CANDLE_CACHE = {}
DOMESTIC_CANDLE_CACHE_TTL = 60 * 60
# STEP 15 V2: 국내 일봉은 하루 단위 데이터이므로 10분마다 강제 재조회하지 않는다.
# 신선 캐시는 1시간, 오래된 캐시는 최대 24시간까지 즉시 사용하고 백그라운드에서 갱신한다.
DOMESTIC_CANDLE_STALE_TTL = 24 * 60 * 60
DOMESTIC_CANDLE_REFRESHING = set()
DOMESTIC_CANDLE_REFRESH_LOCK = threading.Lock()
# STEP 10: /api/chart와 /api/analyze가 동시에 같은 종목을 요청할 때
# 동일한 Naver 1200일봉 HTTP 요청이 중복 발생하지 않도록 single-flight를 사용한다.
DOMESTIC_CANDLE_INFLIGHT = {}
DOMESTIC_CANDLE_INFLIGHT_LOCK = threading.Lock()

# ===== SPEED OPTIMIZATION 4: 국내주식 가격/차트 병렬 조회 =====
# 국내 종목 분석에서 실시간 가격, 거래소 구분, 일봉 차트를 순차 호출하지 않고
# 하나의 분석 요청 안에서 동시에 조회한다. 캐시는 각 함수가 독립적으로 사용한다.
# 기존 ThreadPoolExecutor를 매 요청마다 생성하는 비용도 줄이기 위해 공용 executor를 사용한다.
PRICE_DATA_EXECUTOR = ThreadPoolExecutor(max_workers=8)
# STEP 12: 국내주식 Yahoo .KS/.KQ 동시 조회 전용 풀.
# 기존 가격 executor 안에서 다시 작업을 제출할 때의 worker 고갈을 방지한다.
DOMESTIC_FAST_CANDLE_EXECUTOR = ThreadPoolExecutor(max_workers=6)

def _get_naver_realtime_price_uncached(code):
    try:
        url = f"https://polling.finance.naver.com/api/realtime/domestic/stock/{code}"
        res = HTTP_SESSION.get(url, timeout=1.8).json()
        datas = res.get("datas", [])
        if datas:
            p = str(datas[0].get("closePrice", "")).replace(",", "").strip()
            if p and p.replace(".", "").isdigit():
                return float(p), f"{int(float(p)):,} 원"
    except Exception:
        pass
    return None, None

def get_naver_realtime_price(code):
    key = str(code or "").strip().upper()
    if not key:
        return None, None
    now = time.time()
    cached = NAVER_REALTIME_CACHE.get(key)
    if cached and now - cached["time"] < NAVER_REALTIME_CACHE_TTL:
        return cached["value"]
    value = _get_naver_realtime_price_uncached(key)
    NAVER_REALTIME_CACHE[key] = {"time": now, "value": value}
    return value

def fetch_naver_domestic_flow_metrics(code):
    """국내주식 실데이터: 당일 거래대금 + 최근 5거래일 외국인/기관 순매수.

    STEP 17:
    - 거래대금은 오른쪽 실시간 사이드바에서 검증된 Naver polling realtime의 aa 필드를 우선 사용한다.
    - polling에서 거래대금을 못 가져오는 경우 기존 Naver integration 필드 탐색을 fallback으로 사용한다.
    - 외국인/기관 수급 로직은 기존 구조를 유지한다.
    """
    result={"trading_value":None,"foreigner_latest":None,"institution_latest":None,
            "foreigner_5d":None,"institution_5d":None,
            "trading_value_detail":"거래대금 데이터 확인 필요",
            "foreigner_detail":"외국인 수급 데이터 확인 필요",
            "institution_detail":"기관 수급 데이터 확인 필요"}
    if not code: return result

    def num(v):
        if v is None: return None
        if isinstance(v,(int,float)) and not isinstance(v,bool): return float(v)
        x=str(v).strip().replace(',','').replace(' ','')
        try:
            if any(u in x for u in ('조','억','만')):
                total=0.0; found=False
                for u,mul in [('조',10**12),('억',10**8),('만',10**4)]:
                    m=re.search(r'([+-]?\d+(?:\.\d+)?)'+u,x)
                    if m:
                        total += float(m.group(1))*mul; found=True
                return total if found else None
            return float(x.replace('%',''))
        except Exception:
            return None

    def norm_key(v):
        return re.sub(r'[^0-9a-z가-힣]','',str(v).lower())

    def walk_dicts(obj):
        if isinstance(obj,dict):
            yield obj
            for v in obj.values():
                yield from walk_dicts(v)
        elif isinstance(obj,list):
            for v in obj:
                yield from walk_dicts(v)

    try:
        r=HTTP_SESSION.get(f"https://m.stock.naver.com/api/stock/{code}/integration",timeout=2.2)
        if r.status_code!=200:
            return result
        data=r.json()

        # 1) 거래대금: 오른쪽 실시간 사이드바에서 실제 검증된
        # Naver polling realtime API의 aa(누적 거래대금)를 우선 사용한다.
        # integration 응답은 구조/필드가 종목별로 달라 거래대금이 누락될 수 있어
        # aa를 1순위로 두고, 기존 integration 탐색은 fallback으로 유지한다.
        tv=None
        try:
            query = "SERVICE_ITEM:" + str(code).strip()
            realtime_url = "https://polling.finance.naver.com/api/realtime?query=" + urllib.parse.quote(query, safe=":,")
            realtime_res = HTTP_SESSION.get(realtime_url, timeout=2.5, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/151.0 Safari/537.36",
                "Accept": "application/json,text/plain,*/*",
                "Referer": "https://stock.naver.com/",
            })
            realtime_res.raise_for_status()
            realtime_payload = realtime_res.json()
            realtime_datas=[]
            areas = realtime_payload.get("result", {}).get("areas", []) if isinstance(realtime_payload, dict) else []
            for area in areas:
                if isinstance(area,dict):
                    realtime_datas.extend(area.get("datas") or [])
            if not realtime_datas and isinstance(realtime_payload,dict):
                for area in realtime_payload.get("areas",[]) or []:
                    if isinstance(area,dict):
                        realtime_datas.extend(area.get("datas") or [])
            for item in realtime_datas:
                if not isinstance(item,dict):
                    continue
                item_code=str(item.get("cd") or item.get("itemCode") or "").strip()
                if item_code and item_code != str(code).strip():
                    continue
                candidate=num(item.get("aa") or item.get("accumulatedTradingValue"))
                if candidate is not None and candidate >= 0:
                    tv=candidate
                    break
        except Exception as realtime_exc:
            print(f"[NAVER FLOW] {code}: realtime 거래대금 조회 실패: {type(realtime_exc).__name__}: {realtime_exc}")

        # realtime aa를 가져오지 못한 경우에만 기존 integration 필드 탐색을 fallback으로 사용한다.
        if tv is None:
            priority_groups = [
                {
                    'accumulatedtradingvalue','accumulatedtradingamount',
                    'accumulatedtradingamountvalue','accumulatedamountvalue',
                    '누적거래대금','누적거래대금액'
                },
                {
                    'tradingvalue','tradingvalueamount','tradingamount',
                    '거래대금','거래대금액'
                },
                {
                    'amount','accumulatedamount'
                },
            ]
            candidates_by_group=[[] for _ in priority_groups]
            for obj in walk_dicts(data):
                for raw_k,v in obj.items():
                    k=norm_key(raw_k)
                    for idx,keys in enumerate(priority_groups):
                        if k in keys:
                            n=num(v)
                            if n is not None and n >= 0:
                                candidates_by_group[idx].append(n)
                            break

            for candidates in candidates_by_group:
                if candidates:
                    # 동일 응답 안에서 같은 필드가 여러 번 나타날 수 있으므로 최대값을 사용한다.
                    tv=max(candidates)
                    break

        result['trading_value']=tv
        if tv is not None:
            if tv >= 10**12:
                result['trading_value_detail']=f"당일 거래대금 {tv/10**12:.2f}조원"
            elif tv >= 10**8:
                result['trading_value_detail']=f"당일 거래대금 {tv/10**8:.0f}억원"
            else:
                result['trading_value_detail']=f"당일 거래대금 {tv:,.0f}원"

        # 2) 외국인/기관: 기존 dealTrendInfos 구조를 그대로 우선 사용한다.
        fv=[]; iv=[]
        for it in (data.get('dealTrendInfos') or [])[-5:]:
            if not isinstance(it,dict): continue
            for k in ('foreignerPureBuyQuant','foreignerNetBuyQuantity','foreignPureBuyQuant'):
                v=num(it.get(k))
                if v is not None:
                    fv.append(v); break
            for k in ('organPureBuyQuant','institutionPureBuyQuant','institutionNetBuyQuantity'):
                v=num(it.get(k))
                if v is not None:
                    iv.append(v); break
        if fv:
            total=sum(fv); result['foreigner_latest']=fv[-1]; result['foreigner_5d']=total
            result['foreigner_detail']=f"최근 5거래일 외국인 {'순매수' if total>0 else '순매도' if total<0 else '순매수/순매도 없음'} {abs(total):,.0f}주"
        if iv:
            total=sum(iv); result['institution_latest']=iv[-1]; result['institution_5d']=total
            result['institution_detail']=f"최근 5거래일 기관 {'순매수' if total>0 else '순매도' if total<0 else '순매수/순매도 없음'} {abs(total):,.0f}주"

        if tv is None:
            print(f"[NAVER FLOW] {code}: 거래대금 필드 미검출 (priority keys 탐색 완료)")
    except Exception as exc:
        print(f"[NAVER FLOW] {code} fetch failed: {exc}")
    return result

def _parse_yahoo_domestic_candles(code):
    """국내 종목의 5년 일봉을 Yahoo에서 빠르게 확보한다.
    .KS/.KQ를 동시에 조회해 시장 구분을 별도로 확인하지 않고도
    먼저 도착한 유효 데이터를 사용할 수 있게 한다.
    """
    def _one(suffix):
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{code}{suffix}?range=5y&interval=1d"
            res = HTTP_SESSION.get(url, timeout=1.5).json()
            result = res.get("chart", {}).get("result", [])
            if not result:
                return None
            item = result[0]
            ts = item.get("timestamp", []) or []
            quote = item.get("indicators", {}).get("quote", [{}])[0] or {}
            opens = quote.get("open") or []
            highs = quote.get("high") or []
            lows = quote.get("low") or []
            closes = quote.get("close") or []
            volumes = quote.get("volume") or []
            records = []
            for i in range(len(ts)):
                if i >= len(opens) or opens[i] is None or highs[i] is None or lows[i] is None or closes[i] is None:
                    continue
                records.append({
                    "Date": pd.to_datetime(ts[i], unit="s"),
                    "Open": float(opens[i]),
                    "High": float(highs[i]),
                    "Low": float(lows[i]),
                    "Close": float(closes[i]),
                    "Volume": float(volumes[i] or 0.0) if i < len(volumes) else 0.0,
                })
            if len(records) >= 250:
                return pd.DataFrame(records).set_index("Date").sort_index()
        except Exception:
            return None
        return None

    # 두 시장 suffix를 동시에 요청한다. 유효 응답 하나만 있으면 즉시 사용한다.
    futures = [
        _perf_submit(DOMESTIC_FAST_CANDLE_EXECUTOR, _one, ".KS"),
        _perf_submit(DOMESTIC_FAST_CANDLE_EXECUTOR, _one, ".KQ"),
    ]
    pending = set(futures)
    while pending:
        done, pending = wait(pending, return_when=FIRST_COMPLETED)
        for fut in done:
            try:
                df = fut.result()
            except Exception:
                df = None
            if df is not None and not df.empty:
                return df
    return None




def _refresh_domestic_candles_from_naver(code):
    """빠른 Yahoo 응답 이후 Naver 1200일봉으로 공용 캐시를 보강한다."""
    key = str(code or "").strip().upper()
    if not key:
        return None
    try:
        df = _fetch_domestic_candles_naver_uncached(key)
        if df is not None and not df.empty:
            DOMESTIC_CANDLE_CACHE[key] = {"time": time.time(), "df": df.copy(deep=True)}
            return df
    except Exception as exc:
        print(f"[DOMESTIC CANDLE NAVER REFRESH] {key} failed: {type(exc).__name__}: {exc}")
    return None


def _fetch_domestic_candles_uncached(code, anchor_price=None):
    """국내주식 일봉 원본 조회.

    STEP 12의 Yahoo 우선 경로에서 국내 종목이 "실제 데이터를 가져오지
    못했습니다"로 끝나는 문제가 확인되어, 검증된 Naver 경로를 우선한다.
    Yahoo는 Naver가 실패했을 때의 최종 fallback으로만 사용한다.
    이렇게 하면 기존 국내 퀀트/차트의 데이터 안정성을 우선하면서
    공용 캐시와 single-flight 효과는 그대로 유지할 수 있다.
    """
    # 1) 검증된 Naver 1200일봉
    try:
        url = f"https://fchart.stock.naver.com/sise.nhn?symbol={code}&timeframe=day&count=1200&requestType=0"
        res = HTTP_SESSION.get(url, timeout=2.2)
        if res.status_code == 200 and '<item data=' in res.text:
            root = ET.fromstring(res.content)
            records = []
            for it in root.findall(".//item"):
                d_str = it.get("data", "")
                p = d_str.split("|")
                if len(p) >= 6:
                    records.append({
                        "Date": pd.to_datetime(p[0]),
                        "Open": float(p[1]),
                        "High": float(p[2]),
                        "Low": float(p[3]),
                        "Close": float(p[4]),
                        "Volume": float(p[5]),
                    })
            if len(records) >= 15:
                df = pd.DataFrame(records).set_index("Date").sort_index()
                df.attrs["_gaemi_raw_df"] = df.copy()
                if anchor_price and anchor_price > 0 and not df.empty:
                    last_close = float(df["Close"].iloc[-1])
                    if last_close > 0 and abs(last_close - anchor_price) / anchor_price > 0.3:
                        ratio = anchor_price / last_close
                        df["Open"] *= ratio
                        df["High"] *= ratio
                        df["Low"] *= ratio
                        df["Close"] *= ratio
                return df
    except Exception as exc:
        print(f"[DOMESTIC NAVER CANDLE] {code} failed: {type(exc).__name__}: {exc}")

    # 2) Naver 신형 Chart API fallback
    # 일부 국내 종목(예: Reg.S 종목)은 legacy fchart에서 차트가 404/빈 응답이
    # 발생할 수 있다. 이 경우 api.stock.naver.com의 신형 chart endpoint를 사용한다.
    # 기존 fchart가 정상인 종목은 기존 경로를 그대로 사용한다.
    try:
        end_dt = datetime.now().strftime("%Y%m%d")
        start_dt = (datetime.now() - timedelta(days=365 * 5 + 20)).strftime("%Y%m%d")
        url = (
            f"https://api.stock.naver.com/chart/domestic/item/{code}"
            f"?periodType=dayCandle&startDateTime={start_dt}&endDateTime={end_dt}"
        )
        res = HTTP_SESSION.get(url, timeout=2.2, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/151.0 Safari/537.36",
            "Accept": "application/json,text/plain,*/*",
            "Referer": "https://stock.naver.com/",
        })
        res.raise_for_status()
        payload = res.json()

        # 응답 버전에 따라 priceInfos/priceInfo/data/result 아래에 배열이 들어올 수 있으므로
        # 날짜 필드(localDate)를 가진 레코드 배열을 재귀적으로 찾는다.
        candidates = []
        def _walk_chart_payload(obj, depth=0):
            if depth > 7:
                return
            if isinstance(obj, list):
                if obj and all(isinstance(x, dict) for x in obj[:min(len(obj), 8)]):
                    if any((x.get("localDate") or x.get("date")) for x in obj[:min(len(obj), 8)]):
                        candidates.append(obj)
                for child in obj[:20]:
                    _walk_chart_payload(child, depth + 1)
            elif isinstance(obj, dict):
                for child in obj.values():
                    _walk_chart_payload(child, depth + 1)
        _walk_chart_payload(payload)

        best = max(candidates, key=len) if candidates else []
        records = []
        for it in best:
            if not isinstance(it, dict):
                continue
            date_value = it.get("localDate") or it.get("date")
            try:
                date_value = pd.to_datetime(str(date_value))
                op = it.get("openPrice", it.get("open"))
                hi = it.get("highPrice", it.get("high"))
                lo = it.get("lowPrice", it.get("low"))
                cl = it.get("closePrice", it.get("close"))
                vol = it.get("accumulatedTradingVolume", it.get("volume", 0))
                if any(v is None for v in (op, hi, lo, cl)):
                    continue
                records.append({
                    "Date": date_value,
                    "Open": float(str(op).replace(",", "")),
                    "High": float(str(hi).replace(",", "")),
                    "Low": float(str(lo).replace(",", "")),
                    "Close": float(str(cl).replace(",", "")),
                    "Volume": float(str(vol or 0).replace(",", "")),
                })
            except Exception:
                continue

        if len(records) >= 15:
            df = pd.DataFrame(records).drop_duplicates(subset=["Date"]).set_index("Date").sort_index()
            df.attrs["_gaemi_raw_df"] = df.copy()
            if anchor_price and anchor_price > 0 and not df.empty:
                last_close = float(df["Close"].iloc[-1])
                if last_close > 0 and abs(last_close - anchor_price) / anchor_price > 0.3:
                    ratio = anchor_price / last_close
                    df["Open"] *= ratio
                    df["High"] *= ratio
                    df["Low"] *= ratio
                    df["Close"] *= ratio
            return df
    except Exception as exc:
        print(f"[DOMESTIC NAVER NEW CHART] {code} failed: {type(exc).__name__}: {exc}")

    # 3) Naver 공식/공개 JSON형 과거시세 fallback
    # 일부 국내 종목(특히 외국계/Reg.S 종목)은 fchart와 신형 chart endpoint 중
    # 하나가 404를 반환할 수 있다. 이 경우 api.finance.naver.com의 siseJson
    # 엔드포인트에서 동일한 일봉 OHLCV를 받아 차트를 구성한다.
    try:
        end_dt = datetime.now().strftime("%Y%m%d")
        start_dt = (datetime.now() - timedelta(days=365 * 6 + 30)).strftime("%Y%m%d")
        url = (
            "https://api.finance.naver.com/siseJson.naver"
            f"?symbol={code}&requestType=1&startTime={start_dt}&endTime={end_dt}&timeframe=day"
        )
        res = HTTP_SESSION.get(url, timeout=2.2, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/151.0 Safari/537.36",
            "Accept": "application/json,text/plain,*/*",
            "Referer": f"https://finance.naver.com/item/main.naver?code={code}",
        })
        if res.status_code == 200 and res.text.strip():
            raw = res.text.strip()
            try:
                rows = json.loads(raw)
            except Exception:
                # siseJson은 JSON처럼 보이지만 환경에 따라 JavaScript-like
                # 배열 텍스트를 반환할 수 있어 literal_eval을 보조 사용한다.
                normalized = raw
                normalized = re.sub(r'\bnull\b', 'None', normalized)
                normalized = re.sub(r'\btrue\b', 'True', normalized, flags=re.I)
                normalized = re.sub(r'\bfalse\b', 'False', normalized, flags=re.I)
                rows = ast.literal_eval(normalized)

            if isinstance(rows, list) and len(rows) >= 15:
                records = []
                for item in rows:
                    if not isinstance(item, (list, tuple)) or len(item) < 6:
                        continue
                    try:
                        # [날짜, 시가, 고가, 저가, 종가, 거래량, ...]
                        d = pd.to_datetime(str(item[0]))
                        op = float(str(item[1]).replace(",", ""))
                        hi = float(str(item[2]).replace(",", ""))
                        lo = float(str(item[3]).replace(",", ""))
                        cl = float(str(item[4]).replace(",", ""))
                        vol = float(str(item[5] or 0).replace(",", ""))
                        if not any(np.isnan(v) for v in (op, hi, lo, cl)):
                            records.append({
                                "Date": d,
                                "Open": op,
                                "High": hi,
                                "Low": lo,
                                "Close": cl,
                                "Volume": vol,
                            })
                    except Exception:
                        continue

                if len(records) >= 15:
                    df = pd.DataFrame(records).drop_duplicates(subset=["Date"]).set_index("Date").sort_index()
                    df.attrs["_gaemi_raw_df"] = df.copy()
                    if anchor_price and anchor_price > 0 and not df.empty:
                        last_close = float(df["Close"].iloc[-1])
                        if last_close > 0 and abs(last_close - anchor_price) / anchor_price > 0.3:
                            ratio = anchor_price / last_close
                            df["Open"] *= ratio
                            df["High"] *= ratio
                            df["Low"] *= ratio
                            df["Close"] *= ratio
                    return df
    except Exception as exc:
        print(f"[DOMESTIC NAVER SISEJSON] {code} failed: {type(exc).__name__}: {exc}")

    # 4) Naver trend 120일 fallback
    try:
        url = f"https://m.stock.naver.com/api/stock/{code}/trend?pageSize=120"
        res = HTTP_SESSION.get(url, timeout=2.0).json()
        if isinstance(res, list) and len(res) > 10:
            records = []
            for it in res:
                records.append({
                    "Date": pd.to_datetime(it["localTrdDd"]),
                    "Open": float(str(it["openPrice"]).replace(",", "")),
                    "High": float(str(it["highPrice"]).replace(",", "")),
                    "Low": float(str(it["lowPrice"]).replace(",", "")),
                    "Close": float(str(it["closePrice"]).replace(",", "")),
                    "Volume": float(str(it["accumulatedTradingVolume"]).replace(",", "")),
                })
            if len(records) >= 15:
                df = pd.DataFrame(records).set_index("Date").sort_index()
                df.attrs["_gaemi_raw_df"] = df.copy()
                return df
    except Exception as exc:
        print(f"[DOMESTIC NAVER TREND] {code} failed: {type(exc).__name__}: {exc}")

    # 5) Yahoo 최종 fallback (.KS/.KQ 순차 확인)
    for suffix in [".KS", ".KQ"]:
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{code}{suffix}?range=5y&interval=1d"
            res = HTTP_SESSION.get(url, timeout=2.0).json()
            result = res.get("chart", {}).get("result", [])
            if result:
                ts = result[0].get("timestamp", []) or []
                quote = result[0].get("indicators", {}).get("quote", [{}])[0] or {}
                opens = quote.get("open") or []
                highs = quote.get("high") or []
                lows = quote.get("low") or []
                closes = quote.get("close") or []
                volumes = quote.get("volume") or []
                records = []
                for i in range(len(ts)):
                    if i >= len(opens) or i >= len(highs) or i >= len(lows) or i >= len(closes):
                        continue
                    if opens[i] is None or highs[i] is None or lows[i] is None or closes[i] is None:
                        continue
                    records.append({
                        "Date": pd.to_datetime(ts[i], unit="s"),
                        "Open": float(opens[i]),
                        "High": float(highs[i]),
                        "Low": float(lows[i]),
                        "Close": float(closes[i]),
                        "Volume": float(volumes[i] or 0.0) if i < len(volumes) else 0.0,
                    })
                if len(records) >= 15:
                    df = pd.DataFrame(records).set_index("Date").sort_index()
                    df.attrs["_gaemi_raw_df"] = df.copy()
                    return df
        except Exception as exc:
            print(f"[DOMESTIC YAHOO FALLBACK] {code}{suffix} failed: {type(exc).__name__}: {exc}")

    return None

def _schedule_domestic_candle_refresh(key):
    """오래된 국내 일봉 캐시를 사용자 응답과 분리해 백그라운드에서 갱신한다."""
    key = str(key or "").strip().upper()
    if not key:
        return False
    with DOMESTIC_CANDLE_REFRESH_LOCK:
        if key in DOMESTIC_CANDLE_REFRESHING:
            return False
        DOMESTIC_CANDLE_REFRESHING.add(key)
    try:
        def _worker():
            try:
                _refresh_domestic_candles_from_naver(key)
            except Exception as exc:
                print(f"[DOMESTIC CANDLE SWR] {key} refresh failed: {type(exc).__name__}: {exc}")
            finally:
                with DOMESTIC_CANDLE_REFRESH_LOCK:
                    DOMESTIC_CANDLE_REFRESHING.discard(key)
        PRICE_DATA_EXECUTOR.submit(_worker)
        return True
    except Exception as exc:
        with DOMESTIC_CANDLE_REFRESH_LOCK:
            DOMESTIC_CANDLE_REFRESHING.discard(key)
        print(f"[DOMESTIC CANDLE SWR] {key} schedule failed: {type(exc).__name__}: {exc}")
        return False


def fetch_domestic_candles(code, anchor_price=None):
    key = str(code or "").strip().upper()
    if not key:
        return None

    now = time.time()
    cached = DOMESTIC_CANDLE_CACHE.get(key)
    if cached:
        age = now - cached.get("time", 0)
        if age < DOMESTIC_CANDLE_CACHE_TTL:
            try:
                df = cached["df"].copy(deep=True)
                if anchor_price and anchor_price > 0 and not df.empty:
                    last_close = float(df["Close"].iloc[-1])
                    if last_close > 0 and abs(last_close - anchor_price) / anchor_price > 0.3:
                        ratio = anchor_price / last_close
                        df["Open"] *= ratio
                        df["High"] *= ratio
                        df["Low"] *= ratio
                        df["Close"] *= ratio
                return df
            except Exception:
                DOMESTIC_CANDLE_CACHE.pop(key, None)
        elif age < DOMESTIC_CANDLE_STALE_TTL:
            # STEP 15 V2: stale-while-revalidate.
            # 일봉 원천 데이터가 1시간 지났다는 이유만으로 사용자 요청을 Naver 응답까지
            # 막지 않는다. 기존 1200일 데이터를 즉시 반환하고 백그라운드에서 최신화한다.
            _schedule_domestic_candle_refresh(key)
            try:
                df = cached["df"].copy(deep=True)
                if anchor_price and anchor_price > 0 and not df.empty:
                    last_close = float(df["Close"].iloc[-1])
                    if last_close > 0 and abs(last_close - anchor_price) / anchor_price > 0.3:
                        ratio = anchor_price / last_close
                        df["Open"] *= ratio
                        df["High"] *= ratio
                        df["Low"] *= ratio
                        df["Close"] *= ratio
                return df
            except Exception:
                DOMESTIC_CANDLE_CACHE.pop(key, None)
        else:
            DOMESTIC_CANDLE_CACHE.pop(key, None)

    # STEP 10: single-flight
    # /api/chart와 /api/analyze가 같은 종목을 동시에 요청하면 한쪽만
    # 실제 Naver/Yahoo 수집을 수행하고 다른 쪽은 그 결과를 기다린다.
    with DOMESTIC_CANDLE_INFLIGHT_LOCK:
        inflight = DOMESTIC_CANDLE_INFLIGHT.get(key)
        if inflight is None:
            inflight = Future()
            DOMESTIC_CANDLE_INFLIGHT[key] = inflight
            owner = True
        else:
            owner = False

    if not owner:
        try:
            raw_df = inflight.result(timeout=6.0)
            if raw_df is None:
                return None
            df = raw_df.copy(deep=True)
            if anchor_price and anchor_price > 0 and not df.empty:
                last_close = float(df["Close"].iloc[-1])
                if last_close > 0 and abs(last_close - anchor_price) / anchor_price > 0.3:
                    ratio = anchor_price / last_close
                    df["Open"] *= ratio
                    df["High"] *= ratio
                    df["Low"] *= ratio
                    df["Close"] *= ratio
            return df
        except Exception:
            return None

    try:
        raw_df = _fetch_domestic_candles_uncached(key, anchor_price=None)
        if raw_df is None or raw_df.empty:
            inflight.set_result(None)
            return None
        DOMESTIC_CANDLE_CACHE[key] = {"time": time.time(), "df": raw_df.copy(deep=True)}
        inflight.set_result(raw_df.copy(deep=True))

        df = raw_df.copy(deep=True)
        if anchor_price and anchor_price > 0:
            last_close = float(df["Close"].iloc[-1])
            if last_close > 0 and abs(last_close - anchor_price) / anchor_price > 0.3:
                ratio = anchor_price / last_close
                df["Open"] *= ratio
                df["High"] *= ratio
                df["Low"] *= ratio
                df["Close"] *= ratio
        return df
    except Exception as exc:
        try:
            inflight.set_result(None)
        except Exception:
            pass
        print(f"[DOMESTIC CANDLE] {key} fetch failed: {type(exc).__name__}: {exc}")
        return None
    finally:
        with DOMESTIC_CANDLE_INFLIGHT_LOCK:
            DOMESTIC_CANDLE_INFLIGHT.pop(key, None)

from overseas_market import fetch_overseas_quote_snapshot




# ===== 복구된 핵심 헬퍼: 최신 통합본에서 누락되어 /api/analyze 500을 일으키던 함수들 =====
def fetch_all_world_board_parallel(force=False):
    """세계시장 보드를 캐시 기반으로 조회한다.

    STEP 4: /api/analyze의 사용자 응답 경로에서는 이 함수를 호출하지 않는다.
    별도 /api/world-board에서만 갱신하며, 5분 캐시로 반복적인 10개 외부 호출을 줄인다.
    """
    global CACHED_WORLD_BOARD, CACHED_WORLD_BOARD_FETCH_TIME

    now = time.time()
    with WORLD_BOARD_CACHE_LOCK:
        if (not force and CACHED_WORLD_BOARD
                and now - CACHED_WORLD_BOARD_FETCH_TIME < WORLD_BOARD_TTL):
            return CACHED_WORLD_BOARD

    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(fetch_single_world_item, WORLD_BOARD_CONFIG))
    results = [r for r in results if r]

    if results:
        with WORLD_BOARD_CACHE_LOCK:
            CACHED_WORLD_BOARD = results
            CACHED_WORLD_BOARD_FETCH_TIME = now

    return CACHED_WORLD_BOARD or results




# ===== WHY MOVED: multi-source news clustering engine =====
from news_analysis import (NEWS_CLUSTER_STOPWORDS, NEWS_GENERIC_TOKENS, NEWS_POLARITY, _news_tokens, _news_cluster_key, _news_polarity, analyze_news_clusters, build_clustered_material_evidence)
from dart_disclosure import fetch_dart_disclosures



from movement_helpers import (MOVEMENT_THEME_KEYWORDS, POSITIVE_NEWS_KEYWORDS, NEGATIVE_NEWS_KEYWORDS, _classify_direction, _extract_news_text, _detect_macro_signals, _build_theme_breadth, _find_earnings_event, _verified_macro_event_snapshot, _movement_rate_to_float, _find_world_item, _news_sentiment_score, _detect_theme)

def analyze_global_impact(formal_name, market_name, world_board, stock_news,
                          global_news=None, dart_disclosures=None, df=None,
                          domestic_flow=None, market_index_name=None,
                          consensus=None, macro_snapshot=None, market_calendar=None,
                          symbol="", alpha_earnings=None):
    """
    글로벌/거시 → 미국 → 국내 → 업종 → 종목 전파 경로를 계산한다.
    숫자 비교가 가능한 실제 데이터만 강한 근거로 사용한다.
    확인되지 않은 경제지표 값이나 테마 동반 종목 수는 만들어내지 않는다.
    """
    layers = []

    qqq = _find_world_item(world_board, ["QQQ"])
    spy = _find_world_item(world_board, ["SPY"])
    soxx = _find_world_item(world_board, ["SOXX"])

    qqq_r = _movement_rate_to_float(qqq.get("rate")) if qqq else None
    spy_r = _movement_rate_to_float(spy.get("rate")) if spy else None
    soxx_r = _movement_rate_to_float(soxx.get("rate")) if soxx else None

    us_values = [x for x in (qqq_r, spy_r) if x is not None]
    us_avg = sum(us_values) / len(us_values) if us_values else None
    us_score = _classify_direction(us_avg)
    us_detail = f"미국 대표지수 평균 {us_avg:+.2f}%" if us_avg is not None else "미국 대표지수 데이터 확인 필요"
    layers.append({"key":"us_market","label":"미국 시장","score":us_score,"detail":us_detail})

    # 업종 연결: SOXX가 있고 반도체/AI 관련성이 실제 감지될 때만 강하게 인정.
    theme_info = _build_theme_breadth(formal_name, stock_news)
    industry_score = 0
    industry_detail = "해당 업종 직접 지표 확인 필요"
    if soxx_r is not None and theme_info["theme"] in ("반도체", "AI"):
        industry_score = _classify_direction(soxx_r)
        industry_detail = f"{theme_info['theme']} 업종 대표 ETF SOXX {soxx_r:+.2f}%"
    elif soxx_r is not None and "반도체" in _extract_news_text(stock_news).lower():
        industry_score = _classify_direction(soxx_r)
        industry_detail = f"반도체 관련 뉴스 + SOXX {soxx_r:+.2f}%"
    layers.append({"key":"industry_global","label":"미국 업종","score":industry_score,"detail":industry_detail})

    # 국내 시장
    domestic_r = None
    if market_name in ("국내주식", "국내증시"):
        ref = _find_world_item(world_board, ["코스피" if market_index_name == "KOSPI" else "코스닥"])
        if ref:
            domestic_r = _movement_rate_to_float(ref.get("rate"))
    domestic_score = _classify_direction(domestic_r)
    domestic_detail = f"{market_index_name or '국내시장'} {domestic_r:+.2f}%" if domestic_r is not None else "국내시장 데이터 확인 필요"
    layers.append({"key":"domestic_market","label":"국내 시장","score":domestic_score,"detail":domestic_detail})

    # 환율: 현재 world_board에 KRW=X가 있을 때만 사용.
    fx = _find_world_item(world_board, ["USD/KRW", "KRW=X", "원달러", "달러/원"])
    fx_r = _movement_rate_to_float(fx.get("rate")) if fx else None
    fx_score = _classify_direction(fx_r)
    fx_detail = f"원/달러 {fx_r:+.2f}%" if fx_r is not None else "원/달러 데이터 확인 필요"
    layers.append({"key":"fx","label":"환율","score":fx_score,"detail":fx_detail})

    # 거시경제: 뉴스의 단순 언급은 점수화하지 않는다.
    macro_signals = _detect_macro_signals(stock_news, global_news)
    macro_snapshot = macro_snapshot or {}
    macro_parts=[]
    for key,label in [("us10y","미국 10년물"),("us2y","미국 2년물"),("fedfunds","연방기금금리"),("cpi","CPI"),("unemployment","실업률"),("payroll","비농업고용")]:
        item=macro_snapshot.get(key)
        if item and item.get("observations"):
            obs=item["observations"][0]
            macro_parts.append(f"{label} {obs['value']:,.2f} ({obs['date']})")
    verified_macro=_verified_macro_event_snapshot(market_calendar)
    if verified_macro:
        top=verified_macro[0]
        surprise=top.get("surprise_pct")
        macro_score=2 if surprise is not None and surprise>0.5 else (-2 if surprise is not None and surprise<-0.5 else 1)
        macro_detail=f"{top['name']} 실제 {top['actual']:g} vs 예상 {top['expected']:g} ({surprise:+.1f}%)" if surprise is not None else f"{top['name']} 실제값과 예상값 확인"
    else:
        macro_score=0
        macro_detail=("실제 FRED 관측: "+" · ".join(macro_parts[:3])) if macro_parts else "발표된 실제값·예상값을 함께 확인할 수 있는 거시 이벤트 없음"
    layers.append({"key":"macro","label":"세계경제·거시환경","score":macro_score,"detail":macro_detail,"signals":macro_signals,"verified_events":verified_macro,"fred":macro_snapshot})

    # 실적: 실제 actual/estimate가 함께 있을 때만 강한 점수.
    earnings_score=0
    earnings_detail="실적 실제값과 예상값을 함께 확인할 수 없음"
    earnings_event=_find_earnings_event(formal_name,symbol,market_calendar)
    # STEP 13: 이미 optional 단계에서 수집한 실적 데이터를 재사용한다.
    # 함수 내부에서 Alpha Vantage EARNINGS를 다시 호출하지 않는다.
    if market_name == "미국주식":
        alpha_earnings = alpha_earnings if isinstance(alpha_earnings, dict) else {}
    else:
        alpha_earnings = {}
    if earnings_event:
        a=_safe_float(earnings_event.get("actual_eps")); e=_safe_float(earnings_event.get("expected_eps"))
        if (a is None or e is None) and alpha_earnings.get("quarterly"):
            latest=alpha_earnings["quarterly"][0]
            a=_safe_float(latest.get("actual_eps")); e=_safe_float(latest.get("expected_eps"))
        if a is not None and e not in (None,0):
            surprise=(a/e-1)*100
            earnings_score=2 if surprise>=10 else (1 if surprise>0 else (-2 if surprise<=-10 else -1))
            earnings_detail=f"EPS 실제 ${a:.2f} vs 예상 ${e:.2f} ({surprise:+.1f}%)"
        else:
            earnings_detail="실적 일정은 확인됐지만 실제값·예상값 동시 확인 불가"
    elif alpha_earnings.get("quarterly"):
        latest=alpha_earnings["quarterly"][0]
        a=_safe_float(latest.get("actual_eps")); e=_safe_float(latest.get("expected_eps"))
        if a is not None and e not in (None,0):
            surprise=(a/e-1)*100
            earnings_score=2 if surprise>=10 else (1 if surprise>0 else (-2 if surprise<=-10 else -1))
            earnings_detail=f"최근 EPS 실제 ${a:.2f} vs 예상 ${e:.2f} ({surprise:+.1f}%)"
    layers.append({"key":"earnings","label":"실적","score":earnings_score,"detail":earnings_detail,"event":earnings_event,"alpha_earnings":alpha_earnings})

    # 기업 이벤트/공시
    positive_disclosures = sum(1 for x in (dart_disclosures or []) if str(x.get("tag_type","")) == "positive")
    negative_disclosures = sum(1 for x in (dart_disclosures or []) if str(x.get("tag_type","")) == "negative")
    event_score = 2 if positive_disclosures > 0 else (-2 if negative_disclosures > 0 else 0)
    event_detail = (
        f"긍정 공시 {positive_disclosures}건" if positive_disclosures else
        (f"부정 공시 {negative_disclosures}건" if negative_disclosures else "직접 공시 이벤트 제한적")
    )
    layers.append({"key":"event","label":"기업 이벤트","score":event_score,"detail":event_detail})

    # 글로벌→국내 전파: 같은 방향 + 관련 업종까지 확인될 때만 강하게 인정.
    transmission_score = 0
    transmission_parts = []
    if us_score != 0 and domestic_score != 0 and (us_score > 0) == (domestic_score > 0):
        transmission_score += 2
        transmission_parts.append("미국·국내 시장 방향 일치")
    if industry_score != 0 and domestic_score != 0 and (industry_score > 0) == (domestic_score > 0):
        transmission_score += 2
        transmission_parts.append("미국 업종·국내 시장 방향 일치")
    if fx_score != 0 and domestic_score != 0:
        transmission_score += 1
        transmission_parts.append("환율 변화도 확인")
    transmission_detail = " · ".join(transmission_parts) if transmission_parts else "글로벌→국내 전파 근거 제한적"
    layers.append({"key":"transmission","label":"글로벌→국내 연결","score":transmission_score,"detail":transmission_detail})

    market_information = build_market_information(global_news)

    return {
        "layers": layers,
        "market_information": market_information,
        "total_score": sum(int(x["score"]) for x in layers),
        "theme": theme_info,
        "macro_signals": macro_signals,
        "transmission_score": transmission_score,
        "summary": " → ".join([x["detail"] for x in layers if x.get("score",0) != 0][:3]) or "확인된 글로벌 연결 근거가 제한적입니다.",
    }



from market_information import build_market_information


def analyze_movement_reason(raw_price, df, formal_name, market_name, world_board, stock_news, dart_disclosures, market_index_name=None, domestic_flow=None, global_news=None, consensus=None, macro_snapshot=None, market_calendar=None, symbol="", alpha_earnings=None):
    """실제 확보된 데이터의 강도를 비교해 가장 설득력 있는 움직임 원인을 선택한다.

    STEP 13: 호출자가 이미 확보한 Alpha Vantage 실적 데이터를 재사용한다.
    분석 함수 내부에서 동일 심볼의 EARNINGS API를 다시 호출하지 않는다.
    """
    reasons = []

    # 1. 시장
    market_score = 0
    market_detail = "시장 영향 확인 필요"
    kospi = _find_world_item(world_board, ["코스피"])
    kosdaq = _find_world_item(world_board, ["코스닥"])
    qqq = _find_world_item(world_board, ["QQQ"])
    spy = _find_world_item(world_board, ["SPY"])

    if market_name in ("국내주식", "국내증시"):
        selected_index = market_index_name if market_index_name in ("KOSPI", "KOSDAQ") else "KOSPI"
        ref = kospi if selected_index == "KOSPI" else kosdaq
        if ref:
            rate = _movement_rate_to_float(ref.get("rate"))
            if rate is not None:
                if rate >= 1.0: market_score, market_detail = 2, f"{ref.get('name')} 강세 ({rate:+.2f}%)"
                elif rate >= 0.3: market_score, market_detail = 1, f"{ref.get('name')} 상승 ({rate:+.2f}%)"
                elif rate <= -1.0: market_score, market_detail = -2, f"{ref.get('name')} 약세 ({rate:+.2f}%)"
                elif rate <= -0.3: market_score, market_detail = -1, f"{ref.get('name')} 하락 ({rate:+.2f}%)"
                else: market_detail = f"{ref.get('name')} 보합 ({rate:+.2f}%)"
    elif market_name == "미국주식":
        ref = qqq if qqq and qqq.get("rate") != "-" else spy
        if ref:
            rate = _movement_rate_to_float(ref.get("rate"))
            if rate is not None:
                if rate >= 1.0: market_score, market_detail = 2, f"{ref.get('name')} 강세 ({rate:+.2f}%)"
                elif rate >= 0.3: market_score, market_detail = 1, f"{ref.get('name')} 상승 ({rate:+.2f}%)"
                elif rate <= -1.0: market_score, market_detail = -2, f"{ref.get('name')} 약세 ({rate:+.2f}%)"
                elif rate <= -0.3: market_score, market_detail = -1, f"{ref.get('name')} 하락 ({rate:+.2f}%)"
                else: market_detail = f"{ref.get('name')} 보합 ({rate:+.2f}%)"
    elif market_name == "암호화폐":
        if df is not None and len(df) >= 2:
            try:
                prev_close = float(df["Close"].iloc[-2])
                current_close = float(df["Close"].iloc[-1])
                rate = ((current_close / prev_close) - 1.0) * 100.0 if prev_close else 0.0
                label = formal_name or symbol or "코인"
                if rate >= 1.0: market_score, market_detail = 2, f"{label} 시장 강세 ({rate:+.2f}%)"
                elif rate >= 0.3: market_score, market_detail = 1, f"{label} 시장 상승 ({rate:+.2f}%)"
                elif rate <= -1.0: market_score, market_detail = -2, f"{label} 시장 약세 ({rate:+.2f}%)"
                elif rate <= -0.3: market_score, market_detail = -1, f"{label} 시장 하락 ({rate:+.2f}%)"
                else: market_detail = f"{label} 시장 보합 ({rate:+.2f}%)"
            except Exception:
                pass
    else:
        market_detail = "시장 공통 요인 영향 낮음"

    reasons.append({"key":"market","label":"시장","score":market_score,"detail":market_detail})

    # 2. 업종/테마
    theme, theme_hits = _detect_theme(formal_name, stock_news)
    theme_score = 0
    theme_detail = "뚜렷한 테마 근거 없음"
    if theme:
        sector_rate = None
        if theme in ("반도체", "AI"):
            soxx = _find_world_item(world_board, ["SOXX"])
            if soxx:
                sector_rate = _movement_rate_to_float(soxx.get("rate"))
        if sector_rate is not None:
            if sector_rate >= 1.0: theme_score, theme_detail = 2, f"{theme} 업종 강세 · SOXX {sector_rate:+.2f}%"
            elif sector_rate >= 0.3: theme_score, theme_detail = 1, f"{theme} 업종 동반 상승 · SOXX {sector_rate:+.2f}%"
            elif sector_rate <= -1.0: theme_score, theme_detail = -2, f"{theme} 업종 약세 · SOXX {sector_rate:+.2f}%"
            elif sector_rate <= -0.3: theme_score, theme_detail = -1, f"{theme} 업종 약세 · SOXX {sector_rate:+.2f}%"
            else: theme_detail = f"{theme} 테마 감지 · 업종 움직임 제한적"
        else:
            # 단순 키워드 감지는 약한 증거로만 취급한다.
            theme_score = 1 if theme_hits >= 2 else 0
            theme_detail = f"{theme} 관련 키워드 {theme_hits}건 감지"

    reasons.append({"key":"theme","label":"업종·테마","score":theme_score,"detail":theme_detail,"theme_hits":theme_hits})

    # 3. 기업 재료
    # 여러 뉴스채널에서 반복되는 공통 주제를 먼저 찾고, 단일 기사만으로는 강한 재료로 인정하지 않는다.
    clustered_material = build_clustered_material_evidence(stock_news, formal_name, symbol)
    news_evidence = []
    for n in clustered_material.get("evidence", []):
        pol = _news_polarity(n)
        news_evidence.append({
            "title": str(n.get("title", "")),
            "type": "positive" if pol > 0 else "negative" if pol < 0 else "neutral",
            "keywords": _news_tokens(str(n.get("title", "")))[:5],
            "media": str(n.get("media") or n.get("source") or "")
        })

    disclosure_positive = 0
    disclosure_negative = 0
    for item in (dart_disclosures or []):
        tag_type = str(item.get("tag_type", ""))
        if tag_type == "positive": disclosure_positive += 1
        elif tag_type == "negative": disclosure_negative += 1

    material_score = int(clustered_material.get("score", 0))
    # 공시는 독립적인 1차 근거이므로 뉴스 클러스터가 없을 때도 보조한다.
    if material_score == 0 and disclosure_positive > disclosure_negative:
        material_score = 1
    elif material_score == 0 and disclosure_negative > disclosure_positive:
        material_score = -1
    material_detail = clustered_material.get("detail", "반복적으로 확인되는 핵심 뉴스 재료 없음")
    if disclosure_positive or disclosure_negative:
        material_detail += f" · 공시 호재 {disclosure_positive}건/악재 {disclosure_negative}건"

    reasons.append({"key":"material","label":"기업 재료","score":material_score,"detail":material_detail,"news_clusters":clustered_material.get("clusters",[])})

    # 4. 거래량
    flow_score = 0
    flow_detail = "거래량 변화 제한적"
    price_change = None
    volume_ratio = None
    try:
        if df is not None and len(df) >= 6:
            close = df["Close"].astype(float)
            volume = df["Volume"].astype(float)
            current_close = float(close.iloc[-1])
            previous_close = float(close.iloc[-2])
            current_volume = float(volume.iloc[-1])
            avg_volume = float(volume.tail(6).iloc[:-1].mean())
            price_change = ((current_close - previous_close) / previous_close) * 100 if previous_close else 0
            volume_ratio = (current_volume / avg_volume) * 100 if avg_volume > 0 else 100
            if price_change >= 1.0 and volume_ratio >= 150:
                flow_score, flow_detail = 2, f"상승 +{price_change:.2f}% + 거래량 {volume_ratio:.0f}%"
            elif price_change >= 0.3 and volume_ratio >= 120:
                flow_score, flow_detail = 1, f"상승 +{price_change:.2f}% · 거래량 {volume_ratio:.0f}%"
            elif price_change <= -1.0 and volume_ratio >= 150:
                flow_score, flow_detail = -2, f"하락 {price_change:.2f}% + 거래량 {volume_ratio:.0f}%"
            elif price_change <= -0.3 and volume_ratio >= 120:
                flow_score, flow_detail = -1, f"하락 {price_change:.2f}% · 거래량 {volume_ratio:.0f}%"
            else:
                flow_detail = f"거래량 {volume_ratio:.0f}%"
    except Exception:
        pass
    reasons.append({"key":"volume","label":"거래량","score":flow_score,"detail":flow_detail})

    # 5. 거래대금
    tv_score = 0
    tv_detail = domestic_flow.get("trading_value_detail", "거래대금 데이터 확인 필요") if domestic_flow else "거래대금 데이터 확인 필요"
    tv_ratio = None
    try:
        if domestic_flow and domestic_flow.get("trading_value") is not None and df is not None and len(df) >= 21:
            tv = float(domestic_flow["trading_value"])
            close_s = df["Close"].astype(float)
            vol_s = df["Volume"].astype(float)
            proxy = close_s * vol_s
            avg_proxy = float(proxy.tail(21).iloc[:-1].mean())
            tv_ratio = (tv / avg_proxy) if avg_proxy > 0 else 0.0
            # 거래대금은 실제 급증 + 가격 방향이 같이 있을 때 강한 원인으로 인정.
            if tv_ratio >= 2.0 and flow_score > 0: tv_score = 2
            elif tv_ratio >= 1.2 and flow_score > 0: tv_score = 1
            elif tv_ratio >= 2.0 and flow_score < 0: tv_score = -2
            elif tv_ratio >= 1.2 and flow_score < 0: tv_score = -1
            tv_detail += f" · 평소 대비 약 {tv_ratio*100:.0f}%" if tv_ratio > 0 else ""
    except Exception:
        pass
    reasons.append({"key":"trading_value","label":"거래대금","score":tv_score,"detail":tv_detail})

    # 6. 외국인 수급
    foreign_score = 0
    foreign_detail = domestic_flow.get("foreigner_detail", "외국인 수급 데이터 확인 필요") if domestic_flow else "외국인 수급 데이터 확인 필요"
    if domestic_flow and domestic_flow.get("foreigner_5d") is not None:
        try:
            v = float(domestic_flow["foreigner_5d"])
            latest = domestic_flow.get("foreigner_latest")
            foreign_score = 2 if v > 0 and latest is not None and float(latest) > 0 else (1 if v > 0 else (-2 if v < 0 and latest is not None and float(latest) < 0 else (-1 if v < 0 else 0)))
        except Exception:
            pass
    reasons.append({"key":"foreign","label":"외국인 수급","score":foreign_score,"detail":foreign_detail})

    # 7. 기관 수급
    institution_score = 0
    institution_detail = domestic_flow.get("institution_detail", "기관 수급 데이터 확인 필요") if domestic_flow else "기관 수급 데이터 확인 필요"
    if domestic_flow and domestic_flow.get("institution_5d") is not None:
        try:
            v = float(domestic_flow["institution_5d"])
            latest = domestic_flow.get("institution_latest")
            institution_score = 2 if v > 0 and latest is not None and float(latest) > 0 else (1 if v > 0 else (-2 if v < 0 and latest is not None and float(latest) < 0 else (-1 if v < 0 else 0)))
        except Exception:
            pass
    reasons.append({"key":"institution","label":"기관 수급","score":institution_score,"detail":institution_detail})

    # 8. 최근 5거래일 시장 흐름
    market5 = None
    if market_name in ("국내주식", "국내증시") and market_index_name in ("KOSPI", "KOSDAQ"):
        ref5 = _find_world_item(world_board, ["코스피" if market_index_name == "KOSPI" else "코스닥"])
        market5 = fetch_naver_index_5day_returns(market_index_name, ref5)
    elif market_name == "미국주식":
        ref5 = _find_world_item(world_board, ["QQQ"])
        market5 = fetch_market_5day_returns_from_symbol("QQQ", "NASDAQ", ref5)
    elif market_name == "암호화폐":
        market5 = fetch_market_5day_returns_from_df(df, formal_name or symbol or "Crypto")

    if market5:
        market5_score = int(market5["score"])
        market5_detail = market5["detail"]
        market5_points = market5["points"]
    else:
        market5_score = 0
        selected_index = market_index_name if market_index_name in ("KOSPI", "KOSDAQ") else ("NASDAQ" if market_name == "미국주식" else (formal_name or symbol or "시장"))
        market5_detail = f"{selected_index} 최근 5거래일 데이터 확인 필요"
        market5_points = []

    reasons.append({
        "key":"market_5d","label":"시장 5일 흐름","score":market5_score,
        "detail":market5_detail,"chart_points":market5_points,
        "total_return":market5.get("total_return") if market5 else None,
        "source":market5.get("source") if market5 else None,
        "index_name":market5.get("index_name") if market5 else selected_index
    })

    # ----------------------------------------------------
    # 핵심: "먼저 걸리는 조건"이 아니라 실제 강도를 비교한다.
    # 상승/하락 방향을 가격 움직임과 맞춰 원인 후보를 평가한다.
    # ----------------------------------------------------
    move_direction = 1
    try:
        if price_change is not None and price_change < 0:
            move_direction = -1
    except Exception:
        pass

    positive_count = sum(1 for x in reasons if x["score"] > 0)
    negative_count = sum(1 for x in reasons if x["score"] < 0)
    evidence_count = sum(1 for x in reasons if x["score"] != 0)
    total_score = sum(int(x["score"]) for x in reasons)

    # 실제 원인 선택용 보너스.
    candidate_scores = {}
    for r in reasons:
        s = int(r["score"])
        # 현재 주가 방향과 반대인 신호는 "원인"으로 선택하지 않도록 감점.
        directional = s * move_direction
        candidate_scores[r["key"]] = directional

    # 강한 근거가 있는 항목에만 보너스를 주되, 단순 키워드 테마는 과대평가하지 않는다.
    if material_score > 0:
        candidate_scores["material"] += 1 if material_score >= 2 else 0
    if foreign_score > 0 and institution_score > 0:
        candidate_scores["foreign"] += 1
        candidate_scores["institution"] += 1
    if tv_score > 0 and flow_score > 0:
        candidate_scores["trading_value"] += 1
    if theme_score > 0 and theme_hits >= 2:
        candidate_scores["theme"] += 1

    # 실제 상승/하락을 설명할 수 있는 양(+)의 후보만 선택.
    positive_candidates = [(k,v) for k,v in candidate_scores.items() if v > 0]
    if positive_candidates:
        max_candidate = max(v for _, v in positive_candidates)
        top_keys = [k for k,v in positive_candidates if v == max_candidate]
    else:
        top_keys = []

    # 재료 > 양매수 수급 > 거래대금/거래량 > 테마 > 시장 순의 tie-break.
    priority = {
        "material": 7,
        "foreign": 6,
        "institution": 6,
        "trading_value": 5,
        "volume": 4,
        "theme": 3,
        "market": 2,
        "market_5d": 1,
    }
    main_key = max(top_keys, key=lambda k: priority.get(k,0)) if top_keys else None

    # 양매수 수급은 하나의 원인으로 묶는다.
    if foreign_score > 0 and institution_score > 0 and main_key in ("foreign","institution"):
        main_key = "flow_pair"

    # 재료가 약하고 거래가 매우 강하면 거래대금/거래량을 우선.
    if main_key == "material" and material_score == 1:
        if tv_score >= 2:
            main_key = "trading_value"
        elif flow_score >= 2:
            main_key = "volume"

    main_label_map = {
        "material":"기업 재료","flow_pair":"외국인·기관 수급",
        "foreign":"외국인 수급","institution":"기관 수급",
        "trading_value":"거래대금","volume":"거래량",
        "theme":"업종·테마","market":"시장","market_5d":"시장 5일 흐름"
    }
    main_reason = main_label_map.get(main_key, "-")

    # 상승/하락 방향은 전체 점수로 판단하되, 혼조이면 실제 가격 방향을 사용한다.
    direction = "상승" if total_score >= 4 else ("하락" if total_score <= -4 else ("상승" if move_direction > 0 and raw_price is not None else "혼조"))
    confidence = "높음" if evidence_count >= 4 else ("중간" if evidence_count >= 2 else "낮음")

    if main_reason != "-":
        if direction == "상승":
            summary = f"{main_reason} 신호가 가장 강하게 잡혔고, 실제 움직임과 함께 확인됩니다."
        elif direction == "하락":
            summary = f"{main_reason} 쪽 약세 신호가 가장 강하게 잡혔고, 실제 움직임과 함께 확인됩니다."
        else:
            summary = f"{main_reason} 신호가 가장 눈에 띄지만 다른 근거와 엇갈리는 부분도 있습니다."
    else:
        summary = "현재 확보된 데이터만으로는 특정 원인을 우선순위 높게 선택하기 어렵습니다."

    global_impact = analyze_global_impact(
        formal_name=formal_name,
        market_name=market_name,
        world_board=world_board,
        stock_news=stock_news,
        global_news=global_news,
        dart_disclosures=dart_disclosures,
        df=df,
        domestic_flow=domestic_flow,
        market_index_name=market_index_name,
        consensus=consensus,
        macro_snapshot=macro_snapshot,
        market_calendar=market_calendar,
        symbol=symbol,
        alpha_earnings=alpha_earnings
    )

    # 직접 공시/재료가 없고 글로벌 전파가 충분히 강할 때만 글로벌 원인을 보조 주원인으로 선택.
    if global_impact["total_score"] >= 5 and global_impact.get("transmission_score", 0) >= 2 and main_key in (None, "market", "market_5d", "theme"):
        main_key = "global_impact"
        main_reason = "글로벌·국내 연결"
        summary = f"{global_impact['transmission_score']}단계 연결 근거가 확인됩니다."

    return {
        "direction": direction,
        "confidence": confidence,
        "total_score": total_score,
        "main_reason": main_reason,
        "main_reason_key": main_key,
        "global_impact": global_impact,
        "summary": summary,
        "reasons": reasons,
        "news_evidence": news_evidence[:5]
    }

# ----------------------------------------------------
# 6. 전체 통합 파이프라인
# ----------------------------------------------------
# STEP 13: 분석 함수 간 데이터 재사용
# optional 단계에서 수집한 Alpha Vantage 실적 데이터를 movement 분석에
# 그대로 전달해 동일 종목의 EARNINGS 재호출을 제거한다.
def _safe_future_result(future, default, label, timeout=None):
    """외부 API 하나가 실패해도 전체 /api/analyze가 500으로 끝나지 않게 한다."""
    if future is None:
        return default
    try:
        if timeout is None:
            return future.result()
        return future.result(timeout=max(0.0, float(timeout)))
    except Exception as exc:
        print(f"[OPTIONAL API] {label} failed/timeout: {type(exc).__name__}: {exc}")
        return default


def _collect_optional_results(futures, timeout=OPTIONAL_API_SOFT_TIMEOUT):
    """Optional API들을 한 번에 시작하고, 전체 대기시간만 제한한다.

    timeout을 넘긴 future는 취소하지 않는다. 해당 작업은 worker에서 계속 실행되어
    각 함수의 공용 캐시를 채우고, 다음 사용자 요청은 캐시 결과를 즉시 사용한다.
    """
    if not futures:
        return {}
    deadline = time.monotonic() + max(0.0, float(timeout))
    pending = set(f for f in futures.values() if f is not None)
    results = {}
    while pending:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        done, pending = wait(pending, timeout=remaining)
        for future in done:
            for name, candidate in futures.items():
                if candidate is future:
                    try:
                        results[name] = future.result()
                    except Exception as exc:
                        print(f"[OPTIONAL API] {name} failed: {type(exc).__name__}: {exc}")
                        results[name] = None
                    break
    for name, future in futures.items():
        if name in results:
            continue
        results[name] = None
        if future is not None and not future.done():
            print(f"[OPTIONAL API] {name} background refresh continues")
    return results


def _schedule_analysis_enrichment(clean_q, base_result, context):
    """핵심 응답 이후 선택 외부 데이터를 백그라운드에서 채워 캐시를 완성한다."""
    with ANALYSIS_ENRICH_LOCK:
        if clean_q in ANALYSIS_ENRICHING:
            return
        ANALYSIS_ENRICHING.add(clean_q)

    def worker():
        try:
            market_name = context["market_name"]
            symbol = context["symbol"]
            formal_name = context["formal_name"]
            raw_price = context["raw_price"]
            is_krw = context["is_krw"]
            market_index_name = context.get("market_index_name")
            if market_index_name is None and market_name == "국내주식":
                market_index_name = _get_cached_domestic_market_type(symbol)
                if market_index_name is None:
                    market_index_name = _refresh_domestic_market_type(symbol)
            df = context["df"]

            optional_futures = {
                "stock_news_rss": _perf_submit(OPTIONAL_API_EXECUTOR, fetch_google_stock_news, symbol, market_name, formal_name, 20),
                "global_news_rss": _perf_submit(OPTIONAL_API_EXECUTOR, fetch_google_economic_news, market_name, 20),
                "api_news": None,
                "macro_snapshot": _perf_submit(OPTIONAL_API_EXECUTOR, fetch_fred_macro_snapshot),
                "consensus": _perf_submit(OPTIONAL_API_EXECUTOR, fetch_market_consensus_unified, symbol, market_name, raw_price, formal_name),
                "alpha_earnings": (_perf_submit(OPTIONAL_API_EXECUTOR, fetch_alpha_vantage_earnings, symbol) if market_name == "미국주식" and symbol else None),
                "dart_disclosures": _perf_submit(OPTIONAL_API_EXECUTOR, fetch_dart_disclosures, symbol, is_krw, 5),
                "domestic_flow": (_perf_submit(OPTIONAL_API_EXECUTOR, fetch_naver_domestic_flow_metrics, symbol) if is_krw and market_name == "국내주식" else None),
                "ticker_news": None,
            }

            # details 단계에서는 모든 외부 API를 무한정 기다리지 않는다.
            # 느린 API 하나 때문에 뉴스/컨센서스/트랙 전체가 영원히 '불러오는 중'으로
            # 남는 문제를 막기 위해 최대 10초까지만 기다리고, 준비된 결과부터 사용한다.
            optional_results = {}
            active_futures = {name: future for name, future in optional_futures.items() if future is not None}
            if active_futures:
                done, pending = wait(
                    set(active_futures.values()),
                    timeout=ANALYSIS_ENRICH_WAIT,
                )
                for name, future in active_futures.items():
                    if future not in done:
                        optional_results[name] = None
                        print(f"[ANALYSIS ENRICH] {name} still running after {ANALYSIS_ENRICH_WAIT:.1f}s")
                        continue
                    try:
                        optional_results[name] = future.result()
                    except Exception as exc:
                        print(f"[ANALYSIS ENRICH] {name} failed: {type(exc).__name__}: {exc}")
                        optional_results[name] = None
            for name, future in optional_futures.items():
                if future is None:
                    optional_results[name] = None

            stock_news = merge_news_sources(optional_results.get("stock_news_rss") or [], optional_results.get("ticker_news") or [])
            global_news = merge_news_sources(optional_results.get("global_news_rss") or [], optional_results.get("api_news") or [])
            macro_snapshot = optional_results.get("macro_snapshot") or {}
            consensus = optional_results.get("consensus")
            if consensus is None:
                consensus = base_result.get("consensus") or {
                    "title": "애널리스트 컨센서스",
                    "gauge_val": "-",
                    "gauge_label": "확인 의견",
                    "status_label": "데이터 확인 필요",
                    "badge_text": "실제 데이터만 표시",
                    "dots": "실제 컨센서스 데이터를 확인할 수 없습니다.",
                    "primary_label": "평균 목표가",
                    "primary_val": "실제 목표가 데이터 없음",
                    "secondary_label": "현재가",
                    "secondary_val": f"{raw_price:,.2f}" if raw_price else "-",
                    "slider_left": f"{raw_price:,.2f}" if raw_price else "-",
                    "slider_mid": "실제 목표가 확인 필요",
                    "slider_right": "-",
                    "fill_pct": 50,
                    "list_title": "실제 애널리스트 의견",
                    "source_tag": "실제 컨센서스 데이터 확인 필요",
                    "items": []
                }
            alpha_earnings = optional_results.get("alpha_earnings") or {}
            dart_disclosures = optional_results.get("dart_disclosures") or []
            domestic_flow = optional_results.get("domestic_flow")
            world_board = CACHED_WORLD_BOARD or []
            market_calendar = CACHED_MARKET_CALENDAR or []

            try:
                movement_reason = analyze_movement_reason(
                    raw_price=raw_price, df=df, formal_name=formal_name, market_name=market_name,
                    world_board=world_board, stock_news=stock_news, dart_disclosures=dart_disclosures,
                    market_index_name=market_index_name, domestic_flow=domestic_flow, global_news=global_news,
                    consensus=consensus, macro_snapshot=macro_snapshot, market_calendar=market_calendar, symbol=symbol,
                    alpha_earnings=alpha_earnings
                )
            except Exception as exc:
                print("[ANALYSIS ENRICH][MOVEMENT] 오류:", repr(exc))
                movement_reason = base_result.get("movement_reason")

            enriched = dict(base_result)
            enriched.update({
                "stock_news": stock_news, "global_news": global_news, "macro_snapshot": macro_snapshot,
                "consensus": consensus, "dart_disclosures": dart_disclosures, "domestic_flow": domestic_flow,
                "movement_reason": movement_reason, "world_board": world_board, "market_calendar": market_calendar,
                "_details_ready": True,
            })
            _set_analysis_cache(clean_q, enriched, time.time())
            with ANALYSIS_ENRICH_LOCK:
                ANALYSIS_ENRICH_CONTEXT.pop(clean_q, None)
            print(f"[ANALYSIS ENRICH] {clean_q} details ready")
        except Exception as exc:
            print(f"[ANALYSIS ENRICH] {clean_q} failed: {type(exc).__name__}: {exc}")
        finally:
            with ANALYSIS_ENRICH_LOCK:
                ANALYSIS_ENRICHING.discard(clean_q)

    # 재검색/새로고침으로 같은 core 캐시를 다시 읽더라도
    # 이미 시작된 enrichment 작업의 실행 맥락을 잃지 않도록 보관한다.
    with ANALYSIS_ENRICH_LOCK:
        ANALYSIS_ENRICH_CONTEXT[clean_q] = context
    ANALYSIS_ENRICH_EXECUTOR.submit(worker)


def _analysis_cache_keys(query, data=None):
    """같은 종목의 이름/코드/티커가 하나의 분석 캐시를 공유하도록 키를 만든다."""
    keys = []
    def add(value):
        v = str(value or "").strip().replace(" ", "").upper()
        if v and v not in keys:
            keys.append(v)
    add(query)
    if isinstance(data, dict):
        add(data.get("symbol"))
        add(data.get("name"))
        add(data.get("formal_name"))
    return keys

def _set_analysis_cache(query, data, timestamp=None):
    ts = time.time() if timestamp is None else timestamp
    for key in _analysis_cache_keys(query, data):
        ANALYSIS_CACHE[key] = (ts, data)

def run_full_pipeline(query, lightweight=False):
    global ANALYSIS_CACHE, CACHED_WORLD_BOARD, CACHED_MARKET_CALENDAR
    clean_q = query.strip().replace(" ", "").upper()
    now_ts = time.time()

    _perf_stage("cache_check")

    # 이름/코드/티커 중 어느 것으로 다시 들어와도 동일한 분석 캐시를 찾는다.
    cached_key = None
    cached_time = None
    cached_data = None
    for candidate in _analysis_cache_keys(clean_q):
        hit = ANALYSIS_CACHE.get(candidate)
        if hit:
            hit_time, hit_data = hit
            if now_ts - hit_time < ANALYSIS_CORE_TTL:
                cached_key, cached_time, cached_data = candidate, hit_time, hit_data
                break

    if cached_data is not None:
        # STEP 4: 세계시장 보드/시장 캘린더는 별도 API가 담당한다.
        if CACHED_WORLD_BOARD:
            cached_data["world_board"] = CACHED_WORLD_BOARD
        if CACHED_MARKET_CALENDAR:
            cached_data["market_calendar"] = CACHED_MARKET_CALENDAR

        # core 캐시가 아직 enrichment 전이면 실제 작업 컨텍스트를 어떤 별칭으로도 찾는다.
        if lightweight and not cached_data.get("_details_ready"):
            enrich_context = None
            enriching = False
            for candidate in _analysis_cache_keys(clean_q, cached_data):
                with ANALYSIS_ENRICH_LOCK:
                    if candidate in ANALYSIS_ENRICHING:
                        enriching = True
                    if enrich_context is None:
                        enrich_context = ANALYSIS_ENRICH_CONTEXT.get(candidate)
                if enriching or enrich_context:
                    break
            if (not enriching) and enrich_context:
                _schedule_analysis_enrichment(cached_key or clean_q, cached_data, enrich_context)
        # 캐시를 찾은 별칭과 실제 symbol/name 모두 같은 객체를 가리키게 한다.
        _set_analysis_cache(clean_q, cached_data, cached_time)
        return cached_data

    market_name = "국내주식"
    market_index_name = None
    is_krw = True

    if clean_q in GLOBAL_INDEX_MAP:
        symbol = GLOBAL_INDEX_MAP[clean_q]
        formal_name = query.strip()

        # 환율/글로벌 지수 조회 실패가 전체 검색을 멈추게 하지 않는다.
        try:
            curr_p, price_str, df = fetch_overseas_candles(symbol)
        except Exception as exc:
            print(f"[GLOBAL INDEX] {symbol} fetch failed: {exc}")
            curr_p, price_str, df = None, "-", None

        if symbol in ["^KS11", "^KQ11"]:
            market_name = "국내증시"
            market_index_name = "KOSPI" if symbol == "^KS11" else "KOSDAQ"
            is_krw = True
            price_str = f"{curr_p:,.2f} pt" if curr_p else "-"
        elif symbol == "KRW=X":
            market_name = "외환"
            market_index_name = None
            is_krw = False
            price_str = f"{curr_p:,.1f} 원" if curr_p else "-"
        elif symbol in ["CL=F", "GC=F"]:
            market_name = "원자재"
            market_index_name = None
            is_krw = False
            price_str = f"${curr_p:,.2f}" if curr_p else "-"
        else:
            market_name = "글로벌증시"
            market_index_name = None
            is_krw = False
            price_str = f"${curr_p:,.2f}" if curr_p else "-"
        raw_price = curr_p

    elif clean_q.endswith("USDT") or clean_q in CRYPTO_MAP or any(c in clean_q for c in ["BTC", "ETH", "코인", "비트"]):
        symbol = CRYPTO_MAP.get(clean_q, clean_q if clean_q.endswith("USDT") else f"{clean_q}USDT")
        formal_name = clean_q
        raw_price, price_str, df = fetch_crypto_candles(symbol)
        market_name = "암호화폐"
        is_krw = False

    elif clean_q in US_STOCK_MAP:
        symbol = US_STOCK_MAP[clean_q]
        formal_name = query.strip()
        # 차트와 실시간 시세는 서로 의존하지 않으므로 동시에 조회한다.
        with ThreadPoolExecutor(max_workers=2) as price_executor:
            chart_future = _perf_submit(price_executor, fetch_overseas_candles, symbol)
            quote_future = _perf_submit(price_executor, fetch_overseas_quote_snapshot, symbol)
            chart_price, _, df = chart_future.result()
            quote_snapshot = quote_future.result()

        if isinstance(quote_snapshot, dict) and quote_snapshot.get("price") is not None:
            try:
                raw_price = float(quote_snapshot.get("price"))
            except (TypeError, ValueError):
                raw_price = chart_price
            price_str = f"{raw_price:,.2f} 달러" if raw_price is not None else None
        else:
            raw_price = chart_price
            price_str = f"{raw_price:,.2f} 달러" if raw_price is not None else None
        market_name = "미국주식"
        is_krw = False
    # STEP 11: 영문 티커는 대소문자와 관계없이 미국주식 경로로 처리한다.
    # 기존에는 소문자 티커(예: xom, nvda)가 US_STOCK_MAP에 없으면
    # 국내주식 분기로 떨어져 Naver autoComplete를 호출했다.
    # 그 결과 XOM 분석에서도 약 480ms가 불필요하게 발생했다.
    # 1~6자리 영문 입력은 Naver 검색을 거치지 않고 Yahoo 경로로 바로 보낸다.
    elif US_TICKER_PATTERN.fullmatch(clean_q or ""):
        symbol = clean_q
        formal_name = query.strip() or symbol
        with ThreadPoolExecutor(max_workers=2) as price_executor:
            chart_future = _perf_submit(price_executor, fetch_overseas_candles, symbol)
            quote_future = _perf_submit(price_executor, fetch_overseas_quote_snapshot, symbol)
            chart_price, _, df = chart_future.result()
            quote_snapshot = quote_future.result()

        if isinstance(quote_snapshot, dict) and quote_snapshot.get("price") is not None:
            try:
                raw_price = float(quote_snapshot.get("price"))
            except (TypeError, ValueError):
                raw_price = chart_price
            price_str = f"{raw_price:,.2f} 달러" if raw_price is not None else None
        else:
            raw_price = chart_price
            price_str = f"{raw_price:,.2f} 달러" if raw_price is not None else None
        market_name = "미국주식"
        is_krw = False
    elif any(c.isupper() for c in query) and not any('\uac00' <= char <= '\ud7a3' for char in query):
        symbol = query.upper().strip()
        formal_name = symbol
        # 차트와 실시간 시세를 병렬 조회한다.
        with ThreadPoolExecutor(max_workers=2) as price_executor:
            chart_future = _perf_submit(price_executor, fetch_overseas_candles, symbol)
            quote_future = _perf_submit(price_executor, fetch_overseas_quote_snapshot, symbol)
            chart_price, _, df = chart_future.result()
            quote_snapshot = quote_future.result()

        if quote_snapshot:
            raw_price = quote_snapshot["price"]
            price_str = f"{raw_price:,.2f} 달러"
        else:
            raw_price = chart_price
            price_str = f"{raw_price:,.2f} 달러" if raw_price is not None else None
        market_name = "미국주식"
        is_krw = False

    else:
        code, name = find_stock_code(query)
        if not code:
            return None
        symbol, formal_name = code, name

        # STEP 9: 국내주식 가격/차트 관련 외부 호출을 병렬화한다.
        # 기존에는 거래소 구분 → 실시간 가격 → 1200일 차트가 순차 실행되어
        # 각 HTTP 지연시간이 합산됐다. 이제 세 작업을 동시에 시작하고 가장
        # 늦게 끝나는 작업만 기다린다. 기존 데이터/기능은 그대로 유지한다.
        # STEP 10: KOSPI/KOSDAQ 구분은 첫 화면의 가격/차트 계산에 필수적이지 않다.
        # 캐시가 있으면 즉시 사용하고, 없으면 core 응답을 막지 않고 백그라운드에서 갱신한다.
        market_index_name = _get_cached_domestic_market_type(symbol)
        if market_index_name is None and not lightweight:
            market_index_name = _perf_submit(PRICE_DATA_EXECUTOR, _refresh_domestic_market_type, symbol).result()
        elif market_index_name is None:
            _perf_submit(OPTIONAL_API_EXECUTOR, _refresh_domestic_market_type, symbol)

        realtime_future = _perf_submit(PRICE_DATA_EXECUTOR, get_naver_realtime_price, symbol)
        candle_future = _perf_submit(PRICE_DATA_EXECUTOR, fetch_domestic_candles, symbol, None)

        raw_price, price_str = realtime_future.result()
        df = candle_future.result()

        # 실시간 가격이 없을 때만 차트 종가를 현재가 대체값으로 사용한다.
        # anchor_price는 기존 함수 계약상 반환 후 보정이 필요할 수 있으므로
        # 실시간 가격을 확보한 경우에만 기존과 동일하게 차트 데이터를 맞춘다.
        if raw_price is None and df is not None and not df.empty:
            raw_price = float(df['Close'].iloc[-1])
            price_str = f"{int(raw_price):,} 원"
        elif raw_price is not None and df is not None and not df.empty:
            last_close = float(df['Close'].iloc[-1])
            if last_close > 0 and abs(last_close - raw_price) / raw_price > 0.3:
                ratio = raw_price / last_close
                df = df.copy(deep=True)
                df['Open'] *= ratio
                df['High'] *= ratio
                df['Low'] *= ratio
                df['Close'] *= ratio

    _perf_stage("price_chart")

    if raw_price is None or df is None:
        return None

    # ====================================================
    # STEP 8: /api/analyze 경량화
    # core 모드에서는 선택 외부 데이터를 기다리지 않는다. 완료 작업은 백그라운드에서
    # 캐시를 채우고, /api/analyze-details가 준비 여부를 알려준다.
    # ====================================================
    # STEP 8 FIX: core 모드에서도 아래 공통 처리부가 참조하므로
    # optional_results를 항상 초기화한다. 기존 코드에서는 core 모드일 때
    # optional_results가 정의되지 않아 UnboundLocalError가 발생했다.
    optional_results = {}
    if lightweight:
        stock_news = []
        global_news = []
        api_news = []
        macro_snapshot = {}
        ticker_news = []
        # STEP 19: STEP 4 원본에서 존재하던 애널리스트 컨센서스 카드가
        # 경량(core) 응답 단계에서 사라지지 않도록 즉시 표시용 최소 객체를 유지한다.
        # 실제 목표가/의견 데이터는 기존 백그라운드 enrichment가 완료되면 교체한다.
        consensus = {
            "title": "애널리스트 컨센서스",
            "gauge_val": "-",
            "gauge_label": "확인 의견",
            "status_label": "데이터 불러오는 중",
            "badge_text": "실제 데이터만 표시",
            "dots": "실제 컨센서스 데이터를 확인하고 있습니다.",
            "primary_label": "평균 목표가",
            "primary_val": "확인 중…",
            "secondary_label": "현재가",
            "secondary_val": price_str if 'price_str' in locals() else (f"{raw_price:,.2f}" if raw_price else "-"),
            "slider_left": "-",
            "slider_mid": "실제 목표가 확인 중",
            "slider_right": "-",
            "fill_pct": 50,
            "list_title": "실제 애널리스트 의견",
            "source_tag": "실제 데이터 조회 중",
            "items": []
        }
        alpha_earnings = {}
        dart_disclosures = []
        domestic_flow = None
        _perf_stage("optional_external_skipped")
    else:
        optional_futures = {
            "stock_news_rss": _perf_submit(OPTIONAL_API_EXECUTOR, fetch_google_stock_news, symbol, market_name, formal_name, 20),
            "global_news_rss": _perf_submit(OPTIONAL_API_EXECUTOR, fetch_google_economic_news, market_name, 20),
            "api_news": None,
            "macro_snapshot": _perf_submit(OPTIONAL_API_EXECUTOR, fetch_fred_macro_snapshot),
            "consensus": _perf_submit(OPTIONAL_API_EXECUTOR, fetch_market_consensus_unified, symbol, market_name, raw_price, formal_name),
            "alpha_earnings": (_perf_submit(OPTIONAL_API_EXECUTOR, fetch_alpha_vantage_earnings, symbol) if market_name == "미국주식" and symbol else None),
            "dart_disclosures": _perf_submit(OPTIONAL_API_EXECUTOR, fetch_dart_disclosures, symbol, is_krw, 5),
            "domestic_flow": (_perf_submit(OPTIONAL_API_EXECUTOR, fetch_naver_domestic_flow_metrics, symbol) if is_krw and market_name == "국내주식" else None),
            "ticker_news": None,
        }
        optional_results = _collect_optional_results(optional_futures)
        _perf_stage("optional_external")
        stock_news = merge_news_sources(optional_results.get("stock_news_rss") or [], optional_results.get("ticker_news") or [])
        global_news = merge_news_sources(optional_results.get("global_news_rss") or [], optional_results.get("api_news") or [])
        macro_snapshot = optional_results.get("macro_snapshot") or {}
        ticker_news = optional_results.get("ticker_news") or []
        consensus = optional_results.get("consensus")
        alpha_earnings = optional_results.get("alpha_earnings") or {}
        dart_disclosures = optional_results.get("dart_disclosures") or []
        domestic_flow = optional_results.get("domestic_flow")


    # STEP 20: core 모드에서 만든 즉시 표시용 기본값을 공통 처리부가
    # 다시 None/빈 값으로 덮어쓰지 않도록 한다.
    # full 모드에서는 기존처럼 optional_results의 실제 데이터를 사용한다.
    stock_news_rss = optional_results.get("stock_news_rss") or []
    global_news_rss = optional_results.get("global_news_rss") or []
    api_news = optional_results.get("api_news") or []
    macro_snapshot = optional_results.get("macro_snapshot") or {}
    ticker_news = optional_results.get("ticker_news") or []

    # 종목 재료와 거시/시장 뉴스를 섞지 않는다.
    stock_news = merge_news_sources(stock_news_rss, ticker_news)
    global_news = merge_news_sources(global_news_rss, api_news)

    if not lightweight:
        consensus = optional_results.get("consensus")
        alpha_earnings = optional_results.get("alpha_earnings") or {}
        dart_disclosures = optional_results.get("dart_disclosures") or []
        domestic_flow = optional_results.get("domestic_flow")

    # ====================================================
    # STEP 4: 세계시장 보드 / 시장 캘린더를 분석 API에서 분리
    # ====================================================
    # /api/analyze가 최초 호출될 때 10개 세계시장 + 실적/경제 캘린더를
    # 기다리지 않는다. 이미 별도 API가 채워둔 서버 캐시가 있다면 사용하고,
    # 없으면 빈 배열로 분석을 진행한다. 화면에서는 별도 API가 병렬로 로딩한다.
    world_board = CACHED_WORLD_BOARD or []
    market_calendar = CACHED_MARKET_CALENDAR or []

    quant = calculate_quant_factors(raw_price, df, symbol=symbol, is_krw=is_krw)
    _perf_stage("quant")


    _perf_stage("movement_reason_start")
    try:
        movement_reason = analyze_movement_reason(
        raw_price=raw_price,
        df=df,
        formal_name=formal_name,
        market_name=market_name,
        world_board=world_board,
        stock_news=stock_news,
        dart_disclosures=dart_disclosures,
        market_index_name=market_index_name,
        domestic_flow=domestic_flow,
        global_news=global_news,
        consensus=consensus,
        macro_snapshot=macro_snapshot,
        market_calendar=market_calendar,
        symbol=symbol,
        alpha_earnings=alpha_earnings
        )
    except Exception as exc:
        print("[MOVEMENT] 분석 엔진 오류:", repr(exc))
        movement_reason = {
            "direction": "상승",
            "confidence": "낮음",
            "total_score": 0,
            "main_reason": "-",
            "main_reason_key": None,
            "global_impact": {"layers": [], "total_score": 0, "transmission_score": 0, "summary": "분석 근거 부족"},
            "summary": "일부 외부 데이터가 없어 원인을 확정하기 어렵습니다.",
            "reasons": [],
            "news_evidence": []
        }

    _perf_stage("movement_reason")
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S KST")

    df['MA20'] = df['Close'].rolling(20).mean()
    df['MA60'] = df['Close'].rolling(60).mean()
    df['STD60'] = df['Close'].rolling(60).std()
    df['BB_UP'] = df['MA60'] + (df['STD60'] * 2)
    df['BB_DN'] = df['MA60'] - (df['STD60'] * 2)

    df = df.sort_index()
    df = df[~df.index.duplicated(keep='last')]

    _perf_stage("chart_build_start")
    candles = []
    ma20_line = []
    ma60_line = []
    bb_up_line = []
    bb_dn_line = []

    for date_idx, row in df.tail(65).iterrows():
        t_str = pd.to_datetime(date_idx).strftime("%Y-%m-%d") if hasattr(date_idx, 'strftime') else str(date_idx)[:10]
        o = float(row['Open'])
        h = float(row['High'])
        l = float(row['Low'])
        c = float(row['Close'])
        if not (np.isnan(o) or np.isnan(h) or np.isnan(l) or np.isnan(c)):
            candles.append({
                "time": t_str,
                "open": o, "high": h,
                "low": l, "close": c
            })
        if 'MA20' in row and not np.isnan(row['MA20']):
            ma20_line.append({"time": t_str, "value": float(row['MA20'])})
        if 'MA60' in row and not np.isnan(row['MA60']):
            ma60_line.append({"time": t_str, "value": float(row['MA60'])})
        if 'BB_UP' in row and not np.isnan(row['BB_UP']):
            bb_up_line.append({"time": t_str, "value": float(row['BB_UP'])})
        if 'BB_DN' in row and not np.isnan(row['BB_DN']):
            bb_dn_line.append({"time": t_str, "value": float(row['BB_DN'])})

    _perf_stage("chart_build")

    # 코멘트용 현재가/등락률은 가능하면 동일한 실시간 Snapshot을 사용한다.
    current_change_pct = None
    current_session = None
    current_previous_close = None
    current_price_source = None
    if market_name == "미국주식":
        try:
            if 'quote_snapshot' in locals() and quote_snapshot:
                current_change_pct = quote_snapshot.get("change_pct")
                current_session = quote_snapshot.get("market_state")
                current_previous_close = quote_snapshot.get("previous_close")
                current_price_source = quote_snapshot.get("price_source")
        except Exception:
            pass

    # 국내/암호화폐/기존 데이터는 차트의 직전 종가를 이용해 보조적으로 계산한다.
    if current_change_pct is None and df is not None and len(df) >= 2 and raw_price is not None:
        try:
            prev_for_pct = float(df["Close"].iloc[-2])
            if prev_for_pct:
                current_change_pct = ((float(raw_price) - prev_for_pct) / prev_for_pct) * 100.0
                current_previous_close = prev_for_pct
                current_price_source = "calculated"
        except Exception:
            pass

    _perf_stage("result_build")

    result_data = {
        "name": formal_name,
        "formal_name": formal_name,
        "symbol": symbol,
        "market": market_name,
        "market_index_name": market_index_name,
        "price": price_str,
        "current_price": raw_price,
        "current_change_pct": current_change_pct,
        "current_previous_close": current_previous_close,
        "current_session": current_session,
        "current_price_source": current_price_source,
        "price_str": price_str,
        "time": now_str,
        "now_str": now_str,
        "quant": quant,
        "movement_reason": movement_reason,
        "consensus": consensus,
        "stock_news": stock_news,
        "global_news": global_news,
        "macro_snapshot": macro_snapshot,
        "world_board": world_board,
        "market_calendar": market_calendar,
        "dart_disclosures": dart_disclosures,
        "candles": candles,
        "ma20_line": ma20_line,
        "ma60_line": ma60_line,
        "bb_up_line": bb_up_line,
        "bb_dn_line": bb_dn_line
    }

    result_data["_details_ready"] = not lightweight
    # 입력한 이름/코드와 서버가 확정한 실제 symbol을 같은 캐시로 묶는다.
    _set_analysis_cache(clean_q, result_data, now_ts)

    if lightweight:
        try:
            enrich_df = df.copy(deep=True)
            _schedule_analysis_enrichment(
                clean_q,
                result_data,
                {
                    "market_name": market_name, "symbol": symbol, "formal_name": formal_name,
                    "raw_price": raw_price, "is_krw": is_krw, "market_index_name": market_index_name,
                    "df": enrich_df,
                },
            )
        except Exception as exc:
            print(f"[ANALYSIS ENRICH] scheduling failed: {type(exc).__name__}: {exc}")

    return result_data

# ----------------------------------------------------
# 8. 웹 프론트엔드 UI
# ----------------------------------------------------


# ===== STEP 21: 우측 실시간 시장 사이드바 =====
RIGHT_MARKET_BOARD_CACHE = {}
RIGHT_MARKET_BOARD_CACHE_TTL = 30
RIGHT_MARKET_BOARD_CACHE_VERSION = 43
RIGHT_MARKET_BOARD_CACHE_LOCK = threading.Lock()
# STEP 41: 국내 우측 실시간 목록에서 현재 gaemiGTP 차트/분석 API가 지원하지
# 않는 외국기업 계열 종목코드(9xxxxx)를 제외한다.
# 거래대금 데이터 자체는 정상이어도 차트/분석이 404가 되는 종목을
# 사용자가 클릭하지 않도록 사이드바 단계에서 미리 걸러낸다.
RIGHT_MARKET_KR_EXCLUDED_PREFIXES = ("9",)
RIGHT_MARKET_KR_EXCLUDED_CODES = {"950260"}
RIGHT_MARKET_BOARD_SYMBOLS = [
    "NVDA", "TSLA", "AAPL", "MSFT", "AMZN", "META", "GOOGL", "AMD",
    "AVGO", "PLTR", "MU", "TSM", "NFLX", "COIN", "MSTR"
]


def _right_sidebar_kr_rows(limit=15):
    """국내 실시간 거래대금 상위.

    STEP 33:
    - 국내 종목 후보는 Naver stock market JSON에서 가져온다.
    - 실제 현재가/등락률/누적거래량/누적거래대금은 Naver 실시간 polling API로
      한 번에 배치 조회한다.
    - 따라서 목록 API의 필드 구조에 거래대금이 없더라도 실제 거래대금(aa)을 사용한다.
    - 해외(Yahoo) / 코인(Binance) 수집 로직은 변경하지 않는다.
    """
    def _clean_key(value):
        return re.sub(r"[^a-z0-9가-힣]", "", str(value or "").lower())

    def _to_number(value):
        if value is None or isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            try:
                return float(value)
            except Exception:
                return None
        text = str(value).strip().replace(",", "").replace("%", "").replace("원", "")
        if text in {"", "-", "--", "nan", "NaN", "None", "null"}:
            return None
        try:
            return float(text)
        except Exception:
            pass
        m = re.match(r"^([0-9.]+)조(?:\s*([0-9.]+)억)?(?:\s*([0-9.]+)만)?$", text)
        if m:
            return float(m.group(1))*1_000_000_000_000 + float(m.group(2) or 0)*100_000_000 + float(m.group(3) or 0)*10_000
        m = re.match(r"^([0-9.]+)억(?:\s*([0-9.]+)만)?$", text)
        if m:
            return float(m.group(1))*100_000_000 + float(m.group(2) or 0)*10_000
        m = re.match(r"^([0-9.]+)백만$", text)
        if m:
            return float(m.group(1))*1_000_000
        m = re.match(r"^([0-9.]+)천만$", text)
        if m:
            return float(m.group(1))*10_000_000
        return None

    def _find_value(item, aliases):
        if not isinstance(item, dict):
            return None
        aliases = {_clean_key(x) for x in aliases}
        for k, v in item.items():
            if _clean_key(k) in aliases:
                return v
        for v in item.values():
            if isinstance(v, dict):
                for k, nv in v.items():
                    if _clean_key(k) in aliases:
                        return nv
        return None

    def _extract_candidate_items(payload):
        """stock.naver.com / m.stock.naver.com 응답에서 종목 배열을 찾는다."""
        candidates = []
        preferred = {"result", "data", "items", "stocks", "stocklist", "stockitems", "list"}

        def visit(obj, depth=0):
            if depth > 6:
                return
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if _clean_key(k) in preferred and isinstance(v, (list, dict)):
                        candidates.append(v)
                    visit(v, depth + 1)
            elif isinstance(obj, list):
                if obj and any(isinstance(x, dict) for x in obj):
                    candidates.append(obj)
                for v in obj[:20]:
                    visit(v, depth + 1)
        visit(payload)

        best = []
        best_score = -1
        for candidate in candidates:
            arr = candidate if isinstance(candidate, list) else [candidate]
            arr = [x for x in arr if isinstance(x, dict)]
            if not arr:
                continue
            score = 0
            for x in arr[:10]:
                keys = {_clean_key(k) for k in x.keys()}
                if keys & {"itemcode", "itemname", "stockcode", "stockname", "code", "name"}:
                    score += 2
                if keys & {"nowprice", "currentprice", "closeprice", "price"}:
                    score += 1
            if score > best_score:
                best_score = score
                best = arr
        return best

    def _candidate_from_item(item):
        code = _find_value(item, ["itemCode", "itemcode", "stockCode", "stockcode", "code", "종목코드"])
        name = _find_value(item, ["itemName", "itemname", "stockName", "stockname", "name", "종목명"])
        price = _find_value(item, ["nowPrice", "tradePrice", "currentPrice", "closePrice", "price", "현재가"])
        pct = _find_value(item, ["prevChangeRate", "changeRate", "fluctuationsRatio", "changePercent", "등락률"])
        code = re.sub(r"[^0-9]", "", str(code or ""))
        name = str(name or "").strip()
        if len(code) != 6 or not name:
            return None
        return {"code": code, "name": name, "price": _to_number(price), "change_pct": _to_number(pct)}

    # 1) 먼저 전체 국내 후보를 한 번에 받는다.
    candidate_items = []
    candidate_urls = [
        "https://stock.naver.com/api/domestic/market/stock/default?tradeType=KRX&marketType=ALL&orderType=quantTop&startIdx=0&pageSize=100",
        "https://m.stock.naver.com/front-api/stock/domestic/stockList?sortType=quantTop&category=KOSPI&page=1&pageSize=100",
        "https://m.stock.naver.com/front-api/stock/domestic/stockList?sortType=quantTop&category=KOSDAQ&page=1&pageSize=100",
    ]
    for url in candidate_urls:
        try:
            res = HTTP_SESSION.get(url, timeout=3.5, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/151.0 Safari/537.36",
                "Accept": "application/json,text/plain,*/*",
                "Referer": "https://stock.naver.com/",
            })
            res.raise_for_status()
            items = _extract_candidate_items(res.json())
            for item in items:
                row = _candidate_from_item(item)
                if row:
                    candidate_items.append(row)
            if len(candidate_items) >= 60:
                break
        except Exception as exc:
            print(f"[RIGHT MARKET BOARD] KR candidate API failed: {type(exc).__name__}: {exc}")

    # 후보가 없으면 국내 기능만을 위한 마지막 fallback으로 기존 HTML을 사용한다.
    if not candidate_items:
        for sosok, exchange in ((0, "KOSPI"), (1, "KOSDAQ")):
            try:
                url = f"https://finance.naver.com/sise/sise_quant.naver?sosok={sosok}"
                res = HTTP_SESSION.get(url, timeout=3.5, headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/151.0 Safari/537.36",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Referer": "https://finance.naver.com/",
                })
                res.raise_for_status()
                tables = pd.read_html(io.StringIO(res.text))
                for table in tables:
                    if not isinstance(table, pd.DataFrame) or table.empty:
                        continue
                    flat = [str(c[-1] if isinstance(c, tuple) else c).strip() for c in table.columns]
                    if "종목명" not in flat:
                        continue
                    table = table.copy(); table.columns = flat
                    for _, item in table.iterrows():
                        name = str(item.get("종목명") or "").strip()
                        if name:
                            candidate_items.append({"code": "", "name": name,
                                "price": _to_number(item.get("현재가")),
                                "change_pct": _to_number(item.get("등락률"))})
                    break
            except Exception as exc:
                print(f"[RIGHT MARKET BOARD] KR HTML fallback failed: {type(exc).__name__}: {exc}")

    # STEP 41: 현재 차트/분석 API에서 지원하지 않는 9xxxxx 계열 종목은
    # 우측 국내 거래대금 목록에서 제외한다.
    # (예: 950260 인제니아테라퓨틱스)
    code_map = {}
    for item in candidate_items:
        code = str(item.get("code") or "").strip()
        if not code or code in code_map:
            continue
        if code in RIGHT_MARKET_KR_EXCLUDED_CODES or any(code.startswith(prefix) for prefix in RIGHT_MARKET_KR_EXCLUDED_PREFIXES):
            continue
        code_map[code] = item
    codes = list(code_map.keys())[:100]

    if codes:
        try:
            query = "SERVICE_ITEM:" + ",".join(codes)
            url = "https://polling.finance.naver.com/api/realtime?query=" + urllib.parse.quote(query, safe=":,")
            res = HTTP_SESSION.get(url, timeout=3.5, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/151.0 Safari/537.36",
                "Accept": "application/json,text/plain,*/*",
                "Referer": "https://stock.naver.com/",
            })
            res.raise_for_status()
            payload = res.json()
            datas = []
            for area in (payload.get("result", {}).get("areas", []) if isinstance(payload, dict) else []):
                if isinstance(area, dict):
                    datas.extend(area.get("datas") or [])
            # 일부 응답은 result.areas가 아닌 최상위 areas를 사용한다.
            if not datas and isinstance(payload, dict):
                for area in payload.get("areas", []) or []:
                    if isinstance(area, dict):
                        datas.extend(area.get("datas") or [])
            rows = []
            for d in datas:
                if not isinstance(d, dict):
                    continue
                code = str(d.get("cd") or d.get("itemCode") or "").strip()
                item = code_map.get(code)
                if not item:
                    continue
                price = _to_number(d.get("nv") or d.get("closePrice") or item.get("price"))

                # Naver polling의 cr/cv는 일부 응답에서 절대값으로 내려오고,
                # 실제 상승/하락 방향은 rf 코드로 구분된다.
                # rf=2 상승, rf=5 하락, 그 외는 보합으로 처리한다.
                raw_pct = _to_number(d.get("cr") or d.get("fluctuationsRatio") or item.get("change_pct"))
                raw_change = _to_number(d.get("cv") or d.get("compareToPreviousClosePrice"))
                rise_fall = str(d.get("rf") or "").strip()
                if raw_pct is not None:
                    pct = abs(raw_pct)
                    if rise_fall == "5":
                        pct = -pct
                    elif rise_fall == "2":
                        pct = pct
                    else:
                        # rf가 없는 예외 응답은 기존 signed 값을 보존한다.
                        pct = raw_pct
                else:
                    pct = None
                if raw_change is not None:
                    change = abs(raw_change)
                    if rise_fall == "5":
                        change = -change
                    elif rise_fall == "2":
                        change = change
                    else:
                        change = raw_change
                else:
                    change = None
                volume = _to_number(d.get("aq") or d.get("accumulatedTradingVolume"))
                trading_value = _to_number(d.get("aa") or d.get("accumulatedTradingValue"))
                if trading_value is None and price is not None and volume is not None:
                    trading_value = price * volume
                if trading_value is None:
                    continue
                rows.append({
                    "rank": 0, "code": code, "symbol": code, "name": item["name"],
                    "price": price, "change_pct": pct, "change": change,
                    "trading_value": trading_value, "currency": "KRW", "market": "국내",
                    "source": "Naver Realtime Polling", "exchange": "KRX",
                })
            if rows:
                # STEP 42: 최종 안전필터. 후보 단계에서 제외되지 않았거나
                # API 응답에서 코드가 다시 들어오는 경우에도 분석 불가능한
                # 9xxxxx 계열 종목은 화면에 절대 노출하지 않는다.
                rows = [
                    row for row in rows
                    if not any(
                        (
                            str(row.get("code") or row.get("symbol") or "").strip() in RIGHT_MARKET_KR_EXCLUDED_CODES
                            or str(row.get("code") or row.get("symbol") or "").strip().startswith(prefix)
                        )
                        for prefix in RIGHT_MARKET_KR_EXCLUDED_PREFIXES
                    )
                ]
                if not rows:
                    return []
                rows.sort(key=lambda r: r.get("trading_value") or 0, reverse=True)
                for i, row in enumerate(rows[:limit], 1):
                    row["rank"] = i
                return rows[:limit]
        except Exception as exc:
            print(f"[RIGHT MARKET BOARD] KR realtime polling failed: {type(exc).__name__}: {exc}")

    # 실시간 polling이 실패하더라도 후보 목록에 가격/거래대금이 있는 경우만 표시한다.
    # 거래대금 없는 추정값을 만들어 순위를 왜곡하지 않는다.
    return []


def _right_sidebar_crypto_rows(limit=15):
    """Binance USDT 현물 전체 티커에서 quoteVolume 기준 상위 코인을 조회한다."""
    url = "https://api.binance.com/api/v3/ticker/24hr"
    res = HTTP_SESSION.get(url, timeout=4.0, headers={"User-Agent": "Mozilla/5.0"})
    res.raise_for_status()
    payload = res.json()
    rows = []
    for item in payload if isinstance(payload, list) else []:
        symbol = str(item.get("symbol") or "").upper()
        if not symbol.endswith("USDT") or symbol.endswith("USDCUSDT"):
            continue
        try:
            price = float(item.get("lastPrice") or 0)
            pct = float(item.get("priceChangePercent") or 0)
            change = float(item.get("priceChange") or 0)
            qv = float(item.get("quoteVolume") or 0)
        except Exception:
            continue
        base = symbol[:-4]
        rows.append({
            "rank": 0,
            "code": symbol,
            "symbol": symbol,
            "name": base,
            "price": price,
            "change_pct": pct,
            "change": change,
            "trading_value": qv,
            "currency": "USDT",
            "market": "코인",
            "source": "Binance",
        })
    rows.sort(key=lambda r: r.get("trading_value") or 0, reverse=True)
    for i, row in enumerate(rows[:limit], 1):
        row["rank"] = i
    return rows[:limit]


def fetch_right_market_board(market="kr", force=False):
    key = str(market or "kr").strip().lower()
    if key not in {"kr", "us", "crypto"}:
        key = "kr"
    now = time.time()
    with RIGHT_MARKET_BOARD_CACHE_LOCK:
        cached = RIGHT_MARKET_BOARD_CACHE.get(key)
        if cached and cached.get("version") == RIGHT_MARKET_BOARD_CACHE_VERSION and not force and now - cached.get("time", 0) < RIGHT_MARKET_BOARD_CACHE_TTL:
            return cached.get("data") or []
    try:
        if key == "kr":
            rows = _right_sidebar_kr_rows()
        elif key == "us":
            rows = _right_sidebar_us_rows()
        else:
            rows = _right_sidebar_crypto_rows()
    except Exception as exc:
        print(f"[RIGHT MARKET BOARD] {key} fetch failed: {type(exc).__name__}: {exc}")
        rows = []
    if rows:
        with RIGHT_MARKET_BOARD_CACHE_LOCK:
            RIGHT_MARKET_BOARD_CACHE[key] = {"time": time.time(), "data": rows, "version": RIGHT_MARKET_BOARD_CACHE_VERSION}
    return rows

DART_DISCLOSURE_WIDGET_JS = r"""
(function () {
    window.GaemiWidgets = window.GaemiWidgets || {};

    function escapeHtml(value) {
        return String(value ?? '')
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    window.GaemiWidgets.renderDartDisclosure = function (items) {
        const card = document.getElementById('rowDart');
        const list = document.getElementById('dartList');
        if (!card || !list) return;

        if (!Array.isArray(items) || items.length === 0) {
            list.innerHTML = '';
            card.style.display = 'none';
            return;
        }

        list.innerHTML = items.map(function (d) {
            const link = escapeHtml(d && d.link ? d.link : '#');
            const tagType = escapeHtml(d && d.tag_type ? d.tag_type : 'neutral');
            const tagName = escapeHtml(d && d.tag_name ? d.tag_name : '공시');
            const title = escapeHtml(d && d.title ? d.title : '');
            const date = escapeHtml(d && d.date ? d.date : '');
            return '<a href="' + link + '" target="_blank" rel="noopener noreferrer" class="dart-item">' +
                '<span class="dart-tag tag-' + tagType + '">' + tagName + '</span>' +
                '<span class="dart-title">' + title + '</span>' +
                '<span style="color:var(--text-muted); font-size:0.72rem; flex-shrink:0;">' + date + '</span>' +
                '</a>';
        }).join('');

        card.style.display = 'block';
    };
})();
"""

HTML_PAGE = r"""<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>gaemiGTP</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Google+Sans:wght@400;500;600;700&family=Roboto:wght@400;500;700&display=swap" rel="stylesheet">
    <script src="https://unpkg.com/lightweight-charts@4.1.1/dist/lightweight-charts.standalone.production.js"></script>
    <script src="/widgets/dart_disclosure.js"></script>
    <script src="/widgets/chart.js"></script>
    <script src="/widgets/stock_news.js"></script>
    <script src="/widgets/global_news.js"></script>
    <script src="/widgets/world_board.js"></script>
    <script src="/widgets/us_leading_sectors.js"></script>
    <script src="/widgets/us_leading_stocks.js"></script>
    <script src="/widgets/quant.js"></script>
    <script src="/widgets/track.js"></script>
    <script src="/widgets/earnings_calendar.js"></script>
    <script src="/widgets/economic_calendar.js"></script>
    <script src="/widgets/analyst_consensus.js"></script>
    <script src="/widgets/vote.js"></script>
    <script src="/widgets/comment.js"></script>
    <link rel="stylesheet" href="/style.css?v=1">
</head>
<body>

    <div id="sidebar-wrapper">
        <div class="side-top-action">
            <span style="font-weight:700; font-size:1.15rem; color:var(--text-title); cursor:pointer;" onclick="resetToHome()">gaemiGTP</span>
            <button class="gemini-menu-icon" onclick="toggleSidebar()" title="사이드 패널 닫기">
                <svg width="22" height="22" viewBox="0 0 24 24" fill="currentColor"><path d="M4 19V5C4 4.45 4.19583 3.97917 4.5875 3.5875C4.97917 3.19583 5.45 3 6 3H18C18.55 3 19.0208 3.19583 19.4125 3.5875C19.8042 3.97917 20 4.45 20 5V19C20 19.55 19.8042 20.0208 19.4125 20.4125C19.0208 20.8042 18.55 21 18 21H6C5.45 21 4.97917 20.8042 4.5875 20.4125C4.19583 20.0208 4 19.55 4 19ZM9 19H18V5H9V19ZM6 5V19V5Z"/></svg>
            </button>
        </div>

        <button class="gemini-new-btn" onclick="resetToHome()">
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 4v16m8-8H4"/></svg>
            <span>새 종목</span>
        </button>

        <div class="sidebar-scroll-area"></div>
    </div>

    <div id="content-wrapper">
        <header>
            <div class="header-left">
                <button class="gemini-menu-icon" id="headerToggleBtn" onclick="toggleSidebar()" title="사이드 패널 열기">
                    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="3" y1="12" x2="21" y2="12"></line><line x1="3" y1="6" x2="21" y2="6"></line><line x1="3" y1="18" x2="21" y2="18"></line></svg>
                </button>
                <div class="logo-top" id="headerLogo" onclick="resetToHome()">gaemiGTP</div>
            </div>

            <button class="gemini-menu-icon" id="themeToggleBtn" onclick="toggleTheme()" title="화면 테마 변경">
                <span id="themeIconArea"></span>
            </button>
        </header>

        <div id="main-view">
            <div class="hero-title">gaemiGTP</div>
            <div class="hero-sub">종목명 하나면 10초 만에 끝납니다</div>
            <div class="search-container">
                <div class="search-box">
                    <input type="text" id="centerInput" placeholder="종목명 입력 (예: SK하이닉스, 삼성전자, NVDA, BTC)" onkeypress="if(event.keyCode==13) openStock(this.value, {source:'main-search'});" autocomplete="off">
                    
                    <div class="model-selector-wrap">
                        <div class="model-pill-btn" onclick="toggleModelDropdown('center')">
                            <span class="model-pill-name" id="centerCurrentModel">gaemi</span>
                            <span class="model-pill-arrow">▲</span>
                        </div>
                        <div class="model-dropdown-menu" id="centerDropdown">
                            <div class="model-dropdown-item active" onclick="selectModel('gaemi', 'gaemi')">
                                <div class="item-title">gaemi <span class="item-check">✓</span></div>
                                <div class="item-sub">도움을 받으세요</div>
                            </div>
                            <div class="model-dropdown-item" onclick="selectModel('info', 'info')">
                                <div class="item-title">info <span class="item-check">✓</span></div>
                                <div class="item-sub">정보를 확인하세요</div>
                            </div>
                            <div class="model-dropdown-item" onclick="selectModel('pro', 'pro')">
                                <div class="item-title">pro <span class="item-check">✓</span></div>
                                <div class="item-sub">실력을 키우세요</div>
                            </div>
                        </div>
                    </div>

                    <button class="send-btn" onclick="openStock(document.getElementById('centerInput').value, {source:'main-search'})">
                        <svg width="18" height="18" viewBox="0 0 24 24"><path d="M12 4L12 20M12 4L5 11M12 4L19 11" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" fill="none"/></svg>
                    </button>
                </div>
            </div>
            <div class="popular-tags">
                <button class="tag-btn" onclick="openStock('SK하이닉스', {source:'example'})">SK하이닉스</button>
                <button class="tag-btn" onclick="openStock('삼성전자', {source:'example'})">삼성전자</button>
                <button class="tag-btn" onclick="openStock('알테오젠', {source:'example'})">알테오젠</button>
                <button class="tag-btn" onclick="openStock('엔비디아', {source:'example'})">엔비디아</button>
                <button class="tag-btn" onclick="openStock('비트코인', {source:'example'})">비트코인</button>
            </div>
        </div>

        <div id="result-scroll-view">
            <div class="result-inner">
                <div class="loading" id="loading"></div>
                <div id="result-card-wrap" style="display:none;">
                    
                    <!-- [마스터 그리드] PC: 832px + 348px 2열 / 모바일: 순차 1열 -->
                    <div class="dashboard-master-grid">
                        
                        <!-- 1행 좌측: 메인 차트 (832px) -->
                        <div class="card" id="rowChart">
                            <div>
                                <div class="price-title" id="rTitle"></div>
                                <div class="price-val" id="rPrice"></div>
                                <div class="price-meta" id="rMeta"></div>
</div>
                            <div class="chart-box-wrap" id="tvChart"></div>
                        </div>

                        <!-- 1행 우측: 증시는 지금 (348px, 차트와 높이 100% 일치) -->
                        <div class="card" id="rowWorld">
                            <div class="board-header">
                                <span class="board-header-title">증시는 지금</span>
                                <span class="board-header-sub">실시간 연동</span>
                            </div>
                            <div class="board-2col-grid" id="worldBoardGrid"></div>
                        </div>


                        <!-- 미국 주도업종: 기존 위젯과 독립된 카드 -->
                        <div id="usLeadingRightStack">
<div class="card us-leading-sector-card" id="rowUsLeadingSectors">
                            <div class="us-leading-sector-head">
                                <span class="us-leading-sector-title">미국 주도업종</span>
                                <span class="us-leading-sector-sub">TOP 10</span>
                            </div>
                            <div id="usLeadingSectorList" class="us-leading-sector-loading">데이터 불러오는 중…</div>
                        </div>
<div class="card us-leading-stock-card" id="rowUsLeadingStocks">
                            <div class="us-leading-stock-head">
                                <span class="us-leading-stock-title" id="usLeadingStockTitle">미국 주도종목 TOP 5</span>
                                <span class="us-leading-stock-sub">TOP 5</span>
                            </div>
                            <div id="usLeadingStockList" class="us-leading-stock-list">
                                <div class="us-leading-sector-loading">업종을 선택하면 주도종목을 불러옵니다.</div>
                            </div>
                        </div>
</div>

                        <!-- 4행 좌측: 퀀트 분석 (832px) -->
                        <div class="card" id="rowQuant">
                            <div style="margin-bottom:14px;">
                                <span style="font-size:1.05rem; font-weight:800; color:var(--text-title);">퀀트 분석</span>
                            </div>

                            <div style="display:flex; align-items:center; gap:14px; padding-bottom:14px; border-bottom:1px solid var(--border-line);">
                                <div class="gauge-inner-box">
                                    <span style="font-size:1.05rem; font-weight:800; color:var(--text-title); line-height:1;" id="qScoreVal"></span>
                                    <span style="font-size:0.55rem; color:var(--text-muted); margin-top:3px; font-weight:700;">모멘텀</span>
                                </div>
                                <div style="min-width:0;">
                                    <div style="display:flex; align-items:center; gap:6px; flex-wrap:wrap;">
                                        <span style="font-size:0.78rem; color:var(--text-muted);">계량 상태</span>
                                        <span style="background:rgba(16,185,129,0.15); color:#10b981; font-size:0.72rem; font-weight:800; padding:2px 7px; border-radius:10px;" id="qScoreBadge"></span>
                                    </div>
                                    <div style="font-size:0.74rem; color:var(--text-muted); margin-top:4px; word-break:keep-all;" id="qScoreDots"></div>
                                </div>
                            </div>

                            <div style="display:flex; justify-content:space-between; align-items:flex-end; margin-top:12px;">
                                <div>
                                    <div style="font-size:0.72rem; color:var(--text-muted); margin-bottom:2px;">통계적 Z-Score (20D 표준편차)</div>
                                    <div style="font-size:1.15rem; font-weight:800; color:#10b981;" id="qZScoreVal"></div>
                                </div>
                                <div style="text-align:right;">
                                    <div style="font-size:0.72rem; color:var(--text-muted); margin-bottom:2px;">연환산 변동성 (20D σ)</div>
                                    <div style="font-size:1.1rem; font-weight:700; color:var(--text-title);" id="qVolVal"></div>
                                </div>
                            </div>

                            <div style="margin:16px 0 12px; position:relative;">
                                <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:6px;">
                                    <span style="font-size:0.72rem; color:var(--text-muted);">최근 60거래일 상승 모멘텀 흐름</span>
                                    <span style="font-size:0.7rem; color:#10b981; font-weight:800;" id="qMomentumValTxt">현재 -</span>
                                </div>
                                <div style="width:100%; height:52px; background:var(--bg-subcard); border-radius:8px; position:relative; border:1px solid var(--border-line); overflow:hidden;">
                                    <svg id="qSignalChart" viewBox="0 0 600 120" preserveAspectRatio="none" style="width:100%; height:100%; display:block;"></svg>
                                </div>
                            </div>

                            <div style="margin:12px 0 12px; position:relative;">
                                <div style="display:flex; justify-content:space-between; font-size:0.7rem; margin-bottom:6px; white-space:nowrap;">
                                    <span style="color:var(--text-muted);" id="qLow52w"></span>
                                    <span style="color:#10b981; font-weight:700;" id="qPos52wLabel">52주 가격 위치</span>
                                    <span style="color:var(--text-muted);" id="qHigh52w"></span>
                                </div>
                                <div style="width:100%; height:6px; background:var(--bg-subcard); border-radius:4px; position:relative; border:1px solid var(--border-line);">
                                    <div style="height:100%; background:linear-gradient(90deg, #38bdf8, #10b981); border-radius:4px;" id="qSliderFill"></div>
                                    <div class="dot-marker-style" id="qDotMarker"></div>
                                </div>
                            </div>

                            <div style="margin-top:auto; border-top:1px solid var(--border-line); padding-top:10px;">
                                <div id="quantDetailList"></div>
                            </div>

                        </div>

                        <!-- 5행 좌측: 트랙 분석 — 종목 뉴스 아래 -->
                        <div class="card" id="rowTrack" style="justify-content:flex-start;">
                            <div style="margin-bottom:14px;">
                                <span style="font-size:1.05rem; font-weight:800; color:var(--text-title);">트랙 분석</span>
                            </div>

                            <div style="display:flex; align-items:center; gap:14px; padding-bottom:14px; border-bottom:1px solid var(--border-line);">
                                <div class="gauge-inner-box" style="border-color:#38bdf8;">
                                    <span style="font-size:1.05rem; font-weight:800; color:var(--text-title); line-height:1;" id="tEvidenceCountVal">-</span>
                                    <span style="font-size:0.55rem; color:var(--text-muted); margin-top:3px; font-weight:700;">근거</span>
                                </div>
                                <div style="min-width:0;">
                                    <div style="display:flex; align-items:center; gap:6px; flex-wrap:wrap;">
                                        <span style="font-size:0.78rem; color:var(--text-muted);">데이터 상태</span>
                                        <span style="background:rgba(56,189,248,0.15); color:#38bdf8; font-size:0.72rem; font-weight:800; padding:2px 7px; border-radius:10px;" id="tStatusBadge">-</span>
                                    </div>
                                    <div style="font-size:0.74rem; color:var(--text-muted); margin-top:4px; word-break:keep-all;" id="tStatusDots">실제 확인된 데이터 기준</div>
                                </div>
                            </div>

                            <div style="display:flex; justify-content:space-between; align-items:flex-end; margin-top:12px;">
                                <div>
                                    <div style="font-size:0.72rem; color:var(--text-muted); margin-bottom:2px;">현재 주가</div>
                                    <div style="font-size:1.15rem; font-weight:800; color:var(--text-title);" id="tPriceVal">-</div>
                                </div>
                                <div style="text-align:right;">
                                    <div style="font-size:0.72rem; color:var(--text-muted); margin-bottom:2px;">오늘 등락률</div>
                                    <div style="font-size:1.1rem; font-weight:700; color:var(--text-title);" id="tChangeVal">-</div>
                                </div>
                            </div>

                            <!-- 근거 확인: 8칸 근거 막대 -->
                            <div style="margin:12px 0 12px; position:relative;">
                                <div style="display:flex; justify-content:space-between; align-items:center; font-size:0.7rem; margin-bottom:6px; white-space:nowrap;">
                                    <span style="color:var(--text-muted);">근거 확인</span>
                                    <span style="color:#38bdf8; font-weight:800;" id="tEvidenceBarLabel">- / 8</span>
                                </div>
                                <div id="tEvidenceBar"
                                     style="display:grid; grid-template-columns:repeat(8,minmax(0,1fr)); gap:4px; width:100%; height:10px;">
                                </div>
                            </div>

                            <!-- 상단 그래프: 실제 종목 최근 60거래일 주가 흐름 -->
                            <div style="margin:16px 0 12px; position:relative;">
                                <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:6px;">
                                    <span style="font-size:0.72rem; color:var(--text-muted);">최근 60거래일 주가 흐름</span>
                                    <span style="font-size:0.7rem; color:#38bdf8; font-weight:800;" id="tPriceTrendTxt">현재 -</span>
                                </div>
                                <div style="width:100%; height:52px; background:var(--bg-subcard); border-radius:8px; position:relative; border:1px solid var(--border-line); overflow:hidden;">
                                    <svg id="tPriceChart" viewBox="0 0 600 120" preserveAspectRatio="none" style="width:100%; height:100%; display:block;"></svg>
                                </div>
                            </div>

                            <div style="margin-top:auto; border-top:1px solid var(--border-line); padding-top:10px;">
                                <div id="trackDetailList"></div>
                            </div>
                        </div>

                        

                        <!-- 4행 우측: 실적 일정 (트랙 분석과 1:1 높이) -->
                            <!-- 실적 일정 -->
                            <div class="card calendar-widget" id="rowEarningsCalendar">
                                <div class="calendar-widget-title">
                                    <span>실적 캘린더</span>
                                </div>

                                <div class="calendar-widget-nav">
                                    <div class="cal-date-ctrl">
                                        <button class="cal-arrow-btn" onclick="shiftEarningsCalendarDate(-1)">&#10094;</button>
                                        <span class="cal-current-date" id="earnCalDisplayDate">--</span>
                                        <button class="cal-arrow-btn" onclick="shiftEarningsCalendarDate(1)">&#10095;</button>
                                    </div>
                                    <button class="cal-today-btn" onclick="resetEarningsCalendarDate()" aria-label="실적 캘린더 오늘" title="오늘로 이동">↻</button>
                                </div>

                                <div class="calendar-star-filter" role="group" aria-label="중요도 필터">
                                    <button class="calendar-star-btn active" data-cal-type="earn" data-stars="all" onclick="setEarningsCalendarStarFilter('all',this)">전체</button>
                                    <button class="calendar-star-btn" data-cal-type="earn" data-stars="3" onclick="setEarningsCalendarStarFilter('3',this)">★★★</button>
                                    <button class="calendar-star-btn" data-cal-type="earn" data-stars="2" onclick="setEarningsCalendarStarFilter('2',this)">★★☆</button>
                                    <button class="calendar-star-btn" data-cal-type="earn" data-stars="1" onclick="setEarningsCalendarStarFilter('1',this)">★☆☆</button>
                                </div>
                                <div class="calendar-widget-subtitle" id="earnCalDayHeader">주요 기업 실적</div>
                                <div class="cal-event-list-wrap calendar-widget-list" id="earnCalEventList"></div>

                                <div class="cal-pagination-bar">
                                    <button class="cal-page-btn" id="btnEarnCalPrev" onclick="changeEarningsCalendarPage(-1)">&#10094;</button>
                                    <span class="cal-page-txt" id="earnCalPageIndicator">1 / 1</span>
                                    <button class="cal-page-btn" id="btnEarnCalNext" onclick="changeEarningsCalendarPage(1)">&#10095;</button>
                                </div>
                            </div>

                        <!-- 5행 우측: 경제 일정 (퀀트 분석과 1:1 높이) -->
                            <!-- 경제 일정 -->
                            <div class="card calendar-widget" id="rowEconomicCalendar">
                                <div class="calendar-widget-title">
                                    <span>경제 캘린더</span>
                                </div>

                                <div class="calendar-widget-nav">
                                    <div class="cal-date-ctrl">
                                        <button class="cal-arrow-btn" onclick="shiftEconomicCalendarDate(-1)">&#10094;</button>
                                        <span class="cal-current-date" id="ecoCalDisplayDate">--</span>
                                        <button class="cal-arrow-btn" onclick="shiftEconomicCalendarDate(1)">&#10095;</button>
                                    </div>
                                    <button class="cal-today-btn" onclick="resetEconomicCalendarDate()" aria-label="경제 캘린더 오늘" title="오늘로 이동">↻</button>
                                </div>

                                <div class="calendar-star-filter" role="group" aria-label="중요도 필터">
                                    <button class="calendar-star-btn active" data-cal-type="eco" data-stars="all" onclick="setCalendarStarFilter('eco','all',this)">전체</button>
                                    <button class="calendar-star-btn" data-cal-type="eco" data-stars="3" onclick="setCalendarStarFilter('eco','3',this)">★★★</button>
                                    <button class="calendar-star-btn" data-cal-type="eco" data-stars="2" onclick="setCalendarStarFilter('eco','2',this)">★★☆</button>
                                    <button class="calendar-star-btn" data-cal-type="eco" data-stars="1" onclick="setCalendarStarFilter('eco','1',this)">★☆☆</button>
                                </div>
                                <div class="calendar-widget-subtitle" id="ecoCalDayHeader">주요 경제지표</div>
                                <div class="cal-event-list-wrap calendar-widget-list" id="ecoCalEventList"></div>

                                <div class="cal-pagination-bar">
                                    <button class="cal-page-btn" id="btnEcoCalPrev" onclick="changeEconomicCalendarPage(-1)">&#10094;</button>
                                    <span class="cal-page-txt" id="ecoCalPageIndicator">1 / 1</span>
                                    <button class="cal-page-btn" id="btnEcoCalNext" onclick="changeEconomicCalendarPage(1)">&#10095;</button>
                                </div>
                            </div>

                        <!-- 3행 좌측: 종목 뉴스 (832px) -->
                        <div class="card" id="rowNews">
                            <div class="board-header-title" style="margin-bottom:12px;">종목 뉴스</div>
                            <div id="stockNewsList" style="flex:1;"></div>
                        </div>

                        <!-- 3행 우측: 해외 뉴스 (348px, 캘린더 바로 하단 & 종목 뉴스와 높이 100% 일치) -->
                        <div class="card" id="rowGlobalNews">
                            <div class="board-header-title" style="margin-bottom:12px;">경제뉴스</div>
                            <div id="globalNewsList" style="flex:1;"></div>
                        </div>

                        <!-- 4행: 애널리스트 컨센서스 (832px) -->
                        <div class="card" id="rowConsensus" style="display:none;">
                            <div style="margin-bottom:16px;">
                                <span style="font-size:1.05rem; font-weight:700; color:var(--text-title);" id="uTitle"></span>
                            </div>

                            <div style="display:flex; align-items:center; gap:14px; padding-bottom:16px; border-bottom:1px solid var(--border-line);">
                                <div class="gauge-inner-box">
                                    <span style="font-size:1.0rem; font-weight:800; color:var(--text-title); line-height:1;" id="uGaugeVal"></span>
                                    <span style="font-size:0.55rem; color:var(--text-muted); margin-top:3px;" id="uGaugeLabel"></span>
                                </div>
                                <div style="min-width:0;">
                                    <div style="display:flex; align-items:center; gap:6px; flex-wrap:wrap;">
                                        <span style="font-size:0.78rem; color:var(--text-muted);" id="uStatusLabel"></span>
                                        <span style="background:rgba(16,185,129,0.15); color:#10b981; font-size:0.72rem; font-weight:700; padding:2px 7px; border-radius:10px;" id="uBadge"></span>
                                    </div>
                                    <div style="font-size:0.74rem; color:var(--text-muted); margin-top:4px; word-break:keep-all;" id="uDots"></div>
                                </div>
                            </div>

                            <div style="display:flex; justify-content:space-between; align-items:flex-end; margin-top:14px;">
                                <div>
                                    <div style="font-size:0.72rem; color:var(--text-muted); margin-bottom:2px;">평균 목표가</div>
                                    <div style="font-size:1.25rem; font-weight:800; color:#10b981;" id="uPrimaryVal"></div>
                                </div>
                                <div style="text-align:right;">
                                    <div style="font-size:0.72rem; color:var(--text-muted); margin-bottom:2px;">현재가</div>
                                    <div style="font-size:1.15rem; font-weight:700; color:var(--text-title);" id="uSecondaryVal"></div>
                                </div>
                            </div>

                            <div style="margin:20px 0 14px; position:relative;">
                                <div style="display:flex; justify-content:space-between; font-size:0.7rem; margin-bottom:6px; white-space:nowrap;">
                                    <span style="color:var(--text-muted);" id="uSliderLeft"></span>
                                    <span style="color:#10b981; font-weight:700;" id="uSliderMid"></span>
                                    <span style="color:var(--text-muted);" id="uSliderRight"></span>
                                </div>
                                <div style="width:100%; height:6px; background:var(--bg-subcard); border-radius:4px; position:relative; border:1px solid var(--border-line);">
                                    <div style="height:100%; background:linear-gradient(90deg, #38bdf8, #10b981); border-radius:4px;" id="uBarFill"></div>
                                    <div class="dot-marker-style" id="uDotMarker"></div>
                                </div>
                            </div>

                            <div style="margin-top:16px; border-top:1px solid var(--border-line); padding-top:12px;">
                                <div style="font-size:0.82rem; font-weight:700; color:var(--text-sub); margin-bottom:8px; display:flex; justify-content:space-between;">
                                    <span id="uListTitle"></span>
                                    <span style="font-size:0.68rem; color:var(--text-muted); font-weight:normal;" id="uSourceTag"></span>
                                </div>
                                <div id="uItemList"></div>
                            </div>
                        </div>

                        <!-- 5행: 개미 투표 (832px) -->
                        <div class="card" id="rowVote">
                            <div class="board-header">
                                <span class="board-header-title">개미 투표</span>
                                <span style="font-size:0.72rem; color:var(--text-muted);">내일 주가 전망</span>
                            </div>
                            <div style="margin:8px 0;">
                                <div style="display:flex; justify-content:space-between; font-size:0.75rem; font-weight:700; margin-bottom:6px;">
                                    <span style="color:#10b981;" id="voteLongPct">상승 50%</span>
                                    <span style="color:#38bdf8;" id="voteShortPct">하락 50%</span>
                                </div>
                                <div style="width:100%; height:8px; background:var(--bg-subcard); border-radius:4px; overflow:hidden; display:flex; border:1px solid var(--border-line);">
                                    <div style="height:100%; background:#10b981; width:50%; transition:width 0.3s;" id="voteLongBar"></div>
                                    <div style="height:100%; background:#38bdf8; width:50%; transition:width 0.3s;" id="voteShortBar"></div>
                                </div>
                            </div>
                            <div class="vote-btn-wrap">
                                <button class="vote-btn" id="btnVoteLong" onclick="castVote('long')">상승 전망</button>
                                <button class="vote-btn" id="btnVoteShort" onclick="castVote('short')">하락 전망</button>
                            </div>
                        </div>

                        <!-- 6행: 개미 토론 (832px) -->
                        <div class="card" id="rowComment">
                            <div class="board-header">
                                <span class="board-header-title">개미 토론</span>
                                <span style="font-size:0.72rem; color:var(--text-muted);" id="commentCountTag">0개의 의견</span>
                            </div>
                            <div id="commentList" style="margin-top:8px;"></div>
                            <div class="comment-input-box">
                                <input type="text" id="commentInput" placeholder="이 종목에 대한 생각을 자유롭게 적어주세요" onkeypress="if(event.keyCode==13) postComment();" autocomplete="off" maxlength="80">
                                <button onclick="postComment()">등록</button>
                            </div>
                        </div>

                        <!-- 7행: DART 전자공시 (832px) -->
                        <div class="card" id="rowDart" style="display:none;">
                            <div class="board-header">
                                <span class="board-header-title">DART 전자공시</span>
                                <span style="font-size:0.72rem; color:var(--text-muted);">최근 핵심 공시</span>
                            </div>
                            <div id="dartList"></div>
                        </div>

</div>

                </div>
            </div>
        </div>

        <div id="bottom-bar">
            <div class="bottom-bar-grid-wrapper">
                <div class="search-container">
                    <div class="search-box">
                        <input type="text" id="bottomInput" placeholder="다른 종목명 입력 (예: 테슬라, 카카오, BTC)" onkeypress="if(event.keyCode==13) openStock(this.value, {source:'bottom-search'});" autocomplete="off">
                        
                        <div class="model-selector-wrap">
                            <div class="model-pill-btn" onclick="toggleModelDropdown('bottom')">
                                <span class="model-pill-name" id="bottomCurrentModel">gaemi</span>
                                <span class="model-pill-arrow">▲</span>
                            </div>
                            <div class="model-dropdown-menu" id="bottomDropdown">
                                <div class="model-dropdown-item active" onclick="selectModel('gaemi', 'gaemi')">
                                    <div class="item-title">gaemi <span class="item-check">✓</span></div>
                                    <div class="item-sub">도움을 받으세요</div>
                                </div>
                                <div class="model-dropdown-item" onclick="selectModel('info', 'info')">
                                    <div class="item-title">info <span class="item-check">✓</span></div>
                                    <div class="item-sub">정보를 확인하세요</div>
                                </div>
                                <div class="model-dropdown-item" onclick="selectModel('pro', 'pro')">
                                    <div class="item-title">pro <span class="item-check">✓</span></div>
                                    <div class="item-sub">실력을 키우세요</div>
                                </div>
                            </div>
                        </div>

                        <button class="send-btn" onclick="openStock(document.getElementById('bottomInput').value, {source:'bottom-search'})">
                            <svg width="18" height="18" viewBox="0 0 24 24"><path d="M12 4L12 20M12 4L5 11M12 4L19 11" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" fill="none"/></svg>
                        </button>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <script src="/widgets/right_market.js"></script>

    <script>
        let chart = null;
        let selectedMode = 'gaemi';
        let currentSymbol = '';
        let globalMarketCalendar = [];
        let currentCalOffset = 0;
        let currentCalFilter = 'all';
        let currentCalPage = 1;
        const calItemsPerPage = 4;

        function setTxt(id, val) { const el = document.getElementById(id); if (el) el.innerText = (val !== undefined && val !== null) ? String(val) : ''; }
        function setHtml(id, val) { const el = document.getElementById(id); if (el) el.innerHTML = (val !== undefined && val !== null) ? String(val) : ''; }

        function initTheme() {
            const saved = localStorage.getItem('gaemi_theme') || 'dark';
            if (saved === 'light') {
                document.body.classList.add('light-theme');
                updateThemeIcon(true);
            } else {
                document.body.classList.remove('light-theme');
                updateThemeIcon(false);
            }
        }

        function toggleTheme() {
            const isLight = document.body.classList.toggle('light-theme');
            localStorage.setItem('gaemi_theme', isLight ? 'light' : 'dark');
            updateThemeIcon(isLight);

            chart = window.gaemiChart || chart;
            if (chart) {
                chart.applyOptions({
                    layout: {
                        background: { color: isLight ? '#ffffff' : '#1e1f20' },
                        textColor: isLight ? '#5f6368' : '#8e918f'
                    },
                    timeScale: { borderColor: isLight ? '#dadce0' : '#282a2c' },
                    rightPriceScale: { borderColor: isLight ? '#dadce0' : '#282a2c' }
                });
            }
        }

        function updateThemeIcon(isLight) {
            const area = document.getElementById('themeIconArea');
            if (!area) return;
            area.innerHTML = isLight 
                ? `<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"></path></svg>`
                : `<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="5"></circle><line x1="12" y1="1" x2="12" y2="3"></line><line x1="12" y1="21" x2="12" y2="23"></line><line x1="4.22" y1="4.22" x2="5.64" y2="5.64"></line><line x1="18.36" y1="18.36" x2="19.78" y2="19.78"></line><line x1="1" y1="12" x2="3" y2="12"></line><line x1="21" y1="12" x2="23" y2="12"></line><line x1="4.22" y1="19.78" x2="5.64" y2="18.36"></line><line x1="18.36" y1="5.64" x2="19.78" y2="4.22"></line></svg>`;
        }

        function resizeChart() {
            const container = document.getElementById('tvChart');
            chart = window.gaemiChart || chart;
            if (chart && container && container.clientWidth > 0) {
                chart.applyOptions({ width: container.clientWidth });
            }
        }

        window.addEventListener('resize', resizeChart);

        function toggleSidebar() {
            const side = document.getElementById('sidebar-wrapper');
            if (side) side.classList.toggle('open');
        }

        function toggleModelDropdown(target) {
            const centerD = document.getElementById('centerDropdown');
            const bottomD = document.getElementById('bottomDropdown');
            const targetD = target === 'center' ? centerD : bottomD;
            const otherD = target === 'center' ? bottomD : centerD;
            if (otherD) otherD.style.display = 'none';
            if (targetD) targetD.style.display = targetD.style.display === 'block' ? 'none' : 'block';
        }

        function selectModel(modeKey, modeName) {
            selectedMode = modeKey;
            setTxt('centerCurrentModel', modeName);
            setTxt('bottomCurrentModel', modeName);

            ['centerDropdown', 'bottomDropdown'].forEach(id => {
                const menu = document.getElementById(id);
                if (menu) {
                    const items = menu.querySelectorAll('.model-dropdown-item');
                    items.forEach(it => {
                        if (it.innerText.includes(modeName)) {
                            it.classList.add('active');
                        } else {
                            it.classList.remove('active');
                        }
                    });
                    menu.style.display = 'none';
                }
            });
        }

        document.addEventListener('click', function(e) {
            if (!e.target.closest('.model-selector-wrap')) {
                const cd = document.getElementById('centerDropdown');
                const bd = document.getElementById('bottomDropdown');
                if (cd) cd.style.display = 'none';
                if (bd) bd.style.display = 'none';
            }
            if (!e.target.closest('#sidebar-wrapper') && !e.target.closest('#headerToggleBtn')) {
                const side = document.getElementById('sidebar-wrapper');
                if (side && side.classList.contains('open')) {
                    side.classList.remove('open');
                }
            }
        });

        function resetToHome() {
            // 진행 중인 분석/상세 요청이 홈 화면으로 돌아온 뒤 결과를 덮어쓰지 못하게 한다.
            activeSearchToken++;
            if (activeSearchController) { try { activeSearchController.abort(); } catch (e) {} }
            if (activeDetailsController) { try { activeDetailsController.abort(); } catch (e) {} }
            document.getElementById('main-view').style.display = 'flex';
            document.getElementById('result-scroll-view').style.display = 'none';
            document.getElementById('bottom-bar').style.display = 'none';
            document.getElementById('result-card-wrap').style.display = 'none';
            document.getElementById('loading').style.display = 'none';
            
            const uCard = document.getElementById('rowConsensus');
            if (uCard) uCard.style.display = 'none';
            const dCard = document.getElementById('rowDart');
            if (dCard) dCard.style.display = 'none';
            
            document.getElementById('centerInput').value = '';
            document.getElementById('bottomInput').value = '';
            currentSymbol = '';

            // 홈으로 돌아가면 저장된 종목 URL도 제거
            const homeUrl = new URL(window.location.href);
            homeUrl.searchParams.delete('q');
            window.history.replaceState({}, '', homeUrl.toString());
            const side = document.getElementById('sidebar-wrapper');
            if (side) side.classList.remove('open');
        }

function renderSparklineSVG(points, isUp) {
            if (!points || points.length < 2) return '';
            const min = Math.min(...points);
            const max = Math.max(...points);
            const range = (max - min) === 0 ? 1 : (max - min);
            const width = 44;
            const height = 16;
            const padY = 2;
            
            const coords = points.map((p, i) => {
                const x = (i / (points.length - 1)) * width;
                const y = height - padY - ((p - min) / range) * (height - padY * 2);
                return `${x.toFixed(1)},${y.toFixed(1)}`;
            });
            
            const pathD = 'M ' + coords.join(' L ');
            const strokeColor = isUp ? '#ef4444' : '#38bdf8';
            
            return `<svg width="${width}" height="${height}" viewBox="0 0 ${width} ${height}" class="mini-spark">
                <path d="${pathD}" fill="none" stroke="${strokeColor}" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/>
            </svg>`;
        }

        function getTargetDateStr(offset) {
            const now = new Date();
            const kstString = new Intl.DateTimeFormat('en-CA', {
                timeZone: 'Asia/Seoul',
                year: 'numeric', month: '2-digit', day: '2-digit'
            }).format(now);
            const [yyyy, mm, dd] = kstString.split('-').map(Number);
            const d = new Date(Date.UTC(yyyy, mm - 1, dd + Number(offset || 0)));
            return `${d.getUTCFullYear()}.${String(d.getUTCMonth() + 1).padStart(2, '0')}.${String(d.getUTCDate()).padStart(2, '0')}`;
        }

        function shiftCalendarDate(delta) {
            currentCalOffset += delta;
            currentCalPage = 1;
            renderMarketCalendarView();
        }

        function resetCalendarDate() {
            currentCalOffset = 0;
            currentCalPage = 1;
            renderMarketCalendarView();
        }

        function setCalendarFilter(filterKey) {
            currentCalFilter = filterKey;
            currentCalPage = 1;
            document.querySelectorAll('.cal-filter-pill').forEach(btn => btn.classList.remove('active'));
            if (filterKey === 'all') document.getElementById('filterAll').classList.add('active');
            if (filterKey === 'star3') document.getElementById('filterStar3').classList.add('active');
            if (filterKey === 'star2') document.getElementById('filterStar2').classList.add('active');
            if (filterKey === 'star1') document.getElementById('filterStar1').classList.add('active');
            if (filterKey === 'earn') document.getElementById('filterEarn').classList.add('active');
            if (filterKey === 'eco') document.getElementById('filterEco').classList.add('active');
            renderMarketCalendarView();
        }

        function changeCalPage(delta) {
            currentCalPage += delta;
            renderMarketCalendarView();
        }


        // 캘린더 알람: 브라우저 알림 권한 + localStorage 저장
        const CAL_ALARM_STORAGE_KEY = 'calendar_alarm_events_v1';
        const CAL_ALARM_MINUTES_BEFORE = 10;

        function getCalendarAlarms() {
            try {
                return JSON.parse(localStorage.getItem(CAL_ALARM_STORAGE_KEY) || '{}');
            } catch (e) {
                return {};
            }
        }

        function saveCalendarAlarms(data) {
            localStorage.setItem(CAL_ALARM_STORAGE_KEY, JSON.stringify(data));
        }

        function getCalendarEventKey(ev) {
            return [
                ev.type || '',
                ev.date_key || '',
                ev.time || '',
                ev.ticker || '',
                ev.name || ''
            ].join('|');
        }

        async function toggleCalendarAlarmByKey(encodedKey) {
            let key;
            try {
                key = decodeURIComponent(encodedKey);
            } catch (e) {
                console.error('[calendar] invalid alarm key:', e);
                return;
            }
            const alarms = getCalendarAlarms();
            const entries = Object.values(alarms || {});
            const target = entries.find(item => item && getCalendarEventKey(item.event) === key);
            if (target && target.event) {
                await toggleCalendarAlarm(target.event);
            }
        }

        async function toggleCalendarAlarm(ev) {
            const key = getCalendarEventKey(ev);
            const alarms = getCalendarAlarms();

            if (alarms[key]) {
                delete alarms[key];
                saveCalendarAlarms(alarms);
                renderMarketCalendarView();
                return;
            }

            if (!('Notification' in window)) {
                alert('이 브라우저는 알림 기능을 지원하지 않습니다.');
                return;
            }

            if (Notification.permission === 'default') {
                const permission = await Notification.requestPermission();
                if (permission !== 'granted') return;
            } else if (Notification.permission !== 'granted') {
                alert('브라우저 알림 권한이 꺼져 있습니다. 브라우저 설정에서 알림을 허용해주세요.');
                return;
            }

            alarms[key] = {
                event: ev,
                createdAt: Date.now(),
                notified: false
            };
            saveCalendarAlarms(alarms);
            renderMarketCalendarView();
        }

        function parseCalendarEventTime(ev) {
            if (!ev || !ev.date_key || !ev.time) return null;
            const m = String(ev.date_key).match(/^(\d{4})[.\/-](\d{1,2})[.\/-](\d{1,2})$/);
            const t = String(ev.time).match(/^(\d{1,2}):(\d{2})$/);
            if (!m || !t) return null;

            // 캘린더의 날짜/시간은 기존 화면 기준 한국시간(KST)으로 처리합니다.
            const utcMs = Date.UTC(
                Number(m[1]), Number(m[2]) - 1, Number(m[3]),
                Number(t[1]), Number(t[2])
            );
            return utcMs - (9 * 60 * 60 * 1000);
        }

        function checkCalendarAlarms() {
            const alarms = getCalendarAlarms();
            const now = Date.now();
            let changed = false;

            Object.keys(alarms).forEach(key => {
                const item = alarms[key];
                if (!item || item.notified) return;

                const eventMs = parseCalendarEventTime(item.event);
                if (!eventMs) return;

                const alarmMs = eventMs - (CAL_ALARM_MINUTES_BEFORE * 60 * 1000);

                if (now >= alarmMs && now < eventMs + (60 * 60 * 1000)) {
                    if ('Notification' in window && Notification.permission === 'granted') {
                        new Notification('📅 일정 알림', {
                            body: `${item.event.name || '일정'}\n10분 후 발표 예정 (${item.event.time})`,
                            tag: 'calendar-' + key
                        });
                    }
                    item.notified = true;
                    changed = true;
                }
            });

            if (changed) saveCalendarAlarms(alarms);
        }

        // 15초마다 확인하여 10분 전 알림을 놓치지 않도록 합니다.
        setInterval(checkCalendarAlarms, 15000);
        setTimeout(checkCalendarAlarms, 1000);

        // ============================================================
        // 경제 일정 / 실적 일정 독립 위젯
        // - 같은 globalMarketCalendar 데이터를 사용하지만
        //   날짜 이동/페이지 상태/렌더링은 서로 완전히 분리
        // ============================================================
        // 경제 캘린더는 독립 위젯으로 분리되어 있습니다.
        function renderEconomicCalendarView() {
            return window.GaemiWidgets && window.GaemiWidgets.economicCalendar
                ? window.GaemiWidgets.economicCalendar.render() : undefined;
        }
        function setCalendarStarFilter(type, value, btn) {
            return window.GaemiWidgets && window.GaemiWidgets.economicCalendar
                ? window.GaemiWidgets.economicCalendar.setStarFilter(value, btn) : undefined;
        }
        function shiftEconomicCalendarDate(delta) {
            return window.GaemiWidgets && window.GaemiWidgets.economicCalendar
                ? window.GaemiWidgets.economicCalendar.shiftDate(delta) : undefined;
        }
        function resetEconomicCalendarDate() {
            return window.GaemiWidgets && window.GaemiWidgets.economicCalendar
                ? window.GaemiWidgets.economicCalendar.resetDate() : undefined;
        }
        function changeEconomicCalendarPage(delta) {
            return window.GaemiWidgets && window.GaemiWidgets.economicCalendar
                ? window.GaemiWidgets.economicCalendar.changePage(delta) : undefined;
        }

        // 실적 캘린더는 독립 위젯으로 분리되어 있습니다.
        // 기존 호출부/버튼 호환성을 위해 얇은 브리지 함수만 유지합니다.
        function renderEarningsCalendarView() {
            return window.GaemiWidgets && window.GaemiWidgets.earningsCalendar
                ? window.GaemiWidgets.earningsCalendar.render()
                : undefined;
        }

        function setEarningsCalendarStarFilter(value, btn) {
            return window.GaemiWidgets && window.GaemiWidgets.earningsCalendar
                ? window.GaemiWidgets.earningsCalendar.setStarFilter(value, btn)
                : undefined;
        }

        function shiftEarningsCalendarDate(delta) {
            return window.GaemiWidgets && window.GaemiWidgets.earningsCalendar
                ? window.GaemiWidgets.earningsCalendar.shiftDate(delta)
                : undefined;
        }

        function resetEarningsCalendarDate() {
            return window.GaemiWidgets && window.GaemiWidgets.earningsCalendar
                ? window.GaemiWidgets.earningsCalendar.resetDate()
                : undefined;
        }

        function changeEarningsCalendarPage(delta) {
            return window.GaemiWidgets && window.GaemiWidgets.earningsCalendar
                ? window.GaemiWidgets.earningsCalendar.changePage(delta)
                : undefined;
        }

        function renderMarketCalendarView() {
            // 기존 호출부와의 호환성을 유지하되 두 위젯을 각각 렌더링합니다.
            renderEconomicCalendarView();
            renderEarningsCalendarView();
        }


        // PC에서 같은 행의 좌/우 패널 높이를 항상 1:1로 맞춘다.
        function syncCalendarRowHeights() {
            if (window.innerWidth <= 1200) return;
            const pairs = [
                ['rowTrack', 'rowEarningsCalendar'],
                ['rowQuant', 'rowEconomicCalendar']
            ];
            pairs.forEach(([leftId, rightId]) => {
                const left = document.getElementById(leftId);
                const right = document.getElementById(rightId);
                if (!left || !right) return;
                left.style.height = '';
                right.style.height = '';
                const h = Math.max(left.getBoundingClientRect().height, right.getBoundingClientRect().height, 560);
                left.style.height = `${Math.ceil(h)}px`;
                right.style.height = `${Math.ceil(h)}px`;
            });
        }
        let calendarHeightSyncTimer = null;
        function scheduleCalendarHeightSync() {
            if (calendarHeightSyncTimer) clearTimeout(calendarHeightSyncTimer);
            calendarHeightSyncTimer = setTimeout(syncCalendarRowHeights, 60);
        }
        window.addEventListener('resize', scheduleCalendarHeightSync);
        if (document.fonts && document.fonts.ready) document.fonts.ready.then(scheduleCalendarHeightSync);
        setTimeout(scheduleCalendarHeightSync, 300);

        async function loadCommunityData(symbol, displayName) {
            currentSymbol = symbol;
            const targetName = displayName || symbol;
if (window.GaemiWidgets?.vote) window.GaemiWidgets.vote.load(symbol);

            if (window.GaemiWidgets?.comment) window.GaemiWidgets.comment.load(symbol);
        }

        // STEP 48: 프론트 공통 수급 파싱 함수
        // doSearch 내부 const로 선언하면 다른 렌더링 경로/구버전 스크립트와
        // 충돌할 때 ReferenceError가 발생할 수 있으므로 전역 함수로 고정한다.
        function getDomesticFlowValue(movementReason, key) {
            try {
                const reasons = movementReason && Array.isArray(movementReason.reasons)
                    ? movementReason.reasons
                    : [];
                const r = reasons.find(x => x && x.key === key);
                if (!r) return null;
                const text = String(r.detail || '');
                const m = text.match(/(순매수|순매도)\s+([0-9,]+)주/);
                if (!m) return null;
                const n = Number(String(m[2]).replace(/,/g, ''));
                if (!Number.isFinite(n)) return null;
                return m[1] === '순매도' ? -n : n;
            } catch (e) {
                console.warn('[FLOW PARSER] failed:', e);
                return null;
            }
        }

        // 브라우저 콘솔에서 현재 서버가 실제로 STEP48 HTML을 내려주는지 확인할 수 있다.
        window.__GAEMIGTP_BUILD__ = 'STEP52-GOOGLE-NEWS-OPENSTOCK';

        const ANALYZE_CACHE = new Map();
        const ANALYZE_CACHE_TTL = 30 * 1000;
        let activeSearchToken = 0;

        // 브라우저 캐시도 서버와 동일하게 '종목명/종목코드/티커'를
        // 하나의 분석 데이터로 묶는다. 검색 입구가 달라도 같은 종목이면
        // 같은 Core/Details 결과를 사용해야 한다.
        function analyzeCacheKeys(query, data = null) {
            const keys = [];
            const add = (value) => {
                const v = String(value ?? '').trim().replace(/\s+/g, '').toUpperCase();
                if (v && !keys.includes(v)) keys.push(v);
            };
            add(query);
            if (data && typeof data === 'object') {
                add(data.symbol);
                add(data.name);
                add(data.formal_name);
            }
            return keys;
        }

        function setAnalyzeCache(query, data, timestamp = Date.now()) {
            analyzeCacheKeys(query, data).forEach(key => {
                ANALYZE_CACHE.set(key, { time: timestamp, data });
            });
        }

        function getAnalyzeCache(query) {
            const now = Date.now();
            for (const key of analyzeCacheKeys(query)) {
                const hit = ANALYZE_CACHE.get(key);
                if (hit && (now - hit.time) < ANALYZE_CACHE_TTL) return hit;
                if (hit) ANALYZE_CACHE.delete(key);
            }
            return null;
        }

        function deleteAnalyzeCache(query, data = null) {
            analyzeCacheKeys(query, data).forEach(key => ANALYZE_CACHE.delete(key));
        }
        let activeSearchController = null;
        let activeDetailsController = null;
        let rightMarketController = null;
        let rightMarketRequestToken = 0;
        const FAST_CHART_CACHE = new Map();
        const FAST_CHART_CACHE_TTL = 30 * 1000;

        // =====================================================================
        // 공통 종목 진입 계층
        // 모든 종목 선택/검색 UI는 openStock() 하나를 거쳐 기존 doSearch()로 들어간다.
        // 분석/뉴스/차트 엔진은 건드리지 않고 입력만 표준화한다.
        // =====================================================================
        function normalizeStockMarket(market, value = '') {
            const rawMarket = String(market || '').trim().toUpperCase();
            if (['KR', 'KOREA', 'DOMESTIC', '국내', '국내주식', '국내증시'].includes(rawMarket)) return 'KR';
            if (['US', 'USA', 'OVERSEAS', '미국', '미국주식', '해외'].includes(rawMarket)) return 'US';
            if (['CRYPTO', 'COIN', '암호화폐', '코인'].includes(rawMarket)) return 'CRYPTO';

            const q = String(value || '').trim().replace(/\s+/g, '').toUpperCase();
            if (/^\d{6}$/.test(q)) return 'KR';
            if (q.endsWith('USDT') || /BTC|ETH/.test(q) || q.includes('코인') || q.includes('비트')) return 'CRYPTO';
            if (/^[A-Z]{1,6}$/.test(q)) return 'US';
            return '';
        }

        function buildStockEntry(input, options = {}) {
            const isObject = input && typeof input === 'object' && !Array.isArray(input);
            const rawQuery = String((isObject ? (input.query ?? input.name ?? '') : input) || '').trim();
            const rawName = String((isObject ? input.name : options.name) || '').trim();
            const rawSymbol = String((isObject ? input.symbol : options.symbol) || '').trim();
            const rawMarket = isObject ? input.market : options.market;
            const source = String((isObject ? input.source : options.source) || 'unknown').trim();
            const market = normalizeStockMarket(rawMarket, rawSymbol || rawQuery || rawName);

            // 화면에 보여줄 검색어와 실제 데이터 조회용 식별자를 분리한다.
            // KR: 잘못된/오래된 symbol이 함께 들어와도 종목명이 있으면 이름으로 서버에서 표준 6자리 코드를 재확인한다.
            // US/CRYPTO: symbol/ticker를 데이터 식별자로 사용한다.
            const displayQuery = rawQuery || rawName || rawSymbol;
            let dataQuery = rawQuery || rawName || rawSymbol;
            if (market === 'KR' && rawName) {
                dataQuery = rawName;
            } else if ((market === 'US' || market === 'CRYPTO') && rawSymbol) {
                dataQuery = rawSymbol;
            }

            const name = rawName || ((rawSymbol && rawQuery && rawSymbol !== rawQuery) ? rawQuery : '');
            return {
                query: dataQuery,
                displayQuery,
                name,
                symbol: rawSymbol || dataQuery,
                market,
                source: source || 'unknown'
            };
        }

        function openStock(input, options = {}) {
            const entry = buildStockEntry(input, options);
            if (!entry.query) return;
            try { window.__GAEMIGTP_LAST_STOCK_ENTRY__ = entry; } catch (e) {}
            // query = API/분석용, displayQuery = 주소창/입력창 표시용
            doSearch(entry.query, false, entry.name || entry.displayQuery, entry.market, entry.source, entry.displayQuery);
        }
        window.openStock = openStock;

        async function doSearch(query, forceServer = false, displayNameOverride = '', marketOverride = '', sourceOverride = '', displayQueryOverride = '') {
            // 재료/경제 이벤트 최종 통합용 변수: doSearch 함수 전체에서 하나만 사용
            let materialText = '';

            // 재료/경제 이벤트 최종 통합용 변수: doSearch 함수 전체에서 하나만 사용

            const cleanQ = String(query || '').trim();
            if (!cleanQ) return;

            // 새 종목을 클릭하면 이전 분석/상세 폴링 요청을 즉시 중단한다.
            // activeSearchToken은 화면 적용 방지용이고, AbortController는 실제 HTTP 요청을 중단한다.
            if (activeSearchController) {
                try { activeSearchController.abort(); } catch (e) {}
            }
            if (activeDetailsController) {
                try { activeDetailsController.abort(); } catch (e) {}
            }
            activeSearchController = new AbortController();
            const searchSignal = activeSearchController.signal;

            // STEP 37: 우측 실시간 시장에서 종목코드로 진입하더라도
            // 목록에서 이미 확보한 한글 종목명을 상세 화면 제목에 유지한다.
            const displayNameHint = String(displayNameOverride || '').trim();

            // STEP 21: 최근검색종목 메뉴에서 사용할 최소 검색 이력을 로컬에 저장한다.
            try {
                const recent = JSON.parse(localStorage.getItem('gaemiGTP_recent_searches') || '[]');
                // STEP 40: 최근검색종목은 검색에 사용한 코드가 아니라
                // 화면에서 사용한 실제 표시명을 우선 저장한다.
                // 우측 국내/해외/코인 목록에서 종목을 클릭한 경우에는
                // displayNameOverride에 전달된 종목명을 그대로 보존한다.
                const recentDisplayName = displayNameHint || cleanQ;
                const recentSymbol = cleanQ.toUpperCase();
                const recentMarket = normalizeStockMarket(marketOverride, cleanQ) || '';
                const recentSource = String(sourceOverride || '').trim() || 'search';
                const recentItem = {
                    name: recentDisplayName,
                    symbol: recentSymbol,
                    market: recentMarket,
                    source: recentSource,
                    price: null,
                    change_pct: null,
                    currency: ''
                };
                const next = [recentItem, ...recent.filter(x => {
                    const oldSymbol = String(x?.symbol || '').toUpperCase();
                    const oldName = String(x?.name || '').toUpperCase();
                    return oldSymbol !== recentSymbol && oldName !== recentSymbol;
                })].slice(0, 15);
                localStorage.setItem('gaemiGTP_recent_searches', JSON.stringify(next));
            } catch (e) {}

            // 현재 분석 종목을 URL에 저장하되, 주소창은 사용자가 선택한 이름/티커를 유지한다.
            // 실제 API/분석 요청에는 cleanQ(표준화된 데이터 조회값)를 사용한다.
            const displayQuery = String(displayQueryOverride || displayNameOverride || cleanQ).trim();
            const searchUrl = new URL(window.location.href);
            searchUrl.searchParams.set('q', displayQuery);
            window.history.replaceState({}, '', searchUrl.toString());

            document.getElementById('centerInput').value = displayQuery;
            document.getElementById('bottomInput').value = displayQuery;

            document.getElementById('main-view').style.display = 'none';
            document.getElementById('result-scroll-view').style.display = 'block';
            document.getElementById('bottom-bar').style.display = 'flex';
            document.getElementById('loading').style.display = 'flex';
            document.getElementById('result-card-wrap').style.display = 'block';
            const fastTitle = document.getElementById('rTitle');
            const fastPrice = document.getElementById('rPrice');
            const fastMeta = document.getElementById('rMeta');
            if (fastTitle) fastTitle.textContent = displayQuery || displayNameHint || cleanQ.toUpperCase();
            if (fastPrice) fastPrice.textContent = '';
            if (fastMeta) fastMeta.textContent = '';

            if (window.innerWidth <= 900) {
                const side = document.getElementById('sidebar-wrapper');
                if (side) side.classList.remove('open');
            }

            const searchToken = ++activeSearchToken;

            // 🚀 첫 화면 차트 전용 요청: 전체 분석을 기다리지 않고 즉시 시작
            const fastChartKey = cleanQ.toUpperCase();
            const fastChartCached = FAST_CHART_CACHE.get(fastChartKey);
            const fastChartStartedAt = performance.now();
            const fastChartPromise = (
                fastChartCached && (Date.now() - fastChartCached.time) < FAST_CHART_CACHE_TTL
            )
                ? Promise.resolve(fastChartCached.data)
                : fetch('/api/chart?q=' + encodeURIComponent(cleanQ), {
                    signal: searchSignal,
                    cache: 'no-store'
                })
                    .then(r => r.ok ? r.json() : null)
                    .then(chartData => {
                        console.log(`[gaemiGTP 초고속차트] ${cleanQ} | ${(performance.now() - fastChartStartedAt).toFixed(0)}ms`);
                        if (chartData) {
                            FAST_CHART_CACHE.set(fastChartKey, {
                                time: Date.now(),
                                data: chartData
                            });
                        }
                        return chartData;
                    })
                    .catch(() => null);

            try {
                const cacheHit = getAnalyzeCache(cleanQ);
                const cached = cacheHit;
                let data;
                let response = null;

                if (!forceServer && cached) {
                    data = cached.data;
                } else {
                    const url = '/api/analyze?q=' + encodeURIComponent(cleanQ) + '&mode=core';
                    response = await fetch(url, { signal: searchSignal, cache: 'no-store' });

                    const contentType = response.headers.get("content-type") || "";
                    if (contentType.includes("application/json")) {
                        data = await response.json();
                    } else {
                        await response.text();
                        throw new Error("서버 응답 오류 (" + response.status + ")");
                    }

                    if (response.ok && data && !data.error) {
                        setAnalyzeCache(cleanQ, data);
                    }
                }

                if (!data) {
                    throw new Error("분석 데이터를 받지 못했습니다.");
                }

                if (data.error) {
                    const detail = data.detail ? ` · ${data.detail}` : '';
                    throw new Error(`${data.error}${detail}`);
                }

                // 홈 이동/다른 종목 재검색 후 도착한 오래된 응답은 화면에 적용하지 않는다.
                if (searchToken !== activeSearchToken) return;

                if (data._perf) {
                    console.log(
                        `[gaemiGTP 성능] ${cleanQ} | 전체 ${data._perf.total_ms}ms | HTTP ${data._perf.http_call_count}회`
                    );
                    console.table(data._perf.slowest_http || []);
                }

                // 🚀 차트는 core 분석 완료를 기다리지 않는다.
                if (fastChartCached && Array.isArray(fastChartCached.data?.candles) && fastChartCached.data.candles.length) {
                    setTimeout(() => {
                        if (searchToken !== activeSearchToken) return;
                        initTvChart(fastChartCached.data.candles);
                        const last = fastChartCached.data.candles[fastChartCached.data.candles.length - 1];
                        if (last && Number.isFinite(Number(last.close))) {
                            if (fastPrice) fastPrice.textContent = formatUsStockPrice(last.close);
                            if (fastMeta) fastMeta.textContent = '차트 데이터 표시 · 실시간 가격 확인 중';
                        }
                    }, 0);
                } else {
                    fastChartPromise.then(chartData => {
                        if (searchToken !== activeSearchToken || !chartData) return;
                        if (chartData?._perf) {
                            console.log(
                                `[gaemiGTP 차트 성능] ${cleanQ} | 차트 ${chartData._perf.total_ms}ms | HTTP ${chartData._perf.http_call_count}회`
                            );
                            console.table(chartData._perf.slowest_http || []);
                        }
                        if (!Array.isArray(chartData.candles) || !chartData.candles.length) return;
                        initTvChart(chartData.candles);
                        const last = chartData.candles[chartData.candles.length - 1];
                        if (last && Number.isFinite(Number(last.close))) {
                            if (fastPrice) fastPrice.textContent = formatUsStockPrice(last.close);
                            if (fastMeta) fastMeta.textContent = '차트 데이터 표시 · 실시간 가격 확인 중';
                        }
                    });
                }

                // /api/chart를 별도로 호출하지 않는다. /api/analyze가 반환한
                // 동일 candles를 바로 사용해 한 종목 클릭당 Yahoo 차트 중복 요청을 제거한다.
                if (searchToken === activeSearchToken && Array.isArray(data.candles) && data.candles.length) {
                    initTvChart(data.candles);
                }

if ((response && !response.ok) || data.error) {
                    alert(data.error || '종목을 찾을 수 없습니다.');
                    document.getElementById('loading').style.display = 'none';
                    return;
                }

                const isKrw = data.market === '국내주식' || data.market === '국내증시';
                const displayName = displayNameHint || data.name || data.formal_name;
                const displayPrice = data.price || data.price_str;
                const displayTime = data.time || data.now_str;

                // 서버가 확정한 실제 symbol/market으로 최근검색 정보를 보정한다.
                try {
                    const recent = JSON.parse(localStorage.getItem('gaemiGTP_recent_searches') || '[]');
                    const beforeSymbol = String(cleanQ || '').trim().toUpperCase();
                    const resolvedSymbol = String(data.symbol || cleanQ).trim().toUpperCase();
                    const resolvedMarket = normalizeStockMarket(data.market, resolvedSymbol || displayName);
                    const idx = recent.findIndex(x => String(x?.symbol || '').trim().toUpperCase() === beforeSymbol);
                    if (idx >= 0) {
                        recent[idx] = {
                            ...recent[idx],
                            name: displayName || recent[idx].name || resolvedSymbol,
                            symbol: resolvedSymbol || recent[idx].symbol,
                            market: resolvedMarket || recent[idx].market || '',
                            source: recent[idx].source || String(sourceOverride || 'search')
                        };
                        localStorage.setItem('gaemiGTP_recent_searches', JSON.stringify(recent));
                    }
                } catch (e) {}

                const usShortName = (isKrw || !data.symbol)
                    ? displayName
                    : getUsShortKrName(data.symbol, displayName);
                setHtml('rTitle', `${usShortName} <span>(${data.symbol})</span>`);
                setTxt('rPrice', displayPrice);
                setHtml('rMeta', `시장: ${data.market} · 출처: ${isKrw ? 'Naver/KRX' : 'Yahoo/Binance'} · 시각: ${displayTime}`);

                loadCommunityData(data.symbol, displayName);

                // 1. 증시는 지금 위젯 렌더링 (독립 위젯으로 분리)
                if (window.GaemiWidgets && window.GaemiWidgets.worldBoard) {
                    window.GaemiWidgets.worldBoard.render(data.world_board || []);
                }


                // 2. 캘린더 데이터 저장 및 렌더링
                if (data.market_calendar) {
                    globalMarketCalendar = data.market_calendar;

                    // 중요:
                    // 최초 화면 렌더링 시에는 API 데이터가 아직 도착하지 않아
                    // 경제 캘린더 자동 이동 플래그가 먼저 켜질 수 있습니다.
                    // 실제 데이터가 도착한 뒤 다시 한 번 자동 이동을 판단합니다.
                    window.globalMarketCalendar = globalMarketCalendar;
                    if (window.GaemiWidgets && window.GaemiWidgets.economicCalendar) {
                        window.GaemiWidgets.economicCalendar.resetState();
                    }
                    renderMarketCalendarView();
                }

                // 3. 퀀트 분석 데이터 렌더링 (독립 위젯)
                if (window.GaemiWidgets && window.GaemiWidgets.quant) {
                    window.GaemiWidgets.quant.render(data.quant);
                }
                // 3.6 트랙 분석 — 독립 위젯
                if (window.GaemiWidgets && window.GaemiWidgets.track) {
                    const mr = data && data.movement_reason;
                    const evidenceReasons = (mr && Array.isArray(mr.reasons) ? mr.reasons : []).slice().sort((a,b) => {
                        const a5 = a.key === 'market_5d' ? 1 : 0;
                        const b5 = b.key === 'market_5d' ? 1 : 0;
                        if (a5 !== b5) return a5 - b5;
                        return Math.abs(Number(b.score || 0)) - Math.abs(Number(a.score || 0));
                    });
                    const evidenceCount = evidenceReasons.filter(x => Number(x.score || 0) !== 0).length;
                    const confidenceStyle = mr && mr.confidence === '높음'
                        ? 'background:rgba(16,185,129,0.14);color:#10b981;border:1px solid rgba(16,185,129,0.25);'
                        : mr && mr.confidence === '중간'
                            ? 'background:rgba(245,158,11,0.14);color:#f59e0b;border:1px solid rgba(245,158,11,0.25);'
                            : 'background:rgba(148,163,184,0.12);color:var(--text-muted);border:1px solid var(--border-line);';
                    window.GaemiWidgets.track.render(data, mr, evidenceReasons, evidenceCount, confidenceStyle);
                }
                // 4. 종목 뉴스 렌더링 — 독립 위젯
                if (window.GaemiWidgets && window.GaemiWidgets.stockNews) {
                    window.GaemiWidgets.stockNews.load({
                        symbol: data.symbol, market: data.market,
                        name: data.name || data.formal_name, initial: data.stock_news
                    });
                }

                // 5. 경제뉴스 위젯 — 독립 API에서 직접 갱신
                if (window.GaemiWidgets && window.GaemiWidgets.globalNews) {
                    window.GaemiWidgets.globalNews.load({
                        market: data.market, initial: data.global_news
                    });
                }

                // 5. 애널리스트 컨센서스 — 독립 위젯
                if (window.GaemiWidgets && window.GaemiWidgets.consensus) {
                    window.GaemiWidgets.consensus.render(data.consensus);
                }

                if (window.GaemiWidgets && typeof window.GaemiWidgets.renderDartDisclosure === 'function') {
                    window.GaemiWidgets.renderDartDisclosure(data.dart_disclosures);
                }

                document.getElementById('loading').style.display = 'none';
                document.getElementById('result-card-wrap').style.display = 'block';

                // STEP 50: 첫 검색부터 전체 분석 데이터를 받아 뉴스/컨센서스/트랙 분석이 빈 상태로 남지 않게 한다.
                if (data._details_ready === false) {
                    pollAnalysisDetails(cleanQ, searchToken);
                }

                // 오른쪽 사이드바 종목 클릭 시 사용자가 보고 있던 스크롤 위치를 유지한다.
                // 자동으로 페이지 맨 위로 이동시키지 않는다.

            } catch (err) {
                if (err && err.name === 'AbortError') return;
                console.error("Analysis Error:", err);
                const msg = err && err.message ? err.message : '알 수 없는 오류';
                if (searchToken === activeSearchToken) {
                    alert('데이터를 처리하는 중 오류가 발생했습니다.\n' + msg);
                    document.getElementById('loading').style.display = 'none';
                }
            }
        }

        async function pollAnalysisDetails(query, searchToken, attempt = 0) {
            // optional 데이터(종목뉴스/해외뉴스/컨센서스/트랙 분석)가 준비될 시간을
            // 기존 4.5초보다 충분히 확보한다. 준비되면 full 재분석을 다시 돌리지 않고
            // 서버의 완성 캐시를 그대로 한 번 렌더링한다.
            if (searchToken !== activeSearchToken || attempt >= 40) return;
            if (activeDetailsController) {
                try { activeDetailsController.abort(); } catch (e) {}
            }
            activeDetailsController = new AbortController();
            const detailsSignal = activeDetailsController.signal;
            try {
                const r = await fetch('/api/analyze-details?q=' + encodeURIComponent(query) + '&wait=1', {cache: 'no-store', signal: detailsSignal});
                if (r.status === 200) {
                    const payload = await r.json();
                    if (payload && payload.ready && payload.data) {
                        if (searchToken !== activeSearchToken) return;
                        deleteAnalyzeCache(query, payload.data);
                        // 완성된 서버 데이터를 현재 화면의 모든 분석 위젯에 반영한다.
                        // 기존에는 track만 갱신되어 퀀트/거래대금/뉴스/컨센서스 등이
                        // core 단계의 빈 데이터를 그대로 유지할 수 있었다.
                        const detailData = payload.data;
                        setAnalyzeCache(query, detailData);

                        // Details가 늦게 완성되어도 현재 화면의 종목 정보 자체도
                        // 최종 서버 데이터로 맞춘다. 새로고침/재검색에서도 동일하다.
                        if (detailData.symbol || detailData.name || detailData.formal_name) {
                            const detailName = detailData.name || detailData.formal_name || query;
                            const detailIsKrw = detailData.market === '국내주식' || detailData.market === '국내증시';
                            const detailUsName = (detailIsKrw || !detailData.symbol)
                                ? detailName
                                : getUsShortKrName(detailData.symbol, detailName);
                            setHtml('rTitle', `${detailUsName} <span>(${detailData.symbol || ''})</span>`);
                            setTxt('rPrice', detailData.price || detailData.price_str || '');
                            setHtml('rMeta', `시장: ${detailData.market || '-'} · 출처: ${detailIsKrw ? 'Naver/KRX' : 'Yahoo/Binance'} · 시각: ${detailData.time || detailData.now_str || '-'}`);
                        }

                        if (window.GaemiWidgets && window.GaemiWidgets.quant) {
                            window.GaemiWidgets.quant.render(detailData.quant);
                        }
                        if (window.GaemiWidgets && window.GaemiWidgets.worldBoard) {
                            window.GaemiWidgets.worldBoard.render(detailData.world_board || []);
                        }
                        if (detailData.market_calendar) {
                            globalMarketCalendar = detailData.market_calendar;
                            window.globalMarketCalendar = globalMarketCalendar;
                            if (window.GaemiWidgets && window.GaemiWidgets.economicCalendar) {
                                window.GaemiWidgets.economicCalendar.resetState();
                            }
                            renderMarketCalendarView();
                        }

                        if (window.GaemiWidgets && window.GaemiWidgets.track) {
                            const mr = detailData && detailData.movement_reason;
                            const evidenceReasons = (mr && Array.isArray(mr.reasons) ? mr.reasons : []).slice().sort((a,b) => {
                                const a5 = a.key === 'market_5d' ? 1 : 0;
                                const b5 = b.key === 'market_5d' ? 1 : 0;
                                if (a5 !== b5) return a5 - b5;
                                return Math.abs(Number(b.score || 0)) - Math.abs(Number(a.score || 0));
                            });
                            const evidenceCount = evidenceReasons.filter(x => Number(x.score || 0) !== 0).length;
                            const confidenceStyle = mr && mr.confidence === '높음'
                                ? 'background:rgba(16,185,129,0.14);color:#10b981;border:1px solid rgba(16,185,129,0.25);'
                                : mr && mr.confidence === '중간'
                                    ? 'background:rgba(245,158,11,0.14);color:#f59e0b;border:1px solid rgba(245,158,11,0.25);'
                                    : 'background:rgba(148,163,184,0.12);color:var(--text-muted);border:1px solid var(--border-line);';
                            window.GaemiWidgets.track.render(detailData, mr, evidenceReasons, evidenceCount, confidenceStyle);
                        }
                        if (window.GaemiWidgets && window.GaemiWidgets.stockNews) {
                            window.GaemiWidgets.stockNews.load({
                                symbol: detailData.symbol, market: detailData.market,
                                name: detailData.name || detailData.formal_name, initial: detailData.stock_news
                            });
                        }
                        if (window.GaemiWidgets && window.GaemiWidgets.globalNews) {
                            window.GaemiWidgets.globalNews.load({
                                market: detailData.market, initial: detailData.global_news
                            });
                        }
                        if (window.GaemiWidgets && window.GaemiWidgets.consensus) {
                            window.GaemiWidgets.consensus.render(detailData.consensus);
                        }
                        if (window.GaemiWidgets && typeof window.GaemiWidgets.renderDartDisclosure === 'function') {
                            window.GaemiWidgets.renderDartDisclosure(detailData.dart_disclosures);
                        }
                        return;
                    }
                }
            } catch (e) {
                if (e && e.name === 'AbortError') return;
            }
            if (searchToken === activeSearchToken) {
                setTimeout(() => pollAnalysisDetails(query, searchToken, attempt + 1), 500);
            }
        }

        function formatUsStockPrice(value) {
            const n = Number(value);
            if (!Number.isFinite(n)) return '-';
            if (n >= 1000) return '$' + n.toLocaleString('en-US', {maximumFractionDigits: 0});
            if (n >= 100) return '$' + n.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2});
            return '$' + n.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2});
        }

        function buildUsStockSparkline(values) {
            if (!Array.isArray(values) || values.length < 2) return '';
            const safe = values
                .map(Number)
                .filter(v => Number.isFinite(v));
            if (safe.length < 2) return '';

            const width = 120;
            const height = 34;
            const padX = 2;
            const padY = 3;
            const lo = Math.min(...safe);
            const hi = Math.max(...safe);
            const span = hi - lo || 1;
            const step = (width - padX * 2) / Math.max(1, safe.length - 1);

            const points = safe.map((value, i) => {
                const x = padX + i * step;
                const y = (height - padY) - ((value - lo) / span) * (height - padY * 2);
                return `${x.toFixed(1)},${y.toFixed(1)}`;
            }).join(' ');

            return `<svg class="us-leading-stock-spark" viewBox="0 0 120 34" preserveAspectRatio="none" aria-label="최근 가격 흐름">
                        <polyline points="${points}"></polyline>
                    </svg>`;
        }


        // STEP 36: 해외 거래대금 상위 종목명을 한글로 표시한다.
        const US_SHORT_KR_NAMES = {
            // 반도체 / AI
            NVDA: '엔비디아', AVGO: '브로드컴', AMD: 'AMD', MU: '마이크론',
            MRVL: '마벨테크놀로지', QCOM: '퀄컴', TSM: 'TSMC', LRCX: '램리서치',
            AMAT: '어플라이드머티어리얼즈', KLAC: 'KLA', ASML: 'ASML', ARM: 'ARM',
            INTC: '인텔', TXN: '텍사스인스트루먼트', ON: '온세미',
            MPWR: '모놀리식파워', ADI: '아날로그디바이스', NXPI: 'NXP반도체',
            SMCI: '슈퍼마이크로컴퓨터', WDC: '웨스턴디지털', STX: '씨게이트',
           
            // 빅테크 / 플랫폼
            TSLA: '테슬라', AAPL: '애플', MSFT: '마이크로소프트', AMZN: '아마존',
            GOOGL: '구글', GOOG: '구글', META: '메타', NFLX: '넷플릭스',
            ORCL: '오라클', CRM: '세일즈포스', ADBE: '어도비', NOW: '서비스나우',
            PLTR: '팔란티어', UBER: '우버', ABNB: '에어비앤비', SHOP: '쇼피파이',
           
            // 금융 / 결제
            JPM: 'JP모건', BAC: '뱅크오브아메리카', WFC: '웰스파고', C: '씨티그룹',
            GS: '골드만삭스', MS: '모건스탠리', V: '비자', MA: '마스터카드',
            COIN: '코인베이스', HOOD: '로빈후드', MSTR: '마이크로스트래티지',
           
            // 전기차 / 자동차
            RIVN: '리비안', LCID: '루시드', NIO: '니오', F: '포드', GM: '제너럴모터스',
           
            // 에너지 / 산업
            XOM: '엑손모빌', CVX: '셰브론', COP: '코노코필립스', SLB: '슐럼버거',
            OXY: '옥시덴털페트롤리엄', CAT: '캐터필러', DE: '디어', GE: 'GE에어로스페이스',
            BA: '보잉', RTX: 'RTX', LMT: '록히드마틴',
           
            // 헬스케어 / 바이오
            LLY: '일라이릴리', NVO: '노보노디스크', UNH: '유나이티드헬스', JNJ: '존슨앤드존슨',
            PFE: '화이자', MRK: '머크', ABBV: '애브비', MRNA: '모더나',
           
            // 소비 / 미디어
            WMT: '월마트', COST: '코스트코', MCD: '맥도날드', NKE: '나이키',
            SBUX: '스타벅스', DIS: '디즈니', KO: '코카콜라', PEP: '펩시코',
           
            // 주요 ETF
            QQQ: '나스닥100 ETF', SPY: 'S&P500 ETF', SOXL: '반도체 3배 ETF',
            TQQQ: '나스닥100 3배 ETF', DIA: '다우존스 ETF', SOXX: '반도체 ETF',
           
            // 기타 자주 조회되는 종목
            TSMC: 'TSMC', PDD: '핀둬둬', BABA: '알리바바', JD: '징둥닷컴',
            BIDU: '바이두', IONQ: '아이온큐', RKLB: '로켓랩', ASTS: 'AST스페이스모바일'
        };

        function getUsShortKrName(symbol, fallback) {
            const key = String(symbol || '').trim().toUpperCase();
            return US_SHORT_KR_NAMES[key] || String(fallback || key);
        }

        function openUsLeadingStock(symbol, name) {
            const cleanSymbol = String(symbol || '').trim();
            const cleanName = getUsShortKrName(cleanSymbol, name);
            if (!cleanSymbol) return;

            openStock({
                symbol: cleanSymbol,
                name: cleanName,
                market: 'US',
                source: 'us-leading'
            });
        }

        async function loadSectorChart(sectorSymbol, sectorName) {
            const symbol = String(sectorSymbol || '').trim().toUpperCase();
            const name = String(sectorName || '').trim();
            if (!symbol) return;

            const title = document.getElementById('rTitle');
            const price = document.getElementById('rPrice');
            const meta = document.getElementById('rMeta');

            if (title) title.textContent = name || symbol;
            if (price) price.textContent = '업종 흐름';
            if (meta) meta.textContent = '미국 주도업종 · 실제 Yahoo 일봉 데이터';

            try {
                const response = await fetch('/api/chart?q=' + encodeURIComponent(symbol), {
                    cache: 'no-store'
                });
                const data = await response.json().catch(() => null);
                if (!response.ok || !data || data.error) {
                    throw new Error((data && data.error) || ('차트 데이터 오류 (' + response.status + ')'));
                }

                if (!Array.isArray(data.candles) || !data.candles.length) {
                    throw new Error('업종 차트 데이터가 없습니다.');
                }

                initTvChart(data.candles);
                if (price) price.textContent = '업종 흐름';
                if (meta) meta.textContent = `${name || symbol} · 최근 65거래일`;
            } catch (error) {
                console.error('US sector chart:', error);
                if (price) price.textContent = '업종 차트를 불러오지 못했습니다.';
                if (meta) meta.textContent = error && error.message ? error.message : '잠시 후 다시 시도해주세요.';
            }
        }

        // 미국 주도종목 위젯은 widgets/us_leading_stocks.js에서 담당한다.
        // 기존 전역 함수명은 호환성을 위해 유지한다.
        async function loadUsLeadingStocks(sectorSymbol, sectorName) {
            if (window.GaemiWidgets && window.GaemiWidgets.usLeadingStocks) {
                return window.GaemiWidgets.usLeadingStocks.load(sectorSymbol, sectorName);
            }
            return null;
        }

        async function loadMarketCalendar() {
            try {
                const response = await fetch('/api/market-calendar');
                const data = await response.json().catch(() => null);
                if (!response.ok || !data || data.error) throw new Error((data && data.error) || '시장 캘린더 데이터 오류');
                if (Array.isArray(data.data)) {
                    globalMarketCalendar = data.data;
                    window.globalMarketCalendar = globalMarketCalendar;
                    if (window.GaemiWidgets && window.GaemiWidgets.economicCalendar) {
                        window.GaemiWidgets.economicCalendar.resetState();
                    }
                    renderMarketCalendarView();
                }
            } catch (error) {
                console.error('Market calendar:', error);
            }
        }


        // STEP 4: 무거운 세계시장/캘린더 데이터는 분석 요청과 분리해서 병렬 로딩한다.
        window.addEventListener('DOMContentLoaded', () => {
            if (window.GaemiWidgets && window.GaemiWidgets.worldBoard) {
                window.GaemiWidgets.worldBoard.load(false);
            }
            if (window.GaemiWidgets && window.GaemiWidgets.economicCalendar) {
                window.GaemiWidgets.economicCalendar.load(false);
            } else {
                loadMarketCalendar();
            }
            loadUsLeadingSectors();
        });
        window.addEventListener('DOMContentLoaded', () => {
            initTheme();

            // 새로고침 시 URL에 저장된 종목을 자동 복원
            const params = new URLSearchParams(window.location.search);
            const savedQuery = params.get('q');

            if (savedQuery && savedQuery.trim()) {
                openStock({ query: savedQuery.trim(), source: 'url-restore' });
            }
        });
    </script>


    <!-- STEP 27: 우측 실시간 시장 사이드바 -->
    <aside id="right-market-rail" class="collapsed" aria-label="실시간 시장 사이드바">
        <section id="right-market-panel">
            <div class="right-market-head">
                <div><span class="right-market-title">실시간</span><span class="right-market-time" id="rightMarketTime">오늘 --:-- 기준</span></div>
            </div>
            <div class="right-market-tabs">
                <button class="right-market-tab active" data-market="kr" onclick="switchRightMarket('kr')">국내</button>
                <button class="right-market-tab" data-market="us" onclick="switchRightMarket('us')">해외</button>
                <button class="right-market-tab" data-market="crypto" onclick="switchRightMarket('crypto')">코인</button>
            </div>
            <div class="right-market-sort"><span id="rightMarketSortLabel">거래대금 상위⌄</span><span>실시간⌄</span></div>
            <div class="right-market-list" id="rightMarketList"><div class="right-market-empty">실시간 시장 데이터를 불러오는 중입니다.</div></div>
        </section>
        <nav id="right-market-icons" aria-label="시장 메뉴">
            <button class="right-market-icon-btn right-market-close" onclick="toggleRightMarket()" title="오른쪽 사이드바 열기/닫기">»</button>
            <button class="right-market-icon-btn active" data-right-menu="live" onclick="selectRightMenu('live')" title="실시간">◉</button>
            <button class="right-market-icon-btn" data-right-menu="favorite" onclick="selectRightMenu('favorite')" title="관심">♡</button>
            <button class="right-market-icon-btn" data-right-menu="recent" onclick="selectRightMenu('recent')" title="최근검색종목">◷</button>
            <button class="right-market-icon-btn" data-right-menu="leaders" onclick="selectRightMenu('leaders')" title="실시간 상단">▥</button>
        </nav>
    </aside>

</body>
</html>
"""

# right_market.js는 widgets/ 폴더의 실제 파일을 직접 서빙한다.
# ----------------------------------------------------
FAST_CHART_PAYLOAD_CACHE = {}
FAST_CHART_PAYLOAD_TTL = 30

# 차트 전용 초고속 경로
# 전체 분석(/api/analyze)이 끝날 때까지 차트를 기다리지 않게 한다.
# ----------------------------------------------------




def run_fast_chart_pipeline(query):
    clean_q = str(query or "").strip().replace(" ", "").upper()
    if not clean_q:
        return None

    cached_payload = FAST_CHART_PAYLOAD_CACHE.get(clean_q)
    if cached_payload and (time.time() - cached_payload["time"]) < FAST_CHART_PAYLOAD_TTL:
        return dict(cached_payload["data"])

    def _finish(df):
        payload = _build_chart_payload(df) if df is not None else None
        if payload:
            FAST_CHART_PAYLOAD_CACHE[clean_q] = {
                "time": time.time(),
                "data": dict(payload),
            }
        return payload

    # 글로벌 지수
    if clean_q in GLOBAL_INDEX_MAP:
        symbol = GLOBAL_INDEX_MAP[clean_q]
        df = _fetch_fast_overseas_chart(symbol)
        return _finish(df)

    # 암호화폐
    if clean_q.endswith("USDT") or clean_q in CRYPTO_MAP or any(c in clean_q for c in ["BTC", "ETH", "코인", "비트"]):
        symbol = CRYPTO_MAP.get(clean_q, clean_q if clean_q.endswith("USDT") else f"{clean_q}USDT")
        try:
            _, _, df = fetch_crypto_candles(symbol)
            return _finish(df)
        except Exception:
            return None

    # 미국주식
    if clean_q in US_STOCK_MAP:
        symbol = US_STOCK_MAP[clean_q]
        df = _fetch_fast_overseas_chart(symbol)
        return _finish(df)

    # STEP 11: 영문 티커는 소문자로 입력해도 Naver 자동완성으로 가지 않는다.
    if US_TICKER_PATTERN.fullmatch(clean_q or ""):
        symbol = clean_q
        df = _fetch_fast_overseas_chart(symbol)
        return _build_chart_payload(df) if df is not None else None

    # 국내주식
    code, _ = find_stock_code(query)
    if not code:
        return None
    df = _fetch_fast_domestic_chart(code)
    return _build_chart_payload(df) if df is not None else None


# ----------------------------------------------------
# 9. 멀티스레드 웹 서버
# ----------------------------------------------------
class WebHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        print("WEB", fmt % args)

    def _client_accepts_gzip(self):
        return "gzip" in str(self.headers.get("Accept-Encoding", "")).lower()

    def _write_bytes_response(self, status, body, content_type, compress=True, cache_control="no-store"):
        perf_headers = getattr(self, "_perf_headers", None) or {}
        send_gzip = bool(compress and len(body) >= 1024 and self._client_accepts_gzip())
        wire_body = gzip.compress(body, compresslevel=6, mtime=0) if send_gzip else body

        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", cache_control)
        if send_gzip:
            self.send_header("Content-Encoding", "gzip")
        for header_name, header_value in perf_headers.items():
            self.send_header(header_name, str(header_value))
        self.send_header("Content-Length", str(len(wire_body)))
        self.end_headers()
        self._perf_headers = {}
        self.wfile.write(wire_body)

    def _json(self, status, payload):
        clean_payload = clean_for_json(payload)
        if isinstance(clean_payload, dict) and isinstance(clean_payload.get("_perf"), dict):
            perf = clean_payload.get("_perf") or {}
            self._perf_headers = {
                "X-Gaemi-Response-Time-Ms": perf.get("total_ms", ""),
                "X-Gaemi-HTTP-Calls": perf.get("http_call_count", ""),
            }
            stages = perf.get("stages") or {}
            if stages:
                # Server-Timing은 브라우저 Network/Timing 화면에서도 단계별 시간을 확인할 수 있다.
                self._perf_headers["Server-Timing"] = ", ".join(
                    f"{str(k).replace('-', '_')};dur={v}" for k, v in stages.items() if v is not None
                )[:1800]
        # STEP 6: JSON 구분자 공백을 제거하고 gzip을 적용해 네트워크 전송량을 줄인다.
        body = json.dumps(
            clean_payload,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        self._write_bytes_response(
            status,
            body,
            "application/json; charset=utf-8",
            compress=True,
            cache_control="no-store",
        )

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)

        if parsed.path == "/health":
            self._json(200, {"ok": True, "telegram_configured": bool(TELEGRAM_TOKEN)})
            return

        if parsed.path == "/widgets/dart_disclosure.js":
            body = DART_DISCLOSURE_WIDGET_JS.encode("utf-8")
            self._write_bytes_response(200, body, "application/javascript; charset=utf-8", compress=False, cache_control="no-cache")
            return

        # 독립 위젯 정적 리소스
        if parsed.path == "/widgets/world_board.js":
            widget_path = os.path.join(os.path.dirname(__file__), "widgets", "world_board.js")
            try:
                with open(widget_path, "rb") as f:
                    payload = f.read()
                self._write_bytes_response(200, payload, "application/javascript; charset=utf-8", compress=True, cache_control="no-cache")
            except OSError:
                self._json(404, {"error": "위젯 리소스를 찾을 수 없습니다."})
            return

        if parsed.path == "/widgets/us_leading_sectors.js":
            widget_path = os.path.join(os.path.dirname(__file__), "widgets", "us_leading_sectors.js")
            try:
                with open(widget_path, "rb") as f:
                    payload = f.read()
                self._write_bytes_response(200, payload, "application/javascript; charset=utf-8", compress=True, cache_control="no-cache")
            except OSError:
                self._json(404, {"error": "위젯 리소스를 찾을 수 없습니다."})
            return

        if parsed.path == "/widgets/us_leading_stocks.js":
            widget_path = os.path.join(os.path.dirname(__file__), "widgets", "us_leading_stocks.js")
            try:
                with open(widget_path, "rb") as f:
                    payload = f.read()
                self._write_bytes_response(200, payload, "application/javascript; charset=utf-8", compress=True, cache_control="no-cache")
            except OSError:
                self._json(404, {"error": "위젯 리소스를 찾을 수 없습니다."})
            return


        if parsed.path == "/widgets/track.js":
            widget_path = os.path.join(os.path.dirname(__file__), "widgets", "track.js")
            try:
                with open(widget_path, "rb") as f:
                    payload = f.read()
                self._write_bytes_response(200, payload, "application/javascript; charset=utf-8", compress=True, cache_control="no-cache")
            except OSError:
                self._json(404, {"error": "위젯 리소스를 찾을 수 없습니다."})
            return

        if parsed.path == "/widgets/earnings_calendar.js":
            widget_path = os.path.join(os.path.dirname(__file__), "widgets", "earnings_calendar.js")
            try:
                with open(widget_path, "rb") as f:
                    payload = f.read()
                self._write_bytes_response(200, payload, "application/javascript; charset=utf-8", compress=True, cache_control="no-cache")
            except OSError:
                self._json(404, {"error": "위젯 리소스를 찾을 수 없습니다."})
            return

        if parsed.path == "/widgets/quant.js":
            widget_path = os.path.join(os.path.dirname(__file__), "widgets", "quant.js")
            try:
                with open(widget_path, "rb") as f:
                    payload = f.read()
                self._write_bytes_response(200, payload, "application/javascript; charset=utf-8", compress=True, cache_control="no-cache")
            except OSError:
                self._json(404, {"error": "위젯 리소스를 찾을 수 없습니다."})
            return

        if parsed.path == "/widgets/right_market.js":
            widget_path = os.path.join(os.path.dirname(__file__), "widgets", "right_market.js")
            try:
                with open(widget_path, "rb") as f:
                    payload = f.read()
                self._write_bytes_response(200, payload, "application/javascript; charset=utf-8", compress=True, cache_control="no-cache")
            except OSError:
                self._json(404, {"error": "위젯 리소스를 찾을 수 없습니다."})
            return
        if parsed.path == "/widgets/stock_news.js":
            widget_path = os.path.join(os.path.dirname(__file__), "widgets", "stock_news.js")
            try:
                with open(widget_path, "rb") as f:
                    payload = f.read()
                self._write_bytes_response(200, payload, "application/javascript; charset=utf-8", compress=True, cache_control="no-cache")
            except OSError:
                self._json(404, {"error": "위젯 리소스를 찾을 수 없습니다."})
            return

        if parsed.path == "/widgets/global_news.js":
            widget_path = os.path.join(os.path.dirname(__file__), "widgets", "global_news.js")
            try:
                with open(widget_path, "rb") as f:
                    payload = f.read()
                self._write_bytes_response(200, payload, "application/javascript; charset=utf-8", compress=True, cache_control="no-cache")
            except OSError:
                self._json(404, {"error": "위젯 리소스를 찾을 수 없습니다."})
            return

        if parsed.path == "/widgets/economic_calendar.js":
            widget_path = os.path.join(os.path.dirname(__file__), "widgets", "economic_calendar.js")
            try:
                with open(widget_path, "rb") as f:
                    payload = f.read()
                self._write_bytes_response(200, payload, "application/javascript; charset=utf-8", compress=True, cache_control="no-cache")
            except OSError:
                self._json(404, {"error": "위젯 리소스를 찾을 수 없습니다."})
            return


        if parsed.path == "/widgets/chart.js":
            widget_path = os.path.join(os.path.dirname(__file__), "widgets", "chart.js")
            try:
                with open(widget_path, "rb") as f:
                    payload = f.read()
                self._write_bytes_response(200, payload, "application/javascript; charset=utf-8", compress=True, cache_control="no-cache")
            except OSError:
                self._json(404, {"error": "위젯 리소스를 찾을 수 없습니다."})
            return

        if parsed.path == "/style.css":
            css_path = os.path.join(os.path.dirname(__file__), "style.css")
            try:
                with open(css_path, "rb") as f:
                    payload = f.read()
                self._write_bytes_response(200, payload, "text/css; charset=utf-8", compress=True, cache_control="no-cache")
            except OSError:
                self._json(404, {"error": "스타일시트를 찾을 수 없습니다."})
            return

        if parsed.path == "/widgets/analyst_consensus.js":
            widget_path = os.path.join(os.path.dirname(__file__), "widgets", "analyst_consensus.js")
            try:
                with open(widget_path, "rb") as f:
                    payload = f.read()
                self._write_bytes_response(200, payload, "application/javascript; charset=utf-8", compress=True, cache_control="no-cache")
            except OSError:
                self._json(404, {"error": "위젯 리소스를 찾을 수 없습니다."})
            return
        if parsed.path == "/widgets/comment.js":
            widget_path = os.path.join(os.path.dirname(__file__), "widgets", "comment.js")
            try:
                with open(widget_path, "rb") as f:
                    payload = f.read()
                self._write_bytes_response(200, payload, "application/javascript; charset=utf-8", compress=True, cache_control="no-cache")
            except OSError:
                self._json(404, {"error": "위젯 리소스를 찾을 수 없습니다."})
            return

        if parsed.path == "/widgets/vote.js":
            widget_path = os.path.join(os.path.dirname(__file__), "widgets", "vote.js")
            try:
                with open(widget_path, "rb") as f:
                    payload = f.read()
                self._write_bytes_response(200, payload, "application/javascript; charset=utf-8", compress=True, cache_control="no-cache")
            except OSError:
                self._json(404, {"error": "위젯 리소스를 찾을 수 없습니다."})
            return

        if parsed.path == "/api/right-market-board":
            try:
                market = str(params.get("market", ["kr"])[0]).strip().lower()
                force = str(params.get("force", ["0"])[0]).strip().lower() in {"1", "true", "yes"}
                rows = fetch_right_market_board(market=market, force=force)
                self._json(200, {"ok": True, "market": market, "data": rows})
            except Exception as exc:
                print(f"API ERROR /api/right-market-board: {type(exc).__name__}: {exc}")
                self._json(500, {"ok": False, "error": "실시간 시장 데이터를 가져오지 못했습니다."})
            return

        if parsed.path == "/api/world-board":
            try:
                force = str(params.get("force", ["0"])[0]).strip().lower() in {"1", "true", "yes"}
                rows = fetch_all_world_board_parallel(force=force)
                self._json(200, {"ok": True, "data": rows})
            except Exception as exc:
                print(f"API ERROR /api/world-board: {type(exc).__name__}: {exc}")
                self._json(500, {"ok": False, "error": "세계시장 데이터를 가져오지 못했습니다."})
            return

        if parsed.path == "/api/stock-news":
            try:
                symbol = str(params.get("symbol", [""])[0]).strip().upper()
                market_name = str(params.get("market", [""])[0]).strip()
                formal_name = str(params.get("name", [""])[0]).strip()
                limit = int(params.get("limit", ["10"])[0])
                rows = fetch_google_stock_news(symbol, market_name, formal_name, max(1, min(limit, 20)))
                self._json(200, {"ok": True, "data": rows})
            except Exception as exc:
                print(f"API ERROR /api/stock-news: {type(exc).__name__}: {exc}")
                self._json(500, {"ok": False, "error": "종목 뉴스를 가져오지 못했습니다."})
            return

        if parsed.path == "/api/global-news":
            try:
                market_name = str(params.get("market", ["국내주식"])[0]).strip() or "국내주식"
                limit = int(params.get("limit", ["10"])[0])
                rows = fetch_google_economic_news(market_name, max(1, min(limit, 20)))
                self._json(200, {"ok": True, "data": rows})
            except Exception as exc:
                print(f"API ERROR /api/global-news: {type(exc).__name__}: {exc}")
                self._json(500, {"ok": False, "error": "경제뉴스를 가져오지 못했습니다."})
            return

        if parsed.path == "/api/market-calendar":
            try:
                force = str(params.get("force", ["0"])[0]).strip().lower() in {"1", "true", "yes"}
                if force:
                    global LAST_CALENDAR_FETCH_TIME
                    LAST_CALENDAR_FETCH_TIME = 0
                rows = fetch_real_market_calendar_live()
                self._json(200, {"ok": True, "data": rows})
            except Exception as exc:
                print(f"API ERROR /api/market-calendar: {type(exc).__name__}: {exc}")
                self._json(500, {"ok": False, "error": "시장 캘린더 데이터를 가져오지 못했습니다."})
            return

        if parsed.path == "/api/us-leading-stocks":
            try:
                sector_symbol = str(params.get("sector", [""])[0]).strip().upper()
                if not sector_symbol:
                    self._json(400, {"ok": False, "error": "sector가 필요합니다."})
                    return
                rows = fetch_us_leading_stocks(sector_symbol)
                self._json(200, {"ok": True, "sector": sector_symbol, "data": rows})
            except Exception as exc:
                print(f"API ERROR /api/us-leading-stocks: {type(exc).__name__}: {exc}")
                self._json(500, {"ok": False, "error": "미국 주도종목 데이터를 가져오지 못했습니다."})
            return

        if parsed.path == "/api/us-leading-sectors":
            try:
                rows = fetch_us_leading_sectors()
                self._json(200, {"ok": True, "data": rows})
            except Exception as exc:
                print(f"API ERROR /api/us-leading-sectors: {type(exc).__name__}: {exc}")
                self._json(500, {"ok": False, "error": "미국 업종 데이터를 가져오지 못했습니다."})
            return

        if parsed.path == "/api/performance":
            try:
                try:
                    limit = int(params.get("limit", ["20"])[0])
                except Exception:
                    limit = 20
                perf_data = _perf_history_snapshot(limit)
                self._json(200, {"ok": True, "data": perf_data})
            except Exception as exc:
                self._json(500, {"ok": False, "error": "성능 데이터를 가져오지 못했습니다.", "detail": str(exc)[:300]})
            return

        if parsed.path == "/api/chart":
            q = str(params.get("q", [""])[0]).strip()
            if not q:
                self._json(400, {"error": "종목명을 입력해주세요."})
                return
            _perf_begin("chart", q)
            try:
                chart_data = run_fast_chart_pipeline(q)
                if not chart_data:
                    perf = _perf_snapshot()
                    print(f"[PERF][chart] {q}: {perf}")
                    self._json(404, {"error": "차트 데이터를 가져오지 못했습니다.", "_perf": perf})
                    return
                chart_data["_perf"] = _perf_snapshot()
                print(f"[PERF][chart] {q}: {chart_data['_perf']}")
                self._json(200, chart_data)
            except Exception as exc:
                perf = _perf_snapshot()
                print(f"[PERF][chart] {q}: {perf}")
                self._json(500, {"error": "차트 데이터를 가져오지 못했습니다.", "_perf": perf})
            return

        if parsed.path == "/api/analyze-details":
            q = str(params.get("q", [""])[0]).strip()
            if not q:
                self._json(400, {"ok": False, "error": "종목명을 입력해주세요."})
                return
            clean_q = q.replace(" ", "").upper()
            # 클라이언트가 짧게 여러 번 폴링하지 않도록, 한 번의 요청에서
            # 최대 8초 동안 enrichment 완료 여부를 확인한다.
            try:
                should_wait = str(params.get("wait", ["0"])[0]).strip().lower() in {"1", "true", "yes"}
            except Exception:
                should_wait = False
            deadline = time.monotonic() + (12.0 if should_wait else 0.0)
            while True:
                cached = None
                for candidate in _analysis_cache_keys(clean_q):
                    hit = ANALYSIS_CACHE.get(candidate)
                    if hit:
                        hit_time, hit_data = hit
                        if time.time() - hit_time < ANALYSIS_CORE_TTL:
                            cached = (hit_time, hit_data)
                            break
                if cached:
                    cached_time, cached_data = cached
                    if cached_data.get("_details_ready"):
                        self._json(200, {"ok": True, "ready": True, "data": cached_data})
                        return
                if time.monotonic() >= deadline:
                    break
                time.sleep(0.2)
            self._json(202, {"ok": True, "ready": False})
            return

        if parsed.path == "/api/analyze":
            q = str(params.get("q", [""])[0]).strip()
            if not q:
                self._json(400, {"error": "종목명을 입력해주세요."})
                return
            mode = str(params.get("mode", ["full"])[0]).strip().lower()
            lightweight = mode == "core"
            _perf_begin("analyze", q)
            try:
                data = run_full_pipeline(q, lightweight=lightweight)
                perf = _perf_snapshot()
                if not data:
                    print(f"[PERF][analyze] {q}: {perf}")
                    self._json(404, {"error": "실제 데이터를 가져오지 못했습니다. 종목명 또는 데이터 제공처를 확인해주세요.", "_perf": perf})
                    return
                data["_perf"] = perf
                print(f"[PERF][analyze] {q}: {perf}")
                self._json(200, data)
            except Exception as exc:
                perf = _perf_snapshot()
                print(f"[PERF][analyze] {q}: {perf}")
                self._json(500, {
                    "error": "서버 처리 중 오류가 발생했습니다.",
                    "detail": f"{type(exc).__name__}: {str(exc)[:400]}",
                    "_perf": perf
                })
            return

        # STEP 6: 초기 HTML도 gzip으로 전송해 첫 화면 로딩 비용을 줄인다.
        html_bytes = HTML_PAGE.encode("utf-8")
        self._write_bytes_response(
            200,
            html_bytes,
            "text/html; charset=utf-8",
            compress=True,
            cache_control="no-store",
        )

def run_web_server():
    port = int(os.environ.get("PORT", 10000))
    server = ThreadingHTTPServer(("0.0.0.0", port), WebHandler)
    print(f"gaemiGTP listening on :{port}")
    server.serve_forever()

if __name__ == "__main__":
    run_web_server()


# ============================================================
# 개미GTP 고정 코멘트 로직 - 재료 변수형 테스트
# 경제 이벤트 문장은 추후 별도 규칙 확정 후 추가
# ============================================================
