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



# DB layer (kept behavior-equivalent; separated from the main server module)
from db import (
    init_database,
    save_market_prices,
    get_db_health,
    get_db_price_summary,
    get_db_recent_prices,
    _submit_db_write,
)

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
from news_sources import (fetch_naver_news, fetch_finlight_news, translate_news_titles_to_korean, fetch_finlight_stock_news, fetch_finlight_economic_news, fetch_google_stock_news, fetch_google_economic_news, fetch_rss_news)
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


# ----------------------------------------------------
# CANONICAL INSTRUMENT RESOLUTION
# ----------------------------------------------------
# 종목 선택 경로가 달라도 최종적으로 동일한 시장/종목코드를 사용한다.
# key 예: KR:005930 / US:NVDA / CRYPTO:BTCUSDT / INDEX:^KS11
INSTRUMENT_RESOLUTION_CACHE = {}
INSTRUMENT_RESOLUTION_CACHE_TTL = 3600

# 동적 종목 마스터: 고정 목록이 아니라 검색/사이드바/향후 증권사 API가 발견한
# 종목을 자동 등록한다. 분석 가능 여부를 이 목록으로 제한하지 않는다.
INSTRUMENT_REGISTRY = {}
INSTRUMENT_REGISTRY_LOCK = threading.Lock()

def register_instrument(instrument):
    if not isinstance(instrument, dict):
        return None
    key = str(instrument.get('key') or '').strip().upper()
    if not key:
        return None
    item = dict(instrument)
    item['key'] = key
    item['symbol'] = str(item.get('symbol') or '').strip().upper()
    item['market'] = _normalize_market_hint(item.get('market')) or item.get('market')
    with INSTRUMENT_REGISTRY_LOCK:
        INSTRUMENT_REGISTRY[key] = dict(item)
    return item

def register_dynamic_instrument(market, symbol, name=''):
    symbol = str(symbol or '').strip().upper()
    market = _normalize_market_hint(market)
    if not symbol or not market:
        return None
    market_names = {'KR':'국내주식','US':'미국주식','CRYPTO':'암호화폐','INDEX':'글로벌증시'}
    return register_instrument({
        'key': instrument_key(market, symbol),
        'market': market,
        'market_name': market_names.get(market, market),
        'symbol': symbol,
        'name': str(name or '').strip() or symbol,
        'market_index_name': None,
        'input': symbol,
    })

def _normalize_market_hint(value):
    v = str(value or '').strip().lower()
    return {
        'kr': 'KR', 'korea': 'KR', '국내': 'KR', '국내주식': 'KR', '국내증시': 'KR',
        'us': 'US', 'usa': 'US', '미국': 'US', '미국주식': 'US',
        'crypto': 'CRYPTO', 'coin': 'CRYPTO', '코인': 'CRYPTO', '암호화폐': 'CRYPTO',
        'index': 'INDEX', '지수': 'INDEX', '글로벌증시': 'INDEX', '외환': 'INDEX', '원자재': 'INDEX',
    }.get(v, str(value or '').strip().upper() or None)


def _global_index_metadata(symbol):
    symbol = str(symbol or '').strip().upper()
    if symbol in ('^KS11', '^KQ11'):
        return 'INDEX', '국내증시', 'KOSPI' if symbol == '^KS11' else 'KOSDAQ'
    if symbol == 'KRW=X':
        return 'INDEX', '외환', None
    if symbol in ('CL=F', 'GC=F'):
        return 'INDEX', '원자재', None
    return 'INDEX', '글로벌증시', None


def instrument_key(market, symbol):
    market_code = _normalize_market_hint(market) or 'US'
    symbol = str(symbol or '').strip().upper()
    if market_code == 'CRYPTO' and symbol and not symbol.endswith('USDT'):
        symbol = f'{symbol}USDT'
    return f'{market_code}:{symbol}'


def resolve_instrument(query='', market_hint=None, symbol_hint=None, name_hint=None):
    """모든 종목 진입점을 하나의 canonical instrument로 정규화한다."""
    raw = str(query or '').strip()
    hint_market = _normalize_market_hint(market_hint)
    hint_symbol = str(symbol_hint or '').strip().upper()
    hint_name = str(name_hint or '').strip()
    lookup = (hint_symbol or raw).replace(' ', '').strip().upper()
    if not lookup:
        return None

    # 서버/프론트가 canonical key를 그대로 전달하는 경우
    if ':' in lookup:
        pref, sym = lookup.split(':', 1)
        pref = _normalize_market_hint(pref)
        if pref in {'KR', 'US', 'CRYPTO', 'INDEX'} and sym:
            if pref == 'CRYPTO' and not sym.endswith('USDT'):
                sym = f'{sym}USDT'
            if pref == 'INDEX':
                kind, market_name, index_name = _global_index_metadata(sym)
                return register_instrument({'key': instrument_key('INDEX', sym), 'market': kind, 'market_name': market_name,
                        'symbol': sym, 'name': hint_name or DOMESTIC_CODE_NAME_MAP.get(sym) or US_SYMBOL_NAME_MAP.get(sym) or CRYPTO_SYMBOL_NAME_MAP.get(sym) or sym, 'market_index_name': index_name, 'input': raw})
            return register_instrument({'key': instrument_key(pref, sym), 'market': pref,
                    'market_name': {'KR':'국내주식','US':'미국주식','CRYPTO':'암호화폐'}[pref],
                    'symbol': sym, 'name': hint_name or (DOMESTIC_CODE_NAME_MAP.get(sym) if pref == 'KR' else US_SYMBOL_NAME_MAP.get(sym) if pref == 'US' else CRYPTO_SYMBOL_NAME_MAP.get(sym)) or sym, 'market_index_name': None, 'input': raw})

    cached = INSTRUMENT_RESOLUTION_CACHE.get(lookup)
    if cached and time.time() - cached[0] < INSTRUMENT_RESOLUTION_CACHE_TTL:
        return register_instrument(dict(cached[1]))

    # 명시적 market hint가 있으면 hint를 우선 적용한다.
    if hint_market == 'KR' and (lookup.isdigit() and len(lookup) == 6):
        result = {'key': instrument_key('KR', lookup), 'market': 'KR', 'market_name': '국내주식',
                  'symbol': lookup, 'name': hint_name or DOMESTIC_CODE_NAME_MAP.get(lookup, lookup), 'market_index_name': None, 'input': raw}
    elif hint_market == 'US' and re.fullmatch(r'[A-Z0-9.\-]{1,8}', lookup):
        result = {'key': instrument_key('US', lookup), 'market': 'US', 'market_name': '미국주식',
                  'symbol': lookup, 'name': hint_name or US_STOCK_MAP.get(lookup, lookup), 'market_index_name': None, 'input': raw}
    elif hint_market == 'CRYPTO':
        sym = lookup if lookup.endswith('USDT') else CRYPTO_MAP.get(lookup, f'{lookup}USDT')
        result = {'key': instrument_key('CRYPTO', sym), 'market': 'CRYPTO', 'market_name': '암호화폐',
                  'symbol': sym, 'name': hint_name or raw or sym, 'market_index_name': None, 'input': raw}
    elif lookup in GLOBAL_INDEX_MAP:
        sym = GLOBAL_INDEX_MAP[lookup]
        _, market_name, index_name = _global_index_metadata(sym)
        result = {'key': instrument_key('INDEX', sym), 'market': 'INDEX', 'market_name': market_name,
                  'symbol': sym, 'name': hint_name or raw or lookup, 'market_index_name': index_name, 'input': raw}
    elif lookup in CRYPTO_MAP or lookup.endswith('USDT') or any(c in lookup for c in ('BTC', 'ETH', 'XRP', 'DOGE', 'SOL', 'ADA', 'AVAX')):
        sym = CRYPTO_MAP.get(lookup, lookup if lookup.endswith('USDT') else f'{lookup}USDT')
        result = {'key': instrument_key('CRYPTO', sym), 'market': 'CRYPTO', 'market_name': '암호화폐',
                  'symbol': sym, 'name': hint_name or raw or sym, 'market_index_name': None, 'input': raw}
    elif lookup in US_STOCK_MAP:
        sym = US_STOCK_MAP[lookup]
        result = {'key': instrument_key('US', sym), 'market': 'US', 'market_name': '미국주식',
                  'symbol': sym, 'name': hint_name or (raw if any('가' <= c <= '힣' for c in raw) else sym), 'market_index_name': None, 'input': raw}
    elif US_TICKER_PATTERN.fullmatch(lookup or ''):
        result = {'key': instrument_key('US', lookup), 'market': 'US', 'market_name': '미국주식',
                  'symbol': lookup, 'name': hint_name or lookup, 'market_index_name': None, 'input': raw}
    elif lookup.isdigit() and len(lookup) == 6:
        result = {'key': instrument_key('KR', lookup), 'market': 'KR', 'market_name': '국내주식',
                  'symbol': lookup, 'name': hint_name or DOMESTIC_CODE_NAME_MAP.get(lookup, lookup), 'market_index_name': None, 'input': raw}
    else:
        # find_stock_code는 아래에서 정의되며 런타임에는 이미 존재한다.
        try:
            code, name = find_stock_code(raw)
        except Exception:
            code, name = None, None
        if not code:
            return None
        result = {'key': instrument_key('KR', code), 'market': 'KR', 'market_name': '국내주식',
                  'symbol': str(code).strip().upper(), 'name': hint_name or str(name or raw).strip(),
                  'market_index_name': None, 'input': raw}

    INSTRUMENT_RESOLUTION_CACHE[lookup] = (time.time(), dict(result))
    return register_instrument(result)


# 코드 -> 대표 종목명 역매핑. canonical URL/새로고침에서도 이름을 복원한다.
DOMESTIC_CODE_NAME_MAP = {}
for _domestic_name, _domestic_code in DOMESTIC_FAST_MAP.items():
    DOMESTIC_CODE_NAME_MAP.setdefault(str(_domestic_code).strip(), _domestic_name)

US_SYMBOL_NAME_MAP = {}
for _us_alias, _us_symbol in US_STOCK_MAP.items():
    _alias = str(_us_alias).strip()
    _sym = str(_us_symbol).strip().upper()
    # 영문 티커보다 한글 별칭을 우선해 화면 복원 이름으로 사용한다.
    if _sym and any('가' <= c <= '힣' for c in _alias):
        US_SYMBOL_NAME_MAP.setdefault(_sym, _alias)

CRYPTO_SYMBOL_NAME_MAP = {}
for _crypto_alias, _crypto_symbol in CRYPTO_MAP.items():
    _alias = str(_crypto_alias).strip()
    _sym = str(_crypto_symbol).strip().upper()
    if _sym and any('가' <= c <= '힣' for c in _alias):
        CRYPTO_SYMBOL_NAME_MAP.setdefault(_sym, _alias)

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
ANALYSIS_INFLIGHT = {}
ANALYSIS_INFLIGHT_LOCK = threading.Lock()
ANALYSIS_INFLIGHT_TIMEOUT = 45.0
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

    # 6) PostgreSQL에 이미 저장된 동일 종목 OHLCV를 최종 안전망으로 사용한다.
    # 과거에 한 번이라도 정상 조회된 종목은 외부 제공처가 순간적으로 실패해도
    # 빈 화면 대신 마지막 저장 데이터를 이용해 차트/퀀트 계산을 계속할 수 있다.
    try:
        db_market = "Korea"
        db_rows = get_db_recent_prices(db_market, str(code).strip().upper(), "1d", 400)
        if isinstance(db_rows, list) and len(db_rows) >= 15:
            records = []
            for item in reversed(db_rows):
                try:
                    records.append({
                        "Date": pd.to_datetime(item.get("timestamp")),
                        "Open": float(item.get("open")),
                        "High": float(item.get("high")),
                        "Low": float(item.get("low")),
                        "Close": float(item.get("close")),
                        "Volume": float(item.get("volume") or 0.0),
                    })
                except Exception:
                    continue
            if len(records) >= 15:
                df = pd.DataFrame(records).drop_duplicates(subset=["Date"]).set_index("Date").sort_index()
                df.attrs["_gaemi_raw_df"] = df.copy()
                print(f"[DOMESTIC DB FALLBACK] {code}: 저장된 {len(df)}건 사용")
                return df
    except Exception as exc:
        print(f"[DOMESTIC DB FALLBACK] {code} failed: {type(exc).__name__}: {exc}")

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


def _schedule_analysis_enrichment(cache_key, base_result, context):
    """핵심 응답 이후 선택 외부 데이터를 백그라운드에서 채워 캐시를 완성한다."""
    with ANALYSIS_ENRICH_LOCK:
        if cache_key in ANALYSIS_ENRICHING:
            return
        ANALYSIS_ENRICHING.add(cache_key)

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
                "stock_news_rss": _perf_submit(OPTIONAL_API_EXECUTOR, fetch_finlight_stock_news, symbol, market_name, formal_name, 20),
                "global_news_rss": _perf_submit(OPTIONAL_API_EXECUTOR, fetch_finlight_economic_news, market_name, 20),
                "api_news": _perf_submit(OPTIONAL_API_EXECUTOR, fetch_alpha_vantage_news, symbol if market_name == "미국주식" else "", "economy_macro"),
                "macro_snapshot": _perf_submit(OPTIONAL_API_EXECUTOR, fetch_fred_macro_snapshot),
                "consensus": _perf_submit(OPTIONAL_API_EXECUTOR, fetch_market_consensus_unified, symbol, market_name, raw_price, formal_name),
                "alpha_earnings": (_perf_submit(OPTIONAL_API_EXECUTOR, fetch_alpha_vantage_earnings, symbol) if market_name == "미국주식" and symbol else None),
                "dart_disclosures": _perf_submit(OPTIONAL_API_EXECUTOR, fetch_dart_disclosures, symbol, is_krw, 5),
                "domestic_flow": (_perf_submit(OPTIONAL_API_EXECUTOR, fetch_naver_domestic_flow_metrics, symbol) if is_krw and market_name == "국내주식" else None),
                "ticker_news": (_perf_submit(OPTIONAL_API_EXECUTOR, fetch_alpha_vantage_news, symbol, "") if market_name == "미국주식" and symbol else None),
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
            ANALYSIS_CACHE[cache_key] = (time.time(), enriched)
            with ANALYSIS_ENRICH_LOCK:
                ANALYSIS_ENRICH_CONTEXT.pop(cache_key, None)
            print(f"[ANALYSIS ENRICH] {cache_key} details ready")
        except Exception as exc:
            print(f"[ANALYSIS ENRICH] {cache_key} failed: {type(exc).__name__}: {exc}")
        finally:
            with ANALYSIS_ENRICH_LOCK:
                ANALYSIS_ENRICHING.discard(cache_key)

    # 재검색/새로고침으로 같은 core 캐시를 다시 읽더라도
    # 이미 시작된 enrichment 작업의 실행 맥락을 잃지 않도록 보관한다.
    with ANALYSIS_ENRICH_LOCK:
        ANALYSIS_ENRICH_CONTEXT[cache_key] = context
    ANALYSIS_ENRICH_EXECUTOR.submit(worker)


def _run_full_pipeline_impl(query, lightweight=False):
    global ANALYSIS_CACHE, CACHED_WORLD_BOARD, CACHED_MARKET_CALENDAR
    clean_q = str(query or '').strip().replace(" ", "").upper()
    instrument = resolve_instrument(query)
    if not instrument:
        return None
    cache_key = instrument["key"]
    now_ts = time.time()

    _perf_stage("cache_check")

    if cache_key in ANALYSIS_CACHE:
        cached_time, cached_data = ANALYSIS_CACHE[cache_key]
        if now_ts - cached_time < ANALYSIS_CORE_TTL and (lightweight or cached_data.get("_details_ready")):
            # STEP 4: 세계시장 보드/시장 캘린더는 별도 API가 담당한다.
            # 분석 캐시 응답에 현재 서버 캐시가 있으면 화면 호환성을 위해
            # 붙여주되, 캐시가 없다고 해서 외부 API를 호출하지 않는다.
            if CACHED_WORLD_BOARD:
                cached_data["world_board"] = CACHED_WORLD_BOARD
            if CACHED_MARKET_CALENDAR:
                cached_data["market_calendar"] = CACHED_MARKET_CALENDAR

            # core 캐시가 아직 enrichment 전이라면 기존 작업이 끝날 때까지
            # 기다릴 수 있도록 context가 보관되어 있는지 확인한다.
            # 새로고침/사이드바 재진입 때 core 결과를 다시 만들면서
            # 뉴스·컨센서스·트랙 분석이 비어 있는 상태가 반복되는 것을 막는다.
            if lightweight and not cached_data.get("_details_ready"):
                with ANALYSIS_ENRICH_LOCK:
                    enriching = cache_key in ANALYSIS_ENRICHING
                    enrich_context = ANALYSIS_ENRICH_CONTEXT.get(cache_key)
                if (not enriching) and enrich_context:
                    _schedule_analysis_enrichment(cache_key, cached_data, enrich_context)
            return cached_data

    market_name = "국내주식"
    market_index_name = None
    is_krw = True

    if instrument["market"] == "INDEX":
        symbol = instrument["symbol"]
        formal_name = instrument.get("name") or query.strip()

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

    elif instrument["market"] == "CRYPTO":
        symbol = instrument["symbol"]
        formal_name = instrument.get("name") or clean_q
        raw_price, price_str, df = fetch_crypto_candles(symbol)
        market_name = "암호화폐"
        is_krw = False

    elif instrument["market"] == "US":
        symbol = instrument["symbol"]
        formal_name = instrument.get("name") or query.strip()
        # 차트와 실시간 시세는 서로 의존하지 않으므로 동시에 조회한다.
        with ThreadPoolExecutor(max_workers=2) as price_executor:
            chart_future = _perf_submit(price_executor, fetch_overseas_candles, symbol)
            quote_future = _perf_submit(price_executor, fetch_overseas_quote_snapshot, symbol)
            chart_price, _, df = chart_future.result()
            quote_snapshot = quote_future.result()

        # Yahoo 차트가 일시적으로 실패한 경우 DB에 과거 OHLCV가 있으면 즉시 복구한다.
        if df is None or getattr(df, "empty", True):
            try:
                db_rows = get_db_recent_prices("US", str(symbol).strip().upper(), "1d", 400)
                if isinstance(db_rows, list) and len(db_rows) >= 15:
                    records = []
                    for item in reversed(db_rows):
                        try:
                            records.append({
                                "Date": pd.to_datetime(item.get("timestamp")),
                                "Open": float(item.get("open")),
                                "High": float(item.get("high")),
                                "Low": float(item.get("low")),
                                "Close": float(item.get("close")),
                                "Volume": float(item.get("volume") or 0.0),
                            })
                        except Exception:
                            continue
                    if len(records) >= 15:
                        df = pd.DataFrame(records).drop_duplicates(subset=["Date"]).set_index("Date").sort_index()
                        chart_price = float(df["Close"].iloc[-1])
                        print(f"[US DB FALLBACK] {symbol}: 저장된 {len(df)}건 사용")
            except Exception as exc:
                print(f"[US DB FALLBACK] {symbol} failed: {type(exc).__name__}: {exc}")

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
    else:
        # canonical key(KR:005930)가 들어온 경우 다시 종목명 검색을 하지 않는다.
        # 기존 코드처럼 find_stock_code(query)에 canonical 문자열을 넣으면
        # "KR:000660" -> 국내 종목 코드 탐색 실패 -> /api/analyze 404가 발생할 수 있다.
        # Resolver가 이미 확정한 symbol/name을 그대로 사용한다.
        code = str(instrument.get("symbol") or "").strip().upper()
        name = str(instrument.get("name") or "").strip()
        if not code:
            return None
        symbol = code
        formal_name = name or code

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
    # PostgreSQL 원천 OHLCV 저장
    # STEP 2: 사용자 응답 경로에서 DB 저장을 기다리지 않는다.
    # 분석에 사용할 df가 이후 계산 과정에서 변경될 수 있으므로
    # 백그라운드 작업에 넘길 복사본을 만든다.
    # ====================================================
    try:
        db_price_df = df.copy(deep=True)
        if isinstance(df.attrs.get("_gaemi_raw_df"), pd.DataFrame):
            db_price_df.attrs["_gaemi_raw_df"] = df.attrs["_gaemi_raw_df"].copy(deep=True)

        _submit_db_write(
            ("market_prices", market_name, str(symbol), "1d"),
            save_market_prices,
            df=db_price_df,
            market_name=market_name,
            symbol=symbol,
            timeframe="1d",
            source="alpha-trade-bot",
        )
    except Exception as exc:
        print(f"[DB ASYNC] 원천 OHLCV 작업 등록 예외 - 기존 분석 계속: {type(exc).__name__}: {exc}")

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
            "stock_news_rss": _perf_submit(OPTIONAL_API_EXECUTOR, fetch_finlight_stock_news, symbol, market_name, formal_name, 20),
            "global_news_rss": _perf_submit(OPTIONAL_API_EXECUTOR, fetch_finlight_economic_news, market_name, 20),
            "api_news": _perf_submit(OPTIONAL_API_EXECUTOR, fetch_alpha_vantage_news, symbol if market_name == "미국주식" else "", "economy_macro"),
            "macro_snapshot": _perf_submit(OPTIONAL_API_EXECUTOR, fetch_fred_macro_snapshot),
            "consensus": _perf_submit(OPTIONAL_API_EXECUTOR, fetch_market_consensus_unified, symbol, market_name, raw_price, formal_name),
            "alpha_earnings": (_perf_submit(OPTIONAL_API_EXECUTOR, fetch_alpha_vantage_earnings, symbol) if market_name == "미국주식" and symbol else None),
            "dart_disclosures": _perf_submit(OPTIONAL_API_EXECUTOR, fetch_dart_disclosures, symbol, is_krw, 5),
            "domestic_flow": (_perf_submit(OPTIONAL_API_EXECUTOR, fetch_naver_domestic_flow_metrics, symbol) if is_krw and market_name == "국내주식" else None),
            "ticker_news": (_perf_submit(OPTIONAL_API_EXECUTOR, fetch_alpha_vantage_news, symbol, "") if market_name == "미국주식" and symbol else None),
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

    # 미국주식은 영어 원문 제목 대신 한국어 제목을 표시한다.
    # 원문 링크/언론사/발행일은 그대로 유지한다.
    if market_name == "미국주식":
        stock_news = translate_news_titles_to_korean(stock_news[:20], market_name)
        global_news = translate_news_titles_to_korean(global_news[:20], market_name)

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

    try:
        # 퀀트 결과도 응답을 막지 않도록 백그라운드 저장한다.
        db_quant_df = df.copy(deep=True)
        db_quant_payload = dict(quant) if isinstance(quant, dict) else quant

        _submit_db_write(
            ("quant_results", market_name, str(symbol), "1d"),
                    market_name=market_name,
            symbol=symbol,
            raw_price=raw_price,
            df=db_quant_df,
            quant=db_quant_payload,
            timeframe="1d",
        )
    except Exception as exc:
        print(f"[DB ASYNC] 퀀트 결과 작업 등록 예외 - 기존 분석 계속: {type(exc).__name__}: {exc}")

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
            volume_value = 0.0
            try:
                if 'Volume' in row.index and pd.notna(row['Volume']):
                    volume_value = max(0.0, float(row['Volume']))
                elif 'volume' in row.index and pd.notna(row['volume']):
                    volume_value = max(0.0, float(row['volume']))
            except Exception:
                volume_value = 0.0
            candles.append({
                "time": t_str,
                "open": o, "high": h,
                "low": l, "close": c,
                "volume": volume_value
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
        "market_code": instrument.get("market"),
        "instrument_key": cache_key,
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
        "bb_dn_line": bb_dn_line,
        "malicious_zones": _build_malicious_zones(df, raw_price)
    }

    result_data["_details_ready"] = not lightweight
    ANALYSIS_CACHE[cache_key] = (now_ts, result_data)

    if lightweight:
        try:
            enrich_df = df.copy(deep=True)
            _schedule_analysis_enrichment(
                cache_key,
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

def run_full_pipeline_legacy(query, lightweight=False):
    """LEGACY: 예전 통합 분석 파이프라인. 활성 라우트에서는 사용하지 않는다."""
    instrument = resolve_instrument(query)
    if not instrument:
        return None
    inflight_key = f'{instrument["key"]}:{"core" if lightweight else "full"}'

    # 빠른 cache hit는 내부 구현에 맡긴다. 여기서는 동시 실행만 합친다.
    owner = False
    event = None
    with ANALYSIS_INFLIGHT_LOCK:
        event = ANALYSIS_INFLIGHT.get(inflight_key)
        if event is None:
            event = threading.Event()
            ANALYSIS_INFLIGHT[inflight_key] = event
            owner = True

    if not owner:
        # 먼저 실행 중인 같은 종목/같은 모드가 끝나길 기다린다.
        event.wait(timeout=ANALYSIS_INFLIGHT_TIMEOUT)
        cached = ANALYSIS_CACHE.get(instrument["key"])
        if cached:
            cached_data = cached[1]
            if lightweight or cached_data.get("_details_ready"):
                return cached_data
        # 기존 작업이 실패/시간초과했으면 현재 요청이 한 번 더 시도한다.
        with ANALYSIS_INFLIGHT_LOCK:
            current = ANALYSIS_INFLIGHT.get(inflight_key)
            if current is event and event.is_set():
                ANALYSIS_INFLIGHT.pop(inflight_key, None)
                event = threading.Event()
                ANALYSIS_INFLIGHT[inflight_key] = event
                owner = True
        if not owner:
            return None

    try:
        result = _run_full_pipeline_impl(query, lightweight=lightweight)
        if result is None:
            # 외부 제공처의 순간 timeout/빈 응답에 한해 1회만 재시도한다.
            time.sleep(0.25)
            result = _run_full_pipeline_impl(query, lightweight=lightweight)
        return result
    finally:
        with ANALYSIS_INFLIGHT_LOCK:
            current = ANALYSIS_INFLIGHT.get(inflight_key)
            if current is event:
                ANALYSIS_INFLIGHT.pop(inflight_key, None)
                event.set()


# ====================================================
# STEP 56: 독립 위젯 데이터 계층
# ====================================================
# 화면 위젯은 서로의 계산 결과를 사용하지 않는다.
# 공통으로 필요한 원천 주가/OHLCV만 메모리에서 짧게 공유한다.
STOCK_CORE_CONTEXT_CACHE = {}
STOCK_CORE_CONTEXT_TTL = 60
STOCK_CORE_CONTEXT_MAX = 64
STOCK_CORE_CONTEXT_LOCK = threading.Lock()


def _format_core_price(market_code, market_name, price):
    if price is None:
        return '-'
    try:
        n = float(price)
    except Exception:
        return '-'
    if market_code == 'KR':
        return f'{int(round(n)):,} 원'
    if market_code == 'CRYPTO':
        return f'${n:,.4f}' if n < 1 else f'${n:,.2f}'
    if market_code == 'INDEX':
        if market_name == '국내증시':
            return f'{n:,.2f} pt'
        if market_name == '외환':
            return f'{n:,.1f} 원'
        if market_name == '원자재':
            return f'${n:,.2f}'
        return f'{n:,.2f}'
    return f'${n:,.2f}'


def _build_core_candles(df, limit=65):
    rows = []
    if df is None or getattr(df, 'empty', True):
        return rows
    frame = df.sort_index().copy()
    frame = frame[~frame.index.duplicated(keep='last')]
    for date_idx, row in frame.tail(limit).iterrows():
        try:
            o, h, l, c = [float(row[k]) for k in ('Open', 'High', 'Low', 'Close')]
            if any(np.isnan(v) for v in (o, h, l, c)):
                continue
            vol = 0.0
            if 'Volume' in row.index and pd.notna(row['Volume']):
                vol = max(0.0, float(row['Volume']))
            elif 'volume' in row.index and pd.notna(row['volume']):
                vol = max(0.0, float(row['volume']))
            rows.append({
                'time': pd.to_datetime(date_idx).strftime('%Y-%m-%d'),
                'open': o, 'high': h, 'low': l, 'close': c, 'volume': vol
            })
        except Exception:
            continue
    return rows


def _calc_core_change_pct(raw_price, df):
    if raw_price is None or df is None or len(df) < 2:
        return None
    try:
        prev = float(df['Close'].iloc[-2])
        if prev == 0:
            return None
        return ((float(raw_price) - prev) / prev) * 100.0
    except Exception:
        return None


def _load_us_df_from_db(symbol):
    try:
        db_rows = get_db_recent_prices('US', str(symbol).strip().upper(), '1d', 400)
        if not isinstance(db_rows, list) or len(db_rows) < 15:
            return None
        records = []
        for item in reversed(db_rows):
            try:
                records.append({
                    'Date': pd.to_datetime(item.get('timestamp')),
                    'Open': float(item.get('open')),
                    'High': float(item.get('high')),
                    'Low': float(item.get('low')),
                    'Close': float(item.get('close')),
                    'Volume': float(item.get('volume') or 0.0),
                })
            except Exception:
                continue
        if len(records) < 15:
            return None
        return pd.DataFrame(records).drop_duplicates(subset=['Date']).set_index('Date').sort_index()
    except Exception:
        return None


def _load_stock_core_context(query='', market_hint=None, symbol_hint=None, name_hint=None):
    instrument = resolve_instrument(query, market_hint=market_hint, symbol_hint=symbol_hint, name_hint=name_hint)
    if not instrument:
        return None
    key = instrument['key']
    now = time.time()
    with STOCK_CORE_CONTEXT_LOCK:
        cached = STOCK_CORE_CONTEXT_CACHE.get(key)
        if cached and now - cached['time'] < STOCK_CORE_CONTEXT_TTL:
            return cached['context']

    market_code = instrument['market']
    symbol = instrument['symbol']
    formal_name = instrument.get('name') or symbol
    market_name = instrument.get('market_name') or '국내주식'
    market_index_name = instrument.get('market_index_name')
    raw_price = None
    price_str = '-'
    df = None
    current_session = None
    current_price_source = None
    current_previous_close = None

    try:
        if market_code == 'KR':
            # 거래소 구분은 표시용이므로 캐시된 값만 사용하고, 없으면 백그라운드 갱신한다.
            market_index_name = _get_cached_domestic_market_type(symbol) or market_index_name
            if market_index_name is None:
                _perf_submit(OPTIONAL_API_EXECUTOR, _refresh_domestic_market_type, symbol)
            futures = {
                'price': _perf_submit(PRICE_DATA_EXECUTOR, get_naver_realtime_price, symbol),
                'candles': _perf_submit(PRICE_DATA_EXECUTOR, fetch_domestic_candles, symbol, None),
            }
            raw_price, price_str = futures['price'].result()
            df = futures['candles'].result()
            if raw_price is None and df is not None and not df.empty:
                raw_price = float(df['Close'].iloc[-1])
                price_str = _format_core_price('KR', market_name, raw_price)
            current_price_source = 'Naver/KRX'
        elif market_code == 'US':
            with ThreadPoolExecutor(max_workers=2) as ex:
                f_chart = ex.submit(fetch_overseas_candles, symbol)
                f_quote = ex.submit(fetch_overseas_quote_snapshot, symbol)
                chart_price, _, df = f_chart.result()
                quote_snapshot = f_quote.result()
            if df is None or getattr(df, 'empty', True):
                df = _load_us_df_from_db(symbol)
                if df is not None and not df.empty:
                    chart_price = float(df['Close'].iloc[-1])
            if isinstance(quote_snapshot, dict) and quote_snapshot.get('price') is not None:
                raw_price = float(quote_snapshot['price'])
                current_session = quote_snapshot.get('market_state')
                current_previous_close = quote_snapshot.get('previous_close')
                current_price_source = quote_snapshot.get('price_source') or 'Yahoo'
            else:
                raw_price = chart_price
                current_price_source = 'Yahoo chart'
            price_str = _format_core_price('US', market_name, raw_price)
            market_name = '미국주식'
        elif market_code == 'CRYPTO':
            raw_price, price_str, df = fetch_crypto_candles(symbol)
            market_name = '암호화폐'
            current_price_source = 'Binance'
        elif market_code == 'INDEX':
            raw_price, price_str, df = fetch_overseas_candles(symbol)
            _, market_name, market_index_name = _global_index_metadata(symbol)
            price_str = _format_core_price('INDEX', market_name, raw_price)
            current_price_source = 'Yahoo'
        else:
            return None
    except Exception as exc:
        print(f'[STOCK CORE] {key} fetch failed: {type(exc).__name__}: {exc}')
        return None

    if raw_price is None or df is None or getattr(df, 'empty', True):
        return None

    change_pct = None
    if current_previous_close:
        try:
            prev = float(current_previous_close)
            if prev:
                change_pct = ((float(raw_price) - prev) / prev) * 100.0
        except Exception:
            change_pct = None
    if change_pct is None:
        change_pct = _calc_core_change_pct(raw_price, df)

    context = {
        'instrument': dict(instrument),
        'market_code': market_code,
        'market': market_name,
        'market_index_name': market_index_name,
        'symbol': symbol,
        'name': formal_name,
        'current_price': raw_price,
        'price': price_str,
        'current_change_pct': change_pct,
        'current_previous_close': current_previous_close,
        'current_session': current_session,
        'current_price_source': current_price_source,
        'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S KST'),
        'candles': _build_core_candles(df),
        '_df': df.copy(deep=True),
    }
    with STOCK_CORE_CONTEXT_LOCK:
        STOCK_CORE_CONTEXT_CACHE[key] = {'time': now, 'context': context}
        if len(STOCK_CORE_CONTEXT_CACHE) > STOCK_CORE_CONTEXT_MAX:
            oldest_key = min(STOCK_CORE_CONTEXT_CACHE, key=lambda k: STOCK_CORE_CONTEXT[k]['time'])
            STOCK_CORE_CONTEXT_CACHE.pop(oldest_key, None)
    return context


def run_independent_stock_core(query='', lightweight=False, market_hint=None, symbol_hint=None, name_hint=None):
    context = _load_stock_core_context(query, market_hint, symbol_hint, name_hint)
    if not context:
        return None
    data = {k: v for k, v in context.items() if k != '_df' and k != 'instrument'}
    data.update({
        'instrument_key': context['instrument']['key'],
        'market_code': context['market_code'],
        'market': context['market'],
        'name': context['name'],
        'formal_name': context['name'],
        'symbol': context['symbol'],
        'price_str': context['price'],
        'now_str': context['time'],
        '_details_ready': True,
    })
    return data


def _widget_instrument_from_params(params):
    market = str(params.get('m', [''])[0]).strip().upper()
    symbol = str(params.get('s', [''])[0]).strip().upper()
    name = str(params.get('name', [''])[0]).strip()
    q = str(params.get('q', [''])[0]).strip()
    if not symbol and not q:
        return None
    return resolve_instrument(q or symbol, market_hint=market, symbol_hint=symbol, name_hint=name)


def _get_widget_core_from_params(params):
    ins = _widget_instrument_from_params(params)
    if not ins:
        return None
    return _load_stock_core_context(ins['key'], market_hint=ins['market'], symbol_hint=ins['symbol'], name_hint=ins.get('name'))


def _load_track_context(ins):
    """트랙 전용 최소 원천 데이터 로더. Core 캐시가 없거나 핵심 quote가 실패해도
    차트/일봉 데이터만으로 트랙 위젯을 계산할 수 있게 한다.
    """
    if not ins:
        return None
    try:
        context = _load_stock_core_context(
            ins['key'], market_hint=ins['market'], symbol_hint=ins['symbol'], name_hint=ins.get('name')
        )
        if context:
            return context
    except Exception as exc:
        print(f"[TRACK CORE] {ins.get('key')} shared core failed: {type(exc).__name__}: {exc}")

    try:
        payload = run_fast_chart_pipeline(ins['symbol'])
        candles = (payload or {}).get('candles') or []
        if len(candles) < 2:
            return None
        rows = []
        for row in candles:
            rows.append({
                'Date': pd.to_datetime(row.get('time')),
                'Open': float(row.get('open')),
                'High': float(row.get('high')),
                'Low': float(row.get('low')),
                'Close': float(row.get('close')),
                'Volume': float(row.get('volume') or 0),
            })
        df = pd.DataFrame(rows).set_index('Date').sort_index()
        current = float(df['Close'].iloc[-1])
        previous = float(df['Close'].iloc[-2]) if len(df) >= 2 else None
        change = ((current - previous) / previous * 100.0) if previous else None
        market_name = ins.get('market_name') or '국내주식'
        return {
            'instrument': dict(ins),
            'market_code': ins['market'],
            'market': market_name,
            'symbol': ins['symbol'],
            'name': ins.get('name') or ins['symbol'],
            'current_price': current,
            'price': _format_core_price(ins['market'], market_name, current),
            'current_change_pct': change,
            'candles': candles,
            '_df': df,
        }
    except Exception as exc:
        print(f"[TRACK FAST] {ins.get('key')} fallback failed: {type(exc).__name__}: {exc}")
        return None


def build_track_widget_data(context):
    df = context.get('_df')
    candles = context.get('candles') or []
    evidence = []
    pct60 = None
    if df is not None and len(df) >= 2:
        try:
            tail = df['Close'].dropna().tail(60)
            if len(tail) >= 2 and float(tail.iloc[0]) != 0:
                pct60 = ((float(tail.iloc[-1]) / float(tail.iloc[0])) - 1.0) * 100.0
        except Exception:
            pass
    if pct60 is not None:
        evidence.append({'key': 'trend_60d', 'label': '최근 60일 추세', 'score': 1 if pct60 > 1 else (-1 if pct60 < -1 else 0), 'detail': f'{pct60:+.2f}%'})
    if df is not None and 'Volume' in df.columns and len(df) >= 6:
        try:
            vol = df['Volume'].astype(float).tail(6)
            current = float(vol.iloc[-1]); avg = float(vol.iloc[:-1].mean())
            ratio = (current / avg * 100.0) if avg > 0 else None
            if ratio is not None:
                score = 1 if ratio >= 120 else 0
                evidence.append({'key': 'volume', 'label': '거래량', 'score': score, 'detail': f'5일 평균 대비 {ratio:.0f}%'})
        except Exception:
            pass
    return {
        'price_str': context.get('price'),
        'current_change_pct': context.get('current_change_pct'),
        'candles': candles,
        'evidence': evidence,
        'evidence_count': len([x for x in evidence if float(x.get('score', 0) or 0) != 0]),
        'status': '실제 원천 데이터 기준',
    }



def run_full_pipeline(query, lightweight=False):
    """활성 종목 진입점은 핵심 데이터만 반환한다. 위젯은 각자 독립 API를 호출한다."""
    return run_independent_stock_core(query, lightweight=lightweight)

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
        market_code = {'kr':'KR','us':'US','crypto':'CRYPTO'}.get(key, key.upper())
        normalized_rows = []
        for raw_row in rows:
            row = dict(raw_row)
            symbol = str(row.get('symbol') or row.get('code') or '').strip().upper()
            name = str(row.get('name') or symbol).strip()
            if symbol:
                row['market_code'] = market_code
                row['instrument_key'] = instrument_key(market_code, symbol)
                register_dynamic_instrument(market_code, symbol, name)
            normalized_rows.append(row)
        rows = normalized_rows
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

    window.GaemiWidgets.dart = {
        async load(context) {
            const ctx = context || {};
            const tokenKey = `${String(ctx.market || '')}:${String(ctx.symbol || '')}`;
            if (window.__gaemiDartLoadKey === tokenKey) return;
            window.__gaemiDartLoadKey = tokenKey;
            const qs = new URLSearchParams({
                m: String(ctx.market || ''),
                s: String(ctx.symbol || ''),
                name: String(ctx.name || '')
            });
            try {
                const r = await fetch('/api/widget/dart?' + qs.toString(), { cache: 'no-store' });
                const payload = await r.json().catch(() => null);
                if (payload && payload.ok) window.GaemiWidgets.renderDartDisclosure(payload.data || []);
            } catch (e) {
                console.warn('[DART] load failed:', e);
            }
        }
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
    <script src="https://cdn.jsdelivr.net/npm/echarts@5.6.0/dist/echarts.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2"></script>
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
    <style>
        :root {
            --bg-body: #131314;
            --bg-card: #1e1f20;
            --bg-sidebar: #1e1f20;
            --bg-subcard: #131314;
            --bg-cal-card: #18191a;
            --text-title: #f2f2f2;
            --text-main: #e3e3e3;
            --text-muted: #8e918f;
            --text-sub: #c4c7c5;
            --border-line: #282a2c;
            --border-focus: #333538;
            --btn-hover: rgba(255, 255, 255, 0.08);
            --signal-border: rgba(16, 185, 129, 0.55);
            --signal-glow: rgba(16, 185, 129, 0.12);
            --bottom-fade: linear-gradient(180deg, rgba(19,19,20,0) 0%, rgba(19,19,20,1) 40%);
            --star-gold: #f59e0b;
        }

        body.light-theme {
            --bg-body: #f8f9fa;
            --bg-card: #ffffff;
            --bg-sidebar: #f1f3f4;
            --bg-subcard: #f8f9fa;
            --bg-cal-card: #ffffff;
            --text-title: #1f1f1f;
            --text-main: #202124;
            --text-muted: #5f6368;
            --text-sub: #3c4043;
            --border-line: #dadce0;
            --border-focus: #bdc1c6;
            --btn-hover: rgba(0, 0, 0, 0.05);
            --signal-border: rgba(16, 185, 129, 0.65);
            --signal-glow: rgba(16, 185, 129, 0.08);
            --bottom-fade: linear-gradient(180deg, rgba(248,249,250,0) 0%, rgba(248,249,250,1) 40%);
            --star-gold: #d97706;
        }

        * { box-sizing: border-box; margin: 0; padding: 0; font-family: 'Google Sans', 'Roboto', -apple-system, BlinkMacSystemFont, "Apple SD Gothic Neo", sans-serif; }
        body { background-color: var(--bg-body); color: var(--text-main); min-height: 100vh; overflow-x: hidden; display: flex; transition: background-color 0.2s, color 0.2s; }
        
        #sidebar-wrapper {
            width: 260px;
            background-color: var(--bg-sidebar);
            height: 100vh;
            position: fixed;
            top: 0;
            left: 0;
            display: flex;
            flex-direction: column;
            padding: 12px 14px 16px;
            z-index: 100;
            border-right: 1px solid var(--border-line);
            transform: translateX(-100%);
            transition: transform 0.25s ease, background-color 0.2s;
            box-shadow: none;
        }
        #sidebar-wrapper.open { transform: translateX(0); box-shadow: 10px 0 30px rgba(0,0,0,0.25); }

        .side-top-action { height: 44px; display: flex; align-items: center; justify-content: space-between; margin-bottom: 12px; padding: 0 4px; }
        .gemini-menu-icon { background: transparent; border: none; color: var(--text-sub); cursor: pointer; padding: 6px; border-radius: 50%; display: flex; align-items: center; justify-content: center; transition: background 0.15s, color 0.15s; }
        .gemini-menu-icon:hover { background: var(--btn-hover); color: var(--text-title); }

        .gemini-new-btn { display: flex; align-items: center; gap: 12px; background: var(--bg-body); border: 1px solid var(--border-line); color: var(--text-main); font-size: 0.875rem; font-weight: 500; padding: 10px 16px; border-radius: 24px; cursor: pointer; width: 100%; transition: all 0.2s; margin-bottom: 16px; }
        .gemini-new-btn:hover { background: var(--btn-hover); border-color: var(--border-focus); color: var(--text-title); }

        .sidebar-scroll-area { flex: 1; overflow-y: auto; }
        .sidebar-section-title { font-size: 0.75rem; color: var(--text-muted); font-weight: 600; padding: 8px 12px; letter-spacing: -0.2px; }
        .sidebar-item { display: flex; align-items: center; padding: 9px 14px; border-radius: 20px; color: var(--text-sub); font-size: 0.875rem; cursor: pointer; margin-bottom: 2px; transition: background 0.15s, color 0.15s; }
        .sidebar-item:hover { background: var(--btn-hover); color: var(--text-title); }

        #content-wrapper { margin-left: 0; width: 100%; min-height: 100vh; display: flex; flex-direction: column; position: relative; }
        header { height: 56px; display: flex; align-items: center; justify-content: space-between; padding: 0 16px; background: var(--bg-body); position: sticky; top: 0; z-index: 40; transition: background-color 0.2s; }
        .header-left { display: flex; align-items: center; gap: 12px; }
        .logo-top { font-weight: 700; font-size: 1.18rem; color: var(--text-title); letter-spacing: -0.5px; cursor: pointer; }

        #main-view { min-height: calc(100vh - 56px); display: flex; flex-direction: column; align-items: center; justify-content: center; padding: 16px; }
        .hero-title { font-size: 2.8rem; font-weight: 700; margin-bottom: 6px; letter-spacing: -1px; color: var(--text-title); text-align: center; }
        .hero-sub { font-size: 0.95rem; color: var(--text-muted); margin-bottom: 24px; font-weight: 400; text-align: center; }

        .search-container { width: 100%; max-width: 832px; position: relative; }
        .search-box { width: 100%; min-height: 60px; background: var(--bg-card); border: 1px solid var(--border-line); border-radius: 30px; padding: 8px 12px 8px 18px; display: flex; align-items: center; box-shadow: 0 4px 20px rgba(0,0,0,0.1); position: relative; }
        .search-box input { flex: 1; min-width: 0; background: transparent; border: none; outline: none; color: var(--text-title); font-size: 0.95rem; }

        /* 모델 선택기 */
        .model-selector-wrap { position: relative; margin-right: 4px; flex-shrink: 0; }
        .model-pill-btn { background: transparent; border: none; border-radius: 20px; padding: 6px 8px; display: flex; align-items: center; gap: 4px; cursor: pointer; user-select: none; }
        .model-pill-btn:hover { background: var(--btn-hover); }
        .model-pill-name { font-size: 0.82rem; font-weight: 600; color: var(--text-main); }
        .model-pill-arrow { font-size: 0.6rem; color: var(--text-muted); }

        .model-dropdown-menu { display: none; position: absolute; bottom: calc(100% + 10px); right: 0; width: 210px; max-width: 80vw; background: var(--bg-card); border: 1px solid var(--border-line); border-radius: 16px; padding: 6px; box-shadow: 0 10px 30px rgba(0,0,0,0.15); z-index: 100; }
        .model-dropdown-item { padding: 9px 12px; border-radius: 10px; cursor: pointer; }
        .model-dropdown-item:hover { background: var(--btn-hover); }
        .model-dropdown-item.active { background: var(--btn-hover); }
        .item-title { font-size: 0.88rem; font-weight: 600; color: var(--text-title); display: flex; justify-content: space-between; }
        .item-sub { font-size: 0.73rem; color: var(--text-muted); margin-top: 2px; }
        .item-check { color: #38bdf8; font-weight: bold; display: none; }
        .model-dropdown-item.active .item-check { display: inline; }

        .send-btn { width: 38px; height: 38px; border-radius: 50%; background: var(--border-line); border: none; color: var(--text-title); display: flex; align-items: center; justify-content: center; cursor: pointer; flex-shrink: 0; }
        .send-btn:hover { background: var(--border-focus); }

        .popular-tags { display: flex; gap: 8px; margin-top: 20px; flex-wrap: wrap; justify-content: center; }
        .tag-btn { background: var(--bg-card); border: 1px solid var(--border-line); border-radius: 16px; padding: 6px 13px; font-size: 0.82rem; color: var(--text-sub); cursor: pointer; }
        .tag-btn:hover { background: var(--btn-hover); color: var(--text-title); }

        #result-scroll-view { display: none; width: 100%; min-height: 100vh; padding: 16px 12px 100px; }
        .result-inner { max-width: 1220px; margin: 0 auto; width: 100%; }

        /* [핵심 마스터 그리드] PC: 832px(좌) + 348px(우) 완벽 대칭 구조 */
        .dashboard-master-grid {
            display: grid;
            grid-template-columns: 832px 348px;
            gap: 16px;
            margin: 0 auto;
            max-width: 1196px;
            justify-content: center;
            align-items: stretch;
            position: relative;
        }

        /* 4행: 트랙 분석 — 종목 뉴스 바로 아래 */
        #rowTrack { grid-column: 1; grid-row: 4; }

        /* 2행: 차트(좌) & 증시(우) */
        #rowChart { grid-column: 1; grid-row: 2; height: 100%; }
        #rowWorld { grid-column: 2; grid-row: 2; height: 100%; }

        /* 3행: 종목 뉴스(좌) & 경제뉴스(우)
           테스트 수정: 뉴스 카드끼리 높이를 강제로 공유하지 않음 */
        #rowNews {
            grid-column: 1;
            grid-row: 3;
            height: auto;
            align-self: start;
        }
        #rowGlobalNews {
            grid-column: 2;
            grid-row: 3;
            height: auto;
            align-self: start;
        }

        /* 4행: 트랙 분석(좌) & 실적 일정(우) */
        #rowTrack {
            grid-column: 1;
            grid-row: 4;
            height: 100%;
            min-height: 0;
            align-self: stretch;
        }
        #rowEarningsCalendar {
            grid-column: 2;
            grid-row: 4;
            height: 100%;
            min-height: 0;
            align-self: stretch;
        }

        /* 5행: 퀀트 분석(좌) & 경제 일정(우) */
        #rowQuant {
            grid-column: 1;
            grid-row: 5;
            height: 100%;
            min-height: 0;
            align-self: stretch;
        }
        #rowEconomicCalendar {
            grid-column: 2;
            grid-row: 5;
            height: 100%;
            min-height: 0;
            align-self: stretch;
        }

        /* 같은 행의 좌/우 카드가 항상 1:1 높이가 되도록 그리드 행을 채운다. */
        .calendar-widget {
            min-height: 0;
            height: 100%;
            display: flex;
            flex-direction: column;
            overflow: hidden;
            align-self: stretch;
            box-sizing: border-box;
            padding: 16px;
        }

        .calendar-widget-title {
            display: flex;
            align-items: center;
            justify-content: flex-start;
            gap: 8px;
            margin-bottom: 10px;
            font-size: 1.05rem;
            line-height: 1.25;
            font-weight: 800;
            color: var(--text-title);
            flex: 0 0 auto;
        }

        .calendar-widget-nav {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 8px;
            margin-bottom: 8px;
            flex: 0 0 auto;
        }

        .calendar-star-filter {
            display: flex;
            align-items: center;
            gap: 6px;
            margin: 0 0 8px;
            flex: 0 0 auto;
        }

        .calendar-star-btn {
            border: 1px solid var(--border-line);
            background: var(--bg-card);
            color: var(--text-sub);
            border-radius: 999px;
            padding: 4px 9px;
            font-size: 0.66rem;
            line-height: 1;
            font-weight: 800;
            cursor: pointer;
        }

        .calendar-star-btn.active {
            background: var(--text-title);
            color: #111;
            border-color: var(--text-title);
        }

        .calendar-widget-subtitle {
            font-size: 0.72rem;
            line-height: 1.25;
            font-weight: 700;
            color: var(--text-muted);
            margin: 1px 0 8px;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            flex: 0 0 auto;
        }

        .calendar-widget-list {
            min-height: 0;
            flex: 1 1 auto;
            overflow: hidden;
            display: flex;
            flex-direction: column;
            gap: 8px;
        }

        .calendar-widget-list .cal-card-item {
            min-height: 64px;
            padding: 9px 10px;
            gap: 9px;
            border-radius: 10px;
            flex: 0 0 auto;
        }

        .calendar-widget-list .cal-time-col {
            min-width: 48px;
            font-size: 0.68rem;
            line-height: 1.25;
            font-weight: 700;
        }

        .calendar-widget-list .cal-event-title {
            font-size: 0.76rem;
            line-height: 1.3;
            font-weight: 750;
        }

        .calendar-widget-list .cal-event-meta {
            font-size: 0.64rem;
            line-height: 1.3;
            margin-top: 3px;
        }

        .calendar-widget-list .cal-importance-wrap {
            margin-top: 4px;
        }

        .calendar-widget-list .cal-alarm-btn {
            width: 27px;
            height: 27px;
            flex: 0 0 27px;
        }

        .calendar-widget .cal-pagination-bar {
            flex: 0 0 auto;
            margin-top: 8px;
        }

        .calendar-widget .cal-event-icon {
            width: 32px;
            height: 32px;
            border-radius: 8px;
            display: flex;
            align-items: center;
            justify-content: center;
            flex: 0 0 32px;
            font-size: 0.58rem;
            font-weight: 800;
            color: var(--text-title);
            background: var(--bg-card);
            border: 1px solid var(--border-line);
        }

        .calendar-widget .cal-earn-item .cal-event-icon {
            color: #7ee787;
            border-color: rgba(126, 231, 135, 0.28);
        }

        .calendar-widget .cal-event-topline {
            display: flex;
            align-items: center;
            gap: 7px;
            min-width: 0;
        }

        .calendar-widget .cal-stars {
            flex: 0 0 auto;
            font-size: 0.72rem;
            letter-spacing: -0.04em;
            color: #ffb000;
            white-space: nowrap;
        }

        .calendar-widget .cal-event-meta {
            color: var(--text-sub);
        }
        /* 실적 캘린더: 기업명 아래에 미국 장 발표 시점을 한 줄로 표시 */
        /* 실적 카드 표시 순서 고정:
           별점/발표상태 → 기업명 → (미국 장 마감 후) → 예상 EPS/매출 */
        .calendar-widget .cal-earn-item .cal-detail-col {
            display: flex !important;
            flex-direction: column !important;
            min-width: 0 !important;
        }
        .calendar-widget .cal-earn-item .cal-event-topline {
            order: 1 !important;
            display: flex !important;
        }
        .calendar-widget .cal-earn-item .cal-event-title {
            order: 2 !important;
            display: block !important;
            width: 100% !important;
            margin-top: 1px !important;
            margin-bottom: 0 !important;
        }
        .calendar-widget .cal-earn-item .cal-earn-timing {
            order: 3 !important;
            display: block !important;
            width: 100% !important;
            margin-top: 1px !important;
            font-size: 0.70rem !important;
            line-height: 1.25 !important;
            font-weight: 700 !important;
            color: var(--text-sub) !important;
            white-space: normal !important;
        }
        .calendar-widget .cal-earn-item .cal-event-meta {
            order: 4 !important;
        }


        .calendar-widget .cal-date-ctrl {
            display: flex;
            align-items: center;
            gap: 8px;
        }

        .calendar-widget .cal-current-date {
            min-width: 116px;
            text-align: center;
            font-size: 0.86rem;
            font-weight: 800;
        }

        @media (max-width: 1100px) {
            #rowEarningsCalendar { order: 4; width: 100%; max-width: 832px; }
            #rowQuant { order: 5; }
            #rowEconomicCalendar { order: 6; width: 100%; max-width: 832px; }
        }


        /* 좌측 832px 단독 배치 행들 */
        #rowConsensus { grid-column: 1; grid-row: 6; }
        #rowVote { grid-column: 1; grid-row: 7; }
        #rowComment { grid-column: 1; grid-row: 8; }
        #rowDart { grid-column: 1; grid-row: 9; }

        .card {
            background: var(--bg-card);
            border-radius: 16px;
            padding: 18px 16px;
            border: 1px solid var(--border-line);
            box-shadow: 0 2px 10px rgba(0,0,0,0.03);
            width: 100%;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
        }

        .price-summary-head {
            min-height: 34px;
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 14px;
            width: 100%;
        }
        .price-title { font-size: 1.05rem; line-height: 1.2; font-weight: 700; color: var(--text-title); word-break: keep-all; white-space: nowrap; }
        .price-title span { color: var(--text-muted); font-size: 0.72rem; font-weight: 400; margin-left: 4px; }
        .price-live-line {
            display: inline-flex;
            align-items: baseline;
            justify-content: flex-end;
            gap: 9px;
            min-width: 0;
            white-space: nowrap;
            flex: 0 0 auto;
        }
        .price-val { font-size: 1.12rem; line-height: 1.2; font-weight: 750; color: var(--text-title); margin: 0; font-variant-numeric: tabular-nums; }
        .price-change { font-size: 0.92rem; line-height: 1.2; font-weight: 700; color: var(--text-sub); font-variant-numeric: tabular-nums; min-width: 0; }
        .price-meta { color: var(--text-muted); font-size: 0.70rem; line-height: 1.3; min-height: 18px; margin-top: 2px; word-break: break-all; }

        .chart-box-wrap { width: 100%; height: 300px; margin-top: 8px; position: relative; background: var(--bg-card); border-radius: 8px; overflow: hidden; }
        @media (max-width: 680px) {
            .price-summary-head { gap: 8px; }
            .price-title { font-size: 0.96rem; }
            .price-title span { font-size: 0.68rem; }
            .price-val { font-size: 1.00rem; }
            .price-change { font-size: 0.82rem; }
        }

        .board-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px; padding-bottom: 6px; border-bottom: 1px solid var(--border-line); }
        .board-header-title { font-size: 0.95rem; font-weight: 800; color: var(--text-title); }
        .board-header-sub { font-size: 0.7rem; color: var(--text-muted); }
        .board-2col-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 6px; flex: 1; }

        .mini-index-card {
            background: var(--bg-subcard);
            border: 1px solid var(--border-line);
            border-radius: 10px;
            padding: 7px 8px;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
            transition: all 0.2s ease;
            cursor: pointer;
            min-height: 58px;
        }
        .mini-index-card:hover { border-color: var(--border-focus); background: var(--btn-hover); transform: translateY(-1px); }
        .mini-top { display: flex; justify-content: space-between; align-items: center; }
        .mini-name { font-size: 0.76rem; font-weight: 800; color: var(--text-title); }
        .mini-tag { font-size: 0.6rem; font-weight: 700; color: var(--text-muted); background: var(--btn-hover); border: 1px solid var(--border-line); padding: 1px 4px; border-radius: 5px; }
        .mini-mid-row { display: flex; justify-content: space-between; align-items: flex-end; margin-top: 3px; }
        .mini-price { font-size: 0.88rem; font-weight: 800; color: var(--text-title); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; line-height: 1.1; }
        .mini-rate { font-size: 0.72rem; font-weight: 700; line-height: 1.1; margin-top: 1px; }

        /* 캘린더 위젯 스타일 */
        .cal-top-title-area { margin-bottom: 10px; }
        .cal-view-tabs {
            display: flex;
            align-items: center;
            gap: 18px;
            height: 30px;
            margin-bottom: 8px;
            border-bottom: 1px solid transparent;
        }
        .cal-view-tab {
            position: relative;
            display: inline-flex;
            align-items: center;
            gap: 5px;
            height: 30px;
            padding: 0 1px;
            border: 0;
            background: transparent;
            color: var(--text-muted);
            font-size: 0.72rem;
            font-weight: 800;
            letter-spacing: -0.2px;
            cursor: pointer;
            white-space: nowrap;
        }
        .cal-view-tab:hover { color: var(--text-title); }
        .cal-view-tab.active { color: var(--text-title); }
        .cal-view-tab.active::after {
            content: "";
            position: absolute;
            left: 0;
            right: 0;
            bottom: -1px;
            height: 2px;
            background: var(--text-title);
            border-radius: 2px;
        }
        .cal-view-custom-icon {
            width: 14px;
            height: 14px;
            flex: 0 0 14px;
        }
        .cal-top-nav { display: flex; align-items: center; justify-content: space-between; margin-bottom: 8px; }
        .cal-date-ctrl { display: flex; align-items: center; gap: 8px; }
        .cal-arrow-btn { background: var(--bg-subcard); border: 1px solid var(--border-line); color: var(--text-sub); width: 26px; height: 26px; border-radius: 8px; display: flex; align-items: center; justify-content: center; cursor: pointer; font-size: 0.75rem; transition: all 0.2s; }
        .cal-arrow-btn:hover { background: var(--btn-hover); color: var(--text-title); border-color: var(--border-focus); }
        .cal-current-date { font-size: 0.98rem; font-weight: 800; color: var(--text-title); letter-spacing: -0.3px; }
        .cal-today-btn { background: var(--bg-subcard); border: 1px solid var(--border-line); color: var(--text-sub); font-size: 0.72rem; font-weight: 700; padding: 3px 8px; border-radius: 8px; cursor: pointer; transition: all 0.2s; }
        .cal-today-btn:hover { background: var(--btn-hover); color: var(--text-title); }

        .cal-filter-bar { display: flex; align-items: center; gap: 4px; margin-bottom: 8px; overflow-x: hidden; padding-bottom: 2px; }
        .cal-filter-pill { background: var(--bg-subcard); border: 1px solid var(--border-line); color: var(--text-muted); font-size: 0.68rem; font-weight: 700; padding: 2px 7px; border-radius: 12px; cursor: pointer; white-space: nowrap; transition: all 0.15s; }
        .cal-filter-pill.active { background: #ffffff; color: #121212; border-color: #ffffff; }
        .cal-filter-pill .star-gold { color: var(--star-gold); }

        .cal-day-header { font-size: 0.73rem; font-weight: 700; color: var(--text-muted); margin: 2px 0 6px; }
        
        .cal-event-list-wrap {
            flex: 1;
            display: flex;
            flex-direction: column;
            gap: 6px;
            overflow: hidden;
            justify-content: flex-start;
        }

        /* 캘린더 카드 고도화: 기존 색상/구조를 유지하면서 여백과 위계를 정리 */
        .cal-card-item {
            position: relative;
            background: var(--bg-cal-card);
            border: 1px solid var(--border-line);
            border-radius: 10px;
            padding: 9px 10px;
            display: flex;
            align-items: flex-start;
            gap: 10px;
            transition: border-color 0.18s ease, background 0.18s ease, transform 0.18s ease;
            cursor: pointer;
            min-height: 72px;
            overflow: hidden;
        }
        .cal-card-item::before {
            content: "";
            position: absolute;
            left: 0;
            top: 10px;
            bottom: 10px;
            width: 2px;
            border-radius: 2px;
            background: transparent;
            opacity: 0.75;
        }
        .cal-card-item:hover {
            border-color: var(--border-focus);
            background: var(--btn-hover);
            transform: translateY(-1px);
        }
        .cal-time-col {
            width: 46px;
            flex-shrink: 0;
            padding-top: 1px;
            font-family: 'Roboto Mono', 'D2Coding', 'Consolas', monospace;
            font-size: 0.72rem;
            font-weight: 800;
            line-height: 1.15;
            color: var(--text-sub);
            font-variant-numeric: tabular-nums;
            letter-spacing: -0.02em;
        }
        .cal-detail-col { flex: 1; min-width: 0; }
        .cal-alarm-icon {
            width: 17px;
            height: 17px;
            display: block;
            fill: none;
            stroke: currentColor;
            stroke-width: 1.65;
            stroke-linecap: round;
            stroke-linejoin: round;
        }
        .cal-alarm-btn {
            flex: 0 0 28px;
            width: 28px;
            height: 28px;
            margin-left: 2px;
            border: 1px solid transparent;
            border-radius: 7px;
            background: transparent;
            color: var(--text-muted);
            font-size: 1rem;
            line-height: 1;
            cursor: pointer;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            align-self: center;
            opacity: 0.78;
            transition: all 0.18s ease;
        }
        .cal-alarm-btn:hover {
            border-color: var(--border-line);
            background: var(--btn-hover);
            opacity: 1;
        }
        .cal-alarm-btn.active {
            color: #10b981;
            border-color: rgba(16,185,129,0.28);
            background: rgba(16,185,129,0.08);
            opacity: 1;
        }
        .cal-event-title {
            font-size: 0.8rem;
            font-weight: 750;
            color: var(--text-title);
            line-height: 1.3;
            word-break: keep-all;
            margin-bottom: 3px;
            letter-spacing: -0.015em;
        }
        .cal-event-meta {
            display: flex;
            align-items: baseline;
            flex-wrap: wrap;
            gap: 2px;
            font-size: 0.66rem;
            color: var(--text-muted);
            line-height: 1.25;
        }
        .cal-actual-val {
            color: #38bdf8;
            font-weight: 750;
            margin-right: 1px;
        }

        /* 중요도 막대: 우리 기존 청록 계열로 절제 */
        .cal-importance-wrap {
            margin-top: 7px;
            width: 100%;
        }
        .cal-importance-bar {
            position: relative;
            width: 100%;
            height: 5px;
            border-radius: 5px;
            background: #2a2f32;
            box-shadow: inset 0 0 0 1px rgba(255,255,255,0.035);
        }
        .cal-importance-bar::before {
            content: "";
            position: absolute;
            inset: 0;
            width: var(--importance-width, 50%);
            border-radius: inherit;
            background: linear-gradient(90deg, #4b5055 0%, #6b7075 100%);
            opacity: 0.9;
        }
        .cal-importance-marker {
            position: absolute;
            top: 50%;
            left: var(--importance-width, 50%);
            width: 9px;
            height: 9px;
            transform: translate(-50%, -50%);
            border-radius: 50%;
            background: #ffffff;
            border: 2px solid #10b981;
            box-shadow: 0 0 0 2px rgba(16,185,129,0.10);
        }
        .cal-importance-labels {
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-top: 4px;
            font-size: 0.55rem;
            line-height: 1;
            color: var(--text-muted);
            font-family: 'Pretendard', 'Noto Sans KR', 'Apple SD Gothic Neo', sans-serif;
        }
        .cal-importance-labels .center {
            color: #10b981;
            font-weight: 800;
        }
        .cal-importance-score {
            margin-left: 3px;
            color: #10b981;
            font-weight: 800;
            font-family: 'Roboto Mono', 'D2Coding', 'Consolas', monospace;
            font-variant-numeric: tabular-nums;
        }

        /* 하단 페이지네이션 바 */
        .cal-pagination-bar {
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 12px;
            padding-top: 8px;
            margin-top: auto;
            border-top: 1px solid var(--border-line);
        }
        .cal-page-btn {
            background: var(--bg-subcard);
            border: 1px solid var(--border-line);
            color: var(--text-sub);
            width: 22px;
            height: 22px;
            border-radius: 6px;
            display: flex;
            align-items: center;
            justify-content: center;
            cursor: pointer;
            font-size: 0.65rem;
            transition: all 0.2s;
        }
        .cal-page-btn:hover:not(:disabled) { background: var(--btn-hover); color: var(--text-title); }
        .cal-page-btn:disabled { opacity: 0.3; cursor: not-allowed; }
        .cal-page-txt { font-size: 0.72rem; font-weight: 700; color: var(--text-muted); }

        /* 퀀트 지표 레이아웃 */
        .gauge-inner-box { width: 62px; height: 62px; border-radius: 50%; border: 4px solid #10b981; display: flex; flex-direction: column; align-items: center; justify-content: center; background: var(--bg-subcard); flex-shrink: 0; }
        .quant-row-item { display: flex; justify-content: space-between; align-items: center; padding: 9px 0; border-bottom: 1px solid var(--border-line); font-size: 0.83rem; }
        .quant-row-item:last-child { border-bottom: none; }
        .quant-row-left { display: flex; flex-direction: column; gap: 2px; min-width: 0; }
        .quant-row-name { font-weight: 700; color: var(--text-title); }
        .quant-row-status { font-size: 0.7rem; font-weight: 700; }
        .quant-row-right { text-align: right; flex-shrink: 0; }
        .quant-row-val { font-weight: 800; color: var(--text-title); }
        .movement-evidence-summary { margin: 2px 0 14px; }
        .movement-evidence-head { display:flex; justify-content:space-between; align-items:center; margin-bottom:7px; }
        .movement-evidence-label { font-size:0.72rem; font-weight:700; color:var(--text-main); }
        .movement-evidence-count { font-size:0.72rem; font-weight:800; color:var(--text-muted); }
        .movement-evidence-track { width:100%; height:7px; display:flex; gap:4px; }
        .movement-evidence-seg { flex:1; height:7px; border-radius:4px; background:rgba(148,163,184,0.15); }
        .movement-evidence-seg.active { background:#10b981; }
        .movement-evidence-seg.active.negative { background:#ef4444; }
        .movement-reason-row { padding:10px 0; border-bottom:1px solid var(--border-line); }
        .movement-reason-row:last-child { border-bottom:none; }
        .movement-reason-top { display:flex; justify-content:space-between; align-items:center; gap:12px; }
        .movement-reason-title { display:flex; align-items:center; gap:7px; min-width:0; }
        .movement-reason-icon { width:18px; height:18px; flex:0 0 18px; color:var(--text-muted); }
        .movement-reason-name { font-size:0.82rem; font-weight:800; color:var(--text-title); }
        .movement-reason-status { font-size:0.66rem; font-weight:800; padding:2px 7px; border-radius:9px; white-space:nowrap; }
        .movement-reason-detail { margin:6px 0 7px 25px; font-size:0.72rem; line-height:1.45; color:var(--text-sub); word-break:keep-all; }
        .movement-reason-meter { margin-left:25px; display:flex; align-items:center; gap:8px; }
        .movement-reason-meter-track { flex:1; height:5px; border-radius:4px; background:rgba(148,163,184,0.12); overflow:hidden; }
        .movement-reason-meter-fill { height:100%; border-radius:4px; }
        .movement-reason-meter-value { width:52px; text-align:right; font-size:0.64rem; font-weight:800; color:var(--text-muted); }
        
        .movement-relative-chart { margin: 4px 0 14px; padding:10px 10px 9px; border:1px solid var(--border-line); border-radius:9px; background:var(--bg-subcard); }
        .movement-relative-head { display:flex; justify-content:space-between; align-items:center; gap:8px; margin-bottom:8px; }
        .movement-relative-title { font-size:0.76rem; font-weight:800; color:var(--text-title); }
        .movement-relative-diff { font-size:0.68rem; font-weight:800; color:var(--text-muted); }
        .movement-relative-chartbox { position:relative; height:92px; overflow:hidden; border-top:1px solid var(--border-line); border-bottom:1px solid var(--border-line); }
        .movement-relative-gridline { position:absolute; left:0; right:0; height:1px; background:var(--border-line); opacity:.55; }
        .movement-relative-svg { width:100%; height:100%; display:block; }
        .movement-relative-legend { display:flex; gap:14px; align-items:center; margin-top:7px; font-size:0.62rem; color:var(--text-sub); }
        .movement-relative-dot { width:7px; height:7px; border-radius:50%; display:inline-block; margin-right:4px; }
        .movement-relative-note { margin-top:5px; font-size:0.62rem; line-height:1.35; color:var(--text-muted); word-break:keep-all; }

.movement-mini-grid { display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:8px; margin-top:2px; }
        .movement-mini-card { min-width:0; background:var(--bg-subcard); border:1px solid var(--border-line); border-radius:9px; padding:9px 8px 8px; }
        .movement-mini-head { display:flex; align-items:center; justify-content:space-between; gap:5px; margin-bottom:7px; }
        .movement-mini-head .movement-reason-title { gap:5px; }
        .movement-mini-head .movement-reason-name { font-size:0.76rem; }
        .movement-mini-head .movement-reason-icon { width:15px; height:15px; flex-basis:15px; }
        .movement-mini-head .movement-reason-status { font-size:0.58rem; padding:2px 5px; }
        .movement-mini-chart { height:58px; display:flex; align-items:flex-end; padding:6px 3px 4px; border-top:1px solid var(--border-line); border-bottom:1px solid var(--border-line); }
        .movement-mini-bars { width:100%; height:100%; display:flex; align-items:flex-end; gap:4px; }
        .movement-mini-bar { flex:1; min-height:8%; border-radius:2px 2px 0 0; }
        .movement-mini-value { display:flex; align-items:baseline; gap:2px; margin-top:7px; font-size:0.8rem; font-weight:800; color:var(--text-title); }
        .movement-mini-unit { font-size:0.62rem; font-weight:700; color:var(--text-muted); }
        .movement-mini-detail { min-height:30px; margin-top:3px; font-size:0.62rem; line-height:1.35; color:var(--text-sub); word-break:keep-all; }
        .movement-mini-card .movement-reason-meter { margin:7px 0 0; }
        .movement-mini-card .movement-reason-meter-value { width:24px; font-size:0.58rem; }
        .movement-market5-card { overflow:hidden; }
        .market5-line-chart { position:relative; height:58px; margin:5px 0 4px; border-top:1px solid var(--border-line); border-bottom:1px solid var(--border-line); }
        .market5-line-chart svg { width:100%; height:100%; display:block; }
        .market5-zero { position:absolute; left:0; right:0; height:1px; background:rgba(148,163,184,0.18); }
        .market5-bars { height:42px; display:flex; align-items:flex-end; gap:4px; padding:4px 2px 0; border-bottom:1px solid var(--border-line); }
        .market5-bar-wrap { flex:1; height:100%; display:flex; flex-direction:column; justify-content:flex-end; align-items:center; min-width:0; }
        .market5-bar { width:100%; min-height:4px; border-radius:2px 2px 0 0; display:block; opacity:.85; }
        .market5-bar-label { font-size:.46rem; color:var(--text-muted); margin-top:2px; white-space:nowrap; }

        @media (max-width: 980px) {
            .movement-mini-grid { grid-template-columns:repeat(2,minmax(0,1fr)); }
        }
        @media (max-width: 560px) {
            .movement-mini-grid { grid-template-columns:1fr; }
        }


        .quant-row-subval { font-size: 0.7rem; color: var(--text-muted); margin-top: 1px; }

        .status-pill-positive { color: #10b981; }
        .status-pill-negative { color: #ef4444; }
        .status-pill-warning { color: #d97706; }
        .status-pill-neutral { color: var(--text-muted); }

        .dart-item { padding: 9px 0; border-bottom: 1px solid var(--border-line); text-decoration: none; display: flex; align-items: center; justify-content: space-between; gap: 8px; }
        .dart-item:last-child { border-bottom: none; }
        .dart-title { color: var(--text-main); font-size: 0.83rem; font-weight: 500; line-height: 1.4; flex: 1; min-width: 0; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
        .dart-title:hover { color: #0284c7; }
        .dart-tag { font-size: 0.68rem; font-weight: 700; padding: 2px 6px; border-radius: 6px; flex-shrink: 0; }
        .tag-positive { background: rgba(16, 185, 129, 0.15); color: #10b981; border: 1px solid rgba(16, 185, 129, 0.3); }
        .tag-negative { background: rgba(239, 68, 68, 0.15); color: #ef4444; border: 1px solid rgba(239, 68, 68, 0.3); }
        .tag-warning { background: rgba(249, 115, 22, 0.15); color: #f97316; border: 1px solid rgba(249, 115, 22, 0.3); }
        .tag-neutral { background: var(--btn-hover); color: var(--text-muted); border: 1px solid var(--border-line); }

        .vote-btn-wrap { display: flex; gap: 10px; margin-top: 10px; }
        .vote-btn { flex: 1; padding: 10px; border-radius: 12px; border: 1px solid var(--border-line); background: var(--bg-subcard); color: var(--text-title); font-size: 0.88rem; font-weight: 700; cursor: pointer; display: flex; align-items: center; justify-content: center; gap: 6px; transition: all 0.2s; }
        .vote-btn:hover { background: var(--btn-hover); border-color: var(--border-focus); }
        .vote-btn.active-long { background: rgba(16, 185, 129, 0.15); border-color: #10b981; color: #10b981; }
        .vote-btn.active-short { background: rgba(56, 189, 248, 0.15); border-color: #38bdf8; color: #38bdf8; }

        .comment-input-box { display: flex; gap: 8px; margin-top: 12px; }
        .comment-input-box input { flex: 1; min-width: 0; background: var(--bg-subcard); border: 1px solid var(--border-line); border-radius: 20px; padding: 9px 14px; color: var(--text-title); font-size: 0.82rem; outline: none; }
        .comment-input-box button { background: #10b981; border: none; color: #ffffff; font-size: 0.8rem; font-weight: 700; padding: 0 16px; border-radius: 20px; cursor: pointer; flex-shrink: 0; }
        .comment-item { padding: 9px 0; border-bottom: 1px solid var(--border-line); font-size: 0.82rem; }
        .comment-item:last-child { border-bottom: none; }
        .comment-head { display: flex; justify-content: space-between; color: var(--text-muted); font-size: 0.7rem; margin-bottom: 3px; }
        .comment-nick { font-weight: 700; color: #10b981; }
        .comment-body { color: var(--text-main); word-break: break-all; }

        .signal-premium-card {
            background: var(--bg-card) !important;
            border: 1.8px solid #10b981 !important;
            border-radius: 18px !important;
            padding: 18px 20px !important;
            box-shadow: 0 6px 24px var(--signal-glow) !important;
            transition: all 0.25s ease;
        }

        .signal-card-header { display: flex; align-items: center; justify-content: center; position: relative; cursor: pointer; user-select: none; padding: 6px 0; }
        .signal-card-title { font-size: 1.15rem !important; font-weight: 800 !important; color: #10b981 !important; text-align: center; letter-spacing: -0.4px; }
        .signal-arrow-icon { width: 22px; height: 22px; fill: #10b981 !important; position: absolute; right: 4px; transition: transform 0.25s ease; }

        .signal-premium-btn {
            width: 100%;
            background: var(--bg-card);
            border: 1.8px solid #10b981;
            border-radius: 16px;
            padding: 15px 18px;
            display: flex;
            align-items: center;
            justify-content: center;
            color: #10b981;
            font-size: 0.95rem;
            font-weight: 800;
            text-decoration: none;
            cursor: pointer;
            box-shadow: 0 4px 18px var(--signal-glow);
            transition: all 0.2s ease;
            text-align: center;
        }
        .signal-premium-btn:hover { background: rgba(16, 185, 129, 0.1); border-color: #059669; color: #059669; }

        .guide-box { padding: 14px 0 0 0; margin-top: 14px; font-size: 0.85rem; line-height: 1.6; color: var(--text-sub); border-top: 1px solid rgba(16, 185, 129, 0.2); }
        .guide-title { font-weight: 800; color: #10b981; margin-bottom: 6px; display: flex; align-items: center; gap: 4px; font-size: 0.92rem; }

        .news-item { height: 60px; box-sizing: border-box; padding: 10px 0; border-bottom: 1px solid var(--border-line); text-decoration: none; display: flex; flex-direction: column; justify-content: center; overflow: hidden; }
        .news-item:last-child { border-bottom: none; }
        .news-title { color: var(--text-main); font-size: 0.85rem; font-weight: 500; line-height: 1.4; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
        .news-title:hover { color: #0284c7; }
        .news-meta { color: var(--text-muted); font-size: 0.72rem; margin-top: 4px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }

        #bottom-bar {
            display: none;
            position: fixed;
            bottom: 0;
            left: 0;
            right: 0;
            height: 74px;
            background: var(--bottom-fade);
            align-items: center;
            justify-content: center;
            padding: 0 12px 6px;
            z-index: 50;
        }
        
        .bottom-bar-grid-wrapper {
            width: 100%;
            max-width: 1196px;
            display: flex;
            justify-content: center;
            align-items: center;
            margin: 0 auto;
        }
        .bottom-bar-grid-wrapper .search-container {
            width: 100%;
            max-width: 832px;
            margin: 0 auto;
        }

        .loading { display: none; text-align: center; color: #0284c7; margin: 40px 0; font-size: 0.9rem; }

        .dot-marker-style {
            position: absolute;
            top: -4px;
            width: 14px;
            height: 14px;
            border-radius: 50%;
            background: #10b981;
            border: 2px solid #ffffff;
            box-shadow: 0 1px 4px rgba(0,0,0,0.3);
            transform: translateX(-50%);
        }

        .val-up { color: #ef4444; }
        .val-down { color: #38bdf8; }

        /* [모바일 1열 순차 정렬] */
        @media (max-width: 1200px) {
            .dashboard-master-grid {
                display: flex;
                flex-direction: column;
                align-items: center;
                max-width: 832px;
                gap: 14px;
            }
            .card, .signal-premium-card, .signal-premium-btn {
                width: 100%;
                max-width: 832px;
            }
            
            /* 모바일 위젯 표시 순서 — PC 그리드에는 영향 없음 */
            #rowChart { order: 2; }
            #rowNews { order: 3; }
            #rowTrack { order: 4; }
            #rowQuant { order: 5; }
            #rowConsensus { order: 6; }
            #rowVote { order: 7; }
            #rowComment { order: 8; }
            #rowWorld { order: 11; width: 100%; max-width: 832px; }
            #rowGlobalNews { order: 12; width: 100%; max-width: 832px; }
            #rowEarningsCalendar { order: 13; }
            #rowEconomicCalendar { order: 14; }
            #usLeadingRightStack {
                position: static;
                grid-column: auto;
                grid-row: auto;
                order: 15;
                width: 100%;
                max-width: 832px;
                z-index: auto;
            }
            #rowUsLeadingSectors,
            #rowUsLeadingStocks {
                width: 100%;
                max-width: 832px;
            }

            #rowDart { order: 16; }

            .bottom-bar-grid-wrapper {
                display: flex;
                justify-content: center;
                max-width: 832px;
            }
        }

        /* ============================================================
           캘린더 최종 디자인 v8
           - PC: 기존 832px : 348px 구조 유지
           - 좌/우 카드 높이 동기화
           - Pretendard/Noto Sans KR 계열의 큰 한글 가독성
           - 경제/실적 캘린더 공통 디자인
        ============================================================ */
        .calendar-widget {
            padding: 18px 16px 14px !important;
            border-radius: 16px !important;
            background: #1b1c1d !important;
            border: 1px solid #2b2d30 !important;
            box-shadow: none !important;
            min-height: 0 !important;
        }
        .calendar-widget,
        .calendar-widget * {
            font-family: 'Pretendard', 'Noto Sans KR', 'Apple SD Gothic Neo', 'Malgun Gothic', sans-serif;
        }
        .calendar-widget-title {
            font-size: 1.15rem !important;
            line-height: 1.35 !important;
            font-weight: 800 !important;
            letter-spacing: -0.045em !important;
            margin-bottom: 13px !important;
            color: #f4f5f5 !important;
        }
        .calendar-widget-nav {
            margin-bottom: 10px !important;
            min-height: 36px;
        }
        .calendar-widget .cal-date-ctrl {
            gap: 10px !important;
        }
        .calendar-widget .cal-arrow-btn {
            width: 32px !important;
            height: 32px !important;
            border-radius: 9px !important;
            font-size: 0.82rem !important;
            background: #111213 !important;
            border-color: #303235 !important;
            color: #e8e9e9 !important;
        }
        .calendar-widget .cal-current-date {
            min-width: 132px !important;
            font-size: 0.94rem !important;
            font-weight: 800 !important;
            letter-spacing: -0.025em !important;
            color: #f5f6f6 !important;
        }
        .calendar-widget .cal-today-btn {
            font-size: 0.78rem !important;
            padding: 6px 10px !important;
            min-height: 32px;
            border-radius: 9px !important;
            background: #111213 !important;
            border-color: #303235 !important;
            color: #f0f1f1 !important;
        }
        .calendar-star-filter {
            gap: 7px !important;
            margin-bottom: 11px !important;
            padding-bottom: 1px;
        }
        .calendar-star-btn {
            min-width: 48px;
            min-height: 30px;
            padding: 5px 10px !important;
            border-radius: 999px !important;
            font-size: 0.72rem !important;
            letter-spacing: -0.02em;
            background: #202224 !important;
            border-color: #2d3033 !important;
            color: #b9bdbe !important;
        }
        .calendar-star-btn.active {
            background: #f4f5f5 !important;
            color: #111213 !important;
            border-color: #f4f5f5 !important;
        }
        .calendar-widget-subtitle {
            font-size: 0.78rem !important;
            line-height: 1.35 !important;
            font-weight: 750 !important;
            margin: 1px 0 10px !important;
            color: #aeb2b3 !important;
        }
        .calendar-widget-list {
            gap: 9px !important;
            overflow-y: auto !important;
            overflow-x: hidden !important;
            scrollbar-width: thin;
            padding-right: 1px;
        }
        .calendar-widget-list .cal-card-item {
            min-height: 78px !important;
            padding: 11px 11px 11px 12px !important;
            gap: 10px !important;
            border-radius: 11px !important;
            background: #202224 !important;
            border: 1px solid #2b2e31 !important;
            align-items: center !important;
            transform: none !important;
        }
        .calendar-widget-list .cal-card-item::before {
            top: 10px !important;
            bottom: 10px !important;
            width: 2px !important;
            background: transparent !important;
        }
        .calendar-widget-list .cal-card-item:hover {
            background: #232527 !important;
            border-color: #383b3e !important;
        }
        .calendar-widget-list .cal-time-col {
            min-width: 54px !important;
            width: 54px !important;
            font-family: 'Pretendard', 'Noto Sans KR', sans-serif !important;
            font-size: 0.76rem !important;
            font-weight: 800 !important;
            line-height: 1.2 !important;
            color: #eef0f0 !important;
            letter-spacing: -0.025em !important;
        }
        .calendar-widget .cal-event-icon {
            width: 34px !important;
            height: 34px !important;
            min-width: 34px !important;
            border-radius: 9px !important;
            background: #151718 !important;
            border-color: #34373a !important;
            font-size: 0.62rem !important;
        }
        .calendar-widget .cal-event-topline {
            gap: 7px !important;
            align-items: center !important;
        }
        .calendar-widget .cal-stars {
            font-size: 0.72rem !important;
            letter-spacing: -0.08em !important;
            color: #ffb000 !important;
        }
        .calendar-widget .cal-event-title {
            font-size: 0.86rem !important;
            line-height: 1.38 !important;
            font-weight: 800 !important;
            letter-spacing: -0.045em !important;
            color: #f3f4f4 !important;
            margin-bottom: 3px !important;
        }
        .calendar-widget .cal-event-meta {
            font-size: 0.68rem !important;
            line-height: 1.35 !important;
            color: #aeb2b3 !important;
            letter-spacing: -0.025em !important;
        }
        .calendar-widget .cal-alarm-btn {
            width: 30px !important;
            height: 30px !important;
            flex-basis: 30px !important;
            border-radius: 8px !important;
            color: #8d9293 !important;
        }
        .calendar-widget .cal-pagination-bar {
            min-height: 36px;
            padding-top: 9px !important;
            margin-top: 9px !important;
            border-top-color: #2a2c2e !important;
        }
        .calendar-widget .cal-page-btn {
            width: 28px !important;
            height: 28px !important;
            border-radius: 8px !important;
            background: #151718 !important;
        }
        .calendar-widget .cal-page-txt {
            font-size: 0.7rem !important;
            color: #9da1a2 !important;
        }
        /* 빈 상태도 중앙에 덩그러니 떠 보이지 않게 위계 조정 */
        .calendar-widget-list > div[style*="height:100%"] {
            min-height: 180px !important;
            height: auto !important;
            flex: 1 1 auto !important;
            align-items: center !important;
            justify-content: center !important;
            font-size: 0.78rem !important;
            color: #8f9495 !important;
        }
        @media (min-width: 1201px) {
            .dashboard-master-grid {
                grid-template-columns: minmax(0, 832px) 348px !important;
                column-gap: 16px !important;
            }
            #rowTrack, #rowEarningsCalendar,
            #rowQuant, #rowEconomicCalendar {
                min-height: 560px;
            }
        }
        @media (max-width: 1200px) {
            .calendar-widget {
                padding: 18px 16px 14px !important;
            }
            .calendar-widget-list .cal-card-item {
                min-height: 78px !important;
            }
        }
        @media (max-width: 520px) {
            .calendar-widget {
                padding: 15px 14px 12px !important;
                border-radius: 14px !important;
            }
            .calendar-widget-title { font-size: 1.08rem !important; }
            .calendar-widget .cal-current-date {
                min-width: 116px !important;
                font-size: 0.88rem !important;
            }
            .calendar-widget .cal-arrow-btn { width: 30px !important; height: 30px !important; }
            .calendar-widget .cal-today-btn { min-height: 30px; padding: 5px 9px !important; }
            .calendar-star-btn { min-width: 46px; min-height: 29px; font-size: 0.69rem !important; }
            .calendar-widget-list .cal-card-item { min-height: 74px !important; }
            .calendar-widget-list .cal-time-col { min-width: 50px !important; width: 50px !important; font-size: 0.72rem !important; }
            .calendar-widget .cal-event-icon { width: 32px !important; height: 32px !important; min-width: 32px !important; }
            .calendar-widget .cal-event-title { font-size: 0.82rem !important; }
        }


        .calendar-widget .calendar-full-btn {
            flex: 0 0 auto;
            margin-top: 9px;
            min-height: 42px;
            border-radius: 10px;
            background: #202224;
            border: 1px solid #2b2e31;
            color: #e9ebeb;
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 10px;
            font-size: 0.76rem;
            font-weight: 750;
            letter-spacing: -0.025em;
            cursor: pointer;
        }
        .calendar-widget .calendar-full-btn span { font-size: 0.9rem; color: #dfe2e2; }
        .calendar-widget .calendar-full-btn:hover { background: #252729; border-color: #383b3e; }


        /* ============================================================
           v14: 경제/실적 캘린더 카드 최종 배치
           시간 | 별표 + 발표대기 / 일정명 / 상세정보 | 알림
           두 캘린더 완전히 동일한 내부 구조
           ============================================================ */
        .calendar-widget-list .cal-card-item {
            display: grid !important;
            grid-template-columns: 54px minmax(0, 1fr) 30px !important;
            grid-template-rows: 1fr !important;
            column-gap: 10px !important;
            align-items: center !important;
            width: 100% !important;
            box-sizing: border-box !important;
        }

        .calendar-widget-list .cal-time-col {
            grid-column: 1 !important;
            grid-row: 1 !important;
            width: 54px !important;
            min-width: 54px !important;
            padding: 0 !important;
            margin: 0 !important;
            background: transparent !important;
            border: 0 !important;
            align-self: center !important;
            text-align: left !important;
        }

        .calendar-widget-list .cal-detail-col {
            grid-column: 2 !important;
            grid-row: 1 !important;
            min-width: 0 !important;
            width: 100% !important;
            overflow: hidden !important;
            display: flex !important;
            flex-direction: column !important;
            justify-content: center !important;
            align-items: flex-start !important;
            gap: 2px !important;
        }

        /* 별표 + 발표대기는 반드시 일정명보다 위 */
        .calendar-widget-list .cal-event-topline {
            order: 1 !important;
            display: flex !important;
            flex-direction: row !important;
            align-items: center !important;
            justify-content: flex-start !important;
            gap: 7px !important;
            width: 100% !important;
            min-width: 0 !important;
            min-height: 16px !important;
            margin: 0 !important;
        }

        .calendar-widget-list .cal-stars {
            order: 1 !important;
            flex: 0 0 auto !important;
            width: auto !important;
            min-width: 0 !important;
            margin: 0 !important;
            padding: 0 !important;
            display: inline-block !important;
            white-space: nowrap !important;
            font-size: 0.72rem !important;
            line-height: 1.2 !important;
            letter-spacing: -0.08em !important;
            color: #ffb000 !important;
        }

        .calendar-widget-list .cal-event-status {
            order: 2 !important;
            flex: 0 0 auto !important;
            display: inline-block !important;
            font-size: 0.66rem !important;
            line-height: 1.2 !important;
            color: #aeb2b3 !important;
            white-space: nowrap !important;
            margin: 0 !important;
        }

        .calendar-widget-list .cal-event-title {
            order: 2 !important;
            display: block !important;
            width: 100% !important;
            min-width: 0 !important;
            margin: 0 !important;
            padding: 0 !important;
            overflow-wrap: anywhere !important;
            word-break: keep-all !important;
            font-size: 0.86rem !important;
            line-height: 1.32 !important;
            font-weight: 800 !important;
            color: #f3f4f4 !important;
        }

        .calendar-widget-list .cal-event-meta {
            order: 3 !important;
            display: block !important;
            width: 100% !important;
            min-width: 0 !important;
            margin: 0 !important;
            padding: 0 !important;
            overflow-wrap: anywhere !important;
            font-size: 0.68rem !important;
            line-height: 1.3 !important;
            color: #aeb2b3 !important;
        }

        .calendar-widget-list .cal-alarm-btn {
            grid-column: 3 !important;
            grid-row: 1 !important;
            width: 30px !important;
            min-width: 30px !important;
            height: 30px !important;
            margin: 0 !important;
            padding: 0 !important;
            justify-self: end !important;
            align-self: center !important;
            flex: none !important;
        }

        /* ============================================================
           v13: 경제/실적 캘린더 공통 내부 구조
           시간 | 별표+발표상태+일정내용 | 알람
           경제/실적 모두 동일한 CSS 구조를 사용
           ============================================================ */
        .calendar-widget-list .cal-card-item {
            display: grid !important;
            grid-template-columns: 54px minmax(0, 1fr) 30px !important;
            column-gap: 10px !important;
            align-items: center !important;
            width: 100%;
            box-sizing: border-box;
        }
        .calendar-widget-list .cal-time-col {
            width: 54px !important;
            min-width: 54px !important;
            padding: 0 !important;
            margin: 0 !important;
            align-self: center;
        }
        .calendar-widget-list .cal-detail-col {
            grid-column: 2;
            min-width: 0 !important;
            width: auto !important;
            overflow: hidden;
            display: flex !important;
            flex-direction: column !important;
            justify-content: center !important;
            gap: 2px !important;
        }
        .calendar-widget-list .cal-event-topline {
            display: flex !important;
            align-items: center !important;
            gap: 7px !important;
            min-width: 0;
            min-height: 16px;
        }
        .calendar-widget-list .cal-stars {
            flex: 0 0 auto !important;
            width: auto !important;
            min-width: 0 !important;
            margin: 0 !important;
            padding: 0 !important;
            display: inline-block !important;
            text-align: left !important;
            white-space: nowrap !important;
            font-size: 0.72rem !important;
            line-height: 1.2 !important;
            letter-spacing: -0.08em !important;
            color: #ffb000 !important;
        }
        .calendar-widget-list .cal-event-status {
            flex: 0 0 auto;
            font-size: 0.66rem;
            line-height: 1.2;
            color: #aeb2b3;
            white-space: nowrap;
        }
        .calendar-widget-list .cal-event-title {
            margin: 0 !important;
            min-width: 0;
            overflow-wrap: anywhere;
            word-break: keep-all;
            font-size: 0.86rem !important;
            line-height: 1.32 !important;
            font-weight: 800 !important;
        }
        .calendar-widget-list .cal-event-meta {
            margin: 0 !important;
            min-width: 0;
            overflow-wrap: anywhere;
            font-size: 0.68rem !important;
            line-height: 1.3 !important;
        }
        .calendar-widget-list .cal-event-icon {
            display: none !important;
        }
        .calendar-widget-list .cal-alarm-btn {
            grid-column: 3;
            width: 30px !important;
            min-width: 30px !important;
            height: 30px !important;
            flex: none !important;
            justify-self: end;
            margin: 0 !important;
        }
        @media (max-width: 520px) {
            .calendar-widget-list .cal-card-item {
                grid-template-columns: 50px minmax(0, 1fr) 28px !important;
                column-gap: 8px !important;
            }
            .calendar-widget-list .cal-time-col {
                width: 50px !important;
                min-width: 50px !important;
            }
            .calendar-widget-list .cal-alarm-btn {
                width: 28px !important;
                min-width: 28px !important;
                height: 28px !important;
            }
            .calendar-widget-list .cal-event-title {
                font-size: 0.82rem !important;
            }
            .calendar-widget-list .cal-event-status {
                font-size: 0.62rem !important;
            }
        }


        /* ============================================================
           v15: 다크/화이트 모드 캘린더 색상 연동
           - 기존 캘린더에 고정된 다크 색상을 제거
           - body.light-theme의 전역 CSS 변수와 연결
           - 캘린더 2개 모두 동일하게 적용
           ============================================================ */
        body.light-theme .calendar-widget {
            background: var(--bg-card) !important;
            border-color: var(--border-line) !important;
            color: var(--text-main) !important;
        }

        body.light-theme .calendar-widget-title,
        body.light-theme .calendar-widget .cal-current-date {
            color: var(--text-title) !important;
        }

        body.light-theme .calendar-widget .cal-arrow-btn,
        body.light-theme .calendar-widget .cal-today-btn {
            background: var(--bg-subcard) !important;
            border-color: var(--border-focus) !important;
            color: var(--text-main) !important;
        }

        body.light-theme .calendar-star-btn {
            background: var(--bg-subcard) !important;
            border-color: var(--border-line) !important;
            color: var(--text-muted) !important;
        }

        body.light-theme .calendar-star-btn.active {
            background: #202124 !important;
            border-color: #202124 !important;
            color: #ffffff !important;
        }

        body.light-theme .calendar-widget-subtitle,
        body.light-theme .calendar-widget .cal-event-meta,
        body.light-theme .calendar-widget .cal-event-status,
        body.light-theme .calendar-widget .cal-page-txt {
            color: var(--text-muted) !important;
        }

        body.light-theme .calendar-widget-list .cal-card-item {
            background: #f8f9fa !important;
            border-color: var(--border-line) !important;
        }

        body.light-theme .calendar-widget-list .cal-card-item:hover {
            background: #ffffff !important;
            border-color: var(--border-focus) !important;
        }

        body.light-theme .calendar-widget-list .cal-time-col,
        body.light-theme .calendar-widget .cal-event-title {
            color: var(--text-main) !important;
        }

        body.light-theme .calendar-widget .cal-alarm-btn {
            color: #5f6368 !important;
        }

        body.light-theme .calendar-widget .cal-pagination-bar {
            border-top-color: var(--border-line) !important;
        }

        body.light-theme .calendar-widget .cal-page-btn {
            background: var(--bg-subcard) !important;
            border-color: var(--border-line) !important;
            color: var(--text-muted) !important;
        }

        body.light-theme .calendar-widget-list > div[style*="height:100%"] {
            color: var(--text-muted) !important;
        }

    
/* 오늘 버튼: 텍스트 대신 새로고침 기호(↻) 사용 */
.cal-today-btn {
    font-size: 1.15rem !important;
    line-height: 1 !important;
    font-weight: 700 !important;
    padding: 6px 10px !important;
    min-width: 36px !important;
    min-height: 34px !important;
}


        .consensus-more-btn {
            display: block;
            width: 100%;
            margin-top: 7px;
            padding: 6px 0;
            border: 1px solid var(--border-line);
            border-radius: 8px;
            background: transparent;
            color: var(--text-muted);
            font-size: 0.72rem;
            font-weight: 700;
            cursor: pointer;
        }

        .consensus-more-btn:hover {
            color: var(--text-title);
        }

        /* 미국 주도업종: PC 애널리스트 오른쪽 / 모바일 경제 캘린더 바로 아래 */
        #rowUsLeadingSectors {
            grid-column: 2;
            grid-row: 6;
            min-height: 0;
            align-self: stretch;
        }

        .us-leading-sector-card {
            padding: 16px;
            box-sizing: border-box;
            background: var(--bg-card);
            border: 1px solid var(--border-line);
            border-radius: 16px;
            display: flex;
            flex-direction: column;
            justify-content: flex-start;
            overflow: hidden;
        }

        .us-leading-sector-head {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 8px;
            margin-bottom: 8px;
            padding-bottom: 6px;
            border-bottom: 1px solid var(--border-line);
            flex: 0 0 auto;
        }

        .us-leading-sector-title {
            font-size: 0.95rem;
            font-weight: 800;
            color: var(--text-title);
            white-space: nowrap;
        }

        .us-leading-sector-sub {
            font-size: 0.68rem;
            color: var(--text-muted);
            white-space: nowrap;
        }

        .us-leading-sector-table {
            width: 100%;
            border-collapse: collapse;
            table-layout: fixed;
        }

        .us-leading-sector-table th {
            font-size: 0.60rem;
            font-weight: 700;
            color: var(--text-muted);
            padding: 4px 2px 6px;
            text-align: right;
            white-space: nowrap;
        }

        .us-leading-sector-table th:first-child { text-align: left; }

        .us-leading-sector-table td {
            font-size: 0.70rem;
            color: var(--text-sub);
            padding: 7px 2px;
            border-top: 1px solid var(--border-line);
            text-align: right;
            white-space: nowrap;
        }

        .us-leading-sector-table td:first-child { text-align: left; }

        .us-leading-sector-row {
            cursor: pointer;
        }

        .us-leading-name {
            width: 116px;
            color: var(--text-title) !important;
            font-weight: 700;
            overflow: hidden;
            text-overflow: ellipsis;
        }

        .us-leading-up { color: #ef4444 !important; font-weight: 700; }
        .us-leading-down { color: #38bdf8 !important; font-weight: 700; }

        .us-leading-sector-loading {
            font-size: 0.72rem;
            color: var(--text-muted);
            padding: 16px 0;
            text-align: center;
        }

        /* 미국 주도종목: 1단계 디자인 전용 */
        /* 미국 주도업종 + 주도종목을 오른쪽 전용 스택으로 분리.
           왼쪽 개미투표와 그리드 행 높이를 공유하지 않도록 한다. */
        #usLeadingRightStack {
            grid-column: 2;
            grid-row: 6;
            position: absolute;
            align-self: start;
            justify-self: stretch;
            width: 100%;
            min-width: 0;
            min-height: 0;
            box-sizing: border-box;
            display: flex;
            flex-direction: column;
            gap: 12px;
            z-index: 2;
        }

        #usLeadingRightStack > .card {
            width: 100%;
            min-width: 0;
        }

        #rowUsLeadingStocks {
            grid-column: auto;
            grid-row: auto;
            align-self: auto;
            min-height: 0;
        }

        .us-leading-stock-card {
            padding: 16px;
            box-sizing: border-box;
            background: var(--bg-card);
            border: 1px solid var(--border-line);
            border-radius: 16px;
            overflow: hidden;
        }

        .us-leading-stock-head {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 8px;
            margin-bottom: 8px;
            padding-bottom: 6px;
            border-bottom: 1px solid var(--border-line);
        }

        .us-leading-stock-title {
            font-size: 0.95rem;
            font-weight: 800;
            color: var(--text-title);
            white-space: nowrap;
        }

        .us-leading-stock-sub {
            font-size: 0.68rem;
            color: var(--text-muted);
            white-space: nowrap;
        }

        .us-leading-stock-list {
            display: flex;
            flex-direction: column;
            gap: 7px;
        }

        .us-leading-stock-item {
            display: grid;
            grid-template-columns: minmax(72px, 1fr) 74px minmax(72px, 1.15fr);
            align-items: center;
            gap: 8px;
            min-height: 58px;
            padding: 7px 8px;
            box-sizing: border-box;
            background: var(--bg-subcard);
            border: 1px solid var(--border-line);
            border-radius: 10px;
        }

        .us-leading-stock-name {
            min-width: 0;
            overflow: hidden;
        }

        .us-leading-stock-ticker {
            font-size: 0.78rem;
            line-height: 1.15;
            font-weight: 800;
            color: var(--text-title);
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }

        .us-leading-stock-company {
            margin-top: 3px;
            font-size: 0.62rem;
            line-height: 1.15;
            color: var(--text-muted);
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }

        .us-leading-stock-price {
            min-width: 0;
            text-align: right;
        }

        .us-leading-stock-value {
            font-size: 0.75rem;
            line-height: 1.15;
            font-weight: 800;
            color: var(--text-title);
            white-space: nowrap;
        }

        .us-leading-stock-change {
            margin-top: 3px;
            font-size: 0.68rem;
            line-height: 1.15;
            font-weight: 800;
            white-space: nowrap;
        }

        .us-leading-stock-change.up { color: #ef4444; }
        .us-leading-stock-change.down { color: #38bdf8; }

        .us-leading-stock-spark {
            width: 100%;
            min-width: 72px;
            height: 34px;
            display: block;
            overflow: visible;
            justify-self: end;
            filter: drop-shadow(0 2px 3px rgba(16, 185, 129, 0.20));
        }

        .us-leading-stock-spark polyline {
            fill: none;
            stroke: #10b981;
            stroke-width: 2.2;
            stroke-linecap: round;
            stroke-linejoin: round;
            vector-effect: non-scaling-stroke;
        }


        /* 모바일 최종 배치 고정:
           PC의 오른쪽 독립 배치는 유지하고 모바일에서는 1열 일반 흐름으로 복귀 */
        @media (max-width: 1200px) {
            .dashboard-master-grid {
                position: relative;
                display: flex;
                flex-direction: column;
                align-items: center;
                max-width: 832px;
                gap: 14px;
            }

            #usLeadingRightStack {
                position: static !important;
                grid-column: auto !important;
                grid-row: auto !important;
                order: 15 !important;
                align-self: center !important;
                justify-self: auto !important;
                width: 100% !important;
                max-width: 832px !important;
                min-width: 0 !important;
                min-height: 0 !important;
                box-sizing: border-box !important;
                display: flex !important;
                flex-direction: column !important;
                gap: 14px !important;
                z-index: auto !important;
            }

            #usLeadingRightStack > .card,
            #rowUsLeadingSectors,
            #rowUsLeadingStocks {
                width: 100% !important;
                max-width: 832px !important;
                min-width: 0 !important;
                box-sizing: border-box !important;
            }
        }


        /* 모바일 최종 폭/흐름 보정 */
        @media (max-width: 1200px) {
            #result-scroll-view {
                width: 100% !important;
                box-sizing: border-box !important;
                overflow-x: hidden !important;
            }

            .result-inner {
                width: 100% !important;
                max-width: 832px !important;
                box-sizing: border-box !important;
                margin: 0 auto !important;
            }

            .dashboard-master-grid {
                display: flex !important;
                flex-direction: column !important;
                width: 100% !important;
                max-width: 832px !important;
                margin: 0 auto !important;
                box-sizing: border-box !important;
                gap: 14px !important;
                overflow: visible !important;
            }

            .dashboard-master-grid > .card,
            .dashboard-master-grid > .signal-premium-card,
            .dashboard-master-grid > .signal-premium-btn,
            #usLeadingRightStack {
                position: static !important;
                float: none !important;
                left: auto !important;
                right: auto !important;
                top: auto !important;
                width: 100% !important;
                max-width: 832px !important;
                min-width: 0 !important;
                margin-left: 0 !important;
                margin-right: 0 !important;
                box-sizing: border-box !important;
                transform: none !important;
            }

            #usLeadingRightStack {
                order: 15 !important;
                display: flex !important;
                flex-direction: column !important;
                gap: 14px !important;
            }

            #rowUsLeadingSectors,
            #rowUsLeadingStocks {
                width: 100% !important;
                max-width: 832px !important;
                min-width: 0 !important;
                box-sizing: border-box !important;
            }
        }

        /* ============================================================
           모바일 레이아웃 최종 격리
           - PC의 grid-column/grid-row/height:100%를 모바일에서 전부 초기화
           - 증시(rowWorld)가 오른쪽으로 붙는 현상 방지
           - '왜 올라갔을까'와 높이를 공유하는 현상 방지
           - 미국 주도업종/주도종목은 경제캘린더 아래 독립된 순서로 배치
        ============================================================ */
        @media (max-width: 1200px) {
            #result-scroll-view {
                width: 100vw !important;
                max-width: 100vw !important;
                padding-left: 12px !important;
                padding-right: 12px !important;
                box-sizing: border-box !important;
                overflow-x: hidden !important;
            }

            .result-inner {
                width: 100% !important;
                max-width: 832px !important;
                margin: 0 auto !important;
                box-sizing: border-box !important;
            }

            .dashboard-master-grid {
                display: flex !important;
                flex-direction: column !important;
                align-items: stretch !important;
                justify-content: flex-start !important;
                width: 100% !important;
                max-width: 832px !important;
                margin: 0 auto !important;
                gap: 14px !important;
                box-sizing: border-box !important;
                position: relative !important;
            }

            /* PC 2열 grid의 위치/높이 값을 모바일에서 완전 초기화 */
            .dashboard-master-grid > * {
                grid-column: auto !important;
                grid-row: auto !important;
                float: none !important;
                position: static !important;
                top: auto !important;
                right: auto !important;
                bottom: auto !important;
                left: auto !important;
                transform: none !important;
                height: auto !important;
                min-height: 0 !important;
                width: 100% !important;
                max-width: 832px !important;
                min-width: 0 !important;
                margin-left: 0 !important;
                margin-right: 0 !important;
                align-self: stretch !important;
                box-sizing: border-box !important;
            }

            /* 기존 모바일 순서를 명확하게 재지정 */
            #rowChart { order: 2 !important; }
            #rowNews { order: 3 !important; }
            #rowTrack { order: 4 !important; }
            #rowQuant { order: 5 !important; }
            #rowConsensus { order: 6 !important; }
            #rowVote { order: 7 !important; }
            #rowComment { order: 8 !important; }
            #rowWorld { order: 11 !important; }
            #rowGlobalNews { order: 12 !important; }
            #rowEarningsCalendar { order: 13 !important; }
            #rowEconomicCalendar { order: 14 !important; }
            #usLeadingRightStack { order: 15 !important; }
            #rowDart { order: 16 !important; }

            /* 주도업종 + 주도종목은 모바일에서만 하나의 세로 스택 */
            #usLeadingRightStack {
                display: flex !important;
                flex-direction: column !important;
                align-items: stretch !important;
                gap: 14px !important;
                position: static !important;
                grid-column: auto !important;
                grid-row: auto !important;
                width: 100% !important;
                max-width: 832px !important;
                min-width: 0 !important;
                height: auto !important;
                min-height: 0 !important;
                box-sizing: border-box !important;
                z-index: auto !important;
            }

            #usLeadingRightStack > .card {
                position: static !important;
                width: 100% !important;
                max-width: 832px !important;
                min-width: 0 !important;
                height: auto !important;
                min-height: 0 !important;
                box-sizing: border-box !important;
            }

            #rowUsLeadingSectors,
            #rowUsLeadingStocks {
                grid-column: auto !important;
                grid-row: auto !important;
                width: 100% !important;
                max-width: 832px !important;
                height: auto !important;
                min-height: 0 !important;
                box-sizing: border-box !important;
            }

            /* 증시/왜 올라갔을까 등 카드가 서로 높이를 강제로 맞추지 않도록 */
            #rowWorld,
            #rowGlobalNews,
            #rowChart,
            #rowNews,
            #rowTrack,
            #rowQuant,
            #rowConsensus,
            #rowVote {
                height: auto !important;
                min-height: 0 !important;
                align-self: stretch !important;
            }
        }

        /* gaemiGTP 시그널: 항상 열림 */

        /* 미국 주도종목 클릭 가능 표시: 기존 레이아웃 크기는 유지 */
        .us-leading-stock-item {
            cursor: pointer;
        }
        .us-leading-stock-item:focus-visible {
            outline: 2px solid var(--border-line);
            outline-offset: -2px;
        }


        /* ===== STEP 23: 좌우 사이드바 완전 독립 오버레이 ===== */
        /* 어느 사이드바를 열고 닫아도 가운데 콘텐츠의 위치/폭은 절대 변하지 않는다. */
        body { padding-right: 0; }
        #content-wrapper { width: 100%; margin-left: 0; margin-right: 0; }
        #right-market-rail {
            position: fixed; top: 0; right: 0; width: 336px; height: 100vh;
            display: flex; flex-direction: row; z-index: 120;
            background: var(--bg-body); border-left: 1px solid var(--border-line);
            box-sizing: border-box;
        }
        #right-market-rail.collapsed { width: 44px; }
        #right-market-rail.collapsed #right-market-panel { display: none; }
        #right-market-panel {
            width: 292px; min-width: 292px; height: 100%; overflow: hidden;
            display: flex; flex-direction: column; background: var(--bg-body);
        }
        .right-market-head { height: 58px; display:flex; align-items:center; justify-content:space-between; padding: 0 14px; border-bottom:1px solid var(--border-line); }
        .right-market-title { font-size:.95rem; font-weight:700; color:var(--text-title); }
        .right-market-time { font-size:.72rem; color:var(--text-muted); margin-left:5px; font-weight:500; }
        .right-market-tabs { display:flex; gap:3px; padding:10px 10px 6px; }
        .right-market-tab { flex:1; border:0; background:transparent; color:var(--text-muted); padding:8px 4px; border-radius:9px; font-weight:700; cursor:pointer; font-size:.78rem; }
        .right-market-tab.active { background:var(--bg-card); color:var(--text-title); box-shadow: inset 0 0 0 1px var(--border-line); }
        .right-market-sort { display:flex; justify-content:space-between; align-items:center; padding:6px 13px 8px; color:var(--text-sub); font-size:.75rem; border-bottom:1px solid var(--border-line); }
        .right-market-list { flex:1; overflow-y:auto; padding:5px 8px 18px; scrollbar-width:thin; }
        .right-market-row { display:grid; grid-template-columns:24px 34px minmax(0,1fr) auto; gap:7px; align-items:center; min-height:61px; padding:5px 5px; border-radius:9px; cursor:pointer; }
        .right-market-row:hover { background:var(--btn-hover); }
        .right-market-rank { color:var(--text-muted); font-size:.78rem; text-align:center; font-weight:700; }
        .right-market-logo { width:31px; height:31px; border-radius:9px; display:flex; align-items:center; justify-content:center; background:var(--bg-card); border:1px solid var(--border-line); color:var(--text-title); font-size:.68rem; font-weight:800; overflow:hidden; }
        .right-market-name { color:var(--text-title); font-size:.78rem; font-weight:700; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
        .right-market-symbol { color:var(--text-muted); font-size:.62rem; margin-top:2px; }
        .right-market-values { text-align:right; min-width:76px; }
        .right-market-price { color:var(--text-title); font-size:.78rem; font-weight:700; white-space:nowrap; }
        .right-market-change { font-size:.68rem; margin-top:3px; white-space:nowrap; }
        .right-market-change.up { color:#ef4444; }
        .right-market-change.down { color:#38a8ff; }
        .right-market-empty { padding:28px 15px; color:var(--text-muted); text-align:center; font-size:.78rem; line-height:1.6; }
        #right-market-icons { width:44px; min-width:44px; border-left:1px solid var(--border-line); display:flex; flex-direction:column; align-items:center; padding-top:8px; gap:7px; background:var(--bg-body); }
        /* 테마 버튼은 우측 시장 레일과 겹치지 않도록 우측 여백을 확보한다. */
        #themeToggleBtn { position: relative; z-index: 13000; }
        header { padding-right: 60px; }
        .right-market-icon-btn { width:36px; height:42px; border:0; border-radius:9px; background:transparent; color:var(--text-sub); cursor:pointer; font-size:1.05rem; font-family:Arial, sans-serif; font-weight:600; display:flex; align-items:center; justify-content:center; transition:.15s; }
        .right-market-icon-btn:hover, .right-market-icon-btn.active { background:var(--bg-card); color:var(--text-title); }
        .right-market-close { font-size:1.25rem; margin-bottom:4px; }
        @media (max-width: 1100px) {
            /* 모바일/좁은 화면에서도 사이드바는 콘텐츠를 밀지 않고 위에 겹친다. */
            #content-wrapper { width:100%; margin-left:0; margin-right:0; }
            #right-market-rail {
                width: min(320px, 100vw);
                max-width: 100vw;
                right: 0;
            }
            #right-market-rail.collapsed { width: 44px; }
            #right-market-panel {
                width: calc(100% - 44px);
                min-width: 0;
                max-width: calc(100vw - 44px);
            }
            #right-market-icons { flex: 0 0 44px; }
        }

        /* STEP 26: 모바일은 오른쪽 패널을 처음에는 닫아두되
           열기/닫기 버튼만 남긴다. 패널이 열릴 때도 가운데 콘텐츠를 밀지 않고
           화면 위에 오버레이되도록 한다. */
        @media (max-width: 560px) {
            body { overflow-x: hidden; }
            /* 고정 우측 레일과 겹치지 않도록 기존 다크/화이트 전환 버튼의 가시 영역을 확보한다. */
            header { padding-right: 60px !important; }
            #content-wrapper {
                width: 100% !important;
                margin-left: 0 !important;
                margin-right: 0 !important;
            }
            #right-market-rail {
                width: 44px;
                max-width: 44px;
                right: 0;
                background: transparent;
                border-left: 0;
                z-index: 120;
            }
            #right-market-rail.collapsed {
                width: 44px;
                max-width: 44px;
            }
            #right-market-rail:not(.collapsed) {
                width: min(320px, 92vw);
                max-width: 92vw;
                background: var(--bg-body);
                border-left: 1px solid var(--border-line);
            }
            #right-market-panel {
                display: none;
                width: calc(100% - 44px);
                min-width: 0;
                max-width: none;
            }
            #right-market-rail:not(.collapsed) #right-market-panel {
                display: flex;
            }
            #right-market-icons {
                width: 44px;
                min-width: 44px;
                border-left: 1px solid var(--border-line);
                background: var(--bg-body);
            }
            #right-market-rail.collapsed #right-market-icons {
                border-left: 1px solid var(--border-line);
            }
            #right-market-rail.collapsed .right-market-icon-btn:not(.right-market-close) {
                display: none;
            }
            .right-market-close {
                display: flex !important;
                width: 36px;
                height: 42px;
            }
        }


        /* STEP 28: 오른쪽 열기/닫기 버튼을 사이드바 최상단에 고정한다.
           다크/화이트 테마 버튼과 겹치지 않도록 헤더의 테마 버튼은 항상 위에 보존한다.
           가운데 콘텐츠의 위치/폭에는 영향을 주지 않는다. */
        .right-market-close {
            order: -1;
        }
        #themeToggleBtn {
            position: relative;
            z-index: 13000;
        }
        @media (max-width: 560px) {
            /* STEP 29: 모바일 닫힘 상태에서는 오른쪽 레일 자체가 폭을 차지하지 않는다.
               열기 버튼만 화면 최상단 오른쪽에 떠서 표시되며, 세로 구분선도 숨긴다.
               따라서 검색창/차트/가운데 콘텐츠가 오른쪽에서 잘리지 않는다. */
            #right-market-rail {
                display: flex !important;
                position: fixed !important;
                top: 0 !important;
                right: 0 !important;
                width: 0 !important;
                max-width: none !important;
                height: 100vh !important;
                z-index: 9999 !important;
                pointer-events: none;
                background: transparent !important;
                border-left: 0 !important;
            }
            #right-market-rail.collapsed {
                width: 0 !important;
                max-width: none !important;
                border-left: 0 !important;
            }
            #right-market-rail #right-market-icons {
                pointer-events: none;
                position: fixed !important;
                top: 0 !important;
                right: 0 !important;
                width: 44px !important;
                min-width: 44px !important;
                height: auto !important;
                flex: none !important;
                display: flex !important;
                visibility: visible !important;
                opacity: 1 !important;
                padding-top: 8px !important;
                background: transparent !important;
                border-left: 0 !important;
            }
            #right-market-rail.collapsed #right-market-panel {
                display: none !important;
            }
            #right-market-rail.collapsed .right-market-icon-btn:not(.right-market-close) {
                display: none !important;
            }
            #right-market-rail .right-market-close {
                display: flex !important;
                pointer-events: auto !important;
                visibility: visible !important;
                opacity: 1 !important;
                width: 36px !important;
                height: 42px !important;
                margin: 0 !important;
                color: var(--text-title) !important;
                background: var(--bg-card) !important;
                border: 1px solid var(--border-line) !important;
                border-radius: 9px !important;
                font-size: 1.15rem !important;
                line-height: 1 !important;
                order: -1 !important;
            }
            #right-market-rail:not(.collapsed) {
                width: min(320px, 92vw) !important;
                max-width: 92vw !important;
                pointer-events: auto;
                background: var(--bg-body) !important;
                border-left: 1px solid var(--border-line) !important;
            }
            #right-market-rail:not(.collapsed) #right-market-panel {
                display: flex !important;
            }
            #right-market-rail:not(.collapsed) #right-market-icons {
                position: absolute !important;
                top: 0 !important;
                right: 0 !important;
                width: 44px !important;
                min-width: 44px !important;
                height: 100% !important;
                flex: 0 0 44px !important;
                background: var(--bg-body) !important;
                border-left: 1px solid var(--border-line) !important;
                pointer-events: auto !important;
            }
        }
</style>
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
                    <input type="text" id="centerInput" placeholder="종목명 입력 (예: SK하이닉스, 삼성전자, NVDA, BTC)" onkeypress="if(event.keyCode==13) selectStock(this.value);" autocomplete="off">
                    
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

                    <button class="send-btn" onclick="selectStock(document.getElementById('centerInput').value)">
                        <svg width="18" height="18" viewBox="0 0 24 24"><path d="M12 4L12 20M12 4L5 11M12 4L19 11" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" fill="none"/></svg>
                    </button>
                </div>
            </div>
            <div class="popular-tags">
                <button class="tag-btn" onclick="selectStock({key:'KR:000660',market:'KR',symbol:'000660',name:'SK하이닉스'})">SK하이닉스</button>
                <button class="tag-btn" onclick="selectStock({key:'KR:005930',market:'KR',symbol:'005930',name:'삼성전자'})">삼성전자</button>
                <button class="tag-btn" onclick="selectStock({key:'KR:196170',market:'KR',symbol:'196170',name:'알테오젠'})">알테오젠</button>
                <button class="tag-btn" onclick="selectStock({key:'US:NVDA',market:'US',symbol:'NVDA',name:'엔비디아'})">엔비디아</button>
                <button class="tag-btn" onclick="selectStock({key:'CRYPTO:BTCUSDT',market:'CRYPTO',symbol:'BTCUSDT',name:'비트코인'})">비트코인</button>
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
                            <div class="price-summary-head">
                                <div class="price-title" id="rTitle"></div>
                                <div class="price-live-line">
                                    <span class="price-val" id="rPrice"></span>
                                    <span class="price-change" id="rChange"></span>
                                </div>
                            </div>
                            <div class="price-meta" id="rMeta"></div>
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

                        <!-- 트랙 분석 -->
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
                        <input type="text" id="bottomInput" placeholder="다른 종목명 입력 (예: 테슬라, 카카오, BTC)" onkeypress="if(event.keyCode==13) selectStock(this.value);" autocomplete="off">
                        
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

                        <button class="send-btn" onclick="selectStock(document.getElementById('bottomInput').value)">
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

        const SUPABASE_URL = "https://uaqvmsubzgzvxbdcwcad.supabase.co";
        const SUPABASE_KEY = "sb_publishable_bcxDofsnPXgBICDDgQ8Ijw_fNjPcsRW";
        let supabaseClient = null;
        try {
            if (window.supabase) {
                supabaseClient = window.supabase.createClient(SUPABASE_URL, SUPABASE_KEY);
            }
        } catch(e) {}

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
            if (chart && typeof window.updateGaemiChartTheme === 'function') {
                window.updateGaemiChartTheme();
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
            if (chart && container && container.clientWidth > 0 && typeof chart.resize === 'function') {
                chart.resize();
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
            // 홈으로 돌아오면 현재 종목 상태를 먼저 무효화한다.
            pendingStockSelection = null;
            activeSearchToken++;
            if (activeSearchController) { try { activeSearchController.abort(); } catch (e) {} }
            if (activeDetailsController) { try { activeDetailsController.abort(); } catch (e) {} }
            activeSearchController = null;
            activeDetailsController = null;
            currentInstrument = null;
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
            homeUrl.searchParams.delete('m');
            homeUrl.searchParams.delete('s');
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

        // 브라우저 콘솔에서 실제 배포본의 단일 종목 흐름 버전을 확인할 수 있다.
        window.__GAEMIGTP_BUILD__ = 'STEP59-CANONICAL-INPUT-EARNINGS-INDEPENDENT';

        let currentInstrument = null;
        let activeSearchToken = 0;
        let activeSearchController = null;
        let activeDetailsController = null;
        let rightMarketController = null;
        let rightMarketRequestToken = 0;

        // STEP 54: 오른쪽 사이드바 연속 선택 안정화.
        // 이전 fetch를 브라우저에서 abort해도 이미 Render에서 시작된 Python/외부 API 작업은
        // 즉시 취소되지 않으므로, 빠른 종목 전환은 서버 작업을 동시에 쌓게 만든다.
        // 최신 선택만 남기고 실제 분석은 한 번에 하나만 실행한다.
        let stockSelectionRunning = false;
        let pendingStockSelection = null;

        // ============================================================
        // STEP 55: 종목 화면 렌더링 분리
        // - 종목 선택은 하나의 상태로 유지한다.
        // - 핵심 데이터(헤더/현재가/차트) 먼저 반영한다.
        // - 분석 위젯은 다음 프레임들에서 독립적으로 반영한다.
        // - 글로벌 위젯(세계시장/경제일정)은 종목 전환 때 재렌더링하지 않는다.
        // - details 응답은 전체 화면을 다시 그리지 않고 해당 카드만 갱신한다.
        // ============================================================
        function isCurrentStockRender(searchToken) {
            return searchToken === activeSearchToken && !pendingStockSelection;
        }

        function renderStockCore(data, searchToken, displayNameOverride = '') {
            if (!isCurrentStockRender(searchToken) || !data) return;

            const isKrw = data.market === '국내주식' || data.market === '국내증시';
            const displayName = String(displayNameOverride || data.name || data.formal_name || data.symbol || '').trim();
            const displayPrice = data.price || data.price_str;
            const displayTime = data.time || data.now_str;
            const usShortName = (isKrw || !data.symbol)
                ? displayName
                : getUsShortKrName(data.symbol, displayName);
            const rawChangePct = data.current_change_pct ?? data.change_pct ?? data.changePercent ?? data.rate ?? null;
            let changeText = '';
            if (rawChangePct !== null && rawChangePct !== undefined && rawChangePct !== '') {
                const n = Number(String(rawChangePct).replace(/[%+,]/g, ''));
                if (Number.isFinite(n)) changeText = `${n > 0 ? '+' : ''}${n.toFixed(2)}%`;
            }

            // 종목명/가격/차트를 한 브라우저 프레임 안에서 함께 교체한다.
            // 로딩 중에는 이전 종목을 지우지 않아 "이름 먼저 변경 → 차트 나중 변경" 현상을 줄인다.
            requestAnimationFrame(() => {
                if (!isCurrentStockRender(searchToken)) return;
                setHtml('rTitle', `${usShortName} <span>(${data.symbol})</span>`);
                setTxt('rPrice', displayPrice);
                setTxt('rChange', changeText);
                setHtml('rMeta', `시장: ${data.market} · 출처: ${isKrw ? 'Naver/KRX' : 'Yahoo/Binance'} · 시각: ${displayTime}`);

                if (Array.isArray(data.candles) && data.candles.length) {
                    try { initTvChart(data.candles); } catch (e) { console.warn('[CHART] render failed:', e); }
                }

                const loading = document.getElementById('loading');
                const result = document.getElementById('result-card-wrap');
                if (loading) loading.style.display = 'none';
                if (result) result.style.display = 'block';
            });
        }

        // STEP 57: 위젯 완전 독립 실행
        // - 고정 delay로 위젯을 순차 호출하지 않는다.
        // - core가 종목을 확정하면 각 위젯은 즉시 자기 API를 호출한다.
        // - 각 위젯이 자기 controller/token으로 이전 요청을 폐기한다.
        // - 한 위젯의 응답 속도/실패는 다른 위젯의 실행을 막지 않는다.
        function startIndependentWidgets(data, searchToken, displayName = '') {
            if (!isCurrentStockRender(searchToken) || !data) return;
            const market = data.market_code || '';
            const symbol = data.symbol || '';
            const name = data.name || data.formal_name || displayName || '';

            // 모든 독립 위젯은 같은 순간에 '출발'만 시키고, 결과는 각자 자기 카드에 반영한다.
            try { if (window.GaemiWidgets?.quant?.load) window.GaemiWidgets.quant.load({ market, symbol, name }); }
            catch (e) { console.warn('[QUANT] dispatch failed:', e); }
            try { if (window.GaemiWidgets?.vote?.load) window.GaemiWidgets.vote.load(symbol); }
            catch (e) { console.warn('[VOTE] dispatch failed:', e); }
            try { if (window.GaemiWidgets?.comment?.load) window.GaemiWidgets.comment.load(symbol); }
            catch (e) { console.warn('[COMMENT] dispatch failed:', e); }
            try { if (window.GaemiWidgets?.track?.load) window.GaemiWidgets.track.load({ market, symbol, name }); }
            catch (e) { console.warn('[TRACK] dispatch failed:', e); }
            try { if (window.GaemiWidgets?.stockNews?.load) window.GaemiWidgets.stockNews.load({ symbol, market: data.market, name, initial: [] }); }
            catch (e) { console.warn('[STOCK NEWS] dispatch failed:', e); }
            try { if (window.GaemiWidgets?.globalNews?.load) window.GaemiWidgets.globalNews.load({ market: data.market, marketCode: market, initial: [] }); }
            catch (e) { console.warn('[GLOBAL NEWS] dispatch failed:', e); }
            try { if (window.GaemiWidgets?.consensus?.load) window.GaemiWidgets.consensus.load({ market, symbol, name, price: data.current_price }); }
            catch (e) { console.warn('[CONSENSUS] dispatch failed:', e); }
            try { if (window.GaemiWidgets?.dart?.load) window.GaemiWidgets.dart.load({ market, symbol, name }); }
            catch (e) { console.warn('[DART] dispatch failed:', e); }
        }

        function renderStockData(data, searchToken, displayNameOverride = '') {
            if (!isCurrentStockRender(searchToken) || !data) return;
            renderStockCore(data, searchToken, displayNameOverride);
            startIndependentWidgets(data, searchToken, displayNameOverride);
        }

        async function _runSelectedStock(query, forceServer = false, displayNameOverride = '', marketHint = '', symbolHint = '') {
            const rawQuery = String(query || '').trim();
            if (!rawQuery) return;

            if (activeSearchController) { try { activeSearchController.abort(); } catch (e) {} }
            if (activeDetailsController) { try { activeDetailsController.abort(); } catch (e) {} }
            activeSearchController = new AbortController();
            const searchSignal = activeSearchController.signal;
            const searchToken = ++activeSearchToken;
            const displayNameHint = String(displayNameOverride || '').trim();

            const center = document.getElementById('centerInput');
            const bottom = document.getElementById('bottomInput');
            // 종목 전환 중에는 이전 종목명을 유지한다. 사이드바에서 이름을 함께 받은 경우
            // 그 이름을 즉시 유지하고, canonical key를 검색창에 노출하지 않는다.
            if (displayNameHint) {
                if (center) center.value = displayNameHint;
                if (bottom) bottom.value = displayNameHint;
            } else if (!currentInstrument) {
                if (center) center.value = rawQuery;
                if (bottom) bottom.value = rawQuery;
            }

            document.getElementById('main-view').style.display = 'none';
            document.getElementById('result-scroll-view').style.display = 'block';
            document.getElementById('bottom-bar').style.display = 'flex';
            // 첫 종목 진입에만 전체 로딩을 보여주고, 종목 전환 때는 기존 화면을 유지한다.
            const pageLoading = document.getElementById('loading');
            if (pageLoading) pageLoading.style.display = currentInstrument ? 'none' : 'flex';
            document.getElementById('result-card-wrap').style.display = 'block';
            const title = document.getElementById('rTitle');
            const price = document.getElementById('rPrice');
            const meta = document.getElementById('rMeta');
            // 첫 종목 진입 때만 초기 문구를 만들고, 종목 전환 중에는 기존 헤더/가격을 유지한다.
            if (!currentInstrument) {
                if (title) title.textContent = displayNameHint || rawQuery.toUpperCase();
                if (price) price.textContent = '';
                if (meta) meta.textContent = '실제 데이터를 불러오는 중입니다.';
            }

            if (window.innerWidth <= 900) {
                const side = document.getElementById('sidebar-wrapper');
                if (side) side.classList.remove('open');
            }

            // 서버가 종목의 canonical key를 확정한다. 프론트는 임의의 종목키를 만들지 않는다.
            try {
                let url = '/api/analyze?q=' + encodeURIComponent(rawQuery) + '&mode=core';
                if (marketHint && symbolHint) {
                    url += '&m=' + encodeURIComponent(marketHint) + '&s=' + encodeURIComponent(symbolHint);
                }
                const timeoutId = setTimeout(() => {
                    try { activeSearchController?.abort(); } catch (e) {}
                }, 15000);
                let response;
                try {
                    response = await fetch(url, { signal: searchSignal, cache: 'no-store' });
                } finally {
                    clearTimeout(timeoutId);
                }
                const contentType = response.headers.get('content-type') || '';
                let data = contentType.includes('application/json') ? await response.json() : null;
                if (!data) throw new Error('서버 응답을 읽지 못했습니다.');
                if (!response.ok || data.error) {
                    const detail = data.detail ? ` · ${data.detail}` : '';
                    throw new Error(`${data.error || '종목 데이터를 가져오지 못했습니다.'}${detail}`);
                }
                if (searchToken !== activeSearchToken) return;

                currentInstrument = {
                    key: data.instrument_key || '',
                    market: data.market_code || '',
                    symbol: data.symbol || '',
                    name: data.name || data.formal_name || displayNameHint || rawQuery
                };
                const resolvedDisplay = displayNameHint || data.name || data.formal_name || data.symbol || rawQuery;
                if (center) center.value = resolvedDisplay;
                if (bottom) bottom.value = resolvedDisplay;

                // URL은 입력 문자열이 아니라 canonical market/symbol로 저장한다.
                const stateUrl = new URL(window.location.href);
                stateUrl.searchParams.delete('q');
                if (data.market_code) stateUrl.searchParams.set('m', data.market_code);
                if (data.symbol) stateUrl.searchParams.set('s', data.symbol);
                window.history.replaceState({}, '', stateUrl.toString());

                // 최근검색도 canonical symbol을 기준으로 중복 제거한다.
                try {
                    const recent = JSON.parse(localStorage.getItem('gaemiGTP_recent_searches') || '[]');
                    const recentItem = {
                        name: displayNameHint || data.name || data.formal_name || data.symbol,
                        symbol: data.symbol || rawQuery.toUpperCase(),
                        market: data.market_code || '',
                        key: data.instrument_key || '',
                        price: data.current_price ?? null,
                        change_pct: data.current_change_pct ?? null,
                        currency: data.market_code === 'KR' ? 'KRW' : ''
                    };
                    const next = [recentItem, ...recent.filter(x => String(x?.key || '') !== String(recentItem.key))].slice(0, 15);
                    localStorage.setItem('gaemiGTP_recent_searches', JSON.stringify(next));
                } catch (e) {}

                renderStockData(data, searchToken, displayNameHint);

            } catch (err) {
                if (err && err.name === 'AbortError') return;
                console.error('Analysis Error:', err);
                if (searchToken === activeSearchToken) {
                    alert('데이터를 처리하는 중 오류가 발생했습니다.\n' + (err?.message || '알 수 없는 오류'));
                    document.getElementById('loading').style.display = 'none';
                }
            }
        }

        // STEP 54: 실제 브라우저 종목 선택 진입점.
        // 현재 분석이 끝나기 전에 새 종목을 누르면 최신 선택만 대기열에 남긴다.
        async function selectStock(query, forceServer = false, displayNameOverride = '') {
            const isObject = query && typeof query === 'object';
            const rawQuery = isObject
                ? String(query.key || query.symbol || '').trim()
                : String(query || '').trim();
            if (!rawQuery) return;

            pendingStockSelection = {
                query: rawQuery,
                marketHint: isObject ? String(query.market || query.market_code || '').trim().toUpperCase() : '',
                symbolHint: isObject ? String(query.symbol || '').trim().toUpperCase() : '',
                forceServer: !!forceServer,
                displayNameOverride: String(isObject ? (query.name || query.displayName || displayNameOverride) : displayNameOverride || '').trim()
            };

            if (activeDetailsController) { try { activeDetailsController.abort(); } catch (e) {} }

            const center = document.getElementById('centerInput');
            const bottom = document.getElementById('bottomInput');
            // 사이드바처럼 이미 종목명을 알고 있는 선택은 이름을 유지한다.
            // KR:000660 같은 canonical key가 검색창에 잠깐 나타났다 다시 이름으로 바뀌는
            // 불필요한 화면 전환을 없앤다. URL/데이터 식별자는 여전히 market+symbol이다.
            const selectionDisplay = pendingStockSelection.displayNameOverride;
            if (selectionDisplay) {
                if (center) center.value = selectionDisplay;
                if (bottom) bottom.value = selectionDisplay;
            } else if (!currentInstrument) {
                if (center) center.value = rawQuery;
                if (bottom) bottom.value = rawQuery;
            }

            if (stockSelectionRunning) return;

            stockSelectionRunning = true;
            try {
                while (pendingStockSelection) {
                    const job = pendingStockSelection;
                    pendingStockSelection = null;
                    try {
                        await _runSelectedStock(job.query, job.forceServer, job.displayNameOverride, job.marketHint, job.symbolHint);
                    } catch (e) {
                        console.error('[STOCK SELECT QUEUE] run failed:', e);
                    }
                }
            } finally {
                stockSelectionRunning = false;
            }
        }

        // 기존 호출부 호환용. 실제 단일 진입점은 selectStock이다.
        async function doSearch(query, forceServer = false, displayNameOverride = '') {
            return selectStock(query, forceServer, displayNameOverride);
        }

        window.selectStock = selectStock;
        window.openStock = selectStock;
        window.doSearch = doSearch;

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
            const cleanSymbol = String(symbol || '').trim().toUpperCase();
            const cleanName = getUsShortKrName(cleanSymbol, name);
            if (!cleanSymbol) return;
            selectStock(cleanSymbol, false, cleanName);
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
            if (window.GaemiWidgets && window.GaemiWidgets.earningsCalendar) {
                window.GaemiWidgets.earningsCalendar.load(false);
            }
            loadUsLeadingSectors();
        });
        window.addEventListener('DOMContentLoaded', () => {
            initTheme();

            // 새로고침 시 URL에 저장된 종목을 자동 복원
            const params = new URLSearchParams(window.location.search);
            const savedMarket = params.get('m');
            const savedSymbol = params.get('s');
            const savedQuery = params.get('q');

            if (savedMarket && savedSymbol) {
                selectStock(`${savedMarket}:${savedSymbol}`);
            } else if (savedQuery && savedQuery.trim()) {
                selectStock(savedQuery.trim());
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
    instrument = resolve_instrument(query)
    if not instrument:
        return None
    cache_key = instrument["key"]

    cached_payload = FAST_CHART_PAYLOAD_CACHE.get(cache_key)
    if cached_payload and (time.time() - cached_payload["time"]) < FAST_CHART_PAYLOAD_TTL:
        return dict(cached_payload["data"])

    def _finish(df):
        payload = _build_chart_payload(df) if df is not None else None
        if payload:
            FAST_CHART_PAYLOAD_CACHE[cache_key] = {
                "time": time.time(),
                "data": dict(payload),
            }
        return payload

    try:
        if instrument["market"] == "INDEX":
            df = _fetch_fast_overseas_chart(instrument["symbol"])
            return _finish(df)
        if instrument["market"] == "CRYPTO":
            _, _, df = fetch_crypto_candles(instrument["symbol"])
            return _finish(df)
        if instrument["market"] == "US":
            df = _fetch_fast_overseas_chart(instrument["symbol"])
            return _finish(df)
        df = _fetch_fast_domestic_chart(instrument["symbol"])
        return _finish(df)
    except Exception as exc:
        print(f"[FAST CHART] {cache_key} failed: {type(exc).__name__}: {exc}")
        return None


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

        # PostgreSQL 연결 상태 확인
        if parsed.path == "/api/db/health":
            self._json(200, get_db_health())
            return

        # PostgreSQL 저장 데이터 확인
        # /api/db/prices?market=Korea&symbol=000660&limit=10
        if parsed.path == "/api/db/prices":
            market = str(params.get("market", [""])[0]).strip()
            symbol = str(params.get("symbol", [""])[0]).strip()

            try:
                limit = int(params.get("limit", ["10"])[0])
            except Exception:
                limit = 10

            if not market or not symbol:
                self._json(
                    400,
                    {"ok": False, "error": "market와 symbol이 필요합니다."}
                )
                return

            summary = get_db_price_summary(
                market=market,
                symbol=symbol,
                timeframe="1d",
            )
            recent = get_db_recent_prices(
                market=market,
                symbol=symbol,
                timeframe="1d",
                limit=limit,
            )

            self._json(
                200,
                {
                    "ok": summary.get("ok", False),
                    "summary": summary,
                    "data": recent,
                }
            )
            return



        # 독립 위젯 API: 각 위젯은 서로의 결과를 기다리지 않는다.
        if parsed.path == "/api/widget/quant":
            try:
                context = _get_widget_core_from_params(params)
                if not context:
                    self._json(404, {"ok": False, "error": "종목 핵심 데이터를 찾을 수 없습니다."})
                    return
                q = calculate_quant_factors(context['current_price'], context['_df'], symbol=context['symbol'], is_krw=(context['market_code'] == 'KR'))
                self._json(200, {"ok": True, "data": q})
            except Exception as exc:
                print(f"API ERROR /api/widget/quant: {type(exc).__name__}: {exc}")
                self._json(500, {"ok": False, "error": "퀀트 데이터를 계산하지 못했습니다."})
            return

        if parsed.path == "/api/widget/track":
            try:
                ins = _widget_instrument_from_params(params)
                context = _load_track_context(ins)
                if not context:
                    self._json(200, {"ok": True, "data": {
                        "price_str": "-", "current_change_pct": None, "candles": [],
                        "evidence": [], "evidence_count": 0, "status": "원천 데이터 확인 필요"
                    }})
                    return
                self._json(200, {"ok": True, "data": build_track_widget_data(context)})
            except Exception as exc:
                print(f"API ERROR /api/widget/track: {type(exc).__name__}: {exc}")
                self._json(200, {"ok": True, "data": {
                    "price_str": "-", "current_change_pct": None, "candles": [],
                    "evidence": [], "evidence_count": 0, "status": "원천 데이터 확인 필요"
                }})
            return

        if parsed.path == "/api/widget/consensus":
            try:
                context = _get_widget_core_from_params(params)
                if not context:
                    self._json(404, {"ok": False, "error": "종목 핵심 데이터를 찾을 수 없습니다."})
                    return
                result = fetch_market_consensus_unified(context['symbol'], context['market'], context['current_price'], context['name'])
                self._json(200, {"ok": True, "data": result or {}})
            except Exception as exc:
                print(f"API ERROR /api/widget/consensus: {type(exc).__name__}: {exc}")
                self._json(500, {"ok": False, "error": "컨센서스 데이터를 가져오지 못했습니다."})
            return

        if parsed.path == "/api/widget/dart":
            try:
                context = _get_widget_core_from_params(params)
                if not context:
                    self._json(404, {"ok": False, "error": "종목 핵심 데이터를 찾을 수 없습니다."})
                    return
                rows = fetch_dart_disclosures(context['symbol'], context['market_code'] == 'KR', 5)
                self._json(200, {"ok": True, "data": rows or []})
            except Exception as exc:
                print(f"API ERROR /api/widget/dart: {type(exc).__name__}: {exc}")
                self._json(500, {"ok": False, "error": "공시 데이터를 가져오지 못했습니다."})
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
                if market_name == "미국주식":
                    rows = translate_news_titles_to_korean(rows, market_name)
                self._json(200, {"ok": True, "data": rows})
            except Exception as exc:
                print(f"API ERROR /api/stock-news: {type(exc).__name__}: {exc}")
                self._json(500, {"ok": False, "error": "종목 뉴스를 가져오지 못했습니다."})
            return

        if parsed.path == "/api/global-news":
            try:
                market_name = str(params.get("market", ["국내주식"])[0]).strip() or "국내주식"
                market_name = {
                    "KR": "국내주식",
                    "US": "미국주식",
                    "CRYPTO": "암호화폐",
                }.get(market_name.upper(), market_name)
                limit = int(params.get("limit", ["10"])[0])
                rows = fetch_google_economic_news(market_name, max(1, min(limit, 20)))
                if market_name == "미국주식":
                    rows = translate_news_titles_to_korean(rows, market_name)
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

        if parsed.path == "/api/earnings-calendar":
            try:
                rows = fetch_all_real_earnings() or []
                self._json(200, {"ok": True, "data": rows})
            except Exception as exc:
                print(f"API ERROR /api/earnings-calendar: {type(exc).__name__}: {exc}")
                self._json(500, {"ok": False, "error": "실적 캘린더 데이터를 가져오지 못했습니다."})
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

        if parsed.path == "/api/analyze":
            q = str(params.get("q", [""])[0]).strip()
            market_param = str(params.get("m", [""])[0]).strip().upper()
            symbol_param = str(params.get("s", [""])[0]).strip().upper()
            name_param = str(params.get("name", [""])[0]).strip()
            if not q and not symbol_param:
                self._json(400, {"error": "종목명 또는 종목코드가 필요합니다."})
                return
            _perf_begin("analyze", q or f"{market_param}:{symbol_param}")
            try:
                data = run_independent_stock_core(
                    q or symbol_param,
                    lightweight=True,
                    market_hint=market_param,
                    symbol_hint=symbol_param,
                    name_hint=name_param,
                )
                perf = _perf_snapshot()
                if not data:
                    self._json(404, {"error": "실제 데이터를 가져오지 못했습니다. 종목명 또는 데이터 제공처를 확인해주세요.", "_perf": perf})
                    return
                data["_perf"] = perf
                self._json(200, data)
            except Exception as exc:
                perf = _perf_snapshot()
                print(f"[PERF][analyze] {q or symbol_param}: {perf}")
                self._json(500, {"error": "서버 처리 중 오류가 발생했습니다.", "detail": f"{type(exc).__name__}: {str(exc)[:400]}", "_perf": perf})
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
    # DB가 실패해도 기존 서버를 막지 않는다.
    try:
        init_database()
    except Exception as exc:
        print(f"[DB] 서버 시작 시 초기화 실패 - 기존 서버 계속 실행: {type(exc).__name__}: {exc}")

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
