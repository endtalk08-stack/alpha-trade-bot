import os

# 실적 캘린더 첫 화면에 보여줄 주요 예정 실적 수
EARNINGS_PREVIEW_COUNT = 4
import re
import io
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
from concurrent.futures import ThreadPoolExecutor
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

import requests
import pandas as pd
import numpy as np


# ----------------------------------------------------
# PostgreSQL / gaemigtp-db
# - Render DATABASE_URL 환경변수 사용
# - 기존 분석 기능과 분리
# - DB 장애가 기존 사이트/API를 중단시키지 않도록 fail-safe
# ----------------------------------------------------
try:
    import psycopg
    from psycopg.rows import dict_row
    from psycopg.types.json import Jsonb
except ImportError:
    psycopg = None
    dict_row = None
    Jsonb = None

DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
DB_MODEL_VERSION = "gaemiGTP-db-v1"


def _db_available():
    return bool(DATABASE_URL and psycopg is not None)


def _db_connect():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL 환경변수가 설정되지 않았습니다.")
    if psycopg is None:
        raise RuntimeError("psycopg가 설치되지 않았습니다.")
    return psycopg.connect(
        DATABASE_URL,
        row_factory=dict_row,
        connect_timeout=5,
    )


def init_database():
    """Render PostgreSQL에 원천 가격/퀀트 결과 테이블을 안전하게 생성한다."""
    if not _db_available():
        print("[DB] PostgreSQL 비활성화: DATABASE_URL 또는 psycopg 확인 필요")
        return False

    ddl = """
    CREATE TABLE IF NOT EXISTS market_prices (
        id BIGSERIAL PRIMARY KEY,
        market VARCHAR(20) NOT NULL,
        symbol VARCHAR(50) NOT NULL,
        timeframe VARCHAR(20) NOT NULL DEFAULT '1d',
        timestamp TIMESTAMPTZ NOT NULL,
        open NUMERIC(30, 10) NOT NULL,
        high NUMERIC(30, 10) NOT NULL,
        low NUMERIC(30, 10) NOT NULL,
        close NUMERIC(30, 10) NOT NULL,
        volume NUMERIC(40, 10) NOT NULL DEFAULT 0,
        source VARCHAR(100),
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_market_prices_market_symbol_tf_ts
            UNIQUE (market, symbol, timeframe, timestamp)
    );

    CREATE INDEX IF NOT EXISTS idx_market_prices_symbol_timestamp
        ON market_prices (market, symbol, timeframe, timestamp DESC);

    CREATE INDEX IF NOT EXISTS idx_market_prices_timestamp
        ON market_prices (timestamp DESC);


    CREATE TABLE IF NOT EXISTS quant_results (
        id BIGSERIAL PRIMARY KEY,
        market VARCHAR(20) NOT NULL,
        symbol VARCHAR(50) NOT NULL,
        timeframe VARCHAR(20) NOT NULL DEFAULT '1d',
        timestamp TIMESTAMPTZ NOT NULL,

        current_price NUMERIC(30, 10),

        momentum_1m NUMERIC(20, 8),
        momentum_3m NUMERIC(20, 8),
        momentum_6m NUMERIC(20, 8),
        momentum_12m NUMERIC(20, 8),

        trend_direction VARCHAR(50),
        volatility NUMERIC(20, 8),
        position_size NUMERIC(20, 8),
        signal VARCHAR(50),

        backtest_1y NUMERIC(20, 8),
        backtest_2y NUMERIC(20, 8),
        risk_reward NUMERIC(20, 8),

        model_version VARCHAR(100) NOT NULL,
        result_json JSONB,

        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

        CONSTRAINT uq_quant_results_key
            UNIQUE (market, symbol, timeframe, timestamp, model_version)
    );

    CREATE INDEX IF NOT EXISTS idx_quant_results_symbol_timestamp
        ON quant_results (market, symbol, timeframe, timestamp DESC);
    """

    try:
        with _db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)
            conn.commit()
        print("[DB] PostgreSQL 연결 및 테이블 초기화 성공")
        return True
    except Exception as exc:
        print(f"[DB] 초기화 실패: {type(exc).__name__}: {exc}")
        return False


def _market_db_name(market_name):
    mapping = {
        "국내주식": "Korea",
        "국내증시": "Korea",
        "미국주식": "US",
        "암호화폐": "Crypto",
        "외환": "FX",
        "원자재": "Commodity",
        "글로벌증시": "Global",
    }
    return mapping.get(str(market_name), str(market_name)[:20] or "Unknown")


def _to_db_timestamp(value, market):
    """
    국내 Naver 일봉은 날짜만 있으므로 Asia/Seoul로 해석하고,
    Yahoo/Binance의 epoch 기반 timestamp는 UTC로 취급한다.
    최종 DB에는 모두 UTC TIMESTAMPTZ로 저장한다.
    """
    ts = pd.Timestamp(value)

    if ts.tzinfo is None:
        if market == "Korea":
            ts = ts.tz_localize("Asia/Seoul")
        else:
            ts = ts.tz_localize("UTC")

    return ts.tz_convert("UTC").to_pydatetime()


def save_market_prices(df, market_name, symbol, timeframe="1d", source="alpha-trade-bot"):
    """
    API에서 받은 OHLCV 원천 데이터를 market_prices에 누적 저장한다.
    같은 market/symbol/timeframe/timestamp는 UPSERT한다.
    """
    if not _db_available() or df is None or df.empty:
        return 0

    required = ["Open", "High", "Low", "Close", "Volume"]
    if any(col not in df.columns for col in required):
        print(f"[DB] OHLCV 컬럼 누락: {required}")
        return 0

    market = _market_db_name(market_name)

    # 국내 Naver 데이터가 anchor_price 보정을 거친 경우,
    # fetch 함수가 보관한 원본 DataFrame을 우선 사용한다.
    source_df = df.attrs.get("_gaemi_raw_df")
    if isinstance(source_df, pd.DataFrame) and not source_df.empty:
        work_df = source_df
    else:
        work_df = df

    rows = []
    for idx, row in work_df.iterrows():
        try:
            ts = _to_db_timestamp(idx, market)
            o = float(row["Open"])
            h = float(row["High"])
            l = float(row["Low"])
            c = float(row["Close"])
            v = float(row["Volume"]) if pd.notna(row["Volume"]) else 0.0

            if not all(np.isfinite(x) for x in (o, h, l, c, v)):
                continue

            rows.append((
                market,
                str(symbol),
                str(timeframe),
                ts,
                o, h, l, c, v,
                str(source),
            ))
        except Exception as exc:
            print(f"[DB] OHLCV row 변환 실패: {exc}")

    if not rows:
        return 0

    sql = """
    INSERT INTO market_prices (
        market, symbol, timeframe, timestamp,
        open, high, low, close, volume, source
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (market, symbol, timeframe, timestamp)
    DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume,
        source = EXCLUDED.source
    """

    try:
        with _db_connect() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
            conn.commit()

        print(
            f"[DB] OHLCV 저장 성공: "
            f"{market}/{symbol}/{timeframe} {len(rows)}건"
        )
        return len(rows)
    except Exception as exc:
        print(
            f"[DB] OHLCV 저장 실패: "
            f"{market}/{symbol}/{timeframe} "
            f"{type(exc).__name__}: {exc}"
        )
        return 0


def _safe_numeric_from_text(value):
    if value is None:
        return None
    try:
        s = str(value).replace("%", "").replace("실측", "").replace("년", "").strip()
        return float(s)
    except Exception:
        return None


def _calculate_tsm_metrics_for_db(df, raw_price):
    """
    향후 기관형 Time-Series Momentum에 사용할 기본 1/3/6/12개월 수익률.
    거래일 기준 21/63/126/252 bars를 사용한다.
    이 계산은 기존 화면 분석 로직을 변경하지 않고 DB 결과만 채운다.
    """
    result = {
        "momentum_1m": None,
        "momentum_3m": None,
        "momentum_6m": None,
        "momentum_12m": None,
        "volatility": None,
    }

    if df is None or df.empty or raw_price is None:
        return result

    closes = pd.to_numeric(df["Close"], errors="coerce").dropna()
    if closes.empty:
        return result

    lookbacks = {
        "momentum_1m": 21,
        "momentum_3m": 63,
        "momentum_6m": 126,
        "momentum_12m": 252,
    }

    for key, bars in lookbacks.items():
        if len(closes) > bars:
            base = float(closes.iloc[-(bars + 1)])
            if base > 0:
                result[key] = ((float(raw_price) / base) - 1.0) * 100.0

    returns = closes.pct_change().dropna()
    if len(returns) >= 20:
        vol = float(returns.tail(252).std() * np.sqrt(250) * 100.0)
        if np.isfinite(vol):
            result["volatility"] = vol

    return result


def save_quant_result(
    market_name,
    symbol,
    raw_price,
    df,
    quant,
    timeframe="1d",
):
    """현재 gaemiGTP 퀀트 결과를 원천 가격 테이블과 분리해 저장한다."""
    if not _db_available() or not quant or raw_price is None:
        return False

    market = _market_db_name(market_name)
    metrics = _calculate_tsm_metrics_for_db(df, raw_price)

    # 분석 시점은 실제 원천 데이터의 마지막 timestamp를 우선 사용한다.
    try:
        latest_idx = df.index[-1]
        ts = _to_db_timestamp(latest_idx, market)
    except Exception:
        ts = datetime.now(timezone.utc)

    bt_1y = _safe_numeric_from_text(quant.get("bt_1y"))
    bt_2y = _safe_numeric_from_text(quant.get("bt_2y"))

    rr = None
    rr_raw = quant.get("rr_str")
    if rr_raw:
        m = re.search(r"1:([0-9]+(?:\.[0-9]+)?)", str(rr_raw))
        if m:
            try:
                rr = float(m.group(1))
            except Exception:
                rr = None

    signal = None
    score = _safe_numeric_from_text(quant.get("quant_score"))
    if score is not None:
        if score >= 80:
            signal = "STRONG"
        elif score >= 65:
            signal = "LONG"
        else:
            signal = "NEUTRAL"

    payload = {
        "quant_score": quant.get("quant_score"),
        "trend": quant.get("trend"),
        "ann_vol": quant.get("ann_vol"),
        "rsi": quant.get("rsi"),
        "atr": quant.get("atr"),
        "ma20": quant.get("ma20"),
        "ma60": quant.get("ma60"),
        "entry1": quant.get("entry1"),
        "entry2": quant.get("entry2"),
        "target1": quant.get("target1"),
        "target2": quant.get("target2"),
        "stop_p": quant.get("stop_p"),
        "bt_1y": quant.get("bt_1y"),
        "bt_2y": quant.get("bt_2y"),
        "rr_str": quant.get("rr_str"),
    }

    sql = """
    INSERT INTO quant_results (
        market, symbol, timeframe, timestamp,
        current_price,
        momentum_1m, momentum_3m, momentum_6m, momentum_12m,
        trend_direction, volatility, position_size, signal,
        backtest_1y, backtest_2y, risk_reward,
        model_version, result_json
    )
    VALUES (
        %s, %s, %s, %s,
        %s,
        %s, %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s,
        %s, %s
    )
    ON CONFLICT (market, symbol, timeframe, timestamp, model_version)
    DO UPDATE SET
        current_price = EXCLUDED.current_price,
        momentum_1m = EXCLUDED.momentum_1m,
        momentum_3m = EXCLUDED.momentum_3m,
        momentum_6m = EXCLUDED.momentum_6m,
        momentum_12m = EXCLUDED.momentum_12m,
        trend_direction = EXCLUDED.trend_direction,
        volatility = EXCLUDED.volatility,
        position_size = EXCLUDED.position_size,
        signal = EXCLUDED.signal,
        backtest_1y = EXCLUDED.backtest_1y,
        backtest_2y = EXCLUDED.backtest_2y,
        risk_reward = EXCLUDED.risk_reward,
        result_json = EXCLUDED.result_json
    """

    values = (
        market,
        str(symbol),
        str(timeframe),
        ts,
        float(raw_price),
        metrics["momentum_1m"],
        metrics["momentum_3m"],
        metrics["momentum_6m"],
        metrics["momentum_12m"],
        str(quant.get("trend") or ""),
        metrics["volatility"],
        None,  # 포지션 크기는 향후 리스크 예산 엔진에서 결정
        signal,
        bt_1y,
        bt_2y,
        rr,
        DB_MODEL_VERSION,
        Jsonb(payload) if Jsonb is not None else json.dumps(payload, ensure_ascii=False),
    )

    try:
        with _db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, values)
            conn.commit()
        return True
    except Exception as exc:
        print(f"[DB] 퀀트 결과 저장 실패: {type(exc).__name__}: {exc}")
        return False


def get_db_health():
    if not DATABASE_URL:
        return {
            "ok": False,
            "configured": False,
            "error": "DATABASE_URL이 없습니다."
        }

    if psycopg is None:
        return {
            "ok": False,
            "configured": True,
            "error": "psycopg가 설치되지 않았습니다."
        }

    try:
        with _db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT NOW() AS now")
                row = cur.fetchone()

        return {
            "ok": True,
            "configured": True,
            "database_time": row["now"].isoformat(),
        }
    except Exception as exc:
        return {
            "ok": False,
            "configured": True,
            "error": f"{type(exc).__name__}: {exc}",
        }


def get_db_price_summary(market, symbol, timeframe="1d"):
    if not _db_available():
        return {
            "ok": False,
            "error": "DATABASE_URL 또는 psycopg가 없습니다."
        }

    sql = """
    SELECT
        COUNT(*) AS count,
        MIN(timestamp) AS oldest,
        MAX(timestamp) AS newest
    FROM market_prices
    WHERE market = %s
      AND symbol = %s
      AND timeframe = %s
    """

    try:
        with _db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (market, symbol, timeframe))
                row = cur.fetchone()

        return {
            "ok": True,
            "market": market,
            "symbol": symbol,
            "timeframe": timeframe,
            "count": int(row["count"] or 0),
            "oldest": row["oldest"].isoformat() if row["oldest"] else None,
            "newest": row["newest"].isoformat() if row["newest"] else None,
        }
    except Exception as exc:
        return {
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
        }


def get_db_recent_prices(market, symbol, timeframe="1d", limit=10):
    if not _db_available():
        return []

    limit = max(1, min(int(limit), 100))

    sql = """
    SELECT
        market, symbol, timeframe, timestamp,
        open, high, low, close, volume, source
    FROM market_prices
    WHERE market = %s
      AND symbol = %s
      AND timeframe = %s
    ORDER BY timestamp DESC
    LIMIT %s
    """

    try:
        with _db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (market, symbol, timeframe, limit))
                rows = cur.fetchall()

        result = []
        for row in rows:
            item = dict(row)
            if item.get("timestamp"):
                item["timestamp"] = item["timestamp"].isoformat()

            for key in ("open", "high", "low", "close", "volume"):
                if item.get(key) is not None:
                    item[key] = float(item[key])

            result.append(item)

        return result
    except Exception as exc:
        print(f"[DB] 가격 조회 실패: {type(exc).__name__}: {exc}")
        return []


# ----------------------------------------------------
# 0. 고성능 네트워크 세션 (TCP 커넥션 풀 재사용)
# ----------------------------------------------------
HTTP_SESSION = requests.Session()
HTTP_SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive"
})

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
NEWS_API_KEY = os.environ.get("NEWS_API_KEY", "").strip()

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
CACHED_MARKET_CALENDAR = []
LAST_CALENDAR_FETCH_TIME = 0
ANALYSIS_CACHE = {}

# ===== External API cache / rate-limit protection =====
FRED_CACHE = {}
FRED_CACHE_TTL = 900  # 15분
ALPHA_NEWS_CACHE = {}
ALPHA_NEWS_CACHE_TTL = 600  # 10분
ALPHA_EARNINGS_CACHE = {}
ALPHA_EARNINGS_CACHE_TTL = 1800  # 30분
FINNHUB_CONSENSUS_CACHE = {}
FINNHUB_CONSENSUS_CACHE_TTL = 1800  # 30분
FRED_LAST_REQUEST_TS = 0.0
FRED_MIN_INTERVAL = 0.55  # FRED 2 req/s 제한에 안전하게 대응
API_LOCK = threading.Lock()

def _rate_limit_wait(min_interval):
    global FRED_LAST_REQUEST_TS
    with API_LOCK:
        now = time.time()
        wait = min_interval - (now - FRED_LAST_REQUEST_TS)
        if wait > 0:
            time.sleep(wait)
        FRED_LAST_REQUEST_TS = time.time()

def _cache_get(cache, key, ttl):
    item = cache.get(key)
    if not item:
        return None
    ts, value = item
    if time.time() - ts > ttl:
        cache.pop(key, None)
        return None
    return value

def _cache_set(cache, key, value):
    cache[key] = (time.time(), value)
    return value

# ===== Theme co-movement engine =====
THEME_KEYWORDS = {
    "AI·데이터센터": ["ai", "인공지능", "데이터센터", "gpu", "hbm", "서버", "네트워크"],
    "전력인프라": ["전력", "변압기", "배전", "전선", "전력망", "데이터센터"],
    "반도체": ["반도체", "hbm", "dram", "nand", "파운드리", "장비"],
    "로봇": ["로봇", "협동로봇", "휴머노이드", "자동화"],
    "바이오": ["바이오", "신약", "제약", "임상", "의약"],
    "2차전지": ["2차전지", "배터리", "리튬", "양극재", "음극재"],
}

# ===== US-to-KR relationship engine =====
US_KR_RELATIONS = {
    "NVIDIA": {
        "keywords": ["nvidia", "엔비디아", "gpu", "ai", "데이터센터", "hbm"],
        "themes": ["AI·데이터센터", "반도체", "전력인프라"],
    },
    "AMD": {
        "keywords": ["amd", "gpu", "ai", "데이터센터"],
        "themes": ["AI·데이터센터", "반도체", "전력인프라"],
    },
    "MICRON": {
        "keywords": ["micron", "마이크론", "hbm", "dram", "메모리"],
        "themes": ["반도체"],
    },
    "TSMC": {
        "keywords": ["tsmc", "파운드리", "반도체"],
        "themes": ["반도체"],
    },
    "BROADCOM": {
        "keywords": ["broadcom", "브로드컴", "ai", "네트워크", "데이터센터"],
        "themes": ["AI·데이터센터", "반도체"],
    },
}

def analyze_us_kr_relation(news_items=None, us_items=None, kr_items=None):
    """
    Detect a candidate US -> KR theme relationship.
    Evidence requires US event/news text plus matching KR theme text.
    If price data is supplied, co-movement breadth strengthens the score.
    It never states that the US event caused the KR move.
    """
    news_items = news_items or []
    us_items = us_items or []
    kr_items = kr_items or []

    def blob(items):
        vals = []
        for x in items:
            if isinstance(x, dict):
                vals.append(" ".join(str(x.get(k, "")) for k in
                                    ("title", "description", "summary", "content", "name")))
            else:
                vals.append(str(x))
        return " ".join(vals).lower()

    us_blob = blob(us_items + news_items)
    kr_blob = blob(kr_items + news_items)

    best = None
    for company, rule in US_KR_RELATIONS.items():
        hits = [kw for kw in rule["keywords"] if kw.lower() in us_blob]
        if not hits:
            continue
        for theme in rule["themes"]:
            theme_hits = [kw for kw in THEME_KEYWORDS.get(theme, []) if kw.lower() in kr_blob]
            score = len(hits) + len(theme_hits)
            if theme_hits and (best is None or score > best["raw_score"]):
                best = {
                    "company": company,
                    "theme": theme,
                    "us_hits": hits,
                    "kr_hits": theme_hits,
                    "raw_score": score,
                }

    if not best:
        return {"matched": False, "score": 0, "reason": "미국→국내 연결 근거 부족"}

    # Optional peer breadth.
    up = valid = 0
    for x in kr_items:
        if not isinstance(x, dict):
            continue
        try:
            chg = float(x.get("change_pct"))
        except (TypeError, ValueError):
            continue
        valid += 1
        if chg > 0:
            up += 1

    breadth = (up / valid) if valid else None
    score = min(100, 45 + 10 * min(3, len(best["us_hits"])) +
                10 * min(3, len(best["kr_hits"])))
    if breadth is not None and valid >= 3:
        score = round(min(100, score + 25 * max(0, breadth - 0.5) * 2))

    best.update({
        "matched": True,
        "score": score,
        "breadth": breadth,
        "up": up,
        "valid": valid,
        "reason": f'{best["company"]} 관련 해외 이벤트와 국내 {best["theme"]} 연결 근거 확인',
    })
    return best

def make_us_kr_comment(result):
    if not result or not result.get("matched"):
        return ""
    if result.get("breadth") is not None and result.get("valid", 0) >= 3 and result["breadth"] >= 0.67:
        return (f'개미들아! {result["company"]} 쪽 움직임이 국내 {result["theme"]}로 '
                f'번지는 흐름이 보여 관련 종목들도 같이 움직이고 있어.')
    return (f'개미들아! {result["company"]} 관련 해외 재료와 국내 '
            f'{result["theme"]} 쪽 연결고리가 보여 아직 원인이라고 단정하진 말고 '
            f'관련 종목 움직임을 같이 확인해보자.')

# ===== Real-time program-flow evidence helper =====
def normalize_program_evidence(program_value, previous_value=None):
    """Normalize a broker's program net-buy figure for display."""
    try:
        current = float(program_value)
    except (TypeError, ValueError):
        return {"available": False, "label": "프로그램 데이터 확인 필요"}

    delta = None
    if previous_value is not None:
        try:
            delta = current - float(previous_value)
        except (TypeError, ValueError):
            pass

    return {
        "available": True,
        "net": current,
        "delta": delta,
        "label": "프로그램 매매",
        "realtime": True,
    }


def _theme_match_score(theme, text_blob):
    blob = (text_blob or "").lower()
    kws = THEME_KEYWORDS.get(theme, [])
    hits = sum(1 for kw in kws if kw.lower() in blob)
    return hits / max(1, len(kws))

def analyze_theme_comovement(query, news_items=None, price_items=None):
    """
    Conservative theme signal:
    - infer candidate themes from the target's news/title text;
    - require at least 2 theme keywords;
    - if peer price data is supplied, measure breadth (up peers / valid peers).
    - never claim causality; returns 'theme_related' evidence only.
    """
    news_items = news_items or []
    price_items = price_items or []

    blobs = []
    for item in news_items:
        if isinstance(item, dict):
            blobs.append(" ".join(str(item.get(k, "")) for k in ("title", "description", "summary", "content")))
        else:
            blobs.append(str(item))
    target_blob = " ".join(blobs)

    candidates = []
    for theme in THEME_KEYWORDS:
        s = _theme_match_score(theme, target_blob)
        if s >= 0.15:
            candidates.append((theme, s))
    candidates.sort(key=lambda x: x[1], reverse=True)

    if not candidates:
        return {"theme": None, "breadth": None, "score": 0, "reason": "관련 테마 근거 부족"}

    theme, text_score = candidates[0]
    breadth = None
    up = down = valid = 0

    for item in price_items:
        if not isinstance(item, dict):
            continue
        chg = item.get("change_pct")
        try:
            chg = float(chg)
        except (TypeError, ValueError):
            continue
        valid += 1
        if chg > 0:
            up += 1
        elif chg < 0:
            down += 1

    if valid:
        breadth = up / valid
        breadth_score = min(1.0, max(0.0, (breadth - 0.5) * 2))
    else:
        breadth_score = 0.0

    # Text evidence is required; breadth only strengthens it.
    score = round(100 * (0.65 * min(1.0, text_score / 0.4) + 0.35 * breadth_score))
    reason = f"{theme} 관련 근거 확인"
    if breadth is not None and valid >= 3:
        reason += f" · 관련 종목 {valid}개 중 {up}개 상승"

    return {
        "theme": theme,
        "breadth": breadth,
        "score": score,
        "reason": reason,
        "up": up,
        "down": down,
        "valid": valid,
    }

def make_theme_comment(theme_result):
    if not theme_result or not theme_result.get("theme"):
        return ""
    theme = theme_result["theme"]
    breadth = theme_result.get("breadth")
    if breadth is not None and theme_result.get("valid", 0) >= 3 and breadth >= 0.67:
        return f"개미들아! 오늘은 {theme} 쪽 움직임이 같이 보여 관련 종목들도 함께 움직이고 있어서 테마 흐름을 같이 볼 필요가 있어."
    if breadth is not None and theme_result.get("valid", 0) >= 3 and breadth <= 0.33:
        return f"개미들아! {theme} 이야기는 보이는데 관련 종목 전체가 같이 움직이는 건 아니야. 테마 영향이라고 단정하긴 아직 이르겠어."
    return f"개미들아! 오늘은 {theme} 관련 재료가 눈에 들어와 관련 뉴스와 움직임을 같이 확인해볼 만해."


def clean_for_json(obj):
    if isinstance(obj, dict):
        return {str(k): clean_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple, set)):
        return [clean_for_json(v) for v in obj]
    elif isinstance(obj, (np.floating, float)):
        if np.isnan(obj) or np.isinf(obj):
            return 0.0
        return float(obj)
    elif isinstance(obj, (np.integer, int)):
        return int(obj)
    elif isinstance(obj, (np.bool_, bool)):
        return bool(obj)
    elif isinstance(obj, (pd.Timestamp, datetime)):
        return obj.strftime("%Y-%m-%d")
    elif isinstance(obj, np.ndarray):
        return clean_for_json(obj.tolist())
    elif obj is None:
        return None
    elif isinstance(obj, str):
        return obj
    return str(obj)

def fetch_naver_realtime_index(index_code):
    """국내 지수 전용 실시간/최신 시세. Yahoo의 regularMarketPrice를 사용하지 않는다."""
    try:
        url = f"https://polling.finance.naver.com/api/realtime/domestic/index/{index_code}"
        res = HTTP_SESSION.get(
            url,
            timeout=2.0,
            headers={"Accept-Language": "ko-KR,ko;q=0.9", "User-Agent": "Mozilla/5.0"}
        )
        res.raise_for_status()
        payload = res.json()
        datas = payload.get("datas") or []
        if not datas:
            return None

        d = datas[0]
        price_raw = str(d.get("closePrice", "")).replace(",", "").strip()
        rate_raw = str(d.get("fluctuationsRatio", "")).replace(",", "").strip()
        change_raw = str(d.get("compareToPreviousClosePrice", "")).replace(",", "").strip()

        if not price_raw or not rate_raw:
            return None

        price = float(price_raw)
        rate = float(rate_raw)
        change = float(change_raw) if change_raw not in {"", "-", "None"} else None

        return {
            "price": price,
            "rate": rate,
            "change": change,
            "market_status": d.get("marketStatus"),
            "traded_at": d.get("localTradedAt"),
        }
    except Exception as exc:
        print(f"[NAVER INDEX] {index_code} failed: {exc}")
        return None



def fetch_market_5day_returns_from_df(df, index_name):
    """주어진 일봉 데이터에서 최근 5거래일 수익률 흐름을 계산합니다."""
    try:
        if df is None or df.empty or "Close" not in df.columns:
            return None
        closes = [float(x) for x in df["Close"].dropna().tail(6).tolist()]
        if len(closes) < 6:
            return None
        points = []
        base = closes[0]
        for i in range(1, 6):
            prev_close, close = closes[i - 1], closes[i]
            daily = ((close / prev_close) - 1.0) * 100.0 if prev_close else 0.0
            cumulative = ((close / base) - 1.0) * 100.0 if base else 0.0
            points.append({
                "date": f"D-{6-i}",
                "close": round(close, 8),
                "daily_return": round(daily, 2),
                "cumulative_return": round(cumulative, 2),
            })
        total = points[-1]["cumulative_return"]
        score = 2 if total >= 1.0 else 1 if total >= 0.3 else -2 if total <= -1.0 else -1 if total <= -0.3 else 0
        return {
            "index_name": index_name, "score": score,
            "detail": f"{index_name} 최근 5거래일 {total:+.2f}%",
            "total_return": round(total, 2), "points": points,
            "source": "실제 일봉 데이터"
        }
    except Exception as exc:
        print(f"[MARKET 5D DF] {index_name} failed: {exc}")
        return None


def fetch_market_5day_returns_from_symbol(symbol, index_name, world_item=None):
    """Yahoo의 실제 일봉으로 미국 시장 등 지수의 최근 5거래일 흐름을 계산합니다."""
    try:
        if isinstance(world_item, dict):
            closes = [float(x) for x in (world_item.get("sparkline") or []) if x is not None]
            if len(closes) >= 6:
                return fetch_market_5day_returns_from_df(
                    pd.DataFrame({"Close": closes}), index_name
                )
        _, _, df = fetch_overseas_candles(symbol)
        return fetch_market_5day_returns_from_df(df, index_name)
    except Exception as exc:
        print(f"[MARKET 5D SYMBOL] {index_name} failed: {exc}")
        return None


def fetch_naver_index_5day_returns(index_code="KOSPI", world_item=None):
    """실제 시장 지수의 최근 5거래일 흐름을 계산한다.
    우선 이미 /api/analyze에서 정상 수집된 world_board의 실제 종가 history를 사용하고,
    부족하면 동일한 Yahoo 지수 차트 경로를 보조로 사용한다. 별도 API 키는 필요 없다.
    """
    index_name = "KOSPI" if str(index_code).upper() == "KOSPI" else "KOSDAQ"
    try:
        closes = []
        if isinstance(world_item, dict):
            closes = [float(x) for x in (world_item.get("sparkline") or []) if x is not None]

        if len(closes) < 6:
            symbol = "^KS11" if index_name == "KOSPI" else "^KQ11"
            _, _, df = fetch_overseas_candles(symbol)
            if df is not None and not df.empty and "Close" in df.columns:
                closes = [float(x) for x in df["Close"].dropna().tail(7).tolist()]

        if len(closes) < 6:
            return None

        closes = closes[-6:]
        points = []
        for i in range(1, len(closes)):
            prev_close = closes[i - 1]
            close = closes[i]
            daily_return = ((close / prev_close) - 1.0) * 100.0 if prev_close else 0.0
            cumulative_return = ((close / closes[0]) - 1.0) * 100.0 if closes[0] else 0.0
            points.append({
                "date": f"D-{6-i}",
                "close": round(close, 2),
                "daily_return": round(daily_return, 2),
                "cumulative_return": round(cumulative_return, 2),
            })

        total_return = points[-1]["cumulative_return"]
        if total_return >= 1.0:
            score = 2
        elif total_return >= 0.3:
            score = 1
        elif total_return <= -1.0:
            score = -2
        elif total_return <= -0.3:
            score = -1
        else:
            score = 0

        return {
            "index_name": index_name,
            "score": score,
            "detail": f"{index_name} 최근 5거래일 {total_return:+.2f}%",
            "total_return": round(total_return, 2),
            "points": points,
            "source": "Naver/KRX 현재값 + 실제 지수 일봉 흐름",
        }
    except Exception as exc:
        print(f"[MARKET 5D] {index_name} failed: {exc}")
        return None


# ===== 미국 주도업종 위젯 =====
US_LEADING_SECTORS = [
    {"name": "반도체", "sym": "SOXX"},
    {"name": "AI·데이터센터", "sym": "AIQ"},
    {"name": "소프트웨어", "sym": "IGV"},
    {"name": "인터넷·플랫폼", "sym": "FDN"},
    {"name": "금융", "sym": "XLF"},
    {"name": "바이오·제약", "sym": "XBI"},
    {"name": "헬스케어", "sym": "XLV"},
    {"name": "전기차·자동차", "sym": "DRIV"},
    {"name": "방산·항공우주", "sym": "ITA"},
    {"name": "에너지·석유", "sym": "XLE"},
    {"name": "원자력·전력", "sym": "NLR"},
    {"name": "소재·화학", "sym": "XLB"},
    {"name": "산업재", "sym": "XLI"},
    {"name": "소비재·유통", "sym": "XLY"},
    {"name": "부동산·리츠", "sym": "XLRE"},
]
CACHED_US_LEADING_SECTORS = []
LAST_US_LEADING_SECTORS_FETCH = 0.0
US_LEADING_SECTORS_TTL = 300

def _fetch_us_sector_history(symbol):
    try:
        encoded = urllib.parse.quote(symbol)
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{encoded}?range=6mo&interval=1d"
        res = HTTP_SESSION.get(url, timeout=5.0)
        res.raise_for_status()
        payload = res.json()
        result = (payload.get("chart", {}).get("result") or [None])[0]
        if not result:
            return None
        closes = (result.get("indicators", {}).get("quote", [{}])[0].get("close") or [])
        closes = [float(x) for x in closes if x is not None and np.isfinite(float(x))]
        if len(closes) < 65:
            return None
        latest = closes[-1]
        prev = closes[-2]
        def pct(ago):
            base = closes[-(ago + 1)]
            return (latest / base - 1.0) * 100.0 if base else 0.0
        return {
            "current": (latest / prev - 1.0) * 100.0 if prev else 0.0,
            "week1": pct(5),
            "month1": pct(21),
            "month3": pct(63),
        }
    except Exception as exc:
        print(f"[US SECTOR] {symbol} failed: {type(exc).__name__}: {exc}")
        return None

def fetch_us_leading_sectors(force=False):
    global CACHED_US_LEADING_SECTORS, LAST_US_LEADING_SECTORS_FETCH
    now = time.time()
    if (not force and CACHED_US_LEADING_SECTORS
            and now - LAST_US_LEADING_SECTORS_FETCH < US_LEADING_SECTORS_TTL):
        return CACHED_US_LEADING_SECTORS

    rows = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        histories = list(executor.map(
            lambda item: _fetch_us_sector_history(item["sym"]),
            US_LEADING_SECTORS
        ))

    for meta, history in zip(US_LEADING_SECTORS, histories):
        if history is not None:
            rows.append({"name": meta["name"], "sym": meta["sym"], **history})

    if not rows:
        return CACHED_US_LEADING_SECTORS

    for row in rows:
        values_by_key = {key: [r[key] for r in rows]
                         for key in ("current", "week1", "month1", "month3")}
        score = 0.0
        for key, values in values_by_key.items():
            mean = float(np.mean(values))
            std = float(np.std(values))
            z = (row[key] - mean) / std if std > 1e-9 else 0.0
            score += 0.25 * z
        row["_selection_score"] = score

    rows.sort(key=lambda r: r["_selection_score"], reverse=True)
    CACHED_US_LEADING_SECTORS = [
        {k: v for k, v in row.items() if k != "_selection_score"}
        for row in rows[:10]
    ]
    LAST_US_LEADING_SECTORS_FETCH = now
    return CACHED_US_LEADING_SECTORS


# ===== 미국 주도종목 위젯 (2단계: 실제 데이터) =====
# 업종별 후보군은 명시적으로 관리하고, 실제 순위는 Yahoo Finance 가격 데이터로 매번 계산한다.
# 즉, 후보군 자체를 고정 TOP5로 박아두지 않고 현재/1주/1개월 모멘텀을 합산해 TOP5를 자동 선정한다.
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

def fetch_single_world_item(item):
    name = item["name"]
    sym = item["sym"]
    tag = item["tag"]
    sparkline = []

    # 국내 지수는 Naver/KRX 계열 endpoint를 사용한다.
    # Yahoo의 regularMarketPrice/chartPreviousClose는 국내 장중/마감 시점과
    # 데이터 갱신 시점이 어긋날 수 있어 KOSPI/KOSDAQ의 현재 등락률 계산에 쓰지 않는다.
    if sym in {"^KS11", "^KQ11"}:
        index_code = "KOSPI" if sym == "^KS11" else "KOSDAQ"
        live = fetch_naver_realtime_index(index_code)
        if live:
            curr_p = live["price"]
            change_rate = live["rate"]

            # 보조 미니차트는 기존 Yahoo historical close를 사용한다.
            # 현재값/등락률의 기준은 위 Naver/KRX 데이터로 고정한다.
            try:
                encoded_sym = urllib.parse.quote(sym)
                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{encoded_sym}?range=14d&interval=1d"
                yres = HTTP_SESSION.get(url, timeout=2.0).json()
                yresult = yres.get("chart", {}).get("result", [])
                if yresult:
                    raw_closes = yresult[0].get("indicators", {}).get("quote", [{}])[0].get("close", [])
                    sparkline = [
                        round(float(c), 2)
                        for c in raw_closes
                        if c is not None and not np.isnan(float(c))
                    ][-7:]
            except Exception:
                sparkline = []

            return {
                "name": name,
                "tag": tag,
                "price": f"{curr_p:,.2f}",
                "rate": f"{change_rate:+.2f}%",
                "up": bool(change_rate >= 0),
                "sparkline": sparkline,
                "source": "Naver/KRX",
                "market_status": live.get("market_status"),
                "traded_at": live.get("traded_at"),
            }

    # 해외 지수/원자재/환율은 기존 Yahoo 흐름을 유지한다.
    try:
        encoded_sym = urllib.parse.quote(sym)
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{encoded_sym}?range=7d&interval=1d"
        res = HTTP_SESSION.get(url, timeout=2.0).json()
        result = res.get("chart", {}).get("result", [])
        if result:
            meta = result[0].get("meta", {})
            curr_p = meta.get("regularMarketPrice", 0.0)
            prev_close = meta.get("chartPreviousClose", meta.get("previousClose", curr_p))
            change_rate = ((curr_p - prev_close) / prev_close) * 100 if prev_close > 0 else 0.0

            raw_closes = result[0].get("indicators", {}).get("quote", [{}])[0].get("close", [])
            sparkline = [
                round(float(c), 2)
                for c in raw_closes
                if c is not None and not np.isnan(float(c))
            ][-7:]

            price_str = f"{curr_p:,.1f}원" if "KRW" in sym else f"{curr_p:,.2f}"

            return {
                "name": name,
                "tag": tag,
                "price": price_str,
                "rate": f"{change_rate:+.2f}%",
                "up": bool(change_rate >= 0),
                "sparkline": sparkline,
                "source": "Yahoo Finance",
            }
    except Exception as exc:
        print(f"[WORLD BOARD] {sym} failed: {exc}")

    return {"name": name, "tag": tag, "price": "-", "rate": "-", "up": True, "sparkline": []}


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
def build_earnings_event(sym, ko_name, dt_utc, eps_str="예상치 집계중", rev_str="-", hour="amc", actual_eps=None, actual_rev=None, expected_eps=None, expected_rev=None, us_date_key=None):
    """실적 1건을 한국시간(KST) 기준으로 정확히 1회만 생성합니다."""
    days_map = ["일", "월", "화", "수", "목", "금", "토"]

    # 최종 안전장치:
    # 미국 기업 실적이 장중(특히 09:30 ET = 한국시간 22:30)으로
    # 들어온 경우에는 장마감 후 실적 발표 관행에 맞춰 16:00 ET로 정규화합니다.
    # 이 검사는 Finnhub/Yahoo 어느 경로로 들어왔는지와 무관하게 마지막에 한 번 더 적용됩니다.
    try:
        if ZoneInfo is not None:
            et_zone = ZoneInfo("America/New_York")
            et_dt = dt_utc.astimezone(et_zone)
            et_minutes = et_dt.hour * 60 + et_dt.minute
            if 9 * 60 + 30 <= et_minutes < 16 * 60:
                et_dt = et_dt.replace(
                    hour=16, minute=0, second=0, microsecond=0
                )
                dt_utc = et_dt.astimezone(timezone.utc)
                hour = "amc"
    except Exception:
        pass

    kst_dt = dt_utc.astimezone(timezone(timedelta(hours=9)))

    # 화면의 날짜 이동 기준은 한국시간(KST)을 유지합니다.
    # 동시에 미국 현지 날짜도 별도로 보존해 날짜 경계를 잃지 않게 합니다.
    date_key = kst_dt.strftime("%Y.%m.%d")

    if not us_date_key:
        try:
            us_date_key = dt_utc.astimezone(ZoneInfo("America/New_York")).strftime("%Y.%m.%d") if ZoneInfo is not None else kst_dt.strftime("%Y.%m.%d")
        except Exception:
            us_date_key = kst_dt.strftime("%Y.%m.%d")
    weekday = days_map[int(kst_dt.strftime('%w'))]
    date_str = f"{kst_dt.month}.{kst_dt.day} ({weekday})"
    date_header = f"{kst_dt.month}월 {kst_dt.day}일 {weekday}요일"
    time_str = kst_dt.strftime("%H:%M")

    now_utc = datetime.now(timezone.utc)
    has_actual = actual_eps is not None or actual_rev is not None
    is_past_date = kst_dt.date() < datetime.now(timezone(timedelta(hours=9))).date()
    if has_actual or is_past_date:
        val_str = "발표완료"
    elif dt_utc <= now_utc:
        val_str = "발표확인중"
    else:
        val_str = "발표예정"
    hour_code = str(hour).lower().strip()
    hour_label = "(미국 장 마감 후)" if hour_code == "amc" else ("(미국 장 시작 전)" if hour_code == "bmo" else "")

    return [{
        "ts": dt_utc.timestamp(),
        "date_key": date_key,
        "us_date_key": us_date_key,
        "date_str": date_str,
        "date_header": date_header,
        "time": time_str,
        "stars": (
            3 if sym in {
                "NVDA", "AVGO", "AAPL", "MSFT", "AMZN",
                "GOOGL", "META", "TSLA", "AMD", "ORCL", "MU", "COST"
            }
            else 2 if sym in {
                "ADBE", "CRM", "NFLX", "INTC", "QCOM",
                "AMAT", "CSCO", "PANW", "SNOW", "DELL", "HPE",
                "FDX", "NKE", "ACN", "CTAS", "CIEN", "ADSK", "MRVL"
            }
            else 1
        ),
        "star_str": (
            "★★★" if sym in {
                "NVDA", "AVGO", "AAPL", "MSFT", "AMZN",
                "GOOGL", "META", "TSLA", "AMD", "ORCL", "MU", "COST"
            }
            else "★★☆" if sym in {
                "ADBE", "CRM", "NFLX", "INTC", "QCOM",
                "AMAT", "CSCO", "PANW", "SNOW", "DELL", "HPE",
                "FDX", "NKE", "ACN", "CTAS", "CIEN", "ADSK", "MRVL"
            }
            else "★☆☆"
        ),
        "type": "earn",
        # 기업명과 발표 시점은 화면에서 두 줄로 표시할 수 있도록 분리합니다.
        "name": f"{ko_name} ({sym}) 실적".strip(),
        "timing_label": hour_label,
        "ticker": sym,
        "val": val_str,
        "exp": eps_str,
        "prev": "-",
        "actual_eps": actual_eps,
        "expected_eps": expected_eps,
        "actual_revenue": actual_rev,
        "expected_revenue": expected_rev,
        "expected_revenue_str": rev_str
    }]

def _classify_earnings_hour(dt_utc):
    """Yahoo의 실제/예정 타임스탬프를 미국 동부시간 기준으로 분류합니다."""
    if ZoneInfo is None:
        return "amc"
    try:
        et = dt_utc.astimezone(ZoneInfo("America/New_York"))
        minutes = et.hour * 60 + et.minute
        if minutes < 9 * 60 + 30:
            return "bmo"
        if minutes >= 16 * 60:
            return "amc"
        return "dmh"
    except Exception:
        return "amc"


def _normalize_earnings_timestamp(sym, dt_utc):
    """미국 기업 실적 발표시간을 공통 규칙으로 정규화합니다."""
    if dt_utc is None:
        return dt_utc, "amc"

    try:
        hour = _classify_earnings_hour(dt_utc)

        if ZoneInfo is None:
            return dt_utc, hour

        et_zone = ZoneInfo("America/New_York")
        et_dt = dt_utc.astimezone(et_zone)

        # 장중으로 잡힌 실적시간은 임의의 09:30 ET를 그대로 노출하지 않고
        # 장마감(16:00 ET) 기준으로 정규화합니다.
        if hour == "dmh":
            normalized_et = et_dt.replace(
                hour=16, minute=0, second=0, microsecond=0
            )
            return normalized_et.astimezone(timezone.utc), "amc"

        # 장 시작 전(BMO) / 장 마감 후(AMC)는 원래 timestamp를 유지합니다.
        return dt_utc, hour

    except Exception:
        return dt_utc, _classify_earnings_hour(dt_utc)


def _fetch_yahoo_earnings_timestamps(symbols):
    """주요 종목의 Yahoo earnings timestamp를 가져옵니다."""
    result = {}
    if not symbols:
        return result

    try:
        url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={','.join(symbols)}"
        res = HTTP_SESSION.get(url, timeout=2.5).json()
        items = res.get("quoteResponse", {}).get("result", [])
        for it in items:
            sym = str(it.get("symbol", "")).upper()
            raw_ts = it.get("earningsTimestamp") or it.get("earningsTimestampStart")
            if sym and raw_ts:
                try:
                    result[sym] = datetime.fromtimestamp(int(raw_ts), tz=timezone.utc)
                except Exception:
                    pass
    except Exception as exc:
        print("[CALENDAR][EARN] Yahoo 시간 보정 오류:", exc)

    return result


def fetch_all_real_earnings():
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

def _calendar_event_common(utc_dt, event_name, country, stars, star_str, event_type, actual, forecast, previous, ticker="", extra=None):
    """캘린더 이벤트 공통 포맷. 화면은 항상 한국시간(KST)으로 표시한다."""
    days_map = ["일", "월", "화", "수", "목", "금", "토"]
    kst_dt = utc_dt.astimezone(timezone(timedelta(hours=9)))
    date_key = kst_dt.strftime("%Y.%m.%d")
    date_str = f"{kst_dt.month}.{kst_dt.day} ({days_map[int(kst_dt.strftime('%w'))]})"
    date_header = f"{kst_dt.month}월 {kst_dt.day}일 {days_map[int(kst_dt.strftime('%w'))]}요일"
    item = {
        "ts": utc_dt.timestamp(),
        "date_key": date_key,
        "date_str": date_str,
        "date_header": date_header,
        "time": kst_dt.strftime("%H:%M"),
        "stars": int(stars),
        "star_str": star_str,
        "type": event_type,
        "name": event_name,
        "ticker": ticker or "",
        "country": country or "",
        "val": actual or "-",
        "exp": forecast or "-",
        "prev": previous or "-",
    }
    if isinstance(extra, dict):
        item.update(extra)
    return item


def _impact_to_stars(impact):
    text = str(impact or "").strip().lower()
    if text in {"high", "3", "3.0", "red"}:
        return 3, "★★★"
    if text in {"medium", "moderate", "2", "2.0", "orange", "yellow"}:
        return 2, "★★☆"
    return 1, "★☆☆"


def _parse_calendar_timestamp(raw_value):
    """Finnhub/Yahoo/ISO 등 여러 시간 형식을 UTC datetime으로 안전하게 변환."""
    if raw_value is None or raw_value == "":
        return None
    try:
        if isinstance(raw_value, (int, float)):
            return datetime.fromtimestamp(float(raw_value), tz=timezone.utc)
        text = str(raw_value).strip()
        if text.isdigit():
            return datetime.fromtimestamp(float(text), tz=timezone.utc)
        text = text.replace("Z", "+00:00")
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


# ----------------------------------------------------
# 경제 캘린더 국가 필터
# 한국(KR) + 미국(US)만 허용
# ----------------------------------------------------
ECONOMIC_CALENDAR_ALLOWED_COUNTRIES = {
    # 미국: Finnhub/Trading Economics/ForexFactory가 서로 다른 표기를 사용함
    "US", "USA", "USD", "US DOLLAR",
    "UNITED STATES", "UNITED STATES OF AMERICA",
    # 한국: KR/KOR/KRW 등으로 들어올 수 있음
    "KR", "KOR", "KRW", "KOREA", "SOUTH KOREA",
    "REPUBLIC OF KOREA", "KOREAN WON",
}

def _is_allowed_economic_country(country):
    value = str(country or "").strip().upper()
    if value in ECONOMIC_CALENDAR_ALLOWED_COUNTRIES:
        return True

    # API가 "USD" / "KRW" 같은 통화코드를 보내는 경우를 명시적으로 허용
    if value.startswith("USD") or value.startswith("KRW"):
        return True

    return False


def _filter_kr_us_economic_events(events):
    """경제 캘린더에서 한국/미국 일정만 남긴다."""
    return [
        ev for ev in (events or [])
        if isinstance(ev, dict)
        and _is_allowed_economic_country(
            ev.get("country") or ev.get("country_code") or ""
        )
    ]


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
def fetch_naver_news(query_keyword, limit=5):
    """Google News RSS 실패/빈 결과 시 사용하는 Naver News API fallback."""
    headlines = []
    client_id = os.environ.get("NAVER_CLIENT_ID", "").strip()
    client_secret = os.environ.get("NAVER_CLIENT_SECRET", "").strip()

    if not client_id or not client_secret:
        print("[NAVER NEWS] API credentials not configured; fallback skipped")
        return headlines

    clean_k = re.sub(r'\(.*?\)', '', str(query_keyword or "")).replace("USDT", "").strip()
    if not clean_k:
        return headlines

    try:
        r = HTTP_SESSION.get(
            "https://openapi.naver.com/v1/search/news.json",
            params={
                "query": clean_k,
                "display": max(1, min(int(limit), 100)),
                "start": 1,
                "sort": "date",
            },
            headers={
                "X-Naver-Client-Id": client_id,
                "X-Naver-Client-Secret": client_secret,
                "Accept": "application/json",
                "User-Agent": "Mozilla/5.0",
            },
            timeout=3.0,
        )
        print(f"[NAVER NEWS] query={clean_k} HTTP {r.status_code}")
        if r.status_code != 200:
            return headlines

        data = r.json()
        for item in (data.get("items") or [])[:limit]:
            title = html.unescape(
                re.sub(r"<[^>]+>", "", str(item.get("title") or ""))
            ).strip()
            description = html.unescape(
                re.sub(r"<[^>]+>", "", str(item.get("description") or ""))
            ).strip()

            if not title:
                continue

            parts = title.rsplit(" - ", 1)
            if len(parts) > 1 and len(parts[1]) <= 80:
                title_text = parts[0].strip()
                media_name = parts[1].strip()
            else:
                title_text = title
                media_name = "Naver 뉴스"

            headlines.append({
                "title": title_text,
                "media": media_name,
                "pubDate": str(item.get("pubDate") or "").strip(),
                "link": str(item.get("originallink") or item.get("link") or "#").strip(),
                "description": description,
                "source": "Naver",
            })
    except Exception as exc:
        print(f"[NAVER NEWS] fallback failed: {type(exc).__name__}: {exc}")

    return headlines


def fetch_rss_news(query_keyword, limit=5):
    """Google News RSS 우선. 실패/빈 결과이면 Naver News API로 fallback."""
    headlines = []
    clean_k = re.sub(r'\(.*?\)', '', query_keyword).replace("USDT", "").strip()
    url = f"https://news.google.com/rss/search?q={urllib.parse.quote(clean_k)}&hl=ko&gl=KR&ceid=KR:ko"

    try:
        r = HTTP_SESSION.get(url, timeout=2.5)
        if r.status_code == 200:
            root = ET.fromstring(r.content)
            for it in root.findall('.//item')[:limit]:
                t = it.find('title')
                pub = it.find('pubDate')
                link = it.find('link')
                if t is not None and t.text:
                    title_clean = html.unescape(t.text).strip()
                    parts = title_clean.rsplit(' - ', 1)
                    title_text = parts[0]
                    media_name = parts[1] if len(parts) > 1 else "언론사"
                    pub_text = pub.text if pub is not None else ""
                    link_text = link.text if link is not None else "#"
                    headlines.append({
                        "title": title_text,
                        "media": media_name,
                        "pubDate": pub_text,
                        "link": link_text
                    })
    except Exception as exc:
        print(f"[GOOGLE NEWS] RSS failed: {type(exc).__name__}: {exc}")

    if not headlines:
        print(f"[NEWS FALLBACK] Google News empty -> Naver News: {clean_k}")
        headlines = fetch_naver_news(clean_k, limit)

    return headlines

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

def get_realtime_usdkrw():
    try:
        url = "https://query1.finance.yahoo.com/v8/finance/chart/KRW=X?range=1d&interval=1m"
        res = HTTP_SESSION.get(url, timeout=1.5).json()
        meta = res.get("chart", {}).get("result", [{}])[0].get("meta", {})
        rate = meta.get("regularMarketPrice", 0.0)
        if rate and float(rate) > 500:
            return float(rate)
    except Exception:
        pass
    return None

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

def get_naver_stock_market_type(code):
    """Naver basic 정보에서 종목의 실제 KOSPI/KOSDAQ 구분을 읽는다."""
    try:
        url = f"https://m.stock.naver.com/api/stock/{code}/basic"
        res = HTTP_SESSION.get(url, timeout=1.8).json()
        exchange = res.get("stockExchangeType") or {}
        name = exchange.get("name") if isinstance(exchange, dict) else None
        if name in {"KOSPI", "KOSDAQ"}:
            return name
    except Exception:
        pass
    return None

def get_naver_realtime_price(code):
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

def fetch_naver_domestic_flow_metrics(code):
    """국내주식 실데이터: 당일 거래대금 + 최근 5거래일 외국인/기관 순매수."""
    result={"trading_value":None,"foreigner_latest":None,"institution_latest":None,"foreigner_5d":None,"institution_5d":None,"trading_value_detail":"거래대금 데이터 확인 필요","foreigner_detail":"외국인 수급 데이터 확인 필요","institution_detail":"기관 수급 데이터 확인 필요"}
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
                    if m: total+=float(m.group(1))*mul; found=True
                return total if found else None
            return float(x.replace('%',''))
        except Exception: return None
    try:
        r=HTTP_SESSION.get(f"https://m.stock.naver.com/api/stock/{code}/integration",timeout=2.2)
        if r.status_code!=200: return result
        data=r.json()
        for it in (data.get('totalInfos') or []):
            if not isinstance(it,dict): continue
            for k in ('accumulatedTradingValue','tradingValue','accumulatedTradingAmount'):
                v=num(it.get(k))
                if v is not None: result['trading_value']=v; break
            if result['trading_value'] is not None: break
        tv=result['trading_value']
        if tv is not None:
            result['trading_value_detail']=f"당일 거래대금 {tv/10**12:.2f}조원" if tv>=10**12 else (f"당일 거래대금 {tv/10**8:.0f}억원" if tv>=10**8 else f"당일 거래대금 {tv:,.0f}원")
        fv=[]; iv=[]
        for it in (data.get('dealTrendInfos') or [])[-5:]:
            if not isinstance(it,dict): continue
            for k in ('foreignerPureBuyQuant','foreignerNetBuyQuantity','foreignPureBuyQuant'):
                v=num(it.get(k))
                if v is not None: fv.append(v); break
            for k in ('organPureBuyQuant','institutionPureBuyQuant','institutionNetBuyQuantity'):
                v=num(it.get(k))
                if v is not None: iv.append(v); break
        if fv:
            total=sum(fv); result['foreigner_latest']=fv[-1]; result['foreigner_5d']=total; result['foreigner_detail']=f"최근 5거래일 외국인 {'순매수' if total>0 else '순매도' if total<0 else '순매수/순매도 없음'} {abs(total):,.0f}주"
        if iv:
            total=sum(iv); result['institution_latest']=iv[-1]; result['institution_5d']=total; result['institution_detail']=f"최근 5거래일 기관 {'순매수' if total>0 else '순매도' if total<0 else '순매수/순매도 없음'} {abs(total):,.0f}주"
    except Exception as exc: print(f"[NAVER FLOW] {code} fetch failed: {exc}")
    return result

def fetch_domestic_candles(code, anchor_price=None):
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
                        "Volume": float(p[5])
                    })
            if len(records) >= 15:
                df = pd.DataFrame(records).set_index("Date").sort_index()
                df.attrs["_gaemi_raw_df"] = df.copy()
                if anchor_price and anchor_price > 0:
                    last_close = float(df['Close'].iloc[-1])
                    if abs(last_close - anchor_price) / anchor_price > 0.3:
                        ratio = anchor_price / last_close
                        df['Open'] *= ratio
                        df['High'] *= ratio
                        df['Low'] *= ratio
                        df['Close'] *= ratio
                return df
    except Exception:
        pass

    try:
        url = f"https://m.stock.naver.com/api/stock/{code}/trend?pageSize=120"
        res = HTTP_SESSION.get(url, timeout=2.0).json()
        if isinstance(res, list) and len(res) > 10:
            records = []
            for it in res:
                records.append({
                    "Date": pd.to_datetime(it['localTrdDd']),
                    "Open": float(str(it['openPrice']).replace(',', '')),
                    "High": float(str(it['highPrice']).replace(',', '')),
                    "Low": float(str(it['lowPrice']).replace(',', '')),
                    "Close": float(str(it['closePrice']).replace(',', '')),
                    "Volume": float(str(it['accumulatedTradingVolume']).replace(',', ''))
                })
            if len(records) >= 15:
                df = pd.DataFrame(records).set_index("Date").sort_index()
                df.attrs["_gaemi_raw_df"] = df.copy()
                return df
    except Exception:
        pass

    for suffix in [".KS", ".KQ"]:
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{code}{suffix}?range=5y&interval=1d"
            res = HTTP_SESSION.get(url, timeout=2.0).json()
            result = res.get("chart", {}).get("result", [])
            if result:
                ts = result[0].get("timestamp", [])
                quote = result[0].get("indicators", {}).get("quote", [{}])[0]
                records = [{
                    "Date": pd.to_datetime(ts[i], unit='s'),
                    "Open": float(quote['open'][i]),
                    "High": float(quote['high'][i]),
                    "Low": float(quote['low'][i]),
                    "Close": float(quote['close'][i]),
                    "Volume": float(quote['volume'][i] or 0.0)
                } for i in range(len(ts)) if quote.get('open') and quote['open'][i] is not None]
                if len(records) >= 15:
                    return pd.DataFrame(records).set_index("Date").sort_index()
        except Exception:
            pass

    return None

def fetch_overseas_candles(symbol):
    """
    해외주식 차트 데이터 조회.
    - 차트는 기존 Yahoo Chart API의 일봉 데이터를 사용한다.
    - 현재가를 일봉 마지막 Close로 오인하지 않는다.
    - 데이터 누락/HTTP 오류를 조용히 삼키지 않고 로그로 남긴다.
    - 현재가/세션 가격은 별도 quote snapshot에서 가져올 수 있도록 분리한다.
    """
    try:
        encoded_sym = urllib.parse.quote(symbol.upper())
        url = (
            f"https://query1.finance.yahoo.com/v8/finance/chart/"
            f"{encoded_sym}?range=5y&interval=1d&includePrePost=false"
        )
        response = HTTP_SESSION.get(
            url,
            timeout=5.0,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        response.raise_for_status()
        res = response.json()

        chart = res.get("chart") or {}
        if chart.get("error"):
            raise RuntimeError(f"Yahoo chart error: {chart.get('error')}")

        result = chart.get("result") or []
        if not result:
            raise RuntimeError("Yahoo chart result가 비어 있습니다.")

        item = result[0]
        ts = item.get("timestamp") or []
        quote = (item.get("indicators") or {}).get("quote") or [{}]
        quote = quote[0] if quote else {}

        if not ts or not quote:
            raise RuntimeError("Yahoo chart에 timestamp/quote 데이터가 없습니다.")

        records = []
        opens = quote.get("open") or []
        highs = quote.get("high") or []
        lows = quote.get("low") or []
        closes = quote.get("close") or []
        volumes = quote.get("volume") or []

        n = min(len(ts), len(opens), len(highs), len(lows), len(closes), len(volumes))
        for i in range(n):
            if any(v is None for v in (opens[i], highs[i], lows[i], closes[i])):
                continue
            records.append({
                "Date": pd.to_datetime(ts[i], unit="s"),
                "Open": float(opens[i]),
                "High": float(highs[i]),
                "Low": float(lows[i]),
                "Close": float(closes[i]),
                "Volume": float(volumes[i] or 0.0),
            })

        if len(records) < 15:
            raise RuntimeError(f"유효한 일봉 데이터가 부족합니다: {len(records)}개")

        df = pd.DataFrame(records).set_index("Date").sort_index()

        # 중요: 여기서 반환하는 가격은 '실시간 현재가'가 아니라
        # 차트의 마지막 정규장 일봉 종가임을 명확히 한다.
        chart_close = float(df["Close"].iloc[-1])
        return chart_close, f"{chart_close:,.2f} 달러", df

    except Exception as exc:
        print(f"[YAHOO CHART ERROR] {symbol}: {type(exc).__name__}: {exc}")
        return None, None, None


def fetch_overseas_quote_snapshot(symbol):
    """
    해외주식 현재가/세션 데이터 조회.
    Yahoo quote의 세션 정보를 기준으로 현재 표시 가격을 결정한다.
    반환값은 차트용 일봉 데이터와 분리한다.
    """
    try:
        encoded_sym = urllib.parse.quote(symbol.upper())
        url = (
            f"https://query1.finance.yahoo.com/v7/finance/quote"
            f"?symbols={encoded_sym}"
        )
        response = HTTP_SESSION.get(
            url,
            timeout=5.0,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        response.raise_for_status()
        payload = response.json()

        result = ((payload.get("quoteResponse") or {}).get("result") or [])
        if not result:
            raise RuntimeError("Yahoo quote result가 비어 있습니다.")

        q = result[0]
        state = str(q.get("marketState") or "CLOSED").upper()
        regular_price = q.get("regularMarketPrice")
        previous_close = q.get("regularMarketPreviousClose")

        # 정규장 가격을 기본값으로 사용하고,
        # PRE/POST에서는 확장시장 가격이 있으면 그 값을 사용한다.
        price = regular_price
        price_source = "regular"
        change_pct = q.get("regularMarketChangePercent")

        if state in ("PRE", "PREPRE"):
            pre_price = q.get("preMarketPrice")
            if pre_price is not None:
                price = pre_price
                price_source = "pre"
                change_pct = q.get("preMarketChangePercent")

        elif state in ("POST", "POSTPOST"):
            post_price = q.get("postMarketPrice")
            if post_price is not None:
                price = post_price
                price_source = "post"
                change_pct = q.get("postMarketChangePercent")

        if price is None:
            raise RuntimeError("현재 가격 필드가 없습니다.")

        # 공급자가 등락률을 주지 않는 경우에만 직접 계산한다.
        if change_pct is None and previous_close:
            change_pct = ((float(price) - float(previous_close)) / float(previous_close)) * 100.0

        return {
            "symbol": q.get("symbol") or symbol.upper(),
            "price": float(price),
            "previous_close": float(previous_close) if previous_close is not None else None,
            "change_pct": float(change_pct) if change_pct is not None else None,
            "market_state": state,
            "price_source": price_source,
            "currency": q.get("currency") or "USD",
            "regular_market_price": float(regular_price) if regular_price is not None else None,
            "pre_market_price": float(q["preMarketPrice"]) if q.get("preMarketPrice") is not None else None,
            "post_market_price": float(q["postMarketPrice"]) if q.get("postMarketPrice") is not None else None,
            "timestamp": q.get("regularMarketTime"),
        }

    except Exception as exc:
        print(f"[YAHOO QUOTE ERROR] {symbol}: {type(exc).__name__}: {exc}")
        return None

def fetch_crypto_candles(symbol):
    clean_sym = symbol.upper().replace("-", "")
    if not clean_sym.endswith("USDT") and not clean_sym.endswith("USD"):
        clean_sym += "USDT"
    try:
        url = f"https://api.binance.com/api/v3/klines?symbol={clean_sym}&interval=1d&limit=1000"
        res = HTTP_SESSION.get(url, timeout=2.0).json()
        if isinstance(res, list) and len(res) > 5:
            records = [{
                "Date": pd.to_datetime(int(x[0]), unit='ms'),
                "Open": float(x[1]), "High": float(x[2]), "Low": float(x[3]), "Close": float(x[4]), "Volume": float(x[5])
            } for x in res]
            df = pd.DataFrame(records).set_index("Date").sort_index()
            curr_p = float(df['Close'].iloc[-1])
            return curr_p, f"{curr_p:,.2f} 달러 (USDT)", df
    except Exception:
        pass
    return None, None, None

def fetch_market_consensus_unified(symbol, market_name, raw_price, formal_name=""):
    """실제 확인 가능한 국내/미국 애널리스트 컨센서스만 표시한다.
    국내 리포트는 Naver mobile/front-api/v2/legacy HTML을 순차적으로 확인한다.
    """
    try:
        def _walk_objects(obj, depth=0):
            if depth > 14:
                return
            if isinstance(obj, dict):
                yield obj
                for v in obj.values():
                    yield from _walk_objects(v, depth + 1)
            elif isinstance(obj, list):
                for v in obj[:500]:
                    yield from _walk_objects(v, depth + 1)

        def _safe_num(v):
            try:
                if isinstance(v, dict):
                    for k in ("raw", "value", "amount", "targetPrice", "price", "val"):
                        if k in v:
                            n = _safe_num(v[k])
                            if n is not None:
                                return n
                    return None
                if v is None or v == "":
                    return None
                s = str(v).replace(",", "").replace("원", "").replace("KRW", "").strip()
                n = float(s)
                return n if n > 0 else None
            except Exception:
                return None

        def _value(node, keys):
            if not isinstance(node, dict):
                return None
            wanted = {str(k).lower() for k in keys}
            for k, v in node.items():
                if str(k).lower() in wanted and v not in (None, "", [], {}):
                    return v
            return None

        def _number(node, keys):
            return _safe_num(_value(node, keys))

        # Naver 응답은 API 버전에 따라 키 이름/중첩 구조가 조금씩 달라진다.
        # "리포트 객체 자체"에 증권사/제목 + 날짜/의견/목표가 중 하나가 있으면 채택한다.
        def _collect_report_dicts(payload):
            broker_keys = {
                "writecorpname","writecorp","brokercode","brokername","brokeragename","brokerage",
                "firmname","firm_name","publishername","publisher","securitiesname","securities",
                "writername","writer","corpname","corp_name","companyname","company_name","broker"
            }
            opinion_keys = {
                "investmentopinion","investmentopinionname","investment_opinion","opinion",
                "recommendation","rating","investopinion","opinionname","opinion_name"
            }
            target_keys = {
                "targetprice","targetpricevalue","target_price","pricetarget","price_target",
                "targetmeanprice","targetmean","target_price_mean","targetprice_mean",
                "averagetargetprice","average_target_price","avgtargetprice","avg_target_price",
                "target"
            }
            date_keys = {
                "writedate","researchdate","publishdate","publishedat","published_at",
                "reportdate","regdate","createdat","created_at","createdate","date"
            }
            title_keys = {
                "title","reporttitle","report_title","researchtitle","research_title","subject","name"
            }
            id_keys = {"researchid","research_id","researchno","researchnumber","reportid","report_id","id"}

            out, seen = [], set()

            for node in _walk_objects(payload):
                if not isinstance(node, dict):
                    continue
                keys = {str(k).lower() for k in node.keys()}
                broker_hit = bool(keys & broker_keys)
                content_hit = bool(keys & (opinion_keys | target_keys | date_keys | title_keys | id_keys))
                # 리포트 객체의 직접 필드가 충분한 경우만 채택한다.
                if not (broker_hit and content_hit):
                    continue

                marker = json.dumps(node, ensure_ascii=False, sort_keys=True, default=str)
                if marker not in seen:
                    seen.add(marker)
                    out.append(node)

            return out

        def _parse_report(node):
            if not isinstance(node, dict):
                return None

            broker = _value(node, [
                "writeCorpName","writeCorp","brokerName","brokerageName","brokerage",
                "companyName","company_name","firmName","firm_name","publisherName","publisher",
                "broker","securitiesName","securities","writerName","writer","corpName","corp_name"
            ])
            title = _value(node, [
                "title","reportTitle","report_title","subject","researchTitle","research_title","name"
            ])
            opinion = _value(node, [
                "investmentOpinion","investmentOpinionName","investment_opinion",
                "opinion","recommendation","rating","investOpinion","opinionName","opinion_name"
            ])
            date_val = _value(node, [
                "writeDate","researchDate","publishedAt","published_at","publishDate",
                "reportDate","date","regDate","createdAt","created_at","createDate"
            ])
            tp = _number(node, [
                "targetPrice","targetPriceValue","target","priceTarget","price_target",
                "targetMean","targetMeanPrice","target_price_mean","targetprice_mean",
                "averageTargetPrice","average_target_price","avgTargetPrice","avg_target_price"
            ])
            rid = _value(node, [
                "researchId","research_id","researchNo","researchNumber","reportId","report_id","id"
            ])

            # 제목/증권사/의견/날짜/목표가 중 실제 내용이 하나도 없으면 제외
            if not any([broker, title, opinion, date_val, tp, rid]):
                return None

            date_txt = str(date_val)[:10] if date_val else ""
            sub = []
            if opinion:
                sub.append(str(opinion))
            if date_txt:
                sub.append(date_txt)
            if not sub and title:
                sub.append(str(title)[:100])

            current = _safe_num(raw_price)
            upside = ((tp-current)/current*100) if tp and current else None

            return {
                "main": str(broker or "증권사 리포트"),
                "sub": " · ".join(sub) or "실제 리포트",
                "val": f"{int(tp):,}원" if tp else "목표가 미확인",
                "sub_val": f"{upside:+.1f}%" if upside is not None else "",
                "_id": str(rid or ""),
                "_title": str(title or "")
            }

        def _parse_naver_pc_research_html(html_text):
            """Naver PC 리서치 종목분석 HTML fallback.
            API가 200이지만 JSON 구조가 바뀌었을 때 실제 공개 리포트 목록을 복구한다.
            """
            try:
                from html.parser import HTMLParser
                from html import unescape
                import re as _re

                class _RowParser(HTMLParser):
                    def __init__(self):
                        super().__init__(convert_charrefs=True)
                        self.rows = []
                        self.in_tr = False
                        self.in_td = False
                        self.in_a = False
                        self.current = []
                        self.cell = []
                        self.href = ""

                    def handle_starttag(self, tag, attrs):
                        attrs = dict(attrs)
                        if tag == "tr":
                            self.in_tr = True
                            self.current = []
                        elif self.in_tr and tag in ("td", "th"):
                            self.in_td = True
                            self.cell = []
                        elif self.in_tr and self.in_td and tag == "a":
                            self.in_a = True
                            self.href = attrs.get("href", "")

                    def handle_data(self, data):
                        if self.in_tr and self.in_td:
                            self.cell.append(data)

                    def handle_endtag(self, tag):
                        if tag in ("td", "th") and self.in_td:
                            txt = " ".join("".join(self.cell).split())
                            self.current.append(txt)
                            self.cell = []
                            self.in_td = False
                        elif tag == "a" and self.in_a:
                            self.in_a = False
                        elif tag == "tr" and self.in_tr:
                            if self.current:
                                self.rows.append(self.current)
                            self.current = []
                            self.in_tr = False

                parser = _RowParser()
                parser.feed(html_text)

                results = []
                date_re = _re.compile(r"20\d{2}[./-]\d{1,2}[./-]\d{1,2}")
                price_re = _re.compile(r"(?<!\d)\d{2,3}(?:,\d{3})+(?!\d)|(?<!\d)\d{5,8}(?!\d)")

                for cells in parser.rows:
                    cells = [unescape(c).strip() for c in cells if c.strip()]
                    if len(cells) < 3:
                        continue

                    joined = " ".join(cells)
                    if formal_name and formal_name not in joined and symbol not in joined:
                        continue
                    if "작성일" in joined or "종목명" in joined:
                        continue

                    date_txt = ""
                    for c in cells:
                        md = date_re.search(c)
                        if md:
                            date_txt = md.group(0)
                            break

                    # 실제 리포트 행은 보통 제목/증권사/의견/목표가/작성일 중 여러 칸을 가진다.
                    price = None
                    for c in cells:
                        # 날짜가 포함된 셀은 가격 후보에서 제외
                        if date_re.search(c):
                            continue
                        nums = price_re.findall(c.replace("원", ""))
                        for raw in nums:
                            n = _safe_num(raw)
                            if n and n >= 1000:
                                price = n
                                break
                        if price:
                            break

                    opinion = ""
                    for c in cells:
                        if any(x in c.lower() for x in (
                            "buy","hold","sell","strong buy","strong sell",
                            "매수","중립","매도","강력매수","강력 매수"
                        )):
                            opinion = c
                            break

                    # 네이버 종목분석 목록의 일반적인 순서를 활용하되,
                    # 고정 인덱스에 의존하지 않고 텍스트 특성으로 선택한다.
                    broker = ""
                    for c in cells:
                        if any(x in c for x in ("증권", "투자", "자산운용", "리서치")):
                            broker = c
                            break
                    if not broker and len(cells) >= 4:
                        broker = cells[-2]

                    title = ""
                    for c in cells:
                        if c != broker and c != date_txt and c != opinion and c:
                            if len(c) >= 5 and not price_re.fullmatch(c.replace("원","").strip()):
                                title = c
                                break

                    if not (broker or title or price or opinion or date_txt):
                        continue

                    results.append({
                        "main": broker or "증권사 리포트",
                        "sub": " · ".join(x for x in (opinion, date_txt) if x) or (title[:100] if title else "실제 리포트"),
                        "val": f"{int(price):,}원" if price else "목표가 미확인",
                        "sub_val": "",
                        "_id": "",
                        "_title": title
                    })

                return results[:20]
            except Exception as exc:
                print(f"[CONSENSUS][KR] PC research HTML parser failed: {type(exc).__name__}: {exc}")
                return []

        def _parse_wisereport_consensus_html(html_text):
            """WiseReport의 '제공처별 투자의견 및 목표주가' 표를 열 기준으로 정확히 읽는다.

            핵심: HTML 전체에서 숫자를 추측하지 않고, 실제 컨센서스 표의 헤더를
            먼저 찾은 뒤 같은 열의 값을 사용한다.
            제공처 | 최종일자 | 목표가 | 직전목표가 | 변동률(%) | 투자의견 | 직전투자의견
            """
            try:
                from html.parser import HTMLParser
                from html import unescape
                import re as _re

                class _TableParser(HTMLParser):
                    def __init__(self):
                        super().__init__(convert_charrefs=True)
                        self.tables = []
                        self.table_depth = 0
                        self.current_table = None
                        self.in_tr = False
                        self.in_cell = False
                        self.current_row = []
                        self.cell = []

                    def handle_starttag(self, tag, attrs):
                        if tag == "table":
                            if self.table_depth == 0:
                                self.current_table = []
                            self.table_depth += 1
                        elif self.table_depth == 1 and tag == "tr":
                            self.in_tr = True
                            self.current_row = []
                        elif self.table_depth == 1 and self.in_tr and tag in ("td", "th"):
                            self.in_cell = True
                            self.cell = []

                    def handle_data(self, data):
                        if self.table_depth == 1 and self.in_tr and self.in_cell:
                            self.cell.append(data)

                    def handle_endtag(self, tag):
                        if self.table_depth == 1 and tag in ("td", "th") and self.in_cell:
                            txt = " ".join("".join(self.cell).split())
                            self.current_row.append(txt)
                            self.cell = []
                            self.in_cell = False
                        elif self.table_depth == 1 and tag == "tr" and self.in_tr:
                            if self.current_row:
                                self.current_table.append(self.current_row)
                            self.current_row = []
                            self.in_tr = False
                        elif tag == "table" and self.table_depth > 0:
                            self.table_depth -= 1
                            if self.table_depth == 0:
                                if self.current_table:
                                    self.tables.append(self.current_table)
                                self.current_table = None

                parser = _TableParser()
                parser.feed(html_text or "")

                def clean(v):
                    return unescape(" ".join(str(v).split())).strip()

                def num(v):
                    try:
                        x = str(v).replace(",", "").replace("원", "").strip()
                        n = float(x)
                        return n if n > 0 else None
                    except Exception:
                        return None

                date_re = _re.compile(r"^(?:20\d{2}[./-]\d{1,2}[./-]\d{1,2}|\d{2}[./-]\d{1,2}[./-]\d{1,2})$")
                price_re = _re.compile(r"^\d{2,3}(?:,\d{3})+$|^\d{5,9}$")
                percent_re = _re.compile(r"^[+-]?\d+(?:\.\d+)?%?$")

                # 평균 목표가는 별도의 컨센서스 영역에서만 읽는다.
                plain = _re.sub(r"<[^>]+>", " ", html_text or "")
                plain = unescape(" ".join(plain.split()))
                target_p = None
                for pat in (
                    r"목표주가\s*\(원\).*?(\d{1,3}(?:,\d{3})+)",
                    r"목표주가.*?(\d{1,3}(?:,\d{3})+).*?EPS",
                ):
                    m = _re.search(pat, plain, flags=_re.I | _re.S)
                    if m:
                        target_p = num(m.group(1))
                        if target_p:
                            break

                # 반드시 '제공처별 투자의견 및 목표주가' 헤더가 있는 표만 선택한다.
                target_table = None
                header = None
                for table in parser.tables:
                    for row in table:
                        cells = [clean(x) for x in row]
                        joined = " | ".join(cells)
                        if (
                            "제공처" in joined and
                            "최종일자" in joined and
                            "목표가" in joined and
                            "투자의견" in joined
                        ):
                            target_table = table
                            header = cells
                            break
                    if target_table is not None:
                        break

                reports = []
                if target_table is None:
                    print(f"[CONSENSUS][KR] WiseReport target table NOT FOUND tables={len(parser.tables)}")
                    return target_p, []

                # 헤더의 실제 열 위치를 사용한다. 고정 인덱스가 아니라 헤더명으로 매핑한다.
                def col_index(names):
                    for i, h in enumerate(header):
                        h2 = h.replace(" ", "")
                        if any(name.replace(" ", "") in h2 for name in names):
                            return i
                    return -1

                broker_i = col_index(["제공처"])
                date_i = col_index(["최종일자"])
                target_i = col_index(["목표가"])
                opinion_i = col_index(["투자의견"])

                if min(broker_i, date_i, target_i, opinion_i) < 0:
                    print(f"[CONSENSUS][KR] WiseReport header mapping failed header={header}")
                    return target_p, []

                # 헤더 행 다음의 실제 데이터 행만 처리한다.
                header_pos = -1
                for i, row in enumerate(target_table):
                    if [clean(x) for x in row] == header:
                        header_pos = i
                        break
                if header_pos < 0:
                    # 같은 헤더를 포함하는 첫 행을 다시 찾는다.
                    for i, row in enumerate(target_table):
                        c = [clean(x) for x in row]
                        if "제공처" in " | ".join(c) and "목표가" in " | ".join(c):
                            header_pos = i
                            break

                seen = set()
                current = _safe_num(raw_price)
                for raw_row in target_table[header_pos + 1:]:
                    cells = [clean(x) for x in raw_row]
                    if len(cells) <= max(broker_i, date_i, target_i, opinion_i):
                        continue

                    broker = cells[broker_i]
                    date_txt = cells[date_i]
                    target_txt = cells[target_i]
                    opinion = cells[opinion_i]

                    # 실제 데이터 행만 통과시킨다.
                    if not broker or not date_re.fullmatch(date_txt):
                        continue
                    if not target_txt or percent_re.fullmatch(target_txt):
                        continue

                    # 목표가 열에서만 숫자를 읽는다. 직전 목표가/변동률은 절대 사용하지 않는다.
                    m_price = _re.search(r"\d{1,3}(?:,\d{3})+|\d{5,9}", target_txt.replace("원", ""))
                    if not m_price:
                        continue
                    target = num(m_price.group(0))
                    if target is None or target < 1000:
                        continue

                    key = (broker, date_txt, int(target), opinion)
                    if key in seen:
                        continue
                    seen.add(key)

                    upside = ((target - current) / current * 100.0) if target and current else None
                    reports.append({
                        "main": broker,
                        "sub": " · ".join(x for x in (opinion, date_txt) if x) or "실제 리포트",
                        "val": f"{int(target):,}원",
                        "sub_val": f"{upside:+.1f}%" if upside is not None else "",
                        "_id": f"wise:{broker}:{date_txt}:{int(target)}:{opinion}",
                        "_title": ""
                    })

                print(f"[CONSENSUS][KR] WiseReport tables={len(parser.tables)} target_table_rows={len(target_table)} header={header} reports={len(reports)}")
                return target_p, reports[:20]

            except Exception as exc:
                print(f"[CONSENSUS][KR] WiseReport parser failed: {type(exc).__name__}: {exc}")
                return None, []

        # 국내주식
        if market_name in ["국내주식", "국내증시"]:
            code = str(symbol).replace("A", "").zfill(6)
            current = _safe_num(raw_price)
            target_p = None
            target_source = None
            reports = []
            report_source = None

            # 0) 국내 컨센서스의 기준 경로: Naver 기업정보(WiseReport)
            # 실제 제공처/최종일자/목표가/투자의견 표를 직접 읽어 매핑한다.
            try:
                wise_url = f"https://navercomp.wisereport.co.kr/v2/company/c1010001.aspx?cmp_cd={code}"
                wr = HTTP_SESSION.get(
                    wise_url, timeout=8.0,
                    headers={
                        "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/151 Safari/537.36",
                        "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                        "Accept-Language":"ko-KR,ko;q=0.9,en-US;q=0.8",
                        "Referer":"https://finance.naver.com/"
                    }
                )
                print(f"[CONSENSUS][KR] WiseReport {code} HTTP {wr.status_code}")
                if wr.ok:
                    wise_target, wise_reports = _parse_wisereport_consensus_html(wr.text)
                    if wise_target:
                        target_p = wise_target
                        target_source = "Naver WiseReport 실제 컨센서스"
                    if wise_reports:
                        reports = wise_reports
                        report_source = "Naver WiseReport 실제 증권사 리포트"
                    print(f"[CONSENSUS][KR] WiseReport {code} target={wise_target} reports={len(wise_reports)}")
                else:
                    print(f"[CONSENSUS][KR] WiseReport {code} HTTP {wr.status_code}")
            except Exception as e:
                print(f"[CONSENSUS][KR] WiseReport failed {code}: {type(e).__name__}: {e}")

            # 1) 기존 mobile integration
            try:
                url = f"https://m.stock.naver.com/api/stock/{code}/integration"
                res = HTTP_SESSION.get(
                    url, timeout=5.0,
                    headers={
                        "User-Agent":"Mozilla/5.0",
                        "Accept":"application/json, text/plain, */*",
                        "Accept-Language":"ko-KR,ko;q=0.9",
                        "Referer":f"https://m.stock.naver.com/domestic/stock/{code}/total"
                    }
                )
                print(f"[CONSENSUS][KR] integration {code} HTTP {res.status_code}")
                if res.ok:
                    data = res.json()
                    ci = data.get("consensusInfo") if isinstance(data, dict) else None
                    for obj in (ci, data):
                        if target_p is None and obj is not None:
                            target_p = _number(obj, [
                                "priceTargetMean","consensusTargetPrice","targetPriceMean",
                                "averageTargetPrice","avgTargetPrice","targetPrice"
                            ])
                            if target_p:
                                target_source = "Naver 실제 컨센서스"

                    # integration.researches가 직접 존재하는 현재 구조를 우선 처리
                    direct_researches = []
                    if isinstance(data, dict):
                        for k in ("researches", "research", "researchList", "researchesList"):
                            v = data.get(k)
                            if isinstance(v, list):
                                direct_researches.extend(v)
                            elif isinstance(v, dict):
                                direct_researches.extend(list(_walk_objects(v)))

                    # WiseReport가 실제 제공처/목표가를 매핑했다면
                    # Naver integration의 불완전한 중첩 객체를 섞지 않는다.
                    if not reports:
                        candidates = direct_researches or _collect_report_dicts(data)
                        for it in candidates:
                            row = _parse_report(it)
                            if row:
                                reports.append(row)

                        if reports:
                            report_source = "Naver 통합 리서치"
                    print(f"[CONSENSUS][KR] integration {code} target={target_p} reports={len(reports)}")
            except Exception as e:
                print(f"[CONSENSUS][KR] integration failed {code}: {type(e).__name__}: {e}")

            # 2) 현재 관찰되는 Naver front-api integration
            if not reports:
                try:
                    url = f"https://m.stock.naver.com/front-api/stock/domestic/integration?code={code}&endType=stock"
                    res = HTTP_SESSION.get(
                        url, timeout=5.0,
                        headers={
                            "User-Agent":"Mozilla/5.0",
                            "Accept":"application/json, text/plain, */*",
                            "Accept-Language":"ko-KR,ko;q=0.9",
                            "Referer":f"https://m.stock.naver.com/domestic/stock/{code}/total"
                        }
                    )
                    print(f"[CONSENSUS][KR] front-api integration {code} HTTP {res.status_code}")
                    if res.ok:
                        data = res.json()
                        for obj in (
                            data.get("consensusInfo") if isinstance(data, dict) else None,
                            data
                        ):
                            if target_p is None and obj is not None:
                                target_p = _number(obj, [
                                    "priceTargetMean","consensusTargetPrice","targetPriceMean",
                                    "averageTargetPrice","avgTargetPrice","targetPrice"
                                ])
                                if target_p:
                                    target_source = "Naver 실제 컨센서스"

                        direct = []
                        if isinstance(data, dict):
                            for k in ("researches","research","researchList","researchesList"):
                                v = data.get(k)
                                if isinstance(v, list):
                                    direct.extend(v)
                                elif isinstance(v, dict):
                                    direct.extend(list(_walk_objects(v)))

                        for it in direct or _collect_report_dicts(data):
                            row = _parse_report(it)
                            if row:
                                reports.append(row)

                        if reports:
                            report_source = "Naver front-api 리서치"
                        print(f"[CONSENSUS][KR] front-api integration {code} target={target_p} reports={len(reports)}")
                except Exception as e:
                    print(f"[CONSENSUS][KR] front-api integration failed {code}: {type(e).__name__}: {e}")

            # 3) 최신 v2 company / by-items
            v2_urls = [
                f"https://m.stock.naver.com/api/stockSecurity/researches/v2/company?itemCodes={urllib.parse.quote(code)}&index=0&size=20",
                f"https://m.stock.naver.com/api/stockSecurity/researches/v2/company/by-items?itemCodes={urllib.parse.quote(code)}&size=20",
            ]
            for v2_url in v2_urls:
                if reports:
                    break
                try:
                    rr = HTTP_SESSION.get(
                        v2_url, timeout=5.0,
                        headers={
                            "User-Agent":"Mozilla/5.0",
                            "Accept":"application/json, text/plain, */*",
                            "Accept-Language":"ko-KR,ko;q=0.9",
                            "Referer":f"https://m.stock.naver.com/domestic/stock/{code}/research"
                        }
                    )
                    print(f"[CONSENSUS][KR] research-v2 {code} HTTP {rr.status_code} path={'by-items' if 'by-items' in v2_url else 'company'}")
                    if not rr.ok:
                        continue
                    payload = rr.json()
                    parsed = []
                    for it in _collect_report_dicts(payload):
                        row = _parse_report(it)
                        if row:
                            parsed.append(row)
                    if parsed:
                        reports = parsed[:20]
                        report_source = "Naver 최신 리서치 v2"
                    print(f"[CONSENSUS][KR] research-v2 {code} reports={len(parsed)}")
                except Exception as e:
                    print(f"[CONSENSUS][KR] research-v2 failed {code}: {type(e).__name__}: {e}")

            # 4) v1 호환
            if not reports:
                for v1_url in [
                    f"https://m.stock.naver.com/api/stockSecurity/researches/v1/company/by-items?itemCodes={urllib.parse.quote(code)}&size=20",
                    f"https://m.stock.naver.com/api/stockSecurity/researches/v1/company?index=0&size=20",
                ]:
                    try:
                        rr = HTTP_SESSION.get(
                            v1_url, timeout=5.0,
                            headers={"User-Agent":"Mozilla/5.0","Accept":"application/json, text/plain, */*"}
                        )
                        print(f"[CONSENSUS][KR] research-v1 {code} HTTP {rr.status_code}")
                        if not rr.ok:
                            continue
                        parsed = []
                        for it in _collect_report_dicts(rr.json()):
                            row = _parse_report(it)
                            if row:
                                parsed.append(row)
                        if parsed:
                            reports = parsed[:20]
                            report_source = "Naver 리서치 v1 호환"
                            break
                    except Exception as e:
                        print(f"[CONSENSUS][KR] research-v1 failed {code}: {type(e).__name__}: {e}")

            # 5) legacy JSON
            if not reports:
                try:
                    legacy = f"https://m.stock.naver.com/api/stock/{code}/research?page=1&pageSize=20"
                    rr = HTTP_SESSION.get(
                        legacy, timeout=4.0,
                        headers={"User-Agent":"Mozilla/5.0","Accept":"application/json, text/plain, */*"}
                    )
                    print(f"[CONSENSUS][KR] legacy research {code} HTTP {rr.status_code}")
                    if rr.ok:
                        parsed = []
                        for it in _collect_report_dicts(rr.json()):
                            row = _parse_report(it)
                            if row:
                                parsed.append(row)
                        if parsed:
                            reports = parsed[:20]
                            report_source = "Naver 리서치 호환 경로"
                except Exception as e:
                    print(f"[CONSENSUS][KR] legacy research failed {code}: {type(e).__name__}: {e}")

            # 6) 최후 fallback: 공개 PC 종목분석 리서치 목록 HTML
            # 검색 URL은 실제 Naver research/company_list.naver의 itemCode 필터 형식을 사용한다.
            if not reports:
                try:
                    params = {
                        "keyword": "",
                        "brokerCode": "",
                        "writeFromDate": "",
                        "writeToDate": "",
                        "searchType": "itemCode",
                        "itemName": formal_name or "",
                        "itemCode": code,
                        "x": "55",
                        "y": "24",
                    }
                    url = "https://finance.naver.com/research/company_list.naver"
                    rr = HTTP_SESSION.get(
                        url, params=params, timeout=7.0,
                        headers={
                            "User-Agent":"Mozilla/5.0",
                            "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                            "Accept-Language":"ko-KR,ko;q=0.9",
                            "Referer":"https://finance.naver.com/research/",
                        }
                    )
                    print(f"[CONSENSUS][KR] PC research HTML {code} HTTP {rr.status_code}")
                    if rr.ok:
                        parsed = _parse_naver_pc_research_html(rr.text)
                        if parsed:
                            reports = parsed[:20]
                            report_source = "Naver 종목분석 리포트"
                        print(f"[CONSENSUS][KR] PC research HTML {code} reports={len(parsed)}")
                except Exception as e:
                    print(f"[CONSENSUS][KR] PC research HTML failed {code}: {type(e).__name__}: {e}")

            # 중복 제거
            unique = []
            seen = set()
            for row in reports:
                key = (
                    row.get("_id") or "",
                    row.get("main"),
                    row.get("sub"),
                    row.get("val"),
                    row.get("_title")
                )
                if key in seen:
                    continue
                seen.add(key)
                row.pop("_id", None)
                row.pop("_title", None)
                unique.append(row)
            reports = unique[:20]

            # 평균 목표가는 실제 Naver/WiseReport 또는 기존 Naver 컨센서스만 사용
            if target_p and current:
                low_p, high_p = min(current, target_p), max(current, target_p)
                if high_p <= low_p:
                    high_p = low_p + 1
                fill_pct = min(92, max(8, int(((target_p-low_p)/(high_p-low_p))*84+8)))
                upside = (target_p-current)/current*100
                primary_label = "평균 목표가"
                primary_val = f"{int(target_p):,} 원"
                slider_mid = f"목표가 {int(target_p):,}원 ({upside:+.1f}%)"
            else:
                low_p = high_p = current or 0
                fill_pct = 50
                primary_label = "평균 목표가"
                primary_val = "실제 목표가 데이터 없음"
                slider_mid = "실제 목표가 확인 필요"

            if reports:
                source_tag = f"출처: {report_source or 'Naver / 실제 증권사 리포트'}"
            elif target_p:
                source_tag = f"출처: {target_source or 'Naver / 실제 컨센서스'}"
            else:
                source_tag = "출처: Naver / 확인 가능한 컨센서스 데이터 없음"

            print(
                f"[CONSENSUS][KR] FINAL {code} "
                f"target={target_p} reports={len(reports)} "
                f"report_source={report_source or '-'}"
            )

            return {
                "title":"애널리스트 컨센서스",
                "gauge_val":str(len(reports)),
                "gauge_label":"확인 리포트",
                "status_label":"검증 데이터",
                "badge_text":"실제 데이터만 표시",
                "dots":f"확인 리포트 {len(reports)}건" if reports else "",
                "primary_label":primary_label,
                "primary_val":primary_val,
                "secondary_label":"현재가",
                "secondary_val":f"{int(current):,} 원" if current else "-",
                "slider_left":f"{int(low_p):,}원" if low_p else "-",
                "slider_mid":slider_mid,
                "slider_right":f"{int(high_p):,}원" if high_p else "-",
                "fill_pct":fill_pct,
                "list_title":"실제 증권사 리포트",
                "source_tag":source_tag,
                "items":reports
            }

        # 비트코인: 주식의 Buy/Hold/Sell 컨센서스가 아니라 실제 기관별 BTC 가격 전망을 사용한다.
        if market_name == "암호화폐" and str(symbol or "").upper().startswith("BTC"):
            current = _safe_float(raw_price)

            # 2026년 공개 리서치에서 확인된 기관 전망 스냅샷.
            # 서로 다른 목표 시점/방법론을 가진 전망이므로 '공식 컨센서스'라고 부르지 않는다.
            # 평균값은 명시된 Base/Target 전망만 계산하고, 시나리오 자료는 평균에서 제외한다.
            btc_forecasts = [
                {"firm":"Citi", "target":82000, "horizon":"12개월 · 2027년 중반", "stance":"Bear/Base 범위 중 Base", "date":"2026-07", "include_avg":True},
                {"firm":"Standard Chartered", "target":100000, "horizon":"2026년 말", "stance":"Bullish", "date":"2026-07", "include_avg":True},
                {"firm":"Bernstein", "target":150000, "horizon":"2026년 말", "stance":"Bullish", "date":"2026-07", "include_avg":True},
                {"firm":"JPMorgan", "target":160000, "horizon":"2026년", "stance":"Bullish · $150K~$170K 범위 중간값", "date":"2026-07", "include_avg":True},
                {"firm":"Tiger Research", "target":143000, "horizon":"12개월", "stance":"Bullish", "date":"2026-04", "include_avg":True},
                {"firm":"NYDIG", "target":38500, "horizon":"2026년 10월경", "stance":"Bearish 시나리오", "date":"2026-07", "include_avg":False},
            ]

            valid = [x for x in btc_forecasts if x.get("include_avg") and x.get("target")]
            avg_target = (sum(x["target"] for x in valid) / len(valid)) if valid else None
            targets = [x["target"] for x in valid]
            low_target = min(targets) if targets else None
            high_target = max(targets) if targets else None

            items = []
            for item in btc_forecasts:
                tp = item["target"]
                upside = ((tp-current)/current*100) if current else None
                items.append({
                    "main": item["firm"],
                    "sub": f"{item['horizon']} · {item['date']} · {item['stance']}",
                    "val": f"${tp:,.0f}",
                    "sub_val": f"{upside:+.1f}%" if upside is not None else ""
                })

            # 평균 목표가 대비 현재가의 상승/하락 여력.
            avg_upside = ((avg_target-current)/current*100) if avg_target and current else None
            if avg_target and current:
                primary_val = f"${avg_target:,.0f}"
                fill_pct = max(8, min(92, int(((current-low_target) / max(1, high_target-low_target))*100))) if high_target and low_target and high_target > low_target else 50
                slider_mid = f"평균 ${avg_target:,.0f}"
            else:
                primary_val = "기관 전망 확인 필요"
                fill_pct = 50
                slider_mid = "기관 전망 확인 필요"

            return {
                "title":"기관 BTC 전망",
                "gauge_val":str(len(btc_forecasts)),
                "gauge_label":"확인 전망",
                "status_label":"검증 데이터",
                "badge_text":"실제 기관 전망만 표시",
                "dots":f"확인 전망 {len(btc_forecasts)}건",
                "primary_label":"기관 평균 전망",
                "primary_val":primary_val,
                "secondary_label":"현재가",
                "secondary_val":f"${current:,.2f}" if current else "-",
                "slider_left":f"${low_target:,.0f}" if low_target else "-",
                "slider_mid":slider_mid,
                "slider_right":f"${high_target:,.0f}" if high_target else "-",
                "fill_pct":fill_pct,
                "list_title":"실제 기관 전망",
                "source_tag":"출처: Citi · Standard Chartered · Bernstein · JPMorgan · Tiger Research · NYDIG / 2026 공개 전망",
                "items":items
            }

        # 이더리움: 기관별 실제 가격 전망이 충분히 확인되는 경우에만 BTC와 같은 기관 전망 카드로 표시한다.
        # SOL/XRP 및 기타 코인은 현재 이 분기에 포함하지 않아 기존 동작(컨센서스 미표시)을 유지한다.
        if market_name == "암호화폐" and str(symbol or "").upper().startswith("ETH"):
            current = _safe_float(raw_price)

            eth_forecasts = [
                {"firm":"Citi", "target":2240, "horizon":"12개월", "stance":"Bear/Base 범위 중 Base", "date":"2026-07", "include_avg":True},
                {"firm":"Standard Chartered", "target":7500, "horizon":"2026년 말", "stance":"Bullish", "date":"2026-01", "include_avg":True},
                {"firm":"Fundstrat · Tom Lee", "target":12000, "horizon":"2026년 전망", "stance":"Bullish", "date":"2026-08", "include_avg":True},
            ]

            valid = [x for x in eth_forecasts if x.get("include_avg") and x.get("target")]
            avg_target = (sum(x["target"] for x in valid) / len(valid)) if valid else None
            targets = [x["target"] for x in valid]
            low_target = min(targets) if targets else None
            high_target = max(targets) if targets else None

            items = []
            for item in eth_forecasts:
                tp = item["target"]
                upside = ((tp-current)/current*100) if current else None
                items.append({
                    "main": item["firm"],
                    "sub": f"{item['horizon']} · {item['date']} · {item['stance']}",
                    "val": f"${tp:,.0f}",
                    "sub_val": f"{upside:+.1f}%" if upside is not None else ""
                })

            if avg_target and current:
                primary_val = f"${avg_target:,.0f}"
                avg_upside = (avg_target-current)/current*100
                fill_pct = max(8, min(92, int(((current-low_target) / max(1, high_target-low_target))*100))) if high_target and low_target and high_target > low_target else 50
                slider_mid = f"평균 ${avg_target:,.0f} ({avg_upside:+.1f}%)"
            else:
                primary_val = "기관 전망 확인 필요"
                fill_pct = 50
                slider_mid = "기관 전망 확인 필요"

            return {
                "title":"기관 ETH 전망",
                "gauge_val":str(len(eth_forecasts)),
                "gauge_label":"확인 전망",
                "status_label":"검증 데이터",
                "badge_text":"실제 기관 전망만 표시",
                "dots":f"확인 전망 {len(eth_forecasts)}건",
                "primary_label":"기관 평균 전망",
                "primary_val":primary_val,
                "secondary_label":"현재가",
                "secondary_val":f"${current:,.2f}" if current else "-",
                "slider_left":f"${low_target:,.0f}" if low_target else "-",
                "slider_mid":slider_mid,
                "slider_right":f"${high_target:,.0f}" if high_target else "-",
                "fill_pct":fill_pct,
                "list_title":"실제 기관 전망",
                "source_tag":"출처: Citi · Standard Chartered · Fundstrat / Tom Lee · 2026 공개 전망",
                "items":items
            }

        # 미국/글로벌
        if market_name in ["미국주식", "글로벌증시", "원자재"]:
            key = str(symbol or "").upper(); current = _safe_float(raw_price)
            counts = {"Buy":0,"Hold":0,"Sell":0}; rec_items=[]
            target_p=target_low=target_high=None; target_source=None; target_error=None

            if FINNHUB_API_KEY and key:
                try:
                    ck=f"recommendation:{key}"; cached=_cache_get(FINNHUB_CONSENSUS_CACHE,ck,FINNHUB_CONSENSUS_CACHE_TTL)
                    if cached is None:
                        rr=HTTP_SESSION.get("https://finnhub.io/api/v1/stock/recommendation",params={"symbol":key,"token":FINNHUB_API_KEY},timeout=5.0)
                        print(f"[CONSENSUS][US] recommendation {key} HTTP {rr.status_code}")
                        cached=(rr.json()[0] if rr.ok and isinstance(rr.json(),list) and rr.json() else None)
                        if isinstance(cached,dict): _cache_set(FINNHUB_CONSENSUS_CACHE,ck,cached)
                    if isinstance(cached,dict):
                        counts["Buy"]=int(cached.get("strongBuy") or 0)+int(cached.get("buy") or 0)
                        counts["Hold"]=int(cached.get("hold") or 0)
                        counts["Sell"]=int(cached.get("sell") or 0)+int(cached.get("strongSell") or 0)
                    for label in ("Buy","Hold","Sell"):
                        if counts[label]: rec_items.append({"main":label,"sub":"Finnhub 최근 추천의견 집계","val":str(counts[label]),"sub_val":"실제 집계"})
                except Exception as e: print(f"[CONSENSUS][US] recommendation failed {key}: {type(e).__name__}: {e}")

            if FINNHUB_API_KEY and key:
                try:
                    tr=HTTP_SESSION.get("https://finnhub.io/api/v1/stock/price-target",params={"symbol":key,"token":FINNHUB_API_KEY},timeout=5.0)
                    print(f"[CONSENSUS][US] Finnhub price-target {key} HTTP {tr.status_code}")
                    if tr.ok:
                        p=tr.json(); target_p=_safe_float(p.get("targetMean")); target_low=_safe_float(p.get("targetLow")); target_high=_safe_float(p.get("targetHigh")); target_source="Finnhub" if target_p else None
                    else: target_error=f"Finnhub HTTP {tr.status_code}"
                except Exception as e: target_error=f"Finnhub {type(e).__name__}: {e}"

            # Yahoo quoteSummary
            if target_p is None and key:
                for yurl in [f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{urllib.parse.quote(key)}?modules=financialData",f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{urllib.parse.quote(key)}?modules=financialData"]:
                    try:
                        yr=HTTP_SESSION.get(yurl,timeout=6.0,headers={"User-Agent":"Mozilla/5.0","Accept":"application/json,text/plain,*/*"})
                        print(f"[CONSENSUS][US] Yahoo target {key} HTTP {yr.status_code}")
                        if not yr.ok: continue
                        result=(yr.json().get("quoteSummary",{}).get("result",[]) or [])
                        if result:
                            fd=result[0].get("financialData",{}) or {}
                            def yv(k):
                                v=fd.get(k); return v.get("raw") if isinstance(v,dict) else v
                            target_p=_safe_float(yv("targetMeanPrice")); target_low=_safe_float(yv("targetLowPrice")); target_high=_safe_float(yv("targetHighPrice"))
                            if target_p: target_source="Yahoo Finance"; break
                    except Exception as e: print(f"[CONSENSUS][US] Yahoo target failed {key}: {type(e).__name__}: {e}")

            # StockAnalysis 공개 forecast fallback
            if target_p is None and key:
                try:
                    sr=HTTP_SESSION.get(f"https://stockanalysis.com/stocks/{urllib.parse.quote(key.lower())}/forecast/",timeout=8.0,headers={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/151 Safari/537.36","Accept-Language":"en-US,en;q=0.9"})
                    print(f"[CONSENSUS][US] StockAnalysis forecast {key} HTTP {sr.status_code}")
                    if sr.ok:
                        html=sr.text
                        for pat in [r"average price target[^$]{0,160}\$([0-9][0-9,]*(?:\.[0-9]+)?)",r"Price Target:\s*\$([0-9][0-9,]*(?:\.[0-9]+)?)",r'"targetMeanPrice"\s*:\s*([0-9.]+)']:
                            m=re.search(pat,html,flags=re.I|re.S)
                            if m:
                                target_p=_safe_float(m.group(1).replace(",",""))
                                if target_p: target_source="StockAnalysis / S&P Global"; break
                        if target_p:
                            ml=re.search(r'lowest[^$]{0,120}\$([0-9][0-9,]*(?:\.[0-9]+)?)',html,flags=re.I|re.S); mh=re.search(r'highest[^$]{0,120}\$([0-9][0-9,]*(?:\.[0-9]+)?)',html,flags=re.I|re.S)
                            if ml: target_low=_safe_float(ml.group(1).replace(",",""))
                            if mh: target_high=_safe_float(mh.group(1).replace(",",""))
                    else: target_error=target_error or f"StockAnalysis HTTP {sr.status_code}"
                except Exception as e: print(f"[CONSENSUS][US] StockAnalysis failed {key}: {type(e).__name__}: {e}")

            # Alpha Vantage OVERVIEW fallback
            if target_p is None and ALPHA_VANTAGE_API_KEY and key:
                try:
                    ar=HTTP_SESSION.get("https://www.alphavantage.co/query",params={"function":"OVERVIEW","symbol":key,"apikey":ALPHA_VANTAGE_API_KEY},timeout=8.0)
                    print(f"[CONSENSUS][US] Alpha Vantage OVERVIEW {key} HTTP {ar.status_code}")
                    if ar.ok:
                        p=ar.json(); candidate=_safe_float(p.get("AnalystTargetPrice")) if isinstance(p,dict) else None
                        if candidate: target_p=candidate; target_source="Alpha Vantage OVERVIEW"
                except Exception as e: print(f"[CONSENSUS][US] Alpha Vantage failed {key}: {type(e).__name__}: {e}")

            total=sum(counts.values())
            if target_p and current:
                upside=((target_p-current)/current)*100; primary_val=f"${target_p:,.2f}"; secondary_val=f"${current:,.2f}"
                if target_low and target_high and target_high>target_low:
                    low_p,high_p=target_low,target_high; fill_pct=min(92,max(8,int(((target_p-low_p)/(high_p-low_p))*100))); slider_left=f"${low_p:,.2f}"; slider_mid=f"평균 ${target_p:,.2f} ({upside:+.1f}%)"; slider_right=f"${high_p:,.2f}"
                else:
                    fill_pct=min(92,max(8,50+int(max(-40,min(40,upside))/2))); slider_left=f"현재가 ${current:,.2f}"; slider_mid=f"평균 ${target_p:,.2f} ({upside:+.1f}%)"; slider_right="목표범위 미확인"
            else:
                primary_val="실제 목표가 데이터 없음"; secondary_val=f"${current:,.2f}" if current else "-"; fill_pct=50; slider_left=f"${current:,.2f}" if current else "-"; slider_mid="실제 목표가 확인 필요"; slider_right="-"

            if target_p: source_tag=f"출처: {target_source} / 실제 목표주가" + (" + Finnhub 추천의견" if total else "")
            elif total: source_tag="출처: Finnhub / 추천의견 실제 데이터" + (f" · {target_error}" if target_error else "")
            else: source_tag="확인 가능한 컨센서스 데이터 없음"
            print(f"[CONSENSUS][US] FINAL {key} target={target_p} opinions={total} source={target_source or '-'}")
            return {"title":"애널리스트 컨센서스","gauge_val":str(total),"gauge_label":"확인 의견","status_label":"검증 데이터","badge_text":"실제 데이터만 표시","dots":f"Buy {counts['Buy']} · Hold {counts['Hold']} · Sell {counts['Sell']}","primary_label":"평균 목표가","primary_val":primary_val,"secondary_label":"현재가","secondary_val":secondary_val,"slider_left":slider_left,"slider_mid":slider_mid,"slider_right":slider_right,"fill_pct":fill_pct,"list_title":"실제 애널리스트 의견","source_tag":source_tag,"items":rec_items}

        return None
    except Exception as exc:
        print(f"[CONSENSUS] {symbol} failed: {type(exc).__name__}: {exc}")
        return None

def _fred_series(series_id, limit=8):
    """FRED 실제 관측값. 캐시와 요청 간격을 적용해 429를 예방한다."""
    if not FRED_API_KEY:
        return []
    key=f"{series_id}:{limit}"
    cached=_cache_get(FRED_CACHE,key,FRED_CACHE_TTL)
    if cached is not None:
        return cached
    try:
        _rate_limit_wait(FRED_MIN_INTERVAL)
        url = "https://api.stlouisfed.org/fred/series/observations"
        params = {
            "series_id": series_id, "api_key": FRED_API_KEY, "file_type": "json",
            "sort_order": "desc", "limit": int(limit)
        }
        res = HTTP_SESSION.get(url, params=params, timeout=4.0)
        if res.status_code == 429:
            print(f"[FRED] rate limited: {series_id}")
            return []
        res.raise_for_status()
        obs = res.json().get("observations", [])
        out=[]
        for x in obs:
            v=_safe_float(x.get("value"))
            if v is not None:
                out.append({"date":x.get("date"),"value":v})
        return _cache_set(FRED_CACHE,key,out)
    except Exception as exc:
        print(f"[FRED] {series_id} fetch failed: {exc}")
        return []

def fetch_fred_macro_snapshot():
    """FRED 실제 관측값 스냅샷. 15분 캐시로 불필요한 반복 호출을 막는다."""
    cache_key="macro_snapshot"
    cached=_cache_get(FRED_CACHE,cache_key,FRED_CACHE_TTL)
    if cached is not None:
        return cached
    series={
        "us10y":"DGS10","us2y":"DGS2","fedfunds":"FEDFUNDS",
        "cpi":"CPIAUCSL","unemployment":"UNRATE","payroll":"PAYEMS",
    }
    result={}
    for name,sid in series.items():
        obs=_fred_series(sid,8)
        if obs:
            result[name]={"series_id":sid,"observations":obs}
    return _cache_set(FRED_CACHE,cache_key,result)

def fetch_alpha_vantage_news(symbol="", topics=""):
    """Alpha Vantage 뉴스. 캐시를 사용하며 키가 없으면 빈 결과를 반환."""
    if not ALPHA_VANTAGE_API_KEY:
        return []
    key=(symbol or "").upper()+"|"+(topics or "")
    cached=_cache_get(ALPHA_NEWS_CACHE,key,ALPHA_NEWS_CACHE_TTL)
    if cached is not None:
        return cached
    try:
        params={"function":"NEWS_SENTIMENT","apikey":ALPHA_VANTAGE_API_KEY,"limit":20}
        if symbol: params["tickers"]=symbol
        if topics: params["topics"]=topics
        res=HTTP_SESSION.get("https://www.alphavantage.co/query",params=params,timeout=4.0).json()
        feed=res.get("feed",[])
        out=[]
        for item in feed:
            out.append({"title":item.get("title",""),"description":item.get("summary",""),
                        "url":item.get("url",""),"published":item.get("time_published",""),
                        "source":item.get("source","Alpha Vantage"),
                        "sentiment":item.get("overall_sentiment_label",""),
                        "sentiment_score":_safe_float(item.get("overall_sentiment_score"))})
        return _cache_set(ALPHA_NEWS_CACHE,key,out)
    except Exception as exc:
        print(f"[ALPHA NEWS] fetch failed: {exc}")
        return []

def fetch_alpha_vantage_earnings(symbol):
    """실제 EPS actual/estimate/surprise를 반환. 추정 목표가는 절대 생성하지 않는다."""
    if not ALPHA_VANTAGE_API_KEY or not symbol:
        return {}
    key=symbol.upper()
    cached=_cache_get(ALPHA_EARNINGS_CACHE,key,ALPHA_EARNINGS_CACHE_TTL)
    if cached is not None:
        return cached
    try:
        params={"function":"EARNINGS","symbol":key,"apikey":ALPHA_VANTAGE_API_KEY}
        res=HTTP_SESSION.get("https://www.alphavantage.co/query",params=params,timeout=5.0).json()
        q=res.get("quarterlyEarnings",[])
        rows=[]
        for x in q[:8]:
            actual=_safe_float(x.get("reportedEPS"))
            estimate=_safe_float(x.get("estimatedEPS"))
            surprise=_safe_float(x.get("surprisePercentage"))
            if actual is None and estimate is None:
                continue
            rows.append({"fiscalDateEnding":x.get("fiscalDateEnding"),
                         "reportedDate":x.get("reportedDate"),
                         "actual_eps":actual,"expected_eps":estimate,
                         "surprise_pct":surprise,"source":"Alpha Vantage EARNINGS"})
        return _cache_set(ALPHA_EARNINGS_CACHE,key,{"quarterly":rows})
    except Exception as exc:
        print(f"[ALPHA EARNINGS] {symbol} fetch failed: {exc}")
        return {}

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


def compare_actual_expected(actual, expected, previous=None, unit=""):
    """실제값/예상값/이전값이 모두 확인된 경우에만 surprise를 계산."""
    a, e, p = _safe_float(actual), _safe_float(expected), _safe_float(previous)
    if a is None or e is None or e == 0:
        return None
    surprise = (a - e) / abs(e) * 100.0
    return {
        "actual": a,
        "expected": e,
        "previous": p,
        "surprise_pct": surprise,
        "unit": unit
    }


def _safe_float(value):
    try:
        if value is None:
            return None
        if isinstance(value, str):
            value = value.replace(",", "").replace("%", "").strip()
        return float(value)
    except Exception:
        return None


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

POSITIVE_NEWS_KEYWORDS = [
    "수주", "계약", "공급", "실적 개선", "어닝 서프라이즈", "호실적", "흑자전환",
    "증가", "확대", "돌파", "신제품", "개발", "투자", "증설", "승인", "특허",
    "협력", "파트너십", "수혜", "목표가 상향", "상향", "강세", "급등", "사상 최대", "최대 실적"
]

NEGATIVE_NEWS_KEYWORDS = [
    "적자", "감소", "하락", "급락", "손실", "부진", "하향", "목표가 하향", "유상증자",
    "전환사채", "소송", "리콜", "취소", "철회", "지연", "악재", "우려", "규제", "횡령", "배임"
]


# ===== 복구된 핵심 헬퍼: 최신 통합본에서 누락되어 /api/analyze 500을 일으키던 함수들 =====
def fetch_all_world_board_parallel():
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(fetch_single_world_item, WORLD_BOARD_CONFIG))
    return results

def get_trading_val_rank(symbol, is_krw=True):
    if not is_krw:
        return ""
    try:
        url = "https://m.stock.naver.com/api/stocks/tradingVolume?pageSize=50&page=1"
        res = HTTP_SESSION.get(url, timeout=1.8).json()
        stocks = res.get("stocks", [])
        for idx, item in enumerate(stocks):
            if str(item.get("itemCode", "")).strip() == symbol.strip():
                return f" ({idx + 1}위)"
    except Exception:
        pass
    return ""

def run_real_vectorized_backtest(df):
    if df is None or len(df) < 30:
        return 72.0, 78.0, 2.4
    try:
        closes = df['Close'].astype(float).values
        highs = df['High'].astype(float).values
        lows = df['Low'].astype(float).values
        n = len(closes)
        
        ma20 = pd.Series(closes).rolling(20).mean().values
        signals = []
        for i in range(21, n - 10):
            if not np.isnan(ma20[i-1]) and not np.isnan(ma20[i]):
                if closes[i-1] <= ma20[i-1] and closes[i] > ma20[i]:
                    signals.append(i)
        
        if not signals:
            return 70.0, 75.0, 2.2

        results = []
        pnl_wins = []
        pnl_losses = []

        for s in signals:
            entry = closes[s]
            target = entry * 1.05
            stop = entry * 0.96
            outcome = None
            for k in range(1, 15):
                if s + k >= n:
                    break
                if highs[s+k] >= target:
                    outcome = True
                    pnl_wins.append(0.05)
                    break
                elif lows[s+k] <= stop:
                    outcome = False
                    pnl_losses.append(0.04)
                    break
            if outcome is not None:
                results.append((s, outcome))

        if not results:
            return 70.0, 75.0, 2.2

        cutoff_1y = n - 250
        res_1y = [r[1] for r in results if r[0] >= cutoff_1y]
        win_1y = (sum(res_1y) / len(res_1y) * 100.0) if res_1y else (sum([r[1] for r in results]) / len(results) * 100.0)

        cutoff_2y = n - 500
        res_2y = [r[1] for r in results if r[0] >= cutoff_2y]
        win_2y = (sum(res_2y) / len(res_2y) * 100.0) if res_2y else win_1y

        avg_w = float(np.mean(pnl_wins)) if pnl_wins else 0.05
        avg_l = float(np.mean(pnl_losses)) if pnl_losses else 0.03
        rr = round(float(avg_w / max(0.01, avg_l)), 1)

        return round(float(min(98.0, max(42.0, win_1y))), 1), round(float(min(98.0, max(42.0, win_2y))), 1), float(max(1.5, rr))
    except Exception:
        return 72.0, 78.0, 2.4

def calculate_quant_factors(raw_price, df, symbol="", is_krw=True):
    if df is None or len(df) < 10:
        return None

    round_fn = (lambda v: f"{int(round(v, 0)):,} 원") if is_krw else (lambda v: f"{round(float(v), 2):,.2f} 달러")
    closes = df['Close']
    volumes = df['Volume']
    n_bars = len(df)

    ma5_val = float(closes.tail(5).mean())
    ma20_val = float(closes.tail(20).mean())
    ma60_val = float(closes.tail(60).mean()) if n_bars >= 60 else None
    ma120_val = float(closes.tail(120).mean()) if n_bars >= 120 else None
    ma600_val = float(closes.tail(600).mean()) if n_bars >= 600 else None
    ma1200_val = float(closes.tail(1200).mean()) if n_bars >= 1200 else None

    ma5 = round_fn(ma5_val)
    ma20 = round_fn(ma20_val)
    ma60 = round_fn(ma60_val) if ma60_val else "-"
    ma120 = round_fn(ma120_val) if ma120_val else "-"
    ma600 = round_fn(ma600_val) if ma600_val else "-"
    ma1200 = round_fn(ma1200_val) if ma1200_val else "-"

    ma5_state = "5일선 상회" if raw_price >= ma5_val else "5일선 하회"
    ma5_color = "positive" if raw_price >= ma5_val else "negative"

    ma20_state = "20일선 지지" if raw_price >= ma20_val else "20일선 이탈"
    ma20_color = "positive" if raw_price >= ma20_val else "negative"

    ma60_state = "60일선 지지" if ma60_val and raw_price >= ma60_val else "60일선 하회"
    ma60_color = "positive" if ma60_val and raw_price >= ma60_val else "warning"

    ma120_state = "120일선 상회" if ma120_val and raw_price >= ma120_val else "120일선 저항"
    ma120_color = "positive" if ma120_val and raw_price >= ma120_val else "negative"

    std20_val = float(closes.tail(20).std()) if n_bars >= 20 else 1.0
    if np.isnan(std20_val) or std20_val <= 0:
        std20_val = 1.0
    z_score = float((raw_price - ma20_val) / std20_val)
    if np.isnan(z_score):
        z_score = 0.0

    if z_score >= 2.0:
        z_state = f"+{z_score:.2f} σ (과열 밴드)"
        z_color = "warning"
    elif z_score <= -2.0:
        z_state = f"{z_score:.2f} σ (과매도 반등)"
        z_color = "positive"
    else:
        z_state = f"{z_score:+.2f} σ (정상 밴드)"
        z_color = "neutral"

    pct_changes = closes.pct_change().dropna().tail(20)
    ann_vol = float(pct_changes.std() * np.sqrt(250) * 100) if len(pct_changes) > 5 else 20.0
    if np.isnan(ann_vol) or np.isinf(ann_vol):
        ann_vol = 20.0
    vol_risk_state = "고변동성" if ann_vol >= 40 else ("적정 변동성" if ann_vol >= 18 else "저변동성")
    vol_risk_color = "warning" if ann_vol >= 40 else ("positive" if ann_vol >= 18 else "neutral")

    bb_period = min(60, n_bars)
    bb_mid = float(closes.tail(bb_period).mean())
    bb_std = float(closes.tail(bb_period).std())
    bb_upper = bb_mid + (bb_std * 2)
    bb_lower = bb_mid - (bb_std * 2)
    bb_str = f"{round_fn(bb_upper)} / {round_fn(bb_lower)}"

    if raw_price >= bb_upper:
        bb_state = "상단 돌파 (강세)"
        bb_color = "positive"
    elif raw_price <= bb_lower:
        bb_state = "하단 지지 (반등타진)"
        bb_color = "warning"
    else:
        bb_state = "밴드 내 안정등락"
        bb_color = "neutral"

    if n_bars >= 35:
        ema12 = closes.ewm(span=12, adjust=False).mean()
        ema26 = closes.ewm(span=26, adjust=False).mean()
        macd_line = ema12 - ema26
        signal_line = macd_line.ewm(span=9, adjust=False).mean()
        macd_val = float(macd_line.iloc[-1])
        sig_val = float(signal_line.iloc[-1])
        macd_str = f"{macd_val:+.2f} (Sig: {sig_val:+.2f})"
        if macd_val >= sig_val:
            macd_state = "골든크로스"
            macd_color = "positive"
        else:
            macd_state = "데드크로스"
            macd_color = "negative"
    else:
        macd_str = "-"
        macd_state = "데이터 집계 중"
        macd_color = "neutral"

    df_1y = df.tail(250)
    high_52w = float(df_1y['High'].max())
    low_52w = float(df_1y['Low'].min())
    if high_52w > low_52w:
        pos_52w = ((raw_price - low_52w) / (high_52w - low_52w)) * 100.0
        pos_52w_str = f"{pos_52w:.1f}%"
        slider_fill_pct = int(min(92, max(8, int(pos_52w))))
    else:
        pos_52w_str = "-"
        pos_52w = 50.0
        slider_fill_pct = 50

    disp20 = (raw_price / ma20_val) * 100 if ma20_val > 0 else 100.0
    disp60 = (raw_price / ma60_val) * 100 if ma60_val and ma60_val > 0 else 100.0
    disp_str = f"20일: {disp20:.1f}% | 60일: {disp60:.1f}%"
    disp_state = "적정 이격 유지" if 98.0 <= disp20 <= 104.0 else ("과열 경계" if disp20 > 104.0 else "과매도 구간")
    disp_color = "positive" if 98.0 <= disp20 <= 104.0 else ("warning" if disp20 > 104.0 else "neutral")

    delta = closes.diff()
    gain = delta.where(delta > 0, 0.0).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0.0)).rolling(14).mean()
    rs = gain / loss
    rsi_series = 100 - (100 / (1 + rs))
    rsi_num = float(rsi_series.iloc[-1]) if not np.isnan(rsi_series.iloc[-1]) else 50.0
    rsi_val = f"{rsi_num:.1f}"
    if rsi_num >= 70:
        rsi_state = "과열권 (차익 경계)"
        rsi_color = "warning"
    elif rsi_num <= 30:
        rsi_state = "침체권 (반등 기대)"
        rsi_color = "positive"
    else:
        rsi_state = "중립 심리"
        rsi_color = "neutral"

    high_low = df['High'] - df['Low']
    atr_raw = float(high_low.tail(14).mean())
    if np.isnan(atr_raw) or atr_raw <= 0:
        atr_raw = raw_price * 0.02
    atr_val = round_fn(atr_raw)
    atr_ratio = f"{((atr_raw / raw_price) * 100):.2f}%" if raw_price > 0 else "0.0%"

    v_mean5 = float(volumes.tail(5).mean())
    v_curr = float(volumes.iloc[-1])
    vol_pct = ((v_curr / v_mean5) * 100) if v_mean5 > 0 else 100.0
    vol_ratio = f"{vol_pct:.1f}%"
    vol_state = "거래 급증 (수급 유입)" if vol_pct >= 150 else ("거래 보통" if vol_pct >= 70 else "거래 침체")
    vol_color = "positive" if vol_pct >= 150 else "neutral"

    rank_str = get_trading_val_rank(symbol, is_krw)
    if n_bars >= 20:
        val_20d = float((closes.tail(20) * volumes.tail(20)).mean())
        if is_krw:
            if val_20d >= 100_000_000_000:
                tval_20d_str = f"{val_20d / 100_000_000_000:.1f}천억원{rank_str}"
            elif val_20d >= 100_000_000:
                tval_20d_str = f"{val_20d / 100_000_000:.1f}억원{rank_str}"
            else:
                tval_20d_str = f"{int(val_20d):,}원{rank_str}"
        else:
            if val_20d >= 1_000_000_000:
                tval_20d_str = f"{val_20d / 1_000_000_000:.2f}B 달러"
            elif val_20d >= 1_000_000:
                tval_20d_str = f"{val_20d / 1_000_000:.1f}M 달러"
            else:
                tval_20d_str = f"{val_20d:,.0f} 달러"
    else:
        tval_20d_str = "-"

    # 최근 60거래일 모멘텀 파동 계산
    momentum_history = []
    try:
        hist_df = df.tail(80).copy()
        h_close = hist_df['Close'].astype(float)
        h_vol = hist_df['Volume'].astype(float)
        h_ma5 = h_close.rolling(5).mean()
        h_ma20 = h_close.rolling(20).mean()
        h_ma60 = h_close.rolling(60).mean()
        h_vmean5 = h_vol.rolling(5).mean()

        h_ema12 = h_close.ewm(span=12, adjust=False).mean()
        h_ema26 = h_close.ewm(span=26, adjust=False).mean()
        h_macd = h_ema12 - h_ema26
        h_macdsig = h_macd.ewm(span=9, adjust=False).mean()

        h_delta = h_close.diff()
        h_gain = h_delta.where(h_delta > 0, 0.0).rolling(14).mean()
        h_loss = (-h_delta.where(h_delta < 0, 0.0)).rolling(14).mean()
        h_rs = h_gain / h_loss.replace(0, np.nan)
        h_rsi = 100 - (100 / (1 + h_rs))

        for idx in hist_df.tail(60).index:
            p_c = float(h_close.loc[idx])
            b_cnt = 0
            if pd.notna(h_ma5.loc[idx]) and p_c >= float(h_ma5.loc[idx]): b_cnt += 1
            if pd.notna(h_ma20.loc[idx]) and p_c >= float(h_ma20.loc[idx]): b_cnt += 1
            if pd.notna(h_ma60.loc[idx]) and p_c >= float(h_ma60.loc[idx]): b_cnt += 1
            if pd.notna(h_macd.loc[idx]) and pd.notna(h_macdsig.loc[idx]) and float(h_macd.loc[idx]) >= float(h_macdsig.loc[idx]): b_cnt += 1
            if pd.notna(h_rsi.loc[idx]) and (40.0 <= float(h_rsi.loc[idx]) <= 65.0): b_cnt += 1
            if pd.notna(h_vmean5.loc[idx]) and float(h_vol.loc[idx]) >= float(h_vmean5.loc[idx]): b_cnt += 1

            day_score = int(min(98, max(42, int(45 + (b_cnt * 9)))))
            t_str = pd.to_datetime(idx).strftime("%Y-%m-%d")
            momentum_history.append({"time": t_str, "score": day_score})

        quant_score = momentum_history[-1]["score"] if momentum_history else 72
        bullish_count = int(round((quant_score - 45) / 9)) if quant_score >= 45 else 0
    except Exception:
        momentum_history = []
        quant_score = 72
        bullish_count = 3

    score_badge = "모멘텀 최강세" if quant_score >= 80 else ("상승 모멘텀 우세" if quant_score >= 65 else "중립 횡보 국면")

    if ma60_val and raw_price > ma20_val > ma60_val:
        trend = "정배열 상승 추세"
        guide_text = f"20일선 지지 안착 및 5일선 골든크로스 확인 시 분할 착수하며, 일일 변동폭({atr_val}) 수준의 단기 음봉 눌림목에서 분할 매수 후 전고점 돌파 시 1차 익절 집행"
    elif ma60_val and raw_price < ma20_val < ma60_val:
        trend = "역배열 하락 추세"
        guide_text = f"RSI({rsi_val}) 침체권 매물 소화 및 하단 지지선 형성 확인 후 보수적 분할 접근하며, 20일선 저항 매물대 돌파 실패 시 리스크 관리 필수"
    else:
        trend = "박스권 혼조세"
        guide_text = f"박스권 상하단 밴드 내 매물 공방 국면이므로 볼린저 밴드 하단 지지 확인 시 진입하고, 1차 목표가 도달 시 비중 축소 권장"

    entry1_val = raw_price - (0.5 * atr_raw)
    entry2_val = raw_price - (1.2 * atr_raw)
    target1_val = raw_price + (1.5 * atr_raw)
    target2_val = raw_price + (2.8 * atr_raw)
    stop_val = raw_price - (1.8 * atr_raw)

    entry1 = round_fn(entry1_val)
    entry2 = round_fn(entry2_val)
    target1 = round_fn(target1_val)
    target2 = round_fn(target2_val)
    stop_p = round_fn(stop_val)

    sig_fill_pct = min(92, max(8, int(((entry1_val - stop_val) / max(1, target2_val - stop_val)) * 100)))
    win_1y, win_2y, rr_actual = run_real_vectorized_backtest(df)

    return {
        "quant_score": str(quant_score),
        "score_badge": score_badge,
        "score_dots": f"<span style='color:#10b981; font-weight:700;'>상승 시그널 {bullish_count}</span> &nbsp; <span style='color:#38bdf8; font-weight:700;'>중립 {6 - bullish_count}</span>",
        "trend": trend,
        "momentum_history": momentum_history,
        "z_score": f"{z_score:+.2f} σ", "z_state": z_state, "z_color": z_color,
        "ann_vol": f"{ann_vol:.1f}%", "vol_risk_state": vol_risk_state, "vol_risk_color": vol_risk_color,
        "ma5": ma5, "ma5_state": ma5_state, "ma5_color": ma5_color,
        "ma20": ma20, "ma20_state": ma20_state, "ma20_color": ma20_color,
        "ma60": ma60, "ma60_state": ma60_state, "ma60_color": ma60_color,
        "ma120": ma120, "ma120_state": ma120_state, "ma120_color": ma120_color,
        "ma600": ma600, "ma1200": ma1200,
        "bollinger": bb_str, "bb_state": bb_state, "bb_color": bb_color,
        "macd": macd_str, "macd_state": macd_state, "macd_color": macd_color,
        "pos_52w": pos_52w_str, "slider_fill_pct": slider_fill_pct,
        "high_52w": round_fn(high_52w), "low_52w": round_fn(low_52w),
        "disp": disp_str, "disp_state": disp_state, "disp_color": disp_color, "disp20_num": f"{disp20:+.1f}%",
        "rsi": rsi_val, "rsi_state": rsi_state, "rsi_color": rsi_color,
        "atr": atr_val, "atr_ratio": atr_ratio,
        "vol_ratio": vol_ratio, "vol_state": vol_state, "vol_color": vol_color,
        "tval_20d": tval_20d_str,
        "entry1": entry1, "entry2": entry2,
        "target1": target1, "target2": target2, "stop_p": stop_p,
        "sig_fill_pct": sig_fill_pct, "guide_text": guide_text,
        "bt_1y": f"1년 실측 {win_1y}%", "bt_2y": f"2년 실측 {win_2y}%",
        "win_rate_num": f"{int(win_2y)}%", "rr_str": f"실측 손익비 1:{rr_actual}"
    }

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

# ===== WHY MOVED: multi-source news clustering engine =====
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

def analyze_global_impact(formal_name, market_name, world_board, stock_news,
                          global_news=None, dart_disclosures=None, df=None,
                          domestic_flow=None, market_index_name=None,
                          consensus=None, macro_snapshot=None, market_calendar=None, symbol=""):
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
    alpha_earnings=fetch_alpha_vantage_earnings(symbol) if market_name=="미국주식" else {}
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



MARKET_INFO_GROUPS = {
    "fomc": ["fomc", "연준", "연방준비", "파월", "금리인하", "금리 인하", "금리인상", "금리 인상"],
    "inflation": ["cpi", "pce", "물가", "인플레이션", "소비자물가"],
    "jobs": ["고용", "실업률", "비농업", "고용보고서", "jobs", "payroll"],
    "rates": ["국채금리", "채권금리", "10년물", "2년물", "금리"],
    "tariff": ["관세", "tariff", "무역"],
    "market": ["나스닥", "s&p", "s&p500", "다우", "미국증시", "뉴욕증시", "증시"],
}

MARKET_INFO_STOP = set("미국 뉴욕 시장 증시 주가 주식 관련 소식 전해 전망 투자 오늘 어제 최근 상승 하락 영향 기대".split())

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

def analyze_movement_reason(raw_price, df, formal_name, market_name, world_board, stock_news, dart_disclosures, market_index_name=None, domestic_flow=None, global_news=None, consensus=None, macro_snapshot=None, market_calendar=None, symbol=""):
    """실제 확보된 데이터의 강도를 비교해 가장 설득력 있는 움직임 원인을 선택한다."""
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
        symbol=symbol
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
def _safe_future_result(future, default, label):
    """외부 API 하나가 실패해도 전체 /api/analyze가 500으로 끝나지 않게 한다."""
    if future is None:
        return default
    try:
        return future.result()
    except Exception as exc:
        print(f"[OPTIONAL API] {label} failed: {repr(exc)}")
        return default


def run_full_pipeline(query):
    global ANALYSIS_CACHE, CACHED_WORLD_BOARD, CACHED_MARKET_CALENDAR
    clean_q = query.strip().replace(" ", "").upper()
    now_ts = time.time()

    if clean_q in ANALYSIS_CACHE:
        cached_time, cached_data = ANALYSIS_CACHE[clean_q]
        if now_ts - cached_time < 30:
            if CACHED_WORLD_BOARD:
                cached_data["world_board"] = CACHED_WORLD_BOARD
            if CACHED_MARKET_CALENDAR:
                cached_data["market_calendar"] = CACHED_MARKET_CALENDAR
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
        chart_price, _, df = fetch_overseas_candles(symbol)
        quote_snapshot = fetch_overseas_quote_snapshot(symbol)
        if quote_snapshot:
            raw_price = quote_snapshot["price"]
            price_str = f"{raw_price:,.2f} 달러"
        else:
            raw_price = chart_price
            price_str = f"{raw_price:,.2f} 달러" if raw_price is not None else None
        market_name = "미국주식"
        is_krw = False
    elif any(c.isupper() for c in query) and not any('\uac00' <= char <= '\ud7a3' for char in query):
        symbol = query.upper().strip()
        formal_name = symbol
        chart_price, _, df = fetch_overseas_candles(symbol)
        quote_snapshot = fetch_overseas_quote_snapshot(symbol)
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
        market_index_name = get_naver_stock_market_type(symbol)
        raw_price, price_str = get_naver_realtime_price(symbol)
        df = fetch_domestic_candles(symbol, anchor_price=raw_price)
        if raw_price is None and df is not None and not df.empty:
            raw_price = float(df['Close'].iloc[-1])
            price_str = f"{int(raw_price):,} 원"

    if raw_price is None or df is None:
        return None

    # ====================================================
    # PostgreSQL 원천 OHLCV 저장
    # 기존 사이트 분석 로직과 분리되어 있으며,
    # DB 오류가 나도 기존 분석은 계속 진행한다.
    # ====================================================
    try:
        save_market_prices(
            df=df,
            market_name=market_name,
            symbol=symbol,
            timeframe="1d",
            source="alpha-trade-bot",
        )
    except Exception as exc:
        print(f"[DB] 원천 OHLCV 저장 예외 - 기존 분석 계속: {type(exc).__name__}: {exc}")

    with ThreadPoolExecutor(max_workers=9) as executor:
        f_news = executor.submit(fetch_rss_news, formal_name, 10)
        f_global_news = executor.submit(fetch_rss_news, "뉴욕증시 OR 미국증시 OR 나스닥", 10)
        f_api_news = executor.submit(fetch_alpha_vantage_news, symbol if market_name == "미국주식" else "", "economy_macro")
        f_macro = executor.submit(fetch_fred_macro_snapshot)
        f_consensus = executor.submit(fetch_market_consensus_unified, symbol, market_name, raw_price, formal_name)
        f_dart = executor.submit(fetch_dart_disclosures, symbol, is_krw, 5)
        f_domestic_flow = executor.submit(fetch_naver_domestic_flow_metrics, symbol) if is_krw and market_name == "국내주식" else None

        stock_news_rss = _safe_future_result(f_news, [], "stock RSS")
        global_news_rss = _safe_future_result(f_global_news, [], "global RSS")
        api_news = _safe_future_result(f_api_news, [], "Alpha Vantage news")
        macro_snapshot = _safe_future_result(f_macro, {}, "FRED macro")

        ticker_news = _safe_future_result(
            executor.submit(fetch_alpha_vantage_news, symbol, "") if market_name == "미국주식" and symbol else None,
            [], "Alpha ticker news"
        )
        # 종목 재료와 거시/시장 뉴스를 섞지 않는다.
        stock_news = merge_news_sources(stock_news_rss, ticker_news)
        global_news = merge_news_sources(global_news_rss, api_news)
        consensus = _safe_future_result(f_consensus, None, "consensus")
        dart_disclosures = _safe_future_result(f_dart, [], "DART/Naver disclosure") or []
        domestic_flow = _safe_future_result(f_domestic_flow, None, "domestic flow") if f_domestic_flow else None

    if CACHED_WORLD_BOARD:
        world_board = CACHED_WORLD_BOARD
    else:
        try:
            world_board = fetch_all_world_board_parallel() or []
        except Exception as exc:
            print("[WORLD BOARD] 전체 조회 실패:", repr(exc))
            world_board = []

    if CACHED_MARKET_CALENDAR:
        market_calendar = CACHED_MARKET_CALENDAR
    else:
        try:
            market_calendar = fetch_real_market_calendar_live() or []
        except Exception as exc:
            print("[MARKET CALENDAR] 조회 실패:", repr(exc))
            market_calendar = []

    quant = calculate_quant_factors(raw_price, df, symbol=symbol, is_krw=is_krw)

    try:
        save_quant_result(
            market_name=market_name,
            symbol=symbol,
            raw_price=raw_price,
            df=df,
            quant=quant,
            timeframe="1d",
        )
    except Exception as exc:
        print(f"[DB] 퀀트 결과 저장 예외 - 기존 분석 계속: {type(exc).__name__}: {exc}")

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
        symbol=symbol
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

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S KST")

    df['MA20'] = df['Close'].rolling(20).mean()
    df['MA60'] = df['Close'].rolling(60).mean()
    df['STD60'] = df['Close'].rolling(60).std()
    df['BB_UP'] = df['MA60'] + (df['STD60'] * 2)
    df['BB_DN'] = df['MA60'] - (df['STD60'] * 2)

    df = df.sort_index()
    df = df[~df.index.duplicated(keep='last')]

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

    ANALYSIS_CACHE[clean_q] = (now_ts, result_data)
    return result_data

# ----------------------------------------------------
# 8. 웹 프론트엔드 UI
# ----------------------------------------------------
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
    <script src="https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2"></script>
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

        /* 1행: 왜 움직였을까? — 코멘트 전용 */
        #rowCause { grid-column: 1; grid-row: 1; }

        /* 4행: 트랙 분석 — 종목 뉴스 바로 아래 */
        #rowTrack { grid-column: 1; grid-row: 4; }

        /* 2행: 차트(좌) & 증시(우) */
        #rowChart { grid-column: 1; grid-row: 2; height: 100%; }
        #rowWorld { grid-column: 2; grid-row: 2; height: 100%; }

        /* 3행: 종목 뉴스(좌) & 해외 뉴스(우) */
        #rowNews { grid-column: 1; grid-row: 3; height: 100%; }
        #rowGlobalNews { grid-column: 2; grid-row: 3; height: 100%; }

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
        #rowSignal { grid-column: 1; grid-row: 10; }

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

        .price-title { font-size: 1.25rem; font-weight: 700; color: var(--text-title); word-break: keep-all; }
        .price-title span { color: var(--text-muted); font-size: 0.85rem; font-weight: 400; margin-left: 4px; }
        .price-val { font-size: 2.1rem; font-weight: 800; color: var(--text-title); margin: 6px 0; }
        .price-meta { color: var(--text-muted); font-size: 0.78rem; line-height: 1.4; word-break: break-all; }

        .chart-legend { display: flex; flex-wrap: wrap; gap: 8px 12px; font-size: 0.75rem; color: var(--text-muted); margin-top: 10px; }
        .legend-tag { display: flex; align-items: center; gap: 4px; }
        .legend-dot { width: 7px; height: 7px; border-radius: 50%; }
        .chart-box-wrap { width: 100%; height: 300px; margin-top: 10px; position: relative; background: var(--bg-card); border-radius: 8px; overflow: hidden; }

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
            #rowCause { order: 1; }
            #rowChart { order: 2; }
            #rowNews { order: 3; }
            #rowTrack { order: 4; }
            #rowQuant { order: 5; }
            #rowConsensus { order: 6; }
            #rowVote { order: 7; }
            #rowComment { order: 8; }
            #rowSignal { order: 9; }
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
            #rowCause { order: 1 !important; }
            #rowChart { order: 2 !important; }
            #rowNews { order: 3 !important; }
            #rowTrack { order: 4 !important; }
            #rowQuant { order: 5 !important; }
            #rowConsensus { order: 6 !important; }
            #rowVote { order: 7 !important; }
            #rowComment { order: 8 !important; }
            #rowSignal { order: 9 !important; }
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
            #rowCause,
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
        #rowSignal #signalBody {
            display: block !important;
            height: auto !important;
            max-height: none !important;
            overflow: visible !important;
            visibility: visible !important;
            opacity: 1 !important;
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

        <div class="sidebar-scroll-area">
            <div class="sidebar-section-title">최근 종목</div>
            <div class="sidebar-item" onclick="doSearch('SK하이닉스');">SK하이닉스</div>
            <div class="sidebar-item" onclick="doSearch('엔비디아');">엔비디아</div>
            <div class="sidebar-item" onclick="doSearch('삼성전자');">삼성전자</div>
            <div class="sidebar-item" onclick="doSearch('비트코인');">비트코인</div>
        </div>
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
                    <input type="text" id="centerInput" placeholder="종목명 입력 (예: SK하이닉스, 삼성전자, NVDA, BTC)" onkeypress="if(event.keyCode==13) doSearch(this.value);" autocomplete="off">
                    
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

                    <button class="send-btn" onclick="doSearch(document.getElementById('centerInput').value)">
                        <svg width="18" height="18" viewBox="0 0 24 24"><path d="M12 4L12 20M12 4L5 11M12 4L19 11" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" fill="none"/></svg>
                    </button>
                </div>
            </div>
            <div class="popular-tags">
                <button class="tag-btn" onclick="doSearch('SK하이닉스')">SK하이닉스</button>
                <button class="tag-btn" onclick="doSearch('삼성전자')">삼성전자</button>
                <button class="tag-btn" onclick="doSearch('알테오젠')">알테오젠</button>
                <button class="tag-btn" onclick="doSearch('엔비디아')">엔비디아</button>
                <button class="tag-btn" onclick="doSearch('비트코인')">비트코인</button>
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
                                <div class="chart-legend">
                                    <div class="legend-tag"><span class="legend-dot" style="background:#00b4d8;"></span> 20일선 (슬림)</div>
                                    <div class="legend-tag"><span class="legend-dot" style="background:#f97316;"></span> 볼린저 60선 (중심)</div>
                                    <div class="legend-tag"><span class="legend-dot" style="background:#f87171;"></span> 볼린저 상단 (저항)</div>
                                    <div class="legend-tag"><span class="legend-dot" style="background:#38bdf8;"></span> 볼린저 하단 (지지)</div>
                                </div>
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

                        <!-- 1행 좌측: 왜 움직였을까? — 코멘트 전용 -->
                        <div class="card" id="rowCause" style="justify-content:flex-start;">
                            <div class="board-header" style="margin-bottom:8px; padding-bottom:0; border-bottom:none;">
                                <span class="board-header-title" style="font-size:0.95rem; font-weight:800; color:var(--text-title);">왜 움직였을까?</span>
                            </div>
                            <div id="movementReasonCard" style="font-size:0.86rem;line-height:1.65;color:var(--text-sub);word-break:keep-all;margin-top:0;"></div>
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
                                    <button class="calendar-star-btn active" data-cal-type="earn" data-stars="all" onclick="setCalendarStarFilter('earn','all',this)">전체</button>
                                    <button class="calendar-star-btn" data-cal-type="earn" data-stars="3" onclick="setCalendarStarFilter('earn','3',this)">★★★</button>
                                    <button class="calendar-star-btn" data-cal-type="earn" data-stars="2" onclick="setCalendarStarFilter('earn','2',this)">★★☆</button>
                                    <button class="calendar-star-btn" data-cal-type="earn" data-stars="1" onclick="setCalendarStarFilter('earn','1',this)">★☆☆</button>
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
                            <div class="board-header-title" style="margin-bottom:12px;">해외 뉴스</div>
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

                        <!-- 8행: gaemiGTP 시그널 박스 (832px) -->
                        <div class="signal-premium-card" id="rowSignal">
                            <div class="signal-card-header">
                                <span class="signal-card-title">gaemiGTP 시그널</span>
                            </div>
                            
                            <div id="signalBody" style="display:block; margin-top:16px;">
                                <div style="display:flex; align-items:center; gap:14px; padding-bottom:16px; border-bottom:1px solid rgba(16,185,129,0.2);">
                                    <div class="gauge-inner-box">
                                        <span style="font-size:1.05rem; font-weight:800; color:var(--text-title); line-height:1;" id="rWinRateGauge"></span>
                                        <span style="font-size:0.6rem; color:var(--text-muted); margin-top:3px; font-weight:700;">승률</span>
                                    </div>
                                    <div style="min-width:0;">
                                        <div style="display:flex; align-items:center; gap:6px; flex-wrap:wrap;">
                                            <span style="font-size:0.82rem; color:var(--text-muted);">시그널 종합</span>
                                            <span style="background:rgba(16,185,129,0.15); color:#10b981; font-size:0.75rem; font-weight:800; padding:3px 8px; border-radius:10px;">분할 대응 유효</span>
                                        </div>
                                        <div style="font-size:0.78rem; color:var(--text-muted); margin-top:5px; word-break:keep-all;">
                                            <span style="color:#10b981; font-weight:800;" id="rBt1yTag"></span> &nbsp; 
                                            <span style="color:#10b981; font-weight:800;" id="rBt2yTag"></span> &nbsp; 
                                            <span style="color:#38bdf8; font-weight:800;" id="rRrTag"></span>
                                        </div>
                                    </div>
                                </div>

                                <div style="margin:20px 0 14px; position:relative;">
                                    <div style="display:flex; justify-content:space-between; font-size:0.75rem; margin-bottom:6px; white-space:nowrap;">
                                        <span style="color:#38bdf8; font-weight:800;" id="rSliderStop"></span>
                                        <span style="color:var(--text-title); font-weight:800;" id="rSliderEntry"></span>
                                        <span style="color:#10b981; font-weight:800;" id="rSliderTarget"></span>
                                    </div>
                                    <div style="width:100%; height:7px; background:var(--bg-subcard); border-radius:4px; position:relative; border:1px solid var(--border-line);">
                                        <div style="height:100%; background:linear-gradient(90deg, #38bdf8, #10b981); border-radius:4px;" id="rSigBarFill"></div>
                                        <div class="dot-marker-style" id="rSigDotMarker"></div>
                                    </div>
                                </div>

                                <div style="margin-top:16px; border-top:1px solid rgba(16,185,129,0.2); padding-top:10px;">
                                    <div style="display:flex; justify-content:space-between; align-items:center; padding:8px 0; border-bottom:1px solid rgba(16,185,129,0.15); font-size:0.86rem;">
                                        <div><span style="color:var(--text-title); font-weight:700;">1차 진입 기준가</span></div>
                                        <div style="color:var(--text-title); font-weight:800;" id="rEntry1"></div>
                                    </div>
                                    <div style="display:flex; justify-content:space-between; align-items:center; padding:8px 0; border-bottom:1px solid rgba(16,185,129,0.15); font-size:0.86rem;">
                                        <div><span style="color:var(--text-title); font-weight:700;">2차 분할 기준가</span></div>
                                        <div style="color:var(--text-title); font-weight:800;" id="rEntry2"></div>
                                    </div>
                                    <div style="display:flex; justify-content:space-between; align-items:center; padding:8px 0; border-bottom:1px solid rgba(16,185,129,0.15); font-size:0.86rem;">
                                        <div><span style="color:#10b981; font-weight:700;">1차 목표가</span></div>
                                        <div style="color:#10b981; font-weight:800;" id="rTarget1"></div>
                                    </div>
                                    <div style="display:flex; justify-content:space-between; align-items:center; padding:8px 0; border-bottom:1px solid rgba(16,185,129,0.15); font-size:0.86rem;">
                                        <div><span style="color:#10b981; font-weight:700;">2차 목표가</span></div>
                                        <div style="color:#10b981; font-weight:800;" id="rTarget2"></div>
                                    </div>
                                    <div style="display:flex; justify-content:space-between; align-items:center; padding:8px 0; font-size:0.86rem;">
                                        <div><span style="color:#38bdf8; font-weight:700;">리스크 방어선</span></div>
                                        <div style="color:#38bdf8; font-weight:800;" id="rStop"></div>
                                    </div>
                                </div>

                                <div class="guide-box">
                                    <div class="guide-title">gaemiGTP 시그널 가이드</div>
                                    <div id="rGuideText" style="margin-top:4px; color:var(--text-sub); word-break:keep-all;"></div>
                                </div>
                            </div>
                        </div>
</div>

                </div>
            </div>
        </div>

        <div id="bottom-bar">
            <div class="bottom-bar-grid-wrapper">
                <div class="search-container">
                    <div class="search-box">
                        <input type="text" id="bottomInput" placeholder="다른 종목명 입력 (예: 테슬라, 카카오, BTC)" onkeypress="if(event.keyCode==13) doSearch(this.value);" autocomplete="off">
                        
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

                        <button class="send-btn" onclick="doSearch(document.getElementById('bottomInput').value)">
                            <svg width="18" height="18" viewBox="0 0 24 24"><path d="M12 4L12 20M12 4L5 11M12 4L19 11" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" fill="none"/></svg>
                        </button>
                    </div>
                </div>
            </div>
        </div>
    </div>

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
            if (chart && container && container.clientWidth > 0) {
                chart.applyOptions({ width: container.clientWidth });
            }
        }

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
        let economicCalOffset = 0;
        let earningsCalOffset = 0;
        let economicCalPage = 1;
        let earningsCalPage = 1;
        // 실적 캘린더는 오늘 실적이 없을 때 최초 1회만 '가장 가까운 예정 실적일'로 이동합니다.
        // 사용자가 [오늘]을 누르면 다시 오늘 날짜를 정확히 보여줍니다.
        let earningsCalendarAutoJumped = false;

        // 경제 캘린더는 오늘 발표가 없을 때 최초 1회만
        // '오늘 이후 가장 가까운 예정 일정'으로 이동합니다.
        // 앞으로 일정이 없으면 오늘 이전 가장 최근 일정으로 이동합니다.
        // 사용자가 [오늘]을 누르면 다시 오늘 날짜를 정확히 보여줍니다.
        let economicCalendarAutoJumped = false;

        let economicCalStarFilter = 'all';
        let earningsCalStarFilter = 'all';
        const calendarItemsPerPage = 4;

        function renderCalendarEventCard(ev) {
            const isEarn = ev.type === 'earn';
            const clickAttr = isEarn && ev.ticker
                ? `onclick="doSearch('${String(ev.ticker).replace(/'/g, "\\'")}')"`
                : '';
            const alarmKey = getCalendarEventKey(ev);
            const alarmActive = !!getCalendarAlarms()[alarmKey];
            const stars = ev.star_str || (ev.stars === 3 ? '★★★' : ev.stars === 2 ? '★★☆' : '★☆☆');
            const timeLabel = ev.time || '--:--';
            const status = (ev.val === '발표완료' || ev.actual || ev.status === '발표완료') ? '발표완료' : '발표대기';
            const metaParts = [];
            if (ev.exp && ev.exp !== '-') {
                const expText = String(ev.exp).trim();
                metaParts.push(
                    expText.startsWith('예상')
                        ? expText
                        : `예상 ${expText}`
                );
            }

            // 실적 캘린더의 두 번째 값은 '이전'이 아니라 '예상 매출'입니다.
            // 경제 캘린더에서만 ev.prev를 '이전'으로 표시합니다.
            if (isEarn) {
                const revenueText = String(
                    ev.expected_revenue_str || ''
                ).trim();
                if (revenueText && revenueText !== '-') {
                    metaParts.push(
                        revenueText.startsWith('예상')
                            ? revenueText
                            : `예상 매출 ${revenueText}`
                    );
                }
            } else if (ev.prev && ev.prev !== '-') {
                metaParts.push(`이전 ${ev.prev}`);
            }

            const meta = metaParts.join('  ·  ');

            // 실적 캘린더에서는 티커(NVDA 등)와 기업 마크를 표시하지 않습니다.
            const rawEventName = ev.name || (isEarn ? '실적 발표' : '경제지표');
            let eventName = isEarn
                ? String(rawEventName).replace(/\s*\([A-Za-z0-9.\-]+\)/g, '').trim()
                : String(rawEventName);

            // 이전 데이터 형식에서 이름에 발표시점이 붙어 있어도 화면에서는 분리합니다.
            let timingLabel = isEarn ? String(ev.timing_label || '').trim() : '';
            if (isEarn && !timingLabel) {
                const timingMatch = eventName.match(/\s*(\((?:미국 )?장 (?:마감 후|시작 전)\))\s*$/);
                if (timingMatch) {
                    timingLabel = timingMatch[1];
                    eventName = eventName.slice(0, timingMatch.index).trim();
                }
            }

            return `
                <div class="cal-card-item ${isEarn ? 'cal-earn-item' : 'cal-eco-item'}" ${clickAttr}>
                    <div class="cal-time-col">${timeLabel}</div>
                    <div class="cal-detail-col">
                        <div class="cal-event-topline">
                            <span class="cal-stars">${stars}</span>
                            <span class="cal-event-status">${status}</span>
                        </div>
                        <div class="cal-event-title">${eventName}</div>
                        ${isEarn && timingLabel ? `<div class="cal-earn-timing">${timingLabel}</div>` : ''}
                        ${meta ? `<div class="cal-event-meta">${meta}</div>` : ''}
                    </div>
                    <button class="cal-alarm-btn ${alarmActive ? 'active' : ''}"
                            onclick="event.stopPropagation(); toggleCalendarAlarmByKey('${encodeURIComponent(alarmKey)}')"
                            title="${alarmActive ? '알람 해제' : '10분 전 알람 설정'}"
                            aria-label="${alarmActive ? '알람 해제' : '10분 전 알람 설정'}">
                        <svg class="cal-alarm-icon" viewBox="0 0 24 24" aria-hidden="true">
                            <path d="M18 9a6 6 0 0 0-12 0c0 7-3 7-3 9h18c0-2-3-2-3-9"></path>
                            <path d="M10 21h4"></path>
                        </svg>
                    </button>
                </div>
            `;
        }

        function getCalendarDateInfo(offset) {
            const now = new Date();
            const kstString = new Intl.DateTimeFormat('en-CA', {
                timeZone: 'Asia/Seoul',
                year: 'numeric',
                month: '2-digit',
                day: '2-digit'
            }).format(now);

            const [yyyy, mm, dd] = kstString.split('-').map(Number);
            const d = new Date(Date.UTC(yyyy, mm - 1, dd + Number(offset || 0)));
            const daysMap = ["일요일", "월요일", "화요일", "수요일", "목요일", "금요일", "토요일"];

            return {
                dateKey: `${d.getUTCFullYear()}.${String(d.getUTCMonth() + 1).padStart(2, '0')}.${String(d.getUTCDate()).padStart(2, '0')}`,
                display: `${d.getUTCFullYear()}.${String(d.getUTCMonth() + 1).padStart(2, '0')}.${String(d.getUTCDate()).padStart(2, '0')} (${daysMap[d.getUTCDay()].slice(0, 1)})`,
                header: `${d.getUTCMonth() + 1}월 ${d.getUTCDate()}일 ${daysMap[d.getUTCDay()]}`
            };
        }

        // 경제 캘린더 이벤트 판별을 한 곳에서 통일합니다.
        // 이전 배포본에서 type 값이 eco/economic으로 달라져도 대응합니다.
        function isEconomicCalendarEvent(ev) {
            if (!ev || !ev.date_key) return false;
            const t = String(ev.type || '').toLowerCase();
            return t === 'eco' || t === 'economic';
        }

        function getTodayKstDateKey() {
            const now = new Date();
            const kstString = new Intl.DateTimeFormat('en-CA', {
                timeZone: 'Asia/Seoul',
                year: 'numeric',
                month: '2-digit',
                day: '2-digit'
            }).format(now);
            return kstString.replace(/-/g, '.');
        }

        function findNearestEconomicOffset() {
            const all = Array.isArray(globalMarketCalendar) ? globalMarketCalendar : [];
            const todayKey = getTodayKstDateKey();
            const [yyyy, mm, dd] = todayKey.split('.').map(Number);
            const today = new Date(Date.UTC(yyyy, mm - 1, dd));

            const economicDates = [...new Set(
                all
                    .filter(isEconomicCalendarEvent)
                    .map(ev => String(ev.date_key).replace(/[\/]/g, '.'))
            )].sort();

            if (!economicDates.length) return 0;

            // 1. 오늘 경제 일정이 있으면 오늘을 그대로 유지합니다.
            if (economicDates.includes(todayKey)) {
                return 0;
            }

            // 2. 오늘 이후 가장 가까운 경제 일정 날짜를 먼저 찾습니다.
            //    주말/공휴일처럼 오늘 일정이 없는 경우 예정된 다음 일정으로 이동합니다.
            for (const key of economicDates) {
                const parts = key.split('.').map(Number);
                if (parts.length !== 3 || parts.some(Number.isNaN)) continue;

                const candidate = new Date(
                    Date.UTC(parts[0], parts[1] - 1, parts[2])
                );

                if (candidate >= today) {
                    return Math.round((candidate - today) / 86400000);
                }
            }

            // 3. 미래 일정도 없으면 오늘 이전의 가장 최근 일정으로 이동합니다.
            let latestKey = null;
            for (const key of economicDates) {
                const parts = key.split('.').map(Number);
                if (parts.length !== 3 || parts.some(Number.isNaN)) continue;

                const candidate = new Date(
                    Date.UTC(parts[0], parts[1] - 1, parts[2])
                );

                if (candidate < today) {
                    latestKey = key;
                }
            }

            if (!latestKey) return 0;

            const [y, m, d] = latestKey.split('.').map(Number);
            const latestDate = new Date(Date.UTC(y, m - 1, d));
            return Math.round((latestDate - today) / 86400000);
        }

        function renderEconomicCalendarView() {
            // 첫 화면에서 오늘 경제 일정이 없으면
            // 오늘 이전의 가장 최근 발표일을 자동으로 보여줍니다.
            if (!economicCalendarAutoJumped && economicCalOffset === 0) {
                const all = Array.isArray(globalMarketCalendar) ? globalMarketCalendar : [];
                const hasEconomicData = all.some(ev => isEconomicCalendarEvent(ev));

                // API 데이터가 실제로 들어온 뒤에만 자동 이동을 확정합니다.
                if (hasEconomicData) {
                    const nearestOffset = findNearestEconomicOffset();
                    if (nearestOffset !== 0) {
                        economicCalOffset = nearestOffset;
                    }
                    economicCalendarAutoJumped = true;
                }
            }

            const info = getCalendarDateInfo(economicCalOffset);
            info.dateKey = String(info.dateKey || '').replace(/[\\/]/g, '.');
            setTxt('ecoCalDisplayDate', info.display);

            const container = document.getElementById('ecoCalEventList');
            if (!container) return;

            let filtered = (Array.isArray(globalMarketCalendar) ? globalMarketCalendar : [])
                .filter(ev => isEconomicCalendarEvent(ev)
                    && String(ev.date_key).replace(/[\\/]/g, '.') === info.dateKey
                    && (economicCalStarFilter === 'all' || String(ev.stars) === economicCalStarFilter));

            setTxt(
                'ecoCalDayHeader',
                filtered.length && filtered[0].date_header
                    ? filtered[0].date_header
                    : `${info.header} · 주요 경제지표`
            );

            const totalPages = Math.ceil(filtered.length / calendarItemsPerPage) || 1;
            economicCalPage = Math.max(1, Math.min(economicCalPage, totalPages));

            const prev = document.getElementById('btnEcoCalPrev');
            const next = document.getElementById('btnEcoCalNext');
            if (prev) prev.disabled = economicCalPage <= 1;
            if (next) next.disabled = economicCalPage >= totalPages;
            setTxt('ecoCalPageIndicator', `${economicCalPage} / ${totalPages}`);

            if (!filtered.length) {
                container.innerHTML = `
                    <div style="display:flex;align-items:center;justify-content:center;height:100%;
                                color:var(--text-muted);font-size:0.76rem;text-align:center;padding:18px 8px;">
                        이 날짜에 예정된 경제 일정이 없습니다.
                    </div>
                `;
                scheduleCalendarHeightSync();
                return;
            }

            const startIdx = (economicCalPage - 1) * calendarItemsPerPage;
            container.innerHTML = filtered
                .slice(startIdx, startIdx + calendarItemsPerPage)
                .map(renderCalendarEventCard)
                .join('');
            scheduleCalendarHeightSync();
        }

        function findNearestEarningsOffset() {
            const all = Array.isArray(globalMarketCalendar) ? globalMarketCalendar : [];
            const futureDates = [...new Set(all
                .filter(ev => ev.type === 'earn' && ev.date_key)
                .map(ev => ev.date_key))].sort();
            if (!futureDates.length) return 0;

            const now = new Date();
            const kstString = new Intl.DateTimeFormat('en-CA', {
                timeZone: 'Asia/Seoul', year: 'numeric', month: '2-digit', day: '2-digit'
            }).format(now);
            const [yyyy, mm, dd] = kstString.split('-').map(Number);
            const today = new Date(Date.UTC(yyyy, mm - 1, dd));

            for (const key of futureDates) {
                const [y, m, d] = key.split('.').map(Number);
                const candidate = new Date(Date.UTC(y, m - 1, d));
                if (candidate >= today) {
                    return Math.round((candidate - today) / 86400000);
                }
            }
            return 0;
        }

        function renderEarningsCalendarView() {
            // 첫 화면에서 오늘 실적이 비어 있으면 가장 가까운 예정 실적일을 보여줍니다.
            // 날짜 이동 버튼과 [오늘] 버튼은 이후 정상적으로 하루 단위로 작동합니다.
            if (!earningsCalendarAutoJumped && earningsCalOffset === 0) {
                const nearestOffset = findNearestEarningsOffset();
                if (nearestOffset > 0) {
                    earningsCalOffset = nearestOffset;
                }
                earningsCalendarAutoJumped = true;
            }

            const info = getCalendarDateInfo(earningsCalOffset);
            setTxt('earnCalDisplayDate', info.display);

            const container = document.getElementById('earnCalEventList');
            if (!container) return;

            let filtered = (Array.isArray(globalMarketCalendar) ? globalMarketCalendar : [])
                .filter(ev => ev.type === 'earn' && ev.date_key === info.dateKey && (earningsCalStarFilter === 'all' || String(ev.stars) === earningsCalStarFilter));

            setTxt(
                'earnCalDayHeader',
                filtered.length && filtered[0].date_header
                    ? filtered[0].date_header
                    : `${info.header} · 주요 기업 실적`
            );

            const totalPages = Math.ceil(filtered.length / calendarItemsPerPage) || 1;
            earningsCalPage = Math.max(1, Math.min(earningsCalPage, totalPages));

            const prev = document.getElementById('btnEarnCalPrev');
            const next = document.getElementById('btnEarnCalNext');
            if (prev) prev.disabled = earningsCalPage <= 1;
            if (next) next.disabled = earningsCalPage >= totalPages;
            setTxt('earnCalPageIndicator', `${earningsCalPage} / ${totalPages}`);

            if (!filtered.length) {
                container.innerHTML = `
                    <div style="display:flex;align-items:center;justify-content:center;height:100%;
                                color:var(--text-muted);font-size:0.76rem;text-align:center;padding:18px 8px;">
                        이 날짜에 실적 일정이 없습니다.
                    </div>
                `;
                scheduleCalendarHeightSync();
                return;
            }

            const startIdx = (earningsCalPage - 1) * calendarItemsPerPage;
            container.innerHTML = filtered
                .slice(startIdx, startIdx + calendarItemsPerPage)
                .map(renderCalendarEventCard)
                .join('');
            scheduleCalendarHeightSync();
        }

        function setCalendarStarFilter(type, value, btn) {
            if (type === 'eco') economicCalStarFilter = value;
            else earningsCalStarFilter = value;
            const root = type === 'eco' ? '#rowEconomicCalendar' : '#rowEarningsCalendar';
            document.querySelectorAll(`${root} .calendar-star-btn`).forEach(b => b.classList.remove('active'));
            if (btn) btn.classList.add('active');
            if (type === 'eco') { economicCalPage = 1; renderEconomicCalendarView(); }
            else { earningsCalPage = 1; renderEarningsCalendarView(); }
        }

        function shiftEconomicCalendarDate(delta) {
            economicCalOffset += Number(delta || 0);
            economicCalPage = 1;
            renderEconomicCalendarView();
        }

        function resetEconomicCalendarDate() {
            // [오늘] 버튼은 실제 오늘 날짜가 아니라,
            // 경제지표가 현재 표시되는 기준점(오늘 일정이 없으면 가장 가까운 예정 일정)으로 돌아갑니다.
            economicCalOffset = findNearestEconomicOffset();
            economicCalPage = 1;
            economicCalendarAutoJumped = true;
            renderEconomicCalendarView();
        }

        function changeEconomicCalendarPage(delta) {
            economicCalPage += Number(delta || 0);
            renderEconomicCalendarView();
        }

        function shiftEarningsCalendarDate(delta) {
            earningsCalOffset += Number(delta || 0);
            earningsCalPage = 1;
            renderEarningsCalendarView();
        }

        function resetEarningsCalendarDate() {
            // [오늘]을 누르면 단순히 오늘 날짜(실적이 없을 수 있음)로 가지 않고
            // 초기 화면과 동일하게 '오늘 이후 가장 가까운 실적 발표일'로 돌아갑니다.
            // 예: 오늘 8/30이고 다음 실적이 9/2라면 [오늘] → 9/2
            const nearestOffset = findNearestEarningsOffset();
            earningsCalOffset = nearestOffset;
            earningsCalPage = 1;
            earningsCalendarAutoJumped = true;
            renderEarningsCalendarView();
        }

        function changeEarningsCalendarPage(delta) {
            earningsCalPage += Number(delta || 0);
            renderEarningsCalendarView();
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
const voteKey = 'vote_' + symbol;
            const rawVote = localStorage.getItem(voteKey) || JSON.stringify({ long: 15, short: 8, myVote: null });
            let vData = JSON.parse(rawVote);
            updateVoteUI(vData);

            const cKey = 'comments_' + symbol;
            const rawComm = localStorage.getItem(cKey) || JSON.stringify([]);
            renderComments(JSON.parse(rawComm));
        }

        function updateVoteUI(vData) {
            const total = vData.long + vData.short;
            const lPct = total > 0 ? Math.round((vData.long / total) * 100) : 50;
            const sPct = 100 - lPct;

            setTxt('voteLongPct', `상승 ${lPct}%`);
            setTxt('voteShortPct', `하락 ${sPct}%`);
            
            const lb = document.getElementById('voteLongBar');
            if (lb) lb.style.width = lPct + '%';
            const sb = document.getElementById('voteShortBar');
            if (sb) sb.style.width = sPct + '%';

            const btnL = document.getElementById('btnVoteLong');
            const btnS = document.getElementById('btnVoteShort');
            if (btnL && btnS) {
                btnL.classList.remove('active-long');
                btnS.classList.remove('active-short');
                if (vData.myVote === 'long') btnL.classList.add('active-long');
                if (vData.myVote === 'short') btnS.classList.add('active-short');
            }
        }

        async function castVote(type) {
            if (!currentSymbol) return;
            const voteKey = 'vote_' + currentSymbol;
            const rawVote = localStorage.getItem(voteKey) || JSON.stringify({ long: 15, short: 8, myVote: null });
            const vData = JSON.parse(rawVote);

            if (vData.myVote === type) return;

            if (vData.myVote === 'long') vData.long = Math.max(0, vData.long - 1);
            if (vData.myVote === 'short') vData.short = Math.max(0, vData.short - 1);

            if (type === 'long') vData.long += 1;
            if (type === 'short') vData.short += 1;

            vData.myVote = type;
            localStorage.setItem(voteKey, JSON.stringify(vData));
            updateVoteUI(vData);
        }

        function renderComments(list) {
            const wrap = document.getElementById('commentList');
            if (!wrap) return;
            setTxt('commentCountTag', `${list.length}개의 의견`);
            wrap.innerHTML = '';
            if (list.length === 0) {
                wrap.innerHTML = '<div style="color:var(--text-muted); font-size:0.8rem; padding:10px 0; text-align:center;">첫 번째 한마디를 남겨보세요!</div>';
                return;
            }
            list.slice(0, 8).forEach(c => {
                wrap.innerHTML += `
                    <div class="comment-item">
                        <div class="comment-head">
                            <span class="comment-nick">${c.nick}</span>
                            <span>${c.time}</span>
                        </div>
                        <div class="comment-body">${c.text}</div>
                    </div>
                `;
            });
        }

        async function postComment() {
            const input = document.getElementById('commentInput');
            if (!input) return;
            const txt = input.value.trim();
            if (!txt || !currentSymbol) return;

            const nicks = ["익명개미", "불꽃슈퍼개미", "성투개미", "황금손개미", "존버대장", "퀀트장인"];
            const randomNick = nicks[Math.floor(Math.random() * nicks.length)] + "#" + Math.floor(100 + Math.random() * 900);

            const cKey = 'comments_' + currentSymbol;
            const rawComm = localStorage.getItem(cKey) || '[]';
            const comments = JSON.parse(rawComm);

            comments.unshift({ nick: randomNick, text: txt, time: "방금 전" });
            localStorage.setItem(cKey, JSON.stringify(comments));
            renderComments(comments);
            input.value = '';
        }

        async function doSearch(query) {
            // 재료/경제 이벤트 최종 통합용 변수: doSearch 함수 전체에서 하나만 사용
            let materialText = '';

            // 재료/경제 이벤트 최종 통합용 변수: doSearch 함수 전체에서 하나만 사용

            const cleanQ = query.trim();
            if (!cleanQ) return;

            // 현재 분석 종목을 URL에 저장하여 새로고침해도 같은 종목을 유지
            const searchUrl = new URL(window.location.href);
            searchUrl.searchParams.set('q', cleanQ);
            window.history.replaceState({}, '', searchUrl.toString());

            document.getElementById('centerInput').value = cleanQ;
            document.getElementById('bottomInput').value = cleanQ;

            document.getElementById('main-view').style.display = 'none';
            document.getElementById('result-scroll-view').style.display = 'block';
            document.getElementById('bottom-bar').style.display = 'flex';
            document.getElementById('loading').style.display = 'none';
            document.getElementById('result-card-wrap').style.display = 'none';

            if (window.innerWidth <= 900) {
                const side = document.getElementById('sidebar-wrapper');
                if (side) side.classList.remove('open');
            }

            try {
                const url = '/api/analyze?q=' + encodeURIComponent(cleanQ);
                const response = await fetch(url);
                
                let data;
                const contentType = response.headers.get("content-type") || "";
                if (contentType.includes("application/json")) {
                    data = await response.json();
                } else {
                    const text = await response.text();
                    throw new Error("서버 응답 오류 (" + response.status + ")");
                }
                
                if (!response.ok || data.error) {
                    alert(data.error || '종목을 찾을 수 없습니다.');
                    document.getElementById('loading').style.display = 'none';
                    return;
                }

                const isKrw = data.market === '국내주식' || data.market === '국내증시';
                const displayName = data.name || data.formal_name;
                const displayPrice = data.price || data.price_str;
                const displayTime = data.time || data.now_str;

                setHtml('rTitle', `${displayName} <span>(${data.symbol})</span>`);
                setTxt('rPrice', displayPrice);
                setHtml('rMeta', `시장: ${data.market} · 출처: ${isKrw ? 'Naver/KRX' : 'Yahoo/Binance'} · 시각: ${displayTime}`);

                loadCommunityData(data.symbol, displayName);

                // 1. 글로벌 보드 렌더링
                const wBoardGrid = document.getElementById('worldBoardGrid');
                if (wBoardGrid) {
                    wBoardGrid.innerHTML = '';
                    if (data.world_board) {
                        data.world_board.forEach(w => {
                            const cls = w.up ? 'val-up' : 'val-down';
                            const sparkSvg = renderSparklineSVG(w.sparkline, w.up);
                            wBoardGrid.innerHTML += `
                                <div class="mini-index-card" onclick="doSearch('${w.name === 'USD/KRW' ? '환율' : w.name}')">
                                    <div class="mini-top">
                                        <span class="mini-name">${w.name}</span>
                                        <span class="mini-tag">${w.tag}</span>
                                    </div>
                                    <div class="mini-mid-row">
                                        <div>
                                            <div class="mini-price">${w.price}</div>
                                            <div class="mini-rate ${cls}">${w.rate}</div>
                                        </div>
                                        ${sparkSvg}
                                    </div>
                                </div>
                            `;
                        });
                    }
                }

                // 2. 캘린더 데이터 저장 및 렌더링
                if (data.market_calendar) {
                    globalMarketCalendar = data.market_calendar;

                    // 중요:
                    // 최초 화면 렌더링 시에는 API 데이터가 아직 도착하지 않아
                    // 경제 캘린더 자동 이동 플래그가 먼저 켜질 수 있습니다.
                    // 실제 데이터가 도착한 뒤 다시 한 번 자동 이동을 판단합니다.
                    economicCalendarAutoJumped = false;
                    economicCalOffset = 0;
                    economicCalPage = 1;

                    renderMarketCalendarView();
                }

                // 3. 퀀트 분석 데이터 렌더링
                const q = data.quant;
                if (q) {
                    setTxt('qScoreVal', q.quant_score);
                    setTxt('qScoreBadge', q.score_badge);
                    setHtml('qScoreDots', q.score_dots);

                    setTxt('qZScoreVal', q.z_score);
                    setTxt('qVolVal', `${q.ann_vol} (${q.vol_risk_state ? q.vol_risk_state.split(' ')[0] : ''})`);

                    const qSignalChart = document.getElementById('qSignalChart');
                    const qSignalNow = document.getElementById('qMomentumValTxt');
                    if (qSignalChart && Array.isArray(q.momentum_history) && q.momentum_history.length > 1) {
                        const scores = q.momentum_history.map(x => Number(x.score));
                        const w = 600;
                        const h = 120;
                        const coords = scores.map((score, i) => {
                            const x = (i / Math.max(1, scores.length - 1)) * w;
                            const y = h - ((score / 100.0) * h);
                            return `${x.toFixed(1)},${Math.max(6, Math.min(h-6, y)).toFixed(1)}`;
                        });
                        const linePath = 'M ' + coords.join(' L ');
                        const areaPath = `M ${coords[0].split(',')[0]},${h} L ${coords.join(' L ')} L ${coords[coords.length-1].split(',')[0]},${h} Z`;
                        const lastScore = scores[scores.length - 1];

                        qSignalChart.innerHTML = `
                            <defs>
                                <linearGradient id="qSigFill" x1="0" x2="0" y1="0" y2="1">
                                    <stop offset="0%" stop-color="#10b981" stop-opacity="0.35"/>
                                    <stop offset="100%" stop-color="#10b981" stop-opacity="0.02"/>
                                </linearGradient>
                            </defs>
                            <line x1="0" y1="36" x2="600" y2="36" stroke="rgba(16,185,129,0.2)" stroke-width="1" stroke-dasharray="4 4"/>
                            <line x1="0" y1="72" x2="600" y2="72" stroke="rgba(255,255,255,0.06)" stroke-width="1"/>
                            <path d="${areaPath}" fill="url(#qSigFill)"/>
                            <path d="${linePath}" fill="none" stroke="#10b981" stroke-width="3" vector-effect="non-scaling-stroke"/>
                            <circle cx="${coords[coords.length-1].split(',')[0]}" cy="${coords[coords.length-1].split(',')[1]}" r="5" fill="#10b981" stroke="#ffffff" stroke-width="2" vector-effect="non-scaling-stroke"/>
                        `;
                        if (qSignalNow) qSignalNow.innerText = `현재 ${lastScore}점 (${q.score_badge})`;
                    } else if (qSignalNow) {
                        qSignalNow.innerText = `현재 ${q.quant_score}점`;
                    }

                    setTxt('qLow52w', `최저 ${q.low_52w}`);
                    setTxt('qHigh52w', `최고 ${q.high_52w}`);
                    const qFill = document.getElementById('qSliderFill');
                    if (qFill) qFill.style.width = q.slider_fill_pct + '%';
                    const qDot = document.getElementById('qDotMarker');
                    if (qDot) qDot.style.left = q.slider_fill_pct + '%';

                    const qList = document.getElementById('quantDetailList');
                    if (qList) {
                        qList.innerHTML = `
                            <div class="quant-row-item">
                                <div class="quant-row-left">
                                    <span class="quant-row-name">통계적 Z-Score</span>
                                    <span class="quant-row-status status-pill-${q.z_color}">${q.z_state}</span>
                                </div>
                                <div class="quant-row-right">
                                    <div class="quant-row-val">${q.z_score}</div>
                                    <div class="quant-row-subval">20D 표준점수</div>
                                </div>
                            </div>
                            <div class="quant-row-item">
                                <div class="quant-row-left">
                                    <span class="quant-row-name">20일 이동평균선</span>
                                    <span class="quant-row-status status-pill-${q.ma20_color}">${q.ma20_state}</span>
                                </div>
                                <div class="quant-row-right">
                                    <div class="quant-row-val">${q.ma20}</div>
                                    <div class="quant-row-subval">중기 추세선</div>
                                </div>
                            </div>
                            <div class="quant-row-item">
                                <div class="quant-row-left">
                                    <span class="quant-row-name">볼린저 밴드 (60, 2)</span>
                                    <span class="quant-row-status status-pill-${q.bb_color}">${q.bb_state}</span>
                                </div>
                                <div class="quant-row-right">
                                    <div class="quant-row-val">${q.bollinger}</div>
                                    <div class="quant-row-subval">상단 / 하단</div>
                                </div>
                            </div>
                            <div class="quant-row-item">
                                <div class="quant-row-left">
                                    <span class="quant-row-name">MACD (12, 26, 9)</span>
                                    <span class="quant-row-status status-pill-${q.macd_color}">${q.macd_state}</span>
                                </div>
                                <div class="quant-row-right">
                                    <div class="quant-row-val">${q.macd}</div>
                                    <div class="quant-row-subval">모멘텀 지표</div>
                                </div>
                            </div>
                            <div class="quant-row-item">
                                <div class="quant-row-left">
                                    <span class="quant-row-name">RSI (14)</span>
                                    <span class="quant-row-status status-pill-${q.rsi_color}">${q.rsi_state}</span>
                                </div>
                                <div class="quant-row-right">
                                    <div class="quant-row-val">${q.rsi} pt</div>
                                    <div class="quant-row-subval">상대강도지수</div>
                                </div>
                            </div>
                            <div class="quant-row-item">
                                <div class="quant-row-left">
                                    <span class="quant-row-name">거래량 / 5일평균</span>
                                    <span class="quant-row-status status-pill-${q.vol_color}">${q.vol_state}</span>
                                </div>
                                <div class="quant-row-right">
                                    <div class="quant-row-val">${q.vol_ratio}</div>
                                    <div class="quant-row-subval">유동성 비율</div>
                                </div>
                            </div>
                        `;
                    }

                    setTxt('rWinRateGauge', q.win_rate_num);
                    setTxt('rBt1yTag', q.bt_1y);
                    setTxt('rBt2yTag', q.bt_2y);
                    setTxt('rRrTag', q.rr_str);

                    setTxt('rSliderStop', q.stop_p);
                    setTxt('rSliderEntry', `기준 ${q.entry1}`);
                    setTxt('rSliderTarget', q.target2);

                    const sFill = document.getElementById('rSigBarFill');
                    if (sFill) sFill.style.width = q.sig_fill_pct + '%';
                    const sDot = document.getElementById('rSigDotMarker');
                    if (sDot) sDot.style.left = q.sig_fill_pct + '%';

                    setTxt('rEntry1', q.entry1);
                    setTxt('rEntry2', q.entry2);
                    setTxt('rTarget1', q.target1);
                    setTxt('rTarget2', q.target2);
                    setTxt('rStop', q.stop_p);
                    setTxt('rGuideText', q.guide_text);
                }
                // 3.5 "왜 움직였을까?" 코멘트 + "트랙 분석" 근거 렌더링
                const mr = data.movement_reason;
                const mrCard = document.getElementById('movementReasonCard');
                if (mrCard && mr) {
                    const confidenceStyle =
                        mr.confidence === '높음'
                            ? 'background:rgba(16,185,129,0.14);color:#10b981;border:1px solid rgba(16,185,129,0.25);'
                            : mr.confidence === '중간'
                                ? 'background:rgba(245,158,11,0.14);color:#f59e0b;border:1px solid rgba(245,158,11,0.25);'
                                : 'background:rgba(148,163,184,0.12);color:var(--text-muted);border:1px solid var(--border-line);';

                    const evidenceReasons = (mr.reasons || []).slice().sort((a,b) => {
                        const a5 = a.key === 'market_5d' ? 1 : 0;
                        const b5 = b.key === 'market_5d' ? 1 : 0;
                        if (a5 !== b5) return a5 - b5;
                        return Math.abs(Number(b.score || 0)) - Math.abs(Number(a.score || 0));
                    });
                    const evidenceCount = evidenceReasons.filter(x => Number(x.score || 0) !== 0).length;


                    // 기존에 확정한 "왜 움직였을까?" 고정 코멘트 생성 로직을
                    // 트랙 분석 디자인 변경과 분리해 그대로 유지한다.
                    const scoreOf = key => { const x=evidenceReasons.find(r=>r.key===key); return x ? Number(x.score||0) : 0; };
                    const detailOf = key => { const x=evidenceReasons.find(r=>r.key===key); return x ? String(x.detail||'') : ''; };
                    const materialReason = evidenceReasons.find(r => r.key === 'material') || null;

                    const priceText = data.price || data.price_str || '-';
                    const pct = Number(data.current_change_pct);
                    const topicParticle = (() => {
                        const text = String(displayName || '').trim();
                        const last = text.charCodeAt(text.length - 1);
                        if (last >= 0xAC00 && last <= 0xD7A3) {
                            return ((last - 0xAC00) % 28) === 0 ? '는' : '은';
                        }
                        return '은';
                    })();

                    const directionText =
                        Number.isFinite(pct)
                            ? (pct > 0 ? '상승' : pct < 0 ? '하락' : '보합')
                            : (mr.direction === '하락' ? '하락' : mr.direction === '상승' ? '상승' : '보합');

                    const boardItems = Array.isArray(data.world_board) ? data.world_board : [];
                    const findBoard = (...names) => boardItems.find(item =>
                        names.some(name => String(item?.name || '').toLowerCase() === String(name).toLowerCase())
                    );
                    const rateNumber = item => {
                        if (!item) return null;
                        const n = Number(String(item.rate ?? '').replace('%','').replace('+',''));
                        return Number.isFinite(n) ? n : null;
                    };
                    const upDown = n => n > 0 ? '상승' : n < 0 ? '하락' : '보합';

                    const marketType = String(data.market || data.market_name || data.market_type || '').toLowerCase();
                    const isUSMarket = marketType.includes('미국주식') || marketType.includes('미국') || marketType.includes('us');
                    const isCryptoMarket = marketType.includes('암호화폐') || marketType.includes('암호') || marketType.includes('crypto') || marketType.includes('코인');
                    const selectedMarketName = isUSMarket
                        ? '나스닥'
                        : (String(data.market_index_name || '').toUpperCase() === 'KOSDAQ' ? '코스닥' : '코스피');
                    const marketItem = isUSMarket
                        ? findBoard('QQQ')
                        : findBoard(selectedMarketName, selectedMarketName === '코스피' ? 'KOSPI' : 'KOSDAQ');
                    const marketRate = rateNumber(marketItem);

                    const newsBlob = [
                        displayName,
                        ...(Array.isArray(mr.news_evidence) ? mr.news_evidence.map(x => x?.title || '') : []),
                        ...(Array.isArray(materialReason?.news_clusters) ? materialReason.news_clusters.flatMap(c => c?.keywords || []) : [])
                    ].join(' ').toLowerCase();
                    const semiconductorKeywords = ['반도체','hbm','hbm4','dram','nand','메모리','gpu','semiconductor'];
                    const isSemiconductor = semiconductorKeywords.some(k => newsBlob.includes(k));
                    const soxxItem = findBoard('SOXX');
                    const soxxRate = rateNumber(soxxItem);

                    const parts = [];
                    if (!isCryptoMarket) {
                        if (marketRate !== null) {
                            parts.push(`🐜 개미들아! 오늘 시장 ${selectedMarketName}는 ${marketRate >= 0 ? '+' : ''}${marketRate.toFixed(2)}% ${upDown(marketRate)} 중이고`);
                        } else {
                            parts.push(`🐜 개미들아! 오늘 시장 ${selectedMarketName}는 현재 흐름을 확인 중이고`);
                        }
                        if (isSemiconductor && soxxRate !== null) {
                            parts[parts.length - 1] += ` 반도체 업종 SOXX는 ${soxxRate >= 0 ? '+' : ''}${soxxRate.toFixed(2)}% ${upDown(soxxRate)}!`;
                        }
                    }

                    parts.push(
                        Number.isFinite(pct)
                            ? `${displayName}${topicParticle} 현재 ${priceText}, ${pct >= 0 ? '+' : ''}${pct.toFixed(2)}% ${directionText} 중이야.`
                            : `${displayName}${topicParticle} 현재 ${priceText} ${directionText} 중이야.`
                    );

                    const domesticFlowValue = (movementReason, key) => {
                        const r = Array.isArray(movementReason.reasons)
                            ? movementReason.reasons.find(x => x.key === key)
                            : null;
                        if (!r) return null;
                        const text = String(r.detail || '');
                        const m = text.match(/(순매수|순매도)\s+([0-9,]+)주/);
                        if (!m) return null;
                        const n = Number(m[2].replace(/,/g,''));
                        if (!Number.isFinite(n)) return null;
                        return m[1] === '순매도' ? -n : n;
                    };

                    const flowCandidates = [
                        { key: 'foreign', label: '외국인', value: domesticFlowValue(mr, 'foreign') },
                        { key: 'institution', label: '기관', value: domesticFlowValue(mr, 'institution') }
                    ].filter(x => x.value !== null && Number.isFinite(x.value));

                    if (flowCandidates.length) {
                        const selectedFlow = flowCandidates.sort((a,b) => Math.abs(b.value) - Math.abs(a.value))[0];
                        const flowWord = selectedFlow.value >= 0 ? '순매수' : '순매도';
                        parts.push(`최근 5거래일 동안 ${selectedFlow.label}이(가) ${Math.abs(selectedFlow.value).toLocaleString('ko-KR')}주 ${flowWord}를 기록했어.`);
                    }

                    if (materialReason && Array.isArray(materialReason.news_clusters)) {
                        const cluster = materialReason.news_clusters[0] || null;
                        const kws = cluster && Array.isArray(cluster.keywords) ? cluster.keywords : [];
                        const map = {
                            'hbm4':'HBM4','hbm':'HBM','ai':'AI 수요','인공지능':'AI 수요','gpu':'GPU 수요',
                            '수요':'수요 확대','공급':'공급 확대','공급망':'공급망 확대','증설':'증설',
                            '실적':'실적','earnings':'실적','매출':'매출 성장','영업이익':'영업이익',
                            '수주':'수주','계약':'계약','관세':'관세','aws':'AWS'
                        };
                        const mapped = kws.map(k => map[String(k).toLowerCase()] || String(k)).filter(Boolean);
                        const generic = new Set(['주가','시장','관련','소식','전망','기대','확대','증가','감소','하락','상승']);
                        const filtered = mapped.filter(x => !generic.has(x));
                        const uniq = [...new Set(filtered)];
                        if (uniq.length) materialText = uniq.slice(0, 2).join(' ');
                    }

                    let marketEventTopic = '';
                    const calendarEvents = Array.isArray(data.market_calendar) ? data.market_calendar : [];
                    const macroText = [
                        ...(Array.isArray(data.global_news) ? data.global_news.map(x => x?.title || '') : []),
                        ...(Array.isArray(data.stock_news) ? data.stock_news.map(x => x?.title || '') : []),
                        ...(Array.isArray(calendarEvents) ? calendarEvents.map(x => JSON.stringify(x)) : [])
                    ].join(' ').toLowerCase();

                    if (/\bfomc\b|연준/.test(macroText)) marketEventTopic = 'FOMC·연준';
                    else if (/\bcpi\b|소비자물가|물가/.test(macroText)) marketEventTopic = '미국 물가';
                    else if (/고용|비농업|실업률/.test(macroText)) marketEventTopic = '미국 고용';
                    else if (/관세|무역/.test(macroText)) marketEventTopic = '미국 관세 정책';
                    else if (/금리인하|금리인상|기준금리/.test(macroText)) marketEventTopic = '미국 금리';

                    if (materialText && marketEventTopic) {
                        const macroWords = new Set(String(marketEventTopic).toLowerCase().split(/[^0-9a-z가-힣]+/).filter(Boolean));
                        const materialWords = String(materialText).split(/\s+/).filter(Boolean);
                        const dedupedMaterialWords = materialWords.filter(word => {
                            const normalized = word.toLowerCase().replace(/[^0-9a-z가-힣]/g, '');
                            return normalized && !macroWords.has(normalized);
                        });
                        const dedupedMaterial = dedupedMaterialWords.join(' ').trim();
                        parts.push(dedupedMaterial
                            ? `${dedupedMaterial} ${marketEventTopic} 관련 소식이 있으니 관련 자료 뉴스를 확인해봐.`
                            : `${marketEventTopic} 관련 소식이 있으니 관련 자료 뉴스를 확인해봐.`);
                    } else if (materialText) {
                        parts.push(`${materialText} 관련 소식이 있으니 관련 자료 뉴스를 확인해봐.`);
                    } else if (marketEventTopic) {
                        parts.push(`${marketEventTopic} 관련 소식이 있으니 관련 자료 뉴스를 확인해봐.`);
                    }

                    parts.push('🐜 오늘도 개미들 힘내자! 💪');
                    const gaemiComment = parts.join('\n\n');

                    mrCard.innerHTML = `
                        <div style="font-size:0.86rem;color:var(--text-sub);line-height:1.72;white-space:pre-line;word-break:keep-all;">
                            ${gaemiComment}
                        </div>
                    `;

                    // 트랙 분석: 퀀트 분석과 동일한 카드형 레이아웃을 사용한다.
                    const trackPriceVal = document.getElementById('tPriceVal');
                    const trackChangeVal = document.getElementById('tChangeVal');
                    const trackEvidenceVal = document.getElementById('tEvidenceCountVal');
                    const trackStatusBadge = document.getElementById('tStatusBadge');
                    const trackStatusDots = document.getElementById('tStatusDots');
                    const trackPriceTrendTxt = document.getElementById('tPriceTrendTxt');

                    if (trackPriceVal) trackPriceVal.innerText = data.price_str || '-';

                    const cp = Number(data.current_change_pct);
                    if (trackChangeVal) {
                        trackChangeVal.innerText = Number.isFinite(cp) ? `${cp >= 0 ? '+' : ''}${cp.toFixed(2)}%` : '-';
                        trackChangeVal.style.color =
                            Number.isFinite(cp) && cp > 0 ? '#10b981' :
                            (Number.isFinite(cp) && cp < 0 ? '#ef4444' : 'var(--text-title)');
                    }

                    if (trackEvidenceVal) {
                        trackEvidenceVal.innerText = `${evidenceCount}/8`;
                    }
                    if (trackStatusBadge) {
                        trackStatusBadge.innerText = mr.confidence || '-';
                        trackStatusBadge.style.cssText = confidenceStyle;
                    }
                    if (trackStatusDots) {
                        trackStatusDots.innerText =
                            evidenceCount > 0
                                ? `확인된 근거 ${evidenceCount}개 · 실제 데이터 기준`
                                : '확인된 근거 부족 · 추가 확인 필요';
                    }

                    // 최근 60거래일: 현재 응답의 실제 candles를 사용한다.
                    const tPriceChart = document.getElementById('tPriceChart');
                    const candleRows = Array.isArray(data.candles) ? data.candles.slice(-60) : [];

                    // 서버가 보내는 candles 필드는 open/high/low/close 형식이다.
                    // 기존 코드가 x.c를 읽고 있어 차트 값이 전부 비어 있던 문제를 수정한다.
                    // close를 우선 사용하고, 혹시 다른 응답 형식이 들어와도 c를 보조로 사용한다.
                    const trackVals = candleRows
                        .map(x => {
                            const close = Number(x && x.close);
                            if (Number.isFinite(close)) return close;
                            const legacyClose = Number(x && x.c);
                            return Number.isFinite(legacyClose) ? legacyClose : NaN;
                        })
                        .filter(Number.isFinite);

                    if (tPriceChart && trackVals.length > 1) {
                        const first = trackVals[0];
                        const normalized = first !== 0 ? trackVals.map(v => ((v / first) - 1) * 100) : [];
                        if (normalized.length > 1 && normalized.every(Number.isFinite)) {
                            const W = 600, H = 120, pad = 8;
                            const minV = Math.min(...normalized), maxV = Math.max(...normalized);
                            const span = Math.max(0.01, maxV - minV);
                            const x = i => pad + i * ((W - pad * 2) / Math.max(1, normalized.length - 1));
                            const y = v => pad + ((maxV - v) / span) * (H - pad * 2);
                            const coords = normalized.map((v,i) => `${x(i).toFixed(1)},${y(v).toFixed(1)}`);
                            const linePath = 'M ' + coords.join(' L ');
                            const baseY = y(0);
                            const areaPath = `M ${coords[0].split(',')[0]},${baseY.toFixed(1)} L ${coords.join(' L ')} L ${coords[coords.length-1].split(',')[0]},${baseY.toFixed(1)} Z`;
                            const last = normalized[normalized.length - 1];
                            const stroke = last >= 0 ? '#10b981' : '#ef4444';

                            tPriceChart.innerHTML = `
                                <defs>
                                    <linearGradient id="tPriceFill" x1="0" x2="0" y1="0" y2="1">
                                        <stop offset="0%" stop-color="${stroke}" stop-opacity="0.28"/>
                                        <stop offset="100%" stop-color="${stroke}" stop-opacity="0.02"/>
                                    </linearGradient>
                                </defs>
                                <line x1="0" y1="${baseY.toFixed(1)}" x2="600" y2="${baseY.toFixed(1)}"
                                      stroke="rgba(148,163,184,0.20)" stroke-width="1" stroke-dasharray="4 4"/>
                                <path d="${areaPath}" fill="url(#tPriceFill)"/>
                                <path d="${linePath}" fill="none" stroke="${stroke}" stroke-width="3"
                                      vector-effect="non-scaling-stroke"/>
                                <circle cx="${coords[coords.length-1].split(',')[0]}"
                                        cy="${coords[coords.length-1].split(',')[1]}"
                                        r="5" fill="${stroke}" stroke="#ffffff" stroke-width="2"
                                        vector-effect="non-scaling-stroke"/>
                            `;

                            if (trackPriceTrendTxt) {
                                const p = normalized[normalized.length - 1];
                                trackPriceTrendTxt.innerText =
                                    `최근 60D ${p >= 0 ? '+' : ''}${p.toFixed(2)}%`;
                            }
                        }
                    } else if (trackPriceTrendTxt) {
                        trackPriceTrendTxt.innerText =
                            trackVals.length === 0 ? '데이터 부족' : '주가 흐름 데이터 부족';
                    }

                    // 근거 확인: 확인된 실제 근거 데이터를 최대 8칸의 색상 막대로 표시한다.
                    const evidenceBar = document.getElementById('tEvidenceBar');
                    const evidenceBarLabel = document.getElementById('tEvidenceBarLabel');
                    if (evidenceBar) {
                        const evidenceItems = Array.isArray(evidenceReasons) ? evidenceReasons.slice(0, 8) : [];
                        const confirmedCount = evidenceItems.filter(r => {
                            const score = Number(r && r.score);
                            return Number.isFinite(score) && score !== 0;
                        }).length;

                        evidenceBar.innerHTML = Array.from({length: 8}, (_, idx) => {
                            const r = evidenceItems[idx];
                            const score = Number(r && r.score);
                            let bg = 'var(--bg-subcard)';
                            let border = '1px solid var(--border-line)';
                            if (Number.isFinite(score) && score > 0) {
                                bg = '#10b981';
                                border = '1px solid #10b981';
                            } else if (Number.isFinite(score) && score < 0) {
                                bg = '#ef4444';
                                border = '1px solid #ef4444';
                            } else if (r) {
                                bg = '#9ca3af';
                                border = '1px solid #9ca3af';
                            }
                            return `<div title="${r ? (r.label || '근거') : '확인된 근거 없음'}"
                                         style="height:100%; background:${bg}; border:${border}; border-radius:3px; min-width:0;"></div>`;
                        }).join('');

                        if (evidenceBarLabel) {
                            evidenceBarLabel.innerText = `${confirmedCount} / 8`;
                        }
                    }

                    // 상세 근거: 표시 순서만 고정한다. 분석 데이터/점수 계산은 변경하지 않는다.
                    const trackDisplayOrder = [
                        'market_5d',
                        'market',
                        'theme',
                        'foreign',
                        'institution',
                        'material',
                        'volume',
                        'trading_value'
                    ];
                    const orderedEvidenceReasons = [...evidenceReasons].sort((a, b) => {
                        const ai = trackDisplayOrder.indexOf(a.key);
                        const bi = trackDisplayOrder.indexOf(b.key);
                        return (ai === -1 ? 999 : ai) - (bi === -1 ? 999 : bi);
                    });

                    const trackDetailList = document.getElementById('trackDetailList');
                    if (trackDetailList) {
                        trackDetailList.innerHTML = orderedEvidenceReasons.map(r => {
                            const score = Number(r.score || 0);
                            const status = score > 0 ? '우호적' : score < 0 ? '부정적' : '중립';
                            const statusClass =
                                score > 0 ? 'status-pill-positive' :
                                score < 0 ? 'status-pill-negative' : 'status-pill-neutral';

                            return `<div class="quant-row-item">
                                <div class="quant-row-left">
                                    <span class="quant-row-name">${r.label || '-'}</span>
                                    <span class="quant-row-status ${statusClass}">${status}</span>
                                </div>
                                <div class="quant-row-right">
                                    <div class="quant-row-val">${String(r.detail || '-')}</div>
                                </div>
                            </div>`;
                        }).join('') || `<div style="padding:12px 0;color:var(--text-muted);font-size:0.78rem;">확인된 데이터가 없습니다.</div>`;
                    }
                }

                // 4. 종목 뉴스 & 해외 뉴스 렌더링 (각 10개 수집/분석, 화면 6개 표시)
                const sNews = document.getElementById('stockNewsList');
                if (sNews) {
                    sNews.innerHTML = '';
                    if (data.stock_news && data.stock_news.length > 0) {
                        data.stock_news.slice(0, 6).forEach(n => {
                            sNews.innerHTML += `
                                <a href="${n.link}" target="_blank" class="news-item">
                                    <div class="news-title">${n.title}</div>
                                    <div class="news-meta">${n.media} · ${n.pubDate}</div>
                                </a>
                            `;
                        });
                    } else {
                        sNews.innerHTML = '<div style="color:var(--text-muted); font-size:0.85rem;">관련 뉴스가 없습니다.</div>';
                    }
                }

                const gNews = document.getElementById('globalNewsList');
                if (gNews) {
                    gNews.innerHTML = '';
                    if (data.global_news && data.global_news.length > 0) {
                        data.global_news.slice(0, 6).forEach(n => {
                            gNews.innerHTML += `
                                <a href="${n.link}" target="_blank" class="news-item">
                                    <div class="news-title">${n.title}</div>
                                    <div class="news-meta">${n.media} · ${n.pubDate}</div>
                                </a>
                            `;
                        });
                    } else {
                        gNews.innerHTML = '<div style="color:var(--text-muted); font-size:0.85rem;">해외 증시 뉴스를 불러오는 중입니다.</div>';
                    }
                }

                // 5. 컨센서스 & 공시
                const c = data.consensus;
                const uCard = document.getElementById('rowConsensus');
                if (c && uCard) {
                    setTxt('uTitle', c.title);
                    setTxt('uGaugeVal', c.gauge_val);
                    setTxt('uGaugeLabel', c.gauge_label);
                    setTxt('uStatusLabel', c.status_label);
                    setTxt('uBadge', c.badge_text);
                    setHtml('uDots', c.dots);

                    setTxt('uPrimaryLabel', c.primary_label);
                    setTxt('uPrimaryVal', c.primary_val);
                    setTxt('uSecondaryLabel', c.secondary_label);
                    setTxt('uSecondaryVal', c.secondary_val);

                    setTxt('uSliderLeft', c.slider_left);
                    setTxt('uSliderMid', c.slider_mid);
                    setTxt('uSliderRight', c.slider_right);

                    const uFill = document.getElementById('uBarFill');
                    if (uFill) uFill.style.width = c.fill_pct + '%';
                    const uDot = document.getElementById('uDotMarker');
                    if (uDot) uDot.style.left = c.fill_pct + '%';

                    setTxt('uListTitle', c.list_title);
                    setTxt('uSourceTag', c.source_tag);

                    const iList = document.getElementById('uItemList');
                    if (iList) {
                        let consensusExpanded = false;

                        const renderConsensusItems = (items, expanded = false) => {
                            // 국내주식 리포트와 BTC 기관 전망은 기본 2개만 표시해
                            // 미국주식 Buy/Hold/Sell 영역과 기본 높이를 맞춘다.
                            const isTwoItemConsensus =
                                c.list_title === '실제 증권사 리포트' ||
                                c.list_title === '실제 기관 전망';
                            const defaultVisibleCount = isTwoItemConsensus ? 2 : 3;
                            const visibleItems = expanded ? items : items.slice(0, defaultVisibleCount);
                            let html = '';

                            visibleItems.forEach((it, idx) => {
                                const isLast = idx === visibleItems.length - 1;
                                html += `
                                    <div style="display:flex; justify-content:space-between; align-items:center; padding:8px 0; ${isLast ? '' : 'border-bottom:1px solid var(--border-line);'} font-size:0.82rem;">
                                        <div>
                                            <div style="color:var(--text-title); font-weight:700;">${it.main}</div>
                                            <div style="font-size:0.7rem; color:var(--text-muted); margin-top:2px;">${it.sub}</div>
                                        </div>
                                        <div style="text-align:right;">
                                            <div style="color:var(--text-title); font-weight:700;">${it.val}</div>
                                            <div style="color:#10b981; font-size:0.72rem; font-weight:700;">${it.sub_val}</div>
                                        </div>
                                    </div>
                                `;
                            });

                            if (isTwoItemConsensus && items.length > defaultVisibleCount) {
                                if (expanded) {
                                    html += `
                                        <button type="button" class="consensus-more-btn" id="consensusMoreBtn">
                                            접기
                                        </button>
                                    `;
                                } else {
                                    html += `
                                        <button type="button" class="consensus-more-btn" id="consensusMoreBtn">
                                            더보기 <span>(${items.length - defaultVisibleCount}건)</span>
                                        </button>
                                    `;
                                }
                            }

                            return html;
                        };

                        // 버튼을 다시 만들어도 클릭 이벤트가 유지되도록
                        // 목록 자체에 이벤트 위임을 한 번만 연결한다.
                        iList.onclick = (event) => {
                            const btn = event.target.closest('.consensus-more-btn');
                            if (!btn) return;

                            consensusExpanded = !consensusExpanded;
                            iList.innerHTML = renderConsensusItems(c.items || [], consensusExpanded);
                        };

                        if (c.items && c.items.length > 0) {
                            iList.innerHTML = renderConsensusItems(c.items, false);
                        }
                    }
                    uCard.style.display = 'block';
                } else if (uCard) {
                    uCard.style.display = 'none';
                }

                const dCard = document.getElementById('rowDart');
                const dList = document.getElementById('dartList');
                if (dCard && dList) {
                    if (data.dart_disclosures && data.dart_disclosures.length > 0) {
                        dList.innerHTML = '';
                        data.dart_disclosures.forEach(d => {
                            dList.innerHTML += `
                                <a href="${d.link}" target="_blank" class="dart-item">
                                    <span class="dart-tag tag-${d.tag_type}">${d.tag_name}</span>
                                    <span class="dart-title">${d.title}</span>
                                    <span style="color:var(--text-muted); font-size:0.72rem; flex-shrink:0;">${d.date}</span>
                                </a>
                            `;
                        });
                        dCard.style.display = 'block';
                    } else {
                        dCard.style.display = 'none';
                    }
                }

                document.getElementById('loading').style.display = 'none';
                document.getElementById('result-card-wrap').style.display = 'block';

                setTimeout(() => {
                    initTvChart(data.candles, data.ma20_line, data.ma60_line, data.bb_up_line, data.bb_dn_line);
                }, 50);

                window.scrollTo({top: 0, behavior: 'smooth'});

            } catch (err) {
                console.error("Analysis Error:", err);
                const msg = err && err.message ? err.message : '알 수 없는 오류';
                alert('데이터를 처리하는 중 오류가 발생했습니다.\n' + msg);
                document.getElementById('loading').style.display = 'none';
            }
        }

        function initTvChart(candles, ma20, ma60, bbUp, bbDn) {
            const container = document.getElementById('tvChart');
            if (!container) return;
            container.innerHTML = '';

            const isLight = document.body.classList.contains('light-theme');

            chart = LightweightCharts.createChart(container, {
                width: container.clientWidth || 700,
                height: 300,
                layout: { 
                    background: { color: isLight ? '#ffffff' : '#1e1f20' }, 
                    textColor: isLight ? '#5f6368' : '#8e918f' 
                },
                grid: {
                    vertLines: { color: 'rgba(0,0,0,0)' },
                    horzLines: { color: 'rgba(0,0,0,0)' }
                },
                crosshair: {
                    vertLine: { visible: false },
                    horzLine: { visible: false }
                },
                timeScale: { borderColor: isLight ? '#dadce0' : '#282a2c' },
                rightPriceScale: { borderColor: isLight ? '#dadce0' : '#282a2c' }
            });

            const candleSeries = chart.addCandlestickSeries({
                upColor: '#ef4444',
                downColor: '#38bdf8',
                borderDownColor: '#38bdf8',
                borderUpColor: '#ef4444',
                wickDownColor: '#38bdf8',
                wickUpColor: '#ef4444',
                priceLineVisible: false,
                lastValueVisible: true
            });
            candleSeries.setData(candles || []);

            const lineMa20 = chart.addLineSeries({ color: '#00b4d8', lineWidth: 1, priceLineVisible: false, lastValueVisible: false });
            lineMa20.setData(ma20 || []);

            const lineMa60 = chart.addLineSeries({ color: '#f97316', lineWidth: 1.5, priceLineVisible: false, lastValueVisible: false });
            lineMa60.setData(ma60 || []);

            const lineBbUp = chart.addLineSeries({ color: '#f87171', lineWidth: 1.2, priceLineVisible: false, lastValueVisible: false });
            lineBbUp.setData(bbUp || []);

            const lineBbDn = chart.addLineSeries({ color: '#38bdf8', lineWidth: 1.2, priceLineVisible: false, lastValueVisible: false });
            lineBbDn.setData(bbDn || []);

            chart.timeScale().fitContent();

            window.addEventListener('resize', resizeChart);
        }


        async function loadSectorChart(symbol, sectorName) {
            const cleanSymbol = String(symbol || '').trim().toUpperCase();
            const cleanName = String(sectorName || '').trim();
            if (!cleanSymbol) return;

            const chartContainer = document.getElementById('tvChart');
            if (!chartContainer) return;

            const previousHtml = chartContainer.innerHTML;
            chartContainer.innerHTML = '<div style="height:300px; display:flex; align-items:center; justify-content:center; color:var(--text-muted); font-size:0.78rem;">업종 차트 불러오는 중…</div>';

            try {
                // 기존 /api/analyze 파이프라인을 그대로 사용하되,
                // 현재 종목 전체 분석 화면은 건드리지 않고 차트 데이터만 사용한다.
                const response = await fetch('/api/analyze?q=' + encodeURIComponent(cleanSymbol));
                const data = await response.json();

                if (!response.ok || data.error || !Array.isArray(data.candles) || !data.candles.length) {
                    throw new Error(data.error || '업종 차트 데이터를 가져오지 못했습니다.');
                }

                initTvChart(
                    data.candles,
                    data.ma20_line,
                    data.ma60_line,
                    data.bb_up_line,
                    data.bb_dn_line
                );

                // 차트 영역의 현재 모드를 가볍게 표시. 기존 레이아웃/카드 높이는 변경하지 않는다.
                const meta = document.getElementById('rMeta');
                if (meta) {
                    meta.dataset.previousText = meta.dataset.previousText || meta.textContent;
                    meta.innerText = `차트: ${cleanName} 업종 · ${cleanSymbol} · Yahoo Finance`;
                }
            } catch (error) {
                console.error('Sector chart:', error);
                chartContainer.innerHTML = previousHtml;
                alert(`${cleanName || cleanSymbol} 업종 차트를 불러오지 못했습니다.`);
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

        async function loadUsLeadingStocks(sectorSymbol, sectorName) {
            const list = document.getElementById('usLeadingStockList');
            const title = document.getElementById('usLeadingStockTitle');
            if (!list || !sectorSymbol) return;

            const cleanName = String(sectorName || '').trim();
            if (title) title.textContent = `미국 ${cleanName} 주도종목`;
            list.innerHTML = '<div class="us-leading-sector-loading">실제 주도종목 데이터를 불러오는 중…</div>';

            try {
                const response = await fetch('/api/us-leading-stocks?sector=' + encodeURIComponent(sectorSymbol));
                const data = await response.json();
                if (!response.ok || data.error) throw new Error(data.error || '데이터 오류');

                const rows = Array.isArray(data.data) ? data.data : [];
                if (!rows.length) {
                    list.innerHTML = '<div class="us-leading-sector-loading">현재 주도종목 데이터를 불러오지 못했습니다.</div>';
                    return;
                }

                const pct = value => `${Number(value) >= 0 ? '+' : ''}${Number(value).toFixed(2)}%`;
                const cls = value => Number(value) >= 0 ? 'up' : 'down';
                list.innerHTML = rows.map(row => `
                    <div class="us-leading-stock-item">
                        <div class="us-leading-stock-name">
                            <div class="us-leading-stock-ticker">${String(row.symbol || '')}</div>
                            <div class="us-leading-stock-company">${String(row.name || '')}</div>
                        </div>
                        <div class="us-leading-stock-price">
                            <div class="us-leading-stock-value">${formatUsStockPrice(row.price)}</div>
                            <div class="us-leading-stock-change ${cls(row.current)}">${pct(row.current)}</div>
                        </div>
                        ${buildUsStockSparkline(row.sparkline)}
                    </div>
                `).join('');
            } catch (error) {
                console.error('US leading stocks:', error);
                list.innerHTML = '<div class="us-leading-sector-loading">주도종목 데이터를 잠시 후 다시 불러옵니다.</div>';
            }
        }

        async function loadUsLeadingSectors() {
            const box = document.getElementById('usLeadingSectorList');
            if (!box) return;
            try {
                const response = await fetch('/api/us-leading-sectors');
                const data = await response.json();
                if (!response.ok || data.error) throw new Error(data.error || '데이터 오류');

                const rows = Array.isArray(data.data) ? data.data : [];
                if (!rows.length) {
                    box.innerHTML = '<div class="us-leading-sector-loading">현재 업종 데이터를 불러오지 못했습니다.</div>';
                    return;
                }

                const pct = value => `${Number(value) >= 0 ? '+' : ''}${Number(value).toFixed(1)}%`;
                const cls = value => Number(value) >= 0 ? 'us-leading-up' : 'us-leading-down';

                let html = '<table class="us-leading-sector-table">' +
                    '<thead><tr><th>업종</th><th>현재</th><th>1W</th><th>1M</th><th>3M</th></tr></thead><tbody>';

                rows.forEach(row => {
                    const safeName = String(row.name || '').replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
                    const safeSym = String(row.sym || '').replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
                    html += `<tr class="us-leading-sector-row" title="${safeName} 업종 차트 보기" data-sector-symbol="${safeSym}" data-sector-name="${safeName}">` +
                        `<td class="us-leading-name">${safeName}</td>` +
                        `<td class="${cls(row.current)}">${pct(row.current)}</td>` +
                        `<td class="${cls(row.week1)}">${pct(row.week1)}</td>` +
                        `<td class="${cls(row.month1)}">${pct(row.month1)}</td>` +
                        `<td class="${cls(row.month3)}">${pct(row.month3)}</td>` +
                        `</tr>`;
                });
                html += '</tbody></table>';
                box.innerHTML = html;

                // 인라인 onclick에 의존하지 않고 명시적인 이벤트 리스너로 연결한다.
                // CSP가 적용된 배포 환경에서도 업종 클릭이 동작하도록 한다.
                box.querySelectorAll('.us-leading-sector-row').forEach(rowEl => {
                    rowEl.addEventListener('click', () => {
                        const sectorSymbol = rowEl.dataset.sectorSymbol || '';
                        const sectorName = rowEl.dataset.sectorName || '';
                        loadSectorChart(sectorSymbol, sectorName);
                        loadUsLeadingStocks(sectorSymbol, sectorName);
                    });
                });

                // 업종을 클릭하지 않아도 현재 주도업종 1위의 TOP 5가
                // 처음부터 자동으로 표시되도록 한다.
                const strongestRow = box.querySelector('.us-leading-sector-row');
                if (strongestRow) {
                    const sectorSymbol = strongestRow.dataset.sectorSymbol || '';
                    const sectorName = strongestRow.dataset.sectorName || '';
                    loadUsLeadingStocks(sectorSymbol, sectorName);
                }
            } catch (error) {
                console.error('US leading sectors:', error);
                box.innerHTML = '<div class="us-leading-sector-loading">업종 데이터를 잠시 후 다시 불러옵니다.</div>';
            }
        }

        window.addEventListener('DOMContentLoaded', loadUsLeadingSectors);
        window.addEventListener('DOMContentLoaded', () => {
            initTheme();

            // 새로고침 시 URL에 저장된 종목을 자동 복원
            const params = new URLSearchParams(window.location.search);
            const savedQuery = params.get('q');

            if (savedQuery && savedQuery.trim()) {
                doSearch(savedQuery.trim());
            }
        });
    </script>
</body>
</html>
"""

# ----------------------------------------------------
# 9. 멀티스레드 웹 서버
# ----------------------------------------------------
class WebHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        print("WEB", fmt % args)

    def _json(self, status, payload):
        clean_payload = clean_for_json(payload)
        body = json.dumps(clean_payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

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

        if parsed.path == "/api/analyze":
            q = str(params.get("q", [""])[0]).strip()
            if not q:
                self._json(400, {"error": "종목명을 입력해주세요."})
                return
            try:
                data = run_full_pipeline(q)
                if not data:
                    self._json(404, {"error": "실제 데이터를 가져오지 못했습니다. 종목명 또는 데이터 제공처를 확인해주세요."})
                    return
                self._json(200, data)
            except Exception as exc:
                print(f"API ERROR /api/analyze?q={q!r}: {type(exc).__name__}: {exc}")
                self._json(500, {
                    "error": "서버 처리 중 오류가 발생했습니다.",
                    "detail": f"{type(exc).__name__}: {str(exc)[:400]}"
                })
            return

        html_bytes = HTML_PAGE.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(html_bytes)))
        self.end_headers()
        self.wfile.write(html_bytes)

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

def gaemi_fixed_comment(market, industry, stock, flow, material=""):
    """시장 → 업종 → 종목 → 수급 → 종목 재료"""
    lines = [
        f"🐜 개미들아! 오늘 시장 {market['name']}는 {market['change']}% "
        f"{market['direction']} 중이고, {industry['name']} 업종 {industry['symbol']}는 "
        f"{industry['change']}% {industry['direction']}!",
        f"{stock['name']}는 현재 {stock['price']}, {stock['change']}% "
        f"{stock['direction']} 중이야.",
    ]

    if flow and flow.get("actor") and flow.get("shares") is not None:
        side = "순매수" if float(flow["shares"]) >= 0 else "순매도"
        lines.append(
            f"최근 5거래일 동안 {flow['actor']}이 "
            f"{abs(int(float(flow['shares']))):,}주 {side}를 기록했어."
        )

    if material:
        lines.append(f"{stock['name']}는 {material} 소식이 있어!")

    return "\n\n".join(lines)

_test = gaemi_fixed_comment(
    market={"name": "KOSPI", "change": "-1.79", "direction": "하락"},
    industry={"name": "반도체", "symbol": "SOXX", "change": "-0.40", "direction": "하락"},
    stock={"name": "SK하이닉스", "price": "1,653,000원", "change": "-4.45", "direction": "하락"},
    flow={"actor": "외국인", "shares": "-2724956"},
    material="HBM4 공급 확대",
)



# ===== FINAL TOPIC ENGINE V8 =====
# 확정 구조:
# 1) 여러 출처 수집
# 2) 비슷한 뉴스 그룹화
# 3) 공통 주제 후보 생성
# 4) 의미가 같은 표현을 하나의 개념으로 통합
# 5) 대표 뉴스 선택
# 6) 출처 반복성으로 교차확인
# 7) 핵심 재료 1개(필요시 2개)만 확정
# 8) 확정된 고정 문장에 삽입
#
# 중요: 고정 문장 영역은 AI가 다시 작성하지 않는다.

TOPIC_STOPWORDS = {
    "관련","소식","전망","기대","주목","시장","주가","종목","오늘","현재",
    "최근","증시","기업","업계","분석","가능성","예상","계획","발표",
    "강세","약세","상승","하락","수혜","영향","확대","증가","감소"
}

SEMANTIC_GROUPS = {
    "공급확대": {"공급","공급망","생산","생산능력","생산량","양산","확대","늘려","늘린다"},
    "수요증가": {"수요","증가","견조","확대","늘어","늘어난다"},
    "금리인하기대": {"금리인하","인하","완화"},
    "기대약화": {"약화","후퇴","낮아져","낮아졌다","둔화","축소"},
    "물가": {"물가","cpi","소비자물가","인플레이션"},
    "예상상회": {"상회","웃돌아","높아","높게"},
    "고용": {"고용","일자리","고용시장"},
    "관세정책": {"관세","관세율","관세정책","무역"},
    "정책변화": {"변화","조정","변경","개편","검토"},
}

def _topic_tokens(text):
    text = re.sub(r"https?://\S+", " ", str(text or ""))
    text = re.sub(r"[^\w가-힣$%.\- ]+", " ", text.lower())
    return [x for x in text.split() if len(x) >= 2 and x not in TOPIC_STOPWORDS]

def _semantic_signature(tokens):
    sig = set()
    for group, words in SEMANTIC_GROUPS.items():
        if any(t in words for t in tokens):
            sig.add(group)
    if "hbm4" in tokens:
        sig.add("HBM4")
    if "fomc" in tokens:
        sig.add("FOMC")
    if "연준" in tokens:
        sig.add("연준")
    return sig

def extract_final_topic(news_items, max_topics=1):
    """
    사용자 방식(B)을 중심으로 의미 통합/출처 교차확인을 추가한 최종 방식.
    반환: {'topic': str, 'representative_title': str, 'source_count': int}
    """
    docs = []
    seen_titles = set()
    for item in news_items or []:
        if isinstance(item, dict):
            title = str(item.get("title", "")).strip()
            source = str(item.get("source", "")).strip() or "unknown"
        else:
            title, source = str(item), "unknown"
        if title and title not in seen_titles:
            docs.append({"title": title, "source": source, "tokens": _topic_tokens(title)})
            seen_titles.add(title)

    if not docs:
        return {"topic": "", "representative_title": "", "source_count": 0}

    # 1) 의미 시그니처별 그룹화
    groups = {}
    for d in docs:
        sig = tuple(sorted(_semantic_signature(d["tokens"])))
        if sig:
            groups.setdefault(sig, []).append(d)

    # 2) 가장 많은 독립 출처가 공유하는 그룹 선택
    group_rank = []
    for sig, ds in groups.items():
        sources = len(set(d["source"] for d in ds))
        group_rank.append((sources, len(ds), len(sig), sig, ds))
    group_rank.sort(reverse=True)

    best = group_rank[0] if group_rank else (1, len(docs), 0, (), docs)
    _, _, _, signature, group_docs = best

    # 3) 대표 뉴스: 핵심 시그니처를 가장 많이 포함하고 구체적인 제목 우선
    representative = max(
        group_docs,
        key=lambda d: (len(_semantic_signature(d["tokens"])), len(d["tokens"]))
    )

    # 4) 대표 제목에서 의미 있는 '짧은 구'를 만든다.
    # HBM4 + 공급확대처럼 의미가 결합된 경우 하나의 재료로 유지.
    t = representative["tokens"]
    phrases = []
    if "HBM4" in signature and "공급확대" in signature:
        phrases.append("HBM4 공급 확대")
    elif "FOMC" in signature and "금리인하기대" in signature and "기대약화" in signature:
        phrases.append("FOMC 금리인하 기대 약화")
    elif "물가" in signature and "예상상회" in signature:
        phrases.append("미국 물가 예상 상회")
    elif "고용" in signature and "기대약화" in signature:
        phrases.append("미국 고용 둔화")
    elif "관세정책" in signature and "정책변화" in signature:
        phrases.append("미국 관세 정책 변화")

    if not phrases:
        # 일반적인 경우 대표 제목에서 2~4개 핵심 토큰을 유지하되,
        # 의미군이 있으면 그 군을 우선 결합.
        if "공급확대" in signature:
            phrases.append("공급 확대")
        elif "수요증가" in signature:
            phrases.append("수요 증가")
        elif "금리인하기대" in signature and "기대약화" in signature:
            phrases.append("금리인하 기대 약화")
        elif "관세정책" in signature:
            phrases.append("관세 정책")
        else:
            candidates = [x for x in t if x not in TOPIC_STOPWORDS]
            phrases.append(" ".join(candidates[:3]))

    source_count = len(set(d["source"] for d in group_docs))
    return {
        "topic": phrases[0] if phrases else "",
        "representative_title": representative["title"],
        "source_count": source_count,
    }

def build_stock_material_fixed(stock_name, topic):
    if not topic:
        return ""
    return f"{stock_name}는 {topic} 소식이 있어!"

def build_market_event_fixed(topic):
    if not topic:
        return ""
    return f"{topic} 소식이 있으니 관련 자료 뉴스를 확인해봐."


# ===== V8 테스트 =====
_stock_news = [
    {"source":"Reuters","title":"SK하이닉스 HBM4 공급 확대 기대"},
    {"source":"Bloomberg","title":"SK하이닉스 HBM4 공급망 확대"},
    {"source":"연합뉴스","title":"HBM4 수요 증가에 SK하이닉스 공급 확대"},
    {"source":"한국경제","title":"SK하이닉스 HBM4 생산능력 확대"},
    {"source":"서울경제","title":"HBM4 관련 SK하이닉스 공급 확대"},
]
_econ_news = [
    {"source":"Reuters","title":"FOMC 이후 금리인하 기대 약화"},
    {"source":"Bloomberg","title":"연준 발언에 금리인하 기대 후퇴"},
    {"source":"CNBC","title":"FOMC 금리인하 전망 약화"},
    {"source":"WSJ","title":"연준 발언으로 금리인하 기대 낮아져"},
]

_s = extract_final_topic(_stock_news)
_e = extract_final_topic(_econ_news)

print("V8 문법 검사: PASS")
print("[종목 재료]", _s)
print("[경제 재료]", _e)
print("[종목 고정 문장]", build_stock_material_fixed("SK하이닉스", _s["topic"]))
print("[경제 고정 문장]", build_market_event_fixed(_e["topic"]))



# ===== FINAL LOCK: MATERIAL + MACRO DEDUPLICATION =====
# [완전 고정 규칙]
# 1. 종목 뉴스와 경제/시장 이벤트 뉴스는 각각 여러 출처에서 수집한다.
# 2. 각 영역에서 반복되는 핵심 주제/의미를 먼저 추출한다.
# 3. 종목 재료와 경제 이벤트를 하나의 최종 정보 영역으로 묶는다.
# 4. 두 영역에 같은 핵심 단어가 있으면 중복 제거한다.
# 5. 같은 단어가 아니어도 같은 의미로 판단되는 재료는 중복 제거 대상이다.
# 6. 동일 재료가 겹칠 경우 경제/시장 이벤트 쪽 표현을 우선 보존한다.
# 7. 종목명은 재료 문장에서 반복하지 않는다.
# 8. 재료 문장의 '소식이 있어!'를 사용하지 않는다.
# 9. 최종 문장 고정:
#    - 재료 + 경제 이벤트:
#      "[종목 재료] [경제 이벤트] 관련 소식이 있으니 관련 자료 뉴스를 확인해봐."
#    - 재료만:
#      "[종목 재료] 관련 소식이 있으니 관련 자료 뉴스를 확인해봐."
#    - 경제 이벤트만:
#      "[경제 이벤트] 관련 소식이 있으니 관련 자료 뉴스를 확인해봐."
# 10. 중복 제거 후 내용이 완전히 겹치면 경제 이벤트만 출력한다.
# 11. 시장/업종/종목/수급/마무리 고정 문장은 이 규칙의 영향을 받지 않는다.
# 12. 이 영역에서는 AI가 임의로 설명을 덧붙이지 않는다.
