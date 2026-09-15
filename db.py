# gaemiGTP 1site - PostgreSQL DB layer
# Extracted from the verified 1site baseline.
import os
import json
import threading
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

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

# ----------------------------------------------------
# DB 비동기 저장 큐
# - 사용자 응답 경로에서 PostgreSQL INSERT/UPSERT를 기다리지 않는다.
# - DB 저장은 별도 worker가 처리하며, 저장 실패가 분석 응답을 막지 않는다.
# - 동일 요청의 중복 DB 작업이 쌓이는 것을 막기 위해 pending key를 관리한다.
# ----------------------------------------------------
DB_WRITE_EXECUTOR = ThreadPoolExecutor(
    max_workers=2,
    thread_name_prefix="gaemi-db",
)
DB_WRITE_PENDING = set()
DB_WRITE_PENDING_LOCK = threading.Lock()


def _submit_db_write(task_key, func, *args, **kwargs):
    """DB 저장을 백그라운드 worker로 넘기고 호출자는 즉시 계속 진행한다."""
    if not _db_available():
        return False

    with DB_WRITE_PENDING_LOCK:
        if task_key in DB_WRITE_PENDING:
            print(f"[DB ASYNC] 중복 작업 건너뜀: {task_key}")
            return False
        DB_WRITE_PENDING.add(task_key)

    try:
        future = DB_WRITE_EXECUTOR.submit(func, *args, **kwargs)
    except Exception as exc:
        with DB_WRITE_PENDING_LOCK:
            DB_WRITE_PENDING.discard(task_key)
        print(f"[DB ASYNC] 작업 등록 실패: {task_key} {type(exc).__name__}: {exc}")
        return False

    def _done_callback(done_future):
        try:
            done_future.result()
        except Exception as exc:
            print(f"[DB ASYNC] 작업 실패: {task_key} {type(exc).__name__}: {exc}")
        finally:
            with DB_WRITE_PENDING_LOCK:
                DB_WRITE_PENDING.discard(task_key)

    future.add_done_callback(_done_callback)
    return True


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
# SPEED OPTIMIZATION 5: 실제 병목 측정
# 각 요청에서 외부 API별 소요시간을 기록한다.
# 화면 동작에는 영향을 주지 않고, /api/analyze와 /api/chart 응답에 _perf로 포함한다.
# ----------------------------------------------------


