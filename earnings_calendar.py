"""Earnings timestamp helpers extracted from bot.py."""
from datetime import datetime, timezone
try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None

from performance import HTTP_SESSION

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
