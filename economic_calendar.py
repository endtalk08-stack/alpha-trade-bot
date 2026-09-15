"""Economic calendar formatting/filter helpers extracted from bot.py."""
from datetime import datetime, timezone, timedelta
try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None

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
