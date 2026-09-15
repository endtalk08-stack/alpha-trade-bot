"""공통 숫자 변환 유틸리티."""

def _safe_float(value):
    try:
        if value is None:
            return None
        if isinstance(value, str):
            value = value.replace(",", "").replace("%", "").strip()
        return float(value)
    except Exception:
        return None
