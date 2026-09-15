import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor

import numpy as np

from performance import HTTP_SESSION


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
