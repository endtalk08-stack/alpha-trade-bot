import numpy as np
import pandas as pd

def _build_chart_payload(df):
    """차트는 캔들 + 거래량만 표시한다. 이평선/볼린저밴드는 전송하지 않는다."""
    if df is None or df.empty:
        return None

    work = df.copy().sort_index()
    work = work[~work.index.duplicated(keep="last")]

    candles = []
    for date_idx, row in work.tail(65).iterrows():
        t_str = pd.to_datetime(date_idx).strftime("%Y-%m-%d")
        o = float(row["Open"])
        h = float(row["High"])
        l = float(row["Low"])
        c = float(row["Close"])
        if not any(np.isnan(v) for v in (o, h, l, c)):
            volume_value = 0.0
            try:
                if "Volume" in row.index and pd.notna(row["Volume"]):
                    volume_value = max(0.0, float(row["Volume"]))
                elif "volume" in row.index and pd.notna(row["volume"]):
                    volume_value = max(0.0, float(row["volume"]))
            except Exception:
                volume_value = 0.0
            candles.append({
                "time": t_str,
                "open": o,
                "high": h,
                "low": l,
                "close": c,
                "volume": volume_value
            })

    return {
        "candles": candles,
        "ma20_line": [],
        "ma60_line": [],
        "bb_up_line": [],
        "bb_dn_line": []
    }

