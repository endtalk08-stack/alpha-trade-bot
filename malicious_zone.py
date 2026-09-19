import numpy as np
import pandas as pd

def _build_malicious_zones(df, raw_price):
    """최근 가격대별 거래량 집중을 이용해 잠재적 악성 매물대 후보를 계산한다."""
    try:
        if df is None or len(df) < 20 or raw_price is None or 'Volume' not in df.columns:
            return []
        work = df.copy().tail(120)
        work = work[['High','Low','Close','Volume']].apply(pd.to_numeric, errors='coerce').dropna()
        work = work[(work['Volume'] > 0) & (work['High'] >= work['Low'])]
        if len(work) < 15: return []
        lo, hi = float(work['Low'].min()), float(work['High'].max())
        if not np.isfinite(lo) or not np.isfinite(hi) or hi <= lo: return []
        bins = min(24, max(12, int(np.sqrt(len(work)) * 3)))
        edges = np.linspace(lo, hi, bins + 1); profile = np.zeros(bins, dtype=float)
        for _, r in work.iterrows():
            l, h, v = float(r['Low']), float(r['High']), float(r['Volume'])
            a = max(0, min(bins-1, int(np.searchsorted(edges, l, side='right')-1)))
            b = max(0, min(bins-1, int(np.searchsorted(edges, h, side='left'))))
            profile[a:b+1] += v / max(1, b-a+1)
        zones=[]; current=float(raw_price); peak=float(profile.max())
        if peak <= 0: return []
        total=float(profile.sum()) or 1.0
        for i in range(1, bins-1):
            if profile[i] < peak*0.48 or profile[i] < profile[i-1] or profile[i] < profile[i+1]: continue
            zlo,zhi=float(edges[i]),float(edges[i+1])
            if zhi <= current*0.995: continue
            intensity=min(99,max(50,round(profile[i]/peak*100)))
            dist=(zlo-current)/current*100 if current else 0
            zones.append({'low':round(zlo,2),'high':round(zhi,2),'intensity':int(intensity),'distance_pct':round(dist,1),'share_pct':round(profile[i]/total*100,1)})
        zones.sort(key=lambda z:(z['distance_pct']))
        return zones[:3]
    except Exception:
        return []
