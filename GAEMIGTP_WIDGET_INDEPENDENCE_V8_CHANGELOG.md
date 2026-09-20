# gaemiGTP STEP58 — Widget Independence / Request Isolation

- Removed fixed 0/6/7/8/16/24/32/40ms widget dispatch delays.
- All stock widgets start independently after Core instrument data is confirmed.
- Each widget keeps its own request controller/token and only updates its own DOM.
- Home navigation now aborts and invalidates every widget request.
- Track keeps a dedicated minimum-data fallback using the fast chart payload when shared Core loading fails.
- Track API returns a safe empty payload on temporary source failure instead of a 404 widget failure.
- Core remains limited to instrument + price/change + OHLCV/candles.
- Why Movement and Malicious Zone remain inactive.
- No active widget results are stored in DB.
- No layout redesign is included.
