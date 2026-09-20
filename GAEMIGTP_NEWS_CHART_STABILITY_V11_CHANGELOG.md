# gaemiGTP — News & Chart Stability v11

## Fixed
- Active stock-news API now uses Google News RSS only.
- Active economic-news API now uses Google News RSS only.
- Removed visible news flicker by keeping the existing card content until the new response arrives, then replacing it atomically.
- Same-market economic news remains visible during stock changes; switching market clears the previous market's news before the new result arrives.
- Stock header/current price/chart are committed in one `requestAnimationFrame` frame.
- During stock changes, the previous header/chart is retained until the new Core payload is ready.
- ECharts stock data update is synchronous inside that frame (`lazyUpdate: false`) to reduce header/chart mismatch.
- Page-level loading spinner is shown only on the initial stock entry, not on every stock switch.

## Scope
- No layout redesign.
- No quant/track logic changes.
- No DB schema or widget-data persistence changes.
