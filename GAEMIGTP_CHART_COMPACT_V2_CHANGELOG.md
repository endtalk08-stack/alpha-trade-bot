# gaemiGTP Chart Compact v2

- ECharts remains candle + volume only.
- Removed duplicate date labels: dates are shown only on the bottom volume axis.
- Kept stock name, symbol, and price information, but changed to a compact single-line header.
- Current price and change percentage are shown to the right of the stock name on one line.
- Fixed header/meta space to reduce layout shift during symbol changes.
- Did not change data APIs, DB schema, analysis logic, or non-chart widgets.
