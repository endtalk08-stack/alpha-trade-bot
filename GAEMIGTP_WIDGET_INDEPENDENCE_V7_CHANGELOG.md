# gaemiGTP STEP56 — Widget Independence

- `/api/analyze` is now a stock-core endpoint in the active path: instrument, price, change, and candles only.
- Quant data uses `/api/widget/quant` and is no longer saved to PostgreSQL.
- Track data uses `/api/widget/track` and no longer depends on movement-reason results.
- Consensus uses `/api/widget/consensus`.
- DART uses `/api/widget/dart`.
- Stock news and global news remain their own APIs.
- World board, US leading sectors/stocks, earnings calendar, economic calendar, vote, and comment remain independent modules.
- Removed active Why Movement and Malicious Zone UI/modules.
- No widget result is written to DB in the new active path. Existing raw market-price storage/fallback code is left untouched for compatibility.
- `/api/analyze-details` and front-end details polling are no longer used in the active path.

- Vote/comment loads were moved out of core and are now independently triggered per selected symbol.
- Widget loaders clear stale previous-symbol content before starting a new request.
