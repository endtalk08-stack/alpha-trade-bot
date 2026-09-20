# gaemiGTP single-stock-flow fix

## 목적
메인 채팅 종목검색, 본문 하단 종목검색, 오른쪽 사이드바 종목선택, 새로고침, 캘린더/세계시장 종목선택이 서로 다른 데이터 상태를 만들지 않도록 종목 선택과 분석 캐시 기준을 하나로 통합했습니다.

## 핵심 변경
- 모든 종목 입력을 `resolve_instrument()`로 정규화
- canonical key 도입: `KR:005930`, `US:NVDA`, `CRYPTO:BTCUSDT`, `INDEX:^KS11`
- 서버 분석 캐시/상세 캐시를 canonical key 기준으로 통일
- 동일 종목/동일 분석 모드의 동시 요청을 single-flight로 합침
- 프론트의 종목 진입점을 `selectStock()`으로 통합하고 기존 `doSearch()`는 호환 alias로 유지
- 일반 종목 검색에서 `/api/chart` 중복 호출 제거
- `/api/analyze` 응답의 `candles`를 공통 렌더러가 직접 차트에 사용
- details 완료 후 `doSearch()` 재호출 제거; 현재 화면에 상세 데이터만 재렌더링
- 홈 이동 시 진행 중 검색/details 요청 abort 및 토큰 무효화
- URL을 `?m=<market>&s=<symbol>` canonical 형태로 저장하고 기존 `?q=`도 복원 호환
- 오른쪽 사이드바/세계시장/실적 캘린더 클릭을 `selectStock()`으로 통합
- `home.out`을 실제 `bot.py`의 `HTML_PAGE`와 동기화
- 실제 서버 HTML 빌드 표기를 `STEP53-SINGLE-STOCK-FLOW`로 변경

## 검증
- Python 전체 모듈 `py_compile` 통과
- inline HTML JavaScript `node --check` 통과
- widgets 전체 JavaScript `node --check` 통과
- 동일 종목 `삼성전자 / 005930 / KR:005930` 동시 core 요청이 실제 분석 1회로 합쳐지는지 테스트 완료
- `/api/analyze-details?key=KR:005930` canonical cache 조회 200 응답 테스트 완료
- 로컬 HTTP 서버 `/health` 및 실제 HTML 빌드 마커 확인 완료

## 참고
외부 데이터 제공처의 네트워크 상태는 배포 환경에 따라 달라질 수 있습니다. 이번 수정은 애플리케이션 내부의 종목 식별/캐시/요청 경쟁/렌더링 재진입 문제를 대상으로 합니다.


## STEP 54 추가 안정화
- 오른쪽 사이드바/연속 종목 선택을 최신 선택 우선 대기열로 제한
- 이전 브라우저 fetch abort만으로 서버 작업이 취소되지 않는 문제를 고려해 동시 분석 적체를 방지
- 이전 종목 완료 응답이 새 선택 사이에 화면에 끼어들지 않도록 대기 선택 존재 여부를 렌더링에서 차단
- core 분석 요청이 15초 이상 걸릴 때 브라우저가 무한 로딩 상태에 머물지 않도록 timeout 적용
- 외부 데이터의 순간적인 빈 응답으로 전체 분석이 None이 된 경우 서버에서 250ms 후 1회 재시도

## STEP 55 — Right-sidebar provider resilience
- Right-sidebar clicks now carry an explicit market-qualified symbol (`KR:`, `US:`, `CRYPTO:`).
- Domestic OHLCV fallback chain ends with PostgreSQL stored prices when web providers temporarily fail.
- US Yahoo chart uses query1/query2 fallback; if `FINNHUB_API_KEY` is configured, Finnhub daily candles are a final provider fallback.
- Yahoo quote failure can fall back to chart-derived current/previous close.
- Goal: prevent first-time/uncached right-sidebar selections from failing just because one external provider is temporarily unavailable.


## Final dynamic instrument integration
- Added runtime instrument registry; discovered sidebar/broker-style symbols are auto-registered instead of being fixed in source.
- Right sidebar rows now carry `market_code` and `instrument_key` and open through the same selection path.
- `/api/analyze` accepts explicit `m` + `s` so dynamically supplied tickers bypass name-resolution ambiguity.
- Added a public Stooq daily OHLCV fallback for new US tickers when Yahoo/DB are unavailable.
- Existing UI, database tables, analysis calculations, and sidebar ranking behavior were preserved.

- Recent/favorite sidebar rows now honor each row's stored market code instead of assuming the currently selected sidebar tab market.

## V4 PATCH — 2026-09-20
- Fixed a domestic canonical-symbol routing bug where `KR:000660` / `KR:005930` could be passed into `find_stock_code()` after server-side resolution.
- Domestic analysis now uses the already-resolved canonical `instrument.symbol` and `instrument.name` directly, preventing false `/api/analyze` 404 responses for sidebar-selected KR stocks.
- Focused test verified `run_full_pipeline('KR:000660', lightweight=True)` returns `KR:000660`, `SK하이닉스`, and chart candles without calling the legacy name resolver.

## STEP 55 — 메인 검색창 하단 고정 종목도 canonical instrument로 통합
- 하단 인기/고정 종목 버튼 5개를 문자열 검색 호출에서 `{key, market, symbol, name}` 객체 호출로 변경.
- SK하이닉스 → `KR:000660`
- 삼성전자 → `KR:005930`
- 알테오젠 → `KR:196170`
- 엔비디아 → `US:NVDA`
- 비트코인 → `CRYPTO:BTCUSDT`
- `selectStock()`의 기존 호환 경로는 유지하여 기존 위젯 호출과의 호환성을 보존.
- 실제 서버 HTML 원본(`bot.py`의 `HTML_PAGE`)과 `home.out`의 HTML을 동일 상태로 유지.
- 목적: 메인 검색 / 하단 고정 종목 / 오른쪽 사이드바가 모두 동일한 canonical 종목 식별 체계를 사용하도록 통합.

## Chart v1 — Apache ECharts candlestick + volume
- 기존 Lightweight Charts 위젯을 Apache ECharts 기반으로 교체했다.
- 범위는 캔들 + 거래량으로 제한하고, 이평선/볼린저/매물대/이벤트 마커는 추가하지 않았다.
- 차트 인스턴스를 종목마다 삭제·재생성하지 않고 최초 1회 생성 후 `setOption()`으로 데이터만 교체한다.
- `/api/analyze`의 기존 `candles` 계약을 유지하면서 각 캔들에 `volume`을 추가했다.
- 테마 변경/창 크기 변경 시 기존 차트 인스턴스를 유지하고 필요한 옵션만 갱신한다.
- 외부 데이터 수집·분석·DB 로직은 변경하지 않았다.
