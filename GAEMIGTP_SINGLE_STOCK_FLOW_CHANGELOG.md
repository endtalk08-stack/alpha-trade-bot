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
