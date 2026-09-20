# gaemiGTP Widget Architecture — STEP56

## 원칙

화면의 위젯은 서로의 계산 결과를 읽지 않는다. 모든 종목 위젯은 같은 `market + symbol` 식별자를 사용하지만, 각 위젯은 자기 API/컨트롤러/렌더러를 가진다.

공통으로 공유할 수 있는 것은 **원천 시장 데이터(O H L C V, 현재가, 등락률)** 뿐이다. 이 데이터는 짧은 메모리 캐시(`STOCK_CORE_CONTEXT_CACHE`)에 보관되며 위젯 결과를 DB에 저장하지 않는다.

## 활성 흐름

종목 선택
→ `/api/analyze` (core only)
→ 종목명 / 현재가 / 등락률 / 캔들 렌더
→ 각 위젯이 자기 API를 독립 호출

## 위젯별 책임

| 위젯 | 데이터 API | 다른 위젯 의존 | DB 저장 |
|---|---|---|---|
| 차트 | `/api/analyze` core의 candles | 없음 | 없음 |
| 퀀트 | `/api/widget/quant` | 없음 | 없음 |
| 트랙 | `/api/widget/track` | 없음 | 없음 |
| 종목뉴스 | `/api/stock-news` | 없음 | 없음 |
| 경제뉴스 | `/api/global-news` | 없음 | 없음 |
| 컨센서스 | `/api/widget/consensus` | 없음 | 없음 |
| DART | `/api/widget/dart` | 없음 | 없음 |
| 세계시장 | `/api/world-board` | 종목과 무관 | 기존 캐시 |
| 실적 캘린더 | `/api/market-calendar` + 실적 모듈 | 종목과 무관 | 기존 구조 |
| 경제 캘린더 | `/api/market-calendar` | 종목과 무관 | 기존 구조 |
| 오른쪽 시장 | `/api/right-market-board` | 종목과 무관 | 메모리 캐시 |
| 미국 주도업종 | `/api/us-leading-sectors` | 종목과 무관 | 메모리/기존 구조 |
| 미국 주도종목 | `/api/us-leading-stocks` | 업종 선택 상태만 사용 | 메모리/기존 구조 |
| 투표 | Supabase/기존 vote.js | 종목 ID | 기존 구조 |
| 토론 | Supabase/기존 comment.js | 종목 ID | 기존 구조 |

## 제거한 위젯

`왜 움직였을까?`, `악성 매물대`는 활성 HTML/JS/API 흐름에서 제거했다. 기존 관련 백업/레거시 코드는 다른 기능과 혼동하지 않도록 당장 기능에 연결하지 않는다.

## 금지 규칙

새 위젯을 추가할 때 `renderStockData()` 안에 직접 계산/외부 API 호출을 넣지 않는다.

새 위젯은 아래 4개를 따로 만든다.

1. 전용 API endpoint
2. 전용 widget JS loader/controller
3. 전용 render 함수
4. 필요 시 전용 메모리 cache

다른 위젯의 `data.quant`, `data.movement_reason`, `data.stock_news` 같은 결과를 읽어서 계산하지 않는다.

## 앞으로 DB를 붙이는 시점

위젯에서 실제로 장기 보관이 필요한 필드와 목적이 확정된 뒤에만 별도 DB 테이블/저장 정책을 설계한다. 현재 버전은 위젯 결과를 DB에 저장하지 않는다.
