# gaemiGTP Widget Architecture — FINAL PRINCIPLE

## 확정 원칙

1. **종목은 하나로 연결**: 모든 진입점은 같은 `market + symbol` canonical instrument를 사용한다.
2. **Core 데이터는 필요시 공유**: 현재가, 등락률, OHLCV/candles 같은 원천 데이터만 공유할 수 있다.
3. **위젯 계산/조회/렌더링은 완전 독립**: 위젯 결과를 다른 위젯의 입력으로 사용하지 않는다. 각 위젯은 자기 API, controller/token, render를 가진다.
4. **DB 저장은 데이터 정의가 확정된 뒤**: 현재 위젯 결과는 DB에 저장하지 않는다. 장기 보관 목적과 필드가 확정될 때 해당 위젯만 별도 저장 정책을 만든다.

## 활성 구조

종목 선택
→ canonical instrument
→ `/api/analyze` (Core only)
→ 헤더/차트 반영
→ 각 위젯은 자기 API를 동시에 독립 호출

## 위젯 계약

| 위젯 | 조회 | 공유 가능 | 다른 위젯 결과 의존 | DB 저장 |
|---|---|---|---|---|
| 차트 | `/api/analyze` Core candles | Core | 없음 | 없음 |
| 퀀트 | `/api/widget/quant` | Core | 없음 | 없음 |
| 트랙 | `/api/widget/track` | Core 또는 자체 최소 원천 | 없음 | 없음 |
| 종목뉴스 | `/api/stock-news` | 종목 ID | 없음 | 없음 |
| 경제뉴스 | `/api/global-news` | 시장 ID | 없음 | 없음 |
| 컨센서스 | `/api/widget/consensus` | Core current price | 없음 | 없음 |
| DART | `/api/widget/dart` | 종목 ID | 없음 | 없음 |

세계시장/경제뉴스/경제·실적 캘린더 등 종목과 무관한 위젯은 종목 전환과 별도 생명주기를 가진다.

## 금지

- `data.quant`를 다른 위젯 계산에 사용하지 않는다.
- `data.movement_reason`를 활성 위젯 입력으로 사용하지 않는다.
- 새 위젯을 중앙 `renderStockData()` 안에서 직접 계산하지 않는다.
- 위젯 데이터를 아직 확정하지 않았으면 DB 테이블을 만들지 않는다.


## 뉴스 소스 확정

- 종목뉴스: Google News RSS만 사용한다.
- 경제뉴스: Google News RSS만 사용한다.
- Finlight 뉴스는 활성 위젯 경로에서 사용하지 않는다.
- 뉴스 새로고침 중 기존 카드 내용을 먼저 비우지 않고, 새 응답 도착 시 한 번에 교체한다.

## 화면 갱신 원칙

- 종목 전환 중 기존 종목 헤더/가격/차트를 먼저 비우지 않는다.
- 새 Core 데이터가 확정되면 종목명·가격·등락률·차트를 같은 브라우저 프레임에서 반영한다.


## v12 economic news fix
- `lastRenderedMarket` state is explicitly initialized in the economic-news widget.
- `/api/global-news` normalizes KR/US/CRYPTO market codes before Google News query selection.
