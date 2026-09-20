# gaemiGTP Widget Architecture — FIXED PRINCIPLES

## 확정 원칙

1. **종목은 하나로 연결**: 모든 진입점은 같은 `market + symbol` canonical instrument를 사용한다.
2. **Core 데이터는 필요시 공유**: 현재가, 등락률, OHLCV/candles 같은 원천 데이터만 공유할 수 있다.
3. **위젯 계산/조회/렌더링은 완전 독립**: 위젯 결과를 다른 위젯의 입력으로 사용하지 않는다. 각 위젯은 자기 API, controller/token, render를 가진다.
4. **DB 저장은 데이터 정의가 확정된 뒤**: 현재 위젯 결과는 DB에 저장하지 않는다. 장기 보관 목적과 필드가 확정될 때 해당 위젯만 별도 저장 정책을 만든다.

## 현재 데이터 흐름

```text
종목 선택
  ↓
canonical instrument (예: KR:005930 / US:NVDA)
  ↓
Core Coordinator
  ↓
Core 원천 데이터 1회 취득 + 종목별 in-flight 공유
  ├─ 현재가 / 등락률
  └─ OHLCV / candles

          ↓ 필요 위젯만 Core 참조

  ├─ 차트       → Core candles → 자기 렌더
  ├─ 퀀트       → Core OHLCV   → 자기 계산/API → 자기 렌더
  └─ 트랙       → Core OHLCV   → 자기 계산/API → 자기 렌더

  ├─ 종목뉴스   → 자기 API → 자기 렌더
  ├─ 경제뉴스   → 시장 기준 자기 API → 자기 렌더
  ├─ 컨센서스   → 자기 API + 필요시 Core 현재가
  └─ DART       → 종목 ID 기준 자기 API
```

## 핵심 규칙

- 같은 종목의 Core 취득은 동시에 여러 위젯이 요청해도 종목별 single-flight로 1회만 수행한다.
- Core 캐시가 없을 때 위젯이 각자 Naver/Yahoo 등 원천 데이터를 재조회하지 않는다.
- 트랙은 별도 chart fallback을 사용하지 않고 공유 Core만 사용한다.
- 일반 종목 선택에서 `/api/chart`를 중복 호출하지 않는다. `/api/chart`는 별도 차트가 필요한 기능에서만 사용한다.
- `analyze-details → 전체 render` 같은 재진입 방식은 사용하지 않는다.
- 세계시장/경제·실적 캘린더 등 종목과 무관한 위젯은 종목 전환과 별도 생명주기를 가진다.
- `왜 움직였을까`와 `악성 매물대`는 현재 활성 구조에서 제외한다.
- 새 위젯은 중앙 분석 결과에 끼워 넣지 않고 `전용 API + 전용 JS + 전용 렌더`로 추가한다.

## 위젯 계약

| 위젯 | 조회 | Core 공유 | 다른 위젯 결과 의존 | DB 저장 |
|---|---|---|---|---|
| 차트 | `/api/analyze` Core candles | 필요 | 없음 | 없음 |
| 퀀트 | `/api/widget/quant` | 필요 | 없음 | 없음 |
| 트랙 | `/api/widget/track` | 필요 | 없음 | 없음 |
| 종목뉴스 | `/api/stock-news` | 종목 ID만 | 없음 | 없음 |
| 경제뉴스 | `/api/global-news` | 시장 ID만 | 없음 | 없음 |
| 컨센서스 | `/api/widget/consensus` | 필요시 현재가 | 없음 | 없음 |
| DART | `/api/widget/dart` | 종목 ID | 없음 | 없음 |

## 현재 정리 완료 항목

- 예전 통합 분석/Enrichment 파이프라인 제거
- `왜 움직였을까`/`악성 매물대` 활성 코드 제거
- 중복 `serialization` 파일 제거
- 사용하지 않는 `style.css`, `home.out`, `css.out`, `chart.out` 제거
- 런타임 로그/ PID 파일 제거
- 사용하지 않는 `movement_helpers.py`, `malicious_zone.py`, `news_analysis.py`, `news_merge.py`, `market_information.py` 제거
- Supabase 클라이언트 로딩 제거

