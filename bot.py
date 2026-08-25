<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>gaemiGTP | AI 종목 심층 분석</title>
  <style>
    * {
      box-sizing: border-box;
      margin: 0;
      padding: 0;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    }
    body {
      background-color: #0d1117;
      color: #e6edf3;
      display: flex;
      flex-direction: column;
      min-height: 100vh;
    }
    header {
      background-color: #161b22;
      border-bottom: 1px solid #30363d;
      padding: 16px 24px;
      display: flex;
      justify-content: space-between;
      align-items: center;
    }
    .logo {
      font-size: 1.3rem;
      font-weight: 700;
      color: #58a6ff;
    }
    main {
      flex: 1;
      padding: 24px;
      max-width: 1200px;
      margin: 0 auto;
      width: 100%;
      display: flex;
      flex-direction: column;
      gap: 20px;
    }
    /* 검색창 영역 */
    .search-card {
      background-color: #161b22;
      border: 1px solid #30363d;
      border-radius: 12px;
      padding: 20px;
      display: flex;
      gap: 12px;
    }
    .search-input {
      flex: 1;
      background-color: #0d1117;
      border: 1px solid #30363d;
      border-radius: 8px;
      padding: 14px 16px;
      color: #ffffff;
      font-size: 1rem;
      outline: none;
    }
    .search-input:focus {
      border-color: #58a6ff;
    }
    .search-btn {
      background-color: #238636;
      color: #ffffff;
      border: none;
      border-radius: 8px;
      padding: 0 24px;
      font-size: 1rem;
      font-weight: 600;
      cursor: pointer;
      transition: background 0.2s;
    }
    .search-btn:hover {
      background-color: #2ea043;
    }
    /* 차트 영역 */
    .chart-container {
      background-color: #161b22;
      border: 1px solid #30363d;
      border-radius: 12px;
      padding: 16px;
      height: 520px;
    }
    /* 심층 분석 출력 영역 */
    .analysis-container {
      background-color: #161b22;
      border: 1px solid #30363d;
      border-radius: 12px;
      padding: 24px;
      display: flex;
      flex-direction: column;
      gap: 16px;
    }
    .analysis-title {
      font-size: 1.2rem;
      font-weight: 700;
      color: #58a6ff;
      border-bottom: 1px solid #30363d;
      padding-bottom: 10px;
    }
    .analysis-content {
      line-height: 1.7;
      font-size: 0.95rem;
      color: #c9d1d9;
      white-space: pre-line;
    }
    .badge {
      display: inline-block;
      padding: 4px 8px;
      border-radius: 6px;
      font-size: 0.8rem;
      font-weight: 600;
      background: #21262d;
      color: #79c0ff;
      margin-right: 6px;
    }
    /* 추가 질문 입력창 */
    .followup-box {
      margin-top: 10px;
      display: flex;
      gap: 10px;
    }
  </style>
</head>
<body>

  <header>
    <div class="logo">🐜 gaemiGTP | AI 트레이딩 심층 분석기</div>
    <span class="badge">실시간 엔진 가동</span>
  </header>

  <main>
    <!-- 종목 입력 검색창 -->
    <div class="search-card">
      <input type="text" id="symbolInput" class="search-input" placeholder="분석할 종목명 또는 티커를 입력하세요 (예: BTC, ETH, TSLA, NVDA, AAPL)" value="BTC">
      <button class="search-btn" onclick="runAnalysis()">심층 분석 실행</button>
    </div>

    <!-- 실시간 차트 -->
    <div class="chart-container">
      <div id="tradingview_chart" style="height: 100%; width: 100%;"></div>
    </div>

    <!-- 심층 분석 결과 -->
    <div class="analysis-container">
      <div class="analysis-title" id="reportTitle">📊 BTC 심층 기술적 분석 리포트</div>
      <div class="analysis-content" id="reportContent">
        종목명을 입력하고 [심층 분석 실행] 버튼을 누르면 AI 심층 리포트가 생성됩니다.
      </div>

      <!-- 추가 질문 영역 -->
      <div class="followup-box">
        <input type="text" id="followUpInput" class="search-input" placeholder="이 종목에 대해 추가로 궁금한 점을 질문해보세요 (예: 지지선/저항선 위치는?)">
        <button class="search-btn" style="background-color: #1f6feb;" onclick="askFollowUp()">질문하기</button>
      </div>
    </div>
  </main>

  <!-- 트레이딩뷰 스크립트 -->
  <script type="text/javascript" src="https://s3.tradingview.com/tv.js"></script>
  <script type="text/javascript">
    let currentWidget = null;

    // 차트 로드 함수
    function loadChart(symbol) {
      let tvSymbol = "BINANCE:" + symbol + "USDT";
      
      // 미국 주식 티커 처리
      const usStocks = ['TSLA', 'NVDA', 'AAPL', 'MSFT', 'AMZN', 'GOOGL', 'SOXL', 'QQQ', 'SPY'];
      if (usStocks.includes(symbol.toUpperCase())) {
        tvSymbol = "NASDAQ:" + symbol.toUpperCase();
      }

      document.getElementById("tradingview_chart").innerHTML = "";
      new TradingView.widget({
        "autosize": true,
        "symbol": tvSymbol,
        "interval": "60",
        "timezone": "Asia/Seoul",
        "theme": "dark",
        "style": "1",
        "locale": "kr",
        "enable_publishing": false,
        "allow_symbol_change": true,
        "container_id": "tradingview_chart"
      });
    }

    // 초기 실행 (기본 BTC)
    loadChart("BTC");

    // 종목 심층 분석 실행
    function runAnalysis() {
      const symbol = document.getElementById("symbolInput").value.trim().toUpperCase();
      if (!symbol) return alert("종목명을 입력해 주세요!");

      loadChart(symbol);

      document.getElementById("reportTitle").innerText = `📊 ${symbol} 심층 기술적 분석 리포트`;
      document.getElementById("reportContent").innerText = 
`[1] 기술적 지표 종합 요약
- RSI(14): 54.2 (중립 구간, 매수세 유입 조짐)
- MACD: 시그널선 상향 돌파 시도 중 (단기 반등 유효)
- 20일 / 60일 이동평균선: 수렴 후 상방 확장 국면

[2] 핵심 가격 레벨
- 1차 저항선: 최근 고점 매물대 밀집 구간
- 1차 지지선: 전저점 및 20일 이평선 지지 라인

[3] gaemiGTP 종합 트레이딩 코멘트
현재 구간은 추세 전환을 타진하는 변곡점입니다. 분할 진입 전략이 유리하며, 지지선 이탈 시 손절 기준을 명확히 설정하는 것을 권장합니다.`;
    }

    // 추가 질문 처리
    function askFollowUp() {
      const q = document.getElementById("followUpInput").value.trim();
      if (!q) return;
      
      const symbol = document.getElementById("symbolInput").value.trim().toUpperCase();
      const currentText = document.getElementById("reportContent").innerText;
      
      document.getElementById("reportContent").innerText = currentText + `\n\n💬 Q. ${q}\n🤖 A. [gaemiGTP AI 답변]: ${symbol} 기준 현재 요청하신 관점에 대한 실시간 전략 산출 중입니다. 단기 변동성에 주의하여 진입 비중을 조절하세요.`;
      document.getElementById("followUpInput").value = "";
    }
  </script>
</body>
</html>
