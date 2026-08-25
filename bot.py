<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>gaemiGTP | 트레이딩 봇 & 실시간 차트</title>
  <style>
    * {
      box-sizing: border-box;
      margin: 0;
      padding: 0;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    }
    body {
      background-color: #0e1117;
      color: #ffffff;
      display: flex;
      flex-direction: column;
      min-height: 100vh;
    }
    /* 상단 네비게이션 */
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
      letter-spacing: 0.5px;
    }
    .status-badge {
      background-color: rgba(35, 134, 54, 0.2);
      color: #3fb950;
      border: 1px solid rgba(63, 185, 80, 0.3);
      padding: 4px 10px;
      border-radius: 20px;
      font-size: 0.85rem;
      font-weight: 600;
    }
    /* 메인 대시보드 레이아웃 */
    main {
      flex: 1;
      padding: 24px;
      max-width: 1400px;
      margin: 0 auto;
      width: 100%;
    }
    .grid-container {
      display: grid;
      grid-template-columns: 3fr 1fr;
      gap: 20px;
    }
    @media (max-width: 900px) {
      .grid-container {
        grid-template-columns: 1fr;
      }
    }
    /* 카드 공통 스타일 */
    .card {
      background-color: #161b22;
      border: 1px solid #30363d;
      border-radius: 10px;
      padding: 16px;
      box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
    }
    .card h2 {
      font-size: 1.1rem;
      font-weight: 600;
      margin-bottom: 14px;
      color: #c9d1d9;
    }
    /* 차트 영역 */
    .chart-box {
      height: 620px;
    }
    /* 봇 상태 사이드바 */
    .bot-info {
      display: flex;
      flex-direction: column;
      gap: 14px;
    }
    .info-item {
      background-color: #0e1117;
      border: 1px solid #21262d;
      border-radius: 6px;
      padding: 12px;
    }
    .info-label {
      font-size: 0.8rem;
      color: #8b949e;
      margin-bottom: 4px;
    }
    .info-value {
      font-size: 1.1rem;
      font-weight: 600;
    }
    .profit {
      color: #3fb950;
    }
    footer {
      text-align: center;
      padding: 16px;
      font-size: 0.85rem;
      color: #8b949e;
      border-top: 1px solid #21262d;
    }
  </style>
</head>
<body>

  <!-- 상단 바 -->
  <header>
    <div class="logo">🐜 gaemiGTP Alpha Bot</div>
    <div class="status-badge">● 시스템 정상 가동 중</div>
  </header>

  <!-- 메인 본문 -->
  <main>
    <div class="grid-container">
      
      <!-- 왼쪽: 트레이딩뷰 실시간 차트 -->
      <div class="card chart-box">
        <h2>실시간 트레이딩 차트</h2>
        <div class="tradingview-widget-container" style="height: calc(100% - 36px); width: 100%;">
          <div id="tradingview_chart" style="height: 100%; width: 100%;"></div>
        </div>
      </div>

      <!-- 오른쪽: 봇 현황 대시보드 -->
      <div class="card">
        <h2>자동매매 모니터링</h2>
        <div class="bot-info">
          <div class="info-item">
            <div class="info-label">대상 종목</div>
            <div class="info-value">BTC / USDT</div>
          </div>
          <div class="info-item">
            <div class="info-label">현재 전략</div>
            <div class="info-value">RSI + 변동성 돌파</div>
          </div>
          <div class="info-item">
            <div class="info-label">오늘의 모의 수익률</div>
            <div class="info-value profit">+3.28%</div>
          </div>
          <div class="info-item">
            <div class="info-label">마지막 신호 감지</div>
            <div class="info-value" style="font-size: 0.95rem; color: #58a6ff;">매수 관망 구간 (Hold)</div>
          </div>
        </div>
      </div>

    </div>
  </main>

  <footer>
    © 2026 gaemiGTP. All rights reserved.
  </footer>

  <!-- 트레이딩뷰 위젯 연동 스크립트 -->
  <script type="text/javascript" src="https://s3.tradingview.com/tv.js"></script>
  <script type="text/javascript">
    new TradingView.widget({
      "autosize": true,
      "symbol": "BINANCE:BTCUSDT",
      "interval": "60",
      "timezone": "Asia/Seoul",
      "theme": "dark",
      "style": "1",
      "locale": "kr",
      "enable_publishing": false,
      "allow_symbol_change": true,
      "container_id": "tradingview_chart"
    });
  </script>
</body>
</html>
