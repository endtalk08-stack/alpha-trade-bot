/* gaemiGTP Widget Catalog — metadata only.
   Shared by central Workspace and right Panel.
   No API, DB, calculation, or rendering logic belongs here.
*/
(function () {
  'use strict';

  window.GaemiGTPWidgetCatalog = Object.freeze({
    'kospi': { title: '코스피', icon: 'sparkles', description: '국내 시장 흐름' },
    'kosdaq': { title: '코스닥', icon: 'activity', description: '국내 코스닥 지수' },
    'qqq': { title: 'QQQ', icon: 'activity', description: '나스닥100 ETF' },
    'spy': { title: 'SPY', icon: 'activity', description: 'S&P500 ETF' },
    'soxx': { title: 'SOXX', icon: 'cpu', description: '반도체 ETF' },
    'dia': { title: 'DIA', icon: 'activity', description: '다우 ETF' },
    'vix': { title: 'VIX', icon: 'activity', description: '시장 변동성 지수' },
    'forex': { title: '원/달러', icon: 'coins', description: '원달러 환율' },
    'oil': { title: 'WTI 유가', icon: 'droplets', description: '원유 선물' },
    'gold': { title: '금', icon: 'gem', description: '금 선물' },
    'night-futures': { title: '코스피200 야간선물', icon: 'moon-star', description: '야간 선물 흐름' },
    'materials': { title: '재료는 있어?', icon: 'newspaper', description: '관련 뉴스·공시·재료' },
    'levels':    { title: '여기 깨지면 큰일인데?', icon: 'chart-no-axes-combined', description: '지지·저항 구간' },
    'chart':     { title: '차트', icon: 'chart-candlestick', description: '가격 흐름' },
    'volume':    { title: '거래량', icon: 'chart-column', description: '거래량 흐름' },
    'trading-value': { title: '거래대금', icon: 'wallet', description: '거래대금 흐름' },
    'economic':  { title: '경제일정', icon: 'calendar-days', description: '주요 경제 일정' },
    'earnings':  { title: '실적', icon: 'file-chart-column', description: '실적 일정·결과' },
    'event':     { title: '이벤트', icon: 'zap', description: '시장 이벤트' },
    'ranking':   { title: '순위', icon: 'trophy', description: '관련 종목 순위' },
  });
}());
