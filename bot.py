<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>gaemiGTP - 중앙 집중형 단일 피드 & 좌측 서랍 대시보드</title>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
        body { background-color: #1e1f20; color: #e3e3e3; min-height: 100vh; overflow-x: hidden; }
        
        /* 1. 상단 고정 헤더 */
        header { position: fixed; top: 0; left: 0; width: 100%; height: 56px; display: flex; justify-content: space-between; align-items: center; padding: 0 20px; z-index: 50; background: #1e1f20; border-bottom: 1px solid #282a2c; }
        .header-left { display: flex; align-items: center; gap: 14px; }
        .menu-btn { background: transparent; border: none; color: #c4c7c5; cursor: pointer; display: flex; align-items: center; justify-content: center; padding: 6px; border-radius: 8px; transition: background 0.15s; }
        .menu-btn:hover { background: #282a2c; color: #fff; }
        .logo-top { font-weight: 700; font-size: 1.15rem; color: #e3e3e3; letter-spacing: -0.5px; cursor: pointer; }
        .pro-top-tag { font-size: 0.72rem; font-weight: 800; color: #f59e0b; background: rgba(245, 158, 11, 0.12); padding: 3px 8px; border-radius: 12px; border: 1px solid rgba(245, 158, 11, 0.3); }

        /* 2. 좌측 슬라이드 서랍 (ChatGPT 스타일) */
        .drawer-overlay { position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.6); z-index: 90; opacity: 0; pointer-events: none; transition: opacity 0.25s ease; }
        .drawer-overlay.active { opacity: 1; pointer-events: auto; }
        
        .drawer { position: fixed; top: 0; left: -290px; width: 290px; height: 100%; background: #17181a; border-right: 1px solid #282a2c; z-index: 100; transition: left 0.25s ease; display: flex; flex-direction: column; padding: 16px; }
        .drawer.active { left: 0; }
        
        /* 서랍 최상단: ChatGPT의 [+ 새 채팅] 위치에 [+ 새 종목 검색] */
        .new-search-main-btn { width: 100%; height: 42px; background: #282a2c; border: 1px solid #37393b; color: #f2f2f2; font-size: 0.88rem; font-weight: 600; border-radius: 10px; display: flex; align-items: center; justify-content: space-between; padding: 0 14px; cursor: pointer; transition: all 0.2s; margin-bottom: 20px; }
        .new-search-main-btn:hover { background: #323538; border-color: #38bdf8; color: #fff; }
        
        .drawer-scroll-area { flex: 1; overflow-y: auto; margin-right: -6px; padding-right: 6px; }
        .drawer-section-title { font-size: 0.72rem; color: #8e918f; font-weight: 700; margin: 12px 6px 6px; }
        .drawer-item { display: flex; align-items: center; justify-content: space-between; padding: 9px 10px; border-radius: 8px; color: #c4c7c5; font-size: 0.82rem; cursor: pointer; text-decoration: none; margin-bottom: 2px; }
        .drawer-item:hover { background: #282a2c; color: #fff; }
        .drawer-badge { font-size: 0.68rem; background: rgba(56, 189, 248, 0.15); color: #38bdf8; padding: 2px 6px; border-radius: 4px; }

        .drawer-bottom-menu { padding-top: 12px; border-top: 1px solid #282a2c; margin-top: auto; }
        .bottom-pro-btn { width: 100%; padding: 10px 12px; background: rgba(245, 158, 11, 0.1); border: 1px solid rgba(245, 158, 11, 0.25); border-radius: 8px; color: #f59e0b; font-size: 0.82rem; font-weight: 700; display: flex; justify-content: space-between; align-items: center; cursor: pointer; }
        .bottom-pro-btn:hover { background: rgba(245, 158, 11, 0.18); }

        /* 3. 중앙 집중형 단일 피드 (One-Column Feed) */
        .feed-container { max-width: 780px; margin: 72px auto 80px; padding: 0 16px; }
        
        /* 공통 카드 스타일 */
        .card { background: #282a2c; border-radius: 16px; padding: 20px; border: 1px solid #33363a; box-shadow: 0 4px 20px rgba(0,0,0,0.25); margin-bottom: 16px; }
        .section-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 14px; }
        .section-title { font-size: 1.05rem; font-weight: 700; color: #f2f2f2; display: flex; align-items: center; gap: 6px; }
        .badge-tag { font-size: 0.72rem; color: #8e918f; background: #1e1f20; padding: 3px 8px; border-radius: 12px; border: 1px solid #37393b; }

        /* [1] 시세 및 캔들 차트 */
        .price-title { font-size: 1.35rem; font-weight: 700; color: #f2f2f2; }
        .price-title span { color: #8e918f; font-size: 0.9rem; font-weight: 400; }
        .price-val { font-size: 2.3rem; font-weight: 800; color: #ffffff; margin: 6px 0; }
        .price-meta { color: #8e918f; font-size: 0.78rem; }
        .mock-chart { width: 100%; height: 280px; background: #1e1f20; border-radius: 10px; margin-top: 14px; display: flex; align-items: center; justify-content: center; color: #8e918f; font-size: 0.88rem; border: 1px dashed #37393b; }

        /* [2] gaemiGTP 종목 고유 뉴스 */
        .stock-news-item { padding: 10px 0; border-bottom: 1px solid #2e3034; text-decoration: none; display: block; }
        .stock-news-item:last-child { border-bottom: none; }
        .stock-news-title { color: #e3e3e3; font-size: 0.86rem; font-weight: 600; line-height: 1.4; }
        .stock-news-meta { color: #8e918f; font-size: 0.72rem; margin-top: 3px; }

        /* [3] 컨센서스 & 글로벌 수급 */
        .gauge-row { display: flex; align-items: center; gap: 14px; padding-bottom: 14px; border-bottom: 1px solid #33363a; }
        .circle-gauge { width: 56px; height: 56px; border-radius: 50%; border: 4px solid #10b981; display: flex; flex-direction: column; align-items: center; justify-content: center; background: #1e1f20; flex-shrink: 0; }
        .slider-bar-wrap { width: 100%; height: 6px; background: #1e1f20; border-radius: 4px; position: relative; margin: 16px 0 6px; }
        .slider-bar-fill { width: 65%; height: 100%; background: linear-gradient(90deg, #38bdf8, #10b981); border-radius: 4px; }
        .slider-dot { position: absolute; left: 65%; top: -4px; width: 14px; height: 14px; border-radius: 50%; background: #10b981; border: 2px solid #282a2c; transform: translateX(-50%); }

        /* [4] 14대 퀀트 기술 지표 */
        .quant-grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 8px; }
        .quant-box { background: #1e1f20; padding: 10px 12px; border-radius: 8px; border: 1px solid #2d3035; }
        .q-name { color: #8e918f; font-size: 0.74rem; margin-bottom: 2px; }
        .q-val { color: #fff; font-size: 0.9rem; font-weight: 700; }

        /* [5 & 6] 동일 규격 아코디언 토글 */
        .accordion-btn { width: 100%; height: 52px; background: #282a2c; border: 1px solid #33363a; border-radius: 32px; padding: 0 20px; color: #fff; font-size: 0.95rem; font-weight: 700; display: flex; align-items: center; justify-content: center; cursor: pointer; box-shadow: 0 4px 20px rgba(0,0,0,0.2); position: relative; margin-bottom: 16px; }
        .accordion-btn:hover { background: #323538; }
        .accordion-arrow { width: 18px; height: 18px; fill: #8e918f; transition: transform 0.2s; position: absolute; right: 20px; }
        .accordion-body { background: #242628; border-radius: 16px; padding: 16px; margin-top: -6px; margin-bottom: 16px; border: 1px solid #33363a; }
        
        .signal-row { display: flex; justify-content: space-between; font-size: 0.85rem; padding: 6px 0; border-bottom: 1px solid #2e3034; }
        .signal-row:last-child { border-bottom: none; }

        /* 마스터 전략 유료화 잠금 */
        .pro-badge { background: linear-gradient(90deg, #f59e0b, #ef4444); color: #fff; font-size: 0.65rem; font-weight: 800; padding: 2px 6px; border-radius: 6px; margin-left: 6px; }
        .master-content-box { position: relative; background: #1e1f20; padding: 14px; border-radius: 10px; border: 1px solid #2e3034; font-size: 0.82rem; line-height: 1.55; color: #d1d5db; overflow: hidden; }
        .blur-text { filter: blur(3.5px); user-select: none; opacity: 0.5; margin-top: 4px; }
        .lock-overlay { position: absolute; top: 0; left: 0; width: 100%; height: 100%; display: flex; flex-direction: column; align-items: center; justify-content: center; background: rgba(30, 31, 32, 0.75); border-radius: 10px; }
        .lock-btn { background: linear-gradient(90deg, #f59e0b, #d97706); color: #121212; border: none; font-weight: 800; font-size: 0.8rem; padding: 7px 16px; border-radius: 20px; cursor: pointer; }

        /* [7] 글로벌 매크로 지수 & 3줄 속보 & 캘린더 (피드 하단 종합 카드) */
        .macro-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 8px; margin-bottom: 16px; }
        .macro-item { background: #1e1f20; padding: 8px 10px; border-radius: 8px; border: 1px solid #2d3035; }
        .macro-sym { font-size: 0.78rem; font-weight: 700; color: #fff; }
        .macro-val { font-size: 0.88rem; font-weight: 700; margin: 2px 0; }
        .val-up { color: #ef4444; }
        .val-down { color: #38bdf8; }

        .brief-box { background: #1e1f20; padding: 12px; border-radius: 10px; border: 1px solid #2e3034; margin-bottom: 12px; }
        .brief-title { font-size: 0.84rem; font-weight: 700; color: #fff; margin-bottom: 6px; }
        .brief-bullets { font-size: 0.74rem; color: #9ca3af; line-height: 1.45; }

        .cal-row { display: flex; justify-content: space-between; align-items: center; padding: 7px 0; border-bottom: 1px solid #2e3034; font-size: 0.76rem; }
        .cal-row:last-child { border-bottom: none; }
        .cal-d { color: #38bdf8; font-weight: 700; width: 65px; }
        .cal-t { color: #fff; font-weight: 600; flex: 1; }
        .cal-v { color: #8e918f; font-size: 0.7rem; text-align: right; }
    </style>
</head>
<body>

    <!-- 1. 상단 고정 헤더 -->
    <header>
        <div class="header-left">
            <button class="menu-btn" onclick="toggleDrawer()">
                <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="3" y1="12" x2="21" y2="12"></line><line x1="3" y1="6" x2="21" y2="6"></line><line x1="3" y1="18" x2="21" y2="18"></line></svg>
            </button>
            <div class="logo-top">gaemiGTP</div>
        </div>
        <div class="pro-top-tag">PRO ACCESS</div>
    </header>

    <!-- 2. 좌측 슬라이드 서랍 (ChatGPT 레이아웃 완전 일치) -->
    <div class="drawer-overlay" id="drawerOverlay" onclick="toggleDrawer()"></div>
    <div class="drawer" id="sideDrawer">
        
        <!-- ChatGPT의 [+ 새 채팅]과 같은 위치 -->
        <button class="new-search-main-btn" onclick="alert('새 종목 검색창으로 이동합니다.'); toggleDrawer();">
            <span>+ 새 종목 검색</span>
            <span style="font-size:0.75rem; color:#8e918f;">Ctrl+K</span>
        </button>
        
        <div class="drawer-scroll-area">
            <div class="drawer-section-title">최근 검색한 종목</div>
            <div class="drawer-item"><span>🔥 SK하이닉스</span><span class="drawer-badge">국내</span></div>
            <div class="drawer-item"><span>🚀 엔비디아 (NVDA)</span><span class="drawer-badge">미국</span></div>
            <div class="drawer-item"><span>📱 삼성전자</span><span class="drawer-badge">국내</span></div>
            <div class="drawer-item"><span>₿ 비트코인 (BTC)</span><span class="drawer-badge">코인</span></div>

            <div class="drawer-section-title" style="margin-top:16px;">관심 핀 종목</div>
            <div class="drawer-item"><span>⭐ 알테오젠</span><span style="font-size:0.75rem; color:#ef4444;">+4.2%</span></div>
            <div class="drawer-item"><span>⭐ 테슬라 (TSLA)</span><span style="font-size:0.75rem; color:#38bdf8;">-1.1%</span></div>
        </div>

        <div class="drawer-bottom-menu">
            <div class="bottom-pro-btn" onclick="alert('PRO 멤버십 관리창으로 이동합니다.');">
                <span>👑 PRO 멤버십 관리</span>
                <span>→</span>
            </div>
        </div>
    </div>

    <!-- 3. 중앙 집중형 단일 피드 (시선 분산 없는 단일 열) -->
    <div class="feed-container">
        
        <!-- [1] 종목 시세 & 실시간 TradingView 캔들 차트 -->
        <div class="card">
            <div style="display:flex; justify-content:space-between; align-items:flex-end;">
                <div>
                    <div class="price-title">SK하이닉스 <span>(000660)</span></div>
                    <div class="price-val">178,500 원</div>
                </div>
                <div class="price-meta">시장: 국내주식 · 출처: Naver 증권 · 2026-08-26 KST</div>
            </div>
            <div class="mock-chart">
                📊 [실시간 TradingView 캔들 차트 (20선/60선/볼린저 밴드 상하단 전체 영역)]
            </div>
        </div>

        <!-- [2] gaemiGTP 종목 고유 뉴스 -->
        <div class="card">
            <div class="section-header">
                <span class="section-title">📰 gaemiGTP 종목 뉴스</span>
                <span class="badge-tag">실시간 수집</span>
            </div>
            <div class="stock-news-item">
                <div class="stock-news-title">SK하이닉스, 6세대 HBM4 개발 순항… 차세대 AI 메모리 주도권 확보</div>
                <div class="stock-news-meta">한국경제 · 25분 전</div>
            </div>
            <div class="stock-news-item">
                <div class="stock-news-title">외국인 투자자, 반도체 대형주 중심 순매수 전환… 수급 개선 뚜렷</div>
                <div class="stock-news-meta">매일경제 · 1시간 전</div>
            </div>
        </div>

        <!-- [3] 애널리스트 컨센서스 & 글로벌 온체인 수급 -->
        <div class="card">
            <div class="section-header">
                <span class="section-title">🎯 gaemiGTP 애널리스트 컨센서스</span>
                <span class="badge-tag">최근 리포트 18개 종합</span>
            </div>
            <div class="gauge-row">
                <div class="circle-gauge">
                    <span style="font-size:1.05rem; font-weight:800; color:#fff; line-height:1;">18</span>
                    <span style="font-size:0.52rem; color:#8e918f;">기관의견</span>
                </div>
                <div>
                    <div style="font-size:0.8rem; margin-bottom:3px;">
                        <span style="color:#8e918f;">전체 의견:</span>
                        <strong style="color:#10b981; background:rgba(16,185,129,0.15); padding:2px 6px; border-radius:6px;">적극 매수 (BUY)</strong>
                    </div>
                    <div style="font-size:0.72rem; color:#8e918f;">
                        <span style="color:#10b981;">● 매수 16</span> &nbsp; <span style="color:#f97316;">● 보유 2</span> &nbsp; <span style="color:#ef4444;">● 매도 0</span>
                    </div>
                </div>
            </div>

            <div style="display:flex; justify-content:space-between; align-items:flex-end; margin-top:12px;">
                <div>
                    <div style="font-size:0.7rem; color:#8e918f;">평균 목표주가</div>
                    <div style="font-size:1.25rem; font-weight:800; color:#10b981;">245,000 원</div>
                </div>
                <div style="text-align:right;">
                    <div style="font-size:0.7rem; color:#8e918f;">현재가</div>
                    <div style="font-size:1.1rem; font-weight:700; color:#ffffff;">178,500 원</div>
                </div>
            </div>

            <div class="slider-bar-wrap">
                <div class="slider-bar-fill"></div>
                <div class="slider-dot"></div>
            </div>
            <div style="display:flex; justify-content:space-between; font-size:0.65rem; color:#6b7280;">
                <span>증권사 최저 210,000원</span>
                <span>증권사 최고 300,000원</span>
            </div>
        </div>

        <!-- [4] 14대 퀀트 기술 지표 (2열 그리드) -->
        <div class="card">
            <div class="section-header">
                <span class="section-title">⚡ gaemiGTP 기술 지표</span>
                <span style="font-size:0.78rem; color:#38bdf8; font-weight:700;">정배열 상승 추세</span>
            </div>
            <div class="quant-grid">
                <div class="quant-box"><div class="q-name">5일선</div><div class="q-val">176,200 원</div></div>
                <div class="quant-box"><div class="q-name">20일선</div><div class="q-val">172,400 원</div></div>
                <div class="quant-box"><div class="q-name">볼린저 밴드 상/하</div><div class="q-val">185,000 / 164,000</div></div>
                <div class="quant-box"><div class="q-name">RSI (14)</div><div class="q-val">58.42 (안정권)</div></div>
                <div class="quant-box"><div class="q-name">52주 위치</div><div class="q-val">84.5% (고점 부근)</div></div>
                <div class="quant-box"><div class="q-name">20일 평균 거래대금</div><div class="q-val">4.2천억원 (1위)</div></div>
            </div>
        </div>

        <!-- [5] ⚡ 시그널 타점 (토글 아코디언) -->
        <div>
            <button class="accordion-btn" onclick="toggleAccordion('signalBody', 'signalArrow')">
                <span>⚡ gaemiGTP 시그널 타점</span>
                <svg class="accordion-arrow" id="signalArrow" style="transform:rotate(180deg);" viewBox="0 0 24 24"><path d="M7 10l5 5 5-5z"/></svg>
            </button>
            <div class="accordion-body" id="signalBody">
                <div class="signal-row"><span>1차 매수 진입 타점</span><strong style="color:#fff;">175,800 원</strong></div>
                <div class="signal-row"><span>2차 분할 진입 타점</span><strong style="color:#fff;">172,000 원</strong></div>
                <div class="signal-row"><span>1차 목표 익절가 (+4.5%)</span><strong style="color:#ef4444;">186,500 원</strong></div>
                <div class="signal-row"><span>2차 목표 익절가 (+8.5%)</span><strong style="color:#ef4444;">193,500 원</strong></div>
                <div class="signal-row"><span>리스크 방어 손절가 (-5.5%)</span><strong style="color:#38bdf8;">168,500 원</strong></div>
                <div style="font-size:0.75rem; color:#8e918f; text-align:center; padding-top:8px; margin-top:6px; border-top:1px dashed #37393d;">
                    * 1년 백테스트 검증 승률 71.4% / 손익비 1:2.4
                </div>
            </div>
        </div>

        <!-- [6] 🔒 마스터 전략 브리핑 (유료화 토글 아코디언) -->
        <div>
            <button class="accordion-btn" onclick="toggleAccordion('masterBody', 'masterArrow')">
                <span>✍️ gaemiGTP 마스터 전략 브리핑 <span class="pro-badge">PRO</span></span>
                <svg class="accordion-arrow" id="masterArrow" style="transform:rotate(180deg);" viewBox="0 0 24 24"><path d="M7 10l5 5 5-5z"/></svg>
            </button>
            <div class="accordion-body" id="masterBody">
                <div class="master-content-box">
                    <div style="color:#f59e0b; font-weight:700; font-size:0.82rem;">[SK하이닉스 수석 전략 뷰]</div>
                    <p style="font-size:0.78rem; margin-top:3px;">
                        외인 순매수 유입과 20일선 지지 안착이 확인되어 단기 상승 에너지가 축적된 상태입니다.
                    </p>
                    <div class="blur-text">
                        목표가 도달 전 182,000원 구간에서의 매물대 소화 전략과 세력 수급 분할 매도 시점, 그리고 긴급 하방 리스크 방어를 위한 숨은 손절선 가이드 전문...
                    </div>
                    <div class="lock-overlay">
                        <div style="font-size:0.75rem; font-weight:700; color:#fff; margin-bottom:6px;">🔒 마스터 정밀 전략 브리핑</div>
                        <button class="lock-btn" onclick="alert('PRO 멤버십 가입 페이지로 연결됩니다!');">
                            PRO 멤버십으로 전문 확인 →
                        </button>
                    </div>
                </div>
            </div>
        </div>

        <!-- [7] 글로벌 매크로 & 3줄 속보 & 경제 캘린더 -->
        <div class="card">
            <div class="section-header">
                <span class="section-title">🌐 글로벌 매크로 & 경제 일정</span>
                <span class="badge-tag">실시간 피드</span>
            </div>

            <!-- 매크로 지수 3열 -->
            <div class="macro-grid">
                <div class="macro-item">
                    <div class="macro-sym">US10Y 국채</div>
                    <div class="macro-val val-down">3.81% (-2bp)</div>
                </div>
                <div class="macro-item">
                    <div class="macro-sym">WTI 유가</div>
                    <div class="macro-val val-up">$75.40 (+1.2%)</div>
                </div>
                <div class="macro-item">
                    <div class="macro-sym">QQQ (나스닥)</div>
                    <div class="macro-val val-down">$709.02 (-0.2%)</div>
                </div>
            </div>

            <!-- 3줄 요약 글로벌 속보 -->
            <div class="brief-box">
                <div class="brief-title">⚡ 연준 "인플레이션 둔화세 확인, 9월 금리 인하 적절"</div>
                <div class="brief-bullets">
                    <div>• 노동 시장 둔화 방지를 위한 선제적 완화 공감대 형성</div>
                    <div>• 국채 금리 하락 및 기술주 매수세 유입 견인</div>
                </div>
            </div>

            <!-- 주요 경제 일정 -->
            <div class="cal-row">
                <span class="cal-d">08.28 (금)</span>
                <span class="cal-t">미국 근원 PCE 물가지수</span>
                <span class="cal-v">예상 2.6%</span>
            </div>
            <div class="cal-row">
                <span class="cal-d">08.28 (금)</span>
                <span class="cal-t">엔비디아 (NVDA) 실적 발표</span>
                <span class="cal-v">장 마감후</span>
            </div>
        </div>

    </div>

    <script>
        function toggleDrawer() {
            const drawer = document.getElementById('sideDrawer');
            const overlay = document.getElementById('drawerOverlay');
            drawer.classList.toggle('active');
            overlay.classList.toggle('active');
        }

        function toggleAccordion(bodyId, arrowId) {
            const body = document.getElementById(bodyId);
            const arrow = document.getElementById(arrowId);
            if (body.style.display === 'none') {
                body.style.display = 'block';
                arrow.style.transform = 'rotate(180deg)';
            } else {
                body.style.display = 'none';
                arrow.style.transform = 'rotate(0deg)';
            }
        }
    </script>

</body>
</html>
