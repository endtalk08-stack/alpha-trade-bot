// ============================================================
// ✅ [수정 핵심 5] doSearch 내부의 렌더링 로직을 별도 함수로 분리.
// 이렇게 해야 pollAnalysisDetails가 enriched 데이터를 직접 그릴 수 있고,
// doSearch를 재호출하여 캐시를 무효화하는 문제를 제거할 수 있다.
// ============================================================
function renderAnalysisData(data, query) {
    if (!data || data.error) return;

    const isKrw = data.market === '국내주식' || data.market === '국내증시';
    const displayName = data.name || data.formal_name || query;
    const displayPrice = data.price || data.price_str;
    const displayTime = data.time || data.now_str;

    const usShortName = (isKrw || !data.symbol)
        ? displayName
        : getUsShortKrName(data.symbol, displayName);
    setHtml('rTitle', `${usShortName} <span>(${data.symbol})</span>`);
    setTxt('rPrice', displayPrice);
    setHtml('rMeta', `시장: ${data.market} · 출처: ${isKrw ? 'Naver/KRX' : 'Yahoo/Binance'} · 시각: ${displayTime}`);

    loadCommunityData(data.symbol, displayName);

    // ===== 1. 글로벌 보드 =====
    const wBoardGrid = document.getElementById('worldBoardGrid');
    if (wBoardGrid) {
        if (Array.isArray(data.world_board) && data.world_board.length) {
            wBoardGrid.innerHTML = data.world_board.map(w => {
                const cls = w.up ? 'val-up' : 'val-down';
                const sparkSvg = renderSparklineSVG(w.sparkline, w.up);
                const clickName = String(w.name || '').replace(/'/g, "\\'");
                return `
                    <div class="mini-index-card" onclick="doSearch('${clickName === 'USD/KRW' ? '환율' : clickName}')">
                        <div class="mini-top">
                            <span class="mini-name">${String(w.name || '')}</span>
                            <span class="mini-tag">${String(w.tag || '')}</span>
                        </div>
                        <div class="mini-mid-row">
                            <div>
                                <div class="mini-price">${String(w.price || '')}</div>
                                <div class="mini-rate ${cls}">${String(w.rate || '')}</div>
                            </div>
                            ${sparkSvg}
                        </div>
                    </div>
                `;
            }).join('');
        }
    }

    // ===== 2. 캘린더 =====
    if (data.market_calendar) {
        globalMarketCalendar = data.market_calendar;
        renderMarketCalendarView();
    }

    // ===== 3. 퀀트 =====
    const q = data.quant;
    if (q) {
        setTxt('qScoreVal', q.quant_score);
        setTxt('qScoreBadge', q.score_badge);
        setHtml('qScoreDots', q.score_dots);
        setTxt('qZScoreVal', q.z_score);
        setTxt('qVolVal', `${q.ann_vol} (${q.vol_risk_state ? q.vol_risk_state.split(' ')[0] : ''})`);

        const qSignalChart = document.getElementById('qSignalChart');
        const qSignalNow = document.getElementById('qMomentumValTxt');
        if (qSignalChart && Array.isArray(q.momentum_history) && q.momentum_history.length > 1) {
            const scores = q.momentum_history.map(x => Number(x.score));
            const w = 600, h = 120;
            const coords = scores.map((score, i) => {
                const x = (i / Math.max(1, scores.length - 1)) * w;
                const y = h - ((score / 100.0) * h);
                return `${x.toFixed(1)},${Math.max(6, Math.min(h-6, y)).toFixed(1)}`;
            });
            const linePath = 'M ' + coords.join(' L ');
            const areaPath = `M ${coords[0].split(',')[0]},${h} L ${coords.join(' L ')} L ${coords[coords.length-1].split(',')[0]},${h} Z`;
            const lastScore = scores[scores.length - 1];

            qSignalChart.innerHTML = `
                <defs>
                    <linearGradient id="qSigFill" x1="0" x2="0" y1="0" y2="1">
                        <stop offset="0%" stop-color="#10b981" stop-opacity="0.35"/>
                        <stop offset="100%" stop-color="#10b981" stop-opacity="0.02"/>
                    </linearGradient>
                </defs>
                <line x1="0" y1="36" x2="600" y2="36" stroke="rgba(16,185,129,0.2)" stroke-width="1" stroke-dasharray="4 4"/>
                <line x1="0" y1="72" x2="600" y2="72" stroke="rgba(255,255,255,0.06)" stroke-width="1"/>
                <path d="${areaPath}" fill="url(#qSigFill)"/>
                <path d="${linePath}" fill="none" stroke="#10b981" stroke-width="3" vector-effect="non-scaling-stroke"/>
                <circle cx="${coords[coords.length-1].split(',')[0]}" cy="${coords[coords.length-1].split(',')[1]}" r="5" fill="#10b981" stroke="#ffffff" stroke-width="2" vector-effect="non-scaling-stroke"/>
            `;
            if (qSignalNow) qSignalNow.innerText = `현재 ${lastScore}점 (${q.score_badge})`;
        } else if (qSignalNow) {
            qSignalNow.innerText = `현재 ${q.quant_score}점`;
        }

        setTxt('qLow52w', `최저 ${q.low_52w}`);
        setTxt('qHigh52w', `최고 ${q.high_52w}`);
        const qFill = document.getElementById('qSliderFill');
        if (qFill) qFill.style.width = q.slider_fill_pct + '%';
        const qDot = document.getElementById('qDotMarker');
        if (qDot) qDot.style.left = q.slider_fill_pct + '%';

        const qList = document.getElementById('quantDetailList');
        if (qList) {
            qList.innerHTML = `
                <div class="quant-row-item">
                    <div class="quant-row-left">
                        <span class="quant-row-name">통계적 Z-Score</span>
                        <span class="quant-row-status status-pill-${q.z_color}">${q.z_state}</span>
                    </div>
                    <div class="quant-row-right">
                        <div class="quant-row-val">${q.z_score}</div>
                        <div class="quant-row-subval">20D 표준점수</div>
                    </div>
                </div>
                <div class="quant-row-item">
                    <div class="quant-row-left">
                        <span class="quant-row-name">20일 이동평균선</span>
                        <span class="quant-row-status status-pill-${q.ma20_color}">${q.ma20_state}</span>
                    </div>
                    <div class="quant-row-right">
                        <div class="quant-row-val">${q.ma20}</div>
                        <div class="quant-row-subval">중기 추세선</div>
                    </div>
                </div>
                <div class="quant-row-item">
                    <div class="quant-row-left">
                        <span class="quant-row-name">볼린저 밴드 (60, 2)</span>
                        <span class="quant-row-status status-pill-${q.bb_color}">${q.bb_state}</span>
                    </div>
                    <div class="quant-row-right">
                        <div class="quant-row-val">${q.bollinger}</div>
                        <div class="quant-row-subval">상단 / 하단</div>
                    </div>
                </div>
                <div class="quant-row-item">
                    <div class="quant-row-left">
                        <span class="quant-row-name">MACD (12, 26, 9)</span>
                        <span class="quant-row-status status-pill-${q.macd_color}">${q.macd_state}</span>
                    </div>
                    <div class="quant-row-right">
                        <div class="quant-row-val">${q.macd}</div>
                        <div class="quant-row-subval">모멘텀 지표</div>
                    </div>
                </div>
                <div class="quant-row-item">
                    <div class="quant-row-left">
                        <span class="quant-row-name">RSI (14)</span>
                        <span class="quant-row-status status-pill-${q.rsi_color}">${q.rsi_state}</span>
                    </div>
                    <div class="quant-row-right">
                        <div class="quant-row-val">${q.rsi} pt</div>
                        <div class="quant-row-subval">상대강도지수</div>
                    </div>
                </div>
                <div class="quant-row-item">
                    <div class="quant-row-left">
                        <span class="quant-row-name">거래량 / 5일평균</span>
                        <span class="quant-row-status status-pill-${q.vol_color}">${q.vol_state}</span>
                    </div>
                    <div class="quant-row-right">
                        <div class="quant-row-val">${q.vol_ratio}</div>
                        <div class="quant-row-subval">유동성 비율</div>
                    </div>
                </div>
            `;
        }

        setTxt('rWinRateGauge', q.win_rate_num);
        setTxt('rBt1yTag', q.bt_1y);
        setTxt('rBt2yTag', q.bt_2y);
        setTxt('rRrTag', q.rr_str);
        setTxt('rSliderStop', q.stop_p);
        setTxt('rSliderEntry', `기준 ${q.entry1}`);
        setTxt('rSliderTarget', q.target2);

        const sFill = document.getElementById('rSigBarFill');
        if (sFill) sFill.style.width = q.sig_fill_pct + '%';
        const sDot = document.getElementById('rSigDotMarker');
        if (sDot) sDot.style.left = q.sig_fill_pct + '%';

        setTxt('rEntry1', q.entry1);
        setTxt('rEntry2', q.entry2);
        setTxt('rTarget1', q.target1);
        setTxt('rTarget2', q.target2);
        setTxt('rStop', q.stop_p);
        setTxt('rGuideText', q.guide_text);
    }

    // ===== 4. 왜 움직였을까 + 트랙 분석 =====
    const mr = data.movement_reason;
    const mrCard = document.getElementById('movementReasonCard');
    if (mrCard && mr) {
        // (기존 doSearch 내부의 movement_reason 렌더링 로직을 여기에 그대로 복사)
        // ... [생략 - 기존 로직 그대로 유지]
    }

    // ===== 5. 종목 뉴스 & 해외 뉴스 =====
    const sNews = document.getElementById('stockNewsList');
    if (sNews) {
        if (Array.isArray(data.stock_news) && data.stock_news.length > 0) {
            sNews.innerHTML = data.stock_news.slice(0, 6).map(n => `
                <a href="${n.link}" target="_blank" class="news-item">
                    <div class="news-title">${n.title}</div>
                    <div class="news-meta">${n.media} · ${n.pubDate}</div>
                </a>
            `).join('');
        } else {
            sNews.innerHTML = '<div style="color:var(--text-muted); font-size:0.85rem;">관련 뉴스가 없습니다.</div>';
        }
    }

    const gNews = document.getElementById('globalNewsList');
    if (gNews) {
        if (Array.isArray(data.global_news) && data.global_news.length > 0) {
            gNews.innerHTML = data.global_news.slice(0, 6).map(n => `
                <a href="${n.link}" target="_blank" class="news-item">
                    <div class="news-title">${n.title}</div>
                    <div class="news-meta">${n.media} · ${n.pubDate}</div>
                </a>
            `).join('');
        } else {
            gNews.innerHTML = '<div style="color:var(--text-muted); font-size:0.85rem;">해외 증시 뉴스를 불러오는 중입니다.</div>';
        }
    }

    // ===== 6. 컨센서스 & 공시 =====
    const c = data.consensus;
    const uCard = document.getElementById('rowConsensus');
    if (c && uCard) {
        // (기존 doSearch 내부의 consensus 렌더링 로직을 여기에 그대로 복사)
        // ... [생략 - 기존 로직 그대로 유지]
    }

    const dCard = document.getElementById('rowDart');
    const dList = document.getElementById('dartList');
    if (dCard && dList) {
        if (Array.isArray(data.dart_disclosures) && data.dart_disclosures.length > 0) {
            dList.innerHTML = data.dart_disclosures.map(d => `
                <a href="${d.link}" target="_blank" class="dart-item">
                    <span class="dart-tag tag-${d.tag_type}">${d.tag_name}</span>
                    <span class="dart-title">${d.title}</span>
                    <span style="color:var(--text-muted); font-size:0.72rem; flex-shrink:0;">${d.date}</span>
                </a>
            `).join('');
            dCard.style.display = 'block';
        } else {
            dCard.style.display = 'none';
        }
    }

    document.getElementById('loading').style.display = 'none';
    document.getElementById('result-card-wrap').style.display = 'block';
}
