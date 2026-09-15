/* gaemiGTP Track Analysis widget — extracted from 1site bot.py */
(function(){
  const GaemiWidgets = window.GaemiWidgets = window.GaemiWidgets || {};
  function render(data, mr, evidenceReasons, evidenceCount, confidenceStyle) {
    // 트랙 분석: 퀀트 분석과 동일한 카드형 레이아웃을 사용한다.
    const trackPriceVal = document.getElementById('tPriceVal');
    const trackChangeVal = document.getElementById('tChangeVal');
    const trackEvidenceVal = document.getElementById('tEvidenceCountVal');
    const trackStatusBadge = document.getElementById('tStatusBadge');
    const trackStatusDots = document.getElementById('tStatusDots');
    const trackPriceTrendTxt = document.getElementById('tPriceTrendTxt');
    
    if (trackPriceVal) trackPriceVal.innerText = data.price_str || '-';
    
    const cp = Number(data.current_change_pct);
    if (trackChangeVal) {
        trackChangeVal.innerText = Number.isFinite(cp) ? `${cp >= 0 ? '+' : ''}${cp.toFixed(2)}%` : '-';
        trackChangeVal.style.color =
            Number.isFinite(cp) && cp > 0 ? '#10b981' :
            (Number.isFinite(cp) && cp < 0 ? '#ef4444' : 'var(--text-title)');
    }
    
    if (trackEvidenceVal) {
        trackEvidenceVal.innerText = `${evidenceCount}/8`;
    }
    if (trackStatusBadge) {
        trackStatusBadge.innerText = mr.confidence || '-';
        trackStatusBadge.style.cssText = confidenceStyle;
    }
    if (trackStatusDots) {
        trackStatusDots.innerText =
            evidenceCount > 0
                ? `확인된 근거 ${evidenceCount}개 · 실제 데이터 기준`
                : '확인된 근거 부족 · 추가 확인 필요';
    }
    
    // 최근 60거래일: 현재 응답의 실제 candles를 사용한다.
    const tPriceChart = document.getElementById('tPriceChart');
    const candleRows = Array.isArray(data.candles) ? data.candles.slice(-60) : [];
    
    // 서버가 보내는 candles 필드는 open/high/low/close 형식이다.
    // 기존 코드가 x.c를 읽고 있어 차트 값이 전부 비어 있던 문제를 수정한다.
    // close를 우선 사용하고, 혹시 다른 응답 형식이 들어와도 c를 보조로 사용한다.
    const trackVals = candleRows
        .map(x => {
            const close = Number(x && x.close);
            if (Number.isFinite(close)) return close;
            const legacyClose = Number(x && x.c);
            return Number.isFinite(legacyClose) ? legacyClose : NaN;
        })
        .filter(Number.isFinite);
    
    if (tPriceChart && trackVals.length > 1) {
        const first = trackVals[0];
        const normalized = first !== 0 ? trackVals.map(v => ((v / first) - 1) * 100) : [];
        if (normalized.length > 1 && normalized.every(Number.isFinite)) {
            const W = 600, H = 120, pad = 8;
            const minV = Math.min(...normalized), maxV = Math.max(...normalized);
            const span = Math.max(0.01, maxV - minV);
            const x = i => pad + i * ((W - pad * 2) / Math.max(1, normalized.length - 1));
            const y = v => pad + ((maxV - v) / span) * (H - pad * 2);
            const coords = normalized.map((v,i) => `${x(i).toFixed(1)},${y(v).toFixed(1)}`);
            const linePath = 'M ' + coords.join(' L ');
            const baseY = y(0);
            const areaPath = `M ${coords[0].split(',')[0]},${baseY.toFixed(1)} L ${coords.join(' L ')} L ${coords[coords.length-1].split(',')[0]},${baseY.toFixed(1)} Z`;
            const last = normalized[normalized.length - 1];
            const stroke = last >= 0 ? '#10b981' : '#ef4444';
    
            tPriceChart.innerHTML = `
                <defs>
                    <linearGradient id="tPriceFill" x1="0" x2="0" y1="0" y2="1">
                        <stop offset="0%" stop-color="${stroke}" stop-opacity="0.28"/>
                        <stop offset="100%" stop-color="${stroke}" stop-opacity="0.02"/>
                    </linearGradient>
                </defs>
                <line x1="0" y1="${baseY.toFixed(1)}" x2="600" y2="${baseY.toFixed(1)}"
                      stroke="rgba(148,163,184,0.20)" stroke-width="1" stroke-dasharray="4 4"/>
                <path d="${areaPath}" fill="url(#tPriceFill)"/>
                <path d="${linePath}" fill="none" stroke="${stroke}" stroke-width="3"
                      vector-effect="non-scaling-stroke"/>
                <circle cx="${coords[coords.length-1].split(',')[0]}"
                        cy="${coords[coords.length-1].split(',')[1]}"
                        r="5" fill="${stroke}" stroke="#ffffff" stroke-width="2"
                        vector-effect="non-scaling-stroke"/>
            `;
    
            if (trackPriceTrendTxt) {
                const p = normalized[normalized.length - 1];
                trackPriceTrendTxt.innerText =
                    `최근 60D ${p >= 0 ? '+' : ''}${p.toFixed(2)}%`;
            }
        }
    } else if (trackPriceTrendTxt) {
        trackPriceTrendTxt.innerText =
            trackVals.length === 0 ? '데이터 부족' : '주가 흐름 데이터 부족';
    }
    
    // 근거 확인: 확인된 실제 근거 데이터를 최대 8칸의 색상 막대로 표시한다.
    const evidenceBar = document.getElementById('tEvidenceBar');
    const evidenceBarLabel = document.getElementById('tEvidenceBarLabel');
    if (evidenceBar) {
        const evidenceItems = Array.isArray(evidenceReasons) ? evidenceReasons.slice(0, 8) : [];
        const confirmedCount = evidenceItems.filter(r => {
            const score = Number(r && r.score);
            return Number.isFinite(score) && score !== 0;
        }).length;
    
        evidenceBar.innerHTML = Array.from({length: 8}, (_, idx) => {
            const r = evidenceItems[idx];
            const score = Number(r && r.score);
            let bg = 'var(--bg-subcard)';
            let border = '1px solid var(--border-line)';
            if (Number.isFinite(score) && score > 0) {
                bg = '#10b981';
                border = '1px solid #10b981';
            } else if (Number.isFinite(score) && score < 0) {
                bg = '#ef4444';
                border = '1px solid #ef4444';
            } else if (r) {
                bg = '#9ca3af';
                border = '1px solid #9ca3af';
            }
            return `<div title="${r ? (r.label || '근거') : '확인된 근거 없음'}"
                         style="height:100%; background:${bg}; border:${border}; border-radius:3px; min-width:0;"></div>`;
        }).join('');
    
        if (evidenceBarLabel) {
            evidenceBarLabel.innerText = `${confirmedCount} / 8`;
        }
    }
    
    // 상세 근거: 표시 순서만 고정한다. 분석 데이터/점수 계산은 변경하지 않는다.
    const trackDisplayOrder = [
        'market_5d',
        'market',
        'theme',
        'foreign',
        'institution',
        'material',
        'volume',
        'trading_value'
    ];
    const orderedEvidenceReasons = [...evidenceReasons].sort((a, b) => {
        const ai = trackDisplayOrder.indexOf(a.key);
        const bi = trackDisplayOrder.indexOf(b.key);
        return (ai === -1 ? 999 : ai) - (bi === -1 ? 999 : bi);
    });
    
    const trackDetailList = document.getElementById('trackDetailList');
    if (trackDetailList) {
        trackDetailList.innerHTML = orderedEvidenceReasons.map(r => {
            const score = Number(r.score || 0);
            const status = score > 0 ? '우호적' : score < 0 ? '부정적' : '중립';
            const statusClass =
                score > 0 ? 'status-pill-positive' :
                score < 0 ? 'status-pill-negative' : 'status-pill-neutral';
    
            return `<div class="quant-row-item">
                <div class="quant-row-left">
                    <span class="quant-row-name">${r.label || '-'}</span>
                    <span class="quant-row-status ${statusClass}">${status}</span>
                </div>
                <div class="quant-row-right">
                    <div class="quant-row-val">${String(r.detail || '-')}</div>
                </div>
            </div>`;
        }).join('') || `<div style="padding:12px 0;color:var(--text-muted);font-size:0.78rem;">확인된 데이터가 없습니다.</div>`;
    }
  }
  GaemiWidgets.track = { render };
})();
