/* gaemiGTP Widget: 퀀트 분석
 * 역할: 기존 data.quant 결과를 퀀트 분석 카드에 표시한다.
 * 계산/API 로직은 기존 bot.py를 그대로 사용하고, 이 파일은 카드 렌더링만 담당한다.
 */
(function (window) {
    'use strict';

    window.GaemiWidgets = window.GaemiWidgets || {};

    function setTxt(id, value) {
        const el = document.getElementById(id);
        if (el) el.textContent = value == null ? '' : String(value);
    }

    function setHtml(id, value) {
        const el = document.getElementById(id);
        if (el) el.innerHTML = value == null ? '' : String(value);
    }

    function render(q) {
        if (!q) return;

setTxt('qScoreVal', q.quant_score);
setTxt('qScoreBadge', q.score_badge);
setHtml('qScoreDots', q.score_dots);

setTxt('qZScoreVal', q.z_score);
setTxt('qVolVal', `${q.ann_vol} (${q.vol_risk_state ? q.vol_risk_state.split(' ')[0] : ''})`);

const qSignalChart = document.getElementById('qSignalChart');
const qSignalNow = document.getElementById('qMomentumValTxt');
if (qSignalChart && Array.isArray(q.momentum_history) && q.momentum_history.length > 1) {
    const scores = q.momentum_history.map(x => Number(x.score));
    const w = 600;
    const h = 120;
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
        <div class="quant-row-item">
            <div class="quant-row-left">
                <span class="quant-row-name">최근 20일 평균 거래대금</span>
                <span class="quant-row-status status-pill-neutral">실제 데이터</span>
            </div>
            <div class="quant-row-right">
                <div class="quant-row-val">${q.tval_20d || '-'}</div>
                <div class="quant-row-subval">거래대금 기준</div>
            </div>
        </div>
    `;
    }
    }

    window.GaemiWidgets.quant = Object.freeze({ render });
})(window);
