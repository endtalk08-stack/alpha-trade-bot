/* gaemiGTP Widget: 증시는 지금
 * 역할: 세계시장 보드 UI + 해당 위젯의 로딩만 담당.
 * 분석/기타 데이터 처리는 bot.py의 기존 API를 그대로 사용한다.
 */
(function (window) {
    'use strict';

    window.GaemiWidgets = window.GaemiWidgets || {};

    function render(rows) {
        const root = document.getElementById('worldBoardGrid');
        if (!root) return;

        const items = Array.isArray(rows) ? rows : [];
        if (!items.length) {
            root.innerHTML = '<div class="us-leading-sector-loading">세계시장 데이터를 불러오지 못했습니다.</div>';
            return;
        }

        root.innerHTML = items.map(w => {
            const cls = w.up ? 'val-up' : 'val-down';
            const sparkSvg = typeof window.renderSparklineSVG === 'function'
                ? window.renderSparklineSVG(w.sparkline, w.up)
                : '';
            const clickName = w.name === 'USD/KRW' ? '환율' : w.name;
            const safeClick = String(clickName || '').replace(/'/g, "\\'");
            return `
                <div class="mini-index-card" onclick="selectStock('${safeClick}')">
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
                </div>`;
        }).join('');
    }

    async function load(force) {
        try {
            const response = await fetch('/api/world-board' + (force ? '?force=1' : ''));
            const data = await response.json().catch(() => null);
            if (!response.ok || !data || data.error) {
                throw new Error((data && data.error) || '세계시장 데이터 오류');
            }
            render(data.data);
            return data.data;
        } catch (error) {
            console.error('World board:', error);
            const box = document.getElementById('worldBoardGrid');
            if (box) box.innerHTML = '<div class="us-leading-sector-loading">세계시장 데이터를 잠시 후 다시 불러옵니다.</div>';
            return null;
        }
    }

    window.GaemiWidgets.worldBoard = { render, load };
    // 기존 코드와의 호환을 유지한다.
    window.renderWorldBoard = render;
    window.loadWorldBoard = load;
})(window);
