/* gaemiGTP Widget: 미국 주도업종
 * 역할: 미국 주도업종 TOP 10의 표시/로딩/선택 이벤트만 담당.
 * 데이터/API와 업종 차트·주도종목 분석은 기존 bot.py 흐름을 그대로 사용한다.
 */
(function (window) {
    'use strict';

    window.GaemiWidgets = window.GaemiWidgets || {};

    function render(rows) {
        const box = document.getElementById('usLeadingSectorList');
        if (!box) return;

        const items = Array.isArray(rows) ? rows : [];
        if (!items.length) {
            box.innerHTML = '<div class="us-leading-sector-loading">현재 업종 데이터를 불러오지 못했습니다.</div>';
            return;
        }

        const pct = value => `${Number(value) >= 0 ? '+' : ''}${Number(value).toFixed(1)}%`;
        const cls = value => Number(value) >= 0 ? 'us-leading-up' : 'us-leading-down';

        let html = '<table class="us-leading-sector-table">' +
            '<thead><tr><th>업종</th><th>현재</th><th>1W</th><th>1M</th><th>3M</th></tr></thead><tbody>';

        items.forEach(row => {
            const safeName = String(row.name || '').replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
            const safeSym = String(row.sym || '').replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
            html += `<tr class="us-leading-sector-row" title="${safeName} 업종 차트 보기" data-sector-symbol="${safeSym}" data-sector-name="${safeName}">` +
                `<td class="us-leading-name">${safeName}</td>` +
                `<td class="${cls(row.current)}">${pct(row.current)}</td>` +
                `<td class="${cls(row.week1)}">${pct(row.week1)}</td>` +
                `<td class="${cls(row.month1)}">${pct(row.month1)}</td>` +
                `<td class="${cls(row.month3)}">${pct(row.month3)}</td>` +
                `</tr>`;
        });
        html += '</tbody></table>';
        box.innerHTML = html;

        box.querySelectorAll('.us-leading-sector-row').forEach(rowEl => {
            rowEl.addEventListener('click', () => {
                const sectorSymbol = rowEl.dataset.sectorSymbol || '';
                const sectorName = rowEl.dataset.sectorName || '';
                if (typeof window.loadSectorChart === 'function') {
                    window.loadSectorChart(sectorSymbol, sectorName);
                }
                if (typeof window.loadUsLeadingStocks === 'function') {
                    window.loadUsLeadingStocks(sectorSymbol, sectorName);
                }
            });
        });

        const strongestRow = box.querySelector('.us-leading-sector-row');
        if (strongestRow && typeof window.loadUsLeadingStocks === 'function') {
            window.loadUsLeadingStocks(
                strongestRow.dataset.sectorSymbol || '',
                strongestRow.dataset.sectorName || ''
            );
        }
    }

    async function load(force) {
        const box = document.getElementById('usLeadingSectorList');
        if (!box) return null;
        try {
            const url = '/api/us-leading-sectors' + (force ? '?force=1' : '');
            const response = await fetch(url);
            const data = await response.json().catch(() => null);
            if (!response.ok || !data || data.error) {
                throw new Error((data && data.error) || '데이터 오류');
            }
            render(data.data);
            return data.data;
        } catch (error) {
            console.error('US leading sectors:', error);
            box.innerHTML = '<div class="us-leading-sector-loading">업종 데이터를 잠시 후 다시 불러옵니다.</div>';
            return null;
        }
    }

    window.GaemiWidgets.usLeadingSectors = { render, load };
    // 기존 코드와의 호환을 유지한다.
    window.renderUsLeadingSectors = render;
    window.loadUsLeadingSectors = load;
})(window);
