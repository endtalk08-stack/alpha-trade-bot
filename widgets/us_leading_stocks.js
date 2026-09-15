/* gaemiGTP Widget: 미국 주도종목
 * 역할: 미국 주도종목 TOP 5의 표시/로딩/선택 이벤트만 담당.
 * 데이터/API는 기존 /api/us-leading-stocks 흐름을 그대로 사용한다.
 */
(function (window) {
    'use strict';
    window.GaemiWidgets = window.GaemiWidgets || {};

    function escapeHtml(value) {
        return String(value == null ? '' : value)
            .replace(/&/g, '&amp;').replace(/</g, '&lt;')
            .replace(/>/g, '&gt;').replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    function render(rows, sectorName) {
        const list = document.getElementById('usLeadingStockList');
        const title = document.getElementById('usLeadingStockTitle');
        if (!list) return;
        if (title && sectorName) title.textContent = `미국 ${String(sectorName).trim()} 주도종목`;

        const items = Array.isArray(rows) ? rows : [];
        if (!items.length) {
            list.innerHTML = '<div class="us-leading-sector-loading">현재 주도종목 데이터를 불러오지 못했습니다.</div>';
            return;
        }

        const pct = value => `${Number(value) >= 0 ? '+' : ''}${Number(value).toFixed(2)}%`;
        const cls = value => Number(value) >= 0 ? 'up' : 'down';
        list.innerHTML = items.map(row => {
            const symbol = escapeHtml(row.symbol || '');
            const name = escapeHtml(row.name || '');
            return `<div class="us-leading-stock-item" role="button" tabindex="0"
                         data-symbol="${symbol}" data-name="${name}">
                        <div class="us-leading-stock-name">
                            <div class="us-leading-stock-ticker">${symbol}</div>
                            <div class="us-leading-stock-company">${name}</div>
                        </div>
                        <div class="us-leading-stock-price">
                            <div class="us-leading-stock-value">${typeof window.formatUsStockPrice === 'function' ? window.formatUsStockPrice(row.price) : String(row.price || '')}</div>
                            <div class="us-leading-stock-change ${cls(row.current)}">${pct(row.current)}</div>
                        </div>
                        ${typeof window.buildUsStockSparkline === 'function' ? window.buildUsStockSparkline(row.sparkline) : ''}
                    </div>`;
        }).join('');

        list.querySelectorAll('.us-leading-stock-item').forEach(el => {
            const open = () => {
                if (typeof window.openUsLeadingStock === 'function') {
                    window.openUsLeadingStock(el.dataset.symbol || '', el.dataset.name || '');
                }
            };
            el.addEventListener('click', open);
            el.addEventListener('keydown', event => {
                if (event.key === 'Enter' || event.key === ' ') {
                    event.preventDefault();
                    open();
                }
            });
        });
    }

    async function load(sectorSymbol, sectorName) {
        const list = document.getElementById('usLeadingStockList');
        const title = document.getElementById('usLeadingStockTitle');
        if (!list || !sectorSymbol) return null;
        if (title) title.textContent = `미국 ${String(sectorName || '').trim()} 주도종목`;
        list.innerHTML = '<div class="us-leading-sector-loading">실제 주도종목 데이터를 불러오는 중…</div>';
        try {
            const response = await fetch('/api/us-leading-stocks?sector=' + encodeURIComponent(sectorSymbol));
            const data = await response.json().catch(() => null);
            if (!response.ok || !data || data.error) throw new Error((data && data.error) || '데이터 오류');
            render(data.data, sectorName);
            return data.data;
        } catch (error) {
            console.error('US leading stocks:', error);
            list.innerHTML = '<div class="us-leading-sector-loading">주도종목 데이터를 잠시 후 다시 불러옵니다.</div>';
            return null;
        }
    }

    window.GaemiWidgets.usLeadingStocks = { render, load };
    window.loadUsLeadingStocks = load;
})(window);
