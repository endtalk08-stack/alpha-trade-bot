/* gaemiGTP Widget: 종목 뉴스
 * 데이터 수집은 독립 API(/api/stock-news), 렌더링만 이 파일이 담당한다.
 */
(function (window) {
    'use strict';

    window.GaemiWidgets = window.GaemiWidgets || {};
    let requestToken = 0;
    let controller = null;
    let hasRenderedOnce = false;

    function esc(value) {
        return String(value ?? '').replace(/[&<>"']/g, ch => ({
            '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
        }[ch]));
    }

    function renderKeyNews(items) {
        const section = document.getElementById('keyNewsSection');
        const root = document.getElementById('keyNewsList');
        if (!section || !root) return;
        const rows = Array.isArray(items) ? items.slice(0, 2) : [];
        if (!rows.length) {
            section.style.display = 'none';
            root.innerHTML = '';
            return;
        }
        section.style.display = '';
        root.innerHTML = `
            <div class="key-news-grid">
                ${rows.map(n => {
                    const label = String(n.sentiment_label || '중립');
                    const labelStyle = label === '호재'
                        ? 'background:rgba(16,185,129,.13);color:#10b981;border:1px solid rgba(16,185,129,.24);'
                        : label === '악재'
                            ? 'background:rgba(239,68,68,.13);color:#ef4444;border:1px solid rgba(239,68,68,.24);'
                            : label === '시장'
                                ? 'background:rgba(59,130,246,.13);color:#60a5fa;border:1px solid rgba(59,130,246,.24);'
                                : 'background:rgba(148,163,184,.10);color:var(--text-muted);border:1px solid var(--border-line);';
                    return `
                        <a href="${esc(n.link || '#')}" target="_blank" rel="noopener noreferrer" class="key-news-card">
                            <div style="display:flex;align-items:center;gap:7px;margin-bottom:9px;">
                                <span style="${labelStyle}font-size:.68rem;font-weight:800;padding:3px 7px;border-radius:6px;">${esc(label)}</span>
                                <span style="font-size:.68rem;color:var(--text-muted);">${esc(n.media || n.source || '')}</span>
                            </div>
                            <div style="font-size:.91rem;font-weight:800;line-height:1.45;color:var(--text-title);word-break:keep-all;">${esc(n.title || '-')}</div>
                            <div style="font-size:.68rem;color:var(--text-muted);margin-top:9px;">${esc(n.pubDate || '')}</div>
                        </a>`;
                }).join('')}
            </div>`;
    }

    function render(items) {
        const root = document.getElementById('stockNewsList');
        if (!root) return;
        if (!Array.isArray(items) || items.length === 0) {
            root.innerHTML = '<div style="color:var(--text-muted); font-size:0.85rem;">관련 뉴스가 없습니다.</div>';
            return;
        }
        const allNews = items.slice(0, 10);
        const firstCount = 6;
        const moreCount = 4;
        let visibleCount = Math.min(firstCount, allNews.length);
        const draw = () => {
            root.innerHTML = allNews.slice(0, visibleCount).map(n => `
                <a href="${n.link || '#'}" target="_blank" rel="noopener noreferrer" class="news-item">
                    <div class="news-title">${n.title || '-'}</div>
                    <div class="news-meta">${n.media || '-'} · ${n.pubDate || '-'}</div>
                </a>`).join('');
            if (visibleCount < allNews.length || visibleCount > firstCount) {
                const moreBtn = document.createElement('button');
                moreBtn.type = 'button';
                moreBtn.textContent = visibleCount < allNews.length
                    ? `더보기 (${Math.min(moreCount, allNews.length - visibleCount)}개)` : '접기';
                moreBtn.style.cssText = `width:100%;margin-top:10px;padding:9px 0;border:1px solid var(--border-line);border-radius:8px;background:var(--bg-card);color:var(--text-sub);font-size:0.82rem;font-weight:700;cursor:pointer;`;
                moreBtn.addEventListener('click', () => {
                    visibleCount = visibleCount < allNews.length ? Math.min(visibleCount + moreCount, allNews.length) : firstCount;
                    draw();
                });
                root.appendChild(moreBtn);
            }
        };
        draw();
    }

    async function load(context) {
        const ctx = context || {};
        const root = document.getElementById('stockNewsList');
        if (!hasRenderedOnce && root && !root.innerHTML.trim()) {
            renderKeyNews([]);
            render([]);
        }
        if (root) { root.setAttribute('aria-busy', 'true'); root.style.opacity = '0.55'; }
        const symbol = String(ctx.symbol || '').trim();
        const market = String(ctx.market || '').trim();
        const name = String(ctx.name || '').trim();
        if (!symbol && !name) return;
        const token = ++requestToken;
        if (controller) { try { controller.abort(); } catch (_) {} }
        controller = new AbortController();
        const qs = new URLSearchParams({symbol, market, name, limit: '10'});
        try {
            const response = await fetch('/api/stock-news?' + qs.toString(), {cache: 'no-store', signal: controller.signal});
            const payload = await response.json().catch(() => null);
            if (token !== requestToken) return;
            if (!response.ok || !payload || payload.ok === false) throw new Error((payload && payload.error) || '뉴스 데이터 오류');
            renderKeyNews(payload.key_news || []);
            render(payload.data || []);
            hasRenderedOnce = true;
            if (root) { root.removeAttribute('aria-busy'); root.style.opacity = '1'; }
        } catch (error) {
            if (error && error.name === 'AbortError') return;
            if (!hasRenderedOnce || !root || !root.innerHTML.trim()) render([]);
            hasRenderedOnce = hasRenderedOnce || !!(root && root.innerHTML.trim());
            if (root) { root.removeAttribute('aria-busy'); root.style.opacity = '1'; }
            console.warn('[STOCK NEWS] load failed:', error);
        }
    }

    window.GaemiWidgets.stockNews = { render, load };
})(window);
