/* gaemiGTP Widget: 경제뉴스
 * 데이터 수집은 독립 API(/api/global-news), 렌더링만 이 파일이 담당한다.
 */
(function (window) {
    'use strict';

    window.GaemiWidgets = window.GaemiWidgets || {};
    let requestToken = 0;
    let controller = null;
    const marketCache = new Map();
    const MARKET_CACHE_TTL_MS = 5 * 60 * 1000;

    function render(items) {
        const root = document.getElementById('globalNewsList');
        if (!root) return;
        if (!Array.isArray(items) || items.length === 0) {
            root.innerHTML = '<div style="color:var(--text-muted); font-size:0.85rem;">경제뉴스가 없습니다.</div>';
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
        const market = String(ctx.marketCode || ctx.market || '국내주식').trim() || '국내주식';
        const cached = marketCache.get(market);
        const now = Date.now();

        // 경제뉴스는 종목별 데이터가 아니라 시장 단위 데이터다.
        // 같은 시장의 종목을 연속해서 선택할 때는 재요청하지 않는다.
        if (cached && Array.isArray(cached.data) && (now - cached.time) < MARKET_CACHE_TTL_MS) {
            render(cached.data);
            return cached.data;
        }

        if (Array.isArray(ctx.initial) && ctx.initial.length) {
            marketCache.set(market, {time: now, data: ctx.initial.slice(0, 10)});
            render(ctx.initial);
        }

        const token = ++requestToken;
        if (controller) { try { controller.abort(); } catch (_) {} }
        controller = new AbortController();
        const qs = new URLSearchParams({market, limit: '10'});
        try {
            const response = await fetch('/api/global-news?' + qs.toString(), {
                cache: 'no-store', signal: controller.signal
            });
            const payload = await response.json().catch(() => null);
            if (token !== requestToken) return null;
            if (!response.ok || !payload || payload.ok === false) {
                throw new Error((payload && payload.error) || '경제뉴스 데이터 오류');
            }
            const items = Array.isArray(payload.data) ? payload.data : [];
            marketCache.set(market, {time: Date.now(), data: items.slice(0, 10)});
            render(items);
            return items;
        } catch (error) {
            if (error && error.name === 'AbortError') return null;
            if (!cached && (!Array.isArray(ctx.initial) || !ctx.initial.length)) render([]);
            console.warn('[GLOBAL NEWS] load failed:', error);
            return null;
        }
    }

    function clearCache(market) {
        if (market) marketCache.delete(String(market));
        else marketCache.clear();
    }

    window.GaemiWidgets.globalNews = { render, load, clearCache };
})(window);
