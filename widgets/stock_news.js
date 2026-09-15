/* gaemiGTP Widget: 종목 뉴스
 * 역할: 종목 뉴스 UI 렌더링만 담당.
 * 데이터 수집/API/분석은 bot.py가 담당한다.
 */
(function (window) {
    'use strict';

    window.GaemiWidgets = window.GaemiWidgets || {};

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
                </a>
            `).join('');

            if (visibleCount < allNews.length || visibleCount > firstCount) {
                const moreBtn = document.createElement('button');
                moreBtn.type = 'button';
                moreBtn.textContent = visibleCount < allNews.length
                    ? `더보기 (${Math.min(moreCount, allNews.length - visibleCount)}개)`
                    : '접기';
                moreBtn.style.cssText = `
                    width:100%; margin-top:10px; padding:9px 0;
                    border:1px solid var(--border-line); border-radius:8px;
                    background:var(--bg-card); color:var(--text-sub);
                    font-size:0.82rem; font-weight:700; cursor:pointer;
                `;
                moreBtn.addEventListener('click', () => {
                    visibleCount = visibleCount < allNews.length
                        ? Math.min(visibleCount + moreCount, allNews.length)
                        : firstCount;
                    draw();
                });
                root.appendChild(moreBtn);
            }
        };

        draw();
    }

    window.GaemiWidgets.stockNews = { render };
})(window);
