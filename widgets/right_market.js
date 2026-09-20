(function(){
    const GaemiWidgets = window.GaemiWidgets = window.GaemiWidgets || {};
    const rightMarket = {};
    let rightMarketController = null;
    let rightMarketRequestToken = 0;
// ===== STEP 21: 우측 실시간 시장 사이드바 =====
        let rightMarketKey = 'kr';
        let rightMarketMenu = 'live';
        const rightMarketCache = {};

        function escapeRightMarketText(value) {
            return String(value ?? '').replace(/[&<>"']/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
        }

        function formatRightMarketPrice(row) {
            const p = Number(row.price);
            if (!Number.isFinite(p)) return '-';
            if (row.currency === 'KRW') return `${Math.round(p).toLocaleString('ko-KR')}원`;
            return `$${p.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2})}`;
        }

        function renderRightMarketRows(rows) {
            const list = document.getElementById('rightMarketList');
            if (!list) return;
            if (!Array.isArray(rows) || !rows.length) {
                list.innerHTML = '<div class="right-market-empty">현재 실시간 데이터를 가져오지 못했습니다.<br>잠시 후 다시 시도합니다.</div>';
                return;
            }
            list.innerHTML = rows.map((row, idx) => {
                const pct = Number(row.change_pct);
                const change = Number(row.change);
                const cls = Number.isFinite(pct) && pct >= 0 ? 'up' : 'down';
                const pctText = Number.isFinite(pct) ? `${pct >= 0 ? '+' : ''}${pct.toFixed(2)}%` : '-';
                let changeText = '';
                if (Number.isFinite(change)) {
                    changeText = row.currency === 'KRW'
                        ? `${change >= 0 ? '+' : ''}${Math.round(change).toLocaleString('ko-KR')}원`
                        : `${change >= 0 ? '+' : ''}${change.toFixed(2)}`;
                }
                const krName = (rightMarketKey === 'us' && typeof getUsShortKrName === 'function')
                    ? getUsShortKrName(row.symbol, row.name)
                    : (row.name || row.symbol || '-');
                const badge = escapeRightMarketText((krName || row.symbol || '?').slice(0, 2));
                const name = escapeRightMarketText(krName || row.symbol || '-');
                const symbol = escapeRightMarketText(row.symbol || '');
                const clickSymbol = escapeRightMarketText(row.symbol || row.name || '');
                const clickName = escapeRightMarketText(krName || row.name || row.symbol || '');
                const rowMarket = String(row.market_code || row.market_code_name || '').trim().toUpperCase();
                const marketCode = rowMarket || (rightMarketKey === 'kr' ? 'KR' : (rightMarketKey === 'us' ? 'US' : 'CRYPTO'));
                const clickQuery = escapeRightMarketText(`${marketCode}:${row.symbol || row.name || ''}`);
                return `<div class="right-market-row" role="button" tabindex="0" data-right-symbol="${clickSymbol}" data-right-query="${clickQuery}" data-right-name="${clickName}" data-right-market="${escapeRightMarketText(marketCode)}">
                    <div class="right-market-rank">${row.rank || idx + 1}</div>
                    <div class="right-market-logo">${badge}</div>
                    <div style="min-width:0"><div class="right-market-name">${name}</div><div class="right-market-symbol">${symbol}</div></div>
                    <div class="right-market-values"><div class="right-market-price">${formatRightMarketPrice(row)}</div><div class="right-market-change ${cls}">${changeText} ${pctText}</div></div>
                </div>`;
            }).join('');
        }

        async function loadRightMarketBoard(force=false) {
            const list = document.getElementById('rightMarketList');
            if (!list) return;
            const cacheKey = rightMarketKey;
            const requestToken = ++rightMarketRequestToken;
            if (rightMarketController) {
                try { rightMarketController.abort(); } catch (e) {}
            }
            rightMarketController = new AbortController();
            const boardSignal = rightMarketController.signal;
            if (!force && rightMarketCache[cacheKey]) {
                renderRightMarketRows(rightMarketCache[cacheKey]);
                return;
            }
            try {
                const r = await fetch(`/api/right-market-board?market=${encodeURIComponent(cacheKey)}${force ? '&force=1' : ''}`, {cache:'no-store', signal: boardSignal});
                const payload = await r.json();
                if (requestToken !== rightMarketRequestToken || cacheKey !== rightMarketKey) return;
                if (payload && payload.ok && Array.isArray(payload.data)) {
                    rightMarketCache[cacheKey] = payload.data;
                    renderRightMarketRows(payload.data);
                } else {
                    renderRightMarketRows([]);
                }
            } catch (e) {
                if (e && e.name === 'AbortError') return;
                if (requestToken === rightMarketRequestToken && cacheKey === rightMarketKey) {
                    renderRightMarketRows([]);
                }
            }
            const t = document.getElementById('rightMarketTime');
            if (t) t.textContent = `오늘 ${new Date().toLocaleTimeString('ko-KR', {hour:'2-digit', minute:'2-digit'})} 기준`;
        }

        function switchRightMarket(key) {
            rightMarketKey = key;
            document.querySelectorAll('.right-market-tab').forEach(btn => btn.classList.toggle('active', btn.dataset.market === key));
            loadRightMarketBoard(false);
        }

        // 오른쪽 종목 행은 inline onclick 대신 이벤트 위임을 사용한다.
        // HTML/JavaScript escape가 섞여 발생할 수 있는 클릭 오류를 제거하고
        // 동적으로 다시 그려진 행에도 동일하게 동작한다.
        function initRightMarketRowEvents() {
            const list = document.getElementById('rightMarketList');
            if (!list || list.dataset.eventsBound === '1') return;
            list.dataset.eventsBound = '1';
            const openRow = (row) => {
                if (!row) return;
                const symbol = row.getAttribute('data-right-symbol') || '';
                const query = row.getAttribute('data-right-query') || '';
                const name = row.getAttribute('data-right-name') || '';
                if (!symbol && !query) return;
                const market = row.getAttribute('data-right-market') || '';
                selectStock({ key: query || symbol, symbol, market, name });
            };
            list.addEventListener('click', (event) => {
                const row = event.target.closest('.right-market-row');
                if (row && list.contains(row)) openRow(row);
            });
            list.addEventListener('keydown', (event) => {
                if (event.key !== 'Enter' && event.key !== ' ') return;
                const row = event.target.closest('.right-market-row');
                if (!row || !list.contains(row)) return;
                event.preventDefault();
                openRow(row);
            });
        }

        function selectRightMenu(menu) {
            rightMarketMenu = menu;
            document.querySelectorAll('[data-right-menu]').forEach(btn => btn.classList.toggle('active', btn.dataset.rightMenu === menu));
            const list = document.getElementById('rightMarketList');
            if (menu === 'live') {
                loadRightMarketBoard(false);
                return;
            }
            if (menu === 'recent') {
                const recent = JSON.parse(localStorage.getItem('gaemiGTP_recent_searches') || '[]');
                // STEP 40: 과거 버전에서 코드만 저장된 이력은 현재 국내 실시간
                // 목록에 같은 종목이 있으면 그 한글명을 복원한다.
                const krRows = Array.isArray(rightMarketCache.kr) ? rightMarketCache.kr : [];
                const krNameMap = {};
                krRows.forEach(row => {
                    const sym = String(row?.symbol || '').toUpperCase();
                    const nm = String(row?.name || '').trim();
                    if (sym && nm && sym !== nm) krNameMap[sym] = nm;
                });
                const normalizedRecent = recent.slice(0, 15).map((x,i) => {
                    const symbol = String(x?.symbol || '').toUpperCase();
                    const name = String(x?.name || '').trim();
                    const resolvedName = (name && name.toUpperCase() !== symbol)
                        ? name
                        : (krNameMap[symbol] || name || symbol);
                    return {...x, name: resolvedName, symbol, rank:i+1};
                });
                renderRightMarketRows(normalizedRecent);
                if (!recent.length && list) list.innerHTML = '<div class="right-market-empty">최근 검색 종목이 없습니다.</div>';
                return;
            }
            if (menu === 'favorite') {
                const fav = JSON.parse(localStorage.getItem('gaemiGTP_favorites') || '[]');
                renderRightMarketRows(fav.slice(0, 15).map((x,i) => ({...x, rank:i+1})));
                if (!fav.length && list) list.innerHTML = '<div class="right-market-empty">관심 종목이 없습니다.</div>';
                return;
            }
            if (menu === 'leaders') {
                loadRightMarketBoard(false).then(() => {
                    const rows = rightMarketCache[rightMarketKey] || [];
                    renderRightMarketRows([...rows].sort((a,b) => (Number(b.change_pct)||0) - (Number(a.change_pct)||0)));
                });
            }
        }

        function setRightMarketOpen(isOpen) {
            const rail = document.getElementById('right-market-rail');
            if (!rail) return;
            // STEP 24: 모바일에서도 닫힌 상태는 아이콘 레일만 남기고,
            // 열린 상태에서만 데이터 패널을 화면 위에 겹쳐 표시한다.
            rail.classList.toggle('collapsed', !isOpen);
            // STEP 23: 오른쪽 사이드바 상태가 가운데 콘텐츠 레이아웃에 영향을 주지 않도록 한다.
            document.body.classList.remove('right-market-collapsed');
            const btn = rail.querySelector('.right-market-close');
            if (btn) {
                btn.textContent = isOpen ? '«' : '»';
                btn.title = isOpen ? '닫기' : '열기';
            }
        }

        function toggleRightMarket() {
            const rail = document.getElementById('right-market-rail');
            if (!rail) return;
            setRightMarketOpen(rail.classList.contains('collapsed'));
        }


    window.addEventListener('DOMContentLoaded', () => {
        const isMobileRightMarket = window.matchMedia && window.matchMedia('(max-width: 560px)').matches;
        setRightMarketOpen(false);
        initRightMarketRowEvents();
        if (isMobileRightMarket) return;
        loadRightMarketBoard(false);
        setInterval(() => loadRightMarketBoard(true), 60000);
    });

    rightMarket.switchRightMarket = switchRightMarket;
    rightMarket.selectRightMenu = selectRightMenu;
    rightMarket.toggleRightMarket = toggleRightMarket;
    rightMarket.setRightMarketOpen = setRightMarketOpen;
    window.switchRightMarket = switchRightMarket;
    window.selectRightMenu = selectRightMenu;
    window.toggleRightMarket = toggleRightMarket;
    GaemiWidgets.rightMarket = rightMarket;
})();
