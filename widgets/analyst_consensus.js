(function () {
    window.GaemiWidgets = window.GaemiWidgets || {};

    // 국내/미국 컨센서스는 백엔드 수집 구조가 서로 다르다.
    // 이 위젯은 c.list_title / c.items 등 백엔드가 구분해 준 실제 데이터를
    // 그대로 표시하고, 국내·미국 데이터를 하나의 계산식으로 섞지 않는다.
    const state = { expanded: false, items: [], mode: '' };

    function setTxt(id, value) {
        const el = document.getElementById(id);
        if (el) el.textContent = value == null ? '' : value;
    }

    function setHtml(id, value) {
        const el = document.getElementById(id);
        if (el) el.innerHTML = value == null ? '' : value;
    }

    function detectMode(c) {
        const title = String(c?.list_title || '');
        if (title === '실제 증권사 리포트') return 'KR';
        if (title === '실제 기관 전망') return 'CRYPTO';
        if (title === '실제 애널리스트 의견') return 'US';
        return 'UNKNOWN';
    }

    function renderItems(c, expanded) {
        const items = Array.isArray(c?.items) ? c.items : [];
        const isTwoItemConsensus =
            c?.list_title === '실제 증권사 리포트' ||
            c?.list_title === '실제 기관 전망';
        const defaultVisibleCount = isTwoItemConsensus ? 2 : 3;
        const visibleItems = expanded ? items : items.slice(0, defaultVisibleCount);
        let html = '';

        visibleItems.forEach((it, idx) => {
            const isLast = idx === visibleItems.length - 1;
            html += `
                <div style="display:flex; justify-content:space-between; align-items:center; padding:8px 0; ${isLast ? '' : 'border-bottom:1px solid var(--border-line);'} font-size:0.82rem;">
                    <div>
                        <div style="color:var(--text-title); font-weight:700;">${it.main ?? ''}</div>
                        <div style="font-size:0.7rem; color:var(--text-muted); margin-top:2px;">${it.sub ?? ''}</div>
                    </div>
                    <div style="text-align:right;">
                        <div style="color:var(--text-title); font-weight:700;">${it.val ?? ''}</div>
                        <div style="color:#10b981; font-size:0.72rem; font-weight:700;">${it.sub_val ?? ''}</div>
                    </div>
                </div>
            `;
        });

        if (isTwoItemConsensus && items.length > defaultVisibleCount) {
            html += expanded
                ? '<button type="button" class="consensus-more-btn" id="consensusMoreBtn">접기</button>'
                : `<button type="button" class="consensus-more-btn" id="consensusMoreBtn">더보기 <span>(${items.length - defaultVisibleCount}건)</span></button>`;
        }
        return html;
    }

    function bindList(c) {
        const list = document.getElementById('uItemList');
        if (!list) return;
        list.onclick = (event) => {
            const btn = event.target.closest('.consensus-more-btn');
            if (!btn) return;
            state.expanded = !state.expanded;
            list.innerHTML = renderItems(c, state.expanded);
        };
    }

    let requestToken = 0;
    let controller = null;

    async function load(context) {
        const ctx = context || {};
        const token = ++requestToken;
        if (controller) { try { controller.abort(); } catch (_) {} }
        controller = new AbortController();
        const qs = new URLSearchParams({m: String(ctx.market || ''), s: String(ctx.symbol || ''), name: String(ctx.name || ''), price: ctx.price == null ? '' : String(ctx.price)});
        try {
            const r = await fetch('/api/widget/consensus?' + qs.toString(), {cache:'no-store', signal:controller.signal});
            const payload = await r.json().catch(() => null);
            if (token !== requestToken) return;
            if (!r.ok || !payload || payload.ok === false) throw new Error(payload?.error || '컨센서스 데이터 오류');
            render(payload.data || null);
        } catch (e) {
            if (e?.name === 'AbortError') return;
            if (token !== requestToken) return;
            console.warn('[CONSENSUS] load failed:', e);
            render(null);
        }
    }

    function render(c) {
        const card = document.getElementById('rowConsensus');
        if (!card) return;
        if (!c) {
            card.style.display = 'none';
            state.items = [];
            state.expanded = false;
            state.mode = '';
            return;
        }

        state.mode = detectMode(c);
        state.items = Array.isArray(c.items) ? c.items : [];
        state.expanded = false;

        setTxt('uTitle', c.title);
        setTxt('uGaugeVal', c.gauge_val);
        setTxt('uGaugeLabel', c.gauge_label);
        setTxt('uStatusLabel', c.status_label);
        setTxt('uBadge', c.badge_text);
        setHtml('uDots', c.dots);
        setTxt('uPrimaryLabel', c.primary_label);
        setTxt('uPrimaryVal', c.primary_val);
        setTxt('uSecondaryLabel', c.secondary_label);
        setTxt('uSecondaryVal', c.secondary_val);
        setTxt('uSliderLeft', c.slider_left);
        setTxt('uSliderMid', c.slider_mid);
        setTxt('uSliderRight', c.slider_right);

        const fill = document.getElementById('uBarFill');
        if (fill) fill.style.width = `${Number(c.fill_pct) || 0}%`;
        const dot = document.getElementById('uDotMarker');
        if (dot) dot.style.left = `${Number(c.fill_pct) || 0}%`;

        setTxt('uListTitle', c.list_title);
        setTxt('uSourceTag', c.source_tag);

        const list = document.getElementById('uItemList');
        if (list) {
            list.innerHTML = state.items.length ? renderItems(c, false) : '';
            bindList(c);
        }
        card.style.display = 'block';
    }

    window.GaemiWidgets.consensus = {
        render, load,
        getMarketMode: () => state.mode
    };
})();
