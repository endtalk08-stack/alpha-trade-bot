/* gaemiGTP — Economic Calendar independent widget */
(function () {
    'use strict';

    window.GaemiWidgets = window.GaemiWidgets || {};

    let offset = 0;
    let page = 1;
    let starFilter = 'all';
    let autoJumped = false;
    let loadToken = 0;
    let loadController = null;
    const ITEMS_PER_PAGE = 4;

    function data() {
        return Array.isArray(window.globalMarketCalendar) ? window.globalMarketCalendar : [];
    }

    function setText(id, value) {
        const el = document.getElementById(id);
        if (el) el.innerText = value !== undefined && value !== null ? String(value) : '';
    }

    function todayKey() {
        return new Intl.DateTimeFormat('en-CA', {
            timeZone: 'Asia/Seoul', year: 'numeric', month: '2-digit', day: '2-digit'
        }).format(new Date()).replace(/-/g, '.');
    }

    function dateInfo(dayOffset) {
        const [y, m, d] = todayKey().split('.').map(Number);
        const date = new Date(Date.UTC(y, m - 1, d + Number(dayOffset || 0)));
        const days = ['일요일', '월요일', '화요일', '수요일', '목요일', '금요일', '토요일'];
        return {
            dateKey: `${date.getUTCFullYear()}.${String(date.getUTCMonth() + 1).padStart(2, '0')}.${String(date.getUTCDate()).padStart(2, '0')}`,
            display: `${date.getUTCFullYear()}.${String(date.getUTCMonth() + 1).padStart(2, '0')}.${String(date.getUTCDate()).padStart(2, '0')} (${days[date.getUTCDay()].slice(0, 1)})`,
            header: `${date.getUTCMonth() + 1}월 ${date.getUTCDate()}일 ${days[date.getUTCDay()]}`
        };
    }

    function isEconomic(ev) {
        if (!ev || !ev.date_key) return false;
        const type = String(ev.type || '').toLowerCase();
        return type === 'eco' || type === 'economic';
    }

    function nearestOffset() {
        const dates = [...new Set(data()
            .filter(isEconomic)
            .map(ev => String(ev.date_key).replace(/[\\/]/g, '.')))].sort();
        if (!dates.length) return 0;

        const [y, m, d] = todayKey().split('.').map(Number);
        const today = new Date(Date.UTC(y, m - 1, d));
        if (dates.includes(todayKey())) return 0;

        for (const key of dates) {
            const [yy, mm, dd] = key.split('.').map(Number);
            if ([yy, mm, dd].some(Number.isNaN)) continue;
            const candidate = new Date(Date.UTC(yy, mm - 1, dd));
            if (candidate >= today) return Math.round((candidate - today) / 86400000);
        }

        let latest = null;
        for (const key of dates) {
            const [yy, mm, dd] = key.split('.').map(Number);
            if ([yy, mm, dd].some(Number.isNaN)) continue;
            const candidate = new Date(Date.UTC(yy, mm - 1, dd));
            if (candidate < today) latest = key;
        }
        if (!latest) return 0;
        const [yy, mm, dd] = latest.split('.').map(Number);
        return Math.round((new Date(Date.UTC(yy, mm - 1, dd)) - today) / 86400000);
    }

    function eventKey(ev) {
        if (typeof window.getCalendarEventKey === 'function') return window.getCalendarEventKey(ev);
        return [ev.type || '', ev.date_key || '', ev.time || '', ev.ticker || '', ev.name || ''].join('|');
    }

    function alarmActive(key) {
        if (typeof window.getCalendarAlarms === 'function') return !!window.getCalendarAlarms()[key];
        try { return !!JSON.parse(localStorage.getItem('calendar_alarm_events_v1') || '{}')[key]; }
        catch (_) { return false; }
    }

    // 화면 표시 전용 한글 지표명. API 원문과 알림 식별키는 유지한다.
    const ECONOMIC_NAME_KO = new Map([
        ['nopa crush report publication', { title: '미국 대두 압착 보고서' }],
        ['u.s. nopa member soybean crush', { title: '미국 대두 압착량' }],
        ['us nopa member soybean crush', { title: '미국 대두 압착량' }],
        ['u.s. advance retail and food services sales ex autos and gas mom sa', { title: '미국 소매판매', subtitle: '자동차·주유소 제외' }],
        ['u.s. advance retail and food services sales ex autos mom sa', { title: '미국 소매판매', subtitle: '자동차 제외' }],
        ['u.s. consumer price index (cpi) yoy', { title: '미국 소비자물가지수(CPI)', subtitle: '전년 대비' }],
        ['u.s. consumer price index (cpi) mom', { title: '미국 소비자물가지수(CPI)', subtitle: '전월 대비' }],
        ['u.s. core consumer price index yoy', { title: '미국 근원 소비자물가지수', subtitle: '전년 대비' }],
        ['u.s. core consumer price index mom', { title: '미국 근원 소비자물가지수', subtitle: '전월 대비' }],
        ['u.s. producer price index (ppi) mom', { title: '미국 생산자물가지수(PPI)', subtitle: '전월 대비' }],
        ['u.s. producer price index (ppi) yoy', { title: '미국 생산자물가지수(PPI)', subtitle: '전년 대비' }],
        ['u.s. nonfarm payrolls', { title: '미국 비농업 고용자 수' }],
        ['u.s. unemployment rate', { title: '미국 실업률' }],
        ['u.s. initial jobless claims', { title: '미국 신규 실업수당 청구건수' }],
        ['u.s. gross domestic product (gdp) qoq', { title: '미국 국내총생산(GDP)', subtitle: '전분기 대비' }],
        ['u.s. fed interest rate decision', { title: '미국 기준금리 결정' }],
        ['michael barr public remarks', { title: '미 연준 바 이사 공개 발언' }],
        ['u.s. new york fed empire state current general business conditions sa', { title: '뉴욕 연은 엠파이어스테이트 경기지수', subtitle: '현재 일반경기 · 계절조정' }],
    ]);

    const ECONOMIC_TERM_KO = [
        [/core retail sales/gi, '근원 소매판매'],
        [/advance retail and food services sales/gi, '소매판매'],
        [/retail sales/gi, '소매판매'],
        [/initial jobless claims|initial claims/gi, '신규 실업수당 청구건수'],
        [/continuing jobless claims|continuing claims/gi, '계속 실업수당 청구건수'],
        [/nonfarm payrolls|non-farm payrolls/gi, '비농업 고용자 수'],
        [/unemployment rate/gi, '실업률'],
        [/consumer price index|\bCPI\b/gi, '소비자물가지수(CPI)'],
        [/core consumer price index|core CPI/gi, '근원 소비자물가지수(CPI)'],
        [/producer price index|\bPPI\b/gi, '생산자물가지수(PPI)'],
        [/personal consumption expenditures|\bPCE\b/gi, '개인소비지출(PCE) 물가지수'],
        [/gross domestic product|\bGDP\b/gi, '국내총생산(GDP)'],
        [/federal funds rate|fed interest rate decision/gi, '연방준비제도 기준금리 결정'],
        [/ISM manufacturing PMI/gi, 'ISM 제조업 구매관리자지수'],
        [/ISM services PMI|ISM non-manufacturing PMI/gi, 'ISM 서비스업 구매관리자지수'],
        [/consumer confidence/gi, '소비자신뢰지수'],
        [/crude oil inventories/gi, '원유재고'],
        [/housing starts/gi, '주택착공건수'],
        [/building permits/gi, '건축허가건수'],
        [/existing home sales/gi, '기존주택판매'],
        [/new home sales/gi, '신규주택판매'],
        [/trade balance/gi, '무역수지'],
        [/durable goods orders/gi, '내구재 주문'],
        [/industrial production/gi, '산업생산'],
        [/capacity utilization/gi, '설비가동률'],
        [/factory orders/gi, '공장주문'],
        [/business inventories/gi, '기업재고'],
        [/personal income/gi, '개인소득'],
        [/personal spending/gi, '개인소비지출'],
        [/average hourly earnings/gi, '평균 시간당 임금'],
        [/adp employment change/gi, 'ADP 민간고용 증감'],
        [/challenger job cuts/gi, '챌린저 감원 보고서'],
        [/consumer credit change/gi, '소비자신용 증감'],
        [/wholesale inventories/gi, '도매재고'],
        [/s&p global manufacturing pmi/gi, 'S&P 글로벌 제조업 PMI'],
        [/s&p global services pmi/gi, 'S&P 글로벌 서비스업 PMI'],
        [/chicago pmi/gi, '시카고 구매관리자지수'],
        [/nfib small business optimism/gi, 'NFIB 소기업 낙관지수'],
        [/michigan consumer sentiment/gi, '미시간대 소비자심리지수'],
        [/michigan inflation expectations/gi, '미시간대 기대인플레이션'],
        [/atlanta fed gd pnow|atlanta fed gdpnow/gi, '애틀랜타 연은 GDPNow'],
        [/prices paid/gi, '지불가격지수'],
        [/advance/gi, '잠정'],
        [/preliminary|prelim/gi, '잠정'],
        [/final/gi, '확정'],
        [/flash/gi, '속보'],
        [/weekly jobless claims/gi, '주간 실업수당 청구건수'],
        [/JOLTS job openings/gi, 'JOLTS 구인 건수'],
        [/consumer sentiment/gi, '소비자심리지수'],
        [/philly fed manufacturing index/gi, '필라델피아 연은 제조업지수'],
        [/new york fed empire state current general business conditions/gi, '뉴욕 연은 엠파이어스테이트 경기지수'],
        [/empire state manufacturing index/gi, '엠파이어스테이트 제조업지수'],
        [/new york fed/gi, '뉴욕 연은'],
        [/empire state/gi, '엠파이어스테이트'],
        [/current general business conditions/gi, '현재 일반경기'],
        [/seasonally adjusted/gi, '계절조정'],
        [/public remarks/gi, '공개 발언'],
        [/michael barr/gi, '미 연준 바 이사'],
        [/empire state manufacturing index/gi, '뉴욕 연은 제조업지수'],
        [/beige book/gi, '연준 베이지북'],
        [/fomc minutes/gi, 'FOMC 의사록'],
        [/fomc statement/gi, 'FOMC 성명서'],
        [/fomc press conference/gi, 'FOMC 기자회견'],
        [/nopa crush report publication/gi, '대두 압착 보고서 발표'],
        [/soybean crush/gi, '대두 압착량'],
    ];

    function economicNameDisplay(raw) {
        const original = String(raw || '경제지표').trim();
        const normalized = original.toLowerCase().replace(/\s+/g, ' ').trim();
        const exact = ECONOMIC_NAME_KO.get(normalized);
        if (exact) return exact;

        let translated = original
            .replace(/^(?:U\.S\.|US|United States)\s+/i, '')
            .replace(/\bEx Autos and Gas\b/gi, '자동차·주유소 제외')
            .replace(/\bEx Autos\b/gi, '자동차 제외')
            .replace(/\bMoM\b/gi, '전월 대비')
            .replace(/\bYoY\b/gi, '전년 대비')
            .replace(/\bQoQ\b/gi, '전분기 대비')
            .replace(/\bSA\b/g, '계절조정');

        for (const [pattern, korean] of ECONOMIC_TERM_KO) {
            translated = translated.replace(pattern, korean);
        }
        translated = translated.replace(/\s+/g, ' ').replace(/\s*([(),])/g, '$1').trim();
        if (!/[가-힣]/.test(translated)) translated = '미국 ' + translated;
        return { title: translated };
    }

    function escapeEconomicText(value) {
        return String(value).replace(/[&<>"']/g, ch => ({
            '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
        }[ch]));
    }

    function renderEvent(ev) {
        const key = eventKey(ev);
        const active = alarmActive(key);
        const stars = ev.star_str || (ev.stars === 3 ? '★★★' : ev.stars === 2 ? '★★☆' : '★☆☆');
        const time = ev.time || '--:--';
        const status = (ev.val === '발표완료' || ev.actual || ev.status === '발표완료') ? '발표완료' : '발표대기';
        const meta = [];
        if (ev.exp && ev.exp !== '-') {
            const exp = String(ev.exp).trim();
            meta.push(exp.startsWith('예상') ? exp : `예상 ${exp}`);
        }
        if (ev.prev && ev.prev !== '-') meta.push(`이전 ${ev.prev}`);
        const displayName = economicNameDisplay(ev.name);
        const name = escapeEconomicText(displayName.title);
        const subtitle = displayName.subtitle ? escapeEconomicText(displayName.subtitle) : '';
        return `
            <div class="cal-card-item cal-eco-item">
                <div class="cal-time-col">${time}</div>
                <div class="cal-detail-col">
                    <div class="cal-event-topline">
                        <span class="cal-stars">${stars}</span>
                        <span class="cal-event-status">${status}</span>
                    </div>
                    <div class="cal-event-title">${name}</div>
                    ${subtitle ? '<div class="cal-event-subtitle">' + subtitle + '</div>' : ''}
                    ${meta.length ? `<div class="cal-event-meta">${meta.join('  ·  ')}</div>` : ''}
                </div>
                <button class="cal-alarm-btn ${active ? 'active' : ''}"
                        onclick="event.stopPropagation(); toggleCalendarAlarmByKey('${encodeURIComponent(key)}')"
                        title="${active ? '알람 해제' : '10분 전 알람 설정'}"
                        aria-label="${active ? '알람 해제' : '10분 전 알람 설정'}">
                    <svg class="cal-alarm-icon" viewBox="0 0 24 24" aria-hidden="true">
                        <path d="M18 9a6 6 0 0 0-12 0c0 7-3 7-3 9h18c0-2-3-2-3-9"></path>
                        <path d="M10 21h4"></path>
                    </svg>
                </button>
            </div>`;
    }

    function render() {
        if (!autoJumped && offset === 0 && data().some(isEconomic)) {
            const next = nearestOffset();
            if (next !== 0) offset = next;
            autoJumped = true;
        }

        const info = dateInfo(offset);
        setText('ecoCalDisplayDate', info.display);
        const container = document.getElementById('ecoCalEventList');
        if (!container) return;

        const filtered = data().filter(ev => isEconomic(ev)
            && String(ev.date_key).replace(/[\\/]/g, '.') === info.dateKey
            && (starFilter === 'all' || String(ev.stars) === starFilter));

        setText('ecoCalDayHeader', filtered.length && filtered[0].date_header
            ? filtered[0].date_header : `${info.header} · 주요 경제지표`);

        const totalPages = Math.ceil(filtered.length / ITEMS_PER_PAGE) || 1;
        page = Math.max(1, Math.min(page, totalPages));
        const prev = document.getElementById('btnEcoCalPrev');
        const next = document.getElementById('btnEcoCalNext');
        if (prev) prev.disabled = page <= 1;
        if (next) next.disabled = page >= totalPages;
        setText('ecoCalPageIndicator', `${page} / ${totalPages}`);

        if (!filtered.length) {
            container.innerHTML = '<div style="display:flex;align-items:center;justify-content:center;height:100%;color:var(--text-muted);font-size:0.76rem;text-align:center;padding:18px 8px;">이 날짜에 예정된 경제 일정이 없습니다.</div>';
        } else {
            const start = (page - 1) * ITEMS_PER_PAGE;
            container.innerHTML = filtered.slice(start, start + ITEMS_PER_PAGE).map(renderEvent).join('');
        }
        if (typeof window.scheduleCalendarHeightSync === 'function') window.scheduleCalendarHeightSync();
    }

    function shiftDate(delta) { offset += Number(delta || 0); page = 1; render(); }
    function resetDate() { offset = 0; page = 1; autoJumped = true; render(); }
    function setStarFilter(value, btn) {
        starFilter = String(value || 'all');
        document.querySelectorAll('.calendar-star-btn[data-cal-type="eco"]').forEach(el => el.classList.remove('active'));
        if (btn) btn.classList.add('active');
        page = 1;
        render();
    }
    function changePage(delta) { page += Number(delta || 0); render(); }
    function resetState() { offset = 0; page = 1; autoJumped = false; render(); }

    async function load(force = false) {
        const token = ++loadToken;
        if (loadController) { try { loadController.abort(); } catch (_) {} }
        loadController = new AbortController();
        try {
            const url = '/api/market-calendar' + (force ? '?force=1' : '');
            const response = await fetch(url, { cache: 'no-store', signal: loadController.signal });
            const payload = await response.json().catch(() => null);
            if (token !== loadToken) return;
            if (!response.ok || !payload || payload.ok === false) throw new Error((payload && payload.error) || '시장 캘린더 데이터 오류');
            const hadEconomicSnapshot = data().some(isEconomic);
            const previousOffset = offset;
            window.globalMarketCalendar = Array.isArray(payload.data) ? payload.data : [];
            if (hadEconomicSnapshot) {
                // 데이터가 갱신되어도 사용자가 보고 있던 날짜를 임의로 바꾸지 않는다.
                offset = previousOffset;
                autoJumped = true;
            } else {
                offset = 0;
                page = 1;
                autoJumped = false;
            }
            render();
        } catch (error) {
            if (error && error.name === 'AbortError') return;
            console.warn('[ECONOMIC CALENDAR] load failed:', error);
            render();
        }
    }

    window.GaemiWidgets.economicCalendar = { render, load, shiftDate, resetDate, setStarFilter, changePage, resetState };

    // 기존 inline onclick/호출부와의 호환 브리지
    window.renderEconomicCalendarView = render;
    window.shiftEconomicCalendarDate = shiftDate;
    window.resetEconomicCalendarDate = resetDate;
    window.setCalendarStarFilter = function (type, value, btn) {
        if (type === 'eco') return setStarFilter(value, btn);
        if (typeof window.setEarningsCalendarStarFilter === 'function') return window.setEarningsCalendarStarFilter(value, btn);
    };
    window.changeEconomicCalendarPage = changePage;
})();
