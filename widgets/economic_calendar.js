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

    // 화면 표시 전용 한글 지표명 매핑. API 원문과 알림 식별키는 변경하지 않는다.
    const ECONOMIC_NAME_KO = new Map([
        ['nopa crush report publication', '미국 대두 압착 보고서'],
        ['u.s. nopa member soybean crush', '미국 대두 압착량'],
        ['us nopa member soybean crush', '미국 대두 압착량'],
        ['u.s. advance retail and food services sales ex autos and gas mom sa', '미국 소매판매 (자동차·주유소 제외, 전월 대비)'],
        ['u.s. advance retail and food services sales ex autos mom sa', '미국 소매판매 (자동차 제외, 전월 대비)'],
        ['u.s. consumer price index (cpi) yoy', '미국 소비자물가지수(CPI, 전년 대비)'],
        ['u.s. consumer price index (cpi) mom', '미국 소비자물가지수(CPI, 전월 대비)'],
        ['u.s. core consumer price index yoy', '미국 근원 소비자물가지수(전년 대비)'],
        ['u.s. core consumer price index mom', '미국 근원 소비자물가지수(전월 대비)'],
        ['u.s. producer price index (ppi) mom', '미국 생산자물가지수(PPI, 전월 대비)'],
        ['u.s. producer price index (ppi) yoy', '미국 생산자물가지수(PPI, 전년 대비)'],
        ['u.s. nonfarm payrolls', '미국 비농업 고용자 수'],
        ['u.s. unemployment rate', '미국 실업률'],
        ['u.s. initial jobless claims', '미국 신규 실업수당 청구건수'],
        ['u.s. gross domestic product (gdp) qoq', '미국 국내총생산(GDP, 전분기 대비)'],
        ['u.s. fed interest rate decision', '미국 연방준비제도 기준금리 결정'],
    ]);

    function economicNameKo(raw) {
        const original = String(raw || '경제지표').trim();
        const normalized = original.toLowerCase().replace(/\s+/g, ' ').trim();
        if (ECONOMIC_NAME_KO.has(normalized)) return ECONOMIC_NAME_KO.get(normalized);
        // 공급처가 바뀌어도 원문에 공통 약어가 포함된 경우만 안전하게 변환한다.
        const short = normalized.replace(/^(u\.s\.|us|united states)\s+/, '');
        if (ECONOMIC_NAME_KO.has(short)) return ECONOMIC_NAME_KO.get(short);
        let name = original
            .replace(/^(?:U\.S\.|US|United States)\s+/i, '미국 ')
            .replace(/\bEx Autos and Gas\b/gi, '(자동차·주유소 제외)')
            .replace(/\bEx Autos\b/gi, '(자동차 제외)')
            .replace(/\bMoM\b/gi, '전월 대비')
            .replace(/\bYoY\b/gi, '전년 대비')
            .replace(/\bQoQ\b/gi, '전분기 대비')
            .replace(/\bSA\b/g, '계절조정');
        // 매핑되지 않은 지표는 잘못 번역하지 않고 원문을 남긴다.
        return name;
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
        const name = escapeEconomicText(economicNameKo(ev.name));
        return `
            <div class="cal-card-item cal-eco-item">
                <div class="cal-time-col">${time}</div>
                <div class="cal-detail-col">
                    <div class="cal-event-topline">
                        <span class="cal-stars">${stars}</span>
                        <span class="cal-event-status">${status}</span>
                    </div>
                    <div class="cal-event-title">${name}</div>
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
            window.globalMarketCalendar = Array.isArray(payload.data) ? payload.data : [];
            offset = 0; page = 1; autoJumped = false;
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
