/* gaemiGTP — Earnings Calendar independent widget */
(function () {
    'use strict';

    window.GaemiWidgets = window.GaemiWidgets || {};

    let offset = 0;
    let page = 1;
    let starFilter = 'all';
    let autoJumped = false;
    let earningsData = [];
    let loadToken = 0;
    let loadController = null;
    const ITEMS_PER_PAGE = 4;

    function setText(id, value) {
        const el = document.getElementById(id);
        if (el) el.innerText = value !== undefined && value !== null ? String(value) : '';
    }

    function calendarData() {
        return Array.isArray(earningsData) ? earningsData : [];
    }

    function todayKstKey() {
        const now = new Date();
        return new Intl.DateTimeFormat('en-CA', {
            timeZone: 'Asia/Seoul', year: 'numeric', month: '2-digit', day: '2-digit'
        }).format(now).replace(/-/g, '.');
    }

    function dateInfo(dayOffset) {
        const [yyyy, mm, dd] = todayKstKey().split('.').map(Number);
        const d = new Date(Date.UTC(yyyy, mm - 1, dd + Number(dayOffset || 0)));
        const days = ['일요일', '월요일', '화요일', '수요일', '목요일', '금요일', '토요일'];
        return {
            dateKey: `${d.getUTCFullYear()}.${String(d.getUTCMonth() + 1).padStart(2, '0')}.${String(d.getUTCDate()).padStart(2, '0')}`,
            display: `${d.getUTCFullYear()}.${String(d.getUTCMonth() + 1).padStart(2, '0')}.${String(d.getUTCDate()).padStart(2, '0')} (${days[d.getUTCDay()].slice(0, 1)})`,
            header: `${d.getUTCMonth() + 1}월 ${d.getUTCDate()}일 ${days[d.getUTCDay()]}`
        };
    }

    function nearestOffset() {
        const dates = [...new Set(calendarData()
            .filter(ev => ev && ev.type === 'earn' && ev.date_key)
            .map(ev => String(ev.date_key)))].sort();
        if (!dates.length) return 0;

        const [yyyy, mm, dd] = todayKstKey().split('.').map(Number);
        const today = new Date(Date.UTC(yyyy, mm - 1, dd));
        for (const key of dates) {
            const [y, m, d] = key.split('.').map(Number);
            if ([y, m, d].some(Number.isNaN)) continue;
            const candidate = new Date(Date.UTC(y, m - 1, d));
            if (candidate >= today) return Math.round((candidate - today) / 86400000);
        }
        return 0;
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

    function renderEvent(ev) {
        const key = eventKey(ev);
        const stars = ev.star_str || (ev.stars === 3 ? '★★★' : ev.stars === 2 ? '★★☆' : '★☆☆');
        const timeLabel = ev.time || '--:--';
        const status = (ev.val === '발표완료' || ev.actual || ev.status === '발표완료') ? '발표완료' : '발표대기';
        const meta = [];
        if (ev.exp && ev.exp !== '-') {
            const text = String(ev.exp).trim();
            meta.push(text.startsWith('예상') ? text : `예상 ${text}`);
        }
        const revenue = String(ev.expected_revenue_str || '').trim();
        if (revenue && revenue !== '-') meta.push(revenue.startsWith('예상') ? revenue : `예상 매출 ${revenue}`);

        const rawName = ev.name || '실적 발표';
        let name = String(rawName).replace(/\s*\([A-Za-z0-9.\-]+\)/g, '').trim();
        let timing = String(ev.timing_label || '').trim();
        if (!timing) {
            const match = name.match(/\s*(\((?:미국 )?장 (?:마감 후|시작 전)\))\s*$/);
            if (match) {
                timing = match[1];
                name = name.slice(0, match.index).trim();
            }
        }
        const clickAttr = ev.ticker
            ? `onclick="selectStock('${String(ev.ticker).replace(/'/g, "\\'")}')"`
            : '';

        return `
            <div class="cal-card-item cal-earn-item" ${clickAttr}>
                <div class="cal-time-col">${timeLabel}</div>
                <div class="cal-detail-col">
                    <div class="cal-event-topline">
                        <span class="cal-stars">${stars}</span>
                        <span class="cal-event-status">${status}</span>
                    </div>
                    <div class="cal-event-title">${name}</div>
                    ${timing ? `<div class="cal-earn-timing">${timing}</div>` : ''}
                    ${meta.length ? `<div class="cal-event-meta">${meta.join('  ·  ')}</div>` : ''}
                </div>
                <button class="cal-alarm-btn ${alarmActive(key) ? 'active' : ''}"
                        onclick="event.stopPropagation(); toggleCalendarAlarmByKey('${encodeURIComponent(key)}')"
                        title="${alarmActive(key) ? '알람 해제' : '10분 전 알람 설정'}"
                        aria-label="${alarmActive(key) ? '알람 해제' : '10분 전 알람 설정'}">
                    <svg class="cal-alarm-icon" viewBox="0 0 24 24" aria-hidden="true">
                        <path d="M18 9a6 6 0 0 0-12 0c0 7-3 7-3 9h18c0-2-3-2-3-9"></path>
                        <path d="M10 21h4"></path>
                    </svg>
                </button>
            </div>`;
    }

    function render() {
        if (!autoJumped && offset === 0) {
            const n = nearestOffset();
            if (n > 0) offset = n;
            autoJumped = true;
        }

        const info = dateInfo(offset);
        setText('earnCalDisplayDate', info.display);
        const container = document.getElementById('earnCalEventList');
        if (!container) return;

        const filtered = calendarData().filter(ev =>
            ev && ev.type === 'earn' && ev.date_key === info.dateKey &&
            (starFilter === 'all' || String(ev.stars) === starFilter)
        );
        setText('earnCalDayHeader', filtered.length && filtered[0].date_header
            ? filtered[0].date_header : `${info.header} · 주요 기업 실적`);

        const totalPages = Math.ceil(filtered.length / ITEMS_PER_PAGE) || 1;
        page = Math.max(1, Math.min(page, totalPages));
        const prev = document.getElementById('btnEarnCalPrev');
        const next = document.getElementById('btnEarnCalNext');
        if (prev) prev.disabled = page <= 1;
        if (next) next.disabled = page >= totalPages;
        setText('earnCalPageIndicator', `${page} / ${totalPages}`);

        if (!filtered.length) {
            container.innerHTML = '<div style="display:flex;align-items:center;justify-content:center;height:100%;color:var(--text-muted);font-size:0.76rem;text-align:center;padding:18px 8px;">이 날짜에 실적 일정이 없습니다.</div>';
        } else {
            const start = (page - 1) * ITEMS_PER_PAGE;
            container.innerHTML = filtered.slice(start, start + ITEMS_PER_PAGE).map(renderEvent).join('');
        }
        if (typeof window.scheduleCalendarHeightSync === 'function') window.scheduleCalendarHeightSync();
    }

    function setStarFilter(value, btn) {
        starFilter = value;
        document.querySelectorAll('#rowEarningsCalendar .calendar-star-btn').forEach(b => b.classList.remove('active'));
        if (btn) btn.classList.add('active');
        page = 1;
        render();
    }

    function shiftDate(delta) {
        offset += Number(delta || 0);
        page = 1;
        render();
    }

    function resetDate() {
        offset = nearestOffset();
        page = 1;
        autoJumped = true;
        render();
    }

    function changePage(delta) {
        page += Number(delta || 0);
        render();
    }


    async function load(force = false) {
        const token = ++loadToken;
        if (loadController) { try { loadController.abort(); } catch (_) {} }
        loadController = new AbortController();
        try {
            const url = '/api/earnings-calendar' + (force ? '?force=1' : '');
            const response = await fetch(url, { cache: 'no-store', signal: loadController.signal });
            const payload = await response.json().catch(() => null);
            if (token !== loadToken) return;
            if (!response.ok || !payload || payload.ok === false) {
                throw new Error((payload && payload.error) || '실적 캘린더 데이터 오류');
            }
            earningsData = Array.isArray(payload.data) ? payload.data : [];
            offset = 0;
            page = 1;
            autoJumped = false;
            render();
        } catch (error) {
            if (error && error.name === 'AbortError') return;
            console.warn('[EARNINGS CALENDAR] load failed:', error);
            render();
        }
    }
    window.GaemiWidgets.earningsCalendar = { render, load, setStarFilter, shiftDate, resetDate, changePage };
})();
