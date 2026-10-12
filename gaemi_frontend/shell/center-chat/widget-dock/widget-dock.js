/* Central Widget Dock — presentation shell for data-backed analysis widgets only. */
(function () {
  'use strict';

  const STATE_KEY = 'gaemiGTP_central_widget_dock_v1';
  let activeIds = [];

  function catalog() { return window.GaemiGTPWidgetCatalog || {}; }
  function escapeHtml(value) {
    return String(value || '')
      .replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;')
      .replaceAll('"', '&quot;').replaceAll("'", '&#039;');
  }
  function root() { return document.getElementById('centralWidgetDock'); }
  function body() { return document.getElementById('centralWidgetDockBody'); }

  function readState() {
    try {
      const saved = localStorage.getItem(STATE_KEY);
      if (saved === null) return ['kospi'];
      const value = JSON.parse(saved);
      if (!Array.isArray(value)) return ['kospi'];
      // 이전 버전의 코스피 위젯 ID를 새 명확한 ID로 한 번만 이관한다.
      return value
        .map(id => id === 'why-up' ? 'kospi' : id)
        .filter((id, index, list) => catalog()[id] && list.indexOf(id) === index);
    } catch (_) { return []; }
  }
  function saveState() {
    try { localStorage.setItem(STATE_KEY, JSON.stringify(activeIds)); } catch (_) {}
  }
  // Preserve the original compact KOSPI card markup. Fill independent cards with existing world-board data.
  const MARKETS = Object.freeze({
    kospi: ['코스피','코스피'], kosdaq: ['코스닥','코스닥'],
    qqq: ['QQQ','QQQ'], spy: ['SPY','SPY'], soxx: ['SOXX','SOXX'],
    dia: ['DIA','DIA'], vix: ['VIX','VIX'], forex: ['원/달러','USD/KRW'],
    oil: ['WTI 유가','WTI 유가'], gold: ['금','금']
  });
  function renderMarketCard(id) {
    const title = MARKETS[id][0];
    return `<article class="central-widget-card central-widget-card--kospi" data-central-widget-id="${escapeHtml(id)}">
      <button type="button" class="central-widget-card__close central-widget-card__close--overlay" data-central-widget-remove="${escapeHtml(id)}" aria-label="${escapeHtml(title)} 위젯 삭제" title="위젯 삭제"><i data-lucide="x" class="w-3 h-3"></i></button>
      <div class="central-widget-card__kospi-title">${escapeHtml(title)}</div>
      <div class="central-widget-card__kospi-body" aria-label="${escapeHtml(title)} 시세">
        <svg class="central-widget-card__sparkline" viewBox="0 0 72 28" preserveAspectRatio="none" aria-hidden="true">
          <defs><linearGradient id="market-sparkline-gradient-${escapeHtml(id)}" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stop-color="currentColor" stop-opacity="0.28"></stop>
            <stop offset="100%" stop-color="currentColor" stop-opacity="0"></stop>
          </linearGradient></defs>
          <polygon data-market-sparkline-fill points="" style="fill:url(#market-sparkline-gradient-${escapeHtml(id)});stroke:none"></polygon>
          <polyline data-market-sparkline points="" fill="none"></polyline>
        </svg>
        <div class="central-widget-card__kospi-copy">
          <div class="central-widget-card__kospi-row central-widget-card__kospi-market"><strong data-market-price>—</strong><span data-market-change class="central-widget-card__kospi-change">—</span></div>
          <div class="central-widget-card__kospi-row central-widget-card__kospi-reason"><span data-market-status>조회 중</span></div>
        </div>
      </div>
    </article>`;
  }
  function updateMarketCard(id, item) {
    const card = Array.from(body()?.querySelectorAll('[data-central-widget-id]')||[]).find(v=>v.dataset.centralWidgetId===id);
    if (!card) return;
    const status=card.querySelector('[data-market-status]');
    if (!item || item.price==='-' || !item.price) {if(status)status.textContent='시세 없음';return;}
    card.querySelector('[data-market-price]').textContent=String(item.price);
    const rate=card.querySelector('[data-market-change]');
    rate.textContent=String(item.rate||'—');
    const parsedRate=Number(String(item.rate||'').replace('%',''));
    const up=Number.isFinite(parsedRate)?parsedRate>=0:!!item.up;
    rate.classList.toggle('central-widget-card__kospi-change--up',up);
    rate.classList.toggle('central-widget-card__kospi-change--down',!up);
    const svg=card.querySelector('.central-widget-card__sparkline');
    svg.classList.toggle('central-widget-card__sparkline--up',up);
    svg.classList.toggle('central-widget-card__sparkline--down',!up);
    const values=(Array.isArray(item.sparkline)?item.sparkline:[]).map(Number).filter(Number.isFinite);
    const line=card.querySelector('[data-market-sparkline]');
    const fill=card.querySelector('[data-market-sparkline-fill]');
    if (values.length>1) {
      const min=Math.min(...values), spread=Math.max(0.0001,Math.max(...values)-min);
      const pts=values.map((v,i)=>(1+i*70/(values.length-1)).toFixed(1)+','+(25-(v-min)/spread*22).toFixed(1)).join(' ');
      line.setAttribute('points',pts);
      fill.setAttribute('points','1,27 '+pts+' 71,27');
      fill.style.fill='url(#market-sparkline-gradient-'+id+')';
    } else {line.setAttribute('points','');fill.setAttribute('points','');}
    if(status)status.textContent=String(item.market_status || item.traded_at || '최근 시세');
  }
  function fillMarketWidgets() {
    const market=window.GaemiGTPMarketData;
    if(!market)return;
    const snapshot=market.snapshot();
    for(const [id,config] of Object.entries(MARKETS)){
      if(!activeIds.includes(id))continue;
      const item=market.find(config[1]);
      if(item)updateMarketCard(id,item);
      else{
        const card=Array.from(body()?.querySelectorAll('[data-central-widget-id]')||[]).find(v=>v.dataset.centralWidgetId===id);
        const status=card?.querySelector('[data-market-status]');
        if(status)status.textContent=snapshot.error?'연결 확인 필요':'조회 중';
      }
    }
  }
  function render() {
    const dock = root();
    const bodyEl = body();
    if (!dock || !bodyEl) return;
    bodyEl.innerHTML = activeIds.map(id => {
      const meta = catalog()[id];
      if (MARKETS[id]) return renderMarketCard(id);
      if (id === 'night-futures') {
        const title = '코스피200 야간선물';
        return `<article class="central-widget-card central-widget-card--kospi" data-central-widget-id="night-futures">
          <button type="button" class="central-widget-card__close central-widget-card__close--overlay" data-central-widget-remove="night-futures" aria-label="위젯 삭제"><i data-lucide="x" class="w-3 h-3"></i></button>
          <div class="central-widget-card__kospi-title">${title}</div>
          <div class="central-widget-card__market-empty">야간선물 데이터 미연결</div>
        </article>`;
      }
      return `<article class="central-widget-card" data-central-widget-id="${escapeHtml(id)}">
        <div class="central-widget-card__top">
          <div class="central-widget-card__name">${escapeHtml(meta.title)}</div>
          <button type="button" class="central-widget-card__close" data-central-widget-remove="${escapeHtml(id)}" aria-label="${escapeHtml(meta.title)} 위젯 삭제" title="위젯 삭제"><i data-lucide="x" class="w-3 h-3"></i></button>
        </div>
        <div class="central-widget-card__desc" data-central-widget-mount="${escapeHtml(id)}">${escapeHtml(meta.description)} · 데이터 준비 중</div>
      </article>`;
    }).join('');
    dock.dataset.count = String(activeIds.length);
    if (activeIds.some(id => MARKETS[id])) { fillMarketWidgets(); void window.GaemiGTPMarketData?.refresh?.(); }
    document.querySelectorAll('[data-central-widget-mount]').forEach(node => {
      window.GaemiGTPAnalysisWidgets?.mount?.(node.dataset.centralWidgetMount, node);
    });
    if (window.lucide) window.lucide.createIcons();
  }
  function add(id) {
    if (!catalog()[id] || activeIds.includes(id)) return false;
    activeIds.push(id); saveState(); render(); return true;
  }
  function remove(id) {
    const next = activeIds.filter(item => item !== id);
    const changed = next.length !== activeIds.length;
    activeIds = next;
    if (changed) { saveState(); render(); window.GaemiGTPPluginSidebar?.render?.(); }
    return changed;
  }
  function initialize() {
    const dock = root();
    if (!dock || dock.dataset.bound === 'true') return;
    dock.dataset.bound = 'true';
    activeIds = readState(); render();
    dock.addEventListener('click', event => {
      const button = event.target.closest('[data-central-widget-remove]');
      if (button) remove(button.dataset.centralWidgetRemove);
    });
  }
  window.addEventListener('gaemi-market-data-change',fillMarketWidgets);
  document.addEventListener('DOMContentLoaded', initialize);
  window.GaemiGTPWidgetDock = { initialize, add, remove, getActiveIds: () => [...activeIds], isActive: id => activeIds.includes(id) };
}());
