(function () {
  'use strict';
  const root = window.GaemiWidgets = window.GaemiWidgets || {};

  function money(v, market) {
    const n = Number(v);
    if (!Number.isFinite(n)) return '-';
    return market === '미국주식'
      ? '$' + n.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2})
      : Math.round(n).toLocaleString('ko-KR') + '원';
  }

  function render(data) {
    const body = document.getElementById('maliciousZoneBody');
    if (!body) return;
    const zones = Array.isArray(data && data.malicious_zones) ? data.malicious_zones : [];
    const price = Number(data && data.current_price);
    const market = String(data && data.market || '');
    if (!zones.length) {
      body.innerHTML = '<div style="padding:18px 4px; color:var(--text-muted); text-align:center;">현재 데이터에서 뚜렷한 매물 집중 구간을 찾지 못했습니다.</div>';
      return;
    }
    const nearest = zones.slice().sort((a,b) => Number(a.distance_pct)-Number(b.distance_pct))[0];
    body.innerHTML = `
      <div style="padding:10px 12px; background:var(--bg-subcard); border-radius:10px; margin-bottom:10px;">
        <div style="font-size:.82rem;color:var(--text-muted);">현재가</div>
        <div style="font-size:1.08rem;font-weight:850;color:var(--text-title);margin-top:2px;">${money(price, market)}</div>
        <div style="font-size:.76rem;color:var(--text-muted);margin-top:5px;">과거 거래량이 집중된 가격대를 기준으로 계산 · 매수/매도 신호 아님</div>
      </div>
      ${zones.map((z, i) => `
        <div style="padding:10px 4px; border-bottom:1px solid var(--border-line);">
          <div style="display:flex;justify-content:space-between;align-items:center;gap:8px;">
            <div style="font-weight:800;color:var(--text-title);">${i === 0 ? '가장 가까운 매물대' : '매물 집중 구간'}</div>
            <div style="font-size:.75rem;font-weight:800;color:#f59e0b;">${Number(z.distance_pct)>=0?'+':''}${Number(z.distance_pct).toFixed(1)}%</div>
          </div>
          <div style="display:flex;justify-content:space-between;align-items:center;margin-top:5px;">
            <span style="font-weight:800;color:var(--text-title);">${money(z.low, market)} ~ ${money(z.high, market)}</span>
            <span style="font-size:.72rem;color:var(--text-muted);">집중도 ${z.intensity}</span>
          </div>
          <div style="height:5px;background:var(--bg-subcard);border-radius:4px;margin-top:7px;overflow:hidden;">
            <div style="width:${Math.max(5, Math.min(100, Number(z.intensity)||0))}%;height:100%;background:#f59e0b;border-radius:4px;"></div>
          </div>
        </div>`).join('')}
      <div style="margin-top:10px;font-size:.76rem;color:var(--text-muted);line-height:1.5;">
        ${nearest ? `현재가 위쪽 <b style="color:var(--text-title);">${Number(nearest.distance_pct).toFixed(1)}%</b> 부근에 과거 거래가 집중된 구간이 있습니다. 가격이 접근하면 본전 매물 출회로 저항이 강해질 수 있는 구간입니다.` : ''}
      </div>`;
  }

  root.maliciousZone = { render };
  window.renderMaliciousZone = render;

  window.addEventListener('gaemi:data', function (event) {
    if (event && event.detail) render(event.detail);
  });
})();
