/* gaemiGTP Widget: Track
 * 독립 데이터 API(/api/widget/track)에서 필요한 데이터만 받아 표시한다.
 * 왜 움직였을까/다른 분석 위젯의 결과에 의존하지 않는다.
 */
(function(){
  const GaemiWidgets = window.GaemiWidgets = window.GaemiWidgets || {};
  let requestToken = 0;
  let controller = null;

  function render(data) {
    data = data || {};
    const priceEl = document.getElementById('tPriceVal');
    const changeEl = document.getElementById('tChangeVal');
    const countEl = document.getElementById('tEvidenceCountVal');
    const badgeEl = document.getElementById('tStatusBadge');
    const dotsEl = document.getElementById('tStatusDots');
    const trendEl = document.getElementById('tPriceTrendTxt');
    if (priceEl) priceEl.innerText = data.price_str || '-';
    const cp = Number(data.current_change_pct);
    if (changeEl) {
      changeEl.innerText = Number.isFinite(cp) ? `${cp >= 0 ? '+' : ''}${cp.toFixed(2)}%` : '-';
      changeEl.style.color = Number.isFinite(cp) && cp > 0 ? '#10b981' : (Number.isFinite(cp) && cp < 0 ? '#ef4444' : 'var(--text-title)');
    }
    const evidence = Array.isArray(data.evidence) ? data.evidence : [];
    const confirmed = evidence.filter(x => Number(x?.score || 0) !== 0);
    if (countEl) countEl.innerText = `${confirmed.length}/${Math.max(1, evidence.length)}`;
    if (badgeEl) badgeEl.innerText = data.status || '실제 데이터 기준';
    if (dotsEl) dotsEl.innerText = evidence.length ? `확인된 근거 ${confirmed.length}개` : '확인된 데이터가 없습니다.';

    const rows = Array.isArray(data.candles) ? data.candles.slice(-60) : [];
    const vals = rows.map(x => Number(x?.close)).filter(Number.isFinite);
    const chart = document.getElementById('tPriceChart');
    if (chart && vals.length > 1) {
      const first = vals[0];
      const normalized = first ? vals.map(v => ((v / first) - 1) * 100) : [];
      const W=600,H=120,pad=8;
      const min=Math.min(...normalized), max=Math.max(...normalized), span=Math.max(.01,max-min);
      const X=i=>pad+i*((W-pad*2)/Math.max(1,normalized.length-1));
      const Y=v=>pad+((max-v)/span)*(H-pad*2);
      const coords=normalized.map((v,i)=>`${X(i).toFixed(1)},${Y(v).toFixed(1)}`);
      const line='M '+coords.join(' L ');
      const last=normalized[normalized.length-1];
      const stroke=last>=0?'#10b981':'#ef4444';
      chart.innerHTML=`<line x1="0" y1="${Y(0).toFixed(1)}" x2="${W}" y2="${Y(0).toFixed(1)}" stroke="rgba(148,163,184,0.20)" stroke-width="1" stroke-dasharray="4 4"/><path d="${line}" fill="none" stroke="${stroke}" stroke-width="3" vector-effect="non-scaling-stroke"/><circle cx="${coords.at(-1).split(',')[0]}" cy="${coords.at(-1).split(',')[1]}" r="5" fill="${stroke}" stroke="#fff" stroke-width="2"/>`;
      if (trendEl) trendEl.innerText = `최근 60D ${last>=0?'+':''}${last.toFixed(2)}%`;
    } else if (trendEl) trendEl.innerText = '데이터 부족';

    const bar=document.getElementById('tEvidenceBar');
    const barLabel=document.getElementById('tEvidenceBarLabel');
    if (bar) {
      bar.innerHTML = evidence.slice(0,8).map(r=>{
        const score=Number(r?.score||0); const bg=score>0?'#10b981':score<0?'#ef4444':'#9ca3af';
        return `<div title="${r?.label||'근거'}" style="height:100%;background:${bg};border:1px solid ${bg};border-radius:3px;min-width:0;"></div>`;
      }).join('') || '<div style="height:100%;background:var(--bg-subcard);border:1px solid var(--border-line);border-radius:3px;flex:1;"></div>';
      if (barLabel) barLabel.innerText = `${confirmed.length} / ${Math.max(1,evidence.length)}`;
    }
    const list=document.getElementById('trackDetailList');
    if(list) list.innerHTML=evidence.map(r=>{
      const score=Number(r?.score||0); const cls=score>0?'status-pill-positive':score<0?'status-pill-negative':'status-pill-neutral';
      const status=score>0?'우호적':score<0?'부정적':'중립';
      return `<div class="quant-row-item"><div class="quant-row-left"><span class="quant-row-name">${r?.label||'-'}</span><span class="quant-row-status ${cls}">${status}</span></div><div class="quant-row-right"><div class="quant-row-val">${r?.detail||'-'}</div></div></div>`;
    }).join('') || `<div style="padding:12px 0;color:var(--text-muted);font-size:.78rem;">확인된 데이터가 없습니다.</div>`;
  }

  async function load(context) {
    render({});
    const ctx=context||{}; const token=++requestToken;
    if(controller){try{controller.abort();}catch(_){} }
    controller=new AbortController();
    const qs=new URLSearchParams({m:String(ctx.market||''),s:String(ctx.symbol||''),name:String(ctx.name||'')});
    try{
      const r=await fetch('/api/widget/track?'+qs.toString(),{cache:'no-store',signal:controller.signal});
      const payload=await r.json().catch(()=>null);
      if(token!==requestToken)return;
      if(!r.ok||!payload||payload.ok===false)throw new Error(payload?.error||'트랙 데이터 오류');
      render(payload.data||{});
    }catch(e){
      if(e?.name==='AbortError')return;
      if(token!==requestToken)return;
      console.warn('[TRACK] load failed:',e); render({});
    }
  }
  GaemiWidgets.track={render,load};
})();
