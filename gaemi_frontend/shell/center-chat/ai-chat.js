/* Central AI chat: API replies, typing and safe bold rendering. Existing session is owned by app.js. */
(function () {
  'use strict';
  function create(ctx) {
    function submitQuestion(question, { newConversation = false } = {}) {
      const text = String(question || '').trim();
      if (!text) return false;
      if (!newConversation && ctx.getPendingQuestionSessionId() === ctx.getSession()?.id) return false;
      if (newConversation || !ctx.getSession()) {
        ctx.abortAnalysis();
        if (!ctx.beginChatSession()) return false;
      }
      const sessionId = ctx.getSession().id;
      const history = ctx.getSession().messages
        .filter((message) => message.kind === 'question' || message.kind === 'answer')
        .slice(-6)
        .map(({ role, text }) => ({ role, text: text.slice(0, 240) }));
      const row = ctx.appendChatMessage('user', text, 'question', sessionId);
      if (!row) return false;
      window.GaemiGTPSidebarHome?.recordRecent?.('question', text);
      row.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
      void requestChatAnswer(text, sessionId, history);
      return true;
    }

    // Parse complete bold markers before typing so their ** delimiters never flash.
    function getChatBoldSegments(text) {
      const segments = [];
      const boldPattern = /\*\*([^*\n]+)\*\*/g;
      let lastIndex = 0;
      for (const match of text.matchAll(boldPattern)) {
        if (match.index > lastIndex) segments.push({ text: text.slice(lastIndex, match.index), bold: false });
        segments.push({ text: match[1], bold: true });
        lastIndex = match.index + match[0].length;
      }
      if (lastIndex < text.length) segments.push({ text: text.slice(lastIndex), bold: false });
      return segments;
    }

    // The five existing response actions are attached only after typing ends.
    async function appendChatReply(answer, sessionId) {
      const text = String(answer || '').trim();
      if (!text || ctx.getSession()?.id !== sessionId) return false;
      const row = ctx.appendChatMessage('assistant', text, 'answer', sessionId);
      if (!row) return false;
      row.textContent = '';
      row.classList.add('typing-cursor');
      for (const segment of getChatBoldSegments(text)) {
        const node = segment.bold ? document.createElement('strong') : document.createTextNode('');
        row.appendChild(node);
        for (let index = 0; index < segment.text.length; index += 3) {
          if (ctx.getSession()?.id !== sessionId || !row.isConnected) return false;
          node.textContent += segment.text.slice(index, index + 3);
          row.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
          await new Promise((resolve) => setTimeout(resolve, 40));
        }
      }
      row.classList.remove('typing-cursor');
      if (ctx.getSession()?.id !== sessionId || !row.isConnected) return false;
      const actions = window.GaemiGTPFirstReplyActions?.appendFirstReplyActions?.(
        row, text, 'ai-' + row.dataset.chatMessageId, ctx.getSession().stock || ''
      );
      // AI 답변의 타이핑 흐름을 마지막 '싫어요' 아이콘까지 이어간다.
      // 기존 공통 액션 생성/클릭 로직은 그대로 재사용한다.
      if (actions) {
        const buttons = [...actions.querySelectorAll('[data-first-reply-action]')];
        buttons.forEach((button) => {
          button.style.visibility = 'hidden';
          button.style.pointerEvents = 'none';
        });
        for (const button of buttons) {
          if (ctx.getSession()?.id !== sessionId || !row.isConnected) return false;
          button.style.visibility = '';
          button.style.pointerEvents = '';
          button.scrollIntoView({ behavior: 'smooth', block: 'nearest', inline: 'nearest' });
          await new Promise((resolve) => setTimeout(resolve, 90));
        }
      }
      return true;
    }

    function appendChatError(text, sessionId) {
      if (ctx.getSession()?.id !== sessionId) return;
      const row = ctx.appendChatMessage('assistant', text, 'error', sessionId);
      row?.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    }

    // A lightweight, on-demand SVG line chart inside the existing chat thread.
    // This intentionally does not mount or reuse the alpha-bot chart widget.
    async function showRequestedPriceChart(question, sessionId, signal) {
      const stock = (window.GaemiGTPCenterChatSearch?.resolveStockMention?.(question)
        || (question.includes('삼성전자') ? '삼성전자' : '')
        || ctx.getSession()?.stock || '삼성전자').trim();
      const response = await fetch('/api/chart?q=' + encodeURIComponent(stock), {signal, cache: 'no-store'});
      if (!response.ok) throw new Error('시세를 가져오지 못했어.');
      const data = await response.json();
      const raw = data.candles || data.chart?.candles || data.chart_data?.candles || data.data?.candles || [];
      const candles = raw.filter((v) => Number.isFinite(Number(v.close)) && Number(v.close) > 0).slice(-65);
      if (!candles.length) throw new Error('표시할 가격 데이터가 없어.');
      if (signal.aborted || ctx.getSession()?.id !== sessionId) return;
      const answer = stock + ' 차트';
      const row = ctx.appendChatMessage('assistant', answer, 'answer', sessionId);
      if (!row) return;
      row.textContent = '';
      const card = document.createElement('div');
      card.style.cssText = 'width:min(100%,640px);padding:12px 8px;border:0;border-radius:0;background:transparent;box-sizing:border-box';
      const closes = candles.map((v) => Number(v.close));
      const low = Math.min(...closes), high = Math.max(...closes);
      const spread = Math.max(high - low, Math.abs(high) * 0.005, 1);
      const min = low - spread * 0.12, max = high + spread * 0.12;
      const width = 600, height = 240, left = 12, right = 10, top = 15, bottom = 34;
      const plotW = width - left - right, plotH = height - top - bottom;
      const y = (v) => top + (max - v) / (max - min) * plotH;
      const x = (i) => left + i / Math.max(1, candles.length - 1) * plotW;
      const svgNS = 'http://www.w3.org/2000/svg';
      const svg = document.createElementNS(svgNS, 'svg');
      svg.setAttribute('viewBox', '0 0 600 240');
      svg.setAttribute('role', 'img');
      svg.setAttribute('aria-label', stock + ' 최근 일봉 종가 추이');
      svg.style.cssText = 'display:block;width:100%;height:auto;overflow:visible';
      const line = (x1,y1,x2,y2,color,w=1) => {
        const p = document.createElementNS(svgNS,'line');
        for(const [k,v] of Object.entries({x1,y1,x2,y2})) p.setAttribute(k,String(v));
        p.setAttribute('stroke',color);p.setAttribute('stroke-width',String(w));svg.appendChild(p);
      };
      const label = (content, tx, ty, anchor='start') => {
        const t=document.createElementNS(svgNS,'text');t.textContent=content;
        t.setAttribute('x',String(tx));t.setAttribute('y',String(ty));
        t.setAttribute('text-anchor',anchor);t.setAttribute('font-size','12');
        t.setAttribute('fill','currentColor');svg.appendChild(t);
      };
      for (let i=0;i<=3;i++) {
        const v=min+(max-min)*i/3, yy=y(v);
        line(left,yy,width-right,yy,'#7b87932b');
        label(Math.round(v).toLocaleString('ko-KR'),width-right,yy-5,'end');
      }
      const path=document.createElementNS(svgNS,'path');
      const d=candles.map((v,i)=>(i?'L':'M')+x(i).toFixed(2)+' '+y(closes[i]).toFixed(2)).join(' ');
      path.setAttribute('d',d);path.setAttribute('stroke','#8b9cb8');
      path.setAttribute('stroke-width','2.5');path.setAttribute('stroke-linejoin','round');
      path.setAttribute('stroke-linecap','round');path.setAttribute('fill','none');svg.appendChild(path);
      const dot=document.createElementNS(svgNS,'circle');
      dot.setAttribute('cx',String(x(candles.length-1)));dot.setAttribute('cy',String(y(closes[closes.length-1])));
      dot.setAttribute('r','3');dot.setAttribute('fill','#8b9cb8');svg.appendChild(dot);
      label(String(candles[0].time||''),left,height-9);
      label(String(candles[candles.length-1].time||''),width-right,height-9,'end');
      card.appendChild(svg);
      row.appendChild(card);
      row.scrollIntoView({behavior:'smooth',block:'nearest'});
    }

    async function requestChatAnswer(question, sessionId, history) {
      const controller = new AbortController();
      ctx.setActiveQuestionController(controller);
      ctx.setPendingQuestionSessionId(sessionId);
      const mentionedStock = window.GaemiGTPCenterChatSearch?.resolveStockMention?.(question) || '';
      const stock = mentionedStock || ctx.getSession()?.stock || '';
      const errors = {
        stock_data_unavailable: '종목 데이터를 확인하지 못했어.',
        invalid_input: '종목명을 함께 입력해 줘.',
        minute_limit: '잠시 뒤에 다시 물어봐.',
        model_daily_limit: '오늘은 더 답변하기 어려워.',
        total_daily_limit: '오늘은 더 답변하기 어려워.',
        private_access_required: '로그인이 필요해.',
        ai_test_not_configured: '아직 답변을 준비하지 못했어.'
      };
      // 기존 중앙 채팅 로딩 스타일을 그대로 사용한다. 별도 AI 아이콘은 표시하지 않는다.
      const loading = document.createElement('div');
      loading.className = 'analysis-loading-sequence animate-fade';
      loading.setAttribute('role', 'status');
      loading.setAttribute('aria-live', 'polite');
      const loadingText = document.createElement('p');
      loadingText.className = 'analysis-loading-message is-visible is-shimmer';
      loadingText.textContent = '생각 중';
      loading.appendChild(loadingText);
      document.getElementById('chatArea')?.appendChild(loading);

      const updateWaiting = (message) => {
        if (ctx.getSession()?.id === sessionId && !controller.signal.aborted && loading.isConnected) {
          loadingText.textContent = message;
        }
      };
      const waitingTimers = [
        setTimeout(() => updateWaiting('조금 더 걸리고 있어'), 10000),
        setTimeout(() => updateWaiting('응답이 지연되고 있어. 잠시만 기다려줘'), 20000)
      ];
      const stopThinking = () => {
        waitingTimers.forEach(clearTimeout);
        loading.remove();
      };
      try {
        // If the first stock analysis is in flight, wait for its shared
        // server-side result instead of collecting the same data twice.
        if (ctx.getPendingAnalysis()?.stock === stock) await ctx.getPendingAnalysis().promise;
        if (controller.signal.aborted || ctx.getSession()?.id !== sessionId) return;
        if (/(차트|주가\\s*그래프|가격\\s*그래프).*(보여|그려|띄워|나와|줘)|(?:보여|그려|띄워).*(차트|그래프)/i.test(question)) {
          try {
            await showRequestedPriceChart(question, sessionId, controller.signal);
          } catch (chartError) {
            if (!controller.signal.aborted) appendChatError(chartError.message || '차트를 표시하지 못했어.', sessionId);
          }
          return;
        }
        const response = await fetch(ctx.backendUrl + '/api/ai-test/chat', {
          method: 'POST',
          credentials: 'same-origin',
          signal: controller.signal,
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ role: 'compare', stock, question, history })
        });
        const result = await response.json();
        if (controller.signal.aborted || ctx.getSession()?.id !== sessionId) return;
        if (!response.ok || !result.ok || !result.answer) {
          stopThinking();
          appendChatError(errors[result.error] || '지금 답변을 가져오지 못했어.', sessionId);
          return;
        }
        if (result.stock) {
          // AI가 다른 종목을 답했다면 이전 종목의 뉴스·공시 결과를 새 종목에 연결하지 않는다.
          const snapshotStock = window.GaemiGTPAnalysisData?.get?.()?.stockName;
          if (snapshotStock && snapshotStock !== result.stock) {
            window.GaemiGTPAnalysisData?.set?.(result.stock, {});
          }
          ctx.getSession().stock = result.stock;
          ctx.setStock(result.stock);
          window.GaemiGTPActiveStock = result.stock;
          window.GaemiGTPWorkspaceShellState?.save?.();
        }
        stopThinking();
        await appendChatReply(result.answer, sessionId);
      } catch (error) {
        if (error?.name !== 'AbortError' && ctx.getSession()?.id === sessionId) {
          appendChatError('지금 답변을 가져오지 못했어.', sessionId);
        }
      } finally {
        stopThinking();
        if (ctx.getActiveQuestionController() === controller) ctx.setActiveQuestionController(null);
        if (ctx.getPendingQuestionSessionId() === sessionId) ctx.setPendingQuestionSessionId(null);
      }
    }

    return { submitQuestion, requestChatAnswer, appendChatReply };
  }
  window.GaemiGTPCenterAIChat = { create };
}());
