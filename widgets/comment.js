/* gaemiGTP - 개미 토론 독립 위젯 */
(function(){
  const W = window.GaemiWidgets = window.GaemiWidgets || {};

  function renderComments(list){
    const wrap = document.getElementById('commentList');
    if(!wrap) return;
    list = Array.isArray(list) ? list : [];
    const tag = document.getElementById('commentCountTag');
    if(tag) tag.textContent = `${list.length}개의 의견`;
    wrap.innerHTML = '';
    if(list.length === 0){
      wrap.innerHTML = '<div style="color:var(--text-muted); font-size:0.8rem; padding:10px 0; text-align:center;">첫 번째 한마디를 남겨보세요!</div>';
      return;
    }
    list.slice(0,8).forEach(c=>{
      const item=document.createElement('div');
      item.className='comment-item';
      const head=document.createElement('div'); head.className='comment-head';
      const nick=document.createElement('span'); nick.className='comment-nick'; nick.textContent=String(c?.nick ?? '익명개미');
      const time=document.createElement('span'); time.textContent=String(c?.time ?? '');
      head.append(nick,time);
      const body=document.createElement('div'); body.className='comment-body'; body.textContent=String(c?.text ?? '');
      item.append(head,body); wrap.appendChild(item);
    });
  }

  function load(symbol){
    if(!symbol) return;
    const key='comments_'+symbol;
    let list=[];
    try{ list=JSON.parse(localStorage.getItem(key)||'[]'); }catch(_){ list=[]; }
    renderComments(list);
  }

  function post(){
    const input=document.getElementById('commentInput');
    const symbol=window.currentSymbol;
    if(!input || !symbol) return;
    const txt=input.value.trim();
    if(!txt) return;
    const nicks=['익명개미','불꽃슈퍼개미','성투개미','황금손개미','존버대장','퀀트장인'];
    const nick=nicks[Math.floor(Math.random()*nicks.length)]+'#'+Math.floor(100+Math.random()*900);
    const key='comments_'+symbol;
    let comments=[];
    try{ comments=JSON.parse(localStorage.getItem(key)||'[]'); }catch(_){ comments=[]; }
    comments.unshift({nick,text:txt,time:'방금 전'});
    localStorage.setItem(key,JSON.stringify(comments));
    renderComments(comments);
    input.value='';
  }

  W.comment = {render:renderComments, load, post};
  // 기존 inline onclick 호환 유지
  window.renderComments=renderComments;
  window.postComment=post;
})();
