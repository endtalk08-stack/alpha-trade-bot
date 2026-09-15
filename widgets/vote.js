/* gaemiGTP — 개미 투표 독립 위젯 */
(function(){
function updateVoteUI(vData) {
            const total = vData.long + vData.short;
            const lPct = total > 0 ? Math.round((vData.long / total) * 100) : 50;
            const sPct = 100 - lPct;

            setTxt('voteLongPct', `상승 ${lPct}%`);
            setTxt('voteShortPct', `하락 ${sPct}%`);
            
            const lb = document.getElementById('voteLongBar');
            if (lb) lb.style.width = lPct + '%';
            const sb = document.getElementById('voteShortBar');
            if (sb) sb.style.width = sPct + '%';

            const btnL = document.getElementById('btnVoteLong');
            const btnS = document.getElementById('btnVoteShort');
            if (btnL && btnS) {
                btnL.classList.remove('active-long');
                btnS.classList.remove('active-short');
                if (vData.myVote === 'long') btnL.classList.add('active-long');
                if (vData.myVote === 'short') btnS.classList.add('active-short');
            }
        }

        async function castVote(type) {
            if (!currentSymbol) return;
            const voteKey = 'vote_' + currentSymbol;
            const rawVote = localStorage.getItem(voteKey) || JSON.stringify({ long: 15, short: 8, myVote: null });
            const vData = JSON.parse(rawVote);

            if (vData.myVote === type) return;

            if (vData.myVote === 'long') vData.long = Math.max(0, vData.long - 1);
            if (vData.myVote === 'short') vData.short = Math.max(0, vData.short - 1);

            if (type === 'long') vData.long += 1;
            if (type === 'short') vData.short += 1;

            vData.myVote = type;
            localStorage.setItem(voteKey, JSON.stringify(vData));
            updateVoteUI(vData);
        }
  function load(symbol){
    if(!symbol) return;
    const voteKey = 'vote_' + symbol;
    const rawVote = localStorage.getItem(voteKey) || JSON.stringify({ long: 15, short: 8, myVote: null });
    updateVoteUI(JSON.parse(rawVote));
  }
  window.GaemiWidgets = window.GaemiWidgets || {};
  window.GaemiWidgets.vote = { load: load, render: load, update: updateVoteUI };
  window.updateVoteUI = updateVoteUI;
  window.castVote = castVote;
})();
