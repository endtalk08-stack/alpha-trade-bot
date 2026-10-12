/* Market source adapter: one API ingress, normalized shared cache, no widget calls. */
(function(){
'use strict';
const URL='/api/world-board';
let rows=[],updatedAt=0,pending=null,error=false;
const ttl=300000;
function snapshot(){return {rows:rows.map(x=>({...x,sparkline:[...x.sparkline]})),updatedAt,error};}
function find(name){const x=rows.find(r=>r.name===name);return x?{...x,sparkline:[...x.sparkline]}:null;}
function normalize(item){
 const price=String(item?.price??'-');
 const rate=String(item?.rate??'-');
 const sparkline=(Array.isArray(item?.sparkline)?item.sparkline:[]).map(Number).filter(Number.isFinite);
 return {name:String(item?.name||''),price,rate,sparkline,up:Boolean(item?.up),market_status:item?.market_status||'',traded_at:item?.traded_at||''};
}
async function refresh(force=false){
 if(pending)return pending;
 if(!force && updatedAt && Date.now()-updatedAt<ttl)return snapshot();
 pending=(async()=>{
   try{
     const response=await fetch(URL,{cache:'no-store'});
     if(!response.ok)throw Error('market unavailable');
     const result=await response.json();
     if(!result.ok || !Array.isArray(result.data))throw Error('invalid market data');
     rows=result.data.filter(item=>item && item.name).map(normalize);
     error=false;updatedAt=Date.now();
   }catch(_){error=true;}
   finally{pending=null;window.dispatchEvent(new CustomEvent('gaemi-market-data-change'));}
   return snapshot();
 })();
 return pending;
}
window.GaemiGTPMarketData={refresh,find,snapshot};
}());
