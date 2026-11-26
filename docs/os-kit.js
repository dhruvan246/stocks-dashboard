/* one-screen kit — the chart pane and price loaders shared by the pages rebuilt as list → detail → chart
 * (styles: the os-* block at the end of theme.css). First user: indices.html (2026-11-26).
 * Needs lightweight-charts 5 on the page for OSKit.Chart. Nothing here guesses a number: a bar with no
 * open / high / low on file is drawn as a flat tick at its close and its legend shows the close only. */
(function(){
'use strict';
var DAY=86400;
var SF_STK='https://dhruvan246.github.io/sf-data/stk/', SF_OHLC='https://dhruvan246.github.io/sf-data/ohlc/';   // CORS-open, so the same address works on localhost
var MONS=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
var K={DAY:DAY};
K.fmtPct=function(n){ return n==null||!isFinite(n)?'—':(n>=0?'+':'')+n.toFixed(2)+'%'; };
K.cls=function(n){ return n==null||!isFinite(n)||n===0?'':(n>0?'os-pos':'os-neg'); };
K.esc=function(s){ return String(s==null?'':s).replace(/[&<>"]/g,function(c){ return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]; }); };
K.fmtN=function(v,d){ if(d==null)d=2; return v==null||!isFinite(v)?'—':Number(v).toLocaleString('en-IN',{minimumFractionDigits:d,maximumFractionDigits:d}); };
K.fmtVol=function(v){ return v==null?'—':v>=1e7?(v/1e7).toFixed(2)+'Cr':v>=1e5?(v/1e5).toFixed(2)+'L':v>=1e3?(v/1e3).toFixed(1)+'K':String(v); };
K.niceDate=function(iso){ var d=new Date(iso+'T00:00:00Z'); return d.getUTCDate()+' '+MONS[d.getUTCMonth()]+' '+d.getUTCFullYear(); };
K.tsISO=function(ts){ return new Date(ts*1000).toISOString().slice(0,10); };
K.gunzipJSON=async function(url){ var buf=await(await fetch(url)).arrayBuffer();
  return JSON.parse(await new Response(new Blob([new Uint8Array(buf)]).stream().pipeThrough(new DecompressionStream('gzip'))).text()); };

/* ---- daily bars of one stock: closes from sf-data/stk, open/high/low from sf-data/ohlc when it lines up bar for bar ---- */
var SB={};
K.loadStockBars=function(sym){
  if(SB[sym])return SB[sym];
  var slug=String(sym).replace(/[^A-Za-z0-9._-]/g,'_');
  var p=(async function(){
    var r=await Promise.all([
      fetch(SF_STK+slug+'.json').then(function(x){ if(!x.ok)throw new Error('HTTP '+x.status); return x.json(); }),
      fetch(SF_OHLC+slug+'.json').then(function(x){ return x.ok?x.json():null; }).catch(function(){ return null; }) ]);
    var S=r[0], J=r[1];
    if(!S||!Array.isArray(S.p)||!S.p.length)throw new Error('empty');
    var n=S.p.length, dd=S.dd||[], vq=Array.isArray(S.v)?S.v:[], v0=n-vq.length;
    var ok=!!(J&&J.n===n&&Array.isArray(J.o)&&J.o.length===n&&J.h.length===n&&J.l.length===n);
    var bars=[], off=S.d0, prev=-Infinity;
    for(var i=0;i<n;i++){ if(i>0)off+=dd[i-1]; if(!(off>prev))continue; prev=off; var c=S.p[i]/100; if(!(c>0))continue;
      var b={ts:S.ts+off*DAY,c:c}; if(ok){ b.o=c+J.o[i]/100; b.h=c+J.h[i]/100; b.l=c+J.l[i]/100; }
      if(v0>=0&&i>=v0&&vq[i-v0]!=null)b.v=vq[i-v0]; bars.push(b); }
    if(!bars.length)throw new Error('empty');
    return {bars:bars,ohlc:ok};
  })();
  SB[sym]=p; p.catch(function(){ delete SB[sym]; });
  return p;
};
/* ---- daily bars of one NSE index series: docs/idx_ohlc/<file>.json (scripts/build_index_ohlc.py), key as in _index.json ---- */
var IB={}, IDX=null;
K.indexFiles=function(){ return IDX||(IDX=fetch('./idx_ohlc/_index.json').then(function(r){ return r.json(); }).then(function(j){ return (j&&j.idx)||{}; }).catch(function(e){ IDX=null; throw e; })); };
K.loadIndexBars=function(key){
  if(IB[key])return IB[key];
  var p=(async function(){
    var ix=await K.indexFiles(), m=ix[key]; if(!m||!m.f)throw new Error('no file');
    var b=await fetch('./idx_ohlc/'+m.f+'.json').then(function(x){ if(!x.ok)throw new Error('HTTP '+x.status); return x.json(); });
    var ts=Date.parse(b.d0+'T00:00:00Z')/1000, bars=[], full=0;
    for(var i=0;i<b.c.length;i++){ if(i>0)ts+=b.dd[i-1]*DAY; var c=b.c[i]; if(c==null||!(c>0))continue;
      var o=b.o[i], h=b.h[i], l=b.l[i], bar={ts:ts,c:c/100};
      if(o!=null&&h!=null&&l!=null&&o>0&&h>0&&l>0){ bar.o=o/100; bar.h=h/100; bar.l=l/100; full++; } else bar.co=1;   // close only
      bars.push(bar); }
    if(!bars.length)throw new Error('empty');
    return {bars:bars,ohlc:full>0,mixed:full<bars.length};
  })();
  IB[key]=p; p.catch(function(){ delete IB[key]; });
  return p;
};
K.aggBars=function(bars,iv){
  if(iv==='D')return bars;
  var out=[], key=null, cur=null;
  bars.forEach(function(b){
    var d=new Date(b.ts*1000), k=iv==='M'?d.getUTCFullYear()*12+d.getUTCMonth():Math.floor((b.ts/DAY+3)/7);   // weeks run Monday–Sunday
    var o=b.o!=null?b.o:b.c, h=b.h!=null?b.h:b.c, l=b.l!=null?b.l:b.c;
    if(k!==key){ cur={ts:b.ts,o:o,h:h,l:l,c:b.c,v:b.v,co:b.co}; out.push(cur); key=k; }
    else{ cur.ts=b.ts; cur.c=b.c; cur.h=Math.max(cur.h,h); cur.l=Math.min(cur.l,l); if(!b.co)cur.co=0; if(b.v!=null)cur.v=(cur.v||0)+b.v; }
  });
  return out;
};
function emaArr(v,n){ var out=new Array(v.length).fill(null); if(v.length<n)return out; var s=0,i; for(i=0;i<n;i++)s+=v[i]; var e=s/n; out[n-1]=e; var k=2/(n+1);
  for(i=n;i<v.length;i++){ e=v[i]*k+e*(1-k); out[i]=e; } return out; }
var EMA_N=[21,50,200], EMA_COL=['#3b82f6','#eab308','#ef4444'];

/* ---- the chart pane ----  new OSKit.Chart({chart:el, legend:el, msg:el, iv:'D', range:'1Y', log:false}) */
K.Chart=function(o){
  var me=this; this.o=o; this.iv=o.iv||'D'; this.range=o.range||'1Y'; this.log=!!o.log; this.ch=null; this.data=null; this.meta={}; this.bars=[]; this.ema=[]; this.tIdx=new Map();
  // chart colours are resolved from the theme once, so a theme switch rebuilds the chart
  new MutationObserver(function(){ if(me.ch){ me.init(); me.draw(); } }).observe(document.documentElement,{attributes:true,attributeFilter:['data-theme']});
};
K.Chart.prototype.init=function(){
  var me=this; if(this.ch){ try{ this.ch.chart.remove(); }catch(e){} this.ch=null; }
  var LC=window.LightweightCharts; if(!LC)return false;
  var cs=getComputedStyle(document.documentElement), v=function(n){ return cs.getPropertyValue(n).trim(); };
  var pos=v('--pos')||'#26a69a', neg=v('--neg')||'#ef5350', bd=v('--border')||'#2a3040';
  var chart=LC.createChart(this.o.chart,{ autoSize:true,
    layout:{background:{color:'transparent'},textColor:v('--text-muted')||'#9aa3b8',fontSize:11,fontFamily:getComputedStyle(document.body).fontFamily},
    grid:{vertLines:{visible:false},horzLines:{color:bd}},
    rightPriceScale:{borderColor:bd,scaleMargins:{top:.08,bottom:.2},mode:this.log?1:0},
    timeScale:{borderColor:bd,rightOffset:5}, crosshair:{mode:0} });
  var c=this.ch={chart:chart,pos:pos,neg:neg,accent:v('--accent-text')||'#5eead4'};
  c.vol=chart.addSeries(LC.HistogramSeries,{priceFormat:{type:'volume'},priceScaleId:'vol',lastValueVisible:false,priceLineVisible:false});
  chart.priceScale('vol').applyOptions({scaleMargins:{top:.84,bottom:0}});
  c.ema=EMA_N.map(function(n,i){ return chart.addSeries(LC.LineSeries,{color:EMA_COL[i],lineWidth:1,priceLineVisible:false,lastValueVisible:false,crosshairMarkerVisible:false}); });
  c.line=chart.addSeries(LC.LineSeries,{color:c.accent,lineWidth:2,priceLineVisible:true,lastValueVisible:true});
  c.candle=chart.addSeries(LC.CandlestickSeries,{upColor:pos,downColor:neg,borderVisible:false,wickUpColor:pos,wickDownColor:neg});
  chart.subscribeCrosshairMove(function(p){ var i=(p&&p.time!=null)?me.tIdx.get(p.time):null; me.legend(i==null?me.bars.length-1:i); });
  return true;
};
/* d = {bars, ohlc} · meta = {name, tag, note, live:{price,title}} */
K.Chart.prototype.show=function(d,meta){
  if(!this.ch&&!this.init()){ this.clear('Chart library did not load — use “Full chart”.'); return false; }
  this.data=d; this.meta=meta||{}; this.o.msg.textContent=''; this.draw(); return true;
};
K.Chart.prototype.draw=function(){
  var d=this.data, c=this.ch; if(!d||!c)return;
  var bars=K.aggBars(d.bars,this.iv), tm=bars.map(function(b){ return K.tsISO(b.ts); }), cl=bars.map(function(b){ return b.c; });
  this.bars=bars; this.tIdx=new Map(tm.map(function(t,i){ return [t,i]; }));
  if(d.ohlc){ c.line.setData([]); c.candle.setData(bars.map(function(b,i){ return {time:tm[i],open:b.o!=null?b.o:b.c,high:b.h!=null?b.h:b.c,low:b.l!=null?b.l:b.c,close:b.c}; })); }
  else{ c.candle.setData([]); c.line.setData(bars.map(function(b,i){ return {time:tm[i],value:b.c}; })); }
  var up=function(b,i){ return (d.ohlc&&b.o!=null)?b.c>=b.o:(i===0||b.c>=bars[i-1].c); };
  c.vol.setData(bars.map(function(b,i){ return b.v==null?null:{time:tm[i],value:b.v,color:(up(b,i)?c.pos:c.neg)+'66'}; }).filter(Boolean));
  this.ema=EMA_N.map(function(n,j){ var e=emaArr(cl,n); c.ema[j].setData(e.map(function(x,i){ return x==null?null:{time:tm[i],value:x}; }).filter(Boolean)); return e; });
  this.applyRange(); this.legend(bars.length-1); this.liveLine();
};
/* a live price with no bar of its own yet is a dashed line — the feed has no open/high/low, so a candle would be made up */
K.Chart.prototype.liveLine=function(){
  var c=this.ch; if(!c)return;
  if(c.pl){ try{ c.plS.removePriceLine(c.pl); }catch(e){} c.pl=null; }
  var lv=this.meta.live, d=this.data; if(!lv||!d||!(lv.price>0)||!this.bars.length)return;
  if(lv.date&&K.tsISO(this.bars[this.bars.length-1].ts)>=lv.date)return;
  c.plS=d.ohlc?c.candle:c.line;
  c.pl=c.plS.createPriceLine({price:lv.price,color:c.accent,lineWidth:1,lineStyle:2,axisLabelVisible:true,title:lv.title||'Live'});
};
K.Chart.prototype.setLive=function(lv){ this.meta.live=lv; this.liveLine(); };
K.Chart.prototype.applyRange=function(){
  var c=this.ch; if(!c||!this.bars.length)return;
  var days={'3M':91,'6M':182,'1Y':365,'3Y':1095}[this.range], last=this.bars[this.bars.length-1].ts, first=this.bars[0].ts;
  if(!days||last-days*DAY<=first){ c.chart.timeScale().fitContent(); return; }
  c.chart.timeScale().setVisibleRange({from:K.tsISO(last-days*DAY),to:K.tsISO(last)});
};
K.Chart.prototype.legend=function(i){
  var b=this.bars[i], d=this.data, m=this.meta, el=this.o.legend; if(!b||!d){ el.innerHTML=''; return; }
  var prev=i>0?this.bars[i-1].c:null, chg=prev?b.c-prev:null, pc=prev?(chg/prev*100):null, cls=K.cls(chg==null?0:chg)||'os-pos', f=K.fmtN, me=this;
  var full=d.ohlc&&b.o!=null&&!b.co;
  el.innerHTML='<div><b>'+K.esc(m.name||'')+'</b> <span class="mut">· 1'+this.iv+(m.tag?' · '+K.esc(m.tag):'')+' · '+K.niceDate(K.tsISO(b.ts))+'</span> '+
    (full?'<span class="mut">O</span> <span class="'+cls+'">'+f(b.o)+'</span> <span class="mut">H</span> <span class="'+cls+'">'+f(b.h)+'</span> <span class="mut">L</span> <span class="'+cls+'">'+f(b.l)+'</span> ':'')+
    '<span class="mut">C</span> <span class="'+cls+'">'+f(b.c)+'</span>'+(chg==null?'':' <span class="'+cls+'">'+(chg>=0?'+':'')+f(chg)+' ('+K.fmtPct(pc)+')</span>')+
    (b.v==null?'':' <span class="mut">Vol</span> '+K.fmtVol(b.v))+'</div>'+
    '<div>'+EMA_N.map(function(n,j){ var e=me.ema[j]&&me.ema[j][i]; return '<span class="mut">EMA '+n+'</span> <span style="color:'+EMA_COL[j]+'">'+(e==null?'—':f(e))+'</span>'; }).join(' &nbsp;')+
    (full?'':' <span class="mut">· close only — no open/high/low on file for this '+(d.ohlc?'date':'series')+'</span>')+'</div>';
};
K.Chart.prototype.clear=function(msg){
  this.data=null; this.bars=[]; var c=this.ch;
  if(c){ c.candle.setData([]); c.line.setData([]); c.vol.setData([]); c.ema.forEach(function(s){ s.setData([]); }); if(c.pl){ try{ c.plS.removePriceLine(c.pl); }catch(e){} c.pl=null; } }
  this.o.legend.innerHTML=''; this.o.msg.textContent=msg||'';
};
K.Chart.prototype.setIv=function(iv){ this.iv=iv; if(iv==='W'&&(this.range==='3M'||this.range==='6M'))this.range='3Y'; if(iv==='M'&&this.range!=='Max')this.range='Max'; this.draw(); };
K.Chart.prototype.setRange=function(r){ this.range=r; this.applyRange(); };
K.Chart.prototype.setLog=function(on){ this.log=!!on; if(this.ch)this.ch.chart.priceScale('right').applyOptions({mode:this.log?1:0}); };
/* make the three panes fill the rest of the window (laptop widths); phones stack and scroll */
K.fitGrid=function(el){ if(!el)return; if(window.innerWidth<900){ el.style.height=''; return; } el.style.height=Math.max(460,window.innerHeight-el.getBoundingClientRect().top-12)+'px'; };
window.OSKit=K;
})();
