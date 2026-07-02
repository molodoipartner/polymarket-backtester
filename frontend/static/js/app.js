const neon='#29f19c',grid='#1d3550',text='#8293aa';
const chartInstances=new Map(),MAX_LINE_POINTS=1200,MAX_TRADE_MARKERS=600;
function sampleLTTB(points,threshold=MAX_LINE_POINTS){
  if(points.length<=threshold||threshold<3)return points;
  const sampled=[points[0]],bucket=(points.length-2)/(threshold-2);let anchor=0;
  for(let i=0;i<threshold-2;i++){
    const averageStart=Math.floor((i+1)*bucket)+1,averageEnd=Math.min(Math.floor((i+2)*bucket)+1,points.length);
    let averageX=0,averageY=0,averageCount=Math.max(1,averageEnd-averageStart);
    for(let j=averageStart;j<averageEnd;j++){averageX+=points[j].x;averageY+=points[j].y}
    averageX/=averageCount;averageY/=averageCount;
    const rangeStart=Math.floor(i*bucket)+1,rangeEnd=Math.min(Math.floor((i+1)*bucket)+1,points.length-1),a=points[anchor];
    let maxArea=-1,next=rangeStart;
    for(let j=rangeStart;j<rangeEnd;j++){const area=Math.abs((a.x-averageX)*(points[j].y-a.y)-(a.x-points[j].x)*(averageY-a.y));if(area>maxArea){maxArea=area;next=j}}
    sampled.push(points[next]);anchor=next;
  }
  sampled.push(points[points.length-1]);return sampled;
}
function sampleEvenly(points,limit=MAX_TRADE_MARKERS){
  if(points.length<=limit)return points;
  return Array.from({length:limit},(_,index)=>points[Math.round(index*(points.length-1)/(limit-1))]);
}
function replaceChart(element,configuration){
  const existing=chartInstances.get(element)||Chart.getChart(element);if(existing)existing.destroy();
  const chart=new Chart(element,configuration);chartInstances.set(element,chart);return chart;
}
const timeTick=value=>{
  const date=new Date(value);
  if(!Number.isFinite(date.getTime()))return '';
  const ms=date.getMilliseconds();
  if(!ms)return date.toLocaleString();
  return date.toLocaleTimeString(undefined,{hour:'2-digit',minute:'2-digit',second:'2-digit'})+'.'+String(ms).padStart(3,'0');
};
const timeScale={type:'linear',ticks:{color:text,maxTicksLimit:8,callback:timeTick},grid:{color:grid}};
const finiteNumber=value=>{
  if(value===null||value===undefined||value==='')return null;
  const number=Number(value);
  return Number.isFinite(number)?number:null;
};
const liveContractSeries=(history,key)=>history.map(row=>{
  const timestamp=finiteNumber(row.t),value=finiteNumber(row[key]);
  return timestamp===null?null:{x:timestamp*1000,y:value};
}).filter(Boolean);
const hasLiveContractValues=(history,key)=>history.some(row=>finiteNumber(row[key])!==null);
const liveCryptoScale=history=>{
  const values=history.map(row=>finiteNumber(row.crypto_price)).filter(value=>value!==null);
  const min=Math.min(...values),max=Math.max(...values);
  const scale={type:'linear',position:'left',ticks:{color:text,callback:v=>'$'+v.toLocaleString()},grid:{color:grid}};
  if(Number.isFinite(min)&&Number.isFinite(max)){
    const padding=Math.max((max-min)*0.2,Math.abs(max||min)*0.00002,0.000001);
    scale.min=min-padding;
    scale.max=max+padding;
  }
  return scale;
};
const liveContractChartDatasets=(history,extra=[])=>{
  const datasets=[
    {label:'Crypto',data:liveContractSeries(history,'crypto_price'),borderColor:neon,backgroundColor:'#29f19c12',yAxisID:'crypto',pointRadius:0,tension:.2,parsing:false}
  ];
  if(hasLiveContractValues(history,'target_price'))datasets.push({label:'Target',data:liveContractSeries(history,'target_price'),borderColor:'#8293aa',borderDash:[5,5],yAxisID:'crypto',pointRadius:0,tension:0,parsing:false});
  if(hasLiveContractValues(history,'yes_bid'))datasets.push({label:'YES bid',data:liveContractSeries(history,'yes_bid'),borderColor:'#60a5fa',backgroundColor:'#60a5fa12',yAxisID:'contract',pointRadius:0,tension:.2,parsing:false});
  if(hasLiveContractValues(history,'yes_ask'))datasets.push({label:'YES ask',data:liveContractSeries(history,'yes_ask'),borderColor:'#fbbf24',backgroundColor:'#fbbf2412',yAxisID:'contract',pointRadius:0,tension:.2,parsing:false});
  return datasets.concat(extra);
};
const liveContractChartOptions=history=>({animation:false,responsive:true,maintainAspectRatio:false,interaction:{intersect:false,mode:'nearest'},plugins:{legend:{labels:{color:text}},tooltip:{callbacks:{title:items=>items.length?timeTick(items[0].parsed.x):''}}},scales:{x:timeScale,crypto:liveCryptoScale(history),contract:{type:'linear',position:'right',min:0,max:100,ticks:{color:text,callback:v=>v.toFixed(0)+'c'},grid:{drawOnChartArea:false,color:grid}}}});
function createEquityChart(rows,elementId,trades=[]){
  const element=document.getElementById(elementId);if(!element)return;
  const values=sampleLTTB(rows.map(row=>({x:new Date(row.t).getTime(),y:Number(row.value)})).filter(point=>Number.isFinite(point.x)&&Number.isFinite(point.y)));
  const point=(timestamp,value)=>({x:new Date(timestamp).getTime(),y:Number(value)});
  const entries=sampleEvenly(trades.map(x=>point(x.opened_at,x.balance_before)));
  const winningExits=sampleEvenly(trades.filter(x=>x.pnl>=0).map(x=>point(x.closed_at,x.balance_after)));
  const losingExits=sampleEvenly(trades.filter(x=>x.pnl<0).map(x=>point(x.closed_at,x.balance_after)));
  replaceChart(element,{type:'line',data:{datasets:[
    {label:'Balance',data:values,borderColor:neon,backgroundColor:'#29f19c12',fill:true,tension:.15,pointRadius:0,borderWidth:2,parsing:false},
    {label:'Entry',type:'scatter',data:entries,pointStyle:'triangle',pointRadius:5,backgroundColor:'#fbbf24'},
    {label:'Winning exit',type:'scatter',data:winningExits,pointRadius:4,backgroundColor:neon},
    {label:'Losing exit',type:'scatter',data:losingExits,pointRadius:4,backgroundColor:'#ff647c'}
  ]},options:{animation:false,normalized:true,responsive:true,maintainAspectRatio:false,interaction:{intersect:false,mode:'nearest'},plugins:{legend:{labels:{color:text}},tooltip:{callbacks:{title:items=>items.length?timeTick(items[0].parsed.x):''}}},scales:{x:timeScale,y:{ticks:{color:text,callback:v=>'$'+v.toLocaleString()},grid:{color:grid}}}}})
}
function createAdvancedCharts(rows,trades,tradeSummary=null){
  const container=document.querySelector('.advanced-analytics');if(container&&!container.open){container.addEventListener('toggle',()=>{if(container.open)createAdvancedCharts(rows,trades,tradeSummary)},{once:true});return}
  const drawdownElement=document.getElementById('strategy-drawdown'),outcomesElement=document.getElementById('strategy-outcomes');
  if(drawdownElement){let peak=0;const values=sampleLTTB(rows.map(row=>{peak=Math.max(peak,row.value);return{x:new Date(row.t).getTime(),y:peak?-(peak-row.value)/peak*100:0}}));replaceChart(drawdownElement,{type:'line',data:{datasets:[{label:'Drawdown %',data:values,borderColor:'#ff647c',backgroundColor:'#ff647c18',fill:true,pointRadius:0,tension:.15,parsing:false}]},options:{animation:false,normalized:true,responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{x:timeScale,y:{ticks:{color:text,callback:value=>value.toFixed(1)+'%'},grid:{color:grid}}}}})}
  if(outcomesElement){const wins=tradeSummary?tradeSummary.wins:trades.filter(trade=>trade.pnl>0).length,losses=tradeSummary?tradeSummary.losses:trades.filter(trade=>trade.pnl<0).length,flat=tradeSummary?tradeSummary.flat:trades.length-wins-losses;replaceChart(outcomesElement,{type:'doughnut',data:{labels:['Profitable','Losing','Flat'],datasets:[{data:[wins,losses,flat],backgroundColor:[neon,'#ff647c','#8293aa'],borderColor:'#0d1b2d',borderWidth:3}]},options:{animation:false,responsive:true,maintainAspectRatio:false,plugins:{legend:{position:'bottom',labels:{color:text}}}}})}
}
function drawTradeContractChart(canvas,payload){
  if(!canvas)return null;
  const record=payload.record||{},trade=payload.trade||{},history=(record.history||[]).filter(row=>Number.isFinite(Number(row.t)));
  const point=(timestamp,value)=>({x:Date.parse(timestamp),y:Number(value)});
  const side=String(trade.side||'').toUpperCase();
  const entryY=Number.isFinite(Number(trade.entry_yes_axis_cents))?Number(trade.entry_yes_axis_cents):null;
  const exitY=Number.isFinite(Number(trade.exit_yes_axis_cents))?Number(trade.exit_yes_axis_cents):null;
  const entry=Number.isFinite(entryY)?[point(trade.opened_at,entryY)]:[];
  const exit=Number.isFinite(exitY)?[point(trade.closed_at,exitY)]:[];
  const entryColor=side==='DOWN'?'#ff647c':neon;
  return replaceChart(canvas,{type:'line',data:{datasets:liveContractChartDatasets(history,[
    {label:side+' entry',type:'scatter',data:entry,backgroundColor:entryColor,borderColor:'#07111f',borderWidth:3,yAxisID:'contract',pointStyle:'triangle',rotation:side==='DOWN'?180:0,pointRotation:side==='DOWN'?180:0,pointRadius:13,pointHoverRadius:15,order:-20,parsing:false},
    {label:'Expiration exit',type:'scatter',data:exit,backgroundColor:Number(trade.pnl)>=0?neon:'#ff647c',borderColor:'#07111f',borderWidth:2,yAxisID:'contract',pointRadius:8,pointHoverRadius:10,order:-10,parsing:false}
  ])},options:liveContractChartOptions(history)});
}
function initStrategyTradeContractCharts(){
  document.querySelectorAll('[data-trade-contract-toggle]').forEach(button=>{
    button.addEventListener('click',async()=>{
      const detailRow=button.closest('tr')?.nextElementSibling;
      if(!detailRow||!detailRow.matches('[data-trade-contract-row]'))return;
      const isOpen=!detailRow.classList.contains('hidden');
      if(isOpen){detailRow.classList.add('hidden');button.textContent='Open';return}
      detailRow.classList.remove('hidden');button.textContent='Hide';
      const status=detailRow.querySelector('[data-trade-contract-status]'),wrap=detailRow.querySelector('[data-trade-contract-chart-wrap]'),canvas=detailRow.querySelector('[data-trade-contract-chart]');
      if(detailRow.dataset.loaded==='true'){if(wrap)wrap.classList.remove('hidden');return}
      if(status)status.textContent='Loading';
      try{
        const response=await fetch(button.dataset.chartUrl,{headers:{Accept:'application/json'}});
        const payload=await response.json();
        if(!response.ok)throw new Error(payload.detail||'Unable to load contract chart');
        const record=payload.record||{},trade=payload.trade||{};
        const title=detailRow.querySelector('[data-trade-contract-title]'),symbol=detailRow.querySelector('[data-trade-contract-symbol]'),meta=detailRow.querySelector('[data-trade-contract-meta]');
        if(title)title.textContent=record.question||record.slug||'Saved contract';
        if(symbol)symbol.textContent=(record.crypto_symbol||'Live contract')+' / '+(trade.side||'trade');
        const entryCents=Number(trade.entry_price_cents);
        const axisCents=Number(trade.entry_yes_axis_cents);
        if(meta)meta.textContent='Entry '+new Date(trade.opened_at).toLocaleString()+' at '+(Number.isFinite(entryCents)?entryCents.toFixed(2):'-')+'c ('+(Number.isFinite(axisCents)?axisCents.toFixed(2):'-')+'c on YES axis); exit at expiration '+new Date(trade.closed_at).toLocaleString()+'.';
        if(wrap)wrap.classList.remove('hidden');
        drawTradeContractChart(canvas,payload);
        detailRow.dataset.loaded='true';
        if(status)status.textContent='Loaded';
      }catch(error){
        if(status)status.textContent='Failed';
        const meta=detailRow.querySelector('[data-trade-contract-meta]');
        if(meta)meta.textContent=error.message;
      }
    });
  });
}
function initLiveRetestLoading(){
  document.querySelectorAll('[data-live-retest-form]').forEach(form=>{
    form.addEventListener('submit',()=>{
      const button=form.querySelector('button[type="submit"]'),loading=form.querySelector('[data-live-retest-loading]');
      if(button){button.disabled=true;button.textContent='Retest running...'}
      if(loading)loading.classList.remove('hidden');
    });
  });
}

function initGroupGeneration(){
  const form=document.getElementById('group-generation-form');if(!form)return;
  const panel=document.getElementById('generation-progress'),status=document.getElementById('generation-status'),bar=document.getElementById('generation-progress-bar'),log=document.getElementById('generation-live-log'),cancelButton=document.getElementById('generation-cancel');
  const stages=['download','validation','backtest','finalization'];
  let activeSource=null,activeJobId=null,startController=null,cancelController=null;
  const closeSource=()=>{if(activeSource){activeSource.close();activeSource=null}};
  const resetCancel=()=>{activeJobId=null;startController=null;cancelButton&&cancelButton.classList.add('hidden');if(cancelButton){cancelButton.disabled=false;cancelButton.textContent='Stop / Cancel'}};
  window.addEventListener('pagehide',closeSource,{once:true});
  const appendLog=message=>{const row=document.createElement('p');row.textContent=new Date().toLocaleTimeString()+'  '+message;log.appendChild(row);while(log.childElementCount>250)log.firstElementChild.remove();log.scrollTop=log.scrollHeight};
  const render=payload=>{
    const index=stages.indexOf(payload.stage);
    document.querySelectorAll('.progress-step').forEach((element,position)=>{element.classList.toggle('complete',payload.stage==='complete'||position<index);element.classList.toggle('active',position===index);element.classList.toggle('failed',payload.stage==='error'||payload.stage==='cancelled')});
    if(payload.stage==='backtest'&&payload.total){bar.style.width=(50+payload.current/payload.total*35)+'%';status.textContent=payload.current+' / '+payload.total}
    else if(payload.stage==='download'){const done=payload.api_requests_completed,total=payload.api_requests_total;if(Number.isFinite(done)&&Number.isFinite(total)){bar.style.width=(10+(total?done/total*35:0))+'%';status.textContent='API requests: '+done+' / '+total}else{bar.style.width='15%';status.textContent='Step 1 / 4'}}
    else if(payload.stage==='validation'){bar.style.width='45%';status.textContent='Step 2 / 4'}
    else if(payload.stage==='finalization'){bar.style.width='92%';status.textContent='Step 4 / 4'}
    else if(payload.stage==='complete'){bar.style.width='100%';status.textContent='Complete'}
    else if(payload.stage==='error'){status.textContent='Failed'}
    else if(payload.stage==='cancelled'){status.textContent='Cancelled';bar.style.width='100%'}
    if(payload.message)appendLog(payload.message);
  };
  if(cancelButton)cancelButton.addEventListener('click',async()=>{
    cancelButton.disabled=true;cancelButton.textContent='Cancelling...';render({stage:'cancelled',message:'Cancellation requested. Stopping active backend work...'});
    if(activeJobId){
      cancelController=new AbortController();
      try{await fetch('/api/group-jobs/'+activeJobId+'/cancel',{method:'POST',headers:{Accept:'application/json'},signal:cancelController.signal})}
      catch(error){if(error.name!=='AbortError')appendLog('Cancel request failed: '+error.message)}
    }
  });
  form.addEventListener('submit',async event=>{
    event.preventDefault();
    closeSource();if(cancelController)cancelController.abort();resetCancel();
    const button=form.querySelector('button[type="submit"]');button.disabled=true;button.textContent='Generation in progress...';panel.classList.remove('hidden');log.textContent='';render({stage:'download',message:'Creating backend job...'});
    try{
      startController=new AbortController();
      const response=await fetch('/api/group-jobs',{method:'POST',body:new FormData(form),headers:{Accept:'application/json'},signal:startController.signal});
      if(!response.ok){const body=await response.json();throw new Error(body.detail||'Unable to start generation')}
      const job=await response.json(),source=new EventSource(job.events_url);activeJobId=job.job_id;activeSource=source;
      cancelButton&&cancelButton.classList.remove('hidden');appendLog('Job created. Connecting to the live progress stream...');
      source.addEventListener('progress',message=>{const payload=JSON.parse(message.data);render(payload);if(payload.stage==='complete'){closeSource();resetCancel();setTimeout(()=>window.location.assign(payload.result_url),500)}else if(payload.stage==='error'||payload.stage==='cancelled'){closeSource();resetCancel();button.disabled=false;button.textContent='Generate group and backtest'}});
      source.onerror=()=>appendLog('The live progress connection was interrupted; reconnecting...');
    }catch(error){render({stage:error.name==='AbortError'?'cancelled':'error',message:error.name==='AbortError'?'Generation start request cancelled.':error.message});resetCancel();button.disabled=false;button.textContent='Generate group and backtest'}
  });
}
function initCacheCalendar(){
  const calendar=document.getElementById('cache-calendar');if(!calendar)return;
  const form=document.getElementById('group-generation-form'),grid=document.getElementById('cache-calendar-grid'),status=document.getElementById('cache-calendar-status'),deleteButton=document.getElementById('cache-delete-selected');
  const startInput=form.querySelector('input[name="start_date"]'),endInput=form.querySelector('input[name="end_date"]');
  let controller=null,timer=null;
  const dayMs=86400000,dateKey=date=>date.toISOString().slice(0,10),parseDay=value=>{const [y,m,d]=value.split('-').map(Number);return new Date(Date.UTC(y,m-1,d))};
  const selectedAssets=()=>Array.from(form.querySelectorAll('input[name="asset_symbols"]:checked')).map(input=>input.value);
  const selectedTimeframe=()=>form.querySelector('input[name="timeframe"]:checked')?.value||'5m';
  const setStatus=text=>{if(status)status.textContent=text};
  const render=payload=>{
    const days=payload.days||[];grid.textContent='';
    let cached=0,partial=0;
    for(const day of days){if(day.complete)cached++;else if((day.covered_assets||[]).length)partial++}
    setStatus(cached+' cached / '+days.length+' days');
    for(const day of days){
      const covered=(day.covered_assets||[]).length,state=day.complete?'cached':covered?'partial':'missing';
      const button=document.createElement('button');button.type='button';button.className='cache-day '+state;
      if(day.date>=startInput.value&&day.date<=endInput.value)button.classList.add('selected');
      button.innerHTML='<strong>'+day.date.slice(5)+'</strong><small>'+(day.complete?'ready':covered?covered+'/'+payload.assets.length+' assets':'missing')+'</small>';
      button.addEventListener('click',()=>{startInput.value=day.date;endInput.value=day.date;refresh()});
      grid.appendChild(button);
    }
  };
  const refresh=()=>{
    clearTimeout(timer);timer=setTimeout(async()=>{
      const assets=selectedAssets();if(!assets.length){grid.textContent='';setStatus('No assets');return}
      if(controller)controller.abort();controller=new AbortController();setStatus('Loading');
      const params=new URLSearchParams({timeframe:selectedTimeframe(),start_date:startInput.value,end_date:endInput.value});
      for(const asset of assets)params.append('asset_symbols',asset);
      try{
        const response=await fetch('/api/fixed-cache/coverage?'+params.toString(),{headers:{Accept:'application/json'},signal:controller.signal});
        if(!response.ok)throw new Error('Unable to load cache calendar');
        render(await response.json());
      }catch(error){if(error.name!=='AbortError'){setStatus('Unavailable');grid.textContent='Cache calendar failed: '+error.message}}
    },150);
  };
  if(deleteButton)deleteButton.addEventListener('click',async()=>{
    const assets=selectedAssets();if(!assets.length)return;
    if(!window.confirm('Delete local cached contracts for the selected date range?'))return;
    deleteButton.disabled=true;deleteButton.textContent='Deleting...';
    const body=new FormData();body.append('timeframe',selectedTimeframe());body.append('start_date',startInput.value);body.append('end_date',endInput.value);for(const asset of assets)body.append('asset_symbols',asset);
    try{
      const response=await fetch('/api/fixed-cache/coverage/delete',{method:'POST',body,headers:{Accept:'application/json'}});
      if(!response.ok){const payload=await response.json();throw new Error(payload.detail||'Unable to delete cache')}
      const payload=await response.json();setStatus('Deleted '+payload.removed_events+' events');refresh();
    }catch(error){setStatus('Delete failed')}
    finally{deleteButton.disabled=false;deleteButton.textContent='Delete selected range cache'}
  });
  form.querySelectorAll('input[name="asset_symbols"],input[name="timeframe"],input[name="start_date"],input[name="end_date"]').forEach(input=>input.addEventListener('change',refresh));
  window.addEventListener('pagehide',()=>controller&&controller.abort(),{once:true});
  refresh();
}
function initRetestProgress(){
  const panel=document.getElementById('retest-progress');if(!panel)return;
  const status=document.getElementById('retest-status'),bar=document.getElementById('retest-progress-bar'),count=document.getElementById('retest-request-count'),log=document.getElementById('retest-live-log'),cancelButton=document.getElementById('retest-cancel');
  const source=new EventSource(panel.dataset.eventsUrl),append=message=>{const row=document.createElement('p');row.textContent=new Date().toLocaleTimeString()+'  '+message;log.appendChild(row);while(log.childElementCount>200)log.firstElementChild.remove();log.scrollTop=log.scrollHeight};
  window.addEventListener('pagehide',()=>source.close(),{once:true});
  if(cancelButton)cancelButton.addEventListener('click',async()=>{cancelButton.disabled=true;cancelButton.textContent='Cancelling...';status.textContent='Cancellation requested...';append('Cancellation requested. Stopping active backend work...');try{await fetch(panel.dataset.cancelUrl,{method:'POST',headers:{Accept:'application/json'}})}catch(error){append('Cancel request failed: '+error.message)}});
  source.addEventListener('progress',message=>{const payload=JSON.parse(message.data);if(payload.message){status.textContent=payload.message;append(payload.message)}if(Number.isFinite(payload.api_requests_completed)){const done=payload.api_requests_completed,total=payload.api_requests_total;count.textContent='API requests: '+done+' of '+total;bar.style.width=(10+(total?done/total*60:0))+'%'}if(payload.stage==='validation')bar.style.width='75%';else if(payload.stage==='backtest')bar.style.width='88%';else if(payload.stage==='complete'){bar.style.width='100%';count.textContent='Complete';source.close();if(cancelButton)cancelButton.classList.add('hidden');setTimeout(()=>window.location.replace(payload.result_url),500)}else if(payload.stage==='error'){bar.style.width='100%';count.textContent='Failed';source.close();if(cancelButton)cancelButton.classList.add('hidden')}else if(payload.stage==='cancelled'){bar.style.width='100%';count.textContent='Cancelled';source.close();if(cancelButton)cancelButton.classList.add('hidden')}});
  source.onerror=()=>append('The progress connection was interrupted; reconnecting...');
}
function initFavoriteForms(){
  document.querySelectorAll('form[data-favorite-form]').forEach(form=>{
    const button=form.querySelector('button[type="submit"]');if(!button)return;
    form.addEventListener('submit',async event=>{
      event.preventDefault();
      const original=button.textContent;button.disabled=true;button.textContent='Saving...';
      try{
        const response=await fetch(form.action,{method:'POST',headers:{Accept:'application/json'}});
        if(!response.ok)throw new Error('Unable to update favorite');
        const payload=await response.json();
        button.textContent=payload.label||original;
        button.classList.toggle('active',!!payload.favorite);
      }catch(error){
        button.textContent=original;
      }finally{
        button.disabled=false;
      }
    });
  });
}
function setupLiveTimeRange(panel){
  if(!panel)return false;
  const startInput=panel.querySelector('[data-live-range-start]'),endInput=panel.querySelector('[data-live-range-end]');
  const startHidden=panel.querySelector('[data-live-range-start-hidden]'),endHidden=panel.querySelector('[data-live-range-end-hidden]');
  const startLabel=panel.querySelector('[data-live-range-start-label]'),endLabel=panel.querySelector('[data-live-range-end-label]');
  const fill=panel.querySelector('[data-live-range-fill]');
  if(!startInput||!endInput)return false;
  let contracts=[];
  try{contracts=JSON.parse(panel.dataset.contracts||'[]')}catch(error){contracts=[]}
  const min=Number(panel.dataset.minTs),max=Number(panel.dataset.maxTs);
  if(!Number.isFinite(min)||!Number.isFinite(max)||max<=min)return false;
  const clamp=value=>Math.min(max,Math.max(min,Number(value)));
  const initialStart=Number.isFinite(Number(panel.dataset.selectedStartTs))?Number(panel.dataset.selectedStartTs):min;
  const initialEnd=Number.isFinite(Number(panel.dataset.selectedEndTs))?Number(panel.dataset.selectedEndTs):max;
  startInput.min=String(Math.floor(min));startInput.max=String(Math.ceil(max));
  endInput.min=String(Math.floor(min));endInput.max=String(Math.ceil(max));
  startInput.value=Math.floor(clamp(initialStart));
  endInput.value=Math.ceil(clamp(initialEnd));
  const formatTs=value=>{
    const date=new Date(Number(value)*1000);
    return Number.isFinite(date.getTime())?date.toLocaleString():'-';
  };
  const setCount=count=>{
    document.querySelectorAll('[data-live-range-count]').forEach(element=>{
      element.textContent=element.dataset.liveRangeCountFormat==='number'?String(count):count+' selected';
    });
  };
  const selectedCount=(start,end)=>contracts.filter(contract=>{
    const contractStart=Number(contract.start_ts),contractEnd=Number(contract.end_ts);
    return Number.isFinite(contractStart)&&Number.isFinite(contractEnd)&&contractStart>=start&&contractEnd<=end;
  }).length;
  const render=changed=>{
    let start=clamp(startInput.value),end=clamp(endInput.value);
    if(start>end){
      if(changed===startInput)end=start;
      else start=end;
    }
    startInput.value=Math.floor(start);endInput.value=Math.ceil(end);
    if(startHidden)startHidden.value=String(startInput.value);
    if(endHidden)endHidden.value=String(endInput.value);
    if(startLabel)startLabel.textContent=formatTs(startInput.value);
    if(endLabel)endLabel.textContent=formatTs(endInput.value);
    if(fill){
      const left=(Number(startInput.value)-min)/(max-min)*100,right=(Number(endInput.value)-min)/(max-min)*100;
      fill.style.left=Math.max(0,Math.min(100,left))+'%';
      fill.style.width=Math.max(0,Math.min(100,right)-Math.max(0,Math.min(100,left)))+'%';
    }
    setCount(selectedCount(Number(startInput.value),Number(endInput.value)));
  };
  panel._liveRangeRender=render;
  if(!panel._liveRangeBound){
    startInput.addEventListener('input',()=>panel._liveRangeRender&&panel._liveRangeRender(startInput));
    endInput.addEventListener('input',()=>panel._liveRangeRender&&panel._liveRangeRender(endInput));
    panel._liveRangeBound=true;
  }
  render(null);
  return true;
}
function initLiveTimeRanges(){
  document.querySelectorAll('[data-live-time-range]').forEach(setupLiveTimeRange);
}
function initLiveAnalysis(){
  const filterForm=document.getElementById('live-analysis-filter-form'),generationForm=document.getElementById('live-analysis-generation-form');
  if(!filterForm||!generationForm)return;
  const summaryStatus=document.getElementById('live-analysis-summary-status'),loadingPanel=document.getElementById('live-analysis-summary-loading');
  const rangePanel=document.getElementById('live-analysis-range'),submitButton=generationForm.querySelector('button[type="submit"]');
  const progressPanel=document.getElementById('live-analysis-progress'),progressStatus=document.getElementById('live-analysis-progress-status'),progressBar=document.getElementById('live-analysis-progress-bar'),progressLog=document.getElementById('live-analysis-live-log'),cancelButton=document.getElementById('live-analysis-cancel');
  let summaryController=null,activeSource=null,activeJobId=null;
  const setSummaryStatus=(text,mode='')=>{if(summaryStatus){summaryStatus.textContent=text;summaryStatus.className='badge '+mode}};
  const appendProgress=message=>{if(!progressLog||!message)return;const row=document.createElement('p');row.textContent=new Date().toLocaleTimeString()+'  '+message;progressLog.appendChild(row);while(progressLog.childElementCount>250)progressLog.firstElementChild.remove();progressLog.scrollTop=progressLog.scrollHeight};
  const closeSource=()=>{if(activeSource){activeSource.close();activeSource=null}};
  const syncGenerationFields=()=>{
    const asset=filterForm.querySelector('[name="asset_symbol"]')?.value||'BTC';
    const timeframe=filterForm.querySelector('[name="timeframe"]')?.value||'5m';
    generationForm.querySelector('input[name="asset_symbol"]').value=asset;
    generationForm.querySelector('input[name="timeframe"]').value=timeframe;
    const groupName=generationForm.querySelector('input[name="group_name"]');
    if(groupName&&!groupName.dataset.touched)groupName.value='Live_'+asset+'_5m';
    const include=!!filterForm.querySelector('[name="include_partial_contracts"]')?.checked;
    let hidden=generationForm.querySelector('input[name="include_partial_contracts"]');
    if(include&&!hidden){hidden=document.createElement('input');hidden.type='hidden';hidden.name='include_partial_contracts';hidden.value='true';generationForm.appendChild(hidden)}
    else if(!include&&hidden)hidden.remove();
    const mode=generationForm.querySelector('[data-live-analysis-contract-mode]');
    if(mode)mode.value=include?'Full + partial suitable contracts':'Full suitable contracts only';
  };
  const summaryParams=()=>{
    const params=new URLSearchParams(new FormData(filterForm));
    if(!filterForm.querySelector('[name="include_partial_contracts"]')?.checked)params.delete('include_partial_contracts');
    return params;
  };
  const renderSummary=payload=>{
    const summary=payload.summary||{};
    document.querySelectorAll('[data-live-analysis-metric]').forEach(element=>{
      const value=summary[element.dataset.liveAnalysisMetric];
      element.textContent=Number.isFinite(Number(value))?Number(value).toLocaleString():String(value??'-');
      element.classList.toggle('loss',element.dataset.liveAnalysisMetric==='partial_contracts'&&Number(value)>0||element.dataset.liveAnalysisMetric==='unsuitable_contracts'&&Number(value)>0);
    });
    document.querySelectorAll('[data-live-range-count]').forEach(element=>{
      const count=Number(summary.selected_contracts)||0;
      element.textContent=element.dataset.liveRangeCountFormat==='number'?String(count):count+' selected';
    });
    if(rangePanel&&summary.range_start_ts&&summary.range_end_ts){
      rangePanel.classList.remove('hidden');
      rangePanel.dataset.contracts=JSON.stringify(summary.contracts||[]);
      rangePanel.dataset.minTs=String(summary.range_start_ts);
      rangePanel.dataset.maxTs=String(summary.range_end_ts);
      rangePanel.dataset.selectedStartTs=String(summary.selected_start_ts||summary.range_start_ts);
      rangePanel.dataset.selectedEndTs=String(summary.selected_end_ts||summary.range_end_ts);
      setupLiveTimeRange(rangePanel);
    }else if(rangePanel){
      rangePanel.classList.add('hidden');
    }
    if(submitButton)submitButton.disabled=(Number(summary.selected_contracts)||0)<=0;
  };
  const loadSummary=async(updateUrl=false)=>{
    syncGenerationFields();
    if(summaryController)summaryController.abort();
    summaryController=new AbortController();
    setSummaryStatus('Loading');
    if(loadingPanel)loadingPanel.classList.remove('hidden');
    if(submitButton)submitButton.disabled=true;
    const params=summaryParams();
    try{
      const response=await fetch((filterForm.dataset.summaryUrl||'/api/live-monitor/analysis/summary')+'?'+params.toString(),{headers:{Accept:'application/json'},signal:summaryController.signal});
      const payload=await response.json();
      if(!response.ok)throw new Error(payload.detail||'Unable to load live analysis summary');
      renderSummary(payload);
      setSummaryStatus('Ready');
      if(loadingPanel)loadingPanel.classList.add('hidden');
      if(updateUrl)history.replaceState(null,'',filterForm.action+'?'+params.toString());
    }catch(error){
      if(error.name==='AbortError')return;
      setSummaryStatus('Failed','loss');
      if(loadingPanel){loadingPanel.classList.remove('hidden');const log=loadingPanel.querySelector('.live-log');if(log)log.innerHTML='<p>Summary failed: '+error.message+'</p>'}
    }
  };
  const renderProgress=payload=>{
    const stages=['validation','backtest','finalization'],index=stages.indexOf(payload.stage);
    progressPanel?.querySelectorAll('[data-live-analysis-stage]').forEach((element,position)=>{
      element.classList.toggle('complete',payload.stage==='complete'||position<index);
      element.classList.toggle('active',position===index);
      element.classList.toggle('failed',payload.stage==='error'||payload.stage==='cancelled');
    });
    if(payload.stage==='validation'){progressBar.style.width='20%';progressStatus.textContent='Step 1 / 3'}
    else if(payload.stage==='backtest'&&payload.total){progressBar.style.width=(35+payload.current/payload.total*45)+'%';progressStatus.textContent=payload.current+' / '+payload.total}
    else if(payload.stage==='backtest'){progressBar.style.width='35%';progressStatus.textContent='Step 2 / 3'}
    else if(payload.stage==='finalization'){progressBar.style.width='88%';progressStatus.textContent='Step 3 / 3'}
    else if(payload.stage==='complete'){progressBar.style.width='100%';progressStatus.textContent='Complete'}
    else if(payload.stage==='error'){progressStatus.textContent='Failed';progressBar.style.width='100%'}
    else if(payload.stage==='cancelled'){progressStatus.textContent='Cancelled';progressBar.style.width='100%'}
    if(payload.message)appendProgress(payload.message);
  };
  if(generationForm.querySelector('input[name="group_name"]'))generationForm.querySelector('input[name="group_name"]').addEventListener('input',event=>{event.currentTarget.dataset.touched='true'});
  filterForm.addEventListener('submit',event=>{event.preventDefault();loadSummary(true)});
  filterForm.querySelectorAll('select,input[type="checkbox"]').forEach(input=>input.addEventListener('change',()=>loadSummary(true)));
  generationForm.addEventListener('submit',async event=>{
    event.preventDefault();closeSource();syncGenerationFields();
    if(progressPanel)progressPanel.classList.remove('hidden');
    if(progressLog)progressLog.textContent='';
    renderProgress({stage:'validation',message:'Creating live-monitor analysis job...'});
    if(submitButton){submitButton.disabled=true;submitButton.textContent='Generation in progress...'}
    try{
      const response=await fetch(generationForm.dataset.jobUrl||'/api/live-monitor/analysis/jobs',{method:'POST',body:new FormData(generationForm),headers:{Accept:'application/json'}});
      const payload=await response.json();
      if(!response.ok)throw new Error(payload.detail||'Unable to start live analysis generation');
      activeJobId=payload.job_id;
      if(cancelButton){cancelButton.classList.remove('hidden');cancelButton.disabled=false;cancelButton.textContent='Stop / Cancel'}
      activeSource=new EventSource(payload.events_url);
      activeSource.addEventListener('progress',message=>{
        const data=JSON.parse(message.data);renderProgress(data);
        if(data.stage==='complete'){closeSource();if(cancelButton)cancelButton.classList.add('hidden');setTimeout(()=>window.location.assign(data.result_url),500)}
        else if(data.stage==='error'||data.stage==='cancelled'){closeSource();if(cancelButton)cancelButton.classList.add('hidden');if(submitButton){submitButton.disabled=false;submitButton.textContent='Generate live-monitor strategies'}}
      });
      activeSource.onerror=()=>appendProgress('The live progress connection was interrupted; reconnecting...');
    }catch(error){
      renderProgress({stage:'error',message:error.message});
      if(submitButton){submitButton.disabled=false;submitButton.textContent='Generate live-monitor strategies'}
    }
  });
  if(cancelButton)cancelButton.addEventListener('click',async()=>{
    if(!activeJobId)return;
    cancelButton.disabled=true;cancelButton.textContent='Cancelling...';
    renderProgress({stage:'cancelled',message:'Cancellation requested. Stopping active backend work...'});
    try{await fetch('/api/group-jobs/'+activeJobId+'/cancel',{method:'POST',headers:{Accept:'application/json'}})}catch(error){appendProgress('Cancel request failed: '+error.message)}
  });
  window.addEventListener('pagehide',()=>{closeSource();if(summaryController)summaryController.abort()},{once:true});
  loadSummary(false);
}
function initLiveMonitor(){
  const panel=document.getElementById('live-monitor');if(!panel)return;
  const form=document.getElementById('live-monitor-form'),sessionsContainer=document.getElementById('live-monitor-sessions'),startStatus=document.getElementById('live-monitor-start-status');
  const savedRows=document.getElementById('live-saved-rows'),savedRefresh=document.getElementById('live-saved-refresh'),savedCategory=document.getElementById('live-saved-category'),savedDeleteCategory=document.getElementById('live-saved-delete-category');
  const savedPrev=document.getElementById('live-saved-prev'),savedNext=document.getElementById('live-saved-next'),savedPageSummary=document.getElementById('live-saved-page-summary');
  const savedDetail=document.getElementById('live-saved-detail'),savedDetailClose=document.getElementById('live-saved-detail-close'),savedChartElement=document.getElementById('live-saved-chart');
  const savedDetailCategory=document.getElementById('live-saved-detail-category'),savedDetailTitle=document.getElementById('live-saved-detail-title'),savedDetailMeta=document.getElementById('live-saved-detail-meta');
  const savedPartialWarning=document.getElementById('live-saved-partial-warning'),savedGapList=document.getElementById('live-saved-gap-list');
  const savedTarget=document.getElementById('live-saved-target'),savedFinal=document.getElementById('live-saved-final'),savedOutcome=document.getElementById('live-saved-outcome'),savedHistoryCount=document.getElementById('live-saved-history-count');
  const savedCoverage=document.getElementById('live-saved-coverage'),savedAverageSpread=document.getElementById('live-saved-average-spread'),savedFinalDeviation=document.getElementById('live-saved-final-deviation'),savedYesRange=document.getElementById('live-saved-yes-range');
  const savedDisconnectCount=document.getElementById('live-saved-disconnect-count'),savedDisconnectedSeconds=document.getElementById('live-saved-disconnected-seconds');
  const savedCryptoDisconnectCount=document.getElementById('live-saved-crypto-disconnect-count'),savedCryptoDisconnectedSeconds=document.getElementById('live-saved-crypto-disconnected-seconds');
  let savedChart=null,selectedSavedCategory='all',savedPage=1;
  const savedPageSize=20;
  const sessionControllers=new Map();
  const fmtCents=(value,empty='-')=>Number.isFinite(value)?(value*100).toFixed(2)+'c':empty;
  const fmtHistoryCents=value=>Number.isFinite(value)?Number(value).toFixed(2)+'c':'-';
  const fmtSize=value=>Number.isFinite(value)?Number(value).toLocaleString(undefined,{maximumFractionDigits:4}):'-';
  const fmtPrice=value=>Number.isFinite(value)?'$'+Number(value).toLocaleString(undefined,{maximumFractionDigits:3}):'-';
  const fmtPriceToBeat=value=>Number.isFinite(value)?'$'+Number(value).toLocaleString(undefined,{minimumFractionDigits:0,maximumFractionDigits:8}):'-';
  const fmtPct=value=>Number.isFinite(value)?Number(value).toFixed(2)+'%':'-';
  const fmtSeconds=value=>Number.isFinite(value)?Math.round(value)+'s':'-';
  const fmtSecondsPrecise=value=>Number.isFinite(value)?Number(value).toFixed(1).replace(/\\.0$/,'')+'s':'-';
  const fmtAge=timestamp=>timestamp?new Date(timestamp*1000).toLocaleTimeString():'Waiting';
  const fmtSavedDate=value=>value?new Date(value).toLocaleString():'-';
  const shortToken=value=>value?value.slice(0,8)+'...'+value.slice(-6):'-';
  const categoryLabel=value=>{
    const base=String(value||'').split('/')[0].toLowerCase();
    return ({btc:'Bitcoin',eth:'Ethereum',sol:'Solana',xrp:'XRP',doge:'Dogecoin',bnb:'BNB',hype:'Hyperliquid'}[base])||base.toUpperCase()||'-';
  };
  const firstController=()=>sessionControllers.values().next().value||null;
  const appendLog=(entry,controller=null)=>{
    const target=controller?.log||firstController()?.log;
    if(!entry)return;
    const level=entry.level||'info',message=entry.message||String(entry);
    const row=document.createElement('p');row.className=level;row.textContent=(entry.time||new Date().toLocaleTimeString())+' ['+(entry.source||'browser')+'] '+message;
    if(target){
      target.appendChild(row);while(target.childElementCount>160)target.firstElementChild.remove();target.scrollTop=target.scrollHeight;
    }
    if(level==='error')console.error('[live-monitor]',entry);
    else if(level==='warning')console.warn('[live-monitor]',entry);
    else console.info('[live-monitor]',entry);
  };
  const renderSnapshotLogs=(logs,controller)=>{
    if(!controller?.log||!Array.isArray(logs)||!logs.length)return;
    controller.log.textContent='';
    for(const entry of logs.slice(-80))appendLog(entry,controller);
  };
  const setDot=(element,connected,error=false)=>{if(!element)return;element.classList.toggle('connected',!!connected&&!error);element.classList.toggle('error',!!error)};
  const setBusy=busy=>{
    const button=form.querySelector('button[type="submit"]');
    button.disabled=busy;button.textContent=busy?'Starting...':'Start tracking';
  };
  const resetSavedChart=()=>{if(savedChart){savedChart.destroy();savedChart=null}};
  const closeController=controller=>{if(controller?.source){controller.source.close();controller.source=null}};
  const destroyController=controller=>{
    closeController(controller);
    if(controller?.chart){
      controller.chart.destroy();
      chartInstances.delete(controller.chartElement);
      controller.chart=null;
    }
    if(controller?.card)controller.card.remove();
    if(controller?.id)sessionControllers.delete(controller.id);
  };
  const setStartStatus=(text,mode='')=>{
    if(!startStatus)return;
    startStatus.textContent=text;
    startStatus.className='muted mt-3 '+mode;
  };
  const createSessionCard=payload=>{
    const id=payload.session_id;
    const card=document.createElement('section');
    card.className='panel live-session-card';
    card.dataset.sessionId=id;
    card.innerHTML=[
      '<div class="section-title">',
      '<div><p class="eyebrow">Live monitor</p><h2 class="text-xl" data-live-event-title>Live session</h2><p class="muted" data-live-event-slug>-</p></div>',
      '<div class="live-card-actions"><button class="secondary" type="button" data-live-chart-toggle>Show chart</button><button class="secondary" type="button" data-live-stop>Stop</button></div>',
      '</div>',
      '<div class="live-status-grid">',
      '<div class="metric-card"><p>Crypto price</p><strong data-live-crypto-price>-</strong><span class="muted"><span class="ws-dot" data-live-crypto-dot></span><span data-live-crypto-status>Waiting</span></span></div>',
      '<div class="metric-card"><p>Order book</p><strong data-live-quote-status>Idle</strong><span class="muted"><span class="ws-dot" data-live-quote-dot></span><span data-live-sample-path>Saved samples will appear after start.</span></span></div>',
      '<div class="metric-card"><p>Session</p><strong data-live-session-short>-</strong><span class="muted">Separate stream and console</span></div>',
      '</div>',
      '<div class="live-monitor-log" data-live-log><p class="muted">Live WebSocket events and errors will appear here.</p></div>',
      '<div class="live-chart-wrap hidden" data-live-chart-wrap><canvas data-live-chart height="120"></canvas></div>',
      '<div class="overflow-x-auto"><table class="live-monitor-table"><thead><tr><th>Market</th><th>Outcome</th><th>Bid</th><th>Ask</th><th>Spread</th><th>Size</th><th>Updates</th><th>Token</th></tr></thead><tbody data-live-rows><tr><td colspan="8" class="empty">Waiting for live session.</td></tr></tbody></table></div>'
    ].join('');
    sessionsContainer.prepend(card);
    const controller={
      id,
      stopUrl:payload.stop_url,
      eventsUrl:payload.events_url,
      source:null,
      chart:null,
      chartOpen:false,
      latestPayload:null,
      card,
      lastPayloadAt:0,
      staleWarningShown:false,
      eventTitle:card.querySelector('[data-live-event-title]'),
      eventSlug:card.querySelector('[data-live-event-slug]'),
      cryptoPrice:card.querySelector('[data-live-crypto-price]'),
      cryptoStatus:card.querySelector('[data-live-crypto-status]'),
      quoteStatus:card.querySelector('[data-live-quote-status]'),
      samplePath:card.querySelector('[data-live-sample-path]'),
      quoteDot:card.querySelector('[data-live-quote-dot]'),
      cryptoDot:card.querySelector('[data-live-crypto-dot]'),
      rows:card.querySelector('[data-live-rows]'),
      log:card.querySelector('[data-live-log]'),
      chartElement:card.querySelector('[data-live-chart]'),
      chartWrap:card.querySelector('[data-live-chart-wrap]'),
      chartToggle:card.querySelector('[data-live-chart-toggle]'),
      stopButton:card.querySelector('[data-live-stop]'),
      sessionShort:card.querySelector('[data-live-session-short]')
    };
    controller.sessionShort.textContent=shortToken(id);
    controller.chartToggle.addEventListener('click',()=>toggleChart(controller));
    controller.stopButton.addEventListener('click',()=>stopSession(controller));
    sessionControllers.set(id,controller);
    return controller;
  };
  const getOrCreateController=payload=>{
    const existing=sessionControllers.get(payload.session_id);
    if(existing){
      existing.stopUrl=payload.stop_url;
      existing.eventsUrl=payload.events_url;
      return existing;
    }
    return createSessionCard(payload);
  };
  const ensureChart=controller=>{
    if(!controller.chartOpen)return null;
    if(controller.chart||!controller.chartElement)return controller.chart;
    controller.chart=replaceChart(controller.chartElement,{type:'line',data:{datasets:[
      {label:'Crypto',data:[],borderColor:neon,backgroundColor:'#29f19c12',yAxisID:'crypto',pointRadius:0,tension:.2,parsing:false},
      {label:'Target',data:[],borderColor:'#8293aa',borderDash:[5,5],yAxisID:'crypto',pointRadius:0,tension:0,parsing:false},
      {label:'YES bid',data:[],borderColor:'#60a5fa',backgroundColor:'#60a5fa12',yAxisID:'contract',pointRadius:0,tension:.2,parsing:false},
      {label:'YES ask',data:[],borderColor:'#fbbf24',backgroundColor:'#fbbf2412',yAxisID:'contract',pointRadius:0,tension:.2,parsing:false}
    ]},options:{animation:false,responsive:true,maintainAspectRatio:false,interaction:{intersect:false,mode:'nearest'},plugins:{legend:{labels:{color:text}},tooltip:{callbacks:{title:items=>items.length?timeTick(items[0].parsed.x):''}}},scales:{x:timeScale,crypto:{type:'linear',position:'left',ticks:{color:text,callback:v=>'$'+v.toLocaleString()},grid:{color:grid}},contract:{type:'linear',position:'right',min:0,max:100,ticks:{color:text,callback:v=>v.toFixed(0)+'c'},grid:{drawOnChartArea:false,color:grid}}}}});
    return controller.chart;
  };
  const closeChart=controller=>{
    if(controller.chart){
      controller.chart.destroy();
      chartInstances.delete(controller.chartElement);
      controller.chart=null;
    }
  };
  const toggleChart=controller=>{
    controller.chartOpen=!controller.chartOpen;
    controller.chartWrap.classList.toggle('hidden',!controller.chartOpen);
    controller.chartToggle.textContent=controller.chartOpen?'Hide chart':'Show chart';
    if(controller.chartOpen)updateChart(controller.latestPayload||{},controller);
    else closeChart(controller);
  };
  const updateChart=(payload,controller)=>{
    if(!controller.chartOpen)return;
    const current=ensureChart(controller);if(!current)return;
    const history=(payload.history||[]).filter(row=>Number.isFinite(row.t));
    const series=key=>history.map(row=>({x:Number(row.t)*1000,y:Number.isFinite(row[key])?Number(row[key]):null}));
    current.data.datasets[0].data=series('crypto_price');
    current.data.datasets[1].data=series('target_price');
    current.data.datasets[2].data=series('yes_bid');
    current.data.datasets[3].data=series('yes_ask');
    current.update('none');
  };
  const drawSavedChart=record=>{
    if(!savedChartElement)return;
    const history=(record.history||[]).filter(row=>finiteNumber(row.t)!==null);
    savedChart=replaceChart(savedChartElement,{type:'line',data:{datasets:liveContractChartDatasets(history)},options:liveContractChartOptions(history)});
  };
  const renderSavedDetail=record=>{
    if(!savedDetail)return;
    const summary=record.summary||{};
    savedDetail.classList.remove('hidden');
    savedDetailCategory.textContent=(record.category?.label||categoryLabel(record.crypto_symbol))+' saved contract';
    savedDetailTitle.textContent=record.question||record.slug||'Saved contract';
    savedDetailMeta.textContent=(record.event_title||'-')+' - saved '+fmtSavedDate(record.saved_at)+' - duration '+fmtSeconds(summary.duration_seconds);
    const warnings=[];
    if(record.unsuitable_contract){
      const parts=[record.unsuitable_reason||'Tracking did not cover the full contract window.'];
      if(record.tracking_started_after_seconds!=null)parts.push('Started after '+fmtSeconds(record.tracking_started_after_seconds)+'.');
      if(record.tracking_ended_before_seconds!=null)parts.push('Ended before close by '+fmtSeconds(record.tracking_ended_before_seconds)+'.');
      warnings.push('Unsuitable: '+parts.join(' '));
    }
    if(record.partial_contract){
      warnings.push('Partial: '+(record.partial_reason||'Connection was interrupted during tracking.'));
    }
    if(warnings.length){
      savedPartialWarning.textContent=warnings.join(' ');
      savedPartialWarning.classList.remove('hidden');
    }else{
      savedPartialWarning.textContent='';
      savedPartialWarning.classList.add('hidden');
    }
    const gaps=record.connection_gap_periods||[];
    if(savedGapList){
      if(gaps.length){
        savedGapList.textContent='';
        for(const gap of gaps){
          const row=document.createElement('p');
          const source=gap.source==='crypto'?'Crypto':'Polymarket';
          const range=Number.isFinite(gap.start_second)&&Number.isFinite(gap.end_second)
            ?fmtSecondsPrecise(gap.start_second)+' - '+fmtSecondsPrecise(gap.end_second)
            :(gap.start_time||'-')+' - '+(gap.end_time||'-');
          const reason=gap.reason?' reason: '+gap.reason+'.':'';
          row.textContent=source+': '+range+'; duration '+fmtSecondsPrecise(gap.duration_seconds)+'.'+reason;
          savedGapList.appendChild(row);
        }
        savedGapList.classList.remove('hidden');
      }else{
        savedGapList.textContent='';
        savedGapList.classList.add('hidden');
      }
    }
    savedTarget.textContent=fmtPriceToBeat(record.target_price);
    savedFinal.textContent=fmtPrice(record.final_crypto_price);
    savedOutcome.textContent=record.winning_outcome||'-';
    savedHistoryCount.textContent=String((record.history||[]).length);
    savedCoverage.textContent=fmtPct(summary.coverage_pct);
    savedAverageSpread.textContent=fmtHistoryCents(summary.average_spread);
    savedFinalDeviation.textContent=fmtPct(summary.final_deviation_pct);
    savedYesRange.textContent=fmtHistoryCents(summary.min_yes_bid)+' / '+fmtHistoryCents(summary.max_yes_ask);
    savedDisconnectCount.textContent=String(record.polymarket_disconnect_count||0);
    savedDisconnectedSeconds.textContent=fmtSeconds(record.polymarket_disconnected_seconds||0);
    savedCryptoDisconnectCount.textContent=String(record.crypto_disconnect_count||0);
    savedCryptoDisconnectedSeconds.textContent=fmtSeconds(record.crypto_disconnected_seconds||0);
    drawSavedChart(record);
  };
  const openSaved=async recordId=>{
    try{
      const response=await fetch('/api/live-monitor/saved/'+encodeURIComponent(recordId),{headers:{Accept:'application/json'}});
      const payload=await response.json();
      if(!response.ok)throw new Error(payload.detail||'Unable to open saved contract');
      renderSavedDetail(payload.record);
    }catch(error){
      console.error('[live-monitor] saved open failed',error);
      appendLog({level:'error',source:'browser',message:error.message});
    }
  };
  const deleteSavedCategory=async()=>{
    if(selectedSavedCategory==='all')return;
    const label=savedCategory?.selectedOptions?.[0]?.textContent||selectedSavedCategory;
    if(!window.confirm('Delete all saved contracts in '+label+'?'))return;
    try{
      const response=await fetch('/api/live-monitor/saved/categories/'+encodeURIComponent(selectedSavedCategory),{method:'DELETE',headers:{Accept:'application/json'}});
      const payload=await response.json();
      if(!response.ok)throw new Error(payload.detail||'Unable to delete category');
      appendLog({level:'warning',source:'browser',message:'Deleted '+(payload.deleted||0)+' saved contract(s) from '+label+'.'});
      selectedSavedCategory='all';
      savedPage=1;
      if(savedDetail){savedDetail.classList.add('hidden');resetSavedChart()}
      loadSaved();
    }catch(error){
      console.error('[live-monitor] saved category delete failed',error);
      appendLog({level:'error',source:'browser',message:error.message});
    }
  };
  const renderSavedCategories=(categories=[])=>{
    if(!savedCategory)return;
    const current=selectedSavedCategory;
    savedCategory.textContent='';
    const all=document.createElement('option');all.value='all';all.textContent='All contracts';savedCategory.appendChild(all);
    let hasCurrent=current==='all';
    for(const category of categories){
      const option=document.createElement('option');
      option.value=category.id;
      option.textContent=(category.label||categoryLabel(category.symbol))+' ('+(category.count||0)+')';
      savedCategory.appendChild(option);
      if(category.id===current)hasCurrent=true;
    }
    if(!hasCurrent)selectedSavedCategory='all';
    savedCategory.value=selectedSavedCategory;
    if(savedDeleteCategory)savedDeleteCategory.classList.toggle('hidden',selectedSavedCategory==='all');
  };
  const renderSavedPagination=(payload={})=>{
    const page=Number(payload.page)||1,totalPages=Number(payload.total_pages)||1,total=Number(payload.total)||0;
    if(savedPageSummary)savedPageSummary.textContent=total?'Page '+page+' / '+totalPages+' - '+total+' contract(s)':'No contracts';
    if(savedPrev)savedPrev.disabled=!payload.has_previous;
    if(savedNext)savedNext.disabled=!payload.has_next;
  };
  const renderRows=(payload,controller)=>{
    const quotes=payload.quotes||[];
    if(!quotes.length){controller.rows.innerHTML='<tr><td colspan="8" class="empty">No token ids found.</td></tr>';return}
    controller.rows.textContent='';
    for(const quote of quotes){
      const tr=document.createElement('tr');
      const spread=Number.isFinite(quote.bid)&&Number.isFinite(quote.ask)?quote.ask-quote.bid:null;
      const size=[fmtSize(quote.bid_size),fmtSize(quote.ask_size)].join(' / ');
      tr.innerHTML='<td></td><td></td><td></td><td></td><td></td><td></td><td></td><td><code></code></td>';
      tr.children[0].textContent=quote.market_question||'-';
      tr.children[1].textContent=quote.outcome||'-';
      tr.children[2].textContent=fmtCents(quote.bid,'No bid');
      tr.children[3].textContent=fmtCents(quote.ask,'No ask');
      tr.children[4].textContent=spread===null?'-':(spread*100).toFixed(2)+'c';
      tr.children[5].textContent=size;
      tr.children[6].textContent=String(quote.event_count||0);
      tr.children[7].querySelector('code').textContent=shortToken(quote.token_id);
      controller.rows.appendChild(tr);
    }
  };
  const render=(payload,controller)=>{
    controller.latestPayload=payload;
    controller.lastPayloadAt=Date.now();controller.staleWarningShown=false;
    controller.eventTitle.textContent=payload.event?.title||'Live session';
    controller.eventSlug.textContent=payload.event?.slug||'-';
    controller.cryptoPrice.textContent=fmtPrice(payload.crypto?.price);
    controller.cryptoStatus.textContent=(payload.crypto?.symbol||'')+' - target '+fmtPriceToBeat(payload.crypto?.target_price)+' - '+(payload.status?.crypto||'waiting')+' - '+fmtAge(payload.crypto?.updated_at);
    controller.quoteStatus.textContent=payload.status?.quotes||'waiting';
    const wsAge=payload.health?.polymarket?.last_message_age;
    const wsAgeText=Number.isFinite(wsAge)?' | ws age '+wsAge.toFixed(1)+'s':'';
    const restPolls=payload.health?.rest?.poll_count||0;
    controller.samplePath.textContent=(payload.saved_path||payload.samples_path||'Saving to server_data/live_monitor')+' | tick '+(payload.tick_count||0)+wsAgeText+' | rest '+restPolls;
    if(payload.status?.last_error)controller.samplePath.textContent='Last error: '+payload.status.last_error;
    setDot(controller.quoteDot,!!payload.health?.polymarket?.connected,!!payload.status?.last_error&&payload.status?.quotes==='reconnecting');
    setDot(controller.cryptoDot,!!payload.health?.crypto?.connected,!!payload.status?.last_error&&payload.status?.crypto==='reconnecting');
    renderRows(payload,controller);updateChart(payload,controller);
  };
  const connectSession=payload=>{
    const controller=getOrCreateController(payload);
    closeController(controller);
    render(payload.snapshot,controller);
    renderSnapshotLogs(payload.snapshot?.logs,controller);
    controller.stopButton.classList.remove('hidden');
    appendLog({level:'info',source:'browser',message:'Connected to live monitor session '+controller.id+'.'},controller);
    controller.source=new EventSource(payload.events_url);
    const listener=message=>render(JSON.parse(message.data),controller);
    controller.source.addEventListener('snapshot',listener);
    controller.source.addEventListener('quote',listener);
    controller.source.addEventListener('crypto',listener);
    controller.source.addEventListener('status',listener);
    controller.source.addEventListener('tick',listener);
    controller.source.addEventListener('rolled',message=>{const data=JSON.parse(message.data);render(data,controller);appendLog({level:'info',source:'browser',message:'Rolled to '+(data.event?.slug||'next contract')},controller)});
    controller.source.addEventListener('saved',message=>{const data=JSON.parse(message.data);render(data,controller);appendLog({level:'info',source:'browser',message:'Saved completed contract'},controller);loadSaved()});
    controller.source.addEventListener('log',message=>{const data=JSON.parse(message.data);render(data,controller);appendLog(data.log,controller)});
    controller.source.addEventListener('ended',message=>{render(JSON.parse(message.data),controller);appendLog({level:'warning',source:'browser',message:'Tracking ended'},controller);closeController(controller);controller.stopButton.classList.add('hidden');loadSaved()});
    controller.source.onerror=error=>{controller.quoteStatus.textContent='Stream reconnecting';console.error('[live-monitor] SSE error',error);appendLog({level:'error',source:'browser',message:'SSE stream interrupted; browser will retry.'},controller)};
    return controller;
  };
  const restoreActiveSession=async()=>{
    try{
      const response=await fetch('/api/live-monitors/active',{headers:{Accept:'application/json'}});
      if(!response.ok)return;
      const payload=await response.json();
      if(!payload.active)return;
      const sessions=payload.sessions||[payload];
      for(const session of sessions)connectSession(session);
      appendLog({level:'info',source:'browser',message:'Restored '+sessions.length+' active session(s) after page load.'});
    }catch(error){
      console.error('[live-monitor] restore failed',error);
      appendLog({level:'error',source:'browser',message:'Unable to restore active session: '+error.message});
    }
  };
  const loadSaved=async()=>{
    if(!savedRows)return;
    try{
      const params=new URLSearchParams({page:String(savedPage),page_size:String(savedPageSize)});
      if(selectedSavedCategory!=='all')params.set('category',selectedSavedCategory);
      const url='/api/live-monitor/saved?'+params.toString();
      const response=await fetch(url,{headers:{Accept:'application/json'}});
      if(!response.ok)throw new Error('Unable to load saved contracts');
      const payload=await response.json(),records=payload.records||[];
      savedPage=Number(payload.page)||savedPage;
      if(!records.length&&Number(payload.total)>0&&savedPage>1){savedPage-=1;loadSaved();return}
      renderSavedCategories(payload.categories||[]);
      renderSavedPagination(payload);
      if(!records.length){savedRows.innerHTML='<tr><td colspan="9" class="empty">No saved contracts yet.</td></tr>';return}
      savedRows.textContent='';
      for(const record of records){
        const tr=document.createElement('tr');
        tr.innerHTML='<td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td><div class="live-saved-actions"><button class="secondary" type="button" data-action="open">Open</button><button class="danger" type="button" data-action="delete">Delete</button></div></td>';
        tr.children[0].textContent=record.category?.label||categoryLabel(record.crypto_symbol);
        tr.children[1].textContent=record.question||record.slug||'-';
        tr.children[2].textContent=record.event_title||'-';
        tr.children[3].textContent=fmtPriceToBeat(record.target_price);
        tr.children[4].textContent=fmtPrice(record.final_crypto_price);
        const tags=[];
        if(record.unsuitable_contract)tags.push('Unsuitable');
        if(record.partial_contract)tags.push('Partial');
        tr.children[5].textContent=(record.winning_outcome||'-')+(tags.length?' - '+tags.join(', '):'');
        tr.children[6].textContent=record.history_count==null?'-':String(record.history_count);
        tr.children[7].textContent=fmtSavedDate(record.saved_at);
        tr.children[8].querySelector('[data-action="open"]').addEventListener('click',()=>openSaved(record.id));
        tr.children[8].querySelector('[data-action="delete"]').addEventListener('click',async()=>{
          if(!window.confirm('Delete this saved contract?'))return;
          const response=await fetch('/api/live-monitor/saved/'+encodeURIComponent(record.id),{method:'DELETE',headers:{Accept:'application/json'}});
          if(response.ok)loadSaved();
        });
        savedRows.appendChild(tr);
      }
    }catch(error){
      console.error('[live-monitor] saved load failed',error);
      renderSavedPagination({total:0});
      savedRows.innerHTML='<tr><td colspan="9" class="empty">Saved contracts failed: '+error.message+'</td></tr>';
    }
  };
  const stopSession=async controller=>{
    controller.stopButton.disabled=true;
    closeController(controller);
    try{
      if(controller.stopUrl)await fetch(controller.stopUrl,{method:'POST',headers:{Accept:'application/json'}});
      controller.quoteStatus.textContent='Stopped';
      controller.cryptoStatus.textContent='Stopped';
      setDot(controller.quoteDot,false);
      setDot(controller.cryptoDot,false);
      appendLog({level:'warning',source:'browser',message:'Tracking stopped'},controller);
      controller.stopButton.classList.add('hidden');
      loadSaved();
    }catch(error){
      console.error('[live-monitor] stop failed',error);
      appendLog({level:'error',source:'browser',message:error.message},controller);
      controller.stopButton.disabled=false;
    }
  };
  window.addEventListener('pagehide',()=>sessionControllers.forEach(closeController),{once:true});
  setInterval(()=>{
    sessionControllers.forEach(controller=>{
      if(!controller.source||!controller.lastPayloadAt)return;
      const age=(Date.now()-controller.lastPayloadAt)/1000;
      if(age>4&&!controller.staleWarningShown){
        controller.staleWarningShown=true;
        setDot(controller.quoteDot,false,true);
        appendLog({level:'error',source:'browser',message:'No SSE payload received for '+age.toFixed(1)+'s. Backend stream may be stuck.'},controller);
      }
    });
  },1000);
  form.addEventListener('submit',async event=>{
    event.preventDefault();setBusy(true);setStartStatus('Starting live monitor...');
    try{
      const response=await fetch('/api/live-monitors',{method:'POST',body:new FormData(form),headers:{Accept:'application/json'}});
      const payload=await response.json();
      if(!response.ok)throw new Error(payload.detail||'Unable to start live monitor');
      connectSession(payload);
      setStartStatus(sessionControllers.size+' active live monitor(s). Limit is 5.');
    }catch(error){
      console.error('[live-monitor] start failed',error);
      setStartStatus(error.message,'error');
      appendLog({level:'error',source:'browser',message:error.message});
    }finally{
      setBusy(false);
    }
  });
  if(savedRefresh)savedRefresh.addEventListener('click',loadSaved);
  if(savedPrev)savedPrev.addEventListener('click',()=>{if(savedPage>1){savedPage-=1;loadSaved()}});
  if(savedNext)savedNext.addEventListener('click',()=>{savedPage+=1;loadSaved()});
  if(savedCategory)savedCategory.addEventListener('change',()=>{selectedSavedCategory=savedCategory.value||'all';savedPage=1;loadSaved()});
  if(savedDeleteCategory)savedDeleteCategory.addEventListener('click',deleteSavedCategory);
  if(savedDetailClose)savedDetailClose.addEventListener('click',()=>{savedDetail.classList.add('hidden');resetSavedChart()});
  loadSaved();
  restoreActiveSession();
}
function initLocalDataTools(){
  const openButton=document.getElementById('local-data-open'),drawer=document.getElementById('local-data-drawer'),backdrop=document.getElementById('local-data-backdrop');
  if(!openButton||!drawer||!backdrop)return;
  const closeButton=document.getElementById('local-data-close'),status=document.getElementById('local-data-status'),list=document.getElementById('local-data-list'),refreshButton=document.getElementById('local-data-refresh'),downloadButton=document.getElementById('local-data-download'),tokenForm=document.getElementById('local-data-token-form'),tokenInput=document.getElementById('local-data-token');
  let loading=false;
  const setStatus=(text,mode='')=>{status.textContent=text;status.className='local-data-status '+mode};
  const open=()=>{drawer.classList.remove('hidden');backdrop.classList.remove('hidden');loadProfiles()};
  const close=()=>{drawer.classList.add('hidden');backdrop.classList.add('hidden')};
  const requestJson=async(url,options={})=>{
    const response=await fetch(url,{headers:{Accept:'application/json',...(options.headers||{})},...options});
    const payload=await response.json().catch(()=>({detail:'Request failed'}));
    if(!response.ok)throw new Error(payload.detail||'Request failed');
    return payload;
  };
  const renderProfiles=payload=>{
    list.textContent='';
    const profiles=payload.profiles||[];
    if(downloadButton)downloadButton.disabled=!payload.download_configured||loading;
    if(!profiles.length){list.innerHTML='<p class="muted">No data profiles found.</p>';return}
    for(const profile of profiles){
      const row=document.createElement('article');
      row.className='local-data-profile'+(profile.current?' current':'')+(!profile.exists?' missing':'');
      const meta=[profile.kind,profile.latest?'latest':null,profile.exists?'ready':'missing'].filter(Boolean).join(' / ');
      row.innerHTML='<div><strong></strong><small></small><code></code></div><button class="secondary" type="button"></button>';
      row.querySelector('strong').textContent=profile.label;
      row.querySelector('small').textContent=meta;
      row.querySelector('code').textContent=profile.path;
      const button=row.querySelector('button');
      button.textContent=profile.current?'Active':'Use';
      button.disabled=profile.current||!profile.exists||loading;
      button.addEventListener('click',()=>useProfile(profile.id));
      list.appendChild(row);
    }
  };
  const loadProfiles=async()=>{
    if(loading)return;
    setStatus('Loading');
    try{
      const payload=await requestJson('/api/local-data/profiles');
      renderProfiles(payload);
      setStatus('Current: '+payload.current_root,'ok');
      if(downloadButton&&!payload.download_configured)setStatus('Paste EXPORT_TOKEN or set RENDER_EXPORT_TOKEN in .env','warn');
    }catch(error){
      setStatus(error.message,'error');
    }
  };
  const downloadRender=async()=>{
    if(loading)return;
    loading=true;
    if(downloadButton){downloadButton.disabled=true;downloadButton.textContent='Downloading...'}
    setStatus('Downloading Render data');
    try{
      const payload=await requestJson('/api/local-data/download-render',{method:'POST'});
      renderProfiles(payload);
      setStatus('Downloaded '+payload.snapshot_id,'ok');
    }catch(error){
      setStatus(error.message,'error');
    }finally{
      loading=false;
      if(downloadButton){downloadButton.textContent='Download Render data';downloadButton.disabled=false}
      loadProfiles();
    }
  };
  const saveToken=async event=>{
    event.preventDefault();
    const token=(tokenInput?.value||'').trim();
    if(!token){setStatus('Paste EXPORT_TOKEN first','warn');return}
    const body=new FormData();body.append('render_export_token',token);
    setStatus('Saving token');
    try{
      await requestJson('/api/local-data/config',{method:'POST',body});
      if(tokenInput)tokenInput.value='';
      setStatus('Token saved locally','ok');
      loadProfiles();
    }catch(error){
      setStatus(error.message,'error');
    }
  };
  const useProfile=async(profileId)=>{
    if(loading)return;
    loading=true;
    setStatus('Switching data profile');
    const body=new FormData();body.append('profile_id',profileId);
    try{
      const payload=await requestJson('/api/local-data/use',{method:'POST',body});
      setStatus('Switched to '+payload.current_root+'. Reloading...','ok');
      setTimeout(()=>window.location.assign(payload.reload_url||'/login'),600);
    }catch(error){
      setStatus(error.message,'error');
      loading=false;
      loadProfiles();
    }
  };
  openButton.addEventListener('click',open);
  closeButton&&closeButton.addEventListener('click',close);
  backdrop.addEventListener('click',close);
  refreshButton&&refreshButton.addEventListener('click',loadProfiles);
  downloadButton&&downloadButton.addEventListener('click',downloadRender);
  tokenForm&&tokenForm.addEventListener('submit',saveToken);
}
window.addEventListener('pagehide',()=>{chartInstances.forEach(chart=>chart.destroy());chartInstances.clear()},{once:true});
document.addEventListener('DOMContentLoaded',()=>{initGroupGeneration();initCacheCalendar();initRetestProgress();initFavoriteForms();initLiveTimeRanges();initLiveAnalysis();initStrategyTradeContractCharts();initLiveRetestLoading();initLiveMonitor();initLocalDataTools()});
