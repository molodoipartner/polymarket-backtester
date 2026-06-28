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
function initLiveMonitor(){
  const panel=document.getElementById('live-monitor');if(!panel)return;
  const form=document.getElementById('live-monitor-form'),stopButton=document.getElementById('live-monitor-stop'),rows=document.getElementById('live-monitor-rows');
  const eventTitle=document.getElementById('live-event-title'),eventSlug=document.getElementById('live-event-slug'),cryptoPrice=document.getElementById('live-crypto-price'),cryptoStatus=document.getElementById('live-crypto-status'),quoteStatus=document.getElementById('live-quote-status'),samplePath=document.getElementById('live-sample-path');
  const quoteDot=document.getElementById('live-quote-dot'),cryptoDot=document.getElementById('live-crypto-dot'),liveLog=document.getElementById('live-monitor-log');
  const chartElement=document.getElementById('live-monitor-chart'),savedRows=document.getElementById('live-saved-rows'),savedRefresh=document.getElementById('live-saved-refresh'),savedCategory=document.getElementById('live-saved-category'),savedDeleteCategory=document.getElementById('live-saved-delete-category');
  const savedPrev=document.getElementById('live-saved-prev'),savedNext=document.getElementById('live-saved-next'),savedPageSummary=document.getElementById('live-saved-page-summary');
  const savedDetail=document.getElementById('live-saved-detail'),savedDetailClose=document.getElementById('live-saved-detail-close'),savedChartElement=document.getElementById('live-saved-chart');
  const savedDetailCategory=document.getElementById('live-saved-detail-category'),savedDetailTitle=document.getElementById('live-saved-detail-title'),savedDetailMeta=document.getElementById('live-saved-detail-meta');
  const savedPartialWarning=document.getElementById('live-saved-partial-warning'),savedGapList=document.getElementById('live-saved-gap-list');
  const savedTarget=document.getElementById('live-saved-target'),savedFinal=document.getElementById('live-saved-final'),savedOutcome=document.getElementById('live-saved-outcome'),savedHistoryCount=document.getElementById('live-saved-history-count');
  const savedCoverage=document.getElementById('live-saved-coverage'),savedAverageSpread=document.getElementById('live-saved-average-spread'),savedFinalDeviation=document.getElementById('live-saved-final-deviation'),savedYesRange=document.getElementById('live-saved-yes-range');
  const savedDisconnectCount=document.getElementById('live-saved-disconnect-count'),savedDisconnectedSeconds=document.getElementById('live-saved-disconnected-seconds');
  const savedCryptoDisconnectCount=document.getElementById('live-saved-crypto-disconnect-count'),savedCryptoDisconnectedSeconds=document.getElementById('live-saved-crypto-disconnected-seconds');
  let source=null,sessionId=null,stopUrl=null,chart=null,savedChart=null,selectedSavedCategory='all',savedPage=1;
  const savedPageSize=20;
  let lastPayloadAt=0,staleWarningShown=false;
  const closeSource=()=>{if(source){source.close();source=null}};
  const fmtCents=(value,empty='-')=>Number.isFinite(value)?(value*100).toFixed(2)+'c':empty;
  const fmtHistoryCents=value=>Number.isFinite(value)?Number(value).toFixed(2)+'c':'-';
  const fmtSize=value=>Number.isFinite(value)?Number(value).toLocaleString(undefined,{maximumFractionDigits:4}):'-';
  const fmtPrice=value=>Number.isFinite(value)?'$'+Number(value).toLocaleString(undefined,{maximumFractionDigits:3}):'-';
  const fmtPriceToBeat=value=>Number.isFinite(value)?'$'+Number(value).toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2}):'-';
  const fmtPct=value=>Number.isFinite(value)?Number(value).toFixed(2)+'%':'-';
  const fmtSeconds=value=>Number.isFinite(value)?Math.round(value)+'s':'-';
  const fmtSecondsPrecise=value=>Number.isFinite(value)?Number(value).toFixed(1).replace(/\\.0$/,'')+'s':'-';
  const fmtAge=timestamp=>timestamp?new Date(timestamp*1000).toLocaleTimeString():'Waiting';
  const fmtSavedDate=value=>value?new Date(value).toLocaleString():'-';
  const shortToken=value=>value?value.slice(0,8)+'...'+value.slice(-6):'-';
  const categoryLabel=value=>{
    const base=String(value||'').split('/')[0].toLowerCase();
    return ({btc:'Bitcoin',eth:'Ethereum',sol:'Solana',xrp:'XRP',doge:'Dogecoin'}[base])||base.toUpperCase()||'-';
  };
  const appendLog=(entry)=>{
    if(!entry||!liveLog)return;
    const level=entry.level||'info',message=entry.message||String(entry);
    const row=document.createElement('p');row.className=level;row.textContent=(entry.time||new Date().toLocaleTimeString())+' ['+(entry.source||'browser')+'] '+message;
    liveLog.appendChild(row);while(liveLog.childElementCount>160)liveLog.firstElementChild.remove();liveLog.scrollTop=liveLog.scrollHeight;
    if(level==='error')console.error('[live-monitor]',entry);
    else if(level==='warning')console.warn('[live-monitor]',entry);
    else console.info('[live-monitor]',entry);
  };
  const setDot=(element,connected,error=false)=>{if(!element)return;element.classList.toggle('connected',!!connected&&!error);element.classList.toggle('error',!!error)};
  const setBusy=busy=>{
    const button=form.querySelector('button[type="submit"]');
    button.disabled=busy;button.textContent=busy?'Starting...':'Start tracking';
  };
  const resetChart=()=>{if(chart){chart.destroy();chart=null}};
  const resetSavedChart=()=>{if(savedChart){savedChart.destroy();savedChart=null}};
  const ensureChart=()=>{
    if(chart||!chartElement)return chart;
    chart=replaceChart(chartElement,{type:'line',data:{datasets:[
      {label:'Crypto',data:[],borderColor:neon,backgroundColor:'#29f19c12',yAxisID:'crypto',pointRadius:0,tension:.2,parsing:false},
      {label:'Target',data:[],borderColor:'#8293aa',borderDash:[5,5],yAxisID:'crypto',pointRadius:0,tension:0,parsing:false},
      {label:'YES bid',data:[],borderColor:'#60a5fa',backgroundColor:'#60a5fa12',yAxisID:'contract',pointRadius:0,tension:.2,parsing:false},
      {label:'YES ask',data:[],borderColor:'#fbbf24',backgroundColor:'#fbbf2412',yAxisID:'contract',pointRadius:0,tension:.2,parsing:false}
    ]},options:{animation:false,responsive:true,maintainAspectRatio:false,interaction:{intersect:false,mode:'nearest'},plugins:{legend:{labels:{color:text}},tooltip:{callbacks:{title:items=>items.length?timeTick(items[0].parsed.x):''}}},scales:{x:timeScale,crypto:{type:'linear',position:'left',ticks:{color:text,callback:v=>'$'+v.toLocaleString()},grid:{color:grid}},contract:{type:'linear',position:'right',min:0,max:100,ticks:{color:text,callback:v=>v.toFixed(0)+'c'},grid:{drawOnChartArea:false,color:grid}}}}});
    return chart;
  };
  const updateChart=payload=>{
    const current=ensureChart();if(!current)return;
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
    const history=(record.history||[]).filter(row=>Number.isFinite(row.t));
    const series=key=>history.map(row=>({x:Number(row.t)*1000,y:Number.isFinite(row[key])?Number(row[key]):null}));
    savedChart=replaceChart(savedChartElement,{type:'line',data:{datasets:[
      {label:'Crypto',data:series('crypto_price'),borderColor:neon,backgroundColor:'#29f19c12',yAxisID:'crypto',pointRadius:0,tension:.2,parsing:false},
      {label:'Target',data:series('target_price'),borderColor:'#8293aa',borderDash:[5,5],yAxisID:'crypto',pointRadius:0,tension:0,parsing:false},
      {label:'YES bid',data:series('yes_bid'),borderColor:'#60a5fa',backgroundColor:'#60a5fa12',yAxisID:'contract',pointRadius:0,tension:.2,parsing:false},
      {label:'YES ask',data:series('yes_ask'),borderColor:'#fbbf24',backgroundColor:'#fbbf2412',yAxisID:'contract',pointRadius:0,tension:.2,parsing:false}
    ]},options:{animation:false,responsive:true,maintainAspectRatio:false,interaction:{intersect:false,mode:'nearest'},plugins:{legend:{labels:{color:text}},tooltip:{callbacks:{title:items=>items.length?timeTick(items[0].parsed.x):''}}},scales:{x:timeScale,crypto:{type:'linear',position:'left',ticks:{color:text,callback:v=>'$'+v.toLocaleString()},grid:{color:grid}},contract:{type:'linear',position:'right',min:0,max:100,ticks:{color:text,callback:v=>v.toFixed(0)+'c'},grid:{drawOnChartArea:false,color:grid}}}}});
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
          row.textContent=source+': '+range+'; duration '+fmtSecondsPrecise(gap.duration_seconds)+'.';
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
  const renderRows=payload=>{
    const quotes=payload.quotes||[];
    if(!quotes.length){rows.innerHTML='<tr><td colspan="8" class="empty">No token ids found.</td></tr>';return}
    rows.textContent='';
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
      rows.appendChild(tr);
    }
  };
  const render=payload=>{
    lastPayloadAt=Date.now();staleWarningShown=false;
    eventTitle.textContent=payload.event?.title||'Live session';
    eventSlug.textContent=payload.event?.slug||'-';
    cryptoPrice.textContent=fmtPrice(payload.crypto?.price);
    cryptoStatus.textContent=(payload.crypto?.symbol||'')+' - target '+fmtPriceToBeat(payload.crypto?.target_price)+' - '+(payload.status?.crypto||'waiting')+' - '+fmtAge(payload.crypto?.updated_at);
    quoteStatus.textContent=payload.status?.quotes||'waiting';
    const wsAge=payload.health?.polymarket?.last_message_age;
    const wsAgeText=Number.isFinite(wsAge)?' | ws age '+wsAge.toFixed(1)+'s':'';
    const restPolls=payload.health?.rest?.poll_count||0;
    samplePath.textContent=(payload.saved_path||payload.samples_path||'Saving to server_data/live_monitor')+' | tick '+(payload.tick_count||0)+wsAgeText+' | rest '+restPolls;
    if(payload.status?.last_error)samplePath.textContent='Last error: '+payload.status.last_error;
    setDot(quoteDot,!!payload.health?.polymarket?.connected,!!payload.status?.last_error&&payload.status?.quotes==='reconnecting');
    setDot(cryptoDot,!!payload.health?.crypto?.connected,!!payload.status?.last_error&&payload.status?.crypto==='reconnecting');
    renderRows(payload);updateChart(payload);
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
        tr.children[6].textContent=String(record.history_count??(record.history||[]).length);
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
  window.addEventListener('pagehide',closeSource,{once:true});
  setInterval(()=>{
    if(!source||!lastPayloadAt)return;
    const age=(Date.now()-lastPayloadAt)/1000;
    if(age>4&&!staleWarningShown){
      staleWarningShown=true;
      setDot(quoteDot,false,true);
      appendLog({level:'error',source:'browser',message:'No SSE payload received for '+age.toFixed(1)+'s. Backend stream may be stuck.'});
    }
  },1000);
  stopButton.addEventListener('click',async()=>{
    stopButton.disabled=true;closeSource();
    if(stopUrl){try{await fetch(stopUrl,{method:'POST',headers:{Accept:'application/json'}})}catch(error){console.error('[live-monitor] stop failed',error);appendLog({level:'error',source:'browser',message:error.message})}}
    quoteStatus.textContent='Stopped';cryptoStatus.textContent='Stopped';setDot(quoteDot,false);setDot(cryptoDot,false);stopButton.classList.add('hidden');stopButton.disabled=false;
    loadSaved();
  });
  form.addEventListener('submit',async event=>{
    event.preventDefault();closeSource();resetChart();setBusy(true);
    try{
      const response=await fetch('/api/live-monitors',{method:'POST',body:new FormData(form),headers:{Accept:'application/json'}});
      const payload=await response.json();
      if(!response.ok)throw new Error(payload.detail||'Unable to start live monitor');
      sessionId=payload.session_id;stopUrl=payload.stop_url;render(payload.snapshot);
      appendLog({level:'info',source:'browser',message:'SSE stream opening for session '+sessionId});
      stopButton.classList.remove('hidden');
      source=new EventSource(payload.events_url);
      const listener=message=>render(JSON.parse(message.data));
      source.addEventListener('snapshot',listener);source.addEventListener('quote',listener);source.addEventListener('crypto',listener);source.addEventListener('status',listener);source.addEventListener('tick',listener);source.addEventListener('rolled',message=>{const data=JSON.parse(message.data);render(data);appendLog({level:'info',source:'browser',message:'Rolled to '+(data.event?.slug||'next contract')})});
      source.addEventListener('saved',message=>{const data=JSON.parse(message.data);render(data);appendLog({level:'info',source:'browser',message:'Saved completed contract'});loadSaved()});
      source.addEventListener('log',message=>{const data=JSON.parse(message.data);render(data);appendLog(data.log)});
      source.addEventListener('ended',message=>{render(JSON.parse(message.data));appendLog({level:'warning',source:'browser',message:'Tracking ended'});closeSource();stopButton.classList.add('hidden');loadSaved()});
      source.onerror=error=>{quoteStatus.textContent='Stream reconnecting';console.error('[live-monitor] SSE error',error);appendLog({level:'error',source:'browser',message:'SSE stream interrupted; browser will retry.'})};
    }catch(error){
      console.error('[live-monitor] start failed',error);
      eventTitle.textContent='Unable to start';
      eventSlug.textContent=error.message;
      quoteStatus.textContent='Failed';
      setDot(quoteDot,false,true);setDot(cryptoDot,false,true);
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
}
window.addEventListener('pagehide',()=>{chartInstances.forEach(chart=>chart.destroy());chartInstances.clear()},{once:true});
document.addEventListener('DOMContentLoaded',()=>{initGroupGeneration();initCacheCalendar();initRetestProgress();initFavoriteForms();initLiveMonitor()});
