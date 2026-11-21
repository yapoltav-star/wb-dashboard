/* Ratings are evidence from our seller account. Scenarios never change WB reviews. */
let RW = {products: [], groups: {}}, RW_SELECTED = new Set(), rwPoll, rwLoaded = false;
const rwEsc = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
function rwScenario(stars, add, remove) {
  const counts = {};
  for(let star=1; star<=5; star++) counts[star] = Math.max(0, Number(stars[star] || 0) - Math.min(Number(stars[star] || 0), Number(remove[star] || 0)));
  counts[5] += add;
  const total = Object.values(counts).reduce((a,b)=>a+b,0);
  const sum = Object.entries(counts).reduce((n,[star,count])=>n+Number(star)*count,0);
  const mean = total ? sum/total : null;
  const shown = mean == null ? null : Math.round((mean+Number.EPSILON)*10)/10;
  const target = shown == null || shown>=5 ? null : Math.round((shown+0.05)*100)/100;
  const need = target == null ? null : Math.max(0, Math.ceil((target*total-sum-1e-9)/(5-target)));
  const exclusions = {};
  let deficit = target == null ? 0 : Math.max(0, target*total-sum);
  for(let star=1;star<=4 && deficit>1e-9;star++) {
    if(target<=star) continue;
    const count=Math.min(counts[star], Math.ceil((deficit-1e-9)/(target-star)));
    if(count) {exclusions[star]=count;deficit-=count*(target-star);}
  }
  return {counts,total,sum,mean,shown,target,need,exclusions};
}
async function loadRatingWorkspace(force=false) {
  clearTimeout(rwPoll);
  const status=document.getElementById('rwStatus');
  status.textContent='Загружаем рейтинги своего кабинета…';
  try {
    const response=await fetch(`${RAILWAY_URL}/api/rating-workspace${force?'?refresh_data=true':''}`);
    const data=await response.json();
    if(!response.ok) throw new Error(data.detail || 'Ошибка загрузки');
    if(data.products?.length) {
      RW=data;
      const select=document.getElementById('rwGroup'), previous=select.value;
      select.innerHTML='<option value="">Новый состав</option>'+Object.keys(RW.groups).map(name=>`<option value="${rwEsc(name)}">${rwEsc(name)}</option>`).join('');
      if(Object.hasOwn(RW.groups,previous)) select.value=previous;
      if(!rwLoaded) { rwLoaded=true; const first=Object.keys(RW.groups)[0]; if(first) {select.value=first;rwChooseGroup();} }
      drawRatingWorkspace();
    }
    status.textContent=data.error || (data.syncing ? 'Обновляем в фоне…' : `Получено ${data.updated_at || '—'} · отзывы синхронизированы: ${data.feedback_sync || 'дата неизвестна'}`);
    if(data.syncing) rwPoll=setTimeout(()=>loadRatingWorkspace(),3000);
  } catch(error) {status.textContent=error.message;}
}
async function rwUploadReport(input) {
  const file=input.files[0];if(!file) return;
  const status=document.getElementById('rwStatus'), button=document.getElementById('rwUploadButton');
  if(file.size>12*1024*1024) {status.textContent='Максимальный размер файла — 12 МБ';input.value='';return;}
  button.disabled=true;status.textContent='Проверяем и сохраняем отчёт WB…';
  clearTimeout(rwPoll);
  try {
    const form=new FormData();form.append('file',file);
    const response=await fetch(`${RAILWAY_URL}/api/rating-workspace/report`,{method:'POST',body:form});
    const result=await response.json();if(!response.ok) throw new Error(result.detail || 'Ошибка загрузки');
    RW_SELECTED=new Set(result.nm_ids.map(String));
    document.getElementById('rwGroup').value='';
    document.getElementById('rwGroupName').value='';
    document.getElementById('rwOnlySelected').checked=true;
    await loadRatingWorkspace();
    rwResetScenario();
    status.textContent=`Отчёт сохранён: ${result.count} товаров · ${result.period_from} — ${result.period_to}. Остальные товары не изменены.`;
  }catch(error){status.textContent=error.message;}
  finally {button.disabled=false;input.value='';}
}
function rwChooseGroup() {
  const name=document.getElementById('rwGroup').value;
  RW_SELECTED=new Set((RW.groups[name] || []).map(String));
  document.getElementById('rwGroupName').value=name;
  document.getElementById('rwOnlySelected').checked=Boolean(name);
  rwResetScenario();
}
function rwResetScenario() {
  ['rwAdd5','rwRemove1','rwRemove2','rwRemove3','rwRemove4'].forEach(id=>document.getElementById(id).value='0');
  drawRatingWorkspace();
}
function rwToggle(nm, checked) {
  if(checked) RW_SELECTED.add(nm); else RW_SELECTED.delete(nm);
  drawRatingWorkspace();
}
async function rwSaveGroup() {
  const name=document.getElementById('rwGroupName').value.trim();
  const status=document.getElementById('rwStatus');
  if(!name || !RW_SELECTED.size) {status.textContent='Введите название и выберите товары для склейки';return;}
  const groups={...RW.groups,[name]:[...RW_SELECTED]};
  try {
    const response=await fetch(`${RAILWAY_URL}/api/rating-workspace/groups`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({groups})});
    const data=await response.json();if(!response.ok) throw new Error(data.detail || 'Ошибка сохранения');
    RW.groups=data.groups;
    const select=document.getElementById('rwGroup');
    select.innerHTML='<option value="">Новый состав</option>'+Object.keys(RW.groups).map(g=>`<option value="${rwEsc(g)}">${rwEsc(g)}</option>`).join('');
    select.value=name;status.textContent='Склейка сохранена';
  }catch(error){status.textContent=error.message;}
}
function drawRatingWorkspace() {
  const selected=RW.products.filter(p=>RW_SELECTED.has(p.nm_id));
  const stars={1:0,2:0,3:0,4:0,5:0};
  selected.forEach(p=>{for(let star=1;star<=5;star++) stars[star]+=Number(p.stars[star] || 0);});
  const covered=selected.filter(p=>p.report);
  const periods=[...new Set(covered.map(p=>`${p.report.period_from} — ${p.report.period_to}`))];
  const complete=selected.length>0 && covered.length===selected.length && periods.length===1;
  const info=document.getElementById('rwReportInfo');
  const sumReport=key=>covered.reduce((sum,p)=>sum+Number(p.report[key] || 0),0);
  const sources=[...new Set(covered.map(p=>`${p.report.filename} · загружен ${p.report.uploaded_at}`))];
  info.textContent=!selected.length?'Выберите товары или загрузите отчёт.':!covered.length?'Для выбранных товаров ещё нет отчёта WB. Загрузите его для расчёта.':
    `Отчёт есть для ${covered.length} из ${selected.length} товаров. Период: ${periods.join('; ')}. Исключено: ${sumReport('excluded')}; не прошли модерацию: ${sumReport('moderation')}; отложено: ${sumReport('deferred')}. ${sources.join('; ')}${complete?'':' Для общего расчёта загрузите отчёты за одинаковый период для всех выбранных товаров.'}`;
  const removals={};
  for(let star=1;star<=4;star++) {
    const input=document.getElementById('rwRemove'+star); input.max=stars[star];
    input.value=String(Math.min(stars[star],Math.max(0,Math.floor(Number(input.value)||0))));
    removals[star]=Number(input.value);
  }
  const addInput=document.getElementById('rwAdd5');addInput.value=String(Math.max(0,Math.floor(Number(addInput.value)||0)));
  const base=rwScenario(complete?stars:{},0,{}), scenario=rwScenario(complete?stars:{},complete?Number(addInput.value):0,complete?removals:{});
  const format=n=>n==null?'—':n.toLocaleString('ru-RU',{maximumFractionDigits:5});
  const wb=selected.map(p=>p.wb_rating).filter(v=>v!=null);
  const unique=[...new Set(wb)];
  const actual=selected.length && wb.length===selected.length && unique.length===1 ? format(unique[0]) : unique.length>1 ? 'Различается' : 'Нет данных';
  document.getElementById('rwCards').innerHTML=[['Рейтинг WB по выбранным',actual],['Среднее по отчёту WB',format(base.mean)],['Прогноз сценария',format(scenario.mean)],['Оценок в расчёте',complete?format(base.total):'—']].map(([label,value])=>`<div class="card"><div class="card-l">${label}</div><div class="card-v">${value}</div></div>`).join('');
  document.getElementById('rwForecast').textContent=scenario.mean==null?'Для сценария нужны отчёты WB за одинаковый период по всем выбранным товарам.':scenario.need==null?'Расчётное округление: 5,0.':`Расчётное округление ${format(scenario.shown)} → ${format(Math.round((scenario.shown+0.1)*10)/10)}: нужно ещё ${scenario.need} оценок 5★ (порог среднего ${format(scenario.target)}).`;
  const exclusionPlan=Object.entries(scenario.exclusions).map(([star,count])=>`${count} × ${star}★`).join(' + ');
  if(exclusionPlan) document.getElementById('rwForecast').textContent+=` Альтернатива в сценарии: исключить ${exclusionPlan}.`;
  const q=document.getElementById('rwSearch').value.trim().toLowerCase();
  const only=document.getElementById('rwOnlySelected').checked;
  const products=RW.products.filter(p=>(!only || RW_SELECTED.has(p.nm_id)) && `${p.article} ${p.nm_id}`.toLowerCase().includes(q));
  document.getElementById('rwSelection').textContent=`В составе: ${selected.length} товаров`;
  const cells=p=>[5,4,3,2,1].map(star=>`<td class="num">${format(p.stars[star] || 0)}</td>`).join('');
  document.getElementById('rwTable').innerHTML=`<div style="overflow:auto"><table class="wb-prod-tbl"><thead><tr><th>В составе</th><th>Артикул</th><th>5★</th><th>4★</th><th>3★</th><th>2★</th><th>1★</th><th>Всего</th><th>Среднее</th><th>Рейтинг WB</th><th>Исключено</th><th>Не прошли модерацию</th><th>Отложено</th><th>Период отчёта</th></tr></thead><tbody><tr class="total-row"><td></td><td>Итого выбранных</td>${complete?cells({stars}):'<td>—</td>'.repeat(5)}<td>${complete?format(base.total):'—'}</td><td>${format(base.mean)}</td><td>${rwEsc(actual)}</td><td>${complete?format(sumReport('excluded')):'—'}</td><td>${complete?format(sumReport('moderation')):'—'}</td><td>${complete?format(sumReport('deferred')):'—'}</td><td>${rwEsc(periods.join('; '))}</td></tr>${products.map(p=>{const calc=rwScenario(p.report?p.stars:{},0,{});return `<tr><td><input type="checkbox" aria-label="Учесть ${rwEsc(p.article)}" ${RW_SELECTED.has(p.nm_id)?'checked':''} onchange="rwToggle('${p.nm_id}',this.checked)"></td><td>${rwEsc(p.article)}<div class="nmid">${p.nm_id}</div></td>${p.report?cells(p):'<td>—</td>'.repeat(5)}<td>${p.report?format(calc.total):'—'}</td><td>${format(calc.mean)}</td><td>${format(p.wb_rating)}</td><td>${p.report?format(p.report.excluded):'—'}</td><td>${p.report?format(p.report.moderation):'—'}</td><td>${p.report?format(p.report.deferred):'—'}</td><td>${p.report?rwEsc(p.report.period_from+' — '+p.report.period_to):'Нет отчёта'}</td></tr>`;}).join('')}</tbody></table></div>`;
}
