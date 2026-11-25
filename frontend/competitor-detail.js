/* WB comparison report: isolated, historical file data only. */
window.CompetitorDetail = (() => {
  let reports = [], index = 0, nm = null, tab = 'overview', page = 0, query = '', sheet = 0, sort = 'orders', direction = -1;
  const esc = s => String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const num = v => {
    if(v == null || v === '' || v === '-' || v === '—') return null;
    const n = Number(String(v).replace(/[\s\u00a0%]/g, '').replace(',', '.'));
    return Number.isFinite(n) ? n : null;
  };
  const fmt = v => num(v) === null ? '—' : num(v).toLocaleString('ru-RU', {maximumFractionDigits:2});
  const cell = v => v == null || v === '' ? '—' : typeof v === 'number' ? fmt(v) : esc(v);
  const pct = v => num(v) === null ? '—' : `${fmt(v)}%`;
  const delta = (a,b,percent=false) => {
    a=num(a); b=num(b); if(a===null || b===null) return '—';
    const d=a-b; return `${d>0?'+':''}${fmt(d)}${percent?' п. п.':''}`;
  };
  const period = text => String(text || '').replace(/(\d{4})-(\d{2})-(\d{2})/g, (_,y,m,d)=>`${d}.${m}.${y}`);
  const active = () => reports[index] || {};
  const article = () => active().report?.articles.find(a => a.nm_id === nm);
  const value = (obj, label) => obj?.[label];
  const isPercent = label => /%|CTR|Процент выкупа/.test(label);
  const table = (heads, rows) => `<div class="cd-scroll" tabindex="0" aria-label="Таблица данных"><table class="cd-table"><thead><tr>${heads.map(h=>`<th scope="col">${h}</th>`).join('')}</tr></thead><tbody>${rows.join('')}</tbody></table></div>`;
  const empty = text => `<p class="cd-empty">${text}</p>`;
  function overview(a) {
    const stages=[['Показы','Показы'],['Переходы','Переходы в карточку, шт'],['Корзины','Добавления в корзину, шт'],['Заказы','Заказы, шт'],['Выкупы','Выкупы, шт']];
    const bars=stages.map(([title,key])=>{
      const current=num(value(a.current,key)), prev=num(value(a.previous,key));
      const max=Math.max(current || 0,prev || 0,1);
      return `<div class="cd-stage"><div class="cd-stage-head"><span>${title}</span><strong>${fmt(current)}</strong><span class="cd-delta">${delta(current,prev)}</span></div><div class="cd-bar-pair" aria-hidden="true"><div style="width:${current===null?0:Math.max(0,current/max*100)}%"></div><div style="width:${prev===null?0:Math.max(0,prev/max*100)}%"></div></div><div class="cd-muted">Предыдущий период: ${fmt(prev)}</div></div>`;
    }).join('');
    const conversions=['CTR','Конверсия в корзину, %','Конверсия в заказ, %','Процент выкупа'];
    const conv=conversions.map(key=>`<tr><th scope="row">${esc(key.replace(', %',''))}</th><td>${pct(value(a.current,key))}</td><td>${pct(value(a.previous,key))}</td><td>${delta(value(a.current,key),value(a.previous,key),true)}</td></tr>`);
    const metrics=Object.entries(a.current).map(([key,v])=>`<tr><th scope="row">${esc(key)}</th><td>${cell(v)}${isPercent(key)&&num(v)!==null?'%':''}</td><td>${cell(a.previous[key])}${isPercent(key)&&num(a.previous[key])!==null?'%':''}</td><td>${delta(v,a.previous[key],isPercent(key))}</td></tr>`);
    return `<div class="cd-grid"><section><h3>Воронка за период</h3><p class="cd-muted">Текущий <span class="cd-key current"></span> и предыдущий <span class="cd-key"></span> · свой масштаб у каждого этапа</p>${bars}</section><section><h3>Конверсии</h3>${table(['Показатель','Текущий','Предыдущий','Δ'],conv)}<p class="cd-note">Конверсии приведены как в отчёте WB. Выкупы и заказы за период могут относиться к разным покупкам.</p><h3>Цена и доставка из файла</h3><dl class="cd-facts">${['Медианная цена покупателя, ₽','Минимальная цена со скидкой (по размерам), ₽','Среднее время доставки','Средняя позиция','Статус продвижения'].map(k=>`<div><dt>${esc(k)}</dt><dd>${cell(a.current[k])}</dd></div>`).join('')}</dl></section></div><details class="cd-all"><summary>Все показатели артикула · ${metrics.length}</summary>${table(['Показатель','Текущий период','Предыдущий период','Δ'],metrics)}</details>`;
  }
  function pager(count) {
    const pages=Math.max(1,Math.ceil(count/25));page=Math.min(page,pages-1);
    return `<div class="cd-pagination"><span role="status">${count?`${page*25+1}–${Math.min((page+1)*25,count)} из ${count}`:'0 строк'}</span><button data-action="prev" ${page===0?'disabled':''}>Назад</button><button data-action="next" ${page+1>=pages?'disabled':''}>Далее</button></div>`;
  }
  function search(a) {
    const cols=[['query_count','Запросов'],['avg_position','Позиция'],['card_opens','Переходы'],['cart_adds','Корзины'],['cart_conv','В корзину, %'],['orders','Заказы'],['order_conv','В заказ, %']];
    let rows=(active().report.queries || []).filter(q=>q.nm_id===a.nm_id && q.query.toLocaleLowerCase('ru').includes(query.toLocaleLowerCase('ru')));
    rows.sort((a,b)=>{
      const x=num(a[sort]),y=num(b[sort]);return x===null?y===null?0:1:y===null?-1:(x-y)*direction;
    });
    const nav=pager(rows.length);
    return `<div class="cd-tools"><label>Поисковый запрос<input data-control="query" value="${esc(query)}" type="search" placeholder="Найти запрос"></label><label>Сортировка<select data-control="sort">${cols.map(([k,v])=>`<option value="${k}" ${sort===k?'selected':''}>${v}</option>`).join('')}</select></label><button data-action="direction">${direction<0?'По убыванию ↓':'По возрастанию ↑'}</button></div><p class="cd-note">В ячейке: текущий период, ниже — предыдущий. Лист WB содержит запросы с заказами, а не весь поисковый трафик. «Запросов» — частота запроса, не показы товара.</p>${rows.length?table(['Запрос',...cols.map(x=>esc(x[1]))],rows.slice(page*25,(page+1)*25).map(r=>`<tr><th scope="row">${esc(r.query)}</th>${cols.map(([k])=>`<td><strong>${k.includes('conv')?pct(r[k]):fmt(r[k])}</strong><small>ранее ${k.includes('conv')?pct(r[k+'_prev']):fmt(r[k+'_prev'])}</small></td>`).join('')}</tr>`)):empty('По выбранному артикулу и фильтру запросы не найдены')}${nav}`;
  }
  function stocks(a) {
    const rows=(active().report.warehouses || []).filter(r=>r.nm_id===a.nm_id);
    if(!rows.length)return empty('В файле нет складских строк для этого артикула');
    const sum=key=>{const vals=rows.map(r=>num(r[key]));return vals.some(v=>v===null)?null:vals.reduce((a,b)=>a+b,0);};
    const totals=[['Остатки в отчёте',sum('stock')],['В пути к покупателям',sum('to_customer')],['Возвращаются',sum('from_customer')]];
    return `<p class="cd-note">Снимок из загруженного файла, не текущие остатки. «Склад WB РФ» — агрегат российских складов, а не отдельный город.</p><div class="cd-kpis">${totals.map(([l,v])=>`<div><span>${l}</span><strong>${fmt(v)}</strong></div>`).join('')}</div>${table(['Регион','Склад','Остаток, шт.','К покупателю','От покупателя','Заказов в день'],rows.map(r=>`<tr><td>${cell(r.region)}</td><th scope="row">${cell(r.warehouse)}</th><td>${fmt(r.stock)}</td><td>${fmt(r.to_customer)}</td><td>${fmt(r.from_customer)}</td><td>${fmt(r.daily_orders)}</td></tr>`))}`;
  }
  function source() {
    const sheets=active().report.sheets;sheet=Math.min(sheet,sheets.length-1);
    const selected=sheets[sheet];const nav=pager(selected.rows.length);
    return `<label class="cd-source-select">Лист отчёта<select data-control="sheet">${sheets.map((s,i)=>`<option value="${i}" ${i===sheet?'selected':''}>${esc(s.name)} · ${s.rows.length} строк</option>`).join('')}</select></label><p class="cd-note">Все строки и колонки файла, включая другие артикулы, предыдущий период, разницы и описание метрик. Пропуски отображаются как «—».</p>${table(['Строка',...Array.from({length:Math.max(0,...selected.rows.map(r=>r.length))},(_,i)=>String(i+1))],selected.rows.slice(page*25,(page+1)*25).map((r,i)=>`<tr><th scope="row">${page*25+i+1}</th>${r.map(v=>`<td>${cell(v)}</td>`).join('')}</tr>`))}${nav}`;
  }
  function render() {
    const host=document.getElementById('compReportDetail');if(!host)return;
    if(!reports.length){host.innerHTML='';return;}
    index=Math.max(0,Math.min(index,reports.length-1));
    const d=active(), report=d.report;
    if(!report?.articles.some(a=>a.nm_id===nm))nm=report?.articles[0]?.nm_id || null;
    const a=article();
    host.innerHTML=`<section class="cd-panel"><header class="cd-header"><div><p class="cd-eyebrow">Разбор конкурента</p><h2>${esc(a ? `${a.brand || 'Без бренда'}${d.store_name?' ('+d.store_name+')':''}` : 'Отчёт конкурента')}</h2><p class="cd-muted">${esc(d.source_filename || 'Загруженный отчёт')}</p></div><div class="cd-selects"><label>Файл<select data-control="report">${reports.map((r,i)=>`<option value="${i}" ${i===index?'selected':''}>${esc(r.label || r.source_filename || 'Отчёт '+r.id)}</option>`).join('')}</select></label>${a?`<label>Артикул<select data-control="article">${report.articles.map(x=>`<option value="${x.nm_id}" ${x.nm_id===nm?'selected':''}>${x.nm_id} · ${esc(x.name || x.brand || '')}</option>`).join('')}</select></label>`:''}</div></header>${!report?empty('В этой загрузке сохранены только основные показатели. Загрузите исходный Excel повторно, чтобы увидеть запросы, склады и предыдущий период.'):!a?empty('В отчёте не найдены артикулы'):`<div class="cd-product"><strong>${esc(a.name || a.nm_id)}</strong><span>${esc(period(report.periods.current) || 'Период не указан')} · сравнение: ${esc(period(report.periods.previous) || 'не указан')}</span></div><nav class="cd-tabs" aria-label="Разделы отчёта">${[['overview','Воронка и показатели'],['search','Поисковые запросы'],['stocks','Остатки и доставка'],['source','Все данные файла']].map(([k,v])=>`<button data-tab="${k}" aria-pressed="${k===tab}" class="${k===tab?'selected':''}">${v}</button>`).join('')}</nav><div class="cd-body" id="cdBody">${tab==='overview'?overview(a):tab==='search'?search(a):tab==='stocks'?stocks(a):source()}</div>`}</section>`;
    host.onclick=e=>{
      const el=e.target.closest('button');if(!el)return;
      if(el.dataset.tab){tab=el.dataset.tab;page=0;query='';render();host.querySelector(`[data-tab="${tab}"]`)?.focus();}
      else if(el.dataset.action){if(el.dataset.action==='prev')page--;else if(el.dataset.action==='next')page++;else direction*=-1;render();}
    };
    host.onchange=e=>{
      const control=e.target.dataset.control;
      if(control==='report'){index=Number(e.target.value);nm=null;sheet=0;query='';}
      if(control==='article'){nm=Number(e.target.value);query='';}
      if(control==='sheet')sheet=Number(e.target.value);
      if(control==='sort')sort=e.target.value;
      if(control && control!=='query'){page=0;render();}
    };
    host.oninput=e=>{
      if(e.target.dataset.control!=='query')return;
      query=e.target.value;page=0;const pos=e.target.selectionStart;render();
      const input=host.querySelector('[data-control="query"]');input.focus();try{input.setSelectionRange(pos,pos);}catch(_){}
    };
  }
  return {
    setReports(data){const oldId=active().id;reports=data;index=Math.max(0,reports.findIndex(r=>r.id===oldId));page=0;render();},
    mount:render,
    selectReport(sid){const i=reports.findIndex(r=>r.id===sid);if(i>=0){index=i;nm=null;page=0;query='';sheet=0;render();}},
    open(nmId,sid){const i=reports.findIndex(r=>(sid==null||r.id===sid)&&r.report?.articles.some(a=>a.nm_id===Number(nmId)));if(i>=0){index=i;nm=Number(nmId);page=0;tab='overview';render();document.getElementById('compReportDetail')?.scrollIntoView({block:'start'});}}
  };
})();
