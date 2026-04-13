/* ═══════════════════════════════════════════════════════════════════
   FCC BI — Dashboard module  (Jobs + Quotes, global filters)
   ═══════════════════════════════════════════════════════════════════ */

const BASE_URL = 'https://saasanalytic.fleetcostcare.com/api';

let monthChart    = null;
let statusChart   = null;
let perfChart     = null;
let qMonthChart   = null;
let qStatusChart  = null;
let qPerfChart    = null;

const _yardBreakoutCharts = {};

let activeDashTab = 'jobs'; // 'jobs' | 'quotes' | 'equipment' | 'pm'

const TEAL  = '#00BFA5';
const NAVY  = '#003366';
const AMBER = '#f59e0b';
const RED   = '#ef4444';
const GREEN = '#16a34a';

const PERF_COLORS = [
  '#00BFA5','#003366','#f59e0b','#6366f1','#ec4899',
  '#14b8a6','#8b5cf6','#f97316','#06b6d4','#84cc16'
];

/* ── Boot ─────────────────────────────────────────────────────────── */
async function initDashboard() {
  // Default: last 90 days
  const now  = new Date();
  const ago  = new Date(now); ago.setDate(ago.getDate() - 90);
  const fromEl = document.getElementById('dash-date-from');
  const toEl   = document.getElementById('dash-date-to');
  if (fromEl && !fromEl.value) fromEl.value = ago.toISOString().slice(0,10);
  if (toEl   && !toEl.value)   toEl.value   = now.toISOString().slice(0,10);

  await Promise.all([ loadYardFilter(), refreshDashboard() ]);
  // Finish Jobs is the default sub-tab — load it
  const df = document.getElementById('dash-date-from')?.value || '';
  const dt = document.getElementById('dash-date-to')?.value || '';
  const dy = getSelectedYards();
  loadFinishJobs(df, dt, dy);
  _setupInfoButtons();
}

/* ── Yard multi-select ──────────────────────────────────────────── */
let _allYardValues = [];

async function loadYardFilter() {
  try {
    const r = await fetch(`${BASE_URL}/bi/segment-values?datasetName=jobs_profit_loss&segmentName=Yard&limit=100`);
    const j = await r.json();
    const vals = (j.data?.values || []).map(v => v.value || v);
    _allYardValues = vals;
    const container = document.getElementById('yard-ms-options');
    if (!container) return;
    container.innerHTML = vals.map(v =>
      `<label class="yard-ms-option">
        <input type="checkbox" value="${v}" checked onchange="updateYardLabel()">
        ${v}
      </label>`
    ).join('');
    updateYardLabel();
    lucide.createIcons();
  } catch(e) {}
}

function toggleYardDropdown() {
  document.getElementById('yard-ms-dropdown').classList.toggle('open');
}

// Close dropdown when clicking outside
document.addEventListener('click', function(e) {
  const wrap = document.getElementById('yard-ms-wrap');
  if (wrap && !wrap.contains(e.target)) {
    document.getElementById('yard-ms-dropdown')?.classList.remove('open');
  }
});

function getSelectedYards() {
  return Array.from(document.querySelectorAll('#yard-ms-options input[type=checkbox]:checked'))
    .map(cb => cb.value);
}

function updateYardLabel() {
  const selected = getSelectedYards();
  const total    = _allYardValues.length;
  const label    = document.getElementById('yard-ms-label');
  if (!label) return;
  if (selected.length === 0)          label.textContent = 'No Yards';
  else if (selected.length === total) label.textContent = 'All Yards';
  else if (selected.length === 1)     label.textContent = selected[0];
  else                                label.textContent = `${selected.length} Yards`;
}

function yardSelectAll() {
  document.querySelectorAll('#yard-ms-options input[type=checkbox]').forEach(cb => cb.checked = true);
  updateYardLabel();
}

function yardClearAll() {
  document.querySelectorAll('#yard-ms-options input[type=checkbox]').forEach(cb => cb.checked = false);
  updateYardLabel();
}

function buildYardFilter(yards) {
  if (!yards || yards.length === 0) return [];
  if (yards.length === _allYardValues.length && _allYardValues.length > 0) return []; // all selected = no filter
  if (yards.length === 1) return [{ segmentName:'Yard', operator:'eq', value: yards[0] }];
  return [{ segmentName:'Yard', operator:'in', value: yards }];
}


/* ── Tab switching ────────────────────────────────────────────────── */
function switchDashTab(tab) {
  activeDashTab = tab;
  ['jobs','quotes','equipment','pm'].forEach(t => {
    document.getElementById(`dash-tab-${t}`)?.classList.toggle('active', t === tab);
    document.getElementById(`dash-${t}-content`).style.display = t === tab ? '' : 'none';
  });
  // Lazy-load equipment / pm on first visit
  const dateFrom = document.getElementById('dash-date-from')?.value || '';
  const dateTo   = document.getElementById('dash-date-to')?.value   || '';
  const yards    = getSelectedYards();
  if (tab === 'equipment' && !equipLoaded[activeEquipTab]) {
    equipLoaded[activeEquipTab] = true;
    loadEquipSubTab(activeEquipTab, dateFrom, dateTo, yards);
  }
  if (tab === 'pm' && !pmLoaded) {
    pmLoaded = true;
    loadPM(dateFrom, dateTo, yards);
  }
}

/* ── Master refresh ───────────────────────────────────────────────── */
async function refreshDashboard() {
  const dateFrom = document.getElementById('dash-date-from')?.value || '';
  const dateTo   = document.getElementById('dash-date-to')?.value   || '';
  const yards    = getSelectedYards();

  // Reset lazy-load flags so sub-tabs re-fetch with new filters
  JOB_TABS.forEach(t   => { jobLoaded[t]   = false; });
  QUOTE_TABS.forEach(t => { quoteLoaded[t] = false; });
  EQUIP_TABS.forEach(t => { equipLoaded[t] = false; });
  pmLoaded = false;

  setDashLoading(true);
  try {
    // Always reload the Quotes Summary panel; Jobs default is Finish Jobs
    await Promise.all([
      loadQuoteKpis(dateFrom, dateTo, yards),
      loadQuoteMonthChart(dateFrom, dateTo, yards),
      loadQuoteStatusChart(dateFrom, dateTo, yards),
      loadQuoteSalespersonTable(dateFrom, dateTo, yards),
      loadYardBreakoutQSummary(dateFrom, dateTo, yards),
    ]);
  } catch(e) { console.error('Dashboard refresh error:', e); }
  setDashLoading(false);

  // Mark the always-loaded tabs so they don't re-fire on tab switch
  jobLoaded['finish']   = true;
  quoteLoaded['summary'] = true;

  // Re-load whichever sub-tab is currently visible (if not the defaults)
  if (activeJobTab !== 'finish') {
    jobLoaded[activeJobTab] = true;
    loadJobSubTab(activeJobTab, dateFrom, dateTo, yards);
  }
  if (activeQuoteTab !== 'summary') {
    quoteLoaded[activeQuoteTab] = true;
    loadQuoteSubTab(activeQuoteTab, dateFrom, dateTo, yards);
  }
  if (activeDashTab === 'equipment') {
    equipLoaded[activeEquipTab] = true;
    loadEquipSubTab(activeEquipTab, dateFrom, dateTo, yards);
  }
  if (activeDashTab === 'pm') {
    pmLoaded = true;
    loadPM(dateFrom, dateTo, yards);
  }
}

/* ── Filter builders ──────────────────────────────────────────────── */
function buildJobFilters(dateFrom, dateTo, yards) {
  const f = [];
  if (dateFrom && dateTo) f.push({ segmentName:'JobStartDate', operator:'between', value: dateFrom, secondValue: dateTo });
  else if (dateFrom)      f.push({ segmentName:'JobStartDate', operator:'gte', value: dateFrom });
  else if (dateTo)        f.push({ segmentName:'JobStartDate', operator:'lte', value: dateTo });
  f.push(...buildYardFilter(yards));
  return f;
}

function buildQuoteFilters(dateFrom, dateTo, yards) {
  const f = [];
  if (dateFrom && dateTo) f.push({ segmentName:'QuoteDate', operator:'between', value: dateFrom, secondValue: dateTo });
  else if (dateFrom)      f.push({ segmentName:'QuoteDate', operator:'gte', value: dateFrom });
  else if (dateTo)        f.push({ segmentName:'QuoteDate', operator:'lte', value: dateTo });
  f.push(...buildYardFilter(yards));
  return f;
}

/* ══════════════════════════════════════════════════════════════════
   JOBS DASHBOARD
   ══════════════════════════════════════════════════════════════════ */

/* ── Job KPI Tiles ────────────────────────────────────────────────── */
async function loadJobKpis(dateFrom, dateTo, yards) {
  const body = {
    datasetName: 'jobs_profit_loss',
    metrics: [
      { metricName:'JobRevenue',    aggregation:'SUM',   alias:'JobRevenue'    },
      { metricName:'TotalExpenses', aggregation:'SUM',   alias:'TotalExpenses' },
      { metricName:'Profit',        aggregation:'SUM',   alias:'Profit'        },
      { metricName:'LaborHours',    aggregation:'SUM',   alias:'LaborHours'    },
      { metricName:'JobCount',      aggregation:'COUNT', alias:'JobCount'      },
    ],
    filters: buildJobFilters(dateFrom, dateTo, yards)
  };
  try {
    _reg('dash-kpi-row','Jobs P/L — KPIs','/bi/kpis',body);
    const r = await fetch(`${BASE_URL}/bi/kpis`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body) });
    const j = await r.json();
    const kpis = j.data?.kpis || [];
    const kv = {};
    kpis.forEach(k => { kv[k.name] = k.value; });
    const rev  = parseFloat(kv.JobRevenue    || 0);
    const exp  = parseFloat(kv.TotalExpenses || 0);
    const prof = parseFloat(kv.Profit        || 0);
    const hrs  = parseFloat(kv.LaborHours    || 0);
    const jobs = parseFloat(kv.JobCount      || 0);
    const pct  = rev > 0 ? (prof / rev * 100) : 0;
    const tiles = [
      { label:'Job Revenue',    value: dFmtCur(rev),          color: NAVY,                    icon:'dollar-sign'   },
      { label:'Total Expenses', value: dFmtCur(exp),          color: '#dc2626',               icon:'trending-down' },
      { label:'Profit',         value: dFmtCur(prof),         color: prof>=0?GREEN:RED,       icon:'trending-up'   },
      { label:'Labor Hours',    value: dFmtNum(hrs,0)+' hrs', color: '#7c3aed',               icon:'clock'         },
      { label:'Job Count',      value: dFmtNum(jobs,0),       color: '#0369a1',               icon:'briefcase'     },
      { label:'Profit %',       value: pct.toFixed(1)+'%',    color: pct>=50?GREEN:AMBER,     icon:'percent'       },
    ];
    document.getElementById('dash-kpi-row').innerHTML = tiles.map(t => `
      <div class="dash-kpi-tile">
        <div class="dkt-icon" style="background:${t.color}20;color:${t.color}"><i data-lucide="${t.icon}" style="width:18px;height:18px;stroke-width:2"></i></div>
        <div class="dkt-body">
          <div class="dkt-label">${t.label}</div>
          <div class="dkt-value" style="color:${t.color}">${t.value}</div>
        </div>
      </div>`).join('');
    lucide.createIcons();
  } catch(e) {
    document.getElementById('dash-kpi-row').innerHTML = `<div style="color:var(--text-muted);font-size:13px;grid-column:1/-1;padding:8px">Could not load KPIs</div>`;
  }
}

/* ── Month Chart ──────────────────────────────────────────────────── */
async function loadMonthChart(dateFrom, dateTo, yards) {
  const body = {
    datasetName: 'jobs_profit_loss',
    groupBySegments: ['JobYear','JobMonth'],
    metrics: [
      { metricName:'JobRevenue',    aggregation:'SUM', alias:'JobRevenue'    },
      { metricName:'TotalExpenses', aggregation:'SUM', alias:'TotalExpenses' },
      { metricName:'Profit',        aggregation:'SUM', alias:'Profit'        },
    ],
    filters: buildJobFilters(dateFrom, dateTo, yards),
    orderBy: [{ field:'JobYear', direction:'ASC' },{ field:'JobMonth', direction:'ASC' }],
    limit: 60
  };
  const ctx = document.getElementById('month-chart');
  if (!ctx) return;
  if (monthChart) { monthChart.destroy(); monthChart = null; }

  try {
    _reg('chart-card-1','Jobs by Month — Revenue/Expenses/Profit','/bi/query',body);
    const r = await fetch(`${BASE_URL}/bi/query`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body) });
    const j = await r.json();
    // Filter out null/bogus years (before 2010)
    const rows = (j.data?.data || []).filter(r => r.JobYear && r.JobYear >= 2010);

    if (!rows.length) { showChartEmpty(ctx, 'No job data for this period'); return; }
    ctx.style.display = '';
    const existing = ctx.parentElement.querySelector('.chart-empty');
    if (existing) existing.remove();

    const MN = ['','Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
    const labels   = rows.map(r => `${MN[+r.JobMonth]||r.JobMonth} ${r.JobYear}`);
    const revenue  = rows.map(r => parseFloat(r.JobRevenue    || 0));
    const expenses = rows.map(r => parseFloat(r.TotalExpenses || 0));
    const profit   = rows.map(r => parseFloat(r.Profit        || 0));

    monthChart = new Chart(ctx, {
      type: 'bar',
      data: {
        labels,
        datasets: [
          { label:'Revenue',  data: revenue,  backgroundColor: TEAL+'bb', borderColor: TEAL,  borderWidth:1, order:2 },
          { label:'Expenses', data: expenses, backgroundColor: NAVY+'88', borderColor: NAVY,  borderWidth:1, order:3 },
          { label:'Profit',   data: profit,   type:'line', borderColor: AMBER, backgroundColor: AMBER+'22', borderWidth:2.5, pointRadius:3, pointBackgroundColor: AMBER, tension:.35, fill:false, order:1 },
        ]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        interaction: { mode:'index', intersect:false },
        plugins: {
          legend: { position:'bottom', labels:{ boxWidth:12, font:{size:11} } },
          tooltip: { callbacks:{ label: c => ` ${c.dataset.label}: ${dFmtCur(c.raw)}` } }
        },
        scales: {
          x: { grid:{ display:false }, ticks:{ font:{size:10}, maxRotation:45, maxTicksLimit:12 } },
          y: { grid:{ color:'#e2e8f0' }, ticks:{ callback: v => dFmtCurShort(v), font:{size:10} }, beginAtZero:true }
        }
      }
    });
  } catch(e) { showChartEmpty(ctx, 'Error loading chart'); }
}

/* ── Salesperson Performance ──────────────────────────────────────── */
async function loadSalespersonPerf(dateFrom, dateTo, yards) {
  const body = {
    datasetName: 'jobs_profit_loss',
    groupBySegments: ['SalesPerson'],
    metrics: [
      { metricName:'JobRevenue',    aggregation:'SUM',   alias:'JobRevenue'    },
      { metricName:'TotalExpenses', aggregation:'SUM',   alias:'TotalExpenses' },
      { metricName:'Profit',        aggregation:'SUM',   alias:'Profit'        },
      { metricName:'JobCount',      aggregation:'COUNT', alias:'JobCount'      },
    ],
    filters: buildJobFilters(dateFrom, dateTo, yards),
    orderBy: [{ field:'Profit', direction:'DESC' }],
    limit: 20
  };

  const tbody = document.getElementById('perf-table-body');
  const ctx   = document.getElementById('perf-chart');

  try {
    _reg('chart-card-3','Salesperson Performance — Revenue/Profit','/bi/query',body);
    _reg('chart-card-4','Salesperson Performance — Table','/bi/query',body);
    const r = await fetch(`${BASE_URL}/bi/query`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body) });
    const j = await r.json();
    const rows = j.data?.data || [];

    if (!rows.length) {
      tbody.innerHTML = '<tr><td colspan="6" style="text-align:center;color:var(--text-muted);padding:20px">No data for this period</td></tr>';
      if (ctx) showChartEmpty(ctx, 'No salesperson data');
      return;
    }

    const maxProfit = Math.max(...rows.map(r => Math.abs(parseFloat(r.Profit||0))));

    tbody.innerHTML = rows.map((row, i) => {
      const rev  = parseFloat(row.JobRevenue    || 0);
      const exp  = parseFloat(row.TotalExpenses || 0);
      const prof = parseFloat(row.Profit        || 0);
      const pct  = rev > 0 ? (prof / rev * 100) : 0;
      const barW = maxProfit > 0 ? Math.max(0, Math.min(100, Math.abs(prof) / maxProfit * 100)) : 0;
      const color = PERF_COLORS[i % PERF_COLORS.length];
      return `
        <tr>
          <td><div style="display:flex;align-items:center;gap:8px">
            <div style="width:8px;height:8px;border-radius:50%;background:${color};flex-shrink:0"></div>
            <span style="font-weight:600">${row.SalesPerson||'—'}</span>
          </div></td>
          <td class="num-cell">${dFmtCur(rev)}</td>
          <td class="num-cell" style="color:#dc2626">${dFmtCur(exp)}</td>
          <td class="num-cell ${prof>=0?'positive':'negative'}">${dFmtCur(prof)}</td>
          <td><div class="progress-cell">
            <div class="progress-bar-wrap"><div class="progress-bar-fill" style="width:${barW}%;background:${color}"></div></div>
            <span class="progress-val ${pct<0?'neg':''}">${pct.toFixed(1)}%</span>
          </div></td>
          <td class="num-cell">${dFmtNum(parseFloat(row.JobCount||0),0)}</td>
        </tr>`;
    }).join('');

    if (ctx) {
      if (perfChart) { perfChart.destroy(); perfChart = null; }
      ctx.style.display = '';
      const existing = ctx.parentElement.querySelector('.chart-empty');
      if (existing) existing.remove();
      const labels  = rows.map(r => r.SalesPerson||'—');
      const profits = rows.map(r => parseFloat(r.Profit||0));
      const revs    = rows.map(r => parseFloat(r.JobRevenue||0));
      perfChart = new Chart(ctx, {
        type: 'bar',
        data: {
          labels,
          datasets: [
            { label:'Revenue', data: revs,    backgroundColor: TEAL+'99', borderColor: TEAL, borderWidth:1 },
            { label:'Profit',  data: profits, backgroundColor: NAVY+'cc', borderColor: NAVY, borderWidth:1 },
          ]
        },
        options: {
          indexAxis: 'y',
          responsive: true, maintainAspectRatio: false,
          plugins: {
            legend: { position:'bottom', labels:{ boxWidth:12, font:{size:11} } },
            tooltip: { callbacks:{ label: c => ` ${c.dataset.label}: ${dFmtCur(c.raw)}` } }
          },
          scales: {
            x: { grid:{ color:'#e2e8f0' }, ticks:{ callback: v => dFmtCurShort(v), font:{size:10} }, beginAtZero:true },
            y: { grid:{ display:false }, ticks:{ font:{size:11} } }
          }
        }
      });
    }
  } catch(e) {
    tbody.innerHTML = '<tr><td colspan="6" style="text-align:center;color:var(--red);padding:20px">Error loading data</td></tr>';
    if (ctx) showChartEmpty(ctx, 'Error loading chart');
  }
}

/* ── Jobs Status Breakdown ────────────────────────────────────────── */
async function loadStatusBreakdown(dateFrom, dateTo, yards) {
  const body = {
    datasetName: 'jobs_profit_loss',
    groupBySegments: ['JobStatus'],
    metrics: [
      { metricName:'JobCount',   aggregation:'COUNT', alias:'JobCount'   },
      { metricName:'JobRevenue', aggregation:'SUM',   alias:'JobRevenue' },
    ],
    filters: buildJobFilters(dateFrom, dateTo, yards),
    orderBy: [{ field:'JobCount', direction:'DESC' }],
    limit: 20
  };

  const ctx = document.getElementById('status-chart');
  if (!ctx) return;
  if (statusChart) { statusChart.destroy(); statusChart = null; }

  try {
    _reg('chart-card-2','Jobs by Status — Count/Revenue','/bi/query',body);
    const r = await fetch(`${BASE_URL}/bi/query`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body) });
    const j = await r.json();
    const rows = j.data?.data || [];

    if (!rows.length) { showChartEmpty(ctx, 'No status data'); document.getElementById('status-chips').innerHTML=''; return; }
    ctx.style.display = '';
    const existing = ctx.parentElement.querySelector('.chart-empty');
    if (existing) existing.remove();

    const labels = rows.map(r => r.JobStatus||'Unknown');
    const counts  = rows.map(r => parseFloat(r.JobCount||0));
    const colors  = rows.map((_,i) => PERF_COLORS[i % PERF_COLORS.length]);

    statusChart = new Chart(ctx, {
      type: 'doughnut',
      data: { labels, datasets:[{ data: counts, backgroundColor: colors, borderWidth:2, borderColor:'#fff', hoverOffset:6 }] },
      options: {
        responsive: true, maintainAspectRatio: false, cutout:'62%',
        plugins: {
          legend: { position:'right', labels:{ boxWidth:10, font:{size:11}, padding:12 } },
          tooltip: { callbacks:{ label: c => {
            const row=rows[c.dataIndex];
            return ` ${c.label}: ${dFmtNum(c.raw,0)} jobs · ${dFmtCur(parseFloat(row?.JobRevenue||0))}`;
          }}}
        }
      }
    });

    const total = counts.reduce((a,b)=>a+b,0);
    document.getElementById('status-chips').innerHTML = rows.slice(0,6).map((row,i)=>`
      <div class="status-chip">
        <div style="width:8px;height:8px;border-radius:2px;background:${colors[i]};flex-shrink:0"></div>
        <span>${row.JobStatus||'Unknown'}</span>
        <strong>${dFmtNum(parseFloat(row.JobCount||0),0)}</strong>
        <span style="color:var(--text-muted);font-size:10px">${total>0?(parseFloat(row.JobCount||0)/total*100).toFixed(0)+'%':''}</span>
      </div>`).join('');
  } catch(e) { showChartEmpty(ctx, 'Error loading chart'); }
}

/* ══════════════════════════════════════════════════════════════════
   QUOTES DASHBOARD
   ══════════════════════════════════════════════════════════════════ */

/* ── Quote KPI Tiles ──────────────────────────────────────────────── */
async function loadQuoteKpis(dateFrom, dateTo, yards) {
  const body = {
    datasetName: 'Quotes_By_Status',
    metrics: [
      { metricName:'TotalQuotes',   aggregation:'COUNT', alias:'TotalQuotes'   },
      { metricName:'TotalQuoteMax', aggregation:'SUM',   alias:'TotalQuoteMax' },
      { metricName:'TotalQuoteMin', aggregation:'SUM',   alias:'TotalQuoteMin' },
    ],
    filters: buildQuoteFilters(dateFrom, dateTo, yards)
  };
  try {
    _reg('quote-kpi-row','Quotes Summary — KPIs','/bi/kpis',body);
    const r = await fetch(`${BASE_URL}/bi/kpis`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body) });
    const j = await r.json();
    const kpis = j.data?.kpis || [];
    const kv = {};
    kpis.forEach(k => { kv[k.name] = k.value; });
    const total  = parseFloat(kv.TotalQuotes   || 0);
    const maxAmt = parseFloat(kv.TotalQuoteMax || 0);
    const minAmt = parseFloat(kv.TotalQuoteMin || 0);
    const tiles = [
      { label:'Total Quotes',    value: dFmtNum(total,0),   color: NAVY,    icon:'file-text'   },
      { label:'Quote Value (Max)', value: dFmtCur(maxAmt), color: GREEN,   icon:'trending-up' },
      { label:'Quote Value (Min)', value: dFmtCur(minAmt), color: '#0369a1', icon:'dollar-sign' },
    ];
    document.getElementById('quote-kpi-row').innerHTML = tiles.map(t => `
      <div class="dash-kpi-tile">
        <div class="dkt-icon" style="background:${t.color}20;color:${t.color}"><i data-lucide="${t.icon}" style="width:18px;height:18px;stroke-width:2"></i></div>
        <div class="dkt-body">
          <div class="dkt-label">${t.label}</div>
          <div class="dkt-value" style="color:${t.color}">${t.value}</div>
        </div>
      </div>`).join('');
    lucide.createIcons();
  } catch(e) {
    document.getElementById('quote-kpi-row').innerHTML = `<div style="color:var(--text-muted);font-size:13px;grid-column:1/-1;padding:8px">Could not load KPIs</div>`;
  }
}

/* ── Quote Amount by Month ────────────────────────────────────────── */
async function loadQuoteMonthChart(dateFrom, dateTo, yards) {
  const body = {
    datasetName: 'Quote_Revenue_Forecast',
    groupBySegments: ['Year','Month'],
    metrics: [
      { metricName:'TotalQuoteAmount', aggregation:'SUM',   alias:'TotalQuoteAmount' },
      { metricName:'QuoteCount',       aggregation:'COUNT', alias:'QuoteCount'       },
    ],
    filters: (() => {
      // Quote_Revenue_Forecast uses QuoteDate for filtering
      const f = [];
      if (dateFrom && dateTo) f.push({ segmentName:'QuoteDate', operator:'between', value: dateFrom, secondValue: dateTo });
      return f;
    })(),
    orderBy: [{ field:'Year', direction:'ASC' },{ field:'Month', direction:'ASC' }],
    limit: 60
  };
  const ctx = document.getElementById('q-month-chart');
  if (!ctx) return;
  if (qMonthChart) { qMonthChart.destroy(); qMonthChart = null; }

  try {
    const r = await fetch(`${BASE_URL}/bi/query`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body) });
    const j = await r.json();
    const rows = (j.data?.data || []).filter(r => r.Year && r.Year >= 2019);

    if (!rows.length) { showChartEmpty(ctx, 'No quote data for this period'); return; }
    ctx.style.display = '';
    const existing = ctx.parentElement.querySelector('.chart-empty');
    if (existing) existing.remove();

    const MN = ['','Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
    const labels  = rows.map(r => `${MN[+r.Month]||r.Month} ${r.Year}`);
    const amounts = rows.map(r => parseFloat(r.TotalQuoteAmount || 0));
    const counts  = rows.map(r => parseFloat(r.QuoteCount || 0));

    qMonthChart = new Chart(ctx, {
      type: 'bar',
      data: {
        labels,
        datasets: [
          { label:'Quote Value', data: amounts, backgroundColor: TEAL+'bb', borderColor: TEAL, borderWidth:1, order:2, yAxisID:'y' },
          { label:'# Quotes',   data: counts,  type:'line', borderColor: AMBER, backgroundColor: AMBER+'22', borderWidth:2.5, pointRadius:3, pointBackgroundColor: AMBER, tension:.35, fill:false, order:1, yAxisID:'y2' },
        ]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        interaction: { mode:'index', intersect:false },
        plugins: {
          legend: { position:'bottom', labels:{ boxWidth:12, font:{size:11} } },
          tooltip: { callbacks:{ label: c => c.dataset.yAxisID==='y2' ? ` ${c.dataset.label}: ${dFmtNum(c.raw,0)}` : ` ${c.dataset.label}: ${dFmtCur(c.raw)}` } }
        },
        scales: {
          x:  { grid:{ display:false }, ticks:{ font:{size:10}, maxRotation:45, maxTicksLimit:12 } },
          y:  { grid:{ color:'#e2e8f0' }, ticks:{ callback: v => dFmtCurShort(v), font:{size:10} }, beginAtZero:true, position:'left' },
          y2: { grid:{ display:false }, ticks:{ font:{size:10} }, beginAtZero:true, position:'right' }
        }
      }
    });
  } catch(e) { showChartEmpty(ctx, 'Error loading chart'); }
}

/* ── Quotes by Status ─────────────────────────────────────────────── */
async function loadQuoteStatusChart(dateFrom, dateTo, yards) {
  const body = {
    datasetName: 'Quotes_By_Status',
    groupBySegments: ['QuoteStatus'],
    metrics: [
      { metricName:'TotalQuotes',   aggregation:'COUNT', alias:'TotalQuotes'   },
      { metricName:'TotalQuoteMax', aggregation:'SUM',   alias:'TotalQuoteMax' },
    ],
    filters: buildQuoteFilters(dateFrom, dateTo, yards),
    orderBy: [{ field:'TotalQuotes', direction:'DESC' }],
    limit: 15
  };
  _reg('chart-card-15','Quotes by Status — Count/Value','/bi/query',body);

  const ctx = document.getElementById('q-status-chart');
  if (!ctx) return;
  if (qStatusChart) { qStatusChart.destroy(); qStatusChart = null; }

  try {
    const r = await fetch(`${BASE_URL}/bi/query`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body) });
    const j = await r.json();
    const rows = j.data?.data || [];

    if (!rows.length) { showChartEmpty(ctx, 'No quote status data'); document.getElementById('q-status-chips').innerHTML=''; return; }
    ctx.style.display = '';
    const existing = ctx.parentElement.querySelector('.chart-empty');
    if (existing) existing.remove();

    const STATUS_LABELS = { PEND:'Pending', AWD:'Awarded', BUD:'Budget', DUP:'Duplicate', CHECK:'In Review', REJ:'Rejected', LOST:'Lost' };
    const labels = rows.map(r => STATUS_LABELS[r.QuoteStatus] || r.QuoteStatus || 'Unknown');
    const counts = rows.map(r => parseFloat(r.TotalQuotes||0));
    const colors = rows.map((_,i) => PERF_COLORS[i % PERF_COLORS.length]);

    qStatusChart = new Chart(ctx, {
      type: 'doughnut',
      data: { labels, datasets:[{ data: counts, backgroundColor: colors, borderWidth:2, borderColor:'#fff', hoverOffset:6 }] },
      options: {
        responsive: true, maintainAspectRatio: false, cutout:'62%',
        plugins: {
          legend: { position:'right', labels:{ boxWidth:10, font:{size:11}, padding:12 } },
          tooltip: { callbacks:{ label: c => {
            const row = rows[c.dataIndex];
            return ` ${c.label}: ${dFmtNum(c.raw,0)} quotes · ${dFmtCur(parseFloat(row?.TotalQuoteMax||0))}`;
          }}}
        }
      }
    });

    const total = counts.reduce((a,b)=>a+b,0);
    document.getElementById('q-status-chips').innerHTML = rows.slice(0,6).map((row,i)=>`
      <div class="status-chip">
        <div style="width:8px;height:8px;border-radius:2px;background:${colors[i]};flex-shrink:0"></div>
        <span>${STATUS_LABELS[row.QuoteStatus]||row.QuoteStatus||'Unknown'}</span>
        <strong>${dFmtNum(parseFloat(row.TotalQuotes||0),0)}</strong>
        <span style="color:var(--text-muted);font-size:10px">${total>0?(parseFloat(row.TotalQuotes||0)/total*100).toFixed(0)+'%':''}</span>
      </div>`).join('');
  } catch(e) { showChartEmpty(ctx, 'Error loading chart'); }
}

/* ── Quote Salesperson Table ──────────────────────────────────────── */
async function loadQuoteSalespersonTable(dateFrom, dateTo, yards) {
  const body = {
    datasetName: 'Quotes_By_Status',
    groupBySegments: ['SalesPerson'],
    metrics: [
      { metricName:'TotalQuotes',   aggregation:'COUNT', alias:'TotalQuotes'   },
      { metricName:'TotalQuoteMax', aggregation:'SUM',   alias:'TotalQuoteMax' },
      { metricName:'TotalQuoteMin', aggregation:'SUM',   alias:'TotalQuoteMin' },
    ],
    filters: buildQuoteFilters(dateFrom, dateTo, yards),
    orderBy: [{ field:'TotalQuoteMax', direction:'DESC' }],
    limit: 20
  };
  _reg('chart-card-16','Salesperson Quote Performance','/bi/query',body);

  const tbody = document.getElementById('q-perf-table-body');
  try {
    const r = await fetch(`${BASE_URL}/bi/query`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body) });
    const j = await r.json();
    const rows = j.data?.data || [];

    if (!rows.length) {
      tbody.innerHTML = '<tr><td colspan="4" style="text-align:center;color:var(--text-muted);padding:20px">No data for this period</td></tr>';
      return;
    }

    const maxAmt = Math.max(...rows.map(r => parseFloat(r.TotalQuoteMax||0)));

    tbody.innerHTML = rows.map((row, i) => {
      const total  = parseFloat(row.TotalQuotes   || 0);
      const maxAmt_row = parseFloat(row.TotalQuoteMax || 0);
      const barW   = maxAmt > 0 ? Math.max(0, Math.min(100, maxAmt_row / maxAmt * 100)) : 0;
      const color  = PERF_COLORS[i % PERF_COLORS.length];
      return `
        <tr>
          <td><div style="display:flex;align-items:center;gap:8px">
            <div style="width:8px;height:8px;border-radius:50%;background:${color};flex-shrink:0"></div>
            <span style="font-weight:600">${row.SalesPerson||'—'}</span>
          </div></td>
          <td class="num-cell">${dFmtNum(total,0)}</td>
          <td class="num-cell positive">${dFmtCur(maxAmt_row)}</td>
          <td><div class="progress-cell">
            <div class="progress-bar-wrap"><div class="progress-bar-fill" style="width:${barW}%;background:${color}"></div></div>
            <span class="progress-val">${barW.toFixed(0)}%</span>
          </div></td>
        </tr>`;
    }).join('');
  } catch(e) {
    tbody.innerHTML = '<tr><td colspan="4" style="text-align:center;color:var(--red);padding:20px">Error loading data</td></tr>';
  }
}

/* ── Chart empty state ────────────────────────────────────────────── */
function showChartEmpty(ctx, msg) {
  const wrap = ctx.parentElement;
  ctx.style.display = 'none';
  const existing = wrap.querySelector('.chart-empty');
  if (existing) existing.remove();
  const div = document.createElement('div');
  div.className = 'chart-empty';
  div.style.cssText = 'display:flex;flex-direction:column;align-items:center;justify-content:center;height:100%;color:var(--text-muted);gap:6px;';
  div.innerHTML = `<div style="opacity:.3;color:var(--navy,#003366)"><i data-lucide="bar-chart-2" style="width:36px;height:36px;stroke-width:1.4"></i></div><div style="font-size:12px">${msg}</div>`;
  lucide.createIcons({ el: div });
  wrap.appendChild(div);
}

/* ── Format helpers ───────────────────────────────────────────────── */
function dFmtCur(n) {
  n = parseFloat(n);
  if (isNaN(n)) return '—';
  const neg = n < 0;
  const abs = Math.abs(n);
  let s;
  if      (abs >= 1e9) s = (abs/1e9).toFixed(2)+'B';
  else if (abs >= 1e6) s = (abs/1e6).toFixed(2)+'M';
  else if (abs >= 1e3) s = (abs/1e3).toFixed(1)+'K';
  else                 s = abs.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2});
  return (neg?'-':'')+'$'+s;
}

function dFmtCurShort(n) {
  n = parseFloat(n);
  if (isNaN(n)) return '$0';
  const neg = n < 0;
  const abs = Math.abs(n);
  let s;
  if      (abs >= 1e9) s = (abs/1e9).toFixed(1)+'B';
  else if (abs >= 1e6) s = (abs/1e6).toFixed(1)+'M';
  else if (abs >= 1e3) s = (abs/1e3).toFixed(0)+'K';
  else                 s = abs.toFixed(0);
  return (neg?'-':'')+'$'+s;
}

function dFmtNum(n, dec=2) {
  n = parseFloat(n);
  if (isNaN(n)) return '—';
  return n.toLocaleString('en-US',{minimumFractionDigits:dec,maximumFractionDigits:dec});
}

/* ── Loading overlay ──────────────────────────────────────────────── */
function setDashLoading(on) {
  const el = document.getElementById('dash-loading');
  if (el) el.style.display = on ? 'flex' : 'none';
}

/* ── Reset ────────────────────────────────────────────────────────── */
function resetDashFilters() {
  const now = new Date();
  const ago = new Date(now); ago.setDate(ago.getDate() - 90);
  document.getElementById('dash-date-from').value = ago.toISOString().slice(0,10);
  document.getElementById('dash-date-to').value   = now.toISOString().slice(0,10);
  yardSelectAll();
  refreshDashboard();
}

/* ══════════════════════════════════════════════════════════════════
   JOB SUB-TAB SWITCHING + LAZY LOAD
   ══════════════════════════════════════════════════════════════════ */

const JOB_TABS    = ['finish','profitloss','forecast','revenue'];
const QUOTE_TABS  = ['summary','bystatus','salesperson','forecast'];
const EQUIP_TABS  = ['pl','utilization','workorders'];
const jobLoaded   = {};
const quoteLoaded = {};
const equipLoaded = {};
let   pmLoaded    = false;
let   activeEquipTab = 'pl';
let activeJobTab   = 'finish';
let activeQuoteTab = 'summary';

function switchJobTab(tab) {
  activeJobTab = tab;
  // Update button states
  document.querySelectorAll('.dash-sub-tab').forEach((btn, i) => {
    btn.classList.toggle('active', JOB_TABS[i] === tab);
  });
  // Show/hide panels
  JOB_TABS.forEach(t => {
    const el = document.getElementById(`job-panel-${t}`);
    if (el) el.style.display = t === tab ? '' : 'none';
  });
  // Lazy-load data on first visit
  if (!jobLoaded[tab]) {
    jobLoaded[tab] = true;
    const dateFrom = document.getElementById('dash-date-from')?.value || '';
    const dateTo   = document.getElementById('dash-date-to')?.value   || '';
    const yards    = getSelectedYards();
    loadJobSubTab(tab, dateFrom, dateTo, yards);
  }
}

function switchQuoteTab(tab) {
  activeQuoteTab = tab;
  document.querySelectorAll('.dash-sub-tab-q').forEach((btn, i) => {
    btn.classList.toggle('active', QUOTE_TABS[i] === tab);
  });
  QUOTE_TABS.forEach(t => {
    const el = document.getElementById(`quote-panel-${t}`);
    if (el) el.style.display = t === tab ? '' : 'none';
  });
  if (!quoteLoaded[tab]) {
    quoteLoaded[tab] = true;
    const dateFrom = document.getElementById('dash-date-from')?.value || '';
    const dateTo   = document.getElementById('dash-date-to')?.value   || '';
    const yards    = getSelectedYards();
    loadQuoteSubTab(tab, dateFrom, dateTo, yards);
  }
}

function loadJobSubTab(tab, dateFrom, dateTo, yards) {
  if (tab === 'finish')     { loadFinishJobs(dateFrom, dateTo, yards); }
  if (tab === 'profitloss') { loadJobProfitLoss(dateFrom, dateTo, yards); }
  if (tab === 'forecast')   { loadForecast(dateFrom, dateTo, yards); }
  if (tab === 'revenue')    { loadRevenueReport(dateFrom, dateTo, yards); }
}

function loadQuoteSubTab(tab, dateFrom, dateTo, yards) {
  if (tab === 'bystatus')   { loadQuoteByStatus(dateFrom, dateTo, yards); }
  if (tab === 'salesperson'){ loadQuoteBySalesperson(dateFrom, dateTo, yards); }
  if (tab === 'forecast')   { loadQuoteForecast(dateFrom, dateTo, yards); }
}

// (refreshDashboard consolidation — see master refresh above)

/* ══════════════════════════════════════════════════════════════════
   FINISH JOBS TAB
   ══════════════════════════════════════════════════════════════════ */

let finishStatusChart = null;
let finishPersonChart = null;

async function loadFinishJobs(dateFrom, dateTo, yards) {
  const filters = [];
  if (dateFrom && dateTo) filters.push({ segmentName:'JobStartDate', operator:'between', value:dateFrom, secondValue:dateTo });
  filters.push(...buildYardFilter(yards));

  // KPIs
  try {
    _reg('finish-kpi-row','Finish Jobs — KPIs','/bi/kpis',{ datasetName:'Jobs_By_Status', metrics:[
      { metricName:'TotalJobs', aggregation:'COUNT', alias:'TotalJobs' },
      { metricName:'TotalEstimatedValue', aggregation:'SUM', alias:'TotalEstimatedValue' },
      { metricName:'AvgEstimatedValue', aggregation:'AVG', alias:'AvgEstimatedValue' },
    ], filters });
    const kpiBody = { datasetName:'Jobs_By_Status', metrics:[
      { metricName:'TotalJobs', aggregation:'COUNT', alias:'TotalJobs' },
      { metricName:'TotalEstimatedValue', aggregation:'SUM', alias:'TotalEstimatedValue' },
      { metricName:'AvgEstimatedValue', aggregation:'AVG', alias:'AvgEstimatedValue' },
    ], filters };
    const kr = await fetch(`${BASE_URL}/bi/kpis`, { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(kpiBody) });
    const kj = await kr.json();
    const kv = {}; (kj.data?.kpis||[]).forEach(k=>{ kv[k.name]=k.value; });
    const tiles = [
      { label:'Total Jobs',    value: dFmtNum(parseFloat(kv.TotalJobs||0),0),     color:NAVY,    icon:'briefcase'   },
      { label:'Est. Value',    value: dFmtCur(parseFloat(kv.TotalEstimatedValue||0)), color:GREEN, icon:'dollar-sign' },
      { label:'Avg Job Value', value: dFmtCur(parseFloat(kv.AvgEstimatedValue||0)),  color:'#7c3aed', icon:'trending-up' },
    ];
    document.getElementById('finish-kpi-row').innerHTML = tiles.map(t=>`
      <div class="dash-kpi-tile">
        <div class="dkt-icon" style="background:${t.color}20;color:${t.color}"><i data-lucide="${t.icon}" style="width:18px;height:18px;stroke-width:2"></i></div>
        <div class="dkt-body"><div class="dkt-label">${t.label}</div><div class="dkt-value" style="color:${t.color}">${t.value}</div></div>
      </div>`).join('');
    lucide.createIcons();
  } catch(e) {}

  // Status donut
  const ctxS = document.getElementById('finish-status-chart');
  if (ctxS) {
    if (finishStatusChart) { finishStatusChart.destroy(); finishStatusChart=null; }
    try {
      const r = await fetch(`${BASE_URL}/bi/query`, { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({
        datasetName:'Jobs_By_Status', groupBySegments:['Status'],
        metrics:[{metricName:'TotalJobs',aggregation:'COUNT',alias:'TotalJobs'},{metricName:'TotalEstimatedValue',aggregation:'SUM',alias:'TotalEstimatedValue'}],
        filters, orderBy:[{field:'TotalJobs',direction:'DESC'}], limit:15
      })});
      const j = await r.json();
      const rows = j.data?.data||[];
      if (!rows.length) { showChartEmpty(ctxS,'No data'); }
      else {
        ctxS.style.display='';
        const ex=ctxS.parentElement.querySelector('.chart-empty'); if(ex) ex.remove();
        const STATUS_MAP = {T:'In Progress',Z:'Complete',C:'Cancelled',X:'On Hold',O:'Open',W:'Waiting',F:'Finished'};
        const labels = rows.map(r=>STATUS_MAP[r.Status]||r.Status||'Unknown');
        const counts = rows.map(r=>parseFloat(r.TotalJobs||0));
        const colors = rows.map((_,i)=>PERF_COLORS[i%PERF_COLORS.length]);
        finishStatusChart = new Chart(ctxS, { type:'doughnut',
          data:{ labels, datasets:[{ data:counts, backgroundColor:colors, borderWidth:2, borderColor:'#fff', hoverOffset:6 }] },
          options:{ responsive:true, maintainAspectRatio:false, cutout:'62%',
            plugins:{ legend:{ position:'right', labels:{boxWidth:10,font:{size:11},padding:12} },
              tooltip:{callbacks:{label:c=>` ${c.label}: ${dFmtNum(c.raw,0)} jobs`}} } }
        });
        const total=counts.reduce((a,b)=>a+b,0);
        document.getElementById('finish-status-chips').innerHTML=rows.slice(0,5).map((row,i)=>`
          <div class="status-chip">
            <div style="width:8px;height:8px;border-radius:2px;background:${colors[i]};flex-shrink:0"></div>
            <span>${STATUS_MAP[row.Status]||row.Status||'Unknown'}</span>
            <strong>${dFmtNum(parseFloat(row.TotalJobs||0),0)}</strong>
            <span style="color:var(--text-muted);font-size:10px">${total>0?(parseFloat(row.TotalJobs||0)/total*100).toFixed(0)+'%':''}</span>
          </div>`).join('');

        // Table
        const maxVal=Math.max(...rows.map(r=>parseFloat(r.TotalEstimatedValue||0)));
        document.getElementById('finish-table-body').innerHTML=rows.map((row,i)=>{
          const val=parseFloat(row.TotalEstimatedValue||0);
          const barW=maxVal>0?Math.min(100,val/maxVal*100):0;
          const color=PERF_COLORS[i%PERF_COLORS.length];
          return `<tr>
            <td><div style="display:flex;align-items:center;gap:8px"><div style="width:8px;height:8px;border-radius:50%;background:${color}"></div><span style="font-weight:600">${STATUS_MAP[row.Status]||row.Status}</span></div></td>
            <td class="num-cell">${dFmtNum(parseFloat(row.TotalJobs||0),0)}</td>
            <td class="num-cell positive">${dFmtCur(val)}</td>
            <td><div class="progress-cell"><div class="progress-bar-wrap"><div class="progress-bar-fill" style="width:${barW}%;background:${color}"></div></div><span class="progress-val">${barW.toFixed(0)}%</span></div></td>
          </tr>`;
        }).join('');
      }
    } catch(e) { showChartEmpty(ctxS,'Error'); }
  }

  // Salesperson bar
  const ctxP = document.getElementById('finish-person-chart');
  if (ctxP) {
    if (finishPersonChart) { finishPersonChart.destroy(); finishPersonChart=null; }
    try {
      const r = await fetch(`${BASE_URL}/bi/query`, { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({
        datasetName:'Jobs_By_Status', groupBySegments:['SalesPerson'],
        metrics:[{metricName:'TotalJobs',aggregation:'COUNT',alias:'TotalJobs'},{metricName:'TotalEstimatedValue',aggregation:'SUM',alias:'TotalEstimatedValue'}],
        filters, orderBy:[{field:'TotalEstimatedValue',direction:'DESC'}], limit:15
      })});
      const j = await r.json();
      const rows = j.data?.data||[];
      if (!rows.length) { showChartEmpty(ctxP,'No data'); }
      else {
        ctxP.style.display='';
        const ex=ctxP.parentElement.querySelector('.chart-empty'); if(ex) ex.remove();
        finishPersonChart = new Chart(ctxP, { type:'bar',
          data:{ labels:rows.map(r=>r.SalesPerson||'—'),
            datasets:[
              { label:'Est. Value', data:rows.map(r=>parseFloat(r.TotalEstimatedValue||0)), backgroundColor:TEAL+'99', borderColor:TEAL, borderWidth:1 },
              { label:'# Jobs', data:rows.map(r=>parseFloat(r.TotalJobs||0)), backgroundColor:NAVY+'66', borderColor:NAVY, borderWidth:1, yAxisID:'y2' },
            ]},
          options:{ indexAxis:'y', responsive:true, maintainAspectRatio:false,
            plugins:{ legend:{position:'bottom',labels:{boxWidth:12,font:{size:11}}},
              tooltip:{callbacks:{label:c=>c.datasetIndex===0?` Est. Value: ${dFmtCur(c.raw)}`:` Jobs: ${dFmtNum(c.raw,0)}`}} },
            scales:{
              x:{ grid:{color:'#e2e8f0'}, ticks:{callback:v=>dFmtCurShort(v),font:{size:10}}, beginAtZero:true },
              y:{ grid:{display:false}, ticks:{font:{size:11}} },
              y2:{ display:false, beginAtZero:true }
            }
          }
        });
      }
    } catch(e) { showChartEmpty(ctxP,'Error'); }
  }

  // Yard breakout
  loadYardBreakoutFinish(dateFrom, dateTo, yards);
}

/* ══════════════════════════════════════════════════════════════════
   JOB PROFIT/LOSS TAB
   ══════════════════════════════════════════════════════════════════ */

let jplChart = null;

async function loadJobProfitLoss(dateFrom, dateTo, yards) {
  const filters = [];
  if (dateFrom && dateTo) filters.push({ segmentName:'JobStartDate', operator:'between', value:dateFrom, secondValue:dateTo });
  filters.push(...buildYardFilter(yards));

  // KPIs
  try {
    const kpiBody = { datasetName:'jobs_profit_by_invoice', metrics:[
      { metricName:'Revenue', aggregation:'SUM', alias:'Revenue' },
      { metricName:'LaborActual', aggregation:'SUM', alias:'LaborActual' },
      { metricName:'MaterialActual', aggregation:'SUM', alias:'MaterialActual' },
      { metricName:'LaborHours', aggregation:'SUM', alias:'LaborHours' },
    ], filters };
    const kr = await fetch(`${BASE_URL}/bi/kpis`, { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(kpiBody) });
    const kj = await kr.json();
    const kv = {}; (kj.data?.kpis||[]).forEach(k=>{ kv[k.name]=k.value; });
    const rev=parseFloat(kv.Revenue||0), lab=parseFloat(kv.LaborActual||0), mat=parseFloat(kv.MaterialActual||0);
    const net=rev-lab-mat;
    const tiles = [
      { label:'Revenue',        value: dFmtCur(rev),           color:NAVY,    icon:'dollar-sign'   },
      { label:'Labor Cost',     value: dFmtCur(lab),           color:'#dc2626', icon:'users'        },
      { label:'Material Cost',  value: dFmtCur(mat),           color:'#d97706', icon:'package'      },
      { label:'Net',            value: dFmtCur(net),           color:net>=0?GREEN:RED, icon:'trending-up' },
      { label:'Labor Hours',    value: dFmtNum(parseFloat(kv.LaborHours||0),0)+' hrs', color:'#7c3aed', icon:'clock' },
    ];
    document.getElementById('jpl-kpi-row').innerHTML = tiles.map(t=>`
      <div class="dash-kpi-tile">
        <div class="dkt-icon" style="background:${t.color}20;color:${t.color}"><i data-lucide="${t.icon}" style="width:18px;height:18px;stroke-width:2"></i></div>
        <div class="dkt-body"><div class="dkt-label">${t.label}</div><div class="dkt-value" style="color:${t.color}">${t.value}</div></div>
      </div>`).join('');
    lucide.createIcons();
  } catch(e) {}

  // Chart + table by salesperson
  const ctx = document.getElementById('jpl-chart');
  if (ctx) {
    if (jplChart) { jplChart.destroy(); jplChart=null; }
    try {
      const r = await fetch(`${BASE_URL}/bi/query`, { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({
datasetName:'jobs_profit_by_invoice', groupBySegments:['SalesPersonName'],
        metrics:[
          {metricName:'Revenue',aggregation:'SUM',alias:'Revenue'},
          {metricName:'LaborActual',aggregation:'SUM',alias:'LaborActual'},
          {metricName:'MaterialActual',aggregation:'SUM',alias:'MaterialActual'},
        ],
        filters, orderBy:[{field:'Revenue',direction:'DESC'}], limit:15
      })});
      const j = await r.json();
      const rows = j.data?.data||[];
      if (!rows.length) { showChartEmpty(ctx,'No data'); }
      else {
        ctx.style.display='';
        const ex=ctx.parentElement.querySelector('.chart-empty'); if(ex) ex.remove();
        jplChart = new Chart(ctx, { type:'bar',
          data:{ labels:rows.map(r=>r.SalesPersonName||'—'),
            datasets:[
              { label:'Revenue',  data:rows.map(r=>parseFloat(r.Revenue||0)),      backgroundColor:TEAL+'99', borderColor:TEAL, borderWidth:1 },
              { label:'Labor',    data:rows.map(r=>parseFloat(r.LaborActual||0)),  backgroundColor:NAVY+'88', borderColor:NAVY, borderWidth:1 },
              { label:'Material', data:rows.map(r=>parseFloat(r.MaterialActual||0)), backgroundColor:AMBER+'99', borderColor:AMBER, borderWidth:1 },
            ]},
          options:{ indexAxis:'y', responsive:true, maintainAspectRatio:false,
            plugins:{ legend:{position:'bottom',labels:{boxWidth:12,font:{size:11}}},
              tooltip:{callbacks:{label:c=>` ${c.dataset.label}: ${dFmtCur(c.raw)}`}} },
            scales:{
              x:{ grid:{color:'#e2e8f0'}, ticks:{callback:v=>dFmtCurShort(v),font:{size:10}}, beginAtZero:true },
              y:{ grid:{display:false}, ticks:{font:{size:10}} }
            }
          }
        });

        // Table
        document.getElementById('jpl-table-body').innerHTML=rows.map((row,i)=>{
          const rev=parseFloat(row.Revenue||0), lab=parseFloat(row.LaborActual||0), mat=parseFloat(row.MaterialActual||0);
          const net=rev-lab-mat;
          const color=PERF_COLORS[i%PERF_COLORS.length];
          return `<tr>
            <td><div style="display:flex;align-items:center;gap:8px"><div style="width:8px;height:8px;border-radius:50%;background:${color}"></div><span style="font-weight:600">${row.SalesPersonName||'—'}</span></div></td>
            <td class="num-cell">${dFmtCur(rev)}</td>
            <td class="num-cell" style="color:#dc2626">${dFmtCur(lab)}</td>
            <td class="num-cell" style="color:#d97706">${dFmtCur(mat)}</td>
            <td class="num-cell ${net>=0?'positive':'negative'}">${dFmtCur(net)}</td>
          </tr>`;
        }).join('');
      }
    } catch(e) { showChartEmpty(ctx,'Error'); }
  }

  // Yard breakout
  loadYardBreakoutJPL(dateFrom, dateTo, yards);
}

/* ══════════════════════════════════════════════════════════════════
   FORECAST TAB
   ══════════════════════════════════════════════════════════════════ */

let forecastChart = null;
let forecastPersonChart = null;

async function loadForecast(dateFrom, dateTo, yards) {
  const filters = [];
  if (dateFrom && dateTo) filters.push({ segmentName:'JobStartDate', operator:'between', value:dateFrom, secondValue:dateTo });
  filters.push(...buildYardFilter(yards));

  // KPIs
  try {
    const kpiBody = { datasetName:'Job_Revenue_Forecast', metrics:[
      { metricName:'EstimatedRevenue', aggregation:'SUM', alias:'EstimatedRevenue' },
      { metricName:'ActualRevenue',    aggregation:'SUM', alias:'ActualRevenue'    },
      { metricName:'Variance',         aggregation:'SUM', alias:'Variance'         },
      { metricName:'JobCount',         aggregation:'COUNT', alias:'JobCount'       },
    ], filters };
    const kr = await fetch(`${BASE_URL}/bi/kpis`, { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(kpiBody) });
    const kj = await kr.json();
    const kv = {}; (kj.data?.kpis||[]).forEach(k=>{ kv[k.name]=k.value; });
    const est=parseFloat(kv.EstimatedRevenue||0), act=parseFloat(kv.ActualRevenue||0), vari=parseFloat(kv.Variance||0);
    const tiles = [
      { label:'Est. Revenue',  value:dFmtCur(est),  color:NAVY,    icon:'target'        },
      { label:'Actual Revenue',value:dFmtCur(act),  color:GREEN,   icon:'dollar-sign'   },
      { label:'Variance',      value:dFmtCur(vari), color:vari<=0?GREEN:RED, icon:'trending-up' },
      { label:'Job Count',     value:dFmtNum(parseFloat(kv.JobCount||0),0), color:'#0369a1', icon:'briefcase' },
    ];
    document.getElementById('forecast-kpi-row').innerHTML = tiles.map(t=>`
      <div class="dash-kpi-tile">
        <div class="dkt-icon" style="background:${t.color}20;color:${t.color}"><i data-lucide="${t.icon}" style="width:18px;height:18px;stroke-width:2"></i></div>
        <div class="dkt-body"><div class="dkt-label">${t.label}</div><div class="dkt-value" style="color:${t.color}">${t.value}</div></div>
      </div>`).join('');
    lucide.createIcons();
  } catch(e) {}

  // Estimated vs Actual by Month
  const ctx = document.getElementById('forecast-chart');
  if (ctx) {
    if (forecastChart) { forecastChart.destroy(); forecastChart=null; }
    try {
      const r = await fetch(`${BASE_URL}/bi/query`, { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({
        datasetName:'Job_Revenue_Forecast', groupBySegments:['Year','Month'],
        metrics:[
          {metricName:'EstimatedRevenue',aggregation:'SUM',alias:'EstimatedRevenue'},
          {metricName:'ActualRevenue',aggregation:'SUM',alias:'ActualRevenue'},
          {metricName:'Variance',aggregation:'SUM',alias:'Variance'},
        ],
        filters, orderBy:[{field:'Year',direction:'ASC'},{field:'Month',direction:'ASC'}], limit:60
      })});
      const j = await r.json();
      const rows = (j.data?.data||[]).filter(r=>r.Year&&r.Year>=2020);
      if (!rows.length) { showChartEmpty(ctx,'No forecast data'); }
      else {
        ctx.style.display='';
        const ex=ctx.parentElement.querySelector('.chart-empty'); if(ex) ex.remove();
        const MN=['','Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
        forecastChart = new Chart(ctx, { type:'bar',
          data:{ labels:rows.map(r=>`${MN[+r.Month]||r.Month} ${r.Year}`),
            datasets:[
              { label:'Estimated', data:rows.map(r=>parseFloat(r.EstimatedRevenue||0)), backgroundColor:NAVY+'88', borderColor:NAVY, borderWidth:1, order:2 },
              { label:'Actual',    data:rows.map(r=>parseFloat(r.ActualRevenue||0)),    backgroundColor:TEAL+'bb', borderColor:TEAL, borderWidth:1, order:3 },
              { label:'Variance',  data:rows.map(r=>parseFloat(r.Variance||0)), type:'line', borderColor:AMBER, backgroundColor:AMBER+'22', borderWidth:2.5, pointRadius:3, pointBackgroundColor:AMBER, tension:.35, fill:false, order:1 },
            ]},
          options:{ responsive:true, maintainAspectRatio:false,
            interaction:{mode:'index',intersect:false},
            plugins:{ legend:{position:'bottom',labels:{boxWidth:12,font:{size:11}}},
              tooltip:{callbacks:{label:c=>` ${c.dataset.label}: ${dFmtCur(c.raw)}`}} },
            scales:{
              x:{grid:{display:false},ticks:{font:{size:10},maxRotation:45,maxTicksLimit:12}},
              y:{grid:{color:'#e2e8f0'},ticks:{callback:v=>dFmtCurShort(v),font:{size:10}},beginAtZero:true}
            }
          }
        });
      }
    } catch(e) { showChartEmpty(ctx,'Error'); }
  }

  // Variance by Salesperson
  const ctxP = document.getElementById('forecast-person-chart');
  if (ctxP) {
    if (forecastPersonChart) { forecastPersonChart.destroy(); forecastPersonChart=null; }
    try {
      const r = await fetch(`${BASE_URL}/bi/query`, { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({
datasetName:'Job_Revenue_Forecast', groupBySegments:['SalesPerson'],
        metrics:[
          {metricName:'EstimatedRevenue',aggregation:'SUM',alias:'EstimatedRevenue'},
          {metricName:'ActualRevenue',aggregation:'SUM',alias:'ActualRevenue'},
        ],
        filters, orderBy:[{field:'EstimatedRevenue',direction:'DESC'}], limit:15
      })});
      const j = await r.json();
      const rows = j.data?.data||[];
      if (!rows.length) { showChartEmpty(ctxP,'No data'); }
      else {
        ctxP.style.display='';
        const ex=ctxP.parentElement.querySelector('.chart-empty'); if(ex) ex.remove();
        forecastPersonChart = new Chart(ctxP, { type:'bar',
          data:{ labels:rows.map(r=>r.SalesPerson||'—'),
            datasets:[
              { label:'Estimated', data:rows.map(r=>parseFloat(r.EstimatedRevenue||0)), backgroundColor:NAVY+'88', borderColor:NAVY, borderWidth:1 },
              { label:'Actual',    data:rows.map(r=>parseFloat(r.ActualRevenue||0)),    backgroundColor:TEAL+'bb', borderColor:TEAL, borderWidth:1 },
            ]},
          options:{ indexAxis:'y', responsive:true, maintainAspectRatio:false,
            plugins:{ legend:{position:'bottom',labels:{boxWidth:12,font:{size:11}}},
              tooltip:{callbacks:{label:c=>` ${c.dataset.label}: ${dFmtCur(c.raw)}`}} },
            scales:{
              x:{grid:{color:'#e2e8f0'},ticks:{callback:v=>dFmtCurShort(v),font:{size:10}},beginAtZero:true},
              y:{grid:{display:false},ticks:{font:{size:11}}}
            }
          }
        });
      }
    } catch(e) { showChartEmpty(ctxP,'Error'); }
  }

  // Yard breakout
  loadYardBreakoutForecast(dateFrom, dateTo, yards);
}

/* ══════════════════════════════════════════════════════════════════
   REVENUE REPORT TAB
   ══════════════════════════════════════════════════════════════════ */

let revPersonChart = null;

async function loadRevenueReport(dateFrom, dateTo, yards) {
  const filters = buildJobFilters(dateFrom, dateTo, yards);

  // KPIs (reuse jobs_profit_loss)
  try {
    const kpiBody = { datasetName:'jobs_profit_loss', metrics:[
      { metricName:'JobRevenue', aggregation:'SUM', alias:'JobRevenue' },
      { metricName:'Profit',     aggregation:'SUM', alias:'Profit'     },
      { metricName:'JobCount',   aggregation:'COUNT', alias:'JobCount' },
    ], filters };
    const kr = await fetch(`${BASE_URL}/bi/kpis`, { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(kpiBody) });
    const kj = await kr.json();
    const kv = {}; (kj.data?.kpis||[]).forEach(k=>{ kv[k.name]=k.value; });
    const rev=parseFloat(kv.JobRevenue||0), prof=parseFloat(kv.Profit||0);
    const margin=rev>0?prof/rev*100:0;
    const tiles = [
      { label:'Total Revenue', value:dFmtCur(rev),   color:NAVY,  icon:'dollar-sign' },
      { label:'Total Profit',  value:dFmtCur(prof),  color:prof>=0?GREEN:RED, icon:'trending-up' },
      { label:'Margin %',      value:margin.toFixed(1)+'%', color:margin>=50?GREEN:AMBER, icon:'percent' },
      { label:'Job Count',     value:dFmtNum(parseFloat(kv.JobCount||0),0), color:'#0369a1', icon:'briefcase' },
    ];
    document.getElementById('rev-kpi-row').innerHTML = tiles.map(t=>`
      <div class="dash-kpi-tile">
        <div class="dkt-icon" style="background:${t.color}20;color:${t.color}"><i data-lucide="${t.icon}" style="width:18px;height:18px;stroke-width:2"></i></div>
        <div class="dkt-body"><div class="dkt-label">${t.label}</div><div class="dkt-value" style="color:${t.color}">${t.value}</div></div>
      </div>`).join('');
    lucide.createIcons();
  } catch(e) {}

  // Revenue by salesperson
  const ctx = document.getElementById('rev-person-chart');
  if (ctx) {
    if (revPersonChart) { revPersonChart.destroy(); revPersonChart=null; }
    try {
      const r = await fetch(`${BASE_URL}/bi/query`, { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({
datasetName:'jobs_profit_loss', groupBySegments:['SalesPerson'],
        metrics:[
          {metricName:'JobRevenue',aggregation:'SUM',alias:'JobRevenue'},
          {metricName:'Profit',aggregation:'SUM',alias:'Profit'},
          {metricName:'JobCount',aggregation:'COUNT',alias:'JobCount'},
        ],
        filters, orderBy:[{field:'JobRevenue',direction:'DESC'}], limit:20
      })});
      const j = await r.json();
      const rows = j.data?.data||[];
      if (!rows.length) { showChartEmpty(ctx,'No data'); }
      else {
        ctx.style.display='';
        const ex=ctx.parentElement.querySelector('.chart-empty'); if(ex) ex.remove();
        revPersonChart = new Chart(ctx, { type:'bar',
          data:{ labels:rows.map(r=>r.SalesPerson||'—'),
            datasets:[
              { label:'Revenue', data:rows.map(r=>parseFloat(r.JobRevenue||0)), backgroundColor:TEAL+'99', borderColor:TEAL, borderWidth:1 },
              { label:'Profit',  data:rows.map(r=>parseFloat(r.Profit||0)),     backgroundColor:NAVY+'cc', borderColor:NAVY, borderWidth:1 },
            ]},
          options:{ indexAxis:'y', responsive:true, maintainAspectRatio:false,
            plugins:{ legend:{position:'bottom',labels:{boxWidth:12,font:{size:11}}},
              tooltip:{callbacks:{label:c=>` ${c.dataset.label}: ${dFmtCur(c.raw)}`}} },
            scales:{
              x:{grid:{color:'#e2e8f0'},ticks:{callback:v=>dFmtCurShort(v),font:{size:10}},beginAtZero:true},
              y:{grid:{display:false},ticks:{font:{size:11}}}
            }
          }
        });

        // Table
        document.getElementById('rev-table-body').innerHTML=rows.map((row,i)=>{
          const rev=parseFloat(row.JobRevenue||0), prof=parseFloat(row.Profit||0);
          const margin=rev>0?prof/rev*100:0;
          const color=PERF_COLORS[i%PERF_COLORS.length];
          return `<tr>
            <td><div style="display:flex;align-items:center;gap:8px"><div style="width:8px;height:8px;border-radius:50%;background:${color}"></div><span style="font-weight:600">${row.SalesPerson||'—'}</span></div></td>
            <td class="num-cell">${dFmtCur(rev)}</td>
            <td class="num-cell ${prof>=0?'positive':'negative'}">${dFmtCur(prof)}</td>
            <td><div class="progress-cell"><div class="progress-bar-wrap"><div class="progress-bar-fill" style="width:${Math.min(100,Math.max(0,margin))}%;background:${color}"></div></div><span class="progress-val">${margin.toFixed(1)}%</span></div></td>
            <td class="num-cell">${dFmtNum(parseFloat(row.JobCount||0),0)}</td>
          </tr>`;
        }).join('');
      }
    } catch(e) { showChartEmpty(ctx,'Error'); }
  }
}

/* ══════════════════════════════════════════════════════════════════
  // Yard breakout
  loadYardBreakoutRevenue(dateFrom, dateTo, yards);
}

/* ══════════════════════════════════════════════════════════════════
   QUOTE BY STATUS TAB
   ══════════════════════════════════════════════════════════════════ */

let qsDonutChart=null, qsValueChart=null;

const STATUS_LABELS = { PEND:'Pending', AWD:'Awarded', BUD:'Budget', DUP:'Duplicate', CHECK:'In Review', REJ:'Rejected', LOST:'Lost' };

async function loadQuoteByStatus(dateFrom, dateTo, yards) {
  const filters = buildQuoteFilters(dateFrom, dateTo, yards);
  try {
    const r = await fetch(`${BASE_URL}/bi/query`, { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({
datasetName:'Quotes_By_Status', groupBySegments:['QuoteStatus'],
      metrics:[
        {metricName:'TotalQuotes',aggregation:'COUNT',alias:'TotalQuotes'},
        {metricName:'TotalQuoteMax',aggregation:'SUM',alias:'TotalQuoteMax'},
      ],
      filters, orderBy:[{field:'TotalQuotes',direction:'DESC'}], limit:15
      })});
    const j = await r.json();
    const rows = j.data?.data||[];

    const ctxD=document.getElementById('qs-donut-chart');
    const ctxV=document.getElementById('qs-value-chart');
    if (!rows.length) { if(ctxD)showChartEmpty(ctxD,'No data'); if(ctxV)showChartEmpty(ctxV,'No data'); return; }

    const labels=rows.map(r=>STATUS_LABELS[r.QuoteStatus]||r.QuoteStatus||'Unknown');
    const counts=rows.map(r=>parseFloat(r.TotalQuotes||0));
    const values=rows.map(r=>parseFloat(r.TotalQuoteMax||0));
    const colors=rows.map((_,i)=>PERF_COLORS[i%PERF_COLORS.length]);

    if(ctxD){
      if(qsDonutChart){qsDonutChart.destroy();qsDonutChart=null;}
      ctxD.style.display='';
      const ex=ctxD.parentElement.querySelector('.chart-empty'); if(ex)ex.remove();
      qsDonutChart=new Chart(ctxD,{type:'doughnut',
        data:{labels,datasets:[{data:counts,backgroundColor:colors,borderWidth:2,borderColor:'#fff',hoverOffset:6}]},
        options:{responsive:true,maintainAspectRatio:false,cutout:'62%',
          plugins:{legend:{position:'right',labels:{boxWidth:10,font:{size:11},padding:12}},
            tooltip:{callbacks:{label:c=>` ${c.label}: ${dFmtNum(c.raw,0)} quotes`}}}}
      });
      const total=counts.reduce((a,b)=>a+b,0);
      document.getElementById('qs-chips').innerHTML=rows.slice(0,5).map((row,i)=>`
        <div class="status-chip">
          <div style="width:8px;height:8px;border-radius:2px;background:${colors[i]};flex-shrink:0"></div>
          <span>${STATUS_LABELS[row.QuoteStatus]||row.QuoteStatus}</span>
          <strong>${dFmtNum(parseFloat(row.TotalQuotes||0),0)}</strong>
          <span style="color:var(--text-muted);font-size:10px">${total>0?(parseFloat(row.TotalQuotes||0)/total*100).toFixed(0)+'%':''}</span>
        </div>`).join('');
    }

    if(ctxV){
      if(qsValueChart){qsValueChart.destroy();qsValueChart=null;}
      ctxV.style.display='';
      const ex=ctxV.parentElement.querySelector('.chart-empty'); if(ex)ex.remove();
      qsValueChart=new Chart(ctxV,{type:'bar',
        data:{labels,datasets:[{label:'Est. Value',data:values,backgroundColor:colors,borderWidth:1}]},
        options:{responsive:true,maintainAspectRatio:false,
          plugins:{legend:{display:false},tooltip:{callbacks:{label:c=>` ${c.label}: ${dFmtCur(c.raw)}`}}},
          scales:{x:{grid:{display:false},ticks:{font:{size:10}}},y:{grid:{color:'#e2e8f0'},ticks:{callback:v=>dFmtCurShort(v),font:{size:10}},beginAtZero:true}}
        }
      });
    }

    // Table
    const maxVal=Math.max(...values);
    document.getElementById('qs-table-body').innerHTML=rows.map((row,i)=>{
      const cnt=parseFloat(row.TotalQuotes||0), val=parseFloat(row.TotalQuoteMax||0);
      const barW=maxVal>0?Math.min(100,val/maxVal*100):0;
      const color=colors[i];
      return `<tr>
        <td><div style="display:flex;align-items:center;gap:8px"><div style="width:8px;height:8px;border-radius:50%;background:${color}"></div><span style="font-weight:600">${STATUS_LABELS[row.QuoteStatus]||row.QuoteStatus}</span></div></td>
        <td class="num-cell">${dFmtNum(cnt,0)}</td>
        <td class="num-cell positive">${dFmtCur(val)}</td>
        <td><div class="progress-cell"><div class="progress-bar-wrap"><div class="progress-bar-fill" style="width:${barW}%;background:${color}"></div></div><span class="progress-val">${barW.toFixed(0)}%</span></div></td>
      </tr>`;
    }).join('');
  } catch(e) {}

  // Yard breakout
  loadYardBreakoutQStatus(dateFrom, dateTo, yards);
}

/* ══════════════════════════════════════════════════════════════════
   QUOTE BY SALESPERSON TAB
   ══════════════════════════════════════════════════════════════════ */

let qspBarChart=null;

async function loadQuoteBySalesperson(dateFrom, dateTo, yards) {
  const filters = buildQuoteFilters(dateFrom, dateTo, yards);
  try {
    const r = await fetch(`${BASE_URL}/bi/query`, { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({
datasetName:'Quotes_By_Status', groupBySegments:['SalesPerson'],
      metrics:[
        {metricName:'TotalQuotes',aggregation:'COUNT',alias:'TotalQuotes'},
        {metricName:'TotalQuoteMax',aggregation:'SUM',alias:'TotalQuoteMax'},
      ],
      filters, orderBy:[{field:'TotalQuoteMax',direction:'DESC'}], limit:20
      })});
    const j = await r.json();
    const rows = j.data?.data||[];
    const ctx=document.getElementById('qsp-bar-chart');
    if (!rows.length) { if(ctx)showChartEmpty(ctx,'No data'); return; }

    if(ctx){
      if(qspBarChart){qspBarChart.destroy();qspBarChart=null;}
      ctx.style.display='';
      const ex=ctx.parentElement.querySelector('.chart-empty'); if(ex)ex.remove();
      qspBarChart=new Chart(ctx,{type:'bar',
        data:{labels:rows.map(r=>r.SalesPerson||'—'),
          datasets:[{label:'Est. Value',data:rows.map(r=>parseFloat(r.TotalQuoteMax||0)),backgroundColor:TEAL+'99',borderColor:TEAL,borderWidth:1}]},
        options:{indexAxis:'y',responsive:true,maintainAspectRatio:false,
          plugins:{legend:{display:false},tooltip:{callbacks:{label:c=>` Est. Value: ${dFmtCur(c.raw)}`}}},
          scales:{x:{grid:{color:'#e2e8f0'},ticks:{callback:v=>dFmtCurShort(v),font:{size:10}},beginAtZero:true},y:{grid:{display:false},ticks:{font:{size:11}}}}
        }
      });
    }

    const maxVal=Math.max(...rows.map(r=>parseFloat(r.TotalQuoteMax||0)));
    document.getElementById('qsp-table-body').innerHTML=rows.map((row,i)=>{
      const cnt=parseFloat(row.TotalQuotes||0), val=parseFloat(row.TotalQuoteMax||0);
      const barW=maxVal>0?Math.min(100,val/maxVal*100):0;
      const color=PERF_COLORS[i%PERF_COLORS.length];
      return `<tr>
        <td><div style="display:flex;align-items:center;gap:8px"><div style="width:8px;height:8px;border-radius:50%;background:${color}"></div><span style="font-weight:600">${row.SalesPerson||'—'}</span></div></td>
        <td class="num-cell">${dFmtNum(cnt,0)}</td>
        <td class="num-cell positive">${dFmtCur(val)}</td>
        <td><div class="progress-cell"><div class="progress-bar-wrap"><div class="progress-bar-fill" style="width:${barW}%;background:${color}"></div></div><span class="progress-val">${barW.toFixed(0)}%</span></div></td>
      </tr>`;
    }).join('');
  } catch(e) {}

  // Yard breakout
  loadYardBreakoutQSP(dateFrom, dateTo, yards);
}

/* ══════════════════════════════════════════════════════════════════
   QUOTE REVENUE FORECAST TAB
   ══════════════════════════════════════════════════════════════════ */

let qfMonthChart=null, qfCountChart=null;

async function loadQuoteForecast(dateFrom, dateTo, yards) {
  const filters=[];
  if(dateFrom&&dateTo) filters.push({segmentName:'QuoteDate',operator:'between',value:dateFrom,secondValue:dateTo});
  filters.push(...buildYardFilter(yards));

  try {
    const r = await fetch(`${BASE_URL}/bi/query`, { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({
datasetName:'Quote_Revenue_Forecast', groupBySegments:['Year','Month'],
      metrics:[
        {metricName:'TotalQuoteAmount',aggregation:'SUM',alias:'TotalQuoteAmount'},
        {metricName:'QuoteCount',aggregation:'COUNT',alias:'QuoteCount'},
      ],
      filters, orderBy:[{field:'Year',direction:'ASC'},{field:'Month',direction:'ASC'}], limit:60
      })});
    const j=await r.json();
    const rows=(j.data?.data||[]).filter(r=>r.Year&&r.Year>=2019);
    const MN=['','Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
    const labels=rows.map(r=>`${MN[+r.Month]||r.Month} ${r.Year}`);

    const ctxM=document.getElementById('qf-month-chart');
    if(ctxM){
      if(qfMonthChart){qfMonthChart.destroy();qfMonthChart=null;}
      if(!rows.length){showChartEmpty(ctxM,'No data');}
      else{
        ctxM.style.display='';
        const ex=ctxM.parentElement.querySelector('.chart-empty');if(ex)ex.remove();
        qfMonthChart=new Chart(ctxM,{type:'bar',
          data:{labels,datasets:[{label:'Quote Value',data:rows.map(r=>parseFloat(r.TotalQuoteAmount||0)),backgroundColor:TEAL+'bb',borderColor:TEAL,borderWidth:1}]},
          options:{responsive:true,maintainAspectRatio:false,
            plugins:{legend:{display:false},tooltip:{callbacks:{label:c=>` Value: ${dFmtCur(c.raw)}`}}},
            scales:{x:{grid:{display:false},ticks:{font:{size:10},maxRotation:45,maxTicksLimit:12}},y:{grid:{color:'#e2e8f0'},ticks:{callback:v=>dFmtCurShort(v),font:{size:10}},beginAtZero:true}}
          }
        });
      }
    }

    const ctxC=document.getElementById('qf-count-chart');
    if(ctxC){
      if(qfCountChart){qfCountChart.destroy();qfCountChart=null;}
      if(!rows.length){showChartEmpty(ctxC,'No data');}
      else{
        ctxC.style.display='';
        const ex=ctxC.parentElement.querySelector('.chart-empty');if(ex)ex.remove();
        qfCountChart=new Chart(ctxC,{type:'line',
          data:{labels,datasets:[{label:'# Quotes',data:rows.map(r=>parseFloat(r.QuoteCount||0)),borderColor:NAVY,backgroundColor:NAVY+'22',borderWidth:2.5,pointRadius:3,pointBackgroundColor:NAVY,tension:.35,fill:true}]},
          options:{responsive:true,maintainAspectRatio:false,
            plugins:{legend:{display:false},tooltip:{callbacks:{label:c=>` Quotes: ${dFmtNum(c.raw,0)}`}}},
            scales:{x:{grid:{display:false},ticks:{font:{size:10},maxRotation:45,maxTicksLimit:12}},y:{grid:{color:'#e2e8f0'},ticks:{font:{size:10}},beginAtZero:true}}
          }
        });
      }
    }
  } catch(e) {}
  // Yard breakout
  loadYardBreakoutQForecast(dateFrom, dateTo, yards);
}


/* ══════════════════════════════════════════════════════════════════
   YARD BREAKOUT CHARTS
   ══════════════════════════════════════════════════════════════════ */


function _destroyYardChart(id) {
  if (_yardBreakoutCharts[id]) { _yardBreakoutCharts[id].destroy(); delete _yardBreakoutCharts[id]; }
}

async function loadYardBreakout({ canvasId, datasetName, filters, metrics, labels, colors, title, groupBySegments }) {
  _destroyYardChart(canvasId);
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;

  try {
    const body = {
      datasetName,
      groupBySegments: groupBySegments || ['Yard'],
      metrics,
      filters,
      orderBy: [{ field: metrics[0].alias, direction: 'DESC' }],
      limit: 20
    };
    // Derive card ID from canvas ID: 'yard-breakout-jpl-chart' → parent card
    const _yardCardId = (() => {
      const el = document.getElementById(canvasId);
      return el?.closest('[id]')?.id || canvasId;
    })();
    _reg(_yardCardId, (title || 'Yard Breakout') + ' — ' + datasetName, '/bi/query', body);
    const r = await fetch(`${BASE_URL}/bi/query`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body)
    });
    const j = await r.json();
    const rows = j.data?.data || [];
    if (!rows.length) {
      canvas.parentElement.innerHTML = '<div style="text-align:center;color:var(--text-muted);padding:40px;font-size:13px">No data for selected yards</div>';
      return;
    }

    const groupKey = (groupBySegments && groupBySegments[0]) || 'Yard';
    const yardLabels = rows.map(r => r[groupKey] || r.Yard || r.YardName || r.YardCode || '?');
    const datasets = metrics.map((m, i) => ({
      label: labels[i] || m.alias,
      data: rows.map(r => parseFloat(r[m.alias] || 0)),
      backgroundColor: colors[i] || PERF_COLORS[i],
      borderRadius: 4,
    }));

    _yardBreakoutCharts[canvasId] = new Chart(canvas, {
      type: 'bar',
      data: { labels: yardLabels, datasets },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: { legend: { position: 'top', labels: { font: { size: 11 }, boxWidth: 12 } } },
        scales: {
          x: { grid: { display: false }, ticks: { font: { size: 11 } } },
          y: {
            ticks: {
              font: { size: 10 },
              callback: v => v >= 1000 ? `$${(v/1000).toFixed(0)}K` : v
            }
          }
        }
      }
    });
  } catch(e) { console.error('Yard breakout error:', canvasId, e); }
}

// ── Per-panel yard breakout loaders ──────────────────────────────

async function loadYardBreakoutPL(dateFrom, dateTo, yards) {
  await loadYardBreakout({
    canvasId: 'yard-breakout-pl-chart',
    datasetName: 'jobs_profit_loss',
    filters: buildJobFilters(dateFrom, dateTo, yards),
    metrics: [
      { metricName:'TotalRevenue', aggregation:'SUM', alias:'TotalRevenue' },
      { metricName:'TotalProfit',  aggregation:'SUM', alias:'TotalProfit'  },
    ],
    labels: ['Revenue','Profit'],
    colors: [TEAL, NAVY],
  });
}

async function loadYardBreakoutFinish(dateFrom, dateTo, yards) {
  await loadYardBreakout({
    canvasId: 'yard-breakout-finish-chart',
    datasetName: 'Jobs_By_Status',
    filters: buildJobFilters(dateFrom, dateTo, yards),
    metrics: [
      { metricName:'TotalJobs',           aggregation:'COUNT', alias:'TotalJobs' },
      { metricName:'TotalEstimatedValue', aggregation:'SUM',   alias:'TotalEstimatedValue' },
    ],
    labels: ['Job Count','Estimated Value'],
    colors: [TEAL, NAVY],
  });
}

async function loadYardBreakoutJPL(dateFrom, dateTo, yards) {
  await loadYardBreakout({
    canvasId: 'yard-breakout-jpl-chart',
    datasetName: 'jobs_profit_by_invoice',
    filters: buildJobFilters(dateFrom, dateTo, yards),
    groupBySegments: ['YardName'],
    metrics: [
      { metricName:'Revenue',      aggregation:'SUM', alias:'Revenue'      },
      { metricName:'LaborActual',  aggregation:'SUM', alias:'LaborActual'  },
    ],
    labels: ['Revenue','Labor Actual'],
    colors: [TEAL, NAVY],
  });
}

async function loadYardBreakoutForecast(dateFrom, dateTo, yards) {
  await loadYardBreakout({
    canvasId: 'yard-breakout-forecast-chart',
    datasetName: 'Job_Revenue_Forecast',
    filters: buildJobFilters(dateFrom, dateTo, yards),
    metrics: [
      { metricName:'EstimatedRevenue', aggregation:'SUM', alias:'EstimatedRevenue' },
      { metricName:'ActualRevenue',    aggregation:'SUM', alias:'ActualRevenue'    },
    ],
    labels: ['Estimated','Actual'],
    colors: [TEAL, NAVY],
  });
}

async function loadYardBreakoutRevenue(dateFrom, dateTo, yards) {
  await loadYardBreakout({
    canvasId: 'yard-breakout-rev-chart',
    datasetName: 'jobs_profit_loss',
    filters: buildJobFilters(dateFrom, dateTo, yards),
    metrics: [
      { metricName:'TotalRevenue', aggregation:'SUM', alias:'TotalRevenue' },
      { metricName:'TotalProfit',  aggregation:'SUM', alias:'TotalProfit'  },
    ],
    labels: ['Revenue','Profit'],
    colors: [TEAL, NAVY],
  });
}

async function loadYardBreakoutQSummary(dateFrom, dateTo, yards) {
  await loadYardBreakout({
    canvasId: 'yard-breakout-qsummary-chart',
    datasetName: 'Quotes_By_Status',
    filters: buildQuoteFilters(dateFrom, dateTo, yards),
    metrics: [
      { metricName:'QuoteCount',   aggregation:'COUNT', alias:'QuoteCount' },
      { metricName:'TotalQuoteMax',aggregation:'SUM',   alias:'TotalQuoteMax' },
    ],
    labels: ['Quote Count','Total Value (Max)'],
    colors: [TEAL, NAVY],
  });
}

async function loadYardBreakoutQStatus(dateFrom, dateTo, yards) {
  await loadYardBreakout({
    canvasId: 'yard-breakout-qstatus-chart',
    datasetName: 'Quotes_By_Status',
    filters: buildQuoteFilters(dateFrom, dateTo, yards),
    metrics: [{ metricName:'QuoteCount', aggregation:'COUNT', alias:'QuoteCount' }],
    labels: ['Quote Count'],
    colors: [TEAL],
  });
}

async function loadYardBreakoutQSP(dateFrom, dateTo, yards) {
  await loadYardBreakout({
    canvasId: 'yard-breakout-qsp-chart',
    datasetName: 'Quotes_By_Status',
    filters: buildQuoteFilters(dateFrom, dateTo, yards),
    metrics: [{ metricName:'QuoteCount', aggregation:'COUNT', alias:'QuoteCount' }],
    labels: ['Quote Count'],
    colors: [TEAL],
  });
}

async function loadYardBreakoutQForecast(dateFrom, dateTo, yards) {
  await loadYardBreakout({
    canvasId: 'yard-breakout-qforecast-chart',
    datasetName: 'Quote_Revenue_Forecast',
    filters: buildQuoteFilters(dateFrom, dateTo, yards),
    metrics: [{ metricName:'TotalQuoteAmount', aggregation:'SUM', alias:'TotalQuoteAmount' }],
    labels: ['Estimated Revenue'],
    colors: [TEAL],
  });
}

/* ══════════════════════════════════════════════════════════════════
   EQUIPMENT P/L TAB
   ══════════════════════════════════════════════════════════════════ */

let eplYardChart = null, eplExpenseChart = null;

function buildEquipFilters(dateFrom, dateTo, yards) {
  const f = [];
  if (dateFrom && dateTo) f.push({ segmentName:'InvoiceDate', operator:'between', value: dateFrom, secondValue: dateTo });
  else if (dateFrom)      f.push({ segmentName:'InvoiceDate', operator:'gte', value: dateFrom });
  else if (dateTo)        f.push({ segmentName:'InvoiceDate', operator:'lte', value: dateTo });
  f.push(...buildYardFilter(yards));
  return f;
}

async function loadEquipPL(dateFrom, dateTo, yards) {
  const filters = buildEquipFilters(dateFrom, dateTo, yards);

  // KPIs
  try {
    const kpiBody = {
datasetName: 'Equipment_Profit_Loss',
      metrics: [
        { metricName:'TotalRevenue',     aggregation:'SUM',          alias:'TotalRevenue'   },
        { metricName:'TotalExpenses',    aggregation:'SUM',          alias:'TotalExpenses'  },
        { metricName:'TotalProfit',      aggregation:'SUM',          alias:'TotalProfit'    },
        { metricName:'AvgProfitPercent', aggregation:'AVG',          alias:'AvgMargin'      },
        { metricName:'UnitCount',        aggregation:'COUNT_DISTINCT',alias:'UnitCount'     },
      ],
      filters
    };
    const kr = await fetch(`${BASE_URL}/bi/kpis`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(kpiBody) });
    const kj = await kr.json();
    const kpis = kj.data?.kpis || [];
    const fmt = (n, pfx='$') => {
      const v = parseFloat(n||0);
      if (Math.abs(v)>=1e6) return `${pfx}${(v/1e6).toFixed(1)}M`;
      if (Math.abs(v)>=1000) return `${pfx}${(v/1000).toFixed(1)}K`;
      return `${pfx}${v.toFixed(0)}`;
    };
    const get = name => parseFloat(kpis.find(k=>k.name===name)?.value||0);
    const rev = get('TotalRevenue'), exp = get('TotalExpenses'), profit = get('TotalProfit'),
          margin = get('AvgMargin'), units = get('UnitCount');
    document.getElementById('epl-kpi-row').innerHTML = `
      <div class="kpi-card"><div class="kpi-icon-wrap"><i data-lucide="dollar-sign"></i></div><div class="kpi-body"><div class="kpi-label">Revenue</div><div class="kpi-value">${fmt(rev)}</div></div></div>
      <div class="kpi-card"><div class="kpi-icon-wrap"><i data-lucide="trending-down"></i></div><div class="kpi-body"><div class="kpi-label">Expenses</div><div class="kpi-value">${fmt(exp)}</div></div></div>
      <div class="kpi-card"><div class="kpi-icon-wrap" style="background:${profit>=0?'rgba(34,197,94,.12)':'rgba(239,68,68,.12)'}"><i data-lucide="${profit>=0?'trending-up':'trending-down'}"></i></div><div class="kpi-body"><div class="kpi-label">Profit</div><div class="kpi-value" style="color:${profit>=0?'var(--green)':'var(--red)'}">${fmt(profit)}</div></div></div>
      <div class="kpi-card"><div class="kpi-icon-wrap"><i data-lucide="percent"></i></div><div class="kpi-body"><div class="kpi-label">Avg Margin</div><div class="kpi-value">${margin.toFixed(1)}%</div></div></div>
      <div class="kpi-card"><div class="kpi-icon-wrap"><i data-lucide="package"></i></div><div class="kpi-body"><div class="kpi-label">Units</div><div class="kpi-value">${units.toFixed(0)}</div></div></div>
    `;
    lucide.createIcons({ el: document.getElementById('epl-kpi-row') });
  } catch(e) { console.error('EPL KPI error', e); }

  // Yard bar chart
  try {
    const r = await fetch(`${BASE_URL}/bi/query`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({
datasetName:'Equipment_Profit_Loss', groupBySegments:['Yard'],
      metrics:[
        { metricName:'TotalRevenue',  aggregation:'SUM', alias:'Revenue'  },
        { metricName:'TotalExpenses', aggregation:'SUM', alias:'Expenses' },
        { metricName:'TotalProfit',   aggregation:'SUM', alias:'Profit'   },
      ],
      filters, orderBy:[{field:'Revenue',direction:'DESC'}], limit:15
      })});
    const j = await r.json();
    const rows = j.data?.data || [];
    if (eplYardChart) { eplYardChart.destroy(); eplYardChart = null; }
    const canvas = document.getElementById('epl-yard-chart');
    if (canvas && rows.length) {
      eplYardChart = new Chart(canvas, {
        type:'bar',
        data:{
          labels: rows.map(r=>r.Yard||'?'),
          datasets:[
            { label:'Revenue',  data: rows.map(r=>parseFloat(r.Revenue||0)),  backgroundColor: TEAL,                borderRadius:4 },
            { label:'Expenses', data: rows.map(r=>parseFloat(r.Expenses||0)), backgroundColor: '#e2a03f',           borderRadius:4 },
            { label:'Profit',   data: rows.map(r=>parseFloat(r.Profit||0)),   backgroundColor: NAVY,                borderRadius:4 },
          ]
        },
        options:{ responsive:true, maintainAspectRatio:false,
          plugins:{ legend:{ position:'top', labels:{ font:{size:11}, boxWidth:12 }}},
          scales:{ x:{ grid:{display:false}, ticks:{font:{size:11}}},
            y:{ ticks:{ font:{size:10}, callback: v => v>=1000?`$${(v/1000).toFixed(0)}K`:v }}}
        }
      });
    }
  } catch(e) { console.error('EPL yard chart error', e); }

  // Expense donut
  try {
    const r = await fetch(`${BASE_URL}/bi/query`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({
datasetName:'Equipment_Profit_Loss', groupBySegments:['Yard'],
      metrics:[
        { metricName:'LaborExpenses',    aggregation:'SUM', alias:'Labor'    },
        { metricName:'MaterialExpenses', aggregation:'SUM', alias:'Material' },
        { metricName:'OverheadExpenses', aggregation:'SUM', alias:'Overhead' },
      ],
      filters, limit:1
      })});
    const j = await r.json();
    const totals = (j.data?.data||[]).reduce((acc,row)=>{
      acc.Labor    += parseFloat(row.Labor   ||0);
      acc.Material += parseFloat(row.Material||0);
      acc.Overhead += parseFloat(row.Overhead||0);
      return acc;
    }, {Labor:0, Material:0, Overhead:0});
    if (eplExpenseChart) { eplExpenseChart.destroy(); eplExpenseChart = null; }
    const canvas = document.getElementById('epl-expense-chart');
    if (canvas) {
      eplExpenseChart = new Chart(canvas, {
        type:'doughnut',
        data:{
          labels:['Labor','Material','Overhead'],
          datasets:[{ data:[totals.Labor, totals.Material, totals.Overhead],
            backgroundColor:[TEAL, NAVY, '#e2a03f'], borderWidth:2 }]
        },
        options:{ responsive:true, maintainAspectRatio:false,
          plugins:{ legend:{ position:'bottom', labels:{ font:{size:11}, boxWidth:12 }}}}
      });
    }
  } catch(e) { console.error('EPL expense chart error', e); }

  // Top units table
  try {
    const r = await fetch(`${BASE_URL}/bi/query`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({
      datasetName:'Equipment_Profit_Loss', groupBySegments:['UnitCode','UnitType','Yard'],
      metrics:[
        { metricName:'TotalRevenue',     aggregation:'SUM', alias:'Revenue'  },
        { metricName:'TotalExpenses',    aggregation:'SUM', alias:'Expenses' },
        { metricName:'TotalProfit',      aggregation:'SUM', alias:'Profit'   },
        { metricName:'AvgProfitPercent', aggregation:'AVG', alias:'Margin'   },
      ],
      filters, orderBy:[{field:'Revenue',direction:'DESC'}], limit:50
    })});
    const j = await r.json();
    const rows = j.data?.data || [];
    const fmt = (v,pfx='$') => { const n=parseFloat(v||0); return n>=1000?`${pfx}${(n/1000).toFixed(1)}K`:`${pfx}${n.toFixed(0)}`; };
    const tbody = document.getElementById('epl-table-body');
    if (tbody) tbody.innerHTML = rows.length
      ? rows.map(r=>`<tr>
          <td>${r.UnitCode||'—'}</td><td>${r.UnitType||'—'}</td><td>${r.Yard||'—'}</td>
          <td>${fmt(r.Revenue)}</td><td>${fmt(r.Expenses)}</td>
          <td style="color:${parseFloat(r.Profit||0)>=0?'var(--green)':'var(--red)'}">${fmt(r.Profit)}</td>
          <td>${parseFloat(r.Margin||0).toFixed(1)}%</td>
        </tr>`).join('')
      : '<tr><td colspan="7" style="text-align:center;color:var(--text-muted);padding:20px">No data</td></tr>';
  } catch(e) { console.error('EPL table error', e); }
}

/* ══════════════════════════════════════════════════════════════════
   EQUIPMENT UTILIZATION TAB
   ══════════════════════════════════════════════════════════════════ */

let utilYardChart = null, utilDowntimeChart = null;

async function loadEquipUtilization(dateFrom, dateTo, yards) {
  // Utilization uses Year/Month not a date field — skip date filter, use yard only
  const filters = buildYardFilter(yards);

  // KPIs
  try {
    const kpiBody = {
      datasetName: 'Equipment_Utilization',
      metrics: [
        { metricName:'TotalTargetHours',    aggregation:'SUM', alias:'TargetHours'    },
        { metricName:'TotalAvailableHours', aggregation:'SUM', alias:'AvailableHours' },
        { metricName:'TotalDowntimeHours',  aggregation:'SUM', alias:'DowntimeHours'  },
        { metricName:'AvgUtilization',      aggregation:'AVG', alias:'AvgUtil'        },
      ],
      filters
    };
    const kr = await fetch(`${BASE_URL}/bi/kpis`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(kpiBody) });
    const kj = await kr.json();
    const kpis = kj.data?.kpis || [];
    const get = name => parseFloat(kpis.find(k=>k.name===name)?.value||0);
    const target = get('TargetHours'), avail = get('AvailableHours'),
          down = get('DowntimeHours'), util = get('AvgUtil');
    const fmtH = v => { const n=parseFloat(v||0); return n>=1000?`${(n/1000).toFixed(1)}K hrs`:`${n.toFixed(0)} hrs`; };
    document.getElementById('util-kpi-row').innerHTML = `
      <div class="kpi-card"><div class="kpi-icon-wrap"><i data-lucide="clock"></i></div><div class="kpi-body"><div class="kpi-label">Target Hours</div><div class="kpi-value">${fmtH(target)}</div></div></div>
      <div class="kpi-card"><div class="kpi-icon-wrap"><i data-lucide="check-circle"></i></div><div class="kpi-body"><div class="kpi-label">Available Hours</div><div class="kpi-value">${fmtH(avail)}</div></div></div>
      <div class="kpi-card"><div class="kpi-icon-wrap" style="background:rgba(239,68,68,.12)"><i data-lucide="alert-triangle"></i></div><div class="kpi-body"><div class="kpi-label">Downtime Hours</div><div class="kpi-value" style="color:var(--red)">${fmtH(down)}</div></div></div>
      <div class="kpi-card"><div class="kpi-icon-wrap"><i data-lucide="activity"></i></div><div class="kpi-body"><div class="kpi-label">Avg Utilization</div><div class="kpi-value">${util.toFixed(1)}%</div></div></div>
    `;
    lucide.createIcons({ el: document.getElementById('util-kpi-row') });
  } catch(e) { console.error('Util KPI error', e); }

  // Utilization % by Yard
  try {
    const r = await fetch(`${BASE_URL}/bi/query`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({
datasetName:'Equipment_Utilization', groupBySegments:['Yard'],
      metrics:[
        { metricName:'AvgUtilization',      aggregation:'AVG', alias:'UtilPct'    },
        { metricName:'TotalTargetHours',    aggregation:'SUM', alias:'Target'     },
        { metricName:'TotalAvailableHours', aggregation:'SUM', alias:'Available'  },
      ],
      filters, orderBy:[{field:'UtilPct',direction:'DESC'}], limit:15
    })});
    const j = await r.json();
    const rows = j.data?.data || [];
    if (utilYardChart) { utilYardChart.destroy(); utilYardChart = null; }
    const canvas = document.getElementById('util-yard-chart');
    if (canvas && rows.length) {
      utilYardChart = new Chart(canvas, {
        type:'bar',
        data:{
          labels: rows.map(r=>r.Yard||'?'),
          datasets:[
            { label:'Utilization %', data: rows.map(r=>parseFloat(r.UtilPct||0)), backgroundColor: TEAL, borderRadius:4, yAxisID:'y' },
          ]
        },
        options:{ responsive:true, maintainAspectRatio:false,
          plugins:{ legend:{ position:'top', labels:{ font:{size:11}, boxWidth:12 }}},
          scales:{
            x:{ grid:{display:false}, ticks:{font:{size:11}}},
            y:{ max:100, ticks:{ font:{size:10}, callback: v => `${v}%` }}
          }
        }
      });
    }
  } catch(e) { console.error('Util yard chart error', e); }

  // Downtime by Yard
  try {
    const r = await fetch(`${BASE_URL}/bi/query`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({
datasetName:'Equipment_Utilization', groupBySegments:['Yard'],
      metrics:[
        { metricName:'TotalDowntimeHours', aggregation:'SUM', alias:'DownHrs'  },
        { metricName:'TotalDowntimeDays',  aggregation:'SUM', alias:'DownDays' },
      ],
      filters, orderBy:[{field:'DownHrs',direction:'DESC'}], limit:15
      })});
    const j = await r.json();
    const rows = j.data?.data || [];
    if (utilDowntimeChart) { utilDowntimeChart.destroy(); utilDowntimeChart = null; }
    const canvas = document.getElementById('util-downtime-chart');
    if (canvas && rows.length) {
      utilDowntimeChart = new Chart(canvas, {
        type:'bar',
        data:{
          labels: rows.map(r=>r.Yard||'?'),
          datasets:[
            { label:'Downtime Hours', data: rows.map(r=>parseFloat(r.DownHrs||0)),  backgroundColor:'#ef4444', borderRadius:4 },
            { label:'Downtime Days',  data: rows.map(r=>parseFloat(r.DownDays||0)), backgroundColor:'#f97316', borderRadius:4 },
          ]
        },
        options:{ responsive:true, maintainAspectRatio:false,
          plugins:{ legend:{ position:'top', labels:{ font:{size:11}, boxWidth:12 }}},
          scales:{ x:{ grid:{display:false}, ticks:{font:{size:11}}},
            y:{ ticks:{ font:{size:10} }}}
        }
      });
    }
  } catch(e) { console.error('Util downtime chart error', e); }

  // Unit detail table
  try {
    const r = await fetch(`${BASE_URL}/bi/query`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({
      datasetName:'Equipment_Utilization', groupBySegments:['UnitCode','UnitType','Yard'],
      metrics:[
        { metricName:'TotalTargetHours',    aggregation:'SUM', alias:'Target'    },
        { metricName:'TotalAvailableHours', aggregation:'SUM', alias:'Available' },
        { metricName:'TotalDowntimeHours',  aggregation:'SUM', alias:'Downtime'  },
        { metricName:'AvgUtilization',      aggregation:'AVG', alias:'UtilPct'   },
      ],
      filters, orderBy:[{field:'UtilPct',direction:'ASC'}], limit:100
    })});
    const j = await r.json();
    const rows = j.data?.data || [];
    const fmtH = v => `${parseFloat(v||0).toFixed(0)} hrs`;
    const tbody = document.getElementById('util-table-body');
    const utilColor = pct => parseFloat(pct||0) >= 80 ? 'var(--green)' : parseFloat(pct||0) >= 50 ? 'var(--amber)' : 'var(--red)';
    if (tbody) tbody.innerHTML = rows.length
      ? rows.map(r=>`<tr>
          <td>${r.UnitCode||'—'}</td><td>${r.UnitType||'—'}</td><td>${r.Yard||'—'}</td>
          <td>${fmtH(r.Target)}</td><td>${fmtH(r.Available)}</td>
          <td style="color:var(--red)">${fmtH(r.Downtime)}</td>
          <td style="color:${utilColor(r.UtilPct)};font-weight:600">${parseFloat(r.UtilPct||0).toFixed(1)}%</td>
        </tr>`).join('')
      : '<tr><td colspan="7" style="text-align:center;color:var(--text-muted);padding:20px">No data</td></tr>';
  } catch(e) { console.error('Util table error', e); }
}

/* ══════════════════════════════════════════════════════════════════
   WORK ORDERS TAB
   ══════════════════════════════════════════════════════════════════ */

let woYardChart = null, woCostChart = null;

function buildWOFilters(dateFrom, dateTo, yards) {
  // WO_Dashboard uses InvoiceDate — check if it has a date segment
  // For now use yard only as WO doesn't expose a date segment in its schema
  return buildYardFilter(yards);
}

async function loadWorkOrders(dateFrom, dateTo, yards) {
  const filters = buildWOFilters(dateFrom, dateTo, yards);

  // KPIs
  try {
    const kpiBody = {
      datasetName: 'WO_Dashboard',
      metrics: [
        { metricName:'TotalWorkOrders',  aggregation:'COUNT', alias:'TotalWOs'    },
        { metricName:'TotalLaborCost',   aggregation:'SUM',   alias:'LaborCost'   },
        { metricName:'TotalMaterialCost',aggregation:'SUM',   alias:'MatCost'     },
        { metricName:'TotalWOCost',      aggregation:'SUM',   alias:'TotalCost'   },
        { metricName:'AvgWOCost',        aggregation:'AVG',   alias:'AvgCost'     },
      ],
      filters
    };
    const kr = await fetch(`${BASE_URL}/bi/kpis`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(kpiBody) });
    const kj = await kr.json();
    const kpis = kj.data?.kpis || [];
    const get = name => parseFloat(kpis.find(k=>k.name===name)?.value||0);
    const wos=get('TotalWOs'), labor=get('LaborCost'), mat=get('MatCost'), total=get('TotalCost'), avg=get('AvgCost');
    const fmt = v => { const n=parseFloat(v||0); return n>=1000?`$${(n/1000).toFixed(1)}K`:`$${n.toFixed(0)}`; };
    document.getElementById('wo-kpi-row').innerHTML = `
      <div class="kpi-card"><div class="kpi-icon-wrap"><i data-lucide="wrench"></i></div><div class="kpi-body"><div class="kpi-label">Work Orders</div><div class="kpi-value">${wos.toFixed(0)}</div></div></div>
      <div class="kpi-card"><div class="kpi-icon-wrap"><i data-lucide="users"></i></div><div class="kpi-body"><div class="kpi-label">Labor Cost</div><div class="kpi-value">${fmt(labor)}</div></div></div>
      <div class="kpi-card"><div class="kpi-icon-wrap"><i data-lucide="package"></i></div><div class="kpi-body"><div class="kpi-label">Material Cost</div><div class="kpi-value">${fmt(mat)}</div></div></div>
      <div class="kpi-card"><div class="kpi-icon-wrap"><i data-lucide="dollar-sign"></i></div><div class="kpi-body"><div class="kpi-label">Total Cost</div><div class="kpi-value">${fmt(total)}</div></div></div>
      <div class="kpi-card"><div class="kpi-icon-wrap"><i data-lucide="bar-chart-2"></i></div><div class="kpi-body"><div class="kpi-label">Avg WO Cost</div><div class="kpi-value">${fmt(avg)}</div></div></div>
    `;
    lucide.createIcons({ el: document.getElementById('wo-kpi-row') });
  } catch(e) { console.error('WO KPI error', e); }

  // WOs by Yard
  try {
    const r = await fetch(`${BASE_URL}/bi/query`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({
datasetName:'WO_Dashboard', groupBySegments:['Yard'],
      metrics:[
        { metricName:'TotalWorkOrders', aggregation:'COUNT', alias:'WOs'   },
        { metricName:'TotalWOCost',     aggregation:'SUM',   alias:'Cost'  },
      ],
      filters, orderBy:[{field:'WOs',direction:'DESC'}], limit:15
    })});
    const j = await r.json();
    const rows = j.data?.data || [];
    if (woYardChart) { woYardChart.destroy(); woYardChart = null; }
    const canvas = document.getElementById('wo-yard-chart');
    if (canvas && rows.length) {
      woYardChart = new Chart(canvas, {
        type:'bar',
        data:{
          labels: rows.map(r=>r.Yard||'?'),
          datasets:[
            { label:'Work Orders', data: rows.map(r=>parseFloat(r.WOs||0)), backgroundColor: TEAL, borderRadius:4, yAxisID:'y' },
          ]
        },
        options:{ responsive:true, maintainAspectRatio:false,
          plugins:{ legend:{ position:'top', labels:{ font:{size:11}, boxWidth:12 }}},
          scales:{ x:{ grid:{display:false}, ticks:{font:{size:11}}},
            y:{ ticks:{ font:{size:10} }}}
        }
      });
    }
  } catch(e) { console.error('WO yard chart error', e); }

  // Cost breakdown by Yard
  try {
    const r = await fetch(`${BASE_URL}/bi/query`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({
datasetName:'WO_Dashboard', groupBySegments:['Yard'],
      metrics:[
        { metricName:'TotalLaborCost',    aggregation:'SUM', alias:'Labor'    },
        { metricName:'TotalMaterialCost', aggregation:'SUM', alias:'Material' },
      ],
      filters, orderBy:[{field:'Labor',direction:'DESC'}], limit:15
      })});
    const j = await r.json();
    const rows = j.data?.data || [];
    if (woCostChart) { woCostChart.destroy(); woCostChart = null; }
    const canvas = document.getElementById('wo-cost-chart');
    if (canvas && rows.length) {
      woCostChart = new Chart(canvas, {
        type:'bar',
        data:{
          labels: rows.map(r=>r.Yard||'?'),
          datasets:[
            { label:'Labor',    data: rows.map(r=>parseFloat(r.Labor   ||0)), backgroundColor: TEAL,      borderRadius:4 },
            { label:'Material', data: rows.map(r=>parseFloat(r.Material||0)), backgroundColor: '#e2a03f', borderRadius:4 },
          ]
        },
        options:{ responsive:true, maintainAspectRatio:false,
          plugins:{ legend:{ position:'top', labels:{ font:{size:11}, boxWidth:12 }}},
          scales:{ x:{ grid:{display:false}, ticks:{font:{size:11}}},
            y:{ ticks:{ font:{size:10}, callback: v => v>=1000?`$${(v/1000).toFixed(0)}K`:v }}}
        }
      });
    }
  } catch(e) { console.error('WO cost chart error', e); }

  // Unit detail table
  try {
    const r = await fetch(`${BASE_URL}/bi/query`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({
      datasetName:'WO_Dashboard', groupBySegments:['UnitCode','UnitType','Yard'],
      metrics:[
        { metricName:'TotalWorkOrders',   aggregation:'COUNT', alias:'WOs'      },
        { metricName:'TotalLaborCost',    aggregation:'SUM',   alias:'Labor'    },
        { metricName:'TotalMaterialCost', aggregation:'SUM',   alias:'Material' },
        { metricName:'TotalWOCost',       aggregation:'SUM',   alias:'Total'    },
        { metricName:'AvgWOCost',         aggregation:'AVG',   alias:'Avg'      },
      ],
      filters, orderBy:[{field:'Total',direction:'DESC'}], limit:100
    })});
    const j = await r.json();
    const rows = j.data?.data || [];
    const fmt = v => { const n=parseFloat(v||0); return n>=1000?`$${(n/1000).toFixed(1)}K`:`$${n.toFixed(0)}`; };
    const tbody = document.getElementById('wo-table-body');
    if (tbody) tbody.innerHTML = rows.length
      ? rows.map(r=>`<tr>
          <td>${r.UnitCode||'—'}</td><td>${r.UnitType||'—'}</td><td>${r.Yard||'—'}</td>
          <td>${parseFloat(r.WOs||0).toFixed(0)}</td>
          <td>${fmt(r.Labor)}</td><td>${fmt(r.Material)}</td>
          <td>${fmt(r.Total)}</td><td>${fmt(r.Avg)}</td>
        </tr>`).join('')
      : '<tr><td colspan="8" style="text-align:center;color:var(--text-muted);padding:20px">No data</td></tr>';
  } catch(e) { console.error('WO table error', e); }
}

/* ══════════════════════════════════════════════════════════════════
   PREVENTIVE MAINTENANCE TAB
   ══════════════════════════════════════════════════════════════════ */

// DueStatus: 0 = Coming Due, 1 = Past Due (no friendly name at API level yet)
const PM_STATUS_LABELS = { 0: 'Coming Due', 1: 'Past Due' };

let pmTypeChart = null, pmYardChart = null;

async function loadPM(dateFrom, dateTo, yards) {
  const filters = buildYardFilter(yards);

  // KPIs
  try {
    const kpiBody = {
      datasetName: 'Preventive_Maintenance',
      metrics: [
        { metricName:'TotalActivities', aggregation:'COUNT',          alias:'Total'    },
        { metricName:'PastDueCount',    aggregation:'SUM',            alias:'PastDue'  },
        { metricName:'ComingDueCount',  aggregation:'SUM',            alias:'ComingDue'},
        { metricName:'UnitCount',       aggregation:'COUNT_DISTINCT', alias:'Units'    },
      ],
      filters
    };
    const kr = await fetch(`${BASE_URL}/bi/kpis`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(kpiBody) });
    const kj = await kr.json();
    const kpis = kj.data?.kpis || [];
    const get = name => parseFloat(kpis.find(k=>k.name===name)?.value||0);
    const total=get('Total'), pastDue=get('PastDue'), comingDue=get('ComingDue'), units=get('Units');
    document.getElementById('pm-kpi-row').innerHTML = `
      <div class="kpi-card"><div class="kpi-icon-wrap"><i data-lucide="clipboard-list"></i></div><div class="kpi-body"><div class="kpi-label">Total Activities</div><div class="kpi-value">${total.toFixed(0)}</div></div></div>
      <div class="kpi-card"><div class="kpi-icon-wrap" style="background:rgba(239,68,68,.12)"><i data-lucide="alert-circle"></i></div><div class="kpi-body"><div class="kpi-label">Past Due</div><div class="kpi-value" style="color:var(--red)">${pastDue.toFixed(0)}</div></div></div>
      <div class="kpi-card"><div class="kpi-icon-wrap" style="background:rgba(245,158,11,.12)"><i data-lucide="clock"></i></div><div class="kpi-body"><div class="kpi-label">Coming Due</div><div class="kpi-value" style="color:var(--amber)">${comingDue.toFixed(0)}</div></div></div>
      <div class="kpi-card"><div class="kpi-icon-wrap"><i data-lucide="package"></i></div><div class="kpi-body"><div class="kpi-label">Units Affected</div><div class="kpi-value">${units.toFixed(0)}</div></div></div>
    `;
    lucide.createIcons({ el: document.getElementById('pm-kpi-row') });
  } catch(e) { console.error('PM KPI error', e); }

  // Activities by ScheduleType (grouped bar: Coming Due vs Past Due)
  try {
    const r = await fetch(`${BASE_URL}/bi/query`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({
      datasetName:'Preventive_Maintenance', groupBySegments:['ScheduleType','DueStatus'],
      metrics:[{ metricName:'TotalActivities', aggregation:'COUNT', alias:'Count' }],
      filters, orderBy:[{field:'Count',direction:'DESC'}], limit:50
    })});
    const j = await r.json();
    const rows = j.data?.data || [];
    // Pivot: scheduleType → { comingDue, pastDue }
    const schedules = [...new Set(rows.map(r=>r.ScheduleType||'?'))];
    const pivot = {};
    rows.forEach(r => {
      const s = r.ScheduleType||'?';
      if (!pivot[s]) pivot[s] = { comingDue:0, pastDue:0 };
      if (r.DueStatus==0) pivot[s].comingDue += parseFloat(r.Count||0);
      if (r.DueStatus==1) pivot[s].pastDue   += parseFloat(r.Count||0);
    });
    if (pmTypeChart) { pmTypeChart.destroy(); pmTypeChart = null; }
    const canvas = document.getElementById('pm-type-chart');
    if (canvas && schedules.length) {
      pmTypeChart = new Chart(canvas, {
        type:'bar',
        data:{
          labels: schedules,
          datasets:[
            { label:'Coming Due', data: schedules.map(s=>pivot[s]?.comingDue||0), backgroundColor:'#f59e0b', borderRadius:4 },
            { label:'Past Due',   data: schedules.map(s=>pivot[s]?.pastDue||0),   backgroundColor:'#ef4444', borderRadius:4 },
          ]
        },
        options:{ responsive:true, maintainAspectRatio:false,
          plugins:{ legend:{ position:'top', labels:{ font:{size:11}, boxWidth:12 }}},
          scales:{ x:{ grid:{display:false}, ticks:{font:{size:11}}},
            y:{ ticks:{ font:{size:10} }}}
        }
      });
    }
  } catch(e) { console.error('PM type chart error', e); }

  // Due status by Yard
  try {
    const r = await fetch(`${BASE_URL}/bi/query`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({
      datasetName:'Preventive_Maintenance', groupBySegments:['Yard','DueStatus'],
      metrics:[
        { metricName:'TotalActivities', aggregation:'COUNT',          alias:'Count' },
        { metricName:'UnitCount',       aggregation:'COUNT_DISTINCT', alias:'Units' },
      ],
      filters, limit:50
    })});
    const j = await r.json();
    const rows = j.data?.data || [];
    const yards2 = [...new Set(rows.map(r=>r.Yard||'?'))];
    const pivot = {};
    rows.forEach(r => {
      const y = r.Yard||'?';
      if (!pivot[y]) pivot[y] = { comingDue:0, pastDue:0 };
      if (r.DueStatus==0) pivot[y].comingDue += parseFloat(r.Units||0);
      if (r.DueStatus==1) pivot[y].pastDue   += parseFloat(r.Units||0);
    });
    if (pmYardChart) { pmYardChart.destroy(); pmYardChart = null; }
    const canvas = document.getElementById('pm-yard-chart');
    if (canvas && yards2.length) {
      pmYardChart = new Chart(canvas, {
        type:'bar',
        data:{
          labels: yards2,
          datasets:[
            { label:'Coming Due (units)', data: yards2.map(y=>pivot[y]?.comingDue||0), backgroundColor:'#f59e0b', borderRadius:4 },
            { label:'Past Due (units)',   data: yards2.map(y=>pivot[y]?.pastDue||0),   backgroundColor:'#ef4444', borderRadius:4 },
          ]
        },
        options:{ responsive:true, maintainAspectRatio:false,
          plugins:{ legend:{ position:'top', labels:{ font:{size:11}, boxWidth:12 }}},
          scales:{ x:{ grid:{display:false}, ticks:{font:{size:11}}},
            y:{ ticks:{ font:{size:10} }}}
        }
      });
    }
  } catch(e) { console.error('PM yard chart error', e); }

  // PM detail table
  try {
    const r = await fetch(`${BASE_URL}/bi/query`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({
      datasetName:'Preventive_Maintenance', groupBySegments:['UnitCode','UnitType','Yard','Activity','ScheduleType','DueStatus'],
      metrics:[{ metricName:'TotalActivities', aggregation:'COUNT', alias:'Count' }],
      filters, orderBy:[{field:'Count',direction:'DESC'}], limit:200
    })});
    const j = await r.json();
    const rows = j.data?.data || [];
    const statusColor = s => s==1 ? 'var(--red)' : 'var(--amber)';
    const tbody = document.getElementById('pm-table-body');
    if (tbody) tbody.innerHTML = rows.length
      ? rows.map(r=>`<tr>
          <td>${r.UnitCode||'—'}</td><td>${r.UnitType||'—'}</td><td>${r.Yard||'—'}</td>
          <td>${r.Activity||'—'}</td><td>${r.ScheduleType||'—'}</td>
          <td style="color:${statusColor(r.DueStatus)};font-weight:600">${PM_STATUS_LABELS[r.DueStatus]||r.DueStatus}</td>
          <td>${parseFloat(r.Count||0).toFixed(0)}</td>
        </tr>`).join('')
      : '<tr><td colspan="7" style="text-align:center;color:var(--text-muted);padding:20px">No data</td></tr>';
  } catch(e) { console.error('PM table error', e); }
}


/* ══════════════════════════════════════════════════════════════════
   QUERY INSPECTOR
   ══════════════════════════════════════════════════════════════════ */

// Registry: cardId -> { title, endpoint, body, lastRun }
const _queryRegistry = {};
let _activeInfoBtn = null;

/**
 * Register a query against a card.
 * Call this right before every fetch in each loadX function.
 * cardId  — the chart-card or kpi-row element ID
 * title   — human-readable name shown in the panel header
 * endpoint — '/bi/query' | '/bi/kpis' | '/bi/segment-values?...'
 * body    — the request body object (will be deep-cloned)
 */
function _reg(cardId, title, endpoint, body) {
  _queryRegistry[cardId] = {
    title,
    endpoint,
    body: JSON.parse(JSON.stringify(body)),
    lastRun: null,
    lastResult: null,
  };
}

/* ── Info button injection ───────────────────────────────────────── */
function _injectInfoButtons() {
  document.querySelectorAll('.chart-card[id], .kpi-row[id]').forEach(card => {
    const id = card.id;
    if (!id) return;
    // Find the header element inside this card
    const header = card.querySelector('.chart-card-header, .kpi-card-header');
    if (!header) return;
    // Don't double-inject
    if (header.querySelector('.card-info-btn')) return;

    const btn = document.createElement('button');
    btn.className = 'card-info-btn';
    btn.title = 'Inspect query';
    btn.innerHTML = 'ⓘ';
    btn.setAttribute('data-card-id', id);
    btn.addEventListener('click', e => {
      e.stopPropagation();
      openQueryInspector(id, btn);
    });
    header.appendChild(btn);
  });

  // Also inject on KPI rows (they use a different structure)
  document.querySelectorAll('[id$="-kpi-row"]').forEach(kpiRow => {
    const id = kpiRow.id;
    // KPI rows don't have a chart-card-header — we add a small floating button above the grid
    if (kpiRow.querySelector('.kpi-info-btn-wrap')) return;
    const wrap = document.createElement('div');
    wrap.className = 'kpi-info-btn-wrap';
    wrap.style.cssText = 'display:flex;justify-content:flex-end;margin-bottom:4px';
    const btn = document.createElement('button');
    btn.className = 'card-info-btn';
    btn.title = 'Inspect KPI query';
    btn.innerHTML = 'ⓘ';
    btn.setAttribute('data-card-id', id);
    btn.addEventListener('click', e => {
      e.stopPropagation();
      openQueryInspector(id, btn);
    });
    wrap.appendChild(btn);
    kpiRow.parentElement.insertBefore(wrap, kpiRow);
  });
}

/* ── Syntax-highlight a JSON object ─────────────────────────────── */
function _syntaxHL(obj) {
  const json = JSON.stringify(obj, null, 2);
  return json
    .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
    .replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g, m => {
      if (/^"/.test(m)) {
        if (/:$/.test(m)) return `<span style="color:#7dd3fc">${m}</span>`; // key
        return `<span style="color:#86efac">${m}</span>`; // string value
      }
      if (/true|false/.test(m)) return `<span style="color:#c4b5fd">${m}</span>`;
      if (/null/.test(m))       return `<span style="color:#94a3b8">${m}</span>`;
      return `<span style="color:#fda4af">${m}</span>`; // number
    });
}

/* ── Open inspector panel ────────────────────────────────────────── */
function openQueryInspector(cardId, btn) {
  // Toggle off if same card clicked again
  if (_activeInfoBtn && _activeInfoBtn === btn) {
    closeQueryInspector();
    return;
  }
  // Deactivate previous
  if (_activeInfoBtn) _activeInfoBtn.classList.remove('active');
  _activeInfoBtn = btn;
  btn.classList.add('active');

  const entry = _queryRegistry[cardId];
  const panel = document.getElementById('query-inspector-panel');
  const overlay = document.getElementById('query-inspector-overlay');
  const titleEl = document.getElementById('qi-title');
  const body = document.getElementById('qi-body');

  panel.classList.add('open');
  overlay.classList.add('open');
  lucide.createIcons({ el: panel });

  if (!entry) {
    titleEl.textContent = 'Query Inspector';
    body.innerHTML = `
      <div style="color:var(--text-muted);font-size:13px;padding:20px 0;text-align:center">
        <strong>No query registered yet</strong><br>
        <span style="font-size:11px">Apply filters or switch to this tab to load data first.</span>
      </div>`;
    return;
  }

  titleEl.textContent = entry.title;

  const endpoint = entry.endpoint;
  const isKpi    = endpoint.includes('/kpis');
  const isQuery  = endpoint.includes('/query');
  const isSegVals = endpoint.includes('/segment-values');

  const datasetName = entry.body?.datasetName || entry.body?.dataset || '—';
  const filters = entry.body?.filters || [];
  const metrics = entry.body?.metrics || [];
  const groupBy = entry.body?.groupBySegments || [];

  // Render filters in a readable way
  const filterTags = filters.length
    ? filters.map(f => {
        const op  = f.operator || '';
        const val = Array.isArray(f.value)
          ? f.value.join(', ')
          : (f.secondValue ? `${f.value} → ${f.secondValue}` : f.value);
        return `<span style="display:inline-flex;align-items:center;gap:4px;background:#f1f5f9;border:1px solid var(--border);border-radius:12px;padding:2px 9px;font-size:11px;margin:2px">
          <strong style="color:var(--navy)">${f.segmentName||f.segment||'?'}</strong>
          <span style="color:var(--text-muted)">${op}</span>
          <span style="color:#0369a1">${val}</span>
        </span>`;
      }).join('')
    : '<span style="color:var(--text-muted);font-size:11px">None (all data)</span>';

  const metricTags = metrics.length
    ? metrics.map(m => `<span style="display:inline-flex;align-items:center;background:#e6f7f5;border:1px solid var(--teal);border-radius:12px;padding:2px 9px;font-size:11px;margin:2px;color:var(--navy);font-weight:600">${m.metricName}${m.aggregation?' <span style=color:var(--text-muted);font-weight:400>('+m.aggregation+')</span>':''}</span>`).join('')
    : '<span style="color:var(--text-muted);font-size:11px">All</span>';

  const groupTags = groupBy.length
    ? groupBy.map(g => `<span style="display:inline-flex;background:var(--blue-pill);border:1px solid #b8d0f0;border-radius:12px;padding:2px 9px;font-size:11px;margin:2px;color:#1d4ed8;font-weight:600">${g}</span>`).join('')
    : '<span style="color:var(--text-muted);font-size:11px">—</span>';

  const orderBy = entry.body?.orderBy;
  const limit   = entry.body?.limit;

  body.innerHTML = `
    <div>
      <div class="qi-section-label">Dataset</div>
      <span class="qi-dataset-badge">
        <i data-lucide="database" style="width:12px;height:12px"></i>
        ${datasetName}
      </span>
    </div>
    <div class="qi-divider"></div>
    <div>
      <div class="qi-section-label">Endpoint</div>
      <span class="qi-endpoint-badge">POST ${endpoint}</span>
    </div>
    <div class="qi-divider"></div>
    <div>
      <div class="qi-section-label">Active Filters</div>
      <div style="display:flex;flex-wrap:wrap;gap:2px">${filterTags}</div>
    </div>
    ${isQuery ? `
    <div>
      <div class="qi-section-label">Metrics</div>
      <div style="display:flex;flex-wrap:wrap;gap:2px">${metricTags}</div>
    </div>
    <div>
      <div class="qi-section-label">Group By</div>
      <div style="display:flex;flex-wrap:wrap;gap:2px">${groupTags}</div>
    </div>
    ${orderBy ? `<div><div class="qi-section-label">Order By</div><span style="font-size:11px;color:var(--text-muted)">${orderBy.map(o=>o.field+' '+o.direction).join(', ')}</span></div>` : ''}
    ${limit ? `<div><div class="qi-section-label">Limit</div><span style="font-size:11px;color:var(--text-muted)">${limit} rows</span></div>` : ''}
    ` : ''}
    <div class="qi-divider"></div>
    <div>
      <div class="qi-section-label">Full Request Body</div>
      <div class="qi-code">${_syntaxHL(entry.body)}</div>
    </div>
    <button class="qi-test-btn" id="qi-test-btn" onclick="runQueryInspectorTest('${cardId}')">
      <i data-lucide="play" style="width:14px;height:14px"></i>
      Test Live Query
    </button>
    <div id="qi-result-area"></div>
  `;

  lucide.createIcons({ el: panel });

  // If there's a cached result, show it
  if (entry.lastResult) {
    _renderQueryResult(entry.lastResult, entry.endpoint);
  }
}

/* ── Close inspector ─────────────────────────────────────────────── */
function closeQueryInspector() {
  document.getElementById('query-inspector-panel')?.classList.remove('open');
  document.getElementById('query-inspector-overlay')?.classList.remove('open');
  if (_activeInfoBtn) { _activeInfoBtn.classList.remove('active'); _activeInfoBtn = null; }
}

/* ── Run live test ───────────────────────────────────────────────── */
async function runQueryInspectorTest(cardId) {
  const entry = _queryRegistry[cardId];
  if (!entry) return;

  const btn = document.getElementById('qi-test-btn');
  const resultArea = document.getElementById('qi-result-area');
  if (!btn || !resultArea) return;

  btn.disabled = true;
  btn.innerHTML = '<span class="qi-spinner"></span> Running...';
  resultArea.innerHTML = '';

  try {
    const url = `${BASE_URL}${entry.endpoint}`;
    const resp = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(entry.body),
    });
    const json = await resp.json();
    entry.lastResult = json;
    _renderQueryResult(json, entry.endpoint);
  } catch(e) {
    resultArea.innerHTML = `<div class="qi-error">Network error: ${e.message}</div>`;
  } finally {
    btn.disabled = false;
    btn.innerHTML = '<i data-lucide="play" style="width:14px;height:14px"></i> Test Live Query';
    lucide.createIcons({ el: btn.parentElement });
  }
}

/* ── Render query result ─────────────────────────────────────────── */
function _renderQueryResult(json, endpoint) {
  const resultArea = document.getElementById('qi-result-area');
  if (!resultArea) return;

  if (!json.success) {
    const msg = json.error?.message || JSON.stringify(json.error || json);
    const details = json.error?.details ? json.error.details.map(d=>`<div style="margin-top:4px;font-size:10px;opacity:.8">${d.path?.join('.')}: ${d.message}</div>`).join('') : '';
    resultArea.innerHTML = `<div class="qi-error"><strong>Error:</strong> ${msg}${details}</div>`;
    return;
  }

  const isKpi = endpoint.includes('/kpis');

  if (isKpi) {
    const kpis = json.data?.kpis || [];
    if (!kpis.length) { resultArea.innerHTML = `<div style="color:var(--text-muted);font-size:12px;padding:8px 0">No KPI data returned</div>`; return; }
    const tiles = kpis.map(k => `
      <div class="qi-kpi-tile">
        <div class="qi-kpi-tile-name">${k.name}</div>
        <div class="qi-kpi-tile-value">${k.formatted ?? k.value}</div>
      </div>`).join('');
    resultArea.innerHTML = `
      <div class="qi-result-section">
        <div class="qi-result-meta"><strong>${kpis.length}</strong> KPI metric${kpis.length!==1?'s':''} returned</div>
        <div class="qi-kpi-grid">${tiles}</div>
      </div>`;
    return;
  }

  // Query result
  const rows = json.data?.data || json.data?.values || [];
  if (!rows.length) { resultArea.innerHTML = `<div style="color:var(--text-muted);font-size:12px;padding:8px 0">No rows returned</div>`; return; }

  const cols = Object.keys(rows[0]);
  const thead = `<tr>${cols.map(c=>`<th>${c}</th>`).join('')}</tr>`;
  const tbody = rows.map(row =>
    `<tr>${cols.map(c => {
      const v = row[c];
      const isNum = typeof v === 'number' || (!isNaN(v) && v !== '' && v !== null);
      const display = v === null ? '<span style="color:#94a3b8">null</span>' : String(v);
      return `<td style="${isNum?'text-align:right':''}">${display}</td>`;
    }).join('')}</tr>`
  ).join('');

  const ms = json.data?.executionTimeMs;
  const metaStr = `<strong>${rows.length}</strong> row${rows.length!==1?'s':''} returned${ms ? ` · <strong>${ms}ms</strong>` : ''}`;

  resultArea.innerHTML = `
    <div class="qi-result-section">
      <div class="qi-result-meta">${metaStr}</div>
      <div class="qi-result-table-wrap">
        <table class="qi-result-table">
          <thead>${thead}</thead>
          <tbody>${tbody}</tbody>
        </table>
      </div>
    </div>`;
}

/* ── Auto-inject buttons after DOM is ready ──────────────────────── */
// Called after initDashboard so Lucide is loaded
function _setupInfoButtons() {
  _injectInfoButtons();
  // Debounced re-inject on sub-tab switches — prevents mutation loop
  let _infoDebounce = null;
  const observer = new MutationObserver(() => {
    clearTimeout(_infoDebounce);
    _infoDebounce = setTimeout(_injectInfoButtons, 300);
  });
  observer.observe(document.getElementById('app'), { childList: true, subtree: true });
}
