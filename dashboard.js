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

let activeDashTab = 'jobs'; // 'jobs' | 'quotes'

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
  document.getElementById('dash-tab-jobs').classList.toggle('active', tab === 'jobs');
  document.getElementById('dash-tab-quotes').classList.toggle('active', tab === 'quotes');
  document.getElementById('dash-jobs-content').style.display   = tab === 'jobs'   ? '' : 'none';
  document.getElementById('dash-quotes-content').style.display = tab === 'quotes' ? '' : 'none';
}

/* ── Master refresh ───────────────────────────────────────────────── */
async function refreshDashboard() {
  const dateFrom = document.getElementById('dash-date-from')?.value || '';
  const dateTo   = document.getElementById('dash-date-to')?.value   || '';
  const yards    = getSelectedYards();

  // Reset lazy-load flags so sub-tabs re-fetch with new filters
  JOB_TABS.forEach(t   => { jobLoaded[t]   = false; });
  QUOTE_TABS.forEach(t => { quoteLoaded[t] = false; });

  setDashLoading(true);
  try {
    // Always reload the P/L Dashboard (Jobs) and Quotes Summary panels
    await Promise.all([
      loadJobKpis(dateFrom, dateTo, yards),
      loadMonthChart(dateFrom, dateTo, yards),
      loadSalespersonPerf(dateFrom, dateTo, yards),
      loadStatusBreakdown(dateFrom, dateTo, yards),
      loadQuoteKpis(dateFrom, dateTo, yards),
      loadQuoteMonthChart(dateFrom, dateTo, yards),
      loadQuoteStatusChart(dateFrom, dateTo, yards),
      loadQuoteSalespersonTable(dateFrom, dateTo, yards),
      loadYardBreakoutPL(dateFrom, dateTo, yards),
      loadYardBreakoutQSummary(dateFrom, dateTo, yards),
    ]);
  } catch(e) { console.error('Dashboard refresh error:', e); }
  setDashLoading(false);

  // Mark the always-loaded tabs so they don't re-fire on tab switch
  jobLoaded['pl']       = true;
  quoteLoaded['summary'] = true;

  // Re-load whichever sub-tab is currently visible (if not the defaults)
  if (activeJobTab !== 'pl') {
    jobLoaded[activeJobTab] = true;
    loadJobSubTab(activeJobTab, dateFrom, dateTo, yards);
  }
  if (activeQuoteTab !== 'summary') {
    quoteLoaded[activeQuoteTab] = true;
    loadQuoteSubTab(activeQuoteTab, dateFrom, dateTo, yards);
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

const JOB_TABS    = ['pl','finish','profitloss','forecast','revenue'];
const QUOTE_TABS  = ['summary','bystatus','salesperson','forecast'];
const jobLoaded   = {};
const quoteLoaded = {};
let activeJobTab   = 'pl';
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
}

/* ══════════════════════════════════════════════════════════════════
   JOB PRO
  // Yard breakout
  loadYardBreakoutFinish(dateFrom, dateTo, yards);
FIT/LOSS TAB
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
}

/* ══════════════════════════════════════════════════════════════════
   FORECAST TAB
   ═════════════════════
  // Yard breakout
  loadYardBreakoutJPL(dateFrom, dateTo, yards);
═════════════════════════════════════════════ */

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
}

/* ══════════════════════════════════════════════════════════════════
   REVENUE REPO
  // Yard breakout
  loadYardBreakoutForecast(dateFrom, dateTo, yards);
RT TAB
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
   QUOTE BY STATUS TAB
   ══════════════════════════════════════════════════════════════════ */

let qsDonutChart=null, qsValueChart=null;
const STATUS_LABELS = { PEND
  // Yard breakout
  loadYardBreakoutRevenue(dateFrom, dateTo, yards);
:'Pending', AWD:'Awarded', BUD:'Budget', DUP:'Duplicate', CHECK:'In Review', REJ:'Rejected', LOST:'Lost' };

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
}

/* ══════════════════════════════════════════════════════════════════
   QUOTE BY SALE
  // Yard breakout
  loadYardBreakoutQStatus(dateFrom, dateTo, yards);
SPERSON TAB
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
}

/* ══════════════════════════════════════════════════════════════════
   QUOTE REVENUE FORECAST TAB
   ═════
  // Yard breakout
  loadYardBreakoutQSP(dateFrom, dateTo, yards);
═════════════════════════════════════════════════════════════ */

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
}

  // Yard breakout
  loadYardBreakoutQForecast(dateFrom, dateTo, yards);

/* ══════════════════════════════════════════════════════════════════
   YARD BREAKOUT CHARTS
   ══════════════════════════════════════════════════════════════════ */

const _yardBreakoutCharts = {};

function _destroyYardChart(id) {
  if (_yardBreakoutCharts[id]) { _yardBreakoutCharts[id].destroy(); delete _yardBreakoutCharts[id]; }
}

async function loadYardBreakout({ canvasId, datasetName, filters, metrics, labels, colors, title }) {
  _destroyYardChart(canvasId);
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;

  try {
    const body = {
      datasetName,
      groupBySegments: ['Yard'],
      metrics,
      filters,
      orderBy: [{ field: metrics[0].alias, direction: 'DESC' }],
      limit: 20
    };
    const r = await fetch(`${BASE_URL}/bi/query`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body)
    });
    const j = await r.json();
    const rows = j.data?.data || [];
    if (!rows.length) {
      canvas.parentElement.innerHTML = '<div style="text-align:center;color:var(--text-muted);padding:40px;font-size:13px">No data for selected yards</div>';
      return;
    }

    const yardLabels = rows.map(r => r.Yard || r.YardCode || '?');
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
    metrics: [
      { metricName:'TotalRevenue', aggregation:'SUM', alias:'TotalRevenue' },
      { metricName:'TotalNet',     aggregation:'SUM', alias:'TotalNet'     },
    ],
    labels: ['Revenue','Net Profit'],
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
