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

/* ── Yard dropdown ────────────────────────────────────────────────── */
async function loadYardFilter() {
  try {
    const r = await fetch(`${BASE_URL}/bi/segment-values?datasetName=jobs_profit_loss&segmentName=Yard&limit=100`);
    const j = await r.json();
    const sel = document.getElementById('dash-yard');
    if (!sel) return;
    const vals = j.data?.values || [];
    sel.innerHTML = '<option value="">All Yards</option>' +
      vals.map(v => `<option value="${v.value||v}">${v.value||v}</option>`).join('');
  } catch(e) {}
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
  const yard     = document.getElementById('dash-yard')?.value       || '';
  setDashLoading(true);
  try {
    await Promise.all([
      loadJobKpis(dateFrom, dateTo, yard),
      loadMonthChart(dateFrom, dateTo, yard),
      loadSalespersonPerf(dateFrom, dateTo, yard),
      loadStatusBreakdown(dateFrom, dateTo, yard),
      loadQuoteKpis(dateFrom, dateTo, yard),
      loadQuoteMonthChart(dateFrom, dateTo, yard),
      loadQuoteStatusChart(dateFrom, dateTo, yard),
      loadQuoteSalespersonTable(dateFrom, dateTo, yard),
    ]);
  } catch(e) { console.error('Dashboard refresh error:', e); }
  setDashLoading(false);
}

/* ── Filter builders ──────────────────────────────────────────────── */
function buildJobFilters(dateFrom, dateTo, yard) {
  const f = [];
  if (dateFrom && dateTo) f.push({ segmentName:'JobStartDate', operator:'between', value: dateFrom, secondValue: dateTo });
  else if (dateFrom)      f.push({ segmentName:'JobStartDate', operator:'gte', value: dateFrom });
  else if (dateTo)        f.push({ segmentName:'JobStartDate', operator:'lte', value: dateTo });
  if (yard)               f.push({ segmentName:'Yard', operator:'eq', value: yard });
  return f;
}

function buildQuoteFilters(dateFrom, dateTo, yard) {
  const f = [];
  if (dateFrom && dateTo) f.push({ segmentName:'QuoteDate', operator:'between', value: dateFrom, secondValue: dateTo });
  else if (dateFrom)      f.push({ segmentName:'QuoteDate', operator:'gte', value: dateFrom });
  else if (dateTo)        f.push({ segmentName:'QuoteDate', operator:'lte', value: dateTo });
  if (yard)               f.push({ segmentName:'Yard', operator:'eq', value: yard });
  return f;
}

/* ══════════════════════════════════════════════════════════════════
   JOBS DASHBOARD
   ══════════════════════════════════════════════════════════════════ */

/* ── Job KPI Tiles ────────────────────────────────────────────────── */
async function loadJobKpis(dateFrom, dateTo, yard) {
  const body = {
    datasetName: 'jobs_profit_loss',
    metrics: [
      { metricName:'JobRevenue',    aggregation:'SUM',   alias:'JobRevenue'    },
      { metricName:'TotalExpenses', aggregation:'SUM',   alias:'TotalExpenses' },
      { metricName:'Profit',        aggregation:'SUM',   alias:'Profit'        },
      { metricName:'LaborHours',    aggregation:'SUM',   alias:'LaborHours'    },
      { metricName:'JobCount',      aggregation:'COUNT', alias:'JobCount'      },
    ],
    filters: buildJobFilters(dateFrom, dateTo, yard)
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
async function loadMonthChart(dateFrom, dateTo, yard) {
  const body = {
    datasetName: 'jobs_profit_loss',
    groupBySegments: ['JobYear','JobMonth'],
    metrics: [
      { metricName:'JobRevenue',    aggregation:'SUM', alias:'JobRevenue'    },
      { metricName:'TotalExpenses', aggregation:'SUM', alias:'TotalExpenses' },
      { metricName:'Profit',        aggregation:'SUM', alias:'Profit'        },
    ],
    filters: buildJobFilters(dateFrom, dateTo, yard),
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
async function loadSalespersonPerf(dateFrom, dateTo, yard) {
  const body = {
    datasetName: 'jobs_profit_loss',
    groupBySegments: ['SalesPerson'],
    metrics: [
      { metricName:'JobRevenue',    aggregation:'SUM',   alias:'JobRevenue'    },
      { metricName:'TotalExpenses', aggregation:'SUM',   alias:'TotalExpenses' },
      { metricName:'Profit',        aggregation:'SUM',   alias:'Profit'        },
      { metricName:'JobCount',      aggregation:'COUNT', alias:'JobCount'      },
    ],
    filters: buildJobFilters(dateFrom, dateTo, yard),
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
async function loadStatusBreakdown(dateFrom, dateTo, yard) {
  const body = {
    datasetName: 'jobs_profit_loss',
    groupBySegments: ['JobStatus'],
    metrics: [
      { metricName:'JobCount',   aggregation:'COUNT', alias:'JobCount'   },
      { metricName:'JobRevenue', aggregation:'SUM',   alias:'JobRevenue' },
    ],
    filters: buildJobFilters(dateFrom, dateTo, yard),
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
async function loadQuoteKpis(dateFrom, dateTo, yard) {
  const body = {
    datasetName: 'Quotes_By_Status',
    metrics: [
      { metricName:'TotalQuotes',   aggregation:'COUNT', alias:'TotalQuotes'   },
      { metricName:'TotalQuoteMax', aggregation:'SUM',   alias:'TotalQuoteMax' },
      { metricName:'TotalQuoteMin', aggregation:'SUM',   alias:'TotalQuoteMin' },
    ],
    filters: buildQuoteFilters(dateFrom, dateTo, yard)
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
async function loadQuoteMonthChart(dateFrom, dateTo, yard) {
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
async function loadQuoteStatusChart(dateFrom, dateTo, yard) {
  const body = {
    datasetName: 'Quotes_By_Status',
    groupBySegments: ['QuoteStatus'],
    metrics: [
      { metricName:'TotalQuotes',   aggregation:'COUNT', alias:'TotalQuotes'   },
      { metricName:'TotalQuoteMax', aggregation:'SUM',   alias:'TotalQuoteMax' },
    ],
    filters: buildQuoteFilters(dateFrom, dateTo, yard),
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
async function loadQuoteSalespersonTable(dateFrom, dateTo, yard) {
  const body = {
    datasetName: 'Quotes_By_Status',
    groupBySegments: ['SalesPerson'],
    metrics: [
      { metricName:'TotalQuotes',   aggregation:'COUNT', alias:'TotalQuotes'   },
      { metricName:'TotalQuoteMax', aggregation:'SUM',   alias:'TotalQuoteMax' },
      { metricName:'TotalQuoteMin', aggregation:'SUM',   alias:'TotalQuoteMin' },
    ],
    filters: buildQuoteFilters(dateFrom, dateTo, yard),
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
  document.getElementById('dash-yard').value = '';
  refreshDashboard();
}
