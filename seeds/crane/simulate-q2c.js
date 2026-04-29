#!/usr/bin/env node
/**
 * fcc-simulate-q2c.js
 *
 * Generates realistic synthetic Q2C data for the FCC instance and ingests
 * it via the DDNL ingest API. Covers all 10 Q2C datasets:
 *
 *   quote         ~3,000 rows   (quote lifecycle, all statuses)
 *   quote_line    ~45,000 rows  (line items per quote)
 *   job           ~1,000 rows   (won jobs, current state)
 *   job_daily     ~60,000 rows  (job × fiscal date WIP series)
 *   ticket        ~20,000 rows  (e-tickets)
 *   invoice       ~8,000 rows   (invoices)
 *   invoice_line  ~40,000 rows  (invoice line items)
 *   payment       ~6,000 rows   (payments received)
 *   ar_snapshot   ~25,000 rows  (customer × snapshot date AR aging)
 *   q2c_funnel    ~1,000 rows   (wide denormalized won-job timeline)
 *
 * Posts directly to the DDNL ingest API in batches.
 * Safe to re-run — all rows carry ModifiedDate for conditional upsert.
 *
 * Run:
 *   node fcc-simulate-q2c.js
 * Or with overrides:
 *   API_BASE=http://localhost:3000 node fcc-simulate-q2c.js
 */

'use strict';

const https = require('https');
const http  = require('http');
const url   = require('url');

// ─── Config ───────────────────────────────────────────────────────────────
const API_BASE    = process.env.API_BASE    || 'https://fcc-app-production.up.railway.app';
const API_KEY     = process.env.API_KEY     || 'ik_fcc_2a98cd7b986ff20ed9bff0fa1aed9644b2621e0b124a1646bbd9c41683a44944';
const INSTANCE_ID = process.env.INSTANCE_ID || 'fcc';
const TENANT_ID   = 'fcc';

const BATCH_SIZE      = 500;   // larger batches = fewer round trips
const BACKFILL_MONTHS = 24;   // 2 years of history
const JOBS_PER_MONTH  = 42;   // ~500/yr
const WIN_RATE        = 0.35;
const LOST_RATE       = 0.45;
// remainder = expired/cancelled

// ─── Reference data ──────────────────────────────────────────────────────
const YARDS   = ['Seattle', 'Portland', 'Spokane', 'Tacoma', 'Vancouver'];
const REGIONS = { Seattle:'West', Portland:'West', Spokane:'Interior', Tacoma:'West', Vancouver:'West' };

const CUSTOMERS = [
  { id:'CUST001', name:'Apex Construction',    seg:'Commercial',    region:'West',     tier:'A' },
  { id:'CUST002', name:'NW Steel Erectors',    seg:'Industrial',    region:'West',     tier:'A' },
  { id:'CUST003', name:'Pacific Builders',     seg:'Commercial',    region:'West',     tier:'B' },
  { id:'CUST004', name:'Cascade Civil',        seg:'Civil',         region:'West',     tier:'B' },
  { id:'CUST005', name:'Summit Structural',    seg:'Industrial',    region:'Interior', tier:'A' },
  { id:'CUST006', name:'Rainier Projects',     seg:'Commercial',    region:'West',     tier:'B' },
  { id:'CUST007', name:'Olympic Heavy Lift',   seg:'Industrial',    region:'West',     tier:'A' },
  { id:'CUST008', name:'Columbia Basin Civil', seg:'Civil',         region:'Interior', tier:'C' },
  { id:'CUST009', name:'Puget Sound Const',    seg:'Commercial',    region:'West',     tier:'B' },
  { id:'CUST010', name:'North Cascade Power',  seg:'Energy',        region:'Interior', tier:'A' },
  { id:'CUST011', name:'Evergreen Bridge Co',  seg:'Civil',         region:'West',     tier:'B' },
  { id:'CUST012', name:'Pacific Port Auth',    seg:'Industrial',    region:'West',     tier:'A' },
  { id:'CUST013', name:'Glacier Wind Energy',  seg:'Energy',        region:'Interior', tier:'C' },
  { id:'CUST014', name:'Shoreline Developers', seg:'Commercial',    region:'West',     tier:'B' },
  { id:'CUST015', name:'Mt Hood Contractors',  seg:'Commercial',    region:'West',     tier:'C' },
];

const SALES_REPS = ['Sarah Kim', 'Marcus Webb', 'Lisa Chen', 'Derek Olson', 'Priya Patel'];
const PROJECT_MANAGERS = ['Tom Briggs', 'Ana Reyes', 'James Park', 'Nina Volkova', 'Chris Dunn'];
const OPERATORS = ['R. Nakamura','B. Sullivan','K. Okafor','P. Hernandez','T. Lindstrom',
                   'J. Wolfe','M. Castillo','D. Nguyen','S. Ramos','A. Petrov'];

const BILLING_TYPES = ['Rental','TM','AIA','Parts'];
const BILLING_WEIGHTS = [0.40, 0.30, 0.20, 0.10];
const JOB_TYPES = ['rental','crewed','service','mixed'];
const SOURCES   = ['referral','existing-customer','web','phone','trade-show'];
const LOST_REASONS = ['price','scope-mismatch','lost-to-competitor','no-budget','timeline'];
const UNIT_CLASSES = ['crawler','mobile','tower','rough-terrain','boom-truck'];
const PAYMENT_METHODS = ['check','ACH','wire','card'];
const PAYMENT_TYPES   = ['invoice-payment','retainage-release','prepayment'];

const LINE_TYPES   = ['equipment','labor','crane','subrental','consumable','transport','misc'];
const ITEM_CODES   = {
  equipment:  ['EQ-CRANE-01','EQ-CRANE-02','EQ-BOOM-01'],
  labor:      ['LB-OPERATOR','LB-RIGGER','LB-SUPER'],
  crane:      ['CR-LATTICE','CR-HYDRAULIC','CR-TOWER'],
  subrental:  ['SR-FORKLIFT','SR-MANBOX','SR-TRANSPORT'],
  consumable: ['CS-FUEL','CS-LUBE','CS-WIRE'],
  transport:  ['TR-LOWBOY','TR-PERMIT','TR-PILOT'],
  misc:       ['MS-MARKUP','MS-ADMIN','MS-SAFETY'],
};

const RISK_FLAG_POOL = [
  'unbilled_wip_high','ar_aging_past_60','low_margin','over_budget',
  'stalled_execution','quote_accuracy_low','retainage_exposure','bad_debt_risk',
];

// ─── Helpers ──────────────────────────────────────────────────────────────
function rnd(a, b)     { return a + Math.random() * (b - a); }
function rndInt(a, b)  { return Math.floor(rnd(a, b + 1)); }
function pick(arr)     { return arr[Math.floor(Math.random() * arr.length)]; }
function pickW(arr, weights) {
  const r = Math.random();
  let cum = 0;
  for (let i = 0; i < arr.length; i++) { cum += weights[i]; if (r < cum) return arr[i]; }
  return arr[arr.length - 1];
}
function fmtDate(d)   { return d ? d.toISOString().replace('T',' ').slice(0,19) : null; }
function fmtDay(d)    { return d ? d.toISOString().slice(0,10) : null; }
function addDays(d,n) { const r=new Date(d); r.setDate(r.getDate()+n); return r; }
function addMonths(d,n){ const r=new Date(d); r.setMonth(r.getMonth()+n); return r; }
function diffDays(a,b){ return Math.round((b-a)/86400000); }
function clamp(v,lo,hi){ return Math.max(lo, Math.min(hi, v)); }

function valueBand(v) {
  if (v < 10000)   return '<$10k';
  if (v < 50000)   return '$10-50k';
  if (v < 250000)  return '$50-250k';
  return '$250k+';
}

function fiscalAttrs(date) {
  if (!date) return {};
  const y  = date.getFullYear();
  const m  = date.getMonth() + 1;
  const fy = y; // Jan 1 fiscal year
  const fq = Math.ceil(m / 3);
  const fyStart = new Date(fy, 0, 1);
  const daysSinceFYStart = Math.floor((date - fyStart) / 86400000);
  const fw  = Math.floor(daysSinceFYStart / 7) + 1;
  const dow = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'][date.getDay()];
  const isWeekend = date.getDay() === 0 || date.getDay() === 6;
  const mm  = String(m).padStart(2,'0');
  return {
    FiscalYear:       fy,
    FiscalQuarter:    `Q${fq}`,
    FiscalMonth:      m,
    FiscalMonthLabel: `${fy}-${mm}`,
    FiscalWeek:       fw,
    FiscalDayOfWeek:  dow,
    IsWeekend:        isWeekend,
    IsHoliday:        false,
  };
}

// ─── HTTP ingest helper ───────────────────────────────────────────────────
function postJson(path, body) {
  return new Promise((resolve, reject) => {
    const parsed = new url.URL(API_BASE + path);
    const lib    = parsed.protocol === 'https:' ? https : http;
    const data   = JSON.stringify(body);
    const opts   = {
      hostname: parsed.hostname,
      port:     parsed.port || (parsed.protocol === 'https:' ? 443 : 80),
      path:     parsed.pathname + parsed.search,
      method:   'POST',
      headers: {
        'Content-Type':   'application/json',
        'Content-Length': Buffer.byteLength(data),
        'X-Api-Key':      API_KEY,
        'X-Instance-Id':  INSTANCE_ID,
      },
    };
    const req = lib.request(opts, res => {
      let raw = '';
      res.on('data', c => raw += c);
      res.on('end', () => {
        try {
          const j = JSON.parse(raw);
          if (res.statusCode >= 400) reject(new Error(`HTTP ${res.statusCode}: ${raw.slice(0,200)}`));
          else resolve(j);
        } catch(e) { reject(new Error(`Parse error: ${raw.slice(0,200)}`)); }
      });
    });
    req.on('error', reject);
    req.write(data);
    req.end();
  });
}

async function ingestBatch(dsName, rows) {
  if (!rows.length) return { indexed: 0, failed: 0 };
  const result = await postJson(`/api/ingest/${dsName}/bulk`, { docs: rows });
  return result; // { success, indexed, failed, errors }
}

async function ingestAll(dsName, rows, label) {
  let totalIndexed = 0, totalFailed = 0;
  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE);
    const r = await ingestBatch(dsName, batch);
    totalIndexed += r.indexed || 0;
    totalFailed  += r.failed  || 0;
    process.stdout.write(`\r  ${label}: ${Math.min(i + BATCH_SIZE, rows.length)}/${rows.length} rows...`);
  }
  console.log(`\r  ${label}: ${totalIndexed} indexed, ${totalFailed} failed` + ' '.repeat(10));
  return { indexed: totalIndexed, failed: totalFailed };
}

// ─── Date range ───────────────────────────────────────────────────────────
const NOW       = new Date();
const SIM_START = addMonths(NOW, -BACKFILL_MONTHS);

// ─── Generate base job universe ───────────────────────────────────────────
// Build all quotes first, derive jobs/tickets/invoices/payments from won quotes

function generateQuoteUniverse() {
  const quotes = [];
  let quoteSeq = 1000;
  let jobSeq   = 2000;

  // Iterate month by month
  let mo = new Date(SIM_START.getFullYear(), SIM_START.getMonth(), 1);
  while (mo <= NOW) {
    const quotesThisMonth = rndInt(JOBS_PER_MONTH, JOBS_PER_MONTH + 15);

    for (let q = 0; q < quotesThisMonth; q++) {
      const quoteId  = `QT-${String(++quoteSeq).padStart(5,'0')}`;
      const cust     = pick(CUSTOMERS);
      const rep      = pick(SALES_REPS);
      const yard     = pick(YARDS);
      const billing  = pickW(BILLING_TYPES, BILLING_WEIGHTS);
      const jobType  = billing === 'AIA' ? 'crewed' : pick(JOB_TYPES);
      const source   = pick(SOURCES);
      const revCount = Math.random() < 0.25 ? 2 : 1;
      const quotedValue = rnd(8000, 320000);

      // Request date within the month
      const reqDate  = addDays(mo, rndInt(0, 25));
      const sentDate = addDays(reqDate, rndInt(0, 4));

      // Status probabilities depend on how old the quote is
      const ageMonths = diffDays(sentDate, NOW) / 30;
      let status;
      const r = Math.random();
      if (ageMonths < 0.5) {
        // Recent — mostly sent/in-flight
        status = r < 0.5 ? 'sent' : (r < 0.65 ? 'responded' : r < 0.80 ? 'won' : r < 0.92 ? 'lost' : 'expired');
      } else {
        // Old — should mostly be resolved
        status = r < WIN_RATE ? 'won' : r < WIN_RATE + LOST_RATE ? 'lost' : r < 0.95 ? 'expired' : 'cancelled';
      }

      const respondedDate = ['responded','won','lost','expired','cancelled'].includes(status)
        ? addDays(sentDate, rndInt(1, 7)) : null;
      const expirationDate = addDays(sentDate, 30);
      const closedDate = ['won','lost','expired','cancelled'].includes(status)
        ? addDays(sentDate, rndInt(3, 21)) : null;

      const lostReason = status === 'lost' ? pick(LOST_REASONS) : null;
      const wonJobCode = status === 'won' ? `J-${String(++jobSeq).padStart(5,'0')}` : null;
      const costEst    = quotedValue * rnd(0.55, 0.75);
      const margin     = quotedValue - costEst;

      const daysReqToSent    = diffDays(reqDate, sentDate);
      const daysSentToResp   = respondedDate ? diffDays(sentDate, respondedDate) : null;
      const daysSentToClosed = closedDate ? diffDays(sentDate, closedDate) : null;
      const daysUntilExp     = !closedDate ? diffDays(NOW, expirationDate) : null;

      const fiscalBase = fiscalAttrs(sentDate);
      const productMix = [pick(UNIT_CLASSES), pick(UNIT_CLASSES)].filter((v,i,a)=>a.indexOf(v)===i).join('/');

      quotes.push({
        PK:                 quoteId,
        QuoteId:            quoteId,
        QuoteNumber:        `Q${quoteSeq}`,
        RevisionNumber:     revCount,
        ParentQuoteId:      null,
        WonJobCode:         wonJobCode,
        InstanceId:         INSTANCE_ID,
        TenantId:           TENANT_ID,
        ModifiedDate:       fmtDate(closedDate || sentDate),
        // Status / segments
        QuoteStatus:        status,
        BillingType:        billing,
        JobType:            jobType,
        Source:             source,
        LostReason:         lostReason,
        SalesRep:           rep,
        ProductMix:         productMix,
        ValueBand:          valueBand(quotedValue),
        // Customer
        CustomerId:         cust.id,
        CustomerName:       cust.name,
        CustomerSegment:    cust.seg,
        CustomerRegion:     cust.region,
        CustomerTier:       cust.tier,
        // Fiscal
        ...fiscalBase,
        // Dates
        QuoteRequestedDate: fmtDay(reqDate),
        QuoteSentDate:      fmtDay(sentDate),
        QuoteRespondedDate: fmtDay(respondedDate),
        QuoteExpirationDate: fmtDay(expirationDate),
        QuoteClosedDate:    fmtDay(closedDate),
        // Metrics
        QuotedValue:        Math.round(quotedValue * 100) / 100,
        QuotedCostEstimate: Math.round(costEst * 100) / 100,
        QuotedMargin:       Math.round(margin * 100) / 100,
        LineCount:          rndInt(4, 20),
        DaysRequestedToSent:  daysReqToSent,
        DaysSentToResponded:  daysSentToResp,
        DaysSentToClosed:     daysSentToClosed,
        DaysUntilExpiration:  daysUntilExp,
        QuoteCount:         1,
        SentFlag:           ['sent','responded','won','lost','expired','cancelled'].includes(status) ? 1 : 0,
        WonFlag:            status === 'won' ? 1 : 0,
        LostFlag:           status === 'lost' ? 1 : 0,
        ExpiredFlag:        status === 'expired' ? 1 : 0,
        CancelledFlag:      status === 'cancelled' ? 1 : 0,
        // internal refs
        _yard:     yard,
        _sentDate: sentDate,
        _quotedValue: quotedValue,
        _costEst:  costEst,
        _billing:  billing,
        _jobType:  jobType,
        _cust:     cust,
        _rep:      rep,
        _wonJobCode: wonJobCode,
        _revCount: revCount,
        _productMix: productMix,
        _closedDate: closedDate,
        _sentDateObj: sentDate,
      });
    }
    mo = addMonths(mo, 1);
  }

  return quotes;
}

// ─── Quote lines ──────────────────────────────────────────────────────────
function generateQuoteLines(quotes) {
  const lines = [];
  let seq = 0;
  for (const q of quotes) {
    const lineCount = q.LineCount || rndInt(4, 12);
    const types = Array.from({length: lineCount}, (_, i) =>
      i === 0 ? (q.BillingType === 'Rental' || q.BillingType === 'AIA' ? 'equipment' : 'labor')
              : pick(LINE_TYPES)
    );
    for (const lt of types) {
      const itemCode = pick(ITEM_CODES[lt] || ITEM_CODES.misc);
      const qty      = lt === 'equipment' ? rndInt(1, 3) : rndInt(1, 40);
      const unitP    = lt === 'equipment' ? rnd(1500, 12000)
                     : lt === 'labor'     ? rnd(85, 250)
                     : rnd(50, 2000);
      const disc     = Math.random() < 0.2 ? rnd(0.02, 0.10) : 0;
      const extended = qty * unitP * (1 - disc);
      const cost     = extended * rnd(0.50, 0.72);

      lines.push({
        PK:              `QL-${String(++seq).padStart(7,'0')}`,
        QuoteLineId:     `QL-${String(seq).padStart(7,'0')}`,
        QuoteId:         q.QuoteId,
        QuoteNumber:     q.QuoteNumber,
        InstanceId:      INSTANCE_ID,
        TenantId:        TENANT_ID,
        ModifiedDate:    q.ModifiedDate,
        // Denorm
        QuoteStatus:     q.QuoteStatus,
        BillingType:     q.BillingType,
        CustomerId:      q.CustomerId,
        CustomerName:    q.CustomerName,
        SalesRep:        q.SalesRep,
        Yard:            q._yard,
        Region:          REGIONS[q._yard] || 'West',
        // Line specifics
        LineType:        lt,
        UnitClass:       lt === 'equipment' ? pick(UNIT_CLASSES) : null,
        TonnageBucket:   lt === 'equipment' ? pick(['<50t','50-100t','100-250t','250t+']) : null,
        ItemCode:        itemCode,
        ItemDescription: itemCode.replace(/-/g,' '),
        // Fiscal (from quote sent date)
        ...fiscalAttrs(q._sentDateObj),
        // Metrics
        Quantity:        Math.round(qty * 10) / 10,
        UnitPrice:       Math.round(unitP * 100) / 100,
        ExtendedPrice:   Math.round(extended * 100) / 100,
        DiscountAmount:  Math.round(qty * unitP * disc * 100) / 100,
        DiscountPct:     Math.round(disc * 1000) / 10,
        CostEstimate:    Math.round(cost * 100) / 100,
        MarginEstimate:  Math.round((extended - cost) * 100) / 100,
      });
    }
  }
  return lines;
}

// ─── Jobs (won quotes only) ───────────────────────────────────────────────
function generateJobs(wonQuotes) {
  return wonQuotes.map(q => {
    const awardedValue = q._quotedValue * rnd(0.88, 1.00); // slight discount possible
    const estRevenue   = awardedValue * rnd(0.95, 1.05);
    const estCost      = q._costEst * rnd(0.90, 1.10);

    const startDate    = addDays(q._closedDate || q._sentDateObj, rndInt(7, 30));
    const durationDays = rndInt(14, 90);
    const workComplete = addDays(startDate, durationDays);

    // Determine job status based on timeline
    const isClosed = workComplete < addDays(NOW, -14);
    const isWorkComplete = workComplete < NOW;
    const isInProgress   = startDate < NOW && !isWorkComplete;
    const isScheduled    = !isInProgress && !isWorkComplete;

    let jobStatus;
    if (isClosed)       jobStatus = 'closed';
    else if (isWorkComplete) jobStatus = Math.random() < 0.7 ? 'billed' : 'work_complete';
    else if (isInProgress)   jobStatus = 'in_progress';
    else                     jobStatus = 'scheduled';

    const ticketedRevenue = isInProgress || isWorkComplete || isClosed
      ? awardedValue * rnd(0.70, 1.05) : awardedValue * rnd(0, 0.30);
    const invoicedGross = isClosed || jobStatus === 'billed'
      ? ticketedRevenue * rnd(0.90, 1.02)
      : isWorkComplete ? ticketedRevenue * rnd(0.60, 0.90)
      : ticketedRevenue * rnd(0, 0.60);
    const creditMemo   = invoicedGross > 0 && Math.random() < 0.08 ? invoicedGross * rnd(0.01, 0.05) : 0;
    const invoicedNet  = invoicedGross - creditMemo;
    const retainage    = q._billing === 'AIA' ? invoicedNet * 0.10 : 0;
    const collected    = isClosed ? invoicedNet * rnd(0.92, 1.00)
                       : invoicedNet * rnd(0.40, 0.95);
    const badDebt      = isClosed && Math.random() < 0.03 ? invoicedNet * rnd(0.01, 0.08) : 0;

    const laborHours   = ticketedRevenue / rnd(80, 160);
    const overtimeHrs  = laborHours * rnd(0.05, 0.18);
    const billableHrs  = laborHours * rnd(0.85, 0.97);

    const directLabor  = laborHours * rnd(55, 95);
    const directEquip  = awardedValue * rnd(0.10, 0.25);
    const subrental    = Math.random() < 0.3 ? awardedValue * rnd(0.02, 0.10) : 0;
    const otherDirect  = awardedValue * rnd(0.01, 0.05);
    const totalDirect  = directLabor + directEquip + subrental + otherDirect;
    const grossMargin  = invoicedNet - totalDirect;
    const grossMarginPct = invoicedNet > 0 ? grossMargin / invoicedNet : 0;

    const isAIA = q._billing === 'AIA';
    const pctComplete = isInProgress ? rnd(10, 85) : isWorkComplete || isClosed ? rnd(95, 100) : 0;
    const pctBilled   = pctComplete > 0 ? pctComplete * rnd(0.85, 0.98) : 0;

    // Risk flags
    const risks = [];
    if (invoicedNet - collected > 5000 && diffDays(NOW, addDays(workComplete, 60)) > 0) risks.push('ar_aging_past_60');
    if (grossMarginPct < 0.12) risks.push('low_margin');
    if (totalDirect > estCost * 1.10) risks.push('over_budget');
    if (isInProgress && diffDays(startDate, NOW) > durationDays * 1.3) risks.push('stalled_execution');
    if (invoicedNet > 0 && Math.abs(invoicedNet / q._quotedValue - 1) > 0.20) risks.push('quote_accuracy_low');
    if (retainage > 10000) risks.push('retainage_exposure');

    const invoiceDate  = isWorkComplete || isClosed ? addDays(workComplete, rndInt(3, 14)) : null;
    const finalInvDate = isClosed ? addDays(invoiceDate || workComplete, rndInt(5, 21)) : null;
    const firstPayDate = invoiceDate ? addDays(invoiceDate, rndInt(10, 45)) : null;
    const finalPayDate = isClosed ? addDays(firstPayDate || invoiceDate, rndInt(7, 60)) : null;
    const closedDate   = isClosed ? addDays(finalPayDate || finalInvDate || workComplete, rndInt(1, 14)) : null;

    const pm = pick(PROJECT_MANAGERS);
    const daysOpen = diffDays(startDate, closedDate || NOW);

    const fiscalBase = fiscalAttrs(startDate);

    return {
      // Postgres-clean quote ref
      _quoteRef: q,
      _startDate: startDate,
      _workComplete: workComplete,
      _invoiceDate: invoiceDate,
      _finalInvDate: finalInvDate,
      _firstPayDate: firstPayDate,
      _finalPayDate: finalPayDate,
      _closedDate: closedDate,
      _wonDate: q._closedDate || q._sentDateObj,
      _awardedValue: awardedValue,
      _totalDirect: totalDirect,
      _ticketedRevenue: ticketedRevenue,
      _invoicedNet: invoicedNet,
      _collected: collected,
      _laborHours: laborHours,
      _overtimeHrs: overtimeHrs,
      _billableHrs: billableHrs,
      _pm: pm,
      _risks: risks,
      _isAIA: isAIA,
      _pctComplete: pctComplete,
      _isClosed: isClosed,
      _isWorkComplete: isWorkComplete,
      _durationDays: durationDays,

      // Ingest fields
      PK:                  q._wonJobCode,
      JobCode:             q._wonJobCode,
      JobName:             `${q._cust.name.split(' ')[0]} ${pick(['Tower Erect','Steel Frame','Industrial Lift','Civil Works','Bridge Span','Plant Maintenance','Structural Set'])} ${rndInt(100,999)}`,
      WinningQuoteId:      q.QuoteId,
      WinningQuoteNumber:  q.QuoteNumber,
      InstanceId:          INSTANCE_ID,
      TenantId:            TENANT_ID,
      ModifiedDate:        fmtDate(closedDate || NOW),
      // Segments
      JobStatus:           jobStatus,
      JobType:             q._jobType,
      BillingType:         q._billing,
      ProjectManager:      pm,
      SalesRep:            q._rep,
      Yard:                q._yard,
      Region:              REGIONS[q._yard] || 'West',
      ValueBand:           valueBand(awardedValue),
      CustomerId:          q._cust.id,
      CustomerName:        q._cust.name,
      CustomerSegment:     q._cust.seg,
      CustomerRegion:      q._cust.region,
      CustomerTier:        q._cust.tier,
      RiskFlags:           risks.join(','),
      IsAIAJob:            isAIA,
      ...fiscalBase,
      // Dates
      WonDate:             fmtDay(q._closedDate || q._sentDateObj),
      JobCreatedDate:      fmtDay(addDays(q._closedDate || q._sentDateObj, 1)),
      FirstScheduledDate:  fmtDay(addDays(q._closedDate || q._sentDateObj, rndInt(2,6))),
      JobStartDate:        fmtDay(startDate),
      FirstTicketDate:     fmtDay(addDays(startDate, rndInt(0,3))),
      WorkCompleteDate:    fmtDay(workComplete),
      FirstInvoiceDate:    fmtDay(invoiceDate),
      FinalInvoiceDate:    fmtDay(finalInvDate),
      FirstPaymentDate:    fmtDay(firstPayDate),
      FinalPaymentDate:    fmtDay(finalPayDate),
      ClosedDate:          fmtDay(closedDate),
      // Metrics
      QuotedValue:         Math.round(q._quotedValue * 100) / 100,
      AwardedValue:        Math.round(awardedValue * 100) / 100,
      EstimatedRevenue:    Math.round(estRevenue * 100) / 100,
      EstimatedCost:       Math.round(estCost * 100) / 100,
      TicketedRevenue:     Math.round(ticketedRevenue * 100) / 100,
      InvoicedGross:       Math.round(invoicedGross * 100) / 100,
      CreditMemoAmount:    Math.round(creditMemo * 100) / 100,
      InvoicedNet:         Math.round(invoicedNet * 100) / 100,
      CollectedAmount:     Math.round(collected * 100) / 100,
      RetainageHeld:       Math.round(retainage * 100) / 100,
      BadDebtAmount:       Math.round(badDebt * 100) / 100,
      DirectLaborCost:     Math.round(directLabor * 100) / 100,
      DirectEquipmentCost: Math.round(directEquip * 100) / 100,
      SubrentalCost:       Math.round(subrental * 100) / 100,
      OtherDirectCost:     Math.round(otherDirect * 100) / 100,
      TotalDirectCost:     Math.round(totalDirect * 100) / 100,
      GrossMargin:         Math.round(grossMargin * 100) / 100,
      GrossMarginPct:      Math.round(grossMarginPct * 10000) / 10000,
      TicketCount:         rndInt(3, 25),
      InvoiceCount:        rndInt(1, 8),
      PaymentCount:        rndInt(1, 6),
      LaborHours:          Math.round(laborHours * 10) / 10,
      OvertimeHours:       Math.round(overtimeHrs * 10) / 10,
      BillableHours:       Math.round(billableHrs * 10) / 10,
      DaysOpen:            daysOpen,
      PctComplete:         Math.round(pctComplete * 10) / 10,
      PctBilled:           Math.round(pctBilled * 10) / 10,
      PctCompleteMinusBilled: Math.round((pctComplete - pctBilled) * 10) / 10,
    };
  });
}

// ─── Job daily ────────────────────────────────────────────────────────────
function generateJobDaily(jobs) {
  const rows = [];
  let seq = 0;
  for (const job of jobs) {
    const startDate = job._startDate;
    const endDate   = job._closedDate || NOW;
    const totalDays = diffDays(startDate, endDate);
    if (totalDays <= 0) continue;

    // Apportion job totals across days
    const dailyRevTarget = job._ticketedRevenue / totalDays;
    const dailyLabTarget = job._laborHours / totalDays;

    let d = new Date(startDate);
    while (d <= endDate && d <= NOW) {
      // Skip weekends — cranes occasionally work weekends but skip for volume
      if (d.getDay() === 0 || d.getDay() === 6) { d = addDays(d, 1); continue; }
      const f = fiscalAttrs(d);
      const isActive  = true; // weekdays only — always a work day for this job
      const isBillDay = isActive && Math.random() < 0.85;
      const daysOpenAsOf = diffDays(startDate, d);
      const isInProg = d >= startDate && d <= (job._workComplete || NOW);

      // Daily revenue/hours with some noise
      const revMult   = isActive ? rnd(0.5, 1.8) : 0;
      const ticketRev = isBillDay ? dailyRevTarget * revMult : 0;
      const invoiced  = job._invoiceDate && d >= job._invoiceDate
        ? (ticketRev * rnd(0.80, 1.10)) : 0;
      const collected = job._firstPayDate && d >= job._firstPayDate
        ? invoiced * rnd(0, 1.2) : 0;
      const laborHrs  = isActive ? dailyLabTarget * rnd(0.6, 1.4) : 0;
      const equipHrs  = isActive ? laborHrs * rnd(0.8, 1.1) : 0;

      // WIP balance as of this day (cumulative ticketed - invoiced)
      // simplified: use daily amounts as proxy
      const wipBalance = Math.max(0, ticketRev - invoiced) * rnd(0.8, 1.2);

      rows.push({
        PK:               `JD-${String(++seq).padStart(8,'0')}`,
        InstanceId:       INSTANCE_ID,
        TenantId:         TENANT_ID,
        ModifiedDate:     fmtDate(d),
        FiscalDate:       fmtDay(d),
        // Denorm job
        JobCode:          job.JobCode,
        JobName:          job.JobName,
        JobStatus:        job.JobStatus,
        JobType:          job.JobType,
        BillingType:      job.BillingType,
        ProjectManager:   job.ProjectManager,
        SalesRep:         job.SalesRep,
        Yard:             job.Yard,
        Region:           job.Region,
        ValueBand:        job.ValueBand,
        IsAIAJob:         job.IsAIAJob,
        CustomerId:       job.CustomerId,
        CustomerName:     job.CustomerName,
        CustomerSegment:  job.CustomerSegment,
        CustomerRegion:   job.CustomerRegion,
        CustomerTier:     job.CustomerTier,
        ...f,
        // Daily metrics
        DayFlag:          1,
        OpenFlag:         1,
        InProgressFlag:   isInProg ? 1 : 0,
        BillableDayFlag:  isBillDay ? 1 : 0,
        DaysOpenAsOf:     daysOpenAsOf,
        TicketedRevenueDaily: Math.round(ticketRev * 100) / 100,
        InvoicedDaily:        Math.round(invoiced * 100) / 100,
        CollectedDaily:       Math.round(collected * 100) / 100,
        LaborHoursDaily:      Math.round(laborHrs * 10) / 10,
        EquipmentHoursDaily:  Math.round(equipHrs * 10) / 10,
        WIPBalanceAsOf:       Math.round(wipBalance * 100) / 100,
      });
      d = addDays(d, 1);
    }
  }
  return rows;
}

// ─── Tickets ─────────────────────────────────────────────────────────────
function generateTickets(jobs) {
  const rows = [];
  let seq = 0;
  for (const job of jobs) {
    const count = job.TicketCount || rndInt(2, 15);
    const startDate = job._startDate;
    const workComplete = job._workComplete;
    const spanDays = diffDays(startDate, workComplete);
    if (spanDays <= 0) continue;

    for (let t = 0; t < count; t++) {
      const ticketDate = addDays(startDate, rndInt(0, spanDays));
      if (ticketDate > NOW) continue;

      const isFinalized  = ticketDate < addDays(NOW, -3);
      const isInvoiced   = isFinalized && ticketDate < addDays(NOW, -7);
      const status = isInvoiced ? 'invoiced' : isFinalized ? 'finalized' : 'work_complete';

      const finalizedDate = isFinalized ? addDays(ticketDate, rndInt(0, 4)) : null;
      const invoicedDate  = isInvoiced  ? addDays(finalizedDate, rndInt(1, 7)) : null;

      const hours    = rnd(4, 14);
      const overtime = hours > 10 ? rnd(0, hours - 8) : 0;
      const billable = hours * rnd(0.88, 1.00);
      const billAmt  = billable * rnd(80, 220);
      const laborCost = hours * rnd(55, 95);
      const equipCost = billAmt * rnd(0.10, 0.30);
      const otherCost = billAmt * rnd(0.02, 0.08);
      const margin    = billAmt - laborCost - equipCost - otherCost;

      const daysFin    = finalizedDate ? diffDays(ticketDate, finalizedDate) : null;
      const daysFinInv = invoicedDate && finalizedDate ? diffDays(finalizedDate, invoicedDate) : null;

      const f = fiscalAttrs(ticketDate);

      rows.push({
        PK:             `TK-${String(++seq).padStart(7,'0')}`,
        TicketId:       `TK-${String(seq).padStart(7,'0')}`,
        TicketNumber:   `T${seq}`,
        InstanceId:     INSTANCE_ID,
        TenantId:       TENANT_ID,
        ModifiedDate:   fmtDate(invoicedDate || finalizedDate || ticketDate),
        // Denorm job
        JobCode:        job.JobCode,
        JobName:        job.JobName,
        JobType:        job.JobType,
        JobStatus:      job.JobStatus,
        BillingType:    job.BillingType,
        ProjectManager: job.ProjectManager,
        SalesRep:       job.SalesRep,
        Yard:           job.Yard,
        Region:         job.Region,
        CustomerId:     job.CustomerId,
        CustomerName:   job.CustomerName,
        CustomerSegment: job.CustomerSegment,
        CustomerRegion:  job.CustomerRegion,
        CustomerTier:    job.CustomerTier,
        // Ticket specifics
        TicketType:     pick(['daily','T&M','service call']),
        TicketStatus:   status,
        OperatorId:     `OP-${rndInt(1,10).toString().padStart(3,'0')}`,
        OperatorName:   pick(OPERATORS),
        UnitCode:       `U-${rndInt(1,50).toString().padStart(3,'0')}`,
        UnitClass:      pick(UNIT_CLASSES),
        TicketDate:     fmtDay(ticketDate),
        WorkStartTime:  fmtDate(addDays(ticketDate, 0)),
        WorkEndTime:    fmtDate(addDays(ticketDate, 0)),
        FinalizedDate:  fmtDay(finalizedDate),
        InvoicedDate:   fmtDay(invoicedDate),
        ...f,
        // Metrics
        Hours:                      Math.round(hours * 10) / 10,
        OvertimeHours:              Math.round(overtime * 10) / 10,
        BillableAmount:             Math.round(billAmt * 100) / 100,
        LaborCost:                  Math.round(laborCost * 100) / 100,
        EquipmentCost:              Math.round(equipCost * 100) / 100,
        OtherCost:                  Math.round(otherCost * 100) / 100,
        Margin:                     Math.round(margin * 100) / 100,
        DaysWorkCompleteToFinalize: daysFin,
        DaysFinalizeToInvoice:      daysFinInv,
        DaysToFinalize:             daysFin,
        DaysToInvoice:              daysFin != null && daysFinInv != null ? daysFin + daysFinInv : null,
        TicketCount:    1,
        FinalizedFlag:  isFinalized ? 1 : 0,
        InvoicedFlag:   isInvoiced ? 1 : 0,
        OnTimeStartFlag: Math.random() < 0.82 ? 1 : 0,
      });
    }
  }
  return rows;
}

// ─── Invoices ────────────────────────────────────────────────────────────
function generateInvoices(jobs) {
  const rows = [];
  let seq = 0;
  for (const job of jobs) {
    if (!job._invoiceDate) continue;
    const count = job.InvoiceCount || rndInt(1, 5);
    const totalNet = job._invoicedNet;
    const perInv   = totalNet / count;

    for (let i = 0; i < count; i++) {
      const invDate  = addDays(job._invoiceDate, i * rndInt(7, 21));
      if (invDate > NOW) continue;

      const invType  = i === count - 1 ? 'final' : (job.BillingType === 'AIA' ? 'progress' : 'TM');
      const gross    = perInv * rnd(0.88, 1.12);
      const discount = Math.random() < 0.1 ? gross * rnd(0.01, 0.05) : 0;
      const tax      = gross * 0.0;
      const retainage = job.BillingType === 'AIA' ? gross * 0.10 : 0;
      const net      = gross - discount - retainage;
      const dueDate  = addDays(invDate, 30);
      const isPaid   = job._finalPayDate && invDate < job._finalPayDate;
      const paidDate = isPaid ? addDays(invDate, rndInt(10, 45)) : null;
      const amtPaid  = isPaid ? net * rnd(0.95, 1.00) : net * rnd(0, 0.40);
      const balDue   = Math.max(0, net - amtPaid);
      const daysPastDue = !isPaid && invDate < addDays(NOW, -30) ? diffDays(dueDate, NOW) : 0;

      const f = fiscalAttrs(invDate);

      rows.push({
        PK:             `INV-${String(++seq).padStart(6,'0')}`,
        InvoiceId:      `INV-${String(seq).padStart(6,'0')}`,
        InvoiceNumber:  `I${seq}`,
        InvoiceType:    invType,
        InstanceId:     INSTANCE_ID,
        TenantId:       TENANT_ID,
        ModifiedDate:   fmtDate(paidDate || invDate),
        // Denorm
        JobCode:        job.JobCode,
        JobName:        job.JobName,
        JobType:        job.JobType,
        JobStatus:      job.JobStatus,
        BillingType:    job.BillingType,
        ProjectManager: job.ProjectManager,
        SalesRep:       job.SalesRep,
        Yard:           job.Yard,
        Region:         job.Region,
        CustomerId:     job.CustomerId,
        CustomerName:   job.CustomerName,
        CustomerSegment: job.CustomerSegment,
        CustomerRegion:  job.CustomerRegion,
        CustomerTier:    job.CustomerTier,
        InvoiceDate:    fmtDay(invDate),
        DueDate:        fmtDay(dueDate),
        PaidDate:       fmtDay(paidDate),
        ...f,
        // Metrics
        InvoiceGross:   Math.round(gross * 100) / 100,
        DiscountAmount: Math.round(discount * 100) / 100,
        TaxAmount:      0,
        RetainageHeld:  Math.round(retainage * 100) / 100,
        InvoiceNet:     Math.round(net * 100) / 100,
        LineCount:      rndInt(2, 10),
        AmountPaid:     Math.round(amtPaid * 100) / 100,
        BalanceDue:     Math.round(balDue * 100) / 100,
        InvoiceCount:   1,
        PaidFlag:       isPaid ? 1 : 0,
        VoidedFlag:     0,
        DaysIssuedToPaid: paidDate ? diffDays(invDate, paidDate) : null,
        DaysPastDue:    Math.max(0, daysPastDue),
        // internal ref
        _invDate: invDate,
        _net: net,
        _jobCode: job.JobCode,
      });
    }
  }
  return rows;
}

// ─── Invoice lines ────────────────────────────────────────────────────────
function generateInvoiceLines(invoices) {
  const rows = [];
  let seq = 0;
  for (const inv of invoices) {
    const count = inv.LineCount || rndInt(2, 8);
    for (let i = 0; i < count; i++) {
      const lt       = i === 0 ? 'equipment' : pick(LINE_TYPES);
      const billing  = inv.BillingType;
      const itemCode = pick(ITEM_CODES[lt] || ITEM_CODES.misc);
      const qty      = lt === 'equipment' ? rndInt(1, 3) : rndInt(1, 30);
      const unitP    = inv.InvoiceNet / count / qty * rnd(0.8, 1.2);
      const extended = qty * unitP;
      const cost     = extended * rnd(0.50, 0.72);
      const margin   = extended - cost;

      // Revenue split by billing type
      const isRental = billing === 'Rental';
      const isTM     = billing === 'TM';
      const isAIA    = billing === 'AIA';

      const f = fiscalAttrs(inv._invDate);

      rows.push({
        PK:              `IL-${String(++seq).padStart(7,'0')}`,
        InvoiceLineId:   `IL-${String(seq).padStart(7,'0')}`,
        InvoiceId:       inv.InvoiceId,
        InvoiceNumber:   inv.InvoiceNumber,
        InvoiceType:     inv.InvoiceType,
        InstanceId:      INSTANCE_ID,
        TenantId:        TENANT_ID,
        ModifiedDate:    inv.ModifiedDate,
        JobCode:         inv.JobCode,
        JobName:         inv.JobName,
        BillingType:     billing,
        LineType:        lt,
        CustomerId:      inv.CustomerId,
        CustomerName:    inv.CustomerName,
        CustomerSegment: inv.CustomerSegment,
        CustomerRegion:  inv.CustomerRegion,
        CustomerTier:    inv.CustomerTier,
        SalesRep:        inv.SalesRep,
        Yard:            inv.Yard,
        Region:          inv.Region,
        ItemCode:        itemCode,
        ItemDescription: itemCode.replace(/-/g,' '),
        InvoiceDate:     inv.InvoiceDate,
        ...f,
        // Metrics
        Quantity:        Math.round(qty * 10) / 10,
        UnitPrice:       Math.round(unitP * 100) / 100,
        ExtendedAmount:  Math.round(extended * 100) / 100,
        LineCost:        Math.round(cost * 100) / 100,
        LineMargin:      Math.round(margin * 100) / 100,
        RevenueRental:   isRental ? Math.round(extended * 100) / 100 : 0,
        RevenueTM:       isTM     ? Math.round(extended * 100) / 100 : 0,
        RevenueAIA:      isAIA    ? Math.round(extended * 100) / 100 : 0,
        RevenueOther:    !isRental && !isTM && !isAIA ? Math.round(extended * 100) / 100 : 0,
      });
    }
  }
  return rows;
}

// ─── Payments ────────────────────────────────────────────────────────────
function generatePayments(invoices) {
  const rows = [];
  let seq = 0;
  for (const inv of invoices) {
    if (inv.AmountPaid <= 0) continue;
    const count  = rndInt(1, 3);
    const perPmt = inv.AmountPaid / count;

    for (let p = 0; p < count; p++) {
      const pmtDate = addDays(inv._invDate, rndInt(10, 50));
      if (pmtDate > NOW) continue;

      const applied   = p === count - 1 ? inv.AmountPaid - (perPmt * p) : perPmt;
      const unapplied = Math.random() < 0.05 ? applied * rnd(0.01, 0.05) : 0;
      const total     = applied + unapplied;

      const f = fiscalAttrs(pmtDate);

      rows.push({
        PK:             `PMT-${String(++seq).padStart(6,'0')}`,
        PaymentId:      `PMT-${String(seq).padStart(6,'0')}`,
        InvoiceId:      inv.InvoiceId,
        JobCode:        inv.JobCode,
        JobName:        inv.JobName,
        InstanceId:     INSTANCE_ID,
        TenantId:       TENANT_ID,
        ModifiedDate:   fmtDate(pmtDate),
        CustomerId:     inv.CustomerId,
        CustomerName:   inv.CustomerName,
        CustomerSegment: inv.CustomerSegment,
        CustomerRegion:  inv.CustomerRegion,
        CustomerTier:    inv.CustomerTier,
        PaymentMethod:  pick(PAYMENT_METHODS),
        PaymentType:    'invoice-payment',
        SalesRep:       inv.SalesRep,
        Yard:           inv.Yard,
        Region:         inv.Region,
        PaymentDate:    fmtDay(pmtDate),
        ...f,
        AppliedAmount:  Math.round(applied * 100) / 100,
        UnappliedAmount: Math.round(unapplied * 100) / 100,
        TotalAmount:    Math.round(total * 100) / 100,
        PaymentCount:   1,
      });
    }
  }
  return rows;
}

// ─── AR Snapshot ─────────────────────────────────────────────────────────
// One row per active customer × business day for last 90 days
function generateARSnapshots(jobs) {
  const rows = [];
  let seq = 0;

  // Build a simple AR balance per customer from closed/billed jobs
  const custAR = {};
  for (const job of jobs) {
    const cid = job.CustomerId;
    if (!custAR[cid]) custAR[cid] = {
      cust: job._quoteRef._cust,
      balance: 0, ar30: 0, ar3160: 0, ar6190: 0, ar90plus: 0,
      retainage: 0, openInv: 0, oldestDpd: 0,
    };
    const ar = custAR[cid];
    const outstanding = job._invoicedNet - job._collected;
    if (outstanding <= 0) continue;
    ar.balance   += outstanding;
    ar.retainage += job.RetainageHeld;
    ar.openInv   += 1;
    // Distribute to aging buckets
    const invAge = job._invoiceDate ? diffDays(job._invoiceDate, NOW) : 0;
    if (invAge < 31)      ar.ar30    += outstanding;
    else if (invAge < 61) ar.ar3160  += outstanding;
    else if (invAge < 91) ar.ar6190  += outstanding;
    else                  ar.ar90plus += outstanding;
    ar.oldestDpd = Math.max(ar.oldestDpd, Math.max(0, invAge - 30));
  }

  // Emit daily snapshots for last 90 business days
  for (let daysAgo = 89; daysAgo >= 0; daysAgo--) {
    const snapDate = addDays(NOW, -daysAgo);
    if (snapDate.getDay() === 0 || snapDate.getDay() === 6) continue; // weekdays only
    const f = fiscalAttrs(snapDate);

    for (const [cid, ar] of Object.entries(custAR)) {
      if (ar.balance <= 0) continue;
      // Add some daily noise
      const noise = rnd(0.95, 1.05);
      const bal = ar.balance * noise;
      const dso = ar.balance > 0 ? rnd(28, 65) : 0;

      rows.push({
        PK:              `ARS-${cid}-${fmtDay(snapDate)}`,
        InstanceId:      INSTANCE_ID,
        TenantId:        TENANT_ID,
        ModifiedDate:    fmtDate(snapDate),
        CustomerId:      cid,
        CustomerName:    ar.cust.name,
        CustomerSegment: ar.cust.seg,
        CustomerRegion:  ar.cust.region,
        CustomerTier:    ar.cust.tier,
        SnapshotDate:    fmtDay(snapDate),
        OldestInvoiceDate: fmtDay(addDays(snapDate, -ar.oldestDpd - rndInt(0,10))),
        ...f,
        TotalAR:                Math.round(bal * 100) / 100,
        ARCurrent:              Math.round(ar.ar30 * noise * 100) / 100,
        AR31_60:                Math.round(ar.ar3160 * noise * 100) / 100,
        AR61_90:                Math.round(ar.ar6190 * noise * 100) / 100,
        ARover90:               Math.round(ar.ar90plus * noise * 100) / 100,
        RetainageHeld:          Math.round(ar.retainage * noise * 100) / 100,
        CreditsOnAccount:       0,
        OldestInvoiceDaysPastDue: ar.oldestDpd,
        DSO:                    Math.round(dso * 10) / 10,
        PastDueFlag:            ar.oldestDpd > 0 ? 1 : 0,
        OpenInvoiceCount:       ar.openInv,
      });
    }
  }
  return rows;
}

// ─── Q2C Funnel (wide, won-only) ──────────────────────────────────────────
function generateFunnel(jobs) {
  return jobs.map(job => {
    const q       = job._quoteRef;
    const wonDate = new Date(job.WonDate || job._wonDate);
    const quoteSentDate = q._sentDateObj;
    const closedDate    = job._closedDate;
    const startDate     = job._startDate;

    const daysLeadToQuote      = rndInt(1, 5); // lead → quote sent (no lead dataset)
    const daysQuoteToRespond   = q.DaysSentToResponded || rndInt(1, 7);
    const daysQuoteToWon       = q.DaysSentToClosed    || rndInt(3, 21);
    const daysWonToStart       = diffDays(wonDate, startDate);
    const daysStartToComplete  = diffDays(startDate, job._workComplete);
    const daysCompleteToInvoice = job._invoiceDate ? diffDays(job._workComplete, job._invoiceDate) : null;
    const daysInvoiceToPaid    = job._finalPayDate && job._invoiceDate ? diffDays(job._invoiceDate, job._finalPayDate) : null;
    const daysTotalCycle       = closedDate ? diffDays(quoteSentDate, closedDate) : null;
    const daysOpenAsOf         = !closedDate ? diffDays(startDate, NOW) : null;

    const unbilledWIP = job._ticketedRevenue - job._invoicedNet;
    const grossMarginPct = job._invoicedNet > 0 ? job.GrossMargin / job._invoicedNet : 0;
    const overtimePct    = job._laborHours > 0 ? job._overtimeHrs / job._laborHours : 0;
    const discountPct    = q._quotedValue > 0 ? (q._quotedValue - job._awardedValue) / q._quotedValue : 0;
    const estAccuracy    = job.EstimatedRevenue > 0 ? job._invoicedNet / job.EstimatedRevenue : 0;
    const quoteAccuracy  = q._quotedValue > 0 ? job._invoicedNet / q._quotedValue : 0;
    const billingLag     = job._ticketedRevenue > 0 ? Math.max(0, unbilledWIP) / job._ticketedRevenue : 0;

    // Multi-anchor fiscal
    const fWon    = fiscalAttrs(wonDate);
    const fClosed = closedDate ? fiscalAttrs(closedDate) : {};
    const fQuote  = fiscalAttrs(quoteSentDate);
    const fStart  = fiscalAttrs(startDate);

    return {
      PK:                  job.JobCode,
      JobCode:             job.JobCode,
      JobName:             job.JobName,
      WinningQuoteId:      q.QuoteId,
      WinningQuoteNumber:  q.QuoteNumber,
      InstanceId:          INSTANCE_ID,
      TenantId:            TENANT_ID,
      ModifiedDate:        fmtDate(closedDate || NOW),
      // Denorm
      CustomerId:          job.CustomerId,
      CustomerName:        job.CustomerName,
      CustomerSegment:     job.CustomerSegment,
      CustomerRegion:      job.CustomerRegion,
      CustomerTier:        job.CustomerTier,
      JobType:             job.JobType,
      BillingType:         job.BillingType,
      ProjectManager:      job.ProjectManager,
      SalesRep:            job.SalesRep,
      Yard:                job.Yard,
      Region:              job.Region,
      Source:              q.Source,
      QuoteRevisionCount:  q._revCount,
      ProductMix:          q._productMix,
      ValueBand:           valueBand(job._awardedValue),
      IsAIAJob:            job._isAIA,
      RiskFlags:           job._risks.join(','),
      // Stage flags
      HasLead:             1,
      HasQuote:            1,
      HasQuoteResponse:    q.DaysSentToResponded != null ? 1 : 0,
      IsWon:               1,
      IsScheduled:         1,
      IsInProgress:        job._isWorkComplete || job._isClosed || job.JobStatus === 'in_progress' ? 1 : 0,
      IsWorkComplete:      job._isWorkComplete || job._isClosed ? 1 : 0,
      IsBilled:            job._invoiceDate ? 1 : 0,
      IsCollected:         job._finalPayDate ? 1 : 0,
      IsClosed:            job._isClosed ? 1 : 0,
      IsCancelled:         0,
      OnTimeStartFlag:     Math.random() < 0.78 ? 1 : 0,
      OverBudgetFlag:      job._risks.includes('over_budget') ? 1 : 0,
      LowMarginFlag:       job._risks.includes('low_margin') ? 1 : 0,
      // Primary fiscal (JobStartDate)
      ...fStart,
      // Multi-anchor fiscal
      QuoteSent_FiscalYear:       fQuote.FiscalYear,
      QuoteSent_FiscalMonthLabel: fQuote.FiscalMonthLabel,
      Won_FiscalYear:             fWon.FiscalYear,
      Won_FiscalMonthLabel:       fWon.FiscalMonthLabel,
      Closed_FiscalYear:          fClosed.FiscalYear || null,
      Closed_FiscalMonthLabel:    fClosed.FiscalMonthLabel || null,
      // Stage timestamps
      LeadCreatedDate:     fmtDay(addDays(quoteSentDate, -daysLeadToQuote)),
      QuoteRequestedDate:  q.QuoteRequestedDate,
      QuoteSentDate:       fmtDay(quoteSentDate),
      QuoteRespondedDate:  q.QuoteRespondedDate,
      WonDate:             job.WonDate,
      JobCreatedDate:      job.JobCreatedDate,
      FirstScheduledDate:  job.FirstScheduledDate,
      JobStartDate:        job.JobStartDate,
      FirstTicketDate:     job.FirstTicketDate,
      WorkCompleteDate:    job.WorkCompleteDate,
      FirstInvoiceDate:    job.FirstInvoiceDate,
      FinalInvoiceDate:    job.FinalInvoiceDate,
      FirstPaymentDate:    job.FirstPaymentDate,
      FinalPaymentDate:    job.FinalPaymentDate,
      ClosedDate:          job.ClosedDate,
      // Cycle times
      DaysLeadToQuote:         daysLeadToQuote,
      DaysQuoteToRespond:      daysQuoteToRespond,
      DaysQuoteToWon:          daysQuoteToWon,
      DaysWonToStart:          daysWonToStart,
      DaysStartToComplete:     daysStartToComplete,
      DaysCompleteToInvoice:   daysCompleteToInvoice,
      DaysInvoiceToPaid:       daysInvoiceToPaid,
      DaysTotalCycle:          daysTotalCycle,
      DaysOpenAsOf:            daysOpenAsOf,
      // Amounts
      QuotedValue:         job.QuotedValue,
      AwardedValue:        job.AwardedValue,
      DiscountFromQuote:   Math.round((q._quotedValue - job._awardedValue) * 100) / 100,
      EstimatedRevenue:    job.EstimatedRevenue,
      EstimatedCost:       job.EstimatedCost,
      EstimatedMargin:     Math.round((job.EstimatedRevenue - job.EstimatedCost) * 100) / 100,
      TicketedRevenue:     job.TicketedRevenue,
      InvoicedGross:       job.InvoicedGross,
      CreditMemoAmount:    job.CreditMemoAmount,
      InvoicedNet:         job.InvoicedNet,
      CollectedAmount:     job.CollectedAmount,
      RetainageHeld:       job.RetainageHeld,
      BadDebtAmount:       job.BadDebtAmount,
      DirectLaborCost:     job.DirectLaborCost,
      DirectEquipmentCost: job.DirectEquipmentCost,
      SubrentalCost:       job.SubrentalCost,
      OtherDirectCost:     job.OtherDirectCost,
      TotalDirectCost:     job.TotalDirectCost,
      GrossMargin:         job.GrossMargin,
      UnbilledWIP:         Math.round(unbilledWIP * 100) / 100,
      LaborHours:          job.LaborHours,
      OvertimeHours:       job.OvertimeHours,
      BillableHours:       job.BillableHours,
      TicketCount:         job.TicketCount,
      InvoiceCount:        job.InvoiceCount,
      PaymentCount:        job.PaymentCount,
      // Ratios
      GrossMarginPct:      Math.round(grossMarginPct * 10000) / 10000,
      OvertimePct:         Math.round(overtimePct * 10000) / 10000,
      DiscountPct:         Math.round(discountPct * 10000) / 10000,
      EstimateAccuracyPct: Math.round(estAccuracy * 10000) / 10000,
      QuoteAccuracyPct:    Math.round(quoteAccuracy * 10000) / 10000,
      BillingLagPct:       Math.round(billingLag * 10000) / 10000,
      // AIA
      PctComplete:              job.PctComplete,
      PctBilled:                job.PctBilled,
      PctCompleteMinusBilled:   job.PctCompleteMinusBilled,
    };
  });
}

// ─── Main ─────────────────────────────────────────────────────────────────
async function main() {
  console.log('=== FCC Q2C Simulator ===');
  console.log(`API: ${API_BASE}`);
  console.log(`Backfill: ${BACKFILL_MONTHS} months\n`);

  // Step 1: Generate quote universe
  console.log('Generating quote universe...');
  const allQuotes = generateQuoteUniverse();
  const wonQuotes = allQuotes.filter(q => q.WonFlag === 1);
  console.log(`  ${allQuotes.length} total quotes, ${wonQuotes.length} won`);

  // Step 2: Generate all derived data
  console.log('Generating quote lines...');
  const quoteLines = generateQuoteLines(allQuotes);
  console.log(`  ${quoteLines.length} quote lines`);

  console.log('Generating jobs...');
  const jobs = generateJobs(wonQuotes);
  console.log(`  ${jobs.length} jobs`);

  console.log('Generating job daily rows...');
  const jobDaily = generateJobDaily(jobs);
  console.log(`  ${jobDaily.length} job-daily rows`);

  console.log('Generating tickets...');
  const tickets = generateTickets(jobs);
  console.log(`  ${tickets.length} tickets`);

  console.log('Generating invoices...');
  const invoices = generateInvoices(jobs);
  console.log(`  ${invoices.length} invoices`);

  console.log('Generating invoice lines...');
  const invoiceLines = generateInvoiceLines(invoices);
  console.log(`  ${invoiceLines.length} invoice lines`);

  console.log('Generating payments...');
  const payments = generatePayments(invoices);
  console.log(`  ${payments.length} payments`);

  console.log('Generating AR snapshots...');
  const arSnapshots = generateARSnapshots(jobs);
  console.log(`  ${arSnapshots.length} AR snapshot rows`);

  console.log('Generating Q2C funnel...');
  const funnel = generateFunnel(jobs);
  console.log(`  ${funnel.length} funnel rows`);

  // Strip internal _* fields before ingest
  function clean(rows) {
    return rows.map(r => {
      const out = {};
      for (const [k,v] of Object.entries(r)) {
        if (!k.startsWith('_')) out[k] = v;
      }
      return out;
    });
  }

  // Step 3: Ingest all datasets
  console.log('\n--- Ingesting ---');
  const datasets = [
    { name: 'quote',        rows: clean(allQuotes) },
    { name: 'quote_line',   rows: clean(quoteLines) },
    { name: 'job',          rows: clean(jobs) },
    { name: 'job_daily',    rows: clean(jobDaily) },
    { name: 'ticket',       rows: clean(tickets) },
    { name: 'invoice',      rows: clean(invoices) },
    { name: 'invoice_line', rows: clean(invoiceLines) },
    { name: 'payment',      rows: clean(payments) },
    { name: 'ar_snapshot',  rows: clean(arSnapshots) },
    { name: 'q2c_funnel',   rows: clean(funnel) },
  ];

  const summary = [];
  for (const ds of datasets) {
    try {
      const r = await ingestAll(ds.name, ds.rows, ds.name);
      summary.push({ dataset: ds.name, rows: ds.rows.length, indexed: r.indexed, failed: r.failed });
    } catch(e) {
      console.error(`\n  ERROR ingesting ${ds.name}: ${e.message}`);
      summary.push({ dataset: ds.name, rows: ds.rows.length, indexed: 0, failed: ds.rows.length, error: e.message });
    }
  }

  console.log('\n=== Summary ===');
  for (const s of summary) {
    const status = s.error ? '✗ ERROR' : s.failed > 0 ? '⚠ PARTIAL' : '✓';
    console.log(`  ${status}  ${s.dataset.padEnd(15)} ${s.indexed}/${s.rows} indexed${s.error ? ' — ' + s.error : ''}`);
  }
  console.log('\nDone.');
}

main().catch(e => { console.error(e); process.exit(1); });
