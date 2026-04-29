#!/usr/bin/env node
/**
 * fcc-simulate-equipment.js
 *
 * Generates realistic synthetic data for the FCC equipment instance:
 *   - equipment:       50 units, one row per unit (current state + lifetime rollups)
 *   - equipment_daily: one row per unit × calendar day for the last 24 months
 *
 * Posts directly to the DDNL ingest API using bulk upsert.
 * Respects ModifiedDate conditional upsert — safe to re-run.
 */

'use strict';

const https = require('https');
const http  = require('http');
const url   = require('url');

// ─── Config ──────────────────────────────────────────────────────────────────
const API_BASE    = process.env.API_BASE    || 'https://fcc-app-production.up.railway.app';
const API_KEY     = process.env.API_KEY     || 'ik_fcc_2a98cd7b986ff20ed9bff0fa1aed9644b2621e0b124a1646bbd9c41683a44944';
const INSTANCE_ID = process.env.INSTANCE_ID || 'fcc';
const TENANT_ID   = 'fcc';  // FCC's own client ID

const BATCH_SIZE      = 200;   // rows per ingest POST
const UNIT_COUNT      = 50;
const BACKFILL_MONTHS = 24;    // daily rows going back 24 months

// ─── Reference data ──────────────────────────────────────────────────────────
const UNIT_CLASSES   = ['crawler', 'mobile', 'tower', 'rough-terrain', 'boom-truck'];
const UNIT_TYPES     = {
  crawler:         ['Crawler Crane', 'Lattice Boom Crawler'],
  mobile:          ['All Terrain', 'Hydraulic Truck Crane'],
  tower:           ['Flat Top Tower', 'Luffing Jib Tower'],
  'rough-terrain': ['Rough Terrain Crane', 'RT Carry Deck'],
  'boom-truck':    ['Boom Truck', 'Knuckle Boom'],
};
const MAKES  = ['Liebherr', 'Manitowoc', 'Tadano', 'Grove', 'Link-Belt', 'Terex', 'Kobelco'];
const YARDS  = ['Seattle', 'Portland', 'Spokane', 'Tacoma', 'Vancouver'];
const REGIONS = { Seattle: 'West', Portland: 'West', Spokane: 'Interior', Tacoma: 'West', Vancouver: 'West' };
const OWNERSHIP = ['owned', 'leased', 'subrented'];
const STATUSES   = ['active', 'parked', 'down', 'transferred'];
const PM_STATUSES = ['current', 'due-soon', 'past-due'];
const COMPLIANCE  = ['current', '90-day', '60-day', '30-day', 'expired'];
const PAYBACK     = ['pre-payback', 'paid-back', 'profit'];
const BILLING_TYPES = ['Rental', 'TM', 'AIA', 'Parts'];
const JOB_CUSTOMERS = ['Apex Construction', 'NW Steel', 'Pacific Builders', 'Cascade Civil',
                        'Summit Structural', 'Rainier Projects', 'Olympic Heavy'];

// ─── Helpers ─────────────────────────────────────────────────────────────────
function rnd(a, b)       { return a + Math.random() * (b - a); }
function rndInt(a, b)    { return Math.floor(rnd(a, b + 1)); }
function pick(arr)       { return arr[Math.floor(Math.random() * arr.length)]; }
function fmtDate(d)      { return d.toISOString().replace('T', ' ').slice(0, 19); }
function fmtDay(d)       { return d.toISOString().slice(0, 10); }

function addDays(d, n) {
  const r = new Date(d);
  r.setDate(r.getDate() + n);
  return r;
}

function addMonths(d, n) {
  const r = new Date(d);
  r.setMonth(r.getMonth() + n);
  return r;
}

function fiscalAttrs(date) {
  const y = date.getFullYear();
  const m = date.getMonth() + 1;
  const fyStartMonth = 1;
  const fiscalYear   = (m >= fyStartMonth) ? y : y - 1;
  const fiscalMonth  = ((m - fyStartMonth + 12) % 12) + 1;
  const fiscalQtr    = Math.ceil(fiscalMonth / 3);
  const fyStart      = new Date(fiscalYear, fyStartMonth - 1, 1);
  const daysSinceStart = Math.floor((date - fyStart) / 86400000);
  const fiscalWeek   = Math.floor(daysSinceStart / 7) + 1;
  const dow          = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'][date.getDay()];
  const isWeekend    = date.getDay() === 0 || date.getDay() === 6;
  return {
    FiscalYear:       fiscalYear,
    FiscalQuarter:    `Q${fiscalQtr}`,
    FiscalMonth:      fiscalMonth,
    FiscalMonthLabel: `${fiscalYear}-${String(fiscalMonth).padStart(2,'0')}`,
    FiscalWeek:       fiscalWeek,
    FiscalDayOfWeek:  dow,
    FiscalDayOfMonth: date.getDate(),
    CalendarYear:     y,
    CalendarMonth:    m,
    IsWeekend:        isWeekend,
    IsHoliday:        false,
  };
}

function tonnageBucket(t) {
  if (t <= 50)  return '0-50T';
  if (t <= 100) return '50-100T';
  if (t <= 250) return '100-250T';
  return '250T+';
}

function ageBucket(years) {
  if (years <= 3)  return '0-3';
  if (years <= 7)  return '4-7';
  if (years <= 15) return '8-15';
  return '15+';
}

// API request helper with retry
async function apiPost(path, body, retries = 3) {
  const fullUrl = `${API_BASE}${path}`;
  const parsed  = url.parse(fullUrl);
  const payload = JSON.stringify(body);
  const lib     = parsed.protocol === 'https:' ? https : http;

  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const result = await new Promise((resolve, reject) => {
        const opts = {
          hostname: parsed.hostname,
          port:     parsed.port || (parsed.protocol === 'https:' ? 443 : 80),
          path:     parsed.path,
          method:   'POST',
          headers: {
            'Content-Type':  'application/json',
            'Content-Length': Buffer.byteLength(payload),
            'X-Api-Key':     API_KEY,
            'X-Instance-Id': INSTANCE_ID,
            'Authorization': 'Basic ' + Buffer.from('ddnl:ddnl!').toString('base64'),
          },
        };
        const req = lib.request(opts, (res) => {
          let data = '';
          res.on('data', c => data += c);
          res.on('end', () => {
            try { resolve({ status: res.statusCode, body: JSON.parse(data) }); }
            catch(e) { resolve({ status: res.statusCode, body: data }); }
          });
        });
        req.on('error', reject);
        req.setTimeout(30000, () => { req.destroy(new Error('timeout')); });
        req.write(payload);
        req.end();
      });
      return result;
    } catch (err) {
      if (attempt === retries) throw err;
      console.warn(`    retry ${attempt}/${retries}: ${err.message}`);
      await new Promise(r => setTimeout(r, 1000 * attempt));
    }
  }
}

async function ingestBatch(dataset, docs) {
  const res = await apiPost(`/api/ingest/${dataset}/bulk`, {
    docs,
    replace: false,
  });
  if (res.status !== 200 || !res.body?.success) {
    throw new Error(`Ingest failed [${res.status}]: ${JSON.stringify(res.body)}`);
  }
  return res.body;
}

async function ingestAll(dataset, docs) {
  let indexed = 0;
  let failed  = 0;
  for (let i = 0; i < docs.length; i += BATCH_SIZE) {
    const batch = docs.slice(i, i + BATCH_SIZE);
    process.stdout.write(`\r  ${dataset}: ${Math.min(i + BATCH_SIZE, docs.length)}/${docs.length} rows...`);
    const r = await ingestBatch(dataset, batch);
    indexed += r.indexed  || 0;
    failed  += r.failed   || 0;
  }
  process.stdout.write('\n');
  return { indexed, failed };
}

// ─── Generate units ───────────────────────────────────────────────────────────
function generateUnits() {
  const units = [];
  const now   = new Date();

  for (let i = 1; i <= UNIT_COUNT; i++) {
    const unitClass   = pick(UNIT_CLASSES);
    const unitType    = pick(UNIT_TYPES[unitClass]);
    const make        = pick(MAKES);
    const tonnage     = rndInt(20, 400);
    const yard        = pick(YARDS);
    const region      = REGIONS[yard];
    const ownership   = pick(OWNERSHIP);
    const acqYearsAgo = rndInt(1, 18);
    const acqYear     = now.getFullYear() - acqYearsAgo;
    const acqDate     = new Date(acqYear, rndInt(0, 11), rndInt(1, 28));
    const ageYears    = Math.floor((now - acqDate) / (365.25 * 86400 * 1000));
    const acqCost     = Math.round(rnd(80_000, 2_500_000) / 1000) * 1000;
    const bookValue   = Math.round(acqCost * Math.max(0.05, 1 - ageYears * 0.07));
    const engHours    = rndInt(500, 25000);
    const pmInterval  = 500; // hours
    const hrsSinceLastPM = rndInt(0, pmInterval + 200);
    const pmStatus    = hrsSinceLastPM > pmInterval + 100 ? 'past-due'
                      : hrsSinceLastPM > pmInterval - 50  ? 'due-soon' : 'current';

    // Compliance
    const inspDaysUntil  = rndInt(-30, 365);
    const insurDaysUntil = rndInt(-30, 365);
    const regDaysUntil   = rndInt(-30, 365);
    const worstDays = Math.min(inspDaysUntil, insurDaysUntil, regDaysUntil);
    const compStatus = worstDays < 0 ? 'expired'
                     : worstDays <= 30  ? '30-day'
                     : worstDays <= 60  ? '60-day'
                     : worstDays <= 90  ? '90-day' : 'current';

    // Activity
    const currentStatus = pick(['active','active','active','parked','down','transferred']);
    const daysSinceLastUse = currentStatus === 'active' ? rndInt(0, 5) : rndInt(5, 120);
    const idleFlag = daysSinceLastUse > 14;

    // Lifetime metrics (will be consistent with daily data generated later)
    const lifeRevPerDay = rnd(300, 1800);
    const lifetimeDays  = Math.floor((now - acqDate) / 86400000);
    const utilRate      = rnd(0.45, 0.82);
    const lifetimeAssignedDays  = Math.round(lifetimeDays * utilRate);
    const lifetimeBillableHours = Math.round(lifetimeAssignedDays * rnd(6, 9));
    const lifetimeRevenue       = Math.round(lifetimeAssignedDays * lifeRevPerDay);
    const lifetimeEngHours      = Math.round(lifetimeDays * rnd(3, 8));
    const lifetimeMaintenanceCost = Math.round(lifetimeRevenue * rnd(0.10, 0.22));
    const lifetimeFailures      = Math.round(lifetimeDays / rnd(60, 180));

    // YTD (current fiscal year)
    const fyStart      = new Date(now.getFullYear(), 0, 1);
    const ytdDays      = Math.floor((now - fyStart) / 86400000);
    const ytdAssigned  = Math.round(ytdDays * utilRate);
    const ytdRevenue   = Math.round(ytdAssigned * lifeRevPerDay);
    const ytdBillableHours  = Math.round(ytdAssigned * rnd(6, 9));
    const ytdAvailableHours = ytdDays * 8;
    const ytdMaintenanceCost = Math.round(ytdRevenue * rnd(0.10, 0.22));

    // Payback
    const totalCost = acqCost + lifetimeMaintenanceCost;
    const paybackStatus = lifetimeRevenue < totalCost * 0.5 ? 'pre-payback'
                        : lifetimeRevenue < totalCost        ? 'paid-back' : 'profit';

    // Current job (if active)
    const currentJob     = currentStatus === 'active' ? `JOB-${1000 + rndInt(1, 200)}` : null;
    const currentCustomer = currentJob ? pick(JOB_CUSTOMERS) : null;

    // Compliance expiring count
    const expiringCount = [inspDaysUntil, insurDaysUntil, regDaysUntil]
      .filter(d => d >= 0 && d <= 30).length;

    const fiscal = fiscalAttrs(now);

    const unitCode = `FCC-${String(i).padStart(3,'0')}`;

    units.push({
      PK:                    `${INSTANCE_ID}_${TENANT_ID}_${unitCode}`,
      UnitCode:              unitCode,
      InstanceId:            INSTANCE_ID,
      TenantId:              TENANT_ID,
      ModifiedDate:          fmtDate(now),

      UnitType:              unitType,
      UnitClass:             unitClass,
      Tonnage:               tonnage,
      TonnageBucket:         tonnageBucket(tonnage),
      Make:                  make,
      Model:                 `${make} ${tonnage}T`,
      SerialNumber:          `SN${String(100000 + i).slice(1)}`,
      Company:               'FCC',
      Yard:                  yard,
      Region:                region,
      OwnershipType:         ownership,
      AcquisitionYear:       acqYear,
      AgeYears:              ageYears,
      AgeBucket:             ageBucket(ageYears),
      Description:           `${unitType} — ${tonnage}T capacity`,

      CurrentStatus:         currentStatus,
      CurrentJobCode:        currentJob,
      CurrentJobCustomer:    currentCustomer,
      CurrentYard:           yard,
      LastAssignmentStartDate: currentJob ? fmtDay(addDays(now, -rndInt(0, 30))) : null,
      LastAssignmentEndDate:   currentJob ? null : fmtDay(addDays(now, -daysSinceLastUse)),
      DaysSinceLastUse:      daysSinceLastUse,
      IdleFlag:              idleFlag,

      EngineHoursLatest:     engHours,
      OdometerLatest:        Math.round(engHours * rnd(1.2, 2.5)),
      LastTelemetryDate:     fmtDay(addDays(now, -rndInt(0, 3))),
      HoursSinceLastPM:      hrsSinceLastPM,
      PMStatus:              pmStatus,

      AnnualInspectionExpDate: fmtDay(addDays(now, inspDaysUntil)),
      DaysUntilInspectionExp:  inspDaysUntil,
      InsurancePolicy:         `POL-${1000 + i}`,
      InsuranceExpDate:        fmtDay(addDays(now, insurDaysUntil)),
      DaysUntilInsuranceExp:   insurDaysUntil,
      RegistrationExpDate:     fmtDay(addDays(now, regDaysUntil)),
      DaysUntilRegistrationExp: regDaysUntil,
      ComplianceStatus:        compStatus,
      ComplianceExpiringCount: expiringCount,

      AcquisitionDate:       fmtDay(acqDate),
      AcquisitionCost:       acqCost,
      BookValue:             bookValue,
      ExpectedLifeYears:     25,
      DisposalDate:          null,
      DisposalValue:         0,

      LifetimeRevenue:       lifetimeRevenue,
      LifetimeEngineHours:   lifetimeEngHours,
      LifetimeBillableHours: lifetimeBillableHours,
      LifetimeAssignedDays:  lifetimeAssignedDays,
      LifetimeMaintenanceCost: lifetimeMaintenanceCost,
      LifetimeFailures:      lifetimeFailures,

      YTDRevenue:            ytdRevenue,
      YTDBillableHours:      ytdBillableHours,
      YTDAvailableHours:     ytdAvailableHours,
      YTDMaintenanceCost:    ytdMaintenanceCost,
      PaybackStatus:         paybackStatus,

      CurrentFiscalYear:       fiscal.FiscalYear,
      CurrentFiscalQuarter:    fiscal.FiscalQuarter,
      CurrentFiscalMonth:      fiscal.FiscalMonth,
      CurrentFiscalMonthLabel: fiscal.FiscalMonthLabel,
    });
  }
  return units;
}

// ─── Generate daily rows ──────────────────────────────────────────────────────
function generateDailyRows(units) {
  const rows   = [];
  const today  = new Date();
  today.setHours(0, 0, 0, 0);
  const startDate = addMonths(today, -BACKFILL_MONTHS);

  // Pre-build a job schedule per unit (sparse, ~65% utilization on weekdays)
  // Each "job" spans 5-45 days
  const unitJobSchedules = {};
  for (const u of units) {
    const schedule = []; // array of {start, end, jobCode, customer, billingType}
    const acqDate  = new Date(u.AcquisitionDate);
    let d = acqDate > startDate ? acqDate : new Date(startDate);
    while (d < today) {
      if (Math.random() < 0.68) {
        // Unit gets a job
        const jobLen = rndInt(5, 45);
        const jobEnd = addDays(d, jobLen);
        schedule.push({
          start: new Date(d),
          end:   jobEnd < today ? jobEnd : today,
          jobCode:     `JOB-${1000 + rndInt(1, 500)}`,
          customer:    pick(JOB_CUSTOMERS),
          billingType: pick(BILLING_TYPES),
          dailyRevenue: rnd(400, 2200),
        });
        d = addDays(jobEnd, rndInt(1, 7)); // gap between jobs
      } else {
        d = addDays(d, rndInt(3, 14)); // idle stretch
      }
    }
    unitJobSchedules[u.UnitCode] = schedule;
  }

  for (const u of units) {
    const acqDate  = new Date(u.AcquisitionDate);
    const schedule = unitJobSchedules[u.UnitCode];

    let d = acqDate > startDate ? new Date(acqDate) : new Date(startDate);
    d.setHours(0, 0, 0, 0);

    // Track engine hours (simple daily delta)
    let engineHoursAccum = Math.max(0, u.EngineHoursLatest - rndInt(200, 2000));

    while (d < today) {
      const dateStr  = fmtDay(d);
      const fiscal   = fiscalAttrs(d);
      const isWeekend = fiscal.IsWeekend;

      // Find active job for this day
      const job = schedule.find(j => d >= j.start && d < j.end);

      // Activity flags
      let assignedFlag  = 0;
      let idleFlag      = 0;
      let transferFlag  = 0;
      let downtimeFlag  = 0;
      let primaryStatus = 'idle';
      let jobCode       = null;
      let customer      = null;
      let billingType   = null;
      let billableHours = 0;
      let revenueHours  = 0;
      let revenue       = 0;
      let revenueRental = 0;
      let revenueTM     = 0;
      let revenueAIA    = 0;
      let revenueOther  = 0;
      let downtimeHours = 0;
      let scheduledDowntime   = 0;
      let unscheduledDowntime = 0;
      let maintenanceCost = 0;
      let partsCost       = 0;
      let laborCost       = 0;
      let pmsScheduled    = 0;
      let pmsOnTime       = 0;
      let pmsLate         = 0;
      let unschedRepairs  = 0;
      let failureEvents   = 0;
      let woOpened = 0;
      let woClosed = 0;

      if (job) {
        // Unit is assigned
        if (!isWeekend && Math.random() > 0.05) {
          assignedFlag = 1;
          primaryStatus = 'active';
          jobCode      = job.jobCode;
          customer     = job.customer;
          billingType  = job.billingType;
          billableHours = rnd(6, 10);
          revenueHours  = billableHours * rnd(0.9, 1.0);
          revenue       = Math.round(job.dailyRevenue * rnd(0.85, 1.15));

          // Split by billing type
          if (billingType === 'Rental')   revenueRental = revenue;
          else if (billingType === 'TM')  revenueTM     = revenue;
          else if (billingType === 'AIA') revenueAIA    = revenue;
          else                            revenueOther  = revenue;

          // Occasional breakdown on active unit
          if (Math.random() < 0.02) {
            downtimeFlag = 1;
            primaryStatus = 'down';
            unscheduledDowntime = rnd(2, 8);
            downtimeHours = unscheduledDowntime;
            failureEvents = 1;
            maintenanceCost = Math.round(rnd(500, 8000));
            laborCost = Math.round(maintenanceCost * rnd(0.4, 0.6));
            partsCost = maintenanceCost - laborCost;
            unschedRepairs = 1;
            woOpened = 1;
            woClosed = Math.random() > 0.3 ? 1 : 0;
            billableHours = Math.max(0, billableHours - unscheduledDowntime);
            revenue = Math.round(revenue * (billableHours / (billableHours + unscheduledDowntime)));
          }
        } else if (isWeekend) {
          // Weekend — unit is parked at job site
          assignedFlag = 1;
          primaryStatus = 'active';
          jobCode  = job.jobCode;
          customer = job.customer;
          billingType = job.billingType;
          billableHours = 0;
          revenue = 0;
        }
      } else {
        // No active job
        if (Math.random() < 0.03) {
          // Transfer day
          transferFlag  = 1;
          primaryStatus = 'transfer';
        } else if (Math.random() < 0.04) {
          // Scheduled PM
          downtimeFlag = 1;
          primaryStatus = 'down';
          scheduledDowntime = rnd(4, 8);
          downtimeHours = scheduledDowntime;
          maintenanceCost = Math.round(rnd(200, 1500));
          laborCost = Math.round(maintenanceCost * 0.5);
          partsCost = maintenanceCost - laborCost;
          pmsScheduled = 1;
          pmsOnTime = Math.random() > 0.15 ? 1 : 0;
          pmsLate   = 1 - pmsOnTime;
          woOpened  = 1;
          woClosed  = 1;
        } else {
          idleFlag = 1;
          primaryStatus = 'idle';
        }
      }

      // Engine hours delta (only on active / maintenance days)
      const engDelta    = (assignedFlag || downtimeFlag) ? rnd(4, 12) : 0;
      engineHoursAccum += engDelta;
      const odomDelta   = assignedFlag ? rnd(0, 30) : 0;
      const fuelGallons = assignedFlag ? rnd(10, 50) : (downtimeFlag ? rnd(2, 10) : 0);

      const availableHours = 8; // standard 8hr day
      const targetHours    = isWeekend ? 0 : 8;
      const targetRevenue  = isWeekend ? 0 : Math.round(rnd(500, 1500));

      const activeFlag    = assignedFlag || downtimeFlag || transferFlag ? 1 : 0;
      const inServiceFlag = 1; // all units in service during simulation window

      rows.push({
        PK:         `${u.UnitCode}_${dateStr.replace(/-/g,'')}`,
        UnitCode:   u.UnitCode,
        FiscalDate: dateStr,
        InstanceId: INSTANCE_ID,
        TenantId:   TENANT_ID,
        ModifiedDate: fmtDate(new Date()),

        // Denormalized unit attributes
        UnitType:      u.UnitType,
        UnitClass:     u.UnitClass,
        Tonnage:       u.Tonnage,
        TonnageBucket: u.TonnageBucket,
        Make:          u.Make,
        Model:         u.Model,
        Company:       u.Company,
        Yard:          u.Yard,
        Region:        u.Region,
        OwnershipType: u.OwnershipType,
        AgeBucket:     u.AgeBucket,
        AcquisitionYear: u.AcquisitionYear,

        // Fiscal
        ...fiscal,

        // Daily activity
        PrimaryJobCode:     jobCode,
        PrimaryJobCustomer: customer,
        PrimaryStatus:      primaryStatus,
        BillingType:        billingType,

        // Availability & utilization
        DayFlag:         1,
        AvailableHours:  availableHours,
        TargetHours:     targetHours,
        BillableHours:   Math.round(billableHours * 10) / 10,
        RevenueHours:    Math.round(revenueHours * 10) / 10,
        AssignedFlag:    assignedFlag,
        IdleFlag:        idleFlag,
        TransferFlag:    transferFlag,

        // Downtime
        DowntimeFlag:             downtimeFlag,
        DowntimeHours:            Math.round(downtimeHours * 10) / 10,
        ScheduledDowntimeHours:   Math.round(scheduledDowntime * 10) / 10,
        UnscheduledDowntimeHours: Math.round(unscheduledDowntime * 10) / 10,

        // Revenue
        Revenue:       Math.round(revenue),
        TargetRevenue: Math.round(targetRevenue),
        RevenueRental: Math.round(revenueRental),
        RevenueTM:     Math.round(revenueTM),
        RevenueAIA:    Math.round(revenueAIA),
        RevenueOther:  Math.round(revenueOther),

        // Maintenance
        MaintenanceCost:   Math.round(maintenanceCost),
        PartsCost:         Math.round(partsCost),
        LaborCost:         Math.round(laborCost),
        PMsScheduled:      pmsScheduled,
        PMsCompletedOnTime: pmsOnTime,
        PMsCompletedLate:  pmsLate,
        UnscheduledRepairs: unschedRepairs,
        FailureEvents:     failureEvents,
        WorkOrdersOpened:  woOpened,
        WorkOrdersClosed:  woClosed,

        // Telemetry
        EngineHoursDelta: Math.round(engDelta * 10) / 10,
        OdometerMilesDelta: Math.round(odomDelta * 10) / 10,
        FuelGallons:      Math.round(fuelGallons * 10) / 10,

        // Fleet sizing
        ActiveFlag:     activeFlag,
        InServiceFlag:  inServiceFlag,
      });

      d = addDays(d, 1);
    }
  }

  return rows;
}

// ─── Main ─────────────────────────────────────────────────────────────────────
async function main() {
  console.log(`FCC Equipment Simulator`);
  console.log(`  API: ${API_BASE}`);
  console.log(`  Instance: ${INSTANCE_ID} / Tenant: ${TENANT_ID}`);
  console.log(`  Units: ${UNIT_COUNT}`);
  console.log(`  Backfill: ${BACKFILL_MONTHS} months`);
  console.log('');

  // ── Generate equipment rows ─────────────────────────────────────────────────
  console.log(`Generating ${UNIT_COUNT} equipment rows...`);
  const units = generateUnits();
  console.log(`  Generated ${units.length} units`);

  // ── Generate equipment_daily rows ───────────────────────────────────────────
  console.log(`Generating equipment_daily rows (~${UNIT_COUNT * 30 * BACKFILL_MONTHS} estimated)...`);
  const dailyRows = generateDailyRows(units);
  console.log(`  Generated ${dailyRows.length.toLocaleString()} daily rows`);

  // ── Ingest equipment ────────────────────────────────────────────────────────
  console.log('\nIngesting equipment...');
  try {
    const r1 = await ingestAll('equipment', units);
    console.log(`  equipment: ${r1.indexed} indexed, ${r1.failed} failed`);
  } catch (e) {
    console.error(`  ERROR: ${e.message}`);
    process.exit(1);
  }

  // ── Ingest equipment_daily ──────────────────────────────────────────────────
  console.log('Ingesting equipment_daily...');
  try {
    const r2 = await ingestAll('equipment_daily', dailyRows);
    console.log(`  equipment_daily: ${r2.indexed} indexed, ${r2.failed} failed`);
  } catch (e) {
    console.error(`  ERROR: ${e.message}`);
    process.exit(1);
  }

  console.log('\nSimulation complete.');
}

main().catch(e => { console.error('FATAL:', e.message); process.exit(1); });
