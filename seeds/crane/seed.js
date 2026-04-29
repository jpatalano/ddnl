#!/usr/bin/env node
/**
 * FCC instance seed script
 * - Creates fiscal calendar (Jan 1 start, standard ISO week)
 * - Populates fiscal_days for 2023-01-01 → 2027-12-31
 * - Creates equipment + equipment_daily dataset definitions
 * - Creates computed field rules for fiscal stamps + order_pk pattern
 */

const { Pool } = require('pg');

const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) { console.error('DATABASE_URL required'); process.exit(1); }

const pool = new Pool({ connectionString: DATABASE_URL, ssl: false });

const CLIENT_ID  = 'fcc';
const FY_START_MONTH = 1; // January
const DATE_FROM  = new Date('2023-01-01');
const DATE_TO    = new Date('2027-12-31');

// ─── Fiscal day computation ────────────────────────────────────────────────

function fiscalAttrs(date, fyStartMonth) {
  const y = date.getFullYear();
  const m = date.getMonth() + 1; // 1-based

  // Fiscal year: if fyStartMonth=1, FY=calendar year
  // If fyStartMonth>1, FY starts partway through calendar year
  let fiscalYear = (m >= fyStartMonth) ? y : y - 1;

  // Fiscal month (1-12, relative to FY start)
  let fiscalMonth = ((m - fyStartMonth + 12) % 12) + 1;

  // Fiscal quarter
  const fiscalQuarter = Math.ceil(fiscalMonth / 3);

  // ISO week
  const jan4 = new Date(y, 0, 4);
  const startOfWeek1 = new Date(jan4);
  startOfWeek1.setDate(jan4.getDate() - ((jan4.getDay() + 6) % 7));
  const daysSinceW1 = Math.floor((date - startOfWeek1) / 86400000);
  let isoWeek = Math.floor(daysSinceW1 / 7) + 1;
  let isoWeekYear = y;
  if (isoWeek < 1) { isoWeekYear = y - 1; isoWeek = isoWeeksInYear(y - 1); }
  else if (isoWeek > isoWeeksInYear(y)) { isoWeekYear = y + 1; isoWeek = 1; }

  // Week start (Monday)
  const dow = (date.getDay() + 6) % 7; // 0=Mon
  const wStart = new Date(date); wStart.setDate(date.getDate() - dow);

  // Fiscal week start within fiscal year
  const fyStart = new Date(fiscalYear, fyStartMonth - 1, 1);
  const daysSinceFYStart = Math.floor((date - fyStart) / 86400000);
  const fiscalWeek = Math.floor(daysSinceFYStart / 7) + 1;
  const fwStart = new Date(fyStart); fwStart.setDate(fyStart.getDate() + (fiscalWeek - 1) * 7);

  // Month start/end
  const monthStart = new Date(y, m - 1, 1);
  const monthEnd   = new Date(y, m, 0);

  // Fiscal month start/end
  const fmStartMonth = ((fyStartMonth - 1 + fiscalMonth - 1) % 12) + 1;
  const fmStartYear  = fiscalYear + Math.floor((fyStartMonth - 1 + fiscalMonth - 1) / 12);
  const fmStart = new Date(fmStartYear, fmStartMonth - 1, 1);
  const fmEnd   = new Date(fmStartYear, fmStartMonth, 0);

  // Quarter start/end
  const qStartFiscalMonth = (fiscalQuarter - 1) * 3 + 1;
  const qStartCalMonth = ((fyStartMonth - 1 + qStartFiscalMonth - 1) % 12) + 1;
  const qStartCalYear  = fiscalYear + Math.floor((fyStartMonth - 1 + qStartFiscalMonth - 1) / 12);
  const qStart = new Date(qStartCalYear, qStartCalMonth - 1, 1);
  const qEnd   = new Date(qStartCalYear, qStartCalMonth + 2, 0);

  // FY start/end
  const fyEnd = new Date(fiscalYear + 1, fyStartMonth - 1, 0);

  // Day of week
  const dayNames    = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
  const dayShorts   = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
  const monthNames  = ['January','February','March','April','May','June','July','August','September','October','November','December'];
  const monthShorts = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];

  const isWeekend = date.getDay() === 0 || date.getDay() === 6;

  // Same day last year (simple -365 or -364)
  const sdly = new Date(date); sdly.setFullYear(date.getFullYear() - 1);

  const fmt = d => d.toISOString().slice(0, 10);

  return {
    day_of_week:          date.getDay() + 1, // 1=Sun
    day_name:             dayNames[date.getDay()],
    day_name_short:       dayShorts[date.getDay()],
    is_weekend:           isWeekend,
    is_holiday:           false,
    iso_week:             isoWeek,
    iso_week_year:        isoWeekYear,
    iso_week_start:       fmt(wStart),
    iso_week_label:       `IW${String(isoWeek).padStart(2,'0')}`,
    fiscal_week:          fiscalWeek,
    fiscal_week_label:    `FW${String(fiscalWeek).padStart(2,'0')}`,
    fiscal_week_start:    fmt(fwStart),
    fiscal_month:         fiscalMonth,
    fiscal_month_name:    monthNames[m - 1],
    fiscal_month_short:   monthShorts[m - 1],
    fiscal_month_label:   `${fiscalYear}-${String(fiscalMonth).padStart(2,'0')}`,
    fiscal_month_start:   fmt(fmStart),
    fiscal_month_end:     fmt(fmEnd),
    fiscal_quarter:       fiscalQuarter,
    fiscal_quarter_label: `Q${fiscalQuarter}`,
    fiscal_quarter_start: fmt(qStart),
    fiscal_quarter_end:   fmt(qEnd),
    fiscal_year:          fiscalYear,
    fiscal_year_label:    `FY${fiscalYear}`,
    fiscal_year_start:    fmt(new Date(fiscalYear, fyStartMonth - 1, 1)),
    fiscal_year_end:      fmt(fyEnd),
    same_day_last_year:   fmt(sdly),
    same_week_start_lyr:  fmt(new Date(sdly.getTime() - ((sdly.getDay() + 6) % 7) * 86400000)),
    same_month_start_lyr: fmt(new Date(sdly.getFullYear(), sdly.getMonth(), 1)),
    same_quarter_start_lyr: fmt(new Date(sdly.getFullYear(), Math.floor(sdly.getMonth() / 3) * 3, 1)),
  };
}

function isoWeeksInYear(y) {
  // A year has 53 ISO weeks if Jan 1 or Dec 31 is Thursday
  const jan1dow = new Date(y, 0, 1).getDay(); // 0=Sun
  const dec31dow = new Date(y, 11, 31).getDay();
  return (jan1dow === 4 || dec31dow === 4) ? 53 : 52;
}

function addDays(date, n) {
  const d = new Date(date); d.setDate(d.getDate() + n); return d;
}

// ─── Main ──────────────────────────────────────────────────────────────────

async function main() {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // 1. Ensure fcc client row exists
    console.log('Seeding fcc client...');
    await client.query(
      `INSERT INTO clients (client_id, name) VALUES ($1, $2) ON CONFLICT (client_id) DO NOTHING`,
      [CLIENT_ID, 'FCC']
    );

    // 2. Create fiscal calendar
    console.log('Creating fiscal calendar...');
    const { rows: [cal] } = await client.query(
      `INSERT INTO fiscal_calendars
         (client_id, name, fiscal_year_start_month, fiscal_year_start_day, week_start_day, week_scheme, description, is_active)
       VALUES ($1, 'Default', $2, 1, 2, 'iso', 'Standard January fiscal year, ISO weeks', true)
       ON CONFLICT (client_id, name) DO UPDATE
         SET fiscal_year_start_month=EXCLUDED.fiscal_year_start_month, updated_at=NOW()
       RETURNING id`,
      [CLIENT_ID, FY_START_MONTH]
    );
    const calId = cal.id;
    console.log(`  calendar id: ${calId}`);

    // 3. Populate fiscal_days
    console.log(`Populating fiscal_days ${DATE_FROM.toISOString().slice(0,10)} → ${DATE_TO.toISOString().slice(0,10)}...`);
    let d = new Date(DATE_FROM);
    let count = 0;
    const BATCH = 500;
    let batch = [];

    const flush = async () => {
      if (!batch.length) return;
      const COLS = 33;
      const placeholders = batch.map((_, i) => {
        const b = i * COLS;
        return `(${ Array.from({length:COLS}, (_,j) => `$${b+j+1}`).join(',') })`;
      }).join(',');
      const values = batch.flat();
      await client.query(
        `INSERT INTO fiscal_days (
          calendar_id, calendar_date,
          day_of_week, day_name, day_name_short, is_weekend, is_holiday,
          iso_week, iso_week_year, iso_week_start, iso_week_label,
          fiscal_week, fiscal_week_label, fiscal_week_start,
          fiscal_month, fiscal_month_name, fiscal_month_short, fiscal_month_label,
          fiscal_month_start, fiscal_month_end,
          fiscal_quarter, fiscal_quarter_label, fiscal_quarter_start, fiscal_quarter_end,
          fiscal_year, fiscal_year_label, fiscal_year_start, fiscal_year_end,
          same_day_last_year, same_week_start_lyr, same_month_start_lyr, same_quarter_start_lyr,
          holiday_name
        ) VALUES ${placeholders}
        ON CONFLICT (calendar_id, calendar_date) DO NOTHING`,
        values
      );
      count += batch.length;
      batch = [];
    };

    while (d <= DATE_TO) {
      const attrs = fiscalAttrs(d, FY_START_MONTH);
      const dateStr = d.toISOString().slice(0, 10);
      batch.push([
        calId, dateStr,
        attrs.day_of_week, attrs.day_name, attrs.day_name_short, attrs.is_weekend, attrs.is_holiday,
        attrs.iso_week, attrs.iso_week_year, attrs.iso_week_start, attrs.iso_week_label,
        attrs.fiscal_week, attrs.fiscal_week_label, attrs.fiscal_week_start,
        attrs.fiscal_month, attrs.fiscal_month_name, attrs.fiscal_month_short, attrs.fiscal_month_label,
        attrs.fiscal_month_start, attrs.fiscal_month_end,
        attrs.fiscal_quarter, attrs.fiscal_quarter_label, attrs.fiscal_quarter_start, attrs.fiscal_quarter_end,
        attrs.fiscal_year, attrs.fiscal_year_label, attrs.fiscal_year_start, attrs.fiscal_year_end,
        attrs.same_day_last_year, attrs.same_week_start_lyr, attrs.same_month_start_lyr, attrs.same_quarter_start_lyr,
        null
      ]);
      if (batch.length >= BATCH) await flush();
      d = addDays(d, 1);
    }
    await flush();
    console.log(`  inserted ${count} fiscal days`);

    // 4. Create dataset definitions
    console.log('Creating dataset definitions...');

    const datasets = [
      {
        name: 'equipment',
        label: 'Equipment',
        description: 'One row per unit — current state, compliance, lifetime rollups',
        primary_key_fields: ['PK'],
        segment_fields: ['UnitType','UnitClass','TonnageBucket','Make','Model','Company','Yard','Region',
                         'OwnershipType','AgeBucket','CurrentStatus','CurrentYard','IdleFlag','PMStatus',
                         'ComplianceStatus','PaybackStatus','InstanceId','TenantId'],
        metric_fields: ['Tonnage','AcquisitionYear','AgeYears','DaysSinceLastUse','EngineHoursLatest',
                        'OdometerLatest','HoursSinceLastPM','DaysUntilInspectionExp','DaysUntilInsuranceExp',
                        'DaysUntilRegistrationExp','ComplianceExpiringCount','AcquisitionCost','BookValue',
                        'DisposalValue','LifetimeRevenue','LifetimeEngineHours','LifetimeBillableHours',
                        'LifetimeAssignedDays','LifetimeMaintenanceCost','LifetimeFailures',
                        'YTDRevenue','YTDBillableHours','YTDAvailableHours','YTDMaintenanceCost'],
      },
      {
        name: 'equipment_daily',
        label: 'Equipment Daily',
        description: 'One row per unit per day — utilization, downtime, revenue, maintenance time-series',
        primary_key_fields: ['PK'],
        segment_fields: ['UnitCode','UnitType','UnitClass','TonnageBucket','Make','Model','Company','Yard',
                         'Region','OwnershipType','AgeBucket','FiscalYear','FiscalQuarter','FiscalMonth',
                         'FiscalMonthLabel','FiscalWeek','FiscalDayOfWeek','CalendarYear','CalendarMonth',
                         'IsWeekend','IsHoliday','PrimaryJobCode','PrimaryJobCustomer','PrimaryStatus',
                         'BillingType','InstanceId','TenantId'],
        metric_fields: ['DayFlag','AvailableHours','TargetHours','BillableHours','RevenueHours',
                        'AssignedFlag','IdleFlag','TransferFlag','DowntimeFlag','DowntimeHours',
                        'ScheduledDowntimeHours','UnscheduledDowntimeHours','Revenue','TargetRevenue',
                        'RevenueRental','RevenueTM','RevenueAIA','RevenueOther','MaintenanceCost',
                        'PartsCost','LaborCost','PMsScheduled','PMsCompletedOnTime','PMsCompletedLate',
                        'UnscheduledRepairs','FailureEvents','WorkOrdersOpened','WorkOrdersClosed',
                        'EngineHoursDelta','OdometerMilesDelta','FuelGallons','ActiveFlag','InServiceFlag',
                        'AcquisitionYear'],
      },
    ];

    for (const ds of datasets) {
      const { rows: [def] } = await client.query(
        `INSERT INTO dataset_definitions
           (client_id, name, label, description, is_active, show_on_explorer)
         VALUES ($1, $2, $3, $4, true, true)
         ON CONFLICT (client_id, name) DO UPDATE
           SET label=EXCLUDED.label, description=EXCLUDED.description, updated_at=NOW()
         RETURNING id`,
        [CLIENT_ID, ds.name, ds.label, ds.description]
      );
      const dsId = def.id;

      // Update primary_key_fields
      await client.query(
        `UPDATE dataset_definitions SET primary_key_fields=$1 WHERE id=$2`,
        [JSON.stringify(ds.primary_key_fields), dsId]
      );

      // Insert field metadata
      for (const f of ds.segment_fields) {
        await client.query(
          `INSERT INTO dataset_field_metadata (dataset_id, field_name, label, field_type)
           VALUES ($1, $2, $3, 'segment')
           ON CONFLICT (dataset_id, field_name) DO NOTHING`,
          [dsId, f, f.replace(/([A-Z])/g, ' $1').trim()]
        );
      }
      for (const f of ds.metric_fields) {
        await client.query(
          `INSERT INTO dataset_field_metadata (dataset_id, field_name, label, field_type)
           VALUES ($1, $2, $3, 'metric')
           ON CONFLICT (dataset_id, field_name) DO NOTHING`,
          [dsId, f, f.replace(/([A-Z])/g, ' $1').trim()]
        );
      }

      console.log(`  ${ds.name} (id: ${dsId}) — ${ds.segment_fields.length} segments, ${ds.metric_fields.length} metrics`);
    }

    await client.query('COMMIT');
    console.log('\nSeed complete.');
  } catch(e) {
    await client.query('ROLLBACK');
    console.error('SEED FAILED:', e.message);
    throw e;
  } finally {
    client.release();
    await pool.end();
  }
}

main().catch(e => { console.error(e); process.exit(1); });
