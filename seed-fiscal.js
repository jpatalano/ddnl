/**
 * seed-fiscal.js  —  DDNL Platform
 *
 * Populates fiscal_calendars + fiscal_days for a given client.
 * Generates one row per calendar date from START_YEAR to END_YEAR (inclusive).
 *
 * Fiscal config is read from env / args — same values the admin UI will eventually write
 * to fiscal_calendars. For produce: Jan 1, ISO weeks (Mon start), calendar year = fiscal year.
 *
 * Usage:
 *   DATABASE_URL=postgresql://... \
 *   CLIENT_ID=produce \
 *   FISCAL_YEAR_START_MONTH=1 \
 *   FISCAL_YEAR_START_DAY=1 \
 *   WEEK_START_DAY=1 \
 *   CALENDAR_START_YEAR=2020 \
 *   CALENDAR_END_YEAR=2027 \
 *   node seed-fiscal.js
 *
 * Safe to re-run — upserts on (client_id, name) and (calendar_id, calendar_date).
 */

'use strict';
const { Pool } = require('pg');

// ─── Config ───────────────────────────────────────────────────────────────────

const CLIENT_ID   = process.env.CLIENT_ID   || 'produce';
const FY_MONTH    = parseInt(process.env.FISCAL_YEAR_START_MONTH || '1',  10);  // 1-12
const FY_DAY      = parseInt(process.env.FISCAL_YEAR_START_DAY   || '1',  10);  // 1-28
const WEEK_START  = parseInt(process.env.WEEK_START_DAY           || '1',  10);  // 1=Mon, 7=Sun
const START_YEAR  = parseInt(process.env.CALENDAR_START_YEAR      || '2020', 10);
const END_YEAR    = parseInt(process.env.CALENDAR_END_YEAR        || '2027', 10);
const CAL_NAME    = process.env.CALENDAR_NAME || 'Default';

const WEEK_SCHEME = 'iso';  // only supported scheme for now

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL && process.env.DATABASE_URL.includes('railway.internal')
    ? false
    : { rejectUnauthorized: false },
});

// ─── Date helpers ─────────────────────────────────────────────────────────────

const DAY_NAMES       = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'];
const DAY_NAMES_SHORT = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun'];
const MONTH_NAMES     = ['January','February','March','April','May','June',
                         'July','August','September','October','November','December'];
const MONTH_SHORT     = ['Jan','Feb','Mar','Apr','May','Jun',
                         'Jul','Aug','Sep','Oct','Nov','Dec'];

/** Returns the fiscal year a date belongs to, given the fiscal year start month/day.
 *  e.g. FY_MONTH=1, FY_DAY=1  →  Jan 1 2024 starts FY2024, Dec 31 2024 is still FY2024
 *  e.g. FY_MONTH=7, FY_DAY=1  →  Jun 30 2024 is FY2024, Jul 1 2024 starts FY2025
 */
function getFiscalYear(date) {
  const fyStart = new Date(date.getFullYear(), FY_MONTH - 1, FY_DAY);
  return date >= fyStart ? date.getFullYear() : date.getFullYear() - 1;
}

/** First day of the fiscal year that contains `date` */
function getFiscalYearStart(date) {
  const fy = getFiscalYear(date);
  return new Date(fy, FY_MONTH - 1, FY_DAY);
}

/** Last day of the fiscal year that contains `date` */
function getFiscalYearEnd(date) {
  const fy = getFiscalYear(date);
  const nextFYStart = new Date(fy + 1, FY_MONTH - 1, FY_DAY);
  const end = new Date(nextFYStart);
  end.setDate(end.getDate() - 1);
  return end;
}

/** Fiscal month (1-12) — months are offset by FY_MONTH so FM1 = the fiscal year's first month */
function getFiscalMonth(date) {
  const fyStart = getFiscalYearStart(date);
  // Count full calendar months from fiscal year start
  let months = (date.getFullYear() - fyStart.getFullYear()) * 12
             + (date.getMonth() - fyStart.getMonth());
  // If the FY start day is after 1, and we're before that day in the current month,
  // we're still in the previous fiscal month
  if (FY_DAY > 1 && date.getDate() < FY_DAY) {
    months -= 1;
  }
  return ((months % 12) + 12) % 12 + 1;  // 1-12
}

/** First day of the fiscal month containing `date` */
function getFiscalMonthStart(date) {
  const fm = getFiscalMonth(date);
  const fyStart = getFiscalYearStart(date);
  const calMonth = (fyStart.getMonth() + fm - 1) % 12;
  const calYear  = fyStart.getFullYear() + Math.floor((fyStart.getMonth() + fm - 1) / 12);
  return new Date(calYear, calMonth, FY_DAY);
}

/** Last day of the fiscal month containing `date` */
function getFiscalMonthEnd(date) {
  const start = getFiscalMonthStart(date);
  const next  = new Date(start);
  next.setMonth(next.getMonth() + 1);
  next.setDate(next.getDate() - 1);
  return next;
}

/** Fiscal quarter (1-4) based on fiscal month */
function getFiscalQuarter(date) {
  return Math.ceil(getFiscalMonth(date) / 3);
}

/** First day of the fiscal quarter containing `date` */
function getFiscalQuarterStart(date) {
  const fq = getFiscalQuarter(date);
  const fyStart = getFiscalYearStart(date);
  const startFM = (fq - 1) * 3 + 1;  // fiscal month 1, 4, 7, or 10
  const calMonth = (fyStart.getMonth() + startFM - 1) % 12;
  const calYear  = fyStart.getFullYear() + Math.floor((fyStart.getMonth() + startFM - 1) / 12);
  return new Date(calYear, calMonth, FY_DAY);
}

/** Last day of the fiscal quarter containing `date` */
function getFiscalQuarterEnd(date) {
  const start = getFiscalQuarterStart(date);
  const end   = new Date(start);
  end.setMonth(end.getMonth() + 3);
  end.setDate(end.getDate() - 1);
  return end;
}

/**
 * ISO week number (1-53) and ISO week year.
 * ISO week: week containing the first Thursday of the year; weeks start Monday.
 * Returns { week, year, weekStart }
 */
function getISOWeek(date) {
  const d    = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate()));
  const day  = d.getUTCDay() || 7;  // 1=Mon ... 7=Sun
  d.setUTCDate(d.getUTCDate() + 4 - day);  // shift to Thursday
  const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
  const week = Math.ceil((((d - yearStart) / 86400000) + 1) / 7);
  // Monday of this ISO week
  const weekStart = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate()));
  const wd = weekStart.getUTCDay() || 7;
  weekStart.setUTCDate(weekStart.getUTCDate() - (wd - 1));
  return { week, year: d.getUTCFullYear(), weekStart: toLocalDate(weekStart) };
}

/**
 * Fiscal week number within the fiscal year (ISO-style but anchored to fiscal year start).
 * Week 1 starts on the fiscal year start date.
 * Returns { week, weekStart }
 */
function getFiscalWeek(date) {
  const fyStart  = getFiscalYearStart(date);
  const daysDiff = Math.floor((localMs(date) - localMs(fyStart)) / 86400000);
  const week     = Math.floor(daysDiff / 7) + 1;
  const weekStartMs = localMs(fyStart) + (week - 1) * 7 * 86400000;
  return { week, weekStart: new Date(weekStartMs) };
}

/** Convert a UTC Date to a local-midnight Date (avoids TZ drift in date arithmetic) */
function toLocalDate(d) {
  return new Date(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate());
}

function localMs(d) {
  return new Date(d.getFullYear(), d.getMonth(), d.getDate()).getTime();
}

/** Format a Date as YYYY-MM-DD string */
function fmt(d) {
  if (!d) return null;
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  return `${y}-${m}-${dd}`;
}

function pad2(n) { return String(n).padStart(2, '0'); }

// ─── Row builder ──────────────────────────────────────────────────────────────

function buildRow(date) {
  // ISO day of week: 1=Mon, 7=Sun
  const dow       = ((date.getDay() + 6) % 7) + 1;  // JS getDay: 0=Sun
  const isWeekend = dow >= 6;

  const iso           = getISOWeek(date);
  const fiscalWeek    = getFiscalWeek(date);
  const fyStart       = getFiscalYearStart(date);
  const fyEnd         = getFiscalYearEnd(date);
  const fmStart       = getFiscalMonthStart(date);
  const fmEnd         = getFiscalMonthEnd(date);
  const fqStart       = getFiscalQuarterStart(date);
  const fqEnd         = getFiscalQuarterEnd(date);
  const fy            = getFiscalYear(date);
  const fm            = getFiscalMonth(date);
  const fq            = getFiscalQuarter(date);

  // Same day last year: go back exactly one fiscal year (account for leap year)
  const sdly = new Date(date);
  sdly.setFullYear(sdly.getFullYear() - 1);
  // If Feb 29 doesn't exist last year, fall back to Feb 28
  if (sdly.getMonth() !== date.getMonth()) sdly.setDate(0);

  // Same week start last year: fiscal_week_start shifted back one fiscal year
  const swsly = new Date(fiscalWeek.weekStart);
  swsly.setFullYear(swsly.getFullYear() - 1);

  // Same month start last year
  const smsly = new Date(fmStart);
  smsly.setFullYear(smsly.getFullYear() - 1);

  // Same quarter start last year
  const sqsly = new Date(fqStart);
  sqsly.setFullYear(sqsly.getFullYear() - 1);

  return {
    calendar_date:          fmt(date),
    day_of_week:            dow,
    day_name:               DAY_NAMES[dow - 1],
    day_name_short:         DAY_NAMES_SHORT[dow - 1],
    is_weekend:             isWeekend,
    is_holiday:             false,
    holiday_name:           null,
    iso_week:               iso.week,
    iso_week_year:          iso.year,
    iso_week_start:         fmt(iso.weekStart),
    iso_week_label:         `W${pad2(iso.week)} ${iso.year}`,
    fiscal_week:            fiscalWeek.week,
    fiscal_week_label:      `FW${pad2(fiscalWeek.week)}`,
    fiscal_week_start:      fmt(fiscalWeek.weekStart),
    fiscal_month:           fm,
    fiscal_month_name:      MONTH_NAMES[fmStart.getMonth()],
    fiscal_month_short:     MONTH_SHORT[fmStart.getMonth()],
    fiscal_month_label:     `FM${pad2(fm)}`,
    fiscal_month_start:     fmt(fmStart),
    fiscal_month_end:       fmt(fmEnd),
    fiscal_quarter:         fq,
    fiscal_quarter_label:   `FQ${fq}`,
    fiscal_quarter_start:   fmt(fqStart),
    fiscal_quarter_end:     fmt(fqEnd),
    fiscal_year:            fy,
    fiscal_year_label:      `FY${fy}`,
    fiscal_year_start:      fmt(fyStart),
    fiscal_year_end:        fmt(fyEnd),
    same_day_last_year:     fmt(sdly),
    same_week_start_lyr:    fmt(swsly),
    same_month_start_lyr:   fmt(smsly),
    same_quarter_start_lyr: fmt(sqsly),
  };
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function run() {
  const client = await pool.connect();
  try {
    console.log(`Seeding fiscal calendar for client=${CLIENT_ID}, FY start=${FY_MONTH}/${FY_DAY}, years ${START_YEAR}–${END_YEAR}`);

    // Upsert fiscal_calendars row
    const calRes = await client.query(`
      INSERT INTO fiscal_calendars
        (client_id, name, fiscal_year_start_month, fiscal_year_start_day, week_start_day, week_scheme)
      VALUES ($1, $2, $3, $4, $5, $6)
      ON CONFLICT (client_id, name)
      DO UPDATE SET
        fiscal_year_start_month = EXCLUDED.fiscal_year_start_month,
        fiscal_year_start_day   = EXCLUDED.fiscal_year_start_day,
        week_start_day          = EXCLUDED.week_start_day,
        week_scheme             = EXCLUDED.week_scheme,
        updated_at              = NOW()
      RETURNING id
    `, [CLIENT_ID, CAL_NAME, FY_MONTH, FY_DAY, WEEK_START, WEEK_SCHEME]);

    const calendarId = calRes.rows[0].id;
    console.log(`  fiscal_calendars id = ${calendarId}`);

    // Generate all dates
    const startDate = new Date(START_YEAR, 0, 1);
    const endDate   = new Date(END_YEAR, 11, 31);
    const rows      = [];
    const cur       = new Date(startDate);
    while (cur <= endDate) {
      rows.push(buildRow(new Date(cur)));
      cur.setDate(cur.getDate() + 1);
    }
    console.log(`  Generated ${rows.length} days`);

    // Upsert in transactional batches of 200 rows.
    // Single-row parameterized INSERT avoids Postgres type-inference issues
    // with large multi-row statements and stays well under the 65535 param limit.
    const cols       = Object.keys(rows[0]);
    const colList    = cols.join(', ');
    const updateSet  = cols.filter(c => c !== 'calendar_date').map(c => `${c} = EXCLUDED.${c}`).join(', ');
    // Build placeholder list for one row: ($2,$3,...) — $1 is always calendarId
    const rowPlaceholders = cols.map((_, i) => `$${i + 2}`).join(', ');
    const upsertSQL = `
      INSERT INTO fiscal_days (calendar_id, ${colList})
      VALUES ($1, ${rowPlaceholders})
      ON CONFLICT (calendar_id, calendar_date)
      DO UPDATE SET ${updateSet}
    `;

    const BATCH = 200;
    let inserted = 0;
    for (let i = 0; i < rows.length; i += BATCH) {
      const batch = rows.slice(i, i + BATCH);
      await client.query('BEGIN');
      for (const row of batch) {
        await client.query(upsertSQL, [calendarId, ...cols.map(c => row[c])]);
      }
      await client.query('COMMIT');
      inserted += batch.length;
      process.stdout.write(`\r  Upserted ${inserted}/${rows.length} days`);
    }
    console.log('\n  Done.');

    // Spot-check a couple of rows
    const check = await client.query(`
      SELECT calendar_date, fiscal_year, fiscal_month, fiscal_quarter, fiscal_week,
             iso_week, same_day_last_year, same_week_start_lyr
      FROM fiscal_days
      WHERE calendar_id = $1 AND calendar_date IN ('2024-01-01','2024-07-04','2025-12-31','2024-02-29')
      ORDER BY calendar_date
    `, [calendarId]);
    console.log('\n  Spot-check:');
    check.rows.forEach(r => console.log('  ', JSON.stringify(r)));

  } finally {
    client.release();
    await pool.end();
  }
}

run().catch(err => { console.error(err); process.exit(1); });
