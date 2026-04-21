#!/usr/bin/env node
/**
 * fetch-weather.js  —  DDNL nightly weather ingest job
 *
 * Pulls weather data from Open-Meteo for all store locations and upserts
 * into the 'weather' dataset via the ingest API.
 *
 * Strategy:
 *   - Past dates  (before today): final actuals — upserted once, never change
 *   - Today:      actuals as of right now
 *   - Future (+1 to +7 days): forecast — upserted nightly, overwritten as
 *                              forecasts are revised or actuals replace them
 *
 * Run this job nightly after midnight so today's actuals are final before
 * the next day's forecast window shifts.
 *
 * Usage:
 *   node fetch-weather.js                   # today + 7-day forecast
 *   node fetch-weather.js --days-back 3     # also pull 3 days of history to patch any gaps
 *   FORECAST_DAYS=14 node fetch-weather.js  # extend forecast window
 *
 * Env:
 *   API_BASE      Override API base URL
 *   API_KEY       Override ingest API key
 *   INSTANCE_ID   Override instance id
 *   FORECAST_DAYS Number of forecast days (default 7)
 *   DRY_RUN=1     Fetch + log but do not ingest
 */

'use strict';

const https        = require('https');
const http         = require('http');
const fs           = require('fs');
const path         = require('path');

// ─── Config ───────────────────────────────────────────────────────────────────

const API_BASE     = process.env.API_BASE     || 'https://produce-analytics-production.up.railway.app';
const API_KEY      = process.env.API_KEY      || 'ik_ef3ed1ba7c466fb4ef44b930a06788abbc6066c14960dcf9ac8a7138fd0aa921';
const INSTANCE_ID  = process.env.INSTANCE_ID  || 'produce';
const DATASET      = 'weather';
const FORECAST_DAYS = parseInt(process.env.FORECAST_DAYS || '7', 10);
const DRY_RUN      = process.env.DRY_RUN === '1';

// --days-back N: also re-fetch N days of past data (to patch any prior gaps)
const daysBackArg  = process.argv.indexOf('--days-back');
const DAYS_BACK    = daysBackArg !== -1 ? parseInt(process.argv[daysBackArg + 1] || '0', 10) : 0;

const LOG_DIR      = path.join(__dirname, 'logs');
const WEATHER_LOG  = path.join(LOG_DIR, 'weather.log.jsonl');
fs.mkdirSync(LOG_DIR, { recursive: true });

// ─── Store definitions (shared with generator) ────────────────────────────────

const STORES = [
  { id: 'store_001', name: 'Seattle Pike Market',          address: '1531 Pike Pl',             city: 'Seattle',  state: 'WA', zip: '98101', region: 'Pacific Northwest', store_type: 'Urban',    lat: 47.6062, lon: -122.3321, timezone: 'America/Los_Angeles' },
  { id: 'store_002', name: 'Miami Fresh Market',           address: '3250 NW 27th Ave',         city: 'Miami',    state: 'FL', zip: '33142', region: 'Southeast',         store_type: 'Urban',    lat: 25.7617, lon:  -80.1918, timezone: 'America/New_York'    },
  { id: 'store_003', name: 'Chicago Green City',           address: '300 E Randolph St',        city: 'Chicago',  state: 'IL', zip: '60601', region: 'Midwest',           store_type: 'Urban',    lat: 41.8781, lon:  -87.6298, timezone: 'America/Chicago'     },
  { id: 'store_004', name: 'Phoenix Desert Fresh',         address: '4710 E Camelback Rd',      city: 'Phoenix',  state: 'AZ', zip: '85018', region: 'Southwest',         store_type: 'Suburban', lat: 33.4484, lon: -112.0740, timezone: 'America/Phoenix'     },
  { id: 'store_005', name: 'NYC Union Square Greenmarket', address: '1 Union Square W',         city: 'New York', state: 'NY', zip: '10003', region: 'Northeast',         store_type: 'Urban',    lat: 40.7128, lon:  -74.0060, timezone: 'America/New_York'    },
  { id: 'store_006', name: 'Denver Mile High Produce',     address: '1600 Glenarm Pl',          city: 'Denver',   state: 'CO', zip: '80202', region: 'Mountain',          store_type: 'Urban',    lat: 39.7392, lon: -104.9903, timezone: 'America/Denver'      },
  { id: 'store_007', name: 'Atlanta Ponce City Market',    address: '675 Ponce De Leon Ave NE', city: 'Atlanta',  state: 'GA', zip: '30308', region: 'Southeast',         store_type: 'Urban',    lat: 33.7490, lon:  -84.3880, timezone: 'America/New_York'    },
];

// ─── WMO weather code → human condition ──────────────────────────────────────

function wmoToCondition(code) {
  if (code === 0)    return 'sunny';
  if (code <= 3)     return 'partly_cloudy';
  if (code <= 49)    return 'foggy';
  if (code <= 59)    return 'drizzle';
  if (code <= 69)    return 'rain';
  if (code <= 79)    return 'snow';
  if (code <= 82)    return 'showers';
  if (code <= 84)    return 'hail';
  if (code <= 99)    return 'thunderstorm';
  return 'unknown';
}

// ─── Traffic impact index ─────────────────────────────────────────────────────
// 1.0 = normal, <1 suppressed, >1 boosted
// Used by the sales generator to scale unit volumes

function trafficIndex(condition, tempHigh) {
  let idx = 1.0;
  if (['snow', 'hail'].includes(condition))        idx -= 0.30;
  if (tempHigh < 35)                               idx -= 0.15;
  if (['rain', 'drizzle', 'showers'].includes(condition)) idx -= 0.18;
  if (condition === 'thunderstorm')                idx -= 0.25;
  if (tempHigh > 95)                               idx -= 0.12;
  if (condition === 'sunny' && tempHigh >= 55 && tempHigh <= 80) idx += 0.08;
  return Math.round(Math.max(0.20, Math.min(1.20, idx)) * 100) / 100;
}

// ─── Date helpers ─────────────────────────────────────────────────────────────

function isoDate(d) { return d.toISOString().slice(0, 10); }

function addDays(dateStr, n) {
  const d = new Date(dateStr + 'T00:00:00Z');
  d.setUTCDate(d.getUTCDate() + n);
  return isoDate(d);
}

// ─── HTTP helpers ─────────────────────────────────────────────────────────────

function httpGet(url) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('https') ? https : http;
    mod.get(url, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        try { resolve(JSON.parse(data)); }
        catch (e) { reject(new Error(`JSON parse error for ${url}: ${data.slice(0, 200)}`)); }
      });
    }).on('error', reject);
  });
}

function apiPost(urlPath, body) {
  return new Promise((resolve, reject) => {
    const payload = JSON.stringify(body);
    const url     = new URL(urlPath, API_BASE);
    const mod     = url.protocol === 'https:' ? https : http;
    const req = mod.request({
      hostname: url.hostname,
      port:     url.port || (url.protocol === 'https:' ? 443 : 80),
      path:     url.pathname + url.search,
      method:   'POST',
      headers: {
        'Content-Type':   'application/json',
        'Content-Length': Buffer.byteLength(payload),
        'X-API-Key':      API_KEY,
        'X-Instance-Id':  INSTANCE_ID,
      }
    }, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        try { resolve({ status: res.statusCode, body: JSON.parse(data) }); }
        catch (e) { resolve({ status: res.statusCode, body: data }); }
      });
    });
    req.on('error', reject);
    req.write(payload);
    req.end();
  });
}

// ─── Fetch from Open-Meteo ────────────────────────────────────────────────────
// Uses archive API for past dates, forecast API for future dates.
// We always request a window that may span both — archive handles up to yesterday,
// forecast handles today onwards. Open-Meteo's /forecast endpoint supports
// both current day and future days in one call with historical_days param.

async function fetchWeatherRange(store, startDate, endDate) {
  const today    = isoDate(new Date());
  const isFuture = startDate > today;
  const isPast   = endDate   < today;

  let rows = [];

  // Past / historical: use archive API
  if (!isFuture) {
    const archiveEnd = isPast ? endDate : addDays(today, -1);
    if (archiveEnd >= startDate) {
      const url = 'https://archive-api.open-meteo.com/v1/archive?' +
        `latitude=${store.lat}&longitude=${store.lon}` +
        `&start_date=${startDate}&end_date=${archiveEnd}` +
        `&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,weathercode` +
        `&temperature_unit=fahrenheit` +
        `&timezone=${encodeURIComponent(store.timezone)}`;
      const data = await httpGet(url);
      if (data.daily?.time) {
        rows = rows.concat(parseDailyRows(store, data.daily, false));
      }
    }
  }

  // Today + future: use forecast API
  if (!isPast) {
    const forecastStart = isFuture ? startDate : today;
    const forecastDays  = Math.ceil(
      (new Date(endDate + 'T00:00:00Z') - new Date(forecastStart + 'T00:00:00Z')) / 86400000
    ) + 1;
    const url = 'https://api.open-meteo.com/v1/forecast?' +
      `latitude=${store.lat}&longitude=${store.lon}` +
      `&forecast_days=${Math.min(forecastDays, 16)}` +
      `&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,weathercode` +
      `&temperature_unit=fahrenheit` +
      `&timezone=${encodeURIComponent(store.timezone)}`;
    const data = await httpGet(url);
    if (data.daily?.time) {
      const filteredTime    = data.daily.time.filter(d => d >= forecastStart && d <= endDate);
      const startIdx        = data.daily.time.indexOf(filteredTime[0]);
      const slicedDaily     = {
        time:                 filteredTime,
        temperature_2m_max:   data.daily.temperature_2m_max.slice(startIdx, startIdx + filteredTime.length),
        temperature_2m_min:   data.daily.temperature_2m_min.slice(startIdx, startIdx + filteredTime.length),
        precipitation_sum:    data.daily.precipitation_sum.slice(startIdx,  startIdx + filteredTime.length),
        weathercode:          data.daily.weathercode.slice(startIdx,         startIdx + filteredTime.length),
      };
      rows = rows.concat(parseDailyRows(store, slicedDaily, true));
    }
  }

  return rows;
}

function parseDailyRows(store, daily, isForecast) {
  const today = isoDate(new Date());
  return daily.time.map((date, i) => {
    const tempHigh  = daily.temperature_2m_max[i]  ?? null;
    const tempLow   = daily.temperature_2m_min[i]  ?? null;
    const precip    = daily.precipitation_sum[i]   ?? 0;
    const wcode     = daily.weathercode[i]         ?? 0;
    const condition = wmoToCondition(wcode);
    const isPast    = date < today;
    const isToday   = date === today;

    return {
      weather_pk:       `${date}__${store.id}`,
      date,
      store_id:         store.id,
      store_name:       store.name,
      city:             store.city,
      state:            store.state,
      region:           store.region,
      lat:              store.lat,
      lon:              store.lon,
      temp_high_f:      tempHigh,
      temp_low_f:       tempLow,
      temp_avg_f:       tempHigh !== null && tempLow !== null
                          ? Math.round(((tempHigh + tempLow) / 2) * 10) / 10
                          : null,
      precipitation_mm: Math.round((precip ?? 0) * 10) / 10,
      weather_code:     wcode,
      condition,
      is_rainy:         ['drizzle','rain','showers','thunderstorm'].includes(condition),
      is_snowy:         ['snow','hail'].includes(condition),
      is_cold:          tempHigh !== null && tempHigh < 35,
      is_hot:           tempHigh !== null && tempHigh > 95,
      is_extreme:       tempHigh !== null && (tempHigh > 95 || tempHigh < 35 || ['snow','hail','thunderstorm'].includes(condition)),
      traffic_index:    tempHigh !== null ? trafficIndex(condition, tempHigh) : 1.0,
      is_forecast:      isForecast && !isPast,   // false once the day has passed
      data_type:        isPast || isToday ? 'actual' : 'forecast',
    };
  });
}

// ─── Ingest helpers ───────────────────────────────────────────────────────────

function chunkArray(arr, n) {
  const out = [];
  for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n));
  return out;
}

async function ingestDocs(docs) {
  if (DRY_RUN) {
    console.log(`    [DRY RUN] would ingest ${docs.length} docs`);
    return { indexed: docs.length, failed: 0 };
  }
  const res = await apiPost(`/api/ingest/${DATASET}/bulk`, { docs, replace: false });
  if (res.status !== 200) {
    throw new Error(`HTTP ${res.status}: ${JSON.stringify(res.body).slice(0, 300)}`);
  }
  return { indexed: res.body.indexed ?? docs.length, failed: res.body.failed ?? 0 };
}

// ─── Structured logger ────────────────────────────────────────────────────────

function writeLog(entry) {
  fs.appendFileSync(WEATHER_LOG, JSON.stringify({ ts: new Date().toISOString(), ...entry }) + '\n');
}

// ─── Main ─────────────────────────────────────────────────────────────────────

(async () => {
  const today     = isoDate(new Date());
  const startDate = DAYS_BACK > 0 ? addDays(today, -DAYS_BACK) : today;
  const endDate   = addDays(today, FORECAST_DAYS);

  console.log(`╔══ DDNL Weather Job ════════════════════════════════════════════╗`);
  console.log(`  window   : ${startDate} → ${endDate}  (today: ${today}, +${FORECAST_DAYS}d forecast)`);
  console.log(`  stores   : ${STORES.length}`);
  console.log(`  api      : ${API_BASE}`);
  console.log(`  dry_run  : ${DRY_RUN}`);
  console.log(`╠═════════════════════════════════════════════════════════════════╣`);

  writeLog({ event: 'job_started', start_date: startDate, end_date: endDate, forecast_days: FORECAST_DAYS, dry_run: DRY_RUN });

  let totalIngested = 0;
  let totalFailed   = 0;

  for (const store of STORES) {
    process.stdout.write(`  ${store.id}  ${(store.city + ', ' + store.state).padEnd(18)}  `);

    let rows;
    try {
      rows = await fetchWeatherRange(store, startDate, endDate);
    } catch (e) {
      console.log(`✗ FETCH — ${e.message}`);
      writeLog({ event: 'fetch_failed', store_id: store.id, error: e.message });
      totalFailed++;
      continue;
    }

    // Ingest in batches of 500
    let ingested = 0;
    let failed   = 0;
    for (const batch of chunkArray(rows, 500)) {
      try {
        const r = await ingestDocs(batch);
        ingested += r.indexed;
        failed   += r.failed;
      } catch (e) {
        console.log(`\n    ✗ INGEST — ${e.message}`);
        writeLog({ event: 'ingest_failed', store_id: store.id, error: e.message });
        failed += batch.length;
      }
    }

    totalIngested += ingested;
    totalFailed   += failed;

    const actuals   = rows.filter(r => r.data_type === 'actual').length;
    const forecasts = rows.filter(r => r.data_type === 'forecast').length;
    const rainy     = rows.filter(r => r.is_rainy).length;
    const snowy     = rows.filter(r => r.is_snowy).length;
    console.log(`✓  ${rows.length} days  actual:${actuals}  forecast:${forecasts}  rain:${rainy}  snow:${snowy}  → ${ingested} upserted`);

    writeLog({ event: 'store_complete', store_id: store.id, city: store.city, state: store.state,
               rows: rows.length, ingested, failed, actuals, forecasts, rainy_days: rainy, snowy_days: snowy });

    // Small pause between stores to be polite to Open-Meteo
    await new Promise(r => setTimeout(r, 300));
  }

  console.log(`╠═════════════════════════════════════════════════════════════════╣`);
  console.log(`  Total upserted : ${totalIngested.toLocaleString()}`);
  console.log(`  Failed         : ${totalFailed}`);
  console.log(`╚═════════════════════════════════════════════════════════════════╝`);

  writeLog({ event: 'job_complete', total_ingested: totalIngested, total_failed: totalFailed });
})().catch(e => {
  console.error('\nWeather job crashed:', e.message, e.stack);
  writeLog({ event: 'job_crashed', error: e.message });
  process.exit(1);
});
