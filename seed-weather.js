#!/usr/bin/env node
/**
 * seed-weather.js  —  One-time historical weather backfill
 *
 * Fetches real weather from Open-Meteo archive API for all store locations
 * from START_DATE to END_DATE (defaults: 2024-01-01 → yesterday) and upserts
 * into the 'weather' dataset via the DDNL ingest API.
 *
 * Run once. The nightly fetch-weather.js job takes over after that.
 *
 * Usage:
 *   node seed-weather.js                         # 2024-01-01 → yesterday
 *   node seed-weather.js 2025-01-01              # custom start
 *   node seed-weather.js 2025-01-01 2025-12-31   # custom range
 */

'use strict';

const https = require('https');
const http  = require('http');
const fs    = require('fs');
const path  = require('path');

// ─── Config ───────────────────────────────────────────────────────────────────

const API_BASE    = process.env.API_BASE    || 'https://produce-analytics-production.up.railway.app';
const API_KEY     = process.env.API_KEY     || 'ik_ef3ed1ba7c466fb4ef44b930a06788abbc6066c14960dcf9ac8a7138fd0aa921';
const INSTANCE_ID = process.env.INSTANCE_ID || 'produce';
const DATASET     = 'weather';

const today     = new Date().toISOString().slice(0, 10);
const yesterday = new Date(Date.now() - 86400000).toISOString().slice(0, 10);

const START_DATE = process.argv[2] || '2024-01-01';
const END_DATE   = process.argv[3] || yesterday;

// ─── Stores ───────────────────────────────────────────────────────────────────

const STORES = [
  { id: 'store_001', name: 'Seattle Pike Market',          address: '1531 Pike Pl',             city: 'Seattle',  state: 'WA', zip: '98101', region: 'Pacific Northwest', store_type: 'Urban',    lat: 47.6062, lon: -122.3321, timezone: 'America/Los_Angeles' },
  { id: 'store_002', name: 'Miami Fresh Market',           address: '3250 NW 27th Ave',         city: 'Miami',    state: 'FL', zip: '33142', region: 'Southeast',         store_type: 'Urban',    lat: 25.7617, lon:  -80.1918, timezone: 'America/New_York'    },
  { id: 'store_003', name: 'Chicago Green City',           address: '300 E Randolph St',        city: 'Chicago',  state: 'IL', zip: '60601', region: 'Midwest',           store_type: 'Urban',    lat: 41.8781, lon:  -87.6298, timezone: 'America/Chicago'     },
  { id: 'store_004', name: 'Phoenix Desert Fresh',         address: '4710 E Camelback Rd',      city: 'Phoenix',  state: 'AZ', zip: '85018', region: 'Southwest',         store_type: 'Suburban', lat: 33.4484, lon: -112.0740, timezone: 'America/Phoenix'     },
  { id: 'store_005', name: 'NYC Union Square Greenmarket', address: '1 Union Square W',         city: 'New York', state: 'NY', zip: '10003', region: 'Northeast',         store_type: 'Urban',    lat: 40.7128, lon:  -74.0060, timezone: 'America/New_York'    },
  { id: 'store_006', name: 'Denver Mile High Produce',     address: '1600 Glenarm Pl',          city: 'Denver',   state: 'CO', zip: '80202', region: 'Mountain',          store_type: 'Urban',    lat: 39.7392, lon: -104.9903, timezone: 'America/Denver'      },
  { id: 'store_007', name: 'Atlanta Ponce City Market',    address: '675 Ponce De Leon Ave NE', city: 'Atlanta',  state: 'GA', zip: '30308', region: 'Southeast',         store_type: 'Urban',    lat: 33.7490, lon:  -84.3880, timezone: 'America/New_York'    },
];

// ─── WMO → condition ─────────────────────────────────────────────────────────

function wmoToCondition(code) {
  if (code === 0)  return 'sunny';
  if (code <= 3)   return 'partly_cloudy';
  if (code <= 49)  return 'foggy';
  if (code <= 59)  return 'drizzle';
  if (code <= 69)  return 'rain';
  if (code <= 79)  return 'snow';
  if (code <= 82)  return 'showers';
  if (code <= 84)  return 'hail';
  if (code <= 99)  return 'thunderstorm';
  return 'unknown';
}

function trafficIndex(condition, tempHigh) {
  let idx = 1.0;
  if (['snow','hail'].includes(condition))                        idx -= 0.30;
  if (tempHigh < 35)                                              idx -= 0.15;
  if (['rain','drizzle','showers'].includes(condition))           idx -= 0.18;
  if (condition === 'thunderstorm')                               idx -= 0.25;
  if (tempHigh > 95)                                              idx -= 0.12;
  if (condition === 'sunny' && tempHigh >= 55 && tempHigh <= 80) idx += 0.08;
  return Math.round(Math.max(0.20, Math.min(1.20, idx)) * 100) / 100;
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
        catch (e) { reject(new Error(`JSON parse: ${data.slice(0, 200)}`)); }
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

// ─── Fetch archive weather for one store ─────────────────────────────────────

async function fetchArchive(store, startDate, endDate) {
  const url = 'https://archive-api.open-meteo.com/v1/archive?' +
    `latitude=${store.lat}&longitude=${store.lon}` +
    `&start_date=${startDate}&end_date=${endDate}` +
    `&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,weathercode` +
    `&temperature_unit=fahrenheit` +
    `&timezone=${encodeURIComponent(store.timezone)}`;

  const data = await httpGet(url);
  if (!data.daily?.time) throw new Error(`No daily data returned`);

  const { time, temperature_2m_max, temperature_2m_min, precipitation_sum, weathercode } = data.daily;

  return time.map((date, i) => {
    const tempHigh  = temperature_2m_max[i] ?? null;
    const tempLow   = temperature_2m_min[i] ?? null;
    const precip    = precipitation_sum[i]  ?? 0;
    const wcode     = weathercode[i]        ?? 0;
    const condition = wmoToCondition(wcode);

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
                          ? Math.round(((tempHigh + tempLow) / 2) * 10) / 10 : null,
      precipitation_mm: Math.round((precip ?? 0) * 10) / 10,
      weather_code:     wcode,
      condition,
      is_rainy:   ['drizzle','rain','showers','thunderstorm'].includes(condition),
      is_snowy:   ['snow','hail'].includes(condition),
      is_cold:    tempHigh !== null && tempHigh < 35,
      is_hot:     tempHigh !== null && tempHigh > 95,
      is_extreme: tempHigh !== null && (tempHigh > 95 || tempHigh < 35 || ['snow','hail','thunderstorm'].includes(condition)),
      traffic_index: tempHigh !== null ? trafficIndex(condition, tempHigh) : 1.0,
      is_forecast: false,
      data_type:   'actual',
    };
  });
}

// ─── Ingest ───────────────────────────────────────────────────────────────────

function chunk(arr, n) {
  const out = [];
  for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n));
  return out;
}

async function ingestDocs(docs) {
  const res = await apiPost(`/api/ingest/${DATASET}/bulk`, { docs, replace: false });
  if (res.status !== 200) throw new Error(`HTTP ${res.status}: ${JSON.stringify(res.body).slice(0,300)}`);
  return res.body.indexed ?? docs.length;
}

// ─── Weather dataset fields schema ──────────────────────────────────────────

const WEATHER_FIELDS = [
  { name: 'weather_pk',       fieldType: 'string',  segmentType: 'dimension', isFilterable: true  },
  { name: 'date',             fieldType: 'date',    segmentType: 'dimension', isFilterable: true, isGroupable: true },
  { name: 'store_id',         fieldType: 'string',  segmentType: 'dimension', isFilterable: true, isGroupable: true },
  { name: 'store_name',       fieldType: 'string',  segmentType: 'dimension', isFilterable: true, isGroupable: true },
  { name: 'city',             fieldType: 'string',  segmentType: 'dimension', isFilterable: true, isGroupable: true },
  { name: 'state',            fieldType: 'string',  segmentType: 'dimension', isFilterable: true, isGroupable: true },
  { name: 'region',           fieldType: 'string',  segmentType: 'dimension', isFilterable: true, isGroupable: true },
  { name: 'lat',              fieldType: 'number',  segmentType: 'metric'                         },
  { name: 'lon',              fieldType: 'number',  segmentType: 'metric'                         },
  { name: 'temp_high_f',      fieldType: 'number',  segmentType: 'metric',    aggregationType: 'AVG', suffix: '°F' },
  { name: 'temp_low_f',       fieldType: 'number',  segmentType: 'metric',    aggregationType: 'AVG', suffix: '°F' },
  { name: 'temp_avg_f',       fieldType: 'number',  segmentType: 'metric',    aggregationType: 'AVG', suffix: '°F' },
  { name: 'precipitation_mm', fieldType: 'number',  segmentType: 'metric',    aggregationType: 'SUM', suffix: 'mm' },
  { name: 'weather_code',     fieldType: 'number',  segmentType: 'dimension'                      },
  { name: 'condition',        fieldType: 'string',  segmentType: 'dimension', isFilterable: true, isGroupable: true },
  { name: 'is_rainy',         fieldType: 'boolean', segmentType: 'dimension', isFilterable: true  },
  { name: 'is_snowy',         fieldType: 'boolean', segmentType: 'dimension', isFilterable: true  },
  { name: 'is_cold',          fieldType: 'boolean', segmentType: 'dimension', isFilterable: true  },
  { name: 'is_hot',           fieldType: 'boolean', segmentType: 'dimension', isFilterable: true  },
  { name: 'is_extreme',       fieldType: 'boolean', segmentType: 'dimension', isFilterable: true  },
  { name: 'traffic_index',    fieldType: 'number',  segmentType: 'metric',    aggregationType: 'AVG' },
  { name: 'is_forecast',      fieldType: 'boolean', segmentType: 'dimension', isFilterable: true  },
  { name: 'data_type',        fieldType: 'string',  segmentType: 'dimension', isFilterable: true  },
];

async function ensureWeatherDataset() {
  // Try to create — if already exists the API will return 409, that's fine
  const res = await apiPost('/api/ingest/admin/datasets', {
    name:         'weather',
    label:        'Weather',
    description:  'Daily weather actuals and forecasts per store location. Platform-provided dataset — updated nightly.',
    dataset_type: 'provided',
    fields:       WEATHER_FIELDS,
  });
  if (res.status === 200 || res.status === 201) {
    console.log(`  ✓ weather dataset created (id: ${res.body.dataset?.id ?? res.body.id})`);
  } else if (res.status === 409 || (res.body?.error || '').includes('already exists') || (res.body?.error || '').includes('duplicate')) {
    console.log(`  ✓ weather dataset already exists`);
  } else {
    throw new Error(`Dataset create failed ${res.status}: ${JSON.stringify(res.body).slice(0,300)}`);
  }
}

// ─── Main ─────────────────────────────────────────────────────────────────────

(async () => {
  console.log(`╔══ DDNL Weather Seed (Historical) ══════════════════════════════╗`);
  console.log(`  range  : ${START_DATE} → ${END_DATE}`);
  console.log(`  stores : ${STORES.length}`);
  console.log(`  api    : ${API_BASE}`);
  console.log(`╠═════════════════════════════════════════════════════════════════╣`);

  // Step 0: ensure the weather dataset exists as a provided dataset
  process.stdout.write(`  Ensuring weather dataset... `);
  await ensureWeatherDataset();

  let totalIngested = 0;
  let totalFailed   = 0;

  for (const store of STORES) {
    process.stdout.write(`  ${store.id}  ${(store.city + ', ' + store.state).padEnd(18)}  `);

    let rows;
    try {
      rows = await fetchArchive(store, START_DATE, END_DATE);
    } catch (e) {
      console.log(`✗ FETCH — ${e.message}`);
      totalFailed++;
      continue;
    }

    let ingested = 0;
    for (const batch of chunk(rows, 50)) {
      try {
        ingested += await ingestDocs(batch);
      } catch (e) {
        console.log(`\n    ✗ INGEST — ${e.message}`);
        totalFailed++;
      }
    }

    totalIngested += ingested;
    const rainy = rows.filter(r => r.is_rainy).length;
    const snowy = rows.filter(r => r.is_snowy).length;
    const hot   = rows.filter(r => r.is_hot).length;
    const cold  = rows.filter(r => r.is_cold).length;
    console.log(`✓  ${rows.length} days  rain:${rainy}  snow:${snowy}  hot:${hot}  cold:${cold}  → ${ingested} upserted`);

    // Polite pause between stores
    await new Promise(r => setTimeout(r, 400));
  }

  console.log(`╠═════════════════════════════════════════════════════════════════╣`);
  console.log(`  Total upserted : ${totalIngested.toLocaleString()}`);
  console.log(`  Failed stores  : ${totalFailed}`);
  console.log(`╚═════════════════════════════════════════════════════════════════╝`);
})().catch(e => {
  console.error('\nSeed crashed:', e.message);
  process.exit(1);
});
