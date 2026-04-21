#!/usr/bin/env node
/**
 * reseed-sales-weather.js  —  Regenerate all historical sales with weather correlation
 *
 * For each day in the range, fetches the real weather traffic_index from ES
 * for each store and bakes it into unit volumes at generation time, then
 * pushes directly to the ingest API (replace=false → upsert by sales_pk).
 *
 * Weather effects applied:
 *   - traffic_index (from weather dataset): scales all unit volumes
 *   - Seasonal category mix: cold weather boosts root veg/citrus,
 *     hot weather boosts berries/tropical, moderate = balanced
 *
 * Usage:
 *   node reseed-sales-weather.js                         # 2024-01-01 → yesterday
 *   node reseed-sales-weather.js 2025-01-01              # custom start
 *   node reseed-sales-weather.js 2025-01-01 2025-06-30  # custom range
 *
 * Env:
 *   CONCURRENCY=3   parallel store workers per day (default 3)
 *   DRY_RUN=1       generate but don't ingest
 */

'use strict';

const https  = require('https');
const http   = require('http');
const crypto = require('crypto');

// ─── Config ───────────────────────────────────────────────────────────────────

const API_BASE    = process.env.API_BASE    || 'https://produce-analytics-production.up.railway.app';
const API_KEY     = process.env.API_KEY     || 'ik_ef3ed1ba7c466fb4ef44b930a06788abbc6066c14960dcf9ac8a7138fd0aa921';
const INSTANCE_ID = process.env.INSTANCE_ID || 'produce';
const ES_BASE     = process.env.ES_BASE     || 'https://elasticsearch-production-1c60.up.railway.app';
const ES_AUTH     = process.env.ES_AUTH     || 'elastic:changeme';
const DRY_RUN     = process.env.DRY_RUN === '1';
const CONCURRENCY = parseInt(process.env.CONCURRENCY || '3', 10);

const yesterday  = new Date(Date.now() - 86400000).toISOString().slice(0, 10);
const START_DATE = process.argv[2] || '2024-01-01';
const END_DATE   = process.argv[3] || yesterday;

// ─── Stores (must match generator exactly) ────────────────────────────────────

const STORES_META = [
  { id: 'store_001', name: 'Seattle Pike Market',          address: '1531 Pike Pl',             city: 'Seattle',  state: 'WA', zip: '98101', region: 'Pacific Northwest', store_type: 'Urban',    m: 1.50 },
  { id: 'store_002', name: 'Miami Fresh Market',           address: '3250 NW 27th Ave',         city: 'Miami',    state: 'FL', zip: '33142', region: 'Southeast',         store_type: 'Urban',    m: 1.25 },
  { id: 'store_003', name: 'Chicago Green City',           address: '300 E Randolph St',        city: 'Chicago',  state: 'IL', zip: '60601', region: 'Midwest',           store_type: 'Urban',    m: 1.10 },
  { id: 'store_004', name: 'Phoenix Desert Fresh',         address: '4710 E Camelback Rd',      city: 'Phoenix',  state: 'AZ', zip: '85018', region: 'Southwest',         store_type: 'Suburban', m: 0.95 },
  { id: 'store_005', name: 'NYC Union Square Greenmarket', address: '1 Union Square W',         city: 'New York', state: 'NY', zip: '10003', region: 'Northeast',         store_type: 'Urban',    m: 1.00 },
  { id: 'store_006', name: 'Denver Mile High Produce',     address: '1600 Glenarm Pl',          city: 'Denver',   state: 'CO', zip: '80202', region: 'Mountain',          store_type: 'Urban',    m: 0.75 },
  { id: 'store_007', name: 'Atlanta Ponce City Market',    address: '675 Ponce De Leon Ave NE', city: 'Atlanta',  state: 'GA', zip: '30308', region: 'Southeast',         store_type: 'Urban',    m: 0.50 },
];

// ─── Taxonomy ─────────────────────────────────────────────────────────────────

const TAXONOMY = [
  { department: 'Fresh Produce', category: 'Vegetables', subcategory: 'Leafy Greens', seasonBias: 'neutral', items: [
    { name: 'Romaine Lettuce',  plu: 'PLU-3053', unit: 'head',  basePrice: 2.49, baseUnits: 120 },
    { name: 'Iceberg Lettuce',  plu: 'PLU-3054', unit: 'head',  basePrice: 1.99, baseUnits: 150 },
    { name: 'Spinach',          plu: 'PLU-3090', unit: 'bag',   basePrice: 3.49, baseUnits: 90  },
    { name: 'Kale',             plu: 'PLU-3627', unit: 'bunch', basePrice: 2.99, baseUnits: 70  },
    { name: 'Arugula',          plu: 'PLU-3096', unit: 'bag',   basePrice: 3.99, baseUnits: 45  },
  ]},
  { department: 'Fresh Produce', category: 'Vegetables', subcategory: 'Root Vegetables', seasonBias: 'cold', items: [
    { name: 'Carrots',          plu: 'PLU-4562', unit: 'bag',   basePrice: 1.49, baseUnits: 200 },
    { name: 'Beets',            plu: 'PLU-3082', unit: 'bunch', basePrice: 2.79, baseUnits: 55  },
    { name: 'Turnips',          plu: 'PLU-3163', unit: 'bunch', basePrice: 1.99, baseUnits: 40  },
    { name: 'Parsnips',         plu: 'PLU-3213', unit: 'bunch', basePrice: 2.29, baseUnits: 30  },
    { name: 'Sweet Potato',     plu: 'PLU-4072', unit: 'lb',    basePrice: 1.29, baseUnits: 180 },
  ]},
  { department: 'Fresh Produce', category: 'Vegetables', subcategory: 'Brassicas', seasonBias: 'cold', items: [
    { name: 'Broccoli',         plu: 'PLU-3083', unit: 'head',  basePrice: 2.49, baseUnits: 160 },
    { name: 'Cauliflower',      plu: 'PLU-3149', unit: 'head',  basePrice: 3.29, baseUnits: 100 },
    { name: 'Cabbage',          plu: 'PLU-3069', unit: 'head',  basePrice: 1.79, baseUnits: 90  },
    { name: 'Brussels Sprouts', plu: 'PLU-3924', unit: 'bag',   basePrice: 3.99, baseUnits: 65  },
    { name: 'Bok Choy',         plu: 'PLU-4565', unit: 'head',  basePrice: 2.19, baseUnits: 45  },
  ]},
  { department: 'Fresh Produce', category: 'Fruit', subcategory: 'Citrus', seasonBias: 'cold', items: [
    { name: 'Navel Oranges',    plu: 'PLU-3107', unit: 'each',  basePrice: 1.29, baseUnits: 220 },
    { name: 'Lemons',           plu: 'PLU-4053', unit: 'each',  basePrice: 0.79, baseUnits: 300 },
    { name: 'Limes',            plu: 'PLU-4286', unit: 'each',  basePrice: 0.59, baseUnits: 350 },
    { name: 'Ruby Grapefruit',  plu: 'PLU-4279', unit: 'each',  basePrice: 1.49, baseUnits: 130 },
    { name: 'Clementines',      plu: 'PLU-3668', unit: 'bag',   basePrice: 4.99, baseUnits: 80  },
  ]},
  { department: 'Fresh Produce', category: 'Fruit', subcategory: 'Tropical', seasonBias: 'hot', items: [
    { name: 'Bananas',          plu: 'PLU-4011', unit: 'lb',    basePrice: 0.49, baseUnits: 500 },
    { name: 'Mangoes',          plu: 'PLU-3114', unit: 'each',  basePrice: 1.49, baseUnits: 120 },
    { name: 'Pineapple',        plu: 'PLU-4430', unit: 'each',  basePrice: 2.99, baseUnits: 80  },
    { name: 'Avocado',          plu: 'PLU-4046', unit: 'each',  basePrice: 1.29, baseUnits: 280 },
    { name: 'Papaya',           plu: 'PLU-4394', unit: 'each',  basePrice: 2.49, baseUnits: 50  },
  ]},
  { department: 'Fresh Produce', category: 'Fruit', subcategory: 'Berries', seasonBias: 'hot', items: [
    { name: 'Strawberries',     plu: 'PLU-3143', unit: 'pint',  basePrice: 3.99, baseUnits: 150 },
    { name: 'Blueberries',      plu: 'PLU-3219', unit: 'pint',  basePrice: 4.49, baseUnits: 110 },
    { name: 'Raspberries',      plu: 'PLU-3218', unit: 'pint',  basePrice: 4.99, baseUnits: 80  },
    { name: 'Blackberries',     plu: 'PLU-3221', unit: 'pint',  basePrice: 4.99, baseUnits: 60  },
  ]},
  { department: 'Organic', category: 'Organic Vegetables', subcategory: 'Organic Greens', seasonBias: 'neutral', items: [
    { name: 'Organic Spinach',  plu: 'PLU-O3090', unit: 'bag',   basePrice: 5.49, baseUnits: 55 },
    { name: 'Organic Kale',     plu: 'PLU-O3627', unit: 'bunch', basePrice: 4.49, baseUnits: 45 },
    { name: 'Organic Arugula',  plu: 'PLU-O3096', unit: 'bag',   basePrice: 5.99, baseUnits: 30 },
  ]},
  { department: 'Organic', category: 'Organic Fruit', subcategory: 'Organic Berries', seasonBias: 'hot', items: [
    { name: 'Organic Strawberries', plu: 'PLU-O3143', unit: 'pint', basePrice: 6.49, baseUnits: 75 },
    { name: 'Organic Blueberries',  plu: 'PLU-O3219', unit: 'pint', basePrice: 6.99, baseUnits: 60 },
  ]},
  { department: 'Fresh Herbs', category: 'Culinary Herbs', subcategory: 'Fresh Herbs', seasonBias: 'hot', items: [
    { name: 'Basil',            plu: 'PLU-4899', unit: 'bunch', basePrice: 2.49, baseUnits: 80  },
    { name: 'Cilantro',         plu: 'PLU-4898', unit: 'bunch', basePrice: 1.49, baseUnits: 120 },
    { name: 'Parsley',          plu: 'PLU-4901', unit: 'bunch', basePrice: 1.29, baseUnits: 90  },
    { name: 'Mint',             plu: 'PLU-4902', unit: 'bunch', basePrice: 1.99, baseUnits: 60  },
    { name: 'Rosemary',         plu: 'PLU-4903', unit: 'bunch', basePrice: 2.29, baseUnits: 45  },
  ]},
];

// ─── RNG helpers ──────────────────────────────────────────────────────────────

function mulberry32(seed) {
  return function() {
    seed |= 0; seed = seed + 0x6D2B79F5 | 0;
    let t = Math.imul(seed ^ seed >>> 15, 1 | seed);
    t = t + Math.imul(t ^ t >>> 7, 61 | t) ^ t;
    return ((t ^ t >>> 14) >>> 0) / 4294967296;
  };
}
function hashInt(str) {
  let h = 5381;
  for (let i = 0; i < str.length; i++) h = (Math.imul(h, 33) ^ str.charCodeAt(i)) >>> 0;
  return h;
}
function round2(n) { return Math.round(n * 100) / 100; }

// ─── Growth model (matches generator) ────────────────────────────────────────

const STORE_GROWTH = {
  store_001: 0.10, store_002: 0.08, store_003: 0.06,
  store_004: 0.05, store_005: 0.12, store_006: 0.04, store_007: 0.03,
};
const STORE_BAD_DAY_RATE = {
  store_001: 0.04, store_002: 0.05, store_003: 0.06,
  store_004: 0.07, store_005: 0.04, store_006: 0.08, store_007: 0.10,
};
const GROWTH_BASELINE = '2026-01-01';

function growthMultiplier(storeId, date) {
  const base    = new Date(GROWTH_BASELINE + 'T00:00:00Z');
  const target  = new Date(date + 'T00:00:00Z');
  const daysOut = (target - base) / 86400000;
  if (daysOut <= 0) return 1.0;
  return Math.pow(1 + (STORE_GROWTH[storeId] ?? 0.05), daysOut / 365);
}

function isBadDay(storeId, date) {
  const rate = STORE_BAD_DAY_RATE[storeId] ?? 0.05;
  const rng  = mulberry32(hashInt(`${date}|${storeId}|badday`));
  return rng() < rate;
}

// ─── Seasonal category mix multiplier ────────────────────────────────────────
// tempAvg drives category-level demand shifts on top of overall traffic_index.
// cold bias categories get boosted in winter; hot bias in summer.

function categoryMixMultiplier(seasonBias, tempAvgF) {
  if (!tempAvgF) return 1.0;
  if (seasonBias === 'cold') {
    // Peaks at 32°F (+30%), neutral at 65°F, slight suppression above 85°F
    return Math.max(0.75, Math.min(1.30, 1.0 + (65 - tempAvgF) / 110));
  }
  if (seasonBias === 'hot') {
    // Peaks at 90°F (+25%), neutral at 65°F, suppressed below 40°F
    return Math.max(0.75, Math.min(1.25, 1.0 + (tempAvgF - 65) / 100));
  }
  return 1.0; // neutral
}

// ─── ES weather lookup ────────────────────────────────────────────────────────
// Batch-fetch all 7 store weather docs for a given date in one mget call.

function esGet(path, body) {
  return new Promise((resolve, reject) => {
    const [user, pass] = ES_AUTH.split(':');
    const url  = new URL(path, ES_BASE);
    const mod  = url.protocol === 'https:' ? https : http;
    const payload = body ? JSON.stringify(body) : null;
    const headers = {
      'Authorization': 'Basic ' + Buffer.from(`${user}:${pass}`).toString('base64'),
      'Content-Type':  'application/json',
    };
    if (payload) headers['Content-Length'] = Buffer.byteLength(payload);
    const req = mod.request({
      hostname: url.hostname,
      port:     url.port || (url.protocol === 'https:' ? 443 : 80),
      path:     url.pathname,
      method:   body ? 'POST' : 'GET',
      headers,
    }, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        try { resolve(JSON.parse(data)); }
        catch (e) { reject(new Error(`ES parse error: ${data.slice(0, 200)}`)); }
      });
    });
    req.on('error', reject);
    if (payload) req.write(payload);
    req.end();
  });
}

// Returns { store_id: { traffic_index, temp_avg_f, condition, ... } }
async function fetchDayWeather(date) {
  const pks = STORES_META.map(s => `${date}__${s.id}`);
  const result = await esGet('/produce__weather__v1/_search', {
    size: 7,
    query: { terms: { weather_pk: pks } },
    _source: ['store_id', 'traffic_index', 'temp_avg_f', 'condition', 'is_extreme', 'is_rainy', 'is_snowy'],
  });
  const map = {};
  for (const hit of (result.hits?.hits || [])) {
    map[hit._source.store_id] = hit._source;
  }
  return map;
}

// ─── Row generator ────────────────────────────────────────────────────────────

function generateRows(store, date, weatherDoc) {
  const dow       = new Date(date + 'T12:00:00').getDay();
  const dowMult   = (dow === 0 || dow === 6) ? 1.2 : 1.0;
  const growthMul = growthMultiplier(store.id, date);
  const badDay    = isBadDay(store.id, date);
  const badDayMul = badDay
    ? (0.30 + mulberry32(hashInt(`${date}|${store.id}|baddaymag`))() * 0.30)
    : 1.0;

  // Weather effects — gracefully degrade if no weather doc
  const trafficIdx = weatherDoc?.traffic_index ?? 1.0;
  const tempAvg    = weatherDoc?.temp_avg_f    ?? 65;

  const rows = [];

  for (const cat of TAXONOMY) {
    const catMix = categoryMixMultiplier(cat.seasonBias, tempAvg);

    for (const item of cat.items) {
      const rowRng = mulberry32(hashInt(`${date}|${store.id}|${item.plu}`));
      const v      = rowRng();
      const units  = Math.max(1, Math.round(
        item.baseUnits * store.m * dowMult * growthMul * badDayMul * trafficIdx * catMix * (0.6 + v * 0.8)
      ));
      const price  = round2(item.basePrice * (0.95 + rowRng() * 0.10));

      rows.push({
        sales_pk:     `${date}__${store.id}__${item.plu}`,
        date,
        store_id:     store.id,
        store_name:   store.name,
        store_city:   store.city,
        store_state:  store.state,
        store_region: store.region,
        store_type:   store.store_type,
        department:   cat.department,
        category:     cat.category,
        subcategory:  cat.subcategory,
        item_name:    item.name,
        plu_code:     item.plu,
        unit_type:    item.unit,
        units_sold:   units,
        item_count:   Math.round(units * (1.0 + rowRng() * 0.4)),
        unit_price:   price,
        revenue:      round2(units * price),
        transactions: Math.max(1, Math.round(units * (0.4 + rowRng() * 0.4))),
      });
    }
  }
  return rows;
}

// ─── Ingest helpers ───────────────────────────────────────────────────────────

function apiPost(urlPath, body) {
  return new Promise((resolve, reject) => {
    const payload = JSON.stringify(body);
    const url     = new URL(urlPath, API_BASE);
    const mod     = url.protocol === 'https:' ? https : http;
    const req = mod.request({
      hostname: url.hostname,
      port:     url.port || (url.protocol === 'https:' ? 443 : 80),
      path:     url.pathname,
      method:   'POST',
      headers: {
        'Content-Type':   'application/json',
        'Content-Length': Buffer.byteLength(payload),
        'X-API-Key':      API_KEY,
        'X-Instance-Id':  INSTANCE_ID,
      },
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

async function ingestDocs(docs) {
  if (DRY_RUN) return docs.length;
  const res = await apiPost('/api/ingest/sales/bulk', { docs, replace: false });
  if (res.status !== 200) throw new Error(`HTTP ${res.status}: ${JSON.stringify(res.body).slice(0, 200)}`);
  return res.body.indexed ?? docs.length;
}

// ─── Date helpers ─────────────────────────────────────────────────────────────

function dateRange(start, end) {
  const dates = [];
  const cur   = new Date(start + 'T00:00:00Z');
  const stop  = new Date(end   + 'T00:00:00Z');
  while (cur <= stop) {
    dates.push(cur.toISOString().slice(0, 10));
    cur.setUTCDate(cur.getUTCDate() + 1);
  }
  return dates;
}

// ─── Main ─────────────────────────────────────────────────────────────────────

(async () => {
  const dates = dateRange(START_DATE, END_DATE);
  const totalDays = dates.length;

  console.log(`╔══ DDNL Sales Reseed (Weather-Correlated) ══════════════════════╗`);
  console.log(`  range      : ${START_DATE} → ${END_DATE}  (${totalDays} days)`);
  console.log(`  stores     : ${STORES_META.length}`);
  console.log(`  docs/day   : ~${STORES_META.length * 39} (${STORES_META.length} stores × 39 SKUs)`);
  console.log(`  total est  : ~${(totalDays * STORES_META.length * 39).toLocaleString()} docs`);
  console.log(`  dry_run    : ${DRY_RUN}`);
  console.log(`╠═════════════════════════════════════════════════════════════════╣`);

  let totalIngested = 0;
  let totalFailed   = 0;
  let daysProcessed = 0;
  const t0 = Date.now();

  for (const date of dates) {
    // Fetch all store weather for this day in one mget
    let weatherMap = {};
    try {
      weatherMap = await fetchDayWeather(date);
    } catch (e) {
      // Non-fatal — just proceed without weather for this day
    }

    const weatherCount = Object.keys(weatherMap).length;
    let dayIngested = 0;
    let dayFailed   = 0;

    // Generate + ingest all stores for this day
    const allDocs = [];
    for (const store of STORES_META) {
      const weatherDoc = weatherMap[store.id] || null;
      const rows = generateRows(store, date, weatherDoc);
      allDocs.push(...rows);
    }

    // Push all stores together in one batch (all 7 × 39 = 273 docs per day)
    try {
      dayIngested = await ingestDocs(allDocs);
      totalIngested += dayIngested;
    } catch (e) {
      dayFailed = allDocs.length;
      totalFailed += dayFailed;
    }

    daysProcessed++;

    // Progress every 30 days
    if (daysProcessed % 30 === 0 || daysProcessed === totalDays) {
      const elapsed  = ((Date.now() - t0) / 1000).toFixed(0);
      const pct      = ((daysProcessed / totalDays) * 100).toFixed(0);
      const wxStores = Object.keys(weatherMap).length;
      process.stdout.write(`\r  [${pct.padStart(3)}%]  day ${daysProcessed}/${totalDays}  ${date}  weather:${wxStores}/7  ingested:${totalIngested.toLocaleString()}  ${elapsed}s`);
    }
  }

  const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
  console.log(`\n╠═════════════════════════════════════════════════════════════════╣`);
  console.log(`  Days processed : ${daysProcessed}`);
  console.log(`  Docs ingested  : ${totalIngested.toLocaleString()}`);
  console.log(`  Docs failed    : ${totalFailed.toLocaleString()}`);
  console.log(`  Elapsed        : ${elapsed}s`);
  console.log(`╚═════════════════════════════════════════════════════════════════╝`);
})().catch(e => {
  console.error('\nReseed crashed:', e.message, e.stack);
  process.exit(1);
});
