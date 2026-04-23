#!/usr/bin/env node
/**
 * generate-sales-csv.js  —  DDNL nightly per-store sales generator
 *
 * Generates weather-correlated sales rows per store and posts directly
 * to the ingest API — no FTP, no intermediate files.
 *
 * Chaos modes per store (ftp-config.js still drives skip/defect/dupe rates):
 *   • skip_rate / streak_days  — whole store skipped, multi-day outage streaks
 *   • defect_rate              — bad rows injected
 *   • dupe_rate                — duplicate PKs (upsert makes them idempotent)
 *
 * Usage:
 *   node generate-sales-csv.js [YYYY-MM-DD]      # all stores, defaults to today
 *   node generate-sales-csv.js 2026-04-20 store_003  # single store
 *
 * Env:
 *   CHAOS_OFF=1   disable all chaos (clean data)
 *   DRY_RUN=1     generate rows but skip ingest POST
 */

'use strict';
const fs     = require('fs');
const path   = require('path');
const crypto = require('crypto');
const https  = require('https');
const http   = require('http');
const CFG    = require('./ftp-config');

// ─── Config ───────────────────────────────────────────────────────────────────

const API_BASE    = process.env.API_BASE    || 'https://produce-analytics-production.up.railway.app';
const API_KEY     = process.env.API_KEY     || 'ik_ef3ed1ba7c466fb4ef44b930a06788abbc6066c14960dcf9ac8a7138fd0aa921';
const INSTANCE_ID = process.env.INSTANCE_ID || 'produce';
const DATASET     = 'sales';

const targetDate = process.argv[2] || new Date().toISOString().slice(0, 10);
const onlyStore  = process.argv[3] || null;
const CHAOS_OFF  = process.env.CHAOS_OFF === '1';
const DRY_RUN    = process.env.DRY_RUN   === '1';

const LOG_DIR    = path.join(__dirname, 'logs');
const GEN_LOG    = path.join(LOG_DIR, 'generator.log.jsonl');
const STREAK_FILE = path.join(__dirname, 'ftp-streak-state.json');

fs.mkdirSync(LOG_DIR, { recursive: true });

// ─── Store metadata ───────────────────────────────────────────────────────────

const STORES_META = [
  { id: 'store_001', name: 'Seattle Pike Market',          city: 'Seattle',  state: 'WA', region: 'Pacific Northwest', store_type: 'Urban',    m: 1.50 },
  { id: 'store_002', name: 'Miami Fresh Market',           city: 'Miami',    state: 'FL', region: 'Southeast',         store_type: 'Urban',    m: 1.25 },
  { id: 'store_003', name: 'Chicago Green City',           city: 'Chicago',  state: 'IL', region: 'Midwest',           store_type: 'Urban',    m: 1.10 },
  { id: 'store_004', name: 'Phoenix Desert Fresh',         city: 'Phoenix',  state: 'AZ', region: 'Southwest',         store_type: 'Suburban', m: 0.95 },
  { id: 'store_005', name: 'NYC Union Square Greenmarket', city: 'New York', state: 'NY', region: 'Northeast',         store_type: 'Urban',    m: 1.00 },
  { id: 'store_006', name: 'Denver Mile High Produce',     city: 'Denver',   state: 'CO', region: 'Mountain',          store_type: 'Urban',    m: 0.75 },
  { id: 'store_007', name: 'Atlanta Ponce City Market',    city: 'Atlanta',  state: 'GA', region: 'Southeast',         store_type: 'Urban',    m: 0.50 },
];

// ─── Taxonomy ─────────────────────────────────────────────────────────────────

const TAXONOMY = [
  { department: 'Fresh Produce', category: 'Vegetables', subcategory: 'Leafy Greens', items: [
    { name: 'Romaine Lettuce',  plu: 'PLU-3053', unit: 'head',  basePrice: 2.49, baseUnits: 120 },
    { name: 'Iceberg Lettuce',  plu: 'PLU-3054', unit: 'head',  basePrice: 1.99, baseUnits: 150 },
    { name: 'Spinach',          plu: 'PLU-3090', unit: 'bag',   basePrice: 3.49, baseUnits: 90  },
    { name: 'Kale',             plu: 'PLU-3627', unit: 'bunch', basePrice: 2.99, baseUnits: 70  },
    { name: 'Arugula',          plu: 'PLU-3096', unit: 'bag',   basePrice: 3.99, baseUnits: 45  },
  ]},
  { department: 'Fresh Produce', category: 'Vegetables', subcategory: 'Root Vegetables', items: [
    { name: 'Carrots',          plu: 'PLU-4562', unit: 'bag',   basePrice: 1.49, baseUnits: 200 },
    { name: 'Beets',            plu: 'PLU-3082', unit: 'bunch', basePrice: 2.79, baseUnits: 55  },
    { name: 'Turnips',          plu: 'PLU-3163', unit: 'bunch', basePrice: 1.99, baseUnits: 40  },
    { name: 'Parsnips',         plu: 'PLU-3213', unit: 'bunch', basePrice: 2.29, baseUnits: 30  },
    { name: 'Sweet Potato',     plu: 'PLU-4072', unit: 'lb',    basePrice: 1.29, baseUnits: 180 },
  ]},
  { department: 'Fresh Produce', category: 'Vegetables', subcategory: 'Brassicas', items: [
    { name: 'Broccoli',         plu: 'PLU-3083', unit: 'head',  basePrice: 2.49, baseUnits: 160 },
    { name: 'Cauliflower',      plu: 'PLU-3149', unit: 'head',  basePrice: 3.29, baseUnits: 100 },
    { name: 'Cabbage',          plu: 'PLU-3069', unit: 'head',  basePrice: 1.79, baseUnits: 90  },
    { name: 'Brussels Sprouts', plu: 'PLU-3924', unit: 'bag',   basePrice: 3.99, baseUnits: 65  },
    { name: 'Bok Choy',         plu: 'PLU-4565', unit: 'head',  basePrice: 2.19, baseUnits: 45  },
  ]},
  { department: 'Fresh Produce', category: 'Fruit', subcategory: 'Citrus', items: [
    { name: 'Navel Oranges',    plu: 'PLU-3107', unit: 'each',  basePrice: 1.29, baseUnits: 220 },
    { name: 'Lemons',           plu: 'PLU-4053', unit: 'each',  basePrice: 0.79, baseUnits: 300 },
    { name: 'Limes',            plu: 'PLU-4286', unit: 'each',  basePrice: 0.59, baseUnits: 350 },
    { name: 'Ruby Grapefruit',  plu: 'PLU-4279', unit: 'each',  basePrice: 1.49, baseUnits: 130 },
    { name: 'Clementines',      plu: 'PLU-3668', unit: 'bag',   basePrice: 4.99, baseUnits: 80  },
  ]},
  { department: 'Fresh Produce', category: 'Fruit', subcategory: 'Tropical', items: [
    { name: 'Bananas',          plu: 'PLU-4011', unit: 'lb',    basePrice: 0.49, baseUnits: 500 },
    { name: 'Mangoes',          plu: 'PLU-3114', unit: 'each',  basePrice: 1.49, baseUnits: 120 },
    { name: 'Pineapple',        plu: 'PLU-4430', unit: 'each',  basePrice: 2.99, baseUnits: 80  },
    { name: 'Avocado',          plu: 'PLU-4046', unit: 'each',  basePrice: 1.29, baseUnits: 280 },
    { name: 'Papaya',           plu: 'PLU-4394', unit: 'each',  basePrice: 2.49, baseUnits: 50  },
  ]},
  { department: 'Fresh Produce', category: 'Fruit', subcategory: 'Berries', items: [
    { name: 'Strawberries',     plu: 'PLU-3143', unit: 'pint',  basePrice: 3.99, baseUnits: 150 },
    { name: 'Blueberries',      plu: 'PLU-3219', unit: 'pint',  basePrice: 4.49, baseUnits: 110 },
    { name: 'Raspberries',      plu: 'PLU-3218', unit: 'pint',  basePrice: 4.99, baseUnits: 80  },
    { name: 'Blackberries',     plu: 'PLU-3221', unit: 'pint',  basePrice: 4.99, baseUnits: 60  },
  ]},
  { department: 'Organic', category: 'Organic Vegetables', subcategory: 'Organic Greens', items: [
    { name: 'Organic Spinach',  plu: 'PLU-O3090', unit: 'bag',   basePrice: 5.49, baseUnits: 55 },
    { name: 'Organic Kale',     plu: 'PLU-O3627', unit: 'bunch', basePrice: 4.49, baseUnits: 45 },
    { name: 'Organic Arugula',  plu: 'PLU-O3096', unit: 'bag',   basePrice: 5.99, baseUnits: 30 },
  ]},
  { department: 'Organic', category: 'Organic Fruit', subcategory: 'Organic Berries', items: [
    { name: 'Organic Strawberries', plu: 'PLU-O3143', unit: 'pint', basePrice: 6.49, baseUnits: 75 },
    { name: 'Organic Blueberries',  plu: 'PLU-O3219', unit: 'pint', basePrice: 6.99, baseUnits: 60 },
  ]},
  { department: 'Fresh Herbs', category: 'Culinary Herbs', subcategory: 'Fresh Herbs', items: [
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

// ─── Streak state ─────────────────────────────────────────────────────────────

function loadStreakState() {
  try { return JSON.parse(fs.readFileSync(STREAK_FILE, 'utf8')); }
  catch { return {}; }
}

function saveStreakState(state) {
  fs.writeFileSync(STREAK_FILE, JSON.stringify(state, null, 2));
}

// ─── Skip decision (streak-aware) ────────────────────────────────────────────

function shouldSkip(storeId, storeCfg, streakState, date) {
  if (CHAOS_OFF) return { skip: false };
  const key = storeId;
  const cur = streakState[key] || { skipping: false, streak_remaining: 0, streak_start: null };
  if (cur.skipping && cur.streak_remaining > 0) {
    return { skip: true, reason: `outage streak (${cur.streak_remaining} days remaining)`,
             newState: { skipping: true, streak_remaining: cur.streak_remaining - 1, streak_start: cur.streak_start } };
  }
  const skipRng = mulberry32(hashInt(`${date}|${storeId}|skip`));
  if (skipRng() < (storeCfg.skip_rate || 0)) {
    const streakLen = storeCfg.streak_days || 1;
    const reasons = ['POS system timeout','Export daemon crashed','Network outage',
                     'Disk quota exceeded','DB lock timeout','Power outage at location'];
    const reason = reasons[Math.floor(skipRng() * reasons.length)];
    return { skip: true, reason: `${reason} (streak: ${streakLen}d)`,
             newState: { skipping: true, streak_remaining: streakLen - 1, streak_start: date } };
  }
  return { skip: false, newState: { skipping: false, streak_remaining: 0, streak_start: null } };
}

// ─── Chaos injection ──────────────────────────────────────────────────────────

const GARBAGE = ['N/A','NULL','\\N','#REF!','','undefined','NaN','-1','ERROR'];

function injectDefect(row, rng) {
  const bad = { ...row };
  switch (Math.floor(rng() * 10)) {
    case 0: delete bad.sales_pk;                                           bad._defect = 'missing_sales_pk';    break;
    case 1: delete bad.units_sold;                                         bad._defect = 'missing_units_sold';  break;
    case 2: bad.revenue = 'TWELVE DOLLARS'; bad.unit_price = 'a lot';     bad._defect = 'non_numeric_revenue'; break;
    case 3: bad.unit_price = -Math.abs(row.unit_price);
            bad.revenue = round2(bad.unit_price * (row.units_sold||1));    bad._defect = 'negative_price';     break;
    case 4: bad.units_sold=0; bad.item_count=0; bad.revenue=0;
            bad.transactions=0;                                            bad._defect = 'zero_units';          break;
    case 5: bad.revenue = round2((row.revenue||0)*100);                    bad._defect = 'revenue_100x';        break;
    case 6: bad.store_id = GARBAGE[Math.floor(rng()*GARBAGE.length)];     bad._defect = 'garbage_store';       break;
    case 7: bad.date='2099-12-31';
            bad.sales_pk=`2099-12-31__${row.store_id}__${row.plu_code}`;  bad._defect = 'future_date';         break;
    case 8: delete bad.department; delete bad.category;                    bad._defect = 'missing_taxonomy';    break;
    case 9: bad.plu_code = GARBAGE[Math.floor(rng()*5)];                  bad._defect = 'garbage_item';        break;
  }
  return bad;
}

// ─── Growth + bad-day model ───────────────────────────────────────────────────

const STORE_GROWTH = {
  store_001:0.10, store_002:0.08, store_003:0.06, store_004:0.05,
  store_005:0.12, store_006:0.04, store_007:0.03,
};
const STORE_BAD_DAY_RATE = {
  store_001:0.04, store_002:0.05, store_003:0.06, store_004:0.07,
  store_005:0.04, store_006:0.08, store_007:0.10,
};
const GROWTH_BASELINE = '2026-01-01';

function growthMultiplier(storeId, date) {
  const days = (new Date(date+'T00:00:00Z') - new Date(GROWTH_BASELINE+'T00:00:00Z')) / 86400000;
  if (days <= 0) return 1.0;
  return Math.pow(1 + (STORE_GROWTH[storeId]??0.05), days/365);
}

function isBadDay(storeId, date) {
  if (CHAOS_OFF) return false;
  return mulberry32(hashInt(`${date}|${storeId}|badday`))() < (STORE_BAD_DAY_RATE[storeId]??0.05);
}

// ─── Row generator ────────────────────────────────────────────────────────────

function generateStoreRows(store, date) {
  const storeCfg = CFG.stores[store.id] || {};
  const dow      = new Date(date+'T12:00:00').getDay();
  const dowMult  = (dow===0||dow===6) ? 1.2 : 1.0;
  const growth   = growthMultiplier(store.id, date);
  const badDay   = isBadDay(store.id, date);
  const badMul   = badDay ? (0.30 + mulberry32(hashInt(`${date}|${store.id}|baddaymag`))() * 0.30) : 1.0;
  const rows     = [];

  for (const cat of TAXONOMY) {
    for (const item of cat.items) {
      const rng   = mulberry32(hashInt(`${date}|${store.id}|${item.plu}`));
      const units = Math.max(1, Math.round(item.baseUnits * store.m * dowMult * growth * badMul * (0.6 + rng() * 0.8)));
      const price = round2(item.basePrice * (0.95 + rng() * 0.10));
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
        item_count:   Math.round(units * (1.0 + rng() * 0.4)),
        unit_price:   price,
        revenue:      round2(units * price),
        transactions: Math.max(1, Math.round(units * (0.4 + rng() * 0.4))),
      });
    }
  }
  return rows;
}

// ─── Chaos application ────────────────────────────────────────────────────────

function applyChaos(rows, storeId, date) {
  if (CHAOS_OFF) return { rows, defects: 0, dupes: 0 };
  const storeCfg   = CFG.stores[storeId] || {};
  const defectRate = storeCfg.defect_rate ?? 0.08;
  const dupeRate   = storeCfg.dupe_rate   ?? 0.03;
  const rng        = mulberry32(hashInt(`${date}|${storeId}|chaos`));
  const out = []; let defects = 0, dupes = 0;
  const duped = new Set();
  for (const row of rows) {
    let r = { ...row };
    if (rng() < defectRate) { r = injectDefect(r, rng); defects++; }
    if (rng() < dupeRate && r.sales_pk && !duped.has(r.sales_pk)) {
      duped.add(r.sales_pk);
      const c = { ...r }; delete c._defect;
      out.push(c, { ...c }); dupes++; continue;
    }
    const c = { ...r }; delete c._defect;
    out.push(c);
  }
  return { rows: out, defects, dupes };
}

// ─── Ingest API call ──────────────────────────────────────────────────────────

function postToIngest(records) {
  return new Promise((resolve, reject) => {
    const body = JSON.stringify({ docs: records });
    const url  = new URL(`${API_BASE}/api/ingest/${DATASET}/bulk`);
    const mod  = url.protocol === 'https:' ? https : http;
    const req  = mod.request({
      hostname: url.hostname,
      port:     url.port || (url.protocol === 'https:' ? 443 : 80),
      path:     url.pathname,
      method:   'POST',
      headers:  {
        'Content-Type':  'application/json',
        'Content-Length': Buffer.byteLength(body),
        'X-API-Key':     API_KEY,
        'X-Instance-Id': INSTANCE_ID,
      }
    }, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        try { resolve({ status: res.statusCode, body: JSON.parse(data) }); }
        catch { resolve({ status: res.statusCode, body: data }); }
      });
    });
    req.on('error', reject);
    req.setTimeout(60000, () => { req.destroy(); reject(new Error('Ingest request timeout')); });
    req.write(body);
    req.end();
  });
}

// ─── Logger ───────────────────────────────────────────────────────────────────

function writeLog(entry) {
  fs.appendFileSync(GEN_LOG, JSON.stringify({ ts: new Date().toISOString(), ...entry }) + '\n');
}

// ─── Main ─────────────────────────────────────────────────────────────────────

(async () => {
  const runId       = crypto.randomBytes(6).toString('hex');
  const streakState = loadStreakState();
  const stores      = onlyStore ? STORES_META.filter(s => s.id === onlyStore) : STORES_META;

  console.log(`╔══ DDNL Sales Generator ════════════════════════════════════════╗`);
  console.log(`  run_id   : ${runId}`);
  console.log(`  date     : ${targetDate}  |  stores: ${stores.length}  |  chaos: ${CHAOS_OFF?'OFF':'ON'}  |  dry: ${DRY_RUN?'YES':'NO'}`);
  console.log(`  endpoint : ${API_BASE}/api/ingest/${DATASET}/bulk`);
  console.log(`╠════════════════════════════════════════════════════════════════╣`);

  const summary = { run_id: runId, date: targetDate, stores: {} };

  for (const store of stores) {
    const storeCfg = CFG.stores[store.id] || {};
    process.stdout.write(`  ${store.id}  ${store.name.padEnd(28)}`);

    // Skip decision
    const { skip, reason, newState } = shouldSkip(store.id, storeCfg, streakState, targetDate);
    if (newState) streakState[store.id] = newState;
    if (skip) {
      console.log(`✗ SKIPPED — ${reason}`);
      writeLog({ event:'store_skipped', run_id:runId, date:targetDate, store_id:store.id, reason });
      summary.stores[store.id] = { status:'skipped', reason };
      continue;
    }

    // Generate
    const cleanRows          = generateStoreRows(store, targetDate);
    const { rows, defects, dupes } = applyChaos(cleanRows, store.id, targetDate);
    const badDay             = isBadDay(store.id, targetDate);
    const growthPct          = ((growthMultiplier(store.id, targetDate)-1)*100).toFixed(1);

    if (DRY_RUN) {
      console.log(`✓ DRY  ${rows.length} rows  defects:${defects}  dupes:${dupes}  growth:+${growthPct}%${badDay?' ⚠ BAD DAY':''}`);
      summary.stores[store.id] = { status:'dry', rows:rows.length };
      continue;
    }

    // POST to ingest API
    try {
      const t0  = Date.now();
      const res = await postToIngest(rows);
      const ms  = Date.now() - t0;
      if (res.status !== 200) throw new Error(`HTTP ${res.status}: ${JSON.stringify(res.body).slice(0,200)}`);
      const { indexed, failed } = res.body || {};
      console.log(`✓  ${rows.length} rows → indexed:${indexed??'?'}  failed:${failed??'?'}  defects:${defects}  dupes:${dupes}  growth:+${growthPct}%${badDay?' ⚠ BAD DAY':''}  (${ms}ms)`);
      writeLog({ event:'store_ingested', run_id:runId, date:targetDate, store_id:store.id,
                 rows:rows.length, indexed, failed, defects, dupes, bad_day:badDay, growth_pct:parseFloat(growthPct), duration_ms:ms });
      summary.stores[store.id] = { status:'ok', rows:rows.length, indexed, failed };
    } catch(e) {
      console.log(`✗ INGEST FAIL — ${e.message}`);
      writeLog({ event:'ingest_failed', run_id:runId, date:targetDate, store_id:store.id, error:e.message });
      summary.stores[store.id] = { status:'error', error:e.message };
    }
  }

  saveStreakState(streakState);

  const ok      = Object.values(summary.stores).filter(s=>s.status==='ok').length;
  const skipped = Object.values(summary.stores).filter(s=>s.status==='skipped').length;
  const failed  = Object.values(summary.stores).filter(s=>s.status==='error').length;
  console.log(`╠════════════════════════════════════════════════════════════════╣`);
  console.log(`  ${ok} ingested  |  ${skipped} skipped  |  ${failed} errors`);
  console.log(`╚════════════════════════════════════════════════════════════════╝\n`);

  writeLog({ event:'run_complete', run_id:runId, date:targetDate, ok, skipped, failed });

  if (failed > 3) {
    // Crash exit so cron notification fires
    process.exit(1);
  }
})().catch(e => {
  console.error('\nGenerator crashed:', e.message);
  fs.appendFileSync(GEN_LOG, JSON.stringify({ ts:new Date().toISOString(), event:'generator_crash', error:e.message })+'\n');
  process.exit(1);
});
