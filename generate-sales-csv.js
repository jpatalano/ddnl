#!/usr/bin/env node
/**
 * generate-sales-csv.js  —  DDNL nightly per-store sales file generator
 *
 * Simulates a real multi-store POS FTP export pipeline.
 * Each store gets its own file with its own filename pattern, chaos settings,
 * and outage streak behavior — all driven by ftp-config.js.
 *
 * Chaos modes per store (see ftp-config.js to tune):
 *   • skip_rate / streak_days  — whole file missing, multi-day outage streaks
 *   • defect_rate              — bad rows: missing fields, wrong types, negative
 *                                prices, future dates, garbage strings
 *   • dupe_rate                — duplicate PKs within the file (re-send rows)
 *   • id_field_reliable        — if false, sales_pk in data is garbage (poller rebuilds)
 *   • store_id_in_file         — if false, store_id absent from rows (poller infers)
 *
 * Usage:
 *   node generate-sales-csv.js [YYYY-MM-DD]   # all stores, defaults to today
 *   node generate-sales-csv.js 2026-04-20 store_003   # single store
 *
 * Env:
 *   SKIP_FTP=1        write files locally only
 *   OUTPUT_DIR=path   also write CSV to this local dir
 *   CHAOS_OFF=1       disable all chaos (clean data, good for initial testing)
 */

'use strict';
const fs     = require('fs');
const path   = require('path');
const crypto = require('crypto');
const CFG    = require('./ftp-config');

// ─── Args / env ───────────────────────────────────────────────────────────────

const targetDate   = process.argv[2] || new Date().toISOString().slice(0, 10);
const onlyStore    = process.argv[3] || null;   // optional single-store filter
const SKIP_FTP     = process.env.SKIP_FTP   === '1';
const CHAOS_OFF    = process.env.CHAOS_OFF  === '1';
const OUTPUT_DIR   = process.env.OUTPUT_DIR || null;

const LOG_DIR      = path.join(__dirname, 'logs');
const GEN_LOG      = path.join(LOG_DIR, 'generator.log.jsonl');
const STREAK_FILE  = path.join(__dirname, 'ftp-streak-state.json');

fs.mkdirSync(LOG_DIR, { recursive: true });

// ─── Taxonomy (matches seed-produce.js exactly) ───────────────────────────────

const STORES_META = [
  { id: 'store_001', name: 'Seattle Pike Market',          address: '1531 Pike Pl',              city: 'Seattle',  state: 'WA', zip: '98101', region: 'Pacific Northwest', store_type: 'Urban',    lat: 47.6062, lon: -122.3321, timezone: 'America/Los_Angeles', m: 1.50 },
  { id: 'store_002', name: 'Miami Fresh Market',           address: '3250 NW 27th Ave',          city: 'Miami',    state: 'FL', zip: '33142', region: 'Southeast',         store_type: 'Urban',    lat: 25.7617, lon:  -80.1918, timezone: 'America/New_York',    m: 1.25 },
  { id: 'store_003', name: 'Chicago Green City',           address: '300 E Randolph St',         city: 'Chicago',  state: 'IL', zip: '60601', region: 'Midwest',           store_type: 'Urban',    lat: 41.8781, lon:  -87.6298, timezone: 'America/Chicago',     m: 1.10 },
  { id: 'store_004', name: 'Phoenix Desert Fresh',         address: '4710 E Camelback Rd',       city: 'Phoenix',  state: 'AZ', zip: '85018', region: 'Southwest',         store_type: 'Suburban', lat: 33.4484, lon: -112.0740, timezone: 'America/Phoenix',     m: 0.95 },
  { id: 'store_005', name: 'NYC Union Square Greenmarket', address: '1 Union Square W',          city: 'New York', state: 'NY', zip: '10003', region: 'Northeast',         store_type: 'Urban',    lat: 40.7128, lon:  -74.0060, timezone: 'America/New_York',    m: 1.00 },
  { id: 'store_006', name: 'Denver Mile High Produce',     address: '1600 Glenarm Pl',           city: 'Denver',   state: 'CO', zip: '80202', region: 'Mountain',          store_type: 'Urban',    lat: 39.7392, lon: -104.9903, timezone: 'America/Denver',      m: 0.75 },
  { id: 'store_007', name: 'Atlanta Ponce City Market',    address: '675 Ponce De Leon Ave NE',  city: 'Atlanta',  state: 'GA', zip: '30308', region: 'Southeast',         store_type: 'Urban',    lat: 33.7490, lon:  -84.3880, timezone: 'America/New_York',    m: 0.50 },
];

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

// ─── RNG helpers ─────────────────────────────────────────────────────────────

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

// ─── Streak state (persisted to disk) ────────────────────────────────────────
// Tracks consecutive skip days per store so streaks are coherent across runs

function loadStreakState() {
  try { return JSON.parse(fs.readFileSync(STREAK_FILE, 'utf8')); }
  catch { return {}; }
}

function saveStreakState(state) {
  fs.writeFileSync(STREAK_FILE, JSON.stringify(state, null, 2));
}

// ─── Filename builder ─────────────────────────────────────────────────────────

function buildFilename(storeId, date, pattern) {
  const yyyymmdd = date.replace(/-/g, '');
  return (pattern || 'sales_{store_id}_{date}.csv')
    .replace('{store_id}',  storeId)
    .replace('{date}',      date)
    .replace('{yyyymmdd}',  yyyymmdd);
}

// ─── Skip decision (streak-aware) ────────────────────────────────────────────

function shouldSkip(storeId, storeCfg, streakState, date) {
  if (CHAOS_OFF) return { skip: false };

  const key = storeId;
  const cur = streakState[key] || { skipping: false, streak_remaining: 0, streak_start: null };

  // Already in an active streak from a prior day?
  if (cur.skipping && cur.streak_remaining > 0) {
    return {
      skip:    true,
      reason:  `outage streak (${cur.streak_remaining} days remaining, started ${cur.streak_start})`,
      newState: { skipping: true, streak_remaining: cur.streak_remaining - 1, streak_start: cur.streak_start }
    };
  }

  // Roll for a new skip event today
  const skipRng = mulberry32(hashInt(`${date}|${storeId}|skip`));
  if (skipRng() < storeCfg.skip_rate) {
    const streakLen = storeCfg.streak_days || 1;
    const reasons   = ['POS system timeout', 'Export daemon crashed', 'Network outage',
                       'Disk quota exceeded', 'FTP auth failure', 'DB lock timeout',
                       'Power outage at location'];
    const reason    = reasons[Math.floor(skipRng() * reasons.length)];
    return {
      skip:    true,
      reason:  `${reason} (streak: ${streakLen} day${streakLen > 1 ? 's' : ''})`,
      newState: { skipping: true, streak_remaining: streakLen - 1, streak_start: date }
    };
  }

  // No skip — clear any stale streak state
  return { skip: false, newState: { skipping: false, streak_remaining: 0, streak_start: null } };
}

// ─── Chaos injection ──────────────────────────────────────────────────────────

const GARBAGE = ['N/A', 'NULL', '\\N', '#REF!', '#VALUE!', '??', '---', '',
                 'undefined', 'NaN', '-1', 'PENDING', 'ERROR', 'x'.repeat(256)];

function injectDefect(row, defectRng) {
  const type = Math.floor(defectRng() * 10);
  const bad  = { ...row };
  switch (type) {
    case 0:  delete bad.sales_pk;                                          bad._defect = 'missing_sales_pk';     break;
    case 1:  delete bad.units_sold;                                        bad._defect = 'missing_units_sold';   break;
    case 2:  bad.revenue = 'TWELVE DOLLARS'; bad.unit_price = 'a lot';    bad._defect = 'non_numeric_revenue';  break;
    case 3:  bad.unit_price = -Math.abs(row.unit_price);
             bad.revenue = round2(bad.unit_price * (row.units_sold || 1)); bad._defect = 'negative_price';      break;
    case 4:  bad.units_sold = 0; bad.item_count = 0;
             bad.revenue = 0; bad.transactions = 0;                        bad._defect = 'zero_units';           break;
    case 5:  bad.revenue = round2((row.revenue || 0) * 100);               bad._defect = 'revenue_100x';         break;
    case 6:  bad.store_id = GARBAGE[Math.floor(defectRng() * 8)];
             bad.store_name = GARBAGE[Math.floor(defectRng() * 8)];        bad._defect = 'garbage_store';        break;
    case 7:  bad.date = '2099-12-31';
             bad.sales_pk = `2099-12-31__${row.store_id}__${row.plu_code}`; bad._defect = 'future_date';         break;
    case 8:  delete bad.department; delete bad.category;                   bad._defect = 'missing_taxonomy';     break;
    case 9:  bad.plu_code = GARBAGE[Math.floor(defectRng() * 5)];
             bad.item_name = GARBAGE[Math.floor(defectRng() * 5)];         bad._defect = 'garbage_item';         break;
  }
  return bad;
}

// ─── Growth + bad-day model ──────────────────────────────────────────────────
//
// Each store grows at its own pace from a baseline date (2026-01-01).
// Growth is applied as a smooth multiplier that ramps up over ~18 months.
// Occasional "bad days" (equipment issues, slow traffic, etc.) cut volume
// by 40-70% for that store on that date — deterministic per date+store so
// re-running the same date always produces the same result.

// Per-store annual growth rate (realistic retail: 3-12%)
const STORE_GROWTH = {
  store_001: 0.10,  // Downtown flagship, strong growth
  store_002: 0.08,  // Westside, solid
  store_003: 0.06,  // Cedar Park, steady suburban
  store_004: 0.05,  // Round Rock, moderate
  store_005: 0.12,  // South Congress, hottest location
  store_006: 0.04,  // Lakeway, slower
  store_007: 0.03,  // Pflugerville, rural, minimal growth
};

// Bad-day probability per store per day (independent of skip/outage)
const STORE_BAD_DAY_RATE = {
  store_001: 0.04,  // ~1 bad day/month
  store_002: 0.05,
  store_003: 0.06,
  store_004: 0.07,
  store_005: 0.04,
  store_006: 0.08,  // more volatile
  store_007: 0.10,  // rural, most volatile
};

const GROWTH_BASELINE = '2026-01-01';

function growthMultiplier(storeId, date) {
  const base     = new Date(GROWTH_BASELINE + 'T00:00:00Z');
  const target   = new Date(date           + 'T00:00:00Z');
  const daysOut  = (target - base) / 86400000;   // may be negative for historical
  if (daysOut <= 0) return 1.0;                  // no modifier before baseline
  const annualRate = STORE_GROWTH[storeId] ?? 0.05;
  // Smooth compound daily growth
  return Math.pow(1 + annualRate, daysOut / 365);
}

function isBadDay(storeId, date) {
  if (CHAOS_OFF) return false;
  const rate = STORE_BAD_DAY_RATE[storeId] ?? 0.05;
  const rng  = mulberry32(hashInt(`${date}|${storeId}|badday`));
  return rng() < rate;
}

// ─── Row generator for one store ─────────────────────────────────────────────

function generateStoreRows(store, date) {
  const storeCfg  = CFG.stores[store.id] || {};
  const dow       = new Date(date + 'T12:00:00').getDay();
  const dowMult   = (dow === 0 || dow === 6) ? 1.2 : 1.0;
  const growthMul = growthMultiplier(store.id, date);
  // Bad day: volume cratered 40-70% (staffing issues, weather, equipment, etc.)
  const badDay    = isBadDay(store.id, date);
  const badDayMul = badDay ? (0.30 + mulberry32(hashInt(`${date}|${store.id}|baddaymag`))() * 0.30) : 1.0;
  const rows      = [];

  for (const cat of TAXONOMY) {
    for (const item of cat.items) {
      const rowRng = mulberry32(hashInt(`${date}|${store.id}|${item.plu}`));
      const v      = rowRng();
      const units  = Math.max(1, Math.round(item.baseUnits * store.m * dowMult * growthMul * badDayMul * (0.6 + v * 0.8)));
      const price  = round2(item.basePrice * (0.95 + rowRng() * 0.10));

      // If store_id_in_file=false we omit store fields — poller must infer
      const storeFields = storeCfg.store_id_in_file === false
        ? {}
        : { store_id: store.id, store_name: store.name, store_city: store.city,
            store_state: store.state, store_region: store.region, store_type: store.store_type };

      // If id_field_reliable=false we mangle the PK — poller rebuilds it
      const sales_pk = storeCfg.id_field_reliable === false
        ? `BADPK-${Math.floor(rowRng() * 99999)}`
        : `${date}__${store.id}__${item.plu}`;

      rows.push({
        sales_pk,
        date,
        ...storeFields,
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

// ─── Apply chaos to a store's rows ───────────────────────────────────────────

function applyChaos(rows, storeId, date) {
  if (CHAOS_OFF) return { rows, defects: [] };

  const storeCfg   = CFG.stores[storeId] || {};
  const defectRate = storeCfg.defect_rate ?? 0.08;
  const dupeRate   = storeCfg.dupe_rate   ?? 0.03;
  const chaosRng   = mulberry32(hashInt(`${date}|${storeId}|chaos`));

  const out     = [];
  const defects = [];
  const duped   = new Set();

  for (const row of rows) {
    let r = { ...row };

    // Defect injection
    if (chaosRng() < defectRate) {
      r = injectDefect(r, chaosRng);
      defects.push({ sales_pk: r.sales_pk ?? '(missing)', defect: r._defect });
    }

    // Duplicate row (simulates POS double-send)
    if (chaosRng() < dupeRate && r.sales_pk && !duped.has(r.sales_pk)) {
      duped.add(r.sales_pk);
      const clean1 = { ...r }; delete clean1._defect;
      out.push(clean1);
      out.push({ ...clean1 }); // exact dupe — upsert makes these idempotent
      defects.push({ sales_pk: r.sales_pk, defect: 'duplicate_pk' });
      continue;
    }

    const clean = { ...r }; delete clean._defect;
    out.push(clean);
  }

  return { rows: out, defects };
}

// ─── CSV serializer ───────────────────────────────────────────────────────────

const CSV_HEADERS = [
  'sales_pk','date','store_id','store_name','store_city','store_state',
  'store_region','store_type','department','category','subcategory',
  'item_name','plu_code','unit_type','units_sold','item_count',
  'unit_price','revenue','transactions',
];

function toCsv(rows) {
  const esc = v => {
    if (v === null || v === undefined) return '';
    const s = String(v);
    return s.includes(',') || s.includes('"') || s.includes('\n')
      ? `"${s.replace(/"/g, '""')}"` : s;
  };
  return [
    CSV_HEADERS.join(','),
    ...rows.map(r => CSV_HEADERS.map(h => esc(r[h])).join(','))
  ].join('\n');
}

// ─── FTP upload (via Python helper) ──────────────────────────────────────────

const { execFile } = require('child_process');

function uploadFile(localPath, remoteFilename) {
  return new Promise((resolve, reject) => {
    execFile('python3', [
      __dirname + '/ftp-upload.py',
      CFG.ftp.host, String(CFG.ftp.port), CFG.ftp.user, CFG.ftp.pass,
      localPath, remoteFilename
    ], { timeout: 20000 }, (err, stdout, stderr) => {
      if (err) reject(new Error(stderr || err.message));
      else resolve();
    });
  });
}

// ─── Structured logger ────────────────────────────────────────────────────────

function writeLog(entry) {
  fs.appendFileSync(GEN_LOG, JSON.stringify({ ts: new Date().toISOString(), ...entry }) + '\n');
}

// ─── Main ─────────────────────────────────────────────────────────────────────

(async () => {
  const runId      = crypto.randomBytes(6).toString('hex');
  const streakState = loadStreakState();
  const stores     = onlyStore
    ? STORES_META.filter(s => s.id === onlyStore)
    : STORES_META;

  console.log(`╔══ DDNL Sales Generator ═══════════════════════════════════════╗`);
  console.log(`  run_id  : ${runId}`);
  console.log(`  date    : ${targetDate}  |  stores: ${stores.length}  |  chaos: ${CHAOS_OFF ? 'OFF' : 'ON'}`);
  console.log(`╠════════════════════════════════════════════════════════════════╣`);

  const summary = { run_id: runId, date: targetDate, stores: {} };

  for (const store of stores) {
    const storeCfg = CFG.stores[store.id] || {};
    const filename = buildFilename(store.id, targetDate, storeCfg.filename_pattern);

    process.stdout.write(`  ${store.id}  ${store.name.padEnd(22)}`);

    // ── Skip decision ──
    const { skip, reason, newState } = shouldSkip(store.id, storeCfg, streakState, targetDate);
    if (newState) streakState[store.id] = newState;

    if (skip) {
      console.log(`  ✗ SKIPPED — ${reason}`);
      writeLog({ event: 'store_skipped', run_id: runId, date: targetDate, store_id: store.id, filename, reason });
      summary.stores[store.id] = { status: 'skipped', reason };
      continue;
    }

    // ── Generate ──
    const cleanRows          = generateStoreRows(store, targetDate);
    const { rows, defects }  = applyChaos(cleanRows, store.id, targetDate);
    const defectCount        = defects.filter(d => d.defect !== 'duplicate_pk').length;
    const dupeCount          = defects.filter(d => d.defect === 'duplicate_pk').length;

    // ── Write CSV ──
    const csv     = toCsv(rows);
    const tmpPath = `/tmp/${filename}`;
    fs.writeFileSync(tmpPath, csv, 'utf8');
    const sizeKb  = (Buffer.byteLength(csv) / 1024).toFixed(1);

    if (OUTPUT_DIR) {
      fs.mkdirSync(OUTPUT_DIR, { recursive: true });
      fs.copyFileSync(tmpPath, path.join(OUTPUT_DIR, filename));
    }

    // ── Upload ──
    let uploaded = false;
    if (!SKIP_FTP) {
      try {
        await uploadFile(tmpPath, filename);
        uploaded = true;
      } catch (e) {
        console.log(`  ✗ FTP FAIL — ${e.message}`);
        writeLog({ event: 'ftp_upload_failed', run_id: runId, date: targetDate, store_id: store.id, filename, error: e.message });
        summary.stores[store.id] = { status: 'ftp_error', error: e.message };
        continue;
      }
    }

    const defectPct = ((defectCount / rows.length) * 100).toFixed(1);
    const badDayFlag = isBadDay(store.id, targetDate);
    const growthPct  = ((growthMultiplier(store.id, targetDate) - 1) * 100).toFixed(1);
    const badDayStr  = badDayFlag ? '  ⚠ BAD DAY' : '';
    console.log(`  ✓  ${rows.length} rows  ${sizeKb.padStart(6)} KB  defects: ${defectCount} (${defectPct}%)  dupes: ${dupeCount}  growth: +${growthPct}%${badDayStr}  → ${filename}`);

    writeLog({
      event: 'file_generated', run_id: runId, date: targetDate,
      store_id: store.id, filename, total_rows: rows.length,
      clean_rows: cleanRows.length, defect_count: defectCount,
      dupe_count: dupeCount, defect_pct: parseFloat(defectPct),
      size_kb: parseFloat(sizeKb), ftp_uploaded: uploaded,
      bad_day: badDayFlag,
      growth_pct: parseFloat(growthPct),
      defect_breakdown: defects.reduce((acc, d) => {
        acc[d.defect] = (acc[d.defect] || 0) + 1; return acc;
      }, {}),
    });

    summary.stores[store.id] = { status: 'ok', rows: rows.length, defects: defectCount, dupes: dupeCount, filename };
  }

  // ── Persist streak state ──
  saveStreakState(streakState);

  // ── Run summary ──
  const ok      = Object.values(summary.stores).filter(s => s.status === 'ok').length;
  const skipped = Object.values(summary.stores).filter(s => s.status === 'skipped').length;
  const failed  = Object.values(summary.stores).filter(s => s.status === 'ftp_error').length;

  console.log(`╠════════════════════════════════════════════════════════════════╣`);
  console.log(`  ${ok} files uploaded  |  ${skipped} skipped  |  ${failed} errors`);
  console.log(`╚════════════════════════════════════════════════════════════════╝\n`);

  writeLog({ event: 'run_complete', run_id: runId, date: targetDate, ok, skipped, failed, stores: summary.stores });
})().catch(e => {
  console.error('\nGenerator crashed:', e.message, e.stack);
  fs.appendFileSync(GEN_LOG,
    JSON.stringify({ ts: new Date().toISOString(), event: 'generator_crash', error: e.message }) + '\n');
  process.exit(1);
});
