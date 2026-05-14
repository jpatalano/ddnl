'use strict';

/**
 * seeds/crane/seed-equipment-costs.js
 *
 * Seeds the `equipment_costs` dataset for the crane instance.
 *
 * Schema (one row per UnitCode + CostDate + CostCategory + CostType):
 *   UnitCode        — joins to equipment dataset
 *   Yard            — denormalized from equipment
 *   CostDate        — YYYY-MM-DD (monthly grain)
 *   CostCategory    — Revenue | Labor | Material | OverHead
 *   CostType        — subcategory (e.g. "Bare", "Operated", "Mechanic Labor", "Lubricants", etc.)
 *   Amount          — positive = revenue/income, negative = cost/expense
 *   FiscalYear      — derived from CostDate
 *   FiscalMonth     — 1–12
 *
 * Usage:
 *   API_BASE=https://... API_KEY=ik_... CLIENT_ID=fcc node seeds/crane/seed-equipment-costs.js
 *
 * Defaults to FCC sandbox.
 */

const https  = require('https');
const http   = require('http');

const API_BASE   = process.env.API_BASE   || 'https://fcc-app-production.up.railway.app';
const API_KEY    = process.env.API_KEY    || 'ik_fcc_2a98cd7b986ff20ed9bff0fa1aed9644b2621e0b124a1646bbd9c41683a44944';
const CLIENT_ID  = process.env.CLIENT_ID  || 'fcc';
const BASIC_AUTH = 'Basic ' + Buffer.from('ddnl:ddnl!').toString('base64');
const DATASET    = 'equipment_costs';
const LABEL      = 'Equipment Costs';
const DATE_FROM  = process.env.DATE_FROM  || '2025-01-01';
const DATE_TO    = process.env.DATE_TO    || '2025-09-30';
const BATCH_SIZE = 500;

// ─── Revenue subcategories ────────────────────────────────────────────────────
const REVENUE_TYPES = ['Bare', 'Operated', 'N/A'];

// ─── Cost subcategories ───────────────────────────────────────────────────────
const LABOR_TYPES    = ['Transportation', 'Admin Time', 'Mechanic Labor', 'Training', 'No Classification'];
const MATERIAL_TYPES = ['Lubricants', 'Parts & Supplies', 'No Classification'];
const OVERHEAD_TYPES = ['Labor Burden', 'Material Burden', 'Workers Comp', 'Union'];

// ─── Simulated equipment fleet ────────────────────────────────────────────────
// Matches the UnitCodes seeded by seed.js / simulate-equipment.js
const YARDS = ['Seattle', 'Portland', 'Tacoma', 'Spokane'];

const UNIT_TYPES = ['Crane', 'Boom Truck', 'Carry Deck', 'Rough Terrain', 'All Terrain'];

// Generate a fleet of 50 units spread across yards
const FLEET = [];
for (let i = 1; i <= 50; i++) {
  FLEET.push({
    UnitCode: String(i).padStart(3, '0') + '-' + randomChoice(['CR', 'BT', 'CD', 'RT', 'AT']),
    Yard:     YARDS[i % YARDS.length],
    UnitType: UNIT_TYPES[i % UNIT_TYPES.length]
  });
}

// ─── Helpers ──────────────────────────────────────────────────────────────────
function randomChoice(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

function rand(min, max) {
  return Math.round((min + Math.random() * (max - min)) * 100) / 100;
}

function monthsBetween(from, to) {
  const months = [];
  const d = new Date(from + 'T00:00:00Z');
  const end = new Date(to + 'T00:00:00Z');
  while (d <= end) {
    months.push(d.toISOString().slice(0, 7) + '-01'); // YYYY-MM-01
    d.setUTCMonth(d.getUTCMonth() + 1);
  }
  return months;
}

function buildRows(unit, months) {
  const rows = [];
  const isActive = Math.random() > 0.15; // ~85% of units have some activity

  for (const month of months) {
    const fy   = parseInt(month.slice(0, 4));
    const fm   = parseInt(month.slice(5, 7));
    const pk   = `${unit.UnitCode}__${month}`;

    if (!isActive && Math.random() > 0.3) continue; // inactive units sparse

    // Revenue — pick 1–2 types
    const revenueTypes = Math.random() > 0.4
      ? [randomChoice(REVENUE_TYPES)]
      : REVENUE_TYPES.slice(0, 2);

    for (const type of revenueTypes) {
      const amount = type === 'N/A' ? 0 : rand(500, 25000);
      if (amount === 0) continue;
      rows.push({
        _pk:          `${pk}__Revenue__${type}`,
        UnitCode:     unit.UnitCode,
        Yard:         unit.Yard,
        UnitType:     unit.UnitType,
        CostDate:     month,
        FiscalYear:   fy,
        FiscalMonth:  fm,
        CostCategory: 'Revenue',
        CostType:     type,
        Amount:       amount
      });
    }

    // Labor costs — 1–3 types
    const laborCount = Math.floor(Math.random() * 3) + 1;
    const laborTypes = shuffle(LABOR_TYPES).slice(0, laborCount);
    for (const type of laborTypes) {
      rows.push({
        _pk:          `${pk}__Labor__${type}`,
        UnitCode:     unit.UnitCode,
        Yard:         unit.Yard,
        UnitType:     unit.UnitType,
        CostDate:     month,
        FiscalYear:   fy,
        FiscalMonth:  fm,
        CostCategory: 'Labor',
        CostType:     type,
        Amount:       -rand(200, 8000)
      });
    }

    // Material costs — 0–2 types
    if (Math.random() > 0.3) {
      const matCount = Math.floor(Math.random() * 2) + 1;
      const matTypes = shuffle(MATERIAL_TYPES).slice(0, matCount);
      for (const type of matTypes) {
        rows.push({
          _pk:          `${pk}__Material__${type}`,
          UnitCode:     unit.UnitCode,
          Yard:         unit.Yard,
          UnitType:     unit.UnitType,
          CostDate:     month,
          FiscalYear:   fy,
          FiscalMonth:  fm,
          CostCategory: 'Material',
          CostType:     type,
          Amount:       -rand(100, 15000)
        });
      }
    }

    // Overhead — always present for active units
    for (const type of OVERHEAD_TYPES) {
      if (Math.random() > 0.5) continue; // not every overhead type every month
      rows.push({
        _pk:          `${pk}__OverHead__${type}`,
        UnitCode:     unit.UnitCode,
        Yard:         unit.Yard,
        UnitType:     unit.UnitType,
        CostDate:     month,
        FiscalYear:   fy,
        FiscalMonth:  fm,
        CostCategory: 'OverHead',
        CostType:     type,
        Amount:       -rand(100, 3000)
      });
    }
  }
  return rows;
}

function shuffle(arr) {
  const a = [...arr];
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

// ─── HTTP helper ──────────────────────────────────────────────────────────────
function post(path, body, method = 'POST') {
  return new Promise((resolve, reject) => {
    const url  = new URL(API_BASE + path);
    const data = body ? JSON.stringify(body) : null;
    const mod  = url.protocol === 'https:' ? https : http;
    const headers = {
      'X-Api-Key':     API_KEY,
      'Authorization': BASIC_AUTH,
      'X-Instance-Id': CLIENT_ID
    };
    if (data) {
      headers['Content-Type']   = 'application/json';
      headers['Content-Length'] = Buffer.byteLength(data);
    }
    const req  = mod.request({
      hostname: url.hostname,
      port:     url.port || (url.protocol === 'https:' ? 443 : 80),
      path:     url.pathname + url.search,
      method,
      headers
    }, res => {
      let buf = '';
      res.on('data', c => buf += c);
      res.on('end', () => {
        try { resolve({ status: res.statusCode, body: JSON.parse(buf) }); }
        catch(e) { resolve({ status: res.statusCode, body: buf }); }
      });
    });
    req.on('error', reject);
    if (data) req.write(data);
    req.end();
  });
}

// Schema for the equipment_costs dataset
const SCHEMA_FIELDS = [
  { name: '_pk',          type: 'keyword', label: 'PK'            },
  { name: 'UnitCode',     type: 'keyword', label: 'Unit Code'     },
  { name: 'Yard',         type: 'keyword', label: 'Yard'          },
  { name: 'UnitType',     type: 'keyword', label: 'Unit Type'     },
  { name: 'CostDate',     type: 'date',    label: 'Cost Date'     },
  { name: 'FiscalYear',   type: 'integer', label: 'Fiscal Year'   },
  { name: 'FiscalMonth',  type: 'integer', label: 'Fiscal Month'  },
  { name: 'CostCategory', type: 'keyword', label: 'Cost Category' },
  { name: 'CostType',     type: 'keyword', label: 'Cost Type'     },
  { name: 'Amount',       type: 'float',   label: 'Amount'        }
];

// ─── Dataset registration (via admin API) ────────────────────────────────────
async function ensureDataset() {
  console.log(`[setup] Checking dataset "${DATASET}"...`);
  // Check if already exists
  const check = await post(`/api/ingest/admin/datasets/${DATASET}`, null, 'GET');
  if (check.status === 200 && check.body?.dataset) {
    console.log(`[setup] Dataset already exists.`);
    return;
  }
  console.log(`[setup] Creating dataset via admin API...`);
  const res = await post('/api/ingest/admin/datasets', {
    name:          DATASET,
    label:         LABEL,
    description:   'Per-unit cost breakdown by category and type — supports the Equipment Revenue P&L report.',
    dataset_type:  'client',
    version_field: 'CostDate',
    version_type:  'timestamp',
    fields:        SCHEMA_FIELDS
  });
  if (res.body?.success) {
    console.log(`[setup] Dataset created. ES alias: ${res.body.dataset?.esAlias}`);
  } else {
    throw new Error(`Dataset creation failed: ${JSON.stringify(res.body)}`);
  }
}

// ─── Ingest in batches ────────────────────────────────────────────────────────
async function ingestBatch(rows, batchNum) {
  const res = await post(`/api/ingest/${DATASET}/bulk`, { docs: rows, replace: false });
  if (res.body?.success || res.body?.indexed !== undefined) {
    console.log(`  [batch ${batchNum}] ✓ ${rows.length} rows ingested`);
  } else {
    console.error(`  [batch ${batchNum}] ✗ Error:`, res.status, JSON.stringify(res.body).slice(0, 200));
  }
}

// ─── Main ─────────────────────────────────────────────────────────────────────
(async () => {
  console.log(`\nEquipment Costs Seed`);
  console.log(`  API_BASE : ${API_BASE}`);
  console.log(`  CLIENT_ID: ${CLIENT_ID}`);
  console.log(`  Dataset  : ${DATASET}`);
  console.log(`  Range    : ${DATE_FROM} → ${DATE_TO}`);
  console.log(`  Units    : ${FLEET.length}`);

  await ensureDataset();

  const months = monthsBetween(DATE_FROM, DATE_TO);
  console.log(`\n[generate] Building rows for ${months.length} months × ${FLEET.length} units...`);

  const allRows = [];
  for (const unit of FLEET) {
    allRows.push(...buildRows(unit, months));
  }
  console.log(`[generate] ${allRows.length} total rows`);

  // Ingest in batches
  for (let i = 0; i < allRows.length; i += BATCH_SIZE) {
    const batch = allRows.slice(i, i + BATCH_SIZE);
    await ingestBatch(batch, Math.floor(i / BATCH_SIZE) + 1);
  }

  console.log(`\n✓ Done — equipment_costs seeded.\n`);
})().catch(err => {
  console.error('Fatal:', err.message);
  process.exit(1);
});
