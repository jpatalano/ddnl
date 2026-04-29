/**
 * seed-produce-stand.js
 *
 * Bootstraps a demo 'produce' instance with:
 *   - Two datasets: 'sales' and 'customers'
 *   - An API key for the instance
 *   - ~30 rows each of realistic produce stand data
 *
 * Run: node seed-produce-stand.js
 * Requires: DATABASE_URL + ELASTICSEARCH_URL in env (or defaults)
 */

require('dotenv').config();
const { pool, initDb } = require('./db');
const es = require('./esClient');
const crypto = require('crypto');

const INSTANCE_ID = 'produce'; // client_id for the demo instance

// ── Schema definitions ─────────────────────────────────────────────────────────

const SALES_FIELDS = [
  { name: 'sale_id',      fieldType: 'segment', segmentType: 'string',  isFilterable: false, isGroupable: false },
  { name: 'sale_date',    fieldType: 'segment', segmentType: 'date',    isFilterable: true,  isGroupable: false },
  { name: 'sale_month',   fieldType: 'segment', segmentType: 'string',  isFilterable: true,  isGroupable: true  },
  { name: 'sale_year',    fieldType: 'segment', segmentType: 'number',  isFilterable: true,  isGroupable: true  },
  { name: 'product',      fieldType: 'segment', segmentType: 'string',  isFilterable: true,  isGroupable: true  },
  { name: 'category',     fieldType: 'segment', segmentType: 'string',  isFilterable: true,  isGroupable: true  },
  { name: 'vendor',       fieldType: 'segment', segmentType: 'string',  isFilterable: true,  isGroupable: true  },
  { name: 'customer_id',  fieldType: 'segment', segmentType: 'string',  isFilterable: true,  isGroupable: false },
  { name: 'stand',        fieldType: 'segment', segmentType: 'string',  isFilterable: true,  isGroupable: true  },
  { name: 'payment_type', fieldType: 'segment', segmentType: 'string',  isFilterable: true,  isGroupable: true  },
  { name: 'revenue',      fieldType: 'metric',  segmentType: 'number',  displayFormat: 'currency', aggregationType: 'SUM',   prefix: '$' },
  { name: 'quantity',     fieldType: 'metric',  segmentType: 'number',  displayFormat: 'number',   aggregationType: 'SUM'   },
  { name: 'unit_price',   fieldType: 'metric',  segmentType: 'number',  displayFormat: 'currency', aggregationType: 'AVG',   prefix: '$' },
  { name: 'sale_count',   fieldType: 'metric',  segmentType: 'number',  displayFormat: 'number',   aggregationType: 'COUNT' },
];

const CUSTOMERS_FIELDS = [
  { name: 'customer_id',     fieldType: 'segment', segmentType: 'string',  isFilterable: false, isGroupable: false },
  { name: 'first_name',      fieldType: 'segment', segmentType: 'string',  isFilterable: true,  isGroupable: false },
  { name: 'last_name',       fieldType: 'segment', segmentType: 'string',  isFilterable: true,  isGroupable: false },
  { name: 'zip',             fieldType: 'segment', segmentType: 'string',  isFilterable: true,  isGroupable: true  },
  { name: 'city',            fieldType: 'segment', segmentType: 'string',  isFilterable: true,  isGroupable: true  },
  { name: 'signup_date',     fieldType: 'segment', segmentType: 'date',    isFilterable: true,  isGroupable: false },
  { name: 'signup_year',     fieldType: 'segment', segmentType: 'number',  isFilterable: true,  isGroupable: true  },
  { name: 'preferred_stand', fieldType: 'segment', segmentType: 'string',  isFilterable: true,  isGroupable: true  },
  { name: 'rewards_member',  fieldType: 'segment', segmentType: 'string',  isFilterable: true,  isGroupable: true  },
  { name: 'lifetime_spend',  fieldType: 'metric',  segmentType: 'number',  displayFormat: 'currency', aggregationType: 'SUM',   prefix: '$' },
  { name: 'visit_count',     fieldType: 'metric',  segmentType: 'number',  displayFormat: 'number',   aggregationType: 'SUM'   },
  { name: 'avg_basket',      fieldType: 'metric',  segmentType: 'number',  displayFormat: 'currency', aggregationType: 'AVG',   prefix: '$' },
  { name: 'customer_count',  fieldType: 'metric',  segmentType: 'number',  displayFormat: 'number',   aggregationType: 'COUNT' },
];

// ── Seed data ──────────────────────────────────────────────────────────────────

const PRODUCTS = [
  { product: 'Apples',      category: 'Fruit',     vendor: 'Green Valley Farm',  unitPrice: 1.50 },
  { product: 'Oranges',     category: 'Fruit',     vendor: 'Citrus Bros',         unitPrice: 1.25 },
  { product: 'Bananas',     category: 'Fruit',     vendor: 'Tropico Supply',      unitPrice: 0.30 },
  { product: 'Strawberries',category: 'Fruit',     vendor: 'Berry Patch Co',      unitPrice: 4.00 },
  { product: 'Blueberries', category: 'Fruit',     vendor: 'Berry Patch Co',      unitPrice: 5.50 },
  { product: 'Carrots',     category: 'Vegetable', vendor: 'Root & Branch Farm',  unitPrice: 0.80 },
  { product: 'Kale',        category: 'Vegetable', vendor: 'Green Valley Farm',   unitPrice: 2.50 },
  { product: 'Tomatoes',    category: 'Vegetable', vendor: 'Sunripe Growers',     unitPrice: 2.00 },
  { product: 'Zucchini',    category: 'Vegetable', vendor: 'Root & Branch Farm',  unitPrice: 1.20 },
  { product: 'Corn',        category: 'Vegetable', vendor: 'Heartland Grains',    unitPrice: 0.75 },
];

const STANDS     = ['Main Street Stand', 'Farmers Market Booth', 'Online Store'];
const PAYMENTS   = ['Cash', 'Card', 'Tap'];
const CUSTOMERS  = [
  { customer_id: 'cust_001', first_name: 'Alice',  last_name: 'Monroe',   zip: '94105', city: 'San Francisco', signup_date: '2023-03-15', signup_year: 2023, preferred_stand: 'Main Street Stand',    rewards_member: 'Yes', lifetime_spend: 0, visit_count: 0 },
  { customer_id: 'cust_002', first_name: 'Bob',    last_name: 'Tanaka',   zip: '94107', city: 'San Francisco', signup_date: '2023-06-22', signup_year: 2023, preferred_stand: 'Farmers Market Booth', rewards_member: 'Yes', lifetime_spend: 0, visit_count: 0 },
  { customer_id: 'cust_003', first_name: 'Carmen', last_name: 'Ruiz',     zip: '94110', city: 'San Francisco', signup_date: '2024-01-08', signup_year: 2024, preferred_stand: 'Main Street Stand',    rewards_member: 'No',  lifetime_spend: 0, visit_count: 0 },
  { customer_id: 'cust_004', first_name: 'David',  last_name: 'Chen',     zip: '94109', city: 'San Francisco', signup_date: '2024-04-19', signup_year: 2024, preferred_stand: 'Online Store',         rewards_member: 'Yes', lifetime_spend: 0, visit_count: 0 },
  { customer_id: 'cust_005', first_name: 'Eva',    last_name: 'Okafor',   zip: '94102', city: 'San Francisco', signup_date: '2024-07-30', signup_year: 2024, preferred_stand: 'Farmers Market Booth', rewards_member: 'No',  lifetime_spend: 0, visit_count: 0 },
  { customer_id: 'cust_006', first_name: 'Frank',  last_name: 'Nguyen',   zip: '94103', city: 'San Francisco', signup_date: '2025-02-14', signup_year: 2025, preferred_stand: 'Main Street Stand',    rewards_member: 'Yes', lifetime_spend: 0, visit_count: 0 },
  { customer_id: 'cust_007', first_name: 'Grace',  last_name: 'Williams', zip: '94114', city: 'San Francisco', signup_date: '2025-05-01', signup_year: 2025, preferred_stand: 'Online Store',         rewards_member: 'No',  lifetime_spend: 0, visit_count: 0 },
];

// Deterministic sale rows — one per (product × customer), spread across dates
function buildSaleRows() {
  const rows = [];
  const dates = [
    '2025-10-03','2025-10-17','2025-11-05','2025-11-22',
    '2025-12-06','2025-12-20','2026-01-08','2026-01-24',
    '2026-02-12','2026-02-28','2026-03-07','2026-03-21',
    '2026-04-04','2026-04-11'
  ];

  let seq = 1;
  for (const date of dates) {
    const month = date.slice(0, 7); // YYYY-MM
    const year  = parseInt(date.slice(0, 4), 10);
    // Pick 3–5 products per day per stand
    for (const stand of STANDS) {
      const dayProducts = PRODUCTS.filter((_, i) => (i + seq) % 3 !== 0).slice(0, 5);
      for (const prod of dayProducts) {
        const cust    = CUSTOMERS[(seq - 1) % CUSTOMERS.length];
        const qty     = Math.ceil(seq % 8) + 1;
        const revenue = parseFloat((prod.unitPrice * qty).toFixed(2));
        rows.push({
          sale_id:      `sale_${String(seq).padStart(4,'0')}`,
          sale_date:    date,
          sale_month:   month,
          sale_year:    year,
          product:      prod.product,
          category:     prod.category,
          vendor:       prod.vendor,
          customer_id:  cust.customer_id,
          stand,
          payment_type: PAYMENTS[seq % PAYMENTS.length],
          revenue,
          quantity:     qty,
          unit_price:   prod.unitPrice,
          sale_count:   1,
        });
        seq++;
      }
    }
  }
  return rows;
}

function buildCustomerRows(saleRows) {
  return CUSTOMERS.map(c => {
    const custSales   = saleRows.filter(s => s.customer_id === c.customer_id);
    const lifetimeSpend = parseFloat(custSales.reduce((s, r) => s + r.revenue, 0).toFixed(2));
    const visitCount    = custSales.length;
    const avgBasket     = visitCount ? parseFloat((lifetimeSpend / visitCount).toFixed(2)) : 0;
    return {
      ...c,
      lifetime_spend: lifetimeSpend,
      visit_count:    visitCount,
      avg_basket:     avgBasket,
      customer_count: 1,
    };
  });
}

// ── Bootstrap ──────────────────────────────────────────────────────────────────

async function bootstrap() {
  console.log('⏳ Initializing DB schema...');
  await initDb();

  // Seed 'produce' client
  await pool.query(
    `INSERT INTO clients (client_id, name) VALUES ($1, $2) ON CONFLICT (client_id) DO NOTHING`,
    [INSTANCE_ID, 'Produce Stand']
  );
  console.log('✅ Client seeded:', INSTANCE_ID);

  // Generate API key
  const { generateKey, hashKey } = require('./ingestRouter');
  const rawKey = generateKey();
  const hash   = hashKey(rawKey);
  await pool.query(
    `INSERT INTO instance_api_keys (client_id, key_hash, label)
     VALUES ($1, $2, 'Seed Script Key')
     ON CONFLICT (key_hash) DO NOTHING`,
    [INSTANCE_ID, hash]
  );
  console.log('🔑 API key generated:', rawKey);
  console.log('   (Save this — it will not be shown again)');

  // Wait for ES to be ready
  console.log('\n⏳ Waiting for Elasticsearch...');
  for (let i = 0; i < 10; i++) {
    try {
      await es.ping();
      console.log('✅ Elasticsearch is up');
      break;
    } catch (e) {
      if (i === 9) { console.error('❌ ES unreachable after 10 tries:', e.message); process.exit(1); }
      console.log(`   Retry ${i + 1}/10...`);
      await new Promise(r => setTimeout(r, 3000));
    }
  }

  // ── Create 'sales' dataset ──
  console.log('\n📦 Creating sales dataset...');
  await createDataset('sales', 'Sales Transactions', 'Produce stand sales by product, customer, and stand', SALES_FIELDS);

  // ── Create 'customers' dataset ──
  console.log('\n📦 Creating customers dataset...');
  await createDataset('customers', 'Customer Profiles', 'Customer lifetime metrics and demographics', CUSTOMERS_FIELDS);

  // ── Ingest data ──
  console.log('\n📥 Building seed data...');
  const saleRows     = buildSaleRows();
  const customerRows = buildCustomerRows(saleRows);
  console.log(`   ${saleRows.length} sale rows, ${customerRows.length} customer rows`);

  console.log('\n📥 Indexing sales...');
  const saleResult = await es.replaceAll(INSTANCE_ID, 'sales', saleRows);
  console.log(`   ✅ indexed: ${saleResult.indexed}, failed: ${saleResult.failed}`);

  console.log('\n📥 Indexing customers...');
  const custResult = await es.replaceAll(INSTANCE_ID, 'customers', customerRows);
  console.log(`   ✅ indexed: ${custResult.indexed}, failed: ${custResult.failed}`);

  // ── Log ingests ──
  await pool.query(
    `INSERT INTO ingest_log (client_id, dataset_name, operation, doc_count, triggered_by)
     VALUES ($1,'sales','replace',$2,'seed-script'),
            ($1,'customers','replace',$3,'seed-script')`,
    [INSTANCE_ID, saleRows.length, customerRows.length]
  );

  console.log('\n🎉 Done! Produce stand demo is ready.');
  console.log(`   Instance: ${INSTANCE_ID}`);
  console.log(`   Datasets: sales (${saleRows.length} rows), customers (${customerRows.length} rows)`);
  console.log(`   API Key:  ${rawKey}`);
  console.log('\n   Try querying: POST /api/bi/query with X-Instance-Id: produce');
}

async function createDataset(name, label, description, fields) {
  const db = await pool.connect();
  try {
    await db.query('BEGIN');

    // Check if already exists (idempotent)
    const { rows: [existing] } = await db.query(
      `SELECT id FROM dataset_definitions WHERE client_id=$1 AND name=$2`,
      [INSTANCE_ID, name]
    );

    if (existing) {
      console.log(`   ⚠️  Dataset '${name}' already exists (id=${existing.id}), skipping definition.`);
      await db.query('ROLLBACK');
      return;
    }

    const { rows: [def] } = await db.query(
      `INSERT INTO dataset_definitions (client_id, name, label, description, current_version)
       VALUES ($1,$2,$3,$4,1) RETURNING *`,
      [INSTANCE_ID, name, label, description]
    );

    await db.query(
      `INSERT INTO dataset_schema_versions (dataset_id, version, fields, compat_status, published_at, published_by)
       VALUES ($1,1,$2,'ok',NOW(),'seed-script')`,
      [def.id, JSON.stringify(fields)]
    );

    const esIdx = await es.createIndex(INSTANCE_ID, name, 1, fields);
    await es.swapAlias(INSTANCE_ID, name, 1);

    const alias = es.aliasName(INSTANCE_ID, name);
    await db.query(
      `UPDATE dataset_definitions SET es_alias=$1, updated_at=NOW() WHERE id=$2`,
      [alias, def.id]
    );
    await db.query(
      `UPDATE dataset_schema_versions SET es_index=$1 WHERE dataset_id=$2 AND version=1`,
      [esIdx, def.id]
    );

    await db.query('COMMIT');
    console.log(`   ✅ '${name}' created — ES index: ${esIdx}, alias: ${alias}`);
  } catch (e) {
    await db.query('ROLLBACK');
    throw e;
  } finally {
    db.release();
  }
}

bootstrap().then(() => process.exit(0)).catch(e => {
  console.error('❌ Seed failed:', e.message);
  process.exit(1);
});
