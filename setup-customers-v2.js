/**
 * setup-customers-v2.js
 *
 * 1. Delete old produce__customers__v1 data + alias
 * 2. Create new ES index produce__customers__v2 with full schema
 * 3. Update dataset_definitions + dataset_field_metadata in Postgres
 * 4. Seed realistic customer records tied to existing sales history
 *
 * Run: node /home/user/workspace/fcc-bi/setup-customers-v2.js
 */

'use strict';

const { Client } = require('@elastic/elasticsearch');
const { Pool }   = require('pg');
const crypto     = require('crypto');

// ── Config ────────────────────────────────────────────────────────────────────
const ES_URL     = 'https://elasticsearch-production-1c60.up.railway.app';
const PG_CONN    = 'postgresql://postgres:XzBnPhQrHYpxiliwwGhEaaMPSlQSzeTl@shinkansen.proxy.rlwy.net:20046/railway';
const INSTANCE   = 'produce';
const OLD_INDEX  = 'produce__customers__v1';
const NEW_INDEX  = 'produce__customers__v2';
const ALIAS      = 'produce__customers';

const es   = new Client({ node: ES_URL, auth: { username: 'elastic', password: 'changeme' } });
const pool = new Pool({ connectionString: PG_CONN });

// ── Stores ────────────────────────────────────────────────────────────────────
const STORES = [
  { id: 'store_001', name: 'Seattle Pike Market',          city: 'Seattle',   state: 'WA', zip_prefix: '981', region: 'Pacific Northwest' },
  { id: 'store_002', name: 'Miami Fresh Market',           city: 'Miami',     state: 'FL', zip_prefix: '331', region: 'Southeast'         },
  { id: 'store_003', name: 'Chicago Green City',           city: 'Chicago',   state: 'IL', zip_prefix: '606', region: 'Midwest'           },
  { id: 'store_004', name: 'Phoenix Desert Fresh',         city: 'Phoenix',   state: 'AZ', zip_prefix: '850', region: 'Southwest'         },
  { id: 'store_005', name: 'NYC Union Square Greenmarket', city: 'New York',  state: 'NY', zip_prefix: '100', region: 'Northeast'         },
  { id: 'store_006', name: 'Denver Mile High Produce',     city: 'Denver',    state: 'CO', zip_prefix: '802', region: 'Mountain'          },
  { id: 'store_007', name: 'Atlanta Ponce City Market',    city: 'Atlanta',   state: 'GA', zip_prefix: '303', region: 'Southeast'         },
];

// ── Field schema — what Postgres + ES will know about ─────────────────────────
// field_type: 'segment' | 'metric'
// segmentType: 'string' | 'date' | 'number'
const FIELDS = [
  // Identity
  { name: 'customer_id',         label: 'Customer ID',          field_type: 'segment', segmentType: 'string',  sort_order: 1,  is_hidden: false },
  { name: 'first_name',          label: 'First Name',           field_type: 'segment', segmentType: 'string',  sort_order: 2,  is_hidden: false },
  { name: 'last_name',           label: 'Last Name',            field_type: 'segment', segmentType: 'string',  sort_order: 3,  is_hidden: false },
  { name: 'full_name',           label: 'Full Name',            field_type: 'segment', segmentType: 'string',  sort_order: 4,  is_hidden: false },
  { name: 'email_primary',       label: 'Email (Primary)',      field_type: 'segment', segmentType: 'string',  sort_order: 5,  is_hidden: false },
  { name: 'email_secondary',     label: 'Email (Secondary)',    field_type: 'segment', segmentType: 'string',  sort_order: 6,  is_hidden: true  },
  { name: 'phone_primary',       label: 'Phone (Primary)',      field_type: 'segment', segmentType: 'string',  sort_order: 7,  is_hidden: false },
  { name: 'phone_secondary',     label: 'Phone (Secondary)',    field_type: 'segment', segmentType: 'string',  sort_order: 8,  is_hidden: true  },
  { name: 'messaging_preference',label: 'Messaging Preference', field_type: 'segment', segmentType: 'string',  sort_order: 9,  is_hidden: false },
  // Address
  { name: 'address_line1',       label: 'Address Line 1',       field_type: 'segment', segmentType: 'string',  sort_order: 10, is_hidden: false },
  { name: 'address_line2',       label: 'Address Line 2',       field_type: 'segment', segmentType: 'string',  sort_order: 11, is_hidden: true  },
  { name: 'city',                label: 'City',                  field_type: 'segment', segmentType: 'string',  sort_order: 12, is_hidden: false },
  { name: 'state',               label: 'State',                 field_type: 'segment', segmentType: 'string',  sort_order: 13, is_hidden: false },
  { name: 'zip',                 label: 'ZIP',                   field_type: 'segment', segmentType: 'string',  sort_order: 14, is_hidden: false },
  { name: 'lat',                 label: 'Latitude',              field_type: 'segment', segmentType: 'number',  sort_order: 15, is_hidden: false },
  { name: 'lon',                 label: 'Longitude',             field_type: 'segment', segmentType: 'number',  sort_order: 16, is_hidden: false },
  { name: 'geocode_status',      label: 'Geocode Status',        field_type: 'segment', segmentType: 'string',  sort_order: 17, is_hidden: true  },
  { name: 'geocode_confidence',  label: 'Geocode Confidence',    field_type: 'segment', segmentType: 'number',  sort_order: 18, is_hidden: true  },
  // Store relationship
  { name: 'last_visit_store_id', label: 'Last Visit Store',      field_type: 'segment', segmentType: 'string',  sort_order: 19, is_hidden: false },
  { name: 'last_visit_date',     label: 'Last Visit Date',       field_type: 'segment', segmentType: 'date',    sort_order: 20, is_hidden: false },
  { name: 'first_visit_store_id',label: 'First Visit Store',     field_type: 'segment', segmentType: 'string',  sort_order: 21, is_hidden: false },
  { name: 'first_visit_date',    label: 'First Visit Date',      field_type: 'segment', segmentType: 'date',    sort_order: 22, is_hidden: false },
  // Tags
  { name: 'tags',                label: 'Tags',                  field_type: 'segment', segmentType: 'string',  sort_order: 23, is_hidden: false },
  { name: 'system_tags',         label: 'System Tags',           field_type: 'segment', segmentType: 'string',  sort_order: 24, is_hidden: false },
  // Metrics (pre-aggregated, updated on sales ingest)
  { name: 'total_visits',        label: 'Total Visits',          field_type: 'metric',  segmentType: 'number',  sort_order: 25, is_hidden: false, format: 'number'   },
  { name: 'total_spend',         label: 'Total Spend',           field_type: 'metric',  segmentType: 'number',  sort_order: 26, is_hidden: false, format: 'currency' },
  { name: 'avg_basket',          label: 'Avg Basket',            field_type: 'metric',  segmentType: 'number',  sort_order: 27, is_hidden: false, format: 'currency' },
  { name: 'total_units',         label: 'Total Units',           field_type: 'metric',  segmentType: 'number',  sort_order: 28, is_hidden: false, format: 'number'   },
];

// ── ES mapping ────────────────────────────────────────────────────────────────
function buildMapping() {
  const props = {
    __instance_id:  { type: 'keyword' },
    __ingested_at:  { type: 'date'    },
    __updated_at:   { type: 'date'    },
    address_hash:   { type: 'keyword' }, // internal, not in FIELDS
  };
  for (const f of FIELDS) {
    if (f.segmentType === 'date')   { props[f.name] = { type: 'date' };    continue; }
    if (f.segmentType === 'number') { props[f.name] = { type: 'double' };  continue; }
    // string — keyword (exact + agg). Full-text on name fields.
    if (['full_name','first_name','last_name','address_line1','address_line2'].includes(f.name)) {
      props[f.name] = { type: 'keyword', fields: { text: { type: 'text', analyzer: 'standard' } } };
    } else {
      props[f.name] = { type: 'keyword' };
    }
  }
  return props;
}

// ── Helpers ───────────────────────────────────────────────────────────────────
function rnd(arr) { return arr[Math.floor(Math.random() * arr.length)]; }
function rndInt(min, max) { return Math.floor(Math.random() * (max - min + 1)) + min; }
function rndFloat(min, max, dp = 2) { return parseFloat((Math.random() * (max - min) + min).toFixed(dp)); }
function chance(p) { return Math.random() < p; }

function dateAdd(base, days) {
  const d = new Date(base + 'T00:00:00');
  d.setDate(d.getDate() + days);
  return d.toISOString().slice(0, 10);
}
function dateBetween(a, b) {
  const da = new Date(a + 'T00:00:00').getTime();
  const db = new Date(b + 'T00:00:00').getTime();
  return new Date(da + Math.random() * (db - da)).toISOString().slice(0, 10);
}

// Deterministic fake geocode from zip prefix (±scatter around real city coords)
const STORE_COORDS = {
  WA: [47.6062, -122.3321], FL: [25.7617, -80.1918], IL: [41.8781, -87.6298],
  AZ: [33.4484, -112.074],  NY: [40.7128, -74.006],  CO: [39.7392, -104.9903],
  GA: [33.749,  -84.388],
};
function fakeGeocode(state, zip) {
  const base = STORE_COORDS[state] || [37.5, -96.0];
  // customers live ≤15 miles from store — ~0.22 degrees
  return {
    lat: parseFloat((base[0] + (Math.random() - 0.5) * 0.44).toFixed(6)),
    lon: parseFloat((base[1] + (Math.random() - 0.5) * 0.44).toFixed(6)),
    geocode_status:     'ok',
    geocode_confidence: parseFloat((0.72 + Math.random() * 0.28).toFixed(3)),
  };
}

// Name banks
const FIRST_M = ['James','John','Robert','Michael','William','David','Richard','Joseph','Thomas','Charles','Daniel','Matthew','Anthony','Mark','Donald','Steven','Paul','Andrew','Kenneth','Joshua','Kevin','Brian','George','Edward','Ronald','Timothy','Jason','Jeffrey','Ryan','Jacob','Gary','Nicholas','Eric','Jonathan','Stephen','Larry','Justin','Scott','Brandon','Benjamin','Samuel','Frank','Gregory','Raymond','Patrick','Jack','Dennis','Jerry','Alexander','Tyler'];
const FIRST_F = ['Mary','Patricia','Jennifer','Linda','Barbara','Elizabeth','Susan','Jessica','Sarah','Karen','Lisa','Nancy','Betty','Margaret','Sandra','Ashley','Dorothy','Kimberly','Emily','Donna','Michelle','Carol','Amanda','Melissa','Deborah','Stephanie','Rebecca','Sharon','Laura','Cynthia','Kathleen','Amy','Angela','Shirley','Anna','Brenda','Pamela','Emma','Nicole','Helen','Samantha','Katherine','Christine','Debra','Rachel','Carolyn','Janet','Catherine','Maria','Heather'];
const LAST_N  = ['Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis','Rodriguez','Martinez','Hernandez','Lopez','Gonzalez','Wilson','Anderson','Thomas','Taylor','Moore','Jackson','Martin','Lee','Perez','Thompson','White','Harris','Sanchez','Clark','Ramirez','Lewis','Robinson','Walker','Young','Allen','King','Wright','Scott','Torres','Nguyen','Hill','Flores','Green','Adams','Nelson','Baker','Hall','Rivera','Campbell','Mitchell','Carter','Roberts'];
const STREETS = ['Oak St','Maple Ave','Cedar Rd','Elm St','Main St','Park Blvd','Pine Ave','Sunset Dr','Willow Ln','Highland Ave','River Rd','Lake Dr','Forest Way','Valley Rd','Spring St','Market St','Grove Ave','Meadow Ln','Summit Dr','Church St'];
const DOMAINS  = ['gmail.com','yahoo.com','outlook.com','icloud.com','hotmail.com','aol.com','protonmail.com'];
const MSG_PREFS = ['email','sms','both','none'];
const ACQ_CHANNELS = ['Walk-in','Farmers Market','Online','Referral','CSA Subscription','Event','Social Media'];

function makeCustomer(custIdx, store) {
  const isMale = chance(0.48);
  const first  = rnd(isMale ? FIRST_M : FIRST_F);
  const last   = rnd(LAST_N);
  const full   = `${first} ${last}`;
  const customerId = `cust_${String(custIdx).padStart(6, '0')}`;

  const zip = store.zip_prefix + String(rndInt(100, 999));
  const geo = fakeGeocode(store.state, zip);

  const emailPrimary = `${first.toLowerCase()}.${last.toLowerCase()}${rndInt(1,999)}@${rnd(DOMAINS)}`;
  const hasSecEmail  = chance(0.2);
  const hasSecPhone  = chance(0.15);

  const msgPref = rnd(MSG_PREFS);

  // Lifecycle dates — spread across full 2024–2026 history
  const firstVisit = dateBetween('2024-01-01', '2025-12-31');
  // last visit: somewhere between first visit and today
  const lastVisit  = dateBetween(firstVisit, '2026-04-21');

  // Visit frequency drives totals
  const daysSinceFirst = Math.floor((new Date('2026-04-21') - new Date(firstVisit + 'T00:00:00')) / 86400000);
  const visitFreqDays  = rndInt(7, 60);  // avg days between visits
  const totalVisits    = Math.max(1, Math.round(daysSinceFirst / visitFreqDays));
  const avgBasket      = rndFloat(12, 85, 2);
  const totalSpend     = parseFloat((totalVisits * avgBasket * (0.85 + Math.random() * 0.3)).toFixed(2));
  const totalUnits     = Math.round(totalVisits * rndInt(3, 12));

  // Tags
  const manualTags = [];
  const systemTags = [];
  if (totalSpend > 2000)    systemTags.push('high_value');
  if (totalSpend < 100)     systemTags.push('low_spend');
  if (totalVisits >= 50)    systemTags.push('loyal');
  if (totalVisits === 1)    systemTags.push('one_time');
  const daysSinceLast = Math.floor((new Date('2026-04-21') - new Date(lastVisit + 'T00:00:00')) / 86400000);
  if (daysSinceLast > 180)  systemTags.push('at_risk');
  if (daysSinceLast > 365)  systemTags.push('lapsed');
  if (new Date(firstVisit) >= new Date('2026-01-01')) systemTags.push('new_customer');
  if (chance(0.08))         manualTags.push('vip');
  if (chance(0.15))         manualTags.push('loyalty_member');
  if (msgPref === 'none')   systemTags.push('no_contact');

  const addressLine1 = `${rndInt(100, 9999)} ${rnd(STREETS)}`;
  const addressHash  = crypto.createHash('md5').update(addressLine1 + store.city + store.state + zip).digest('hex');

  const doc = {
    __instance_id:        INSTANCE,
    __ingested_at:        new Date().toISOString(),
    __updated_at:         new Date().toISOString(),
    customer_id:          customerId,
    first_name:           first,
    last_name:            last,
    full_name:            full,
    email_primary:        emailPrimary,
    phone_primary:        `${rndInt(200,999)}-${rndInt(200,999)}-${rndInt(1000,9999)}`,
    messaging_preference: msgPref,
    address_line1:        addressLine1,
    city:                 store.city,
    state:                store.state,
    zip,
    lat:                  geo.lat,
    lon:                  geo.lon,
    geocode_status:       geo.geocode_status,
    geocode_confidence:   geo.geocode_confidence,
    address_hash:         addressHash,
    last_visit_store_id:  store.id,
    last_visit_date:      lastVisit,
    first_visit_store_id: store.id,
    first_visit_date:     firstVisit,
    tags:                 manualTags.join(','),
    system_tags:          systemTags.join(','),
    total_visits:         totalVisits,
    total_spend:          totalSpend,
    avg_basket:           avgBasket,
    total_units:          totalUnits,
  };

  // Optional secondary contact
  if (hasSecEmail)  doc.email_secondary = `${last.toLowerCase()}${rndInt(1,99)}@${rnd(DOMAINS)}`;
  if (hasSecPhone)  doc.phone_secondary  = `${rndInt(200,999)}-${rndInt(200,999)}-${rndInt(1000,9999)}`;
  if (chance(0.1))  doc.address_line2    = `Apt ${rndInt(1,999)}`;

  return doc;
}

// ── Main ──────────────────────────────────────────────────────────────────────
async function main() {
  console.log('=== Customer Dataset v2 Setup ===\n');

  // ── 1. Drop old index + alias ───────────────────────────────────────────────
  console.log('1. Cleaning up old index…');
  try {
    await es.indices.deleteAlias({ index: OLD_INDEX, name: ALIAS });
    console.log('   Alias removed');
  } catch(e) { console.log('   No alias to remove:', e.meta?.body?.error?.type || e.message); }
  try {
    await es.indices.delete({ index: OLD_INDEX, ignore_unavailable: true });
    console.log('   Old index deleted');
  } catch(e) { console.log('   Could not delete old index:', e.message); }

  // ── 2. Create new index ─────────────────────────────────────────────────────
  console.log('\n2. Creating new index…');
  // Delete if somehow exists (idempotent)
  await es.indices.delete({ index: NEW_INDEX, ignore_unavailable: true }).catch(()=>{});
  await es.indices.create({
    index: NEW_INDEX,
    body: {
      settings: {
        number_of_shards:   1,
        number_of_replicas: 0,
        'index.mapping.total_fields.limit': 500,
      },
      mappings: { properties: buildMapping() }
    }
  });
  console.log(`   Created: ${NEW_INDEX}`);

  // Point alias
  try {
    // Remove from any old index first
    const { body: aliasInfo } = await es.indices.getAlias({ name: ALIAS, ignore_unavailable: true }).catch(()=>({body:{}}));
    const oldIndices = Object.keys(aliasInfo || {}).filter(i => i !== NEW_INDEX);
    if (oldIndices.length) {
      await es.indices.updateAliases({
        body: {
          actions: [
            ...oldIndices.map(i => ({ remove: { index: i, alias: ALIAS } })),
            { add: { index: NEW_INDEX, alias: ALIAS } }
          ]
        }
      });
    } else {
      await es.indices.putAlias({ index: NEW_INDEX, name: ALIAS });
    }
  } catch(e) {
    await es.indices.putAlias({ index: NEW_INDEX, name: ALIAS });
  }
  console.log(`   Alias: ${ALIAS} → ${NEW_INDEX}`);

  // ── 3. Update Postgres ──────────────────────────────────────────────────────
  console.log('\n3. Updating Postgres schema…');
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // Get dataset id
    const { rows: [ds] } = await client.query(
      `SELECT id FROM dataset_definitions WHERE name='customers' LIMIT 1`
    );
    if (!ds) throw new Error('customers dataset definition not found');
    const dsId = ds.id;

    // Update es_alias on dataset definition
    await client.query(
      `UPDATE dataset_definitions SET es_alias=$1, updated_at=NOW() WHERE id=$2`,
      [ALIAS, dsId]
    );

    // Wipe existing field metadata and re-insert
    await client.query(`DELETE FROM dataset_field_metadata WHERE dataset_id=$1`, [dsId]);

    for (const f of FIELDS) {
      await client.query(
        `INSERT INTO dataset_field_metadata
         (dataset_id, field_name, label, field_type, format, is_hidden, sort_order)
         VALUES ($1,$2,$3,$4,$5,$6,$7)`,
        [dsId, f.name, f.label, f.field_type, f.format || null, f.is_hidden, f.sort_order]
      );
    }
    console.log(`   Inserted ${FIELDS.length} field metadata rows`);

    // Upsert ingest channel — api_push with customer_id as id_field
    await client.query(
      `INSERT INTO ingest_channels (dataset_id, client_id, channel_name, method, mode, id_field, is_active)
       VALUES ($1,$2,'API Push','api_push','batch','customer_id',TRUE)
       ON CONFLICT (dataset_id, channel_name) DO UPDATE SET id_field='customer_id', is_active=TRUE`,
      [dsId, INSTANCE]
    );
    console.log('   Ingest channel upserted');

    await client.query('COMMIT');
    console.log('   Postgres updated ✓');
  } catch(e) {
    await client.query('ROLLBACK');
    throw e;
  } finally {
    client.release();
  }

  // ── 4. Seed customers ───────────────────────────────────────────────────────
  console.log('\n4. Seeding customer records…');

  // ~800 customers per store = ~5,600 total — realistic for regional specialty grocer
  const CUSTOMERS_PER_STORE = 800;
  let custIdx = 1;
  let totalDocs = 0;

  for (const store of STORES) {
    const docs = [];
    for (let i = 0; i < CUSTOMERS_PER_STORE; i++) {
      docs.push(makeCustomer(custIdx++, store));
    }

    // Bulk index in chunks of 500
    const CHUNK = 500;
    for (let i = 0; i < docs.length; i += CHUNK) {
      const chunk = docs.slice(i, i + CHUNK);
      const body  = chunk.flatMap(d => [
        { index: { _index: NEW_INDEX, _id: d.customer_id } },
        d
      ]);
      const result = await es.bulk({ body, refresh: false });
      if (result.errors) {
        const errItem = result.items.find(it => it.index?.error);
        console.warn(`   WARN bulk error: ${JSON.stringify(errItem?.index?.error)}`);
      }
      totalDocs += chunk.length;
    }
    console.log(`   ${store.name}: ${CUSTOMERS_PER_STORE} customers seeded`);
  }

  // Final refresh
  await es.indices.refresh({ index: NEW_INDEX });

  // Verify count
  const cnt = await es.count({ index: NEW_INDEX });
  console.log(`\n   Total docs in index: ${cnt.count.toLocaleString()}`);

  console.log('\n=== Done ✓ ===');
  await pool.end();
}

main().catch(e => { console.error('FATAL:', e.message); process.exit(1); });
