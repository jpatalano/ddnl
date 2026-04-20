const express = require('express');
const path = require('path');
const https = require('https');
const http  = require('http');
const { URL } = require('url');
const { pool, initDb } = require('./db');
const esClient = require('./esClient');
const { router: ingestRouter } = require('./ingestRouter');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

// ─── Basic Auth ──────────────────────────────────────────────────────────────
// Credentials via env vars — fallback to defaults for local dev
const BASIC_USER = process.env.BASIC_AUTH_USER || 'ddnl';
const BASIC_PASS = process.env.BASIC_AUTH_PASS || 'ddnl!';

// Mount ingest router BEFORE basic auth — it has its own X-Api-Key auth
app.use('/api/ingest', ingestRouter);

app.use((req, res, next) => {
  // Allow Railway health checks through without auth
  if (req.path === '/healthz' || req.headers['user-agent']?.includes('Railway')) return next();

  const auth = req.headers['authorization'];
  if (auth && auth.startsWith('Basic ')) {
    const [user, pass] = Buffer.from(auth.slice(6), 'base64').toString().split(':');
    if (user === BASIC_USER && pass === BASIC_PASS) return next();
  }
  res.set('WWW-Authenticate', 'Basic realm="DDNL Analytics"');
  res.status(401).send('Unauthorized');
});

// ─── Static files ────────────────────────────────────────────────────────────
app.use(express.static(path.join(__dirname), {
  etag: false,
  setHeaders: (res, filePath) => {
    if (filePath.endsWith('.html')) {
      res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate');
      res.setHeader('Pragma', 'no-cache');
    }
  }
}));

// ─── Instance Config ─────────────────────────────────────────────────────────
// This server runs as a SINGLE instance — no multi-instance array.
// All configuration comes from INSTANCE_CONFIG env var (JSON) at startup.
// Each Railway service gets its own INSTANCE_CONFIG. Zero cross-instance knowledge.
//
// Required fields:  id, name, adapter ('fcc' | 'insight' | 'internal')
// adapter=fcc:      apiBase
// adapter=insight:  apiBase, apiToken
// adapter=internal: (uses ELASTICSEARCH_URL env var, clientId defaults to id)
// Optional:         shortName, theme{}, defaultThemeMode, datasetFilter

let INSTANCE;
try {
  const raw = process.env.INSTANCE_CONFIG;
  if (!raw) throw new Error('INSTANCE_CONFIG env var is not set');
  INSTANCE = JSON.parse(raw);
  if (!INSTANCE.id)      throw new Error('INSTANCE_CONFIG missing required field: id');
  if (!INSTANCE.name)    throw new Error('INSTANCE_CONFIG missing required field: name');
  if (!INSTANCE.adapter) throw new Error('INSTANCE_CONFIG missing required field: adapter');
  // clientId defaults to id for internal adapter
  if (!INSTANCE.clientId) INSTANCE.clientId = INSTANCE.id;
  // apiToken can also come from a dedicated env var (keeps secrets out of JSON blob)
  if (!INSTANCE.apiToken && process.env.INSTANCE_API_TOKEN) {
    INSTANCE.apiToken = process.env.INSTANCE_API_TOKEN;
  }
  console.log(`Instance: ${INSTANCE.id} (${INSTANCE.name}) adapter=${INSTANCE.adapter}`);
} catch (e) {
  console.error('FATAL: Invalid INSTANCE_CONFIG —', e.message);
  process.exit(1);
}

// resolveInstance — always returns the single loaded instance.
// The req parameter is kept for API compatibility but is unused.
function resolveInstance(_req) { return INSTANCE; }

// GET /api/instance — tells the frontend who it is. Always returns a single instance.
app.get('/api/instance', (_req, res) => res.json({ instance: INSTANCE }));

// ─── Helper ──────────────────────────────────────────────────────────────────
function getClientId(_req) { return INSTANCE.clientId; }

function getUserOid(req) {
  return req.headers['x-user-oid'] || 'anonymous';
}

// ─── Reports ─────────────────────────────────────────────────────────────────

// GET /api/reports — list all saved reports for this client (full config included)
app.get('/api/reports', async (req, res) => {
  const clientId = getClientId(req);
  try {
    const { rows } = await pool.query(
      `SELECT id, name, description, created_by, created_at, updated_at, config
       FROM saved_reports
       WHERE client_id = $1
       ORDER BY updated_at DESC`,
      [clientId]
    );
    res.json({ reports: rows });
  } catch (err) {
    console.error('GET /api/reports', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/reports/:id — get a single saved report with full config
app.get('/api/reports/:id', async (req, res) => {
  const clientId = getClientId(req);
  try {
    const { rows } = await pool.query(
      `SELECT * FROM saved_reports WHERE id = $1 AND client_id = $2`,
      [req.params.id, clientId]
    );
    if (!rows.length) return res.status(404).json({ error: 'Not found' });
    res.json({ report: rows[0] });
  } catch (err) {
    console.error('GET /api/reports/:id', err);
    res.status(500).json({ error: err.message });
  }
});

// ── Versioning helper ─────────────────────────────────────────────
// Snapshot the current state of a report into report_versions.
// Returns the new version_number.
async function _snapshotVersion(client, reportId, savedBy) {
  const { rows: existing } = await client.query(
    `SELECT id, name, description, config FROM saved_reports WHERE id=$1`, [reportId]
  );
  if (!existing.length) return 0;
  const r = existing[0];
  const { rows: vrows } = await client.query(
    `SELECT COALESCE(MAX(version_number),0)+1 AS next FROM report_versions WHERE report_id=$1`,
    [reportId]
  );
  const nextVer = vrows[0].next;
  await client.query(
    `INSERT INTO report_versions (report_id, version_number, name, description, config, saved_by)
     VALUES ($1,$2,$3,$4,$5,$6)`,
    [reportId, nextVer, r.name, r.description, JSON.stringify(r.config), savedBy || null]
  );
  return nextVer;
}

// POST /api/reports — create a new saved report (always new, no upsert)
// Body: { name, description?, config }
app.post('/api/reports', async (req, res) => {
  const clientId = getClientId(req);
  const userOid  = getUserOid(req);
  const { name, description, config } = req.body;
  if (!name || !config) return res.status(400).json({ error: 'name and config required' });
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const r = await client.query(
      `INSERT INTO saved_reports (client_id, created_by, name, description, config)
       VALUES ($1,$2,$3,$4,$5) RETURNING *`,
      [clientId, userOid, name, description || null, JSON.stringify(config)]
    );
    const row = r.rows[0];
    // Snapshot as v1
    await client.query(
      `INSERT INTO report_versions (report_id, version_number, name, description, config, saved_by)
       VALUES ($1,1,$2,$3,$4,$5)`,
      [row.id, name, description || null, JSON.stringify(config), userOid || null]
    );
    await client.query('COMMIT');
    res.json({ report: row, version: 1 });
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('POST /api/reports', err);
    res.status(500).json({ error: err.message });
  } finally { client.release(); }
});

// PUT /api/reports/:id — overwrite a saved report, snapshot previous state first
// Body: { name, description?, config }
app.put('/api/reports/:id', async (req, res) => {
  const clientId = getClientId(req);
  const userOid  = getUserOid(req);
  const { name, description, config } = req.body;
  if (!name || !config) return res.status(400).json({ error: 'name and config required' });
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    // Verify ownership
    const check = await client.query(
      `SELECT id FROM saved_reports WHERE id=$1 AND client_id=$2`,
      [req.params.id, clientId]
    );
    if (!check.rows.length) { await client.query('ROLLBACK'); return res.status(404).json({ error: 'Not found' }); }
    // Snapshot current state before overwriting
    const versionNumber = await _snapshotVersion(client, req.params.id, userOid);
    // Update the report
    const r = await client.query(
      `UPDATE saved_reports SET name=$1, description=$2, config=$3, updated_at=NOW()
       WHERE id=$4 AND client_id=$5 RETURNING *`,
      [name, description || null, JSON.stringify(config), req.params.id, clientId]
    );
    await client.query('COMMIT');
    res.json({ report: r.rows[0], version: versionNumber + 1 });
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('PUT /api/reports/:id', err);
    res.status(500).json({ error: err.message });
  } finally { client.release(); }
});

// GET /api/reports/:id/versions — list all versions for a report
app.get('/api/reports/:id/versions', async (req, res) => {
  const clientId = getClientId(req);
  try {
    // Verify ownership
    const check = await pool.query(
      `SELECT id FROM saved_reports WHERE id=$1 AND client_id=$2`,
      [req.params.id, clientId]
    );
    if (!check.rows.length) return res.status(404).json({ error: 'Not found' });
    const { rows } = await pool.query(
      `SELECT id, version_number, name, description, saved_by, saved_at
       FROM report_versions
       WHERE report_id=$1
       ORDER BY version_number DESC`,
      [req.params.id]
    );
    res.json({ versions: rows });
  } catch (err) {
    console.error('GET /api/reports/:id/versions', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/reports/:id/versions/:versionId — fetch full config of one version
app.get('/api/reports/:id/versions/:versionId', async (req, res) => {
  const clientId = getClientId(req);
  try {
    const check = await pool.query(
      `SELECT id FROM saved_reports WHERE id=$1 AND client_id=$2`,
      [req.params.id, clientId]
    );
    if (!check.rows.length) return res.status(404).json({ error: 'Not found' });
    const { rows } = await pool.query(
      `SELECT * FROM report_versions WHERE id=$1 AND report_id=$2`,
      [req.params.versionId, req.params.id]
    );
    if (!rows.length) return res.status(404).json({ error: 'Version not found' });
    res.json({ version: rows[0] });
  } catch (err) {
    console.error('GET /api/reports/:id/versions/:versionId', err);
    res.status(500).json({ error: err.message });
  }
});

// DELETE /api/reports/:id
app.delete('/api/reports/:id', async (req, res) => {
  const clientId = getClientId(req);
  try {
    await pool.query(
      `DELETE FROM saved_reports WHERE id=$1 AND client_id=$2`,
      [req.params.id, clientId]
    );
    res.json({ ok: true });
  } catch (err) {
    console.error('DELETE /api/reports/:id', err);
    res.status(500).json({ error: err.message });
  }
});

// ─── Dashboards ──────────────────────────────────────────────────────────────

// GET /api/dashboards — list all saved dashboards for this client
app.get('/api/dashboards', async (req, res) => {
  const clientId = getClientId(req);
  try {
    const { rows } = await pool.query(
      `SELECT id, name, is_default, created_by, created_at, updated_at
       FROM dashboards
       WHERE client_id = $1
       ORDER BY is_default DESC, updated_at DESC`,
      [clientId]
    );
    res.json({ dashboards: rows });
  } catch (err) {
    console.error('GET /api/dashboards', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/dashboards/:id — get a single dashboard with full config
app.get('/api/dashboards/:id', async (req, res) => {
  const clientId = getClientId(req);
  try {
    const { rows } = await pool.query(
      `SELECT * FROM dashboards WHERE id=$1 AND client_id=$2`,
      [req.params.id, clientId]
    );
    if (!rows.length) return res.status(404).json({ error: 'Not found' });
    res.json({ dashboard: rows[0] });
  } catch (err) {
    console.error('GET /api/dashboards/:id', err);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/dashboards — create or update a dashboard
// Body: { name, config, is_default?, id? }
app.post('/api/dashboards', async (req, res) => {
  const clientId = getClientId(req);
  const userOid  = getUserOid(req);
  const { name, config, is_default, id } = req.body;

  if (!name || !config) return res.status(400).json({ error: 'name and config required' });

  try {
    let row;
    if (id) {
      const r = await pool.query(
        `UPDATE dashboards
         SET name=$1, config=$2, is_default=$3, updated_at=NOW()
         WHERE id=$4 AND client_id=$5
         RETURNING *`,
        [name, JSON.stringify(config), !!is_default, id, clientId]
      );
      row = r.rows[0];
      if (!row) return res.status(404).json({ error: 'Not found' });
    } else {
      const r = await pool.query(
        `INSERT INTO dashboards (client_id, created_by, name, config, is_default)
         VALUES ($1, $2, $3, $4, $5)
         RETURNING *`,
        [clientId, userOid, name, JSON.stringify(config), !!is_default]
      );
      row = r.rows[0];
    }
    res.json({ dashboard: row });
  } catch (err) {
    console.error('POST /api/dashboards', err);
    res.status(500).json({ error: err.message });
  }
});

// DELETE /api/dashboards/:id
app.delete('/api/dashboards/:id', async (req, res) => {
  const clientId = getClientId(req);
  try {
    await pool.query(
      `DELETE FROM dashboards WHERE id=$1 AND client_id=$2`,
      [req.params.id, clientId]
    );
    res.json({ ok: true });
  } catch (err) {
    console.error('DELETE /api/dashboards/:id', err);
    res.status(500).json({ error: err.message });
  }
});

// ─── Saved Charts (Dataset Explorer) ────────────────────────────────────────

// GET /api/charts
app.get('/api/charts', async (req, res) => {
  const clientId = getClientId(req);
  try {
    const { rows } = await pool.query(
      `SELECT id, name, dataset, config, created_by, created_at, updated_at
       FROM saved_charts
       WHERE client_id = $1
       ORDER BY updated_at DESC`,
      [clientId]
    );
    res.json({ charts: rows });
  } catch (err) {
    console.error('GET /api/charts', err);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/charts — create or update
app.post('/api/charts', async (req, res) => {
  const clientId = getClientId(req);
  const userOid  = getUserOid(req);
  const { name, dataset, config, id } = req.body;
  if (!name) return res.status(400).json({ error: 'name required' });
  try {
    let row;
    if (id) {
      const r = await pool.query(
        `UPDATE saved_charts
         SET name=$1, dataset=$2, config=$3, updated_at=NOW()
         WHERE id=$4 AND client_id=$5
         RETURNING *`,
        [name, dataset || null, JSON.stringify(config || {}), id, clientId]
      );
      row = r.rows[0];
      if (!row) return res.status(404).json({ error: 'Not found' });
    } else {
      const r = await pool.query(
        `INSERT INTO saved_charts (client_id, created_by, name, dataset, config)
         VALUES ($1, $2, $3, $4, $5)
         RETURNING *`,
        [clientId, userOid, name, dataset || null, JSON.stringify(config || {})]
      );
      row = r.rows[0];
    }
    res.json({ chart: row });
  } catch (err) {
    console.error('POST /api/charts', err);
    res.status(500).json({ error: err.message });
  }
});

// DELETE /api/charts/:id
app.delete('/api/charts/:id', async (req, res) => {
  const clientId = getClientId(req);
  try {
    await pool.query(
      `DELETE FROM saved_charts WHERE id=$1 AND client_id=$2`,
      [req.params.id, clientId]
    );
    res.json({ ok: true });
  } catch (err) {
    console.error('DELETE /api/charts/:id', err);
    res.status(500).json({ error: err.message });
  }
});

// ─── Schedules ───────────────────────────────────────────────────────────────

// GET /api/schedules — list all schedules for this client
app.get('/api/schedules', async (req, res) => {
  const clientId = getClientId(req);
  try {
    const { rows } = await pool.query(
      `SELECT s.*, r.name AS report_name
       FROM schedules s
       LEFT JOIN saved_reports r ON r.id = s.report_id
       WHERE s.client_id = $1
       ORDER BY s.created_at DESC`,
      [clientId]
    );
    res.json({ schedules: rows });
  } catch (err) {
    console.error('GET /api/schedules', err);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/schedules — create or update a schedule
// Body: { name, report_id, cron, config, active?, id? }
app.post('/api/schedules', async (req, res) => {
  const clientId = getClientId(req);
  const { name, report_id, cron, config, active, id } = req.body;

  if (!name) return res.status(400).json({ error: 'name required' });

  try {
    let row;
    if (id) {
      const r = await pool.query(
        `UPDATE schedules
         SET name=$1, report_id=$2, cron=$3, config=$4, active=$5
         WHERE id=$6 AND client_id=$7
         RETURNING *`,
        [name, report_id || null, cron || null, JSON.stringify(config || {}), active !== false, id, clientId]
      );
      row = r.rows[0];
      if (!row) return res.status(404).json({ error: 'Not found' });
    } else {
      const r = await pool.query(
        `INSERT INTO schedules (client_id, name, report_id, cron, config, active)
         VALUES ($1, $2, $3, $4, $5, $6)
         RETURNING *`,
        [clientId, name, report_id || null, cron || null, JSON.stringify(config || {}), active !== false]
      );
      row = r.rows[0];
    }
    res.json({ schedule: row });
  } catch (err) {
    console.error('POST /api/schedules', err);
    res.status(500).json({ error: err.message });
  }
});

// DELETE /api/schedules/:id
app.delete('/api/schedules/:id', async (req, res) => {
  const clientId = getClientId(req);
  try {
    await pool.query(
      `DELETE FROM schedules WHERE id=$1 AND client_id=$2`,
      [req.params.id, clientId]
    );
    res.json({ ok: true });
  } catch (err) {
    console.error('DELETE /api/schedules/:id', err);
    res.status(500).json({ error: err.message });
  }
});

// ─── BI Proxy ────────────────────────────────────────────────────────────────
// Intercepts /api/bi/* requests and routes them to the correct upstream based on
// the active instance (resolved from Host header).
//
// fcc-adapter  → direct proxy to client FCC API (no translation)
// insight-adapter → normalises Insight API request/response to platform shape

// Minimal fetch helper using built-in http/https modules
function fetchJSON(urlStr, opts = {}) {
  return new Promise((resolve, reject) => {
    const u = new URL(urlStr);
    const lib = u.protocol === 'https:' ? https : http;
    const body = opts.body ? Buffer.from(opts.body, 'utf8') : null;
    const headers = { 'Content-Type': 'application/json', ...(opts.headers || {}) };
    if (body) headers['Content-Length'] = body.length;
    const req = lib.request({
      hostname: u.hostname, port: u.port || (u.protocol === 'https:' ? 443 : 80),
      path: u.pathname + u.search, method: opts.method || 'GET', headers
    }, (res) => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        try { resolve({ status: res.statusCode, body: JSON.parse(data) }); }
        catch(e) { resolve({ status: res.statusCode, body: data }); }
      });
    });
    req.on('error', reject);
    if (body) req.write(body);
    req.end();
  });
}


// ── Insight API type inference ────────────────────────────────────
// Insight API returns segments/metrics as plain string arrays with no type info.
// We infer segmentType from the field name.
function inferSegmentType(name) {
  const n = name.toLowerCase();
  if (/_date$|_month$|_year$/.test(n)) return 'date';
  if (/_id$/.test(n)) return 'string';
  if (/total|count|price|amount|sales|visits|points|avg|sum|pieces/.test(n)) return 'number';
  return 'string';
}

// Friendly display name from snake_case
function toDisplayAlias(name) {
  return name.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
}

// ── Insight API response normalizers ─────────────────────────────

// Hardcoded dataset definitions — exact schema from /api/v1/datasets once deployed.
// Segments and metrics are objects with { name, type, defaultAggregation? }.
// This is used as a fallback when the live endpoint returns 404.
const INSIGHT_FALLBACK_DATASETS = {
  sales: {
    description: 'Invoice / sales transactions',
    segments: [
      { name: 'invoice_id',      type: 'number' },
      { name: 'internal_id',     type: 'string' },
      { name: 'status',          type: 'string' },
      { name: 'location_id',     type: 'number' },
      { name: 'location_name',   type: 'string' },
      { name: 'route_id',        type: 'number' },
      { name: 'route_name',      type: 'string' },
      { name: 'customer_id',     type: 'number' },
      { name: 'detail_clerk',    type: 'string' },
      { name: 'pickup_clerk',    type: 'string' },
      { name: 'kiosk',           type: 'string' },
      { name: 'coupon',          type: 'string' },
      { name: 'dropoff_date',    type: 'date'   },
      { name: 'ready_date',      type: 'date'   },
      { name: 'pickup_date',     type: 'date'   },
      { name: 'dropoff_month',   type: 'string' },
      { name: 'dropoff_year',    type: 'number' },
    ],
    metrics: [
      { name: 'total',             type: 'currency', defaultAggregation: 'SUM'   },
      { name: 'base_price',        type: 'currency', defaultAggregation: 'SUM'   },
      { name: 'upcharge_total',    type: 'currency', defaultAggregation: 'SUM'   },
      { name: 'alteration_total',  type: 'currency', defaultAggregation: 'SUM'   },
      { name: 'void_total',        type: 'currency', defaultAggregation: 'SUM'   },
      { name: 'coupon_total',      type: 'currency', defaultAggregation: 'SUM'   },
      { name: 'adjustment_total',  type: 'currency', defaultAggregation: 'SUM'   },
      { name: 'discount_total',    type: 'currency', defaultAggregation: 'SUM'   },
      { name: 'tax_total',         type: 'currency', defaultAggregation: 'SUM'   },
      { name: 'pieces',            type: 'count',    defaultAggregation: 'SUM'   },
      { name: 'invoice_count',     type: 'count',    defaultAggregation: 'COUNT' },
    ]
  },
  customers: {
    description: 'Customer profiles and lifetime metrics',
    segments: [
      { name: 'customer_id',            type: 'number' },
      { name: 'internal_id',            type: 'string' },
      { name: 'status',                 type: 'string' },
      { name: 'location_id',            type: 'number' },
      { name: 'location_name',          type: 'string' },
      { name: 'route_id',               type: 'number' },
      { name: 'route_name',             type: 'string' },
      { name: 'city',                   type: 'string' },
      { name: 'state',                  type: 'string' },
      { name: 'zip',                    type: 'string' },
      { name: 'payment_method',         type: 'string' },
      { name: 'referral_source',        type: 'string' },
      { name: 'signup_origin',          type: 'string' },
      { name: 'signup_type',            type: 'string' },
      { name: 'rewards_program',        type: 'string' },
      { name: 'original_signup_date',   type: 'date'   },
      { name: 'original_signup_month',  type: 'string' },
      { name: 'original_signup_year',   type: 'number' },
      { name: 'last_visit_date',        type: 'date'   },
      { name: 'last_visit_month',       type: 'string' },
    ],
    metrics: [
      { name: 'sales_pickup_30',       type: 'currency', defaultAggregation: 'SUM' },
      { name: 'sales_pickup_60',       type: 'currency', defaultAggregation: 'SUM' },
      { name: 'sales_pickup_90',       type: 'currency', defaultAggregation: 'SUM' },
      { name: 'sales_pickup_365',      type: 'currency', defaultAggregation: 'SUM' },
      { name: 'sales_pickup_lifetime', type: 'currency', defaultAggregation: 'SUM' },
      { name: 'visits_365',            type: 'count',    defaultAggregation: 'SUM' },
      { name: 'visits_lifetime',       type: 'count',    defaultAggregation: 'SUM' },
      { name: 'visit_average_sales',   type: 'currency', defaultAggregation: 'AVG' },
      { name: 'visit_average_pieces',  type: 'count',    defaultAggregation: 'AVG' },
      { name: 'visits_interval_avg',   type: 'number',   defaultAggregation: 'AVG' },
      { name: 'rewards_points',        type: 'count',    defaultAggregation: 'SUM' },
      { name: 'customer_count',        type: 'count',    defaultAggregation: 'COUNT' },
    ]
  }
};

// Normalize GET /api/v1/datasets response (segments/metrics are objects with { name, type })
// → FCC list shape: [{ name, description, segmentCount, metricCount }]
function normalizeInsightDatasetList(raw) {
  return Object.entries(raw).map(([name, def]) => ({
    name,
    description:  def.description || name,
    segmentCount: (def.segments || []).length,
    metricCount:  (def.metrics  || []).length
  }));
}

// Map Insight segment type → internal segmentType
function insightTypeToSegmentType(t) {
  if (t === 'date')   return 'date';
  if (t === 'number') return 'number';
  return 'string';
}

// Map Insight metric type → displayFormat
function insightTypeToDisplayFormat(t) {
  if (t === 'currency') return 'currency';
  if (t === 'count')    return 'number';
  return 'auto';
}

// Normalize one dataset entry (segments/metrics as objects)  →  FCC detail shape
function normalizeInsightDatasetDetail(name, entry) {
  const segments = (entry.segments || []).map(seg => {
    const segName = typeof seg === 'string' ? seg : seg.name;
    const segType = typeof seg === 'object' ? insightTypeToSegmentType(seg.type) : inferSegmentType(segName);
    return {
      segment:      { segmentName: segName, segmentType: segType },
      displayAlias: toDisplayAlias(segName)
    };
  });
  const metrics = (entry.metrics || []).map(m => {
    const mName = typeof m === 'string' ? m : m.name;
    const mType = typeof m === 'object' ? m.type : null;
    const mAgg  = (typeof m === 'object' && m.defaultAggregation) ? m.defaultAggregation : 'SUM';
    return {
      metricName:      mName,
      aggregationType: mAgg,
      displayFormat:   mType ? insightTypeToDisplayFormat(mType) : 'auto',
      prefix:          mType === 'currency' ? '$' : '',
      suffix:          ''
    };
  });
  return {
    description: entry.description || name,
    dataset:     { name, baseSQL: '', datasetSegments: segments },
    segments,
    metrics
  };
}

// Normalize query response  →  FCC shape: { success, data: { data: [...] } }
function normalizeInsightQueryResponse(raw) {
  if (raw && Array.isArray(raw.data)) {
    return { success: true, data: { data: raw.data } };
  }
  // fallback — wrap whatever came back
  return { success: true, data: { data: Array.isArray(raw) ? raw : [] } };
}

// Derive KPIs from query rows (sum each metric column across all rows)
function deriveKpis(rows, metrics) {
  const kpis = metrics.map(m => {
    const alias = m.alias || m.metricName;
    const total = rows.reduce((s, r) => s + (parseFloat(r[alias]) || 0), 0);
    return { name: alias, value: total };
  });
  return { success: true, data: { kpis } };
}

// ── /api/bi/* proxy ───────────────────────────────────────────────

// ── Internal adapter helpers (datasets hosted in our own ES) ────────────────

async function internalGetDatasets(clientId) {
  const { rows } = await pool.query(
    `SELECT dd.name, dd.label as description, dd.is_active,
            (SELECT COUNT(*) FROM dataset_schema_versions dsv
             JOIN LATERAL jsonb_array_elements(dsv.fields) f(v) ON TRUE
             WHERE dsv.dataset_id=dd.id AND dsv.version=dd.current_version AND v->>'fieldType'='segment') as segment_count,
            (SELECT COUNT(*) FROM dataset_schema_versions dsv
             JOIN LATERAL jsonb_array_elements(dsv.fields) f(v) ON TRUE
             WHERE dsv.dataset_id=dd.id AND dsv.version=dd.current_version AND v->>'fieldType'='metric') as metric_count
     FROM dataset_definitions dd
     WHERE dd.client_id=$1 AND dd.is_active=TRUE ORDER BY dd.name`,
    [clientId]
  );
  return rows.map(r => ({
    name: r.name, description: r.description,
    segmentCount: parseInt(r.segment_count,10),
    metricCount:  parseInt(r.metric_count,10),
    isActive: r.is_active
  }));
}

async function internalGetDatasetDetail(clientId, datasetName) {
  const { rows: [def] } = await pool.query(
    `SELECT dd.id AS dd_id, dd.name, dd.label, dd.current_version, dd.es_alias,
            dsv.fields
     FROM dataset_definitions dd
     JOIN dataset_schema_versions dsv ON dsv.dataset_id=dd.id AND dsv.version=dd.current_version
     WHERE dd.client_id=$1 AND dd.name=$2 AND dd.is_active=TRUE`,
    [clientId, datasetName]
  );
  if (!def) return null;

  // Overlay Designer-saved metadata (format, decimals, labels, hidden) on top of schema fields
  const { rows: metaRows } = await pool.query(
    `SELECT field_name, label, format, decimal_places, is_hidden, lookup_dataset_id, lookup_key_field
     FROM dataset_field_metadata WHERE dataset_id=$1`,
    [def.dd_id]
  );
  const metaByField = {};
  metaRows.forEach(m => { metaByField[m.field_name] = m; });

  const fields = def.fields || [];
  const segments = fields
    .filter(f => f.fieldType === 'segment')
    .map(f => {
      const meta = metaByField[f.name] || {};
      return {
        segment: { segmentName: f.name, segmentType: f.segmentType || 'string',
                   isFilterable: f.isFilterable !== false, isGroupable: f.isGroupable !== false,
                   isHidden: meta.is_hidden || false,
                   lookupDatasetId: meta.lookup_dataset_id || null,
                   lookupKeyField:  meta.lookup_key_field  || null },
        displayAlias: meta.label || f.label || f.name.replace(/_/g,' ').replace(/\b\w/g,c=>c.toUpperCase())
      };
    });
  const metrics = fields
    .filter(f => f.fieldType === 'metric')
    .map(f => {
      const meta = metaByField[f.name] || {};
      // Designer format takes precedence; fall back to schema-level, then 'number'
      const fmt = meta.format || f.displayFormat || 'number';
      const dec = meta.decimal_places != null ? meta.decimal_places
                : (fmt === 'currency' ? 2 : fmt === 'percent' ? 1 : 0);
      return {
        metricName:      f.name,
        displayAlias:    meta.label || f.label || f.name.replace(/_/g,' ').replace(/\b\w/g,c=>c.toUpperCase()),
        aggregationType: f.aggregationType || 'SUM',
        displayFormat:   fmt,
        decimalPlaces:   dec,
        prefix:          fmt === 'currency' ? '$' : (f.prefix || ''),
        suffix:          f.suffix || ''
      };
    });

  return {
    description: def.label,
    dataset: { name: def.name, baseSQL: '', datasetSegments: segments },
    segments, metrics
  };
}

app.get('/api/bi/datasets', async (req, res) => {
  const inst = resolveInstance(req);

  if (inst.adapter === 'internal') {
    try {
      const datasets = await internalGetDatasets(inst.clientId);
      return res.json({ success: true, data: { datasets } });
    } catch (e) { return res.status(500).json({ error: e.message }); }
  }

  if (inst.adapter === 'insight') {
    // Try the live endpoint; fall back to hardcoded definitions if not yet deployed (404)
    try {
      const r = await fetchJSON(`${inst.apiBase}/api/v1/datasets`, {
        headers: { Authorization: inst.apiToken }
      });
      if (r.status === 200) {
        const datasets = normalizeInsightDatasetList(r.body);
        return res.json({ success: true, data: { datasets } });
      }
      // Non-200 (including 404) — fall through to hardcoded fallback
    } catch (e) { /* fall through */ }

    // Fallback: return hardcoded dataset list
    const datasets = Object.entries(INSIGHT_FALLBACK_DATASETS).map(([name, def]) => ({
      name,
      description:  def.description,
      segmentCount: def.segments.length,
      metricCount:  def.metrics.length
    }));
    return res.json({ success: true, data: { datasets } });
  }

  // FCC — proxy directly
  try {
    const r = await fetchJSON(`${inst.apiBase}/bi/datasets`);
    return res.status(r.status).json(r.body);
  } catch (e) {
    return res.status(502).json({ error: e.message });
  }
});

// ─── Bootstrap ───────────────────────────────────────────────────────────────
// One-time setup: seeds client row in Postgres + generates first ingest API key.
// Protected by BOOTSTRAP_SECRET env var. Idempotent — safe to call again.
app.post('/api/system/bootstrap', async (req, res) => {
  const secret = process.env.BOOTSTRAP_SECRET;
  if (!secret) return res.status(403).json({ error: 'BOOTSTRAP_SECRET not configured' });
  if (req.headers['x-bootstrap-secret'] !== secret) return res.status(403).json({ error: 'Invalid bootstrap secret' });
  const { generateKey, hashKey } = require('./ingestRouter');
  const clientId = INSTANCE.clientId;
  try {
    await pool.query(
      `INSERT INTO clients (client_id, name) VALUES ($1,$2) ON CONFLICT (client_id) DO UPDATE SET name=$2`,
      [clientId, INSTANCE.name]
    );
    const { rows: existing } = await pool.query(
      `SELECT id FROM instance_api_keys WHERE client_id=$1 AND revoked=FALSE LIMIT 1`, [clientId]
    );
    let apiKey = null;
    if (!existing.length) {
      const raw = generateKey();
      await pool.query(
        `INSERT INTO instance_api_keys (client_id, key_hash, label) VALUES ($1,$2,'Bootstrap Key')`,
        [clientId, hashKey(raw)]
      );
      apiKey = raw;
    }
    res.json({
      success:    true,
      instanceId: clientId,
      message:    existing.length ? 'Already bootstrapped — no new key generated' : 'Bootstrap complete',
      apiKey:     apiKey || '(key exists — use /api/ingest/admin/api-keys to manage)',
      warning:    apiKey ? 'Store this key securely. It will not be shown again.' : null
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/bi/datasets/:name', async (req, res) => {
  const inst = resolveInstance(req);
  const dsName = req.params.name;

  if (inst.adapter === 'internal') {
    try {
      const detail = await internalGetDatasetDetail(inst.clientId, dsName);
      if (!detail) return res.status(404).json({ error: `Dataset '${dsName}' not found` });
      return res.json({ success: true, data: detail });
    } catch (e) { return res.status(500).json({ error: e.message }); }
  }

  if (inst.adapter === 'insight') {
    // Try live endpoint first; fall back to hardcoded schema if not yet deployed
    try {
      const r = await fetchJSON(`${inst.apiBase}/api/v1/datasets`, {
        headers: { Authorization: inst.apiToken }
      });
      if (r.status === 200) {
        const entry = r.body[dsName];
        if (!entry) return res.status(404).json({ error: `Dataset '${dsName}' not found` });
        return res.json({ success: true, data: normalizeInsightDatasetDetail(dsName, entry) });
      }
      // Non-200 — fall through to hardcoded
    } catch (e) { /* fall through */ }
    // Fallback: build detail from hardcoded definitions
    const def = INSIGHT_FALLBACK_DATASETS[dsName];
    if (!def) return res.status(404).json({ error: `Dataset '${dsName}' not found` });
    return res.json({ success: true, data: normalizeInsightDatasetDetail(dsName, def) });
  }

  // FCC — proxy directly
  try {
    const r = await fetchJSON(`${inst.apiBase}/bi/datasets/${dsName}`);
    return res.status(r.status).json(r.body);
  } catch (e) {
    return res.status(502).json({ error: e.message });
  }
});

app.post('/api/bi/query', async (req, res) => {
  const inst = resolveInstance(req);
  const body = req.body;

  if (inst.adapter === 'internal') {
    try {
      const result = await esClient.query(inst.clientId, body.datasetName, body);
      return res.json({ success: true, data: result });
    } catch (e) { return res.status(500).json({ success: false, error: e.message }); }
  }

  if (inst.adapter === 'insight') {
    const dsName = body.datasetName;
    if (!dsName) return res.status(400).json({ error: 'datasetName required' });
    // Request body is already compatible — Insight API uses the same shape
    const upstream = {
      groupBySegments: body.groupBySegments || [],
      metrics:         (body.metrics || []).map(m => ({
        metricName:  m.metricName,
        aggregation: m.aggregation || 'SUM',
        alias:       m.alias || m.metricName
      })),
      pagination: body.pagination || { page: 1, pageSize: 1000 }
    };
    // Only include filters/orderBy if non-empty — Insight API rejects empty arrays
    if (body.filters  && body.filters.length)  upstream.filters  = body.filters;
    if (body.orderBy  && body.orderBy.length)  upstream.orderBy  = body.orderBy;
    try {
      const r = await fetchJSON(`${inst.apiBase}/api/v1/datasets/${dsName}/query`, {
        method:  'POST',
        headers: { Authorization: inst.apiToken },
        body:    JSON.stringify(upstream)
      });
      if (r.status !== 200) {
        const errDetail = typeof r.body === 'object' ? r.body : { raw: r.body };
        return res.status(r.status).json({ success: false, error: 'Upstream query error', detail: errDetail });
      }
      return res.json(normalizeInsightQueryResponse(r.body));
    } catch (e) {
      return res.status(503).json({ success: false, error: 'Query service unavailable — upstream returned no response', detail: e.message });
    }
  }

  // FCC — proxy directly
  try {
    const r = await fetchJSON(`${inst.apiBase}/bi/query`, {
      method: 'POST',
      body:   JSON.stringify(body)
    });
    return res.status(r.status).json(r.body);
  } catch (e) {
    return res.status(502).json({ error: e.message });
  }
});

app.post('/api/bi/kpis', async (req, res) => {
  const inst = resolveInstance(req);
  const body = req.body;

  if (inst.adapter === 'internal') {
    try {
      // Run as groupBySegments=[] to get aggregate totals
      const kpiBody = { ...body, groupBySegments: [] };
      const result  = await esClient.query(inst.clientId, body.datasetName, kpiBody);
      const kpis    = Object.entries(result.data[0] || {}).map(([name, value]) => ({ name, value }));
      return res.json({ success: true, data: { kpis } });
    } catch (e) { return res.status(500).json({ success: false, error: e.message }); }
  }

  if (inst.adapter === 'insight') {
    // Insight API has no dedicated KPI endpoint — run the same query and derive KPIs
    const dsName = body.datasetName;
    if (!dsName) return res.status(400).json({ error: 'datasetName required' });
    const upstream = {
      groupBySegments: body.groupBySegments || [],
      metrics:         (body.metrics || []).map(m => ({
        metricName:  m.metricName,
        aggregation: m.aggregation || 'SUM',
        alias:       m.alias || m.metricName
      })),
      pagination: { page: 1, pageSize: 1000 }
    };
    if (body.filters && body.filters.length) upstream.filters = body.filters;
    try {
      const r = await fetchJSON(`${inst.apiBase}/api/v1/datasets/${dsName}/query`, {
        method:  'POST',
        headers: { Authorization: inst.apiToken },
        body:    JSON.stringify(upstream)
      });
      if (r.status !== 200) return res.status(r.status).json({ success: false, error: r.body });
      const rows = (r.body && Array.isArray(r.body.data)) ? r.body.data : [];
      return res.json(deriveKpis(rows, upstream.metrics));
    } catch (e) {
      // Query endpoint temporarily unavailable — return empty KPIs gracefully
      console.warn('[insight-adapter] KPI query error:', e.message);
      return res.json({ success: true, data: { kpis: [] } });
    }
  }

  // FCC — proxy directly
  try {
    const r = await fetchJSON(`${inst.apiBase}/bi/kpis`, {
      method: 'POST',
      body:   JSON.stringify(body)
    });
    return res.status(r.status).json(r.body);
  } catch (e) {
    return res.status(502).json({ error: e.message });
  }
});


// POST /api/bi/kpis/compare
// Body: same as /bi/kpis but also requires { compareFrom, compareTo } dates
// Returns { current: [{name,value}], prior: [{name,value}] }
app.post('/api/bi/kpis/compare', async (req, res) => {
  const inst = resolveInstance(req);
  const { compareFrom, compareTo, ...kpiBody } = req.body;
  if (!compareFrom || !compareTo) return res.status(400).json({ error: 'compareFrom and compareTo required' });
  if (inst.adapter !== 'internal') return res.json({ success: true, data: null }); // only for internal adapter

  try {
    // Run current period and prior period in parallel
    const dateSeg = req.body.dateSegment || kpiBody.filters?.find(f => f.type === 'date_range')?.segmentName || null;

    // Build prior-period filter: replace date filters with compare range
    const priorFilters = (kpiBody.filters || []).filter(f => f.type !== 'date_range');
    if (dateSeg) {
      priorFilters.push({ segmentName: dateSeg, type: 'date_range', from: compareFrom, to: compareTo });
    }

    const currentBody = { ...kpiBody, groupBySegments: [] };
    const priorBody   = { ...kpiBody, groupBySegments: [], filters: priorFilters.length ? priorFilters : undefined };

    const [curResult, priResult] = await Promise.all([
      esClient.query(inst.clientId, kpiBody.datasetName, currentBody),
      esClient.query(inst.clientId, kpiBody.datasetName, priorBody)
    ]);

    const current = Object.entries(curResult.data[0] || {}).map(([name, value]) => ({ name, value }));
    const prior   = Object.entries(priResult.data[0] || {}).map(([name, value]) => ({ name, value }));

    return res.json({ success: true, data: { current, prior, compareFrom, compareTo } });
  } catch(e) { return res.status(500).json({ success: false, error: e.message }); }
});

app.get('/api/bi/segment-values', async (req, res) => {
  const inst = resolveInstance(req);
  const dataset  = req.query.dataset  || req.query.datasetName;
  const segment  = req.query.segment  || req.query.segmentName;
  if (!dataset || !segment) return res.status(400).json({ error: 'dataset and segment required' });

  if (inst.adapter === 'internal') {
    try {
      const values = await esClient.segmentValues(inst.clientId, dataset, segment);
      return res.json({ success: true, data: { values } });
    } catch (e) { return res.status(500).json({ error: e.message }); }
  }

  if (inst.adapter === 'insight') {
    // No dedicated endpoint — query grouped by the segment with a COUNT metric
    const upstream = {
      groupBySegments: [segment],
      metrics: [],
      pagination: { page: 1, pageSize: 500 }
    };
    try {
      const r = await fetchJSON(`${inst.apiBase}/api/v1/datasets/${dataset}/query`, {
        method:  'POST',
        headers: { Authorization: inst.apiToken },
        body:    JSON.stringify(upstream)
      });
      if (r.status !== 200) return res.status(r.status).json({ success: false, error: r.body });
      const rows = (r.body && Array.isArray(r.body.data)) ? r.body.data : [];
      const values = rows
        .map(r => r[segment])
        .filter(v => v !== null && v !== undefined && v !== '')
        .map(v => ({ value: String(v), displayValue: String(v) }));
      return res.json({ success: true, data: { values } });
    } catch (e) {
      // Query endpoint temporarily unavailable — return empty values so filter dropdowns degrade gracefully
      console.warn('[insight-adapter] segment-values error:', e.message);
      return res.json({ success: true, data: { values: [] } });
    }
  }

  // FCC — proxy directly (API expects datasetName/segmentName)
  try {
    const r = await fetchJSON(
      `${inst.apiBase}/bi/segment-values?datasetName=${encodeURIComponent(dataset)}&segmentName=${encodeURIComponent(segment)}`
    );
    return res.status(r.status).json(r.body);
  } catch (e) {
    return res.status(502).json({ error: e.message });
  }
});

// POST /api/bi/datasets/:name/records
// Raw document drilldown — returns actual source records matching the given filters.
// Used by the drilldown drawer to show underlying rows behind any aggregated result.
app.post('/api/bi/datasets/:name/records', async (req, res) => {
  const inst   = resolveInstance(req);
  const dsName = req.params.name;
  const { filters = [], size = 100 } = req.body;

  if (inst.adapter !== 'internal') {
    // Future: proxy to upstream adapter's raw endpoint if it exposes one
    return res.status(501).json({ success: false, error: 'Source record drilldown only supported for internal datasets' });
  }

  try {
    const { records, total } = await esClient.rawQuery(inst.clientId, dsName, filters, Math.min(size, 500));
    return res.json({ success: true, data: { records, total, returned: records.length } });
  } catch (e) {
    return res.status(500).json({ success: false, error: e.message });
  }
});

// ─── Designer routes (browser-accessible, basic-auth protected) ─────────────
// Bridge the browser (apiFetch → /api/bi/admin/*) to Postgres directly.
// Only meaningful for internal adapter (produce). FCC/Acuity proxy to upstream.

// GET /api/bi/admin/datasets/:name/metadata
app.get('/api/bi/admin/datasets/:name/metadata', async (req, res) => {
  const inst = resolveInstance(req);
  if (inst.adapter !== 'internal') return res.json({ metadata: [] });
  try {
    const { rows: [dd] } = await pool.query(
      `SELECT id FROM dataset_definitions WHERE client_id=$1 AND name=$2`,
      [inst.clientId, req.params.name]
    );
    if (!dd) return res.status(404).json({ error: 'Dataset not found' });
    const { rows } = await pool.query(
      `SELECT * FROM dataset_field_metadata WHERE dataset_id=$1 ORDER BY sort_order, field_name`,
      [dd.id]
    );
    res.json({ success: true, metadata: rows });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// POST /api/bi/admin/datasets/:name/metadata
app.post('/api/bi/admin/datasets/:name/metadata', async (req, res) => {
  const inst = resolveInstance(req);
  if (inst.adapter !== 'internal') return res.status(403).json({ error: 'Not supported for this adapter' });
  const { fields = [] } = req.body;
  try {
    const { rows: [dd] } = await pool.query(
      `SELECT id FROM dataset_definitions WHERE client_id=$1 AND name=$2`,
      [inst.clientId, req.params.name]
    );
    if (!dd) return res.status(404).json({ error: 'Dataset not found' });
    const pgClient = await pool.connect();
    try {
      await pgClient.query('BEGIN');
      for (const f of fields) {
        if (!f.field_name) continue;
        await pgClient.query(`
          INSERT INTO dataset_field_metadata
            (dataset_id, field_name, label, format, decimal_places,
             is_hidden, lookup_dataset_id, lookup_key_field, display_field, field_type, updated_by)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'designer')
          ON CONFLICT (dataset_id, field_name) DO UPDATE SET
            label             = COALESCE(NULLIF($3,''),       dataset_field_metadata.label),
            format            = COALESCE(NULLIF($4,''),       dataset_field_metadata.format),
            decimal_places    = COALESCE($5::smallint,        dataset_field_metadata.decimal_places),
            is_hidden         = COALESCE($6::boolean,         dataset_field_metadata.is_hidden),
            lookup_dataset_id = $7::integer,
            lookup_key_field  = COALESCE(NULLIF($8,''),       dataset_field_metadata.lookup_key_field),
            display_field     = COALESCE(NULLIF($9,''),       dataset_field_metadata.display_field),
            field_type        = COALESCE(NULLIF($10,''),      dataset_field_metadata.field_type),
            updated_by        = 'designer',
            updated_at        = NOW()
        `, [
          dd.id, f.field_name,
          f.label             || null,
          f.format            || null,
          f.decimal_places != null ? parseInt(f.decimal_places) || null : null,
          f.is_hidden != null      ? !!f.is_hidden : null,
          f.lookup_dataset_id      ? parseInt(f.lookup_dataset_id) : null,
          f.lookup_key_field  || null,
          f.display_field     || null,
          f.field_type        || null,
        ]);
      }
      await pgClient.query('COMMIT');
      res.json({ success: true, saved: fields.length });
    } catch(e) {
      await pgClient.query('ROLLBACK'); throw e;
    } finally { pgClient.release(); }
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// GET /api/bi/admin/lookups
app.get('/api/bi/admin/lookups', async (req, res) => {
  const inst = resolveInstance(req);
  if (inst.adapter !== 'internal') return res.json({ lookups: [] });
  try {
    const { rows } = await pool.query(
      `SELECT ld.*, COUNT(ldr.id)::int AS row_count
       FROM lookup_datasets ld
       LEFT JOIN lookup_dataset_rows ldr ON ldr.lookup_dataset_id = ld.id
       WHERE ld.client_id=$1 AND ld.is_active=TRUE
       GROUP BY ld.id ORDER BY ld.name`,
      [inst.clientId]
    );
    res.json({ success: true, lookups: rows });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// GET /api/bi/admin/lookups/:name
app.get('/api/bi/admin/lookups/:name', async (req, res) => {
  const inst = resolveInstance(req);
  if (inst.adapter !== 'internal') return res.status(404).json({ error: 'Not found' });
  try {
    const { rows: [ld] } = await pool.query(
      `SELECT * FROM lookup_datasets WHERE client_id=$1 AND name=$2 AND is_active=TRUE`,
      [inst.clientId, req.params.name]
    );
    if (!ld) return res.status(404).json({ error: `Lookup '${req.params.name}' not found` });
    const { rows: fields } = await pool.query(
      `SELECT * FROM lookup_dataset_fields WHERE lookup_dataset_id=$1 ORDER BY sort_order`,
      [ld.id]
    );
    const { rows: dataRows } = await pool.query(
      `SELECT key_value, data, auto_populated, updated_at
       FROM lookup_dataset_rows WHERE lookup_dataset_id=$1 ORDER BY key_value`,
      [ld.id]
    );
    res.json({ success: true, lookup: ld, fields, rows: dataRows });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// GET /api/bi/admin/fiscal
app.get('/api/bi/admin/fiscal', async (req, res) => {
  const inst = resolveInstance(req);
  if (inst.adapter !== 'internal') return res.json({ calendars: [] });
  try {
    const { rows } = await pool.query(
      `SELECT fc.*, COUNT(fd.id)::int AS day_count
       FROM fiscal_calendars fc
       LEFT JOIN fiscal_days fd ON fd.calendar_id = fc.id
       WHERE fc.client_id=$1
       GROUP BY fc.id ORDER BY fc.created_at`,
      [inst.clientId]
    );
    res.json({ success: true, calendars: rows });
  } catch(e) { res.status(500).json({ error: e.message }); }
});


// GET /api/bi/fiscal/presets?date=YYYY-MM-DD
// Returns date boundaries for all fiscal presets for the given date (defaults to today).
// Client calls this once on init — no need to ship fiscal_days to the browser.
app.get('/api/bi/fiscal/presets', async (req, res) => {
  const inst = resolveInstance(req);
  if (inst.adapter !== 'internal') return res.json({ success: true, data: null });

  const refDate = req.query.date || new Date().toISOString().slice(0, 10);

  try {
    const calRow = await pool.query(
      `SELECT id FROM fiscal_calendars WHERE client_id=$1 AND is_active=TRUE ORDER BY id LIMIT 1`,
      [inst.clientId]
    );
    if (!calRow.rows.length) return res.json({ success: true, data: null });
    const calId = calRow.rows[0].id;

    // Look up the fiscal_days row for refDate to get week/month/year membership
    const { rows: [today] } = await pool.query(
      `SELECT fiscal_week, fiscal_week_start,
              fiscal_month, fiscal_month_start, fiscal_month_end,
              fiscal_quarter,
              fiscal_year, fiscal_year_start, fiscal_year_end
       FROM fiscal_days
       WHERE calendar_id=$1 AND calendar_date=$2::date`,
      [calId, refDate]
    );

    if (!today) return res.json({ success: true, data: null, reason: 'refDate not in fiscal_days' });

    // Fiscal week: start of this week → refDate (or end of week for full week view)
    const [fwRow] = (await pool.query(
      `SELECT MIN(calendar_date)::text AS from_, MAX(calendar_date)::text AS to_
       FROM fiscal_days
       WHERE calendar_id=$1 AND fiscal_week=$2
         AND fiscal_year=$3`,
      [calId, today.fiscal_week, today.fiscal_year]
    )).rows;

    // Last fiscal week
    const [fwLastRow] = (await pool.query(
      `SELECT MIN(calendar_date)::text AS from_, MAX(calendar_date)::text AS to_
       FROM fiscal_days
       WHERE calendar_id=$1 AND fiscal_week=$2
         AND fiscal_year=$3`,
      [calId, today.fiscal_week - 1, today.fiscal_year]
    )).rows;

    // Fiscal month
    const [fmRow] = (await pool.query(
      `SELECT MIN(calendar_date)::text AS from_, MAX(calendar_date)::text AS to_
       FROM fiscal_days
       WHERE calendar_id=$1 AND fiscal_month=$2
         AND fiscal_year=$3`,
      [calId, today.fiscal_month, today.fiscal_year]
    )).rows;

    // Last fiscal month
    const [fmLastRow] = (await pool.query(
      `SELECT MIN(calendar_date)::text AS from_, MAX(calendar_date)::text AS to_
       FROM fiscal_days
       WHERE calendar_id=$1 AND fiscal_month=$2
         AND fiscal_year=$3`,
      [calId, today.fiscal_month - 1, today.fiscal_year]
    )).rows;

    // Fiscal year
    const [fyRow] = (await pool.query(
      `SELECT MIN(calendar_date)::text AS from_, MAX(calendar_date)::text AS to_
       FROM fiscal_days
       WHERE calendar_id=$1 AND fiscal_year=$2`,
      [calId, today.fiscal_year]
    )).rows;

    // Fiscal quarter (current)
    const [fqRow] = (await pool.query(
      `SELECT MIN(calendar_date)::text AS from_, MAX(calendar_date)::text AS to_
       FROM fiscal_days
       WHERE calendar_id=$1 AND fiscal_quarter=$2 AND fiscal_year=$3`,
      [calId, today.fiscal_quarter, today.fiscal_year]
    )).rows;

    // Last fiscal quarter
    const prevQ  = today.fiscal_quarter === 1 ? 4 : today.fiscal_quarter - 1;
    const prevQY = today.fiscal_quarter === 1 ? today.fiscal_year - 1 : today.fiscal_year;
    const [fqLastRow] = (await pool.query(
      `SELECT MIN(calendar_date)::text AS from_, MAX(calendar_date)::text AS to_
       FROM fiscal_days
       WHERE calendar_id=$1 AND fiscal_quarter=$2 AND fiscal_year=$3`,
      [calId, prevQ, prevQY]
    )).rows;

    // Last fiscal year
    const [fyLastRow] = (await pool.query(
      `SELECT MIN(calendar_date)::text AS from_, MAX(calendar_date)::text AS to_
       FROM fiscal_days
       WHERE calendar_id=$1 AND fiscal_year=$2`,
      [calId, today.fiscal_year - 1]
    )).rows;

    res.json({
      success: true,
      data: {
        refDate,
        fiscal_week:         fwRow?.from_     ? { from: fwRow.from_,     to: fwRow.to_     } : null,
        fiscal_week_last:    fwLastRow?.from_  ? { from: fwLastRow.from_,  to: fwLastRow.to_  } : null,
        fiscal_month:        fmRow?.from_     ? { from: fmRow.from_,     to: fmRow.to_     } : null,
        fiscal_month_last:   fmLastRow?.from_  ? { from: fmLastRow.from_,  to: fmLastRow.to_  } : null,
        fiscal_quarter:      fqRow?.from_     ? { from: fqRow.from_,     to: fqRow.to_     } : null,
        fiscal_quarter_last: fqLastRow?.from_  ? { from: fqLastRow.from_,  to: fqLastRow.to_  } : null,
        fiscal_year:         fyRow?.from_     ? { from: fyRow.from_,     to: fyRow.to_     } : null,
        fiscal_year_last:    fyLastRow?.from_  ? { from: fyLastRow.from_,  to: fyLastRow.to_  } : null,
      }
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// GET /api/bi/fiscal/compare-range?from=YYYY-MM-DD&to=YYYY-MM-DD
// Returns same-period-last-year and prior-period date windows using fiscal_days
app.get('/api/bi/fiscal/compare-range', async (req, res) => {
  const inst = resolveInstance(req);
  if (inst.adapter !== 'internal') return res.json({ success: true, data: null });
  const { from, to } = req.query;
  if (!from || !to) return res.status(400).json({ error: 'from and to required' });
  try {
    // 1. Same-period last year: map each boundary date via same_day_last_year
    const splyr = await pool.query(
      `SELECT
         MIN(same_day_last_year)::text AS lyr_from,
         MAX(same_day_last_year)::text AS lyr_to
       FROM fiscal_days
       WHERE calendar_date IN ($1::date, $2::date)
         AND calendar_id = (SELECT id FROM fiscal_calendars WHERE client_id=$3 AND is_active=true ORDER BY id LIMIT 1)`,
      [from, to, inst.clientId]
    );
    const lyrFrom = splyr.rows[0]?.lyr_from;
    const lyrTo   = splyr.rows[0]?.lyr_to;

    // 2. Prior period: shift back by the same number of days
    const dayCount = await pool.query(
      `SELECT ($1::date - $2::date)::int AS span`, [to, from]
    );
    const span = dayCount.rows[0]?.span || 0;
    // Prior period ends the day before `from`, spans `span` days
    const priorTo   = new Date(new Date(from).getTime() - 86400000).toISOString().slice(0,10);
    const priorFrom = new Date(new Date(from).getTime() - 86400000 - span * 86400000).toISOString().slice(0,10);

    res.json({
      success: true,
      data: {
        samePeriodLastYear: lyrFrom && lyrTo ? { from: lyrFrom, to: lyrTo } : null,
        priorPeriod: { from: priorFrom, to: priorTo, days: span + 1 }
      }
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ─── Fallback ─────────────────────────────────────────────────────────────────
app.get('/debug', (req, res) => res.sendFile(path.join(__dirname, 'debug.html')));
app.get('*', (req, res) => {
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate');
  res.sendFile(path.join(__dirname, 'index.html'));
});

// ─── Boot ─────────────────────────────────────────────────────────────────────
initDb()
  .then(() => app.listen(PORT, () => console.log(`[${INSTANCE.id}] running on port ${PORT}`)))
  .catch(err => {
    console.error('Failed to init DB, starting without persistence:', err.message);
    app.listen(PORT, () => console.log(`[${INSTANCE.id}] running (no DB) on port ${PORT}`));
  });
