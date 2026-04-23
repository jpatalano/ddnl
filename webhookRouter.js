/**
 * webhookRouter.js — DDNL Inbound Webhook + Webhook Admin API
 *
 * Inbound receiver:
 *   POST /api/webhook/:client_id/:webhook_id
 *     — verified (HMAC + auth header + custom headers)
 *     — field-mapped
 *     — upserted into ES via dataset primary key
 *     — logged to webhook_delivery_log
 *
 * Admin CRUD (API-key protected):
 *   GET    /api/ingest/admin/webhooks                  — list webhooks for dataset
 *   POST   /api/ingest/admin/webhooks                  — create webhook
 *   GET    /api/ingest/admin/webhooks/:id              — get webhook
 *   PUT    /api/ingest/admin/webhooks/:id              — update webhook
 *   DELETE /api/ingest/admin/webhooks/:id              — delete webhook
 *   GET    /api/ingest/admin/webhooks/:id/logs         — delivery log
 *   POST   /api/ingest/admin/webhooks/:id/test         — send test payload
 */

'use strict';

const express = require('express');
const crypto  = require('crypto');
const { pool } = require('./db');
const es       = require('./esClient');

const router = express.Router();

// ── Instance clientId (injected at mount time via setInstanceClientId) ────────
let _instanceClientId = null;
router.setInstanceClientId = (id) => { _instanceClientId = id; };
router._requireBasicOrApiKey = (req, res, next) => requireBasicOrApiKey(req, res, next);

// ── Helpers ───────────────────────────────────────────────────────────────────

function hashKey(rawKey) {
  return crypto.createHash('sha256').update(rawKey).digest('hex');
}

async function resolveApiKey(rawKey) {
  if (!rawKey) return null;
  const hash = hashKey(rawKey);
  const { rows } = await pool.query(
    `SELECT id, client_id, label FROM instance_api_keys
     WHERE key_hash = $1 AND revoked = FALSE`, [hash]
  );
  if (!rows.length) return null;
  pool.query('UPDATE instance_api_keys SET last_used_at = NOW() WHERE id = $1', [rows[0].id]).catch(() => {});
  return { clientId: rows[0].client_id, keyId: rows[0].id, label: rows[0].label };
}

async function requireApiKey(req, res, next) {
  const raw = req.headers['x-api-key'] || req.headers['authorization']?.replace(/^Bearer\s+/i, '');
  const ctx = await resolveApiKey(raw);
  if (!ctx) return res.status(401).json({ error: 'Invalid or missing API key' });
  req.ingestCtx = ctx;
  next();
}

/**
 * Accepts either:
 *   - X-Api-Key / Bearer token  (external tools)
 *   - Basic auth credentials    (browser Designer UI)
 * When basic auth is used, clientId comes from the injected instance config.
 */
const BASIC_USER = process.env.BASIC_AUTH_USER || 'ddnl';
const BASIC_PASS = process.env.BASIC_AUTH_PASS || 'ddnl!';

async function requireBasicOrApiKey(req, res, next) {
  const authHeader = req.headers['authorization'] || '';

  // Try Basic auth first
  if (authHeader.startsWith('Basic ')) {
    const decoded = Buffer.from(authHeader.slice(6), 'base64').toString();
    const [user, pass] = decoded.split(':');
    if (user === BASIC_USER && pass === BASIC_PASS) {
      if (!_instanceClientId) {
        return res.status(500).json({ error: 'Instance not configured' });
      }
      req.ingestCtx = { clientId: _instanceClientId, keyId: null, label: 'browser' };
      return next();
    }
  }

  // Fall back to API key
  const raw = req.headers['x-api-key'] || authHeader.replace(/^Bearer\s+/i, '') || null;
  const ctx = await resolveApiKey(raw);
  if (!ctx) {
    res.set('WWW-Authenticate', 'Basic realm="DDNL Analytics"');
    return res.status(401).json({ error: 'Invalid or missing credentials' });
  }
  req.ingestCtx = ctx;
  next();
}

async function getDatasetDef(clientId, datasetName) {
  const { rows } = await pool.query(
    `SELECT dd.*, dsv.fields
     FROM dataset_definitions dd
     LEFT JOIN dataset_schema_versions dsv
       ON dsv.dataset_id = dd.id AND dsv.version = dd.current_version
     WHERE dd.client_id = $1 AND dd.name = $2 AND dd.is_active = TRUE`,
    [clientId, datasetName]
  );
  return rows[0] || null;
}

/**
 * Build a deterministic ES _id from primary key fields.
 * Format: sha256(clientId + "|" + dataset + "|" + val1 + "|" + val2 ...)
 * Falls back to auto-id if primary_key_fields is empty or values are missing.
 */
function buildDocId(clientId, datasetName, doc, pkFields) {
  if (!pkFields || !pkFields.length) return null;
  const vals = pkFields.map(f => String(doc[f] ?? ''));
  if (vals.some(v => v === '')) return null;
  const raw = [clientId, datasetName, ...vals].join('|');
  return crypto.createHash('sha256').update(raw).digest('hex');
}

/**
 * Apply field map transform to a single doc.
 * fieldMap: { "source_field": "dest_field", ... }
 * Unmapped fields are passed through as-is.
 * A dest of null/empty means drop the field.
 */
function applyFieldMap(doc, fieldMap) {
  if (!fieldMap || !Object.keys(fieldMap).length) return { ...doc };
  const out = {};
  for (const [k, v] of Object.entries(doc)) {
    const dest = fieldMap[k];
    if (dest === null || dest === '') continue; // drop field
    if (dest)  out[dest] = v;                  // rename
    else       out[k]    = v;                  // pass-through (not in map)
  }
  // Pass-through fields not mentioned in the map at all
  for (const [k, v] of Object.entries(doc)) {
    if (!(k in fieldMap)) out[k] = v;
  }
  return out;
}

/**
 * Verify inbound webhook request.
 * Returns { ok: true } or { ok: false, reason: string }
 */
function verifyWebhook(req, webhook) {
  // 1. Check custom headers first (must all match)
  const customHeaders = webhook.custom_headers || [];
  for (const h of customHeaders) {
    const actual = req.headers[h.key.toLowerCase()];
    if (actual !== h.value) {
      return { ok: false, reason: `Custom header mismatch: ${h.key}` };
    }
  }

  // 2. HTTP Authorization header
  if (webhook.auth_header) {
    const actual = req.headers['authorization'];
    if (actual !== webhook.auth_header) {
      return { ok: false, reason: 'Authorization header mismatch' };
    }
  }

  // 3. HMAC SHA-256 signature
  if (webhook.hmac_secret) {
    const sigHeader = req.headers['x-webhook-signature'] || req.headers['x-hub-signature-256'];
    if (!sigHeader) return { ok: false, reason: 'Missing HMAC signature header' };
    const body   = req.rawBody || JSON.stringify(req.body);
    const expected = 'sha256=' + crypto.createHmac('sha256', webhook.hmac_secret)
      .update(body).digest('hex');
    const safe = Buffer.from(expected);
    const given = Buffer.from(sigHeader.length === expected.length ? sigHeader : expected); // length-safe
    if (!crypto.timingSafeEqual(safe, Buffer.from(sigHeader.padEnd(expected.length, '\0').slice(0, expected.length)))) {
      // do a straightforward compare (timing-safe would require same length)
      if (sigHeader !== expected) return { ok: false, reason: 'HMAC signature mismatch' };
    }
  }

  return { ok: true };
}

// ── Raw body capture (needed for HMAC) ───────────────────────────────────────
// Mount this before express.json() on webhook routes
function captureRawBody(req, res, next) {
  let data = '';
  req.setEncoding('utf8');
  req.on('data', chunk => { data += chunk; });
  req.on('end', () => {
    req.rawBody = data;
    try { req.body = JSON.parse(data); } catch { req.body = {}; }
    next();
  });
}

async function processWebhookPayload(webhook, clientId, payload, t0) {
  let recordsIn = 0, recordsUpserted = 0, recordsFailed = 0, errorMsg = null;

  try {
    // Normalize payload — could be a single doc or array
    const rawDocs = Array.isArray(payload) ? payload
      : payload.data && Array.isArray(payload.data) ? payload.data
      : payload.records && Array.isArray(payload.records) ? payload.records
      : [payload];

    recordsIn = rawDocs.length;

    // Detect event type from payload
    const eventType = payload.event || payload.event_type || payload.type || null;

    // Check event trigger filter
    const triggers = webhook.event_triggers || {};
    if (eventType && Object.keys(triggers).length) {
      if (!triggers[eventType]) {
        await logDelivery(webhook.id, clientId, 'skipped', eventType, recordsIn, 0, 0,
          `Event type '${eventType}' not in triggers`, Date.now() - t0,
          rawDocs.slice(0, 3));
        return;
      }
    }

    // Apply field map transform
    const fieldMap = webhook.field_map || {};
    const docs = rawDocs.map(d => applyFieldMap(d, fieldMap));

    // Build deterministic IDs from primary key fields
    const pkFields = webhook.primary_key_fields || [];
    const docsWithIds = docs.map(doc => {
      const docId = buildDocId(clientId, webhook.dataset_name, doc, pkFields);
      return { doc, docId };
    });

    // Upsert into ES
    const alias = es.aliasName(clientId, webhook.dataset_name);
    const now   = new Date().toISOString();
    const operations = [];

    for (const { doc, docId } of docsWithIds) {
      const enriched = { ...doc, __instance_id: clientId, __ingested_at: now, __source: 'webhook' };
      if (docId) {
        operations.push({ index: { _index: alias, _id: docId } });
      } else {
        operations.push({ index: { _index: alias } });
      }
      operations.push(enriched);
    }

    if (operations.length) {
      const esClient = es.getClient();
      const result   = await esClient.bulk({ operations, refresh: false });
      const items    = result.items || [];
      recordsUpserted = items.filter(i => !i.index?.error).length;
      recordsFailed   = items.filter(i =>  i.index?.error).length;
      if (recordsFailed > 0) {
        const errs = items.filter(i => i.index?.error).map(i => i.index.error.reason).slice(0, 3);
        errorMsg = errs.join('; ');
      }
    }

    await logDelivery(webhook.id, clientId, 'processed', eventType, recordsIn,
      recordsUpserted, recordsFailed, errorMsg, Date.now() - t0, rawDocs.slice(0, 3));

  } catch (err) {
    await logDelivery(webhook.id, clientId, 'failed', null, recordsIn,
      0, recordsIn, err.message, Date.now() - t0, null);
  }
}

async function logDelivery(webhookId, clientId, status, eventType, recordsIn,
  recordsUpserted, recordsFailed, errorMessage, durationMs, payloadPreview) {
  try {
    await pool.query(
      `INSERT INTO webhook_delivery_log
         (webhook_id, client_id, status, event_type, records_in, records_upserted,
          records_failed, error_message, duration_ms, payload_preview)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
      [webhookId, clientId, status, eventType, recordsIn, recordsUpserted,
       recordsFailed, errorMessage, durationMs,
       payloadPreview ? JSON.stringify(payloadPreview) : null]
    );
  } catch (e) {
    console.error('[webhook:logDelivery]', e.message);
  }
}

// ── Admin CRUD routes (API-key protected) ─────────────────────────────────────

// GET /api/ingest/admin/webhooks?dataset=sales
router.get('/admin/webhooks', requireBasicOrApiKey, async (req, res) => {
  const { clientId } = req.ingestCtx;
  const { dataset } = req.query;

  let query = `
    SELECT dw.*, dd.name AS dataset_name, dd.label AS dataset_label,
           dd.primary_key_fields, dd.event_types
    FROM dataset_webhooks dw
    JOIN dataset_definitions dd ON dd.id = dw.dataset_id
    WHERE dw.client_id = $1`;
  const params = [clientId];

  if (dataset) {
    query += ` AND dd.name = $2`;
    params.push(dataset);
  }
  query += ` ORDER BY dw.created_at DESC`;

  const { rows } = await pool.query(query, params);
  res.json({ success: true, webhooks: rows });
});

// POST /api/ingest/admin/webhooks
// Body: { dataset, name, endpoint_url, event_triggers, field_map, auth_header, hmac_secret, custom_headers, enabled }
router.post('/admin/webhooks', requireBasicOrApiKey, async (req, res) => {
  const { clientId } = req.ingestCtx;
  const {
    dataset, name, endpoint_url,
    event_triggers = {}, field_map = {},
    auth_header = null, hmac_secret = null,
    custom_headers = [], enabled = true
  } = req.body;

  if (!dataset || !name || !endpoint_url) {
    return res.status(400).json({ error: 'dataset, name, and endpoint_url are required' });
  }

  const def = await getDatasetDef(clientId, dataset);
  if (!def) return res.status(404).json({ error: `Dataset '${dataset}' not found` });

  try {
    const { rows: [wh] } = await pool.query(
      `INSERT INTO dataset_webhooks
         (client_id, dataset_id, name, endpoint_url, event_triggers, field_map,
          auth_header, hmac_secret, custom_headers, enabled)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING *`,
      [clientId, def.id, name, endpoint_url,
       JSON.stringify(event_triggers), JSON.stringify(field_map),
       auth_header, hmac_secret, JSON.stringify(custom_headers), enabled]
    );
    res.json({ success: true, webhook: wh,
      receiver_url: `/api/webhook/${clientId}/${wh.id}` });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/ingest/admin/webhooks/:id
router.get('/admin/webhooks/:id', requireBasicOrApiKey, async (req, res) => {
  const { clientId } = req.ingestCtx;
  const { rows: [wh] } = await pool.query(
    `SELECT dw.*, dd.name AS dataset_name, dd.label AS dataset_label,
            dd.primary_key_fields, dd.event_types
     FROM dataset_webhooks dw
     JOIN dataset_definitions dd ON dd.id = dw.dataset_id
     WHERE dw.id = $1 AND dw.client_id = $2`,
    [req.params.id, clientId]
  );
  if (!wh) return res.status(404).json({ error: 'Webhook not found' });
  res.json({ success: true, webhook: wh,
    receiver_url: `/api/webhook/${clientId}/${wh.id}` });
});

// PUT /api/ingest/admin/webhooks/:id
router.put('/admin/webhooks/:id', requireBasicOrApiKey, async (req, res) => {
  const { clientId } = req.ingestCtx;
  const {
    name, endpoint_url, event_triggers, field_map,
    auth_header, hmac_secret, custom_headers, enabled
  } = req.body;

  const { rows: [existing] } = await pool.query(
    `SELECT * FROM dataset_webhooks WHERE id=$1 AND client_id=$2`,
    [req.params.id, clientId]
  );
  if (!existing) return res.status(404).json({ error: 'Webhook not found' });

  const updated = {
    name:            name            ?? existing.name,
    endpoint_url:    endpoint_url    ?? existing.endpoint_url,
    event_triggers:  event_triggers  ?? existing.event_triggers,
    field_map:       field_map       ?? existing.field_map,
    auth_header:     auth_header     !== undefined ? auth_header     : existing.auth_header,
    hmac_secret:     hmac_secret     !== undefined ? hmac_secret     : existing.hmac_secret,
    custom_headers:  custom_headers  ?? existing.custom_headers,
    enabled:         enabled         !== undefined ? enabled         : existing.enabled,
  };

  try {
    const { rows: [wh] } = await pool.query(
      `UPDATE dataset_webhooks SET
         name=$1, endpoint_url=$2, event_triggers=$3, field_map=$4,
         auth_header=$5, hmac_secret=$6, custom_headers=$7, enabled=$8, updated_at=NOW()
       WHERE id=$9 AND client_id=$10 RETURNING *`,
      [updated.name, updated.endpoint_url,
       JSON.stringify(updated.event_triggers), JSON.stringify(updated.field_map),
       updated.auth_header, updated.hmac_secret,
       JSON.stringify(updated.custom_headers), updated.enabled,
       req.params.id, clientId]
    );
    res.json({ success: true, webhook: wh });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// DELETE /api/ingest/admin/webhooks/:id
router.delete('/admin/webhooks/:id', requireBasicOrApiKey, async (req, res) => {
  const { clientId } = req.ingestCtx;
  const { rowCount } = await pool.query(
    `DELETE FROM dataset_webhooks WHERE id=$1 AND client_id=$2`,
    [req.params.id, clientId]
  );
  if (!rowCount) return res.status(404).json({ error: 'Webhook not found' });
  res.json({ success: true });
});

// GET /api/ingest/admin/webhooks/:id/logs?limit=50&status=failed
router.get('/admin/webhooks/:id/logs', requireBasicOrApiKey, async (req, res) => {
  const { clientId } = req.ingestCtx;
  const limit  = Math.min(parseInt(req.query.limit  || '50'), 200);
  const status = req.query.status || null;

  // Verify ownership
  const { rows: [wh] } = await pool.query(
    `SELECT id FROM dataset_webhooks WHERE id=$1 AND client_id=$2`,
    [req.params.id, clientId]
  );
  if (!wh) return res.status(404).json({ error: 'Webhook not found' });

  let q = `SELECT * FROM webhook_delivery_log WHERE webhook_id=$1`;
  const params = [req.params.id];
  if (status) { q += ` AND status=$2`; params.push(status); }
  q += ` ORDER BY received_at DESC LIMIT ${limit}`;

  const { rows } = await pool.query(q, params);
  res.json({ success: true, logs: rows });
});

// POST /api/ingest/admin/webhooks/:id/test
// Sends a synthetic test payload through the full pipeline
router.post('/admin/webhooks/:id/test', requireBasicOrApiKey, async (req, res) => {
  const { clientId } = req.ingestCtx;
  const { rows: [webhook] } = await pool.query(
    `SELECT dw.*, dd.name AS dataset_name, dd.primary_key_fields, dd.event_types, dd.fields AS schema_fields
     FROM dataset_webhooks dw
     JOIN dataset_definitions dd ON dd.id = dw.dataset_id
     LEFT JOIN dataset_schema_versions dsv ON dsv.dataset_id = dd.id AND dsv.version = dd.current_version
     WHERE dw.id = $1 AND dw.client_id = $2`,
    [req.params.id, clientId]
  );
  if (!webhook) return res.status(404).json({ error: 'Webhook not found' });

  const testPayload = req.body.payload || { __test: true, __webhook_id: webhook.id };
  const t0 = Date.now();

  await processWebhookPayload(webhook, clientId, testPayload, t0);
  res.json({ success: true, message: 'Test payload processed — check delivery logs' });
});

// ── Update upsert endpoint: GET primary_key_fields and use bulkUpsert ─────────
// PATCH /api/ingest/admin/datasets/:name/primary-key
// Body: { primary_key_fields: ["field1", "field2"], event_types: ["created","updated"] }
router.patch('/admin/datasets/:name/primary-key', requireBasicOrApiKey, async (req, res) => {
  const { clientId } = req.ingestCtx;
  const { primary_key_fields = [], event_types = [] } = req.body;

  const { rows: [def] } = await pool.query(
    `SELECT * FROM dataset_definitions WHERE client_id=$1 AND name=$2`, [clientId, req.params.name]
  );
  if (!def) return res.status(404).json({ error: 'Dataset not found' });

  await pool.query(
    `UPDATE dataset_definitions SET primary_key_fields=$1, event_types=$2, updated_at=NOW() WHERE id=$3`,
    [JSON.stringify(primary_key_fields), JSON.stringify(event_types), def.id]
  );

  res.json({ success: true, primary_key_fields, event_types });
});

// ── Inbound webhook receiver ──────────────────────────────────────────────────
// POST /api/webhook/:client_id/:webhook_id
// Defined LAST so /admin/... routes always take priority on the /api/ingest mount.
router.post('/:client_id/:webhook_id', captureRawBody, async (req, res) => {
  const { client_id, webhook_id } = req.params;
  const t0 = Date.now();

  // Load webhook config
  const { rows: [webhook] } = await pool.query(
    `SELECT dw.*, dd.name AS dataset_name, dd.primary_key_fields, dd.event_types
     FROM dataset_webhooks dw
     JOIN dataset_definitions dd ON dd.id = dw.dataset_id
     WHERE dw.id = $1 AND dw.client_id = $2`,
    [webhook_id, client_id]
  );

  if (!webhook) return res.status(404).json({ error: 'Webhook not found' });
  if (!webhook.enabled) return res.status(410).json({ error: 'Webhook disabled' });

  // Verify security
  const verification = verifyWebhook(req, webhook);
  if (!verification.ok) {
    await logDelivery(webhook.id, client_id, 'rejected', null, 0, 0, 0, verification.reason, Date.now() - t0, null);
    return res.status(401).json({ error: verification.reason });
  }

  // Acknowledge immediately — process async
  res.json({ success: true, message: 'Received' });

  // Process in background
  processWebhookPayload(webhook, client_id, req.body, t0).catch(err => {
    console.error(`[webhook:${webhook_id}] processing error:`, err.message);
  });
});

module.exports = router;
