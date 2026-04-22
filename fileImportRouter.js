'use strict';
/**
 * fileImportRouter.js — Dataset from File
 *
 * Routes (all under /api/ingest, protected by requireBasicOrApiKey):
 *
 *   POST /admin/datasets/parse-file
 *     Upload + parse a file. Returns column schema + preview rows.
 *     No data is written anywhere — purely analytical.
 *
 *   POST /admin/datasets/:name/define
 *     Save a dataset schema (fields, PK) to Postgres.
 *     Creates dataset_versions + dataset_definitions rows.
 *     Does NOT ingest any data. Idempotent — updates if exists.
 *
 *   POST /admin/datasets/:name/import-file
 *     Ingest all rows from an uploaded file into an EXISTING dataset.
 *     Dataset must already exist (created via /define or otherwise).
 *     Uses deterministic SHA-256 _id from PK fields for upsert.
 */

const express = require('express');
const multer  = require('multer');
const XLSX    = require('xlsx');
const { parse: parseCsv } = require('csv-parse/sync');
const crypto  = require('crypto');
const { pool } = require('./db');
const es       = require('./esClient');

const router  = express.Router();
const upload  = multer({ storage: multer.memoryStorage(), limits: { fileSize: 50 * 1024 * 1024 } }); // 50 MB

// ── Auth + ClientId — injected from server.js after INSTANCE is available ────
let _requireAuth = (req, res, next) => next();
let _clientId    = null;

router.setAuth       = (mw) => { _requireAuth = mw; };
router.setClientId   = (id) => { _clientId    = id; };

const authProxy    = (req, res, next) => _requireAuth(req, res, next);
const getClientId  = ()               => _clientId;

// ── Helpers ───────────────────────────────────────────────────────────────────

/** Infer field type from a sample of values. */
function inferType(samples) {
  const nonNull = samples.filter(v => v !== null && v !== undefined && v !== '');
  if (!nonNull.length) return 'text';

  const total    = nonNull.length;
  const numHits  = nonNull.filter(v => !isNaN(Number(v)) && String(v).trim() !== '').length;
  const boolHits = nonNull.filter(v => ['true','false','yes','no','1','0'].includes(String(v).toLowerCase())).length;
  const dateRe   = /^\d{4}-\d{2}-\d{2}|^\d{1,2}\/\d{1,2}\/\d{2,4}|^\d{1,2}-[a-z]{3}-\d{2,4}/i;
  const dateHits = nonNull.filter(v => dateRe.test(String(v))).length;

  if (numHits  / total > 0.85) return 'number';
  if (dateHits / total > 0.85) return 'date';
  if (boolHits / total > 0.85) return 'boolean';
  return 'text';
}

/** Coerce a raw string value to the inferred type for ingest. */
function coerce(val, type) {
  if (val === null || val === undefined || val === '') return null;
  if (type === 'number')  return isNaN(Number(val)) ? val : Number(val);
  if (type === 'boolean') return ['true','yes','1'].includes(String(val).toLowerCase());
  if (type === 'date')    return val; // keep as string — ES maps as date
  return String(val);
}

/** Slugify a column header to a valid field name. */
function toFieldName(header) {
  return String(header)
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
    || 'field_' + Math.random().toString(36).slice(2, 6);
}

/** Parse buffer → array of row objects based on file extension. */
function parseBuffer(buffer, filename) {
  const ext = filename.split('.').pop().toLowerCase();

  if (['csv', 'tsv', 'txt'].includes(ext)) {
    return parseCsv(buffer.toString('utf8'), {
      columns:          true,
      skip_empty_lines: true,
      trim:             true,
      relax_quotes:     true,
      delimiter:        ext === 'tsv' ? '\t' : undefined,
    });
  }

  if (['xlsx', 'xls', 'ods'].includes(ext)) {
    const wb = XLSX.read(buffer, { type: 'buffer', cellDates: true });
    const ws = wb.Sheets[wb.SheetNames[0]];
    return XLSX.utils.sheet_to_json(ws, { defval: null, raw: false });
  }

  if (ext === 'json') {
    const parsed = JSON.parse(buffer.toString('utf8'));
    return Array.isArray(parsed) ? parsed : [parsed];
  }

  throw new Error(`Unsupported file type: .${ext}`);
}

// ── POST /admin/datasets/parse-file ──────────────────────────────────────────
// Upload + parse only. Returns column schema + preview. Writes nothing to DB/ES.

router.post('/admin/datasets/parse-file', authProxy, upload.single('file'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'No file uploaded' });

    const rows = parseBuffer(req.file.buffer, req.file.originalname);
    if (!rows.length) return res.status(400).json({ error: 'File is empty or has no rows' });

    // Collect all column keys from first 20 rows (handles sparse sheets)
    const keySet = new Set();
    rows.slice(0, 20).forEach(r => Object.keys(r).forEach(k => keySet.add(k)));
    const keys = Array.from(keySet);

    const columns = keys.map(key => {
      const samples = rows.slice(0, 200).map(r => r[key]);
      const type    = inferType(samples);
      const nonNull = samples.filter(v => v !== null && v !== undefined && v !== '');
      return {
        source_name: key,
        name:        toFieldName(key),
        label:       String(key).trim(),
        type,
        nullable:    nonNull.length < samples.length,
        sample:      nonNull.slice(0, 3),
      };
    });

    const preview = rows.slice(0, 10).map(r => {
      const out = {};
      columns.forEach(col => { out[col.name] = coerce(r[col.source_name], col.type); });
      return out;
    });

    res.json({
      success:    true,
      filename:   req.file.originalname,
      total_rows: rows.length,
      columns,
      preview,
    });

    console.log(`[file-import] parse-file: ${req.file.originalname} → ${rows.length} rows, ${columns.length} columns`);
  } catch (e) {
    console.error('[file-import] parse-file error:', e.message);
    res.status(400).json({ error: e.message });
  }
});

// ── POST /admin/datasets/:name/define ─────────────────────────────────────────
// Save dataset schema to Postgres. No data ingested.
// Body (JSON): { label, fields: [{ name, label, type, order, required }], primary_key_fields: [] }

router.post('/admin/datasets/:name/define', authProxy, express.json(), async (req, res) => {
  const clientId = getClientId();
  if (!clientId) return res.status(500).json({ error: 'Server misconfiguration: clientId not set' });

  const dsName = req.params.name;
  const { label, fields = [], primary_key_fields = [], status_source_field = null } = req.body || {};

  if (!dsName)        return res.status(400).json({ error: 'Dataset name is required' });
  if (!fields.length) return res.status(400).json({ error: 'At least one field is required' });

  try {
    const existing = await pool.query(
      `SELECT id, current_version FROM dataset_definitions WHERE client_id=$1 AND name=$2`,
      [clientId, dsName]
    ).then(r => r.rows[0]);

    const dsLabel = label || (dsName.charAt(0).toUpperCase() + dsName.slice(1).replace(/_/g, ' '));

    if (existing) {
      // Update: bump version, update fields + pk + status mapping
      const newVer = (existing.current_version || 1) + 1;
      await pool.query(
        `INSERT INTO dataset_versions (client_id, name, label, version, fields, created_at)
         VALUES ($1,$2,$3,$4,$5,NOW())`,
        [clientId, dsName, dsLabel, newVer, JSON.stringify(fields)]
      );
      await pool.query(
        `UPDATE dataset_definitions
         SET label=$1, current_version=$2, primary_key_fields=$3, status_source_field=$4, updated_at=NOW()
         WHERE id=$5`,
        [dsLabel, newVer, JSON.stringify(primary_key_fields), status_source_field, existing.id]
      );
      console.log(`[file-import] define: updated dataset '${dsName}' (v${newVer}) for client '${clientId}'`);
      return res.json({ success: true, dataset: dsName, action: 'updated', version: newVer });
    }

    // Create new
    await pool.query(
      `INSERT INTO dataset_versions (client_id, name, label, version, fields, created_at)
       VALUES ($1,$2,$3,1,$4,NOW())`,
      [clientId, dsName, dsLabel, JSON.stringify(fields)]
    );

    await pool.query(
      `INSERT INTO dataset_definitions
         (client_id, name, label, current_version, is_active, dataset_type, primary_key_fields,
          status_source_field, created_at, updated_at)
       VALUES ($1,$2,$3,1,true,'client',$4,$5,NOW(),NOW())`,
      [clientId, dsName, dsLabel, JSON.stringify(primary_key_fields), status_source_field]
    );

    console.log(`[file-import] define: created dataset '${dsName}' for client '${clientId}'`);
    res.json({ success: true, dataset: dsName, action: 'created', version: 1 });
  } catch (e) {
    console.error('[file-import] define error:', e);
    res.status(500).json({ error: e.message || String(e) || 'Unknown error' });
  }
});

// ── POST /admin/datasets/:name/import-file ────────────────────────────────────
// Ingest all rows from an uploaded file into an EXISTING dataset.
// Dataset must exist — create it first via /define.
// Body: multipart — file + column_map (JSON string) + primary_key_fields (JSON string)

router.post('/admin/datasets/:name/import-file', authProxy, upload.single('file'), async (req, res) => {
  const clientId = getClientId();
  if (!clientId) return res.status(500).json({ error: 'Server misconfiguration: clientId not set' });

  const dsName = req.params.name;
  if (!req.file) return res.status(400).json({ error: 'No file uploaded' });

  let colMap, pkFields;
  try {
    colMap   = JSON.parse(req.body.column_map        || '[]');
    pkFields = JSON.parse(req.body.primary_key_fields || '[]');
  } catch (e) {
    return res.status(400).json({ error: 'Invalid JSON in column_map or primary_key_fields' });
  }

  const includedCols = colMap.filter(c => c.include !== false);
  if (!includedCols.length) return res.status(400).json({ error: 'No columns selected' });

  try {
    // Dataset must exist — fetch status_source_field too
    const dsRow = await pool.query(
      `SELECT id, status_source_field FROM dataset_definitions WHERE client_id=$1 AND name=$2`,
      [clientId, dsName]
    ).then(r => r.rows[0]);

    if (!dsRow) {
      return res.status(404).json({
        error: `Dataset '${dsName}' not found. Save the dataset definition first.`,
      });
    }

    // Resolve status mapping: use field from body if provided, else fall back to saved definition
    const statusSourceField = req.body.status_source_field || dsRow.status_source_field || null;

    // Parse rows from file
    const rows   = parseBuffer(req.file.buffer, req.file.originalname);
    const alias  = `${clientId}__${dsName}`;
    const index  = `${alias}__v1`;
    const hasPk  = pkFields.length > 0;

    console.log(`[file-import] import-file: '${dsName}' for '${clientId}' — ${rows.length} rows, index=${index}`);

    // Bulk upsert in batches of 500
    let upserted = 0, failed = 0;
    const BATCH  = 500;

    for (let i = 0; i < rows.length; i += BATCH) {
      const batch = rows.slice(i, i + BATCH);
      const ops   = [];

      for (const row of batch) {
        const doc = {};
        includedCols.forEach(col => {
          doc[col.name] = coerce(row[col.source_name], col.type);
        });
        doc.__instance_id = clientId;
        doc.__ingested_at = new Date().toISOString();
        // Apply _status: map from source column if configured, then default to 'active'
        if (statusSourceField && row[statusSourceField] !== undefined) {
          const raw = String(row[statusSourceField] || '').toLowerCase().trim();
          doc._status = ['deleted','inactive','false','0','no'].includes(raw) ? 'deleted'
                      : ['archived'].includes(raw)                            ? 'archived'
                      : 'active';
        } else {
          es.applyStatusDefault(doc);
        }

        const meta = hasPk
          ? { index: { _index: index, _id: crypto.createHash('sha256')
              .update(`${clientId}|${dsName}|${pkFields.map(f => String(doc[f] ?? '')).join('|')}`)
              .digest('hex') } }
          : { index: { _index: index } };

        ops.push(meta, doc);
      }

      const esClient = es.getClient();
      const bulkRes  = await esClient.bulk({ operations: ops, refresh: false });
      if (bulkRes.errors) {
        bulkRes.items.forEach(item => {
          if (item.index?.error) {
            failed++;
            if (failed <= 3) console.warn('[file-import] bulk error:', JSON.stringify(item.index.error));
          } else {
            upserted++;
          }
        });
      } else {
        upserted += batch.length;
      }
    }

    console.log(`[file-import] import-file done: upserted=${upserted} failed=${failed}`);
    res.json({ success: true, dataset: dsName, total_rows: rows.length, upserted, failed });

  } catch (e) {
    console.error('[file-import] import-file error:', e);
    res.status(500).json({ error: e.message || String(e) || 'Unknown error' });
  }
});

module.exports = router;
