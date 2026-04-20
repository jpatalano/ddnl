/**
 * ingestRouter.js — Dataset ingest API
 *
 * All routes are authenticated by instance API key.
 * Header: X-Api-Key: <raw_key>
 * The instance (client_id) is resolved from the key — no need to pass it separately.
 *
 * Routes:
 *   POST /api/ingest/:dataset/bulk      — push N docs (replace=false appends, replace=true full swap)
 *   POST /api/ingest/:dataset/single    — push one doc
 *   POST /api/ingest/:dataset/reindex   — rebuild ES index from current schema (clears + re-creates)
 *   GET  /api/ingest/:dataset/status    — index health + last ingest log entry
 *
 * Admin routes (also API-key protected, role must be admin):
 *   GET  /api/admin/datasets                        — list all dataset definitions for instance
 *   POST /api/admin/datasets                        — create dataset + initial schema version
 *   GET  /api/admin/datasets/:name                  — get definition + all schema versions
 *   POST /api/admin/datasets/:name/publish          — publish a new schema version (runs compat check)
 *   POST /api/admin/datasets/:name/reindex          — reindex against new schema version
 *   POST /api/admin/api-keys                        — generate new API key for instance
 *   GET  /api/admin/api-keys                        — list keys (hashed, no raw)
 *   DELETE /api/admin/api-keys/:id                  — revoke key
 */

const express    = require('express');
const crypto     = require('crypto');
const multer     = require('multer');
const { parse: csvParse } = require('csv-parse/sync');
const { pool }   = require('./db');
const es         = require('./esClient');

const router = express.Router();

// ── Key helpers ────────────────────────────────────────────────────────────────

function hashKey(rawKey) {
  return crypto.createHash('sha256').update(rawKey).digest('hex');
}

function generateKey() {
  return 'ik_' + crypto.randomBytes(32).toString('hex');
}

/**
 * Resolve instance from API key.
 * Returns { clientId, keyId, label } or null.
 */
async function resolveApiKey(rawKey) {
  if (!rawKey) return null;
  const hash = hashKey(rawKey);
  const { rows } = await pool.query(
    `SELECT id, client_id, label FROM instance_api_keys
     WHERE key_hash = $1 AND revoked = FALSE`,
    [hash]
  );
  if (!rows.length) return null;
  // Update last_used_at async (don't await)
  pool.query('UPDATE instance_api_keys SET last_used_at = NOW() WHERE id = $1', [rows[0].id]).catch(() => {});
  return { clientId: rows[0].client_id, keyId: rows[0].id, label: rows[0].label };
}

// ── Auth middleware ────────────────────────────────────────────────────────────

async function requireApiKey(req, res, next) {
  const raw = req.headers['x-api-key'];
  const ctx = await resolveApiKey(raw);
  if (!ctx) return res.status(401).json({ error: 'Invalid or missing API key' });
  req.ingestCtx = ctx;  // { clientId, keyId, label }
  next();
}

// ── Dataset helpers ────────────────────────────────────────────────────────────

async function getDatasetDef(clientId, datasetName) {
  const { rows } = await pool.query(
    `SELECT dd.*, dsv.fields, dsv.es_index, dsv.version as schema_version
     FROM dataset_definitions dd
     JOIN dataset_schema_versions dsv
       ON dsv.dataset_id = dd.id AND dsv.version = dd.current_version
     WHERE dd.client_id = $1 AND dd.name = $2 AND dd.is_active = TRUE`,
    [clientId, datasetName]
  );
  return rows[0] || null;
}

async function logIngest(clientId, datasetName, operation, result, triggeredBy, durationMs) {
  await pool.query(
    `INSERT INTO ingest_log (client_id, dataset_name, operation, doc_count, failed_count, errors, duration_ms, triggered_by)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
    [clientId, datasetName, operation,
     result.indexed || 0, result.failed || 0,
     JSON.stringify(result.errors || []),
     durationMs, triggeredBy]
  );
}

// ── Compat checker ─────────────────────────────────────────────────────────────

/**
 * Compare two field arrays and return { status, notes, breaking }.
 * status: 'ok' | 'additive' | 'breaking'
 */
function checkCompat(oldFields, newFields) {
  const notes = [];
  let breaking = false;

  const oldMap = Object.fromEntries(oldFields.map(f => [f.name, f]));
  const newMap = Object.fromEntries(newFields.map(f => [f.name, f]));

  // Removed fields
  for (const [name, f] of Object.entries(oldMap)) {
    if (!newMap[name]) {
      notes.push({ type: 'removed', field: name, fieldType: f.fieldType });
      breaking = true;
    }
  }

  // Type changes
  for (const [name, nf] of Object.entries(newMap)) {
    const of_ = oldMap[name];
    if (of_ && of_.segmentType !== nf.segmentType) {
      notes.push({ type: 'type_changed', field: name, from: of_.segmentType, to: nf.segmentType });
      breaking = true;
    }
  }

  // Added fields (additive — fine)
  for (const name of Object.keys(newMap)) {
    if (!oldMap[name]) {
      notes.push({ type: 'added', field: name });
    }
  }

  return {
    status:   breaking ? 'breaking' : notes.length ? 'additive' : 'ok',
    breaking,
    notes
  };
}

/**
 * Find all saved reports, charts, dashboard tiles that reference fields
 * from the old schema that are now removed/changed. Insert schema_invalidations rows.
 */
async function runInvalidationCheck(clientId, datasetId, datasetName, fromVersion, toVersion, compatNotes) {
  const breakingNotes = compatNotes.filter(n => n.type === 'removed' || n.type === 'type_changed');
  if (!breakingNotes.length) return [];

  const invalidations = [];

  // Check saved_reports
  const { rows: reports } = await pool.query(
    `SELECT id, name, config FROM saved_reports WHERE client_id = $1`, [clientId]
  );
  for (const r of reports) {
    const cfg = r.config || {};
    const usedFields = extractFieldsFromReportConfig(cfg);
    for (const note of breakingNotes) {
      if (usedFields.includes(note.field)) {
        invalidations.push({ resourceType: 'report', resourceId: r.id, resourceName: r.name, fieldName: note.field, changeType: note.type });
      }
    }
  }

  // Check saved_charts
  const { rows: charts } = await pool.query(
    `SELECT id, name, config FROM saved_charts WHERE client_id = $1 AND dataset = $2`, [clientId, datasetName]
  );
  for (const c of charts) {
    const cfg = c.config || {};
    const usedFields = [
      ...(cfg.groupBySegments || []),
      ...(cfg.metrics || []).map(m => m.name || m.metricName),
      cfg.labelCol, cfg.valueCol, cfg.groupCol, cfg.dateSegment
    ].filter(Boolean);
    for (const note of breakingNotes) {
      if (usedFields.includes(note.field)) {
        invalidations.push({ resourceType: 'chart', resourceId: c.id, resourceName: c.name, fieldName: note.field, changeType: note.type });
      }
    }
  }

  // Persist invalidations
  for (const inv of invalidations) {
    await pool.query(
      `INSERT INTO schema_invalidations
       (dataset_id, from_version, to_version, resource_type, resource_id, resource_name, field_name, change_type)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
      [datasetId, fromVersion, toVersion, inv.resourceType, inv.resourceId, inv.resourceName, inv.fieldName, inv.changeType]
    );
  }

  return invalidations;
}

function extractFieldsFromReportConfig(cfg) {
  const fields = [];
  if (cfg.groupBySegments) fields.push(...cfg.groupBySegments);
  if (cfg.metrics) fields.push(...cfg.metrics.map(m => m.metricName || m.name).filter(Boolean));
  if (cfg.columns) fields.push(...cfg.columns.map(c => c.field || c.name).filter(Boolean));
  if (cfg.filters) fields.push(...cfg.filters.map(f => f.segmentName || f.field).filter(Boolean));
  if (cfg.orderBy) fields.push(...cfg.orderBy.map(o => o.field).filter(Boolean));
  return [...new Set(fields)];
}

// ── Lookup auto-maintenance ───────────────────────────────────────────────────

/**
 * After any ingest, extract distinct values for fields that are wired to a lookup
 * dataset (via dataset_field_metadata.lookup_dataset_id) and upsert them as
 * auto_populated=TRUE rows.
 *
 * Merge strategy (null-safe):
 *   - New key never seen before  → INSERT full data, auto_populated=TRUE
 *   - Key exists, auto_populated=TRUE → UPDATE data (ingest owns it)
 *   - Key exists, auto_populated=FALSE → UPDATE only NULL fields (Designer owns it)
 *
 * This runs async after the ES index call — ingest response is never blocked.
 */
async function syncLookupFromDocs(clientId, datasetName, datasetId, docs) {
  try {
    // Find all fields on this dataset that have a lookup relationship
    const { rows: metaRows } = await pool.query(
      `SELECT fm.field_name, fm.lookup_dataset_id, fm.lookup_key_field,
              ld.key_field as lookup_key, ld.id as ld_id
       FROM dataset_field_metadata fm
       JOIN lookup_datasets ld ON ld.id = fm.lookup_dataset_id
       WHERE fm.dataset_id = $1 AND fm.lookup_dataset_id IS NOT NULL`,
      [datasetId]
    );
    if (!metaRows.length) return;  // no lookup relationships on this dataset

    for (const meta of metaRows) {
      const keyField    = meta.field_name;          // e.g. 'store_id' in the doc
      const lookupId    = meta.ld_id;

      // Get all fields defined on this lookup dataset so we know what to extract
      const { rows: lookupFields } = await pool.query(
        `SELECT field_name, is_key_field FROM lookup_dataset_fields
         WHERE lookup_dataset_id = $1 ORDER BY sort_order`,
        [lookupId]
      );
      const dataFieldNames = lookupFields
        .filter(f => !f.is_key_field)
        .map(f => f.field_name);  // e.g. ['store_name', 'city', 'region', ...]

      // Collect distinct key+data combinations from the incoming docs
      // Last-writer wins within this batch (fine for stable reference data)
      const seen = new Map();  // key_value → data object
      for (const doc of docs) {
        const keyValue = doc[keyField];
        if (!keyValue) continue;
        const data = {};
        for (const f of dataFieldNames) {
          if (doc[f] !== undefined) data[f] = doc[f];
        }
        seen.set(String(keyValue), data);
      }
      if (!seen.size) continue;

      // Upsert each distinct key
      for (const [keyValue, data] of seen) {
        // For auto_populated=FALSE rows, only fill in fields that are currently null
        // We do this with a CASE expression so we never touch Designer-set values
        await pool.query(
          `INSERT INTO lookup_dataset_rows (lookup_dataset_id, key_value, data, auto_populated)
           VALUES ($1, $2, $3, TRUE)
           ON CONFLICT (lookup_dataset_id, key_value) DO UPDATE
             SET data = CASE
                   WHEN lookup_dataset_rows.auto_populated = TRUE
                     THEN $3::jsonb                                         -- ingest owns it, full replace
                   ELSE lookup_dataset_rows.data ||
                        (SELECT jsonb_object_agg(key, value)
                         FROM jsonb_each($3::jsonb)
                         WHERE lookup_dataset_rows.data->key IS NULL)       -- Designer owns it, fill nulls only
                 END,
             updated_at = NOW()
          `,
          [lookupId, keyValue, JSON.stringify(data)]
        );
      }

      console.log(`  [lookup] synced ${seen.size} '${keyField}' keys → lookup_dataset ${lookupId}`);
    }
  } catch (e) {
    // Never fail an ingest because of lookup sync — log and move on
    console.warn(`  [lookup] sync failed for ${clientId}/${datasetName}:`, e.message);
  }
}

// ── Ingest routes ──────────────────────────────────────────────────────────────

// POST /api/ingest/:dataset/bulk
// Body: { docs: [...], replace: false }
router.post('/:dataset/bulk', requireApiKey, async (req, res) => {
  const { clientId, label } = req.ingestCtx;
  const { dataset } = req.params;
  const { docs = [], replace = false } = req.body;
  const t0 = Date.now();

  if (!Array.isArray(docs) || docs.length === 0) {
    return res.status(400).json({ error: 'docs must be a non-empty array' });
  }

  const def = await getDatasetDef(clientId, dataset);
  if (!def) return res.status(404).json({ error: `Dataset '${dataset}' not found for this instance` });

  try {
    const result = replace
      ? await es.replaceAll(clientId, dataset, docs)
      : await es.bulkIndex(clientId, dataset, docs);

    const durationMs = Date.now() - t0;
    await logIngest(clientId, dataset, replace ? 'replace' : 'bulk', result, label, durationMs);

    // Auto-maintain lookup datasets — async, never blocks the response
    syncLookupFromDocs(clientId, dataset, def.id, docs).catch(() => {});

    res.json({ success: true, indexed: result.indexed, failed: result.failed,
               errors: result.errors, durationMs });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /api/ingest/:dataset/single
router.post('/:dataset/single', requireApiKey, async (req, res) => {
  const { clientId, label } = req.ingestCtx;
  const { dataset } = req.params;
  const doc = req.body;
  const t0  = Date.now();

  if (!doc || typeof doc !== 'object' || Array.isArray(doc)) {
    return res.status(400).json({ error: 'Body must be a single document object' });
  }

  const def = await getDatasetDef(clientId, dataset);
  if (!def) return res.status(404).json({ error: `Dataset '${dataset}' not found for this instance` });

  try {
    const result = await es.bulkIndex(clientId, dataset, [doc]);
    const durationMs = Date.now() - t0;
    await logIngest(clientId, dataset, 'single', result, label, durationMs);

    // Auto-maintain lookup datasets — async, never blocks the response
    syncLookupFromDocs(clientId, dataset, def.id, [doc]).catch(() => {});

    res.json({ success: true, ...result, durationMs });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/ingest/:dataset/status
router.get('/:dataset/status', requireApiKey, async (req, res) => {
  const { clientId } = req.ingestCtx;
  const { dataset }  = req.params;

  const def = await getDatasetDef(clientId, dataset);
  if (!def) return res.status(404).json({ error: `Dataset '${dataset}' not found` });

  const stats = await es.indexStats(clientId, dataset);

  const { rows: logs } = await pool.query(
    `SELECT * FROM ingest_log WHERE client_id=$1 AND dataset_name=$2 ORDER BY created_at DESC LIMIT 5`,
    [clientId, dataset]
  );

  res.json({ success: true, dataset, version: def.current_version, esAlias: def.es_alias, stats, recentLogs: logs });
});

// ── Admin routes ───────────────────────────────────────────────────────────────

// GET /api/admin/datasets
router.get('/admin/datasets', requireApiKey, async (req, res) => {
  const { clientId } = req.ingestCtx;
  const { rows } = await pool.query(
    `SELECT dd.*, COUNT(dsv.id) as version_count
     FROM dataset_definitions dd
     LEFT JOIN dataset_schema_versions dsv ON dsv.dataset_id = dd.id
     WHERE dd.client_id = $1
     GROUP BY dd.id ORDER BY dd.created_at DESC`,
    [clientId]
  );
  res.json({ success: true, datasets: rows });
});

// POST /api/admin/datasets
// Body: { name, label, description, fields: [{name, fieldType, segmentType, displayFormat, aggregationType, prefix, suffix, isFilterable, isGroupable}] }
router.post('/admin/datasets', requireApiKey, async (req, res) => {
  const { clientId, label: keyLabel } = req.ingestCtx;
  const { name, label, description, fields = [] } = req.body;

  if (!name || !label) return res.status(400).json({ error: 'name and label are required' });
  if (!fields.length)  return res.status(400).json({ error: 'at least one field is required' });

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // 1. Create dataset definition
    const { rows: [def] } = await client.query(
      `INSERT INTO dataset_definitions (client_id, name, label, description, current_version)
       VALUES ($1,$2,$3,$4,1) RETURNING *`,
      [clientId, name.toLowerCase().replace(/\s+/g, '_'), label, description]
    );

    // 2. Create schema version 1
    await client.query(
      `INSERT INTO dataset_schema_versions (dataset_id, version, fields, compat_status)
       VALUES ($1, 1, $2, 'ok')`,
      [def.id, JSON.stringify(fields)]
    );

    // 3. Create ES index + alias
    const esIdx = await es.createIndex(clientId, name, 1, fields);
    await es.swapAlias(clientId, name, 1);

    // 4. Store es_alias + es_index back
    const alias = es.aliasName(clientId, name);
    await client.query(
      `UPDATE dataset_definitions SET es_alias=$1, updated_at=NOW() WHERE id=$2`,
      [alias, def.id]
    );
    await client.query(
      `UPDATE dataset_schema_versions SET es_index=$1, published_at=NOW(), published_by=$2 WHERE dataset_id=$3 AND version=1`,
      [esIdx, keyLabel || 'system', def.id]
    );

    await client.query('COMMIT');
    res.json({ success: true, dataset: { ...def, esAlias: alias, esIndex: esIdx } });
  } catch (e) {
    await client.query('ROLLBACK');
    res.status(500).json({ error: e.message });
  } finally {
    client.release();
  }
});

// GET /api/admin/datasets/:name
router.get('/admin/datasets/:name', requireApiKey, async (req, res) => {
  const { clientId } = req.ingestCtx;
  const { rows: [def] } = await pool.query(
    `SELECT * FROM dataset_definitions WHERE client_id=$1 AND name=$2`, [clientId, req.params.name]
  );
  if (!def) return res.status(404).json({ error: 'Dataset not found' });

  const { rows: versions } = await pool.query(
    `SELECT * FROM dataset_schema_versions WHERE dataset_id=$1 ORDER BY version DESC`, [def.id]
  );
  const { rows: invalidations } = await pool.query(
    `SELECT * FROM schema_invalidations WHERE dataset_id=$1 AND resolved=FALSE ORDER BY created_at DESC`, [def.id]
  );
  const stats = await es.indexStats(clientId, req.params.name);

  res.json({ success: true, dataset: def, versions, invalidations, stats });
});

// POST /api/admin/datasets/:name/publish
// Body: { fields: [...] }  — new version of the schema
router.post('/admin/datasets/:name/publish', requireApiKey, async (req, res) => {
  const { clientId, label: keyLabel } = req.ingestCtx;
  const { fields = [] } = req.body;

  if (!fields.length) return res.status(400).json({ error: 'fields are required' });

  const { rows: [def] } = await pool.query(
    `SELECT * FROM dataset_definitions WHERE client_id=$1 AND name=$2`, [clientId, req.params.name]
  );
  if (!def) return res.status(404).json({ error: 'Dataset not found' });

  // Get old fields for compat check
  const { rows: [oldVer] } = await pool.query(
    `SELECT * FROM dataset_schema_versions WHERE dataset_id=$1 AND version=$2`,
    [def.id, def.current_version]
  );
  const oldFields = oldVer?.fields || [];

  // Compat check
  const compat = checkCompat(oldFields, fields);
  const newVersion = def.current_version + 1;

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // Create new schema version row
    await client.query(
      `INSERT INTO dataset_schema_versions
       (dataset_id, version, fields, compat_status, compat_notes, published_at, published_by)
       VALUES ($1,$2,$3,$4,$5,NOW(),$6)`,
      [def.id, newVersion, JSON.stringify(fields),
       compat.status, JSON.stringify(compat.notes), keyLabel || 'system']
    );

    // Create new ES index from new mapping
    const newIdx = await es.createIndex(clientId, req.params.name, newVersion, fields);

    // Swap alias to new index
    await es.swapAlias(clientId, req.params.name, newVersion, def.current_version);

    // Update dataset definition
    await client.query(
      `UPDATE dataset_definitions SET current_version=$1, updated_at=NOW() WHERE id=$2`,
      [newVersion, def.id]
    );
    await client.query(
      `UPDATE dataset_schema_versions SET es_index=$1 WHERE dataset_id=$2 AND version=$3`,
      [newIdx, def.id, newVersion]
    );

    await client.query('COMMIT');

    // Run invalidation check (async, after commit)
    const invalidations = await runInvalidationCheck(
      clientId, def.id, req.params.name,
      def.current_version, newVersion, compat.notes
    );

    res.json({
      success: true,
      newVersion,
      esIndex: newIdx,
      compat: { status: compat.status, notes: compat.notes },
      invalidations: invalidations.length,
      warning: compat.breaking
        ? `Breaking changes detected. ${invalidations.length} saved report(s)/chart(s) may be affected.`
        : null
    });
  } catch (e) {
    await client.query('ROLLBACK');
    res.status(500).json({ error: e.message });
  } finally {
    client.release();
  }
});


// ── Lookup admin routes ────────────────────────────────────────────────────────

// GET /api/admin/lookups
// List all lookup datasets for this instance
router.get('/admin/lookups', requireApiKey, async (req, res) => {
  const { clientId } = req.ingestCtx;
  const { rows } = await pool.query(
    `SELECT ld.*,
            COUNT(ldr.id)::int AS row_count
     FROM lookup_datasets ld
     LEFT JOIN lookup_dataset_rows ldr ON ldr.lookup_dataset_id = ld.id
     WHERE ld.client_id = $1 AND ld.is_active = TRUE
     GROUP BY ld.id ORDER BY ld.name`,
    [clientId]
  );
  res.json({ success: true, lookups: rows });
});

// GET /api/admin/lookups/:name
// Get full lookup definition + fields + rows
router.get('/admin/lookups/:name', requireApiKey, async (req, res) => {
  const { clientId } = req.ingestCtx;
  const { rows: [ld] } = await pool.query(
    `SELECT * FROM lookup_datasets WHERE client_id=$1 AND name=$2 AND is_active=TRUE`,
    [clientId, req.params.name]
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
});

// POST /api/admin/lookups/:name/rows
// Structured API feed: upsert one or many rows with full data (address, hours, etc.)
// This is the "authoritative external system" path — always writes, sets auto_populated=FALSE.
// Ingest sync will fill only NULL fields on these rows going forward.
// Body: { rows: [{ key_value, ...fieldData }] }  OR a single object
router.post('/admin/lookups/:name/rows', requireApiKey, async (req, res) => {
  const { clientId } = req.ingestCtx;
  const { rows: [ld] } = await pool.query(
    `SELECT * FROM lookup_datasets WHERE client_id=$1 AND name=$2 AND is_active=TRUE`,
    [clientId, req.params.name]
  );
  if (!ld) return res.status(404).json({ error: `Lookup '${req.params.name}' not found` });

  const payload  = req.body;
  const incoming = Array.isArray(payload.rows) ? payload.rows
                 : Array.isArray(payload)       ? payload
                 : [payload];

  if (!incoming.length) return res.status(400).json({ error: 'No rows provided' });

  let upserted = 0;
  const errors = [];

  for (const row of incoming) {
    const keyValue = row[ld.key_field];
    if (!keyValue) { errors.push({ row, error: `Missing key field '${ld.key_field}'` }); continue; }

    const data = { ...row };
    delete data[ld.key_field];

    try {
      // External API feed: merge into existing data, mark as not auto_populated
      // so ingest sync will only fill remaining null fields going forward
      await pool.query(
        `INSERT INTO lookup_dataset_rows (lookup_dataset_id, key_value, data, auto_populated)
         VALUES ($1,$2,$3,FALSE)
         ON CONFLICT (lookup_dataset_id, key_value)
         DO UPDATE SET data           = lookup_dataset_rows.data || $3::jsonb,
                       auto_populated = FALSE,
                       updated_at     = NOW()`,
        [ld.id, String(keyValue), JSON.stringify(data)]
      );
      upserted++;
    } catch (e) {
      errors.push({ key_value: keyValue, error: e.message });
    }
  }

  res.json({ success: true, upserted, errors });
});

// DELETE /api/admin/lookups/:name/rows/:keyValue
router.delete('/admin/lookups/:name/rows/:keyValue', requireApiKey, async (req, res) => {
  const { clientId } = req.ingestCtx;
  const { rows: [ld] } = await pool.query(
    `SELECT id FROM lookup_datasets WHERE client_id=$1 AND name=$2`,
    [clientId, req.params.name]
  );
  if (!ld) return res.status(404).json({ error: `Lookup '${req.params.name}' not found` });
  await pool.query(
    `DELETE FROM lookup_dataset_rows WHERE lookup_dataset_id=$1 AND key_value=$2`,
    [ld.id, req.params.keyValue]
  );
  res.json({ success: true });
});

// ── API Key management ─────────────────────────────────────────────────────────

// POST /api/admin/api-keys  — generate key for instance
// NOTE: Can bootstrap first key via a one-time admin endpoint (see server.js)
router.post('/admin/api-keys', requireApiKey, async (req, res) => {
  const { clientId } = req.ingestCtx;
  const { label } = req.body;

  const raw  = generateKey();
  const hash = hashKey(raw);

  await pool.query(
    `INSERT INTO instance_api_keys (client_id, key_hash, label) VALUES ($1,$2,$3)`,
    [clientId, hash, label || 'API Key']
  );

  // Return raw key ONCE — never stored in plain text
  res.json({ success: true, key: raw, label: label || 'API Key',
             warning: 'Store this key securely — it cannot be retrieved again.' });
});

// GET /api/admin/api-keys
router.get('/admin/api-keys', requireApiKey, async (req, res) => {
  const { clientId } = req.ingestCtx;
  const { rows } = await pool.query(
    `SELECT id, label, last_used_at, revoked, created_at FROM instance_api_keys WHERE client_id=$1 ORDER BY created_at DESC`,
    [clientId]
  );
  res.json({ success: true, keys: rows });
});

// DELETE /api/admin/api-keys/:id
router.delete('/admin/api-keys/:id', requireApiKey, async (req, res) => {
  const { clientId } = req.ingestCtx;
  await pool.query(
    `UPDATE instance_api_keys SET revoked=TRUE WHERE id=$1 AND client_id=$2`,
    [req.params.id, clientId]
  );
  res.json({ success: true });
});


// ── Channel management routes ─────────────────────────────────────────────────
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 100 * 1024 * 1024 }  // 100MB max CSV
});

// GET /api/ingest/:dataset/channels — list all channels for this dataset
router.get('/:dataset/channels', requireApiKey, async (req, res) => {
  const { clientId } = req.ingestCtx;
  const { dataset }  = req.params;
  const def = await getDatasetDef(clientId, dataset);
  if (!def) return res.status(404).json({ error: `Dataset '${dataset}' not found` });

  const { rows } = await pool.query(
    `SELECT id, channel_name, method, mode, id_field, is_active, options,
            last_run_at, last_run_count, last_run_ok, webhook_token, created_at, updated_at
     FROM ingest_channels WHERE dataset_id=$1 ORDER BY created_at`,
    [def.id]
  );
  // Mask webhook_token — only show last 6 chars
  const safe = rows.map(r => ({
    ...r,
    webhook_url: r.webhook_token
      ? `${process.env.PUBLIC_URL || ''}/api/ingest/${dataset}/webhook/${r.webhook_token}`
      : null,
    webhook_token: r.webhook_token ? '••••' + r.webhook_token.slice(-6) : null
  }));
  res.json({ success: true, channels: safe });
});

// POST /api/ingest/:dataset/channels — create or update a channel
// Body: { channel_name, method, mode, id_field, is_active, options }
router.post('/:dataset/channels', requireApiKey, async (req, res) => {
  const { clientId } = req.ingestCtx;
  const { dataset }  = req.params;
  const { channel_name, method, mode = 'batch', id_field, is_active = true, options = {} } = req.body;

  if (!channel_name || !method) return res.status(400).json({ error: 'channel_name and method are required' });
  const VALID_METHODS = ['api_push', 'csv', 'webhook', 'sftp'];
  if (!VALID_METHODS.includes(method)) return res.status(400).json({ error: `method must be one of: ${VALID_METHODS.join(', ')}` });

  const def = await getDatasetDef(clientId, dataset);
  if (!def) return res.status(404).json({ error: `Dataset '${dataset}' not found` });

  // Auto-generate webhook token for webhook channels
  let webhookToken = null;
  if (method === 'webhook') {
    const { rows: existing } = await pool.query(
      `SELECT webhook_token FROM ingest_channels WHERE dataset_id=$1 AND channel_name=$2`,
      [def.id, channel_name]
    );
    webhookToken = existing[0]?.webhook_token || crypto.randomBytes(24).toString('hex');
  }

  const { rows: [ch] } = await pool.query(
    `INSERT INTO ingest_channels
       (dataset_id, client_id, channel_name, method, mode, id_field, is_active, options, webhook_token)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
     ON CONFLICT (dataset_id, channel_name) DO UPDATE
       SET method=$4, mode=$5, id_field=$6, is_active=$7, options=$8,
           webhook_token=COALESCE(ingest_channels.webhook_token, $9),
           updated_at=NOW()
     RETURNING *`,
    [def.id, clientId, channel_name, method, mode, id_field || null,
     is_active, JSON.stringify(options), webhookToken]
  );

  res.json({
    success: true,
    channel: {
      ...ch,
      webhook_url: ch.webhook_token
        ? `${process.env.PUBLIC_URL || ''}/api/ingest/${dataset}/webhook/${ch.webhook_token}`
        : null,
      webhook_token: ch.webhook_token ? '••••' + ch.webhook_token.slice(-6) : null
    }
  });
});

// PATCH /api/ingest/:dataset/channels/:channelName — toggle active or update options
router.patch('/:dataset/channels/:channelName', requireApiKey, async (req, res) => {
  const { clientId } = req.ingestCtx;
  const { dataset, channelName } = req.params;
  const def = await getDatasetDef(clientId, dataset);
  if (!def) return res.status(404).json({ error: `Dataset '${dataset}' not found` });

  const updates = [];
  const vals    = [def.id, channelName];
  let i = 3;
  if (req.body.is_active  !== undefined) { updates.push(`is_active=$${i++}`);  vals.push(req.body.is_active); }
  if (req.body.id_field   !== undefined) { updates.push(`id_field=$${i++}`);   vals.push(req.body.id_field || null); }
  if (req.body.options    !== undefined) { updates.push(`options=$${i++}`);    vals.push(JSON.stringify(req.body.options)); }
  if (req.body.mode       !== undefined) { updates.push(`mode=$${i++}`);       vals.push(req.body.mode); }
  if (!updates.length) return res.status(400).json({ error: 'Nothing to update' });

  updates.push('updated_at=NOW()');
  const { rows: [ch] } = await pool.query(
    `UPDATE ingest_channels SET ${updates.join(',')}
     WHERE dataset_id=$1 AND channel_name=$2 RETURNING *`,
    vals
  );
  if (!ch) return res.status(404).json({ error: 'Channel not found' });
  res.json({ success: true, channel: ch });
});

// ── CSV ingest ─────────────────────────────────────────────────────────────────
// POST /api/ingest/:dataset/csv
// Multipart: field "file" = CSV file, optional field "channel" = channel_name
// Resolves id_field from the named channel (or falls back to null = auto-id).
router.post('/:dataset/csv', requireApiKey, upload.single('file'), async (req, res) => {
  const { clientId, label } = req.ingestCtx;
  const { dataset } = req.params;
  const channelName = req.body?.channel || 'Nightly CSV';
  const t0 = Date.now();

  if (!req.file) return res.status(400).json({ error: 'No file uploaded — use multipart field "file"' });

  const def = await getDatasetDef(clientId, dataset);
  if (!def) return res.status(404).json({ error: `Dataset '${dataset}' not found` });

  // Resolve channel config for id_field
  const { rows: [ch] } = await pool.query(
    `SELECT id_field FROM ingest_channels
     WHERE dataset_id=$1 AND channel_name=$2 AND method='csv'`,
    [def.id, channelName]
  );
  const idField = ch?.id_field || null;

  // Parse CSV
  let rows;
  try {
    rows = csvParse(req.file.buffer, {
      columns:          true,    // first row = header
      skip_empty_lines: true,
      trim:             true,
      cast:             true,    // auto-cast numbers/booleans
    });
  } catch (e) {
    return res.status(400).json({ error: `CSV parse error: ${e.message}` });
  }

  if (!rows.length) return res.status(400).json({ error: 'CSV contained no data rows' });

  try {
    const result = await es.bulkIndex(clientId, dataset, rows, idField);
    const durationMs = Date.now() - t0;

    // Update channel last_run stats
    if (ch) {
      await pool.query(
        `UPDATE ingest_channels SET last_run_at=NOW(), last_run_count=$1, last_run_ok=$2
         WHERE dataset_id=$3 AND channel_name=$4`,
        [result.indexed, result.failed === 0, def.id, channelName]
      );
    }
    await logIngest(clientId, dataset, 'csv', result, label, durationMs);
    syncLookupFromDocs(clientId, dataset, def.id, rows).catch(() => {});

    res.json({ success: true, channel: channelName, idField, rows: rows.length,
               indexed: result.indexed, failed: result.failed, errors: result.errors, durationMs });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── Webhook ingest ─────────────────────────────────────────────────────────────
// POST /api/ingest/:dataset/webhook/:token
// No X-Api-Key needed — token in URL authenticates the channel.
// Body: single doc object OR { docs: [...] } array
// Optional HMAC: header X-Webhook-Signature: sha256=<hex> verifies against channel options.secret
router.post('/:dataset/webhook/:token', async (req, res) => {
  const { dataset, token } = req.params;
  const t0 = Date.now();

  // Resolve channel by webhook token
  const { rows: [ch] } = await pool.query(
    `SELECT ic.*, dd.client_id as owner_client_id, dd.id as ds_id
     FROM ingest_channels ic
     JOIN dataset_definitions dd ON dd.id = ic.dataset_id
     WHERE ic.webhook_token=$1 AND ic.method='webhook' AND ic.is_active=TRUE
       AND dd.name=$2`,
    [token, dataset]
  );
  if (!ch) return res.status(401).json({ error: 'Invalid or inactive webhook token' });

  // Optional HMAC signature verification
  const secret = ch.options?.secret;
  if (secret) {
    const sig = req.headers['x-webhook-signature'] || '';
    const expected = 'sha256=' + crypto
      .createHmac('sha256', secret)
      .update(JSON.stringify(req.body))
      .digest('hex');
    if (sig !== expected) {
      return res.status(401).json({ error: 'Invalid webhook signature' });
    }
  }

  const payload = req.body;
  const docs    = Array.isArray(payload)       ? payload
                : Array.isArray(payload?.docs)  ? payload.docs
                : [payload];

  if (!docs.length || !docs[0]) return res.status(400).json({ error: 'No documents in payload' });

  const idField = ch.id_field || null;
  try {
    const result = await es.bulkIndex(ch.owner_client_id, dataset, docs, idField);
    const durationMs = Date.now() - t0;

    // Update channel last_run stats
    await pool.query(
      `UPDATE ingest_channels SET last_run_at=NOW(), last_run_count=$1, last_run_ok=$2 WHERE id=$3`,
      [result.indexed, result.failed === 0, ch.id]
    );
    await logIngest(ch.owner_client_id, dataset, 'webhook', result, `webhook:${token.slice(-6)}`, durationMs);

    const def = await getDatasetDef(ch.owner_client_id, dataset);
    if (def) syncLookupFromDocs(ch.owner_client_id, dataset, def.id, docs).catch(() => {});

    res.json({ success: true, indexed: result.indexed, failed: result.failed, durationMs });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── Production-grade API push ─────────────────────────────────────────────────
//
// POST /api/ingest/:dataset/push
//
// Designed for high-volume ingest (nightly jobs, real-time feeds, re-sends).
// Key features:
//   - id_field auto-resolved from channel config (set once in Designer)
//   - When id_field is set: uses bulkUpsert — scripted ES update that atomically
//     increments __ingest_version, fully idempotent (safe to re-send same batch)
//   - When no id_field: falls back to bulkIndex (append-only, auto-id)
//   - Chunked internally by esClient (BULK_CHUNK_SIZE docs/request)
//   - Structured per-batch logging: batch_id, doc count, chunks, timing, errors
//   - Payload cap: 50 000 docs per call — split larger jobs at the sender
//   - replace=true: full dataset swap (delete-by-query + reindex)
//
// Body: { docs: [...], replace?: false, channel?: 'API Push' }
// Query param: ?channel=<channel_name>  (overrides body.channel)
//
const MAX_DOCS_PER_CALL = 50_000;

router.post('/:dataset/push', requireApiKey, async (req, res) => {
  const { clientId, label } = req.ingestCtx;
  const { dataset } = req.params;
  const channelName = req.query.channel || req.body?.channel || 'API Push';
  const { docs = [], replace = false } = req.body;
  const batchId = crypto.randomBytes(8).toString('hex');  // unique per call for log correlation
  const t0 = Date.now();

  // ── Input validation ────────────────────────────────────────────────────────
  if (!Array.isArray(docs) || docs.length === 0) {
    return res.status(400).json({
      success: false,
      error:   'docs must be a non-empty array',
      batch_id: batchId
    });
  }
  if (docs.length > MAX_DOCS_PER_CALL) {
    return res.status(400).json({
      success:  false,
      error:    `Payload too large: ${docs.length} docs exceeds the ${MAX_DOCS_PER_CALL.toLocaleString()} doc limit per call. Split into smaller batches.`,
      batch_id: batchId
    });
  }

  // ── Resolve dataset & channel ────────────────────────────────────────────────
  const def = await getDatasetDef(clientId, dataset);
  if (!def) {
    return res.status(404).json({
      success:  false,
      error:    `Dataset '${dataset}' not found for this instance`,
      batch_id: batchId
    });
  }

  const { rows: [ch] } = await pool.query(
    `SELECT id, id_field FROM ingest_channels
     WHERE dataset_id=$1 AND channel_name=$2 AND method='api_push'`,
    [def.id, channelName]
  );
  const idField = ch?.id_field || null;

  console.log(`[ingest/push] batch_id=${batchId} client=${clientId} dataset=${dataset} channel=${channelName} docs=${docs.length} idField=${idField || 'none'} replace=${replace}`);

  try {
    let result;

    if (replace) {
      // Full swap: wipe instance slice + re-index everything
      result = await es.replaceAll(clientId, dataset, docs);
      result.chunks = Math.ceil(docs.length / es.BULK_CHUNK_SIZE);
    } else if (idField) {
      // High-volume upsert path: scripted update with __ingest_version increment
      result = await es.bulkUpsert(clientId, dataset, docs, idField);
    } else {
      // Append-only: no id_field configured, fall back to standard bulk index
      result = await es.bulkIndex(clientId, dataset, docs);
      result.chunks = Math.ceil(docs.length / es.BULK_CHUNK_SIZE);
    }

    const durationMs = Date.now() - t0;

    // ── Structured log ────────────────────────────────────────────────────────
    console.log(`[ingest/push] batch_id=${batchId} done: indexed=${result.indexed} failed=${result.failed} chunks=${result.chunks || 1} durationMs=${durationMs}`);
    if (result.failed > 0) {
      console.warn(`[ingest/push] batch_id=${batchId} errors (first ${result.errors.length}):`, JSON.stringify(result.errors));
    }

    // ── Update channel last-run stats ─────────────────────────────────────────
    if (ch) {
      await pool.query(
        `UPDATE ingest_channels
         SET last_run_at=NOW(), last_run_count=$1, last_run_ok=$2
         WHERE id=$3`,
        [result.indexed, result.failed === 0, ch.id]
      );
    }

    // ── Persist ingest log row ────────────────────────────────────────────────
    await logIngest(
      clientId, dataset,
      replace ? 'replace' : (idField ? 'push_upsert' : 'push'),
      result, label, durationMs
    );

    // ── Async lookup maintenance (never blocks response) ──────────────────────
    syncLookupFromDocs(clientId, dataset, def.id, docs).catch(e =>
      console.warn(`[ingest/push] batch_id=${batchId} lookup sync error:`, e.message)
    );

    // ── Response ─────────────────────────────────────────────────────────────
    return res.json({
      success:   true,
      batch_id:  batchId,
      channel:   channelName,
      id_field:  idField,
      indexed:   result.indexed,
      failed:    result.failed,
      chunks:    result.chunks || 1,
      durationMs,
      errors:    result.errors    // [] when clean; up to 20 sample errors if any
    });

  } catch (e) {
    const durationMs = Date.now() - t0;
    console.error(`[ingest/push] batch_id=${batchId} FATAL:`, e.message);
    return res.status(500).json({
      success:   false,
      batch_id:  batchId,
      error:     e.message,
      durationMs
    });
  }
});


module.exports = { router, resolveApiKey, hashKey, generateKey };
