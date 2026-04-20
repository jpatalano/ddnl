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

const express = require('express');
const crypto  = require('crypto');
const { pool } = require('./db');
const es      = require('./esClient');

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

module.exports = { router, resolveApiKey, hashKey, generateKey };
