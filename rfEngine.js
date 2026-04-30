// rfEngine.js — Reference Data engine
// =============================================================================
// Per-instance enrichment dimensions (locations, employees, roles, etc.).
// Multi-tenant: every operation is scoped by client_id. No instance-specific
// values are hardcoded.
//
// Concepts
//   rf_tables          metadata: one row per declared rf table per instance
//   rf_fields          metadata: column definitions for an rf table
//   dataset_rf_joins   metadata: wires a fact dataset → rf table for auto-discover + enrich
//   rf_<name>          physical Postgres table holding the rows for an rf table.
//                      ALL instances share this table; rows are namespaced by client_id.
//   rf_<owner>_<field>_link
//                      physical link table for multi_select_rf fields.
//
// Field types
//   text | longtext | number | boolean | date | datetime | dropdown
//   rf_ref            single FK to another rf table; options.target = '<rf_name>',
//                     options.select_style = 'autocomplete' | 'dropdown' (default: autocomplete)
//   multi_select_rf   N:M link to another rf table; options.target = '<rf_name>'
//
// Per-row meta sidecar (`_meta` JSONB)
//   { <field_name>: { source: 'pos'|'manual'|...,
//                     updated_at: '...',
//                     updated_by: '...' } }
//
// Source-scoped upsert
//   When an API source writes via /api/ingest/rf/<name>/<source>/bulk, only fields
//   whose rf_fields.source matches that source are written. All other fields are
//   left untouched. The "manual" source is reserved for human edits via the admin UI.
// =============================================================================

const { pool } = require('./db');

// ── Identifier safety ────────────────────────────────────────────────────────
// Postgres identifiers are validated against a strict allowlist before being
// interpolated into DDL/DML. Throws on anything suspicious.
const IDENT_RE = /^[a-z_][a-z0-9_]{0,62}$/;
function safeIdent(name, kind = 'identifier') {
  if (typeof name !== 'string' || !IDENT_RE.test(name)) {
    throw new Error(`Invalid ${kind}: ${JSON.stringify(name)}`);
  }
  return name;
}

// ── Type mapping ─────────────────────────────────────────────────────────────
// Maps an rf field_type to a Postgres column type for storage_table_name.
function pgTypeFor(field) {
  switch (field.field_type) {
    case 'text':
    case 'longtext':
    case 'dropdown':
    case 'rf_ref':       return 'TEXT';
    case 'number':       return 'NUMERIC';
    case 'boolean':      return 'BOOLEAN';
    case 'date':         return 'DATE';
    case 'datetime':     return 'TIMESTAMPTZ';
    case 'multi_select_rf':
      return null;       // stored in a link table, not on the row
    default:
      throw new Error(`Unknown field_type: ${field.field_type}`);
  }
}

// ── Cache ────────────────────────────────────────────────────────────────────
// Per-(client, table) row cache keyed by PK; invalidated on edit/upsert + 60s TTL.
const CACHE = new Map(); // key: `${clientId}:${rfName}` → { rows: Map<pk,row>, expires: ts }
const CACHE_TTL_MS = 60_000;

function cacheKey(clientId, rfName) { return `${clientId}:${rfName}`; }

function invalidateCache(clientId, rfName) {
  CACHE.delete(cacheKey(clientId, rfName));
}

// ── Metadata loaders ─────────────────────────────────────────────────────────

async function getRfTable(clientId, rfName, client) {
  const c = client || pool;
  const { rows } = await c.query(
    `SELECT * FROM rf_tables WHERE client_id=$1 AND name=$2`, [clientId, rfName]
  );
  return rows[0] || null;
}

async function getRfFields(rfTableId, client) {
  const c = client || pool;
  const { rows } = await c.query(
    `SELECT * FROM rf_fields WHERE rf_table_id=$1 ORDER BY sort_order, id`, [rfTableId]
  );
  return rows;
}

async function listRfTables(clientId, client) {
  const c = client || pool;
  const { rows } = await c.query(
    `SELECT * FROM rf_tables
     WHERE client_id=$1
     ORDER BY group_label NULLS LAST, sort_order, label`,
    [clientId]
  );
  return rows;
}

// ── DDL: ensure storage tables exist ─────────────────────────────────────────
// Idempotent. Adds columns to existing tables when new fields are declared.

async function ensureStorageTable(rfTable, fields, client) {
  const c = client || pool;
  const tbl = safeIdent(rfTable.storage_table_name, 'storage_table_name');
  const pk  = safeIdent(rfTable.pk_field, 'pk_field');

  // 1) Base table — created with PK + meta + audit columns.
  //    PK column type is inferred from the rf_fields entry where is_pk=true (default TEXT).
  const pkField = fields.find(f => f.is_pk) || { field_type: 'text' };
  const pkType  = pgTypeFor(pkField) || 'TEXT';

  await c.query(`
    CREATE TABLE IF NOT EXISTS ${tbl} (
      client_id          VARCHAR(255) NOT NULL,
      ${pk}              ${pkType}    NOT NULL,
      _meta              JSONB        NOT NULL DEFAULT '{}',
      _source_origin     VARCHAR(64),                    -- which source first created this row
      _created_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
      _updated_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
      _updated_by        VARCHAR(255),
      PRIMARY KEY (client_id, ${pk})
    )
  `);
  await c.query(`CREATE INDEX IF NOT EXISTS ${tbl}_client_idx ON ${tbl}(client_id)`);

  // 2) Add missing non-PK, non-multi_select columns. Type changes are NOT performed
  //    automatically — those need a manual migration (we'd rather error than corrupt).
  const { rows: existing } = await c.query(`
    SELECT column_name FROM information_schema.columns
    WHERE table_schema = current_schema() AND table_name = $1
  `, [tbl]);
  const have = new Set(existing.map(r => r.column_name));

  for (const f of fields) {
    if (f.is_pk) continue;
    const pgType = pgTypeFor(f);
    if (pgType === null) continue; // multi_select_rf: skip column, link table created below
    const col = safeIdent(f.field_name, 'field_name');
    if (!have.has(col)) {
      await c.query(`ALTER TABLE ${tbl} ADD COLUMN IF NOT EXISTS ${col} ${pgType}`);
    }
  }

  // 3) Multi-select link tables — one per multi_select_rf field on this rf table.
  for (const f of fields) {
    if (f.field_type !== 'multi_select_rf') continue;
    const link = safeIdent(`${rfTable.storage_table_name}_${f.field_name}_link`, 'link_table_name');
    await c.query(`
      CREATE TABLE IF NOT EXISTS ${link} (
        client_id      VARCHAR(255) NOT NULL,
        owner_pk       TEXT         NOT NULL,
        target_pk      TEXT         NOT NULL,
        source         VARCHAR(64)  NOT NULL DEFAULT 'manual',
        _updated_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        PRIMARY KEY (client_id, owner_pk, target_pk)
      )
    `);
    await c.query(`CREATE INDEX IF NOT EXISTS ${link}_owner_idx ON ${link}(client_id, owner_pk)`);
  }
}

// ── Seed loader ──────────────────────────────────────────────────────────────
// Reads seeds/<instance>/rf.json (if present) and idempotently:
//   - upserts rf_tables, rf_fields, dataset_rf_joins
//   - creates/alters storage tables to match the declared schema
//   - seeds dictionary rows (rf table entries declared inline, e.g. status 0=Active/1=Inactive)
//
// Designed to run on every boot. Safe to call repeatedly.

async function loadSeed(clientId, seed) {
  if (!seed || !Array.isArray(seed.tables)) return { tables: 0 };
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    let count = 0;

    for (const t of seed.tables) {
      const name = safeIdent(t.name, 'rf table name');
      const storage = `rf_${name}`;
      // Upsert rf_tables row
      const { rows: tblRows } = await client.query(`
        INSERT INTO rf_tables
          (client_id, name, storage_table_name, label, label_singular, icon,
           group_label, pk_field, pk_field_label, pk_locked_message,
           auto_discover, show_in_admin_nav, sort_order, updated_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13, NOW())
        ON CONFLICT (client_id, name) DO UPDATE SET
          label=EXCLUDED.label,
          label_singular=EXCLUDED.label_singular,
          icon=EXCLUDED.icon,
          group_label=EXCLUDED.group_label,
          pk_field=EXCLUDED.pk_field,
          pk_field_label=EXCLUDED.pk_field_label,
          pk_locked_message=EXCLUDED.pk_locked_message,
          auto_discover=EXCLUDED.auto_discover,
          show_in_admin_nav=EXCLUDED.show_in_admin_nav,
          sort_order=EXCLUDED.sort_order,
          updated_at=NOW()
        RETURNING *
      `, [
        clientId, name, storage, t.label, t.label_singular || null, t.icon || null,
        t.group_label || null, t.pk_field, t.pk_field_label || null, t.pk_locked_message || null,
        !!t.auto_discover, t.show_in_admin_nav !== false, t.sort_order || 100
      ]);
      const rfTable = tblRows[0];

      // Upsert rf_fields rows
      const declaredFieldNames = new Set();
      for (let i = 0; i < (t.fields || []).length; i++) {
        const f = t.fields[i];
        const fname = safeIdent(f.field_name, 'field_name');
        declaredFieldNames.add(fname);
        await client.query(`
          INSERT INTO rf_fields
            (rf_table_id, field_name, field_label, field_type, options, source,
             is_pk, is_required, is_segment, is_metric, sort_order)
          VALUES ($1,$2,$3,$4,$5::jsonb,$6,$7,$8,$9,$10,$11)
          ON CONFLICT (rf_table_id, field_name) DO UPDATE SET
            field_label=EXCLUDED.field_label,
            field_type=EXCLUDED.field_type,
            options=EXCLUDED.options,
            source=EXCLUDED.source,
            is_pk=EXCLUDED.is_pk,
            is_required=EXCLUDED.is_required,
            is_segment=EXCLUDED.is_segment,
            is_metric=EXCLUDED.is_metric,
            sort_order=EXCLUDED.sort_order
        `, [
          rfTable.id, fname, f.field_label || null, f.field_type,
          JSON.stringify(f.options || {}),
          f.source || 'manual',
          !!f.is_pk, !!f.is_required, !!f.is_segment, !!f.is_metric,
          f.sort_order != null ? f.sort_order : (i * 10 + 10)
        ]);
      }
      // Note: we do NOT delete fields that are no longer in the seed — rename/remove is manual
      //       to avoid accidental data loss. Operators drop columns explicitly when needed.

      // Materialize storage table
      const fields = await getRfFields(rfTable.id, client);
      await ensureStorageTable(rfTable, fields, client);

      // Seed dictionary rows (tiny lookups whose values are owned by config, not imports)
      if (Array.isArray(t.seed_rows) && t.seed_rows.length) {
        for (const row of t.seed_rows) {
          await upsertRow({
            clientId,
            rfName: name,
            pkValue: row[t.pk_field],
            data: row,
            source: 'seed',
            updatedBy: 'system',
            client
          });
        }
      }
      count++;
    }

    // Dataset → rf joins
    if (Array.isArray(seed.dataset_joins)) {
      for (const j of seed.dataset_joins) {
        const rfTable = await getRfTable(clientId, j.rf_table, client);
        if (!rfTable) {
          console.warn(`[rf seed] dataset_join references unknown rf table '${j.rf_table}' — skipping`);
          continue;
        }
        await client.query(`
          INSERT INTO dataset_rf_joins
            (client_id, dataset_name, rf_table_id, source_field, name_hint_field)
          VALUES ($1,$2,$3,$4,$5)
          ON CONFLICT (client_id, dataset_name, rf_table_id, source_field) DO UPDATE SET
            name_hint_field = EXCLUDED.name_hint_field
        `, [clientId, j.dataset_name, rfTable.id, j.source_field, j.name_hint_field || null]);
      }
    }

    await client.query('COMMIT');
    return { tables: count };
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally {
    client.release();
  }
}

// ── Row CRUD ─────────────────────────────────────────────────────────────────

// Upsert one row. Source-scoped: only fields belonging to `source` are written
// (or all fields if source is 'manual' — the admin UI's god-mode source).
// Implementation: build a single $-placeholder list, used identically by both
// the INSERT VALUES and the ON CONFLICT DO UPDATE. Cleaner than tracking two
// parallel placeholder sequences.
async function upsertRow({ clientId, rfName, pkValue, data, source, updatedBy, client }) {
  const release = !client;
  const conn = release ? await pool.connect() : client;
  try {
    if (release) await conn.query('BEGIN');
    const rfTable = await getRfTable(clientId, rfName, conn);
    if (!rfTable) throw new Error(`rf table not found: ${rfName}`);
    const fields  = await getRfFields(rfTable.id, conn);
    const pkField = fields.find(f => f.is_pk);
    if (!pkField) throw new Error(`rf table ${rfName} has no PK field`);
    if (pkValue == null || pkValue === '') throw new Error(`PK value required for ${rfName}`);

    const tbl = safeIdent(rfTable.storage_table_name, 'storage_table_name');
    const pk  = safeIdent(pkField.field_name, 'pk_field');

    // Which fields can this source write?
    const canWrite = (f) => {
      if (f.is_pk) return false;
      if (f.field_type === 'multi_select_rf') return false; // handled via setMultiSelect
      if (source === 'manual') return true;                 // admin UI: any non-multi field
      return f.source === source;                           // strict source match
    };

    // Collect (col, val) pairs for the writable fields the caller actually provided.
    const writes = [];
    const metaPatch = {};
    const now = new Date().toISOString();
    for (const f of fields) {
      if (!canWrite(f)) continue;
      if (!Object.prototype.hasOwnProperty.call(data, f.field_name)) continue;
      let val = data[f.field_name];
      if (val === '') val = null;
      if (f.field_type === 'boolean' && val != null) val = !!val;
      if (f.field_type === 'number'  && val != null) val = Number(val);
      writes.push({ col: safeIdent(f.field_name, 'field_name'), val });
      metaPatch[f.field_name] = { source, updated_at: now, updated_by: updatedBy || null };
    }

    // Build params + placeholders as a single list:
    //   $1 client_id, $2 pk, $3..$N column values, then audit vars.
    const params = [clientId, pkValue];
    for (const w of writes) params.push(w.val);
    const metaJson  = JSON.stringify(metaPatch);
    const updatedByVal = updatedBy || null;
    params.push(metaJson);            // $A: _meta patch
    params.push(source);               // $B: source origin
    params.push(updatedByVal);         // $C: _updated_by

    const aIdx = 2 + writes.length + 1;  // index of $A
    const bIdx = aIdx + 1;
    const cIdx = aIdx + 2;

    const insertCols = ['client_id', pk, ...writes.map(w => w.col), '_meta', '_source_origin', '_updated_by'];
    const insertVals = [
      '$1', '$2',
      ...writes.map((_, i) => `$${3 + i}`),
      `$${aIdx}`, `$${bIdx}`, `$${cIdx}`
    ];

    const updateSets = [
      ...writes.map((w, i) => `${w.col} = $${3 + i}`),
      `_meta = COALESCE(${tbl}._meta, '{}'::jsonb) || $${aIdx}::jsonb`,
      // _source_origin only if NULL (i.e. on the first write that creates the row — but ON CONFLICT means row already exists, so this is a no-op for row creation. Kept for safety.)
      `_source_origin = COALESCE(${tbl}._source_origin, $${bIdx})`,
      `_updated_at = NOW()`,
      `_updated_by = $${cIdx}`
    ];

    const sql = `
      INSERT INTO ${tbl} (${insertCols.join(', ')})
      VALUES (${insertVals.join(', ')})
      ON CONFLICT (client_id, ${pk}) DO UPDATE SET ${updateSets.join(', ')}
      RETURNING *
    `;
    const { rows } = await conn.query(sql, params);
    if (release) await conn.query('COMMIT');
    invalidateCache(clientId, rfName);
    return rows[0];
  } catch (e) {
    if (release) await conn.query('ROLLBACK');
    throw e;
  } finally {
    if (release) conn.release();
  }
}

// Replace the full set of multi-select link rows for one (owner_pk, field) pair,
// scoped to a single source. Other-source links are left untouched.
async function setMultiSelect({ clientId, rfName, fieldName, ownerPk, targetPks, source, client }) {
  const c = client || pool;
  const release = !client;
  const conn = release ? await pool.connect() : c;
  try {
    if (release) await conn.query('BEGIN');
    const rfTable = await getRfTable(clientId, rfName, conn);
    if (!rfTable) throw new Error(`rf table not found: ${rfName}`);
    const link = safeIdent(`${rfTable.storage_table_name}_${fieldName}_link`, 'link_table_name');
    // Delete existing links for this (owner, source)
    await conn.query(
      `DELETE FROM ${link} WHERE client_id=$1 AND owner_pk=$2 AND source=$3`,
      [clientId, String(ownerPk), source]
    );
    // Insert new
    for (const tp of (targetPks || [])) {
      await conn.query(`
        INSERT INTO ${link} (client_id, owner_pk, target_pk, source)
        VALUES ($1,$2,$3,$4)
        ON CONFLICT (client_id, owner_pk, target_pk) DO UPDATE SET
          source = EXCLUDED.source,
          _updated_at = NOW()
      `, [clientId, String(ownerPk), String(tp), source]);
    }
    if (release) await conn.query('COMMIT');
    invalidateCache(clientId, rfName);
  } catch (e) {
    if (release) await conn.query('ROLLBACK');
    throw e;
  } finally {
    if (release) conn.release();
  }
}

// Get one row by PK, including all multi-select arrays.
async function getRow(clientId, rfName, pkValue) {
  const rfTable = await getRfTable(clientId, rfName);
  if (!rfTable) return null;
  const fields = await getRfFields(rfTable.id);
  const pkField = fields.find(f => f.is_pk);
  if (!pkField) return null;
  const tbl = safeIdent(rfTable.storage_table_name, 'storage_table_name');
  const pk  = safeIdent(pkField.field_name, 'pk_field');
  const { rows } = await pool.query(
    `SELECT * FROM ${tbl} WHERE client_id=$1 AND ${pk}=$2`,
    [clientId, pkValue]
  );
  if (!rows[0]) return null;
  const out = rows[0];
  // Multi-select aggregations
  for (const f of fields) {
    if (f.field_type !== 'multi_select_rf') continue;
    const link = safeIdent(`${rfTable.storage_table_name}_${f.field_name}_link`, 'link_table_name');
    const { rows: linkRows } = await pool.query(
      `SELECT target_pk FROM ${link} WHERE client_id=$1 AND owner_pk=$2 ORDER BY target_pk`,
      [clientId, String(pkValue)]
    );
    out[f.field_name] = linkRows.map(r => r.target_pk);
  }
  return out;
}

// List rows for an rf table. Light pagination — these tables are small.
async function listRows(clientId, rfName, { limit = 5000, offset = 0 } = {}) {
  const rfTable = await getRfTable(clientId, rfName);
  if (!rfTable) return { rows: [], total: 0 };
  const tbl = safeIdent(rfTable.storage_table_name, 'storage_table_name');
  const { rows: dataRows } = await pool.query(
    `SELECT * FROM ${tbl} WHERE client_id=$1 ORDER BY _created_at DESC LIMIT $2 OFFSET $3`,
    [clientId, limit, offset]
  );
  const { rows: cnt } = await pool.query(
    `SELECT COUNT(*)::int AS n FROM ${tbl} WHERE client_id=$1`, [clientId]
  );
  return { rows: dataRows, total: cnt[0].n };
}

// Cache full table → Map<pk, row> for read-time enrichment.
async function getCachedTable(clientId, rfName) {
  const k = cacheKey(clientId, rfName);
  const hit = CACHE.get(k);
  if (hit && hit.expires > Date.now()) return hit.rows;
  const rfTable = await getRfTable(clientId, rfName);
  if (!rfTable) return new Map();
  const fields = await getRfFields(rfTable.id);
  const pkField = fields.find(f => f.is_pk);
  const tbl = safeIdent(rfTable.storage_table_name, 'storage_table_name');
  const pk  = safeIdent(pkField.field_name, 'pk_field');
  const { rows } = await pool.query(
    `SELECT * FROM ${tbl} WHERE client_id=$1`, [clientId]
  );
  const map = new Map();
  for (const r of rows) map.set(String(r[pk]), r);
  CACHE.set(k, { rows: map, expires: Date.now() + CACHE_TTL_MS });
  return map;
}

// ── Auto-discovery hook ──────────────────────────────────────────────────────
// Called by ingestRouter for each fact row. For every join configured against
// the dataset, if the row's source_field PK is unknown, insert a stub.
//
// `factRow` is the source object (pre-ES-shape). Returns the count of new rows.
async function autoDiscoverFromFactRow(clientId, datasetName, factRow) {
  if (!factRow || typeof factRow !== 'object') return 0;
  const { rows: joins } = await pool.query(`
    SELECT j.*, t.name AS rf_name, t.storage_table_name, t.pk_field, t.auto_discover
    FROM dataset_rf_joins j
    JOIN rf_tables t ON t.id = j.rf_table_id
    WHERE j.client_id=$1 AND j.dataset_name=$2
  `, [clientId, datasetName]);
  if (!joins.length) return 0;

  let created = 0;
  for (const j of joins) {
    if (!j.auto_discover) continue;
    const rawPk = factRow[j.source_field];
    if (rawPk == null || rawPk === '') continue;
    const pkValue = String(rawPk);

    // Skip if already exists (cheap cache check first)
    const cached = await getCachedTable(clientId, j.rf_name);
    if (cached.has(pkValue)) continue;

    // Build stub data — PK + optional name hint
    const data = {};
    if (j.name_hint_field && factRow[j.name_hint_field] != null && factRow[j.name_hint_field] !== '') {
      data.name = String(factRow[j.name_hint_field]);
    } else {
      data.name = pkValue; // fallback: use the PK value as a placeholder name
    }

    try {
      // 'manual' source so the name_hint sticks until a human edits it; the field
      // sources match this. (auto_discovered source can't write any field.)
      await upsertRow({
        clientId, rfName: j.rf_name, pkValue,
        data, source: 'manual', updatedBy: 'auto_discover'
      });
      created++;
    } catch (e) {
      console.warn(`[rf auto_discover] ${j.rf_name} pk=${pkValue}: ${e.message}`);
    }
  }
  return created;
}

// ── Read-time enrichment ─────────────────────────────────────────────────────
// Given a list of fact rows, enrich them in place with rf data based on
// configured dataset_rf_joins. Each enriched field is namespaced as
// `<rf_name>.<field>` to avoid collision with fact fields.
async function enrichFactRows(clientId, datasetName, factRows) {
  if (!Array.isArray(factRows) || !factRows.length) return factRows;
  const { rows: joins } = await pool.query(`
    SELECT j.*, t.name AS rf_name, t.pk_field
    FROM dataset_rf_joins j
    JOIN rf_tables t ON t.id = j.rf_table_id
    WHERE j.client_id=$1 AND j.dataset_name=$2
  `, [clientId, datasetName]);
  if (!joins.length) return factRows;

  for (const j of joins) {
    const dict = await getCachedTable(clientId, j.rf_name);
    if (!dict.size) continue;
    for (const row of factRows) {
      const fk = row[j.source_field];
      if (fk == null) continue;
      const ref = dict.get(String(fk));
      if (!ref) continue;
      // Attach as namespaced fields to avoid collision
      for (const k of Object.keys(ref)) {
        if (k === 'client_id' || k.startsWith('_')) continue;
        row[`${j.rf_name}.${k}`] = ref[k];
      }
    }
  }
  return factRows;
}

module.exports = {
  // metadata
  getRfTable, getRfFields, listRfTables,
  // ddl + seed
  ensureStorageTable, loadSeed,
  // row crud
  upsertRow, setMultiSelect, getRow, listRows,
  // ingest hooks
  autoDiscoverFromFactRow, enrichFactRows,
  // cache
  invalidateCache, getCachedTable,
};
